use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Api, Client, Resource, ResourceExt};
use serde_json::json;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{error, info};

use crate::controller::utils::{resolve_namespace, resolve_value};
use crate::controller::{Context, Error, Result};
use crate::crd::{Database, DatabaseStatus, Instance, Namespace};
use crate::surreal::connect_instance;

/// Reconciles a Database resource.
pub async fn reconcile(db_resource: Arc<Database>, ctx: Arc<Context>) -> Result<Action> {
    let client = &ctx.client;
    let db_name = db_resource.name_any();
    let db_namespace = db_resource
        .namespace()
        .ok_or_else(|| Error::InternalError("Database is cluster-scoped".to_string()))?;

    info!("Reconciling Database {}/{}", db_namespace, db_name);

    let resolved_ns_namespace = resolve_namespace(&db_resource.spec.namespace_ref, &db_namespace);
    let ns_api: Api<Namespace> = Api::namespaced(client.clone(), &resolved_ns_namespace);

    // 1. Fetch the referenced Namespace resource
    let ns = match ns_api.get(&db_resource.spec.namespace_ref.name).await {
        Ok(namespace) => namespace,
        Err(e) => {
            let err_msg = format!(
                "Failed to get Namespace {}: {}",
                db_resource.spec.namespace_ref.name, e
            );
            error!("{}", err_msg);
            update_status(
                &db_resource,
                client,
                &db_namespace,
                false,
                None,
                None,
                Some(err_msg),
            )
            .await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };

    // 2. Check if Namespace is ready
    let _ns_status = match ns.status {
        Some(ref status) if status.created => status,
        Some(ref status) => {
            let err_msg = match status.error.clone() {
                Some(err) => format!(
                    "Referenced Namespace {} is unhealthy: {}",
                    ns.name_any(),
                    err
                ),
                None => format!("Referenced Namespace {} is not ready yet", ns.name_any()),
            };
            info!("{}", err_msg);
            let current_status = db_resource.status.clone().unwrap_or_default();
            update_status(
                &db_resource,
                client,
                &db_namespace,
                false,
                current_status.applied_schema_hash,
                current_status.applied_schema_generation,
                Some(err_msg),
            )
            .await?;
            return Ok(Action::requeue(Duration::from_secs(10)));
        }
        None => {
            let err_msg = format!("Namespace {} has no status yet", ns.name_any());
            info!("{}", err_msg);
            let current_status = db_resource.status.clone().unwrap_or_default();
            update_status(
                &db_resource,
                client,
                &db_namespace,
                false,
                current_status.applied_schema_hash,
                current_status.applied_schema_generation,
                Some(err_msg),
            )
            .await?;
            return Ok(Action::requeue(Duration::from_secs(10)));
        }
    };

    // 2b. Ensure Database has the correct OwnerReference to Namespace
    let has_owner = db_resource
        .metadata
        .owner_references
        .as_ref()
        .map(|refs| {
            refs.iter()
                .any(|r| r.uid == ns.metadata.uid.as_ref().cloned().unwrap_or_default())
        })
        .unwrap_or(false);
    if !has_owner {
        if let Some(owner_ref) = ns.controller_owner_ref(&()) {
            let api: Api<Database> = Api::namespaced(client.clone(), &db_namespace);
            let patch = json!({
                "metadata": {
                    "ownerReferences": [owner_ref]
                }
            });
            api.patch(
                &db_resource.name_any(),
                &PatchParams::default(),
                &Patch::Merge(&patch),
            )
            .await
            .map_err(Error::KubeError)?;
        }
    }

    // 3. Fetch the Instance from the Namespace spec
    let resolved_instance_ns = resolve_namespace(&ns.spec.instance_ref, &resolved_ns_namespace);
    let instance_api: Api<Instance> = Api::namespaced(client.clone(), &resolved_instance_ns);
    let instance = match instance_api.get(&ns.spec.instance_ref.name).await {
        Ok(inst) => inst,
        Err(e) => {
            let err_msg = format!(
                "Failed to get Instance {} from Namespace {}: {}",
                ns.spec.instance_ref.name,
                ns.name_any(),
                e
            );
            error!("{}", err_msg);
            update_status(
                &db_resource,
                client,
                &db_namespace,
                false,
                None,
                None,
                Some(err_msg),
            )
            .await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };

    // 4. Resolve credentials from the Instance
    let endpoint = match resolve_value(
        client,
        &instance.spec.connection_string,
        &resolved_instance_ns,
    )
    .await
    {
        Ok(endpoint) => endpoint,
        Err(e) => {
            let err_msg = format!(
                "Failed to resolve connectionString for Instance {}: {}",
                instance.name_any(),
                e
            );
            error!("{}", err_msg);
            update_status(
                &db_resource,
                client,
                &db_namespace,
                false,
                None,
                None,
                Some(err_msg),
            )
            .await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };

    let username = match resolve_value(client, &instance.spec.username, &resolved_instance_ns).await
    {
        Ok(user) => user,
        Err(e) => {
            let err_msg = format!(
                "Failed to resolve username for Instance {}: {}",
                instance.name_any(),
                e
            );
            error!("{}", err_msg);
            update_status(
                &db_resource,
                client,
                &db_namespace,
                false,
                None,
                None,
                Some(err_msg),
            )
            .await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };

    let password = match resolve_value(client, &instance.spec.password, &resolved_instance_ns).await
    {
        Ok(pass) => pass,
        Err(e) => {
            let err_msg = format!(
                "Failed to resolve password for Instance {}: {}",
                instance.name_any(),
                e
            );
            error!("{}", err_msg);
            update_status(
                &db_resource,
                client,
                &db_namespace,
                false,
                None,
                None,
                Some(err_msg),
            )
            .await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };

    // 5. Connect to SurrealDB and ensure Database exists
    let ns_name = ns.name_any();
    match connect_instance(&endpoint, &username, &password).await {
        Ok(db) => {
            // In SurrealQL, we must switch namespaces before creating a database
            let query_str = format!("USE NS `{}`; DEFINE DB `{}`;", ns_name, db_name);
            if let Err(e) = db.query(&query_str).await {
                let err_msg = format!("Failed to define database in SurrealDB: {}", e);
                error!("{}", err_msg);
                update_status(
                    &db_resource,
                    client,
                    &db_namespace,
                    false,
                    None,
                    None,
                    Some(err_msg),
                )
                .await?;
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
            info!(
                "Successfully ensured database {}/{} exists in SurrealDB",
                ns_name, db_name
            );

            // Retain the existing applied schema fields in status to avoid overwriting them
            let current_status = db_resource.status.clone().unwrap_or_default();
            update_status(
                &db_resource,
                client,
                &db_namespace,
                true,
                current_status.applied_schema_hash,
                current_status.applied_schema_generation,
                None,
            )
            .await?;
        }
        Err(e) => {
            let err_msg = format!(
                "Failed to connect to SurrealDB endpoint {}: {}",
                endpoint, e
            );
            error!("{}", err_msg);
            let current_status = db_resource.status.clone().unwrap_or_default();
            update_status(
                &db_resource,
                client,
                &db_namespace,
                false,
                current_status.applied_schema_hash,
                current_status.applied_schema_generation,
                Some(err_msg),
            )
            .await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    }

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// Error handler for Database reconciler.
pub fn error_policy(_db: Arc<Database>, error: &Error, _ctx: Arc<Context>) -> Action {
    error!("Database reconciliation failed: {:?}", error);
    Action::requeue(Duration::from_secs(15))
}

/// Updates the status subresource of the Database resource.
async fn update_status(
    db: &Database,
    client: &Client,
    namespace: &str,
    created: bool,
    applied_schema_hash: Option<String>,
    applied_schema_generation: Option<i64>,
    error_msg: Option<String>,
) -> Result<()> {
    let api: Api<Database> = Api::namespaced(client.clone(), namespace);
    let patch = json!({
        "status": DatabaseStatus {
            created,
            applied_schema_hash,
            applied_schema_generation,
            error: error_msg,
            observed_generation: db.metadata.generation,
        }
    });

    api.patch_status(
        &db.name_any(),
        &kube::api::PatchParams::default(),
        &kube::api::Patch::Merge(&patch),
    )
    .await
    .map_err(Error::KubeError)?;

    Ok(())
}
