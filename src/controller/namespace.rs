use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Api, Client, Resource, ResourceExt};
use serde_json::json;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{error, info};

use crate::controller::utils::{resolve_namespace, resolve_value};
use crate::controller::{Context, Error, Result};
use crate::crd::{Instance, Namespace, NamespaceStatus};
use crate::surreal::connect_instance;

/// Reconciles a Namespace resource.
pub async fn reconcile(ns: Arc<Namespace>, ctx: Arc<Context>) -> Result<Action> {
    let client = &ctx.client;
    let ns_name = ns.name_any();
    let ns_namespace = ns
        .namespace()
        .ok_or_else(|| Error::InternalError("Namespace is cluster-scoped".to_string()))?;

    info!("Reconciling Namespace {}/{}", ns_namespace, ns_name);

    let resolved_instance_ns = resolve_namespace(&ns.spec.instance_ref, &ns_namespace);
    let instance_api: Api<Instance> = Api::namespaced(client.clone(), &resolved_instance_ns);

    // 1. Fetch the referenced Instance
    let instance = match instance_api.get(&ns.spec.instance_ref.name).await {
        Ok(inst) => inst,
        Err(e) => {
            let err_msg = format!(
                "Failed to get Instance {}: {}",
                ns.spec.instance_ref.name, e
            );
            error!("{}", err_msg);
            update_status(&ns, client, &ns_namespace, false, Some(err_msg)).await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };

    // 1b. Check if the referenced Instance is connected/ready
    let is_connected = instance
        .status
        .as_ref()
        .map(|s| s.connected)
        .unwrap_or(false);
    if !is_connected {
        let err_msg = match instance.status.as_ref().and_then(|s| s.error.clone()) {
            Some(err) => format!(
                "Referenced Instance {} is unhealthy: {}",
                instance.name_any(),
                err
            ),
            None => format!(
                "Referenced Instance {} is not connected yet",
                instance.name_any()
            ),
        };
        error!("{}", err_msg);
        update_status(&ns, client, &ns_namespace, false, Some(err_msg)).await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    // 1c. Ensure Namespace has the correct OwnerReference to Instance
    let has_owner = ns
        .metadata
        .owner_references
        .as_ref()
        .map(|refs| {
            refs.iter()
                .any(|r| r.uid == instance.metadata.uid.as_ref().cloned().unwrap_or_default())
        })
        .unwrap_or(false);
    if !has_owner {
        if let Some(owner_ref) = instance.controller_owner_ref(&()) {
            let api: Api<Namespace> = Api::namespaced(client.clone(), &ns_namespace);
            let patch = json!({
                "metadata": {
                    "ownerReferences": [owner_ref]
                }
            });
            api.patch(
                &ns.name_any(),
                &PatchParams::default(),
                &Patch::Merge(&patch),
            )
            .await
            .map_err(Error::KubeError)?;
        }
    }

    // 2. Resolve credentials from the Instance
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
            update_status(&ns, client, &ns_namespace, false, Some(err_msg)).await?;
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
            update_status(&ns, client, &ns_namespace, false, Some(err_msg)).await?;
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
            update_status(&ns, client, &ns_namespace, false, Some(err_msg)).await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };

    // 3. Connect to SurrealDB and ensure Namespace exists
    match connect_instance(&endpoint, &username, &password).await {
        Ok(db) => {
            let query_str = format!("DEFINE NS `{}`;", ns_name);
            if let Err(e) = db.query(&query_str).await {
                let err_msg = format!("Failed to define namespace in SurrealDB: {}", e);
                error!("{}", err_msg);
                update_status(&ns, client, &ns_namespace, false, Some(err_msg)).await?;
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
            info!(
                "Successfully ensured namespace {} exists in SurrealDB",
                ns_name
            );
            update_status(&ns, client, &ns_namespace, true, None).await?;
        }
        Err(e) => {
            let err_msg = format!(
                "Failed to connect to SurrealDB endpoint {}: {}",
                endpoint, e
            );
            error!("{}", err_msg);
            update_status(&ns, client, &ns_namespace, false, Some(err_msg)).await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    }

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// Error handler for Namespace reconciler.
pub fn error_policy(_ns: Arc<Namespace>, error: &Error, _ctx: Arc<Context>) -> Action {
    error!("Reconciliation failed: {:?}", error);
    Action::requeue(Duration::from_secs(15))
}

/// Updates the status subresource of the Namespace resource.
async fn update_status(
    ns: &Namespace,
    client: &Client,
    namespace: &str,
    created: bool,
    error_msg: Option<String>,
) -> Result<()> {
    let api: Api<Namespace> = Api::namespaced(client.clone(), namespace);
    let patch = json!({
        "status": NamespaceStatus {
            created,
            error: error_msg,
            observed_generation: ns.metadata.generation,
        }
    });

    api.patch_status(
        &ns.name_any(),
        &kube::api::PatchParams::default(),
        &kube::api::Patch::Merge(&patch),
    )
    .await
    .map_err(Error::KubeError)?;

    Ok(())
}
