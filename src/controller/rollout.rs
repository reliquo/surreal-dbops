use chrono::Utc;
use futures::StreamExt;
use kube::api::{ListParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Api, Client, Resource, ResourceExt};
use serde_json::json;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{error, info, warn};

use crate::controller::utils::{resolve_and_interpolate_schema, resolve_namespace, resolve_value};
use crate::controller::{Context, Error, Result};
use crate::crd::{
    ApprovalPolicy, Condition, Database, Instance, Namespace, Rollout, RolloutStatus, Schema,
};
use crate::surreal::{compute_diff, connect_instance};

/// Reconciles a Rollout resource.
pub async fn reconcile(rollout: Arc<Rollout>, ctx: Arc<Context>) -> Result<Action> {
    let client = &ctx.client;
    let rollout_name = rollout.name_any();
    let rollout_namespace = rollout
        .namespace()
        .ok_or_else(|| Error::InternalError("Rollout is cluster-scoped".to_string()))?;

    info!("Reconciling Rollout {}/{}", rollout_namespace, rollout_name);

    // 0. Skip reconciliation if rollout is already in a terminal state
    if let Some(ref status) = rollout.status {
        if status.phase == Some("Completed".to_string())
            || status.phase == Some("Failed".to_string())
        {
            info!(
                "Rollout {}/{} is already in terminal state ({:?}), skipping reconciliation",
                rollout_namespace, rollout_name, status.phase
            );
            return Ok(Action::await_change());
        }
    }

    // 1. Fetch the target Schema
    let resolved_schema_ns = resolve_namespace(&rollout.spec.schema_ref, &rollout_namespace);
    let schema_api: Api<Schema> = Api::namespaced(client.clone(), &resolved_schema_ns);
    let schema = match schema_api.get(&rollout.spec.schema_ref.name).await {
        Ok(s) => s,
        Err(e) => {
            let err_msg = format!(
                "Failed to fetch Schema {}: {}",
                rollout.spec.schema_ref.name, e
            );
            error!("{}", err_msg);
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };

    // 2. Resolve fully rendered desired schema
    let desired_schema_text =
        match resolve_and_interpolate_schema(client, &schema, &resolved_schema_ns).await {
            Ok(text) => text,
            Err(e) => {
                let err_msg = format!("Failed to resolve and interpolate schema text: {}", e);
                error!("{}", err_msg);
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
        };

    // 3. Find all Databases referencing this Schema
    let db_api: Api<Database> = Api::all(client.clone());
    let all_databases = db_api
        .list(&ListParams::default())
        .await
        .map_err(Error::KubeError)?;

    let mut affected_databases = Vec::new();
    for db in all_databases.items {
        let db_ns = db.namespace().unwrap_or_default();
        let ref_schema_ns = resolve_namespace(&db.spec.schema_ref, &db_ns);
        if db.spec.schema_ref.name == schema.name_any() && ref_schema_ns == resolved_schema_ns {
            affected_databases.push(db);
        }
    }

    let affected_count = affected_databases.len();
    info!(
        "Found {} databases affected by rollout {}",
        affected_count, rollout_name
    );

    // 3a. Ensure Rollout has the correct OwnerReferences (Schema as controller, Databases as non-controllers)
    let mut expected_owners = Vec::new();
    if let Some(schema_owner) = schema.controller_owner_ref(&()) {
        expected_owners.push(schema_owner);
    }
    for db in &affected_databases {
        if let Some(mut db_owner) = db.controller_owner_ref(&()) {
            db_owner.controller = Some(false);
            db_owner.block_owner_deletion = Some(true);
            expected_owners.push(db_owner);
        }
    }
    let current_owners = rollout
        .metadata
        .owner_references
        .clone()
        .unwrap_or_default();
    let mut owners_changed = current_owners.len() != expected_owners.len();
    if !owners_changed {
        for expected in &expected_owners {
            if !current_owners.iter().any(|c| c.uid == expected.uid) {
                owners_changed = true;
                break;
            }
        }
    }
    if owners_changed {
        let api: Api<Rollout> = Api::namespaced(client.clone(), &rollout_namespace);
        let patch = json!({
            "metadata": {
                "ownerReferences": expected_owners
            }
        });
        api.patch(
            &rollout_name,
            &PatchParams::default(),
            &Patch::Merge(&patch),
        )
        .await
        .map_err(Error::KubeError)?;
    }

    // Get current status or start clean
    let mut status = rollout.status.clone().unwrap_or_default();
    status.affected_databases = affected_count;

    // 3b. Verify health of all affected databases before proceeding. If any are not ready yet, requeue to wait.
    for target_db in &affected_databases {
        let db_ns = target_db.namespace().unwrap_or_default();
        if let Err(e) = check_db_health(client, target_db, &db_ns).await {
            let err_msg = format!("Rollout is waiting for dependent resources: {}", e);
            info!("{}", err_msg);

            status.phase = Some("Blocked".to_string());
            update_condition(
                &mut status.conditions,
                Condition {
                    r#type: "Progressing".to_string(),
                    status: "False".to_string(),
                    last_transition_time: Utc::now().to_rfc3339(),
                    reason: "DependencyUnhealthy".to_string(),
                    message: err_msg,
                },
            );
            patch_status(
                &rollout_name,
                &rollout_namespace,
                client,
                &rollout.status,
                &status,
            )
            .await?;

            return Ok(Action::requeue(Duration::from_secs(10)));
        }
    }

    if status.phase.is_none() {
        status.phase = Some("Progressing".to_string());
    }

    // 4. Compute Schema Diff using the first database (if any exist and are ready)
    let mut diff_statements = Vec::new();
    let mut destructive = false;

    if let Some(target_db) = affected_databases.first() {
        let db_ns = target_db.namespace().unwrap_or_default();
        match get_db_client(client, target_db, &db_ns).await {
            Ok(db_client) => {
                // Connect to the specific DB
                let ns_ref_ns = resolve_namespace(&target_db.spec.namespace_ref, &db_ns);
                let ns_api: Api<Namespace> = Api::namespaced(client.clone(), &ns_ref_ns);
                if let Ok(ns) = ns_api.get(&target_db.spec.namespace_ref.name).await {
                    if let Err(e) = db_client
                        .use_ns(ns.name_any())
                        .use_db(target_db.name_any())
                        .await
                    {
                        warn!(
                            "Failed to use namespace {} and database {}: {}",
                            ns.name_any(),
                            target_db.name_any(),
                            e
                        );
                    }

                    match compute_diff(&db_client, &desired_schema_text).await {
                        Ok((statements, is_destructive)) => {
                            diff_statements = statements;
                            destructive = is_destructive;
                        }
                        Err(e) => {
                            warn!(
                                "Failed to compute diff against database {}: {}",
                                target_db.name_any(),
                                e
                            );
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Failed to connect to database {} to calculate diff: {}",
                    target_db.name_any(),
                    e
                );
            }
        }
    }

    status.diff = Some(diff_statements.join("\n"));
    status.destructive = destructive;

    // 5. Evaluate Approval Policy
    let policy = schema
        .spec
        .require_approval
        .unwrap_or(ApprovalPolicy::Destructive);
    let approval_required = match policy {
        ApprovalPolicy::None => false,
        ApprovalPolicy::All => true,
        ApprovalPolicy::Destructive => destructive,
    };

    // Read Mutating webhook/annotations approval values
    let is_annotated_approved = rollout
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("database.reliquo.io/approved"))
        .map(|val| val == "true")
        .unwrap_or(false);

    if is_annotated_approved {
        status.approved = true;
        if let Some(ref annotations) = rollout.metadata.annotations {
            if let Some(approver) = annotations.get("database.reliquo.io/approved-by") {
                status.approved_by = Some(approver.clone());
            }
            if let Some(time) = annotations.get("database.reliquo.io/approved-at") {
                status.approved_at = Some(time.clone());
            }
        }
    }

    if approval_required && !status.approved {
        status.phase = Some("Blocked".to_string());
        update_condition(
            &mut status.conditions,
            Condition {
                r#type: "Approved".to_string(),
                status: "False".to_string(),
                last_transition_time: Utc::now().to_rfc3339(),
                reason: "PendingApproval".to_string(),
                message: "Destructive changes require manual approval.".to_string(),
            },
        );
        update_condition(
            &mut status.conditions,
            Condition {
                r#type: "Progressing".to_string(),
                status: "False".to_string(),
                last_transition_time: Utc::now().to_rfc3339(),
                reason: "Blocked".to_string(),
                message: "Rollout blocked waiting for approval annotation.".to_string(),
            },
        );

        patch_status(
            &rollout_name,
            &rollout_namespace,
            client,
            &rollout.status,
            &status,
        )
        .await?;
        info!("Rollout {} is Blocked waiting for approval", rollout_name);
        return Ok(Action::requeue(Duration::from_secs(15)));
    }

    // 6. Roll out changes
    update_condition(
        &mut status.conditions,
        Condition {
            r#type: "Approved".to_string(),
            status: "True".to_string(),
            last_transition_time: Utc::now().to_rfc3339(),
            reason: "ApprovedOrSafe".to_string(),
            message: "Approval resolved.".to_string(),
        },
    );
    update_condition(
        &mut status.conditions,
        Condition {
            r#type: "Progressing".to_string(),
            status: "True".to_string(),
            last_transition_time: Utc::now().to_rfc3339(),
            reason: "ApplyingSchema".to_string(),
            message: format!("Applying schema to {} databases.", affected_count),
        },
    );
    status.phase = Some("Progressing".to_string());
    patch_status(
        &rollout_name,
        &rollout_namespace,
        client,
        &rollout.status,
        &status,
    )
    .await?;

    let concurrency = schema.spec.concurrency_limit.unwrap_or(50);

    // We filter out databases that have already applied this generation of the schema
    let pending_databases: Vec<Database> = affected_databases
        .into_iter()
        .filter(|db| {
            if let Some(ref db_status) = db.status {
                if let Some(gen) = db_status.applied_schema_generation {
                    if gen == rollout.spec.generation {
                        return false;
                    }
                }
            }
            true
        })
        .collect();

    let completed_count = affected_count - pending_databases.len();
    status.applied_databases = completed_count;
    patch_status(
        &rollout_name,
        &rollout_namespace,
        client,
        &rollout.status,
        &status,
    )
    .await?;

    // Create a stream to process pending migrations concurrently
    let db_client_cloned = client.clone();
    let rollout_spec_gen = rollout.spec.generation;
    let schema_hash = schema
        .status
        .as_ref()
        .and_then(|s| s.current_version_hash.clone())
        .unwrap_or_default();

    let mut stream = futures::stream::iter(pending_databases)
        .map(|db| {
            let client = db_client_cloned.clone();
            let query = diff_statements.join("\n");
            let gen = rollout_spec_gen;
            let hash = schema_hash.clone();
            async move {
                let db_ns = db.namespace().unwrap_or_default();
                let db_name = db.name_any();
                match apply_schema_to_db(&client, &db, &db_ns, &query, gen, &hash).await {
                    Ok(_) => {
                        info!(
                            "Successfully rolled out schema to database {}/{}",
                            db_ns, db_name
                        );
                        Ok(())
                    }
                    Err(e) => {
                        error!(
                            "Failed to roll out schema to database {}/{}: {}",
                            db_ns, db_name, e
                        );
                        Err(e)
                    }
                }
            }
        })
        .buffer_unordered(concurrency);

    while let Some(res) = stream.next().await {
        match res {
            Ok(_) => {
                status.applied_databases += 1;
            }
            Err(_) => {
                status.failed_databases += 1;
            }
        }
        // Patch status progressively
        let _ = patch_status(
            &rollout_name,
            &rollout_namespace,
            client,
            &rollout.status,
            &status,
        )
        .await;
    }

    // 7. Check Completion
    if status.applied_databases + status.failed_databases == status.affected_databases {
        if status.failed_databases > 0 {
            status.phase = Some("Failed".to_string());
            update_condition(
                &mut status.conditions,
                Condition {
                    r#type: "Ready".to_string(),
                    status: "False".to_string(),
                    last_transition_time: Utc::now().to_rfc3339(),
                    reason: "MigrationFailed".to_string(),
                    message: format!(
                        "Rollout completed with {} failures.",
                        status.failed_databases
                    ),
                },
            );
        } else {
            status.phase = Some("Completed".to_string());
            status.completed_at = Some(Utc::now().to_rfc3339());
            update_condition(
                &mut status.conditions,
                Condition {
                    r#type: "Ready".to_string(),
                    status: "True".to_string(),
                    last_transition_time: Utc::now().to_rfc3339(),
                    reason: "RolloutCompleted".to_string(),
                    message: "All databases successfully migrated.".to_string(),
                },
            );
        }
        update_condition(
            &mut status.conditions,
            Condition {
                r#type: "Progressing".to_string(),
                status: "False".to_string(),
                last_transition_time: Utc::now().to_rfc3339(),
                reason: "Finished".to_string(),
                message: "Migration completed.".to_string(),
            },
        );
        patch_status(
            &rollout_name,
            &rollout_namespace,
            client,
            &rollout.status,
            &status,
        )
        .await?;
    }

    Ok(Action::await_change())
}

/// Error handler for Rollout reconciler.
pub fn error_policy(_rollout: Arc<Rollout>, error: &Error, _ctx: Arc<Context>) -> Action {
    error!("Rollout reconciliation failed: {:?}", error);
    Action::requeue(Duration::from_secs(15))
}

/// Helper function to verify health of a target database and its dependency chain (Namespace -> Instance)
async fn check_db_health(client: &Client, db: &Database, db_namespace: &str) -> Result<()> {
    // Check if the Database is ready
    if let Some(ref status) = db.status {
        if !status.created {
            let err_msg = status
                .error
                .clone()
                .unwrap_or_else(|| "Database is not ready".to_string());
            return Err(Error::InternalError(format!(
                "Database {} is unhealthy: {}",
                db.name_any(),
                err_msg
            )));
        }
    } else {
        return Err(Error::InternalError(format!(
            "Database {} has no status yet",
            db.name_any()
        )));
    }

    // Fetch and check Namespace
    let resolved_ns_namespace = resolve_namespace(&db.spec.namespace_ref, db_namespace);
    let ns_api: Api<Namespace> = Api::namespaced(client.clone(), &resolved_ns_namespace);
    let ns = ns_api
        .get(&db.spec.namespace_ref.name)
        .await
        .map_err(Error::KubeError)?;
    if let Some(ref status) = ns.status {
        if !status.created {
            let err_msg = status
                .error
                .clone()
                .unwrap_or_else(|| "Namespace is not ready".to_string());
            return Err(Error::InternalError(format!(
                "Namespace {} is unhealthy: {}",
                ns.name_any(),
                err_msg
            )));
        }
    } else {
        return Err(Error::InternalError(format!(
            "Namespace {} has no status yet",
            ns.name_any()
        )));
    }

    // Fetch and check Instance
    let resolved_instance_ns = resolve_namespace(&ns.spec.instance_ref, &resolved_ns_namespace);
    let instance_api: Api<Instance> = Api::namespaced(client.clone(), &resolved_instance_ns);
    let instance = instance_api
        .get(&ns.spec.instance_ref.name)
        .await
        .map_err(Error::KubeError)?;
    if let Some(ref status) = instance.status {
        if !status.connected {
            let err_msg = status
                .error
                .clone()
                .unwrap_or_else(|| "Instance is not connected".to_string());
            return Err(Error::InternalError(format!(
                "Instance {} is unhealthy: {}",
                instance.name_any(),
                err_msg
            )));
        }
    } else {
        return Err(Error::InternalError(format!(
            "Instance {} has no status yet",
            instance.name_any()
        )));
    }

    Ok(())
}

/// Helper function to establish connection to a specific database
async fn get_db_client(
    client: &Client,
    db: &Database,
    db_namespace: &str,
) -> Result<surrealdb::Surreal<surrealdb::engine::any::Any>> {
    let resolved_ns_namespace = resolve_namespace(&db.spec.namespace_ref, db_namespace);
    let ns_api: Api<Namespace> = Api::namespaced(client.clone(), &resolved_ns_namespace);
    let ns = ns_api
        .get(&db.spec.namespace_ref.name)
        .await
        .map_err(Error::KubeError)?;

    let resolved_instance_ns = resolve_namespace(&ns.spec.instance_ref, &resolved_ns_namespace);
    let instance_api: Api<Instance> = Api::namespaced(client.clone(), &resolved_instance_ns);
    let instance = instance_api
        .get(&ns.spec.instance_ref.name)
        .await
        .map_err(Error::KubeError)?;

    // Check if the Database is ready
    if let Some(ref status) = db.status {
        if !status.created {
            let err_msg = status
                .error
                .clone()
                .unwrap_or_else(|| "Database is not ready".to_string());
            return Err(Error::InternalError(format!(
                "Database {} is unhealthy: {}",
                db.name_any(),
                err_msg
            )));
        }
    } else {
        return Err(Error::InternalError(format!(
            "Database {} has no status yet",
            db.name_any()
        )));
    }

    // Check if the Namespace is ready
    if let Some(ref status) = ns.status {
        if !status.created {
            let err_msg = status
                .error
                .clone()
                .unwrap_or_else(|| "Namespace is not ready".to_string());
            return Err(Error::InternalError(format!(
                "Namespace {} is unhealthy: {}",
                ns.name_any(),
                err_msg
            )));
        }
    } else {
        return Err(Error::InternalError(format!(
            "Namespace {} has no status yet",
            ns.name_any()
        )));
    }

    // Check if the Instance is connected/ready
    if let Some(ref status) = instance.status {
        if !status.connected {
            let err_msg = status
                .error
                .clone()
                .unwrap_or_else(|| "Instance is not connected".to_string());
            return Err(Error::InternalError(format!(
                "Instance {} is unhealthy: {}",
                instance.name_any(),
                err_msg
            )));
        }
    } else {
        return Err(Error::InternalError(format!(
            "Instance {} has no status yet",
            instance.name_any()
        )));
    }

    let endpoint = resolve_value(
        client,
        &instance.spec.connection_string,
        &resolved_instance_ns,
    )
    .await?;
    let username = resolve_value(client, &instance.spec.username, &resolved_instance_ns).await?;
    let password = resolve_value(client, &instance.spec.password, &resolved_instance_ns).await?;

    connect_instance(&endpoint, &username, &password)
        .await
        .map_err(|e| Error::SurrealError(e.to_string()))
}

/// Connects to a target database, runs the diff statements in a transaction, and updates K8s Database status.
async fn apply_schema_to_db(
    client: &Client,
    db_resource: &Database,
    db_namespace: &str,
    query: &str,
    generation: i64,
    hash: &str,
) -> Result<()> {
    let db_client = get_db_client(client, db_resource, db_namespace).await?;
    let ns_ref_ns = resolve_namespace(&db_resource.spec.namespace_ref, db_namespace);
    let ns_api: Api<Namespace> = Api::namespaced(client.clone(), &ns_ref_ns);
    let ns = ns_api
        .get(&db_resource.spec.namespace_ref.name)
        .await
        .map_err(Error::KubeError)?;

    // Switch namespace and database
    db_client
        .use_ns(ns.name_any())
        .use_db(db_resource.name_any())
        .await
        .map_err(|e| Error::SurrealError(format!("Failed to select NS/DB: {}", e)))?;

    // Execute within transaction
    let transaction_query = format!("BEGIN TRANSACTION;\n{}\nCOMMIT TRANSACTION;", query);
    let response = db_client.query(&transaction_query).await.map_err(|e| {
        Error::SurrealError(format!("Failed to apply schema transaction query: {}", e))
    })?;
    response
        .check()
        .map_err(|e| Error::SurrealError(format!("Failed to apply schema transaction: {}", e)))?;

    // Update Database status
    let db_api: Api<Database> = Api::namespaced(client.clone(), db_namespace);
    let patch = json!({
        "status": {
            "appliedSchemaHash": hash,
            "appliedSchemaGeneration": generation,
            "error": null,
        }
    });
    db_api
        .patch_status(
            &db_resource.name_any(),
            &PatchParams::default(),
            &Patch::Merge(&patch),
        )
        .await
        .map_err(Error::KubeError)?;

    Ok(())
}

/// Helper function to patch status of Rollout resource only if it has changed.
async fn patch_status(
    name: &str,
    namespace: &str,
    client: &Client,
    current_status: &Option<RolloutStatus>,
    new_status: &RolloutStatus,
) -> Result<()> {
    if Some(new_status) == current_status.as_ref() {
        return Ok(());
    }
    let api: Api<Rollout> = Api::namespaced(client.clone(), namespace);
    let patch = json!({
        "status": new_status
    });
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .map_err(Error::KubeError)?;
    Ok(())
}

/// Helper to upsert a condition in the condition list without updating the last transition time if status/reason/message didn't change.
fn update_condition(conditions: &mut Vec<Condition>, mut new_cond: Condition) {
    if let Some(pos) = conditions.iter().position(|c| c.r#type == new_cond.r#type) {
        let old_cond = &conditions[pos];
        if old_cond.status == new_cond.status
            && old_cond.reason == new_cond.reason
            && old_cond.message == new_cond.message
        {
            new_cond.last_transition_time = old_cond.last_transition_time.clone();
        }
        conditions[pos] = new_cond;
    } else {
        conditions.push(new_cond);
    }
}
