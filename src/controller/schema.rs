use std::sync::Arc;
use kube::{Api, Client, Resource, ResourceExt};
use kube::api::{PostParams, DeleteParams, ListParams};
use kube::runtime::controller::Action;
use serde_json::json;
use tokio::time::Duration;
use tracing::{info, error};
use sha2::{Sha256, Digest};
use std::collections::BTreeMap;

use crate::crd::{Schema, Rollout, RolloutSpec, SchemaStatus, LocalObjectReference};
use crate::controller::{Context, Error, Result};

/// Reconciles a Schema resource.
pub async fn reconcile(schema: Arc<Schema>, ctx: Arc<Context>) -> Result<Action> {
    let client = &ctx.client;
    let schema_name = schema.name_any();
    let schema_namespace = schema.namespace().ok_or_else(|| Error::InternalError("Schema is cluster-scoped".to_string()))?;

    info!("Reconciling Schema {}/{}", schema_namespace, schema_name);

    // 1. Resolve raw schema content and interpolate variables
    let interpolated_schema = match crate::controller::utils::resolve_and_interpolate_schema(client, &schema, &schema_namespace).await {
        Ok(text) => text,
        Err(e) => {
            let err_msg = format!("Failed to resolve and interpolate schema content: {}", e);
            error!("{}", err_msg);
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };

    // 3. Compute version hash
    let mut hasher = Sha256::new();
    hasher.update(interpolated_schema.as_bytes());
    let schema_hash = format!("sha256:{:x}", hasher.finalize());
    info!("Computed schema hash for {}: {}", schema_name, schema_hash);

    let generation = schema.metadata.generation.unwrap_or(1);
    let rollout_name = format!("{}-rollout-{}", schema_name, generation);

    // 4. Ensure Rollout exists for this generation
    let rollout_api: Api<Rollout> = Api::namespaced(client.clone(), &schema_namespace);
    match rollout_api.get(&rollout_name).await {
        Ok(_) => {
            info!("Rollout {} already exists", rollout_name);
        }
        Err(kube::Error::Api(ref response)) if response.code == 404 => {
            info!("Creating new Rollout {}", rollout_name);
            let mut rollout = Rollout::new(
                &rollout_name,
                RolloutSpec {
                    schema_ref: LocalObjectReference {
                        name: schema_name.clone(),
                        namespace: Some(schema_namespace.clone()),
                    },
                    generation,
                },
            );
            
            // Add label so we can list rollouts for this schema
            rollout.metadata.labels = Some(BTreeMap::from([
                ("schema".to_string(), schema_name.clone()),
            ]));

            // Set OwnerReference to clean up when Schema is deleted
            if let Some(owner_ref) = schema.controller_owner_ref(&()) {
                rollout.metadata.owner_references = Some(vec![owner_ref]);
            }

            rollout_api.create(&PostParams::default(), &rollout).await
                .map_err(|e| Error::KubeError(e))?;
        }
        Err(e) => {
            return Err(Error::KubeError(e));
        }
    }

    // 5. Prune old historical rollouts
    let history_limit = schema.spec.revision_history_limit.unwrap_or(10);
    prune_rollout_history(&rollout_api, &schema_name, history_limit).await?;

    // 6. Update Schema status
    update_status(&schema, client, &schema_namespace, Some(schema_hash), Some(rollout_name)).await?;

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// Error handler for Schema reconciler.
pub fn error_policy(_schema: Arc<Schema>, error: &Error, _ctx: Arc<Context>) -> Action {
    error!("Schema reconciliation failed: {:?}", error);
    Action::requeue(Duration::from_secs(15))
}

/// Prunes completed/failed rollout resources exceeding history limit.
async fn prune_rollout_history(
    rollout_api: &Api<Rollout>,
    schema_name: &str,
    limit: usize,
) -> Result<()> {
    let lp = ListParams::default().labels(&format!("schema={}", schema_name));
    let rollouts = rollout_api.list(&lp).await.map_err(Error::KubeError)?;

    // Filter to rollouts that are completed or failed (terminal state)
    let mut terminal_rollouts: Vec<Rollout> = rollouts.items.into_iter()
        .filter(|r| {
            if let Some(ref status) = r.status {
                if let Some(ref phase) = status.phase {
                    return phase == "Completed" || phase == "Failed";
                }
            }
            false
        })
        .collect();

    // Sort by creation time (oldest first)
    terminal_rollouts.sort_by(|a, b| {
        a.metadata.creation_timestamp.cmp(&b.metadata.creation_timestamp)
    });

    if terminal_rollouts.len() > limit {
        let prune_count = terminal_rollouts.len() - limit;
        info!("Pruning {} old rollouts for schema {}", prune_count, schema_name);
        for r in terminal_rollouts.iter().take(prune_count) {
            let name = r.name_any();
            info!("Deleting historical rollout {}", name);
            let _ = rollout_api.delete(&name, &DeleteParams::default()).await;
        }
    }

    Ok(())
}

/// Updates the status subresource of the Schema resource.
async fn update_status(
    schema: &Schema,
    client: &Client,
    namespace: &str,
    hash: Option<String>,
    rollout_name: Option<String>,
) -> Result<()> {
    let api: Api<Schema> = Api::namespaced(client.clone(), namespace);
    let patch = json!({
        "status": SchemaStatus {
            current_version_hash: hash,
            active_rollout_name: rollout_name,
            observed_generation: schema.metadata.generation,
        }
    });

    api.patch_status(&schema.name_any(), &kube::api::PatchParams::default(), &kube::api::Patch::Merge(&patch))
        .await
        .map_err(Error::KubeError)?;

    Ok(())
}
