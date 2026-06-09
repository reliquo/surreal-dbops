use kube::runtime::controller::Action;
use kube::{Api, Client, ResourceExt};
use serde_json::json;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{error, info};

use crate::controller::utils::resolve_value;
use crate::controller::{Context, Error, Result};
use crate::crd::{Instance, InstanceStatus};
use crate::surreal::connect_instance;

/// Reconciles an Instance resource.
pub async fn reconcile(instance: Arc<Instance>, ctx: Arc<Context>) -> Result<Action> {
    let client = &ctx.client;
    let name = instance.name_any();
    let namespace = instance
        .namespace()
        .ok_or_else(|| Error::InternalError("Instance is cluster-scoped".to_string()))?;

    info!("Reconciling Instance {}/{}", namespace, name);

    // 1. Resolve credentials
    let endpoint = match resolve_value(client, &instance.spec.connection_string, &namespace).await {
        Ok(ep) => ep,
        Err(e) => {
            let err_msg = format!("Failed to resolve connectionString: {}", e);
            update_status(&instance, client, &namespace, false, Some(err_msg)).await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };

    let username = match resolve_value(client, &instance.spec.username, &namespace).await {
        Ok(user) => user,
        Err(e) => {
            let err_msg = format!("Failed to resolve username: {}", e);
            update_status(&instance, client, &namespace, false, Some(err_msg)).await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };

    let password = match resolve_value(client, &instance.spec.password, &namespace).await {
        Ok(pass) => pass,
        Err(e) => {
            let err_msg = format!("Failed to resolve password: {}", e);
            update_status(&instance, client, &namespace, false, Some(err_msg)).await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };

    // 2. Validate connection
    match connect_instance(&endpoint, &username, &password).await {
        Ok(_) => {
            info!(
                "Successfully validated connection to SurrealDB Instance {}",
                name
            );
            update_status(&instance, client, &namespace, true, None).await?;
        }
        Err(e) => {
            let err_msg = format!("Failed to connect to SurrealDB: {}", e);
            error!("{}", err_msg);
            update_status(&instance, client, &namespace, false, Some(err_msg)).await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    }

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// Error handler for Instance reconciler.
pub fn error_policy(_instance: Arc<Instance>, error: &Error, _ctx: Arc<Context>) -> Action {
    error!("Instance reconciliation failed: {:?}", error);
    Action::requeue(Duration::from_secs(15))
}

/// Updates the status subresource of the Instance resource.
async fn update_status(
    instance: &Instance,
    client: &Client,
    namespace: &str,
    connected: bool,
    error_msg: Option<String>,
) -> Result<()> {
    let api: Api<Instance> = Api::namespaced(client.clone(), namespace);
    let patch = json!({
        "status": InstanceStatus {
            connected,
            error: error_msg,
            observed_generation: instance.metadata.generation,
        }
    });

    api.patch_status(
        &instance.name_any(),
        &kube::api::PatchParams::default(),
        &kube::api::Patch::Merge(&patch),
    )
    .await
    .map_err(Error::KubeError)?;

    Ok(())
}
