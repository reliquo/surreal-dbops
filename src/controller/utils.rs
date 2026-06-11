use crate::controller::{Error, Result};
use crate::crd::{LocalObjectReference, Schema, ValueOrRefSource};
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::{Api, Client};

/// Resolves a LocalObjectReference, defaulting the namespace if not specified.
pub fn resolve_namespace(reference: &LocalObjectReference, resource_ns: &str) -> String {
    reference
        .namespace
        .clone()
        .unwrap_or_else(|| resource_ns.to_string())
}

/// Resolves a ValueOrRefSource into a raw string by reading from Kubernetes Secrets or ConfigMaps if referenced.
pub async fn resolve_value(
    client: &Client,
    source: &ValueOrRefSource,
    resource_ns: &str,
) -> Result<String> {
    if let Some(ref val) = source.value {
        return Ok(val.clone());
    }

    if let Some(ref from) = source.value_from {
        if let Some(ref secret_ref) = from.secret_key_ref {
            let secrets_api: Api<Secret> = Api::namespaced(client.clone(), resource_ns);
            let secret = secrets_api.get(&secret_ref.name).await.map_err(|e| {
                Error::ConfigError(format!("Failed to read secret {}: {}", secret_ref.name, e))
            })?;

            if let Some(ref data) = secret.data {
                if let Some(bytes) = data.get(&secret_ref.key) {
                    let val = String::from_utf8(bytes.0.clone()).map_err(|e| {
                        Error::ConfigError(format!(
                            "Secret key {} is not valid UTF-8: {}",
                            secret_ref.key, e
                        ))
                    })?;
                    return Ok(val);
                }
            }
            return Err(Error::ConfigError(format!(
                "Key {} not found in secret {}",
                secret_ref.key, secret_ref.name
            )));
        }

        if let Some(ref cm_ref) = from.config_map_ref {
            let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), resource_ns);
            let cm = cm_api.get(&cm_ref.name).await.map_err(|e| {
                Error::ConfigError(format!("Failed to read ConfigMap {}: {}", cm_ref.name, e))
            })?;

            if let Some(ref data) = cm.data {
                if let Some(val) = data.get(&cm_ref.key) {
                    return Ok(val.clone());
                }
            }
            return Err(Error::ConfigError(format!(
                "Key {} not found in ConfigMap {}",
                cm_ref.key, cm_ref.name
            )));
        }
    }

    Err(Error::ConfigError(
        "ValueOrRefSource has neither value nor valueFrom specified".to_string(),
    ))
}

/// Resolves the raw schema text and interpolates variables for a Schema.
pub async fn resolve_and_interpolate_schema(
    client: &Client,
    schema: &Schema,
    schema_namespace: &str,
) -> Result<String> {
    let raw_schema_text = resolve_value(client, &schema.spec.schema, schema_namespace).await?;
    let mut interpolated_schema = raw_schema_text;

    if let Some(ref variables) = schema.spec.variables {
        for (key, source) in variables {
            let val = resolve_value(client, source, schema_namespace).await?;
            let placeholder = format!("${{{}}}", key);
            interpolated_schema = interpolated_schema.replace(&placeholder, &val);
        }
    }

    Ok(interpolated_schema)
}
