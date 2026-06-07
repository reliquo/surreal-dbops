pub mod instance;
pub mod namespace;
pub mod schema;
pub mod database;
pub mod rollout;

// Re-export custom resources and specs/statuses
pub use instance::{Instance, InstanceSpec, InstanceStatus};
pub use namespace::{Namespace, NamespaceSpec, NamespaceStatus};
pub use schema::{Schema, SchemaSpec, SchemaStatus, ApprovalPolicy};
pub use database::{Database, DatabaseSpec, DatabaseStatus};
pub use rollout::{Rollout, RolloutSpec, RolloutStatus};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Reference to another resource, with optional cross-namespace selection.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LocalObjectReference {
    pub name: String,
    pub namespace: Option<String>,
}

/// Flexible container that holds either a raw value or references a Secret/ConfigMap.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ValueOrRefSource {
    pub value: Option<String>,
    pub value_from: Option<ValueFromSource>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ValueFromSource {
    pub secret_key_ref: Option<SecretKeySelector>,
    pub config_map_ref: Option<ConfigMapKeySelector>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeySelector {
    pub name: String,
    pub key: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapKeySelector {
    pub name: String,
    pub key: String,
}

/// Standard Kubernetes Conditions status block.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    pub r#type: String,
    pub status: String,
    pub last_transition_time: String,
    pub reason: String,
    pub message: String,
}
