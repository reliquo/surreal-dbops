use crate::crd::ValueOrRefSource;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Defines a SurrealDB server instance connection configuration.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[kube(
    group = "surreal-dbops.reliquo.io",
    version = "v1alpha1",
    kind = "Instance",
    plural = "instances",
    namespaced,
    status = "InstanceStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct InstanceSpec {
    /// The database connection string (e.g. ws://surrealdb.reliquo:8000).
    pub connection_string: ValueOrRefSource,
    /// Administrative username.
    pub username: ValueOrRefSource,
    /// Administrative password.
    pub password: ValueOrRefSource,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct InstanceStatus {
    pub connected: bool,
    pub error: Option<String>,
    pub observed_generation: Option<i64>,
}
