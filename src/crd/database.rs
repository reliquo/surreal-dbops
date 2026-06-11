use crate::crd::LocalObjectReference;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Declares a SurrealDB Database instance managed in-operator.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[kube(
    group = "surreal-dbops.reliquo.io",
    version = "v1alpha1",
    kind = "Database",
    plural = "databases",
    namespaced,
    status = "DatabaseStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseSpec {
    /// Reference to the logical Namespace CRD.
    pub namespace_ref: LocalObjectReference,
    /// Reference to the Schema CRD template to apply.
    pub schema_ref: LocalObjectReference,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseStatus {
    pub created: bool,
    pub applied_schema_hash: Option<String>,
    pub applied_schema_generation: Option<i64>,
    pub error: Option<String>,
    pub observed_generation: Option<i64>,
}
