use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use crate::crd::LocalObjectReference;

/// Declares a SurrealDB Namespace linked to a specific SurrealDB Instance.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[kube(
    group = "surrealdb.reliquo.io",
    version = "v1alpha1",
    kind = "Namespace",
    plural = "namespaces",
    namespaced,
    status = "NamespaceStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceSpec {
    /// Reference to the Instance CRD hosting this namespace.
    pub instance_ref: LocalObjectReference,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceStatus {
    pub created: bool,
    pub error: Option<String>,
    pub observed_generation: Option<i64>,
}
