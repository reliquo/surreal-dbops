use std::collections::BTreeMap;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use crate::crd::ValueOrRefSource;

/// Defines a database schema template, with variables and rollout policies.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[kube(
    group = "surrealdb.reliquo.io",
    version = "v1alpha1",
    kind = "Schema",
    plural = "schemas",
    namespaced,
    status = "SchemaStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct SchemaSpec {
    /// Number of completed/failed Rollout resources to retain for audit history.
    pub revision_history_limit: Option<usize>,
    /// Limit on the number of database rollouts to process concurrently. Defaults to 50.
    pub concurrency_limit: Option<usize>,
    /// Approval policy for database rollouts. Options: destructive (default) | all | none.
    pub require_approval: Option<ApprovalPolicy>,
    /// The SurrealQL schema source.
    pub schema: ValueOrRefSource,
    /// Key-value variables to interpolate into the schema definition.
    pub variables: Option<BTreeMap<String, ValueOrRefSource>>,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum ApprovalPolicy {
    Destructive,
    All,
    None,
}

impl Default for ApprovalPolicy {
    fn default() -> Self {
        ApprovalPolicy::Destructive
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct SchemaStatus {
    pub current_version_hash: Option<String>,
    pub active_rollout_name: Option<String>,
    pub observed_generation: Option<i64>,
}
