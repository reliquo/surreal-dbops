use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use crate::crd::{Condition, LocalObjectReference};

/// Tracks a schema migration execution.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[kube(
    group = "surrealdb.reliquo.io",
    version = "v1alpha1",
    kind = "Rollout",
    plural = "rollouts",
    namespaced,
    status = "RolloutStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct RolloutSpec {
    /// Reference to the Schema template being rolled out.
    pub schema_ref: LocalObjectReference,
    /// Generation of the Schema that this rollout corresponds to.
    pub generation: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct RolloutStatus {
    pub phase: Option<String>,          // Blocked, Progressing, Completed, Failed
    pub diff: Option<String>,           // Generated SurrealQL schema diff
    pub destructive: bool,              // True if the diff contains destructive statements
    
    // Concurrency stats
    pub affected_databases: usize,
    pub applied_databases: usize,
    pub failed_databases: usize,

    // Audit trail
    pub approved: bool,
    pub approved_by: Option<String>,
    pub approved_at: Option<String>,
    pub completed_at: Option<String>,

    pub conditions: Vec<Condition>,
    pub observed_generation: Option<i64>,
}
