use crate::crd::{LocalObjectReference, ValueOrRefSource};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Declares a SurrealDB Namespace linked to a specific SurrealDB Instance.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[kube(
    group = "surreal-dbops.reliquo.io",
    version = "v1alpha1",
    kind = "Namespace",
    plural = "namespaces",
    namespaced,
    status = "NamespaceStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceSpec {
    /// Optional name of the SurrealDB namespace. Defaults to metadata.name if not specified.
    pub name: Option<String>,
    /// Reference to the Instance CRD hosting this namespace.
    pub instance_ref: LocalObjectReference,
    /// List of user credentials to provision for this namespace.
    pub user_credentials: Option<Vec<UserCredentials>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UserCredentials {
    pub username: ValueOrRefSource,
    pub password: ValueOrRefSource,
    pub roles: Option<Vec<String>>,
    pub duration: Option<UserDuration>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UserDuration {
    pub token: Option<String>,
    pub session: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceStatus {
    pub created: bool,
    pub error: Option<String>,
    pub observed_generation: Option<i64>,
}
