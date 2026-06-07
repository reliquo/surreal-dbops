pub mod instance;
pub mod namespace;
pub mod database;
pub mod schema;
pub mod rollout;
pub mod utils;

use std::sync::Arc;
use kube::Client;
use thiserror::Error;

/// Context passed to each controller reconciliation loop.
#[derive(Clone)]
pub struct Context {
    pub client: Client,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("SurrealDB error: {0}")]
    SurrealError(String),

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Internal controller error: {0}")]
    InternalError(String),
}

pub type Result<T> = std::result::Result<T, Error>;
