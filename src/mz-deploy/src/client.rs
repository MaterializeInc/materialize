//! Database client layer for communicating with a Materialize region.
//!
//! All interaction with the live database flows through this module. The
//! [`Client`] type (defined in [`connection`]) holds a `tokio_postgres`
//! connection and exposes scoped sub-clients that group related operations:
//!
//! - **[`introspection`]** — Read-only catalog queries: schema/cluster/object
//!   existence checks, dependency lookups, and batch metadata retrieval.
//! - **[`provisioning`]** — DDL operations that create or alter databases,
//!   schemas, and clusters to match the project definition.
//! - **[`deployment_ops`]** — Blue/green deployment lifecycle: staging,
//!   hydration monitoring, cutover, and abort.
//! - **[`validation`]** — Pre-deployment validation: checks that the target
//!   environment matches expected state before applying changes.
//! - **[`type_info`]** — `SHOW COLUMNS` queries used to generate and refresh
//!   the `types.lock` data-contract file.
//!
//! ## Supporting Submodules
//!
//! - **[`config`]** — Profile and project settings loading (`profiles.toml`,
//!   `project.toml`).
//! - **[`models`]** — Data structures shared across sub-clients (deployment
//!   records, cluster configs, conflict records, etc.).
//! - **[`errors`]** — Error types: [`ConnectionError`] for transport/query
//!   failures, [`DatabaseValidationError`] for semantic mismatches.
//!
//! Most sub-client types are internal; this module re-exports the key public
//! types so that consumers only need `use crate::client::*`.

mod connection;
mod deployment_ops;
mod errors;
mod introspection;
mod models;
mod provisioning;
mod type_info;
mod validation;

pub use crate::config::Profile;
pub use connection::Client;

/// Double-quote a SQL identifier, escaping any embedded double quotes.
pub fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
pub use deployment_ops::{
    ClusterDeploymentStatus, ClusterStatusContext, FailureReason, HydrationStatusUpdate,
    DEFAULT_ALLOWED_LAG_SECS,
};
pub use errors::{format_relative_path, ConnectionError, DatabaseValidationError};
pub use models::{
    ApplyState, Cluster, ClusterConfig, ClusterGrant, ClusterOptions, ClusterReplica,
    ConflictRecord, DeploymentDetails, DeploymentHistoryEntry, DeploymentKind, DeploymentMetadata,
    DeploymentObjectRecord, PendingStatement, ReplacementMvRecord, SchemaDeploymentRecord,
    StagingDeployment,
};