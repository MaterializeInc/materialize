//! Error types for CLI operations.
//!
//! This module provides error types for high-level CLI commands that wrap
//! lower-level errors from the client and project modules.

use crate::client::{ConflictRecord, ConnectionError, DatabaseValidationError};
use crate::project::deployment_snapshot::DeploymentSnapshotError;
use crate::project::error::{DependencyError, ProjectError};
use crate::types::{TypeCheckError, TypesError};
use owo_colors::OwoColorize;
use thiserror::Error;

/// Top-level error type for CLI operations.
///
/// This wraps errors from project loading, database operations, and
/// adds CLI-specific error variants.
#[derive(Debug, Error)]
pub enum CliError {
    /// Error during project compilation/loading
    #[error(transparent)]
    Project(#[from] ProjectError),

    /// Database connection error
    #[error(transparent)]
    Connection(#[from] ConnectionError),

    /// Deployment snapshot operation error
    #[error(transparent)]
    DeploymentSnapshot(#[from] DeploymentSnapshotError),

    /// Dependency analysis error
    #[error(transparent)]
    Dependency(#[from] DependencyError),

    /// Validation error (missing databases, schemas, clusters)
    #[error(transparent)]
    Validation(DatabaseValidationError),

    /// Types lock file error
    #[error(transparent)]
    Types(#[from] TypesError),

    /// Deployment conflict detected - schemas were updated after deployment started
    #[error("deployment conflict: {count} schema{plural} updated since deployment started",
        count = conflicts.len(),
        plural = if conflicts.len() == 1 { "" } else { "s" })]
    DeploymentConflict { conflicts: Vec<ConflictRecord> },

    /// Staging environment not found
    #[error("staging environment '{name}' not found")]
    StagingEnvironmentNotFound { name: String },

    /// Staging environment already promoted
    #[error("staging environment '{name}' has already been promoted")]
    StagingAlreadyPromoted { name: String },

    /// Failed to determine git SHA
    #[error("failed to determine git SHA for staging environment name")]
    GitShaFailed,

    /// Git repository has uncommitted changes
    #[error("git repository has uncommitted changes")]
    GitDirty,

    /// No schemas to deploy
    #[error("no schemas found to deploy")]
    NoSchemas,

    /// Attempting to overwrite production objects without proper safety flags
    #[error("refusing to overwrite production objects")]
    ProductionOverwriteNotAllowed {
        objects: Vec<(String, String, String)>, // (database, schema, object)
    },

    /// Failed to create deployment table
    #[error("failed to create deployment tracking table")]
    DeploymentTableCreationFailed { source: ConnectionError },

    /// Failed to execute SQL during deployment
    #[error("failed to execute SQL statement")]
    SqlExecutionFailed {
        statement: String,
        source: ConnectionError,
    },

    /// Failed to write deployment state
    #[error("failed to write deployment state to tracking table")]
    DeploymentStateWriteFailed { source: ConnectionError },

    /// Invalid staging environment name
    #[error("invalid staging environment name: '{name}'")]
    InvalidEnvironmentName { name: String },

    /// Schema does not exist in database
    #[error("schema '{schema}' does not exist in database '{database}'")]
    SchemaNotFound { database: String, schema: String },

    /// Cluster does not exist
    #[error("cluster '{name}' does not exist")]
    ClusterNotFound { name: String },

    /// Tests failed during execution
    #[error("{failed} test{plural} failed, {passed} passed",
        plural = if *failed == 1 { "" } else { "s" })]
    TestsFailed { failed: usize, passed: usize },

    /// Type check failed
    #[error(transparent)]
    TypeCheckFailed(#[from] TypeCheckError),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Generic error message
    #[error("{0}")]
    Message(String),
}

impl CliError {
    /// Get contextual hint for resolving this error.
    ///
    /// Returns `None` for errors that wrap other error types (they provide their own hints).
    pub fn hint(&self) -> Option<String> {
        match self {
            Self::Project(_) | Self::Connection(_) => None,
            Self::DeploymentConflict { conflicts } => {
                let conflict_list = conflicts
                    .iter()
                    .map(|c| format!("  - {}.{} (last promoted by '{}' at {:?})",
                        c.database.yellow(),
                        c.schema.yellow(),
                        c.environment,
                        c.promoted_at))
                    .collect::<Vec<_>>()
                    .join("\n");
                Some(format!(
                    "the following schemas were updated in production after your deployment started:\n{}\n\n\
                     Rebase your deployment by running:\n  \
                     {} {} {}\n  \
                     {} {} {} --name <staging-env>\n\n\
                     Or use {} to force the deployment (may overwrite recent changes)",
                    conflict_list,
                    "mz-deploy".cyan(),
                    "abort".cyan(),
                    "--name <staging-env>".cyan(),
                    "mz-deploy".cyan(),
                    "stage".cyan(),
                    ".".cyan(),
                    "--force".yellow().bold()
                ))
            }
            Self::StagingEnvironmentNotFound { name } => Some(format!(
                "verify the staging environment name '{}' is correct, or deploy to staging first using:\n  \
                 {} {} {} --name {}",
                name.yellow(),
                "mz-deploy".cyan(),
                "stage".cyan(),
                ".".cyan(),
                name.cyan()
            )),
            Self::StagingAlreadyPromoted { .. } => Some(
                "this staging environment has already been applied to production.\n\
                 Deploy a new staging environment to make changes"
                    .to_string(),
            ),
            Self::GitShaFailed => Some(
                "either run mz-deploy from inside a git repository, or provide a staging environment name using:\n  \
                 mz-deploy stage . --name <environment-name>"
                    .to_string(),
            ),
            Self::GitDirty => Some(
                "commit or stash your changes before deploying, or use the --allow-dirty flag to deploy anyway"
                    .to_string(),
            ),
            Self::NoSchemas => Some(
                "create at least one schema directory under your project directory (e.g., materialize/public/)"
                    .to_string(),
            ),
            Self::ProductionOverwriteNotAllowed { objects } => {
                let object_list = objects
                    .iter()
                    .take(5)
                    .map(|(db, schema, obj)| {
                        format!("  - {}.{}.{}", db.yellow(), schema.yellow(), obj.yellow())
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                let more = if objects.len() > 5 {
                    format!("\n  ... and {} more", objects.len() - 5)
                } else {
                    String::new()
                };
                Some(format!(
                    "the following objects already exist in production:\n{}{}\n\n\
                     To update existing objects, use blue/green deployment:\n  \
                     {} {} {}\n  \
                     {} {} {} <staging-env>",
                    object_list,
                    more,
                    "mz-deploy".cyan(),
                    "stage".cyan(),
                    ".".cyan(),
                    "mz-deploy".cyan(),
                    "apply".cyan(),
                    "--staging-env".cyan()
                ))
            }
            Self::DeploymentTableCreationFailed { .. } => Some(
                "ensure your database user has CREATE privileges on the database"
                    .to_string(),
            ),
            Self::SqlExecutionFailed { statement, .. } => Some(format!(
                "check the SQL statement for syntax errors:\n  {}",
                statement.lines().take(3).collect::<Vec<_>>().join("\n  ")
            )),
            Self::DeploymentStateWriteFailed { .. } => Some(
                "the SQL was applied successfully, but deployment tracking failed.\n\
                 The next deployment may re-apply some objects"
                    .to_string(),
            ),
            Self::InvalidEnvironmentName { .. } => Some(
                "environment names must contain only alphanumeric characters, hyphens, and underscores"
                    .to_string(),
            ),
            Self::SchemaNotFound { database, schema } => Some(format!(
                "create the schema first, or check that you're connected to the correct database.\n  \
                 CREATE SCHEMA {}.{}",
                database.cyan(),
                schema.cyan()
            )),
            Self::ClusterNotFound { name } => Some(format!(
                "create the cluster first:\n  \
                 CREATE CLUSTER {} SIZE = '{}' REPLICATION FACTOR = 1",
                name.cyan(),
                "M.1-small".cyan()
            )),
            Self::TestsFailed { .. } => Some(
                "review the test output above for details on which assertions failed"
                    .to_string(),
            ),
            Self::TypeCheckFailed(_) => Some(
                "review the type checking errors above and fix any SQL syntax or dependency issues"
                    .to_string(),
            ),
            Self::Validation(_) | Self::Types(_) | Self::DeploymentSnapshot(_) | Self::Dependency(_) => {
                // These errors provide their own context via transparent wrapping
                None
            }
            Self::Io(_) | Self::Message(_) => None,
        }
    }
}

impl From<DatabaseValidationError> for CliError {
    fn from(error: DatabaseValidationError) -> Self {
        CliError::Validation(error)
    }
}

impl From<String> for CliError {
    fn from(msg: String) -> Self {
        CliError::Message(msg)
    }
}
