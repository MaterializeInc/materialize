// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Error types for CLI operations.
//!
//! This module provides error types for high-level CLI commands that wrap
//! lower-level errors from the client and project modules.

use crate::cli::commands::test::TestValidationError;
use crate::client::{
    ConflictRecord, ConnectionError, DatabaseValidationError, ProductionClusterRecord,
};
use crate::config::ConfigError;
use crate::project::analysis::deployment_snapshot::DeploymentSnapshotError;
use crate::project::compiler::typecheck::TypeCheckError;
use crate::project::error::{DependencyError, ProjectError};
use crate::project::ir::object_id::ObjectId;
use crate::secret_resolver::SecretResolveError;
use crate::types::TypesError;
use chrono::{DateTime, Local};
use owo_colors::{OwoColorize, Stream, Style};
use thiserror::Error;

/// An object `setup::verify` expected to find but didn't.
///
/// Used to build an actionable hint on [`CliError::SetupRequired`] that names
/// the missing pieces so the user knows whether to run `setup` for the first
/// time or re-run it after an mz-deploy upgrade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MissingObject {
    /// The `_mz_deploy_server` cluster.
    Cluster(String),
    /// The `_mz_deploy` database.
    Database(String),
    /// A table, view, or index inside `_mz_deploy`.
    SchemaObject {
        schema: String,
        name: String,
        kind: String,
    },
    /// One of the `materialize_*` roles.
    Role(String),
}

/// Top-level error type for CLI operations.
///
/// This wraps errors from project loading, database operations, and
/// adds CLI-specific error variants.
#[derive(Debug, Error)]
pub enum CliError {
    /// Error during project compilation/loading
    #[error(transparent)]
    Project(#[from] ProjectError),

    /// Configuration loading error
    #[error(transparent)]
    Config(#[from] ConfigError),

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

    /// `dev` would deploy to a cluster that hosts a promoted deployment.
    #[error("refusing to deploy dev overlay onto production cluster")]
    DevTargetsProductionCluster { cluster: ProductionClusterRecord },

    /// Required mz-deploy infrastructure is missing or partially installed.
    /// Emitted by every non-`setup` command when `setup::verify` fails.
    #[error("mz-deploy infrastructure is not fully initialized")]
    SetupRequired { missing: Vec<MissingObject> },

    /// `setup` was invoked by a role that is not the owner of the existing
    /// `_mz_deploy` database. Only the owner can re-run `setup`.
    #[error("`_mz_deploy` is owned by role '{owner}', not by the current role '{current_role}'")]
    SetupNotDatabaseOwner { owner: String, current_role: String },

    /// `setup` was invoked by a non-superuser. Setup issues
    /// `GRANT ... ON SYSTEM` statements that only superusers can execute.
    #[error("`mz-deploy setup` requires a superuser role (current role: '{current_role}')")]
    SetupRequiresSuperuser { current_role: String },

    /// A unit test targets an object that isn't a view or materialized view.
    /// Unlike assertion mismatches, this is a project-definition error: the
    /// `test` run aborts rather than reporting it as a failed test.
    #[error("unit test '{test_name}' on '{object_id}' cannot be desugared: {reason}")]
    InvalidUnitTestTarget {
        test_name: String,
        object_id: String,
        reason: String,
    },

    /// Failed to create deployment table
    #[error("failed to create deployment tracking table: {source}")]
    DeploymentTableCreationFailed { source: ConnectionError },

    /// Failed to execute SQL during deployment
    #[error("failed to execute SQL statement: {source}")]
    SqlExecutionFailed {
        statement: String,
        source: ConnectionError,
    },

    /// Failed to repoint a sink to a new upstream object
    #[error("failed to repoint sink {sink}: {reason}")]
    SinkRepointFailed { sink: String, reason: String },

    /// Failed to write deployment state
    #[error("failed to write deployment state to tracking table: {source}")]
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

    /// Tests filter did not match anythinh
    #[error("no tests matched filter '{filter}'")]
    TestsFilterMissed { filter: String },

    /// Test validation failed (schema mismatch, missing mocks)
    #[error(transparent)]
    TestValidationFailed(#[from] TestValidationError),

    /// Type check failed
    #[error(transparent)]
    TypeCheckFailed(#[from] TypeCheckError),

    /// Timeout waiting for deployment to be ready
    #[error("timeout waiting for deployment '{name}' to be ready after {seconds} seconds")]
    ReadyTimeout { name: String, seconds: u64 },

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Clusters are not yet hydrated
    #[error("some clusters are still hydrating")]
    ClustersHydrating,

    /// Deployment is failing due to cluster health issues
    #[error("deployment '{name}' is failing due to cluster health issues")]
    DeploymentFailing { name: String },

    /// Cannot add new objects to an existing stable schema during incremental deployment
    #[error("cannot add new objects to existing stable schema '{database}.{schema}'")]
    NewObjectInExistingStableSchema {
        database: String,
        schema: String,
        objects: Vec<String>,
    },

    /// Secret resolution failed
    #[error("failed to resolve secret '{secret_name}': {source}")]
    SecretResolution {
        secret_name: String,
        source: SecretResolveError,
    },

    /// Cluster is not ready for mz-deploy operations
    #[error("cluster '{cluster}' is not ready: {reason}")]
    ClusterNotReady { cluster: String, reason: String },

    /// Current role has no mz-deploy role membership
    #[error("current role is not a member of any mz-deploy role")]
    NoMzDeployRole,

    /// Current role has multiple mz-deploy role memberships
    #[error("current role is a member of multiple mz-deploy roles: {}", roles.join(", "))]
    MultipleMzDeployRoles { roles: Vec<String> },

    /// Current role is not authorized for this operation
    #[error(
        "this command requires the '{required_role}' role, but you are connected as '{current_role}'"
    )]
    RoleNotAuthorized {
        current_role: String,
        required_role: String,
    },

    /// Current role lacks CREATEDB privilege required to create overlay databases
    #[error(
        "user {role} lacks CREATEDB privilege, required to create overlay \
         database {overlay_db}.\n\n\
         Ask an administrator to run:\n    GRANT CREATEDB ON SYSTEM TO {role};"
    )]
    MissingCreatedb { role: String, overlay_db: String },

    /// Project directory has no basename usable as a project identifier
    /// (e.g. root or `.`).
    #[error("cannot derive project name from directory '{path}'")]
    InvalidProjectDirectory { path: String },

    /// Project references external objects not declared in project.toml
    #[error("undeclared external dependencies")]
    UndeclaredDependencies { undeclared: Vec<ObjectId> },

    /// Declared dependencies not found in the target database during lock
    #[error("declared dependencies not found in target database")]
    DeclaredDependenciesMissing { missing: Vec<ObjectId> },

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
            Self::Project(_) => None,
            Self::Connection(e) => match e {
                ConnectionError::DeploymentNotFound { deploy_id } => Some(format!(
                    "verify the staging environment name '{}' is correct, or deploy to staging first using:\n  \
                     {} {} {} --deploy-id {}",
                    deploy_id.if_supports_color(Stream::Stderr, |t| t.yellow()),
                    "mz-deploy".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    "stage".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    ".".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    deploy_id.if_supports_color(Stream::Stderr, |t| t.cyan())
                )),
                ConnectionError::DeploymentAlreadyPromoted { .. } => Some(
                    "this staging environment has already been applied to production.\n\
                     Deploy a new staging environment to make changes"
                        .to_string(),
                ),
                _ => None,
            },
            Self::DeploymentConflict { conflicts } => {
                let conflict_list = conflicts
                    .iter()
                    .map(|c| {
                        let promoted_datetime: DateTime<Local> = c.promoted_at.into();
                        let promoted_str = promoted_datetime
                            .format("%a %b %d %H:%M:%S %Y %z")
                            .to_string();
                        format!("  - {}.{} (last promoted by '{}' at {})",
                            c.database.if_supports_color(Stream::Stderr, |t| t.yellow()),
                            c.schema.if_supports_color(Stream::Stderr, |t| t.yellow()),
                            c.deploy_id,
                            promoted_str)
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                let force_style = Style::new().yellow().bold();
                Some(format!(
                    "the following schemas were updated in production after your deployment started:\n{}\n\n\
                     Rebase your deployment by running:\n  \
                     {} {} {}\n  \
                     {} {} {} --deploy-id <staging-env>\n\n\
                     Or use {} to force the deployment (may overwrite recent changes)",
                    conflict_list,
                    "mz-deploy".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    "abort".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    "<staging-env>".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    "mz-deploy".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    "stage".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    ".".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    "--force".if_supports_color(Stream::Stderr, |t| force_style.style(t))
                ))
            }
            Self::GitShaFailed => Some(
                "either run mz-deploy from inside a git repository, or provide a staging environment name using:\n  \
                 mz-deploy stage . --deploy-id <environment-name>"
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
                        format!(
                            "  - {}.{}.{}",
                            db.if_supports_color(Stream::Stderr, |t| t.yellow()),
                            schema.if_supports_color(Stream::Stderr, |t| t.yellow()),
                            obj.if_supports_color(Stream::Stderr, |t| t.yellow())
                        )
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
                    "mz-deploy".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    "stage".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    ".".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    "mz-deploy".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    "apply".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    "--staging-env".if_supports_color(Stream::Stderr, |t| t.cyan())
                ))
            }
            Self::SetupRequired { missing } => {
                let list = missing
                    .iter()
                    .take(5)
                    .map(|m| match m {
                        MissingObject::Cluster(name) => {
                            format!("  • cluster {}", name.if_supports_color(Stream::Stderr, |t| t.yellow()))
                        }
                        MissingObject::Database(name) => {
                            format!("  • database {}", name.if_supports_color(Stream::Stderr, |t| t.yellow()))
                        }
                        MissingObject::SchemaObject { schema, name, kind } => {
                            format!(
                                "  • {} _mz_deploy.{}.{}",
                                kind,
                                schema.if_supports_color(Stream::Stderr, |t| t.yellow()),
                                name.if_supports_color(Stream::Stderr, |t| t.yellow()),
                            )
                        }
                        MissingObject::Role(name) => {
                            format!("  • role {}", name.if_supports_color(Stream::Stderr, |t| t.yellow()))
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                let more = if missing.len() > 5 {
                    format!("\n  ... and {} more", missing.len() - 5)
                } else {
                    String::new()
                };
                Some(format!(
                    "the following mz-deploy objects are missing:\n{}{}\n\n\
                     run {} as an admin role with the {}, {}, and {} system \
                     privileges to initialize or self-heal the installation.",
                    list,
                    more,
                    "mz-deploy setup".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    "CREATECLUSTER".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    "CREATEDB".if_supports_color(Stream::Stderr, |t| t.cyan()),
                    "CREATEROLE".if_supports_color(Stream::Stderr, |t| t.cyan()),
                ))
            }
            Self::InvalidUnitTestTarget { .. } => Some(
                "unit tests can only target CREATE VIEW or CREATE MATERIALIZED VIEW \
                 statements. Move the test to a view/MV or remove it from the target \
                 object's file."
                    .to_string(),
            ),
            Self::SetupNotDatabaseOwner {
                owner,
                current_role,
            } => Some(format!(
                "{} is the only command that writes to `_mz_deploy`, and only \
                 the owning role can do so.\n\n  \
                 Re-run as {}, or have {} run {} to transfer ownership.",
                "mz-deploy setup".if_supports_color(Stream::Stderr, |t| t.cyan()),
                owner.if_supports_color(Stream::Stderr, |t| t.cyan()),
                owner.if_supports_color(Stream::Stderr, |t| t.cyan()),
                format!("ALTER DATABASE _mz_deploy OWNER TO {}", current_role).if_supports_color(Stream::Stderr, |t| t.cyan()),
            )),
            Self::SetupRequiresSuperuser { current_role } => Some(format!(
                "{} grants {} and {} on the system. With RBAC enabled, only \
                 a superuser can issue system grants, and the active role \
                 {} is not a superuser.\n\n  \
                 Re-run setup using a Materialize admin user, or have an \
                 admin run it once on your behalf. After setup completes, \
                 ordinary deployer/developer/monitor roles use mz-deploy \
                 normally — only this bootstrap step requires elevated \
                 privileges.",
                "mz-deploy setup".if_supports_color(Stream::Stderr, |t| t.cyan()),
                "CREATEDB".if_supports_color(Stream::Stderr, |t| t.cyan()),
                "CREATECLUSTER".if_supports_color(Stream::Stderr, |t| t.cyan()),
                current_role.if_supports_color(Stream::Stderr, |t| t.cyan()),
            )),
            Self::DevTargetsProductionCluster { cluster } => {
                let promoted_local: DateTime<Local> = cluster.promoted_at.into();
                let promoted_str = promoted_local.format("%b %d, %Y").to_string();
                Some(format!(
                    "cluster {} hosts a promoted deployment ({}.{}, promoted {}) \
                     and cannot be targeted by dev.\n\n\
                     re-run with a non-production cluster, e.g.:\n\n  \
                     mz-deploy dev <dev-cluster>",
                    cluster
                        .cluster_name
                        .if_supports_color(Stream::Stderr, |t| t.yellow()),
                    cluster.database,
                    cluster.schema,
                    promoted_str,
                ))
            }
            Self::DeploymentTableCreationFailed { .. } => Some(
                "ensure your database user has CREATE privileges on the database"
                    .to_string(),
            ),
            Self::SqlExecutionFailed { statement, .. } => Some(format!(
                "SQL statement:\n  {}",
                statement.lines().take(5).collect::<Vec<_>>().join("\n  ")
            )),
            Self::SinkRepointFailed { sink, .. } => Some(format!(
                "the sink '{}' could not be repointed to the new upstream object.\n\
                 This may happen if:\n  \
                 - The new object has an incompatible schema (e.g., Avro schema mismatch)\n  \
                 - The replacement object doesn't exist in the new schema\n\n\
                 To proceed, you may need to manually drop and recreate the sink",
                sink.if_supports_color(Stream::Stderr, |t| t.yellow())
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
                database.if_supports_color(Stream::Stderr, |t| t.cyan()),
                schema.if_supports_color(Stream::Stderr, |t| t.cyan())
            )),
            Self::ClusterNotFound { name } => Some(format!(
                "create the cluster first:\n  \
                 CREATE CLUSTER {} SIZE = '{}' REPLICATION FACTOR = 1",
                name.if_supports_color(Stream::Stderr, |t| t.cyan()),
                "M.1-small".if_supports_color(Stream::Stderr, |t| t.cyan())
            )),
            Self::TestsFailed { .. } => None,
            Self::TestValidationFailed(_) => Some(
                "review the validation error above and update your test to match the schema.\n\
                 Run 'mz-deploy compile' to regenerate the type cache if needed"
                    .to_string(),
            ),
            Self::TypeCheckFailed(_) => None,
            Self::ReadyTimeout { .. } => Some(
                "deployment is taking longer than expected to hydrate. You can:\n  \
                 - Increase timeout with --timeout flag\n  \
                 - Check cluster replica status with: mz-deploy ready <env>"
                    .to_string(),
            ),
            Self::ClustersHydrating => Some("check cluster replica status with: mz-deploy ready <env>".to_string()),
            Self::DeploymentFailing { .. } => Some(
                "one or more clusters are not ready. Check for:\n  \
                 - Missing replicas (cluster has no replicas configured)\n  \
                 - OOM-looping replicas (3+ OOM kills in 24 hours)\n\n\
                 Use 'mz-deploy ready <env>' for details"
                    .to_string(),
            ),
            Self::SecretResolution { .. } => Some(
                "check that environment variables are set and function arguments are string literals"
                    .to_string(),
            ),
            Self::NewObjectInExistingStableSchema { objects, .. } => {
                let object_list = objects
                    .iter()
                    .take(5)
                    .map(|obj| format!("  - {}", obj.if_supports_color(Stream::Stderr, |t| t.yellow())))
                    .collect::<Vec<_>>()
                    .join("\n");
                let more = if objects.len() > 5 {
                    format!("\n  ... and {} more", objects.len() - 5)
                } else {
                    String::new()
                };
                Some(format!(
                    "the following new objects cannot be added during incremental deployment:\n{}{}\n\n\
                     To add new objects, either:\n  \
                     - Place them in a new schema (new schemas deploy via blue-green swap)\n  \
                     - Deploy them separately before modifying existing objects in the same schema",
                    object_list,
                    more,
                ))
            }
            Self::ClusterNotReady { cluster, reason } => {
                if reason.contains("replication factor") {
                    Some(format!(
                        "the cluster '{}' has no replicas running. Ensure the cluster is scaled up:\n  \
                         ALTER CLUSTER {} SET (REPLICATION FACTOR = 1)",
                        cluster.if_supports_color(Stream::Stderr, |t| t.yellow()),
                        cluster.if_supports_color(Stream::Stderr, |t| t.cyan())
                    ))
                } else {
                    Some(format!(
                        "grant USAGE on the cluster to your role:\n  \
                         GRANT USAGE ON CLUSTER {} TO <your-role>",
                        cluster.if_supports_color(Stream::Stderr, |t| t.cyan())
                    ))
                }
            }
            Self::NoMzDeployRole => Some(
                "your role must be a member of one of: materialize_deployer, materialize_developer, or materialize_monitor.\n\
                 Run 'mz-deploy setup' first, then grant a role:\n  \
                 GRANT \"materialize_developer\" TO <your-role>"
                    .to_string(),
            ),
            Self::MultipleMzDeployRoles { .. } => Some(
                "each database role should have exactly one mz-deploy role.\n\
                 Set up distinct profiles with separate roles for deploying vs. monitoring"
                    .to_string(),
            ),
            Self::RoleNotAuthorized { current_role, required_role } => {
                let action = if current_role == "materialize_developer" {
                    "developing"
                } else {
                    "monitoring"
                };
                Some(format!(
                    "your current profile is configured for {}, but this command requires deploying privileges.\n\
                     Switch to a profile whose role is a member of '{}'",
                    action,
                    required_role.if_supports_color(Stream::Stderr, |t| t.cyan())
                ))
            }
            Self::UndeclaredDependencies { undeclared } => {
                let dep_list = undeclared
                    .iter()
                    .map(|d| format!("\t\"{}\",", d))
                    .collect::<Vec<_>>()
                    .join("\n");
                Some(format!(
                    "add these to project.toml:\n\n    dependencies = [\n{}\n    ]\n\n  \
                     then run `{}` to fetch their schemas.",
                    dep_list,
                    "mz-deploy lock".if_supports_color(Stream::Stderr, |t| t.cyan())
                ))
            }
            Self::DeclaredDependenciesMissing { missing } => {
                let list = missing
                    .iter()
                    .map(|d| format!("  {} {}", "×".if_supports_color(Stream::Stderr, |t| t.red()), d))
                    .collect::<Vec<_>>()
                    .join("\n");
                Some(format!(
                    "{}\n\nthese objects must exist before running `lock`. Check that they \
                     are deployed and that your profile points to the correct environment.",
                    list
                ))
            }
            Self::Config(ConfigError::NoProfileConfigured) => Some(format!(
                "record a default profile for this project:\n  \
                 {}\n\n\
                 or pass {} for a one-off run, or export {}.",
                "mz-deploy profile set <name>".if_supports_color(Stream::Stderr, |t| t.cyan()),
                "--profile <name>".if_supports_color(Stream::Stderr, |t| t.cyan()),
                "MZ_DEPLOY_PROFILE=<name>".if_supports_color(Stream::Stderr, |t| t.cyan()),
            )),
            Self::Config(_)
            | Self::Validation(_)
            | Self::Types(_)
            | Self::DeploymentSnapshot(_)
            | Self::Dependency(_) => {
                // These errors provide their own context via transparent wrapping
                None
            }
            Self::MissingCreatedb { .. } => None,
            Self::InvalidProjectDirectory { .. } => None,
            Self::Io(_) | Self::Message(_) | Self::TestsFilterMissed { .. } => None,
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
