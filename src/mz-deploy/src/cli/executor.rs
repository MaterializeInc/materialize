//! Deployment execution utilities.
//!
//! This module contains the `DeploymentExecutor` for running SQL statements
//! during deployment, along with helper functions for collecting deployment
//! metadata and generating environment names.

use crate::cli::CliError;
use crate::cli::git::get_git_commit;
use crate::client::Client;
use crate::project::{self, typed};
use std::path::Path;

/// Collect deployment metadata (user and git commit).
///
/// This function retrieves the current database user and git commit hash
/// for recording deployment provenance. If the current user cannot be
/// determined, it defaults to "unknown".
pub async fn collect_deployment_metadata(
    client: &Client,
    directory: &Path,
) -> project::deployment_snapshot::DeploymentMetadata {
    let deployed_by = client
        .introspection()
        .get_current_user()
        .await
        .unwrap_or_else(|e| {
            eprintln!("warning: failed to get current user: {}", e);
            "unknown".to_string()
        });

    let git_commit = get_git_commit(directory);

    project::deployment_snapshot::DeploymentMetadata {
        deployed_by,
        git_commit,
    }
}

/// Generate a random 7-character hex environment name.
///
/// Uses SHA256 hash of current timestamp to generate a unique identifier
/// for deployments when no explicit name is provided.
pub fn generate_random_env_name() -> String {
    use sha2::{Digest, Sha256};
    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_nanos();

    let mut hasher = Sha256::new();
    hasher.update(now.to_le_bytes());
    let hash = hasher.finalize();

    // Take first 4 bytes of hash and format as 7-char hex
    format!(
        "{:07x}",
        u32::from_le_bytes([hash[0], hash[1], hash[2], hash[3]]) & 0xFFFFFFF
    )
}

/// Helper for executing database object deployments.
///
/// This struct consolidates the pattern of executing a database object's
/// SQL statements (main statement + indexes + grants + comments) with
/// consistent error handling. Supports dry-run mode where SQL is printed
/// instead of executed.
pub struct DeploymentExecutor<'a> {
    client: &'a Client,
    dry_run: bool,
}

impl<'a> DeploymentExecutor<'a> {
    /// Create a new deployment executor that executes SQL.
    pub fn new(client: &'a Client) -> Self {
        Self {
            client,
            dry_run: false,
        }
    }

    /// Create a deployment executor with configurable dry-run mode.
    pub fn with_dry_run(client: &'a Client, dry_run: bool) -> Self {
        Self { client, dry_run }
    }

    /// Returns true if this executor is in dry-run mode.
    pub fn is_dry_run(&self) -> bool {
        self.dry_run
    }

    /// Execute all SQL statements for a database object.
    ///
    /// This executes the main CREATE statement, followed by any indexes,
    /// grants, and comments associated with the object.
    pub async fn execute_object(&self, typed_obj: &typed::DatabaseObject) -> Result<(), CliError> {
        // Execute main statement
        self.execute_sql(&typed_obj.stmt).await?;

        // Execute indexes
        for index in &typed_obj.indexes {
            self.execute_sql(index).await?;
        }

        // Execute grants
        for grant in &typed_obj.grants {
            self.execute_sql(grant).await?;
        }

        // Execute comments
        for comment in &typed_obj.comments {
            self.execute_sql(comment).await?;
        }

        Ok(())
    }

    /// Execute (or print in dry-run mode) a single SQL statement.
    pub async fn execute_sql(&self, stmt: &impl ToString) -> Result<(), CliError> {
        let sql = stmt.to_string();

        if self.dry_run {
            println!("{};", sql);
            println!();
        } else {
            self.client.execute(&sql, &[]).await.map_err(|source| {
                CliError::SqlExecutionFailed {
                    statement: sql,
                    source,
                }
            })?;
        }
        Ok(())
    }
}
