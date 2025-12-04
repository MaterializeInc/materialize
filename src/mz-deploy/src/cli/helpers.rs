//! Shared helper functions for CLI commands.
//!
//! This module contains common functionality used across multiple commands
//! to reduce code duplication and ensure consistent behavior.

use crate::cli::CliError;
use crate::client::{Client, Profile};
use crate::project::{self, typed};
use crate::utils::git::get_git_commit;
use std::path::Path;

/// Connect to the database using the specified profile.
///
/// # Arguments
/// * `profile` - Database profile containing connection information
///
/// # Returns
/// Connected database client
///
/// # Errors
/// Returns `CliError::Connection` if connection fails
pub async fn connect_to_database(profile: &Profile) -> Result<Client, CliError> {
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    client
        .log_connection_info()
        .await
        .map_err(CliError::Connection)?;

    Ok(client)
}

/// Collect deployment metadata (user and git commit).
///
/// This function retrieves the current database user and git commit hash
/// for recording deployment provenance. If the current user cannot be
/// determined, it defaults to "unknown".
///
/// # Arguments
/// * `client` - Database client for querying current user
/// * `directory` - Project directory for determining git commit
///
/// # Returns
/// Deployment metadata containing user and optional git commit
pub async fn collect_deployment_metadata(
    client: &Client,
    directory: &Path,
) -> project::deployment_snapshot::DeploymentMetadata {
    let deployed_by = client.get_current_user().await.unwrap_or_else(|e| {
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
///
/// # Returns
/// A 7-character lowercase hex string (e.g., "a3f7b2c")
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
/// consistent error handling.
pub struct DeploymentExecutor<'a> {
    client: &'a Client,
}

impl<'a> DeploymentExecutor<'a> {
    /// Create a new deployment executor.
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Execute all SQL statements for a database object.
    ///
    /// This executes the main CREATE statement, followed by any indexes,
    /// grants, and comments associated with the object.
    ///
    /// # Arguments
    /// * `typed_obj` - The typed database object to deploy
    ///
    /// # Returns
    /// Ok(()) if all statements execute successfully
    ///
    /// # Errors
    /// Returns `CliError::SqlExecutionFailed` if any statement fails
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

    /// Execute a single SQL statement with error handling.
    ///
    /// # Arguments
    /// * `stmt` - Any type that can be converted to SQL string (via ToString)
    ///
    /// # Errors
    /// Returns `CliError::SqlExecutionFailed` with statement context
    async fn execute_sql(&self, stmt: &impl ToString) -> Result<(), CliError> {
        let sql = stmt.to_string();
        self.client
            .execute(&sql, &[])
            .await
            .map_err(|source| CliError::SqlExecutionFailed {
                statement: sql,
                source,
            })?;
        Ok(())
    }
}
