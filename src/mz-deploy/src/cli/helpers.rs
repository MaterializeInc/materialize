//! Shared helper functions for CLI commands.
//!
//! This module contains common functionality used across multiple commands
//! to reduce code duplication and ensure consistent behavior.

use crate::client::Client;
use crate::project;
use crate::utils::git::get_git_commit;
use std::path::Path;

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
