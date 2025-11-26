//! Deployment snapshot tracking.
//!
//! This module provides functionality for capturing and comparing deployment state snapshots.
//! Instead of hashing raw files, we hash the normalized HIR (High-level Intermediate Representation)
//! objects, so formatting and comment changes don't trigger unnecessary redeployments.
//!
//! A deployment snapshot captures the state of all deployed objects with their content hashes,
//! enabling change detection (like git diff but for database objects) and supporting
//! blue/green deployment workflows.

use std::collections::{BTreeMap, HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};

use crate::client::{Client, ConnectionError};
use crate::project::object_id::ObjectId;
use crate::project::{hir, mir};

/// Represents a point-in-time snapshot of deployment state.
///
/// Maps object IDs to their content hashes, where the hash is computed from
/// the normalized HIR representation (not raw file contents).
/// Also tracks which schemas were deployed as atomic units.
#[derive(Debug, Clone)]
pub struct DeploymentSnapshot {
    /// Map of ObjectId to content hash
    pub objects: BTreeMap<ObjectId, String>,
    /// Set of (database, schema) tuples that were deployed
    pub schemas: HashSet<(String, String)>,
}

/// Metadata collected during deployment.
#[derive(Debug, Clone)]
pub struct DeploymentMetadata {
    /// Materialize user/role that performed the deployment
    pub deployed_by: String,
    /// Git commit hash if the project is in a git repository
    pub git_commit: Option<String>,
}

/// Error types for deployment snapshot operations.
#[derive(Debug, thiserror::Error)]
pub enum DeploymentSnapshotError {
    #[error("failed to connect to database: {0}")]
    Connection(#[from] ConnectionError),

    #[error("failed to build snapshot from MIR: {0}")]
    MirAccess(String),

    #[error("invalid object FQN: {0}")]
    InvalidFqn(String),

    #[error("deployment '{environment}' already exists")]
    DeploymentAlreadyExists { environment: String },

    #[error("deployment '{environment}' not found")]
    DeploymentNotFound { environment: String },

    #[error("deployment '{environment}' has already been promoted")]
    DeploymentAlreadyPromoted { environment: String },
}

impl Default for DeploymentSnapshot {
    fn default() -> Self {
        Self {
            objects: BTreeMap::new(),
            schemas: HashSet::new(),
        }
    }
}

/// Compute a deterministic hash of a HIR DatabaseObject.
///
/// This hashes the normalized AST representation by converting to stable string form, which means:
/// - Formatting and whitespace changes are ignored
/// - SQL comments are ignored (not part of the AST)
/// - Semantic changes (column types, query logic, etc.) are detected
///
/// The hash includes:
/// - The main CREATE statement
/// - All indexes
/// - All grants
/// - (Optionally) Comments - currently excluded since they're documentation
pub fn compute_hir_hash(db_obj: &hir::DatabaseObject) -> String {
    let mut hasher = DefaultHasher::new();

    // Hash the main statement by converting to stable string representation
    // This ensures consistent output regardless of formatting
    let stmt_string = db_obj.stmt.to_string();
    stmt_string.hash(&mut hasher);

    // Hash all indexes
    for index in &db_obj.indexes {
        let index_string = index.to_string();
        index_string.hash(&mut hasher);
    }

    // Hash all grants
    for grant in &db_obj.grants {
        let grant_string = grant.to_string();
        grant_string.hash(&mut hasher);
    }

    // Note: We intentionally skip comments from the hash computation.
    // Comments are documentation and shouldn't trigger redeployment.
    // for comment in &db_obj.comments {
    //     let comment_string = comment.to_string();
    //     comment_string.hash(&mut hasher);
    // }

    // Format as hex string with "hash:" prefix for clarity
    format!("hash:{:016x}", hasher.finish())
}

/// Build a deployment snapshot from a MIR Project by hashing all HIR objects.
///
/// This iterates through all objects in the project and computes their
/// content hashes based on the normalized HIR representation.
pub fn build_snapshot_from_mir(
    mir_project: &mir::Project,
) -> Result<DeploymentSnapshot, DeploymentSnapshotError> {
    let mut objects = BTreeMap::new();
    let mut schemas = HashSet::new();

    // Get all objects in topological order
    let sorted_objects = mir_project
        .get_sorted_objects()
        .map_err(|e| DeploymentSnapshotError::MirAccess(e.to_string()))?;

    // Compute hash for each object and collect schemas
    for (object_id, hir_obj) in sorted_objects {
        let hash = compute_hir_hash(hir_obj);
        objects.insert(object_id.clone(), hash);

        // Track which schema this object belongs to
        schemas.insert((object_id.database.clone(), object_id.schema.clone()));
    }

    Ok(DeploymentSnapshot { objects, schemas })
}

/// Initialize the deployment tracking infrastructure in the database.
///
/// Creates the `deploy` schema, `schema_deployments` table, and `deployment_objects` table
/// if they don't exist. This is idempotent and safe to call multiple times.
pub async fn initialize_deployment_table(client: &Client) -> Result<(), DeploymentSnapshotError> {
    client.create_deployments().await?;

    Ok(())
}

/// Load the current deployment state snapshot from the database for a specific environment.
///
/// # Arguments
/// * `client` - Database client connection
/// * `environment` - None for production, Some("staging") for staging environments
///
/// # Returns
/// DeploymentSnapshot with current deployment state, or empty snapshot if no deployments exist
pub async fn load_from_database(
    client: &Client,
    environment: Option<&str>,
) -> Result<DeploymentSnapshot, DeploymentSnapshotError> {
    let deployment_snapshot = client
        .get_deployment_objects(environment)
        .await
        .map_err(DeploymentSnapshotError::Connection)?;

    Ok(deployment_snapshot)
}

/// Write deployment snapshot to the database using the normalized schema.
///
/// This writes to both deployments and objects tables.
/// Schema deployments are inserted (no delete), while object deployments
/// are appended (insert-only history).
///
/// # Arguments
/// * `client` - Database client connection
/// * `snapshot` - The deployment snapshot to write
/// * `environment` - Environment name (e.g., "<init>" for direct deploy, "staging" for staged)
/// * `metadata` - Deployment metadata (user, git commit, etc.)
/// * `promoted_at` - Optional promoted_at timestamp (Some(now) for direct apply, None for stage)
pub async fn write_to_database(
    client: &Client,
    snapshot: &DeploymentSnapshot,
    environment: &str,
    metadata: &DeploymentMetadata,
    promoted_at: Option<std::time::SystemTime>,
) -> Result<(), DeploymentSnapshotError> {
    use crate::client::{DeploymentObjectRecord, SchemaDeploymentRecord};
    use std::time::SystemTime;

    let now = SystemTime::now();

    // Build schema deployment records
    let mut schema_records = Vec::new();
    for (database, schema) in &snapshot.schemas {
        schema_records.push(SchemaDeploymentRecord {
            environment: environment.to_string(),
            database: database.clone(),
            schema: schema.clone(),
            deployed_at: now,
            deployed_by: metadata.deployed_by.clone(),
            promoted_at,
            git_commit: None,
        });
    }

    // Build deployment object records
    let mut object_records = Vec::new();
    for (object_id, hash) in &snapshot.objects {
        object_records.push(DeploymentObjectRecord {
            environment: environment.to_string(),
            database: object_id.database.clone(),
            schema: object_id.schema.clone(),
            object: object_id.object.clone(),
            object_hash: hash.clone(),
            deployed_at: now,
        });
    }

    // Write to database
    client.insert_schema_deployments(&schema_records).await?;
    client.append_deployment_objects(&object_records).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_snapshot() {
        let snapshot = DeploymentSnapshot::default();
        assert!(snapshot.objects.is_empty());
    }

    // TODO: Add more tests for hash computation with actual HIR objects
}
