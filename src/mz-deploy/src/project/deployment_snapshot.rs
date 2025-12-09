//! Deployment snapshot tracking.
//!
//! This module provides functionality for capturing and comparing deployment state snapshots.
//! Instead of hashing raw files, we hash the normalized typed representation
//! objects, so formatting and comment changes don't trigger unnecessary redeployments.
//!
//! A deployment snapshot captures the state of all deployed objects with their content hashes,
//! enabling change detection (like git diff but for database objects) and supporting
//! blue/green deployment workflows.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::client::{Client, ConnectionError};
use crate::project::object_id::ObjectId;
use crate::project::{planned, typed};

/// A wrapper that bridges `std::hash::Hasher` to `sha2::Digest`.
///
/// This allows us to use the `Hash` trait on AST nodes while using SHA256 for stability.
struct Sha256Hasher {
    digest: Sha256,
}

impl Sha256Hasher {
    fn new() -> Self {
        Self {
            digest: Sha256::new(),
        }
    }

    fn finalize(self) -> String {
        let result = self.digest.finalize();
        format!("sha256:{:x}", result)
    }
}

impl Hasher for Sha256Hasher {
    fn write(&mut self, bytes: &[u8]) {
        self.digest.update(bytes);
    }

    fn finish(&self) -> u64 {
        // This is never called when using Hash trait, but required by trait
        // We use finalize() instead to get the full hash
        panic!("Sha256Hasher::finish() should not be called, use finalize() instead");
    }
}

/// Represents a point-in-time snapshot of deployment state.
///
/// Maps object IDs to their content hashes, where the hash is computed from
/// the normalized typed representation (not raw file contents).
/// Also tracks which schemas were deployed as atomic units.
#[derive(Debug, Clone)]
pub struct DeploymentSnapshot {
    /// Map of ObjectId to content hash
    pub objects: BTreeMap<ObjectId, String>,
    /// Set of (database, schema) tuples that were deployed
    pub schemas: BTreeSet<(String, String)>,
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

    #[error("failed to build snapshot from planned representation: {0}")]
    PlannedAccess(String),

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
            schemas: BTreeSet::new(),
        }
    }
}

/// Compute a deterministic hash of a typed DatabaseObject.
/// The hash includes:
/// - The main CREATE statement
/// - All indexes
///
/// Uses SHA256 for stable, deterministic hashing across platforms and Rust versions.
pub fn compute_typed_hash(db_obj: &typed::DatabaseObject) -> String {
    let mut hasher = Sha256Hasher::new();

    // Hash the main statement directly using its Hash implementation
    db_obj.stmt.hash(&mut hasher);

    let mut indexes = db_obj.indexes.clone();

    // Ensure hash is stable by sorting indexes deterministically
    indexes.sort_by(|a, b| {
        a.in_cluster
            .cmp(&b.in_cluster)
            .then(a.on_name.cmp(&b.on_name))
            .then(a.name.cmp(&b.name))
            .then_with(|| {
                let key_a = a.key_parts.as_ref().map(|ks| {
                    ks.iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                });
                let key_b = b.key_parts.as_ref().map(|ks| {
                    ks.iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                });

                key_a.cmp(&key_b)
            })
    });

    // Hash all indexes directly using their Hash implementation
    for index in &indexes {
        index.hash(&mut hasher);
    }

    hasher.finalize()
}

/// Build a deployment snapshot from a planned Project by hashing all typed objects.
///
/// This iterates through all objects in the project and computes their
/// content hashes based on the normalized typed representation.
pub fn build_snapshot_from_planned(
    planned_project: &planned::Project,
) -> Result<DeploymentSnapshot, DeploymentSnapshotError> {
    let mut objects = BTreeMap::new();
    let mut schemas = BTreeSet::new();

    // Get all objects in topological order
    let sorted_objects = planned_project
        .get_sorted_objects()
        .map_err(|e| DeploymentSnapshotError::PlannedAccess(e.to_string()))?;

    // Compute hash for each object and collect schemas
    for (object_id, typed_obj) in sorted_objects {
        let hash = compute_typed_hash(typed_obj);
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
/// * `deploy_id` - Deploy ID (e.g., "<init>" for direct deploy, "staging" for staged)
/// * `metadata` - Deployment metadata (user, git commit, etc.)
/// * `promoted_at` - Optional promoted_at timestamp (Some(now) for direct apply, None for stage)
/// * `kind` - Type of deployment (Tables or Objects)
pub async fn write_to_database(
    client: &Client,
    snapshot: &DeploymentSnapshot,
    deploy_id: &str,
    metadata: &DeploymentMetadata,
    promoted_at: Option<DateTime<Utc>>,
    kind: crate::client::DeploymentKind,
) -> Result<(), DeploymentSnapshotError> {
    use crate::client::{DeploymentObjectRecord, SchemaDeploymentRecord};

    let now = Utc::now();

    // Build schema deployment records
    let mut schema_records = Vec::new();
    for (database, schema) in &snapshot.schemas {
        schema_records.push(SchemaDeploymentRecord {
            deploy_id: deploy_id.to_string(),
            database: database.clone(),
            schema: schema.clone(),
            deployed_at: now,
            deployed_by: metadata.deployed_by.clone(),
            promoted_at,
            git_commit: metadata.git_commit.clone(),
            kind,
        });
    }

    // Build deployment object records
    let mut object_records = Vec::new();
    for (object_id, hash) in &snapshot.objects {
        object_records.push(DeploymentObjectRecord {
            deploy_id: deploy_id.to_string(),
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
