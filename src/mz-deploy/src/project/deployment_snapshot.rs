//! Deployment snapshot tracking.
//!
//! This module provides functionality for capturing and comparing deployment state snapshots.
//! Instead of hashing raw files, we hash the normalized typed representation
//! objects, so formatting and comment changes don't trigger unnecessary redeployments.
//!
//! A deployment snapshot captures the state of all deployed objects with their content hashes,
//! enabling change detection (like git diff but for database objects) and supporting
//! blue/green deployment workflows.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use chrono::{DateTime, Utc};

use crate::project::SchemaQualifier;
use crate::project::ast::Statement;
use sha2::{Digest, Sha256};

use crate::client::{
    Client, ConnectionError, DeploymentKind, DeploymentObjectRecord, SchemaDeploymentRecord,
};
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
#[derive(Debug, Clone, serde::Serialize)]
pub struct DeploymentSnapshot {
    /// Map of ObjectId to content hash
    pub objects: BTreeMap<ObjectId, String>,
    /// Map of (database, schema) to deployment kind
    /// - Objects: Regular schemas containing views/MVs that need swapping
    /// - Sinks: Schemas containing only sinks (no swap needed, sinks created after swap)
    /// - Tables: Schemas containing only tables
    pub schemas: BTreeMap<SchemaQualifier, DeploymentKind>,
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
            schemas: BTreeMap::new(),
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
    let mut schemas = BTreeMap::new();

    // Get all objects in topological order
    let sorted_objects = planned_project
        .get_sorted_objects()
        .map_err(|e| DeploymentSnapshotError::PlannedAccess(e.to_string()))?;

    // Compute hash for each object and collect schemas
    // Default to Objects kind - callers can override for specific schemas
    for (object_id, typed_obj) in sorted_objects {
        // Skip apply-managed objects — they are never recorded by record_stage_metadata
        // and should not participate in snapshot-based change detection.
        match &typed_obj.stmt {
            Statement::CreateTable(_)
            | Statement::CreateTableFromSource(_)
            | Statement::CreateSource(_)
            | Statement::CreateSecret(_)
            | Statement::CreateConnection(_) => continue,
            _ => {}
        }

        let hash = compute_typed_hash(typed_obj);
        objects.insert(object_id.clone(), hash);

        // Track which schema this object belongs to.
        // Schemas marked as replacement in the project config get Replacement kind;
        // all others default to Objects.
        let sq = SchemaQualifier::new(object_id.database.clone(), object_id.schema.clone());
        let kind = if planned_project.replacement_schemas.contains(&sq) {
            DeploymentKind::Replacement
        } else {
            DeploymentKind::Objects
        };
        schemas.entry(sq).or_insert(kind);
    }

    Ok(DeploymentSnapshot { objects, schemas })
}

/// Initialize the deployment tracking infrastructure in the database.
///
/// Creates the `deploy` schema, `schema_deployments` table, and `deployment_objects` table
/// if they don't exist. This is idempotent and safe to call multiple times.
pub async fn initialize_deployment_table(client: &Client) -> Result<(), DeploymentSnapshotError> {
    client.deployments().create_deployments().await?;

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
        .deployments()
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
/// * `snapshot` - The deployment snapshot to write (includes per-schema deployment kind)
/// * `deploy_id` - Deploy ID (e.g., "<init>" for direct deploy, "staging" for staged)
/// * `metadata` - Deployment metadata (user, git commit, etc.)
/// * `promoted_at` - Optional promoted_at timestamp (Some(now) for direct apply, None for stage)
pub async fn write_to_database(
    client: &Client,
    snapshot: &DeploymentSnapshot,
    deploy_id: &str,
    metadata: &DeploymentMetadata,
    promoted_at: Option<DateTime<Utc>>,
) -> Result<(), DeploymentSnapshotError> {
    let now = Utc::now();

    // Build schema deployment records (kind is now per-schema from the snapshot)
    let mut schema_records = Vec::new();
    for (sq, kind) in &snapshot.schemas {
        schema_records.push(SchemaDeploymentRecord {
            deploy_id: deploy_id.to_string(),
            database: sq.database.clone(),
            schema: sq.schema.clone(),
            deployed_at: now,
            deployed_by: metadata.deployed_by.clone(),
            promoted_at,
            git_commit: metadata.git_commit.clone(),
            kind: *kind,
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
    client
        .deployments()
        .insert_schema_deployments(&schema_records)
        .await?;
    client
        .deployments()
        .append_deployment_objects(&object_records)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn test_empty_snapshot() {
        let snapshot = DeploymentSnapshot::default();
        assert!(snapshot.objects.is_empty());
    }

    /// Parse a single SQL statement into a typed::DatabaseObject.
    fn make_typed_object(sql: &str) -> typed::DatabaseObject {
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();
        let stmt = match parsed.into_iter().next().unwrap().ast {
            mz_sql_parser::ast::Statement::CreateView(s) => Statement::CreateView(s),
            mz_sql_parser::ast::Statement::CreateMaterializedView(s) => {
                Statement::CreateMaterializedView(s)
            }
            mz_sql_parser::ast::Statement::CreateTable(s) => Statement::CreateTable(s),
            mz_sql_parser::ast::Statement::CreateSource(s) => Statement::CreateSource(s),
            mz_sql_parser::ast::Statement::CreateConnection(s) => Statement::CreateConnection(s),
            mz_sql_parser::ast::Statement::CreateSecret(s) => Statement::CreateSecret(s),
            mz_sql_parser::ast::Statement::CreateSink(s) => Statement::CreateSink(s),
            other => panic!("Unexpected statement type: {:?}", other),
        };
        typed::DatabaseObject {
            stmt,
            indexes: vec![],
            grants: vec![],
            comments: vec![],
            tests: vec![],
        }
    }

    /// Build a planned::Project from (database, schema, name, typed_obj) tuples.
    fn make_planned_project(
        objects: Vec<(&str, &str, &str, typed::DatabaseObject)>,
    ) -> planned::Project {
        let mut db_map: BTreeMap<String, BTreeMap<String, Vec<typed::DatabaseObject>>> =
            BTreeMap::new();
        for (database, schema, _name, typed_obj) in objects {
            db_map
                .entry(database.to_string())
                .or_default()
                .entry(schema.to_string())
                .or_default()
                .push(typed_obj);
        }
        let databases: Vec<typed::Database> = db_map
            .into_iter()
            .map(|(db_name, schemas)| typed::Database {
                name: db_name,
                schemas: schemas
                    .into_iter()
                    .map(|(schema_name, objs)| typed::Schema {
                        name: schema_name,
                        objects: objs,
                        mod_statements: None,
                    })
                    .collect(),
                mod_statements: None,
            })
            .collect();
        let typed_project = typed::Project {
            databases,
            replacement_schemas: BTreeSet::new(),
        };
        planned::Project::from(typed_project)
    }

    #[test]
    fn test_apply_managed_objects_excluded_from_snapshot() {
        let view_obj = make_typed_object("CREATE VIEW my_view AS SELECT 1");
        let mv_obj = make_typed_object("CREATE MATERIALIZED VIEW my_mv IN CLUSTER c AS SELECT 1");
        let table_obj = make_typed_object("CREATE TABLE my_table (id INT)");
        let source_obj = make_typed_object(
            "CREATE SOURCE my_source IN CLUSTER source_cluster FROM LOAD GENERATOR COUNTER",
        );
        let conn_obj =
            make_typed_object("CREATE CONNECTION my_conn TO KAFKA (BROKER 'localhost:9092')");
        let secret_obj = make_typed_object("CREATE SECRET my_secret AS 'hunter2'");

        let planned_project = make_planned_project(vec![
            ("db", "public", "my_view", view_obj),
            ("db", "public", "my_mv", mv_obj),
            ("db", "storage", "my_table", table_obj),
            ("db", "storage", "my_source", source_obj),
            ("db", "storage", "my_conn", conn_obj),
            ("db", "storage", "my_secret", secret_obj),
        ]);

        let snapshot = build_snapshot_from_planned(&planned_project).unwrap();

        // Only the view and MV should be in the snapshot
        let object_names: Vec<&str> = snapshot
            .objects
            .keys()
            .map(|id| id.object.as_str())
            .collect();
        assert_eq!(
            object_names,
            vec!["my_mv", "my_view"],
            "Only views and MVs should appear in snapshot, got: {:?}",
            object_names
        );

        // Apply-managed objects should NOT be present
        for name in &["my_table", "my_source", "my_conn", "my_secret"] {
            assert!(
                !snapshot.objects.keys().any(|id| id.object == *name),
                "{} should not be in the snapshot",
                name
            );
        }

        // Schema tracking should still include the public schema (has view/MV)
        assert!(
            snapshot
                .schemas
                .contains_key(&SchemaQualifier::new("db".into(), "public".into()))
        );
        // Storage schema should NOT be tracked (all its objects are apply-managed)
        assert!(
            !snapshot
                .schemas
                .contains_key(&SchemaQualifier::new("db".into(), "storage".into())),
            "Schema with only apply-managed objects should not appear in snapshot"
        );
    }
}
