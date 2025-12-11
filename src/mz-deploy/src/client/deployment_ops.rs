//! Deployment tracking operations.
//!
//! This module contains methods for managing deployment records in the database,
//! including creating tracking tables, inserting/querying deployment records,
//! and managing deployment lifecycle (staging, promotion, abort).

use crate::client::errors::ConnectionError;
use crate::client::models::{
    ApplyState, ConflictRecord, DeploymentDetails, DeploymentHistoryEntry, DeploymentKind,
    DeploymentMetadata, DeploymentObjectRecord, PendingStatement, SchemaDeploymentRecord,
    StagingDeployment,
};
use crate::project::deployment_snapshot::DeploymentSnapshot;
use crate::project::object_id::ObjectId;
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use tokio_postgres::Client as PgClient;
use tokio_postgres::types::ToSql;

/// Reason why a cluster deployment is failing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureReason {
    /// Cluster has no replicas configured.
    NoReplicas,
    /// All replicas are experiencing repeated OOM kills (3+ in 24h).
    AllReplicasProblematic { problematic: i64, total: i64 },
}

impl fmt::Display for FailureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FailureReason::NoReplicas => write!(f, "no replicas configured"),
            FailureReason::AllReplicasProblematic { problematic, total } => {
                write!(
                    f,
                    "all {} of {} replicas OOM-looping (3+ crashes in 24h)",
                    problematic, total
                )
            }
        }
    }
}

/// Status of a cluster in a staging deployment.
#[derive(Debug, Clone, PartialEq)]
pub enum ClusterDeploymentStatus {
    /// Cluster is fully hydrated and lag is within threshold.
    Ready,
    /// Cluster is still hydrating.
    Hydrating { hydrated: i64, total: i64 },
    /// Cluster is hydrated but lag exceeds threshold.
    Lagging { max_lag_secs: i64 },
    /// Cluster is in a failing state.
    Failing { reason: FailureReason },
}

/// Full status context for a cluster in a staging deployment.
#[derive(Debug, Clone)]
pub struct ClusterStatusContext {
    /// Cluster name (with deployment suffix).
    pub cluster_name: String,
    /// Cluster ID.
    pub cluster_id: String,
    /// Overall status.
    pub status: ClusterDeploymentStatus,
    /// Number of hydrated objects.
    pub hydrated_count: i64,
    /// Total number of objects.
    pub total_count: i64,
    /// Maximum lag in seconds across all objects.
    pub max_lag_secs: i64,
    /// Total number of replicas.
    pub total_replicas: i64,
    /// Number of problematic (OOM-looping) replicas.
    pub problematic_replicas: i64,
}

/// A hydration status update from the SUBSCRIBE stream.
///
/// This represents a single update from the streaming subscription
/// to cluster hydration status. Retractions (mz_diff == -1) are
/// filtered out before yielding these updates.
#[derive(Debug, Clone)]
pub struct HydrationStatusUpdate {
    /// Cluster name (with deployment suffix).
    pub cluster_name: String,
    /// Cluster ID.
    pub cluster_id: String,
    /// Overall status.
    pub status: ClusterDeploymentStatus,
    /// Reason for failure, if status is Failing.
    pub failure_reason: Option<FailureReason>,
    /// Number of hydrated objects.
    pub hydrated_count: i64,
    /// Total number of objects.
    pub total_count: i64,
    /// Maximum lag in seconds across all objects.
    pub max_lag_secs: i64,
    /// Total number of replicas.
    pub total_replicas: i64,
    /// Number of problematic (OOM-looping) replicas.
    pub problematic_replicas: i64,
}

/// Create the deployment tracking database and tables.
///
/// This creates:
/// - `_mz_deploy` database
/// - `_mz_deploy.public.deployments` table for tracking deployment metadata
/// - `_mz_deploy.public.objects` table for tracking deployed objects and their hashes
/// - `_mz_deploy.public.clusters` table for tracking clusters used by deployments
/// - `_mz_deploy.public.pending_statements` table for deferred statements (sinks)
/// - `_mz_deploy.public.production` view for querying current production state
pub async fn create_deployments(client: &PgClient) -> Result<(), ConnectionError> {
    client
        .execute("CREATE DATABASE IF NOT EXISTS _mz_deploy;", &[])
        .await
        .map_err(ConnectionError::Query)?;

    client
        .execute(
            r#"CREATE TABLE IF NOT EXISTS _mz_deploy.public.deployments (
            deploy_id TEXT NOT NULL,
            deployed_at TIMESTAMPTZ NOT NULL,
            promoted_at TIMESTAMPTZ,
            database    TEXT NOT NULL,
            schema      TEXT NOT NULL,
            deployed_by TEXT NOT NULL,
            commit      TEXT,
            kind        TEXT NOT NULL
        ) WITH (
            PARTITION BY (deploy_id, deployed_at, promoted_at)
        );"#,
            &[],
        )
        .await
        .map_err(ConnectionError::Query)?;

    client
        .execute(
            r#"CREATE TABLE IF NOT EXISTS _mz_deploy.public.objects (
            deploy_id TEXT NOT NULL,
            database TEXT NOT NULL,
            schema   TEXT NOT NULL,
            object   TEXT NOT NULL,
            hash     TEXT NOT NULL
        ) WITH (
            PARTITION BY (deploy_id, database, schema)
        );"#,
            &[],
        )
        .await
        .map_err(ConnectionError::Query)?;

    client
        .execute(
            r#"CREATE TABLE IF NOT EXISTS _mz_deploy.public.clusters (
            deploy_id TEXT NOT NULL,
            cluster_id TEXT NOT NULL
        ) WITH (
            PARTITION BY (deploy_id)
        );"#,
            &[],
        )
        .await
        .map_err(ConnectionError::Query)?;

    client
        .execute(
            r#"CREATE TABLE IF NOT EXISTS _mz_deploy.public.pending_statements (
            deploy_id TEXT NOT NULL,
            sequence_num INT NOT NULL,
            database TEXT NOT NULL,
            schema TEXT NOT NULL,
            object TEXT NOT NULL,
            object_hash TEXT NOT NULL,
            statement_sql TEXT NOT NULL,
            statement_kind TEXT NOT NULL,
            executed_at TIMESTAMPTZ
        ) WITH (
            PARTITION BY (deploy_id)
        );"#,
            &[],
        )
        .await
        .map_err(ConnectionError::Query)?;

    client
        .execute(
            r#"
        CREATE VIEW IF NOT EXISTS _mz_deploy.public.production AS
        WITH candidates AS (
            SELECT DISTINCT ON (database, schema) database, schema, deploy_id, promoted_at, commit, kind
            FROM _mz_deploy.public.deployments
            WHERE promoted_at IS NOT NULL
            ORDER BY database, schema, promoted_at DESC
        )

        SELECT c.database, c.schema, c.deploy_id, c.promoted_at, c.commit, c.kind
        FROM candidates c
        JOIN mz_schemas s ON c.schema = s.name
        JOIN mz_databases d ON c.database = d.name;
    "#,
            &[],
        )
        .await
        .map_err(ConnectionError::Query)?;

    Ok(())
}

/// Insert schema deployment records (insert-only, no DELETE).
pub async fn insert_schema_deployments(
    client: &PgClient,
    deployments: &[SchemaDeploymentRecord],
) -> Result<(), ConnectionError> {
    if deployments.is_empty() {
        return Ok(());
    }

    let insert_sql = r#"
        INSERT INTO _mz_deploy.public.deployments
            (deploy_id, database, schema, deployed_at, deployed_by, promoted_at, commit, kind)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8)
    "#;

    for deployment in deployments {
        let kind_str = deployment.kind.to_string();
        client
            .execute(
                insert_sql,
                &[
                    &deployment.deploy_id,
                    &deployment.database,
                    &deployment.schema,
                    &deployment.deployed_at,
                    &deployment.deployed_by,
                    &deployment.promoted_at,
                    &deployment.git_commit,
                    &kind_str,
                ],
            )
            .await
            .map_err(ConnectionError::Query)?;
    }

    Ok(())
}

/// Append deployment object records (insert-only, never update or delete).
pub async fn append_deployment_objects(
    client: &PgClient,
    objects: &[DeploymentObjectRecord],
) -> Result<(), ConnectionError> {
    if objects.is_empty() {
        return Ok(());
    }

    let insert_sql = r#"
        INSERT INTO _mz_deploy.public.objects
            (deploy_id, database, schema, object, hash)
        VALUES
            ($1, $2, $3, $4, $5)
    "#;

    for obj in objects {
        client
            .execute(
                insert_sql,
                &[
                    &obj.deploy_id,
                    &obj.database,
                    &obj.schema,
                    &obj.object,
                    &obj.object_hash,
                ],
            )
            .await
            .map_err(ConnectionError::Query)?;
    }

    Ok(())
}

/// Insert cluster records for a staging deployment.
///
/// Accepts cluster names and resolves them to cluster IDs internally.
/// Fails if any cluster names cannot be resolved (cluster doesn't exist).
pub async fn insert_deployment_clusters(
    client: &PgClient,
    deploy_id: &str,
    clusters: &[String],
) -> Result<(), ConnectionError> {
    if clusters.is_empty() {
        return Ok(());
    }

    // Step 1: Query mz_catalog to get cluster IDs for the given names
    let placeholders: Vec<String> = (1..=clusters.len()).map(|i| format!("${}", i)).collect();
    let placeholders_str = placeholders.join(", ");

    let select_sql = format!(
        "SELECT name, id FROM mz_catalog.mz_clusters WHERE name IN ({})",
        placeholders_str
    );

    #[allow(clippy::as_conversions)]
    let params: Vec<&(dyn ToSql + Sync)> =
        clusters.iter().map(|c| c as &(dyn ToSql + Sync)).collect();

    let rows = client.query(&select_sql, &params).await?;

    // Verify all clusters were found
    if rows.len() != clusters.len() {
        let found_names: BTreeSet<String> = rows.iter().map(|row| row.get("name")).collect();
        let missing: Vec<&str> = clusters
            .iter()
            .filter(|name| !found_names.contains(*name))
            .map(|s| s.as_str())
            .collect();

        return Err(ConnectionError::IntrospectionFailed {
            object_type: "cluster".to_string(),
            source: format!(
                "Failed to resolve cluster names to IDs. The following clusters do not exist: {}",
                missing.join(", ")
            )
            .into(),
        });
    }

    // Step 2: Insert the cluster IDs into _mz_deploy.public.clusters
    let insert_sql = r#"
        INSERT INTO _mz_deploy.public.clusters (deploy_id, cluster_id)
        VALUES ($1, $2)
    "#;

    for row in rows {
        let cluster_id: String = row.get("id");
        client
            .execute(insert_sql, &[&deploy_id, &cluster_id])
            .await
            .map_err(ConnectionError::Query)?;
    }

    Ok(())
}

/// Get cluster names for a staging deployment.
///
/// Returns cluster names by resolving cluster IDs via JOIN with mz_catalog.mz_clusters.
/// If a cluster ID exists in _mz_deploy.public.clusters but the cluster was deleted from the catalog,
/// that cluster will be silently omitted from results.
pub async fn get_deployment_clusters(
    client: &PgClient,
    deploy_id: &str,
) -> Result<Vec<String>, ConnectionError> {
    let query = r#"
        SELECT c.name
        FROM _mz_deploy.public.clusters dc
        JOIN mz_catalog.mz_clusters c ON dc.cluster_id = c.id
        WHERE dc.deploy_id = $1
        ORDER BY c.name
    "#;

    let rows = client
        .query(query, &[&deploy_id])
        .await
        .map_err(ConnectionError::Query)?;

    Ok(rows.iter().map(|row| row.get("name")).collect())
}

/// Validate that all cluster IDs in a deployment still exist in the catalog.
///
/// Returns an error if any cluster IDs in _mz_deploy.public.clusters cannot be resolved
/// to clusters in mz_catalog.mz_clusters (i.e., clusters were deleted).
pub async fn validate_deployment_clusters(
    client: &PgClient,
    deploy_id: &str,
) -> Result<(), ConnectionError> {
    let query = r#"
        SELECT dc.cluster_id
        FROM _mz_deploy.public.clusters dc
        LEFT JOIN mz_catalog.mz_clusters c ON dc.cluster_id = c.id
        WHERE dc.deploy_id = $1 AND c.id IS NULL
    "#;

    let rows = client
        .query(query, &[&deploy_id])
        .await
        .map_err(ConnectionError::Query)?;

    if !rows.is_empty() {
        let missing_ids: Vec<String> = rows.iter().map(|row| row.get("cluster_id")).collect();
        return Err(ConnectionError::IntrospectionFailed {
            object_type: "cluster".to_string(),
            source: format!(
                "Deployment '{}' references {} cluster(s) that no longer exist: {}. \
                 These clusters may have been deleted. Run 'mz-deploy abort {}' to clean up.",
                deploy_id,
                missing_ids.len(),
                missing_ids.join(", "),
                deploy_id
            )
            .into(),
        });
    }

    Ok(())
}

/// Delete cluster records for a staging deployment.
pub async fn delete_deployment_clusters(
    client: &PgClient,
    deploy_id: &str,
) -> Result<(), ConnectionError> {
    client
        .execute(
            "DELETE FROM _mz_deploy.public.clusters WHERE deploy_id = $1",
            &[&deploy_id],
        )
        .await
        .map_err(ConnectionError::Query)?;
    Ok(())
}

/// Update promoted_at timestamp for a staging deployment.
pub async fn update_promoted_at(client: &PgClient, deploy_id: &str) -> Result<(), ConnectionError> {
    let update_sql = r#"
        UPDATE _mz_deploy.public.deployments
        SET promoted_at = NOW()
        WHERE deploy_id = $1
    "#;

    client
        .execute(update_sql, &[&deploy_id])
        .await
        .map_err(ConnectionError::Query)?;
    Ok(())
}

/// Delete all deployment records for a specific deployment.
pub async fn delete_deployment(client: &PgClient, deploy_id: &str) -> Result<(), ConnectionError> {
    client
        .execute(
            "DELETE FROM _mz_deploy.public.deployments WHERE deploy_id = $1",
            &[&deploy_id],
        )
        .await
        .map_err(ConnectionError::Query)?;
    client
        .execute(
            "DELETE FROM _mz_deploy.public.objects WHERE deploy_id = $1",
            &[&deploy_id],
        )
        .await
        .map_err(ConnectionError::Query)?;
    Ok(())
}

/// Get schema deployment records from the database for a specific deployment.
pub async fn get_schema_deployments(
    client: &PgClient,
    deploy_id: Option<&str>,
) -> Result<Vec<SchemaDeploymentRecord>, ConnectionError> {
    let query = if deploy_id.is_none() {
        r#"
            SELECT deploy_id, database, schema,
                   promoted_at as deployed_at,
                   '' as deployed_by,
                   promoted_at,
                   commit,
                   kind
            FROM _mz_deploy.public.production
            ORDER BY database, schema
        "#
    } else {
        r#"
            SELECT deploy_id, database, schema,
                   deployed_at,
                   deployed_by,
                   promoted_at,
                   commit,
                   kind
            FROM _mz_deploy.public.deployments
            WHERE deploy_id = $1
            ORDER BY database, schema
        "#
    };

    let rows = if deploy_id.is_none() {
        client
            .query(query, &[])
            .await
            .map_err(ConnectionError::Query)?
    } else {
        client
            .query(query, &[&deploy_id])
            .await
            .map_err(ConnectionError::Query)?
    };

    let mut records = Vec::new();
    for row in rows {
        let deploy_id: String = row.get("deploy_id");
        let database: String = row.get("database");
        let schema: String = row.get("schema");
        let deployed_at: DateTime<Utc> = row.get("deployed_at");
        let deployed_by: String = row.get("deployed_by");
        let promoted_at: Option<DateTime<Utc>> = row.get("promoted_at");
        let git_commit: Option<String> = row.get("commit");
        let kind_str: String = row.get("kind");

        let kind = kind_str.parse().map_err(|e| {
            ConnectionError::Message(format!("Failed to parse deployment kind: {}", e))
        })?;

        records.push(SchemaDeploymentRecord {
            deploy_id,
            database,
            schema,
            deployed_at,
            deployed_by,
            promoted_at,
            git_commit,
            kind,
        });
    }

    Ok(records)
}

/// Get deployment object records from the database for a specific deployment.
pub async fn get_deployment_objects(
    client: &PgClient,
    deploy_id: Option<&str>,
) -> Result<DeploymentSnapshot, ConnectionError> {
    let query = if deploy_id.is_none() {
        r#"
            SELECT o.database, o.schema, o.object, o.hash
            FROM _mz_deploy.public.objects o
            JOIN _mz_deploy.public.production p
              ON o.database = p.database AND o.schema = p.schema
            WHERE o.deploy_id = p.deploy_id
        "#
    } else {
        r#"
            SELECT database, schema, object, hash
            FROM _mz_deploy.public.objects
            WHERE deploy_id = $1
        "#
    };

    let rows = if deploy_id.is_none() {
        client
            .query(query, &[])
            .await
            .map_err(ConnectionError::Query)?
    } else {
        client
            .query(query, &[&deploy_id])
            .await
            .map_err(ConnectionError::Query)?
    };

    let mut objects = BTreeMap::new();
    let mut schemas = BTreeMap::new();
    for row in rows {
        let database: String = row.get("database");
        let schema: String = row.get("schema");
        let object: String = row.get("object");
        let object_hash: String = row.get("hash");

        let object_id = ObjectId {
            database: database.clone(),
            schema: schema.clone(),
            object,
        };
        objects.insert(object_id, object_hash);
        // Default to Objects kind for snapshots loaded from DB (used for comparison only)
        schemas
            .entry((database, schema))
            .or_insert(DeploymentKind::Objects);
    }

    Ok(DeploymentSnapshot { objects, schemas })
}

/// Get metadata about a deployment for validation.
pub async fn get_deployment_metadata(
    client: &PgClient,
    deploy_id: &str,
) -> Result<Option<DeploymentMetadata>, ConnectionError> {
    let query = r#"
        SELECT deploy_id,
               promoted_at,
               database,
               schema
        FROM _mz_deploy.public.deployments
        WHERE deploy_id = $1
    "#;

    let rows = client
        .query(query, &[&deploy_id])
        .await
        .map_err(ConnectionError::Query)?;

    if rows.is_empty() {
        return Ok(None);
    }

    let first_row = &rows[0];
    let deploy_id: String = first_row.get("deploy_id");
    let promoted_at: Option<DateTime<Utc>> = first_row.get("promoted_at");

    let mut schemas = Vec::new();
    for row in rows {
        let database: String = row.get("database");
        let schema: String = row.get("schema");
        schemas.push((database, schema));
    }

    Ok(Some(DeploymentMetadata {
        deploy_id,
        promoted_at,
        schemas,
    }))
}

/// Get detailed information about a specific deployment.
///
/// Returns deployment details if the deployment exists, or None if not found.
pub async fn get_deployment_details(
    client: &PgClient,
    deploy_id: &str,
) -> Result<Option<DeploymentDetails>, ConnectionError> {
    let query = r#"
        SELECT deploy_id,
               deployed_at,
               promoted_at,
               deployed_by,
               commit,
               kind,
               database,
               schema
        FROM _mz_deploy.public.deployments
        WHERE deploy_id = $1
        ORDER BY database, schema
    "#;

    let rows = client
        .query(query, &[&deploy_id])
        .await
        .map_err(ConnectionError::Query)?;

    if rows.is_empty() {
        return Ok(None);
    }

    let first_row = &rows[0];
    let deployed_at: DateTime<Utc> = first_row.get("deployed_at");
    let promoted_at: Option<DateTime<Utc>> = first_row.get("promoted_at");
    let deployed_by: String = first_row.get("deployed_by");
    let git_commit: Option<String> = first_row.get("commit");
    let kind_str: String = first_row.get("kind");
    let kind: DeploymentKind = kind_str
        .parse()
        .map_err(ConnectionError::Message)?;

    let mut schemas = Vec::new();
    for row in rows {
        let database: String = row.get("database");
        let schema: String = row.get("schema");
        schemas.push((database, schema));
    }

    Ok(Some(DeploymentDetails {
        deployed_at,
        promoted_at,
        deployed_by,
        git_commit,
        kind,
        schemas,
    }))
}

/// List all staging deployments (promoted_at IS NULL), grouped by deploy_id.
///
/// Returns a map from deploy_id to staging deployment details.
pub async fn list_staging_deployments(
    client: &PgClient,
) -> Result<BTreeMap<String, StagingDeployment>, ConnectionError> {
    let query = r#"
        SELECT deploy_id,
               deployed_at,
               deployed_by,
               commit,
               kind,
               database,
               schema
        FROM _mz_deploy.public.deployments
        WHERE promoted_at IS NULL
        ORDER BY deploy_id, database, schema
    "#;

    let rows = client
        .query(query, &[])
        .await
        .map_err(ConnectionError::Query)?;

    let mut deployments: BTreeMap<String, StagingDeployment> = BTreeMap::new();

    for row in rows {
        let deploy_id: String = row.get("deploy_id");
        let deployed_at: DateTime<Utc> = row.get("deployed_at");
        let deployed_by: String = row.get("deployed_by");
        let git_commit: Option<String> = row.get("commit");
        let kind_str: String = row.get("kind");
        let database: String = row.get("database");
        let schema: String = row.get("schema");

        deployments
            .entry(deploy_id)
            .or_insert_with(|| {
                // Parse kind - default to Objects if parsing fails (shouldn't happen)
                let kind = kind_str.parse().unwrap_or(DeploymentKind::Objects);
                StagingDeployment {
                    deployed_at,
                    deployed_by: deployed_by.clone(),
                    git_commit: git_commit.clone(),
                    kind,
                    schemas: Vec::new(),
                }
            })
            .schemas
            .push((database, schema));
    }

    Ok(deployments)
}

/// List deployment history in chronological order (promoted deployments only).
///
/// Returns a vector of deployment history entries ordered by promotion time.
pub async fn list_deployment_history(
    client: &PgClient,
    limit: Option<usize>,
) -> Result<Vec<DeploymentHistoryEntry>, ConnectionError> {
    // We need to limit unique deployments, not individual schema rows
    // First get distinct deployments, then join with schemas
    let query = if let Some(limit) = limit {
        format!(
            r#"
            WITH unique_deployments AS (
                SELECT DISTINCT deploy_id, promoted_at, deployed_by, commit, kind
                FROM _mz_deploy.public.deployments
                WHERE promoted_at IS NOT NULL
                ORDER BY promoted_at DESC
                LIMIT {}
            )
            SELECT d.deploy_id,
                   d.promoted_at,
                   d.deployed_by,
                   d.commit,
                   d.kind,
                   d.database,
                   d.schema
            FROM _mz_deploy.public.deployments d
            JOIN unique_deployments u
              ON d.deploy_id = u.deploy_id
              AND d.promoted_at = u.promoted_at
              AND d.deployed_by = u.deployed_by
            ORDER BY d.promoted_at DESC, d.database, d.schema
        "#,
            limit
        )
    } else {
        r#"
            SELECT deploy_id,
                   promoted_at,
                   deployed_by,
                   commit,
                   kind,
                   database,
                   schema
            FROM _mz_deploy.public.deployments
            WHERE promoted_at IS NOT NULL
            ORDER BY promoted_at DESC, database, schema
        "#
        .to_string()
    };

    let rows = client
        .query(&query, &[])
        .await
        .map_err(ConnectionError::Query)?;

    // Group by (deploy_id, promoted_at, deployed_by, commit, kind)
    let mut deployments: Vec<DeploymentHistoryEntry> = Vec::new();
    let mut current_deploy_id: Option<String> = None;

    for row in rows {
        let deploy_id: String = row.get("deploy_id");
        let promoted_at: DateTime<Utc> = row.get("promoted_at");
        let deployed_by: String = row.get("deployed_by");
        let git_commit: Option<String> = row.get("commit");
        let kind_str: String = row.get("kind");
        let database: String = row.get("database");
        let schema: String = row.get("schema");

        // Check if this is a new deployment or same as current
        if current_deploy_id.as_ref() != Some(&deploy_id) {
            // Parse kind - default to Objects if parsing fails (shouldn't happen)
            let kind = kind_str.parse().unwrap_or(DeploymentKind::Objects);
            // Start a new deployment group
            deployments.push(DeploymentHistoryEntry {
                deploy_id: deploy_id.clone(),
                promoted_at,
                deployed_by,
                git_commit,
                kind,
                schemas: vec![(database, schema)],
            });
            current_deploy_id = Some(deploy_id);
        } else {
            // Add schema to current deployment
            if let Some(last) = deployments.last_mut() {
                last.schemas.push((database, schema));
            }
        }
    }

    Ok(deployments)
}

/// Check for deployment conflicts (schemas updated after deployment started).
pub async fn check_deployment_conflicts(
    client: &PgClient,
    deploy_id: &str,
) -> Result<Vec<ConflictRecord>, ConnectionError> {
    let query = r#"
        SELECT p.database, p.schema, p.deploy_id, p.promoted_at
        FROM _mz_deploy.public.production p
        JOIN _mz_deploy.public.deployments d USING (database, schema)
        WHERE d.deploy_id = $1 AND p.promoted_at > d.deployed_at
    "#;

    let rows = client
        .query(query, &[&deploy_id])
        .await
        .map_err(ConnectionError::Query)?;

    let conflicts = rows
        .iter()
        .map(|row| ConflictRecord {
            database: row.get("database"),
            schema: row.get("schema"),
            deploy_id: row.get("deploy_id"),
            promoted_at: row.get("promoted_at"),
        })
        .collect();

    Ok(conflicts)
}

/// Check if the deployment tracking table exists.
pub async fn deployment_table_exists(client: &PgClient) -> Result<bool, ConnectionError> {
    let query = r#"
        SELECT EXISTS(
            SELECT 1
            FROM mz_catalog.mz_tables t
            JOIN mz_catalog.mz_schemas s ON t.schema_id = s.id
            JOIN mz_catalog.mz_databases d ON s.database_id = d.id
            WHERE t.name = 'deployments'
                AND s.name = 'public'
                AND d.name = '_mz_deploy'
        )
    "#;

    let row = client
        .query_one(query, &[])
        .await
        .map_err(ConnectionError::Query)?;

    Ok(row.get(0))
}

/// Default allowed lag threshold in seconds (5 minutes).
pub const DEFAULT_ALLOWED_LAG_SECS: i64 = 300;

/// Get detailed hydration and health status for clusters in a staging deployment.
///
/// This function checks:
/// - Hydration progress for each cluster
/// - Wallclock lag to determine if data is fresh
/// - Replica health (detecting OOM-looping replicas)
///
/// # Arguments
/// * `client` - Database client
/// * `deploy_id` - Staging deployment ID
/// * `allowed_lag_secs` - Maximum allowed lag in seconds before marking as "lagging"
///
/// # Returns
/// A vector of `ClusterStatusContext` with full status details for each cluster.
pub async fn get_deployment_hydration_status(
    client: &PgClient,
    deploy_id: &str,
    allowed_lag_secs: i64,
) -> Result<Vec<ClusterStatusContext>, ConnectionError> {
    let pattern = format!("%_{}", deploy_id);

    let query = format!(
        r#"
        WITH
        -- Detect problematic replicas: 3+ OOM kills in 24h (subscribe-friendly)
        problematic_replicas AS (
            SELECT replica_id
            FROM mz_internal.mz_cluster_replica_status_history
            WHERE occurred_at + INTERVAL '24 hours' > mz_now()
              AND reason = 'oom-killed'
            GROUP BY replica_id
            HAVING COUNT(*) >= 3
        ),

        -- Cluster health: count total vs problematic replicas
        cluster_health AS (
            SELECT
                c.name AS cluster_name,
                c.id AS cluster_id,
                COUNT(r.id) AS total_replicas,
                COUNT(pr.replica_id) AS problematic_replicas
            FROM mz_clusters c
            LEFT JOIN mz_cluster_replicas r ON c.id = r.cluster_id
            LEFT JOIN problematic_replicas pr ON r.id = pr.replica_id
            WHERE c.name LIKE $1
            GROUP BY c.name, c.id
        ),

        -- Hydration counts per cluster (best replica)
        hydration_counts AS (
            SELECT
                c.name AS cluster_name,
                r.id AS replica_id,
                COUNT(*) FILTER (WHERE mhs.hydrated) AS hydrated,
                COUNT(*) AS total
            FROM mz_clusters c
            JOIN mz_cluster_replicas r ON c.id = r.cluster_id
            LEFT JOIN mz_internal.mz_hydration_statuses mhs ON mhs.replica_id = r.id
            WHERE c.name LIKE $1
            GROUP BY c.name, r.id
        ),

        hydration_best AS (
            SELECT cluster_name, MAX(hydrated) AS hydrated, MAX(total) AS total
            FROM hydration_counts
            GROUP BY cluster_name
        ),

        -- Max lag per cluster using mz_wallclock_global_lag
        cluster_lag AS (
            SELECT
                c.name AS cluster_name,
                MAX(EXTRACT(EPOCH FROM wgl.lag)) AS max_lag_secs
            FROM mz_clusters c
            JOIN mz_cluster_replicas r ON c.id = r.cluster_id
            JOIN mz_internal.mz_hydration_statuses mhs ON mhs.replica_id = r.id
            JOIN mz_internal.mz_wallclock_global_lag wgl ON wgl.object_id = mhs.object_id
            WHERE c.name LIKE $1
            GROUP BY c.name
        )

        SELECT
            ch.cluster_name,
            ch.cluster_id,
            CASE
                WHEN ch.total_replicas = 0 THEN 'failing'
                WHEN ch.total_replicas = ch.problematic_replicas THEN 'failing'
                WHEN COALESCE(hb.hydrated, 0) < COALESCE(hb.total, 0) THEN 'hydrating'
                WHEN COALESCE(cl.max_lag_secs, 0) > {allowed_lag_secs} THEN 'lagging'
                ELSE 'ready'
            END AS status,
            CASE
                WHEN ch.total_replicas = 0 THEN 'no_replicas'
                WHEN ch.total_replicas = ch.problematic_replicas THEN 'all_replicas_problematic'
                ELSE NULL
            END AS failure_reason,
            COALESCE(hb.hydrated, 0) AS hydrated_count,
            COALESCE(hb.total, 0) AS total_count,
            COALESCE(cl.max_lag_secs, 0)::bigint AS max_lag_secs,
            ch.total_replicas,
            ch.problematic_replicas
        FROM cluster_health ch
        LEFT JOIN hydration_best hb ON ch.cluster_name = hb.cluster_name
        LEFT JOIN cluster_lag cl ON ch.cluster_name = cl.cluster_name
    "#,
        allowed_lag_secs = allowed_lag_secs
    );

    let rows = client
        .query(&query, &[&pattern])
        .await
        .map_err(ConnectionError::Query)?;

    let mut results = Vec::new();
    for row in rows {
        let cluster_name: String = row.get("cluster_name");
        let cluster_id: String = row.get("cluster_id");
        let status_str: String = row.get("status");
        let failure_reason: Option<String> = row.get("failure_reason");
        let hydrated_count: i64 = row.get("hydrated_count");
        let total_count: i64 = row.get("total_count");
        let max_lag_secs: i64 = row.get("max_lag_secs");
        let total_replicas: i64 = row.get("total_replicas");
        let problematic_replicas: i64 = row.get("problematic_replicas");

        let status = match status_str.as_str() {
            "ready" => ClusterDeploymentStatus::Ready,
            "hydrating" => ClusterDeploymentStatus::Hydrating {
                hydrated: hydrated_count,
                total: total_count,
            },
            "lagging" => ClusterDeploymentStatus::Lagging { max_lag_secs },
            "failing" => {
                let reason = match failure_reason.as_deref() {
                    Some("no_replicas") => FailureReason::NoReplicas,
                    Some("all_replicas_problematic") => FailureReason::AllReplicasProblematic {
                        problematic: problematic_replicas,
                        total: total_replicas,
                    },
                    _ => FailureReason::NoReplicas, // fallback
                };
                ClusterDeploymentStatus::Failing { reason }
            }
            _ => ClusterDeploymentStatus::Ready, // fallback
        };

        results.push(ClusterStatusContext {
            cluster_name,
            cluster_id,
            status,
            hydrated_count,
            total_count,
            max_lag_secs,
            total_replicas,
            problematic_replicas,
        });
    }

    Ok(results)
}

// =============================================================================
// Apply State Management
// =============================================================================

/// Create apply state schemas with comments for tracking apply progress.
///
/// Creates two schemas in `_mz_deploy`:
/// - `apply_<deploy_id>_pre` with comment 'swapped=false'
/// - `apply_<deploy_id>_post` with comment 'swapped=true'
///
/// The schemas are created first (if they don't exist), then comments are set
/// (if they don't have comments). During the swap transaction, the schemas
/// exchange names, which effectively moves the 'swapped=true' comment to the
/// `_pre` schema.
pub async fn create_apply_state_schemas(
    client: &PgClient,
    deploy_id: &str,
) -> Result<(), ConnectionError> {
    let pre_schema = format!("apply_{}_pre", deploy_id);
    let post_schema = format!("apply_{}_post", deploy_id);

    // Create _pre schema if it doesn't exist
    let create_pre = format!("CREATE SCHEMA IF NOT EXISTS _mz_deploy.{}", pre_schema);
    client
        .execute(&create_pre, &[])
        .await
        .map_err(ConnectionError::Query)?;

    // Create _post schema if it doesn't exist
    let create_post = format!("CREATE SCHEMA IF NOT EXISTS _mz_deploy.{}", post_schema);
    client
        .execute(&create_post, &[])
        .await
        .map_err(ConnectionError::Query)?;

    // Query to check if a schema has a comment (using mz_internal.mz_comments)
    let comment_check_query = r#"
        SELECT c.comment
        FROM mz_catalog.mz_schemas s
        JOIN mz_catalog.mz_databases d ON s.database_id = d.id
        LEFT JOIN mz_internal.mz_comments c ON s.id = c.id
        WHERE s.name = $1 AND d.name = '_mz_deploy'
    "#;

    // Set comment on _pre schema if it doesn't have one
    let rows = client
        .query(comment_check_query, &[&pre_schema])
        .await
        .map_err(ConnectionError::Query)?;

    if !rows.is_empty() {
        let comment: Option<String> = rows[0].get("comment");
        if comment.is_none() {
            let comment_pre = format!(
                "COMMENT ON SCHEMA _mz_deploy.{} IS 'swapped=false'",
                pre_schema
            );
            client
                .execute(&comment_pre, &[])
                .await
                .map_err(ConnectionError::Query)?;
        }
    }

    // Set comment on _post schema if it doesn't have one
    let rows = client
        .query(comment_check_query, &[&post_schema])
        .await
        .map_err(ConnectionError::Query)?;

    if !rows.is_empty() {
        let comment: Option<String> = rows[0].get("comment");
        if comment.is_none() {
            let comment_post = format!(
                "COMMENT ON SCHEMA _mz_deploy.{} IS 'swapped=true'",
                post_schema
            );
            client
                .execute(&comment_post, &[])
                .await
                .map_err(ConnectionError::Query)?;
        }
    }

    Ok(())
}

/// Get the current apply state for a deployment.
///
/// Checks for the existence of `_mz_deploy.apply_<deploy_id>_pre` schema
/// and its comment to determine the state:
/// - Schema doesn't exist → NotStarted
/// - Schema exists with comment 'swapped=false' → PreSwap
/// - Schema exists with comment 'swapped=true' → PostSwap
pub async fn get_apply_state(
    client: &PgClient,
    deploy_id: &str,
) -> Result<ApplyState, ConnectionError> {
    let pre_schema = format!("apply_{}_pre", deploy_id);

    // Query schema existence and comment using mz_internal.mz_comments
    let query = r#"
        SELECT c.comment
        FROM mz_catalog.mz_schemas s
        JOIN mz_catalog.mz_databases d ON s.database_id = d.id
        LEFT JOIN mz_internal.mz_comments c ON s.id = c.id
        WHERE s.name = $1 AND d.name = '_mz_deploy'
    "#;

    let rows = client
        .query(query, &[&pre_schema])
        .await
        .map_err(ConnectionError::Query)?;

    if rows.is_empty() {
        return Ok(ApplyState::NotStarted);
    }

    let comment: Option<String> = rows[0].get("comment");
    match comment.as_deref() {
        Some("swapped=false") => Ok(ApplyState::PreSwap),
        Some("swapped=true") => Ok(ApplyState::PostSwap),
        _ => {
            // Unexpected comment or no comment - treat as not started
            Ok(ApplyState::NotStarted)
        }
    }
}

/// Delete apply state schemas after successful completion.
pub async fn delete_apply_state_schemas(
    client: &PgClient,
    deploy_id: &str,
) -> Result<(), ConnectionError> {
    let pre_schema = format!("apply_{}_pre", deploy_id);
    let post_schema = format!("apply_{}_post", deploy_id);

    // Drop schemas if they exist
    let drop_pre = format!("DROP SCHEMA IF EXISTS _mz_deploy.{}", pre_schema);
    client
        .execute(&drop_pre, &[])
        .await
        .map_err(ConnectionError::Query)?;

    let drop_post = format!("DROP SCHEMA IF EXISTS _mz_deploy.{}", post_schema);
    client
        .execute(&drop_post, &[])
        .await
        .map_err(ConnectionError::Query)?;

    Ok(())
}

// =============================================================================
// Pending Statements Management
// =============================================================================

/// Insert pending statements for deferred execution (e.g., sinks).
pub async fn insert_pending_statements(
    client: &PgClient,
    statements: &[PendingStatement],
) -> Result<(), ConnectionError> {
    if statements.is_empty() {
        return Ok(());
    }

    let insert_sql = r#"
        INSERT INTO _mz_deploy.public.pending_statements
            (deploy_id, sequence_num, database, schema, object, object_hash, statement_sql, statement_kind, executed_at)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    "#;

    for stmt in statements {
        client
            .execute(
                insert_sql,
                &[
                    &stmt.deploy_id,
                    &stmt.sequence_num,
                    &stmt.database,
                    &stmt.schema,
                    &stmt.object,
                    &stmt.object_hash,
                    &stmt.statement_sql,
                    &stmt.statement_kind,
                    &stmt.executed_at,
                ],
            )
            .await
            .map_err(ConnectionError::Query)?;
    }

    Ok(())
}

/// Get pending statements for a deployment that haven't been executed yet.
pub async fn get_pending_statements(
    client: &PgClient,
    deploy_id: &str,
) -> Result<Vec<PendingStatement>, ConnectionError> {
    let query = r#"
        SELECT deploy_id, sequence_num, database, schema, object, object_hash,
               statement_sql, statement_kind, executed_at
        FROM _mz_deploy.public.pending_statements
        WHERE deploy_id = $1 AND executed_at IS NULL
        ORDER BY sequence_num
    "#;

    let rows = client
        .query(query, &[&deploy_id])
        .await
        .map_err(ConnectionError::Query)?;

    let mut statements = Vec::new();
    for row in rows {
        statements.push(PendingStatement {
            deploy_id: row.get("deploy_id"),
            sequence_num: row.get("sequence_num"),
            database: row.get("database"),
            schema: row.get("schema"),
            object: row.get("object"),
            object_hash: row.get("object_hash"),
            statement_sql: row.get("statement_sql"),
            statement_kind: row.get("statement_kind"),
            executed_at: row.get("executed_at"),
        });
    }

    Ok(statements)
}

/// Mark a pending statement as executed.
pub async fn mark_statement_executed(
    client: &PgClient,
    deploy_id: &str,
    sequence_num: i32,
) -> Result<(), ConnectionError> {
    let update_sql = r#"
        UPDATE _mz_deploy.public.pending_statements
        SET executed_at = NOW()
        WHERE deploy_id = $1 AND sequence_num = $2
    "#;

    client
        .execute(update_sql, &[&deploy_id, &sequence_num])
        .await
        .map_err(ConnectionError::Query)?;

    Ok(())
}

/// Delete all pending statements for a deployment.
pub async fn delete_pending_statements(
    client: &PgClient,
    deploy_id: &str,
) -> Result<(), ConnectionError> {
    client
        .execute(
            "DELETE FROM _mz_deploy.public.pending_statements WHERE deploy_id = $1",
            &[&deploy_id],
        )
        .await
        .map_err(ConnectionError::Query)?;

    Ok(())
}
