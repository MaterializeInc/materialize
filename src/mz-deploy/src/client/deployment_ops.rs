//! Deployment tracking operations.
//!
//! This module contains methods for managing deployment records in the database,
//! including creating tracking tables, inserting/querying deployment records,
//! and managing deployment lifecycle (staging, promotion, abort).

use crate::client::errors::ConnectionError;
use crate::client::models::{
    ConflictRecord, DeploymentMetadata, DeploymentObjectRecord, SchemaDeploymentRecord,
};
use crate::project::deployment_snapshot::DeploymentSnapshot;
use crate::project::object_id::ObjectId;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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

/// Create the deployment tracking schemas and tables.
///
/// This creates:
/// - `deploy` schema
/// - `deploy.deployments` table for tracking deployment metadata
/// - `deploy.objects` table for tracking deployed objects and their hashes
/// - `deploy.clusters` table for tracking clusters used by deployments
/// - `deploy.production` view for querying current production state
pub async fn create_deployments(client: &PgClient) -> Result<(), ConnectionError> {
    client
        .execute("CREATE SCHEMA IF NOT EXISTS deploy;", &[])
        .await
        .map_err(ConnectionError::Query)?;

    client
        .execute(
            r#"CREATE TABLE IF NOT EXISTS deploy.deployments (
            deploy_id TEXT NOT NULL,
            deployed_at TIMESTAMP NOT NULL,
            promoted_at TIMESTAMP,
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
            r#"CREATE TABLE IF NOT EXISTS deploy.objects (
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
            r#"CREATE TABLE IF NOT EXISTS deploy.clusters (
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
            r#"
        CREATE VIEW IF NOT EXISTS deploy.production AS
        WITH candidates AS (
            SELECT DISTINCT ON (database, schema) database, schema, deploy_id, promoted_at, commit, kind
            FROM deploy.deployments
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
        INSERT INTO deploy.deployments
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
        INSERT INTO deploy.objects
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

    // Step 2: Insert the cluster IDs into deploy.clusters
    let insert_sql = r#"
        INSERT INTO deploy.clusters (deploy_id, cluster_id)
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
/// If a cluster ID exists in deploy.clusters but the cluster was deleted from the catalog,
/// that cluster will be silently omitted from results.
pub async fn get_deployment_clusters(
    client: &PgClient,
    deploy_id: &str,
) -> Result<Vec<String>, ConnectionError> {
    let query = r#"
        SELECT c.name
        FROM deploy.clusters dc
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
/// Returns an error if any cluster IDs in deploy.clusters cannot be resolved
/// to clusters in mz_catalog.mz_clusters (i.e., clusters were deleted).
pub async fn validate_deployment_clusters(
    client: &PgClient,
    deploy_id: &str,
) -> Result<(), ConnectionError> {
    let query = r#"
        SELECT dc.cluster_id
        FROM deploy.clusters dc
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
            "DELETE FROM deploy.clusters WHERE deploy_id = $1",
            &[&deploy_id],
        )
        .await
        .map_err(ConnectionError::Query)?;
    Ok(())
}

/// Update promoted_at timestamp for a staging deployment.
pub async fn update_promoted_at(client: &PgClient, deploy_id: &str) -> Result<(), ConnectionError> {
    let update_sql = r#"
        UPDATE deploy.deployments
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
            "DELETE FROM deploy.deployments WHERE deploy_id = $1",
            &[&deploy_id],
        )
        .await
        .map_err(ConnectionError::Query)?;
    client
        .execute(
            "DELETE FROM deploy.objects WHERE deploy_id = $1",
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
                   CAST(EXTRACT(EPOCH FROM promoted_at) AS DOUBLE PRECISION) as deployed_at_epoch,
                   '' as deployed_by,
                   CAST(EXTRACT(EPOCH FROM promoted_at) AS DOUBLE PRECISION) as promoted_at_epoch,
                   commit,
                   kind
            FROM deploy.production
            ORDER BY database, schema
        "#
    } else {
        r#"
            SELECT deploy_id, database, schema,
                   CAST(EXTRACT(EPOCH FROM deployed_at) AS DOUBLE PRECISION) as deployed_at_epoch,
                   deployed_by,
                   CAST(EXTRACT(EPOCH FROM promoted_at) AS DOUBLE PRECISION) as promoted_at_epoch,
                   commit,
                   kind
            FROM deploy.deployments
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
        let deployed_at_epoch: f64 = row.get("deployed_at_epoch");
        let deployed_by: String = row.get("deployed_by");
        let promoted_at_epoch: Option<f64> = row.get("promoted_at_epoch");
        let git_commit: Option<String> = row.get("commit");
        let kind_str: String = row.get("kind");

        let deployed_at = UNIX_EPOCH + Duration::from_secs_f64(deployed_at_epoch);
        let promoted_at =
            promoted_at_epoch.map(|epoch| UNIX_EPOCH + Duration::from_secs_f64(epoch));

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
            FROM deploy.objects o
            JOIN deploy.production p
              ON o.database = p.database AND o.schema = p.schema
            WHERE o.deploy_id = p.deploy_id
        "#
    } else {
        r#"
            SELECT database, schema, object, hash
            FROM deploy.objects
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
    let mut schemas = BTreeSet::new();
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
        schemas.insert((database, schema));
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
               CAST(EXTRACT(EPOCH FROM promoted_at) AS DOUBLE PRECISION) as promoted_at_epoch,
               database,
               schema
        FROM deploy.deployments
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
    let promoted_at_epoch: Option<f64> = first_row.get("promoted_at_epoch");
    let promoted_at = promoted_at_epoch.map(|epoch| UNIX_EPOCH + Duration::from_secs_f64(epoch));

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

/// List all staging deployments (promoted_at IS NULL), grouped by deploy_id.
///
/// Returns a map from deploy_id to list of (database, schema) tuples and deployment metadata.
pub async fn list_staging_deployments(
    client: &PgClient,
) -> Result<
    BTreeMap<
        String,
        (
            SystemTime,
            String,
            Option<String>,
            String,
            Vec<(String, String)>,
        ),
    >,
    ConnectionError,
> {
    let query = r#"
        SELECT deploy_id,
               CAST(EXTRACT(EPOCH FROM deployed_at) AS DOUBLE PRECISION) as deployed_at_epoch,
               deployed_by,
               commit,
               kind,
               database,
               schema
        FROM deploy.deployments
        WHERE promoted_at IS NULL
        ORDER BY deploy_id, database, schema
    "#;

    let rows = client
        .query(query, &[])
        .await
        .map_err(ConnectionError::Query)?;

    let mut deployments: BTreeMap<
        String,
        (
            SystemTime,
            String,
            Option<String>,
            String,
            Vec<(String, String)>,
        ),
    > = BTreeMap::new();

    for row in rows {
        let deploy_id: String = row.get("deploy_id");
        let deployed_at_epoch: f64 = row.get("deployed_at_epoch");
        let deployed_by: String = row.get("deployed_by");
        let commit: Option<String> = row.get("commit");
        let kind: String = row.get("kind");
        let database: String = row.get("database");
        let schema: String = row.get("schema");

        let deployed_at = UNIX_EPOCH + Duration::from_secs_f64(deployed_at_epoch);

        deployments
            .entry(deploy_id)
            .or_insert_with(|| {
                (
                    deployed_at,
                    deployed_by.clone(),
                    commit.clone(),
                    kind.clone(),
                    Vec::new(),
                )
            })
            .4
            .push((database, schema));
    }

    Ok(deployments)
}

/// List deployment history in chronological order (promoted deployments only).
///
/// Returns a vector of tuples containing (deploy_id, promoted_at, deployed_by, commit, kind, schemas),
/// representing complete deployments ordered by promotion time.
#[allow(clippy::type_complexity)]
pub async fn list_deployment_history(
    client: &PgClient,
    limit: Option<usize>,
) -> Result<
    Vec<(
        String,
        SystemTime,
        String,
        Option<String>,
        String,
        Vec<(String, String)>,
    )>,
    ConnectionError,
> {
    // We need to limit unique deployments, not individual schema rows
    // First get distinct deployments, then join with schemas
    let query = if let Some(limit) = limit {
        format!(
            r#"
            WITH unique_deployments AS (
                SELECT DISTINCT deploy_id, promoted_at, deployed_by, commit, kind
                FROM deploy.deployments
                WHERE promoted_at IS NOT NULL
                ORDER BY promoted_at DESC
                LIMIT {}
            )
            SELECT d.deploy_id,
                   CAST(EXTRACT(EPOCH FROM d.promoted_at) AS DOUBLE PRECISION) as promoted_at_epoch,
                   d.deployed_by,
                   d.commit,
                   d.kind,
                   d.database,
                   d.schema
            FROM deploy.deployments d
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
                   CAST(EXTRACT(EPOCH FROM promoted_at) AS DOUBLE PRECISION) as promoted_at_epoch,
                   deployed_by,
                   commit,
                   kind,
                   database,
                   schema
            FROM deploy.deployments
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
    let mut deployments: Vec<(
        String,
        SystemTime,
        String,
        Option<String>,
        String,
        Vec<(String, String)>,
    )> = Vec::new();
    let mut current_key: Option<(String, SystemTime, String, Option<String>, String)> = None;

    for row in rows {
        let deploy_id: String = row.get("deploy_id");
        let promoted_at_epoch: f64 = row.get("promoted_at_epoch");
        let deployed_by: String = row.get("deployed_by");
        let commit: Option<String> = row.get("commit");
        let kind: String = row.get("kind");
        let database: String = row.get("database");
        let schema: String = row.get("schema");

        let promoted_at = UNIX_EPOCH + Duration::from_secs_f64(promoted_at_epoch);
        let key = (
            deploy_id.clone(),
            promoted_at,
            deployed_by.clone(),
            commit.clone(),
            kind.clone(),
        );

        // Check if this is a new deployment or same as current
        if current_key.as_ref() != Some(&key) {
            // Start a new deployment group
            deployments.push((
                deploy_id,
                promoted_at,
                deployed_by,
                commit,
                kind,
                vec![(database, schema)],
            ));
            current_key = Some(key);
        } else {
            // Add schema to current deployment
            if let Some(last) = deployments.last_mut() {
                last.5.push((database, schema));
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
        SELECT p.database, p.schema, p.deploy_id,
               CAST(EXTRACT(EPOCH FROM p.promoted_at) AS DOUBLE PRECISION) as promoted_at_epoch
        FROM deploy.production p
        JOIN deploy.deployments d USING (database, schema)
        WHERE d.deploy_id = $1 AND p.promoted_at > d.deployed_at
    "#;

    let rows = client
        .query(query, &[&deploy_id])
        .await
        .map_err(ConnectionError::Query)?;

    let conflicts = rows
        .iter()
        .map(|row| {
            let promoted_at_epoch: f64 = row.get("promoted_at_epoch");
            let promoted_at = UNIX_EPOCH + Duration::from_secs_f64(promoted_at_epoch);

            ConflictRecord {
                database: row.get("database"),
                schema: row.get("schema"),
                deploy_id: row.get("deploy_id"),
                promoted_at,
            }
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
                AND s.name = 'deploy'
                AND d.name = 'materialize'
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
