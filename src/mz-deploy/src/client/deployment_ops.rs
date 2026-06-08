// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Deployment tracking operations.
//!
//! This module contains methods for managing deployment records in the database,
//! including creating tracking tables, inserting/querying deployment records,
//! and managing deployment lifecycle (staging, promotion, abort).
//!
//! ## Hydration State Machine
//!
//! During staging, clusters transition through the following states:
//!
//! ```text
//!   ┌──────────┐    all objects hydrated     ┌───────┐
//!   │ Hydrating │ ────────────────────────▶  │ Ready │
//!   └──────────┘    & lag ≤ threshold         └───────┘
//!        │                                       ▲
//!        │         lag > threshold          ┌─────────┐
//!        └────────────────────────────────▶ │ Lagging │
//!                                           └─────────┘
//!        │
//!        ▼ (no replicas OR all replicas OOM-looping)
//!   ┌─────────┐
//!   │ Failing  │
//!   └─────────┘
//! ```
//!
//! - **Hydrating** → objects are being backfilled; progress tracked as
//!   `hydrated / total` via `mz_internal.mz_hydration_statuses`.
//! - **Ready** → all objects hydrated and max wallclock lag ≤ threshold
//!   (default 300s / 5 minutes).
//! - **Lagging** → all objects hydrated but wallclock lag exceeds threshold.
//! - **Failing** → no replicas configured, or all replicas are problematic
//!   (3+ OOM kills within 24 hours per `mz_cluster_replica_status_history`).
//!
//! ## Apply State Tracking
//!
//! The apply (cutover) process uses a pair of schemas in `_mz_deploy` as a
//! state marker: `apply_<id>_pre` and `apply_<id>_post`. During the atomic
//! swap transaction these schemas exchange names, moving the `swapped=true`
//! comment to the `_pre` schema. This enables crash recovery:
//! - Schema absent → `NotStarted`
//! - `_pre` comment = `swapped=false` → `PreSwap` (resume pre-swap work)
//! - `_pre` comment = `swapped=true` → `PostSwap` (resume post-swap work)
//!
//! ## SUBSCRIBE Streaming
//!
//! `subscribe_deployment_hydration` opens a `SUBSCRIBE` cursor over the
//! hydration status query. Retractions (`mz_diff == -1`) are filtered out
//! before yielding updates — only insertions are surfaced to the caller.

use crate::client::connection::{Client, DeploymentsClient, DeploymentsClientMut};
use crate::client::errors::ConnectionError;
use crate::client::models::{
    ApplyState, ConflictRecord, DeploymentDetails, DeploymentHistoryEntry, DeploymentKind,
    DeploymentMetadata, DeploymentMode, DeploymentObjectRecord, PendingStatement,
    ProductionClusterRecord, SchemaDeploymentRecord, StagingDeployment,
};
use crate::client::quote_identifier;
use crate::project::SchemaQualifier;
use crate::project::analysis::deployment_snapshot::DeploymentSnapshot;
use crate::project::ir::object_id::ObjectId;
use async_stream::try_stream;
use chrono::{DateTime, Utc};
use futures::Stream;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use tokio_postgres::types::ToSql;

/// Reason why a cluster deployment is failing.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, Clone, serde::Serialize)]
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

/// Insert schema deployment records (insert-only, no DELETE).
pub(super) async fn insert_schema_deployments(
    client: &Client,
    deployments: &[SchemaDeploymentRecord],
) -> Result<(), ConnectionError> {
    if deployments.is_empty() {
        return Ok(());
    }

    let insert_sql = r#"
        INSERT INTO _mz_deploy.tables.deployments
            (deploy_id, database, schema, deployed_at, deployed_by, promoted_at, commit, kind, mode)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    "#;

    for deployment in deployments {
        let kind_str = deployment.kind.to_string();
        let mode_str = deployment.mode.to_string();
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
                    &mode_str,
                ],
            )
            .await?;
    }

    Ok(())
}

/// Append deployment object records (insert-only, never update or delete).
pub(super) async fn append_deployment_objects(
    client: &Client,
    objects: &[DeploymentObjectRecord],
) -> Result<(), ConnectionError> {
    if objects.is_empty() {
        return Ok(());
    }

    let insert_sql = r#"
        INSERT INTO _mz_deploy.tables.objects
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
            .await?;
    }

    Ok(())
}

/// Insert cluster records for a staging deployment.
///
/// Accepts cluster names and resolves them to cluster IDs internally.
/// Fails if any cluster names cannot be resolved (cluster doesn't exist).
pub(super) async fn insert_deployment_clusters(
    client: &Client,
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

    // Step 2: Insert the cluster IDs into _mz_deploy.tables.clusters
    let insert_sql = r#"
        INSERT INTO _mz_deploy.tables.clusters (deploy_id, cluster_id)
        VALUES ($1, $2)
    "#;

    for row in rows {
        let cluster_id: String = row.get("id");
        client
            .execute(insert_sql, &[&deploy_id, &cluster_id])
            .await?;
    }

    Ok(())
}

/// Get cluster names for a staging deployment.
///
/// Returns cluster names by resolving cluster IDs via JOIN with mz_catalog.mz_clusters.
/// If a cluster ID exists in _mz_deploy.public.clusters but the cluster was deleted from the catalog,
/// that cluster will be silently omitted from results.
pub(super) async fn get_deployment_clusters(
    client: &Client,
    deploy_id: &str,
) -> Result<Vec<String>, ConnectionError> {
    let query = r#"
        SELECT name
        FROM _mz_deploy.public.deployment_clusters
        WHERE deploy_id = $1
        ORDER BY name
    "#;

    let rows = client.query(query, &[&deploy_id]).await?;

    Ok(rows.iter().map(|row| row.get("name")).collect())
}

/// Validate that all cluster IDs in a deployment still exist in the catalog.
///
/// Returns an error if any cluster IDs in _mz_deploy.public.clusters cannot be resolved
/// to clusters in mz_catalog.mz_clusters (i.e., clusters were deleted).
pub(super) async fn validate_deployment_clusters(
    client: &Client,
    deploy_id: &str,
) -> Result<(), ConnectionError> {
    let query = r#"
        SELECT cluster_id
        FROM _mz_deploy.public.missing_clusters
        WHERE deploy_id = $1
    "#;

    let rows = client.query(query, &[&deploy_id]).await?;

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
pub(super) async fn delete_deployment_clusters(
    client: &Client,
    deploy_id: &str,
) -> Result<(), ConnectionError> {
    client
        .execute(
            "DELETE FROM _mz_deploy.tables.clusters WHERE deploy_id = $1",
            &[&deploy_id],
        )
        .await?;
    Ok(())
}

/// Update promoted_at timestamp for a staging deployment.
pub(super) async fn update_promoted_at(
    client: &Client,
    deploy_id: &str,
) -> Result<(), ConnectionError> {
    let update_sql = r#"
        UPDATE _mz_deploy.tables.deployments
        SET promoted_at = NOW()
        WHERE deploy_id = $1
    "#;

    client.execute(update_sql, &[&deploy_id]).await?;
    Ok(())
}

/// Delete all deployment records for a specific deployment.
pub(super) async fn delete_deployment(
    client: &Client,
    deploy_id: &str,
) -> Result<(), ConnectionError> {
    client
        .execute(
            "DELETE FROM _mz_deploy.tables.deployments WHERE deploy_id = $1",
            &[&deploy_id],
        )
        .await?;
    client
        .execute(
            "DELETE FROM _mz_deploy.tables.objects WHERE deploy_id = $1",
            &[&deploy_id],
        )
        .await?;
    Ok(())
}

/// Get schema deployment records from the database for a specific deployment.
pub(super) async fn get_schema_deployments(
    client: &Client,
    deploy_id: Option<&str>,
) -> Result<Vec<SchemaDeploymentRecord>, ConnectionError> {
    // The production view does not expose a mode column, so the no-filter
    // branch hardcodes `'stage'` as a schema-compatibility placeholder. Every
    // row from the production view is by definition a promoted deployment
    // (non-null `promoted_at`); no caller of `SchemaDeploymentRecord` branches
    // on `mode` when the record represents a promotion. The filtered branch
    // reads `mode` from `_mz_deploy.public.deployments` normally.
    let query = if deploy_id.is_none() {
        r#"
            SELECT deploy_id, database, schema,
                   promoted_at as deployed_at,
                   '' as deployed_by,
                   promoted_at,
                   commit,
                   kind,
                   'stage' as mode
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
                   kind,
                   mode
            FROM _mz_deploy.public.deployments
            WHERE deploy_id = $1
            ORDER BY database, schema
        "#
    };

    let rows = if deploy_id.is_none() {
        client.query(query, &[]).await?
    } else {
        client.query(query, &[&deploy_id]).await?
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
        let mode_str: String = row.get("mode");

        let kind = kind_str.parse().map_err(|e| {
            ConnectionError::Message(format!("Failed to parse deployment kind: {}", e))
        })?;
        let mode = mode_str.parse::<DeploymentMode>().map_err(|e| {
            ConnectionError::Message(format!("Failed to parse deployment mode: {}", e))
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
            mode,
        });
    }

    Ok(records)
}

/// Get deployment object records from the database for a specific deployment.
pub(super) async fn get_deployment_objects(
    client: &Client,
    deploy_id: Option<&str>,
) -> Result<DeploymentSnapshot, ConnectionError> {
    let query = if deploy_id.is_none() {
        r#"
            SELECT o.database, o.schema, o.object, o.hash, p.kind
            FROM _mz_deploy.public.objects o
            JOIN _mz_deploy.public.production p
              ON o.database = p.database AND o.schema = p.schema
            WHERE o.deploy_id = p.deploy_id
        "#
    } else {
        r#"
            SELECT o.database, o.schema, o.object, o.hash, d.kind
            FROM _mz_deploy.public.objects o
            JOIN _mz_deploy.public.deployments d
              ON o.deploy_id = d.deploy_id
                AND o.database = d.database
                AND o.schema = d.schema
            WHERE o.deploy_id = $1
        "#
    };

    let rows = if deploy_id.is_none() {
        client.query(query, &[]).await?
    } else {
        client.query(query, &[&deploy_id]).await?
    };

    let mut objects = BTreeMap::new();
    let mut schemas = BTreeMap::new();
    for row in rows {
        let database: String = row.get("database");
        let schema: String = row.get("schema");
        let object: String = row.get("object");
        let object_hash: String = row.get("hash");

        let kind_str: String = row.get("kind");
        let kind = kind_str.parse().map_err(|e| {
            ConnectionError::Message(format!("Failed to parse deployment kind: {}", e))
        })?;

        let object_id = ObjectId::new(database.clone(), schema.clone(), object);
        objects.insert(object_id, object_hash);
        schemas
            .entry(SchemaQualifier::new(database, schema))
            .or_insert(kind);
    }

    Ok(DeploymentSnapshot { objects, schemas })
}

/// Get metadata about a deployment for validation.
pub(super) async fn get_deployment_metadata(
    client: &Client,
    deploy_id: &str,
) -> Result<Option<DeploymentMetadata>, ConnectionError> {
    let query = r#"
        SELECT deploy_id,
               promoted_at,
               mode,
               database,
               schema
        FROM _mz_deploy.public.deployments
        WHERE deploy_id = $1
    "#;

    let rows = client.query(query, &[&deploy_id]).await?;

    if rows.is_empty() {
        return Ok(None);
    }

    let first_row = &rows[0];
    let deploy_id: String = first_row.get("deploy_id");
    let promoted_at: Option<DateTime<Utc>> = first_row.get("promoted_at");
    let mode_str: String = first_row.get("mode");
    let mode = mode_str
        .parse::<DeploymentMode>()
        .map_err(|e| ConnectionError::Message(format!("Failed to parse deployment mode: {}", e)))?;

    let mut schemas = Vec::new();
    for row in rows {
        let database: String = row.get("database");
        let schema: String = row.get("schema");
        schemas.push(SchemaQualifier::new(database, schema));
    }

    Ok(Some(DeploymentMetadata {
        deploy_id,
        promoted_at,
        mode,
        schemas,
    }))
}

/// Get detailed information about a specific deployment.
///
/// Returns deployment details if the deployment exists, or None if not found.
pub(super) async fn get_deployment_details(
    client: &Client,
    deploy_id: &str,
) -> Result<Option<DeploymentDetails>, ConnectionError> {
    let query = r#"
        SELECT deploy_id,
               deployed_at,
               promoted_at,
               deployed_by,
               commit,
               kind,
               mode,
               database,
               schema
        FROM _mz_deploy.public.deployments
        WHERE deploy_id = $1
        ORDER BY database, schema
    "#;

    let rows = client.query(query, &[&deploy_id]).await?;

    if rows.is_empty() {
        return Ok(None);
    }

    let first_row = &rows[0];
    let deployed_at: DateTime<Utc> = first_row.get("deployed_at");
    let promoted_at: Option<DateTime<Utc>> = first_row.get("promoted_at");
    let deployed_by: String = first_row.get("deployed_by");
    let git_commit: Option<String> = first_row.get("commit");
    let kind_str: String = first_row.get("kind");
    let kind: DeploymentKind = kind_str.parse().map_err(ConnectionError::Message)?;
    let mode_str: String = first_row.get("mode");
    let mode = mode_str
        .parse::<DeploymentMode>()
        .map_err(|e| ConnectionError::Message(format!("Failed to parse deployment mode: {}", e)))?;

    let mut schemas = Vec::new();
    for row in rows {
        let database: String = row.get("database");
        let schema: String = row.get("schema");
        schemas.push(SchemaQualifier::new(database, schema));
    }

    Ok(Some(DeploymentDetails {
        deployed_at,
        promoted_at,
        deployed_by,
        git_commit,
        kind,
        mode,
        schemas,
    }))
}

/// List all staging deployments (promoted_at IS NULL), grouped by deploy_id.
///
/// Returns a map from deploy_id to staging deployment details.
pub(super) async fn list_staging_deployments(
    client: &Client,
) -> Result<BTreeMap<String, StagingDeployment>, ConnectionError> {
    let query = r#"
        SELECT deploy_id,
               deployed_at,
               deployed_by,
               commit,
               kind,
               mode,
               database,
               schema
        FROM _mz_deploy.public.staging_deployments
        ORDER BY deploy_id, database, schema
    "#;

    let rows = client.query(query, &[]).await?;

    let mut deployments: BTreeMap<String, StagingDeployment> = BTreeMap::new();

    for row in rows {
        let deploy_id: String = row.get("deploy_id");
        let deployed_at: DateTime<Utc> = row.get("deployed_at");
        let deployed_by: String = row.get("deployed_by");
        let git_commit: Option<String> = row.get("commit");
        let kind_str: String = row.get("kind");
        let mode_str: String = row.get("mode");
        let database: String = row.get("database");
        let schema: String = row.get("schema");

        // Parse outside the closure so errors propagate instead of silently
        // defaulting — a garbled `kind` or `mode` column is data corruption
        // and should surface loud.
        let kind: DeploymentKind = kind_str.parse().map_err(|e| {
            ConnectionError::Message(format!("Failed to parse deployment kind: {}", e))
        })?;
        let mode: DeploymentMode = mode_str.parse().map_err(|e| {
            ConnectionError::Message(format!("Failed to parse deployment mode: {}", e))
        })?;

        deployments
            .entry(deploy_id)
            .or_insert_with(|| StagingDeployment {
                deployed_at,
                deployed_by: deployed_by.clone(),
                git_commit: git_commit.clone(),
                kind,
                mode,
                schemas: Vec::new(),
            })
            .schemas
            .push(SchemaQualifier::new(database, schema));
    }

    Ok(deployments)
}

/// List deployment history in chronological order (promoted deployments only).
///
/// Returns a vector of deployment history entries ordered by promotion time.
pub(super) async fn list_deployment_history(
    client: &Client,
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

    let rows = client.query(&query, &[]).await?;

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
                schemas: vec![SchemaQualifier::new(database, schema)],
            });
            current_deploy_id = Some(deploy_id);
        } else {
            // Add schema to current deployment
            if let Some(last) = deployments.last_mut() {
                last.schemas.push(SchemaQualifier::new(database, schema));
            }
        }
    }

    Ok(deployments)
}

/// Check for deployment conflicts (schemas updated after deployment started).
pub(super) async fn check_deployment_conflicts(
    client: &Client,
    deploy_id: &str,
) -> Result<Vec<ConflictRecord>, ConnectionError> {
    let query = r#"
        SELECT p.database, p.schema, p.deploy_id, p.promoted_at
        FROM _mz_deploy.public.production p
        JOIN _mz_deploy.public.deployments d USING (database, schema)
        WHERE d.deploy_id = $1 AND p.promoted_at > d.deployed_at
    "#;

    let rows = client.query(query, &[&deploy_id]).await?;

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

/// List clusters that host at least one promoted deployment.
///
/// For each such cluster, returns one representative protected deployment
/// (the most recently promoted one) so callers can explain *why* the
/// cluster is classified production. Used by `dev` to refuse overlay
/// deployments that would land on production compute.
pub(super) async fn list_production_clusters(
    client: &Client,
) -> Result<Vec<ProductionClusterRecord>, ConnectionError> {
    // Walk the catalog rather than reading `_mz_deploy.tables.clusters`
    // directly. Two reasons: (1) the promote swap renames the staging
    // cluster into place, changing the `mz_clusters.id` of the production
    // cluster — any id we stored at stage time is stale. (2) `mz_catalog`
    // is readable by every role, while `_mz_deploy.tables.*` is only
    // readable by `materialize_deployer`, so this query works for the
    // `dev` guard run as `materialize_developer`.
    let query = r#"
        SELECT DISTINCT ON (c.name)
            c.name         AS cluster_name,
            p.database     AS database,
            p.schema       AS schema,
            p.promoted_at  AS promoted_at
        FROM _mz_deploy.public.production p
        JOIN mz_catalog.mz_databases d ON d.name = p.database
        JOIN mz_catalog.mz_schemas s
          ON s.name = p.schema AND s.database_id = d.id
        JOIN mz_catalog.mz_objects o
          ON o.schema_id = s.id AND o.cluster_id IS NOT NULL
        JOIN mz_catalog.mz_clusters c ON c.id = o.cluster_id
        ORDER BY c.name, p.promoted_at DESC
    "#;

    let rows = client.query(query, &[]).await?;
    let records = rows
        .iter()
        .map(|row| ProductionClusterRecord {
            cluster_name: row.get("cluster_name"),
            database: row.get("database"),
            schema: row.get("schema"),
            promoted_at: row.get("promoted_at"),
        })
        .collect();
    Ok(records)
}

/// Check if the deployment tracking table exists.
pub(super) async fn deployment_table_exists(client: &Client) -> Result<bool, ConnectionError> {
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

    let row = client.query_one(query, &[]).await?;

    Ok(row.get(0))
}

/// Default allowed lag threshold in seconds (5 minutes).
pub const DEFAULT_ALLOWED_LAG_SECS: i64 = 300;

/// Build the shared hydration-status SQL query.
///
/// Both `get_deployment_hydration_status` (one-shot SELECT) and
/// `subscribe_deployment_hydration` (SUBSCRIBE) use the same CTE logic.
/// The query expects a single `$1` parameter for the cluster name LIKE pattern.
fn hydration_status_query(allowed_lag_secs: i64) -> String {
    format!(
        r#"
        WITH
        problematic_replicas AS (
            SELECT replica_id
            FROM mz_internal.mz_cluster_replica_status_history
            WHERE occurred_at + INTERVAL '24 hours' > mz_now()
              AND reason = 'oom-killed'
            GROUP BY replica_id
            HAVING COUNT(*) >= 3
        ),
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
    )
}

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
pub(super) async fn get_deployment_hydration_status(
    client: &Client,
    deploy_id: &str,
    allowed_lag_secs: i64,
) -> Result<Vec<ClusterStatusContext>, ConnectionError> {
    let pattern = format!("%_{}", deploy_id);
    let query = hydration_status_query(allowed_lag_secs);
    let rows = client.query(&query, &[&pattern]).await?;

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
pub(super) async fn create_apply_state_schemas(
    client: &Client,
    deploy_id: &str,
) -> Result<(), ConnectionError> {
    let pre_schema = format!("apply_{}_pre", deploy_id);
    let post_schema = format!("apply_{}_post", deploy_id);
    let pre_schema_quoted = quote_identifier(&pre_schema);
    let post_schema_quoted = quote_identifier(&post_schema);

    let create_pre = format!(
        "CREATE SCHEMA IF NOT EXISTS _mz_deploy.{}",
        pre_schema_quoted
    );
    client.execute(&create_pre, &[]).await?;

    let create_post = format!(
        "CREATE SCHEMA IF NOT EXISTS _mz_deploy.{}",
        post_schema_quoted
    );
    client.execute(&create_post, &[]).await?;

    let comment_check_query = r#"
        SELECT c.comment
        FROM mz_catalog.mz_schemas s
        JOIN mz_catalog.mz_databases d ON s.database_id = d.id
        LEFT JOIN mz_internal.mz_comments c ON s.id = c.id
        WHERE s.name = $1 AND d.name = '_mz_deploy'
    "#;

    let rows = client.query(comment_check_query, &[&pre_schema]).await?;
    if !rows.is_empty() {
        let comment: Option<String> = rows[0].get("comment");
        if comment.is_none() {
            let comment_pre = format!(
                "COMMENT ON SCHEMA _mz_deploy.{} IS 'swapped=false'",
                pre_schema_quoted
            );
            client.execute(&comment_pre, &[]).await?;
        }
    }

    let rows = client.query(comment_check_query, &[&post_schema]).await?;
    if !rows.is_empty() {
        let comment: Option<String> = rows[0].get("comment");
        if comment.is_none() {
            let comment_post = format!(
                "COMMENT ON SCHEMA _mz_deploy.{} IS 'swapped=true'",
                post_schema_quoted
            );
            client.execute(&comment_post, &[]).await?;
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
pub(super) async fn get_apply_state(
    client: &Client,
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

    let rows = client.query(query, &[&pre_schema]).await?;

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
pub(super) async fn delete_apply_state_schemas(
    client: &Client,
    deploy_id: &str,
) -> Result<(), ConnectionError> {
    let pre_schema = format!("apply_{}_pre", deploy_id);
    let post_schema = format!("apply_{}_post", deploy_id);

    let drop_pre = format!(
        "DROP SCHEMA IF EXISTS _mz_deploy.{}",
        quote_identifier(&pre_schema)
    );
    client.execute(&drop_pre, &[]).await?;

    let drop_post = format!(
        "DROP SCHEMA IF EXISTS _mz_deploy.{}",
        quote_identifier(&post_schema)
    );
    client.execute(&drop_post, &[]).await?;

    Ok(())
}

/// Insert pending statements for deferred execution (e.g., sinks).
pub(super) async fn insert_pending_statements(
    client: &Client,
    statements: &[PendingStatement],
) -> Result<(), ConnectionError> {
    if statements.is_empty() {
        return Ok(());
    }

    let insert_sql = r#"
        INSERT INTO _mz_deploy.tables.pending_statements
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
            .await?;
    }

    Ok(())
}

/// Get pending statements for a deployment that haven't been executed yet.
pub(super) async fn get_pending_statements(
    client: &Client,
    deploy_id: &str,
) -> Result<Vec<PendingStatement>, ConnectionError> {
    let query = r#"
        SELECT deploy_id, sequence_num, database, schema, object, object_hash,
               statement_sql, statement_kind, executed_at
        FROM _mz_deploy.public.pending_statements
        WHERE deploy_id = $1 AND executed_at IS NULL
        ORDER BY sequence_num
    "#;

    let rows = client.query(query, &[&deploy_id]).await?;

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
pub(super) async fn mark_statement_executed(
    client: &Client,
    deploy_id: &str,
    sequence_num: i32,
) -> Result<(), ConnectionError> {
    let update_sql = r#"
        UPDATE _mz_deploy.tables.pending_statements
        SET executed_at = NOW()
        WHERE deploy_id = $1 AND sequence_num = $2
    "#;

    client
        .execute(update_sql, &[&deploy_id, &sequence_num])
        .await?;

    Ok(())
}

/// Delete all pending statements for a deployment.
pub(super) async fn delete_pending_statements(
    client: &Client,
    deploy_id: &str,
) -> Result<(), ConnectionError> {
    client
        .execute(
            "DELETE FROM _mz_deploy.tables.pending_statements WHERE deploy_id = $1",
            &[&deploy_id],
        )
        .await?;

    Ok(())
}

/// Insert replacement MV records for a deployment.
pub(super) async fn insert_replacement_mvs(
    client: &Client,
    records: &[super::models::ReplacementMvRecord],
) -> Result<(), ConnectionError> {
    for record in records {
        client
            .execute(
                r#"INSERT INTO _mz_deploy.tables.replacement_mvs
                   (deploy_id, target_database, target_schema, target_name,
                    replacement_schema)
                   VALUES ($1, $2, $3, $4, $5)"#,
                &[
                    &record.deploy_id,
                    &record.target_database,
                    &record.target_schema,
                    &record.target_name,
                    &record.replacement_schema,
                ],
            )
            .await?;
    }

    Ok(())
}

/// Get replacement MV records for a deployment.
pub(super) async fn get_replacement_mvs(
    client: &Client,
    deploy_id: &str,
) -> Result<Vec<super::models::ReplacementMvRecord>, ConnectionError> {
    let rows = client
        .query(
            r#"SELECT deploy_id, target_database, target_schema, target_name,
                      replacement_schema
               FROM _mz_deploy.public.replacement_mvs
               WHERE deploy_id = $1
               ORDER BY target_database, target_schema, target_name"#,
            &[&deploy_id],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|row| super::models::ReplacementMvRecord {
            deploy_id: row.get("deploy_id"),
            target_database: row.get("target_database"),
            target_schema: row.get("target_schema"),
            target_name: row.get("target_name"),
            replacement_schema: row.get("replacement_schema"),
        })
        .collect())
}

impl DeploymentsClient<'_> {
    pub async fn insert_schema_deployments(
        &self,
        deployments: &[SchemaDeploymentRecord],
    ) -> Result<(), ConnectionError> {
        insert_schema_deployments(self.client, deployments).await
    }

    pub async fn append_deployment_objects(
        &self,
        objects: &[DeploymentObjectRecord],
    ) -> Result<(), ConnectionError> {
        append_deployment_objects(self.client, objects).await
    }

    pub async fn insert_deployment_clusters(
        &self,
        deploy_id: &str,
        clusters: &[String],
    ) -> Result<(), ConnectionError> {
        insert_deployment_clusters(self.client, deploy_id, clusters).await
    }

    pub async fn get_deployment_clusters(
        &self,
        deploy_id: &str,
    ) -> Result<Vec<String>, ConnectionError> {
        get_deployment_clusters(self.client, deploy_id).await
    }

    pub async fn validate_deployment_clusters(
        &self,
        deploy_id: &str,
    ) -> Result<(), ConnectionError> {
        validate_deployment_clusters(self.client, deploy_id).await
    }

    /// List clusters that host at least one promoted deployment, with one
    /// representative protected deployment per cluster.
    pub async fn list_production_clusters(
        &self,
    ) -> Result<Vec<ProductionClusterRecord>, ConnectionError> {
        list_production_clusters(self.client).await
    }

    pub async fn get_deployment_hydration_status(
        &self,
        deploy_id: &str,
    ) -> Result<Vec<ClusterStatusContext>, ConnectionError> {
        get_deployment_hydration_status(self.client, deploy_id, DEFAULT_ALLOWED_LAG_SECS).await
    }

    pub async fn get_deployment_hydration_status_with_lag(
        &self,
        deploy_id: &str,
        allowed_lag_secs: i64,
    ) -> Result<Vec<ClusterStatusContext>, ConnectionError> {
        get_deployment_hydration_status(self.client, deploy_id, allowed_lag_secs).await
    }

    pub async fn delete_deployment_clusters(&self, deploy_id: &str) -> Result<(), ConnectionError> {
        delete_deployment_clusters(self.client, deploy_id).await
    }

    pub async fn update_promoted_at(&self, deploy_id: &str) -> Result<(), ConnectionError> {
        update_promoted_at(self.client, deploy_id).await
    }

    pub async fn delete_deployment(&self, deploy_id: &str) -> Result<(), ConnectionError> {
        delete_deployment(self.client, deploy_id).await
    }

    pub async fn get_schema_deployments(
        &self,
        deploy_id: Option<&str>,
    ) -> Result<Vec<SchemaDeploymentRecord>, ConnectionError> {
        get_schema_deployments(self.client, deploy_id).await
    }

    pub async fn get_deployment_objects(
        &self,
        deploy_id: Option<&str>,
    ) -> Result<DeploymentSnapshot, ConnectionError> {
        get_deployment_objects(self.client, deploy_id).await
    }

    pub async fn get_deployment_metadata(
        &self,
        deploy_id: &str,
    ) -> Result<Option<DeploymentMetadata>, ConnectionError> {
        get_deployment_metadata(self.client, deploy_id).await
    }

    /// Validate that a staging deployment exists and has not been promoted.
    pub async fn validate_staging(&self, deploy_id: &str) -> Result<(), ConnectionError> {
        let metadata = self.get_deployment_metadata(deploy_id).await?;
        match metadata {
            Some(meta) if meta.promoted_at.is_some() => {
                Err(ConnectionError::DeploymentAlreadyPromoted {
                    deploy_id: deploy_id.to_string(),
                })
            }
            Some(_) => Ok(()),
            None => Err(ConnectionError::DeploymentNotFound {
                deploy_id: deploy_id.to_string(),
            }),
        }
    }

    pub async fn get_deployment_details(
        &self,
        deploy_id: &str,
    ) -> Result<Option<DeploymentDetails>, ConnectionError> {
        get_deployment_details(self.client, deploy_id).await
    }

    pub async fn list_staging_deployments(
        &self,
    ) -> Result<BTreeMap<String, StagingDeployment>, ConnectionError> {
        list_staging_deployments(self.client).await
    }

    pub async fn list_deployment_history(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<DeploymentHistoryEntry>, ConnectionError> {
        list_deployment_history(self.client, limit).await
    }

    pub async fn check_deployment_conflicts(
        &self,
        deploy_id: &str,
    ) -> Result<Vec<ConflictRecord>, ConnectionError> {
        check_deployment_conflicts(self.client, deploy_id).await
    }

    pub async fn deployment_table_exists(&self) -> Result<bool, ConnectionError> {
        deployment_table_exists(self.client).await
    }

    pub async fn create_apply_state_schemas(&self, deploy_id: &str) -> Result<(), ConnectionError> {
        create_apply_state_schemas(self.client, deploy_id).await
    }

    pub async fn get_apply_state(&self, deploy_id: &str) -> Result<ApplyState, ConnectionError> {
        get_apply_state(self.client, deploy_id).await
    }

    pub async fn delete_apply_state_schemas(&self, deploy_id: &str) -> Result<(), ConnectionError> {
        delete_apply_state_schemas(self.client, deploy_id).await
    }

    pub async fn insert_pending_statements(
        &self,
        statements: &[PendingStatement],
    ) -> Result<(), ConnectionError> {
        insert_pending_statements(self.client, statements).await
    }

    pub async fn get_pending_statements(
        &self,
        deploy_id: &str,
    ) -> Result<Vec<PendingStatement>, ConnectionError> {
        get_pending_statements(self.client, deploy_id).await
    }

    pub async fn mark_statement_executed(
        &self,
        deploy_id: &str,
        sequence_num: i32,
    ) -> Result<(), ConnectionError> {
        mark_statement_executed(self.client, deploy_id, sequence_num).await
    }

    pub async fn delete_pending_statements(&self, deploy_id: &str) -> Result<(), ConnectionError> {
        delete_pending_statements(self.client, deploy_id).await
    }

    pub async fn insert_replacement_mvs(
        &self,
        records: &[super::models::ReplacementMvRecord],
    ) -> Result<(), ConnectionError> {
        insert_replacement_mvs(self.client, records).await
    }

    pub async fn get_replacement_mvs(
        &self,
        deploy_id: &str,
    ) -> Result<Vec<super::models::ReplacementMvRecord>, ConnectionError> {
        get_replacement_mvs(self.client, deploy_id).await
    }

    pub async fn delete_replacement_mvs(&self, deploy_id: &str) -> Result<(), ConnectionError> {
        delete_replacement_mvs(self.client, deploy_id).await
    }
}

impl DeploymentsClientMut<'_> {
    /// Subscribe to hydration status changes for a staging deployment.
    pub fn subscribe_deployment_hydration(
        &mut self,
        deploy_id: &str,
        allowed_lag_secs: i64,
    ) -> impl Stream<Item = Result<HydrationStatusUpdate, ConnectionError>> + '_ {
        let deploy_id = deploy_id.to_string();

        try_stream! {
                let txn = self.client.begin_transaction().await?;
                let pattern = format!("%_{}", deploy_id);
                let query = hydration_status_query(allowed_lag_secs);
                let subscribe_sql = format!(
                    "DECLARE c CURSOR FOR SUBSCRIBE ({query})"
                );

                txn.execute(&subscribe_sql, &[&pattern]).await?;

                loop {
                    let rows = txn.query("FETCH ALL c", &[]).await?;
                    if rows.is_empty() {
                        continue;
                    }

                    for row in rows {
                        let mz_diff: i64 = row.get(1);
                        if mz_diff == -1 {
                            continue;
                        }

                        let status_str: String = row.get(4);
                        let failure_reason_str: Option<String> = row.get(5);
                        let hydrated_count: i64 = row.get(6);
                        let total_count: i64 = row.get(7);
                        let max_lag_secs: i64 = row.get(8);
                        let total_replicas: i64 = row.get(9);
                        let problematic_replicas: i64 = row.get(10);

                        let failure_reason = failure_reason_str.as_deref().map(|s| match s {
                            "no_replicas" => FailureReason::NoReplicas,
                            "all_replicas_problematic" => FailureReason::AllReplicasProblematic {
                                problematic: problematic_replicas,
                                total: total_replicas,
                            },
                            _ => FailureReason::NoReplicas,
                        });

                        let status = match status_str.as_str() {
                            "ready" => ClusterDeploymentStatus::Ready,
                            "hydrating" => ClusterDeploymentStatus::Hydrating {
                                hydrated: hydrated_count,
                                total: total_count,
                            },
                            "lagging" => ClusterDeploymentStatus::Lagging { max_lag_secs },
                            "failing" => ClusterDeploymentStatus::Failing {
                                reason: failure_reason.clone().unwrap_or(FailureReason::NoReplicas),
                            },
                            _ => ClusterDeploymentStatus::Ready,
                        };

                        yield HydrationStatusUpdate {
                            cluster_name: row.get(2),
                            cluster_id: row.get(3),
                            status,
                            failure_reason,
                            hydrated_count,
                            total_count,
                            max_lag_secs,
                            total_replicas,
                            problematic_replicas,
                        };
                }
            }
        }
    }
}

/// Delete all replacement MV records for a deployment.
pub(super) async fn delete_replacement_mvs(
    client: &Client,
    deploy_id: &str,
) -> Result<(), ConnectionError> {
    client
        .execute(
            "DELETE FROM _mz_deploy.tables.replacement_mvs WHERE deploy_id = $1",
            &[&deploy_id],
        )
        .await?;

    Ok(())
}
