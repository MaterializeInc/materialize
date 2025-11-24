//! Introspection API for querying Materialize system catalog.
//!
//! This module provides read-only methods for querying schemas, clusters, and other
//! catalog objects using the mz_catalog system tables.

use std::collections::HashSet;
use super::connection::ConnectionError;
use super::models::Cluster;
use tokio_postgres::Client as PgClient;
use crate::client::SchemaDeploymentRecord;
use crate::project::object_id::ObjectId;

/// API for introspecting Materialize catalog objects.
///
/// All methods are read-only and query the mz_catalog system tables.
pub struct IntrospectionApi<'a> {
    client: &'a PgClient,
}

impl<'a> IntrospectionApi<'a> {
    /// Create a new introspection API wrapping a PostgreSQL client.
    pub(crate) fn new(client: &'a PgClient) -> Self {
        Self { client }
    }

    /// Check if a schema exists in the specified database.
    ///
    /// # Arguments
    /// * `database` - Database name (e.g., "materialize")
    /// * `schema` - Schema name (e.g., "public")
    ///
    /// # Returns
    /// `true` if the schema exists, `false` otherwise
    pub async fn schema_exists(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<bool, ConnectionError> {
        let query = r#"
            SELECT EXISTS(
                SELECT 1
                FROM mz_catalog.mz_schemas s
                JOIN mz_catalog.mz_databases d ON s.database_id = d.id
                WHERE s.name = $1 AND d.name = $2
            ) AS exists
        "#;

        let row = self
            .client
            .query_one(query, &[&schema, &database])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(row.get("exists"))
    }

    /// Check if a cluster exists.
    ///
    /// # Arguments
    /// * `name` - Cluster name (e.g., "quickstart")
    ///
    /// # Returns
    /// `true` if the cluster exists, `false` otherwise
    pub async fn cluster_exists(&self, name: &str) -> Result<bool, ConnectionError> {
        let query = r#"
            SELECT EXISTS(
                SELECT 1 FROM mz_catalog.mz_clusters WHERE name = $1
            ) AS exists
        "#;

        let row = self
            .client
            .query_one(query, &[&name])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(row.get("exists"))
    }

    /// Get a cluster by name.
    ///
    /// # Arguments
    /// * `name` - Cluster name (e.g., "quickstart")
    ///
    /// # Returns
    /// `Some(Cluster)` if found with its configuration, `None` if not found
    pub async fn get_cluster(&self, name: &str) -> Result<Option<Cluster>, ConnectionError> {
        let query = r#"
            SELECT
                id,
                name,
                size,
                replication_factor
            FROM mz_catalog.mz_clusters
            WHERE name = $1
        "#;

        let rows = self
            .client
            .query(query, &[&name])
            .await
            .map_err(ConnectionError::Query)?;

        if rows.is_empty() {
            return Ok(None);
        }

        let row = &rows[0];

        // Materialize returns uint4 for replication_factor, which postgres crate doesn't directly support
        // Try to get it as different integer types
        let replication_factor: Option<i64> = row
            .try_get("replication_factor")
            .or_else(|_| {
                row.try_get::<_, Option<i32>>("replication_factor")
                    .map(|v| v.map(|x| x as i64))
            })
            .or_else(|_| {
                row.try_get::<_, Option<i16>>("replication_factor")
                    .map(|v| v.map(|x| x as i64))
            })
            .unwrap_or(None);

        Ok(Some(Cluster {
            id: row.get("id"),
            name: row.get("name"),
            size: row.get("size"),
            replication_factor,
        }))
    }

    /// List all clusters.
    ///
    /// # Returns
    /// Vector of all clusters
    pub async fn list_clusters(&self) -> Result<Vec<Cluster>, ConnectionError> {
        let query = r#"
            SELECT
                id,
                name,
                size,
                replication_factor
            FROM mz_catalog.mz_clusters
            ORDER BY name
        "#;

        let rows = self
            .client
            .query(query, &[])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(rows
            .iter()
            .map(|row| Cluster {
                id: row.get("id"),
                name: row.get("name"),
                size: row.get("size"),
                replication_factor: row.get("replication_factor"),
            })
            .collect())
    }

    /// Check if the deployment tracking table exists.
    ///
    /// # Returns
    /// true if deploy.deployments table exists
    pub async fn deployment_table_exists(&self) -> Result<bool, ConnectionError> {
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

        let row = self
            .client
            .query_one(query, &[])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(row.get(0))
    }

    /// Get the current Materialize user/role.
    ///
    /// # Returns
    /// The current user name
    pub async fn get_current_user(&self) -> Result<String, ConnectionError> {
        let query = "SELECT current_user()";

        let row = self
            .client
            .query_one(query, &[])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(row.get(0))
    }

    /// Check which objects from a set exist in the production database.
    ///
    /// This is used to validate deployment safety by checking if objects
    /// that need redeployment actually exist in production.
    ///
    /// # Arguments
    /// * `objects` - Set of ObjectIds to check for existence
    ///
    /// # Returns
    /// Vector of FQNs for objects that exist in production
    pub async fn check_objects_exist(
        &self,
        objects: &HashSet<ObjectId>,
    ) -> Result<Vec<String>, ConnectionError> {
        let fqns: Vec<String> = objects.iter().map(|o| o.to_string()).collect();
        if fqns.is_empty() {
            return Ok(Vec::new());
        }

        // Build dynamic IN clause since Materialize doesn't support array parameters
        let placeholders: Vec<String> = (1..=fqns.len()).map(|i| format!("${}", i)).collect();
        let placeholders_str = placeholders.join(", ");

        // Query to check which objects exist
        let query = format!(
            r#"
            SELECT d.name || '.' || s.name || '.' || mo.name as fqn
            FROM mz_objects mo
            JOIN mz_schemas s ON mo.schema_id = s.id
            JOIN mz_databases d ON s.database_id = d.id
            WHERE d.name || '.' || s.name || '.' || mo.name IN ({})
            AND mo.type IN ('table', 'view', 'materialized-view', 'source', 'sink')
            ORDER BY fqn
        "#,
            placeholders_str
        );

        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
        for fqn in &fqns {
            params.push(fqn);
        }

        let rows = self
            .client
            .query(&query, &params)
            .await
            .map_err(ConnectionError::Query)?;

        Ok(rows.iter().map(|row| row.get("fqn")).collect())
    }

    /// Get schema deployment records from the database for a specific environment.
    ///
    /// # Arguments
    /// * `environment` - None for production, Some("staging") for staging
    ///
    /// # Returns
    /// Vector of schema deployment records
    pub async fn get_schema_deployments(
        &self,
        environment: Option<&str>,
    ) -> Result<Vec<SchemaDeploymentRecord>, ConnectionError> {
        use std::time::UNIX_EPOCH;

        // For production, query the production view which filters for promoted deployments
        // For staging, query the deployments table directly
        let query = if environment.is_none() {
            r#"
                SELECT environment, database, schema,
                       CAST(EXTRACT(EPOCH FROM promoted_at) AS DOUBLE PRECISION) as deployed_at_epoch,
                       '' as deployed_by,
                       CAST(EXTRACT(EPOCH FROM promoted_at) AS DOUBLE PRECISION) as promoted_at_epoch
                FROM deploy.production
                ORDER BY database, schema
            "#
        } else {
            r#"
                SELECT environment, database, schema,
                       CAST(EXTRACT(EPOCH FROM deployed_at) AS DOUBLE PRECISION) as deployed_at_epoch,
                       deployed_by,
                       CAST(EXTRACT(EPOCH FROM promoted_at) AS DOUBLE PRECISION) as promoted_at_epoch
                FROM deploy.deployments
                WHERE environment = $1
                ORDER BY database, schema
            "#
        };

        let rows = if environment.is_none() {
            self.client
                .query(query, &[])
                .await
                .map_err(ConnectionError::Query)?
        } else {
            self.client
                .query(query, &[&environment])
                .await
                .map_err(ConnectionError::Query)?
        };

        let mut records = Vec::new();
        for row in rows {
            let environment: String = row.get("environment");
            let database: String = row.get("database");
            let schema: String = row.get("schema");
            let deployed_at_epoch: f64 = row.get("deployed_at_epoch");
            let deployed_by: String = row.get("deployed_by");
            let promoted_at_epoch: Option<f64> = row.get("promoted_at_epoch");

            let deployed_at = UNIX_EPOCH + std::time::Duration::from_secs_f64(deployed_at_epoch);
            let promoted_at = promoted_at_epoch
                .map(|epoch| UNIX_EPOCH + std::time::Duration::from_secs_f64(epoch));

            records.push(crate::client::models::SchemaDeploymentRecord {
                environment,
                database,
                schema,
                deployed_at,
                deployed_by,
                promoted_at,
                git_commit: None,
            });
        }

        Ok(records)
    }

    /// Get deployment object records from the database for a specific environment.
    ///
    /// Queries the latest deployment for each object in the specified environment.
    ///
    /// # Arguments
    /// * `environment` - None for production, Some("staging") for staging
    ///
    /// # Returns
    /// Vector of deployment object records
    pub async fn get_deployment_objects(
        &self,
        environment: Option<&str>,
    ) -> Result<crate::project::deployment_snapshot::DeploymentSnapshot, ConnectionError> {
        use std::collections::{BTreeMap, HashSet};

        // For production, join objects with production view to get only promoted objects
        // For staging, query objects table directly
        let query = if environment.is_none() {
            r#"
                SELECT o.database, o.schema, o.object, o.hash
                FROM deploy.objects o
                JOIN deploy.production p
                  ON o.database = p.database AND o.schema = p.schema
                WHERE o.environment = p.environment
            "#
        } else {
            r#"
                SELECT database, schema, object, hash
                FROM deploy.objects
                WHERE environment = $1
            "#
        };

        let rows = if environment.is_none() {
            self.client
                .query(query, &[])
                .await
                .map_err(ConnectionError::Query)?
        } else {
            self.client
                .query(query, &[&environment])
                .await
                .map_err(ConnectionError::Query)?
        };

        let mut objects = BTreeMap::new();
        let mut schemas = HashSet::new();
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

        Ok(crate::project::deployment_snapshot::DeploymentSnapshot { objects, schemas })
    }

    /// Get metadata about a deployment environment for validation.
    ///
    /// Returns None if the environment doesn't exist.
    ///
    /// # Arguments
    /// * `environment` - The environment name to query
    ///
    /// # Returns
    /// Some(DeploymentMetadata) if found, None otherwise
    pub async fn get_deployment_metadata(
        &self,
        environment: &str,
    ) -> Result<Option<crate::client::models::DeploymentMetadata>, ConnectionError> {
        use std::time::UNIX_EPOCH;

        let query = r#"
            SELECT environment,
                   CAST(EXTRACT(EPOCH FROM promoted_at) AS DOUBLE PRECISION) as promoted_at_epoch,
                   database,
                   schema
            FROM deploy.deployments
            WHERE environment = $1
        "#;

        let rows = self
            .client
            .query(query, &[&environment])
            .await
            .map_err(ConnectionError::Query)?;

        if rows.is_empty() {
            return Ok(None);
        }

        let first_row = &rows[0];
        let environment: String = first_row.get("environment");
        let promoted_at_epoch: Option<f64> = first_row.get("promoted_at_epoch");
        let promoted_at =
            promoted_at_epoch.map(|epoch| UNIX_EPOCH + std::time::Duration::from_secs_f64(epoch));

        let mut schemas = Vec::new();
        for row in rows {
            let database: String = row.get("database");
            let schema: String = row.get("schema");
            schemas.push((database, schema));
        }

        Ok(Some(crate::client::models::DeploymentMetadata {
            environment,
            promoted_at,
            schemas,
        }))
    }

    /// Get staging schema names for a specific environment.
    ///
    /// Returns schema names with the staging suffix (e.g., "public_staging").
    ///
    /// # Arguments
    /// * `environment` - The staging environment name
    ///
    /// # Returns
    /// Vector of (database, schema) tuples with staging suffix
    pub async fn get_staging_schemas(
        &self,
        environment: &str,
    ) -> Result<Vec<(String, String)>, ConnectionError> {
        let suffix = format!("_{}", environment);
        let pattern = format!("%{}", suffix);

        let query = r#"
            SELECT d.name as database, s.name as schema
            FROM mz_schemas s
            JOIN mz_databases d ON s.database_id = d.id
            WHERE s.name LIKE $1
        "#;

        let rows = self
            .client
            .query(query, &[&pattern])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(rows
            .iter()
            .map(|row| {
                let database: String = row.get("database");
                let schema: String = row.get("schema");
                (database, schema)
            })
            .collect())
    }

    /// Get staging cluster names for a specific environment.
    ///
    /// Returns cluster names with the staging suffix (e.g., "quickstart_staging").
    ///
    /// # Arguments
    /// * `environment` - The staging environment name
    ///
    /// # Returns
    /// Vector of cluster names with staging suffix
    pub async fn get_staging_clusters(
        &self,
        environment: &str,
    ) -> Result<Vec<String>, ConnectionError> {
        let suffix = format!("_{}", environment);
        let pattern = format!("%{}", suffix);

        let query = r#"
            SELECT name
            FROM mz_clusters
            WHERE name LIKE $1
        "#;

        let rows = self
            .client
            .query(query, &[&pattern])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(rows.iter().map(|row| row.get("name")).collect())
    }

    /// Check for deployment conflicts (schemas updated after deployment started).
    ///
    /// This performs a git-merge-style conflict check by comparing the staging
    /// deployment's start time against production's last promotion time. If any
    /// schemas were promoted to production after the staging deployment began,
    /// they are returned as conflicts.
    ///
    /// # Arguments
    /// * `environment` - The staging environment name to check
    ///
    /// # Returns
    /// Vector of conflict records for schemas that were updated after deployment started
    pub async fn check_deployment_conflicts(
        &self,
        environment: &str,
    ) -> Result<Vec<crate::client::models::ConflictRecord>, ConnectionError> {
        use std::time::{Duration, UNIX_EPOCH};

        let query = r#"
            SELECT p.database, p.schema, p.environment,
                   CAST(EXTRACT(EPOCH FROM p.promoted_at) AS DOUBLE PRECISION) as promoted_at_epoch
            FROM deploy.production p
            JOIN deploy.deployments d USING (database, schema)
            WHERE d.environment = $1 AND p.promoted_at > d.deployed_at
        "#;

        let rows = self
            .client
            .query(query, &[&environment])
            .await
            .map_err(ConnectionError::Query)?;

        let conflicts = rows
            .iter()
            .map(|row| {
                let promoted_at_epoch: f64 = row.get("promoted_at_epoch");
                let promoted_at = UNIX_EPOCH + Duration::from_secs_f64(promoted_at_epoch);

                crate::client::models::ConflictRecord {
                    database: row.get("database"),
                    schema: row.get("schema"),
                    environment: row.get("environment"),
                    promoted_at,
                }
            })
            .collect();

        Ok(conflicts)
    }
}
