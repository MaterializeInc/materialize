//! Database client for mz-deploy.
//!
//! This module provides the main `Client` struct for interacting with Materialize.
//! The client handles connection management and delegates specialized operations
//! to submodules:
//!
//! - `errors` - Error types for client operations
//! - `deployment_ops` - Deployment tracking and management
//! - `introspection` - Database metadata queries
//! - `validation` - Project validation against the database

use crate::client::config::{Profile, ProfilesConfig};
use crate::client::deployment_ops::{self, ClusterStatusContext, DEFAULT_ALLOWED_LAG_SECS};
use crate::client::errors::{ConnectionError, DatabaseValidationError};
use crate::client::introspection;
use crate::client::models::{
    Cluster, ClusterOptions, ConflictRecord, DeploymentMetadata, DeploymentObjectRecord,
    SchemaDeploymentRecord,
};
use crate::client::validation;
use crate::project::deployment_snapshot::DeploymentSnapshot;
use crate::project::object_id::ObjectId;
use crate::project::planned;
use crate::types::{ColumnType, Types};
use crate::utils::sql_utils::quote_identifier;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client as PgClient, NoTls, Row, ToStatement};

/// Database client for interacting with Materialize.
///
/// The `Client` struct provides methods for:
/// - Connecting to the database
/// - Schema and cluster management
/// - Deployment tracking
/// - Database introspection
/// - Project validation
pub struct Client {
    client: PgClient,
    profile: Profile,
}

impl Client {
    // =========================================================================
    // Connection Methods
    // =========================================================================

    /// Connect to the database using a named profile.
    ///
    /// Note: This method searches for profiles.toml in the current working directory.
    /// For project-specific configuration, use `ProfilesConfig::load_profile()` with
    /// a project directory and then `connect_with_profile()`.
    pub async fn connect(profile_name: Option<&str>) -> Result<Self, ConnectionError> {
        // Load profiles configuration (searches in CWD for backwards compatibility)
        let config = ProfilesConfig::load(None)?;

        // Get the requested profile or default
        let profile = if let Some(name) = profile_name {
            config.get_profile(name)?
        } else {
            config.get_default_profile()?
        };

        // Expand environment variables
        let profile = config.expand_env_vars(profile)?;

        // Connect to the database
        Self::connect_with_profile(profile).await
    }

    /// Connect to the database using a Profile directly.
    pub async fn connect_with_profile(profile: Profile) -> Result<Self, ConnectionError> {
        // Build connection string
        let mut conn_str = format!("host={} port={}", profile.host, profile.port);

        if let Some(ref username) = profile.username {
            conn_str.push_str(&format!(" user={}", username));
        }

        if let Some(ref password) = profile.password {
            conn_str.push_str(&format!(" password={}", password));
        }

        // Connect to the database
        let (client, connection) =
            tokio_postgres::connect(&conn_str, NoTls)
                .await
                .map_err(|source| ConnectionError::Connect {
                    host: profile.host.clone(),
                    port: profile.port,
                    source,
                })?;

        // Spawn the connection handler
        mz_ore::task::spawn(|| "mz-deploy-connection", async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(Client { client, profile })
    }

    /// Get the profile used for this connection.
    pub fn profile(&self) -> &Profile {
        &self.profile
    }

    /// Get a reference to the underlying tokio-postgres client.
    pub fn postgres_client(&self) -> &PgClient {
        &self.client
    }

    // =========================================================================
    // Basic Query Methods
    // =========================================================================

    /// Execute a SQL statement that doesn't return rows.
    pub async fn execute<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, ConnectionError>
    where
        T: ?Sized + ToStatement,
    {
        self.client
            .execute(statement, params)
            .await
            .map_err(ConnectionError::Query)
    }

    /// Execute a SQL query and return the resulting rows.
    pub async fn query_one<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, ConnectionError>
    where
        T: ?Sized + ToStatement,
    {
        self.client
            .query_one(statement, params)
            .await
            .map_err(ConnectionError::Query)
    }

    /// Execute a SQL query and return the resulting rows.
    pub async fn query<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, ConnectionError>
    where
        T: ?Sized + ToStatement,
    {
        self.client
            .query(statement, params)
            .await
            .map_err(ConnectionError::Query)
    }

    /// Query SHOW COLUMNS for all external dependencies and return their schemas as a Types object.
    pub async fn query_external_types(
        &mut self,
        project: &planned::Project,
    ) -> Result<Types, ConnectionError> {
        let mut objects = BTreeMap::new();
        let oids = project
            .external_dependencies
            .iter()
            .cloned()
            .chain(project.get_tables());

        for oid in oids {
            let quoted_db = quote_identifier(&oid.database);
            let quoted_schema = quote_identifier(&oid.schema);
            let quoted_object = quote_identifier(&oid.object);

            let rows = self
                .client
                .query(
                    &format!(
                        "SHOW COLUMNS FROM {}.{}.{}",
                        quoted_db, quoted_schema, quoted_object
                    ),
                    &[],
                )
                .await?;

            let mut columns = BTreeMap::new();
            for row in rows {
                let name: String = row.get("name");
                let type_str: String = row.get("type");
                let nullable: bool = row.get("nullable");

                let column_type = ColumnType {
                    r#type: type_str,
                    nullable,
                };

                columns.insert(name, column_type);
            }

            objects.insert(oid.to_string(), columns);
        }

        Ok(Types {
            version: 1,
            objects,
        })
    }

    /// Query types for internal project views from the database.
    ///
    /// This is used after type checking to capture the column schemas of all views
    /// defined in the project. These types are cached in `.mz-deploy/types.cache`
    /// and used by the test command to validate unit tests.
    ///
    /// Note: This should be called after the views have been created in the database
    /// (either as permanent or temporary views during type checking).
    ///
    /// # Arguments
    /// * `object_ids` - The object IDs to query types for
    /// * `flatten` - If true, query using flattened FQN names (for temporary views)
    ///
    /// # Returns
    /// A Types struct containing the column schemas for all queried objects
    pub async fn query_internal_types(
        &mut self,
        object_ids: &[&ObjectId],
        flatten: bool,
    ) -> Result<Types, ConnectionError> {
        let mut objects = BTreeMap::new();

        for oid in object_ids {
            // Build the object name (flattened for temp views, or regular FQN)
            let object_ref = if flatten {
                // For temporary views, the name is a single flattened identifier
                format!("\"{}.{}.{}\"", oid.database, oid.schema, oid.object)
            } else {
                let quoted_db = quote_identifier(&oid.database);
                let quoted_schema = quote_identifier(&oid.schema);
                let quoted_object = quote_identifier(&oid.object);
                format!("{}.{}.{}", quoted_db, quoted_schema, quoted_object)
            };

            let rows = self
                .client
                .query(&format!("SHOW COLUMNS FROM {}", object_ref), &[])
                .await?;

            let mut columns = BTreeMap::new();
            for row in rows {
                let name: String = row.get("name");
                let type_str: String = row.get("type");
                let nullable: bool = row.get("nullable");

                let column_type = ColumnType {
                    r#type: type_str,
                    nullable,
                };

                columns.insert(name, column_type);
            }

            // Always store with regular FQN key (not flattened)
            objects.insert(oid.to_string(), columns);
        }

        Ok(Types {
            version: 1,
            objects,
        })
    }

    // =========================================================================
    // Schema Operations
    // =========================================================================

    /// Create a schema in the specified database (idempotent).
    pub async fn create_schema(&self, database: &str, schema: &str) -> Result<(), ConnectionError> {
        let sql = format!(
            "CREATE SCHEMA IF NOT EXISTS {}.{}",
            quote_identifier(database),
            quote_identifier(schema)
        );

        self.client.execute(&sql, &[]).await.map_err(|e| {
            ConnectionError::SchemaCreationFailed {
                database: database.to_string(),
                schema: schema.to_string(),
                source: Box::new(e),
            }
        })?;

        Ok(())
    }

    /// Check if a schema exists in the specified database.
    pub async fn schema_exists(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<bool, ConnectionError> {
        introspection::schema_exists(&self.client, database, schema).await
    }

    // =========================================================================
    // Cluster Operations
    // =========================================================================

    /// Create a cluster with the specified configuration.
    pub async fn create_cluster(
        &self,
        name: &str,
        options: &ClusterOptions,
    ) -> Result<(), ConnectionError> {
        let sql = format!(
            "CREATE CLUSTER {} (SIZE = '{}', REPLICATION FACTOR = {})",
            quote_identifier(name),
            options.size,
            options.replication_factor
        );

        self.client.execute(&sql, &[]).await.map_err(|e| {
            if e.to_string().contains("already exists") {
                ConnectionError::ClusterAlreadyExists {
                    name: name.to_string(),
                }
            } else {
                ConnectionError::ClusterCreationFailed {
                    name: name.to_string(),
                    source: Box::new(e),
                }
            }
        })?;

        Ok(())
    }

    /// Check if a cluster exists.
    pub async fn cluster_exists(&self, name: &str) -> Result<bool, ConnectionError> {
        introspection::cluster_exists(&self.client, name).await
    }

    /// Get a cluster by name.
    pub async fn get_cluster(&self, name: &str) -> Result<Option<Cluster>, ConnectionError> {
        introspection::get_cluster(&self.client, name).await
    }

    /// List all clusters.
    pub async fn list_clusters(&self) -> Result<Vec<Cluster>, ConnectionError> {
        introspection::list_clusters(&self.client).await
    }

    // =========================================================================
    // Deployment Tracking Operations
    // =========================================================================

    /// Create the deployment tracking schemas/tables for staging deployments.
    pub async fn create_deployments(&self) -> Result<(), ConnectionError> {
        deployment_ops::create_deployments(&self.client).await
    }

    /// Insert schema deployment records (insert-only, no DELETE).
    pub async fn insert_schema_deployments(
        &self,
        deployments: &[SchemaDeploymentRecord],
    ) -> Result<(), ConnectionError> {
        deployment_ops::insert_schema_deployments(&self.client, deployments).await
    }

    /// Append deployment object records (insert-only, never update or delete).
    pub async fn append_deployment_objects(
        &self,
        objects: &[DeploymentObjectRecord],
    ) -> Result<(), ConnectionError> {
        deployment_ops::append_deployment_objects(&self.client, objects).await
    }

    /// Insert cluster records for a staging deployment.
    pub async fn insert_deployment_clusters(
        &self,
        deploy_id: &str,
        clusters: &[String],
    ) -> Result<(), ConnectionError> {
        deployment_ops::insert_deployment_clusters(&self.client, deploy_id, clusters).await
    }

    /// Get cluster names for a staging deployment.
    pub async fn get_deployment_clusters(
        &self,
        deploy_id: &str,
    ) -> Result<Vec<String>, ConnectionError> {
        deployment_ops::get_deployment_clusters(&self.client, deploy_id).await
    }

    /// Validate that all cluster IDs in a deployment still exist in the catalog.
    pub async fn validate_deployment_clusters(
        &self,
        deploy_id: &str,
    ) -> Result<(), ConnectionError> {
        deployment_ops::validate_deployment_clusters(&self.client, deploy_id).await
    }

    /// Get detailed hydration and health status for clusters in a staging deployment.
    ///
    /// Uses the default allowed lag threshold of 5 minutes.
    pub async fn get_deployment_hydration_status(
        &self,
        deploy_id: &str,
    ) -> Result<Vec<ClusterStatusContext>, ConnectionError> {
        deployment_ops::get_deployment_hydration_status(
            &self.client,
            deploy_id,
            DEFAULT_ALLOWED_LAG_SECS,
        )
        .await
    }

    /// Get detailed hydration and health status with custom lag threshold.
    ///
    /// # Arguments
    /// * `deploy_id` - Staging deployment ID
    /// * `allowed_lag_secs` - Maximum allowed lag in seconds before marking as "lagging"
    pub async fn get_deployment_hydration_status_with_lag(
        &self,
        deploy_id: &str,
        allowed_lag_secs: i64,
    ) -> Result<Vec<ClusterStatusContext>, ConnectionError> {
        deployment_ops::get_deployment_hydration_status(&self.client, deploy_id, allowed_lag_secs)
            .await
    }

    /// Subscribe to hydration status changes for a staging deployment.
    ///
    /// Returns a transaction that has a cursor set up for subscribing to hydration updates.
    /// The subscription includes hydration progress, wallclock lag, and replica health.
    pub async fn subscribe_deployment_hydration(
        &mut self,
        deploy_id: &str,
        allowed_lag_secs: i64,
    ) -> Result<tokio_postgres::Transaction<'_>, ConnectionError> {
        let txn = self.client.transaction().await?;
        let pattern = format!("%_{}", deploy_id);

        let subscribe_sql = format!(
            r#"
            DECLARE c CURSOR FOR SUBSCRIBE (
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
            )
        "#,
            allowed_lag_secs = allowed_lag_secs
        );

        txn.execute(&subscribe_sql, &[&pattern]).await?;

        Ok(txn)
    }

    /// Delete cluster records for a staging deployment.
    pub async fn delete_deployment_clusters(&self, deploy_id: &str) -> Result<(), ConnectionError> {
        deployment_ops::delete_deployment_clusters(&self.client, deploy_id).await
    }

    /// Update promoted_at timestamp for a staging deployment.
    pub async fn update_promoted_at(&self, deploy_id: &str) -> Result<(), ConnectionError> {
        deployment_ops::update_promoted_at(&self.client, deploy_id).await
    }

    /// Delete all deployment records for a specific deployment.
    pub async fn delete_deployment(&self, deploy_id: &str) -> Result<(), ConnectionError> {
        deployment_ops::delete_deployment(&self.client, deploy_id).await
    }

    /// Check if the deployment tracking table exists.
    pub async fn deployment_table_exists(&self) -> Result<bool, ConnectionError> {
        deployment_ops::deployment_table_exists(&self.client).await
    }

    /// Get schema deployment records from the database for a specific deployment.
    pub async fn get_schema_deployments(
        &self,
        deploy_id: Option<&str>,
    ) -> Result<Vec<SchemaDeploymentRecord>, ConnectionError> {
        deployment_ops::get_schema_deployments(&self.client, deploy_id).await
    }

    /// Get deployment object records from the database for a specific deployment.
    pub async fn get_deployment_objects(
        &self,
        deploy_id: Option<&str>,
    ) -> Result<DeploymentSnapshot, ConnectionError> {
        deployment_ops::get_deployment_objects(&self.client, deploy_id).await
    }

    /// Get metadata about a deployment for validation.
    pub async fn get_deployment_metadata(
        &self,
        deploy_id: &str,
    ) -> Result<Option<DeploymentMetadata>, ConnectionError> {
        deployment_ops::get_deployment_metadata(&self.client, deploy_id).await
    }

    /// Get detailed information about a specific deployment.
    #[allow(clippy::type_complexity)]
    pub async fn get_deployment_details(
        &self,
        deploy_id: &str,
    ) -> Result<
        Option<(
            std::time::SystemTime,
            Option<std::time::SystemTime>,
            String,
            Option<String>,
            String,
            Vec<(String, String)>,
        )>,
        ConnectionError,
    > {
        deployment_ops::get_deployment_details(&self.client, deploy_id).await
    }

    /// List all staging deployments (promoted_at IS NULL), grouped by deploy_id.
    #[allow(clippy::type_complexity)]
    pub async fn list_staging_deployments(
        &self,
    ) -> Result<
        BTreeMap<
            String,
            (
                std::time::SystemTime,
                String,
                Option<String>,
                String,
                Vec<(String, String)>,
            ),
        >,
        ConnectionError,
    > {
        deployment_ops::list_staging_deployments(&self.client).await
    }

    /// List deployment history in chronological order (promoted deployments only).
    #[allow(clippy::type_complexity)]
    pub async fn list_deployment_history(
        &self,
        limit: Option<usize>,
    ) -> Result<
        Vec<(
            String,
            std::time::SystemTime,
            String,
            Option<String>,
            String,
            Vec<(String, String)>,
        )>,
        ConnectionError,
    > {
        deployment_ops::list_deployment_history(&self.client, limit).await
    }

    /// Check for deployment conflicts (schemas updated after deployment started).
    pub async fn check_deployment_conflicts(
        &self,
        deploy_id: &str,
    ) -> Result<Vec<ConflictRecord>, ConnectionError> {
        deployment_ops::check_deployment_conflicts(&self.client, deploy_id).await
    }

    // =========================================================================
    // Introspection Operations
    // =========================================================================

    /// Get the current Materialize user/role.
    pub async fn get_current_user(&self) -> Result<String, ConnectionError> {
        introspection::get_current_user(&self.client).await
    }

    /// Check which objects from a set exist in the production database.
    pub async fn check_objects_exist(
        &self,
        objects: &BTreeSet<ObjectId>,
    ) -> Result<Vec<String>, ConnectionError> {
        introspection::check_objects_exist(&self.client, objects).await
    }

    /// Check which tables from the given set exist in the database.
    pub async fn check_tables_exist(
        &self,
        tables: &BTreeSet<ObjectId>,
    ) -> Result<BTreeSet<ObjectId>, ConnectionError> {
        introspection::check_tables_exist(&self.client, tables).await
    }

    /// Get staging schema names for a specific deployment.
    pub async fn get_staging_schemas(
        &self,
        deploy_id: &str,
    ) -> Result<Vec<(String, String)>, ConnectionError> {
        introspection::get_staging_schemas(&self.client, deploy_id).await
    }

    /// Get staging cluster names for a specific deployment.
    pub async fn get_staging_clusters(
        &self,
        deploy_id: &str,
    ) -> Result<Vec<String>, ConnectionError> {
        introspection::get_staging_clusters(&self.client, deploy_id).await
    }

    /// Drop all objects in a schema.
    pub async fn drop_schema_objects(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<String>, ConnectionError> {
        introspection::drop_schema_objects(&self.client, database, schema).await
    }

    /// Drop specific objects by their ObjectIds.
    pub async fn drop_objects(
        &self,
        objects: &BTreeSet<ObjectId>,
    ) -> Result<Vec<String>, ConnectionError> {
        introspection::drop_objects(&self.client, objects).await
    }

    /// Drop staging schemas by name.
    pub async fn drop_staging_schemas(
        &self,
        schemas: &[(String, String)],
    ) -> Result<(), ConnectionError> {
        introspection::drop_staging_schemas(&self.client, schemas).await
    }

    /// Drop staging clusters by name.
    pub async fn drop_staging_clusters(&self, clusters: &[String]) -> Result<(), ConnectionError> {
        introspection::drop_staging_clusters(&self.client, clusters).await
    }

    // =========================================================================
    // Validation Operations
    // =========================================================================

    /// Validate that all required databases, schemas, and external dependencies exist.
    pub async fn validate_project(
        &mut self,
        planned_project: &planned::Project,
        project_root: &Path,
    ) -> Result<(), DatabaseValidationError> {
        validation::validate_project_impl(&self.client, planned_project, project_root).await
    }

    /// Validate that sources and sinks don't share clusters with indexes or materialized views.
    pub async fn validate_cluster_isolation(
        &mut self,
        planned_project: &planned::Project,
    ) -> Result<(), DatabaseValidationError> {
        validation::validate_cluster_isolation_impl(&self.client, planned_project).await
    }

    /// Validate that the user has sufficient privileges to deploy the project.
    pub async fn validate_privileges(
        &mut self,
        planned_project: &planned::Project,
    ) -> Result<(), DatabaseValidationError> {
        validation::validate_privileges_impl(&self.client, planned_project).await
    }

    /// Validate that all sources referenced by CREATE TABLE FROM SOURCE statements exist.
    pub async fn validate_sources_exist(
        &mut self,
        planned_project: &planned::Project,
    ) -> Result<(), DatabaseValidationError> {
        validation::validate_sources_exist_impl(&self.client, planned_project).await
    }

    /// Validate that all tables referenced by objects to be deployed exist in the database.
    pub async fn validate_table_dependencies(
        &mut self,
        planned_project: &planned::Project,
        objects_to_deploy: &BTreeSet<ObjectId>,
    ) -> Result<(), DatabaseValidationError> {
        validation::validate_table_dependencies_impl(
            &self.client,
            planned_project,
            objects_to_deploy,
        )
        .await
    }
}
