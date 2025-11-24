//! Creation API for creating Materialize resources.
//!
//! This module provides methods for creating schemas, clusters, and other database
//! objects using DDL statements.

use super::connection::ConnectionError;
use super::models::ClusterOptions;
use tokio_postgres::Client as PgClient;

/// API for creating Materialize resources.
///
/// All methods execute DDL statements to create database objects.
pub struct CreationApi<'a> {
    client: &'a PgClient,
}

impl<'a> CreationApi<'a> {
    /// Create a new creation API wrapping a PostgreSQL client.
    pub(crate) fn new(client: &'a PgClient) -> Self {
        Self { client }
    }

    /// Create a schema in the specified database.
    ///
    /// Uses `IF NOT EXISTS` to make the operation idempotent.
    ///
    /// # Arguments
    /// * `database` - Database name (e.g., "materialize")
    /// * `schema` - Schema name (e.g., "public_staging")
    ///
    /// # Returns
    /// `Ok(())` if successful or if schema already exists
    ///
    /// # Errors
    /// Returns `ConnectionError::SchemaCreationFailed` if the creation fails
    pub async fn create_schema(&self, database: &str, schema: &str) -> Result<(), ConnectionError> {
        let sql = format!("CREATE SCHEMA IF NOT EXISTS {}.{}", database, schema);

        self.client.execute(&sql, &[]).await.map_err(|e| {
            ConnectionError::SchemaCreationFailed {
                database: database.to_string(),
                schema: schema.to_string(),
                source: Box::new(e),
            }
        })?;

        Ok(())
    }

    /// Create a cluster with the specified configuration.
    ///
    /// **Note**: CREATE CLUSTER does not support IF NOT EXISTS. Callers must check
    /// for existence before calling this method to avoid errors.
    ///
    /// # Arguments
    /// * `name` - Cluster name (e.g., "quickstart_staging")
    /// * `options` - Cluster configuration (size and replication factor)
    ///
    /// # Returns
    /// `Ok(())` if successful
    ///
    /// # Errors
    /// * Returns `ConnectionError::ClusterCreationFailed` if the creation fails
    /// * Returns `ConnectionError::ClusterAlreadyExists` if the cluster already exists
    pub async fn create_cluster(
        &self,
        name: &str,
        options: ClusterOptions,
    ) -> Result<(), ConnectionError> {
        let sql = format!(
            "CREATE CLUSTER {} (SIZE = '{}', REPLICATION FACTOR = {})",
            name, options.size, options.replication_factor
        );

        self.client.execute(&sql, &[]).await.map_err(|e| {
            // Check if error is due to cluster already existing
            let err_msg = e.to_string();
            if err_msg.contains("already exists") {
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

    /// Create the schema deployments table for tracking schema-level deployments.
    ///
    /// This table tracks when schemas were deployed atomically along with promotion timestamps.
    /// This operation is idempotent.
    pub async fn create_deployments(&self) -> Result<(), ConnectionError> {
        self.client
            .execute("CREATE SCHEMA IF NOT EXISTS deploy;", &[])
            .await
            .map_err(ConnectionError::Query)?;

        self.client
            .execute(
                r#"CREATE TABLE IF NOT EXISTS deploy.deployments (
                    environment TEXT NOT NULL,
                    deployed_at TIMESTAMP NOT NULL,
                    promoted_at TIMESTAMP,
                    database    TEXT NOT NULL,
                    schema      TEXT NOT NULL,
                    deployed_by TEXT NOT NULL
                ) WITH (
                    PARTITION BY (environment, deployed_at, promoted_at)
                );"#,
                &[],
            )
            .await
            .map_err(ConnectionError::Query)?;

        self.client
            .execute(
                r#"CREATE TABLE IF NOT EXISTS deploy.objects (
                    environment TEXT NOT NULL,
                    database TEXT NOT NULL,
                    schema   TEXT NOT NULL,
                    object   TEXT NOT NULL,
                    hash     TEXT NOT NULL
                ) WITH (
                    PARTITION BY (environment, database, schema)
                );"#,
                &[],
            )
            .await
            .map_err(ConnectionError::Query)?;

        self.client
            .execute(
                r#"
                CREATE VIEW IF NOT EXISTS deploy.production AS
                WITH candidates AS (
                    SELECT DISTINCT ON (database, schema) database, schema, environment, promoted_at
                    FROM deploy.deployments
                    WHERE promoted_at IS NOT NULL
                    ORDER BY database, schema, promoted_at DESC
                )

                SELECT c.database, c.schema, c.environment, c.promoted_at
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
    ///
    /// This function should only be called after validating that the deployment
    /// environment doesn't already exist (for stage) or is valid for promotion (for apply).
    ///
    /// # Arguments
    /// * `deployments` - The schema deployment records to insert
    pub async fn insert_schema_deployments(
        &self,
        deployments: &[crate::client::models::SchemaDeploymentRecord],
    ) -> Result<(), ConnectionError> {
        if deployments.is_empty() {
            return Ok(());
        }

        // Insert new records
        let insert_sql = r#"
            INSERT INTO deploy.deployments
                (environment, database, schema, deployed_at, deployed_by, promoted_at)
            VALUES
                ($1, $2, $3, $4, $5, $6)
        "#;

        for deployment in deployments {
            self.client
                .execute(
                    insert_sql,
                    &[
                        &deployment.environment,
                        &deployment.database,
                        &deployment.schema,
                        &deployment.deployed_at,
                        &deployment.deployed_by,
                        &deployment.promoted_at,
                    ],
                )
                .await
                .map_err(ConnectionError::Query)?;
        }

        Ok(())
    }

    /// Append deployment object records (insert-only, never update or delete).
    ///
    /// This table is append-only to maintain a complete history of all object deployments.
    ///
    /// # Arguments
    /// * `objects` - The deployment object records to append
    pub async fn append_deployment_objects(
        &self,
        objects: &[crate::client::models::DeploymentObjectRecord],
    ) -> Result<(), ConnectionError> {
        if objects.is_empty() {
            return Ok(());
        }

        // Insert records (append-only, no delete)
        let insert_sql = r#"
            INSERT INTO deploy.objects
                (environment, database, schema, object, hash)
            VALUES
                ($1, $2, $3, $4, $5)
        "#;

        for obj in objects {
            self.client
                .execute(
                    insert_sql,
                    &[
                        &obj.environment,
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

    /// Update promoted_at timestamp for a staging environment to mark it as promoted to production.
    ///
    /// This function should only be called after validating that:
    /// 1. The environment exists
    /// 2. The promoted_at is currently NULL
    ///
    /// # Arguments
    /// * `environment` - The staging environment name to promote
    pub async fn update_promoted_at(&self, environment: &str) -> Result<(), ConnectionError> {
        let update_sql = r#"
            UPDATE deploy.deployments
            SET promoted_at = NOW()
            WHERE environment = $1
        "#;

        self.client
            .execute(update_sql, &[&environment])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(())
    }

    /// Delete all deployment records for a specific environment.
    ///
    /// This is used by the abort command to clean up a staged deployment.
    ///
    /// # Arguments
    /// * `environment` - The environment name to delete
    pub async fn delete_deployment(&self, environment: &str) -> Result<(), ConnectionError> {
        // Delete from deployments table
        let delete_deployments_sql = "DELETE FROM deploy.deployments WHERE environment = $1";
        self.client
            .execute(delete_deployments_sql, &[&environment])
            .await
            .map_err(ConnectionError::Query)?;

        // Delete from objects table
        let delete_objects_sql = "DELETE FROM deploy.objects WHERE environment = $1";
        self.client
            .execute(delete_objects_sql, &[&environment])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(())
    }
}
