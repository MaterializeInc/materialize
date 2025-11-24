//! Destruction API for dropping Materialize resources.
//!
//! This module provides methods for safely dropping database objects
//! in the correct order (respecting dependencies).

use std::collections::HashSet;
use super::connection::ConnectionError;
use tokio_postgres::Client as PgClient;
use crate::project::object_id::ObjectId;

/// API for dropping Materialize resources.
///
/// All methods use DROP IF EXISTS for idempotency.
pub struct DestructionApi<'a> {
    client: &'a PgClient,
}

impl<'a> DestructionApi<'a> {
    /// Create a new destruction API wrapping a PostgreSQL client.
    pub(crate) fn new(client: &'a PgClient) -> Self {
        Self { client }
    }

    /// Drop all objects in a schema.
    ///
    /// This drops all views, materialized views, tables, sources, and sinks
    /// in the specified schema. Objects are dropped in reverse dependency order
    /// using CASCADE to handle dependencies.
    ///
    /// # Arguments
    /// * `database` - Database name (e.g., "materialize")
    /// * `schema` - Schema name (e.g., "public")
    ///
    /// # Returns
    /// `Ok(Vec<String>)` with the list of dropped object FQNs
    pub async fn drop_schema_objects(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<String>, ConnectionError> {
        // Query all objects in the schema
        let query = r#"
            SELECT mo.name, mo.type
            FROM mz_objects mo
            JOIN mz_schemas s ON mo.schema_id = s.id
            JOIN mz_databases d ON s.database_id = d.id
            WHERE d.name = $1 AND s.name = $2
            AND mo.type IN ('table', 'view', 'materialized-view', 'source', 'sink')
            ORDER BY mo.id DESC
        "#;

        let rows = self
            .client
            .query(query, &[&database, &schema])
            .await
            .map_err(ConnectionError::Query)?;

        let mut dropped = Vec::new();
        for row in rows {
            let name: String = row.get("name");
            let obj_type: String = row.get("type");

            let fqn = format!("{}.{}.{}", database, schema, name);
            let drop_type = match obj_type.as_str() {
                "table" => "TABLE",
                "view" => "VIEW",
                "materialized-view" => "MATERIALIZED VIEW",
                "source" => "SOURCE",
                "sink" => "SINK",
                _ => continue,
            };

            let drop_sql = format!("DROP {} IF EXISTS {} CASCADE", drop_type, fqn);
            self.client
                .execute(&drop_sql, &[])
                .await
                .map_err(ConnectionError::Query)?;

            dropped.push(fqn);
        }

        Ok(dropped)
    }

    /// Drop specific objects by their ObjectIds.
    ///
    /// Objects are dropped using IF EXISTS and CASCADE to handle dependencies.
    ///
    /// # Arguments
    /// * `objects` - Set of ObjectIds to drop
    ///
    /// # Returns
    /// `Ok(Vec<String>)` with the list of dropped object FQNs
    pub async fn drop_objects(
        &self,
        objects: &HashSet<ObjectId>,
    ) -> Result<Vec<String>, ConnectionError> {
        let mut dropped = Vec::new();

        if objects.is_empty() {
            return Ok(dropped);
        }

        let placeholders: Vec<String> = (1..=objects.len()).map(|i| format!("${}", i)).collect();
        let placeholders_str = placeholders.join(", ");

        // Query to get object types
        let query = format!(
            r#"
            SELECT mo.name, s.name as schema_name, d.name as database_name, mo.type
            FROM mz_objects mo
            JOIN mz_schemas s ON mo.schema_id = s.id
            JOIN mz_databases d ON s.database_id = d.id
            WHERE d.name || '.' || s.name || '.' || mo.name IN ({})
            AND mo.type IN ('table', 'view', 'materialized-view', 'source', 'sink')
            ORDER BY mo.id DESC
        "#,
            placeholders_str
        );

        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
        let fqns: Vec<_> = objects.into_iter().map(|object| object.to_string()).collect();
        for fqn in &fqns {
            params.push(fqn);
        }

        let rows = self
            .client
            .query(&query, &params)
            .await
            .map_err(ConnectionError::Query)?;

        for row in rows {
            let name: String = row.get("name");
            let schema: String = row.get("schema_name");
            let database: String = row.get("database_name");
            let obj_type: String = row.get("type");

            let fqn = format!("{}.{}.{}", database, schema, name);
            let drop_type = match obj_type.as_str() {
                "table" => "TABLE",
                "view" => "VIEW",
                "materialized-view" => "MATERIALIZED VIEW",
                "source" => "SOURCE",
                "sink" => "SINK",
                _ => continue,
            };

            let drop_sql = format!("DROP {} IF EXISTS {} CASCADE", drop_type, fqn);
            self.client
                .execute(&drop_sql, &[])
                .await
                .map_err(ConnectionError::Query)?;

            dropped.push(fqn);
        }

        Ok(dropped)
    }

    /// Drop staging schemas by name.
    ///
    /// Drops each schema with CASCADE to handle dependencies.
    ///
    /// # Arguments
    /// * `schemas` - List of (database, schema) tuples to drop
    pub async fn drop_staging_schemas(
        &self,
        schemas: &[(String, String)],
    ) -> Result<(), ConnectionError> {
        for (database, schema) in schemas {
            let drop_sql = format!(
                "DROP SCHEMA IF EXISTS \"{}\".\"{}\" CASCADE",
                database, schema
            );
            self.client
                .execute(&drop_sql, &[])
                .await
                .map_err(ConnectionError::Query)?;
        }

        Ok(())
    }

    /// Drop staging clusters by name.
    ///
    /// Drops each cluster with CASCADE to handle dependencies.
    ///
    /// # Arguments
    /// * `clusters` - List of cluster names to drop
    pub async fn drop_staging_clusters(&self, clusters: &[String]) -> Result<(), ConnectionError> {
        for cluster in clusters {
            let drop_sql = format!("DROP CLUSTER IF EXISTS \"{}\" CASCADE", cluster);
            self.client
                .execute(&drop_sql, &[])
                .await
                .map_err(ConnectionError::Query)?;
        }

        Ok(())
    }
}
