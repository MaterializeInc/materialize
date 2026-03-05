//! Database validation operations.
//!
//! This module contains methods for validating projects against the database,
//! including checking for required databases, schemas, clusters, and privileges.

use crate::client::errors::DatabaseValidationError;
use crate::client::connection::ValidationClient;
use crate::project::ast::Statement;
use crate::project::object_id::ObjectId;
use crate::project::planned;
use mz_sql_parser::ast::CreateSinkConnection;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::path::PathBuf;
use tokio_postgres::Client as PgClient;
use tokio_postgres::types::ToSql;

const LOOKUP_BATCH_SIZE: usize = 1000;

enum CatalogLookup {
    Objects,
    Sources,
    Tables,
    Connections,
}

impl CatalogLookup {
    fn table_name(&self) -> &'static str {
        match self {
            CatalogLookup::Objects => "mz_objects",
            CatalogLookup::Sources => "mz_sources",
            CatalogLookup::Tables => "mz_tables",
            CatalogLookup::Connections => "mz_connections",
        }
    }
}

/// Internal helper to query which sources exist on the given clusters using IN clause.
pub(crate) async fn query_sources_by_cluster(
    client: &PgClient,
    cluster_names: &BTreeSet<String>,
) -> Result<BTreeMap<String, Vec<String>>, DatabaseValidationError> {
    if cluster_names.is_empty() {
        return Ok(BTreeMap::new());
    }

    // Build IN clause with placeholders
    let placeholders: Vec<String> = (1..=cluster_names.len())
        .map(|i| format!("${}", i))
        .collect();
    let in_clause = placeholders.join(", ");

    let query = format!(
        r#"
        SELECT
            c.name as cluster_name,
            d.name || '.' || s.name || '.' || mo.name as fqn
        FROM mz_catalog.mz_sources src
        JOIN mz_catalog.mz_objects mo ON src.id = mo.id
        JOIN mz_catalog.mz_schemas s ON mo.schema_id = s.id
        JOIN mz_catalog.mz_databases d ON s.database_id = d.id
        JOIN mz_catalog.mz_clusters c ON src.cluster_id = c.id
        WHERE mo.id LIKE 'u%' AND c.name IN ({})
        "#,
        in_clause
    );

    #[allow(clippy::as_conversions)]
    let params: Vec<&(dyn ToSql + Sync)> = cluster_names
        .iter()
        .map(|s| s as &(dyn ToSql + Sync))
        .collect();

    let rows = client
        .query(&query, &params)
        .await
        .map_err(DatabaseValidationError::QueryError)?;

    let mut result: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in rows {
        let cluster_name: String = row.get("cluster_name");
        let fqn: String = row.get("fqn");
        result
            .entry(cluster_name)
            .or_insert_with(Vec::new)
            .push(fqn);
    }

    Ok(result)
}

async fn query_existing_names(
    client: &PgClient,
    table_name: &str,
    column_name: &str,
    names: &BTreeSet<String>,
) -> Result<BTreeSet<String>, DatabaseValidationError> {
    let mut existing = BTreeSet::new();
    if names.is_empty() {
        return Ok(existing);
    }

    let name_list: Vec<String> = names.iter().cloned().collect();
    for chunk in name_list.chunks(LOOKUP_BATCH_SIZE) {
        let placeholders: Vec<String> = (1..=chunk.len()).map(|i| format!("${}", i)).collect();
        let query = format!(
            "SELECT {column} FROM {table} WHERE {column} IN ({placeholders})",
            column = column_name,
            table = table_name,
            placeholders = placeholders.join(", ")
        );

        #[allow(clippy::as_conversions)]
        let params: Vec<&(dyn ToSql + Sync)> =
            chunk.iter().map(|name| name as &(dyn ToSql + Sync)).collect();

        let rows = client
            .query(&query, &params)
            .await
            .map_err(DatabaseValidationError::QueryError)?;
        for row in rows {
            let name: String = row.get(column_name);
            existing.insert(name);
        }
    }

    Ok(existing)
}

async fn query_existing_schema_pairs(
    client: &PgClient,
    schema_pairs: &BTreeSet<(String, String)>,
) -> Result<BTreeSet<(String, String)>, DatabaseValidationError> {
    let mut existing = BTreeSet::new();
    if schema_pairs.is_empty() {
        return Ok(existing);
    }

    let fqn_to_pair: BTreeMap<String, (String, String)> = schema_pairs
        .iter()
        .map(|(database, schema)| (format!("{}.{}", database, schema), (database.clone(), schema.clone())))
        .collect();
    let fqns: Vec<String> = fqn_to_pair.keys().cloned().collect();

    for chunk in fqns.chunks(LOOKUP_BATCH_SIZE) {
        let placeholders: Vec<String> = (1..=chunk.len()).map(|i| format!("${}", i)).collect();
        let query = format!(
            r#"
            SELECT d.name || '.' || s.name AS fqn
            FROM mz_schemas s
            JOIN mz_databases d ON s.database_id = d.id
            WHERE d.name || '.' || s.name IN ({})
            "#,
            placeholders.join(", ")
        );

        #[allow(clippy::as_conversions)]
        let params: Vec<&(dyn ToSql + Sync)> =
            chunk.iter().map(|fqn| fqn as &(dyn ToSql + Sync)).collect();

        let rows = client
            .query(&query, &params)
            .await
            .map_err(DatabaseValidationError::QueryError)?;
        for row in rows {
            let fqn: String = row.get("fqn");
            if let Some(pair) = fqn_to_pair.get(&fqn) {
                existing.insert(pair.clone());
            }
        }
    }

    Ok(existing)
}

async fn query_existing_object_ids(
    client: &PgClient,
    object_ids: &BTreeSet<ObjectId>,
    lookup: CatalogLookup,
) -> Result<BTreeSet<ObjectId>, DatabaseValidationError> {
    let mut existing = BTreeSet::new();
    if object_ids.is_empty() {
        return Ok(existing);
    }

    let fqn_to_object: BTreeMap<String, ObjectId> = object_ids
        .iter()
        .map(|obj| (obj.to_string(), obj.clone()))
        .collect();
    let fqns: Vec<String> = fqn_to_object.keys().cloned().collect();
    let table_name = lookup.table_name();

    for chunk in fqns.chunks(LOOKUP_BATCH_SIZE) {
        let placeholders: Vec<String> = (1..=chunk.len()).map(|i| format!("${}", i)).collect();
        let query = format!(
            r#"
            SELECT d.name || '.' || s.name || '.' || t.name AS fqn
            FROM {table_name} t
            JOIN mz_schemas s ON t.schema_id = s.id
            JOIN mz_databases d ON s.database_id = d.id
            WHERE d.name || '.' || s.name || '.' || t.name IN ({placeholders})
            "#,
            table_name = table_name,
            placeholders = placeholders.join(", ")
        );

        #[allow(clippy::as_conversions)]
        let params: Vec<&(dyn ToSql + Sync)> =
            chunk.iter().map(|fqn| fqn as &(dyn ToSql + Sync)).collect();

        let rows = client
            .query(&query, &params)
            .await
            .map_err(DatabaseValidationError::QueryError)?;
        for row in rows {
            let fqn: String = row.get("fqn");
            if let Some(obj) = fqn_to_object.get(&fqn) {
                existing.insert(obj.clone());
            }
        }
    }

    Ok(existing)
}

/// Internal implementation of validate_project.
pub(crate) async fn validate_project_impl(
    client: &PgClient,
    planned_project: &planned::Project,
    project_root: &Path,
) -> Result<(), DatabaseValidationError> {
    let (external_databases, external_schemas) = collect_external_dependencies(planned_project);
    let missing_databases = find_missing_databases(client, &external_databases).await?;
    let missing_schemas = find_missing_schemas(client, &external_schemas).await?;
    let missing_clusters = find_missing_clusters(client, planned_project).await?;
    let object_paths = build_object_paths(planned_project, project_root);
    let missing_external_deps = find_missing_external_dependencies(client, planned_project).await?;
    let compilation_errors =
        build_compilation_errors(planned_project, &object_paths, &missing_external_deps);

    if !missing_databases.is_empty()
        || !missing_schemas.is_empty()
        || !missing_clusters.is_empty()
        || !compilation_errors.is_empty()
    {
        Err(DatabaseValidationError::Multiple {
            databases: missing_databases,
            schemas: missing_schemas,
            clusters: missing_clusters,
            compilation_errors,
        })
    } else {
        Ok(())
    }
}

/// Derives the set of external database/schema prerequisites from project dependencies.
///
/// Project-owned databases are excluded because deployment can create them if needed.
fn collect_external_dependencies(
    planned_project: &planned::Project,
) -> (BTreeSet<String>, BTreeSet<(String, String)>) {
    let project_databases: BTreeSet<_> = planned_project
        .databases
        .iter()
        .map(|db| db.name.clone())
        .collect();

    let mut external_databases = BTreeSet::new();
    let mut external_schemas = BTreeSet::new();
    for ext_dep in &planned_project.external_dependencies {
        if !project_databases.contains(&ext_dep.database) {
            external_databases.insert(ext_dep.database.clone());
        }
        external_schemas.insert((ext_dep.database.clone(), ext_dep.schema.clone()));
    }
    (external_databases, external_schemas)
}

/// Checks catalog state for external databases that must pre-exist.
async fn find_missing_databases(
    client: &PgClient,
    external_databases: &BTreeSet<String>,
) -> Result<Vec<String>, DatabaseValidationError> {
    let existing = query_existing_names(client, "mz_databases", "name", external_databases).await?;
    Ok(external_databases
        .difference(&existing)
        .cloned()
        .collect())
}

/// Checks catalog state for external schemas that must pre-exist.
async fn find_missing_schemas(
    client: &PgClient,
    external_schemas: &BTreeSet<(String, String)>,
) -> Result<Vec<(String, String)>, DatabaseValidationError> {
    let existing = query_existing_schema_pairs(client, external_schemas).await?;
    Ok(external_schemas
        .difference(&existing)
        .cloned()
        .collect())
}

/// Checks whether all cluster dependencies referenced by the project are present.
async fn find_missing_clusters(
    client: &PgClient,
    planned_project: &planned::Project,
) -> Result<Vec<String>, DatabaseValidationError> {
    let required: BTreeSet<String> = planned_project
        .cluster_dependencies
        .iter()
        .map(|cluster| cluster.name.clone())
        .collect();
    let existing = query_existing_names(client, "mz_clusters", "name", &required).await?;
    Ok(required.difference(&existing).cloned().collect())
}

/// Reconstructs source file paths for planned objects under `models/`.
///
/// These paths are used to attach dependency errors to concrete files for users.
fn build_object_paths(
    planned_project: &planned::Project,
    project_root: &Path,
) -> BTreeMap<ObjectId, PathBuf> {
    let mut object_paths = BTreeMap::new();
    for db in &planned_project.databases {
        for schema in &db.schemas {
            for obj in &schema.objects {
                let file_path = project_root
                    .join("models")
                    .join(&obj.id.database)
                    .join(&obj.id.schema)
                    .join(format!("{}.sql", obj.id.object));
                object_paths.insert(obj.id.clone(), file_path);
            }
        }
    }
    object_paths
}

/// Checks whether externally-referenced objects actually exist in the target catalog.
async fn find_missing_external_dependencies(
    client: &PgClient,
    planned_project: &planned::Project,
) -> Result<BTreeSet<ObjectId>, DatabaseValidationError> {
    let external_deps: BTreeSet<ObjectId> =
        planned_project.external_dependencies.iter().cloned().collect();
    let existing = query_existing_object_ids(client, &external_deps, CatalogLookup::Objects).await?;
    Ok(external_deps.difference(&existing).cloned().collect())
}

/// Converts missing external dependencies into user-facing, file-scoped errors.
///
/// Grouping by file/object keeps output aligned with how users navigate project SQL.
fn build_compilation_errors(
    planned_project: &planned::Project,
    object_paths: &BTreeMap<ObjectId, PathBuf>,
    missing_external_deps: &BTreeSet<ObjectId>,
) -> Vec<DatabaseValidationError> {
    let mut errors = Vec::new();
    for db in &planned_project.databases {
        for schema in &db.schemas {
            for obj in &schema.objects {
                let missing_for_object: Vec<_> = obj
                    .dependencies
                    .iter()
                    .filter(|dep| missing_external_deps.contains(*dep))
                    .cloned()
                    .collect();
                if missing_for_object.is_empty() {
                    continue;
                }
                if let Some(file_path) = object_paths.get(&obj.id) {
                    errors.push(DatabaseValidationError::CompilationFailed {
                        file_path: file_path.clone(),
                        object_name: obj.id.clone(),
                        missing_dependencies: missing_for_object,
                    });
                }
            }
        }
    }
    errors
}

impl ValidationClient<'_> {
    /// Validate that all required databases, schemas, and external dependencies exist.
    pub async fn validate_project(
        &self,
        planned_project: &planned::Project,
        project_root: &Path,
    ) -> Result<(), DatabaseValidationError> {
        validate_project_impl(self.client.postgres_client(), planned_project, project_root).await
    }

    /// Validate that sources and sinks don't share clusters with indexes or materialized views.
    pub async fn validate_cluster_isolation(
        &self,
        planned_project: &planned::Project,
    ) -> Result<(), DatabaseValidationError> {
        validate_cluster_isolation_impl(self.client.postgres_client(), planned_project).await
    }

    /// Validate that the user has sufficient privileges to deploy the project.
    pub async fn validate_privileges(
        &self,
        planned_project: &planned::Project,
    ) -> Result<(), DatabaseValidationError> {
        validate_privileges_impl(self.client.postgres_client(), planned_project).await
    }

    /// Validate that all sources referenced by CREATE TABLE FROM SOURCE statements exist.
    pub async fn validate_sources_exist(
        &self,
        planned_project: &planned::Project,
    ) -> Result<(), DatabaseValidationError> {
        validate_sources_exist_impl(self.client.postgres_client(), planned_project).await
    }

    /// Validate that all connections referenced by CREATE SINK statements exist.
    pub async fn validate_sink_connections_exist(
        &self,
        planned_project: &planned::Project,
    ) -> Result<(), DatabaseValidationError> {
        validate_sink_connections_exist_impl(self.client.postgres_client(), planned_project).await
    }

    /// Validate that all tables referenced by objects to be deployed exist in the database.
    pub async fn validate_table_dependencies(
        &self,
        planned_project: &planned::Project,
        objects_to_deploy: &BTreeSet<ObjectId>,
    ) -> Result<(), DatabaseValidationError> {
        validate_table_dependencies_impl(
            self.client.postgres_client(),
            planned_project,
            objects_to_deploy,
        )
        .await
    }
}

/// Internal implementation of validate_cluster_isolation.
pub(crate) async fn validate_cluster_isolation_impl(
    client: &PgClient,
    planned_project: &planned::Project,
) -> Result<(), DatabaseValidationError> {
    // Get all clusters used by the project
    let mut all_clusters: BTreeSet<String> = BTreeSet::new();
    for cluster in &planned_project.cluster_dependencies {
        all_clusters.insert(cluster.name.clone());
    }

    // Query sources from the database for these clusters
    let sources_by_cluster = query_sources_by_cluster(client, &all_clusters).await?;

    // Validate cluster isolation using the project's validation method
    planned_project
        .validate_cluster_isolation(&sources_by_cluster)
        .map_err(|(cluster_name, compute_objects, storage_objects)| {
            DatabaseValidationError::ClusterConflict {
                cluster_name,
                compute_objects,
                storage_objects,
            }
        })
}

/// Internal implementation of validate_privileges.
pub(crate) async fn validate_privileges_impl(
    client: &PgClient,
    planned_project: &planned::Project,
) -> Result<(), DatabaseValidationError> {
    // Check if user is a superuser
    let row = client
        .query_one("SELECT mz_is_superuser()", &[])
        .await
        .map_err(DatabaseValidationError::QueryError)?;
    let is_superuser: bool = row.get(0);

    if is_superuser {
        return Ok(()); // Superuser has all privileges
    }

    // Collect all required databases from the project
    let mut priv_required_databases = BTreeSet::new();
    for db in &planned_project.databases {
        priv_required_databases.insert(db.name.clone());
    }

    // Check USAGE privileges on databases using the provided query
    let missing_usage = if !priv_required_databases.is_empty() {
        // Build IN clause with placeholders
        let placeholders: Vec<String> = (1..=priv_required_databases.len())
            .map(|i| format!("${}", i))
            .collect();
        let in_clause = placeholders.join(", ");

        let query = format!(
            r#"
            SELECT name
            FROM mz_internal.mz_show_my_database_privileges
            WHERE name IN ({})
            GROUP BY name
            HAVING NOT BOOL_OR(privilege_type = 'USAGE')
            "#,
            in_clause
        );

        #[allow(clippy::as_conversions)]
        let params: Vec<&(dyn ToSql + Sync)> = priv_required_databases
            .iter()
            .map(|s| s as &(dyn ToSql + Sync))
            .collect();

        let rows = client
            .query(&query, &params)
            .await
            .map_err(DatabaseValidationError::QueryError)?;

        rows.iter()
            .map(|row| row.get::<_, String>("name"))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    // Check CREATECLUSTER privilege if project has cluster dependencies
    let missing_createcluster = if !planned_project.cluster_dependencies.is_empty() {
        let query = r#"
            SELECT EXISTS (
                SELECT * FROM mz_internal.mz_show_my_system_privileges
                WHERE privilege_type = 'CREATECLUSTER'
            )
        "#;

        let row = client
            .query_one(query, &[])
            .await
            .map_err(DatabaseValidationError::QueryError)?;

        let has_createcluster: bool = row.get(0);
        !has_createcluster
    } else {
        false
    };

    // Return error if missing any privileges
    if !missing_usage.is_empty() || missing_createcluster {
        return Err(DatabaseValidationError::InsufficientPrivileges {
            missing_database_usage: missing_usage,
            missing_createcluster,
        });
    }

    Ok(())
}

/// Internal implementation of validate_sources_exist.
pub(crate) async fn validate_sources_exist_impl(
    client: &PgClient,
    planned_project: &planned::Project,
) -> Result<(), DatabaseValidationError> {
    let defined_sources: BTreeSet<ObjectId> = planned_project
        .iter_objects()
        .filter(|obj| matches!(obj.typed_object.stmt, Statement::CreateSource(_)))
        .map(|obj| obj.id.clone())
        .collect();

    let mut referenced_sources = BTreeSet::new();
    for obj in planned_project.iter_objects() {
        if let Statement::CreateTableFromSource(ref stmt) = obj.typed_object.stmt {
            let source_id =
                ObjectId::from_raw_item_name(&stmt.source, &obj.id.database, &obj.id.schema);
            if !defined_sources.contains(&source_id) {
                referenced_sources.insert(source_id);
            }
        }
    }

    let existing = query_existing_object_ids(client, &referenced_sources, CatalogLookup::Sources).await?;
    let missing_sources: Vec<ObjectId> = referenced_sources.difference(&existing).cloned().collect();
    if !missing_sources.is_empty() {
        return Err(DatabaseValidationError::MissingSources(missing_sources));
    }

    Ok(())
}

/// Internal implementation of validate_sink_connections_exist.
///
/// Validates that all connections referenced by sinks exist in the database.
/// Sinks reference connections (Kafka, Iceberg) that are not managed by mz-deploy.
pub(crate) async fn validate_sink_connections_exist_impl(
    client: &PgClient,
    planned_project: &planned::Project,
) -> Result<(), DatabaseValidationError> {
    let mut referenced_connections = BTreeSet::new();
    for obj in planned_project.iter_objects() {
        if let Statement::CreateSink(ref stmt) = obj.typed_object.stmt {
            let connection_ids = match &stmt.connection {
                CreateSinkConnection::Kafka { connection, .. } => {
                    vec![ObjectId::from_raw_item_name(
                        connection,
                        &obj.id.database,
                        &obj.id.schema,
                    )]
                }
                CreateSinkConnection::Iceberg {
                    connection,
                    aws_connection,
                    ..
                } => {
                    vec![
                        ObjectId::from_raw_item_name(connection, &obj.id.database, &obj.id.schema),
                        ObjectId::from_raw_item_name(
                            aws_connection,
                            &obj.id.database,
                            &obj.id.schema,
                        ),
                    ]
                }
            };

            for conn_id in connection_ids {
                referenced_connections.insert(conn_id);
            }
        }
    }

    let existing = query_existing_object_ids(
        client,
        &referenced_connections,
        CatalogLookup::Connections,
    )
    .await?;
    let missing_connections: Vec<ObjectId> = referenced_connections
        .difference(&existing)
        .cloned()
        .collect();
    if !missing_connections.is_empty() {
        return Err(DatabaseValidationError::MissingConnections(missing_connections));
    }

    Ok(())
}

/// Internal implementation of validate_table_dependencies.
pub(crate) async fn validate_table_dependencies_impl(
    client: &PgClient,
    planned_project: &planned::Project,
    objects_to_deploy: &BTreeSet<ObjectId>,
) -> Result<(), DatabaseValidationError> {
    let project_tables: BTreeSet<ObjectId> = planned_project.get_tables().collect();

    let mut required_tables = BTreeSet::new();
    for object_id in objects_to_deploy {
        if let Some(obj) = planned_project.find_object(object_id) {
            for dep_id in &obj.dependencies {
                if project_tables.contains(dep_id) {
                    required_tables.insert(dep_id.clone());
                }
            }
        }
    }

    let existing_tables =
        query_existing_object_ids(client, &required_tables, CatalogLookup::Tables).await?;
    let missing_table_set: BTreeSet<ObjectId> =
        required_tables.difference(&existing_tables).cloned().collect();

    let mut objects_needing_tables = Vec::new();
    for object_id in objects_to_deploy {
        if let Some(obj) = planned_project.find_object(object_id) {
            let mut missing_tables = Vec::new();
            for dep_id in &obj.dependencies {
                if project_tables.contains(dep_id) && missing_table_set.contains(dep_id) {
                    missing_tables.push(dep_id.clone());
                }
            }

            if !missing_tables.is_empty() {
                objects_needing_tables.push((object_id.clone(), missing_tables));
            }
        }
    }

    if !objects_needing_tables.is_empty() {
        return Err(DatabaseValidationError::MissingTableDependencies {
            objects_needing_tables,
        });
    }

    Ok(())
}
