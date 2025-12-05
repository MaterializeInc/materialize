//! Database validation operations.
//!
//! This module contains methods for validating projects against the database,
//! including checking for required databases, schemas, clusters, and privileges.

use crate::client::errors::DatabaseValidationError;
use crate::project::ast::Statement;
use crate::project::object_id::ObjectId;
use crate::project::planned;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::path::PathBuf;
use tokio_postgres::types::ToSql;
use tokio_postgres::Client as PgClient;

/// Internal helper to query which sources exist on the given clusters using IN clause.
pub(crate) async fn query_sources_by_cluster(
    client: &PgClient,
    cluster_names: &HashSet<String>,
) -> Result<HashMap<String, Vec<String>>, DatabaseValidationError> {
    if cluster_names.is_empty() {
        return Ok(HashMap::new());
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

    let params: Vec<&(dyn ToSql + Sync)> = cluster_names
        .iter()
        .map(|s| s as &(dyn ToSql + Sync))
        .collect();

    let rows = client
        .query(&query, &params)
        .await
        .map_err(DatabaseValidationError::QueryError)?;

    let mut result: HashMap<String, Vec<String>> = HashMap::new();
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

/// Internal implementation of validate_project.
pub(crate) async fn validate_project_impl(
    client: &PgClient,
    planned_project: &planned::Project,
    project_root: &Path,
) -> Result<(), DatabaseValidationError> {
    let mut missing_databases = Vec::new();
    let mut missing_schemas = Vec::new();
    let mut missing_clusters = Vec::new();

    // Collect all required databases
    let mut required_databases = HashSet::new();
    for db in &planned_project.databases {
        required_databases.insert(db.name.clone());
    }
    for ext_dep in &planned_project.external_dependencies {
        required_databases.insert(ext_dep.database.clone());
    }

    // Collect schemas - split into project schemas (we can create) vs external schemas (must exist)
    let mut external_schemas = HashSet::new();
    for ext_dep in &planned_project.external_dependencies {
        external_schemas.insert((ext_dep.database.clone(), ext_dep.schema.clone()));
    }

    // Check databases exist
    for database in &required_databases {
        let query = "SELECT name FROM mz_databases WHERE name = $1";
        let rows = client
            .query(query, &[database])
            .await
            .map_err(DatabaseValidationError::QueryError)?;
        if rows.is_empty() {
            missing_databases.push(database.clone());
        }
    }

    // Check only external dependency schemas exist (project schemas will be created if needed)
    for (database, schema) in &external_schemas {
        let query = r#"
            SELECT s.name FROM mz_schemas s
            JOIN mz_databases d ON s.database_id = d.id
            WHERE s.name = $1 AND d.name = $2"#;
        let rows = client
            .query(query, &[schema, database])
            .await
            .map_err(DatabaseValidationError::QueryError)?;
        if rows.is_empty() {
            missing_schemas.push((database.clone(), schema.clone()));
        }
    }

    // Check clusters exist
    for cluster in &planned_project.cluster_dependencies {
        let query = "SELECT name FROM mz_clusters WHERE name = $1";
        let rows = client
            .query(query, &[&cluster.name])
            .await
            .map_err(DatabaseValidationError::QueryError)?;
        if rows.is_empty() {
            missing_clusters.push(cluster.name.clone());
        }
    }

    // Build ObjectId to file path mapping by reconstructing paths from ObjectIds
    // Path format: <root>/<database>/<schema>/<object>.sql
    let mut object_paths: HashMap<ObjectId, PathBuf> = HashMap::new();
    for db in &planned_project.databases {
        for schema in &db.schemas {
            for obj in &schema.objects {
                let file_path = project_root
                    .join(&obj.id.database)
                    .join(&obj.id.schema)
                    .join(format!("{}.sql", obj.id.object));
                object_paths.insert(obj.id.clone(), file_path);
            }
        }
    }

    // Check external dependencies and group missing ones by file
    let mut missing_external_deps = HashSet::new();
    for ext_dep in &planned_project.external_dependencies {
        let query = r#"
            SELECT mo.name
            FROM mz_objects mo
            JOIN mz_schemas s ON mo.schema_id = s.id
            JOIN mz_databases d ON s.database_id = d.id
            WHERE mo.name = $1 AND s.name = $2 AND d.name = $3
           "#;

        let rows = client
            .query(
                query,
                &[&ext_dep.object, &ext_dep.schema, &ext_dep.database],
            )
            .await
            .map_err(DatabaseValidationError::QueryError)?;
        if rows.is_empty() {
            missing_external_deps.insert(ext_dep.clone());
        }
    }

    // Group missing dependencies by the files that reference them
    let mut file_missing_deps: HashMap<PathBuf, (ObjectId, Vec<ObjectId>)> = HashMap::new();

    for db in &planned_project.databases {
        for schema in &db.schemas {
            for obj in &schema.objects {
                let mut missing_for_this_object = Vec::new();

                for dep in &obj.dependencies {
                    if missing_external_deps.contains(dep) {
                        missing_for_this_object.push(dep.clone());
                    }
                }

                if !missing_for_this_object.is_empty()
                    && let Some(file_path) = object_paths.get(&obj.id)
                {
                    file_missing_deps
                        .insert(file_path.clone(), (obj.id.clone(), missing_for_this_object));
                }
            }
        }
    }

    // Create compilation error for each file with missing dependencies
    let mut compilation_errors = Vec::new();
    for (file_path, (object_id, missing_deps)) in file_missing_deps {
        compilation_errors.push(DatabaseValidationError::CompilationFailed {
            file_path,
            object_name: object_id,
            missing_dependencies: missing_deps,
        });
    }

    // Return results
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

/// Internal implementation of validate_cluster_isolation.
pub(crate) async fn validate_cluster_isolation_impl(
    client: &PgClient,
    planned_project: &planned::Project,
) -> Result<(), DatabaseValidationError> {
    // Get all clusters used by the project
    let mut all_clusters: HashSet<String> = HashSet::new();
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
    let mut required_databases = HashSet::new();
    for db in &planned_project.databases {
        required_databases.insert(db.name.clone());
    }

    // Check USAGE privileges on databases using the provided query
    let missing_usage = if !required_databases.is_empty() {
        // Build IN clause with placeholders
        let placeholders: Vec<String> = (1..=required_databases.len())
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

        let params: Vec<&(dyn ToSql + Sync)> = required_databases
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
    let mut missing_sources = Vec::new();

    // Collect all source references from CREATE TABLE FROM SOURCE statements
    for obj in planned_project.iter_objects() {
        if let Statement::CreateTableFromSource(ref stmt) = obj.typed_object.stmt {
            // Extract the source ObjectId from the statement
            let source_id =
                ObjectId::from_raw_item_name(&stmt.source, &obj.id.database, &obj.id.schema);

            // Check if source exists in the database
            let query = r#"
                SELECT s.name
                FROM mz_sources s
                JOIN mz_schemas sch ON s.schema_id = sch.id
                JOIN mz_databases d ON sch.database_id = d.id
                WHERE s.name = $1 AND sch.name = $2 AND d.name = $3"#;

            let rows = client
                .query(
                    query,
                    &[&source_id.object, &source_id.schema, &source_id.database],
                )
                .await
                .map_err(DatabaseValidationError::QueryError)?;

            if rows.is_empty() {
                missing_sources.push(source_id);
            }
        }
    }

    if !missing_sources.is_empty() {
        return Err(DatabaseValidationError::MissingSources(missing_sources));
    }

    Ok(())
}

/// Internal implementation of validate_table_dependencies.
pub(crate) async fn validate_table_dependencies_impl(
    client: &PgClient,
    planned_project: &planned::Project,
    objects_to_deploy: &HashSet<ObjectId>,
) -> Result<(), DatabaseValidationError> {
    let mut objects_needing_tables = Vec::new();

    // Build a set of all table IDs in the project
    let project_tables: HashSet<ObjectId> = planned_project.get_tables().collect();

    // For each object to be deployed, check if it depends on tables
    for object_id in objects_to_deploy {
        // Find the object in the planned project
        if let Some(obj) = planned_project.find_object(object_id) {
            let mut missing_tables = Vec::new();

            // Check each dependency
            for dep_id in &obj.dependencies {
                // Is this dependency a table?
                if project_tables.contains(dep_id) {
                    // Check if the table exists in the database
                    let query = r#"
                        SELECT t.name
                        FROM mz_tables t
                        JOIN mz_schemas s ON t.schema_id = s.id
                        JOIN mz_databases d ON s.database_id = d.id
                        WHERE t.name = $1 AND s.name = $2 AND d.name = $3"#;

                    let rows = client
                        .query(query, &[&dep_id.object, &dep_id.schema, &dep_id.database])
                        .await
                        .map_err(DatabaseValidationError::QueryError)?;

                    if rows.is_empty() {
                        missing_tables.push(dep_id.clone());
                    }
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
