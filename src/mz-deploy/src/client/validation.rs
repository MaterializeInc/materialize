// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Database validation operations.
//!
//! Validates that the target Materialize environment satisfies all prerequisites
//! for deploying the project. Runs before any DDL is executed.
//!
//! ## Validation Checklist
//!
//! | Check | Function | What it verifies |
//! |-------|----------|-----------------|
//! | External databases exist | `find_missing_databases` | Databases referenced by external dependencies are present |
//! | External schemas exist | `find_missing_schemas` | Schemas referenced by external dependencies are present |
//! | Clusters exist | `find_missing_clusters` | All clusters in `project.cluster_dependencies` are present |
//! | External objects exist | `find_missing_external_dependencies` | Objects outside the project that are referenced exist in the catalog |
//! | Cluster isolation | `validate_cluster_isolation_impl` | Sources/sinks don't share clusters with MVs/indexes (prevents accidental recreation during swap) |
//! | Privileges | `validate_privileges_impl` | Current role has USAGE on databases and CREATECLUSTER system privilege |
//! | Sources exist | `validate_sources_exist_impl` | Sources referenced by `CREATE TABLE FROM SOURCE` exist |
//! | Sink connections exist | `validate_sink_connections_exist_impl` | Connections referenced by sinks exist |
//! | Schema ownership | `validate_schema_ownership_impl` | Current role owns all production schemas that will be swapped |
//! | Cluster ownership | `validate_cluster_ownership_impl` | Current role owns all production clusters that will be swapped |
//! | Table dependencies | `validate_table_dependencies_impl` | Tables depended on by objects being deployed exist |
//! | Source references | `validate_source_references_impl` | Each `CREATE TABLE FROM SOURCE` names an object its source can read |

//!
//! ## Batching Strategy
//!
//! Catalog lookups use `IN` clause queries batched in chunks of
//! `LOOKUP_BATCH_SIZE` (1000) to avoid exceeding query parameter limits
//! while minimizing round trips.

use crate::client::connection::{Client, ValidationClient};
use crate::client::errors::{
    DatabaseValidationError, MissingSourceReference, SourceReferenceMismatch,
};
use crate::client::sql_placeholders;
use crate::project::SchemaQualifier;
use crate::project::ast::Statement;
use crate::project::ir::graph;
use crate::project::ir::object_id::ObjectId;
use crate::suggest::{MAX_DID_YOU_MEAN, did_you_mean};
use crate::verbose;
use mz_sql_parser::ast::{CreateSinkConnection, Ident, UnresolvedItemName};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;
use std::path::PathBuf;
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
    client: &Client,
    cluster_names: &BTreeSet<String>,
) -> Result<BTreeMap<String, Vec<String>>, DatabaseValidationError> {
    if cluster_names.is_empty() {
        return Ok(BTreeMap::new());
    }

    let in_clause = sql_placeholders(cluster_names.len());

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
    client: &Client,
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
        let placeholders = sql_placeholders(chunk.len());
        let query = format!(
            "SELECT {column} FROM {table} WHERE {column} IN ({placeholders})",
            column = column_name,
            table = table_name,
            placeholders = placeholders
        );

        #[allow(clippy::as_conversions)]
        let params: Vec<&(dyn ToSql + Sync)> = chunk
            .iter()
            .map(|name| name as &(dyn ToSql + Sync))
            .collect();

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
    client: &Client,
    schema_pairs: &BTreeSet<(String, String)>,
) -> Result<BTreeSet<(String, String)>, DatabaseValidationError> {
    let mut existing = BTreeSet::new();
    if schema_pairs.is_empty() {
        return Ok(existing);
    }

    let fqn_to_pair: BTreeMap<String, (String, String)> = schema_pairs
        .iter()
        .map(|(database, schema)| {
            (
                format!("{}.{}", database, schema),
                (database.clone(), schema.clone()),
            )
        })
        .collect();
    let fqns: Vec<String> = fqn_to_pair.keys().cloned().collect();

    for chunk in fqns.chunks(LOOKUP_BATCH_SIZE) {
        let placeholders = sql_placeholders(chunk.len());
        let query = format!(
            r#"
            SELECT d.name || '.' || s.name AS fqn
            FROM mz_schemas s
            JOIN mz_databases d ON s.database_id = d.id
            WHERE d.name || '.' || s.name IN ({})
            "#,
            placeholders
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
    client: &Client,
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
        let placeholders = sql_placeholders(chunk.len());
        let query = format!(
            r#"
            SELECT d.name || '.' || s.name || '.' || t.name AS fqn
            FROM {table_name} t
            JOIN mz_schemas s ON t.schema_id = s.id
            JOIN mz_databases d ON s.database_id = d.id
            WHERE d.name || '.' || s.name || '.' || t.name IN ({placeholders})
            "#,
            table_name = table_name,
            placeholders = placeholders
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
    client: &Client,
    planned_project: &graph::Project,
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
    planned_project: &graph::Project,
) -> (BTreeSet<String>, BTreeSet<(String, String)>) {
    let project_databases: BTreeSet<_> = planned_project
        .databases
        .iter()
        .map(|db| db.name.clone())
        .collect();

    let mut external_databases = BTreeSet::new();
    let mut external_schemas = BTreeSet::new();
    for ext_dep in &planned_project.external_dependencies {
        // System-schema deps have no database, so there's nothing to require.
        let Some(db) = ext_dep.database() else {
            continue;
        };
        if !project_databases.contains(db) {
            external_databases.insert(db.to_string());
        }
        external_schemas.insert((db.to_string(), ext_dep.schema().to_string()));
    }
    (external_databases, external_schemas)
}

/// Checks catalog state for external databases that must pre-exist.
async fn find_missing_databases(
    client: &Client,
    external_databases: &BTreeSet<String>,
) -> Result<Vec<String>, DatabaseValidationError> {
    let existing = query_existing_names(client, "mz_databases", "name", external_databases).await?;
    Ok(external_databases.difference(&existing).cloned().collect())
}

/// Checks catalog state for external schemas that must pre-exist.
async fn find_missing_schemas(
    client: &Client,
    external_schemas: &BTreeSet<(String, String)>,
) -> Result<Vec<SchemaQualifier>, DatabaseValidationError> {
    let existing = query_existing_schema_pairs(client, external_schemas).await?;
    Ok(external_schemas
        .difference(&existing)
        .map(|(db, schema)| SchemaQualifier::new(db.clone(), schema.clone()))
        .collect())
}

/// Checks whether all cluster dependencies referenced by the project are present.
async fn find_missing_clusters(
    client: &Client,
    planned_project: &graph::Project,
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
    planned_project: &graph::Project,
    project_root: &Path,
) -> BTreeMap<ObjectId, PathBuf> {
    let mut object_paths = BTreeMap::new();
    for db in &planned_project.databases {
        for schema in &db.schemas {
            for obj in &schema.objects {
                let file_path = project_root
                    .join("models")
                    .join(obj.id.expect_database())
                    .join(obj.id.schema())
                    .join(format!("{}.sql", obj.id.object()));
                object_paths.insert(obj.id.clone(), file_path);
            }
        }
    }
    object_paths
}

/// Checks whether externally-referenced objects actually exist in the target catalog.
async fn find_missing_external_dependencies(
    client: &Client,
    planned_project: &graph::Project,
) -> Result<BTreeSet<ObjectId>, DatabaseValidationError> {
    // System-schema dependencies are database-less and always present. Their
    // 2-part name never matches the 3-part FQN the existence query builds, so
    // including them here would wrongly report them missing.
    let external_deps: BTreeSet<ObjectId> = planned_project
        .external_dependencies
        .iter()
        .filter(|dep| dep.database().is_some())
        .cloned()
        .collect();
    let existing =
        query_existing_object_ids(client, &external_deps, CatalogLookup::Objects).await?;
    Ok(external_deps.difference(&existing).cloned().collect())
}

/// Converts missing external dependencies into user-facing, file-scoped errors.
///
/// Grouping by file/object keeps output aligned with how users navigate project SQL.
fn build_compilation_errors(
    planned_project: &graph::Project,
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
        planned_project: &graph::Project,
        project_root: &Path,
    ) -> Result<(), DatabaseValidationError> {
        validate_project_impl(self.client, planned_project, project_root).await
    }

    /// Validate that sources and sinks don't share clusters with indexes or materialized views.
    pub async fn validate_cluster_isolation(
        &self,
        planned_project: &graph::Project,
    ) -> Result<(), DatabaseValidationError> {
        validate_cluster_isolation_impl(self.client, planned_project).await
    }

    /// Validate that the user has sufficient privileges to deploy the project.
    pub async fn validate_privileges(
        &self,
        planned_project: &graph::Project,
    ) -> Result<(), DatabaseValidationError> {
        validate_privileges_impl(self.client, planned_project).await
    }

    /// Validate that all sources referenced by CREATE TABLE FROM SOURCE statements exist.
    pub async fn validate_sources_exist(
        &self,
        planned_project: &graph::Project,
    ) -> Result<(), DatabaseValidationError> {
        validate_sources_exist_impl(self.client, planned_project).await
    }

    /// Validate that all connections referenced by CREATE SINK statements exist.
    pub async fn validate_sink_connections_exist(
        &self,
        planned_project: &graph::Project,
    ) -> Result<(), DatabaseValidationError> {
        validate_sink_connections_exist_impl(self.client, planned_project).await
    }

    /// Validate that the current role owns all production schemas that will be swapped.
    pub async fn validate_schema_ownership(
        &self,
        schema_set: &BTreeSet<SchemaQualifier>,
    ) -> Result<(), DatabaseValidationError> {
        validate_schema_ownership_impl(self.client, schema_set).await
    }

    /// Validate that the current role owns all production clusters that will be swapped.
    pub async fn validate_cluster_ownership(
        &self,
        cluster_set: &BTreeSet<String>,
    ) -> Result<(), DatabaseValidationError> {
        validate_cluster_ownership_impl(self.client, cluster_set).await
    }

    /// Validate that every `CREATE TABLE ... FROM SOURCE` in `tables_to_create`
    /// names an upstream object its source can read.
    ///
    /// Refreshes each source's references before checking, so the check reads
    /// what the upstream system exposes now rather than what it exposed when
    /// the source was created.
    pub async fn validate_source_references(
        &self,
        planned_project: &graph::Project,
        tables_to_create: &BTreeSet<ObjectId>,
    ) -> Result<(), DatabaseValidationError> {
        validate_source_references_impl(self.client, planned_project, tables_to_create).await
    }

    /// Validate that all tables referenced by objects to be deployed exist in the database.
    pub async fn validate_table_dependencies(
        &self,
        planned_project: &graph::Project,
        objects_to_deploy: &BTreeSet<ObjectId>,
    ) -> Result<(), DatabaseValidationError> {
        validate_table_dependencies_impl(self.client, planned_project, objects_to_deploy).await
    }
}

/// Internal implementation of validate_schema_ownership.
pub(crate) async fn validate_schema_ownership_impl(
    client: &Client,
    schema_set: &BTreeSet<SchemaQualifier>,
) -> Result<(), DatabaseValidationError> {
    if schema_set.is_empty() {
        return Ok(());
    }

    let fqn_to_schema: BTreeMap<String, &SchemaQualifier> = schema_set
        .iter()
        .map(|sq| (format!("{}.{}", sq.database, sq.schema), sq))
        .collect();
    let fqns: Vec<String> = fqn_to_schema.keys().cloned().collect();

    let mut unowned_schemas = Vec::new();
    let mut current_user = String::new();

    for chunk in fqns.chunks(LOOKUP_BATCH_SIZE) {
        let placeholders = sql_placeholders(chunk.len());
        let query = format!(
            r#"
            SELECT d.name || '.' || s.name AS fqn, current_user() AS current_user
            FROM mz_schemas s
            JOIN mz_databases d ON s.database_id = d.id
            JOIN mz_roles r ON s.owner_id = r.id
            WHERE d.name || '.' || s.name IN ({placeholders})
              AND r.name != current_user()
            "#,
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
            if let Some(sq) = fqn_to_schema.get(&fqn) {
                unowned_schemas.push((*sq).clone());
            }
            if current_user.is_empty() {
                current_user = row.get("current_user");
            }
        }
    }

    if !unowned_schemas.is_empty() {
        unowned_schemas.sort();
        return Err(DatabaseValidationError::SchemaOwnershipMismatch {
            unowned_schemas,
            current_user,
        });
    }

    Ok(())
}

/// Internal implementation of validate_cluster_ownership.
pub(crate) async fn validate_cluster_ownership_impl(
    client: &Client,
    cluster_set: &BTreeSet<String>,
) -> Result<(), DatabaseValidationError> {
    if cluster_set.is_empty() {
        return Ok(());
    }

    let cluster_names: Vec<String> = cluster_set.iter().cloned().collect();

    let mut unowned_clusters = Vec::new();
    let mut current_user = String::new();

    for chunk in cluster_names.chunks(LOOKUP_BATCH_SIZE) {
        let placeholders = sql_placeholders(chunk.len());
        let query = format!(
            r#"
            SELECT c.name AS cluster_name, current_user() AS current_user
            FROM mz_clusters c
            JOIN mz_roles r ON c.owner_id = r.id
            WHERE c.name IN ({placeholders})
              AND r.name != current_user()
            "#,
        );

        #[allow(clippy::as_conversions)]
        let params: Vec<&(dyn ToSql + Sync)> = chunk
            .iter()
            .map(|name| name as &(dyn ToSql + Sync))
            .collect();

        let rows = client
            .query(&query, &params)
            .await
            .map_err(DatabaseValidationError::QueryError)?;

        for row in rows {
            let cluster_name: String = row.get("cluster_name");
            unowned_clusters.push(cluster_name);
            if current_user.is_empty() {
                current_user = row.get("current_user");
            }
        }
    }

    if !unowned_clusters.is_empty() {
        unowned_clusters.sort();
        return Err(DatabaseValidationError::ClusterOwnershipMismatch {
            unowned_clusters,
            current_user,
        });
    }

    Ok(())
}

/// Internal implementation of validate_cluster_isolation.
pub(crate) async fn validate_cluster_isolation_impl(
    client: &Client,
    planned_project: &graph::Project,
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
    client: &Client,
    planned_project: &graph::Project,
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
        let in_clause = sql_placeholders(priv_required_databases.len());

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
    client: &Client,
    planned_project: &graph::Project,
) -> Result<(), DatabaseValidationError> {
    let defined_sources: BTreeSet<ObjectId> = planned_project
        .iter_objects()
        .filter(|obj| matches!(obj.typed_object.stmt, Statement::CreateSource(_)))
        .map(|obj| obj.id.clone())
        .collect();

    let mut referenced_sources = BTreeSet::new();
    for obj in planned_project.iter_objects() {
        if let Statement::CreateTableFromSource(ref stmt) = obj.typed_object.stmt {
            let source_id = ObjectId::from_raw_item_name(
                &stmt.source,
                obj.id.expect_database(),
                obj.id.schema(),
            );
            if !defined_sources.contains(&source_id) {
                referenced_sources.insert(source_id);
            }
        }
    }

    let existing =
        query_existing_object_ids(client, &referenced_sources, CatalogLookup::Sources).await?;
    let missing_sources: Vec<ObjectId> =
        referenced_sources.difference(&existing).cloned().collect();
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
    client: &Client,
    planned_project: &graph::Project,
) -> Result<(), DatabaseValidationError> {
    let mut referenced_connections = BTreeSet::new();
    for obj in planned_project.iter_objects() {
        if let Statement::CreateSink(ref stmt) = obj.typed_object.stmt {
            let connection_ids = match &stmt.connection {
                CreateSinkConnection::Kafka { connection, .. } => {
                    vec![ObjectId::from_raw_item_name(
                        connection,
                        obj.id.expect_database(),
                        obj.id.schema(),
                    )]
                }
                CreateSinkConnection::Iceberg {
                    catalog_connection,
                    aws_connection,
                    ..
                } => {
                    let mut ids = vec![ObjectId::from_raw_item_name(
                        catalog_connection,
                        obj.id.expect_database(),
                        obj.id.schema(),
                    )];
                    if let Some(aws_connection) = aws_connection {
                        ids.push(ObjectId::from_raw_item_name(
                            aws_connection,
                            obj.id.expect_database(),
                            obj.id.schema(),
                        ));
                    }
                    ids
                }
            };

            for conn_id in connection_ids {
                referenced_connections.insert(conn_id);
            }
        }
    }

    let existing =
        query_existing_object_ids(client, &referenced_connections, CatalogLookup::Connections)
            .await?;
    let missing_connections: Vec<ObjectId> = referenced_connections
        .difference(&existing)
        .cloned()
        .collect();
    if !missing_connections.is_empty() {
        return Err(DatabaseValidationError::MissingConnections(
            missing_connections,
        ));
    }

    Ok(())
}

/// Internal implementation of validate_table_dependencies.
pub(crate) async fn validate_table_dependencies_impl(
    client: &Client,
    planned_project: &graph::Project,
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
    let missing_table_set: BTreeSet<ObjectId> = required_tables
        .difference(&existing_tables)
        .cloned()
        .collect();

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

/// One row of `mz_internal.mz_source_references`: an upstream object a source
/// can read.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SourceReference {
    namespace: Option<String>,
    name: String,
}

impl fmt::Display for SourceReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.namespace {
            Some(namespace) => write!(f, "{}.{}", namespace, self.name),
            None => write!(f, "{}", self.name),
        }
    }
}

/// Split a reference as written into its object name and, when the reference is
/// qualified, the namespace immediately preceding it.
///
/// A leading database qualifier (only SQL Server references carry one) is
/// dropped: `mz_source_references` records no database, so there is nothing to
/// match it against.
fn split_reference(reference: &UnresolvedItemName) -> Option<(&Ident, Option<&Ident>)> {
    let mut parts = reference.0.iter().rev();
    let name = parts.next()?;
    Some((name, parts.next()))
}

/// Whether the recorded references can settle `reference` at all.
///
/// MySQL's system schemas are the blind spot. Both `CREATE SOURCE` and
/// `ALTER SOURCE ... REFRESH REFERENCES` retrieve MySQL tables with system
/// schemas excluded, so `mz_source_references` never lists a table in `mysql`,
/// `sys`, `performance_schema`, or `information_schema`. Creating a table from
/// such a reference does resolve it, so only the server can judge one, and
/// reporting it missing here would block a deploy that works.
///
/// The namespace alone decides this, without consulting the source's connection
/// type. A Postgres or SQL Server schema that happens to be named `mysql` or
/// `sys` is skipped too, which costs nothing beyond leaving those references to
/// the server.
fn reference_is_verifiable(reference: &UnresolvedItemName) -> bool {
    let Some((_, Some(namespace))) = split_reference(reference) else {
        return true;
    };
    !mz_mysql_util::SYSTEM_SCHEMAS.contains(&namespace.as_str())
}

/// Whether `reference`, as written in a `CREATE TABLE ... FROM SOURCE`
/// statement, names one of `available`.
///
/// Mirrors the server's resolution (`SourceReferenceResolver`), except that a
/// bare object name matches in any namespace: the ambiguous case is left for
/// the server to report.
fn reference_is_available(reference: &UnresolvedItemName, available: &[SourceReference]) -> bool {
    let Some((name, namespace)) = split_reference(reference) else {
        return false;
    };

    available.iter().any(|candidate| {
        candidate.name == name.as_str()
            && match namespace {
                Some(namespace) => candidate.namespace.as_deref() == Some(namespace.as_str()),
                None => true,
            }
    })
}

/// Everything one source records about what it can read.
#[derive(Debug)]
struct SourceReferences {
    /// The source's catalog ID, which names it in an error's suggested query.
    id: String,
    references: Vec<SourceReference>,
}

/// Exposed references spelled closely enough to `reference` to be the one the
/// project meant, best first. Empty when nothing comes close.
///
/// A candidate whose object name is exactly right and whose namespace is not
/// leads, no matter how unalike the two namespaces are. Naming the right object
/// in the wrong schema is both a common slip and one edit distance scores as
/// unrelated. The rest are ranked on the object name alone. Scoring the
/// reference whole would let a shared namespace pad the distance budget without
/// saying anything about whether the names match: `public.widgets` and
/// `public.orders` sit 4 edits apart, inside the budget a name that long earns,
/// and are nothing alike.
///
/// Suggestions carry the namespace even where the project wrote a bare
/// reference. That stays a valid substitution and says where the object lives.
fn suggest_references(
    reference: &UnresolvedItemName,
    available: &[SourceReference],
) -> Vec<String> {
    let Some((name, _)) = split_reference(reference) else {
        return Vec::new();
    };

    let (exact, rest): (Vec<&SourceReference>, Vec<&SourceReference>) = available
        .iter()
        .partition(|candidate| candidate.name == name.as_str());
    let mut suggestions: Vec<String> = exact.iter().map(|c| c.to_string()).collect();

    let mut names: Vec<&str> = rest.iter().map(|c| c.name.as_str()).collect();
    names.sort();
    names.dedup();
    for near in did_you_mean(name.as_str(), &names) {
        // One name can sit in several namespaces, and which one the project
        // meant is exactly what it got wrong, so offer each.
        suggestions.extend(
            rest.iter()
                .filter(|candidate| candidate.name == near)
                .map(|candidate| candidate.to_string()),
        );
    }

    suggestions.truncate(MAX_DID_YOU_MEAN);
    suggestions
}

/// Query the references recorded for each of `sources`, keyed by the source's
/// fully qualified name.
async fn query_source_references(
    client: &Client,
    sources: &BTreeSet<ObjectId>,
) -> Result<BTreeMap<ObjectId, SourceReferences>, DatabaseValidationError> {
    let mut by_source: BTreeMap<ObjectId, SourceReferences> = BTreeMap::new();
    if sources.is_empty() {
        return Ok(by_source);
    }

    let fqn_to_source: BTreeMap<String, &ObjectId> = sources
        .iter()
        .map(|source| (source.to_string(), source))
        .collect();
    let fqns: Vec<String> = fqn_to_source.keys().cloned().collect();

    for chunk in fqns.chunks(LOOKUP_BATCH_SIZE) {
        let placeholders = sql_placeholders(chunk.len());
        let query = format!(
            r#"
            SELECT d.name || '.' || sc.name || '.' || s.name AS source,
                   s.id AS source_id,
                   refs.namespace,
                   refs.name
            FROM mz_internal.mz_source_references refs
            JOIN mz_catalog.mz_sources s ON refs.source_id = s.id
            JOIN mz_catalog.mz_schemas sc ON s.schema_id = sc.id
            JOIN mz_catalog.mz_databases d ON sc.database_id = d.id
            WHERE d.name || '.' || sc.name || '.' || s.name IN ({placeholders})
            "#,
        );

        #[allow(clippy::as_conversions)]
        let params: Vec<&(dyn ToSql + Sync)> =
            chunk.iter().map(|fqn| fqn as &(dyn ToSql + Sync)).collect();

        let rows = client
            .query(&query, &params)
            .await
            .map_err(DatabaseValidationError::QueryError)?;

        for row in rows {
            let fqn: String = row.get("source");
            let Some(source) = fqn_to_source.get(&fqn) else {
                continue;
            };
            by_source
                .entry((*source).clone())
                .or_insert_with(|| SourceReferences {
                    id: row.get("source_id"),
                    references: Vec::new(),
                })
                .references
                .push(SourceReference {
                    namespace: row.get("namespace"),
                    name: row.get("name"),
                });
        }
    }

    for source in by_source.values_mut() {
        source.references.sort();
    }

    Ok(by_source)
}

/// Internal implementation of validate_source_references.
pub(crate) async fn validate_source_references_impl(
    client: &Client,
    planned_project: &graph::Project,
    tables_to_create: &BTreeSet<ObjectId>,
) -> Result<(), DatabaseValidationError> {
    let mut requested: BTreeMap<ObjectId, Vec<(ObjectId, UnresolvedItemName)>> = BTreeMap::new();
    for table_id in tables_to_create {
        let Some(obj) = planned_project.find_object(table_id) else {
            continue;
        };
        let Statement::CreateTableFromSource(ref stmt) = obj.typed_object.stmt else {
            continue;
        };
        // Without a REFERENCE clause the table reads the source's single
        // output, so there is no name to check.
        let Some(reference) = &stmt.external_reference else {
            continue;
        };
        if !reference_is_verifiable(reference) {
            continue;
        }
        let source_id = ObjectId::from_raw_item_name(
            &stmt.source,
            table_id.expect_database(),
            table_id.schema(),
        );
        requested
            .entry(source_id)
            .or_default()
            .push((table_id.clone(), reference.clone()));
    }
    if requested.is_empty() {
        return Ok(());
    }

    // A source the project creates in this same run does not exist yet: apply
    // plans every phase before executing any of it. Nothing can be checked
    // against a source that isn't there, and its references will be recorded
    // when it is created.
    let sources: BTreeSet<ObjectId> = requested.keys().cloned().collect();
    let existing_sources =
        query_existing_object_ids(client, &sources, CatalogLookup::Sources).await?;
    requested.retain(|source, _| existing_sources.contains(source));
    if requested.is_empty() {
        return Ok(());
    }

    // The recorded references are a snapshot from when the source was created,
    // while creating the table resolves its reference against the upstream
    // system as it is now. Refresh first so a miss here is a real miss.
    let mut unreadable: BTreeMap<ObjectId, String> = BTreeMap::new();
    for source in requested.keys() {
        let sql = format!(
            "ALTER SOURCE {} REFRESH REFERENCES",
            source.to_unresolved_item_name()
        );
        verbose!("{}", sql);
        if let Err(e) = client.execute(&sql, &[]).await {
            // The role may not own the source, or the source may not support
            // the statement. Fall back to the recorded references and say so
            // if a table then fails to match.
            unreadable.insert(source.clone(), e.to_string());
        }
    }

    let available = query_source_references(client, &existing_sources).await?;

    let mut mismatches = Vec::new();
    for (source, tables) in requested {
        let Some(recorded) = available.get(&source) else {
            // A source with no recorded references tells us nothing: an empty
            // record is not the same as an empty upstream system, and failing
            // here would reject tables that apply fine.
            continue;
        };
        let missing: Vec<MissingSourceReference> = tables
            .into_iter()
            .filter(|(_, reference)| !reference_is_available(reference, &recorded.references))
            .map(|(table, reference)| MissingSourceReference {
                table,
                suggestions: suggest_references(&reference, &recorded.references),
                reference: reference.to_string(),
            })
            .collect();
        if missing.is_empty() {
            continue;
        }
        mismatches.push(SourceReferenceMismatch {
            unreadable: unreadable.get(&source).cloned(),
            source_id: recorded.id.clone(),
            available_count: recorded.references.len(),
            source,
            tables: missing,
        });
    }

    if !mismatches.is_empty() {
        return Err(DatabaseValidationError::MissingSourceReferences(mismatches));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(parts: &[&str]) -> UnresolvedItemName {
        UnresolvedItemName(parts.iter().map(|p| Ident::new_unchecked(*p)).collect())
    }

    fn available(namespace: Option<&str>, name: &str) -> SourceReference {
        SourceReference {
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
        }
    }

    #[mz_ore::test]
    fn test_reference_is_available() {
        let refs = vec![
            available(Some("public"), "users"),
            available(Some("sales"), "orders"),
        ];

        assert!(reference_is_available(
            &reference(&["public", "users"]),
            &refs
        ));
        // A bare name resolves in whichever namespace holds it.
        assert!(reference_is_available(&reference(&["orders"]), &refs));
        // A leading database qualifier has nothing to match against.
        assert!(reference_is_available(
            &reference(&["upstream", "sales", "orders"]),
            &refs
        ));

        assert!(!reference_is_available(
            &reference(&["sales", "users"]),
            &refs
        ));
        assert!(!reference_is_available(&reference(&["widgets"]), &refs));
        assert!(!reference_is_available(
            &reference(&["public", "users"]),
            &[]
        ));
    }

    #[mz_ore::test]
    fn test_reference_is_verifiable() {
        assert!(reference_is_verifiable(&reference(&["public", "users"])));
        assert!(reference_is_verifiable(&reference(&["users"])));

        // MySQL system schemas are never recorded, so nothing here can be
        // judged against the recorded set.
        assert!(!reference_is_verifiable(&reference(&["mysql", "users"])));
        assert!(!reference_is_verifiable(&reference(&["sys", "users"])));
        assert!(!reference_is_verifiable(&reference(&[
            "performance_schema",
            "users"
        ])));
        assert!(!reference_is_verifiable(&reference(&[
            "information_schema",
            "tables"
        ])));
        // The namespace is the part before the object name, whatever precedes it.
        assert!(!reference_is_verifiable(&reference(&[
            "upstream", "mysql", "users"
        ])));
    }

    #[mz_ore::test]
    fn test_suggest_references_catches_a_typo() {
        let refs = vec![
            available(Some("public"), "widgets"),
            available(Some("public"), "orders"),
        ];

        assert_eq!(
            suggest_references(&reference(&["public", "widgest"]), &refs),
            vec!["public.widgets".to_string()]
        );
        // A bare reference is answered with the namespace the object lives in.
        assert_eq!(
            suggest_references(&reference(&["widgest"]), &refs),
            vec!["public.widgets".to_string()]
        );
    }

    #[mz_ore::test]
    fn test_suggest_references_leads_with_the_right_name_in_another_namespace() {
        let refs = vec![
            available(Some("public"), "widgets"),
            available(Some("staging"), "widgets"),
            available(Some("sales"), "widgetry"),
        ];

        // Edit distance alone would rank sales.widgetry, in the very namespace
        // asked for and two characters off, ahead of the two exact name matches.
        assert_eq!(
            suggest_references(&reference(&["sales", "widgets"]), &refs),
            vec![
                "public.widgets".to_string(),
                "staging.widgets".to_string(),
                "sales.widgetry".to_string()
            ]
        );
    }

    #[mz_ore::test]
    fn test_suggest_references_ignores_the_namespace_when_scoring() {
        // Scoring whole references would put public.orders within 4 edits of
        // public.widgets, inside the budget a name that long earns, purely
        // because they share a namespace.
        let refs = vec![
            available(Some("public"), "orders"),
            available(Some("public"), "users"),
            available(Some("public"), "products"),
        ];

        assert!(suggest_references(&reference(&["public", "widgets"]), &refs).is_empty());
    }

    #[mz_ore::test]
    fn test_suggest_references_stays_quiet_when_nothing_is_close() {
        let refs = vec![
            available(Some("public"), "users"),
            available(Some("public"), "orders"),
        ];

        assert!(
            suggest_references(&reference(&["public", "shipping_manifests"]), &refs).is_empty()
        );
        assert!(suggest_references(&reference(&["public", "users"]), &[]).is_empty());
    }

    #[mz_ore::test]
    fn test_suggest_references_is_capped() {
        let refs: Vec<SourceReference> = (0..10)
            .map(|i| available(Some(&format!("s{i}")), "widgets"))
            .collect();

        assert_eq!(
            suggest_references(&reference(&["public", "widgets"]), &refs).len(),
            MAX_DID_YOU_MEAN
        );
    }

    #[mz_ore::test]
    fn test_suggest_references_offers_every_namespace_holding_the_name() {
        // Which namespace holds the object is the part the project got wrong,
        // so a misspelling that resolves to one name in two schemas offers both.
        let refs = vec![
            available(Some("public"), "widgets"),
            available(Some("staging"), "widgets"),
        ];

        assert_eq!(
            suggest_references(&reference(&["widgest"]), &refs),
            vec!["public.widgets".to_string(), "staging.widgets".to_string()]
        );
    }

    #[mz_ore::test]
    fn test_reference_is_available_without_namespace() {
        // Kafka topics and other unnamespaced references record a null namespace.
        let refs = vec![available(None, "events")];

        assert!(reference_is_available(&reference(&["events"]), &refs));
        assert!(!reference_is_available(
            &reference(&["public", "events"]),
            &refs
        ));
    }
}
