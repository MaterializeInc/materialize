// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dependency extraction and graph assembly from compiled project state.
//!
//! This module walks the SQL AST of every object to build a project-wide
//! dependency graph and determine schema types. Query-level traversal is
//! delegated to mz-sql-parser's auto-generated [`Visit`] trait — a
//! [`DependencyVisitor`] overrides `visit_query` (for CTE scope management)
//! and `visit_table_factor` (to collect table references as dependencies).
//! All other AST nodes (expressions, set operations, etc.) are handled by
//! the default traversal.
//!
//! # CTE Scoping
//!
//! CTE references must be excluded from the dependency set because they
//! are query-local names, not database objects. The visitor uses
//! [`CteScope`] to track CTE names
//! across nested queries. All CTE names in a `WITH` block are pushed at
//! once — this is correct for both simple and mutually recursive CTEs
//! because self-references in simple CTEs are SQL errors that Materialize
//! rejects.
//!
//! # Statement-Level Dispatch
//!
//! The top-level [`extract_dependencies`] function matches on statement type
//! and calls `visitor.visit_query()` only on the relevant subtree (e.g., the
//! query body of a CREATE VIEW, not the view name itself). Non-query
//! dependencies (source connections, connection options) are extracted by
//! dedicated helper functions.
//!
//! # Dependency Validation
//!
//! After graph assembly, `external_dependencies` (the set of references to
//! objects not defined in the project) is validated against the dependencies
//! declared in `project.toml` via [`validate_dependencies()`]. This cross-check
//! produces two sets: undeclared external references (hard error) and declared
//! dependencies that are never referenced (warning).

use super::super::ast::{Cluster, Statement};
use crate::project::ir::object_id::ObjectId;
use crate::project::ir::{
    compiled,
    graph::{Database, DatabaseObject, Project, Schema, SchemaType},
    unit_test::UnitTest,
};
use crate::project::resolve::cte_scope::CteScope;
use mz_sql_parser::ast::visit::{self, Visit};
use mz_sql_parser::ast::*;
use rayon::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

/// Determine the schema type based on the objects it contains.
///
/// Returns:
/// - `SchemaType::Storage` if the schema contains tables, sinks, or tables from sources
/// - `SchemaType::Compute` if the schema contains views or materialized views
/// - `SchemaType::Empty` if the schema contains no objects
///
/// Note: Due to compiled-project validation, schemas cannot contain both storage
/// and compute objects, so we only need to check the first object.
fn determine_schema_type(objects: &[DatabaseObject]) -> SchemaType {
    if objects.is_empty() {
        return SchemaType::Empty;
    }

    // Check the first object to determine schema type
    // Validation ensures all objects in a schema are the same type
    match &objects[0].typed_object.stmt {
        Statement::CreateTable(_)
        | Statement::CreateTableFromSource(_)
        | Statement::CreateSource(_)
        | Statement::CreateSink(_)
        | Statement::CreateSecret(_)
        | Statement::CreateConnection(_) => SchemaType::Storage,
        Statement::CreateView(_) | Statement::CreateMaterializedView(_) => SchemaType::Compute,
    }
}

/// A flattened compiled object with its database/schema context,
/// used as the unit of work for dependency extraction.
struct TypedObjectTask {
    db_name: String,
    schema_name: String,
    typed_obj: compiled::DatabaseObject,
}

/// The result of processing a single compiled object through dependency extraction.
struct ProcessedObject {
    db_name: String,
    schema_name: String,
    object_id: ObjectId,
    typed_object: compiled::DatabaseObject,
    dependencies: BTreeSet<ObjectId>,
    clusters: BTreeSet<Cluster>,
    tests: Vec<(ObjectId, UnitTest)>,
}

impl From<compiled::Project> for Project {
    /// Converts a compiled project into a dependency-aware project graph.
    ///
    /// Graph assembly is structured as collect → process → reassemble:
    ///
    /// 1. **Collect** — Flatten all compiled objects into `TypedObjectTask`s and
    ///    collect defined object IDs for external dependency detection.
    ///
    /// 2. **Process** — Extract dependencies and clusters from each object.
    ///    This is the CPU-intensive step.
    ///
    /// 3. **Reassemble** — Merge results into the dependency graph and
    ///    hierarchical `Project` structure.
    fn from(compiled_project: compiled::Project) -> Self {
        // Flatten all compiled objects and collect defined object IDs.
        let mut object_tasks = Vec::new();
        let mut defined_objects = BTreeSet::new();

        // Track database/schema metadata for reassembly
        struct DbMeta {
            name: String,
            mod_statements: Option<Vec<mz_sql_parser::ast::Statement<Raw>>>,
            schema_metas: Vec<SchemaMeta>,
        }
        struct SchemaMeta {
            name: String,
            mod_statements: Option<Vec<mz_sql_parser::ast::Statement<Raw>>>,
        }
        let mut db_metas: Vec<DbMeta> = Vec::new();

        for typed_db in &compiled_project.databases {
            for typed_schema in &typed_db.schemas {
                for typed_obj in &typed_schema.objects {
                    let object_id = ObjectId::new(
                        typed_db.name.clone(),
                        typed_schema.name.clone(),
                        typed_obj.stmt.ident().object.clone(),
                    );
                    defined_objects.insert(object_id);
                }
            }
        }

        for typed_db in compiled_project.databases {
            let mut schema_metas = Vec::new();

            for typed_schema in typed_db.schemas {
                schema_metas.push(SchemaMeta {
                    name: typed_schema.name.clone(),
                    mod_statements: typed_schema.mod_statements,
                });

                for typed_obj in typed_schema.objects {
                    object_tasks.push(TypedObjectTask {
                        db_name: typed_db.name.clone(),
                        schema_name: typed_schema.name.clone(),
                        typed_obj,
                    });
                }
            }

            db_metas.push(DbMeta {
                name: typed_db.name,
                mod_statements: typed_db.mod_statements,
                schema_metas,
            });
        }

        // Extract dependencies and clusters from each object.
        let processed: Vec<ProcessedObject> = object_tasks
            .into_par_iter()
            .map(|task| {
                let object_id = ObjectId::new(
                    task.db_name.clone(),
                    task.schema_name.clone(),
                    task.typed_obj.stmt.ident().object.clone(),
                );

                let (dependencies, clusters) =
                    extract_dependencies(&task.typed_obj.stmt, &task.db_name, &task.schema_name);

                // Collect tests
                let tests: Vec<_> = task
                    .typed_obj
                    .tests
                    .iter()
                    .map(|test_stmt| {
                        let unit_test = UnitTest::from_execute_statement(test_stmt);
                        (object_id.clone(), unit_test)
                    })
                    .collect();

                ProcessedObject {
                    db_name: task.db_name,
                    schema_name: task.schema_name,
                    object_id,
                    typed_object: task.typed_obj,
                    dependencies,
                    clusters,
                    tests,
                }
            })
            .collect();

        // Merge results into dependency graph and hierarchical structure.
        let mut dependency_graph = BTreeMap::new();
        let mut external_dependencies = BTreeSet::new();
        let mut cluster_dependencies = BTreeSet::new();
        let mut tests = Vec::new();

        // Group graph objects by (db_name, schema_name)
        let mut objects_by_location: BTreeMap<(String, String), Vec<DatabaseObject>> =
            BTreeMap::new();

        for po in processed {
            // Merge clusters
            for cluster in po.clusters {
                cluster_dependencies.insert(cluster);
            }

            // Check for external dependencies
            for dep in &po.dependencies {
                if !defined_objects.contains(dep) {
                    external_dependencies.insert(dep.clone());
                }
            }

            dependency_graph.insert(po.object_id.clone(), po.dependencies.clone());
            tests.extend(po.tests);

            objects_by_location
                .entry((po.db_name.clone(), po.schema_name.clone()))
                .or_default()
                .push(DatabaseObject {
                    id: po.object_id,
                    typed_object: po.typed_object,
                    dependencies: po.dependencies,
                });
        }

        // Reassemble into hierarchical structure
        let mut databases = Vec::new();

        for meta in db_metas {
            let mut schemas = Vec::new();

            for schema_meta in meta.schema_metas {
                let objects = objects_by_location
                    .remove(&(meta.name.clone(), schema_meta.name.clone()))
                    .unwrap_or_default();

                let schema_type = determine_schema_type(&objects);

                schemas.push(Schema {
                    name: schema_meta.name,
                    objects,
                    mod_statements: schema_meta.mod_statements,
                    schema_type,
                });
            }

            databases.push(Database {
                name: meta.name,
                schemas,
                mod_statements: meta.mod_statements,
            });
        }

        Project {
            databases,
            dependency_graph,
            external_dependencies,
            cluster_dependencies,
            tests,
            replacement_schemas: compiled_project.replacement_schemas,
            compile_dirty: BTreeSet::new(),
        }
    }
}

/// Find all external indexes on an object. That is,
/// any index that lives on a different cluster than the one where
/// the main object is installed.
pub(crate) fn extract_external_indexes(
    object: &DatabaseObject,
) -> Vec<(Cluster, CreateIndexStatement<Raw>)> {
    match &object.typed_object.stmt {
        Statement::CreateMaterializedView(materialized_view) => {
            let mv_cluster =
                Cluster::new(materialized_view.in_cluster.clone().unwrap().to_string());

            object
                .typed_object
                .indexes
                .iter()
                .filter_map(|index| {
                    let index_cluster = Cluster::new(index.in_cluster.clone().unwrap().to_string());

                    (mv_cluster != index_cluster).then(|| (index_cluster, index.clone()))
                })
                .collect()
        }
        _ => object
            .typed_object
            .indexes
            .iter()
            .map(|index| {
                let cluster = Cluster::new(index.in_cluster.clone().unwrap().to_string());
                (cluster, index.clone())
            })
            .collect(),
    }
}

/// Visitor that collects table reference dependencies from query ASTs.
///
/// Overrides `visit_query` for CTE scope management and `visit_table_factor`
/// to collect table references. All other traversal (expressions, set
/// operations, subqueries) is handled by mz-sql-parser's auto-generated
/// default implementations.
struct DependencyVisitor<'a> {
    default_database: &'a str,
    default_schema: &'a str,
    deps: BTreeSet<ObjectId>,
    cte_scope: CteScope,
}

impl<'a> DependencyVisitor<'a> {
    fn new(default_database: &'a str, default_schema: &'a str) -> Self {
        Self {
            default_database,
            default_schema,
            deps: BTreeSet::new(),
            cte_scope: CteScope::new(),
        }
    }
}

impl<'ast> Visit<'ast, Raw> for DependencyVisitor<'_> {
    fn visit_query(&mut self, node: &'ast Query<Raw>) {
        let names = CteScope::collect_cte_names(&node.ctes);
        self.cte_scope.push(names);
        visit::visit_query(self, node);
        self.cte_scope.pop();
    }

    fn visit_table_factor(&mut self, node: &'ast TableFactor<Raw>) {
        match node {
            TableFactor::Table { name, .. } => {
                let unresolved = name.name();
                if unresolved.0.len() == 1 && self.cte_scope.is_cte(&unresolved.0[0].to_string()) {
                    return;
                }
                self.deps.insert(ObjectId::from_raw_item_name(
                    name,
                    self.default_database,
                    self.default_schema,
                ));
                // Don't call default — it would visit_item_name which we don't need
            }
            _ => visit::visit_table_factor(self, node),
        }
    }
}

/// Extract all dependencies from a statement.
///
/// Returns a tuple of (object_dependencies, cluster_dependencies).
///
/// This function is public to allow the changeset module to analyze
/// cluster dependencies for incremental deployment.
pub(crate) fn extract_dependencies(
    stmt: &Statement,
    default_database: &str,
    default_schema: &str,
) -> (BTreeSet<ObjectId>, BTreeSet<Cluster>) {
    let mut visitor = DependencyVisitor::new(default_database, default_schema);
    let mut clusters = BTreeSet::new();

    match stmt {
        Statement::CreateView(s) => {
            visitor.visit_query(&s.definition.query);
        }
        Statement::CreateMaterializedView(s) => {
            visitor.visit_query(&s.query);

            // Extract cluster dependency from IN CLUSTER clause
            if let Some(ref cluster_name) = s.in_cluster {
                clusters.insert(Cluster::new(cluster_name.to_string()));
            }
        }
        Statement::CreateTableFromSource(s) => {
            // Table depends on the source it's created from
            let source_id =
                ObjectId::from_raw_item_name(&s.source, default_database, default_schema);
            visitor.deps.insert(source_id);
        }
        Statement::CreateSink(s) => {
            // Sink depends on the shard it reads from
            let from_id = ObjectId::from_raw_item_name(&s.from, default_database, default_schema);
            visitor.deps.insert(from_id);

            if let Some(ref cluster_name) = s.in_cluster {
                clusters.insert(Cluster::new(cluster_name.to_string()));
            }
        }
        Statement::CreateSource(s) => {
            // Source depends on its connection
            extract_source_connection_dep(
                &s.connection,
                default_database,
                default_schema,
                &mut visitor.deps,
            );

            if let Some(ref cluster_name) = s.in_cluster {
                clusters.insert(Cluster::new(cluster_name.to_string()));
            }
        }
        Statement::CreateConnection(s) => {
            extract_connection_option_deps(
                &s.values,
                default_database,
                default_schema,
                &mut visitor.deps,
            );
        }
        // These don't have dependencies on other database objects
        Statement::CreateTable(_) | Statement::CreateSecret(_) => {}
    }

    (visitor.deps, clusters)
}

/// Extract the connection dependency from a source's connection clause.
fn extract_source_connection_dep(
    connection: &CreateSourceConnection<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut BTreeSet<ObjectId>,
) {
    match connection {
        CreateSourceConnection::Kafka { connection, .. }
        | CreateSourceConnection::Postgres { connection, .. }
        | CreateSourceConnection::SqlServer { connection, .. }
        | CreateSourceConnection::MySql { connection, .. } => {
            deps.insert(ObjectId::from_raw_item_name(
                connection,
                default_database,
                default_schema,
            ));
        }
        CreateSourceConnection::LoadGenerator { .. } => {}
    }
}

/// Extract dependencies from connection options (secrets, other connections).
fn extract_connection_option_deps(
    options: &[ConnectionOption<Raw>],
    default_database: &str,
    default_schema: &str,
    deps: &mut BTreeSet<ObjectId>,
) {
    for option in options {
        if let Some(ref value) = option.value {
            extract_with_option_value_deps(value, default_database, default_schema, deps);
        }
    }
}

/// Result of validating declared dependencies against discovered external references.
pub(crate) struct DependencyValidation {
    /// External references found in SQL that are not declared in project.toml.
    pub undeclared: BTreeSet<ObjectId>,
    /// Dependencies declared in project.toml that no SQL object references.
    pub unused: BTreeSet<ObjectId>,
}

/// Cross-reference declared dependencies (from project.toml) against discovered
/// external references (from the compiled dependency graph).
///
/// Returns the set difference in both directions:
/// - `undeclared`: discovered but not declared (should be a hard error)
/// - `unused`: declared but not discovered (should be a warning)
pub(crate) fn validate_dependencies(
    declared: &BTreeSet<ObjectId>,
    discovered: &BTreeSet<ObjectId>,
) -> DependencyValidation {
    DependencyValidation {
        undeclared: discovered.difference(declared).cloned().collect(),
        unused: declared.difference(discovered).cloned().collect(),
    }
}

/// Extract dependencies from a single WithOptionValue, recursing into nested structures.
fn extract_with_option_value_deps(
    value: &WithOptionValue<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut BTreeSet<ObjectId>,
) {
    match value {
        WithOptionValue::Secret(name) | WithOptionValue::Item(name) => {
            deps.insert(ObjectId::from_raw_item_name(
                name,
                default_database,
                default_schema,
            ));
        }
        WithOptionValue::ConnectionAwsPrivatelink(pl) => {
            deps.insert(ObjectId::from_raw_item_name(
                &pl.connection,
                default_database,
                default_schema,
            ));
        }
        WithOptionValue::ConnectionKafkaBroker(broker) => match &broker.tunnel {
            KafkaBrokerTunnel::SshTunnel(name) => {
                deps.insert(ObjectId::from_raw_item_name(
                    name,
                    default_database,
                    default_schema,
                ));
            }
            KafkaBrokerTunnel::AwsPrivatelink(aws) => {
                deps.insert(ObjectId::from_raw_item_name(
                    &aws.connection,
                    default_database,
                    default_schema,
                ));
            }
            KafkaBrokerTunnel::Direct => {}
        },
        WithOptionValue::Sequence(items) => {
            for item in items {
                extract_with_option_value_deps(item, default_database, default_schema, deps);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod dependency_validation_tests {
    use super::*;
    use crate::project::ir::object_id::ObjectId;
    use std::collections::BTreeSet;

    fn oid(db: &str, schema: &str, obj: &str) -> ObjectId {
        ObjectId::new(db.to_string(), schema.to_string(), obj.to_string())
    }

    #[mz_ore::test]
    fn test_all_externals_declared() {
        let declared = BTreeSet::from([
            oid("ontology", "public", "customers"),
            oid("ontology", "public", "orders"),
        ]);
        let discovered = BTreeSet::from([
            oid("ontology", "public", "customers"),
            oid("ontology", "public", "orders"),
        ]);
        let result = validate_dependencies(&declared, &discovered);
        assert!(result.undeclared.is_empty());
        assert!(result.unused.is_empty());
    }

    #[mz_ore::test]
    fn test_undeclared_external_detected() {
        let declared = BTreeSet::from([oid("ontology", "public", "customers")]);
        let discovered = BTreeSet::from([
            oid("ontology", "public", "customers"),
            oid("ontology", "public", "orders"),
        ]);
        let result = validate_dependencies(&declared, &discovered);
        assert_eq!(result.undeclared.len(), 1);
        assert!(
            result
                .undeclared
                .contains(&oid("ontology", "public", "orders"))
        );
        assert!(result.unused.is_empty());
    }

    #[mz_ore::test]
    fn test_unused_declared_detected() {
        let declared = BTreeSet::from([
            oid("ontology", "public", "customers"),
            oid("analytics", "public", "page_views"),
        ]);
        let discovered = BTreeSet::from([oid("ontology", "public", "customers")]);
        let result = validate_dependencies(&declared, &discovered);
        assert!(result.undeclared.is_empty());
        assert_eq!(result.unused.len(), 1);
        assert!(
            result
                .unused
                .contains(&oid("analytics", "public", "page_views"))
        );
    }

    #[mz_ore::test]
    fn test_both_empty() {
        let declared = BTreeSet::new();
        let discovered = BTreeSet::new();
        let result = validate_dependencies(&declared, &discovered);
        assert!(result.undeclared.is_empty());
        assert!(result.unused.is_empty());
    }

    #[mz_ore::test]
    fn test_both_undeclared_and_unused() {
        let declared = BTreeSet::from([oid("a", "b", "unused")]);
        let discovered = BTreeSet::from([oid("x", "y", "undeclared")]);
        let result = validate_dependencies(&declared, &discovered);
        assert_eq!(result.undeclared.len(), 1);
        assert_eq!(result.unused.len(), 1);
    }
}
