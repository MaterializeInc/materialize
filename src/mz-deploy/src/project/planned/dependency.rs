//! Dependency extraction and project conversion from typed representation.
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
//! [`CteScope`](crate::project::cte_scope::CteScope) to track CTE names
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

use super::super::ast::{Cluster, Statement};
use super::super::constraint;
use super::super::cte_scope::CteScope;
use super::super::typed;
use super::types::{Database, DatabaseObject, Project, Schema, SchemaType};
use crate::project::object_id::ObjectId;
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
/// Note: Due to validation in the typed phase, schemas cannot contain both storage
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

/// A flattened typed object with its database/schema context,
/// used as the unit of work for the dependency extraction pipeline.
struct TypedObjectTask {
    db_name: String,
    schema_name: String,
    typed_obj: typed::DatabaseObject,
}

/// The result of processing a single typed object through dependency extraction.
struct ProcessedObject {
    db_name: String,
    schema_name: String,
    object_id: ObjectId,
    typed_object: typed::DatabaseObject,
    dependencies: BTreeSet<ObjectId>,
    clusters: BTreeSet<Cluster>,
    constraint_mvs: Vec<(
        ObjectId,
        CreateConstraintStatement<Raw>,
        typed::DatabaseObject,
    )>,
    tests: Vec<(ObjectId, crate::unit_test::UnitTest)>,
}

impl From<typed::Project> for Project {
    /// Converts a typed project into a planned project with dependency information.
    ///
    /// The conversion pipeline is structured as collect → process → reassemble:
    ///
    /// 1. **Collect** — Flatten all typed objects into `TypedObjectTask`s and
    ///    collect defined object IDs for external dependency detection.
    ///
    /// 2. **Process** — Extract dependencies, clusters, and constraint MVs from
    ///    each object. This is the CPU-intensive step.
    ///
    /// 3. **Reassemble** — Merge results into the dependency graph and
    ///    hierarchical `Project` structure.
    fn from(typed_project: typed::Project) -> Self {
        // ── Step 1: Collect ─────────────────────────────────────────────
        // Flatten all typed objects and collect defined object IDs.
        let collect_start = std::time::Instant::now();

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

        for typed_db in &typed_project.databases {
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

        for typed_db in typed_project.databases {
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

        crate::timing!("  planned: collect", collect_start.elapsed());

        // ── Step 2: Process ─────────────────────────────────────────────
        // Extract dependencies and clusters from each object.
        let process_start = std::time::Instant::now();

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

                // Constraint lowering: enforced constraints become companion MVs
                let mut constraint_mvs = Vec::new();
                for c in &task.typed_obj.constraints {
                    if let Some(mv_obj) = constraint::lower_to_materialized_view(
                        c,
                        &task.typed_obj.stmt.ident().object,
                        &task.db_name,
                        &task.schema_name,
                    ) {
                        constraint_mvs.push((object_id.clone(), c.clone(), mv_obj));
                    }
                }

                // Collect tests
                let tests: Vec<_> = task
                    .typed_obj
                    .tests
                    .iter()
                    .map(|test_stmt| {
                        let unit_test =
                            crate::unit_test::UnitTest::from_execute_statement(test_stmt);
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
                    constraint_mvs,
                    tests,
                }
            })
            .collect();

        crate::timing!("  planned: process", process_start.elapsed());

        // ── Step 3: Reassemble ──────────────────────────────────────────
        // Merge results into dependency graph and hierarchical structure.
        let reassemble_start = std::time::Instant::now();

        let mut dependency_graph = BTreeMap::new();
        let mut external_dependencies = BTreeSet::new();
        let mut cluster_dependencies = BTreeSet::new();
        let mut tests = Vec::new();

        // Group planned objects by (db_name, schema_name)
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
                    is_constraint_mv: false,
                });

            // Process lowered constraint MVs
            for (parent_id, constraint_stmt, mv_obj) in po.constraint_mvs {
                let mv_id = ObjectId::new(
                    po.db_name.clone(),
                    po.schema_name.clone(),
                    mv_obj.stmt.ident().object.clone(),
                );

                let (mut mv_deps, mv_clusters) =
                    extract_dependencies(&mv_obj.stmt, &po.db_name, &po.schema_name);

                for cluster in mv_clusters {
                    cluster_dependencies.insert(cluster);
                }

                mv_deps.insert(parent_id);

                if let Some(ref refs) = constraint_stmt.references {
                    let ref_id =
                        ObjectId::from_raw_item_name(&refs.object, &po.db_name, &po.schema_name);
                    mv_deps.insert(ref_id);
                }

                for dep in &mv_deps {
                    if !defined_objects.contains(dep) {
                        external_dependencies.insert(dep.clone());
                    }
                }

                defined_objects.insert(mv_id.clone());
                dependency_graph.insert(mv_id.clone(), mv_deps.clone());

                objects_by_location
                    .entry((po.db_name.clone(), po.schema_name.clone()))
                    .or_default()
                    .push(DatabaseObject {
                        id: mv_id,
                        typed_object: mv_obj,
                        dependencies: mv_deps,
                        is_constraint_mv: true,
                    });
            }
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

        crate::timing!("  planned: reassemble", reassemble_start.elapsed());

        Project {
            databases,
            dependency_graph,
            external_dependencies,
            cluster_dependencies,
            tests,
            replacement_schemas: typed_project.replacement_schemas,
        }
    }
}

/// Find all external indexes on an object. That is,
/// any index that lives on a different cluster than the one where
/// the main object is installed.
pub fn extract_external_indexes(
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
pub fn extract_dependencies(
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
