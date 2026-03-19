//! Dependency extraction and project conversion from typed representation.
//!
//! This module walks the SQL AST of every object to build a project-wide
//! dependency graph and determine schema types. The core algorithm is a
//! recursive descent through the AST, extracting table references and
//! cluster usages.
//!
//! # CTE Scoping
//!
//! CTE references must be excluded from the dependency set because they
//! are query-local names, not database objects. Two scoping modes exist:
//!
//! - **Simple CTEs** (`WITH a AS (...), b AS (...)`) — each CTE can see
//!   parent CTEs and earlier siblings, but not itself or later siblings.
//!   Scope is built incrementally.
//! - **Mutually recursive CTEs** (`WITH MUTUALLY RECURSIVE ...`) — all CTEs
//!   in the block share a single scope and can reference each other freely.
//!
//! Functions with a `_with_ctes` suffix carry an explicit CTE name set
//! so that unqualified single-identifier references can be checked against
//! the current scope before being treated as external dependencies.
//!
//! # Limitations
//!
//! - `TableFactor::Function` (table functions) and `TableFactor::RowsFrom`
//!   are not tracked — they may reference tables indirectly but extracting
//!   those dependencies would require function-signature analysis.

use super::super::ast::{Cluster, Statement};
use super::super::constraint;
use super::super::typed;
use super::types::{Database, DatabaseObject, Project, Schema, SchemaType};
use crate::project::object_id::ObjectId;
use mz_sql_parser::ast::*;
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
    constraint_mvs: Vec<(ObjectId, CreateConstraintStatement<Raw>, typed::DatabaseObject)>,
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

        // ── Step 2: Process ─────────────────────────────────────────────
        // Extract dependencies and clusters from each object.

        let processed: Vec<ProcessedObject> = object_tasks
            .into_iter()
            .map(|task| {
                let object_id = ObjectId::new(
                    task.db_name.clone(),
                    task.schema_name.clone(),
                    task.typed_obj.stmt.ident().object.clone(),
                );

                let (dependencies, clusters) = extract_dependencies(
                    &task.typed_obj.stmt,
                    &task.db_name,
                    &task.schema_name,
                );

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

        // ── Step 3: Reassemble ──────────────────────────────────────────
        // Merge results into dependency graph and hierarchical structure.

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
                    let ref_id = ObjectId::from_raw_item_name(
                        &refs.object,
                        &po.db_name,
                        &po.schema_name,
                    );
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
    let mut deps = BTreeSet::new();
    let mut clusters = BTreeSet::new();

    match stmt {
        Statement::CreateView(s) => {
            extract_query_dependencies(
                &s.definition.query,
                default_database,
                default_schema,
                &mut deps,
            );
        }
        Statement::CreateMaterializedView(s) => {
            extract_query_dependencies(&s.query, default_database, default_schema, &mut deps);

            // Extract cluster dependency from IN CLUSTER clause
            if let Some(ref cluster_name) = s.in_cluster {
                clusters.insert(Cluster::new(cluster_name.to_string()));
            }
        }
        Statement::CreateTableFromSource(s) => {
            // Table depends on the source it's created from
            let source_id =
                ObjectId::from_raw_item_name(&s.source, default_database, default_schema);
            deps.insert(source_id);
        }
        Statement::CreateSink(s) => {
            // Sink depends on the shard it reads from
            let from_id = ObjectId::from_raw_item_name(&s.from, default_database, default_schema);
            deps.insert(from_id);

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
                &mut deps,
            );

            if let Some(ref cluster_name) = s.in_cluster {
                clusters.insert(Cluster::new(cluster_name.to_string()));
            }
        }
        Statement::CreateConnection(s) => {
            extract_connection_option_deps(&s.values, default_database, default_schema, &mut deps);
        }
        // These don't have dependencies on other database objects
        Statement::CreateTable(_) | Statement::CreateSecret(_) => {}
    }

    (deps, clusters)
}

/// Entry point for query dependency extraction (views and materialized views).
///
/// Starts with an empty CTE scope — the `_with_ctes` variant handles
/// nested scoping as it recurses.
fn extract_query_dependencies(
    query: &Query<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut BTreeSet<ObjectId>,
) {
    extract_query_dependencies_with_ctes(
        query,
        default_database,
        default_schema,
        deps,
        &BTreeSet::new(),
    );
}

/// Extract dependencies from a query, carrying the accumulated CTE scope.
///
/// Handles both simple and mutually recursive CTE blocks, merging parent
/// scope with locally defined CTEs before recursing into the query body.
fn extract_query_dependencies_with_ctes(
    query: &Query<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut BTreeSet<ObjectId>,
    parent_cte_names: &BTreeSet<String>,
) {
    // Collect CTE names from this query level
    let local_cte_names = match &query.ctes {
        CteBlock::Simple(ctes) => ctes
            .iter()
            .map(|cte| cte.alias.name.to_string())
            .collect::<BTreeSet<String>>(),
        CteBlock::MutuallyRecursive(mut_rec_block) => mut_rec_block
            .ctes
            .iter()
            .map(|cte| cte.name.to_string())
            .collect::<BTreeSet<String>>(),
    };

    // Merge parent and local CTE names for the combined scope
    let mut combined_cte_names = parent_cte_names.clone();
    combined_cte_names.extend(local_cte_names.iter().cloned());

    // Extract from CTEs (WITH clause)
    match &query.ctes {
        CteBlock::Simple(ctes) => {
            // For Simple CTEs, build scope incrementally: each CTE can only see
            // parent CTEs and earlier CTEs at this level (not itself or later CTEs)
            let mut incremental_cte_names = parent_cte_names.clone();
            for cte in ctes {
                extract_query_dependencies_with_ctes(
                    &cte.query,
                    default_database,
                    default_schema,
                    deps,
                    &incremental_cte_names,
                );
                // Add this CTE to the scope for the next CTE
                incremental_cte_names.insert(cte.alias.name.to_string());
            }
        }
        CteBlock::MutuallyRecursive(mut_rec_block) => {
            // For MutuallyRecursive CTEs, all CTEs can reference each other in any order
            // Pass the combined scope (parent + all CTEs at this level) to each CTE
            for cte in &mut_rec_block.ctes {
                extract_query_dependencies_with_ctes(
                    &cte.query,
                    default_database,
                    default_schema,
                    deps,
                    &combined_cte_names,
                );
            }
        }
    }

    // Extract from the main query body, passing combined CTE names to exclude
    extract_set_expr_dependencies_with_ctes(
        &query.body,
        default_database,
        default_schema,
        deps,
        &combined_cte_names,
    );
}

/// Extract dependencies from a set expression, excluding CTE names.
fn extract_set_expr_dependencies_with_ctes(
    set_expr: &SetExpr<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut BTreeSet<ObjectId>,
    cte_names: &BTreeSet<String>,
) {
    match set_expr {
        SetExpr::Select(select) => {
            extract_select_dependencies_with_ctes(
                select,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
        }
        SetExpr::Query(query) => {
            extract_query_dependencies_with_ctes(
                query,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
        }
        SetExpr::SetOperation { left, right, .. } => {
            extract_set_expr_dependencies_with_ctes(
                left,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
            extract_set_expr_dependencies_with_ctes(
                right,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
        }
        SetExpr::Values(_) | SetExpr::Show(_) | SetExpr::Table(_) => {
            // These don't reference other tables
        }
    }
}

/// Extract dependencies from a SELECT statement, excluding CTE names.
fn extract_select_dependencies_with_ctes(
    select: &Select<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut BTreeSet<ObjectId>,
    cte_names: &BTreeSet<String>,
) {
    // Extract from FROM clause
    for table_with_joins in &select.from {
        extract_table_factor_dependencies_with_ctes(
            &table_with_joins.relation,
            default_database,
            default_schema,
            deps,
            cte_names,
        );

        // Extract from JOINs
        for join in &table_with_joins.joins {
            extract_table_factor_dependencies_with_ctes(
                &join.relation,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
        }
    }

    // Extract from WHERE clause (subqueries)
    if let Some(ref selection) = select.selection {
        extract_expr_dependencies_with_ctes(
            selection,
            default_database,
            default_schema,
            deps,
            cte_names,
        );
    }

    // Extract from SELECT items (subqueries, function calls)
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            extract_expr_dependencies_with_ctes(
                expr,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
        }
    }
}

/// Extract dependencies from a table factor (FROM clause item), excluding CTE names.
///
/// For `TableFactor::Table`, checks whether the reference is a CTE by looking
/// for unqualified single-identifier names in the current CTE scope.
fn extract_table_factor_dependencies_with_ctes(
    table_factor: &TableFactor<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut BTreeSet<ObjectId>,
    cte_names: &BTreeSet<String>,
) {
    match table_factor {
        TableFactor::Table { name, .. } => {
            // name is &RawItemName
            // Check if this is a CTE reference (unqualified single identifier)
            let unresolved_name = name.name();
            if unresolved_name.0.len() == 1 {
                let table_name = unresolved_name.0[0].to_string();
                if cte_names.contains(&table_name) {
                    // This is a CTE reference - don't add it as a dependency
                    return;
                }
            }

            let obj_id = ObjectId::from_raw_item_name(name, default_database, default_schema);
            deps.insert(obj_id);
        }
        TableFactor::Derived { subquery, .. } => {
            extract_query_dependencies_with_ctes(
                subquery,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
        }
        TableFactor::Function { .. } => {
            // Table functions might reference tables, but this is complex to extract
            // For now, we don't track these dependencies
        }
        TableFactor::RowsFrom { .. } => {
            // ROWS FROM might reference tables, but this is complex to extract
            // For now, we don't track these dependencies
        }
        TableFactor::NestedJoin { join, .. } => {
            extract_table_factor_dependencies_with_ctes(
                &join.relation,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
            for nested_join in &join.joins {
                extract_table_factor_dependencies_with_ctes(
                    &nested_join.relation,
                    default_database,
                    default_schema,
                    deps,
                    cte_names,
                );
            }
        }
    }
}

/// Extract dependencies from an expression tree, excluding CTE names.
///
/// Recurses into subqueries (`EXISTS`, `IN (SELECT ...)`), binary operations,
/// CASE expressions, function calls, and array/list constructors to find
/// any nested table references.
fn extract_expr_dependencies_with_ctes(
    expr: &Expr<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut BTreeSet<ObjectId>,
    cte_names: &BTreeSet<String>,
) {
    match expr {
        Expr::Subquery(query) | Expr::Exists(query) => {
            extract_query_dependencies_with_ctes(
                query,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
        }
        Expr::InSubquery { expr, subquery, .. } => {
            extract_expr_dependencies_with_ctes(
                expr,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
            extract_query_dependencies_with_ctes(
                subquery,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            extract_expr_dependencies_with_ctes(
                expr,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
            extract_expr_dependencies_with_ctes(
                low,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
            extract_expr_dependencies_with_ctes(
                high,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
        }
        Expr::Op { expr1, expr2, .. } => {
            // Extract from operands of binary/unary operations (e.g., AND, OR, =, +, etc.)
            extract_expr_dependencies_with_ctes(
                expr1,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
            if let Some(expr2) = expr2 {
                extract_expr_dependencies_with_ctes(
                    expr2,
                    default_database,
                    default_schema,
                    deps,
                    cte_names,
                );
            }
        }
        Expr::Cast { expr, .. } => {
            extract_expr_dependencies_with_ctes(
                expr,
                default_database,
                default_schema,
                deps,
                cte_names,
            );
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                extract_expr_dependencies_with_ctes(
                    operand,
                    default_database,
                    default_schema,
                    deps,
                    cte_names,
                );
            }
            for cond in conditions {
                extract_expr_dependencies_with_ctes(
                    cond,
                    default_database,
                    default_schema,
                    deps,
                    cte_names,
                );
            }
            for result in results {
                extract_expr_dependencies_with_ctes(
                    result,
                    default_database,
                    default_schema,
                    deps,
                    cte_names,
                );
            }
            if let Some(else_result) = else_result {
                extract_expr_dependencies_with_ctes(
                    else_result,
                    default_database,
                    default_schema,
                    deps,
                    cte_names,
                );
            }
        }
        Expr::Function(func) => {
            // Extract from function arguments
            match &func.args {
                FunctionArgs::Star => {}
                FunctionArgs::Args { args, order_by } => {
                    for arg in args {
                        extract_expr_dependencies_with_ctes(
                            arg,
                            default_database,
                            default_schema,
                            deps,
                            cte_names,
                        );
                    }
                    for order in order_by {
                        extract_expr_dependencies_with_ctes(
                            &order.expr,
                            default_database,
                            default_schema,
                            deps,
                            cte_names,
                        );
                    }
                }
            }
        }
        Expr::Array(exprs) | Expr::List(exprs) | Expr::Row { exprs } => {
            for expr in exprs {
                extract_expr_dependencies_with_ctes(
                    expr,
                    default_database,
                    default_schema,
                    deps,
                    cte_names,
                );
            }
        }
        // Other expression types don't contain subqueries
        _ => {}
    }
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
