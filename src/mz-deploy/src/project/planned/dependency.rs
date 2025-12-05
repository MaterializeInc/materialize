//! Dependency extraction and project conversion from typed representation.

use super::super::ast::{Cluster, Statement};
use super::super::typed;
use super::types::{Database, DatabaseObject, Project, Schema, SchemaType};
use crate::project::object_id::ObjectId;
use mz_sql_parser::ast::*;
use std::collections::{HashMap, HashSet};

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
        | Statement::CreateSink(_) => SchemaType::Storage,
        Statement::CreateView(_) | Statement::CreateMaterializedView(_) => SchemaType::Compute,
    }
}

impl From<typed::Project> for Project {
    fn from(typed_project: typed::Project) -> Self {
        let mut dependency_graph = HashMap::new();
        let mut databases = Vec::new();
        let mut defined_objects = HashSet::new();
        let mut cluster_dependencies = HashSet::new();
        let mut tests = Vec::new();

        // First pass: collect all objects defined in the project
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

        // Second pass: build dependency graph and track external dependencies and clusters
        let mut external_dependencies = HashSet::new();

        for typed_db in typed_project.databases {
            let mut schemas = Vec::new();

            for typed_schema in typed_db.schemas {
                let mut objects = Vec::new();

                for typed_obj in typed_schema.objects {
                    let object_id = ObjectId::new(
                        typed_db.name.clone(),
                        typed_schema.name.clone(),
                        typed_obj.stmt.ident().object.clone(),
                    );

                    // Extract dependencies from the statement
                    let (dependencies, clusters) =
                        extract_dependencies(&typed_obj.stmt, &typed_db.name, &typed_schema.name);

                    // Track cluster dependencies
                    for cluster in clusters {
                        cluster_dependencies.insert(cluster);
                    }

                    // Check for external dependencies
                    for dep in &dependencies {
                        if !defined_objects.contains(dep) {
                            external_dependencies.insert(dep.clone());
                        }
                    }

                    dependency_graph.insert(object_id.clone(), dependencies.clone());

                    // Collect tests for this object
                    for test_stmt in &typed_obj.tests {
                        let unit_test = crate::unit_test::UnitTest::from_execute_statement(
                            test_stmt, &object_id,
                        );
                        tests.push((object_id.clone(), unit_test));
                    }

                    objects.push(DatabaseObject {
                        id: object_id,
                        typed_object: typed_obj,
                        dependencies,
                    });
                }

                // Determine schema type based on objects
                let schema_type = determine_schema_type(&objects);

                schemas.push(Schema {
                    name: typed_schema.name,
                    objects,
                    mod_statements: typed_schema.mod_statements,
                    schema_type,
                });
            }

            databases.push(Database {
                name: typed_db.name,
                schemas,
                mod_statements: typed_db.mod_statements,
            });
        }

        Project {
            databases,
            dependency_graph,
            external_dependencies,
            cluster_dependencies,
            tests,
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
                    let index_cluster =
                        Cluster::new(index.in_cluster.clone().unwrap().to_string());

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
) -> (HashSet<ObjectId>, HashSet<Cluster>) {
    let mut deps = HashSet::new();
    let mut clusters = HashSet::new();

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
        // These don't have dependencies on other database objects
        Statement::CreateTable(_) => {}
    }

    (deps, clusters)
}

/// Extract dependencies from a query (used by views and materialized views).
fn extract_query_dependencies(
    query: &Query<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut HashSet<ObjectId>,
) {
    extract_query_dependencies_with_ctes(
        query,
        default_database,
        default_schema,
        deps,
        &HashSet::new(),
    );
}

/// Extract dependencies from a query, with parent CTE scope.
fn extract_query_dependencies_with_ctes(
    query: &Query<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut HashSet<ObjectId>,
    parent_cte_names: &HashSet<String>,
) {
    // Collect CTE names from this query level
    let local_cte_names = match &query.ctes {
        CteBlock::Simple(ctes) => ctes
            .iter()
            .map(|cte| cte.alias.name.to_string())
            .collect::<HashSet<String>>(),
        CteBlock::MutuallyRecursive(mut_rec_block) => mut_rec_block
            .ctes
            .iter()
            .map(|cte| cte.name.to_string())
            .collect::<HashSet<String>>(),
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
    deps: &mut HashSet<ObjectId>,
    cte_names: &HashSet<String>,
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
    deps: &mut HashSet<ObjectId>,
    cte_names: &HashSet<String>,
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

/// Extract dependencies from a table factor, excluding CTE names.
fn extract_table_factor_dependencies_with_ctes(
    table_factor: &TableFactor<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut HashSet<ObjectId>,
    cte_names: &HashSet<String>,
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

/// Extract dependencies from an expression, excluding CTE names.
fn extract_expr_dependencies_with_ctes(
    expr: &Expr<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut HashSet<ObjectId>,
    cte_names: &HashSet<String>,
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
