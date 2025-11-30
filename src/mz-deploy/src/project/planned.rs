//! Planned representation for Materialize projects.
//!
//! This module provides a dependency-aware representation of a Materialize project.
//! It builds on top of the validated typed representation and adds dependency tracking between objects,
//! enabling topological sorting for deployment order.
//!
//! # Transformation Flow
//!
//! ```text
//! raw::Project → typed::Project → planned::Project
//!                    ↓              ↓
//!              (validated)    (with dependencies)
//! ```
//!
//! # Dependency Extraction
//!
//! Dependencies are extracted from:
//! - View and materialized view queries (FROM clauses, JOINs, subqueries, CTEs)
//! - Tables created from sources
//! - Indexes (the table/view they're created on)
//! - Sinks (the object they read from)
//!
//! # Example
//!
//! ```text
//! CREATE TABLE users (...);
//! CREATE VIEW active_users AS SELECT * FROM users WHERE active = true;
//! CREATE INDEX idx ON active_users (id);
//!
//! Dependencies:
//! - active_users depends on: users
//! - idx depends on: active_users
//! ```

use super::ast::{Cluster, Statement};
use super::error::DependencyError;
use super::typed;
use crate::project::object_id::ObjectId;
use mz_sql_parser::ast::*;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

/// A database object with its dependencies.
#[derive(Debug)]
pub struct DatabaseObject {
    /// The object identifier
    pub id: ObjectId,
    /// The validated typed statement
    pub typed_object: typed::DatabaseObject,
    /// Set of objects this object depends on
    pub dependencies: HashSet<ObjectId>,
}

/// A module-level statement with context about where it should be executed.
///
/// Module statements are executed before object statements and come from
/// database.sql or schema.sql files. They're used for setup like grants,
/// comments, and other database/schema-level configuration.
#[derive(Debug)]
pub enum ModStatement<'a> {
    /// Database-level statement (from database.sql file)
    Database {
        /// The database name
        database: &'a str,
        /// The statement to execute
        statement: &'a mz_sql_parser::ast::Statement<Raw>,
    },
    /// Schema-level statement (from schema.sql file)
    Schema {
        /// The database name
        database: &'a str,
        /// The schema name
        schema: &'a str,
        /// The statement to execute
        statement: &'a mz_sql_parser::ast::Statement<Raw>,
    },
}

/// A schema containing objects with dependency information.
#[derive(Debug)]
pub struct Schema {
    pub name: String,
    pub objects: Vec<DatabaseObject>,
    /// Optional module-level statements (from schema.sql file)
    pub mod_statements: Option<Vec<mz_sql_parser::ast::Statement<Raw>>>,
}

impl Hash for Schema {
    fn hash<H: Hasher>(&self, state: &mut H) {
        todo!()
    }
}

/// A database containing schemas with dependency information.
#[derive(Debug)]
pub struct Database {
    pub name: String,
    pub schemas: Vec<Schema>,
    /// Optional module-level statements (from database.sql file)
    pub mod_statements: Option<Vec<mz_sql_parser::ast::Statement<Raw>>>,
}

/// A project with full dependency tracking.
#[derive(Debug)]
pub struct Project {
    pub databases: Vec<Database>,
    /// Global dependency graph: object_id -> set of dependencies
    pub dependency_graph: HashMap<ObjectId, HashSet<ObjectId>>,
    /// External dependencies: objects referenced but not defined in this project
    pub external_dependencies: HashSet<ObjectId>,
    /// Cluster dependencies: clusters referenced by indexes and materialized views
    pub cluster_dependencies: HashSet<Cluster>,
    /// Unit tests defined in the project, organized by the object they test
    pub tests: Vec<(ObjectId, crate::unit_test::UnitTest)>,
}

impl Project {
    /// Get topologically sorted objects for deployment.
    ///
    /// Returns objects in an order where dependencies come before dependents.
    /// External dependencies are excluded from the sort as they are not deployable.
    pub fn topological_sort(&self) -> Result<Vec<ObjectId>, DependencyError> {
        let mut sorted = Vec::new();
        let mut visited = HashSet::new();
        let mut in_progress = HashSet::new();

        for object_id in self.dependency_graph.keys() {
            // Skip external dependencies - we don't deploy them
            if self.external_dependencies.contains(object_id) {
                continue;
            }

            if !visited.contains(object_id) {
                self.visit(object_id, &mut visited, &mut in_progress, &mut sorted)?;
            }
        }

        Ok(sorted)
    }

    fn visit(
        &self,
        object_id: &ObjectId,
        visited: &mut HashSet<ObjectId>,
        in_progress: &mut HashSet<ObjectId>,
        sorted: &mut Vec<ObjectId>,
    ) -> Result<(), DependencyError> {
        if self.external_dependencies.contains(object_id) {
            return Ok(());
        }

        if in_progress.contains(object_id) {
            return Err(DependencyError::CircularDependency {
                object: object_id.clone(),
            });
        }

        if visited.contains(object_id) {
            return Ok(());
        }

        in_progress.insert(object_id.clone());

        if let Some(deps) = self.dependency_graph.get(object_id) {
            for dep in deps {
                self.visit(dep, visited, in_progress, sorted)?;
            }
        }

        in_progress.remove(object_id);
        visited.insert(object_id.clone());
        sorted.push(object_id.clone());

        Ok(())
    }

    /// Get all database objects in topological order with their typed statements.
    ///
    /// Returns a vector of (ObjectId, typed DatabaseObject) tuples in deployment order.
    /// This allows access to the fully qualified SQL statements for each object.
    pub fn get_sorted_objects(
        &self,
    ) -> Result<Vec<(ObjectId, &typed::DatabaseObject)>, DependencyError> {
        let sorted_ids = self.topological_sort()?;
        let mut result = Vec::new();

        for object_id in sorted_ids {
            // Find the corresponding typed object
            if let Some(typed_obj) = self.find_typed_object(&object_id) {
                result.push((object_id, typed_obj));
            }
        }

        Ok(result)
    }

    /// Find the typed object for a given ObjectId.
    fn find_typed_object(&self, object_id: &ObjectId) -> Option<&typed::DatabaseObject> {
        for database in &self.databases {
            if database.name != object_id.database {
                continue;
            }
            for schema in &database.schemas {
                if schema.name != object_id.schema {
                    continue;
                }
                for obj in &schema.objects {
                    if obj.id == *object_id {
                        return Some(&obj.typed_object);
                    }
                }
            }
        }
        None
    }

    /// Returns all module-level statements in execution order.
    ///
    /// Module statements are executed before object statements and come from
    /// database.sql or schema.sql files. They're used for setup like grants,
    /// comments, and other database/schema-level configuration.
    ///
    /// # Execution Order
    ///
    /// 1. All database-level mod statements (in the order databases appear)
    /// 2. All schema-level mod statements (in the order schemas appear)
    ///
    /// This ensures that database setup happens before schema setup, which
    /// happens before object creation.
    ///
    /// # Returns
    ///
    /// A vector of `ModStatement` enums, each containing:
    /// - Context (database name, schema name for schema-level statements)
    /// - Reference to the statement to execute
    pub fn iter_mod_statements(&self) -> Vec<ModStatement<'_>> {
        let mut result = Vec::new();

        // First: all database-level mod statements
        for database in &self.databases {
            if let Some(stmts) = &database.mod_statements {
                for stmt in stmts {
                    result.push(ModStatement::Database {
                        database: &database.name,
                        statement: stmt,
                    });
                }
            }
        }

        // Second: all schema-level mod statements
        for database in &self.databases {
            for schema in &database.schemas {
                if let Some(stmts) = &schema.mod_statements {
                    for stmt in stmts {
                        result.push(ModStatement::Schema {
                            database: &database.name,
                            schema: &schema.name,
                            statement: stmt,
                        });
                    }
                }
            }
        }

        result
    }

    /// Build a reverse dependency graph.
    ///
    /// Maps each object to the set of objects that depend on it.
    /// Used for incremental deployment to find downstream dependencies.
    ///
    /// # Returns
    /// HashMap where key is an ObjectId and value is the set of objects that depend on it
    pub fn build_reverse_dependency_graph(&self) -> HashMap<ObjectId, HashSet<ObjectId>> {
        let mut reverse = HashMap::new();

        for (obj_id, deps) in &self.dependency_graph {
            for dep in deps {
                reverse
                    .entry(dep.clone())
                    .or_insert_with(HashSet::new)
                    .insert(obj_id.clone());
            }
        }

        reverse
    }

    /// Get topologically sorted objects filtered by a set of object IDs.
    ///
    /// Returns objects in deployment order, but only those in the filter set.
    /// Maintains topological ordering within the filtered subset.
    ///
    /// # Arguments
    /// * `filter` - Set of ObjectIds to include in the result
    ///
    /// # Returns
    /// Vector of (ObjectId, typed DatabaseObject) tuples in deployment order
    pub fn get_sorted_objects_filtered(
        &self,
        filter: &HashSet<ObjectId>,
    ) -> Result<Vec<(ObjectId, &typed::DatabaseObject)>, DependencyError> {
        let sorted_ids = self.topological_sort()?;

        // Filter to only include objects in the filter set
        let filtered_ids: Vec<ObjectId> = sorted_ids
            .into_iter()
            .filter(|id| filter.contains(id))
            .collect();

        let mut result = Vec::new();
        for object_id in filtered_ids {
            if let Some(typed_obj) = self.find_typed_object(&object_id) {
                result.push((object_id, typed_obj));
            }
        }

        Ok(result)
    }

    /// Iterate over all database objects in the project.
    ///
    /// This flattens the database → schema → object hierarchy into a single iterator.
    ///
    /// # Returns
    /// Iterator over references to all DatabaseObject instances in the project
    ///
    /// # Example
    /// ```ignore
    /// for obj in project.iter_objects() {
    ///     println!("Object: {}", obj.id);
    /// }
    /// ```
    pub fn iter_objects(&self) -> impl Iterator<Item = &DatabaseObject> {
        self.databases
            .iter()
            .flat_map(|db| db.schemas.iter())
            .flat_map(|schema| schema.objects.iter())
    }

    /// Find a database object by its ObjectId.
    ///
    /// This is more efficient than manually iterating through the hierarchy.
    ///
    /// # Arguments
    /// * `id` - The ObjectId to search for
    ///
    /// # Returns
    /// Some(&DatabaseObject) if found, None otherwise
    ///
    /// # Example
    /// ```ignore
    /// if let Some(obj) = project.find_object(&object_id) {
    ///     println!("Found: {}", obj.id);
    /// }
    /// ```
    pub fn find_object(&self, id: &ObjectId) -> Option<&DatabaseObject> {
        self.iter_objects().find(|obj| &obj.id == id)
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

                schemas.push(Schema {
                    name: typed_schema.name,
                    objects,
                    mod_statements: typed_schema.mod_statements,
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
        Statement::CreateView(_) | Statement::CreateTable(_) => object
            .typed_object
            .indexes
            .iter()
            .map(|index| {
                let cluster = Cluster {
                    name: index.in_cluster.clone().unwrap().to_string(),
                };

                (cluster, index.clone())
            })
            .collect(),
        Statement::CreateMaterializedView(materialized_view) => {
            let mv_cluster = Cluster {
                name: materialized_view.in_cluster.clone().unwrap().to_string(),
            };

            object
                .typed_object
                .indexes
                .iter()
                .filter_map(|index| {
                    let index_cluster = Cluster {
                        name: index.in_cluster.clone().unwrap().to_string(),
                    };

                    (mv_cluster != index_cluster).then(|| (index_cluster, index.clone()))
                })
                .collect()
        }
        _ => unimplemented!(),
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

            // Sink also depends on the connection it uses
            // CreateSinkConnection is an enum, need to extract connection name from each variant
            let connection_name = match &s.connection {
                CreateSinkConnection::Kafka { connection, .. } => Some(connection),
                CreateSinkConnection::Iceberg { connection, .. } => Some(connection),
            };
            if let Some(connection) = connection_name {
                let connection_id =
                    ObjectId::from_raw_item_name(connection, default_database, default_schema);
                deps.insert(connection_id);
            }
        }
        Statement::CreateSubsource(s) => {
            // Subsource depends on its parent source (if specified)
            if let Some(ref source) = s.of_source {
                let source_id =
                    ObjectId::from_raw_item_name(source, default_database, default_schema);
                deps.insert(source_id);
            }
        }
        // These don't have dependencies on other database objects
        Statement::CreateConnection(_)
        | Statement::CreateWebhookSource(_)
        | Statement::CreateSource(_)
        | Statement::CreateTable(_)
        | Statement::CreateSecret(_) => {}
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
    // Extract from CTEs (WITH clause)
    // CteBlock is an enum, need to check which variant
    if let CteBlock::Simple(ctes) = &query.ctes {
        for cte in ctes {
            extract_query_dependencies(&cte.query, default_database, default_schema, deps);
        }
    }

    // Extract from the main query body
    extract_set_expr_dependencies(&query.body, default_database, default_schema, deps);
}

/// Extract dependencies from a set expression.
fn extract_set_expr_dependencies(
    set_expr: &SetExpr<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut HashSet<ObjectId>,
) {
    match set_expr {
        SetExpr::Select(select) => {
            extract_select_dependencies(select, default_database, default_schema, deps);
        }
        SetExpr::Query(query) => {
            extract_query_dependencies(query, default_database, default_schema, deps);
        }
        SetExpr::SetOperation { left, right, .. } => {
            extract_set_expr_dependencies(left, default_database, default_schema, deps);
            extract_set_expr_dependencies(right, default_database, default_schema, deps);
        }
        SetExpr::Values(_) | SetExpr::Show(_) | SetExpr::Table(_) => {
            // These don't reference other tables
        }
    }
}

/// Extract dependencies from a SELECT statement.
fn extract_select_dependencies(
    select: &Select<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut HashSet<ObjectId>,
) {
    // Extract from FROM clause
    for table_with_joins in &select.from {
        extract_table_factor_dependencies(
            &table_with_joins.relation,
            default_database,
            default_schema,
            deps,
        );

        // Extract from JOINs
        for join in &table_with_joins.joins {
            extract_table_factor_dependencies(
                &join.relation,
                default_database,
                default_schema,
                deps,
            );
        }
    }

    // Extract from WHERE clause (subqueries)
    if let Some(ref selection) = select.selection {
        extract_expr_dependencies(selection, default_database, default_schema, deps);
    }

    // Extract from SELECT items (subqueries, function calls)
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            extract_expr_dependencies(expr, default_database, default_schema, deps);
        }
    }
}

/// Extract dependencies from a table factor (table reference or subquery).
fn extract_table_factor_dependencies(
    table_factor: &TableFactor<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut HashSet<ObjectId>,
) {
    match table_factor {
        TableFactor::Table { name, .. } => {
            // name is &RawItemName
            let obj_id = ObjectId::from_raw_item_name(name, default_database, default_schema);
            deps.insert(obj_id);
        }
        TableFactor::Derived { subquery, .. } => {
            extract_query_dependencies(subquery, default_database, default_schema, deps);
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
            extract_table_factor_dependencies(
                &join.relation,
                default_database,
                default_schema,
                deps,
            );
            for nested_join in &join.joins {
                extract_table_factor_dependencies(
                    &nested_join.relation,
                    default_database,
                    default_schema,
                    deps,
                );
            }
        }
    }
}

/// Extract dependencies from an expression (for subqueries in WHERE, etc).
fn extract_expr_dependencies(
    expr: &Expr<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut HashSet<ObjectId>,
) {
    match expr {
        Expr::Subquery(query) | Expr::Exists(query) => {
            extract_query_dependencies(query, default_database, default_schema, deps);
        }
        Expr::InSubquery { expr, subquery, .. } => {
            extract_expr_dependencies(expr, default_database, default_schema, deps);
            extract_query_dependencies(subquery, default_database, default_schema, deps);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            extract_expr_dependencies(expr, default_database, default_schema, deps);
            extract_expr_dependencies(low, default_database, default_schema, deps);
            extract_expr_dependencies(high, default_database, default_schema, deps);
        }
        Expr::Op { .. } => {
            // Binary/unary operations - skip for now as structure is complex
            // TODO: Recursively extract from operands when needed
        }
        Expr::Cast { expr, .. } => {
            extract_expr_dependencies(expr, default_database, default_schema, deps);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                extract_expr_dependencies(operand, default_database, default_schema, deps);
            }
            for cond in conditions {
                extract_expr_dependencies(cond, default_database, default_schema, deps);
            }
            for result in results {
                extract_expr_dependencies(result, default_database, default_schema, deps);
            }
            if let Some(else_result) = else_result {
                extract_expr_dependencies(else_result, default_database, default_schema, deps);
            }
        }
        Expr::Function(func) => {
            // Extract from function arguments
            match &func.args {
                FunctionArgs::Star => {}
                FunctionArgs::Args { args, order_by } => {
                    for arg in args {
                        extract_expr_dependencies(arg, default_database, default_schema, deps);
                    }
                    for order in order_by {
                        extract_expr_dependencies(
                            &order.expr,
                            default_database,
                            default_schema,
                            deps,
                        );
                    }
                }
            }
        }
        Expr::Array(exprs) | Expr::List(exprs) | Expr::Row { exprs } => {
            for expr in exprs {
                extract_expr_dependencies(expr, default_database, default_schema, deps);
            }
        }
        // Other expression types don't contain subqueries
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_sql_parser::ast::Ident;

    #[test]
    fn test_object_id_from_item_name() {
        let name = UnresolvedItemName(vec![Ident::new("users").unwrap()]);
        let id = ObjectId::from_item_name(&name, "db", "public");
        assert_eq!(id.database, "db");
        assert_eq!(id.schema, "public");
        assert_eq!(id.object, "users");

        let name = UnresolvedItemName(vec![
            Ident::new("myschema").unwrap(),
            Ident::new("users").unwrap(),
        ]);
        let id = ObjectId::from_item_name(&name, "db", "public");
        assert_eq!(id.database, "db");
        assert_eq!(id.schema, "myschema");
        assert_eq!(id.object, "users");

        let name = UnresolvedItemName(vec![
            Ident::new("mydb").unwrap(),
            Ident::new("myschema").unwrap(),
            Ident::new("users").unwrap(),
        ]);
        let id = ObjectId::from_item_name(&name, "db", "public");
        assert_eq!(id.database, "mydb");
        assert_eq!(id.schema, "myschema");
        assert_eq!(id.object, "users");
    }

    #[test]
    fn test_object_id_fqn() {
        let id = ObjectId::new("db".to_string(), "schema".to_string(), "table".to_string());
        assert_eq!(id.to_string(), "db.schema.table");
    }

    #[test]
    fn test_cluster_equality() {
        let c1 = Cluster::new("quickstart".to_string());
        let c2 = Cluster::new("quickstart".to_string());
        let c3 = Cluster::new("prod".to_string());

        assert_eq!(c1, c2);
        assert_ne!(c1, c3);
    }

    #[test]
    fn test_cluster_in_hashset() {
        let mut clusters = HashSet::new();
        clusters.insert(Cluster::new("quickstart".to_string()));
        clusters.insert(Cluster::new("quickstart".to_string())); // duplicate
        clusters.insert(Cluster::new("prod".to_string()));

        assert_eq!(clusters.len(), 2);
        assert!(clusters.contains(&Cluster::new("quickstart".to_string())));
        assert!(clusters.contains(&Cluster::new("prod".to_string())));
    }

    #[test]
    fn test_extract_dependencies_materialized_view_with_cluster() {
        let sql = "CREATE MATERIALIZED VIEW mv IN CLUSTER quickstart AS SELECT * FROM users";
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateMaterializedView(mv_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateMaterializedView(mv_stmt.clone());
            let (deps, clusters) = extract_dependencies(&stmt, "db", "public");

            // Should have one dependency (users table)
            assert_eq!(deps.len(), 1);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "users".to_string()
            )));

            // Should have one cluster dependency
            assert_eq!(clusters.len(), 1);
            assert!(clusters.contains(&Cluster::new("quickstart".to_string())));
        } else {
            panic!("Expected CreateMaterializedView statement");
        }
    }

    #[test]
    fn test_extract_dependencies_materialized_view_without_cluster() {
        let sql = "CREATE MATERIALIZED VIEW mv AS SELECT * FROM users";
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateMaterializedView(mv_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateMaterializedView(mv_stmt.clone());
            let (deps, clusters) = extract_dependencies(&stmt, "db", "public");

            // Should have one dependency (users table)
            assert_eq!(deps.len(), 1);

            // Should have no cluster dependencies
            assert_eq!(clusters.len(), 0);
        } else {
            panic!("Expected CreateMaterializedView statement");
        }
    }

    #[test]
    fn test_extract_dependencies_view_no_clusters() {
        let sql = "CREATE VIEW v AS SELECT * FROM users";
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (_deps, clusters) = extract_dependencies(&stmt, "db", "public");

            // Views don't have cluster dependencies
            assert_eq!(clusters.len(), 0);
        } else {
            panic!("Expected CreateView statement");
        }
    }

    #[test]
    fn test_multiple_materialized_views_with_different_clusters() {
        let sqls = vec![
            "CREATE MATERIALIZED VIEW mv1 IN CLUSTER quickstart AS SELECT * FROM t1",
            "CREATE MATERIALIZED VIEW mv2 IN CLUSTER prod AS SELECT * FROM t2",
            "CREATE MATERIALIZED VIEW mv3 IN CLUSTER quickstart AS SELECT * FROM t3",
        ];

        let mut all_clusters = HashSet::new();

        for sql in sqls {
            let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();
            if let mz_sql_parser::ast::Statement::CreateMaterializedView(mv_stmt) = &parsed[0].ast {
                let stmt = Statement::CreateMaterializedView(mv_stmt.clone());
                let (_deps, clusters) = extract_dependencies(&stmt, "db", "public");
                all_clusters.extend(clusters);
            }
        }

        // Should have 2 unique clusters (quickstart and prod)
        assert_eq!(all_clusters.len(), 2);
        assert!(all_clusters.contains(&Cluster::new("quickstart".to_string())));
        assert!(all_clusters.contains(&Cluster::new("prod".to_string())));
    }

    #[test]
    fn test_build_reverse_dependency_graph() {
        use std::collections::HashMap;

        // Create a simple dependency graph
        let mut dependency_graph = HashMap::new();

        let obj1 = ObjectId::new("db".to_string(), "public".to_string(), "table1".to_string());
        let obj2 = ObjectId::new("db".to_string(), "public".to_string(), "view1".to_string());
        let obj3 = ObjectId::new("db".to_string(), "public".to_string(), "view2".to_string());

        // view1 depends on table1
        let mut deps1 = HashSet::new();
        deps1.insert(obj1.clone());
        dependency_graph.insert(obj2.clone(), deps1);

        // view2 depends on view1
        let mut deps2 = HashSet::new();
        deps2.insert(obj2.clone());
        dependency_graph.insert(obj3.clone(), deps2);

        // table1 has no dependencies
        dependency_graph.insert(obj1.clone(), HashSet::new());

        let project = Project {
            databases: vec![],
            dependency_graph,
            external_dependencies: HashSet::new(),
            cluster_dependencies: HashSet::new(),
            tests: vec![],
        };

        // Build reverse graph
        let reverse = project.build_reverse_dependency_graph();

        // table1 should have view1 as a dependent
        assert!(reverse.get(&obj1).unwrap().contains(&obj2));

        // view1 should have view2 as a dependent
        assert!(reverse.get(&obj2).unwrap().contains(&obj3));

        // view2 should have no dependents
        assert!(!reverse.contains_key(&obj3));
    }

    #[test]
    fn test_get_sorted_objects_filtered() {
        use crate::project::typed;
        use crate::project::raw;
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let src_dir = temp_dir.path();

        // Create test structure
        let db_path = src_dir.join("test_db");
        let schema_path = db_path.join("public");
        fs::create_dir_all(&schema_path).unwrap();

        // Create table
        fs::write(
            schema_path.join("table1.sql"),
            "CREATE TABLE table1 (id INT);",
        )
        .unwrap();

        // Create view depending on table
        fs::write(
            schema_path.join("view1.sql"),
            "CREATE VIEW view1 AS SELECT * FROM table1;",
        )
        .unwrap();

        // Create another view depending on view1
        fs::write(
            schema_path.join("view2.sql"),
            "CREATE VIEW view2 AS SELECT * FROM view1;",
        )
        .unwrap();

        // Load and convert to planned
        let raw_project = raw::load_project(src_dir).unwrap();
        let typed_project = typed::Project::try_from(raw_project).unwrap();
        let planned_project = Project::from(typed_project);

        // Create filter that only includes view1
        let mut filter = HashSet::new();
        let view1_id = ObjectId::new(
            "test_db".to_string(),
            "public".to_string(),
            "view1".to_string(),
        );
        filter.insert(view1_id.clone());

        // Get filtered objects
        let filtered = planned_project.get_sorted_objects_filtered(&filter).unwrap();

        // Should only contain view1
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].0, view1_id);
    }
}
