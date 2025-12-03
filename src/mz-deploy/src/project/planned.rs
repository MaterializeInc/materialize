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

/// The type of objects contained in a schema.
///
/// Schemas are segregated by object type to prevent accidental recreation:
/// - Storage schemas contain tables, sinks, and tables from sources
/// - Compute schemas contain views and materialized views
/// - Empty schemas contain no objects
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaType {
    /// Schema contains storage objects (tables, sinks)
    Storage,
    /// Schema contains computation objects (views, materialized views)
    Compute,
    /// Schema contains no objects
    Empty,
}

/// A schema containing objects with dependency information.
#[derive(Debug)]
pub struct Schema {
    pub name: String,
    pub objects: Vec<DatabaseObject>,
    /// Optional module-level statements (from schema.sql file)
    pub mod_statements: Option<Vec<mz_sql_parser::ast::Statement<Raw>>>,
    /// The type of objects in this schema (Storage, Compute, or Empty)
    pub schema_type: SchemaType,
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

    pub fn get_tables(&self) -> impl Iterator<Item = ObjectId> {
        self.databases
            .iter()
            .flat_map(|db| db.schemas.iter())
            .flat_map(|schema| schema.objects.iter())
            .filter(|object| matches!(object.typed_object.stmt, Statement::CreateTable(_) | Statement::CreateTableFromSource(_)))
            .map(|object| object.id.clone())
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

    /// Validate that sources and sinks don't share clusters with indexes or materialized views.
    ///
    /// This validation prevents accidentally recreating sources/sinks when updating compute objects.
    ///
    /// # Arguments
    /// * `sources_by_cluster` - Map of cluster name to list of source FQNs from the database
    ///
    /// # Returns
    /// * `Ok(())` if no conflicts found
    /// * `Err((cluster_name, compute_objects, storage_objects))` if conflicts detected
    ///
    /// # Example
    /// ```ignore
    /// let sources = query_sources_by_cluster(&client).await?;
    /// project.validate_cluster_isolation(&sources)?;
    /// ```
    pub fn validate_cluster_isolation(
        &self,
        sources_by_cluster: &HashMap<String, Vec<String>>,
    ) -> Result<(), (String, Vec<String>, Vec<String>)> {
        // Build a map of cluster -> compute objects (indexes, MVs)
        let mut cluster_compute_objects: HashMap<String, Vec<String>> = HashMap::new();

        for db in &self.databases {
            for schema in &db.schemas {
                for obj in &schema.objects {
                    // Check for materialized views
                    if let Statement::CreateMaterializedView(mv) = &obj.typed_object.stmt {
                        if let Some(cluster_name) = &mv.in_cluster {
                            cluster_compute_objects
                                .entry(cluster_name.to_string())
                                .or_insert_with(Vec::new)
                                .push(obj.id.to_string());
                        }
                    }

                    // Check for indexes
                    for index in &obj.typed_object.indexes {
                        if let Some(cluster_name) = &index.in_cluster {
                            let index_name = index
                                .name
                                .as_ref()
                                .map(|n| format!(" (index: {})", n))
                                .unwrap_or_default();
                            cluster_compute_objects
                                .entry(cluster_name.to_string())
                                .or_insert_with(Vec::new)
                                .push(format!("{}{}", obj.id, index_name));
                        }
                    }
                }
            }
        }

        // Build a map of cluster -> sinks
        let mut cluster_sinks: HashMap<String, Vec<String>> = HashMap::new();

        for db in &self.databases {
            for schema in &db.schemas {
                for obj in &schema.objects {
                    if let Statement::CreateSink(sink) = &obj.typed_object.stmt {
                        if let Some(cluster_name) = &sink.in_cluster {
                            cluster_sinks
                                .entry(cluster_name.to_string())
                                .or_insert_with(Vec::new)
                                .push(obj.id.to_string());
                        }
                    }
                }
            }
        }

        // Get all clusters that have compute objects or sinks
        let mut all_clusters: HashSet<String> = HashSet::new();
        all_clusters.extend(cluster_compute_objects.keys().cloned());
        all_clusters.extend(cluster_sinks.keys().cloned());

        // Check for conflicts: cluster has both compute objects AND (sources OR sinks)
        for cluster_name in all_clusters {
            let compute_objects = cluster_compute_objects.get(&cluster_name);
            let sources = sources_by_cluster.get(&cluster_name);
            let sinks = cluster_sinks.get(&cluster_name);

            let has_compute = compute_objects.is_some() && !compute_objects.unwrap().is_empty();
            let has_sources = sources.is_some() && !sources.unwrap().is_empty();
            let has_sinks = sinks.is_some() && !sinks.unwrap().is_empty();

            if has_compute && (has_sources || has_sinks) {
                let mut storage_objects = Vec::new();
                if let Some(sources) = sources {
                    storage_objects.extend(sources.iter().cloned());
                }
                if let Some(sinks) = sinks {
                    storage_objects.extend(sinks.iter().cloned());
                }

                return Err((
                    cluster_name,
                    compute_objects.unwrap().clone(),
                    storage_objects,
                ));
            }
        }

        Ok(())
    }
}

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

/// Extract dependencies from a set expression.
fn extract_set_expr_dependencies(
    set_expr: &SetExpr<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut HashSet<ObjectId>,
) {
    extract_set_expr_dependencies_with_ctes(
        set_expr,
        default_database,
        default_schema,
        deps,
        &HashSet::new(),
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

/// Extract dependencies from a SELECT statement.
fn extract_select_dependencies(
    select: &Select<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut HashSet<ObjectId>,
) {
    extract_select_dependencies_with_ctes(
        select,
        default_database,
        default_schema,
        deps,
        &HashSet::new(),
    );
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
            extract_expr_dependencies_with_ctes(expr, default_database, default_schema, deps, cte_names);
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
    extract_table_factor_dependencies_with_ctes(
        table_factor,
        default_database,
        default_schema,
        deps,
        &HashSet::new(),
    );
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

/// Extract dependencies from an expression (for subqueries in WHERE, etc).
fn extract_expr_dependencies(
    expr: &Expr<Raw>,
    default_database: &str,
    default_schema: &str,
    deps: &mut HashSet<ObjectId>,
) {
    extract_expr_dependencies_with_ctes(
        expr,
        default_database,
        default_schema,
        deps,
        &HashSet::new(),
    );
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
            extract_expr_dependencies_with_ctes(low, default_database, default_schema, deps, cte_names);
            extract_expr_dependencies_with_ctes(high, default_database, default_schema, deps, cte_names);
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
        use crate::project::raw;
        use crate::project::typed;
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let src_dir = temp_dir.path();

        // Create test structure with separate schemas for tables and views
        let db_path = src_dir.join("test_db");
        let tables_schema_path = db_path.join("tables");
        let views_schema_path = db_path.join("views");
        fs::create_dir_all(&tables_schema_path).unwrap();
        fs::create_dir_all(&views_schema_path).unwrap();

        // Create table in tables schema
        fs::write(
            tables_schema_path.join("table1.sql"),
            "CREATE TABLE table1 (id INT);",
        )
        .unwrap();

        // Create view depending on table in views schema
        fs::write(
            views_schema_path.join("view1.sql"),
            "CREATE VIEW view1 AS SELECT * FROM tables.table1;",
        )
        .unwrap();

        // Create another view depending on view1 in views schema
        fs::write(
            views_schema_path.join("view2.sql"),
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
            "views".to_string(),
            "view1".to_string(),
        );
        filter.insert(view1_id.clone());

        // Get filtered objects
        let filtered = planned_project
            .get_sorted_objects_filtered(&filter)
            .unwrap();

        // Should only contain view1
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].0, view1_id);
    }

    #[test]
    fn test_extract_dependencies_with_mutually_recursive_ctes() {
        // Test basic mutually recursive CTEs that reference each other and external tables
        let sql = r#"
            CREATE MATERIALIZED VIEW mv AS
            WITH MUTUALLY RECURSIVE
              is_even (n int, result bool) AS (
                SELECT 0 as n, TRUE as result
                UNION ALL
                SELECT ni.n, ie_prev.result
                FROM numbers_input ni, is_odd ie_prev
                WHERE ni.n > 0 AND ni.n - 1 = ie_prev.n
              ),
              is_odd (n int, result bool) AS (
                SELECT ni.n, NOT ie.result as result
                FROM numbers_input ni, is_even ie
                WHERE ni.n > 0 AND ni.n - 1 = ie.n
              )
            SELECT n, result AS is_even
            FROM is_even
            ORDER BY n
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateMaterializedView(mv_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateMaterializedView(mv_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // Should only have dependency on numbers_input, not on is_even or is_odd (internal CTEs)
            assert_eq!(deps.len(), 1);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "numbers_input".to_string()
            )));
        } else {
            panic!("Expected CreateMaterializedView statement");
        }
    }

    #[test]
    fn test_extract_dependencies_mutually_recursive_with_subquery() {
        // Test mutually recursive CTEs with subqueries in WHERE clause
        let sql = r#"
            CREATE VIEW v AS
            WITH MUTUALLY RECURSIVE
              cte1 (id int) AS (
                SELECT id FROM table1
                WHERE id IN (SELECT id FROM cte2)
              ),
              cte2 (id int) AS (
                SELECT id FROM table2
                WHERE EXISTS (SELECT 1 FROM cte1)
              )
            SELECT * FROM cte1
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // Should have dependencies on table1 and table2, but not on cte1 or cte2
            assert_eq!(deps.len(), 2);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table1".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table2".to_string()
            )));
        } else {
            panic!("Expected CreateView statement");
        }
    }

    #[test]
    fn test_extract_dependencies_mutually_recursive_with_derived_table() {
        // Test mutually recursive CTEs with derived tables (subqueries in FROM)
        let sql = r#"
            CREATE MATERIALIZED VIEW mv AS
            WITH MUTUALLY RECURSIVE
              cte1 (id int, value text) AS (
                SELECT id, value FROM (
                  SELECT id, value FROM base_table WHERE id > 0
                ) sub
                WHERE id IN (SELECT id FROM cte2)
              ),
              cte2 (id int, value text) AS (
                SELECT id, value FROM (
                  SELECT id, value FROM another_table
                  WHERE value IN (SELECT value FROM cte1)
                )
              )
            SELECT * FROM cte2
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateMaterializedView(mv_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateMaterializedView(mv_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // Should have dependencies on base_table and another_table
            assert_eq!(deps.len(), 2);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "base_table".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "another_table".to_string()
            )));
        } else {
            panic!("Expected CreateMaterializedView statement");
        }
    }

    #[test]
    fn test_extract_dependencies_mutually_recursive_nested_cte_reference() {
        // Test that CTE references inside nested queries don't get added as dependencies
        let sql = r#"
            CREATE VIEW v AS
            WITH MUTUALLY RECURSIVE
              cte_a (id int) AS (
                SELECT id FROM real_table
                WHERE id IN (
                  SELECT id FROM (
                    SELECT id FROM cte_b
                  ) subquery
                )
              ),
              cte_b (id int) AS (
                SELECT id FROM cte_a
              )
            SELECT * FROM cte_b
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // Should only have dependency on real_table, not on cte_a or cte_b
            assert_eq!(deps.len(), 1);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "real_table".to_string()
            )));
        } else {
            panic!("Expected CreateView statement");
        }
    }

    #[test]
    fn test_extract_dependencies_simple_cte_cannot_forward_reference() {
        // Test that Simple CTEs build scope incrementally
        // In this case, cte1 tries to reference cte2 which comes later
        // With our incremental scoping, cte2 will be treated as an external table
        let sql = r#"
            CREATE VIEW v AS
            WITH
              cte1 AS (SELECT * FROM cte2),
              cte2 AS (SELECT * FROM base_table)
            SELECT * FROM cte1
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // With incremental scoping, cte1 doesn't know about cte2 yet
            // So cte2 is treated as an external dependency (along with base_table from cte2's definition)
            assert_eq!(deps.len(), 2);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "cte2".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "base_table".to_string()
            )));
        } else {
            panic!("Expected CreateView statement");
        }
    }

    #[test]
    fn test_extract_dependencies_simple_cte_backward_reference() {
        // Test that Simple CTEs can reference earlier CTEs
        let sql = r#"
            CREATE VIEW v AS
            WITH
              cte1 AS (SELECT * FROM base_table),
              cte2 AS (SELECT * FROM cte1)
            SELECT * FROM cte2
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // cte2 can see cte1, so only base_table is an external dependency
            assert_eq!(deps.len(), 1);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "base_table".to_string()
            )));
        } else {
            panic!("Expected CreateView statement");
        }
    }

    #[test]
    fn test_uncorrelated_subquery_in_where() {
        // Test uncorrelated subquery in WHERE clause
        let sql = r#"
            CREATE VIEW v AS
            SELECT * FROM table1
            WHERE id IN (SELECT id FROM table2 WHERE status = 'active')
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // Should have dependencies on both table1 and table2
            assert_eq!(deps.len(), 2);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table1".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table2".to_string()
            )));
        } else {
            panic!("Expected CreateView statement");
        }
    }

    #[test]
    fn test_correlated_subquery_in_where() {
        // Test correlated subquery in WHERE clause (references outer query)
        let sql = r#"
            CREATE VIEW v AS
            SELECT * FROM table1 t1
            WHERE EXISTS (
                SELECT 1 FROM table2 t2
                WHERE t2.parent_id = t1.id
                AND t2.status = 'active'
            )
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // Should have dependencies on both table1 and table2
            assert_eq!(deps.len(), 2);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table1".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table2".to_string()
            )));
        } else {
            panic!("Expected CreateView statement");
        }
    }

    #[test]
    fn test_subquery_in_select_list() {
        // Test subquery in SELECT list (scalar subquery)
        let sql = r#"
            CREATE VIEW v AS
            SELECT
                id,
                name,
                (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) as order_count
            FROM users
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // Should have dependencies on both users and orders
            assert_eq!(deps.len(), 2);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "users".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "orders".to_string()
            )));
        } else {
            panic!("Expected CreateView statement");
        }
    }

    #[test]
    fn test_nested_uncorrelated_subqueries() {
        // Test nested uncorrelated subqueries
        let sql = r#"
            CREATE VIEW v AS
            SELECT * FROM table1
            WHERE id IN (
                SELECT user_id FROM table2
                WHERE category_id IN (
                    SELECT id FROM table3 WHERE active = true
                )
            )
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // Should have dependencies on table1, table2, and table3
            assert_eq!(deps.len(), 3);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table1".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table2".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table3".to_string()
            )));
        } else {
            panic!("Expected CreateView statement");
        }
    }

    #[test]
    fn test_subquery_with_simple_cte() {
        // Test subquery referencing a Simple CTE (should not be treated as external dependency)
        let sql = r#"
            CREATE VIEW v AS
            WITH cte1 AS (
                SELECT * FROM base_table
            )
            SELECT * FROM table1
            WHERE id IN (SELECT id FROM cte1)
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // Should have dependencies on table1 and base_table, but NOT on cte1
            assert_eq!(deps.len(), 2);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table1".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "base_table".to_string()
            )));
        } else {
            panic!("Expected CreateView statement");
        }
    }

    #[test]
    fn test_correlated_subquery_with_cte() {
        // Test correlated subquery with CTE
        let sql = r#"
            CREATE VIEW v AS
            WITH active_users AS (
                SELECT id, name FROM users WHERE active = true
            )
            SELECT * FROM orders o
            WHERE EXISTS (
                SELECT 1 FROM active_users au
                WHERE au.id = o.user_id
            )
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // Should have dependencies on orders and users, but NOT on active_users
            assert_eq!(deps.len(), 2);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "orders".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "users".to_string()
            )));
        } else {
            panic!("Expected CreateView statement");
        }
    }

    #[test]
    fn test_subquery_in_from_with_cte() {
        // Test subquery in FROM clause (derived table) that references CTE
        let sql = r#"
            CREATE VIEW v AS
            WITH summary AS (
                SELECT category, COUNT(*) as cnt FROM products GROUP BY category
            )
            SELECT * FROM (
                SELECT s.category, s.cnt, c.name
                FROM summary s
                JOIN categories c ON s.category = c.id
            ) derived
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // Should have dependencies on products and categories, but NOT on summary
            assert_eq!(deps.len(), 2);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "products".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "categories".to_string()
            )));
        } else {
            panic!("Expected CreateView statement");
        }
    }

    #[test]
    fn test_multiple_subqueries_mixed_correlation() {
        // Test multiple subqueries with mixed correlation
        // Split into two tests since Materialize may have parser issues with complex WHERE clauses
        let sql1 = r#"
            CREATE VIEW v AS
            SELECT t1.id
            FROM table1 t1
            WHERE t1.id IN (SELECT user_id FROM table2)
        "#;

        let sql2 = r#"
            CREATE VIEW v2 AS
            SELECT t1.id
            FROM table1 t1
            WHERE EXISTS (
                SELECT 1 FROM table3 t3
                WHERE t3.parent_id = t1.id
            )
        "#;

        // Test first query with IN subquery
        let parsed1 = mz_sql_parser::parser::parse_statements(sql1).unwrap();
        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed1[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");
            assert_eq!(deps.len(), 2);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table1".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table2".to_string()
            )));
        }

        // Test second query with EXISTS subquery
        let parsed2 = mz_sql_parser::parser::parse_statements(sql2).unwrap();
        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed2[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            eprintln!("Found {} dependencies:", deps.len());
            for dep in &deps {
                eprintln!("  - {}", dep);
            }

            assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table1".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table3".to_string()
            )));
        }
    }

    #[test]
    fn test_subquery_in_case_expression() {
        // Test subquery in CASE expression
        let sql = r#"
            CREATE VIEW v AS
            SELECT
                id,
                CASE
                    WHEN status = 'pending' THEN (SELECT COUNT(*) FROM pending_queue)
                    WHEN status = 'active' THEN (SELECT COUNT(*) FROM active_queue)
                    ELSE 0
                END as queue_size
            FROM tasks
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // Should have dependencies on tasks, pending_queue, and active_queue
            assert_eq!(deps.len(), 3);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "tasks".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "pending_queue".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "active_queue".to_string()
            )));
        } else {
            panic!("Expected CreateView statement");
        }
    }

    #[test]
    fn test_subquery_with_table_alias() {
        // Test subquery with table alias
        let sql = r#"
            CREATE VIEW v AS
            SELECT t1.id
            FROM table1 t1
            WHERE t1.id IN (SELECT user_id FROM table2)
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // Debug: print what we found
            eprintln!("Found {} dependencies:", deps.len());
            for dep in &deps {
                eprintln!("  - {}", dep);
            }

            // Should have dependencies on table1 and table2
            assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table1".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "table2".to_string()
            )));
        } else {
            panic!("Expected CreateView statement");
        }
    }

    #[test]
    fn test_wmr_with_correlated_subquery() {
        // Test WITH MUTUALLY RECURSIVE with correlated subquery
        let sql = r#"
            CREATE VIEW v AS
            WITH MUTUALLY RECURSIVE
              cte1 (id int, parent_id int) AS (
                SELECT id, parent_id FROM base_table
                WHERE EXISTS (
                    SELECT 1 FROM cte2 c2
                    WHERE c2.id = base_table.parent_id
                )
              ),
              cte2 (id int, parent_id int) AS (
                SELECT id, parent_id FROM another_table
                WHERE id IN (SELECT parent_id FROM cte1)
              )
            SELECT * FROM cte1
        "#;
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

        if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateView(view_stmt.clone());
            let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

            // Should have dependencies on base_table and another_table, but NOT on cte1 or cte2
            assert_eq!(deps.len(), 2);
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "base_table".to_string()
            )));
            assert!(deps.contains(&ObjectId::new(
                "db".to_string(),
                "public".to_string(),
                "another_table".to_string()
            )));
        } else {
            panic!("Expected CreateView statement");
        }
    }

    // Helper function to create a minimal Project for cluster isolation testing
    fn create_test_project_for_cluster_validation() -> Project {
        Project {
            databases: vec![],
            dependency_graph: HashMap::new(),
            external_dependencies: HashSet::new(),
            cluster_dependencies: HashSet::new(),
            tests: vec![],
        }
    }

    #[test]
    fn test_validate_cluster_isolation_no_conflicts() {
        let project = create_test_project_for_cluster_validation();
        let sources_by_cluster = HashMap::new();

        let result = project.validate_cluster_isolation(&sources_by_cluster);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_cluster_isolation_separate_clusters() {
        // Create a project with MV on compute_cluster and sink on storage_cluster
        let mv_sql = "CREATE MATERIALIZED VIEW mv IN CLUSTER compute_cluster AS SELECT 1";
        let sink_sql = "CREATE SINK sink IN CLUSTER storage_cluster FROM mv INTO KAFKA CONNECTION conn (TOPIC 'test')";

        let mv_parsed = mz_sql_parser::parser::parse_statements(mv_sql).unwrap();
        let sink_parsed = mz_sql_parser::parser::parse_statements(sink_sql).unwrap();

        let mv_stmt = if let mz_sql_parser::ast::Statement::CreateMaterializedView(s) = &mv_parsed[0].ast {
            Statement::CreateMaterializedView(s.clone())
        } else {
            panic!("Expected CreateMaterializedView");
        };

        let sink_stmt = if let mz_sql_parser::ast::Statement::CreateSink(s) = &sink_parsed[0].ast {
            Statement::CreateSink(s.clone())
        } else {
            panic!("Expected CreateSink");
        };

        let mv_obj = DatabaseObject {
            id: ObjectId::new("db".to_string(), "schema".to_string(), "mv".to_string()),
            typed_object: typed::DatabaseObject {
                stmt: mv_stmt,
                indexes: vec![],
                grants: vec![],
                comments: vec![],
                tests: vec![],
            },
            dependencies: HashSet::new(),
        };

        let sink_obj = DatabaseObject {
            id: ObjectId::new("db".to_string(), "schema".to_string(), "sink".to_string()),
            typed_object: typed::DatabaseObject {
                stmt: sink_stmt,
                indexes: vec![],
                grants: vec![],
                comments: vec![],
                tests: vec![],
            },
            dependencies: HashSet::new(),
        };

        let project = Project {
            databases: vec![Database {
                name: "db".to_string(),
                schemas: vec![Schema {
                    name: "schema".to_string(),
                    objects: vec![mv_obj, sink_obj],
                    mod_statements: None,
                    schema_type: SchemaType::Storage, // Has sink
                }],
                mod_statements: None,
            }],
            dependency_graph: HashMap::new(),
            external_dependencies: HashSet::new(),
            cluster_dependencies: HashSet::new(),
            tests: vec![],
        };

        // Sources on storage_cluster (different from compute objects)
        let mut sources_by_cluster = HashMap::new();
        sources_by_cluster.insert("storage_cluster".to_string(), vec!["db.schema.source1".to_string()]);

        let result = project.validate_cluster_isolation(&sources_by_cluster);
        assert!(result.is_ok(), "Should succeed when storage and compute are on separate clusters");
    }

    #[test]
    fn test_validate_cluster_isolation_conflict_mv_and_source() {
        // Create a project with MV on shared_cluster
        let mv_sql = "CREATE MATERIALIZED VIEW mv IN CLUSTER shared_cluster AS SELECT 1";
        let mv_parsed = mz_sql_parser::parser::parse_statements(mv_sql).unwrap();

        let mv_stmt = if let mz_sql_parser::ast::Statement::CreateMaterializedView(s) = &mv_parsed[0].ast {
            Statement::CreateMaterializedView(s.clone())
        } else {
            panic!("Expected CreateMaterializedView");
        };

        let mv_obj = DatabaseObject {
            id: ObjectId::new("db".to_string(), "schema".to_string(), "mv".to_string()),
            typed_object: typed::DatabaseObject {
                stmt: mv_stmt,
                indexes: vec![],
                grants: vec![],
                comments: vec![],
                tests: vec![],
            },
            dependencies: HashSet::new(),
        };

        let project = Project {
            databases: vec![Database {
                name: "db".to_string(),
                schemas: vec![Schema {
                    name: "schema".to_string(),
                    objects: vec![mv_obj],
                    mod_statements: None,
                    schema_type: SchemaType::Compute, // Has MV
                }],
                mod_statements: None,
            }],
            dependency_graph: HashMap::new(),
            external_dependencies: HashSet::new(),
            cluster_dependencies: HashSet::new(),
            tests: vec![],
        };

        // Source on the same cluster as MV
        let mut sources_by_cluster = HashMap::new();
        sources_by_cluster.insert("shared_cluster".to_string(), vec!["db.schema.source1".to_string()]);

        let result = project.validate_cluster_isolation(&sources_by_cluster);
        assert!(result.is_err(), "Should fail when MV and source share a cluster");

        if let Err((cluster_name, compute_objects, storage_objects)) = result {
            assert_eq!(cluster_name, "shared_cluster");
            assert_eq!(compute_objects.len(), 1);
            assert!(compute_objects.contains(&"db.schema.mv".to_string()));
            assert_eq!(storage_objects.len(), 1);
            assert!(storage_objects.contains(&"db.schema.source1".to_string()));
        }
    }

    #[test]
    fn test_validate_cluster_isolation_conflict_index_and_sink() {
        // Create a project with a table and an index on shared_cluster, plus a sink on the same cluster
        let table_sql = "CREATE TABLE t (id INT)";
        let sink_sql = "CREATE SINK sink IN CLUSTER shared_cluster FROM t INTO KAFKA CONNECTION conn (TOPIC 'test')";

        let table_parsed = mz_sql_parser::parser::parse_statements(table_sql).unwrap();
        let sink_parsed = mz_sql_parser::parser::parse_statements(sink_sql).unwrap();

        let table_stmt = if let mz_sql_parser::ast::Statement::CreateTable(s) = &table_parsed[0].ast {
            Statement::CreateTable(s.clone())
        } else {
            panic!("Expected CreateTable");
        };

        let sink_stmt = if let mz_sql_parser::ast::Statement::CreateSink(s) = &sink_parsed[0].ast {
            Statement::CreateSink(s.clone())
        } else {
            panic!("Expected CreateSink");
        };

        // Create an index on the table
        let index_sql = "CREATE INDEX idx IN CLUSTER shared_cluster ON t (id)";
        let index_parsed = mz_sql_parser::parser::parse_statements(index_sql).unwrap();
        let index_stmt = if let mz_sql_parser::ast::Statement::CreateIndex(s) = &index_parsed[0].ast {
            s.clone()
        } else {
            panic!("Expected CreateIndex");
        };

        let table_obj = DatabaseObject {
            id: ObjectId::new("db".to_string(), "schema".to_string(), "t".to_string()),
            typed_object: typed::DatabaseObject {
                stmt: table_stmt,
                indexes: vec![index_stmt],
                grants: vec![],
                comments: vec![],
                tests: vec![],
            },
            dependencies: HashSet::new(),
        };

        let sink_obj = DatabaseObject {
            id: ObjectId::new("db".to_string(), "schema".to_string(), "sink".to_string()),
            typed_object: typed::DatabaseObject {
                stmt: sink_stmt,
                indexes: vec![],
                grants: vec![],
                comments: vec![],
                tests: vec![],
            },
            dependencies: HashSet::new(),
        };

        let project = Project {
            databases: vec![Database {
                name: "db".to_string(),
                schemas: vec![Schema {
                    name: "schema".to_string(),
                    objects: vec![table_obj, sink_obj],
                    mod_statements: None,
                    schema_type: SchemaType::Storage, // Has sink and table
                }],
                mod_statements: None,
            }],
            dependency_graph: HashMap::new(),
            external_dependencies: HashSet::new(),
            cluster_dependencies: HashSet::new(),
            tests: vec![],
        };

        // No sources, but sink and index on same cluster
        let sources_by_cluster = HashMap::new();

        let result = project.validate_cluster_isolation(&sources_by_cluster);
        assert!(result.is_err(), "Should fail when index and sink share a cluster");

        if let Err((cluster_name, compute_objects, storage_objects)) = result {
            assert_eq!(cluster_name, "shared_cluster");
            assert_eq!(compute_objects.len(), 1);
            assert_eq!(storage_objects.len(), 1);
            assert!(storage_objects.contains(&"db.schema.sink".to_string()));
        }
    }

    #[test]
    fn test_validate_cluster_isolation_multiple_storage_objects() {
        // Create a project with MV on shared_cluster
        let mv_sql = "CREATE MATERIALIZED VIEW mv IN CLUSTER shared_cluster AS SELECT 1";
        let mv_parsed = mz_sql_parser::parser::parse_statements(mv_sql).unwrap();

        let mv_stmt = if let mz_sql_parser::ast::Statement::CreateMaterializedView(s) = &mv_parsed[0].ast {
            Statement::CreateMaterializedView(s.clone())
        } else {
            panic!("Expected CreateMaterializedView");
        };

        // Create a sink on the same cluster
        let sink_sql = "CREATE SINK sink IN CLUSTER shared_cluster FROM mv INTO KAFKA CONNECTION conn (TOPIC 'test')";
        let sink_parsed = mz_sql_parser::parser::parse_statements(sink_sql).unwrap();

        let sink_stmt = if let mz_sql_parser::ast::Statement::CreateSink(s) = &sink_parsed[0].ast {
            Statement::CreateSink(s.clone())
        } else {
            panic!("Expected CreateSink");
        };

        let mv_obj = DatabaseObject {
            id: ObjectId::new("db".to_string(), "schema".to_string(), "mv".to_string()),
            typed_object: typed::DatabaseObject {
                stmt: mv_stmt,
                indexes: vec![],
                grants: vec![],
                comments: vec![],
                tests: vec![],
            },
            dependencies: HashSet::new(),
        };

        let sink_obj = DatabaseObject {
            id: ObjectId::new("db".to_string(), "schema".to_string(), "sink".to_string()),
            typed_object: typed::DatabaseObject {
                stmt: sink_stmt,
                indexes: vec![],
                grants: vec![],
                comments: vec![],
                tests: vec![],
            },
            dependencies: HashSet::new(),
        };

        let project = Project {
            databases: vec![Database {
                name: "db".to_string(),
                schemas: vec![Schema {
                    name: "schema".to_string(),
                    objects: vec![mv_obj, sink_obj],
                    mod_statements: None,
                    schema_type: SchemaType::Storage, // Has sink
                }],
                mod_statements: None,
            }],
            dependency_graph: HashMap::new(),
            external_dependencies: HashSet::new(),
            cluster_dependencies: HashSet::new(),
            tests: vec![],
        };

        // Multiple sources on the same cluster
        let mut sources_by_cluster = HashMap::new();
        sources_by_cluster.insert(
            "shared_cluster".to_string(),
            vec![
                "db.schema.source1".to_string(),
                "db.schema.source2".to_string(),
            ],
        );

        let result = project.validate_cluster_isolation(&sources_by_cluster);
        assert!(result.is_err(), "Should fail when MV shares cluster with sources and sinks");

        if let Err((cluster_name, _compute_objects, storage_objects)) = result {
            assert_eq!(cluster_name, "shared_cluster");
            // Should have 2 sources + 1 sink = 3 storage objects
            assert_eq!(storage_objects.len(), 3);
        }
    }

    #[test]
    fn test_validate_cluster_isolation_only_compute_objects() {
        // Create a project with only MVs and indexes (no sinks)
        let mv_sql = "CREATE MATERIALIZED VIEW mv IN CLUSTER compute_cluster AS SELECT 1";
        let mv_parsed = mz_sql_parser::parser::parse_statements(mv_sql).unwrap();

        let mv_stmt = if let mz_sql_parser::ast::Statement::CreateMaterializedView(s) = &mv_parsed[0].ast {
            Statement::CreateMaterializedView(s.clone())
        } else {
            panic!("Expected CreateMaterializedView");
        };

        let mv_obj = DatabaseObject {
            id: ObjectId::new("db".to_string(), "schema".to_string(), "mv".to_string()),
            typed_object: typed::DatabaseObject {
                stmt: mv_stmt,
                indexes: vec![],
                grants: vec![],
                comments: vec![],
                tests: vec![],
            },
            dependencies: HashSet::new(),
        };

        let project = Project {
            databases: vec![Database {
                name: "db".to_string(),
                schemas: vec![Schema {
                    name: "schema".to_string(),
                    objects: vec![mv_obj],
                    mod_statements: None,
                    schema_type: SchemaType::Compute, // Has MV
                }],
                mod_statements: None,
            }],
            dependency_graph: HashMap::new(),
            external_dependencies: HashSet::new(),
            cluster_dependencies: HashSet::new(),
            tests: vec![],
        };

        // No sources on any cluster
        let sources_by_cluster = HashMap::new();

        let result = project.validate_cluster_isolation(&sources_by_cluster);
        assert!(result.is_ok(), "Should succeed when cluster only has compute objects");
    }

    #[test]
    fn test_validate_cluster_isolation_only_storage_objects() {
        // Create a project with only a sink (no MVs or indexes)
        let sink_sql = "CREATE SINK sink IN CLUSTER storage_cluster FROM t INTO KAFKA CONNECTION conn (TOPIC 'test')";
        let sink_parsed = mz_sql_parser::parser::parse_statements(sink_sql).unwrap();

        let sink_stmt = if let mz_sql_parser::ast::Statement::CreateSink(s) = &sink_parsed[0].ast {
            Statement::CreateSink(s.clone())
        } else {
            panic!("Expected CreateSink");
        };

        let sink_obj = DatabaseObject {
            id: ObjectId::new("db".to_string(), "schema".to_string(), "sink".to_string()),
            typed_object: typed::DatabaseObject {
                stmt: sink_stmt,
                indexes: vec![],
                grants: vec![],
                comments: vec![],
                tests: vec![],
            },
            dependencies: HashSet::new(),
        };

        let project = Project {
            databases: vec![Database {
                name: "db".to_string(),
                schemas: vec![Schema {
                    name: "schema".to_string(),
                    objects: vec![sink_obj],
                    mod_statements: None,
                    schema_type: SchemaType::Storage, // Has sink
                }],
                mod_statements: None,
            }],
            dependency_graph: HashMap::new(),
            external_dependencies: HashSet::new(),
            cluster_dependencies: HashSet::new(),
            tests: vec![],
        };

        // Sources on the same cluster
        let mut sources_by_cluster = HashMap::new();
        sources_by_cluster.insert("storage_cluster".to_string(), vec!["db.schema.source1".to_string()]);

        let result = project.validate_cluster_isolation(&sources_by_cluster);
        assert!(result.is_ok(), "Should succeed when cluster only has storage objects (sources + sinks)");
    }
}
