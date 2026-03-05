//! Project implementation methods for querying and traversal.

use super::super::ast::Statement;
use super::super::error::DependencyError;
use super::super::typed;
use super::types::{DatabaseObject, ModStatement, Project};
use crate::project::object_id::ObjectId;
use std::collections::{BTreeMap, BTreeSet};

impl Project {
    /// Get topologically sorted objects for deployment.
    ///
    /// Returns objects in an order where dependencies come before dependents.
    /// External dependencies are excluded from the sort as they are not deployable.
    pub fn topological_sort(&self) -> Result<Vec<ObjectId>, DependencyError> {
        let mut sorted = Vec::new();
        let mut visited = BTreeSet::new();
        let mut in_progress = BTreeSet::new();

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
            .filter(|object| {
                matches!(
                    object.typed_object.stmt,
                    Statement::CreateTable(_) | Statement::CreateTableFromSource(_)
                )
            })
            .map(|object| object.id.clone())
    }

    fn visit(
        &self,
        object_id: &ObjectId,
        visited: &mut BTreeSet<ObjectId>,
        in_progress: &mut BTreeSet<ObjectId>,
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
                        // Skip SET api — it's a directive for mz-deploy,
                        // not SQL to send to Materialize.
                        if matches!(stmt, mz_sql_parser::ast::Statement::SetVariable(s) if s.variable.as_str().eq_ignore_ascii_case("api"))
                        {
                            continue;
                        }
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
    pub fn build_reverse_dependency_graph(&self) -> BTreeMap<ObjectId, BTreeSet<ObjectId>> {
        let mut reverse: BTreeMap<ObjectId, BTreeSet<ObjectId>> = BTreeMap::new();

        for (obj_id, deps) in &self.dependency_graph {
            for dep in deps {
                reverse
                    .entry(dep.clone())
                    .or_default()
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
        filter: &BTreeSet<ObjectId>,
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
    pub fn validate_cluster_isolation(
        &self,
        sources_by_cluster: &BTreeMap<String, Vec<String>>,
    ) -> Result<(), (String, Vec<String>, Vec<String>)> {
        super::validation::validate_cluster_isolation(self, sources_by_cluster)
    }
}
