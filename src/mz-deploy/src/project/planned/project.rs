//! Project implementation methods for querying and traversal.

use super::super::ast::Statement;
use super::super::error::DependencyError;
use super::super::typed;
use super::types::{DatabaseObject, ModStatement, Project};
use crate::project::object_id::ObjectId;
use std::collections::{HashMap, HashSet};

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
        let mut reverse: HashMap<ObjectId, HashSet<ObjectId>> = HashMap::new();

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
                                .or_default()
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
                                .or_default()
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
                                .or_default()
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
