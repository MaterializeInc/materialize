//! Change detection for incremental deployment.
//!
//! This module compares deployment snapshots to determine which objects need redeployment.

use super::ast::Cluster;
use super::deployment_snapshot::DeploymentSnapshot;
use super::mir::{self, Project};
use crate::project::object_id::ObjectId;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

/// Represents the set of changes between two project states.
///
/// Used to determine which objects need redeployment based on file changes.
#[derive(Debug, Clone)]
pub struct ChangeSet {
    /// Files that have changed (added, modified, or deleted)
    pub changed_files: HashSet<String>,

    /// Objects that exist in changed files
    pub changed_objects: HashSet<ObjectId>,

    /// Schemas where ANY file changed (entire schema is dirty)
    pub dirty_schemas: HashSet<(String, String)>, // (database, schema)

    /// Clusters used by objects in dirty schemas
    pub dirty_clusters: HashSet<Cluster>,

    /// All objects that need redeployment (includes transitive dependencies)
    pub objects_to_deploy: HashSet<ObjectId>,
}

impl ChangeSet {
    /// Create a ChangeSet by comparing old and new deployment snapshots.
    ///
    /// This method compares two deployment snapshots (object-based hashing)
    /// to determine which objects have changed.
    ///
    /// # Arguments
    /// * `old_snapshot` - Previous deployment snapshot
    /// * `new_snapshot` - Current deployment snapshot
    /// * `project` - MIR project with dependency information
    ///
    /// # Returns
    /// A ChangeSet identifying all objects requiring redeployment
    pub fn from_deployment_snapshot_comparison(
        old_snapshot: &DeploymentSnapshot,
        new_snapshot: &DeploymentSnapshot,
        project: &Project,
    ) -> Self {
        // Step 1: Find changed objects by comparing hashes
        let mut changed_objects = HashSet::new();

        // Objects with different hashes or newly added
        for (object_id, new_hash) in &new_snapshot.objects {
            if old_snapshot.objects.get(object_id) != Some(new_hash) {
                changed_objects.insert(object_id.clone());
            }
        }

        // Deleted objects
        for object_id in old_snapshot.objects.keys() {
            if !new_snapshot.objects.contains_key(object_id) {
                changed_objects.insert(object_id.clone());
            }
        }

        // Step 2: Map changed objects to schemas
        let dirty_schemas = Self::map_objects_to_schemas(&changed_objects, project);

        // Step 3: Find all objects in dirty schemas
        let mut objects_in_dirty_schemas = HashSet::new();
        for db in &project.databases {
            for schema in &db.schemas {
                if dirty_schemas.contains(&(db.name.clone(), schema.name.clone())) {
                    for obj in &schema.objects {
                        objects_in_dirty_schemas.insert(obj.id.clone());
                    }
                }
            }
        }

        // Step 4: Extract clusters used by dirty objects
        let dirty_clusters = Self::extract_dirty_clusters(&objects_in_dirty_schemas, project);

        // Step 5: Find all objects in dirty clusters
        let objects_in_dirty_clusters = Self::find_objects_in_clusters(&dirty_clusters, project);

        // Step 6: Compute transitive closure (downstream dependencies)
        let mut objects_to_deploy = HashSet::new();
        objects_to_deploy.extend(objects_in_dirty_schemas.iter().cloned());
        objects_to_deploy.extend(objects_in_dirty_clusters.iter().cloned());

        let reverse_deps = project.build_reverse_dependency_graph();
        Self::add_downstream_dependencies(&mut objects_to_deploy, &reverse_deps);

        ChangeSet {
            changed_files: HashSet::new(),
            changed_objects,
            dirty_schemas,
            dirty_clusters,
            objects_to_deploy,
        }
    }

    /// Map changed objects to schemas.
    ///
    /// Returns the set of (database, schema) tuples that contain changed objects.
    fn map_objects_to_schemas(
        changed_objects: &HashSet<ObjectId>,
        project: &Project,
    ) -> HashSet<(String, String)> {
        let mut dirty_schemas = HashSet::new();

        for obj_id in changed_objects {
            // Find the object in the project to verify it exists
            let mut found = false;
            for db in &project.databases {
                if db.name == obj_id.database {
                    for schema in &db.schemas {
                        if schema.name == obj_id.schema {
                            // Mark this schema as dirty
                            dirty_schemas.insert((obj_id.database.clone(), obj_id.schema.clone()));
                            found = true;
                            break;
                        }
                    }
                }
                if found {
                    break;
                }
            }
        }

        dirty_schemas
    }

    /// Extract clusters used by dirty objects.
    fn extract_dirty_clusters(
        dirty_objects: &HashSet<ObjectId>,
        project: &Project,
    ) -> HashSet<Cluster> {
        let mut clusters = HashSet::new();

        for db in &project.databases {
            for schema in &db.schemas {
                for obj in &schema.objects {
                    if dirty_objects.contains(&obj.id) {
                        // Extract clusters from this object
                        let (_, obj_clusters) =
                            mir::extract_dependencies(&obj.hir_object.stmt, &db.name, &schema.name);
                        clusters.extend(obj_clusters);
                    }
                }
            }
        }

        clusters
    }

    /// Find all objects that use any of the dirty clusters.
    fn find_objects_in_clusters(
        dirty_clusters: &HashSet<Cluster>,
        project: &Project,
    ) -> HashSet<ObjectId> {
        if dirty_clusters.is_empty() {
            return HashSet::new();
        }

        let mut objects = HashSet::new();

        for db in &project.databases {
            for schema in &db.schemas {
                for obj in &schema.objects {
                    let (_, obj_clusters) =
                        mir::extract_dependencies(&obj.hir_object.stmt, &db.name, &schema.name);

                    // If this object uses any dirty cluster, mark it
                    if obj_clusters.iter().any(|c| dirty_clusters.contains(c)) {
                        objects.insert(obj.id.clone());
                    }
                }
            }
        }

        objects
    }

    /// Add all downstream dependencies (transitive closure).
    ///
    /// If object A depends on dirty object B, then A must also be deployed.
    fn add_downstream_dependencies(
        objects: &mut HashSet<ObjectId>,
        reverse_deps: &HashMap<ObjectId, HashSet<ObjectId>>,
    ) {
        let mut queue: Vec<ObjectId> = objects.iter().cloned().collect();

        while let Some(obj_id) = queue.pop() {
            if let Some(dependents) = reverse_deps.get(&obj_id) {
                for dependent in dependents {
                    if objects.insert(dependent.clone()) {
                        // Newly added, need to check its dependents
                        queue.push(dependent.clone());
                    }
                }
            }
        }
    }

    /// Check if any changes were detected.
    pub fn is_empty(&self) -> bool {
        self.objects_to_deploy.is_empty()
    }

    /// Get the number of objects that need deployment.
    pub fn deployment_count(&self) -> usize {
        self.objects_to_deploy.len()
    }
}

impl Display for ChangeSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Incremental deployment: {} objects need redeployment",
            self.deployment_count()
        )?;

        if !self.changed_objects.is_empty() {
            writeln!(f, "Changed objects:")?;
            for obj in &self.changed_objects {
                writeln!(f, "  - {}.{}.{}", obj.database, obj.schema, obj.object)?;
            }
        }

        if !self.dirty_schemas.is_empty() {
            writeln!(f, "Dirty schemas:")?;
            for (db, schema) in &self.dirty_schemas {
                writeln!(f, "  - {}.{}", db, schema)?;
            }
        }

        if !self.dirty_clusters.is_empty() {
            writeln!(f, "Dirty clusters:")?;
            for cluster in &self.dirty_clusters {
                writeln!(f, "  - {}", cluster.name)?;
            }
        }

        if !self.objects_to_deploy.is_empty() {
            writeln!(f, "Objects to deploy:")?;
            for obj in &self.objects_to_deploy {
                writeln!(f, "  - {}.{}.{}", obj.database, obj.schema, obj.object)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_parse_object_file_path() {
        let path = "materialize/public/users.sql";
        let parts: Vec<&str> = path.split('/').collect();

        match parts.as_slice() {
            [db, schema, file] if file.ends_with(".sql") => {
                assert_eq!(*db, "materialize");
                assert_eq!(*schema, "public");
                assert_eq!(file.strip_suffix(".sql").unwrap(), "users");
            }
            _ => panic!("Path didn't match expected pattern"),
        }
    }

    #[test]
    fn test_parse_schema_mod_file_path() {
        let path = "materialize/public.sql";
        let parts: Vec<&str> = path.split('/').collect();

        match parts.as_slice() {
            [db, schema_file] if schema_file.ends_with(".sql") => {
                assert_eq!(*db, "materialize");
                assert_eq!(schema_file.strip_suffix(".sql").unwrap(), "public");
            }
            _ => panic!("Path didn't match expected pattern"),
        }
    }

    #[test]
    fn test_parse_database_mod_file_path() {
        let path = "materialize.sql";
        let parts: Vec<&str> = path.split('/').collect();

        match parts.as_slice() {
            [db_file] if db_file.ends_with(".sql") => {
                assert_eq!(db_file.strip_suffix(".sql").unwrap(), "materialize");
            }
            _ => panic!("Path didn't match expected pattern"),
        }
    }
}
