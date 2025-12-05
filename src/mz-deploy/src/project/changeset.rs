//! Change detection for incremental deployment.
//!
//! This module implements a Dirty Propagation Algorithm to determine
//! which database objects, schemas, and clusters need redeployment after changes.
//!
//! ## Algorithm Overview
//!
//! The algorithm computes three result sets via fixed-point iteration:
//! - `DirtyStmt(object)` - All objects that must be reprocessed
//! - `DirtyCluster(cluster)` - All clusters that must be refreshed
//! - `DirtySchema(database, schema)` - All schemas containing dirty objects
//!
//! ## Propagation Rules
//!
//! ### Rule Category 1 — Statement Dirtiness
//! ```datalog
//! DirtyStmt(O) :- ChangedStmt(O)                             # Changed objects are dirty
//! DirtyStmt(O) :- StmtUsesCluster(O, C), DirtyCluster(C)     # Objects on dirty statement clusters are dirty
//! DirtyStmt(O) :- DependsOn(O, P), DirtyStmt(P)              # Downstream dependents are dirty
//! DirtyStmt(O) :- DirtySchema(Db, Sch), ObjectInSchema(O, Db, Sch) # Objects in dirty schemas are dirty
//! ```
//!
//! **Key Insight:** Index clusters do NOT cause objects to be marked dirty. Indexes are physical
//! optimizations that can be managed independently without redeploying the object's statement.
//! If object A's index uses a dirty cluster, object A is NOT marked for redeployment.
//!
//! ### Rule Category 2 — Cluster Dirtiness
//! ```datalog
//! DirtyCluster(C) :- ChangedStmt(O), StmtUsesCluster(O, C)   # Clusters of changed statements are dirty
//! DirtyCluster(C) :- ChangedStmt(O), IndexUsesCluster(O, _, C) # Clusters of changed indexes are dirty
//! ```
//!
//! **Note:** Clusters are only marked dirty when the STATEMENT itself changes,
//! not when the object is dirty for other reasons (dependencies, schema propagation, etc.).
//!
//! ### Rule Category 3 — Schema Dirtiness
//! ```datalog
//! DirtySchema(Db, Sch) :- DirtyStmt(O), ObjectInSchema(O, Db, Sch)  # Dirty objects make their schemas dirty
//! ```
//!
//! **Key Property:** All dirty objects contribute to schema dirtiness, which triggers
//! schema-level atomic redeployment.

use super::ast::Cluster;
use super::deployment_snapshot::DeploymentSnapshot;
use super::planned::{self, Project};
use crate::project::object_id::ObjectId;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};

/// Represents the set of changes between two project states.
///
/// Used to determine which objects need redeployment based on snapshot comparison.
#[derive(Debug, Clone)]
pub struct ChangeSet {
    /// Objects that exist in changed files
    pub changed_objects: BTreeSet<ObjectId>,

    /// Schemas where ANY file changed (entire schema is dirty)
    pub dirty_schemas: BTreeSet<(String, String)>,

    /// Clusters used by objects in dirty schemas
    pub dirty_clusters: BTreeSet<Cluster>,

    /// All objects that need redeployment (includes transitive dependencies)
    pub objects_to_deploy: BTreeSet<ObjectId>,
}

impl ChangeSet {
    /// Create a ChangeSet by comparing old and new deployment snapshots using Datalog.
    ///
    /// This method uses Datalog fixed-point computation to determine the transitive
    /// closure of all objects, clusters, and schemas affected by changes.
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
        let changed_objects = find_changed_objects(old_snapshot, new_snapshot);

        // Step 2: Extract base facts from project
        let base_facts = extract_base_facts(project);

        // Step 3: Run Datalog fixed-point computation
        let (dirty_stmts, dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_objects, &base_facts);

        ChangeSet {
            changed_objects: changed_objects.into_iter().collect(),
            dirty_schemas: dirty_schemas.into_iter().collect(),
            dirty_clusters: dirty_clusters.into_iter().collect(),
            objects_to_deploy: dirty_stmts.into_iter().collect(),
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

//
// BASE FACT EXTRACTION
//

/// Base facts extracted from the project for Datalog computation.
#[derive(Debug)]
struct BaseFacts {
    /// ObjectInSchema(object, database, schema)
    object_in_schema: Vec<(ObjectId, String, String)>,

    /// DependsOn(child, parent) - child depends on parent
    depends_on: Vec<(ObjectId, ObjectId)>,

    /// StmtUsesCluster(object, cluster_name)
    stmt_uses_cluster: Vec<(ObjectId, String)>,

    /// IndexUsesCluster(object, index_name, cluster_name)
    index_uses_cluster: Vec<(ObjectId, String, String)>,
}

/// Find changed objects by comparing snapshot hashes.
fn find_changed_objects(
    old_snapshot: &DeploymentSnapshot,
    new_snapshot: &DeploymentSnapshot,
) -> BTreeSet<ObjectId> {
    let mut changed = BTreeSet::new();

    // Objects with different hashes or newly added
    for (object_id, new_hash) in &new_snapshot.objects {
        if old_snapshot.objects.get(object_id) != Some(new_hash) {
            changed.insert(object_id.clone());
        }
    }

    // Deleted objects
    for object_id in old_snapshot.objects.keys() {
        if !new_snapshot.objects.contains_key(object_id) {
            changed.insert(object_id.clone());
        }
    }

    changed
}

/// Extract all base facts from the project for Datalog computation.
fn extract_base_facts(project: &Project) -> BaseFacts {
    let mut object_in_schema = Vec::new();
    let mut depends_on = Vec::new();
    let mut stmt_uses_cluster = Vec::new();
    let mut index_uses_cluster = Vec::new();

    // Extract facts from each object in the project
    for db in &project.databases {
        for schema in &db.schemas {
            for obj in &schema.objects {
                let obj_id = obj.id.clone();

                // ObjectInSchema fact
                object_in_schema.push((obj_id.clone(), db.name.clone(), schema.name.clone()));

                // DependsOn facts from dependency graph
                if let Some(deps) = project.dependency_graph.get(&obj_id) {
                    for parent in deps {
                        depends_on.push((obj_id.clone(), parent.clone()));
                    }
                }

                // Extract cluster usage from statement
                let (_, clusters) =
                    planned::extract_dependencies(&obj.typed_object.stmt, &db.name, &schema.name);

                // StmtUsesCluster facts
                for cluster in clusters {
                    stmt_uses_cluster.push((obj_id.clone(), cluster.name.clone()));
                }

                // IndexUsesCluster facts - extract from indexes
                for index in &obj.typed_object.indexes {
                    // Extract cluster directly from CreateIndexStatement
                    if let Some(cluster_name) = &index.in_cluster {
                        let index_name = index
                            .name
                            .as_ref()
                            .map(|n| n.to_string())
                            .unwrap_or_else(|| "unnamed_index".to_string());

                        // Convert cluster name to string
                        let cluster_str = cluster_name.to_string();

                        index_uses_cluster.push((obj_id.clone(), index_name, cluster_str));
                    }
                }
            }
        }
    }

    BaseFacts {
        object_in_schema,
        depends_on,
        stmt_uses_cluster,
        index_uses_cluster,
    }
}

/// Compute dirty objects, clusters, and schemas
fn compute_dirty_datalog(
    changed_stmts: &BTreeSet<ObjectId>,
    base_facts: &BaseFacts,
) -> (
    BTreeSet<ObjectId>,
    BTreeSet<Cluster>,
    BTreeSet<(String, String)>,
) {
    // Build indexes for efficient lookup
    let stmt_cluster_index = build_stmt_cluster_index(&base_facts.stmt_uses_cluster);
    let index_cluster_index = build_index_cluster_index(&base_facts.index_uses_cluster);
    let reverse_deps = build_reverse_deps(&base_facts.depends_on);
    let object_schema_map = build_object_schema_map(&base_facts.object_in_schema);

    // Note: index_cluster_index is still used for marking clusters dirty when statements change (Rule 2),
    // but NOT for marking objects dirty

    // Initialize result sets
    let mut dirty_stmts: BTreeSet<ObjectId> = changed_stmts.clone();
    let mut dirty_clusters: BTreeSet<String> = BTreeSet::new();
    let mut dirty_schemas: BTreeSet<(String, String)> = BTreeSet::new();

    // Fixed-point iteration
    loop {
        let prev_stmts_size = dirty_stmts.len();
        let prev_clusters_size = dirty_clusters.len();
        let prev_schemas_size = dirty_schemas.len();

        // Rule 1: DirtyCluster(C) :- ChangedStmt(O), StmtUsesCluster(O, C)
        // Only mark statement clusters dirty when the STATEMENT itself changed,
        // not when the object is dirty for other reasons (index clusters, schema, etc.)
        for obj in changed_stmts {
            if let Some(clusters) = stmt_cluster_index.get(obj) {
                dirty_clusters.extend(clusters.iter().cloned());
            }
        }

        // Rule 2: DirtyCluster(C) :- ChangedStmt(O), IndexUsesCluster(O, _, C)
        // Only mark index clusters dirty when the STATEMENT itself changed,
        // not when the object is dirty for other reasons (schema, dependencies, etc.)
        for obj in changed_stmts {
            if let Some(clusters) = index_cluster_index.get(obj) {
                dirty_clusters.extend(clusters.iter().cloned());
            }
        }

        // Rule 3: DirtyStmt(O) :- StmtUsesCluster(O, C), DirtyCluster(C)
        // Objects using dirty statement clusters become dirty
        for (obj, clusters) in &stmt_cluster_index {
            if clusters.iter().any(|c| dirty_clusters.contains(c)) {
                dirty_stmts.insert(obj.clone());
            }
        }

        // Rule 4: DirtyStmt(O) :- DependsOn(O, P), DirtyStmt(P)
        // Dependencies propagate dirtiness
        let mut new_dirty = Vec::new();
        for dirty_obj in &dirty_stmts.clone() {
            if let Some(dependents) = reverse_deps.get(dirty_obj) {
                for dependent in dependents {
                    if !dirty_stmts.contains(dependent) {
                        new_dirty.push(dependent.clone());
                    }
                }
            }
        }
        dirty_stmts.extend(new_dirty);

        // Rule 5: DirtySchema(Db, Sch) :- DirtyStmt(O), ObjectInSchema(O, Db, Sch)
        // All dirty objects make their schemas dirty
        for obj in &dirty_stmts {
            if let Some((db, sch)) = object_schema_map.get(obj) {
                dirty_schemas.insert((db.clone(), sch.clone()));
            }
        }

        // Rule 6: DirtyStmt(O) :- DirtySchema(Db, Sch), ObjectInSchema(O, Db, Sch)
        // All objects in dirty schemas become dirty
        for (obj, (db, sch)) in &object_schema_map {
            if dirty_schemas.contains(&(db.clone(), sch.clone())) {
                dirty_stmts.insert(obj.clone());
            }
        }

        // Check if any set grew (fixed point reached when no changes)
        if dirty_stmts.len() == prev_stmts_size
            && dirty_clusters.len() == prev_clusters_size
            && dirty_schemas.len() == prev_schemas_size
        {
            break;
        }
    }

    // Convert cluster names to Cluster structs
    let dirty_cluster_structs = dirty_clusters.into_iter().map(Cluster::new).collect();

    (dirty_stmts, dirty_cluster_structs, dirty_schemas)
}

fn build_stmt_cluster_index(facts: &[(ObjectId, String)]) -> BTreeMap<ObjectId, Vec<String>> {
    let mut index: BTreeMap<ObjectId, Vec<String>> = BTreeMap::new();
    for (obj, cluster) in facts {
        index.entry(obj.clone()).or_default().push(cluster.clone());
    }
    index
}

fn build_index_cluster_index(
    facts: &[(ObjectId, String, String)],
) -> BTreeMap<ObjectId, Vec<String>> {
    let mut index: BTreeMap<ObjectId, Vec<String>> = BTreeMap::new();
    for (obj, _index_name, cluster) in facts {
        index.entry(obj.clone()).or_default().push(cluster.clone());
    }
    index
}

fn build_reverse_deps(facts: &[(ObjectId, ObjectId)]) -> BTreeMap<ObjectId, Vec<ObjectId>> {
    let mut index: BTreeMap<ObjectId, Vec<ObjectId>> = BTreeMap::new();
    for (child, parent) in facts {
        index.entry(parent.clone()).or_default().push(child.clone());
    }
    index
}

fn build_object_schema_map(
    facts: &[(ObjectId, String, String)],
) -> BTreeMap<ObjectId, (String, String)> {
    facts
        .iter()
        .map(|(obj, db, sch)| (obj.clone(), (db.clone(), sch.clone())))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_schema_propagation_all_objects_in_dirty_schema_are_dirty() {
        // Test that when one object in a schema becomes dirty,
        // ALL objects in that schema become dirty (schema-level atomicity)

        // Create base facts for a schema with 3 objects
        let obj1 = ObjectId::new("db".to_string(), "schema".to_string(), "table1".to_string());
        let obj2 = ObjectId::new("db".to_string(), "schema".to_string(), "table2".to_string());
        let obj3 = ObjectId::new("db".to_string(), "schema".to_string(), "view1".to_string());

        let base_facts = BaseFacts {
            object_in_schema: vec![
                (obj1.clone(), "db".to_string(), "schema".to_string()),
                (obj2.clone(), "db".to_string(), "schema".to_string()),
                (obj3.clone(), "db".to_string(), "schema".to_string()),
            ],
            depends_on: vec![],
            stmt_uses_cluster: vec![],
            index_uses_cluster: vec![],
        };

        // Only obj1 is changed
        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(obj1.clone());

        // Run Datalog computation
        let (dirty_stmts, _dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts);

        // Verify schema is dirty
        assert!(
            dirty_schemas.contains(&("db".to_string(), "schema".to_string())),
            "Schema should be marked dirty"
        );

        // CRITICAL: All objects in the dirty schema should be dirty
        assert!(
            dirty_stmts.contains(&obj1),
            "obj1 (changed) should be dirty"
        );
        assert!(
            dirty_stmts.contains(&obj2),
            "obj2 (same schema as changed obj1) should be dirty"
        );
        assert!(
            dirty_stmts.contains(&obj3),
            "obj3 (same schema as changed obj1) should be dirty"
        );

        println!("Dirty objects: {:?}", dirty_stmts);
        println!("Dirty schemas: {:?}", dirty_schemas);
    }

    #[test]
    fn test_index_cluster_does_not_dirty_parent_object_cluster() {
        // Critical test: If an index uses a dirty cluster, the index should be redeployed,
        // but the parent object and its cluster should NOT be marked dirty.
        //
        // Scenario:
        // - winning_bids MV on "staging" cluster
        // - Index on winning_bids using "quickstart" cluster
        // - some_other_obj on "quickstart" cluster changes
        //
        // Expected:
        // - quickstart cluster becomes dirty ✓
        // - Index needs redeployment ✓
        // - winning_bids needs redeployment (to deploy its index) ✓
        // - BUT staging cluster should NOT be dirty ✗ (current bug)

        let mv = ObjectId::new(
            "db".to_string(),
            "schema".to_string(),
            "winning_bids".to_string(),
        );
        let other = ObjectId::new(
            "db".to_string(),
            "schema".to_string(),
            "other_obj".to_string(),
        );

        let base_facts = BaseFacts {
            object_in_schema: vec![
                (mv.clone(), "db".to_string(), "schema".to_string()),
                (other.clone(), "db".to_string(), "schema".to_string()),
            ],
            depends_on: vec![],
            stmt_uses_cluster: vec![
                (mv.clone(), "staging".to_string()),
                (other.clone(), "quickstart".to_string()),
            ],
            index_uses_cluster: vec![(
                mv.clone(),
                "idx_item".to_string(),
                "quickstart".to_string(),
            )],
        };

        // Only other_obj is changed
        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(other.clone());

        // Run Datalog computation
        let (dirty_stmts, dirty_clusters, _dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts);

        println!("Dirty stmts: {:?}", dirty_stmts);
        println!("Dirty clusters: {:?}", dirty_clusters);

        // Verify quickstart cluster is dirty
        assert!(
            dirty_clusters.iter().any(|c| c.name == "quickstart"),
            "quickstart cluster should be dirty because other_obj changed"
        );

        // Verify winning_bids needs redeployment (because its index uses dirty cluster)
        assert!(
            dirty_stmts.contains(&mv),
            "winning_bids should be redeployed (its index uses dirty quickstart cluster)"
        );

        // CRITICAL: staging cluster should NOT be dirty
        // The MV's statement uses staging, but the MV is only dirty because of its index,
        // not because its statement changed. Therefore staging should not be marked dirty.
        assert!(
            !dirty_clusters.iter().any(|c| c.name == "staging"),
            "staging cluster should NOT be dirty - winning_bids is only dirty due to its index, not its statement"
        );
    }

    #[test]
    fn test_index_cluster_does_not_dirty_schema() {
        // Scenario:
        // - table1 and table2 in the same schema
        // - table1 has index on cluster "index_cluster"
        // - some_other_obj uses "index_cluster" and changes
        //
        // Expected (NEW BEHAVIOR):
        // - index_cluster becomes dirty ✓
        // - table1 should NOT be dirty (indexes don't cause redeployment) ✓
        // - schema should NOT be dirty ✓
        // - table2 should NOT be dirty ✓
        //
        // This ensures that objects are only redeployed when their statement changes,
        // not when their index clusters become dirty.

        let table1 = ObjectId::new("db".to_string(), "schema".to_string(), "table1".to_string());
        let table2 = ObjectId::new("db".to_string(), "schema".to_string(), "table2".to_string());
        let other = ObjectId::new(
            "db".to_string(),
            "other_schema".to_string(),
            "other_obj".to_string(),
        );

        let base_facts = BaseFacts {
            object_in_schema: vec![
                (table1.clone(), "db".to_string(), "schema".to_string()),
                (table2.clone(), "db".to_string(), "schema".to_string()),
                (other.clone(), "db".to_string(), "other_schema".to_string()),
            ],
            depends_on: vec![],
            stmt_uses_cluster: vec![(other.clone(), "index_cluster".to_string())],
            index_uses_cluster: vec![(
                table1.clone(),
                "idx1".to_string(),
                "index_cluster".to_string(),
            )],
        };

        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(other.clone());

        let (dirty_stmts, dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts);

        // index_cluster should be dirty
        assert!(dirty_clusters.iter().any(|c| c.name == "index_cluster"));

        // table1 should NOT be dirty (indexes don't cause object redeployment)
        assert!(
            !dirty_stmts.contains(&table1),
            "table1 should NOT be dirty - indexes don't cause redeployment"
        );

        // Schema should NOT be dirty
        assert!(
            !dirty_schemas.contains(&("db".to_string(), "schema".to_string())),
            "schema should NOT be dirty"
        );

        // And table2 should NOT be dirty
        assert!(!dirty_stmts.contains(&table2), "table2 should NOT be dirty");
    }

    #[test]
    fn test_schema_propagation_does_not_dirty_index_clusters() {
        // Scenario from real deployment:
        // - flip_activities and flippers in materialize.public schema
        // - flip_activities has index on "quickstart" cluster
        // - winning_bids in materialize.internal schema has index on "quickstart"
        // - When flippers changes:
        //   - materialize.public schema becomes dirty
        //   - flip_activities becomes dirty (schema propagation)
        //   - BUT quickstart cluster should NOT become dirty
        //   - winning_bids should NOT be redeployed

        let flippers = ObjectId::new(
            "materialize".to_string(),
            "public".to_string(),
            "flippers".to_string(),
        );
        let flip_activities = ObjectId::new(
            "materialize".to_string(),
            "public".to_string(),
            "flip_activities".to_string(),
        );
        let winning_bids = ObjectId::new(
            "materialize".to_string(),
            "internal".to_string(),
            "winning_bids".to_string(),
        );

        let base_facts = BaseFacts {
            object_in_schema: vec![
                (
                    flippers.clone(),
                    "materialize".to_string(),
                    "public".to_string(),
                ),
                (
                    flip_activities.clone(),
                    "materialize".to_string(),
                    "public".to_string(),
                ),
                (
                    winning_bids.clone(),
                    "materialize".to_string(),
                    "internal".to_string(),
                ),
            ],
            depends_on: vec![],
            stmt_uses_cluster: vec![],
            index_uses_cluster: vec![
                (
                    flip_activities.clone(),
                    "idx_flipper".to_string(),
                    "quickstart".to_string(),
                ),
                (
                    winning_bids.clone(),
                    "idx_item".to_string(),
                    "quickstart".to_string(),
                ),
            ],
        };

        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(flippers.clone());

        let (dirty_stmts, dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts);

        // materialize.public schema should be dirty
        assert!(dirty_schemas.contains(&("materialize".to_string(), "public".to_string())));

        // flip_activities should be dirty due to schema propagation
        assert!(
            dirty_stmts.contains(&flip_activities),
            "flip_activities should be dirty due to schema propagation"
        );

        // CRITICAL: quickstart cluster should NOT be dirty
        // flip_activities is dirty due to schema propagation, not because its statement changed
        assert!(
            !dirty_clusters.iter().any(|c| c.name == "quickstart"),
            "quickstart cluster should NOT be dirty - flip_activities is dirty due to schema propagation, not statement change"
        );

        // winning_bids should NOT be dirty
        assert!(
            !dirty_stmts.contains(&winning_bids),
            "winning_bids should NOT be dirty - quickstart cluster is not dirty"
        );
    }

    #[test]
    fn test_dependency_propagation_with_index_cluster_conflict() {
        // Real-world bug scenario:
        // - winning_bids changes (has index on quickstart)
        // - flip_activities depends on winning_bids (also has index on quickstart)
        // - flippers depends on flip_activities
        //
        // What happens:
        // 1. winning_bids changes → quickstart becomes dirty
        // 2. flip_activities becomes dirty (index on dirty quickstart)
        // 3. BUT flip_activities also depends on winning_bids!
        // 4. So flip_activities should ALSO be schema-propagating
        // 5. Which should make materialize.public schema dirty
        // 6. Which should make flippers dirty
        //
        // The bug was: step 4 was skipped because flip_activities was already dirty

        let winning_bids = ObjectId::new(
            "materialize".to_string(),
            "internal".to_string(),
            "winning_bids".to_string(),
        );
        let flip_activities = ObjectId::new(
            "materialize".to_string(),
            "public".to_string(),
            "flip_activities".to_string(),
        );
        let flippers = ObjectId::new(
            "materialize".to_string(),
            "public".to_string(),
            "flippers".to_string(),
        );

        let base_facts = BaseFacts {
            object_in_schema: vec![
                (
                    winning_bids.clone(),
                    "materialize".to_string(),
                    "internal".to_string(),
                ),
                (
                    flip_activities.clone(),
                    "materialize".to_string(),
                    "public".to_string(),
                ),
                (
                    flippers.clone(),
                    "materialize".to_string(),
                    "public".to_string(),
                ),
            ],
            depends_on: vec![
                (flip_activities.clone(), winning_bids.clone()), // flip_activities depends on winning_bids
                (flippers.clone(), flip_activities.clone()), // flippers depends on flip_activities
            ],
            stmt_uses_cluster: vec![(winning_bids.clone(), "staging".to_string())],
            index_uses_cluster: vec![
                (
                    winning_bids.clone(),
                    "idx_item".to_string(),
                    "quickstart".to_string(),
                ),
                (
                    flip_activities.clone(),
                    "idx_flipper".to_string(),
                    "quickstart".to_string(),
                ),
            ],
        };

        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(winning_bids.clone());

        let (dirty_stmts, dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts);

        println!("Dirty stmts: {:?}", dirty_stmts);
        println!("Dirty schemas: {:?}", dirty_schemas);

        // winning_bids should be dirty (changed)
        assert!(
            dirty_stmts.contains(&winning_bids),
            "winning_bids should be dirty"
        );

        // materialize.internal schema should be dirty
        assert!(
            dirty_schemas.contains(&("materialize".to_string(), "internal".to_string())),
            "materialize.internal schema should be dirty"
        );

        // quickstart cluster should be dirty (winning_bids has index on it)
        assert!(
            dirty_clusters.iter().any(|c| c.name == "quickstart"),
            "quickstart cluster should be dirty"
        );

        // flip_activities should be dirty (depends on winning_bids)
        assert!(
            dirty_stmts.contains(&flip_activities),
            "flip_activities should be dirty - depends on winning_bids"
        );

        // CRITICAL: materialize.public schema should be dirty
        // flip_activities is dirty due to both:
        // 1. Its index is on dirty quickstart (index-only dirty)
        // 2. It depends on winning_bids (schema-propagating dirty)
        // The second reason should make materialize.public schema dirty
        assert!(
            dirty_schemas.contains(&("materialize".to_string(), "public".to_string())),
            "materialize.public schema should be dirty - flip_activities depends on winning_bids"
        );

        // flippers should be dirty (materialize.public schema is dirty)
        assert!(
            dirty_stmts.contains(&flippers),
            "flippers should be dirty - its schema (materialize.public) is dirty"
        );
    }

    #[test]
    fn test_index_cluster_does_not_cause_unnecessary_redeployment() {
        // Real-world scenario from auction_house project:
        // - materialize.foo.b changes (has default index in quickstart)
        // - materialize.internal.winning_bids has MV in staging cluster + index in quickstart
        // - materialize.public.flip_activities depends on winning_bids
        //
        // Expected:
        // - Only foo.b should be dirty
        // - materialize.foo schema should be dirty
        // - quickstart cluster should be dirty
        // - staging cluster should NOT be dirty (no objects using it changed)
        // - materialize.internal schema should NOT be dirty
        // - winning_bids should NOT be dirty (index in dirty cluster doesn't cause redeployment)
        // - flip_activities should NOT be dirty (winning_bids isn't dirty)

        let foo_b = ObjectId::new(
            "materialize".to_string(),
            "foo".to_string(),
            "b".to_string(),
        );
        let winning_bids = ObjectId::new(
            "materialize".to_string(),
            "internal".to_string(),
            "winning_bids".to_string(),
        );
        let flip_activities = ObjectId::new(
            "materialize".to_string(),
            "public".to_string(),
            "flip_activities".to_string(),
        );

        let base_facts = BaseFacts {
            object_in_schema: vec![
                (foo_b.clone(), "materialize".to_string(), "foo".to_string()),
                (
                    winning_bids.clone(),
                    "materialize".to_string(),
                    "internal".to_string(),
                ),
                (
                    flip_activities.clone(),
                    "materialize".to_string(),
                    "public".to_string(),
                ),
            ],
            depends_on: vec![(flip_activities.clone(), winning_bids.clone())],
            // foo.b has default index in quickstart
            // winning_bids has MV in staging, index in quickstart
            stmt_uses_cluster: vec![(winning_bids.clone(), "staging".to_string())],
            index_uses_cluster: vec![
                (
                    foo_b.clone(),
                    "default_idx".to_string(),
                    "quickstart".to_string(),
                ),
                (
                    winning_bids.clone(),
                    "idx1".to_string(),
                    "quickstart".to_string(),
                ),
            ],
        };

        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(foo_b.clone());

        let (dirty_stmts, dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts);

        // Only foo.b should be dirty
        assert!(dirty_stmts.contains(&foo_b), "foo.b should be dirty");
        assert_eq!(
            dirty_stmts.len(),
            1,
            "only foo.b should be dirty, got: {:?}",
            dirty_stmts
        );

        // materialize.foo schema should be dirty
        assert!(
            dirty_schemas.contains(&("materialize".to_string(), "foo".to_string())),
            "materialize.foo schema should be dirty"
        );

        // quickstart cluster should be dirty (foo.b has index on it)
        assert!(
            dirty_clusters.iter().any(|c| c.name == "quickstart"),
            "quickstart cluster should be dirty"
        );

        // staging cluster should NOT be dirty (no changed objects use it)
        assert!(
            !dirty_clusters.iter().any(|c| c.name == "staging"),
            "staging cluster should NOT be dirty"
        );

        // materialize.internal schema should NOT be dirty
        assert!(
            !dirty_schemas.contains(&("materialize".to_string(), "internal".to_string())),
            "materialize.internal schema should NOT be dirty"
        );

        // winning_bids should NOT be dirty (even though it has index in quickstart)
        assert!(
            !dirty_stmts.contains(&winning_bids),
            "winning_bids should NOT be dirty - index cluster doesn't cause redeployment"
        );

        // flip_activities should NOT be dirty (winning_bids isn't dirty)
        assert!(
            !dirty_stmts.contains(&flip_activities),
            "flip_activities should NOT be dirty - winning_bids isn't dirty"
        );
    }
}
