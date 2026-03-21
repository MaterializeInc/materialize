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
//! DirtyStmt(O) :- DependsOn(O, P), DirtyStmt(P), NOT IsChangedReplacement(P)  # Downstream dependents are dirty (except through changed replacement MVs)
//! DirtyStmt(O) :- DirtySchema(Db, Sch), ObjectInSchema(O, Db, Sch) # Objects in dirty schemas are dirty
//! ```
//!
//! **Key Insight:** Index clusters do NOT cause objects to be marked dirty. Indexes are physical
//! optimizations that can be managed independently without redeploying the object's statement.
//! If object A's index uses a dirty cluster, object A is NOT marked for redeployment.
//!
//! ### Rule Category 2 — Cluster Dirtiness
//! ```datalog
//! DirtyCluster(C) :- ChangedStmt(O), StmtUsesCluster(O, C), NOT IsSink(O)   # Clusters of changed statements are dirty (excluding sinks)
//! DirtyCluster(C) :- ChangedStmt(O), IndexUsesCluster(O, _, C), NOT IsSink(O) # Clusters of changed indexes are dirty (excluding sinks)
//! ```
//!
//! **Note:** Clusters are only marked dirty when the STATEMENT itself changes,
//! not when the object is dirty for other reasons (dependencies, schema propagation, etc.).
//! **Sinks are excluded** because they write to external systems and are created after the swap.
//!
//! ### Rule Category 3 — Schema Dirtiness
//! ```datalog
//! DirtySchema(Db, Sch) :- DirtyStmt(O), ObjectInSchema(O, Db, Sch), NOT IsSink(O)  # Dirty objects make their schemas dirty (excluding sinks)
//! ```
//!
//! **Key Property:** All dirty objects (except sinks) contribute to schema dirtiness, which triggers
//! schema-level atomic redeployment. Sinks are excluded because they are created after the swap
//! during apply and shouldn't cause other objects to be redeployed.

mod base_facts;
mod datalog;
mod diff;
mod logging;
mod types;

pub use types::ChangeSet;

use super::deployment_snapshot::DeploymentSnapshot;
use super::planned::Project;
use crate::client::DeploymentKind;
use crate::project::SchemaQualifier;
use crate::project::object_id::ObjectId;
use std::collections::BTreeSet;

use base_facts::extract_base_facts;
use datalog::compute_dirty_datalog;
use diff::find_changed_objects;

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

        // Step 2b: Identify changed replacement objects (exist in old snapshot AND old schema was Replacement)
        // These use the in-place replacement protocol, so dirtiness should NOT propagate
        // through them to downstream objects.
        // Objects transitioning from a non-replacement schema (e.g., Objects) to Replacement
        // must go through blue/green swap, not CREATE REPLACEMENT.
        let changed_replacements: BTreeSet<ObjectId> = base_facts
            .is_replacement
            .iter()
            .filter(|obj| {
                old_snapshot.objects.contains_key(*obj)
                    && old_snapshot
                        .schemas
                        .get(&SchemaQualifier::new(
                            obj.database.clone(),
                            obj.schema.clone(),
                        ))
                        .copied()
                        == Some(DeploymentKind::Replacement)
            })
            .cloned()
            .collect();

        // Step 3: Run Datalog fixed-point computation
        let (dirty_stmts, dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_objects, &base_facts, &changed_replacements);

        // Step 4: Separate replacement objects into new vs changed
        // - New: use blue-green swap. Includes objects not in old snapshot OR objects whose
        //   old schema was not Replacement (e.g., transitioning from Objects to Replacement)
        // - Changed: use CREATE REPLACEMENT. Only for objects that existed in old snapshot
        //   AND whose old schema was already Replacement kind.
        let (new_replacement_objects, changed_replacement_objects) = dirty_stmts
            .iter()
            .filter(|obj| base_facts.is_replacement.contains(*obj))
            .cloned()
            .partition(|obj| {
                !old_snapshot.objects.contains_key(obj)
                    || old_snapshot
                        .schemas
                        .get(&SchemaQualifier::new(
                            obj.database.clone(),
                            obj.schema.clone(),
                        ))
                        .copied()
                        != Some(DeploymentKind::Replacement)
            });

        ChangeSet {
            changed_objects: changed_objects.into_iter().collect(),
            dirty_schemas: dirty_schemas.into_iter().collect(),
            dirty_clusters: dirty_clusters.into_iter().collect(),
            objects_to_deploy: dirty_stmts.into_iter().collect(),
            new_replacement_objects,
            changed_replacement_objects,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::DeploymentKind;
    use crate::project::SchemaQualifier;
    use crate::project::ast::Statement;
    use crate::project::deployment_snapshot::DeploymentSnapshot;
    use crate::project::object_id::ObjectId;
    use crate::project::planned::{Database, DatabaseObject, Project, Schema, SchemaType};
    use crate::project::typed;
    use base_facts::BaseFacts;
    use datalog::compute_dirty_datalog;
    use std::collections::{BTreeMap, BTreeSet};

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
            is_sink: BTreeSet::new(),
            is_replacement: BTreeSet::new(),
        };

        // Only obj1 is changed
        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(obj1.clone());

        // Run Datalog computation
        let (dirty_stmts, _dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts, &BTreeSet::new());

        // Verify schema is dirty
        assert!(
            dirty_schemas.contains(&SchemaQualifier::new(
                "db".to_string(),
                "schema".to_string()
            )),
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
        // - quickstart cluster becomes dirty
        // - Index needs redeployment
        // - winning_bids needs redeployment (to deploy its index)
        // - BUT staging cluster should NOT be dirty

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
            is_sink: BTreeSet::new(),
            is_replacement: BTreeSet::new(),
        };

        // Only other_obj is changed
        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(other.clone());

        // Run Datalog computation
        let (dirty_stmts, dirty_clusters, _dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts, &BTreeSet::new());

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
        // - index_cluster becomes dirty
        // - table1 should NOT be dirty (indexes don't cause redeployment)
        // - schema should NOT be dirty
        // - table2 should NOT be dirty

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
            is_sink: BTreeSet::new(),
            is_replacement: BTreeSet::new(),
        };

        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(other.clone());

        let (dirty_stmts, dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts, &BTreeSet::new());

        // index_cluster should be dirty
        assert!(dirty_clusters.iter().any(|c| c.name == "index_cluster"));

        // table1 should NOT be dirty (indexes don't cause object redeployment)
        assert!(
            !dirty_stmts.contains(&table1),
            "table1 should NOT be dirty - indexes don't cause redeployment"
        );

        // Schema should NOT be dirty
        assert!(
            !dirty_schemas.contains(&SchemaQualifier::new(
                "db".to_string(),
                "schema".to_string()
            )),
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
            is_sink: BTreeSet::new(),
            is_replacement: BTreeSet::new(),
        };

        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(flippers.clone());

        let (dirty_stmts, dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts, &BTreeSet::new());

        // materialize.public schema should be dirty
        assert!(dirty_schemas.contains(&SchemaQualifier::new(
            "materialize".to_string(),
            "public".to_string()
        )));

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
            is_sink: BTreeSet::new(),
            is_replacement: BTreeSet::new(),
        };

        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(winning_bids.clone());

        let (dirty_stmts, dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts, &BTreeSet::new());

        println!("Dirty stmts: {:?}", dirty_stmts);
        println!("Dirty schemas: {:?}", dirty_schemas);

        // winning_bids should be dirty (changed)
        assert!(
            dirty_stmts.contains(&winning_bids),
            "winning_bids should be dirty"
        );

        // materialize.internal schema should be dirty
        assert!(
            dirty_schemas.contains(&SchemaQualifier::new(
                "materialize".to_string(),
                "internal".to_string()
            )),
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
        assert!(
            dirty_schemas.contains(&SchemaQualifier::new(
                "materialize".to_string(),
                "public".to_string()
            )),
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
        // Real-world scenario from auction_house project

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
            is_sink: BTreeSet::new(),
            is_replacement: BTreeSet::new(),
        };

        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(foo_b.clone());

        let (dirty_stmts, dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts, &BTreeSet::new());

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
            dirty_schemas.contains(&SchemaQualifier::new(
                "materialize".to_string(),
                "foo".to_string()
            )),
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
            !dirty_schemas.contains(&SchemaQualifier::new(
                "materialize".to_string(),
                "internal".to_string()
            )),
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

    // =========================================================================
    // Replacement MV tests
    // =========================================================================

    #[test]
    fn test_replacement_mv_does_not_dirty_its_schema() {
        // A changed replacement MV should NOT make its schema dirty.

        let mv1 = ObjectId::new("db".to_string(), "analytics".to_string(), "mv1".to_string());
        let mv2 = ObjectId::new("db".to_string(), "analytics".to_string(), "mv2".to_string());
        let view1 = ObjectId::new(
            "db".to_string(),
            "analytics".to_string(),
            "view1".to_string(),
        );

        let mut is_replacement = BTreeSet::new();
        is_replacement.insert(mv1.clone());
        is_replacement.insert(mv2.clone());
        // view1 is NOT a replacement (mixed schema scenario)

        let base_facts = BaseFacts {
            object_in_schema: vec![
                (mv1.clone(), "db".to_string(), "analytics".to_string()),
                (mv2.clone(), "db".to_string(), "analytics".to_string()),
                (view1.clone(), "db".to_string(), "analytics".to_string()),
            ],
            depends_on: vec![],
            stmt_uses_cluster: vec![],
            index_uses_cluster: vec![],
            is_sink: BTreeSet::new(),
            is_replacement,
        };

        // Only mv1 changed
        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(mv1.clone());

        let (dirty_stmts, _dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts, &BTreeSet::new());

        // mv1 should be dirty (it changed)
        assert!(dirty_stmts.contains(&mv1), "mv1 should be dirty");

        // Schema should NOT be dirty (replacement MVs don't propagate to schemas)
        assert!(
            !dirty_schemas.contains(&SchemaQualifier::new(
                "db".to_string(),
                "analytics".to_string()
            )),
            "analytics schema should NOT be dirty - replacement MV doesn't dirty schema"
        );

        // mv2 should NOT be dirty (schema isn't dirty, so no propagation)
        assert!(
            !dirty_stmts.contains(&mv2),
            "mv2 should NOT be dirty - schema not dirty"
        );

        // view1 should NOT be dirty either
        assert!(
            !dirty_stmts.contains(&view1),
            "view1 should NOT be dirty - schema not dirty"
        );
    }

    #[test]
    fn test_replacement_mv_does_dirty_its_cluster() {
        // Unlike sinks, replacement MVs DO make their clusters dirty.

        let mv1 = ObjectId::new("db".to_string(), "analytics".to_string(), "mv1".to_string());

        let mut is_replacement = BTreeSet::new();
        is_replacement.insert(mv1.clone());

        let base_facts = BaseFacts {
            object_in_schema: vec![(mv1.clone(), "db".to_string(), "analytics".to_string())],
            depends_on: vec![],
            stmt_uses_cluster: vec![(mv1.clone(), "analytics_cluster".to_string())],
            index_uses_cluster: vec![],
            is_sink: BTreeSet::new(),
            is_replacement,
        };

        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(mv1.clone());

        let (_dirty_stmts, dirty_clusters, _dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts, &BTreeSet::new());

        assert!(
            dirty_clusters.iter().any(|c| c.name == "analytics_cluster"),
            "analytics_cluster should be dirty - replacement MVs DO dirty clusters"
        );
    }

    #[test]
    fn test_replacement_mv_not_pulled_in_by_dirty_schema() {
        // When a non-replacement object makes a schema dirty,
        // replacement MVs in that schema should NOT be pulled in (Rule 6 exclusion).

        let regular = ObjectId::new(
            "db".to_string(),
            "analytics".to_string(),
            "regular".to_string(),
        );
        let other = ObjectId::new(
            "db".to_string(),
            "analytics".to_string(),
            "other".to_string(),
        );
        let replacement_mv = ObjectId::new(
            "db".to_string(),
            "analytics".to_string(),
            "my_mv".to_string(),
        );

        let mut is_replacement = BTreeSet::new();
        is_replacement.insert(replacement_mv.clone());

        let base_facts = BaseFacts {
            object_in_schema: vec![
                (regular.clone(), "db".to_string(), "analytics".to_string()),
                (other.clone(), "db".to_string(), "analytics".to_string()),
                (
                    replacement_mv.clone(),
                    "db".to_string(),
                    "analytics".to_string(),
                ),
            ],
            depends_on: vec![],
            stmt_uses_cluster: vec![],
            index_uses_cluster: vec![],
            is_sink: BTreeSet::new(),
            is_replacement,
        };

        // regular object changed -> schema dirty -> other pulled in
        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(regular.clone());

        let (dirty_stmts, _dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts, &BTreeSet::new());

        // Schema should be dirty (regular object changed)
        assert!(
            dirty_schemas.contains(&SchemaQualifier::new(
                "db".to_string(),
                "analytics".to_string()
            )),
            "schema should be dirty from regular object change"
        );

        // other (non-replacement) should be pulled in via schema propagation
        assert!(
            dirty_stmts.contains(&other),
            "other should be dirty via schema propagation"
        );

        // replacement_mv should NOT be pulled in
        assert!(
            !dirty_stmts.contains(&replacement_mv),
            "replacement MV should NOT be pulled in by dirty schema"
        );
    }

    #[test]
    fn test_replacement_mv_dirty_via_dependency() {
        // Replacement MVs should still become dirty if they depend on
        // another dirty object (Rule 4: DependsOn propagation).

        let upstream = ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "source_view".to_string(),
        );
        let replacement_mv = ObjectId::new(
            "db".to_string(),
            "analytics".to_string(),
            "my_mv".to_string(),
        );

        let mut is_replacement = BTreeSet::new();
        is_replacement.insert(replacement_mv.clone());

        let base_facts = BaseFacts {
            object_in_schema: vec![
                (upstream.clone(), "db".to_string(), "public".to_string()),
                (
                    replacement_mv.clone(),
                    "db".to_string(),
                    "analytics".to_string(),
                ),
            ],
            depends_on: vec![(replacement_mv.clone(), upstream.clone())],
            stmt_uses_cluster: vec![],
            index_uses_cluster: vec![],
            is_sink: BTreeSet::new(),
            is_replacement,
        };

        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(upstream.clone());

        let (dirty_stmts, _dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts, &BTreeSet::new());

        // replacement_mv should be dirty via dependency
        assert!(
            dirty_stmts.contains(&replacement_mv),
            "replacement MV should be dirty - depends on changed upstream"
        );

        // But analytics schema should NOT be dirty (replacement MV doesn't propagate)
        assert!(
            !dirty_schemas.contains(&SchemaQualifier::new(
                "db".to_string(),
                "analytics".to_string()
            )),
            "analytics schema should NOT be dirty - replacement MV doesn't dirty schema"
        );
    }

    #[test]
    fn test_replacement_mv_dirty_via_cluster() {
        // When a cluster becomes dirty, replacement MVs on that cluster
        // should become dirty (Rule 3: StmtUsesCluster propagation).

        let regular = ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "regular_mv".to_string(),
        );
        let replacement_mv = ObjectId::new(
            "db".to_string(),
            "analytics".to_string(),
            "my_mv".to_string(),
        );

        let mut is_replacement = BTreeSet::new();
        is_replacement.insert(replacement_mv.clone());

        let base_facts = BaseFacts {
            object_in_schema: vec![
                (regular.clone(), "db".to_string(), "public".to_string()),
                (
                    replacement_mv.clone(),
                    "db".to_string(),
                    "analytics".to_string(),
                ),
            ],
            depends_on: vec![],
            stmt_uses_cluster: vec![
                (regular.clone(), "shared_cluster".to_string()),
                (replacement_mv.clone(), "shared_cluster".to_string()),
            ],
            index_uses_cluster: vec![],
            is_sink: BTreeSet::new(),
            is_replacement,
        };

        // regular object changes -> shared_cluster dirty -> replacement_mv dirty
        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(regular.clone());

        let (dirty_stmts, dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts, &BTreeSet::new());

        assert!(
            dirty_clusters.iter().any(|c| c.name == "shared_cluster"),
            "shared_cluster should be dirty"
        );

        assert!(
            dirty_stmts.contains(&replacement_mv),
            "replacement MV should be dirty - its cluster is dirty"
        );

        // analytics schema still should NOT be dirty
        assert!(
            !dirty_schemas.contains(&SchemaQualifier::new(
                "db".to_string(),
                "analytics".to_string()
            )),
            "analytics schema should NOT be dirty even though replacement MV is dirty"
        );
    }

    #[test]
    fn test_mixed_replacement_and_regular_in_shared_cluster() {
        // Real-world scenario: one cluster runs both a replacement schema (MVs)
        // and a regular schema (views + indexes).

        let regular_view = ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "my_view".to_string(),
        );
        let regular_view2 = ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "other_view".to_string(),
        );
        let replacement_mv1 = ObjectId::new(
            "db".to_string(),
            "analytics".to_string(),
            "mv_alpha".to_string(),
        );
        let replacement_mv2 = ObjectId::new(
            "db".to_string(),
            "analytics".to_string(),
            "mv_beta".to_string(),
        );

        let mut is_replacement = BTreeSet::new();
        is_replacement.insert(replacement_mv1.clone());
        is_replacement.insert(replacement_mv2.clone());

        let base_facts = BaseFacts {
            object_in_schema: vec![
                (regular_view.clone(), "db".to_string(), "public".to_string()),
                (
                    regular_view2.clone(),
                    "db".to_string(),
                    "public".to_string(),
                ),
                (
                    replacement_mv1.clone(),
                    "db".to_string(),
                    "analytics".to_string(),
                ),
                (
                    replacement_mv2.clone(),
                    "db".to_string(),
                    "analytics".to_string(),
                ),
            ],
            depends_on: vec![],
            stmt_uses_cluster: vec![
                (regular_view.clone(), "shared".to_string()),
                (replacement_mv1.clone(), "shared".to_string()),
                (replacement_mv2.clone(), "shared".to_string()),
            ],
            index_uses_cluster: vec![],
            is_sink: BTreeSet::new(),
            is_replacement,
        };

        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(regular_view.clone());

        let (dirty_stmts, dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts, &BTreeSet::new());

        // Cluster dirty
        assert!(dirty_clusters.iter().any(|c| c.name == "shared"));

        // public schema dirty (regular object changed)
        assert!(dirty_schemas.contains(&SchemaQualifier::new(
            "db".to_string(),
            "public".to_string()
        )));

        // regular_view2 pulled in via schema propagation
        assert!(dirty_stmts.contains(&regular_view2));

        // Both replacement MVs dirty via cluster
        assert!(
            dirty_stmts.contains(&replacement_mv1),
            "replacement_mv1 should be dirty via cluster"
        );
        assert!(
            dirty_stmts.contains(&replacement_mv2),
            "replacement_mv2 should be dirty via cluster"
        );

        // analytics schema should NOT be dirty
        assert!(
            !dirty_schemas.contains(&SchemaQualifier::new(
                "db".to_string(),
                "analytics".to_string()
            )),
            "analytics schema should NOT be dirty - only replacement MVs are there"
        );
    }

    // =========================================================================
    // Schema kind transition tests (Objects -> Replacement)
    // =========================================================================

    /// Helper to parse a CREATE MATERIALIZED VIEW statement.
    fn parse_materialized_view(sql: &str) -> Statement {
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();
        if let mz_sql_parser::ast::Statement::CreateMaterializedView(s) = &parsed[0].ast {
            Statement::CreateMaterializedView(s.clone())
        } else {
            panic!("Expected CreateMaterializedView");
        }
    }

    /// Build a minimal Project containing a single schema with given objects.
    fn build_project(
        db: &str,
        schema: &str,
        objects: Vec<(ObjectId, Statement)>,
        is_replacement: bool,
    ) -> Project {
        let db_objects: Vec<DatabaseObject> = objects
            .into_iter()
            .map(|(id, stmt)| DatabaseObject {
                id,
                typed_object: typed::DatabaseObject {
                    path: std::path::PathBuf::from("test.sql"),
                    stmt,
                    indexes: vec![],
                    constraints: vec![],
                    grants: vec![],
                    comments: vec![],
                    tests: vec![],
                },
                dependencies: BTreeSet::new(),
                is_constraint_mv: false,
            })
            .collect();

        let mut replacement_schemas = BTreeSet::new();
        if is_replacement {
            replacement_schemas.insert(SchemaQualifier::new(db.to_string(), schema.to_string()));
        }

        Project {
            databases: vec![Database {
                name: db.to_string(),
                schemas: vec![Schema {
                    name: schema.to_string(),
                    objects: db_objects,
                    mod_statements: None,
                    schema_type: SchemaType::Compute,
                }],
                mod_statements: None,
            }],
            dependency_graph: BTreeMap::new(),
            external_dependencies: BTreeSet::new(),
            cluster_dependencies: BTreeSet::new(),
            tests: vec![],
            replacement_schemas,
        }
    }

    #[test]
    fn test_schema_transition_objects_to_replacement_routes_to_new_replacement() {
        // When a schema transitions from Objects kind to Replacement kind,
        // existing objects should go through blue/green swap (new_replacement_objects),
        // NOT CREATE REPLACEMENT (changed_replacement_objects).

        let obj_id = ObjectId::new(
            "db".to_string(),
            "analytics".to_string(),
            "my_mv".to_string(),
        );

        // Old snapshot: object existed in a schema with Objects kind
        let old_snapshot = DeploymentSnapshot {
            objects: BTreeMap::from([(obj_id.clone(), "hash_old".to_string())]),
            schemas: BTreeMap::from([(
                SchemaQualifier::new("db".to_string(), "analytics".to_string()),
                DeploymentKind::Objects,
            )]),
        };

        // New snapshot: object changed (different hash)
        let new_snapshot = DeploymentSnapshot {
            objects: BTreeMap::from([(obj_id.clone(), "hash_new".to_string())]),
            schemas: BTreeMap::from([(
                SchemaQualifier::new("db".to_string(), "analytics".to_string()),
                DeploymentKind::Replacement,
            )]),
        };

        // Project now treats the schema as replacement
        let project = build_project(
            "db",
            "analytics",
            vec![(
                obj_id.clone(),
                parse_materialized_view("CREATE MATERIALIZED VIEW my_mv IN CLUSTER c1 AS SELECT 1"),
            )],
            true, // is_replacement
        );

        let changeset =
            ChangeSet::from_deployment_snapshot_comparison(&old_snapshot, &new_snapshot, &project);

        // Object should be in new_replacement_objects (blue/green), NOT changed_replacement_objects
        assert!(
            changeset.new_replacement_objects.contains(&obj_id),
            "Object transitioning from Objects->Replacement schema should use blue/green swap"
        );
        assert!(
            !changeset.changed_replacement_objects.contains(&obj_id),
            "Object transitioning from Objects->Replacement schema should NOT use CREATE REPLACEMENT"
        );
    }

    #[test]
    fn test_steady_state_replacement_routes_to_changed_replacement() {
        // When a schema was already Replacement kind and stays Replacement,
        // existing objects should use CREATE REPLACEMENT (changed_replacement_objects).

        let obj_id = ObjectId::new(
            "db".to_string(),
            "analytics".to_string(),
            "my_mv".to_string(),
        );

        // Old snapshot: object existed in a schema already with Replacement kind
        let old_snapshot = DeploymentSnapshot {
            objects: BTreeMap::from([(obj_id.clone(), "hash_old".to_string())]),
            schemas: BTreeMap::from([(
                SchemaQualifier::new("db".to_string(), "analytics".to_string()),
                DeploymentKind::Replacement,
            )]),
        };

        // New snapshot: object changed
        let new_snapshot = DeploymentSnapshot {
            objects: BTreeMap::from([(obj_id.clone(), "hash_new".to_string())]),
            schemas: BTreeMap::from([(
                SchemaQualifier::new("db".to_string(), "analytics".to_string()),
                DeploymentKind::Replacement,
            )]),
        };

        let project = build_project(
            "db",
            "analytics",
            vec![(
                obj_id.clone(),
                parse_materialized_view("CREATE MATERIALIZED VIEW my_mv IN CLUSTER c1 AS SELECT 1"),
            )],
            true,
        );

        let changeset =
            ChangeSet::from_deployment_snapshot_comparison(&old_snapshot, &new_snapshot, &project);

        // Object should be in changed_replacement_objects (CREATE REPLACEMENT)
        assert!(
            changeset.changed_replacement_objects.contains(&obj_id),
            "Object in steady-state Replacement schema should use CREATE REPLACEMENT"
        );
        assert!(
            !changeset.new_replacement_objects.contains(&obj_id),
            "Object in steady-state Replacement schema should NOT use blue/green swap"
        );
    }
}
