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

use super::ast::{Cluster, Statement};
use super::deployment_snapshot::DeploymentSnapshot;
use super::planned::{self, Project};
use crate::project::SchemaQualifier;
use crate::project::object_id::ObjectId;
use crate::verbose;
use owo_colors::OwoColorize;
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
    pub dirty_schemas: BTreeSet<SchemaQualifier>,

    /// Clusters used by objects in dirty schemas
    pub dirty_clusters: BTreeSet<Cluster>,

    /// All objects that need redeployment (includes transitive dependencies)
    pub objects_to_deploy: BTreeSet<ObjectId>,

    /// New replacement MVs (in replacement schemas but NOT in old snapshot).
    /// These are deployed via normal blue-green schema swap.
    pub new_replacement_objects: BTreeSet<ObjectId>,

    /// Changed replacement MVs (in replacement schemas AND in old snapshot with different hash).
    /// These are deployed via CREATE REPLACEMENT MV protocol.
    pub changed_replacement_objects: BTreeSet<ObjectId>,
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

        // Step 2b: Identify changed replacement objects (exist in old snapshot)
        // These use the in-place replacement protocol, so dirtiness should NOT propagate
        // through them to downstream objects.
        let changed_replacements: BTreeSet<ObjectId> = base_facts
            .is_replacement
            .iter()
            .filter(|obj| old_snapshot.objects.contains_key(*obj))
            .cloned()
            .collect();

        // Step 3: Run Datalog fixed-point computation
        let (dirty_stmts, dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_objects, &base_facts, &changed_replacements);

        // Step 4: Separate replacement objects into new vs changed
        // - New: in replacement schemas but NOT in old snapshot (first deployment, use blue-green)
        // - Changed: in replacement schemas AND in old snapshot (update, use CREATE REPLACEMENT)
        let (new_replacement_objects, changed_replacement_objects) = dirty_stmts
            .iter()
            .filter(|obj| base_facts.is_replacement.contains(*obj))
            .cloned()
            .partition(|obj| !old_snapshot.objects.contains_key(obj));

        ChangeSet {
            changed_objects: changed_objects.into_iter().collect(),
            dirty_schemas: dirty_schemas.into_iter().collect(),
            dirty_clusters: dirty_clusters.into_iter().collect(),
            objects_to_deploy: dirty_stmts.into_iter().collect(),
            new_replacement_objects,
            changed_replacement_objects,
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

    /// IsSink(object) - objects that are sinks (should not propagate dirtiness to clusters/schemas)
    is_sink: BTreeSet<ObjectId>,

    /// IsReplacement(object) - objects in replacement schemas (should not propagate dirtiness to schemas)
    is_replacement: BTreeSet<ObjectId>,
}

/// Find changed objects by comparing snapshot hashes.
fn find_changed_objects(
    old_snapshot: &DeploymentSnapshot,
    new_snapshot: &DeploymentSnapshot,
) -> BTreeSet<ObjectId> {
    verbose!(
        "{} {}",
        "▶".cyan(),
        "Comparing deployment snapshots...".cyan().bold()
    );
    let mut changed = BTreeSet::new();

    // Objects with different hashes or newly added
    for (object_id, new_hash) in &new_snapshot.objects {
        match old_snapshot.objects.get(object_id) {
            Some(old_hash) if old_hash != new_hash => {
                verbose!(
                    "  ├─ {}: {} ({} {} → {})",
                    "Changed".green(),
                    object_id.to_string().cyan(),
                    "hash".dimmed(),
                    old_hash[..8].to_string().dimmed(),
                    new_hash[..8].to_string().dimmed()
                );
                changed.insert(object_id.clone());
            }
            None => {
                verbose!(
                    "  ├─ {}: {} ({} {})",
                    "New".green(),
                    object_id.to_string().cyan(),
                    "hash".dimmed(),
                    new_hash[..8].to_string().dimmed()
                );
                changed.insert(object_id.clone());
            }
            _ => {}
        }
    }

    // Deleted objects
    for object_id in old_snapshot.objects.keys() {
        if !new_snapshot.objects.contains_key(object_id) {
            verbose!("  ├─ {}: {}", "Deleted".red(), object_id.to_string().cyan());
            changed.insert(object_id.clone());
        }
    }

    verbose!(
        "  └─ Found {} changed object(s)",
        changed.len().to_string().bold()
    );
    changed
}

/// Extract all base facts from the project for Datalog computation.
fn extract_base_facts(project: &Project) -> BaseFacts {
    verbose!(
        "{} {}",
        "▶".cyan(),
        "Extracting base facts from project...".cyan().bold()
    );
    let mut object_in_schema = Vec::new();
    let mut depends_on = Vec::new();
    let mut stmt_uses_cluster = Vec::new();
    let mut index_uses_cluster = Vec::new();
    let mut is_sink = BTreeSet::new();
    let mut is_replacement = BTreeSet::new();

    // Extract facts from each object in the project
    for db in &project.databases {
        for schema in &db.schemas {
            // Check if this schema is a replacement schema
            let is_replacement_schema = project
                .replacement_schemas
                .iter()
                .any(|(d, s)| d == &db.name && s == &schema.name);

            for obj in &schema.objects {
                let obj_id = obj.id.clone();

                // ObjectInSchema fact
                object_in_schema.push((obj_id.clone(), db.name.clone(), schema.name.clone()));

                // IsSink fact - sinks should not propagate dirtiness to clusters/schemas
                if matches!(obj.typed_object.stmt, Statement::CreateSink(_)) {
                    verbose!("  ├─ {}: {}", "IsSink".yellow(), obj_id.to_string().cyan());
                    is_sink.insert(obj_id.clone());
                }

                // IsReplacement fact - replacement MVs should not propagate dirtiness to schemas
                if is_replacement_schema {
                    verbose!(
                        "  ├─ {}: {}",
                        "IsReplacement".yellow(),
                        obj_id.to_string().cyan()
                    );
                    is_replacement.insert(obj_id.clone());
                }

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

    verbose!(
        "  └─ Base facts: {} objects, {} dependencies, {} stmt→cluster, {} index→cluster, {} sinks, {} replacements",
        object_in_schema.len().to_string().bold(),
        depends_on.len().to_string().bold(),
        stmt_uses_cluster.len().to_string().bold(),
        index_uses_cluster.len().to_string().bold(),
        is_sink.len().to_string().bold(),
        is_replacement.len().to_string().bold()
    );

    BaseFacts {
        object_in_schema,
        depends_on,
        stmt_uses_cluster,
        index_uses_cluster,
        is_sink,
        is_replacement,
    }
}

/// Pre-computed indexes for efficient Datalog rule evaluation.
struct DatalogIndexes {
    /// Object -> clusters used by the statement
    stmt_to_clusters: BTreeMap<ObjectId, Vec<String>>,
    /// Object -> clusters used by indexes on that object
    index_to_clusters: BTreeMap<ObjectId, Vec<String>>,
    /// Parent -> list of dependent children (reverse of depends_on)
    dependents: BTreeMap<ObjectId, Vec<ObjectId>>,
    /// Object -> (database, schema) it belongs to
    object_to_schema: BTreeMap<ObjectId, SchemaQualifier>,
}

impl DatalogIndexes {
    fn from_base_facts(facts: &BaseFacts) -> Self {
        // stmt_to_clusters: group by object
        let mut stmt_to_clusters: BTreeMap<ObjectId, Vec<String>> = BTreeMap::new();
        for (obj, cluster) in &facts.stmt_uses_cluster {
            stmt_to_clusters
                .entry(obj.clone())
                .or_default()
                .push(cluster.clone());
        }

        // index_to_clusters: group by object (ignoring index name)
        let mut index_to_clusters: BTreeMap<ObjectId, Vec<String>> = BTreeMap::new();
        for (obj, _index_name, cluster) in &facts.index_uses_cluster {
            index_to_clusters
                .entry(obj.clone())
                .or_default()
                .push(cluster.clone());
        }

        // dependents: reverse the depends_on relation (parent -> children)
        let mut dependents: BTreeMap<ObjectId, Vec<ObjectId>> = BTreeMap::new();
        for (child, parent) in &facts.depends_on {
            dependents
                .entry(parent.clone())
                .or_default()
                .push(child.clone());
        }

        // object_to_schema: direct mapping
        let object_to_schema = facts
            .object_in_schema
            .iter()
            .map(|(obj, db, sch)| (obj.clone(), (db.clone(), sch.clone())))
            .collect();

        DatalogIndexes {
            stmt_to_clusters,
            index_to_clusters,
            dependents,
            object_to_schema,
        }
    }
}

/// Compute dirty objects, clusters, and schemas using fixed-point iteration.
///
/// Implements the Datalog rules defined at the top of this module.
///
/// **Important:** Sinks are special - they do NOT propagate dirtiness to clusters or schemas.
/// Sinks write to external systems and are created after the swap during apply, so they
/// shouldn't cause other objects to be redeployed.
fn compute_dirty_datalog(
    changed_stmts: &BTreeSet<ObjectId>,
    base_facts: &BaseFacts,
    changed_replacements: &BTreeSet<ObjectId>,
) -> (
    BTreeSet<ObjectId>,
    BTreeSet<Cluster>,
    BTreeSet<SchemaQualifier>,
) {
    verbose!(
        "{} {}",
        "▶".cyan(),
        "Starting fixed-point computation...".cyan().bold()
    );
    verbose!(
        "  ├─ Initial changed statements: [{}]",
        changed_stmts
            .iter()
            .map(|o| o.to_string().cyan().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    verbose!(
        "  └─ Known sinks: [{}]",
        base_facts
            .is_sink
            .iter()
            .map(|o| o.to_string().yellow().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let indexes = DatalogIndexes::from_base_facts(base_facts);

    // Initialize result sets
    let mut dirty_stmts: BTreeSet<ObjectId> = changed_stmts.clone();
    let mut dirty_clusters: BTreeSet<String> = BTreeSet::new();
    let mut dirty_schemas: BTreeSet<SchemaQualifier> = BTreeSet::new();

    // Fixed-point iteration: apply rules until no changes
    let mut iteration = 0;
    loop {
        iteration += 1;
        let prev_sizes = (dirty_stmts.len(), dirty_clusters.len(), dirty_schemas.len());
        verbose!(
            "\n{} {} (stmts={}, clusters={}, schemas={})",
            "▶".cyan(),
            format!("Iteration {}", iteration).cyan().bold(),
            dirty_stmts.len().to_string().bold(),
            dirty_clusters.len().to_string().bold(),
            dirty_schemas.len().to_string().bold()
        );

        // --- Cluster dirtiness rules (only from changed statements, excluding sinks) ---
        // Rule 1: DirtyCluster(C) :- ChangedStmt(O), StmtUsesCluster(O, C), NOT IsSink(O)
        // Rule 2: DirtyCluster(C) :- ChangedStmt(O), IndexUsesCluster(O, _, C), NOT IsSink(O)
        for obj in changed_stmts {
            // Sinks should NOT make clusters dirty
            if base_facts.is_sink.contains(obj) {
                verbose!(
                    "  ├─ {}: {} is a sink, not marking clusters dirty",
                    "SKIP".yellow().bold(),
                    obj.to_string().cyan()
                );
                continue;
            }
            if let Some(clusters) = indexes.stmt_to_clusters.get(obj) {
                for cluster in clusters {
                    if dirty_clusters.insert(cluster.clone()) {
                        verbose!(
                            "  ├─ {}: DirtyCluster({}) ← ChangedStmt({}) uses cluster",
                            "Rule 1".bold(),
                            cluster.magenta(),
                            obj.to_string().cyan()
                        );
                    }
                }
            }
            if let Some(clusters) = indexes.index_to_clusters.get(obj) {
                for cluster in clusters {
                    if dirty_clusters.insert(cluster.clone()) {
                        verbose!(
                            "  ├─ {}: DirtyCluster({}) ← ChangedStmt({}) has index on cluster",
                            "Rule 2".bold(),
                            cluster.magenta(),
                            obj.to_string().cyan()
                        );
                    }
                }
            }
        }

        // --- Statement dirtiness rules ---
        // Rule 3: DirtyStmt(O) :- StmtUsesCluster(O, C), DirtyCluster(C)
        for (obj, clusters) in &indexes.stmt_to_clusters {
            for cluster in clusters {
                if dirty_clusters.contains(cluster) && dirty_stmts.insert(obj.clone()) {
                    verbose!(
                        "  ├─ {}: DirtyStmt({}) ← uses DirtyCluster({})",
                        "Rule 3".bold(),
                        obj.to_string().cyan(),
                        cluster.magenta()
                    );
                    break;
                }
            }
        }

        // Rule 4: DirtyStmt(O) :- DependsOn(O, P), DirtyStmt(P), NOT IsChangedReplacement(P)
        // Changed replacement MVs are swapped in-place via ALTER MV APPLY REPLACEMENT,
        // so their downstream dependents don't need redeployment.
        let current_dirty: Vec<_> = dirty_stmts.iter().cloned().collect();
        for dirty_obj in current_dirty {
            if changed_replacements.contains(&dirty_obj) {
                verbose!(
                    "  ├─ {}: {} is a changed replacement MV, not propagating to dependents",
                    "SKIP".yellow().bold(),
                    dirty_obj.to_string().cyan()
                );
                continue;
            }
            if let Some(children) = indexes.dependents.get(&dirty_obj) {
                for child in children {
                    if dirty_stmts.insert(child.clone()) {
                        verbose!(
                            "  ├─ {}: DirtyStmt({}) ← depends on DirtyStmt({})",
                            "Rule 4".bold(),
                            child.to_string().cyan(),
                            dirty_obj.to_string().cyan()
                        );
                    }
                }
            }
        }

        // --- Schema dirtiness rules (excluding sinks and replacement MVs) ---
        // Rule 5: DirtySchema(Db, Sch) :- DirtyStmt(O), ObjectInSchema(O, Db, Sch), NOT IsSink(O), NOT IsReplacement(O)
        for obj in &dirty_stmts {
            // Sinks should NOT make schemas dirty
            if base_facts.is_sink.contains(obj) {
                verbose!(
                    "  ├─ {}: {} is a sink, not marking schema dirty",
                    "SKIP".yellow().bold(),
                    obj.to_string().cyan()
                );
                continue;
            }
            // Replacement MVs should NOT make schemas dirty
            // (we want per-MV granularity, not schema-level atomic redeployment)
            if base_facts.is_replacement.contains(obj) {
                verbose!(
                    "  ├─ {}: {} is a replacement MV, not marking schema dirty",
                    "SKIP".yellow().bold(),
                    obj.to_string().cyan()
                );
                continue;
            }
            if let Some((db, sch)) = indexes.object_to_schema.get(obj) {
                if dirty_schemas.insert((db.clone(), sch.clone())) {
                    verbose!(
                        "  ├─ {}: DirtySchema({}) ← DirtyStmt({}) in schema",
                        "Rule 5".bold(),
                        format!("{}.{}", db, sch).blue(),
                        obj.to_string().cyan()
                    );
                }
            }
        }

        // Rule 6: DirtyStmt(O) :- DirtySchema(Db, Sch), ObjectInSchema(O, Db, Sch), NOT IsReplacement(O)
        for (obj, (db, sch)) in &indexes.object_to_schema {
            if dirty_schemas.iter().any(|(d, s)| d == db && s == sch) {
                // Replacement MVs should NOT be pulled in by schema dirtiness
                if base_facts.is_replacement.contains(obj) {
                    continue;
                }
                if dirty_stmts.insert(obj.clone()) {
                    verbose!(
                        "  ├─ {}: DirtyStmt({}) ← in DirtySchema({})",
                        "Rule 6".bold(),
                        obj.to_string().cyan(),
                        format!("{}.{}", db, sch).blue()
                    );
                }
            }
        }

        // Fixed point reached when no sets grew
        if (dirty_stmts.len(), dirty_clusters.len(), dirty_schemas.len()) == prev_sizes {
            verbose!(
                "\n{} Fixed point reached after {} iteration(s)",
                "✓".green(),
                iteration.to_string().bold()
            );
            break;
        }
    }

    // Log final results
    verbose!("{} {}", "▶".cyan(), "Final Results".cyan().bold());
    verbose!(
        "  ├─ Dirty statements ({}): [{}]",
        dirty_stmts.len().to_string().bold(),
        dirty_stmts
            .iter()
            .map(|o| o.to_string().cyan().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    verbose!(
        "  ├─ Dirty clusters ({}): [{}]",
        dirty_clusters.len().to_string().bold(),
        dirty_clusters
            .iter()
            .map(|c| c.magenta().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    verbose!(
        "  └─ Dirty schemas ({}): [{}]",
        dirty_schemas.len().to_string().bold(),
        dirty_schemas
            .iter()
            .map(|(db, sch)| format!("{}.{}", db, sch).blue().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Convert cluster names to Cluster structs
    let dirty_cluster_structs = dirty_clusters.into_iter().map(Cluster::new).collect();

    (dirty_stmts, dirty_cluster_structs, dirty_schemas)
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
            is_sink: BTreeSet::new(),
            is_replacement: BTreeSet::new(),
        };

        let mut changed_stmts = BTreeSet::new();
        changed_stmts.insert(flippers.clone());

        let (dirty_stmts, dirty_clusters, dirty_schemas) =
            compute_dirty_datalog(&changed_stmts, &base_facts, &BTreeSet::new());

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

    // =========================================================================
    // Replacement MV tests
    // =========================================================================

    #[test]
    fn test_replacement_mv_does_not_dirty_its_schema() {
        // A changed replacement MV should NOT make its schema dirty.
        // This gives per-MV granularity: only the changed MV is redeployed,
        // not every object in the schema.

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
            !dirty_schemas.contains(&("db".to_string(), "analytics".to_string())),
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
        // This is because the staging cluster needs to be created for hydration.

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
            dirty_schemas.contains(&("db".to_string(), "analytics".to_string())),
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
            !dirty_schemas.contains(&("db".to_string(), "analytics".to_string())),
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
            !dirty_schemas.contains(&("db".to_string(), "analytics".to_string())),
            "analytics schema should NOT be dirty even though replacement MV is dirty"
        );
    }

    #[test]
    fn test_mixed_replacement_and_regular_in_shared_cluster() {
        // Real-world scenario: one cluster runs both a replacement schema (MVs)
        // and a regular schema (views + indexes). When a regular object changes:
        // - The cluster becomes dirty
        // - Regular objects on that cluster are pulled in
        // - Replacement MVs on that cluster are pulled in
        // - But replacement MVs don't propagate schema dirtiness

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
        assert!(dirty_schemas.contains(&("db".to_string(), "public".to_string())));

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
            !dirty_schemas.contains(&("db".to_string(), "analytics".to_string())),
            "analytics schema should NOT be dirty - only replacement MVs are there"
        );
    }
}
