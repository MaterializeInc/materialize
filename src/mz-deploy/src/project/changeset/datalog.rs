//! Datalog-style fixed-point computation of dirty objects, clusters, and schemas.
//!
//! Implements the propagation rules documented in the parent module's header.
//! Each `apply_*` helper corresponds to one or more Datalog rules and is
//! annotated with the exact rule it encodes.
//!
//! ## Algorithm
//!
//! 1. **Seed:** Initialize `dirty_stmts` from `changed_stmts` (objects whose
//!    hashes differ between the old and new snapshots).
//! 2. **Build indexes:** Pre-compute reverse lookup maps (`DatalogIndexes`)
//!    from base facts for O(1) rule evaluation.
//! 3. **Fixed-point loop:** In each iteration, apply all five rule groups in
//!    a fixed order:
//!    1. Cluster dirtiness (from changed statements only)
//!    2. Statement dirtiness from dirty clusters
//!    3. Dependency propagation (downstream of dirty statements)
//!    4. Schema dirtiness (from dirty statements, excluding sinks/replacements)
//!    5. Statement dirtiness from dirty schemas (excluding replacements)
//! 4. **Termination:** The loop converges when no set (`dirty_stmts`,
//!    `dirty_clusters`, `dirty_schemas`) grows in an iteration. Convergence
//!    is guaranteed because all sets are monotonically non-decreasing and
//!    bounded by the finite project universe.
//!
//! ## Rule Application Order
//!
//! The ordering within a single iteration matters for convergence speed but
//! not correctness — the fixed-point semantics guarantee the same result
//! regardless of order. The chosen order (clusters → stmts-from-clusters →
//! dependencies → schemas → stmts-from-schemas) maximizes information
//! propagated per iteration, typically converging in 2–3 rounds.

use super::super::SchemaQualifier;
use super::super::ast::Cluster;
use super::super::object_id::ObjectId;
use super::base_facts::BaseFacts;
use super::logging::{log_datalog_start, log_final_results, log_iteration};
use crate::verbose;
use owo_colors::OwoColorize;
use std::collections::{BTreeMap, BTreeSet};

/// Pre-computed indexes for efficient Datalog rule evaluation.
pub(super) struct DatalogIndexes {
    /// Object -> clusters used by the statement
    stmt_to_clusters: BTreeMap<ObjectId, Vec<String>>,
    /// Object -> clusters used by indexes on that object
    index_to_clusters: BTreeMap<ObjectId, Vec<String>>,
    /// Parent -> list of dependent children (reverse of depends_on)
    dependents: BTreeMap<ObjectId, Vec<ObjectId>>,
    /// Object -> schema qualifier it belongs to
    pub object_to_schema: BTreeMap<ObjectId, SchemaQualifier>,
}

impl DatalogIndexes {
    pub fn from_base_facts(facts: &BaseFacts) -> Self {
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
            .map(|(obj, db, sch)| (obj.clone(), SchemaQualifier::new(db.clone(), sch.clone())))
            .collect();

        DatalogIndexes {
            stmt_to_clusters,
            index_to_clusters,
            dependents,
            object_to_schema,
        }
    }
}

/// Mutable working sets carried across fixed-point iterations.
///
/// Keeps the Datalog loop state explicit and grouped so each rule helper can
/// update shared dirtiness sets without additional tuple plumbing.
pub(super) struct DirtyState {
    pub dirty_stmts: BTreeSet<ObjectId>,
    pub dirty_clusters: BTreeSet<String>,
    pub dirty_schemas: BTreeSet<SchemaQualifier>,
}

impl DirtyState {
    /// Seeds dirty state from objects whose hashes changed between snapshots.
    pub fn new(changed_stmts: &BTreeSet<ObjectId>) -> Self {
        Self {
            dirty_stmts: changed_stmts.clone(),
            dirty_clusters: BTreeSet::new(),
            dirty_schemas: BTreeSet::new(),
        }
    }

    pub fn sizes(&self) -> (usize, usize, usize) {
        (
            self.dirty_stmts.len(),
            self.dirty_clusters.len(),
            self.dirty_schemas.len(),
        )
    }
}

/// Compute dirty objects, clusters, and schemas using fixed-point iteration.
///
/// Implements the Datalog rules defined at the top of this module.
///
/// **Important:** Sinks are special - they do NOT propagate dirtiness to clusters or schemas.
/// Sinks write to external systems and are created after the swap during apply, so they
/// shouldn't cause other objects to be redeployed.
pub(super) fn compute_dirty_datalog(
    changed_stmts: &BTreeSet<ObjectId>,
    base_facts: &BaseFacts,
    changed_replacements: &BTreeSet<ObjectId>,
) -> (
    BTreeSet<ObjectId>,
    BTreeSet<Cluster>,
    BTreeSet<SchemaQualifier>,
) {
    log_datalog_start(changed_stmts, base_facts);
    let indexes = DatalogIndexes::from_base_facts(base_facts);
    let mut state = DirtyState::new(changed_stmts);

    // Fixed-point iteration: apply rules until no changes
    let mut iteration = 0;
    loop {
        iteration += 1;
        let prev_sizes = state.sizes();
        log_iteration(iteration, &state);

        apply_cluster_dirtiness_rules(changed_stmts, base_facts, &indexes, &mut state);
        apply_stmt_dirtiness_from_clusters(&indexes, &mut state);
        apply_stmt_dependency_rules(changed_replacements, &indexes, &mut state);
        apply_schema_dirtiness_rules(base_facts, &indexes, &mut state);
        apply_stmt_dirtiness_from_schemas(base_facts, &indexes, &mut state);

        // Fixed point reached when no sets grew
        if state.sizes() == prev_sizes {
            verbose!(
                "\n{} Fixed point reached after {} iteration(s)",
                "✓".green(),
                iteration.to_string().bold()
            );
            break;
        }
    }

    log_final_results(&state);

    // Convert cluster names to Cluster structs
    let dirty_cluster_structs = state.dirty_clusters.into_iter().map(Cluster::new).collect();

    (
        state.dirty_stmts,
        dirty_cluster_structs,
        state.dirty_schemas,
    )
}

/// Applies cluster dirtiness rules that originate only from changed statements.
///
/// This intentionally excludes sink propagation so sink changes do not force
/// unrelated cluster redeployments.
///
/// ```datalog
/// DirtyCluster(C) :- ChangedStmt(O), StmtUsesCluster(O, C), NOT IsSink(O)
/// DirtyCluster(C) :- ChangedStmt(O), IndexUsesCluster(O, _, C), NOT IsSink(O)
/// ```
fn apply_cluster_dirtiness_rules(
    changed_stmts: &BTreeSet<ObjectId>,
    base_facts: &BaseFacts,
    indexes: &DatalogIndexes,
    state: &mut DirtyState,
) {
    for obj in changed_stmts {
        if base_facts.is_sink.contains(obj) {
            verbose!(
                "  ├─ {}: {} is a sink, not marking clusters dirty",
                "SKIP".yellow().bold(),
                obj.to_string().cyan()
            );
            continue;
        }
        // Rule: DirtyCluster(C) :- ChangedStmt(O), StmtUsesCluster(O, C), NOT IsSink(O)
        if let Some(clusters) = indexes.stmt_to_clusters.get(obj) {
            for cluster in clusters {
                if state.dirty_clusters.insert(cluster.clone()) {
                    verbose!(
                        "  ├─ {}: DirtyCluster({}) ← ChangedStmt({}) uses cluster",
                        "Rule 1".bold(),
                        cluster.magenta(),
                        obj.to_string().cyan()
                    );
                }
            }
        }
        // Rule: DirtyCluster(C) :- ChangedStmt(O), IndexUsesCluster(O, _, C), NOT IsSink(O)
        if let Some(clusters) = indexes.index_to_clusters.get(obj) {
            for cluster in clusters {
                if state.dirty_clusters.insert(cluster.clone()) {
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
}

/// Applies statement dirtiness induced by dirty clusters.
///
/// ```datalog
/// DirtyStmt(O) :- StmtUsesCluster(O, C), DirtyCluster(C)
/// ```
fn apply_stmt_dirtiness_from_clusters(indexes: &DatalogIndexes, state: &mut DirtyState) {
    for (obj, clusters) in &indexes.stmt_to_clusters {
        for cluster in clusters {
            if state.dirty_clusters.contains(cluster) && state.dirty_stmts.insert(obj.clone()) {
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
}

/// Applies downstream dependency propagation for dirty statements.
///
/// Changed replacement MVs are excluded so in-place replacement does not fan out
/// to dependents that do not require redeployment.
///
/// ```datalog
/// DirtyStmt(O) :- DependsOn(O, P), DirtyStmt(P), NOT IsChangedReplacement(P)
/// ```
fn apply_stmt_dependency_rules(
    changed_replacements: &BTreeSet<ObjectId>,
    indexes: &DatalogIndexes,
    state: &mut DirtyState,
) {
    let current_dirty: Vec<_> = state.dirty_stmts.iter().cloned().collect();
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
                if state.dirty_stmts.insert(child.clone()) {
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
}

/// Marks schemas dirty from dirty statements eligible for schema-level redeploy.
///
/// Sinks and replacement MVs are excluded to preserve stage/apply semantics.
///
/// ```datalog
/// DirtySchema(Db, Sch) :- DirtyStmt(O), ObjectInSchema(O, Db, Sch),
///                          NOT IsSink(O), NOT IsReplacement(O)
/// ```
fn apply_schema_dirtiness_rules(
    base_facts: &BaseFacts,
    indexes: &DatalogIndexes,
    state: &mut DirtyState,
) {
    for obj in &state.dirty_stmts {
        if base_facts.is_sink.contains(obj) {
            verbose!(
                "  ├─ {}: {} is a sink, not marking schema dirty",
                "SKIP".yellow().bold(),
                obj.to_string().cyan()
            );
            continue;
        }
        if base_facts.is_replacement.contains(obj) {
            verbose!(
                "  ├─ {}: {} is a replacement MV, not marking schema dirty",
                "SKIP".yellow().bold(),
                obj.to_string().cyan()
            );
            continue;
        }
        if let Some(sq) = indexes.object_to_schema.get(obj)
            && state
                .dirty_schemas
                .insert(SchemaQualifier::new(sq.database.clone(), sq.schema.clone()))
        {
            verbose!(
                "  ├─ {}: DirtySchema({}) ← DirtyStmt({}) in schema",
                "Rule 5".bold(),
                format!("{}.{}", sq.database, sq.schema).blue(),
                obj.to_string().cyan()
            );
        }
    }
}

/// Pulls statements into the dirty set when their schema is marked dirty.
///
/// ```datalog
/// DirtyStmt(O) :- DirtySchema(Db, Sch), ObjectInSchema(O, Db, Sch),
///                  NOT IsReplacement(O)
/// ```
fn apply_stmt_dirtiness_from_schemas(
    base_facts: &BaseFacts,
    indexes: &DatalogIndexes,
    state: &mut DirtyState,
) {
    for (obj, sq) in &indexes.object_to_schema {
        let schema_is_dirty = state
            .dirty_schemas
            .iter()
            .any(|dirty| dirty.database == sq.database && dirty.schema == sq.schema);
        if !schema_is_dirty || base_facts.is_replacement.contains(obj) {
            continue;
        }
        if state.dirty_stmts.insert(obj.clone()) {
            verbose!(
                "  ├─ {}: DirtyStmt({}) ← in DirtySchema({})",
                "Rule 6".bold(),
                obj.to_string().cyan(),
                format!("{}.{}", sq.database, sq.schema).blue()
            );
        }
    }
}
