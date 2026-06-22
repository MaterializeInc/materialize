// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Datalog-style fixed-point computation of dirty objects, clusters, and schemas.
//!
//! Implements the propagation rules documented in the parent module's header.
//! The evaluator is organized as:
//!
//! 1. immutable input facts (`BaseFacts`)
//! 2. precomputed join indexes and helper relations (`FactIndexes`)
//! 3. mutable derived relations (`DirtyState`)
//! 4. rule-group evaluators that collect pending derivations before merging
//!
//! Each `derive_*` helper corresponds to one or more Datalog rules and is
//! annotated with the exact rule it encodes.
//!
//! ## Algorithm
//!
//! 1. **Seed:** Initialize `dirty_stmts` from `changed_stmts` (objects whose
//!    hashes differ between the old and new snapshots).
//! 2. **Build indexes:** Pre-compute reverse lookup maps and the current
//!    `ClusterBoundary` relation from base facts for O(1) rule evaluation.
//! 3. **Fixed-point loop:** In each iteration, apply all five rule groups in
//!    a fixed order:
//!    1. Cluster dirtiness (from changed statements only, within `ClusterBoundary`)
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
//!
//! `ClusterBoundary` is materialized as all clusters referenced by statements
//! or indexes in the project.

use super::base_facts::BaseFacts;
use super::logging::{log_datalog_start, log_final_results, log_iteration};
use crate::project::SchemaQualifier;
use crate::project::ast::Cluster;
use crate::project::ir::object_id::ObjectId;
use crate::verbose;
use owo_colors::{OwoColorize, Stream, Style};
use std::collections::{BTreeMap, BTreeSet};

/// Pre-computed indexes for efficient Datalog rule evaluation.
pub(super) struct FactIndexes {
    /// Object -> clusters used by the statement
    stmt_to_clusters: BTreeMap<ObjectId, Vec<Cluster>>,
    /// Object -> clusters used by indexes on that object
    index_to_clusters: BTreeMap<ObjectId, Vec<Cluster>>,
    /// Parent -> list of dependent children (reverse of depends_on)
    dependents: BTreeMap<ObjectId, Vec<ObjectId>>,
    /// Object -> schema qualifier it belongs to
    object_to_schema: BTreeMap<ObjectId, SchemaQualifier>,
    /// Schema qualifier -> objects in that schema
    schema_to_objects: BTreeMap<SchemaQualifier, Vec<ObjectId>>,
    /// Clusters eligible to become `DirtyCluster` in the current evaluation
    cluster_boundary: BTreeSet<Cluster>,
}

impl FactIndexes {
    pub(super) fn from_base_facts(facts: &BaseFacts) -> Self {
        // stmt_to_clusters: group by object
        let mut stmt_to_clusters: BTreeMap<ObjectId, Vec<Cluster>> = BTreeMap::new();
        for (obj, cluster) in &facts.stmt_uses_cluster {
            stmt_to_clusters
                .entry(obj.clone())
                .or_default()
                .push(cluster.clone());
        }

        // index_to_clusters: group by object (ignoring index name)
        let mut index_to_clusters: BTreeMap<ObjectId, Vec<Cluster>> = BTreeMap::new();
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

        // object_to_schema / schema_to_objects: direct mappings
        let mut object_to_schema = BTreeMap::new();
        let mut schema_to_objects: BTreeMap<SchemaQualifier, Vec<ObjectId>> = BTreeMap::new();
        for (obj, db, sch) in &facts.object_in_schema {
            let qualifier = SchemaQualifier::new(db.clone(), sch.clone());
            object_to_schema.insert(obj.clone(), qualifier.clone());
            schema_to_objects
                .entry(qualifier)
                .or_default()
                .push(obj.clone());
        }

        let cluster_boundary = stmt_to_clusters
            .values()
            .flat_map(|clusters| clusters.iter().cloned())
            .chain(
                index_to_clusters
                    .values()
                    .flat_map(|clusters| clusters.iter().cloned()),
            )
            .collect();

        FactIndexes {
            stmt_to_clusters,
            index_to_clusters,
            dependents,
            object_to_schema,
            schema_to_objects,
            cluster_boundary,
        }
    }
}

/// Mutable working sets carried across fixed-point iterations.
///
/// Keeps the Datalog loop state explicit and grouped so each rule helper can
/// update shared dirtiness sets without additional tuple plumbing.
pub(super) struct DirtyState {
    pub dirty_stmts: BTreeSet<ObjectId>,
    pub dirty_clusters: BTreeSet<Cluster>,
    pub dirty_schemas: BTreeSet<SchemaQualifier>,
}

impl DirtyState {
    /// Seeds dirty state from objects whose hashes changed between snapshots.
    pub(super) fn new(changed_stmts: &BTreeSet<ObjectId>) -> Self {
        Self {
            dirty_stmts: changed_stmts.clone(),
            dirty_clusters: BTreeSet::new(),
            dirty_schemas: BTreeSet::new(),
        }
    }

    pub(super) fn sizes(&self) -> (usize, usize, usize) {
        (
            self.dirty_stmts.len(),
            self.dirty_clusters.len(),
            self.dirty_schemas.len(),
        )
    }
}

/// Newly derived facts from one rule group before they are merged into `DirtyState`.
#[derive(Default)]
struct PendingFacts {
    dirty_stmts: BTreeSet<ObjectId>,
    dirty_clusters: BTreeSet<Cluster>,
    dirty_schemas: BTreeSet<SchemaQualifier>,
}

impl PendingFacts {
    fn merge_into(self, state: &mut DirtyState) {
        state.dirty_stmts.extend(self.dirty_stmts);
        state.dirty_clusters.extend(self.dirty_clusters);
        state.dirty_schemas.extend(self.dirty_schemas);
    }
}

/// Fixed-point evaluator for the dirty propagation rules.
struct Evaluator<'a> {
    changed_stmts: &'a BTreeSet<ObjectId>,
    base_facts: &'a BaseFacts,
    changed_replacements: &'a BTreeSet<ObjectId>,
    indexes: FactIndexes,
}

impl<'a> Evaluator<'a> {
    fn new(
        changed_stmts: &'a BTreeSet<ObjectId>,
        base_facts: &'a BaseFacts,
        changed_replacements: &'a BTreeSet<ObjectId>,
    ) -> Self {
        Self {
            changed_stmts,
            base_facts,
            changed_replacements,
            indexes: FactIndexes::from_base_facts(base_facts),
        }
    }

    fn run(&self) -> DirtyState {
        let mut state = DirtyState::new(self.changed_stmts);

        let mut iteration = 0;
        loop {
            iteration += 1;
            let prev_sizes = state.sizes();
            log_iteration(iteration, &state);

            self.apply_rule_group(&mut state, Self::derive_cluster_dirtiness);
            self.apply_rule_group(&mut state, Self::derive_stmt_dirtiness_from_clusters);
            self.apply_rule_group(&mut state, Self::derive_stmt_dependency_dirtiness);
            self.apply_rule_group(&mut state, Self::derive_schema_dirtiness);
            self.apply_rule_group(&mut state, Self::derive_stmt_dirtiness_from_schemas);

            // Fixed point reached when no sets grew
            if state.sizes() == prev_sizes {
                verbose!(
                    "\n{} Fixed point reached after {} iteration(s)",
                    "✓".if_supports_color(Stream::Stderr, |t| t.green()),
                    iteration
                        .to_string()
                        .if_supports_color(Stream::Stderr, |t| t.bold())
                );
                break;
            }
        }

        state
    }

    fn apply_rule_group(
        &self,
        state: &mut DirtyState,
        derive: fn(&Self, &DirtyState, &mut PendingFacts),
    ) {
        let mut pending = PendingFacts::default();
        derive(self, state, &mut pending);
        pending.merge_into(state);
    }

    /// Applies cluster dirtiness rules that originate only from changed statements.
    ///
    /// This intentionally excludes sink propagation so sink changes do not force
    /// unrelated cluster redeployments.
    ///
    /// ```datalog
    /// DirtyCluster(C) :- ChangedStmt(O), StmtUsesCluster(O, C),
    ///                    NOT IsSink(O), ClusterBoundary(C)
    /// DirtyCluster(C) :- ChangedStmt(O), IndexUsesCluster(O, _, C),
    ///                    NOT IsSink(O), ClusterBoundary(C)
    /// ```
    fn derive_cluster_dirtiness(&self, state: &DirtyState, pending: &mut PendingFacts) {
        for obj in self.changed_stmts {
            if self.base_facts.is_sink.contains(obj) {
                let skip_style = Style::new().yellow().bold();
                verbose!(
                    "  ├─ {}: {} is a sink, not marking clusters dirty",
                    "SKIP".if_supports_color(Stream::Stderr, |t| skip_style.style(t)),
                    obj.to_string()
                        .if_supports_color(Stream::Stderr, |t| t.cyan())
                );
                continue;
            }

            if let Some(clusters) = self.indexes.stmt_to_clusters.get(obj) {
                for cluster in clusters {
                    if self.indexes.cluster_boundary.contains(cluster)
                        && !state.dirty_clusters.contains(cluster)
                        && pending.dirty_clusters.insert(cluster.clone())
                    {
                        verbose!(
                            "  ├─ {}: DirtyCluster({}) ← ChangedStmt({}) uses cluster",
                            "Rule 1".if_supports_color(Stream::Stderr, |t| t.bold()),
                            cluster.if_supports_color(Stream::Stderr, |t| t.magenta()),
                            obj.to_string()
                                .if_supports_color(Stream::Stderr, |t| t.cyan())
                        );
                    }
                }
            }

            if let Some(clusters) = self.indexes.index_to_clusters.get(obj) {
                for cluster in clusters {
                    if self.indexes.cluster_boundary.contains(cluster)
                        && !state.dirty_clusters.contains(cluster)
                        && pending.dirty_clusters.insert(cluster.clone())
                    {
                        verbose!(
                            "  ├─ {}: DirtyCluster({}) ← ChangedStmt({}) has index on cluster",
                            "Rule 2".if_supports_color(Stream::Stderr, |t| t.bold()),
                            cluster.if_supports_color(Stream::Stderr, |t| t.magenta()),
                            obj.to_string()
                                .if_supports_color(Stream::Stderr, |t| t.cyan())
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
    fn derive_stmt_dirtiness_from_clusters(&self, state: &DirtyState, pending: &mut PendingFacts) {
        for (obj, clusters) in &self.indexes.stmt_to_clusters {
            for cluster in clusters {
                if state.dirty_clusters.contains(cluster)
                    && !state.dirty_stmts.contains(obj)
                    && pending.dirty_stmts.insert(obj.clone())
                {
                    verbose!(
                        "  ├─ {}: DirtyStmt({}) ← uses DirtyCluster({})",
                        "Rule 3".if_supports_color(Stream::Stderr, |t| t.bold()),
                        obj.to_string()
                            .if_supports_color(Stream::Stderr, |t| t.cyan()),
                        cluster.if_supports_color(Stream::Stderr, |t| t.magenta())
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
    fn derive_stmt_dependency_dirtiness(&self, state: &DirtyState, pending: &mut PendingFacts) {
        for dirty_obj in &state.dirty_stmts {
            if self.changed_replacements.contains(dirty_obj) {
                let skip_style = Style::new().yellow().bold();
                verbose!(
                    "  ├─ {}: {} is a changed replacement MV, not propagating to dependents",
                    "SKIP".if_supports_color(Stream::Stderr, |t| skip_style.style(t)),
                    dirty_obj
                        .to_string()
                        .if_supports_color(Stream::Stderr, |t| t.cyan())
                );
                continue;
            }
            if let Some(children) = self.indexes.dependents.get(dirty_obj) {
                for child in children {
                    if !state.dirty_stmts.contains(child)
                        && pending.dirty_stmts.insert(child.clone())
                    {
                        verbose!(
                            "  ├─ {}: DirtyStmt({}) ← depends on DirtyStmt({})",
                            "Rule 4".if_supports_color(Stream::Stderr, |t| t.bold()),
                            child
                                .to_string()
                                .if_supports_color(Stream::Stderr, |t| t.cyan()),
                            dirty_obj
                                .to_string()
                                .if_supports_color(Stream::Stderr, |t| t.cyan())
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
    fn derive_schema_dirtiness(&self, state: &DirtyState, pending: &mut PendingFacts) {
        for obj in &state.dirty_stmts {
            if self.base_facts.is_sink.contains(obj) {
                let skip_style = Style::new().yellow().bold();
                verbose!(
                    "  ├─ {}: {} is a sink, not marking schema dirty",
                    "SKIP".if_supports_color(Stream::Stderr, |t| skip_style.style(t)),
                    obj.to_string()
                        .if_supports_color(Stream::Stderr, |t| t.cyan())
                );
                continue;
            }
            if self.base_facts.is_replacement.contains(obj) {
                let skip_style = Style::new().yellow().bold();
                verbose!(
                    "  ├─ {}: {} is a replacement MV, not marking schema dirty",
                    "SKIP".if_supports_color(Stream::Stderr, |t| skip_style.style(t)),
                    obj.to_string()
                        .if_supports_color(Stream::Stderr, |t| t.cyan())
                );
                continue;
            }
            if let Some(sq) = self.indexes.object_to_schema.get(obj)
                && !state.dirty_schemas.contains(sq)
                && pending.dirty_schemas.insert(sq.clone())
            {
                verbose!(
                    "  ├─ {}: DirtySchema({}) ← DirtyStmt({}) in schema",
                    "Rule 5".if_supports_color(Stream::Stderr, |t| t.bold()),
                    format!("{}.{}", sq.database, sq.schema)
                        .if_supports_color(Stream::Stderr, |t| t.blue()),
                    obj.to_string()
                        .if_supports_color(Stream::Stderr, |t| t.cyan())
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
    fn derive_stmt_dirtiness_from_schemas(&self, state: &DirtyState, pending: &mut PendingFacts) {
        for dirty_schema in &state.dirty_schemas {
            if let Some(objects) = self.indexes.schema_to_objects.get(dirty_schema) {
                for obj in objects {
                    if self.base_facts.is_replacement.contains(obj)
                        || state.dirty_stmts.contains(obj)
                        || !pending.dirty_stmts.insert(obj.clone())
                    {
                        continue;
                    }
                    verbose!(
                        "  ├─ {}: DirtyStmt({}) ← in DirtySchema({})",
                        "Rule 6".if_supports_color(Stream::Stderr, |t| t.bold()),
                        obj.to_string()
                            .if_supports_color(Stream::Stderr, |t| t.cyan()),
                        format!("{}.{}", dirty_schema.database, dirty_schema.schema)
                            .if_supports_color(Stream::Stderr, |t| t.blue())
                    );
                }
            }
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
    let evaluator = Evaluator::new(changed_stmts, base_facts, changed_replacements);
    let state = evaluator.run();

    log_final_results(&state);
    (state.dirty_stmts, state.dirty_clusters, state.dirty_schemas)
}
