// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The scalar e-graph: an instance of the generic `core::EGraph<L>`.
//!
//! SP2a deleted the standalone scalar substrate; `ScalarEGraph` is now a type
//! alias for `EGraph<ScalarLang>`. This file keeps the scalar-specific driver
//! (`saturate`), the per-rebuild analysis fixpoint (`recompute_analysis`), the
//! growth budget, and the inherent analysis/col_types accessors. The hash-cons,
//! union-find, and congruence closure come from `crate::eqsat::core`.
//!
//! Per-class analysis is maintained two ways, exactly as before: incrementally
//! by `ScalarLang`'s `on_add`/`on_union` hooks (currency during rule
//! application) and, after each congruence closure, by `recompute_analysis` (the
//! monotone least-fixpoint that makes constant-folding's self-referential
//! classes sound).

use mz_repr::ReprColumnType;

use crate::eqsat::core::EGraph;
use crate::eqsat::scalar::analysis::{self, ClassAnalysis};
use crate::eqsat::scalar::lang::ScalarLang;
use crate::eqsat::scalar::node::SNode;
use crate::eqsat::scalar::rules;

pub use crate::eqsat::core::Id;

/// The scalar e-graph: the generic core specialized to the scalar language.
pub type ScalarEGraph = EGraph<ScalarLang>;

/// E-node budget for the saturation loop, mirroring the relational engine.
/// Saturation stops growing once the total e-node count crosses this; extraction
/// from a partially saturated graph is still sound.
const MAX_ENODES: usize = 600;

/// Per-iteration work-item cap (one (class id, e-node) snapshot per item).
/// Remaining nodes are visited next iteration, which starts from a rebuilt graph.
const MATCH_LIMIT: usize = 1_000;

/// Maximum saturation iterations. A hard stop against non-terminating rule
/// interactions; terminating rule sets reach a fixpoint well under this.
const MAX_ITERS: usize = 100;

impl EGraph<ScalarLang> {
    /// The analysis value for `id`'s canonical class.
    ///
    /// CAVEAT: the value reflects the *incremental* analysis maintained by the
    /// `ScalarLang` `on_add`/`on_union` hooks. The core's `rebuild` restores
    /// congruence but does NOT recompute analysis, so after a bare `rebuild` this
    /// is only the incremental result — which can under-approximate `could_error`
    /// for the self-referential classes constant folding produces. The
    /// conservative least-fixpoint is restored by [`recompute_analysis`], which
    /// [`saturate`] runs after every `rebuild`. Read this only on a graph that has
    /// been through `saturate`/`recompute_analysis`, except where the incremental
    /// result is provably sufficient (no self-references, e.g. a single `union`
    /// of self-reference-free classes).
    ///
    /// Panics if `id` has no analysis entry (it was never returned by `add`/the
    /// hooks have not populated it).
    pub(crate) fn analysis(&self, id: Id) -> &ClassAnalysis {
        let rep = self.find(id);
        self.data()
            .analysis
            .get(&rep)
            .expect("class must have an analysis entry; was the id produced by this e-graph?")
    }

    /// The column types this expression is evaluated against. Empty when the
    /// e-graph was built without types (only the type-agnostic rules rely on it).
    pub(crate) fn col_types(&self) -> &[ReprColumnType] {
        &self.data().col_types
    }
}

/// Recompute every class's analysis as a monotone least-fixpoint over the current
/// class layout. Lifted from the recompute tail of the old `ScalarEGraph::rebuild`.
///
/// Seed every class to the merge identity, then repeatedly recompute each class as
/// the merge over its nodes' `make` contributions — reading the current (possibly
/// still-seed) child values — until a pass changes nothing. Pre-seeding makes
/// self-referential classes (from constant folding) sound: `make` always finds its
/// children present, and a self-referential child reads the class's current value
/// rather than being dropped. The lattice is finite and `make`/`merge` are
/// monotone (`could_error` only rises, `literal` only goes None→Some), so this
/// converges to the conservative upper bound the analysis contract requires.
///
/// This must run after each `rebuild` (see [`saturate`]): the core's `rebuild`
/// only restores congruence and fires the incremental `on_union` hook, so without
/// this pass a class formed by constant folding could carry an under-approximated
/// `could_error` — the one unsound direction for a guard that blocks
/// error-unsafe rewrites.
pub fn recompute_analysis(eg: &mut EGraph<ScalarLang>) {
    let all_ids = eg.class_ids();
    eg.data_mut().analysis.clear();
    for &id in &all_ids {
        eg.data_mut().analysis.insert(
            id,
            ClassAnalysis {
                could_error: false,
                literal: None,
            },
        );
    }
    loop {
        let mut changed = false;
        for &id in &all_ids {
            let nodes = eg.nodes(id);
            // A live class always holds at least one node.
            debug_assert!(
                !nodes.is_empty(),
                "recompute_analysis: class {id} has no nodes"
            );
            let mut acc = ClassAnalysis {
                could_error: false,
                literal: None,
            };
            for node in &nodes {
                let node_a = {
                    let store = &eg.data().analysis;
                    let find = |c| eg.find(c);
                    analysis::make(node, store, &find)
                };
                acc = analysis::merge(acc, node_a);
            }
            // Read the current value into locals so the immutable borrow ends
            // before the mutable `data_mut()` insert below.
            let (cur_err, cur_has_lit) = {
                let cur = eg.data().analysis.get(&id).expect("class seeded above");
                (cur.could_error, cur.literal.is_some())
            };
            // Both fields are monotone, so an inequality is always an increase.
            // Comparing `literal` by presence suffices: equal classes carry the
            // same literal.
            if cur_err != acc.could_error || cur_has_lit != acc.literal.is_some() {
                changed = true;
                eg.data_mut().analysis.insert(id, acc);
            }
        }
        if !changed {
            break;
        }
    }
}

/// Saturate the e-graph to a fixpoint, applying every rule wherever it matches.
///
/// Each iteration restores congruence (`rebuild`), recomputes the analysis
/// fixpoint (`recompute_analysis`), then snapshots all (class, node) pairs
/// read-only (bounded by `MATCH_LIMIT`) and applies rules with mutable access so
/// rules can build nested intermediate nodes. Breaks on fixpoint (no new unions)
/// or when the e-node budget is exceeded. Returns the number of iterations run.
pub fn saturate(eg: &mut EGraph<ScalarLang>) -> usize {
    let mut iters = 0;
    for _ in 0..MAX_ITERS {
        iters += 1;
        eg.rebuild();
        recompute_analysis(eg);
        // Bound runaway growth: stop and extract from what we have (sound).
        if eg.node_count() > MAX_ENODES {
            break;
        }

        // Read-only snapshot of (class id, node), bounded by MATCH_LIMIT.
        let mut work: Vec<(Id, SNode)> = Vec::new();
        'collect: for id in eg.class_ids() {
            for node in eg.nodes(id) {
                work.push((id, node));
                if work.len() >= MATCH_LIMIT {
                    break 'collect;
                }
            }
        }
        // Apply: rules may now mutate (add nested nodes). Union each returned
        // class id into the source class.
        let mut changed = false;
        for (id, node) in work {
            for rule in rules::rules() {
                for new_id in rule(eg, &node) {
                    if eg.union(id, new_id) {
                        changed = true;
                    }
                }
            }
        }

        if !changed {
            break;
        }
    }
    iters
}
