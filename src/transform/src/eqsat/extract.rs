// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Pluggable plan extractors.
//!
//! An [`Extractor`] reconstructs the chosen [`Rel`] from a saturated e-graph
//! under an [`Objective`]. [`GreedyExtractor`] is the default bottom-up
//! dynamic program; [`IlpExtractor`] (a 0/1 program) optimizes the
//! non-compositional set-cardinality objective the greedy form cannot.

use std::collections::BTreeMap;

use mz_ore::collections::HashMap;

use crate::analysis::equivalences::EquivalenceClasses;
use crate::eqsat::colored_derive::ColoredLayer;
use crate::eqsat::cost::CostModel;
use crate::eqsat::egraph::{EGraph, ENode, Id};
use crate::eqsat::ir::Rel;
use crate::eqsat::objective::Objective;

/// Count of ILP-to-greedy fallbacks this process, by reason. Read by the
/// solve-time measurement and the fallback test. Not silent: every fallback also
/// emits a debug log via [`record_ilp_fallback`].
pub(crate) static ILP_FALLBACKS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Record one ILP-to-greedy fallback with its reason. Bumps [`ILP_FALLBACKS`]
/// and emits a debug log so a fallback is never silent.
fn record_ilp_fallback(reason: &str) {
    ILP_FALLBACKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    tracing::debug!(reason, "ilp extractor fell back to greedy");
}

/// Reconstructs the best [`Rel`] for `root` from a saturated e-graph under an
/// [`Objective`]. Returns `None` only when no representative can be built (the
/// caller then falls back to the un-optimized fragment, always sound).
///
/// `colored` is the optional colored-layer used for color-aware equality
/// resolution (SP4b); `None` reproduces today's behavior.
pub(crate) trait Extractor: std::fmt::Debug {
    fn extract(
        &self,
        egraph: &EGraph,
        root: Id,
        model: &CostModel,
        objective: &dyn Objective,
        colored: Option<&mut ColoredLayer<'_>>,
        spellings: Option<&HashMap<Id, EquivalenceClasses>>,
    ) -> Option<Rel>;
}

/// The default extractor: greedy bottom-up dynamic programming. It re-costs
/// each candidate subplan and keeps the cheapest per e-class under the
/// objective. Sound for any objective, but for a non-compositional objective
/// (set-cardinality arrangement sharing) it finds a local, not global, optimum.
#[derive(Debug, Clone)]
pub struct GreedyExtractor;

impl Extractor for GreedyExtractor {
    fn extract(
        &self,
        egraph: &EGraph,
        root: Id,
        model: &CostModel,
        objective: &dyn Objective,
        colored: Option<&mut ColoredLayer<'_>>,
        spellings: Option<&HashMap<Id, EquivalenceClasses>>,
    ) -> Option<Rel> {
        egraph.extract_with(root, model, objective, colored, spellings)
    }
}

/// Extracts by solving a 0/1 program that minimizes the count of distinct
/// `(class, key)` arrangements not covered by the oracle. The set-cardinality
/// objective is non-compositional, so greedy bottom-up extraction cannot
/// optimize it; this can.
///
/// microlp is early-stage; any solver failure falls back to [`GreedyExtractor`],
/// which is always sound.
#[derive(Debug, Clone, Default)]
pub struct IlpExtractor {
    /// When set, weight the node tier by each selected node's direct scalar-payload
    /// count, mirroring cost.rs's scalar-aware node count. Gated behind
    /// `enable_eqsat_filter_sharing`. When false the objective is byte-identical
    /// to the flat-node-tier form, so the corpus is unaffected.
    pub weight_scalar_nodes: bool,
    /// When set, add an arity tier to the objective ranked below the arrangement
    /// count and above time, so a widening carry (wider arranged collection) is
    /// declined. Gated behind enable_eqsat_scalar_sharing. When false the
    /// objective is byte-identical to today.
    pub width_aware: bool,
}

impl Extractor for IlpExtractor {
    fn extract(
        &self,
        egraph: &EGraph,
        root: Id,
        model: &CostModel,
        objective: &dyn Objective,
        colored: Option<&mut ColoredLayer<'_>>,
        spellings: Option<&HashMap<Id, EquivalenceClasses>>,
    ) -> Option<Rel> {
        // The objective drives the arrangement-count primary term in `solve`;
        // a time-degree tier then a node-count tier break ties beneath it.
        match self.solve(egraph, root, model, spellings) {
            Some(rel) => Some(rel),
            // microlp is early-stage; any solver failure falls back to greedy,
            // which is always sound. Forward the colored layer to the fallback.
            None => GreedyExtractor.extract(egraph, root, model, objective, colored, spellings),
        }
    }
}

impl IlpExtractor {
    /// Build and solve the 0/1 extraction program. Returns the extracted plan
    /// on success, or `None` to signal fallback to [`GreedyExtractor`].
    fn solve(
        &self,
        egraph: &EGraph,
        root: Id,
        model: &CostModel,
        spellings: Option<&HashMap<Id, EquivalenceClasses>>,
    ) -> Option<Rel> {
        use good_lp::{ProblemVariables, Solution, SolverModel, constraint, variable};

        // Collect every class and its e-nodes reachable from root.
        let reachable = egraph.reachable(root);

        // Model size cap: if the reachable subgraph is too large, defer to greedy.
        // MAX_ENODES is already capped upstream, but be defensive here too.
        let total_nodes: usize = reachable.values().map(|v| v.len()).sum();
        if total_nodes > 600 {
            record_ilp_fallback("size_cap");
            return None;
        }

        // A cyclic reachable subgraph (join commutativity is inherently cyclic) is
        // handled directly: the subtour-elimination cuts below encode acyclicity
        // of the SELECTED subgraph, so the program only ever admits a finite DAG
        // plan and never needs to bail on the cycle.

        // Assign a stable index to every (class, node) pair and every distinct
        // (class, key) arrangement.
        //
        // `node_idx[(class, node_pos)]` -> variable index into `node_sel`.
        // `arr_idx[(class, key_cols)]` -> variable index into `arr_sel`.
        let mut node_vars: Vec<(Id, ENode)> = Vec::new();
        // Map from (class_id, node position in class) to node_vars index.
        let mut node_idx: BTreeMap<(Id, usize), usize> = BTreeMap::new();

        for (&class, nodes) in &reachable {
            for (pos, node) in nodes.iter().enumerate() {
                node_idx.insert((class, pos), node_vars.len());
                node_vars.push((class, node.clone()));
            }
        }

        // Collect distinct (arranged_class, key_cols) pairs across all nodes.
        // Also track whether each arrangement originates from an ArrangeBy node:
        // only ArrangeBy arrangements can be oracle-suppressed, matching the cost
        // model (see cost.rs::collect_memory_into, which only suppresses ArrangeBy,
        // not Reduce/TopK group-key arrangements).
        let mut arr_set: std::collections::BTreeSet<(Id, Vec<usize>)> =
            std::collections::BTreeSet::new();
        let mut arr_from_arrange_by: BTreeMap<(Id, Vec<usize>), bool> = BTreeMap::new();
        for (_, node) in &node_vars {
            let from_ab = matches!(node, ENode::ArrangeBy { .. } | ENode::ArrangeByMany { .. });
            for (arr_class, key_cols) in egraph.arrangements_of(node) {
                let key = (arr_class, key_cols);
                // If any node contributing this arrangement is an ArrangeBy, mark it.
                arr_from_arrange_by
                    .entry(key.clone())
                    .and_modify(|v| *v |= from_ab)
                    .or_insert(from_ab);
                arr_set.insert(key);
            }
        }
        let arr_vars: Vec<(Id, Vec<usize>)> = arr_set.into_iter().collect();
        let arr_idx: BTreeMap<(Id, Vec<usize>), usize> = arr_vars
            .iter()
            .enumerate()
            .map(|(i, k)| (k.clone(), i))
            .collect();

        // ILP variable creation.
        let mut vars = ProblemVariables::new();
        // Binary variable per (class, node): 1 if this node is selected.
        let node_sel: Vec<_> = (0..node_vars.len())
            .map(|_| vars.add(variable().binary()))
            .collect();
        // Binary variable per distinct arrangement: 1 if this arrangement is used.
        let arr_sel: Vec<_> = (0..arr_vars.len())
            .map(|_| vars.add(variable().binary()))
            .collect();

        // `class_used[class_id]` binary: 1 iff some node in this class is selected.
        // Keyed by class, pointing into a flat vec of variables.
        let class_order: Vec<Id> = reachable.keys().copied().collect();
        let class_pos: BTreeMap<Id, usize> = class_order
            .iter()
            .enumerate()
            .map(|(i, &c)| (c, i))
            .collect();
        let class_used: Vec<_> = (0..class_order.len())
            .map(|_| vars.add(variable().binary()))
            .collect();

        // Acyclicity of the SELECTED subgraph is enforced by subtour-elimination
        // cuts scoped to non-trivial SCCs of the candidate graph. Only a class on
        // a directed cycle can be selected into an infinite term, so a class in a
        // trivial SCC needs no cut. `cyclic_classes` returns the SCC partition
        // plus the set of classes that sit in a non-trivial SCC.
        //
        // The cut form is specialized to the SCC size. A self-loop (size 1) is
        // forbidden node by node. A size-2 SCC is forbidden by a single
        // mutual-exclusion inequality over its cross-edges. Only size >= 3 falls
        // back to the general big-M MTZ level encoding. The exact cuts at sizes 1
        // and 2 are pure-binary, which microlp solves far faster than the
        // continuous big-M level variables MTZ would use.
        let (scc_of, _scc_size, cyclic) = cyclic_classes(egraph, &reachable);

        // Group the non-trivial-SCC classes by SCC index. Iterating `class_order`
        // (BTreeMap key order) keeps the grouping and every downstream cut
        // deterministic.
        let mut scc_members: BTreeMap<usize, Vec<Id>> = BTreeMap::new();
        for &cls in &class_order {
            if cyclic.contains(&cls) {
                scc_members.entry(scc_of[&cls]).or_default().push(cls);
            }
        }

        // Level variables exist ONLY for size >= 3 SCCs (the MTZ fallback).
        // Size-1 and size-2 SCCs use the pure-binary cuts below and need no level
        // variable and no big-M. Each level is bounded [0, |SCC| - 1] with per-SCC
        // big-M |SCC|, tight, so an unselected edge is never falsely binding.
        let mut level_of: BTreeMap<Id, good_lp::Variable> = BTreeMap::new();
        for members in scc_members.values() {
            if members.len() >= 3 {
                let lvl_max = (members.len() - 1) as f64;
                for &cls in members {
                    level_of.insert(cls, vars.add(variable().min(0.0).max(lvl_max)));
                }
            }
        }

        // Build the objective expression, in strictly separated tiers:
        //   1. PRIMARY: count of non-oracle-covered arrangements (integer steps).
        //   2. ARITY (width-aware only): total arity of the arrangements selected
        //      in tier 1, so among arrangements that tie on count the ILP prefers
        //      the narrower one, declining a widening carry.
        //   3. TIME: total work-term degree of the selected nodes. This is what
        //      lets the ILP prefer an `IndexedFilter` (a bounded lookup, work 0)
        //      over the sibling full-scan `Filter` (work 1), and an operator
        //      sitting above a lookup is charged the lookup's bounded output.
        //      Without it the ILP ties the two forms on arrangements and falls
        //      through to a pure node-count tie-break, which rewards sharing the
        //      full scan over two distinct lookups (the relation_cse regression).
        //   4. NODES: a structural tie-break preferring fewer nodes.
        // The tier weights are scaled to the reachable subgraph so that each tier
        // strictly dominates the next: one arrangement outweighs all arity and
        // time terms, any arity difference outweighs all time and node-count
        // terms, and any time difference outweighs all node-count terms.
        // Arity tier (width-aware): ranked below the arrangement-count primary
        // and above the time tier. Off => not computed and w_time keeps its
        // original 0.5/work_total anchoring, byte-identical.
        let arr_arity: Vec<f64> = arr_vars
            .iter()
            .map(|(arr_class, _)| egraph.try_arity(*arr_class).unwrap_or(1) as f64)
            .collect();
        let arity_total: f64 = arr_arity.iter().sum::<f64>() + 1.0;
        let w_arity = 0.5 / arity_total;

        let best_degrees = best_output_degrees(egraph, &reachable);
        let node_work: Vec<f64> = node_vars
            .iter()
            .map(|(_, node)| node_work_degree(node, egraph, &best_degrees))
            .collect();
        let work_total: f64 = node_work.iter().sum::<f64>() + 1.0;
        let w_time = if self.width_aware {
            0.5 * w_arity / work_total
        } else {
            0.5 / work_total
        };

        let mut obj_expr: good_lp::Expression = good_lp::Expression::from(0.0);
        for (arr_i, (arr_class, arr_key)) in arr_vars.iter().enumerate() {
            // Oracle coverage applies only to ArrangeBy arrangements; the cost
            // model does not suppress Reduce/TopK group-key arrangements.
            let covered = arr_from_arrange_by
                .get(&(*arr_class, arr_key.clone()))
                .copied()
                .unwrap_or(false)
                && is_oracle_covered(egraph, model, *arr_class, arr_key);
            if !covered {
                obj_expr += arr_sel[arr_i];
                if self.width_aware {
                    obj_expr += w_arity * arr_arity[arr_i] * arr_sel[arr_i];
                }
            }
        }
        // Node tier. Flag off runs the ORIGINAL formula verbatim, so the
        // objective is byte-identical to today (same op-order, same solver
        // path). Flag on weights each selected node by its direct scalar-payload
        // count and rescales w_nodes by total node-tier MASS (not node count),
        // so no scalar-laden assignment can let the node tier dominate a genuine
        // time difference.
        if self.weight_scalar_nodes {
            let node_mass: Vec<f64> = node_vars
                .iter()
                .map(|(_, node)| 1.0 + node_scalar_count(node, egraph) as f64)
                .collect();
            let mass_total: f64 = node_mass.iter().sum::<f64>() + 1.0;
            let w_nodes = 0.5 * w_time / mass_total;
            for (vi, nsel) in node_sel.iter().enumerate() {
                obj_expr += (w_time * node_work[vi] + w_nodes * node_mass[vi]) * *nsel;
            }
        } else {
            // ORIGINAL, untouched. Copy verbatim from the pre-edit code.
            let w_nodes = 0.5 * w_time / (node_vars.len() as f64 + 1.0);
            for (vi, nsel) in node_sel.iter().enumerate() {
                obj_expr += (w_time * node_work[vi] + w_nodes) * *nsel;
            }
        }

        let mut lp_model = vars.minimise(obj_expr).using(good_lp::microlp);

        // Constraint 1: root class has exactly one selected node.
        let root_class = egraph.find(root);
        let root_pos = class_pos[&root_class];
        {
            let nodes = &reachable[&root_class];
            let mut sum = good_lp::Expression::from(0.0);
            for (pos, _) in nodes.iter().enumerate() {
                let vi = node_idx[&(root_class, pos)];
                sum += node_sel[vi];
            }
            lp_model = lp_model.with(constraint!(sum == 1));
        }

        // Constraint 2: root class is used.
        lp_model = lp_model.with(constraint!(class_used[root_pos] == 1));

        // Constraint 3: for every class, class_used[c] == sum of node_sel in c.
        for (&class, nodes) in &reachable {
            let cp = class_pos[&class];
            let mut sum = good_lp::Expression::from(0.0);
            for (pos, _) in nodes.iter().enumerate() {
                let vi = node_idx[&(class, pos)];
                sum += node_sel[vi];
            }
            // sum of node_sel in class == class_used[class]
            let cu = class_used[cp];
            lp_model = lp_model.with(constraint!(sum == cu));
        }

        // Constraint 4: a selected node implies each child class is used.
        // For each node n in class p, and each child class c of n:
        //   class_used[c] >= node_sel[(p, n_pos)]
        for (&class, nodes) in &reachable {
            for (pos, node) in nodes.iter().enumerate() {
                let vi = node_idx[&(class, pos)];
                for child in egraph.child_classes(node) {
                    let cp = class_pos[&child];
                    // class_used[cp] >= node_sel[vi]
                    lp_model = lp_model.with(constraint!(class_used[cp] >= node_sel[vi]));
                }
            }
        }

        // Constraint 5: a selected node implies its required arrangement indicators.
        // For each node n in class p, and each arrangement (arr_class, arr_key) it needs:
        //   arr_sel[arr_idx[(arr_class, arr_key)]] >= node_sel[(p, n_pos)]
        for (&class, nodes) in &reachable {
            for (pos, node) in nodes.iter().enumerate() {
                let vi = node_idx[&(class, pos)];
                for (arr_class, arr_key) in egraph.arrangements_of(node) {
                    let ai = arr_idx[&(arr_class, arr_key)];
                    // arr_sel[ai] >= node_sel[vi]
                    lp_model = lp_model.with(constraint!(arr_sel[ai] >= node_sel[vi]));
                }
            }
        }

        // Subtour-elimination cuts (exact Dantzig-Fulkerson-Johnson), scoped to
        // non-trivial SCCs and specialized to the SCC size.
        //
        // Cut 1 (self-loop, |S| = 1): for every node whose child class equals its
        // own class, forbid selecting it. A selected self-loop is an infinite
        // term. This is the single self-loop mechanism, a constraint rather than
        // candidate-set exclusion, and it applies regardless of SCC size so a
        // self-loop nested in a larger SCC is covered too. Iterating `reachable`
        // (a BTreeMap) keeps the constraint order deterministic.
        for (&class, nodes) in &reachable {
            for (pos, node) in nodes.iter().enumerate() {
                if egraph.child_classes(node).into_iter().any(|c| c == class) {
                    let vi = node_idx[&(class, pos)];
                    lp_model = lp_model.with(constraint!(node_sel[vi] == 0));
                }
            }
        }

        // Cuts 2 and 3 per non-trivial SCC, by size.
        for members in scc_members.values() {
            match members.len() {
                // Size 1: only a self-loop, already forbidden by cut 1. No
                // multi-class cycle is possible in a singleton component.
                1 => {}
                // Cut 2 (mutual exclusion, |S| = 2). A selected 2-cycle over
                // {A, B} exists iff A's selected node has a child in B AND B's
                // selected node has a child in A. `xa` sums node_sel over EVERY
                // A-node with any child class == B (any node type, via
                // `child_classes`, not just a commute Project), `xb` symmetric.
                // `xa + xb <= 1` forbids exactly that conjunction, together with
                // the exactly-one-node-per-used-class constraint. Sound and
                // complete for the 2-cycle, with no continuous vars and no big-M.
                2 => {
                    let (a, b) = (members[0], members[1]);
                    let mut xa = good_lp::Expression::from(0.0);
                    for (pos, node) in reachable[&a].iter().enumerate() {
                        if egraph.child_classes(node).into_iter().any(|c| c == b) {
                            xa += node_sel[node_idx[&(a, pos)]];
                        }
                    }
                    let mut xb = good_lp::Expression::from(0.0);
                    for (pos, node) in reachable[&b].iter().enumerate() {
                        if egraph.child_classes(node).into_iter().any(|c| c == a) {
                            xb += node_sel[node_idx[&(b, pos)]];
                        }
                    }
                    lp_model = lp_model.with(constraint!(xa + xb <= 1));
                }
                // Cut 3 (MTZ fallback, |S| >= 3). For a within-SCC child edge, a
                // SELECTED parent node forces level[child] + 1 <= level[parent].
                // The big-M is the SCC size, the tight per-SCC bound, so an
                // unselected edge is never falsely binding. This path is dormant
                // corpus-wide (every SCC is size <= 2) but kept as the general
                // acyclicity encoding for a future larger SCC.
                _ => {
                    let m = members.len() as f64;
                    let scc = scc_of[&members[0]];
                    for &class in members {
                        let lp = level_of[&class];
                        for (pos, node) in reachable[&class].iter().enumerate() {
                            let vi = node_idx[&(class, pos)];
                            for child in egraph.child_classes(node) {
                                if scc_of[&child] == scc {
                                    let lc = level_of[&child];
                                    lp_model = lp_model.with(constraint!(
                                        lc + 1.0 <= lp + m * (1.0 - node_sel[vi])
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Solve and read back the solution. microlp may panic or return an error
        // on hard problems; catch either form and fall back to greedy.
        let selected: BTreeMap<Id, ENode> =
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let solution = lp_model.solve().ok()?;
                // Read back the selected node per class.
                // For each class, find the one node_sel that rounds to 1.
                let mut sel: BTreeMap<Id, ENode> = BTreeMap::new();
                for (&class, nodes) in &reachable {
                    for (pos, node) in nodes.iter().enumerate() {
                        let vi = node_idx[&(class, pos)];
                        if solution.value(node_sel[vi]) > 0.5 {
                            sel.insert(class, node.clone());
                            break;
                        }
                    }
                }
                Some(sel)
            })) {
                Ok(Some(sel)) => sel,
                // Solver returned an error or the closure returned None.
                Ok(None) => {
                    record_ilp_fallback("solver_error");
                    return None;
                }
                // microlp panicked; fall back to greedy.
                Err(_) => {
                    record_ilp_fallback("solver_panic");
                    return None;
                }
            };

        // Reconstruct the Rel top-down from root.
        let Some(rel) = Self::build_selected(egraph, root_class, &selected, model, spellings)
        else {
            record_ilp_fallback("build_failed");
            return None;
        };
        // Post-validate polarity: reject any plan where a non-linear operator
        // (Reduce with aggregates, or TopK) has a sign-bearing input. The ILP
        // does not encode polarity constraints, so saturation may have placed a
        // Negate-rooted form in a class that feeds a non-linear reduce. Falling
        // back to greedy is always sound.
        if !validate_polarity(&rel) {
            record_ilp_fallback("polarity");
            return None;
        }
        Some(rel)
    }

    /// Reconstruct a [`Rel`] top-down from `class`, picking the selected e-node
    /// per class as determined by `selected`. Returns `None` if any required
    /// class has no selected node or if reconstruction fails.
    fn build_selected(
        egraph: &EGraph,
        class: Id,
        selected: &BTreeMap<Id, ENode>,
        model: &CostModel,
        spellings: Option<&HashMap<Id, EquivalenceClasses>>,
    ) -> Option<Rel> {
        // Use a stack to avoid recursion on deep plans.
        // We do iterative construction: build bottom-up by post-order.
        // For simplicity and correctness, use recursive helper with a depth guard.
        Self::build_selected_inner(egraph, class, selected, 0, model, spellings)
    }

    fn build_selected_inner(
        egraph: &EGraph,
        class: Id,
        selected: &BTreeMap<Id, ENode>,
        depth: usize,
        model: &CostModel,
        spellings: Option<&HashMap<Id, EquivalenceClasses>>,
    ) -> Option<Rel> {
        // Guard against cycles in pathological e-graphs.
        if depth > 500 {
            return None;
        }
        let node = selected.get(&class)?;
        // For each child class, recursively build the sub-plan.
        let child = |child_id: Id| -> Option<Rel> {
            let canon = egraph.find(child_id);
            Self::build_selected_inner(egraph, canon, selected, depth + 1, model, spellings)
        };
        Some(match node {
            ENode::Constant {
                card,
                arity,
                col_types,
            } => Rel::Constant {
                card: *card,
                arity: *arity,
                col_types: col_types.clone(),
            },
            ENode::Get { name, arity } => Rel::Get {
                name: name.clone(),
                arity: *arity,
            },
            ENode::Opaque(m) => Rel::Opaque(Box::new(m.clone())),
            ENode::LocalGet {
                id,
                arity,
                get,
                version,
            } => Rel::LocalGet {
                id: *id,
                arity: *arity,
                get: get.clone(),
                version: *version,
            },
            ENode::Project { input, outputs } => Rel::Project {
                input: Box::new(child(*input)?),
                outputs: outputs.clone(),
            },
            ENode::Map { input, scalars } => Rel::Map {
                input: Box::new(child(*input)?),
                scalars: egraph.resolve_scalars(scalars),
            },
            ENode::FlatMap { input, func, exprs } => Rel::FlatMap {
                input: Box::new(child(*input)?),
                func: func.clone(),
                exprs: egraph.resolve_scalars(exprs),
            },
            ENode::Filter { input, predicates } => Rel::Filter {
                input: Box::new(child(*input)?),
                predicates: egraph.resolve_scalars(predicates),
            },
            ENode::IndexedFilter {
                input,
                predicates,
                committed,
            } => Rel::IndexedFilter {
                input: Box::new(child(*input)?),
                predicates: egraph.resolve_scalars(predicates),
                committed: committed.clone(),
            },
            ENode::Reduce {
                input,
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
            } => Rel::Reduce {
                input: Box::new(child(*input)?),
                group_key: egraph.resolve_scalars(group_key),
                aggregates: aggregates.clone(),
                monotonic: *monotonic,
                expected_group_size: *expected_group_size,
            },
            ENode::TopK { input, shape } => Rel::TopK {
                input: Box::new(child(*input)?),
                shape: shape.clone(),
            },
            ENode::Negate { input } => Rel::Negate {
                input: Box::new(child(*input)?),
            },
            ENode::Threshold { input } => Rel::Threshold {
                input: Box::new(child(*input)?),
            },
            ENode::Join {
                inputs,
                equivalences,
            } => {
                let resolved_inputs: Vec<Rel> =
                    inputs.iter().map(|&i| child(i)).collect::<Option<_>>()?;
                let base = egraph.resolve_equivalences(equivalences);
                let class_id = egraph.find(class);
                let equivalences = match spellings.and_then(|m| m.get(&class_id)) {
                    Some(ec) => crate::eqsat::join_spelling::select_join_spelling(
                        model,
                        &resolved_inputs,
                        &base,
                        &ec.classes,
                    ),
                    None => base,
                };
                Rel::Join {
                    inputs: resolved_inputs,
                    equivalences,
                }
            }
            ENode::WcoJoin {
                inputs,
                equivalences,
            } => Rel::WcoJoin {
                inputs: inputs.iter().map(|&i| child(i)).collect::<Option<_>>()?,
                equivalences: egraph.resolve_equivalences(equivalences),
            },
            ENode::ArrangeBy { input, key } => Rel::ArrangeBy {
                input: Box::new(child(*input)?),
                key: egraph.resolve_scalars(key),
            },
            ENode::ArrangeByMany { input, keys } => Rel::ArrangeByMany {
                input: Box::new(child(*input)?),
                keys: keys.iter().map(|k| egraph.resolve_scalars(k)).collect(),
            },
            ENode::Union { inputs } => {
                let mut rels: Vec<Rel> = inputs.iter().map(|&i| child(i)).collect::<Option<_>>()?;
                let base = Box::new(rels.remove(0));
                Rel::Union { base, inputs: rels }
            }
        })
    }
}

/// Whether the output multiplicities of `rel` are guaranteed non-negative.
///
/// Mirrors the polarity-demand machinery in `egraph.rs::build_rel`: `Negate` is
/// the only operator that makes multiplicities negative; barriers (`Reduce`,
/// `TopK`, `Threshold`) produce non-negative outputs regardless of input; all
/// other operators are sign-transparent and delegate to their children.
fn is_nonneg_safe(rel: &Rel) -> bool {
    match rel {
        // A negate always produces negative multiplicities.
        Rel::Negate { .. } => false,
        // Barriers: output is non-negative regardless of input.
        Rel::Reduce { .. } | Rel::TopK { .. } | Rel::Threshold { .. } => true,
        // Leaves: base collections are non-negative.
        Rel::Get { .. } | Rel::Constant { .. } | Rel::Opaque(_) | Rel::LocalGet { .. } => true,
        // Sign-transparent: non-negative iff all inputs are non-negative.
        Rel::Project { input, .. }
        | Rel::Map { input, .. }
        | Rel::FlatMap { input, .. }
        | Rel::Filter { input, .. }
        | Rel::IndexedFilter { input, .. }
        | Rel::ArrangeBy { input, .. }
        | Rel::ArrangeByMany { input, .. } => is_nonneg_safe(input),
        Rel::Union { base, inputs } => is_nonneg_safe(base) && inputs.iter().all(is_nonneg_safe),
        Rel::Join { inputs, .. } | Rel::WcoJoin { inputs, .. } => inputs.iter().all(is_nonneg_safe),
        // Let/LetRec do not appear in the ILP extractor's e-graph. Treat as safe.
        Rel::Let { .. } | Rel::LetRec { .. } => true,
    }
}

/// Returns `true` when `rel` is polarity-sound: no non-linear operator (a
/// `Reduce` with at least one aggregate, or a `TopK`) has a sign-bearing input.
///
/// Post-validates the plan produced by [`IlpExtractor::build_selected`], which
/// does not enforce polarity constraints during construction. A `false` return
/// triggers fallback to [`GreedyExtractor`], which enforces polarity via the
/// `Demand::Nonneg` machinery and is always sound.
fn validate_polarity(rel: &Rel) -> bool {
    match rel {
        Rel::Reduce {
            input, aggregates, ..
        } => {
            // A non-linear reduce (non-empty aggregates) requires a nonneg input
            // because reduce(r) != negate(reduce(negate(r))) for non-linear
            // aggregates. A distinct (empty aggregates) is polarity-insensitive.
            if !aggregates.is_empty() && !is_nonneg_safe(input) {
                return false;
            }
            validate_polarity(input)
        }
        Rel::TopK { input, .. } => {
            // TopK always requires a nonneg input; per-group ordering is
            // meaningless over signed multiplicities.
            if !is_nonneg_safe(input) {
                return false;
            }
            validate_polarity(input)
        }
        // All other operators: just recurse into children.
        Rel::Project { input, .. }
        | Rel::Map { input, .. }
        | Rel::FlatMap { input, .. }
        | Rel::Filter { input, .. }
        | Rel::IndexedFilter { input, .. }
        | Rel::Negate { input }
        | Rel::Threshold { input }
        | Rel::ArrangeBy { input, .. }
        | Rel::ArrangeByMany { input, .. } => validate_polarity(input),
        Rel::Union { base, inputs } => {
            validate_polarity(base) && inputs.iter().all(validate_polarity)
        }
        Rel::Join { inputs, .. } | Rel::WcoJoin { inputs, .. } => {
            inputs.iter().all(validate_polarity)
        }
        // Leaves: always valid.
        Rel::Get { .. }
        | Rel::Constant { .. }
        | Rel::Opaque(_)
        | Rel::LocalGet { .. }
        | Rel::Let { .. }
        | Rel::LetRec { .. } => true,
    }
}

/// Whether the arrangement `(class, key_cols)` is already oracle-covered.
///
/// Uses the same logic as the cost model's `arrange_by_oracle_covered`: the
/// arrangement is free when the class contains an `Opaque` wrapping a global
/// `Get` whose available index covers exactly `key_cols`.
fn is_oracle_covered(egraph: &EGraph, model: &CostModel, class: Id, key_cols: &[usize]) -> bool {
    use crate::eqsat::ir::EScalar;
    use mz_expr::MirScalarExpr;
    // Synthesize the EScalar key from the column indices to reuse `arrange_by_oracle_covered`.
    let key: Vec<EScalar> = key_cols
        .iter()
        .map(|&c| EScalar::plain(MirScalarExpr::column(c)))
        .collect();
    // Check if the class contains an Opaque global Get.
    for node in egraph.rel_class_nodes(class) {
        if let ENode::Opaque(mir) = node {
            // Build a temporary Rel::Opaque to use the cost model's oracle check.
            let input = crate::eqsat::ir::Rel::Opaque(Box::new(mir.clone()));
            if model.arrange_by_oracle_covered_pub(&input, &key) {
                return true;
            }
        }
    }
    false
}

/// The work-term degree a node contributes to the time axis: the exponent of
/// `N` in the worst-case work it performs. Mirrors `CostModel::collect_work`,
/// but reads child output sizes from `best_output_degrees` (the per-class
/// cheapest output degree) so an operator above an `IndexedFilter` is charged
/// the lookup's bounded (degree 0) output rather than a full input scan.
fn node_work_degree(node: &ENode, egraph: &EGraph, best: &BTreeMap<Id, f64>) -> f64 {
    // A child not present in the map (should not happen) falls back to a finite
    // base degree so it never poisons the tie-break with infinity.
    let child = |id: Id| best.get(&egraph.find(id)).copied().unwrap_or(1.0);
    match node {
        // Leaves and binding references do no work.
        ENode::Constant { .. } | ENode::Get { .. } | ENode::Opaque(_) | ENode::LocalGet { .. } => {
            0.0
        }
        // A literal point lookup probes the index with a bounded key set; its
        // work does not scale with the input. (Matches `collect_work`.)
        ENode::IndexedFilter { .. } => 0.0,
        // A unary operator scans its input: work is the input's output degree.
        ENode::Project { input, .. }
        | ENode::Map { input, .. }
        | ENode::FlatMap { input, .. }
        | ENode::Filter { input, .. }
        | ENode::Reduce { input, .. }
        | ENode::TopK { input, .. }
        | ENode::ArrangeBy { input, .. }
        | ENode::ArrangeByMany { input, .. }
        | ENode::Negate { input }
        | ENode::Threshold { input } => child(*input),
        ENode::Union { inputs } => inputs.iter().map(|&i| child(i)).fold(0.0, f64::max),
        // Join work proxy: the cross-product degree (sum of input degrees). The
        // exact AGM bound is the cost model's job; for the tie-break a monotone
        // proxy suffices, and identical join shapes cancel across alternatives.
        ENode::Join { inputs, .. } | ENode::WcoJoin { inputs, .. } => {
            inputs.iter().map(|&i| child(i)).sum()
        }
    }
}

/// The direct scalar-payload node count of `node`, the exact per-node term of
/// `Rel::scalar_node_count` (ir.rs): the sum of `scalar_expr_cost` over the
/// node's interned scalar children (`ENode::scalar_children`, resolved via
/// `resolve_scalars` to their cached `EScalar` exprs) plus the two un-interned
/// scalar payloads a node carries verbatim (`Reduce.aggregates`,
/// `TopK.shape.limit`). Uses `scalar_expr_cost`, the same metric
/// `Rel::scalar_node_count` sums, so summing `1 + node_scalar_count` over the
/// DAG-selected relational nodes equals `node_count + scalar_node_count`, the
/// `nodes` axis at cost.rs:370. Does NOT recurse into the relational subtree:
/// the ILP sums it per selected node, so a shared relational child's scalar mass
/// is counted once, not once per parent. Sound as a compute-once preference ONLY
/// for memory-free streaming operators (filters, maps). Do not reuse for a
/// scalar carried across an arrangement boundary without a width charge (WS2).
fn node_scalar_count(node: &ENode, egraph: &EGraph) -> usize {
    let interned: usize = egraph
        .resolve_scalars(&node.scalar_children())
        .iter()
        .map(|s| crate::eqsat::ir::scalar_expr_cost(&s.expr))
        .sum();
    let raw: usize = match node {
        ENode::Reduce { aggregates, .. } => aggregates
            .iter()
            .map(|a| crate::eqsat::ir::scalar_expr_cost(&a.expr))
            .sum(),
        ENode::TopK { shape, .. } => shape
            .limit
            .as_ref()
            .map_or(0, crate::eqsat::ir::scalar_expr_cost),
        _ => 0,
    };
    interned + raw
}

/// Strongly-connected components of the reachable candidate graph, computed by
/// Tarjan's algorithm. Edges run from a class to each child class of each of its
/// e-nodes. Returns `scc_of[class]` (the component index of each class) and
/// `scc_size[index]` (the number of classes in each component).
///
/// The traversal iterates the deterministic `reachable` BTreeMap and the
/// `child_classes` order, so the component numbering is stable across runs.
/// Recursion is expressed with an explicit stack, so depth is bounded by the
/// heap not the call stack (class count is <= the 600-node model cap anyway).
fn tarjan_sccs(
    egraph: &EGraph,
    reachable: &BTreeMap<Id, Vec<ENode>>,
) -> (BTreeMap<Id, usize>, Vec<usize>) {
    // Successor classes per class, in deterministic order. Duplicate successors
    // are harmless for the component partition.
    let mut succ: BTreeMap<Id, Vec<Id>> = BTreeMap::new();
    for (&class, nodes) in reachable {
        let mut out = Vec::new();
        for node in nodes {
            out.extend(egraph.child_classes(node));
        }
        succ.insert(class, out);
    }

    // Tarjan bookkeeping. `index_of` is the DFS discovery number, `lowlink` the
    // lowest discovery number reachable from a class, `component_stack` the set
    // of classes not yet assigned to a component, and `on_stack` its membership.
    let mut index_of: BTreeMap<Id, usize> = BTreeMap::new();
    let mut lowlink: BTreeMap<Id, usize> = BTreeMap::new();
    let mut on_stack: std::collections::BTreeSet<Id> = std::collections::BTreeSet::new();
    let mut component_stack: Vec<Id> = Vec::new();
    let mut next_index = 0usize;
    let mut scc_of: BTreeMap<Id, usize> = BTreeMap::new();
    let mut scc_size: Vec<usize> = Vec::new();

    // Mark a class discovered and push it onto the component stack.
    fn discover(
        class: Id,
        next_index: &mut usize,
        index_of: &mut BTreeMap<Id, usize>,
        lowlink: &mut BTreeMap<Id, usize>,
        on_stack: &mut std::collections::BTreeSet<Id>,
        component_stack: &mut Vec<Id>,
    ) {
        index_of.insert(class, *next_index);
        lowlink.insert(class, *next_index);
        *next_index += 1;
        component_stack.push(class);
        on_stack.insert(class);
    }

    for &start in reachable.keys() {
        if index_of.contains_key(&start) {
            continue;
        }
        discover(
            start,
            &mut next_index,
            &mut index_of,
            &mut lowlink,
            &mut on_stack,
            &mut component_stack,
        );
        // Explicit DFS: each frame is (class, next successor position).
        let mut call_stack: Vec<(Id, usize)> = vec![(start, 0)];
        while let Some(&(v, pos)) = call_stack.last() {
            let neighbors = &succ[&v];
            if pos < neighbors.len() {
                call_stack.last_mut().unwrap().1 += 1;
                let w = neighbors[pos];
                if !index_of.contains_key(&w) {
                    discover(
                        w,
                        &mut next_index,
                        &mut index_of,
                        &mut lowlink,
                        &mut on_stack,
                        &mut component_stack,
                    );
                    call_stack.push((w, 0));
                } else if on_stack.contains(&w) {
                    let iw = index_of[&w];
                    let lv = lowlink[&v];
                    lowlink.insert(v, lv.min(iw));
                }
            } else {
                // v is fully explored. If it is a component root, pop the
                // component off the stack.
                if lowlink[&v] == index_of[&v] {
                    let scc = scc_size.len();
                    let mut size = 0;
                    loop {
                        let w = component_stack.pop().unwrap();
                        on_stack.remove(&w);
                        scc_of.insert(w, scc);
                        size += 1;
                        if w == v {
                            break;
                        }
                    }
                    scc_size.push(size);
                }
                call_stack.pop();
                if let Some(&(parent, _)) = call_stack.last() {
                    let lp = lowlink[&parent];
                    let lv = lowlink[&v];
                    lowlink.insert(parent, lp.min(lv));
                }
            }
        }
    }

    (scc_of, scc_size)
}

/// The SCC partition of the reachable candidate graph plus the set of classes
/// that sit in a non-trivial SCC. A non-trivial SCC is one that can carry a
/// directed cycle: size `>= 2`, or a size-1 component whose class has a
/// self-loop (an e-node with a child class equal to its own class). Tarjan
/// reports a size-1 self-loop component as size 1, so the self-loop must be
/// detected here, else a selected self-loop node would be extractable as an
/// infinite term. A class outside every non-trivial SCC can never lie on a
/// cycle, so it needs no MTZ level variable or acyclicity constraint.
fn cyclic_classes(
    egraph: &EGraph,
    reachable: &BTreeMap<Id, Vec<ENode>>,
) -> (
    BTreeMap<Id, usize>,
    Vec<usize>,
    std::collections::BTreeSet<Id>,
) {
    let (scc_of, scc_size) = tarjan_sccs(egraph, reachable);
    let has_self_loop = |cls: Id, nodes: &[ENode]| {
        nodes
            .iter()
            .any(|n| egraph.child_classes(n).into_iter().any(|c| c == cls))
    };
    let cyclic: std::collections::BTreeSet<Id> = reachable
        .iter()
        .filter_map(|(&cls, nodes)| {
            let non_trivial = scc_size[scc_of[&cls]] >= 2 || has_self_loop(cls, nodes);
            non_trivial.then_some(cls)
        })
        .collect();
    (scc_of, scc_size, cyclic)
}

/// The per-class cheapest output-size degree, by a bottom-up fixpoint over the
/// reachable subgraph. A class's degree is the minimum over its nodes; a node's
/// output degree mirrors `CostModel::size_degree`, except an `IndexedFilter`
/// yields a bounded (degree 0) output. Values only decrease from `+inf`, so the
/// fixpoint terminates.
fn best_output_degrees(egraph: &EGraph, reachable: &BTreeMap<Id, Vec<ENode>>) -> BTreeMap<Id, f64> {
    let mut best: std::collections::BTreeMap<Id, f64> =
        reachable.keys().map(|&c| (c, f64::INFINITY)).collect();
    loop {
        let mut changed = false;
        for (&class, nodes) in reachable {
            let child = |id: Id| best.get(&egraph.find(id)).copied().unwrap_or(f64::INFINITY);
            let mut cmin = best[&class];
            for node in nodes {
                let d = match node {
                    ENode::Constant { .. } => 0.0,
                    ENode::Get { .. } | ENode::Opaque(_) | ENode::LocalGet { .. } => 1.0,
                    ENode::IndexedFilter { .. } => 0.0,
                    ENode::Project { input, .. }
                    | ENode::Map { input, .. }
                    | ENode::FlatMap { input, .. }
                    | ENode::Filter { input, .. }
                    | ENode::Reduce { input, .. }
                    | ENode::TopK { input, .. }
                    | ENode::ArrangeBy { input, .. }
                    | ENode::ArrangeByMany { input, .. }
                    | ENode::Negate { input }
                    | ENode::Threshold { input } => child(*input),
                    ENode::Union { inputs } => inputs.iter().map(|&i| child(i)).fold(0.0, f64::max),
                    ENode::Join { inputs, .. } | ENode::WcoJoin { inputs, .. } => {
                        inputs.iter().map(|&i| child(i)).sum()
                    }
                };
                if d < cmin {
                    cmin = d;
                }
            }
            if cmin < best[&class] {
                best.insert(class, cmin);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn greedy_extractor_matches_extract_with() {
        use crate::eqsat::cost::CostModel;
        use crate::eqsat::egraph::EGraph;
        use crate::eqsat::ir::Rel;
        use crate::eqsat::objective::ArrangementCount;
        let mut eg = EGraph::new();
        let plan = Rel::Get {
            name: "t".into(),
            arity: 2,
        };
        let root = eg.add_rel(&plan);
        let model = CostModel::new();
        let direct = eg.extract_with(root, &model, &ArrangementCount, None, None);
        let via = GreedyExtractor.extract(&eg, root, &model, &ArrangementCount, None, None);
        assert_eq!(direct, via);
    }

    #[mz_ore::test]
    fn ilp_extracts_min_arrangement_plan() {
        use crate::eqsat::cost::CostModel;
        use crate::eqsat::egraph::EGraph;
        use crate::eqsat::ir::Rel;
        use crate::eqsat::objective::ArrangementCount;
        // A class with two equivalent forms: one needs an extra ArrangeBy, one does
        // not. The ILP must pick the arrangement-free form.
        let mut eg = EGraph::new();
        let cheap = Rel::Get {
            name: "t".into(),
            arity: 1,
        };
        let pricey = Rel::ArrangeBy {
            input: Box::new(Rel::Get {
                name: "t".into(),
                arity: 1,
            }),
            key: vec![],
        };
        let a = eg.add_rel(&cheap);
        let b = eg.add_rel(&pricey);
        eg.union(a, b);
        eg.rebuild();
        let model = CostModel::new();
        let plan = IlpExtractor::default()
            .extract(&eg, a, &model, &ArrangementCount, None, None)
            .unwrap();
        assert_eq!(model.cost(&plan).arrangements, 0);
    }

    /// Join commutativity is inherently cyclic: `Join(a, b)` and `Join(b, a)`
    /// are equal up to a column swap, so the two alternatives cross-reference
    /// through swap Projects and the reachable candidate graph has a directed
    /// cycle. The two classes form one non-trivial SCC of size 2, so the size-2
    /// mutual-exclusion cut `xa + xb <= 1` forbids selecting both cross-edges at
    /// once. That lets the ILP extract the acyclic optimum (the plain Join)
    /// directly rather than bailing to greedy on the cycle.
    ///
    /// The class also carries a self-referential identity Project whose input is
    /// its own class. The universal self-loop cut pins its `node_sel` to 0, so
    /// the solver must never select it.
    #[mz_ore::test]
    fn ilp_handles_join_commutativity_cycle() {
        use crate::eqsat::cost::CostModel;
        use crate::eqsat::egraph::{CNode, EGraph, ENode};
        use crate::eqsat::ir::Rel;

        let mut eg = EGraph::new();
        let a = eg.add(CNode::Rel(ENode::Get {
            name: "a".into(),
            arity: 1,
        }));
        let b = eg.add(CNode::Rel(ENode::Get {
            name: "b".into(),
            arity: 1,
        }));

        // Class A: Join(a, b). Class B: Join(b, a).
        let join_ab = eg.add(CNode::Rel(ENode::Join {
            inputs: vec![a, b],
            equivalences: vec![],
        }));
        let join_ba = eg.add(CNode::Rel(ENode::Join {
            inputs: vec![b, a],
            equivalences: vec![],
        }));

        // Project[1,0] over B is the column-swapped B, equal to Join(a, b): union
        // it into class A. Symmetrically for B. Now A references B and B
        // references A, so the reachable candidate graph is cyclic.
        let swap_of_b = eg.add(CNode::Rel(ENode::Project {
            input: join_ba,
            outputs: vec![1, 0],
        }));
        eg.union(join_ab, swap_of_b);
        let swap_of_a = eg.add(CNode::Rel(ENode::Project {
            input: join_ab,
            outputs: vec![1, 0],
        }));
        eg.union(join_ba, swap_of_a);

        // A self-referential identity Project whose input is class A itself.
        let self_proj = eg.add(CNode::Rel(ENode::Project {
            input: join_ab,
            outputs: vec![0, 1],
        }));
        eg.union(join_ab, self_proj);
        eg.rebuild();

        let root = eg.find(join_ab);
        let model = CostModel::new();

        // Call `solve` directly so the assertion sees the ILP result, never a
        // greedy fallback. The SCC-scoped MTZ constraints let it extract the
        // acyclic optimum rather than bailing to greedy on the cycle.
        let plan = IlpExtractor::default()
            .solve(&eg, root, &model, None)
            .expect("ILP must solve the cyclic candidate graph rather than bail on the cycle");

        // The acyclic optimum is the plain Join over the two leaves. A plain Join
        // of two Gets contains no Project, so this also proves neither the
        // commuting swap Project nor the self-referential Project was selected.
        match &plan {
            Rel::Join { inputs, .. } => {
                assert_eq!(inputs.len(), 2, "expected a 2-input Join, got {plan:?}");
                assert!(
                    inputs.iter().all(|i| matches!(i, Rel::Get { .. })),
                    "expected both Join inputs to be leaf Gets, got {plan:?}",
                );
            }
            other => panic!("expected a plain Join (never a Project), got {other:?}"),
        }
    }

    /// Size-2 mutual-exclusion cut admits the one acyclic selection. The two
    /// classes form one non-trivial SCC of size 2 with a cross-edge each way, so
    /// the cut `xa + xb <= 1` forbids taking both at once. Exactly one acyclic
    /// selection survives: `p1 = Project(leaf)`, `leaf = Get`. Asserting `Some`
    /// proves the cut does not over-constrain and rule out that valid selection,
    /// and the `Project(Get)` shape proves the 2-cycle was broken at the leaf.
    #[mz_ore::test]
    fn ilp_size_two_cut_admits_acyclic_selection() {
        use crate::eqsat::cost::CostModel;
        use crate::eqsat::egraph::{CNode, EGraph, ENode};
        use crate::eqsat::ir::Rel;

        // leaf <- p1 <- p2, then union(leaf, p2) closes a cycle through
        // congruence. Reachable from p1 are exactly two classes: p1 and the
        // merged leaf/p2 class. They form one size-2 SCC. The only acyclic
        // selection is p1 = Project(leaf), leaf = Get.
        let mut eg = EGraph::new();
        let leaf = eg.add(CNode::Rel(ENode::Get {
            name: "r".into(),
            arity: 1,
        }));
        let p1 = eg.add(CNode::Rel(ENode::Project {
            input: leaf,
            outputs: vec![0],
        }));
        let p2 = eg.add(CNode::Rel(ENode::Project {
            input: p1,
            outputs: vec![0],
        }));
        eg.union(leaf, p2);
        eg.rebuild();

        let root = eg.find(p1);
        let model = CostModel::new();
        let plan = IlpExtractor::default()
            .solve(&eg, root, &model, None)
            .expect("the size-2 cut must admit the acyclic selection, not over-constrain it");
        assert!(
            matches!(&plan, Rel::Project { input, .. } if matches!(**input, Rel::Get { .. })),
            "expected the Project(Get) chain, got {plan:?}",
        );
    }

    /// A size-1 SCC with a self-loop is still non-trivial. A class carries a
    /// self-referential identity Project (its input is its own class) alongside a
    /// finite ArrangeBy alternative. Tarjan reports the class as a size-1
    /// component, so the self-loop rule is what keeps it non-trivial. Without a
    /// cut the ILP would take the zero-arrangement self Project, an infinite term.
    /// The universal self-loop cut pins the self Project's `node_sel` to 0, so the
    /// solver must take the finite ArrangeBy instead.
    #[mz_ore::test]
    fn ilp_size_one_self_loop_scc_never_extractable() {
        use crate::eqsat::cost::CostModel;
        use crate::eqsat::egraph::{CNode, EGraph, ENode};
        use crate::eqsat::ir::Rel;

        let mut eg = EGraph::new();
        let base = eg.add(CNode::Rel(ENode::Get {
            name: "t".into(),
            arity: 1,
        }));
        // A key-less ArrangeBy maintains one arrangement, so this finite form
        // costs strictly more than the zero-arrangement self Project below.
        let arr = eg.add(CNode::Rel(ENode::ArrangeBy {
            input: base,
            key: vec![],
        }));
        // Identity Project whose input is the class it lands in, a self-loop.
        let self_proj = eg.add(CNode::Rel(ENode::Project {
            input: arr,
            outputs: vec![0],
        }));
        eg.union(arr, self_proj);
        eg.rebuild();

        let root = eg.find(arr);
        // Confirm the scoping classifies this as a non-trivial size-1 SCC.
        let reachable = eg.reachable(root);
        let (scc_of, scc_size, cyclic) = cyclic_classes(&eg, &reachable);
        assert!(
            cyclic.contains(&root),
            "the self-loop class must be non-trivial, cyclic = {cyclic:?}",
        );
        assert_eq!(
            scc_size[scc_of[&root]], 1,
            "the self-loop class must be a size-1 SCC (Tarjan sees no larger cycle)",
        );

        let model = CostModel::new();
        // The self Project is infeasible, so `solve` must extract the finite
        // ArrangeBy rather than bail.
        let plan = IlpExtractor::default()
            .solve(&eg, root, &model, None)
            .expect("ILP must solve rather than bail, only the self-loop form is infeasible");
        assert!(
            !matches!(&plan, Rel::Project { .. }),
            "the self-loop Project must never be selected, got {plan:?}",
        );
        assert!(
            matches!(&plan, Rel::ArrangeBy { input, .. } if matches!(**input, Rel::Get { .. })),
            "expected the finite ArrangeBy(Get), got {plan:?}",
        );
    }

    /// SCC-scoping restores base-ILP cost on acyclic structure: a purely acyclic
    /// fragment has no non-trivial SCC, so `cyclic_classes` returns an empty set
    /// and the program emits zero level variables and zero acyclicity
    /// constraints. This is what removes the global-MTZ overhead on the common
    /// acyclic case.
    #[mz_ore::test]
    fn ilp_acyclic_fragment_emits_no_level_constraints() {
        use crate::eqsat::egraph::{CNode, EGraph, ENode};

        // leaf <- p1 <- p2, a plain acyclic chain, no unions.
        let mut eg = EGraph::new();
        let leaf = eg.add(CNode::Rel(ENode::Get {
            name: "r".into(),
            arity: 1,
        }));
        let p1 = eg.add(CNode::Rel(ENode::Project {
            input: leaf,
            outputs: vec![0],
        }));
        let p2 = eg.add(CNode::Rel(ENode::Project {
            input: p1,
            outputs: vec![0],
        }));
        eg.rebuild();

        let reachable = eg.reachable(eg.find(p2));
        let (_scc_of, scc_size, cyclic) = cyclic_classes(&eg, &reachable);
        assert!(
            cyclic.is_empty(),
            "an acyclic fragment must have no non-trivial SCC, cyclic = {cyclic:?}",
        );
        // Every class is its own singleton component.
        assert!(
            scc_size.iter().all(|&s| s == 1),
            "every acyclic class must be a size-1 SCC, scc_size = {scc_size:?}",
        );
    }

    /// Size >= 3 MTZ fallback. Corpus-wide every non-trivial SCC is size 2, so
    /// the MTZ path is dormant and golden churn never exercises it. This
    /// hand-built size-3 cycle keeps it from rotting and insures a future larger
    /// SCC. Three classes A -> B -> C -> A each carry a zero-arrangement Project
    /// to the next class (the cycle) and a one-arrangement `ArrangeBy(Get)`
    /// grounding. The full 3-cycle is the cheapest selection on arrangements
    /// (zero), so absent the MTZ cut the ILP would pick it, an infinite term that
    /// fails reconstruction. The MTZ level constraints make the 3-cycle
    /// infeasible, forcing the ILP to ground at a leaf and pay one arrangement.
    #[mz_ore::test]
    fn ilp_size_three_cycle_mtz_fallback() {
        use crate::eqsat::cost::CostModel;
        use crate::eqsat::egraph::{CNode, EGraph, ENode};
        use crate::eqsat::ir::Rel;

        let mut eg = EGraph::new();
        let a_leaf = eg.add(CNode::Rel(ENode::Get {
            name: "a".into(),
            arity: 1,
        }));
        let b_leaf = eg.add(CNode::Rel(ENode::Get {
            name: "b".into(),
            arity: 1,
        }));
        let c_leaf = eg.add(CNode::Rel(ENode::Get {
            name: "c".into(),
            arity: 1,
        }));

        // A key-less ArrangeBy grounds each class in a finite, one-arrangement
        // form. The Project below is the zero-arrangement cross-edge to the next
        // class, so grounding is what the ILP pays to escape the cycle.
        let a_arr = eg.add(CNode::Rel(ENode::ArrangeBy {
            input: a_leaf,
            key: vec![],
        }));
        let b_arr = eg.add(CNode::Rel(ENode::ArrangeBy {
            input: b_leaf,
            key: vec![],
        }));
        let c_arr = eg.add(CNode::Rel(ENode::ArrangeBy {
            input: c_leaf,
            key: vec![],
        }));

        // Close the cycle: class A gets a Project referencing class B, B one
        // referencing C, C one referencing A. Union each into the arranged class
        // so A -> B -> C -> A is one size-3 SCC.
        let a_to_b = eg.add(CNode::Rel(ENode::Project {
            input: b_arr,
            outputs: vec![0],
        }));
        let b_to_c = eg.add(CNode::Rel(ENode::Project {
            input: c_arr,
            outputs: vec![0],
        }));
        let c_to_a = eg.add(CNode::Rel(ENode::Project {
            input: a_arr,
            outputs: vec![0],
        }));
        eg.union(a_arr, a_to_b);
        eg.union(b_arr, b_to_c);
        eg.union(c_arr, c_to_a);
        eg.rebuild();

        let root = eg.find(a_arr);

        // Confirm the three classes form one non-trivial size-3 SCC, so the MTZ
        // fallback (not the size-1 or size-2 binary cuts) is the path under test.
        let reachable = eg.reachable(root);
        let (scc_of, scc_size, cyclic) = cyclic_classes(&eg, &reachable);
        assert!(
            cyclic.contains(&root),
            "the cycle classes must be non-trivial, cyclic = {cyclic:?}",
        );
        assert_eq!(
            scc_size[scc_of[&root]], 3,
            "the three classes must form one size-3 SCC exercising the MTZ fallback",
        );

        let model = CostModel::new();
        let plan = IlpExtractor::default()
            .solve(&eg, root, &model, None)
            .expect("the MTZ fallback must break the 3-cycle and yield a finite plan");

        // The acyclic optimum grounds root A directly: ArrangeBy(Get "a"), one
        // arrangement. The infinite 3-cycle (a Project chain that never grounds)
        // must never be extracted.
        assert!(
            matches!(&plan, Rel::ArrangeBy { input, .. } if matches!(**input, Rel::Get { .. })),
            "expected the grounded ArrangeBy(Get), never the 3-cycle, got {plan:?}",
        );
        assert_eq!(
            model.cost(&plan).arrangements,
            1,
            "breaking the cycle must cost exactly one arrangement, got {plan:?}",
        );
    }

    /// Non-trivial comparison: a class with two semantically equivalent forms, one
    /// routing via an explicit ArrangeBy and one going directly into Reduce. The
    /// ILP must not produce a plan with more arrangements than greedy.
    ///
    /// This tests the oracle-coverage alignment fix (Finding 3): only ArrangeBy
    /// arrangements should be oracle-suppressible. Before the fix the ILP could
    /// mis-account Reduce group-key arrangements as oracle-covered, producing a
    /// higher real arrangement count than greedy.
    #[mz_ore::test]
    fn ilp_arrangement_count_not_worse_than_greedy() {
        use crate::eqsat::cost::CostModel;
        use crate::eqsat::egraph::EGraph;
        use crate::eqsat::ir::{EScalar, Rel};
        use crate::eqsat::objective::ArrangementCount;
        use mz_expr::MirScalarExpr;

        // Two equivalent forms of Reduce(distinct, group=[col0]) over Get t:
        //   form A: Reduce directly over Get (zero extra arrangements)
        //   form B: Reduce over ArrangeBy(Get, key=[col0]) (one extra arrangement)
        let mut eg = EGraph::new();
        let get = Rel::Get {
            name: "t".into(),
            arity: 2,
        };
        let key_col0 = vec![EScalar::plain(MirScalarExpr::column(0))];
        let reduce_direct = Rel::Reduce {
            input: Box::new(get.clone()),
            group_key: key_col0.clone(),
            aggregates: vec![],
            monotonic: false,
            expected_group_size: None,
        };
        let reduce_via_arrange = Rel::Reduce {
            input: Box::new(Rel::ArrangeBy {
                input: Box::new(get.clone()),
                key: key_col0.clone(),
            }),
            group_key: key_col0.clone(),
            aggregates: vec![],
            monotonic: false,
            expected_group_size: None,
        };
        let a = eg.add_rel(&reduce_direct);
        let b = eg.add_rel(&reduce_via_arrange);
        eg.union(a, b);
        eg.rebuild();

        let model = CostModel::new();
        let greedy_plan = GreedyExtractor
            .extract(&eg, a, &model, &ArrangementCount, None, None)
            .expect("greedy must find a plan");
        let ilp_plan = IlpExtractor::default()
            .extract(&eg, a, &model, &ArrangementCount, None, None)
            .expect("ILP must find a plan");
        let greedy_cost = model.cost(&greedy_plan);
        let ilp_cost = model.cost(&ilp_plan);
        assert!(
            ilp_cost.arrangements <= greedy_cost.arrangements,
            "ILP produced more arrangements ({}) than greedy ({})",
            ilp_cost.arrangements,
            greedy_cost.arrangements
        );
    }

    #[mz_ore::test]
    fn scalar_aware_node_tier_prefers_split_filter() {
        // The WS1 acceptance shape (`SELECT * FROM r WHERE a UNION ALL SELECT *
        // FROM r WHERE a AND b`): a Union of consumer 1 (bare `Filter[a](r)`)
        // and consumer 2, whose class holds both the fused `Filter[a,b](r)`
        // and the split `Filter[b](Filter[a](r))`. The split's inner filter is
        // byte-identical to consumer 1's, so it hash-conses into the same
        // class: consumer 1 pays for `Filter[a]` regardless, so choosing split
        // for consumer 2 only adds the outer `Filter[b]`, while choosing fused
        // adds a brand-new node that re-pays predicate `a`'s scalar cost from
        // scratch. Across the whole plan this makes fused and split tie on
        // both the time tier (each totals the same number of filter passes:
        // consumer 1's shared filter, plus one filter for consumer 2) and, flag
        // off, on flat node count (4 selected nodes either way). Only the
        // scalar-aware node tier breaks the tie, in favor of split.
        //
        // A single-consumer construction (fused vs. split with no sibling
        // reusing the inner filter) does NOT tie: splitting one filter into
        // two strictly adds a filter pass with nothing to amortize it against,
        // so both the real cost model (cost.rs's sorted `time` axis) and the
        // ILP's summed work-degree proxy already prefer fused before the node
        // tier is ever consulted. The sibling consumer is essential here, not
        // decoration; a bare fused-vs-split union with no shared consumer would
        // fail even with the flag on.
        use crate::eqsat::ir::{EScalar, Rel};
        use crate::eqsat::objective::ArrangementCount;
        use mz_expr::MirScalarExpr;

        let get = Rel::Get {
            name: "r".into(),
            arity: 2,
        };
        let a = EScalar::plain(MirScalarExpr::column(0).call_is_null().not());
        let b = EScalar::plain(MirScalarExpr::column(1).call_is_null().not());

        // Consumer 1: always needs `Filter[a](r)`.
        let consumer1 = Rel::Filter {
            predicates: vec![a.clone()],
            input: Box::new(get.clone()),
        };
        // Consumer 2, fused form: one Filter node re-encoding both predicates.
        let fused = Rel::Filter {
            predicates: vec![a.clone(), b.clone()],
            input: Box::new(get.clone()),
        };
        // Consumer 2, split form: the inner Filter[a] is byte-identical to
        // consumer 1's and hash-conses into the same class.
        let split = Rel::Filter {
            predicates: vec![b.clone()],
            input: Box::new(Rel::Filter {
                predicates: vec![a.clone()],
                input: Box::new(get.clone()),
            }),
        };
        let root_rel = Rel::Union {
            base: Box::new(consumer1),
            inputs: vec![fused],
        };

        let mut eg = EGraph::new();
        let root = eg.add_rel(&root_rel);
        let split_id = eg.add_rel(&split);
        // Consumer 2's class is the Union's second child (`base` is consumer
        // 1). Union it with the split alternative so the ILP must choose.
        let root_node = eg.rel_class_nodes(eg.find(root))[0].clone();
        let consumer2_class = match root_node {
            ENode::Union { inputs } => inputs[1],
            other => panic!("expected a Union root, got {other:?}"),
        };
        eg.union(consumer2_class, split_id);
        eg.rebuild();

        let model = CostModel::new();
        let plan = IlpExtractor {
            weight_scalar_nodes: true,
            width_aware: false,
        }
        .extract(&eg, root, &model, &ArrangementCount, None, None)
        .expect("extract");
        let Rel::Union { inputs, .. } = &plan else {
            panic!("expected a Union root, got {plan:?}");
        };
        assert_eq!(inputs.len(), 1, "expected exactly one non-base input");
        // Split form: an outer single-predicate Filter over an inner Filter.
        assert!(
            matches!(&inputs[0], Rel::Filter { predicates, input }
                if predicates.len() == 1 && matches!(**input, Rel::Filter { .. })),
            "expected consumer 2 to extract as the split form, got {plan:?}",
        );
    }

    #[mz_ore::test]
    fn width_aware_arity_tier_prefers_narrower_arrangement() {
        use crate::eqsat::cost::CostModel;
        use crate::eqsat::egraph::EGraph;
        use crate::eqsat::ir::{EScalar, Rel};
        use crate::eqsat::objective::ArrangementCount;
        use mz_expr::MirScalarExpr;

        // Two arrangement forms tying on every tier the ILP scores before arity:
        // each needs exactly one arrangement (PRIMARY), and each ArrangeBy sits
        // directly over a leaf Get, whose work-degree proxy and best-output-degree
        // are both constants independent of arity (see `node_work_degree` and
        // `best_output_degrees`), so the TIME and NODES tiers are identical on
        // both sides too. Only the arrangement's arity differs: a 1-column
        // relation versus a 3-column relation, each arranged on column 0. This
        // isolates the arity tier as the sole tie-breaker.
        let key = vec![EScalar::plain(MirScalarExpr::column(0))];
        let narrow = Rel::ArrangeBy {
            input: Box::new(Rel::Get {
                name: "narrow".into(),
                arity: 1,
            }),
            key: key.clone(),
        };
        let wide = Rel::ArrangeBy {
            input: Box::new(Rel::Get {
                name: "wide".into(),
                arity: 3,
            }),
            key: key.clone(),
        };

        let mut eg = EGraph::new();
        let n = eg.add_rel(&narrow);
        let w = eg.add_rel(&wide);
        eg.union(n, w);
        eg.rebuild();

        let model = CostModel::new();

        // Flag on: the ILP must prefer the narrower (arity 1) arrangement.
        let plan = IlpExtractor {
            width_aware: true,
            ..Default::default()
        }
        .extract(&eg, n, &model, &ArrangementCount, None, None)
        .expect("extract");
        let Rel::ArrangeBy { input, .. } = &plan else {
            panic!("expected an ArrangeBy root, got {plan:?}");
        };
        match input.as_ref() {
            Rel::Get { arity, .. } => assert_eq!(
                *arity, 1,
                "expected the narrow (arity 1) relation, got arity {arity}"
            ),
            other => panic!("expected a Get input, got {other:?}"),
        }

        // Flag off: the arity tier is not computed, so the tie between the two
        // forms is genuine on every remaining tier and either side is a valid
        // optimum. The one invariant that must still hold is the one the
        // arrangement-count tier already guarantees: exactly one arrangement is
        // used, not both.
        let default_plan = IlpExtractor::default()
            .extract(&eg, n, &model, &ArrangementCount, None, None)
            .expect("extract");
        assert_eq!(model.cost(&default_plan).arrangements, 1);
    }

    /// Polarity soundness reproducer (Finding 1): when a Reduce input class
    /// contains both a Negate-rooted form and a nonneg-safe form, the ILP must
    /// not extract the Negate-rooted form as the Reduce input.
    ///
    /// Mirrors the reviewer's case: `Reduce(MAX, Filter(p, Negate(Get a)))` with
    /// a nonneg-safe sibling form `Filter(p, Get a)` merged into the same class.
    /// Before the polarity post-validation fix, the ILP could pick the cheaper
    /// (fewer-nodes) Negate-rooted form, producing a multiplicity-unsound plan.
    #[mz_ore::test]
    fn ilp_polarity_soundness_reduce_negate_input() {
        use crate::eqsat::cost::CostModel;
        use crate::eqsat::egraph::EGraph;
        use crate::eqsat::ir::{EScalar, Rel};
        use crate::eqsat::objective::ArrangementCount;
        use mz_expr::{AggregateExpr, AggregateFunc, MirScalarExpr};

        let mut eg = EGraph::new();
        let get_a = Rel::Get {
            name: "a".into(),
            arity: 1,
        };
        // Filter(true, Negate(Get a)): sign-bearing input (negative multiplicities).
        let filter_negate = Rel::Filter {
            input: Box::new(Rel::Negate {
                input: Box::new(get_a.clone()),
            }),
            predicates: vec![EScalar::plain(MirScalarExpr::literal_true())],
        };
        // Reduce(MAX, Filter(true, Negate(Get a))): unsound, a non-linear reduce
        // over a sign-bearing input.
        let reduce_unsound = Rel::Reduce {
            input: Box::new(filter_negate.clone()),
            group_key: vec![],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::MaxInt64,
                expr: MirScalarExpr::column(0),
                distinct: false,
            }],
            monotonic: false,
            expected_group_size: None,
        };
        // A nonneg-safe sibling form: Filter(true, Get a).
        let filter_safe = Rel::Filter {
            input: Box::new(get_a.clone()),
            predicates: vec![EScalar::plain(MirScalarExpr::literal_true())],
        };
        let root = eg.add_rel(&reduce_unsound);
        // Merge filter_negate and filter_safe into one class so the ILP may
        // consider either as the Reduce input.
        let fa = eg.add_rel(&filter_negate);
        let fb = eg.add_rel(&filter_safe);
        eg.union(fa, fb);
        eg.rebuild();

        let model = CostModel::new();

        // Greedy must return a polarity-sound plan.
        let greedy = GreedyExtractor.extract(&eg, root, &model, &ArrangementCount, None, None);
        if let Some(ref plan) = greedy {
            assert!(
                validate_polarity(plan),
                "greedy returned a polarity-unsound plan: {:?}",
                plan
            );
        }

        // ILP must return either None (triggering greedy fallback) or a
        // polarity-sound plan. It must never return Reduce(Negate(...)).
        let ilp = IlpExtractor::default().extract(&eg, root, &model, &ArrangementCount, None, None);
        if let Some(ref plan) = ilp {
            assert!(
                validate_polarity(plan),
                "ILP returned a polarity-unsound plan: {:?}",
                plan
            );
        }
    }

    // -------------------------------------------------------------------------
    // SP4a multi-sort fusion: round-trip corpus, keying safeguard, and a
    // (non-gating) scalar-sharing measurement harness.
    //
    // These lock in the §6.3 keying invariant: extraction/cost/CSE resolve scalar
    // `Id`s back to `EScalar` *content*, so the combined relational+scalar e-graph
    // is behavior-neutral versus the pre-fusion `EScalar`-payload graph.

    /// An `EScalar` whose cached `lit` is the canonical function of its expr,
    /// matching what `intern_scalar` re-derives — so a corpus scalar survives the
    /// lower/raise round-trip under `assert_eq!`.
    fn es(expr: mz_expr::MirScalarExpr) -> crate::eqsat::ir::EScalar {
        use crate::eqsat::ir::EScalar;
        EScalar::new(expr.clone(), EScalar::lit_of(&expr))
    }

    /// A bare column reference `#i`.
    fn col(i: usize) -> mz_expr::MirScalarExpr {
        mz_expr::MirScalarExpr::column(i)
    }

    /// A named base relation of the given arity.
    fn get(name: &str, arity: usize) -> crate::eqsat::ir::Rel {
        crate::eqsat::ir::Rel::Get {
            name: name.into(),
            arity,
        }
    }

    /// One `Rel` per scalar-bearing `ENode` kind — Map, Filter, FlatMap, Reduce,
    /// Join, WcoJoin, ArrangeBy, ArrangeByMany, IndexedFilter — exercising every
    /// scalar payload position so the round-trip covers the full `EScalar`<->`Id`
    /// boundary.
    fn scalar_bearing_corpus() -> Vec<crate::eqsat::ir::Rel> {
        use crate::eqsat::ir::Rel;
        use mz_expr::{BinaryFunc, MirScalarExpr, TableFunc};
        use mz_repr::{Datum, ReprRelationType, ReprScalarType};

        // `#0 = 1`: a non-trivial predicate that must resolve through the cache.
        let eq_pred = || {
            es(col(0).call_binary(
                MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64),
                BinaryFunc::Eq(mz_expr::func::Eq),
            ))
        };

        vec![
            // Map.scalars
            Rel::Map {
                input: Box::new(get("t", 2)),
                scalars: vec![es(col(0)), es(col(1))],
            },
            // Filter.predicates
            Rel::Filter {
                input: Box::new(get("t", 1)),
                predicates: vec![eq_pred()],
            },
            // FlatMap.exprs
            Rel::FlatMap {
                input: Box::new(get("t", 2)),
                func: TableFunc::GenerateSeriesInt64,
                exprs: vec![
                    es(col(0)),
                    es(MirScalarExpr::literal_ok(
                        Datum::Int64(1),
                        ReprScalarType::Int64,
                    )),
                ],
            },
            // Reduce.group_key
            Rel::Reduce {
                input: Box::new(get("t", 2)),
                group_key: vec![es(col(0))],
                aggregates: vec![],
                monotonic: false,
                expected_group_size: None,
            },
            // Join.equivalences
            Rel::Join {
                inputs: vec![get("a", 1), get("b", 1)],
                equivalences: vec![vec![es(col(0)), es(col(1))]],
            },
            // WcoJoin.equivalences
            Rel::WcoJoin {
                inputs: vec![get("a", 1), get("b", 1)],
                equivalences: vec![vec![es(col(0)), es(col(1))]],
            },
            // ArrangeBy.key
            Rel::ArrangeBy {
                input: Box::new(get("t", 2)),
                key: vec![es(col(0))],
            },
            // ArrangeByMany.keys
            Rel::ArrangeByMany {
                input: Box::new(get("t", 2)),
                keys: vec![vec![es(col(0))], vec![es(col(1))]],
            },
            // IndexedFilter.predicates (the `committed` MIR is carried verbatim).
            Rel::IndexedFilter {
                input: Box::new(get("t", 1)),
                predicates: vec![eq_pred()],
                committed: Box::new(mz_expr::MirRelationExpr::constant(
                    vec![],
                    ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]),
                )),
            },
        ]
    }

    /// §8 stage-1 round-trip, exhaustively over the scalar-bearing nodes: lowering
    /// a `Rel` into the combined e-graph and extracting it back is the identity.
    /// Scalar payloads are interned as scalar `Id` e-classes on the way in and
    /// resolved from the `escalar` cache on the way out, so `raise(lower(p)) == p`.
    #[mz_ore::test]
    fn round_trip_corpus_all_scalar_bearing_nodes() {
        use crate::eqsat::cost::CostModel;
        use crate::eqsat::egraph::EGraph;
        for rel in scalar_bearing_corpus() {
            let mut eg = EGraph::new();
            let root = eg.add_rel(&rel);
            eg.rebuild();
            let out = eg
                .extract(root, &CostModel::new())
                .expect("scalar-bearing root must extract");
            assert_eq!(out, rel, "round-trip must be the identity for {rel:?}");
        }
    }

    /// §6.3 keying invariant. A class holds two cost-equal `Map` forms whose only
    /// difference is scalar *ordering*; extraction's tie-break must resolve each
    /// candidate's scalar `Id`s back to `EScalar` content and pick the
    /// `EScalar`-`Ord`-smaller plan — byte-identical to the pre-fusion graph,
    /// where that same `Ord` ran over `EScalar` payloads directly.
    ///
    /// The test is made *discriminating* by interning the scalars so that raw
    /// intern-order `Id`s run OPPOSITE to `EScalar`-`Ord`: a comparator that
    /// leaked raw `Id`s (the regression §6.3 guards against) would pick the other,
    /// cost-equal form `[#1,#0]` and the assertion would fail. Passing proves the
    /// comparison resolves through the cache.
    #[mz_ore::test]
    fn extraction_tiebreak_resolves_scalars_through_cache() {
        use crate::eqsat::cost::CostModel;
        use crate::eqsat::egraph::{CNode, EGraph, ENode};
        use crate::eqsat::ir::Rel;

        let mut eg = EGraph::new();
        let input = eg.add_rel(&get("t", 2));

        // Intern `#1` FIRST so it receives the smaller `Id`, then `#0`. The raw
        // intern-order of the two scalars is therefore the reverse of their
        // `EScalar`-`Ord` (`#0 < #1`).
        let s1 = eg.intern_scalar(&es(col(1)));
        let s0 = eg.intern_scalar(&es(col(0)));
        assert!(
            s1 < s0,
            "test setup: #1 must intern to the smaller raw Id so raw-Id order is \
             opposite to EScalar-Ord (got s1={s1}, s0={s0})"
        );

        // Two `Map` alternatives differing ONLY in scalar order: by raw `Id` the
        // `[s1,s0]` form is smaller; by `EScalar` content the `[s0,s1]` (=`[#0,#1]`)
        // form is smaller.
        let m_id_order = eg.add(CNode::Rel(ENode::Map {
            input,
            scalars: vec![s1, s0],
        }));
        let m_escalar_order = eg.add(CNode::Rel(ENode::Map {
            input,
            scalars: vec![s0, s1],
        }));
        eg.union(m_id_order, m_escalar_order);
        eg.rebuild();
        let root = eg.find(m_id_order);

        let model = CostModel::new();

        // The two resolved forms, and the pre-fusion winner pinned EXPLICITLY: the
        // `EScalar`-`Ord`-smaller plan is the one with scalars `[#0,#1]`.
        let form_escalar_order = Rel::Map {
            input: Box::new(get("t", 2)),
            scalars: vec![es(col(0)), es(col(1))],
        };
        let form_id_order = Rel::Map {
            input: Box::new(get("t", 2)),
            scalars: vec![es(col(1)), es(col(0))],
        };
        // Precondition: genuinely cost-equal, so the pick is a pure tie-break
        // (otherwise extraction would choose on cost and skip the keying path).
        assert_eq!(
            model.cost(&form_escalar_order),
            model.cost(&form_id_order),
            "test setup: the two Map forms must be cost-equal so the pick is a pure tie-break"
        );
        let expected = std::cmp::min(form_escalar_order.clone(), form_id_order.clone());
        assert_eq!(
            expected, form_escalar_order,
            "sanity: [#0,#1] is the EScalar-Ord-smaller form"
        );

        let extracted = eg
            .extract(root, &model)
            .expect("merged Map class must extract");
        assert_eq!(
            extracted, expected,
            "extraction must resolve scalar Ids to EScalar content and pick the \
             Ord-smaller plan [#0,#1] — a raw-Id comparator would have picked [#1,#0]"
        );
    }

    /// Non-gating measurement (in the SP3a/SP3b tradition): report scalar-class
    /// sharing — distinct scalar e-classes vs total scalar references — over the
    /// corpus, quantifying the CSE structure SP4b will exploit. Run with:
    ///   bin/cargo-test -p mz-transform -- --run-ignored ignored-only scalar_sharing
    #[mz_ore::test]
    #[ignore] // measurement only; not part of the behavior-neutral gate.
    fn scalar_sharing() {
        use crate::eqsat::egraph::{EGraph, Id};
        use std::collections::BTreeSet;

        // Lower the whole corpus into ONE shared graph so identical scalars across
        // plans hash-cons into a single class (the sharing being measured).
        let mut eg = EGraph::new();
        for rel in scalar_bearing_corpus() {
            eg.add_rel(&rel);
        }
        eg.rebuild();

        // Total scalar references = every scalar `Id` slot across all relational
        // nodes; distinct scalar classes = their canonical-id set.
        let mut total_refs = 0usize;
        let mut distinct: BTreeSet<Id> = BTreeSet::new();
        for id in eg.class_ids() {
            for node in eg.rel_class_nodes(id) {
                for sid in node.scalar_children() {
                    total_refs += 1;
                    distinct.insert(eg.find(sid));
                }
            }
        }
        let distinct_classes = distinct.len();
        let ratio = if total_refs == 0 {
            0.0
        } else {
            distinct_classes as f64 / total_refs as f64
        };
        println!(
            "scalar-sharing: {distinct_classes} distinct scalar classes / \
             {total_refs} scalar references (distinct/refs = {ratio:.3}; lower means more sharing)"
        );
        assert!(total_refs > 0, "corpus must contain scalar references");
    }
}
