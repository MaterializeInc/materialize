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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::analysis::equivalences::EquivalenceClasses;
use crate::eqsat::colored_derive::ColoredLayer;
use crate::eqsat::cost::CostModel;
use crate::eqsat::egraph::{EGraph, ENode, Id};
use crate::eqsat::ir::Rel;
use crate::eqsat::objective::Objective;

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
#[derive(Debug, Clone)]
pub struct IlpExtractor;

/// Whether the subgraph reachable from `root_class` contains a directed cycle,
/// where an edge runs from a class to each child class of each of its e-nodes.
///
/// The ILP encodes a finite DAG selection, so a cyclic reachable subgraph has no
/// representable plan. Within a Let-free fragment the e-graph is acyclic by
/// construction, so this is a guard against an upstream invariant violation, not
/// an expected case. Three-color DFS over canonical class ids: a back edge to a
/// node still on the recursion stack is a cycle. Recursion depth is bounded by
/// the class count, itself bounded by the `total_nodes > 600` cap in `solve`.
fn reachable_has_cycle(
    egraph: &EGraph,
    reachable: &BTreeMap<Id, Vec<ENode>>,
    root_class: Id,
) -> bool {
    fn visit(
        egraph: &EGraph,
        reachable: &BTreeMap<Id, Vec<ENode>>,
        class: Id,
        on_stack: &mut BTreeSet<Id>,
        done: &mut BTreeSet<Id>,
    ) -> bool {
        if done.contains(&class) {
            return false;
        }
        // Already on the current DFS path: this edge closes a cycle.
        if !on_stack.insert(class) {
            return true;
        }
        if let Some(nodes) = reachable.get(&class) {
            for node in nodes {
                for child in egraph.child_classes(node) {
                    if visit(egraph, reachable, child, on_stack, done) {
                        return true;
                    }
                }
            }
        }
        on_stack.remove(&class);
        done.insert(class);
        false
    }
    let mut on_stack = BTreeSet::new();
    let mut done = BTreeSet::new();
    visit(egraph, reachable, root_class, &mut on_stack, &mut done)
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
    fn solve(&self, egraph: &EGraph, root: Id, model: &CostModel, spellings: Option<&HashMap<Id, EquivalenceClasses>>) -> Option<Rel> {
        use good_lp::{ProblemVariables, Solution, SolverModel, constraint, variable};

        // Collect every class and its e-nodes reachable from root.
        let reachable = egraph.reachable(root);

        // Model size cap: if the reachable subgraph is too large, defer to greedy.
        // MAX_ENODES is already capped upstream, but be defensive here too.
        let total_nodes: usize = reachable.values().map(|v| v.len()).sum();
        if total_nodes > 600 {
            return None;
        }

        // Defense in depth: the ILP selects a finite DAG and cannot represent a
        // cyclic plan. A Let-free fragment's e-graph is acyclic by construction
        // (recursive references stay opaque `LocalGet` leaves and are never
        // unioned into their own definition, see `engine`), so a cycle here means
        // an upstream invariant was violated. Reject it up front and let the
        // caller fall back to greedy, rather than discovering it 500 frames deep
        // in `build_selected_inner`.
        if reachable_has_cycle(egraph, &reachable, egraph.find(root)) {
            return None;
        }

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

        // Build the objective expression, in three strictly separated tiers:
        //   1. PRIMARY: count of non-oracle-covered arrangements (integer steps).
        //   2. TIME: total work-term degree of the selected nodes. This is what
        //      lets the ILP prefer an `IndexedFilter` (a bounded lookup, work 0)
        //      over the sibling full-scan `Filter` (work 1), and an operator
        //      sitting above a lookup is charged the lookup's bounded output.
        //      Without it the ILP ties the two forms on arrangements and falls
        //      through to a pure node-count tie-break, which rewards sharing the
        //      full scan over two distinct lookups (the relation_cse regression).
        //   3. NODES: a structural tie-break preferring fewer nodes.
        // The tier weights are scaled to the reachable subgraph so that each tier
        // strictly dominates the next: one arrangement outweighs all time terms,
        // and any time difference outweighs all node-count terms.
        let best_degrees = best_output_degrees(egraph, &reachable);
        let node_work: Vec<f64> = node_vars
            .iter()
            .map(|(_, node)| node_work_degree(node, egraph, &best_degrees))
            .collect();
        let work_total: f64 = node_work.iter().sum::<f64>() + 1.0;
        let w_time = 0.5 / work_total;
        let w_nodes = 0.5 * w_time / (node_vars.len() as f64 + 1.0);

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
            }
        }
        // Time tier + node-count tier, per selected node.
        for (vi, nsel) in node_sel.iter().enumerate() {
            obj_expr += (w_time * node_work[vi] + w_nodes) * *nsel;
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
                Ok(None) => return None,
                // microlp panicked; fall back to greedy.
                Err(_) => return None,
            };

        // Reconstruct the Rel top-down from root.
        let rel = Self::build_selected(egraph, root_class, &selected, model, spellings)?;
        // Post-validate polarity: reject any plan where a non-linear operator
        // (Reduce with aggregates, or TopK) has a sign-bearing input. The ILP
        // does not encode polarity constraints, so saturation may have placed a
        // Negate-rooted form in a class that feeds a non-linear reduce. Falling
        // back to greedy is always sound.
        if !validate_polarity(&rel) {
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
    fn reachable_has_cycle_detects_back_edge() {
        use crate::eqsat::egraph::{CNode, EGraph, ENode};
        use crate::eqsat::ir::Rel;
        // Construct a pathological cyclic e-graph: leaf <- Project <- Project,
        // then union the outer Project's class into the leaf's, closing a cycle
        // through congruence. The engine never builds this (recursive references
        // stay opaque `LocalGet` leaves), but the guard in `solve` must catch it.
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
        assert!(
            reachable_has_cycle(&eg, &eg.reachable(root), root),
            "the leaf <-> Project cycle must be detected",
        );

        // An ordinary acyclic plan must not be flagged (no false positive that
        // would silently disable the ILP on every well-formed fragment).
        let mut eg2 = EGraph::new();
        let acyclic = eg2.add_rel(&Rel::Project {
            outputs: vec![0],
            input: Box::new(Rel::Get {
                name: "t".into(),
                arity: 2,
            }),
        });
        let r2 = eg2.find(acyclic);
        assert!(!reachable_has_cycle(&eg2, &eg2.reachable(r2), r2));
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
        let plan = IlpExtractor
            .extract(&eg, a, &model, &ArrangementCount, None, None)
            .unwrap();
        assert_eq!(model.cost(&plan).arrangements, 0);
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
        let ilp_plan = IlpExtractor
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
        let ilp = IlpExtractor.extract(&eg, root, &model, &ArrangementCount, None, None);
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
