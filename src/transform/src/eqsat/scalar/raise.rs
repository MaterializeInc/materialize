// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Raise bridge: reconstruct a [`MirScalarExpr`] from an e-class.
//!
//! Phase 0 cost is size only (total e-node count of the reconstructed tree).
//! Extraction is bottom-up: pick the cheapest e-node in each class, recursing
//! into children first. With zero rules each class holds exactly one node, so
//! this reconstructs the lowered expression exactly.
//!
//! Form-targeting bonuses (Phase 2) will refine the objective; the structure of
//! the bottom-up DP stays the same.

use std::collections::{HashMap, HashSet};

use mz_expr::{MirScalarExpr, VariadicFunc};

use crate::eqsat::scalar::egraph::{Id, ScalarEGraph};
use crate::eqsat::scalar::node::SNode;

/// Reconstruct the min-cost [`MirScalarExpr`] for the class of `id`.
///
/// Cost is the size (e-node count) of the reconstructed subtree. Ties are
/// broken by the `Ord` on `SNode` for determinism.
///
/// Panics if a class is empty, which cannot happen for an `id` produced by
/// `lower`.
pub fn raise(egraph: &ScalarEGraph, id: Id) -> MirScalarExpr {
    let costs = compute_costs(egraph, id);
    let mut memo: HashMap<Id, MirScalarExpr> = HashMap::new();
    build(egraph, id, &costs, &mut memo)
}

/// Cost of the cheapest reconstruction of `node`'s subtree given per-class costs
/// of its children. Returns `None` if any child has no finite cost yet.
///
/// Child ids are canonicalized before lookup: `costs` is keyed by canonical id,
/// but a node's stored child ids can be stale when `raise` runs mid-saturation
/// (rules call it between unions, before the next `rebuild` recanonicalizes class
/// contents), so a raw lookup would spuriously miss a costed class.
fn node_cost(egraph: &ScalarEGraph, node: &SNode, costs: &HashMap<Id, usize>) -> Option<usize> {
    let mut total = 1usize;
    for child in node.children() {
        total = total.saturating_add(*costs.get(&egraph.find(child))?);
    }
    Some(total)
}

/// Compute `costs[class] = min over nodes of (1 + sum of child costs)` for every
/// class reachable from `id`.
///
/// Rules introduce cycles (constant folding unions a call class with its own
/// literal-descendant class, and De Morgan plus `not_not` relate a class to a
/// rewrite of itself), so a single bottom-up pass cannot cost every class: a
/// class whose only finite derivation routes through a class still on the DFS
/// stack would be missed and later panic in `build`. We instead relax to a
/// least-fixpoint. Costs only ever decrease (or go from absent to present) and
/// are bounded below by 1, and every class holds at least one finite-cost
/// derivation (the lowered subtree it came from), so the iteration converges and
/// assigns a finite cost to every reachable class.
fn compute_costs(egraph: &ScalarEGraph, id: Id) -> HashMap<Id, usize> {
    // Collect every class reachable from `id` (canonical ids).
    let mut reachable: Vec<Id> = Vec::new();
    let mut seen: HashSet<Id> = HashSet::new();
    let mut stack = vec![egraph.find(id)];
    while let Some(rep) = stack.pop() {
        if !seen.insert(rep) {
            continue;
        }
        reachable.push(rep);
        for node in egraph.nodes(rep) {
            for child in node.children() {
                stack.push(egraph.find(child));
            }
        }
    }
    // Relax until no cost improves. A class adopts a cost only when it is its
    // first finite cost or strictly cheaper than the current one, so the loop
    // makes monotone progress and terminates.
    let mut costs: HashMap<Id, usize> = HashMap::new();
    loop {
        let mut changed = false;
        for &rep in &reachable {
            let best = egraph
                .nodes(rep)
                .iter()
                .filter_map(|node| node_cost(egraph, node, &costs))
                .min();
            if let Some(best) = best {
                if costs.get(&rep).is_none_or(|&cur| best < cur) {
                    costs.insert(rep, best);
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    costs
}

/// Reconstruct the cheapest node in `id`'s class, recursing into its children.
fn build(
    egraph: &ScalarEGraph,
    id: Id,
    costs: &HashMap<Id, usize>,
    memo: &mut HashMap<Id, MirScalarExpr>,
) -> MirScalarExpr {
    let rep = egraph.find(id);
    if let Some(expr) = memo.get(&rep) {
        return expr.clone();
    }
    let nodes = egraph.nodes(rep);
    // Pick the cheapest node, breaking ties by `Ord` for determinism.
    let best = nodes
        .into_iter()
        .filter(|node| node_cost(egraph, node, costs).is_some())
        .min_by(|a, b| {
            let ca = node_cost(egraph, a, costs).expect("filtered to finite cost");
            let cb = node_cost(egraph, b, costs).expect("filtered to finite cost");
            ca.cmp(&cb).then_with(|| a.cmp(b))
        })
        .expect("non-empty class with a finite-cost node");
    let expr = reconstruct(egraph, &best, costs, memo);
    memo.insert(rep, expr.clone());
    expr
}

/// Turn a single `SNode` into a `MirScalarExpr`, recursing into child classes.
fn reconstruct(
    egraph: &ScalarEGraph,
    node: &SNode,
    costs: &HashMap<Id, usize>,
    memo: &mut HashMap<Id, MirScalarExpr>,
) -> MirScalarExpr {
    match node {
        SNode::Column(index, name) => {
            MirScalarExpr::Column(*index, mz_ore::treat_as_equal::TreatAsEqual(name.0.clone()))
        }
        SNode::Literal(lit, typ) => MirScalarExpr::Literal(lit.clone(), typ.clone()),
        SNode::CallUnmaterializable(func) => MirScalarExpr::CallUnmaterializable(func.clone()),
        SNode::CallUnary { func, expr } => MirScalarExpr::CallUnary {
            func: func.clone(),
            expr: Box::new(build(egraph, *expr, costs, memo)),
        },
        SNode::CallBinary { func, expr1, expr2 } => MirScalarExpr::CallBinary {
            func: func.clone(),
            expr1: Box::new(build(egraph, *expr1, costs, memo)),
            expr2: Box::new(build(egraph, *expr2, costs, memo)),
        },
        SNode::CallVariadic { func, exprs } => {
            let mut operands: Vec<MirScalarExpr> = exprs
                .iter()
                .map(|e| build(egraph, *e, costs, memo))
                .collect();
            // Sort And/Or operands to match `MirScalarExpr::reduce`'s
            // `exprs.sort()` in `reduce_and_canonicalize_and_or`. This produces
            // the same canonical operand order at extraction so EXPLAIN goldens
            // differ from reduce only on genuine plan changes, not
            // operand-order noise. Sorting is exact-eval safe: And/Or are an
            // order-independent join (value, max-error, and short-circuit all
            // agree regardless of operand order). This also eliminates
            // HashSet-iteration-order nondeterminism in extracted And/Or. Only
            // And/Or are sorted; other variadics (Coalesce, etc.) are
            // order-significant and are left unchanged.
            if matches!(func, VariadicFunc::And(_) | VariadicFunc::Or(_)) {
                operands.sort();
            }
            MirScalarExpr::CallVariadic {
                func: func.clone(),
                exprs: operands,
            }
        }
        SNode::If { cond, then, els } => MirScalarExpr::If {
            cond: Box::new(build(egraph, *cond, costs, memo)),
            then: Box::new(build(egraph, *then, costs, memo)),
            els: Box::new(build(egraph, *els, costs, memo)),
        },
    }
}
