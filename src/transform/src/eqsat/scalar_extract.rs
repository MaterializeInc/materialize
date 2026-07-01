// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Determinism-parity scalar extractor over the combined e-graph.
//!
//! A verbatim port of `scalar/raise.rs`'s bottom-up tree-size extraction, reading
//! `CNode::Scalar` nodes from `EGraph<CombinedLang>`. The tie-break
//! (`cost` then `SNode::Ord`) and the And/Or operand `sort()` are copied exactly
//! so extraction is byte-identical to the standalone scalar engine. The
//! rationale comments below are ported from `scalar/raise.rs` as well. See
//! there for the original write-up.

// `raise` is a pinned public API that later slices wire into production use.
// Until then, only the round-trip test below exercises it.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use mz_expr::{MirScalarExpr, VariadicFunc};

use crate::eqsat::core::Id;
use crate::eqsat::egraph::{CNode, EGraph};
use crate::eqsat::scalar::node::SNode;

/// The scalar e-nodes of `id`'s class.
fn scalar_nodes(eg: &EGraph, id: Id) -> Vec<SNode> {
    eg.nodes(id)
        .into_iter()
        .filter_map(|n| match n {
            CNode::Scalar(s) => Some(s),
            _ => None,
        })
        .collect()
}

/// Reconstruct the min-cost `MirScalarExpr` for the class of `id`.
pub fn raise(eg: &EGraph, id: Id) -> MirScalarExpr {
    let costs = compute_costs(eg, id);
    let mut memo: HashMap<Id, MirScalarExpr> = HashMap::new();
    build(eg, id, &costs, &mut memo)
}

/// Cost of the cheapest reconstruction of `node`'s subtree given per-class costs
/// of its children. Returns `None` if any child has no finite cost yet.
///
/// Child ids are canonicalized before lookup: `costs` is keyed by canonical id,
/// but a node's stored child ids can be stale when `raise` runs mid-saturation
/// (rules call it between unions, before the next `rebuild` recanonicalizes class
/// contents), so a raw lookup would spuriously miss a costed class.
fn node_cost(eg: &EGraph, node: &SNode, costs: &HashMap<Id, usize>) -> Option<usize> {
    let mut total = 1usize;
    for child in node.children() {
        total = total.saturating_add(*costs.get(&eg.find(child))?);
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
fn compute_costs(eg: &EGraph, id: Id) -> HashMap<Id, usize> {
    let mut reachable: Vec<Id> = Vec::new();
    let mut seen: HashSet<Id> = HashSet::new();
    let mut stack = vec![eg.find(id)];
    while let Some(rep) = stack.pop() {
        if !seen.insert(rep) {
            continue;
        }
        reachable.push(rep);
        for node in scalar_nodes(eg, rep) {
            for child in node.children() {
                stack.push(eg.find(child));
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
            let best = scalar_nodes(eg, rep)
                .iter()
                .filter_map(|node| node_cost(eg, node, &costs))
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
    eg: &EGraph,
    id: Id,
    costs: &HashMap<Id, usize>,
    memo: &mut HashMap<Id, MirScalarExpr>,
) -> MirScalarExpr {
    let rep = eg.find(id);
    if let Some(expr) = memo.get(&rep) {
        return expr.clone();
    }
    let nodes = scalar_nodes(eg, rep);
    // Pick the cheapest node, breaking ties by `Ord` for determinism.
    let best = nodes
        .into_iter()
        .filter(|node| node_cost(eg, node, costs).is_some())
        .min_by(|a, b| {
            let ca = node_cost(eg, a, costs).expect("filtered to finite cost");
            let cb = node_cost(eg, b, costs).expect("filtered to finite cost");
            ca.cmp(&cb).then_with(|| a.cmp(b))
        })
        .expect("non-empty class with a finite-cost node");
    let expr = reconstruct(eg, &best, costs, memo);
    memo.insert(rep, expr.clone());
    expr
}

fn reconstruct(
    eg: &EGraph,
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
            expr: Box::new(build(eg, *expr, costs, memo)),
        },
        SNode::CallBinary { func, expr1, expr2 } => MirScalarExpr::CallBinary {
            func: func.clone(),
            expr1: Box::new(build(eg, *expr1, costs, memo)),
            expr2: Box::new(build(eg, *expr2, costs, memo)),
        },
        SNode::CallVariadic { func, exprs } => {
            let mut operands: Vec<MirScalarExpr> =
                exprs.iter().map(|e| build(eg, *e, costs, memo)).collect();
            // Sort And/Or operands to match `MirScalarExpr::reduce`'s
            // `exprs.sort()` in `reduce_and_canonicalize_and_or`. This produces
            // the same canonical operand order at extraction so EXPLAIN goldens
            // differ from reduce only on genuine plan changes, not
            // operand-order noise. Sorting is exact-eval safe: And/Or are an
            // order-independent join (value, max-error, and short-circuit all
            // agree regardless of operand order). This also eliminates
            // HashSet-iteration-order nondeterminism in extracted And/Or. Only
            // And/Or are sorted. Other variadics (Coalesce, etc.) are
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
            cond: Box::new(build(eg, *cond, costs, memo)),
            then: Box::new(build(eg, *then, costs, memo)),
            els: Box::new(build(eg, *els, costs, memo)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eqsat::egraph::EGraph;
    use mz_expr::{MirScalarExpr, UnaryFunc};

    #[mz_ore::test]
    fn raise_roundtrips_lowered_scalar() {
        let e = MirScalarExpr::column(0).call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let mut eg = EGraph::new();
        let root = crate::eqsat::scalar::lower::lower_into(&mut eg, &e);
        assert_eq!(raise(&eg, root), e);
    }
}
