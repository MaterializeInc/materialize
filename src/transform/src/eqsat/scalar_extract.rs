// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Determinism-parity scalar extractor over the combined e-graph.
//!
//! A verbatim port of `scalar/raise.rs`'s bottom-up tree-size extraction, reading
//! `CNode::Scalar` nodes from `EGraph<CombinedLang>`. The tie-break
//! (`cost` then `SNode::Ord`) and the And/Or operand `sort()` are copied exactly
//! so extraction is byte-identical to the standalone scalar engine.

// `raise` is a pinned public API that later slices wire into production use; until
// then only the round-trip test below exercises it.
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

fn node_cost(eg: &EGraph, node: &SNode, costs: &HashMap<Id, usize>) -> Option<usize> {
    let mut total = 1usize;
    for child in node.children() {
        total = total.saturating_add(*costs.get(&eg.find(child))?);
    }
    Some(total)
}

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
