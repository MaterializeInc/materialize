// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Rest-list filters for `TElem::FilterSplice` right-hand sides. Both run on the
//! base `EGraph` during apply. Scalar rules are `colored: false`, so their apply
//! bodies only ever compile against the base graph.

use std::collections::HashSet;

use mz_expr::VariadicFunc;

use crate::eqsat::egraph::{CNode, EGraph, Id};
use crate::eqsat::scalar::node::SNode;

/// First-occurrence dedup by canonical e-class id. Sort-agnostic, so this backs
/// a grammar-general `dedup(xs)` over any variadic (scalar `And`/`Or`, relational
/// `Union`).
pub(crate) fn rest_dedup_by_id(g: &EGraph, ids: &[Id]) -> Vec<Id> {
    let mut seen = HashSet::new();
    ids.iter()
        .copied()
        .filter(|&id| seen.insert(g.find(id)))
        .collect()
}

/// Keep operands whose scalar-literal analysis is not `Some(Some(value))`. Drops
/// the connective unit (`true` for And, `false` for Or) from a boolean fold. The
/// per-element predicate reads the base scalar analysis, so it is scalar-specific.
pub(crate) fn rest_drop_scalar_lit(g: &EGraph, ids: &[Id], value: bool) -> Vec<Id> {
    ids.iter()
        .copied()
        .filter(|&id| scalar_lit_bool(g, id) != Some(value))
        .collect()
}

/// The boolean literal of scalar class `id`, per the base scalar `literal`
/// analysis. `None` for null / non-boolean / non-literal / no-analysis classes.
/// Mirrors `BaseView::scalar_lit_bool_or_null` collapsed to the bool case.
fn scalar_lit_bool(g: &EGraph, id: Id) -> Option<bool> {
    let (row, _ty) = g
        .data()
        .scalar
        .analysis
        .get(&g.find(id))?
        .literal
        .as_ref()?;
    let row = row.as_ref().ok()?;
    match row.unpack_first() {
        mz_repr::Datum::True => Some(true),
        mz_repr::Datum::False => Some(false),
        _ => None,
    }
}

/// One outer operand's inner-set under `inner`: the canonical, sorted, unique
/// ids of the operand's `inner` variadic node, or `{find(operand)}` if it holds
/// none. Ports `scalar::rules::inner_sets` (per operand).
fn inner_set(g: &EGraph, operand: Id, inner: &VariadicFunc) -> Vec<Id> {
    let canon = g.find(operand);
    for node in g.nodes(canon) {
        if let CNode::Scalar(SNode::CallVariadic { func, exprs }) = node {
            if &func == inner {
                let mut ids: Vec<Id> = exprs.iter().map(|&e| g.find(e)).collect();
                ids.sort();
                ids.dedup();
                return ids;
            }
        }
    }
    vec![canon]
}

/// The deterministic drop index for inner-set subsumption absorption, or `None`.
/// Mirrors `scalar::rules::absorb_and_or`'s search: the first operand `Q` (by
/// index) proper-subsumed by some distinct `P` (`inner-set(P) ⊊ inner-set(Q)`)
/// whose dropped extras `inner-set(Q) \ inner-set(P)` are all `could_error ==
/// false`.
pub(crate) fn absorb_drop_index(g: &EGraph, ids: &[Id], inner: &VariadicFunc) -> Option<usize> {
    if ids.len() < 2 {
        return None;
    }
    let sets: Vec<Vec<Id>> = ids.iter().map(|&o| inner_set(g, o, inner)).collect();
    for q in 0..sets.len() {
        for p in 0..sets.len() {
            if p == q || sets[p].len() >= sets[q].len() {
                continue;
            }
            if !sets[p].iter().all(|id| sets[q].contains(id)) {
                continue;
            }
            let extras_can_error = sets[q]
                .iter()
                .filter(|id| !sets[p].contains(id))
                .any(|&id| scalar_could_error(g, id));
            if !extras_can_error {
                return Some(q);
            }
        }
    }
    None
}

/// The kept operands after absorption, sorted by id (matching the old engine's
/// `kept.sort()` so extraction order agrees with the oracle). A single
/// remaining operand is left to `and_single`/`or_single` downstream.
pub(crate) fn rest_absorb(g: &EGraph, ids: &[Id], inner: &VariadicFunc) -> Vec<Id> {
    match absorb_drop_index(g, ids, inner) {
        Some(q) => {
            let mut kept: Vec<Id> = ids
                .iter()
                .copied()
                .enumerate()
                .filter(|(i, _)| *i != q)
                .map(|(_, id)| id)
                .collect();
            kept.sort();
            kept
        }
        None => ids.to_vec(),
    }
}

/// Whether scalar class `id` may error, per the base scalar `could_error`
/// analysis.
fn scalar_could_error(g: &EGraph, id: Id) -> bool {
    g.data()
        .scalar
        .analysis
        .get(&g.find(id))
        .map_or(false, |a| a.could_error)
}
