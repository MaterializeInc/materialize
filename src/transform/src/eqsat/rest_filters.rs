// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Rest-list filters for `TElem::FilterSplice` right-hand sides. Both run on the
//! base `EGraph` during apply. Scalar rules are `colored: false`, so their apply
//! bodies only ever compile against the base graph.

use std::collections::HashSet;

use crate::eqsat::egraph::{EGraph, Id};

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
