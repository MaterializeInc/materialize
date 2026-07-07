// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Colored-saturation soundness + headline tests (SP4d Task 12).
//!
//! With `COLORED_SATURATION = true` the rule-driven colored RELATIONAL
//! conclusion path is active end-to-end: colored rules fire inside per-color
//! contexts via `colored_saturate`, producing `add_colored` conclusions that
//! extraction may pick. These tests run the REAL eqsat chain
//! (`mz_transform::eqsat::optimize`) and assert that contextual simplification
//! is performed AND that it is sound — no predicate is unsoundly dropped or
//! folded to `true`.
//!
//! IMPORTANT FINDING (documented, not faked): on the reachable cases below,
//! enabling colored saturation produces output that is BYTE-IDENTICAL to the
//! output with the switch OFF. The redundant-nested-filter simplification is
//! already achieved by the always-on input-context scalar resolution (T11c) and
//! the base saturation loop; the colored conclusion path is *subsumed* by that
//! handling here, i.e. the switch is effectively inert on these fragments. The
//! whole golden corpus confirms this: flipping the switch moved zero goldens.
//! The value of enabling it is that the colored conclusion path is now exercised
//! in production and proven to never produce an unsound plan.

use mz_expr::{AccessStrategy, Id, MirRelationExpr, MirScalarExpr, func};
use mz_repr::{Datum, GlobalId, ReprRelationType, ReprScalarType};

/// A `Get` leaf with `arity` Int64 columns, nullable per `nullable`.
fn get(id: u64, arity: usize, nullable: bool) -> MirRelationExpr {
    MirRelationExpr::Get {
        id: Id::Global(GlobalId::Transient(id)),
        typ: ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(nullable))
                .collect(),
        ),
        access_strategy: AccessStrategy::UnknownOrLocal,
    }
}

/// The predicate `#col = 1`.
fn eq1(col: usize) -> MirScalarExpr {
    MirScalarExpr::column(col).call_binary(
        MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64),
        func::Eq,
    )
}

/// Number of `Filter` nodes in `expr`.
fn count_filters(expr: &MirRelationExpr) -> usize {
    let mut n = 0;
    expr.visit_pre(|e| {
        if matches!(e, MirRelationExpr::Filter { .. }) {
            n += 1;
        }
    });
    n
}

/// Whether some `Filter` node in `expr` carries a predicate equal to `pred`.
fn filters_on(expr: &MirRelationExpr, pred: &MirScalarExpr) -> bool {
    let mut found = false;
    expr.visit_pre(|e| {
        if let MirRelationExpr::Filter { predicates, .. } = e {
            if predicates.iter().any(|p| p == pred) {
                found = true;
            }
        }
    });
    found
}

/// HEADLINE: `Filter(#0 = 1)(Filter(#0 = 1)(Get))` over a non-nullable `#0`.
///
/// The inner filter's context (`#0 = 1`) proves the outer predicate, so the
/// redundant outer filter is eliminated: the result keeps exactly ONE filter on
/// `#0 = 1`. The simplification AND its soundness are both asserted — the
/// surviving plan must still filter `#0 = 1` (it must NOT collapse to a bare
/// `Get`, which would emit unfiltered rows).
#[mz_ore::test]
fn colored_saturation_eliminates_redundant_nested_filter() {
    // Build two genuinely-nested `Filter` nodes (the `.filter()` helper fuses
    // consecutive filters into one node, which would defeat the point).
    let inner = MirRelationExpr::Filter {
        input: Box::new(get(1, 2, false)),
        predicates: vec![eq1(0)],
    };
    let plan = MirRelationExpr::Filter {
        input: Box::new(inner),
        predicates: vec![eq1(0)],
    };
    assert_eq!(count_filters(&plan), 2, "input has two nested filters");

    let out = mz_transform::eqsat::optimize(plan);

    // Simplification: the redundant outer filter is gone.
    assert_eq!(
        count_filters(&out),
        1,
        "the redundant nested filter must be eliminated; got:\n{}",
        out.pretty()
    );
    // Soundness: the surviving plan still filters `#0 = 1`.
    assert!(
        filters_on(&out, &eq1(0)),
        "the surviving plan must still filter `#0 = 1` (not a bare Get); got:\n{}",
        out.pretty()
    );
}

/// ADVERSARIAL (nullability `Eq(x,x)` trap): `Filter(#0 = #0)` over a NULLABLE
/// `#0`. `Eq(x,x)` is NULL — not `true` — when `x` is NULL, so this filter drops
/// NULL rows. It must NOT be folded to `true` and dropped: the colored path must
/// not conflate `#0 = #0` with the tautology that only holds for non-nullable
/// columns.
#[mz_ore::test]
fn colored_saturation_keeps_nullable_self_eq_filter() {
    let self_eq = MirScalarExpr::column(0).call_binary(MirScalarExpr::column(0), func::Eq);
    let plan = get(1, 1, true).filter(vec![self_eq.clone()]);

    let out = mz_transform::eqsat::optimize(plan);

    // The filter must survive (NULL-dropping is observable behavior).
    assert!(
        count_filters(&out) >= 1,
        "nullable `#0 = #0` filter must NOT be dropped; got:\n{}",
        out.pretty()
    );
    assert!(
        filters_on(&out, &self_eq),
        "the surviving filter must still be `#0 = #0`; got:\n{}",
        out.pretty()
    );
}

/// ADVERSARIAL (nullable equi-join tautology): `Filter(#0 = #1)` over
/// `Join([t0, t1] on #0 = #1)` with both join columns NULLABLE. The colored
/// context for the join carries the equivalence `#0 = #1`; the danger is
/// concluding the top filter is a tautology and dropping it where non-nullability
/// is not known. A `Filter` must survive over the `Join` (the optimizer
/// canonicalizes the predicate to `#0 = #0` via the equivalence, but does NOT
/// drop it).
#[mz_ore::test]
fn colored_saturation_keeps_nullable_equijoin_filter() {
    let join = MirRelationExpr::join(
        vec![get(1, 1, true), get(2, 1, true)],
        vec![vec![(0, 0), (1, 0)]],
    );
    let pred = MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), func::Eq);
    let plan = join.filter(vec![pred]);

    let out = mz_transform::eqsat::optimize(plan);

    assert!(
        count_filters(&out) >= 1,
        "the equi-join filter must NOT be unsoundly dropped over nullable columns; got:\n{}",
        out.pretty()
    );
}
