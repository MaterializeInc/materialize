// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Capability proof: the eqsat logical optimizer splits a `Reduce` whose
//! aggregates mix `ReductionType`s into one `Reduce` per type, joined on the
//! group key.
//!
//! `ReducePlan::create_from` panics during lowering on a single `Reduce` whose
//! aggregates span more than one reduction type (e.g. an Accumulable `sum`
//! alongside a Hierarchical `min`). Production performs the split in
//! `ReduceReduction` inside `fixpoint_logical_02`. This test verifies the eqsat
//! post-pass (`raise::logical_fixpoint_02`) reproduces that split so eqsat
//! output is lowerable, which is the prerequisite for deleting
//! `fixpoint_logical_02`.

use mz_compute_types::plan::reduce::{ReductionType, reduction_type};
use mz_expr::{AccessStrategy, AggregateExpr, AggregateFunc, Id, MirRelationExpr, MirScalarExpr};
use mz_repr::{GlobalId, ReprRelationType, ReprScalarType};

/// Build a source relation with `arity` Int64 non-nullable columns and a unique
/// transient global id, raised as an opaque `Get` leaf so eqsat does not fold it
/// away.
fn src(id: u64, arity: usize) -> MirRelationExpr {
    MirRelationExpr::Get {
        id: Id::Global(GlobalId::Transient(id)),
        typ: ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        ),
        access_strategy: AccessStrategy::UnknownOrLocal,
    }
}

/// Build `Reduce` grouped on #0 with two aggregates over #1 of different
/// reduction types: `sum(#1)` (Accumulable) and `min(#1)` (Hierarchical).
fn mixed_type_reduce() -> MirRelationExpr {
    // Sanity: the two functions really are different reduction types, otherwise
    // the test would not exercise the split.
    assert_eq!(
        reduction_type(&AggregateFunc::SumInt64),
        ReductionType::Accumulable
    );
    assert_eq!(
        reduction_type(&AggregateFunc::MinInt64),
        ReductionType::Hierarchical
    );

    let sum = AggregateExpr {
        func: AggregateFunc::SumInt64,
        expr: MirScalarExpr::column(1),
        distinct: false,
    };
    let min = AggregateExpr {
        func: AggregateFunc::MinInt64,
        expr: MirScalarExpr::column(1),
        distinct: false,
    };
    MirRelationExpr::Reduce {
        input: Box::new(src(1, 2)),
        group_key: vec![MirScalarExpr::column(0)],
        aggregates: vec![sum, min],
        monotonic: false,
        expected_group_size: None,
    }
}

/// Returns `true` iff `expr` contains a `Reduce` whose aggregates mix more than
/// one `ReductionType`. Such a node would panic in `ReducePlan::create_from`.
fn has_mixed_type_reduce(expr: &MirRelationExpr) -> bool {
    let mut found = false;
    expr.visit_pre(|e| {
        if let MirRelationExpr::Reduce { aggregates, .. } = e {
            let mut types = aggregates.iter().map(|a| reduction_type(&a.func));
            if let Some(first) = types.next() {
                if types.any(|t| t != first) {
                    found = true;
                }
            }
        }
    });
    found
}

/// Count the `Reduce` nodes in `expr`.
fn count_reduces(expr: &MirRelationExpr) -> usize {
    let mut n = 0;
    expr.visit_pre(|e| {
        if matches!(e, MirRelationExpr::Reduce { .. }) {
            n += 1;
        }
    });
    n
}

/// Returns `true` iff `expr` contains a `Join`.
fn has_join(expr: &MirRelationExpr) -> bool {
    let mut found = false;
    expr.visit_pre(|e| {
        if matches!(e, MirRelationExpr::Join { .. }) {
            found = true;
        }
    });
    found
}

#[mz_ore::test]
fn eqsat_splits_mixed_reduction_type_reduce() {
    let input = mixed_type_reduce();
    // Precondition: the input is exactly the panic-inducing shape.
    assert!(
        has_mixed_type_reduce(&input),
        "test input must contain a mixed-reduction-type Reduce"
    );
    assert_eq!(count_reduces(&input), 1, "input has a single Reduce");

    let out = mz_transform::eqsat::optimize_logical(input, false);

    // The mixed-type Reduce must be gone: no remaining Reduce mixes types, so
    // `ReducePlan::create_from` would not panic on this plan.
    assert!(
        !has_mixed_type_reduce(&out),
        "eqsat must split the mixed-reduction-type Reduce; got {out:?}"
    );
    // The split produces one Reduce per type (here two: Accumulable +
    // Hierarchical) joined on the group key.
    assert_eq!(
        count_reduces(&out),
        2,
        "expected one Reduce per reduction type after split; got {out:?}"
    );
    assert!(
        has_join(&out),
        "the per-type reduces must be joined on the group key; got {out:?}"
    );
    // Arity preserved: group key + two aggregates.
    assert_eq!(out.arity(), 3, "arity preserved; got {out:?}");
}
