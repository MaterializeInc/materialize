// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Hand-built plan shapes for the surface cells random MIR cannot reach.
//!
//! [`crate::generate`] draws plans from the generator shared with the
//! `mz-transform` fuzz targets, which covers the bug-rich relational operators
//! well but has no arm for a table function or a recursive binding, never marks
//! an input monotonic, and draws only accumulable and hierarchical aggregates.
//! Those omissions are not accidents to fix in the shared generator: its draw
//! sequence determines what a stored fuzz corpus entry decodes to, and the
//! release-qualification corpus is carried between runs. Widening it would remap
//! every entry and discard that coverage.
//!
//! So the gaps are closed here instead, with plans written out rather than drawn.
//! Each shape names the cells it exists to reach, and the runner holds it to that
//! claim like any other workload, so a shape that stops producing its cell fails
//! rather than quietly covering less.
//!
//! # Input properties are part of the shape
//!
//! Some cells depend on the *data* as much as the plan. A monotonic reduce is
//! only correct over an append-only collection, so those shapes declare
//! [`ShapeInputs::AppendOnly`] and the workload is built without retractions.
//! Declaring `monotonic: true` over a collection that retracts would produce
//! wrong answers and the oracles would rightly flag them, which would be the
//! suite reporting a bug in its own test data.

use std::num::NonZeroU64;

use mz_expr::{
    AggregateExpr, AggregateFunc, ColumnOrder, EvalError, Id, LetRecLimit, LocalId,
    MirRelationExpr, MirScalarExpr, TableFunc, func,
};
use mz_repr::{Datum, GlobalId, ReprScalarType};
use mz_transform::mirgen::{Ty, nullable_relation_type};

use crate::workload::ids;

/// Whether a shape needs append-only input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShapeInputs {
    /// The default: batches carry retractions, exercising correction and
    /// consolidation.
    Retracting,
    /// Insert-only. Required by any shape declaring monotonicity, since a
    /// monotonic operator over a retracting collection is simply incorrect.
    AppendOnly,
}

/// A hand-built workload shape.
pub struct Shape {
    /// Stable name, used for the corpus filename.
    pub name: &'static str,
    /// Column types of each input the plan reads.
    pub inputs: Vec<Vec<Ty>>,
    /// Whether the inputs may retract.
    pub input_mode: ShapeInputs,
    /// Whether the MIR optimizer runs before lowering.
    pub optimize: bool,
    /// The plan, over `Get`s of the declared inputs.
    pub plan: MirRelationExpr,
    /// Why this shape exists: the cells random MIR does not reach. Documentation
    /// only, since the claim the runner enforces is computed from the lowered
    /// plan; a mismatch between the two is exactly the drift worth noticing.
    pub targets: &'static str,
}

/// A typed `Get` of input `i`.
fn get(i: usize, schema: &[Ty]) -> MirRelationExpr {
    MirRelationExpr::global_get(
        GlobalId::User(ids::input(i)),
        nullable_relation_type(schema),
    )
}

fn int64(v: i64) -> MirScalarExpr {
    MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64)
}

/// Every targeted shape.
pub fn all() -> Vec<Shape> {
    let mut shapes = vec![
        constant_error(),
        flat_map_plain(),
        flat_map_with_mfp(),
        letrec_unbounded(),
        letrec_limited(),
        letrec_limited_return_at(),
        monotonic_reduce(),
        monotonic_top1(),
        monotonic_topk(),
    ];
    shapes.extend(multi_key_join());
    shapes
}

/// A collection that is an error rather than rows.
///
/// `gen_scalar` emits error literals inside expressions, but `gen_rel` never
/// roots a collection at one, so the renderer's error-collection path was only
/// ever reached incidentally.
fn constant_error() -> Shape {
    Shape {
        name: "shape-constant-error",
        inputs: vec![],
        input_mode: ShapeInputs::Retracting,
        optimize: false,
        plan: MirRelationExpr::Constant {
            rows: Err(EvalError::DivisionByZero),
            typ: nullable_relation_type(&[Ty::Int64]),
        },
        targets: "Constant/Error",
    }
}

/// A table function over each input row.
///
/// The series bounds are literals rather than columns: `generate_series` over
/// attacker-chosen bounds would emit an unbounded number of rows, and the point
/// here is to reach the `FlatMap` render path, not to stress it.
fn flat_map_plain() -> Shape {
    let schema = vec![Ty::Int64];
    Shape {
        name: "shape-flat-map",
        inputs: vec![schema.clone()],
        input_mode: ShapeInputs::Retracting,
        optimize: false,
        plan: MirRelationExpr::FlatMap {
            input: Box::new(get(0, &schema)),
            func: TableFunc::GenerateSeriesInt64,
            exprs: vec![int64(1), int64(3), int64(1)],
        },
        targets: "FlatMap/Stream/NoMfp",
    }
}

/// A table function with a filter fused onto its output.
///
/// The lowering folds the following `Filter` into the `FlatMap`'s `mfp_after`,
/// which is a distinct render path from the unfused one.
fn flat_map_with_mfp() -> Shape {
    let schema = vec![Ty::Int64];
    let flat_map = MirRelationExpr::FlatMap {
        input: Box::new(get(0, &schema)),
        func: TableFunc::GenerateSeriesInt64,
        exprs: vec![int64(1), int64(4), int64(1)],
    };
    Shape {
        name: "shape-flat-map-mfp",
        inputs: vec![schema],
        input_mode: ShapeInputs::Retracting,
        optimize: false,
        // Keep the rows whose generated value is not 2, so the MFP is non-trivial.
        plan: flat_map.filter(vec![
            MirScalarExpr::column(1)
                .call_binary(int64(2), func::Eq)
                .not(),
        ]),
        targets: "FlatMap/Stream/MfpAfter",
    }
}

/// A recursive binding, with `limit` controlling which `LetRec` cell it reaches.
///
/// The recursion is `distinct(input ∪ self)`, which reaches a fixpoint: `Union`
/// is multiset union and would grow without bound, so the `Distinct` is what
/// makes this converge rather than run until the iteration limit.
///
/// The fold oracle cannot see through a `LetRec`, so these workloads carry the
/// export-invariance and incremental oracles instead. That is precisely where the
/// incremental oracle stops being redundant: with no independent reference, it is
/// the only check that a maintained recursive collection matches a freshly
/// computed one.
fn letrec(name: &'static str, limit: Option<LetRecLimit>, targets: &'static str) -> Shape {
    let schema = vec![Ty::Int64];
    let local = LocalId::new(0);
    let typ = nullable_relation_type(&schema);
    let recursive = MirRelationExpr::Get {
        id: Id::Local(local),
        typ: typ.clone(),
        access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
    };
    Shape {
        name,
        inputs: vec![schema.clone()],
        input_mode: ShapeInputs::Retracting,
        optimize: false,
        plan: MirRelationExpr::LetRec {
            ids: vec![local],
            values: vec![get(0, &schema).union(recursive.clone()).distinct()],
            limits: vec![limit],
            body: Box::new(recursive),
        },
        targets,
    }
}

fn letrec_unbounded() -> Shape {
    letrec("shape-letrec-unbounded", None, "LetRec/Unbounded")
}

fn letrec_limited() -> Shape {
    // Generous enough that the converging recursion finishes first: the cell is
    // about the limit being *present*, not about hitting it. Hitting it with
    // `return_at_limit: false` would raise an error instead.
    letrec(
        "shape-letrec-limited",
        Some(LetRecLimit {
            max_iters: NonZeroU64::new(100).expect("nonzero"),
            return_at_limit: false,
        }),
        "LetRec/Limited",
    )
}

fn letrec_limited_return_at() -> Shape {
    letrec(
        "shape-letrec-return-at",
        Some(LetRecLimit {
            max_iters: NonZeroU64::new(100).expect("nonzero"),
            return_at_limit: true,
        }),
        "LetRec/LimitedReturnAt",
    )
}

/// A hierarchical aggregate over an append-only collection.
///
/// `monotonic: true` selects a different reduce plan, which maintains only the
/// running extreme instead of a bucketed reduction tree. It is only correct over
/// a collection that never retracts, hence [`ShapeInputs::AppendOnly`].
fn monotonic_reduce() -> Shape {
    let schema = vec![Ty::Int64, Ty::Int64];
    Shape {
        name: "shape-monotonic-reduce",
        inputs: vec![schema.clone()],
        input_mode: ShapeInputs::AppendOnly,
        optimize: false,
        plan: MirRelationExpr::Reduce {
            input: Box::new(get(0, &schema)),
            group_key: vec![MirScalarExpr::column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::MaxInt64,
                expr: MirScalarExpr::column(1),
                distinct: false,
            }],
            monotonic: true,
            expected_group_size: None,
        },
        targets: "Reduce/Monotonic (or MonotonicConsolidating)",
    }
}

/// `LIMIT 1` over an append-only collection, which lowers to `MonotonicTop1`.
fn monotonic_top1() -> Shape {
    top_k_shape("shape-monotonic-top1", 1, "TopK/MonotonicTop1")
}

/// `LIMIT 2` over an append-only collection, which lowers to a limited
/// `MonotonicTopK` rather than the `Top1` special case.
fn monotonic_topk() -> Shape {
    top_k_shape("shape-monotonic-topk", 2, "TopK/MonotonicTopKLimited")
}

fn top_k_shape(name: &'static str, limit: i64, targets: &'static str) -> Shape {
    let schema = vec![Ty::Int64, Ty::Int64];
    Shape {
        name,
        inputs: vec![schema.clone()],
        input_mode: ShapeInputs::AppendOnly,
        optimize: false,
        plan: MirRelationExpr::TopK {
            input: Box::new(get(0, &schema)),
            group_key: vec![0],
            // A total order over both columns, so which rows the limit keeps is
            // unambiguous and the result is deterministic. A partial order would
            // let two correct implementations keep different tied rows.
            order_key: vec![
                ColumnOrder {
                    column: 0,
                    desc: false,
                    nulls_last: true,
                },
                ColumnOrder {
                    column: 1,
                    desc: false,
                    nulls_last: true,
                },
            ],
            limit: Some(int64(limit)),
            offset: 0,
            monotonic: true,
            expected_group_size: None,
        },
        targets,
    }
}

/// One collection joined on two different keys, so the optimizer arranges it
/// twice.
///
/// `ArrangeBy/Several` is the cell for a single collection carrying more than one
/// arrangement, which the drawn joins never produced. Needs the optimizer, both
/// to fill in the join implementations and to decide the arrangements.
fn multi_key_join() -> Vec<Shape> {
    let schema = vec![Ty::Int64, Ty::Int64];
    let join = MirRelationExpr::join(
        vec![get(0, &schema), get(1, &schema), get(2, &schema)],
        // input0.col0 = input1.col0, and input0.col1 = input2.col0.
        vec![vec![(0, 0), (1, 0)], vec![(0, 1), (2, 0)]],
    );
    vec![Shape {
        name: "shape-multi-key-join",
        inputs: vec![schema.clone(), schema.clone(), schema],
        input_mode: ShapeInputs::Retracting,
        optimize: true,
        plan: join,
        targets: "ArrangeBy/Several (best effort; the optimizer decides)",
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every shape's plan type-checks, which is the cheapest guard against a
    /// hand-written plan that cannot lower at all.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer`
    fn shapes_typecheck() {
        for shape in all() {
            let typ = shape.plan.typ();
            assert!(
                !typ.column_types.is_empty(),
                "{}: plan has no columns",
                shape.name
            );
        }
    }

    /// Shape names are unique, since they become corpus filenames.
    #[mz_ore::test]
    fn shape_names_are_unique() {
        let mut names: Vec<_> = all().iter().map(|s| s.name).collect();
        let count = names.len();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), count, "duplicate shape name");
    }

    /// Any shape declaring monotonicity must also declare append-only inputs.
    ///
    /// A monotonic operator over a retracting collection computes the wrong
    /// answer, and the oracles would report it as a divergence: the suite
    /// flagging a bug in its own test data rather than in compute.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer`
    fn monotonic_shapes_are_append_only() {
        use mz_expr::visit::Visit;

        for shape in all() {
            let mut monotonic = false;
            shape.plan.visit_post(&mut |e| match e {
                MirRelationExpr::Reduce { monotonic: m, .. }
                | MirRelationExpr::TopK { monotonic: m, .. } => monotonic |= *m,
                _ => {}
            });
            if monotonic {
                assert_eq!(
                    shape.input_mode,
                    ShapeInputs::AppendOnly,
                    "{}: declares monotonicity but allows retractions",
                    shape.name
                );
            }
        }
    }
}
