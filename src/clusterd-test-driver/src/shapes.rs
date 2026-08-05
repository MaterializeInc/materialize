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
//! [`ShapeData::GeneratedAppendOnly`] and the workload is built without
//! retractions. Declaring `monotonic: true` over a collection that retracts would
//! produce wrong answers and the oracles would rightly flag them, which would be
//! the suite reporting a bug in its own test data.
//!
//! Where *when* a row arrives is the point rather than what it holds, a shape
//! writes its batches out with [`ShapeData::Explicit`]: an error that only exists
//! from the second timestamp on, a graph deep enough that the recursion has to
//! iterate, a join whose keys were chosen to meet.

use std::num::NonZeroU64;

use mz_expr::func::variadic::{ArrayCreate, RecordCreate};
use mz_expr::{
    AggregateExpr, AggregateFunc, ColumnOrder, EvalError, Id, LetRecLimit, LocalId,
    MirRelationExpr, MirScalarExpr, TableFunc, VariadicFunc, func,
};
use mz_repr::{ColumnName, Datum, GlobalId, ReprScalarType, SqlScalarType};
use mz_transform::mirgen::{Ty, nullable_relation_type};

use crate::workload::{Batch, Update, Value, ids};

/// Where a shape's input data comes from.
#[derive(Debug, Clone, PartialEq)]
pub enum ShapeData {
    /// Rows drawn from the shape's name, inserted and retracted across the
    /// batches. The default, and what exercises correction and consolidation.
    Generated,
    /// As [`Self::Generated`], insert-only. Required by any shape declaring
    /// monotonicity, since a monotonic operator over a retracting collection is
    /// simply incorrect.
    GeneratedAppendOnly,
    /// Batches written out, one list per declared input.
    ///
    /// For shapes where *when* a row appears is the point rather than what it
    /// holds: an error that only exists at some timestamps, a graph whose
    /// recursion depth has to be known, a collection that empties out.
    Explicit(Vec<Vec<Batch>>),
    /// `rows` synthesized rows per input at timestamp `0`, and nothing declared.
    ///
    /// For shapes where the *size* is the point. See [`Workload::volume`] and
    /// [`crate::workload::InputSpec::volume_updates`].
    Volume { rows: usize },
}

/// A hand-built workload shape.
pub struct Shape {
    /// Stable name, used for the corpus filename.
    pub name: &'static str,
    /// Column types of each input the plan reads.
    pub inputs: Vec<Vec<Ty>>,
    /// Where the input data comes from.
    pub data: ShapeData,
    /// Whether the MIR optimizer runs before lowering.
    pub optimize: bool,
    /// The plan, over `Get`s of the declared inputs.
    pub plan: MirRelationExpr,
    /// Why this shape exists: the cells random MIR does not reach. Documentation
    /// only, since the claim the runner enforces is computed from the lowered
    /// plan; a mismatch between the two is exactly the drift worth noticing.
    pub targets: &'static str,
}

/// An `int8` row at multiplicity `diff`.
fn row64(values: &[i64], diff: i64) -> Update {
    Update {
        values: values.iter().map(|v| Value::Int64(v.to_string())).collect(),
        diff,
    }
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

/// A literal lookup into a locally-arranged collection.
///
/// `Let l = ArrangeBy(input, [#0]) in Filter(Get(l), #0 = k)`. Lowering turns a
/// filter that pins an arrangement key to a literal into a seek rather than a
/// scan.
///
/// This reaches the lookup cells without index imports, which the workload format
/// does not have. `LiteralConstraints` handles only `Get`s of *global* ids, so
/// lowering keeps its own literal-constraint path for local ids, and that is the
/// one a `Let` binding takes.
fn arrangement_lookup() -> Shape {
    let schema = vec![Ty::Int64, Ty::Int64];
    let local = LocalId::new(0);
    let typ = nullable_relation_type(&schema);
    let arranged = MirRelationExpr::ArrangeBy {
        input: Box::new(get(0, &schema)),
        keys: vec![vec![MirScalarExpr::column(0)]],
    };
    let body = MirRelationExpr::Get {
        id: Id::Local(local),
        typ,
        access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
    }
    .filter(vec![
        MirScalarExpr::column(0).call_binary(int64(1), func::Eq),
    ]);
    Shape {
        name: "shape-arrangement-lookup",
        inputs: vec![schema],
        // The sought key has to be in the data, or the seek reaches the lookup path
        // and returns nothing: the cell is covered while the operator is never
        // asked to find anything. Two rows share key `1` so the seek returns more
        // than one, and a third sits under a different key so it has something to
        // skip past.
        data: ShapeData::Explicit(vec![vec![Batch {
            updates: vec![row64(&[1, 10], 1), row64(&[1, 11], 1), row64(&[2, 20], 1)],
        }]]),
        optimize: false,
        plan: MirRelationExpr::Let {
            id: local,
            value: Box::new(arranged),
            body: Box::new(body),
        },
        targets: "Get/ArrangementLookup",
    }
}

/// Every targeted shape.
pub fn all() -> Vec<Shape> {
    vec![
        arrangement_lookup(),
        constant_error(),
        constant_rows(),
        flat_map_plain(),
        flat_map_with_mfp(),
        letrec_unbounded(),
        letrec_limited(),
        letrec_limited_return_at(),
        monotonic_reduce(),
        monotonic_top1(),
        monotonic_topk(),
        multi_key_join(),
        transitive_closure(),
        error_appears_midstream(),
        error_retracted(),
        empty_join(),
        collection_empties_out(),
        hierarchical_empties_out(),
        mutual_recursion(),
        basic_reduce("shape-basic-reduce", 1, "Reduce/BasicSingle"),
        basic_reduce("shape-basic-reduce-multiple", 2, "Reduce/BasicMultiple"),
        volume_reduce(),
        volume_join(),
        volume_arrangement(),
    ]
}

/// How many rows a volume shape synthesizes per input.
///
/// The column-paged batcher ships chunks at roughly 2 MiB, and an update of two
/// `int8` columns costs on the order of tens of bytes once time and diff are
/// counted, so a hundred thousand of them is a handful of chunks rather than one.
/// That is the point: a single chunk exercises the same code as four rows do.
///
/// Deliberately not larger. Every volume shape runs under the whole strategy
/// matrix, so the cost multiplies by eight, and each of these shapes keeps its
/// *result* small (see [`crate::workload::InputSpec::volume_updates`]) precisely so
/// that the size stays upstream where the strategies live rather than in what has
/// to be read back and compared.
const VOLUME_ROWS: usize = 100_000;

/// A hierarchical reduce over enough rows to fill more than one batcher chunk.
///
/// `max` is hierarchical, so lowering builds a bucketed reduction tree, and the
/// tree is what the paged batcher and the dictionary-compressed spine sit under.
/// Grouping on column 1 leaves 64 output rows out of a hundred thousand inputs.
fn volume_reduce() -> Shape {
    let schema = vec![Ty::Int64, Ty::Int64];
    Shape {
        name: "shape-volume-reduce",
        inputs: vec![schema.clone()],
        data: ShapeData::Volume { rows: VOLUME_ROWS },
        optimize: false,
        plan: MirRelationExpr::Reduce {
            input: Box::new(get(0, &schema)),
            group_key: vec![MirScalarExpr::column(1)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::MaxInt64,
                expr: MirScalarExpr::column(0),
                distinct: false,
            }],
            monotonic: false,
            expected_group_size: None,
        },
        targets: "Reduce/Bucketed at a size where the batcher strategies differ",
    }
}

/// A join over enough rows that both sides arrange more than one chunk.
///
/// Column 0 is unique on both sides, so the join finds one partner per key and
/// emits as many rows as it reads rather than squaring them. The `Distinct` on
/// column 1 then leaves 64 rows to read back, so the volume stays in the join's
/// arrangements where the strategy flags apply.
fn volume_join() -> Shape {
    let schema = vec![Ty::Int64, Ty::Int64];
    let join = MirRelationExpr::join(
        vec![get(0, &schema), get(1, &schema)],
        vec![vec![(0, 0), (1, 0)]],
    );
    Shape {
        name: "shape-volume-join",
        inputs: vec![schema.clone(), schema],
        data: ShapeData::Volume {
            rows: VOLUME_ROWS / 2,
        },
        optimize: true,
        plan: join.project(vec![1]).distinct(),
        targets: "Join/Linear at a size where the join strategies differ",
    }
}

/// A collection arranged by a unique key, at size.
///
/// The `ArrangeBy` is the point: it is the site dictionary compression and the
/// paged batcher both apply to, with no reduce or join above it to change what
/// reaches them. Reading it back through a `Distinct` keeps the peek small.
fn volume_arrangement() -> Shape {
    let schema = vec![Ty::Int64, Ty::Int64];
    let local = LocalId::new(0);
    let typ = nullable_relation_type(&schema);
    Shape {
        name: "shape-volume-arrangement",
        inputs: vec![schema.clone()],
        data: ShapeData::Volume { rows: VOLUME_ROWS },
        optimize: false,
        plan: MirRelationExpr::Let {
            id: local,
            value: Box::new(MirRelationExpr::ArrangeBy {
                input: Box::new(get(0, &schema)),
                keys: vec![vec![MirScalarExpr::column(0)]],
            }),
            body: Box::new(
                MirRelationExpr::Get {
                    id: Id::Local(local),
                    typ,
                    access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
                }
                .project(vec![1])
                .distinct(),
            ),
        },
        targets: "ArrangeBy at a size where the arrangement strategies differ",
    }
}

/// A literal collection of rows, unioned into a read of real data.
///
/// `Constant/Rows` is otherwise reached only when a random draw happens to keep a
/// literal collection, which makes a cell that every SQL query with a `VALUES`
/// clause produces depend on the luck of the set-cover pass. The union is what
/// keeps it: a bare constant would be a dataflow with no input at all, and the
/// interesting part is a constant flowing alongside maintained data.
fn constant_rows() -> Shape {
    let schema = vec![Ty::Int64];
    let typ = nullable_relation_type(&schema);
    let constant =
        MirRelationExpr::constant(vec![vec![Datum::Int64(7)], vec![Datum::Int64(8)]], typ);
    Shape {
        name: "shape-constant-rows",
        inputs: vec![schema.clone()],
        data: ShapeData::Generated,
        optimize: false,
        plan: get(0, &schema).union(constant),
        targets: "Constant/Rows",
    }
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
        data: ShapeData::Generated,
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
        data: ShapeData::Generated,
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
        data: ShapeData::Generated,
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
        data: ShapeData::Generated,
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
/// a collection that never retracts, hence [`ShapeData::GeneratedAppendOnly`].
fn monotonic_reduce() -> Shape {
    let schema = vec![Ty::Int64, Ty::Int64];
    Shape {
        name: "shape-monotonic-reduce",
        inputs: vec![schema.clone()],
        data: ShapeData::GeneratedAppendOnly,
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
        data: ShapeData::GeneratedAppendOnly,
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
fn multi_key_join() -> Shape {
    let schema = vec![Ty::Int64, Ty::Int64];
    let join = MirRelationExpr::join(
        vec![get(0, &schema), get(1, &schema), get(2, &schema)],
        // input0.col0 = input1.col0, and input0.col1 = input2.col0.
        vec![vec![(0, 0), (1, 0)], vec![(0, 1), (2, 0)]],
    );
    // Written out rather than generated: a join is only worth rendering if its
    // keys meet, and the values here are chosen so every input contributes a
    // matching partner. `(1, 2)` joins `(1, 9)` on column 0 and `(2, 5)` on
    // column 1.
    let left = vec![Batch {
        updates: vec![row64(&[1, 2], 1), row64(&[1, 3], 1), row64(&[2, 2], 1)],
    }];
    let by_col0 = vec![Batch {
        updates: vec![row64(&[1, 9], 1), row64(&[2, 8], 1)],
    }];
    let by_col1 = vec![Batch {
        updates: vec![row64(&[2, 5], 1), row64(&[3, 6], 1)],
    }];
    Shape {
        name: "shape-multi-key-join",
        inputs: vec![schema.clone(), schema.clone(), schema],
        data: ShapeData::Explicit(vec![left, by_col0, by_col1]),
        optimize: true,
        plan: join,
        targets: "ArrangeBy/Several (best effort; the optimizer decides)",
    }
}

/// Transitive closure over a small graph, the one shape whose recursion actually
/// iterates.
///
/// `reach = edges ∪ π(reach ⋈ edges)`, which for the path `0 → 1 → 2 → 3` needs
/// three rounds before it stops growing. The existing `LetRec` shapes converge in
/// two rounds by construction (`distinct(input ∪ self)` is already a fixpoint after
/// the first), so they reach the `LetRec` cells without ever testing iteration.
///
/// This is where the incremental oracle stops being a redundant cross-check: the
/// constant folder cannot see through a `LetRec`, so a maintained recursive
/// collection has no independent reference, and comparing it against a freshly
/// computed one is the only thing that can tell a correct fixpoint from a wrong
/// one.
fn transitive_closure() -> Shape {
    let schema = vec![Ty::Int64, Ty::Int64];
    let local = LocalId::new(0);
    let typ = nullable_relation_type(&schema);
    let reach = MirRelationExpr::Get {
        id: Id::Local(local),
        typ,
        access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
    };
    // reach(a, b) ⋈ edges(b, c), keeping (a, c).
    let step = MirRelationExpr::join(
        vec![reach.clone(), get(0, &schema)],
        vec![vec![(0, 1), (1, 0)]],
    )
    .project(vec![0, 3]);
    Shape {
        name: "shape-transitive-closure",
        inputs: vec![schema.clone()],
        // A three-hop path, extended at the second timestamp so the recursion has
        // to grow the closure incrementally rather than only from a snapshot.
        data: ShapeData::Explicit(vec![vec![
            Batch {
                updates: vec![row64(&[0, 1], 1), row64(&[1, 2], 1)],
            },
            Batch {
                updates: vec![row64(&[2, 3], 1)],
            },
        ]]),
        optimize: true,
        plan: MirRelationExpr::LetRec {
            ids: vec![local],
            values: vec![get(0, &schema).union(step).distinct()],
            limits: vec![None],
            body: Box::new(reach),
        },
        targets: "LetRec/Unbounded with a recursion that iterates",
    }
}

/// A collection that holds rows at one timestamp and an error at the next.
///
/// `1 / (c0 - 1)` is fine while `c0` avoids `1` and divides by zero once a row with
/// `c0 = 1` arrives. Nothing in the generated corpus can produce this: every input
/// writes all of its rows in the first batch, so an erroring plan errors from the
/// first timestamp onward and the transition is never rendered.
///
/// The transition is the interesting part. An error is not a value in the `ok`
/// collection but a row in a separate `err` collection, so an error appearing
/// mid-stream is a *retraction of the rows* plus an insertion into `err`, and every
/// export has to reflect that at the right timestamp: the index, the persist sink,
/// and the subscribe each carry errors differently.
fn error_appears_midstream() -> Shape {
    let schema = vec![Ty::Int64];
    Shape {
        name: "shape-error-appears",
        inputs: vec![schema.clone()],
        data: ShapeData::Explicit(vec![vec![
            Batch {
                updates: vec![row64(&[3], 1), row64(&[4], 1)],
            },
            // Divides by zero from here on.
            Batch {
                updates: vec![row64(&[1], 1)],
            },
        ]]),
        optimize: false,
        plan: get(0, &schema).map(vec![int64(1).call_binary(
            MirScalarExpr::column(0).call_binary(int64(1), func::SubInt64),
            func::DivInt64,
        )]),
        targets: "an error entering the err collection mid-stream",
    }
}

/// The reverse of [`error_appears_midstream`]: the erroring row is retracted, so
/// the collection stops being an error and becomes rows again.
///
/// Recovering from an error is the direction more likely to be wrong, since it
/// requires the `err` collection to retract too rather than merely to accumulate.
fn error_retracted() -> Shape {
    let schema = vec![Ty::Int64];
    Shape {
        name: "shape-error-retracted",
        inputs: vec![schema.clone()],
        data: ShapeData::Explicit(vec![vec![
            Batch {
                updates: vec![row64(&[1], 1), row64(&[4], 1)],
            },
            Batch {
                updates: vec![row64(&[1], -1)],
            },
        ]]),
        optimize: false,
        plan: get(0, &schema).map(vec![int64(1).call_binary(
            MirScalarExpr::column(0).call_binary(int64(1), func::SubInt64),
            func::DivInt64,
        )]),
        targets: "an error leaving the err collection mid-stream",
    }
}

/// A join one of whose inputs is empty.
///
/// The empty case is worth testing on purpose. It used to be tested by accident,
/// which is different: a fifth of all drawn leaves came out empty, so most join
/// workloads compared an empty result against an empty reference and passed without
/// joining anything. Now the draws are non-empty and this shape owns the case.
fn empty_join() -> Shape {
    let schema = vec![Ty::Int64, Ty::Int64];
    Shape {
        name: "shape-empty-join",
        inputs: vec![schema.clone(), schema.clone()],
        data: ShapeData::Explicit(vec![
            vec![Batch {
                updates: vec![row64(&[1, 2], 1), row64(&[2, 3], 1)],
            }],
            vec![Batch { updates: vec![] }],
        ]),
        optimize: true,
        plan: MirRelationExpr::join(
            vec![get(0, &schema), get(1, &schema)],
            vec![vec![(0, 0), (1, 0)]],
        ),
        targets: "a join over an empty input",
    }
}

/// `array_agg`, the one Basic aggregate expressible over integer columns.
///
/// Basic is the third reduction type, rendered by `render_basic_aggregates`, and
/// nothing else here reaches it: the generator draws only accumulable and
/// hierarchical aggregates, and every other Basic function takes jsonb, text or a
/// record. `ArrayConcat` takes an array of any element type, so an `int8` column
/// can feed it.
///
/// `ArrayConcat` accumulates records whose first field is the array to concatenate,
/// the remaining fields being the `order_by` columns, so the aggregate's input is
/// `row(array[c1])` even with no ordering. That shape is not obvious from the
/// variant, and getting it wrong panics in `unwrap_list` rather than failing a
/// type check. `n_aggregates` picks between the single and multiple Basic plans,
/// which are different render paths.
fn basic_reduce(name: &'static str, n_aggregates: usize, targets: &'static str) -> Shape {
    let schema = vec![Ty::Int64, Ty::Int64];
    let agg_over = |column: usize| AggregateExpr {
        func: AggregateFunc::ArrayConcat { order_by: vec![] },
        expr: MirScalarExpr::CallVariadic {
            func: VariadicFunc::RecordCreate(RecordCreate {
                field_names: vec![ColumnName::from("elements")],
            }),
            exprs: vec![MirScalarExpr::CallVariadic {
                func: VariadicFunc::ArrayCreate(ArrayCreate {
                    elem_type: SqlScalarType::Int64,
                }),
                exprs: vec![MirScalarExpr::column(column)],
            }],
        },
        distinct: false,
    };
    Shape {
        name,
        inputs: vec![schema.clone()],
        // Groups of more than one row, so the concatenation has something to
        // concatenate, and a retraction so the aggregate has to be recomputed.
        data: ShapeData::Explicit(vec![vec![
            Batch {
                updates: vec![row64(&[1, 10], 1), row64(&[1, 11], 1), row64(&[2, 20], 1)],
            },
            Batch {
                updates: vec![row64(&[1, 10], -1), row64(&[2, 21], 1)],
            },
        ]]),
        optimize: false,
        plan: MirRelationExpr::Reduce {
            input: Box::new(get(0, &schema)),
            group_key: vec![MirScalarExpr::column(0)],
            aggregates: (0..n_aggregates).map(|i| agg_over(1 - i % 2)).collect(),
            monotonic: false,
            expected_group_size: None,
        },
        targets,
    }
}

/// Two mutually recursive bindings.
///
/// Every other `LetRec` shape binds one collection. The renderer's iterative scope
/// handles any number, with a feedback edge per binding, and mutual recursion is
/// what makes the bindings' iteration orders matter. `even` and `odd` alternate: a
/// row in `even` reaches `odd` on the next hop and back again.
///
/// The folder cannot see through a `LetRec`, so this rides on the export-invariance
/// and incremental oracles.
fn mutual_recursion() -> Shape {
    let schema = vec![Ty::Int64, Ty::Int64];
    let (even, odd) = (LocalId::new(0), LocalId::new(1));
    let typ = nullable_relation_type(&schema);
    let local = |id: LocalId| MirRelationExpr::Get {
        id: Id::Local(id),
        typ: typ.clone(),
        access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
    };
    // One hop along the edge relation, keeping (start, destination).
    let hop = |from: MirRelationExpr| {
        MirRelationExpr::join(vec![from, get(0, &schema)], vec![vec![(0, 1), (1, 0)]])
            .project(vec![0, 3])
    };
    Shape {
        name: "shape-mutual-recursion",
        inputs: vec![schema.clone()],
        data: ShapeData::Explicit(vec![vec![
            Batch {
                updates: vec![row64(&[0, 1], 1), row64(&[1, 2], 1), row64(&[2, 3], 1)],
            },
            Batch {
                updates: vec![row64(&[3, 4], 1)],
            },
        ]]),
        optimize: true,
        plan: MirRelationExpr::LetRec {
            ids: vec![even, odd],
            values: vec![
                // even = edges ∪ hop(odd)
                get(0, &schema).union(hop(local(odd))).distinct(),
                // odd = hop(even)
                hop(local(even)).distinct(),
            ],
            limits: vec![None, None],
            body: Box::new(local(even).union(local(odd)).distinct()),
        },
        targets: "LetRec with more than one binding",
    }
}

/// A bucketed hierarchical reduce whose groups shrink to nothing.
///
/// `min`/`max` are hierarchical: the reduce maintains a tree of partial extremes
/// rather than a running total, and a retraction has to walk back up it. Retracting
/// the current extreme is the case that makes the tree re-derive a value it had
/// already discarded, and retracting the last row of a group has to remove the
/// group. `expected_group_size` is what makes lowering build the bucketed plan
/// rather than collapsing it, so the tree is really there.
///
/// [`collection_empties_out`] is the accumulable counterpart, where a retraction is
/// just a subtraction. The two paths fail differently, which is why both exist.
fn hierarchical_empties_out() -> Shape {
    let schema = vec![Ty::Int64, Ty::Int64];
    Shape {
        name: "shape-hierarchical-empties-out",
        inputs: vec![schema.clone()],
        data: ShapeData::Explicit(vec![vec![
            Batch {
                updates: vec![
                    row64(&[1, 5], 1),
                    row64(&[1, 9], 1),
                    row64(&[1, 7], 1),
                    row64(&[2, 4], 1),
                ],
            },
            // Retract the maximum of group 1, so it has to fall back to 7.
            Batch {
                updates: vec![row64(&[1, 9], -1)],
            },
            // Empty group 2 out entirely, and take group 1 down to one row.
            Batch {
                updates: vec![row64(&[2, 4], -1), row64(&[1, 7], -1)],
            },
        ]]),
        optimize: false,
        plan: MirRelationExpr::Reduce {
            input: Box::new(get(0, &schema)),
            group_key: vec![MirScalarExpr::column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::MaxInt64,
                expr: MirScalarExpr::column(1),
                distinct: false,
            }],
            monotonic: false,
            expected_group_size: Some(4),
        },
        targets: "Reduce/Bucketed with retractions of the running extreme",
    }
}

/// A collection that fills up and then empties out completely.
///
/// The reduce keeps a count per group, so the last retraction in a group has to
/// remove the group's output row rather than leave a zero behind. A collection
/// going empty is the state most likely to be mishandled and the one a schedule
/// that only lowers multiplicities never reaches.
fn collection_empties_out() -> Shape {
    let schema = vec![Ty::Int64, Ty::Int64];
    Shape {
        name: "shape-empties-out",
        inputs: vec![schema.clone()],
        data: ShapeData::Explicit(vec![vec![
            Batch {
                updates: vec![row64(&[1, 1], 1), row64(&[1, 2], 1), row64(&[2, 1], 1)],
            },
            // Group 1 loses one of its two rows, group 2 loses its only row.
            Batch {
                updates: vec![row64(&[1, 1], -1), row64(&[2, 1], -1)],
            },
            // Everything left goes away: the collection is empty at ts 2.
            Batch {
                updates: vec![row64(&[1, 2], -1)],
            },
        ]]),
        optimize: false,
        plan: MirRelationExpr::Reduce {
            input: Box::new(get(0, &schema)),
            group_key: vec![MirScalarExpr::column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::SumInt64,
                expr: MirScalarExpr::column(1),
                distinct: false,
            }],
            monotonic: false,
            expected_group_size: None,
        },
        targets: "a maintained collection reaching empty",
    }
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
            if !monotonic {
                continue;
            }
            match &shape.data {
                ShapeData::GeneratedAppendOnly => {}
                // Written-out batches are checked directly, which is stricter than
                // taking the mode's word for it.
                ShapeData::Explicit(per_input) => {
                    for batches in per_input {
                        for batch in batches {
                            for update in &batch.updates {
                                assert!(
                                    update.diff > 0,
                                    "{}: declares monotonicity but retracts",
                                    shape.name
                                );
                            }
                        }
                    }
                }
                // Synthesized rows are inserts only, so they satisfy monotonicity.
                ShapeData::Volume { .. } => {}
                ShapeData::Generated => panic!(
                    "{}: declares monotonicity but allows retractions",
                    shape.name
                ),
            }
        }
    }

    /// A shape with written-out batches declares as many batch lists as inputs.
    ///
    /// `shape_workload` enforces this too, but failing here names the shape without
    /// needing the whole corpus to generate first.
    #[mz_ore::test]
    fn explicit_shapes_cover_every_input() {
        for shape in all() {
            if let ShapeData::Explicit(per_input) = &shape.data {
                assert_eq!(
                    per_input.len(),
                    shape.inputs.len(),
                    "{}: {} input schema(s) but {} batch list(s)",
                    shape.name,
                    shape.inputs.len(),
                    per_input.len()
                );
            }
        }
    }
}
