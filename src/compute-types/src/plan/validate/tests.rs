// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::Cell;
use std::num::NonZeroU64;

use mz_expr::{
    LetRecLimit, MapFilterProject, MfpPlan, MirScalarExpr, SafeMfpPlan, permutation_for_arrangement,
};
use mz_repr::{
    Datum, Diff, RelationDesc, ReprScalarType, Row, SqlRelationType, SqlScalarType, StableRow,
    Timestamp,
};
use timely::progress::Antichain;

use super::*;
use crate::dataflows::{BuildDesc, IndexDesc, IndexImport, SourceImport};
use crate::plan::join::delta_join::{DeltaPathPlan, DeltaStagePlan};
use crate::plan::join::linear_join::LinearStagePlan;
use crate::plan::join::{DeltaJoinPlan, JoinClosure, JoinPlan, LinearJoinPlan};
use crate::plan::reduce::{AccumulablePlan, KeyValPlan, LirAggregateExpr, ReducePlan};
use crate::plan::scalar::LirScalarExpr;
use crate::plan::threshold::{BasicThresholdPlan, ThresholdPlan};
use crate::plan::top_k::{BasicTopKPlan, TopKPlan};
use crate::plan::{ArrangementStrategy, AvailableCollections, GetPlan};
use crate::sinks::{ComputeSinkConnection, ComputeSinkDesc, SubscribeSinkConnection};
use crate::sources::{SourceInstanceArguments, SourceInstanceDesc};

thread_local! {
    static NEXT_LIR_ID: Cell<u64> = const { Cell::new(1) };
}

/// Wraps `node` with a fresh `LirId`.
fn plan(node: LirRelationNode) -> LirRelationExpr {
    let id = NEXT_LIR_ID.with(|next| {
        let id = next.get();
        next.set(id + 1);
        id
    });
    node.as_plan(LirId(id))
}

fn col(c: usize) -> LirScalarExpr {
    LirScalarExpr::column(c)
}

/// The `(key, permutation, thinning)` triple of an arrangement by `key` columns.
fn triple(key: &[usize], arity: usize) -> (Vec<LirScalarExpr>, Vec<usize>, Vec<usize>) {
    let key: Vec<_> = key.iter().map(|c| col(*c)).collect();
    let (permutation, thinning) = permutation_for_arrangement(&key, arity);
    (key, permutation, thinning)
}

/// A `Get` of a global id that passes the given forms through.
fn get_global(id: GlobalId, keys: AvailableCollections) -> LirRelationExpr {
    plan(LirRelationNode::Get {
        id: Id::Global(id),
        keys,
        plan: GetPlan::PassArrangements,
    })
}

fn get_local(id: LocalId) -> LirRelationExpr {
    plan(LirRelationNode::Get {
        id: Id::Local(id),
        keys: AvailableCollections::new_raw(),
        plan: GetPlan::PassArrangements,
    })
}

fn raw_mfp(
    input_arity: usize,
    exprs: Vec<LirScalarExpr>,
    preds: Vec<(usize, LirScalarExpr)>,
    proj: Vec<usize>,
) -> MapFilterProject<LirScalarExpr> {
    MapFilterProject {
        expressions: exprs,
        predicates: preds,
        projection: proj,
        input_arity,
    }
}

/// An `Mfp` over a raw input with the given input arity.
fn mfp(
    input: LirRelationExpr,
    input_arity: usize,
    exprs: Vec<LirScalarExpr>,
    preds: Vec<(usize, LirScalarExpr)>,
    proj: Vec<usize>,
) -> LirRelationExpr {
    plan(LirRelationNode::Mfp {
        input: Box::new(input),
        mfp: MfpPlan::from_parts(
            SafeMfpPlan::from_mfp(raw_mfp(input_arity, exprs, preds, proj)),
            vec![],
            vec![],
        ),
        input_key_val: None,
    })
}

fn constant(rows: Vec<Vec<Datum>>) -> LirRelationExpr {
    plan(LirRelationNode::Constant {
        rows: Ok(rows
            .into_iter()
            .map(|r| (StableRow(Row::pack_slice(&r)), Timestamp::MIN, Diff::ONE))
            .collect()),
    })
}

fn negate(input: LirRelationExpr) -> LirRelationExpr {
    plan(LirRelationNode::Negate {
        input: Box::new(input),
    })
}

fn union(inputs: Vec<LirRelationExpr>) -> LirRelationExpr {
    plan(LirRelationNode::Union {
        temporal_bucketing_strategies: vec![ArrangementStrategy::Direct; inputs.len()],
        inputs,
        consolidate_output: false,
    })
}

fn limit() -> Option<LetRecLimit> {
    Some(LetRecLimit {
        max_iters: NonZeroU64::new(10).expect("non-zero"),
        return_at_limit: false,
    })
}

const T1: GlobalId = GlobalId::Transient(1);
const T2: GlobalId = GlobalId::Transient(2);
const OUT: GlobalId = GlobalId::Transient(10);

fn int8_type(arity: usize) -> SqlRelationType {
    SqlRelationType::new(vec![SqlScalarType::Int64.nullable(false); arity])
}

/// A dataflow importing each `(id, arity)` from persist, and building `objects` in order.
fn dataflow(
    imports: &[(GlobalId, usize)],
    objects: Vec<(GlobalId, LirRelationExpr)>,
) -> LirDataflowDescription {
    let mut df = LirDataflowDescription::new("test".into());
    for (id, arity) in imports {
        df.source_imports.insert(
            *id,
            SourceImport {
                desc: SourceInstanceDesc {
                    arguments: SourceInstanceArguments { operators: None },
                    storage_metadata: (),
                    typ: int8_type(*arity),
                },
                monotonic: false,
                with_snapshot: true,
                upper: Antichain::new(),
            },
        );
    }
    df.objects_to_build = objects
        .into_iter()
        .map(|(id, plan)| BuildDesc { id, plan })
        .collect();
    df
}

/// Validates `df` and returns its errors, which must exist.
fn errors(df: &LirDataflowDescription) -> Vec<ValidationError> {
    match validate_dataflow(df, &ValidateConfig::default()) {
        Ok(_) => panic!("expected the dataflow to be rejected"),
        Err(errs) => errs,
    }
}

#[track_caller]
fn assert_error(errs: &[ValidationError], rule: Rule, needle: &str) {
    assert!(
        errs.iter()
            .any(|e| e.rule == rule && e.message.contains(needle)),
        "no {rule:?} error containing {needle:?} in {errs:#?}"
    );
}

#[mz_ore::test]
fn accepts_minimal_valid_plan() {
    let plan = mfp(
        get_global(T1, AvailableCollections::new_raw()),
        2,
        vec![col(1)],
        vec![(2, col(0).call_unary(mz_expr::func::IsNull))],
        vec![2, 0],
    );
    let df = dataflow(&[(T1, 2)], vec![(OUT, plan)]);
    let types = validate_dataflow(&df, &ValidateConfig::default()).expect("valid");
    assert!(types.contains_key(&OUT));
}

#[mz_ore::test]
fn rejects_duplicate_lir_id() {
    let get = get_global(T1, AvailableCollections::new_raw());
    let lir_id = get.lir_id;
    let plan = LirRelationNode::Negate {
        input: Box::new(get),
    }
    .as_plan(lir_id);
    let errs = errors(&dataflow(&[(T1, 1)], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Scoping, "used more than once");
}

#[mz_ore::test]
fn rejects_unbound_global_get() {
    let plan = get_global(T2, AvailableCollections::new_raw());
    let errs = errors(&dataflow(&[(T1, 1)], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Scoping, "neither an import nor a built object");
}

#[mz_ore::test]
fn rejects_get_of_later_object() {
    let first = get_global(T2, AvailableCollections::new_raw());
    let second = get_global(T1, AvailableCollections::new_raw());
    let errs = errors(&dataflow(&[(T1, 1)], vec![(OUT, first), (T2, second)]));
    assert_error(&errs, Rule::Scoping, "not built before this one");
}

#[mz_ore::test]
fn rejects_unbound_local_get() {
    let plan = get_local(LocalId::new(1));
    let errs = errors(&dataflow(&[], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Scoping, "not bound by an enclosing Let");
}

#[mz_ore::test]
fn rejects_local_get_outside_its_let() {
    // `l1` is bound in the body of the inner `Let`, which is the value of the outer one.
    let l1 = LocalId::new(1);
    let inner = plan(LirRelationNode::Let {
        id: l1,
        value: Box::new(get_global(T1, AvailableCollections::new_raw())),
        body: Box::new(get_local(l1)),
    });
    let outer = plan(LirRelationNode::Let {
        id: LocalId::new(2),
        value: Box::new(inner),
        body: Box::new(get_local(l1)),
    });
    let errs = errors(&dataflow(&[(T1, 1)], vec![(OUT, outer)]));
    assert_error(&errs, Rule::Scoping, "local id l1 is not bound");
}

#[mz_ore::test]
fn rejects_column_out_of_range() {
    let plan = mfp(
        get_global(T1, AvailableCollections::new_raw()),
        5,
        vec![col(7)],
        vec![],
        vec![0],
    );
    let errs = errors(&dataflow(&[(T1, 5)], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Arity, "column 7");
    assert_error(&errs, Rule::Arity, "arity 5");
}

#[mz_ore::test]
fn rejects_mfp_input_arity_mismatch() {
    let plan = mfp(
        get_global(T1, AvailableCollections::new_raw()),
        3,
        vec![],
        vec![],
        vec![0],
    );
    let errs = errors(&dataflow(&[(T1, 2)], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Arity, "expects input arity 3");
}

#[mz_ore::test]
fn rejects_letrec_length_mismatch() {
    let plan = plan(LirRelationNode::LetRec {
        ids: vec![LocalId::new(1), LocalId::new(2)],
        values: vec![get_global(T1, AvailableCollections::new_raw())],
        limits: vec![limit()],
        body: Box::new(get_global(T1, AvailableCollections::new_raw())),
    });
    let errs = errors(&dataflow(&[(T1, 1)], vec![(OUT, plan)]));
    assert_error(
        &errs,
        Rule::Shape,
        "LetRec has 2 ids, 1 values, and 1 limits",
    );
}

#[mz_ore::test]
fn accepts_recursive_binding() {
    // l1 = T1 + l1, read by the body.
    let l1 = LocalId::new(1);
    let value = union(vec![
        get_global(T1, AvailableCollections::new_raw()),
        get_local(l1),
    ]);
    let plan = plan(LirRelationNode::LetRec {
        ids: vec![l1],
        values: vec![value],
        limits: vec![limit()],
        body: Box::new(get_local(l1)),
    });
    let df = dataflow(&[(T1, 2)], vec![(OUT, plan)]);
    validate_dataflow(&df, &ValidateConfig::default()).expect("valid");
}

#[mz_ore::test]
fn rejects_recursive_binding_without_arity() {
    // l1 = l1 has no rows and no arity.
    let l1 = LocalId::new(1);
    let plan = plan(LirRelationNode::LetRec {
        ids: vec![l1],
        values: vec![get_local(l1)],
        limits: vec![limit()],
        body: Box::new(get_local(l1)),
    });
    let errs = errors(&dataflow(&[], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Arity, "recursive binding l1");
}

#[mz_ore::test]
fn rejects_misplaced_let() {
    let l1 = LocalId::new(1);
    let let_ = plan(LirRelationNode::Let {
        id: l1,
        value: Box::new(get_global(T1, AvailableCollections::new_raw())),
        body: Box::new(get_local(l1)),
    });
    let errs = errors(&dataflow(&[(T1, 1)], vec![(OUT, negate(let_))]));
    assert_error(&errs, Rule::Shape, "prefix of bindings");
}

#[mz_ore::test]
fn rejects_rebound_local_id() {
    let l1 = LocalId::new(1);
    let inner = plan(LirRelationNode::Let {
        id: l1,
        value: Box::new(get_local(l1)),
        body: Box::new(get_local(l1)),
    });
    let outer = plan(LirRelationNode::Let {
        id: l1,
        value: Box::new(get_global(T1, AvailableCollections::new_raw())),
        body: Box::new(inner),
    });
    let errs = errors(&dataflow(&[(T1, 1)], vec![(OUT, outer)]));
    assert_error(&errs, Rule::Scoping, "bound more than once");
}

#[mz_ore::test]
fn rejects_join_input_index() {
    let join = plan(LirRelationNode::Join {
        inputs: vec![get_global(T1, AvailableCollections::new_raw())],
        plan: JoinPlan::Linear(LinearJoinPlan {
            source_relation: 3,
            source_key: None,
            initial_closure: None,
            stage_plans: vec![],
            final_closure: None,
        }),
    });
    let errs = errors(&dataflow(&[(T1, 1)], vec![(OUT, join)]));
    assert_error(&errs, Rule::Shape, "source relation 3 does not exist");
}

#[mz_ore::test]
fn accepts_linear_join() {
    // T1 (2 columns) streams into a lookup of T2 (2 columns) arranged by its column 0, on
    // T1's column 0. The closure sees the key, T1's column 1, and T2's column 1.
    let mut df = dataflow(&[(T1, 2)], vec![]);
    df.index_imports.insert(
        GlobalId::Transient(3),
        IndexImport {
            desc: IndexDesc {
                on_id: T2,
                key: vec![MirScalarExpr::column(0)],
            },
            typ: ReprRelationType::from(&int8_type(2)),
            monotonic: false,
            with_snapshot: true,
        },
    );
    let closure = JoinClosure {
        ready_equivalences: vec![],
        before: SafeMfpPlan::from_mfp(raw_mfp(3, vec![], vec![], vec![0, 1, 2])),
    };
    let join = plan(LirRelationNode::Join {
        inputs: vec![
            get_global(T1, AvailableCollections::new_raw()),
            get_global(
                T2,
                AvailableCollections::new_arranged(vec![triple(&[0], 2)]),
            ),
        ],
        plan: JoinPlan::Linear(LinearJoinPlan {
            source_relation: 0,
            source_key: None,
            initial_closure: None,
            stage_plans: vec![LinearStagePlan {
                lookup_relation: 1,
                stream_key: vec![col(0)],
                stream_thinning: vec![1],
                lookup_key: vec![col(0)],
                closure,
            }],
            final_closure: None,
        }),
    });
    df.objects_to_build.push(BuildDesc {
        id: OUT,
        plan: join,
    });
    let types = validate_dataflow(&df, &ValidateConfig::default()).expect("valid");
    assert!(types.contains_key(&OUT));
}

#[mz_ore::test]
fn rejects_join_closure_wider_than_stage() {
    let mut df = dataflow(&[(T1, 2), (T2, 2)], vec![]);
    let closure = JoinClosure {
        ready_equivalences: vec![vec![col(0), col(4)]],
        before: SafeMfpPlan::from_mfp(raw_mfp(3, vec![], vec![], vec![0])),
    };
    let join = plan(LirRelationNode::Join {
        inputs: vec![
            get_global(T1, AvailableCollections::new_raw()),
            get_global(T2, AvailableCollections::new_raw()),
        ],
        plan: JoinPlan::Linear(LinearJoinPlan {
            source_relation: 0,
            source_key: None,
            initial_closure: None,
            stage_plans: vec![LinearStagePlan {
                lookup_relation: 1,
                stream_key: vec![col(0)],
                stream_thinning: vec![1],
                lookup_key: vec![col(0)],
                closure,
            }],
            final_closure: None,
        }),
    });
    df.objects_to_build.push(BuildDesc {
        id: OUT,
        plan: join,
    });
    let errs = errors(&df);
    assert_error(
        &errs,
        Rule::Arity,
        "column 4, which is out of range for arity 3",
    );
}

#[mz_ore::test]
fn rejects_predicate_before_support() {
    // The predicate reads column 1 but is evaluated once the row has one column.
    let plan = mfp(
        get_global(T1, AvailableCollections::new_raw()),
        2,
        vec![],
        vec![(1, col(1))],
        vec![0],
    );
    let errs = errors(&dataflow(&[(T1, 2)], vec![(OUT, plan)]));
    assert_error(
        &errs,
        Rule::Shape,
        "predicate at position 1 references column 1",
    );
}

#[mz_ore::test]
fn rejects_empty_constant() {
    let errs = errors(&dataflow(&[], vec![(OUT, constant(vec![]))]));
    assert_error(
        &errs,
        Rule::Arity,
        "cannot infer the arity of an empty constant",
    );
}

#[mz_ore::test]
fn accepts_empty_constant_with_arity_from_context() {
    let plan = union(vec![
        mfp(constant(vec![]), 7, vec![], vec![], vec![0, 1]),
        constant(vec![]),
        get_global(T1, AvailableCollections::new_raw()),
    ]);
    let df = dataflow(&[(T1, 2)], vec![(OUT, plan)]);
    validate_dataflow(&df, &ValidateConfig::default()).expect("valid");
}

/// Objects `v` and `i = ArrangeBy(Get v)`, with an index export at `i` on `v`, as
/// `DataflowDescription::export_index` builds them.
fn indexed_view(v_plan: LirRelationExpr) -> LirDataflowDescription {
    let v = GlobalId::Transient(20);
    let i = GlobalId::Transient(21);
    let arrange = plan(LirRelationNode::ArrangeBy {
        input_key: None,
        input: Box::new(get_global(v, AvailableCollections::new_raw())),
        input_mfp: MfpPlan::from_parts(
            SafeMfpPlan::from_mfp(MapFilterProject::new(1)),
            vec![],
            vec![],
        ),
        forms: AvailableCollections::new_arranged(vec![triple(&[0], 1)]),
        strategy: ArrangementStrategy::Direct,
    });
    let mut df = dataflow(&[], vec![(v, v_plan), (i, arrange)]);
    df.index_exports.insert(
        i,
        (
            IndexDesc {
                on_id: v,
                key: vec![MirScalarExpr::column(0)],
            },
            ReprRelationType::from(&int8_type(1)),
        ),
    );
    df
}

#[mz_ore::test]
fn accepts_index_on_empty_view() {
    let df = indexed_view(constant(vec![]));
    let types = validate_dataflow(&df, &ValidateConfig::default()).expect("valid");
    assert_eq!(types[&GlobalId::Transient(21)].arity(), 1);
}

#[mz_ore::test]
fn accepts_index_on_error_constant_view() {
    let error = plan(LirRelationNode::Constant {
        rows: Err(mz_expr::EvalError::DivisionByZero),
    });
    let df = indexed_view(error);
    validate_dataflow(&df, &ValidateConfig::default()).expect("valid");
}

#[mz_ore::test]
fn checks_stated_arity_over_empty_input() {
    // The Mfp states arity 2 over an empty input, which the Union compares with arity 3.
    let plan = union(vec![
        mfp(constant(vec![]), 7, vec![], vec![], vec![0, 1]),
        get_global(T1, AvailableCollections::new_raw()),
    ]);
    let errs = errors(&dataflow(&[(T1, 3)], vec![(OUT, plan)]));
    assert_error(
        &errs,
        Rule::Arity,
        "Union input 1 has arity 3, but input 0 has arity 2",
    );
}

#[mz_ore::test]
fn rejects_constant_rows_of_different_arity() {
    let plan = constant(vec![
        vec![Datum::Int64(1)],
        vec![Datum::Int64(1), Datum::Null],
    ]);
    let errs = errors(&dataflow(&[], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Arity, "constant row 1 has 2 columns");
}

#[mz_ore::test]
fn rejects_union_arity_mismatch() {
    let plan = union(vec![
        get_global(T1, AvailableCollections::new_raw()),
        get_global(T2, AvailableCollections::new_raw()),
    ]);
    let errs = errors(&dataflow(&[(T1, 1), (T2, 2)], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Arity, "Union input 1 has arity 2");
}

#[mz_ore::test]
fn rejects_union_strategy_length_mismatch() {
    let plan = plan(LirRelationNode::Union {
        inputs: vec![get_global(T1, AvailableCollections::new_raw())],
        consolidate_output: false,
        temporal_bucketing_strategies: vec![],
    });
    let errs = errors(&dataflow(&[(T1, 1)], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Shape, "temporal bucketing strategies");
}

#[mz_ore::test]
fn rejects_non_transient_placeholder_collision() {
    // An object with the id of an import.
    let plan = get_global(T1, AvailableCollections::new_raw());
    let errs = errors(&dataflow(&[(T1, 1)], vec![(T1, plan)]));
    assert_error(
        &errs,
        Rule::Scoping,
        "same id as an import or an earlier object",
    );
}

#[mz_ore::test]
fn reports_each_problem_once() {
    // The Negate over a failed input is not reported, the second, independent error is.
    let plan = union(vec![
        negate(get_global(T2, AvailableCollections::new_raw())),
        mfp(
            get_global(T1, AvailableCollections::new_raw()),
            1,
            vec![],
            vec![],
            vec![3],
        ),
    ]);
    let errs = errors(&dataflow(&[(T1, 1)], vec![(OUT, plan)]));
    assert_eq!(errs.len(), 2, "{errs:#?}");
    assert_error(&errs, Rule::Scoping, "neither an import");
    assert_error(&errs, Rule::Arity, "projection references column 3");
}

#[mz_ore::test]
fn error_display() {
    let plan = get_global(T2, AvailableCollections::new_raw());
    let lir_id = plan.lir_id;
    let errs = errors(&dataflow(&[], vec![(OUT, plan)]));
    assert_eq!(
        errs[0].to_string(),
        format!(
            "LIR node {lir_id} (Get): Scoping: global id t2 is neither an import nor a built object"
        )
    );
}

#[mz_ore::test]
fn deep_plan_does_not_overflow() {
    // Dropping a plan this deep recurses once per level, more than the default test thread
    // stack holds in unoptimized builds, so the plan is dropped on a thread with a larger
    // stack. Validation runs on the test thread.
    let mut plan = get_global(T1, AvailableCollections::new_raw());
    for _ in 0..5000 {
        plan = negate(plan);
    }
    let df = dataflow(&[(T1, 1)], vec![(OUT, plan)]);
    let result = validate_dataflow(&df, &ValidateConfig::default());
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(move || drop(df))
        .expect("spawn")
        .join()
        .expect("drop does not panic");
    let errs = result.expect_err("too deep");
    assert_error(&errs, Rule::Shape, "nests deeper than");
}

/// Imports index `idx` on `on` by `key` columns, over a collection of `arity` columns.
fn import_index(
    df: &mut LirDataflowDescription,
    idx: GlobalId,
    on: GlobalId,
    key: &[usize],
    arity: usize,
) {
    df.index_imports.insert(
        idx,
        IndexImport {
            desc: IndexDesc {
                on_id: on,
                key: key.iter().map(|c| MirScalarExpr::column(*c)).collect(),
            },
            typ: ReprRelationType::from(&int8_type(arity)),
            monotonic: false,
            with_snapshot: true,
        },
    );
}

fn index_export(
    df: &mut LirDataflowDescription,
    id: GlobalId,
    on: GlobalId,
    key: &[usize],
    arity: usize,
) {
    df.index_exports.insert(
        id,
        (
            IndexDesc {
                on_id: on,
                key: key.iter().map(|c| MirScalarExpr::column(*c)).collect(),
            },
            ReprRelationType::from(&int8_type(arity)),
        ),
    );
}

fn arranged(key: &[usize], arity: usize) -> AvailableCollections {
    AvailableCollections::new_arranged(vec![triple(key, arity)])
}

fn raw_and_arranged(key: &[usize], arity: usize) -> AvailableCollections {
    AvailableCollections {
        raw: true,
        arranged: vec![triple(key, arity)],
    }
}

fn get_local_keys(id: LocalId, keys: AvailableCollections) -> LirRelationExpr {
    plan(LirRelationNode::Get {
        id: Id::Local(id),
        keys,
        plan: GetPlan::PassArrangements,
    })
}

/// A closure that passes on all of its `width` columns.
fn identity_closure(width: usize) -> JoinClosure {
    JoinClosure {
        ready_equivalences: vec![],
        before: SafeMfpPlan::from_mfp(raw_mfp(width, vec![], vec![], (0..width).collect())),
    }
}

/// A linear join of two inputs of two columns each on their column 0, with the given source
/// key and stream thinning.
fn linear_join(
    inputs: Vec<LirRelationExpr>,
    source_key: Option<Vec<LirScalarExpr>>,
    stream_thinning: Vec<usize>,
) -> LirRelationExpr {
    plan(LirRelationNode::Join {
        inputs,
        plan: JoinPlan::Linear(LinearJoinPlan {
            source_relation: 0,
            source_key,
            initial_closure: None,
            stage_plans: vec![LinearStagePlan {
                lookup_relation: 1,
                stream_key: vec![col(0)],
                stream_thinning,
                lookup_key: vec![col(0)],
                closure: identity_closure(3),
            }],
            final_closure: None,
        }),
    })
}

fn identity_mfp_plan(arity: usize) -> MfpPlan<LirScalarExpr> {
    MfpPlan::from_parts(
        SafeMfpPlan::from_mfp(MapFilterProject::new(arity)),
        vec![],
        vec![],
    )
}

fn arrange_by(
    input: LirRelationExpr,
    forms: AvailableCollections,
    arity: usize,
) -> LirRelationExpr {
    plan(LirRelationNode::ArrangeBy {
        input_key: None,
        input: Box::new(input),
        input_mfp: identity_mfp_plan(arity),
        forms,
        strategy: ArrangementStrategy::Direct,
    })
}

fn threshold(input: LirRelationExpr, key: &[usize], arity: usize) -> LirRelationExpr {
    plan(LirRelationNode::Threshold {
        input: Box::new(input),
        threshold_plan: ThresholdPlan::Basic(BasicThresholdPlan {
            ensure_arrangement: triple(key, arity),
        }),
    })
}

#[mz_ore::test]
fn rejects_missing_join_arrangement() {
    // T2 is imported raw only, but the stage looks it up arranged by column 0.
    let join = linear_join(
        vec![
            get_global(T1, AvailableCollections::new_raw()),
            get_global(T2, AvailableCollections::new_raw()),
        ],
        None,
        vec![1],
    );
    let errs = errors(&dataflow(&[(T1, 2), (T2, 2)], vec![(OUT, join)]));
    assert_error(&errs, Rule::Arrangement, "input 1 arranged by [#0]");
}

#[mz_ore::test]
fn accepts_join_with_arranged_inputs() {
    // Both inputs are indexes by column 0, and the join streams T1's arrangement.
    let mut df = dataflow(&[], vec![]);
    import_index(&mut df, GlobalId::Transient(3), T1, &[0], 2);
    import_index(&mut df, GlobalId::Transient(4), T2, &[0], 2);
    let join = linear_join(
        vec![
            get_global(T1, arranged(&[0], 2)),
            get_global(T2, arranged(&[0], 2)),
        ],
        Some(vec![col(0)]),
        vec![1],
    );
    df.objects_to_build.push(BuildDesc {
        id: OUT,
        plan: join,
    });
    validate_dataflow(&df, &ValidateConfig::default()).expect("valid");
}

#[mz_ore::test]
fn accepts_delta_join_with_arranged_inputs() {
    let mut df = dataflow(&[], vec![]);
    import_index(&mut df, GlobalId::Transient(3), T1, &[0], 2);
    import_index(&mut df, GlobalId::Transient(4), T2, &[0], 2);
    let path = |source: usize, lookup: usize| DeltaPathPlan {
        source_relation: source,
        source_key: Some(vec![col(0)]),
        initial_closure: identity_closure(2),
        stage_plans: vec![DeltaStagePlan {
            lookup_relation: lookup,
            stream_key: vec![col(0)],
            stream_thinning: vec![1],
            lookup_key: vec![col(0)],
            closure: identity_closure(3),
        }],
        final_closure: None,
    };
    let join = plan(LirRelationNode::Join {
        inputs: vec![
            get_global(T1, arranged(&[0], 2)),
            get_global(T2, arranged(&[0], 2)),
        ],
        plan: JoinPlan::Delta(DeltaJoinPlan {
            path_plans: vec![path(0, 1), path(1, 0)],
        }),
    });
    df.objects_to_build.push(BuildDesc {
        id: OUT,
        plan: join,
    });
    validate_dataflow(&df, &ValidateConfig::default()).expect("valid");
}

#[mz_ore::test]
fn rejects_non_canonical_stream_thinning() {
    let mut df = dataflow(&[(T1, 2)], vec![]);
    import_index(&mut df, GlobalId::Transient(4), T2, &[0], 2);
    let join = linear_join(
        vec![
            get_global(T1, AvailableCollections::new_raw()),
            get_global(T2, arranged(&[0], 2)),
        ],
        None,
        vec![0],
    );
    df.objects_to_build.push(BuildDesc {
        id: OUT,
        plan: join,
    });
    let errs = errors(&df);
    assert_error(&errs, Rule::Arrangement, "stream thinning [0]");
}

#[mz_ore::test]
fn rejects_pass_arrangements_missing_key() {
    let plan = get_global(T1, arranged(&[0], 2));
    let errs = errors(&dataflow(&[(T1, 2)], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Arrangement, "arranged by [#0]");
}

#[mz_ore::test]
fn rejects_pass_arrangements_missing_raw() {
    let mut df = dataflow(&[], vec![]);
    import_index(&mut df, GlobalId::Transient(3), T1, &[0], 2);
    df.objects_to_build.push(BuildDesc {
        id: OUT,
        plan: get_global(T1, AvailableCollections::new_raw()),
    });
    let errs = errors(&df);
    assert_error(&errs, Rule::Arrangement, "Get reads the raw collection");
}

#[mz_ore::test]
fn rejects_get_passing_no_form() {
    let mut df = dataflow(&[], vec![]);
    import_index(&mut df, GlobalId::Transient(3), T1, &[0], 2);
    df.objects_to_build.push(BuildDesc {
        id: OUT,
        plan: get_global(T1, AvailableCollections::default()),
    });
    let errs = errors(&df);
    assert_error(&errs, Rule::Arrangement, "passes on no form");
}

#[mz_ore::test]
fn rejects_topk_without_raw_collection() {
    let mut df = dataflow(&[], vec![]);
    import_index(&mut df, GlobalId::Transient(3), T1, &[0], 2);
    let top_k = plan(LirRelationNode::TopK {
        input: Box::new(get_global(T1, arranged(&[0], 2))),
        top_k_plan: TopKPlan::Basic(BasicTopKPlan {
            group_key: vec![0],
            order_key: vec![],
            offset: 0,
            limit: None,
            arity: 2,
            buckets: vec![],
        }),
        temporal_bucketing_strategy: ArrangementStrategy::Direct,
    });
    df.objects_to_build.push(BuildDesc {
        id: OUT,
        plan: top_k,
    });
    let errs = errors(&df);
    assert_error(&errs, Rule::Arrangement, "TopK reads the raw collection");
}

#[mz_ore::test]
fn rejects_wrong_thinning() {
    let (key, permutation, _thinning) = triple(&[0], 2);
    let forms = AvailableCollections::new_arranged(vec![(key, permutation, vec![0, 1])]);
    let plan = arrange_by(get_global(T1, AvailableCollections::new_raw()), forms, 2);
    let errs = errors(&dataflow(&[(T1, 2)], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Arrangement, "thinning [0, 1]");
}

#[mz_ore::test]
fn rejects_arrange_by_existing_key() {
    let mut df = dataflow(&[], vec![]);
    import_index(&mut df, GlobalId::Transient(3), T1, &[0], 2);
    let plan = arrange_by(get_global(T1, arranged(&[0], 2)), arranged(&[0], 2), 2);
    df.objects_to_build.push(BuildDesc { id: OUT, plan });
    let errs = errors(&df);
    assert_error(&errs, Rule::Arrangement, "already arranged by [#0]");
}

#[mz_ore::test]
fn rejects_arrange_by_without_raw_or_input_key() {
    // Forming a new arrangement from an arranged-only input needs an input key.
    let mut df = dataflow(&[], vec![]);
    import_index(&mut df, GlobalId::Transient(3), T1, &[0], 2);
    let plan = arrange_by(get_global(T1, arranged(&[0], 2)), arranged(&[1], 2), 2);
    df.objects_to_build.push(BuildDesc { id: OUT, plan });
    let errs = errors(&df);
    assert_error(
        &errs,
        Rule::Arrangement,
        "ArrangeBy reads the raw collection",
    );
}

#[mz_ore::test]
fn rejects_negate_of_threshold() {
    // Threshold produces only its arrangement, and Negate reads the raw collection.
    let plan = negate(threshold(
        arrange_by(
            get_global(T1, AvailableCollections::new_raw()),
            arranged(&[0, 1], 2),
            2,
        ),
        &[0, 1],
        2,
    ));
    let errs = errors(&dataflow(&[(T1, 2)], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Arrangement, "Negate reads the raw collection");
}

#[mz_ore::test]
fn checks_arrangements_over_empty_input() {
    // No rows reach the Threshold, but rendering still looks up its arrangement.
    let mut df = dataflow(&[], vec![(OUT, threshold(constant(vec![]), &[0], 1))]);
    index_export(&mut df, OUT, OUT, &[0], 1);
    let errs = errors(&df);
    assert_error(
        &errs,
        Rule::Arrangement,
        "Threshold reads the arrangement by [#0]",
    );
}

#[mz_ore::test]
fn rejects_index_export_without_arrangement() {
    let mut df = dataflow(
        &[(T1, 1)],
        vec![(OUT, get_global(T1, AvailableCollections::new_raw()))],
    );
    index_export(&mut df, OUT, T1, &[0], 1);
    let errs = errors(&df);
    assert_error(
        &errs,
        Rule::Arrangement,
        "index export t10 reads the arrangement by [#0]",
    );
}

#[mz_ore::test]
fn rejects_sink_without_readable_form() {
    let mut df = dataflow(
        &[(T1, 1)],
        vec![(
            OUT,
            threshold(
                arrange_by(
                    get_global(T1, AvailableCollections::new_raw()),
                    arranged(&[0], 1),
                    1,
                ),
                &[0],
                1,
            ),
        )],
    );
    subscribe(&mut df, OUT, None);
    let errs = errors(&df);
    assert_error(&errs, Rule::Arrangement, "sink export t30");
}

/// Adds a subscribe `t30` of the one-column collection `from`, reading `from_arrangement`.
fn subscribe(
    df: &mut LirDataflowDescription,
    from: GlobalId,
    from_arrangement: Option<(Vec<LirScalarExpr>, MfpPlan<LirScalarExpr>)>,
) {
    df.sink_exports.insert(
        GlobalId::Transient(30),
        ComputeSinkDesc {
            from,
            from_desc: RelationDesc::from_names_and_types([(
                "a",
                SqlScalarType::Int64.nullable(false),
            )]),
            connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection {
                output: vec![],
            }),
            with_snapshot: false,
            up_to: Antichain::new(),
            non_null_assertions: vec![],
            refresh_schedule: None,
            from_arrangement,
        },
    );
}

#[mz_ore::test]
fn snapshotless_index_serves_sinks_only() {
    // Without its snapshot, the index import provides its arrangement or the raw collection,
    // depending on the replica's configuration. A sink reads either, an object neither.
    let mut df = dataflow(&[], vec![]);
    import_index(&mut df, GlobalId::Transient(3), T1, &[0], 1);
    df.index_imports
        .get_mut(&GlobalId::Transient(3))
        .expect("imported")
        .with_snapshot = false;
    subscribe(&mut df, T1, Some((vec![col(0)], identity_mfp_plan(1))));
    validate_dataflow(&df, &ValidateConfig::default()).expect("valid");

    df.objects_to_build.push(BuildDesc {
        id: OUT,
        plan: get_global(T1, arranged(&[0], 1)),
    });
    let errs = errors(&df);
    assert_error(&errs, Rule::Arrangement, "arranged by [#0]");
}

#[mz_ore::test]
fn rejects_index_key_lir_cannot_express() {
    let mut df = dataflow(&[], vec![]);
    import_index(&mut df, GlobalId::Transient(3), T1, &[0], 1);
    df.index_imports
        .get_mut(&GlobalId::Transient(3))
        .expect("imported")
        .desc
        .key = vec![MirScalarExpr::CallUnmaterializable(
        mz_expr::UnmaterializableFunc::MzNow,
    )];
    let errs = errors(&df);
    assert_error(&errs, Rule::Function, "index import t3 key");
}

#[mz_ore::test]
fn rejects_letrec_value_without_raw() {
    let l1 = LocalId::new(1);
    let value = threshold(
        arrange_by(
            get_global(T1, AvailableCollections::new_raw()),
            arranged(&[0], 1),
            1,
        ),
        &[0],
        1,
    );
    let plan = plan(LirRelationNode::LetRec {
        ids: vec![l1],
        values: vec![value],
        limits: vec![limit()],
        body: Box::new(get_local(l1)),
    });
    let errs = errors(&dataflow(&[(T1, 1)], vec![(OUT, plan)]));
    assert_error(
        &errs,
        Rule::Arrangement,
        "value of l1 lacks the raw collection",
    );
}

#[mz_ore::test]
fn letrec_arrangements_reach_later_values_only() {
    // l1 is arranged, l2 reads that arrangement, and the body sees only raw collections.
    let l1 = LocalId::new(1);
    let l2 = LocalId::new(2);
    let letrec = |body: LirRelationExpr| {
        let v1 = arrange_by(
            get_global(T1, AvailableCollections::new_raw()),
            raw_and_arranged(&[0], 1),
            1,
        );
        let v2 = get_local_keys(l1, raw_and_arranged(&[0], 1));
        plan(LirRelationNode::LetRec {
            ids: vec![l1, l2],
            values: vec![v1, v2],
            limits: vec![limit(), limit()],
            body: Box::new(body),
        })
    };
    let accepted = letrec(get_local_keys(l1, AvailableCollections::new_raw()));
    let df = dataflow(&[(T1, 1)], vec![(OUT, accepted)]);
    validate_dataflow(&df, &ValidateConfig::default()).expect("valid");
    let rejected = letrec(get_local_keys(l1, arranged(&[0], 1)));
    let errs = errors(&dataflow(&[(T1, 1)], vec![(OUT, rejected)]));
    assert_error(&errs, Rule::Arrangement, "arranged by [#0]");
}

#[mz_ore::test]
fn accepts_get_keys_with_identity_permutation() {
    // Lowering states an identity permutation and a trivial thinning for an arrangement by a
    // non-prefix key once a residual MFP is the identity under its permutation, and copies
    // them into `Get`s of the binding.
    let l1 = LocalId::new(1);
    let mut df = dataflow(&[], vec![]);
    import_index(&mut df, GlobalId::Transient(3), T1, &[1], 3);
    let stated =
        AvailableCollections::new_arranged(vec![(vec![col(1)], vec![0, 1, 2], vec![0, 1])]);
    let let_ = plan(LirRelationNode::Let {
        id: l1,
        value: Box::new(get_global(T1, arranged(&[1], 3))),
        body: Box::new(get_local_keys(l1, stated)),
    });
    df.objects_to_build.push(BuildDesc {
        id: OUT,
        plan: let_,
    });
    validate_dataflow(&df, &ValidateConfig::default()).expect("valid");
}

#[mz_ore::test]
fn rejects_get_keys_of_wrong_length() {
    let mut df = dataflow(&[], vec![]);
    import_index(&mut df, GlobalId::Transient(3), T1, &[1], 3);
    let stated = AvailableCollections::new_arranged(vec![(vec![col(1)], vec![0, 1, 2], vec![0])]);
    df.objects_to_build.push(BuildDesc {
        id: OUT,
        plan: get_global(T1, stated),
    });
    let errs = errors(&df);
    assert_error(&errs, Rule::Arrangement, "a thinning of 1 entries");
}

fn enforced() -> ValidateConfig {
    ValidateConfig {
        enforce_function_allowlist: true,
    }
}

/// Validates `df` with `config` and returns its errors, which must exist.
fn errors_with(df: &LirDataflowDescription, config: &ValidateConfig) -> Vec<ValidationError> {
    match validate_dataflow(df, config) {
        Ok(_) => panic!("expected the dataflow to be rejected"),
        Err(errs) => errs,
    }
}

fn lit(datum: Datum, typ: ReprScalarType) -> LirScalarExpr {
    LirScalarExpr::literal_ok(datum, typ)
}

fn raw_t1() -> LirRelationExpr {
    get_global(T1, AvailableCollections::new_raw())
}

/// An `Mfp` with temporal bounds.
fn temporal_mfp(
    input: LirRelationExpr,
    input_arity: usize,
    lower: Vec<LirScalarExpr>,
    upper: Vec<LirScalarExpr>,
) -> LirRelationExpr {
    let proj = (0..input_arity).collect();
    plan(LirRelationNode::Mfp {
        input: Box::new(input),
        mfp: MfpPlan::from_parts(
            SafeMfpPlan::from_mfp(raw_mfp(input_arity, vec![], vec![], proj)),
            lower,
            upper,
        ),
        input_key_val: None,
    })
}

fn top_k_with_limit(input: LirRelationExpr, arity: usize, limit: LirScalarExpr) -> LirRelationExpr {
    plan(LirRelationNode::TopK {
        input: Box::new(input),
        top_k_plan: TopKPlan::Basic(BasicTopKPlan {
            group_key: vec![],
            order_key: vec![],
            offset: 0,
            limit: Some(limit),
            arity,
            buckets: vec![],
        }),
        temporal_bucketing_strategy: ArrangementStrategy::Direct,
    })
}

fn flat_map(
    input: LirRelationExpr,
    input_arity: usize,
    exprs: Vec<LirScalarExpr>,
    func: mz_expr::TableFunc,
    func_arity: usize,
) -> LirRelationExpr {
    let width = input_arity + func_arity;
    plan(LirRelationNode::FlatMap {
        input_key: None,
        input: Box::new(input),
        exprs,
        func,
        mfp_after: identity_mfp_plan(width),
    })
}

/// A `Reduce` that groups a raw input of arity 2 by column 0 and aggregates column 1.
fn aggregate_by_first(input: LirRelationExpr, func: mz_expr::AggregateFunc) -> LirRelationExpr {
    accumulate_by_first(input, func.clone(), func)
}

/// Like [`aggregate_by_first`], but states `accumulated` as the function the accumulation
/// path applies.
fn accumulate_by_first(
    input: LirRelationExpr,
    func: mz_expr::AggregateFunc,
    accumulated: mz_expr::AggregateFunc,
) -> LirRelationExpr {
    let aggregate = |func| LirAggregateExpr {
        func,
        expr: col(1),
        distinct: false,
    };
    plan(LirRelationNode::Reduce {
        input_key: None,
        input: Box::new(input),
        key_val_plan: KeyValPlan {
            key_plan: SafeMfpPlan::from_mfp(raw_mfp(2, vec![], vec![], vec![0])),
            val_plan: SafeMfpPlan::from_mfp(raw_mfp(2, vec![], vec![], vec![1])),
        },
        plan: ReducePlan::Accumulable(AccumulablePlan {
            full_aggrs: vec![aggregate(func)],
            simple_aggrs: vec![(0, aggregate(accumulated))],
            distinct_aggrs: vec![],
        }),
        mfp_after: SafeMfpPlan::from_mfp(raw_mfp(2, vec![], vec![], vec![0, 1])),
        temporal_bucketing_strategy: ArrangementStrategy::Direct,
    })
}

#[mz_ore::test]
fn rejects_limit_not_int64() {
    let plan = top_k_with_limit(raw_t1(), 2, lit(Datum::Int32(5), ReprScalarType::Int32));
    let errs = errors(&dataflow(&[(T1, 2)], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Type, "TopK limit has type Int32");
}

#[mz_ore::test]
fn accepts_int64_limit() {
    let plan = top_k_with_limit(raw_t1(), 2, lit(Datum::Int64(5), ReprScalarType::Int64));
    validate_dataflow(&dataflow(&[(T1, 2)], vec![(OUT, plan)]), &enforced()).expect("valid");
}

#[mz_ore::test]
fn rejects_non_bool_filter() {
    let plan = mfp(raw_t1(), 2, vec![], vec![(1, col(0))], vec![0, 1]);
    let errs = errors(&dataflow(&[(T1, 2)], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Type, "predicate at position 1 has type Int64");
}

#[mz_ore::test]
fn rejects_non_bool_if_condition() {
    let cond = LirScalarExpr::If {
        cond: Box::new(col(0)),
        then: Box::new(col(0)),
        els: Box::new(col(1)),
    };
    let plan = mfp(raw_t1(), 2, vec![cond], vec![], vec![2]);
    let errs = errors(&dataflow(&[(T1, 2)], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Type, "If condition has type Int64");
}

#[mz_ore::test]
fn rejects_temporal_bound_not_mz_timestamp() {
    let plan = temporal_mfp(raw_t1(), 2, vec![col(0)], vec![]);
    let errs = errors(&dataflow(&[(T1, 2)], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Type, "temporal bound has type Int64");
}

#[mz_ore::test]
fn accepts_mz_timestamp_bounds_when_enforced() {
    let to_ts = col(0).call_unary(mz_expr::func::CastInt64ToMzTimestamp);
    let step = to_ts.clone().call_unary(mz_expr::func::StepMzTimestamp);
    let plan = temporal_mfp(raw_t1(), 2, vec![to_ts], vec![step]);
    validate_dataflow(&dataflow(&[(T1, 2)], vec![(OUT, plan)]), &enforced()).expect("valid");
}

#[mz_ore::test]
fn rejects_literal_type_mismatch() {
    let literal = lit(Datum::Int64(1), ReprScalarType::String);
    let plan = mfp(raw_t1(), 2, vec![literal], vec![], vec![2]);
    let errs = errors(&dataflow(&[(T1, 2)], vec![(OUT, plan)]));
    assert_error(
        &errs,
        Rule::Type,
        "literal is not an instance of its declared type String",
    );
}

#[mz_ore::test]
fn rejects_literal_without_datum() {
    let literal = LirScalarExpr::Literal(
        Ok(StableRow(Row::default())),
        ReprScalarType::Int64.nullable(false),
    );
    let plan = mfp(raw_t1(), 2, vec![literal], vec![], vec![2]);
    let errs = errors(&dataflow(&[(T1, 2)], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Shape, "literal holds 0 datums");
}

#[mz_ore::test]
fn rejects_all_null_constant_column() {
    let plan = constant(vec![vec![Datum::Null, Datum::Int64(1)]]);
    let df = dataflow(&[], vec![(OUT, plan)]);
    let errs = errors_with(&df, &enforced());
    assert_error(
        &errs,
        Rule::Type,
        "cannot infer the type of an all-null constant column",
    );
    // The optimizer folds `SELECT NULL` and similar queries into such constants.
    validate_dataflow(&df, &ValidateConfig::default()).expect("valid when not enforced");
}

#[mz_ore::test]
fn infers_constant_types() {
    let plan = constant(vec![
        vec![Datum::Int64(1), Datum::Null],
        vec![Datum::Int64(2), Datum::String("a")],
    ]);
    let types = validate_dataflow(&dataflow(&[], vec![(OUT, plan)]), &enforced()).expect("valid");
    assert_eq!(
        types[&OUT].column_types,
        vec![
            ReprScalarType::Int64.nullable(false),
            ReprScalarType::String.nullable(true)
        ]
    );
}

#[mz_ore::test]
fn rejects_constant_column_of_mixed_types() {
    let plan = constant(vec![vec![Datum::Int64(1)], vec![Datum::Int32(2)]]);
    let errs = errors(&dataflow(&[], vec![(OUT, plan)]));
    assert_error(
        &errs,
        Rule::Type,
        "constant column 0 mixes types Int64 and Int32",
    );
}

#[mz_ore::test]
fn rejects_unlisted_function_when_enforced() {
    let cast = col(0).call_unary(mz_expr::func::CastInt64ToString);
    let plan = mfp(raw_t1(), 2, vec![cast], vec![], vec![2]);
    let errs = errors_with(&dataflow(&[(T1, 2)], vec![(OUT, plan)]), &enforced());
    assert_error(&errs, Rule::Function, "is not supported in user plans");
}

#[mz_ore::test]
fn accepts_unlisted_function_when_not_enforced() {
    let cast = col(0).call_unary(mz_expr::func::CastInt64ToString);
    let plan = mfp(raw_t1(), 2, vec![cast], vec![], vec![2]);
    let types = validate_dataflow(
        &dataflow(&[(T1, 2)], vec![(OUT, plan)]),
        &ValidateConfig::default(),
    )
    .expect("valid");
    assert_eq!(
        types[&OUT].column_types,
        vec![ReprScalarType::String.nullable(false)]
    );
}

#[mz_ore::test]
fn rejects_eq_on_mismatched_types() {
    let eq = col(0).call_binary(
        lit(Datum::String("a"), ReprScalarType::String),
        mz_expr::func::Eq,
    );
    let plan = mfp(raw_t1(), 2, vec![], vec![(1, eq)], vec![0, 1]);
    let errs = errors_with(&dataflow(&[(T1, 2)], vec![(OUT, plan)]), &enforced());
    assert_error(&errs, Rule::Type, "arguments of the same type");
}

#[mz_ore::test]
fn accepts_allowlisted_functions_when_enforced() {
    let sum = col(0).call_binary(col(1), mz_expr::func::AddInt64);
    let eq = col(0).call_binary(
        lit(Datum::Int64(3), ReprScalarType::Int64),
        mz_expr::func::Eq,
    );
    let lt = col(1).call_binary(col(2), mz_expr::func::Lt);
    let pred = LirScalarExpr::CallVariadic {
        func: mz_expr::VariadicFunc::And(mz_expr::func::variadic::And),
        exprs: vec![eq, lt.call_unary(mz_expr::func::Not)],
    };
    let plan = mfp(raw_t1(), 2, vec![sum], vec![(3, pred)], vec![2]);
    let types =
        validate_dataflow(&dataflow(&[(T1, 2)], vec![(OUT, plan)]), &enforced()).expect("valid");
    assert_eq!(
        types[&OUT].column_types,
        vec![ReprScalarType::Int64.nullable(false)]
    );
}

#[mz_ore::test]
fn infers_output_types() {
    let sum = col(0).call_binary(col(1), mz_expr::func::AddInt64);
    let plan = mfp(raw_t1(), 2, vec![sum], vec![], vec![0, 2]);
    let types = validate_dataflow(
        &dataflow(&[(T1, 2)], vec![(OUT, plan)]),
        &ValidateConfig::default(),
    )
    .expect("valid");
    assert_eq!(
        types[&OUT].column_types,
        vec![
            ReprScalarType::Int64.nullable(false),
            ReprScalarType::Int64.nullable(false)
        ]
    );
}

#[mz_ore::test]
fn infers_aggregate_types() {
    let plan = aggregate_by_first(raw_t1(), mz_expr::AggregateFunc::SumInt64);
    let types =
        validate_dataflow(&dataflow(&[(T1, 2)], vec![(OUT, plan)]), &enforced()).expect("valid");
    assert_eq!(
        types[&OUT].column_types,
        vec![
            ReprScalarType::Int64.nullable(false),
            ReprScalarType::Numeric.nullable(false)
        ]
    );
}

#[mz_ore::test]
fn rejects_aggregate_of_wrong_type() {
    // `any` over an int8 column panics in `datum_to_accumulator`.
    let plan = aggregate_by_first(raw_t1(), mz_expr::AggregateFunc::Any);
    let errs = errors_with(&dataflow(&[(T1, 2)], vec![(OUT, plan)]), &enforced());
    assert_error(&errs, Rule::Type, "function any expects");
}

#[mz_ore::test]
fn rejects_unlisted_aggregate_when_enforced() {
    let plan = aggregate_by_first(raw_t1(), mz_expr::AggregateFunc::SumInt32);
    let errs = errors_with(&dataflow(&[(T1, 2)], vec![(OUT, plan)]), &enforced());
    assert_error(&errs, Rule::Function, "is not supported in user plans");
}

#[mz_ore::test]
fn rejects_union_type_mismatch() {
    let plan = union(vec![raw_t1(), constant(vec![vec![Datum::Int32(1)]])]);
    let errs = errors(&dataflow(&[(T1, 1)], vec![(OUT, plan)]));
    assert_error(&errs, Rule::Type, "Union input 1 column 0 has type Int32");
}

#[mz_ore::test]
fn infers_recursive_binding_types() {
    // l1 = T1 + (l1 with its first column incremented), read by the body.
    let l1 = LocalId::new(1);
    let step = col(0).call_binary(
        lit(Datum::Int64(1), ReprScalarType::Int64),
        mz_expr::func::AddInt64,
    );
    let value = union(vec![
        raw_t1(),
        mfp(get_local(l1), 2, vec![step], vec![], vec![2, 1]),
    ]);
    let plan = plan(LirRelationNode::LetRec {
        ids: vec![l1],
        values: vec![value],
        limits: vec![limit()],
        body: Box::new(get_local(l1)),
    });
    let types =
        validate_dataflow(&dataflow(&[(T1, 2)], vec![(OUT, plan)]), &enforced()).expect("valid");
    assert_eq!(
        types[&OUT].column_types,
        vec![ReprScalarType::Int64.nullable(false); 2]
    );
}

#[mz_ore::test]
fn checks_recursive_reads_against_inferred_types() {
    // l1 = T1 + (l1 with `NOT` applied to its int8 column).
    let l1 = LocalId::new(1);
    let value = union(vec![
        raw_t1(),
        mfp(
            get_local(l1),
            1,
            vec![col(0).call_unary(mz_expr::func::Not)],
            vec![],
            vec![1],
        ),
    ]);
    let plan = plan(LirRelationNode::LetRec {
        ids: vec![l1],
        values: vec![value],
        limits: vec![limit()],
        body: Box::new(get_local(l1)),
    });
    let errs = errors_with(&dataflow(&[(T1, 1)], vec![(OUT, plan)]), &enforced());
    assert_error(&errs, Rule::Type, "function NOT expects");
}

#[mz_ore::test]
fn rejects_wrap_of_width_zero_when_enforced() {
    let wrap = mz_expr::TableFunc::Wrap {
        types: vec![],
        width: 0,
    };
    let plan = flat_map(raw_t1(), 1, vec![col(0)], wrap, 0);
    let errs = errors_with(&dataflow(&[(T1, 1)], vec![(OUT, plan)]), &enforced());
    assert_error(&errs, Rule::Function, "is not supported in user plans");
}

#[mz_ore::test]
fn rejects_wrap_of_partial_row() {
    // Three arguments in rows of two leave a short last row.
    let wrap = mz_expr::TableFunc::Wrap {
        types: vec![SqlScalarType::Int64.nullable(false); 2],
        width: 2,
    };
    let plan = flat_map(raw_t1(), 1, vec![col(0), col(0), col(0)], wrap, 2);
    let errs = errors_with(&dataflow(&[(T1, 1)], vec![(OUT, plan)]), &enforced());
    assert_error(&errs, Rule::Type, "got 3 arguments");
}

#[mz_ore::test]
fn accepts_wrap_when_enforced() {
    let wrap = mz_expr::TableFunc::Wrap {
        types: vec![SqlScalarType::Int64.nullable(false); 2],
        width: 2,
    };
    let plan = flat_map(raw_t1(), 1, vec![col(0), col(0)], wrap, 2);
    let types =
        validate_dataflow(&dataflow(&[(T1, 1)], vec![(OUT, plan)]), &enforced()).expect("valid");
    assert_eq!(
        types[&OUT].column_types,
        vec![ReprScalarType::Int64.nullable(false); 3]
    );
}

#[mz_ore::test]
fn rejects_accumulated_dummy() {
    // Accumulating an `Int64` with `Dummy` panics in `datum_to_accumulator`.
    let plan = accumulate_by_first(
        raw_t1(),
        mz_expr::AggregateFunc::Count,
        mz_expr::AggregateFunc::Dummy,
    );
    let errs = errors_with(&dataflow(&[(T1, 2)], vec![(OUT, plan)]), &enforced());
    assert_error(
        &errs,
        Rule::Shape,
        "simple aggregate at position 0 is dummy, but aggregate 0 of the plan is count",
    );
}

#[mz_ore::test]
fn rejects_accumulated_any_over_int64() {
    let plan = accumulate_by_first(
        raw_t1(),
        mz_expr::AggregateFunc::Count,
        mz_expr::AggregateFunc::Any,
    );
    let errs = errors_with(&dataflow(&[(T1, 2)], vec![(OUT, plan)]), &enforced());
    assert_error(
        &errs,
        Rule::Shape,
        "simple aggregate at position 0 is any, but aggregate 0 of the plan is count",
    );
}

/// `LetRec`s nested `depth` deep through their values. Each level's binding reads `T1`, of
/// arity 1, and the innermost value reads the outermost-but-one binding, so each level needs
/// two visits of its value to find its binding's types.
fn nested_let_rec(depth: u64) -> LirRelationExpr {
    let mut value = union(vec![raw_t1(), get_local(LocalId::new(1))]);
    for level in 1..=depth {
        let id = LocalId::new(level);
        value = plan(LirRelationNode::LetRec {
            ids: vec![id],
            values: vec![value],
            limits: vec![limit()],
            body: Box::new(union(vec![raw_t1(), get_local(id)])),
        });
    }
    value
}

#[mz_ore::test]
fn accepts_nested_let_rec() {
    let df = dataflow(&[(T1, 1)], vec![(OUT, nested_let_rec(4))]);
    validate_dataflow(&df, &enforced()).expect("valid");
}

#[mz_ore::test]
fn bounds_work_on_deeply_nested_let_rec() {
    // Each level at least doubles the visits of the levels inside it, so without a bound
    // this takes more than 2^40 visits.
    let df = dataflow(&[(T1, 1)], vec![(OUT, nested_let_rec(40))]);
    let errs = errors(&df);
    assert_error(&errs, Rule::Shape, "operator visits");
}

#[mz_ore::test]
fn infers_types_of_long_binding_chains() {
    // l0 = T1 + l0, and l(i) = l(i - 1). Updating each binding as soon as its value is
    // visited finds all types in one pass, where updating them after each pass would need
    // one pass per binding, more than the iteration limit allows.
    let n = 2 * u64::try_from(LET_REC_TYPE_ITERATIONS).expect("fits");
    let ids: Vec<_> = (0..n).map(LocalId::new).collect();
    let mut values = vec![union(vec![raw_t1(), get_local(ids[0])])];
    values.extend(ids.iter().take(ids.len() - 1).map(|id| get_local(*id)));
    let plan = plan(LirRelationNode::LetRec {
        limits: vec![limit(); ids.len()],
        body: Box::new(get_local(ids[ids.len() - 1])),
        ids,
        values,
    });
    let types =
        validate_dataflow(&dataflow(&[(T1, 1)], vec![(OUT, plan)]), &enforced()).expect("valid");
    assert_eq!(
        types[&OUT].column_types,
        vec![ReprScalarType::Int64.nullable(false)]
    );
}
