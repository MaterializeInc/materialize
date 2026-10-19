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
use mz_repr::{Datum, Diff, Row, SqlRelationType, SqlScalarType, StableRow, Timestamp};
use timely::progress::Antichain;

use super::*;
use crate::dataflows::{BuildDesc, IndexDesc, IndexImport, SourceImport};
use crate::plan::join::linear_join::LinearStagePlan;
use crate::plan::join::{JoinClosure, JoinPlan, LinearJoinPlan};
use crate::plan::scalar::LirScalarExpr;
use crate::plan::{ArrangementStrategy, GetPlan};
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
        vec![(2, col(0))],
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
