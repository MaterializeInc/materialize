// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Batch evaluation must agree with row-at-a-time evaluation on every row,
//! including the rows that null out, filter out, or fail.

use mz_repr::batch::{DatumBatch, TypedVec};
use mz_repr::{Datum, DatumVec, ReprScalarType, Row, RowArena};
use proptest::prelude::*;
use proptest::strategy::Union;

use crate::func::{AddInt64, DivInt64, Eq, Gt, IsNull, Lt, MulInt64, Not, NotEq, SubInt64};
use crate::linear::plan::SafeMfpPlan;
use crate::{BinaryFunc, EvalError, MapFilterProject, MirScalarExpr};

type Input = (Option<i64>, Option<i64>, Option<String>);

/// Every outcome of evaluating one input row.
type Outcome = Option<Result<Row, EvalError>>;

fn input_batches(rows: &[Input]) -> Vec<DatumBatch<EvalError>> {
    let mut a = DatumBatch::new(TypedVec::Int64(Vec::new()));
    let mut b = DatumBatch::new(TypedVec::Int64(Vec::new()));
    let mut c = DatumBatch::new(TypedVec::for_type(&ReprScalarType::String).unwrap());
    for (x, y, z) in rows {
        assert!(a.push_datum(x.map_or(Datum::Null, Datum::Int64)));
        assert!(b.push_datum(y.map_or(Datum::Null, Datum::Int64)));
        assert!(c.push_datum(z.as_deref().map_or(Datum::Null, Datum::String)));
    }
    vec![a, b, c]
}

fn input_row((x, y, z): &Input) -> Row {
    Row::pack_slice(&[
        x.map_or(Datum::Null, Datum::Int64),
        y.map_or(Datum::Null, Datum::Int64),
        z.as_deref().map_or(Datum::Null, Datum::String),
    ])
}

fn row_by_row(plan: &SafeMfpPlan, rows: &[Input]) -> Vec<Outcome> {
    let mut datum_vec = DatumVec::new();
    let mut row_buf = Row::default();
    rows.iter()
        .map(|input| {
            let row = input_row(input);
            let arena = RowArena::new();
            let mut datums = datum_vec.borrow_with(&row);
            match plan.evaluate_into(&mut datums, &arena, &mut row_buf) {
                Ok(Some(row)) => Some(Ok(row.clone())),
                Ok(None) => None,
                Err(err) => Some(Err(err)),
            }
        })
        .collect()
}

fn batched(plan: &SafeMfpPlan, rows: &[Input]) -> Option<Vec<Outcome>> {
    let mut outcomes: Vec<Outcome> = vec![None; rows.len()];
    plan.evaluate_batch(&input_batches(rows), rows.len(), |row, result| {
        assert!(outcomes[row].is_none(), "row {row} emitted twice");
        outcomes[row] = Some(result.map(Row::clone).map_err(EvalError::clone));
    })?;
    Some(outcomes)
}

fn plan(mfp: MapFilterProject) -> SafeMfpPlan {
    mfp.into_plan().unwrap().into_nontemporal().unwrap()
}

fn int(value: i64) -> MirScalarExpr {
    MirScalarExpr::literal_ok(Datum::Int64(value), ReprScalarType::Int64)
}

#[mz_ore::test]
fn arithmetic_comparisons_nulls_and_errors() {
    let col = MirScalarExpr::column;
    let mfp = MapFilterProject::new(3)
        .map(vec![
            col(0).call_binary(col(1), AddInt64),
            col(0).call_binary(col(1), DivInt64),
            col(0).call_binary(int(2), MulInt64),
            col(0).call_binary(col(1), SubInt64),
        ])
        .filter(vec![
            col(0).call_binary(int(3), Gt),
            col(2).call_unary(IsNull).call_unary(Not),
        ])
        .project(vec![0, 1, 3, 4, 5, 6, 2]);
    let plan = plan(mfp);

    let rows: Vec<Input> = vec![
        (Some(10), Some(2), Some("a".into())),
        (Some(1), Some(2), Some("filtered by a > 3".into())),
        (Some(10), Some(0), Some("division by zero".into())),
        (Some(i64::MAX), Some(1), Some("overflow".into())),
        (None, Some(1), Some("null a filters".into())),
        (Some(10), None, Some("null b passes, nulls out".into())),
        (Some(10), Some(2), None),
        (Some(4), Some(2), Some("last".into())),
    ];
    let expected = row_by_row(&plan, &rows);
    let actual = batched(&plan, &rows).expect("this plan has a batch form");
    assert_eq!(actual, expected);

    // Spot checks that the fixture exercises what its comments claim.
    assert!(matches!(expected[0], Some(Ok(_))));
    assert!(expected[1].is_none());
    assert!(matches!(expected[2], Some(Err(EvalError::DivisionByZero))));
    assert!(matches!(
        expected[3],
        Some(Err(EvalError::NumericFieldOverflow))
    ));
    assert!(expected[4].is_none());
    assert!(matches!(expected[5], Some(Ok(_))));
    assert!(expected[6].is_none());
}

#[mz_ore::test]
fn unsupported_shapes_report_none() {
    let col = MirScalarExpr::column;
    // Conditionals have no batch form yet.
    let mfp = MapFilterProject::new(3).map(vec![
        col(0).call_binary(int(0), Gt).if_then_else(col(0), col(1)),
    ]);
    assert!(batched(&plan(mfp), &[(Some(1), Some(2), None)]).is_none());

    // Temporal plans stay row-at-a-time.
    let temporal = crate::MfpPlan::from_parts(
        SafeMfpPlan::from_mfp(MapFilterProject::new(3)),
        vec![col(0)],
        vec![],
    );
    assert!(
        temporal
            .evaluate_batch(&input_batches(&[]), 0, |_, _| {})
            .is_none()
    );
}

fn arb_input() -> impl Strategy<Value = Input> {
    let int = Union::new_weighted(vec![
        (3, any::<i8>().prop_map(|v| Some(i64::from(v))).boxed()),
        (1, Just(Some(0i64)).boxed()),
        (1, Just(Some(i64::MAX)).boxed()),
        (1, Just(None).boxed()),
    ]);
    let string = Union::new_weighted(vec![
        (2, "[a-c]{0,2}".prop_map(Some).boxed()),
        (1, Just(None).boxed()),
    ]);
    (int.clone(), int, string)
}

fn arb_int_expr(depth: u32) -> BoxedStrategy<MirScalarExpr> {
    let leaf = Union::new(vec![
        Just(MirScalarExpr::column(0)).boxed(),
        Just(MirScalarExpr::column(1)).boxed(),
        any::<i8>().prop_map(|v| int(i64::from(v))).boxed(),
        Just(MirScalarExpr::literal_null(ReprScalarType::Int64)).boxed(),
        Just(MirScalarExpr::literal(
            Err(EvalError::DivisionByZero),
            ReprScalarType::Int64,
        ))
        .boxed(),
    ])
    .boxed();
    if depth == 0 {
        return leaf;
    }
    let arith = Union::new(vec![
        Just(BinaryFunc::from(AddInt64)),
        Just(BinaryFunc::from(SubInt64)),
        Just(BinaryFunc::from(MulInt64)),
        Just(BinaryFunc::from(DivInt64)),
    ]);
    Union::new_weighted(vec![
        (2, leaf),
        (
            3,
            (arb_int_expr(depth - 1), arb_int_expr(depth - 1), arith)
                .prop_map(|(a, b, f)| a.call_binary(b, f))
                .boxed(),
        ),
        (
            1,
            (
                arb_bool_expr(depth - 1),
                arb_int_expr(depth - 1),
                arb_int_expr(depth - 1),
            )
                .prop_map(|(c, t, e)| c.if_then_else(t, e))
                .boxed(),
        ),
    ])
    .boxed()
}

fn arb_bool_expr(depth: u32) -> BoxedStrategy<MirScalarExpr> {
    let leaf = Union::new(vec![
        Just(MirScalarExpr::literal_true()).boxed(),
        Just(MirScalarExpr::literal_false()).boxed(),
        Just(MirScalarExpr::literal_null(ReprScalarType::Bool)).boxed(),
        Just(MirScalarExpr::column(2).call_unary(IsNull)).boxed(),
    ])
    .boxed();
    if depth == 0 {
        return leaf;
    }
    let compare = Union::new(vec![
        Just(BinaryFunc::from(Gt)),
        Just(BinaryFunc::from(Lt)),
        Just(BinaryFunc::from(Eq)),
        Just(BinaryFunc::from(NotEq)),
    ]);
    Union::new_weighted(vec![
        (1, leaf),
        (
            3,
            (arb_int_expr(depth - 1), arb_int_expr(depth - 1), compare)
                .prop_map(|(a, b, f)| a.call_binary(b, f))
                .boxed(),
        ),
        (
            1,
            arb_int_expr(depth - 1)
                .prop_map(|e| e.call_unary(IsNull))
                .boxed(),
        ),
        (
            1,
            arb_bool_expr(depth - 1)
                .prop_map(|e| e.call_unary(Not))
                .boxed(),
        ),
        (
            1,
            Just(MirScalarExpr::column(2).call_binary(
                MirScalarExpr::literal_ok(Datum::String("a"), ReprScalarType::String),
                Eq,
            ))
            .boxed(),
        ),
    ])
    .boxed()
}

fn arb_mfp() -> impl Strategy<Value = MapFilterProject> {
    (
        prop::collection::vec(arb_int_expr(2), 0..3),
        prop::collection::vec(arb_bool_expr(2), 0..3),
        prop::collection::vec(0usize..6, 0..4),
    )
        .prop_map(|(expressions, predicates, projection)| {
            let arity = 3 + expressions.len();
            MapFilterProject::new(3)
                .map(expressions)
                .filter(predicates)
                .project(projection.into_iter().filter(|c| *c < arity))
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn batched_matches_row_by_row(
        mfp in arb_mfp(),
        rows in prop::collection::vec(arb_input(), 0..12),
    ) {
        let plan = plan(mfp);
        let expected = row_by_row(&plan, &rows);
        // Plans with shapes that have no batch form report `None`; the
        // deterministic tests cover that they do so honestly.
        if let Some(actual) = batched(&plan, &rows) {
            prop_assert_eq!(actual, expected);
        }
    }
}
