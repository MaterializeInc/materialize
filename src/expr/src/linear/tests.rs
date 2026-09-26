// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::{Datum, Diff, Row, RowArena, Timestamp};

use crate::{ErrorScope, EvalError, MapFilterProject, MirScalarExpr, func};

/// Evaluates `mfp` on `input` and returns its single result.
fn eval_one(mfp: MapFilterProject, input: &[Datum], scope: ErrorScope) -> Result<Row, EvalError> {
    let plan = mfp.into_plan().expect("valid plan");
    let arena = RowArena::new();
    let mut datums = input.to_vec();
    let mut row_builder = Row::default();
    let mut results = plan
        .evaluate::<EvalError, _>(
            &mut datums,
            &arena,
            Timestamp::MIN,
            Diff::ONE,
            |_| true,
            &mut row_builder,
            scope,
        )
        .map(|result| result.map(|(row, _, _)| row).map_err(|(e, _, _)| e))
        .collect::<Vec<_>>();
    assert_eq!(results.len(), 1, "expected a single result");
    results.pop().expect("one result")
}

/// Input `(id, sin)` where `sin` fails to parse as an integer.
fn input() -> [Datum<'static>; 2] {
    [Datum::Int32(2), Datum::String("123-456-789")]
}

/// Maps `sin::int8` into column 2.
fn parse_sin() -> MapFilterProject {
    MapFilterProject::new(2).map([MirScalarExpr::column(1).call_unary(func::CastStringToInt64)])
}

#[mz_ore::test]
fn projected_away_error_is_dropped_in_cell_scope() {
    let mfp = parse_sin().project([0]);
    let row = eval_one(mfp, &input(), ErrorScope::Cell).expect("error projected away");
    assert_eq!(row, Row::pack_slice(&[Datum::Int32(2)]));
}

#[mz_ore::test]
fn projected_error_stays_in_its_cell() {
    let mfp = parse_sin().project([0, 2]);
    assert!(eval_one(mfp.clone(), &input(), ErrorScope::Row).is_err());
    let row = eval_one(mfp, &input(), ErrorScope::Cell).expect("error stays in its cell");
    let datums = row.unpack();
    assert_eq!(datums[0], Datum::Int32(2));
    let Datum::Error(err) = datums[1] else {
        panic!("expected an error datum, got {:?}", datums[1]);
    };
    assert!(matches!(
        EvalError::from_datum_error(err),
        EvalError::Int64OutOfRange(_) | EvalError::Parse(_)
    ));
}

#[mz_ore::test]
fn row_scope_elevates_error_datums_in_the_input() {
    let arena = RowArena::new();
    let err = EvalError::DivisionByZero.to_datum(&arena);
    let mfp = MapFilterProject::new(2).project([0, 1]);
    let result = eval_one(mfp.clone(), &[Datum::Int32(1), err], ErrorScope::Row);
    assert_eq!(result, Err(EvalError::DivisionByZero));
    let row = eval_one(mfp, &[Datum::Int32(1), err], ErrorScope::Cell).expect("passes through");
    assert_eq!(row.unpack()[1], err);
}

#[mz_ore::test]
fn predicate_errors_taint_the_row_in_cell_scope() {
    let mfp = parse_sin()
        .filter([MirScalarExpr::column(2).call_is_null()])
        .project([0]);
    assert!(eval_one(mfp.clone(), &input(), ErrorScope::Row).is_err());
    assert!(eval_one(mfp.clone(), &input(), ErrorScope::Boundary).is_err());
    let row = eval_one(mfp, &input(), ErrorScope::Cell).expect("the row survives, tainted");
    assert_eq!(row.unpack(), vec![Datum::Int32(2)]);
    assert!(
        row.row_error().is_some(),
        "the predicate error taints the row"
    );
}

#[mz_ore::test]
fn row_errors_pass_through_cell_scope_and_elevate_at_boundaries() {
    let arena = RowArena::new();
    let tag = EvalError::DivisionByZero.to_datum(&arena);
    // The trailing datum beyond the input arity is the input row's row-level error.
    let mfp = MapFilterProject::new(1).project([0]);
    let row = eval_one(mfp.clone(), &[Datum::Int32(1), tag], ErrorScope::Cell).expect("kept");
    assert_eq!(row.unpack(), vec![Datum::Int32(1)]);
    assert!(row.row_error().is_some());
    let result = eval_one(mfp, &[Datum::Int32(1), tag], ErrorScope::Boundary);
    assert_eq!(result, Err(EvalError::DivisionByZero));

    // A rejecting predicate drops a tainted row without an error.
    let rejecting = MapFilterProject::new(1).filter([MirScalarExpr::literal_false()]);
    let plan = rejecting
        .into_plan()
        .expect("valid")
        .into_nontemporal()
        .expect("safe");
    let mut datums = vec![Datum::Int32(1), tag];
    let mut row_buf = Row::default();
    let result = plan.evaluate_into_scoped(&mut datums, &arena, &mut row_buf, ErrorScope::Boundary);
    assert_eq!(result, Ok(None));
}

#[mz_ore::test]
fn reading_an_error_datum_follows_expression_semantics() {
    // `false AND error` is `false`, so the error in column 2 is masked.
    let mfp = parse_sin()
        .map([MirScalarExpr::literal_false()
            .and(MirScalarExpr::column(2).call_binary(MirScalarExpr::column(2), func::Eq))])
        .project([3]);
    let row = eval_one(mfp, &input(), ErrorScope::Cell).expect("error masked by AND");
    assert_eq!(row, Row::pack_slice(&[Datum::False]));
}

#[mz_ore::test]
fn predicates_combine_like_and_in_cell_scope() {
    // `sin::int8 = sin::int8` errors and `id = 3` is false for the input. Planning decides the
    // order in which the predicates run, and `false` masks the error in either order.
    let erroring = MirScalarExpr::column(2).call_binary(MirScalarExpr::column(2), func::Eq);
    let rejecting = MirScalarExpr::column(0).call_binary(
        MirScalarExpr::literal_ok(Datum::Int32(3), mz_repr::ReprScalarType::Int32),
        func::Eq,
    );
    for predicates in [
        [erroring.clone(), rejecting.clone()],
        [rejecting.clone(), erroring.clone()],
    ] {
        let plan = parse_sin()
            .filter(predicates)
            .project([0])
            .into_plan()
            .expect("valid plan")
            .into_nontemporal()
            .expect("non-temporal");
        let arena = RowArena::new();
        let mut row_buf = Row::default();
        let mut datums = input().to_vec();
        let result = plan.evaluate_into_scoped(&mut datums, &arena, &mut row_buf, ErrorScope::Cell);
        assert_eq!(result, Ok(None), "`false` masks the error");
    }
}

#[mz_ore::test]
fn boundary_scope_elevates_only_projected_errors() {
    let unprojected = parse_sin().project([0]);
    let row = eval_one(unprojected, &input(), ErrorScope::Boundary).expect("projected away");
    assert_eq!(row, Row::pack_slice(&[Datum::Int32(2)]));

    let projected = parse_sin().project([0, 2]);
    assert!(eval_one(projected, &input(), ErrorScope::Boundary).is_err());
}

#[mz_ore::test]
fn tainted_rows_are_deterministic() {
    let mfp = parse_sin()
        .filter([MirScalarExpr::column(2).call_is_null()])
        .project([0, 1]);
    let a = eval_one(mfp.clone(), &input(), ErrorScope::Cell).expect("tainted");
    let b = eval_one(mfp, &input(), ErrorScope::Cell).expect("tainted");
    assert_eq!(a, b, "retractions must reproduce the row byte for byte");
}
