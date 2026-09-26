// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_expr::{ErrorScope, EvalError, MapFilterProject, MirScalarExpr, func};
use mz_repr::{Datum, ReprScalarType, Row, RowArena};

use crate::plan::join::JoinClosure;
use crate::plan::scalar::{LirScalarExpr, lses_from_mses};

/// A closure over `(x, y)` whose equivalences are `classes`, projecting both columns.
fn closure(classes: Vec<Vec<MirScalarExpr>>) -> JoinClosure {
    let before = MapFilterProject::<LirScalarExpr>::new(2)
        .into_plan()
        .expect("valid plan")
        .into_nontemporal()
        .expect("non-temporal");
    JoinClosure {
        ready_equivalences: classes.iter().map(lses_from_mses).collect(),
        before,
    }
}

/// `x = 10 / y`, which errors for `y = 0`.
fn x_is_ten_over_y() -> Vec<MirScalarExpr> {
    let ten = MirScalarExpr::literal_ok(Datum::Int32(10), ReprScalarType::Int32);
    vec![
        MirScalarExpr::column(0),
        ten.call_binary(MirScalarExpr::column(1), func::DivInt32),
    ]
}

#[mz_ore::test]
fn erroring_equivalence_taints_the_pair_in_cell_scope() {
    let closure = closure(vec![x_is_ten_over_y()]);
    let arena = RowArena::new();
    let mut row = Row::default();

    let mut datums = vec![Datum::Int32(1), Datum::Int32(0)];
    let result = closure.apply(&mut datums, &arena, &mut row, ErrorScope::Cell);
    let tainted = result
        .expect("no error")
        .expect("the pair survives")
        .clone();
    assert_eq!(tainted.unpack(), vec![Datum::Int32(1), Datum::Int32(0)]);
    let error = tainted.row_error().expect("tainted");
    assert_eq!(
        EvalError::from_datum_error(error),
        EvalError::DivisionByZero
    );

    let mut datums = vec![Datum::Int32(1), Datum::Int32(0)];
    let result = closure.apply(&mut datums, &arena, &mut row, ErrorScope::Row);
    assert_eq!(result, Err(EvalError::DivisionByZero));
}

#[mz_ore::test]
fn unequal_equivalence_masks_an_erroring_one() {
    // `x = y` is false for the input, so the pair does not exist and has no error.
    let x_is_y = vec![MirScalarExpr::column(0), MirScalarExpr::column(1)];
    let closure = closure(vec![x_is_ten_over_y(), x_is_y]);
    let arena = RowArena::new();
    let mut row = Row::default();

    let mut datums = vec![Datum::Int32(1), Datum::Int32(0)];
    let result = closure.apply(&mut datums, &arena, &mut row, ErrorScope::Cell);
    assert_eq!(result, Ok(None));
}
