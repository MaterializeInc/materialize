// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_expr_derive::sqlfunc;
use mz_repr::{Datum, DatumList};

use crate::EvalError;

#[sqlfunc(
    sqlname = "isnull",
    is_monotone = true,
    category = "Boolean",
    doc = "Returns true if the value is null."
)]
fn is_null<'a>(a: Datum<'a>) -> bool {
    a.is_null()
}

#[sqlfunc(
    sqlname = "istrue",
    category = "Boolean",
    doc = "Returns true if the value is true."
)]
fn is_true<'a>(a: Datum<'a>) -> bool {
    a == Datum::True
}

#[sqlfunc(
    sqlname = "isfalse",
    category = "Boolean",
    doc = "Returns true if the value is false."
)]
fn is_false<'a>(a: Datum<'a>) -> bool {
    a == Datum::False
}

#[sqlfunc(
    category = "PostgreSQL compatibility",
    doc = "Returns the number of bytes used to store a value."
)]
fn pg_column_size<'a>(a: Datum<'a>) -> Result<Option<i32>, EvalError> {
    match a {
        Datum::Null => Ok(None),
        datum => {
            let sz = mz_repr::datum_size(&datum);
            i32::try_from(sz)
                .map(Some)
                .or_else(|_| Err(EvalError::Int32OutOfRange(sz.to_string().into())))
        }
    }
}

#[sqlfunc(
    category = "System information",
    doc = "Returns the byte size of a row."
)]
// TODO[btv] - if we plan to keep changing row format,
// should we make this unmaterializable?
fn mz_row_size<'a>(a: DatumList<'a>) -> Result<i32, EvalError> {
    let sz = mz_repr::row_size(a.iter());
    i32::try_from(sz).or_else(|_| Err(EvalError::Int32OutOfRange(sz.to_string().into())))
}
