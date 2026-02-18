// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_expr_derive::sqlfunc;
use mz_repr::adt::array::Array;
use mz_repr::{Datum, Int2Vector, SqlScalarType};

use crate::EvalError;
use crate::scalar::func::stringify_datum;

#[sqlfunc(
    sqlname = "int2vectortoarray",
    is_monotone = true,
    introduces_nulls = false,
    output_type_expr = SqlScalarType::Array(Box::from(SqlScalarType::Int16))
        .nullable(input_type.nullable)
)]
fn cast_int2_vector_to_array<'a>(a: Int2Vector<'a>) -> Array<'a> {
    a.0
}

#[sqlfunc(
    sqlname = "int2vectortostr",
    preserves_uniqueness = true,
    inverse = to_unary!(super::CastStringToInt2Vector)
)]
fn cast_int2_vector_to_string<'a>(a: Int2Vector<'a>) -> Result<String, EvalError> {
    let mut buf = String::new();
    stringify_datum(&mut buf, Datum::Array(a.0), &SqlScalarType::Int2Vector)?;
    Ok(buf)
}
