// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_expr_derive::sqlfunc;
use mz_repr::adt::numeric::{self, Numeric, NumericMaxScale};
use mz_repr::{SqlScalarType, strconv};
use serde::{Deserialize, Serialize};

use crate::EvalError;

#[sqlfunc(
    sqlname = "~",
    preserves_uniqueness = true,
    inverse = super::BitNotUint16
)]
fn bit_not_uint16(a: u16) -> u16 {
    !a
}

#[sqlfunc(
    sqlname = "uint2_to_real",
    preserves_uniqueness = true,
    inverse = super::CastFloat32ToUint16,
    is_monotone = true
)]
fn cast_uint16_to_float32(a: u16) -> f32 {
    f32::from(a)
}

#[sqlfunc(
    sqlname = "uint2_to_double",
    preserves_uniqueness = true,
    inverse = super::CastFloat64ToUint16,
    is_monotone = true
)]
fn cast_uint16_to_float64(a: u16) -> f64 {
    f64::from(a)
}

#[sqlfunc(
    sqlname = "uint2_to_uint4",
    preserves_uniqueness = true,
    inverse = super::CastUint32ToUint16,
    is_monotone = true
)]
fn cast_uint16_to_uint32(a: u16) -> u32 {
    u32::from(a)
}

#[sqlfunc(
    sqlname = "uint2_to_uint8",
    preserves_uniqueness = true,
    inverse = super::CastUint64ToUint16,
    is_monotone = true
)]
fn cast_uint16_to_uint64(a: u16) -> u64 {
    u64::from(a)
}

#[sqlfunc(
    sqlname = "uint2_to_smallint",
    preserves_uniqueness = true,
    inverse = super::CastInt16ToUint16,
    is_monotone = true
)]
fn cast_uint16_to_int16(a: u16) -> Result<i16, EvalError> {
    i16::try_from(a).or_else(|_| Err(EvalError::Int16OutOfRange(a.to_string().into())))
}

#[sqlfunc(
    sqlname = "uint2_to_integer",
    preserves_uniqueness = true,
    inverse = super::CastInt32ToUint16,
    is_monotone = true
)]
fn cast_uint16_to_int32(a: u16) -> i32 {
    i32::from(a)
}
#[sqlfunc(
    sqlname = "uint2_to_bigint",
    preserves_uniqueness = true,
    inverse = super::CastInt64ToUint16,
    is_monotone = true
)]
fn cast_uint16_to_int64(a: u16) -> i64 {
    i64::from(a)
}

#[sqlfunc(
    sqlname = "uint2_to_text",
    preserves_uniqueness = true,
    inverse = super::CastStringToUint16
)]
fn cast_uint16_to_string(a: u16) -> String {
    let mut buf = String::new();
    strconv::format_uint16(&mut buf, a);
    buf
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash
)]
pub struct CastUint16ToNumeric(pub Option<NumericMaxScale>);

#[sqlfunc(
    CastUint16ToNumeric,
    sqlname = "uint2_to_numeric",
    could_error = self.0.is_some(),
    inverse = super::CastNumericToUint16,
    is_monotone = true,
    output_type_expr = SqlScalarType::Numeric { max_scale: self.0 }
        .nullable(input_type.nullable)
)]
fn cast_uint16_to_numeric(&self, a: u16) -> Result<Numeric, EvalError> {
    let mut a = Numeric::from(i32::from(a));
    if let Some(scale) = self.0 {
        if numeric::rescale(&mut a, scale.into_u8()).is_err() {
            return Err(EvalError::NumericFieldOverflow);
        }
    }
    Ok(a)
}
