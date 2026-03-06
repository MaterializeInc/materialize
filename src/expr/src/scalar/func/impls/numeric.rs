// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use dec::{OrderedDecimal, Rounding};
use mz_expr_derive::{sqldoc, sqlfunc};
use mz_lowertest::MzReflect;
use mz_repr::adt::numeric::{self, Numeric, NumericMaxScale};
use mz_repr::{SqlColumnType, SqlScalarType, strconv};
use serde::{Deserialize, Serialize};

use crate::EvalError;
use crate::scalar::func::EagerUnaryFunc;

/// Negates the value.
#[sqlfunc(
    sqlname = "-",
    category = "Numbers",
    preserves_uniqueness = true,
    inverse = to_unary!(NegNumeric),
    is_monotone = true
)]
fn neg_numeric(mut a: Numeric) -> Numeric {
    numeric::cx_datum().neg(&mut a);
    numeric::munge_numeric(&mut a).unwrap();
    a
}

/// Returns the absolute value.
#[sqlfunc(sqlname = "abs", category = "Numbers")]
fn abs_numeric(mut a: Numeric) -> Numeric {
    numeric::cx_datum().abs(&mut a);
    a
}

/// Returns the smallest integer not less than the value.
#[sqlfunc(sqlname = "ceilnumeric", category = "Numbers", is_monotone = true)]
fn ceil_numeric(mut a: Numeric) -> Numeric {
    // ceil will be nop if has no fractional digits.
    if a.exponent() >= 0 {
        return a;
    }
    let mut cx = numeric::cx_datum();
    cx.set_rounding(Rounding::Ceiling);
    cx.round(&mut a);
    numeric::munge_numeric(&mut a).unwrap();
    a
}

/// Computes the exponential (e^x).
#[sqlfunc(sqlname = "expnumeric", category = "Numbers")]
fn exp_numeric(mut a: Numeric) -> Result<Numeric, EvalError> {
    let mut cx = numeric::cx_datum();
    cx.exp(&mut a);
    let cx_status = cx.status();
    if cx_status.overflow() {
        Err(EvalError::FloatOverflow)
    } else if cx_status.subnormal() {
        Err(EvalError::FloatUnderflow)
    } else {
        numeric::munge_numeric(&mut a).unwrap();
        Ok(a)
    }
}

/// Returns the largest integer not greater than the value.
#[sqlfunc(sqlname = "floornumeric", category = "Numbers", is_monotone = true)]
fn floor_numeric(mut a: Numeric) -> Numeric {
    // floor will be nop if has no fractional digits.
    if a.exponent() >= 0 {
        return a;
    }
    let mut cx = numeric::cx_datum();
    cx.set_rounding(Rounding::Floor);
    cx.round(&mut a);
    numeric::munge_numeric(&mut a).unwrap();
    a
}

fn log_guard_numeric(val: &Numeric, function_name: &str) -> Result<(), EvalError> {
    if val.is_negative() {
        return Err(EvalError::NegativeOutOfDomain(function_name.into()));
    }
    if val.is_zero() {
        return Err(EvalError::ZeroOutOfDomain(function_name.into()));
    }
    Ok(())
}

// From the `decNumber` library's documentation:
// > Inexact results will almost always be correctly rounded, but may be up to 1
// > ulp (unit in last place) in error in rare cases.
//
// See decNumberLog10 documentation at http://speleotrove.com/decimal/dnnumb.html
fn log_numeric<F>(mut a: Numeric, logic: F, name: &'static str) -> Result<Numeric, EvalError>
where
    F: Fn(&mut dec::Context<Numeric>, &mut Numeric),
{
    log_guard_numeric(&a, name)?;
    let mut cx = numeric::cx_datum();
    logic(&mut cx, &mut a);
    numeric::munge_numeric(&mut a).unwrap();
    Ok(a)
}

/// Computes the natural logarithm.
#[sqlfunc(sqlname = "lnnumeric", category = "Numbers")]
fn ln_numeric(a: Numeric) -> Result<Numeric, EvalError> {
    log_numeric(a, dec::Context::ln, "ln")
}

/// Computes the base-10 logarithm.
#[sqlfunc(sqlname = "log10numeric", category = "Numbers")]
fn log10_numeric(a: Numeric) -> Result<Numeric, EvalError> {
    log_numeric(a, dec::Context::log10, "log10")
}

/// Rounds to the nearest integer.
#[sqlfunc(sqlname = "roundnumeric", category = "Numbers", is_monotone = true)]
fn round_numeric(mut a: Numeric) -> Numeric {
    // round will be nop if has no fractional digits.
    if a.exponent() >= 0 {
        return a;
    }
    numeric::cx_datum().round(&mut a);
    a
}

/// Truncates toward zero.
#[sqlfunc(sqlname = "truncnumeric", category = "Numbers", is_monotone = true)]
fn trunc_numeric(mut a: Numeric) -> Numeric {
    // trunc will be nop if has no fractional digits.
    if a.exponent() >= 0 {
        return a;
    }
    let mut cx = numeric::cx_datum();
    cx.set_rounding(Rounding::Down);
    cx.round(&mut a);
    numeric::munge_numeric(&mut a).unwrap();
    a
}

/// Computes the square root.
#[sqlfunc(sqlname = "sqrtnumeric", category = "Numbers")]
fn sqrt_numeric(mut a: Numeric) -> Result<Numeric, EvalError> {
    if a.is_negative() {
        return Err(EvalError::NegSqrt);
    }
    let mut cx = numeric::cx_datum();
    cx.sqrt(&mut a);
    numeric::munge_numeric(&mut a).unwrap();
    Ok(a)
}

/// Converts numeric to int2.
#[sqlfunc(
    sqlname = "numeric_to_smallint",
    category = "Cast",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastInt16ToNumeric(None)),
    is_monotone = true
)]
pub fn cast_numeric_to_int16(mut a: Numeric) -> Result<i16, EvalError> {
    let mut cx = numeric::cx_datum();
    cx.round(&mut a);
    cx.clear_status();
    let i = cx
        .try_into_i32(a)
        .or_else(|_| Err(EvalError::Int16OutOfRange(a.to_string().into())))?;
    i16::try_from(i).or_else(|_| Err(EvalError::Int16OutOfRange(i.to_string().into())))
}

/// Converts numeric to int4.
#[sqlfunc(
    sqlname = "numeric_to_integer",
    category = "Cast",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastInt32ToNumeric(None)),
    is_monotone = true
)]
pub fn cast_numeric_to_int32(mut a: Numeric) -> Result<i32, EvalError> {
    let mut cx = numeric::cx_datum();
    cx.round(&mut a);
    cx.clear_status();
    cx.try_into_i32(a)
        .or_else(|_| Err(EvalError::Int32OutOfRange(a.to_string().into())))
}

/// Converts numeric to int8.
#[sqlfunc(
    sqlname = "numeric_to_bigint",
    category = "Cast",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastInt64ToNumeric(None)),
    is_monotone = true
)]
pub fn cast_numeric_to_int64(mut a: Numeric) -> Result<i64, EvalError> {
    let mut cx = numeric::cx_datum();
    cx.round(&mut a);
    cx.clear_status();
    cx.try_into_i64(a)
        .or_else(|_| Err(EvalError::Int64OutOfRange(a.to_string().into())))
}

/// Converts numeric to float4.
#[sqlfunc(
    sqlname = "numeric_to_real",
    category = "Cast",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastFloat32ToNumeric(None)),
    is_monotone = true
)]
pub fn cast_numeric_to_float32(a: Numeric) -> Result<f32, EvalError> {
    let i = a.to_string().parse::<f32>().unwrap();
    if i.is_infinite() {
        Err(EvalError::Float32OutOfRange(i.to_string().into()))
    } else {
        Ok(i)
    }
}

/// Converts numeric to float8.
#[sqlfunc(
    sqlname = "numeric_to_double",
    category = "Cast",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastFloat64ToNumeric(None)),
    is_monotone = true
)]
pub fn cast_numeric_to_float64(a: Numeric) -> Result<f64, EvalError> {
    let i = a.to_string().parse::<f64>().unwrap();
    if i.is_infinite() {
        Err(EvalError::Float64OutOfRange(i.to_string().into()))
    } else {
        Ok(i)
    }
}

/// Converts numeric to text.
#[sqlfunc(
    sqlname = "numeric_to_text",
    category = "Cast",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastStringToNumeric(None))
)]
fn cast_numeric_to_string(a: Numeric) -> String {
    let mut buf = String::new();
    strconv::format_numeric(&mut buf, &OrderedDecimal(a));
    buf
}

/// Converts numeric to uint2.
#[sqlfunc(
    sqlname = "numeric_to_uint2",
    category = "Cast",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastUint16ToNumeric(None)),
    is_monotone = true
)]
fn cast_numeric_to_uint16(mut a: Numeric) -> Result<u16, EvalError> {
    let mut cx = numeric::cx_datum();
    cx.round(&mut a);
    cx.clear_status();
    let u = cx
        .try_into_u32(a)
        .or_else(|_| Err(EvalError::UInt16OutOfRange(a.to_string().into())))?;
    u16::try_from(u).or_else(|_| Err(EvalError::UInt16OutOfRange(u.to_string().into())))
}

/// Converts numeric to uint4.
#[sqlfunc(
    sqlname = "numeric_to_uint4",
    category = "Cast",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastUint32ToNumeric(None)),
    is_monotone = true
)]
fn cast_numeric_to_uint32(mut a: Numeric) -> Result<u32, EvalError> {
    let mut cx = numeric::cx_datum();
    cx.round(&mut a);
    cx.clear_status();
    cx.try_into_u32(a)
        .or_else(|_| Err(EvalError::UInt32OutOfRange(a.to_string().into())))
}

/// Converts numeric to uint8.
#[sqlfunc(
    sqlname = "numeric_to_uint8",
    category = "Cast",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastUint64ToNumeric(None)),
    is_monotone = true
)]
fn cast_numeric_to_uint64(mut a: Numeric) -> Result<u64, EvalError> {
    let mut cx = numeric::cx_datum();
    cx.round(&mut a);
    cx.clear_status();
    cx.try_into_u64(a)
        .or_else(|_| Err(EvalError::UInt64OutOfRange(a.to_string().into())))
}

/// Converts a byte size to a human-readable string.
#[sqlfunc(
    sqlname = "pg_size_pretty",
    category = "PostgreSQL compatibility",
    preserves_uniqueness = false
)]
fn pg_size_pretty(mut a: Numeric) -> Result<String, EvalError> {
    let mut cx = numeric::cx_datum();
    let units = ["bytes", "kB", "MB", "GB", "TB", "PB"];

    for (pos, unit) in units.iter().rev().skip(1).rev().enumerate() {
        // return if abs(round(a)) < 10 in the next unit it would be converted to.
        if Numeric::from(-10239.5) < a && a < Numeric::from(10239.5) {
            // do not round a when the unit is bytes, as no conversion has happened.
            if pos > 0 {
                cx.round(&mut a);
            }

            return Ok(format!("{} {unit}", a.to_standard_notation_string()));
        }

        cx.div(&mut a, &Numeric::from(1024));
        numeric::munge_numeric(&mut a).unwrap();
    }

    cx.round(&mut a);
    Ok(format!(
        "{} {}",
        a.to_standard_notation_string(),
        units.last().unwrap()
    ))
}

/// Adjusts the scale of a numeric value.
#[sqldoc(unique_name = "adjust_numeric_scale", category = "Numbers")]
#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct AdjustNumericScale(pub NumericMaxScale);

impl EagerUnaryFunc for AdjustNumericScale {
    type Input<'a> = Numeric;
    type Output<'a> = Result<Numeric, EvalError>;

    fn call<'a>(&self, mut d: Self::Input<'a>) -> Self::Output<'a> {
        if numeric::rescale(&mut d, self.0.into_u8()).is_err() {
            return Err(EvalError::NumericFieldOverflow);
        };
        Ok(d)
    }

    fn output_sql_type(&self, input: SqlColumnType) -> SqlColumnType {
        SqlScalarType::Numeric {
            max_scale: Some(self.0),
        }
        .nullable(input.nullable)
    }
}

impl fmt::Display for AdjustNumericScale {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("adjust_numeric_scale")
    }
}
