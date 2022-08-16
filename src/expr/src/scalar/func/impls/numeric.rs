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
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_repr::adt::numeric::{self, Numeric, NumericMaxScale};
use mz_repr::{strconv, ColumnType, ScalarType};

use crate::scalar::func::EagerUnaryFunc;
use crate::EvalError;

sqlfunc!(
    #[sqlname = "-"]
    #[preserves_uniqueness = true]
    fn neg_numeric(mut a: Numeric) -> Numeric {
        numeric::cx_datum().neg(&mut a);
        numeric::munge_numeric(&mut a).unwrap();
        a
    }
);

sqlfunc!(
    #[sqlname = "abs"]
    fn abs_numeric(mut a: Numeric) -> Numeric {
        numeric::cx_datum().abs(&mut a);
        a
    }
);

sqlfunc!(
    #[sqlname = "ceilnumeric"]
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
);

sqlfunc!(
    #[sqlname = "expnumeric"]
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
);

sqlfunc!(
    #[sqlname = "floornumeric"]
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
);

fn log_guard_numeric(val: &Numeric, function_name: &str) -> Result<(), EvalError> {
    if val.is_negative() {
        return Err(EvalError::NegativeOutOfDomain(function_name.to_owned()));
    }
    if val.is_zero() {
        return Err(EvalError::ZeroOutOfDomain(function_name.to_owned()));
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

sqlfunc!(
    #[sqlname = "lnnumeric"]
    fn ln_numeric(a: Numeric) -> Result<Numeric, EvalError> {
        log_numeric(a, dec::Context::ln, "ln")
    }
);

sqlfunc!(
    #[sqlname = "log10numeric"]
    fn log10_numeric(a: Numeric) -> Result<Numeric, EvalError> {
        log_numeric(a, dec::Context::log10, "log10")
    }
);

sqlfunc!(
    #[sqlname = "roundnumeric"]
    fn round_numeric(mut a: Numeric) -> Numeric {
        // round will be nop if has no fractional digits.
        if a.exponent() >= 0 {
            return a;
        }
        numeric::cx_datum().round(&mut a);
        a
    }
);

sqlfunc!(
    #[sqlname = "truncnumeric"]
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
);

sqlfunc!(
    #[sqlname = "sqrtnumeric"]
    fn sqrt_numeric(mut a: Numeric) -> Result<Numeric, EvalError> {
        if a.is_negative() {
            return Err(EvalError::NegSqrt);
        }
        let mut cx = numeric::cx_datum();
        cx.sqrt(&mut a);
        numeric::munge_numeric(&mut a).unwrap();
        Ok(a)
    }
);

sqlfunc!(
    #[sqlname = "numeric_to_smallint"]
    fn cast_numeric_to_int16(mut a: Numeric) -> Result<i16, EvalError> {
        let mut cx = numeric::cx_datum();
        cx.round(&mut a);
        cx.clear_status();
        let i = cx.try_into_i32(a).or(Err(EvalError::Int16OutOfRange))?;
        i16::try_from(i).or(Err(EvalError::Int16OutOfRange))
    }
);

sqlfunc!(
    #[sqlname = "numeric_to_integer"]
    fn cast_numeric_to_int32(mut a: Numeric) -> Result<i32, EvalError> {
        let mut cx = numeric::cx_datum();
        cx.round(&mut a);
        cx.clear_status();
        cx.try_into_i32(a).or(Err(EvalError::Int32OutOfRange))
    }
);

sqlfunc!(
    #[sqlname = "numeric_to_bigint"]
    fn cast_numeric_to_int64(mut a: Numeric) -> Result<i64, EvalError> {
        let mut cx = numeric::cx_datum();
        cx.round(&mut a);
        cx.clear_status();
        cx.try_into_i64(a).or(Err(EvalError::Int64OutOfRange))
    }
);

sqlfunc!(
    #[sqlname = "numeric_to_real"]
    fn cast_numeric_to_float32(a: Numeric) -> Result<f32, EvalError> {
        let i = a.to_string().parse::<f32>().unwrap();
        if i.is_infinite() {
            Err(EvalError::Float32OutOfRange)
        } else {
            Ok(i)
        }
    }
);

sqlfunc!(
    #[sqlname = "numeric_to_double"]
    fn cast_numeric_to_float64(a: Numeric) -> Result<f64, EvalError> {
        let i = a.to_string().parse::<f64>().unwrap();
        if i.is_infinite() {
            Err(EvalError::Float64OutOfRange)
        } else {
            Ok(i)
        }
    }
);

sqlfunc!(
    #[sqlname = "numeric_to_text"]
    fn cast_numeric_to_string(a: Numeric) -> String {
        let mut buf = String::new();
        strconv::format_numeric(&mut buf, &OrderedDecimal(a));
        buf
    }
);

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct RescaleNumeric(pub NumericMaxScale);

impl<'a> EagerUnaryFunc<'a> for RescaleNumeric {
    type Input = Numeric;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, mut d: Numeric) -> Result<Numeric, EvalError> {
        if numeric::rescale(&mut d, self.0.into_u8()).is_err() {
            return Err(EvalError::NumericFieldOverflow);
        };
        Ok(d)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric {
            max_scale: Some(self.0),
        }
        .nullable(input.nullable)
    }
}

impl fmt::Display for RescaleNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("rescale_numeric")
    }
}
