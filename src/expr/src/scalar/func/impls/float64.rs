// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use chrono::{DateTime, NaiveDateTime, Utc};
use mz_lowertest::MzReflect;
use mz_ore::cast::TryCastFrom;
use mz_repr::adt::numeric::{self, Numeric, NumericMaxScale};
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{strconv, ColumnType, ScalarType};
use serde::{Deserialize, Serialize};

use crate::scalar::func::EagerUnaryFunc;
use crate::scalar::DomainLimit;
use crate::EvalError;

sqlfunc!(
    #[sqlname = "-"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(NegFloat64)]
    fn neg_float64(a: f64) -> f64 {
        -a
    }
);

sqlfunc!(
    #[sqlname = "abs"]
    fn abs_float64(a: f64) -> f64 {
        a.abs()
    }
);

sqlfunc!(
    #[sqlname = "roundf64"]
    fn round_float64(a: f64) -> f64 {
        // f64::round violates IEEE 754 by rounding ties away from zero rather than
        // to nearest even. There appears to be no way to round ties to nearest even
        // in Rust natively, so bail out to C.
        extern "C" {
            fn rint(f: f64) -> f64;
        }
        unsafe { rint(a) }
    }
);

sqlfunc!(
    #[sqlname = "truncf64"]
    fn trunc_float64(a: f64) -> f64 {
        a.trunc()
    }
);

sqlfunc!(
    #[sqlname = "ceilf64"]
    fn ceil_float64(a: f64) -> f64 {
        a.ceil()
    }
);

sqlfunc!(
    #[sqlname = "floorf64"]
    fn floor_float64(a: f64) -> f64 {
        a.floor()
    }
);

sqlfunc!(
    #[sqlname = "double_to_smallint"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastInt16ToFloat64)]
    fn cast_float64_to_int16(a: f64) -> Result<i16, EvalError> {
        let f = round_float64(a);
        // TODO(benesch): remove potentially dangerous usage of `as`.
        #[allow(clippy::as_conversions)]
        if (f >= (i16::MIN as f64)) && (f < -(i16::MIN as f64)) {
            Ok(f as i16)
        } else {
            Err(EvalError::Int16OutOfRange)
        }
    }
);

sqlfunc!(
    #[sqlname = "double_to_integer"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastInt32ToFloat64)]
    fn cast_float64_to_int32(a: f64) -> Result<i32, EvalError> {
        let f = round_float64(a);
        // This condition is delicate because i32::MIN can be represented exactly by
        // an f64 but not i32::MAX. We follow PostgreSQL's approach here.
        //
        // See: https://github.com/postgres/postgres/blob/ca3b37487/src/include/c.h#L1074-L1096
        // TODO(benesch): remove potentially dangerous usage of `as`.
        #[allow(clippy::as_conversions)]
        if (f >= (i32::MIN as f64)) && (f < -(i32::MIN as f64)) {
            Ok(f as i32)
        } else {
            Err(EvalError::Int32OutOfRange)
        }
    }
);

sqlfunc!(
    #[sqlname = "f64toi64"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastInt64ToFloat64)]
    fn cast_float64_to_int64(a: f64) -> Result<i64, EvalError> {
        let f = round_float64(a);
        // This condition is delicate because i64::MIN can be represented exactly by
        // an f64 but not i64::MAX. We follow PostgreSQL's approach here.
        //
        // See: https://github.com/postgres/postgres/blob/ca3b37487/src/include/c.h#L1074-L1096
        // TODO(benesch): remove potentially dangerous usage of `as`.
        #[allow(clippy::as_conversions)]
        if (f >= (i64::MIN as f64)) && (f < -(i64::MIN as f64)) {
            Ok(f as i64)
        } else {
            Err(EvalError::Int64OutOfRange)
        }
    }
);

sqlfunc!(
    #[sqlname = "double_to_real"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastFloat32ToFloat64)]
    fn cast_float64_to_float32(a: f64) -> Result<f32, EvalError> {
        // TODO(benesch): remove potentially dangerous usage of `as`.
        #[allow(clippy::as_conversions)]
        let result = a as f32;
        if result.is_infinite() && !a.is_infinite() {
            Err(EvalError::FloatOverflow)
        } else if result == 0.0 && a != 0.0 {
            Err(EvalError::FloatUnderflow)
        } else {
            Ok(result)
        }
    }
);

sqlfunc!(
    #[sqlname = "double_to_text"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastStringToFloat64)]
    fn cast_float64_to_string(a: f64) -> String {
        let mut s = String::new();
        strconv::format_float64(&mut s, a);
        s
    }
);

sqlfunc!(
    #[sqlname = "double_to_uint2"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastUint16ToFloat64)]
    fn cast_float64_to_uint16(a: f64) -> Result<u16, EvalError> {
        let f = round_float64(a);
        // TODO(benesch): remove potentially dangerous usage of `as`.
        #[allow(clippy::as_conversions)]
        if (f >= 0.0) && (f <= (u16::MAX as f64)) {
            Ok(f as u16)
        } else {
            Err(EvalError::UInt16OutOfRange)
        }
    }
);

sqlfunc!(
    #[sqlname = "double_to_uint4"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastUint32ToFloat64)]
    fn cast_float64_to_uint32(a: f64) -> Result<u32, EvalError> {
        let f = round_float64(a);
        // TODO(benesch): remove potentially dangerous usage of `as`.
        #[allow(clippy::as_conversions)]
        if (f >= 0.0) && (f <= (u32::MAX as f64)) {
            Ok(f as u32)
        } else {
            Err(EvalError::UInt32OutOfRange)
        }
    }
);

sqlfunc!(
    #[sqlname = "double_to_uint8"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastUint64ToFloat64)]
    fn cast_float64_to_uint64(a: f64) -> Result<u64, EvalError> {
        let f = round_float64(a);
        // TODO(benesch): remove potentially dangerous usage of `as`.
        #[allow(clippy::as_conversions)]
        if (f >= 0.0) && (f <= (u64::MAX as f64)) {
            Ok(f as u64)
        } else {
            Err(EvalError::UInt64OutOfRange)
        }
    }
);

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastFloat64ToNumeric(pub Option<NumericMaxScale>);

impl<'a> EagerUnaryFunc<'a> for CastFloat64ToNumeric {
    type Input = f64;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: f64) -> Result<Numeric, EvalError> {
        if a.is_infinite() {
            return Err(EvalError::InfinityOutOfDomain(
                "casting double precision to numeric".to_owned(),
            ));
        }
        let mut a = Numeric::from(a);
        if let Some(scale) = self.0 {
            if numeric::rescale(&mut a, scale.into_u8()).is_err() {
                return Err(EvalError::NumericFieldOverflow);
            }
        }
        match numeric::munge_numeric(&mut a) {
            Ok(_) => Ok(a),
            Err(_) => Err(EvalError::NumericFieldOverflow),
        }
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { max_scale: self.0 }.nullable(input.nullable)
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastNumericToFloat64)
    }
}

impl fmt::Display for CastFloat64ToNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("double_to_numeric")
    }
}

sqlfunc!(
    #[sqlname = "sqrtf64"]
    fn sqrt_float64(a: f64) -> Result<f64, EvalError> {
        if a < 0.0 {
            return Err(EvalError::NegSqrt);
        }
        Ok(a.sqrt())
    }
);

sqlfunc!(
    #[sqlname = "cbrtf64"]
    fn cbrt_float64(a: f64) -> f64 {
        a.cbrt()
    }
);

sqlfunc!(
    fn cos(a: f64) -> Result<f64, EvalError> {
        if a.is_infinite() {
            return Err(EvalError::InfinityOutOfDomain("cos".to_owned()));
        }
        Ok(a.cos())
    }
);

sqlfunc!(
    fn acos(a: f64) -> Result<f64, EvalError> {
        if a < -1.0 || 1.0 < a {
            return Err(EvalError::OutOfDomain(
                DomainLimit::Inclusive(-1),
                DomainLimit::Inclusive(1),
                "acos".to_owned(),
            ));
        }
        Ok(a.acos())
    }
);

sqlfunc!(
    fn cosh(a: f64) -> f64 {
        a.cosh()
    }
);

sqlfunc!(
    fn acosh(a: f64) -> Result<f64, EvalError> {
        if a < 1.0 {
            return Err(EvalError::OutOfDomain(
                DomainLimit::Inclusive(1),
                DomainLimit::None,
                "acosh".to_owned(),
            ));
        }
        Ok(a.acosh())
    }
);

sqlfunc!(
    fn sin(a: f64) -> Result<f64, EvalError> {
        if a.is_infinite() {
            return Err(EvalError::InfinityOutOfDomain("sin".to_owned()));
        }
        Ok(a.sin())
    }
);

sqlfunc!(
    fn asin(a: f64) -> Result<f64, EvalError> {
        if a < -1.0 || 1.0 < a {
            return Err(EvalError::OutOfDomain(
                DomainLimit::Inclusive(-1),
                DomainLimit::Inclusive(1),
                "asin".to_owned(),
            ));
        }
        Ok(a.asin())
    }
);

sqlfunc!(
    fn sinh(a: f64) -> f64 {
        a.sinh()
    }
);

sqlfunc!(
    fn asinh(a: f64) -> f64 {
        a.asinh()
    }
);

sqlfunc!(
    fn tan(a: f64) -> Result<f64, EvalError> {
        if a.is_infinite() {
            return Err(EvalError::InfinityOutOfDomain("tan".to_owned()));
        }
        Ok(a.tan())
    }
);

sqlfunc!(
    fn atan(a: f64) -> f64 {
        a.atan()
    }
);

sqlfunc!(
    fn tanh(a: f64) -> f64 {
        a.tanh()
    }
);

sqlfunc!(
    fn atanh(a: f64) -> Result<f64, EvalError> {
        if a < -1.0 || 1.0 < a {
            return Err(EvalError::OutOfDomain(
                DomainLimit::Inclusive(-1),
                DomainLimit::Inclusive(1),
                "atanh".to_owned(),
            ));
        }
        Ok(a.atanh())
    }
);

sqlfunc!(
    fn cot(a: f64) -> Result<f64, EvalError> {
        if a.is_infinite() {
            return Err(EvalError::InfinityOutOfDomain("cot".to_owned()));
        }
        Ok(1.0 / a.tan())
    }
);

sqlfunc!(
    fn radians(a: f64) -> f64 {
        a.to_radians()
    }
);

sqlfunc!(
    fn degrees(a: f64) -> f64 {
        a.to_degrees()
    }
);

sqlfunc!(
    #[sqlname = "log10f64"]
    fn log10(a: f64) -> Result<f64, EvalError> {
        if a.is_sign_negative() {
            return Err(EvalError::NegativeOutOfDomain("log10".to_owned()));
        }
        if a == 0.0 {
            return Err(EvalError::ZeroOutOfDomain("log10".to_owned()));
        }
        Ok(a.log10())
    }
);

sqlfunc!(
    #[sqlname = "lnf64"]
    fn ln(a: f64) -> Result<f64, EvalError> {
        if a.is_sign_negative() {
            return Err(EvalError::NegativeOutOfDomain("ln".to_owned()));
        }
        if a == 0.0 {
            return Err(EvalError::ZeroOutOfDomain("ln".to_owned()));
        }
        Ok(a.ln())
    }
);

sqlfunc!(
    #[sqlname = "expf64"]
    fn exp(a: f64) -> f64 {
        a.exp()
    }
);

sqlfunc!(
    #[sqlname = "mz_sleep"]
    fn sleep(a: f64) -> Option<CheckedTimestamp<DateTime<Utc>>> {
        let duration = std::time::Duration::from_secs_f64(a);
        std::thread::sleep(duration);
        None
    }
);

sqlfunc!(
    #[sqlname = "tots"]
    fn to_timestamp(f: f64) -> Result<CheckedTimestamp<DateTime<Utc>>, EvalError> {
        const NANO_SECONDS_PER_SECOND: i64 = 1_000_000_000;
        if f.is_nan() {
            Err(EvalError::TimestampCannotBeNan)
        } else if f.is_infinite() {
            // TODO(jkosh44) implement infinite timestamps
            Err(EvalError::TimestampOutOfRange)
        } else {
            let mut secs = i64::try_cast_from(f.trunc()).ok_or(EvalError::TimestampOutOfRange)?;
            // NOTE(benesch): PostgreSQL has microsecond precision in its timestamps,
            // while chrono has nanosecond precision. While we normally accept
            // nanosecond precision, here we round to the nearest microsecond because
            // f64s lose quite a bit of accuracy in the nanosecond digits when dealing
            // with common Unix timestamp values (> 1 billion).
            let microsecs = (f.fract() * 1_000_000.0).round();
            let mut nanosecs =
                i64::try_cast_from(microsecs * 1_000.0).ok_or(EvalError::TimestampOutOfRange)?;
            if nanosecs < 0 {
                secs = secs.checked_sub(1).ok_or(EvalError::TimestampOutOfRange)?;
                nanosecs = NANO_SECONDS_PER_SECOND
                    .checked_add(nanosecs)
                    .ok_or(EvalError::TimestampOutOfRange)?;
            }
            // Ensure `nanosecs` is less than 1 second.
            secs = secs
                .checked_add(nanosecs / NANO_SECONDS_PER_SECOND)
                .ok_or(EvalError::TimestampOutOfRange)?;
            nanosecs %= NANO_SECONDS_PER_SECOND;
            let nanosecs = u32::try_from(nanosecs).map_err(|_| EvalError::TimestampOutOfRange)?;
            match NaiveDateTime::from_timestamp_opt(secs, nanosecs) {
                Some(ts) => {
                    let dt = DateTime::<Utc>::from_utc(ts, Utc);
                    CheckedTimestamp::from_timestamplike(dt)
                        .map_err(|_| EvalError::TimestampOutOfRange)
                }
                None => Err(EvalError::TimestampOutOfRange),
            }
        }
    }
);
