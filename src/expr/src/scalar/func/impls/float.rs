// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::EvalError;
use chrono::{DateTime, NaiveDateTime, Utc};
use repr::strconv;

sqlfunc!(
    #[sqlname = "-"]
    fn neg_float32(a: f32) -> f32 {
        -a
    }
);

sqlfunc!(
    #[sqlname = "-"]
    fn neg_float64(a: f64) -> f64 {
        -a
    }
);

sqlfunc!(
    #[sqlname = "abs"]
    fn abs_float32(a: f32) -> f32 {
        a.abs()
    }
);

sqlfunc!(
    #[sqlname = "abs"]
    fn abs_float64(a: f64) -> f64 {
        a.abs()
    }
);

sqlfunc!(
    #[sqlname = "roundf32"]
    fn round_float32(a: f32) -> f32 {
        // f32::round violates IEEE 754 by rounding ties away from zero rather than
        // to nearest even. There appears to be no way to round ties to nearest even
        // in Rust natively, so bail out to C.
        extern "C" {
            fn rintf(f: f32) -> f32;
        }
        unsafe { rintf(a) }
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
    #[sqlname = "ceilf32"]
    fn ceil_float32(a: f32) -> f32 {
        a.ceil()
    }
);

sqlfunc!(
    #[sqlname = "ceilf64"]
    fn ceil_float64(a: f64) -> f64 {
        a.ceil()
    }
);

sqlfunc!(
    #[sqlname = "floorf32"]
    fn floor_float32(a: f32) -> f32 {
        a.floor()
    }
);

sqlfunc!(
    #[sqlname = "floorf64"]
    fn floor_float64(a: f64) -> f64 {
        a.floor()
    }
);

sqlfunc!(
    #[sqlname = "f32toi16"]
    fn cast_float32_to_int16(a: f32) -> Result<i16, EvalError> {
        let f = round_float32(Some(a))?.unwrap();
        if (f >= (i16::MIN as f32)) && (f < -(i16::MIN as f32)) {
            Ok(f as i16)
        } else {
            Err(EvalError::Int16OutOfRange)
        }
    }
);

sqlfunc!(
    #[sqlname = "f32toi32"]
    fn cast_float32_to_int32(a: f32) -> Result<i32, EvalError> {
        let f = round_float32(Some(a))?.unwrap();
        // This condition is delicate because i32::MIN can be represented exactly by
        // an f32 but not i32::MAX. We follow PostgreSQL's approach here.
        //
        // See: https://github.com/postgres/postgres/blob/ca3b37487/src/include/c.h#L1074-L1096
        if (f >= (i32::MIN as f32)) && (f < -(i32::MIN as f32)) {
            Ok(f as i32)
        } else {
            Err(EvalError::Int32OutOfRange)
        }
    }
);

sqlfunc!(
    #[sqlname = "f32toi64"]
    fn cast_float32_to_int64(a: f32) -> Result<i64, EvalError> {
        let f = round_float32(Some(a))?.unwrap();
        // This condition is delicate because i64::MIN can be represented exactly by
        // an f32 but not i64::MAX. We follow PostgreSQL's approach here.
        //
        // See: https://github.com/postgres/postgres/blob/ca3b37487/src/include/c.h#L1074-L1096
        if (f >= (i64::MIN as f32)) && (f < -(i64::MIN as f32)) {
            Ok(f as i64)
        } else {
            Err(EvalError::Int64OutOfRange)
        }
    }
);

sqlfunc!(
    #[sqlname = "f64toi16"]
    fn cast_float64_to_int16(a: f64) -> Result<i16, EvalError> {
        let f = round_float64(Some(a))?.unwrap();
        if (f >= (i16::MIN as f64)) && (f < -(i16::MIN as f64)) {
            Ok(f as i16)
        } else {
            Err(EvalError::Int16OutOfRange)
        }
    }
);

sqlfunc!(
    #[sqlname = "f64toi32"]
    fn cast_float64_to_int32(a: f64) -> Result<i32, EvalError> {
        let f = round_float64(Some(a))?.unwrap();
        // This condition is delicate because i32::MIN can be represented exactly by
        // an f64 but not i32::MAX. We follow PostgreSQL's approach here.
        //
        // See: https://github.com/postgres/postgres/blob/ca3b37487/src/include/c.h#L1074-L1096
        if (f >= (i32::MIN as f64)) && (f < -(i32::MIN as f64)) {
            Ok(f as i32)
        } else {
            Err(EvalError::Int32OutOfRange)
        }
    }
);

sqlfunc!(
    #[sqlname = "f64toi64"]
    fn cast_float64_to_int64(a: f64) -> Result<i64, EvalError> {
        let f = round_float64(Some(a))?.unwrap();
        // This condition is delicate because i64::MIN can be represented exactly by
        // an f64 but not i64::MAX. We follow PostgreSQL's approach here.
        //
        // See: https://github.com/postgres/postgres/blob/ca3b37487/src/include/c.h#L1074-L1096
        if (f >= (i64::MIN as f64)) && (f < -(i64::MIN as f64)) {
            Ok(f as i64)
        } else {
            Err(EvalError::Int64OutOfRange)
        }
    }
);

sqlfunc!(
    #[sqlname = "f64tof32"]
    fn cast_float64_to_float32(a: f64) -> Result<f32, EvalError> {
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
    #[sqlname = "f32tof64"]
    fn cast_float32_to_float64(a: f32) -> f64 {
        a.into()
    }
);

sqlfunc!(
    #[sqlname = "f32tostr"]
    fn cast_float32_to_string(a: f32) -> String {
        let mut s = String::new();
        strconv::format_float32(&mut s, a);
        s
    }
);

sqlfunc!(
    #[sqlname = "mz_sleep"]
    fn sleep(a: f64) -> Option<DateTime<Utc>> {
        let duration = std::time::Duration::from_secs_f64(a);
        std::thread::sleep(duration);
        None
    }
);

sqlfunc!(
    #[sqlname = "tots"]
    fn to_timestamp(f: f64) -> Option<DateTime<Utc>> {
        if !f.is_finite() {
            None
        } else {
            let secs = f.trunc() as i64;
            // NOTE(benesch): PostgreSQL has microsecond precision in its timestamps,
            // while chrono has nanosecond precision. While we normally accept
            // nanosecond precision, here we round to the nearest microsecond because
            // f64s lose quite a bit of accuracy in the nanosecond digits when dealing
            // with common Unix timestamp values (> 1 billion).
            let nanosecs = ((f.fract() * 1_000_000.0).round() as u32) * 1_000;
            match NaiveDateTime::from_timestamp_opt(secs as i64, nanosecs as u32) {
                Some(ts) => Some(DateTime::<Utc>::from_utc(ts, Utc)),
                None => None,
            }
        }
    }
);
