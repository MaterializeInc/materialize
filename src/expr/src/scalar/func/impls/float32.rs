// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_repr::adt::numeric::{self, Numeric, NumericMaxScale};
use mz_repr::{strconv, ColumnType, ScalarType};

use crate::scalar::func::EagerUnaryFunc;
use crate::EvalError;

sqlfunc!(
    #[sqlname = "-"]
    fn neg_float32(a: f32) -> f32 {
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
    #[sqlname = "truncf32"]
    fn trunc_float32(a: f32) -> f32 {
        a.trunc()
    }
);

sqlfunc!(
    #[sqlname = "ceilf32"]
    fn ceil_float32(a: f32) -> f32 {
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
    #[sqlname = "real_to_smallint"]
    fn cast_float32_to_int16(a: f32) -> Result<i16, EvalError> {
        let f = round_float32(a);
        if (f >= (i16::MIN as f32)) && (f < -(i16::MIN as f32)) {
            Ok(f as i16)
        } else {
            Err(EvalError::Int16OutOfRange)
        }
    }
);

sqlfunc!(
    #[sqlname = "real_to_integer"]
    fn cast_float32_to_int32(a: f32) -> Result<i32, EvalError> {
        let f = round_float32(a);
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
    #[sqlname = "real_to_bigint"]
    fn cast_float32_to_int64(a: f32) -> Result<i64, EvalError> {
        let f = round_float32(a);
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
    #[sqlname = "real_to_double"]
    fn cast_float32_to_float64(a: f32) -> f64 {
        a.into()
    }
);

sqlfunc!(
    #[sqlname = "real_to_text"]
    fn cast_float32_to_string(a: f32) -> String {
        let mut s = String::new();
        strconv::format_float32(&mut s, a);
        s
    }
);

sqlfunc!(
    #[sqlname = "real_to_uint2"]
    fn cast_float32_to_uint16(a: f32) -> Result<u16, EvalError> {
        let f = round_float32(a);
        if (f >= 0.0) && (f <= (u16::MAX as f32)) {
            Ok(f as u16)
        } else {
            Err(EvalError::UInt16OutOfRange)
        }
    }
);

sqlfunc!(
    #[sqlname = "real_to_uint4"]
    fn cast_float32_to_uint32(a: f32) -> Result<u32, EvalError> {
        let f = round_float32(a);
        if (f >= 0.0) && (f <= (u32::MAX as f32)) {
            Ok(f as u32)
        } else {
            Err(EvalError::UInt32OutOfRange)
        }
    }
);

sqlfunc!(
    #[sqlname = "real_to_uint8"]
    fn cast_float32_to_uint64(a: f32) -> Result<u64, EvalError> {
        let f = round_float32(a);
        if (f >= 0.0) && (f <= (u64::MAX as f32)) {
            Ok(f as u64)
        } else {
            Err(EvalError::UInt64OutOfRange)
        }
    }
);

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastFloat32ToNumeric(pub Option<NumericMaxScale>);

impl<'a> EagerUnaryFunc<'a> for CastFloat32ToNumeric {
    type Input = f32;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: f32) -> Result<Numeric, EvalError> {
        if a.is_infinite() {
            return Err(EvalError::InfinityOutOfDomain(
                "casting real to numeric".to_owned(),
            ));
        }
        let mut a = Numeric::from(a);
        if let Some(scale) = self.0 {
            if numeric::rescale(&mut a, scale.into_u8()).is_err() {
                return Err(EvalError::NumericFieldOverflow);
            }
        }
        numeric::munge_numeric(&mut a).unwrap();
        Ok(a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { max_scale: self.0 }.nullable(input.nullable)
    }
}

impl fmt::Display for CastFloat32ToNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("real_to_numeric")
    }
}
