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
use mz_repr::adt::system::Oid;
use mz_repr::{strconv, ColumnType, ScalarType};

use crate::scalar::func::EagerUnaryFunc;
use crate::EvalError;

sqlfunc!(
    #[sqlname = "-"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(NegInt64)]
    fn neg_int64(a: i64) -> Result<i64, EvalError> {
        a.checked_neg().ok_or(EvalError::Int64OutOfRange)
    }
);

sqlfunc!(
    #[sqlname = "~"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(BitNotInt64)]
    fn bit_not_int64(a: i64) -> i64 {
        !a
    }
);

sqlfunc!(
    #[sqlname = "abs"]
    fn abs_int64(a: i64) -> Result<i64, EvalError> {
        a.checked_abs().ok_or(EvalError::Int64OutOfRange)
    }
);

sqlfunc!(
    #[sqlname = "bigint_to_boolean"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastBoolToInt64)]
    fn cast_int64_to_bool(a: i64) -> bool {
        a != 0
    }
);

sqlfunc!(
    #[sqlname = "bigint_to_smallint"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(super::CastInt16ToInt64)]
    fn cast_int64_to_int16(a: i64) -> Result<i16, EvalError> {
        i16::try_from(a).or(Err(EvalError::Int16OutOfRange))
    }
);

sqlfunc!(
    #[sqlname = "bigint_to_integer"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(super::CastInt32ToInt64)]
    fn cast_int64_to_int32(a: i64) -> Result<i32, EvalError> {
        i32::try_from(a).or(Err(EvalError::Int32OutOfRange))
    }
);

sqlfunc!(
    #[sqlname = "bigint_to_oid"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(super::CastOidToInt64)]
    fn cast_int64_to_oid(a: i64) -> Result<Oid, EvalError> {
        // Unlike casting a 16-bit or 32-bit integers to OID, casting a 64-bit
        // integers to an OID rejects negative values.
        u32::try_from(a).map(Oid).or(Err(EvalError::OidOutOfRange))
    }
);

sqlfunc!(
    #[sqlname = "bigint_to_uint2"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(super::CastUint16ToInt64)]
    fn cast_int64_to_uint16(a: i64) -> Result<u16, EvalError> {
        u16::try_from(a).or(Err(EvalError::UInt16OutOfRange))
    }
);

sqlfunc!(
    #[sqlname = "bigint_to_uint4"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(super::CastUint32ToInt64)]
    fn cast_int64_to_uint32(a: i64) -> Result<u32, EvalError> {
        u32::try_from(a).or(Err(EvalError::UInt32OutOfRange))
    }
);

sqlfunc!(
    #[sqlname = "bigint_to_uint8"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(super::CastUint64ToInt64)]
    fn cast_int64_to_uint64(a: i64) -> Result<u64, EvalError> {
        u64::try_from(a).or(Err(EvalError::UInt64OutOfRange))
    }
);

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastInt64ToNumeric(pub Option<NumericMaxScale>);

impl<'a> EagerUnaryFunc<'a> for CastInt64ToNumeric {
    type Input = i64;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: i64) -> Result<Numeric, EvalError> {
        let mut a = Numeric::from(a);
        if let Some(scale) = self.0 {
            if numeric::rescale(&mut a, scale.into_u8()).is_err() {
                return Err(EvalError::NumericFieldOverflow);
            }
        }
        // Besides `rescale`, cast is infallible.
        Ok(a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { max_scale: self.0 }.nullable(input.nullable)
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastNumericToInt64)
    }
}

impl fmt::Display for CastInt64ToNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("bigint_to_numeric")
    }
}

sqlfunc!(
    #[sqlname = "bigint_to_real"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastFloat32ToInt64)]
    fn cast_int64_to_float32(a: i64) -> f32 {
        a as f32
    }
);

sqlfunc!(
    #[sqlname = "bigint_to_double"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastFloat64ToInt64)]
    fn cast_int64_to_float64(a: i64) -> f64 {
        a as f64
    }
);

sqlfunc!(
    #[sqlname = "bigint_to_text"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(super::CastStringToInt64)]
    fn cast_int64_to_string(a: i64) -> String {
        let mut buf = String::new();
        strconv::format_int64(&mut buf, a);
        buf
    }
);
