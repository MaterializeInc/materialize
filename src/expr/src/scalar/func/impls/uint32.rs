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
    #[sqlname = "uint4_to_real"]
    fn cast_uint32_to_float32(a: u32) -> f32 {
        a as f32
    }
);

sqlfunc!(
    #[sqlname = "uint4_to_double"]
    #[preserves_uniqueness = true]
    fn cast_uint32_to_float64(a: u32) -> f64 {
        f64::from(a)
    }
);

sqlfunc!(
    #[sqlname = "uint4_to_uint2"]
    #[preserves_uniqueness = true]
    fn cast_uint32_to_uint16(a: u32) -> Result<u16, EvalError> {
        u16::try_from(a).or(Err(EvalError::UInt16OutOfRange))
    }
);

sqlfunc!(
    #[sqlname = "uint4_to_integer"]
    #[preserves_uniqueness = true]
    fn cast_uint32_to_int32(a: u32) -> Result<i32, EvalError> {
        i32::try_from(a).or(Err(EvalError::Int32OutOfRange))
    }
);

sqlfunc!(
    #[sqlname = "uint4_to_bigint"]
    #[preserves_uniqueness = true]
    fn cast_uint32_to_int64(a: u32) -> i64 {
        i64::from(a)
    }
);

sqlfunc!(
    #[sqlname = "uint4_to_uint8"]
    #[preserves_uniqueness = true]
    fn cast_uint32_to_uint64(a: u32) -> u64 {
        u64::from(a)
    }
);

sqlfunc!(
    #[sqlname = "uint4_to_text"]
    #[preserves_uniqueness = true]
    fn cast_uint32_to_string(a: u32) -> String {
        let mut buf = String::new();
        strconv::format_uint32(&mut buf, a);
        buf
    }
);

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastUint32ToNumeric(pub Option<NumericMaxScale>);

impl<'a> EagerUnaryFunc<'a> for CastUint32ToNumeric {
    type Input = u32;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: u32) -> Result<Numeric, EvalError> {
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
}

impl fmt::Display for CastUint32ToNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("uint4_to_numeric")
    }
}
