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
    #[sqlname = "uint2_to_real"]
    #[preserves_uniqueness = true]
    fn cast_uint16_to_float32(a: u16) -> f32 {
        f32::from(a)
    }
);

sqlfunc!(
    #[sqlname = "uint2_to_double"]
    #[preserves_uniqueness = true]
    fn cast_uint16_to_float64(a: u16) -> f64 {
        f64::from(a)
    }
);

sqlfunc!(
    #[sqlname = "uint2_to_integer"]
    #[preserves_uniqueness = true]
    fn cast_uint16_to_int32(a: u16) -> i32 {
        i32::from(a)
    }
);

sqlfunc!(
    #[sqlname = "uint2_to_uint4"]
    #[preserves_uniqueness = true]
    fn cast_uint16_to_uint32(a: u16) -> u32 {
        u32::from(a)
    }
);

sqlfunc!(
    #[sqlname = "uint2_to_bigint"]
    #[preserves_uniqueness = true]
    fn cast_uint16_to_int64(a: u16) -> i64 {
        i64::from(a)
    }
);

sqlfunc!(
    #[sqlname = "uint2_to_uint8"]
    #[preserves_uniqueness = true]
    fn cast_uint16_to_uint64(a: u16) -> u64 {
        u64::from(a)
    }
);

sqlfunc!(
    #[sqlname = "uint2_to_text"]
    #[preserves_uniqueness = true]
    fn cast_uint16_to_string(a: u16) -> String {
        let mut buf = String::new();
        strconv::format_uint16(&mut buf, a);
        buf
    }
);

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastUint16ToNumeric(pub Option<NumericMaxScale>);

impl<'a> EagerUnaryFunc<'a> for CastUint16ToNumeric {
    type Input = u16;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: u16) -> Result<Numeric, EvalError> {
        let mut a = Numeric::from(i32::from(a));
        if let Some(scale) = self.0 {
            if numeric::rescale(&mut a, scale.into_u8()).is_err() {
                return Err(EvalError::NumericFieldOverflow);
            }
        }
        Ok(a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { max_scale: self.0 }.nullable(input.nullable)
    }
}

impl fmt::Display for CastUint16ToNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("uint2_to_numeric")
    }
}
