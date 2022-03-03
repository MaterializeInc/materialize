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
    fn neg_int16(a: i16) -> i16 {
        -a
    }
);

sqlfunc!(
    #[sqlname = "~"]
    fn bit_not_int16(a: i16) -> i16 {
        !a
    }
);

sqlfunc!(
    #[sqlname = "abs"]
    fn abs_int16(a: i16) -> i16 {
        a.abs()
    }
);

sqlfunc!(
    #[sqlname = "i16tof32"]
    #[preserves_uniqueness = true]
    fn cast_int16_to_float32(a: i16) -> f32 {
        f32::from(a)
    }
);

sqlfunc!(
    #[sqlname = "i16tof64"]
    #[preserves_uniqueness = true]
    fn cast_int16_to_float64(a: i16) -> f64 {
        f64::from(a)
    }
);

sqlfunc!(
    #[sqlname = "i16toi32"]
    #[preserves_uniqueness = true]
    fn cast_int16_to_int32(a: i16) -> i32 {
        i32::from(a)
    }
);

sqlfunc!(
    #[sqlname = "i16toi64"]
    #[preserves_uniqueness = true]
    fn cast_int16_to_int64(a: i16) -> i64 {
        i64::from(a)
    }
);

sqlfunc!(
    #[sqlname = "i16tostr"]
    #[preserves_uniqueness = true]
    fn cast_int16_to_string(a: i16) -> String {
        let mut buf = String::new();
        strconv::format_int16(&mut buf, a);
        buf
    }
);

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastInt16ToNumeric(pub Option<NumericMaxScale>);

impl<'a> EagerUnaryFunc<'a> for CastInt16ToNumeric {
    type Input = i16;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: i16) -> Result<Numeric, EvalError> {
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

impl fmt::Display for CastInt16ToNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("i16tonumeric")
    }
}
