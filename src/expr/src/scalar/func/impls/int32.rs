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

use lowertest::MzStructReflect;
use repr::adt::numeric::{self, Numeric};
use repr::adt::system::{Oid, RegClass, RegProc, RegType};
use repr::{strconv, ColumnType, ScalarType};

use crate::scalar::func::EagerUnaryFunc;
use crate::EvalError;

sqlfunc!(
    #[sqlname = "-"]
    fn neg_int32(a: i32) -> i32 {
        -a
    }
);

sqlfunc!(
    #[sqlname = "~"]
    fn bit_not_int32(a: i32) -> i32 {
        !a
    }
);

sqlfunc!(
    #[sqlname = "abs"]
    fn abs_int32(a: i32) -> i32 {
        a.abs()
    }
);

sqlfunc!(
    #[sqlname = "i32tobool"]
    fn cast_int32_to_bool(a: i32) -> bool {
        a != 0
    }
);

sqlfunc!(
    #[sqlname = "i32tof32"]
    fn cast_int32_to_float32(a: i32) -> f32 {
        a as f32
    }
);

sqlfunc!(
    #[sqlname = "i32tof64"]
    #[preserves_uniqueness = true]
    fn cast_int32_to_float64(a: i32) -> f64 {
        f64::from(a)
    }
);

sqlfunc!(
    #[sqlname = "i32toi16"]
    #[preserves_uniqueness = true]
    fn cast_int32_to_int16(a: i32) -> Result<i16, EvalError> {
        i16::try_from(a).or(Err(EvalError::Int16OutOfRange))
    }
);

sqlfunc!(
    #[sqlname = "i32toi64"]
    #[preserves_uniqueness = true]
    fn cast_int32_to_int64(a: i32) -> i64 {
        i64::from(a)
    }
);

sqlfunc!(
    #[sqlname = "i32tostr"]
    #[preserves_uniqueness = true]
    fn cast_int32_to_string(a: i32) -> String {
        let mut buf = String::new();
        strconv::format_int32(&mut buf, a);
        buf
    }
);

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect,
)]
pub struct CastInt32ToNumeric(pub Option<u8>);

impl<'a> EagerUnaryFunc<'a> for CastInt32ToNumeric {
    type Input = i32;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: i32) -> Result<Numeric, EvalError> {
        let mut a = Numeric::from(a);
        if let Some(scale) = self.0 {
            if numeric::rescale(&mut a, scale).is_err() {
                return Err(EvalError::NumericFieldOverflow);
            }
        }
        // Besides `rescale`, cast is infallible.
        Ok(a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { scale: self.0 }.nullable(input.nullable)
    }
}

impl fmt::Display for CastInt32ToNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("i32tonumeric")
    }
}

sqlfunc!(
    #[sqlname = "i32tooid"]
    #[preserves_uniqueness = true]
    fn cast_int32_to_oid(a: i32) -> Oid {
        Oid(a)
    }
);

sqlfunc!(
    #[sqlname = "i32toregclass"]
    #[preserves_uniqueness = true]
    fn cast_int32_to_reg_class(a: i32) -> RegClass {
        RegClass(a)
    }
);

sqlfunc!(
    #[sqlname = "i32toregproc"]
    #[preserves_uniqueness = true]
    fn cast_int32_to_reg_proc(a: i32) -> RegProc {
        RegProc(a)
    }
);

sqlfunc!(
    #[sqlname = "i32toregtype"]
    #[preserves_uniqueness = true]
    fn cast_int32_to_reg_type(a: i32) -> RegType {
        RegType(a)
    }
);
