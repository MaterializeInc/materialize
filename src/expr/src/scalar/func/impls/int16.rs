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
use repr::{ColumnType, ScalarType};
use repr::adt::numeric::{self, Numeric};

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

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect,
)]
pub struct CastInt16ToNumeric(pub Option<u8>);

impl EagerUnaryFunc for CastInt16ToNumeric {
    type Input = i16;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: i16) -> Result<Numeric, EvalError> {
        let mut a = Numeric::from(i32::from(a));
        if let Some(scale) = self.0 {
            if numeric::rescale(&mut a, scale).is_err() {
                return Err(EvalError::NumericFieldOverflow);
            }
        }
        Ok(a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { scale: self.0 }.nullable(input.nullable)
    }
}

impl fmt::Display for CastInt16ToNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("i16tonumeric")
    }
}
