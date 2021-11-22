// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use lowertest::MzStructReflect;
use repr::{ColumnType, Datum, RowArena, ScalarType};
use serde::{Deserialize, Serialize};

use crate::scalar::func::LazyUnaryFunc;
use crate::{EvalError, MirScalarExpr};

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect,
)]
pub struct IsNull;

impl LazyUnaryFunc for IsNull {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        Ok(a.eval(datums, temp_storage)?.is_null().into())
    }

    fn output_type(&self, _input_type: ColumnType) -> ColumnType {
        ScalarType::Bool.nullable(false)
    }

    fn propagates_nulls(&self) -> bool {
        false
    }

    fn introduces_nulls(&self) -> bool {
        false
    }

    fn preserves_uniqueness(&self) -> bool {
        false
    }
}

impl fmt::Display for IsNull {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("isnull")
    }
}

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect,
)]
pub struct IsTrue;

impl LazyUnaryFunc for IsTrue {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        Ok((a.eval(datums, temp_storage)? == Datum::True).into())
    }

    fn output_type(&self, _input_type: ColumnType) -> ColumnType {
        ScalarType::Bool.nullable(false)
    }

    fn propagates_nulls(&self) -> bool {
        false
    }

    fn introduces_nulls(&self) -> bool {
        false
    }

    fn preserves_uniqueness(&self) -> bool {
        false
    }
}

impl fmt::Display for IsTrue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("istrue")
    }
}

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect,
)]
pub struct IsFalse;

impl LazyUnaryFunc for IsFalse {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        Ok((a.eval(datums, temp_storage)? == Datum::False).into())
    }

    fn output_type(&self, _input_type: ColumnType) -> ColumnType {
        ScalarType::Bool.nullable(false)
    }

    fn propagates_nulls(&self) -> bool {
        false
    }

    fn introduces_nulls(&self) -> bool {
        false
    }

    fn preserves_uniqueness(&self) -> bool {
        false
    }
}

impl fmt::Display for IsFalse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("isfalse")
    }
}

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect,
)]
pub struct PgColumnSize;

impl LazyUnaryFunc for PgColumnSize {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        match a.eval(datums, temp_storage)? {
            Datum::Null => Ok(Datum::Null),
            d => {
                let sz = repr::datum_size(&d);
                Ok(i32::try_from(sz)
                    .or(Err(EvalError::Int32OutOfRange))?
                    .into())
            }
        }
    }

    fn output_type(&self, _input_type: ColumnType) -> ColumnType {
        ScalarType::Int32.nullable(true)
    }

    fn propagates_nulls(&self) -> bool {
        true
    }

    fn introduces_nulls(&self) -> bool {
        false
    }

    fn preserves_uniqueness(&self) -> bool {
        false
    }
}

impl fmt::Display for PgColumnSize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("pg_column_size")
    }
}

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect,
)]
pub struct MzRowSize;

impl LazyUnaryFunc for MzRowSize {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        match a.eval(datums, temp_storage)? {
            Datum::Null => Ok(Datum::Null),
            d => {
                let sz = repr::row_size(d.unwrap_list().iter());
                Ok(i32::try_from(sz)
                    .or(Err(EvalError::Int32OutOfRange))?
                    .into())
            }
        }
    }

    fn output_type(&self, _input_type: ColumnType) -> ColumnType {
        ScalarType::Int32.nullable(true)
    }

    fn propagates_nulls(&self) -> bool {
        true
    }

    fn introduces_nulls(&self) -> bool {
        false
    }

    fn preserves_uniqueness(&self) -> bool {
        false
    }
}

impl fmt::Display for MzRowSize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("mz_row_size")
    }
}
