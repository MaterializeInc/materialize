// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use mz_lowertest::MzReflect;
use mz_repr::{ColumnType, Datum, RowArena, ScalarType};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::scalar::func::{stringify_datum, LazyUnaryFunc};
use crate::{EvalError, MirScalarExpr};

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct CastMapToString {
    pub ty: ScalarType,
}

impl LazyUnaryFunc for CastMapToString {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        let a = a.eval(datums, temp_storage)?;
        if a.is_null() {
            return Ok(Datum::Null);
        }
        let mut buf = String::new();
        stringify_datum(&mut buf, a, &self.ty)?;
        Ok(Datum::String(temp_storage.push_string(buf)))
    }

    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        ScalarType::String.nullable(input_type.nullable)
    }

    fn propagates_nulls(&self) -> bool {
        true
    }

    fn introduces_nulls(&self) -> bool {
        false
    }

    fn preserves_uniqueness(&self) -> bool {
        true
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        // TODO? If we moved typeconv into expr, we could evaluate this
        None
    }
}

impl fmt::Display for CastMapToString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("maptostr")
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct MapLength;

impl LazyUnaryFunc for MapLength {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        let a = a.eval(datums, temp_storage)?;
        if a.is_null() {
            return Ok(Datum::Null);
        }
        match a.unwrap_map().iter().count().try_into() {
            Ok(c) => Ok(Datum::Int32(c)),
            Err(_) => Err(EvalError::Int32OutOfRange),
        }
    }

    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        ScalarType::Int32.nullable(input_type.nullable)
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

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        None
    }
}

impl fmt::Display for MapLength {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("map_length")
    }
}
