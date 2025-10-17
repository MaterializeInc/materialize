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
use mz_repr::{Datum, RowArena, SqlColumnType, SqlScalarType};
use serde::{Deserialize, Serialize};

use crate::scalar::func::{LazyUnaryFunc, stringify_datum};
use crate::{EvalError, MirScalarExpr};

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastInt2VectorToArray;

// This could be simplified to an EagerUnaryFunc once we have
// auto-parameterization of array built-in functions.
impl LazyUnaryFunc for CastInt2VectorToArray {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
        output: &mut Vec<Datum<'a>>,
    ) -> Result<(), EvalError> {
        a.eval(datums, temp_storage, output)
    }

    /// The output SqlColumnType of this function
    fn output_type(&self, input_type: SqlColumnType) -> SqlColumnType {
        SqlScalarType::Array(Box::from(SqlScalarType::Int16)).nullable(input_type.nullable)
    }

    /// Whether this function will produce NULL on NULL input
    fn propagates_nulls(&self) -> bool {
        true
    }

    /// Whether this function will produce NULL on non-NULL input
    fn introduces_nulls(&self) -> bool {
        false
    }

    /// Whether this function preserves uniqueness
    fn preserves_uniqueness(&self) -> bool {
        false
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        None
    }

    fn is_monotone(&self) -> bool {
        true // A noop is trivially monotone.
    }
}

impl fmt::Display for CastInt2VectorToArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("int2vectortoarray")
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastInt2VectorToString;

impl LazyUnaryFunc for CastInt2VectorToString {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
        output: &mut Vec<Datum<'a>>,
    ) -> Result<(), EvalError> {
        a.eval(datums, temp_storage, output)?;
        if output.last() == Some(&Datum::Null) {
            return Ok(());
        }
        let a = output.pop().unwrap();
        let mut buf = String::new();
        stringify_datum(&mut buf, a, &SqlScalarType::Int2Vector)?;
        output.push(Datum::String(temp_storage.push_string(buf)));
        Ok(())
    }

    fn output_type(&self, input_type: SqlColumnType) -> SqlColumnType {
        SqlScalarType::String.nullable(input_type.nullable)
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
        to_unary!(super::CastStringToInt2Vector)
    }

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for CastInt2VectorToString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("int2vectortostr")
    }
}
