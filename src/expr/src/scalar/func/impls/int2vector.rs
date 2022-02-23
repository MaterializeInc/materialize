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
use mz_repr::{ColumnType, Datum, RowArena, ScalarType};

use crate::scalar::func::LazyUnaryFunc;
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
    ) -> Result<Datum<'a>, EvalError> {
        a.eval(datums, temp_storage)
    }

    /// The output ColumnType of this function
    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        ScalarType::Array(Box::from(ScalarType::Int16)).nullable(input_type.nullable)
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
}

impl fmt::Display for CastInt2VectorToArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("int2vectortoarray")
    }
}
