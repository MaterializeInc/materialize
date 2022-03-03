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
pub struct CastArrayToListOneDim;

impl LazyUnaryFunc for CastArrayToListOneDim {
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

        let arr = a.unwrap_array();
        let ndims = arr.dims().ndims();
        if ndims > 1 {
            return Err(EvalError::Unsupported {
                feature: format!(
                    "casting multi-dimensional array to list; got array with {} dimensions",
                    ndims
                ),
                issue_no: None,
            });
        }

        Ok(Datum::List(arr.elements()))
    }

    /// The output ColumnType of this function
    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        ScalarType::List {
            element_type: Box::new(input_type.scalar_type.unwrap_array_element_type().clone()),
            custom_oid: None,
        }
        .nullable(true)
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
        true
    }
}

impl fmt::Display for CastArrayToListOneDim {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("arraytolist")
    }
}
