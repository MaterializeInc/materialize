// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use mz_repr::adt::array::ArrayDimension;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_repr::{ColumnType, Datum, RowArena, ScalarType};

use crate::scalar::func::{stringify_datum, LazyUnaryFunc};
use crate::{EvalError, MirScalarExpr};

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
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
            custom_id: None,
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

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        None
    }
}

impl fmt::Display for CastArrayToListOneDim {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("arraytolist")
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct CastArrayToString {
    pub ty: ScalarType,
}

impl LazyUnaryFunc for CastArrayToString {
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
        // TODO? If we moved typeconv into `expr` we could determine the right
        // inverse of this.
        None
    }
}

impl fmt::Display for CastArrayToString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("arraytostr")
    }
}

/// Casts an array of one type to an array of another type. Does so by casting
/// each element of the first array to the desired inner type and collecting
/// the results into a new array.
#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct CastArrayToArray {
    pub return_ty: ScalarType,
    pub cast_expr: Box<MirScalarExpr>,
}

impl LazyUnaryFunc for CastArrayToArray {
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
        let dims = arr.dims().into_iter().collect::<Vec<ArrayDimension>>();

        let casted_datums = arr
            .elements()
            .iter()
            .map(|datum| self.cast_expr.eval(&[datum], temp_storage))
            .collect::<Result<Vec<Datum<'a>>, EvalError>>()?;

        Ok(temp_storage.make_datum(|packer| {
            packer.push_array(&dims, casted_datums).expect("failed to construct array");
        }))
    }

    fn output_type(&self, _input_type: ColumnType) -> ColumnType {
        self.return_ty.clone().nullable(true)
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

impl fmt::Display for CastArrayToArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("arraytoarray")
    }
}
