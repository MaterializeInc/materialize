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
use mz_repr::adt::array::ArrayDimension;
use mz_repr::{Datum, DatumListIter, Row, RowArena, RowPacker, SqlColumnType, SqlScalarType};
use serde::{Deserialize, Serialize};

use crate::scalar::func::{LazyUnaryFunc, stringify_datum};
use crate::{EvalError, MirScalarExpr};

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastArrayToListOneDim;

impl LazyUnaryFunc for CastArrayToListOneDim {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
        output: &mut Vec<Datum<'a>>,
    ) -> Result<(), EvalError> {
        a.eval(datums, temp_storage, output)?;
        let a = output.pop().unwrap();
        if a.is_null() {
            output.push(Datum::Null);
            return Ok(());
        }

        let arr = a.unwrap_array();
        let ndims = arr.dims().ndims();
        if ndims > 1 {
            return Err(EvalError::Unsupported {
                feature: format!(
                    "casting multi-dimensional array to list; got array with {} dimensions",
                    ndims
                )
                .into(),
                discussion_no: None,
            });
        }

        output.push(Datum::List(arr.elements()));
        Ok(())
    }

    /// The output SqlColumnType of this function
    fn output_type(&self, input_type: SqlColumnType) -> SqlColumnType {
        SqlScalarType::List {
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

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for CastArrayToListOneDim {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("arraytolist")
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastArrayToString {
    pub ty: SqlScalarType,
}

impl LazyUnaryFunc for CastArrayToString {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
        output: &mut Vec<Datum<'a>>,
    ) -> Result<(), EvalError> {
        a.eval(datums, temp_storage, output)?;
        let a = output.pop().unwrap();
        if a.is_null() {
            output.push(Datum::Null);
            return Ok(());
        }
        let mut buf = String::new();
        stringify_datum(&mut buf, a, &self.ty)?;
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
        // TODO? If we moved typeconv into `expr` we could determine the right
        // inverse of this.
        None
    }

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for CastArrayToString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("arraytostr")
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastArrayToJsonb {
    pub cast_element: Box<MirScalarExpr>,
}

impl LazyUnaryFunc for CastArrayToJsonb {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
        output: &mut Vec<Datum<'a>>,
    ) -> Result<(), EvalError> {
        fn pack<'a>(
            temp_storage: &'a RowArena,
            elems: &mut DatumListIter<'a>,
            dims: &[ArrayDimension],
            cast_element: &'a MirScalarExpr,
            packer: &mut RowPacker,
            output: &mut Vec<Datum<'a>>,
        ) -> Result<(), EvalError> {
            packer.push_list_with(|packer| match dims {
                [] => Ok(()),
                [dim] => {
                    for _ in 0..dim.length {
                        let elem = elems.next().unwrap();
                        cast_element.eval(&[elem], temp_storage, output)?;
                        let elem = match output.pop().unwrap() {
                            Datum::Null => Datum::JsonNull,
                            d => d,
                        };
                        packer.push(elem);
                    }
                    Ok(())
                }
                [dim, rest @ ..] => {
                    for _ in 0..dim.length {
                        pack(temp_storage, elems, rest, cast_element, packer, output)?;
                    }
                    Ok(())
                }
            })
        }

        a.eval(datums, temp_storage, output)?;
        if output.last() == Some(&Datum::Null) {
            return Ok(());
        }
        let a = output.pop().unwrap().unwrap_array();
        let elements = a.elements();
        let dims = a.dims().into_iter().collect::<Vec<_>>();
        let mut row = Row::default();
        pack(
            temp_storage,
            &mut elements.iter(),
            &dims,
            &self.cast_element,
            &mut row.packer(),
            output,
        )?;
        output.push(temp_storage.push_unary_row(row));
        Ok(())
    }

    fn output_type(&self, input_type: SqlColumnType) -> SqlColumnType {
        SqlScalarType::Jsonb.nullable(input_type.nullable)
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

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for CastArrayToJsonb {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("arraytojsonb")
    }
}

/// Casts an array of one type to an array of another type. Does so by casting
/// each element of the first array to the desired inner type and collecting
/// the results into a new array.
#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastArrayToArray {
    pub return_ty: SqlScalarType,
    pub cast_expr: Box<MirScalarExpr>,
}

impl LazyUnaryFunc for CastArrayToArray {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
        output: &mut Vec<Datum<'a>>,
    ) -> Result<(), EvalError> {
        a.eval(datums, temp_storage, output)?;
        let a = output.pop().unwrap();
        if a.is_null() {
            output.push(Datum::Null);
            return Ok(());
        }

        let arr = a.unwrap_array();
        let dims = arr.dims().into_iter().collect::<Vec<ArrayDimension>>();

        let mut casted_datums = Vec::new();
        for datum in arr.elements().iter() {
            self.cast_expr.eval(&[datum], temp_storage, output)?;
            casted_datums.push(output.pop().unwrap());
        }

        output.push(
            temp_storage.try_make_datum(|packer| packer.try_push_array(&dims, casted_datums))?,
        );
        Ok(())
    }

    fn output_type(&self, _input_type: SqlColumnType) -> SqlColumnType {
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

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for CastArrayToArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("arraytoarray")
    }
}
