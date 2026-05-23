// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_expr_derive::sqlfunc;
use mz_lowertest::MzReflect;
use mz_repr::adt::array::{Array, ArrayDimension};
use mz_repr::{Datum, DatumList, Row, RowArena, RowPacker, SqlScalarType};
use serde::{Deserialize, Serialize};

use crate::scalar::func::stringify_datum;
use crate::{EvalError, MirScalarExpr};

#[sqlfunc(
    sqlname = "arraytolist",
    preserves_uniqueness = true,
    introduces_nulls = false
)]
fn cast_array_to_list_one_dim<'a, T>(a: Array<'a, T>) -> Result<DatumList<'a, T>, EvalError> {
    let ndims = a.dims().ndims();
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
    Ok(a.elements())
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct CastArrayToString {
    pub ty: SqlScalarType,
}

#[sqlfunc(
    CastArrayToString,
    sqlname = "arraytostr",
    preserves_uniqueness = true,
    introduces_nulls = false
)]
fn cast_array_to_string<'a>(
    &self,
    a: Array<'a>,
    _temp_storage: &'a RowArena,
) -> Result<String, EvalError> {
    let mut buf = String::new();
    stringify_datum(&mut buf, Datum::Array(a), &self.ty)?;
    Ok(buf)
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct CastArrayToJsonb {
    pub cast_element: Box<MirScalarExpr>,
}

#[sqlfunc(
    CastArrayToJsonb,
    sqlname = "arraytojsonb",
    output_type_expr = "SqlScalarType::Jsonb.nullable(false)",
    preserves_uniqueness = true,
    introduces_nulls = false
)]
fn cast_array_to_jsonb<'a>(
    &'a self,
    a: Array<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    fn pack<'a>(
        temp_storage: &RowArena,
        elems: &mut impl Iterator<Item = Datum<'a>>,
        dims: &[ArrayDimension],
        cast_element: &MirScalarExpr,
        packer: &mut RowPacker,
    ) -> Result<(), EvalError> {
        packer.push_list_with(|packer| match dims {
            [] => Ok(()),
            [dim] => {
                for _ in 0..dim.length {
                    let elem = elems.next().unwrap();
                    let elem = match cast_element.eval(&[elem], temp_storage)? {
                        Datum::Null => Datum::JsonNull,
                        d => d,
                    };
                    packer.push(elem);
                }
                Ok(())
            }
            [dim, rest @ ..] => {
                for _ in 0..dim.length {
                    pack(temp_storage, elems, rest, cast_element, packer)?;
                }
                Ok(())
            }
        })
    }

    let elements = a.elements();
    let dims = a.dims().into_iter().collect::<Vec<_>>();
    let mut row = Row::default();
    pack(
        temp_storage,
        &mut elements.into_iter(),
        &dims,
        &self.cast_element,
        &mut row.packer(),
    )?;
    Ok(temp_storage.push_unary_row(row))
}

/// Casts an array of one type to an array of another type. Does so by casting
/// each element of the first array to the desired inner type and collecting
/// the results into a new array.
#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct CastArrayToArray {
    pub return_ty: SqlScalarType,
    pub cast_expr: Box<MirScalarExpr>,
}

#[sqlfunc(
    CastArrayToArray,
    sqlname = "arraytoarray",
    output_type_expr = "self.return_ty.clone().nullable(true)",
    introduces_nulls = false
)]
fn cast_array_to_array<'a>(
    &'a self,
    a: Array<'a>,
    temp_storage: &'a RowArena,
) -> Result<Array<'a>, EvalError> {
    let dims = a.dims().into_iter().collect::<Vec<ArrayDimension>>();
    let casted_datums = a
        .elements()
        .iter()
        .map(|datum| self.cast_expr.eval(&[datum], temp_storage))
        .collect::<Result<Vec<Datum<'a>>, EvalError>>()?;
    Ok(temp_storage.make_datum_array(&dims, casted_datums)?)
}
