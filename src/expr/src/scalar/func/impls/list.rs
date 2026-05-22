// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use mz_expr_derive::sqlfunc;
use mz_lowertest::MzReflect;
use mz_repr::{AsColumnType, Datum, DatumList, Row, RowArena, SqlColumnType, SqlScalarType};
use serde::{Deserialize, Serialize};

use crate::func::binary::EagerBinaryFunc;
use crate::scalar::func::stringify_datum;
use crate::{EvalError, MirScalarExpr};

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
pub struct CastListToString {
    pub ty: SqlScalarType,
}

#[sqlfunc(
    CastListToString,
    sqlname = "listtostr",
    preserves_uniqueness = true,
    introduces_nulls = false
)]
fn cast_list_to_string<'a>(
    &self,
    a: DatumList<'a>,
    _temp_storage: &'a RowArena,
) -> Result<String, EvalError> {
    let mut buf = String::new();
    stringify_datum(&mut buf, Datum::List(a), &self.ty)?;
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
pub struct CastListToJsonb {
    pub cast_element: Box<MirScalarExpr>,
}

#[sqlfunc(
    CastListToJsonb,
    sqlname = "listtojsonb",
    output_type_expr = "SqlScalarType::Jsonb.nullable(false)",
    preserves_uniqueness = true,
    introduces_nulls = false
)]
fn cast_list_to_jsonb<'a>(
    &'a self,
    a: DatumList<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut row = Row::default();
    row.packer().push_list_with(|packer| {
        for elem in a.iter() {
            let elem = match self.cast_element.eval(&[elem], temp_storage)? {
                Datum::Null => Datum::JsonNull,
                d => d,
            };
            packer.push(elem);
        }
        Ok::<_, EvalError>(())
    })?;
    Ok(temp_storage.push_unary_row(row))
}

/// Casts between two list types by casting each element of `a` ("list1") using
/// `cast_expr` and collecting the results into a new list ("list2").
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
pub struct CastList1ToList2 {
    /// List2's type
    pub return_ty: SqlScalarType,
    /// The expression to cast List1's elements to List2's elements' type
    pub cast_expr: Box<MirScalarExpr>,
}

#[sqlfunc(
    CastList1ToList2,
    sqlname = "list1tolist2",
    output_type_expr = "self.return_ty.without_modifiers().nullable(false)",
    introduces_nulls = false
)]
fn cast_list1_to_list2<'a>(
    &'a self,
    a: DatumList<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut cast_datums = Vec::new();
    for el in a.iter() {
        cast_datums.push(self.cast_expr.eval(&[el], temp_storage)?);
    }
    Ok(temp_storage.make_datum(|packer| packer.push_list(cast_datums)))
}

#[sqlfunc(sqlname = "list_length")]
fn list_length<'a>(a: DatumList<'a>) -> Result<i32, EvalError> {
    let count = a.iter().count();
    count
        .try_into()
        .map_err(|_| EvalError::Int32OutOfRange(count.to_string().into()))
}

/// The `list_length_max` implementation.
///
/// We're not deriving `sqlfunc` here because we need to pass in the `max_layer` parameter.
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
pub struct ListLengthMax {
    /// Maximal allowed layer to query.
    pub max_layer: usize,
}
impl EagerBinaryFunc for ListLengthMax {
    type Input<'a> = (DatumList<'a>, i64);
    type Output<'a> = Result<Option<i32>, EvalError>;
    // TODO(benesch): remove potentially dangerous usage of `as`.
    #[allow(clippy::as_conversions)]
    fn call<'a>(&self, (a, b): Self::Input<'a>, _: &'a RowArena) -> Self::Output<'a> {
        fn max_len_on_layer(i: DatumList<'_>, on_layer: i64) -> Option<usize> {
            let i = i.iter();
            if on_layer > 1 {
                let mut max_len = None;
                for d in i {
                    if let Datum::List(i) = d {
                        max_len = std::cmp::max(max_len_on_layer(i, on_layer - 1), max_len);
                    }
                }
                max_len
            } else {
                Some(i.count())
            }
        }
        if b as usize > self.max_layer || b < 1 {
            Err(EvalError::InvalidLayer {
                max_layer: self.max_layer,
                val: b,
            })
        } else {
            match max_len_on_layer(a, b) {
                Some(l) => match l.try_into() {
                    Ok(c) => Ok(Some(c)),
                    Err(_) => Err(EvalError::Int32OutOfRange(l.to_string().into())),
                },
                None => Ok(None),
            }
        }
    }
    fn output_sql_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType {
        let output = Self::Output::as_column_type();
        let propagates_nulls = self.propagates_nulls();
        let nullable = output.nullable;
        let input_nullable = input_types.iter().any(|t| t.nullable);
        output.nullable(nullable || (propagates_nulls && input_nullable))
    }
}
impl fmt::Display for ListLengthMax {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("list_length_max")
    }
}
