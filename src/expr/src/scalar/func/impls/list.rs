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
use mz_repr::{AsColumnType, Datum, DatumList, Row, RowArena, SqlColumnType, SqlScalarType};
use serde::{Deserialize, Serialize};

use crate::func::binary::EagerBinaryFunc;
use crate::scalar::func::{LazyUnaryFunc, stringify_datum};
use crate::{EvalError, MirScalarExpr};

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastListToString {
    pub ty: SqlScalarType,
}

impl LazyUnaryFunc for CastListToString {
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
        // TODO? if typeconv was in expr, we could determine this
        None
    }

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for CastListToString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("listtostr")
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastListToJsonb {
    pub cast_element: Box<MirScalarExpr>,
}

impl LazyUnaryFunc for CastListToJsonb {
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
        let mut row = Row::default();
        row.packer().push_list_with(|packer| {
            for elem in a.unwrap_list().iter() {
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

impl fmt::Display for CastListToJsonb {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("listtojsonb")
    }
}

/// Casts between two list types by casting each element of `a` ("list1") using
/// `cast_expr` and collecting the results into a new list ("list2").
#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastList1ToList2 {
    /// List2's type
    pub return_ty: SqlScalarType,
    /// The expression to cast List1's elements to List2's elements' type
    pub cast_expr: Box<MirScalarExpr>,
}

impl LazyUnaryFunc for CastList1ToList2 {
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
        let mut cast_datums = Vec::new();
        for el in a.unwrap_list().iter() {
            // `cast_expr` is evaluated as an expression that casts the
            // first column in `datums` (i.e. `datums[0]`) from the list elements'
            // current type to a target type.
            cast_datums.push(self.cast_expr.eval(&[el], temp_storage)?);
        }

        Ok(temp_storage.make_datum(|packer| packer.push_list(cast_datums)))
    }

    fn output_type(&self, input_type: SqlColumnType) -> SqlColumnType {
        self.return_ty
            .without_modifiers()
            .nullable(input_type.nullable)
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
        // TODO: this could be figured out--might be easier after enum dispatch?
        None
    }

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for CastList1ToList2 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("list1tolist2")
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct ListLength;

impl LazyUnaryFunc for ListLength {
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
        let count = a.unwrap_list().iter().count();
        match count.try_into() {
            Ok(c) => Ok(Datum::Int32(c)),
            Err(_) => Err(EvalError::Int32OutOfRange(count.to_string().into())),
        }
    }

    fn output_type(&self, input_type: SqlColumnType) -> SqlColumnType {
        SqlScalarType::Int32.nullable(input_type.nullable)
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

impl fmt::Display for ListLength {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("list_length")
    }
}

/// The `list_length_max` implementation.
///
/// We're not deriving `sqlfunc` here because we need to pass in the `max_layer` parameter.
#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct ListLengthMax {
    /// Maximal allowed layer to query.
    pub max_layer: usize,
}
impl<'a> EagerBinaryFunc<'a> for ListLengthMax {
    type Input1 = DatumList<'a>;
    type Input2 = i64;
    type Output = Result<Option<i32>, EvalError>;
    // TODO(benesch): remove potentially dangerous usage of `as`.
    #[allow(clippy::as_conversions)]
    fn call(&self, a: Self::Input1, b: Self::Input2, _: &'a RowArena) -> Self::Output {
        fn max_len_on_layer(i: DatumList<'_>, on_layer: i64) -> Option<usize> {
            let mut i = i.iter();
            if on_layer > 1 {
                let mut max_len = None;
                while let Some(Datum::List(i)) = i.next() {
                    max_len = std::cmp::max(max_len_on_layer(i, on_layer - 1), max_len);
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
    fn output_type(
        &self,
        input_type_a: SqlColumnType,
        input_type_b: SqlColumnType,
    ) -> SqlColumnType {
        let output = Self::Output::as_column_type();
        let propagates_nulls = self.propagates_nulls();
        let nullable = output.nullable;
        output.nullable(
            nullable || (propagates_nulls && (input_type_a.nullable || input_type_b.nullable)),
        )
    }
}
impl fmt::Display for ListLengthMax {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("list_length_max")
    }
}
