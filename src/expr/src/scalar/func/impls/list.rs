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
use mz_repr::{Datum, Row, RowArena, SqlColumnType, SqlScalarType};
use serde::{Deserialize, Serialize};

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
        output: &mut Vec<Datum<'a>>,
    ) -> Result<(), EvalError> {
        a.eval(datums, temp_storage, output)?;
        if output.last() == Some(&Datum::Null) {
            return Ok(());
        }
        let a = output.pop().unwrap();
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
        output: &mut Vec<Datum<'a>>,
    ) -> Result<(), EvalError> {
        a.eval(datums, temp_storage, output)?;
        if output.last() == Some(&Datum::Null) {
            return Ok(());
        }
        let a = output.pop().unwrap();
        let mut row = Row::default();
        row.packer().push_list_with(|packer| {
            for elem in a.unwrap_list().iter() {
                self.cast_element.eval(&[elem], temp_storage, output)?;
                let elem = match output.pop().unwrap() {
                    Datum::Null => Datum::JsonNull,
                    d => d,
                };
                packer.push(elem);
            }
            Ok::<_, EvalError>(())
        })?;
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
        output: &mut Vec<Datum<'a>>,
    ) -> Result<(), EvalError> {
        a.eval(datums, temp_storage, output)?;
        if output.last() == Some(&Datum::Null) {
            return Ok(());
        }
        let a = output.pop().unwrap();
        let mut cast_datums = Vec::new();
        for el in a.unwrap_list().iter() {
            // `cast_expr` is evaluated as an expression that casts the
            // first column in `datums` (i.e. `datums[0]`) from the list elements'
            // current type to a target type.
            self.cast_expr.eval(&[el], temp_storage, output)?;
            cast_datums.push(output.pop().unwrap());
        }

        output.push(temp_storage.make_datum(|packer| packer.push_list(cast_datums)));
        Ok(())
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
        output: &mut Vec<Datum<'a>>,
    ) -> Result<(), EvalError> {
        a.eval(datums, temp_storage, output)?;
        if output.last() == Some(&Datum::Null) {
            return Ok(());
        }
        let a = output.pop().unwrap();
        let count = a.unwrap_list().iter().count();
        match count.try_into() {
            Ok(c) => {
                output.push(Datum::Int32(c));
                Ok(())
            }
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
