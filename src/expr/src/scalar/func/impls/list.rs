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
pub struct CastListToString {
    pub ty: ScalarType,
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
        // TODO? if typeconv was in expr, we could determine this
        None
    }
}

impl fmt::Display for CastListToString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("listtostr")
    }
}

/// Casts between two list types by casting each element of `a` ("list1") using
/// `cast_expr` and collecting the results into a new list ("list2").
#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastList1ToList2 {
    /// List2's type
    pub return_ty: ScalarType,
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

    fn output_type(&self, input_type: ColumnType) -> ColumnType {
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
}

impl fmt::Display for CastList1ToList2 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("list1tolist2")
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
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
        match a.unwrap_list().iter().count().try_into() {
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

impl fmt::Display for ListLength {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("list_length")
    }
}
