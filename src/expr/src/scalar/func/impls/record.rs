// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use itertools::Itertools;
use mz_lowertest::MzReflect;
use mz_repr::{ColumnType, Datum, RowArena, ScalarType};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::scalar::func::{stringify_datum, LazyUnaryFunc};
use crate::{EvalError, MirScalarExpr};

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct CastRecordToString {
    pub ty: ScalarType,
}

impl LazyUnaryFunc for CastRecordToString {
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
        // TODO? if we moved typeconv into expr, we could evaluate this
        None
    }
}

impl fmt::Display for CastRecordToString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("recordtostr")
    }
}

/// Casts between two record types by casting each element of `a` ("record1") using
/// `cast_expr` and collecting the results into a new record ("record2").
#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastRecord1ToRecord2 {
    pub return_ty: ScalarType,
    pub cast_exprs: Vec<MirScalarExpr>,
}

impl LazyUnaryFunc for CastRecord1ToRecord2 {
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
        for (el, cast_expr) in a.unwrap_list().iter().zip_eq(&self.cast_exprs) {
            cast_datums.push(cast_expr.eval(&[el], temp_storage)?);
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
        // TODO: we could determine Record1's type from `cast_exprs`
        None
    }
}

impl fmt::Display for CastRecord1ToRecord2 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("record1torecord2")
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct RecordGet(pub usize);

impl LazyUnaryFunc for RecordGet {
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
        Ok(a.unwrap_list().iter().nth(self.0).unwrap())
    }

    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        match input_type.scalar_type {
            ScalarType::Record { mut fields, .. } => {
                let (_name, mut ty) = fields.swap_remove(self.0);
                ty.nullable = ty.nullable || input_type.nullable;
                ty
            }
            _ => unreachable!("RecordGet specified nonexistent field"),
        }
    }

    fn propagates_nulls(&self) -> bool {
        true
    }

    fn introduces_nulls(&self) -> bool {
        // Return null if the inner field is null
        true
    }

    fn preserves_uniqueness(&self) -> bool {
        false
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        None
    }
}

impl fmt::Display for RecordGet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "record_get[{}]", self.0)
    }
}
