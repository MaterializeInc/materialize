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
use mz_expr_derive::sqlfunc;
use mz_lowertest::MzReflect;
use mz_repr::{Datum, DatumList, RowArena, SqlScalarType};
use serde::{Deserialize, Serialize};

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
pub struct CastRecordToString {
    pub ty: SqlScalarType,
}

#[sqlfunc(
    CastRecordToString,
    sqlname = "recordtostr",
    preserves_uniqueness = true,
    introduces_nulls = false
)]
fn cast_record_to_string<'a>(
    &self,
    a: DatumList<'a>,
    _temp_storage: &'a RowArena,
) -> Result<String, EvalError> {
    let mut buf = String::new();
    stringify_datum(&mut buf, Datum::List(a), &self.ty)?;
    Ok(buf)
}

/// Casts between two record types by casting each element of `a` ("record1") using
/// `cast_expr` and collecting the results into a new record ("record2").
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
pub struct CastRecord1ToRecord2 {
    pub return_ty: SqlScalarType,
    pub cast_exprs: Box<[MirScalarExpr]>,
}

#[sqlfunc(
    CastRecord1ToRecord2,
    sqlname = "record1torecord2",
    output_type_expr = "self.return_ty.without_modifiers().nullable(false)",
    introduces_nulls = false
)]
fn cast_record1_to_record2<'a>(
    &'a self,
    a: DatumList<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut cast_datums = Vec::new();
    for (el, cast_expr) in a.iter().zip_eq(&*self.cast_exprs) {
        cast_datums.push(cast_expr.eval(&[el], temp_storage)?);
    }
    Ok(temp_storage.make_datum(|packer| packer.push_list(cast_datums)))
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
pub struct RecordGet(pub usize);

#[sqlfunc(
    RecordGet,
    output_type_expr = "match &input_type.scalar_type { SqlScalarType::Record { fields, .. } => fields[self.0].1.clone(), _ => unreachable!(\"RecordGet on non-record input: {:?}\", input_type.scalar_type) }",
    introduces_nulls = true,
    skip_display = true
)]
fn record_get<'a>(&self, a: DatumList<'a>) -> Datum<'a> {
    a.iter().nth(self.0).unwrap()
}

impl fmt::Display for RecordGet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "record_get[{}]", self.0)
    }
}
