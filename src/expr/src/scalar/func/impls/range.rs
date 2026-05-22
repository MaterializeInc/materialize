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
use mz_repr::adt::range::Range;
use mz_repr::{Datum, RowArena, SqlScalarType};
use serde::{Deserialize, Serialize};

use crate::EvalError;
use crate::scalar::func::stringify_datum;

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
pub struct CastRangeToString {
    pub ty: SqlScalarType,
}

#[sqlfunc(
    CastRangeToString,
    sqlname = "rangetostr",
    preserves_uniqueness = true,
    introduces_nulls = false
)]
fn cast_range_to_string<'a, T>(
    &self,
    a: Range<T>,
    temp_storage: &'a RowArena,
) -> Result<String, EvalError>
where
    T: Into<Datum<'a>>,
{
    let d =
        a.into_bounds(|bound| temp_storage.make_datum_nested(|packer| packer.push(bound.into())));
    let mut buf = String::new();
    stringify_datum(&mut buf, Datum::Range(d), &self.ty)?;
    Ok(buf)
}

#[sqlfunc(sqlname = "rangelower", is_monotone = true)]
fn range_lower<T>(a: Range<T>) -> Option<T> {
    a.inner.map(|inner| inner.lower.bound).flatten()
}

#[sqlfunc(sqlname = "rangeupper")]
fn range_upper<T>(a: Range<T>) -> Option<T> {
    a.inner.map(|inner| inner.upper.bound).flatten()
}

#[sqlfunc(sqlname = "range_empty")]
fn range_empty<T>(a: Range<T>) -> bool {
    a.inner.is_none()
}

#[sqlfunc(sqlname = "range_lower_inc")]
fn range_lower_inc<T>(a: Range<T>) -> bool {
    match a.inner {
        None => false,
        Some(inner) => inner.lower.inclusive,
    }
}

#[sqlfunc(sqlname = "range_upper_inc")]
fn range_upper_inc<T>(a: Range<T>) -> bool {
    match a.inner {
        None => false,
        Some(inner) => inner.upper.inclusive,
    }
}

#[sqlfunc(sqlname = "range_lower_inf")]
fn range_lower_inf<T>(a: Range<T>) -> bool {
    match a.inner {
        None => false,
        Some(inner) => inner.lower.bound.is_none(),
    }
}

#[sqlfunc(sqlname = "range_upper_inf")]
fn range_upper_inf<T>(a: Range<T>) -> bool {
    match a.inner {
        None => false,
        Some(inner) => inner.upper.bound.is_none(),
    }
}
