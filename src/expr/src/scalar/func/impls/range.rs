// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_expr_derive::sqlfunc;
use mz_repr::adt::range::Range;
use mz_repr::{Datum, ExcludeNull, SqlScalarType};
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
    Hash
)]
pub struct CastRangeToString {
    pub ty: SqlScalarType,
}

// TODO? if typeconv was in expr, we could determine the inverse of this cast
#[sqlfunc(CastRangeToString, sqlname = "rangetostr", preserves_uniqueness = true)]
fn cast_range_to_string<'a>(&self, a: ExcludeNull<Datum<'a>>) -> Result<String, EvalError> {
    let mut buf = String::new();
    stringify_datum(&mut buf, *a, &self.ty)?;
    Ok(buf)
}

// The monotone claim survives this function mapping empty and
// unbounded-lower ranges to NULL, which the interpreter's endpoint box
// cannot represent, only because those inputs form a downward-closed
// prefix of the range ordering (`None` inner sorts below `Some`, and a
// `None` lower bound sorts below every finite one): a range whose
// endpoints both yield values contains no NULL-yielding interior. Any
// change to range ordering or to this function's NULL cases must revisit
// the claim; see `try_parse_monotonic_iso8601_timestamp` for the
// SpecialUnary alternative.
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
