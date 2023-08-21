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
use mz_repr::adt::range::Range;
use mz_repr::{ColumnType, Datum, RowArena, ScalarType};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::scalar::func::{stringify_datum, LazyUnaryFunc};
use crate::{EvalError, MirScalarExpr};

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct CastRangeToString {
    pub ty: ScalarType,
}

impl LazyUnaryFunc for CastRangeToString {
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

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for CastRangeToString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("rangetostr")
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct RangeLower;

impl LazyUnaryFunc for RangeLower {
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
        let r = a.unwrap_range();
        Ok(Datum::from(
            r.inner.map(|inner| inner.lower.bound).flatten(),
        ))
    }

    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        input_type
            .scalar_type
            .unwrap_range_element_type()
            .clone()
            .nullable(true)
    }

    fn propagates_nulls(&self) -> bool {
        true
    }

    fn introduces_nulls(&self) -> bool {
        true
    }

    fn preserves_uniqueness(&self) -> bool {
        false
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        None
    }

    fn is_monotone(&self) -> bool {
        true // Ranges are sorted by lower first.
    }
}

impl fmt::Display for RangeLower {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("rangelower")
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct RangeUpper;

impl LazyUnaryFunc for RangeUpper {
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
        let r = a.unwrap_range();
        Ok(Datum::from(
            r.inner.map(|inner| inner.upper.bound).flatten(),
        ))
    }

    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        input_type
            .scalar_type
            .unwrap_range_element_type()
            .clone()
            .nullable(true)
    }

    fn propagates_nulls(&self) -> bool {
        true
    }

    fn introduces_nulls(&self) -> bool {
        true
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

impl fmt::Display for RangeUpper {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("rangeupper")
    }
}

sqlfunc!(
    #[sqlname = "range_empty"]
    fn range_empty(a: Range<Datum<'a>>) -> bool {
        a.inner.is_none()
    }
);

sqlfunc!(
    #[sqlname = "range_lower_inc"]
    fn range_lower_inc(a: Range<Datum<'a>>) -> bool {
        match a.inner {
            None => false,
            Some(inner) => inner.lower.inclusive,
        }
    }
);

sqlfunc!(
    #[sqlname = "range_upper_inc"]
    fn range_upper_inc(a: Range<Datum<'a>>) -> bool {
        match a.inner {
            None => false,
            Some(inner) => inner.upper.inclusive,
        }
    }
);

sqlfunc!(
    #[sqlname = "range_lower_inf"]
    fn range_lower_inf(a: Range<Datum<'a>>) -> bool {
        match a.inner {
            None => false,
            Some(inner) => inner.lower.bound.is_none(),
        }
    }
);

sqlfunc!(
    #[sqlname = "range_upper_inf"]
    fn range_upper_inf(a: Range<Datum<'a>>) -> bool {
        match a.inner {
            None => false,
            Some(inner) => inner.upper.bound.is_none(),
        }
    }
);
