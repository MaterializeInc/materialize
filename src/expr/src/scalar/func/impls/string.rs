// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use lowertest::MzStructReflect;
use ore::result::ResultExt;
use repr::adt::interval::Interval;
use repr::adt::numeric::{self, Numeric};
use repr::{strconv, ColumnType, ScalarType};

use crate::scalar::func::EagerUnaryFunc;
use crate::EvalError;

sqlfunc!(
    #[sqlname = "strtobool"]
    fn cast_string_to_bool<'a>(a: &'a str) -> Result<bool, EvalError> {
        strconv::parse_bool(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtobytes"]
    #[preserves_uniqueness = true]
    fn cast_string_to_bytes<'a>(a: &'a str) -> Result<Vec<u8>, EvalError> {
        strconv::parse_bytes(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtoi16"]
    fn cast_string_to_int16<'a>(a: &'a str) -> Result<i16, EvalError> {
        strconv::parse_int16(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtoi32"]
    fn cast_string_to_int32<'a>(a: &'a str) -> Result<i32, EvalError> {
        strconv::parse_int32(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtoi64"]
    fn cast_string_to_int64<'a>(a: &'a str) -> Result<i64, EvalError> {
        strconv::parse_int64(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtof32"]
    fn cast_string_to_float32<'a>(a: &'a str) -> Result<f32, EvalError> {
        strconv::parse_float32(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtof64"]
    fn cast_string_to_float64<'a>(a: &'a str) -> Result<f64, EvalError> {
        strconv::parse_float64(a).err_into()
    }
);

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect,
)]
pub struct CastStringToNumeric(pub Option<u8>);

impl<'a> EagerUnaryFunc<'a> for CastStringToNumeric {
    type Input = &'a str;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: &'a str) -> Result<Numeric, EvalError> {
        let mut d = strconv::parse_numeric(a)?;
        if let Some(scale) = self.0 {
            if numeric::rescale(&mut d.0, scale).is_err() {
                return Err(EvalError::NumericFieldOverflow);
            }
        }
        Ok(d.into_inner())
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { scale: self.0 }.nullable(input.nullable)
    }
}

impl fmt::Display for CastStringToNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("i64tonumeric")
    }
}

sqlfunc!(
    #[sqlname = "strtodate"]
    fn cast_string_to_date<'a>(a: &'a str) -> Result<NaiveDate, EvalError> {
        strconv::parse_date(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtotime"]
    fn cast_string_to_time<'a>(a: &'a str) -> Result<NaiveTime, EvalError> {
        strconv::parse_time(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtots"]
    fn cast_string_to_timestamp<'a>(a: &'a str) -> Result<NaiveDateTime, EvalError> {
        strconv::parse_timestamp(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtotstz"]
    fn cast_string_to_timestamp_tz<'a>(a: &'a str) -> Result<DateTime<Utc>, EvalError> {
        strconv::parse_timestamptz(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtoiv"]
    fn cast_string_to_interval<'a>(a: &'a str) -> Result<Interval, EvalError> {
        strconv::parse_interval(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtouuid"]
    fn cast_string_to_uuid<'a>(a: &'a str) -> Result<Uuid, EvalError> {
        strconv::parse_uuid(a).err_into()
    }
);
