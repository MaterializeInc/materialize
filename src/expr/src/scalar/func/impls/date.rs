// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
use mz_lowertest::MzReflect;
use mz_repr::adt::date::Date;
use mz_repr::adt::datetime::DateTimeUnits;
use mz_repr::adt::numeric::Numeric;
use mz_repr::adt::timestamp::{CheckedTimestamp, DateLike, TimestampPrecision};
use mz_repr::{SqlColumnType, SqlScalarType, strconv};
use serde::{Deserialize, Serialize};

use crate::EvalError;
use crate::func::most_significant_unit;
use crate::scalar::func::EagerUnaryFunc;

sqlfunc!(
    #[sqlname = "date_to_text"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(super::CastStringToDate)]
    fn cast_date_to_string(a: Date) -> String {
        let mut buf = String::new();
        strconv::format_date(&mut buf, a);
        buf
    }
);

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastDateToTimestamp(pub Option<TimestampPrecision>);

impl<'a> EagerUnaryFunc<'a> for CastDateToTimestamp {
    type Input = Date;
    type Output = Result<CheckedTimestamp<NaiveDateTime>, EvalError>;

    fn call(&self, a: Date) -> Result<CheckedTimestamp<NaiveDateTime>, EvalError> {
        let out =
            CheckedTimestamp::from_timestamplike(NaiveDate::from(a).and_hms_opt(0, 0, 0).unwrap())?;
        let updated = out.round_to_precision(self.0)?;
        Ok(updated)
    }

    fn output_type(&self, input: SqlColumnType) -> SqlColumnType {
        SqlScalarType::Timestamp { precision: self.0 }.nullable(input.nullable)
    }

    fn preserves_uniqueness(&self) -> bool {
        true
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastTimestampToDate)
    }

    fn is_monotone(&self) -> bool {
        true
    }
}

impl fmt::Display for CastDateToTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("date_to_timestamp")
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastDateToTimestampTz(pub Option<TimestampPrecision>);

impl<'a> EagerUnaryFunc<'a> for CastDateToTimestampTz {
    type Input = Date;
    type Output = Result<CheckedTimestamp<DateTime<Utc>>, EvalError>;

    fn call(&self, a: Date) -> Result<CheckedTimestamp<DateTime<Utc>>, EvalError> {
        let out =
            CheckedTimestamp::from_timestamplike(DateTime::<Utc>::from_naive_utc_and_offset(
                NaiveDate::from(a).and_hms_opt(0, 0, 0).unwrap(),
                Utc,
            ))?;
        let updated = out.round_to_precision(self.0)?;
        Ok(updated)
    }

    fn output_type(&self, input: SqlColumnType) -> SqlColumnType {
        SqlScalarType::TimestampTz { precision: self.0 }.nullable(input.nullable)
    }

    fn preserves_uniqueness(&self) -> bool {
        true
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastTimestampTzToDate)
    }

    fn is_monotone(&self) -> bool {
        true
    }
}

impl fmt::Display for CastDateToTimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("date_to_timestamp_with_timezone")
    }
}

pub fn extract_date_inner(units: DateTimeUnits, date: NaiveDate) -> Result<Numeric, EvalError> {
    match units {
        DateTimeUnits::Epoch => Ok(Numeric::from(date.extract_epoch())),
        DateTimeUnits::Millennium => Ok(Numeric::from(date.millennium())),
        DateTimeUnits::Century => Ok(Numeric::from(date.century())),
        DateTimeUnits::Decade => Ok(Numeric::from(date.decade())),
        DateTimeUnits::Year => Ok(Numeric::from(date.year())),
        DateTimeUnits::Quarter => Ok(Numeric::from(date.quarter())),
        DateTimeUnits::Week => Ok(Numeric::from(date.iso_week_number())),
        DateTimeUnits::Month => Ok(Numeric::from(date.month())),
        DateTimeUnits::Day => Ok(Numeric::from(date.day())),
        DateTimeUnits::DayOfWeek => Ok(Numeric::from(date.day_of_week())),
        DateTimeUnits::DayOfYear => Ok(Numeric::from(date.ordinal())),
        DateTimeUnits::IsoDayOfWeek => Ok(Numeric::from(date.iso_day_of_week())),
        DateTimeUnits::Hour
        | DateTimeUnits::Minute
        | DateTimeUnits::Second
        | DateTimeUnits::Milliseconds
        | DateTimeUnits::Microseconds => Err(EvalError::UnsupportedUnits(
            format!("{}", units).into(),
            "date".into(),
        )),
        DateTimeUnits::Timezone
        | DateTimeUnits::TimezoneHour
        | DateTimeUnits::TimezoneMinute
        | DateTimeUnits::IsoDayOfYear => Err(EvalError::Unsupported {
            feature: format!("'{}' timestamp units", units).into(),
            discussion_no: None,
        }),
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct ExtractDate(pub DateTimeUnits);

impl<'a> EagerUnaryFunc<'a> for ExtractDate {
    type Input = Date;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: Date) -> Result<Numeric, EvalError> {
        extract_date_inner(self.0, a.into())
    }

    fn output_type(&self, input: SqlColumnType) -> SqlColumnType {
        SqlScalarType::Numeric { max_scale: None }.nullable(input.nullable)
    }

    fn is_monotone(&self) -> bool {
        most_significant_unit(self.0)
    }
}

impl fmt::Display for ExtractDate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "extract_{}_d", self.0)
    }
}
