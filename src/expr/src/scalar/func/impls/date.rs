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
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_repr::adt::datetime::DateTimeUnits;
use mz_repr::adt::numeric::Numeric;
use mz_repr::{strconv, ColumnType, ScalarType};

use crate::scalar::func::EagerUnaryFunc;
use crate::EvalError;

/// Common set of methods for date component.
pub trait DateLike: chrono::Datelike {
    fn extract_epoch(&self) -> i64 {
        let naive_date =
            NaiveDate::from_ymd(self.year(), self.month(), self.day()).and_hms(0, 0, 0);
        naive_date.timestamp()
    }

    fn millennium(&self) -> i32 {
        (self.year() + if self.year() > 0 { 999 } else { -1_000 }) / 1_000
    }

    fn century(&self) -> i32 {
        (self.year() + if self.year() > 0 { 99 } else { -100 }) / 100
    }

    fn decade(&self) -> i32 {
        self.year().div_euclid(10)
    }

    fn quarter(&self) -> f64 {
        (f64::from(self.month()) / 3.0).ceil()
    }

    /// Extract the iso week of the year
    ///
    /// Note that because isoweeks are defined in terms of January 4th, Jan 1 is only in week
    /// 1 about half of the time
    fn iso_week_number(&self) -> u32 {
        self.iso_week().week()
    }

    fn day_of_week(&self) -> u32 {
        self.weekday().num_days_from_sunday()
    }

    fn iso_day_of_week(&self) -> u32 {
        self.weekday().number_from_monday()
    }
}

impl<T> DateLike for T where T: chrono::Datelike {}

sqlfunc!(
    #[sqlname = "date_to_text"]
    #[preserves_uniqueness = true]
    fn cast_date_to_string(a: NaiveDate) -> String {
        let mut buf = String::new();
        strconv::format_date(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "date_to_timestamp"]
    #[preserves_uniqueness = true]
    fn cast_date_to_timestamp(a: NaiveDate) -> NaiveDateTime {
        a.and_hms(0, 0, 0)
    }
);

sqlfunc!(
    #[sqlname = "date_to_timestamp_with_timezone"]
    #[preserves_uniqueness = true]
    fn cast_date_to_timestamp_tz(a: NaiveDate) -> DateTime<Utc> {
        DateTime::<Utc>::from_utc(a.and_hms(0, 0, 0), Utc)
    }
);

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
            format!("{}", units),
            "date".to_string(),
        )),
        DateTimeUnits::Timezone
        | DateTimeUnits::TimezoneHour
        | DateTimeUnits::TimezoneMinute
        | DateTimeUnits::IsoDayOfYear => Err(EvalError::Unsupported {
            feature: format!("'{}' timestamp units", units),
            issue_no: None,
        }),
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct ExtractDate(pub DateTimeUnits);

impl<'a> EagerUnaryFunc<'a> for ExtractDate {
    type Input = NaiveDate;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: NaiveDate) -> Result<Numeric, EvalError> {
        extract_date_inner(self.0, a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { max_scale: None }.nullable(input.nullable)
    }
}

impl fmt::Display for ExtractDate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "extract_{}_d", self.0)
    }
}
