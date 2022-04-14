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

use mz_lowertest::MzReflect;
use mz_repr::adt::datetime::DateTimeUnits;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::numeric::{DecimalLike, Numeric};
use mz_repr::{strconv, ColumnType, ScalarType};

use crate::scalar::func::{EagerUnaryFunc, TimestampLike};
use crate::EvalError;

sqlfunc!(
    #[sqlname = "tstostr"]
    #[preserves_uniqueness = true]
    fn cast_timestamp_to_string(a: NaiveDateTime) -> String {
        let mut buf = String::new();
        strconv::format_timestamp(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "tstztostr"]
    #[preserves_uniqueness = true]
    fn cast_timestamp_tz_to_string(a: DateTime<Utc>) -> String {
        let mut buf = String::new();
        strconv::format_timestamptz(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "tstodate"]
    fn cast_timestamp_to_date(a: NaiveDateTime) -> NaiveDate {
        a.date()
    }
);

sqlfunc!(
    #[sqlname = "tstztodate"]
    fn cast_timestamp_tz_to_date(a: DateTime<Utc>) -> NaiveDate {
        a.naive_utc().date()
    }
);

sqlfunc!(
    #[sqlname = "tstotstz"]
    #[preserves_uniqueness = true]
    fn cast_timestamp_to_timestamp_tz(a: NaiveDateTime) -> DateTime<Utc> {
        DateTime::<Utc>::from_utc(a, Utc)
    }
);

sqlfunc!(
    #[sqlname = "tstztots"]
    fn cast_timestamp_tz_to_timestamp(a: DateTime<Utc>) -> NaiveDateTime {
        a.naive_utc()
    }
);

sqlfunc!(
    #[sqlname = "tstotime"]
    fn cast_timestamp_to_time(a: NaiveDateTime) -> NaiveTime {
        a.time()
    }
);

sqlfunc!(
    #[sqlname = "tstztotime"]
    fn cast_timestamp_tz_to_time(a: DateTime<Utc>) -> NaiveTime {
        a.naive_utc().time()
    }
);

pub fn date_part_interval_inner<D>(units: DateTimeUnits, interval: Interval) -> Result<D, EvalError>
where
    D: DecimalLike,
{
    match units {
        DateTimeUnits::Epoch => Ok(interval.as_epoch_seconds()),
        DateTimeUnits::Millennium => Ok(D::from(interval.millennia())),
        DateTimeUnits::Century => Ok(D::from(interval.centuries())),
        DateTimeUnits::Decade => Ok(D::from(interval.decades())),
        DateTimeUnits::Year => Ok(D::from(interval.years())),
        DateTimeUnits::Quarter => Ok(D::from(interval.quarters())),
        DateTimeUnits::Month => Ok(D::from(interval.months())),
        DateTimeUnits::Day => Ok(D::lossy_from(interval.days())),
        DateTimeUnits::Hour => Ok(D::lossy_from(interval.hours())),
        DateTimeUnits::Minute => Ok(D::lossy_from(interval.minutes())),
        DateTimeUnits::Second => Ok(interval.seconds()),
        DateTimeUnits::Milliseconds => Ok(interval.milliseconds()),
        DateTimeUnits::Microseconds => Ok(interval.microseconds()),
        DateTimeUnits::Week
        | DateTimeUnits::Timezone
        | DateTimeUnits::TimezoneHour
        | DateTimeUnits::TimezoneMinute
        | DateTimeUnits::DayOfWeek
        | DateTimeUnits::DayOfYear
        | DateTimeUnits::IsoDayOfWeek
        | DateTimeUnits::IsoDayOfYear => Err(EvalError::Unsupported {
            feature: format!("'{}' timestamp units", units),
            issue_no: None,
        }),
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct ExtractInterval(pub DateTimeUnits);

impl<'a> EagerUnaryFunc<'a> for ExtractInterval {
    type Input = Interval;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: Interval) -> Result<Numeric, EvalError> {
        date_part_interval_inner(self.0, a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { max_scale: None }.nullable(input.nullable)
    }
}

impl fmt::Display for ExtractInterval {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "extract_{}_iv", self.0)
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct DatePartInterval(pub DateTimeUnits);

impl<'a> EagerUnaryFunc<'a> for DatePartInterval {
    type Input = Interval;
    type Output = Result<f64, EvalError>;

    fn call(&self, a: Interval) -> Result<f64, EvalError> {
        date_part_interval_inner(self.0, a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Float64.nullable(input.nullable)
    }
}

impl fmt::Display for DatePartInterval {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "date_part_{}_iv", self.0)
    }
}

pub fn date_part_timestamp_inner<T, D>(units: DateTimeUnits, ts: T) -> Result<D, EvalError>
where
    T: TimestampLike,
    D: DecimalLike,
{
    match units {
        DateTimeUnits::Epoch => Ok(TimestampLike::extract_epoch(&ts)),
        DateTimeUnits::Millennium => Ok(D::from(ts.millennium())),
        DateTimeUnits::Century => Ok(D::from(ts.century())),
        DateTimeUnits::Decade => Ok(D::from(ts.decade())),
        DateTimeUnits::Year => Ok(D::from(ts.year())),
        DateTimeUnits::Quarter => Ok(D::from(ts.quarter())),
        DateTimeUnits::Week => Ok(D::from(ts.week())),
        DateTimeUnits::Month => Ok(D::from(ts.month())),
        DateTimeUnits::Day => Ok(D::from(ts.day())),
        DateTimeUnits::DayOfWeek => Ok(D::from(ts.day_of_week())),
        DateTimeUnits::DayOfYear => Ok(D::from(ts.ordinal())),
        DateTimeUnits::IsoDayOfWeek => Ok(D::from(ts.iso_day_of_week())),
        DateTimeUnits::Hour => Ok(D::from(ts.hour())),
        DateTimeUnits::Minute => Ok(D::from(ts.minute())),
        DateTimeUnits::Second => Ok(ts.extract_second()),
        DateTimeUnits::Milliseconds => Ok(ts.extract_millisecond()),
        DateTimeUnits::Microseconds => Ok(ts.extract_microsecond()),
        DateTimeUnits::Timezone
        | DateTimeUnits::TimezoneHour
        | DateTimeUnits::TimezoneMinute
        | DateTimeUnits::IsoDayOfYear => Err(EvalError::Unsupported {
            feature: format!("'{}' timestamp units", units),
            issue_no: None,
        }),
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct ExtractTimestamp(pub DateTimeUnits);

impl<'a> EagerUnaryFunc<'a> for ExtractTimestamp {
    type Input = NaiveDateTime;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: NaiveDateTime) -> Result<Numeric, EvalError> {
        date_part_timestamp_inner(self.0, a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { max_scale: None }.nullable(input.nullable)
    }
}

impl fmt::Display for ExtractTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "extract_{}_ts", self.0)
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct ExtractTimestampTz(pub DateTimeUnits);

impl<'a> EagerUnaryFunc<'a> for ExtractTimestampTz {
    type Input = DateTime<Utc>;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: DateTime<Utc>) -> Result<Numeric, EvalError> {
        date_part_timestamp_inner(self.0, a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { max_scale: None }.nullable(input.nullable)
    }
}

impl fmt::Display for ExtractTimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "extract_{}_tstz", self.0)
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct DatePartTimestamp(pub DateTimeUnits);

impl<'a> EagerUnaryFunc<'a> for DatePartTimestamp {
    type Input = NaiveDateTime;
    type Output = Result<f64, EvalError>;

    fn call(&self, a: NaiveDateTime) -> Result<f64, EvalError> {
        date_part_timestamp_inner(self.0, a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Float64.nullable(input.nullable)
    }
}

impl fmt::Display for DatePartTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "date_part_{}_ts", self.0)
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct DatePartTimestampTz(pub DateTimeUnits);

impl<'a> EagerUnaryFunc<'a> for DatePartTimestampTz {
    type Input = DateTime<Utc>;
    type Output = Result<f64, EvalError>;

    fn call(&self, a: DateTime<Utc>) -> Result<f64, EvalError> {
        date_part_timestamp_inner(self.0, a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Float64.nullable(input.nullable)
    }
}

impl fmt::Display for DatePartTimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "date_part_{}_tstz", self.0)
    }
}
