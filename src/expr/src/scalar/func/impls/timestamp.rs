// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Utc};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_repr::adt::datetime::{DateTimeUnits, Timezone};
use mz_repr::adt::interval::Interval;
use mz_repr::adt::numeric::{DecimalLike, Numeric};
use mz_repr::{strconv, ColumnType, ScalarType};

use crate::scalar::func::{EagerUnaryFunc, TimestampLike};
use crate::EvalError;

sqlfunc!(
    #[sqlname = "timestamp_to_text"]
    #[preserves_uniqueness = true]
    fn cast_timestamp_to_string(a: NaiveDateTime) -> String {
        let mut buf = String::new();
        strconv::format_timestamp(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "timestamp_with_time_zone_to_text"]
    #[preserves_uniqueness = true]
    fn cast_timestamp_tz_to_string(a: DateTime<Utc>) -> String {
        let mut buf = String::new();
        strconv::format_timestamptz(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "timestamp_to_date"]
    fn cast_timestamp_to_date(a: NaiveDateTime) -> NaiveDate {
        a.date()
    }
);

sqlfunc!(
    #[sqlname = "timestamp_with_time_zone_to_date"]
    fn cast_timestamp_tz_to_date(a: DateTime<Utc>) -> NaiveDate {
        a.naive_utc().date()
    }
);

sqlfunc!(
    #[sqlname = "timestamp_to_timestamp_with_time_zone"]
    #[preserves_uniqueness = true]
    fn cast_timestamp_to_timestamp_tz(a: NaiveDateTime) -> DateTime<Utc> {
        DateTime::<Utc>::from_utc(a, Utc)
    }
);

sqlfunc!(
    #[sqlname = "timestamp_with_time_zone_to_timestamp"]
    fn cast_timestamp_tz_to_timestamp(a: DateTime<Utc>) -> NaiveDateTime {
        a.naive_utc()
    }
);

sqlfunc!(
    #[sqlname = "timestamp_to_time"]
    fn cast_timestamp_to_time(a: NaiveDateTime) -> NaiveTime {
        a.time()
    }
);

sqlfunc!(
    #[sqlname = "timestamp_with_time_zone_to_time"]
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

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
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

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
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
        DateTimeUnits::Week => Ok(D::from(ts.iso_week_number())),
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

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
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

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
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

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
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

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
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

pub fn date_trunc_inner<T: TimestampLike>(units: DateTimeUnits, ts: T) -> Result<T, EvalError> {
    match units {
        DateTimeUnits::Millennium => Ok(ts.truncate_millennium()),
        DateTimeUnits::Century => Ok(ts.truncate_century()),
        DateTimeUnits::Decade => Ok(ts.truncate_decade()),
        DateTimeUnits::Year => Ok(ts.truncate_year()),
        DateTimeUnits::Quarter => Ok(ts.truncate_quarter()),
        DateTimeUnits::Week => Ok(ts.truncate_week()?),
        DateTimeUnits::Day => Ok(ts.truncate_day()),
        DateTimeUnits::Hour => Ok(ts.truncate_hour()),
        DateTimeUnits::Minute => Ok(ts.truncate_minute()),
        DateTimeUnits::Second => Ok(ts.truncate_second()),
        DateTimeUnits::Month => Ok(ts.truncate_month()),
        DateTimeUnits::Milliseconds => Ok(ts.truncate_milliseconds()),
        DateTimeUnits::Microseconds => Ok(ts.truncate_microseconds()),
        DateTimeUnits::Epoch
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

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct DateTruncTimestamp(pub DateTimeUnits);

impl<'a> EagerUnaryFunc<'a> for DateTruncTimestamp {
    type Input = NaiveDateTime;
    type Output = Result<NaiveDateTime, EvalError>;

    fn call(&self, a: NaiveDateTime) -> Result<NaiveDateTime, EvalError> {
        date_trunc_inner(self.0, a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Timestamp.nullable(input.nullable)
    }
}

impl fmt::Display for DateTruncTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "date_trunc_{}_ts", self.0)
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct DateTruncTimestampTz(pub DateTimeUnits);

impl<'a> EagerUnaryFunc<'a> for DateTruncTimestampTz {
    type Input = DateTime<Utc>;
    type Output = Result<DateTime<Utc>, EvalError>;

    fn call(&self, a: DateTime<Utc>) -> Result<DateTime<Utc>, EvalError> {
        date_trunc_inner(self.0, a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::TimestampTz.nullable(input.nullable)
    }
}

impl fmt::Display for DateTruncTimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "date_trunc_{}_tstz", self.0)
    }
}

/// Converts the timestamp `dt`, which is assumed to be in the time of the timezone `tz` to a timestamptz in UTC.
/// This operation is fallible because certain timestamps at timezones that observe DST are simply impossible or
/// ambiguous. In case of ambiguity (when a hour repeats) we will prefer the latest variant, and when an hour is
/// impossible, we will attempt to fix it by advancing it. For example, `EST` and `2020-11-11T12:39:14` would return
/// `2020-11-11T17:39:14Z`. A DST observing timezone like `America/New_York` would cause the following DST anomalies:
/// `2020-11-01T00:59:59` -> `2020-11-01T04:59:59Z` and `2020-11-01T01:00:00` -> `2020-11-01T06:00:00Z`
/// `2020-03-08T02:59:59` -> `2020-03-08T07:59:59Z` and `2020-03-08T03:00:00` -> `2020-03-08T07:00:00Z`
pub fn timezone_timestamp(tz: Timezone, mut dt: NaiveDateTime) -> Result<DateTime<Utc>, EvalError> {
    let offset = match tz {
        Timezone::FixedOffset(offset) => offset,
        Timezone::Tz(tz) => match tz.offset_from_local_datetime(&dt).latest() {
            Some(offset) => offset.fix(),
            None => {
                dt += Duration::hours(1);
                tz.offset_from_local_datetime(&dt)
                    .latest()
                    .ok_or(EvalError::InvalidTimezoneConversion)?
                    .fix()
            }
        },
    };
    Ok(DateTime::from_utc(dt - offset, Utc))
}

/// Converts the UTC timestamptz `utc` to the local timestamp of the timezone `tz`.
/// For example, `EST` and `2020-11-11T17:39:14Z` would return `2020-11-11T12:39:14`.
pub fn timezone_timestamptz(tz: Timezone, utc: DateTime<Utc>) -> NaiveDateTime {
    let offset = match tz {
        Timezone::FixedOffset(offset) => offset,
        Timezone::Tz(tz) => tz.offset_from_utc_datetime(&utc.naive_utc()).fix(),
    };
    utc.naive_utc() + offset
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct TimezoneTimestamp(pub Timezone);

impl<'a> EagerUnaryFunc<'a> for TimezoneTimestamp {
    type Input = NaiveDateTime;
    type Output = Result<DateTime<Utc>, EvalError>;

    fn call(&self, a: NaiveDateTime) -> Result<DateTime<Utc>, EvalError> {
        timezone_timestamp(self.0, a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::TimestampTz.nullable(input.nullable)
    }
}

impl fmt::Display for TimezoneTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "timezone_{}_ts", self.0)
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct TimezoneTimestampTz(pub Timezone);

impl<'a> EagerUnaryFunc<'a> for TimezoneTimestampTz {
    type Input = DateTime<Utc>;
    type Output = NaiveDateTime;

    fn call(&self, a: DateTime<Utc>) -> NaiveDateTime {
        timezone_timestamptz(self.0, a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Timestamp.nullable(input.nullable)
    }
}

impl fmt::Display for TimezoneTimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "timezone_{}_tstz", self.0)
    }
}
