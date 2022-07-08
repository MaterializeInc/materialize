// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use chrono::{NaiveDateTime, NaiveTime, Offset, TimeZone, Timelike};
use mz_repr::adt::datetime::DateTimeField;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_repr::adt::datetime::{DateTimeUnits, Timezone};
use mz_repr::adt::interval::Interval;
use mz_repr::adt::numeric::{DecimalLike, Numeric};
use mz_repr::{strconv, ColumnType, ScalarType};

use crate::scalar::func::EagerUnaryFunc;
use crate::EvalError;

/// Common set of methods for time component.
pub trait TimeLike: chrono::Timelike {
    fn extract_epoch<T>(&self) -> T
    where
        T: DecimalLike,
    {
        T::from(self.hour() * 60 * 60 + self.minute() * 60) + self.extract_second::<T>()
    }

    fn extract_second<T>(&self) -> T
    where
        T: DecimalLike,
    {
        let s = T::from(self.second());
        let ns = T::from(self.nanosecond()) / T::from(1e9);
        s + ns
    }

    fn extract_millisecond<T>(&self) -> T
    where
        T: DecimalLike,
    {
        let s = T::from(self.second() * 1_000);
        let ns = T::from(self.nanosecond()) / T::from(1e6);
        s + ns
    }

    fn extract_microsecond<T>(&self) -> T
    where
        T: DecimalLike,
    {
        let s = T::from(self.second() * 1_000_000);
        let ns = T::from(self.nanosecond()) / T::from(1e3);
        s + ns
    }
}

impl<T> TimeLike for T where T: chrono::Timelike {}

sqlfunc!(
    #[sqlname = "timetostr"]
    #[preserves_uniqueness = true]
    fn cast_time_to_string(a: NaiveTime) -> String {
        let mut buf = String::new();
        strconv::format_time(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "timetoiv"]
    #[preserves_uniqueness = true]
    fn cast_time_to_interval<'a>(t: NaiveTime) -> Interval {
        // wont overflow because value can't exceed 24 hrs + 1_000_000 ns = 86_400 seconds + 1_000_000 ns = 86_400_001_000 us
        let micros: i64 = Interval::convert_date_time_unit(
            DateTimeField::Second,
            DateTimeField::Microseconds,
            i64::from(t.num_seconds_from_midnight()),
        )
        .unwrap()
            + i64::from(t.nanosecond()) / i64::from(Interval::NANOSECOND_PER_MICROSECOND);

        Interval::new(0, 0, micros)
    }
);

pub fn date_part_time_inner<D>(units: DateTimeUnits, time: NaiveTime) -> Result<D, EvalError>
where
    D: DecimalLike,
{
    match units {
        DateTimeUnits::Epoch => Ok(time.extract_epoch()),
        DateTimeUnits::Hour => Ok(D::from(time.hour())),
        DateTimeUnits::Minute => Ok(D::from(time.minute())),
        DateTimeUnits::Second => Ok(time.extract_second()),
        DateTimeUnits::Milliseconds => Ok(time.extract_millisecond()),
        DateTimeUnits::Microseconds => Ok(time.extract_microsecond()),
        DateTimeUnits::Millennium
        | DateTimeUnits::Century
        | DateTimeUnits::Decade
        | DateTimeUnits::Year
        | DateTimeUnits::Quarter
        | DateTimeUnits::Month
        | DateTimeUnits::Week
        | DateTimeUnits::Day
        | DateTimeUnits::DayOfYear
        | DateTimeUnits::DayOfWeek
        | DateTimeUnits::IsoDayOfYear
        | DateTimeUnits::IsoDayOfWeek => Err(EvalError::UnsupportedUnits(
            format!("{}", units),
            "time".to_string(),
        )),
        DateTimeUnits::Timezone | DateTimeUnits::TimezoneHour | DateTimeUnits::TimezoneMinute => {
            Err(EvalError::Unsupported {
                feature: format!("'{}' timestamp units", units),
                issue_no: None,
            })
        }
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct ExtractTime(pub DateTimeUnits);

impl<'a> EagerUnaryFunc<'a> for ExtractTime {
    type Input = NaiveTime;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: NaiveTime) -> Result<Numeric, EvalError> {
        date_part_time_inner(self.0, a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { max_scale: None }.nullable(input.nullable)
    }
}

impl fmt::Display for ExtractTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "extract_{}_t", self.0)
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct DatePartTime(pub DateTimeUnits);

impl<'a> EagerUnaryFunc<'a> for DatePartTime {
    type Input = NaiveTime;
    type Output = Result<f64, EvalError>;

    fn call(&self, a: NaiveTime) -> Result<f64, EvalError> {
        date_part_time_inner(self.0, a)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Float64.nullable(input.nullable)
    }
}

impl fmt::Display for DatePartTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "date_part_{}_t", self.0)
    }
}

/// Converts the time `t`, which is assumed to be in UTC, to the timezone `tz`.
/// For example, `EST` and `17:39:14` would return `12:39:14`.
pub fn timezone_time(tz: Timezone, t: NaiveTime, wall_time: &NaiveDateTime) -> NaiveTime {
    let offset = match tz {
        Timezone::FixedOffset(offset) => offset,
        Timezone::Tz(tz) => tz.offset_from_utc_datetime(&wall_time).fix(),
    };
    t + offset
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct TimezoneTime {
    pub tz: Timezone,
    #[proptest(strategy = "crate::func::any_naive_datetime()")]
    pub wall_time: NaiveDateTime,
}

impl<'a> EagerUnaryFunc<'a> for TimezoneTime {
    type Input = NaiveTime;
    type Output = NaiveTime;

    fn call(&self, a: NaiveTime) -> NaiveTime {
        timezone_time(self.tz, a, &self.wall_time)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Time.nullable(input.nullable)
    }
}

impl fmt::Display for TimezoneTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "timezone_{}_t", self.tz)
    }
}
