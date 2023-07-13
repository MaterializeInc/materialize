// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the PostgreSQL project. The original
// source code was retrieved on June 1, 2023 from:
//
//     https://github.com/postgres/postgres/blob/REL_15_3/src/backend/utils/adt/timestamp.c
//
// The original source code is subject to the terms of the PostgreSQL license, a
// copy of which can be found in the LICENSE file at the root of this
// repository.

//! Methods for checked timestamp operations.

use std::fmt::Display;
use std::ops::Sub;

use ::chrono::{
    DateTime, Datelike, Days, Duration, Months, NaiveDate, NaiveDateTime, NaiveTime, Utc,
};
use mz_ore::cast::{self, CastFrom};
use mz_proto::{RustType, TryFromProtoError};
use once_cell::sync::Lazy;
use proptest::arbitrary::Arbitrary;
use proptest::strategy::{BoxedStrategy, Strategy};
use serde::{Serialize, Serializer};
use thiserror::Error;

use crate::adt::datetime::DateTimePart;
use crate::adt::interval::Interval;
use crate::adt::numeric::DecimalLike;
use crate::chrono::ProtoNaiveDateTime;
use crate::scalar::{arb_naive_date_time, arb_utc_date_time};
use crate::Datum;

include!(concat!(env!("OUT_DIR"), "/mz_repr.adt.date.rs"));

const MONTHS_PER_YEAR: i64 = cast::u16_to_i64(Interval::MONTH_PER_YEAR);
const HOURS_PER_DAY: i64 = cast::u16_to_i64(Interval::HOUR_PER_DAY);
const MINUTES_PER_HOUR: i64 = cast::u16_to_i64(Interval::MINUTE_PER_HOUR);
const SECONDS_PER_MINUTE: i64 = cast::u16_to_i64(Interval::SECOND_PER_MINUTE);

const NANOSECONDS_PER_HOUR: i64 = NANOSECONDS_PER_MINUTE * MINUTES_PER_HOUR;
const NANOSECONDS_PER_MINUTE: i64 = NANOSECONDS_PER_SECOND * SECONDS_PER_MINUTE;
const NANOSECONDS_PER_SECOND: i64 = 10i64.pow(9);

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

/// Common set of methods for date component.
pub trait DateLike: chrono::Datelike {
    fn extract_epoch(&self) -> i64 {
        let naive_date = NaiveDate::from_ymd_opt(self.year(), self.month(), self.day())
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
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

/// A timestamp with both a date and a time component, but not necessarily a
/// timezone component.
pub trait TimestampLike:
    Clone
    + PartialOrd
    + std::ops::Add<Duration, Output = Self>
    + std::ops::Sub<Duration, Output = Self>
    + std::ops::Sub<Output = Duration>
    + for<'a> TryInto<Datum<'a>, Error = TimestampError>
    + for<'a> TryFrom<Datum<'a>, Error = ()>
    + TimeLike
    + DateLike
{
    fn new(date: NaiveDate, time: NaiveTime) -> Self;

    /// Returns the weekday as a `usize` between 0 and 6, where 0 represents
    /// Sunday and 6 represents Saturday.
    fn weekday0(&self) -> usize {
        usize::cast_from(self.weekday().num_days_from_sunday())
    }

    /// Like [`chrono::Datelike::year_ce`], but works on the ISO week system.
    fn iso_year_ce(&self) -> u32 {
        let year = self.iso_week().year();
        if year < 1 {
            u32::try_from(1 - year).expect("known to be positive")
        } else {
            u32::try_from(year).expect("known to be positive")
        }
    }

    fn timestamp(&self) -> i64;

    fn timestamp_subsec_micros(&self) -> u32;

    fn extract_epoch<T>(&self) -> T
    where
        T: DecimalLike,
    {
        T::lossy_from(self.timestamp()) + T::from(self.timestamp_subsec_micros()) / T::from(1e6)
    }

    fn truncate_microseconds(&self) -> Self {
        let time = NaiveTime::from_hms_micro_opt(
            self.hour(),
            self.minute(),
            self.second(),
            self.nanosecond() / 1_000,
        )
        .unwrap();

        Self::new(self.date(), time)
    }

    fn truncate_milliseconds(&self) -> Self {
        let time = NaiveTime::from_hms_milli_opt(
            self.hour(),
            self.minute(),
            self.second(),
            self.nanosecond() / 1_000_000,
        )
        .unwrap();

        Self::new(self.date(), time)
    }

    fn truncate_second(&self) -> Self {
        let time = NaiveTime::from_hms_opt(self.hour(), self.minute(), self.second()).unwrap();

        Self::new(self.date(), time)
    }

    fn truncate_minute(&self) -> Self {
        Self::new(
            self.date(),
            NaiveTime::from_hms_opt(self.hour(), self.minute(), 0).unwrap(),
        )
    }

    fn truncate_hour(&self) -> Self {
        Self::new(
            self.date(),
            NaiveTime::from_hms_opt(self.hour(), 0, 0).unwrap(),
        )
    }

    fn truncate_day(&self) -> Self {
        Self::new(self.date(), NaiveTime::from_hms_opt(0, 0, 0).unwrap())
    }

    fn truncate_week(&self) -> Result<Self, TimestampError> {
        let num_days_from_monday = i64::from(self.date().weekday().num_days_from_monday());
        let new_date = NaiveDate::from_ymd_opt(self.year(), self.month(), self.day())
            .unwrap()
            .checked_sub_signed(Duration::days(num_days_from_monday))
            .ok_or(TimestampError::OutOfRange)?;
        Ok(Self::new(
            new_date,
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        ))
    }

    fn truncate_month(&self) -> Self {
        Self::new(
            NaiveDate::from_ymd_opt(self.year(), self.month(), 1).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        )
    }

    fn truncate_quarter(&self) -> Self {
        let month = self.month();
        let quarter = if month <= 3 {
            1
        } else if month <= 6 {
            4
        } else if month <= 9 {
            7
        } else {
            10
        };

        Self::new(
            NaiveDate::from_ymd_opt(self.year(), quarter, 1).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        )
    }

    fn truncate_year(&self) -> Self {
        Self::new(
            NaiveDate::from_ymd_opt(self.year(), 1, 1).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        )
    }
    fn truncate_decade(&self) -> Self {
        Self::new(
            NaiveDate::from_ymd_opt(self.year() - self.year().rem_euclid(10), 1, 1).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        )
    }
    fn truncate_century(&self) -> Self {
        // Expects the first year of the century, meaning 2001 instead of 2000.
        Self::new(
            NaiveDate::from_ymd_opt(
                if self.year() > 0 {
                    self.year() - (self.year() - 1) % 100
                } else {
                    self.year() - self.year() % 100 - 99
                },
                1,
                1,
            )
            .unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        )
    }
    fn truncate_millennium(&self) -> Self {
        // Expects the first year of the millennium, meaning 2001 instead of 2000.
        Self::new(
            NaiveDate::from_ymd_opt(
                if self.year() > 0 {
                    self.year() - (self.year() - 1) % 1000
                } else {
                    self.year() - self.year() % 1000 - 999
                },
                1,
                1,
            )
            .unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        )
    }

    /// Return the date component of the timestamp
    fn date(&self) -> NaiveDate;

    /// Return the date and time of the timestamp
    fn date_time(&self) -> NaiveDateTime;

    /// Return the date and time of the timestamp
    fn from_date_time(dt: NaiveDateTime) -> Self;

    /// Returns a string representing the timezone's offset from UTC.
    fn timezone_offset(&self) -> &'static str;

    /// Returns a string representing the hour portion of the timezone's offset
    /// from UTC.
    fn timezone_hours(&self) -> &'static str;

    /// Returns a string representing the minute portion of the timezone's
    /// offset from UTC.
    fn timezone_minutes(&self) -> &'static str;

    /// Returns the abbreviated name of the timezone with the specified
    /// capitalization.
    fn timezone_name(&self, caps: bool) -> &'static str;

    /// Adds given Duration to the current date and time.
    fn checked_add_signed(self, rhs: Duration) -> Option<Self>;

    /// Subtracts given Duration from the current date and time.
    fn checked_sub_signed(self, rhs: Duration) -> Option<Self>;
}

impl TryFrom<Datum<'_>> for NaiveDateTime {
    type Error = ();

    #[inline]
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Timestamp(dt) => Ok(dt.t),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for DateTime<Utc> {
    type Error = ();

    #[inline]
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::TimestampTz(dt_tz) => Ok(dt_tz.t),
            _ => Err(()),
        }
    }
}

impl TimestampLike for chrono::NaiveDateTime {
    fn new(date: NaiveDate, time: NaiveTime) -> Self {
        NaiveDateTime::new(date, time)
    }

    fn date(&self) -> NaiveDate {
        self.date()
    }

    fn date_time(&self) -> NaiveDateTime {
        self.clone()
    }

    fn from_date_time(dt: NaiveDateTime) -> NaiveDateTime {
        dt
    }

    fn timestamp(&self) -> i64 {
        self.timestamp()
    }

    fn timestamp_subsec_micros(&self) -> u32 {
        self.timestamp_subsec_micros()
    }

    fn timezone_offset(&self) -> &'static str {
        "+00"
    }

    fn timezone_hours(&self) -> &'static str {
        "+00"
    }

    fn timezone_minutes(&self) -> &'static str {
        "00"
    }

    fn timezone_name(&self, _caps: bool) -> &'static str {
        ""
    }

    fn checked_add_signed(self, rhs: Duration) -> Option<Self> {
        self.checked_add_signed(rhs)
    }

    fn checked_sub_signed(self, rhs: Duration) -> Option<Self> {
        self.checked_sub_signed(rhs)
    }
}

impl TimestampLike for chrono::DateTime<chrono::Utc> {
    fn new(date: NaiveDate, time: NaiveTime) -> Self {
        Self::from_date_time(NaiveDateTime::new(date, time))
    }

    fn date(&self) -> NaiveDate {
        self.naive_utc().date()
    }

    fn date_time(&self) -> NaiveDateTime {
        self.naive_utc()
    }

    fn from_date_time(dt: NaiveDateTime) -> Self {
        DateTime::<Utc>::from_utc(dt, Utc)
    }

    fn timestamp(&self) -> i64 {
        self.timestamp()
    }

    fn timestamp_subsec_micros(&self) -> u32 {
        self.timestamp_subsec_micros()
    }

    fn timezone_offset(&self) -> &'static str {
        "+00"
    }

    fn timezone_hours(&self) -> &'static str {
        "+00"
    }

    fn timezone_minutes(&self) -> &'static str {
        "00"
    }

    fn timezone_name(&self, caps: bool) -> &'static str {
        if caps {
            "UTC"
        } else {
            "utc"
        }
    }

    fn checked_add_signed(self, rhs: Duration) -> Option<Self> {
        self.checked_add_signed(rhs)
    }

    fn checked_sub_signed(self, rhs: Duration) -> Option<Self> {
        self.checked_sub_signed(rhs)
    }
}

#[derive(Debug, Error)]
pub enum TimestampError {
    #[error("timestamp out of range")]
    OutOfRange,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CheckedTimestamp<T> {
    t: T,
}

impl<T: Serialize> Serialize for CheckedTimestamp<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.t.serialize(serializer)
    }
}

// We support intersection of the limits of Postgres, Avro, and chrono dates:
// the set of dates that are representable in all used formats.
//
// - Postgres supports 4713 BC to 294276 AD (any time on those days inclusive).
// - Avro supports i64 milliseconds since the Unix epoch: -292275055-05-16
// 16:47:04.192 to 292278994-08-17 07:12:55.807.
// - Avro also supports i64 microseconds since the Unix epoch: -290308-12-21
//   19:59:05.224192 to 294247-01-10 04:00:54.775807.
// - chrono's NaiveDate supports January 1, 262145 BCE to December 31, 262143
//   CE.
//
// Thus on the low end we have 4713-12-31 BC from Postgres, and on the high end
// 262143-12-31 from chrono.

pub static LOW_DATE: Lazy<NaiveDate> =
    Lazy::new(|| NaiveDate::from_ymd_opt(-4713, 12, 31).unwrap());
pub static HIGH_DATE: Lazy<NaiveDate> =
    Lazy::new(|| NaiveDate::from_ymd_opt(262143, 12, 31).unwrap());

impl<T: TimestampLike> CheckedTimestamp<T> {
    pub fn from_timestamplike(t: T) -> Result<Self, TimestampError> {
        let d = t.date();
        if d < *LOW_DATE {
            return Err(TimestampError::OutOfRange);
        }
        if d > *HIGH_DATE {
            return Err(TimestampError::OutOfRange);
        }
        Ok(Self { t })
    }

    pub fn checked_add_signed(self, rhs: Duration) -> Option<T> {
        self.t.checked_add_signed(rhs)
    }

    pub fn checked_sub_signed(self, rhs: Duration) -> Option<T> {
        self.t.checked_sub_signed(rhs)
    }

    /// Returns the difference between `self` and the provided [`CheckedTimestamp`] as a number of
    /// "unit"s.
    ///
    /// Note: used for `DATEDIFF(...)`, which isn't a Postgres function, but is in a number of
    /// other databases.
    pub fn diff_as(&self, other: &Self, unit: DateTimePart) -> Result<i64, TimestampError> {
        const QUARTERS_PER_YEAR: i64 = 4;
        const DAYS_PER_WEEK: i64 = 7;

        fn diff_inner<U>(
            a: &CheckedTimestamp<U>,
            b: &CheckedTimestamp<U>,
            unit: DateTimePart,
        ) -> Option<i64>
        where
            U: TimestampLike,
        {
            match unit {
                DateTimePart::Millennium => {
                    i64::cast_from(a.millennium()).checked_sub(i64::cast_from(b.millennium()))
                }
                DateTimePart::Century => {
                    i64::cast_from(a.century()).checked_sub(i64::cast_from(b.century()))
                }
                DateTimePart::Decade => {
                    i64::cast_from(a.decade()).checked_sub(i64::cast_from(b.decade()))
                }
                DateTimePart::Year => {
                    i64::cast_from(a.year()).checked_sub(i64::cast_from(b.year()))
                }
                DateTimePart::Quarter => {
                    let years = i64::cast_from(a.year()).checked_sub(i64::cast_from(b.year()))?;
                    let quarters = years.checked_mul(QUARTERS_PER_YEAR)?;
                    #[allow(clippy::as_conversions)]
                    let diff = (a.quarter() - b.quarter()) as i64;
                    quarters.checked_add(diff)
                }
                DateTimePart::Month => {
                    let years = i64::cast_from(a.year()).checked_sub(i64::cast_from(b.year()))?;
                    let months = years.checked_mul(MONTHS_PER_YEAR)?;
                    let diff = i64::cast_from(a.month()).checked_sub(i64::cast_from(b.month()))?;
                    months.checked_add(diff)
                }
                DateTimePart::Week => {
                    let diff = a.clone() - b.clone();
                    diff.num_days().checked_div(DAYS_PER_WEEK)
                }
                DateTimePart::Day => {
                    let diff = a.clone() - b.clone();
                    Some(diff.num_days())
                }
                DateTimePart::Hour => {
                    let diff = a.clone() - b.clone();
                    Some(diff.num_hours())
                }
                DateTimePart::Minute => {
                    let diff = a.clone() - b.clone();
                    Some(diff.num_minutes())
                }
                DateTimePart::Second => {
                    let diff = a.clone() - b.clone();
                    Some(diff.num_seconds())
                }
                DateTimePart::Milliseconds => {
                    let diff = a.clone() - b.clone();
                    Some(diff.num_milliseconds())
                }
                DateTimePart::Microseconds => {
                    let diff = a.clone() - b.clone();
                    diff.num_microseconds()
                }
            }
        }

        diff_inner(self, other, unit).ok_or(TimestampError::OutOfRange)
    }

    /// Implementation was roughly ported from Postgres's `timestamp.c`.
    ///
    /// <https://github.com/postgres/postgres/blob/REL_15_3/src/backend/utils/adt/timestamp.c#L3631>
    pub fn age(&self, other: &Self) -> Result<Interval, TimestampError> {
        /// Returns the number of days in the month for which the [`CheckedTimestamp`] is in.
        fn num_days_in_month<T: TimestampLike>(dt: &CheckedTimestamp<T>) -> Option<i64> {
            // Creates a new Date in the same month and year as our original timestamp. Adds one
            // month then subtracts one day, to get the last day of our original month.
            let last_day = NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)?
                .checked_add_months(Months::new(1))?
                .checked_sub_days(Days::new(1))?
                .day();

            Some(CastFrom::cast_from(last_day))
        }

        /// All of the `checked_*` functions return `Option<T>`, so we do all of the math in this
        /// inner function so we can use the `?` operator, maping to a `TimestampError` at the end.
        fn age_inner<U: TimestampLike>(
            a: &CheckedTimestamp<U>,
            b: &CheckedTimestamp<U>,
        ) -> Option<Interval> {
            let mut nanos =
                i64::cast_from(a.nanosecond()).checked_sub(i64::cast_from(b.nanosecond()))?;
            let mut seconds = i64::cast_from(a.second()).checked_sub(i64::cast_from(b.second()))?;
            let mut minutes = i64::cast_from(a.minute()).checked_sub(i64::cast_from(b.minute()))?;
            let mut hours = i64::cast_from(a.hour()).checked_sub(i64::cast_from(b.hour()))?;
            let mut days = i64::cast_from(a.day()).checked_sub(i64::cast_from(b.day()))?;
            let mut months = i64::cast_from(a.month()).checked_sub(i64::cast_from(b.month()))?;
            let mut years = i64::cast_from(a.year()).checked_sub(i64::cast_from(b.year()))?;

            // Flip sign if necessary.
            if a < b {
                nanos = nanos.checked_neg()?;
                seconds = seconds.checked_neg()?;
                minutes = minutes.checked_neg()?;
                hours = hours.checked_neg()?;
                days = days.checked_neg()?;
                months = months.checked_neg()?;
                years = years.checked_neg()?;
            }

            // Carry negative fields into the next higher field.
            while nanos < 0 {
                nanos = nanos.checked_add(NANOSECONDS_PER_SECOND)?;
                seconds = seconds.checked_sub(1)?;
            }
            while seconds < 0 {
                seconds = seconds.checked_add(SECONDS_PER_MINUTE)?;
                minutes = minutes.checked_sub(1)?;
            }
            while minutes < 0 {
                minutes = minutes.checked_add(MINUTES_PER_HOUR)?;
                hours = hours.checked_sub(1)?;
            }
            while hours < 0 {
                hours = hours.checked_add(HOURS_PER_DAY)?;
                days = days.checked_sub(1)?
            }
            while days < 0 {
                if a < b {
                    days = num_days_in_month(a).and_then(|x| days.checked_add(x))?;
                } else {
                    days = num_days_in_month(b).and_then(|x| days.checked_add(x))?;
                }
                months = months.checked_sub(1)?;
            }
            while months < 0 {
                months = months.checked_add(MONTHS_PER_YEAR)?;
                years = years.checked_sub(1)?;
            }

            // Revert the sign back, if we flipped it originally.
            if a < b {
                nanos = nanos.checked_neg()?;
                seconds = seconds.checked_neg()?;
                minutes = minutes.checked_neg()?;
                hours = hours.checked_neg()?;
                days = days.checked_neg()?;
                months = months.checked_neg()?;
                years = years.checked_neg()?;
            }

            let months = i32::try_from(years * MONTHS_PER_YEAR + months).ok()?;
            let days = i32::try_from(days).ok()?;
            let micros = Duration::nanoseconds(
                nanos
                    .checked_add(seconds.checked_mul(NANOSECONDS_PER_SECOND)?)?
                    .checked_add(minutes.checked_mul(NANOSECONDS_PER_MINUTE)?)?
                    .checked_add(hours.checked_mul(NANOSECONDS_PER_HOUR)?)?,
            )
            .num_microseconds()?;

            Some(Interval {
                months,
                days,
                micros,
            })
        }

        // If at any point we overflow, map to a TimestampError.
        age_inner(self, other).ok_or(TimestampError::OutOfRange)
    }
}

impl TryFrom<NaiveDateTime> for CheckedTimestamp<NaiveDateTime> {
    type Error = TimestampError;

    fn try_from(value: NaiveDateTime) -> Result<Self, Self::Error> {
        Self::from_timestamplike(value)
    }
}

impl TryFrom<DateTime<Utc>> for CheckedTimestamp<DateTime<Utc>> {
    type Error = TimestampError;

    fn try_from(value: DateTime<Utc>) -> Result<Self, Self::Error> {
        Self::from_timestamplike(value)
    }
}

impl<T: TimestampLike> std::ops::Deref for CheckedTimestamp<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &self.t
    }
}

impl From<CheckedTimestamp<NaiveDateTime>> for NaiveDateTime {
    fn from(val: CheckedTimestamp<NaiveDateTime>) -> Self {
        val.t
    }
}

impl From<CheckedTimestamp<DateTime<Utc>>> for DateTime<Utc> {
    fn from(val: CheckedTimestamp<DateTime<Utc>>) -> Self {
        val.t
    }
}

impl CheckedTimestamp<NaiveDateTime> {
    pub fn to_naive(&self) -> NaiveDateTime {
        self.t
    }
}

impl CheckedTimestamp<DateTime<Utc>> {
    pub fn to_naive(&self) -> NaiveDateTime {
        self.t.date_naive().and_time(self.t.time())
    }
}

impl Display for CheckedTimestamp<NaiveDateTime> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.t.fmt(f)
    }
}

impl Display for CheckedTimestamp<DateTime<Utc>> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.t.fmt(f)
    }
}

impl RustType<ProtoNaiveDateTime> for CheckedTimestamp<NaiveDateTime> {
    fn into_proto(&self) -> ProtoNaiveDateTime {
        self.t.into_proto()
    }

    fn from_proto(proto: ProtoNaiveDateTime) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            t: NaiveDateTime::from_proto(proto)?,
        })
    }
}

impl RustType<ProtoNaiveDateTime> for CheckedTimestamp<DateTime<Utc>> {
    fn into_proto(&self) -> ProtoNaiveDateTime {
        self.t.into_proto()
    }

    fn from_proto(proto: ProtoNaiveDateTime) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            t: DateTime::<Utc>::from_proto(proto)?,
        })
    }
}

impl<T: Sub<Output = Duration>> Sub<CheckedTimestamp<T>> for CheckedTimestamp<T> {
    type Output = Duration;

    #[inline]
    fn sub(self, rhs: CheckedTimestamp<T>) -> Duration {
        self.t - rhs.t
    }
}

impl<T: Sub<Duration, Output = T>> Sub<Duration> for CheckedTimestamp<T> {
    type Output = T;

    #[inline]
    fn sub(self, rhs: Duration) -> T {
        self.t - rhs
    }
}

impl Arbitrary for CheckedTimestamp<NaiveDateTime> {
    type Parameters = ();
    type Strategy = BoxedStrategy<CheckedTimestamp<NaiveDateTime>>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        arb_naive_date_time()
            .prop_map(|dt| CheckedTimestamp::try_from(dt).unwrap())
            .boxed()
    }
}

impl Arbitrary for CheckedTimestamp<DateTime<Utc>> {
    type Parameters = ();
    type Strategy = BoxedStrategy<CheckedTimestamp<DateTime<Utc>>>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        arb_utc_date_time()
            .prop_map(|dt| CheckedTimestamp::try_from(dt).unwrap())
            .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use proptest::prelude::*;

    #[mz_ore::test]
    fn test_max_age() {
        let low = CheckedTimestamp::try_from(
            LOW_DATE.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()),
        )
        .unwrap();
        let high = CheckedTimestamp::try_from(
            HIGH_DATE.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()),
        )
        .unwrap();

        let years = HIGH_DATE.year() - LOW_DATE.year();
        let months = years * 12;

        // Test high - low.
        let result = high.age(&low).unwrap();
        assert_eq!(result, Interval::new(months, 0, 0));

        // Test low - high.
        let result = low.age(&high).unwrap();
        assert_eq!(result, Interval::new(-months, 0, 0));
    }

    proptest! {
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn test_age_naive(a: CheckedTimestamp<NaiveDateTime>, b: CheckedTimestamp<NaiveDateTime>) {
            let result = a.age(&b);
            prop_assert!(result.is_ok());
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn test_age_utc(a: CheckedTimestamp<DateTime<Utc>>, b: CheckedTimestamp<DateTime<Utc>>) {
            let result = a.age(&b);
            prop_assert!(result.is_ok());
        }
    }
}
