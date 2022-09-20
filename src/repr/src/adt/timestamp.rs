// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Methods for checked timestamp operations.

use std::fmt::Display;
use std::ops::Sub;

use ::chrono::{DateTime, Duration, NaiveDateTime, NaiveTime, Utc};
use ::chrono::{Datelike, NaiveDate};
use once_cell::sync::Lazy;
use serde::{Serialize, Serializer};
use thiserror::Error;

use mz_proto::{RustType, TryFromProtoError};

use crate::chrono::ProtoNaiveDateTime;
use crate::Datum;

use super::numeric::DecimalLike;

include!(concat!(env!("OUT_DIR"), "/mz_repr.adt.date.rs"));

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
        self.weekday().num_days_from_sunday() as usize
    }

    /// Like [`chrono::Datelike::year_ce`], but works on the ISO week system.
    fn iso_year_ce(&self) -> u32 {
        let year = self.iso_week().year();
        if year < 1 {
            (1 - year) as u32
        } else {
            year as u32
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
        let time = NaiveTime::from_hms_micro(
            self.hour(),
            self.minute(),
            self.second(),
            self.nanosecond() / 1_000,
        );

        Self::new(self.date(), time)
    }

    fn truncate_milliseconds(&self) -> Self {
        let time = NaiveTime::from_hms_milli(
            self.hour(),
            self.minute(),
            self.second(),
            self.nanosecond() / 1_000_000,
        );

        Self::new(self.date(), time)
    }

    fn truncate_second(&self) -> Self {
        let time = NaiveTime::from_hms(self.hour(), self.minute(), self.second());

        Self::new(self.date(), time)
    }

    fn truncate_minute(&self) -> Self {
        Self::new(
            self.date(),
            NaiveTime::from_hms(self.hour(), self.minute(), 0),
        )
    }

    fn truncate_hour(&self) -> Self {
        Self::new(self.date(), NaiveTime::from_hms(self.hour(), 0, 0))
    }

    fn truncate_day(&self) -> Self {
        Self::new(self.date(), NaiveTime::from_hms(0, 0, 0))
    }

    fn truncate_week(&self) -> Result<Self, TimestampError> {
        let num_days_from_monday = self.date().weekday().num_days_from_monday() as i64;
        let new_date = NaiveDate::from_ymd(self.year(), self.month(), self.day())
            .checked_sub_signed(Duration::days(num_days_from_monday))
            .ok_or(TimestampError::OutOfRange)?;
        Ok(Self::new(new_date, NaiveTime::from_hms(0, 0, 0)))
    }

    fn truncate_month(&self) -> Self {
        Self::new(
            NaiveDate::from_ymd(self.year(), self.month(), 1),
            NaiveTime::from_hms(0, 0, 0),
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
            NaiveDate::from_ymd(self.year(), quarter, 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }

    fn truncate_year(&self) -> Self {
        Self::new(
            NaiveDate::from_ymd(self.year(), 1, 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }
    fn truncate_decade(&self) -> Self {
        Self::new(
            NaiveDate::from_ymd(self.year() - self.year().rem_euclid(10), 1, 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }
    fn truncate_century(&self) -> Self {
        // Expects the first year of the century, meaning 2001 instead of 2000.
        Self::new(
            NaiveDate::from_ymd(
                if self.year() > 0 {
                    self.year() - (self.year() - 1) % 100
                } else {
                    self.year() - self.year() % 100 - 99
                },
                1,
                1,
            ),
            NaiveTime::from_hms(0, 0, 0),
        )
    }
    fn truncate_millennium(&self) -> Self {
        // Expects the first year of the millennium, meaning 2001 instead of 2000.
        Self::new(
            NaiveDate::from_ymd(
                if self.year() > 0 {
                    self.year() - (self.year() - 1) % 1000
                } else {
                    self.year() - self.year() % 1000 - 999
                },
                1,
                1,
            ),
            NaiveTime::from_hms(0, 0, 0),
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
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Timestamp(dt) => Ok(dt.t),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for DateTime<Utc> {
    type Error = ();
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

static LOW_DATE: Lazy<NaiveDate> = Lazy::new(|| NaiveDate::from_ymd(-4713, 12, 31));
static HIGH_DATE: Lazy<NaiveDate> = Lazy::new(|| NaiveDate::from_ymd(262143, 12, 31));

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
        self.t.date().naive_utc().and_time(self.t.time())
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
