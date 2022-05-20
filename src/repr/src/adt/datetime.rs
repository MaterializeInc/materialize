// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Date and time utilities.

#![allow(missing_docs)]

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt;
use std::str::FromStr;

use chrono::{FixedOffset, NaiveDate, NaiveTime};
use chrono_tz::Tz;
use proptest::prelude::*;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::adt::interval::Interval;
use crate::chrono::{any_fixed_offset, any_timezone};
use crate::proto::newapi::{RustType, TryFromProtoError};

use mz_lowertest::MzReflect;

include!(concat!(env!("OUT_DIR"), "/mz_repr.adt.datetime.rs"));

/// Units of measurements associated with dates and times.
///
/// TODO(benesch): with enough thinking, this type could probably be merged with
/// `DateTimeField`.
#[derive(
    Arbitrary,
    Clone,
    Copy,
    Debug,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    MzReflect,
)]
pub enum DateTimeUnits {
    Epoch,
    Millennium,
    Century,
    Decade,
    Year,
    Quarter,
    Week,
    Month,
    Hour,
    Day,
    DayOfWeek,
    DayOfYear,
    IsoDayOfWeek,
    IsoDayOfYear,
    Minute,
    Second,
    Milliseconds,
    Microseconds,
    Timezone,
    TimezoneHour,
    TimezoneMinute,
}

impl fmt::Display for DateTimeUnits {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Epoch => f.write_str("epoch"),
            Self::Millennium => f.write_str("millennium"),
            Self::Century => f.write_str("century"),
            Self::Decade => f.write_str("decade"),
            Self::Year => f.write_str("year"),
            Self::Quarter => f.write_str("quarter"),
            Self::Week => f.write_str("week"),
            Self::Month => f.write_str("month"),
            Self::Hour => f.write_str("hour"),
            Self::Day => f.write_str("day"),
            Self::DayOfWeek => f.write_str("dow"),
            Self::DayOfYear => f.write_str("doy"),
            Self::IsoDayOfWeek => f.write_str("isodow"),
            Self::IsoDayOfYear => f.write_str("isodoy"),
            Self::Minute => f.write_str("minute"),
            Self::Second => f.write_str("seconds"),
            Self::Milliseconds => f.write_str("milliseconds"),
            Self::Microseconds => f.write_str("microseconds"),
            Self::Timezone => f.write_str("timezone"),
            Self::TimezoneHour => f.write_str("timezone_hour"),
            Self::TimezoneMinute => f.write_str("timezone_minute"),
        }
    }
}

impl FromStr for DateTimeUnits {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "epoch" => Ok(Self::Epoch),
            "mil" | "millennia" | "millennium" | "millenniums" => Ok(Self::Millennium),
            "c" | "cent" | "century" | "centuries" => Ok(Self::Century),
            "dec" | "decs" | "decade" | "decades" => Ok(Self::Decade),
            "y" | "year" | "years" | "yr" | "yrs" => Ok(Self::Year),
            "qtr" | "quarter" => Ok(Self::Quarter),
            "w" | "week" | "weeks" => Ok(Self::Week),
            "d" | "day" | "days" => Ok(Self::Day),
            "dow" => Ok(Self::DayOfWeek),
            "doy" => Ok(Self::DayOfYear),
            "isodow" => Ok(Self::IsoDayOfWeek),
            "isodoy" => Ok(Self::IsoDayOfYear),
            "h" | "hour" | "hours" | "hr" | "hrs" => Ok(Self::Hour),
            "us" | "usec" | "usecs" | "useconds" | "microsecond" | "microseconds" => {
                Ok(Self::Microseconds)
            }
            "m" | "min" | "mins" | "minute" | "minutes" => Ok(Self::Minute),
            "mon" | "mons" | "month" | "months" => Ok(Self::Month),
            "ms" | "msec" | "msecs" | "mseconds" | "millisecond" | "milliseconds" => {
                Ok(Self::Milliseconds)
            }
            "s" | "sec" | "second" | "seconds" | "secs" => Ok(Self::Second),
            "timezone" => Ok(Self::Timezone),
            "timezone_h" | "timezone_hour" => Ok(Self::TimezoneHour),
            "timezone_m" | "timezone_minute" => Ok(Self::TimezoneMinute),
            _ => Err(format!("unknown units {}", s)),
        }
    }
}

impl From<&DateTimeUnits> for ProtoDateTimeUnits {
    fn from(value: &DateTimeUnits) -> Self {
        use proto_date_time_units::Kind;
        ProtoDateTimeUnits {
            kind: Some(match value {
                DateTimeUnits::Epoch => Kind::Epoch(()),
                DateTimeUnits::Millennium => Kind::Millennium(()),
                DateTimeUnits::Century => Kind::Century(()),
                DateTimeUnits::Decade => Kind::Decade(()),
                DateTimeUnits::Year => Kind::Year(()),
                DateTimeUnits::Quarter => Kind::Quarter(()),
                DateTimeUnits::Week => Kind::Week(()),
                DateTimeUnits::Month => Kind::Month(()),
                DateTimeUnits::Hour => Kind::Hour(()),
                DateTimeUnits::Day => Kind::Day(()),
                DateTimeUnits::DayOfWeek => Kind::DayOfWeek(()),
                DateTimeUnits::DayOfYear => Kind::DayOfYear(()),
                DateTimeUnits::IsoDayOfWeek => Kind::IsoDayOfWeek(()),
                DateTimeUnits::IsoDayOfYear => Kind::IsoDayOfYear(()),
                DateTimeUnits::Minute => Kind::Minute(()),
                DateTimeUnits::Second => Kind::Second(()),
                DateTimeUnits::Milliseconds => Kind::Milliseconds(()),
                DateTimeUnits::Microseconds => Kind::Microseconds(()),
                DateTimeUnits::Timezone => Kind::Timezone(()),
                DateTimeUnits::TimezoneHour => Kind::TimezoneHour(()),
                DateTimeUnits::TimezoneMinute => Kind::TimezoneMinute(()),
            }),
        }
    }
}

impl TryFrom<ProtoDateTimeUnits> for DateTimeUnits {
    type Error = TryFromProtoError;

    fn try_from(value: ProtoDateTimeUnits) -> Result<Self, Self::Error> {
        use proto_date_time_units::Kind;
        let kind = value
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoDateTimeUnits.kind"))?;
        Ok(match kind {
            Kind::Epoch(_) => DateTimeUnits::Epoch,
            Kind::Millennium(_) => DateTimeUnits::Millennium,
            Kind::Century(_) => DateTimeUnits::Century,
            Kind::Decade(_) => DateTimeUnits::Decade,
            Kind::Year(_) => DateTimeUnits::Year,
            Kind::Quarter(_) => DateTimeUnits::Quarter,
            Kind::Week(_) => DateTimeUnits::Week,
            Kind::Month(_) => DateTimeUnits::Month,
            Kind::Hour(_) => DateTimeUnits::Hour,
            Kind::Day(_) => DateTimeUnits::Day,
            Kind::DayOfWeek(_) => DateTimeUnits::DayOfWeek,
            Kind::DayOfYear(_) => DateTimeUnits::DayOfYear,
            Kind::IsoDayOfWeek(_) => DateTimeUnits::IsoDayOfWeek,
            Kind::IsoDayOfYear(_) => DateTimeUnits::IsoDayOfYear,
            Kind::Minute(_) => DateTimeUnits::Minute,
            Kind::Second(_) => DateTimeUnits::Second,
            Kind::Milliseconds(_) => DateTimeUnits::Milliseconds,
            Kind::Microseconds(_) => DateTimeUnits::Microseconds,
            Kind::Timezone(_) => DateTimeUnits::Timezone,
            Kind::TimezoneHour(_) => DateTimeUnits::TimezoneHour,
            Kind::TimezoneMinute(_) => DateTimeUnits::TimezoneMinute,
        })
    }
}

// Order of definition is important for PartialOrd and Ord to be derived correctly
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub enum DateTimeField {
    Microseconds,
    Milliseconds,
    Second,
    Minute,
    Hour,
    Day,
    Month,
    Year,
    Decade,
    Century,
    Millennium,
}

impl fmt::Display for DateTimeField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            DateTimeField::Millennium => "MILLENNIUM",
            DateTimeField::Century => "CENTURY",
            DateTimeField::Decade => "DECADE",
            DateTimeField::Year => "YEAR",
            DateTimeField::Month => "MONTH",
            DateTimeField::Day => "DAY",
            DateTimeField::Hour => "HOUR",
            DateTimeField::Minute => "MINUTE",
            DateTimeField::Second => "SECOND",
            DateTimeField::Milliseconds => "MILLISECONDS",
            DateTimeField::Microseconds => "MICROSECONDS",
        })
    }
}

/// Iterate over `DateTimeField`s in descending significance
impl IntoIterator for DateTimeField {
    type Item = DateTimeField;
    type IntoIter = DateTimeFieldIterator;
    fn into_iter(self) -> DateTimeFieldIterator {
        DateTimeFieldIterator(Some(self))
    }
}

impl FromStr for DateTimeField {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_uppercase().as_ref() {
            "MILLENNIUM" | "MILLENNIUMS" | "MILLENNIA" | "MIL" | "MILS" => Ok(Self::Millennium),
            "CENTURY" | "CENTURIES" | "CENT" | "C" => Ok(Self::Century),
            "DECADE" | "DECADES" | "DEC" | "DECS" => Ok(Self::Decade),
            "YEAR" | "YEARS" | "YR" | "YRS" | "Y" => Ok(Self::Year),
            "MONTH" | "MONTHS" | "MON" | "MONS" => Ok(Self::Month),
            "DAY" | "DAYS" | "D" => Ok(Self::Day),
            "HOUR" | "HOURS" | "HR" | "HRS" | "H" => Ok(Self::Hour),
            "MINUTE" | "MINUTES" | "MIN" | "MINS" | "M" => Ok(Self::Minute),
            "SECOND" | "SECONDS" | "SEC" | "SECS" | "S" => Ok(Self::Second),
            "MILLISECOND" | "MILLISECONDS" | "MILLISECON" | "MILLISECONS" | "MSECOND"
            | "MSECONDS" | "MSEC" | "MSECS" | "MS" => Ok(Self::Milliseconds),
            "MICROSECOND" | "MICROSECONDS" | "MICROSECON" | "MICROSECONS" | "USECOND"
            | "USECONDS" | "USEC" | "USECS" | "US" => Ok(Self::Microseconds),
            _ => Err(format!("invalid DateTimeField: {}", s)),
        }
    }
}

impl DateTimeField {
    /// Iterate the DateTimeField to the next value.
    /// # Panics
    /// - When called on Second
    pub fn next_smallest(self) -> Self {
        self.into_iter()
            .next()
            .unwrap_or_else(|| panic!("Cannot get smaller DateTimeField than {}", self))
    }
    /// Iterate the DateTimeField to the prior value.
    /// # Panics
    /// - When called on Year
    pub fn next_largest(self) -> Self {
        self.into_iter()
            .next_back()
            .unwrap_or_else(|| panic!("Cannot get larger DateTimeField than {}", self))
    }

    /// Returns the number of microseconds in a single unit of `field`.
    ///
    /// # Panics
    ///
    /// Panics if called on a non-time/day field.
    pub fn micros_multiplier(self) -> i64 {
        use DateTimeField::*;
        match self {
            Day | Hour | Minute | Second | Milliseconds | Microseconds => {}
            _other => unreachable!("Do not call with a non-time/day field"),
        }

        Interval::convert_date_time_unit(self, Self::Microseconds, 1i64).unwrap()
    }

    /// Returns the number of months in a single unit of `field`.
    ///
    /// # Panics
    ///
    /// Panics if called on a duration field.
    pub fn month_multiplier(self) -> i64 {
        use DateTimeField::*;
        match self {
            Millennium | Century | Decade | Year => {}
            _other => unreachable!("Do not call with a duration field"),
        }

        Interval::convert_date_time_unit(self, Self::Microseconds, 1i64).unwrap()
    }
}

/// An iterator over DateTimeFields
///
/// Always starts with the value smaller than the current one.
///
/// ```
/// use mz_repr::adt::datetime::DateTimeField::*;
/// let mut itr = Hour.into_iter();
/// assert_eq!(itr.next(), Some(Minute));
/// assert_eq!(itr.next(), Some(Second));
/// assert_eq!(itr.next(), Some(Milliseconds));
/// assert_eq!(itr.next(), Some(Microseconds));
/// assert_eq!(itr.next(), None);
/// ```
#[derive(Debug)]
pub struct DateTimeFieldIterator(Option<DateTimeField>);

/// Go through fields in descending significance order
impl Iterator for DateTimeFieldIterator {
    type Item = DateTimeField;
    fn next(&mut self) -> Option<Self::Item> {
        use DateTimeField::*;
        self.0 = match self.0 {
            Some(Millennium) => Some(Century),
            Some(Century) => Some(Decade),
            Some(Decade) => Some(Year),
            Some(Year) => Some(Month),
            Some(Month) => Some(Day),
            Some(Day) => Some(Hour),
            Some(Hour) => Some(Minute),
            Some(Minute) => Some(Second),
            Some(Second) => Some(Milliseconds),
            Some(Milliseconds) => Some(Microseconds),
            Some(Microseconds) => None,
            None => None,
        };
        self.0.clone()
    }
}

impl DoubleEndedIterator for DateTimeFieldIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        use DateTimeField::*;
        self.0 = match self.0 {
            Some(Millennium) => None,
            Some(Century) => Some(Millennium),
            Some(Decade) => Some(Century),
            Some(Year) => Some(Decade),
            Some(Month) => Some(Year),
            Some(Day) => Some(Month),
            Some(Hour) => Some(Day),
            Some(Minute) => Some(Hour),
            Some(Second) => Some(Minute),
            Some(Milliseconds) => Some(Second),
            Some(Microseconds) => Some(Milliseconds),
            None => None,
        };
        self.0.clone()
    }
}

/// Tracks a unit and a fraction from a parsed time-like string, e.g. INTERVAL
/// '1.2' DAYS.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct DateTimeFieldValue {
    /// Integer part of the value.
    pub unit: i64,
    /// Fractional part of value, padded to billions/has 9 digits of precision,
    /// e.g. `.5` is represented as `500000000`.
    pub fraction: i64,
}

impl Default for DateTimeFieldValue {
    fn default() -> Self {
        DateTimeFieldValue {
            unit: 0,
            fraction: 0,
        }
    }
}

impl DateTimeFieldValue {
    /// Construct `DateTimeFieldValue { unit, fraction }`.
    pub fn new(unit: i64, fraction: i64) -> Self {
        DateTimeFieldValue { unit, fraction }
    }

    /// How much padding is added to the fractional portion to achieve a given precision.
    /// e.g. with the current precision `.5` is represented as `500_000_000`.
    const FRACTIONAL_DIGIT_PRECISION: i64 = 1_000_000_000;
}

/// Parsed timezone.
#[derive(Arbitrary, Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, MzReflect)]
pub enum Timezone {
    #[serde(with = "fixed_offset_serde")]
    FixedOffset(#[proptest(strategy = "any_fixed_offset()")] FixedOffset),
    Tz(#[proptest(strategy = "any_timezone()")] Tz),
}

impl From<&Timezone> for ProtoTimezone {
    fn from(value: &Timezone) -> Self {
        use proto_timezone::Kind;
        ProtoTimezone {
            kind: Some(match value {
                Timezone::FixedOffset(fo) => Kind::FixedOffset(fo.into_proto()),
                Timezone::Tz(tz) => Kind::Tz(tz.into_proto()),
            }),
        }
    }
}

impl TryFrom<ProtoTimezone> for Timezone {
    type Error = TryFromProtoError;

    fn try_from(value: ProtoTimezone) -> Result<Self, Self::Error> {
        use proto_timezone::Kind;
        let kind = value
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoTimezone::kind"))?;
        Ok(match kind {
            Kind::FixedOffset(pof) => Timezone::FixedOffset(FixedOffset::from_proto(pof)?),
            Kind::Tz(ptz) => Timezone::Tz(Tz::from_proto(ptz)?),
        })
    }
}

// We need to implement Serialize and Deserialize traits to include Timezone in the UnaryFunc enum.
// FixedOffset doesn't implement these, even with the "serde" feature enabled.
mod fixed_offset_serde {
    use super::*;
    use serde::{de::Error, Deserializer, Serializer};
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<FixedOffset, D::Error> {
        let offset = i32::deserialize(deserializer)?;
        FixedOffset::east_opt(offset).ok_or_else(|| {
            Error::custom(format!("Invalid timezone offset: |{}| >= 86_400", offset))
        })
    }

    pub fn serialize<S: Serializer>(
        offset: &FixedOffset,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_i32(offset.local_minus_utc())
    }
}

impl PartialOrd for Timezone {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// We need to implement Ord and PartialOrd to include Timezone in the UnaryFunc enum. Neither FixedOffset nor Tz
// implement these so we do a simple ordinal comparison (FixedOffset variant < Tz variant), and break ties using
// i32/str comparisons respectively.
impl Ord for Timezone {
    fn cmp(&self, other: &Self) -> Ordering {
        use Timezone::*;
        match (self, other) {
            (FixedOffset(a), FixedOffset(b)) => a.local_minus_utc().cmp(&b.local_minus_utc()),
            (Tz(a), Tz(b)) => a.name().cmp(b.name()),
            (FixedOffset(_), Tz(_)) => Ordering::Less,
            (Tz(_), FixedOffset(_)) => Ordering::Greater,
        }
    }
}

impl Default for Timezone {
    fn default() -> Self {
        Self::FixedOffset(FixedOffset::east(0))
    }
}

impl fmt::Display for Timezone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Timezone::FixedOffset(offset) => offset.fmt(f),
            Timezone::Tz(tz) => tz.fmt(f),
        }
    }
}

impl FromStr for Timezone {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, String> {
        build_timezone_offset_second(&tokenize_timezone(value)?, value)
    }
}

/// All of the fields that can appear in a literal `DATE`, `TIME`, `TIMESTAMP`
/// or `INTERVAL` string.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParsedDateTime {
    pub millennium: Option<DateTimeFieldValue>,
    pub century: Option<DateTimeFieldValue>,
    pub decade: Option<DateTimeFieldValue>,
    pub year: Option<DateTimeFieldValue>,
    pub month: Option<DateTimeFieldValue>,
    pub day: Option<DateTimeFieldValue>,
    pub hour: Option<DateTimeFieldValue>,
    pub minute: Option<DateTimeFieldValue>,
    // second.fraction is equivalent to nanoseconds.
    pub second: Option<DateTimeFieldValue>,
    pub millisecond: Option<DateTimeFieldValue>,
    pub microsecond: Option<DateTimeFieldValue>,
    pub timezone_offset_second: Option<Timezone>,
}

impl Default for ParsedDateTime {
    fn default() -> Self {
        ParsedDateTime {
            millennium: None,
            century: None,
            decade: None,
            year: None,
            month: None,
            day: None,
            hour: None,
            minute: None,
            second: None,
            millisecond: None,
            microsecond: None,
            timezone_offset_second: None,
        }
    }
}

impl ParsedDateTime {
    /// Compute an Interval from an ParsedDateTime.
    ///
    /// # Errors
    /// - If any component overflows a parameter (i.e. i64).
    pub fn compute_interval(&self) -> Result<Interval, String> {
        use DateTimeField::*;
        let mut months = 0i32;
        let mut days = 0i32;
        let mut micros = 0i64;

        // Add all DateTimeFields, from Millennium to Microseconds.
        self.add_field(Millennium, &mut months, &mut days, &mut micros)?;

        for field in Millennium.into_iter().take_while(|f| *f >= Microseconds) {
            self.add_field(field, &mut months, &mut days, &mut micros)?;
        }

        match Interval::new(months, days, micros) {
            Ok(i) => Ok(i),
            Err(e) => Err(e.to_string()),
        }
    }
    /// Adds the appropriate values from self's ParsedDateTime to `months`,
    /// `days`, and `micros`. These fields are then appropriate to construct
    /// std::time::Duration, once accounting for their sign.
    ///
    /// # Errors
    /// - If any component overflows a parameter (i.e. i64).
    fn add_field(
        &self,
        d: DateTimeField,
        months: &mut i32,
        days: &mut i32,
        micros: &mut i64,
    ) -> Result<(), String> {
        use DateTimeField::*;
        /// divide i by d rounding to the closest integer
        fn div_and_round(i: i128, d: i64) -> Option<i64> {
            let mut res = i / i128::from(d);
            let round_digit = (i / (i128::from(d) / 10)) % 10;
            if round_digit > 4 {
                res += 1;
            } else if round_digit < -4 {
                res -= 1;
            }
            i64::try_from(res).ok()
        }
        match d {
            Millennium | Century | Decade | Year => {
                let (y, y_f) = match self.units_of(d) {
                    Some(y) => (y.unit, y.fraction),
                    None => return Ok(()),
                };
                // months += y.to_month()
                *months = Interval::convert_date_time_unit(d, DateTimeField::Month, y)
                    .and_then(|y_m| i32::try_from(y_m).ok())
                    .and_then(|y_m| months.checked_add(y_m))
                    .ok_or_else(|| {
                        format!(
                            "Overflows maximum months; cannot exceed {}/{} months",
                            i32::MAX,
                            i32::MIN,
                        )
                    })?;

                // months += y_f.to_month() / DateTimeFieldValue::FRACTION_MULTIPLIER
                *months = Interval::convert_date_time_unit(d, DateTimeField::Month, y_f)
                    .and_then(|y_f_m| {
                        y_f_m.checked_div(DateTimeFieldValue::FRACTIONAL_DIGIT_PRECISION)
                    })
                    .and_then(|y_f_m| i32::try_from(y_f_m).ok())
                    .and_then(|y_f_m| months.checked_add(y_f_m))
                    .ok_or_else(|| {
                        format!(
                            "Overflows maximum months; cannot exceed {}/{} months",
                            i32::MAX,
                            i32::MIN,
                        )
                    })?;
                Ok(())
            }
            Month => {
                let (m, m_f) = match self.units_of(Month) {
                    Some(m) => (m.unit, m.fraction),
                    None => return Ok(()),
                };

                // months += m
                *months = i32::try_from(m)
                    .ok()
                    .and_then(|m_month| months.checked_add(m_month))
                    .ok_or_else(|| {
                        format!(
                            "Overflows maximum months; cannot exceed {}/{} months",
                            i32::MAX,
                            i32::MIN,
                        )
                    })?;

                let m_f_days = Interval::convert_date_time_unit(d, DateTimeField::Day, m_f)
                    .ok_or_else(|| "Intermediate overflow in MONTH fraction".to_owned())?;
                // days += m_f.to_day() / DateTimeFieldValue::FRACTION_MULTIPLIER
                *days = m_f_days
                    .checked_div(DateTimeFieldValue::FRACTIONAL_DIGIT_PRECISION)
                    .and_then(|m_f_days| i32::try_from(m_f_days).ok())
                    .and_then(|m_f_days| days.checked_add(m_f_days))
                    .ok_or_else(|| {
                        format!(
                            "Overflows maximum seconds; cannot exceed {}/{} days",
                            i32::MAX,
                            i32::MIN,
                        )
                    })?;

                // micros += (m_f.to_day() % DateTimeFieldValue::FRACTION_MULTIPLIER).to_micros() / DateTimeFieldValue::FRACTION_MULTIPLIER
                *micros = i128::from(m_f_days)
                    .checked_rem(DateTimeFieldValue::FRACTIONAL_DIGIT_PRECISION.into())
                    .and_then(|m_f_us| {
                        Interval::convert_date_time_unit(
                            DateTimeField::Day,
                            DateTimeField::Microseconds,
                            m_f_us,
                        )
                    })
                    .and_then(|m_f_us| {
                        div_and_round(m_f_us, DateTimeFieldValue::FRACTIONAL_DIGIT_PRECISION)
                    })
                    .and_then(|m_f_us| micros.checked_add(m_f_us))
                    .ok_or_else(|| {
                        format!(
                            "Overflows maximum microseconds; cannot exceed {}/{} microseconds",
                            i64::MAX,
                            i64::MIN
                        )
                    })?;

                Ok(())
            }
            Day => {
                let (t, t_f) = match self.units_of(d) {
                    Some(t) => (t.unit, t.fraction),
                    None => return Ok(()),
                };

                // days += t
                *days = i32::try_from(t)
                    .ok()
                    .and_then(|t_day| days.checked_add(t_day))
                    .ok_or_else(|| {
                        format!(
                            "Overflows maximum days; cannot exceed {}/{} days",
                            i32::MAX,
                            i32::MIN,
                        )
                    })?;

                // micros += t_f.to_micros() / DateTimeFieldValue::FRACTION_MULTIPLIER
                *micros = Interval::convert_date_time_unit(
                    d,
                    DateTimeField::Microseconds,
                    i128::from(t_f),
                )
                .and_then(|t_f_us| {
                    div_and_round(t_f_us, DateTimeFieldValue::FRACTIONAL_DIGIT_PRECISION)
                })
                .and_then(|t_f_us| micros.checked_add(t_f_us))
                .ok_or_else(|| {
                    format!(
                        "Overflows maximum microseconds; cannot exceed {}/{} microseconds",
                        i64::MAX,
                        i64::MIN
                    )
                })?;

                Ok(())
            }
            Hour | Minute | Second | Milliseconds | Microseconds => {
                let (t, t_f) = match self.units_of(d) {
                    Some(t) => (t.unit, t.fraction),
                    None => return Ok(()),
                };

                // micros += t.to_micros()
                *micros = Interval::convert_date_time_unit(d, DateTimeField::Microseconds, t)
                    .and_then(|t_s| micros.checked_add(t_s))
                    .ok_or_else(|| {
                        format!(
                            "Overflows maximum microseconds; cannot exceed {}/{} microseconds",
                            i64::MAX,
                            i64::MIN,
                        )
                    })?;

                // micros += t_f.to_micros() / DateTimeFieldValue::FRACTION_MULTIPLIER
                *micros = Interval::convert_date_time_unit(d, DateTimeField::Microseconds, t_f)
                    .and_then(|t_f_ns| {
                        div_and_round(
                            t_f_ns.into(),
                            DateTimeFieldValue::FRACTIONAL_DIGIT_PRECISION,
                        )
                    })
                    .and_then(|t_f_ns| micros.checked_add(t_f_ns))
                    .ok_or_else(|| {
                        format!(
                            "Overflows maximum microseconds; cannot exceed {}/{} microseconds",
                            i64::MAX,
                            i64::MIN,
                        )
                    })?;
                Ok(())
            }
        }
    }

    /// Compute a chrono::NaiveDate from an ParsedDateTime.
    ///
    /// # Errors
    /// - If year, month, or day overflows their respective parameter in
    ///   [chrono::naive::date::NaiveDate::from_ymd_opt](https://docs.rs/chrono/0.4/chrono/naive/struct.NaiveDate.html#method.from_ymd_opt).
    ///
    /// Note: Postgres does not recognize Year 0, but in order to make
    /// arithmetic work as expected, the Year 1 BC in a ParsedDateTime
    /// is mapped to the Year 0 in a NaiveDate, and vice-versa.
    pub fn compute_date(&self) -> Result<chrono::NaiveDate, String> {
        match (self.year, self.month, self.day) {
            (Some(year), Some(month), Some(day)) => {
                // Adjust for BC years
                let year = if year.unit < 0 {
                    year.unit + 1
                } else {
                    year.unit
                };
                let p_err = |e, field| format!("{} in date is invalid: {}", field, e);
                let year = year.try_into().map_err(|e| p_err(e, "Year"))?;
                let month = month.unit.try_into().map_err(|e| p_err(e, "Month"))?;
                let day = day.unit.try_into().map_err(|e| p_err(e, "Day"))?;
                NaiveDate::from_ymd_opt(year, month, day)
                    .ok_or_else(|| "invalid or out-of-range date".into())
            }
            (_, _, _) => Err("YEAR, MONTH, DAY are all required".into()),
        }
    }

    /// Compute a chrono::NaiveDate from an ParsedDateTime.
    ///
    /// # Errors
    /// - If hour, minute, or second (both `unit` and `fraction`) overflow `u32`.
    pub fn compute_time(&self) -> Result<chrono::NaiveTime, String> {
        let p_err = |e, field| format!("invalid {}: {}", field, e);
        let hour = match self.hour {
            Some(hour) => hour.unit.try_into().map_err(|e| p_err(e, "HOUR"))?,
            None => 0,
        };
        let minute = match self.minute {
            Some(minute) => minute.unit.try_into().map_err(|e| p_err(e, "MINUTE"))?,
            None => 0,
        };
        let (second, nano) = match self.second {
            Some(second) => {
                let nano: u32 = second
                    .fraction
                    .try_into()
                    .map_err(|e| p_err(e, "NANOSECOND"))?;
                let second: u32 = second.unit.try_into().map_err(|e| p_err(e, "MINUTE"))?;
                (second, nano)
            }
            None => (0, 0),
        };
        Ok(NaiveTime::from_hms_nano(hour, minute, second, nano))
    }

    /// Builds a ParsedDateTime from an interval string (`value`).
    ///
    /// # Arguments
    ///
    /// * `value` is a PostgreSQL-compatible interval string, e.g `INTERVAL 'value'`.
    /// * `leading_time_precision` optionally identifies the leading time component
    ///   HOUR | MINUTE to disambiguate {}:{} formatted intervals
    /// * `ambiguous_resolver` identifies the DateTimeField of the final part
    ///   if it's ambiguous, e.g. in `INTERVAL '1' MONTH` '1' is ambiguous as its
    ///   DateTimeField, but MONTH resolves the ambiguity.
    pub fn build_parsed_datetime_interval(
        value: &str,
        leading_time_precision: Option<DateTimeField>,
        ambiguous_resolver: DateTimeField,
    ) -> Result<ParsedDateTime, String> {
        use DateTimeField::*;

        let mut pdt = ParsedDateTime::default();
        let mut value_parts = VecDeque::new();
        let mut value_tokens = tokenize_time_str(value.trim())?;
        let mut token_buffer = VecDeque::new();

        while let Some(t) = value_tokens.pop_front() {
            match t {
                TimeStrToken::Delim => {
                    if !token_buffer.is_empty() {
                        value_parts.push_back(token_buffer.clone());
                        token_buffer.clear();
                    }
                }
                // Equivalent to trimming Colons from each leading element.
                TimeStrToken::Colon if token_buffer.is_empty() => {}
                _ => token_buffer.push_back(t),
            }
        }

        if !token_buffer.is_empty() {
            value_parts.push_back(token_buffer)
        }

        let mut annotated_parts = Vec::new();

        while let Some(part) = value_parts.pop_front() {
            let mut fmt = determine_format_w_datetimefield(part.clone(), leading_time_precision)?;
            // If you cannot determine the format of this part, try to infer its
            // format.
            if fmt.is_none() {
                fmt = match value_parts.pop_front() {
                    Some(next_part) => {
                        match determine_format_w_datetimefield(next_part.clone(), None)? {
                            Some(TimePartFormat::SqlStandard(f)) => {
                                match f {
                                    // Do not capture this token because expression
                                    // is going to fail.
                                    Year | Month | Day => None,
                                    // If following part is H:M:S, infer that this
                                    // part is Day. Because this part can use a
                                    // fraction, it should be parsed as PostgreSQL.
                                    _ => {
                                        // We can capture these annotated tokens
                                        // because expressions are commutative.
                                        annotated_parts.push(AnnotatedIntervalPart {
                                            fmt: TimePartFormat::SqlStandard(f),
                                            tokens: next_part.clone(),
                                        });
                                        Some(TimePartFormat::PostgreSql(Day))
                                    }
                                }
                            }
                            // None | Some(TimePartFormat::PostgreSQL(f))
                            // If next_fmt is TimePartFormat::PostgreSQL, that
                            // indicates that the following string was a TimeUnit,
                            // e.g. `day`, and this is where those tokens get
                            // consumed and propagated to their preceding numerical
                            // value.
                            next_fmt => next_fmt,
                        }
                    }
                    // Allow resolution of final part using ambiguous_resolver.
                    None => Some(TimePartFormat::PostgreSql(ambiguous_resolver)),
                }
            }
            match fmt {
                Some(fmt) => annotated_parts.push(AnnotatedIntervalPart {
                    fmt,
                    tokens: part.clone(),
                }),
                None => {
                    return Err("Cannot determine format of all parts. Add \
                        explicit time components, e.g. INTERVAL '1 day' or INTERVAL '1' DAY"
                        .into());
                }
            }
        }

        for mut ap in annotated_parts {
            match ap.fmt {
                TimePartFormat::SqlStandard(f) => {
                    fill_pdt_interval_sql(&mut ap.tokens, f, &mut pdt)?;
                    pdt.check_interval_bounds(f)?;
                }
                TimePartFormat::PostgreSql(f) => fill_pdt_interval_pg(&mut ap.tokens, f, &mut pdt)?,
            }

            // Consume unused TimeUnits for PG compat.
            while let Some(TimeStrToken::TimeUnit(_)) = ap.tokens.front() {
                ap.tokens.pop_front();
            }

            if !ap.tokens.is_empty() {
                return Err(format!(
                    "have unprocessed tokens {}",
                    itertools::join(ap.tokens, "").trim_end(),
                ));
            }
        }

        Ok(pdt)
    }
    /// Builds a ParsedDateTime from a TIMESTAMP string (`value`).
    ///
    /// # Arguments
    ///
    /// * `value` is a SQL-formatted TIMESTAMP string.
    pub fn build_parsed_datetime_timestamp(value: &str) -> Result<ParsedDateTime, String> {
        let mut pdt = ParsedDateTime::default();

        let mut ts_actual = tokenize_time_str(value)?;

        fill_pdt_date(&mut pdt, &mut ts_actual)?;

        if let Some(TimeStrToken::DateTimeDelimiter) = ts_actual.front() {
            ts_actual.pop_front();
            ltrim_delim_or_colon(&mut ts_actual);
        }

        fill_pdt_time(&mut pdt, &mut ts_actual)?;
        pdt.check_datelike_bounds()?;

        if ts_actual.is_empty() {
            Ok(pdt)
        } else {
            Err(format!(
                "have unprocessed tokens {}",
                itertools::join(ts_actual, "").trim_end(),
            ))
        }
    }
    /// Builds a ParsedDateTime from a TIME string (`value`).
    ///
    /// # Arguments
    ///
    /// * `value` is a SQL-formatted TIME string.
    pub fn build_parsed_datetime_time(value: &str) -> Result<ParsedDateTime, String> {
        let mut pdt = ParsedDateTime::default();

        let mut time_actual = tokenize_time_str(value)?;
        fill_pdt_time(&mut pdt, &mut time_actual)?;
        pdt.check_datelike_bounds()?;

        if time_actual.is_empty() {
            Ok(pdt)
        } else {
            Err(format!(
                "have unprocessed tokens {}",
                itertools::join(time_actual, "").trim_end(),
            ))
        }
    }

    /// Write to the specified field of a ParsedDateTime iff it is currently set
    /// to None; otherwise generate an error to propagate to the user.
    pub fn write_field_iff_none(
        &mut self,
        f: DateTimeField,
        u: Option<DateTimeFieldValue>,
    ) -> Result<(), String> {
        use DateTimeField::*;

        if u.is_some() {
            match f {
                Millennium if self.millennium.is_none() => {
                    self.millennium = u;
                }
                Century if self.century.is_none() => {
                    self.century = u;
                }
                Decade if self.decade.is_none() => {
                    self.decade = u;
                }
                Year if self.year.is_none() => {
                    self.year = u;
                }
                Month if self.month.is_none() => {
                    self.month = u;
                }
                Day if self.day.is_none() => {
                    self.day = u;
                }
                Hour if self.hour.is_none() => {
                    self.hour = u;
                }
                Minute if self.minute.is_none() => {
                    self.minute = u;
                }
                Second if self.second.is_none() => {
                    if u.as_ref().unwrap().fraction != 0
                        && (self.millisecond.is_some() || self.microsecond.is_some())
                    {
                        return Err(format!(
                            "Cannot set {} or {} field if {} field has a fraction component",
                            Milliseconds, Microseconds, f
                        ));
                    }
                    self.second = u;
                }
                Milliseconds if self.millisecond.is_none() => {
                    if self.seconds_has_fraction() {
                        return Err(format!(
                            "Cannot set {} or {} field if {} field has a fraction component",
                            f, Microseconds, Second
                        ));
                    }
                    self.millisecond = u;
                }
                Microseconds if self.microsecond.is_none() => {
                    if self.seconds_has_fraction() {
                        return Err(format!(
                            "Cannot set {} or {} field if {} field has a fraction component",
                            Milliseconds, f, Second
                        ));
                    }
                    self.microsecond = u;
                }
                _ => return Err(format!("{} field set twice", f)),
            }
        }
        Ok(())
    }

    fn seconds_has_fraction(&self) -> bool {
        return self.second.is_some() && self.second.as_ref().unwrap().fraction != 0;
    }

    pub fn check_datelike_bounds(&mut self) -> Result<(), String> {
        if let Some(year) = self.year {
            // 1BC is not represented as year 0 at the parser level, only internally
            if year.unit == 0 {
                return Err("YEAR cannot be zero".to_string());
            }
        }
        if let Some(month) = self.month {
            if month.unit < 1 || month.unit > 12 {
                return Err(format!("MONTH must be [1, 12], got {}", month.unit));
            };
        }
        if let Some(day) = self.day {
            if day.unit < 1 || day.unit > 31 {
                return Err(format!("DAY must be [1, 31], got {}", day.unit));
            };
        }
        if let Some(hour) = self.hour {
            if hour.unit < 0 || hour.unit > 23 {
                return Err(format!("HOUR must be [0, 23], got {}", hour.unit));
            };
        }
        if let Some(minute) = self.minute {
            if minute.unit < 0 || minute.unit > 59 {
                return Err(format!("MINUTE must be [0, 59], got {}", minute.unit));
            };
        }

        if let Some(second) = &mut self.second {
            // Chrono supports leap seconds by moving them into nanos:
            // https://docs.rs/chrono/0.4.19/chrono/naive/struct.NaiveTime.html#leap-second-handling
            // See also Tom Lane saying that leap seconds are not well handled:
            // https://www.postgresql.org/message-id/23016.1327976502@sss.pgh.pa.us
            if second.unit == 60 {
                second.unit = 59;
                second.fraction = second.fraction.saturating_add(1_000_000_000);
            }
            if second.unit < 0 || second.unit > 60 {
                return Err(format!("SECOND must be [0, 60], got {}", second.unit));
            };
            if second.fraction < 0 || second.fraction > 1_000_000_000 {
                return Err(format!(
                    "NANOSECOND must be [0, 1_000_000_000], got {}",
                    second.fraction
                ));
            };
        }

        Ok(())
    }
    pub fn check_interval_bounds(&self, d: DateTimeField) -> Result<(), String> {
        use DateTimeField::*;

        match d {
            Millennium | Century | Decade | Year | Month => {
                if let Some(month) = self.month {
                    if month.unit < -12 || month.unit > 12 {
                        return Err(format!("MONTH must be [-12, 12], got {}", month.unit));
                    };
                }
            }
            Hour | Minute | Second | Milliseconds | Microseconds => {
                if let Some(minute) = self.minute {
                    if minute.unit < -59 || minute.unit > 59 {
                        return Err(format!("MINUTE must be [-59, 59], got {}", minute.unit));
                    };
                }

                let mut seconds = 0;
                let mut nanoseconds = 0;

                if let Some(second) = self.second {
                    seconds += second.unit;
                    nanoseconds += second.fraction;
                }

                if let Some(millisecond) = self.millisecond {
                    seconds += millisecond.unit / 1_000;
                    nanoseconds += (millisecond.unit % 1_000) * 1_000_000;
                    nanoseconds += (millisecond.fraction / 1_000) % 1_000_000_000;
                }

                if let Some(microsecond) = self.microsecond {
                    seconds += microsecond.unit / 1_000_000;
                    nanoseconds += (microsecond.unit % 1_000_000) * 1_000;
                    nanoseconds += (microsecond.fraction / 1_000_000) % 1_000_000_000;
                }

                if seconds < -60 || seconds > 60 {
                    return Err(format!("SECOND must be [-60, 60], got {}", seconds));
                };
                if nanoseconds < -1_000_000_000 || nanoseconds > 1_000_000_000 {
                    return Err(format!(
                        "NANOSECOND must be [-1_000_000_000, 1_000_000_000], got {}",
                        nanoseconds
                    ));
                };
            }
            Day => {}
        }

        Ok(())
    }

    pub fn clear_date(&mut self) {
        self.year = None;
        self.month = None;
        self.day = None;
    }

    /// Retrieve any value that we parsed out of the literal string for the
    /// `field`.
    fn units_of(&self, field: DateTimeField) -> Option<DateTimeFieldValue> {
        match field {
            DateTimeField::Millennium => self.millennium,
            DateTimeField::Century => self.century,
            DateTimeField::Decade => self.decade,
            DateTimeField::Year => self.year,
            DateTimeField::Month => self.month,
            DateTimeField::Day => self.day,
            DateTimeField::Hour => self.hour,
            DateTimeField::Minute => self.minute,
            DateTimeField::Second => self.second,
            DateTimeField::Milliseconds => self.millisecond,
            DateTimeField::Microseconds => self.microsecond,
        }
    }
}

/// Fills the year, month, and day fields of `pdt` using the `TimeStrToken`s in
/// `actual`.
///
/// # Args
///
/// - `pdt`: The ParsedDateTime to fill.
/// - `actual`: The queue of tokens representing the string you want use to fill
///   `pdt`'s fields.
fn fill_pdt_date(
    pdt: &mut ParsedDateTime,
    actual: &mut VecDeque<TimeStrToken>,
) -> Result<(), String> {
    use TimeStrToken::*;

    // Check for one number that represents YYYYMMDDD.
    match actual.front() {
        Some(Num(mut val, digits)) if 6 <= *digits && *digits <= 8 => {
            pdt.day = Some(DateTimeFieldValue::new(val % 100, 0));
            val /= 100;
            pdt.month = Some(DateTimeFieldValue::new(val % 100, 0));
            val /= 100;
            // Handle 2 digit year case
            if *digits == 6 {
                if val < 70 {
                    val += 2000;
                } else {
                    val += 1900;
                }
            }
            pdt.year = Some(DateTimeFieldValue::new(val, 0));
            actual.pop_front();
            // Trim remaining optional tokens, but never an immediately
            // following colon
            if let Some(Delim) = actual.front() {
                actual.pop_front();
                ltrim_delim_or_colon(actual);
            }
            return Ok(());
        }
        _ => (),
    }

    let valid_formats = [
        [
            Num(0, 1), // year
            Dash,
            Num(0, 1), // month
            Dash,
            Num(0, 1), // day
        ],
        [
            Num(0, 1), // year
            Delim,
            Num(0, 1), // month
            Dash,
            Num(0, 1), // day
        ],
        [
            Num(0, 1), // year
            Delim,
            Num(0, 1), // month
            Delim,
            Num(0, 1), // day
        ],
    ];

    let original_actual = actual.clone();

    for expected in valid_formats {
        match fill_pdt_from_tokens(pdt, actual, &expected, DateTimeField::Year, 1) {
            Ok(()) => {
                return Ok(());
            }
            Err(_) => {
                *actual = original_actual.clone();
                pdt.clear_date();
            }
        }
    }

    Err("does not match any format for date component".into())
}

/// Fills the hour, minute, and second fields of `pdt` using the `TimeStrToken`s in
/// `actual`.
///
/// # Args
///
/// - `pdt`: The ParsedDateTime to fill.
/// - `actual`: The queue of tokens representing the string you want use to fill
///   `pdt`'s fields.
fn fill_pdt_time(
    pdt: &mut ParsedDateTime,
    actual: &mut VecDeque<TimeStrToken>,
) -> Result<(), String> {
    match determine_format_w_datetimefield(actual.clone(), None)? {
        Some(TimePartFormat::SqlStandard(leading_field)) => {
            let expected = expected_dur_like_tokens(leading_field)?;

            fill_pdt_from_tokens(pdt, actual, expected, leading_field, 1)
        }
        _ => Ok(()),
    }
}

/// Fills a ParsedDateTime's fields when encountering SQL standard-style interval
/// parts, e.g. `1-2` for Y-M `4:5:6.7` for H:M:S.NS.
///
/// Note that:
/// - SQL-standard style groups ({Y-M}{D}{H:M:S.NS}) require that no fields in
///   the group have been modified, and do not allow any fields to be modified
///   afterward.
/// - Single digits, e.g. `3` in `3 4:5:6.7` could be parsed as SQL standard
///   tokens, but end up being parsed as PostgreSQL-style tokens because of their
///   greater expressivity, in that they allow fractions, and otherwise-equivalence.
fn fill_pdt_interval_sql(
    actual: &mut VecDeque<TimeStrToken>,
    leading_field: DateTimeField,
    pdt: &mut ParsedDateTime,
) -> Result<(), String> {
    use DateTimeField::*;

    // Ensure that no fields have been previously modified.
    match leading_field {
        Year | Month => {
            if pdt.year.is_some() || pdt.month.is_some() {
                return Err("YEAR or MONTH field set twice".into());
            }
        }
        Day => {
            if pdt.day.is_some() {
                return Err("DAY field set twice".into());
            }
        }
        Hour | Minute | Second => {
            if pdt.hour.is_some() || pdt.minute.is_some() || pdt.second.is_some() {
                return Err("HOUR, MINUTE, SECOND field set twice".into());
            }
        }
        Millennium | Century | Decade | Milliseconds | Microseconds => {
            return Err(format!(
                "Cannot specify {} field for SQL standard-style interval parts",
                leading_field
            ))
        }
    }

    let expected = expected_sql_standard_interval_tokens(leading_field);

    let sign = trim_and_return_sign(actual);

    fill_pdt_from_tokens(pdt, actual, expected, leading_field, sign)?;

    // Write default values to any unwritten member of the `leading_field`'s group.
    // This will ensure that those fields cannot be written to at a later time, which
    // is part of the SQL standard behavior for timelike components.
    match leading_field {
        Year | Month => {
            if pdt.year.is_none() {
                pdt.year = Some(DateTimeFieldValue::default());
            }
            if pdt.month.is_none() {
                pdt.month = Some(DateTimeFieldValue::default());
            }
        }
        Day => {
            if pdt.day.is_none() {
                pdt.day = Some(DateTimeFieldValue::default());
            }
        }
        Hour | Minute | Second => {
            if pdt.hour.is_none() {
                pdt.hour = Some(DateTimeFieldValue::default());
            }
            if pdt.minute.is_none() {
                pdt.minute = Some(DateTimeFieldValue::default());
            }
            if pdt.second.is_none() {
                pdt.second = Some(DateTimeFieldValue::default());
            }
        }
        Millennium | Century | Decade | Milliseconds | Microseconds => {
            return Err(format!(
                "Cannot specify {} field for SQL standard-style interval parts",
                leading_field
            ))
        }
    }

    Ok(())
}

/// Fills a ParsedDateTime's fields for a single PostgreSQL-style interval
/// parts, e.g. `1 month`. Invoke this function once for each PostgreSQL-style
/// interval part.
///
/// Note that:
/// - This function only meaningfully parses the numerical component of the
///   string, and relies on determining the DateTimeField component from
///   AnnotatedIntervalPart, passed in as `time_unit`.
/// - Only PostgreSQL-style parts can use fractional components in positions
///   other than seconds, e.g. `1.5 months`.
fn fill_pdt_interval_pg(
    actual: &mut VecDeque<TimeStrToken>,
    time_unit: DateTimeField,
    pdt: &mut ParsedDateTime,
) -> Result<(), String> {
    use TimeStrToken::*;

    // We remove all spaces during tokenization, so TimeUnit only shows up if
    // there is no space between the number and the TimeUnit, e.g. `1y 2d 3h`, which
    // PostgreSQL allows.
    let expected = [Num(0, 1), Dot, Nanos(0), TimeUnit(DateTimeField::Year)];

    let sign = trim_and_return_sign(actual);

    fill_pdt_from_tokens(pdt, actual, &expected, time_unit, sign)?;

    Ok(())
}

/// Fills a ParsedDateTime's fields using the `actual` tokens, starting at `leading_field`
/// and descending to less significant DateTimeFields.
///
/// # Errors
/// - If `actual` doesn't match `expected`, modulo skippable TimeStrTokens.
/// - Setting a field in ParsedDateTime that has already been set.
///
/// # Panics
/// - Trying to advance to the next smallest DateTimeField if you're currently
///     at DateTimeField::Second.
fn fill_pdt_from_tokens<'a, E: IntoIterator<Item = &'a TimeStrToken>>(
    pdt: &mut ParsedDateTime,
    actual: &mut VecDeque<TimeStrToken>,
    expected: E,
    leading_field: DateTimeField,
    sign: i64,
) -> Result<(), String> {
    use TimeStrToken::*;
    let mut current_field = leading_field;

    let mut i = 0u8;

    let mut unit_buf: Option<DateTimeFieldValue> = None;

    let mut expected = expected.into_iter().peekable();

    while let (Some(atok), Some(etok)) = (actual.front(), expected.peek()) {
        match (atok, etok) {
            // The following forms of punctuation signal the end of a field and can
            // trigger a write.
            (Dash, Dash) | (Colon, Colon) => {
                pdt.write_field_iff_none(current_field, unit_buf)?;
                unit_buf = None;
                current_field = current_field.next_smallest();
            }
            (Delim, Delim) => {
                pdt.write_field_iff_none(current_field, unit_buf)?;
                unit_buf = None;
                current_field = current_field.next_smallest();

                // Spaces require special processing to allow users to enter an
                // arbitrary number of delimiters wherever they're allowed. Note
                // that this does not include colons.
                actual.pop_front();
                expected.next();
                i += 1;

                while let Some(Delim) = actual.front() {
                    actual.pop_front();
                }

                continue;
            }
            (TimeUnit(f), TimeUnit(_)) => {
                if unit_buf.is_some() && *f != current_field {
                    return Err(format!(
                            "Invalid syntax at offset {}: provided TimeUnit({}) but expected TimeUnit({})",
                            i,
                            f,
                            current_field
                        ));
                }
            }
            // If we got a DateTimeUnits, attempt to convert it to a TimeUnit.
            (DateTimeUnit(u), TimeUnit(_)) => {
                let f = match u {
                    DateTimeUnits::Hour => DateTimeField::Hour,
                    DateTimeUnits::Minute => DateTimeField::Minute,
                    DateTimeUnits::Second => DateTimeField::Second,
                    DateTimeUnits::Milliseconds => DateTimeField::Milliseconds,
                    DateTimeUnits::Microseconds => DateTimeField::Microseconds,
                    _ => return Err(format!("unsupported unit {}", u)),
                };
                if unit_buf.is_some() && f != current_field {
                    return Err(format!(
                            "Invalid syntax at offset {}: provided DateTimeUnit({}) but expected TimeUnit({})",
                            u,
                            f,
                            current_field
                        ));
                }
            }
            (Dot, Dot) => {}
            (Num(val, _), Num(_, _)) => match unit_buf {
                Some(_) => {
                    return Err(
                        "Invalid syntax; parts must be separated by '-', ':', or ' '".to_string(),
                    )
                }
                None => {
                    unit_buf = Some(DateTimeFieldValue {
                        unit: *val * sign,
                        fraction: 0,
                    });
                }
            },
            (Nanos(val), Nanos(_)) => match unit_buf {
                Some(ref mut u) => {
                    u.fraction = *val * sign;
                }
                None => {
                    unit_buf = Some(DateTimeFieldValue {
                        unit: 0,
                        fraction: *val * sign,
                    });
                }
            },
            (Num(n, _), Nanos(_)) => {
                // Create disposable copy of n.
                let mut nc = *n;

                let mut width = 0;
                // Destructively count the number of digits in n.
                while nc != 0 {
                    nc /= 10;
                    width += 1;
                }

                let mut n = *n;

                // Nanoseconds have 9 digits of precision.
                let precision = 9;

                if width > precision {
                    // Trim n to its 9 most significant digits.
                    n /= 10_i64.pow(width - precision);
                } else {
                    // Right-pad n with 0s.
                    n *= 10_i64.pow(precision - width);
                }

                match unit_buf {
                    Some(ref mut u) => {
                        u.fraction = n * sign;
                    }
                    None => {
                        unit_buf = Some(DateTimeFieldValue {
                            unit: 0,
                            fraction: n * sign,
                        });
                    }
                }
            }
            // Allow skipping expected spaces (Delim), numbers, dots, and nanoseconds.
            (_, Num(_, _)) | (_, Dot) | (_, Nanos(_)) | (_, Delim) => {
                expected.next();
                continue;
            }
            (provided, expected) => {
                return Err(format!(
                    "Invalid syntax at offset {i}: provided {provided:?} but expected {expected:?}",
                ))
            }
        }
        i += 1;
        actual.pop_front();
        expected.next();
    }

    ltrim_delim_or_colon(actual);

    pdt.write_field_iff_none(current_field, unit_buf)?;

    Ok(())
}

/// Interval strings can be presented in one of two formats:
/// - SQL Standard, e.g. `1-2 3 4:5:6.7`
/// - PostgreSQL, e.g. `1 year 2 months 3 days`
/// TimePartFormat indicates which type of parsing to use and encodes a
/// DateTimeField, which indicates "where" you should begin parsing the
/// associated tokens w/r/t their respective syntax.
#[derive(Debug, Eq, PartialEq, Clone)]
enum TimePartFormat {
    SqlStandard(DateTimeField),
    PostgreSql(DateTimeField),
}

/// AnnotatedIntervalPart contains the tokens to be parsed, as well as the format
/// to parse them.
#[derive(Debug, Eq, PartialEq, Clone)]
struct AnnotatedIntervalPart {
    pub tokens: VecDeque<TimeStrToken>,
    pub fmt: TimePartFormat,
}

/// Determines the format of the interval part (uses None to identify an
/// indeterminant/ambiguous format). This is necessary because the interval
/// string format is not LL(1); we instead parse as few tokens as possible to
/// generate the string's semantics.
///
/// Note that `toks` should _not_ contain space
fn determine_format_w_datetimefield(
    mut toks: VecDeque<TimeStrToken>,
    leading_time_precision: Option<DateTimeField>,
) -> Result<Option<TimePartFormat>, String> {
    use DateTimeField::*;
    use TimePartFormat::*;
    use TimeStrToken::*;

    trim_and_return_sign(&mut toks);

    if let Some(Num(_, _)) = toks.front() {
        toks.pop_front();
    }

    match toks.pop_front() {
        // Implies {?}{?}{?}, ambiguous case.
        None | Some(Delim) => Ok(None),
        Some(Dot) => {
            match toks.front() {
                Some(Num(_, _)) | Some(Nanos(_)) => {
                    toks.pop_front();
                }
                _ => {}
            }
            match toks.pop_front() {
                // Implies {Num.NumTimeUnit}
                Some(TimeUnit(f)) => Ok(Some(PostgreSql(f))),
                // Implies {?}{?}{?}, ambiguous case.
                _ => Ok(None),
            }
        }
        // Implies {Y-...}{}{}
        Some(Dash) => Ok(Some(SqlStandard(Year))),
        // Implies {}{}{?:...}
        Some(Colon) => {
            if let Some(Num(_, _)) = toks.front() {
                toks.pop_front();
            }

            match toks.pop_front() {
                // Implies {H:M:?...}
                Some(Colon) | Some(Delim) => Ok(Some(SqlStandard(Hour))),
                // Implies {M:S.NS}
                Some(Dot) => Ok(Some(SqlStandard(Minute))),
                // Implies {a:b}. We default to {H:M}, and the leading
                // precision can be specified explicitly
                None => Ok(leading_time_precision
                    .map(SqlStandard)
                    .or(Some(SqlStandard(Hour)))),
                _ => Err("Cannot determine format of all parts".into()),
            }
        }
        // Implies {Num}?{TimeUnit}
        Some(TimeUnit(f)) => Ok(Some(PostgreSql(f))),
        Some(DateTimeUnit(DateTimeUnits::Hour)) => Ok(Some(PostgreSql(Hour))),
        Some(DateTimeUnit(DateTimeUnits::Minute)) => Ok(Some(PostgreSql(Minute))),
        Some(DateTimeUnit(DateTimeUnits::Second)) => Ok(Some(PostgreSql(Second))),
        Some(DateTimeUnit(DateTimeUnits::Milliseconds)) => Ok(Some(PostgreSql(Milliseconds))),
        Some(DateTimeUnit(DateTimeUnits::Microseconds)) => Ok(Some(PostgreSql(Microseconds))),
        Some(DateTimeUnit(_)) => Ok(None),
        _ => Err("Cannot determine format of all parts".into()),
    }
}

/// Get the expected TimeStrTokens to parse SQL Standard time-like
/// DateTimeFields, i.e. HOUR, MINUTE, SECOND. This is used for INTERVAL,
/// TIMESTAMP, and TIME.
///
/// # Errors
/// - If `from` is YEAR, MONTH, or DAY.
fn expected_dur_like_tokens(from: DateTimeField) -> Result<&'static [TimeStrToken], String> {
    use DateTimeField::*;
    use TimeStrToken::*;

    const ALL_TOKS: [TimeStrToken; 7] = [
        Num(0, 1), // hour
        Colon,
        Num(0, 1), // minute
        Colon,
        Num(0, 1), // second
        Dot,
        Nanos(0), // Nanos
    ];
    let start = match from {
        Hour => 0,
        Minute => 2,
        Second => 4,
        _ => {
            return Err(format!(
                "expected_dur_like_tokens can only be called with HOUR, MINUTE, SECOND; got {}",
                from
            ))
        }
    };

    Ok(&ALL_TOKS[start..ALL_TOKS.len()])
}

/// Get the expected TimeStrTokens to parse TimePartFormat::SqlStandard parts,
/// starting from some `DateTimeField`. Delim tokens are never actually included
/// in the output, but are illustrative of what the expected input of SQL
/// Standard interval values looks like.
fn expected_sql_standard_interval_tokens(from: DateTimeField) -> &'static [TimeStrToken] {
    use DateTimeField::*;
    use TimeStrToken::*;

    const ALL_TOKS: [TimeStrToken; 6] = [
        Num(0, 1), // year
        Dash,
        Num(0, 1), // month
        Delim,
        Num(0, 1), // day
        Delim,
    ];

    let (start, end) = match from {
        Year => (0, 4),
        Month => (2, 4),
        Day => (4, 6),
        _ => {
            return expected_dur_like_tokens(from)
                .expect("input to expected_dur_like_tokens shown to be valid");
        }
    };

    &ALL_TOKS[start..end]
}

fn trim_and_return_sign(z: &mut VecDeque<TimeStrToken>) -> i64 {
    use TimeStrToken::*;

    match z.front() {
        Some(Dash) => {
            z.pop_front();
            -1
        }
        Some(Plus) => {
            z.pop_front();
            1
        }
        _ => 1,
    }
}

/// PostgreSQL treats out-of-place colons as errant punctuation marks, and
/// trims them.
fn ltrim_delim_or_colon(z: &mut VecDeque<TimeStrToken>) {
    while Some(&TimeStrToken::Colon) == z.front() || Some(&TimeStrToken::Delim) == z.front() {
        z.pop_front();
    }
}

/// TimeStrToken represents valid tokens in time-like strings,
/// i.e those used in INTERVAL, TIMESTAMP/TZ, DATE, and TIME.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TimeStrToken {
    Dash,
    Colon,
    Dot,
    Plus,
    Zulu,
    // Holds the parsed number and the number of digits in the original string
    Num(i64, usize),
    Nanos(i64),
    // String representation of a named timezone e.g. 'EST'
    TzName(String),
    // Tokenized version of a DateTimeField string e.g. 'YEAR'
    TimeUnit(DateTimeField),
    // Fallback if TimeUnit isn't parseable.
    DateTimeUnit(DateTimeUnits),
    // Used to support ISO-formatted timestamps.
    DateTimeDelimiter,
    // Space arbitrary non-enum punctuation (e.g. !), or leading/trailing
    // colons.
    Delim,
}

impl std::fmt::Display for TimeStrToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TimeStrToken::*;
        match self {
            Dash => write!(f, "-"),
            Colon => write!(f, ":"),
            Dot => write!(f, "."),
            Plus => write!(f, "+"),
            Zulu => write!(f, "Z"),
            Num(i, digits) => write!(f, "{:01$}", i, digits - 1),
            Nanos(i) => write!(f, "{}", i),
            TzName(n) => write!(f, "{}", n),
            TimeUnit(d) => write!(f, "{:?}", d),
            DateTimeUnit(u) => write!(f, "{}", u),
            DateTimeDelimiter => write!(f, "T"),
            Delim => write!(f, " "),
        }
    }
}

/// Convert a string from a time-like datatype (INTERVAL, TIMESTAMP/TZ, DATE, and TIME)
/// into Vec<TimeStrToken>.
///
/// # Warning
/// - Any sequence of numeric characters following a decimal that exceeds 9 characters
///   gets truncated to 9 characters, e.g. `0.1234567899` is truncated to `0.123456789`.
///
/// # Errors
/// - Any sequence of alphabetic characters cannot be cast into a DateTimeField.
/// - Any sequence of numeric characters cannot be cast into an i64.
/// - Any non-alpha numeric character cannot be cast into a TimeStrToken, e.g. `%`.
pub(crate) fn tokenize_time_str(value: &str) -> Result<VecDeque<TimeStrToken>, String> {
    let mut toks = VecDeque::new();
    let mut num_buf = String::with_capacity(4);
    let mut char_buf = String::with_capacity(7);
    fn parse_num(n: &str, idx: usize) -> Result<TimeStrToken, String> {
        Ok(TimeStrToken::Num(
            n.parse().map_err(|e| {
                format!("Unable to parse value as a number at index {}: {}", idx, e)
            })?,
            n.len(),
        ))
    }
    fn maybe_tokenize_num_buf(
        n: &mut String,
        i: usize,
        is_frac: &mut bool,
        t: &mut VecDeque<TimeStrToken>,
    ) -> Result<(), String> {
        if !n.is_empty() {
            if *is_frac {
                // Fractions only support 9 places of precision.
                n.truncate(9);
                let raw: i64 = n
                    .parse()
                    .map_err(|e| format!("couldn't parse fraction {}: {}", n, e))?;
                // this is guaranteed to be ascii, so len is fine
                let multiplicand = 1_000_000_000 / 10_i64.pow(n.len() as u32);
                t.push_back(TimeStrToken::Nanos(raw * multiplicand));
                n.clear();
            } else {
                t.push_back(parse_num(n, i)?);
                n.clear();
            }
        }
        *is_frac = false;
        Ok(())
    }
    fn maybe_tokenize_char_buf(
        c: &mut String,
        t: &mut VecDeque<TimeStrToken>,
    ) -> Result<(), String> {
        if !c.is_empty() {
            // Supports ISO-formatted datetime strings.
            if c == "T" || c == "t" {
                t.push_back(TimeStrToken::DateTimeDelimiter);
            } else {
                match c.to_uppercase().parse() {
                    Ok(u) => t.push_back(TimeStrToken::TimeUnit(u)),
                    Err(_) => t.push_back(TimeStrToken::DateTimeUnit(c.parse()?)),
                }
            }
            c.clear();
        }
        Ok(())
    }
    let mut next_num_is_frac = false;
    for (i, chr) in value.chars().enumerate() {
        if !num_buf.is_empty() && !char_buf.is_empty() {
            return Err("Could not tokenize".into());
        }
        match chr {
            '+' => {
                maybe_tokenize_num_buf(&mut num_buf, i, &mut next_num_is_frac, &mut toks)?;
                maybe_tokenize_char_buf(&mut char_buf, &mut toks)?;
                toks.push_back(TimeStrToken::Plus);
            }
            '-' => {
                maybe_tokenize_num_buf(&mut num_buf, i, &mut next_num_is_frac, &mut toks)?;
                maybe_tokenize_char_buf(&mut char_buf, &mut toks)?;
                toks.push_back(TimeStrToken::Dash);
            }
            ':' => {
                maybe_tokenize_num_buf(&mut num_buf, i, &mut next_num_is_frac, &mut toks)?;
                maybe_tokenize_char_buf(&mut char_buf, &mut toks)?;
                toks.push_back(TimeStrToken::Colon);
            }
            '.' => {
                maybe_tokenize_num_buf(&mut num_buf, i, &mut next_num_is_frac, &mut toks)?;
                maybe_tokenize_char_buf(&mut char_buf, &mut toks)?;
                toks.push_back(TimeStrToken::Dot);
                next_num_is_frac = true;
            }
            chr if chr.is_digit(10) => {
                maybe_tokenize_char_buf(&mut char_buf, &mut toks)?;
                num_buf.push(chr);
            }
            chr if chr.is_ascii_alphabetic() => {
                maybe_tokenize_num_buf(&mut num_buf, i, &mut next_num_is_frac, &mut toks)?;
                char_buf.push(chr);
            }
            chr if chr.is_ascii_whitespace() || chr.is_ascii_punctuation() => {
                maybe_tokenize_num_buf(&mut num_buf, i, &mut next_num_is_frac, &mut toks)?;
                maybe_tokenize_char_buf(&mut char_buf, &mut toks)?;
                toks.push_back(TimeStrToken::Delim);
            }
            _ => {
                return Err(format!(
                    "Invalid character at offset {} in {}: {:?}",
                    i, value, chr
                ))
            }
        }
    }

    maybe_tokenize_num_buf(&mut num_buf, value.len(), &mut next_num_is_frac, &mut toks)?;
    maybe_tokenize_char_buf(&mut char_buf, &mut toks)?;

    ltrim_delim_or_colon(&mut toks);

    Ok(toks)
}

fn tokenize_timezone(value: &str) -> Result<Vec<TimeStrToken>, String> {
    let mut toks: Vec<TimeStrToken> = vec![];
    let mut num_buf = String::with_capacity(4);
    // If the timezone string has a colon, we need to parse all numbers naively.
    // Otherwise we need to parse long sequences of digits as [..hhhhmm]
    let split_nums: bool = !value.contains(':');

    // Takes a string and tries to parse it as a number token and insert it into
    // the token list
    fn parse_num(
        toks: &mut Vec<TimeStrToken>,
        n: &str,
        split_nums: bool,
        idx: usize,
    ) -> Result<(), String> {
        if n.is_empty() {
            return Ok(());
        }

        let (first, second) = if n.len() > 2 && split_nums {
            let (first, second) = n.split_at(n.len() - 2);
            (first, Some(second))
        } else {
            (n, None)
        };

        toks.push(TimeStrToken::Num(
            first.parse().map_err(|e| {
                format!(
                    "Unable to tokenize value {} as a number at index {}: {}",
                    first, idx, e
                )
            })?,
            first.len(),
        ));

        if let Some(second) = second {
            toks.push(TimeStrToken::Num(
                second.parse().map_err(|e| {
                    format!(
                        "Unable to tokenize value {} as a number at index {}: {}",
                        second, idx, e
                    )
                })?,
                second.len(),
            ));
        }

        Ok(())
    }
    for (i, chr) in value.chars().enumerate() {
        match chr {
            '-' => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(TimeStrToken::Dash);
            }
            ' ' => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(TimeStrToken::Delim);
            }
            ':' => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(TimeStrToken::Colon);
            }
            '+' => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(TimeStrToken::Plus);
            }
            chr if (chr == 'z' || chr == 'Z') && (i == value.len() - 1) => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(TimeStrToken::Zulu);
            }
            chr if chr.is_digit(10) => num_buf.push(chr),
            chr if chr.is_ascii_alphabetic() => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                let substring = &value[i..];
                toks.push(TimeStrToken::TzName(substring.to_string()));
                return Ok(toks);
            }
            chr => {
                return Err(format!(
                    "Error tokenizing timezone string ('{}'): invalid character {:?} at offset {}",
                    value, chr, i
                ))
            }
        }
    }
    parse_num(&mut toks, &num_buf, split_nums, 0)?;
    Ok(toks)
}

fn build_timezone_offset_second(tokens: &[TimeStrToken], value: &str) -> Result<Timezone, String> {
    use TimeStrToken::*;
    let all_formats = [
        vec![Plus, Num(0, 1), Colon, Num(0, 1), Colon, Num(0, 1)],
        vec![Dash, Num(0, 1), Colon, Num(0, 1), Colon, Num(0, 1)],
        vec![Plus, Num(0, 1), Colon, Num(0, 1)],
        vec![Dash, Num(0, 1), Colon, Num(0, 1)],
        vec![Plus, Num(0, 1), Num(0, 1), Num(0, 1)],
        vec![Dash, Num(0, 1), Num(0, 1), Num(0, 1)],
        vec![Plus, Num(0, 1), Num(0, 1)],
        vec![Dash, Num(0, 1), Num(0, 1)],
        vec![Plus, Num(0, 1)],
        vec![Dash, Num(0, 1)],
        vec![TzName("".to_string())],
        vec![Zulu],
    ];

    let mut is_positive = true;
    let mut hour_offset: Option<i64> = None;
    let mut minute_offset: Option<i64> = None;
    let mut second_offset: Option<i64> = None;

    for format in all_formats.iter() {
        let actual = tokens.iter();

        if actual.len() != format.len() {
            continue;
        }

        for (i, (atok, etok)) in actual.zip(format).enumerate() {
            match (atok, etok) {
                (Colon, Colon) | (Plus, Plus) => { /* Matching punctuation */ }
                (Dash, Dash) => {
                    is_positive = false;
                }
                (Num(val, _), Num(_, _)) => {
                    let val = *val;
                    match (hour_offset, minute_offset, second_offset) {
                        (None, None, None) => {
                            // Postgres allows timezones in the range -15:59:59..15:59:59
                            if val <= 15 {
                                hour_offset = Some(val);
                            } else {
                                return Err(format!(
                                    "Invalid timezone string ({}): timezone hour invalid {}",
                                    value, val
                                ));
                            }
                        }
                        (Some(_), None, None) => {
                            if val < 60 {
                                minute_offset = Some(val);
                            } else {
                                return Err(format!(
                                    "Invalid timezone string ({}): timezone minute invalid {}",
                                    value, val
                                ));
                            }
                        }
                        (Some(_), Some(_), None) => {
                            if val < 60 {
                                second_offset = Some(val);
                            } else {
                                return Err(format!(
                                    "Invalid timezone string ({}): timezone second invalid {}",
                                    value, val
                                ));
                            }
                        }
                        // We've already seen an hour a minute and a second so we should
                        // never see another number
                        (Some(_), Some(_), Some(_)) => {
                            return Err(format!(
                                "Invalid timezone string ({}): invalid value {} at token index {}",
                                value, val, i
                            ))
                        }
                        _ => unreachable!("parsed a minute before an hour!"),
                    }
                }
                (Zulu, Zulu) => return Ok(Default::default()),
                (TzName(val), TzName(_)) => {
                    return match Tz::from_str_insensitive(val) {
                        Ok(tz) => Ok(Timezone::Tz(tz)),
                        Err(err) => Err(format!(
                            "Invalid timezone string ({}): {}. \
                            Failed to parse {} at token index {}",
                            value, err, val, i
                        )),
                    };
                }
                (_, _) => {
                    // Theres a mismatch between this format and the actual
                    // token stream Stop trying to parse in this format and go
                    // to the next one
                    is_positive = true;
                    hour_offset = None;
                    minute_offset = None;
                    second_offset = None;
                    break;
                }
            }
        }

        // Return the first valid parsed result
        if let Some(hour_offset) = hour_offset {
            let mut tz_offset_second: i64 = hour_offset * 60 * 60;

            if let Some(minute_offset) = minute_offset {
                tz_offset_second += minute_offset * 60;
            }

            if let Some(second_offset) = second_offset {
                tz_offset_second += second_offset;
            }

            let offset = if is_positive {
                FixedOffset::east(tz_offset_second as i32)
            } else {
                FixedOffset::west(tz_offset_second as i32)
            };

            return Ok(Timezone::FixedOffset(offset));
        }
    }

    Err(format!("Cannot parse timezone offset {}", value))
}

/// Takes a 'date timezone' 'date time timezone' string and splits it into 'date
/// {time}' and 'timezone' components
pub(crate) fn split_timestamp_string(value: &str) -> (&str, &str) {
    // First we need to see if the string contains " +" or " -" because
    // timestamps can come in a format YYYY-MM-DD {+|-}<tz> (where the timezone
    // string can have colons)
    let cut = value.find(" +").or_else(|| value.find(" -"));

    if let Some(cut) = cut {
        let (first, second) = value.split_at(cut);
        return (first.trim(), second.trim());
    }

    // If we have a hh:mm:dd component, we need to go past that to see if we can
    // find a tz
    let colon = value.find(':');

    if let Some(colon) = colon {
        let substring = value.get(colon..);
        if let Some(substring) = substring {
            let tz = substring
                .find(|c: char| (c == '-') || (c == '+') || (c == ' ') || c.is_ascii_alphabetic());

            if let Some(tz) = tz {
                let (first, second) = value.split_at(colon + tz);
                return (first.trim(), second.trim());
            }
        }
        (value.trim(), "")
    } else {
        // We don't have a time, so the only formats available are YYY-mm-dd<tz>
        // or YYYY-MM-dd <tz> Numeric offset timezones need to be separated from
        // the ymd by a space
        let cut = value.find(|c: char| c.is_ascii_alphabetic());

        if let Some(cut) = cut {
            let (first, second) = value.split_at(cut);
            return (first.trim(), second.trim());
        }

        (value.trim(), "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::protobuf_roundtrip;

    #[test]
    fn iterate_datetimefield() {
        use DateTimeField::*;
        assert_eq!(
            Millennium.into_iter().take(10).collect::<Vec<_>>(),
            vec![
                Century,
                Decade,
                Year,
                Month,
                Day,
                Hour,
                Minute,
                Second,
                Milliseconds,
                Microseconds
            ]
        )
    }

    #[test]
    fn test_expected_dur_like_tokens() {
        use DateTimeField::*;
        use TimeStrToken::*;
        assert_eq!(
            expected_sql_standard_interval_tokens(Hour),
            vec![Num(0, 1), Colon, Num(0, 1), Colon, Num(0, 1), Dot, Nanos(0)]
        );
        assert_eq!(
            expected_sql_standard_interval_tokens(Minute),
            vec![Num(0, 1), Colon, Num(0, 1), Dot, Nanos(0)]
        );
        assert_eq!(
            expected_sql_standard_interval_tokens(Second),
            vec![Num(0, 1), Dot, Nanos(0)]
        );
    }

    #[test]
    fn test_expected_sql_standard_interval_tokens() {
        use DateTimeField::*;
        use TimeStrToken::*;
        assert_eq!(
            expected_sql_standard_interval_tokens(Year),
            vec![Num(0, 1), Dash, Num(0, 1), Delim]
        );

        assert_eq!(
            expected_sql_standard_interval_tokens(Day),
            vec![Num(0, 1), Delim]
        );
        assert_eq!(
            expected_sql_standard_interval_tokens(Hour),
            vec![Num(0, 1), Colon, Num(0, 1), Colon, Num(0, 1), Dot, Nanos(0)]
        );
    }
    #[test]
    fn test_trim_and_return_sign() {
        let test_cases = [
            ("-2", -1, "2"),
            ("3", 1, "3"),
            ("+5", 1, "5"),
            ("-", -1, ""),
            ("-YEAR", -1, "YEAR"),
            ("YEAR", 1, "YEAR"),
        ];

        for test in test_cases.iter() {
            let mut s = tokenize_time_str(test.0).unwrap();

            assert_eq!(trim_and_return_sign(&mut s), test.1);
            assert_eq!(s.front(), tokenize_time_str(test.2).unwrap().front());
        }
    }
    #[test]
    fn test_determine_format_w_datetimefield() {
        use DateTimeField::*;
        use TimePartFormat::*;

        let test_cases = [
            ("1-2 3", Some(SqlStandard(Year))),
            ("4:5", Some(SqlStandard(Hour))),
            ("4:5.6", Some(SqlStandard(Minute))),
            ("-4:5.6", Some(SqlStandard(Minute))),
            ("+4:5.6", Some(SqlStandard(Minute))),
            ("year", Some(PostgreSql(Year))),
            ("4year", Some(PostgreSql(Year))),
            ("-4year", Some(PostgreSql(Year))),
            ("5", None),
            ("5.6", None),
            ("3 4:5:6.7", None),
        ];

        for test in test_cases.iter() {
            let s = tokenize_time_str(test.0).unwrap();

            match (
                determine_format_w_datetimefield(s, None).unwrap(),
                test.1.as_ref(),
            ) {
                (Some(a), Some(b)) => {
                    if a != *b {
                        panic!(
                            "determine_format_w_datetimefield_and_time returned {:?}, expected {:?}",
                            a, b,
                        )
                    }
                }
                (None, None) => {}
                (x, y) => panic!(
                    "determine_format_w_datetimefield_and_time returned {:?}, expected {:?}",
                    x, y,
                ),
            }
        }
    }
    #[test]
    fn test_determine_format_w_datetimefield_and_leading_time() {
        use DateTimeField::*;
        use TimePartFormat::*;

        assert_eq!(
            determine_format_w_datetimefield(tokenize_time_str("4:5").unwrap(), None,).unwrap(),
            Some(SqlStandard(Hour))
        );
        assert_eq!(
            determine_format_w_datetimefield(
                tokenize_time_str("4:5").unwrap(),
                Some(DateTimeField::Minute),
            )
            .unwrap(),
            Some(SqlStandard(Minute))
        );
        assert_eq!(
            determine_format_w_datetimefield(
                tokenize_time_str("4:5").unwrap(),
                Some(DateTimeField::Hour),
            )
            .unwrap(),
            Some(SqlStandard(Hour))
        );
    }
    #[test]
    fn test_determine_format_w_datetimefield_error() {
        let test_cases = [
            ("1+2", "Cannot determine format of all parts"),
            ("1:2+3", "Cannot determine format of all parts"),
            ("1:1YEAR2", "Cannot determine format of all parts"),
        ];

        for test in test_cases.iter() {
            let s = tokenize_time_str(test.0).unwrap();
            match determine_format_w_datetimefield(s, None) {
                Err(e) => assert_eq!(e.to_string(), test.1),
                Ok(f) => panic!(
                    "Test passed when expected to fail: {}, generated {:?}",
                    test.0, f
                ),
            };
        }
    }

    #[test]
    fn test_fill_pdt_from_tokens() {
        use DateTimeField::*;
        let test_cases = [
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 0)),
                    ..Default::default()
                },
                "1 2 3 4 5 6",
                "0 0 0 0 0 0",
                Year,
                1,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(4, 0)),
                    hour: Some(DateTimeFieldValue::new(5, 0)),
                    minute: Some(DateTimeFieldValue::new(6, 0)),
                    ..Default::default()
                },
                "4 5 6",
                "0 0 0",
                Day,
                1,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(-4, 0)),
                    hour: Some(DateTimeFieldValue::new(-5, 0)),
                    minute: Some(DateTimeFieldValue::new(-6, 0)),
                    ..Default::default()
                },
                "4 5 6",
                "0 0 0",
                Day,
                -1,
            ),
            // Mixed delimiter parsing
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 0)),
                    ..Default::default()
                },
                "1-2:3-4 5 6",
                "0-0:0-0 0 0",
                Year,
                1,
            ),
            // Skip an element at the end
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(5, 0)),
                    minute: Some(DateTimeFieldValue::new(6, 0)),
                    ..Default::default()
                },
                "1 2 3 5 6",
                "0 0 0 0 0 0",
                Year,
                1,
            ),
            // Skip an element w/ non-space parsing
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 0)),
                    ..Default::default()
                },
                "1-2:3- 5 6",
                "0-0:0-0 0 0",
                Year,
                1,
            ),
            // Get Nanos from tokenizer
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 700_000_000)),
                    ..Default::default()
                },
                "1-2:3-4 5 6.7",
                "0-0:0-0 0 0.0",
                Year,
                1,
            ),
            // Proper fraction/nano conversion anywhere
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 200_000_000)),
                    ..Default::default()
                },
                "1.2",
                "0.0",
                Year,
                1,
            ),
            (
                ParsedDateTime {
                    minute: Some(DateTimeFieldValue::new(1, 200_000_000)),
                    ..Default::default()
                },
                "1.2",
                "0.0",
                Minute,
                1,
            ),
            // Parse TimeUnit
            (
                ParsedDateTime {
                    month: Some(DateTimeFieldValue::new(3, 0)),
                    ..Default::default()
                },
                "3MONTHS",
                "0YEAR",
                Month,
                1,
            ),
            (
                ParsedDateTime {
                    month: Some(DateTimeFieldValue::new(1, 0)),
                    day: Some(DateTimeFieldValue::new(2, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    ..Default::default()
                },
                "1MONTHS 2DAYS 3HOURS",
                "0YEAR 0YEAR 0YEAR",
                Month,
                1,
            ),
            (
                ParsedDateTime {
                    month: Some(DateTimeFieldValue::new(1, 0)),
                    day: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "1MONTHS-2",
                "0YEAR-0",
                Month,
                1,
            ),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let mut actual = tokenize_time_str(test.1).unwrap();
            let expected = tokenize_time_str(test.2).unwrap();

            fill_pdt_from_tokens(&mut pdt, &mut actual, &expected, test.3, test.4).unwrap();

            assert_eq!(pdt, test.0);
        }
    }

    #[test]
    fn test_fill_pdt_from_tokens_errors() {
        use DateTimeField::*;
        let test_cases = [
            // Mismatched syntax
            (
                "1 2 3",
                "0-0 0",
                Year,
                1,
                "Invalid syntax at offset 1: provided Delim but expected Dash",
            ),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let mut actual = tokenize_time_str(test.0).unwrap();
            let expected = tokenize_time_str(test.1).unwrap();

            match fill_pdt_from_tokens(&mut pdt, &mut actual, &expected, test.2, test.3) {
                Err(e) => assert_eq!(e.to_string(), test.4),
                Ok(_) => panic!("Test passed when expected to fail, generated {:?}", pdt),
            };
        }
    }
    #[test]
    #[should_panic(expected = "Cannot get smaller DateTimeField than MICROSECONDS")]
    fn test_fill_pdt_from_tokens_panic() {
        use DateTimeField::*;
        let test_cases = [
            // Mismatched syntax
            ("1 2", "0 0", Microseconds, 1),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let mut actual = tokenize_time_str(test.0).unwrap();
            let expected = tokenize_time_str(test.1).unwrap();

            if fill_pdt_from_tokens(&mut pdt, &mut actual, &expected, test.2, test.3).is_ok() {
                panic!(
                    "test_fill_pdt_from_tokens_panic should have panicked. input {}\nformat {}\
                     \nDateTimeField {}\nGenerated ParsedDateTime {:?}",
                    test.0, test.1, test.2, pdt
                );
            };
        }
    }

    #[test]
    fn test_fill_pdt_interval_pg() {
        use DateTimeField::*;
        let test_cases = [
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "2",
                Year,
            ),
            (
                ParsedDateTime {
                    month: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    ..Default::default()
                },
                "2.3",
                Month,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(-2, -300_000_000)),
                    ..Default::default()
                },
                "-2.3",
                Day,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "2",
                Hour,
            ),
            (
                ParsedDateTime {
                    minute: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    ..Default::default()
                },
                "2.3",
                Minute,
            ),
            (
                ParsedDateTime {
                    second: Some(DateTimeFieldValue::new(-2, -300_000_000)),
                    ..Default::default()
                },
                "-2.3",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "2year",
                Year,
            ),
            (
                ParsedDateTime {
                    month: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    ..Default::default()
                },
                "2.3month",
                Month,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(-2, -300_000_000)),
                    ..Default::default()
                },
                "-2.3day",
                Day,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "2hour",
                Hour,
            ),
            (
                ParsedDateTime {
                    minute: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    ..Default::default()
                },
                "2.3minute",
                Minute,
            ),
            (
                ParsedDateTime {
                    second: Some(DateTimeFieldValue::new(-2, -300_000_000)),
                    ..Default::default()
                },
                "-2.3second",
                Second,
            ),
            (
                ParsedDateTime {
                    second: Some(DateTimeFieldValue::new(-2, -300_000_000)),
                    ..Default::default()
                },
                ":::::::::-2.3second",
                Second,
            ),
            (
                ParsedDateTime {
                    second: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    ..Default::default()
                },
                ":::::::::+2.3second",
                Second,
            ),
            (
                ParsedDateTime {
                    millisecond: Some(DateTimeFieldValue::new(1, 200_000_000)),
                    ..Default::default()
                },
                "1.2milliseconds",
                Milliseconds,
            ),
            (
                ParsedDateTime {
                    microsecond: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    ..Default::default()
                },
                "2.3microseconds",
                Microseconds,
            ),
            (
                ParsedDateTime {
                    millennium: Some(DateTimeFieldValue::new(4, 500_000_000)),
                    ..Default::default()
                },
                "4.5millennium",
                Millennium,
            ),
            (
                ParsedDateTime {
                    century: Some(DateTimeFieldValue::new(6, 700_000_000)),
                    ..Default::default()
                },
                "6.7century",
                Century,
            ),
            (
                ParsedDateTime {
                    decade: Some(DateTimeFieldValue::new(8, 900_000_000)),
                    ..Default::default()
                },
                "8.9decade",
                Decade,
            ),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let mut actual = tokenize_time_str(test.1).unwrap();
            fill_pdt_interval_pg(&mut actual, test.2, &mut pdt).unwrap();

            assert_eq!(pdt, test.0);
        }
    }

    #[test]
    fn fill_pdt_interval_pg_errors() {
        use DateTimeField::*;
        let test_cases = [
            // Invalid syntax
            (
                "1.2.",
                Month,
                "Invalid syntax at offset 3: provided Dot but expected TimeUnit(Year)",
            ),
            // Running into this error means that determine_format_w_datetimefield
            // failed.
            (
                "1YEAR",
                Month,
                "Invalid syntax at offset 1: provided TimeUnit(YEAR) but expected TimeUnit(MONTH)",
            ),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let mut actual = tokenize_time_str(test.0).unwrap();
            match fill_pdt_interval_pg(&mut actual, test.1, &mut pdt) {
                Err(e) => assert_eq!(e.to_string(), test.2),
                Ok(_) => panic!(
                    "Test passed when expected to fail, generated {:?}, expected error {}",
                    pdt, test.2,
                ),
            };
        }
    }

    #[test]
    fn test_fill_pdt_interval_sql() {
        use DateTimeField::*;
        let test_cases = [
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "1-2",
                Year,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(1, 0)),
                    minute: Some(DateTimeFieldValue::new(2, 0)),
                    second: Some(DateTimeFieldValue::new(3, 400_000_000)),
                    ..Default::default()
                },
                "1:2:3.4",
                Hour,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(1, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(3, 400_000_000)),
                    ..Default::default()
                },
                "1::3.4",
                Hour,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(1, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(0, 400_000_000)),
                    ..Default::default()
                },
                "1::.4",
                Hour,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(0, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(0, 400_000_000)),
                    ..Default::default()
                },
                ".4",
                Second,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(0, 0)),
                    minute: Some(DateTimeFieldValue::new(1, 0)),
                    second: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    ..Default::default()
                },
                "1:2.3",
                Minute,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(-1, 0)),
                    month: Some(DateTimeFieldValue::new(-2, 0)),
                    ..Default::default()
                },
                "-1-2",
                Year,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(-1, 0)),
                    minute: Some(DateTimeFieldValue::new(-2, 0)),
                    second: Some(DateTimeFieldValue::new(-3, -400_000_000)),
                    ..Default::default()
                },
                "-1:2:3.4",
                Hour,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "+1-2",
                Year,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(1, 0)),
                    minute: Some(DateTimeFieldValue::new(2, 0)),
                    second: Some(DateTimeFieldValue::new(3, 400_000_000)),
                    ..Default::default()
                },
                "+1:2:3.4",
                Hour,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(-1, 0)),
                    month: Some(DateTimeFieldValue::new(-2, 0)),
                    ..Default::default()
                },
                "::::::-1-2",
                Year,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(-1, 0)),
                    minute: Some(DateTimeFieldValue::new(-2, 0)),
                    second: Some(DateTimeFieldValue::new(-3, -400_000_000)),
                    ..Default::default()
                },
                ":::::::-1:2:3.4",
                Hour,
            ),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let mut actual = tokenize_time_str(test.1).unwrap();
            fill_pdt_interval_sql(&mut actual, test.2, &mut pdt).unwrap();

            assert_eq!(pdt, test.0);
        }
    }

    #[test]
    fn test_fill_pdt_interval_sql_errors() {
        use DateTimeField::*;
        let test_cases = [
            // Invalid syntax
            (
                "1.2",
                Year,
                "Invalid syntax at offset 1: provided Dot but expected Dash",
            ),
            (
                "1-2:3.4",
                Minute,
                "Invalid syntax at offset 1: provided Dash but expected Colon",
            ),
            (
                "1YEAR",
                Year,
                "Invalid syntax at offset 1: provided TimeUnit(Year) but expected Dash",
            ),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let mut actual = tokenize_time_str(test.0).unwrap();
            match fill_pdt_interval_sql(&mut actual, test.1, &mut pdt) {
                Err(e) => assert_eq!(e.to_string(), test.2),
                Ok(_) => panic!("Test passed when expected to fail, generated {:?}", pdt),
            };
        }
    }

    #[test]
    fn test_build_parsed_datetime_time() {
        run_test_build_parsed_datetime_time(
            "3:4:5.6",
            ParsedDateTime {
                hour: Some(DateTimeFieldValue::new(3, 0)),
                minute: Some(DateTimeFieldValue::new(4, 0)),
                second: Some(DateTimeFieldValue::new(5, 600_000_000)),
                ..Default::default()
            },
        );
        run_test_build_parsed_datetime_time(
            "3:4",
            ParsedDateTime {
                hour: Some(DateTimeFieldValue::new(3, 0)),
                minute: Some(DateTimeFieldValue::new(4, 0)),
                ..Default::default()
            },
        );
        run_test_build_parsed_datetime_time(
            "3:4.5",
            ParsedDateTime {
                minute: Some(DateTimeFieldValue::new(3, 0)),
                second: Some(DateTimeFieldValue::new(4, 500_000_000)),
                ..Default::default()
            },
        );
        run_test_build_parsed_datetime_time(
            "0::4.5",
            ParsedDateTime {
                hour: Some(DateTimeFieldValue::new(0, 0)),
                second: Some(DateTimeFieldValue::new(4, 500_000_000)),
                ..Default::default()
            },
        );
        run_test_build_parsed_datetime_time(
            "0::.5",
            ParsedDateTime {
                hour: Some(DateTimeFieldValue::new(0, 0)),
                second: Some(DateTimeFieldValue::new(0, 500_000_000)),
                ..Default::default()
            },
        );

        fn run_test_build_parsed_datetime_time(test: &str, res: ParsedDateTime) {
            assert_eq!(
                ParsedDateTime::build_parsed_datetime_time(test).unwrap(),
                res
            );
        }
    }

    #[test]
    fn test_build_parsed_datetime_timestamp() {
        run_test_build_parsed_datetime_timestamp(
            "2000-01-02",
            ParsedDateTime {
                year: Some(DateTimeFieldValue::new(2000, 0)),
                month: Some(DateTimeFieldValue::new(1, 0)),
                day: Some(DateTimeFieldValue::new(2, 0)),
                ..Default::default()
            },
        );
        run_test_build_parsed_datetime_timestamp(
            "2000",
            ParsedDateTime {
                year: Some(DateTimeFieldValue::new(2000, 0)),
                ..Default::default()
            },
        );
        run_test_build_parsed_datetime_timestamp(
            "2000-1-",
            ParsedDateTime {
                year: Some(DateTimeFieldValue::new(2000, 0)),
                month: Some(DateTimeFieldValue::new(1, 0)),
                ..Default::default()
            },
        );
        run_test_build_parsed_datetime_timestamp(
            "2000-01-02 3:4:5.6",
            ParsedDateTime {
                year: Some(DateTimeFieldValue::new(2000, 0)),
                month: Some(DateTimeFieldValue::new(1, 0)),
                day: Some(DateTimeFieldValue::new(2, 0)),
                hour: Some(DateTimeFieldValue::new(3, 0)),
                minute: Some(DateTimeFieldValue::new(4, 0)),
                second: Some(DateTimeFieldValue::new(5, 600_000_000)),
                ..Default::default()
            },
        );
        run_test_build_parsed_datetime_timestamp(
            "2000-01-02T3:4:5.6",
            ParsedDateTime {
                year: Some(DateTimeFieldValue::new(2000, 0)),
                month: Some(DateTimeFieldValue::new(1, 0)),
                day: Some(DateTimeFieldValue::new(2, 0)),
                hour: Some(DateTimeFieldValue::new(3, 0)),
                minute: Some(DateTimeFieldValue::new(4, 0)),
                second: Some(DateTimeFieldValue::new(5, 600_000_000)),
                ..Default::default()
            },
        );

        fn run_test_build_parsed_datetime_timestamp(test: &str, res: ParsedDateTime) {
            assert_eq!(
                ParsedDateTime::build_parsed_datetime_timestamp(test).unwrap(),
                res
            );
        }
    }

    #[test]
    fn test_build_parsed_datetime_interval() {
        use DateTimeField::*;
        let test_cases = [
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 700_000_000)),
                    ..Default::default()
                },
                "1-2 3 4:5:6.7",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 700_000_000)),
                    ..Default::default()
                },
                "1 year 2 months 3 days 4 hours 5 minutes 6.7 seconds",
                Second,
            ),
            (
                ParsedDateTime {
                    second: Some(DateTimeFieldValue::new(1, 0)),
                    ..Default::default()
                },
                "1",
                Second,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(1, 0)),
                    ..Default::default()
                },
                "1",
                Day,
            ),
            (
                ParsedDateTime {
                    month: Some(DateTimeFieldValue::new(1, 0)),
                    ..Default::default()
                },
                "1",
                Month,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(1, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "1:",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "1-",
                Second,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(1, 0)),
                    hour: Some(DateTimeFieldValue::new(2, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "1 2:",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(4, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "1-2 3:4",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(0, 0)),
                    day: Some(DateTimeFieldValue::new(2, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "1- 2 3:",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(5, 0)),
                    month: Some(DateTimeFieldValue::new(6, 0)),
                    hour: Some(DateTimeFieldValue::new(1, 0)),
                    minute: Some(DateTimeFieldValue::new(2, 0)),
                    second: Some(DateTimeFieldValue::new(3, 400_000_000)),
                    ..Default::default()
                },
                "1:2:3.4 5-6",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    ..Default::default()
                },
                "1-2 3",
                Hour,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(-1, 0)),
                    month: Some(DateTimeFieldValue::new(-2, 0)),
                    day: Some(DateTimeFieldValue::new(-3, 0)),
                    hour: Some(DateTimeFieldValue::new(-4, 0)),
                    minute: Some(DateTimeFieldValue::new(-5, 0)),
                    second: Some(DateTimeFieldValue::new(-6, -700_000_000)),
                    ..Default::default()
                },
                "-1-2 -3 -4:5:6.7",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(-1, 0)),
                    month: Some(DateTimeFieldValue::new(-2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(-4, 0)),
                    minute: Some(DateTimeFieldValue::new(-5, 0)),
                    second: Some(DateTimeFieldValue::new(-6, -700_000_000)),
                    ..Default::default()
                },
                "-1-2 3 -4:5:6.7",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(-1, 0)),
                    month: Some(DateTimeFieldValue::new(-2, 0)),
                    day: Some(DateTimeFieldValue::new(-3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(0, 500_000_000)),
                    ..Default::default()
                },
                "-1-2 -3 4::.5",
                Second,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(0, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(-1, -270_000_000)),
                    ..Default::default()
                },
                "-::1.27",
                Second,
            ),
            (
                ParsedDateTime {
                    second: Some(DateTimeFieldValue::new(1, 270_000_000)),
                    ..Default::default()
                },
                ":::::1.27",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(0, 0)),
                    day: Some(DateTimeFieldValue::new(2, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                ":::1- ::2 ::::3:",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 700_000_000)),
                    ..Default::default()
                },
                "1 years 2 months 3 days 4 hours 5 minutes 6.7 seconds",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 700_000_000)),
                    ..Default::default()
                },
                "1y 2mon 3d 4h 5m 6.7s",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 700_000_000)),
                    ..Default::default()
                },
                "6.7 seconds 5 minutes 3 days 4 hours 1 year 2 month",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(-1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(-3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(-6, -700_000_000)),
                    ..Default::default()
                },
                "-6.7 seconds 5 minutes -3 days 4 hours -1 year 2 month",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    day: Some(DateTimeFieldValue::new(4, 500_000_000)),
                    ..Default::default()
                },
                "1y 2.3mon 4.5d",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(-1, -200_000_000)),
                    month: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    day: Some(DateTimeFieldValue::new(-3, -400_000_000)),
                    hour: Some(DateTimeFieldValue::new(4, 500_000_000)),
                    minute: Some(DateTimeFieldValue::new(5, 600_000_000)),
                    second: Some(DateTimeFieldValue::new(-6, -700_000_000)),
                    ..Default::default()
                },
                "-6.7 seconds 5.6 minutes -3.4 days 4.5 hours -1.2 year 2.3 month",
                Second,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(1, 0)),
                    second: Some(DateTimeFieldValue::new(0, -270_000_000)),
                    ..Default::default()
                },
                "1 day -0.27 seconds",
                Second,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(-1, 0)),
                    second: Some(DateTimeFieldValue::new(0, 270_000_000)),
                    ..Default::default()
                },
                "-1 day 0.27 seconds",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(10, 333_000_000)),
                    ..Default::default()
                },
                "10.333 years",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(10, 333_000_000)),
                    ..Default::default()
                },
                "10.333",
                Year,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(5, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(4, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "1-2 3:4 5 day",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(5, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(4, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "5 day 3:4 1-2",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(5, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(4, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "1-2 5 day 3:4",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    day: Some(DateTimeFieldValue::new(2, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(4, 0)),
                    second: Some(DateTimeFieldValue::new(5, 600_000_000)),
                    ..Default::default()
                },
                "+1 year +2 days +3:4:5.6",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "1-2",
                Month,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "1-2",
                Minute,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(1, 999_999_999)),
                    ..Default::default()
                },
                "1.999999999999999999 days",
                Second,
            ),
            (
                ParsedDateTime {
                    millisecond: Some(DateTimeFieldValue::new(1, 200_000_000)),
                    ..Default::default()
                },
                "1.2ms",
                Second,
            ),
            (
                ParsedDateTime {
                    millisecond: Some(DateTimeFieldValue::new(1, 0)),
                    ..Default::default()
                },
                "1ms",
                Second,
            ),
            (
                ParsedDateTime {
                    millisecond: Some(DateTimeFieldValue::new(2100, 0)),
                    ..Default::default()
                },
                "2100ms",
                Second,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(1, 0)),
                    millisecond: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "1h 2ms",
                Second,
            ),
            (
                ParsedDateTime {
                    millisecond: Some(DateTimeFieldValue::new(42, 900_000_000)),
                    ..Default::default()
                },
                "42.9 milliseconds",
                Second,
            ),
            (
                ParsedDateTime {
                    second: Some(DateTimeFieldValue::new(5, 0)),
                    millisecond: Some(DateTimeFieldValue::new(37, 660_000_000)),
                    ..Default::default()
                },
                "5.0 seconds 37.66 milliseconds",
                Second,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(14, 0)),
                    millisecond: Some(DateTimeFieldValue::new(60, 0)),
                    ..Default::default()
                },
                "14 days 60 ms",
                Second,
            ),
            (
                ParsedDateTime {
                    microsecond: Some(DateTimeFieldValue::new(42, 900_000_000)),
                    ..Default::default()
                },
                "42.9 microseconds",
                Second,
            ),
            (
                ParsedDateTime {
                    second: Some(DateTimeFieldValue::new(5, 0)),
                    microsecond: Some(DateTimeFieldValue::new(37, 660_000_000)),
                    ..Default::default()
                },
                "5.0 seconds 37.66 microseconds",
                Second,
            ),
            (
                ParsedDateTime {
                    millennium: Some(DateTimeFieldValue::new(9, 800_000_000)),
                    ..Default::default()
                },
                "9.8 millenniums",
                Second,
            ),
            (
                ParsedDateTime {
                    century: Some(DateTimeFieldValue::new(7, 600_000_000)),
                    ..Default::default()
                },
                "7.6 centuries",
                Second,
            ),
            (
                ParsedDateTime {
                    decade: Some(DateTimeFieldValue::new(5, 400_000_000)),
                    ..Default::default()
                },
                "5.4 decades",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 200_000_000)),
                    decade: Some(DateTimeFieldValue::new(4, 300_000_000)),
                    century: Some(DateTimeFieldValue::new(5, 600_000_000)),
                    millennium: Some(DateTimeFieldValue::new(8, 700_000_000)),
                    ..Default::default()
                },
                "8.7 mils 5.6 cent 4.3 decs 1.2 y",
                Second,
            ),
        ];

        for test in test_cases.iter() {
            let actual =
                ParsedDateTime::build_parsed_datetime_interval(test.1, None, test.2).unwrap();
            if actual != test.0 {
                panic!(
                    "In test INTERVAL '{}' {}\n actual: {:?} \n expected: {:?}",
                    test.1, test.2, actual, test.0
                );
            }
        }
    }

    #[test]
    fn test_build_parsed_datetime_interval_errors() {
        use DateTimeField::*;
        let test_cases = [
            (
                "1 year 2 years",
                Second,
                "YEAR field set twice",
            ),
            (
                "1-2 3-4",
                Second,
                "YEAR or MONTH field set twice",
            ),
            (
                "1-2 3 year",
                Second,
                "YEAR field set twice",
            ),
            (
                "1-2 3",
                Month,
                "MONTH field set twice",
            ),
            (
                "1-2 3:4 5",
                Second,
                "SECOND field set twice",
            ),
            (
                "1:2:3.4 5-6 7",
                Year,
                "YEAR field set twice",
            ),
            (
                "-:::::1.27",
                Second,
                "have unprocessed tokens 1.270000000",
            ),
            (
                "-1 ::.27",
                Second,
                "Cannot determine format of all parts. Add explicit time components, e.g. \
                INTERVAL '1 day' or INTERVAL '1' DAY",
            ),
            (
                "1:2:3.4.5",
                Second,
                "have unprocessed tokens .500000000",
            ),
            (
                "1+2:3.4",
                Second,
                "Cannot determine format of all parts",
            ),
            (
                "1x2:3.4",
                Second,
                "unknown units x",
            ),
            (
                "0 foo",
                Second,
                "unknown units foo",
            ),
            (
                "1-2 3:4 5 second",
                Second,
                "SECOND field set twice",
            ),
            (
                "1-2 5 second 3:4",
                Second,
                "HOUR, MINUTE, SECOND field set twice",
            ),
            (
                "1 2-3 4:5",
                Day,
                "Cannot determine format of all parts. Add explicit time components, e.g. \
                INTERVAL '1 day' or INTERVAL '1' DAY",
            ),
            (
                "9223372036854775808 months",
                Day,
                "Unable to parse value as a number at index 19: number too large to fit in target type",
            ),
            (
                "-9223372036854775808 months",
                Day,
                "Unable to parse value as a number at index 20: number too large to fit in target type",
            ),
            (
                "9223372036854775808 seconds",
                Day,
                "Unable to parse value as a number at index 19: number too large to fit in target type",
            ),
            (
                "-9223372036854775808 seconds",
                Day,
                "Unable to parse value as a number at index 20: number too large to fit in target type",
            ),
            (
                "1.234 second 5 ms",
                Second,
                "Cannot set MILLISECONDS or MICROSECONDS field if SECOND field has a fraction component",
            ),
            (
                "1.234 second 5 us",
                Second,
                "Cannot set MILLISECONDS or MICROSECONDS field if SECOND field has a fraction component",
            ),
            (
                "7 ms 4.321 second",
                Second,
                "Cannot set MILLISECONDS or MICROSECONDS field if SECOND field has a fraction component",
            ),
            (
                "7 us 4.321 second",
                Second,
                "Cannot set MILLISECONDS or MICROSECONDS field if SECOND field has a fraction component",
            ),
        ];
        for test in test_cases.iter() {
            match ParsedDateTime::build_parsed_datetime_interval(test.0, None, test.1) {
                Err(e) => assert_eq!(e.to_string(), test.2),
                Ok(pdt) => panic!(
                    "Test INTERVAL '{}' {} passed when expected to fail with {}, generated ParsedDateTime {:?}",
                    test.0,
                    test.1,
                    test.2,
                    pdt,
                ),
            }
        }
    }

    #[test]
    fn test_split_timestamp_string() {
        let test_cases = [
            (
                "1969-06-01 10:10:10.410 UTC",
                "1969-06-01 10:10:10.410",
                "UTC",
            ),
            (
                "1969-06-01 10:10:10.410+4:00",
                "1969-06-01 10:10:10.410",
                "+4:00",
            ),
            (
                "1969-06-01 10:10:10.410-4:00",
                "1969-06-01 10:10:10.410",
                "-4:00",
            ),
            ("1969-06-01 10:10:10.410", "1969-06-01 10:10:10.410", ""),
            ("1969-06-01 10:10:10.410+4", "1969-06-01 10:10:10.410", "+4"),
            ("1969-06-01 10:10:10.410-4", "1969-06-01 10:10:10.410", "-4"),
            ("1969-06-01 10:10:10+4:00", "1969-06-01 10:10:10", "+4:00"),
            ("1969-06-01 10:10:10-4:00", "1969-06-01 10:10:10", "-4:00"),
            ("1969-06-01 10:10:10 UTC", "1969-06-01 10:10:10", "UTC"),
            ("1969-06-01 10:10:10", "1969-06-01 10:10:10", ""),
            ("1969-06-01 10:10+4:00", "1969-06-01 10:10", "+4:00"),
            ("1969-06-01 10:10-4:00", "1969-06-01 10:10", "-4:00"),
            ("1969-06-01 10:10 UTC", "1969-06-01 10:10", "UTC"),
            ("1969-06-01 10:10", "1969-06-01 10:10", ""),
            ("1969-06-01 UTC", "1969-06-01", "UTC"),
            ("1969-06-01 +4:00", "1969-06-01", "+4:00"),
            ("1969-06-01 -4:00", "1969-06-01", "-4:00"),
            ("1969-06-01 +4", "1969-06-01", "+4"),
            ("1969-06-01 -4", "1969-06-01", "-4"),
            ("1969-06-01", "1969-06-01", ""),
            ("1969-06-01 10:10:10.410Z", "1969-06-01 10:10:10.410", "Z"),
            ("1969-06-01 10:10:10.410z", "1969-06-01 10:10:10.410", "z"),
            ("1969-06-01Z", "1969-06-01", "Z"),
            ("1969-06-01z", "1969-06-01", "z"),
            ("1969-06-01 10:10:10.410   ", "1969-06-01 10:10:10.410", ""),
            (
                "1969-06-01     10:10:10.410   ",
                "1969-06-01     10:10:10.410",
                "",
            ),
            ("   1969-06-01 10:10:10.412", "1969-06-01 10:10:10.412", ""),
            (
                "   1969-06-01 10:10:10.413   ",
                "1969-06-01 10:10:10.413",
                "",
            ),
            (
                "1969-06-01 10:10:10.410 +4:00",
                "1969-06-01 10:10:10.410",
                "+4:00",
            ),
            (
                "1969-06-01 10:10:10.410+4 :00",
                "1969-06-01 10:10:10.410",
                "+4 :00",
            ),
            (
                "1969-06-01 10:10:10.410      +4:00",
                "1969-06-01 10:10:10.410",
                "+4:00",
            ),
            (
                "1969-06-01 10:10:10.410+4:00     ",
                "1969-06-01 10:10:10.410",
                "+4:00",
            ),
            (
                "1969-06-01 10:10:10.410  Z  ",
                "1969-06-01 10:10:10.410",
                "Z",
            ),
            ("1969-06-01    +4  ", "1969-06-01", "+4"),
            ("1969-06-01   Z   ", "1969-06-01", "Z"),
        ];

        for test in test_cases.iter() {
            let (ts, tz) = split_timestamp_string(test.0);

            assert_eq!(ts, test.1);
            assert_eq!(tz, test.2);
        }
    }

    #[test]
    fn test_parse_timezone_offset_second() {
        use Timezone::{FixedOffset as F, Tz as T};
        let test_cases = [
            ("+0:00", F(FixedOffset::east(0))),
            ("-0:00", F(FixedOffset::east(0))),
            ("+0:000000", F(FixedOffset::east(0))),
            ("+000000:00", F(FixedOffset::east(0))),
            ("+000000:000000", F(FixedOffset::east(0))),
            ("+0", F(FixedOffset::east(0))),
            ("+00", F(FixedOffset::east(0))),
            ("+000", F(FixedOffset::east(0))),
            ("+0000", F(FixedOffset::east(0))),
            ("+00000000", F(FixedOffset::east(0))),
            ("+0000001:000000", F(FixedOffset::east(3600))),
            ("+0000000:000001", F(FixedOffset::east(60))),
            ("+0000001:000001", F(FixedOffset::east(3660))),
            ("+0000001:000001:000001", F(FixedOffset::east(3661))),
            ("+4:00", F(FixedOffset::east(14400))),
            ("-4:00", F(FixedOffset::west(14400))),
            ("+2:30", F(FixedOffset::east(9000))),
            ("-5:15", F(FixedOffset::west(18900))),
            ("+0:20", F(FixedOffset::east(1200))),
            ("-0:20", F(FixedOffset::west(1200))),
            ("+0:0:20", F(FixedOffset::east(20))),
            ("+5", F(FixedOffset::east(18000))),
            ("-5", F(FixedOffset::west(18000))),
            ("+05", F(FixedOffset::east(18000))),
            ("-05", F(FixedOffset::west(18000))),
            ("+500", F(FixedOffset::east(18000))),
            ("-500", F(FixedOffset::west(18000))),
            ("+530", F(FixedOffset::east(19800))),
            ("-530", F(FixedOffset::west(19800))),
            ("+050", F(FixedOffset::east(3000))),
            ("-050", F(FixedOffset::west(3000))),
            ("+15", F(FixedOffset::east(54000))),
            ("-15", F(FixedOffset::west(54000))),
            ("+1515", F(FixedOffset::east(54900))),
            ("+15:15:15", F(FixedOffset::east(54915))),
            ("+015", F(FixedOffset::east(900))),
            ("-015", F(FixedOffset::west(900))),
            ("+0015", F(FixedOffset::east(900))),
            ("-0015", F(FixedOffset::west(900))),
            ("+00015", F(FixedOffset::east(900))),
            ("-00015", F(FixedOffset::west(900))),
            ("+005", F(FixedOffset::east(300))),
            ("-005", F(FixedOffset::west(300))),
            ("+0000005", F(FixedOffset::east(300))),
            ("+00000100", F(FixedOffset::east(3600))),
            ("Z", F(FixedOffset::east(0))),
            ("z", F(FixedOffset::east(0))),
            ("UTC", T(Tz::UTC)),
            ("Pacific/Auckland", T(Tz::Pacific__Auckland)),
            ("America/New_York", T(Tz::America__New_York)),
            ("America/Los_Angeles", T(Tz::America__Los_Angeles)),
            ("utc", T(Tz::UTC)),
            ("pAcIfIc/AUcKlAnD", T(Tz::Pacific__Auckland)),
            ("AMERICA/NEW_YORK", T(Tz::America__New_York)),
            ("america/los_angeles", T(Tz::America__Los_Angeles)),
        ];

        for (timezone, expected) in test_cases.iter() {
            match timezone.parse::<Timezone>() {
                Ok(tz) => assert_eq!(&tz, expected),
                Err(e) => panic!(
                    "Test failed when expected to pass test case: {} error: {}",
                    timezone, e
                ),
            }
        }

        let failure_test_cases = [
            "+25:00", "+120:00", "+0:61", "+0:500", " 12:30", "+-12:30", "+2525", "+2561",
            "+255900", "+25", "+5::30", "+5:30:", "+5:", "++5:00", "--5:00", " UTC", "a", "zzz",
            "ZZZ", "ZZ Top", " +", " -", " ", "1", "12", "1234", "+16", "-17", "-14:60", "1:30:60",
        ];

        for test in failure_test_cases.iter() {
            match test.parse::<Timezone>() {
                Ok(t) => panic!("Test passed when expected to fail test case: {} parsed tz offset (seconds): {}", test, t),
                Err(e) => println!("{}", e),
            }
        }
    }

    proptest! {
        #[test]
        fn datetimeunits_serialization_roundtrip(expect in any::<DateTimeUnits>() ) {
            let actual = protobuf_roundtrip::<_, ProtoDateTimeUnits>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}

#[test]
fn test_parseddatetime_add_field() {
    use DateTimeField::*;
    let pdt_unit = ParsedDateTime {
        millennium: Some(DateTimeFieldValue::new(8, 0)),
        century: Some(DateTimeFieldValue::new(9, 0)),
        decade: Some(DateTimeFieldValue::new(10, 0)),
        year: Some(DateTimeFieldValue::new(1, 0)),
        month: Some(DateTimeFieldValue::new(2, 0)),
        day: Some(DateTimeFieldValue::new(2, 0)),
        hour: Some(DateTimeFieldValue::new(3, 0)),
        minute: Some(DateTimeFieldValue::new(4, 0)),
        second: Some(DateTimeFieldValue::new(5, 0)),
        millisecond: Some(DateTimeFieldValue::new(6, 0)),
        microsecond: Some(DateTimeFieldValue::new(7, 0)),
        ..Default::default()
    };

    let pdt_frac = ParsedDateTime {
        millennium: Some(DateTimeFieldValue::new(8, 555_555_555)),
        century: Some(DateTimeFieldValue::new(9, 555_555_555)),
        decade: Some(DateTimeFieldValue::new(10, 555_555_555)),
        year: Some(DateTimeFieldValue::new(1, 555_555_555)),
        month: Some(DateTimeFieldValue::new(2, 555_555_555)),
        day: Some(DateTimeFieldValue::new(2, 555_555_555)),
        hour: Some(DateTimeFieldValue::new(3, 555_555_555)),
        minute: Some(DateTimeFieldValue::new(4, 555_555_555)),
        second: Some(DateTimeFieldValue::new(5, 555_555_555)),
        millisecond: Some(DateTimeFieldValue::new(6, 555_555_555)),
        microsecond: Some(DateTimeFieldValue::new(7, 555_555_555)),
        ..Default::default()
    };

    let pdt_frac_neg = ParsedDateTime {
        millennium: Some(DateTimeFieldValue::new(-8, -555_555_555)),
        century: Some(DateTimeFieldValue::new(-9, -555_555_555)),
        decade: Some(DateTimeFieldValue::new(-10, -555_555_555)),
        year: Some(DateTimeFieldValue::new(-1, -555_555_555)),
        month: Some(DateTimeFieldValue::new(-2, -555_555_555)),
        day: Some(DateTimeFieldValue::new(-2, -555_555_555)),
        hour: Some(DateTimeFieldValue::new(-3, -555_555_555)),
        minute: Some(DateTimeFieldValue::new(-4, -555_555_555)),
        second: Some(DateTimeFieldValue::new(-5, -555_555_555)),
        millisecond: Some(DateTimeFieldValue::new(-6, -555_555_555)),
        microsecond: Some(DateTimeFieldValue::new(-7, -555_555_555)),
        ..Default::default()
    };

    let pdt_s_rollover = ParsedDateTime {
        millisecond: Some(DateTimeFieldValue::new(1002, 666_666_666)),
        microsecond: Some(DateTimeFieldValue::new(1000003, 777_777_777)),
        ..Default::default()
    };

    run_test_parseddatetime_add_field(pdt_unit.clone(), Millennium, (8 * 12 * 1_000, 0, 0));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Century, (9 * 12 * 100, 0, 0));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Decade, (10 * 12 * 10, 0, 0));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Year, (12, 0, 0));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Month, (2, 0, 0));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Day, (0, 2, 0));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Hour, (0, 0, 3 * 60 * 60 * 1_000_000));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Minute, (0, 0, 4 * 60 * 1_000_000));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Second, (0, 0, 5 * 1_000_000));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Milliseconds, (0, 0, 6 * 1_000));
    run_test_parseddatetime_add_field(pdt_unit, Microseconds, (0, 0, 7));
    run_test_parseddatetime_add_field(pdt_frac.clone(), Millennium, (102_666, 0, 0));
    run_test_parseddatetime_add_field(pdt_frac.clone(), Century, (11466, 0, 0));
    run_test_parseddatetime_add_field(pdt_frac.clone(), Decade, (1266, 0, 0));
    run_test_parseddatetime_add_field(pdt_frac.clone(), Year, (18, 0, 0));
    run_test_parseddatetime_add_field(
        pdt_frac.clone(),
        Month,
        (
            2,
            16,
            // 15:59:59.99856
            (15 * 60 * 60 * 1_000_000) + (59 * 60 * 1_000_000) + (59 * 1_000_000) + 998_560,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac.clone(),
        Day,
        (
            0,
            2,
            // 13:19:59.999952
            (13 * 60 * 60 * 1_000_000) + (19 * 60 * 1_000_000) + (59 * 1_000_000) + 999_952,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac.clone(),
        Hour,
        (
            0,
            0,
            // 03:33:19.999998
            (3 * 60 * 60 * 1_000_000) + (33 * 60 * 1_000_000) + (19 * 1_000_000) + 999_998,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac.clone(),
        Minute,
        (
            0,
            0,
            // 00:04:33.333333
            (4 * 60 * 1_000_000) + (33 * 1_000_000) + 333_333,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac.clone(),
        Second,
        (
            0,
            0,
            // 00:00:05.555556
            (5 * 1_000_000) + 555_556,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac.clone(),
        Milliseconds,
        (
            0, 0, // 00:00:00.006556
            6_556,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac,
        Microseconds,
        (
            0, 0, // 00:00:00.000008
            8,
        ),
    );
    run_test_parseddatetime_add_field(pdt_frac_neg.clone(), Year, (-18, 0, 0));
    run_test_parseddatetime_add_field(
        pdt_frac_neg.clone(),
        Month,
        (
            -2,
            -16,
            // -15:59:59.99856
            (-15 * 60 * 60 * 1_000_000) + (-59 * 60 * 1_000_000) + (-59 * 1_000_000) + -998_560,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac_neg.clone(),
        Day,
        (
            0,
            -2,
            // 13:19:59.999952
            (-13 * 60 * 60 * 1_000_000) + (-19 * 60 * 1_000_000) + (-59 * 1_000_000) + -999_952,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac_neg.clone(),
        Hour,
        (
            0,
            0,
            // -03:33:19.999998
            (-3 * 60 * 60 * 1_000_000) + (-33 * 60 * 1_000_000) + (-19 * 1_000_000) + -999_998,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac_neg.clone(),
        Minute,
        (
            0,
            0,
            // -00:04:33.333333
            (-4 * 60 * 1_000_000) + (-33 * 1_000_000) + -333_333,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac_neg.clone(),
        Second,
        (
            0,
            0,
            // -00:00:05.555556
            (-5 * 1_000_000) + -555_556,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac_neg.clone(),
        Milliseconds,
        (
            0, 0, // -00:00:00.006556
            -6_556,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac_neg,
        Microseconds,
        (
            0, 0, // -00:00:00.000008
            -8,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_s_rollover.clone(),
        Milliseconds,
        (
            0,
            0, // 00:00:01.002667
            (1 * 1_000_000) + 2_667,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_s_rollover,
        Microseconds,
        (
            0,
            0, // 00:00:01.000004
            (1 * 1_000_000) + 4,
        ),
    );

    fn run_test_parseddatetime_add_field(
        pdt: ParsedDateTime,
        f: DateTimeField,
        expected: (i32, i32, i64),
    ) {
        let mut res = (0, 0, 0);

        pdt.add_field(f, &mut res.0, &mut res.1, &mut res.2)
            .unwrap();

        if res.0 != expected.0 || res.1 != expected.1 || res.2 != expected.2 {
            panic!(
                "test_parseddatetime_add_field failed \n actual: {:?} \n expected: {:?}",
                res, expected
            );
        }
    }
}

#[test]
fn test_parseddatetime_compute_interval() {
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            year: Some(DateTimeFieldValue::new(1, 0)),
            month: Some(DateTimeFieldValue::new(1, 0)),
            ..Default::default()
        },
        Interval {
            months: 13,
            ..Default::default()
        },
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            year: Some(DateTimeFieldValue::new(1, 0)),
            month: Some(DateTimeFieldValue::new(-1, 0)),
            ..Default::default()
        },
        Interval {
            months: 11,
            ..Default::default()
        },
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            year: Some(DateTimeFieldValue::new(-1, 0)),
            month: Some(DateTimeFieldValue::new(1, 0)),
            ..Default::default()
        },
        Interval {
            months: -11,
            ..Default::default()
        },
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            day: Some(DateTimeFieldValue::new(1, 0)),
            hour: Some(DateTimeFieldValue::new(-2, 0)),
            minute: Some(DateTimeFieldValue::new(-3, 0)),
            second: Some(DateTimeFieldValue::new(-4, -500_000_000)),
            ..Default::default()
        },
        // 1 day -2:03:04.5
        Interval::new(
            0,
            1,
            (-2 * 60 * 60 * 1_000_000) + (-3 * 60 * 1_000_000) + (-4 * 1_000_000) + -500_000,
        )
        .unwrap(),
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            day: Some(DateTimeFieldValue::new(-1, 0)),
            hour: Some(DateTimeFieldValue::new(2, 0)),
            minute: Some(DateTimeFieldValue::new(3, 0)),
            second: Some(DateTimeFieldValue::new(4, 500_000_000)),
            ..Default::default()
        },
        // -1 day 02:03:04.5
        Interval::new(
            0,
            -1,
            (2 * 60 * 60 * 1_000_000) + (3 * 60 * 1_000_000) + (4 * 1_000_000) + 500_000,
        )
        .unwrap(),
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            day: Some(DateTimeFieldValue::new(1, 0)),
            second: Some(DateTimeFieldValue::new(0, -270_000_000)),
            ..Default::default()
        },
        // 1 day -00:00:00.27
        Interval::new(0, 1, -270_000).unwrap(),
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            day: Some(DateTimeFieldValue::new(-1, 0)),
            second: Some(DateTimeFieldValue::new(0, 270_000_000)),
            ..Default::default()
        },
        // -1 day 00:00:00.27
        Interval::new(0, -1, 270_000).unwrap(),
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            year: Some(DateTimeFieldValue::new(-1, -555_555_555)),
            month: Some(DateTimeFieldValue::new(2, 555_555_555)),
            day: Some(DateTimeFieldValue::new(-3, -555_555_555)),
            hour: Some(DateTimeFieldValue::new(4, 555_555_555)),
            minute: Some(DateTimeFieldValue::new(-5, -555_555_555)),
            second: Some(DateTimeFieldValue::new(6, 555_555_555)),
            ..Default::default()
        },
        // -1 year -4 months +13 days +07:07:53.220829
        Interval::new(
            -16,
            13,
            (7 * 60 * 60 * 1_000_000) + (7 * 60 * 1_000_000) + (53 * 1_000_000) + 220_829,
        )
        .unwrap(),
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            second: Some(DateTimeFieldValue::new(1, 0)),
            millisecond: Some(DateTimeFieldValue::new(2_003, 0)),
            ..Default::default()
        },
        // 00:00:03.003
        Interval::new(0, 0, (3 * 1_000_000) + 3_000).unwrap(),
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            second: Some(DateTimeFieldValue::new(1, 0)),
            microsecond: Some(DateTimeFieldValue::new(2_000_003, 0)),
            ..Default::default()
        },
        // 00:00:03.000003
        Interval::new(0, 0, (3 * 1_000_000) + 3).unwrap(),
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            millisecond: Some(DateTimeFieldValue::new(1, 200_000_000)),
            microsecond: Some(DateTimeFieldValue::new(3, 400_000_000)),
            ..Default::default()
        },
        // 00:00:00.0012034
        Interval::new(0, 0, 1_203).unwrap(),
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            millennium: Some(DateTimeFieldValue::new(1, 0)),
            century: Some(DateTimeFieldValue::new(2, 0)),
            decade: Some(DateTimeFieldValue::new(3, 0)),
            year: Some(DateTimeFieldValue::new(4, 0)),
            ..Default::default()
        },
        // 1234 years
        Interval::new(1234 * 12, 0, 0).unwrap(),
    );

    fn run_test_parseddatetime_compute_interval(pdt: ParsedDateTime, expected: Interval) {
        let actual = pdt.compute_interval().unwrap();

        if actual != expected {
            panic!(
                "test_interval_compute_interval failed\n input {:?}\nactual {:?}\nexpected {:?}",
                pdt, actual, expected
            )
        }
    }
}
