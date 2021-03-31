// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Date and time utilities.

#![allow(missing_docs)]

use std::collections::VecDeque;
use std::convert::TryInto;
use std::fmt;
use std::str::FromStr;

use chrono::{FixedOffset, NaiveDate, NaiveTime};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

use crate::adt::interval::Interval;
use std::cmp::Ordering;

/// Units of measurements associated with dates and times.
///
/// TODO(benesch): with enough thinking, this type could probably be merged with
/// `DateTimeField`.
#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
            Self::Second => f.write_str(""),
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

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub enum DateTimeField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl fmt::Display for DateTimeField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            DateTimeField::Year => "YEAR",
            DateTimeField::Month => "MONTH",
            DateTimeField::Day => "DAY",
            DateTimeField::Hour => "HOUR",
            DateTimeField::Minute => "MINUTE",
            DateTimeField::Second => "SECOND",
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
            "YEAR" | "YEARS" | "Y" => Ok(Self::Year),
            "MONTH" | "MONTHS" | "MON" | "MONS" => Ok(Self::Month),
            "DAY" | "DAYS" | "D" => Ok(Self::Day),
            "HOUR" | "HOURS" | "H" => Ok(Self::Hour),
            "MINUTE" | "MINUTES" | "M" => Ok(Self::Minute),
            "SECOND" | "SECONDS" | "S" => Ok(Self::Second),
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

    /// Returns the number of seconds in a single unit of `field`.
    ///
    /// # Panics
    ///
    /// Panics if called on a non-duration field.
    pub fn seconds_multiplier(self) -> i64 {
        use DateTimeField::*;
        match self {
            Day => 60 * 60 * 24,
            Hour => 60 * 60,
            Minute => 60,
            Second => 1,
            _other => unreachable!("Do not call with a non-duration field"),
        }
    }

    /// Returns the number of nanoseconds in a single unit of `field`.
    ///
    /// # Panics
    ///
    /// Panics if called on a non-duration field.
    pub fn nanos_multiplier(self) -> i64 {
        self.seconds_multiplier() * 1_000_000_000
    }
}

/// An iterator over DateTimeFields
///
/// Always starts with the value smaller than the current one.
///
/// ```
/// use repr::adt::datetime::DateTimeField::*;
/// let mut itr = Hour.into_iter();
/// assert_eq!(itr.next(), Some(Minute));
/// assert_eq!(itr.next(), Some(Second));
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
            Some(Year) => Some(Month),
            Some(Month) => Some(Day),
            Some(Day) => Some(Hour),
            Some(Hour) => Some(Minute),
            Some(Minute) => Some(Second),
            Some(Second) => None,
            None => None,
        };
        self.0.clone()
    }
}

impl DoubleEndedIterator for DateTimeFieldIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        use DateTimeField::*;
        self.0 = match self.0 {
            Some(Year) => None,
            Some(Month) => Some(Year),
            Some(Day) => Some(Month),
            Some(Hour) => Some(Day),
            Some(Minute) => Some(Hour),
            Some(Second) => Some(Minute),
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
}

/// Parsed timezone.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Timezone {
    #[serde(with = "fixed_offset_serde")]
    FixedOffset(FixedOffset),
    Tz(Tz),
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
    pub year: Option<DateTimeFieldValue>,
    pub month: Option<DateTimeFieldValue>,
    pub day: Option<DateTimeFieldValue>,
    pub hour: Option<DateTimeFieldValue>,
    pub minute: Option<DateTimeFieldValue>,
    // second.fraction is equivalent to nanoseconds.
    pub second: Option<DateTimeFieldValue>,
    pub timezone_offset_second: Option<Timezone>,
}

impl Default for ParsedDateTime {
    fn default() -> Self {
        ParsedDateTime {
            year: None,
            month: None,
            day: None,
            hour: None,
            minute: None,
            second: None,
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
        let mut months = 0i64;
        let mut seconds = 0i64;
        let mut nanos = 0i64;

        // Add all DateTimeFields, from Year to Seconds.
        self.add_field(Year, &mut months, &mut seconds, &mut nanos)?;

        for field in Year.into_iter().take_while(|f| *f <= Second) {
            self.add_field(field, &mut months, &mut seconds, &mut nanos)?;
        }

        let months: i32 = match months.try_into() {
            Ok(m) => m,
            Err(_) => {
                return Err(format!(
                    "exceeds min/max months (+/-2147483647); have {}",
                    months
                ))
            }
        };

        match Interval::new(months, seconds, nanos) {
            Ok(i) => Ok(i),
            Err(e) => Err(e.to_string()),
        }
    }
    /// Adds the appropriate values from self's ParsedDateTime to `months`,
    /// `seconds`, and `nanos`. These fields are then appropriate to construct
    /// std::time::Duration, once accounting for their sign.
    ///
    /// # Errors
    /// - If any component overflows a parameter (i.e. i64).
    fn add_field(
        &self,
        d: DateTimeField,
        months: &mut i64,
        seconds: &mut i64,
        nanos: &mut i64,
    ) -> Result<(), String> {
        use DateTimeField::*;
        match d {
            Year => {
                let (y, y_f) = match self.units_of(Year) {
                    Some(y) => (y.unit, y.fraction),
                    None => return Ok(()),
                };
                // months += y * 12
                *months = y
                    .checked_mul(12)
                    .and_then(|y_m| months.checked_add(y_m))
                    .ok_or_else(|| {
                        format!(
                            "Overflows maximum months; \
                             cannot exceed {} months",
                            std::i64::MAX
                        )
                    })?;

                // months += y_f * 12 / 1_000_000_000
                *months = y_f
                    .checked_mul(12)
                    .and_then(|y_f_m| months.checked_add(y_f_m / 1_000_000_000))
                    .ok_or_else(|| {
                        format!(
                            "Overflows maximum months; \
                             cannot exceed {} months",
                            std::i64::MAX
                        )
                    })?;
                Ok(())
            }
            Month => {
                let (m, m_f) = match self.units_of(Month) {
                    Some(m) => (m.unit, m.fraction),
                    None => return Ok(()),
                };

                *months = m.checked_add(*months).ok_or_else(|| {
                    format!(
                        "Overflows maximum months; \
                         cannot exceed {} months",
                        std::i64::MAX
                    )
                })?;

                let m_f_ns = m_f
                    .checked_mul(30 * Day.seconds_multiplier())
                    .ok_or_else(|| "Intermediate overflow in MONTH fraction".to_owned())?;

                // seconds += m_f * 30 * seconds_multiplier(Day) / 1_000_000_000
                *seconds = seconds.checked_add(m_f_ns / 1_000_000_000).ok_or_else(|| {
                    format!(
                        "Overflows maximum seconds; \
                         cannot exceed {} seconds",
                        std::i64::MAX
                    )
                })?;

                *nanos += m_f_ns % 1_000_000_000;
                Ok(())
            }
            dhms => {
                let (t, t_f) = match self.units_of(dhms) {
                    Some(t) => (t.unit, t.fraction),
                    None => return Ok(()),
                };

                *seconds = t
                    .checked_mul(d.seconds_multiplier())
                    .and_then(|t_s| seconds.checked_add(t_s))
                    .ok_or_else(|| {
                        format!(
                            "Overflows maximum seconds; \
                             cannot exceed {} seconds",
                            std::i64::MAX
                        )
                    })?;

                let t_f_ns = t_f
                    .checked_mul(dhms.seconds_multiplier())
                    .ok_or_else(|| format!("Intermediate overflow in {} fraction", dhms))?;

                // seconds += t_f * seconds_multiplier(dhms) / 1_000_000_000
                *seconds = seconds.checked_add(t_f_ns / 1_000_000_000).ok_or_else(|| {
                    format!(
                        "Overflows maximum seconds; \
                         cannot exceed {} seconds",
                        std::i64::MAX
                    )
                })?;

                *nanos += t_f_ns % 1_000_000_000;
                Ok(())
            }
        }
    }

    /// Compute a chrono::NaiveDate from an ParsedDateTime.
    ///
    /// # Errors
    /// - If year, month, or day overflows their respective parameter in
    ///   [chrono::naive::date::NaiveDate::from_ymd_opt](https://docs.rs/chrono/0.4/chrono/naive/struct.NaiveDate.html#method.from_ymd_opt).
    pub fn compute_date(&self) -> Result<chrono::NaiveDate, String> {
        match (self.year, self.month, self.day) {
            (Some(year), Some(month), Some(day)) => {
                let p_err = |e, field| format!("{} in date is invalid: {}", field, e);
                let year = year.unit.try_into().map_err(|e| p_err(e, "Year"))?;
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
    /// * `ambiguous_resolver` identifies the DateTimeField of the final part
    ///   if it's ambiguous, e.g. in `INTERVAL '1' MONTH` '1' is ambiguous as its
    ///   DateTimeField, but MONTH resolves the ambiguity.
    pub fn build_parsed_datetime_interval(
        value: &str,
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
            let mut fmt = determine_format_w_datetimefield(&part)?;
            // If you cannot determine the format of this part, try to infer its
            // format.
            if fmt.is_none() {
                fmt = match value_parts.pop_front() {
                    Some(next_part) => {
                        match determine_format_w_datetimefield(&next_part)? {
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
                    self.second = u;
                }
                _ => return Err(format!("{} field set twice", f)),
            }
        }
        Ok(())
    }
    pub fn check_datelike_bounds(&self) -> Result<(), String> {
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

        if let Some(second) = self.second {
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
            Year | Month => {
                if let Some(month) = self.month {
                    if month.unit < -12 || month.unit > 12 {
                        return Err(format!("MONTH must be [-12, 12], got {}", month.unit));
                    };
                }
            }
            Hour | Minute | Second => {
                if let Some(minute) = self.minute {
                    if minute.unit < -59 || minute.unit > 59 {
                        return Err(format!("MINUTE must be [-59, 59], got {}", minute.unit));
                    };
                }
                if let Some(second) = self.second {
                    if second.unit < -60 || second.unit > 60 {
                        return Err(format!("SECOND must be [-60, 60], got {}", second.unit));
                    };
                    if second.fraction < -1_000_000_000 || second.fraction > 1_000_000_000 {
                        return Err(format!(
                            "NANOSECOND must be [-1_000_000_000, 1_000_000_000], got {}",
                            second.fraction
                        ));
                    };
                }
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
            DateTimeField::Year => self.year,
            DateTimeField::Month => self.month,
            DateTimeField::Day => self.day,
            DateTimeField::Hour => self.hour,
            DateTimeField::Minute => self.minute,
            DateTimeField::Second => self.second,
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
    mut pdt: &mut ParsedDateTime,
    mut actual: &mut VecDeque<TimeStrToken>,
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
            // 1BC is not represented as year 0 in postgres
            if val == 0 {
                return Err("YEAR cannot be zero".into());
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

    let valid_formats = vec![
        vec![
            Num(0, 1), // year
            Dash,
            Num(0, 1), // month
            Dash,
            Num(0, 1), // day
        ],
        vec![
            Num(0, 1), // year
            Delim,
            Num(0, 1), // month
            Dash,
            Num(0, 1), // day
        ],
        vec![
            Num(0, 1), // year
            Delim,
            Num(0, 1), // month
            Delim,
            Num(0, 1), // day
        ],
    ];

    let original_actual = actual.clone();

    for expected in valid_formats {
        let mut expected = VecDeque::from(expected);

        match fill_pdt_from_tokens(&mut pdt, &mut actual, &mut expected, DateTimeField::Year, 1) {
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
    mut pdt: &mut ParsedDateTime,
    mut actual: &mut VecDeque<TimeStrToken>,
) -> Result<(), String> {
    match determine_format_w_datetimefield(actual)? {
        Some(TimePartFormat::SqlStandard(leading_field)) => {
            let mut expected = expected_dur_like_tokens(leading_field)?;

            fill_pdt_from_tokens(&mut pdt, &mut actual, &mut expected, leading_field, 1)
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
    mut actual: &mut VecDeque<TimeStrToken>,
    leading_field: DateTimeField,
    mut pdt: &mut ParsedDateTime,
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
    }

    let mut expected = expected_sql_standard_interval_tokens(leading_field);

    let sign = trim_and_return_sign(&mut actual);

    fill_pdt_from_tokens(&mut pdt, &mut actual, &mut expected, leading_field, sign)?;

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
    mut actual: &mut VecDeque<TimeStrToken>,
    time_unit: DateTimeField,
    mut pdt: &mut ParsedDateTime,
) -> Result<(), String> {
    use TimeStrToken::*;

    // We remove all spaces during tokenization, so TimeUnit only shows up if
    // there is no space between the number and the TimeUnit, e.g. `1y 2d 3h`, which
    // PostgreSQL allows.
    let mut expected = VecDeque::from(vec![
        Num(0, 1),
        Dot,
        Nanos(0),
        TimeUnit(DateTimeField::Year),
    ]);

    let sign = trim_and_return_sign(&mut actual);

    fill_pdt_from_tokens(&mut pdt, &mut actual, &mut expected, time_unit, sign)?;

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
fn fill_pdt_from_tokens(
    pdt: &mut ParsedDateTime,
    actual: &mut VecDeque<TimeStrToken>,
    expected: &mut VecDeque<TimeStrToken>,
    leading_field: DateTimeField,
    sign: i64,
) -> Result<(), String> {
    use TimeStrToken::*;
    let mut current_field = leading_field;

    let mut i = 0u8;

    let mut unit_buf: Option<DateTimeFieldValue> = None;

    while let (Some(atok), Some(etok)) = (actual.front(), expected.front()) {
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
                expected.pop_front();
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
                expected.pop_front();
                continue;
            }
            (provided, expected) => {
                return Err(format!(
                    "Invalid syntax at offset {}: provided {:?} but expected {:?}",
                    i, provided, expected
                ))
            }
        }
        i += 1;
        actual.pop_front();
        expected.pop_front();
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
    toks: &VecDeque<TimeStrToken>,
) -> Result<Option<TimePartFormat>, String> {
    use DateTimeField::*;
    use TimePartFormat::*;
    use TimeStrToken::*;

    let mut toks = toks.clone();

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
                Some(Colon) | Some(Delim) | None => Ok(Some(SqlStandard(Hour))),
                // Implies {M:S.NS}
                Some(Dot) => Ok(Some(SqlStandard(Minute))),
                _ => Err("Cannot determine format of all parts".into()),
            }
        }
        // Implies {Num}?{TimeUnit}
        Some(TimeUnit(f)) => Ok(Some(PostgreSql(f))),
        _ => Err("Cannot determine format of all parts".into()),
    }
}

/// Get the expected TimeStrTokens to parse SQL Standard time-like
/// DateTimeFields, i.e. HOUR, MINUTE, SECOND. This is used for INTERVAL,
/// TIMESTAMP, and TIME.
///
/// # Errors
/// - If `from` is YEAR, MONTH, or DAY.
fn expected_dur_like_tokens(from: DateTimeField) -> Result<VecDeque<TimeStrToken>, String> {
    use DateTimeField::*;
    use TimeStrToken::*;

    let all_toks = [
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

    Ok(VecDeque::from(all_toks[start..all_toks.len()].to_vec()))
}

/// Get the expected TimeStrTokens to parse TimePartFormat::SqlStandard parts,
/// starting from some `DateTimeField`. Delim tokens are never actually included
/// in the output, but are illustrative of what the expected input of SQL
/// Standard interval values looks like.
fn expected_sql_standard_interval_tokens(from: DateTimeField) -> VecDeque<TimeStrToken> {
    use DateTimeField::*;
    use TimeStrToken::*;

    let all_toks = [
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
        hms => {
            return expected_dur_like_tokens(hms)
                .expect("input to expected_dur_like_tokens shown to be valid");
        }
    };

    VecDeque::from(all_toks[start..end].to_vec())
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
            DateTimeDelimiter => write!(f, "T"),
            Delim => write!(f, " "),
        }
    }
}

/// Convert a string from a time-like datatype (INTERVAL, TIMESTAMP/TZ, DATE, and TIME)
/// into Vec<TimeStrToken>.
///
/// # Warning
/// - Any sequence of numeric characters following a decimal that exceeds 9 charactrers
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
                t.push_back(parse_num(&n, i)?);
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
                t.push_back(TimeStrToken::TimeUnit(c.to_uppercase().parse()?));
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
                    return match val.parse() {
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
mod test {
    use super::*;

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
                determine_format_w_datetimefield(&s).unwrap(),
                test.1.as_ref(),
            ) {
                (Some(a), Some(b)) => {
                    if a != *b {
                        panic!(
                            "determine_format_w_datetimefield returned {:?}, expected {:?}",
                            a, b,
                        )
                    }
                }
                (None, None) => {}
                (x, y) => panic!(
                    "determine_format_w_datetimefield returned {:?}, expected {:?}",
                    x, y,
                ),
            }
        }
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
            match determine_format_w_datetimefield(&s) {
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
            // Mixed delimeter parsing
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
            let mut expected = tokenize_time_str(test.2).unwrap();

            fill_pdt_from_tokens(&mut pdt, &mut actual, &mut expected, test.3, test.4).unwrap();

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
            let mut expected = tokenize_time_str(test.1).unwrap();

            match fill_pdt_from_tokens(&mut pdt, &mut actual, &mut expected, test.2, test.3) {
                Err(e) => assert_eq!(e.to_string(), test.4),
                Ok(_) => panic!("Test passed when expected to fail, generated {:?}", pdt),
            };
        }
    }
    #[test]
    #[should_panic(expected = "Cannot get smaller DateTimeField than SECOND")]
    fn test_fill_pdt_from_tokens_panic() {
        use DateTimeField::*;
        let test_cases = [
            // Mismatched syntax
            ("1 2", "0 0", Second, 1),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let mut actual = tokenize_time_str(test.0).unwrap();
            let mut expected = tokenize_time_str(test.1).unwrap();

            if fill_pdt_from_tokens(&mut pdt, &mut actual, &mut expected, test.2, test.3).is_ok() {
                panic!(
                    "test_fill_pdt_from_tokens_panic should have paniced. input {}\nformat {}\
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
        ];

        for test in test_cases.iter() {
            let actual = ParsedDateTime::build_parsed_datetime_interval(test.1, test.2).unwrap();
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
                "invalid DateTimeField: X",
            ),
            (
                "0 foo",
                Second,
                "invalid DateTimeField: FOO",
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
        ];
        for test in test_cases.iter() {
            match ParsedDateTime::build_parsed_datetime_interval(test.0, test.1) {
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
}

#[test]
fn test_parseddatetime_add_field() {
    use DateTimeField::*;
    let pdt_unit = ParsedDateTime {
        year: Some(DateTimeFieldValue::new(1, 0)),
        month: Some(DateTimeFieldValue::new(2, 0)),
        day: Some(DateTimeFieldValue::new(2, 0)),
        hour: Some(DateTimeFieldValue::new(3, 0)),
        minute: Some(DateTimeFieldValue::new(4, 0)),
        second: Some(DateTimeFieldValue::new(5, 0)),
        ..Default::default()
    };

    let pdt_frac = ParsedDateTime {
        year: Some(DateTimeFieldValue::new(1, 555_555_555)),
        month: Some(DateTimeFieldValue::new(2, 555_555_555)),
        day: Some(DateTimeFieldValue::new(2, 555_555_555)),
        hour: Some(DateTimeFieldValue::new(3, 555_555_555)),
        minute: Some(DateTimeFieldValue::new(4, 555_555_555)),
        second: Some(DateTimeFieldValue::new(5, 555_555_555)),
        ..Default::default()
    };

    let pdt_frac_neg = ParsedDateTime {
        year: Some(DateTimeFieldValue::new(-1, -555_555_555)),
        month: Some(DateTimeFieldValue::new(-2, -555_555_555)),
        day: Some(DateTimeFieldValue::new(-2, -555_555_555)),
        hour: Some(DateTimeFieldValue::new(-3, -555_555_555)),
        minute: Some(DateTimeFieldValue::new(-4, -555_555_555)),
        second: Some(DateTimeFieldValue::new(-5, -555_555_555)),
        ..Default::default()
    };

    run_test_parseddatetime_add_field(pdt_unit.clone(), Year, (12, 0, 0));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Month, (2, 0, 0));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Day, (0, 2 * 60 * 60 * 24, 0));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Hour, (0, 3 * 60 * 60, 0));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Minute, (0, 4 * 60, 0));
    run_test_parseddatetime_add_field(pdt_unit, Second, (0, 5, 0));
    run_test_parseddatetime_add_field(pdt_frac.clone(), Year, (18, 0, 0));
    run_test_parseddatetime_add_field(
        pdt_frac.clone(),
        Month,
        (
            2,
            // 16 days 15:59:59.99856
            16 * 60 * 60 * 24 + 15 * 60 * 60 + 59 * 60 + 59,
            998_560_000,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac.clone(),
        Day,
        (
            0,
            // 2 days 13:19:59.999952
            2 * 60 * 60 * 24 + 13 * 60 * 60 + 19 * 60 + 59,
            999_952_000,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac.clone(),
        Hour,
        (
            0,
            // 03:33:19.999998
            3 * 60 * 60 + 33 * 60 + 19,
            999_998_000,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac.clone(),
        Minute,
        (
            0,
            // 00:04:33.333333
            4 * 60 + 33,
            333_333_300,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac,
        Second,
        (
            0,
            // 00:00:05.555556
            5,
            555_555_555,
        ),
    );
    run_test_parseddatetime_add_field(pdt_frac_neg.clone(), Year, (-18, 0, 0));
    run_test_parseddatetime_add_field(
        pdt_frac_neg.clone(),
        Month,
        (
            -2,
            // -16 days -15:59:59.99856
            -(16 * 60 * 60 * 24 + 15 * 60 * 60 + 59 * 60 + 59),
            -998_560_000,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac_neg.clone(),
        Day,
        (
            0,
            // -2 days 13:19:59.999952
            -(2 * 60 * 60 * 24 + 13 * 60 * 60 + 19 * 60 + 59),
            -999_952_000,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac_neg.clone(),
        Hour,
        (
            0,
            // -03:33:19.999998
            -(3 * 60 * 60 + 33 * 60 + 19),
            -999_998_000,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac_neg.clone(),
        Minute,
        (
            0,
            // -00:04:33.333333
            -(4 * 60 + 33),
            -333_333_300,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac_neg,
        Second,
        (
            0,
            // -00:00:05.555556
            -5,
            -555_555_555,
        ),
    );

    fn run_test_parseddatetime_add_field(
        pdt: ParsedDateTime,
        f: DateTimeField,
        expected: (i64, i64, i64),
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
        // 21:56:55.5
        Interval::new(0, 21 * 60 * 60 + 56 * 60 + 55, 500_000_000).unwrap(),
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            day: Some(DateTimeFieldValue::new(-1, 0)),
            hour: Some(DateTimeFieldValue::new(2, 0)),
            minute: Some(DateTimeFieldValue::new(3, 0)),
            second: Some(DateTimeFieldValue::new(4, 500_000_000)),
            ..Default::default()
        },
        // -21:56:55.5
        Interval::new(0, -(21 * 60 * 60 + 56 * 60 + 55), -500_000_000).unwrap(),
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            day: Some(DateTimeFieldValue::new(1, 0)),
            second: Some(DateTimeFieldValue::new(0, -270_000_000)),
            ..Default::default()
        },
        // 23:59:59.73
        Interval::new(0, 23 * 60 * 60 + 59 * 60 + 59, 730_000_000).unwrap(),
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            day: Some(DateTimeFieldValue::new(-1, 0)),
            second: Some(DateTimeFieldValue::new(0, 270_000_000)),
            ..Default::default()
        },
        // -23:59:59.73
        Interval::new(0, -(23 * 60 * 60 + 59 * 60 + 59), -730_000_000).unwrap(),
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
        // -1 year -4 months +13 days +07:07:53.220828
        Interval::new(
            -16,
            13 * 60 * 60 * 24 + 7 * 60 * 60 + 7 * 60 + 53,
            220_828_255,
        )
        .unwrap(),
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
        // -1 year -4 months +13 days +07:07:53.220828255
        Interval::new(
            -16,
            13 * 60 * 60 * 24 + 7 * 60 * 60 + 7 * 60 + 53,
            220_828_255,
        )
        .unwrap(),
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
