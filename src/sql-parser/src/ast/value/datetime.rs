// Copyright 2019 Materialize, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// #![deny(missing_docs)]

use std::fmt;
use std::time::Duration;

use super::ValueError;

/// An intermediate value for Intervals, which tracks all data from
/// the user, as well as the computed ParsedDateTime.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IntervalValue {
    /// The raw `[value]` that was present in `INTERVAL '[value]'`
    pub value: String,
    /// the fully parsed date time
    pub parsed: ParsedDateTime,
    /// The most significant DateTimeField to propagate to Interval in
    /// compute_interval.
    pub precision_high: DateTimeField,
    /// The least significant DateTimeField to propagate to Interval in
    /// compute_interval.
    /// precision_low is also used to provide a TimeUnit if the final
    /// part of `value` is ambiguous, e.g. INTERVAL '1-2 3' DAY uses
    /// 'day' as the TimeUnit for 3.
    pub precision_low: DateTimeField,
    /// Maximum nanosecond precision can be specified in SQL source as
    /// `INTERVAL '__' SECOND(_)`.
    pub fsec_max_precision: Option<u64>,
}

impl Default for IntervalValue {
    fn default() -> Self {
        Self {
            value: String::default(),
            parsed: ParsedDateTime::default(),
            precision_high: DateTimeField::Year,
            precision_low: DateTimeField::Second,
            fsec_max_precision: None,
        }
    }
}

impl IntervalValue {
    /// Compute an Interval from an IntervalValue. This could be adapted
    /// to `impl TryFrom<IntervalValue> for Interval` but is slightly more
    /// ergononmic because it doesn't require exposing all of the private
    /// functions this method calls.
    ///
    /// # Errors
    /// - If any component overflows a parameter (i.e. i64).
    pub fn compute_interval(&self) -> Result<Interval, ValueError> {
        use DateTimeField::*;
        let mut months = 0i64;
        let mut seconds = 0i64;
        let mut nanos = 0i64;

        // Add all DateTimeFields, from Year to Seconds.
        self.add_field(Year, &mut months, &mut seconds, &mut nanos)?;

        for field in Year.into_iter().take_while(|f| *f <= Second) {
            self.add_field(field, &mut months, &mut seconds, &mut nanos)?;
        }

        self.truncate_high_fields(&mut months, &mut seconds);
        self.truncate_low_fields(&mut months, &mut seconds, &mut nanos)?;

        // Handle negative seconds with positive nanos or vice versa.
        if nanos < 0 && seconds > 0 {
            nanos += 1_000_000_000_i64;
            seconds -= 1;
        } else if nanos > 0 && seconds < 0 {
            nanos -= 1_000_000_000_i64;
            seconds += 1;
        }

        Ok(Interval {
            months,
            duration: Duration::new(seconds.abs() as u64, nanos.abs() as u32),
            is_positive_dur: seconds >= 0 && nanos >= 0,
        })
    }
    /// Adds the appropriate values from self's ParsedDateTime to the i64 params.
    ///
    /// # Errors
    /// - If any component overflows a parameter (i.e. i64).
    /// - If the specified precision is not within (0,6).
    fn add_field(
        &self,
        d: DateTimeField,
        months: &mut i64,
        seconds: &mut i64,
        nanos: &mut i64,
    ) -> Result<(), ValueError> {
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
                        ValueError(format!(
                            "INTERVAL '{}' overflows maximum months; \
                             cannot exceed {} months",
                            self.value,
                            std::i64::MAX
                        ))
                    })?;

                // months += y_f * 12 / 1_000_000_000
                *months = y_f
                    .checked_mul(12)
                    .and_then(|y_f_m| months.checked_add(y_f_m / 1_000_000_000))
                    .ok_or_else(|| {
                        ValueError(format!(
                            "INTERVAL '{}' overflows maximum months; \
                             cannot exceed {} months",
                            self.value,
                            std::i64::MAX
                        ))
                    })?;
                Ok(())
            }
            Month => {
                let (m, m_f) = match self.units_of(Month) {
                    Some(m) => (m.unit, m.fraction),
                    None => return Ok(()),
                };

                *months = m.checked_add(*months).ok_or_else(|| {
                    ValueError(format!(
                        "INTERVAL '{}' overflows maximum months; \
                         cannot exceed {} months",
                        self.value,
                        std::i64::MAX
                    ))
                })?;

                let m_f_ns = m_f
                    .checked_mul(30 * seconds_multiplier(Day))
                    .ok_or_else(|| {
                        ValueError("Intermediate overflow in MONTH fraction".to_string())
                    })?;

                // seconds += m_f * 30 * seconds_multiplier(Day) / 1_000_000_000
                *seconds = seconds.checked_add(m_f_ns / 1_000_000_000).ok_or_else(|| {
                    ValueError(format!(
                        "INTERVAL '{}' overflows maximum seconds; \
                         cannot exceed {} seconds",
                        self.value,
                        std::i64::MAX
                    ))
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
                    .checked_mul(seconds_multiplier(d))
                    .and_then(|t_s| seconds.checked_add(t_s))
                    .ok_or_else(|| {
                        ValueError(format!(
                            "INTERVAL '{}' overflows maximum seconds; \
                             cannot exceed {} seconds",
                            self.value,
                            std::i64::MAX
                        ))
                    })?;

                let t_f_ns = t_f.checked_mul(seconds_multiplier(dhms)).ok_or_else(|| {
                    ValueError(format!("Intermediate overflow in {} fraction", dhms))
                })?;

                // seconds += t_f * seconds_multiplier(dhms) / 1_000_000_000
                *seconds = seconds.checked_add(t_f_ns / 1_000_000_000).ok_or_else(|| {
                    ValueError(format!(
                        "INTERVAL '{}' overflows maximum seconds; \
                         cannot exceed {} seconds",
                        self.value,
                        std::i64::MAX
                    ))
                })?;

                *nanos += t_f_ns % 1_000_000_000;
                Ok(())
            }
        }
    }

    /// Truncate parameters' values that are more significant than self.precision_high.
    fn truncate_high_fields(&self, months: &mut i64, seconds: &mut i64) {
        use DateTimeField::*;
        match self.precision_high {
            Year => {}
            Month => {
                *months %= 12;
            }
            Day => {
                *months = 0;
            }
            hms => {
                *months = 0;
                *seconds %= seconds_multiplier(hms.next_largest());
            }
        }
    }

    /// Truncate parameters' values that are less significant than self.precision_low.
    ///
    /// # Errors
    /// - If the specified precision is not within (0,6).
    fn truncate_low_fields(
        &self,
        months: &mut i64,
        seconds: &mut i64,
        nanos: &mut i64,
    ) -> Result<(), ValueError> {
        use DateTimeField::*;
        match self.precision_low {
            Year => {
                *months -= *months % 12;
                *seconds = 0;
                *nanos = 0;
            }
            Month => {
                *seconds = 0;
                *nanos = 0;
            }
            // Round nanoseconds.
            Second => {
                let default_precision = 6;
                let precision = match self.fsec_max_precision {
                    Some(p) => p,
                    None => default_precision,
                };

                if precision > default_precision {
                    return Err(ValueError(format!(
                        "SECOND precision must be (0, 6), have SECOND({})",
                        precision
                    )));
                }

                // Check if value should round up to nearest fractional place.
                let remainder = *nanos % 10_i64.pow(9 - precision as u32);
                if remainder / 10_i64.pow(8 - precision as u32) > 4 {
                    *nanos += 10_i64.pow(9 - precision as u32);
                }

                *nanos -= remainder;
            }
            dhm => {
                *seconds -= *seconds % seconds_multiplier(dhm);
                *nanos = 0;
            }
        }
        Ok(())
    }

    /// Retrieve any value that we parsed out of the literal string for the
    /// `field`.
    fn units_of(&self, field: DateTimeField) -> Option<DateTimeUnit> {
        match field {
            DateTimeField::Year => self.parsed.year,
            DateTimeField::Month => self.parsed.month,
            DateTimeField::Day => self.parsed.day,
            DateTimeField::Hour => self.parsed.hour,
            DateTimeField::Minute => self.parsed.minute,
            DateTimeField::Second => self.parsed.second,
        }
    }
}

/// Returns the number of seconds in a single unit of `field`.
fn seconds_multiplier(field: DateTimeField) -> i64 {
    match field {
        DateTimeField::Day => 60 * 60 * 24,
        DateTimeField::Hour => 60 * 60,
        DateTimeField::Minute => 60,
        DateTimeField::Second => 1,
        _other => unreachable!("Do not call with a non-duration field"),
    }
}

/// An interval of time meant to express SQL intervals. Obtained by parsing an
/// `INTERVAL '<value>' <unit> [TO <precision>]`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Interval {
    /// A possibly negative number of months for field types like `YEAR`
    pub months: i64,
    /// An actual timespan, possibly negative
    pub duration: Duration,
    /// Whether or not `duration` is positive
    pub is_positive_dur: bool,
}

impl Default for Interval {
    fn default() -> Self {
        Self {
            months: 0,
            duration: Duration::default(),
            is_positive_dur: true,
        }
    }
}

/// The fields of a Date
///
/// This is not guaranteed to be a valid date
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParsedDate {
    pub year: i64,
    pub month: u8,
    pub day: u8,
}

/// The fields in a `Timestamp`
///
/// Similar to a [`ParsedDateTime`], except that all the fields are required.
/// `nano` is equivalent to `ParsedDateTime.second.fraction`.
///
/// This is not guaranteed to be a valid date
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParsedTimestamp {
    pub year: i64,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub nano: u32,
    pub timezone_offset_second: i64,
}

/// Tracks a unit and a fraction from a parsed time-like string, e.g. INTERVAL
/// '1.2' DAYS.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct DateTimeUnit {
    /// Integer part of the value.
    pub unit: i64,
    /// Fractional part of value, padded to billions/has 9 digits of precision,
    /// e.g. `.5` is represented as `500000000`.
    pub fraction: i64,
}

impl Default for DateTimeUnit {
    fn default() -> Self {
        DateTimeUnit {
            unit: 0,
            fraction: 0,
        }
    }
}
/// All of the fields that can appear in a literal `DATE`, `TIMESTAMP` or `INTERVAL` string.
///
/// This is only used in an `Interval`, which can have any contiguous set of
/// fields set, otherwise you are probably looking for [`ParsedDate`] or
/// [`ParsedTimestamp`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParsedDateTime {
    pub year: Option<DateTimeUnit>,
    pub month: Option<DateTimeUnit>,
    pub day: Option<DateTimeUnit>,
    pub hour: Option<DateTimeUnit>,
    pub minute: Option<DateTimeUnit>,
    // second.fraction is equivalent to nanoseconds.
    pub second: Option<DateTimeUnit>,
    pub timezone_offset_second: Option<i64>,
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
    /// Write to the specified field of a ParsedDateTime iff it is currently set
    /// to None; otherwise generate an error to propagate to the user.
    pub fn write_field_iff_none(
        &mut self,
        f: DateTimeField,
        u: Option<DateTimeUnit>,
    ) -> Result<(), failure::Error> {
        use DateTimeField::*;

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
            _ => failure::bail!("{} field set twice", f),
        }
        Ok(())
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
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_ref() {
            "YEAR" | "YEARS" | "Y" => Ok(Self::Year),
            "MONTH" | "MONTHS" | "MON" | "MONS" => Ok(Self::Month),
            "DAY" | "DAYS" | "D" => Ok(Self::Day),
            "HOUR" | "HOURS" | "H" => Ok(Self::Hour),
            "MINUTE" | "MINUTES" | "M" => Ok(Self::Minute),
            "SECOND" | "SECONDS" | "S" => Ok(Self::Second),
            _ => failure::bail!("invalid DateTimeField: {}", s),
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
}

/// An iterator over DateTimeFields
///
/// Always starts with the value smaller than the current one.
///
/// ```
/// use sql_parser::ast::DateTimeField::*;
/// let mut itr = Hour.into_iter();
/// assert_eq!(itr.next(), Some(Minute));
/// assert_eq!(itr.next(), Some(Second));
/// assert_eq!(itr.next(), None);
/// ```
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

/// Similar to a [`DateTimeField`], but with a few more options
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExtractField {
    Millenium,
    Century,
    Decade,
    Year,
    /// The ISO Week-Numbering year
    ///
    /// See https://en.wikipedia.org/wiki/ISO_week_date
    IsoYear,
    Quarter,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Milliseconds,
    Microseconds,
    // Weirder fields
    Timezone,
    TimezoneHour,
    TimezoneMinute,
    WeekOfYear,
    /// The day of the year (1 - 365/366)
    DayOfYear,
    /// The day of the week (0 - 6; Sunday is 0)
    DayOfWeek,
    /// The day of the week (1 - 7; Sunday is 7)
    IsoDayOfWeek,
    /// The number of seconds
    ///
    /// * for DateTime fields, the number of seconds since 1970-01-01 00:00:00-00
    /// * for intervals, the total number of seconds in the interval
    Epoch,
}

impl fmt::Display for ExtractField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExtractField::Millenium => f.write_str("MILLENIUM"),
            ExtractField::Century => f.write_str("CENTURY"),
            ExtractField::Decade => f.write_str("DECADE"),
            ExtractField::Year => f.write_str("YEAR"),
            ExtractField::IsoYear => f.write_str("ISOYEAR"),
            ExtractField::Quarter => f.write_str("QUARTER"),
            ExtractField::Month => f.write_str("MONTH"),
            ExtractField::Day => f.write_str("DAY"),
            ExtractField::Hour => f.write_str("HOUR"),
            ExtractField::Minute => f.write_str("MINUTE"),
            ExtractField::Second => f.write_str("SECOND"),
            ExtractField::Milliseconds => f.write_str("MILLISECONDS"),
            ExtractField::Microseconds => f.write_str("MICROSECONDS"),
            // Weirder fields
            ExtractField::Timezone => f.write_str("TIMEZONE"),
            ExtractField::TimezoneHour => f.write_str("TIMEZONE_HOUR"),
            ExtractField::TimezoneMinute => f.write_str("TIMEZONE_MINUTE"),
            ExtractField::WeekOfYear => f.write_str("WEEK"),
            ExtractField::DayOfYear => f.write_str("DOY"),
            ExtractField::DayOfWeek => f.write_str("DOW"),
            ExtractField::IsoDayOfWeek => f.write_str("ISODOW"),
            ExtractField::Epoch => f.write_str("EPOCH"),
        }
    }
}

use std::str::FromStr;

impl FromStr for ExtractField {
    type Err = ValueError;
    fn from_str(s: &str) -> Result<ExtractField, Self::Err> {
        Ok(match &*s.to_uppercase() {
            "MILLENIUM" => ExtractField::Millenium,
            "CENTURY" => ExtractField::Century,
            "DECADE" => ExtractField::Decade,
            "YEAR" => ExtractField::Year,
            "ISOYEAR" => ExtractField::IsoYear,
            "QUARTER" => ExtractField::Quarter,
            "MONTH" => ExtractField::Month,
            "DAY" => ExtractField::Day,
            "HOUR" => ExtractField::Hour,
            "MINUTE" => ExtractField::Minute,
            "SECOND" => ExtractField::Second,
            "MILLISECONDS" => ExtractField::Milliseconds,
            "MICROSECONDS" => ExtractField::Microseconds,
            // Weirder fields
            "TIMEZONE" => ExtractField::Timezone,
            "TIMEZONE_HOUR" => ExtractField::TimezoneHour,
            "TIMEZONE_MINUTE" => ExtractField::TimezoneMinute,
            "WEEK" => ExtractField::WeekOfYear,
            "DOY" => ExtractField::DayOfYear,
            "DOW" => ExtractField::DayOfWeek,
            "ISODOW" => ExtractField::IsoDayOfWeek,
            "EPOCH" => ExtractField::Epoch,
            _ => return Err(ValueError(format!("invalid EXTRACT specifier: {}", s))),
        })
    }
}
