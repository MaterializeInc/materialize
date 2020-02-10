// Copyright Materialize, Inc. All rights reserved.
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
    fn units_of(&self, field: DateTimeField) -> Option<DateTimeFieldValue> {
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

/// The fields of a Time
///
/// This is not guaranteed to be a valid time of day
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParsedTime {
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub nano: u32,
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
    /// Construct DateTimeFieldValue { unit, fraction }.
    pub fn new(unit: i64, fraction: i64) -> Self {
        DateTimeFieldValue { unit, fraction }
    }
}
/// All of the fields that can appear in a literal `DATE`, `TIMESTAMP` or `INTERVAL` string.
///
/// This is only used in an `Interval`, which can have any contiguous set of
/// fields set, otherwise you are probably looking for [`ParsedDate`] or
/// [`ParsedTimestamp`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParsedDateTime {
    pub year: Option<DateTimeFieldValue>,
    pub month: Option<DateTimeFieldValue>,
    pub day: Option<DateTimeFieldValue>,
    pub hour: Option<DateTimeFieldValue>,
    pub minute: Option<DateTimeFieldValue>,
    // second.fraction is equivalent to nanoseconds.
    pub second: Option<DateTimeFieldValue>,
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
        u: Option<DateTimeFieldValue>,
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
    pub fn validate_datelike_ymd(&self) -> Result<(), failure::Error> {
        match (self.year, self.month, self.day) {
            (Some(year), Some(month), Some(day)) => {
                if year.unit == 0 {
                    failure::bail!("YEAR cannot be zero.")
                }

                if month.unit > 12 || month.unit <= 0 {
                    failure::bail!("MONTH must be (1, 12), got {}", month.unit)
                }
                if day.unit == 0 {
                    failure::bail!("DAY cannot be zero.")
                }
                Ok(())
            }
            (_, _, _) => failure::bail!("YEAR, MONTH, DAY are all required."),
        }
    }
    pub fn validate_timelike_hms(&self) -> Result<(), failure::Error> {
        if let Some(hour) = self.hour {
            if hour.unit > 23 {
                failure::bail!("HOUR must be < 24, got {}", hour.unit)
            }
        }

        if let Some(minute) = self.minute {
            if minute.unit > 59 {
                failure::bail!("HOUR must be < 60, got {}", minute.unit)
            }
        }

        if let Some(second) = self.second {
            if second.unit > 60 {
                failure::bail!("SECOND must be < 61, got {}", second.unit)
            }
            if second.fraction > 1_000_000_000 {
                failure::bail!(
                    "NANOSECOND must be < 1_000_000_000, got {}",
                    second.fraction
                )
            }
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
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
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

#[test]
fn test_interval_value_truncate_low_fields() {
    use DateTimeField::*;

    let mut test_cases = [
        (
            IntervalValue {
                precision_low: Year,
                ..Default::default()
            },
            (321, 654_321, 321_000_000),
            (26 * 12, 0, 0),
        ),
        (
            IntervalValue {
                precision_low: Month,
                ..Default::default()
            },
            (321, 654_321, 321_000_000),
            (321, 0, 0),
        ),
        (
            IntervalValue {
                precision_low: Day,
                ..Default::default()
            },
            (321, 654_321, 321_000_000),
            (321, 7 * 60 * 60 * 24, 0),
        ),
        (
            IntervalValue {
                precision_low: Hour,
                ..Default::default()
            },
            (321, 654_321, 321_000_000),
            (321, 181 * 60 * 60, 0),
        ),
        (
            IntervalValue {
                precision_low: Minute,
                ..Default::default()
            },
            (321, 654_321, 321_000_000),
            (321, 10905 * 60, 0),
        ),
        (
            IntervalValue {
                precision_low: Second,
                ..Default::default()
            },
            (321, 654_321, 321_000_000),
            (321, 654_321, 321_000_000),
        ),
        (
            IntervalValue {
                precision_low: Second,
                fsec_max_precision: Some(1),
                ..Default::default()
            },
            (321, 654_321, 321_000_000),
            (321, 654_321, 300_000_000),
        ),
        (
            IntervalValue {
                precision_low: Second,
                fsec_max_precision: Some(0),
                ..Default::default()
            },
            (321, 654_321, 321_000_000),
            (321, 654_321, 0),
        ),
    ];

    for test in test_cases.iter_mut() {
        test.0
            .truncate_low_fields(&mut (test.1).0, &mut (test.1).1, &mut (test.1).2)
            .unwrap();
        if (test.1).0 != (test.2).0 || (test.1).1 != (test.2).1 || (test.1).2 != (test.2).2 {
            panic!(
                "test_interval_value_truncate_low_fields failed \n actual: {:?} \n expected: {:?}",
                test.1, test.2
            );
        }
    }
}

#[test]
fn test_interval_value_truncate_high_fields() {
    use DateTimeField::*;

    let mut test_cases = [
        (
            IntervalValue {
                precision_high: Year,
                ..Default::default()
            },
            (321, 654_321),
            (321, 654_321),
        ),
        (
            IntervalValue {
                precision_high: Month,
                ..Default::default()
            },
            (321, 654_321),
            (321 % 12, 654_321),
        ),
        (
            IntervalValue {
                precision_high: Day,
                ..Default::default()
            },
            (321, 654_321),
            (0, 654_321),
        ),
        (
            IntervalValue {
                precision_high: Hour,
                ..Default::default()
            },
            (321, 654_321),
            (0, 654_321 % (60 * 60 * 24)),
        ),
        (
            IntervalValue {
                precision_high: Minute,
                ..Default::default()
            },
            (321, 654_321),
            (0, 654_321 % (60 * 60)),
        ),
        (
            IntervalValue {
                precision_high: Second,
                ..Default::default()
            },
            (321, 654_321),
            (0, 654_321 % 60),
        ),
    ];

    for test in test_cases.iter_mut() {
        test.0
            .truncate_high_fields(&mut (test.1).0, &mut (test.1).1);
        if (test.1).0 != (test.2).0 || (test.1).1 != (test.2).1 {
            panic!(
                "test_interval_value_truncate_high_fields failed \n actual: {:?} \n expected: {:?}",
                test.1, test.2
            );
        }
    }
}

#[test]
fn test_interval_value_add_field() {
    use DateTimeField::*;
    let iv_unit = IntervalValue {
        parsed: ParsedDateTime {
            year: Some(DateTimeFieldValue::new(1, 0)),
            month: Some(DateTimeFieldValue::new(2, 0)),
            day: Some(DateTimeFieldValue::new(2, 0)),
            hour: Some(DateTimeFieldValue::new(3, 0)),
            minute: Some(DateTimeFieldValue::new(4, 0)),
            second: Some(DateTimeFieldValue::new(5, 0)),
            ..Default::default()
        },
        ..Default::default()
    };

    let iv_frac = IntervalValue {
        parsed: ParsedDateTime {
            year: Some(DateTimeFieldValue::new(1, 555_555_555)),
            month: Some(DateTimeFieldValue::new(2, 555_555_555)),
            day: Some(DateTimeFieldValue::new(2, 555_555_555)),
            hour: Some(DateTimeFieldValue::new(3, 555_555_555)),
            minute: Some(DateTimeFieldValue::new(4, 555_555_555)),
            second: Some(DateTimeFieldValue::new(5, 555_555_555)),
            ..Default::default()
        },
        ..Default::default()
    };

    let iv_frac_neg = IntervalValue {
        parsed: ParsedDateTime {
            year: Some(DateTimeFieldValue::new(-1, -555_555_555)),
            month: Some(DateTimeFieldValue::new(-2, -555_555_555)),
            day: Some(DateTimeFieldValue::new(-2, -555_555_555)),
            hour: Some(DateTimeFieldValue::new(-3, -555_555_555)),
            minute: Some(DateTimeFieldValue::new(-4, -555_555_555)),
            second: Some(DateTimeFieldValue::new(-5, -555_555_555)),
            ..Default::default()
        },
        ..Default::default()
    };

    run_test_interval_value_add_field(iv_unit.clone(), Year, (12, 0, 0));
    run_test_interval_value_add_field(iv_unit.clone(), Month, (2, 0, 0));
    run_test_interval_value_add_field(iv_unit.clone(), Day, (0, 2 * 60 * 60 * 24, 0));
    run_test_interval_value_add_field(iv_unit.clone(), Hour, (0, 3 * 60 * 60, 0));
    run_test_interval_value_add_field(iv_unit.clone(), Minute, (0, 4 * 60, 0));
    run_test_interval_value_add_field(iv_unit, Second, (0, 5, 0));
    run_test_interval_value_add_field(iv_frac.clone(), Year, (18, 0, 0));
    run_test_interval_value_add_field(
        iv_frac.clone(),
        Month,
        (
            2,
            // 16 days 15:59:59.99856
            16 * 60 * 60 * 24 + 15 * 60 * 60 + 59 * 60 + 59,
            998_560_000,
        ),
    );
    run_test_interval_value_add_field(
        iv_frac.clone(),
        Day,
        (
            0,
            // 2 days 13:19:59.999952
            2 * 60 * 60 * 24 + 13 * 60 * 60 + 19 * 60 + 59,
            999_952_000,
        ),
    );
    run_test_interval_value_add_field(
        iv_frac.clone(),
        Hour,
        (
            0,
            // 03:33:19.999998
            3 * 60 * 60 + 33 * 60 + 19,
            999_998_000,
        ),
    );
    run_test_interval_value_add_field(
        iv_frac.clone(),
        Minute,
        (
            0,
            // 00:04:33.333333
            4 * 60 + 33,
            333_333_300,
        ),
    );
    run_test_interval_value_add_field(
        iv_frac,
        Second,
        (
            0,
            // 00:00:05.555556
            5,
            555_555_555,
        ),
    );
    run_test_interval_value_add_field(iv_frac_neg.clone(), Year, (-18, 0, 0));
    run_test_interval_value_add_field(
        iv_frac_neg.clone(),
        Month,
        (
            -2,
            // -16 days -15:59:59.99856
            -(16 * 60 * 60 * 24 + 15 * 60 * 60 + 59 * 60 + 59),
            -998_560_000,
        ),
    );
    run_test_interval_value_add_field(
        iv_frac_neg.clone(),
        Day,
        (
            0,
            // -2 days 13:19:59.999952
            -(2 * 60 * 60 * 24 + 13 * 60 * 60 + 19 * 60 + 59),
            -999_952_000,
        ),
    );
    run_test_interval_value_add_field(
        iv_frac_neg.clone(),
        Hour,
        (
            0,
            // -03:33:19.999998
            -(3 * 60 * 60 + 33 * 60 + 19),
            -999_998_000,
        ),
    );
    run_test_interval_value_add_field(
        iv_frac_neg.clone(),
        Minute,
        (
            0,
            // -00:04:33.333333
            -(4 * 60 + 33),
            -333_333_300,
        ),
    );
    run_test_interval_value_add_field(
        iv_frac_neg,
        Second,
        (
            0,
            // -00:00:05.555556
            -5,
            -555_555_555,
        ),
    );

    fn run_test_interval_value_add_field(
        iv: IntervalValue,
        f: DateTimeField,
        expected: (i64, i64, i64),
    ) {
        let mut res = (0, 0, 0);

        iv.add_field(f, &mut res.0, &mut res.1, &mut res.2).unwrap();

        if res.0 != expected.0 || res.1 != expected.1 || res.2 != expected.2 {
            panic!(
                "test_interval_value_add_field failed \n actual: {:?} \n expected: {:?}",
                res, expected
            );
        }
    }
}

#[test]
fn test_interval_value_compute_interval() {
    use DateTimeField::*;
    run_test_interval_value_compute_interval(
        IntervalValue {
            parsed: ParsedDateTime {
                year: Some(DateTimeFieldValue::new(1, 0)),
                month: Some(DateTimeFieldValue::new(1, 0)),
                ..Default::default()
            },
            ..Default::default()
        },
        Interval {
            months: 13,
            ..Default::default()
        },
    );
    run_test_interval_value_compute_interval(
        IntervalValue {
            parsed: ParsedDateTime {
                year: Some(DateTimeFieldValue::new(1, 0)),
                month: Some(DateTimeFieldValue::new(-1, 0)),
                ..Default::default()
            },
            ..Default::default()
        },
        Interval {
            months: 11,
            ..Default::default()
        },
    );
    run_test_interval_value_compute_interval(
        IntervalValue {
            parsed: ParsedDateTime {
                year: Some(DateTimeFieldValue::new(-1, 0)),
                month: Some(DateTimeFieldValue::new(1, 0)),
                ..Default::default()
            },
            ..Default::default()
        },
        Interval {
            months: -11,
            ..Default::default()
        },
    );
    run_test_interval_value_compute_interval(
        IntervalValue {
            parsed: ParsedDateTime {
                day: Some(DateTimeFieldValue::new(1, 0)),
                hour: Some(DateTimeFieldValue::new(-2, 0)),
                minute: Some(DateTimeFieldValue::new(-3, 0)),
                second: Some(DateTimeFieldValue::new(-4, -500_000_000)),
                ..Default::default()
            },
            ..Default::default()
        },
        // 21:56:55.5
        Interval {
            duration: Duration::new(21 * 60 * 60 + 56 * 60 + 55, 500_000_000),
            ..Default::default()
        },
    );
    run_test_interval_value_compute_interval(
        IntervalValue {
            parsed: ParsedDateTime {
                day: Some(DateTimeFieldValue::new(-1, 0)),
                hour: Some(DateTimeFieldValue::new(2, 0)),
                minute: Some(DateTimeFieldValue::new(3, 0)),
                second: Some(DateTimeFieldValue::new(4, 500_000_000)),
                ..Default::default()
            },
            ..Default::default()
        },
        // -21:56:55.5
        Interval {
            is_positive_dur: false,
            duration: Duration::new(21 * 60 * 60 + 56 * 60 + 55, 500_000_000),
            ..Default::default()
        },
    );
    run_test_interval_value_compute_interval(
        IntervalValue {
            parsed: ParsedDateTime {
                day: Some(DateTimeFieldValue::new(1, 0)),
                second: Some(DateTimeFieldValue::new(0, -270_000_000)),
                ..Default::default()
            },
            ..Default::default()
        },
        // 23:59:59.73
        Interval {
            duration: Duration::new(23 * 60 * 60 + 59 * 60 + 59, 730_000_000),
            ..Default::default()
        },
    );
    run_test_interval_value_compute_interval(
        IntervalValue {
            parsed: ParsedDateTime {
                day: Some(DateTimeFieldValue::new(-1, 0)),
                second: Some(DateTimeFieldValue::new(0, 270_000_000)),
                ..Default::default()
            },
            ..Default::default()
        },
        // -23:59:59.73
        Interval {
            months: 0,
            is_positive_dur: false,
            duration: Duration::new(23 * 60 * 60 + 59 * 60 + 59, 730_000_000),
        },
    );
    run_test_interval_value_compute_interval(
        IntervalValue {
            parsed: ParsedDateTime {
                year: Some(DateTimeFieldValue::new(-1, -555_555_555)),
                month: Some(DateTimeFieldValue::new(2, 555_555_555)),
                day: Some(DateTimeFieldValue::new(-3, -555_555_555)),
                hour: Some(DateTimeFieldValue::new(4, 555_555_555)),
                minute: Some(DateTimeFieldValue::new(-5, -555_555_555)),
                second: Some(DateTimeFieldValue::new(6, 555_555_555)),
                ..Default::default()
            },
            ..Default::default()
        },
        // -1 year -4 months +13 days +07:07:53.220828
        Interval {
            months: -16,
            is_positive_dur: true,
            duration: Duration::new(13 * 60 * 60 * 24 + 7 * 60 * 60 + 7 * 60 + 53, 220_828_000),
        },
    );
    run_test_interval_value_compute_interval(
        IntervalValue {
            parsed: ParsedDateTime {
                year: Some(DateTimeFieldValue::new(-1, -555_555_555)),
                month: Some(DateTimeFieldValue::new(2, 555_555_555)),
                day: Some(DateTimeFieldValue::new(-3, -555_555_555)),
                hour: Some(DateTimeFieldValue::new(4, 555_555_555)),
                minute: Some(DateTimeFieldValue::new(-5, -555_555_555)),
                second: Some(DateTimeFieldValue::new(6, 555_555_555)),
                ..Default::default()
            },
            fsec_max_precision: Some(1),
            ..Default::default()
        },
        // -1 year -4 months +13 days +07:07:53.2
        Interval {
            months: -16,
            is_positive_dur: true,
            duration: Duration::new(13 * 60 * 60 * 24 + 7 * 60 * 60 + 7 * 60 + 53, 200_000_000),
        },
    );
    run_test_interval_value_compute_interval(
        IntervalValue {
            parsed: ParsedDateTime {
                year: Some(DateTimeFieldValue::new(-1, -555_555_555)),
                month: Some(DateTimeFieldValue::new(2, 555_555_555)),
                day: Some(DateTimeFieldValue::new(-3, -555_555_555)),
                hour: Some(DateTimeFieldValue::new(4, 555_555_555)),
                minute: Some(DateTimeFieldValue::new(-5, -555_555_555)),
                second: Some(DateTimeFieldValue::new(6, 555_555_555)),
                ..Default::default()
            },
            precision_high: Month,
            precision_low: Minute,
            ..Default::default()
        },
        // -4 months +13 days +07:07:00
        Interval {
            months: -4,
            is_positive_dur: true,
            duration: Duration::new(13 * 60 * 60 * 24 + 7 * 60 * 60 + 7 * 60, 0),
        },
    );
    run_test_interval_value_compute_interval(
        IntervalValue {
            parsed: ParsedDateTime {
                year: Some(DateTimeFieldValue::new(-1, -555_555_555)),
                month: Some(DateTimeFieldValue::new(2, 555_555_555)),
                day: Some(DateTimeFieldValue::new(-3, -555_555_555)),
                hour: Some(DateTimeFieldValue::new(4, 555_555_555)),
                minute: Some(DateTimeFieldValue::new(-5, -555_555_555)),
                second: Some(DateTimeFieldValue::new(6, 555_555_555)),
                ..Default::default()
            },
            precision_high: Day,
            precision_low: Hour,
            ..Default::default()
        },
        // 13 days 07:00:00
        Interval {
            months: 0,
            is_positive_dur: true,
            duration: Duration::new(13 * 60 * 60 * 24 + 7 * 60 * 60, 0),
        },
    );
    run_test_interval_value_compute_interval(
        IntervalValue {
            value: "".to_string(),
            parsed: ParsedDateTime {
                year: Some(DateTimeFieldValue::new(-1, -555_555_555)),
                month: Some(DateTimeFieldValue::new(2, 555_555_555)),
                day: Some(DateTimeFieldValue::new(-3, -555_555_555)),
                hour: Some(DateTimeFieldValue::new(4, 555_555_555)),
                minute: Some(DateTimeFieldValue::new(-5, -555_555_555)),
                second: Some(DateTimeFieldValue::new(6, 555_555_555)),
                ..Default::default()
            },
            precision_high: Day,
            precision_low: Hour,
            fsec_max_precision: Some(1),
        },
        // 13 days 07:00:00
        Interval {
            months: 0,
            is_positive_dur: true,
            duration: Duration::new(13 * 60 * 60 * 24 + 7 * 60 * 60, 0),
        },
    );

    fn run_test_interval_value_compute_interval(iv: IntervalValue, expected: Interval) {
        let actual = iv.compute_interval().unwrap();

        if actual != expected {
            panic!(
                "test_interval_compute_interval failed\n input {:?}\nactual {:?}\nexpected {:?}",
                iv, actual, expected
            )
        }
    }
}
