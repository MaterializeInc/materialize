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

use super::ValueError;

/// An intermediate value for Intervals, which tracks all data from
/// the user, as well as the computed ParsedDateTime.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IntervalValue {
    /// The raw `[value]` that was present in `INTERVAL '[value]'`
    pub value: String,
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
            precision_high: DateTimeField::Year,
            precision_low: DateTimeField::Second,
            fsec_max_precision: None,
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
