// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
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
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::ast::display::{self, AstDisplay, AstFormatter};
use crate::ast::Ident;

#[derive(Debug)]
pub struct ValueError(pub(crate) String);

impl std::error::Error for ValueError {}

impl fmt::Display for ValueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Primitive SQL values.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Value {
    /// Numeric value.
    Number(String),
    /// String value.
    String(String),
    /// Hex string value.
    HexString(String),
    /// Boolean value.
    Boolean(bool),
    /// INTERVAL literals, roughly in the following format:
    ///
    /// ```text
    /// INTERVAL '<value>' <leading_field> [ TO <last_field>
    ///     [ (<fractional_seconds_precision>) ] ]
    /// ```
    /// e.g. `INTERVAL '123:45.678' MINUTE TO SECOND(2)`.
    Interval(IntervalValue),
    /// Array of Values.
    Array(Vec<Value>),
    /// `NULL` value.
    Null,
}

impl AstDisplay for Value {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Value::Number(v) => f.write_str(v),
            Value::String(v) => {
                f.write_str("'");
                f.write_node(&display::escape_single_quote_string(v));
                f.write_str("'");
            }
            Value::HexString(v) => {
                f.write_str("X'");
                f.write_str(v);
                f.write_str("'");
            }
            Value::Boolean(v) => f.write_str(v),
            Value::Interval(IntervalValue {
                value,
                precision_high: _,
                precision_low: _,
                fsec_max_precision: Some(fsec_max_precision),
            }) => {
                f.write_str("INTERVAL '");
                f.write_node(&display::escape_single_quote_string(value));
                f.write_str("' SECOND (");
                f.write_str(fsec_max_precision);
                f.write_str(")");
            }
            Value::Interval(IntervalValue {
                value,
                precision_high,
                precision_low,
                fsec_max_precision,
            }) => {
                f.write_str("INTERVAL '");
                f.write_node(&display::escape_single_quote_string(value));
                f.write_str("'");
                match (precision_high, precision_low, fsec_max_precision) {
                    (DateTimeField::Year, DateTimeField::Second, None) => {}
                    (DateTimeField::Year, DateTimeField::Second, Some(ns)) => {
                        f.write_str(" SECOND(");
                        f.write_str(ns);
                        f.write_str(")");
                    }
                    (DateTimeField::Year, low, None) => {
                        f.write_str(" ");
                        f.write_str(low);
                    }
                    (high, low, None) => {
                        f.write_str(" ");
                        f.write_str(high);
                        f.write_str(" TO ");
                        f.write_str(low);
                    }
                    (high, low, Some(ns)) => {
                        f.write_str(" ");
                        f.write_str(high);
                        f.write_str(" TO ");
                        f.write_str(low);
                        f.write_str("(");
                        f.write_str(ns);
                        f.write_str(")");
                    }
                }
            }
            Value::Array(values) => {
                f.write_str("[");
                f.write_node(&display::comma_separated(values));
                f.write_str("]");
            }
            Value::Null => f.write_str("NULL"),
        }
    }
}
impl_display!(Value);

impl From<Ident> for Value {
    fn from(ident: Ident) -> Self {
        Self::String(ident.0)
    }
}

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DateTimeField {
    Millennium,
    Century,
    Decade,
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Milliseconds,
    Microseconds,
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

impl FromStr for DateTimeField {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_uppercase().as_ref() {
            "MILLENNIUM" | "MILLENNIA" | "MIL" | "MILS" => Ok(Self::Millennium),
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

/// An intermediate value for Intervals, which tracks all data from
/// the user, as well as the computed ParsedDateTime.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
