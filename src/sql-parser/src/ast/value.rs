// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright 2019 Materialize, Inc. All rights reserved.
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

mod datetime;
pub use datetime::{
    DateTimeField, ExtractField, Interval, IntervalValue, ParsedDate, ParsedDateTime,
    ParsedTimestamp,
};

#[derive(Debug)]
pub struct ValueError(String);

impl std::error::Error for ValueError {}

impl fmt::Display for ValueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Primitive SQL values such as number and string
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    /// Numeric literal
    Number(String),
    /// 'string value'
    SingleQuotedString(String),
    /// N'string value'
    NationalStringLiteral(String),
    /// X'hex value'
    HexStringLiteral(String),
    /// Boolean value true or false
    Boolean(bool),
    /// `DATE '...'` literals
    Date(String, ParsedDate),
    /// `TIME '...'` literals
    Time(String),
    /// `TIMESTAMP '...'` literals
    Timestamp(String, ParsedTimestamp),
    /// `TIMESTAMP WITH TIME ZONE` literals
    TimestampTz(String, ParsedTimestamp),
    /// INTERVAL literals, roughly in the following format:
    ///
    /// ```text
    /// INTERVAL '<value>' <leading_field> [ (<leading_precision>) ]
    ///     [ TO <last_field> [ (<nanosecond_precision>) ] ]
    /// ```
    /// e.g. `INTERVAL '123:45.67' MINUTE(3) TO SECOND(2)`.
    ///
    /// The parser does not validate the `<value>`, nor does it ensure
    /// that the `<leading_field>` units >= the units in `<last_field>`,
    /// so the user will have to reject intervals like `HOUR TO YEAR`.
    Interval(IntervalValue),
    /// `NULL` value
    Null,
    /// An array of values
    Array(Vec<Value>),
}

impl fmt::Display for Value {
    #[allow(clippy::unneeded_field_pattern)] // want to be warned if we add another field
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Number(v) => write!(f, "{}", v),
            Value::SingleQuotedString(v) => write!(f, "'{}'", escape_single_quote_string(v)),
            Value::NationalStringLiteral(v) => write!(f, "N'{}'", v),
            Value::HexStringLiteral(v) => write!(f, "X'{}'", v),
            Value::Boolean(v) => write!(f, "{}", v),
            Value::Date(v, _) => write!(f, "DATE '{}'", escape_single_quote_string(v)),
            Value::Time(v) => write!(f, "TIME '{}'", escape_single_quote_string(v)),
            Value::Timestamp(v, _) => write!(f, "TIMESTAMP '{}'", escape_single_quote_string(v)),
            Value::TimestampTz(v, _) => write!(
                f,
                "TIMESTAMP WITH TIME ZONE '{}'",
                escape_single_quote_string(v)
            ),
            Value::Interval(IntervalValue {
                parsed: _,
                value,
                precision_high: _,
                precision_low: _,
                nanosecond_precision: Some(nanosecond_precision),
            }) => write!(
                f,
                "INTERVAL '{}' SECOND ({})",
                escape_single_quote_string(value),
                nanosecond_precision
            ),
            Value::Interval(IntervalValue {
                parsed: _,
                value,
                precision_high,
                precision_low,
                nanosecond_precision,
            }) => {
                write!(f, "INTERVAL '{}'", escape_single_quote_string(value),)?;
                match (precision_high, precision_low, nanosecond_precision) {
                    (DateTimeField::Year, DateTimeField::Second, None) => {}
                    (DateTimeField::Year, DateTimeField::Second, Some(ns)) => {
                        write!(f, " SECOND({})", ns)?;
                    }
                    (DateTimeField::Year, low, None) => {
                        write!(f, " {}", low)?;
                    }
                    (high, low, None) => {
                        write!(f, " {} TO {}", high, low)?;
                    }
                    (high, low, Some(ns)) => {
                        write!(f, " {} TO {}({})", high, low, ns)?;
                    }
                }
                Ok(())
            }
            Value::Null => write!(f, "NULL"),
            Value::Array(array) => {
                let mut values = array.iter().peekable();
                write!(f, "ARRAY[")?;
                while let Some(value) = values.next() {
                    write!(f, "{}", value)?;
                    if values.peek().is_some() {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "]")?;
                Ok(())
            }
        }
    }
}

pub struct EscapeSingleQuoteString<'a>(&'a str);

impl<'a> fmt::Display for EscapeSingleQuoteString<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for c in self.0.chars() {
            if c == '\'' {
                write!(f, "\'\'")?;
            } else {
                write!(f, "{}", c)?;
            }
        }
        Ok(())
    }
}

pub fn escape_single_quote_string(s: &str) -> EscapeSingleQuoteString<'_> {
    EscapeSingleQuoteString(s)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn interval_values() {
        let mut iv = IntervalValue::default();
        iv.parsed.year = None;
        match iv.compute_interval() {
            Ok(_) => {}
            Err(e) => panic!("should not error: {:?}", e),
        }
    }

    #[test]
    fn iterate_datetimefield() {
        use DateTimeField::*;
        assert_eq!(
            Year.into_iter().take(10).collect::<Vec<_>>(),
            vec![Month, Day, Hour, Minute, Second]
        )
    }
}
