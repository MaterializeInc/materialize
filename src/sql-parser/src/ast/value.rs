// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. All rights reserved.
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

use repr::datetime::DateTimeField;

mod datetime;
use crate::ast::display::{AstDisplay, AstFormatter};
pub use datetime::{ExtractField, IntervalValue};

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
    /// X'hex value'
    HexStringLiteral(String),
    /// Boolean value true or false
    Boolean(bool),
    /// INTERVAL literals, roughly in the following format:
    ///
    /// ```text
    /// INTERVAL '<value>' <leading_field> [ TO <last_field>
    ///     [ (<fractional_seconds_precision>) ] ]
    /// ```
    /// e.g. `INTERVAL '123:45.678' MINUTE TO SECOND(2)`.
    Interval(IntervalValue),
    /// `NULL` value
    Null,
}

impl AstDisplay for Value {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Value::Number(v) => f.write_str(v),
            Value::SingleQuotedString(v) => {
                f.write_str("'");
                f.write_node(&escape_single_quote_string(v));
                f.write_str("'");
            }
            Value::HexStringLiteral(v) => {
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
                f.write_node(&escape_single_quote_string(value));
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
                f.write_node(&escape_single_quote_string(value));
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
            Value::Null => f.write_str("NULL"),
        }
    }
}
impl_display!(Value);

pub struct EscapeSingleQuoteString<'a>(&'a str);

impl<'a> AstDisplay for EscapeSingleQuoteString<'a> {
    fn fmt(&self, f: &mut AstFormatter) {
        for c in self.0.chars() {
            if c == '\'' {
                f.write_str("\'\'");
            } else {
                f.write_str(c);
            }
        }
    }
}
impl<'a> fmt::Display for EscapeSingleQuoteString<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.to_ast_string())
    }
}

pub fn escape_single_quote_string(s: &str) -> EscapeSingleQuoteString<'_> {
    EscapeSingleQuoteString(s)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn iterate_datetimefield() {
        use DateTimeField::*;
        assert_eq!(
            Year.into_iter().take(10).collect::<Vec<_>>(),
            vec![Month, Day, Hour, Minute, Second]
        )
    }
}
