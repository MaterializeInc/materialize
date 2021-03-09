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

use repr::adt::datetime::DateTimeField;

use crate::ast::defs::AstInfo;
use crate::ast::display::{self, AstDisplay, AstFormatter};

#[derive(Debug)]
pub struct ValueError(pub(crate) String);

impl std::error::Error for ValueError {}

impl fmt::Display for ValueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Primitive SQL values.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    fn fmt(&self, f: &mut AstFormatter) {
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

/// SQL data types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType<T: AstInfo> {
    /// Array
    Array(Box<DataType<T>>),
    /// List
    List(Box<DataType<T>>),
    /// Map
    Map {
        key_type: Box<DataType<T>>,
        value_type: Box<DataType<T>>,
    },
    /// Types who don't embed other types, e.g. INT
    Other {
        name: T::ObjectName,
        /// Typ modifiers appended to the type name, e.g. `numeric(38,0)`.
        typ_mod: Vec<u64>,
    },
}

impl<T: AstInfo> AstDisplay for DataType<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            DataType::Array(ty) => {
                f.write_node(&ty);
                f.write_str("[]");
            }
            DataType::List(ty) => {
                f.write_node(&ty);
                f.write_str(" list");
            }
            DataType::Map {
                key_type,
                value_type,
            } => {
                f.write_str("map(");
                f.write_node(&key_type);
                f.write_str("=>");
                f.write_node(&value_type);
                f.write_str(")");
            }
            DataType::Other { name, typ_mod } => {
                f.write_node(name);
                if typ_mod.len() > 0 {
                    f.write_str("(");
                    f.write_node(&display::comma_separated(typ_mod));
                    f.write_str(")");
                }
            }
        }
    }
}
impl_display_t!(DataType);
