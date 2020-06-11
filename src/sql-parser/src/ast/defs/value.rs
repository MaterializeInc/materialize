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

use crate::ast::display::{self, AstDisplay, AstFormatter};

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
                f.write_node(&display::escape_single_quote_string(v));
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
pub enum DataType {
    /// Fixed-length character type e.g. CHAR(10)
    Char(Option<u64>),
    /// Variable-length character type e.g. VARCHAR(10)
    Varchar(Option<u64>),
    /// Uuid type
    Uuid,
    /// Large character object e.g. CLOB(1000)
    Clob(u64),
    /// Fixed-length binary type e.g. BINARY(10)
    Binary(u64),
    /// Variable-length binary type e.g. VARBINARY(10)
    Varbinary(u64),
    /// Large binary object e.g. BLOB(1000)
    Blob(u64),
    /// Decimal type with optional precision and scale e.g. DECIMAL(10,2)
    Decimal(Option<u64>, Option<u64>),
    /// Floating point with optional precision e.g. FLOAT(8)
    Float(Option<u64>),
    /// Small integer
    SmallInt,
    /// Integer
    Int,
    /// Big integer
    BigInt,
    /// Floating point e.g. REAL
    Real,
    /// Double e.g. DOUBLE PRECISION
    Double,
    /// Boolean
    Boolean,
    /// Date
    Date,
    /// Time without time zone
    Time,
    /// Time with time zone
    TimeTz,
    /// Timestamp without time zone
    Timestamp,
    /// Timestamp with time zone
    TimestampTz,
    /// Interval
    Interval,
    /// Regclass used in postgresql serial
    Regclass,
    /// Text
    Text,
    /// Bytea
    Bytea,
    /// List
    List(Box<DataType>),
    /// Binary JSON
    Jsonb,
}

impl AstDisplay for DataType {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            DataType::Char(size) => format_type_with_optional_length(f, "char", size),
            DataType::Varchar(size) => {
                format_type_with_optional_length(f, "character varying", size)
            }
            DataType::Uuid => f.write_str("uuid"),
            DataType::Clob(size) => {
                f.write_str("clob(");
                f.write_str(size);
                f.write_str(")");
            }
            DataType::Binary(size) => {
                f.write_str("binary(");
                f.write_str(size);
                f.write_str(")");
            }
            DataType::Varbinary(size) => {
                f.write_str("varbinary(");
                f.write_str(size);
                f.write_str(")");
            }
            DataType::Blob(size) => {
                f.write_str("blob(");
                f.write_str(size);
                f.write_str(")");
            }
            DataType::Decimal(precision, scale) => {
                if let Some(scale) = scale {
                    f.write_str("numeric(");
                    f.write_str(precision.unwrap());
                    f.write_str(",");
                    f.write_str(scale);
                    f.write_str(")");
                } else {
                    format_type_with_optional_length(f, "numeric", precision)
                }
            }
            DataType::Float(size) => format_type_with_optional_length(f, "float", size),
            DataType::SmallInt => f.write_str("smallint"),
            DataType::Int => f.write_str("int"),
            DataType::BigInt => f.write_str("bigint"),
            DataType::Real => f.write_str("real"),
            DataType::Double => f.write_str("double"),
            DataType::Boolean => f.write_str("boolean"),
            DataType::Date => f.write_str("date"),
            DataType::Time => f.write_str("time"),
            DataType::TimeTz => f.write_str("time with time zone"),
            DataType::Timestamp => f.write_str("timestamp"),
            DataType::TimestampTz => f.write_str("timestamp with time zone"),
            DataType::Interval => f.write_str("interval"),
            DataType::Regclass => f.write_str("regclass"),
            DataType::Text => f.write_str("text"),
            DataType::Bytea => f.write_str("bytea"),
            DataType::List(ty) => {
                f.write_str("list(");
                f.write_node(&ty);
                f.write_str(")")
            }
            DataType::Jsonb => f.write_str("jsonb"),
        }
    }
}

fn format_type_with_optional_length(
    f: &mut AstFormatter,
    sql_type: &'static str,
    len: &Option<u64>,
) {
    f.write_str(sql_type);
    if let Some(len) = len {
        f.write_str("(");
        f.write_str(len);
        f.write_str(")");
    }
}
