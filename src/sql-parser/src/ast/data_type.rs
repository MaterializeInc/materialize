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
    /// Arrays
    Array(Box<DataType>),
    /// Binary JSON
    Jsonb,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataType::Char(size) => format_type_with_optional_length(f, "char", size),
            DataType::Varchar(size) => {
                format_type_with_optional_length(f, "character varying", size)
            }
            DataType::Uuid => write!(f, "uuid"),
            DataType::Clob(size) => write!(f, "clob({})", size),
            DataType::Binary(size) => write!(f, "binary({})", size),
            DataType::Varbinary(size) => write!(f, "varbinary({})", size),
            DataType::Blob(size) => write!(f, "blob({})", size),
            DataType::Decimal(precision, scale) => {
                if let Some(scale) = scale {
                    write!(f, "numeric({},{})", precision.unwrap(), scale)
                } else {
                    format_type_with_optional_length(f, "numeric", precision)
                }
            }
            DataType::Float(size) => format_type_with_optional_length(f, "float", size),
            DataType::SmallInt => write!(f, "smallint"),
            DataType::Int => write!(f, "int"),
            DataType::BigInt => write!(f, "bigint"),
            DataType::Real => write!(f, "real"),
            DataType::Double => write!(f, "double"),
            DataType::Boolean => write!(f, "boolean"),
            DataType::Date => write!(f, "date"),
            DataType::Time => write!(f, "time"),
            DataType::TimeTz => write!(f, "time with time zone"),
            DataType::Timestamp => write!(f, "timestamp"),
            DataType::TimestampTz => write!(f, "timestamp with time zone"),
            DataType::Interval => write!(f, "interval"),
            DataType::Regclass => write!(f, "regclass"),
            DataType::Text => write!(f, "text"),
            DataType::Bytea => write!(f, "bytea"),
            DataType::Array(ty) => write!(f, "{}[]", ty),
            DataType::Jsonb => write!(f, "jsonb"),
        }
    }
}

fn format_type_with_optional_length(
    f: &mut fmt::Formatter,
    sql_type: &'static str,
    len: &Option<u64>,
) -> fmt::Result {
    write!(f, "{}", sql_type)?;
    if let Some(len) = len {
        write!(f, "({})", len)?;
    }
    Ok(())
}
