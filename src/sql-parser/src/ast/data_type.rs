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

use crate::ast::display::{AstDisplay, AstFormatter};

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
            DataType::Array(ty) => {
                f.write_node(&ty);
                f.write_str("[]")
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
