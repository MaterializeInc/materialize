// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The mapping from SQL types to arrow-udf type names.

use std::fmt;

use mz_repr::SqlScalarType;

/// An arrow-udf type, as it appears in signature strings.
///
/// Each variant also fixes the Arrow `DataType` the guest expects, which the
/// runtime's codec owns.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UdfType {
    Boolean,
    Int16,
    Int32,
    Int64,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    /// Decimal text in a `Utf8` column.
    Decimal,
    Date32,
    Time64,
    /// Microseconds since the Unix epoch, without a time zone.
    Timestamp,
    /// `MonthDayNano`.
    Interval,
    /// JSON text in a `Utf8` column.
    Json,
    String,
    Binary,
    List(Box<UdfType>),
}

impl UdfType {
    /// The arrow-udf type for a SQL type, ignoring type modifiers.
    ///
    /// `timestamp` and `timestamptz` both map to [`UdfType::Timestamp`], with
    /// `timestamptz` values in UTC. One-dimensional arrays and lists both map
    /// to [`UdfType::List`].
    pub fn from_sql(typ: &SqlScalarType) -> Result<Self, UnsupportedType> {
        Ok(match typ {
            SqlScalarType::Bool => UdfType::Boolean,
            SqlScalarType::Int16 => UdfType::Int16,
            SqlScalarType::Int32 => UdfType::Int32,
            SqlScalarType::Int64 => UdfType::Int64,
            SqlScalarType::UInt16 => UdfType::UInt16,
            SqlScalarType::UInt32 => UdfType::UInt32,
            SqlScalarType::UInt64 => UdfType::UInt64,
            SqlScalarType::Float32 => UdfType::Float32,
            SqlScalarType::Float64 => UdfType::Float64,
            SqlScalarType::Numeric { .. } => UdfType::Decimal,
            SqlScalarType::Date => UdfType::Date32,
            SqlScalarType::Time => UdfType::Time64,
            SqlScalarType::Timestamp { .. } | SqlScalarType::TimestampTz { .. } => {
                UdfType::Timestamp
            }
            SqlScalarType::Interval => UdfType::Interval,
            SqlScalarType::Jsonb => UdfType::Json,
            SqlScalarType::String | SqlScalarType::VarChar { .. } | SqlScalarType::Char { .. } => {
                UdfType::String
            }
            SqlScalarType::Bytes => UdfType::Binary,
            SqlScalarType::List { element_type, .. } => {
                UdfType::List(Box::new(Self::from_sql(element_type)?))
            }
            SqlScalarType::Array(element_type) => {
                UdfType::List(Box::new(Self::from_sql(element_type)?))
            }
            _ => return Err(UnsupportedType(typ.clone())),
        })
    }
}

impl UdfType {
    /// The SQL type a value of this type is presented as when a function is
    /// declared from its signature alone. Lists become one-dimensional
    /// arrays.
    pub fn default_sql_type(&self) -> SqlScalarType {
        match self {
            UdfType::Boolean => SqlScalarType::Bool,
            UdfType::Int16 => SqlScalarType::Int16,
            UdfType::Int32 => SqlScalarType::Int32,
            UdfType::Int64 => SqlScalarType::Int64,
            UdfType::UInt16 => SqlScalarType::UInt16,
            UdfType::UInt32 => SqlScalarType::UInt32,
            UdfType::UInt64 => SqlScalarType::UInt64,
            UdfType::Float32 => SqlScalarType::Float32,
            UdfType::Float64 => SqlScalarType::Float64,
            UdfType::Decimal => SqlScalarType::Numeric { max_scale: None },
            UdfType::Date32 => SqlScalarType::Date,
            UdfType::Time64 => SqlScalarType::Time,
            UdfType::Timestamp => SqlScalarType::Timestamp { precision: None },
            UdfType::Interval => SqlScalarType::Interval,
            UdfType::Json => SqlScalarType::Jsonb,
            UdfType::String => SqlScalarType::String,
            UdfType::Binary => SqlScalarType::Bytes,
            UdfType::List(elem) => SqlScalarType::Array(Box::new(elem.default_sql_type())),
        }
    }
}

impl std::str::FromStr for UdfType {
    type Err = String;

    /// Parses a type name as it appears in a signature string.
    fn from_str(s: &str) -> Result<Self, String> {
        if let Some(elem) = s.strip_suffix("[]") {
            return Ok(UdfType::List(Box::new(elem.parse()?)));
        }
        Ok(match s {
            "boolean" => UdfType::Boolean,
            "int16" => UdfType::Int16,
            "int32" => UdfType::Int32,
            "int64" => UdfType::Int64,
            "uint16" => UdfType::UInt16,
            "uint32" => UdfType::UInt32,
            "uint64" => UdfType::UInt64,
            "float32" => UdfType::Float32,
            "float64" => UdfType::Float64,
            "decimal" => UdfType::Decimal,
            "date32" => UdfType::Date32,
            "time64" => UdfType::Time64,
            "timestamp" => UdfType::Timestamp,
            "interval" => UdfType::Interval,
            "json" => UdfType::Json,
            "string" => UdfType::String,
            "binary" => UdfType::Binary,
            _ => return Err(format!("unsupported arrow-udf type {s:?}")),
        })
    }
}

impl fmt::Display for UdfType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            UdfType::Boolean => "boolean",
            UdfType::Int16 => "int16",
            UdfType::Int32 => "int32",
            UdfType::Int64 => "int64",
            UdfType::UInt16 => "uint16",
            UdfType::UInt32 => "uint32",
            UdfType::UInt64 => "uint64",
            UdfType::Float32 => "float32",
            UdfType::Float64 => "float64",
            UdfType::Decimal => "decimal",
            UdfType::Date32 => "date32",
            UdfType::Time64 => "time64",
            UdfType::Timestamp => "timestamp",
            UdfType::Interval => "interval",
            UdfType::Json => "json",
            UdfType::String => "string",
            UdfType::Binary => "binary",
            UdfType::List(elem) => return write!(f, "{elem}[]"),
        };
        f.write_str(name)
    }
}

/// A SQL type with no arrow-udf equivalent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsupportedType(pub SqlScalarType);

impl fmt::Display for UnsupportedType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "type {:?} is not supported by WebAssembly functions",
            self.0
        )
    }
}

impl std::error::Error for UnsupportedType {}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn list_names_nest() {
        let typ = SqlScalarType::List {
            element_type: Box::new(SqlScalarType::Array(Box::new(SqlScalarType::String))),
            custom_id: None,
        };
        assert_eq!(UdfType::from_sql(&typ).unwrap().to_string(), "string[][]");
    }

    #[mz_ore::test]
    fn unsupported_types_are_rejected() {
        assert!(UdfType::from_sql(&SqlScalarType::Uuid).is_err());
        assert!(UdfType::from_sql(&SqlScalarType::Oid).is_err());
    }
}
