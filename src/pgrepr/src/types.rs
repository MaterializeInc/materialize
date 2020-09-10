// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use lazy_static::lazy_static;
use repr::ScalarType;

/// The type of a [`Value`](crate::Value).
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Type {
    /// A boolean value.
    Bool,
    /// A byte array, i.e., a variable-length binary string.
    Bytea,
    /// A date.
    Date,
    /// A 4-byte floating point number.
    Float4,
    /// An 8-byte floating point number.
    Float8,
    /// A 4-byte signed integer.
    Int4,
    /// An 8-byte signed integer.
    Int8,
    /// A time interval.
    Interval,
    /// A binary JSON blob.
    Jsonb,
    /// A sequence of homogeneous values.
    List(Box<Type>),
    /// An arbitrary precision number.
    Numeric,
    /// A sequence of heterogeneous values.
    Record(Vec<Type>),
    /// A variable-length string.
    Text,
    /// A time of day without a day.
    Time,
    /// A date and time, without a timezone.
    Timestamp,
    /// A date and time, with a timezone.
    TimestampTz,
    /// A universally unique identifier.
    Uuid,
}

lazy_static! {
    pub static ref LIST: postgres_types::Type = postgres_types::Type::new(
        "LIST".to_owned(),
        // OID chosen to be in the first OID not considered stable by
        // PostgreSQL. See the "OID Assignment" section of the PostgreSQL
        // documentation for details:
        // https://www.postgresql.org/docs/current/system-catalog-initial-data.html#SYSTEM-CATALOG-OID-ASSIGNMENT
        16_384,
        postgres_types::Kind::Pseudo,
        "mz_catalog".to_owned(),
    );
}

impl Type {
    /// Returns the type corresponding to the provided OID, if the OID is known.
    pub fn from_oid(oid: u32) -> Option<Type> {
        let ty = postgres_types::Type::from_oid(oid)?;
        match ty {
            postgres_types::Type::BOOL => Some(Type::Bool),
            postgres_types::Type::BYTEA => Some(Type::Bytea),
            postgres_types::Type::DATE => Some(Type::Date),
            postgres_types::Type::FLOAT4 => Some(Type::Float4),
            postgres_types::Type::FLOAT8 => Some(Type::Float8),
            postgres_types::Type::INT4 => Some(Type::Int4),
            postgres_types::Type::INT8 => Some(Type::Int8),
            postgres_types::Type::INTERVAL => Some(Type::Interval),
            postgres_types::Type::JSONB => Some(Type::Jsonb),
            postgres_types::Type::NUMERIC => Some(Type::Numeric),
            postgres_types::Type::TEXT | postgres_types::Type::VARCHAR => Some(Type::Text),
            postgres_types::Type::TIME => Some(Type::Time),
            postgres_types::Type::TIMESTAMP => Some(Type::Timestamp),
            postgres_types::Type::TIMESTAMPTZ => Some(Type::TimestampTz),
            postgres_types::Type::UUID => Some(Type::Uuid),
            _ => None,
        }
    }

    pub(crate) fn inner(&self) -> &'static postgres_types::Type {
        match self {
            Type::Bool => &postgres_types::Type::BOOL,
            Type::Bytea => &postgres_types::Type::BYTEA,
            Type::Date => &postgres_types::Type::DATE,
            Type::Float4 => &postgres_types::Type::FLOAT4,
            Type::Float8 => &postgres_types::Type::FLOAT8,
            Type::Int4 => &postgres_types::Type::INT4,
            Type::Int8 => &postgres_types::Type::INT8,
            Type::Interval => &postgres_types::Type::INTERVAL,
            Type::Jsonb => &postgres_types::Type::JSONB,
            Type::Numeric => &postgres_types::Type::NUMERIC,
            Type::Text => &postgres_types::Type::TEXT,
            Type::Time => &postgres_types::Type::TIME,
            Type::Timestamp => &postgres_types::Type::TIMESTAMP,
            Type::TimestampTz => &postgres_types::Type::TIMESTAMPTZ,
            Type::Uuid => &postgres_types::Type::UUID,
            Type::List(_) => &LIST,
            Type::Record(_) => &postgres_types::Type::RECORD,
        }
    }

    /// Returns the name that PostgreSQL uses for this type.
    pub fn name(&self) -> &'static str {
        self.inner().name()
    }

    /// Returns the [OID] of this type.
    ///
    /// [OID]: https://www.postgresql.org/docs/current/datatype-oid.html
    pub fn oid(&self) -> u32 {
        self.inner().oid()
    }

    /// Returns the number of bytes in the binary representation of this
    /// type, or -1 if the type has a variable-length representation.
    pub fn typlen(&self) -> i16 {
        match self {
            Type::Bool => 1,
            Type::Bytea => -1,
            Type::Date => 4,
            Type::Float4 => 4,
            Type::Float8 => 8,
            Type::Int4 => 4,
            Type::Int8 => 8,
            Type::Interval => 16,
            Type::Jsonb => -1,
            Type::Numeric => -1,
            Type::Text => -1,
            Type::Time => 4,
            Type::Timestamp => 8,
            Type::TimestampTz => 8,
            Type::Uuid => 16,
            Type::List(_) => -1,
            Type::Record(_) => -1,
        }
    }
}

impl From<&ScalarType> for Type {
    fn from(typ: &ScalarType) -> Type {
        match typ {
            ScalarType::Bool => Type::Bool,
            ScalarType::Int32 => Type::Int4,
            ScalarType::Int64 => Type::Int8,
            ScalarType::Float32 => Type::Float4,
            ScalarType::Float64 => Type::Float8,
            ScalarType::Decimal(_, _) => Type::Numeric,
            ScalarType::Date => Type::Date,
            ScalarType::Time => Type::Time,
            ScalarType::Timestamp => Type::Timestamp,
            ScalarType::TimestampTz => Type::TimestampTz,
            ScalarType::Interval => Type::Interval,
            ScalarType::Bytes => Type::Bytea,
            ScalarType::String => Type::Text,
            ScalarType::Jsonb => Type::Jsonb,
            ScalarType::Uuid => Type::Uuid,
            ScalarType::List(t) => Type::List(Box::new(From::from(&**t))),
            ScalarType::Record { fields } => {
                Type::Record(fields.iter().map(|(_name, ty)| Type::from(ty)).collect())
            }
        }
    }
}
