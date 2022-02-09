// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::oid;
use anyhow::bail;
use lazy_static::lazy_static;
use repr::adt::numeric::NUMERIC_DATUM_MAX_PRECISION;
use repr::ScalarType;

/// The type of a [`Value`](crate::Value).
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Type {
    /// A variable-length multidimensional array of values.
    Array(Box<Type>),
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
    /// A 2-byte signed integer.
    Int2,
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
    /// A map with text keys and homogeneous values.
    Map {
        /// The type of the values in the map.
        value_type: Box<Type>,
    },
    /// An arbitrary precision number.
    Numeric {
        /// Number of digits after the decimal
        max_scale: u16,
        /// Number of overall digits, capped to 38 in Materialize
        max_precision: u16,
    },
    /// An object identifier.
    Oid,
    /// A sequence of heterogeneous values.
    Record(Vec<Type>),
    /// A variable-length string.
    Text,
    /// A fixed-length string.
    Char {
        ///
        length: Option<usize>,
    },
    /// A variable-length string with an optional limit.
    VarChar {
        /// Length of VarChar in characters
        max_length: Option<usize>,
    },
    /// A time of day without a day.
    Time,
    /// A date and time, without a timezone.
    Timestamp,
    /// A date and time, with a timezone.
    TimestampTz,
    /// A universally unique identifier.
    Uuid,
    /// A function name.
    RegProc,
    /// A type name.
    RegType,
    /// A class name.
    RegClass,
}

lazy_static! {
    /// An anonymous [`Type::List`].
    pub static ref LIST: postgres_types::Type = postgres_types::Type::new(
        "list".to_owned(),
        // OID chosen to be the first OID not considered stable by
        // PostgreSQL. See the "OID Assignment" section of the PostgreSQL
        // documentation for details:
        // https://www.postgresql.org/docs/current/system-catalog-initial-data.html#SYSTEM-CATALOG-OID-ASSIGNMENT
        oid::TYPE_LIST_OID,
        postgres_types::Kind::Pseudo,
        "mz_catalog".to_owned(),
    );

    /// An anonymous [`Type::Map`].
    pub static ref MAP: postgres_types::Type = postgres_types::Type::new(
        "map".to_owned(),
        // OID chosen to follow our "LIST" type.
        oid::TYPE_MAP_OID,
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
            postgres_types::Type::INT2 => Some(Type::Int2),
            postgres_types::Type::INT4 => Some(Type::Int4),
            postgres_types::Type::INT8 => Some(Type::Int8),
            postgres_types::Type::INTERVAL => Some(Type::Interval),
            postgres_types::Type::JSONB => Some(Type::Jsonb),
            postgres_types::Type::NUMERIC => Some(Type::Numeric {
                max_precision: NUMERIC_DATUM_MAX_PRECISION.try_into().unwrap(),
                max_scale: 0_u16,
            }),
            postgres_types::Type::OID => Some(Type::Oid),
            postgres_types::Type::TEXT => Some(Type::Text),
            postgres_types::Type::BPCHAR | postgres_types::Type::CHAR => {
                Some(Type::Char { length: None })
            }
            postgres_types::Type::VARCHAR => Some(Type::VarChar { max_length: None }),
            postgres_types::Type::TIME => Some(Type::Time),
            postgres_types::Type::TIMESTAMP => Some(Type::Timestamp),
            postgres_types::Type::TIMESTAMPTZ => Some(Type::TimestampTz),
            postgres_types::Type::UUID => Some(Type::Uuid),
            postgres_types::Type::REGCLASS => Some(Type::RegClass),
            postgres_types::Type::REGPROC => Some(Type::RegProc),
            postgres_types::Type::REGTYPE => Some(Type::RegType),
            postgres_types::Type::BOOL_ARRAY => Some(Type::Array(Box::new(Type::Bool))),
            postgres_types::Type::BYTEA_ARRAY => Some(Type::Array(Box::new(Type::Bytea))),
            postgres_types::Type::BPCHAR_ARRAY => {
                Some(Type::Array(Box::new(Type::Char { length: None })))
            }
            postgres_types::Type::DATE_ARRAY => Some(Type::Array(Box::new(Type::Date))),
            postgres_types::Type::FLOAT4_ARRAY => Some(Type::Array(Box::new(Type::Float4))),
            postgres_types::Type::FLOAT8_ARRAY => Some(Type::Array(Box::new(Type::Float8))),
            postgres_types::Type::INT2_ARRAY => Some(Type::Array(Box::new(Type::Int2))),
            postgres_types::Type::INT4_ARRAY => Some(Type::Array(Box::new(Type::Int4))),
            postgres_types::Type::INT8_ARRAY => Some(Type::Array(Box::new(Type::Int8))),
            postgres_types::Type::INTERVAL_ARRAY => Some(Type::Array(Box::new(Type::Interval))),
            postgres_types::Type::JSONB_ARRAY => Some(Type::Array(Box::new(Type::Jsonb))),
            postgres_types::Type::NUMERIC_ARRAY => Some(Type::Array(Box::new(Type::Numeric {
                max_precision: NUMERIC_DATUM_MAX_PRECISION.try_into().unwrap(),
                max_scale: 0_u16,
            }))),
            postgres_types::Type::OID_ARRAY => Some(Type::Array(Box::new(Type::Oid))),
            postgres_types::Type::TEXT_ARRAY => Some(Type::Array(Box::new(Type::Text))),
            postgres_types::Type::TIME_ARRAY => Some(Type::Array(Box::new(Type::Time))),
            postgres_types::Type::TIMESTAMP_ARRAY => Some(Type::Array(Box::new(Type::Timestamp))),
            postgres_types::Type::TIMESTAMPTZ_ARRAY => {
                Some(Type::Array(Box::new(Type::TimestampTz)))
            }
            postgres_types::Type::UUID_ARRAY => Some(Type::Array(Box::new(Type::Uuid))),
            postgres_types::Type::VARCHAR_ARRAY => {
                Some(Type::Array(Box::new(Type::VarChar { max_length: None })))
            }
            postgres_types::Type::REGCLASS_ARRAY => Some(Type::Array(Box::new(Type::RegClass))),
            postgres_types::Type::REGPROC_ARRAY => Some(Type::Array(Box::new(Type::RegProc))),
            postgres_types::Type::REGTYPE_ARRAY => Some(Type::Array(Box::new(Type::RegType))),
            _ => None,
        }
    }
    /// Returns the `Type` corresponding to the oid, with applicable typ_mod applied
    /// returns errors for unknown type OIDs and invalid typ_mods
    pub fn from_oids(oid: u32, typ_mod: u64) -> Result<Type, anyhow::Error> {
        let ty = match postgres_types::Type::from_oid(oid) {
            Some(t) => t,
            None => bail!("unknown OID: {oid}"),
        };
        Ok(match ty {
            postgres_types::Type::BOOL => Type::Bool,
            postgres_types::Type::BYTEA => Type::Bytea,
            postgres_types::Type::DATE => Type::Date,
            postgres_types::Type::FLOAT4 => Type::Float4,
            postgres_types::Type::FLOAT8 => Type::Float8,
            postgres_types::Type::INT2 => Type::Int2,
            postgres_types::Type::INT4 => Type::Int4,
            postgres_types::Type::INT8 => Type::Int8,
            postgres_types::Type::INTERVAL => Type::Interval,
            postgres_types::Type::JSONB => Type::Jsonb,
            postgres_types::Type::NUMERIC => Type::Numeric {
                max_precision: NUMERIC_DATUM_MAX_PRECISION.try_into().unwrap(),
                max_scale: match repr::adt::numeric::extract_typ_mod(&[typ_mod])? {
                    Some(s) => s.into(),
                    None => 0_u16,
                },
            },
            postgres_types::Type::OID => Type::Oid,
            postgres_types::Type::TEXT => Type::Text,
            postgres_types::Type::BPCHAR | postgres_types::Type::CHAR => Type::Char {
                length: repr::adt::char::extract_typ_mod(&[typ_mod])?,
            },
            postgres_types::Type::VARCHAR => Type::VarChar {
                max_length: repr::adt::varchar::extract_typ_mod(&[typ_mod])?,
            },
            postgres_types::Type::TIME => Type::Time,
            postgres_types::Type::TIMESTAMP => Type::Timestamp,
            postgres_types::Type::TIMESTAMPTZ => Type::TimestampTz,
            postgres_types::Type::UUID => Type::Uuid,
            postgres_types::Type::REGCLASS => Type::RegClass,
            postgres_types::Type::REGPROC => Type::RegProc,
            postgres_types::Type::REGTYPE => Type::RegType,
            postgres_types::Type::BOOL_ARRAY => Type::Array(Box::new(Type::Bool)),
            postgres_types::Type::BYTEA_ARRAY => Type::Array(Box::new(Type::Bytea)),
            postgres_types::Type::BPCHAR_ARRAY => Type::Array(Box::new(Type::Char {
                length: repr::adt::char::extract_typ_mod(&[typ_mod])?,
            })),
            postgres_types::Type::DATE_ARRAY => Type::Array(Box::new(Type::Date)),
            postgres_types::Type::FLOAT4_ARRAY => Type::Array(Box::new(Type::Float4)),
            postgres_types::Type::FLOAT8_ARRAY => Type::Array(Box::new(Type::Float8)),
            postgres_types::Type::INT2_ARRAY => Type::Array(Box::new(Type::Int2)),
            postgres_types::Type::INT4_ARRAY => Type::Array(Box::new(Type::Int4)),
            postgres_types::Type::INT8_ARRAY => Type::Array(Box::new(Type::Int8)),
            postgres_types::Type::INTERVAL_ARRAY => Type::Array(Box::new(Type::Interval)),
            postgres_types::Type::JSONB_ARRAY => Type::Array(Box::new(Type::Jsonb)),
            postgres_types::Type::NUMERIC_ARRAY => Type::Array(Box::new(Type::Numeric {
                max_precision: NUMERIC_DATUM_MAX_PRECISION.try_into().unwrap(),
                max_scale: match repr::adt::numeric::extract_typ_mod(&[typ_mod])? {
                    Some(s) => s.into(),
                    None => 0_u16,
                },
            })),
            postgres_types::Type::OID_ARRAY => Type::Array(Box::new(Type::Oid)),
            postgres_types::Type::TEXT_ARRAY => Type::Array(Box::new(Type::Text)),
            postgres_types::Type::TIME_ARRAY => Type::Array(Box::new(Type::Time)),
            postgres_types::Type::TIMESTAMP_ARRAY => Type::Array(Box::new(Type::Timestamp)),
            postgres_types::Type::TIMESTAMPTZ_ARRAY => Type::Array(Box::new(Type::TimestampTz)),
            postgres_types::Type::UUID_ARRAY => Type::Array(Box::new(Type::Uuid)),
            postgres_types::Type::VARCHAR_ARRAY => Type::Array(Box::new(Type::VarChar {
                max_length: repr::adt::varchar::extract_typ_mod(&[typ_mod])?,
            })),
            postgres_types::Type::REGCLASS_ARRAY => Type::Array(Box::new(Type::RegClass)),
            postgres_types::Type::REGPROC_ARRAY => Type::Array(Box::new(Type::RegProc)),
            postgres_types::Type::REGTYPE_ARRAY => Type::Array(Box::new(Type::RegType)),
            _ => bail!("Unknown OID: {oid}"),
        })
    }
    pub(crate) fn inner(&self) -> &'static postgres_types::Type {
        match self {
            Type::Array(t) => match &**t {
                Type::Array(_) => unreachable!(),
                Type::Bool => &postgres_types::Type::BOOL_ARRAY,
                Type::Bytea => &postgres_types::Type::BYTEA_ARRAY,
                Type::Date => &postgres_types::Type::DATE_ARRAY,
                Type::Float4 => &postgres_types::Type::FLOAT4_ARRAY,
                Type::Float8 => &postgres_types::Type::FLOAT8_ARRAY,
                Type::Int2 => &postgres_types::Type::INT2_ARRAY,
                Type::Int4 => &postgres_types::Type::INT4_ARRAY,
                Type::Int8 => &postgres_types::Type::INT8_ARRAY,
                Type::Interval => &postgres_types::Type::INTERVAL_ARRAY,
                Type::Jsonb => &postgres_types::Type::JSONB_ARRAY,
                Type::List(_) => unreachable!(),
                Type::Map { .. } => unreachable!(),
                Type::Numeric { .. } => &postgres_types::Type::NUMERIC_ARRAY,
                Type::Oid => &postgres_types::Type::OID_ARRAY,
                Type::Record(_) => &postgres_types::Type::RECORD_ARRAY,
                Type::Text => &postgres_types::Type::TEXT_ARRAY,
                Type::Char { .. } => &postgres_types::Type::BPCHAR_ARRAY,
                Type::VarChar { .. } => &postgres_types::Type::VARCHAR_ARRAY,
                Type::Time => &postgres_types::Type::TIME_ARRAY,
                Type::Timestamp => &postgres_types::Type::TIMESTAMP_ARRAY,
                Type::TimestampTz => &postgres_types::Type::TIMESTAMPTZ_ARRAY,
                Type::Uuid => &postgres_types::Type::UUID_ARRAY,
                Type::RegClass => &postgres_types::Type::REGCLASS_ARRAY,
                Type::RegProc => &postgres_types::Type::REGPROC_ARRAY,
                Type::RegType => &postgres_types::Type::REGTYPE_ARRAY,
            },
            Type::Bool => &postgres_types::Type::BOOL,
            Type::Bytea => &postgres_types::Type::BYTEA,
            Type::Date => &postgres_types::Type::DATE,
            Type::Float4 => &postgres_types::Type::FLOAT4,
            Type::Float8 => &postgres_types::Type::FLOAT8,
            Type::Int2 => &postgres_types::Type::INT2,
            Type::Int4 => &postgres_types::Type::INT4,
            Type::Int8 => &postgres_types::Type::INT8,
            Type::Interval => &postgres_types::Type::INTERVAL,
            Type::Jsonb => &postgres_types::Type::JSONB,
            Type::List(_) => &LIST,
            Type::Map { .. } => &MAP,
            Type::Numeric { .. } => &postgres_types::Type::NUMERIC,
            Type::Oid => &postgres_types::Type::OID,
            Type::Record(_) => &postgres_types::Type::RECORD,
            Type::Text => &postgres_types::Type::TEXT,
            Type::Char { .. } => &postgres_types::Type::BPCHAR,
            Type::VarChar { .. } => &postgres_types::Type::VARCHAR,
            Type::Time => &postgres_types::Type::TIME,
            Type::Timestamp => &postgres_types::Type::TIMESTAMP,
            Type::TimestampTz => &postgres_types::Type::TIMESTAMPTZ,
            Type::Uuid => &postgres_types::Type::UUID,
            Type::RegClass => &postgres_types::Type::REGCLASS,
            Type::RegProc => &postgres_types::Type::REGPROC,
            Type::RegType => &postgres_types::Type::REGTYPE,
        }
    }

    /// Returns the name that PostgreSQL uses for this type.
    pub fn name(&self) -> &'static str {
        // postgres_types' `name()` uses the pg_catalog name, and not the pretty
        // SQL standard name.
        match self.inner() {
            &postgres_types::Type::BOOL_ARRAY => "boolean[]",
            &postgres_types::Type::BYTEA_ARRAY => "bytea[]",
            &postgres_types::Type::BPCHAR_ARRAY => "character[]",
            &postgres_types::Type::DATE_ARRAY => "date[]",
            &postgres_types::Type::FLOAT4_ARRAY => "real[]",
            &postgres_types::Type::FLOAT8_ARRAY => "double precision[]",
            &postgres_types::Type::INT2_ARRAY => "smallint[]",
            &postgres_types::Type::INT4_ARRAY => "integer[]",
            &postgres_types::Type::INT8_ARRAY => "bigint[]",
            &postgres_types::Type::INTERVAL_ARRAY => "interval[]",
            &postgres_types::Type::JSONB_ARRAY => "jsonb[]",
            &postgres_types::Type::NUMERIC_ARRAY => "numeric[]",
            &postgres_types::Type::OID_ARRAY => "oid[]",
            &postgres_types::Type::RECORD_ARRAY => "record[]",
            &postgres_types::Type::TEXT_ARRAY => "text[]",
            &postgres_types::Type::TIME_ARRAY => "time[]",
            &postgres_types::Type::TIMESTAMP_ARRAY => "timestamp[]",
            &postgres_types::Type::TIMESTAMPTZ_ARRAY => "timestamp with time zone[]",
            &postgres_types::Type::UUID_ARRAY => "uuid[]",
            &postgres_types::Type::VARCHAR_ARRAY => "character varying[]",
            &postgres_types::Type::BOOL => "boolean",
            &postgres_types::Type::BPCHAR => "character",
            &postgres_types::Type::FLOAT4 => "real",
            &postgres_types::Type::FLOAT8 => "double precision",
            &postgres_types::Type::INT2 => "smallint",
            &postgres_types::Type::INT4 => "integer",
            &postgres_types::Type::INT8 => "bigint",
            &postgres_types::Type::TIMESTAMPTZ => "timestamp with time zone",
            &postgres_types::Type::VARCHAR => "character varying",
            &postgres_types::Type::REGCLASS_ARRAY => "regclass[]",
            &postgres_types::Type::REGPROC_ARRAY => "regproc[]",
            &postgres_types::Type::REGTYPE_ARRAY => "regtype[]",
            other => other.name(),
        }
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
            Type::Array(_) => -1,
            Type::Bool => 1,
            Type::Bytea => -1,
            Type::Date => 4,
            Type::Float4 => 4,
            Type::Float8 => 8,
            Type::Int2 => 2,
            Type::Int4 => 4,
            Type::Int8 => 8,
            Type::Interval => 16,
            Type::Jsonb => -1,
            Type::List(_) => -1,
            Type::Map { .. } => -1,
            Type::Numeric { .. } => -1,
            Type::Oid => 4,
            Type::Record(_) => -1,
            Type::Text => -1,
            Type::Char { .. } => -1,
            Type::VarChar { .. } => -1,
            Type::Time => 4,
            Type::Timestamp => 8,
            Type::TimestampTz => 8,
            Type::Uuid => 16,
            Type::RegClass => 4,
            Type::RegProc => 4,
            Type::RegType => 4,
        }
    }

    /// Provides a [`ScalarType`] from `self`, but without necessarily
    /// associating any meaningful values within the returned type.
    ///
    /// For example `Type::Numeric` returns `SScalarType::Numeric { scale: None }`,
    /// meaning that its scale might need values from elsewhere.
    pub fn to_scalar_type_lossy(&self) -> ScalarType {
        match self {
            Type::Array(t) => ScalarType::Array(Box::new(t.to_scalar_type_lossy())),
            Type::Bool => ScalarType::Bool,
            Type::Bytea => ScalarType::Bytes,
            Type::Date => ScalarType::Date,
            Type::Float4 => ScalarType::Float32,
            Type::Float8 => ScalarType::Float64,
            Type::Int2 => ScalarType::Int16,
            Type::Int4 => ScalarType::Int32,
            Type::Int8 => ScalarType::Int64,
            Type::Interval => ScalarType::Interval,
            Type::Jsonb => ScalarType::Jsonb,
            Type::List(t) => ScalarType::List {
                element_type: Box::new(t.to_scalar_type_lossy()),
                custom_oid: None,
            },
            Type::Map { value_type } => ScalarType::Map {
                value_type: Box::new(value_type.to_scalar_type_lossy()),
                custom_oid: None,
            },
            Type::Numeric { .. } => ScalarType::Numeric { scale: None },
            Type::Oid => ScalarType::Oid,
            Type::Record(_) => ScalarType::Record {
                fields: vec![],
                custom_oid: None,
                custom_name: None,
            },
            Type::Text => ScalarType::String,
            Type::Time => ScalarType::Time,
            Type::Char { .. } => ScalarType::Char { length: None },
            Type::VarChar { .. } => ScalarType::VarChar { length: None },
            Type::Timestamp => ScalarType::Timestamp,
            Type::TimestampTz => ScalarType::TimestampTz,
            Type::Uuid => ScalarType::Uuid,
            Type::RegClass => ScalarType::RegClass,
            Type::RegProc => ScalarType::RegProc,
            Type::RegType => ScalarType::RegType,
        }
    }
}

impl TryFrom<&Type> for ScalarType {
    type Error = anyhow::Error;
    fn try_from(typ: &Type) -> Result<ScalarType, anyhow::Error> {
        Ok(match typ {
            Type::Array(t) => ScalarType::Array(Box::new(TryFrom::try_from(&**t)?)),
            Type::Bool => ScalarType::Bool,
            Type::Bytea => ScalarType::Bytes,
            Type::Date => ScalarType::Date,
            Type::Float4 => ScalarType::Float32,
            Type::Float8 => ScalarType::Float64,
            Type::Int2 => ScalarType::Int16,
            Type::Int4 => ScalarType::Int32,
            Type::Int8 => ScalarType::Int64,
            Type::Interval => ScalarType::Interval,
            Type::Jsonb => ScalarType::Jsonb,
            Type::List(t) => ScalarType::List {
                element_type: Box::new(TryFrom::try_from(&**t)?),
                custom_oid: Some(t.oid()),
            },
            Type::Map { value_type } => ScalarType::Map {
                value_type: Box::new(TryFrom::try_from(&**value_type)?),
                custom_oid: Some(value_type.oid()),
            },
            Type::Numeric {
                max_scale,
                max_precision,
            } => {
                if max_precision > &(NUMERIC_DATUM_MAX_PRECISION.try_into().unwrap()) {
                    bail!("requested max_precision: {max_precision} exeeds Materialize limit: {NUMERIC_DATUM_MAX_PRECISION}")
                }

                ScalarType::Numeric {
                    scale: {
                        match u8::try_from(*max_scale) {
                            Ok(scale) => Some(scale),
                            Err(e) => bail!(
                                "requested max_scale: {max_scale} too wide for materialize: {e}"
                            ),
                        }
                    },
                }
            }
            Type::Oid => ScalarType::Oid,
            Type::Record(_) => ScalarType::Record {
                fields: vec![],
                custom_oid: None,
                custom_name: None,
            },
            Type::Text => ScalarType::String,
            Type::Time => ScalarType::Time,
            Type::Char { length } => ScalarType::Char { length: *length },
            Type::VarChar { max_length } => ScalarType::VarChar {
                length: *max_length,
            },
            Type::Timestamp => ScalarType::Timestamp,
            Type::TimestampTz => ScalarType::TimestampTz,
            Type::Uuid => ScalarType::Uuid,
            Type::RegClass => ScalarType::RegClass,
            Type::RegProc => ScalarType::RegProc,
            Type::RegType => ScalarType::RegType,
        })
    }
}

impl From<&ScalarType> for Type {
    fn from(typ: &ScalarType) -> Type {
        match typ {
            ScalarType::Array(t) => Type::Array(Box::new(From::from(&**t))),
            ScalarType::Bool => Type::Bool,
            ScalarType::Bytes => Type::Bytea,
            ScalarType::Date => Type::Date,
            ScalarType::Float64 => Type::Float8,
            ScalarType::Float32 => Type::Float4,
            ScalarType::Int16 => Type::Int2,
            ScalarType::Int32 => Type::Int4,
            ScalarType::Int64 => Type::Int8,
            ScalarType::Interval => Type::Interval,
            ScalarType::Jsonb => Type::Jsonb,
            ScalarType::List { element_type, .. } => {
                Type::List(Box::new(From::from(&**element_type)))
            }
            ScalarType::Map { value_type, .. } => Type::Map {
                value_type: Box::new(From::from(&**value_type)),
            },
            ScalarType::Oid => Type::Oid,
            ScalarType::Record { fields, .. } => Type::Record(
                fields
                    .iter()
                    .map(|(_name, ty)| Type::from(&ty.scalar_type))
                    .collect(),
            ),
            ScalarType::String => Type::Text,
            ScalarType::Char { length } => Type::Char { length: *length },
            ScalarType::VarChar { length } => Type::VarChar {
                max_length: *length,
            },
            ScalarType::Time => Type::Time,
            ScalarType::Timestamp => Type::Timestamp,
            ScalarType::TimestampTz => Type::TimestampTz,
            ScalarType::Uuid => Type::Uuid,
            ScalarType::Numeric { scale } => Type::Numeric {
                max_scale: match *scale {
                    Some(scale) => scale.into(),
                    None => 0,
                },
                max_precision: NUMERIC_DATUM_MAX_PRECISION.try_into().unwrap(),
            },
            ScalarType::RegClass => Type::RegClass,
            ScalarType::RegProc => Type::RegProc,
            ScalarType::RegType => Type::RegType,
        }
    }
}
