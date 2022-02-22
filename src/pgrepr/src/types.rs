// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use anyhow::bail;
use lazy_static::lazy_static;

use mz_repr::adt::char::CharLength as AdtCharLength;
use mz_repr::adt::numeric::{NumericMaxScale, NUMERIC_DATUM_MAX_PRECISION};
use mz_repr::adt::varchar::VarCharMaxLength;
use mz_repr::ScalarType;

use crate::oid;

/// Mirror of PostgreSQL's [`VARHDRSZ`] constant.
///
/// [`VARHDRSZ`]: https://github.com/postgres/postgres/blob/REL_14_0/src/include/c.h#L627
const VARHDRSZ: i32 = 4;

/// The type of a [`Value`](crate::Value).
///
/// The [`Display`](fmt::Display) representation of a type is guaranteed to be
/// valid PostgreSQL syntax that names the type and any modifiers.
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
    /// A time interval
    Interval {
        /// Optional precision
        precision: Option<IntervalPrecision>,
    },
    /// A textual JSON blob.
    Json,
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
        /// Optional constraints on the type.
        constraints: Option<NumericConstraints>,
    },
    /// An object identifier.
    Oid,
    /// A sequence of heterogeneous values.
    Record(Vec<Type>),
    /// A variable-length string.
    Text,
    /// A (usually) fixed-length string.
    Char {
        /// The length of the string.
        ///
        /// If unspecified, the type represents a variable-length string.
        length: Option<CharLength>,
    },
    /// A variable-length string with an optional limit.
    VarChar {
        /// An optional maximum length to enforce, in characters.
        max_length: Option<CharLength>,
    },
    /// A time of day without a day.
    Time,
    /// A time with a time zone.
    TimeTz,
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

/// A precision associatd with [`Type::Interval`]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct IntervalPrecision(i32);

impl IntervalPrecision {
    fn from_typmod(typmod: i32) -> Option<IntervalPrecision> {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/varchar.c#L139
        if typmod >= VARHDRSZ {
            Some(IntervalPrecision(typmod - VARHDRSZ))
        } else {
            None
        }
    }

    fn into_typmod(self) -> i32 {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/varchar.c#L60-L65
        self.0 + VARHDRSZ
    }

    /// Consumes the newtype wrapper, returning the contents as an `i32`.
    pub fn into_i32(self) -> i32 {
        self.0
    }
}
/// A length associated with [`Type::Char`] and [`Type::VarChar`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct CharLength(i32);

impl CharLength {
    fn from_typmod(typmod: i32) -> Option<CharLength> {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/varchar.c#L139
        if typmod >= VARHDRSZ {
            Some(CharLength(typmod - VARHDRSZ))
        } else {
            None
        }
    }

    fn into_typmod(self) -> i32 {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/varchar.c#L60-L65
        self.0 + VARHDRSZ
    }

    /// Consumes the newtype wrapper, returning the contents as an `i32`.
    pub fn into_i32(self) -> i32 {
        self.0
    }
}

impl From<AdtCharLength> for CharLength {
    fn from(length: AdtCharLength) -> CharLength {
        // The `AdtCharLength` newtype wrapper ensures that the inner `u32` is
        // small enough to fit into an `i32` with room for `VARHDRSZ`.
        CharLength(i32::try_from(length.into_u32()).unwrap())
    }
}

impl From<VarCharMaxLength> for CharLength {
    fn from(length: VarCharMaxLength) -> CharLength {
        // The `VarCharMaxLength` newtype wrapper ensures that the inner `u32`
        // is small enough to fit into an `i32` with room for `VARHDRSZ`.
        CharLength(i32::try_from(length.into_u32()).unwrap())
    }
}

impl fmt::Display for CharLength {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/varchar.c#L77
        write!(f, "({})", self.0)
    }
}

/// Constraints on [`Type::Numeric`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct NumericConstraints {
    /// The maximum precision.
    max_precision: i32,
    /// The maximum scale.
    max_scale: i32,
}

impl NumericConstraints {
    fn from_typmod(typmod: i32) -> Option<NumericConstraints> {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/numeric.c#L829-L862
        if typmod >= VARHDRSZ {
            Some(NumericConstraints {
                max_precision: ((typmod - VARHDRSZ) >> 16) & 0xffff,
                max_scale: (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024,
            })
        } else {
            None
        }
    }

    fn into_typmod(self) -> i32 {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/numeric.c#L826
        ((self.max_precision << 16) | (self.max_scale & 0x7ff)) + VARHDRSZ
    }
}

impl fmt::Display for NumericConstraints {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/numeric.c#L1292-L1294
        write!(f, "({},{})", self.max_precision, self.max_scale)
    }
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
        Type::from_oid_and_typmod(oid, 0).ok()
    }

    /// Returns the `Type` corresponding to the provided OID and packed type
    /// modifier ("typmod").
    ///
    /// For details about typmods, see the [`typmod`](Type::typmod) method.
    ///
    /// # Errors
    ///
    /// Returns an error if the OID is unknown or if the typmod is invalid for
    /// the type.
    pub fn from_oid_and_typmod(oid: u32, typmod: i32) -> Result<Type, anyhow::Error> {
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
            postgres_types::Type::INTERVAL => Type::Interval {
                precision: IntervalPrecision::from_typmod(typmod),
            },
            postgres_types::Type::JSON => Type::Json,
            postgres_types::Type::JSONB => Type::Jsonb,
            postgres_types::Type::NUMERIC => Type::Numeric {
                constraints: NumericConstraints::from_typmod(typmod),
            },
            postgres_types::Type::OID => Type::Oid,
            postgres_types::Type::TEXT => Type::Text,
            postgres_types::Type::BPCHAR | postgres_types::Type::CHAR => Type::Char {
                length: CharLength::from_typmod(typmod),
            },
            postgres_types::Type::VARCHAR => Type::VarChar {
                max_length: CharLength::from_typmod(typmod),
            },
            postgres_types::Type::TIME => Type::Time,
            postgres_types::Type::TIMETZ => Type::TimeTz,
            postgres_types::Type::TIMESTAMP => Type::Timestamp,
            postgres_types::Type::TIMESTAMPTZ => Type::TimestampTz,
            postgres_types::Type::UUID => Type::Uuid,
            postgres_types::Type::REGCLASS => Type::RegClass,
            postgres_types::Type::REGPROC => Type::RegProc,
            postgres_types::Type::REGTYPE => Type::RegType,
            postgres_types::Type::BOOL_ARRAY => Type::Array(Box::new(Type::Bool)),
            postgres_types::Type::BYTEA_ARRAY => Type::Array(Box::new(Type::Bytea)),
            postgres_types::Type::BPCHAR_ARRAY => Type::Array(Box::new(Type::Char {
                length: CharLength::from_typmod(typmod),
            })),
            postgres_types::Type::DATE_ARRAY => Type::Array(Box::new(Type::Date)),
            postgres_types::Type::FLOAT4_ARRAY => Type::Array(Box::new(Type::Float4)),
            postgres_types::Type::FLOAT8_ARRAY => Type::Array(Box::new(Type::Float8)),
            postgres_types::Type::INT2_ARRAY => Type::Array(Box::new(Type::Int2)),
            postgres_types::Type::INT4_ARRAY => Type::Array(Box::new(Type::Int4)),
            postgres_types::Type::INT8_ARRAY => Type::Array(Box::new(Type::Int8)),
            postgres_types::Type::INTERVAL_ARRAY => Type::Array(Box::new(Type::Interval {
                precision: IntervalPrecision::from_typmod(typmod),
            })),
            postgres_types::Type::JSON_ARRAY => Type::Array(Box::new(Type::Json)),
            postgres_types::Type::JSONB_ARRAY => Type::Array(Box::new(Type::Jsonb)),
            postgres_types::Type::NUMERIC_ARRAY => Type::Array(Box::new(Type::Numeric {
                constraints: NumericConstraints::from_typmod(typmod),
            })),
            postgres_types::Type::OID_ARRAY => Type::Array(Box::new(Type::Oid)),
            postgres_types::Type::TEXT_ARRAY => Type::Array(Box::new(Type::Text)),
            postgres_types::Type::TIME_ARRAY => Type::Array(Box::new(Type::Time)),
            postgres_types::Type::TIMETZ_ARRAY => Type::Array(Box::new(Type::TimeTz)),
            postgres_types::Type::TIMESTAMP_ARRAY => Type::Array(Box::new(Type::Timestamp)),
            postgres_types::Type::TIMESTAMPTZ_ARRAY => Type::Array(Box::new(Type::TimestampTz)),
            postgres_types::Type::UUID_ARRAY => Type::Array(Box::new(Type::Uuid)),
            postgres_types::Type::VARCHAR_ARRAY => Type::Array(Box::new(Type::VarChar {
                max_length: CharLength::from_typmod(typmod),
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
                Type::Interval { .. } => &postgres_types::Type::INTERVAL_ARRAY,
                Type::Json => &postgres_types::Type::JSON_ARRAY,
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
                Type::TimeTz => &postgres_types::Type::TIMETZ_ARRAY,
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
            Type::Interval { .. } => &postgres_types::Type::INTERVAL,
            Type::Json => &postgres_types::Type::JSON,
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
            Type::TimeTz => &postgres_types::Type::TIMETZ,
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
            Type::Interval { .. } => 16,
            Type::Json => -1,
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
            Type::TimeTz => 4,
            Type::Timestamp => 8,
            Type::TimestampTz => 8,
            Type::Uuid => 16,
            Type::RegClass => 4,
            Type::RegProc => 4,
            Type::RegType => 4,
        }
    }

    /// Returns the packed type modifier ("typmod") for the type.
    ///
    /// The typmod is a 32-bit integer associated with the type that encodes
    /// optional constraints on the type. For example, the typmod on
    /// `Type::VarChar` encodes an optional constraint on the value's length.
    /// Most types are never associated with a typmod.
    ///
    /// Negative typmods indicate no constraint.
    pub fn typmod(&self) -> i32 {
        match self {
            Type::Numeric {
                constraints: Some(constraints),
            } => constraints.into_typmod(),
            Type::Char {
                length: Some(length),
            } => length.into_typmod(),
            Type::VarChar {
                max_length: Some(max_length),
            } => max_length.into_typmod(),
            Type::Interval {
                precision: Some(precision),
            } => precision.into_typmod(),
            Type::Array(_)
            | Type::Bool
            | Type::Bytea
            | Type::Char { length: None }
            | Type::Date
            | Type::Float4
            | Type::Float8
            | Type::Int2
            | Type::Int4
            | Type::Int8
            | Type::Interval { precision: None }
            | Type::Json
            | Type::Jsonb
            | Type::List(_)
            | Type::Map { .. }
            | Type::Numeric { constraints: None }
            | Type::Oid
            | Type::Record(_)
            | Type::RegClass
            | Type::RegProc
            | Type::RegType
            | Type::Text
            | Type::Time
            | Type::TimeTz
            | Type::Timestamp
            | Type::TimestampTz
            | Type::Uuid
            | Type::VarChar { max_length: None } => -1,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.inner().name())?;
        match self {
            Type::Char {
                length: Some(length),
            } => length.fmt(f),
            Type::VarChar {
                max_length: Some(max_length),
            } => max_length.fmt(f),
            Type::Numeric {
                constraints: Some(constraints),
            } => constraints.fmt(f),
            _ => Ok(()),
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
            // ScalarType::Interval hardcodes a precision of 6 for now
            Type::Interval { .. } => ScalarType::Interval,
            Type::Json => bail!("type json not supported"),
            Type::Jsonb => ScalarType::Jsonb,
            Type::List(t) => ScalarType::List {
                element_type: Box::new(TryFrom::try_from(&**t)?),
                custom_oid: Some(t.oid()),
            },
            Type::Map { value_type } => ScalarType::Map {
                value_type: Box::new(TryFrom::try_from(&**value_type)?),
                custom_oid: Some(value_type.oid()),
            },
            Type::Numeric { constraints } => {
                let max_scale = match constraints {
                    Some(constraints) => {
                        if constraints.max_precision > i32::from(NUMERIC_DATUM_MAX_PRECISION) {
                            bail!(
                                "precision for type numeric must be between 1 and {}",
                                NUMERIC_DATUM_MAX_PRECISION,
                            );
                        }
                        if constraints.max_scale > constraints.max_precision {
                            bail!(
                                "scale for type numeric must be between 0 and precision {}",
                                constraints.max_precision,
                            );
                        }
                        Some(NumericMaxScale::try_from(i64::from(constraints.max_scale))?)
                    }
                    None => None,
                };
                ScalarType::Numeric { max_scale }
            }
            Type::Oid => ScalarType::Oid,
            Type::Record(_) => ScalarType::Record {
                fields: vec![],
                custom_oid: None,
                custom_name: None,
            },
            Type::Text => ScalarType::String,
            Type::Time => ScalarType::Time,
            Type::TimeTz => bail!("type timetz not supported"),
            Type::Char { length } => ScalarType::Char {
                length: match length {
                    Some(length) => Some(AdtCharLength::try_from(i64::from(length.into_i32()))?),
                    None => None,
                },
            },
            Type::VarChar { max_length } => ScalarType::VarChar {
                max_length: match max_length {
                    Some(max_length) => Some(VarCharMaxLength::try_from(i64::from(
                        max_length.into_i32(),
                    ))?),
                    None => None,
                },
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
            // ScalarType::Interval hardcodes a precision of 6 for now
            ScalarType::Interval => Type::Interval {
                precision: Some(IntervalPrecision(6)),
            },
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
            ScalarType::Char { length } => Type::Char {
                length: (*length).map(CharLength::from),
            },
            ScalarType::VarChar { max_length } => Type::VarChar {
                max_length: (*max_length).map(CharLength::from),
            },
            ScalarType::Time => Type::Time,
            ScalarType::Timestamp => Type::Timestamp,
            ScalarType::TimestampTz => Type::TimestampTz,
            ScalarType::Uuid => Type::Uuid,
            ScalarType::Numeric { max_scale } => Type::Numeric {
                constraints: Some(NumericConstraints {
                    max_precision: i32::from(NUMERIC_DATUM_MAX_PRECISION),
                    max_scale: match max_scale {
                        Some(max_scale) => i32::from(max_scale.into_u8()),
                        None => i32::from(NUMERIC_DATUM_MAX_PRECISION),
                    },
                }),
            },
            ScalarType::RegClass => Type::RegClass,
            ScalarType::RegProc => Type::RegProc,
            ScalarType::RegType => Type::RegType,
        }
    }
}
