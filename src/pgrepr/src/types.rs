// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt;

use once_cell::sync::Lazy;

use mz_repr::adt::char::{CharLength as AdtCharLength, InvalidCharLengthError};
use mz_repr::adt::numeric::{
    InvalidNumericMaxScaleError, NumericMaxScale, NUMERIC_DATUM_MAX_PRECISION,
};
use mz_repr::adt::varchar::{InvalidVarCharMaxLengthError, VarCharMaxLength};
use mz_repr::ScalarType;

use crate::oid;

/// Mirror of PostgreSQL's [`VARHDRSZ`] constant.
///
/// [`VARHDRSZ`]: https://github.com/postgres/postgres/blob/REL_14_0/src/include/c.h#L627
const VARHDRSZ: i32 = 4;

/// Mirror of PostgreSQL's [`MAX_INTERVAL_PRECISION`] constant.
///
/// See: <https://github.com/postgres/postgres/blob/27b77ecf9/src/include/datatype/timestamp.h#L54>
const MAX_INTERVAL_PRECISION: i32 = 6;

/// Mirror of PostgreSQL's [`MAX_TIMESTAMP_PRECISION`] constant.
///
/// See: <https://github.com/postgres/postgres/blob/27b77ecf9/src/include/datatype/timestamp.h#L53>
const MAX_TIMESTAMP_PRECISION: i32 = 6;

/// Mirror of PostgreSQL's [`MAX_TIME_PRECISION`] constant.
///
/// See: <https://github.com/postgres/postgres/blob/27b77ecf9/src/include/utils/date.h#L51>
const MAX_TIME_PRECISION: i32 = 6;

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
    /// A single-byte character.
    Char,
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
    /// A 2-byte unsigned integer. This does not exist in PostgreSQL.
    UInt2,
    /// A 4-byte unsigned integer. This does not exist in PostgreSQL.
    UInt4,
    /// An 8-byte unsigned integer. This does not exist in PostgreSQL.
    UInt8,
    /// A time interval.
    Interval {
        /// Optional constraints on the type.
        constraints: Option<IntervalConstraints>,
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
    BpChar {
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
    Time {
        /// An optional precision for the fractional digits in the second field.
        precision: Option<TimePrecision>,
    },
    /// A time with a time zone.
    TimeTz {
        /// An optional precision for the fractional digits in the second field.
        precision: Option<TimePrecision>,
    },
    /// A date and time, without a timezone.
    Timestamp {
        /// An optional precision for the fractional digits in the second field.
        precision: Option<TimestampPrecision>,
    },
    /// A date and time, with a timezone.
    TimestampTz {
        /// An optional precision for the fractional digits in the second field.
        precision: Option<TimestampPrecision>,
    },
    /// A universally unique identifier.
    Uuid,
    /// A function name.
    RegProc,
    /// A type name.
    RegType,
    /// A class name.
    RegClass,
    /// A small int vector.
    Int2Vector,
}

/// An unpacked [`typmod`](Type::typmod) for a [`Type`].
pub trait TypeConstraint: fmt::Display {
    /// Unpacks the type constraint from a typmod value.
    fn from_typmod(typmod: i32) -> Result<Option<Self>, String>
    where
        Self: Sized;

    /// Packs the type constraint into a typmod value.
    fn into_typmod(&self) -> i32;
}

/// A length associated with [`Type::Char`] and [`Type::VarChar`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct CharLength(i32);

impl TypeConstraint for CharLength {
    fn from_typmod(typmod: i32) -> Result<Option<CharLength>, String> {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/varchar.c#L139
        if typmod >= VARHDRSZ {
            Ok(Some(CharLength(typmod - VARHDRSZ)))
        } else {
            Ok(None)
        }
    }

    fn into_typmod(&self) -> i32 {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/varchar.c#L60-L65
        self.0 + VARHDRSZ
    }
}

impl CharLength {
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

/// Constraints associated with [`Type::Interval`]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct IntervalConstraints {
    /// The range of the interval.
    range: i32,
    /// The precision of the interval.
    precision: i32,
}

impl TypeConstraint for IntervalConstraints {
    fn from_typmod(typmod: i32) -> Result<Option<IntervalConstraints>, String> {
        if typmod < 0 {
            Ok(None)
        } else {
            // https://github.com/postgres/postgres/blob/27b77ecf9/src/include/utils/timestamp.h#L53-L54
            let range = typmod >> 16 & 0x7fff;
            let precision = typmod & 0xffff;
            if precision > MAX_INTERVAL_PRECISION {
                return Err(format!(
                    "exceeds maximum interval precision {MAX_INTERVAL_PRECISION}"
                ));
            }
            Ok(Some(IntervalConstraints { range, precision }))
        }
    }

    fn into_typmod(&self) -> i32 {
        (self.range << 16) | self.precision
    }
}

impl fmt::Display for IntervalConstraints {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // https://github.com/postgres/postgres/blob/27b77ecf9/src/include/utils/timestamp.h#L52
        // TODO: handle output of range.
        write!(f, "({})", self.precision)
    }
}

/// A precision associated with [`Type::Time`] and [`Type::TimeTz`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TimePrecision(i32);

impl TypeConstraint for TimePrecision {
    fn from_typmod(typmod: i32) -> Result<Option<TimePrecision>, String> {
        if typmod > MAX_TIME_PRECISION {
            Err(format!(
                "exceeds maximum time precision {MAX_TIME_PRECISION}"
            ))
        } else if typmod >= 0 {
            Ok(Some(TimePrecision(typmod)))
        } else {
            Ok(None)
        }
    }

    fn into_typmod(&self) -> i32 {
        self.0
    }
}

impl fmt::Display for TimePrecision {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // https://github.com/postgres/postgres/blob/27b77ecf9/src/backend/utils/adt/date.c#L97
        write!(f, "({})", self.0)
    }
}

/// A precision associated with [`Type::Timestamp`] and [`Type::TimestampTz`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TimestampPrecision(i32);

impl TypeConstraint for TimestampPrecision {
    fn from_typmod(typmod: i32) -> Result<Option<TimestampPrecision>, String> {
        if typmod > MAX_TIMESTAMP_PRECISION {
            Err(format!(
                "exceeds maximum timestamp precision {MAX_TIMESTAMP_PRECISION}"
            ))
        } else if typmod >= 0 {
            Ok(Some(TimestampPrecision(typmod)))
        } else {
            Ok(None)
        }
    }

    fn into_typmod(&self) -> i32 {
        self.0
    }
}

impl fmt::Display for TimestampPrecision {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // https://github.com/postgres/postgres/blob/54bd1e43c/src/backend/utils/adt/timestamp.c#L131
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

impl TypeConstraint for NumericConstraints {
    fn from_typmod(typmod: i32) -> Result<Option<NumericConstraints>, String> {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/numeric.c#L829-L862
        if typmod >= VARHDRSZ {
            Ok(Some(NumericConstraints {
                max_precision: ((typmod - VARHDRSZ) >> 16) & 0xffff,
                max_scale: (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024,
            }))
        } else {
            Ok(None)
        }
    }

    fn into_typmod(&self) -> i32 {
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

/// An anonymous [`Type::List`], akin to [`postgres_types::Type::ANYARRAY`].
pub static LIST: Lazy<postgres_types::Type> = Lazy::new(|| {
    postgres_types::Type::new(
        "list".to_owned(),
        // OID chosen to be the first OID not considered stable by
        // PostgreSQL. See the "OID Assignment" section of the PostgreSQL
        // documentation for details:
        // https://www.postgresql.org/docs/current/system-catalog-initial-data.html#SYSTEM-CATALOG-OID-ASSIGNMENT
        oid::TYPE_LIST_OID,
        postgres_types::Kind::Pseudo,
        "mz_catalog".to_owned(),
    )
});

/// An anonymous [`Type::Map`], akin to [`postgres_types::Type::ANYARRAY`].
pub static MAP: Lazy<postgres_types::Type> = Lazy::new(|| {
    postgres_types::Type::new(
        "map".to_owned(),
        // OID chosen to follow our "LIST" type.
        oid::TYPE_MAP_OID,
        postgres_types::Kind::Pseudo,
        "mz_catalog".to_owned(),
    )
});

/// An anonymous [`Type::List`], akin to [`postgres_types::Type::ANYCOMPATIBLEARRAY`].
pub static ANYCOMPATIBLELIST: Lazy<postgres_types::Type> = Lazy::new(|| {
    postgres_types::Type::new(
        "anycompatiblelist".to_owned(),
        oid::TYPE_ANYCOMPATIBLELIST_OID,
        postgres_types::Kind::Pseudo,
        "mz_catalog".to_owned(),
    )
});

/// An anonymous [`Type::Map`], akin to [`postgres_types::Type::ANYCOMPATIBLEARRAY`].
pub static ANYCOMPATIBLEMAP: Lazy<postgres_types::Type> = Lazy::new(|| {
    postgres_types::Type::new(
        "anycompatiblemap".to_owned(),
        oid::TYPE_ANYCOMPATIBLEMAP_OID,
        postgres_types::Kind::Pseudo,
        "mz_catalog".to_owned(),
    )
});

/// An anonymous [`Type::UInt2`], akin to [`postgres_types::Type::INT2`].
pub static UINT2: Lazy<postgres_types::Type> = Lazy::new(|| {
    postgres_types::Type::new(
        "uint2".to_owned(),
        oid::TYPE_UINT2_OID,
        postgres_types::Kind::Pseudo,
        "mz_catalog".to_owned(),
    )
});

/// An anonymous [`Type::UInt4`], akin to [`postgres_types::Type::INT4`].
pub static UINT4: Lazy<postgres_types::Type> = Lazy::new(|| {
    postgres_types::Type::new(
        "uint4".to_owned(),
        oid::TYPE_UINT4_OID,
        postgres_types::Kind::Pseudo,
        "mz_catalog".to_owned(),
    )
});

/// An anonymous [`Type::UInt8`], akin to [`postgres_types::Type::INT8`].
pub static UINT8: Lazy<postgres_types::Type> = Lazy::new(|| {
    postgres_types::Type::new(
        "uint8".to_owned(),
        oid::TYPE_UINT8_OID,
        postgres_types::Kind::Pseudo,
        "mz_catalog".to_owned(),
    )
});

/// An anonymous [`Type::Array`], akin to [`postgres_types::Type::INT2_ARRAY`].
pub static UINT2_ARRAY: Lazy<postgres_types::Type> = Lazy::new(|| {
    postgres_types::Type::new(
        "uint2_array".to_owned(),
        oid::TYPE_UINT2_ARRAY_OID,
        postgres_types::Kind::Pseudo,
        "mz_catalog".to_owned(),
    )
});

/// An anonymous [`Type::Array`], akin to [`postgres_types::Type::INT4_ARRAY`].
pub static UINT4_ARRAY: Lazy<postgres_types::Type> = Lazy::new(|| {
    postgres_types::Type::new(
        "uint4_array".to_owned(),
        oid::TYPE_UINT4_ARRAY_OID,
        postgres_types::Kind::Pseudo,
        "mz_catalog".to_owned(),
    )
});

/// An anonymous [`Type::Array`], akin to [`postgres_types::Type::INT8_ARRAY`].
pub static UINT8_ARRAY: Lazy<postgres_types::Type> = Lazy::new(|| {
    postgres_types::Type::new(
        "uint8_array".to_owned(),
        oid::TYPE_UINT8_ARRAY_OID,
        postgres_types::Kind::Pseudo,
        "mz_catalog".to_owned(),
    )
});

impl Type {
    /// Returns the type corresponding to the provided OID, if the OID is known.
    pub fn from_oid(oid: u32) -> Result<Type, TypeFromOidError> {
        Type::from_oid_and_typmod(oid, -1)
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
    pub fn from_oid_and_typmod(oid: u32, typmod: i32) -> Result<Type, TypeFromOidError> {
        let typ = postgres_types::Type::from_oid(oid).ok_or(TypeFromOidError::UnknownOid(oid))?;
        let mut typ = match typ {
            postgres_types::Type::BOOL => Type::Bool,
            postgres_types::Type::BYTEA => Type::Bytea,
            postgres_types::Type::DATE => Type::Date,
            postgres_types::Type::FLOAT4 => Type::Float4,
            postgres_types::Type::FLOAT8 => Type::Float8,
            postgres_types::Type::INT2 => Type::Int2,
            postgres_types::Type::INT4 => Type::Int4,
            postgres_types::Type::INT8 => Type::Int8,
            postgres_types::Type::INTERVAL => Type::Interval { constraints: None },
            postgres_types::Type::JSON => Type::Json,
            postgres_types::Type::JSONB => Type::Jsonb,
            postgres_types::Type::NUMERIC => Type::Numeric { constraints: None },
            postgres_types::Type::OID => Type::Oid,
            postgres_types::Type::TEXT => Type::Text,
            postgres_types::Type::BPCHAR | postgres_types::Type::CHAR => {
                Type::BpChar { length: None }
            }
            postgres_types::Type::VARCHAR => Type::VarChar { max_length: None },
            postgres_types::Type::TIME => Type::Time { precision: None },
            postgres_types::Type::TIMETZ => Type::TimeTz { precision: None },
            postgres_types::Type::TIMESTAMP => Type::Timestamp { precision: None },
            postgres_types::Type::TIMESTAMPTZ => Type::TimestampTz { precision: None },
            postgres_types::Type::UUID => Type::Uuid,
            postgres_types::Type::REGCLASS => Type::RegClass,
            postgres_types::Type::REGPROC => Type::RegProc,
            postgres_types::Type::REGTYPE => Type::RegType,
            postgres_types::Type::BOOL_ARRAY => Type::Array(Box::new(Type::Bool)),
            postgres_types::Type::BYTEA_ARRAY => Type::Array(Box::new(Type::Bytea)),
            postgres_types::Type::BPCHAR_ARRAY => {
                Type::Array(Box::new(Type::BpChar { length: None }))
            }
            postgres_types::Type::DATE_ARRAY => Type::Array(Box::new(Type::Date)),
            postgres_types::Type::FLOAT4_ARRAY => Type::Array(Box::new(Type::Float4)),
            postgres_types::Type::FLOAT8_ARRAY => Type::Array(Box::new(Type::Float8)),
            postgres_types::Type::INT2_ARRAY => Type::Array(Box::new(Type::Int2)),
            postgres_types::Type::INT4_ARRAY => Type::Array(Box::new(Type::Int4)),
            postgres_types::Type::INT8_ARRAY => Type::Array(Box::new(Type::Int8)),
            postgres_types::Type::INTERVAL_ARRAY => {
                Type::Array(Box::new(Type::Interval { constraints: None }))
            }
            postgres_types::Type::JSON_ARRAY => Type::Array(Box::new(Type::Json)),
            postgres_types::Type::JSONB_ARRAY => Type::Array(Box::new(Type::Jsonb)),
            postgres_types::Type::NUMERIC_ARRAY => {
                Type::Array(Box::new(Type::Numeric { constraints: None }))
            }
            postgres_types::Type::OID_ARRAY => Type::Array(Box::new(Type::Oid)),
            postgres_types::Type::TEXT_ARRAY => Type::Array(Box::new(Type::Text)),
            postgres_types::Type::TIME_ARRAY => {
                Type::Array(Box::new(Type::Time { precision: None }))
            }
            postgres_types::Type::TIMETZ_ARRAY => {
                Type::Array(Box::new(Type::TimeTz { precision: None }))
            }
            postgres_types::Type::TIMESTAMP_ARRAY => {
                Type::Array(Box::new(Type::Timestamp { precision: None }))
            }
            postgres_types::Type::TIMESTAMPTZ_ARRAY => {
                Type::Array(Box::new(Type::TimestampTz { precision: None }))
            }
            postgres_types::Type::UUID_ARRAY => Type::Array(Box::new(Type::Uuid)),
            postgres_types::Type::VARCHAR_ARRAY => {
                Type::Array(Box::new(Type::VarChar { max_length: None }))
            }
            postgres_types::Type::REGCLASS_ARRAY => Type::Array(Box::new(Type::RegClass)),
            postgres_types::Type::REGPROC_ARRAY => Type::Array(Box::new(Type::RegProc)),
            postgres_types::Type::REGTYPE_ARRAY => Type::Array(Box::new(Type::RegType)),
            postgres_types::Type::INT2_VECTOR => Type::Int2Vector,
            postgres_types::Type::INT2_VECTOR_ARRAY => Type::Array(Box::new(Type::Int2Vector)),
            _ => return Err(TypeFromOidError::UnknownOid(oid)),
        };

        // Apply the typmod. For arrays, the typmod applies to the element type.
        // We use a funny-looking immediately-invoked closure to share the
        // construction of the invalid typmod error across all error paths.
        let res = ({
            let typ = &mut typ;
            || {
                let elem_typ = match typ {
                    Type::Array(typ) => &mut **typ,
                    typ => typ,
                };
                match elem_typ {
                    Type::BpChar { length } => *length = CharLength::from_typmod(typmod)?,
                    Type::Numeric { constraints } => {
                        *constraints = NumericConstraints::from_typmod(typmod)?
                    }
                    Type::Interval { constraints } => {
                        *constraints = IntervalConstraints::from_typmod(typmod)?
                    }
                    Type::Time { precision } => *precision = TimePrecision::from_typmod(typmod)?,
                    Type::TimeTz { precision } => *precision = TimePrecision::from_typmod(typmod)?,
                    Type::Timestamp { precision } => {
                        *precision = TimestampPrecision::from_typmod(typmod)?
                    }
                    Type::TimestampTz { precision } => {
                        *precision = TimestampPrecision::from_typmod(typmod)?
                    }
                    Type::VarChar { max_length } => *max_length = CharLength::from_typmod(typmod)?,
                    _ if typmod != -1 => return Err("type does not support type modifiers".into()),
                    _ => (),
                }
                Ok(())
            }
        })();
        match res {
            Ok(()) => Ok(typ),
            Err(detail) => Err(TypeFromOidError::InvalidTypmod {
                typ,
                typmod,
                detail,
            }),
        }
    }

    pub(crate) fn inner(&self) -> &'static postgres_types::Type {
        match self {
            Type::Array(t) => match &**t {
                Type::Array(_) => unreachable!(),
                Type::Bool => &postgres_types::Type::BOOL_ARRAY,
                Type::Bytea => &postgres_types::Type::BYTEA_ARRAY,
                Type::Char => &postgres_types::Type::CHAR_ARRAY,
                Type::Date => &postgres_types::Type::DATE_ARRAY,
                Type::Float4 => &postgres_types::Type::FLOAT4_ARRAY,
                Type::Float8 => &postgres_types::Type::FLOAT8_ARRAY,
                Type::Int2 => &postgres_types::Type::INT2_ARRAY,
                Type::Int4 => &postgres_types::Type::INT4_ARRAY,
                Type::Int8 => &postgres_types::Type::INT8_ARRAY,
                Type::UInt2 => &UINT2_ARRAY,
                Type::UInt4 => &UINT4_ARRAY,
                Type::UInt8 => &UINT8_ARRAY,
                Type::Interval { .. } => &postgres_types::Type::INTERVAL_ARRAY,
                Type::Json => &postgres_types::Type::JSON_ARRAY,
                Type::Jsonb => &postgres_types::Type::JSONB_ARRAY,
                Type::List(_) => unreachable!(),
                Type::Map { .. } => unreachable!(),
                Type::Numeric { .. } => &postgres_types::Type::NUMERIC_ARRAY,
                Type::Oid => &postgres_types::Type::OID_ARRAY,
                Type::Record(_) => &postgres_types::Type::RECORD_ARRAY,
                Type::Text => &postgres_types::Type::TEXT_ARRAY,
                Type::BpChar { .. } => &postgres_types::Type::BPCHAR_ARRAY,
                Type::VarChar { .. } => &postgres_types::Type::VARCHAR_ARRAY,
                Type::Time { .. } => &postgres_types::Type::TIME_ARRAY,
                Type::TimeTz { .. } => &postgres_types::Type::TIMETZ_ARRAY,
                Type::Timestamp { .. } => &postgres_types::Type::TIMESTAMP_ARRAY,
                Type::TimestampTz { .. } => &postgres_types::Type::TIMESTAMPTZ_ARRAY,
                Type::Uuid => &postgres_types::Type::UUID_ARRAY,
                Type::RegClass => &postgres_types::Type::REGCLASS_ARRAY,
                Type::RegProc => &postgres_types::Type::REGPROC_ARRAY,
                Type::RegType => &postgres_types::Type::REGTYPE_ARRAY,
                Type::Int2Vector => &postgres_types::Type::INT2_VECTOR_ARRAY,
            },
            Type::Bool => &postgres_types::Type::BOOL,
            Type::Bytea => &postgres_types::Type::BYTEA,
            Type::Char => &postgres_types::Type::CHAR,
            Type::Date => &postgres_types::Type::DATE,
            Type::Float4 => &postgres_types::Type::FLOAT4,
            Type::Float8 => &postgres_types::Type::FLOAT8,
            Type::Int2 => &postgres_types::Type::INT2,
            Type::Int4 => &postgres_types::Type::INT4,
            Type::Int8 => &postgres_types::Type::INT8,
            Type::UInt2 => &UINT2,
            Type::UInt4 => &UINT4,
            Type::UInt8 => &UINT8,
            Type::Interval { .. } => &postgres_types::Type::INTERVAL,
            Type::Json => &postgres_types::Type::JSON,
            Type::Jsonb => &postgres_types::Type::JSONB,
            Type::List(_) => &LIST,
            Type::Map { .. } => &MAP,
            Type::Numeric { .. } => &postgres_types::Type::NUMERIC,
            Type::Oid => &postgres_types::Type::OID,
            Type::Record(_) => &postgres_types::Type::RECORD,
            Type::Text => &postgres_types::Type::TEXT,
            Type::BpChar { .. } => &postgres_types::Type::BPCHAR,
            Type::VarChar { .. } => &postgres_types::Type::VARCHAR,
            Type::Time { .. } => &postgres_types::Type::TIME,
            Type::TimeTz { .. } => &postgres_types::Type::TIMETZ,
            Type::Timestamp { .. } => &postgres_types::Type::TIMESTAMP,
            Type::TimestampTz { .. } => &postgres_types::Type::TIMESTAMPTZ,
            Type::Uuid => &postgres_types::Type::UUID,
            Type::RegClass => &postgres_types::Type::REGCLASS,
            Type::RegProc => &postgres_types::Type::REGPROC,
            Type::RegType => &postgres_types::Type::REGTYPE,
            Type::Int2Vector => &postgres_types::Type::INT2_VECTOR,
        }
    }

    /// Returns the item's name in a way that guarantees it's resolvable in the
    /// catalog.
    pub fn catalog_name(&self) -> &'static str {
        self.inner().name()
    }

    /// Returns the user-friendly name that PostgreSQL uses for this type.
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
            &postgres_types::Type::INT2_VECTOR => "int2vector",
            other => other.name(),
        }
    }

    /// Returns the [OID] of this type.
    ///
    /// [OID]: https://www.postgresql.org/docs/current/datatype-oid.html
    pub fn oid(&self) -> u32 {
        self.inner().oid()
    }

    /// Returns the constraint on the type, if any.
    pub fn constraint(&self) -> Option<&dyn TypeConstraint> {
        match self {
            Type::BpChar {
                length: Some(length),
            } => Some(length),
            Type::VarChar {
                max_length: Some(max_length),
            } => Some(max_length),
            Type::Numeric {
                constraints: Some(constraints),
            } => Some(constraints),
            Type::Interval {
                constraints: Some(constraints),
            } => Some(constraints),
            Type::Time {
                precision: Some(precision),
            } => Some(precision),
            Type::TimeTz {
                precision: Some(precision),
            } => Some(precision),
            Type::Timestamp {
                precision: Some(precision),
            } => Some(precision),
            Type::TimestampTz {
                precision: Some(precision),
            } => Some(precision),
            Type::Array(_)
            | Type::Bool
            | Type::Bytea
            | Type::BpChar { length: None }
            | Type::Char
            | Type::Date
            | Type::Float4
            | Type::Float8
            | Type::Int2
            | Type::Int4
            | Type::Int8
            | Type::UInt2
            | Type::UInt4
            | Type::UInt8
            | Type::Interval { constraints: None }
            | Type::Json
            | Type::Jsonb
            | Type::List(_)
            | Type::Map { .. }
            | Type::Numeric { constraints: None }
            | Type::Int2Vector
            | Type::Oid
            | Type::Record(_)
            | Type::RegClass
            | Type::RegProc
            | Type::RegType
            | Type::Text
            | Type::Time { precision: None }
            | Type::TimeTz { precision: None }
            | Type::Timestamp { precision: None }
            | Type::TimestampTz { precision: None }
            | Type::Uuid
            | Type::VarChar { max_length: None } => None,
        }
    }

    /// Returns the number of bytes in the binary representation of this
    /// type, or -1 if the type has a variable-length representation.
    pub fn typlen(&self) -> i16 {
        match self {
            Type::Array(_) => -1,
            Type::Bool => 1,
            Type::Bytea => -1,
            Type::Char => 1,
            Type::Date => 4,
            Type::Float4 => 4,
            Type::Float8 => 8,
            Type::Int2 => 2,
            Type::Int4 => 4,
            Type::Int8 => 8,
            Type::UInt2 => 2,
            Type::UInt4 => 4,
            Type::UInt8 => 8,
            Type::Interval { .. } => 16,
            Type::Json => -1,
            Type::Jsonb => -1,
            Type::List(_) => -1,
            Type::Map { .. } => -1,
            Type::Numeric { .. } => -1,
            Type::Oid => 4,
            Type::Record(_) => -1,
            Type::Text => -1,
            Type::BpChar { .. } => -1,
            Type::VarChar { .. } => -1,
            Type::Time { .. } => 4,
            Type::TimeTz { .. } => 4,
            Type::Timestamp { .. } => 8,
            Type::TimestampTz { .. } => 8,
            Type::Uuid => 16,
            Type::RegClass => 4,
            Type::RegProc => 4,
            Type::RegType => 4,
            Type::Int2Vector => -1,
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
        match self.constraint() {
            Some(constraint) => constraint.into_typmod(),
            None => -1,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.inner().name())?;
        if let Some(constraint) = self.constraint() {
            constraint.fmt(f)?;
        }
        Ok(())
    }
}

impl TryFrom<&Type> for ScalarType {
    type Error = TypeConversionError;

    fn try_from(typ: &Type) -> Result<ScalarType, TypeConversionError> {
        match typ {
            Type::Array(t) => Ok(ScalarType::Array(Box::new(TryFrom::try_from(&**t)?))),
            Type::Bool => Ok(ScalarType::Bool),
            Type::Bytea => Ok(ScalarType::Bytes),
            Type::Char => Ok(ScalarType::PgLegacyChar),
            Type::Date => Ok(ScalarType::Date),
            Type::Float4 => Ok(ScalarType::Float32),
            Type::Float8 => Ok(ScalarType::Float64),
            Type::Int2 => Ok(ScalarType::Int16),
            Type::Int4 => Ok(ScalarType::Int32),
            Type::Int8 => Ok(ScalarType::Int64),
            Type::UInt2 => Ok(ScalarType::UInt16),
            Type::UInt4 => Ok(ScalarType::UInt32),
            Type::UInt8 => Ok(ScalarType::UInt64),
            Type::Interval { .. } => Ok(ScalarType::Interval),
            Type::Json => Err(TypeConversionError::UnsupportedType(Type::Json)),
            Type::Jsonb => Ok(ScalarType::Jsonb),
            Type::List(t) => Ok(ScalarType::List {
                element_type: Box::new(TryFrom::try_from(&**t)?),
                custom_id: None,
            }),
            Type::Map { value_type } => Ok(ScalarType::Map {
                value_type: Box::new(TryFrom::try_from(&**value_type)?),
                custom_id: None,
            }),
            Type::Numeric { constraints } => {
                let max_scale = match constraints {
                    Some(constraints) => {
                        if constraints.max_precision > i32::from(NUMERIC_DATUM_MAX_PRECISION) {
                            return Err(TypeConversionError::InvalidNumericConstraint(format!(
                                "precision for type numeric must be between 1 and {}",
                                NUMERIC_DATUM_MAX_PRECISION,
                            )));
                        }
                        if constraints.max_scale > constraints.max_precision {
                            return Err(TypeConversionError::InvalidNumericConstraint(format!(
                                "scale for type numeric must be between 0 and precision {}",
                                constraints.max_precision,
                            )));
                        }
                        Some(NumericMaxScale::try_from(i64::from(constraints.max_scale))?)
                    }
                    None => None,
                };
                Ok(ScalarType::Numeric { max_scale })
            }
            Type::Oid => Ok(ScalarType::Oid),
            Type::Record(_) => Ok(ScalarType::Record {
                fields: vec![],
                custom_id: None,
            }),
            Type::Text => Ok(ScalarType::String),
            Type::Time { precision: None } => Ok(ScalarType::Time),
            Type::Time { precision: Some(_) } => {
                Err(TypeConversionError::UnsupportedType(typ.clone()))
            }
            Type::TimeTz { .. } => Err(TypeConversionError::UnsupportedType(typ.clone())),
            Type::BpChar { length } => Ok(ScalarType::Char {
                length: match length {
                    Some(length) => Some(AdtCharLength::try_from(i64::from(length.into_i32()))?),
                    None => None,
                },
            }),
            Type::VarChar { max_length } => Ok(ScalarType::VarChar {
                max_length: match max_length {
                    Some(max_length) => Some(VarCharMaxLength::try_from(i64::from(
                        max_length.into_i32(),
                    ))?),
                    None => None,
                },
            }),
            Type::Timestamp { precision: None } => Ok(ScalarType::Timestamp),
            Type::Timestamp { precision: Some(_) } => {
                Err(TypeConversionError::UnsupportedType(typ.clone()))
            }
            Type::TimestampTz { precision: None } => Ok(ScalarType::TimestampTz),
            Type::TimestampTz { precision: Some(_) } => {
                Err(TypeConversionError::UnsupportedType(typ.clone()))
            }
            Type::Uuid => Ok(ScalarType::Uuid),
            Type::RegClass => Ok(ScalarType::RegClass),
            Type::RegProc => Ok(ScalarType::RegProc),
            Type::RegType => Ok(ScalarType::RegType),
            Type::Int2Vector => Ok(ScalarType::Int2Vector),
        }
    }
}

/// An error that can occur when constructing a [`Type`] from an OID.
#[derive(Debug, Clone)]
pub enum TypeFromOidError {
    /// The OID does not specify a known type.
    UnknownOid(u32),
    /// The specified typmod is invalid for the type.
    InvalidTypmod {
        /// The type.
        typ: Type,
        /// The type modifier.
        typmod: i32,
        /// Details about the nature of the invalidity.
        detail: String,
    },
}

impl fmt::Display for TypeFromOidError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TypeFromOidError::UnknownOid(oid) => write!(f, "type with OID {oid} is unknown"),
            TypeFromOidError::InvalidTypmod {
                typ,
                typmod,
                detail,
            } => {
                write!(
                    f,
                    "typmod {typmod} is invalid for type {}: {detail}",
                    typ.name()
                )
            }
        }
    }
}

impl Error for TypeFromOidError {}

/// An error that can occur when converting a [`Type`] to a [`ScalarType`].
#[derive(Debug, Clone)]
pub enum TypeConversionError {
    /// The source type is unsupported as a `ScalarType`.
    UnsupportedType(Type),
    /// The source type contained an invalid max scale for a
    /// [`ScalarType::Numeric`].
    InvalidNumericMaxScale(InvalidNumericMaxScaleError),
    /// The source type contained an invalid constraint for a
    /// [`ScalarType::Numeric`].
    InvalidNumericConstraint(String),
    /// The source type contained an invalid length for a
    /// [`ScalarType::Char`].
    InvalidCharLength(InvalidCharLengthError),
    /// The source type contained an invalid max length for a
    /// [`ScalarType::VarChar`].
    InvalidVarCharMaxLength(InvalidVarCharMaxLengthError),
}

impl fmt::Display for TypeConversionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TypeConversionError::UnsupportedType(ty) => write!(f, "type {ty} not supported"),
            TypeConversionError::InvalidNumericMaxScale(e) => e.fmt(f),
            TypeConversionError::InvalidNumericConstraint(msg) => f.write_str(msg),
            TypeConversionError::InvalidCharLength(e) => e.fmt(f),
            TypeConversionError::InvalidVarCharMaxLength(e) => e.fmt(f),
        }
    }
}

impl Error for TypeConversionError {}

impl From<InvalidNumericMaxScaleError> for TypeConversionError {
    fn from(e: InvalidNumericMaxScaleError) -> TypeConversionError {
        TypeConversionError::InvalidNumericMaxScale(e)
    }
}

impl From<InvalidCharLengthError> for TypeConversionError {
    fn from(e: InvalidCharLengthError) -> TypeConversionError {
        TypeConversionError::InvalidCharLength(e)
    }
}

impl From<InvalidVarCharMaxLengthError> for TypeConversionError {
    fn from(e: InvalidVarCharMaxLengthError) -> TypeConversionError {
        TypeConversionError::InvalidVarCharMaxLength(e)
    }
}

impl From<&ScalarType> for Type {
    fn from(typ: &ScalarType) -> Type {
        match typ {
            ScalarType::Array(t) => Type::Array(Box::new(From::from(&**t))),
            ScalarType::Bool => Type::Bool,
            ScalarType::Bytes => Type::Bytea,
            ScalarType::PgLegacyChar => Type::Char,
            ScalarType::Date => Type::Date,
            ScalarType::Float64 => Type::Float8,
            ScalarType::Float32 => Type::Float4,
            ScalarType::Int16 => Type::Int2,
            ScalarType::Int32 => Type::Int4,
            ScalarType::Int64 => Type::Int8,
            ScalarType::UInt16 => Type::UInt2,
            ScalarType::UInt32 => Type::UInt4,
            ScalarType::UInt64 => Type::UInt8,
            ScalarType::Interval => Type::Interval { constraints: None },
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
            ScalarType::Char { length } => Type::BpChar {
                length: (*length).map(CharLength::from),
            },
            ScalarType::VarChar { max_length } => Type::VarChar {
                max_length: (*max_length).map(CharLength::from),
            },
            ScalarType::Time => Type::Time { precision: None },
            ScalarType::Timestamp => Type::Timestamp { precision: None },
            ScalarType::TimestampTz => Type::TimestampTz { precision: None },
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
            ScalarType::Int2Vector => Type::Int2Vector,
        }
    }
}
