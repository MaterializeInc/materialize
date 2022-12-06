// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// `EnumKind` unconditionally introduces a lifetime. TODO: remove this once
// https://github.com/rust-lang/rust-clippy/pull/9037 makes it into stable
#![allow(clippy::extra_unused_lifetimes)]

use std::fmt::{self, Debug, Write};
use std::hash::Hash;
use std::iter;
use std::ops::Add;

use chrono::{DateTime, NaiveDateTime, NaiveTime, TimeZone, Utc};
use dec::OrderedDecimal;
use enum_kinds::EnumKind;
use itertools::Itertools;
use once_cell::sync::Lazy;
use ordered_float::OrderedFloat;
use proptest::prelude::*;
use serde::{Deserialize, Serialize, Serializer};
use uuid::Uuid;

use mz_lowertest::MzReflect;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};

use crate::adt::array::{Array, ArrayDimension};
use crate::adt::char::{Char, CharLength};
use crate::adt::date::Date;
use crate::adt::interval::Interval;
use crate::adt::jsonb::{Jsonb, JsonbRef};
use crate::adt::numeric::{Numeric, NumericMaxScale};
use crate::adt::range::Range;
use crate::adt::system::{Oid, PgLegacyChar, RegClass, RegProc, RegType};
use crate::adt::timestamp::{CheckedTimestamp, TimestampError};
use crate::adt::varchar::{VarChar, VarCharMaxLength};
use crate::GlobalId;
use crate::{ColumnName, ColumnType, DatumList, DatumMap};
use crate::{Row, RowArena};

pub use crate::relation_and_scalar::proto_scalar_type::ProtoRecordField;
pub use crate::relation_and_scalar::ProtoScalarType;

/// A single value.
///
/// # Notes
///
/// ## Equality
/// `Datum` must always derive [`Eq`] to enforce equality with `repr::Row`.
///
/// ## `Datum`-containing types
/// Because Rust disallows recursive enums, complex types which need to contain
/// other `Datum`s instead store bytes representing that data in other structs,
/// usually prefixed with `Datum` (e.g. `DatumList`). These types perform a form
/// of ad-hoc deserialization of their inner bytes to `Datum`s via
/// `crate::row::read_datum`.
///
/// To create a new instance of a `Datum`-referencing `Datum`, you need to store
/// the inner `Datum`'s bytes in a row (so you can in turn borrow those bytes in
/// the outer `Datum`). The idiom we've devised for this is a series of
/// functions on `repr::row::RowPacker` prefixed with `push_`.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, EnumKind)]
#[enum_kind(DatumKind)]
pub enum Datum<'a> {
    /// The `false` boolean value.
    False,
    /// The `true` boolean value.
    True,
    /// A 16-bit signed integer.
    Int16(i16),
    /// A 32-bit signed integer.
    Int32(i32),
    /// A 64-bit signed integer.
    Int64(i64),
    /// An 8-bit unsigned integer.
    UInt8(u8),
    /// An 16-bit unsigned integer.
    UInt16(u16),
    /// A 32-bit unsigned integer.
    UInt32(u32),
    /// A 64-bit unsigned integer.
    UInt64(u64),
    /// A 32-bit floating point number.
    Float32(OrderedFloat<f32>),
    /// A 64-bit floating point number.
    Float64(OrderedFloat<f64>),
    /// A date.
    Date(Date),
    /// A time.
    Time(NaiveTime),
    /// A date and time, without a timezone.
    Timestamp(CheckedTimestamp<NaiveDateTime>),
    /// A date and time, with a timezone.
    TimestampTz(CheckedTimestamp<DateTime<Utc>>),
    /// A span of time.
    Interval(Interval),
    /// A sequence of untyped bytes.
    Bytes(&'a [u8]),
    /// A sequence of Unicode codepoints encoded as UTF-8.
    String(&'a str),
    /// Unlike [`Datum::List`], arrays are like tensors and are not permitted to
    /// be ragged.
    Array(Array<'a>),
    /// A sequence of `Datum`s.
    ///
    /// Unlike [`Datum::Array`], lists are permitted to be ragged.
    List(DatumList<'a>),
    /// A mapping from string keys to `Datum`s.
    Map(DatumMap<'a>),
    /// An exact decimal number, possibly with a fractional component, with up
    /// to 39 digits of precision.
    Numeric(OrderedDecimal<Numeric>),
    /// An unknown value within a JSON-typed `Datum`.
    ///
    /// This variant is distinct from [`Datum::Null`] as a null datum is
    /// distinct from a non-null datum that contains the JSON value `null`.
    JsonNull,
    /// A universally unique identifier.
    Uuid(Uuid),
    MzTimestamp(crate::Timestamp),
    /// A placeholder value.
    ///
    /// Dummy values are never meant to be observed. Many operations on `Datum`
    /// panic if called on this variant.
    ///
    /// Dummies are useful as placeholders in e.g. a `Vec<Datum>`, where it is
    /// known that a certain element of the vector is never observed and
    /// therefore needn't be computed, but where *some* `Datum` must still be
    /// provided to maintain the shape of the vector. While any valid datum
    /// could be used for this purpose, having a dedicated variant makes it
    /// obvious when these optimizations have gone awry. If we used e.g.
    /// `Datum::Null`, an unexpected `Datum::Null` could indicate any number of
    /// problems: bad user data, bad function metadata, or a bad optimization.
    ///
    // TODO(benesch): get rid of this variant. With a more capable optimizer, I
    // don't think there would be any need for dummy datums.
    Dummy,
    // Keep `Null` last so that calling `<` on Datums sorts nulls last, to
    // match the default in PostgreSQL. Note that this doesn't have an effect
    // on ORDER BY, because that is handled by compare_columns. The only
    // situation it has an effect is array comparisons, e.g.,
    // `SELECT ARRAY[1] < ARRAY[NULL]::int[]`. In such a situation, we end up
    // calling `<` on Datums (see `fn lt` in scalar/func.rs).
    /// An unknown value.
    Null,
    // A range of values, e.g. [-1, 1).
    Range(Range<'a>),
}

/// This implementation of serialize is designed to be able to print out Datums
/// in a human-readable way in JSON. This implementation may not suit other
/// serialization purposes.
impl<'a> Serialize for Datum<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use Datum::*;
        match self {
            False => serializer.serialize_bool(false),
            True => serializer.serialize_bool(true),
            Int16(i) => serializer.serialize_i16(*i),
            Int32(i) => serializer.serialize_i32(*i),
            Int64(i) => serializer.serialize_i64(*i),
            UInt8(u) => serializer.serialize_u8(*u),
            UInt16(u) => serializer.serialize_u16(*u),
            UInt32(u) => serializer.serialize_u32(*u),
            UInt64(u) => serializer.serialize_u64(*u),
            Float32(f) => serializer.serialize_f32(**f),
            Float64(f) => serializer.serialize_f64(**f),
            Date(d) => chrono::NaiveDate::from(d).serialize(serializer),
            Time(t) => t.serialize(serializer),
            Timestamp(ts) => ts.serialize(serializer),
            TimestampTz(tstz) => tstz.serialize(serializer),
            Interval(i) => i.serialize(serializer),
            Bytes(b) => serializer.serialize_bytes(b),
            String(s) => serializer.serialize_str(s),
            Array(a) => a.serialize(serializer),
            List(l) => l.serialize(serializer),
            Map(m) => m.serialize(serializer),
            Numeric(n) => serializer.serialize_str(&n.to_string()),
            Uuid(u) => u.serialize(serializer),
            MzTimestamp(t) => serializer.serialize_str(&t.to_string()),
            Dummy => serializer.serialize_str("Dummy"),
            JsonNull => serializer.serialize_str("JsonNull"),
            Null => serializer.serialize_none(),
            r @ Range { .. } => serializer.serialize_str(&r.to_string()),
        }
    }
}

impl TryFrom<Datum<'_>> for bool {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::False => Ok(false),
            Datum::True => Ok(true),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for Option<bool> {
    type Error = ();

    fn try_from(datum: Datum<'_>) -> Result<Self, Self::Error> {
        match datum {
            Datum::Null => Ok(None),
            Datum::False => Ok(Some(false)),
            Datum::True => Ok(Some(true)),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for f32 {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Float32(f) => Ok(*f),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for Option<f32> {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Null => Ok(None),
            Datum::Float32(f) => Ok(Some(*f)),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for f64 {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Float64(f) => Ok(*f),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for Option<f64> {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Null => Ok(None),
            Datum::Float64(f) => Ok(Some(*f)),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for i16 {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Int16(i) => Ok(i),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for Option<i16> {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Null => Ok(None),
            Datum::Int16(i) => Ok(Some(i)),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for i32 {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Int32(i) => Ok(i),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for Option<i32> {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Null => Ok(None),
            Datum::Int32(i) => Ok(Some(i)),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for i64 {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Int64(i) => Ok(i),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for Option<i64> {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Null => Ok(None),
            Datum::Int64(i) => Ok(Some(i)),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for u16 {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::UInt16(u) => Ok(u),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for Option<u16> {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Null => Ok(None),
            Datum::UInt16(u) => Ok(Some(u)),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for u32 {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::UInt32(u) => Ok(u),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for Option<u32> {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Null => Ok(None),
            Datum::UInt32(u) => Ok(Some(u)),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for u64 {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::UInt64(u) => Ok(u),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for Option<u64> {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Null => Ok(None),
            Datum::UInt64(u) => Ok(Some(u)),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for CheckedTimestamp<NaiveDateTime> {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::Timestamp(dt) => Ok(dt),
            _ => Err(()),
        }
    }
}

impl TryFrom<Datum<'_>> for CheckedTimestamp<DateTime<Utc>> {
    type Error = ();
    fn try_from(from: Datum<'_>) -> Result<Self, Self::Error> {
        match from {
            Datum::TimestampTz(dt_tz) => Ok(dt_tz),
            _ => Err(()),
        }
    }
}

impl<'a> Datum<'a> {
    /// Reports whether this datum is null (i.e., is [`Datum::Null`]).
    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    /// Unwraps the boolean value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::False`] or [`Datum::True`].
    #[track_caller]
    pub fn unwrap_bool(&self) -> bool {
        match self {
            Datum::False => false,
            Datum::True => true,
            _ => panic!("Datum::unwrap_bool called on {:?}", self),
        }
    }

    /// Unwraps the 16-bit integer value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Int16`].
    #[track_caller]
    pub fn unwrap_int16(&self) -> i16 {
        match self {
            Datum::Int16(i) => *i,
            _ => panic!("Datum::unwrap_int16 called on {:?}", self),
        }
    }

    /// Unwraps the 32-bit integer value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Int32`].
    #[track_caller]
    pub fn unwrap_int32(&self) -> i32 {
        match self {
            Datum::Int32(i) => *i,
            _ => panic!("Datum::unwrap_int32 called on {:?}", self),
        }
    }

    /// Unwraps the 64-bit integer value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Int64`].
    #[track_caller]
    pub fn unwrap_int64(&self) -> i64 {
        match self {
            Datum::Int64(i) => *i,
            _ => panic!("Datum::unwrap_int64 called on {:?}", self),
        }
    }

    /// Unwraps the 8-bit unsigned integer value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::UInt8`].
    #[track_caller]
    pub fn unwrap_uint8(&self) -> u8 {
        match self {
            Datum::UInt8(u) => *u,
            _ => panic!("Datum::unwrap_uint8 called on {:?}", self),
        }
    }

    /// Unwraps the 16-bit unsigned integer value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::UInt16`].
    #[track_caller]
    pub fn unwrap_uint16(&self) -> u16 {
        match self {
            Datum::UInt16(u) => *u,
            _ => panic!("Datum::unwrap_uint16 called on {:?}", self),
        }
    }

    /// Unwraps the 32-bit unsigned integer value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::UInt32`].
    #[track_caller]
    pub fn unwrap_uint32(&self) -> u32 {
        match self {
            Datum::UInt32(u) => *u,
            _ => panic!("Datum::unwrap_uint32 called on {:?}", self),
        }
    }

    /// Unwraps the 64-bit unsigned integer value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::UInt64`].
    #[track_caller]
    pub fn unwrap_uint64(&self) -> u64 {
        match self {
            Datum::UInt64(u) => *u,
            _ => panic!("Datum::unwrap_uint64 called on {:?}", self),
        }
    }

    #[track_caller]
    pub fn unwrap_ordered_float32(&self) -> OrderedFloat<f32> {
        match self {
            Datum::Float32(f) => *f,
            _ => panic!("Datum::unwrap_ordered_float32 called on {:?}", self),
        }
    }

    #[track_caller]
    pub fn unwrap_ordered_float64(&self) -> OrderedFloat<f64> {
        match self {
            Datum::Float64(f) => *f,
            _ => panic!("Datum::unwrap_ordered_float64 called on {:?}", self),
        }
    }

    /// Unwraps the 32-bit floating-point value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Float32`].
    #[track_caller]
    pub fn unwrap_float32(&self) -> f32 {
        match self {
            Datum::Float32(f) => f.into_inner(),
            _ => panic!("Datum::unwrap_float32 called on {:?}", self),
        }
    }

    /// Unwraps the 64-bit floating-point value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Float64`].
    #[track_caller]
    pub fn unwrap_float64(&self) -> f64 {
        match self {
            Datum::Float64(f) => f.into_inner(),
            _ => panic!("Datum::unwrap_float64 called on {:?}", self),
        }
    }

    /// Unwraps the date value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Date`].
    #[track_caller]
    pub fn unwrap_date(&self) -> Date {
        match self {
            Datum::Date(d) => *d,
            _ => panic!("Datum::unwrap_date called on {:?}", self),
        }
    }

    /// Unwraps the time vaqlue within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Time`].
    #[track_caller]
    pub fn unwrap_time(&self) -> chrono::NaiveTime {
        match self {
            Datum::Time(t) => *t,
            _ => panic!("Datum::unwrap_time called on {:?}", self),
        }
    }

    /// Unwraps the timestamp value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Timestamp`].
    #[track_caller]
    pub fn unwrap_timestamp(&self) -> CheckedTimestamp<chrono::NaiveDateTime> {
        match self {
            Datum::Timestamp(ts) => *ts,
            _ => panic!("Datum::unwrap_timestamp called on {:?}", self),
        }
    }

    /// Unwraps the timestamptz value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::TimestampTz`].
    #[track_caller]
    pub fn unwrap_timestamptz(&self) -> CheckedTimestamp<chrono::DateTime<Utc>> {
        match self {
            Datum::TimestampTz(ts) => *ts,
            _ => panic!("Datum::unwrap_timestamptz called on {:?}", self),
        }
    }

    /// Unwraps the interval value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Interval`].
    #[track_caller]
    pub fn unwrap_interval(&self) -> Interval {
        match self {
            Datum::Interval(iv) => *iv,
            _ => panic!("Datum::unwrap_interval called on {:?}", self),
        }
    }

    /// Unwraps the string value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::String`].
    #[track_caller]
    pub fn unwrap_str(&self) -> &'a str {
        match self {
            Datum::String(s) => s,
            _ => panic!("Datum::unwrap_string called on {:?}", self),
        }
    }

    /// Unwraps the bytes value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Bytes`].
    #[track_caller]
    pub fn unwrap_bytes(&self) -> &'a [u8] {
        match self {
            Datum::Bytes(b) => b,
            _ => panic!("Datum::unwrap_bytes called on {:?}", self),
        }
    }

    /// Unwraps the uuid value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Uuid`].
    #[track_caller]
    pub fn unwrap_uuid(&self) -> Uuid {
        match self {
            Datum::Uuid(u) => *u,
            _ => panic!("Datum::unwrap_uuid called on {:?}", self),
        }
    }

    /// Unwraps the array value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Array`].
    #[track_caller]
    pub fn unwrap_array(&self) -> Array<'a> {
        match self {
            Datum::Array(array) => *array,
            _ => panic!("Datum::unwrap_array called on {:?}", self),
        }
    }

    /// Unwraps the list value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::List`].
    #[track_caller]
    pub fn unwrap_list(&self) -> DatumList<'a> {
        match self {
            Datum::List(list) => *list,
            _ => panic!("Datum::unwrap_list called on {:?}", self),
        }
    }

    /// Unwraps the map value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Map`].
    #[track_caller]
    pub fn unwrap_map(&self) -> DatumMap<'a> {
        match self {
            Datum::Map(dict) => *dict,
            _ => panic!("Datum::unwrap_dict called on {:?}", self),
        }
    }

    /// Unwraps the numeric value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Numeric`].
    #[track_caller]
    pub fn unwrap_numeric(&self) -> OrderedDecimal<Numeric> {
        match self {
            Datum::Numeric(n) => *n,
            _ => panic!("Datum::unwrap_numeric called on {:?}", self),
        }
    }

    /// Unwraps the mz_repr::Timestamp value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::MzTimestamp`].
    #[track_caller]
    pub fn unwrap_mz_timestamp(&self) -> crate::Timestamp {
        match self {
            Datum::MzTimestamp(t) => *t,
            _ => panic!("Datum::unwrap_mz_timestamp called on {:?}", self),
        }
    }

    /// Reports whether this datum is an instance of the specified column type.
    pub fn is_instance_of(self, column_type: &ColumnType) -> bool {
        fn is_instance_of_scalar(datum: Datum, scalar_type: &ScalarType) -> bool {
            if let ScalarType::Jsonb = scalar_type {
                // json type checking
                match datum {
                    Datum::JsonNull
                    | Datum::False
                    | Datum::True
                    | Datum::Numeric(_)
                    | Datum::String(_) => true,
                    Datum::List(list) => list
                        .iter()
                        .all(|elem| is_instance_of_scalar(elem, scalar_type)),
                    Datum::Map(dict) => dict
                        .iter()
                        .all(|(_key, val)| is_instance_of_scalar(val, scalar_type)),
                    _ => false,
                }
            } else {
                // sql type checking
                match (datum, scalar_type) {
                    (Datum::Dummy, _) => panic!("Datum::Dummy observed"),
                    (Datum::Null, _) => false,
                    (Datum::False, ScalarType::Bool) => true,
                    (Datum::False, _) => false,
                    (Datum::True, ScalarType::Bool) => true,
                    (Datum::True, _) => false,
                    (Datum::Int16(_), ScalarType::Int16) => true,
                    (Datum::Int16(_), _) => false,
                    (Datum::Int32(_), ScalarType::Int32) => true,
                    (Datum::Int32(_), _) => false,
                    (Datum::Int64(_), ScalarType::Int64) => true,
                    (Datum::Int64(_), _) => false,
                    (Datum::UInt8(_), ScalarType::PgLegacyChar) => true,
                    (Datum::UInt8(_), _) => false,
                    (Datum::UInt16(_), ScalarType::UInt16) => true,
                    (Datum::UInt16(_), _) => false,
                    (Datum::UInt32(_), ScalarType::Oid) => true,
                    (Datum::UInt32(_), ScalarType::RegClass) => true,
                    (Datum::UInt32(_), ScalarType::RegProc) => true,
                    (Datum::UInt32(_), ScalarType::RegType) => true,
                    (Datum::UInt32(_), ScalarType::UInt32) => true,
                    (Datum::UInt32(_), _) => false,
                    (Datum::UInt64(_), ScalarType::UInt64) => true,
                    (Datum::UInt64(_), _) => false,
                    (Datum::Float32(_), ScalarType::Float32) => true,
                    (Datum::Float32(_), _) => false,
                    (Datum::Float64(_), ScalarType::Float64) => true,
                    (Datum::Float64(_), _) => false,
                    (Datum::Date(_), ScalarType::Date) => true,
                    (Datum::Date(_), _) => false,
                    (Datum::Time(_), ScalarType::Time) => true,
                    (Datum::Time(_), _) => false,
                    (Datum::Timestamp(_), ScalarType::Timestamp) => true,
                    (Datum::Timestamp(_), _) => false,
                    (Datum::TimestampTz(_), ScalarType::TimestampTz) => true,
                    (Datum::TimestampTz(_), _) => false,
                    (Datum::Interval(_), ScalarType::Interval) => true,
                    (Datum::Interval(_), _) => false,
                    (Datum::Bytes(_), ScalarType::Bytes) => true,
                    (Datum::Bytes(_), _) => false,
                    (Datum::String(_), ScalarType::String)
                    | (Datum::String(_), ScalarType::VarChar { .. })
                    | (Datum::String(_), ScalarType::Char { .. }) => true,
                    (Datum::String(_), _) => false,
                    (Datum::Uuid(_), ScalarType::Uuid) => true,
                    (Datum::Uuid(_), _) => false,
                    (Datum::Array(array), ScalarType::Array(t)) => {
                        array.elements.iter().all(|e| match e {
                            Datum::Null => true,
                            _ => is_instance_of_scalar(e, t),
                        })
                    }
                    (Datum::Array(array), ScalarType::Int2Vector) => {
                        array.dims().len() == 1
                            && array
                                .elements
                                .iter()
                                .all(|e| is_instance_of_scalar(e, &ScalarType::Int16))
                    }
                    (Datum::Array(_), _) => false,
                    (Datum::List(list), ScalarType::List { element_type, .. }) => list
                        .iter()
                        .all(|e| e.is_null() || is_instance_of_scalar(e, element_type)),
                    (Datum::List(list), ScalarType::Record { fields, .. }) => {
                        list.iter().zip_eq(fields).all(|(e, (_, t))| {
                            (e.is_null() && t.nullable) || is_instance_of_scalar(e, &t.scalar_type)
                        })
                    }
                    (Datum::List(_), _) => false,
                    (Datum::Map(map), ScalarType::Map { value_type, .. }) => map
                        .iter()
                        .all(|(_k, v)| v.is_null() || is_instance_of_scalar(v, value_type)),
                    (Datum::Map(_), _) => false,
                    (Datum::JsonNull, _) => false,
                    (Datum::Numeric(_), ScalarType::Numeric { .. }) => true,
                    (Datum::Numeric(_), _) => false,
                    (Datum::MzTimestamp(_), ScalarType::MzTimestamp) => true,
                    (Datum::MzTimestamp(_), _) => false,
                    (Datum::Range(_), _) => {
                        unreachable!("ranges not available in dataflows or types")
                    }
                }
            }
        }
        if column_type.nullable {
            if let Datum::Null = self {
                return true;
            }
        }
        is_instance_of_scalar(self, &column_type.scalar_type)
    }
}

impl<'a> From<bool> for Datum<'a> {
    fn from(b: bool) -> Datum<'a> {
        if b {
            Datum::True
        } else {
            Datum::False
        }
    }
}

impl<'a> From<i16> for Datum<'a> {
    fn from(i: i16) -> Datum<'a> {
        Datum::Int16(i)
    }
}

impl<'a> From<i32> for Datum<'a> {
    fn from(i: i32) -> Datum<'a> {
        Datum::Int32(i)
    }
}

impl<'a> From<i64> for Datum<'a> {
    fn from(i: i64) -> Datum<'a> {
        Datum::Int64(i)
    }
}

impl<'a> From<u16> for Datum<'a> {
    fn from(u: u16) -> Datum<'a> {
        Datum::UInt16(u)
    }
}

impl<'a> From<u32> for Datum<'a> {
    fn from(u: u32) -> Datum<'a> {
        Datum::UInt32(u)
    }
}

impl<'a> From<u64> for Datum<'a> {
    fn from(u: u64) -> Datum<'a> {
        Datum::UInt64(u)
    }
}

impl<'a> From<OrderedFloat<f32>> for Datum<'a> {
    fn from(f: OrderedFloat<f32>) -> Datum<'a> {
        Datum::Float32(f)
    }
}

impl<'a> From<OrderedFloat<f64>> for Datum<'a> {
    fn from(f: OrderedFloat<f64>) -> Datum<'a> {
        Datum::Float64(f)
    }
}

impl<'a> From<f32> for Datum<'a> {
    fn from(f: f32) -> Datum<'a> {
        Datum::Float32(OrderedFloat(f))
    }
}

impl<'a> From<f64> for Datum<'a> {
    fn from(f: f64) -> Datum<'a> {
        Datum::Float64(OrderedFloat(f))
    }
}

impl<'a> From<i128> for Datum<'a> {
    fn from(d: i128) -> Datum<'a> {
        Datum::Numeric(OrderedDecimal(Numeric::try_from(d).unwrap()))
    }
}

impl<'a> From<u128> for Datum<'a> {
    fn from(d: u128) -> Datum<'a> {
        Datum::Numeric(OrderedDecimal(Numeric::try_from(d).unwrap()))
    }
}

impl<'a> From<Numeric> for Datum<'a> {
    fn from(n: Numeric) -> Datum<'a> {
        Datum::Numeric(OrderedDecimal(n))
    }
}

impl<'a> From<chrono::Duration> for Datum<'a> {
    fn from(duration: chrono::Duration) -> Datum<'a> {
        let micros = duration.num_microseconds().unwrap_or(0);
        Datum::Interval(Interval::new(0, 0, micros))
    }
}

impl<'a> From<Interval> for Datum<'a> {
    fn from(other: Interval) -> Datum<'a> {
        Datum::Interval(other)
    }
}

impl<'a> From<&'a str> for Datum<'a> {
    fn from(s: &'a str) -> Datum<'a> {
        Datum::String(s)
    }
}

impl<'a> From<&'a [u8]> for Datum<'a> {
    fn from(b: &'a [u8]) -> Datum<'a> {
        Datum::Bytes(b)
    }
}

impl<'a> From<Date> for Datum<'a> {
    fn from(d: Date) -> Datum<'a> {
        Datum::Date(d)
    }
}

impl<'a> From<NaiveTime> for Datum<'a> {
    fn from(t: NaiveTime) -> Datum<'a> {
        Datum::Time(t)
    }
}

impl<'a> From<CheckedTimestamp<NaiveDateTime>> for Datum<'a> {
    fn from(dt: CheckedTimestamp<NaiveDateTime>) -> Datum<'a> {
        Datum::Timestamp(dt)
    }
}

impl<'a> From<CheckedTimestamp<DateTime<Utc>>> for Datum<'a> {
    fn from(dt: CheckedTimestamp<DateTime<Utc>>) -> Datum<'a> {
        Datum::TimestampTz(dt)
    }
}

impl<'a> TryInto<Datum<'a>> for NaiveDateTime {
    type Error = TimestampError;

    fn try_into(self) -> Result<Datum<'a>, Self::Error> {
        let t = CheckedTimestamp::from_timestamplike(self)?;
        Ok(t.into())
    }
}

impl<'a> TryInto<Datum<'a>> for DateTime<Utc> {
    type Error = TimestampError;

    fn try_into(self) -> Result<Datum<'a>, Self::Error> {
        let t = CheckedTimestamp::from_timestamplike(self)?;
        Ok(t.into())
    }
}

impl<'a> From<Uuid> for Datum<'a> {
    fn from(uuid: Uuid) -> Datum<'a> {
        Datum::Uuid(uuid)
    }
}
impl<'a> From<crate::Timestamp> for Datum<'a> {
    fn from(ts: crate::Timestamp) -> Datum<'a> {
        Datum::MzTimestamp(ts)
    }
}

impl<'a, T> From<Option<T>> for Datum<'a>
where
    Datum<'a>: From<T>,
{
    fn from(o: Option<T>) -> Datum<'a> {
        if let Some(d) = o {
            d.into()
        } else {
            Datum::Null
        }
    }
}

fn write_delimited<T, TS, F>(
    f: &mut fmt::Formatter,
    delimiter: &str,
    things: TS,
    write: F,
) -> fmt::Result
where
    TS: IntoIterator<Item = T>,
    F: Fn(&mut fmt::Formatter, T) -> fmt::Result,
{
    let mut iter = things.into_iter().peekable();
    while let Some(thing) = iter.next() {
        write(f, thing)?;
        if iter.peek().is_some() {
            f.write_str(delimiter)?;
        }
    }
    Ok(())
}

impl fmt::Display for Datum<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Datum::Null => f.write_str("null"),
            Datum::True => f.write_str("true"),
            Datum::False => f.write_str("false"),
            Datum::Int16(num) => write!(f, "{}", num),
            Datum::Int32(num) => write!(f, "{}", num),
            Datum::Int64(num) => write!(f, "{}", num),
            Datum::UInt8(num) => write!(f, "{}", num),
            Datum::UInt16(num) => write!(f, "{}", num),
            Datum::UInt32(num) => write!(f, "{}", num),
            Datum::UInt64(num) => write!(f, "{}", num),
            Datum::Float32(num) => write!(f, "{}", num),
            Datum::Float64(num) => write!(f, "{}", num),
            Datum::Date(d) => write!(f, "{}", d),
            Datum::Time(t) => write!(f, "{}", t),
            Datum::Timestamp(t) => write!(f, "{}", t),
            Datum::TimestampTz(t) => write!(f, "{}", t),
            Datum::Interval(iv) => write!(f, "{}", iv),
            Datum::Bytes(dat) => {
                f.write_str("0x")?;
                for b in dat.iter() {
                    write!(f, "{:02x}", b)?;
                }
                Ok(())
            }
            Datum::String(s) => {
                f.write_str("\"")?;
                for c in s.chars() {
                    if c == '"' {
                        f.write_str("\\\"")?;
                    } else {
                        f.write_char(c)?;
                    }
                }
                f.write_str("\"")
            }
            Datum::Uuid(u) => write!(f, "{}", u),
            Datum::Array(array) => {
                f.write_str("{")?;
                write_delimited(f, ", ", &array.elements, |f, e| write!(f, "{}", e))?;
                f.write_str("}")
            }
            Datum::List(list) => {
                f.write_str("[")?;
                write_delimited(f, ", ", list, |f, e| write!(f, "{}", e))?;
                f.write_str("]")
            }
            Datum::Map(dict) => {
                f.write_str("{")?;
                write_delimited(f, ", ", dict, |f, (k, v)| write!(f, "{}: {}", k, v))?;
                f.write_str("}")
            }
            Datum::Numeric(n) => write!(f, "{}", n.0.to_standard_notation_string()),
            Datum::MzTimestamp(t) => write!(f, "{}", t),
            Datum::JsonNull => f.write_str("json_null"),
            Datum::Dummy => f.write_str("dummy"),
            Datum::Range(i) => write!(f, "{}", i),
        }
    }
}

impl From<&Datum<'_>> for serde_json::Value {
    fn from(datum: &Datum) -> serde_json::Value {
        // Convert most floats to a JSON Number. JSON Numbers don't support NaN or
        // Infinity, so those will still be rendered as strings.
        fn float_to_json(f: f64) -> serde_json::Value {
            match serde_json::Number::from_f64(f) {
                Some(n) => serde_json::Value::Number(n),
                None => serde_json::Value::String(f.to_string()),
            }
        }
        match datum {
            Datum::Null | Datum::JsonNull => serde_json::Value::Null,
            Datum::False => serde_json::Value::Bool(false),
            Datum::True => serde_json::Value::Bool(true),
            Datum::Int16(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Datum::Int32(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Datum::Int64(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Datum::UInt8(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Datum::UInt16(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Datum::UInt32(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Datum::UInt64(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Datum::Float32(n) => float_to_json(n.into_inner() as f64),
            Datum::Float64(n) => float_to_json(n.into_inner()),
            Datum::Numeric(d) => {
                // serde_json requires floats to be finite
                if !d.0.is_finite() {
                    serde_json::Value::String(d.0.to_string())
                } else {
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(f64::try_from(d.0).unwrap()).unwrap(),
                    )
                }
            }
            Datum::String(s) => serde_json::Value::String(s.to_string()),
            Datum::List(list) => {
                serde_json::Value::Array(list.iter().map(|datum| Self::from(&datum)).collect())
            }
            Datum::Array(array) => serde_json::Value::Array(
                array
                    .elements()
                    .iter()
                    .map(|datum| Self::from(&datum))
                    .collect(),
            ),
            Datum::Map(map) => serde_json::Value::Object(
                map.iter()
                    .map(|(k, v)| (k.to_owned(), Self::from(&v)))
                    .collect(),
            ),
            Datum::Dummy => unreachable!(),
            Datum::Bytes(_)
            | Datum::Date(_)
            | Datum::Interval(_)
            | Datum::Time(_)
            | Datum::Timestamp(_)
            | Datum::TimestampTz(_)
            | Datum::MzTimestamp(_)
            | Datum::Uuid(_)
            | Datum::Range { .. } => serde_json::Value::String(datum.to_string()),
        }
    }
}

/// The type of a [`Datum`].
///
/// There is a direct correspondence between `Datum` variants and `ScalarType`
/// variants.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Ord, PartialOrd, Hash, EnumKind, MzReflect,
)]
#[enum_kind(ScalarBaseType, derive(Hash))]
pub enum ScalarType {
    /// The type of [`Datum::True`] and [`Datum::False`].
    Bool,
    /// The type of [`Datum::Int16`].
    Int16,
    /// The type of [`Datum::Int32`].
    Int32,
    /// The type of [`Datum::Int64`].
    Int64,
    /// The type of [`Datum::UInt16`].
    UInt16,
    /// The type of [`Datum::UInt32`].
    UInt32,
    /// The type of [`Datum::UInt64`].
    UInt64,
    /// The type of [`Datum::Float32`].
    Float32,
    /// The type of [`Datum::Float64`].
    Float64,
    /// The type of [`Datum::Numeric`].
    ///
    /// `Numeric` values cannot exceed [`NUMERIC_DATUM_MAX_PRECISION`] digits of
    /// precision.
    ///
    /// This type additionally specifies the maximum scale of the decimal. The
    /// scale specifies the number of digits after the decimal point.
    ///
    /// [`NUMERIC_DATUM_MAX_PRECISION`]: crate::adt::numeric::NUMERIC_DATUM_MAX_PRECISION
    Numeric { max_scale: Option<NumericMaxScale> },
    /// The type of [`Datum::Date`].
    Date,
    /// The type of [`Datum::Time`].
    Time,
    /// The type of [`Datum::Timestamp`].
    Timestamp,
    /// The type of [`Datum::TimestampTz`].
    TimestampTz,
    /// The type of [`Datum::Interval`].
    Interval,
    /// A single byte character type backed by a [`Datum::UInt8`].
    ///
    /// PostgreSQL calls this type `"char"`. Note the quotes, which distinguish
    /// it from the type `ScalarType::Char`.
    PgLegacyChar,
    /// The type of [`Datum::Bytes`].
    Bytes,
    /// The type of [`Datum::String`].
    String,
    /// Stored as [`Datum::String`], but expresses a fixed-width, blank-padded
    /// string.
    ///
    /// Note that a `length` of `None` is used in special cases, such as
    /// creating lists.
    Char { length: Option<CharLength> },
    /// Stored as [`Datum::String`], but can optionally express a limit on the
    /// string's length.
    VarChar {
        max_length: Option<VarCharMaxLength>,
    },
    /// The type of a datum that may represent any valid JSON value.
    ///
    /// Valid datum variants for this type are:
    ///
    ///   * [`Datum::JsonNull`]
    ///   * [`Datum::False`]
    ///   * [`Datum::True`]
    ///   * [`Datum::String`]
    ///   * [`Datum::Numeric`]
    ///   * [`Datum::List`]
    ///   * [`Datum::Map`]
    Jsonb,
    /// The type of [`Datum::Uuid`].
    Uuid,
    /// The type of [`Datum::Array`].
    ///
    /// Elements within the array are of the specified type. It is illegal for
    /// the element type to be itself an array type. Array elements may always
    /// be [`Datum::Null`].
    Array(Box<ScalarType>),
    /// The type of [`Datum::List`].
    ///
    /// Elements within the list are of the specified type. List elements may
    /// always be [`Datum::Null`].
    List {
        element_type: Box<ScalarType>,
        custom_id: Option<GlobalId>,
    },
    /// An ordered and named sequence of datums.
    Record {
        /// The names and types of the fields of the record, in order from left
        /// to right.
        fields: Vec<(ColumnName, ColumnType)>,
        custom_id: Option<GlobalId>,
    },
    /// A PostgreSQL object identifier.
    Oid,
    /// The type of [`Datum::Map`]
    ///
    /// Keys within the map are always of type [`ScalarType::String`].
    /// Values within the map are of the specified type. Values may always
    /// be [`Datum::Null`].
    Map {
        value_type: Box<ScalarType>,
        custom_id: Option<GlobalId>,
    },
    /// A PostgreSQL function name.
    RegProc,
    /// A PostgreSQL type name.
    RegType,
    /// A PostgreSQL class name.
    RegClass,
    /// A vector on small ints; this is a legacy type in PG used primarily in
    /// the catalog.
    Int2Vector,
    /// A Materialize timestamp.
    MzTimestamp,
}

impl RustType<ProtoRecordField> for (ColumnName, ColumnType) {
    fn into_proto(&self) -> ProtoRecordField {
        ProtoRecordField {
            column_name: Some(self.0.into_proto()),
            column_type: Some(self.1.into_proto()),
        }
    }

    fn from_proto(proto: ProtoRecordField) -> Result<Self, TryFromProtoError> {
        Ok((
            proto
                .column_name
                .into_rust_if_some("ProtoRecordField::column_name")?,
            proto
                .column_type
                .into_rust_if_some("ProtoRecordField::column_type")?,
        ))
    }
}

impl RustType<ProtoScalarType> for ScalarType {
    fn into_proto(&self) -> ProtoScalarType {
        use crate::relation_and_scalar::proto_scalar_type::Kind::*;
        use crate::relation_and_scalar::proto_scalar_type::*;

        ProtoScalarType {
            kind: Some(match self {
                ScalarType::Bool => Bool(()),
                ScalarType::Int16 => Int16(()),
                ScalarType::Int32 => Int32(()),
                ScalarType::Int64 => Int64(()),
                ScalarType::UInt16 => UInt16(()),
                ScalarType::UInt32 => UInt32(()),
                ScalarType::UInt64 => UInt64(()),
                ScalarType::Float32 => Float32(()),
                ScalarType::Float64 => Float64(()),
                ScalarType::Date => Date(()),
                ScalarType::Time => Time(()),
                ScalarType::Timestamp => Timestamp(()),
                ScalarType::TimestampTz => TimestampTz(()),
                ScalarType::Interval => Interval(()),
                ScalarType::PgLegacyChar => PgLegacyChar(()),
                ScalarType::Bytes => Bytes(()),
                ScalarType::String => String(()),
                ScalarType::Jsonb => Jsonb(()),
                ScalarType::Uuid => Uuid(()),
                ScalarType::Oid => Oid(()),
                ScalarType::RegProc => RegProc(()),
                ScalarType::RegType => RegType(()),
                ScalarType::RegClass => RegClass(()),
                ScalarType::Int2Vector => Int2Vector(()),

                ScalarType::Numeric { max_scale } => Numeric(max_scale.into_proto()),
                ScalarType::Char { length } => Char(ProtoChar {
                    length: length.into_proto(),
                }),
                ScalarType::VarChar { max_length } => VarChar(ProtoVarChar {
                    max_length: max_length.into_proto(),
                }),

                ScalarType::List {
                    element_type,
                    custom_id,
                } => List(Box::new(ProtoList {
                    element_type: Some(element_type.into_proto()),
                    custom_id: custom_id.map(|id| id.into_proto()),
                })),
                ScalarType::Record { custom_id, fields } => Record(ProtoRecord {
                    custom_id: custom_id.map(|id| id.into_proto()),
                    fields: fields.into_proto(),
                }),
                ScalarType::Array(typ) => Array(typ.into_proto()),
                ScalarType::Map {
                    value_type,
                    custom_id,
                } => Map(Box::new(ProtoMap {
                    value_type: Some(value_type.into_proto()),
                    custom_id: custom_id.map(|id| id.into_proto()),
                })),
                ScalarType::MzTimestamp => MzTimestamp(()),
            }),
        }
    }

    fn from_proto(proto: ProtoScalarType) -> Result<Self, TryFromProtoError> {
        use crate::relation_and_scalar::proto_scalar_type::Kind::*;

        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoScalarType::Kind"))?;

        match kind {
            Bool(()) => Ok(ScalarType::Bool),
            Int16(()) => Ok(ScalarType::Int16),
            Int32(()) => Ok(ScalarType::Int32),
            Int64(()) => Ok(ScalarType::Int64),
            UInt16(()) => Ok(ScalarType::UInt16),
            UInt32(()) => Ok(ScalarType::UInt32),
            UInt64(()) => Ok(ScalarType::UInt64),
            Float32(()) => Ok(ScalarType::Float32),
            Float64(()) => Ok(ScalarType::Float64),
            Date(()) => Ok(ScalarType::Date),
            Time(()) => Ok(ScalarType::Time),
            Timestamp(()) => Ok(ScalarType::Timestamp),
            TimestampTz(()) => Ok(ScalarType::TimestampTz),
            Interval(()) => Ok(ScalarType::Interval),
            PgLegacyChar(()) => Ok(ScalarType::PgLegacyChar),
            Bytes(()) => Ok(ScalarType::Bytes),
            String(()) => Ok(ScalarType::String),
            Jsonb(()) => Ok(ScalarType::Jsonb),
            Uuid(()) => Ok(ScalarType::Uuid),
            Oid(()) => Ok(ScalarType::Oid),
            RegProc(()) => Ok(ScalarType::RegProc),
            RegType(()) => Ok(ScalarType::RegType),
            RegClass(()) => Ok(ScalarType::RegClass),
            Int2Vector(()) => Ok(ScalarType::Int2Vector),

            Numeric(x) => Ok(ScalarType::Numeric {
                max_scale: x.into_rust()?,
            }),
            Char(x) => Ok(ScalarType::Char {
                length: x.length.into_rust()?,
            }),

            VarChar(x) => Ok(ScalarType::VarChar {
                max_length: x.max_length.into_rust()?,
            }),
            Array(x) => Ok(ScalarType::Array({
                let st: ScalarType = (*x).into_rust()?;
                st.into()
            })),
            List(x) => Ok(ScalarType::List {
                element_type: Box::new(
                    x.element_type
                        .map(|x| *x)
                        .into_rust_if_some("ProtoList::element_type")?,
                ),
                custom_id: x.custom_id.map(|id| id.into_rust().unwrap()),
            }),
            Record(x) => Ok(ScalarType::Record {
                custom_id: x.custom_id.map(|id| id.into_rust().unwrap()),
                fields: x.fields.into_rust()?,
            }),
            Map(x) => Ok(ScalarType::Map {
                value_type: Box::new(
                    x.value_type
                        .map(|x| *x)
                        .into_rust_if_some("ProtoMap::value_type")?,
                ),
                custom_id: x.custom_id.map(|id| id.into_rust().unwrap()),
            }),
            MzTimestamp(()) => Ok(ScalarType::MzTimestamp),
        }
    }
}

/// Types that implement this trait can be stored in an SQL column with the specified ColumnType
pub trait AsColumnType {
    /// The SQL column type of this Rust type
    fn as_column_type() -> ColumnType;
}

/// A bridge between native Rust types and SQL runtime types represented in Datums
pub trait DatumType<'a, E>: Sized {
    /// Whether this Rust type can represent NULL values
    fn nullable() -> bool;

    /// Try to convert a Result whose Ok variant is a Datum into this native Rust type (Self). If
    /// it fails the error variant will contain the original result.
    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>>;

    /// Convert this Rust type into a Result containing a Datum, or an error
    fn into_result(self, temp_storage: &'a RowArena) -> Result<Datum<'a>, E>;
}

impl<B: AsColumnType> AsColumnType for Option<B> {
    fn as_column_type() -> ColumnType {
        B::as_column_type().nullable(true)
    }
}

impl<'a, E, B: DatumType<'a, E>> DatumType<'a, E> for Option<B> {
    fn nullable() -> bool {
        true
    }
    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::Null) => Ok(None),
            Ok(datum) => B::try_from_result(Ok(datum)).map(Some),
            _ => Err(res),
        }
    }
    fn into_result(self, temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        match self {
            Some(inner) => inner.into_result(temp_storage),
            None => Ok(Datum::Null),
        }
    }
}

impl<E, B: AsColumnType> AsColumnType for Result<B, E> {
    fn as_column_type() -> ColumnType {
        B::as_column_type()
    }
}

impl<'a, E, B: DatumType<'a, E>> DatumType<'a, E> for Result<B, E> {
    fn nullable() -> bool {
        B::nullable()
    }
    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        B::try_from_result(res).map(Ok)
    }
    fn into_result(self, temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        self.and_then(|inner| inner.into_result(temp_storage))
    }
}

/// Macro to derive DatumType for all Datum variants that are simple Copy types
macro_rules! impl_datum_type_copy {
    ($lt:lifetime, $native:ty, $variant:ident) => {
        impl<$lt> AsColumnType for $native {
            fn as_column_type() -> ColumnType {
                ScalarType::$variant.nullable(false)
            }
        }

        impl<$lt, E> DatumType<$lt, E> for $native {
            fn nullable() -> bool {
                false
            }

            fn try_from_result(res: Result<Datum<$lt>, E>) -> Result<Self, Result<Datum<$lt>, E>> {
                match res {
                    Ok(Datum::$variant(f)) => Ok(f.into()),
                    _ => Err(res),
                }
            }

            fn into_result(self, _temp_storage: &$lt RowArena) -> Result<Datum<$lt>, E> {
                Ok(Datum::$variant(self.into()))
            }
        }
    };
    ($native:ty, $variant:ident) => {
        impl_datum_type_copy!('a, $native, $variant);
    };
}

impl_datum_type_copy!(f32, Float32);
impl_datum_type_copy!(f64, Float64);
impl_datum_type_copy!(i16, Int16);
impl_datum_type_copy!(i32, Int32);
impl_datum_type_copy!(i64, Int64);
impl_datum_type_copy!(u16, UInt16);
impl_datum_type_copy!(u32, UInt32);
impl_datum_type_copy!(u64, UInt64);
impl_datum_type_copy!(Interval, Interval);
impl_datum_type_copy!(Date, Date);
impl_datum_type_copy!(NaiveTime, Time);
impl_datum_type_copy!(CheckedTimestamp<NaiveDateTime>, Timestamp);
impl_datum_type_copy!(CheckedTimestamp<DateTime<Utc>>, TimestampTz);
impl_datum_type_copy!(Uuid, Uuid);
impl_datum_type_copy!('a, &'a str, String);
impl_datum_type_copy!('a, &'a [u8], Bytes);
impl_datum_type_copy!(crate::Timestamp, MzTimestamp);

impl<'a, E> DatumType<'a, E> for Datum<'a> {
    fn nullable() -> bool {
        true
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(datum) => Ok(datum),
            _ => Err(res),
        }
    }

    fn into_result(self, _temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(self)
    }
}

impl<'a, E> DatumType<'a, E> for DatumList<'a> {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::List(list)) => Ok(list),
            _ => Err(res),
        }
    }

    fn into_result(self, _temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(Datum::List(self))
    }
}

impl<'a, E> DatumType<'a, E> for DatumMap<'a> {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::Map(map)) => Ok(map),
            _ => Err(res),
        }
    }

    fn into_result(self, _temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(Datum::Map(self))
    }
}

impl<'a, E> DatumType<'a, E> for Range<'a> {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::Range(range)) => Ok(range),
            _ => Err(res),
        }
    }

    fn into_result(self, _temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(Datum::Range(self))
    }
}

impl AsColumnType for bool {
    fn as_column_type() -> ColumnType {
        ScalarType::Bool.nullable(false)
    }
}

impl<'a, E> DatumType<'a, E> for bool {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::True) => Ok(true),
            Ok(Datum::False) => Ok(false),
            _ => Err(res),
        }
    }

    fn into_result(self, _temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        if self {
            Ok(Datum::True)
        } else {
            Ok(Datum::False)
        }
    }
}

impl AsColumnType for String {
    fn as_column_type() -> ColumnType {
        ScalarType::String.nullable(false)
    }
}

impl<'a, E> DatumType<'a, E> for String {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::String(s)) => Ok(s.to_owned()),
            _ => Err(res),
        }
    }

    fn into_result(self, temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(Datum::String(temp_storage.push_string(self)))
    }
}

impl AsColumnType for Vec<u8> {
    fn as_column_type() -> ColumnType {
        ScalarType::Bytes.nullable(false)
    }
}

impl<'a, E> DatumType<'a, E> for Vec<u8> {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::Bytes(b)) => Ok(b.to_owned()),
            _ => Err(res),
        }
    }

    fn into_result(self, temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(Datum::Bytes(temp_storage.push_bytes(self)))
    }
}

impl AsColumnType for Numeric {
    fn as_column_type() -> ColumnType {
        ScalarType::Numeric { max_scale: None }.nullable(false)
    }
}

impl<'a, E> DatumType<'a, E> for Numeric {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::Numeric(n)) => Ok(n.into_inner()),
            _ => Err(res),
        }
    }

    fn into_result(self, _temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(Datum::from(self))
    }
}

impl AsColumnType for PgLegacyChar {
    fn as_column_type() -> ColumnType {
        ScalarType::PgLegacyChar.nullable(false)
    }
}

impl<'a, E> DatumType<'a, E> for PgLegacyChar {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::UInt8(a)) => Ok(PgLegacyChar(a)),
            _ => Err(res),
        }
    }

    fn into_result(self, _temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(Datum::UInt8(self.0))
    }
}

impl AsColumnType for Oid {
    fn as_column_type() -> ColumnType {
        ScalarType::Oid.nullable(false)
    }
}

impl<'a, E> DatumType<'a, E> for Oid {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::UInt32(a)) => Ok(Oid(a)),
            _ => Err(res),
        }
    }

    fn into_result(self, _temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(Datum::UInt32(self.0))
    }
}

impl AsColumnType for RegClass {
    fn as_column_type() -> ColumnType {
        ScalarType::RegClass.nullable(false)
    }
}

impl<'a, E> DatumType<'a, E> for RegClass {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::UInt32(a)) => Ok(RegClass(a)),
            _ => Err(res),
        }
    }

    fn into_result(self, _temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(Datum::UInt32(self.0))
    }
}

impl AsColumnType for RegProc {
    fn as_column_type() -> ColumnType {
        ScalarType::RegProc.nullable(false)
    }
}

impl<'a, E> DatumType<'a, E> for RegProc {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::UInt32(a)) => Ok(RegProc(a)),
            _ => Err(res),
        }
    }

    fn into_result(self, _temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(Datum::UInt32(self.0))
    }
}

impl AsColumnType for RegType {
    fn as_column_type() -> ColumnType {
        ScalarType::RegType.nullable(false)
    }
}

impl<'a, E> DatumType<'a, E> for RegType {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::UInt32(a)) => Ok(RegType(a)),
            _ => Err(res),
        }
    }

    fn into_result(self, _temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(Datum::UInt32(self.0))
    }
}

impl<'a, E> DatumType<'a, E> for Char<&'a str> {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::String(a)) => Ok(Char(a)),
            _ => Err(res),
        }
    }

    fn into_result(self, _temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(Datum::String(self.0))
    }
}

impl<'a, E> DatumType<'a, E> for Char<String> {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::String(a)) => Ok(Char(a.to_owned())),
            _ => Err(res),
        }
    }

    fn into_result(self, temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(Datum::String(temp_storage.push_string(self.0)))
    }
}

impl<'a, E> DatumType<'a, E> for VarChar<&'a str> {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::String(a)) => Ok(VarChar(a)),
            _ => Err(res),
        }
    }

    fn into_result(self, _temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(Datum::String(self.0))
    }
}

impl<'a, E> DatumType<'a, E> for VarChar<String> {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(Datum::String(a)) => Ok(VarChar(a.to_owned())),
            _ => Err(res),
        }
    }

    fn into_result(self, temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(Datum::String(temp_storage.push_string(self.0)))
    }
}

impl<'a, E> DatumType<'a, E> for Jsonb {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        Ok(JsonbRef::try_from_result(res)?.to_owned())
    }

    fn into_result(self, temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(temp_storage.push_unary_row(self.into_row()))
    }
}

impl AsColumnType for Jsonb {
    fn as_column_type() -> ColumnType {
        ScalarType::Jsonb.nullable(false)
    }
}

impl<'a, E> DatumType<'a, E> for JsonbRef<'a> {
    fn nullable() -> bool {
        false
    }

    fn try_from_result(res: Result<Datum<'a>, E>) -> Result<Self, Result<Datum<'a>, E>> {
        match res {
            Ok(
                d @ (Datum::JsonNull
                | Datum::True
                | Datum::False
                | Datum::Numeric(_)
                | Datum::String(_)
                | Datum::List(_)
                | Datum::Map(_)),
            ) => Ok(JsonbRef::from_datum(d)),
            _ => Err(res),
        }
    }

    fn into_result(self, _temp_storage: &'a RowArena) -> Result<Datum<'a>, E> {
        Ok(self.into_datum())
    }
}

impl<'a> AsColumnType for JsonbRef<'a> {
    fn as_column_type() -> ColumnType {
        ScalarType::Jsonb.nullable(false)
    }
}

impl<'a> ScalarType {
    /// Returns the contained numeric maximum scale.
    ///
    /// # Panics
    ///
    /// Panics if the scalar type is not [`ScalarType::Numeric`].
    pub fn unwrap_numeric_max_scale(&self) -> Option<NumericMaxScale> {
        match self {
            ScalarType::Numeric { max_scale } => *max_scale,
            _ => panic!("ScalarType::unwrap_numeric_scale called on {:?}", self),
        }
    }

    /// Returns the [`ScalarType`] of elements in a [`ScalarType::List`].
    ///
    /// # Panics
    ///
    /// Panics if called on anything other than a [`ScalarType::List`].
    pub fn unwrap_list_element_type(&self) -> &ScalarType {
        match self {
            ScalarType::List { element_type, .. } => element_type,
            _ => panic!("ScalarType::unwrap_list_element_type called on {:?}", self),
        }
    }

    /// Returns the [`ScalarType`] of elements in the nth layer a
    /// [`ScalarType::List`].
    ///
    /// For example, in an `int list list`, the:
    /// - 0th layer is `int list list`
    /// - 1st layer is `int list`
    /// - 2nd layer is `int`
    ///
    /// # Panics
    ///
    /// Panics if the nth-1 layer is anything other than a
    /// [`ScalarType::List`].
    pub fn unwrap_list_nth_layer_type(&self, layer: usize) -> &ScalarType {
        if layer == 0 {
            return self;
        }
        match self {
            ScalarType::List { element_type, .. } => {
                element_type.unwrap_list_nth_layer_type(layer - 1)
            }
            _ => panic!(
                "ScalarType::unwrap_list_nth_layer_type called on {:?}",
                self
            ),
        }
    }

    /// Returns vector of [`ScalarType`] elements in a [`ScalarType::Record`].
    ///
    /// # Panics
    ///
    /// Panics if called on anything other than a [`ScalarType::Record`].
    pub fn unwrap_record_element_type(&self) -> Vec<&ScalarType> {
        match self {
            ScalarType::Record { fields, .. } => {
                fields.iter().map(|(_, t)| &t.scalar_type).collect_vec()
            }
            _ => panic!(
                "ScalarType::unwrap_record_element_type called on {:?}",
                self
            ),
        }
    }

    /// Returns number of dimensions/axes (also known as "rank") on a
    /// [`ScalarType::List`].
    ///
    /// # Panics
    ///
    /// Panics if called on anything other than a [`ScalarType::List`].
    pub fn unwrap_list_n_layers(&self) -> usize {
        let mut descender = self.unwrap_list_element_type();
        let mut layers = 1;

        while let ScalarType::List { element_type, .. } = descender {
            layers += 1;
            descender = element_type;
        }

        layers
    }

    /// Returns `self` with any type modifiers removed.
    ///
    /// Namely, this should set optional scales or limits to `None`.
    pub fn without_modifiers(&self) -> ScalarType {
        use ScalarType::*;
        match self {
            List {
                element_type,
                custom_id: None,
            } => List {
                element_type: Box::new(element_type.without_modifiers()),
                custom_id: None,
            },
            Map {
                value_type,
                custom_id: None,
            } => Map {
                value_type: Box::new(value_type.without_modifiers()),
                custom_id: None,
            },
            Record {
                fields,
                custom_id: None,
            } => {
                let fields = fields
                    .iter()
                    .map(|(column_name, column_type)| {
                        (
                            column_name.clone(),
                            ColumnType {
                                scalar_type: column_type.scalar_type.without_modifiers(),
                                nullable: column_type.nullable,
                            },
                        )
                    })
                    .collect_vec();
                Record {
                    fields,
                    custom_id: None,
                }
            }
            Array(a) => Array(Box::new(a.without_modifiers())),
            Numeric { .. } => Numeric { max_scale: None },
            // Char's default length should not be `Some(1)`, but instead `None`
            // to support Char values of different lengths in e.g. lists.
            Char { .. } => Char { length: None },
            VarChar { .. } => VarChar { max_length: None },
            v => v.clone(),
        }
    }

    /// Returns the [`ScalarType`] of elements in a [`ScalarType::Array`] or the
    /// elements of a vector type, e.g. [`ScalarType::Int16`] for
    /// [`ScalarType::Int2Vector`].
    ///
    /// # Panics
    ///
    /// Panics if called on anything other than a [`ScalarType::Array`] or
    /// [`ScalarType::Int2Vector`].
    pub fn unwrap_array_element_type(&self) -> &ScalarType {
        match self {
            ScalarType::Array(s) => &**s,
            ScalarType::Int2Vector => &ScalarType::Int16,
            _ => panic!("ScalarType::unwrap_array_element_type called on {:?}", self),
        }
    }

    /// Returns the [`ScalarType`] of elements in a [`ScalarType::Array`],
    /// [`ScalarType::Int2Vector`], or [`ScalarType::List`].
    ///
    /// # Panics
    ///
    /// Panics if called on anything other than a [`ScalarType::Array`],
    /// [`ScalarType::Int2Vector`], or [`ScalarType::List`].
    pub fn unwrap_collection_element_type(&self) -> &ScalarType {
        match self {
            ScalarType::Array(element_type) => element_type,
            ScalarType::Int2Vector => &ScalarType::Int16,
            ScalarType::List { element_type, .. } => element_type,
            _ => panic!(
                "ScalarType::unwrap_collection_element_type called on {:?}",
                self
            ),
        }
    }

    /// Returns the [`ScalarType`] of values in a [`ScalarType::Map`].
    ///
    /// # Panics
    ///
    /// Panics if called on anything other than a [`ScalarType::Map`].
    pub fn unwrap_map_value_type(&self) -> &ScalarType {
        match self {
            ScalarType::Map { value_type, .. } => &**value_type,
            _ => panic!("ScalarType::unwrap_map_value_type called on {:?}", self),
        }
    }

    /// Returns the length of a [`ScalarType::Char`].
    ///
    /// # Panics
    ///
    /// Panics if called on anything other than a [`ScalarType::Char`].
    pub fn unwrap_char_length(&self) -> Option<CharLength> {
        match self {
            ScalarType::Char { length, .. } => *length,
            _ => panic!("ScalarType::unwrap_char_length called on {:?}", self),
        }
    }

    /// Returns the max length of a [`ScalarType::VarChar`].
    ///
    /// # Panics
    ///
    /// Panics if called on anything other than a [`ScalarType::VarChar`].
    pub fn unwrap_varchar_max_length(&self) -> Option<VarCharMaxLength> {
        match self {
            ScalarType::VarChar { max_length, .. } => *max_length,
            _ => panic!("ScalarType::unwrap_varchar_max_length called on {:?}", self),
        }
    }

    /// Returns a "near match" of `self`, which are types that are implicitly
    /// castable from `self` and offer a means to leverage Materialize's type
    /// system to achieve more reasonable approaches to unifying types.
    ///
    /// However, it's very important to not blithely accept the `near_match`,
    /// which can be suboptimal/unnecessary, e.g. in the case of an already
    /// homogeneous group.
    ///
    /// The feature is preferrable in MZ, but unnecessary in PG because PG's
    /// type system offers totally linear progression through the complexity of
    /// types. e.g. with numbers, there is a linear progression in the domain
    /// each can represent. However, MZ's support for unsigned integers create a
    /// non-linear type system, i.e. while the magnitude of `Int32` and
    /// `UInt32`'s domains are the same, they are not equal.
    ///
    /// Without this feature, Materialize will:
    /// - Guess that a mixute of the same width of int and uint cannot be
    ///   coerced to a homogeneous type.
    /// - Select the `Float64` based version of common binary functions (e.g.
    ///   `=`), which introduces an unexpected float cast to integer values.
    ///
    /// Note that if adding any near matches besides unsigned ints, consider
    /// extending/generalizing how `guess_best_common_type` uses this function.
    pub fn near_match(&self) -> Option<&'static ScalarType> {
        match self {
            ScalarType::UInt16 => Some(&ScalarType::Int32),
            ScalarType::UInt32 => Some(&ScalarType::Int64),
            ScalarType::UInt64 => Some(&ScalarType::Numeric { max_scale: None }),
            _ => None,
        }
    }

    /// Derives a column type from this scalar type with the specified
    /// nullability.
    pub fn nullable(self, nullable: bool) -> ColumnType {
        ColumnType {
            nullable,
            scalar_type: self,
        }
    }

    /// Returns whether or not `self` is a vector-like type, i.e.
    /// [`ScalarType::Array`], [`ScalarType::Int2Vector`], or
    /// [`ScalarType::List`], irrespective of its element type.
    pub fn is_vec(&self) -> bool {
        matches!(
            self,
            ScalarType::Array(_) | ScalarType::Int2Vector | ScalarType::List { .. }
        )
    }

    pub fn is_custom_type(&self) -> bool {
        use ScalarType::*;
        match self {
            List {
                element_type: t,
                custom_id,
            }
            | Map {
                value_type: t,
                custom_id,
            } => custom_id.is_some() || t.is_custom_type(),
            Record {
                fields, custom_id, ..
            } => {
                custom_id.is_some()
                    || fields
                        .iter()
                        .map(|(_, t)| t)
                        .any(|t| t.scalar_type.is_custom_type())
            }
            _ => false,
        }
    }

    /// Determines equality among scalar types that acknowledges custom OIDs,
    /// but ignores other embedded values.
    ///
    /// In most situations, you want to use `base_eq` rather than `ScalarType`'s
    /// implementation of `Eq`. `base_eq` expresses the semantics of direct type
    /// interoperability whereas `Eq` expresses an exact comparison between the
    /// values.
    ///
    /// For instance, `base_eq` signals that e.g. two [`ScalarType::Numeric`]
    /// values can be added together, irrespective of their embedded scale. In
    /// contrast, two `Numeric` values with different scales are never `Eq` to
    /// one another.
    pub fn base_eq(&self, other: &ScalarType) -> bool {
        self.eq_inner(other, false)
    }

    // Determines equality among scalar types that ignores any custom OIDs or
    // embedded values.
    pub fn structural_eq(&self, other: &ScalarType) -> bool {
        self.eq_inner(other, true)
    }

    pub fn eq_inner(&self, other: &ScalarType, structure_only: bool) -> bool {
        use ScalarType::*;
        match (self, other) {
            (
                List {
                    element_type: l,
                    custom_id: oid_l,
                },
                List {
                    element_type: r,
                    custom_id: oid_r,
                },
            )
            | (
                Map {
                    value_type: l,
                    custom_id: oid_l,
                },
                Map {
                    value_type: r,
                    custom_id: oid_r,
                },
            ) => l.eq_inner(r, structure_only) && (oid_l == oid_r || structure_only),
            (Array(a), Array(b)) => a.eq_inner(b, structure_only),
            (
                Record {
                    fields: fields_a,
                    custom_id: oid_a,
                },
                Record {
                    fields: fields_b,
                    custom_id: oid_b,
                },
            ) => {
                (oid_a == oid_b || structure_only)
                    && fields_a.len() == fields_b.len()
                    && fields_a
                        .iter()
                        .zip(fields_b)
                        // Ignore nullability.
                        .all(|(a, b)| {
                            (a.0 == b.0 || structure_only)
                                && a.1.scalar_type.eq_inner(&b.1.scalar_type, structure_only)
                        })
            }
            (s, o) => ScalarBaseType::from(s) == ScalarBaseType::from(o),
        }
    }

    /// Returns various interesting datums for a ScalarType (max, min, 0 values,
    /// etc.).
    pub fn interesting_datums(&self) -> impl Iterator<Item = Datum> {
        // TODO: Is there a better way than packing everything into Lazys and
        // Rows? `&[Datum::X(x)]` doesn't seem to work because some Datum
        // variants need to call non-const functions to construct themselves.

        static BOOL: Lazy<Row> = Lazy::new(|| Row::pack_slice(&[Datum::True, Datum::False]));
        static INT16: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::Int16(0),
                Datum::Int16(1),
                Datum::Int16(-1),
                Datum::Int16(i16::MIN),
                Datum::Int16(i16::MIN + 1),
                Datum::Int16(i16::MAX),
            ])
        });
        static INT32: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::Int32(0),
                Datum::Int32(1),
                Datum::Int32(-1),
                Datum::Int32(i32::MIN),
                Datum::Int32(i32::MIN + 1),
                Datum::Int32(i32::MAX),
            ])
        });
        static INT64: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::Int64(0),
                Datum::Int64(1),
                Datum::Int64(-1),
                Datum::Int64(i64::MIN),
                Datum::Int64(i64::MIN + 1),
                Datum::Int64(i64::MAX),
            ])
        });
        static UINT16: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[Datum::UInt16(0), Datum::UInt16(1), Datum::UInt16(u16::MAX)])
        });
        static UINT32: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[Datum::UInt32(0), Datum::UInt32(1), Datum::UInt32(u32::MAX)])
        });
        static UINT64: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[Datum::UInt64(0), Datum::UInt64(1), Datum::UInt64(u64::MAX)])
        });
        static FLOAT32: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::Float32(OrderedFloat(0.0)),
                Datum::Float32(OrderedFloat(1.0)),
                Datum::Float32(OrderedFloat(-1.0)),
                Datum::Float32(OrderedFloat(f32::MIN)),
                Datum::Float32(OrderedFloat(f32::MIN_POSITIVE)),
                Datum::Float32(OrderedFloat(f32::MAX)),
                Datum::Float32(OrderedFloat(f32::EPSILON)),
                Datum::Float32(OrderedFloat(f32::NAN)),
                Datum::Float32(OrderedFloat(f32::INFINITY)),
                Datum::Float32(OrderedFloat(f32::NEG_INFINITY)),
            ])
        });
        static FLOAT64: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::Float64(OrderedFloat(0.0)),
                Datum::Float64(OrderedFloat(1.0)),
                Datum::Float64(OrderedFloat(-1.0)),
                Datum::Float64(OrderedFloat(f64::MIN)),
                Datum::Float64(OrderedFloat(f64::MIN_POSITIVE)),
                Datum::Float64(OrderedFloat(f64::MAX)),
                Datum::Float64(OrderedFloat(f64::EPSILON)),
                Datum::Float64(OrderedFloat(f64::NAN)),
                Datum::Float64(OrderedFloat(f64::INFINITY)),
                Datum::Float64(OrderedFloat(f64::NEG_INFINITY)),
            ])
        });
        static NUMERIC: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::Numeric(OrderedDecimal(Numeric::from(0.0))),
                Datum::Numeric(OrderedDecimal(Numeric::from(1.0))),
                Datum::Numeric(OrderedDecimal(Numeric::from(-1.0))),
                Datum::Numeric(OrderedDecimal(Numeric::from(f64::MIN))),
                Datum::Numeric(OrderedDecimal(Numeric::from(f64::MIN_POSITIVE))),
                Datum::Numeric(OrderedDecimal(Numeric::from(f64::MAX))),
                Datum::Numeric(OrderedDecimal(Numeric::from(f64::EPSILON))),
                Datum::Numeric(OrderedDecimal(Numeric::from(f64::NAN))),
                Datum::Numeric(OrderedDecimal(Numeric::from(f64::INFINITY))),
                Datum::Numeric(OrderedDecimal(Numeric::from(f64::NEG_INFINITY))),
            ])
        });
        static DATE: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::Date(Date::from_pg_epoch(0).unwrap()),
                Datum::Date(Date::from_pg_epoch(Date::LOW_DAYS).unwrap()),
                Datum::Date(Date::from_pg_epoch(Date::HIGH_DAYS).unwrap()),
            ])
        });
        static TIME: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::Time(NaiveTime::from_hms_micro_opt(0, 0, 0, 0).unwrap()),
                Datum::Time(NaiveTime::from_hms_micro_opt(23, 59, 59, 999_999).unwrap()),
            ])
        });
        static TIMESTAMP: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::Timestamp(
                    NaiveDateTime::from_timestamp_opt(0, 0)
                        .unwrap()
                        .try_into()
                        .unwrap(),
                ),
                Datum::Timestamp(
                    crate::adt::timestamp::LOW_DATE
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                        .try_into()
                        .unwrap(),
                ),
                Datum::Timestamp(
                    crate::adt::timestamp::HIGH_DATE
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                        .try_into()
                        .unwrap(),
                ),
            ])
        });
        static TIMESTAMPTZ: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::TimestampTz(
                    DateTime::from_utc(NaiveDateTime::from_timestamp_opt(0, 0).unwrap(), Utc)
                        .try_into()
                        .unwrap(),
                ),
                Datum::TimestampTz(
                    DateTime::from_utc(
                        crate::adt::timestamp::LOW_DATE
                            .and_hms_opt(0, 0, 0)
                            .unwrap(),
                        Utc,
                    )
                    .try_into()
                    .unwrap(),
                ),
                Datum::TimestampTz(
                    DateTime::from_utc(
                        crate::adt::timestamp::HIGH_DATE
                            .and_hms_opt(0, 0, 0)
                            .unwrap(),
                        Utc,
                    )
                    .try_into()
                    .unwrap(),
                ),
            ])
        });
        static INTERVAL: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::Interval(Interval::new(0, 0, 0)),
                Datum::Interval(Interval::new(i32::MIN, i32::MIN, i64::MIN)),
                Datum::Interval(Interval::new(i32::MAX, i32::MAX, i64::MAX)),
            ])
        });
        static PGLEGACYCHAR: Lazy<Row> =
            Lazy::new(|| Row::pack_slice(&[Datum::UInt8(u8::MIN), Datum::UInt8(u8::MAX)]));
        static BYTES: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[Datum::Bytes(&[]), Datum::Bytes(&[0]), Datum::Bytes(&[255])])
        });
        static STRING: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::String(""),
                Datum::String(" "),
                Datum::String("'"),
                Datum::String("\""),
                Datum::String("."),
            ])
        });
        static CHAR: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::String(" "),
                Datum::String("'"),
                Datum::String("\""),
                Datum::String("."),
            ])
        });
        static JSONB: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::True,
                Datum::False,
                Datum::JsonNull,
                Datum::String(""),
                Datum::String(" "),
                Datum::String("'"),
                Datum::String("\""),
                Datum::Numeric(OrderedDecimal(Numeric::from(0.0))),
                Datum::Numeric(OrderedDecimal(Numeric::from(1.0))),
                Datum::Numeric(OrderedDecimal(Numeric::from(-1.0))),
                Datum::Numeric(OrderedDecimal(Numeric::from(i64::MAX))),
                Datum::Numeric(OrderedDecimal(Numeric::from(i64::MIN))),
                // TODO: Add List, Map.
            ])
        });
        static UUID: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::Uuid(Uuid::from_u128(u128::MIN)),
                Datum::Uuid(Uuid::from_u128(u128::MAX)),
            ])
        });
        static ARRAY: Lazy<Row> = Lazy::new(|| Row::pack_slice(&[]));
        static LIST: Lazy<Row> = Lazy::new(|| Row::pack_slice(&[]));
        static RECORD: Lazy<Row> = Lazy::new(|| Row::pack_slice(&[]));
        static OID: Lazy<Row> = Lazy::new(|| Row::pack_slice(&[]));
        static MAP: Lazy<Row> = Lazy::new(|| Row::pack_slice(&[]));
        static REGPROC: Lazy<Row> = Lazy::new(|| Row::pack_slice(&[]));
        static REGTYPE: Lazy<Row> = Lazy::new(|| Row::pack_slice(&[]));
        static REGCLASS: Lazy<Row> = Lazy::new(|| Row::pack_slice(&[]));
        static INT2VECTOR: Lazy<Row> = Lazy::new(|| Row::pack_slice(&[]));
        static MZTIMESTAMP: Lazy<Row> = Lazy::new(|| {
            Row::pack_slice(&[
                Datum::MzTimestamp(crate::Timestamp::MIN),
                Datum::MzTimestamp(crate::Timestamp::MAX),
            ])
        });

        match self {
            ScalarType::Bool => (*BOOL).iter(),
            ScalarType::Int16 => (*INT16).iter(),
            ScalarType::Int32 => (*INT32).iter(),
            ScalarType::Int64 => (*INT64).iter(),
            ScalarType::UInt16 => (*UINT16).iter(),
            ScalarType::UInt32 => (*UINT32).iter(),
            ScalarType::UInt64 => (*UINT64).iter(),
            ScalarType::Float32 => (*FLOAT32).iter(),
            ScalarType::Float64 => (*FLOAT64).iter(),
            ScalarType::Numeric { .. } => (*NUMERIC).iter(),
            ScalarType::Date => (*DATE).iter(),
            ScalarType::Time => (*TIME).iter(),
            ScalarType::Timestamp => (*TIMESTAMP).iter(),
            ScalarType::TimestampTz => (*TIMESTAMPTZ).iter(),
            ScalarType::Interval => (*INTERVAL).iter(),
            ScalarType::PgLegacyChar => (*PGLEGACYCHAR).iter(),
            ScalarType::Bytes => (*BYTES).iter(),
            ScalarType::String => (*STRING).iter(),
            ScalarType::Char { .. } => (*CHAR).iter(),
            ScalarType::VarChar { .. } => (*STRING).iter(),
            ScalarType::Jsonb => (*JSONB).iter(),
            ScalarType::Uuid => (*UUID).iter(),
            ScalarType::Array(_) => (*ARRAY).iter(),
            ScalarType::List { .. } => (*LIST).iter(),
            ScalarType::Record { .. } => (*RECORD).iter(),
            ScalarType::Oid => (*OID).iter(),
            ScalarType::Map { .. } => (*MAP).iter(),
            ScalarType::RegProc => (*REGPROC).iter(),
            ScalarType::RegType => (*REGTYPE).iter(),
            ScalarType::RegClass => (*REGCLASS).iter(),
            ScalarType::Int2Vector => (*INT2VECTOR).iter(),
            ScalarType::MzTimestamp => (*MZTIMESTAMP).iter(),
        }
    }

    /// Returns all non-parameterized types and some versions of some
    /// parameterized types.
    pub fn enumerate() -> &'static [Self] {
        // TODO: Is there a compile-time way to make sure any new
        // non-parameterized types get added here?
        &[
            ScalarType::Bool,
            ScalarType::Int16,
            ScalarType::Int32,
            ScalarType::Int64,
            ScalarType::UInt16,
            ScalarType::UInt32,
            ScalarType::UInt64,
            ScalarType::Float32,
            ScalarType::Float64,
            ScalarType::Numeric {
                max_scale: Some(NumericMaxScale(
                    crate::adt::numeric::NUMERIC_DATUM_MAX_PRECISION,
                )),
            },
            ScalarType::Date,
            ScalarType::Time,
            ScalarType::Timestamp,
            ScalarType::TimestampTz,
            ScalarType::Interval,
            ScalarType::PgLegacyChar,
            ScalarType::Bytes,
            ScalarType::String,
            ScalarType::Char {
                length: Some(CharLength(1)),
            },
            ScalarType::VarChar { max_length: None },
            ScalarType::Jsonb,
            ScalarType::Uuid,
            ScalarType::Oid,
            ScalarType::RegProc,
            ScalarType::RegType,
            ScalarType::RegClass,
            ScalarType::Int2Vector,
            ScalarType::MzTimestamp,
            // TODO: Fill in some variants of these.
            /*
            ScalarType::Array(_),
            ScalarType::List {
                element_type: todo!(),
                custom_id: todo!(),
            },
            ScalarType::Record {
                fields: todo!(),
                custom_id: todo!(),
            },
            ScalarType::Map {
                value_type: todo!(),
                custom_id: todo!(),
            },
            */
        ]
    }
}

// See the chapter "Generating Recurisve Data" from the proptest book:
// https://altsysrq.github.io/proptest-book/proptest/tutorial/recursive.html
impl Arbitrary for ScalarType {
    type Parameters = ();
    type Strategy = BoxedStrategy<ScalarType>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        // A strategy for generating the leaf cases of ScalarType
        let leaf = prop_oneof![
            Just(ScalarType::Bool),
            Just(ScalarType::Int16),
            Just(ScalarType::Int32),
            Just(ScalarType::Int64),
            Just(ScalarType::Float32),
            Just(ScalarType::Float64),
            any::<Option<NumericMaxScale>>()
                .prop_map(|max_scale| ScalarType::Numeric { max_scale }),
            Just(ScalarType::Date),
            Just(ScalarType::Time),
            Just(ScalarType::Timestamp),
            Just(ScalarType::TimestampTz),
            Just(ScalarType::Interval),
            Just(ScalarType::PgLegacyChar),
            Just(ScalarType::Bytes),
            Just(ScalarType::String),
            any::<Option<CharLength>>().prop_map(|length| ScalarType::Char { length }),
            any::<Option<VarCharMaxLength>>()
                .prop_map(|max_length| ScalarType::VarChar { max_length }),
            Just(ScalarType::Jsonb),
            Just(ScalarType::Uuid),
            Just(ScalarType::Oid),
            Just(ScalarType::RegProc),
            Just(ScalarType::RegType),
            Just(ScalarType::RegClass),
            Just(ScalarType::Int2Vector),
        ];

        leaf.prop_recursive(
            2, // For now, just go one level deep
            4,
            5,
            |inner| {
                prop_oneof![
                    // Array
                    inner.clone().prop_map(|x| ScalarType::Array(Box::new(x))),
                    // List
                    (inner.clone(), any::<Option<GlobalId>>()).prop_map(|(x, id)| {
                        ScalarType::List {
                            element_type: Box::new(x),
                            custom_id: id,
                        }
                    }),
                    // Map
                    (inner.clone(), any::<Option<GlobalId>>()).prop_map(|(x, id)| {
                        ScalarType::Map {
                            value_type: Box::new(x),
                            custom_id: id,
                        }
                    }),
                    // Record
                    {
                        // Now we have to use `inner` to create a Record type. First we
                        // create strategy that creates ColumnType.
                        let column_type_strat =
                            (inner, any::<bool>()).prop_map(|(scalar_type, nullable)| ColumnType {
                                scalar_type,
                                nullable,
                            });

                        // Then we use that to create the fields of the record case.
                        // fields has type vec<(ColumnName,ColumnType)>
                        let fields_strat =
                            prop::collection::vec((any::<ColumnName>(), column_type_strat), 0..10);

                        // Now we combine it with the default strategies to get Records.
                        (fields_strat, any::<Option<GlobalId>>()).prop_map(|(fields, custom_id)| {
                            ScalarType::Record { fields, custom_id }
                        })
                    }
                ]
            },
        )
        .boxed()
    }
}

static EMPTY_ARRAY_ROW: Lazy<Row> = Lazy::new(|| {
    let mut row = Row::default();
    row.packer()
        .push_array(&[], iter::empty::<Datum>())
        .expect("array known to be valid");
    row
});
static EMPTY_LIST_ROW: Lazy<Row> = Lazy::new(|| {
    let mut row = Row::default();
    row.packer().push_list(iter::empty::<Datum>());
    row
});

impl Datum<'_> {
    pub fn empty_array() -> Datum<'static> {
        EMPTY_ARRAY_ROW.unpack_first()
    }

    pub fn empty_list() -> Datum<'static> {
        EMPTY_LIST_ROW.unpack_first()
    }
}

/// A mirror type for [`Datum`] that can be proptest-generated.
#[derive(Debug, PartialEq, Clone)]
pub enum PropDatum {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),

    Date(Date),
    Time(chrono::NaiveTime),
    Timestamp(CheckedTimestamp<chrono::NaiveDateTime>),
    TimestampTz(CheckedTimestamp<chrono::DateTime<chrono::Utc>>),

    Interval(Interval),
    Numeric(Numeric),

    Bytes(Vec<u8>),
    String(String),

    Array(PropArray),
    List(PropList),
    Map(PropDict),

    JsonNull,
    Uuid(Uuid),
    Dummy,
}

/// Generate an arbitrary [`PropDatum`].
///
/// TODO: we also need a variant that can be parameterized by
/// a [`ColumnType`] or a [`ColumnType`] [`Strategy`].
pub fn arb_datum() -> BoxedStrategy<PropDatum> {
    let leaf = prop_oneof![
        Just(PropDatum::Null),
        any::<bool>().prop_map(PropDatum::Bool),
        any::<i16>().prop_map(PropDatum::Int16),
        any::<i32>().prop_map(PropDatum::Int32),
        any::<i64>().prop_map(PropDatum::Int64),
        any::<f32>().prop_map(PropDatum::Float32),
        any::<f64>().prop_map(PropDatum::Float64),
        arb_date().prop_map(PropDatum::Date),
        add_arb_duration(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap())
            .prop_map(PropDatum::Time),
        add_arb_duration(chrono::NaiveDateTime::from_timestamp_opt(0, 0).unwrap())
            .prop_map(|t| PropDatum::Timestamp(CheckedTimestamp::from_timestamplike(t).unwrap())),
        add_arb_duration(chrono::Utc.timestamp_opt(0, 0).unwrap())
            .prop_map(|t| PropDatum::TimestampTz(CheckedTimestamp::from_timestamplike(t).unwrap())),
        arb_interval().prop_map(PropDatum::Interval),
        arb_numeric().prop_map(PropDatum::Numeric),
        prop::collection::vec(any::<u8>(), 1024).prop_map(PropDatum::Bytes),
        ".*".prop_map(PropDatum::String),
        Just(PropDatum::JsonNull),
        Just(PropDatum::Uuid(Uuid::nil())),
        Just(PropDatum::Dummy)
    ];
    leaf.prop_recursive(3, 8, 16, |inner| {
        prop_oneof!(
            arb_array(inner.clone()).prop_map(PropDatum::Array),
            arb_list(inner.clone()).prop_map(PropDatum::List),
            arb_dict(inner).prop_map(PropDatum::Map),
        )
    })
    .boxed()
}

fn arb_array_dimension() -> BoxedStrategy<ArrayDimension> {
    (1..4_usize)
        .prop_map(|length| ArrayDimension {
            lower_bound: 1,
            length,
        })
        .boxed()
}

#[derive(Debug, PartialEq, Clone)]
pub struct PropArray(Row, Vec<PropDatum>);

fn arb_array(element_strategy: BoxedStrategy<PropDatum>) -> BoxedStrategy<PropArray> {
    prop::collection::vec(
        arb_array_dimension(),
        1..(crate::adt::array::MAX_ARRAY_DIMENSIONS as usize),
    )
    .prop_flat_map(move |dimensions| {
        let n_elts: usize = dimensions.iter().map(|d| d.length).product();
        (
            Just(dimensions),
            prop::collection::vec(element_strategy.clone(), n_elts),
        )
    })
    .prop_map(|(dimensions, elements)| {
        let element_datums: Vec<Datum<'_>> = elements.iter().map(|pd| pd.into()).collect();
        let mut row = Row::default();
        row.packer()
            .push_array(&dimensions, element_datums)
            .unwrap();
        PropArray(row, elements)
    })
    .boxed()
}

#[derive(Debug, PartialEq, Clone)]
pub struct PropList(Row, Vec<PropDatum>);

fn arb_list(element_strategy: BoxedStrategy<PropDatum>) -> BoxedStrategy<PropList> {
    prop::collection::vec(element_strategy, 1..50)
        .prop_map(|elements| {
            let element_datums: Vec<Datum<'_>> = elements.iter().map(|pd| pd.into()).collect();
            let mut row = Row::default();
            row.packer().push_list(element_datums.iter());
            PropList(row, elements)
        })
        .boxed()
}

#[derive(Debug, PartialEq, Clone)]
pub struct PropDict(Row, Vec<(String, PropDatum)>);

fn arb_dict(element_strategy: BoxedStrategy<PropDatum>) -> BoxedStrategy<PropDict> {
    prop::collection::vec((".*", element_strategy), 1..50)
        .prop_map(|mut entries| {
            entries.sort_by_key(|(k, _)| k.clone());
            entries.dedup_by_key(|(k, _)| k.clone());
            let mut row = Row::default();
            let entry_iter = entries.iter().map(|(k, v)| (k.as_str(), Datum::from(v)));
            row.packer().push_dict(entry_iter);
            PropDict(row, entries)
        })
        .boxed()
}

fn arb_date() -> BoxedStrategy<Date> {
    (Date::LOW_DAYS..Date::HIGH_DAYS)
        .prop_map(move |days| Date::from_pg_epoch(days).unwrap())
        .boxed()
}

fn arb_interval() -> BoxedStrategy<Interval> {
    (
        any::<i32>(),
        any::<i32>(),
        ((((i64::from(i32::MIN) * 60) - 59) * 60) * 1_000_000 - 59_999_999
            ..(((i64::from(i32::MAX) * 60) + 59) * 60) * 1_000_000 + 59_999_999),
    )
        .prop_map(|(months, days, micros)| Interval {
            months,
            days,
            micros,
        })
        .boxed()
}

fn add_arb_duration<T: 'static + Copy + Add<chrono::Duration> + std::fmt::Debug>(
    to: T,
) -> BoxedStrategy<T::Output>
where
    T::Output: std::fmt::Debug,
{
    any::<i64>()
        .prop_map(move |v| to + chrono::Duration::nanoseconds(v))
        .boxed()
}

fn arb_numeric() -> BoxedStrategy<Numeric> {
    any::<i128>()
        .prop_map(|v| Numeric::try_from(v).unwrap())
        .boxed()
}

impl<'a> From<&'a PropDatum> for Datum<'a> {
    fn from(pd: &'a PropDatum) -> Self {
        use PropDatum::*;
        match pd {
            Null => Datum::Null,
            Bool(b) => Datum::from(*b),
            Int16(i) => Datum::from(*i),
            Int32(i) => Datum::from(*i),
            Int64(i) => Datum::from(*i),
            Float32(f) => Datum::from(*f),
            Float64(f) => Datum::from(*f),
            Date(d) => Datum::from(*d),
            Time(t) => Datum::from(*t),
            Timestamp(t) => Datum::from(*t),
            TimestampTz(t) => Datum::from(*t),
            Interval(i) => Datum::from(*i),
            Numeric(s) => Datum::from(*s),
            Bytes(b) => Datum::from(&b[..]),
            String(s) => Datum::from(s.as_str()),
            Array(PropArray(row, _)) => {
                let array = row.unpack_first().unwrap_array();
                Datum::Array(array)
            }
            List(PropList(row, _)) => {
                let list = row.unpack_first().unwrap_list();
                Datum::List(list)
            }
            Map(PropDict(row, _)) => {
                let map = row.unpack_first().unwrap_map();
                Datum::Map(map)
            }
            JsonNull => Datum::JsonNull,
            Uuid(u) => Datum::from(*u),
            Dummy => Datum::Dummy,
        }
    }
}

#[test]
fn verify_base_eq_record_nullability() {
    let s1 = ScalarType::Record {
        fields: vec![(
            "c".into(),
            ColumnType {
                scalar_type: ScalarType::Bool,
                nullable: true,
            },
        )],
        custom_id: None,
    };
    let s2 = ScalarType::Record {
        fields: vec![(
            "c".into(),
            ColumnType {
                scalar_type: ScalarType::Bool,
                nullable: false,
            },
        )],
        custom_id: None,
    };
    let s3 = ScalarType::Record {
        fields: vec![],
        custom_id: None,
    };
    assert!(s1.base_eq(&s2));
    assert!(!s1.base_eq(&s3));
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_proto::protobuf_roundtrip;

    proptest! {
       #[test]
        fn scalar_type_protobuf_roundtrip(expect in any::<ScalarType>() ) {
            let actual = protobuf_roundtrip::<_, ProtoScalarType>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #[test]
        fn array_packing_unpacks_correctly(array in arb_array(arb_datum())) {
            let PropArray(row, elts) = array;
            let datums: Vec<Datum<'_>> = elts.iter().map(|e| e.into()).collect();
            let unpacked_datums: Vec<Datum<'_>> = row.unpack_first().unwrap_array().elements().iter().collect();
            assert_eq!(unpacked_datums, datums);
        }

        #[test]
        fn list_packing_unpacks_correctly(array in arb_list(arb_datum())) {
            let PropList(row, elts) = array;
            let datums: Vec<Datum<'_>> = elts.iter().map(|e| e.into()).collect();
            let unpacked_datums: Vec<Datum<'_>> = row.unpack_first().unwrap_list().iter().collect();
            assert_eq!(unpacked_datums, datums);
        }

        #[test]
        fn dict_packing_unpacks_correctly(array in arb_dict(arb_datum())) {
            let PropDict(row, elts) = array;
            let datums: Vec<(&str, Datum<'_>)> = elts.iter().map(|(k, e)| (k.as_str(), e.into())).collect();
            let unpacked_datums: Vec<(&str, Datum<'_>)> = row.unpack_first().unwrap_map().iter().collect();
            assert_eq!(unpacked_datums, datums);
        }

        #[test]
        fn row_packing_roundtrips_single_valued(prop_datums in prop::collection::vec(arb_datum(), 1..100)) {
            let datums: Vec<Datum<'_>> = prop_datums.iter().map(|pd| pd.into()).collect();
            let row = Row::pack(&datums);
            let unpacked = row.unpack();
            assert_eq!(datums, unpacked);
        }
    }
}
