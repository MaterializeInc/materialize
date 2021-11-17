// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{self, Write};
use std::hash::Hash;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use dec::OrderedDecimal;
use enum_kinds::EnumKind;
use itertools::Itertools;
use lazy_static::lazy_static;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use lowertest::MzEnumReflect;

use crate::adt::array::Array;
use crate::adt::interval::Interval;
use crate::adt::numeric::Numeric;
use crate::{ColumnName, ColumnType, DatumList, DatumMap};
use crate::{Row, RowArena};

/// A single value.
///
/// Note that `Datum` must always derive [`Eq`] to enforce equality with
/// `repr::Row`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
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
    /// A 32-bit floating point number.
    Float32(OrderedFloat<f32>),
    /// A 64-bit floating point number.
    Float64(OrderedFloat<f64>),
    /// A date.
    Date(NaiveDate),
    /// A time.
    Time(NaiveTime),
    /// A date and time, without a timezone.
    Timestamp(NaiveDateTime),
    /// A date and time, with a timezone.
    TimestampTz(DateTime<Utc>),
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
    // Keep `Null` last so that it sorts last, to match the default sort order
    // in PostgreSQL.
    /// An unknown value.
    Null,
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
    pub fn unwrap_date(&self) -> chrono::NaiveDate {
        match self {
            Datum::Date(d) => *d,
            _ => panic!("Datum::unwrap_date called on {:?}", self),
        }
    }

    /// Unwraps the time value within this datum.
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
    pub fn unwrap_timestamp(&self) -> chrono::NaiveDateTime {
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
    pub fn unwrap_timestamptz(&self) -> chrono::DateTime<Utc> {
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

    /// Reports whether this datum is an instance of the specified column type.
    pub fn is_instance_of(self, column_type: &ColumnType) -> bool {
        fn is_instance_of_scalar(datum: Datum, scalar_type: &ScalarType) -> bool {
            if let ScalarType::Jsonb = scalar_type {
                // json type checking
                match datum {
                    Datum::JsonNull
                    | Datum::False
                    | Datum::True
                    | Datum::Int64(_)
                    | Datum::Float64(_)
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
                    (Datum::Int32(_), ScalarType::Oid) => true,
                    (Datum::Int32(_), ScalarType::RegProc) => true,
                    (Datum::Int32(_), _) => false,
                    (Datum::Int64(_), ScalarType::Int64) => true,
                    (Datum::Int64(_), _) => false,
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
                            Datum::Array(_) => is_instance_of_scalar(e, scalar_type),
                            _ => is_instance_of_scalar(e, t),
                        })
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

impl<'a> From<Numeric> for Datum<'a> {
    fn from(n: Numeric) -> Datum<'a> {
        Datum::Numeric(OrderedDecimal(n))
    }
}

impl<'a> From<chrono::Duration> for Datum<'a> {
    fn from(duration: chrono::Duration) -> Datum<'a> {
        Datum::Interval(
            Interval::new(
                0,
                duration.num_seconds(),
                duration.num_nanoseconds().unwrap_or(0) % 1_000_000_000,
            )
            .unwrap(),
        )
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

impl<'a> From<NaiveDate> for Datum<'a> {
    fn from(d: NaiveDate) -> Datum<'a> {
        Datum::Date(d)
    }
}

impl<'a> From<NaiveTime> for Datum<'a> {
    fn from(t: NaiveTime) -> Datum<'a> {
        Datum::Time(t)
    }
}

impl<'a> From<NaiveDateTime> for Datum<'a> {
    fn from(dt: NaiveDateTime) -> Datum<'a> {
        Datum::Timestamp(dt)
    }
}

impl<'a> From<DateTime<Utc>> for Datum<'a> {
    fn from(dt: DateTime<Utc>) -> Datum<'a> {
        Datum::TimestampTz(dt)
    }
}

impl<'a> From<Uuid> for Datum<'a> {
    fn from(uuid: Uuid) -> Datum<'a> {
        Datum::Uuid(uuid)
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

#[derive(Debug)]
pub struct WithArena<'a, T> {
    arena: &'a RowArena,
    data: T,
}

impl<'a, T> WithArena<'a, T> {
    pub fn new(arena: &'a RowArena, data: T) -> Self {
        WithArena { arena, data }
    }
}

/// Blanket implementation for types that don't need access to an allocation context
impl<'a, T> From<WithArena<'a, T>> for Datum<'a>
where
    Datum<'a>: From<T>,
{
    fn from(context: WithArena<'a, T>) -> Datum<'a> {
        context.data.into()
    }
}

impl<'a> From<WithArena<'a, String>> for Datum<'a> {
    fn from(context: WithArena<'a, String>) -> Datum<'a> {
        Datum::String(context.arena.push_string(context.data))
    }
}

impl<'a> From<WithArena<'a, Option<String>>> for Datum<'a> {
    fn from(context: WithArena<'a, Option<String>>) -> Datum<'a> {
        match context.data {
            Some(b) => Datum::String(context.arena.push_string(b)),
            None => Datum::Null,
        }
    }
}

impl<'a> From<WithArena<'a, Vec<u8>>> for Datum<'a> {
    fn from(context: WithArena<'a, Vec<u8>>) -> Datum<'a> {
        Datum::Bytes(context.arena.push_bytes(context.data))
    }
}

impl<'a> From<WithArena<'a, Option<Vec<u8>>>> for Datum<'a> {
    fn from(context: WithArena<'a, Option<Vec<u8>>>) -> Datum<'a> {
        match context.data {
            Some(b) => Datum::Bytes(context.arena.push_bytes(b)),
            None => Datum::Null,
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
            Datum::JsonNull => f.write_str("json_null"),
            Datum::Dummy => f.write_str("dummy"),
        }
    }
}

/// The type of a [`Datum`].
///
/// There is a direct correspondence between `Datum` variants and `ScalarType`
/// variants.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Ord,
    PartialOrd,
    Hash,
    EnumKind,
    MzEnumReflect,
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
    /// The type of [`Datum::Float32`].
    Float32,
    /// The type of [`Datum::Float64`].
    Float64,
    /// The type of [`Datum::Numeric`].
    ///
    /// `Numeric` values cannot exceed [`NUMERIC_DATUM_MAX_PRECISION`] digits of
    /// precision.
    ///
    /// This type additionally specifies the scale of the decimal. The scale
    /// specifies the number of digits after the decimal point. The scale must
    /// be less than or equal to the maximum precision.
    ///
    /// [`NUMERIC_DATUM_MAX_PRECISION`]:
    /// crate::adt::numeric::NUMERIC_DATUM_MAX_PRECISION
    Numeric { scale: Option<u8> },
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
    /// The type of [`Datum::Bytes`].
    Bytes,
    /// The type of [`Datum::String`].
    String,
    /// Stored as [`Datum::String`], but expresses a fixed-width, blank-padded
    /// string.
    ///
    /// Note that a `length` of `None` is used in special cases, such as
    /// creating lists.
    Char { length: Option<usize> },
    /// Stored as [`Datum::String`], but can optionally express a limit on the string's length.
    VarChar { length: Option<usize> },
    /// The type of a datum that may represent any valid JSON value.
    ///
    /// Valid datum variants for this type are:
    ///
    ///   * [`Datum::Null`]
    ///   * [`Datum::False`]
    ///   * [`Datum::True`]
    ///   * [`Datum::String`]
    ///   * [`Datum::Float64`]
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
        custom_oid: Option<u32>,
    },
    /// An ordered and named sequence of datums.
    Record {
        /// The names and types of the fields of the record, in order from left
        /// to right.
        fields: Vec<(ColumnName, ColumnType)>,
        custom_oid: Option<u32>,
        custom_name: Option<String>,
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
        custom_oid: Option<u32>,
    },
    /// A PostgreSQL function name.
    RegProc,
    /// A PostgreSQL type name.
    RegType,
}

/// [FromTy] is a utility trait for [ScalarType] that defines a mapping between a Rust type T and
/// its runtime representation as a ScalarType. Not all ScalarType variants have a 1-1 mapping to a
/// Rust type but for those variants can simply be ignored.
///
/// The main usecase of FromTy is to use rustc's inference to lift type information from compile
/// time to runtime, a primitive form of reflection. See the implementation of the `fn_unary` macro
/// in `expr` for an example use of this trait.
pub trait FromTy<T> {
    fn from_ty() -> Self;
}

impl FromTy<bool> for ScalarType {
    fn from_ty() -> Self {
        Self::Bool
    }
}

impl FromTy<String> for ScalarType {
    fn from_ty() -> Self {
        Self::String
    }
}

impl FromTy<Vec<u8>> for ScalarType {
    fn from_ty() -> Self {
        Self::Bytes
    }
}

impl FromTy<f32> for ScalarType {
    fn from_ty() -> Self {
        Self::Float32
    }
}

impl FromTy<f64> for ScalarType {
    fn from_ty() -> Self {
        Self::Float64
    }
}

impl FromTy<i16> for ScalarType {
    fn from_ty() -> Self {
        Self::Int16
    }
}

impl FromTy<i32> for ScalarType {
    fn from_ty() -> Self {
        Self::Int32
    }
}

impl FromTy<i64> for ScalarType {
    fn from_ty() -> Self {
        Self::Int64
    }
}

impl FromTy<DateTime<Utc>> for ScalarType {
    fn from_ty() -> Self {
        Self::TimestampTz
    }
}

impl<'a> ScalarType {
    /// Returns the contained numeric scale.
    ///
    /// # Panics
    ///
    /// Panics if the scalar type is not [`ScalarType::Numeric`].
    pub fn unwrap_numeric_scale(&self) -> Option<u8> {
        match self {
            ScalarType::Numeric { scale } => *scale,
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

    /// Returns number of dimensions/axes (also known as "rank") on a
    /// [`ScalarType::List`].
    ///
    /// # Panics
    ///
    /// Panics if called on anything other than a [`ScalarType::List`].
    pub fn unwrap_list_n_dims(&self) -> usize {
        let mut descender = self.unwrap_list_element_type();
        let mut dims = 1;

        while let ScalarType::List { element_type, .. } = descender {
            dims += 1;
            descender = element_type;
        }

        dims
    }

    /// Returns `self` with any embedded values set to a value appropriate for a
    /// collection of the type. Namely, this should set optional scales or
    /// limits to `None`.
    pub fn default_embedded_value(&self) -> ScalarType {
        use ScalarType::*;
        match self {
            List {
                element_type,
                custom_oid: None,
            } => List {
                element_type: Box::new(element_type.default_embedded_value()),
                custom_oid: None,
            },
            Map {
                value_type,
                custom_oid: None,
            } => Map {
                value_type: Box::new(value_type.default_embedded_value()),
                custom_oid: None,
            },
            Array(a) => Array(Box::new(a.default_embedded_value())),
            Numeric { .. } => Numeric { scale: None },
            // Char's default length should not be `Some(1)`, but instead `None`
            // to support Char values of different lengths in e.g. lists.
            Char { .. } => Char { length: None },
            VarChar { .. } => VarChar { length: None },
            v => v.clone(),
        }
    }

    /// Returns the [`ScalarType`] of elements in a [`ScalarType::Array`].
    ///
    /// # Panics
    ///
    /// Panics if called on anything other than a [`ScalarType::Array`].
    pub fn unwrap_array_element_type(&self) -> &ScalarType {
        match self {
            ScalarType::Array(s) => &**s,
            _ => panic!("ScalarType::unwrap_array_element_type called on {:?}", self),
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

    /// Returns the length of a [`ScalarType::Char`] or [`ScalarType::VarChar`].
    ///
    /// # Panics
    ///
    /// Panics if called on anything other than a [`ScalarType::Char`] or [`ScalarType::VarChar`].
    pub fn unwrap_char_varchar_length(&self) -> Option<usize> {
        match self {
            ScalarType::Char { length, .. } | ScalarType::VarChar { length } => *length,
            _ => panic!(
                "ScalarType::unwrap_char_varchar_length called on {:?}",
                self
            ),
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
    /// [`ScalarType::List`] or [`ScalarType::Array`], irrespective of its
    /// element type.
    pub fn is_vec(&self) -> bool {
        matches!(self, ScalarType::List { .. } | ScalarType::Array(_))
    }

    pub fn is_custom_type(&self) -> bool {
        use ScalarType::*;
        match self {
            List {
                element_type: t,
                custom_oid,
            }
            | Map {
                value_type: t,
                custom_oid,
            } => custom_oid.is_some() || t.is_custom_type(),
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
        use ScalarType::*;
        match (self, other) {
            (
                List {
                    element_type: l,
                    custom_oid: oid_l,
                },
                List {
                    element_type: r,
                    custom_oid: oid_r,
                },
            )
            | (
                Map {
                    value_type: l,
                    custom_oid: oid_l,
                },
                Map {
                    value_type: r,
                    custom_oid: oid_r,
                },
            ) => l.base_eq(r) && oid_l == oid_r,

            (Array(a), Array(b)) => a.base_eq(b),
            (
                Record {
                    fields: fields_a,
                    custom_oid: oid_a,
                    custom_name: name_a,
                },
                Record {
                    fields: fields_b,
                    custom_oid: oid_b,
                    custom_name: name_b,
                },
            ) => fields_a.eq(fields_b) && oid_a == oid_b && name_a == name_b,
            (s, o) => ScalarBaseType::from(s) == ScalarBaseType::from(o),
        }
    }

    pub fn is_string_like(&self) -> bool {
        match self {
            ScalarType::String | ScalarType::Char { .. } | ScalarType::VarChar { .. } => true,
            _ => false,
        }
    }
}

lazy_static! {
    static ref EMPTY_ARRAY_ROW: Row = unsafe {
        Row::from_bytes_unchecked(vec![
            22, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ])
    };
    static ref EMPTY_LIST_ROW: Row =
        unsafe { Row::from_bytes_unchecked(vec![23, 0, 0, 0, 0, 0, 0, 0, 0]) };
}

impl Datum<'static> {
    pub fn empty_array() -> Datum<'static> {
        EMPTY_ARRAY_ROW.unpack_first()
    }
    pub fn empty_list() -> Datum<'static> {
        EMPTY_LIST_ROW.unpack_first()
    }
}

// Verify that bytes for static datums with manually stuffed bytes are correct.
#[test]
fn verify_static_datum_bytes<'a>() {
    let arena = crate::RowArena::new();
    {
        let empty_array_datum: Datum = arena.make_datum(|packer| {
            packer
                .push_array::<_, Datum<'a>>(
                    &[crate::adt::array::ArrayDimension {
                        lower_bound: 1,
                        length: 0,
                    }],
                    std::iter::empty(),
                )
                .unwrap();
        });
        if EMPTY_ARRAY_ROW.iter().next().is_none() || Datum::empty_array() != empty_array_datum {
            panic!(
                "expected EMPTY_ARRAY bytes: {:?}",
                Row::pack_slice(&[empty_array_datum]).data()
            );
        }
    }

    {
        let empty_list_datum: Datum = arena.make_datum(|packer| {
            packer.push_list::<_, Datum<'a>>(std::iter::empty());
        });
        if EMPTY_LIST_ROW.iter().next().is_none() || Datum::empty_list() != empty_list_datum {
            panic!(
                "expected EMPTY_LIST bytes: {:?}",
                Row::pack_slice(&[empty_list_datum]).data()
            );
        }
    }
}
