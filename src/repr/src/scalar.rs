// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{self, Write};
use std::hash::{Hash, Hasher};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::adt::decimal::Significand;
use crate::adt::interval::Interval;
use crate::{ColumnName, ColumnType, DatumDict, DatumList};

/// A single value.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum Datum<'a> {
    /// An unknown value.
    Null,
    /// The `false` boolean value.
    False,
    /// The `true` boolean value.
    True,
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
    /// An exact decimal number, possibly with a fractional component, with up
    /// to 38 digits of precision.
    Decimal(Significand),
    /// A sequence of untyped bytes.
    Bytes(&'a [u8]),
    /// A sequence of Unicode codepoints encoded as UTF-8.
    String(&'a str),
    /// A sequence of `Datum`s.
    List(DatumList<'a>),
    /// A mapping from string keys to `Datum`s.
    Dict(DatumDict<'a>),
    /// An unknown value within a JSON-typed `Datum`.
    ///
    /// This variant is distinct from [`Datum::Null`] as a null datum is
    /// distinct from a non-null datum that contains the JSON value `null`.
    JsonNull,
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
}

impl<'a> Datum<'a> {
    /// Reports whether this datum is null (i.e., is [`Datum::Null`]).
    pub fn is_null(&self) -> bool {
        match self {
            Datum::Null => true,
            _ => false,
        }
    }

    /// Unwraps the boolean value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::False`] or [`Datum::True`].
    pub fn unwrap_bool(&self) -> bool {
        match self {
            Datum::False => false,
            Datum::True => true,
            _ => panic!("Datum::unwrap_bool called on {:?}", self),
        }
    }

    /// Unwraps the 32-bit integer value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Int32`].
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
    pub fn unwrap_int64(&self) -> i64 {
        match self {
            Datum::Int64(i) => *i,
            _ => panic!("Datum::unwrap_int64 called on {:?}", self),
        }
    }

    pub fn unwrap_ordered_float32(&self) -> OrderedFloat<f32> {
        match self {
            Datum::Float32(f) => *f,
            _ => panic!("Datum::unwrap_ordered_float32 called on {:?}", self),
        }
    }

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
    pub fn unwrap_interval(&self) -> Interval {
        match self {
            Datum::Interval(iv) => *iv,
            _ => panic!("Datum::unwrap_interval called on {:?}", self),
        }
    }

    /// Unwraps the decimal value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Decimal`].
    pub fn unwrap_decimal(&self) -> Significand {
        match self {
            Datum::Decimal(d) => *d,
            _ => panic!("Datum::unwrap_decimal called on {:?}", self),
        }
    }

    /// Unwraps the string value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::String`].
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
    pub fn unwrap_bytes(&self) -> &'a [u8] {
        match self {
            Datum::Bytes(b) => b,
            _ => panic!("Datum::unwrap_bytes called on {:?}", self),
        }
    }

    /// Unwraps the list value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::List`].
    pub fn unwrap_list(&self) -> DatumList<'a> {
        match self {
            Datum::List(list) => *list,
            _ => panic!("Datum::unwrap_list called on {:?}", self),
        }
    }

    /// Unwraps the dict value within this datum.
    ///
    /// # Panics
    ///
    /// Panics if the datum is not [`Datum::Dict`].
    pub fn unwrap_dict(&self) -> DatumDict<'a> {
        match self {
            Datum::Dict(dict) => *dict,
            _ => panic!("Datum::unwrap_dict called on {:?}", self),
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
                    | Datum::Float64(_)
                    | Datum::String(_) => true,
                    Datum::List(list) => list
                        .iter()
                        .all(|elem| is_instance_of_scalar(elem, scalar_type)),
                    Datum::Dict(dict) => dict
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
                    (Datum::Int32(_), ScalarType::Int32) => true,
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
                    (Datum::Decimal(_), ScalarType::Decimal(_, _)) => true,
                    (Datum::Decimal(_), _) => false,
                    (Datum::Bytes(_), ScalarType::Bytes) => true,
                    (Datum::Bytes(_), _) => false,
                    (Datum::String(_), ScalarType::String) => true,
                    (Datum::String(_), _) => false,
                    (Datum::List(list), ScalarType::List(t)) => list
                        .iter()
                        .all(|e| e.is_null() || is_instance_of_scalar(e, t)),
                    (Datum::List(list), ScalarType::Record { fields }) => list
                        .iter()
                        .zip_eq(fields)
                        .all(|(e, (_, t))| e.is_null() || is_instance_of_scalar(e, t)),
                    (Datum::List(_), _) => false,
                    (Datum::Dict(_), _) => false,
                    (Datum::JsonNull, _) => false,
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

impl From<bool> for Datum<'static> {
    fn from(b: bool) -> Datum<'static> {
        if b {
            Datum::True
        } else {
            Datum::False
        }
    }
}

impl From<i32> for Datum<'static> {
    fn from(i: i32) -> Datum<'static> {
        Datum::Int32(i)
    }
}

impl From<i64> for Datum<'static> {
    fn from(i: i64) -> Datum<'static> {
        Datum::Int64(i)
    }
}

impl From<OrderedFloat<f32>> for Datum<'static> {
    fn from(f: OrderedFloat<f32>) -> Datum<'static> {
        Datum::Float32(f)
    }
}

impl From<OrderedFloat<f64>> for Datum<'static> {
    fn from(f: OrderedFloat<f64>) -> Datum<'static> {
        Datum::Float64(f)
    }
}

impl From<f32> for Datum<'static> {
    fn from(f: f32) -> Datum<'static> {
        Datum::Float32(OrderedFloat(f))
    }
}

impl From<f64> for Datum<'static> {
    fn from(f: f64) -> Datum<'static> {
        Datum::Float64(OrderedFloat(f))
    }
}

impl From<i128> for Datum<'static> {
    fn from(d: i128) -> Datum<'static> {
        Datum::Decimal(Significand::new(d))
    }
}

impl From<Significand> for Datum<'static> {
    fn from(d: Significand) -> Datum<'static> {
        Datum::Decimal(d)
    }
}

impl From<chrono::Duration> for Datum<'static> {
    fn from(duration: chrono::Duration) -> Datum<'static> {
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

impl From<Interval> for Datum<'static> {
    fn from(other: Interval) -> Datum<'static> {
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
            Datum::Int32(num) => write!(f, "{}", num),
            Datum::Int64(num) => write!(f, "{}", num),
            Datum::Float32(num) => write!(f, "{}", num),
            Datum::Float64(num) => write!(f, "{}", num),
            Datum::Date(d) => write!(f, "{}", d),
            Datum::Time(t) => write!(f, "{}", t),
            Datum::Timestamp(t) => write!(f, "{}", t),
            Datum::TimestampTz(t) => write!(f, "{}", t),
            Datum::Interval(iv) => write!(f, "{}", iv),
            Datum::Decimal(sig) => write!(f, "{}dec", sig.as_i128()),
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
            Datum::List(list) => {
                f.write_str("[")?;
                write_delimited(f, ", ", list, |f, e| write!(f, "{}", e))?;
                f.write_str("]")
            }
            Datum::Dict(dict) => {
                f.write_str("{")?;
                write_delimited(f, ", ", dict, |f, (k, v)| write!(f, "{}: {}", k, v))?;
                f.write_str("}")
            }
            Datum::JsonNull => f.write_str("json_null"),
            Datum::Dummy => f.write_str("dummy"),
        }
    }
}

/// The type of a [`Datum`].
///
/// There is a direct correspondence between `Datum` variants and `ScalarType`
/// variants.
#[derive(Clone, Debug, Eq, Serialize, Deserialize, Ord, PartialOrd)]
pub enum ScalarType {
    /// The type of [`Datum::True`] and [`Datum::False`].
    Bool,
    /// The type of [`Datum::Int32`].
    Int32,
    /// The type of [`Datum::Int64`].
    Int64,
    /// The type of [`Datum::Float32`].
    Float32,
    /// The type of [`Datum::Float64`].
    Float64,
    /// The type of [`Datum::Decimal`].
    ///
    /// This type additionally specifies the precision and scale of the decimal
    /// . The precision constrains the total number of digits in the number,
    /// while the scale specifies the number of digits after the decimal point.
    /// The maximum precision is [`MAX_DECIMAL_PRECISION`]. The scale
    /// must be less than or equal to the precision.
    ///
    /// [`MAX_DECIMAL_PRECISION`]: crate::adt::decimal::MAX_DECIMAL_PRECISION
    Decimal(u8, u8),
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
    ///   * [`Datum::Dict`]
    Jsonb,
    /// The type of [`Datum::List`].
    ///
    /// Elements within the list are of the specified type. List elements may
    /// always be [`Datum::Null`].
    List(Box<ScalarType>),
    /// An ordered and named sequence of datums.
    Record {
        /// The names and types of the fields of the record, in order from left
        /// to right.
        fields: Vec<(ColumnName, ScalarType)>,
    },
}

impl<'a> ScalarType {
    /// Returns the contained decimal precision and scale.
    ///
    /// # Panics
    ///
    /// Panics if the scalar type is not [`ScalarType::Decimal`].
    pub fn unwrap_decimal_parts(&self) -> (u8, u8) {
        match self {
            ScalarType::Decimal(p, s) => (*p, *s),
            _ => panic!("ScalarType::unwrap_decimal_parts called on {:?}", self),
        }
    }

    /// Returns a copy of `Self` with any embedded fields "zeroed" out. Meant to
    /// make comparisons easier, allowing you to mimic `std::mem::discriminant`
    /// equality.
    pub fn desaturate(&self) -> ScalarType {
        match self {
            ScalarType::Record { .. } => ScalarType::Record { fields: vec![] },
            ScalarType::Decimal(..) => ScalarType::Decimal(0, 0),
            _ => self.clone(),
        }
    }
}

// TODO(benesch): the implementations of PartialEq and Hash for ScalarType can
// be removed when decimal precision is either fixed or removed.

impl PartialEq for ScalarType {
    fn eq(&self, other: &Self) -> bool {
        use ScalarType::*;
        match (self, other) {
            (Decimal(_, s1), Decimal(_, s2)) => s1 == s2,

            (Bool, Bool)
            | (Int32, Int32)
            | (Int64, Int64)
            | (Float32, Float32)
            | (Float64, Float64)
            | (Date, Date)
            | (Time, Time)
            | (Timestamp, Timestamp)
            | (TimestampTz, TimestampTz)
            | (Interval, Interval)
            | (Bytes, Bytes)
            | (String, String)
            | (Jsonb, Jsonb) => true,

            (List(a), List(b)) => a.eq(b),
            (Record { fields: fields_a }, Record { fields: fields_b }) => fields_a.eq(fields_b),

            (Bool, _)
            | (Int32, _)
            | (Int64, _)
            | (Float32, _)
            | (Float64, _)
            | (Decimal(_, _), _)
            | (Date, _)
            | (Time, _)
            | (Timestamp, _)
            | (TimestampTz, _)
            | (Interval, _)
            | (Bytes, _)
            | (String, _)
            | (Jsonb, _)
            | (List(_), _)
            | (Record { .. }, _) => false,
        }
    }
}

impl Hash for ScalarType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use ScalarType::*;
        match self {
            Bool => state.write_u8(0),
            Int32 => state.write_u8(1),
            Int64 => state.write_u8(2),
            Float32 => state.write_u8(3),
            Float64 => state.write_u8(4),
            Decimal(_, s) => {
                // TODO(benesch): we should properly implement decimal precision
                // tracking, or just remove it.
                state.write_u8(5);
                state.write_u8(*s);
            }
            Date => state.write_u8(6),
            Time => state.write_u8(7),
            Timestamp => state.write_u8(8),
            TimestampTz => state.write_u8(9),
            Interval => state.write_u8(10),
            Bytes => state.write_u8(11),
            String => state.write_u8(12),
            Jsonb => state.write_u8(13),
            List(t) => {
                state.write_u8(14);
                t.hash(state);
            }
            Record { fields } => {
                state.write_u8(15);
                fields.hash(state);
            }
        }
    }
}

impl fmt::Display for ScalarType {
    /// Arbitrary display name for scalars
    ///
    /// Right now the names correspond most closely to Rust names (e.g. i32).
    /// There are other functions in other packages that construct a mapping
    /// between `ScalarType`s and type names in other systems, like PostgreSQL.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ScalarType::*;
        match self {
            Bool => f.write_str("bool"),
            Int32 => f.write_str("i32"),
            Int64 => f.write_str("i64"),
            Float32 => f.write_str("f32"),
            Float64 => f.write_str("f64"),
            Decimal(p, s) => write!(f, "decimal({}, {})", p, s),
            Date => f.write_str("date"),
            Time => f.write_str("time"),
            Timestamp => f.write_str("timestamp"),
            TimestampTz => f.write_str("timestamptz"),
            Interval => f.write_str("interval"),
            Bytes => f.write_str("bytes"),
            String => f.write_str("string"),
            Jsonb => f.write_str("jsonb"),
            List(t) => write!(f, "{}[]", t),
            Record { fields } => {
                f.write_str("record(")?;
                write_delimited(f, ", ", fields, |f, (n, t)| write!(f, "{}: {}", n, t))?;
                f.write_str(")")
            }
        }
    }
}
