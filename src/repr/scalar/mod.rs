// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::fmt::{self, Write};
use std::hash::{Hash, Hasher};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone, Utc};
use failure::format_err;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use self::datetime::Interval;
use self::decimal::Significand;
use crate::{ColumnType, DatumDict, DatumList};

pub mod datetime;
pub mod decimal;
pub mod regex;

/// A literal value.
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
    /// A Date
    Date(NaiveDate),
    /// A DateTime
    Timestamp(NaiveDateTime),
    /// A time zone aware DateTime
    TimestampTz(DateTime<Utc>),
    /// A span of time
    ///
    /// Either a concrete number of seconds, or an abstract number of Months
    Interval(Interval),
    /// An exact decimal number, possibly with a fractional component, with up
    /// to 38 digits of precision.
    Decimal(Significand),
    /// A sequence of untyped bytes.
    Bytes(&'a [u8]),
    /// A sequence of Unicode codepoints encoded as UTF-8.
    String(&'a str),
    /// A heterogenous sequence of Datums
    List(DatumList<'a>),
    /// A mapping from string keys to Datums,
    Dict(DatumDict<'a>),
    /// Json null does not behave like SQL null :'(
    JsonNull,
}

impl<'a> Datum<'a> {
    pub fn is_null(&self) -> bool {
        match self {
            Datum::Null => true,
            _ => false,
        }
    }

    /// Create a Datum representing a Date
    ///
    /// Errors if the the combination of year/month/day is invalid
    pub fn from_ymd(year: i32, month: u8, day: u8) -> Result<Datum<'static>, failure::Error> {
        let d = NaiveDate::from_ymd_opt(year, month.into(), day.into())
            .ok_or_else(|| format_err!("Invalid date: {}-{:02}-{:02}", year, month, day))?;
        Ok(Datum::Date(d))
    }

    /// Create a Datum representing a Timestamp
    ///
    /// Errors if the the combination of year/month/day is invalid
    pub fn from_ymd_hms_nano(
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nano: u32,
    ) -> Result<Datum<'static>, failure::Error> {
        let d = NaiveDate::from_ymd_opt(year, month.into(), day.into())
            .ok_or_else(|| format_err!("Invalid date: {}-{:02}-{:02}", year, month, day))?
            .and_hms_nano_opt(hour.into(), minute.into(), second.into(), nano)
            .ok_or_else(|| {
                format_err!(
                    "Invalid time: {:02}:{:02}:{:02}.{} (in date {}-{:02}-{:02})",
                    hour,
                    minute,
                    second,
                    nano,
                    year,
                    month,
                    day
                )
            })?;
        Ok(Datum::Timestamp(d))
    }

    /// Create a Datum representing a TimestampTz
    #[allow(clippy::too_many_arguments)]
    pub fn from_ymd_hms_nano_tz_offset(
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nano: u32,
        timezone_offset_second: i64,
    ) -> Result<Datum<'static>, failure::Error> {
        let d = NaiveDate::from_ymd_opt(year, month.into(), day.into())
            .ok_or_else(|| format_err!("Invalid date: {}-{:02}-{:02}", year, month, day))?
            .and_hms_nano_opt(hour.into(), minute.into(), second.into(), nano)
            .ok_or_else(|| {
                format_err!(
                    "Invalid time: {:02}:{:02}:{:02}.{} (in date {}-{:02}-{:02})",
                    hour,
                    minute,
                    second,
                    nano,
                    year,
                    month,
                    day
                )
            })?;
        let offset = FixedOffset::east(timezone_offset_second as i32);
        let dt_fixed_offset = offset
            .from_local_datetime(&d)
            .earliest()
            .ok_or_else(|| format_err!("Invalid tz conversion"))?;
        Ok(Datum::TimestampTz(DateTime::<Utc>::from_utc(
            dt_fixed_offset.naive_utc(),
            Utc,
        )))
    }

    pub fn unwrap_bool(&self) -> bool {
        match self {
            Datum::False => false,
            Datum::True => true,
            _ => panic!("Datum::unwrap_bool called on {:?}", self),
        }
    }

    pub fn unwrap_int32(&self) -> i32 {
        match self {
            Datum::Int32(i) => *i,
            _ => panic!("Datum::unwrap_int32 called on {:?}", self),
        }
    }

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

    pub fn unwrap_float32(&self) -> f32 {
        match self {
            Datum::Float32(f) => f.into_inner(),
            _ => panic!("Datum::unwrap_float32 called on {:?}", self),
        }
    }

    pub fn unwrap_float64(&self) -> f64 {
        match self {
            Datum::Float64(f) => f.into_inner(),
            _ => panic!("Datum::unwrap_float64 called on {:?}", self),
        }
    }

    pub fn unwrap_date(&self) -> chrono::NaiveDate {
        match self {
            Datum::Date(d) => *d,
            _ => panic!("Datum::unwrap_date called on {:?}", self),
        }
    }

    pub fn unwrap_timestamp(&self) -> chrono::NaiveDateTime {
        match self {
            Datum::Timestamp(ts) => *ts,
            _ => panic!("Datum::unwrap_timestamp called on {:?}", self),
        }
    }

    pub fn unwrap_timestamptz(&self) -> chrono::DateTime<Utc> {
        match self {
            Datum::TimestampTz(ts) => *ts,
            _ => panic!("Datum::unwrap_timestamptz called on {:?}", self),
        }
    }

    pub fn unwrap_interval(&self) -> Interval {
        match self {
            Datum::Interval(iv) => *iv,
            _ => panic!("Datum::unwrap_interval called on {:?}", self),
        }
    }

    pub fn unwrap_decimal(&self) -> Significand {
        match self {
            Datum::Decimal(d) => *d,
            _ => panic!("Datum::unwrap_decimal called on {:?}", self),
        }
    }

    pub fn unwrap_str(&self) -> &'a str {
        match self {
            Datum::String(s) => s,
            _ => panic!("Datum::unwrap_string called on {:?}", self),
        }
    }

    pub fn unwrap_bytes(&self) -> &'a [u8] {
        match self {
            Datum::Bytes(b) => b,
            _ => panic!("Datum::unwrap_bytes called on {:?}", self),
        }
    }

    pub fn unwrap_list(&self) -> DatumList<'a> {
        match self {
            Datum::List(list) => *list,
            _ => panic!("Datum::unwrap_list called on {:?}", self),
        }
    }

    pub fn unwrap_dict(&self) -> DatumDict<'a> {
        match self {
            Datum::Dict(dict) => *dict,
            _ => panic!("Datum::unwrap_dict called on {:?}", self),
        }
    }

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
                    (Datum::Null, ScalarType::Null) => true,
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
        let n_secs = duration.num_seconds();
        Datum::Interval(Interval::Duration {
            is_positive: n_secs >= 0,
            duration: std::time::Duration::new(
                n_secs.abs() as u64,
                (duration.num_nanoseconds().unwrap_or(0) % 1_000_000_000) as u32,
            ),
        })
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
        }
    }
}

/// The fundamental type of a [`Datum`].
///
/// A fundamental type is what is typically thought of as a type, like "Int32"
/// or "String." The full [`ColumnType`] struct bundles additional information, like
/// an optional default value and nullability, that must also be considered part
/// of a datum's type.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, Serialize, Deserialize, Ord, PartialOrd)]
pub enum ScalarType {
    /// The type of a datum that can only be null.
    ///
    /// This is uncommon. Most [`Datum::Null`]s appear with a non-null type.
    Null,
    Bool,
    Int32,
    Int64,
    Float32,
    Float64,
    /// An exact decimal number with a specified precision and scale. The
    /// precision constrains the total number of digits in the number, while the
    /// scale specifies the number of digits after the decimal point. The
    /// maximum precision is [`decimal::MAX_DECIMAL_PRECISION`]. The scale must
    /// be less than or equal to the precision.
    Decimal(u8, u8),
    Date,
    Timestamp,
    TimestampTz,
    /// A possibly-negative time span
    ///
    /// Represented by the [`Interval`] enum
    Interval,
    Bytes,
    String,
    /// Json behaves like postgres' jsonb type but is stored as Datum::JsonNull/True/False/String/Float64/List/Dict.
    /// The sql type system is responsible for preventing these being used as normal sql datums without casting.
    Jsonb,
}

impl<'a> ScalarType {
    pub fn unwrap_decimal_parts(&self) -> (u8, u8) {
        match self {
            ScalarType::Decimal(p, s) => (*p, *s),
            _ => panic!("ScalarType::unwrap_decimal_parts called on {:?}", self),
        }
    }

    pub fn dummy_datum(&self) -> Datum<'a> {
        match self {
            ScalarType::Null => Datum::Null,
            ScalarType::Bool => Datum::False,
            ScalarType::Int32 => Datum::Int32(0),
            ScalarType::Int64 => Datum::Int64(0),
            ScalarType::Float32 => Datum::Float32(OrderedFloat(0.0)),
            ScalarType::Float64 => Datum::Float64(OrderedFloat(0.0)),
            ScalarType::Decimal(_, _) => Datum::Decimal(Significand::new(0)),
            ScalarType::Date => Datum::Date(NaiveDate::from_ymd(1, 1, 1)),
            ScalarType::Timestamp => Datum::Timestamp(NaiveDateTime::from_timestamp(0, 0)),
            ScalarType::TimestampTz => {
                Datum::TimestampTz(DateTime::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc))
            }
            ScalarType::Interval => Datum::Interval(Interval::Months(0)),
            ScalarType::Bytes => Datum::Bytes(&[]),
            ScalarType::String => Datum::String(""),
            ScalarType::Jsonb => Datum::JsonNull,
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

            (Null, Null)
            | (Bool, Bool)
            | (Int32, Int32)
            | (Int64, Int64)
            | (Float32, Float32)
            | (Float64, Float64)
            | (Date, Date)
            | (Timestamp, Timestamp)
            | (TimestampTz, TimestampTz)
            | (Interval, Interval)
            | (Bytes, Bytes)
            | (String, String)
            | (Jsonb, Jsonb) => true,

            (Null, _)
            | (Bool, _)
            | (Int32, _)
            | (Int64, _)
            | (Float32, _)
            | (Float64, _)
            | (Decimal(_, _), _)
            | (Date, _)
            | (Timestamp, _)
            | (TimestampTz, _)
            | (Interval, _)
            | (Bytes, _)
            | (String, _)
            | (Jsonb, _) => false,
        }
    }
}

impl Hash for ScalarType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use ScalarType::*;
        match self {
            Null => state.write_u8(0),
            Bool => state.write_u8(1),
            Int32 => state.write_u8(2),
            Int64 => state.write_u8(3),
            Float32 => state.write_u8(4),
            Float64 => state.write_u8(5),
            Decimal(_, s) => {
                // TODO(benesch): we should properly implement decimal precision
                // tracking, or just remove it.
                state.write_u8(6);
                state.write_u8(*s);
            }
            Date => state.write_u8(7),
            Timestamp => state.write_u8(8),
            TimestampTz => state.write_u8(9),
            Interval => state.write_u8(10),
            Bytes => state.write_u8(11),
            String => state.write_u8(12),
            Jsonb => state.write_u8(13),
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
            Null => f.write_str("null"),
            Bool => f.write_str("bool"),
            Int32 => f.write_str("i32"),
            Int64 => f.write_str("i64"),
            Float32 => f.write_str("f32"),
            Float64 => f.write_str("f64"),
            Decimal(p, s) => write!(f, "decimal({}, {})", p, s),
            Date => f.write_str("date"),
            Timestamp => f.write_str("timestamp"),
            TimestampTz => f.write_str("timestamptz"),
            Interval => f.write_str("interval"),
            Bytes => f.write_str("bytes"),
            String => f.write_str("string"),
            Jsonb => f.write_str("json"),
        }
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}{}",
            self.scalar_type,
            if self.nullable { "?" } else { "" }
        )
    }
}
