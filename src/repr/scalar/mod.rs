// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

pub mod decimal;
pub mod regex;
use chrono::{NaiveDate, NaiveDateTime};
use failure::format_err;
use ordered_float::OrderedFloat;
use pretty::{BoxDoc, Doc};
use serde::{Deserialize, Serialize};
use sqlparser::ast::Interval as SqlInterval;
use std::fmt::{self, Write};
use std::hash::{Hash, Hasher};

use self::decimal::Significand;
use crate::ColumnType;

/// A literal value.
///
/// Note that datums may be scalar, like [`Datum::Int32`], or composite, like
/// [`Datum::Tuple`], but they are always constant.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
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

    pub fn scalar_type(&self) -> ScalarType {
        match self {
            Datum::Null => ScalarType::Null,
            Datum::False => ScalarType::Bool,
            Datum::True => ScalarType::Bool,
            Datum::Int32(_) => ScalarType::Int32,
            Datum::Int64(_) => ScalarType::Int64,
            Datum::Float32(_) => ScalarType::Float32,
            Datum::Float64(_) => ScalarType::Float64,
            Datum::Date(_) => ScalarType::Date,
            Datum::Timestamp(_) => ScalarType::Timestamp,
            Datum::Interval(_) => ScalarType::Interval,
            Datum::Decimal(_) => panic!("don't support decimal"), // todo: figure out what to do here
            Datum::Bytes(_) => ScalarType::Bytes,
            Datum::String(_) => ScalarType::String,
        }
    }

    pub fn is_instance_of(&self, column_type: ColumnType) -> bool {
        match (self, &column_type.scalar_type) {
            (Datum::Null, _) if column_type.nullable => true,
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
            (Datum::Interval(_), ScalarType::Interval) => true,
            (Datum::Interval(_), _) => false,
            (Datum::Decimal(_), ScalarType::Decimal(_, _)) => true,
            (Datum::Decimal(_), _) => false,
            (Datum::Bytes(_), ScalarType::Bytes) => true,
            (Datum::Bytes(_), _) => false,
            (Datum::String(_), ScalarType::String) => true,
            (Datum::String(_), _) => false,
        }
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

impl From<SqlInterval> for Datum<'static> {
    fn from(other: SqlInterval) -> Datum<'static> {
        Datum::Interval(other.into())
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
        }
    }
}

impl<'a> From<Datum<'a>> for Doc<'static, BoxDoc<'static, ()>> {
    fn from(datum: Datum<'a>) -> Doc<'static, BoxDoc<'static, ()>> {
        Doc::text(datum.to_string())
    }
}

/// The fundamental type of a [`Datum`].
///
/// A fundamental type is what is typically thought of as a type, like "Int32"
/// or "String." The full [`ColumnType`] struct bundles additional information, like
/// an optional default value and nullability, that must also be considered part
/// of a datum's type.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, Eq, Serialize, Deserialize, Ord, PartialOrd)]
pub enum ScalarType {
    /// The type of a datum that can only be null.
    ///
    /// This is uncommon. Most [`Datum:Null`]s appear with a different type.
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
    Time,
    Timestamp,
    /// A possibly-negative time span
    ///
    /// Represented by the [`Interval`] enum
    Interval,
    Bytes,
    String,
}

impl ScalarType {
    pub fn unwrap_decimal_parts(self) -> (u8, u8) {
        match self {
            ScalarType::Decimal(p, s) => (p, s),
            _ => panic!("ScalarType::unwrap_decimal_parts called on {:?}", self),
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
            | (Time, Time)
            | (Timestamp, Timestamp)
            | (Interval, Interval)
            | (Bytes, Bytes)
            | (String, String) => true,

            (Null, _)
            | (Bool, _)
            | (Int32, _)
            | (Int64, _)
            | (Float32, _)
            | (Float64, _)
            | (Decimal(_, _), _)
            | (Date, _)
            | (Time, _)
            | (Timestamp, _)
            | (Interval, _)
            | (Bytes, _)
            | (String, _) => false,
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
            Time => state.write_u8(8),
            Timestamp => state.write_u8(9),
            Interval => state.write_u8(10),
            Bytes => state.write_u8(11),
            String => state.write_u8(12),
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
            Time => f.write_str("time"),
            Timestamp => f.write_str("timestamp"),
            Interval => f.write_str("interval"),
            Bytes => f.write_str("bytes"),
            String => f.write_str("string"),
        }
    }
}

/// Either a number of months, or a number of seconds
///
/// Inlined from [`sqlparser::ast::Interval`] so that we can impl deserialize, ord
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Hash, Deserialize)]
pub enum Interval {
    /// A possibly negative number of months for field types like `YEAR`
    Months(i64),
    /// An actual timespan, possibly negative, because why not
    Duration {
        is_positive: bool,
        duration: std::time::Duration,
    },
}

impl Interval {
    /// Computes the year part of the interval.
    ///
    /// The year part is the number of whole years in the interval. For example,
    /// this function returns `3.0` for the interval `3 years 4 months`.
    pub fn years(&self) -> f64 {
        match self {
            Interval::Months(n) => (n / 12) as f64,
            Interval::Duration { .. } => 0.0,
        }
    }

    /// Computes the month part of the interval.
    ///
    /// The whole part is the number of whole months in the interval, modulo 12.
    /// For example, this function returns `4.0` for the interval `3 years 4
    /// months`.
    pub fn months(&self) -> f64 {
        match self {
            Interval::Months(n) => (n % 12) as f64,
            Interval::Duration { .. } => 0.0,
        }
    }

    /// Computes the day part of the interval.
    ///
    /// The day part is the number of whole days in the interval. For example,
    /// this function returns `5.0` for the interval `5 days 4 hours 3 minutes
    /// 2.1 seconds`.
    pub fn days(&self) -> f64 {
        match self {
            Interval::Months(_) => 0.0,
            Interval::Duration { duration, .. } => (duration.as_secs() / (60 * 60 * 24)) as f64,
        }
    }

    /// Computes the hour part of the interval.
    ///
    /// The hour part is the number of whole hours in the interval, modulo 24.
    /// For example, this function returns `4.0` for the interval `5 days 4
    /// hours 3 minutes 2.1 seconds`.
    pub fn hours(&self) -> f64 {
        match self {
            Interval::Months(_) => 0.0,
            Interval::Duration { duration, .. } => ((duration.as_secs() / (60 * 60)) % 24) as f64,
        }
    }

    /// Computes the minute part of the interval.
    ///
    /// The minute part is the number of whole minutes in the interval, modulo
    /// 60. For example, this function returns `3.0` for the interval `5 days 4
    /// hours 3 minutes 2.1 seconds`.
    pub fn minutes(&self) -> f64 {
        match self {
            Interval::Months(_) => 0.0,
            Interval::Duration { duration, .. } => ((duration.as_secs() / 60) % 60) as f64,
        }
    }

    /// Computes the second part of the interval.
    ///
    /// The second part is the number of fractional seconds in the interval,
    /// modulo 60.0.
    pub fn seconds(&self) -> f64 {
        match self {
            Interval::Months(_) => 0.0,
            Interval::Duration { duration, .. } => {
                let s = (duration.as_secs() % 60) as f64;
                let ns = f64::from(duration.subsec_nanos()) / 1e9;
                s + ns
            }
        }
    }
}

impl From<SqlInterval> for Interval {
    fn from(other: SqlInterval) -> Interval {
        match other {
            SqlInterval::Months(count) => Interval::Months(count),
            SqlInterval::Duration {
                is_positive,
                duration,
            } => Interval::Duration {
                is_positive,
                duration,
            },
        }
    }
}

/// Format an interval in a human form
///
/// Example outputs:
///
/// * 1 year
/// * 2 years
/// * 00
/// * 01:00.01
impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Interval::Months(c) => {
                let mut c = *c;
                if c == 0 {
                    f.write_str("0 months")?;
                    return Ok(());
                }
                if c < 0 {
                    f.write_char('-')?;
                }
                c = c.abs();
                if c >= 12 {
                    let years = c / 12;
                    c %= 12;
                    write!(f, "{} year", years)?;
                    if years > 1 {
                        f.write_char('s')?;
                    }
                    if c > 0 {
                        f.write_char(' ')?;
                    }
                }
                if c > 0 {
                    write!(f, "{} month", c)?;
                    if c > 1 {
                        f.write_char('s')?;
                    }
                }
            }
            Interval::Duration {
                is_positive,
                duration,
            } => {
                if !*is_positive {
                    f.write_char('-')?;
                }
                let mut secs = duration.as_secs();
                let nanos = duration.subsec_nanos();
                let mut hours = secs / 3600;
                let mut days = 0;
                if hours > 0 {
                    secs %= 3600;
                    if hours >= 24 {
                        days = hours / 24;
                        hours %= 24;
                        write!(f, "{} day", days)?;
                        if days > 1 {
                            f.write_char('s')?;
                        }
                        f.write_char(' ')?;
                    }
                    write!(f, "{:02}:", hours)?;
                }
                let minutes = secs / 60;
                if minutes > 0 || hours > 0 || days > 0 {
                    secs %= 60;
                    write!(f, "{:02}:", minutes)?;
                }
                write!(f, "{:02}", secs)?;
                if nanos > 0 {
                    write!(f, ".{}", nanos)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn interval_fmt() {
        assert_eq!(&Interval::Months(1).to_string(), "1 month");
        assert_eq!(&Interval::Months(0).to_string(), "0 months");
        assert_eq!(&Interval::Months(12).to_string(), "1 year");
        assert_eq!(&Interval::Months(13).to_string(), "1 year 1 month");
        assert_eq!(&Interval::Months(24).to_string(), "2 years");
        assert_eq!(&Interval::Months(25).to_string(), "2 years 1 month");
        assert_eq!(&Interval::Months(26).to_string(), "2 years 2 months");

        fn dur(is_positive: bool, d: u64) -> String {
            Interval::Duration {
                is_positive,
                duration: std::time::Duration::from_secs(d),
            }
            .to_string()
        }
        assert_eq!(&dur(true, 86_400 * 2), "2 days 00:00:00");
        assert_eq!(&dur(true, 86_400 * 2 + 3_600 * 3), "2 days 03:00:00");
        assert_eq!(
            &dur(true, 86_400 * 2 + 3_600 * 3 + 60 * 45 + 6),
            "2 days 03:45:06"
        );
        assert_eq!(
            &dur(true, 86_400 * 2 + 3_600 * 3 + 60 * 45),
            "2 days 03:45:00"
        );
        assert_eq!(&dur(true, 86_400 * 2 + 6), "2 days 00:00:06");
        assert_eq!(&dur(true, 86_400 * 2 + 60 * 45 + 6), "2 days 00:45:06");
        assert_eq!(&dur(true, 86_400 * 2 + 3_600 * 3 + 6), "2 days 03:00:06");
        assert_eq!(&dur(true, 3_600 * 3 + 60 * 45 + 6), "03:45:06");
        assert_eq!(&dur(true, 3_600 * 3 + 6), "03:00:06");
        assert_eq!(&dur(true, 3_600 * 3), "03:00:00");
        assert_eq!(&dur(true, 60 * 45 + 6), "45:06");
        assert_eq!(&dur(true, 60 * 45), "45:00");
        assert_eq!(&dur(true, 6), "06");

        assert_eq!(&dur(false, 86_400 * 2 + 6), "-2 days 00:00:06");
        assert_eq!(&dur(false, 86_400 * 2 + 60 * 45 + 6), "-2 days 00:45:06");
        assert_eq!(&dur(false, 86_400 * 2 + 3_600 * 3 + 6), "-2 days 03:00:06");
        assert_eq!(&dur(false, 3_600 * 3 + 60 * 45 + 6), "-03:45:06");
        assert_eq!(&dur(false, 3_600 * 3 + 6), "-03:00:06");
        assert_eq!(&dur(false, 3_600 * 3), "-03:00:00");
        assert_eq!(&dur(false, 60 * 45 + 6), "-45:06");
        assert_eq!(&dur(false, 60 * 45), "-45:00");
        assert_eq!(&dur(false, 6), "-06");
    }
}
