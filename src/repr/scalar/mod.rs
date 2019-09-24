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

use self::decimal::Significand;
use self::regex::Regex;
use crate::ColumnType;

/// A literal value.
///
/// Note that datums may be scalar, like [`Datum::Int32`], or composite, like
/// [`Datum::Tuple`], but they are always constant.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub enum Datum {
    /// An unknown value.
    Null,
    /// The `true` boolean value.
    True,
    /// The `false` boolean value.
    False,
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
    Bytes(Vec<u8>),
    /// A sequence of Unicode codepoints encoded as UTF-8.
    String(String),
    /// A compiled regular expression.
    Regex(Box<Regex>),
}

impl Datum {
    pub fn is_null(&self) -> bool {
        match self {
            Datum::Null => true,
            _ => false,
        }
    }

    /// Create a Datum representing a Date
    ///
    /// Errors if the the combination of year/month/day is invalid
    pub fn from_ymd(year: i32, month: u8, day: u8) -> Result<Datum, failure::Error> {
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
    ) -> Result<Datum, failure::Error> {
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

    pub fn unwrap_interval_months(&self) -> i64 {
        match self {
            Datum::Interval(Interval::Months(count)) => *count,
            _ => panic!("Datum::unwrap_interval_months called on {:?}", self),
        }
    }

    /// Returns `(is_positive, duration)`
    pub fn unwrap_interval_duration(&self) -> (bool, std::time::Duration) {
        match self {
            Datum::Interval(Interval::Duration {
                is_positive,
                duration,
            }) => (*is_positive, *duration),
            _ => panic!("Datum::unwrap_interval_months called on {:?}", self),
        }
    }

    pub fn unwrap_decimal(&self) -> Significand {
        match self {
            Datum::Decimal(d) => *d,
            _ => panic!("Datum::unwrap_decimal called on {:?}", self),
        }
    }

    pub fn unwrap_str(&self) -> &str {
        match self {
            Datum::String(s) => s,
            _ => panic!("Datum::unwrap_string called on {:?}", self),
        }
    }

    pub fn unwrap_string(self) -> String {
        match self {
            Datum::String(s) => s,
            _ => panic!("Datum::unwrap_string called on {:?}", self),
        }
    }

    pub fn unwrap_regex(self) -> Regex {
        match self {
            Datum::Regex(r) => *r,
            _ => panic!("Datum::unwrap_regex calloed on {:?}", self),
        }
    }

    pub fn is_instance_of(&self, column_type: &ColumnType) -> bool {
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
            (Datum::Regex(_), ScalarType::Regex) => true,
            (Datum::Regex(_), _) => false,
        }
    }
}

impl From<bool> for Datum {
    fn from(b: bool) -> Datum {
        if b {
            Datum::True
        } else {
            Datum::False
        }
    }
}

impl From<i32> for Datum {
    fn from(i: i32) -> Datum {
        Datum::Int32(i)
    }
}

impl From<i64> for Datum {
    fn from(i: i64) -> Datum {
        Datum::Int64(i)
    }
}

impl From<OrderedFloat<f32>> for Datum {
    fn from(f: OrderedFloat<f32>) -> Datum {
        Datum::Float32(f)
    }
}

impl From<OrderedFloat<f64>> for Datum {
    fn from(f: OrderedFloat<f64>) -> Datum {
        Datum::Float64(f)
    }
}

impl From<f32> for Datum {
    fn from(f: f32) -> Datum {
        Datum::Float32(OrderedFloat(f))
    }
}

impl From<f64> for Datum {
    fn from(f: f64) -> Datum {
        Datum::Float64(OrderedFloat(f))
    }
}

impl From<i128> for Datum {
    fn from(d: i128) -> Datum {
        Datum::Decimal(Significand::new(d))
    }
}

impl From<Significand> for Datum {
    fn from(d: Significand) -> Datum {
        Datum::Decimal(d)
    }
}

impl From<chrono::Duration> for Datum {
    fn from(duration: chrono::Duration) -> Datum {
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

impl From<SqlInterval> for Datum {
    fn from(other: SqlInterval) -> Datum {
        Datum::Interval(other.into())
    }
}

impl From<String> for Datum {
    fn from(s: String) -> Datum {
        Datum::String(s)
    }
}

impl From<&str> for Datum {
    fn from(s: &str) -> Datum {
        Datum::String(s.to_owned())
    }
}

impl From<::regex::Regex> for Datum {
    fn from(r: ::regex::Regex) -> Datum {
        Datum::Regex(Box::new(Regex(r)))
    }
}

impl From<Vec<u8>> for Datum {
    fn from(b: Vec<u8>) -> Datum {
        Datum::Bytes(b)
    }
}

impl<T> From<Option<T>> for Datum
where
    Datum: From<T>,
{
    fn from(o: Option<T>) -> Datum {
        if let Some(d) = o {
            d.into()
        } else {
            Datum::Null
        }
    }
}

impl fmt::Display for Datum {
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
            Datum::Regex(b) => {
                let Regex(rex) = b.as_ref();
                write!(f, "/{}/", rex)
            }
        }
    }
}

impl<'a> From<&'a Datum> for Doc<'a, BoxDoc<'a, ()>> {
    fn from(datum: &'a Datum) -> Doc<'a, BoxDoc<'a, ()>> {
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
    Regex,
}

impl ScalarType {
    pub fn unwrap_decimal_parts(&self) -> (u8, u8) {
        match self {
            ScalarType::Decimal(p, s) => (*p, *s),
            _ => panic!("ScalarType::unwrap_decimal_parts called on {:?}", self),
        }
    }
}

impl fmt::Display for ScalarType {
    /// Arbitrary display name for scalars
    ///
    /// Right now the names correspond most closely to rust names (e.g. i32
    /// instead of int4), but we will want to make them more like some other
    /// system at some point.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ScalarType::*;
        match self {
            Null => f.write_str("null"),
            Bool => f.write_str("bool"),
            Int32 => f.write_str("i32"),
            Int64 => f.write_str("i64"),
            Float32 => f.write_str("f32"),
            Float64 => f.write_str("f64"),
            Decimal(scale, precision) => write!(f, "decimal({}, {})", scale, precision),
            Date => f.write_str("date"),
            Time => f.write_str("time"),
            Timestamp => f.write_str("timestamp"),
            Interval => f.write_str("interval"),
            Bytes => f.write_str("bytes"),
            String => f.write_str("string"),
            Regex => f.write_str("regex"),
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
