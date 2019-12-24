// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::borrow::Cow;

use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
use lazy_static::lazy_static;

use repr::decimal::Decimal;
use repr::{ColumnType, Datum, Interval, RelationType, Row, ScalarType};

lazy_static! {
    static ref EPOCH: NaiveDateTime = NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0);
    static ref EPOCH_NUM_DAYS_FROM_CE: i32 = EPOCH.num_days_from_ce();
}

/// A PostgreSQL datum.
#[derive(Debug)]
pub enum Value {
    /// A boolean value.
    Bool(bool),
    /// A byte array, i.e., a variable-length binary string.
    Bytea(Vec<u8>),
    /// A 4-byte signed integer.
    Int4(i32),
    /// An 8-byte signed integer.
    Int8(i64),
    /// A 4-byte floating point number.
    Float4(f32),
    /// An 8-byte floating point number.
    Float8(f64),
    /// A date.
    Date(NaiveDate),
    /// A date and time, without a timezone.
    Timestamp(NaiveDateTime),
    /// A date and time, with a timezone.
    TimestampTz(DateTime<Utc>),
    /// A time interval.
    Interval(Interval),
    /// A variable-length string.
    Text(String),
    /// An arbitrary precision number.
    Numeric(Decimal),
    /// A binary JSON blob.
    Jsonb(String),
}

impl Value {
    /// Constructs a new `Value` from a Materialize datum.
    ///
    /// The conversion happens in the obvious manner, except that `Datum::Null`
    /// is converted to `None` to align with how PostgreSQL handles NULL.
    pub fn from_datum(datum: Datum, typ: &ColumnType) -> Option<Value> {
        if let Datum::Null = datum {
            None
        } else if let ScalarType::Jsonb = &typ.scalar_type {
            let string = expr::datum_to_serde(datum).to_string();
            Some(Value::Jsonb(string))
        } else {
            Some(match datum {
                Datum::Null => unreachable!(),
                Datum::True => Value::Bool(true),
                Datum::False => Value::Bool(false),
                Datum::Int32(i) => Value::Int4(i),
                Datum::Int64(i) => Value::Int8(i),
                Datum::Float32(f) => Value::Float4(*f),
                Datum::Float64(f) => Value::Float8(*f),
                Datum::Date(d) => Value::Date(d),
                Datum::Timestamp(d) => Value::Timestamp(d),
                Datum::TimestampTz(d) => Value::TimestampTz(d),
                Datum::Interval(i) => Value::Interval(i),
                Datum::Decimal(d) => {
                    let (_, scale) = typ.scalar_type.unwrap_decimal_parts();
                    Value::Numeric(d.with_scale(scale))
                }
                Datum::Bytes(b) => Value::Bytea(b.to_vec()),
                Datum::String(s) => Value::Text(s.to_owned()),
                Datum::JsonNull | Datum::List(_) | Datum::Dict(_) => {
                    panic!("Don't know how to serialize {}::{}", datum, typ)
                }
            })
        }
    }

    /// Serializes this value using the [text encoding
    /// format](crate::Format::Text).
    pub fn to_text(&self) -> Cow<[u8]> {
        match self {
            Value::Bool(false) => b"f"[..].into(),
            Value::Bool(true) => b"t"[..].into(),
            Value::Bytea(b) => b.into(),
            Value::Date(d) => d.to_string().into_bytes().into(),
            Value::Timestamp(ts) => ts
                .format("%Y-%m-%d %H:%M:%S.%f")
                .to_string()
                .into_bytes()
                .into(),
            Value::TimestampTz(ts) => ts
                .format("%Y-%m-%d %H:%M:%S.%f%:z")
                .to_string()
                .into_bytes()
                .into(),
            Value::Interval(i) => match i {
                repr::Interval::Months(count) => format!("{} months", count).into_bytes().into(),
                repr::Interval::Duration {
                    is_positive,
                    duration,
                } => format!("{}{:?}", if *is_positive { "" } else { "-" }, duration)
                    .into_bytes()
                    .into(),
            },
            Value::Int4(i) => format!("{}", i).into_bytes().into(),
            Value::Int8(i) => format!("{}", i).into_bytes().into(),
            Value::Float4(f) => format!("{}", f).into_bytes().into(),
            Value::Float8(f) => format!("{}", f).into_bytes().into(),
            Value::Numeric(n) => format!("{}", n).into_bytes().into(),
            Value::Text(s) => s.as_bytes().into(),
            Value::Jsonb(s) => s.as_bytes().into(),
        }
    }

    /// Serializes this value using the [binary encoding
    /// format](crate::Format::Binary).
    pub fn to_binary(&self) -> Result<Cow<[u8]>, failure::Error> {
        Ok(match self {
            Value::Bool(false) => [0u8][..].into(),
            Value::Bool(true) => [1u8][..].into(),
            Value::Bytea(b) => b.into(),
            // https://github.com/postgres/postgres/blob/59354ccef5d7/src/backend/utils/adt/date.c#L223
            Value::Date(d) => {
                let day = d.num_days_from_ce() - *EPOCH_NUM_DAYS_FROM_CE;
                let mut buf = vec![0u8; 4];
                NetworkEndian::write_i32(&mut buf, day);
                buf.into()
            }
            Value::Timestamp(ts) => {
                let timestamp = (ts.timestamp() - EPOCH.timestamp()) * 1_000_000
                    + i64::from(ts.timestamp_subsec_micros());
                let mut buf = vec![0u8; 8];
                NetworkEndian::write_i64(&mut buf, timestamp);
                buf.into()
            }
            Value::TimestampTz(ts) => {
                let timestamp = (ts.timestamp() - EPOCH.timestamp()) * 1_000_000
                    + i64::from(ts.timestamp_subsec_micros());
                let mut buf = vec![0u8; 8];
                NetworkEndian::write_i64(&mut buf, timestamp);
                buf.into()
            }
            // https://github.com/postgres/postgres/blob/517bf2d9107f0d45c5fea2e3904e8d3b10ce6bb2/src/backend/utils/adt/timestamp.c#L1008
            // Postgres stores interval objects as a 16 byte memory blob split into 3 parts: 64 bits representing the interval in microseconds,
            // then 32 bits describing the interval in days, then 32 bits representing the interval in months
            // See also: https://github.com/diesel-rs/diesel/blob/a8b52bd05be202807e71579acf841735b6f1765e/diesel/src/pg/types/date_and_time/mod.rs#L39
            // for the Diesel implementation of the same logic
            Value::Interval(i) => {
                let mut buf = Vec::with_capacity(16);
                match i {
                    repr::Interval::Months(n) => {
                        buf.write_i64::<NetworkEndian>(0)?;
                        buf.write_i32::<NetworkEndian>(0)?;
                        buf.write_i32::<NetworkEndian>(*n as i32)?;
                    }
                    repr::Interval::Duration { duration, .. } => {
                        buf.write_i64::<NetworkEndian>(duration.as_micros() as i64)?;
                        buf.write_i32::<NetworkEndian>(0)?;
                        buf.write_i32::<NetworkEndian>(0)?;
                    }
                }
                buf.into()
            }
            Value::Int4(i) => {
                let mut buf = vec![0u8; 4];
                NetworkEndian::write_i32(&mut buf, *i);
                buf.into()
            }
            Value::Int8(i) => {
                let mut buf = vec![0u8; 8];
                NetworkEndian::write_i64(&mut buf, *i);
                buf.into()
            }
            Value::Float4(f) => {
                let mut buf = vec![0u8; 4];
                NetworkEndian::write_f32(&mut buf, *f);
                buf.into()
            }
            Value::Float8(f) => {
                let mut buf = vec![0u8; 8];
                NetworkEndian::write_f64(&mut buf, *f);
                buf.into()
            }
            // https://github.com/postgres/postgres/blob/59354ccef5/src/backend/utils/adt/numeric.c#L868-L891
            Value::Numeric(n) => {
                // This implementation is derived from Diesel.
                // https://github.com/diesel-rs/diesel/blob/bd13f24609c6893166aab2aaf92020bb5899f402/diesel/src/pg/types/numeric.rs
                let mut significand = n.significand();
                let scale = u16::from(n.scale());
                let non_neg = significand >= 0;
                significand = significand.abs();

                // Ensure that the significand will always lie on a digit boundary
                for _ in 0..(4 - scale % 4) {
                    significand *= 10;
                }

                let mut digits = vec![];
                while significand > 0 {
                    digits.push((significand % 10_000) as i16);
                    significand /= 10_000;
                }
                digits.reverse();
                let digits_after_decimal = scale / 4 + 1;
                let weight = digits.len() as i16 - digits_after_decimal as i16 - 1;

                let unnecessary_zeroes = if weight >= 0 {
                    let index_of_decimal = (weight + 1) as usize;
                    digits
                        .get(index_of_decimal..)
                        .expect("enough digits exist")
                        .iter()
                        .rev()
                        .take_while(|i| **i == 0)
                        .count()
                } else {
                    0
                };

                let relevant_digits = digits.len() - unnecessary_zeroes;
                digits.truncate(relevant_digits);

                let sign = if non_neg { 0 } else { 0x4000 };

                let mut buf = Vec::with_capacity(8 + 2 * digits.len());
                buf.write_u16::<NetworkEndian>(digits.len() as u16)?;
                buf.write_i16::<NetworkEndian>(weight)?;
                buf.write_u16::<NetworkEndian>(sign)?;
                buf.write_u16::<NetworkEndian>(scale)?;
                for digit in digits.iter() {
                    buf.write_i16::<NetworkEndian>(*digit)?;
                }
                buf.into()
            }
            Value::Text(ref s) => s.as_bytes().into(),
            Value::Jsonb(s) => {
                // https://github.com/postgres/postgres/blob/14aec03502302eff6c67981d8fd121175c436ce9/src/backend/utils/adt/jsonb.c#L148
                let version = 1;
                let mut buf: Vec<u8> = vec![version];
                buf.extend_from_slice(s.as_bytes());
                buf.into()
            }
        })
    }
}

/// Converts a Materialize row into a vector of PostgreSQL values.
///
/// Calling this function is equivalent to mapping [`Value::from_datum`] over
/// every datum in `row`.
pub fn values_from_row(row: Row, typ: &RelationType) -> Vec<Option<Value>> {
    row.iter()
        .zip(typ.column_types.iter())
        .map(|(col, typ)| Value::from_datum(col, typ))
        .collect()
}
