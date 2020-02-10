// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::str;

use bytes::{BufMut, BytesMut};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use postgres_types::{FromSql, IsNull, ToSql, Type as PgType};

use repr::decimal::MAX_DECIMAL_PRECISION;
use repr::jsonb::Jsonb;
use repr::{strconv, ColumnType, Datum, RelationType, Row, RowArena, ScalarType};

use crate::{Format, Interval, Numeric, Type};

pub mod interval;
pub mod numeric;

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
    /// A time.
    Time(NaiveTime),
    /// A date and time, without a timezone.
    Timestamp(NaiveDateTime),
    /// A date and time, with a timezone.
    TimestampTz(DateTime<Utc>),
    /// A time interval.
    Interval(Interval),
    /// A variable-length string.
    Text(String),
    /// An arbitrary precision number.
    Numeric(Numeric),
    /// A binary JSON blob.
    Jsonb(Jsonb),
}

impl Value {
    /// Constructs a new `Value` from a Materialize datum.
    ///
    /// The conversion happens in the obvious manner, except that `Datum::Null`
    /// is converted to `None` to align with how PostgreSQL handles NULL.
    pub fn from_datum(datum: Datum, typ: &ColumnType) -> Option<Value> {
        match (datum, &typ.scalar_type) {
            (Datum::Null, _) => None,
            (Datum::True, ScalarType::Bool) => Some(Value::Bool(true)),
            (Datum::False, ScalarType::Bool) => Some(Value::Bool(false)),
            (Datum::Int32(i), ScalarType::Int32) => Some(Value::Int4(i)),
            (Datum::Int64(i), ScalarType::Int64) => Some(Value::Int8(i)),
            (Datum::Float32(f), ScalarType::Float32) => Some(Value::Float4(*f)),
            (Datum::Float64(f), ScalarType::Float64) => Some(Value::Float8(*f)),
            (Datum::Date(d), ScalarType::Date) => Some(Value::Date(d)),
            (Datum::Time(t), ScalarType::Time) => Some(Value::Time(t)),
            (Datum::Timestamp(ts), ScalarType::Timestamp) => Some(Value::Timestamp(ts)),
            (Datum::TimestampTz(ts), ScalarType::TimestampTz) => Some(Value::TimestampTz(ts)),
            (Datum::Interval(iv), ScalarType::Interval) => Some(Value::Interval(Interval(iv))),
            (Datum::Decimal(d), ScalarType::Decimal(_, scale)) => {
                Some(Value::Numeric(Numeric(d.with_scale(*scale))))
            }
            (Datum::Bytes(b), ScalarType::Bytes) => Some(Value::Bytea(b.to_vec())),
            (Datum::String(s), ScalarType::String) => Some(Value::Text(s.to_owned())),
            (_, ScalarType::Jsonb) => Some(Value::Jsonb(Jsonb::from_datum(datum))),
            _ => panic!("can't serialize {}::{}", datum, typ),
        }
    }

    /// Converts a Materialize datum and type from this value.
    ///
    /// The conversion happens in the obvious manner, except that a
    /// `Value::Numeric`'s scale will be recorded in the returned scalar type,
    /// not the datum.
    ///
    /// To construct a null datum, see the [`null_datum`] function.
    pub fn into_datum<'a>(self, buf: &'a RowArena) -> (Datum<'a>, ScalarType) {
        match self {
            Value::Bool(true) => (Datum::True, ScalarType::Bool),
            Value::Bool(false) => (Datum::False, ScalarType::Bool),
            Value::Int4(i) => (Datum::Int32(i), ScalarType::Int32),
            Value::Int8(i) => (Datum::Int64(i), ScalarType::Int64),
            Value::Float4(f) => (Datum::Float32(f.into()), ScalarType::Float32),
            Value::Float8(f) => (Datum::Float64(f.into()), ScalarType::Float64),
            Value::Date(d) => (Datum::Date(d), ScalarType::Date),
            Value::Time(t) => (Datum::Time(t), ScalarType::Time),
            Value::Timestamp(ts) => (Datum::Timestamp(ts), ScalarType::Timestamp),
            Value::TimestampTz(ts) => (Datum::TimestampTz(ts), ScalarType::TimestampTz),
            Value::Interval(iv) => (Datum::Interval(iv.0), ScalarType::Interval),
            Value::Numeric(d) => (
                Datum::from(d.0.significand()),
                ScalarType::Decimal(MAX_DECIMAL_PRECISION, d.0.scale()),
            ),
            Value::Bytea(b) => (Datum::Bytes(buf.push_bytes(b)), ScalarType::Bytes),
            Value::Text(s) => (Datum::String(buf.push_string(s)), ScalarType::String),
            Value::Jsonb(jsonb) => (
                buf.push_row(jsonb.into_row()).unpack_first(),
                ScalarType::Jsonb,
            ),
        }
    }

    /// Serializes this value to `buf` in the specified `format`.
    pub fn encode(&self, format: Format, buf: &mut BytesMut) {
        match format {
            Format::Text => self.encode_text(buf),
            Format::Binary => self.encode_binary(buf),
        }
    }

    /// Serializes this value to `buf` using the [text encoding
    /// format](Format::Text).
    pub fn encode_text(&self, buf: &mut BytesMut) {
        match self {
            Value::Bool(b) => strconv::format_bool(buf, *b),
            Value::Bytea(b) => strconv::format_bytes(buf, b),
            Value::Date(d) => strconv::format_date(buf, *d),
            Value::Time(t) => strconv::format_time(buf, *t),
            Value::Timestamp(ts) => strconv::format_timestamp(buf, *ts),
            Value::TimestampTz(ts) => strconv::format_timestamptz(buf, *ts),
            Value::Interval(iv) => strconv::format_interval(buf, iv.0),
            Value::Int4(i) => strconv::format_int32(buf, *i),
            Value::Int8(i) => strconv::format_int64(buf, *i),
            Value::Float4(f) => strconv::format_float32(buf, *f),
            Value::Float8(f) => strconv::format_float64(buf, *f),
            Value::Numeric(n) => strconv::format_decimal(buf, &n.0),
            Value::Text(s) => buf.put(s.as_bytes()),
            Value::Jsonb(js) => strconv::format_jsonb(buf, js),
        }
    }

    /// Serializes this value to `buf` using the [binary encoding
    /// format](Format::Binary).
    pub fn encode_binary(&self, buf: &mut BytesMut) {
        let is_null = match self {
            Value::Bool(b) => b.to_sql(&PgType::BOOL, buf),
            Value::Bytea(b) => b.to_sql(&PgType::BYTEA, buf),
            Value::Date(d) => d.to_sql(&PgType::DATE, buf),
            Value::Time(t) => t.to_sql(&PgType::TIME, buf),
            Value::Timestamp(ts) => ts.to_sql(&PgType::TIMESTAMP, buf),
            Value::TimestampTz(ts) => ts.to_sql(&PgType::TIMESTAMPTZ, buf),
            Value::Interval(iv) => iv.to_sql(&PgType::INTERVAL, buf),
            Value::Int4(i) => i.to_sql(&PgType::INT4, buf),
            Value::Int8(i) => i.to_sql(&PgType::INT8, buf),
            Value::Float4(f) => f.to_sql(&PgType::FLOAT4, buf),
            Value::Float8(f) => f.to_sql(&PgType::FLOAT8, buf),
            Value::Numeric(n) => n.to_sql(&PgType::NUMERIC, buf),
            Value::Text(s) => s.to_sql(&PgType::TEXT, buf),
            Value::Jsonb(jsonb) => jsonb.as_serde_json().to_sql(&PgType::JSONB, buf),
        }
        .expect("encode_binary should never trigger a to_sql failure");
        match is_null {
            IsNull::Yes => panic!("encode_binary impossibly called on a null value"),
            IsNull::No => (),
        }
    }

    /// Deserializes a value of type `ty` from `raw` using the specified
    /// `format`.
    pub fn decode(
        format: Format,
        ty: Type,
        raw: &[u8],
    ) -> Result<Value, Box<dyn Error + Sync + Send>> {
        match format {
            Format::Text => Value::decode_text(ty, raw),
            Format::Binary => Value::decode_binary(ty, raw),
        }
    }

    /// Deserializes a value of type `ty` from `raw` using the [text encoding
    /// format](Format::Text).
    pub fn decode_text(ty: Type, raw: &[u8]) -> Result<Value, Box<dyn Error + Sync + Send>> {
        let raw = str::from_utf8(raw)?;
        Ok(match ty {
            Type::Bool => Value::Bool(strconv::parse_bool(raw)?),
            Type::Bytea => Value::Bytea(strconv::parse_bytes(raw)?),
            Type::Int4 => Value::Int4(strconv::parse_int32(raw)?),
            Type::Int8 => Value::Int8(strconv::parse_int64(raw)?),
            Type::Float4 => Value::Float4(strconv::parse_float32(raw)?),
            Type::Float8 => Value::Float8(strconv::parse_float64(raw)?),
            Type::Date => Value::Date(strconv::parse_date(raw)?),
            Type::Time => Value::Time(strconv::parse_time(raw)?),
            Type::Timestamp => Value::Timestamp(strconv::parse_timestamp(raw)?),
            Type::TimestampTz => Value::TimestampTz(strconv::parse_timestamptz(raw)?),
            Type::Interval => Value::Interval(Interval(strconv::parse_interval(raw)?)),
            Type::Text => Value::Text(raw.to_owned()),
            Type::Numeric => Value::Numeric(Numeric(strconv::parse_decimal(raw)?)),
            Type::Jsonb => Value::Jsonb(strconv::parse_jsonb(raw)?),
            Type::Unknown => panic!("cannot decode unknown type"),
        })
    }

    /// Deserializes a value of type `ty` from `raw` using the [binary encoding
    /// format](Format::Binary).
    pub fn decode_binary(ty: Type, raw: &[u8]) -> Result<Value, Box<dyn Error + Sync + Send>> {
        match ty {
            Type::Bool => bool::from_sql(ty.inner(), raw).map(Value::Bool),
            Type::Bytea => Vec::<u8>::from_sql(ty.inner(), raw).map(Value::Bytea),
            Type::Date => chrono::NaiveDate::from_sql(ty.inner(), raw).map(Value::Date),
            Type::Float4 => f32::from_sql(ty.inner(), raw).map(Value::Float4),
            Type::Float8 => f64::from_sql(ty.inner(), raw).map(Value::Float8),
            Type::Int4 => i32::from_sql(ty.inner(), raw).map(Value::Int4),
            Type::Int8 => i64::from_sql(ty.inner(), raw).map(Value::Int8),
            Type::Interval => Interval::from_sql(ty.inner(), raw).map(Value::Interval),
            Type::Jsonb => {
                let val = serde_json::Value::from_sql(ty.inner(), raw)?;
                Ok(Value::Jsonb(Jsonb::new(val)?))
            }
            Type::Numeric => Numeric::from_sql(ty.inner(), raw).map(Value::Numeric),
            Type::Text => String::from_sql(ty.inner(), raw).map(Value::Text),
            Type::Time => NaiveTime::from_sql(ty.inner(), raw).map(Value::Time),
            Type::Timestamp => NaiveDateTime::from_sql(ty.inner(), raw).map(Value::Timestamp),
            Type::TimestampTz => DateTime::<Utc>::from_sql(ty.inner(), raw).map(Value::TimestampTz),
            Type::Unknown => panic!("cannot decode unknown type"),
        }
    }
}

/// Constructs a null datum of the specified type.
pub fn null_datum(ty: Type) -> (Datum<'static>, ScalarType) {
    let ty = match ty {
        Type::Bool => ScalarType::Bool,
        Type::Bytea => ScalarType::Bytes,
        Type::Date => ScalarType::Date,
        Type::Float4 => ScalarType::Float32,
        Type::Float8 => ScalarType::Float64,
        Type::Int4 => ScalarType::Int32,
        Type::Int8 => ScalarType::Int64,
        Type::Interval => ScalarType::Interval,
        Type::Jsonb => ScalarType::Jsonb,
        Type::Numeric => ScalarType::Decimal(MAX_DECIMAL_PRECISION, 0),
        Type::Text => ScalarType::String,
        Type::Time => ScalarType::Time,
        Type::Timestamp => ScalarType::Timestamp,
        Type::TimestampTz => ScalarType::TimestampTz,
        Type::Unknown => ScalarType::Unknown,
    };
    (Datum::Null, ty)
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
