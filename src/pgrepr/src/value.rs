// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use bytes::{BufMut, BytesMut};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use postgres_types::{IsNull, ToSql, Type};

use repr::{ColumnType, Datum, RelationType, Row, ScalarType};

use crate::{Format, Interval, Numeric};

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
    Jsonb(String),
}

impl Value {
    /// Constructs a new `Value` from a Materialize datum.
    ///
    /// The conversion happens in the obvious manner, except that `Datum::Null`
    /// is converted to `None` to align with how PostgreSQL handles NULL.
    pub fn from_datum(datum: Datum, typ: &ColumnType) -> Option<Value> {
        if let ScalarType::Jsonb = &typ.scalar_type {
            Some(Value::Jsonb(expr::datum_to_serde(datum).to_string()))
        } else {
            match datum {
                Datum::Null => None,
                Datum::True => Some(Value::Bool(true)),
                Datum::False => Some(Value::Bool(false)),
                Datum::Int32(i) => Some(Value::Int4(i)),
                Datum::Int64(i) => Some(Value::Int8(i)),
                Datum::Float32(f) => Some(Value::Float4(*f)),
                Datum::Float64(f) => Some(Value::Float8(*f)),
                Datum::Date(d) => Some(Value::Date(d)),
                Datum::Timestamp(ts) => Some(Value::Timestamp(ts)),
                Datum::TimestampTz(ts) => Some(Value::TimestampTz(ts)),
                Datum::Interval(iv) => Some(Value::Interval(Interval(iv))),
                Datum::Decimal(d) => {
                    let (_, scale) = typ.scalar_type.unwrap_decimal_parts();
                    Some(Value::Numeric(Numeric(d.with_scale(scale))))
                }
                Datum::Bytes(b) => Some(Value::Bytea(b.to_vec())),
                Datum::String(s) => Some(Value::Text(s.to_owned())),
                Datum::JsonNull | Datum::List(_) | Datum::Dict(_) => {
                    panic!("can't serialize {}::{}", datum, typ)
                }
            }
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
            Value::Bool(false) => buf.put(&b"f"[..]),
            Value::Bool(true) => buf.put(&b"t"[..]),
            Value::Bytea(b) => buf.put(b.as_slice()),
            Value::Date(d) => buf.put(d.to_string().as_bytes()),
            Value::Timestamp(ts) => {
                buf.put(ts.format("%Y-%m-%d %H:%M:%S.%f").to_string().as_bytes())
            }
            Value::TimestampTz(ts) => {
                buf.put(ts.format("%Y-%m-%d %H:%M:%S.%f%:z").to_string().as_bytes())
            }
            Value::Interval(iv) => buf.put(iv.to_string().as_bytes()),
            Value::Int4(i) => buf.put(i.to_string().as_bytes()),
            Value::Int8(i) => buf.put(i.to_string().as_bytes()),
            Value::Float4(f) => buf.put(f.to_string().as_bytes()),
            Value::Float8(f) => buf.put(f.to_string().as_bytes()),
            Value::Numeric(n) => buf.put(n.to_string().as_bytes()),
            Value::Text(s) => buf.put(s.as_bytes()),
            Value::Jsonb(s) => buf.put(s.as_bytes()),
        }
    }

    /// Serializes this value to `buf` using the [binary encoding
    /// format](Format::Binary).
    pub fn encode_binary(&self, buf: &mut BytesMut) {
        let is_null = match self {
            Value::Bool(b) => b.to_sql(&Type::BOOL, buf),
            Value::Bytea(b) => b.to_sql(&Type::BYTEA, buf),
            Value::Date(d) => d.to_sql(&Type::DATE, buf),
            Value::Timestamp(ts) => ts.to_sql(&Type::TIMESTAMP, buf),
            Value::TimestampTz(ts) => ts.to_sql(&Type::TIMESTAMPTZ, buf),
            Value::Interval(iv) => iv.to_sql(&Type::INTERVAL, buf),
            Value::Int4(i) => i.to_sql(&Type::INT4, buf),
            Value::Int8(i) => i.to_sql(&Type::INT8, buf),
            Value::Float4(f) => f.to_sql(&Type::FLOAT4, buf),
            Value::Float8(f) => f.to_sql(&Type::FLOAT8, buf),
            Value::Numeric(n) => n.to_sql(&Type::NUMERIC, buf),
            Value::Text(s) => s.to_sql(&Type::TEXT, buf),
            Value::Jsonb(s) => {
                // https://github.com/postgres/postgres/blob/14aec03502302eff6c67981d8fd121175c436ce9/src/backend/utils/adt/jsonb.c#L148
                let version = 1;
                buf.put_u8(version);
                buf.put(s.as_bytes());
                Ok(postgres_types::IsNull::No)
            }
        }
        .expect("encode_binary should never trigger a to_sql failure");
        match is_null {
            IsNull::Yes => panic!("encode_binary impossibly called on a null value"),
            IsNull::No => (),
        }
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
