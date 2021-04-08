// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::io;
use std::str;

use bytes::{BufMut, BytesMut};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use postgres_types::{FromSql, IsNull, ToSql, Type as PgType};
use repr::ColumnType;
use uuid::Uuid;

use ore::fmt::FormatBuffer;
use repr::adt::array::ArrayDimension;
use repr::adt::decimal::MAX_DECIMAL_PRECISION;
use repr::adt::jsonb::JsonbRef;
use repr::adt::rdn as adt_rdn;
use repr::strconv::{self, Nestable};
use repr::{ColumnName, Datum, RelationType, Row, RowArena, ScalarType};

use crate::{types, Format, Interval, Jsonb, Numeric, RDNValue, Type};

pub mod interval;
pub mod jsonb;
pub mod numeric;
pub mod rdn;
pub mod record;

/// A PostgreSQL datum.
#[derive(Debug)]
pub enum Value {
    /// A variable-length, multi-dimensional array of values.
    Array {
        /// The dimensions of the array.
        dims: Vec<ArrayDimension>,
        /// The elements of the array.
        elements: Vec<Option<Value>>,
    },
    /// A boolean value.
    Bool(bool),
    /// A byte array, i.e., a variable-length binary string.
    Bytea(Vec<u8>),
    /// A date.
    Date(NaiveDate),
    /// A 4-byte floating point number.
    Float4(f32),
    /// An 8-byte floating point number.
    Float8(f64),
    /// A 4-byte signed integer.
    Int4(i32),
    /// An 8-byte signed integer.
    Int8(i64),
    /// A time interval.
    Interval(Interval),
    /// A binary JSON blob.
    Jsonb(Jsonb),
    /// A sequence of homogeneous values.
    List(Vec<Option<Value>>),
    /// A map of string keys and homogeneous values.
    Map(BTreeMap<String, Option<Value>>),
    /// An arbitrary precision number.
    Numeric(Numeric),
    /// A sequence of heterogeneous values.
    Record(Vec<Option<Value>>),
    /// A time.
    Time(NaiveTime),
    /// A date and time, without a timezone.
    Timestamp(NaiveDateTime),
    /// A date and time, with a timezone.
    TimestampTz(DateTime<Utc>),
    /// A variable-length string.
    Text(String),
    /// A universally unique identifier.
    Uuid(Uuid),
    /// Refactored numeric using `rust-dec`.
    RDN(RDNValue),
}

impl Value {
    /// Constructs a new `Value` from a Materialize datum.
    ///
    /// The conversion happens in the obvious manner, except that `Datum::Null`
    /// is converted to `None` to align with how PostgreSQL handles NULL.
    pub fn from_datum(datum: Datum, typ: &ScalarType) -> Option<Value> {
        match (datum, typ) {
            (Datum::Null, _) => None,
            (Datum::True, ScalarType::Bool) => Some(Value::Bool(true)),
            (Datum::False, ScalarType::Bool) => Some(Value::Bool(false)),
            (Datum::Int32(i), ScalarType::Int32) => Some(Value::Int4(i)),
            (Datum::Int32(i), ScalarType::Oid) => Some(Value::Int4(i)),
            (Datum::Int64(i), ScalarType::Int64) => Some(Value::Int8(i)),
            (Datum::Float32(f), ScalarType::Float32) => Some(Value::Float4(*f)),
            (Datum::Float64(f), ScalarType::Float64) => Some(Value::Float8(*f)),
            (Datum::Decimal(d), ScalarType::Decimal(_, scale)) => {
                Some(Value::Numeric(Numeric(d.with_scale(*scale))))
            }
            (Datum::Numeric(mut n), ScalarType::Numeric { scale }) => {
                if let Some(scale) = scale {
                    adt_rdn::rescale(&mut n, *scale).unwrap();
                }
                Some(Value::RDN(RDNValue(n)))
            }
            (Datum::Date(d), ScalarType::Date) => Some(Value::Date(d)),
            (Datum::Time(t), ScalarType::Time) => Some(Value::Time(t)),
            (Datum::Timestamp(ts), ScalarType::Timestamp) => Some(Value::Timestamp(ts)),
            (Datum::TimestampTz(ts), ScalarType::TimestampTz) => Some(Value::TimestampTz(ts)),
            (Datum::Interval(iv), ScalarType::Interval) => Some(Value::Interval(Interval(iv))),
            (Datum::Bytes(b), ScalarType::Bytes) => Some(Value::Bytea(b.to_vec())),
            (Datum::String(s), ScalarType::String) => Some(Value::Text(s.to_owned())),
            (_, ScalarType::Jsonb) => {
                Some(Value::Jsonb(Jsonb(JsonbRef::from_datum(datum).to_owned())))
            }
            (Datum::Uuid(u), ScalarType::Uuid) => Some(Value::Uuid(u)),
            (Datum::Array(array), ScalarType::Array(elem_type)) => {
                let dims = array.dims().into_iter().collect();
                let elements = array
                    .elements()
                    .iter()
                    .map(|elem| Value::from_datum(elem, elem_type))
                    .collect();
                Some(Value::Array { dims, elements })
            }
            (Datum::List(list), ScalarType::List { element_type, .. }) => {
                let elements = list
                    .iter()
                    .map(|elem| Value::from_datum(elem, element_type))
                    .collect();
                Some(Value::List(elements))
            }
            (Datum::List(record), ScalarType::Record { fields, .. }) => {
                let fields = record
                    .iter()
                    .zip(fields)
                    .map(|(e, (_name, ty))| Value::from_datum(e, &ty.scalar_type))
                    .collect();
                Some(Value::Record(fields))
            }
            (Datum::Map(dict), ScalarType::Map { value_type, .. }) => {
                let entries = dict
                    .iter()
                    .map(|(k, v)| (k.to_owned(), Value::from_datum(v, value_type)))
                    .collect();
                Some(Value::Map(entries))
            }
            _ => panic!("can't serialize {}::{:?}", datum, typ),
        }
    }

    /// Converts a Materialize datum and type from this value.
    ///
    /// The conversion happens in the obvious manner, except that a
    /// `Value::Numeric`'s scale will be recorded in the returned scalar type,
    /// not the datum.
    ///
    /// To construct a null datum, see the [`null_datum`] function.
    pub fn into_datum<'a>(self, buf: &'a RowArena, typ: &Type) -> (Datum<'a>, ScalarType) {
        match self {
            Value::Array { .. } => {
                // This situation is handled gracefully by Value::decode; if we
                // wind up here it's a programming error.
                unreachable!("into_datum cannot be called on Value::Array");
            }
            Value::Bool(true) => (Datum::True, ScalarType::Bool),
            Value::Bool(false) => (Datum::False, ScalarType::Bool),
            Value::Bytea(b) => (Datum::Bytes(buf.push_bytes(b)), ScalarType::Bytes),
            Value::Date(d) => (Datum::Date(d), ScalarType::Date),
            Value::Float4(f) => (Datum::Float32(f.into()), ScalarType::Float32),
            Value::Float8(f) => (Datum::Float64(f.into()), ScalarType::Float64),
            Value::Int4(i) => match typ {
                Type::Oid => (Datum::Int32(i), ScalarType::Int32),
                Type::Int4 => (Datum::Int32(i), ScalarType::Int32),
                _ => unreachable!(),
            },
            Value::Int8(i) => (Datum::Int64(i), ScalarType::Int64),
            Value::Jsonb(js) => (buf.push_unary_row(js.0.into_row()), ScalarType::Jsonb),
            Value::List(elems) => {
                let elem_pg_type = match typ {
                    Type::List(t) => &*t,
                    _ => panic!("Value::List should have type Type::List. Found {:?}", typ),
                };
                let (_, elem_type) = null_datum(&elem_pg_type);
                let mut row = Row::default();
                row.push_list(elems.into_iter().map(|elem| match elem {
                    Some(elem) => elem.into_datum(buf, &elem_pg_type).0,
                    None => Datum::Null,
                }));
                (
                    buf.push_unary_row(row),
                    ScalarType::List {
                        element_type: Box::new(elem_type),
                        custom_oid: None,
                    },
                )
            }
            Value::Map(map) => {
                let elem_pg_type = match typ {
                    Type::Map { value_type } => &*value_type,
                    _ => panic!("Value::Map should have type Type::Map. Found {:?}", typ),
                };
                let (_, elem_type) = null_datum(&elem_pg_type);
                let mut row = Row::default();
                row.push_dict_with(|row| {
                    for (k, v) in map {
                        row.push(Datum::String(&k));
                        row.push(match v {
                            Some(elem) => elem.into_datum(buf, &elem_pg_type).0,
                            None => Datum::Null,
                        });
                    }
                });
                (
                    buf.push_unary_row(row),
                    ScalarType::Map {
                        value_type: Box::new(elem_type),
                        custom_oid: None,
                    },
                )
            }
            Value::Numeric(d) => (
                Datum::from(d.0.significand()),
                ScalarType::Decimal(MAX_DECIMAL_PRECISION, d.0.scale()),
            ),
            Value::Record(_) => {
                // This situation is handled gracefully by Value::decode; if we
                // wind up here it's a programming error.
                unreachable!("into_datum cannot be called on Value::Record");
            }
            Value::Time(t) => (Datum::Time(t), ScalarType::Time),
            Value::Timestamp(ts) => (Datum::Timestamp(ts), ScalarType::Timestamp),
            Value::TimestampTz(ts) => (Datum::TimestampTz(ts), ScalarType::TimestampTz),
            Value::Interval(iv) => (Datum::Interval(iv.0), ScalarType::Interval),
            Value::Text(s) => (Datum::String(buf.push_string(s)), ScalarType::String),
            Value::Uuid(u) => (Datum::Uuid(u), ScalarType::Uuid),
            Value::RDN(n) => (
                Datum::from(n.0),
                ScalarType::Numeric {
                    scale: Some(u8::try_from(adt_rdn::get_scale(&n.0)).unwrap()),
                },
            ),
        }
    }

    /// Serializes this value to `buf` in the specified `format`.
    pub fn encode(&self, ty: &Type, format: Format, buf: &mut BytesMut) -> Result<(), io::Error> {
        match format {
            Format::Text => {
                self.encode_text(buf);
                Ok(())
            }
            Format::Binary => self.encode_binary(ty, buf),
        }
    }

    /// Serializes this value to `buf` using the [text encoding
    /// format](Format::Text).
    pub fn encode_text<F>(&self, buf: &mut F) -> Nestable
    where
        F: FormatBuffer,
    {
        match self {
            Value::Array { dims, elements } => {
                strconv::format_array(buf, dims, elements, |buf, elem| match elem {
                    None => buf.write_null(),
                    Some(elem) => elem.encode_text(buf.nonnull_buffer()),
                })
            }
            Value::Bool(b) => strconv::format_bool(buf, *b),
            Value::Bytea(b) => strconv::format_bytes(buf, b),
            Value::Date(d) => strconv::format_date(buf, *d),
            Value::Int4(i) => strconv::format_int32(buf, *i),
            Value::Int8(i) => strconv::format_int64(buf, *i),
            Value::Interval(iv) => strconv::format_interval(buf, iv.0),
            Value::Float4(f) => strconv::format_float32(buf, *f),
            Value::Float8(f) => strconv::format_float64(buf, *f),
            Value::Jsonb(js) => strconv::format_jsonb(buf, js.0.as_ref()),
            Value::List(elems) => strconv::format_list(buf, elems, |buf, elem| match elem {
                None => buf.write_null(),
                Some(elem) => elem.encode_text(buf.nonnull_buffer()),
            }),
            Value::Map(elems) => strconv::format_map(buf, elems, |buf, value| match value {
                None => buf.write_null(),
                Some(elem) => elem.encode_text(buf.nonnull_buffer()),
            }),
            Value::Numeric(n) => strconv::format_decimal(buf, &n.0),
            Value::Record(elems) => strconv::format_record(buf, elems, |buf, elem| match elem {
                None => buf.write_null(),
                Some(elem) => elem.encode_text(buf.nonnull_buffer()),
            }),
            Value::Text(s) => strconv::format_string(buf, s),
            Value::Time(t) => strconv::format_time(buf, *t),
            Value::Timestamp(ts) => strconv::format_timestamp(buf, *ts),
            Value::TimestampTz(ts) => strconv::format_timestamptz(buf, *ts),
            Value::Uuid(u) => strconv::format_uuid(buf, *u),
            Value::RDN(n) => strconv::format_numeric(buf, &n.0),
        }
    }

    /// Serializes this value to `buf` using the [binary encoding
    /// format](Format::Binary).
    pub fn encode_binary(&self, ty: &Type, buf: &mut BytesMut) -> Result<(), io::Error> {
        let is_null = match self {
            Value::Array { dims, elements } => {
                let ndims = pg_len("number of array dimensions", dims.len())?;
                let has_null = elements.iter().any(|e| e.is_none());
                let elem_type = match ty {
                    Type::Array(elem_type) => elem_type,
                    _ => unreachable!(),
                };
                buf.put_i32(ndims);
                buf.put_i32(has_null.into());
                buf.put_u32(elem_type.oid());
                for dim in dims {
                    buf.put_i32(pg_len("array dimension length", dim.length)?);
                    buf.put_i32(pg_len("array dimension lower bound", dim.lower_bound)?);
                }
                for elem in elements {
                    encode_element(buf, elem.as_ref(), elem_type)?;
                }
                Ok(postgres_types::IsNull::No)
            }
            Value::Bool(b) => b.to_sql(&PgType::BOOL, buf),
            Value::Bytea(b) => b.to_sql(&PgType::BYTEA, buf),
            Value::Date(d) => d.to_sql(&PgType::DATE, buf),
            Value::Float4(f) => f.to_sql(&PgType::FLOAT4, buf),
            Value::Float8(f) => f.to_sql(&PgType::FLOAT8, buf),
            Value::Int4(i) => i.to_sql(&PgType::INT4, buf),
            Value::Int8(i) => i.to_sql(&PgType::INT8, buf),
            Value::Interval(iv) => iv.to_sql(&PgType::INTERVAL, buf),
            Value::Jsonb(js) => js.to_sql(&PgType::JSONB, buf),
            Value::List(_) => {
                // for now just use text encoding
                self.encode_text(buf);
                Ok(postgres_types::IsNull::No)
            }
            Value::Map(_) => {
                // for now just use text encoding
                self.encode_text(buf);
                Ok(postgres_types::IsNull::No)
            }
            Value::Numeric(n) => n.to_sql(&PgType::NUMERIC, buf),
            Value::Record(fields) => {
                let nfields = pg_len("record field length", fields.len())?;
                buf.put_i32(nfields);
                let field_types = match ty {
                    Type::Record(fields) => fields,
                    _ => unreachable!(),
                };
                for (f, ty) in fields.iter().zip(field_types) {
                    buf.put_u32(ty.oid());
                    encode_element(buf, f.as_ref(), ty)?;
                }
                Ok(postgres_types::IsNull::No)
            }
            Value::Text(s) => s.to_sql(&PgType::TEXT, buf),
            Value::Time(t) => t.to_sql(&PgType::TIME, buf),
            Value::Timestamp(ts) => ts.to_sql(&PgType::TIMESTAMP, buf),
            Value::TimestampTz(ts) => ts.to_sql(&PgType::TIMESTAMPTZ, buf),
            Value::Uuid(u) => u.to_sql(&PgType::UUID, buf),
            Value::RDN(n) => n.to_sql(&types::RDN, buf),
        }
        .expect("encode_binary should never trigger a to_sql failure");
        if let IsNull::Yes = is_null {
            panic!("encode_binary impossibly called on a null value")
        }
        Ok(())
    }

    /// Deserializes a value of type `ty` from `raw` using the specified
    /// `format`.
    pub fn decode(
        format: Format,
        ty: &Type,
        raw: &[u8],
    ) -> Result<Value, Box<dyn Error + Sync + Send>> {
        match format {
            Format::Text => Value::decode_text(ty, raw),
            Format::Binary => Value::decode_binary(ty, raw),
        }
    }

    /// Deserializes a value of type `ty` from `raw` using the [text encoding
    /// format](Format::Text).
    pub fn decode_text(ty: &Type, raw: &[u8]) -> Result<Value, Box<dyn Error + Sync + Send>> {
        let raw = str::from_utf8(raw)?;
        Ok(match ty {
            Type::Array(_) => return Err("input of array types is not implemented".into()),
            Type::Bool => Value::Bool(strconv::parse_bool(raw)?),
            Type::Bytea => Value::Bytea(strconv::parse_bytes(raw)?),
            Type::Date => Value::Date(strconv::parse_date(raw)?),
            Type::Float4 => Value::Float4(strconv::parse_float32(raw)?),
            Type::Float8 => Value::Float8(strconv::parse_float64(raw)?),
            Type::Int4 | Type::Oid => Value::Int4(strconv::parse_int32(raw)?),
            Type::Int8 => Value::Int8(strconv::parse_int64(raw)?),
            Type::Interval => Value::Interval(Interval(strconv::parse_interval(raw)?)),
            Type::Jsonb => Value::Jsonb(Jsonb(strconv::parse_jsonb(raw)?)),
            Type::List(elem_type) => Value::List(strconv::parse_list(
                raw,
                matches!(**elem_type, Type::List(..)),
                || None,
                |elem_text| Value::decode_text(elem_type, elem_text.as_bytes()).map(Some),
            )?),
            Type::Map { value_type } => Value::Map(strconv::parse_map(
                raw,
                matches!(**value_type, Type::Map { .. }),
                |elem_text| Value::decode_text(value_type, elem_text.as_bytes()).map(Some),
            )?),
            Type::Numeric => Value::Numeric(Numeric(strconv::parse_decimal(raw)?)),
            Type::Record(_) => {
                return Err("input of anonymous composite types is not implemented".into())
            }
            Type::Text => Value::Text(raw.to_owned()),
            Type::Time => Value::Time(strconv::parse_time(raw)?),
            Type::Timestamp => Value::Timestamp(strconv::parse_timestamp(raw)?),
            Type::TimestampTz => Value::TimestampTz(strconv::parse_timestamptz(raw)?),
            Type::Uuid => Value::Uuid(Uuid::parse_str(raw)?),
            Type::RDN => {
                let d = strconv::parse_numeric(raw)?;
                Value::RDN(RDNValue(d))
            }
        })
    }

    /// Deserializes a value of type `ty` from `raw` using the [binary encoding
    /// format](Format::Binary).
    pub fn decode_binary(ty: &Type, raw: &[u8]) -> Result<Value, Box<dyn Error + Sync + Send>> {
        match ty {
            Type::Array(_) => Err("input of array types is not implemented".into()),
            Type::Bool => bool::from_sql(ty.inner(), raw).map(Value::Bool),
            Type::Bytea => Vec::<u8>::from_sql(ty.inner(), raw).map(Value::Bytea),
            Type::Date => chrono::NaiveDate::from_sql(ty.inner(), raw).map(Value::Date),
            Type::Float4 => f32::from_sql(ty.inner(), raw).map(Value::Float4),
            Type::Float8 => f64::from_sql(ty.inner(), raw).map(Value::Float8),
            Type::Int4 | Type::Oid => i32::from_sql(ty.inner(), raw).map(Value::Int4),
            Type::Int8 => i64::from_sql(ty.inner(), raw).map(Value::Int8),
            Type::Interval => Interval::from_sql(ty.inner(), raw).map(Value::Interval),
            Type::Jsonb => Jsonb::from_sql(ty.inner(), raw).map(Value::Jsonb),
            Type::List(_) => Value::decode_text(ty, raw), // just using the text encoding for now
            Type::Map { .. } => Value::decode_text(ty, raw), // just using the text encoding for now
            Type::Numeric => Numeric::from_sql(ty.inner(), raw).map(Value::Numeric),
            Type::Record(_) => Err("input of anonymous composite types is not implemented".into()),
            Type::Text => String::from_sql(ty.inner(), raw).map(Value::Text),
            Type::Time => NaiveTime::from_sql(ty.inner(), raw).map(Value::Time),
            Type::Timestamp => NaiveDateTime::from_sql(ty.inner(), raw).map(Value::Timestamp),
            Type::TimestampTz => DateTime::<Utc>::from_sql(ty.inner(), raw).map(Value::TimestampTz),
            Type::Uuid => Uuid::from_sql(ty.inner(), raw).map(Value::Uuid),
            Type::RDN => RDNValue::from_sql(ty.inner(), raw).map(Value::RDN),
        }
    }
}

fn encode_element(buf: &mut BytesMut, elem: Option<&Value>, ty: &Type) -> Result<(), io::Error> {
    match elem {
        None => buf.put_i32(-1),
        Some(elem) => {
            let base = buf.len();
            buf.put_i32(0);
            elem.encode_binary(ty, buf)?;
            let len = pg_len("encoded element", buf.len() - base - 4)?;
            buf[base..base + 4].copy_from_slice(&len.to_be_bytes());
        }
    }
    Ok(())
}

fn pg_len(what: &str, len: usize) -> Result<i32, io::Error> {
    len.try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("{} does not fit into an i32", what),
        )
    })
}

/// Constructs a null datum of the specified type.
pub fn null_datum(ty: &Type) -> (Datum<'static>, ScalarType) {
    let ty = match ty {
        Type::Array(t) => {
            let (_, elem_type) = null_datum(t);
            ScalarType::Array(Box::new(elem_type))
        }
        Type::Bool => ScalarType::Bool,
        Type::Bytea => ScalarType::Bytes,
        Type::Date => ScalarType::Date,
        Type::Float4 => ScalarType::Float32,
        Type::Float8 => ScalarType::Float64,
        Type::Int4 => ScalarType::Int32,
        Type::Int8 => ScalarType::Int64,
        Type::Interval => ScalarType::Interval,
        Type::Jsonb => ScalarType::Jsonb,
        Type::List(t) => {
            let (_, elem_type) = null_datum(t);
            ScalarType::List {
                element_type: Box::new(elem_type),
                custom_oid: None,
            }
        }
        Type::Numeric => ScalarType::Decimal(MAX_DECIMAL_PRECISION, 0),
        Type::RDN => ScalarType::Numeric { scale: None },
        Type::Oid => ScalarType::Oid,
        Type::Text => ScalarType::String,
        Type::Time => ScalarType::Time,
        Type::Timestamp => ScalarType::Timestamp,
        Type::TimestampTz => ScalarType::TimestampTz,
        Type::Uuid => ScalarType::Uuid,
        Type::Record(fields) => {
            let fields = fields
                .iter()
                .enumerate()
                .map(|(i, ty)| {
                    let name = ColumnName::from(format!("f{}", i + 1));
                    (
                        name,
                        ColumnType {
                            nullable: true,
                            scalar_type: null_datum(ty).1,
                        },
                    )
                })
                .collect();
            ScalarType::Record {
                fields,
                custom_oid: None,
                custom_name: None,
            }
        }
        Type::Map { value_type } => {
            let (_, value_type) = null_datum(value_type);
            ScalarType::Map {
                value_type: Box::new(value_type),
                custom_oid: None,
            }
        }
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
        .map(|(col, typ)| Value::from_datum(col, &typ.scalar_type))
        .collect()
}
