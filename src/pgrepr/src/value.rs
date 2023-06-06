// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::error::Error;
use std::{io, str};

use bytes::{BufMut, BytesMut};
use chrono::{DateTime, NaiveDateTime, NaiveTime, Utc};
use mz_ore::cast::ReinterpretCast;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::char;
use mz_repr::adt::date::Date;
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::mz_acl_item::MzAclItem;
use mz_repr::adt::range::{Range, RangeInner};
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::strconv::{self, Nestable};
use mz_repr::{Datum, RelationType, Row, RowArena, ScalarType};
use postgres_types::{FromSql, IsNull, ToSql, Type as PgType};
use uuid::Uuid;

use crate::types::{UINT2, UINT4, UINT8};
use crate::{Format, Interval, Jsonb, Numeric, Type, UInt2, UInt4, UInt8};

pub mod interval;
pub mod jsonb;
pub mod numeric;
pub mod record;
pub mod unsigned;

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
    /// A single-byte character.
    Char(u8),
    /// A date.
    Date(Date),
    /// A 4-byte floating point number.
    Float4(f32),
    /// An 8-byte floating point number.
    Float8(f64),
    /// A 2-byte signed integer.
    Int2(i16),
    /// A 4-byte signed integer.
    Int4(i32),
    /// An 8-byte signed integer.
    Int8(i64),
    /// A 2-byte unsigned integer.
    UInt2(UInt2),
    /// A 4-byte unsigned integer.
    UInt4(UInt4),
    /// An 8-byte unsigned integer.
    UInt8(UInt8),
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
    /// An object identifier.
    Oid(u32),
    /// A sequence of heterogeneous values.
    Record(Vec<Option<Value>>),
    /// A time.
    Time(NaiveTime),
    /// A date and time, without a timezone.
    Timestamp(CheckedTimestamp<NaiveDateTime>),
    /// A date and time, with a timezone.
    TimestampTz(CheckedTimestamp<DateTime<Utc>>),
    /// A variable-length string.
    Text(String),
    /// A fixed-length string.
    BpChar(String),
    /// A variable-length string with an optional limit.
    VarChar(String),
    /// A universally unique identifier.
    Uuid(Uuid),
    /// A small int vector.
    Int2Vector {
        /// The elements of the vector.
        elements: Vec<Option<Value>>,
    },
    /// A Materialize timestamp.
    MzTimestamp(mz_repr::Timestamp),
    /// A contiguous range of values along a domain.
    Range(Range<Box<Value>>),
    /// A list of privileges granted to a role.
    MzAclItem(MzAclItem),
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
            (Datum::Int16(i), ScalarType::Int16) => Some(Value::Int2(i)),
            (Datum::Int32(i), ScalarType::Int32) => Some(Value::Int4(i)),
            (Datum::Int64(i), ScalarType::Int64) => Some(Value::Int8(i)),
            (Datum::UInt8(c), ScalarType::PgLegacyChar) => Some(Value::Char(c)),
            (Datum::UInt16(u), ScalarType::UInt16) => Some(Value::UInt2(UInt2(u))),
            (Datum::UInt32(oid), ScalarType::Oid) => Some(Value::Oid(oid)),
            (Datum::UInt32(oid), ScalarType::RegClass) => Some(Value::Oid(oid)),
            (Datum::UInt32(oid), ScalarType::RegProc) => Some(Value::Oid(oid)),
            (Datum::UInt32(oid), ScalarType::RegType) => Some(Value::Oid(oid)),
            (Datum::UInt32(u), ScalarType::UInt32) => Some(Value::UInt4(UInt4(u))),
            (Datum::UInt64(u), ScalarType::UInt64) => Some(Value::UInt8(UInt8(u))),
            (Datum::Float32(f), ScalarType::Float32) => Some(Value::Float4(*f)),
            (Datum::Float64(f), ScalarType::Float64) => Some(Value::Float8(*f)),
            (Datum::Numeric(d), ScalarType::Numeric { .. }) => Some(Value::Numeric(Numeric(d))),
            (Datum::MzTimestamp(t), ScalarType::MzTimestamp) => Some(Value::MzTimestamp(t)),
            (Datum::MzAclItem(mai), ScalarType::MzAclItem) => Some(Value::MzAclItem(mai)),
            (Datum::Date(d), ScalarType::Date) => Some(Value::Date(d)),
            (Datum::Time(t), ScalarType::Time) => Some(Value::Time(t)),
            (Datum::Timestamp(ts), ScalarType::Timestamp) => Some(Value::Timestamp(ts)),
            (Datum::TimestampTz(ts), ScalarType::TimestampTz) => Some(Value::TimestampTz(ts)),
            (Datum::Interval(iv), ScalarType::Interval) => Some(Value::Interval(Interval(iv))),
            (Datum::Bytes(b), ScalarType::Bytes) => Some(Value::Bytea(b.to_vec())),
            (Datum::String(s), ScalarType::String) => Some(Value::Text(s.to_owned())),
            (Datum::String(s), ScalarType::VarChar { .. }) => Some(Value::VarChar(s.to_owned())),
            (Datum::String(s), ScalarType::Char { length }) => {
                Some(Value::BpChar(char::format_str_pad(s, *length)))
            }
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
            (Datum::Array(array), ScalarType::Int2Vector) => {
                let dims = array.dims().into_iter();
                assert!(dims.count() == 1, "int2vector must be 1 dimensional");
                let elements = array
                    .elements()
                    .iter()
                    .map(|elem| Value::from_datum(elem, &ScalarType::Int16))
                    .collect();
                Some(Value::Int2Vector { elements })
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
            (Datum::Range(range), ScalarType::Range { element_type }) => {
                let value_range = range.into_bounds(|b| {
                    Box::new(
                        Value::from_datum(b.datum(), element_type)
                            .expect("RangeBounds never contain Datum::Null"),
                    )
                });
                Some(Value::Range(value_range))
            }
            _ => panic!("can't serialize {}::{:?}", datum, typ),
        }
    }

    /// Converts a Materialize datum from this value.
    pub fn into_datum<'a>(self, buf: &'a RowArena, typ: &Type) -> Datum<'a> {
        match self {
            Value::Array { dims, elements } => {
                let element_pg_type = match typ {
                    Type::Array(t) => &*t,
                    _ => panic!("Value::Array should have type Type::Array. Found {:?}", typ),
                };
                buf.make_datum(|packer| {
                    packer
                        .push_array(
                            &dims,
                            elements.into_iter().map(|element| match element {
                                Some(element) => element.into_datum(buf, element_pg_type),
                                None => Datum::Null,
                            }),
                        )
                        .unwrap();
                })
            }
            Value::Int2Vector { .. } => {
                // This situation is handled gracefully by Value::decode; if we
                // wind up here it's a programming error.
                unreachable!("into_datum cannot be called on Value::Int2Vector");
            }
            Value::Bool(true) => Datum::True,
            Value::Bool(false) => Datum::False,
            Value::Bytea(b) => Datum::Bytes(buf.push_bytes(b)),
            Value::Char(c) => Datum::UInt8(c),
            Value::Date(d) => Datum::Date(d),
            Value::Float4(f) => Datum::Float32(f.into()),
            Value::Float8(f) => Datum::Float64(f.into()),
            Value::Int2(i) => Datum::Int16(i),
            Value::Int4(i) => Datum::Int32(i),
            Value::Int8(i) => Datum::Int64(i),
            Value::UInt2(u) => Datum::UInt16(u.0),
            Value::UInt4(u) => Datum::UInt32(u.0),
            Value::UInt8(u) => Datum::UInt64(u.0),
            Value::Jsonb(js) => buf.push_unary_row(js.0.into_row()),
            Value::List(elems) => {
                let elem_pg_type = match typ {
                    Type::List(t) => &*t,
                    _ => panic!("Value::List should have type Type::List. Found {:?}", typ),
                };
                buf.make_datum(|packer| {
                    packer.push_list(elems.into_iter().map(|elem| match elem {
                        Some(elem) => elem.into_datum(buf, elem_pg_type),
                        None => Datum::Null,
                    }));
                })
            }
            Value::Map(map) => {
                let elem_pg_type = match typ {
                    Type::Map { value_type } => &*value_type,
                    _ => panic!("Value::Map should have type Type::Map. Found {:?}", typ),
                };
                buf.make_datum(|packer| {
                    packer.push_dict_with(|row| {
                        for (k, v) in map {
                            row.push(Datum::String(&k));
                            row.push(match v {
                                Some(elem) => elem.into_datum(buf, elem_pg_type),
                                None => Datum::Null,
                            });
                        }
                    });
                })
            }
            Value::Oid(oid) => Datum::UInt32(oid),
            Value::Record(_) => {
                // This situation is handled gracefully by Value::decode; if we
                // wind up here it's a programming error.
                unreachable!("into_datum cannot be called on Value::Record");
            }
            Value::Time(t) => Datum::Time(t),
            Value::Timestamp(ts) => Datum::Timestamp(ts),
            Value::TimestampTz(ts) => Datum::TimestampTz(ts),
            Value::Interval(iv) => Datum::Interval(iv.0),
            Value::Text(s) => Datum::String(buf.push_string(s)),
            Value::BpChar(s) => Datum::String(buf.push_string(s.trim_end().into())),
            Value::VarChar(s) => Datum::String(buf.push_string(s)),
            Value::Uuid(u) => Datum::Uuid(u),
            Value::Numeric(n) => Datum::Numeric(n.0),
            Value::MzTimestamp(t) => Datum::MzTimestamp(t),
            Value::Range(range) => {
                let elem_pg_type = match typ {
                    Type::Range { element_type } => &*element_type,
                    _ => panic!("Value::Range should have type Type::Range. Found {:?}", typ),
                };
                let range = range.into_bounds(|elem| elem.into_datum(buf, elem_pg_type));

                buf.make_datum(|packer| packer.push_range(range).unwrap())
            }
            Value::MzAclItem(mz_acl_item) => Datum::MzAclItem(mz_acl_item),
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
    pub fn encode_text(&self, buf: &mut BytesMut) -> Nestable {
        match self {
            Value::Array { dims, elements } => {
                strconv::format_array(buf, dims, elements, |buf, elem| match elem {
                    None => Ok::<_, ()>(buf.write_null()),
                    Some(elem) => Ok(elem.encode_text(buf.nonnull_buffer())),
                })
                .expect("provided closure never fails")
            }
            Value::Int2Vector { elements } => {
                strconv::format_legacy_vector(buf, elements, |buf, elem| {
                    Ok::<_, ()>(
                        elem.as_ref()
                            .expect("Int2Vector does not support NULL values")
                            .encode_text(buf.nonnull_buffer()),
                    )
                })
                .expect("provided closure never fails")
            }
            Value::Bool(b) => strconv::format_bool(buf, *b),
            Value::Bytea(b) => strconv::format_bytes(buf, b),
            Value::Char(c) => {
                buf.put_u8(*c);
                Nestable::MayNeedEscaping
            }
            Value::Date(d) => strconv::format_date(buf, *d),
            Value::Int2(i) => strconv::format_int16(buf, *i),
            Value::Int4(i) => strconv::format_int32(buf, *i),
            Value::Int8(i) => strconv::format_int64(buf, *i),
            Value::UInt2(u) => strconv::format_uint16(buf, u.0),
            Value::UInt4(u) => strconv::format_uint32(buf, u.0),
            Value::UInt8(u) => strconv::format_uint64(buf, u.0),
            Value::Interval(iv) => strconv::format_interval(buf, iv.0),
            Value::Float4(f) => strconv::format_float32(buf, *f),
            Value::Float8(f) => strconv::format_float64(buf, *f),
            Value::Jsonb(js) => strconv::format_jsonb(buf, js.0.as_ref()),
            Value::List(elems) => strconv::format_list(buf, elems, |buf, elem| match elem {
                None => Ok::<_, ()>(buf.write_null()),
                Some(elem) => Ok(elem.encode_text(buf.nonnull_buffer())),
            })
            .expect("provided closure never fails"),
            Value::Map(elems) => strconv::format_map(buf, elems, |buf, value| match value {
                None => Ok::<_, ()>(buf.write_null()),
                Some(elem) => Ok(elem.encode_text(buf.nonnull_buffer())),
            })
            .expect("provided closure never fails"),
            Value::Oid(oid) => strconv::format_uint32(buf, *oid),
            Value::Record(elems) => strconv::format_record(buf, elems, |buf, elem| match elem {
                None => Ok::<_, ()>(buf.write_null()),
                Some(elem) => Ok(elem.encode_text(buf.nonnull_buffer())),
            })
            .expect("provided closure never fails"),
            Value::Text(s) | Value::VarChar(s) | Value::BpChar(s) => strconv::format_string(buf, s),
            Value::Time(t) => strconv::format_time(buf, *t),
            Value::Timestamp(ts) => strconv::format_timestamp(buf, ts),
            Value::TimestampTz(ts) => strconv::format_timestamptz(buf, ts),
            Value::Uuid(u) => strconv::format_uuid(buf, *u),
            Value::Numeric(d) => strconv::format_numeric(buf, &d.0),
            Value::MzTimestamp(t) => strconv::format_mz_timestamp(buf, *t),
            Value::Range(range) => strconv::format_range(buf, range, |buf, elem| match elem {
                Some(elem) => Ok(elem.encode_text(buf.nonnull_buffer())),
                None => Ok::<_, ()>(buf.write_null()),
            })
            .expect("provided closure never fails"),
            Value::MzAclItem(mz_acl_item) => strconv::format_mz_acl_item(buf, *mz_acl_item),
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
                    buf.put_i32(dim.lower_bound.try_into().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            "array dimension lower bound does not fit into an i32",
                        )
                    })?);
                }
                for elem in elements {
                    encode_element(buf, elem.as_ref(), elem_type)?;
                }
                Ok(postgres_types::IsNull::No)
            }
            // TODO: what is the binary format of vector types?
            Value::Int2Vector { .. } => {
                Err("binary encoding of int2vector is not implemented".into())
            }
            Value::Bool(b) => b.to_sql(&PgType::BOOL, buf),
            Value::Bytea(b) => b.to_sql(&PgType::BYTEA, buf),
            Value::Char(c) => i8::reinterpret_cast(*c).to_sql(&PgType::CHAR, buf),
            Value::Date(d) => d.pg_epoch_days().to_sql(&PgType::DATE, buf),
            Value::Float4(f) => f.to_sql(&PgType::FLOAT4, buf),
            Value::Float8(f) => f.to_sql(&PgType::FLOAT8, buf),
            Value::Int2(i) => i.to_sql(&PgType::INT2, buf),
            Value::Int4(i) => i.to_sql(&PgType::INT4, buf),
            Value::Int8(i) => i.to_sql(&PgType::INT8, buf),
            Value::UInt2(u) => u.to_sql(&*UINT2, buf),
            Value::UInt4(u) => u.to_sql(&*UINT4, buf),
            Value::UInt8(u) => u.to_sql(&*UINT8, buf),
            Value::Interval(iv) => iv.to_sql(&PgType::INTERVAL, buf),
            Value::Jsonb(js) => js.to_sql(&PgType::JSONB, buf),
            Value::List(_) => {
                // A binary encoding for list is tricky. We only get one OID to
                // describe the type of this list to the client. And we can't
                // just up front allocate an OID for every possible list type,
                // like PostgreSQL does for arrays, because, unlike arrays,
                // lists can be arbitrarily nested.
                //
                // So, we'd need to synthesize a type with a stable OID whenever
                // a new anonymous list type is *observed* in Materialize. Or we
                // could mandate that only named list types can be sent over
                // pgwire, and not anonymous list types, since named list types
                // get a stable OID when they're created. Then we'd need to
                // expose a table with the list OID -> element OID mapping for
                // clients to query. And THEN we'd need to teach every client we
                // care about how to query this table.
                //
                // This isn't intractible. It's how PostgreSQL's range type
                // works, which is supported by many drivers. But our job is
                // harder because most PostgreSQL drivers don't want to carry
                // around code for Materialize-specific types. So we'd have to
                // add type plugin infrastructure for those drivers, then
                // distribute the list/map support as a plugin.
                //
                // Serializing the actual list would be simple, though: just a
                // 32-bit integer describing the list length, followed by the
                // encoding of each element in order.
                //
                // tl;dr it's a lot of work. For now, the recommended workaround
                // is to either use the text encoding or convert the list to a
                // different type (JSON, an array, unnest into rows) that does
                // have a binary encoding.
                Err("binary encoding of list types is not implemented".into())
            }
            Value::Map(_) => {
                // Map binary encodings are hard for the same reason as list
                // binary encodings (described above). You just have key and
                // value OIDs to deal with rather than an element OID.
                Err("binary encoding of map types is not implemented".into())
            }
            Value::Oid(i) => i.to_sql(&PgType::OID, buf),
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
            Value::BpChar(s) => s.to_sql(&PgType::BPCHAR, buf),
            Value::VarChar(s) => s.to_sql(&PgType::VARCHAR, buf),
            Value::Time(t) => t.to_sql(&PgType::TIME, buf),
            Value::Timestamp(ts) => ts.to_sql(&PgType::TIMESTAMP, buf),
            Value::TimestampTz(ts) => ts.to_sql(&PgType::TIMESTAMPTZ, buf),
            Value::Uuid(u) => u.to_sql(&PgType::UUID, buf),
            Value::Numeric(a) => a.to_sql(&PgType::NUMERIC, buf),
            Value::MzTimestamp(t) => t.to_string().to_sql(&PgType::TEXT, buf),
            Value::Range(range) => {
                buf.put_u8(range.pg_flag_bits());

                let elem_type = match ty {
                    Type::Range { element_type } => element_type,
                    _ => unreachable!(),
                };

                if let Some(RangeInner { lower, upper }) = &range.inner {
                    for bound in [&lower.bound, &upper.bound] {
                        if let Some(bound) = bound {
                            let base = buf.len();
                            buf.put_i32(0);
                            bound.encode_binary(elem_type, buf)?;
                            let len = pg_len("encoded range bound", buf.len() - base - 4)?;
                            buf[base..base + 4].copy_from_slice(&len.to_be_bytes());
                        }
                    }
                }
                Ok(postgres_types::IsNull::No)
            }
            Value::MzAclItem(mz_acl_item) => {
                buf.extend_from_slice(&mz_acl_item.encode_binary());
                Ok(postgres_types::IsNull::No)
            }
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
    pub fn decode_text<'a>(
        ty: &'a Type,
        raw: &'a [u8],
    ) -> Result<Value, Box<dyn Error + Sync + Send>> {
        let s = str::from_utf8(raw)?;
        Ok(match ty {
            Type::Array(elem_type) => {
                let elements = strconv::parse_array(
                    s,
                    || None,
                    |elem_text| Value::decode_text(elem_type, elem_text.as_bytes()).map(Some),
                )?;
                // At the moment, we only support one dimensional arrays. Note
                // that empty arrays are represented as zero dimensional arrays,
                // per PostgreSQL.
                let mut dims = vec![];
                if !elements.is_empty() {
                    dims.push(ArrayDimension {
                        lower_bound: 1,
                        length: elements.len(),
                    })
                }
                Value::Array { dims, elements }
            }
            Type::Int2Vector { .. } => {
                return Err("input of Int2Vector types is not implemented".into())
            }
            Type::Bool => Value::Bool(strconv::parse_bool(s)?),
            Type::Bytea => Value::Bytea(strconv::parse_bytes(s)?),
            Type::Char => Value::Char(raw.get(0).copied().unwrap_or(0)),
            Type::Date => Value::Date(strconv::parse_date(s)?),
            Type::Float4 => Value::Float4(strconv::parse_float32(s)?),
            Type::Float8 => Value::Float8(strconv::parse_float64(s)?),
            Type::Int2 => Value::Int2(strconv::parse_int16(s)?),
            Type::Int4 => Value::Int4(strconv::parse_int32(s)?),
            Type::Int8 => Value::Int8(strconv::parse_int64(s)?),
            Type::UInt2 => Value::UInt2(UInt2(strconv::parse_uint16(s)?)),
            Type::UInt4 => Value::UInt4(UInt4(strconv::parse_uint32(s)?)),
            Type::UInt8 => Value::UInt8(UInt8(strconv::parse_uint64(s)?)),
            Type::Interval { .. } => Value::Interval(Interval(strconv::parse_interval(s)?)),
            Type::Json => return Err("input of json types is not implemented".into()),
            Type::Jsonb => Value::Jsonb(Jsonb(strconv::parse_jsonb(s)?)),
            Type::List(elem_type) => Value::List(strconv::parse_list(
                s,
                matches!(**elem_type, Type::List(..)),
                || None,
                |elem_text| Value::decode_text(elem_type, elem_text.as_bytes()).map(Some),
            )?),
            Type::Map { value_type } => Value::Map(strconv::parse_map(
                s,
                matches!(**value_type, Type::Map { .. }),
                |elem_text| Value::decode_text(value_type, elem_text.as_bytes()).map(Some),
            )?),
            Type::Numeric { .. } => Value::Numeric(Numeric(strconv::parse_numeric(s)?)),
            Type::Oid | Type::RegClass | Type::RegProc | Type::RegType => {
                Value::Oid(strconv::parse_oid(s)?)
            }
            Type::Record(_) => {
                return Err("input of anonymous composite types is not implemented".into())
            }
            Type::Text => Value::Text(s.to_owned()),
            Type::BpChar { .. } => Value::BpChar(s.to_owned()),
            Type::VarChar { .. } => Value::VarChar(s.to_owned()),
            Type::Time { .. } => Value::Time(strconv::parse_time(s)?),
            Type::TimeTz { .. } => return Err("input of timetz types is not implemented".into()),
            Type::Timestamp { .. } => Value::Timestamp(strconv::parse_timestamp(s)?),
            Type::TimestampTz { .. } => Value::TimestampTz(strconv::parse_timestamptz(s)?),
            Type::Uuid => Value::Uuid(Uuid::parse_str(s)?),
            Type::MzTimestamp => Value::MzTimestamp(strconv::parse_mz_timestamp(s)?),
            Type::Range { element_type } => Value::Range(strconv::parse_range(s, |elem_text| {
                Value::decode_text(element_type, elem_text.as_bytes()).map(Box::new)
            })?),
            Type::MzAclItem => Value::MzAclItem(strconv::parse_mz_acl_item(s)?),
        })
    }

    /// Deserializes a value of type `ty` from `raw` using the [binary encoding
    /// format](Format::Binary).
    pub fn decode_binary(ty: &Type, raw: &[u8]) -> Result<Value, Box<dyn Error + Sync + Send>> {
        match ty {
            Type::Array(_) => Err("input of array types is not implemented".into()),
            Type::Int2Vector => Err("input of int2vector types is not implemented".into()),
            Type::Bool => bool::from_sql(ty.inner(), raw).map(Value::Bool),
            Type::Bytea => Vec::<u8>::from_sql(ty.inner(), raw).map(Value::Bytea),
            Type::Char => {
                i8::from_sql(ty.inner(), raw).map(|c| Value::Char(u8::reinterpret_cast(c)))
            }
            Type::Date => {
                let days = i32::from_sql(ty.inner(), raw)?;
                Ok(Value::Date(Date::from_pg_epoch(days)?))
            }
            Type::Float4 => f32::from_sql(ty.inner(), raw).map(Value::Float4),
            Type::Float8 => f64::from_sql(ty.inner(), raw).map(Value::Float8),
            Type::Int2 => i16::from_sql(ty.inner(), raw).map(Value::Int2),
            Type::Int4 => i32::from_sql(ty.inner(), raw).map(Value::Int4),
            Type::Int8 => i64::from_sql(ty.inner(), raw).map(Value::Int8),
            Type::UInt2 => UInt2::from_sql(ty.inner(), raw).map(Value::UInt2),
            Type::UInt4 => UInt4::from_sql(ty.inner(), raw).map(Value::UInt4),
            Type::UInt8 => UInt8::from_sql(ty.inner(), raw).map(Value::UInt8),
            Type::Interval { .. } => Interval::from_sql(ty.inner(), raw).map(Value::Interval),
            Type::Json => Err("input of json types is not implemented".into()),
            Type::Jsonb => Jsonb::from_sql(ty.inner(), raw).map(Value::Jsonb),
            Type::List(_) => Err("binary decoding of list types is not implemented".into()),
            Type::Map { .. } => Err("binary decoding of map types is not implemented".into()),
            Type::Numeric { .. } => Numeric::from_sql(ty.inner(), raw).map(Value::Numeric),
            Type::Oid | Type::RegClass | Type::RegProc | Type::RegType => {
                u32::from_sql(ty.inner(), raw).map(Value::Oid)
            }
            Type::Record(_) => Err("input of anonymous composite types is not implemented".into()),
            Type::Text => String::from_sql(ty.inner(), raw).map(Value::Text),
            Type::BpChar { .. } => String::from_sql(ty.inner(), raw).map(Value::BpChar),
            Type::VarChar { .. } => String::from_sql(ty.inner(), raw).map(Value::VarChar),
            Type::Time { .. } => NaiveTime::from_sql(ty.inner(), raw).map(Value::Time),
            Type::TimeTz { .. } => Err("input of timetz types is not implemented".into()),
            Type::Timestamp { .. } => {
                let ts = NaiveDateTime::from_sql(ty.inner(), raw)?;
                Ok(Value::Timestamp(CheckedTimestamp::from_timestamplike(ts)?))
            }
            Type::TimestampTz { .. } => {
                let ts = DateTime::<Utc>::from_sql(ty.inner(), raw)?;
                Ok(Value::TimestampTz(CheckedTimestamp::from_timestamplike(
                    ts,
                )?))
            }
            Type::Uuid => Uuid::from_sql(ty.inner(), raw).map(Value::Uuid),
            Type::MzTimestamp => {
                let s = String::from_sql(ty.inner(), raw)?;
                let t: mz_repr::Timestamp = s.parse()?;
                Ok(Value::MzTimestamp(t))
            }
            Type::Range { .. } => Err("binary decoding of range types is not implemented".into()),
            Type::MzAclItem => {
                let mz_acl_item = MzAclItem::decode_binary(raw)?;
                Ok(Value::MzAclItem(mz_acl_item))
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies that we correctly print the chain of parsing errors, all the way through the stack.
    #[mz_ore::test]
    fn decode_text_error_smoke_test() {
        let bool_array = Value::Array {
            dims: vec![ArrayDimension {
                lower_bound: 0,
                length: 1,
            }],
            elements: vec![Some(Value::Bool(true))],
        };

        let mut buf = BytesMut::new();
        bool_array.encode_text(&mut buf);
        let buf = buf.to_vec();

        let int_array_tpe = Type::Array(Box::new(Type::Int4));
        let decoded_int_array = Value::decode_text(&int_array_tpe, &buf);

        assert_eq!(
            decoded_int_array.map_err(|e| e.to_string()).unwrap_err(),
            "invalid input syntax for type array: specifying array lower bounds is not supported: \"[0:0]={t}\"".to_string()
        );
    }
}
