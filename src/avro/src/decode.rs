// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::mem::transmute;

use chrono::{NaiveDate, NaiveDateTime};
use failure::{bail, format_err, Error};

use crate::schema::{
    RecordField, ResolvedDefaultValueField, ResolvedRecordField, SchemaNode, SchemaPiece,
    SchemaPieceOrNamed,
};
use crate::types::{DecimalValue, Scalar, Value};
use crate::util::{safe_len, zag_i32, zag_i64, DecodeError};
use std::fmt::Display;
use std::{
    collections::HashMap,
    io::{Read, Seek, SeekFrom},
};

#[inline]
fn decode_long_nonneg<R: Read>(reader: &mut R) -> Result<u64, Error> {
    let u = match zag_i64(reader)? {
        i if i >= 0 => i as u64,
        i => bail!("Expected non-negative integer, got {}", i),
    };
    Ok(u)
}

fn decode_int_nonneg<R: Read>(reader: &mut R) -> Result<u32, Error> {
    let u = match zag_i32(reader)? {
        i if i >= 0 => i as u32,
        i => bail!("Expected non-negative integer, got {}", i),
    };
    Ok(u)
}

#[inline]
fn decode_len<R: Read>(reader: &mut R) -> Result<usize, Error> {
    zag_i64(reader).and_then(|i| safe_len(i as usize))
}

#[inline]
fn decode_float<R: Read>(reader: &mut R) -> Result<f32, Error> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf[..])?;
    Ok(unsafe { transmute::<[u8; 4], f32>(buf) })
}

#[inline]
fn decode_double<R: Read>(reader: &mut R) -> Result<f64, Error> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf[..])?;
    Ok(unsafe { transmute::<[u8; 8], f64>(buf) })
}

#[derive(Debug, Clone, Copy)]
enum TsUnit {
    Millis,
    Micros,
}

impl Display for TsUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TsUnit::Millis => write!(f, "ms"),
            TsUnit::Micros => write!(f, "us"),
        }
    }
}

fn build_ts_value(value: i64, unit: TsUnit) -> Result<Value, Error> {
    let units_per_second = match unit {
        TsUnit::Millis => 1_000,
        TsUnit::Micros => 1_000_000,
    };
    let nanos_per_unit = 1_000_000_000 / units_per_second as u32;
    let seconds = value / units_per_second;
    let fraction = (value % units_per_second) as u32;
    Ok(Value::Timestamp(
        NaiveDateTime::from_timestamp_opt(seconds, fraction * nanos_per_unit).ok_or_else(|| {
            DecodeError::new(format!(
                "Invalid {} timestamp {}.{}",
                unit, seconds, fraction
            ))
        })?,
    ))
}

pub trait Skip {
    fn skip(&mut self, len: usize) -> Result<(), Error>;
}

impl Skip for std::fs::File {
    fn skip(&mut self, len: usize) -> Result<(), Error> {
        self.seek(SeekFrom::Current(len as i64))?;
        Ok(())
    }
}

impl Skip for &[u8] {
    fn skip(&mut self, len: usize) -> Result<(), Error> {
        if len > self.len() {
            bail!(
                "Failed to skip {} bytes in buffer: length is {}.",
                len,
                self.len()
            );
        }
        *self = &self[len..];
        Ok(())
    }
}

impl<S: Skip + ?Sized> Skip for Box<S> {
    fn skip(&mut self, len: usize) -> Result<(), Error> {
        self.as_mut().skip(len)
    }
}

pub trait AvroRead: Read + Skip {}
impl AvroRead for &[u8] {}
impl AvroRead for std::fs::File {}
impl<T: AsRef<[u8]>> Skip for std::io::Cursor<T> {
    fn skip(&mut self, len: usize) -> Result<(), Error> {
        self.seek(SeekFrom::Current(len as i64))?;
        Ok(())
    }
}

impl<T: AsRef<[u8]>> AvroRead for std::io::Cursor<T> {}

impl<R: AvroRead + ?Sized> AvroRead for Box<R> {}

pub enum ValueOrReader<'a, V, R: AvroRead> {
    Value(V),
    Reader { len: usize, r: &'a mut R },
}

enum SchemaOrDefault<'b, R: AvroRead> {
    Schema(&'b mut R, SchemaNode<'b>),
    Default(&'b Value),
}
pub struct AvroFieldAccess<'b, R: AvroRead> {
    schema: SchemaOrDefault<'b, R>,
}

impl<'b, R: AvroRead> AvroFieldAccess<'b, R> {
    pub fn decode_field<D: AvroDecode>(self, d: &mut D) -> Result<(), Error> {
        match self.schema {
            SchemaOrDefault::Schema(r, schema) => {
                let mut des = GeneralDeserializer { schema };
                des.deserialize(r, d)
            }
            SchemaOrDefault::Default(value) => give_value(d, value),
        }
    }
    pub fn decode_to_value(self) -> Result<Value, Error> {
        match self.schema {
            SchemaOrDefault::Schema(r, schema) => decode(schema, r),
            SchemaOrDefault::Default(val) => Ok(val.clone()),
        }
    }
}

pub trait AvroRecordAccess<R: AvroRead> {
    fn next_field<'b>(
        &'b mut self,
    ) -> Result<Option<(&'b str, usize, AvroFieldAccess<'b, R>)>, Error>;
}

struct SimpleRecordAccess<'a, R: AvroRead> {
    schema: SchemaNode<'a>,
    r: &'a mut R,
    fields: &'a [RecordField],
    i: usize,
}

impl<'a, R: AvroRead> SimpleRecordAccess<'a, R> {
    fn new(schema: SchemaNode<'a>, r: &'a mut R, fields: &'a [RecordField]) -> Self {
        Self {
            schema,
            r,
            fields,
            i: 0,
        }
    }
}

impl<'a, R: AvroRead> AvroRecordAccess<R> for SimpleRecordAccess<'a, R> {
    fn next_field<'b>(
        &'b mut self,
    ) -> Result<Option<(&'b str, usize, AvroFieldAccess<'b, R>)>, Error> {
        assert!(self.i <= self.fields.len());
        if self.i == self.fields.len() {
            Ok(None)
        } else {
            let f = &self.fields[self.i];
            self.i += 1;
            Ok(Some((
                f.name.as_str(),
                f.position,
                AvroFieldAccess {
                    schema: SchemaOrDefault::Schema(self.r, self.schema.step(&f.schema)),
                },
            )))
        }
    }
}

struct ValueRecordAccess<'a> {
    values: &'a [(String, Value)],
    i: usize,
}

impl<'a> ValueRecordAccess<'a> {
    fn new(values: &'a [(String, Value)]) -> Self {
        Self { values, i: 0 }
    }
}

impl<'a> AvroRecordAccess<&'a [u8]> for ValueRecordAccess<'a> {
    fn next_field<'b>(
        &'b mut self,
    ) -> Result<Option<(&'b str, usize, AvroFieldAccess<'b, &'a [u8]>)>, Error> {
        assert!(self.i <= self.values.len());
        if self.i == self.values.len() {
            Ok(None)
        } else {
            let (name, val) = &self.values[self.i];
            self.i += 1;
            Ok(Some((
                name.as_str(),
                self.i - 1,
                AvroFieldAccess {
                    schema: SchemaOrDefault::Default(val),
                },
            )))
        }
    }
}

struct ResolvedRecordAccess<'a, R: AvroRead> {
    defaults: &'a [ResolvedDefaultValueField],
    i_defaults: usize,
    fields: &'a [ResolvedRecordField],
    i_fields: usize,
    r: &'a mut R,
    schema: SchemaNode<'a>,
}

impl<'a, R: AvroRead> ResolvedRecordAccess<'a, R> {
    fn new(
        defaults: &'a [ResolvedDefaultValueField],
        fields: &'a [ResolvedRecordField],
        r: &'a mut R,
        schema: SchemaNode<'a>,
    ) -> Self {
        Self {
            defaults,
            i_defaults: 0,
            fields,
            i_fields: 0,
            r,
            schema,
        }
    }
}

impl<'a, R: AvroRead> AvroRecordAccess<R> for ResolvedRecordAccess<'a, R> {
    fn next_field<'b>(
        &'b mut self,
    ) -> Result<Option<(&'b str, usize, AvroFieldAccess<'b, R>)>, Error> {
        assert!(self.i_defaults <= self.defaults.len() && self.i_fields <= self.fields.len());
        if self.i_defaults < self.defaults.len() {
            let default = &self.defaults[self.i_defaults];
            self.i_defaults += 1;
            Ok(Some((
                default.name.as_str(),
                default.position,
                AvroFieldAccess {
                    schema: SchemaOrDefault::Default(&default.default),
                },
            )))
        } else {
            while self.i_fields < self.fields.len() {
                let field = &self.fields[self.i_fields];
                self.i_fields += 1;
                match field {
                    ResolvedRecordField::Absent(absent_schema) => {
                        // we don't care what's in the value, but we still need to read it in order to skip ahead the proper amount in the input.
                        let mut d = GeneralDeserializer {
                            schema: absent_schema.top_node(),
                        };
                        d.deserialize(self.r, &mut TrivialDecoder)?;
                        continue;
                    }
                    ResolvedRecordField::Present(field) => {
                        return Ok(Some((
                            field.name.as_str(),
                            field.position,
                            AvroFieldAccess {
                                schema: SchemaOrDefault::Schema(
                                    self.r,
                                    self.schema.step(&field.schema),
                                ),
                            },
                        )));
                    }
                }
            }
            Ok(None)
        }
    }
}

pub trait AvroArrayAccess {
    fn decode_next<D: AvroDecode>(&mut self, d: &mut D) -> Result<bool, Error>;
}

pub trait AvroMapAccess {
    type R: AvroRead;
    fn next_entry<'b>(
        &'b mut self,
    ) -> Result<Option<(String, AvroEntryAccess<'b, Self::R>)>, Error>;
}

pub struct AvroEntryAccess<'a, R: AvroRead> {
    schema: SchemaNode<'a>,
    r: &'a mut R,
}

impl<'a, R: AvroRead> AvroEntryAccess<'a, R> {
    pub fn decode_entry<D: AvroDecode>(&mut self, d: &mut D) -> Result<(), Error> {
        let mut dsr = GeneralDeserializer {
            schema: self.schema,
        };
        dsr.deserialize(self.r, d)
    }
}

pub struct SimpleMapAccess<'a, R: AvroRead> {
    entry_schema: SchemaNode<'a>,
    r: &'a mut R,
    done: bool,
    remaining: usize,
}

impl<'a, R: AvroRead> AvroMapAccess for SimpleMapAccess<'a, R> {
    type R = R;
    fn next_entry<'b>(&'b mut self) -> Result<Option<(String, AvroEntryAccess<'b, R>)>, Error> {
        if self.done {
            return Ok(None);
        }
        if self.remaining == 0 {
            // TODO -- we can use len_in_bytes to quickly skip non-demanded arrays
            let (len, _len_in_bytes) = match zag_i64(self.r)? {
                len if len > 0 => (len as usize, None),
                neglen if neglen < 0 => (neglen.abs() as usize, Some(decode_len(self.r)?)),
                0 => {
                    self.done = true;
                    return Ok(None);
                }
                _ => unreachable!(),
            };
            self.remaining = len;
        }
        assert!(self.remaining > 0);
        self.remaining -= 1;

        // TODO - We can try to avoid this allocation, but  nobody uses maps in Materialize
        // right now so it doesn't really matter.
        let key_len = decode_len(self.r)?;
        let mut key_buf = vec![];
        key_buf.resize_with(key_len, Default::default);
        self.r.read_exact(&mut key_buf)?;
        let key = String::from_utf8(key_buf)?;

        let a = AvroEntryAccess {
            schema: self.entry_schema,
            r: self.r,
        };
        Ok(Some((key, a)))
    }
}

struct SimpleArrayAccess<'a, R: AvroRead> {
    r: &'a mut R,
    schema: SchemaNode<'a>,
    remaining: usize,
    done: bool,
}

impl<'a, R: AvroRead> SimpleArrayAccess<'a, R> {
    fn new(r: &'a mut R, schema: SchemaNode<'a>) -> Self {
        Self {
            r,
            schema,
            remaining: 0,
            done: false,
        }
    }
}

struct ValueArrayAccess<'a> {
    values: &'a [Value],
    i: usize,
}

impl<'a> ValueArrayAccess<'a> {
    fn new(values: &'a [Value]) -> Self {
        Self { values, i: 0 }
    }
}

impl<'a> AvroArrayAccess for ValueArrayAccess<'a> {
    fn decode_next<D: AvroDecode>(&mut self, d: &mut D) -> Result<bool, Error> {
        assert!(self.i <= self.values.len());
        if self.i == self.values.len() {
            Ok(false)
        } else {
            give_value(d, &self.values[self.i])?;
            self.i += 1;
            Ok(true)
        }
    }
}

impl<'a, R: AvroRead> AvroArrayAccess for SimpleArrayAccess<'a, R> {
    fn decode_next<D: AvroDecode>(&mut self, d: &mut D) -> Result<bool, Error> {
        if self.done {
            return Ok(false);
        }
        if self.remaining == 0 {
            // TODO -- we can use len_in_bytes to quickly skip non-demanded arrays
            let (len, _len_in_bytes) = match zag_i64(self.r)? {
                len if len > 0 => (len as usize, None),
                neglen if neglen < 0 => (neglen.abs() as usize, Some(decode_len(self.r)?)),
                0 => {
                    self.done = true;
                    return Ok(false);
                }
                _ => unreachable!(),
            };
            self.remaining = len;
        }
        assert!(self.remaining > 0);
        self.remaining -= 1;
        let mut des = GeneralDeserializer {
            schema: self.schema,
        };
        des.deserialize(self.r, d)?;
        Ok(true)
    }
}

pub trait AvroDecode {
    fn record<R: AvroRead, A: AvroRecordAccess<R>>(&mut self, _a: &mut A) -> Result<(), Error> {
        bail!("Unexpected record");
    }

    fn union_branch<'a, R: AvroRead, D: AvroDeserializer>(
        &mut self,
        _idx: usize,
        _n_variants: usize,
        _null_variant: Option<usize>,
        _deserializer: &'a mut D,
        _reader: &'a mut R,
    ) -> Result<(), Error> {
        bail!("Unexpected union");
    }

    fn array<A: AvroArrayAccess>(&mut self, _a: &mut A) -> Result<(), Error> {
        bail!("Unexpected array");
    }

    fn map<M: AvroMapAccess>(&mut self, _m: &mut M) -> Result<(), Error> {
        bail!("Unexpected map");
    }

    fn enum_variant(&mut self, _symbol: &str, _idx: usize) -> Result<(), Error> {
        bail!("Unexpected enum");
    }

    fn scalar(&mut self, _scalar: Scalar) -> Result<(), Error> {
        bail!("Unexpected scalar");
    }

    fn decimal<'a, R: AvroRead>(
        &mut self,
        _precision: usize,
        _scale: usize,
        _r: ValueOrReader<'a, &'a [u8], R>,
    ) -> Result<(), Error> {
        bail!("Unexpected decimal");
    }
    fn bytes<'a, R: AvroRead>(&mut self, _r: ValueOrReader<'a, &'a [u8], R>) -> Result<(), Error> {
        bail!("Unexpected bytes");
    }
    fn string<'a, R: AvroRead>(&mut self, _r: ValueOrReader<'a, &'a str, R>) -> Result<(), Error> {
        bail!("Unexpected string");
    }
    fn json<'a, R: AvroRead>(
        &mut self,
        _r: ValueOrReader<'a, &'a serde_json::Value, R>,
    ) -> Result<(), Error> {
        bail!("Unexpected json");
    }
    fn fixed<'a, R: AvroRead>(&mut self, _r: ValueOrReader<'a, &'a [u8], R>) -> Result<(), Error> {
        bail!("Unexpected fixed");
    }
}

pub struct TrivialDecoder;

impl TrivialDecoder {
    fn maybe_skip<'a, V, R: AvroRead>(&mut self, r: ValueOrReader<'a, V, R>) -> Result<(), Error> {
        if let ValueOrReader::Reader { len, r } = r {
            r.skip(len)
        } else {
            Ok(())
        }
    }
}

impl AvroDecode for TrivialDecoder {
    fn record<R: AvroRead, A: AvroRecordAccess<R>>(&mut self, a: &mut A) -> Result<(), Error> {
        while let Some((_, _, f)) = a.next_field()? {
            f.decode_field(&mut TrivialDecoder)?;
        }
        Ok(())
    }
    fn union_branch<'a, R: AvroRead, D: AvroDeserializer>(
        &mut self,
        _idx: usize,
        _n_variants: usize,
        _null_variant: Option<usize>,
        deserializer: &'a mut D,
        reader: &'a mut R,
    ) -> Result<(), Error> {
        deserializer.deserialize(reader, self)
    }

    fn enum_variant(&mut self, _symbol: &str, _idx: usize) -> Result<(), Error> {
        Ok(())
    }
    fn scalar(&mut self, _scalar: Scalar) -> Result<(), Error> {
        Ok(())
    }
    fn decimal<'a, R: AvroRead>(
        &mut self,
        _precision: usize,
        _scale: usize,
        r: ValueOrReader<'a, &'a [u8], R>,
    ) -> Result<(), Error> {
        self.maybe_skip(r)
    }
    fn bytes<'a, R: AvroRead>(&mut self, r: ValueOrReader<'a, &'a [u8], R>) -> Result<(), Error> {
        self.maybe_skip(r)
    }
    fn string<'a, R: AvroRead>(&mut self, r: ValueOrReader<'a, &'a str, R>) -> Result<(), Error> {
        self.maybe_skip(r)
    }
    fn json<'a, R: AvroRead>(
        &mut self,
        r: ValueOrReader<'a, &'a serde_json::Value, R>,
    ) -> Result<(), Error> {
        self.maybe_skip(r)
    }
    fn fixed<'a, R: AvroRead>(&mut self, r: ValueOrReader<'a, &'a [u8], R>) -> Result<(), Error> {
        self.maybe_skip(r)
    }
    fn array<A: AvroArrayAccess>(&mut self, a: &mut A) -> Result<(), Error> {
        while a.decode_next(&mut TrivialDecoder)? {}
        Ok(())
    }
}

impl<'a> AvroDeserializer for &'a Value {
    fn deserialize<R: AvroRead, D: AvroDecode>(
        &mut self,
        _r: &mut R,
        d: &mut D,
    ) -> Result<(), Error> {
        give_value(d, *self)
    }
}

pub fn give_value<D: AvroDecode>(d: &mut D, v: &Value) -> Result<(), Error> {
    use ValueOrReader::Value as V;
    match v {
        Value::Null => d.scalar(Scalar::Null),
        Value::Boolean(val) => d.scalar(Scalar::Boolean(*val)),
        Value::Int(val) => d.scalar(Scalar::Int(*val)),
        Value::Long(val) => d.scalar(Scalar::Long(*val)),
        Value::Float(val) => d.scalar(Scalar::Float(*val)),
        Value::Double(val) => d.scalar(Scalar::Double(*val)),
        Value::Date(val) => d.scalar(Scalar::Date(*val)),
        Value::Timestamp(val) => d.scalar(Scalar::Timestamp(*val)),
        // The &[u8] parameter here (and elsewhere in this function) is arbitrary, but we have to put in something in order for the function
        // to type-check
        Value::Decimal(val) => d.decimal::<&[u8]>(val.precision, val.scale, V(&val.unscaled)),
        Value::Bytes(val) => d.bytes::<&[u8]>(V(val)),
        Value::String(val) => d.string::<&[u8]>(V(val)),
        Value::Fixed(_len, val) => d.fixed::<&[u8]>(V(val)),
        Value::Enum(idx, symbol) => d.enum_variant(symbol, *idx as usize),
        Value::Union {
            index,
            inner,
            n_variants,
            null_variant,
        } => {
            let mut empty_reader: &[u8] = &[];
            d.union_branch(
                *index,
                *n_variants,
                *null_variant,
                &mut inner.as_ref(),
                &mut empty_reader,
            )
        }
        Value::Array(val) => {
            let mut a = ValueArrayAccess::new(val);
            d.array(&mut a)
        }
        Value::Map(_val) => todo!(),
        Value::Record(val) => {
            let mut a = ValueRecordAccess::new(val);
            d.record(&mut a)
        }
        Value::Json(val) => d.json::<&[u8]>(V(val)),
    }
}

pub trait AvroDeserializer {
    fn deserialize<R: AvroRead, D: AvroDecode>(
        &mut self,
        r: &mut R,
        d: &mut D,
    ) -> Result<(), Error>;
}

pub struct GeneralDeserializer<'a> {
    pub schema: SchemaNode<'a>,
}

impl<'a> AvroDeserializer for GeneralDeserializer<'a> {
    fn deserialize<R: AvroRead, D: AvroDecode>(
        &mut self,
        r: &mut R,
        d: &mut D,
    ) -> Result<(), Error> {
        use ValueOrReader::Reader;
        match self.schema.inner {
            SchemaPiece::Null => d.scalar(Scalar::Null),
            SchemaPiece::Boolean => {
                let mut buf = [0u8; 1];
                r.read_exact(&mut buf[..])?;
                let val = match buf[0] {
                    0u8 => false,
                    1u8 => true,
                    _ => return Err(DecodeError::new("not a bool").into()),
                };
                d.scalar(Scalar::Boolean(val))
            }
            SchemaPiece::Int => {
                let val = zag_i32(r)?;
                d.scalar(Scalar::Int(val))
            }
            SchemaPiece::Long => {
                let val = zag_i64(r)?;
                d.scalar(Scalar::Long(val))
            }
            SchemaPiece::Float => {
                let val = decode_float(r)?;
                d.scalar(Scalar::Float(val))
            }
            SchemaPiece::Double => {
                let val = decode_double(r)?;
                d.scalar(Scalar::Double(val))
            }
            SchemaPiece::Date => {
                let days = zag_i32(r)?;
                let val = NaiveDate::from_ymd(1970, 1, 1)
                    .checked_add_signed(chrono::Duration::days(days.into()))
                    .ok_or_else(|| {
                        DecodeError::new(format!("Invalid num days from epoch: {0}", days))
                    })?;
                d.scalar(Scalar::Date(val))
            }
            SchemaPiece::TimestampMilli => {
                let total_millis = zag_i64(r)?;
                let scalar = match build_ts_value(total_millis, TsUnit::Millis)? {
                    Value::Timestamp(ts) => Scalar::Timestamp(ts),
                    _ => unreachable!(),
                };
                d.scalar(scalar)
            }
            SchemaPiece::TimestampMicro => {
                let total_micros = zag_i64(r)?;
                let scalar = match build_ts_value(total_micros, TsUnit::Micros)? {
                    Value::Timestamp(ts) => Scalar::Timestamp(ts),
                    _ => unreachable!(),
                };
                d.scalar(scalar)
            }
            SchemaPiece::Decimal {
                precision,
                scale,
                fixed_size,
            } => {
                let len = fixed_size.map(Ok).unwrap_or_else(|| decode_len(r))?;
                d.decimal(*precision, *scale, Reader { len, r })
            }
            SchemaPiece::Bytes => {
                let len = decode_len(r)?;
                d.bytes(Reader { len, r })
            }
            SchemaPiece::String => {
                let len = decode_len(r)?;
                d.string(Reader { len, r })
            }
            SchemaPiece::Json => {
                let len = decode_len(r)?;
                d.json(Reader { len, r })
            }
            SchemaPiece::Array(inner) => {
                // From the spec:
                // Arrays are encoded as a series of blocks. Each block consists of a long count value, followed by that many array items. A block with count zero indicates the end of the array. Each item is encoded per the array's item schema.
                // If a block's count is negative, its absolute value is used, and the count is followed immediately by a long block size indicating the number of bytes in the block. This block size permits fast skipping through data, e.g., when projecting a record to a subset of its fields.

                let mut a = SimpleArrayAccess::new(r, self.schema.step(inner));
                d.array(&mut a)
            }
            SchemaPiece::Map(inner) => {
                // See logic for `SchemaPiece::Array` above. Maps are encoded similarly.
                let mut m = SimpleMapAccess {
                    entry_schema: self.schema.step(inner),
                    r,
                    done: false,
                    remaining: 0,
                };
                d.map(&mut m)
            }
            SchemaPiece::Union(inner) => {
                let index = decode_long_nonneg(r)? as usize;
                let variants = inner.variants();
                match variants.get(index) {
                    Some(variant) => {
                        let n_variants = variants.len();
                        let null_variant = variants
                            .iter()
                            .position(|v| v == &SchemaPieceOrNamed::Piece(SchemaPiece::Null));
                        self.schema = self.schema.step(variant);
                        d.union_branch(index, n_variants, null_variant, self, r)
                    }
                    None => Err(DecodeError::new(format!(
                        "Union index out of bounds (new): {}",
                        index
                    ))
                    .into()),
                }
            }
            SchemaPiece::ResolveIntLong => {
                let val = zag_i32(r)? as i64;
                d.scalar(Scalar::Long(val))
            }
            SchemaPiece::ResolveIntFloat => {
                let val = zag_i32(r)? as f32;
                d.scalar(Scalar::Float(val))
            }
            SchemaPiece::ResolveIntDouble => {
                let val = zag_i32(r)? as f64;
                d.scalar(Scalar::Double(val))
            }
            SchemaPiece::ResolveLongFloat => {
                let val = zag_i64(r)? as f32;
                d.scalar(Scalar::Float(val))
            }
            SchemaPiece::ResolveLongDouble => {
                let val = zag_i64(r)? as f64;
                d.scalar(Scalar::Double(val))
            }
            SchemaPiece::ResolveFloatDouble => {
                let val = decode_float(r)? as f64;
                d.scalar(Scalar::Double(val))
            }
            SchemaPiece::ResolveConcreteUnion {
                index,
                inner,
                n_reader_variants,
                reader_null_variant,
            } => {
                self.schema = self.schema.step(&**inner);
                d.union_branch(*index, *n_reader_variants, *reader_null_variant, self, r)
            }
            SchemaPiece::ResolveUnionUnion {
                permutation,
                n_reader_variants,
                reader_null_variant,
            } => {
                let index = decode_long_nonneg(r)? as usize;
                if index >= permutation.len() {
                    return Err(DecodeError::new(format!(
                        "Union variant out of bounds for writer schema: {}; max {}",
                        index,
                        permutation.len()
                    ))
                    .into());
                }
                match &permutation[index] {
                    Err(e) => {
                        return Err(DecodeError::new(format!(
                            "Union variant not found in reader schema: {}",
                            e
                        ))
                        .into())
                    }
                    Ok((index, variant)) => {
                        self.schema = self.schema.step(variant);
                        d.union_branch(*index, *n_reader_variants, *reader_null_variant, self, r)
                    }
                }
            }
            SchemaPiece::ResolveUnionConcrete { index, inner } => {
                let found_index = decode_long_nonneg(r)? as usize;
                if *index != found_index {
                    Err(DecodeError::new(
                        "Union variant does not match reader schema's concrete type",
                    )
                    .into())
                } else {
                    self.schema = self.schema.step(inner.as_ref());
                    // The reader is not expecting a union here, so don't call `D::union_branch`
                    self.deserialize(r, d)
                }
            }
            SchemaPiece::Record {
                doc: _,
                fields,
                lookup: _,
            } => {
                let mut a = SimpleRecordAccess::new(self.schema, r, fields);
                d.record(&mut a)
                // d.begin_record()?;
                // for field in fields {
                //     d.record_field(&field.name, field.position)?;
                //     decode_new(schema.step(&field.schema), r, d)?;
                // }
                // d.end_record()
            }
            SchemaPiece::Enum {
                symbols,
                doc: _,
                default_idx: _,
            } => {
                let index = decode_int_nonneg(r)? as usize;
                match symbols.get(index) {
                    None => Err(DecodeError::new(format!(
                        "enum symbol index out of bounds: {}, [0, {})",
                        index,
                        symbols.len()
                    ))
                    .into()),
                    Some(symbol) => d.enum_variant(symbol, index),
                }
            }
            SchemaPiece::Fixed { size } => d.fixed(Reader { len: *size, r }),
            // XXX - This does not deliver fields to the consumer in the same order they were
            // declared in the reader schema, which might cause headache for consumers...
            // Unfortunately, there isn't a good way to do so without pre-decoding the whole record
            // (which would require a lot of allocations)
            // and then sorting the fields. So, just let the consumer deal with re-ordering.
            SchemaPiece::ResolveRecord {
                defaults,
                fields,
                n_reader_fields: _,
            } => {
                let mut a = ResolvedRecordAccess::new(defaults, fields, r, self.schema);
                d.record(&mut a)
            }
            SchemaPiece::ResolveEnum {
                doc: _,
                symbols,
                default,
            } => {
                let index = decode_int_nonneg(r)? as usize;
                match symbols.get(index) {
                    None => Err(DecodeError::new(format!(
                        "enum symbol index out of bounds: {}, [0, {})",
                        index,
                        symbols.len()
                    ))
                    .into()),
                    Some(op) => match op {
                        Err(missing) => {
                            if let Some((reader_index, symbol)) = default.clone() {
                                d.enum_variant(&symbol, reader_index)
                            } else {
                                return Err(DecodeError::new(format!(
                                    "Enum symbol {} at index {} in writer schema not found in reader",
                                    missing, index
                                ))
                                    .into());
                            }
                        }
                        Ok((index, name)) => d.enum_variant(name, *index),
                    },
                }
            }
            SchemaPiece::ResolveIntTsMilli => {
                let total_millis = zag_i32(r)?;
                let scalar = match build_ts_value(total_millis as i64, TsUnit::Millis)? {
                    Value::Timestamp(ts) => Scalar::Timestamp(ts),
                    _ => unreachable!(),
                };
                d.scalar(scalar)
            }
            SchemaPiece::ResolveIntTsMicro => {
                let total_micros = zag_i32(r)?;
                let scalar = match build_ts_value(total_micros as i64, TsUnit::Micros)? {
                    Value::Timestamp(ts) => Scalar::Timestamp(ts),
                    _ => unreachable!(),
                };
                d.scalar(scalar)
            }
            SchemaPiece::ResolveDateTimestamp => {
                let days = zag_i32(r)?;

                let date = NaiveDate::from_ymd(1970, 1, 1)
                    .checked_add_signed(chrono::Duration::days(days.into()))
                    .ok_or_else(|| {
                        DecodeError::new(format!("Invalid num days from epoch: {0}", days))
                    })?;
                let dt = date.and_hms(0, 0, 0);
                d.scalar(Scalar::Timestamp(dt))
            }
        }
    }
}
struct ValueDecoder {
    value: Option<Value>,
}
impl ValueDecoder {
    fn new() -> Self {
        Self { value: None }
    }
}
impl AvroDecode for ValueDecoder {
    fn record<R: AvroRead, A: AvroRecordAccess<R>>(&mut self, a: &mut A) -> Result<(), Error> {
        assert!(self.value.is_none());
        let mut fields = vec![];
        while let Some((name, idx, f)) = a.next_field()? {
            let val = f.decode_to_value()?;
            fields.push((idx, (name.to_string(), val)));
        }
        fields.sort_by_key(|(idx, _)| *idx);
        self.value = Some(Value::Record(
            fields
                .into_iter()
                .map(|(_idx, (name, val))| (name, val))
                .collect(),
        ));
        Ok(())
    }
    fn union_branch<'a, R: AvroRead, D: AvroDeserializer>(
        &mut self,
        index: usize,
        n_variants: usize,
        null_variant: Option<usize>,
        deserializer: &'a mut D,
        reader: &'a mut R,
    ) -> Result<(), Error> {
        assert!(self.value.is_none());
        let mut d = ValueDecoder::new();
        deserializer.deserialize(reader, &mut d)?;
        let inner = d
            .value
            .ok_or_else(|| format_err!("Internal error: no union variant decoded!"))?;
        self.value = Some(Value::Union {
            index,
            inner: Box::new(inner),
            n_variants,
            null_variant,
        });
        Ok(())
    }
    fn array<A: AvroArrayAccess>(&mut self, a: &mut A) -> Result<(), Error> {
        assert!(self.value.is_none());
        let mut items = vec![];
        let mut d = ValueDecoder::new();
        while a.decode_next(&mut d)? {
            if let Some(value) = d.value.take() {
                items.push(value)
            }
        }
        self.value = Some(Value::Array(items));
        Ok(())
    }
    fn enum_variant(&mut self, symbol: &str, idx: usize) -> Result<(), Error> {
        assert!(self.value.is_none());
        self.value = Some(Value::Enum(idx, symbol.to_string()));
        Ok(())
    }
    fn scalar(&mut self, scalar: Scalar) -> Result<(), Error> {
        assert!(self.value.is_none());
        self.value = Some(scalar.into());
        Ok(())
    }
    fn decimal<'a, R: AvroRead>(
        &mut self,
        precision: usize,
        scale: usize,
        r: ValueOrReader<'a, &'a [u8], R>,
    ) -> Result<(), Error> {
        assert!(self.value.is_none());
        let unscaled = match r {
            ValueOrReader::Value(buf) => buf.to_vec(),
            ValueOrReader::Reader { len, r } => {
                let mut buf = vec![];
                buf.resize_with(len, Default::default);
                r.read_exact(&mut buf)?;
                buf
            }
        };
        self.value = Some(Value::Decimal(DecimalValue {
            unscaled,
            precision,
            scale,
        }));
        Ok(())
    }
    fn bytes<'a, R: AvroRead>(&mut self, r: ValueOrReader<'a, &'a [u8], R>) -> Result<(), Error> {
        assert!(self.value.is_none());
        let buf = match r {
            ValueOrReader::Value(buf) => buf.to_vec(),
            ValueOrReader::Reader { len, r } => {
                let mut buf = vec![];
                buf.resize_with(len, Default::default);
                r.read_exact(&mut buf)?;
                buf
            }
        };
        self.value = Some(Value::Bytes(buf));
        Ok(())
    }
    fn string<'a, R: AvroRead>(&mut self, r: ValueOrReader<'a, &'a str, R>) -> Result<(), Error> {
        assert!(self.value.is_none());
        let s = match r {
            ValueOrReader::Value(s) => s.to_string(),
            ValueOrReader::Reader { len, r } => {
                let mut buf = vec![];
                buf.resize_with(len, Default::default);
                r.read_exact(&mut buf)?;
                String::from_utf8(buf)?
            }
        };
        self.value = Some(Value::String(s));
        Ok(())
    }
    fn json<'a, R: AvroRead>(
        &mut self,
        r: ValueOrReader<'a, &'a serde_json::Value, R>,
    ) -> Result<(), Error> {
        assert!(self.value.is_none());
        let val = match r {
            ValueOrReader::Value(val) => val.clone(),
            ValueOrReader::Reader { len, r } => {
                let mut buf = vec![];
                buf.resize_with(len, Default::default);
                r.read_exact(&mut buf)?;
                serde_json::from_slice(&buf)?
            }
        };
        self.value = Some(Value::Json(val));
        Ok(())
    }
    fn fixed<'a, R: AvroRead>(&mut self, r: ValueOrReader<'a, &'a [u8], R>) -> Result<(), Error> {
        assert!(self.value.is_none());
        let buf = match r {
            ValueOrReader::Value(buf) => buf.to_vec(),
            ValueOrReader::Reader { len, r } => {
                let mut buf = vec![];
                buf.resize_with(len, Default::default);
                r.read_exact(&mut buf)?;
                buf
            }
        };
        self.value = Some(Value::Fixed(buf.len(), buf));
        Ok(())
    }
    fn map<M: AvroMapAccess>(&mut self, m: &mut M) -> Result<(), Error> {
        assert!(self.value.is_none());
        let mut entries = HashMap::new();
        let mut d = ValueDecoder::new();
        while let Some((name, mut a)) = m.next_entry()? {
            a.decode_entry(&mut d)?;
            let val = d
                .value
                .take()
                .ok_or_else(|| format_err!("Internal error: no map entry decoded"))?;
            entries.insert(name, val);
        }
        self.value = Some(Value::Map(entries));
        Ok(())
    }
}
/// Decode a `Value` from avro format given its `Schema`.
pub fn decode<'a, R: AvroRead>(schema: SchemaNode<'a>, reader: &'a mut R) -> Result<Value, Error> {
    let mut d = ValueDecoder::new();
    let mut dsr = GeneralDeserializer { schema };
    dsr.deserialize(reader, &mut d)?;
    d.value.ok_or_else(|| format_err!("No value decoed"))
}
