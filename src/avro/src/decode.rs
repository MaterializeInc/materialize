// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::mem::transmute;

use chrono::{NaiveDate, NaiveDateTime};
use failure::Error;

use crate::from_avro_datum;
use crate::schema::{ResolvedRecordField, SchemaNode, SchemaPiece, SchemaPieceOrNamed};
use crate::types::{DecimalValue, Value};
use crate::util::{safe_len, zag_i32, zag_i64, DecodeError};
use std::{fmt::Display, io::Read};

#[inline]
fn decode_long<R: Read>(reader: &mut R) -> Result<Value, Error> {
    zag_i64(reader).map(Value::Long)
}

#[inline]
fn decode_int<R: Read>(reader: &mut R) -> Result<Value, Error> {
    zag_i32(reader).map(Value::Int)
}

#[inline]
fn decode_len<R: Read>(reader: &mut R) -> Result<usize, Error> {
    zag_i64(reader).and_then(|len| safe_len(len as usize))
}

fn decode_decimal<R: Read>(
    reader: &mut R,
    precision: usize,
    scale: usize,
    fixed_size: Option<usize>,
) -> Result<DecimalValue, Error> {
    let len = match fixed_size {
        Some(len) => len,
        None => decode_len(reader)?,
    };
    let mut buf = Vec::with_capacity(len);
    // FIXME(brennan) - this is UB iff `reader`
    // looks at the bytes. Nuke it during the avro decod3
    // refactor.
    unsafe {
        buf.set_len(len);
    }
    reader.read_exact(&mut buf)?;
    Ok(DecimalValue {
        unscaled: buf,
        precision,
        scale,
    })
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

fn decode_string<R: Read>(reader: &mut R) -> Result<String, Error> {
    let len = decode_len(reader)?;
    let mut buf = Vec::with_capacity(len);
    unsafe {
        buf.set_len(len);
    }
    reader.read_exact(&mut buf)?;

    String::from_utf8(buf).map_err(|_| DecodeError::new("not a valid utf-8 string").into())
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

/// Decode a `Value` from avro format given its `Schema`.
pub fn decode<'a, R: Read>(schema: SchemaNode<'a>, reader: &'a mut R) -> Result<Value, Error> {
    match schema.inner {
        SchemaPiece::Null => Ok(Value::Null),
        SchemaPiece::Boolean => {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf[..])?;

            match buf[0] {
                0u8 => Ok(Value::Boolean(false)),
                1u8 => Ok(Value::Boolean(true)),
                _ => Err(DecodeError::new("not a bool").into()),
            }
        }
        SchemaPiece::Int => decode_int(reader),
        SchemaPiece::Long => decode_long(reader),
        SchemaPiece::Float => decode_float(reader).map(Value::Float),
        SchemaPiece::Double => decode_double(reader).map(Value::Double),
        SchemaPiece::Date => match decode_int(reader)? {
            Value::Int(days) => Ok(Value::Date(
                NaiveDate::from_ymd(1970, 1, 1)
                    .checked_add_signed(chrono::Duration::days(days.into()))
                    .ok_or_else(|| {
                        DecodeError::new(format!("Invalid num days from epoch: {0}", days))
                    })?,
            )),
            other => {
                Err(DecodeError::new(format!("Not an Int32 input for Date: {:?}", other)).into())
            }
        },
        SchemaPiece::TimestampMilli => match decode_long(reader)? {
            Value::Long(millis) => build_ts_value(millis, TsUnit::Millis),
            other => Err(DecodeError::new(format!(
                "Not an Int64 input for Millisecond DateTime: {:?}",
                other
            ))
            .into()),
        },
        SchemaPiece::TimestampMicro => match decode_long(reader)? {
            Value::Long(micros) => build_ts_value(micros, TsUnit::Micros),
            other => Err(DecodeError::new(format!(
                "Not an Int64 input for Microsecond DateTime: {:?}",
                other
            ))
            .into()),
        },
        SchemaPiece::Decimal {
            precision,
            scale,
            fixed_size,
        } => {
            let decimal = decode_decimal(reader, *precision, *scale, *fixed_size)?;
            Ok(Value::Decimal(decimal))
        }
        SchemaPiece::Bytes => {
            let len = decode_len(reader)?;
            let mut buf = Vec::with_capacity(len);
            unsafe {
                buf.set_len(len);
            }
            reader.read_exact(&mut buf)?;
            Ok(Value::Bytes(buf))
        }
        SchemaPiece::String => decode_string(reader).map(Value::String),
        SchemaPiece::Json => {
            let s = decode_string(reader)?;
            let j = serde_json::from_str(s.as_str())?;
            Ok(Value::Json(j))
        }
        SchemaPiece::Array(inner) => {
            let mut items = Vec::new();

            loop {
                let len = decode_len(reader)?;
                // arrays are 0-terminated, 0i64 is also encoded as 0 in Avro
                // reading a length of 0 means the end of the array
                if len == 0 {
                    break;
                }

                items.reserve(len as usize);
                for _ in 0..len {
                    items.push(decode(schema.step(&**inner), reader)?);
                }
            }

            Ok(Value::Array(items))
        }
        SchemaPiece::Map(inner) => {
            let mut items = HashMap::new();

            loop {
                let len = decode_len(reader)?;
                // maps are 0-terminated, 0i64 is also encoded as 0 in Avro
                // reading a length of 0 means the end of the map
                if len == 0 {
                    break;
                }

                items.reserve(len as usize);
                for _ in 0..len {
                    if let Value::String(key) = decode(
                        schema.step(&SchemaPieceOrNamed::Piece(SchemaPiece::String)),
                        reader,
                    )? {
                        let value = decode(schema.step(&**inner), reader)?;
                        items.insert(key, value);
                    } else {
                        return Err(DecodeError::new("map key is not a string").into());
                    }
                }
            }

            Ok(Value::Map(items))
        }
        SchemaPiece::Union(inner) => {
            let index = zag_i64(reader)? as usize;
            let variants = inner.variants();
            match variants.get(index) {
                Some(variant) => {
                    decode(schema.step(variant), reader).map(|x| Value::Union(index, Box::new(x)))
                }
                None => Err(DecodeError::new("Union index out of bounds").into()),
            }
        }
        SchemaPiece::ResolveIntTsMilli => {
            let millis = zag_i32(reader)?.into();
            build_ts_value(millis, TsUnit::Millis)
        }
        SchemaPiece::ResolveIntTsMicro => {
            let micros = zag_i32(reader)?.into();
            build_ts_value(micros, TsUnit::Micros)
        }
        SchemaPiece::ResolveDateTimestamp => {
            let days = zag_i32(reader)?;

            let date = NaiveDate::from_ymd(1970, 1, 1)
                .checked_add_signed(chrono::Duration::days(days.into()))
                .ok_or_else(|| {
                    DecodeError::new(format!("Invalid num days from epoch: {0}", days))
                })?;
            let dt = date.and_hms(0, 0, 0);
            Ok(Value::Timestamp(dt))
        }
        SchemaPiece::ResolveIntLong => zag_i32(reader).map(|x| Value::Long(x.into())),
        SchemaPiece::ResolveIntFloat => zag_i32(reader).map(|x| Value::Float(x as f32)),
        SchemaPiece::ResolveIntDouble => zag_i32(reader).map(|x| Value::Double(x.into())),
        SchemaPiece::ResolveLongFloat => zag_i64(reader).map(|x| Value::Float(x as f32)),
        SchemaPiece::ResolveLongDouble => zag_i64(reader).map(|x| Value::Double(x as f64)),
        SchemaPiece::ResolveFloatDouble => decode_float(reader).map(|x| Value::Double(x as f64)),
        SchemaPiece::ResolveConcreteUnion { index, inner } => {
            decode(schema.step(&**inner), reader).map(|x| Value::Union(*index, Box::new(x)))
        }
        SchemaPiece::ResolveUnionUnion { permutation } => {
            let index = zag_i64(reader)? as usize;
            if index >= permutation.len() {
                return Err(DecodeError::new(format!(
                    "Union variant out of bounds for writer schema: {}; max {}",
                    index,
                    permutation.len()
                ))
                .into());
            }
            match &permutation[index] {
                Err(e) => Err(DecodeError::new(format!(
                    "Union variant not found in reader schema: {}",
                    e
                ))
                .into()),
                Ok((index, variant)) => {
                    decode(schema.step(variant), reader).map(|x| Value::Union(*index, Box::new(x)))
                }
            }
        }
        SchemaPiece::ResolveUnionConcrete { index, inner } => {
            let found_index = zag_i64(reader)? as usize;
            if *index != found_index {
                Err(
                    DecodeError::new("Union variant does not match reader schema's concrete type")
                        .into(),
                )
            } else {
                decode(schema.step(&**inner), reader)
            }
        }
        SchemaPiece::Record { ref fields, .. } => {
            // Benchmarks indicate ~10% improvement using this method.
            let mut items = Vec::new();
            items.reserve(fields.len());
            for field in fields {
                // This clone is also expensive. See if we can do away with it...
                items.push((
                    field.name.clone(),
                    decode(schema.step(&field.schema), reader)?,
                ));
            }
            Ok(Value::Record(items))
            // fields
            // .iter()
            // .map(|field| decode(&field.schema, reader).map(|value| (field.name.clone(), value)))
            // .collect::<Result<Vec<(String, Value)>, _>>()
            // .map(|items| Value::Record(items))
        }
        SchemaPiece::Enum { ref symbols, .. } => {
            if let Value::Int(index) = decode_int(reader)? {
                let index = index as usize;
                if index < symbols.len() {
                    let symbol = symbols[index].clone();
                    Ok(Value::Enum(index, symbol))
                } else {
                    Err(DecodeError::new("enum symbol index out of bounds").into())
                }
            } else {
                Err(DecodeError::new("enum symbol not found").into())
            }
        }
        SchemaPiece::Fixed { size, .. } => {
            let mut buf = vec![0u8; *size as usize];
            reader.read_exact(&mut buf)?;
            Ok(Value::Fixed(*size, buf))
        }
        SchemaPiece::ResolveRecord {
            defaults,
            fields,
            n_reader_fields,
        } => {
            let mut items: Vec<Option<(String, Value)>> = vec![None; *n_reader_fields];
            for default in defaults {
                assert!(default.position < items.len(), "Internal error: n_reader_fields should have been big enough to cover all default positions!");
                if items[default.position].is_some() {
                    return Err(DecodeError::new(format!(
                        "Duplicate record field: {}",
                        &default.name
                    ))
                    .into());
                }
                items[default.position] = Some((default.name.clone(), default.default.clone()));
            }
            for field in fields {
                match field {
                    ResolvedRecordField::Present(field) => {
                        assert!(field.position < items.len(), "Internal error: n_reader_fields should have been big enough to cover all field positions!");
                        if items[field.position].is_some() {
                            return Err(DecodeError::new(format!(
                                "Duplicate record field: {}",
                                &field.name
                            ))
                            .into());
                        }
                        items[field.position] = Some((
                            field.name.clone(),
                            decode(schema.step(&field.schema), reader)?,
                        ))
                    }
                    ResolvedRecordField::Absent(writer_schema) => {
                        let _ignored_val = from_avro_datum(writer_schema, reader)?;
                    }
                }
            }
            let items: Option<Vec<(String, Value)>> = items.into_iter().collect();
            match items {
                Some(items) => Ok(Value::Record(items)),
                None => Err(DecodeError::new(format!(
                    "Not all fields present in {} -- issue with schema resolution?",
                    &schema.name.unwrap()
                ))
                .into()),
            }
        }
        SchemaPiece::ResolveEnum {
            doc: _,
            symbols,
            default,
        } => {
            if let Value::Int(index) = decode_int(reader)? {
                if index >= 0 && (index as usize) < symbols.len() {
                    match symbols[index as usize].clone() {
                        Ok((reader_index, symbol)) => Ok(Value::Enum(reader_index, symbol)),
                        Err(missing) => {
                            if let Some((reader_index, symbol)) = default.clone() {
                                Ok(Value::Enum(reader_index, symbol))
                            } else {
                                Err(DecodeError::new(format!(
                                    "Enum symbol {} at index {} in writer schema not found in reader",
                                    missing, index
                                ))
                                .into())
                            }
                        }
                    }
                } else {
                    Err(DecodeError::new("enum symbol index out of bounds").into())
                }
            } else {
                Err(DecodeError::new("enum symbol not found").into())
            }
        }
    }
}
