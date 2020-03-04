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
use futures::future::{BoxFuture, FutureExt};
use tokio::io::{AsyncRead, AsyncReadExt};

#[inline]
async fn decode_long<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Value, Error> {
    zag_i64(reader).await.map(Value::Long)
}

#[inline]
async fn decode_int<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Value, Error> {
    zag_i32(reader).await.map(Value::Int)
}

#[inline]
async fn decode_len<R: AsyncRead + Unpin>(reader: &mut R) -> Result<usize, Error> {
    zag_i64(reader).await.and_then(|len| safe_len(len as usize))
}

async fn decode_decimal<R: AsyncRead + Unpin>(
    reader: &mut R,
    precision: usize,
    scale: usize,
    fixed_size: Option<usize>,
) -> Result<DecimalValue, Error> {
    let len = match fixed_size {
        Some(len) => len,
        None => decode_len(reader).await?,
    };
    let mut buf = Vec::with_capacity(len);
    unsafe {
        buf.set_len(len);
    }
    reader.read_exact(&mut buf).await?;
    Ok(DecimalValue {
        unscaled: buf,
        precision,
        scale,
    })
}

#[inline]
async fn decode_float<R: AsyncRead + Unpin>(reader: &mut R) -> Result<f32, Error> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf[..]).await?;
    Ok(unsafe { transmute::<[u8; 4], f32>(buf) })
}

#[inline]
async fn decode_double<R: AsyncRead + Unpin>(reader: &mut R) -> Result<f64, Error> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf[..]).await?;
    Ok(unsafe { transmute::<[u8; 8], f64>(buf) })
}

/// Decode a `Value` from avro format given its `Schema`.
pub fn decode<'a, R: AsyncRead + Unpin + Send>(
    schema: SchemaNode<'a>,
    reader: &'a mut R,
) -> BoxFuture<'a, Result<Value, Error>> {
    async move {
        match schema.inner {
            SchemaPiece::Null => Ok(Value::Null),
            SchemaPiece::Boolean => {
                let mut buf = [0u8; 1];
                reader.read_exact(&mut buf[..]).await?;

                match buf[0] {
                    0u8 => Ok(Value::Boolean(false)),
                    1u8 => Ok(Value::Boolean(true)),
                    _ => Err(DecodeError::new("not a bool").into()),
                }
            }
            SchemaPiece::Int => decode_int(reader).await,
            SchemaPiece::Long => decode_long(reader).await,
            SchemaPiece::Float => decode_float(reader).await.map(Value::Float),
            SchemaPiece::Double => decode_double(reader).await.map(Value::Double),
            SchemaPiece::Date => match decode_int(reader).await? {
                Value::Int(days) => Ok(Value::Date(
                    NaiveDate::from_ymd(1970, 1, 1)
                        .checked_add_signed(chrono::Duration::days(days.into()))
                        .ok_or_else(|| {
                            DecodeError::new(format!("Invalid num days from epoch: {0}", days))
                        })?,
                )),
                other => Err(
                    DecodeError::new(format!("Not an Int32 input for Date: {:?}", other)).into(),
                ),
            },
            SchemaPiece::TimestampMilli => match decode_long(reader).await? {
                Value::Long(millis) => {
                    let seconds = millis / 1_000;
                    let millis = (millis % 1_000) as u32;
                    Ok(Value::Timestamp(
                        NaiveDateTime::from_timestamp_opt(seconds, millis * 1_000_000).ok_or_else(
                            || {
                                DecodeError::new(format!(
                                    "Invalid ms timestamp {}.{}",
                                    seconds, millis
                                ))
                            },
                        )?,
                    ))
                }
                other => Err(DecodeError::new(format!(
                    "Not an Int64 input for Millisecond DateTime: {:?}",
                    other
                ))
                .into()),
            },
            SchemaPiece::TimestampMicro => match decode_long(reader).await? {
                Value::Long(micros) => {
                    let seconds = micros / 1_000_000;
                    let micros = (micros % 1_000_000) as u32;
                    Ok(Value::Timestamp(
                        NaiveDateTime::from_timestamp_opt(seconds, micros * 1_000).ok_or_else(
                            || {
                                DecodeError::new(format!(
                                    "Invalid mu timestamp {}.{}",
                                    seconds, micros
                                ))
                            },
                        )?,
                    ))
                }
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
                let decimal = decode_decimal(reader, *precision, *scale, *fixed_size).await?;
                Ok(Value::Decimal(decimal))
            }
            SchemaPiece::Bytes => {
                let len = decode_len(reader).await?;
                let mut buf = Vec::with_capacity(len);
                unsafe {
                    buf.set_len(len);
                }
                reader.read_exact(&mut buf).await?;
                Ok(Value::Bytes(buf))
            }
            SchemaPiece::String => {
                let len = decode_len(reader).await?;
                let mut buf = Vec::with_capacity(len);
                unsafe {
                    buf.set_len(len);
                }
                reader.read_exact(&mut buf).await?;

                String::from_utf8(buf)
                    .map(Value::String)
                    .map_err(|_| DecodeError::new("not a valid utf-8 string").into())
            }
            SchemaPiece::Array(inner) => {
                let mut items = Vec::new();

                loop {
                    let len = decode_len(reader).await?;
                    // arrays are 0-terminated, 0i64 is also encoded as 0 in Avro
                    // reading a length of 0 means the end of the array
                    if len == 0 {
                        break;
                    }

                    items.reserve(len as usize);
                    for _ in 0..len {
                        items.push(decode(schema.step(&**inner), reader).await?);
                    }
                }

                Ok(Value::Array(items))
            }
            SchemaPiece::Map(inner) => {
                let mut items = HashMap::new();

                loop {
                    let len = decode_len(reader).await?;
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
                        )
                        .await?
                        {
                            let value = decode(schema.step(&**inner), reader).await?;
                            items.insert(key, value);
                        } else {
                            return Err(DecodeError::new("map key is not a string").into());
                        }
                    }
                }

                Ok(Value::Map(items))
            }
            SchemaPiece::Union(inner) => {
                let index = zag_i64(reader).await? as usize;
                let variants = inner.variants();
                match variants.get(index) {
                    Some(variant) => decode(schema.step(variant), reader)
                        .await
                        .map(|x| Value::Union(index, Box::new(x))),
                    None => Err(DecodeError::new("Union index out of bounds").into()),
                }
            }
            SchemaPiece::ResolveIntLong => zag_i32(reader).await.map(|x| Value::Long(x as i64)),
            SchemaPiece::ResolveIntFloat => zag_i32(reader).await.map(|x| Value::Float(x as f32)),
            SchemaPiece::ResolveIntDouble => zag_i32(reader).await.map(|x| Value::Double(x as f64)),
            SchemaPiece::ResolveLongFloat => zag_i64(reader).await.map(|x| Value::Float(x as f32)),
            SchemaPiece::ResolveLongDouble => {
                zag_i64(reader).await.map(|x| Value::Double(x as f64))
            }
            SchemaPiece::ResolveFloatDouble => {
                decode_float(reader).await.map(|x| Value::Double(x as f64))
            }
            SchemaPiece::ResolveConcreteUnion { index, inner } => {
                decode(schema.step(&**inner), reader)
                    .await
                    .map(|x| Value::Union(*index, Box::new(x)))
            }
            SchemaPiece::ResolveUnionUnion { permutation } => {
                let index = zag_i64(reader).await? as usize;
                match &permutation[index] {
                    None => {
                        Err(DecodeError::new("Union variant not found in reader schema").into())
                    }
                    Some((index, variant)) => decode(schema.step(variant), reader)
                        .await
                        .map(|x| Value::Union(*index, Box::new(x))),
                }
            }
            SchemaPiece::ResolveUnionConcrete { index, inner } => {
                let found_index = zag_i64(reader).await? as usize;
                if *index != found_index {
                    Err(DecodeError::new(
                        "Union variant does not match reader schema's concrete type",
                    )
                    .into())
                } else {
                    decode(schema.step(&**inner), reader).await
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
                        decode(schema.step(&field.schema), reader).await?,
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
                if let Value::Int(index) = decode_int(reader).await? {
                    if index >= 0 && (index as usize) <= symbols.len() {
                        let symbol = symbols[index as usize].clone();
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
                reader.read_exact(&mut buf).await?;
                Ok(Value::Fixed(*size, buf))
            }
            SchemaPiece::ResolveRecord {
                defaults,
                fields,
                n_reader_fields,
            } => {
                let mut items: Vec<Option<(String, Value)>> = vec![None; *n_reader_fields];
                for default in defaults {
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
                            if items[field.position].is_some() {
                                return Err(DecodeError::new(format!(
                                    "Duplicate record field: {}",
                                    &field.name
                                ))
                                .into());
                            }
                            items[field.position] = Some((
                                field.name.clone(),
                                decode(schema.step(&field.schema), reader).await?,
                            ))
                        }
                        ResolvedRecordField::Absent(writer_schema) => {
                            let _ignored_val = from_avro_datum(writer_schema, reader).await?;
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
            SchemaPiece::ResolveEnum { doc: _, symbols } => {
                if let Value::Int(index) = decode_int(reader).await? {
                    if index >= 0 && (index as usize) <= symbols.len() {
                        match symbols[index as usize].clone() {
                            Some(symbol) => Ok(Value::Enum(index, symbol)), // Todo (brennan) -- should this actually be the index in reader? Does it matter?
                            None => Err(DecodeError::new(format!(
                                "Enum symbol at index {} in writer schema not found in reader",
                                index
                            ))
                            .into()),
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
    .boxed()
}
