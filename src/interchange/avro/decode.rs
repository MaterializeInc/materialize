use std::io::Read;
use std::mem::transmute;

use chrono::{NaiveDate, NaiveDateTime};
use failure::{Error, Fail, bail};

use repr::Datum;
use repr::decimal::Significand;
use avro_rs::schema::Schema;

/// Describes errors happened trying to allocate too many bytes
#[derive(Fail, Debug)]
#[fail(display = "Allocation error: {}", _0)]
pub struct AllocationError(String);

impl AllocationError {
    pub fn new<S>(msg: S) -> AllocationError
    where
        S: Into<String>,
    {
        AllocationError(msg.into())
    }
}

const MAX_ALLOCATION_BYTES: usize = 512 * 1024 * 1024;

/// Describes errors happened while decoding Avro data.
#[derive(Fail, Debug)]
#[fail(display = "Decoding error: {}", _0)]
pub struct DecodeError(String);

impl DecodeError {
    pub fn new<S>(msg: S) -> DecodeError
    where
        S: Into<String>,
    {
        DecodeError(msg.into())
    }
}

fn decode_variable<R: Read>(reader: &mut R) -> Result<u64, Error> {
    let mut i = 0u64;
    let mut buf = [0u8; 1];

    let mut j = 0;
    loop {
        if j > 9 {
            // if j * 7 > 64
            return Err(DecodeError::new("Overflow when decoding integer value").into());
        }
        reader.read_exact(&mut buf[..])?;
        i |= (u64::from(buf[0] & 0x7F)) << (j * 7);
        if (buf[0] >> 7) == 0 {
            break;
        } else {
            j += 1;
        }
    }

    Ok(i)
}

pub fn zag_i32<R: Read>(reader: &mut R) -> Result<i32, Error> {
    let i = zag_i64(reader)?;
    if i < i64::from(i32::min_value()) || i > i64::from(i32::max_value()) {
        Err(DecodeError::new("int out of range").into())
    } else {
        Ok(i as i32)
    }
}

pub fn zag_i64<R: Read>(reader: &mut R) -> Result<i64, Error> {
    let z = decode_variable(reader)?;
    Ok(if z & 0x1 == 0 {
        (z >> 1) as i64
    } else {
        !(z >> 1) as i64
    })
}

#[inline]
fn decode_long<R: Read>(reader: &mut R) -> Result<Datum, Error> {
    zag_i64(reader).map(Datum::Int64)
}

#[inline]
fn decode_int<R: Read>(reader: &mut R) -> Result<Datum, Error> {
    zag_i32(reader).map(Datum::Int32)
}

#[inline]
fn decode_len<R: Read>(reader: &mut R) -> Result<usize, Error> {
    zag_i64(reader).and_then(|len| safe_len(len as usize))
}

pub fn safe_len(len: usize) -> Result<usize, Error> {
    if len <= MAX_ALLOCATION_BYTES {
        Ok(len)
    } else {
        Err(AllocationError::new(format!(
            "Unable to allocate {} bytes (Maximum allowed: {})",
            len, MAX_ALLOCATION_BYTES
        ))
        .into())
    }
}

/// Decode a `Datum` from avro format given its `Schema`.
pub fn decode<R: Read>(schema: &Schema, reader: &mut R) -> Result<Option<Vec<Datum>>, Error> {
    match schema {
        Schema::Union(ref inner) => {
            let index = zag_i64(reader)?;
            let variants = inner.variants();
            match variants.get(index as usize) {
                Some(variant) => match variant {
                    Schema::Record { fields, .. } => {
                        let mut out = Vec::new();
                        for field in fields {
                            out.push(decode_datum(&field.schema, reader)?);
                        }
                        Ok(Some(out))
                    }
                    Schema::Null => Ok(None),
                    _ => bail!("unsupported avro schema {:?}", schema),
                }
                None => Err(DecodeError::new("Union index out of bounds").into()),
            }
        }
        _ => bail!("unsupported avro schema {:?}", schema),
    }
}

pub fn decode_datum<R: Read>(schema: &Schema, reader: &mut R) -> Result<Datum, Error> {
    match schema {
        Schema::Null => Ok(Datum::Null),
        Schema::Boolean => {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf[..])?;

            match buf[0] {
                0u8 => Ok(Datum::False),
                1u8 => Ok(Datum::True),
                _ => Err(DecodeError::new("not a bool").into()),
            }
        }
        Schema::Int => decode_int(reader),
        Schema::Long => decode_long(reader),
        Schema::Float => {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf[..])?;
            Ok(Datum::Float32(unsafe { transmute::<[u8; 4], f32>(buf) }.into()))
        }
        Schema::Double => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf[..])?;
            Ok(Datum::Float64(unsafe { transmute::<[u8; 8], f64>(buf) }.into()))
        }
        Schema::Date => match decode_int(reader)? {
            Datum::Int32(days) => Ok(Datum::Date(
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
        Schema::TimestampMilli => match decode_long(reader)? {
            Datum::Int64(millis) => {
                let seconds = millis / 1_000;
                let millis = (millis % 1_000) as u32;
                Ok(Datum::Timestamp(
                    NaiveDateTime::from_timestamp_opt(seconds, millis * 1_000_000).ok_or_else(
                        || DecodeError::new(format!("Invalid ms timestamp {}.{}", seconds, millis)),
                    )?,
                ))
            }
            other => Err(DecodeError::new(format!(
                "Not an Int64 input for Millisecond DateTime: {:?}",
                other
            ))
            .into()),
        },
        Schema::TimestampMicro => match decode_long(reader)? {
            Datum::Int64(micros) => {
                let seconds = micros / 1_000_000;
                let micros = (micros % 1_000_000) as u32;
                Ok(Datum::Timestamp(
                    NaiveDateTime::from_timestamp_opt(seconds, micros * 1_000).ok_or_else(
                        || DecodeError::new(format!("Invalid mu timestamp {}.{}", seconds, micros)),
                    )?,
                ))
            }
            other => Err(DecodeError::new(format!(
                "Not an Int64 input for Microsecond DateTime: {:?}",
                other
            ))
            .into()),
        },
        Schema::Decimal {
            fixed_size,
            ..
        } => {
            let len = match fixed_size {
                Some(len) => *len,
                None => decode_len(reader)?,
            };
            let mut buf = Vec::with_capacity(len);
            unsafe {
                buf.set_len(len);
            }
            reader.read_exact(&mut buf)?;
            Ok(Datum::Decimal(Significand::from_twos_complement_be(&buf)?))
        }
        Schema::Bytes => {
            let len = decode_len(reader)?;
            let mut buf = Vec::with_capacity(len);
            unsafe {
                buf.set_len(len);
            }
            reader.read_exact(&mut buf)?;
            Ok(Datum::Bytes(buf))
        }
        Schema::String => {
            let len = decode_len(reader)?;
            let mut buf = Vec::with_capacity(len);
            unsafe {
                buf.set_len(len);
            }
            reader.read_exact(&mut buf)?;

            String::from_utf8(buf)
                .map(Datum::String)
                .map_err(|_| DecodeError::new("not a valid utf-8 string").into())
        }
        other @ Schema::Fixed { .. }
        | other @ Schema::Array(..)
        | other @ Schema::Map(..)
        | other @ Schema::Record { .. }
        | other @ Schema::Enum { .. }
        | other @ Schema::Union(_)
        => {
            bail!("unsupported avro type: {:?}", other)
        }
    }
}
