use std::collections::HashMap;
use std::io::Read;
use std::mem::transmute;

use failure::{Error, err_msg};

use types::Value;
use schema::Schema;
use util::{zag_i32, zag_i64};

pub fn decode<R: Read>(schema: &Schema, reader: &mut R) -> Result<Value, Error> {
    match schema {
        &Schema::Null => Ok(Value::Null),
        &Schema::Boolean => {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf[..])?;

            match buf {
                [0u8] => Ok(Value::Boolean(false)),
                [1u8] => Ok(Value::Boolean(true)),
                _ => Err(err_msg("not a bool")),
            }
        },
        &Schema::Int => zag_i32(reader).map(Value::Int),
        &Schema::Long => zag_i64(reader).map(Value::Long),
        &Schema::Float => {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf[..])?;
            Ok(Value::Float(unsafe { transmute::<[u8; 4], f32>(buf) }))
        },
        &Schema::Double => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf[..])?;
            Ok(Value::Double(unsafe { transmute::<[u8; 8], f64>(buf) }))
        }
        &Schema::Bytes => {
            if let Value::Long(len) = decode(&Schema::Long, reader)? {
                let mut buf = vec![0u8; len as usize];
                reader.read_exact(&mut buf)?;
                Ok(Value::Bytes(buf))
            } else {
                Err(err_msg("bytes len not found"))
            }
        },
        &Schema::String => {
            if let Value::Long(len) = decode(&Schema::Long, reader)? {
                // let mut buf = String::with_capacity(len as usize);
                // reader.read_exact(&mut buf.as_bytes_mut())?;
                let mut buf = vec![0u8; len as usize];
                reader.read_exact(&mut buf)?;

                String::from_utf8(buf)
                    .map_err(|_| err_msg("not a valid utf-8 string"))
                    .map(Value::String)
            } else {
                Err(err_msg("string len not found"))
            }
        },
        &Schema::Fixed { size, .. } => {
            let mut buf = vec![0u8; size as usize];
            reader.read_exact(&mut buf)?;
            Ok(Value::Fixed(size, buf))
        },
        &Schema::Array(ref inner) => {
            let mut items = Vec::new();

            loop {
                if let Value::Long(len) = decode(&Schema::Long, reader)? {
                    // arrays are 0-terminated, 0i64 is also encoded as 0 in Avro
                    // reading a length of 0 means the end of the array
                    if len == 0 {
                        break
                    }

                    items.reserve(len as usize);
                    for _ in 0..len {
                        items.push(decode(inner, reader)?);
                    }
                } else {
                    return Err(err_msg("array len not found"));
                }
            }

            Ok(Value::Array(items))
        },
        &Schema::Map(ref inner) => {
            let mut items = HashMap::new();

            loop {
                if let Value::Long(len) = decode(&Schema::Long, reader)? {
                    // maps are 0-terminated, 0i64 is also encoded as 0 in Avro
                    // reading a length of 0 means the end of the map
                    if len == 0 {
                        break
                    }

                    items.reserve(len as usize);
                    for _ in 0..len {
                        if let Value::String(key) = decode(&Schema::String, reader)? {
                            let value = decode(inner, reader)?;
                            items.insert(key, value);
                        } else {
                            return Err(err_msg("map key is not a string"))
                        }
                    }
                } else {
                    return Err(err_msg("map len not found"));
                }
            }

            Ok(Value::Map(items))
        },
        &Schema::Union(ref inner) => {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf)?;

            match buf {
                [0u8] => Ok(Value::Union(None)),
                [1u8] => decode(inner, reader),
                _ => Err(err_msg("union index out of bounds")),
            }
        },
        &Schema::Record(ref record_schema) => {
            record_schema.fields
                .iter()
                .map(|field| {
                    decode(&field.schema, reader)
                        .map(|value| (field.name.clone(), value))
                })
                .collect::<Result<HashMap<String, Value>, _>>()
                .map(Value::Record)
        },
        &Schema::Enum { ref symbols, .. } => {
            if let Value::Int(index) = decode(&Schema::Int, reader)? {
                if index >= 0 && (index as usize) <= symbols.len() {
                    Ok(Value::Int(index))  // TODO: Value::Enum
                } else {
                    Err(err_msg("enum symbol index out of bounds"))
                }
            } else {
                Err(err_msg("enum symbol not found"))
            }
        },
    }
}