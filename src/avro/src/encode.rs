// Copyright Materialize, Inc., Flavien Raynaud and other contributors.
//
// Use of this software is governed by the Apache License, Version 2.0

use std::convert::TryInto;
use std::mem::transmute;

use crate::schema::{Schema, SchemaNode, SchemaPiece};
use crate::types::{DecimalValue, Value};
use crate::util::{zig_i32, zig_i64};

/// Encode a `Value` into avro format.
///
/// **NOTE** This will not perform schema validation. The value is assumed to
/// be valid with regards to the schema. Schema are needed only to guide the
/// encoding for complex type values.
pub fn encode(value: &Value, schema: &Schema, buffer: &mut Vec<u8>) {
    encode_ref(&value, schema.top_node(), buffer)
}

fn encode_bytes<B: AsRef<[u8]> + ?Sized>(s: &B, buffer: &mut Vec<u8>) {
    let bytes = s.as_ref();
    encode(
        &Value::Long(bytes.len() as i64),
        &Schema {
            named: vec![],
            indices: Default::default(),
            top: SchemaPiece::Long.into(),
        },
        buffer,
    );
    buffer.extend_from_slice(bytes);
}

fn encode_long(i: i64, buffer: &mut Vec<u8>) {
    zig_i64(i, buffer)
}

fn encode_int(i: i32, buffer: &mut Vec<u8>) {
    zig_i32(i, buffer)
}

/// Encode a `Value` into avro format.
///
/// **NOTE** This will not perform schema validation. The value is assumed to
/// be valid with regards to the schema. Schema are needed only to guide the
/// encoding for complex type values.
pub fn encode_ref(value: &Value, schema: SchemaNode, buffer: &mut Vec<u8>) {
    match value {
        Value::Null => (),
        Value::Boolean(b) => buffer.push(if *b { 1u8 } else { 0u8 }),
        Value::Int(i) => encode_int(*i, buffer),
        Value::Long(i) => encode_long(*i, buffer),
        Value::Float(x) => buffer.extend_from_slice(&unsafe { transmute::<f32, [u8; 4]>(*x) }),
        Value::Date(d) => {
            let span = (*d) - chrono::NaiveDate::from_ymd(1970, 1, 1);
            encode_int(
                span.num_days()
                    .try_into()
                    .expect("Num days is too large to encode as i32"),
                buffer,
            )
        }
        Value::Timestamp(d) => {
            let mult = match schema.inner {
                SchemaPiece::TimestampMilli => 1_000,
                SchemaPiece::TimestampMicro => 1_000_000,
                other => panic!("Invalid schema for timestamp: {:?}", other),
            };
            let ts_seconds = d
                .timestamp()
                .checked_mul(mult)
                .expect("All chrono dates can be converted to timestamps");
            let sub_part: i64 = if mult == 1_000 {
                d.timestamp_subsec_millis().into()
            } else {
                d.timestamp_subsec_micros().into()
            };
            let ts = if ts_seconds >= 0 {
                ts_seconds + sub_part
            } else {
                ts_seconds - sub_part
            };
            encode_long(ts, buffer)
        }
        Value::Double(x) => buffer.extend_from_slice(&unsafe { transmute::<f64, [u8; 8]>(*x) }),
        Value::Decimal(DecimalValue { unscaled, .. })
        | Value::RDN(DecimalValue { unscaled, .. }) => match schema.name {
            None => encode_bytes(unscaled, buffer),
            Some(_) => buffer.extend(unscaled),
        },
        Value::Bytes(bytes) => encode_bytes(bytes, buffer),
        Value::String(s) => match schema.inner {
            SchemaPiece::String => {
                encode_bytes(s, buffer);
            }
            SchemaPiece::Enum { symbols, .. } => {
                if let Some(index) = symbols.iter().position(|item| item == s) {
                    encode_int(index as i32, buffer);
                }
            }
            _ => (),
        },
        Value::Fixed(_, bytes) => buffer.extend(bytes),
        Value::Enum(i, _) => encode_int(*i as i32, buffer),
        Value::Union { index, inner, .. } => {
            if let SchemaPiece::Union(schema_inner) = schema.inner {
                let schema_inner = &schema_inner.variants()[*index];
                encode_long(*index as i64, buffer);
                encode_ref(&*inner, schema.step(schema_inner), buffer);
            }
        }
        Value::Array(items) => {
            if let SchemaPiece::Array(inner) = schema.inner {
                if !items.is_empty() {
                    encode_long(items.len() as i64, buffer);
                    for item in items.iter() {
                        encode_ref(item, schema.step(&**inner), buffer);
                    }
                }
                buffer.push(0u8);
            }
        }
        Value::Map(items) => {
            if let SchemaPiece::Map(inner) = schema.inner {
                if !items.is_empty() {
                    encode_long(items.len() as i64, buffer);
                    for (key, value) in items {
                        encode_bytes(key, buffer);
                        encode_ref(value, schema.step(&**inner), buffer);
                    }
                }
                buffer.push(0u8);
            }
        }
        Value::Record(fields) => {
            if let SchemaPiece::Record {
                fields: inner_fields,
                ..
            } = schema.inner
            {
                for (i, &(_, ref value)) in fields.iter().enumerate() {
                    encode_ref(value, schema.step(&inner_fields[i].schema), buffer);
                }
            }
        }
        Value::Json(j) => {
            encode_bytes(&j.to_string(), buffer);
        }
        Value::Uuid(u) => {
            encode_bytes(&u.to_string(), buffer);
        }
    }
}

pub fn encode_to_vec(value: &Value, schema: &Schema) -> Vec<u8> {
    let mut buffer = Vec::new();
    encode(&value, schema, &mut buffer);
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_encode_empty_array() {
        let mut buf = Vec::new();
        let empty: Vec<Value> = Vec::new();
        encode(
            &Value::Array(empty),
            &r#"{"type": "array", "items": "int"}"#.parse().unwrap(),
            &mut buf,
        );
        assert_eq!(vec![0u8], buf);
    }

    #[test]
    fn test_encode_empty_map() {
        let mut buf = Vec::new();
        let empty: HashMap<String, Value> = HashMap::new();
        encode(
            &Value::Map(empty),
            &r#"{"type": "map", "values": "int"}"#.parse().unwrap(),
            &mut buf,
        );
        assert_eq!(vec![0u8], buf);
    }
}
