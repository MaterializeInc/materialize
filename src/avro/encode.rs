use std::convert::TryInto;
use std::mem::transmute;

use crate::schema::Schema;
use crate::types::Value;
use crate::util::{zig_i32, zig_i64};

/// Encode a `Value` into avro format.
///
/// **NOTE** This will not perform schema validation. The value is assumed to
/// be valid with regards to the schema. Schema are needed only to guide the
/// encoding for complex type values.
pub fn encode(value: &Value, schema: &Schema, buffer: &mut Vec<u8>) {
    encode_ref(&value, schema, buffer)
}

fn encode_bytes<B: AsRef<[u8]> + ?Sized>(s: &B, buffer: &mut Vec<u8>) {
    let bytes = s.as_ref();
    encode(&Value::Long(bytes.len() as i64), &Schema::Long, buffer);
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
pub fn encode_ref(value: &Value, schema: &Schema, buffer: &mut Vec<u8>) {
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
            let mult = match *schema {
                Schema::TimestampMilli => 1_000,
                Schema::TimestampMicro => 1_000_000,
                ref other => panic!("Invalid schema for timestamp: {:?}", other),
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
        Value::Decimal { unscaled, .. } => match *schema {
            Schema::Decimal {
                fixed_size: Some(_),
                ..
            } => buffer.extend(unscaled),
            Schema::Decimal {
                fixed_size: None, ..
            } => encode_bytes(unscaled, buffer),
            _ => (),
        },
        Value::Bytes(bytes) => encode_bytes(bytes, buffer),
        Value::String(s) => match *schema {
            Schema::String => {
                encode_bytes(s, buffer);
            }
            Schema::Enum { ref symbols, .. } => {
                if let Some(index) = symbols.iter().position(|item| item == s) {
                    encode_int(index as i32, buffer);
                }
            }
            _ => (),
        },
        Value::Fixed(_, bytes) => buffer.extend(bytes),
        Value::Enum(i, _) => encode_int(*i, buffer),
        Value::Union(item) => {
            if let Schema::Union(ref inner) = *schema {
                // Find the schema that is matched here. Due to validation, this should always
                // return a value.
                let (idx, inner_schema) = inner
                    .find_schema(item)
                    .expect("Invalid Union validation occurred");
                encode_long(idx as i64, buffer);
                encode_ref(&*item, inner_schema, buffer);
            }
        }
        Value::Array(items) => {
            if let Schema::Array(ref inner) = *schema {
                if !items.is_empty() {
                    encode_long(items.len() as i64, buffer);
                    for item in items.iter() {
                        encode_ref(item, inner, buffer);
                    }
                }
                buffer.push(0u8);
            }
        }
        Value::Map(items) => {
            if let Schema::Map(ref inner) = *schema {
                if !items.is_empty() {
                    encode_long(items.len() as i64, buffer);
                    for (key, value) in items {
                        encode_bytes(key, buffer);
                        encode_ref(value, inner, buffer);
                    }
                }
                buffer.push(0u8);
            }
        }
        Value::Record(fields) => {
            if let Schema::Record {
                fields: ref schema_fields,
                ..
            } = *schema
            {
                for (i, &(_, ref value)) in fields.iter().enumerate() {
                    encode_ref(value, &schema_fields[i].schema, buffer);
                }
            }
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
            &Schema::Array(Box::new(Schema::Int)),
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
            &Schema::Map(Box::new(Schema::Int)),
            &mut buf,
        );
        assert_eq!(vec![0u8], buf);
    }
}
