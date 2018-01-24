use std::mem::transmute;

use types::{ToAvro, Value};
use schema::Schema;
use util::{zig_i32, zig_i64};

macro_rules! stream {
    ($($x:expr),+) => ({
        let mut result = Vec::new();
        $(
            result.extend($x);
        )+
        result
    });
}

pub fn encode(value: Value, schema: Option<&Schema>) -> Vec<u8> {
    match value {
        Value::Null => Vec::new(),
        Value::Boolean(b) => vec![if b { 1u8 } else { 0u8 }],
        Value::Int(i) => zig_i32(i),
        Value::Long(i) => zig_i64(i),
        Value::Float(x) => unsafe { transmute::<f32, [u8; 4]>(x) }.to_vec(),
        Value::Double(x) => unsafe { transmute::<f64, [u8; 8]>(x) }.to_vec(),
        Value::Bytes(bytes) => stream!(encode(Value::Long(bytes.len() as i64), None), bytes),
        Value::String(s) => stream!(encode(Value::Long(s.len() as i64), None), s.into_bytes()),
        Value::Fixed(_, bytes) => bytes,
        Value::Union(None) => vec![0u8],
        Value::Union(Some(item)) => {
            if let Some(&Schema::Union(ref inner)) = schema {
                stream!(vec![1u8], encode(*item, Some(inner)))
            } else {  // Should not happen
                panic!("Eaten by a grue!")
            }
        }
        Value::Array(items) => {
            if let Some(&Schema::Array(ref inner)) = schema {
                stream!(
                    encode(items.len().avro(), None),
                    items.into_iter()
                        .map(|item| encode(item, Some(inner)))
                        .fold(Vec::new(), |acc, stream| stream!(acc, stream)),
                    vec![0u8])
            } else {  // Should not happen
                panic!("Eaten by a grue!")
            }
        },
        Value::Map(items) => {
            if let Some(&Schema::Map(ref inner)) = schema {
                stream!(
                    encode(items.len().avro(), None),
                    items.into_iter()
                        .map(|(key, value)| stream!(encode(Value::String(key), None), encode(value, Some(inner))))
                        .fold(Vec::new(), |acc, stream| stream!(acc, stream)),
                    vec![0u8])
            } else {  // Should not happen
                panic!("Eaten by a grue!")
            }
        },
        Value::Record(mut fields) => {
            if let Some(&Schema::Record(ref rschema)) = schema {
                rschema.fields.iter()
                    // The value is considered valid (should have been created using `.with_schema`)
                    .map(|field| (field, fields.remove(&field.name).unwrap()))
                    .map(|(field, value)| encode(value, Some(&field.schema)))
                    .fold(Vec::new(), |acc, stream| stream!(acc, stream))
            } else {  // Should not happen
                panic!("Eaten by a grue!")
            }
        },
    }
}
