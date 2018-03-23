use std::mem::transmute;

use types::{ToAvro, Value};
use util::{zig_i32, zig_i64};

pub fn encode(value: Value, buffer: &mut Vec<u8>) {
    match value {
        Value::Null => (),
        Value::Boolean(b) => buffer.push(if b { 1u8 } else { 0u8 }),
        Value::Int(i) => zig_i32(i, buffer),
        Value::Long(i) => zig_i64(i, buffer),
        Value::Float(x) => buffer.extend(unsafe { transmute::<f32, [u8; 4]>(x) }.to_vec()),
        Value::Double(x) => buffer.extend(unsafe { transmute::<f64, [u8; 8]>(x) }.to_vec()),
        Value::Bytes(bytes) => { encode(Value::Long(bytes.len() as i64), buffer); buffer.extend(bytes); },
        Value::String(s) => { encode(Value::Long(s.len() as i64), buffer); buffer.extend(s.into_bytes()); },
        Value::Fixed(_, bytes) => buffer.extend(bytes),
        Value::Enum(i) => encode(Value::Int(i), buffer),
        Value::Union(None) => buffer.push(0u8),
        Value::Union(Some(item)) => { buffer.push(1u8); encode(*item, buffer); },
        Value::Array(items) => {
            encode(items.len().avro(), buffer);
            for item in items.into_iter() {
                encode(item, buffer);
            }
            buffer.push(0u8);
        },
        Value::Map(items) => {
            encode(items.len().avro(), buffer);
            for (key, value) in items.into_iter() {
                encode(Value::String(key), buffer);
                encode(value, buffer);
            }
            buffer.push(0u8);
        },
        Value::Record(fields) => {
            for (_, value) in fields.into_iter() {
                encode(value, buffer);
            }
        },
    }
}

pub fn encode_raw(value: Value) -> Vec<u8> {
    let mut buffer = Vec::new();
    encode(value, &mut buffer);
    buffer
}