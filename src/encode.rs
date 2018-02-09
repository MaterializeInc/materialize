use std::mem::transmute;

use types::{ToAvro, Value};
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

pub fn encode(value: Value) -> Vec<u8> {
    match value {
        Value::Null => Vec::new(),
        Value::Boolean(b) => vec![if b { 1u8 } else { 0u8 }],
        Value::Int(i) => zig_i32(i),
        Value::Long(i) => zig_i64(i),
        Value::Float(x) => unsafe { transmute::<f32, [u8; 4]>(x) }.to_vec(),
        Value::Double(x) => unsafe { transmute::<f64, [u8; 8]>(x) }.to_vec(),
        Value::Bytes(bytes) => stream!(encode(Value::Long(bytes.len() as i64)), bytes),
        Value::String(s) => stream!(encode(Value::Long(s.len() as i64)), s.into_bytes()),
        Value::Fixed(_, bytes) => bytes,
        Value::Union(None) => vec![0u8],
        Value::Union(Some(item)) => stream!(vec![1u8], encode(*item)),
        Value::Array(items) => {
            stream!(
                encode(items.len().avro()),
                items.into_iter()
                    .map(|item| encode(item))
                    .fold(Vec::new(), |acc, stream| stream!(acc, stream)),
                vec![0u8])
        },
        Value::Map(items) => {
            stream!(
                encode(items.len().avro()),
                items.into_iter()
                    .map(|(key, value)| stream!(encode(Value::String(key)), encode(value)))
                    .fold(Vec::new(), |acc, stream| stream!(acc, stream)),
                vec![0u8])
        },
        Value::Record(fields) => {
            fields.into_iter()
                .map(|(_, value)| encode(value))
                .fold(Vec::new(), |acc, stream| stream!(acc, stream))
        },
    }
}
