use std::collections::HashMap;
use std::mem::transmute;

use schema::Schema;
use types::{ToAvro, Value};
use util::{zigzag_i32, zigzag_i64};

pub type Output = Vec<u8>;

macro_rules! stream {
    ($($x:expr),+) => ({
        let mut result = Vec::new();
        $(
            result.extend($x);
        )+
        result
    });
}

pub trait EncodeAvro {
    fn encode(self, schema: &Schema) -> Option<Output>;
}

impl EncodeAvro for () {
    fn encode(self, schema: &Schema) -> Option<Output> {
        match schema {
            &Schema::Null => Some(Vec::new()),
            _ => None,
        }
    }
}

impl EncodeAvro for bool {
    fn encode(self, schema: &Schema) -> Option<Output> {
        match schema {
            &Schema::Boolean => Some(vec![if self { 1u8 } else { 0u8 }]),
            _ => None,
        }
    }
}

impl EncodeAvro for i32 {
    fn encode(self, schema: &Schema) -> Option<Output> {
        match schema {
            &Schema::Int => Some(zigzag_i32(self)),
            &Schema::Long => (self as i64).encode(schema),
            &Schema::Float => (self as f32).encode(schema),
            &Schema::Double => (self as f64).encode(schema),
            _ => None,
        }
    }
}

impl EncodeAvro for i64 {
    fn encode(self, schema: &Schema) -> Option<Output> {
        match schema {
            &Schema::Long => Some(zigzag_i64(self)),
            &Schema::Float => (self as f32).encode(schema),
            &Schema::Double => (self as f64).encode(schema),
            _ => None,
        }
    }
}

impl<'a> EncodeAvro for usize {
    fn encode(self, schema: &Schema) -> Option<Output> {
        (self as i64).encode(schema)
    }
}

impl EncodeAvro for f32 {
    fn encode(self, schema: &Schema) -> Option<Output> {
        match schema {
            &Schema::Float => Some(
                unsafe { transmute::< Self, [u8; 4]> ( self ) }.to_vec()),
            &Schema::Double => (self as f64).encode(&schema),
            _ => None,
    }
    }
}

impl EncodeAvro for f64 {
    fn encode(self, schema: &Schema) -> Option<Output> {
        match schema {
            &Schema::Double => Some(
                unsafe { transmute::<Self, [u8; 8]>(self) }.to_vec()),
            _ => None,
        }
    }
}

impl<'a> EncodeAvro for &'a str {
    fn encode(self, schema: &Schema) -> Option<Output> {
        self.to_owned().encode(schema)
    }
}

impl EncodeAvro for String {
    fn encode(self, schema: &Schema) -> Option<Output> {
        match schema {
            &Schema::String => Some(
                stream!(self.len().encode(&Schema::Long).unwrap(), self.into_bytes())),
            &Schema::Bytes | &Schema::Fixed { .. } => (self.as_bytes()).encode(schema),
            _ => None,
        }
    }
}

impl<'a> EncodeAvro for &'a [u8] {
    fn encode(self, schema: &Schema) -> Option<Output> {
        match schema {
            &Schema::String => String::from_utf8(self.to_owned())
                .ok().and_then(|s| s.encode(schema)),
            &Schema::Bytes => Some(stream!(
                self.len().encode(&Schema::Long).unwrap(), self.to_owned())),
            &Schema::Fixed { size, .. } if size == self.len() => Some(self.to_owned()),
            _ => None,
        }
    }
}

impl<T> EncodeAvro for Option<T> where T: EncodeAvro {
    fn encode(self, schema: &Schema) -> Option<Output> {
        match schema {
            &Schema::Union(ref inner) => match self {
                Some(item) => item
                    .encode(inner)
                    .map(|stream| stream!(vec![1u8], stream)),
                None => Some(vec![0u8]),
            },
            _ => None,
        }
    }
}

impl<T> EncodeAvro for Vec<T> where T: EncodeAvro {
    fn encode(self, schema: &Schema) -> Option<Output> {
        match schema {
            &Schema::Array(ref inner) =>
                self.into_iter()
                    .map(|item| item.encode(inner))
                    .collect::<Option<Vec<_>>>()
                    .map(|items| stream!(
                        items.into_iter()
                            .fold(Vec::new(), |acc, stream| stream!(acc, stream)),
                        vec![0u8])),
            _ => None,
        }
    }
}

impl<T> EncodeAvro for HashMap<String, T> where T: EncodeAvro {
    fn encode(self, schema: &Schema) -> Option<Output> {
        match schema {
            &Schema::Map(ref inner) =>
                self.into_iter()
                    .map(|(key, value)| value.encode(inner)
                        .map(|v| (key.encode(&Schema::String).unwrap(), v)))
                    .collect::<Option<HashMap<_, _>>>()
                    .map(|items| stream!(
                        items.len().encode(&Schema::Long).unwrap(),
                        items.into_iter()
                            .fold(Vec::new(), |acc, (key, value)| stream!(acc, key, value)),
                        vec![0u8])),
            _ => None,
        }
    }
}

impl<T> EncodeAvro for Box<T> where T: EncodeAvro {
    fn encode(self, schema: &Schema) -> Option<Output> {
        (*self).encode(schema)
    }
}

impl EncodeAvro for Value {
    fn encode(self, schema: &Schema) -> Option<Output> {
        match self {
            Value::Null => ().encode(schema),
            Value::Boolean(b) => b.encode(schema),
            Value::Int(i) => i.encode(schema),
            Value::Long(i) => i.encode(schema),
            Value::Float(x) => x.encode(schema),
            Value::Double(x) => x.encode(schema),
            Value::Bytes(bytes) | Value::Fixed(_, bytes) => (&bytes).encode(schema),
            Value::String(s) => s.encode(schema),
            Value::Union(option) => option.encode(schema),
            Value::Array(items) => items.encode(schema),
            Value::Map(items) => items.encode(schema),
            Value::Record(mut items) => {
                match schema {
                    &Schema::Record(ref record_schema) =>
                        record_schema.fields
                            .iter()
                            .map(|ref field| items.remove(&field.name)
                                .or_else(|| field.default.clone().map(|d| d.avro()))
                                .and_then(|item| item.encode(&field.schema)))
                            .collect::<Option<Vec<_>>>()
                            .map(|items| items.into_iter()
                                .fold(Vec::new(), |acc, stream| stream!(acc, stream))),
                    _ => None,
                }
            }
        }
    }
}
