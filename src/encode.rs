use std::collections::HashMap;
use std::mem::transmute;

use types::Value;
use util::zigzag;

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
    fn encode(self) -> Output;
}

impl EncodeAvro for () {
    fn encode(self) -> Output {
        Vec::new()
    }
}

impl EncodeAvro for bool {
    fn encode(self) -> Output {
        vec![if self { 1u8 } else { 0u8 }]
    }
}

impl EncodeAvro for i32 {
    fn encode(self) -> Output {
        zigzag(((self << 1) ^ (self >> 31)) as i64)
    }
}

impl EncodeAvro for i64 {
    fn encode(self) -> Output {
        zigzag((self << 1) ^ (self >> 63))
    }
}

impl<'a> EncodeAvro for usize {
    fn encode(self) -> Output {
        (self as i64).encode()
    }
}

impl EncodeAvro for f32 {
    fn encode(self) -> Output {
        unsafe { transmute::<Self, [u8; 4]>(self) }.to_vec()
    }
}

impl EncodeAvro for f64 {
    fn encode(self) -> Output {
        unsafe { transmute::<Self, [u8; 8]>(self) }.to_vec()
    }
}

impl<'a> EncodeAvro for &'a str {
    fn encode(self) -> Output {
        self.to_owned().encode()
    }
}

impl EncodeAvro for String {
    fn encode(self) -> Output {
        stream!(self.len().encode(), self.into_bytes())
    }
}

impl<'a> EncodeAvro for &'a [u8] {
    fn encode(self) -> Output {
        stream!(self.len().encode(), self.to_vec())  // TODO: better than to_vec?
    }
}

impl<T> EncodeAvro for Option<T> where T: EncodeAvro {
    fn encode(self) -> Output {
        match self {
            Some(item) => stream!(vec![1u8], item.encode()),
            None => vec![0u8]
        }
    }
}

impl<T> EncodeAvro for Vec<T> where T: EncodeAvro {
    fn encode(self) -> Output {
        stream!(
            self.into_iter()
                .fold(Vec::new(), |acc, item| stream!(acc, item.encode())),
            vec![0u8]
        )
    }
}

impl<T> EncodeAvro for HashMap<String, T> where T: EncodeAvro {
    fn encode(self) -> Output {
        stream!(
            self.len().encode(),
            self.into_iter()
                .fold(Vec::new(), |acc, (key, value)|
                    stream!(acc, key.encode(), value.encode())
                ),
            vec![0u8]
        )
    }
}

impl<T> EncodeAvro for Box<T> where T: EncodeAvro {
    fn encode(self) -> Output {
        (*self).encode()
    }
}

impl EncodeAvro for Value {
    fn encode(self) -> Output {
        match self {
            Value::Null => ().encode(),
            Value::Boolean(b) => b.encode(),
            Value::Int(i) => i.encode(),
            Value::Long(i) => i.encode(),
            Value::Float(x) => x.encode(),
            Value::Double(x) => x.encode(),
            Value::Bytes(bytes) => (&bytes).encode(),
            Value::Fixed(_, bytes) => bytes,
            Value::String(s) => s.encode(),
            Value::Union(option) => option.encode(),
            Value::Array(items) => items.encode(),
            Value::Map(items) => items.encode(),
            Value::Record(record_schema, mut items) => {
                record_schema.fields
                    .iter()
                    .filter_map(|ref field| items.remove(&field.name))
                    .fold(Vec::new(), |acc, value| stream!(acc, value.encode()))
            }
        }
    }
}
