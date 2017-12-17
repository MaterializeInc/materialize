use std::collections::HashMap;
use std::collections::LinkedList;
use std::rc::Rc;

use schema::Schema;
use types::Value;

pub type Output = (Vec<u8>, Schema);

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
        (Vec::new(), Schema::Null)
    }
}

impl EncodeAvro for bool {
    fn encode(self) -> Output {
        (vec![if self { 1u8 } else { 0u8 }], Schema::Boolean)
    }
}

impl EncodeAvro for i32 {
    fn encode(self) -> Output {
        (encode_i32(self), Schema::Int)
    }
}

impl EncodeAvro for i64 {
    fn encode(self) -> Output {
        (encode_i64(self), Schema::Long)
    }
}

impl<'a> EncodeAvro for usize {
    fn encode(self) -> Output {
        (self as i64).encode()
    }
}

impl EncodeAvro for f32 {
    fn encode(self) -> Output {
        (format!("{:08X}", self.to_bits()).into_bytes(), Schema::Float)
    }
}

impl EncodeAvro for f64 {
    fn encode(self) -> Output {
        (format!("{:016X}", self.to_bits()).into_bytes(), Schema::Double)
        // TODO: something faster than format!?
    }
}

impl<'a> EncodeAvro for &'a str {
    fn encode(self) -> Output {
        // (self.len().encode().0 + self.into(), Schema::String)
        self.to_owned().encode()
    }
}

impl EncodeAvro for String {
    fn encode(self) -> Output {
        (stream!(self.len().encode().0, self.into_bytes()), Schema::String)
    }
}

impl<'a> EncodeAvro for &'a [u8] {
    fn encode(self) -> Output {
        (self.to_vec(), Schema::Bytes)  // TODO: better than to_vec?
    }
}

impl<T> EncodeAvro for Option<T> where T: EncodeAvro {
    fn encode(self) -> Output {
        match self {
            Some(item) => {
                let (stream, schema) = item.encode();
                (stream!(vec![1u8], stream), Schema::Union(Rc::new(schema)))
            },
            None => (vec![0u8], Schema::Union(Rc::new(Schema::Null)))
        }
    }
}

impl<T> EncodeAvro for Vec<T> where T: EncodeAvro {
    fn encode(self) -> Output {
        let mut schema = Schema::Null;
        let mut result = self.len().encode().0;

        for item in self.into_iter() {
            let (stream, s) = item.encode();

            result.extend(stream);
            schema = s;
        }

        result.push(0u8);

        (result, schema)
    }
}

impl<T> EncodeAvro for HashMap<String, T> where T: EncodeAvro {
    fn encode(self) -> Output {
        let mut schema = Schema::Null;
        let mut result = self.len().encode().0;

        for (key, value) in self.into_iter() {
            let (stream, s) = value.encode();

            result.extend(key.encode().0);
            result.extend(stream);
            schema = s;
        }

        (result, schema)
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
            Value::String(s) => s.encode(),
            Value::Fixed(bytes) => (&bytes).encode(),  // TODO: check it works
            Value::Union(option) => option.encode(),
            Value::Array(items) => items.encode(),
            Value::Map(items) => items.encode(),
            Value::Record(schema, mut items) => {
                let result = match &schema {
                    &Schema::Record { ref fields, .. } => {
                        fields
                            .into_iter()
                            .filter_map(|field| items.remove(&field.name))
                            .fold(Vec::new(), |mut acc, value| { acc.extend(value.encode().0); acc })
                    },
                    _ => Vec::new(),  // should not happen
                };

                (result, schema)
            }
        }
    }
}

fn encode_i32(z: i32) -> Vec<u8> {
    zigzag((((z << 1) ^ (z >> 31)) as u32) as u64)
}

fn encode_i64(z: i64) -> Vec<u8> {
    zigzag(((z << 1) ^ (z >> 63)) as u64)
}

fn zigzag(mut z: u64) -> Vec<u8> {
    let mut result = LinkedList::new();

    loop {
        if z <= 0x7F {
            result.push_front((z & 0x7F) as u8);
            break
        } else {
            result.push_front((0x80 | (z & 0x7F)) as u8);
            z >>= 7;
        }
    }

    result
        .into_iter()
        .collect::<Vec<_>>()
}
