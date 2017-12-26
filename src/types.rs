use std::collections::HashMap;
use std::rc::Rc;

use serde_json::Value as JsonValue;

use schema::{RecordSchema, Schema};

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
    String(String),
    Fixed(usize, Vec<u8>),
    Union(Option<Box<Value>>),
    Array(Vec<Value>),
    Map(HashMap<String, Value>),
    Record(HashMap<String, Value>),
}

pub trait ToAvro {
    fn avro(self) -> Value;
}

macro_rules! to_avro(
    ($t:ty, $v:expr) => (
        impl ToAvro for $t {
            fn avro(self) -> Value {
                $v(self)
            }
        }
    );
);

to_avro!(bool, Value::Boolean);
to_avro!(i32, Value::Int);
to_avro!(i64, Value::Long);
to_avro!(f32, Value::Float);
to_avro!(f64, Value::Double);
to_avro!(String, Value::String);

impl ToAvro for () {
    fn avro(self) -> Value {
        Value::Null
    }
}

impl<'a> ToAvro for &'a str {
    fn avro(self) -> Value {
        Value::String(self.to_owned())
    }
}

impl<'a> ToAvro for &'a [u8] {
    fn avro(self) -> Value {
        Value::Bytes(self.to_owned())
    }
}

impl<T> ToAvro for Option<T> where T: ToAvro {
    fn avro(self) -> Value {
        Value::Union(self.map(|v| Box::new(v.avro())))
    }
}

impl<T> ToAvro for HashMap<String, T> where T: ToAvro {
    fn avro(self) -> Value {
        Value::Map(self
            .into_iter()
            .map(|(key, value)| (key, value.avro()))
            .collect::<_>())
    }
}

impl<'a, T> ToAvro for HashMap<&'a str, T> where T: ToAvro {
    fn avro(self) -> Value {
        Value::Map(self
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.avro()))
            .collect::<_>())
    }
}

impl ToAvro for Value {
    fn avro(self) -> Value {
        self
    }
}

impl<T> ToAvro for Box<T> where T: ToAvro {
    fn avro(self) -> Value {
        (*self).avro()
    }
}

#[derive(Debug)]
pub struct Record {
    schema: Rc<RecordSchema>,
    fields: HashMap<String, Value>,
}

impl Record {
    pub fn new(schema: &Schema) -> Option<Record> {
        match schema {
            &Schema::Record(ref record_schema) => {
                Some(Record {
                    schema: record_schema.clone(),
                    fields: HashMap::new(),
                })
            },
            _ => None,
        }
    }

    pub fn put<V>(&mut self, field: &str, value: V) where V: ToAvro {
        if let Some(_) = self.schema.lookup.get(field) {
            // if let Some(value) = value.avro().with_schema(&self.schema.fields[index].schema.clone()) {
            self.fields.insert(field.to_owned(), value.avro());
            // }
        }
    }
}

impl ToAvro for Record {
    fn avro(self) -> Value {
        Value::Record(self.fields)
    }
}

impl ToAvro for JsonValue {
    fn avro(self) -> Value {
        match self {
            JsonValue::Null => Value::Null,
            JsonValue::Bool(b) => Value::Boolean(b),
            JsonValue::Number(ref n) if n.is_i64() => Value::Long(n.as_i64().unwrap()),
            JsonValue::Number(ref n) if n.is_f64() => Value::Double(n.as_f64().unwrap()),
            JsonValue::Number(n) => Value::Long(n.as_u64().unwrap() as i64),  // TODO: Not so great
            JsonValue::String(s) => Value::String(s),
            JsonValue::Array(items) =>
                Value::Array(items.into_iter()
                    .map(|item| item.avro())
                    .collect::<_>()),
            JsonValue::Object(items) =>
                Value::Map(items.into_iter()
                    .map(|(key, value)| (key, value.avro()))
                    .collect::<_>()),
        }
    }
}
