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
    Record(Rc<RecordSchema>, HashMap<String, Value>),
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
        if let Some(_) = self.schema.fields_lookup.get(field) {
            // if let Some(value) = value.avro().with_schema(&self.schema.fields[index].schema.clone()) {
            self.fields.insert(field.to_owned(), value.avro());
            // }
        }
    }
}

impl ToAvro for Record {
    fn avro(self) -> Value {
        Value::Record(self.schema.clone(), self.fields)
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

impl Value {
    pub fn with_schema(self, schema: &Schema) -> Option<Value> {
        match schema {
            &Schema::Null => self.with_null(),
            &Schema::Boolean => self.with_boolean(),
            &Schema::Int => self.with_int(),
            &Schema::Long => self.with_long(),
            &Schema::Float => self.with_float(),
            &Schema::Double => self.with_double(),
            &Schema::Bytes => self.with_bytes(),
            &Schema::String => self.with_string(),
            &Schema::Fixed { size, .. } => self.with_fixed(size),
            &Schema::Array(ref inner) => self.with_array(inner.clone()),
            &Schema::Map(ref inner) => self.with_map(inner.clone()),
            &Schema::Union(ref inner) => self.with_union(inner.clone()),
            &Schema::Record(ref record_schema) => self.with_record(record_schema.clone()),
            _ => None,
        }
    }

    fn with_null(self) -> Option<Value> {
        match self {
            Value::Null => Some(Value::Null),
            _ => None,
        }
    }

    fn with_boolean(self) -> Option<Value> {
        match self {
            Value::Boolean(b) => Some(Value::Boolean(b)),
            _ => None,
        }
    }

    fn with_int(self) -> Option<Value> {
        match self {
            Value::Int(i) => Some(Value::Int(i)),
            _ => None,
        }
    }

    fn with_long(self) -> Option<Value> {
        match self {
            Value::Int(i) => Some(Value::Long(i as i64)),
            Value::Long(i) => Some(Value::Long(i)),
            _ => None,
        }
    }

    fn with_float(self) -> Option<Value> {
        match self {
            Value::Int(i) => Some(Value::Float(i as f32)),
            Value::Long(i) => Some(Value::Float(i as f32)),
            Value::Float(x) => Some(Value::Float(x)),
            _ => None,
        }
    }

    fn with_double(self) -> Option<Value> {
        match self {
            Value::Int(i) => Some(Value::Double(i as f64)),
            Value::Long(i) => Some(Value::Double(i as f64)),
            Value::Float(x) => Some(Value::Double(x as f64)),
            Value::Double(x) => Some(Value::Double(x)),
            _ => None,
        }
    }

    fn with_bytes(self) -> Option<Value> {
        match self {
            Value::Bytes(bytes) => Some(Value::Bytes(bytes)),
            Value::String(s) => Some(Value::Bytes(s.into_bytes())),
            _ => None,
        }
    }

    fn with_string(self) -> Option<Value> {
        match self {
            Value::String(s) => Some(Value::String(s)),
            Value::Bytes(bytes) => String::from_utf8(bytes).ok().map(Value::String),
            _ => None,
        }
    }

    fn with_fixed(self, size: i32) -> Option<Value> {
        match self {
            Value::Fixed(s, bytes) => {
                if s == (size as usize) {
                    Some(Value::Fixed(s, bytes))
                } else {
                    None
                }
            },
            Value::Bytes(bytes) => {
                if bytes.len() == (size as usize) {
                    Some(Value::Fixed(bytes.len(), bytes))
                } else {
                    None
                }
            },
            Value::String(s) => {
                if s.len() == (size as usize) {
                    Some(Value::Fixed(s.len(), s.into_bytes()))
                } else {
                    None
                }
            },
            _ => None,
        }
    }

    fn with_array(self, inner: Rc<Schema>) -> Option<Value> {
        match self {
            Value::Array(items) => {
                items
                    .into_iter()
                    .map(|item| item.with_schema(&inner.clone()))
                    .collect::<Option<_>>()
                    .map(Value::Array)
            },
            _ => None,
        }
    }

    fn with_map(self, inner: Rc<Schema>) -> Option<Value> {
        match self {
            Value::Map(items) => {
                items
                    .into_iter()
                    .map(|(key, value)|
                        value
                            .with_schema(&inner.clone())
                            .map(|v| (key, v)))
                    .collect::<Option<_>>()
                    .map(Value::Map)
            },
            _ => None,
        }
    }

    fn with_union(self, inner: Rc<Schema>) -> Option<Value> {
        match self {
            Value::Union(None) => Some(Value::Union(None)),
            Value::Union(Some(value)) =>
                value
                    .with_schema(&inner.clone())
                    .map(|v| Value::Union(Some(Box::new(v)))),
            Value::Null => Some(Value::Union(None)),
            value => value
                .with_schema(&inner.clone())
                .map(|v| Value::Union(Some(Box::new(v)))),
        }
    }

    fn with_record(self, record_schema: Rc<RecordSchema>) -> Option<Value> {
        match self {
            Value::Record(rc, items) => {
                items
                    .into_iter()
                    .map(|(key, value)| {
                        record_schema.fields_lookup.get(&key)
                            .and_then(|&index| record_schema.fields.get(index))
                            .map(|field| field.schema.clone())  // TODO: field Rc<Schema>
                            .and_then(|schema| value.with_schema(&schema))
                            .map(|value| (key, value))
                        // TODO: record_schema.field not in items
                        // TODO: if items has extra fields, filter them
                    })
                    .collect::<Option<_>>()
                    .map(|items| Value::Record(rc, items))
            },
            _ => None,
        }
    }
}
