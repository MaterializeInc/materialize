use std::collections::HashMap;
use std::rc::Rc;

use serde::{Serialize, Serializer};
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

impl ToAvro for usize {
    fn avro(self) -> Value {
        (self as i64).avro()
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
    pub rschema: Rc<RecordSchema>,
    pub fields: HashMap<String, Value>,
    // lookup: HashMap<String, usize>,
}

impl Record {
    pub fn new(schema: &Schema) -> Option<Record> {
        match schema {
            &Schema::Record(ref rschema) => {
                Some(Record {
                    rschema: rschema.clone(),
                    // lookup: rschema.lookup(),
                    fields: HashMap::new(),
                })
            },
            _ => None,
        }
    }

    /*
    pub fn from_value(value: &Value) -> Option<Record> {
        match value {
            &Value::Record(ref fields) => {
                Some(Record {
                    rschema: rschema.clone(),
                    // lookup: rschema.lookup(),
                    fields: fields.clone(),
                })
            },
            _ => None,
        }
    }
    */

    pub fn put<V>(&mut self, field: &str, value: V) where V: ToAvro {
        let lookup = self.rschema.lookup();  // TODO
        if lookup.get(field).is_none() {
            return
        }

        self.fields.insert(field.to_owned(), value.avro());
    }

    pub fn schema(&self) -> Schema {
        Schema::Record(self.rschema.clone())
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
            &Schema::Array(ref inner) => self.with_array(inner),
            &Schema::Map(ref inner) => self.with_map(inner),
            &Schema::Union(ref inner) => self.with_union(inner),
            &Schema::Record(ref rschema) => self.with_record(rschema.clone()),
            &Schema::Enum { ref symbols, .. } => self.with_enum(symbols),
            &Schema::Fixed { ref size, .. } => self.with_fixed(*size),
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
            Value::Bytes(bytes)
            | Value::Fixed(_, bytes) => Some(Value::Bytes(bytes)),
            Value::String(s) => Some(Value::Bytes(s.into_bytes())),
            _ => None,
        }
    }

    fn with_string(self) -> Option<Value> {
        match self {
            Value::String(s) => Some(Value::String(s)),
            Value::Bytes(bytes)
            | Value::Fixed(_, bytes) => String::from_utf8(bytes).ok().map(Value::String),
            _ => None,
        }
    }

    fn with_array(self, schema: &Schema) -> Option<Value> {
        match self {
            Value::Array(items) =>
                items.into_iter()
                    .map(|item| item.with_schema(schema))
                    .collect::<Option<_>>()
                    .map(Value::Array),
            _ => None,
        }
    }

    fn with_map(self, schema: &Schema) -> Option<Value> {
        match self {
            Value::Map(items) =>
                items.into_iter()
                    .map(|(key, value)| value.with_schema(schema).map(|v| (key, v)))
                    .collect::<Option<_>>()
                    .map(Value::Map),
            _ => None,
        }
    }

    fn with_union(self, schema: &Schema) -> Option<Value> {
        match self {
            Value::Union(None) => Some(Value::Union(None)),
            Value::Union(Some(inner)) => (*inner).with_schema(schema).map(|i| Value::Union(Some(Box::new(i)))),
            value => value.with_schema(schema),
        }
    }

    fn with_enum(self, symbols: &Vec<String>) -> Option<Value> {
        // TODO Value::Enum
        match self {
            Value::Int(i) if i >= 0 && i < symbols.len() as i32 => Some(Value::Int(i)),
            _ => None,
        }
    }

    fn with_fixed(self, size: usize) -> Option<Value> {
        match self {
            Value::Fixed(s, bytes) => {
                if s == size {
                    Some(Value::Fixed(size, bytes))
                } else {
                    None
                }
            },
            Value::String(s) => {
                if s.as_bytes().len() == size {
                    Some(Value::Fixed(size, s.into_bytes()))
                } else {
                    None
                }
            },
            Value::Bytes(bytes) => {
                if bytes.len() == size {
                    Some(Value::Fixed(size, bytes))
                } else {
                    None
                }
            },
            _ => None,
        }
    }

    fn with_record(self, rschema: Rc<RecordSchema>) -> Option<Value> {
        match self {
            Value::Record(mut items) => {
                // Fill in defaults if needed
                for field in rschema.fields.iter() {
                    if !items.contains_key(&field.name) {
                        if let Some(default) = field.default.clone() {
                            items.insert(field.name.clone(), default.avro());
                        } else {
                            return None
                        }
                    }
                }

                // Remove fields that do not exist
                let lookup = rschema.lookup();
                let items = items.into_iter()
                    .filter_map(|(key, value)| lookup.get::<str>(&key).map(|_| (key, value)))
                    .collect::<HashMap<_, _>>();

                Some(Value::Record(items))
            },
            _ => None,
        }
    }
}

/*
impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where
        S: Serializer {

    }
}
*/
