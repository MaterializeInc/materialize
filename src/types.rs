use std::collections::HashMap;
use std::rc::Rc;

use failure::{Error, err_msg};
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
    Enum(i32),
    Union(Option<Box<Value>>),
    Array(Vec<Value>),
    Map(HashMap<String, Value>),
    Record(Vec<(String, Value)>),
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

/*
impl<S: Serialize> ToAvro for S {
    fn avro(self) -> Value {
        use ser::Serializer;

        self.serialize(&mut Serializer::new()).unwrap()
    }
}
*/

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
    fn avro(mut self) -> Value {
        let mut record_fields = Vec::new();
        for field in self.rschema.fields.iter() {
            if let Some(value) = self.fields.remove(&field.name) {
                record_fields.push((field.name.clone(), value));
            }
        }

        Value::Record(record_fields)
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
    pub fn validate(&self, schema: &Schema) -> bool {
        match (self, schema) {
            (&Value::Null, &Schema::Null) => true,
            (&Value::Boolean(_), &Schema::Boolean) => true,
            (&Value::Int(_), &Schema::Int) => true,
            (&Value::Long(_), &Schema::Long) => true,
            (&Value::Float(_), &Schema::Float) => true,
            (&Value::Double(_), &Schema::Double) => true,
            (&Value::Bytes(_), &Schema::Bytes) => true,
            (&Value::String(_), &Schema::String) => true,
            (&Value::Fixed(n, _), &Schema::Fixed { size, .. }) => n == size,
            (&Value::Enum(i), &Schema::Enum { ref symbols, .. }) => i >= 0 && i < (symbols.len() as i32),
            (&Value::Union(None), &Schema::Union(_)) => true,
            (&Value::Union(Some(ref value)), &Schema::Union(ref inner)) => value.validate(inner),
            (&Value::Array(ref items), &Schema::Array(ref inner)) => items.iter().all(|item| item.validate(inner)),
            (&Value::Map(ref items), &Schema::Map(ref inner)) => items.iter().all(|(_, value)| value.validate(inner)),
            (&Value::Record(ref fields), &Schema::Record(ref rschema)) =>
                rschema.fields.len() == fields.len() && rschema.fields.iter().zip(fields.iter())
                    .all(|(rfield, &(ref name, ref value))|
                        rfield.name == *name && value.validate(&rfield.schema)),
            _ => false,
        }
    }

    pub fn resolve(self, schema: &Schema) -> Result<Self, Error> {
        match schema {
            &Schema::Null => self.resolve_null(),
            &Schema::Boolean => self.resolve_boolean(),
            &Schema::Int => self.resolve_int(),
            &Schema::Long => self.resolve_long(),
            &Schema::Float => self.resolve_float(),
            &Schema::Double => self.resolve_double(),
            &Schema::Bytes => self.resolve_bytes(),
            &Schema::String => self.resolve_string(),
            &Schema::Fixed { size, .. } => self.resolve_fixed(size),
            &Schema::Union(ref inner) => self.resolve_union(inner),
            &Schema::Array(ref inner) => self.resolve_array(inner),
            &Schema::Map(ref inner) => self.resolve_map(inner),
            &Schema::Record(ref rschema) => self.resolve_record(rschema),
            _ => Err(err_msg(format!("Cannot resolve schema {:?} with {:?}", schema, self))),
        }
    }

    // TODO: macro?
    fn resolve_null(self) -> Result<Self, Error> {
        match self {
            Value::Null => Ok(Value::Null),
            other => Err(err_msg(format!("Null expected, got {:?}", other))),
        }
    }

    fn resolve_boolean(self) -> Result<Self, Error> {
        match self {
            Value::Boolean(b) => Ok(Value::Boolean(b)),
            other => Err(err_msg(format!("Boolean expected, got {:?}", other))),
        }
    }

    fn resolve_int(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Int(n)),
            other => Err(err_msg(format!("Int expected, got {:?}", other))),
        }
    }

    fn resolve_long(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Long(n as i64)),
            Value::Long(n) => Ok(Value::Long(n)),
            other => Err(err_msg(format!("Long expected, got {:?}", other))),
        }
    }

    fn resolve_float(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Float(n as f32)),
            Value::Long(n) => Ok(Value::Float(n as f32)),
            Value::Float(x) => Ok(Value::Float(x)),
            other => Err(err_msg(format!("Float expected, got {:?}", other))),
        }
    }

    fn resolve_double(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Double(n as f64)),
            Value::Long(n) => Ok(Value::Double(n as f64)),
            Value::Float(x) => Ok(Value::Double(x as f64)),
            Value::Double(x) => Ok(Value::Double(x)),
            other => Err(err_msg(format!("Double expected, got {:?}", other))),
        }
    }

    fn resolve_bytes(self) -> Result<Self, Error> {
        match self {
            Value::Bytes(bytes) => Ok(Value::Bytes(bytes)),
            Value::String(s) => Ok(Value::Bytes(s.into_bytes())),
            other => Err(err_msg(format!("Bytes expected, got {:?}", other))),
        }
    }

    fn resolve_string(self) -> Result<Self, Error> {
        match self {
            Value::String(s) => Ok(Value::String(s)),
            Value::Bytes(bytes) => Ok(Value::String(String::from_utf8(bytes)?)),
            other => Err(err_msg(format!("String expected, got {:?}", other))),
        }
    }

    fn resolve_fixed(self, size: usize) -> Result<Self, Error> {
        match self {
            Value::Fixed(n, bytes) => if n == size {
                Ok(Value::Fixed(n, bytes))
            } else {
                Err(err_msg(format!("Fixed size mismatch, {} expected, got {}", size, n)))
            },
            other => Err(err_msg(format!("String expected, got {:?}", other))),
        }
    }

    fn resolve_union(self, schema: &Schema) -> Result<Self, Error> {
        match self {
            Value::Union(None) => Ok(Value::Union(None)),
            Value::Union(Some(inner)) => Ok(Value::Union(Some(Box::new(inner.resolve(schema)?)))),
            other => Err(err_msg(format!("Union({:?}) expected, got {:?}", schema, other))),
        }
    }

    fn resolve_array(self, schema: &Schema) -> Result<Self, Error> {
        match self {
            Value::Array(items) => Ok(Value::Array(
                items.into_iter()
                    .map(|item| item.resolve(schema))
                    .collect::<Result<Vec<_>, _>>()?)),
            other => Err(err_msg(format!("Array({:?}) expected, got {:?}", schema, other))),
        }
    }

    fn resolve_map(self, schema: &Schema) -> Result<Self, Error> {
        match self {
            Value::Map(items) => Ok(Value::Map(
                items.into_iter()
                    .map(|(key, value)| value.resolve(schema).map(|value| (key, value)))
                    .collect::<Result<HashMap<_, _>, _>>()?)),
            other => Err(err_msg(format!("Map({:?}) expected, got {:?}", schema, other))),
        }
    }

    fn resolve_record(self, rschema: &RecordSchema) -> Result<Self, Error> {
        match self {
            Value::Record(fields) => {
                let mut lookup = fields.into_iter().collect::<HashMap<_, _>>();  // TODO: Value::Record(HashMap<String, Value>))?
                let new_fields = rschema.fields.iter()
                    .map(|field| {
                        let value = match lookup.remove(&field.name) {
                            Some(value) => value,
                            None => match field.default {
                                Some(ref value) => value.clone().avro(),
                                _ => return Err(err_msg(format!("missing field {} in record", field.name))),
                            }
                        };

                        value.resolve(&field.schema)
                            .map(|value| (field.name.clone(), value))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Value::Record(new_fields))
            },
            other => Err(err_msg(format!("Record({:?}) expected, got {:?}", rschema, other))),
        }
    }
}
