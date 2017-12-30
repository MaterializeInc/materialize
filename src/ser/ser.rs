use std::collections::HashMap;
use std::fmt;
use std::error::{self, Error as StdError};
use std::iter::once;
use std::rc::Rc;

use serde::ser::{self, Error as SerdeError, Serialize};

use schema::{RecordSchema, Schema};
use types::{Record, ToAvro, Value};

#[derive(Clone)]
pub struct Serializer<'a> {
    schema: &'a Schema,
}

pub struct SeqSerializer<'a> {
    serializer: Serializer<'a>,
    items: Vec<Value>,
}

pub struct MapSerializer<'a> {
    serializer: Serializer<'a>,
    indices: HashMap<String, usize>,
    values: Vec<Value>,
}

pub struct StructSerializer {
    rschema: Rc<RecordSchema>,
    // lookup: HashMap<&'a str, usize>,  TODO
    items: HashMap<String, Value>,
    index: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Error {
    message: String,
}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error {
            message: msg.to_string(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(error::Error::description(self))
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        &self.message
    }
}

impl<'a> Serializer<'a> {
    pub fn new(schema: &'a Schema) -> Serializer<'a> {
        Serializer {
            schema: schema,
        }
    }
}

impl<'a> SeqSerializer<'a> {
    pub fn new(schema: &'a Schema, len: Option<usize>) -> SeqSerializer<'a> {
        let items = match len {
            Some(len) => Vec::with_capacity(len),
            None => Vec::new(),
        };

        SeqSerializer {
            serializer: Serializer::new(schema),
            items: items,
        }
    }
}

impl<'a> MapSerializer<'a> {
    pub fn new(schema: &'a Schema, len: Option<usize>) -> MapSerializer<'a> {
        let (indices, values) = match len {
            Some(len) => (HashMap::with_capacity(len), Vec::with_capacity(len)),
            None => (HashMap::new(), Vec::new()),
        };

        MapSerializer {
            serializer: Serializer::new(schema),
            indices: indices,
            values: values,
        }
    }
}

impl StructSerializer {
    pub fn new(rschema: Rc<RecordSchema>, len: usize) -> StructSerializer {
        StructSerializer {
            rschema: rschema.clone(),
            items: HashMap::with_capacity(len),
            index: 0,
        }
    }
}

impl<'a, 'b> ser::Serializer for &'b mut Serializer<'a> {
    type Ok = Value;
    type Error = Error;
    type SerializeSeq = SeqSerializer<'a>;
    type SerializeTuple = SeqSerializer<'a>;
    type SerializeTupleStruct = SeqSerializer<'a>;
    type SerializeTupleVariant = SeqSerializer<'a>;
    type SerializeMap = MapSerializer<'a>;
    type SerializeStruct = StructSerializer;
    type SerializeStructVariant = StructSerializer;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            &Schema::Boolean => Ok(Value::Boolean(v)),
            _ => Err(Error::custom("schema is not bool")),
        }
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.serialize_i32(v as i32)
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.serialize_i32(v as i32)
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            &Schema::Int => Ok(Value::Int(v)),
            &Schema::Long => self.serialize_i64(v as i64),
            &Schema::Float => self.serialize_f32(v as f32),
            &Schema::Double => self.serialize_f64(v as f64),
            _ => Err(Error::custom("schema is not int|long|float|double")),
        }
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            &Schema::Long => Ok(Value::Long(v)),
            &Schema::Float => self.serialize_f32(v as f32),
            &Schema::Double => self.serialize_f64(v as f64),
            _ => Err(Error::custom("schema is not long|float|double")),
        }
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.serialize_i32(v as i32)
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.serialize_i32(v as i32)
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        if v <= i32::max_value() as u32 {
            self.serialize_i32(v as i32)
        } else {
            self.serialize_i64(v as i64)
        }
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        if v <= i64::max_value() as u64 {
            self.serialize_i64(v as i64)
        } else {
            Err(Error::custom("u64 is too large"))
        }
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            &Schema::Float => Ok(Value::Float(v)),
            &Schema::Double => self.serialize_f64(v as f64),
            _ => Err(Error::custom("schema is not float|double")),
        }
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            &Schema::Double => self.serialize_f64(v),
            _ => Err(Error::custom("schema is not double")),
        }
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(&once(v).collect::<String>())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            &Schema::String => Ok(Value::String(v.to_owned())),
            &Schema::Bytes
            | &Schema::Fixed { .. } => self.serialize_bytes(v.as_ref()),
            _ => Err(Error::custom("schema is not string|bytes|fixed")),
        }
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            &Schema::Bytes => Ok(Value::Bytes(v.to_owned())),
            &Schema::Fixed { size, .. } => {
                if size == v.len() {
                    Ok(Value::Bytes(v.to_owned()))
                } else {
                    Err(Error::custom("fixed size does not match"))
                }
            },
            &Schema::String => ::std::str::from_utf8(v)
                .map_err(|e| Error::custom(e.description()))
                .and_then(|s| s.serialize(self)),
            _ => Err(Error::custom("schema is not string|bytes|fixed")),
        }
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            &Schema::Union(_) => Ok(Value::Null),
            _ => Err(Error::custom("schema is not union")),
        }
    }

    fn serialize_some<T: ? Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> where
        T: Serialize {
        match self.schema {
            &Schema::Union(ref inner) => {
                let v = value.serialize(&mut Serializer::new(inner))?;
                Ok(Value::Union(Some(Box::new(v))))
            },
            _ => Err(Error::custom("schema is not union")),
        }
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            &Schema::Null => Ok(Value::Null),
            _ => Err(Error::custom("schema is not null")),
        }
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<Self::Ok, Self::Error> {
        unimplemented!()  // TODO: enum
    }

    fn serialize_newtype_struct<T: ? Sized>(self, _: &'static str, value: &T) -> Result<Self::Ok, Self::Error> where
        T: Serialize {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ? Sized>(self, _: &'static str, _: u32, _: &'static str, value: &T) -> Result<Self::Ok, Self::Error> where
        T: Serialize {
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        match self.schema {
            &Schema::Array(ref inner) => Ok(SeqSerializer::new(inner, len)),
            _ => Err(Error::custom("schema is not array")),
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(self, _: &'static str, len: usize) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Self::SerializeTupleVariant, Self::Error> {
        unimplemented!()  // TODO ?
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        match self.schema {
            &Schema::Map(ref inner) => Ok(MapSerializer::new(inner, len)),
            _ => Err(Error::custom("schema is not map")),
        }
    }

    fn serialize_struct(self, _: &'static str, len: usize) -> Result<Self::SerializeStruct, Self::Error> {
        match self.schema {
            &Schema::Record(ref rschema) => Ok(StructSerializer::new(rschema.clone(), len)),
            _ => Err(Error::custom("schema is not record")),
        }
    }

    fn serialize_struct_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Self::SerializeStructVariant, Self::Error> {
        unimplemented!()  // TODO ?
    }
}

impl<'a> ser::SerializeSeq for SeqSerializer<'a> {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T: ? Sized>(&mut self, value: &T) -> Result<(), Self::Error> where
        T: Serialize {
        self.items.push(value.serialize(&mut self.serializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Array(self.items))
    }
}

impl<'a> ser::SerializeTuple for SeqSerializer<'a> {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T: ? Sized>(&mut self, value: &T) -> Result<(), Self::Error> where
        T: Serialize {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeSeq::end(self)
    }
}

impl<'a> ser::SerializeTupleStruct for SeqSerializer<'a> {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: ? Sized>(&mut self, value: &T) -> Result<(), Self::Error> where
        T: Serialize {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeSeq::end(self)
    }
}

impl<'a> ser::SerializeTupleVariant for SeqSerializer<'a> {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: ? Sized>(&mut self, _: &T) -> Result<(), Self::Error> where
        T: Serialize {
        unimplemented!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
}

impl<'a> ser::SerializeMap for MapSerializer<'a> {
    type Ok = Value;
    type Error = Error;

    fn serialize_key<T: ? Sized>(&mut self, key: &T) -> Result<(), Self::Error> where
        T: Serialize {
        let key = key.serialize(&mut Serializer::new(&Schema::String))?;

        if let Value::String(key) = key {
            self.indices.insert(key, self.values.len());
            Ok(())
        } else {
            Err(Error::custom("map key is not a string"))
        }
    }

    fn serialize_value<T: ? Sized>(&mut self, value: &T) -> Result<(), Self::Error> where
        T: Serialize {
        self.values.push(value.serialize(&mut self.serializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let mut items = HashMap::new();
        for (key, index) in self.indices.into_iter() {
            if let Some(value) = self.values.get(index) {
                items.insert(key, value.clone());
            }
        }

        Ok(Value::Map(items))
    }
}

impl ser::SerializeStruct for StructSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: ? Sized>(&mut self, _: &'static str, value: &T) -> Result<(), Self::Error> where
        T: Serialize {
        let to_insert = match self.rschema.fields.get(self.index) {
            Some(ref field) => {
                let v = value.serialize(&mut Serializer::new(&field.schema))?;
                Some((field.name.to_owned(), v))
            },
            None => None,
        };

        match to_insert {
            Some((key, value)) => {
                self.index += 1;
                self.items.insert(key, value);
                Ok(())
            },
            None => Err(Error::custom("struct field not found in schema")),
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        // TODO: index == rschema.fields.len()
        Ok(Value::Record(self.items, self.rschema))
    }
}

impl ser::SerializeStructVariant for StructSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: ? Sized>(&mut self, _: &'static str, _: &T) -> Result<(), Self::Error> where
        T: Serialize {
        unimplemented!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where
        S: ser::Serializer {
        match self {
            &Value::Null => serializer.serialize_unit(),
            &Value::Boolean(b) => serializer.serialize_bool(b),
            &Value::Int(i) => serializer.serialize_i32(i),
            &Value::Long(i) => serializer.serialize_i64(i),
            &Value::Float(x) => serializer.serialize_f32(x),
            &Value::Double(x) => serializer.serialize_f64(x),
            &Value::Bytes(ref bytes) => serializer.serialize_bytes(bytes.as_ref()),
            &Value::String(ref s) => serializer.serialize_str(&s),
            &Value::Fixed(_, ref bytes) => serializer.serialize_bytes(bytes.as_ref()),
            &Value::Array(ref items) => {
                use serde::ser::SerializeSeq;

                let mut sseq = serializer.serialize_seq(Some(items.len()))?;
                items.iter()
                    .map(|item| sseq.serialize_element(item))
                    .collect::<Result<Vec<()>, _>>()?;
                sseq.end()
            },
            &Value::Map(ref items) => {
                use serde::ser::SerializeMap;

                let mut mseq = serializer.serialize_map(Some(items.len()))?;
                items.iter()
                    .map(|(key, value)| {
                        mseq.serialize_key(key)?;
                        mseq.serialize_value(value)
                    })
                    .collect::<Result<Vec<()>, _>>()?;
                mseq.end()
            },
            &Value::Union(ref option) => match option {
                &Some(ref item) => serializer.serialize_some(item),
                &None => serializer.serialize_none(),
            },
            &Value::Record(ref items, ref rschema) => {
                use serde::ser::SerializeStruct;

                let mut sseq = serializer.serialize_struct("", items.len())?;

                for field in rschema.fields.iter() {
                    if let Some(value) = items.get(&field.name) {
                        sseq.serialize_field("", value)?;
                    }
                }

                sseq.end()
            }
        }
    }
}

impl<'a> Serialize for Record<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where
        S: ser::Serializer {
        use serde::ser::SerializeStruct;

        let mut sseq = serializer.serialize_struct("", self.fields.len())?;

        for field in self.rschema.fields.iter() {
            if let Some(value) = self.fields.get(&field.name) {
                sseq.serialize_field("", value)?;
            } else if let Some(ref default) = field.default {
                sseq.serialize_field("", &default.clone().avro())?;
            } else {
                return Err(S::Error::custom("missing field when serializing struct"))
            }
        }

        sseq.end()
    }
}
