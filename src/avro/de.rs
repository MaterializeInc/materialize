//! Logic for serde-compatible deserialization.
use std::collections::{
    hash_map::{Keys, Values},
    HashMap,
};
use std::error::{self, Error as StdError};
use std::fmt;
use std::slice::Iter;

use serde::{
    de::{self, DeserializeSeed, Error as SerdeError, Visitor},
    forward_to_deserialize_any, Deserialize,
};

use crate::types::Value;

#[derive(Clone, Debug, PartialEq)]
pub struct Error {
    message: String,
}

impl de::Error for Error {
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

pub struct Deserializer<'de> {
    input: &'de Value,
}

struct SeqDeserializer<'de> {
    input: Iter<'de, Value>,
}

struct MapDeserializer<'de> {
    input_keys: Keys<'de, String, Value>,
    input_values: Values<'de, String, Value>,
}

struct StructDeserializer<'de> {
    input: Iter<'de, (String, Value)>,
    value: Option<&'de Value>,
}

impl<'de> Deserializer<'de> {
    pub fn new(input: &'de Value) -> Self {
        Deserializer { input }
    }
}

impl<'de> SeqDeserializer<'de> {
    pub fn new(input: &'de [Value]) -> Self {
        SeqDeserializer {
            input: input.iter(),
        }
    }
}

impl<'de> MapDeserializer<'de> {
    pub fn new(input: &'de HashMap<String, Value>) -> Self {
        MapDeserializer {
            input_keys: input.keys(), // input.keys().map(|k| Value::String(k.clone())).collect::<Vec<_>>().iter(),
            input_values: input.values(),
            // keys: input.keys().map(|s| Value::String(s.to_owned())).collect::<Vec<Value>>(),
            // values: input.values().map(|s| s.to_owned()).collect::<Vec<Value>>(),
        }
    }
}

impl<'de> StructDeserializer<'de> {
    pub fn new(input: &'de [(String, Value)]) -> Self {
        StructDeserializer {
            input: input.iter(),
            value: None,
        }
    }
}

impl<'a, 'de> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::Null => visitor.visit_unit(),
            Value::Boolean(b) => visitor.visit_bool(b),
            Value::Int(i) => visitor.visit_i32(i),
            Value::Long(i) => visitor.visit_i64(i),
            Value::Float(x) => visitor.visit_f32(x),
            Value::Double(x) => visitor.visit_f64(x),
            _ => Err(Error::custom("incorrect value")),
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64
    }

    fn deserialize_char<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::custom("avro does not support char"))
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::String(ref s) => visitor.visit_str(s),
            Value::Bytes(ref bytes) | Value::Fixed(_, ref bytes) => ::std::str::from_utf8(bytes)
                .map_err(|e| Error::custom(e.description()))
                .and_then(|s| visitor.visit_str(s)),
            _ => Err(Error::custom("not a string|bytes|fixed")),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::String(ref s) => visitor.visit_string(s.to_owned()),
            Value::Bytes(ref bytes) | Value::Fixed(_, ref bytes) => {
                String::from_utf8(bytes.to_owned())
                    .map_err(|e| Error::custom(e.description()))
                    .and_then(|s| visitor.visit_string(s))
            }
            _ => Err(Error::custom("not a string|bytes|fixed")),
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::String(ref s) => visitor.visit_bytes(s.as_bytes()),
            Value::Bytes(ref bytes) | Value::Fixed(_, ref bytes) => visitor.visit_bytes(bytes),
            _ => Err(Error::custom("not a string|bytes|fixed")),
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::String(ref s) => visitor.visit_byte_buf(s.clone().into_bytes()),
            Value::Bytes(ref bytes) | Value::Fixed(_, ref bytes) => {
                visitor.visit_byte_buf(bytes.to_owned())
            }
            _ => Err(Error::custom("not a string|bytes|fixed")),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::Union(ref inner) if inner.as_ref() == &Value::Null => visitor.visit_none(),
            Value::Union(ref inner) => visitor.visit_some(&mut Deserializer::new(inner)),
            _ => Err(Error::custom("not a union")),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::Null => visitor.visit_unit(),
            _ => Err(Error::custom("not a null")),
        }
    }

    fn deserialize_unit_struct<V>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::Array(ref items) => visitor.visit_seq(SeqDeserializer::new(items)),
            _ => Err(Error::custom("not an array")),
        }
    }

    fn deserialize_tuple<V>(self, _: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _: &'static str,
        _: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::Map(ref items) => visitor.visit_map(MapDeserializer::new(items)),
            _ => Err(Error::custom("not a map")),
        }
    }

    fn deserialize_struct<V>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::Record(ref fields) => visitor.visit_map(StructDeserializer::new(fields)),
            _ => Err(Error::custom("not a record")),
        }
    }

    fn deserialize_enum<V>(
        self,
        _: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            // &Value::Enum(i) => ,  TODO
            _ => Err(Error::custom("not an enum")),
        }
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }
}

impl<'de> de::SeqAccess<'de> for SeqDeserializer<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.input.next() {
            Some(item) => seed.deserialize(&mut Deserializer::new(&item)).map(Some),
            None => Ok(None),
        }
    }
}

impl<'de> de::MapAccess<'de> for MapDeserializer<'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        match self.input_keys.next() {
            Some(ref key) => seed
                .deserialize(StringDeserializer {
                    input: (*key).clone(),
                })
                .map(Some),
            None => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        match self.input_values.next() {
            Some(ref value) => seed.deserialize(&mut Deserializer::new(value)),
            None => Err(Error::custom("should not happen - too many values")),
        }
    }
}

impl<'de> de::MapAccess<'de> for StructDeserializer<'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        match self.input.next() {
            Some(item) => {
                let (ref field, ref value) = *item;
                self.value = Some(value);
                seed.deserialize(StringDeserializer {
                    input: field.clone(),
                })
                .map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        match self.value.take() {
            Some(value) => seed.deserialize(&mut Deserializer::new(value)),
            None => Err(Error::custom("should not happen - too many values")),
        }
    }
}

struct StringDeserializer {
    input: String,
}

impl<'de> de::Deserializer<'de> for StringDeserializer {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_string(self.input)
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}

/// Interpret a `Value` as an instance of type `D`.
///
/// This conversion can fail if the structure of the `Value` does not match the
/// structure expected by `D`.
pub fn from_value<'de, D: Deserialize<'de>>(value: &'de Value) -> Result<D, Error> {
    let mut de = Deserializer::new(value);
    D::deserialize(&mut de)
}
