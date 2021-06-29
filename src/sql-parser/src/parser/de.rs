use serde::de::{self, DeserializeSeed, EnumAccess, SeqAccess, VariantAccess, Visitor};
use serde::forward_to_deserialize_any;

use super::Parser;
use crate::ast::Value;
use crate::lexer::Token;

#[derive(Debug)]
pub struct Error(String);
impl serde::de::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Error(msg.to_string())
    }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

type Result<T> = std::result::Result<T, Error>;

impl<'de, 'a> de::Deserializer<'de> for &'a mut Parser<'de> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        unimplemented!()
    }

    forward_to_deserialize_any! { f32 f64 char bytes byte_buf map ignored_any }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.parse_value().unwrap() {
            Value::Boolean(b) => visitor.visit_bool(b),
            _ => panic!(),
        }
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.parse_value().unwrap() {
            Value::Number(n) => visitor.visit_i64(n.parse().unwrap()),
            _ => panic!(),
        }
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_i64(visitor)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_i64(visitor)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_i64(visitor)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(self.parse_literal_uint().unwrap())
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_u64(visitor)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_u64(visitor)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_u64(visitor)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_string(self.parse_literal_string().unwrap())
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        // None fields are never serialized so the only way to end up here is if the field was Some
        visitor.visit_some(self)
    }

    /// Unit types are not encoded at all
    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_unit()
    }

    // Unit struct means a named value containing no data.
    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(CommaSeparated::new(&mut self))
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(Seq { parser: self, len })
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_seq(Seq { parser: self, len })
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        // Struct fields are always in order, so use a sequence
        visitor.visit_seq(Map::new(self, fields))
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_enum(Enum::new(self))
    }

    /// Expecting a name of a field or a name of a variant
    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let ident = self.parse_identifier().unwrap();
        visitor.visit_string(ident.into_string())
    }
}

/// We're currently parsing a sequence with `len` remaining elements
struct Seq<'a, 'de: 'a> {
    parser: &'a mut Parser<'de>,
    len: usize,
}

impl<'de, 'a> SeqAccess<'de> for Seq<'a, 'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.len == 0 {
            return Ok(None);
        }
        self.len -= 1;

        seed.deserialize(&mut *self.parser).map(Some)
    }
}

/// We're currently parsing a map with `fields` remaining fields
struct Map<'a, 'de: 'a> {
    parser: &'a mut Parser<'de>,
    fields: Vec<&'static str>,
}

impl<'a, 'de> Map<'a, 'de> {
    fn new(parser: &'a mut Parser<'de>, fields: &'static [&'static str]) -> Self {
        let mut fields: Vec<&'static str> = fields.into();
        fields.reverse();
        Map { parser, fields }
    }
}

impl<'de, 'a> SeqAccess<'de> for Map<'a, 'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if let Some(field) = self.fields.pop() {
            // Lookahead to check if the next field in SQL is the next field of the Map. If it's not we
            // return None and Deserialize with either instantiate the default value or produce an error
            if self.parser.peek_identifier().as_ref().map(|i| i.as_str()) == Some(field) {
                let _ = self.parser.parse_identifier();
                return seed.deserialize(&mut *self.parser).map(Some);
            }
        }
        Ok(None)
    }
}

struct Enum<'a, 'de: 'a> {
    parser: &'a mut Parser<'de>,
}

impl<'a, 'de> Enum<'a, 'de> {
    fn new(parser: &'a mut Parser<'de>) -> Self {
        Enum { parser }
    }
}

impl<'de, 'a> EnumAccess<'de> for Enum<'a, 'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant)> {
        let val = seed.deserialize(&mut *self.parser)?;
        Ok((val, self))
    }
}

impl<'de, 'a> VariantAccess<'de> for Enum<'a, 'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        seed.deserialize(self.parser)
    }

    fn tuple_variant<V: Visitor<'de>>(mut self, len: usize, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(Seq {
            parser: &mut self.parser,
            len,
        })
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_seq(Map::new(self.parser, fields))
    }
}

struct CommaSeparated<'a, 'de: 'a> {
    parser: &'a mut Parser<'de>,
    first: bool,
}

impl<'a, 'de> CommaSeparated<'a, 'de> {
    fn new(parser: &'a mut Parser<'de>) -> Self {
        CommaSeparated {
            parser,
            first: true,
        }
    }
}

impl<'de, 'a> SeqAccess<'de> for CommaSeparated<'a, 'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        if self.first || self.parser.consume_token(&Token::Comma) {
            self.first = false;
            seed.deserialize(&mut *self.parser).map(Some).or(Ok(None))
        } else {
            Ok(None)
        }
    }
}
