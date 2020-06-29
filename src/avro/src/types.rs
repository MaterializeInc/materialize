// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic handling the intermediate representation of Avro values.
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::u8;

use chrono::{NaiveDate, NaiveDateTime};
use failure::Fail;
use serde_json::Value as JsonValue;

use crate::schema::{RecordField, SchemaNode, SchemaPiece};

/// Describes errors happened while performing schema resolution on Avro data.
#[derive(Fail, Debug)]
#[fail(display = "Schema resolution error: {}", _0)]
pub struct SchemaResolutionError(pub String);

impl SchemaResolutionError {
    pub fn new<S>(msg: S) -> SchemaResolutionError
    where
        S: Into<String>,
    {
        SchemaResolutionError(msg.into())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DecimalValue {
    /// An unscaled two's-complement integer value in big-endian byte order.
    pub unscaled: Vec<u8>,
    pub precision: usize,
    pub scale: usize,
}

/// Represents any valid Avro value
/// More information about Avro values can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/spec.html#schemas)
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    // Fixed-length types
    /// A `null` Avro value.
    Null,
    /// A `boolean` Avro value.
    Boolean(bool),
    /// A `int` Avro value.
    Int(i32),
    /// A `long` Avro value.
    Long(i64),
    /// A `float` Avro value.
    Float(f32),
    /// A `double` Avro value.
    Double(f64),
    /// A `Date` coming from an avro Logical `Date`
    Date(NaiveDate),
    /// A `DateTime` coming from an avro Logical `Timestamp`
    Timestamp(NaiveDateTime),

    // Variable-length types
    /// A `decimal` Avro value
    ///
    /// The value of the decimal can be computed as follows:
    /// <em>unscaled</em> × 10<sup>-<em>scale</em></sup>.
    Decimal(DecimalValue),
    /// A `bytes` Avro value.
    Bytes(Vec<u8>),
    /// A `string` Avro value.
    String(String),
    /// A `fixed` Avro value.
    /// The size of the fixed value is represented as a `usize`.
    Fixed(usize, Vec<u8>),
    /// An `enum` Avro value.
    ///
    /// An Enum is represented by a symbol and its position in the symbols list
    /// of its corresponding schema.
    /// This allows schema-less encoding, as well as schema resolution while
    /// reading values.
    Enum(i32, String),
    /// An `union` Avro value.
    Union(usize, Box<Value>),
    /// An `array` Avro value.
    Array(Vec<Value>),
    /// A `map` Avro value.
    Map(HashMap<String, Value>),
    /// A `record` Avro value.
    ///
    /// A Record is represented by a vector of (`<field name>`, `value`).
    /// This allows schema-less encoding.
    ///
    /// See [Record](types.Record) for a more user-friendly support.
    Record(Vec<(String, Value)>),
    /// A `string` Avro value that has been interpreted as JSON.
    ///
    /// This is not part of the Avro spec, but is emitted by Debezium,
    /// and distinguished by setting the `"connect.name"` property to `"io.debezium.data.Json"`.
    Json(serde_json::Value),
}

/// Any structure implementing the [ToAvro](trait.ToAvro.html) trait will be usable
/// from a [Writer](../writer/struct.Writer.html).
pub trait ToAvro {
    /// Transforms this value into an Avro-compatible [Value](enum.Value.html).
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

impl<T, S: BuildHasher> ToAvro for HashMap<String, T, S>
where
    T: ToAvro,
{
    fn avro(self) -> Value {
        Value::Map(
            self.into_iter()
                .map(|(key, value)| (key, value.avro()))
                .collect::<_>(),
        )
    }
}

impl<'a, T, S: BuildHasher> ToAvro for HashMap<&'a str, T, S>
where
    T: ToAvro,
{
    fn avro(self) -> Value {
        Value::Map(
            self.into_iter()
                .map(|(key, value)| (key.to_owned(), value.avro()))
                .collect::<_>(),
        )
    }
}

impl ToAvro for Value {
    fn avro(self) -> Value {
        self
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

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

/// Utility interface to build `Value::Record` objects.
#[derive(Debug, Clone)]
pub struct Record<'a> {
    /// List of fields contained in the record.
    /// Ordered according to the fields in the schema given to create this
    /// `Record` object. Any unset field defaults to `Value::Null`.
    pub fields: Vec<(String, Value)>,
    schema_lookup: &'a HashMap<String, usize>,
    schema_fields: &'a Vec<RecordField>,
}

impl<'a> Record<'a> {
    /// Create a `Record` given a `SchemaNode`.
    ///
    /// If the `SchemaNode` is not a `SchemaPiece::Record` variant, `None` will be returned.
    pub fn new(schema: SchemaNode<'a>) -> Option<Record<'a>> {
        let ret = match schema.inner {
            SchemaPiece::Record {
                fields: schema_fields,
                lookup: schema_lookup,
                ..
            } => {
                let mut fields = Vec::with_capacity(schema_fields.len());
                for schema_field in schema_fields.iter() {
                    fields.push((schema_field.name.clone(), Value::Null));
                }

                Some(Record {
                    fields,
                    schema_lookup,
                    schema_fields,
                })
            }
            _ => None,
        };
        ret
    }

    /// Put a compatible value (implementing the `ToAvro` trait) in the
    /// `Record` for a given `field` name.
    ///
    /// **NOTE** Only ensure that the field name is present in the `SchemaNode` given when creating
    /// this `Record`. Does not perform any schema validation.
    pub fn put<V>(&mut self, field: &str, value: V)
    where
        V: ToAvro,
    {
        if let Some(&position) = self.schema_lookup.get(field) {
            self.fields[position].1 = value.avro()
        }
    }

    /// Get the field description corresponding to the given name.
    pub fn field_by_name(&self, name: &str) -> Option<&'a RecordField> {
        self.schema_lookup
            .get(name)
            .map(|idx| &self.schema_fields[*idx])
    }
}

impl<'a> ToAvro for Record<'a> {
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
            JsonValue::Number(n) => Value::Long(n.as_u64().unwrap() as i64), // TODO: Not so great
            JsonValue::String(s) => Value::String(s),
            JsonValue::Array(items) => {
                Value::Array(items.into_iter().map(|item| item.avro()).collect::<_>())
            }
            JsonValue::Object(items) => Value::Map(
                items
                    .into_iter()
                    .map(|(key, value)| (key, value.avro()))
                    .collect::<_>(),
            ),
        }
    }
}

impl Value {
    /// Validate the value against the given [Schema](../schema/enum.Schema.html).
    ///
    /// See the [Avro specification](https://avro.apache.org/docs/current/spec.html)
    /// for the full set of rules of schema validation.
    pub fn validate(&self, schema: SchemaNode) -> bool {
        match (self, schema.inner) {
            (&Value::Null, SchemaPiece::Null) => true,
            (&Value::Boolean(_), SchemaPiece::Boolean) => true,
            (&Value::Int(_), SchemaPiece::Int) => true,
            (&Value::Long(_), SchemaPiece::Long) => true,
            (&Value::Float(_), SchemaPiece::Float) => true,
            (&Value::Double(_), SchemaPiece::Double) => true,
            (&Value::Date(_), SchemaPiece::Date) => true,
            (&Value::Timestamp(_), SchemaPiece::TimestampMicro) => true,
            (&Value::Timestamp(_), SchemaPiece::TimestampMilli) => true,
            (
                &Value::Decimal(DecimalValue {
                    precision: vp,
                    scale: vs,
                    ..
                }),
                SchemaPiece::Decimal {
                    precision: sp,
                    scale: ss,
                    fixed_size: _,
                },
            ) => vp == *sp && vs == *ss,
            (&Value::Bytes(_), SchemaPiece::Bytes) => true,
            (&Value::String(_), SchemaPiece::String) => true,
            (&Value::Fixed(n, _), SchemaPiece::Fixed { size }) => n == *size,
            (&Value::String(ref s), SchemaPiece::Enum { symbols, .. }) => symbols.contains(s),
            (&Value::Enum(i, ref s), SchemaPiece::Enum { symbols, .. }) => symbols
                .get(i as usize)
                .map(|symbol| symbol == s)
                .unwrap_or(false),
            (&Value::Union(idx, ref value), SchemaPiece::Union(inner)) => {
                inner.variants().len() > idx && value.validate(schema.step(&inner.variants()[idx]))
            }
            (&Value::Array(ref items), SchemaPiece::Array(inner)) => {
                let node = schema.step(&**inner);
                items.iter().all(|item| item.validate(node))
            }
            (&Value::Map(ref items), SchemaPiece::Map(inner)) => {
                let node = schema.step(&**inner);
                items.iter().all(|(_, value)| value.validate(node))
            }
            (&Value::Record(ref record_fields), SchemaPiece::Record { fields, .. }) => {
                fields.len() == record_fields.len()
                    && fields.iter().zip(record_fields.iter()).all(
                        |(field, &(ref name, ref value))| {
                            let node = schema.step(&field.schema);
                            field.name == *name && value.validate(node)
                        },
                    )
            }
            (Value::Json(_), SchemaPiece::Json) => true,
            _ => false,
        }
    }

    // TODO - `into_` functions for all possible Value variants (perhaps generate this using a macro?)
    pub fn into_string(self) -> Option<String> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn into_nullable_bool(self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(b),
            Value::Union(_, v) => v.into_nullable_bool(),
            _ => None,
        }
    }

    pub fn into_integral(self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(i as i64),
            Value::Long(l) => Some(l),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Schema;

    #[test]
    fn validate() {
        let value_schema_valid = vec![
            (Value::Int(42), "\"int\"", true),
            (Value::Int(42), "\"boolean\"", false),
            (
                Value::Union(0, Box::new(Value::Null)),
                r#"["null", "int"]"#,
                true,
            ),
            (
                Value::Union(1, Box::new(Value::Int(42))),
                r#"["null", "int"]"#,
                true,
            ),
            (
                Value::Union(0, Box::new(Value::Null)),
                r#"["double", "int"]"#,
                false,
            ),
            (
                Value::Union(3, Box::new(Value::Int(42))),
                r#"["null", "double", "string", "int"]"#,
                true,
            ),
            (
                Value::Array(vec![Value::Long(42i64)]),
                r#"{"type": "array", "items": "long"}"#,
                true,
            ),
            (
                Value::Array(vec![Value::Boolean(true)]),
                r#"{"type": "array", "items": "long"}"#,
                false,
            ),
            (Value::Record(vec![]), "\"null\"", false),
        ];

        for (value, schema, valid) in value_schema_valid.into_iter() {
            let schema = Schema::parse_str(schema).unwrap();
            assert_eq!(valid, value.validate(schema.top_node()));
        }
    }

    #[test]
    fn validate_fixed() {
        let schema =
            Schema::parse_str(r#"{"type": "fixed", "size": 4, "name": "some_fixed"}"#).unwrap();

        assert!(Value::Fixed(4, vec![0, 0, 0, 0]).validate(schema.top_node()));
        assert!(!Value::Fixed(5, vec![0, 0, 0, 0, 0]).validate(schema.top_node()));
    }

    #[test]
    fn validate_enum() {
        let schema = Schema::parse_str(r#"{"type": "enum", "name": "some_enum", "symbols": ["spades", "hearts", "diamonds", "clubs"]}"#).unwrap();

        assert!(Value::Enum(0, "spades".to_string()).validate(schema.top_node()));
        assert!(Value::String("spades".to_string()).validate(schema.top_node()));

        assert!(!Value::Enum(1, "spades".to_string()).validate(schema.top_node()));
        assert!(!Value::String("lorem".to_string()).validate(schema.top_node()));

        let other_schema = Schema::parse_str(r#"{"type": "enum", "name": "some_other_enum", "symbols": ["hearts", "diamonds", "clubs", "spades"]}"#).unwrap();

        assert!(!Value::Enum(0, "spades".to_string()).validate(other_schema.top_node()));
    }

    #[test]
    fn validate_record() {
        let schema = Schema::parse_str(
            r#"{
           "type": "record",
           "fields": [
             {"type": "long", "name": "a"},
             {"type": "string", "name": "b"}
           ],
           "name": "some_record"
        }"#,
        )
        .unwrap();

        assert!(Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("b".to_string(), Value::String("foo".to_string())),
        ])
        .validate(schema.top_node()));

        assert!(!Value::Record(vec![
            ("b".to_string(), Value::String("foo".to_string())),
            ("a".to_string(), Value::Long(42i64)),
        ])
        .validate(schema.top_node()));

        assert!(!Value::Record(vec![
            ("a".to_string(), Value::Boolean(false)),
            ("b".to_string(), Value::String("foo".to_string())),
        ])
        .validate(schema.top_node()));

        assert!(!Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("c".to_string(), Value::String("foo".to_string())),
        ])
        .validate(schema.top_node()));

        assert!(!Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("b".to_string(), Value::String("foo".to_string())),
            ("c".to_string(), Value::Null),
        ])
        .validate(schema.top_node()));
    }

    #[test]
    fn validate_decimal() {
        assert!(Value::Decimal(DecimalValue {
            unscaled: vec![7],
            precision: 12,
            scale: 5
        })
        .validate(
            Schema::parse_str(
                r#"
            {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 12,
                "scale": 5
            }
        "#
            )
            .unwrap()
            .top_node()
        ));

        assert!(!Value::Decimal(DecimalValue {
            unscaled: vec![7],
            precision: 13,
            scale: 5
        })
        .validate(
            Schema::parse_str(
                r#"
            {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 12,
                "scale": 5
            }
        "#
            )
            .unwrap()
            .top_node()
        ));
    }
}
