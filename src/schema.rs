use std::collections::HashMap;
use std::rc::Rc;

use failure::{Error, err_msg};
use serde_json::{Map, Value};

use util::MapHelper;

use types::ToAvro;
use types::Value as AvroValue;

#[derive(Clone, Debug, PartialEq)]
pub enum Schema {
    Null,
    Boolean,
    Double,
    Float,
    Long,
    Int,
    Bytes,
    String,
    Array(Rc<Schema>),
    Map(Rc<Schema>),
    Union(Rc<Schema>),
    Record(Rc<RecordSchema>),
    /*
    {
        name: Name,
        doc: Documentation,
        fields: Vec<RecordField>,
        fields_lookup: HashMap<String, usize>,
    },
    */
    Enum {
        name: Name,
        doc: Documentation,
        symbols: Vec<String>,
    },
    Fixed {
        name: Name,
        size: i32,
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordSchema {
    pub name: Name,
    pub doc: Documentation,
    pub fields: Vec<RecordField>,
    pub fields_lookup: HashMap<String, usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Name {
    pub name: String,
    pub namespace: Option<String>,
    pub aliases: Option<Vec<String>>,
}

pub type Documentation = Option<String>;

impl Name {
    pub fn new(name: &str) -> Name {
        Name {
            name: name.to_owned(),
            namespace: None,
            aliases: None,
        }
    }

    fn parse(complex: &Map<String, Value>) -> Result<Self, Error> {
        let name = complex.name()
            .ok_or_else(|| err_msg("No `name` field"))?;

        let namespace = complex.string("namespace");

        let aliases: Option<Vec<String>> = complex.get("aliases")
            .and_then(|aliases| aliases.as_array())
            .and_then(|aliases| {
                aliases.iter()
                    .map(|alias| alias.as_str())
                    .map(|alias| alias.map(|a| a.to_string()))
                    .collect::<Option<_>>()
            });

        Ok(Name {
            name: name,
            namespace: namespace,
            aliases: aliases,
        })
    }

    /*
    fn fullname(&self, default_namespace: Option<String>) -> String {
        if self.name.contains(".") {
            self.name.clone()
        } else {
            // TODO: why is .clone() needed? :(
            let namespace = self.namespace.clone().or(default_namespace);
            match namespace {
                Some(namespace) => format!("{}.{}", namespace, self.name),
                None => self.name.clone(),
            }
        }
    }
    */
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordField {
    pub name: String,
    pub doc: Option<String>,
    pub default: Option<AvroValue>,
    pub schema: Schema,
    pub order: RecordFieldOrder,
    pub position: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RecordFieldOrder {
    Ascending,
    Descending,
    Ignore,
}

impl RecordField {
    pub fn new(name: &str, schema: Schema, position: usize) -> RecordField {
        RecordField {
            name: name.to_owned(),
            doc: None,
            default: None,
            schema: schema,
            order: RecordFieldOrder::Ascending,
            position: position,
        }
    }

    fn parse(field: &Map<String, Value>, position: usize) -> Result<Self, Error> {
        let name = field.name()
            .ok_or_else(|| err_msg("No `name` in record field"))?;

        // TODO: "type" = "<record name>"
        let schema = field.get("type")
            .ok_or_else(|| err_msg("No `type` in record field"))
            .and_then(|type_| Schema::parse(type_))?;

        let default = field.get("default")
            .map(|f| f.clone());

        let order = field.get("order")
            .and_then(|o| o.as_str())
            .and_then(|order| match order {
                "ascending" => Some(RecordFieldOrder::Ascending),
                "descending" => Some(RecordFieldOrder::Descending),
                "ignore" => Some(RecordFieldOrder::Ignore),
                _ => None,
            })
            .unwrap_or_else(|| RecordFieldOrder::Ascending);


        Ok(RecordField {
            name: name,
            doc: field.doc(),
            default: default.map(|d| d.avro()),
            schema: schema,
            order: order,
            position: position,
        })
    }
}

impl Schema {
    pub fn parse(value: &Value) -> Result<Self, Error> {
        match *value {
            Value::String(ref t) => Schema::parse_primitive(t.as_str()),
            Value::Object(ref data) => Schema::parse_complex(data),
            Value::Array(ref data) => Schema::parse_union(data),
            _ => Err(err_msg("Error"))
        }
    }

    fn parse_primitive(primitive: &str) -> Result<Self, Error> {
        match primitive {
            "null" => Ok(Schema::Null),
            "boolean" => Ok(Schema::Boolean),
            "double" => Ok(Schema::Double),
            "float" => Ok(Schema::Float),
            "long" => Ok(Schema::Long),
            "int" => Ok(Schema::Int),
            "bytes" => Ok(Schema::Bytes),
            "string" => Ok(Schema::String),
            other => Err(err_msg(format!("Unknown primitive type: {}", other)))
        }
    }

    fn parse_complex(complex: &Map<String, Value>) -> Result<Self, Error> {
        match complex.get("type") {
            Some(&Value::String(ref t)) => match t.as_str() {
                "record" => Schema::parse_record(complex),
                "enum" => Schema::parse_enum(complex),
                "array" => Schema::parse_array(complex),
                "map" => Schema::parse_map(complex),
                "fixed" => Schema::parse_fixed(complex),
                other => Err(err_msg(format!("Unknown complex type: {}", other)))
            }
            _ => Err(err_msg("No `type` in complex type")),
        }
    }

    fn parse_record(complex: &Map<String, Value>) -> Result<Self, Error> {
        let name = Name::parse(complex)?;

        let fields: Vec<RecordField> = complex.get("fields")
            .and_then(|fields| fields.as_array())
            .ok_or_else(|| err_msg("No `fields` in record"))
            .and_then(|fields| {
                fields.iter()
                    .filter_map(|field| field.as_object())
                    .enumerate()
                    .map(|(position, field)| {
                        RecordField::parse(field, position)
                    })
                    .collect::<Result<_, _>>()
            })?;

        let mut fields_lookup = HashMap::new();
        for field in fields.iter() {
            fields_lookup.insert(field.name.clone(), field.position); // TODO: clone
        }

        Ok(Schema::Record(Rc::new(RecordSchema {
            name: name,
            doc: complex.doc(),
            fields: fields,
            fields_lookup: fields_lookup,
        })))
    }

    fn parse_enum(complex: &Map<String, Value>) -> Result<Self, Error> {
        let name = Name::parse(complex)?;

        let symbols = complex.get("symbols")
            .and_then(|v| v.as_array())
            .ok_or_else(|| err_msg("No `symbols` field in enum"))
            .and_then(|symbols| {
                symbols.iter()
                    .map(|symbol| symbol.as_str().map(|s| s.to_string()))
                    .collect::<Option<_>>()
                    .ok_or_else(|| err_msg("Unable to parse `symbols` in enum"))
            })?;

        Ok(Schema::Enum {
            name: name,
            doc: complex.doc(),
            symbols: symbols,
        })
    }

    fn parse_array(complex: &Map<String, Value>) -> Result<Self, Error> {
        complex.get("items")
            .ok_or_else(|| err_msg("No `items` in array"))
            .and_then(|items| Schema::parse(items))
            .map(|schema| Schema::Array(Rc::new(schema)))
    }

    fn parse_map(complex: &Map<String, Value>) -> Result<Self, Error> {
        complex.get("values")
            .ok_or_else(|| err_msg("No `values` in map"))
            .and_then(|items| Schema::parse(items))
            .map(|schema| Schema::Map(Rc::new(schema)))
    }

    fn parse_union(items: &Vec<Value>) -> Result<Self, Error> {
        /*
        items.iter()
            .map(|item| Schema::parse(item))
            .collect::<Result<_, _>>()
            .map(|schemas| Schema::Union(schemas))
        */

        match items.as_slice() {
            // &[Value::String(ref null), ref x] | &[ref x, Value::String(ref null)] if null == "null" => {
            &[Value::String(ref null), ref x] if null == "null" => {
                Schema::parse(&x).map(|s| Schema::Union(Rc::new(s)))
            },
            _ => Err(err_msg("Unions only support null and type")),
        }
    }

    fn parse_fixed(complex: &Map<String, Value>) -> Result<Self, Error> {
        let name = Name::parse(complex)?;

        let size = complex.get("size")
            .and_then(|v| v.as_i64())
            .map(|s| s as i32)
            .ok_or_else(|| err_msg("No `size` in fixed"))?;

        Ok(Schema::Fixed {
            name: name,
            size: size,
        })
    }

    pub fn record(name: Name, fields: Vec<RecordField>) -> Schema {
        let mut fields_lookup = HashMap::new();
        for field in fields.iter() {
            fields_lookup.insert(field.name.clone(), field.position); // TODO: clone
        }

        Schema::Record(Rc::new(RecordSchema {
            name: name,
            doc: None,
            fields: fields,
            fields_lookup: fields_lookup,
        }))
    }
}

impl Into<Map<String, Value>> for Name {
    fn into(self) -> Map<String, Value> {
        let mut object = Map::new();
        object.insert("name".to_owned(), self.name.into());

        if let Some(namespace) = self.namespace {
            object.insert("namespace".to_owned(), namespace.into());
        }

        if let Some(aliases) = self.aliases {
            object.insert("aliases".to_owned(), aliases.into());
        }

        object
    }
}

impl Into<Value> for Schema {
    fn into(self) -> Value {
        match self {
            Schema::Null => Value::String("null".to_owned()),
            Schema::Boolean => Value::String("bool".to_owned()),
            Schema::Int => Value::String("int".to_owned()),
            Schema::Long => Value::String("long".to_owned()),
            Schema::Float => Value::String("float".to_owned()),
            Schema::Double => Value::String("double".to_owned()),
            Schema::Bytes => Value::String("bytes".to_owned()),
            Schema::String => Value::String("string".to_owned()),
            Schema::Fixed { name, size } => {
                let mut object: Map<String, Value> = name.into();
                object.insert("type".to_owned(), "fixed".into());
                object.insert("size".to_owned(), size.into());
                Value::Object(object)
            },
            Schema::Array(ref inner) => {
                let mut object = Map::with_capacity(2);
                object.insert("type".to_owned(), "array".into());
                object.insert("items".to_owned(), (*inner.clone()).clone().into());
                Value::Object(object)
            },
            Schema::Map(ref inner) => {
                let mut object = Map::with_capacity(2);
                object.insert("type".to_owned(), "array".into());
                object.insert("values".to_owned(), (*inner.clone()).clone().into());
                Value::Object(object)
            },
            Schema::Record(ref record_schema) => {
                let mut object: Map<String, Value> = record_schema.name.clone().into();
                object.insert("type".to_owned(), "record".into());
                object.insert("fields".to_owned(), record_schema.fields.clone().into());
                Value::Object(object)
            },
            Schema::Union(ref inner) => {
                Value::Array(vec![
                    Schema::Null.into(),
                    (*inner.clone()).clone().into(),
                ])
            },
            Schema::Enum { name, symbols, .. } => {
                let mut object: Map<String, Value> = name.into();
                object.insert("symbols".into(), symbols.into());
                Value::Object(object)
            }
        }
    }
}

impl Into<Value> for RecordField {
    fn into(self) -> Value {
        let mut object = Map::new();
        object.insert("name".to_owned(), self.name.into());
        object.insert("type".to_owned(), self.schema.into());

        if let Some(default) = self.default {
            // object.insert("default".to_owned(), default.into());
        }

        Value::Object(object)
    }
}
