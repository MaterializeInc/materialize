use std::collections::HashMap;
use std::rc::Rc;

use failure::{Error, err_msg};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_json::{Map, Value};

use util::MapHelper;

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
        size: usize,
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
    pub default: Option<Value>,
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
            default: default,
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
            .ok_or_else(|| err_msg("No `size` in fixed"))?;

        Ok(Schema::Fixed {
            name: name,
            size: size as usize,
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

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where
        S: Serializer {
        match *self {
            Schema::Null => serializer.serialize_str("null"),
            Schema::Boolean => serializer.serialize_str("boolean"),
            Schema::Int => serializer.serialize_str("int"),
            Schema::Long => serializer.serialize_str("long"),
            Schema::Float => serializer.serialize_str("float"),
            Schema::Double => serializer.serialize_str("double"),
            Schema::Bytes => serializer.serialize_str("bytes"),
            Schema::String => serializer.serialize_str("string"),
            Schema::Array(ref inner) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "array")?;
                map.serialize_entry("items", &*inner.clone())?;
                map.end()
            },
            Schema::Map(ref inner) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "map")?;
                map.serialize_entry("values", &*inner.clone())?;
                map.end()
            },
            Schema::Union(ref inner) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element("null")?;
                seq.serialize_element(&*inner.clone())?;
                seq.end()
            },
            Schema::Record(ref record_schema) => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "record")?;
                map.serialize_entry("name", &record_schema.name.name)?;
                // TODO: namespace, etc...
                map.serialize_entry("fields", &record_schema.fields)?;
                map.end()
            },
            Schema::Enum { ref name, ref symbols, .. } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "enum")?;
                map.serialize_entry("name", &name.name)?;
                map.serialize_entry("symbols", symbols)?;
                map.end()
            },
            Schema::Fixed { ref name, ref size } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "fixed")?;
                map.serialize_entry("name", &name.name)?;
                map.serialize_entry("size", size)?;
                map.end()
            }
        }
    }
}

impl Serialize for RecordField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where
        S: Serializer {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("type", &self.schema)?;

        if let Some(ref default) = self.default {
            map.serialize_entry("default", default)?;
        }

        map.end()
    }
}
