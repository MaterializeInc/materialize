extern crate failure;
extern crate serde_json;

use std::rc::Rc;

use failure::{Error, err_msg};
use serde_json::{Map, Value};

#[derive(Debug)]
pub enum Schema {
    Null,
    Boolean,
    Double,
    Float,
    Long,
    Int,
    Bytes,
    String,
    Record(RecordSchema),
    Enum(EnumSchema),
    Array(Rc<Schema>),
    Map(Rc<Schema>),
    Union(Vec<Schema>),
    Fixed(FixedSchema),
}

#[derive(Debug)]
pub struct Name {
    pub name: String,
    pub namespace: Option<String>,
    pub aliases: Option<Vec<String>>,
}

impl Name {
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
    fn fullname(&self) -> String {
        if self.name.contains(".") {
            self.name.clone()
        } else {
            match self.namespace {
                Some(ref namespace) => format!("{}.{}", namespace, self.name),
                None => self.name.clone(),
            }
        }
    }
    */
}

#[derive(Debug)]
pub struct RecordField {
    pub name: String,
    pub doc: Option<String>,
    pub schema: Schema,
    pub default: Option<Value>,
}

#[derive(Debug)]
pub struct RecordSchema {
    pub name: Name,
    pub doc: Option<String>,
    pub fields: Vec<RecordField>,
}

#[derive(Debug)]
pub struct EnumSchema {
    pub name: Name,
    pub doc: Option<String>,
    pub symbols: Vec<String>,
}

#[derive(Debug)]
pub struct FixedSchema {
    pub name: Name,
    pub size: i32,
}

impl RecordField {
    fn parse(field: &Map<String, Value>) -> Result<Self, Error> {
        let name = field.name()
            .ok_or_else(|| err_msg("No `name` in record field"))?;

        // TODO: "type" = "<record name>"
        let schema = field.get("type")
            .ok_or_else(|| err_msg("No `type` in record field"))
            .and_then(|type_| Schema::parse(type_))?;

        let default = field.get("default")
            .map(|f| f.clone());

        Ok(RecordField {
            name: name,
            doc: field.doc(),
            schema: schema,
            default: default,
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

        let fields = complex.get("fields")
            .and_then(|fields| fields.as_array())
            .ok_or_else(|| err_msg("No `fields` in record"))
            .and_then(|fields| {
                fields.iter()
                    .filter_map(|field| field.as_object())  // TODO: errors
                    .map(|field| RecordField::parse(field))
                    .collect::<Result<_, _>>()
            })?;

        Ok(Schema::Record(RecordSchema {
            name: name,
            doc: complex.doc(),
            fields: fields,
        }))
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

        Ok(Schema::Enum(EnumSchema {
            name: name,
            doc: complex.doc(),
            symbols: symbols,
        }))
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
        items.iter()
            .map(|item| Schema::parse(item))
            .collect::<Result<_, _>>()
            .map(|schemas| Schema::Union(schemas))
    }

    fn parse_fixed(complex: &Map<String, Value>) -> Result<Self, Error> {
        let name = Name::parse(complex)?;

        let size = complex.get("size")
            .and_then(|v| v.as_i64())
            .map(|s| s as i32)
            .ok_or_else(|| err_msg("No `size` in fixed"))?;

        Ok(Schema::Fixed(FixedSchema {
            name: name,
            size: size,
        }))
    }
}

trait MapHelper {
    fn string(&self, key: &str) -> Option<String>;

    fn name(&self) -> Option<String> {
        self.string("name")
    }

    fn doc(&self) -> Option<String> {
        self.string("doc")
    }
}

impl MapHelper for Map<String, Value> {
    fn string(&self, key: &str) -> Option<String> {
        self.get(key)
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
