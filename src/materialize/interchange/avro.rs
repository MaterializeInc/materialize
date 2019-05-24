// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use avro_rs::types::Value;
use avro_rs::Schema;
use byteorder::{BigEndian, ByteOrder};
use failure::{bail, Error};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use url::Url;

use crate::repr::{Datum, FType, Type};
use ore::collections::CollectionExt;

/// Converts an Apache Avro schema into a [`repr::Type`].
pub fn parse_schema(schema: &str) -> Result<Type, Error> {
    let schema = Schema::parse_str(schema)?;
    Ok(Type {
        name: None,
        nullable: is_nullable(&schema),
        ftype: parse_schema_1(&schema),
    })
}

fn parse_schema_1(schema: &Schema) -> FType {
    match schema {
        Schema::Null => FType::Null,
        Schema::Boolean => FType::Bool,
        Schema::Int => FType::Int32,
        Schema::Long => FType::Int64,
        Schema::Float => FType::Float32,
        Schema::Double => FType::Float64,
        Schema::Bytes | Schema::Fixed { .. } => FType::Bytes,
        Schema::String | Schema::Enum { .. } => FType::String,

        Schema::Array(schema) => {
            let el_type = Type {
                name: None,
                nullable: is_nullable(schema),
                ftype: parse_schema_1(schema),
            };

            FType::Array(Box::new(el_type))
        }

        Schema::Map(s) => FType::Tuple(vec![
            Type {
                name: Some("key".into()),
                nullable: false,
                ftype: FType::String,
            },
            Type {
                name: Some("value".into()),
                nullable: is_nullable(s),
                ftype: parse_schema_1(s),
            },
        ]),

        Schema::Union(us) => {
            let utypes: Vec<_> = us
                .variants()
                .iter()
                // Null variants are handled by is_nullable, which makes
                // the entire union nullable in the presence of a null
                // variant.
                .filter(|s| !is_null(s))
                .map(|s| Type {
                    name: None,
                    nullable: is_nullable(s),
                    ftype: parse_schema_1(s),
                })
                .collect();

            if utypes.len() == 1 {
                utypes.into_element().ftype
            } else {
                FType::Tuple(utypes)
            }
        }

        Schema::Record { fields, .. } => {
            let ftypes = fields
                .iter()
                .map(|f| Type {
                    name: Some(f.name.clone()),
                    nullable: is_nullable(&f.schema),
                    ftype: parse_schema_1(&f.schema),
                })
                .collect();

            FType::Tuple(ftypes)
        }
    }
}

fn is_nullable(schema: &Schema) -> bool {
    match schema {
        Schema::Null => true,
        Schema::Union(us) => us.variants().iter().any(|v| is_null(v)),
        _ => false,
    }
}

fn is_null(schema: &Schema) -> bool {
    match schema {
        Schema::Null => true,
        _ => false,
    }
}

/// Manages decoding of Avro-encoded bytes.
pub struct Decoder {
    reader_schema: Schema,
    writer_schemas: Option<SchemaCache>,
}

impl Decoder {
    /// Creates a new `Decoder`
    ///
    /// The provided schema is called the "reader schema", which is the schema
    /// that we are expecting to use to decode records. The records may indicate
    /// that they are encoded with a different schema; as long as those.
    pub fn new(reader_schema: &str, schema_registry_url: Option<url::Url>) -> Decoder {
        Decoder {
            // It is assumed that the reader schema has already been verified
            // to be a valid Avro schema.
            reader_schema: Schema::parse_str(reader_schema).unwrap(),
            writer_schemas: schema_registry_url.map(SchemaCache::new),
        }
    }

    /// Decodes Avro-encoded `bytes` into a `Datum`.
    pub fn decode(&mut self, mut bytes: &[u8]) -> Result<Datum, failure::Error> {
        // The first byte is a magic byte (0) that indicates the Confluent
        // serialization format version, and the next four bytes are a big
        // endian 32-bit schema ID.
        //
        // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
        if bytes.len() < 5 {
            bail!(
                "avro datum is too few bytes: expected at least 5 bytes, got {}",
                bytes.len()
            );
        }
        let magic = bytes[0];
        let schema_id = BigEndian::read_i32(&bytes[1..5]);
        bytes = &bytes[5..];

        if magic != 0 {
            bail!(
                "wrong avro serialization magic: expected 0, got {}",
                bytes[0]
            );
        }

        // If we haven't been asked to use a schema registry, we have no way to
        // discover the writer's schema. That's ok; we'll just use the reader's
        // schema and hope it lines up.
        let writer_schema = match &mut self.writer_schemas {
            Some(cache) => cache.get(schema_id)?,
            None => &self.reader_schema,
        };

        let val = avro_rs::from_avro_datum(&writer_schema, &mut bytes, Some(&self.reader_schema))?;
        let mut row = Vec::new();
        match val {
            Value::Record(cols) => {
                for (_field_name, col) in cols {
                    row.push(match col {
                        Value::Null => Datum::Null,
                        Value::Boolean(b) => {
                            if b {
                                Datum::True
                            } else {
                                Datum::False
                            }
                        }
                        Value::Long(i) => Datum::Int64(i),
                        Value::Float(f) => Datum::Float32(f.into()),
                        Value::Double(f) => Datum::Float64(f.into()),
                        Value::Bytes(b) => Datum::Bytes(b),
                        Value::String(s) => Datum::String(s),
                        other => bail!("unsupported avro value: {:?}", other),
                    })
                }
            }
            _ => bail!("unsupported avro value: {:?}", val),
        }
        Ok(Datum::Tuple(row))
    }
}

struct SchemaCache {
    cache: HashMap<i32, Schema>,
    ccsr_client: ccsr::Client,
}

impl SchemaCache {
    fn new(schema_registry_url: Url) -> SchemaCache {
        SchemaCache {
            cache: HashMap::new(),
            ccsr_client: ccsr::Client::new(schema_registry_url),
        }
    }

    fn get(&mut self, id: i32) -> Result<&Schema, failure::Error> {
        match self.cache.entry(id) {
            Entry::Occupied(o) => Ok(o.into_mut()),
            Entry::Vacant(v) => {
                // TODO(benesch): make this asynchronous, to avoid blocking the
                // Timely thread on this network request.
                let res = self.ccsr_client.get_schema_by_id(id)?;
                let schema = Schema::parse_str(&res.raw)?;
                Ok(v.insert(schema))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use failure::ResultExt;
    use pretty_assertions::assert_eq;
    use serde::Deserialize;
    use std::fs::File;

    use crate::repr::Type;

    #[derive(Deserialize)]
    struct TestCase {
        name: String,
        input: serde_json::Value,
        expected: Type,
    }

    #[test]
    fn test_schema_parsing() -> Result<(), failure::Error> {
        let file = File::open("interchange/testdata/avro-schema.json")
            .context("opening test data file")?;
        let test_cases: Vec<TestCase> =
            serde_json::from_reader(file).context("parsing JSON test data")?;

        for tc in test_cases {
            // Stringifying the JSON we just parsed is rather silly, but it
            // avoids embedding JSON strings inside of JSON, which is hard on
            // the eyes.
            let schema = serde_json::to_string(&tc.input)?;
            let output = super::parse_schema(&schema)?;
            assert_eq!(output, tc.expected, "failed test case name: {}", tc.name)
        }

        Ok(())
    }
}
