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

use crate::repr::{ColumnType, Datum, RelationType, ScalarType};
use ore::collections::CollectionExt;

/// Converts an Apache Avro schema into a [`repr::RelationType`].
pub fn parse_schema(schema: &str) -> Result<RelationType, Error> {
    let schema = Schema::parse_str(schema)?;

    // The top-level record needs to be a diff "envelope" that contains
    // `before` and `after` fields, where the `before` and `after` fields
    // have the same schema.
    let row_schema = match &schema {
        Schema::Record { fields, .. } => {
            let before = fields.iter().find(|f| f.name == "before");
            let after = fields.iter().find(|f| f.name == "after");
            match (before, after) {
                (Some(before), Some(after)) => {
                    if !eq_ignoring_names(&before.schema, &after.schema) {
                        bail!("source schema has mismatched 'before' and 'after' schemas")
                    }
                    &before.schema
                }
                (None, _) => bail!("source schema is missing 'before' field"),
                (_, None) => bail!("source schema is missing 'after' field"),
            }
        }
        _ => bail!("source schema does not match required envelope format"),
    };

    // The "row" schema used by the `before` and `after` fields needs to be
    // a nullable record type.
    let row_schema = match &row_schema {
        Schema::Union(us) => {
            if us.variants().len() != 2 {
                bail!("source schema 'before'/'after' fields are not of expected type");
            }
            let has_null = us.variants().iter().any(|s| is_null(s));
            let record = us.variants().iter().find(|s| match s {
                Schema::Record { .. } => true,
                _ => false,
            });
            if !has_null {
                bail!("source schema has non-nullable 'before'/'after' fields");
            }
            match record {
                Some(record) => record,
                None => bail!("source schema 'before/'after' fields are not of expected type"),
            }
        }
        _ => bail!("source schema has non-nullable 'before'/'after' fields"),
    };

    // The diff envelope is sane. Convert the actual record schema for the row.
    match row_schema {
        Schema::Record { fields, .. } => {
            let column_types = fields
                .iter()
                .map(|f| {
                    Ok(ColumnType {
                        name: Some(f.name.clone()),
                        nullable: is_nullable(&f.schema),
                        scalar_type: parse_schema_1(&f.schema)?,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;

            Ok(RelationType { column_types })
        }
        _ => bail!("row schemas must be records, got: {:?}", row_schema),
    }
}

fn parse_schema_1(schema: &Schema) -> Result<ScalarType, Error> {
    Ok(match schema {
        Schema::Null => ScalarType::Null,
        Schema::Boolean => ScalarType::Bool,
        Schema::Int => ScalarType::Int32,
        Schema::Long => ScalarType::Int64,
        Schema::Float => ScalarType::Float32,
        Schema::Double => ScalarType::Float64,
        Schema::Bytes | Schema::Fixed { .. } => ScalarType::Bytes,
        Schema::String | Schema::Enum { .. } => ScalarType::String,

        Schema::Union(us) => {
            let utypes: Vec<_> = us
                .variants()
                .iter()
                // Null variants are handled by is_nullable, which makes
                // the entire union nullable in the presence of a null
                // variant.
                .filter(|s| !is_null(s))
                .map(|s| {
                    Ok(ColumnType {
                        name: None,
                        nullable: is_nullable(s),
                        scalar_type: parse_schema_1(s)?,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;

            if utypes.len() == 1 {
                utypes.into_element().scalar_type
            } else {
                bail!("Unsupported union type: {:?}", schema)
            }
        }

        // Schema::Array(schema) => {
        //     let el_type = ColumnType {
        //         name: None,
        //         nullable: is_nullable(schema),
        //         scalar_type: parse_schema_1(schema),
        //     };

        //     ScalarType::Array(Box::new(el_type))
        // }

        // Schema::Map(s) => ScalarType::Tuple(vec![
        //     ColumnType {
        //         name: Some("key".into()),
        //         nullable: false,
        //         scalar_type: ScalarType::String,
        //     },
        //     ColumnType {
        //         name: Some("value".into()),
        //         nullable: is_nullable(s),
        //         scalar_type: parse_schema_1(s),
        //     },
        // ]),

        // Schema::Record { fields, .. } => {
        //     let scalar_types = fields
        //         .iter()
        //         .map(|f| ColumnType {
        //             name: Some(f.name.clone()),
        //             nullable: is_nullable(&f.schema),
        //             scalar_type: parse_schema_1(&f.schema),
        //         })
        //         .collect();

        //     ScalarType::Tuple(scalar_types)
        // }
        //
        _ => bail!("Unsupported scalar type in schema: {:?}", schema),
    })
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

fn eq_ignoring_names(a: &Schema, b: &Schema) -> bool {
    match (a, b) {
        (Schema::Null, Schema::Null) => true,
        (Schema::Boolean, Schema::Boolean) => true,
        (Schema::Int, Schema::Int) => true,
        (Schema::Long, Schema::Long) => true,
        (Schema::Float, Schema::Float) => true,
        (Schema::Double, Schema::Double) => true,
        (Schema::Bytes, Schema::Bytes) => true,
        (Schema::String, Schema::String) => true,
        (Schema::Array(a), Schema::Array(b)) => eq_ignoring_names(&*a, &*b),
        (Schema::Map(a), Schema::Map(b)) => eq_ignoring_names(&*a, &*b),
        (Schema::Union(a), Schema::Union(b)) => a
            .variants()
            .iter()
            .zip(b.variants())
            .all(|(a, b)| eq_ignoring_names(a, b)),
        (Schema::Record { fields: a, .. }, Schema::Record { fields: b, .. }) => a
            .iter()
            .zip(b.iter())
            .all(|(a, b)| eq_ignoring_names(&a.schema, &b.schema)),
        (Schema::Enum { symbols: a, .. }, Schema::Enum { symbols: b, .. }) => a == b,
        (Schema::Fixed { size: a, .. }, Schema::Fixed { size: b, .. }) => a == b,
        _ => false,
    }
}

#[derive(Debug)]
pub struct DiffPair {
    pub before: Option<Vec<Datum>>,
    pub after: Option<Vec<Datum>>,
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

    /// Decodes Avro-encoded `bytes` into a `DiffPair`.
    pub fn decode(&mut self, mut bytes: &[u8]) -> Result<DiffPair, failure::Error> {
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

        fn value_to_datum(v: Value) -> Result<Datum, failure::Error> {
            match v {
                Value::Null => Ok(Datum::Null),
                Value::Boolean(true) => Ok(Datum::True),
                Value::Boolean(false) => Ok(Datum::False),
                Value::Long(i) => Ok(Datum::Int64(i)),
                Value::Float(f) => Ok(Datum::Float32(f.into())),
                Value::Double(f) => Ok(Datum::Float64(f.into())),
                Value::Bytes(b) => Ok(Datum::Bytes(b)),
                Value::String(s) => Ok(Datum::String(s)),
                Value::Union(v) => value_to_datum(*v),
                other => bail!("unsupported avro value: {:?}", other),
            }
        };

        fn extract_row(v: Value) -> Result<Option<Vec<Datum>>, failure::Error> {
            let v = match v {
                Value::Union(v) => *v,
                _ => bail!("unsupported avro value: {:?}", v),
            };
            match v {
                Value::Record(fields) => {
                    let mut row = Vec::new();
                    for (_, col) in fields {
                        row.push(value_to_datum(col)?);
                    }
                    Ok(Some(row))
                }
                Value::Null => Ok(None),
                _ => bail!("unsupported avro value: {:?}", v),
            }
        }

        let val = avro_rs::from_avro_datum(&writer_schema, &mut bytes, Some(&self.reader_schema))?;
        let mut before = None;
        let mut after = None;
        match val {
            Value::Record(fields) => {
                for (name, val) in fields {
                    if name == "before" {
                        before = extract_row(val)?;
                    } else if name == "after" {
                        after = extract_row(val)?;
                    } else {
                        // Intentionally ignore other fields.
                    }
                }
            }
            _ => bail!("avro envelope had unexpected type: {:?}", val),
        }
        Ok(DiffPair { before, after })
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

    use crate::repr::RelationType;

    #[derive(Deserialize)]
    struct TestCase {
        name: String,
        input: serde_json::Value,
        expected: RelationType,
    }

    #[test]
    #[ignore] // TODO(benesch): update tests for diff envelope.
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
