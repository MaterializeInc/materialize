// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use avro_rs::Schema as AvroSchema;
use failure::Error;

use crate::repr::{Type, Schema};
use ore::vec::VecExt;

/// Convert an Apache Avro schema into a [`repr::Schema`].
pub fn parse_schema(schema: &str) -> Result<Schema, Error> {
    let avro_schema = AvroSchema::parse_str(schema)?;
    Ok(Schema {
        name: None,
        nullable: is_nullable(&avro_schema),
        typ: parse_schema_1(&avro_schema),
    })
}

fn parse_schema_1(avro_schema: &AvroSchema) -> Type {
    match avro_schema {
        AvroSchema::Null => Type::Null,
        AvroSchema::Boolean => Type::Bool,
        AvroSchema::Int => Type::Int32,
        AvroSchema::Long => Type::Int64,
        AvroSchema::Float => Type::Float32,
        AvroSchema::Double => Type::Float64,
        AvroSchema::Bytes | AvroSchema::Fixed { .. } => Type::Bytes,
        AvroSchema::String | AvroSchema::Enum { .. } => Type::String,

        AvroSchema::Array(schema) => {
            let el_type = Schema {
                name: None,
                nullable: is_nullable(schema),
                typ: parse_schema_1(schema),
            };

            Type::Array(Box::new(el_type))
        }

        AvroSchema::Map(s) => {
            Type::Tuple(vec![
                Schema {
                    name: Some("key".into()),
                    nullable: false,
                    typ: Type::String,
                },
                Schema {
                    name: Some("value".into()),
                    nullable: is_nullable(s),
                    typ: parse_schema_1(s),
                }
            ])
        }

        AvroSchema::Union(us) => {
            let utypes: Vec<_> = us.variants().iter()
                // Null variants are handled by is_nullable, which makes
                // the entire union nullable in the presence of a null
                // variant.
                .filter(|s| !is_null(s))
                .map(|s| Schema {
                    name: None,
                    nullable: is_nullable(s),
                    typ: parse_schema_1(s),
                })
                .collect();

            if utypes.len() == 1 {
                utypes.into_element().typ
            } else {
                Type::Tuple(utypes)
            }
        }

        AvroSchema::Record { fields, .. } => {
            let ftypes = fields.iter().map(|f| Schema {
                name: Some(f.name.clone()),
                nullable: is_nullable(&f.schema),
                typ: parse_schema_1(&f.schema),
            }).collect();

            Type::Tuple(ftypes)
        }
    }
}

fn is_nullable(schema: &AvroSchema) -> bool {
    match schema {
        AvroSchema::Null => true,
        AvroSchema::Union(us) => us.variants().iter().any(|v| is_null(v)),
        _ => false,
    }
}

fn is_null(schema: &AvroSchema) -> bool {
    match schema {
        AvroSchema::Null => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use failure::ResultExt;
    use pretty_assertions::assert_eq;
    use serde::Deserialize;
    use std::fs::File;

    use crate::repr::Schema;

    #[derive(Deserialize)]
    struct TestCase {
        name: String,
        input: serde_json::Value,
        expected: Schema,
    }

    #[test]
    fn test_schema_parsing() -> Result<(), failure::Error> {
        let file = File::open("interchange/testdata/avro-schema.json")
            .context("opening test data file")?;
        let test_cases: Vec<TestCase> = serde_json::from_reader(file)
            .context("parsing JSON test data")?;

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