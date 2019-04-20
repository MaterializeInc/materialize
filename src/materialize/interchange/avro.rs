// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use avro_rs::types::Value;
use avro_rs::Schema;
use failure::{bail, Error};

use crate::repr::{Datum, FType, Type};
use ore::vec::VecExt;

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
    schema: Schema,
}

impl Decoder {
    /// Creates a new `Decoder`.
    pub fn new(schema: &str) -> Decoder {
        Decoder {
            schema: Schema::parse_str(schema).unwrap(),
        }
    }

    /// Decodes Avro-encoded `bytes` that adhere to `schema` into a `Datum`.
    pub fn decode(&self, mut bytes: &[u8]) -> Result<Datum, failure::Error> {
        let val = avro_rs::from_avro_datum(&self.schema, &mut bytes, Some(&self.schema))?;
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
