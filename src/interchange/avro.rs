// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;

use avro_rs::schema::{Schema, SchemaFingerprint};
use avro_rs::types::Value;
use byteorder::{BigEndian, ByteOrder};
use failure::{bail, Error};
use sha2::Sha256;
use url::Url;

use ore::collections::CollectionExt;
use repr::decimal::{Significand, MAX_DECIMAL_PRECISION};
use repr::{ColumnType, Datum, RelationType, ScalarType};

/// Validates an Avro key schema for use as a source.
///
/// An Avro key schema is valid for our purposes iff every field
/// mentioned in the key schema exists in the specified relation
/// type with the same type. If the schema is valid, returns a
/// vector describing the order and position of the primary key
/// columns.
pub fn validate_key_schema(
    key_schema: &str,
    validated_value_schema: &RelationType,
) -> Result<Vec<usize>, Error> {
    let mut vname_to_value_column = HashMap::new();
    for (i, column) in validated_value_schema.column_types.iter().enumerate() {
        if let Some(name) = &column.name {
            vname_to_value_column.insert(name, (i, column));
        }
    }

    let key_schema = parse_schema(key_schema)?;
    let mut indices = Vec::new();
    for key_column in validate_schema_1(&key_schema)?.column_types.iter() {
        if let Some(name) = &key_column.name {
            // The code around ColumnTypes
            let value = vname_to_value_column.get(name);
            match value {
                Some((index, value_column)) => {
                    if key_column.scalar_type == value_column.scalar_type
                        && key_column.nullable == value_column.nullable
                    {
                        indices.push(index.clone());
                    } else {
                        bail!(
                            "key and value column types do not match: key {:?} vs. value {:?}",
                            key_column,
                            value_column,
                        )
                    }
                }
                None => bail!("Value schema missing primary key column: {}", name),
            }
        }
    }

    Ok(indices)
}

/// Converts an Apache Avro schema into a [`repr::RelationType`].
pub fn validate_value_schema(schema: &str) -> Result<RelationType, Error> {
    let schema = parse_schema(schema)?;

    // The top-level record needs to be a diff "envelope" that contains
    // `before` and `after` fields, where the `before` and `after` fields
    // have the same schema.
    let row_schema = match &schema {
        Schema::Record { fields, .. } => {
            let before = fields.iter().find(|f| f.name == "before");
            let after = fields.iter().find(|f| f.name == "after");
            match (before, after) {
                (Some(before), Some(after)) => {
                    if let Some((left, right)) =
                        first_mismatched_schema_types(&before.schema, &after.schema)
                    {
                        bail!(
                            "source schema has mismatched 'before' and 'after' schemas: before={:?} after={:?}",
                            left,
                            right
                        )
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
    validate_schema_1(row_schema)
}

fn validate_schema_1(schema: &Schema) -> Result<RelationType, Error> {
    match schema {
        Schema::Record { fields, .. } => {
            let column_types = fields
                .iter()
                .map(|f| {
                    Ok(ColumnType {
                        name: Some(f.name.clone()),
                        nullable: is_nullable(&f.schema),
                        scalar_type: validate_schema_2(&f.schema)?,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;

            Ok(RelationType { column_types })
        }
        _ => bail!("row schemas must be records, got: {:?}", schema),
    }
}

fn validate_schema_2(schema: &Schema) -> Result<ScalarType, Error> {
    Ok(match schema {
        Schema::Null => ScalarType::Null,
        Schema::Boolean => ScalarType::Bool,
        Schema::Int => ScalarType::Int32,
        Schema::Long => ScalarType::Int64,
        Schema::Float => ScalarType::Float32,
        Schema::Double => ScalarType::Float64,
        Schema::Date => ScalarType::Date,
        Schema::TimestampMilli => ScalarType::Timestamp,
        Schema::TimestampMicro => ScalarType::Timestamp,
        Schema::Decimal {
            precision, scale, ..
        } => {
            if *precision > MAX_DECIMAL_PRECISION as usize {
                bail!(
                    "decimals with precision greater than {} are not supported",
                    MAX_DECIMAL_PRECISION
                )
            }
            ScalarType::Decimal(*precision as u8, *scale as u8)
        }
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
                        scalar_type: validate_schema_2(s)?,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;

            if utypes.len() == 1 {
                utypes.into_element().scalar_type
            } else {
                bail!("Unsupported union type: {:?}", schema)
            }
        }

        Schema::Array(_) | Schema::Map(_) | Schema::Record { .. } => {
            bail!("Unsupported scalar type in schema: {:?}", schema)
        }
    })
}

pub fn parse_schema(schema: &str) -> Result<Schema, Error> {
    // munge resolves named types in Avro schemas, which are not currently
    // supported by our Avro library. Follow [0] for details.
    //
    // [0]: https://github.com/flavray/avro-rs/pull/53
    //
    // TODO(benesch): fix this upstream.
    fn munge(
        schema: serde_json::Value,
        types: &mut HashMap<String, serde_json::Value>,
    ) -> serde_json::Value {
        use serde_json::Value::*;
        match schema {
            Null | Bool(_) | Number(_) => schema,

            String(s) => match s.as_ref() {
                "null" | "boolean" | "int" | "long" | "float" | "double" | "bytes" | "string" => {
                    String(s)
                }
                other => types.get(other).cloned().unwrap_or_else(|| String(s)),
            },

            Array(vs) => Array(vs.into_iter().map(|v| munge(v, types)).collect()),

            Object(mut map) => {
                if let Some(String(name)) = map.get("name") {
                    types.insert(name.clone(), Object(map.clone()));
                }
                if let Some(fields) = map.remove("fields") {
                    let fields = match fields {
                        Array(fields) => Array(
                            fields
                                .into_iter()
                                .map(|f| match f {
                                    Object(mut fmap) => {
                                        if let Some(typ) = fmap.remove("type") {
                                            fmap.insert("type".into(), munge(typ, types));
                                        }
                                        Object(fmap)
                                    }
                                    other => other,
                                })
                                .collect(),
                        ),
                        other => other,
                    };
                    map.insert("fields".into(), fields);
                }
                Object(map)
            }
        }
    }
    let schema = serde_json::from_str(schema)?;
    let schema = munge(schema, &mut HashMap::new());
    Schema::parse(&schema)
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

/// Return None if they are equal, otherwise return the first mismatched schema
fn first_mismatched_schema_types<'a>(
    a: &'a Schema,
    b: &'a Schema,
) -> Option<(&'a Schema, &'a Schema)> {
    match (a, b) {
        (Schema::Null, Schema::Null) => None,
        (Schema::Boolean, Schema::Boolean) => None,
        (Schema::Int, Schema::Int) => None,
        (Schema::Long, Schema::Long) => None,
        (Schema::Float, Schema::Float) => None,
        (Schema::Double, Schema::Double) => None,
        (Schema::Bytes, Schema::Bytes) => None,
        (Schema::Date, Schema::Date) => None,
        (Schema::TimestampMilli, Schema::TimestampMilli) => None,
        (Schema::TimestampMicro, Schema::TimestampMicro) => None,
        (
            Schema::Decimal {
                precision: p1,
                scale: s1,
                fixed_size: fs1,
            },
            Schema::Decimal {
                precision: p2,
                scale: s2,
                fixed_size: fs2,
            },
        ) if p1 == p2 && s1 == s2 && fs1 == fs2 => None,
        (Schema::String, Schema::String) => None,
        (Schema::Array(a), Schema::Array(b)) => first_mismatched_schema_types(&*a, &*b),
        (Schema::Map(a), Schema::Map(b)) => first_mismatched_schema_types(&*a, &*b),
        (Schema::Union(a), Schema::Union(b)) => a
            .variants()
            .iter()
            .zip(b.variants())
            .flat_map(|(a, b)| first_mismatched_schema_types(a, b))
            .nth(0),
        (Schema::Record { fields: a, .. }, Schema::Record { fields: b, .. }) => a
            .iter()
            .zip(b.iter())
            .flat_map(|(a, b)| first_mismatched_schema_types(&a.schema, &b.schema))
            .nth(0),
        (Schema::Enum { symbols: a, .. }, Schema::Enum { symbols: b, .. }) if a == b => None,
        (Schema::Fixed { size: a, .. }, Schema::Fixed { size: b, .. }) if a == b => None,
        (left, right) => Some((left, right)),
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
    fast_row_schema: Option<Schema>,
}

impl fmt::Debug for Decoder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Decoder")
            .field("reader_schema", &self.reader_schema)
            .field(
                "write_schema",
                if self.writer_schemas.is_some() {
                    &"some"
                } else {
                    &"none"
                },
            )
            .field("fast_row_schema", &self.fast_row_schema)
            .finish()
    }
}

impl Decoder {
    /// Creates a new `Decoder`
    ///
    /// The provided schema is called the "reader schema", which is the schema
    /// that we are expecting to use to decode records. The records may indicate
    /// that they are encoded with a different schema; as long as those.
    pub fn new(reader_schema: &str, schema_registry_url: Option<url::Url>) -> Decoder {
        // It is assumed that the reader schema has already been verified
        // to be a valid Avro schema.
        let reader_schema = parse_schema(reader_schema).unwrap();
        let writer_schemas = schema_registry_url
            .map(|url| SchemaCache::new(url, reader_schema.fingerprint::<Sha256>()));

        let fast_row_schema = match &reader_schema {
            // If the first two fields in the record are `before` and `after`,
            // we don't need to decode the whole record. This can yield a
            // substantial performance win when there is additional heavyweight
            // metadata at the end of each record which would be immediately
            // discarded.
            Schema::Record { fields, .. }
                if fields[0].name == "before" && fields[1].name == "after" =>
            {
                Some(fields[0].schema.clone())
            }
            _ => None,
        };

        Decoder {
            reader_schema,
            writer_schemas,
            fast_row_schema,
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

        let (writer_schema, reader_schema) = match &mut self.writer_schemas {
            Some(cache) => match cache.get(schema_id)? {
                // If we get a schema back, the writer schema differs from our
                // schema, so we need to perform schema resolution. If not,
                // the schemas are identical, so we can skip schema resolution.
                Some(writer_schema) => (writer_schema, Some(&self.reader_schema)),
                None => (&self.reader_schema, None),
            },
            // If we haven't been asked to use a schema registry, we have no way
            // to discover the writer's schema. That's ok; we'll just use the
            // reader's schema and hope it lines up.
            None => (&self.reader_schema, None),
        };

        fn value_to_datum(v: Value) -> Result<Datum, failure::Error> {
            match v {
                Value::Null => Ok(Datum::Null),
                Value::Boolean(true) => Ok(Datum::True),
                Value::Boolean(false) => Ok(Datum::False),
                Value::Int(i) => Ok(Datum::Int32(i)),
                Value::Long(i) => Ok(Datum::Int64(i)),
                Value::Float(f) => Ok(Datum::Float32(f.into())),
                Value::Double(f) => Ok(Datum::Float64(f.into())),
                Value::Date(d) => Ok(Datum::Date(d)),
                Value::Timestamp(d) => Ok(Datum::Timestamp(d)),
                Value::Decimal { unscaled, .. } => Ok(Datum::Decimal(
                    Significand::from_twos_complement_be(&unscaled)?,
                )),
                Value::Bytes(b) => Ok(Datum::Bytes(b)),
                Value::String(s) => Ok(Datum::String(s)),
                Value::Union(v) => value_to_datum(*v),
                other @ Value::Fixed(..)
                | other @ Value::Enum(..)
                | other @ Value::Array(_)
                | other @ Value::Map(_)
                | other @ Value::Record(_) => bail!("unsupported avro value: {:?}", other),
            }
        };

        fn extract_row(v: Value) -> Result<Option<Vec<Datum>>, failure::Error> {
            let v = match v {
                Value::Union(v) => *v,
                _ => bail!("unsupported avro value: {:?}", v),
            };
            match v {
                Value::Record(fields) => {
                    let mut row = Vec::with_capacity(fields.len());
                    for (_, col) in fields {
                        row.push(value_to_datum(col)?);
                    }
                    Ok(Some(row))
                }
                Value::Null => Ok(None),
                _ => bail!("unsupported avro value: {:?}", v),
            }
        }

        let mut before = None;
        let mut after = None;
        if let (Some(schema), None) = (&self.fast_row_schema, reader_schema) {
            // The record is laid out such that we can extract the `before` and
            // `after` fields without decoding the entire record.
            before = extract_row(avro_rs::from_avro_datum(&schema, &mut bytes, None)?)?;
            after = extract_row(avro_rs::from_avro_datum(&schema, &mut bytes, None)?)?;
        } else {
            let val = avro_rs::from_avro_datum(writer_schema, &mut bytes, reader_schema)?;
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
        }
        Ok(DiffPair { before, after })
    }
}

struct SchemaCache {
    cache: HashMap<i32, Option<Schema>>,
    ccsr_client: ccsr::Client,

    reader_fingerprint: SchemaFingerprint,
}

impl SchemaCache {
    fn new(schema_registry_url: Url, reader_fingerprint: SchemaFingerprint) -> SchemaCache {
        SchemaCache {
            cache: HashMap::new(),
            ccsr_client: ccsr::Client::new(schema_registry_url),
            reader_fingerprint,
        }
    }

    /// Looks up the writer schema for ID. If the schema is literally identical
    /// to the reader schema, as determined by the reader schema fingerprint
    /// that this schema cache was initialized with, returns None.
    fn get(&mut self, id: i32) -> Result<Option<&Schema>, failure::Error> {
        match self.cache.entry(id) {
            Entry::Occupied(o) => Ok(o.into_mut().as_ref()),
            Entry::Vacant(v) => {
                // TODO(benesch): make this asynchronous, to avoid blocking the
                // Timely thread on this network request.
                let res = self.ccsr_client.get_schema_by_id(id)?;
                let schema = parse_schema(&res.raw)?;
                if schema.fingerprint::<Sha256>().bytes == self.reader_fingerprint.bytes {
                    Ok(v.insert(None).as_ref())
                } else {
                    Ok(v.insert(Some(schema)).as_ref())
                }
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

    use repr::RelationType;

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
            let output = super::validate_value_schema(&schema)?;
            assert_eq!(output, tc.expected, "failed test case name: {}", tc.name)
        }

        Ok(())
    }
}
