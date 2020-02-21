// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::iter;

use avro_rs::schema::{RecordField, Schema, SchemaFingerprint, UnionSchema};
use avro_rs::types::Value;
use byteorder::{BigEndian, ByteOrder, NetworkEndian, WriteBytesExt};
use failure::bail;
use serde_json::json;

use sha2::Sha256;
use url::Url;

use ore::collections::CollectionExt;
use repr::decimal::{Significand, MAX_DECIMAL_PRECISION};
use repr::{ColumnType, Datum, RelationDesc, RelationType, Row, RowPacker, ScalarType};

use crate::error::Result;

/// Validates an Avro key schema for use as a source.
///
/// An Avro key schema is valid for our purposes iff every field
/// mentioned in the key schema exists in the specified relation
/// type with the same type. If the schema is valid, returns a
/// vector describing the order and position of the primary key
/// columns.
pub fn validate_key_schema(key_schema: &str, value_desc: &RelationDesc) -> Result<Vec<usize>> {
    let key_schema = parse_schema(key_schema)?;
    let key_desc = validate_schema_1(&key_schema)?;
    let mut indices = Vec::new();
    for (name, key_type) in key_desc.iter() {
        if let Some(name) = name {
            match value_desc.get_by_name(name) {
                Some((index, value_type)) if key_type == value_type => {
                    indices.push(index);
                }
                Some((_, value_type)) => bail!(
                    "key and value column types do not match: key {:?} vs. value {:?}",
                    key_type,
                    value_type,
                ),
                None => bail!("Value schema missing primary key column: {}", name),
            }
        }
    }
    Ok(indices)
}

/// Converts an Apache Avro schema into a [`repr::RelationDesc`].
pub fn validate_value_schema(schema: &str) -> Result<RelationDesc> {
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

fn validate_schema_1(schema: &Schema) -> Result<RelationDesc> {
    match schema {
        Schema::Record { fields, .. } => {
            let column_types = fields
                .iter()
                .map(|f| {
                    Ok(ColumnType {
                        nullable: is_nullable(&f.schema),
                        scalar_type: validate_schema_2(&f.schema)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let column_names = fields.iter().map(|f| Some(f.name.clone()));
            Ok(RelationDesc::new(
                RelationType::new(column_types),
                column_names,
            ))
        }
        _ => bail!("row schemas must be records, got {:?}", schema),
    }
}

fn validate_schema_2(schema: &Schema) -> Result<ScalarType> {
    Ok(match schema {
        Schema::Null => ScalarType::Unknown,
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
                        nullable: is_nullable(s),
                        scalar_type: validate_schema_2(s)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

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

pub fn parse_schema(schema: &str) -> Result<Schema> {
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
    pub before: Option<Row>,
    pub after: Option<Row>,
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
    pub fn decode(&mut self, mut bytes: &[u8]) -> Result<DiffPair> {
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

        fn value_to_datum(v: &Value) -> Result<Datum<'_>> {
            match v {
                Value::Null => Ok(Datum::Null),
                Value::Boolean(true) => Ok(Datum::True),
                Value::Boolean(false) => Ok(Datum::False),
                Value::Int(i) => Ok(Datum::Int32(*i)),
                Value::Long(i) => Ok(Datum::Int64(*i)),
                Value::Float(f) => Ok(Datum::Float32((*f).into())),
                Value::Double(f) => Ok(Datum::Float64((*f).into())),
                Value::Date(d) => Ok(Datum::Date(*d)),
                Value::Timestamp(d) => Ok(Datum::Timestamp(*d)),
                Value::Decimal { unscaled, .. } => Ok(Datum::Decimal(
                    Significand::from_twos_complement_be(&unscaled)?,
                )),
                Value::Bytes(b) => Ok(Datum::Bytes(b)),
                Value::String(s) => Ok(Datum::String(s)),
                Value::Union(v) => value_to_datum(v),
                other @ Value::Fixed(..)
                | other @ Value::Enum(..)
                | other @ Value::Array(_)
                | other @ Value::Map(_)
                | other @ Value::Record(_) => bail!("unsupported avro value: {:?}", other),
            }
        };

        fn extract_row<'a, I>(v: Value, extra: I) -> Result<Option<Row>>
        where
            I: IntoIterator<Item = Datum<'a>>,
        {
            let v = match v {
                Value::Union(v) => *v,
                _ => bail!("unsupported avro value: {:?}", v),
            };
            match v {
                Value::Record(fields) => {
                    let mut row = RowPacker::new();
                    for (_, col) in fields.iter() {
                        row.push(value_to_datum(col)?);
                    }
                    for d in extra {
                        row.push(d);
                    }
                    Ok(Some(row.finish()))
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
            before = extract_row(
                avro_rs::from_avro_datum(&schema, &mut bytes, None)?,
                iter::once(Datum::Int64(-1)),
            )?;
            after = extract_row(
                avro_rs::from_avro_datum(&schema, &mut bytes, None)?,
                iter::once(Datum::Int64(1)),
            )?;
        } else {
            let val = avro_rs::from_avro_datum(writer_schema, &mut bytes, reader_schema)?;
            match val {
                Value::Record(fields) => {
                    for (name, val) in fields {
                        if name == "before" {
                            before = extract_row(val, iter::once(Datum::Int64(-1)))?;
                        } else if name == "after" {
                            after = extract_row(val, iter::once(Datum::Int64(1)))?;
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

pub fn encode_schema(desc: &RelationDesc) -> Result<serde_json::Value> {
    let mut fields = Vec::new();
    for (name, typ) in desc.iter() {
        let field_name = match name {
            Some(name) => name.as_str(),
            None => bail!("All Kafka sink columns must have a name."),
        };

        // todo@jldlaughlin: Support all ScalarTypes #1517
        let field_type = match typ.scalar_type {
            ScalarType::Unknown => "null",
            ScalarType::Bool => "boolean",
            ScalarType::Int32 => "int",
            ScalarType::Int64 => "long",
            ScalarType::Float32 => "float",
            ScalarType::Float64 => "double",
            ScalarType::Decimal(_, _) => "decimal",
            ScalarType::Date => "date",
            //            ScalarType::Timestamp => ,
            //            ScalarType::TimestampTz => ,
            ScalarType::Interval => "duration",
            ScalarType::Bytes => "bytes",
            ScalarType::String => "string",
            //            ScalarType::Jsonb => ,
            _ => bail!(
                "Do not support schemas with field type: {:#?}",
                typ.scalar_type
            ),
        };
        let field_types = match typ.nullable {
            true => json!({
                "name": field_name,
                "type": ["null", field_type],
            }),
            false => json!({
                "name": field_name,
                "type": field_type, // Want a string, not a list!
            }),
        };
        fields.push(field_types);
    }

    // Add before and after wrapper.
    Ok(json!({
        "type": "record",
        "name": "envelope",
        "fields":
            [
                {"name": "before",
                 "type": [
                    {
                        "name": "row",
                        "type": "record",
                        "fields": fields,
                    },
                        "null"
                  ]},
                {"name": "after",
                 "type": [ "row",  "null" ]}
            ]
    }))
}

/// Manages encoding of Avro-encoded bytes.
pub struct Encoder {
    writer_schema: Schema,
}

impl fmt::Debug for Encoder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Encoder")
            .field("writer_schema", &self.writer_schema)
            .finish()
    }
}

impl Encoder {
    pub fn new(raw_schema: &str) -> Self {
        let writer_schema = parse_schema(raw_schema).unwrap();
        Encoder { writer_schema }
    }

    /// Encodes a repr::Row to a Avro-compliant Vec<u8>.
    /// See function implementation for Confluent-specific details.
    pub fn encode(&self, schema_id: i32, row: &Row) -> Vec<u8> {
        // The first byte is a magic byte (0) that indicates the Confluent
        // serialization format version, and the next four bytes are a
        // 32-bit schema ID.
        //
        // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
        let mut buf = Vec::new();
        buf.write_u8(0).unwrap();
        buf.write_i32::<NetworkEndian>(schema_id).unwrap();
        buf.extend(self.row_to_avro(row).unwrap());
        buf
    }

    fn row_to_avro(&self, row: &Row) -> Result<Vec<u8>> {
        match &self.writer_schema {
            Schema::Record { fields, .. } => match fields.as_slice() {
                [before, _after] => {
                    let avro_val = Self::data_to_avro(&before.schema, &row.unpack())?;
                    // Add wrapper Record with before and after RecordFields
                    let wrapped_avro_val = Value::Record(vec![
                        ("before".into(), Value::Union(Box::from(Value::Null))),
                        ("after".into(), Value::Union(Box::from(avro_val))),
                    ]);
                    avro_rs::to_avro_datum(&self.writer_schema, wrapped_avro_val)
                }
                _ => bail!("Expected schema to contain before and after fields."),
            },
            _ => bail!("Expected schema to be wrapped in a Schema::Record"),
        }
    }

    fn data_to_avro(record_schema: &Schema, data: &[Datum]) -> Result<Value> {
        Ok(match data {
            [] => bail!("Expected to convert Datum to type {:#?}, but no Datum found."),
            [datum] => {
                match record_schema {
                    Schema::Null => match datum {
                        Datum::Null => Value::Null,
                        _ => bail!(
                            "Schema expected Datum to be Null, Datum was non-Null type: {:#?}.",
                            datum
                        ),
                    },
                    Schema::Boolean => Value::Boolean(datum.unwrap_bool()),
                    Schema::Int => Value::Int(datum.unwrap_int32()),
                    Schema::Long => Value::Long(datum.unwrap_int64()),
                    Schema::Float => Value::Float(datum.unwrap_float32()),
                    Schema::Double => Value::Double(datum.unwrap_float64()),
                    Schema::Date => Value::Date(datum.unwrap_date()),
                    Schema::TimestampMilli => Value::Timestamp(datum.unwrap_timestamp()),
                    Schema::TimestampMicro => Value::Timestamp(datum.unwrap_timestamp()),
                    Schema::Decimal {
                        precision, scale, ..
                    } => Value::Decimal {
                        unscaled: datum.unwrap_decimal().as_i128().to_be_bytes().to_vec(),
                        precision: precision.clone(),
                        scale: scale.clone(),
                    },
                    Schema::Bytes => Value::Bytes(Vec::from(datum.unwrap_bytes())),
                    Schema::String => Value::String(String::from(datum.unwrap_str())),
                    Schema::Array(array) => {
                        let mut value_array = Vec::new();
                        for d in datum.unwrap_list().iter() {
                            value_array.push(Self::data_to_avro(&*array, &[d]).unwrap())
                        }
                        Value::Array(value_array)
                    }
                    Schema::Map(map) => {
                        let mut value_map = HashMap::new();
                        for (key, datum) in datum.unwrap_dict().iter() {
                            value_map.insert(
                                String::from(key),
                                Self::data_to_avro(&*map, &[datum]).unwrap(),
                            );
                        }
                        Value::Map(value_map)
                    }
                    Schema::Enum { symbols, .. } => {
                        let symbol = datum.unwrap_str();
                        let position = symbols.iter().position(|s| s == symbol);
                        match position {
                            Some(p) => Value::Enum(p as i32, String::from(symbol)),
                            None => bail!(
                                "Datum has value {:#?}, not found in Enum symbols: {:#?}",
                                symbol,
                                symbols
                            ),
                        }
                    }
                    Schema::Fixed { size, .. } => {
                        Value::Fixed(*size, Vec::from(datum.unwrap_bytes()))
                    }
                    // Schema::Union and Schema::Record can serialize >= 1 Datums
                    Schema::Union(union) => Self::convert_to_avro_union(data, union)?,
                    Schema::Record { fields, .. } => Self::convert_to_avro_record(data, fields)?,
                }
            }
            _ => match record_schema {
                // Schema::Union and Schema::Record can serialize >= 1 Datums
                Schema::Union(union) => Self::convert_to_avro_union(data, union)?,
                Schema::Record { fields, .. } => Self::convert_to_avro_record(data, fields)?,
                _ => bail!(
                    "Expected to convert Datum to type {:#?}, but more than one Datum found.",
                    record_schema
                ),
            },
        })
    }

    fn convert_to_avro_record(data: &[Datum], fields: &[RecordField]) -> Result<Value> {
        let mut vals = Vec::new();
        for (rf, datum) in fields.iter().zip(data) {
            match rf {
                avro_rs::schema::RecordField { name, schema, .. } => {
                    vals.push((String::from(name), Self::data_to_avro(schema, &[*datum])?));
                }
            }
        }
        Ok(Value::Record(vals))
    }

    fn convert_to_avro_union(data: &[Datum], union: &UnionSchema) -> Result<Value> {
        let mut value = None;
        for s in union.variants() {
            if let Ok(v) = Self::data_to_avro(s, data) {
                value = Some(v)
            }
        }
        match value {
            Some(v) => Ok(v),
            None => bail!("Unable to parse Datum into any Avro schema options."),
        }
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
    fn get(&mut self, id: i32) -> Result<Option<&Schema>> {
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
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use failure::ResultExt;
    use ordered_float::OrderedFloat;
    use pretty_assertions::assert_eq;
    use serde::Deserialize;
    use std::fs::File;

    use avro_rs::schema::Schema;
    use avro_rs::types::Value;
    use repr::decimal::Significand;
    use repr::{Datum, RelationDesc};

    #[derive(Deserialize)]
    struct TestCase {
        name: String,
        input: serde_json::Value,
        expected: RelationDesc,
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

    #[test]
    /// Test that primitive Avro Schema types are allow Datums to be correctly
    /// serialized into Avro Values.
    ///
    /// Complete list of primitive types in test, also found in this
    /// documentation:
    /// https://avro.apache.org/docs/current/spec.html#schemas
    fn test_row_to_avro_primitive_types() -> Result<(), failure::Error> {
        //        // The Encoder's schema is not used in data_to_avro(), use simple mock instead.
        //        let dummy_relation_desc = RelationDesc::empty();
        //        let schema = super::encode_schema(&dummy_relation_desc)?;
        //
        //        let encoder = super::Encoder::new(&schema.to_string());

        // Data to be used later in assertions.
        let date = NaiveDate::from_ymd(2020, 1, 8);
        let date_time = NaiveDateTime::new(date, NaiveTime::from_hms(1, 1, 1));
        let bytes: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let string = String::from("test");

        // Simple transformations from primitive Avro Schema types
        // to Avro Values.
        let valid_pairings = [
            (Schema::Null, Datum::Null, Value::Null),
            (Schema::Boolean, Datum::True, Value::Boolean(true)),
            (Schema::Boolean, Datum::False, Value::Boolean(false)),
            (Schema::Int, Datum::Int32(1), Value::Int(1)),
            (Schema::Long, Datum::Int64(1), Value::Long(1)),
            (
                Schema::Float,
                Datum::Float32(OrderedFloat::from(1f32)),
                Value::Float(1f32),
            ),
            (
                Schema::Double,
                Datum::Float64(OrderedFloat::from(1f64)),
                Value::Double(1f64),
            ),
            (Schema::Date, Datum::Date(date), Value::Date(date)),
            (
                Schema::TimestampMilli,
                Datum::Timestamp(date_time),
                Value::Timestamp(date_time),
            ),
            (
                Schema::TimestampMicro,
                Datum::Timestamp(date_time),
                Value::Timestamp(date_time),
            ),
            (
                Schema::Decimal {
                    precision: 1usize,
                    scale: 1usize,
                    fixed_size: None,
                },
                Datum::Decimal(Significand::new(1i128)),
                Value::Decimal {
                    unscaled: bytes.clone(),
                    precision: 1,
                    scale: 1,
                },
            ),
            (
                Schema::Bytes,
                Datum::Bytes(&bytes),
                Value::Bytes(bytes.clone()),
            ),
            (
                Schema::String,
                Datum::String(&string),
                Value::String(string.clone()),
            ),
        ];
        for (s, d, expected) in valid_pairings.iter() {
            let avro_value = super::Encoder::data_to_avro(&s, &[*d])?;
            assert_eq!(*expected, avro_value);
        }

        Ok(())
    }
}
