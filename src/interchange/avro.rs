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

use byteorder::{BigEndian, ByteOrder, NetworkEndian, WriteBytesExt};
use chrono::Timelike;
use failure::bail;
use futures::executor::block_on;
use itertools::Itertools;
use serde_json::json;
use sha2::Sha256;
use url::Url;

use avro::schema::{
    resolve_schemas, Schema, SchemaFingerprint, SchemaNode, SchemaNodeOrNamed, SchemaPiece,
    SchemaPieceOrNamed,
};
use avro::types::{DecimalValue, Value};
use ore::collections::CollectionExt;
use repr::decimal::{Significand, MAX_DECIMAL_PRECISION};
use repr::jsonb::Jsonb;
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
    let key_desc = validate_schema_1(key_schema.top_node())?;
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
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EnvelopeType {
    None,
    Debezium,
    Upsert,
}

/// Converts an Apache Avro schema into a [`repr::RelationDesc`].
pub fn validate_value_schema(schema: &str, envelope: EnvelopeType) -> Result<RelationDesc> {
    let schema = parse_schema(schema)?;
    let node = schema.top_node();

    let row_schema = match envelope {
        EnvelopeType::Debezium => {
            // The top-level record needs to be a diff "envelope" that contains
            // `before` and `after` fields, where the `before` and `after` fields
            // have the same schema.
            let row_schema = match node.inner {
                SchemaPiece::Record { fields, .. } => {
                    let before = fields.iter().find(|f| f.name == "before");
                    let after = fields.iter().find(|f| f.name == "after");
                    match (before, after) {
                        (Some(before), Some(after)) => {
                            let left = node.step(&before.schema);
                            let right = node.step(&after.schema);
                            if let Some((left, right)) = first_mismatched_schema_types(left, right)
                            {
                                bail!(
                                "source schema has mismatched 'before' and 'after' schemas: before={:?} after={:?}",
                                left.inner,
                                right.inner
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
            match row_schema.get_piece_and_name(&schema).0 {
                SchemaPiece::Union(us) => {
                    if us.variants().len() != 2 {
                        bail!("source schema 'before'/'after' fields are not of expected type");
                    }
                    let has_null = us.variants().iter().any(|s| is_null(s));
                    let record =
                        us.variants()
                            .iter()
                            .find(|s| match s.get_piece_and_name(&schema).0 {
                                SchemaPiece::Record { .. } => true,
                                _ => false,
                            });
                    if !has_null {
                        bail!("source schema has non-nullable 'before'/'after' fields");
                    }
                    match record {
                        Some(record) => record,
                        None => {
                            bail!("source schema 'before/'after' fields are not of expected type")
                        }
                    }
                }
                _ => bail!("source schema has non-nullable 'before'/'after' fields"),
            }
        }
        EnvelopeType::Upsert => match node.inner {
            SchemaPiece::Union(us) => {
                if us.variants().len() > 2 {
                    bail!(
                        "upsert schema can only be record or union[null, record], got: {:?}",
                        schema.top
                    );
                }
                let has_null = us.variants().iter().any(|s| is_null(s));
                let record = us
                    .variants()
                    .iter()
                    .find(|s| match s.get_piece_and_name(&schema).0 {
                        SchemaPiece::Record { .. } => true,
                        _ => false,
                    });
                if !has_null {
                    bail!(
                        "upsert schema can only be record or union[null, record], got: {:?}",
                        schema.top
                    );
                }
                match record {
                    Some(record) => record,
                    None => bail!(
                        "upsert schema can only be record or union[null, record], got: {:?}",
                        schema.top
                    ),
                }
            }
            SchemaPiece::Record { .. } => &schema.top,
            _ => bail!(
                "upsert schema can only be record or union[null, record], got: {:?}",
                schema.top
            ),
        },
        EnvelopeType::None => &schema.top,
    };

    // The diff envelope is sane. Convert the actual record schema for the row.
    validate_schema_1(node.step(row_schema))
}

fn validate_schema_1(schema: SchemaNode) -> Result<RelationDesc> {
    match schema.inner {
        SchemaPiece::Record { fields, .. } => {
            let column_types = fields
                .iter()
                .map(|f| {
                    Ok(ColumnType {
                        nullable: is_nullable(schema.step(&f.schema).inner),
                        scalar_type: validate_schema_2(schema.step(&f.schema))?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let column_names = fields.iter().map(|f| Some(f.name.clone()));
            Ok(RelationDesc::new(
                RelationType::new(column_types),
                column_names,
            ))
        }
        _ => bail!("row schemas must be records, got: {:?}", schema.inner),
    }
}

fn validate_schema_2(schema: SchemaNode) -> Result<ScalarType> {
    Ok(match schema.inner {
        SchemaPiece::Null => ScalarType::Unknown,
        SchemaPiece::Boolean => ScalarType::Bool,
        SchemaPiece::Int => ScalarType::Int32,
        SchemaPiece::Long => ScalarType::Int64,
        SchemaPiece::Float => ScalarType::Float32,
        SchemaPiece::Double => ScalarType::Float64,
        SchemaPiece::Date => ScalarType::Date,
        SchemaPiece::TimestampMilli => ScalarType::Timestamp,
        SchemaPiece::TimestampMicro => ScalarType::Timestamp,
        SchemaPiece::Decimal {
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
        SchemaPiece::Bytes | SchemaPiece::Fixed { .. } => ScalarType::Bytes,
        SchemaPiece::String | SchemaPiece::Enum { .. } => ScalarType::String,

        SchemaPiece::Union(us) => {
            let utypes: Vec<_> = us
                .variants()
                .iter()
                // Null variants are handled by is_nullable, which makes
                // the entire union nullable in the presence of a null
                // variant.
                .filter(|s| !is_null(s))
                .map(|s| {
                    Ok(ColumnType {
                        nullable: is_nullable(schema.step(s).inner),
                        scalar_type: validate_schema_2(schema.step(s))?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            if utypes.len() == 1 {
                utypes.into_element().scalar_type
            } else {
                bail!("Unsupported union type: {:?}", schema.inner)
            }
        }
        SchemaPiece::Json => ScalarType::Jsonb,

        _ => bail!("Unsupported type in schema: {:?}", schema.inner),
    })
}

pub fn parse_schema(schema: &str) -> Result<Schema> {
    let schema = serde_json::from_str(schema)?;
    Schema::parse(&schema)
}

fn is_nullable(schema: &SchemaPiece) -> bool {
    match schema {
        SchemaPiece::Null => true,
        SchemaPiece::Union(us) => us.variants().iter().any(|v| is_null(v)),
        _ => false,
    }
}

fn is_null(schema: &SchemaPieceOrNamed) -> bool {
    match schema {
        SchemaPieceOrNamed::Piece(SchemaPiece::Null) => true,
        _ => false,
    }
}

/// Return None if they are equal, otherwise return the first mismatched schema
fn first_mismatched_schema_types<'a>(
    a: SchemaNode<'a>,
    b: SchemaNode<'a>,
) -> Option<(SchemaNode<'a>, SchemaNode<'a>)> {
    match (a.inner, b.inner) {
        (SchemaPiece::Null, SchemaPiece::Null) => None,
        (SchemaPiece::Boolean, SchemaPiece::Boolean) => None,
        (SchemaPiece::Int, SchemaPiece::Int) => None,
        (SchemaPiece::Long, SchemaPiece::Long) => None,
        (SchemaPiece::Float, SchemaPiece::Float) => None,
        (SchemaPiece::Double, SchemaPiece::Double) => None,
        (SchemaPiece::Bytes, SchemaPiece::Bytes) => None,
        (SchemaPiece::Date, SchemaPiece::Date) => None,
        (SchemaPiece::TimestampMilli, SchemaPiece::TimestampMilli) => None,
        (SchemaPiece::TimestampMicro, SchemaPiece::TimestampMicro) => None,
        (
            SchemaPiece::Decimal {
                precision: p1,
                scale: s1,
                fixed_size: f1,
            },
            SchemaPiece::Decimal {
                precision: p2,
                scale: s2,
                fixed_size: f2,
            },
        ) if p1 == p2 && s1 == s2 && f1 == f2 => None,
        (SchemaPiece::String, SchemaPiece::String) => None,
        (SchemaPiece::Array(ai), SchemaPiece::Array(bi))
        | (SchemaPiece::Map(ai), SchemaPiece::Map(bi)) => {
            first_mismatched_schema_types(a.step(&**ai), b.step(&**bi))
        }
        (SchemaPiece::Union(ai), SchemaPiece::Union(bi)) => ai
            .variants()
            .iter()
            .zip(bi.variants())
            .flat_map(|(ai, bi)| first_mismatched_schema_types(a.step(ai), b.step(bi)))
            .next(),
        (SchemaPiece::Record { fields: ai, .. }, SchemaPiece::Record { fields: bi, .. }) => ai
            .iter()
            .zip(bi.iter())
            .flat_map(|(ai, bi)| {
                first_mismatched_schema_types(a.step(&ai.schema), b.step(&bi.schema))
            })
            .next(),
        (SchemaPiece::Enum { symbols: ai, .. }, SchemaPiece::Enum { symbols: bi, .. })
            if ai == bi =>
        {
            None
        }
        (SchemaPiece::Fixed { size: ai }, SchemaPiece::Fixed { size: bi }) if ai == bi => None,
        (SchemaPiece::Json, SchemaPiece::Json) => None,
        _ => Some((a, b)),
    }
}

fn pack_value(v: Value, mut row: RowPacker) -> Result<RowPacker> {
    match v {
        Value::Null => row.push(Datum::Null),
        Value::Boolean(true) => row.push(Datum::True),
        Value::Boolean(false) => row.push(Datum::False),
        Value::Int(i) => row.push(Datum::Int32(i)),
        Value::Long(i) => row.push(Datum::Int64(i)),
        Value::Float(f) => row.push(Datum::Float32((f).into())),
        Value::Double(f) => row.push(Datum::Float64((f).into())),
        Value::Date(d) => row.push(Datum::Date(d)),
        Value::Timestamp(d) => row.push(Datum::Timestamp(d)),
        Value::Decimal(DecimalValue { unscaled, .. }) => row.push(Datum::Decimal(
            Significand::from_twos_complement_be(&unscaled)?,
        )),
        Value::Bytes(b) => row.push(Datum::Bytes(&b)),
        Value::String(s) => row.push(Datum::String(&s)),
        Value::Union(_, v) => {
            row = pack_value(*v, row)?;
        }
        Value::Json(j) => {
            row = Jsonb::new(j)?.pack_into(row);
        }
        other @ Value::Fixed(..)
        | other @ Value::Enum(..)
        | other @ Value::Array(_)
        | other @ Value::Map(_)
        | other @ Value::Record(_) => bail!("unsupported avro value: {:?}", other),
    };
    Ok(row)
}

pub fn extract_row<'a, I>(mut v: Value, strip_union: bool, extra: I) -> Result<Option<Row>>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    if strip_union {
        v = match v {
            Value::Union(_, v) => *v,
            _ => bail!("unsupported avro value: {:?}", v),
        }
    }
    match v {
        Value::Record(fields) => {
            let mut row = RowPacker::new();
            for (_, col) in fields.into_iter() {
                row = pack_value(col, row)?;
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

/// Extract a debezium-format Avro object by parsing it fully,
/// i.e., when the record isn't laid out such that we can extract the `before` and
/// `after` fields without decoding the entire record.
pub fn extract_debezium_slow(v: Value) -> Result<DiffPair<Row>> {
    let mut before = None;
    let mut after = None;
    match v {
        Value::Record(fields) => {
            for (name, val) in fields {
                if name == "before" {
                    before = extract_row(val, true, iter::once(Datum::Int64(-1)))?;
                } else if name == "after" {
                    after = extract_row(val, true, iter::once(Datum::Int64(1)))?;
                } else {
                    // Intentionally ignore other fields.
                }
            }
        }
        _ => bail!("avro envelope had unexpected type: {:?}", v),
    };
    Ok(DiffPair { before, after })
}

#[derive(Debug)]
pub struct DiffPair<T> {
    pub before: Option<T>,
    pub after: Option<T>,
}

/// Manages decoding of Avro-encoded bytes.
pub struct Decoder {
    reader_schema: Schema,
    writer_schemas: Option<SchemaCache>,
    fast_row_schema: Option<Schema>,
    is_debezium: bool,
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

// TODO (#2133) -- Avro coding can probably be made significantly faster.
impl Decoder {
    /// Creates a new `Decoder`
    ///
    /// The provided schema is called the "reader schema", which is the schema
    /// that we are expecting to use to decode records. The records may indicate
    /// that they are encoded with a different schema; as long as those.
    pub fn new(
        reader_schema: &str,
        schema_registry_url: Option<url::Url>,
        is_debezium: bool,
    ) -> Decoder {
        // It is assumed that the reader schema has already been verified
        // to be a valid Avro schema.
        let reader_schema = parse_schema(reader_schema).unwrap();
        let writer_schemas = schema_registry_url
            .map(|url| SchemaCache::new(url, reader_schema.fingerprint::<Sha256>()));

        let fast_row_schema = match reader_schema.top_node().inner {
            // If the first two fields in the record are `before` and `after`,
            // we don't need to decode the whole record. This can yield a
            // substantial performance win when there is additional heavyweight
            // metadata at the end of each record which would be immediately
            // discarded.
            SchemaPiece::Record { fields, .. }
                if fields[0].name == "before" && fields[1].name == "after" =>
            {
                let node = SchemaNodeOrNamed {
                    root: &reader_schema,
                    inner: fields[0].schema.as_ref(),
                };
                Some(node.to_schema())
            }
            _ => None,
        };

        Decoder {
            reader_schema,
            writer_schemas,
            fast_row_schema,
            is_debezium,
        }
    }

    /// Decodes Avro-encoded `bytes` into a `DiffPair`.
    pub fn decode(&mut self, mut bytes: &[u8]) -> Result<DiffPair<Row>> {
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
            bail!("wrong avro serialization magic: expected 0, got {}", magic);
        }

        let (resolved_schema, reader_schema) = match &mut self.writer_schemas {
            Some(cache) => match cache.get(schema_id, &self.reader_schema)? {
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

        let result = if self.is_debezium {
            if let (Some(schema), None) = (&self.fast_row_schema, reader_schema) {
                // The record is laid out such that we can extract the `before` and
                // `after` fields without decoding the entire record.
                let before = extract_row(
                    block_on(avro::from_avro_datum(&schema, &mut bytes))?,
                    true,
                    iter::once(Datum::Int64(-1)),
                )?;
                let after = extract_row(
                    block_on(avro::from_avro_datum(&schema, &mut bytes))?,
                    true,
                    iter::once(Datum::Int64(1)),
                )?;
                DiffPair { before, after }
            } else {
                let val = block_on(avro::from_avro_datum(resolved_schema, &mut bytes))?;
                extract_debezium_slow(val)?
            }
        } else {
            let val = block_on(avro::from_avro_datum(resolved_schema, &mut bytes))?;
            let row = extract_row(val, false, iter::empty())?;
            DiffPair {
                before: None,
                after: row,
            }
        };
        Ok(result)
    }
}

/// Builds an Avro schema that corresponds to `desc`.
///
/// Requires that all column names in `desc` are present. The returned schema
/// has some special properties to ease encoding:
///
///   * Union schemas are only used to represent nullability. The first
///     variant is always the null variant, and the second and last variant
///     is the non-null variant.
fn build_schema(desc: &RelationDesc) -> Schema {
    let mut fields = Vec::new();
    for (name, typ) in desc.iter() {
        let mut field_type = match typ.scalar_type {
            ScalarType::Unknown => json!("null"),
            ScalarType::Bool => json!("boolean"),
            ScalarType::Int32 => json!("int"),
            ScalarType::Int64 => json!("long"),
            ScalarType::Float32 => json!("float"),
            ScalarType::Float64 => json!("double"),
            ScalarType::Decimal(p, s) => json!({
                "type": "bytes",
                "logicalType": "decimal",
                "precision": p,
                "scale": s,
            }),
            ScalarType::Date => json!({
                "type": "int",
                "logicalType": "date",
            }),
            ScalarType::Time => json!({
                "type": "long",
                "logicalType": "time-micros",
            }),
            ScalarType::Timestamp | ScalarType::TimestampTz => json!({
                "type": "long",
                "connect.name": "io.debezium.time.MicroTimestamp",
                "logicalType": "timestamp-micros"
            }),
            ScalarType::Interval => json!({
                "type": "fixed",
                "size": 12,
                "logicalType": "duration"
            }),
            ScalarType::Bytes => json!("bytes"),
            ScalarType::String => json!("string"),
            ScalarType::Jsonb => json!({
                "type": "string",
                "connect.name": "io.debezium.data.Json",
            }),
        };
        if typ.nullable && typ.scalar_type != ScalarType::Unknown {
            field_type = json!(["null", field_type]);
        }
        fields.push(json!({
            "name": name.expect("function preconditions require name to be present"),
            "type": field_type,
        }));
    }
    let schema = json!({
        "type": "record",
        "name": "envelope",
        "fields": [
            {
                "name": "before",
                "type": [
                    "null",
                    {
                        "name": "row",
                        "type": "record",
                        "fields": fields,
                    }
                ]
            },
            {
                "name": "after",
                "type": ["null", "row"]
            }
        ]
    });
    Schema::parse(&schema).expect("valid schema constructed")
}

/// Manages encoding of Avro-encoded bytes.
pub struct Encoder {
    desc: RelationDesc,
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
    pub fn new(mut desc: RelationDesc) -> Self {
        // Invent names for columns that don't have a name.
        let missing_names: Vec<_> = desc
            .iter_names()
            .enumerate()
            .filter_map(|(i, name)| match name {
                Some(_) => None,
                None => Some(i),
            })
            .collect();
        for i in missing_names {
            desc.set_name(i, Some(format!("column{}", i).into()));
        }

        let writer_schema = build_schema(&desc);
        Encoder {
            desc,
            writer_schema,
        }
    }

    pub fn writer_schema(&self) -> &Schema {
        &self.writer_schema
    }

    /// Encodes a repr::Row to a Avro-compliant Vec<u8>.
    /// See function implementation for Confluent-specific details.
    pub fn encode(&self, schema_id: i32, diff_pair: DiffPair<&Row>) -> Vec<u8> {
        // The first byte is a magic byte (0) that indicates the Confluent
        // serialization format version, and the next four bytes are a
        // 32-bit schema ID.
        //
        // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
        let mut buf = Vec::new();
        buf.write_u8(0).expect("writing to vec cannot fail");
        buf.write_i32::<NetworkEndian>(schema_id)
            .expect("writing to vec cannot fail");
        avro::write_avro_datum(
            &self.writer_schema,
            self.diff_pair_to_avro(diff_pair),
            &mut buf,
        )
        .expect("schema constructed to match val");
        buf
    }

    pub fn diff_pair_to_avro(&self, diff_pair: DiffPair<&Row>) -> Value {
        let before = match diff_pair.before {
            None => Value::Union(0, Box::new(Value::Null)),
            Some(row) => {
                let row = self.row_to_avro(row.unpack());
                Value::Union(1, Box::new(row))
            }
        };
        let after = match diff_pair.after {
            None => Value::Union(0, Box::new(Value::Null)),
            Some(row) => {
                let row = self.row_to_avro(row.unpack());
                Value::Union(1, Box::new(row))
            }
        };
        Value::Record(vec![("before".into(), before), ("after".into(), after)])
    }

    fn row_to_avro(&self, row: Vec<Datum>) -> Value {
        let fields = self
            .desc
            .iter()
            .zip_eq(row)
            .map(|((name, typ), datum)| {
                let name = name.expect("name known to exist").as_str().to_owned();
                if typ.nullable && typ.scalar_type != ScalarType::Unknown && datum.is_null() {
                    return (name, Value::Union(0, Box::new(Value::Null)));
                }
                let mut val = match typ.scalar_type {
                    ScalarType::Unknown => Value::Null,
                    ScalarType::Bool => Value::Boolean(datum.unwrap_bool()),
                    ScalarType::Int32 => Value::Int(datum.unwrap_int32()),
                    ScalarType::Int64 => Value::Long(datum.unwrap_int64()),
                    ScalarType::Float32 => Value::Float(datum.unwrap_float32()),
                    ScalarType::Float64 => Value::Double(datum.unwrap_float64()),
                    ScalarType::Decimal(p, s) => Value::Decimal(DecimalValue {
                        unscaled: datum.unwrap_decimal().as_i128().to_be_bytes().to_vec(),
                        precision: p.into(),
                        scale: s.into(),
                    }),
                    ScalarType::Date => Value::Date(datum.unwrap_date()),
                    ScalarType::Time => Value::Long({
                        let time = datum.unwrap_time();
                        (time.num_seconds_from_midnight() * 1_000_000) as i64
                            + (time.nanosecond() as i64) / 1_000
                    }),
                    ScalarType::Timestamp => Value::Timestamp(datum.unwrap_timestamp()),
                    ScalarType::TimestampTz => {
                        Value::Timestamp(datum.unwrap_timestamptz().naive_utc())
                    }
                    ScalarType::Interval => Value::Fixed(12, {
                        let iv = datum.unwrap_interval();
                        let mut buf = Vec::with_capacity(12);
                        buf.extend(&(iv.months as i32).to_le_bytes());
                        buf.extend(&0i32.to_le_bytes());
                        buf.extend(&(iv.duration.as_millis() as i32).to_le_bytes());
                        debug_assert_eq!(buf.len(), 12);
                        buf
                    }),
                    ScalarType::Bytes => Value::Bytes(Vec::from(datum.unwrap_bytes())),
                    ScalarType::String => Value::String(datum.unwrap_str().to_owned()),
                    ScalarType::Jsonb => {
                        Value::Json(Jsonb::from_datum(datum.clone()).into_serde_json())
                    }
                };
                if typ.nullable && typ.scalar_type != ScalarType::Unknown {
                    val = Value::Union(1, Box::new(val));
                }
                (name, val)
            })
            .collect();
        Value::Record(fields)
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
    fn get(&mut self, id: i32, reader_schema: &Schema) -> Result<Option<&Schema>> {
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
                    let resolved = resolve_schemas(&schema, reader_schema)?;
                    Ok(v.insert(Some(resolved)).as_ref())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use failure::ResultExt;
    use ordered_float::OrderedFloat;
    use serde::Deserialize;
    use std::fs::File;

    use avro::types::{DecimalValue, Value};
    use repr::decimal::Significand;
    use repr::{Datum, RelationDesc};

    use super::*;

    #[derive(Deserialize)]
    struct TestCase {
        name: String,
        input: serde_json::Value,
        expected: RelationDesc,
    }

    #[test]
    #[ignore] // TODO(benesch): update tests for diff envelope.
    fn test_schema_parsing() -> Result<()> {
        let file = File::open("interchange/testdata/avro-schema.json")
            .context("opening test data file")?;
        let test_cases: Vec<TestCase> =
            serde_json::from_reader(file).context("parsing JSON test data")?;

        for tc in test_cases {
            // Stringifying the JSON we just parsed is rather silly, but it
            // avoids embedding JSON strings inside of JSON, which is hard on
            // the eyes.
            let schema = serde_json::to_string(&tc.input)?;
            let output = super::validate_value_schema(&schema, EnvelopeType::Debezium)?;
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
    fn test_diff_pair_to_avro_primitive_types() -> Result<()> {
        // Data to be used later in assertions.
        let date = NaiveDate::from_ymd(2020, 1, 8);
        let date_time = NaiveDateTime::new(date, NaiveTime::from_hms(1, 1, 1));
        let bytes: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let string = String::from("test");

        // Simple transformations from primitive Avro Schema types
        // to Avro Values.
        let valid_pairings = vec![
            (ScalarType::Unknown, Datum::Null, Value::Null),
            (ScalarType::Bool, Datum::True, Value::Boolean(true)),
            (ScalarType::Bool, Datum::False, Value::Boolean(false)),
            (ScalarType::Int32, Datum::Int32(1), Value::Int(1)),
            (ScalarType::Int64, Datum::Int64(1), Value::Long(1)),
            (
                ScalarType::Float32,
                Datum::Float32(OrderedFloat::from(1f32)),
                Value::Float(1f32),
            ),
            (
                ScalarType::Float64,
                Datum::Float64(OrderedFloat::from(1f64)),
                Value::Double(1f64),
            ),
            (ScalarType::Date, Datum::Date(date), Value::Date(date)),
            (
                ScalarType::Timestamp,
                Datum::Timestamp(date_time),
                Value::Timestamp(date_time),
            ),
            (
                ScalarType::TimestampTz,
                Datum::TimestampTz(DateTime::from_utc(date_time, Utc)),
                Value::Timestamp(date_time),
            ),
            (
                ScalarType::Decimal(1, 1),
                Datum::Decimal(Significand::new(1i128)),
                Value::Decimal(DecimalValue {
                    unscaled: bytes.clone(),
                    precision: 1,
                    scale: 1,
                }),
            ),
            (
                ScalarType::Bytes,
                Datum::Bytes(&bytes),
                Value::Bytes(bytes.clone()),
            ),
            (
                ScalarType::String,
                Datum::String(&string),
                Value::String(string.clone()),
            ),
        ];
        for (typ, datum, expected) in valid_pairings {
            let desc = RelationDesc::new(
                RelationType::new(vec![ColumnType::new(typ)]),
                vec!["column1".into()],
            );
            let avro_value = Encoder::new(desc).row_to_avro(vec![datum]);
            assert_eq!(
                Value::Record(vec![("column1".into(), expected)]),
                avro_value
            );
        }

        Ok(())
    }
}
