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
use std::convert::TryFrom;
use std::fmt;
use std::iter;

use byteorder::{BigEndian, ByteOrder, NetworkEndian, WriteBytesExt};
use chrono::Timelike;
use failure::{bail, format_err};
use itertools::Itertools;
use log::warn;
use serde_json::json;
use sha2::Sha256;

use avro::schema::{
    resolve_schemas, RecordField, Schema, SchemaFingerprint, SchemaNode, SchemaPiece,
    SchemaPieceOrNamed,
};
use avro::types::{DecimalValue, Value};
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
            SchemaPiece::Record { .. } => &schema.top,
            _ => bail!("upsert schema can only be record, got: {:?}", schema.top),
        },
        EnvelopeType::None => &schema.top,
    };

    // The diff envelope is sane. Convert the actual record schema for the row.
    validate_schema_1(node.step(row_schema))
}

fn validate_schema_1(schema: SchemaNode) -> Result<RelationDesc> {
    match schema.inner {
        SchemaPiece::Record { fields, .. } => {
            let mut column_types = vec![];
            for f in fields {
                if let SchemaPiece::Union(us) = schema.step(&f.schema).inner {
                    if us.variants().is_empty()
                        || (us.variants().len() == 1 && is_null(&us.variants()[0]))
                    {
                        bail!(format_err!("Empty or null-only unions are not supported"));
                    } else {
                        let nullable = us.variants().len() > 1;
                        for v in us.variants() {
                            if !is_null(v) {
                                let node = schema.step(v);
                                if let SchemaPiece::Union(_) = node.inner {
                                    unreachable!("Internal error: directly nested avro union!");
                                }
                                column_types.push(ColumnType {
                                    nullable,
                                    scalar_type: validate_schema_2(node)?,
                                });
                            }
                        }
                    }
                } else {
                    let scalar_type = validate_schema_2(schema.step(&f.schema))?;
                    column_types.push(ColumnType {
                        nullable: false,
                        scalar_type,
                    });
                }
            }
            let mut column_names = vec![];
            for f in fields {
                if let SchemaPiece::Union(us) = schema.step(&f.schema).inner {
                    let vs = us.variants();
                    if vs.len() == 1 || (vs.len() == 2 && vs.iter().any(is_null)) {
                        column_names.push(Some(f.name.clone()));
                    } else {
                        for (i, v) in vs.iter().enumerate() {
                            if !is_null(v) {
                                column_names.push(Some(format!("{}{}", &f.name, i + 1)));
                            }
                        }
                    }
                } else {
                    column_names.push(Some(f.name.clone()));
                }
            }
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

        SchemaPiece::Json => ScalarType::Jsonb,

        _ => bail!("Unsupported type in schema: {:?}", schema.inner),
    })
}

pub fn parse_schema(schema: &str) -> Result<Schema> {
    let schema = serde_json::from_str(schema)?;
    Schema::parse(&schema)
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

fn pack_value(v: Value, mut row: RowPacker, n: SchemaNode) -> Result<RowPacker> {
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
        Value::String(s) | Value::Enum(_ /* idx */, s) => row.push(Datum::String(&s)),
        Value::Union(idx, v) => {
            let mut v = Some(*v);
            if let SchemaPiece::Union(us) = n.inner {
                for (var_idx, var_s) in us
                    .variants()
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| !is_null(s))
                {
                    if var_idx == idx {
                        let next = n.step(var_s);
                        row = pack_value(v.take().unwrap(), row, next)?;
                    } else {
                        row.push(Datum::Null);
                    }
                }
            } else {
                unreachable!("Avro value out of sync with schema");
            }
        }
        Value::Json(j) => {
            row = Jsonb::new(j)?.pack_into(row);
        }
        other @ Value::Fixed(..)
        | other @ Value::Array(_)
        | other @ Value::Map(_)
        | other @ Value::Record(_) => bail!("unsupported avro value: {:?}", other),
    };
    Ok(row)
}

pub fn extract_nullable_row<'a, I>(v: Value, extra: I, n: SchemaNode) -> Result<Option<Row>>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let (v, n) = match v {
        Value::Union(idx, v) => {
            let next = if let SchemaPiece::Union(us) = n.inner {
                n.step(&us.variants()[idx])
            } else {
                unreachable!("Avro value out of sync with schema")
            };
            (*v, next)
        }
        _ => bail!("unsupported avro value: {:?}", v),
    };
    extract_row(v, extra, n)
}

pub fn extract_row<'a, I>(v: Value, extra: I, n: SchemaNode) -> Result<Option<Row>>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    match v {
        Value::Record(fields) => match n.inner {
            SchemaPiece::Record {
                fields: schema_fields,
                ..
            } => {
                let mut row = RowPacker::new();
                for (i, (_, col)) in fields.into_iter().enumerate() {
                    let f_schema = &schema_fields[i].schema;
                    let f_node = n.step(f_schema);
                    row = pack_value(col, row, f_node)?;
                }
                for d in extra {
                    row.push(d);
                }
                Ok(Some(row.finish()))
            }
            _ => unreachable!("Avro value out of sync with schema"),
        },
        Value::Null => Ok(None),
        _ => bail!("unsupported avro value: {:?}", v),
    }
}

/// Extract a debezium-format Avro object by parsing it fully,
/// i.e., when the record isn't laid out such that we can extract the `before` and
/// `after` fields without decoding the entire record.

fn unwrap_record_fields(n: SchemaNode) -> &[RecordField] {
    if let SchemaPiece::Record { fields, .. } = n.inner {
        fields
    } else {
        panic!("node is not a record!");
    }
}

#[derive(Debug)]
pub struct DiffPair<T> {
    pub before: Option<T>,
    pub after: Option<T>,
}

#[derive(Debug, Clone, Copy)]
pub struct BinlogSchemaIndices {
    /// Index of the "source" field in the payload schema
    source_idx: usize,
    /// Index of the "file" field in the source schema
    source_file_idx: usize,
    /// Index of the "pos" field in the source schema
    source_pos_idx: usize,
    /// Index of the "row" field in the source schema
    source_row_idx: usize,
    /// Index of the "snapshot" field in the source schema
    source_snapshot_idx: usize,
}

impl BinlogSchemaIndices {
    pub fn new_from_schema(top_node: SchemaNode) -> Option<Self> {
        let top_indices = field_indices(top_node)?;
        let source_idx = *top_indices.get("source")?;
        let source_node = top_node.step(&unwrap_record_fields(top_node)[source_idx].schema);
        let source_indices = field_indices(source_node)?;
        let source_file_idx = *source_indices.get("file")?;
        let source_pos_idx = *source_indices.get("pos")?;
        let source_row_idx = *source_indices.get("row")?;
        let source_snapshot_idx = *source_indices.get("snapshot")?;

        Some(Self {
            source_idx,
            source_file_idx,
            source_pos_idx,
            source_row_idx,
            source_snapshot_idx,
        })
    }
}

/// Additional context needed for decoding
/// Debezium-formatted data.
#[derive(Debug)]
pub struct DebeziumDecodeState {
    /// Index of the "before" field in the payload schema
    before_idx: usize,
    /// Index of the "after" field in the payload schema
    after_idx: usize,
    // TODO - this is not a complete fix for #3026.
    // In particular, it doesn't help us with non-MySQL connectors,
    // nor with the initial scan.
    /// Last recorded offset (pos, row) for each MySQL binlog file.
    /// Messages that are not ahead of the last recorded offset will be skipped.
    binlog_offsets: HashMap<String, (usize, usize)>,
    binlog_schema_indices: Option<BinlogSchemaIndices>,
}

fn field_indices(node: SchemaNode) -> Option<HashMap<String, usize>> {
    if let SchemaPiece::Record { fields, .. } = node.inner {
        Some(
            fields
                .iter()
                .enumerate()
                .map(|(i, f)| (f.name.clone(), i))
                .collect(),
        )
    } else {
        None
    }
}

fn take_field_by_index(
    idx: usize,
    expected_name: &str,
    fields: &mut [(String, Value)],
) -> Result<Value> {
    let (name, value) = fields.get_mut(idx).ok_or_else(|| {
        format_err!(
            "Value does not match schema: \"{}\" field not at index {}",
            expected_name,
            idx
        )
    })?;
    if name != expected_name {
        bail!(
            "Value does not match schema: expected \"{}\", found \"{}\"",
            expected_name,
            name
        );
    }
    Ok(std::mem::replace(value, Value::Null))
}

impl DebeziumDecodeState {
    pub fn new_from_schema(schema: &Schema) -> Option<Self> {
        let top_node = schema.top_node();
        let top_indices = field_indices(top_node)?;
        let before_idx = *top_indices.get("before")?;
        let after_idx = *top_indices.get("after")?;
        let binlog_schema_indices = BinlogSchemaIndices::new_from_schema(top_node);

        Some(Self {
            before_idx,
            after_idx,
            binlog_offsets: Default::default(),
            binlog_schema_indices,
        })
    }

    pub fn extract(
        &mut self,
        v: Value,
        n: SchemaNode,
        coord: Option<i64>,
    ) -> Result<DiffPair<Row>> {
        fn is_snapshot(v: Value) -> Result<Option<bool>> {
            let answer = match v {
                Value::Union(_idx, inner) => is_snapshot(*inner)?,
                Value::Boolean(b) => Some(b),
                // Since https://issues.redhat.com/browse/DBZ-1295 ,
                // "snapshot" is three-valued. "last" is the last row
                // in the snapshot, but still part of it.
                Value::String(s) => Some(&s == "true" || &s == "last"),
                Value::Null => None,
                _ => bail!("\"snapshot\" is neither a boolean nor a string"),
            };
            Ok(answer)
        }

        match v {
            Value::Record(mut fields) => {
                if let Some(schema_indices) = self.binlog_schema_indices {
                    let source_val =
                        take_field_by_index(schema_indices.source_idx, "source", &mut fields)?;
                    let mut source_fields = match source_val {
                        Value::Record(fields) => fields,
                        _ => bail!("\"source\" is not a record: {:?}", source_val),
                    };
                    let snapshot_val = take_field_by_index(
                        schema_indices.source_snapshot_idx,
                        "snapshot",
                        &mut source_fields,
                    )?;

                    if is_snapshot(snapshot_val)? != Some(true) {
                        let file_val = take_field_by_index(
                            schema_indices.source_file_idx,
                            "file",
                            &mut source_fields,
                        )?
                        .into_string()
                        .ok_or_else(|| format_err!("\"file\" is not a string"))?;
                        let pos_val = take_field_by_index(
                            schema_indices.source_pos_idx,
                            "pos",
                            &mut source_fields,
                        )?
                        .into_integral()
                        .ok_or_else(|| format_err!("\"pos\" is not an integer"))?;
                        let row_val = take_field_by_index(
                            schema_indices.source_row_idx,
                            "row",
                            &mut source_fields,
                        )?
                        .into_integral()
                        .ok_or_else(|| format_err!("\"row\" is not an integer"))?;
                        let pos = usize::try_from(pos_val)?;
                        let row = usize::try_from(row_val)?;
                        match self.binlog_offsets.entry(file_val) {
                            Entry::Occupied(mut oe) => {
                                let (old_max_pos, old_max_row) = *oe.get();
                                if old_max_pos > pos || (old_max_pos == pos && old_max_row >= row) {
                                    let offset_string = if let Some(coord) = coord {
                                        format!(" at position {}", coord)
                                    } else {
                                        format!("")
                                    };
                                    warn!("Debezium did not advance: previously read ({}, {}), now read ({}, {}). Skipping record{}.",
                                          old_max_pos, old_max_row, pos, row, offset_string);
                                    return Ok(DiffPair {
                                        before: None,
                                        after: None,
                                    });
                                }
                                oe.insert((pos, row));
                            }
                            Entry::Vacant(ve) => {
                                ve.insert((pos as usize, row as usize));
                            }
                        }
                    }
                }
                let before_val = take_field_by_index(self.before_idx, "before", &mut fields)?;
                let after_val = take_field_by_index(self.after_idx, "after", &mut fields)?;
                // we will not have gotten this far if before/after aren't records, so the unwrap is okay.
                let before_node = n.step(&unwrap_record_fields(n)[self.before_idx].schema);
                let after_node = n.step(&unwrap_record_fields(n)[self.after_idx].schema);
                let before =
                    extract_nullable_row(before_val, iter::once(Datum::Int64(-1)), before_node)?;
                let after =
                    extract_nullable_row(after_val, iter::once(Datum::Int64(1)), after_node)?;
                Ok(DiffPair { before, after })
            }
            _ => bail!("avro envelope had unexpected type: {:?}", v),
        }
    }
}

/// Manages decoding of Avro-encoded bytes.
pub struct Decoder {
    reader_schema: Schema,
    writer_schemas: Option<SchemaCache>,
    envelope: EnvelopeType,
    debezium: Option<DebeziumDecodeState>,
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
        schema_registry: Option<ccsr::ClientConfig>,
        envelope: EnvelopeType,
    ) -> Result<Decoder> {
        // It is assumed that the reader schema has already been verified
        // to be a valid Avro schema.
        let reader_schema = parse_schema(reader_schema).unwrap();
        let writer_schemas =
            schema_registry.map(|sr| SchemaCache::new(sr, reader_schema.fingerprint::<Sha256>()));

        let debezium = if envelope == EnvelopeType::Debezium {
            Some(
                DebeziumDecodeState::new_from_schema(&reader_schema)
                    .ok_or_else(|| format_err!("Failed to extract Debezium schema information!"))?,
            )
        } else {
            None
        };
        Ok(Decoder {
            reader_schema,
            writer_schemas,
            envelope,
            debezium,
        })
    }

    /// Decodes Avro-encoded `bytes` into a `DiffPair`.
    pub async fn decode(&mut self, mut bytes: &[u8], coord: Option<i64>) -> Result<DiffPair<Row>> {
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

        let resolved_schema = match &mut self.writer_schemas {
            Some(cache) => cache
                .get(schema_id, &self.reader_schema)
                .await?
                .unwrap_or(&self.reader_schema),
            // If we haven't been asked to use a schema registry, we have no way
            // to discover the writer's schema. That's ok; we'll just use the
            // reader's schema and hope it lines up.
            None => &self.reader_schema,
        };

        let result = if self.envelope == EnvelopeType::Debezium {
            let dbz_state = self.debezium.as_mut().ok_or_else(|| {
                format_err!("Debezium schema extraction failed; can't decode message.")
            })?;
            let val = avro::from_avro_datum(resolved_schema, &mut bytes)?;
            dbz_state.extract(val, self.reader_schema.top_node(), coord)?
        } else {
            let val = avro::from_avro_datum(resolved_schema, &mut bytes)?;
            let row = extract_row(val, iter::empty(), self.reader_schema.top_node())?;
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
        let mut field_type = match &typ.scalar_type {
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
            ScalarType::List(_t) => unimplemented!("jamii/list"),
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

    pub fn encode_unchecked(&self, schema_id: i32, diff_pair: DiffPair<&Row>) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.write_u8(0).expect("writing to vec cannot fail");
        buf.write_i32::<NetworkEndian>(schema_id)
            .expect("writing to vec cannot fail");
        let avro = self.diff_pair_to_avro(diff_pair);
        debug_assert!(avro.validate(self.writer_schema.top_node()));
        avro::encode_unchecked(&avro, &self.writer_schema, &mut buf);
        buf
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
                let mut val = match &typ.scalar_type {
                    ScalarType::Unknown => Value::Null,
                    ScalarType::Bool => Value::Boolean(datum.unwrap_bool()),
                    ScalarType::Int32 => Value::Int(datum.unwrap_int32()),
                    ScalarType::Int64 => Value::Long(datum.unwrap_int64()),
                    ScalarType::Float32 => Value::Float(datum.unwrap_float32()),
                    ScalarType::Float64 => Value::Double(datum.unwrap_float64()),
                    ScalarType::Decimal(p, s) => Value::Decimal(DecimalValue {
                        unscaled: datum.unwrap_decimal().as_i128().to_be_bytes().to_vec(),
                        precision: (*p).into(),
                        scale: (*s).into(),
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
                    ScalarType::List(_t) => unimplemented!("jamii/list"),
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
    fn new(
        schema_registry: ccsr::ClientConfig,
        reader_fingerprint: SchemaFingerprint,
    ) -> SchemaCache {
        SchemaCache {
            cache: HashMap::new(),
            ccsr_client: schema_registry.build(),
            reader_fingerprint,
        }
    }

    /// Looks up the writer schema for ID. If the schema is literally identical
    /// to the reader schema, as determined by the reader schema fingerprint
    /// that this schema cache was initialized with, returns None.
    async fn get(&mut self, id: i32, reader_schema: &Schema) -> Result<Option<&Schema>> {
        match self.cache.entry(id) {
            Entry::Occupied(o) => Ok(o.into_mut().as_ref()),
            Entry::Vacant(v) => {
                // TODO(benesch): make this asynchronous, to avoid blocking the
                // Timely thread on this network request.
                let res = self.ccsr_client.get_schema_by_id(id).await?;
                let schema = parse_schema(&res.raw)?;
                if schema.fingerprint::<Sha256>().bytes == self.reader_fingerprint.bytes {
                    Ok(v.insert(None).as_ref())
                } else {
                    // the writer schema differs from the reader schema,
                    // so we need to perform schema resolution.
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
