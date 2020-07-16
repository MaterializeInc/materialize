// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt;
use std::io::Read;
use std::iter;

use byteorder::{BigEndian, ByteOrder, NetworkEndian, WriteBytesExt};
use chrono::Timelike;
use failure::{bail, format_err};
use itertools::Itertools;
use lazy_static::lazy_static;
use log::warn;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;

use avro::schema::{
    resolve_schemas, RecordField, Schema, SchemaFingerprint, SchemaNode, SchemaPiece,
    SchemaPieceOrNamed,
};
use avro::{
    give_value,
    types::{DecimalValue, Scalar, Value},
    AvroArrayAccess, AvroDecode, AvroDeserializer, AvroRead, AvroRecordAccess, GeneralDeserializer,
    TrivialDecoder, ValueOrReader,
};
use repr::adt::decimal::{Significand, MAX_DECIMAL_PRECISION};
use repr::adt::jsonb::{JsonbPacker, JsonbRef};
use repr::{ColumnName, ColumnType, Datum, RelationDesc, Row, RowPacker, ScalarType};

use crate::error::Result;
use ordered_float::OrderedFloat;
use smallvec::SmallVec;

lazy_static! {
    // TODO(rkhaitan): this schema intentionally omits the data_collections field
    // that is typically present in Debezium transaction metadata topics. See
    // https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-transaction-metadata
    // for more information. We chose to omit this field because it is redundant
    // for sinks where each consistency topic corresponds to exactly one sink.
    // We will need to add it in order to be able to reingest sinked topics.
    static ref DEBEZIUM_TRANSACTION_SCHEMA: Schema =
    Schema::parse(&json!({
        "type": "record",
        "name": "envelope",
        "fields": [
            {
                "name": "id",
                "type": "string"
            },
            {
                "name": "status",
                "type": "string"
            },
            {
                "name": "event_count",
                "type": [
                  "null",
                  "long"
                ]
            }
        ]
    })).expect("valid schema constructed");
}

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
    Ok(indices)
}
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EnvelopeType {
    None,
    Debezium,
    Upsert,
}

#[derive(Debug)]
pub struct DebeziumSourceCoordinates {
    snapshot: bool,
    pos: usize,
    row: usize,
}
struct DebeziumSourceDecoder<'a> {
    snapshot: Option<bool>,
    pos: Option<i64>,
    row: Option<i64>,
    file_buf: &'a mut Vec<u8>,
    has_file: bool,
}
impl<'a> DebeziumSourceDecoder<'a> {
    fn coords(self) -> Result<DebeziumSourceCoordinates> {
        let snapshot = self.snapshot.ok_or_else(|| format_err!("no snapshot"))?;
        let pos = self.pos.ok_or_else(|| format_err!("no pos"))? as usize;
        let row = self.row.ok_or_else(|| format_err!("no row"))? as usize;
        if !self.has_file {
            bail!("no file");
        }
        Ok(DebeziumSourceCoordinates { snapshot, pos, row })
    }
}

struct AvroStringDecoder<'a> {
    pub buf: &'a mut Vec<u8>,
    pub decoded: bool,
}

impl<'a> AvroStringDecoder<'a> {
    pub fn with_buf(buf: &'a mut Vec<u8>) -> Self {
        Self {
            buf,
            decoded: false,
        }
    }
}

impl<'a> AvroDecode for AvroStringDecoder<'a> {
    fn string<'b, R: AvroRead>(&mut self, r: ValueOrReader<'b, &'b str, R>) -> Result<()> {
        if self.decoded {
            bail!("Attempted to decode two strings");
        }
        self.decoded = true;
        match r {
            ValueOrReader::Value(val) => {
                self.buf.resize_with(val.len(), Default::default);
                val.as_bytes().read_exact(&mut self.buf)?;
            }
            ValueOrReader::Reader { len, r } => {
                self.buf.resize_with(len, Default::default);
                r.read_exact(&mut self.buf)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum DbzSnapshot {
    True,
    Last,
    False,
}

struct AvroDbzSnapshotDecoder(Option<DbzSnapshot>);

impl AvroDecode for AvroDbzSnapshotDecoder {
    fn union_branch<'a, R: AvroRead, D: AvroDeserializer>(
        &mut self,
        _idx: usize,
        _n_variants: usize,
        _null_variant: Option<usize>,
        deserializer: &'a mut D,
        reader: &'a mut R,
    ) -> Result<()> {
        deserializer.deserialize(reader, self)
    }
    fn scalar(&mut self, scalar: Scalar) -> Result<()> {
        match scalar {
            Scalar::Null => Ok(()),
            Scalar::Boolean(val) => {
                self.0 = Some(if val {
                    DbzSnapshot::True
                } else {
                    DbzSnapshot::False
                });
                Ok(())
            }
            _ => bail!("`snapshot` value had unexpected type"),
        }
    }
    fn string<'a, R: AvroRead>(&mut self, r: ValueOrReader<'a, &'a str, R>) -> Result<()> {
        let mut s = SmallVec::<[u8; 8]>::new();
        let s = match r {
            ValueOrReader::Value(val) => val.as_bytes(),
            ValueOrReader::Reader { len, r } => {
                s.resize_with(len, Default::default);
                r.read_exact(&mut s)?;
                &s
            }
        };
        self.0 = Some(match s {
            b"true" => DbzSnapshot::True,
            b"last" => DbzSnapshot::Last,
            b"false" => DbzSnapshot::False,
            _ => bail!(
                "`snapshot` had unexpected value {}",
                String::from_utf8_lossy(s)
            ),
        });
        Ok(())
    }
}

impl<'a> AvroDecode for DebeziumSourceDecoder<'a> {
    fn record<R: AvroRead, A: AvroRecordAccess<R>>(&mut self, a: &mut A) -> Result<()> {
        while let Some((name, _, field)) = a.next_field()? {
            match name {
                "snapshot" => {
                    let mut d = AvroDbzSnapshotDecoder(None);
                    field.decode_field(&mut d)?;
                    self.snapshot = Some(match d.0 {
                        None | Some(DbzSnapshot::False) => false,
                        Some(DbzSnapshot::True) | Some(DbzSnapshot::Last) => true,
                    });
                }
                "pos" => {
                    self.pos = Some(
                        field
                            .decode_to_value()?
                            .into_integral()
                            .ok_or_else(|| format_err!("\"pos\" is not an integer"))?,
                    );
                }
                "row" => {
                    self.row = Some(
                        field
                            .decode_to_value()?
                            .into_integral()
                            .ok_or_else(|| format_err!("\"row\" is not an integer"))?,
                    );
                }
                "file" => {
                    let mut d = AvroStringDecoder::with_buf(self.file_buf);
                    field.decode_field(&mut d)?;
                    self.has_file = d.decoded;
                }
                _ => {
                    field.decode_field(&mut TrivialDecoder)?;
                }
            }
        }
        Ok(())
    }
}
struct OptionalRowDecoder<'a> {
    pub packer: &'a mut RowPacker,
    pub buf: &'a mut Vec<u8>,
    pub decoded_row: bool,
}

impl<'a> AvroDecode for OptionalRowDecoder<'a> {
    fn union_branch<'b, R: AvroRead, D: AvroDeserializer>(
        &mut self,
        idx: usize,
        _n_variants: usize,
        null_variant: Option<usize>,
        deserializer: &'b mut D,
        reader: &'b mut R,
    ) -> Result<()> {
        if Some(idx) == null_variant {
            // we are done, the row is null!
            Ok(())
        } else {
            self.decoded_row = true;
            let mut d = AvroFlatDecoder {
                packer: &mut self.packer,
                buf: &mut self.buf,
                is_top: true,
            };
            deserializer.deserialize(reader, &mut d)
        }
    }
}

#[derive(Debug)]
pub struct AvroDebeziumDecoder<'a> {
    pub packer: &'a mut RowPacker,
    pub buf: &'a mut Vec<u8>,
    pub file_buf: &'a mut Vec<u8>,
    pub before: Option<Row>,
    pub after: Option<Row>,
    pub coords: Option<DebeziumSourceCoordinates>,
}

impl<'a> AvroDecode for AvroDebeziumDecoder<'a> {
    fn record<R: AvroRead, A: AvroRecordAccess<R>>(&mut self, a: &mut A) -> Result<()> {
        while let Some((name, _, field)) = a.next_field()? {
            match name {
                "before" => {
                    let mut d = OptionalRowDecoder {
                        packer: &mut self.packer,
                        buf: &mut self.buf,
                        decoded_row: false,
                    };
                    field.decode_field(&mut d)?;

                    self.before = if d.decoded_row {
                        self.packer.push(Datum::Int64(-1));
                        Some(self.packer.finish_and_reuse())
                    } else {
                        None
                    }
                }
                "after" => {
                    let mut d = OptionalRowDecoder {
                        packer: &mut self.packer,
                        buf: &mut self.buf,
                        decoded_row: false,
                    };
                    field.decode_field(&mut d)?;

                    self.after = if d.decoded_row {
                        self.packer.push(Datum::Int64(1));
                        Some(self.packer.finish_and_reuse())
                    } else {
                        None
                    }
                }
                "source" => {
                    let mut d = DebeziumSourceDecoder {
                        snapshot: None,
                        pos: None,
                        row: None,
                        file_buf: &mut self.file_buf,
                        has_file: false,
                    };
                    field.decode_field(&mut d)?;
                    self.coords = Some(d.coords()?);
                }
                _ => {
                    field.decode_field(&mut TrivialDecoder)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct AvroFlatDecoder<'a> {
    pub packer: &'a mut RowPacker,
    pub buf: &'a mut Vec<u8>,
    pub is_top: bool,
}

impl<'a> AvroDecode for AvroFlatDecoder<'a> {
    #[inline]
    fn record<R: AvroRead, A: AvroRecordAccess<R>>(&mut self, a: &mut A) -> Result<()> {
        let mut str_buf = std::mem::take(self.buf);
        let mut pack_record = |rp: &mut RowPacker| -> Result<()> {
            let mut expected = 0;
            let mut stash = vec![];
            let mut dec = AvroFlatDecoder {
                packer: rp,
                buf: &mut str_buf,
                is_top: false,
            };
            // The idea here is that if the deserializer gives us fields in the order we're expecting,
            // we can decode them directly into the row.
            // If not, we need to decode them into a Value (the old, slow decoding path) and stash them,
            // so that we can put everything in the right order at the end.
            //
            // TODO(btv) - this is pretty bad, as a misordering at the top of the schema graph will
            // cause the _entire_ chunk under it to be decoded in the slow way!
            // Maybe instead, we should decode to separate sub-RowPackers and then add an API
            // to RowPacker that just copies in the bytes from another one.
            while let Some((_name, idx, f)) = a.next_field()? {
                if idx == expected {
                    expected += 1;
                    f.decode_field(&mut dec)?;
                } else {
                    let val = f.decode_to_value()?;
                    stash.push((idx, val));
                }
            }
            stash.sort_by_key(|(idx, _val)| *idx);
            for (idx, val) in stash {
                assert!(idx == expected);
                expected += 1;
                give_value(&mut dec, &val)?;
            }
            Ok(())
        };
        if self.is_top {
            pack_record(self.packer)?;
        } else {
            self.packer.push_list_with(pack_record)?;
        }
        *self.buf = str_buf;
        Ok(())
    }
    #[inline]
    fn union_branch<'b, R: AvroRead, D: AvroDeserializer>(
        &mut self,
        idx: usize,
        n_variants: usize,
        null_variant: Option<usize>,
        deserializer: &'b mut D,
        reader: &'b mut R,
    ) -> Result<()> {
        if null_variant == Some(idx) {
            for _ in 0..n_variants - 1 {
                self.packer.push(Datum::Null)
            }
        } else {
            for i in 0..n_variants {
                if null_variant != Some(i) {
                    if i == idx {
                        deserializer.deserialize(reader, self)?;
                    } else {
                        self.packer.push(Datum::Null)
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    #[inline]
    fn enum_variant(&mut self, symbol: &str, _idx: usize) -> Result<()> {
        self.packer.push(Datum::String(symbol));
        Ok(())
    }
    #[inline]
    fn scalar(&mut self, scalar: avro::types::Scalar) -> Result<()> {
        match scalar {
            avro::types::Scalar::Null => self.packer.push(Datum::Null),
            avro::types::Scalar::Boolean(val) => {
                if val {
                    self.packer.push(Datum::True)
                } else {
                    self.packer.push(Datum::False)
                }
            }
            avro::types::Scalar::Int(val) => self.packer.push(Datum::Int32(val)),
            avro::types::Scalar::Long(val) => self.packer.push(Datum::Int64(val)),
            avro::types::Scalar::Float(val) => self.packer.push(Datum::Float32(OrderedFloat(val))),
            avro::types::Scalar::Double(val) => self.packer.push(Datum::Float64(OrderedFloat(val))),
            avro::types::Scalar::Date(val) => self.packer.push(Datum::Date(val)),
            avro::types::Scalar::Timestamp(val) => self.packer.push(Datum::Timestamp(val)),
        }
        Ok(())
    }
    #[inline]
    fn decimal<'b, R: AvroRead>(
        &mut self,
        _precision: usize,
        _scale: usize,
        r: ValueOrReader<'b, &'b [u8], R>,
    ) -> Result<()> {
        let buf = match r {
            ValueOrReader::Value(val) => val,
            ValueOrReader::Reader { len, r } => {
                self.buf.resize_with(len, Default::default);
                r.read_exact(&mut self.buf)?;
                &self.buf
            }
        };
        self.packer
            .push(Datum::Decimal(Significand::from_twos_complement_be(buf)?));
        Ok(())
    }
    #[inline]
    fn bytes<'b, R: AvroRead>(&mut self, r: ValueOrReader<'b, &'b [u8], R>) -> Result<()> {
        let buf = match r {
            ValueOrReader::Value(val) => val,
            ValueOrReader::Reader { len, r } => {
                self.buf.resize_with(len, Default::default);
                r.read_exact(&mut self.buf)?;
                &self.buf
            }
        };
        self.packer.push(Datum::Bytes(buf));
        Ok(())
    }
    #[inline]
    fn string<'b, R: AvroRead>(&mut self, r: ValueOrReader<'b, &'b str, R>) -> Result<()> {
        let s = match r {
            ValueOrReader::Value(val) => val,
            ValueOrReader::Reader { len, r } => {
                // TODO - this copy is unnecessary,
                // we should special case to just look at the bytes
                // directly when r is &[u8].
                // It probably doesn't make a huge difference though.
                self.buf.resize_with(len, Default::default);
                r.read_exact(&mut self.buf)?;
                std::str::from_utf8(&self.buf)?
            }
        };
        self.packer.push(Datum::String(s));
        Ok(())
    }
    #[inline]
    fn json<'b, R: AvroRead>(
        &mut self,
        r: ValueOrReader<'b, &'b serde_json::Value, R>,
    ) -> Result<()> {
        match r {
            ValueOrReader::Value(val) => {
                *self.packer =
                    JsonbPacker::new(std::mem::take(self.packer)).pack_serde_json(val.clone())?;
            }
            ValueOrReader::Reader { len, r } => {
                self.buf.resize_with(len, Default::default);
                r.read_exact(&mut self.buf)?;
                *self.packer =
                    JsonbPacker::new(std::mem::take(self.packer)).pack_slice(&self.buf)?;
            }
        }
        Ok(())
    }
    #[inline]
    fn fixed<'b, R: AvroRead>(&mut self, r: ValueOrReader<'b, &'b [u8], R>) -> Result<()> {
        self.bytes(r)
    }
    #[inline]
    fn array<A: AvroArrayAccess>(&mut self, a: &mut A) -> Result<()> {
        let mut str_buf = std::mem::take(self.buf);
        self.packer.push_list_with(|rp| -> Result<()> {
            let mut dec = AvroFlatDecoder {
                packer: rp,
                buf: &mut str_buf,
                is_top: false,
            };
            while a.decode_next(&mut dec)? {}
            Ok(())
        })?;
        *self.buf = str_buf;
        Ok(())
    }
}

/// Converts an Apache Avro schema into a list of column names and types.
pub fn validate_value_schema(
    schema: &str,
    envelope: EnvelopeType,
) -> Result<Vec<(ColumnName, ColumnType)>> {
    let schema = parse_schema(schema)?;
    let node = schema.top_node();

    let row_schema = match envelope {
        EnvelopeType::Debezium => {
            // The top-level record needs to be a diff "envelope" that contains
            // `before` and `after` fields, where the `before` and `after` fields
            // have the same schema.
            match node.inner {
                SchemaPiece::Record { fields, .. } => {
                    let before = fields.iter().find(|f| f.name == "before");
                    let after = fields.iter().find(|f| f.name == "after");
                    match (before, after) {
                        (Some(before), Some(after)) => {
                            let left = node.step(&before.schema);
                            let right = node.step(&after.schema);
                            match (left.inner, right.inner) {
                                (SchemaPiece::Union(before), SchemaPiece::Union(after)) => {
                                    if before.variants().len() != 2 {
                                        bail!("Source schema 'before' field has the wrong number of variants");
                                    }
                                    if after.variants().len() != 2 {
                                        bail!("Source schema 'after' field has the wrong number of variants");
                                    }
                                    let before_null =
                                        before.variants().iter().position(|s| is_null(s));
                                    let after_null =
                                        before.variants().iter().position(|s| is_null(s));
                                    if before_null != after_null {
                                        bail!("Source schema 'before' and 'after' fields do not match.");
                                    }

                                    let null_idx = match before_null {
                                        Some(null_idx) => null_idx,
                                        None => bail!("Source schema 'before'/'after' fields are not of expected type")
                                    };
                                    let record_idx = 1 - null_idx;
                                    let (before_piece, after_piece) = (
                                        &before.variants()[record_idx],
                                        &after.variants()[record_idx],
                                    );
                                    let before_name = match before_piece {
                                        SchemaPieceOrNamed::Piece(_) => bail!(
                                            "Source schema 'before' field should be a record."
                                        ),
                                        SchemaPieceOrNamed::Named(name) => name,
                                    };
                                    let after_name = match after_piece {
                                        SchemaPieceOrNamed::Piece(_) => {
                                            bail!("Source schema 'after' field should be a record.")
                                        }
                                        SchemaPieceOrNamed::Named(name) => name,
                                    };
                                    if before_name != after_name {
                                        bail!("Source schema 'before' and 'after' fields should be the same named record.");
                                    }
                                    match schema.lookup(*before_name).piece {
                                        SchemaPiece::Record { .. } => before_piece,
                                        _ => bail!("Source schema 'before' and 'after' fields should contain a record."),
                                    }
                                }
                                (_, SchemaPiece::Union(_)) => {
                                    bail!("Source schema 'before' field should be a union.")
                                }
                                (SchemaPiece::Union(_), _) => {
                                    bail!("Source schema 'after' field should be a union.")
                                }
                                (_, _) => bail!(
                                    "Source schema 'before' and 'after' fields should be unions."
                                ),
                            }
                        }
                        (None, _) => bail!("source schema is missing 'before' field"),
                        (_, None) => bail!("source schema is missing 'after' field"),
                    }
                }
                _ => bail!("Top-level envelope must be a record."),
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

fn validate_schema_1(schema: SchemaNode) -> Result<Vec<(ColumnName, ColumnType)>> {
    match schema.inner {
        SchemaPiece::Record { fields, .. } => {
            let mut columns = vec![];
            let mut seen_avro_nodes = Default::default();
            for f in fields {
                columns.extend(get_named_columns(
                    &mut seen_avro_nodes,
                    schema.step(&f.schema),
                    &f.name,
                )?);
            }
            Ok(columns)
        }
        _ => bail!("row schemas must be records, got: {:?}", schema.inner),
    }
}

fn get_named_columns<'a>(
    seen_avro_nodes: &mut HashSet<usize>,
    schema: SchemaNode<'a>,
    base_name: &str,
) -> Result<Vec<(ColumnName, ColumnType)>> {
    if let SchemaPiece::Union(us) = schema.inner {
        let mut columns = vec![];
        let vs = us.variants();
        if vs.is_empty() || (vs.len() == 1 && is_null(&vs[0])) {
            bail!(format_err!("Empty or null-only unions are not supported"));
        } else {
            for (i, v) in vs.iter().filter(|v| !is_null(v)).enumerate() {
                let named_idx = match v {
                    SchemaPieceOrNamed::Named(idx) => Some(*idx),
                    _ => None,
                };
                if let Some(named_idx) = named_idx {
                    if !seen_avro_nodes.insert(named_idx) {
                        bail!(
                            "Recursively defined type in schema: {}",
                            v.get_human_name(schema.root)
                        );
                    }
                }
                let node = schema.step(v);
                if let SchemaPiece::Union(_) = node.inner {
                    unreachable!("Internal error: directly nested avro union!");
                }

                let name = if vs.len() == 1 || (vs.len() == 2 && vs.iter().any(is_null)) {
                    // There is only one non-null variant in the
                    // union, so we can use the field name directly.
                    base_name.to_string()
                } else {
                    // There are multiple non-null variants in the
                    // union, so we need to invent field names for
                    // each variant.
                    format!("{}{}", &base_name, i + 1)
                };

                // If there is more than one variant in the union,
                // the column's output type is nullable, as this
                // column will be null whenever it is uninhabited.
                let ty = validate_schema_2(seen_avro_nodes, node)?;
                let nullable = vs.len() > 1;
                columns.push((name.into(), ColumnType::new(ty).nullable(nullable)));
                if let Some(named_idx) = named_idx {
                    seen_avro_nodes.remove(&named_idx);
                }
            }
        }
        Ok(columns)
    } else {
        let scalar_type = validate_schema_2(seen_avro_nodes, schema)?;
        Ok(vec![(
            base_name.into(),
            ColumnType::new(scalar_type).nullable(false),
        )])
    }
}

fn validate_schema_2(
    seen_avro_nodes: &mut HashSet<usize>,
    schema: SchemaNode,
) -> Result<ScalarType> {
    Ok(match schema.inner {
        SchemaPiece::Null => bail!("null outside of union types is not supported"),
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
        SchemaPiece::Record { fields, .. } => {
            let mut columns = vec![];
            for f in fields {
                let named_idx = match &f.schema {
                    SchemaPieceOrNamed::Named(idx) => Some(*idx),
                    _ => None,
                };
                if let Some(named_idx) = named_idx {
                    if !seen_avro_nodes.insert(named_idx) {
                        bail!(
                            "Recursively defined type in schema: {}",
                            f.schema.get_human_name(schema.root)
                        );
                    }
                }
                let next_node = schema.step(&f.schema);
                columns.extend(
                    get_named_columns(seen_avro_nodes, next_node, &f.name)?
                        .into_iter()
                        // We strip out the nullability flag, because ScalarType::Record
                        // fields are always nullable.
                        .map(|(name, coltype)| (name, coltype.scalar_type)),
                );
                if let Some(named_idx) = named_idx {
                    seen_avro_nodes.remove(&named_idx);
                }
            }
            ScalarType::Record { fields: columns }
        }
        SchemaPiece::Array(inner) => {
            let named_idx = match inner.as_ref() {
                SchemaPieceOrNamed::Named(idx) => Some(*idx),
                _ => None,
            };
            if let Some(named_idx) = named_idx {
                if !seen_avro_nodes.insert(named_idx) {
                    bail!(
                        "Recursively defined type in schema: {}",
                        inner.get_human_name(schema.root)
                    );
                }
            }
            let next_node = schema.step(inner);
            let ret = ScalarType::List(Box::new(validate_schema_2(seen_avro_nodes, next_node)?));
            if let Some(named_idx) = named_idx {
                seen_avro_nodes.remove(&named_idx);
            }
            ret
        }

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
        Value::Union { index, inner, .. } => {
            let mut v = Some(*inner);
            if let SchemaPiece::Union(us) = n.inner {
                for (var_idx, var_s) in us
                    .variants()
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| !is_null(s))
                {
                    if var_idx == index {
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
        Value::Json(j) => row = JsonbPacker::new(row).pack_serde_json(j)?,
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
        Value::Union { index, inner, .. } => {
            let next = if let SchemaPiece::Union(us) = n.inner {
                n.step(&us.variants()[index])
            } else {
                unreachable!("Avro value out of sync with schema")
            };
            (*inner, next)
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

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum DebeziumDeduplicationStrategy {
    Ordered,
    Full,
}

#[derive(Debug)]
pub enum DebeziumDeduplicationState {
    Ordered {
        /// Last recorded (pos, row, offset) for each MySQL binlog file.
        /// Messages that are not ahead of the last recorded pos/row will be skipped.
        binlog_offsets: HashMap<String, (usize, usize, Option<i64>)>,
    },
    Full {
        seen_offsets: HashMap<String, HashSet<(usize, usize)>>,
    },
}

impl DebeziumDeduplicationState {
    fn new(strat: DebeziumDeduplicationStrategy) -> Self {
        match strat {
            DebeziumDeduplicationStrategy::Ordered => DebeziumDeduplicationState::Ordered {
                binlog_offsets: Default::default(),
            },
            DebeziumDeduplicationStrategy::Full => DebeziumDeduplicationState::Full {
                seen_offsets: Default::default(),
            },
        }
    }
    #[must_use]
    fn should_use_record(
        &mut self,
        file: &str,
        pos: usize,
        row: usize,
        coord: Option<i64>,
        debug_name: &str,
        worker_idx: usize,
    ) -> bool {
        match self {
            DebeziumDeduplicationState::Ordered { binlog_offsets } => {
                match binlog_offsets.entry(file.to_owned()) {
                    Entry::Occupied(mut oe) => {
                        let (old_max_pos, old_max_row, old_offset) = *oe.get();
                        if (old_max_pos, old_max_row) >= (pos, row) {
                            let offset_string = if let Some(coord) = coord {
                                format!(" at offset {}", coord)
                            } else {
                                format!("")
                            };
                            let old_offset_string = if let Some(old_offset) = old_offset {
                                format!(" at offset {}", old_offset)
                            } else {
                                format!("")
                            };
                            warn!("Debezium for source {}:{} did not advance in binlog file {}: previously read ({}, {}){}, now read ({}, {}){}. Skipping record.",
                                    debug_name, worker_idx, file, old_max_pos, old_max_row, old_offset_string, pos, row, offset_string);
                            return false;
                        }
                        oe.insert((pos, row, coord));
                    }
                    Entry::Vacant(ve) => {
                        ve.insert((pos, row, coord));
                    }
                }
                true
            }

            DebeziumDeduplicationState::Full { seen_offsets } => {
                if let Some(seen_offsets) = seen_offsets.get_mut(file) {
                    if seen_offsets.insert((pos, row)) {
                        true
                    } else {
                        warn!("Source {}:{} already ingested binlog coordinates ({}, {}) in file {}. Skipping record.",
                              debug_name, worker_idx, pos, row, file);
                        false
                    }
                } else {
                    let mut hs = HashSet::new();
                    hs.insert((pos, row));
                    seen_offsets.insert(file.to_owned(), hs);
                    true
                }
            }
        }
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
    dedup: DebeziumDeduplicationState,
    binlog_schema_indices: Option<BinlogSchemaIndices>,
    /// Human-readable name used for printing debug information
    debug_name: String,
    /// Worker we are running on (used for printing debug information)
    worker_idx: usize,
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
    pub fn new(
        schema: &Schema,
        debug_name: String,
        worker_idx: usize,
        dedup_strat: DebeziumDeduplicationStrategy,
    ) -> Option<Self> {
        let top_node = schema.top_node();
        let top_indices = field_indices(top_node)?;
        let before_idx = *top_indices.get("before")?;
        let after_idx = *top_indices.get("after")?;
        let binlog_schema_indices = BinlogSchemaIndices::new_from_schema(top_node);

        Some(Self {
            before_idx,
            after_idx,
            dedup: DebeziumDeduplicationState::new(dedup_strat),
            binlog_schema_indices,
            debug_name,
            worker_idx,
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
                Value::Union { inner, .. } => is_snapshot(*inner)?,
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
                        if !self.dedup.should_use_record(
                            &file_val,
                            pos,
                            row,
                            coord,
                            &self.debug_name,
                            self.worker_idx,
                        ) {
                            return Ok(DiffPair {
                                before: None,
                                after: None,
                            });
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
    debezium_dedup: Option<DebeziumDeduplicationState>,
    debug_name: String,
    worker_index: usize,
    buf1: Vec<u8>,
    buf2: Vec<u8>,
    packer: RowPacker,
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
        debug_name: String,
        worker_index: usize,
        debezium_dedup: Option<DebeziumDeduplicationStrategy>,
    ) -> Result<Decoder> {
        assert!(
            (envelope == EnvelopeType::Debezium && debezium_dedup.is_some())
                || (envelope != EnvelopeType::Debezium && debezium_dedup.is_none())
        );
        let debezium_dedup = debezium_dedup.map(DebeziumDeduplicationState::new);
        // It is assumed that the reader schema has already been verified
        // to be a valid Avro schema.
        let reader_schema = parse_schema(reader_schema).unwrap();
        let writer_schemas =
            schema_registry.map(|sr| SchemaCache::new(sr, reader_schema.fingerprint::<Sha256>()));

        Ok(Decoder {
            reader_schema,
            writer_schemas,
            envelope,
            debezium_dedup,
            debug_name,
            worker_index,
            buf1: vec![],
            buf2: vec![],
            packer: Default::default(),
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
            let mut dec = AvroDebeziumDecoder {
                packer: &mut self.packer,
                buf: &mut self.buf1,
                file_buf: &mut self.buf2,
                before: None,
                after: None,
                coords: None,
            };
            let mut dsr: GeneralDeserializer = GeneralDeserializer {
                schema: resolved_schema.top_node(),
            };
            // Unwrap is OK: we assert in Decoder::new that this is non-none when envelope == dbz.
            let dedup = self.debezium_dedup.as_mut().unwrap();
            dsr.deserialize(&mut bytes, &mut dec)?;
            let should_use = if let Some(source) = dec.coords {
                // TODO - avoid the unnecessary utf8 check here.
                let file = std::str::from_utf8(dec.file_buf)?;
                source.snapshot
                    || dedup.should_use_record(
                        file,
                        source.pos,
                        source.row,
                        coord,
                        &self.debug_name,
                        self.worker_index,
                    )
            } else {
                true
            };
            if should_use {
                DiffPair {
                    before: dec.before,
                    after: dec.after,
                }
            } else {
                DiffPair {
                    before: None,
                    after: None,
                }
            }
        } else {
            let mut dec = AvroFlatDecoder {
                packer: &mut self.packer,
                buf: &mut self.buf1,
                is_top: true,
            };
            let mut dsr: GeneralDeserializer = GeneralDeserializer {
                schema: resolved_schema.top_node(),
            };
            dsr.deserialize(&mut bytes, &mut dec)?;
            DiffPair {
                before: None,
                after: Some(dec.packer.finish_and_reuse()),
            }
        };
        log::trace!(
            "[customer-data] Decoded diff pair {:?}{} in {}",
            result,
            if let Some(coord) = coord {
                format!(" at offset {}", coord)
            } else {
                format!("")
            },
            self.debug_name
        );
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
fn build_schema(columns: &[(ColumnName, ColumnType)], include_transaction: bool) -> Schema {
    let mut fields = Vec::new();
    for (name, typ) in columns.iter() {
        let mut field_type = match &typ.scalar_type {
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
            ScalarType::List(_t) => unimplemented!("list types"),
            ScalarType::Record { .. } => unimplemented!("record types"),
        };
        if typ.nullable {
            field_type = json!(["null", field_type]);
        }
        fields.push(json!({
            "name": name,
            "type": field_type,
        }));
    }

    let mut schema_fields = Vec::new();
    schema_fields.push(json!({
        "name": "before",
        "type": [
            "null",
            {
                "name": "row",
                "type": "record",
                "fields": fields,
             }
        ]
    }));

    schema_fields.push(json!({
        "name": "after",
        "type": ["null", "row"],
    }));

    // TODO(rkhaitan): this schema omits the total_order and data collection_order
    // fields found in Debezium's transaction metadata struct. We chose to omit
    // those because the order is not stable across reruns and has no semantic
    // meaning for records within a timestamp in Materialize. These fields may
    // be useful in the future for deduplication.
    if include_transaction {
        schema_fields.push(json!({
        "name": "transaction",
            "type":
                {
                    "name": "transaction_metadata",
                    "type": "record",
                    "fields": [
                        {
                            "name": "id",
                            "type": "string",
                        }
                    ]
                }
        }));
    }

    let schema = json!({
        "type": "record",
        "name": "envelope",
        "fields": schema_fields,
    });
    Schema::parse(&schema).expect("valid schema constructed")
}

pub fn get_debezium_transaction_schema() -> &'static Schema {
    &DEBEZIUM_TRANSACTION_SCHEMA
}

pub fn encode_debezium_transaction_unchecked(
    schema_id: i32,
    id: &str,
    status: &str,
    message_count: Option<i64>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_avro_header(&mut buf, schema_id);

    let transaction_id = Value::String(id.to_owned());
    let status = Value::String(status.to_owned());
    let event_count = match message_count {
        None => Value::Union {
            index: 0,
            inner: Box::new(Value::Null),
            n_variants: 2,
            null_variant: Some(0),
        },
        Some(count) => Value::Union {
            index: 1,
            inner: Box::new(Value::Long(count)),
            n_variants: 2,
            null_variant: Some(0),
        },
    };

    let avro = Value::Record(vec![
        ("id".into(), transaction_id),
        ("status".into(), status),
        ("event_count".into(), event_count),
    ]);
    debug_assert!(avro.validate(DEBEZIUM_TRANSACTION_SCHEMA.top_node()));
    avro::encode_unchecked(&avro, &DEBEZIUM_TRANSACTION_SCHEMA, &mut buf);
    buf
}

fn encode_avro_header(buf: &mut Vec<u8>, schema_id: i32) {
    // The first byte is a magic byte (0) that indicates the Confluent
    // serialization format version, and the next four bytes are a
    // 32-bit schema ID.
    //
    // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
    buf.write_u8(0).expect("writing to vec cannot fail");
    buf.write_i32::<NetworkEndian>(schema_id)
        .expect("writing to vec cannot fail");
}

/// Manages encoding of Avro-encoded bytes.
pub struct Encoder {
    columns: Vec<(ColumnName, ColumnType)>,
    writer_schema: Schema,
    include_transaction: bool,
}

impl fmt::Debug for Encoder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Encoder")
            .field("writer_schema", &self.writer_schema)
            .finish()
    }
}

impl Encoder {
    pub fn new(desc: RelationDesc, include_transaction: bool) -> Self {
        // Invent names for columns that don't have a name.
        let mut columns: Vec<_> = desc
            .into_iter()
            .enumerate()
            .map(|(i, (name, ty))| match name {
                None => (ColumnName::from(format!("column{}", i + 1)), ty),
                Some(name) => (name, ty),
            })
            .collect();

        // Deduplicate names.
        let mut seen = HashSet::new();
        for (name, _ty) in &mut columns {
            let stem_len = name.as_str().len();
            let mut i = 1;
            while seen.contains(name) {
                name.as_mut_str().truncate(stem_len);
                if name.as_str().ends_with(|c: char| c.is_ascii_digit()) {
                    name.as_mut_str().push('_');
                }
                name.as_mut_str().push_str(&i.to_string());
                i += 1;
            }
            seen.insert(name);
        }

        let writer_schema = build_schema(&columns, include_transaction);
        Encoder {
            columns,
            writer_schema,
            include_transaction,
        }
    }

    pub fn writer_schema(&self) -> &Schema {
        &self.writer_schema
    }

    fn validate_transaction_id(&self, transaction_id: &Option<String>) {
        // We need to preserve the invariant that transaction id is always Some(..)
        // when users requested that we emit transaction information, and never
        // otherwise.
        assert_eq!(
            self.include_transaction,
            transaction_id.is_some(),
            "Testing to make sure transaction IDs are always present only when required"
        );
    }

    pub fn encode_unchecked(
        &self,
        schema_id: i32,
        diff_pair: DiffPair<&Row>,
        transaction_id: Option<String>,
    ) -> Vec<u8> {
        self.validate_transaction_id(&transaction_id);
        let mut buf = Vec::new();
        encode_avro_header(&mut buf, schema_id);
        let avro = self.diff_pair_to_avro(diff_pair, transaction_id);
        debug_assert!(avro.validate(self.writer_schema.top_node()));
        avro::encode_unchecked(&avro, &self.writer_schema, &mut buf);
        buf
    }

    /// Encodes a repr::Row to a Avro-compliant Vec<u8>.
    pub fn encode(
        &self,
        schema_id: i32,
        diff_pair: DiffPair<&Row>,
        transaction_id: Option<String>,
    ) -> Vec<u8> {
        self.validate_transaction_id(&transaction_id);
        let mut buf = Vec::new();
        encode_avro_header(&mut buf, schema_id);
        buf.write_i32::<NetworkEndian>(schema_id)
            .expect("writing to vec cannot fail");
        avro::write_avro_datum(
            &self.writer_schema,
            self.diff_pair_to_avro(diff_pair, transaction_id),
            &mut buf,
        )
        .expect("schema constructed to match val");
        buf
    }

    pub fn diff_pair_to_avro(
        &self,
        diff_pair: DiffPair<&Row>,
        transaction_id: Option<String>,
    ) -> Value {
        let before = match diff_pair.before {
            None => Value::Union {
                index: 0,
                inner: Box::new(Value::Null),
                n_variants: 2,
                null_variant: Some(0),
            },
            Some(row) => {
                let row = self.row_to_avro(row.unpack());
                Value::Union {
                    index: 1,
                    inner: Box::new(row),
                    n_variants: 2,
                    null_variant: Some(0),
                }
            }
        };
        let after = match diff_pair.after {
            None => Value::Union {
                index: 0,
                inner: Box::new(Value::Null),
                n_variants: 2,
                null_variant: Some(0),
            },
            Some(row) => {
                let row = self.row_to_avro(row.unpack());
                Value::Union {
                    index: 1,
                    inner: Box::new(row),
                    n_variants: 2,
                    null_variant: Some(0),
                }
            }
        };

        let transaction = if let Some(transaction_id) = transaction_id {
            let id = Value::String(transaction_id);
            Some(Value::Record(vec![("id".into(), id)]))
        } else {
            None
        };

        let mut fields = Vec::new();
        fields.push(("before".into(), before));
        fields.push(("after".into(), after));

        if let Some(transaction) = transaction {
            fields.push(("transaction".into(), transaction));
        }

        Value::Record(fields)
    }

    fn row_to_avro(&self, row: Vec<Datum>) -> Value {
        let fields = self
            .columns
            .iter()
            .zip_eq(row)
            .map(|((name, typ), datum)| {
                let name = name.as_str().to_owned();
                if typ.nullable && datum.is_null() {
                    return (
                        name,
                        Value::Union {
                            index: 0,
                            inner: Box::new(Value::Null),
                            n_variants: 2,
                            null_variant: Some(0),
                        },
                    );
                }
                let mut val = match &typ.scalar_type {
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
                    // This feature isn't actually supported by the Avro Java
                    // client (https://issues.apache.org/jira/browse/AVRO-2123),
                    // so no one is likely to be using it, so we're just using
                    // our own very convenient format.
                    ScalarType::Interval => Value::Fixed(20, {
                        let iv = datum.unwrap_interval();
                        let mut buf = Vec::with_capacity(24);
                        buf.extend(&iv.months.to_le_bytes());
                        buf.extend(&iv.duration.to_le_bytes());
                        debug_assert_eq!(buf.len(), 20);
                        buf
                    }),
                    ScalarType::Bytes => Value::Bytes(Vec::from(datum.unwrap_bytes())),
                    ScalarType::String => Value::String(datum.unwrap_str().to_owned()),
                    ScalarType::Jsonb => Value::Json(JsonbRef::from_datum(datum).to_serde_json()),
                    ScalarType::List(_t) => unimplemented!("list types"),
                    ScalarType::Record { .. } => unimplemented!("record types"),
                };
                if typ.nullable {
                    val = Value::Union {
                        index: 1,
                        inner: Box::new(val),
                        n_variants: 2,
                        null_variant: Some(0),
                    };
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
    use repr::adt::decimal::Significand;
    use repr::{Datum, RelationDesc};

    use super::*;

    #[derive(Deserialize)]
    struct TestCase {
        name: String,
        input: serde_json::Value,
        expected: Vec<(ColumnName, ColumnType)>,
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
            let desc = RelationDesc::empty().with_nonnull_column("column1", typ);
            let avro_value = Encoder::new(desc, false).row_to_avro(vec![datum]);
            assert_eq!(
                Value::Record(vec![("column1".into(), expected)]),
                avro_value
            );
        }

        Ok(())
    }
}
