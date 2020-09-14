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
use std::{cell::RefCell, iter, rc::Rc};

use anyhow::{anyhow, bail};
use byteorder::{BigEndian, ByteOrder, NetworkEndian, WriteBytesExt};
use chrono::Timelike;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::warn;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;
use uuid::Uuid;

use mz_avro::schema::{
    resolve_schemas, RecordField, Schema, SchemaFingerprint, SchemaNode, SchemaPiece,
    SchemaPieceOrNamed,
};
use mz_avro::{
    define_unexpected,
    error::{DecodeError, Error as AvroError},
    give_value,
    types::{DecimalValue, Scalar, Value},
    AvroArrayAccess, AvroDecode, AvroDeserializer, AvroRead, AvroRecordAccess, GeneralDeserializer,
    StatefulAvroDecodeable, TrivialDecoder, ValueDecoder, ValueOrReader,
};
use repr::adt::decimal::{Significand, MAX_DECIMAL_PRECISION};
use repr::adt::jsonb::{JsonbPacker, JsonbRef};
use repr::{ColumnName, ColumnType, Datum, RelationDesc, Row, RowPacker, ScalarType};

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
pub fn validate_key_schema(
    key_schema: &str,
    value_desc: &RelationDesc,
) -> anyhow::Result<Vec<usize>> {
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

#[derive(Debug, Copy, Clone)]
pub enum RowCoordinates {
    MySql { pos: usize, row: usize },
    Postgres { lsn: usize },
}

#[derive(Debug)]
pub struct DebeziumSourceCoordinates {
    snapshot: bool,
    row: RowCoordinates,
}
struct DebeziumSourceDecoder<'a> {
    file_buf: &'a mut Vec<u8>,
}

struct AvroStringDecoder<'a> {
    pub buf: &'a mut Vec<u8>,
}

impl<'a> AvroStringDecoder<'a> {
    pub fn with_buf(buf: &'a mut Vec<u8>) -> Self {
        Self { buf }
    }
}

impl<'a> AvroDecode for AvroStringDecoder<'a> {
    type Out = ();
    fn string<'b, R: AvroRead>(
        self,
        r: ValueOrReader<'b, &'b str, R>,
    ) -> Result<Self::Out, AvroError> {
        match r {
            ValueOrReader::Value(val) => {
                self.buf.resize_with(val.len(), Default::default);
                val.as_bytes().read_exact(self.buf)?;
            }
            ValueOrReader::Reader { len, r } => {
                self.buf.resize_with(len, Default::default);
                r.read_exact(self.buf)?;
            }
        }
        Ok(())
    }
    define_unexpected! {
        record, union_branch, array, map, enum_variant, scalar, decimal, bytes, json, uuid, fixed
    }
}

#[derive(Debug, PartialEq, Eq)]
enum DbzSnapshot {
    True,
    Last,
    False,
}

struct AvroDbzSnapshotDecoder;
impl AvroDecode for AvroDbzSnapshotDecoder {
    type Out = Option<DbzSnapshot>;
    fn union_branch<'a, R: AvroRead, D: AvroDeserializer>(
        self,
        _idx: usize,
        _n_variants: usize,
        _null_variant: Option<usize>,
        deserializer: D,
        reader: &'a mut R,
    ) -> Result<Self::Out, AvroError> {
        deserializer.deserialize(reader, self)
    }
    fn scalar(self, scalar: Scalar) -> Result<Self::Out, AvroError> {
        match scalar {
            Scalar::Null => Ok(None),
            Scalar::Boolean(val) => Ok(Some(if val {
                DbzSnapshot::True
            } else {
                DbzSnapshot::False
            })),
            _ => {
                Err(DecodeError::Custom("`snapshot` value had unexpected type".to_string()).into())
            }
        }
    }
    fn string<'a, R: AvroRead>(
        self,
        r: ValueOrReader<'a, &'a str, R>,
    ) -> Result<Self::Out, AvroError> {
        let mut s = SmallVec::<[u8; 8]>::new();
        let s = match r {
            ValueOrReader::Value(val) => val.as_bytes(),
            ValueOrReader::Reader { len, r } => {
                s.resize_with(len, Default::default);
                r.read_exact(&mut s)?;
                &s
            }
        };
        Ok(Some(match s {
            b"true" => DbzSnapshot::True,
            b"last" => DbzSnapshot::Last,
            b"false" => DbzSnapshot::False,
            _ => {
                return Err(DecodeError::Custom(format!(
                    "`snapshot` had unexpected value {}",
                    String::from_utf8_lossy(s)
                ))
                .into())
            }
        }))
    }
    define_unexpected! {
        record, array, map, enum_variant, decimal, bytes, json, uuid, fixed
    }
}

impl<'a> AvroDecode for DebeziumSourceDecoder<'a> {
    type Out = DebeziumSourceCoordinates;
    fn record<R: AvroRead, A: AvroRecordAccess<R>>(
        self,
        a: &mut A,
    ) -> Result<Self::Out, AvroError> {
        let mut snapshot = false;
        // Binlog file "pos" and "row" - present in MySQL sources.
        let mut pos = None;
        let mut row = None;
        // "log sequence number" - monotonically increasing log offset in Postgres
        let mut lsn = None;
        let mut has_file = false;
        while let Some((name, _, field)) = a.next_field()? {
            match name {
                "snapshot" => {
                    let d = AvroDbzSnapshotDecoder;
                    let maybe_snapshot = field.decode_field(d)?;
                    snapshot = match maybe_snapshot {
                        None | Some(DbzSnapshot::False) => false,
                        Some(DbzSnapshot::True) | Some(DbzSnapshot::Last) => true,
                    };
                }
                "pos" => {
                    let next = ValueDecoder;
                    let val = field.decode_field(next)?;

                    pos = Some(val.into_integral().ok_or_else(|| {
                        DecodeError::Custom("\"pos\" is not an integer".to_string())
                    })?);
                }
                "row" => {
                    let next = ValueDecoder;
                    let val = field.decode_field(next)?;

                    row = Some(val.into_integral().ok_or_else(|| {
                        DecodeError::Custom("\"row\" is not an integer".to_string())
                    })?);
                }
                "file" => {
                    let d = AvroStringDecoder::with_buf(self.file_buf);
                    field.decode_field(d)?;
                    has_file = true;
                }
                "lsn" => {
                    let next = ValueDecoder;
                    let val = field.decode_field(next)?;
                    let val = match val {
                        Value::Union { inner, .. } => *inner,
                        val => val,
                    };
                    lsn = Some(val.into_integral().ok_or_else(|| {
                        DecodeError::Custom("\"lsn\" is not an integer".to_string())
                    })?);
                }
                _ => {
                    field.decode_field(TrivialDecoder)?;
                }
            }
        }
        let mysql_any = pos.is_some() || row.is_some() || has_file;
        let pg_any = lsn.is_some();
        let row = if mysql_any {
            if pg_any {
                return Err(DecodeError::Custom(
                "Found both MySQL (file/pos/row) and Postgres (LSN) source coordinates! We don't know how to interpret this.".to_string()).into());
            }
            let pos = pos.ok_or_else(|| DecodeError::Custom("no pos".to_string()))? as usize;
            let row = row.ok_or_else(|| DecodeError::Custom("no row".to_string()))? as usize;
            if !has_file {
                return Err(DecodeError::Custom("no file".to_string()).into());
            }
            RowCoordinates::MySql { pos, row }
        } else {
            let lsn = lsn.ok_or_else(|| DecodeError::Custom("no lsn".to_string()))? as usize;
            RowCoordinates::Postgres { lsn }
        };
        Ok(DebeziumSourceCoordinates { snapshot, row })
    }
    define_unexpected! {
        union_branch, array, map, enum_variant, scalar, decimal, bytes, string, json, uuid, fixed
    }
}
struct OptionalRowDecoder<'a> {
    pub packer: &'a mut RowPacker,
    pub buf: &'a mut Vec<u8>,
}

impl<'a> AvroDecode for OptionalRowDecoder<'a> {
    type Out = bool;
    fn union_branch<'b, R: AvroRead, D: AvroDeserializer>(
        self,
        idx: usize,
        _n_variants: usize,
        null_variant: Option<usize>,
        deserializer: D,
        reader: &'b mut R,
    ) -> Result<Self::Out, AvroError> {
        if Some(idx) == null_variant {
            // we are done, the row is null!
            Ok(false)
        } else {
            let d = AvroFlatDecoder {
                packer: self.packer,
                buf: self.buf,
                is_top: true,
            };
            deserializer.deserialize(reader, d)?;
            Ok(true)
        }
    }
    define_unexpected! {
        record, array, map, enum_variant, scalar, decimal, bytes, string, json, uuid, fixed
    }
}

#[derive(Debug)]
pub struct AvroDebeziumDecoder<'a> {
    pub packer: &'a mut RowPacker,
    pub buf: &'a mut Vec<u8>,
    pub file_buf: &'a mut Vec<u8>,
}

impl<'a> AvroDecode for AvroDebeziumDecoder<'a> {
    type Out = (DiffPair<Row>, Option<DebeziumSourceCoordinates>);
    fn record<R: AvroRead, A: AvroRecordAccess<R>>(
        self,
        a: &mut A,
    ) -> Result<Self::Out, AvroError> {
        let mut before = None;
        let mut after = None;
        let mut coords = None;
        while let Some((name, _, field)) = a.next_field()? {
            match name {
                "before" => {
                    let d = OptionalRowDecoder {
                        packer: self.packer,
                        buf: self.buf,
                    };
                    let decoded_row = field.decode_field(d)?;

                    before = if decoded_row {
                        self.packer.push(Datum::Int64(-1));
                        Some(self.packer.finish_and_reuse())
                    } else {
                        None
                    }
                }
                "after" => {
                    let d = OptionalRowDecoder {
                        packer: self.packer,
                        buf: self.buf,
                    };
                    let decoded_row = field.decode_field(d)?;

                    after = if decoded_row {
                        self.packer.push(Datum::Int64(1));
                        Some(self.packer.finish_and_reuse())
                    } else {
                        None
                    }
                }
                "source" => {
                    let d = DebeziumSourceDecoder {
                        file_buf: self.file_buf,
                    };
                    coords = Some(field.decode_field(d)?);
                }
                _ => {
                    field.decode_field(TrivialDecoder)?;
                }
            }
        }
        Ok((DiffPair { before, after }, coords))
    }
    define_unexpected! {
        union_branch, array, map, enum_variant, scalar, decimal, bytes, string, json, uuid, fixed
    }
}

struct RowDecoder {
    state: (Rc<RefCell<RowPacker>>, Rc<RefCell<Vec<u8>>>),
}

impl AvroDecode for RowDecoder {
    type Out = RowWrapper;
    fn record<R: AvroRead, A: AvroRecordAccess<R>>(
        self,
        a: &mut A,
    ) -> Result<Self::Out, AvroError> {
        let mut packer_borrow = self.state.0.borrow_mut();
        let mut buf_borrow = self.state.1.borrow_mut();
        let inner = AvroFlatDecoder {
            packer: &mut packer_borrow,
            buf: &mut buf_borrow,
            is_top: true,
        };
        inner.record(a)?;
        let row = packer_borrow.finish_and_reuse();
        Ok(RowWrapper(row))
    }
    define_unexpected! {
        union_branch, array, map, enum_variant, scalar, decimal, bytes, string, json, uuid, fixed
    }
}

// Get around orphan rule
#[derive(Debug)]
struct RowWrapper(Row);
impl StatefulAvroDecodeable for RowWrapper {
    type Decoder = RowDecoder;
    // TODO - can we make this some sort of &'a mut (RowPacker, Vec<u8>) without
    // running into lifetime crap?
    type State = (Rc<RefCell<RowPacker>>, Rc<RefCell<Vec<u8>>>);

    fn new_decoder(state: Self::State) -> Self::Decoder {
        Self::Decoder { state }
    }
}

#[derive(Debug)]
pub struct AvroFlatDecoder<'a> {
    pub packer: &'a mut RowPacker,
    pub buf: &'a mut Vec<u8>,
    pub is_top: bool,
}

impl<'a> AvroDecode for AvroFlatDecoder<'a> {
    type Out = ();
    #[inline]
    fn record<R: AvroRead, A: AvroRecordAccess<R>>(
        self,
        a: &mut A,
    ) -> Result<Self::Out, AvroError> {
        let mut str_buf = std::mem::take(self.buf);
        let mut pack_record = |rp: &mut RowPacker| -> Result<(), AvroError> {
            let mut expected = 0;
            let mut stash = vec![];
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
                let dec = AvroFlatDecoder {
                    packer: rp,
                    buf: &mut str_buf,
                    is_top: false,
                };
                if idx == expected {
                    expected += 1;
                    f.decode_field(dec)?;
                } else {
                    let next = ValueDecoder;
                    let val = f.decode_field(next)?;
                    stash.push((idx, val));
                }
            }
            stash.sort_by_key(|(idx, _val)| *idx);
            for (idx, val) in stash {
                assert!(idx == expected);
                expected += 1;
                let dec = AvroFlatDecoder {
                    packer: rp,
                    buf: &mut str_buf,
                    is_top: false,
                };
                give_value(dec, &val)?;
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
        self,
        idx: usize,
        n_variants: usize,
        null_variant: Option<usize>,
        deserializer: D,
        reader: &'b mut R,
    ) -> Result<Self::Out, AvroError> {
        if null_variant == Some(idx) {
            for _ in 0..n_variants - 1 {
                self.packer.push(Datum::Null)
            }
        } else {
            let mut deserializer = Some(deserializer);
            for i in 0..n_variants {
                let dec = AvroFlatDecoder {
                    packer: self.packer,
                    buf: self.buf,
                    is_top: false,
                };
                if null_variant != Some(i) {
                    if i == idx {
                        deserializer.take().unwrap().deserialize(reader, dec)?;
                    } else {
                        self.packer.push(Datum::Null)
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn enum_variant(self, symbol: &str, _idx: usize) -> Result<Self::Out, AvroError> {
        self.packer.push(Datum::String(symbol));
        Ok(())
    }
    #[inline]
    fn scalar(self, scalar: mz_avro::types::Scalar) -> Result<Self::Out, AvroError> {
        match scalar {
            mz_avro::types::Scalar::Null => self.packer.push(Datum::Null),
            mz_avro::types::Scalar::Boolean(val) => {
                if val {
                    self.packer.push(Datum::True)
                } else {
                    self.packer.push(Datum::False)
                }
            }
            mz_avro::types::Scalar::Int(val) => self.packer.push(Datum::Int32(val)),
            mz_avro::types::Scalar::Long(val) => self.packer.push(Datum::Int64(val)),
            mz_avro::types::Scalar::Float(val) => {
                self.packer.push(Datum::Float32(OrderedFloat(val)))
            }
            mz_avro::types::Scalar::Double(val) => {
                self.packer.push(Datum::Float64(OrderedFloat(val)))
            }
            mz_avro::types::Scalar::Date(val) => self.packer.push(Datum::Date(val)),
            mz_avro::types::Scalar::Timestamp(val) => self.packer.push(Datum::Timestamp(val)),
        }
        Ok(())
    }
    #[inline]
    fn decimal<'b, R: AvroRead>(
        self,
        _precision: usize,
        _scale: usize,
        r: ValueOrReader<'b, &'b [u8], R>,
    ) -> Result<Self::Out, AvroError> {
        let buf = match r {
            ValueOrReader::Value(val) => val,
            ValueOrReader::Reader { len, r } => {
                self.buf.resize_with(len, Default::default);
                r.read_exact(self.buf)?;
                &self.buf
            }
        };
        self.packer.push(Datum::Decimal(
            Significand::from_twos_complement_be(buf)
                .map_err(|e| DecodeError::Custom(e.to_string()))?,
        ));
        Ok(())
    }
    #[inline]
    fn bytes<'b, R: AvroRead>(
        self,
        r: ValueOrReader<'b, &'b [u8], R>,
    ) -> Result<Self::Out, AvroError> {
        let buf = match r {
            ValueOrReader::Value(val) => val,
            ValueOrReader::Reader { len, r } => {
                self.buf.resize_with(len, Default::default);
                r.read_exact(self.buf)?;
                &self.buf
            }
        };
        self.packer.push(Datum::Bytes(buf));
        Ok(())
    }
    #[inline]
    fn string<'b, R: AvroRead>(
        self,
        r: ValueOrReader<'b, &'b str, R>,
    ) -> Result<Self::Out, AvroError> {
        let s = match r {
            ValueOrReader::Value(val) => val,
            ValueOrReader::Reader { len, r } => {
                // TODO - this copy is unnecessary,
                // we should special case to just look at the bytes
                // directly when r is &[u8].
                // It probably doesn't make a huge difference though.
                self.buf.resize_with(len, Default::default);
                r.read_exact(self.buf)?;
                std::str::from_utf8(&self.buf).map_err(|_| DecodeError::StringUtf8Error)?
            }
        };
        self.packer.push(Datum::String(s));
        Ok(())
    }
    #[inline]
    fn json<'b, R: AvroRead>(
        self,
        r: ValueOrReader<'b, &'b serde_json::Value, R>,
    ) -> Result<Self::Out, AvroError> {
        match r {
            ValueOrReader::Value(val) => {
                *self.packer = JsonbPacker::new(std::mem::take(self.packer))
                    .pack_serde_json(val.clone())
                    .map_err(|e| DecodeError::Custom(e.to_string()))?;
            }
            ValueOrReader::Reader { len, r } => {
                self.buf.resize_with(len, Default::default);
                r.read_exact(self.buf)?;
                *self.packer = JsonbPacker::new(std::mem::take(self.packer))
                    .pack_slice(&self.buf)
                    .map_err(|e| DecodeError::Custom(e.to_string()))?;
            }
        }
        Ok(())
    }
    #[inline]
    fn uuid<'b, R: AvroRead>(
        self,
        r: ValueOrReader<'b, &'b [u8], R>,
    ) -> Result<Self::Out, AvroError> {
        let buf = match r {
            ValueOrReader::Value(val) => val,
            ValueOrReader::Reader { len, r } => {
                self.buf.resize_with(len, Default::default);
                r.read_exact(self.buf)?;
                &self.buf
            }
        };
        let s = std::str::from_utf8(&buf).map_err(|_e| DecodeError::UuidUtf8Error)?;
        self.packer.push(Datum::Uuid(
            Uuid::parse_str(s).map_err(DecodeError::BadUuid)?,
        ));
        Ok(())
    }
    #[inline]
    fn fixed<'b, R: AvroRead>(
        self,
        r: ValueOrReader<'b, &'b [u8], R>,
    ) -> Result<Self::Out, AvroError> {
        self.bytes(r)
    }
    #[inline]
    fn array<A: AvroArrayAccess>(mut self, a: &mut A) -> Result<Self::Out, AvroError> {
        self.is_top = false;
        let mut str_buf = std::mem::take(self.buf);
        self.packer.push_list_with(|rp| -> Result<(), AvroError> {
            loop {
                let next = AvroFlatDecoder {
                    packer: rp,
                    buf: &mut str_buf,
                    is_top: false,
                };
                if a.decode_next(next)?.is_none() {
                    break;
                }
            }
            Ok(())
        })?;
        *self.buf = str_buf;
        Ok(())
    }
    define_unexpected! {map}
}

/// Converts an Apache Avro schema into a list of column names and types.
pub fn validate_value_schema(
    schema: &str,
    envelope: EnvelopeType,
) -> anyhow::Result<Vec<(ColumnName, ColumnType)>> {
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

fn validate_schema_1(schema: SchemaNode) -> anyhow::Result<Vec<(ColumnName, ColumnType)>> {
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
) -> anyhow::Result<Vec<(ColumnName, ColumnType)>> {
    if let SchemaPiece::Union(us) = schema.inner {
        let mut columns = vec![];
        let vs = us.variants();
        if vs.is_empty() || (vs.len() == 1 && is_null(&vs[0])) {
            bail!(anyhow!("Empty or null-only unions are not supported"));
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
                columns.push((name.into(), ty.nullable(vs.len() > 1)));
                if let Some(named_idx) = named_idx {
                    seen_avro_nodes.remove(&named_idx);
                }
            }
        }
        Ok(columns)
    } else {
        let scalar_type = validate_schema_2(seen_avro_nodes, schema)?;
        Ok(vec![(base_name.into(), scalar_type.nullable(false))])
    }
}

fn validate_schema_2(
    seen_avro_nodes: &mut HashSet<usize>,
    schema: SchemaNode,
) -> anyhow::Result<ScalarType> {
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
        SchemaPiece::Uuid => ScalarType::Uuid,
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

pub fn parse_schema(schema: &str) -> anyhow::Result<Schema> {
    let schema = serde_json::from_str(schema)?;
    Schema::parse(&schema)
}

fn is_null(schema: &SchemaPieceOrNamed) -> bool {
    match schema {
        SchemaPieceOrNamed::Piece(SchemaPiece::Null) => true,
        _ => false,
    }
}

fn pack_value(v: Value, mut row: RowPacker, n: SchemaNode) -> anyhow::Result<RowPacker> {
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
        Value::Uuid(u) => row.push(Datum::Uuid(u)),
        other @ Value::Fixed(..)
        | other @ Value::Array(_)
        | other @ Value::Map(_)
        | other @ Value::Record(_) => bail!("unsupported avro value: {:?}", other),
    };
    Ok(row)
}

pub fn extract_nullable_row<'a, I>(v: Value, extra: I, n: SchemaNode) -> anyhow::Result<Option<Row>>
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

pub fn extract_row<'a, I>(v: Value, extra: I, n: SchemaNode) -> anyhow::Result<Option<Row>>
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
        /// (Or "", in the Postgres case)
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
        row: RowCoordinates,
        coord: Option<i64>,
        debug_name: &str,
        worker_idx: usize,
    ) -> bool {
        let (pos, row) = match row {
            RowCoordinates::MySql { pos, row } => (pos, row),
            RowCoordinates::Postgres { lsn } => (lsn, 0),
        };
        match self {
            DebeziumDeduplicationState::Ordered { binlog_offsets } => {
                match binlog_offsets.get_mut(file) {
                    Some((old_max_pos, old_max_row, old_offset)) => {
                        if (*old_max_pos, *old_max_row) >= (pos, row) {
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
                        *old_max_pos = pos;
                        *old_max_row = row;
                        *old_offset = coord;
                    }
                    None => {
                        // The extra lookup is fine - this is the cold path.
                        binlog_offsets.insert(file.to_owned(), (pos, row, coord));
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
) -> anyhow::Result<Value> {
    let (name, value) = fields.get_mut(idx).ok_or_else(|| {
        anyhow!(
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
    ) -> anyhow::Result<DiffPair<Row>> {
        fn is_snapshot(v: Value) -> anyhow::Result<Option<bool>> {
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
                        .ok_or_else(|| anyhow!("\"file\" is not a string"))?;
                        let pos_val = take_field_by_index(
                            schema_indices.source_pos_idx,
                            "pos",
                            &mut source_fields,
                        )?
                        .into_integral()
                        .ok_or_else(|| anyhow!("\"pos\" is not an integer"))?;
                        let row_val = take_field_by_index(
                            schema_indices.source_row_idx,
                            "row",
                            &mut source_fields,
                        )?
                        .into_integral()
                        .ok_or_else(|| anyhow!("\"row\" is not an integer"))?;
                        let pos = usize::try_from(pos_val)?;
                        let row = usize::try_from(row_val)?;
                        // TODO(btv) Add LSN handling here too (OCF code path).
                        // Better yet, just delete this and make it go through the same code path as Kafka
                        if !self.dedup.should_use_record(
                            &file_val,
                            RowCoordinates::MySql { pos, row },
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
    ) -> anyhow::Result<Decoder> {
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
    pub async fn decode(
        &mut self,
        mut bytes: &[u8],
        coord: Option<i64>,
    ) -> anyhow::Result<DiffPair<Row>> {
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
            let dec = AvroDebeziumDecoder {
                packer: &mut self.packer,
                buf: &mut self.buf1,
                file_buf: &mut self.buf2,
            };
            let dsr = GeneralDeserializer {
                schema: resolved_schema.top_node(),
            };
            // Unwrap is OK: we assert in Decoder::new that this is non-none when envelope == dbz.
            let dedup = self.debezium_dedup.as_mut().unwrap();
            let (diff, coords) = dsr.deserialize(&mut bytes, dec)?;
            let should_use = if let Some(source) = coords {
                // This would have ideally been `Option<&str>`,
                // but that can't be used to lookup in a `HashMap` of `Option<String>` without cloning.
                // So, just use `""` to represent lack of a filename.
                let file = match source.row {
                    // TODO - avoid the unnecessary utf8 check here.
                    RowCoordinates::MySql { .. } => std::str::from_utf8(&self.buf2)?,
                    RowCoordinates::Postgres { .. } => "",
                };
                source.snapshot
                    || dedup.should_use_record(
                        file,
                        source.row,
                        coord,
                        &self.debug_name,
                        self.worker_index,
                    )
            } else {
                true
            };
            if should_use {
                diff
            } else {
                DiffPair {
                    before: None,
                    after: None,
                }
            }
        } else {
            let dec = AvroFlatDecoder {
                packer: &mut self.packer,
                buf: &mut self.buf1,
                is_top: true,
            };
            let dsr = GeneralDeserializer {
                schema: resolved_schema.top_node(),
            };
            dsr.deserialize(&mut bytes, dec)?;
            DiffPair {
                before: None,
                after: Some(self.packer.finish_and_reuse()),
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
            ScalarType::Uuid => json!({
                "type": "string",
                "logicalType": "uuid",
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
    mz_avro::encode_unchecked(&avro, &DEBEZIUM_TRANSACTION_SCHEMA, &mut buf);
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
        let columns = column_names_and_types(desc);
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
        mz_avro::encode_unchecked(&avro, &self.writer_schema, &mut buf);
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
        mz_avro::write_avro_datum(
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
                let row = self.row_to_avro(row.iter());
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
                let row = self.row_to_avro(row.iter());
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

    pub fn row_to_avro<'a, I>(&self, row: I) -> Value
    where
        I: IntoIterator<Item = Datum<'a>>,
    {
        encode_datums_as_avro(row, &self.columns)
    }
}

/// Extracts deduplicated column names and types from a relation description.
pub fn column_names_and_types(desc: RelationDesc) -> Vec<(ColumnName, ColumnType)> {
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
    columns
}

/// Encodes a sequence of `Datum` as Avro, using supplied column names and types.
pub fn encode_datums_as_avro<'a, I>(datums: I, names_types: &[(ColumnName, ColumnType)]) -> Value
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let fields = names_types
        .iter()
        .zip_eq(datums)
        .map(|((name, typ), datum)| {
            let name = name.as_str().to_owned();
            use mz_avro::types::ToAvro;
            (name, TypedDatum::new(datum, typ.clone()).avro())
        })
        .collect();
    Value::Record(fields)
}

/// Bundled information sufficient to encode as Avro.
#[derive(Debug)]
pub struct TypedDatum<'a> {
    datum: Datum<'a>,
    typ: ColumnType,
}

impl<'a> TypedDatum<'a> {
    /// Pairs a datum and its type, for Avro encoding.
    pub fn new(datum: Datum<'a>, typ: ColumnType) -> Self {
        Self { datum, typ }
    }
}

impl<'a> mz_avro::types::ToAvro for TypedDatum<'a> {
    fn avro(self) -> Value {
        let TypedDatum { datum, typ } = self;
        if typ.nullable && datum.is_null() {
            Value::Union {
                index: 0,
                inner: Box::new(Value::Null),
                n_variants: 2,
                null_variant: Some(0),
            }
        } else {
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
                ScalarType::TimestampTz => Value::Timestamp(datum.unwrap_timestamptz().naive_utc()),
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
                ScalarType::Uuid => Value::Uuid(datum.unwrap_uuid()),
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
            val
        }
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
    async fn get(&mut self, id: i32, reader_schema: &Schema) -> anyhow::Result<Option<&Schema>> {
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

/// Logic for the Avro representation of the CDCv2 protocol.
pub mod cdc_v2 {

    use repr::{ColumnName, ColumnType, Diff, RelationDesc, Row, RowPacker, ScalarType, Timestamp};
    use serde_json::json;

    use super::RowWrapper;

    use avro_derive::AvroDecodeable;
    use differential_dataflow::capture::{Message, Progress};
    use mz_avro::schema::Schema;
    use mz_avro::types::Value;
    use mz_avro::{
        define_unexpected,
        error::{DecodeError, Error as AvroError},
        ArrayAsVecDecoder, AvroDecode, AvroDecodeable, AvroDeserializer, AvroRead,
        StatefulAvroDecodeable,
    };
    use std::{cell::RefCell, rc::Rc};

    /// Collected state to encode update batches and progress statements.
    #[derive(Debug)]
    pub struct Encoder {
        columns: Vec<(ColumnName, ColumnType)>,
        schema: Schema,
    }

    impl Encoder {
        /// Creates a new CDCv2 encoder from a relation description.
        pub fn new(desc: RelationDesc) -> Self {
            let columns = super::column_names_and_types(desc);
            let row_schema = build_row_schema_json(&columns, "data");
            let schema = build_schema(row_schema);
            Self { columns, schema }
        }

        /// Encodes a batch of updates as an Avro value.
        pub fn encode_updates(&self, updates: &[(Row, i64, i64)]) -> Value {
            let mut enc_updates = Vec::new();
            for (data, time, diff) in updates {
                let enc_data = super::encode_datums_as_avro(data, &self.columns);
                let enc_time = Value::Long(time.clone());
                let enc_diff = Value::Long(diff.clone());
                let mut enc_update = Vec::new();
                enc_update.push(("data".to_string(), enc_data));
                enc_update.push(("time".to_string(), enc_time));
                enc_update.push(("diff".to_string(), enc_diff));
                enc_updates.push(Value::Record(enc_update));
            }
            Value::Union {
                index: 0,
                inner: Box::new(Value::Array(enc_updates)),
                n_variants: 2,
                null_variant: None,
            }
        }

        /// Encodes the contents of a progress statement as an Avro value.
        pub fn encode_progress(
            &self,
            lower: &[i64],
            upper: &[i64],
            counts: &[(i64, i64)],
        ) -> Value {
            let enc_lower = Value::Array(lower.iter().cloned().map(Value::Long).collect());
            let enc_upper = Value::Array(upper.iter().cloned().map(Value::Long).collect());
            let enc_counts = Value::Array(
                counts
                    .iter()
                    .map(|(time, count)| {
                        Value::Record(vec![
                            ("time".to_string(), Value::Long(time.clone())),
                            ("count".to_string(), Value::Long(count.clone())),
                        ])
                    })
                    .collect(),
            );
            let enc_progress = Value::Record(vec![
                ("lower".to_string(), enc_lower),
                ("upper".to_string(), enc_upper),
                ("counts".to_string(), enc_counts),
            ]);

            Value::Union {
                index: 1,
                inner: Box::new(enc_progress),
                n_variants: 2,
                null_variant: None,
            }
        }
    }

    #[derive(AvroDecodeable)]
    #[state_type(Rc<RefCell<RowPacker>>, Rc<RefCell<Vec<u8>>>)]
    struct MyUpdate {
        #[state_expr(self._STATE.0.clone(), self._STATE.1.clone())]
        data: RowWrapper,
        timestamp: Timestamp,
        diff: Diff,
    }
    #[derive(AvroDecodeable)]
    struct Count {
        time: Timestamp,
        count: usize,
    }

    fn make_counts_decoder() -> impl AvroDecode<Out = Vec<(Timestamp, usize)>> {
        ArrayAsVecDecoder::new(|| {
            <Count as AvroDecodeable>::new_decoder().map_decoder(|ct| Ok((ct.time, ct.count)))
        })
    }
    #[derive(AvroDecodeable)]
    struct MyProgress {
        lower: Vec<Timestamp>,
        upper: Vec<Timestamp>,
        #[decoder_factory(make_counts_decoder)]
        counts: Vec<(Timestamp, usize)>,
    }

    impl AvroDecode for Decoder {
        type Out = Message<Row, Timestamp, Diff>;
        fn union_branch<'a, R: AvroRead, D: AvroDeserializer>(
            self,
            idx: usize,
            _n_variants: usize,
            _null_variant: Option<usize>,
            deserializer: D,
            r: &'a mut R,
        ) -> Result<Self::Out, AvroError> {
            match idx {
                0 => {
                    let packer = Rc::new(RefCell::new(RowPacker::new()));
                    let buf = Rc::new(RefCell::new(vec![]));
                    let d = ArrayAsVecDecoder::new(|| {
                        <MyUpdate as StatefulAvroDecodeable>::new_decoder((
                            packer.clone(),
                            buf.clone(),
                        ))
                        .map_decoder(|update| Ok((update.data.0, update.timestamp, update.diff)))
                    });
                    let updates = deserializer.deserialize(r, d)?;
                    Ok(Message::Updates(updates))
                }
                1 => {
                    let progress = deserializer
                        .deserialize(r, <MyProgress as AvroDecodeable>::new_decoder())?;
                    let progress = Progress {
                        lower: progress.lower,
                        upper: progress.upper,
                        counts: progress.counts,
                    };
                    Ok(Message::Progress(progress))
                }

                other => Err(DecodeError::Custom(format!(
                    "Unrecognized union variant in CDCv2 decoder: {}",
                    other
                ))
                .into()),
            }
        }
        define_unexpected! {
            record, array, map, enum_variant, scalar, decimal, bytes, string, json, uuid, fixed
        }
    }

    /// Collected state to decode update batches and progress statements.
    #[derive(Debug)]
    pub struct Decoder;

    /// Builds the JSON for the row schema, which can be independently useful.
    pub fn build_row_schema_json(
        columns: &[(ColumnName, ColumnType)],
        name: &str,
    ) -> serde_json::value::Value {
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
                }),
                ScalarType::Uuid => json!({
                    "type": "string",
                    "logicalType": "uuid",
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
        json!({
            "type": "record",
            "fields": fields,
            "name": name
        })
    }

    /// Construct the schema for the CDC V2 protocol.
    pub fn build_schema(row_schema: serde_json::Value) -> Schema {
        let updates_schema = json!({
            "type": "array",
            "items": {
                "name" : "update",
                "type" : "record",
                "fields" : [
                    {
                        "name": "data",
                        "type": row_schema,
                    },
                    {
                        "name" : "time",
                        "type" : "long",
                    },
                    {
                        "name" : "diff",
                        "type" : "long",
                    },
                ],
            },
        });

        let progress_schema = json!({
            "name" : "progress",
            "type" : "record",
            "fields" : [
                {
                    "name": "lower",
                    "type": {
                        "type": "array",
                        "items": "long"
                    }
                },
                {
                    "name": "upper",
                    "type": {
                        "type": "array",
                        "items": "long"
                    }
                },
                {
                    "name": "counts",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "counts",
                            "fields": [
                                {
                                    "name": "time",
                                    "type": "long",
                                },
                                {
                                    "name": "count",
                                    "type": "long",
                                },
                            ],
                        }
                    }
                },
            ],
        });
        let message_schema = json!([updates_schema, progress_schema,]);

        Schema::parse(&message_schema).expect("schema constrution failed")
    }

    #[cfg(test)]
    mod tests {

        use super::*;
        use mz_avro::AvroDeserializer;
        use mz_avro::GeneralDeserializer;

        #[test]
        fn test_roundtrip() {
            let desc = RelationDesc::empty()
                .with_column("id", ScalarType::Int64.nullable(false))
                .with_column("price", ScalarType::Float64.nullable(true));

            let encoder = Encoder::new(desc.clone());
            let row_schema =
                build_row_schema_json(&crate::avro::column_names_and_types(desc), "data");
            let schema = build_schema(row_schema);

            let values = vec![
                encoder.encode_updates(&[]),
                encoder.encode_progress(&[0], &[3], &[]),
                encoder.encode_progress(&[3], &[], &[]),
            ];
            use mz_avro::encode::encode_to_vec;;
            let mut values: Vec<_> = values
                .into_iter()
                .map(|v| encode_to_vec(&v, &schema))
                .collect();

            let g = GeneralDeserializer {
                schema: schema.top_node(),
            };
            assert!(matches!(
                g.deserialize(&mut &values.remove(0)[..], Decoder).unwrap(),
                Message::Updates(_)
            ),);
            assert!(matches!(
                g.deserialize(&mut &values.remove(0)[..], Decoder).unwrap(),
                Message::Progress(_)
            ),);
            assert!(matches!(
                g.deserialize(&mut &values.remove(0)[..], Decoder).unwrap(),
                Message::Progress(_)
            ),);
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use ordered_float::OrderedFloat;
    use serde::Deserialize;
    use std::fs::File;

    use mz_avro::types::{DecimalValue, Value};
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
    fn test_schema_parsing() -> anyhow::Result<()> {
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
    fn test_diff_pair_to_avro_primitive_types() -> anyhow::Result<()> {
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
            let desc = RelationDesc::empty().with_column("column1", typ.nullable(false));
            let avro_value = Encoder::new(desc, false).row_to_avro(std::iter::once(datum));
            assert_eq!(
                Value::Record(vec![("column1".into(), expected)]),
                avro_value
            );
        }

        Ok(())
    }
}
