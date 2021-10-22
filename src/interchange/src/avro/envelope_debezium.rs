// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Envelope: Debezium
//!
//! The most important struct in here is the [`AvroDebeziumDecoder`]

use std::collections::HashMap;
use std::num::ParseIntError;

use anyhow::anyhow;
use anyhow::bail;
use smallvec::SmallVec;

use mz_avro::error::{DecodeError, Error as AvroError};
use mz_avro::schema::{RecordField, SchemaNode, SchemaPiece};
use mz_avro::types::{Scalar, Value};
use mz_avro::{
    define_unexpected, give_value, AvroDecode, AvroDeserializer, AvroRead, AvroRecordAccess,
    Schema, ValueOrReader,
};
use mz_avro::{TrivialDecoder, ValueDecoder};
use ore::str::StrExt;
use repr::{Datum, Row};

use crate::avro::{AvroFlatDecoder, AvroStringDecoder, OptionalRecordDecoder};

mod deduplication;

pub use deduplication::DebeziumDeduplicationStrategy;

use self::deduplication::DebeziumDeduplicationState;

#[derive(Debug)]
pub struct AvroDebeziumDecoder<'a> {
    pub packer: &'a mut Row,
    pub buf: &'a mut Vec<u8>,
    pub file_buf: &'a mut Vec<u8>,
    pub filenames_to_indices: &'a mut HashMap<Vec<u8>, usize>,
}

impl<'a> AvroDecode for AvroDebeziumDecoder<'a> {
    type Out = Option<DebeziumSourceCoordinates>;
    fn record<R: AvroRead, A: AvroRecordAccess<R>>(
        self,
        a: &mut A,
    ) -> Result<Self::Out, AvroError> {
        let mut transaction = None;
        let mut coords = None;
        while let Some((name, _, field)) = a.next_field()? {
            match name {
                "before" => {
                    let d = OptionalRecordDecoder {
                        packer: self.packer,
                        buf: self.buf,
                    };
                    let decoded_row = field.decode_field(d)?;
                    if !decoded_row {
                        self.packer.push(Datum::Null);
                    }
                }
                "after" => {
                    let d = OptionalRecordDecoder {
                        packer: self.packer,
                        buf: self.buf,
                    };
                    let decoded_row = field.decode_field(d)?;
                    if !decoded_row {
                        self.packer.push(Datum::Null);
                    }
                }
                "source" => {
                    let d = DebeziumSourceDecoder {
                        file_buf: self.file_buf,
                        filenames_to_indices: self.filenames_to_indices,
                    };
                    coords = Some(field.decode_field(d)?);
                }
                "transaction" => {
                    let d = DebeziumTransactionDecoder;
                    transaction = field.decode_field(d)?;
                }
                _ => {
                    field.decode_field(TrivialDecoder)?;
                }
            }
        }
        if let Some(transaction) = transaction {
            if let Some(DebeziumSourceCoordinates {
                row: RowCoordinates::Postgres { total_order, .. },
                ..
            }) = coords.as_mut()
            {
                *total_order = Some(transaction.total_order);
            }
        }
        Ok(coords)
    }
    define_unexpected! {
        union_branch, array, map, enum_variant, scalar, decimal, bytes, string, json, uuid, fixed
    }
}

// See https://rusanu.com/2012/01/17/what-is-an-lsn-log-sequence-number/
#[derive(Debug, Copy, Clone)]
pub(crate) struct MSSqlLsn {
    pub(crate) file_seq_num: u32,
    pub(crate) log_block_offset: u32,
    pub(crate) slot_num: u16,
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum RowCoordinates {
    MySql {
        file_idx: usize,
        pos: usize,
        row: usize,
    },
    Postgres {
        last_commit_lsn: Option<usize>,
        lsn: usize,
        total_order: Option<usize>,
    },
    MSSql {
        change_lsn: MSSqlLsn,
        event_serial_no: usize,
    },
    Unknown,
}

#[derive(Copy, Clone, Debug)]
pub struct DebeziumSourceCoordinates {
    pub(super) snapshot: bool,
    pub(super) row: RowCoordinates,
}

#[derive(Debug)]
pub struct DebeziumTransactionMetadata {
    // The order of the record within the transaction
    total_order: usize,
}

struct DebeziumSourceDecoder<'a> {
    file_buf: &'a mut Vec<u8>,
    filenames_to_indices: &'a mut HashMap<Vec<u8>, usize>,
}

struct DebeziumTransactionDecoder;

/// Whether the debezium decoder is currently parsing snapshot rows
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

fn decode_change_lsn(input: &str) -> Option<MSSqlLsn> {
    // SQL Server change LSNs are 10-byte integers. Debezium
    // encodes them as hex, in the following format: xxxxxxxx:xxxxxxxx:xxxx
    if input.len() != 22 {
        return None;
    }
    if input.as_bytes()[8] != b':' || input.as_bytes()[17] != b':' {
        return None;
    }
    let file_seq_num = u32::from_str_radix(&input[0..8], 16).ok()?;
    let log_block_offset = u32::from_str_radix(&input[9..17], 16).ok()?;
    let slot_num = u16::from_str_radix(&input[18..22], 16).ok()?;

    Some(MSSqlLsn {
        file_seq_num,
        log_block_offset,
        slot_num,
    })
}

// TODO - If #[derive(AvroDecodable)] supported optional fields, we wouldn't need to do this by hand.
impl AvroDecode for DebeziumTransactionDecoder {
    type Out = Option<DebeziumTransactionMetadata>;
    fn record<R: AvroRead, A: AvroRecordAccess<R>>(
        self,
        a: &mut A,
    ) -> Result<Self::Out, AvroError> {
        let mut total_order = None;
        while let Some((name, _, field)) = a.next_field()? {
            match name {
                "total_order" => {
                    let val = field.decode_field(ValueDecoder)?;
                    total_order = Some(val.into_usize().ok_or_else(|| {
                        DecodeError::Custom("\"total_order\" is not an integer".to_string())
                    })?);
                }
                _ => field.decode_field(TrivialDecoder)?,
            }
        }
        Ok(total_order.map(|total_order| DebeziumTransactionMetadata { total_order }))
    }
    fn union_branch<'a, R: AvroRead, D: AvroDeserializer>(
        self,
        idx: usize,
        _n_variants: usize,
        null_variant: Option<usize>,
        deserializer: D,
        reader: &'a mut R,
    ) -> Result<Self::Out, AvroError> {
        if Some(idx) == null_variant {
            Ok(None)
        } else {
            deserializer.deserialize(reader, self)
        }
    }
    define_unexpected! {
        array, map, enum_variant, scalar, decimal, bytes, string, json, uuid, fixed
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
        // Additional sequencing information for Postgres sources
        let mut sequence: Option<Vec<Option<usize>>> = None;
        // SQL Server lsn - 10-byte, hex-encoded value.
        // and "event_serial_no" - serial number of the event, when there is more than one per LSN.
        let mut change_lsn = None;
        let mut event_serial_no = None;

        let mut file_idx = None;
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
                // MySQL
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
                    file_idx = Some(match self.filenames_to_indices.get(self.file_buf) {
                        Some(idx) => *idx,
                        None => {
                            let n_files = self.filenames_to_indices.len();
                            self.filenames_to_indices
                                .insert(std::mem::take(self.file_buf), n_files);
                            n_files
                        }
                    });
                }
                // Postgres
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
                "sequence" => {
                    let next = ValueDecoder;
                    let val = field.decode_field(next)?;
                    let val = match val {
                        Value::Union { inner, .. } => *inner,
                        val => val,
                    };
                    let val = match val {
                        Value::String(val) => val,
                        Value::Null => continue,
                        _ => {
                            return Err(AvroError::Decode(DecodeError::Custom(
                                "\"sequence\" is not a string".to_string(),
                            )))
                        }
                    };
                    let seq: Vec<Option<String>> = serde_json::from_str(&val)
                        .map_err(|e| AvroError::Decode(DecodeError::Custom(e.to_string())))?;
                    sequence = Some(
                        seq.into_iter()
                            .map(|s| s.map(|s| s.parse()).transpose())
                            .collect::<Result<_, ParseIntError>>()
                            .map_err(|e| AvroError::Decode(DecodeError::Custom(e.to_string())))?,
                    );
                }
                // SQL Server
                "change_lsn" => {
                    let next = ValueDecoder;
                    let val = field.decode_field(next)?;
                    let val = match val {
                        Value::Union { inner, .. } => *inner,
                        val => val,
                    };
                    match val {
                        Value::Null => {}
                        Value::String(s) => {
                            if let Some(i) = decode_change_lsn(&s) {
                                change_lsn = Some(i);
                            } else {
                                return Err(AvroError::Decode(DecodeError::Custom(format!(
                                    "Couldn't decode MS SQL LSN: {}",
                                    s
                                ))));
                            }
                        }
                        _ => {
                            return Err(AvroError::Decode(DecodeError::Custom(
                                "\"change_lsn\" is not a string".to_string(),
                            )))
                        }
                    }
                }
                "event_serial_no" => {
                    let next = ValueDecoder;
                    let val = field.decode_field(next)?;
                    let val = match val {
                        Value::Union { inner, .. } => *inner,
                        val => val,
                    };
                    event_serial_no = match val {
                        Value::Null => None,
                        Value::Int(i) => Some(i.into()),
                        Value::Long(i) => Some(i),
                        val => {
                            return Err(AvroError::Decode(DecodeError::Custom(format!(
                                "\"event_serial_no\" is not an integer: {:?}",
                                val
                            ))))
                        }
                    };
                }
                _ => {
                    field.decode_field(TrivialDecoder)?;
                }
            }
        }
        let mysql_any = pos.is_some() || row.is_some() || file_idx.is_some();
        let pg_any = lsn.is_some();
        let mssql_any = change_lsn.is_some() || event_serial_no.is_some();
        if (mysql_any as usize) + (pg_any as usize) + (mssql_any as usize) > 1 {
            return Err(DecodeError::Custom(
            "Found source coordinate information for multiple databases - we don't know how to interpret this.".to_string()).into());
        }
        let row = if mysql_any {
            let pos = pos.ok_or_else(|| DecodeError::Custom("no pos".to_string()))? as usize;
            let row = row.ok_or_else(|| DecodeError::Custom("no row".to_string()))? as usize;
            let file_idx = file_idx
                .ok_or_else(|| DecodeError::Custom("no binlog filename".to_string()))?
                as usize;
            RowCoordinates::MySql { file_idx, pos, row }
        } else if pg_any {
            let last_commit_lsn = match sequence {
                Some(sequence) => sequence.get(0).cloned().expect("lastCommitLsn must exist"),
                None => None,
            };
            let lsn = lsn.ok_or_else(|| DecodeError::Custom("no lsn".to_string()))? as usize;
            RowCoordinates::Postgres {
                last_commit_lsn,
                lsn,
                total_order: None,
            }
        } else if mssql_any {
            let change_lsn =
                change_lsn.ok_or_else(|| DecodeError::Custom("no change_lsn".to_string()))?;
            let event_serial_no = event_serial_no
                .ok_or_else(|| DecodeError::Custom("no event_serial_no".to_string()))?
                as usize;
            RowCoordinates::MSSql {
                change_lsn,
                event_serial_no,
            }
        } else {
            RowCoordinates::Unknown
        };
        Ok(DebeziumSourceCoordinates { snapshot, row })
    }
    define_unexpected! {
        union_branch, array, map, enum_variant, scalar, decimal, bytes, string, json, uuid, fixed
    }
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

/// Additional context needed for decoding
/// Debezium-formatted data.
#[derive(Debug)]
pub struct DebeziumDecodeState {
    /// Index of the "before" field in the payload schema
    before_idx: usize,
    /// Index of the "after" field in the payload schema
    after_idx: usize,
    dedup: Option<DebeziumDeduplicationState>,
    binlog_schema_indices: Option<BinlogSchemaIndices>,
    /// Human-readable name used for printing debug information
    debug_name: String,
    /// Worker we are running on (used for printing debug information)
    worker_idx: usize,
    /// Map of binlog filenames to a unique index. Used to avoid having to write the full filename
    /// in the output row.
    filenames_to_indices: HashMap<Vec<u8>, usize>,
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
    let (name, value) = fields.get_mut(idx).ok_or_else(|| -> anyhow::Error {
        anyhow!(
            "Value does not match schema: {} field not at index {}",
            expected_name.quoted(),
            idx
        )
    })?;
    if name != expected_name {
        bail!(
            "Value does not match schema: expected {}, found {}",
            expected_name.quoted(),
            name.quoted()
        );
    }
    Ok(std::mem::replace(value, Value::Null))
}

// TODO [btv] - this entire struct is ONLY used in the OCF code path. It should be deleted and merged with
// `Decoder` as part of the source-orthogonality refactor.
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
            dedup: DebeziumDeduplicationState::new(dedup_strat, None),
            binlog_schema_indices,
            debug_name,
            worker_idx,
            filenames_to_indices: Default::default(),
        })
    }

    pub fn extract(
        &mut self,
        v: Value,
        upstream_time_millis: Option<i64>,
    ) -> anyhow::Result<Option<Row>> {
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
                let dedup_val = if let Some(schema_indices) = self.binlog_schema_indices {
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
                        let n_fnames = self.filenames_to_indices.len();
                        let file_idx = *self
                            .filenames_to_indices
                            .entry(file_val.into_bytes())
                            .or_insert(n_fnames);
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
                        Some((file_idx, pos, row))
                    } else {
                        None
                    }
                } else {
                    None
                };
                let before_idx = fields.iter().position(|(name, _value)| "before" == name);
                let after_idx = fields.iter().position(|(name, _value)| "after" == name);
                if let (Some(before_idx), Some(after_idx)) = (before_idx, after_idx) {
                    let before = &fields[before_idx].1;
                    let after = &fields[after_idx].1;
                    let mut packer = Row::default();
                    let mut buf = vec![];
                    give_value(
                        AvroFlatDecoder {
                            packer: &mut packer,
                            buf: &mut buf,
                            is_top: false,
                        },
                        before,
                    )?;
                    give_value(
                        AvroFlatDecoder {
                            packer: &mut packer,
                            buf: &mut buf,
                            is_top: false,
                        },
                        after,
                    )?;
                    if let Some((file_idx, pos, row)) = dedup_val {
                        packer.push_list_with(|packer| {
                            packer.push(Datum::Int32(file_idx as i32));
                            packer.push(Datum::Int64(pos as i64));
                            packer.push(Datum::Int64(row as i64));
                        });
                    } else {
                        packer.push(Datum::Null);
                    }
                    packer.push(match upstream_time_millis {
                        Some(millis) => Datum::Int64(millis),
                        None => Datum::Null,
                    });
                    Ok(Some(packer))
                } else {
                    bail!("avro envelope does not contain `before` and `after`");
                }
            }
            _ => bail!("avro envelope had unexpected type: {:?}", v),
        }
    }
}
