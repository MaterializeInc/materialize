// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::io::Read;
use std::rc::Rc;

use anyhow::bail;
use ordered_float::OrderedFloat;

use mz_avro::error::{DecodeError, Error as AvroError};
use mz_avro::schema::{SchemaNode, SchemaPiece};
use mz_avro::types::{DecimalValue, Value};
use mz_avro::{
    define_unexpected, give_value, AvroArrayAccess, AvroDecode, AvroDeserializer, AvroMapAccess,
    AvroRead, AvroRecordAccess, GeneralDeserializer, StatefulAvroDecodable, ValueDecoder,
    ValueOrReader,
};
use repr::adt::decimal::Significand;
use repr::adt::jsonb::JsonbPacker;
use repr::{Datum, Row, RowPacker};
use uuid::Uuid;

use super::{
    is_null, AvroDebeziumDecoder, ConfluentAvroResolver, DebeziumDeduplicationState,
    DebeziumDeduplicationStrategy, EnvelopeType, RowCoordinates,
};

/// Manages decoding of Avro-encoded bytes.
pub struct Decoder {
    csr_avro: ConfluentAvroResolver,
    envelope: EnvelopeType,
    debezium_dedup: Option<DebeziumDeduplicationState>,
    debug_name: String,
    worker_index: usize,
    buf1: Vec<u8>,
    buf2: Vec<u8>,
    packer: RowPacker,
    reject_non_inserts: bool,
}

impl fmt::Debug for Decoder {
    // TODO - rethink the usefulness of this debug impl. The Decoder
    // has become much more complicated since it was written
    // (though, maybe _that_ is the root problem we should solve...)
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Decoder")
            .field("csr_avro", &self.csr_avro)
            .finish()
    }
}

impl Decoder {
    /// Creates a new `Decoder`
    ///
    /// The provided schema is called the "reader schema", which is the schema
    /// that we are expecting to use to decode records. The records may indicate
    /// that they are encoded with a different schema; as long as those.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        reader_schema: &str,
        schema_registry: Option<ccsr::ClientConfig>,
        envelope: EnvelopeType,
        debug_name: String,
        worker_index: usize,
        debezium_dedup: Option<DebeziumDeduplicationStrategy>,
        key_indices: Option<Vec<usize>>,
        confluent_wire_format: bool,
        reject_non_inserts: bool,
    ) -> anyhow::Result<Decoder> {
        assert!(
            (envelope == EnvelopeType::Debezium && debezium_dedup.is_some())
                || (envelope != EnvelopeType::Debezium && debezium_dedup.is_none())
        );
        let debezium_dedup =
            debezium_dedup.map(|strat| DebeziumDeduplicationState::new(strat, key_indices));
        let csr_avro =
            ConfluentAvroResolver::new(reader_schema, schema_registry, confluent_wire_format)?;

        Ok(Decoder {
            csr_avro,
            envelope,
            debezium_dedup,
            debug_name,
            worker_index,
            buf1: vec![],
            buf2: vec![],
            packer: Default::default(),
            reject_non_inserts,
        })
    }

    /// Decodes Avro-encoded `bytes` into a `DiffPair`.
    pub async fn decode(
        &mut self,
        bytes: &[u8],
        coord: Option<i64>,
        upstream_time_millis: Option<i64>,
    ) -> anyhow::Result<Option<Row>> {
        let (mut bytes, resolved_schema) = self.csr_avro.resolve(bytes).await?;
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
            let (row, coords) = dsr.deserialize(&mut bytes, dec)?;
            if self.reject_non_inserts {
                if !matches!(row.iter().next(), None | Some(Datum::Null)) {
                    panic!(
                        "[customer-data] Updates and deletes are not allowed for this source! \
                         This probably means it was started with `start_offset`. Got row: {:?}",
                        row
                    )
                }
            }
            let should_use = if let Some(source) = coords {
                let mssql_fsn_buf;
                // This would have ideally been `Option<&[u8]>`,
                // but that can't be used to lookup in a `HashMap` of `Option<Vec<u8>>` without cloning.
                // So, just use `""` to represent lack of a filename.
                let file = match source.row {
                    RowCoordinates::MySql { .. } => &self.buf2,
                    RowCoordinates::MSSql { change_lsn, .. } => {
                        mssql_fsn_buf = change_lsn.file_seq_num.to_ne_bytes();
                        &mssql_fsn_buf[..]
                    }
                    RowCoordinates::Postgres { .. } => &b""[..],
                    RowCoordinates::Unknown { .. } => &b""[..],
                };
                dedup.should_use_record(
                    file,
                    source.row,
                    coord,
                    upstream_time_millis,
                    &self.debug_name,
                    self.worker_index,
                    source.snapshot,
                    &row,
                )
            } else {
                true
            };
            if should_use {
                Some(row)
            } else {
                None
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
            Some(self.packer.finish_and_reuse())
        };
        log::trace!(
            "[customer-data] Decoded row {:?}{} in {}",
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

pub struct AvroStringDecoder<'a> {
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

pub(super) struct OptionalRecordDecoder<'a> {
    pub packer: &'a mut RowPacker,
    pub buf: &'a mut Vec<u8>,
}

impl<'a> AvroDecode for OptionalRecordDecoder<'a> {
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
                is_top: false,
            };
            deserializer.deserialize(reader, d)?;
            Ok(true)
        }
    }
    define_unexpected! {
        record, array, map, enum_variant, scalar, decimal, bytes, string, json, uuid, fixed
    }
}

pub(super) struct RowDecoder {
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
pub(super) struct RowWrapper(pub Row);

impl StatefulAvroDecodable for RowWrapper {
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
    #[inline]
    fn map<A: AvroMapAccess>(self, a: &mut A) -> Result<Self::Out, AvroError> {
        // Map (key, value) pairs need to be unique and ordered.
        let mut map = BTreeMap::new();
        while let Some((name, f)) = a.next_entry()? {
            map.insert(name, f.decode_field(ValueDecoder)?);
        }
        self.packer
            .push_dict_with(|packer| -> Result<(), AvroError> {
                for (key, val) in map {
                    packer.push(Datum::String(key.as_str()));
                    give_value(
                        AvroFlatDecoder {
                            packer,
                            buf: &mut vec![],
                            is_top: false,
                        },
                        &val,
                    )?;
                }
                Ok(())
            })?;

        Ok(())
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

#[derive(Clone, Debug)]
pub struct DiffPair<T> {
    pub before: Option<T>,
    pub after: Option<T>,
}
