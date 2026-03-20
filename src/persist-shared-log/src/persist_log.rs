// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Acceptor and learner implementation backed by a persist shard.
//!
//! The `WriteHandle` drives the acceptor (blind writes); the
//! `ReadHandle`/`Subscribe` drives the learner (CAS evaluation + reads).
//! Data lives in differential format.
//!
//! A single persist shard stores all proposals:
//! - K: [`OrderedKey`] — `(batch_id, position, shard)` StructArray that gives
//!   a stable total order through compaction
//! - V: [`Proposal`] — serialized protobuf bytes
//! - T: `u64` (incremented by 1 per batch, in lock-step with persist upper)
//! - D: `i64` (+1 for proposals, -1 for learner retractions)

pub mod acceptor;
pub mod client;
pub mod latency_blob;
pub mod learner;

use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BinaryArray, BinaryBuilder, StringArray, StringBuilder,
    StructArray, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field};
use bytes::{BufMut, Bytes};

use mz_persist_types::Codec;
use mz_persist_types::codec_impls::{
    SimpleColumnarData, SimpleColumnarDecoder, SimpleColumnarEncoder,
};
use mz_persist_types::columnar::{ColumnDecoder, ColumnEncoder, Schema};
use mz_persist_types::stats::NoneStats;
use mz_persist_types::stats::structured::StructStats;

/// A consensus proposal stored as the value in a persist shard.
///
/// Wraps serialized `ProtoLogProposal` protobuf bytes. Each proposal is stored
/// as a single row in the persist shard at timestamp T (the batch number),
/// paired with an [`OrderedKey`] that provides stable total ordering.
///
/// Uses [`Bytes`] for the encoded payload so that cloning (e.g. on
/// compare-and-append retries) is O(1) via refcount instead of a memcpy.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Proposal {
    /// Serialized ProtoLogProposal (protobuf bytes).
    pub encoded: Bytes,
}

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

/// Stateless schema for [`Proposal`].
#[derive(Debug, PartialEq)]
pub struct ProposalSchema;

impl Codec for Proposal {
    type Storage = ();
    type Schema = ProposalSchema;

    fn codec_name() -> String {
        "Proposal".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        buf.put(self.encoded.as_ref());
    }

    fn decode<'a>(buf: &'a [u8], _schema: &ProposalSchema) -> Result<Self, String> {
        Ok(Proposal {
            encoded: Bytes::copy_from_slice(buf),
        })
    }

    fn encode_schema(_schema: &Self::Schema) -> Bytes {
        Bytes::new()
    }

    fn decode_schema(buf: &Bytes) -> Self::Schema {
        assert_eq!(*buf, Bytes::new());
        ProposalSchema
    }
}

// ---------------------------------------------------------------------------
// SimpleColumnarData (maps to BinaryBuilder / BinaryArray)
// ---------------------------------------------------------------------------

impl SimpleColumnarData for Proposal {
    type ArrowBuilder = BinaryBuilder;
    type ArrowColumn = BinaryArray;

    fn goodbytes(builder: &Self::ArrowBuilder) -> usize {
        builder.values_slice().len()
    }

    fn push(&self, builder: &mut Self::ArrowBuilder) {
        builder.append_value(self.encoded.as_ref());
    }

    fn push_null(builder: &mut Self::ArrowBuilder) {
        builder.append_null();
    }

    fn read(&mut self, idx: usize, column: &Self::ArrowColumn) {
        // Allocates a fresh Bytes per proposal. This is on the learner's listen
        // path (one allocation per proposal per batch), which is acceptable.
        self.encoded = Bytes::copy_from_slice(column.value(idx));
    }
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

impl Schema<Proposal> for ProposalSchema {
    type ArrowColumn = BinaryArray;
    type Statistics = NoneStats;

    type Decoder = SimpleColumnarDecoder<Proposal>;
    type Encoder = SimpleColumnarEncoder<Proposal>;

    fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
        Ok(SimpleColumnarEncoder::default())
    }

    fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
        Ok(SimpleColumnarDecoder::new(col))
    }
}

// ===========================================================================
// OrderedKey
// ===========================================================================

/// The key for proposals in the persist shard.
///
/// Encodes `(batch_id, position, shard)` as a StructArray so that Arrow-native
/// sorting gives a stable total order through compaction:
///
/// 1. `batch_id` — derived from the persist upper at flush time
/// 2. `position` — index within the batch (0..N-1)
/// 3. `shard` — the consensus shard key (e.g. `"s0"`)
///
/// `(batch_id, position)` is globally unique, so consolidation never merges
/// distinct proposals. The `shard` field enables pushdown filtering by
/// consensus key name.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderedKey {
    pub batch_id: u64,
    pub position: u32,
    pub shard: String,
}

// ---------------------------------------------------------------------------
// OrderedKey Codec
// ---------------------------------------------------------------------------

/// Stateless schema for [`OrderedKey`].
#[derive(Debug, PartialEq)]
pub struct OrderedKeySchema;

impl Codec for OrderedKey {
    type Storage = ();
    type Schema = OrderedKeySchema;

    fn codec_name() -> String {
        "OrderedKey".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        buf.put_u64(self.batch_id);
        buf.put_u32(self.position);
        buf.put_u32(self.shard.len() as u32);
        buf.put(self.shard.as_bytes());
    }

    fn decode<'a>(buf: &'a [u8], _schema: &OrderedKeySchema) -> Result<Self, String> {
        if buf.len() < 16 {
            return Err(format!(
                "OrderedKey: expected at least 16 bytes, got {}",
                buf.len()
            ));
        }
        let batch_id = u64::from_be_bytes(buf[0..8].try_into().unwrap());
        let position = u32::from_be_bytes(buf[8..12].try_into().unwrap());
        let shard_len = u32::from_be_bytes(buf[12..16].try_into().unwrap()) as usize;
        if buf.len() < 16 + shard_len {
            return Err(format!(
                "OrderedKey: expected {} bytes for shard, got {}",
                shard_len,
                buf.len() - 16
            ));
        }
        let shard =
            String::from_utf8(buf[16..16 + shard_len].to_vec()).map_err(|e| e.to_string())?;
        Ok(OrderedKey {
            batch_id,
            position,
            shard,
        })
    }

    fn encode_schema(_schema: &Self::Schema) -> Bytes {
        Bytes::new()
    }

    fn decode_schema(buf: &Bytes) -> Self::Schema {
        assert_eq!(*buf, Bytes::new());
        OrderedKeySchema
    }
}

// ---------------------------------------------------------------------------
// OrderedKey ColumnEncoder / ColumnDecoder
// ---------------------------------------------------------------------------

/// Columnar encoder for [`OrderedKey`].
///
/// Produces a `StructArray` with fields `[batch_id, position, shard]`.
/// Field declaration order is load-bearing: `ArrayOrd::Struct` compares
/// field-by-field in declaration order, giving the total ordering
/// `batch_id > position > shard`.
#[derive(Debug)]
pub struct OrderedKeyEncoder {
    batch_id: UInt64Builder,
    position: UInt32Builder,
    shard: StringBuilder,
}

impl ColumnEncoder<OrderedKey> for OrderedKeyEncoder {
    type FinishedColumn = StructArray;

    fn goodbytes(&self) -> usize {
        self.batch_id.values_slice().len() * 8
            + self.position.values_slice().len() * 4
            + self.shard.values_slice().len()
    }

    fn append(&mut self, val: &OrderedKey) {
        self.batch_id.append_value(val.batch_id);
        self.position.append_value(val.position);
        self.shard.append_value(&val.shard);
    }

    fn append_null(&mut self) {
        self.batch_id.append_null();
        self.position.append_null();
        self.shard.append_null();
    }

    fn finish(mut self) -> StructArray {
        let fields: Vec<Field> = vec![
            Field::new("batch_id", DataType::UInt64, false),
            Field::new("position", DataType::UInt32, false),
            Field::new("shard", DataType::Utf8, false),
        ];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(ArrayBuilder::finish(&mut self.batch_id)),
            Arc::new(ArrayBuilder::finish(&mut self.position)),
            Arc::new(ArrayBuilder::finish(&mut self.shard)),
        ];
        StructArray::new(fields.into(), arrays, None)
    }
}

/// Columnar decoder for [`OrderedKey`].
#[derive(Debug)]
pub struct OrderedKeyDecoder {
    batch_id: UInt64Array,
    position: UInt32Array,
    shard: StringArray,
}

impl ColumnDecoder<OrderedKey> for OrderedKeyDecoder {
    fn decode(&self, idx: usize, val: &mut OrderedKey) {
        val.batch_id = self.batch_id.value(idx);
        val.position = self.position.value(idx);
        val.shard = self.shard.value(idx).to_string();
    }

    fn is_null(&self, _idx: usize) -> bool {
        false
    }

    fn goodbytes(&self) -> usize {
        self.batch_id.values().len() * 8
            + self.position.values().len() * 4
            + self.shard.values().len()
    }

    fn stats(&self) -> StructStats {
        StructStats {
            len: self.batch_id.len(),
            cols: Default::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// OrderedKey Schema
// ---------------------------------------------------------------------------

impl Schema<OrderedKey> for OrderedKeySchema {
    type ArrowColumn = StructArray;
    type Statistics = NoneStats;
    type Decoder = OrderedKeyDecoder;
    type Encoder = OrderedKeyEncoder;

    fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
        Ok(OrderedKeyEncoder {
            batch_id: UInt64Builder::new(),
            position: UInt32Builder::new(),
            shard: StringBuilder::new(),
        })
    }

    fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
        let batch_id = col
            .column_by_name("batch_id")
            .ok_or_else(|| anyhow::anyhow!("missing batch_id column"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow::anyhow!("batch_id is not UInt64Array"))?
            .clone();
        let position = col
            .column_by_name("position")
            .ok_or_else(|| anyhow::anyhow!("missing position column"))?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| anyhow::anyhow!("position is not UInt32Array"))?
            .clone();
        let shard = col
            .column_by_name("shard")
            .ok_or_else(|| anyhow::anyhow!("missing shard column"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("shard is not StringArray"))?
            .clone();
        Ok(OrderedKeyDecoder {
            batch_id,
            position,
            shard,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the shard name (consensus key) from a serialized `ProtoLogProposal`.
///
/// Decodes just enough of the protobuf to extract the `key` field from
/// either a `ProtoCasProposal` or `ProtoTruncateProposal`.
pub fn extract_shard_name(encoded: &[u8]) -> String {
    use mz_persist::generated::consensus_service::{ProtoLogProposal, proto_log_proposal};
    use prost::Message;

    match ProtoLogProposal::decode(encoded) {
        Ok(proposal) => match proposal.op {
            Some(proto_log_proposal::Op::Cas(cas)) => cas.key,
            Some(proto_log_proposal::Op::Truncate(trunc)) => trunc.key,
            None => String::new(),
        },
        Err(_) => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_persist_types::columnar::{ColumnEncoder, Schema};
    use mz_persist_types::Codec;

    #[test]
    fn ordered_key_codec_roundtrip() {
        let key = OrderedKey {
            batch_id: 42,
            position: 7,
            shard: "s0".into(),
        };
        let mut buf = Vec::new();
        key.encode(&mut buf);
        let decoded = OrderedKey::decode(&buf, &OrderedKeySchema).unwrap();
        assert_eq!(key, decoded);
    }

    /// Verify that the StructArray-based columnar encoding sorts OrderedKeys
    /// in (batch_id, position, shard) order — the same order that persist's
    /// compaction uses (via `ArrayOrd::Struct`). This is the key property
    /// that makes proposal ordering stable through compaction.
    #[test]
    fn ordered_key_arrow_sort_order() {
        use mz_persist_types::arrow::ArrayOrd;

        // Keys deliberately inserted out of order.
        let keys = vec![
            OrderedKey { batch_id: 3, position: 0, shard: "s0".into() },
            OrderedKey { batch_id: 1, position: 1, shard: "s1".into() },
            OrderedKey { batch_id: 1, position: 0, shard: "s0".into() },
            OrderedKey { batch_id: 2, position: 0, shard: "s1".into() },
            OrderedKey { batch_id: 1, position: 0, shard: "s1".into() },
        ];

        let mut encoder = OrderedKeySchema.encoder().unwrap();
        for key in &keys {
            encoder.append(key);
        }
        let array = encoder.finish();

        // Use ArrayOrd (the same comparator persist's compaction uses) to
        // sort the indices.
        let ord = ArrayOrd::new(&array);
        let mut indices: Vec<usize> = (0..keys.len()).collect();
        indices.sort_by(|a, b| ord.at(*a).cmp(&ord.at(*b)));

        // Read back in sorted order.
        let decoder = OrderedKeySchema.decoder(array).unwrap();
        let sorted: Vec<OrderedKey> = indices
            .iter()
            .map(|&i| {
                let mut k = OrderedKey::default();
                decoder.decode(i, &mut k);
                k
            })
            .collect();

        // Expected order: (1,0,s0), (1,0,s1), (1,1,s1), (2,0,s1), (3,0,s0)
        assert_eq!(
            sorted,
            vec![
                OrderedKey { batch_id: 1, position: 0, shard: "s0".into() },
                OrderedKey { batch_id: 1, position: 0, shard: "s1".into() },
                OrderedKey { batch_id: 1, position: 1, shard: "s1".into() },
                OrderedKey { batch_id: 2, position: 0, shard: "s1".into() },
                OrderedKey { batch_id: 3, position: 0, shard: "s0".into() },
            ],
            "ArrayOrd must sort by (batch_id, position, shard)"
        );
    }
}
