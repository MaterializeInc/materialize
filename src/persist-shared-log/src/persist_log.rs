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
//! - K: `Proposal` (serialized protobuf bytes)
//! - V: `()`
//! - T: `u64` (incremented by 1 per batch, in lock-step with persist upper)
//! - D: `i64` (always +1, proposals are append-only)

pub mod acceptor;
pub mod client;
pub mod latency_blob;
pub mod learner;

use arrow::array::{BinaryArray, BinaryBuilder};
use bytes::{BufMut, Bytes};

use mz_persist_types::Codec;
use mz_persist_types::codec_impls::{
    SimpleColumnarData, SimpleColumnarDecoder, SimpleColumnarEncoder,
};
use mz_persist_types::columnar::Schema;
use mz_persist_types::stats::NoneStats;

/// A consensus proposal stored as the key in a persist shard.
///
/// Wraps serialized `ProtoLogProposal` protobuf bytes. Each proposal is stored
/// as a single row in the persist shard at timestamp T (the batch number).
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
