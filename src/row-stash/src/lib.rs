// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Abstraction over Persist that temporarily stages Rows for Peek Responses.

use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{BinaryArray, BinaryBuilder};
use bytes::{BufMut, Bytes};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use futures::stream::Stream;
use futures::StreamExt;
use mz_ore::soft_assert_eq_or_log;
use mz_persist_client::batch::ProtoBatch;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::codec_impls::{
    SimpleColumnarData, SimpleColumnarDecoder, SimpleColumnarEncoder, UnitSchema,
};
use mz_persist_types::columnar::Schema;
use mz_persist_types::stats::NoneStats;
use mz_persist_types::{Codec, Codec64, ShardId};
use mz_repr::{Row, RowRef};
use sha2::Digest;
use timely::progress::Timestamp;
use uuid::Uuid;

/// Handle to the Persist shard that we can use to stash [`Row`]s for later
/// reading.
///
/// **No data should ever be appended to this shard.**
///
/// The goal of this type is support arbitrary sized peek responses. When a
/// user runs a `SELECT` query, the result may be too large to directly
/// transmit to the control plane (`environmentd`). Instead our data plane
/// (`clusterd`) will sink the results into Persist (i.e. S3) and then
/// `environmentd` can stream straight back to the client.
///
/// Internally the the data is staged in a [`mz_persist_client::batch::Batch`].
/// These batches should **never** get appended to the shard.
pub struct StashedRowHandle<T, D> {
    inner: ReadHandle<StashedRow, (), T, D>,
}

impl<T, D> StashedRowHandle<T, D>
where
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Semigroup + Codec64 + Send + Sync,
{
    /// Open a new [`StashedRowHandle`].
    pub async fn new(client: &PersistClient, organization_id: Uuid) -> Self {
        let shard_id = Self::shard_id(organization_id);
        let diagnostics = Diagnostics {
            shard_name: "staged_rows".to_string(),
            handle_purpose: "Staging Rows for Peek Responses".to_string(),
        };
        let handle = client
            .open_leased_reader::<StashedRow, (), T, D>(
                shard_id,
                Arc::new(StashedRowSchema),
                Arc::new(UnitSchema),
                diagnostics,
                false,
            )
            .await
            .expect("valid usage");

        StashedRowHandle { inner: handle }
    }

    /// Consume a [`StashedRows`] returning a [`Stream`] of [`Row`]s.
    ///
    /// After the stream is completed the [`ProtoBatch`] will be deleted.
    pub async fn consume(
        &mut self,
        batch: ProtoBatch,
    ) -> impl Stream<Item = Result<(Row, D), anyhow::Error>> + Send + 'static {
        self.inner
            .consume(batch)
            .await
            .map(|((stashed_row, val), _ts, diff)| {
                mz_ore::soft_assert_or_log!(val.is_ok(), "Value was an error?");

                stashed_row
                    .map(|stashed| (stashed.inner, diff))
                    .map_err(|err| anyhow::anyhow!("stashed row: {err}"))
            })
    }

    /// Return a deterministic [`ShardId`] for staging rows.
    pub fn shard_id(organization_id: Uuid) -> ShardId {
        const SHARD_NAME: &str = "staged_rows";
        /// Bump this if we need to change the ShardId across a release.
        const SEED: usize = 1;

        let hash = sha2::Sha256::digest(format!("{organization_id}{SHARD_NAME}{SEED}")).to_vec();
        soft_assert_eq_or_log!(hash.len(), 32, "SHA256 returns 32 bytes (256 bits)");
        let uuid = Uuid::from_slice(&hash[0..16]).expect("from_slice accepts exactly 16 bytes");
        ShardId::from_str(&format!("s{uuid}")).expect("known to be valid")
    }
}

/// [`Row`]s that have been stashed in Persist for later reading.
#[derive(Debug)]
pub struct StashedRows {
    batch: ProtoBatch,
}

impl From<ProtoBatch> for StashedRows {
    fn from(value: ProtoBatch) -> Self {
        StashedRows { batch: value }
    }
}

/// Newtype wrapper around [`Row`] that can be stored in Persist.
///
/// **Not a durable representation**.
///
/// When serializaing a [`StagedRow`] we use our internal byte representation
/// that can change across releases.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StashedRow {
    inner: Row,
}

impl From<Row> for StashedRow {
    fn from(value: Row) -> Self {
        StashedRow { inner: value }
    }
}

impl Codec for StashedRow {
    type Schema = StashedRowSchema;
    type Storage = Self;

    fn codec_name() -> String {
        "StagedRow".to_string()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        buf.put(self.inner.data());
    }

    fn decode<'a>(buf: &'a [u8], _schema: &Self::Schema) -> Result<Self, String> {
        let inner = RowRef::from_slice(buf).to_owned();
        Ok(StashedRow { inner })
    }

    fn encode_schema(_schema: &Self::Schema) -> Bytes {
        Bytes::new()
    }

    fn decode_schema(buf: &bytes::Bytes) -> Self::Schema {
        assert_eq!(*buf, Bytes::new());
        StashedRowSchema
    }
}

/// [`Schema`] for [`StagedRow`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct StashedRowSchema;

impl Schema<StashedRow> for StashedRowSchema {
    type ArrowColumn = BinaryArray;
    type Statistics = NoneStats;

    type Decoder = SimpleColumnarDecoder<StashedRow>;
    type Encoder = SimpleColumnarEncoder<StashedRow>;

    fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
        Ok(SimpleColumnarEncoder::default())
    }

    fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
        Ok(SimpleColumnarDecoder::new(col))
    }
}

impl SimpleColumnarData for StashedRow {
    type ArrowBuilder = BinaryBuilder;
    type ArrowColumn = BinaryArray;

    fn goodbytes(builder: &Self::ArrowBuilder) -> usize {
        builder.values_slice().len()
    }

    fn push(&self, builder: &mut Self::ArrowBuilder) {
        builder.append_value(self.inner.data());
    }

    fn push_null(builder: &mut Self::ArrowBuilder) {
        builder.append_null();
    }

    fn read(&mut self, idx: usize, column: &Self::ArrowColumn) {
        let slice = RowRef::from_slice(column.value(idx));
        // TODO(parkmycar): Copy the slice directly into our Row.
        self.inner = slice.to_owned();
    }
}
