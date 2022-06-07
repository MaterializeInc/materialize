// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A handle to a batch of updates

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_persist::indexed::columnar::ColumnarRecordsVecBuilder;
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::{Atomicity, BlobMulti};
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use tracing::{debug_span, instrument, warn, Instrument};
use uuid::Uuid;

use crate::error::InvalidUsage;
use crate::r#impl::machine::{retry_external, FOREVER};
use crate::ShardId;

/// A handle to a batch of updates that has been written to blob storage but
/// which has not yet been appended to a shard.
///
/// A [Batch] needs to be marked as consumed or it needs to be deleted via [Self::delete].
/// Otherwise, a dangling batch will leak and backing blobs will remain in blob storage.
#[derive(Debug)]
pub struct Batch<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    /// [Description] of updates contained in this batch.
    desc: Description<T>,

    shard_id: ShardId,

    /// Keys to blobs that make up this batch of updates.
    pub(crate) blob_keys: Vec<String>,

    /// Handle to the [BlobMulti] that the blobs of this batch were uploaded to.
    _blob: Arc<dyn BlobMulti + Send + Sync>,

    // These provide a bit more safety against appending a batch with the wrong
    // type to a shard.
    _phantom: PhantomData<(K, V, T, D)>,
}

impl<K, V, T, D> Drop for Batch<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    fn drop(&mut self) {
        if self.blob_keys.len() > 0 {
            warn!(
                "un-consumed Batch, with {} dangling blob keys: {:?}",
                self.blob_keys.len(),
                self.blob_keys
            );
        }
    }
}

impl<K, V, T, D> Batch<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    pub(crate) fn new(
        blob: Arc<dyn BlobMulti + Send + Sync>,
        shard_id: ShardId,
        desc: Description<T>,
        blob_keys: Vec<String>,
    ) -> Self {
        Self {
            desc,
            blob_keys,
            shard_id,
            _blob: blob,
            _phantom: PhantomData,
        }
    }

    /// The `shard_id` of this [Batch].
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// The `upper` of this [Batch].
    pub fn upper(&self) -> &Antichain<T> {
        self.desc.upper()
    }

    /// The `lower` of this [Batch].
    pub fn lower(&self) -> &Antichain<T> {
        self.desc.lower()
    }

    /// Marks the blobs that this batch handle points to as consumed, likely
    /// because they were appended to a shard.
    ///
    /// Consumers of a blob need to make this explicit, so that we can log
    /// warnings in case a batch is not used.
    pub(crate) fn mark_consumed(&mut self) {
        self.blob_keys.clear();
    }

    /// Deletes the blobs that make up this batch from the given blob store and
    /// marks them as deleted.
    #[instrument(level = "debug", skip_all, fields(shard = %self.shard_id))]
    pub async fn delete(mut self) {
        // TODO: This is temporarily disabled because nemesis seems to have
        // caught that we sometimes delete batches that are later needed.
        // Temporarily removing the deletions while we figure out the bug in
        // case it has anything to do with CI timeouts.
        //
        // let deadline = Instant::now() + FOREVER;
        // for key in self.blob_keys.iter() {
        //     retry_external("batch::delete", || async {
        //         self.blob.delete(deadline, key).await
        //     })
        //     .await;
        // }
        self.blob_keys.clear();
    }
}

/// A builder for [Batches](Batch) that allows adding updates piece by piece and
/// then finishing it.
#[derive(Debug)]
pub struct BatchBuilder<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    size_hint: usize,
    lower: Antichain<T>,
    max_ts: T,

    shard_id: ShardId,
    records: ColumnarRecordsVecBuilder,
    blob: Arc<dyn BlobMulti + Send + Sync>,

    key_buf: Vec<u8>,
    val_buf: Vec<u8>,

    // These provide a bit more safety against appending a batch with the wrong
    // type to a shard.
    _phantom: PhantomData<(K, V, T, D)>,
}

impl<K, V, T, D> BatchBuilder<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    pub(crate) fn new(
        size_hint: usize,
        lower: Antichain<T>,
        blob: Arc<dyn BlobMulti + Send + Sync>,
        shard_id: ShardId,
    ) -> Self {
        Self {
            size_hint,
            lower,
            max_ts: T::minimum(),
            records: ColumnarRecordsVecBuilder::default(),
            blob,
            shard_id,
            key_buf: Vec::new(),
            val_buf: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Finish writing this batch and return a handle to the written batch.
    ///
    /// This fails if any of the updates in this batch are beyond the given
    /// `upper`.
    #[instrument(level = "debug", name = "batch::finish", skip_all, fields(shard = %self.shard_id))]
    pub async fn finish(self, upper: Antichain<T>) -> Result<Batch<K, V, T, D>, InvalidUsage<T>> {
        if upper.less_equal(&self.max_ts) {
            return Err(InvalidUsage::UpdateBeyondUpper {
                max_ts: self.max_ts,
                expected_upper: upper.clone(),
            });
        }

        let since = Antichain::from_elem(T::minimum());
        let desc = Description::new(self.lower, upper, since);

        let updates = self.records.finish();

        if updates.len() == 0 {
            return Ok(Batch::new(
                Arc::clone(&self.blob),
                self.shard_id,
                desc,
                vec![],
            ));
        }

        // TODO: Get rid of the from_le_bytes.
        let encoded_desc = Description::new(
            Antichain::from(
                desc.lower()
                    .elements()
                    .iter()
                    .map(|x| u64::from_le_bytes(T::encode(x)))
                    .collect::<Vec<_>>(),
            ),
            Antichain::from(
                desc.upper()
                    .elements()
                    .iter()
                    .map(|x| u64::from_le_bytes(T::encode(x)))
                    .collect::<Vec<_>>(),
            ),
            Antichain::from(
                desc.since()
                    .elements()
                    .iter()
                    .map(|x| u64::from_le_bytes(T::encode(x)))
                    .collect::<Vec<_>>(),
            ),
        );

        let batch = BlobTraceBatchPart {
            desc: encoded_desc.clone(),
            updates,
            index: 0,
        };

        let mut buf = Vec::new();
        debug_span!("make_batch::encode").in_scope(|| {
            batch.encode(&mut buf);
        });
        let buf = Bytes::from(buf);

        let key = Uuid::new_v4().to_string();
        let () = retry_external("make_batch::set", || async {
            self.blob
                .set(
                    Instant::now() + FOREVER,
                    &key,
                    Bytes::clone(&buf),
                    Atomicity::RequireAtomic,
                )
                .await
        })
        .instrument(debug_span!("make_batch::set", payload_len = buf.len()))
        .await;

        let batch = Batch::new(
            Arc::clone(&self.blob),
            self.shard_id.clone(),
            desc,
            vec![key],
        );

        Ok(batch)
    }

    /// Adds the given update to the batch.
    ///
    /// The update timestamp must be greater or equal to `lower` that was given
    /// when creating this [BatchBuilder].
    //
    // NOTE: This function is async right now, even though it doesn't need it.
    // We're keeping the option open in case we might need it in the future.
    pub async fn add(&mut self, key: &K, val: &V, ts: &T, diff: &D) -> Result<(), InvalidUsage<T>> {
        if !self.lower.less_equal(ts) {
            return Err(InvalidUsage::UpdateNotBeyondLower {
                ts: ts.clone(),
                lower: self.lower.clone(),
            });
        }

        self.max_ts.join_assign(ts);

        self.key_buf.clear();
        self.val_buf.clear();
        K::encode(key, &mut self.key_buf);
        V::encode(val, &mut self.val_buf);
        let t = T::encode(ts);
        let d = D::encode(diff);

        if self.records.len() == 0 {
            // Use the first record to attempt to pre-size the builder
            // allocations.
            let additional = usize::saturating_add(self.size_hint, 1);
            self.records
                .reserve(additional, self.key_buf.len(), self.val_buf.len());
        }
        self.records.push(((&self.key_buf, &self.val_buf), t, d));

        // TODO(aljoscha): As of now, this batch builder doesn't have constant
        // memory usage but scales with the number of added records. Instead of
        // keeping one records builder around, we should periodically finish and
        // flush the updates to a blob, store the blob key in the builder, and
        // reset the records builder.

        Ok(())
    }
}
