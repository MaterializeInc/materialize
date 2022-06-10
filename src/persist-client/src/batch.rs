// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A handle to a batch of updates

use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::cast::CastFrom;
use mz_persist::indexed::columnar::{ColumnarRecords, ColumnarRecordsVecBuilder};
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::{Atomicity, BlobMulti};
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::task::JoinHandle;
use tracing::{debug_span, instrument, trace_span, warn, Instrument};
use uuid::Uuid;

use crate::error::InvalidUsage;
use crate::r#impl::machine::{retry_external, FOREVER};
use crate::r#impl::metrics::RetriesMetrics;
use crate::{PersistConfig, ShardId};

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

    parts: BatchParts<T>,

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
        cfg: PersistConfig,
        retry_metrics: Arc<RetriesMetrics>,
        size_hint: usize,
        lower: Antichain<T>,
        blob: Arc<dyn BlobMulti + Send + Sync>,
        shard_id: ShardId,
    ) -> Self {
        let parts = BatchParts::new(
            cfg.batch_builder_max_outstanding_parts,
            retry_metrics,
            shard_id,
            lower.clone(),
            Arc::clone(&blob),
        );
        Self {
            size_hint,
            lower,
            max_ts: T::minimum(),
            records: ColumnarRecordsVecBuilder::new_with_len(cfg.blob_target_size),
            blob,
            parts,
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
    pub async fn finish(
        mut self,
        upper: Antichain<T>,
    ) -> Result<Batch<K, V, T, D>, InvalidUsage<T>> {
        if PartialOrder::less_than(&upper, &self.lower) {
            return Err(InvalidUsage::InvalidBounds {
                lower: self.lower.clone(),
                upper,
            });
        }
        if upper.less_equal(&self.max_ts) {
            return Err(InvalidUsage::UpdateBeyondUpper {
                max_ts: self.max_ts,
                expected_upper: upper.clone(),
            });
        }

        // NB: If self.records is empty, this call will return an empty vec, not
        // a single-element vec with an empty ColumnarRecords. This is a
        // critical performance optimization that avoids the write to s3
        // entirely if an append only has a frontier update (which is the
        // overwhelming common case in practice). The assert is a safety net in
        // case we accidentally break this behavior in ColumnarRecords.
        for part in self.records.finish() {
            assert!(part.len() > 0);
            self.parts.write(part, upper.clone()).await;
        }
        let keys = self.parts.finish().await;

        let since = Antichain::from_elem(T::minimum());
        let desc = Description::new(self.lower, upper, since);
        let batch = Batch::new(self.blob, self.shard_id.clone(), desc, keys);

        Ok(batch)
    }

    /// Adds the given update to the batch.
    ///
    /// The update timestamp must be greater or equal to `lower` that was given
    /// when creating this [BatchBuilder].
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

        // If we've filled up a chunk of ColumnarRecords, flush it out now to
        // blob storage to keep our memory usage capped.
        if let Some(parts) = self.records.take_filled() {
            for part in parts {
                // TODO: This upper would ideally be `[self.max_ts+1]` but
                // there's nothing that lets us increment a timestamp. An empty
                // antichain is guaranteed to correctly bound the data in this
                // part, but it doesn't really tell us anything. Figure out how
                // to make a tighter bound, possibly by changing the part
                // description to be an _inclusive_ upper.
                let upper = Antichain::new();
                self.parts.write(part, upper).await;
            }
        }

        Ok(())
    }
}

// TODO: If this is dropped, cancel (and delete?) any writing parts and delete
// any finished ones.
#[derive(Debug)]
struct BatchParts<T> {
    max_outstanding: usize,
    retry_metrics: Arc<RetriesMetrics>,
    shard_id: ShardId,
    lower: Antichain<T>,
    blob: Arc<dyn BlobMulti + Send + Sync>,
    writing_parts: VecDeque<(String, JoinHandle<()>)>,
    finished_parts: Vec<String>,
}

impl<T: Timestamp + Codec64> BatchParts<T> {
    fn new(
        max_outstanding: usize,
        retry_metrics: Arc<RetriesMetrics>,
        shard_id: ShardId,
        lower: Antichain<T>,
        blob: Arc<dyn BlobMulti + Send + Sync>,
    ) -> Self {
        BatchParts {
            max_outstanding,
            retry_metrics,
            shard_id,
            lower,
            blob,
            writing_parts: VecDeque::new(),
            finished_parts: Vec::new(),
        }
    }

    async fn write(&mut self, updates: ColumnarRecords, upper: Antichain<T>) {
        let since = Antichain::from_elem(T::minimum());
        let desc = Description::new(self.lower.clone(), upper, since);
        let retry_metrics = Arc::clone(&self.retry_metrics);
        let blob = Arc::clone(&self.blob);
        let key = Uuid::new_v4().to_string();
        let blob_key = key.clone();
        let index = u64::cast_from(self.finished_parts.len() + self.writing_parts.len());

        let write_span = debug_span!("batch::write_part", shard = %self.shard_id).or_current();
        let handle = mz_ore::task::spawn(
            || "batch::write_part",
            async move {
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
                    updates: vec![updates],
                    index,
                };

                let buf = mz_ore::task::spawn_blocking(
                    || "batch::encode_part",
                    move || {
                        let mut buf = Vec::new();
                        batch.encode(&mut buf);

                        // Drop batch as soon as we can to reclaim its memory.
                        drop(batch);
                        Bytes::from(buf)
                    },
                )
                .instrument(debug_span!("batch::encode_part"))
                .await
                .expect("part encode task failed");

                let () = retry_external(&retry_metrics.external.batch_set, || async {
                    blob.set(
                        Instant::now() + FOREVER,
                        &blob_key,
                        Bytes::clone(&buf),
                        Atomicity::RequireAtomic,
                    )
                    .await
                })
                .instrument(trace_span!("batch::set", payload_len = buf.len()))
                .await;
            }
            .instrument(write_span),
        );
        self.writing_parts.push_back((key, handle));

        while self.writing_parts.len() > self.max_outstanding {
            let (key, handle) = self
                .writing_parts
                .pop_front()
                .expect("pop failed when len was just > some usize");
            let () = handle
                .instrument(debug_span!("batch::max_outstanding"))
                .await
                .expect("part upload task failed");
            self.finished_parts.push(key);
        }
    }

    #[instrument(level = "debug", name = "batch::finish_upload", skip_all, fields(shard = %self.shard_id))]
    async fn finish(self) -> Vec<String> {
        let mut keys = self.finished_parts;
        for (key, handle) in self.writing_parts {
            let () = handle.await.expect("part upload task failed");
            keys.push(key);
        }
        keys
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::PersistClientCache;
    use crate::tests::all_ok;
    use crate::PersistLocation;

    use super::*;

    #[tokio::test]
    async fn batch_builder_flushing() {
        mz_ore::test::init_logging();
        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let mut cache = PersistClientCache::new_no_metrics();
        // Set blob_target_size to 0 so that each row gets forced into its own
        // batch. Set max_outstanding to a small value that's >1 to test various
        // edge cases below.
        cache.cfg.batch_builder_max_outstanding_parts = 2;
        cache.cfg.blob_target_size = 0;
        let client = cache
            .open(PersistLocation {
                blob_uri: "mem://".to_owned(),
                consensus_uri: "mem://".to_owned(),
            })
            .await
            .expect("client construction failed");
        let (mut write, read) = client
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;

        // A new builder has no writing or finished parts.
        let mut builder = write.builder(0, Antichain::from_elem(0));
        assert_eq!(builder.parts.writing_parts.len(), 0);
        assert_eq!(builder.parts.finished_parts.len(), 0);

        // We set blob_target_size to 0, so the first update gets forced out
        // into a batch.
        builder
            .add(&data[0].0 .0, &data[0].0 .1, &data[0].1, &data[0].2)
            .await
            .expect("invalid usage");
        assert_eq!(builder.parts.writing_parts.len(), 1);
        assert_eq!(builder.parts.finished_parts.len(), 0);

        // We set batch_builder_max_outstanding_parts to 2, so we are allowed to
        // pipeline a second part.
        builder
            .add(&data[1].0 .0, &data[1].0 .1, &data[1].1, &data[1].2)
            .await
            .expect("invalid usage");
        assert_eq!(builder.parts.writing_parts.len(), 2);
        assert_eq!(builder.parts.finished_parts.len(), 0);

        // But now that we have 3 parts, the add call back-pressures until the
        // first one finishes.
        builder
            .add(&data[2].0 .0, &data[2].0 .1, &data[2].1, &data[2].2)
            .await
            .expect("invalid usage");
        assert_eq!(builder.parts.writing_parts.len(), 2);
        assert_eq!(builder.parts.finished_parts.len(), 1);

        // Finish off the batch and verify that the keys and such get plumbed
        // correctly by reading the data back.
        let batch = builder
            .finish(Antichain::from_elem(4))
            .await
            .expect("invalid usage");
        write
            .append_batch(batch, Antichain::from_elem(0), Antichain::from_elem(4))
            .await
            .expect("invalid usage")
            .expect("unexpected upper");
        assert_eq!(
            read.expect_snapshot(3).await.read_all().await,
            all_ok(&data, 3)
        );
    }
}
