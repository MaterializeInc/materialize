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
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::cast::CastFrom;
use mz_persist::indexed::columnar::{ColumnarRecords, ColumnarRecordsBuilder};
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::{Atomicity, Blob};
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::task::JoinHandle;
use tracing::{debug_span, info, instrument, trace_span, warn, Instrument};

use crate::async_runtime::CpuHeavyRuntime;
use crate::error::InvalidUsage;
use crate::internal::machine::retry_external;
use crate::internal::metrics::{BatchWriteMetrics, Metrics};
use crate::internal::paths::{PartId, PartialBatchKey};
use crate::internal::state::{HollowBatch, HollowBatchPart};
use crate::write::WriterEnrichedHollowBatch;
use crate::{PersistConfig, ShardId, WriterId};

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
    pub(crate) shard_id: ShardId,

    /// A handle to the data represented by this batch.
    pub(crate) batch: HollowBatch<T>,

    /// Handle to the [Blob] that the blobs of this batch were uploaded to.
    pub(crate) _blob: Arc<dyn Blob + Send + Sync>,

    // These provide a bit more safety against appending a batch with the wrong
    // type to a shard.
    pub(crate) _phantom: PhantomData<(K, V, T, D)>,
}

impl<K, V, T, D> Drop for Batch<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    fn drop(&mut self) {
        if self.batch.parts.len() > 0 {
            warn!(
                "un-consumed Batch, with {} dangling blob keys: {:?}",
                self.batch.parts.len(),
                self.batch
                    .parts
                    .iter()
                    .map(|x| &x.key.0)
                    .collect::<Vec<_>>(),
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
        blob: Arc<dyn Blob + Send + Sync>,
        shard_id: ShardId,
        batch: HollowBatch<T>,
    ) -> Self {
        Self {
            shard_id,
            batch,
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
        self.batch.desc.upper()
    }

    /// The `lower` of this [Batch].
    pub fn lower(&self) -> &Antichain<T> {
        self.batch.desc.lower()
    }

    /// Marks the blobs that this batch handle points to as consumed, likely
    /// because they were appended to a shard.
    ///
    /// Consumers of a blob need to make this explicit, so that we can log
    /// warnings in case a batch is not used.
    pub(crate) fn mark_consumed(&mut self) {
        self.batch.parts.clear();
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
        // for key in self.blob_keys.iter() {
        //     retry_external("batch::delete", || async {
        //         self.blob.delete(key).await
        //     })
        //     .await;
        // }
        self.batch.parts.clear();
    }

    pub fn into_hollow_batch(mut self) -> HollowBatch<T> {
        let ret = self.batch.clone();
        self.mark_consumed();
        ret
    }

    /// Turns this [`Batch`] into a [`WriterEnrichedHollowBatch`], which can be
    /// used to transfer this batch across process boundaries, for example when
    /// exchanging data between timely workers.
    ///
    /// **NOTE**: If this batch is not eventually appended to a shard or
    /// dropped, the data that it represents will have leaked. The caller is
    /// responsible for turning this back into a [`Batch`] using
    /// [`WriteHandle::batch_from_hollow_batch`](crate::write::WriteHandle::batch_from_hollow_batch).
    pub fn into_writer_hollow_batch(mut self) -> WriterEnrichedHollowBatch<T> {
        let ret = WriterEnrichedHollowBatch {
            shard_id: self.shard_id,
            batch: self.batch.clone(),
        };
        self.mark_consumed();
        ret
    }
}

/// Indicates what work was done in a call to [BatchBuilder::add]
#[derive(Debug)]
pub enum Added {
    /// A record was inserted into a pending batch part
    Record,
    /// A record was inserted into a pending batch part
    /// and the part was sent to blob storage
    RecordAndParts,
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
    blob_target_size: usize,

    shard_id: ShardId,
    blob: Arc<dyn Blob + Send + Sync>,
    metrics: Arc<Metrics>,

    current_part: Vec<((Vec<u8>, Vec<u8>), T, D)>,
    current_part_bytes: usize,
    greatest_kv_seen: Option<(Vec<u8>, Vec<u8>)>,
    runs: Vec<usize>,
    parts_written: usize,

    num_updates: usize,
    parts: BatchParts<T>,

    key_buf: Vec<u8>,
    val_buf: Vec<u8>,

    since: Antichain<T>,

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
        metrics: Arc<Metrics>,
        size_hint: usize,
        lower: Antichain<T>,
        blob: Arc<dyn Blob + Send + Sync>,
        cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
        shard_id: ShardId,
        writer_id: WriterId,
        since: Antichain<T>,
    ) -> Self {
        let parts = BatchParts::new(
            cfg.batch_builder_max_outstanding_parts,
            Arc::clone(&metrics),
            shard_id,
            writer_id,
            lower.clone(),
            Arc::clone(&blob),
            cpu_heavy_runtime,
            &metrics.user,
        );
        Self {
            size_hint,
            lower,
            max_ts: T::minimum(),
            blob_target_size: cfg.blob_target_size,
            blob,
            metrics,
            current_part: Vec::new(),
            current_part_bytes: 0,
            greatest_kv_seen: None,
            parts_written: 0,
            runs: Vec::new(),
            num_updates: 0,
            parts,
            shard_id,
            key_buf: Vec::new(),
            val_buf: Vec::new(),
            since,
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
        // if upper.less_equal(&self.max_ts) {
        //     return Err(InvalidUsage::UpdateBeyondUpper {
        //         max_ts: self.max_ts,
        //         expected_upper: upper.clone(),
        //     });
        // }

        info!(
            "{}: finishing batch. {} updates in current part, {} updates total",
            self.shard_id,
            self.current_part.len(),
            self.num_updates
        );

        Self::consolidate_run(
            &mut self.current_part,
            &mut self.runs,
            self.parts_written,
            &mut self.greatest_kv_seen,
            &mut self.num_updates,
        );
        Self::write_run(
            &mut self.parts,
            &mut self.current_part,
            &mut self.parts_written,
            Description::new(self.lower.clone(), upper.clone(), self.since.clone()),
        )
        .await;

        let parts = self.parts.finish().await;

        let desc = Description::new(self.lower, upper, self.since);
        let batch = Batch::new(
            self.blob,
            self.shard_id.clone(),
            HollowBatch {
                desc,
                parts,
                len: self.num_updates,
                runs: self.runs,
            },
        );
        info!("{}: Created batch {:?}", self.shard_id, batch.batch);

        Ok(batch)
    }

    /// TODO
    pub async fn add_encoded(&mut self, ts: &T, diff: &D) -> Result<Added, InvalidUsage<T>> {
        if !self.lower.less_equal(ts) {
            return Err(InvalidUsage::UpdateNotBeyondLower {
                ts: ts.clone(),
                lower: self.lower.clone(),
            });
        }

        self.max_ts.join_assign(ts);

        let size = ColumnarRecordsBuilder::columnar_record_size(&self.key_buf, &self.val_buf);
        let (ts, diff) = (ts.clone(), diff.clone());

        // WIP: If we've filled up a chunk of ColumnarRecords, flush it out now to blob storage to keep our memory usage capped.
        let mut part_written = false;
        if size + self.current_part_bytes >= self.blob_target_size {
            info!("flushing part");
            Self::consolidate_run(
                &mut self.current_part,
                &mut self.runs,
                self.parts_written,
                &mut self.greatest_kv_seen,
                &mut self.num_updates,
            );
            // TODO: This upper would ideally be `[self.max_ts+1]` but
            // there's nothing that lets us increment a timestamp. An empty
            // antichain is guaranteed to correctly bound the data in this
            // part, but it doesn't really tell us anything. Figure out how
            // to make a tighter bound, possibly by changing the part
            // description to be an _inclusive_ upper.
            let upper = Antichain::new();
            Self::write_run(
                &mut self.parts,
                &mut self.current_part,
                &mut self.parts_written,
                Description::new(self.lower.clone(), upper.clone(), self.since.clone()),
            )
            .await;

            part_written = true;
            self.current_part_bytes = 0;
        }

        // WIP remove the clone here if possible
        self.current_part
            .push(((self.key_buf.clone(), self.val_buf.clone()), ts, diff));
        self.current_part_bytes += size;

        if part_written {
            Ok(Added::RecordAndParts)
        } else {
            Ok(Added::Record)
        }
    }

    /// Adds the given update to the batch.
    ///
    /// The update timestamp must be greater or equal to `lower` that was given
    /// when creating this [BatchBuilder].
    pub async fn add(
        &mut self,
        key: &K,
        val: &V,
        ts: &T,
        diff: &D,
    ) -> Result<Added, InvalidUsage<T>> {
        self.key_buf.clear();
        self.val_buf.clear();
        self.metrics
            .codecs
            .key
            .encode(|| K::encode(key, &mut self.key_buf));
        self.metrics
            .codecs
            .val
            .encode(|| V::encode(val, &mut self.val_buf));
        self.add_encoded(ts, diff).await
    }

    /// Consolidates `updates`, and determines whether the updates should extend the
    /// current run (if any).  A new run will be created if `updates` contains a key
    /// that overlaps with the current or any previous run.
    fn consolidate_run(
        updates: &mut Vec<((Vec<u8>, Vec<u8>), T, D)>,
        runs: &mut Vec<usize>,
        number_of_compacted_runs: usize,
        greatest_kv: &mut Option<(Vec<u8>, Vec<u8>)>,
        num_updates: &mut usize,
        // timings: &mut Timings,
    ) -> usize {
        // let start = Instant::now();
        consolidate_updates(updates);

        *num_updates += updates.len();

        match (&greatest_kv, updates.last()) {
            // our updates contain a key that exists within the range of a run we've
            // already created, we should start a new run, as this part is no longer
            // contiguous with the previous run/part
            (Some(greatest_kv_seen), Some(greatest_kv_in_batch))
                if *greatest_kv_seen > greatest_kv_in_batch.0 =>
            {
                runs.push(number_of_compacted_runs);
            }
            (_, Some(greatest_kv_in_batch)) => *greatest_kv = Some(greatest_kv_in_batch.0.clone()),
            (Some(_), None) | (None, None) => {}
        };

        // timings.consolidation += start.elapsed();
        updates.len()
    }

    /// Encodes `updates` into columnar format and writes them as a single part to blob. It is the
    /// caller's responsibility to chunk `updates` into a batch no greater than [crate::PersistConfig::blob_target_size]
    /// and must absolutely be less than [mz_persist::indexed::columnar::KEY_VAL_DATA_MAX_LEN]
    async fn write_run(
        batch_parts: &mut BatchParts<T>,
        updates: &mut Vec<((Vec<u8>, Vec<u8>), T, D)>,
        compaction_parts_count: &mut usize,
        desc: Description<T>,
        // timings: &mut Timings,
    ) {
        if updates.is_empty() {
            info!("skipping update, is empty");
            return;
        }
        *compaction_parts_count += 1;

        let mut builder = ColumnarRecordsBuilder::default();
        // let start = Instant::now();
        for ((k, v), t, d) in updates.drain(..) {
            // WIP: handle return
            builder.push(((&k, &v), T::encode(&t), D::encode(&d)));
        }
        // timings.part_columnar_encoding += start.elapsed();

        let columnar = builder.finish();

        // let start = Instant::now();
        batch_parts
            .write(columnar, desc.upper().clone(), desc.since().clone())
            .await;
        // timings.part_writing += start.elapsed();
    }
}

// TODO: If this is dropped, cancel (and delete?) any writing parts and delete
// any finished ones.
#[derive(Debug)]
pub(crate) struct BatchParts<T> {
    max_outstanding: usize,
    metrics: Arc<Metrics>,
    shard_id: ShardId,
    writer_id: WriterId,
    lower: Antichain<T>,
    blob: Arc<dyn Blob + Send + Sync>,
    cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
    writing_parts: VecDeque<(PartialBatchKey, JoinHandle<usize>)>,
    finished_parts: Vec<HollowBatchPart>,
    batch_metrics: BatchWriteMetrics,
}

impl<T: Timestamp + Codec64> BatchParts<T> {
    pub(crate) fn new(
        max_outstanding: usize,
        metrics: Arc<Metrics>,
        shard_id: ShardId,
        writer_id: WriterId,
        lower: Antichain<T>,
        blob: Arc<dyn Blob + Send + Sync>,
        cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
        batch_metrics: &BatchWriteMetrics,
    ) -> Self {
        BatchParts {
            max_outstanding,
            metrics,
            shard_id,
            writer_id,
            lower,
            blob,
            cpu_heavy_runtime,
            writing_parts: VecDeque::new(),
            finished_parts: Vec::new(),
            batch_metrics: batch_metrics.clone(),
        }
    }

    pub(crate) async fn write(
        &mut self,
        updates: ColumnarRecords,
        upper: Antichain<T>,
        since: Antichain<T>,
    ) {
        let desc = Description::new(self.lower.clone(), upper, since);
        let metrics = Arc::clone(&self.metrics);
        let blob = Arc::clone(&self.blob);
        let cpu_heavy_runtime = Arc::clone(&self.cpu_heavy_runtime);
        let batch_metrics = self.batch_metrics.clone();
        let partial_key = PartialBatchKey::new(&self.writer_id, &PartId::new());
        let key = partial_key.complete(&self.shard_id);
        let index = u64::cast_from(self.finished_parts.len() + self.writing_parts.len());

        let write_span = debug_span!("batch::write_part", shard = %self.shard_id).or_current();
        let handle = mz_ore::task::spawn(
            || "batch::write_part",
            async move {
                let goodbytes = updates.goodbytes();
                let batch = BlobTraceBatchPart {
                    desc,
                    updates: vec![updates],
                    index,
                };

                let start = Instant::now();
                let buf = cpu_heavy_runtime
                    .spawn_named(|| "batch::encode_part", async move {
                        let mut buf = Vec::new();
                        batch.encode(&mut buf);

                        // Drop batch as soon as we can to reclaim its memory.
                        drop(batch);
                        Bytes::from(buf)
                    })
                    .instrument(debug_span!("batch::encode_part"))
                    .await
                    .expect("part encode task failed");
                // Can't use the `CodecMetrics::encode` helper because of async.
                metrics.codecs.batch.encode_count.inc();
                metrics
                    .codecs
                    .batch
                    .encode_seconds
                    .inc_by(start.elapsed().as_secs_f64());

                let start = Instant::now();
                let payload_len = buf.len();
                let () = retry_external(&metrics.retries.external.batch_set, || async {
                    blob.set(&key, Bytes::clone(&buf), Atomicity::RequireAtomic)
                        .await
                })
                .instrument(trace_span!("batch::set", payload_len))
                .await;
                batch_metrics.seconds.inc_by(start.elapsed().as_secs_f64());
                batch_metrics.bytes.inc_by(u64::cast_from(payload_len));
                batch_metrics.goodbytes.inc_by(u64::cast_from(goodbytes));
                payload_len
            }
            .instrument(write_span),
        );
        self.writing_parts.push_back((partial_key, handle));

        while self.writing_parts.len() > self.max_outstanding {
            batch_metrics.write_stalls.inc();
            let (key, handle) = self
                .writing_parts
                .pop_front()
                .expect("pop failed when len was just > some usize");
            let encoded_size_bytes = match handle
                .instrument(debug_span!("batch::max_outstanding"))
                .await
            {
                Ok(x) => x,
                Err(err) if err.is_cancelled() => 0,
                Err(err) => panic!("part upload task failed: {}", err),
            };
            self.finished_parts.push(HollowBatchPart {
                key,
                encoded_size_bytes,
            });
        }
    }

    #[instrument(level = "debug", name = "batch::finish_upload", skip_all, fields(shard = %self.shard_id))]
    pub(crate) async fn finish(self) -> Vec<HollowBatchPart> {
        let mut parts = self.finished_parts;
        for (key, handle) in self.writing_parts {
            let encoded_size_bytes = match handle.await {
                Ok(x) => x,
                Err(err) if err.is_cancelled() => 0,
                Err(err) => panic!("part upload task failed: {}", err),
            };
            parts.push(HollowBatchPart {
                key,
                encoded_size_bytes,
            });
        }
        parts
    }
}

pub(crate) fn validate_truncate_batch<T: Timestamp>(
    batch: &Description<T>,
    truncate: &Description<T>,
) -> Result<(), InvalidUsage<T>> {
    if !PartialOrder::less_equal(batch.lower(), truncate.lower())
        || PartialOrder::less_than(batch.upper(), truncate.upper())
    {
        return Err(InvalidUsage::InvalidBatchBounds {
            batch_lower: batch.lower().clone(),
            batch_upper: batch.upper().clone(),
            append_lower: truncate.lower().clone(),
            append_upper: truncate.upper().clone(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::cache::PersistClientCache;
    use crate::internal::paths::{BlobKey, PartialBlobKey};
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
        let (mut write, mut read) = client
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
        assert_eq!(batch.batch.parts.len(), 3);
        write
            .append_batch(batch, Antichain::from_elem(0), Antichain::from_elem(4))
            .await
            .expect("invalid usage")
            .expect("unexpected upper");
        assert_eq!(read.expect_snapshot_and_fetch(3).await, all_ok(&data, 3));
    }

    #[tokio::test]
    async fn batch_builder_keys() {
        mz_ore::test::init_logging();

        let mut cache = PersistClientCache::new_no_metrics();
        // Set blob_target_size to 0 so that each row gets forced into its own batch part
        cache.cfg.blob_target_size = 0;
        let client = cache
            .open(PersistLocation {
                blob_uri: "mem://".to_owned(),
                consensus_uri: "mem://".to_owned(),
            })
            .await
            .expect("client construction failed");
        let shard_id = ShardId::new();
        let (mut write, _) = client
            .expect_open::<String, String, u64, i64>(shard_id)
            .await;

        let batch = write
            .expect_batch(
                &[
                    (("1".into(), "one".into()), 1, 1),
                    (("2".into(), "two".into()), 2, 1),
                    (("3".into(), "three".into()), 3, 1),
                ],
                0,
                4,
            )
            .await;

        assert_eq!(batch.batch.parts.len(), 3);
        for part in &batch.batch.parts {
            match BlobKey::parse_ids(&part.key.complete(&shard_id)) {
                Ok((shard, PartialBlobKey::Batch(writer, _))) => {
                    assert_eq!(shard.to_string(), shard_id.to_string());
                    assert_eq!(writer.to_string(), write.writer_id.to_string());
                }
                _ => panic!("unparseable blob key"),
            }
        }
    }
}
