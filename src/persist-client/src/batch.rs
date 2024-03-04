// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A handle to a batch of updates

use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use futures_util::stream::{FuturesUnordered, StreamExt};
use mz_dyncfg::Config;
use mz_ore::cast::CastFrom;
use mz_ore::instrument;
use mz_ore::task::{JoinHandle, JoinHandleExt};
use mz_persist::indexed::columnar::{ColumnarRecords, ColumnarRecordsBuilder};
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::{Atomicity, Blob};
use mz_persist_types::stats::{trim_to_budget, truncate_bytes, TruncateBound, TRUNCATE_LEN};
use mz_persist_types::{Codec, Codec64};
use mz_proto::RustType;
use mz_timely_util::order::Reverse;
use proptest_derive::Arbitrary;
use semver::Version;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::{debug_span, error, trace_span, warn, Instrument};

use crate::async_runtime::IsolatedRuntime;
use crate::cfg::MiB;
use crate::error::InvalidUsage;
use crate::internal::encoding::{LazyPartStats, Schemas};
use crate::internal::machine::retry_external;
use crate::internal::metrics::{BatchWriteMetrics, Metrics, ShardMetrics};
use crate::internal::paths::{PartId, PartialBatchKey, WriterKey};
use crate::internal::state::{HollowBatch, HollowBatchPart};
use crate::stats::{untrimmable_columns, PartStats, STATS_BUDGET_BYTES, STATS_COLLECTION_ENABLED};
use crate::write::WriterId;
use crate::{PersistConfig, ShardId};

include!(concat!(env!("OUT_DIR"), "/mz_persist_client.batch.rs"));

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
    pub(crate) batch_delete_enabled: bool,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) shard_id: ShardId,

    /// The version of Materialize which wrote this batch.
    pub(crate) version: Version,

    /// A handle to the data represented by this batch.
    pub(crate) batch: HollowBatch<T>,

    /// Handle to the [Blob] that the blobs of this batch were uploaded to.
    pub(crate) blob: Arc<dyn Blob + Send + Sync>,

    // These provide a bit more safety against appending a batch with the wrong
    // type to a shard.
    pub(crate) _phantom: PhantomData<fn() -> (K, V, T, D)>,
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
        batch_delete_enabled: bool,
        metrics: Arc<Metrics>,
        blob: Arc<dyn Blob + Send + Sync>,
        shard_id: ShardId,
        version: Version,
        batch: HollowBatch<T>,
    ) -> Self {
        Self {
            batch_delete_enabled,
            metrics,
            shard_id,
            version,
            batch,
            blob,
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
    #[instrument(level = "debug", fields(shard = %self.shard_id))]
    pub async fn delete(mut self) {
        self.mark_consumed();
        if !self.batch_delete_enabled {
            return;
        }
        let deletes = FuturesUnordered::new();
        for part in self.batch.parts.iter() {
            let metrics = Arc::clone(&self.metrics);
            let blob = Arc::clone(&self.blob);
            deletes.push(async move {
                retry_external(&metrics.retries.external.batch_delete, || async {
                    blob.delete(&part.key).await
                })
                .await;
            });
        }
        let () = deletes.collect().await;
    }

    /// Turns this [`Batch`] into a `HollowBatch`.
    ///
    /// **NOTE**: If this batch is not eventually appended to a shard or
    /// dropped, the data that it represents will have leaked.
    pub fn into_hollow_batch(mut self) -> HollowBatch<T> {
        let ret = self.batch.clone();
        self.mark_consumed();
        ret
    }

    /// Turns this [`Batch`] into a [`ProtoBatch`], which can be used to
    /// transfer this batch across process boundaries, for example when
    /// exchanging data between timely workers.
    ///
    /// **NOTE**: If this batch is not eventually appended to a shard or
    /// dropped, the data that it represents will have leaked. The caller is
    /// responsible for turning this back into a [`Batch`] using
    /// [`WriteHandle::batch_from_transmittable_batch`](crate::write::WriteHandle::batch_from_transmittable_batch).
    pub fn into_transmittable_batch(mut self) -> ProtoBatch {
        let ret = ProtoBatch {
            shard_id: self.shard_id.into_proto(),
            version: self.version.to_string(),
            batch: Some(self.batch.into_proto()),
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

/// A snapshot of dynamic configs to make it easier to reason about an individual
/// run of BatchBuilder.
#[derive(Debug, Clone)]
pub struct BatchBuilderConfig {
    writer_key: WriterKey,
    pub(crate) blob_target_size: usize,
    pub(crate) batch_delete_enabled: bool,
    pub(crate) batch_builder_max_outstanding_parts: usize,
    pub(crate) stats_collection_enabled: bool,
    pub(crate) stats_budget: usize,
    pub(crate) stats_untrimmable_columns: Arc<UntrimmableColumns>,
}

// TODO: Remove this once we're comfortable that there aren't any bugs.
pub(crate) const BATCH_DELETE_ENABLED: Config<bool> = Config::new(
    "persist_batch_delete_enabled",
    false,
    "Whether to actually delete blobs when batch delete is called (Materialize).",
);

/// A target maximum size of blob payloads in bytes. If a logical "batch" is
/// bigger than this, it will be broken up into smaller, independent pieces.
/// This is best-effort, not a guarantee (though as of 2022-06-09, we happen to
/// always respect it). This target size doesn't apply for an individual update
/// that exceeds it in size, but that scenario is almost certainly a mis-use of
/// the system.
pub(crate) const BLOB_TARGET_SIZE: Config<usize> = Config::new(
    "persist_blob_target_size",
    128 * MiB,
    "A target maximum size of persist blob payloads in bytes (Materialize).",
);

impl BatchBuilderConfig {
    /// Initialize a batch builder config based on a snapshot of the Persist config.
    pub fn new(value: &PersistConfig, _writer_id: &WriterId) -> Self {
        let writer_key = WriterKey::for_version(&value.build_version);
        BatchBuilderConfig {
            writer_key,
            blob_target_size: BLOB_TARGET_SIZE.get(value),
            batch_delete_enabled: BATCH_DELETE_ENABLED.get(value),
            batch_builder_max_outstanding_parts: value
                .dynamic
                .batch_builder_max_outstanding_parts(),
            stats_collection_enabled: STATS_COLLECTION_ENABLED.get(value),
            stats_budget: STATS_BUDGET_BYTES.get(value),
            stats_untrimmable_columns: Arc::new(untrimmable_columns(value)),
        }
    }
}

/// A list of (lowercase) column names that persist will always retain
/// stats for, even if it means going over the stats budget.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Arbitrary)]
pub(crate) struct UntrimmableColumns {
    /// Always retain columns whose lowercased names exactly equal any of these strings.
    pub equals: Vec<Cow<'static, str>>,
    /// Always retain columns whose lowercased names start with any of these strings.
    pub prefixes: Vec<Cow<'static, str>>,
    /// Always retain columns whose lowercased names end with any of these strings.
    pub suffixes: Vec<Cow<'static, str>>,
}

impl UntrimmableColumns {
    pub(crate) fn should_retain(&self, name: &str) -> bool {
        // TODO: see if there's a better way to match different formats than lowercasing
        // https://github.com/MaterializeInc/materialize/issues/21353#issue-1863623805
        let name_lower = name.to_lowercase();
        for s in &self.equals {
            if *s == name_lower {
                return true;
            }
        }
        for s in &self.prefixes {
            if name_lower.starts_with(s.as_ref()) {
                return true;
            }
        }
        for s in &self.suffixes {
            if name_lower.ends_with(s.as_ref()) {
                return true;
            }
        }
        false
    }
}

/// A builder for [Batches](Batch) that allows adding updates piece by piece and
/// then finishing it.
#[derive(Debug)]
pub struct BatchBuilder<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
{
    // TODO: Merge BatchBuilderInternal back into BatchBuilder once we no longer
    // need this separate schemas nonsense for compaction.
    //
    // In the meantime:
    // - Compaction uses `BatchBuilderInternal` directly, providing the real
    //   schema for stats, but with the builder's schema set to a fake Vec<u8>
    //   one.
    // - User writes use `BatchBuilder` with both this `stats_schemas` and
    //   `builder._schemas` the same.
    //
    // Instead of this BatchBuilder{,Internal} split, I initially tried to just
    // split the `add` and `finish` methods into versions that could override
    // the stats schema, but there are ownership issues with that approach that
    // I think are unresolvable.
    pub(crate) stats_schemas: Schemas<K, V>,
    pub(crate) builder: BatchBuilderInternal<K, V, T, D>,
}

impl<K, V, T, D> BatchBuilder<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    /// Finish writing this batch and return a handle to the written batch.
    ///
    /// This fails if any of the updates in this batch are beyond the given
    /// `upper`.
    pub async fn finish(
        self,
        registered_upper: Antichain<T>,
    ) -> Result<Batch<K, V, T, D>, InvalidUsage<T>> {
        self.builder
            .finish(&self.stats_schemas, registered_upper)
            .await
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
        self.builder
            .add(&self.stats_schemas, key, val, ts, diff)
            .await
    }
}

#[derive(Debug)]
pub(crate) struct BatchBuilderInternal<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
{
    lower: Antichain<T>,
    inclusive_upper: Antichain<Reverse<T>>,

    shard_id: ShardId,
    version: Version,
    blob: Arc<dyn Blob + Send + Sync>,
    metrics: Arc<Metrics>,
    _schemas: Schemas<K, V>,
    consolidate: bool,

    buffer: BatchBuffer<T, D>,

    max_kvt_in_run: Option<(Vec<u8>, Vec<u8>, T)>,
    runs: Vec<usize>,
    parts_written: usize,

    num_updates: usize,
    parts: BatchParts<T>,

    since: Antichain<T>,
    inline_upper: Antichain<T>,

    // These provide a bit more safety against appending a batch with the wrong
    // type to a shard.
    _phantom: PhantomData<(K, V, T, D)>,
}

impl<K, V, T, D> BatchBuilderInternal<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    pub(crate) fn new(
        cfg: BatchBuilderConfig,
        metrics: Arc<Metrics>,
        shard_metrics: Arc<ShardMetrics>,
        schemas: Schemas<K, V>,
        batch_write_metrics: BatchWriteMetrics,
        lower: Antichain<T>,
        blob: Arc<dyn Blob + Send + Sync>,
        isolated_runtime: Arc<IsolatedRuntime>,
        shard_id: ShardId,
        version: Version,
        since: Antichain<T>,
        inline_upper: Option<Antichain<T>>,
        consolidate: bool,
    ) -> Self {
        let parts = BatchParts::new(
            cfg.clone(),
            Arc::clone(&metrics),
            shard_metrics,
            shard_id,
            lower.clone(),
            Arc::clone(&blob),
            isolated_runtime,
            &batch_write_metrics,
        );
        Self {
            lower,
            inclusive_upper: Antichain::new(),
            blob,
            buffer: BatchBuffer::new(
                Arc::clone(&metrics),
                batch_write_metrics,
                cfg.blob_target_size,
                consolidate,
            ),
            metrics,
            _schemas: schemas,
            consolidate,
            max_kvt_in_run: None,
            parts_written: 0,
            runs: Vec::new(),
            num_updates: 0,
            parts,
            shard_id,
            version,
            since,
            // TODO: The default case would ideally be `{t + 1 for t in self.inclusive_upper}` but
            // there's nothing that lets us increment a timestamp. An empty
            // antichain is guaranteed to correctly bound the data in this
            // part, but it doesn't really tell us anything. Figure out how
            // to make a tighter bound, possibly by changing the part
            // description to be an _inclusive_ upper.
            inline_upper: inline_upper.unwrap_or_else(|| Antichain::new()),
            _phantom: PhantomData,
        }
    }

    /// Finish writing this batch and return a handle to the written batch.
    ///
    /// This fails if any of the updates in this batch are beyond the given
    /// `upper`.
    #[instrument(level = "debug", name = "batch::finish", fields(shard = %self.shard_id))]
    pub async fn finish<StatsK: Codec, StatsV: Codec>(
        mut self,
        stats_schemas: &Schemas<StatsK, StatsV>,
        registered_upper: Antichain<T>,
    ) -> Result<Batch<K, V, T, D>, InvalidUsage<T>> {
        if PartialOrder::less_than(&registered_upper, &self.lower) {
            return Err(InvalidUsage::InvalidBounds {
                lower: self.lower.clone(),
                upper: registered_upper,
            });
        }
        // when since is less than or equal to lower, the upper is a strict bound on the updates'
        // timestamp because no compaction has been performed. Because user batches are always
        // uncompacted, this ensures that new updates are recorded with valid timestamps.
        // Otherwise, we can make no assumptions about the timestamps
        if PartialOrder::less_equal(&self.since, &self.lower) {
            for ts in self.inclusive_upper.iter() {
                if registered_upper.less_equal(&ts.0) {
                    return Err(InvalidUsage::UpdateBeyondUpper {
                        ts: ts.0.clone(),
                        expected_upper: registered_upper.clone(),
                    });
                }
            }
        }

        let (key_lower, remainder) = self.buffer.drain();
        self.flush_part(stats_schemas, key_lower, remainder).await;

        let batch_delete_enabled = self.parts.cfg.batch_delete_enabled;
        let parts = self.parts.finish().await;

        let desc = Description::new(self.lower, registered_upper, self.since);
        let batch = Batch::new(
            batch_delete_enabled,
            Arc::clone(&self.metrics),
            self.blob,
            self.shard_id.clone(),
            self.version,
            HollowBatch {
                desc,
                parts,
                len: self.num_updates,
                runs: self.runs,
            },
        );

        Ok(batch)
    }

    /// Adds the given update to the batch.
    ///
    /// The update timestamp must be greater or equal to `lower` that was given
    /// when creating this [BatchBuilder].
    pub async fn add<StatsK: Codec, StatsV: Codec>(
        &mut self,
        stats_schemas: &Schemas<StatsK, StatsV>,
        key: &K,
        val: &V,
        ts: &T,
        diff: &D,
    ) -> Result<Added, InvalidUsage<T>> {
        if !self.lower.less_equal(ts) {
            return Err(InvalidUsage::UpdateNotBeyondLower {
                ts: ts.clone(),
                lower: self.lower.clone(),
            });
        }

        self.inclusive_upper.insert(Reverse(ts.clone()));

        match self.buffer.push(key, val, ts.clone(), diff.clone()) {
            Some((key_lower, part_to_flush)) => {
                self.flush_part(stats_schemas, key_lower, part_to_flush)
                    .await;
                Ok(Added::RecordAndParts)
            }
            None => Ok(Added::Record),
        }
    }

    /// Flushes the current part to Blob storage, first consolidating and then
    /// columnar encoding the updates. It is the caller's responsibility to
    /// chunk `current_part` to be no greater than
    /// [BatchBuilderConfig::blob_target_size], and must absolutely be less than
    /// [mz_persist::indexed::columnar::KEY_VAL_DATA_MAX_LEN]
    async fn flush_part<StatsK: Codec, StatsV: Codec>(
        &mut self,
        stats_schemas: &Schemas<StatsK, StatsV>,
        key_lower: Vec<u8>,
        columnar: ColumnarRecords,
    ) {
        let num_updates = columnar.len();
        if num_updates == 0 {
            return;
        }

        if self.consolidate {
            // if our parts are consolidated, we can rely on their sorted order to
            // appropriately determine runs of ordered parts
            let ((min_part_k, min_part_v), min_part_t, _d) =
                columnar.get(0).expect("num updates is greater than zero");
            let min_part_t = T::decode(min_part_t);
            let ((max_part_k, max_part_v), max_part_t, _d) = columnar
                .get(num_updates.saturating_sub(1))
                .expect("num updates is greater than zero");
            let max_part_t = T::decode(max_part_t);

            if let Some((max_run_k, max_run_v, max_run_t)) = &mut self.max_kvt_in_run {
                // start a new run if our part contains an update that exists in the
                // range already covered by the existing parts of the current run
                if (min_part_k, min_part_v, &min_part_t) < (max_run_k, max_run_v, max_run_t) {
                    self.runs.push(self.parts_written);
                }

                // given the above check, whether or not we extended an existing run or
                // started a new one, this part contains the greatest KVT in the run
                max_run_k.clear();
                max_run_v.clear();
                max_run_k.extend_from_slice(max_part_k);
                max_run_v.extend_from_slice(max_part_v);
                *max_run_t = max_part_t;
            } else {
                self.max_kvt_in_run = Some((max_part_k.to_vec(), max_part_v.to_vec(), max_part_t));
            }
        } else {
            // if our parts are not consolidated, we simply say each part is its own run.
            // NB: there is an implicit run starting at index 0
            if self.parts_written > 0 {
                self.runs.push(self.parts_written);
            }
        }

        let start = Instant::now();
        self.parts
            .write(
                stats_schemas,
                key_lower,
                columnar,
                self.inline_upper.clone(),
                self.since.clone(),
            )
            .await;
        self.metrics
            .compaction
            .batch
            .step_part_writing
            .inc_by(start.elapsed().as_secs_f64());

        self.parts_written += 1;
        self.num_updates += num_updates;
    }
}

#[derive(Debug)]
struct BatchBuffer<T, D> {
    metrics: Arc<Metrics>,
    batch_write_metrics: BatchWriteMetrics,
    blob_target_size: usize,
    consolidate: bool,

    key_buf: Vec<u8>,
    val_buf: Vec<u8>,

    current_part: Vec<((Range<usize>, Range<usize>), T, D)>,
    current_part_total_bytes: usize,
    current_part_key_bytes: usize,
    current_part_value_bytes: usize,
}

impl<T, D> BatchBuffer<T, D>
where
    T: Ord + Codec64,
    D: Semigroup + Codec64,
{
    fn new(
        metrics: Arc<Metrics>,
        batch_write_metrics: BatchWriteMetrics,
        blob_target_size: usize,
        should_consolidate: bool,
    ) -> Self {
        BatchBuffer {
            metrics,
            batch_write_metrics,
            blob_target_size,
            consolidate: should_consolidate,
            key_buf: Default::default(),
            val_buf: Default::default(),
            current_part: Default::default(),
            current_part_total_bytes: Default::default(),
            current_part_key_bytes: Default::default(),
            current_part_value_bytes: Default::default(),
        }
    }

    fn push<K: Codec, V: Codec>(
        &mut self,
        key: &K,
        val: &V,
        ts: T,
        diff: D,
    ) -> Option<(Vec<u8>, ColumnarRecords)> {
        let initial_key_buf_len = self.key_buf.len();
        let initial_val_buf_len = self.val_buf.len();
        self.metrics
            .codecs
            .key
            .encode(|| K::encode(key, &mut self.key_buf));
        self.metrics
            .codecs
            .val
            .encode(|| V::encode(val, &mut self.val_buf));
        let k_range = initial_key_buf_len..self.key_buf.len();
        let v_range = initial_val_buf_len..self.val_buf.len();
        let size = ColumnarRecordsBuilder::columnar_record_size(k_range.len(), v_range.len());

        self.current_part_total_bytes += size;
        self.current_part_key_bytes += k_range.len();
        self.current_part_value_bytes += v_range.len();
        self.current_part.push(((k_range, v_range), ts, diff));

        // if we've filled up a batch part, flush out to blob to keep our memory usage capped.
        if self.current_part_total_bytes >= self.blob_target_size {
            Some(self.drain())
        } else {
            None
        }
    }

    fn drain(&mut self) -> (Vec<u8>, ColumnarRecords) {
        let mut updates = Vec::with_capacity(self.current_part.len());
        for ((k_range, v_range), t, d) in self.current_part.drain(..) {
            updates.push(((&self.key_buf[k_range], &self.val_buf[v_range]), t, d));
        }

        if self.consolidate {
            let start = Instant::now();
            consolidate_updates(&mut updates);
            self.batch_write_metrics
                .step_consolidation
                .inc_by(start.elapsed().as_secs_f64());
        }

        if updates.is_empty() {
            self.key_buf.clear();
            self.val_buf.clear();
            return (
                vec![],
                ColumnarRecordsBuilder::default().finish(&self.metrics.columnar),
            );
        }

        let ((mut key_lower, _), _, _) = &updates[0];
        let start = Instant::now();
        let mut builder = ColumnarRecordsBuilder::default();
        builder.reserve_exact(
            self.current_part.len(),
            self.current_part_key_bytes,
            self.current_part_value_bytes,
        );
        for ((k, v), t, d) in updates {
            if self.consolidate {
                debug_assert!(
                    key_lower <= k,
                    "consolidated data should be presented in order"
                )
            } else {
                key_lower = k.min(key_lower);
            }
            // if this fails, the individual record is too big to fit in a ColumnarRecords by itself.
            // The limits are big, so this is a pretty extreme case that we intentionally don't handle
            // right now.
            assert!(builder.push(((k, v), T::encode(&t), D::encode(&d))));
        }
        let key_lower = truncate_bytes(key_lower, TRUNCATE_LEN, TruncateBound::Lower)
            .expect("lower bound always exists");
        let columnar = builder.finish(&self.metrics.columnar);

        self.batch_write_metrics
            .step_columnar_encoding
            .inc_by(start.elapsed().as_secs_f64());

        self.key_buf.clear();
        self.val_buf.clear();
        self.current_part_total_bytes = 0;
        self.current_part_key_bytes = 0;
        self.current_part_value_bytes = 0;
        assert_eq!(self.current_part.len(), 0);

        (key_lower, columnar)
    }
}

// TODO: If this is dropped, cancel (and delete?) any writing parts and delete
// any finished ones.
#[derive(Debug)]
pub(crate) struct BatchParts<T> {
    cfg: BatchBuilderConfig,
    metrics: Arc<Metrics>,
    shard_metrics: Arc<ShardMetrics>,
    shard_id: ShardId,
    lower: Antichain<T>,
    blob: Arc<dyn Blob + Send + Sync>,
    isolated_runtime: Arc<IsolatedRuntime>,
    writing_parts: VecDeque<JoinHandle<HollowBatchPart>>,
    finished_parts: Vec<HollowBatchPart>,
    batch_metrics: BatchWriteMetrics,
}

impl<T: Timestamp + Codec64> BatchParts<T> {
    pub(crate) fn new(
        cfg: BatchBuilderConfig,
        metrics: Arc<Metrics>,
        shard_metrics: Arc<ShardMetrics>,
        shard_id: ShardId,
        lower: Antichain<T>,
        blob: Arc<dyn Blob + Send + Sync>,
        isolated_runtime: Arc<IsolatedRuntime>,
        batch_metrics: &BatchWriteMetrics,
    ) -> Self {
        BatchParts {
            cfg,
            metrics,
            shard_metrics,
            shard_id,
            lower,
            blob,
            isolated_runtime,
            writing_parts: VecDeque::new(),
            finished_parts: Vec::new(),
            batch_metrics: batch_metrics.clone(),
        }
    }

    pub(crate) async fn write<K: Codec, V: Codec>(
        &mut self,
        schemas: &Schemas<K, V>,
        key_lower: Vec<u8>,
        updates: ColumnarRecords,
        upper: Antichain<T>,
        since: Antichain<T>,
    ) {
        let desc = Description::new(self.lower.clone(), upper, since);
        let metrics = Arc::clone(&self.metrics);
        let shard_metrics = Arc::clone(&self.shard_metrics);
        let blob = Arc::clone(&self.blob);
        let isolated_runtime = Arc::clone(&self.isolated_runtime);
        let batch_metrics = self.batch_metrics.clone();
        let partial_key = PartialBatchKey::new(&self.cfg.writer_key, &PartId::new());
        let key = partial_key.complete(&self.shard_id);
        let index = u64::cast_from(self.finished_parts.len() + self.writing_parts.len());
        let stats_collection_enabled = self.cfg.stats_collection_enabled;
        let stats_budget = self.cfg.stats_budget;
        let schemas = schemas.clone();
        let untrimmable_columns = Arc::clone(&self.cfg.stats_untrimmable_columns);

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

                let (stats, (buf, encode_time)) = isolated_runtime
                    .spawn_named(|| "batch::encode_part", async move {
                        let stats = if stats_collection_enabled {
                            let stats_start = Instant::now();
                            match PartStats::legacy_part_format(&schemas, &batch.updates) {
                                Ok(x) => {
                                    let mut trimmed_bytes = 0;
                                    let x = LazyPartStats::encode(&x, |s| {
                                        trimmed_bytes = trim_to_budget(s, stats_budget, |s| {
                                            untrimmable_columns.should_retain(s)
                                        });
                                    });
                                    Some((x, stats_start.elapsed(), trimmed_bytes))
                                }
                                Err(err) => {
                                    error!("failed to construct part stats: {}", err);
                                    None
                                }
                            }
                        } else {
                            None
                        };

                        let encode_start = Instant::now();
                        let mut buf = Vec::new();
                        batch.encode(&mut buf);

                        // Drop batch as soon as we can to reclaim its memory.
                        drop(batch);
                        (stats, (Bytes::from(buf), encode_start.elapsed()))
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
                    .inc_by(encode_time.as_secs_f64());

                let start = Instant::now();
                let payload_len = buf.len();
                let () = retry_external(&metrics.retries.external.batch_set, || async {
                    shard_metrics.blob_sets.inc();
                    blob.set(&key, Bytes::clone(&buf), Atomicity::RequireAtomic)
                        .await
                })
                .instrument(trace_span!("batch::set", payload_len))
                .await;
                batch_metrics.seconds.inc_by(start.elapsed().as_secs_f64());
                batch_metrics.bytes.inc_by(u64::cast_from(payload_len));
                batch_metrics.goodbytes.inc_by(u64::cast_from(goodbytes));
                let stats = stats.map(|(stats, stats_step_timing, trimmed_bytes)| {
                    batch_metrics
                        .step_stats
                        .inc_by(stats_step_timing.as_secs_f64());
                    if trimmed_bytes > 0 {
                        metrics.pushdown.parts_stats_trimmed_count.inc();
                        metrics
                            .pushdown
                            .parts_stats_trimmed_bytes
                            .inc_by(u64::cast_from(trimmed_bytes));
                    }
                    stats
                });

                HollowBatchPart {
                    key: partial_key,
                    encoded_size_bytes: payload_len,
                    key_lower,
                    stats,
                }
            }
            .instrument(write_span),
        );
        self.writing_parts.push_back(handle);

        while self.writing_parts.len() > self.cfg.batch_builder_max_outstanding_parts {
            batch_metrics.write_stalls.inc();
            let handle = self
                .writing_parts
                .pop_front()
                .expect("pop failed when len was just > some usize");
            let part = handle
                .instrument(debug_span!("batch::max_outstanding"))
                .wait_and_assert_finished()
                .await;
            self.finished_parts.push(part);
        }
    }

    #[instrument(level = "debug", name = "batch::finish_upload", fields(shard = %self.shard_id))]
    pub(crate) async fn finish(self) -> Vec<HollowBatchPart> {
        let mut parts = self.finished_parts;
        for handle in self.writing_parts {
            let part = handle.wait_and_assert_finished().await;
            parts.push(part);
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
    use crate::tests::{all_ok, CodecProduct};
    use crate::PersistLocation;

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn batch_builder_flushing() {
        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let cache = PersistClientCache::new_no_metrics();
        // Set blob_target_size to 0 so that each row gets forced into its own
        // batch. Set max_outstanding to a small value that's >1 to test various
        // edge cases below.
        cache.cfg.set_config(&BLOB_TARGET_SIZE, 0);
        cache.cfg.dynamic.set_batch_builder_max_outstanding_parts(2);
        let client = cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("client construction failed");
        let (mut write, mut read) = client
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;

        // A new builder has no writing or finished parts.
        let builder = write.builder(Antichain::from_elem(0));
        let schemas = builder.stats_schemas;
        let mut builder = builder.builder;
        assert_eq!(builder.parts.writing_parts.len(), 0);
        assert_eq!(builder.parts.finished_parts.len(), 0);

        // We set blob_target_size to 0, so the first update gets forced out
        // into a batch.
        builder
            .add(
                &schemas,
                &data[0].0 .0,
                &data[0].0 .1,
                &data[0].1,
                &data[0].2,
            )
            .await
            .expect("invalid usage");
        assert_eq!(builder.parts.writing_parts.len(), 1);
        assert_eq!(builder.parts.finished_parts.len(), 0);

        // We set batch_builder_max_outstanding_parts to 2, so we are allowed to
        // pipeline a second part.
        builder
            .add(
                &schemas,
                &data[1].0 .0,
                &data[1].0 .1,
                &data[1].1,
                &data[1].2,
            )
            .await
            .expect("invalid usage");
        assert_eq!(builder.parts.writing_parts.len(), 2);
        assert_eq!(builder.parts.finished_parts.len(), 0);

        // But now that we have 3 parts, the add call back-pressures until the
        // first one finishes.
        builder
            .add(
                &schemas,
                &data[2].0 .0,
                &data[2].0 .1,
                &data[2].1,
                &data[2].2,
            )
            .await
            .expect("invalid usage");
        assert_eq!(builder.parts.writing_parts.len(), 2);
        assert_eq!(builder.parts.finished_parts.len(), 1);

        // Finish off the batch and verify that the keys and such get plumbed
        // correctly by reading the data back.
        let batch = builder
            .finish(&schemas, Antichain::from_elem(4))
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

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn batch_builder_keys() {
        let cache = PersistClientCache::new_no_metrics();
        // Set blob_target_size to 0 so that each row gets forced into its own batch part
        cache.cfg.set_config(&BLOB_TARGET_SIZE, 0);
        let client = cache
            .open(PersistLocation::new_in_mem())
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
                    assert_eq!(writer, WriterKey::for_version(&cache.cfg.build_version));
                }
                _ => panic!("unparseable blob key"),
            }
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn batch_builder_partial_order() {
        let cache = PersistClientCache::new_no_metrics();
        // Set blob_target_size to 0 so that each row gets forced into its own batch part
        cache.cfg.set_config(&BLOB_TARGET_SIZE, 0);
        let client = cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("client construction failed");
        let shard_id = ShardId::new();
        let (mut write, _) = client
            .expect_open::<String, String, CodecProduct, i64>(shard_id)
            .await;

        let batch = write
            .batch(
                &[
                    (
                        ("1".to_owned(), "one".to_owned()),
                        CodecProduct::new(0, 10),
                        1,
                    ),
                    (
                        ("2".to_owned(), "two".to_owned()),
                        CodecProduct::new(10, 0),
                        1,
                    ),
                ],
                Antichain::from_elem(CodecProduct::new(0, 0)),
                Antichain::from_iter([CodecProduct::new(0, 11), CodecProduct::new(10, 1)]),
            )
            .await
            .expect("invalid usage");

        assert_eq!(batch.batch.parts.len(), 2);
        for part in &batch.batch.parts {
            match BlobKey::parse_ids(&part.key.complete(&shard_id)) {
                Ok((shard, PartialBlobKey::Batch(writer, _))) => {
                    assert_eq!(shard.to_string(), shard_id.to_string());
                    assert_eq!(writer, WriterKey::for_version(&cache.cfg.build_version));
                }
                _ => panic!("unparseable blob key"),
            }
        }
    }

    #[mz_ore::test]
    fn untrimmable_columns() {
        let untrimmable = UntrimmableColumns {
            equals: vec!["abc".into(), "def".into()],
            prefixes: vec!["123".into(), "234".into()],
            suffixes: vec!["xyz".into()],
        };

        // equals
        assert!(untrimmable.should_retain("abc"));
        assert!(untrimmable.should_retain("ABC"));
        assert!(untrimmable.should_retain("aBc"));
        assert!(!untrimmable.should_retain("abcd"));
        assert!(untrimmable.should_retain("deF"));
        assert!(!untrimmable.should_retain("defg"));

        // prefix
        assert!(untrimmable.should_retain("123"));
        assert!(untrimmable.should_retain("123-4"));
        assert!(untrimmable.should_retain("1234"));
        assert!(untrimmable.should_retain("234"));
        assert!(!untrimmable.should_retain("345"));

        // suffix
        assert!(untrimmable.should_retain("ijk_xyZ"));
        assert!(untrimmable.should_retain("ww-XYZ"));
        assert!(!untrimmable.should_retain("xya"));
    }
}
