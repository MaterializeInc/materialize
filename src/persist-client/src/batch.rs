// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A handle to a batch of updates

use arrow::array::Array;
use std::borrow::Cow;
use std::collections::{BTreeSet, VecDeque};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use futures_util::stream::{FuturesUnordered, StreamExt};
use mz_dyncfg::Config;
use mz_ore::cast::CastFrom;
use mz_ore::task::{JoinHandle, JoinHandleExt};
use mz_ore::{instrument, soft_panic_or_log};
use mz_persist::indexed::columnar::{
    ColumnarRecords, ColumnarRecordsBuilder, ColumnarRecordsStructuredExt,
};
use mz_persist::indexed::encoding::{BatchColumnarFormat, BlobTraceBatchPart, BlobTraceUpdates};
use mz_persist::location::Blob;
use mz_persist_types::stats::{trim_to_budget, truncate_bytes, TruncateBound, TRUNCATE_LEN};
use mz_persist_types::{Codec, Codec64};
use mz_proto::RustType;
use mz_timely_util::order::Reverse;
use proptest_derive::Arbitrary;
use semver::Version;
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::{debug_span, trace_span, warn, Instrument};

use crate::async_runtime::IsolatedRuntime;
use crate::cfg::MiB;
use crate::error::InvalidUsage;
use crate::internal::encoding::{LazyInlineBatchPart, LazyPartStats, Schemas};
use crate::internal::machine::retry_external;
use crate::internal::metrics::{BatchWriteMetrics, Metrics, RetryMetrics, ShardMetrics};
use crate::internal::paths::{PartId, PartialBatchKey, WriterKey};
use crate::internal::state::{
    BatchPart, HollowBatch, HollowBatchPart, ProtoInlineBatchPart, WRITE_DIFFS_SUM,
};
use crate::stats::{
    encode_updates, untrimmable_columns, STATS_BUDGET_BYTES, STATS_COLLECTION_ENABLED,
};
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
    pub(crate) shard_metrics: Arc<ShardMetrics>,

    /// The version of Materialize which wrote this batch.
    pub(crate) version: Version,

    /// A handle to the data represented by this batch.
    pub(crate) batch: HollowBatch<T>,

    /// Handle to the [Blob] that the blobs of this batch were uploaded to.
    pub(crate) blob: Arc<dyn Blob>,

    // These provide a bit more safety against appending a batch with the wrong
    // type to a shard.
    pub(crate) _phantom: PhantomData<fn() -> (K, V, T, D)>,
}

impl<K, V, T, D> Drop for Batch<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    fn drop(&mut self) {
        if self.batch.part_count() > 0 {
            warn!(
                "un-consumed Batch, with {} parts and dangling blob keys: {:?}",
                self.batch.part_count(),
                self.batch
                    .parts
                    .iter()
                    .map(|x| x.printable_name())
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
        blob: Arc<dyn Blob>,
        shard_metrics: Arc<ShardMetrics>,
        version: Version,
        batch: HollowBatch<T>,
    ) -> Self {
        Self {
            batch_delete_enabled,
            metrics,
            shard_metrics,
            version,
            batch,
            blob,
            _phantom: PhantomData,
        }
    }

    /// The `shard_id` of this [Batch].
    pub fn shard_id(&self) -> ShardId {
        self.shard_metrics.shard_id
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
    #[instrument(level = "debug", fields(shard = %self.shard_id()))]
    pub async fn delete(mut self) {
        self.mark_consumed();
        if !self.batch_delete_enabled {
            return;
        }
        let mut deletes = PartDeletes::default();
        for part in self.batch.parts.iter() {
            deletes.add(part);
        }
        let () = deletes
            .delete(
                &self.blob,
                self.shard_id(),
                &self.metrics.retries.external.batch_delete,
            )
            .await;
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
            shard_id: self.shard_metrics.shard_id.into_proto(),
            version: self.version.to_string(),
            batch: Some(self.batch.into_proto()),
        };
        self.mark_consumed();
        ret
    }

    pub(crate) async fn flush_to_blob<StatsK: Codec, StatsV: Codec>(
        &mut self,
        cfg: &BatchBuilderConfig,
        batch_metrics: &BatchWriteMetrics,
        isolated_runtime: &Arc<IsolatedRuntime>,
        stats_schemas: &Schemas<StatsK, StatsV>,
    ) {
        // It's necessary for correctness to keep the parts in the same order.
        // We could introduce concurrency here with FuturesOrdered, but it would
        // be pretty unexpected to have inline writes in more than one part, so
        // don't bother.
        let mut parts = Vec::new();
        for part in self.batch.parts.drain(..) {
            let (updates, ts_rewrite) = match part {
                BatchPart::Hollow(x) => {
                    parts.push(BatchPart::Hollow(x));
                    continue;
                }
                BatchPart::Inline {
                    updates,
                    ts_rewrite,
                } => (updates, ts_rewrite),
            };
            let updates = updates
                .decode::<T>(&self.metrics.columnar)
                .expect("valid inline part");
            let key_lower = updates.key_lower().to_vec();
            let diffs_sum =
                diffs_sum::<D>(updates.updates.records()).expect("inline parts are not empty");

            let write_span =
                debug_span!("batch::flush_to_blob", shard = %self.shard_metrics.shard_id)
                    .or_current();
            let handle = mz_ore::task::spawn(
                || "batch::flush_to_blob",
                BatchParts::write_hollow_part(
                    cfg.clone(),
                    Arc::clone(&self.blob),
                    Arc::clone(&self.metrics),
                    Arc::clone(&self.shard_metrics),
                    batch_metrics.clone(),
                    Arc::clone(isolated_runtime),
                    updates,
                    key_lower,
                    ts_rewrite,
                    D::encode(&diffs_sum),
                    stats_schemas.clone(),
                )
                .instrument(write_span),
            );
            let part = handle.await.expect("part write task failed");
            parts.push(part);
        }
        self.batch.parts = parts;
    }
}

impl<K, V, T, D> Batch<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + TotalOrder,
    D: Semigroup + Codec64,
{
    /// Efficiently rewrites the timestamps in this not-yet-committed batch.
    ///
    /// This [Batch] represents potentially large amounts of data, which may
    /// have partly or entirely been spilled to s3. This call bulk edits the
    /// timestamps of all data in this batch in a metadata-only operation (i.e.
    /// without network calls).
    ///
    /// Specifically, every timestamp in the batch is logically advanced_by the
    /// provided `frontier`.
    ///
    /// This method may be called multiple times, with later calls overriding
    /// previous ones, but the rewrite frontier may not regress across calls.
    ///
    /// When this batch was created, it was given an `upper`, which bounds the
    /// staged data it represents. To allow rewrite past this original `upper`,
    /// this call accepts a new `upper` which replaces the previous one. Like
    /// the rewrite frontier, the upper may not regress across calls.
    ///
    /// Multiple batches with various rewrite frontiers may be used in a single
    /// [crate::write::WriteHandle::compare_and_append_batch] call. This is an
    /// expected usage.
    ///
    /// This feature requires that the timestamp impls `TotalOrder`. This is
    /// because we need to be able to verify that the contained data, after the
    /// rewrite forward operation, still respects the new upper. It turns out
    /// that, given the metadata persist currently collects during batch
    /// collection, this is possible for totally ordered times, but it's known
    /// to be _not possible_ for partially ordered times. It is believed that we
    /// could fix this by collecting different metadata in batch creation (e.g.
    /// the join of or an antichain of the original contained timestamps), but
    /// the experience of #26384 has shaken our confidence in our own abilities
    /// to reason about partially ordered times and anyway all the initial uses
    /// have totally ordered times.
    pub fn rewrite_ts(
        &mut self,
        frontier: &Antichain<T>,
        new_upper: Antichain<T>,
    ) -> Result<(), InvalidUsage<T>> {
        self.batch
            .rewrite_ts(frontier, new_upper)
            .map_err(InvalidUsage::InvalidRewrite)
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
    pub(crate) batch_columnar_format: BatchColumnarFormat,
    pub(crate) batch_columnar_stats_only_override: bool,
    pub(crate) batch_record_part_format: bool,
    pub(crate) inline_writes_single_max_bytes: usize,
    pub(crate) stats_collection_enabled: bool,
    pub(crate) stats_budget: usize,
    pub(crate) stats_untrimmable_columns: Arc<UntrimmableColumns>,
    pub(crate) write_diffs_sum: bool,
}

// TODO: Remove this once we're comfortable that there aren't any bugs.
pub(crate) const BATCH_DELETE_ENABLED: Config<bool> = Config::new(
    "persist_batch_delete_enabled",
    false,
    "Whether to actually delete blobs when batch delete is called (Materialize).",
);

pub(crate) const BATCH_COLUMNAR_FORMAT: Config<&'static str> = Config::new(
    "persist_batch_columnar_format",
    BatchColumnarFormat::default().as_str(),
    "Columnar format for a batch written to Persist, either 'row', 'both', or 'both_v2' (Materialize).",
);

pub(crate) const BATCH_COLUMNAR_STATS_ONLY_OVERRIDE: Config<bool> = Config::new(
    "persist_batch_columnar_stats_only_override",
    false,
    "Regardless of the value for 'persist_batch_columnar_format' only use structured \
    data for stats collection and do no durably persist it (Materialize).",
);

pub(crate) const BATCH_RECORD_PART_FORMAT: Config<bool> = Config::new(
    "persist_batch_record_part_format",
    false,
    "Wether we record the format of the Part in state (Materialize).",
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

pub(crate) const INLINE_WRITES_SINGLE_MAX_BYTES: Config<usize> = Config::new(
    "persist_inline_writes_single_max_bytes",
    0,
    "The (exclusive) maximum size of a write that persist will inline in metadata.",
);

pub(crate) const INLINE_WRITES_TOTAL_MAX_BYTES: Config<usize> = Config::new(
    "persist_inline_writes_total_max_bytes",
    0,
    "\
    The (exclusive) maximum total size of inline writes in metadata before \
    persist will backpressure them by flushing out to s3.",
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
            batch_columnar_format: BatchColumnarFormat::from_str(&BATCH_COLUMNAR_FORMAT.get(value)),
            batch_columnar_stats_only_override: BATCH_COLUMNAR_STATS_ONLY_OVERRIDE.get(value),
            batch_record_part_format: BATCH_RECORD_PART_FORMAT.get(value),
            inline_writes_single_max_bytes: INLINE_WRITES_SINGLE_MAX_BYTES.get(value),
            stats_collection_enabled: STATS_COLLECTION_ENABLED.get(value),
            stats_budget: STATS_BUDGET_BYTES.get(value),
            stats_untrimmable_columns: Arc::new(untrimmable_columns(value)),
            write_diffs_sum: WRITE_DIFFS_SUM.get(value),
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

    // Reusable buffers for encoding data. Should be cleared after use!
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) key_buf: Vec<u8>,
    pub(crate) val_buf: Vec<u8>,

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
        self.metrics
            .codecs
            .key
            .encode(|| K::encode(key, &mut self.key_buf));
        self.metrics
            .codecs
            .val
            .encode(|| V::encode(val, &mut self.val_buf));
        let result = self
            .builder
            .add(&self.stats_schemas, &self.key_buf, &self.val_buf, ts, diff)
            .await;
        self.key_buf.clear();
        self.val_buf.clear();
        result
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
    blob: Arc<dyn Blob>,
    metrics: Arc<Metrics>,
    expect_consolidated: bool,

    buffer: BatchBuffer,

    max_kvt_in_run: Option<(Vec<u8>, Vec<u8>, T)>,
    runs: Vec<usize>,
    parts_written: usize,

    num_updates: usize,
    parts: BatchParts<T>,

    since: Antichain<T>,
    inline_upper: Antichain<T>,

    // These provide a bit more safety against appending a batch with the wrong
    // type to a shard.
    _phantom: PhantomData<fn(K, V, T, D)>,
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
        batch_write_metrics: BatchWriteMetrics,
        lower: Antichain<T>,
        blob: Arc<dyn Blob>,
        isolated_runtime: Arc<IsolatedRuntime>,
        shard_id: ShardId,
        version: Version,
        since: Antichain<T>,
        inline_upper: Option<Antichain<T>>,
        expect_consolidated: bool,
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
            buffer: BatchBuffer::new(Arc::clone(&metrics), cfg.blob_target_size),
            metrics,
            expect_consolidated,
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
    pub async fn finish(
        mut self,
        stats_schemas: &Schemas<K, V>,
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

        let remainder = self.buffer.drain();
        self.flush_part(stats_schemas, remainder).await;

        let batch_delete_enabled = self.parts.cfg.batch_delete_enabled;
        let shard_metrics = Arc::clone(&self.parts.shard_metrics);
        let parts = self.parts.finish().await;

        let desc = Description::new(self.lower, registered_upper, self.since);
        let batch = Batch::new(
            batch_delete_enabled,
            Arc::clone(&self.metrics),
            self.blob,
            shard_metrics,
            self.version,
            HollowBatch::new(desc, parts, self.num_updates, self.runs),
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
        key: &[u8],
        val: &[u8],
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
            Some(part_to_flush) => {
                self.flush_part(stats_schemas, part_to_flush).await;
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
        columnar: ColumnarRecords,
    ) {
        let key_lower = {
            let keys = columnar.keys();
            if keys.is_empty() {
                &[]
            } else if self.expect_consolidated {
                columnar.keys().value(0)
            } else {
                // WIP ::arrow::compute::min_binary(columnar.keys()).expect("min of nonempty array")
                columnar
                    .keys()
                    .iter()
                    .flatten()
                    .min()
                    .expect("min of nonempty array")
            }
        };
        let key_lower = truncate_bytes(key_lower, TRUNCATE_LEN, TruncateBound::Lower)
            .expect("lower bound always exists");

        let num_updates = columnar.len();
        if num_updates == 0 {
            return;
        }
        let diffs_sum = diffs_sum::<D>(&columnar).expect("part is non empty");

        if self.expect_consolidated {
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
                // Our caller has promised to provide us data in sorted order. Verify that
                // the smallest data in the part is not regressing... but for now, keep splitting
                // runs as before.
                if (min_part_k, min_part_v, &min_part_t) < (max_run_k, max_run_v, max_run_t) {
                    soft_panic_or_log!("expected data in sorted order");
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
                diffs_sum,
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
struct BatchBuffer {
    metrics: Arc<Metrics>,
    blob_target_size: usize,
    records_builder: ColumnarRecordsBuilder,
}

impl BatchBuffer {
    fn new(metrics: Arc<Metrics>, blob_target_size: usize) -> Self {
        BatchBuffer {
            metrics,
            blob_target_size,
            records_builder: ColumnarRecordsBuilder::default(),
        }
    }

    fn push<T: Codec64, D: Codec64>(
        &mut self,
        key: &[u8],
        val: &[u8],
        ts: T,
        diff: D,
    ) -> Option<ColumnarRecords> {
        let update = ((key, val), ts.encode(), diff.encode());
        assert!(
            self.records_builder.push(update),
            "single update overflowed an i32"
        );

        // if we've filled up a batch part, flush out to blob to keep our memory usage capped.
        if self.records_builder.total_bytes() >= self.blob_target_size {
            Some(self.drain())
        } else {
            None
        }
    }

    fn drain(&mut self) -> ColumnarRecords {
        // TODO: we're in a position to do a very good estimate here, instead of using the default.
        let builder = mem::take(&mut self.records_builder);
        let records = builder.finish(&self.metrics.columnar);
        assert_eq!(self.records_builder.len(), 0);
        records
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
    blob: Arc<dyn Blob>,
    isolated_runtime: Arc<IsolatedRuntime>,
    writing_parts: VecDeque<JoinHandle<BatchPart<T>>>,
    finished_parts: Vec<BatchPart<T>>,
    batch_metrics: BatchWriteMetrics,
}

impl<T: Timestamp + Codec64> BatchParts<T> {
    pub(crate) fn new(
        cfg: BatchBuilderConfig,
        metrics: Arc<Metrics>,
        shard_metrics: Arc<ShardMetrics>,
        shard_id: ShardId,
        lower: Antichain<T>,
        blob: Arc<dyn Blob>,
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

    pub(crate) async fn write<K: Codec, V: Codec, D: Codec64>(
        &mut self,
        schemas: &Schemas<K, V>,
        key_lower: Vec<u8>,
        updates: ColumnarRecords,
        upper: Antichain<T>,
        since: Antichain<T>,
        diffs_sum: D,
    ) {
        let desc = Description::new(self.lower.clone(), upper, since);
        let batch_metrics = self.batch_metrics.clone();
        let index = u64::cast_from(self.finished_parts.len() + self.writing_parts.len());
        let ts_rewrite = None;

        let handle = if updates.goodbytes() < self.cfg.inline_writes_single_max_bytes {
            let span = debug_span!("batch::inline_part", shard = %self.shard_id).or_current();
            mz_ore::task::spawn(
                || "batch::inline_part",
                async move {
                    let start = Instant::now();
                    let updates = LazyInlineBatchPart::from(&ProtoInlineBatchPart {
                        desc: Some(desc.into_proto()),
                        index: index.into_proto(),
                        updates: Some(updates.into_proto()),
                    });
                    batch_metrics
                        .step_inline
                        .inc_by(start.elapsed().as_secs_f64());
                    BatchPart::Inline {
                        updates,
                        ts_rewrite,
                    }
                }
                .instrument(span),
            )
        } else {
            let part = BlobTraceBatchPart {
                desc,
                updates: BlobTraceUpdates::Row(updates),
                index,
            };
            let write_span =
                debug_span!("batch::write_part", shard = %self.shard_metrics.shard_id).or_current();
            mz_ore::task::spawn(
                || "batch::write_part",
                BatchParts::write_hollow_part(
                    self.cfg.clone(),
                    Arc::clone(&self.blob),
                    Arc::clone(&self.metrics),
                    Arc::clone(&self.shard_metrics),
                    batch_metrics.clone(),
                    Arc::clone(&self.isolated_runtime),
                    part,
                    key_lower,
                    ts_rewrite,
                    D::encode(&diffs_sum),
                    schemas.clone(),
                )
                .instrument(write_span),
            )
        };
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

    async fn write_hollow_part<K: Codec, V: Codec>(
        cfg: BatchBuilderConfig,
        blob: Arc<dyn Blob>,
        metrics: Arc<Metrics>,
        shard_metrics: Arc<ShardMetrics>,
        batch_metrics: BatchWriteMetrics,
        isolated_runtime: Arc<IsolatedRuntime>,
        mut updates: BlobTraceBatchPart<T>,
        key_lower: Vec<u8>,
        ts_rewrite: Option<Antichain<T>>,
        diffs_sum: [u8; 8],
        schemas: Schemas<K, V>,
    ) -> BatchPart<T> {
        let partial_key = PartialBatchKey::new(&cfg.writer_key, &PartId::new());
        let key = partial_key.complete(&shard_metrics.shard_id);
        let goodbytes = updates.updates.records().goodbytes();
        let metrics_ = Arc::clone(&metrics);

        let (stats, (buf, encode_time)) = isolated_runtime
            .spawn_named(|| "batch::encode_part", async move {
                // Only encode our updates in a structured format if required, it's expensive.
                let stats = 'collect_stats: {
                    if cfg.stats_collection_enabled || cfg.batch_columnar_format.is_structured() {
                        let result = metrics_.columnar.arrow().measure_part_build(|| {
                            encode_updates(&schemas, &updates.updates, &cfg.batch_columnar_format)
                        });

                        // We can't collect stats if we failed to encode in a columnar format.
                        let Ok(((key_col, val_col), stats)) = result else {
                            tracing::error!(?result, "failed to encode in columnar format!");
                            break 'collect_stats None;
                        };

                        // Write a structured batch if the dyncfg is enabled and we're the stats
                        // override is not set.
                        if let BlobTraceUpdates::Row(record) = &updates.updates {
                            if cfg.batch_columnar_format.is_structured()
                                && !cfg.batch_columnar_stats_only_override
                            {
                                let record_ext = ColumnarRecordsStructuredExt {
                                    key: key_col,
                                    val: val_col,
                                };
                                updates.updates = BlobTraceUpdates::Both(record.clone(), record_ext)
                            }
                        }

                        // Collect stats about the updates, if stats collection is enabled.
                        if cfg.stats_collection_enabled {
                            let trimmed_start = Instant::now();
                            let mut trimmed_bytes = 0;
                            let trimmed_stats = LazyPartStats::encode(&stats, |s| {
                                trimmed_bytes = trim_to_budget(s, cfg.stats_budget, |s| {
                                    cfg.stats_untrimmable_columns.should_retain(s)
                                })
                            });
                            let trimmed_duration = trimmed_start.elapsed();

                            Some((trimmed_stats, trimmed_duration, trimmed_bytes))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                };

                let encode_start = Instant::now();
                let mut buf = Vec::new();
                updates.encode(&mut buf, &metrics_.columnar);

                // Drop batch as soon as we can to reclaim its memory.
                drop(updates);
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
            blob.set(&key, Bytes::clone(&buf)).await
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
        let format = if cfg.batch_record_part_format {
            Some(cfg.batch_columnar_format)
        } else {
            None
        };

        BatchPart::Hollow(HollowBatchPart {
            key: partial_key,
            encoded_size_bytes: payload_len,
            key_lower,
            stats,
            ts_rewrite,
            diffs_sum: cfg.write_diffs_sum.then_some(diffs_sum),
            format,
        })
    }

    #[instrument(level = "debug", name = "batch::finish_upload", fields(shard = %self.shard_id))]
    pub(crate) async fn finish(self) -> Vec<BatchPart<T>> {
        let mut parts = self.finished_parts;
        for handle in self.writing_parts {
            let part = handle.wait_and_assert_finished().await;
            parts.push(part);
        }
        parts
    }
}

pub(crate) fn validate_truncate_batch<T: Timestamp>(
    batch: &HollowBatch<T>,
    truncate: &Description<T>,
    any_batch_rewrite: bool,
) -> Result<(), InvalidUsage<T>> {
    // If rewrite_ts is used, we don't allow truncation, to keep things simpler
    // to reason about.
    if any_batch_rewrite {
        // We allow a new upper to be specified at rewrite time, so that's easy:
        // it must match exactly. This is both consistent with the upper
        // requirement below and proves that there is no data to truncate past
        // the upper.
        if truncate.upper() != batch.desc.upper() {
            return Err(InvalidUsage::InvalidRewrite(format!(
                "rewritten batch might have data past {:?} up to {:?}",
                truncate.upper().elements(),
                batch.desc.upper().elements(),
            )));
        }
        // To prove that there is no data to truncate below the lower, require
        // that the lower is <= the rewrite ts.
        for part in batch.parts.iter() {
            let part_lower_bound = part.ts_rewrite().unwrap_or(batch.desc.lower());
            if !PartialOrder::less_equal(truncate.lower(), part_lower_bound) {
                return Err(InvalidUsage::InvalidRewrite(format!(
                    "rewritten batch might have data below {:?} at {:?}",
                    truncate.lower().elements(),
                    part_lower_bound.elements(),
                )));
            }
        }
    }

    let batch = &batch.desc;
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

#[derive(Debug, Default)]
pub(crate) struct PartDeletes(BTreeSet<PartialBatchKey>);

impl PartDeletes {
    // Adds the part to the set to be deleted and returns true if it was newly
    // inserted.
    pub fn add<T>(&mut self, part: &BatchPart<T>) -> bool {
        match part {
            BatchPart::Hollow(x) => self.0.insert(x.key.clone()),
            BatchPart::Inline { .. } => {
                // Nothing to delete.
                true
            }
        }
    }

    pub async fn delete(
        self,
        blob: &Arc<dyn Blob>,
        shard_id: ShardId,
        metrics: &Arc<RetryMetrics>,
    ) {
        let deletes = FuturesUnordered::new();
        for key in self.0 {
            let metrics = Arc::clone(metrics);
            let blob = Arc::clone(blob);
            deletes.push(async move {
                let key = key.complete(&shard_id);
                retry_external(&metrics, || blob.delete(&key)).await;
            });
        }
        let () = deletes.collect().await;
    }
}

impl Deref for PartDeletes {
    type Target = BTreeSet<PartialBatchKey>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Returns the total sum of diffs or None if there were no updates.
fn diffs_sum<D: Semigroup + Codec64>(updates: &ColumnarRecords) -> Option<D> {
    let mut sum = None;
    for (_kv, _t, d) in updates.iter() {
        let d = D::decode(d);
        match &mut sum {
            None => sum = Some(d),
            Some(x) => x.plus_equals(&d),
        }
    }

    sum
}

#[cfg(test)]
mod tests {
    use mz_dyncfg::ConfigUpdates;
    use timely::order::Product;

    use crate::cache::PersistClientCache;
    use crate::internal::paths::{BlobKey, PartialBlobKey};
    use crate::tests::{all_ok, new_test_client};
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
        let ((k, v), t, d) = &data[0];
        let key = k.encode_to_vec();
        let val = v.encode_to_vec();
        builder
            .add(&schemas, &key, &val, t, d)
            .await
            .expect("invalid usage");
        assert_eq!(builder.parts.writing_parts.len(), 1);
        assert_eq!(builder.parts.finished_parts.len(), 0);

        // We set batch_builder_max_outstanding_parts to 2, so we are allowed to
        // pipeline a second part.
        let ((k, v), t, d) = &data[1];
        let key = k.encode_to_vec();
        let val = v.encode_to_vec();
        builder
            .add(&schemas, &key, &val, t, d)
            .await
            .expect("invalid usage");
        assert_eq!(builder.parts.writing_parts.len(), 2);
        assert_eq!(builder.parts.finished_parts.len(), 0);

        // But now that we have 3 parts, the add call back-pressures until the
        // first one finishes.
        let ((k, v), t, d) = &data[2];
        let key = k.encode_to_vec();
        let val = v.encode_to_vec();
        builder
            .add(&schemas, &key, &val, t, d)
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
        assert_eq!(batch.batch.part_count(), 3);
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

        assert_eq!(batch.batch.part_count(), 3);
        for part in &batch.batch.parts {
            let part = match part {
                BatchPart::Hollow(x) => x,
                BatchPart::Inline { .. } => panic!("batch unexpectedly used inline part"),
            };
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
            .expect_open::<String, String, Product<u32, u32>, i64>(shard_id)
            .await;

        let batch = write
            .batch(
                &[
                    (("1".to_owned(), "one".to_owned()), Product::new(0, 10), 1),
                    (("2".to_owned(), "two".to_owned()), Product::new(10, 0), 1),
                ],
                Antichain::from_elem(Product::new(0, 0)),
                Antichain::from_iter([Product::new(0, 11), Product::new(10, 1)]),
            )
            .await
            .expect("invalid usage");

        assert_eq!(batch.batch.part_count(), 2);
        for part in &batch.batch.parts {
            let part = match part {
                BatchPart::Hollow(x) => x,
                BatchPart::Inline { .. } => panic!("batch unexpectedly used inline part"),
            };
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

    // NB: Most edge cases are exercised in datadriven tests.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn rewrite_ts_example(dyncfgs: ConfigUpdates) {
        let client = new_test_client(&dyncfgs).await;
        let (mut write, read) = client
            .expect_open::<String, (), u64, i64>(ShardId::new())
            .await;

        let mut batch = write.builder(Antichain::from_elem(0));
        batch.add(&"foo".to_owned(), &(), &0, &1).await.unwrap();
        let batch = batch.finish(Antichain::from_elem(1)).await.unwrap();

        // Roundtrip through a transmittable batch.
        let batch = batch.into_transmittable_batch();
        let mut batch = write.batch_from_transmittable_batch(batch);
        batch
            .rewrite_ts(&Antichain::from_elem(2), Antichain::from_elem(3))
            .unwrap();
        write
            .expect_compare_and_append_batch(&mut [&mut batch], 0, 3)
            .await;

        let (actual, _) = read.expect_listen(0).await.read_until(&3).await;
        let expected = vec![(((Ok("foo".to_owned())), Ok(())), 2, 1)];
        assert_eq!(actual, expected);
    }
}
