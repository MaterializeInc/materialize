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
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Int64Array};
use bytes::Bytes;
use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use futures_util::stream::StreamExt;
use futures_util::{FutureExt, stream};
use mz_dyncfg::Config;
use mz_ore::cast::CastFrom;
use mz_ore::instrument;
use mz_persist::indexed::encoding::{BatchColumnarFormat, BlobTraceBatchPart, BlobTraceUpdates};
use mz_persist::location::Blob;
use mz_persist_types::arrow::{ArrayBound, ArrayOrd};
use mz_persist_types::columnar::{ColumnDecoder, Schema};
use mz_persist_types::parquet::{CompressionFormat, EncodingConfig};
use mz_persist_types::part::{Part, PartBuilder};
use mz_persist_types::schema::SchemaId;
use mz_persist_types::stats::{
    PartStats, TRUNCATE_LEN, TruncateBound, trim_to_budget, truncate_bytes,
};
use mz_persist_types::{Codec, Codec64};
use mz_proto::RustType;
use mz_timely_util::order::Reverse;
use proptest_derive::Arbitrary;
use semver::Version;
use timely::PartialOrder;
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tracing::{Instrument, debug_span, trace_span, warn};

use crate::async_runtime::IsolatedRuntime;
use crate::cfg::{BATCH_BUILDER_MAX_OUTSTANDING_PARTS, MiB};
use crate::error::InvalidUsage;
use crate::internal::compact::{CompactConfig, Compactor};
use crate::internal::encoding::{LazyInlineBatchPart, LazyPartStats, LazyProto, Schemas};
use crate::internal::machine::retry_external;
use crate::internal::merge::{MergeTree, Pending};
use crate::internal::metrics::{BatchWriteMetrics, Metrics, RetryMetrics, ShardMetrics};
use crate::internal::paths::{PartId, PartialBatchKey, WriterKey};
use crate::internal::state::{
    BatchPart, ENABLE_INCREMENTAL_COMPACTION, HollowBatch, HollowBatchPart, HollowRun,
    HollowRunRef, ProtoInlineBatchPart, RunId, RunMeta, RunOrder, RunPart,
};
use crate::stats::{STATS_BUDGET_BYTES, STATS_COLLECTION_ENABLED, untrimmable_columns};
use crate::{PersistConfig, ShardId};

include!(concat!(env!("OUT_DIR"), "/mz_persist_client.batch.rs"));

/// A handle to a batch of updates that has been written to blob storage but
/// which has not yet been appended to a shard.
///
/// A [Batch] needs to be marked as consumed or it needs to be deleted via [Self::delete].
/// Otherwise, a dangling batch will leak and backing blobs will remain in blob storage.
#[derive(Debug)]
pub struct Batch<K, V, T, D> {
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

impl<K, V, T, D> Drop for Batch<K, V, T, D> {
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
    D: Monoid + Codec64,
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
        if !self.batch_delete_enabled {
            self.mark_consumed();
            return;
        }
        let mut deletes = PartDeletes::default();
        for part in self.batch.parts.drain(..) {
            deletes.add(&part);
        }
        let () = deletes
            .delete(
                &*self.blob,
                self.shard_id(),
                usize::MAX,
                &*self.metrics,
                &*self.metrics.retries.external.batch_delete,
            )
            .await;
    }

    /// Returns the schemas of parts in this batch.
    pub fn schemas(&self) -> impl Iterator<Item = SchemaId> + '_ {
        self.batch.parts.iter().flat_map(|b| b.schema_id())
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

    pub(crate) async fn flush_to_blob(
        &mut self,
        cfg: &BatchBuilderConfig,
        batch_metrics: &BatchWriteMetrics,
        isolated_runtime: &Arc<IsolatedRuntime>,
        write_schemas: &Schemas<K, V>,
    ) {
        // It's necessary for correctness to keep the parts in the same order.
        // We could introduce concurrency here with FuturesOrdered, but it would
        // be pretty unexpected to have inline writes in more than one part, so
        // don't bother.
        let mut parts = Vec::new();
        for (run_meta, run_parts) in self.batch.runs() {
            for part in run_parts {
                let (updates, ts_rewrite, schema_id) = match part {
                    RunPart::Single(BatchPart::Inline {
                        updates,
                        ts_rewrite,
                        schema_id,
                        deprecated_schema_id: _,
                    }) => (updates, ts_rewrite, schema_id),
                    other @ RunPart::Many(_) | other @ RunPart::Single(BatchPart::Hollow(_)) => {
                        parts.push(other.clone());
                        continue;
                    }
                };
                let updates = updates
                    .decode::<T>(&self.metrics.columnar)
                    .expect("valid inline part");
                let diffs_sum =
                    diffs_sum::<D>(updates.updates.diffs()).expect("inline parts are not empty");
                let mut write_schemas = write_schemas.clone();
                write_schemas.id = *schema_id;

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
                        run_meta.order.unwrap_or(RunOrder::Unordered),
                        ts_rewrite.clone(),
                        D::encode(&diffs_sum),
                        write_schemas,
                    )
                    .instrument(write_span),
                );
                let part = handle.await.expect("part write task failed");
                parts.push(RunPart::Single(part));
            }
        }
        self.batch.parts = parts;
    }

    /// The sum of the encoded sizes of all parts in the batch.
    pub fn encoded_size_bytes(&self) -> usize {
        self.batch.encoded_size_bytes()
    }
}

impl<K, V, T, D> Batch<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + TotalOrder,
    D: Monoid + Codec64,
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
    /// the experience of database-issues#7825 has shaken our confidence in our own abilities
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
    pub(crate) inline_writes_single_max_bytes: usize,
    pub(crate) stats_collection_enabled: bool,
    pub(crate) stats_budget: usize,
    pub(crate) stats_untrimmable_columns: Arc<UntrimmableColumns>,
    pub(crate) encoding_config: EncodingConfig,
    pub(crate) preferred_order: RunOrder,
    pub(crate) structured_key_lower_len: usize,
    pub(crate) run_length_limit: usize,
    pub(crate) enable_incremental_compaction: bool,
    /// The number of runs to cap the built batch at, or None if we should
    /// continue to generate one run per part for unordered batches.
    /// See the config definition for details.
    pub(crate) max_runs: Option<usize>,
}

// TODO: Remove this once we're comfortable that there aren't any bugs.
pub(crate) const BATCH_DELETE_ENABLED: Config<bool> = Config::new(
    "persist_batch_delete_enabled",
    true,
    "Whether to actually delete blobs when batch delete is called (Materialize).",
);

pub(crate) const ENCODING_ENABLE_DICTIONARY: Config<bool> = Config::new(
    "persist_encoding_enable_dictionary",
    true,
    "A feature flag to enable dictionary encoding for Parquet data (Materialize).",
);

pub(crate) const ENCODING_COMPRESSION_FORMAT: Config<&'static str> = Config::new(
    "persist_encoding_compression_format",
    "none",
    "A feature flag to enable compression of Parquet data (Materialize).",
);

pub(crate) const STRUCTURED_KEY_LOWER_LEN: Config<usize> = Config::new(
    "persist_batch_structured_key_lower_len",
    256,
    "The maximum size in proto bytes of any structured key-lower metadata to preserve. \
    (If we're unable to fit the lower in budget, or the budget is zero, no metadata is kept.)",
);

pub(crate) const MAX_RUN_LEN: Config<usize> = Config::new(
    "persist_batch_max_run_len",
    usize::MAX,
    "The maximum length a run can have before it will be spilled as a hollow run \
    into the blob store.",
);

pub(crate) const MAX_RUNS: Config<usize> = Config::new(
    "persist_batch_max_runs",
    1,
    "The maximum number of runs a batch builder should generate for user batches. \
    (Compaction outputs always generate a single run.) \
    The minimum value is 2; below this, compaction is disabled.",
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
    4096,
    "The (exclusive) maximum size of a write that persist will inline in metadata.",
);

pub(crate) const INLINE_WRITES_TOTAL_MAX_BYTES: Config<usize> = Config::new(
    "persist_inline_writes_total_max_bytes",
    1 * MiB,
    "\
    The (exclusive) maximum total size of inline writes in metadata before \
    persist will backpressure them by flushing out to s3.",
);

impl BatchBuilderConfig {
    /// Initialize a batch builder config based on a snapshot of the Persist config.
    pub fn new(value: &PersistConfig, _shard_id: ShardId) -> Self {
        let writer_key = WriterKey::for_version(&value.build_version);

        let preferred_order = RunOrder::Structured;

        BatchBuilderConfig {
            writer_key,
            blob_target_size: BLOB_TARGET_SIZE.get(value).clamp(1, usize::MAX),
            batch_delete_enabled: BATCH_DELETE_ENABLED.get(value),
            batch_builder_max_outstanding_parts: BATCH_BUILDER_MAX_OUTSTANDING_PARTS.get(value),
            inline_writes_single_max_bytes: INLINE_WRITES_SINGLE_MAX_BYTES.get(value),
            stats_collection_enabled: STATS_COLLECTION_ENABLED.get(value),
            stats_budget: STATS_BUDGET_BYTES.get(value),
            stats_untrimmable_columns: Arc::new(untrimmable_columns(value)),
            encoding_config: EncodingConfig {
                use_dictionary: ENCODING_ENABLE_DICTIONARY.get(value),
                compression: CompressionFormat::from_str(&ENCODING_COMPRESSION_FORMAT.get(value)),
            },
            preferred_order,
            structured_key_lower_len: STRUCTURED_KEY_LOWER_LEN.get(value),
            run_length_limit: MAX_RUN_LEN.get(value).clamp(2, usize::MAX),
            max_runs: match MAX_RUNS.get(value) {
                limit @ 2.. => Some(limit),
                _ => None,
            },
            enable_incremental_compaction: ENABLE_INCREMENTAL_COMPACTION.get(value),
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
        // https://github.com/MaterializeInc/database-issues/issues/6421#issue-1863623805
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
    inline_desc: Description<T>,
    inclusive_upper: Antichain<Reverse<T>>,

    records_builder: PartBuilder<K, K::Schema, V, V::Schema>,
    pub(crate) builder: BatchBuilderInternal<K, V, T, D>,
}

impl<K, V, T, D> BatchBuilder<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Monoid + Codec64,
{
    pub(crate) fn new(
        builder: BatchBuilderInternal<K, V, T, D>,
        inline_desc: Description<T>,
    ) -> Self {
        let records_builder = PartBuilder::new(
            builder.write_schemas.key.as_ref(),
            builder.write_schemas.val.as_ref(),
        );
        Self {
            inline_desc,
            inclusive_upper: Antichain::new(),
            records_builder,
            builder,
        }
    }

    /// Finish writing this batch and return a handle to the written batch.
    ///
    /// This fails if any of the updates in this batch are beyond the given
    /// `upper`.
    pub async fn finish(
        mut self,
        registered_upper: Antichain<T>,
    ) -> Result<Batch<K, V, T, D>, InvalidUsage<T>> {
        if PartialOrder::less_than(&registered_upper, self.inline_desc.lower()) {
            return Err(InvalidUsage::InvalidBounds {
                lower: self.inline_desc.lower().clone(),
                upper: registered_upper,
            });
        }

        // When since is less than or equal to lower, the upper is a strict bound
        // on the updates' timestamp because no advancement has been performed. Because user batches
        // are always unadvanced, this ensures that new updates are recorded with valid timestamps.
        // Otherwise, we can make no assumptions about the timestamps
        if PartialOrder::less_equal(self.inline_desc.since(), self.inline_desc.lower()) {
            for ts in self.inclusive_upper.iter() {
                if registered_upper.less_equal(&ts.0) {
                    return Err(InvalidUsage::UpdateBeyondUpper {
                        ts: ts.0.clone(),
                        expected_upper: registered_upper.clone(),
                    });
                }
            }
        }

        let updates = self.records_builder.finish();
        self.builder
            .flush_part(self.inline_desc.clone(), updates)
            .await;

        self.builder
            .finish(Description::new(
                self.inline_desc.lower().clone(),
                registered_upper,
                self.inline_desc.since().clone(),
            ))
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
        if !self.inline_desc.lower().less_equal(ts) {
            return Err(InvalidUsage::UpdateNotBeyondLower {
                ts: ts.clone(),
                lower: self.inline_desc.lower().clone(),
            });
        }
        self.inclusive_upper.insert(Reverse(ts.clone()));

        let added = {
            self.records_builder
                .push(key, val, ts.clone(), diff.clone());
            if self.records_builder.goodbytes() >= self.builder.parts.cfg.blob_target_size {
                let part = self.records_builder.finish_and_replace(
                    self.builder.write_schemas.key.as_ref(),
                    self.builder.write_schemas.val.as_ref(),
                );
                Some(part)
            } else {
                None
            }
        };

        let added = if let Some(full_batch) = added {
            self.builder
                .flush_part(self.inline_desc.clone(), full_batch)
                .await;
            Added::RecordAndParts
        } else {
            Added::Record
        };
        Ok(added)
    }
}

#[derive(Debug)]
pub(crate) struct BatchBuilderInternal<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
{
    shard_id: ShardId,
    version: Version,
    blob: Arc<dyn Blob>,
    metrics: Arc<Metrics>,

    write_schemas: Schemas<K, V>,
    parts: BatchParts<T>,

    // These provide a bit more safety against appending a batch with the wrong
    // type to a shard.
    _phantom: PhantomData<fn(K, V, T, D)>,
}

impl<K, V, T, D> BatchBuilderInternal<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Monoid + Codec64,
{
    pub(crate) fn new(
        _cfg: BatchBuilderConfig,
        parts: BatchParts<T>,
        metrics: Arc<Metrics>,
        write_schemas: Schemas<K, V>,
        blob: Arc<dyn Blob>,
        shard_id: ShardId,
        version: Version,
    ) -> Self {
        Self {
            blob,
            metrics,
            write_schemas,
            parts,
            shard_id,
            version,
            _phantom: PhantomData,
        }
    }

    /// Finish writing this batch and return a handle to the written batch.
    ///
    /// This fails if any of the updates in this batch are beyond the given
    /// `upper`.
    #[instrument(level = "debug", name = "batch::finish", fields(shard = %self.shard_id))]
    pub async fn finish(
        self,
        registered_desc: Description<T>,
    ) -> Result<Batch<K, V, T, D>, InvalidUsage<T>> {
        let write_run_ids = self.parts.cfg.enable_incremental_compaction;
        let batch_delete_enabled = self.parts.cfg.batch_delete_enabled;
        let shard_metrics = Arc::clone(&self.parts.shard_metrics);
        let runs = self.parts.finish().await;

        let mut run_parts = vec![];
        let mut run_splits = vec![];
        let mut run_meta = vec![];
        let total_updates = runs
            .iter()
            .map(|(_, _, num_updates)| num_updates)
            .sum::<usize>();
        for (order, parts, num_updates) in runs {
            if parts.is_empty() {
                continue;
            }
            if run_parts.len() != 0 {
                run_splits.push(run_parts.len());
            }
            run_meta.push(RunMeta {
                order: Some(order),
                schema: self.write_schemas.id,
                // Field has been deprecated but kept around to roundtrip state.
                deprecated_schema: None,
                id: if write_run_ids {
                    Some(RunId::new())
                } else {
                    None
                },
                len: if write_run_ids {
                    Some(num_updates)
                } else {
                    None
                },
            });
            run_parts.extend(parts);
        }
        let desc = registered_desc;

        let batch = Batch::new(
            batch_delete_enabled,
            Arc::clone(&self.metrics),
            self.blob,
            shard_metrics,
            self.version,
            HollowBatch::new(desc, run_parts, total_updates, run_meta, run_splits),
        );

        Ok(batch)
    }

    /// Flushes the current part to Blob storage, first consolidating and then
    /// columnar encoding the updates. It is the caller's responsibility to
    /// chunk `current_part` to be no greater than
    /// [BatchBuilderConfig::blob_target_size], and must absolutely be less than
    /// [mz_persist::indexed::columnar::KEY_VAL_DATA_MAX_LEN]
    pub async fn flush_part(&mut self, part_desc: Description<T>, columnar: Part) {
        let num_updates = columnar.len();
        if num_updates == 0 {
            return;
        }
        let diffs_sum = diffs_sum::<D>(&columnar.diff).expect("part is non empty");

        let start = Instant::now();
        self.parts
            .write(&self.write_schemas, part_desc, columnar, diffs_sum)
            .await;
        self.metrics
            .compaction
            .batch
            .step_part_writing
            .inc_by(start.elapsed().as_secs_f64());
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RunWithMeta<T> {
    pub parts: Vec<RunPart<T>>,
    pub num_updates: usize,
}

impl<T> RunWithMeta<T> {
    pub fn new(parts: Vec<RunPart<T>>, num_updates: usize) -> Self {
        Self { parts, num_updates }
    }

    pub fn single(part: RunPart<T>, num_updates: usize) -> Self {
        Self {
            parts: vec![part],
            num_updates,
        }
    }
}

#[derive(Debug)]
enum WritingRuns<T> {
    /// Building a single run with the specified ordering. Parts are expected to be internally
    /// sorted and added in order. Merging a vec of parts will shift them out to a hollow run
    /// in blob, bounding the total length of a run in memory.
    Ordered(RunOrder, MergeTree<Pending<RunWithMeta<T>>>),
    /// Building multiple runs which may have different orders. Merging a vec of runs will cause
    /// them to be compacted together, bounding the total number of runs we generate.
    Compacting(MergeTree<(RunOrder, Pending<RunWithMeta<T>>)>),
}

// TODO: If this is dropped, cancel (and delete?) any writing parts and delete
// any finished ones.
#[derive(Debug)]
pub(crate) struct BatchParts<T> {
    cfg: BatchBuilderConfig,
    metrics: Arc<Metrics>,
    shard_metrics: Arc<ShardMetrics>,
    shard_id: ShardId,
    blob: Arc<dyn Blob>,
    isolated_runtime: Arc<IsolatedRuntime>,
    next_index: u64,
    writing_runs: WritingRuns<T>,
    batch_metrics: BatchWriteMetrics,
}

impl<T: Timestamp + Codec64> BatchParts<T> {
    pub(crate) fn new_compacting<K, V, D>(
        cfg: CompactConfig,
        desc: Description<T>,
        runs_per_compaction: usize,
        metrics: Arc<Metrics>,
        shard_metrics: Arc<ShardMetrics>,
        shard_id: ShardId,
        blob: Arc<dyn Blob>,
        isolated_runtime: Arc<IsolatedRuntime>,
        batch_metrics: &BatchWriteMetrics,
        schemas: Schemas<K, V>,
    ) -> Self
    where
        K: Codec + Debug,
        V: Codec + Debug,
        T: Lattice + Send + Sync,
        D: Monoid + Ord + Codec64 + Send + Sync,
    {
        let writing_runs = {
            let cfg = cfg.clone();
            let blob = Arc::clone(&blob);
            let metrics = Arc::clone(&metrics);
            let shard_metrics = Arc::clone(&shard_metrics);
            let isolated_runtime = Arc::clone(&isolated_runtime);
            // Clamping to prevent extreme values given weird configs.
            let runs_per_compaction = runs_per_compaction.clamp(2, 1024);

            let merge_fn = move |parts: Vec<(RunOrder, Pending<RunWithMeta<T>>)>| {
                let blob = Arc::clone(&blob);
                let metrics = Arc::clone(&metrics);
                let shard_metrics = Arc::clone(&shard_metrics);
                let cfg = cfg.clone();
                let isolated_runtime = Arc::clone(&isolated_runtime);
                let write_schemas = schemas.clone();
                let compact_desc = desc.clone();
                let handle = mz_ore::task::spawn(
                    || "batch::compact_runs",
                    async move {
                        let runs: Vec<_> = stream::iter(parts)
                            .then(|(order, parts)| async move {
                                let completed_run = parts.into_result().await;
                                (
                                    RunMeta {
                                        order: Some(order),
                                        schema: schemas.id,
                                        // Field has been deprecated but kept around to
                                        // roundtrip state.
                                        deprecated_schema: None,
                                        id: if cfg.batch.enable_incremental_compaction {
                                            Some(RunId::new())
                                        } else {
                                            None
                                        },
                                        len: if cfg.batch.enable_incremental_compaction {
                                            Some(completed_run.num_updates)
                                        } else {
                                            None
                                        },
                                    },
                                    completed_run.parts,
                                )
                            })
                            .collect()
                            .await;

                        let run_refs: Vec<_> = runs
                            .iter()
                            .map(|(meta, run)| (&compact_desc, meta, run.as_slice()))
                            .collect();

                        let output_batch = Compactor::<K, V, T, D>::compact_runs(
                            &cfg,
                            &shard_id,
                            &compact_desc,
                            run_refs,
                            blob,
                            metrics,
                            shard_metrics,
                            isolated_runtime,
                            write_schemas,
                        )
                        .await
                        .expect("successful compaction");

                        assert_eq!(
                            output_batch.run_meta.len(),
                            1,
                            "compaction is guaranteed to emit a single run"
                        );
                        let total_compacted_updates: usize = output_batch.len;

                        RunWithMeta::new(output_batch.parts, total_compacted_updates)
                    }
                    .instrument(debug_span!("batch::compact_runs")),
                );
                (RunOrder::Structured, Pending::new(handle))
            };
            WritingRuns::Compacting(MergeTree::new(runs_per_compaction, merge_fn))
        };
        BatchParts {
            cfg: cfg.batch,
            metrics,
            shard_metrics,
            shard_id,
            blob,
            isolated_runtime,
            next_index: 0,
            writing_runs,
            batch_metrics: batch_metrics.clone(),
        }
    }

    pub(crate) fn new_ordered<D: Monoid + Codec64>(
        cfg: BatchBuilderConfig,
        order: RunOrder,
        metrics: Arc<Metrics>,
        shard_metrics: Arc<ShardMetrics>,
        shard_id: ShardId,
        blob: Arc<dyn Blob>,
        isolated_runtime: Arc<IsolatedRuntime>,
        batch_metrics: &BatchWriteMetrics,
    ) -> Self {
        let writing_runs = {
            let cfg = cfg.clone();
            let blob = Arc::clone(&blob);
            let metrics = Arc::clone(&metrics);
            let writer_key = cfg.writer_key.clone();
            // Don't spill "unordered" runs to S3, since we'll split them up into many single-element
            // runs below.
            let run_length_limit = (order == RunOrder::Unordered)
                .then_some(usize::MAX)
                .unwrap_or(cfg.run_length_limit);
            let merge_fn = move |parts: Vec<Pending<RunWithMeta<T>>>| {
                let blob = Arc::clone(&blob);
                let writer_key = writer_key.clone();
                let metrics = Arc::clone(&metrics);
                let handle = mz_ore::task::spawn(
                    || "batch::spill_run",
                    async move {
                        let completed_runs: Vec<RunWithMeta<T>> = stream::iter(parts)
                            .then(|p| p.into_result())
                            .collect()
                            .await;

                        let mut all_run_parts = Vec::new();
                        let mut total_updates = 0;

                        for completed_run in completed_runs {
                            all_run_parts.extend(completed_run.parts);
                            total_updates += completed_run.num_updates;
                        }

                        let run_ref = HollowRunRef::set::<D>(
                            shard_id,
                            blob.as_ref(),
                            &writer_key,
                            HollowRun {
                                parts: all_run_parts,
                            },
                            &*metrics,
                        )
                        .await;

                        RunWithMeta::single(RunPart::Many(run_ref), total_updates)
                    }
                    .instrument(debug_span!("batch::spill_run")),
                );
                Pending::new(handle)
            };
            WritingRuns::Ordered(order, MergeTree::new(run_length_limit, merge_fn))
        };
        BatchParts {
            cfg,
            metrics,
            shard_metrics,
            shard_id,
            blob,
            isolated_runtime,
            next_index: 0,
            writing_runs,
            batch_metrics: batch_metrics.clone(),
        }
    }

    pub(crate) fn expected_order(&self) -> RunOrder {
        match self.writing_runs {
            WritingRuns::Ordered(order, _) => order,
            WritingRuns::Compacting(_) => RunOrder::Unordered,
        }
    }

    pub(crate) async fn write<K: Codec, V: Codec, D: Codec64>(
        &mut self,
        write_schemas: &Schemas<K, V>,
        desc: Description<T>,
        updates: Part,
        diffs_sum: D,
    ) {
        let batch_metrics = self.batch_metrics.clone();
        let index = self.next_index;
        self.next_index += 1;
        let num_updates = updates.len();
        let ts_rewrite = None;
        let schema_id = write_schemas.id;

        // If we're going to encode structured data then halve our limit since we're storing
        // it twice, once as binary encoded and once as structured.
        let inline_threshold = self.cfg.inline_writes_single_max_bytes;

        let updates = BlobTraceUpdates::from_part(updates);
        let (name, write_future) = if updates.goodbytes() < inline_threshold {
            let span = debug_span!("batch::inline_part", shard = %self.shard_id).or_current();
            (
                "batch::inline_part",
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

                    RunWithMeta::single(
                        RunPart::Single(BatchPart::Inline {
                            updates,
                            ts_rewrite,
                            schema_id,
                            // Field has been deprecated but kept around to roundtrip state.
                            deprecated_schema_id: None,
                        }),
                        num_updates,
                    )
                }
                .instrument(span)
                .boxed(),
            )
        } else {
            let part = BlobTraceBatchPart {
                desc,
                updates,
                index,
            };
            let cfg = self.cfg.clone();
            let blob = Arc::clone(&self.blob);
            let metrics = Arc::clone(&self.metrics);
            let shard_metrics = Arc::clone(&self.shard_metrics);
            let isolated_runtime = Arc::clone(&self.isolated_runtime);
            let expected_order = self.expected_order();
            let encoded_diffs_sum = D::encode(&diffs_sum);
            let write_schemas_clone = write_schemas.clone();
            let write_span =
                debug_span!("batch::write_part", shard = %self.shard_metrics.shard_id).or_current();
            (
                "batch::write_part",
                async move {
                    let part = BatchParts::write_hollow_part(
                        cfg,
                        blob,
                        metrics,
                        shard_metrics,
                        batch_metrics,
                        isolated_runtime,
                        part,
                        expected_order,
                        ts_rewrite,
                        encoded_diffs_sum,
                        write_schemas_clone,
                    )
                    .await;
                    RunWithMeta::single(RunPart::Single(part), num_updates)
                }
                .instrument(write_span)
                .boxed(),
            )
        };

        match &mut self.writing_runs {
            WritingRuns::Ordered(_order, run) => {
                let part = Pending::new(mz_ore::task::spawn(|| name, write_future));
                run.push(part);

                // If there are more than the max outstanding parts, block on all but the
                //  most recent.
                for part in run
                    .iter_mut()
                    .rev()
                    .skip(self.cfg.batch_builder_max_outstanding_parts)
                    .take_while(|p| !p.is_finished())
                {
                    self.batch_metrics.write_stalls.inc();
                    part.block_until_ready().await;
                }
            }
            WritingRuns::Compacting(batches) => {
                let run = Pending::Writing(mz_ore::task::spawn(|| name, write_future));
                batches.push((RunOrder::Unordered, run));

                // Allow up to `max_outstanding_parts` (or one compaction) to be pending, and block
                // on the rest.
                let mut part_budget = self.cfg.batch_builder_max_outstanding_parts;
                let mut compaction_budget = 1;
                for (_, part) in batches
                    .iter_mut()
                    .rev()
                    .skip_while(|(order, _)| match order {
                        RunOrder::Unordered if part_budget > 0 => {
                            part_budget -= 1;
                            true
                        }
                        RunOrder::Structured | RunOrder::Codec if compaction_budget > 0 => {
                            compaction_budget -= 1;
                            true
                        }
                        _ => false,
                    })
                    .take_while(|(_, p)| !p.is_finished())
                {
                    self.batch_metrics.write_stalls.inc();
                    part.block_until_ready().await;
                }
            }
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
        run_order: RunOrder,
        ts_rewrite: Option<Antichain<T>>,
        diffs_sum: [u8; 8],
        write_schemas: Schemas<K, V>,
    ) -> BatchPart<T> {
        let partial_key = PartialBatchKey::new(&cfg.writer_key, &PartId::new());
        let key = partial_key.complete(&shard_metrics.shard_id);
        let goodbytes = updates.updates.goodbytes();
        let metrics_ = Arc::clone(&metrics);
        let schema_id = write_schemas.id;

        let (stats, key_lower, structured_key_lower, (buf, encode_time)) = isolated_runtime
            .spawn_named(|| "batch::encode_part", async move {
                // Measure the expensive steps of the part build - re-encoding and stats collection.
                let stats = metrics_.columnar.arrow().measure_part_build(|| {
                    let stats = if cfg.stats_collection_enabled {
                        let ext = updates.updates.get_or_make_structured::<K, V>(
                            write_schemas.key.as_ref(),
                            write_schemas.val.as_ref(),
                        );

                        let key_stats = write_schemas
                            .key
                            .decoder_any(ext.key.as_ref())
                            .expect("decoding just-encoded data")
                            .stats();

                        let part_stats = PartStats { key: key_stats };

                        // Collect stats about the updates, if stats collection is enabled.
                        let trimmed_start = Instant::now();
                        let mut trimmed_bytes = 0;
                        let trimmed_stats = LazyPartStats::encode(&part_stats, |s| {
                            trimmed_bytes = trim_to_budget(s, cfg.stats_budget, |s| {
                                cfg.stats_untrimmable_columns.should_retain(s)
                            })
                        });
                        let trimmed_duration = trimmed_start.elapsed();
                        Some((trimmed_stats, trimmed_duration, trimmed_bytes))
                    } else {
                        None
                    };

                    // Ensure the updates are in the specified columnar format before encoding.
                    updates.updates = updates.updates.as_structured::<K, V>(
                        write_schemas.key.as_ref(),
                        write_schemas.val.as_ref(),
                    );

                    stats
                });

                let key_lower = if let Some(records) = updates.updates.records() {
                    let key_bytes = records.keys();
                    if key_bytes.is_empty() {
                        &[]
                    } else if run_order == RunOrder::Codec {
                        key_bytes.value(0)
                    } else {
                        ::arrow::compute::min_binary(key_bytes).expect("min of nonempty array")
                    }
                } else {
                    &[]
                };
                let key_lower = truncate_bytes(key_lower, TRUNCATE_LEN, TruncateBound::Lower)
                    .expect("lower bound always exists");

                let structured_key_lower = if cfg.structured_key_lower_len > 0 {
                    updates.updates.structured().and_then(|ext| {
                        let min_key = if run_order == RunOrder::Structured {
                            0
                        } else {
                            let ord = ArrayOrd::new(ext.key.as_ref());
                            (0..ext.key.len())
                                .min_by_key(|i| ord.at(*i))
                                .expect("non-empty batch")
                        };
                        let lower = ArrayBound::new(Arc::clone(&ext.key), min_key)
                            .to_proto_lower(cfg.structured_key_lower_len);
                        if lower.is_none() {
                            batch_metrics.key_lower_too_big.inc()
                        }
                        lower.map(|proto| LazyProto::from(&proto))
                    })
                } else {
                    None
                };

                let encode_start = Instant::now();
                let mut buf = Vec::new();
                updates.encode(&mut buf, &metrics_.columnar, &cfg.encoding_config);

                // Drop batch as soon as we can to reclaim its memory.
                drop(updates);
                (
                    stats,
                    key_lower,
                    structured_key_lower,
                    (Bytes::from(buf), encode_start.elapsed()),
                )
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
        match run_order {
            RunOrder::Unordered => batch_metrics.unordered.inc(),
            RunOrder::Codec => batch_metrics.codec_order.inc(),
            RunOrder::Structured => batch_metrics.structured_order.inc(),
        }
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

        BatchPart::Hollow(HollowBatchPart {
            key: partial_key,
            encoded_size_bytes: payload_len,
            key_lower,
            structured_key_lower,
            stats,
            ts_rewrite,
            diffs_sum: Some(diffs_sum),
            format: Some(BatchColumnarFormat::Structured),
            schema_id,
            // Field has been deprecated but kept around to roundtrip state.
            deprecated_schema_id: None,
        })
    }

    #[instrument(level = "debug", name = "batch::finish_upload", fields(shard = %self.shard_id))]
    pub(crate) async fn finish(self) -> Vec<(RunOrder, Vec<RunPart<T>>, usize)> {
        match self.writing_runs {
            WritingRuns::Ordered(RunOrder::Unordered, run) => {
                let completed_runs = run.finish();
                let mut output = Vec::with_capacity(completed_runs.len());
                for completed_run in completed_runs {
                    let completed_run = completed_run.into_result().await;
                    // Each part becomes its own run for unordered case
                    for part in completed_run.parts {
                        output.push((RunOrder::Unordered, vec![part], completed_run.num_updates));
                    }
                }
                output
            }
            WritingRuns::Ordered(order, run) => {
                let completed_runs = run.finish();
                let mut all_parts = Vec::new();
                let mut all_update_counts = 0;
                for completed_run in completed_runs {
                    let completed_run = completed_run.into_result().await;
                    all_parts.extend(completed_run.parts);
                    all_update_counts += completed_run.num_updates;
                }
                vec![(order, all_parts, all_update_counts)]
            }
            WritingRuns::Compacting(batches) => {
                let runs = batches.finish();
                let mut output = Vec::new();
                for (order, run) in runs {
                    let completed_run = run.into_result().await;
                    output.push((order, completed_run.parts, completed_run.num_updates));
                }
                output
            }
        }
    }
}

pub(crate) fn validate_truncate_batch<T: Timestamp>(
    batch: &HollowBatch<T>,
    truncate: &Description<T>,
    any_batch_rewrite: bool,
    validate_part_bounds_on_write: bool,
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
            let part_lower_bound = part.ts_rewrite().unwrap_or_else(|| batch.desc.lower());
            if !PartialOrder::less_equal(truncate.lower(), part_lower_bound) {
                return Err(InvalidUsage::InvalidRewrite(format!(
                    "rewritten batch might have data below {:?} at {:?}",
                    truncate.lower().elements(),
                    part_lower_bound.elements(),
                )));
            }
        }
    }

    if !validate_part_bounds_on_write {
        return Ok(());
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

#[derive(Debug)]
pub(crate) struct PartDeletes<T> {
    /// Keys to hollow parts or runs that we're ready to delete.
    blob_keys: BTreeSet<PartialBatchKey>,
    /// Keys to hollow runs that may not have had all their parts deleted (or added to blob_keys) yet.
    hollow_runs: BTreeMap<PartialBatchKey, HollowRunRef<T>>,
}

impl<T> Default for PartDeletes<T> {
    fn default() -> Self {
        Self {
            blob_keys: Default::default(),
            hollow_runs: Default::default(),
        }
    }
}

impl<T: Timestamp> PartDeletes<T> {
    // Adds the part to the set to be deleted and returns true if it was newly
    // inserted.
    pub fn add(&mut self, part: &RunPart<T>) -> bool {
        match part {
            RunPart::Many(r) => self.hollow_runs.insert(r.key.clone(), r.clone()).is_none(),
            RunPart::Single(BatchPart::Hollow(x)) => self.blob_keys.insert(x.key.clone()),
            RunPart::Single(BatchPart::Inline { .. }) => {
                // Nothing to delete.
                true
            }
        }
    }

    pub fn contains(&self, part: &RunPart<T>) -> bool {
        match part {
            RunPart::Many(r) => self.hollow_runs.contains_key(&r.key),
            RunPart::Single(BatchPart::Hollow(x)) => self.blob_keys.contains(&x.key),
            RunPart::Single(BatchPart::Inline { .. }) => false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match self {
            Self {
                blob_keys,
                hollow_runs,
            } => blob_keys.len() + hollow_runs.len(),
        }
    }

    pub async fn delete(
        mut self,
        blob: &dyn Blob,
        shard_id: ShardId,
        concurrency: usize,
        metrics: &Metrics,
        delete_metrics: &RetryMetrics,
    ) where
        T: Codec64,
    {
        loop {
            let () = stream::iter(mem::take(&mut self.blob_keys))
                .map(|key| {
                    let key = key.complete(&shard_id);
                    async move {
                        retry_external(delete_metrics, || blob.delete(&key)).await;
                    }
                })
                .buffer_unordered(concurrency)
                .collect()
                .await;

            let Some((run_key, run_ref)) = self.hollow_runs.pop_first() else {
                break;
            };

            if let Some(run) = run_ref.get(shard_id, blob, metrics).await {
                // Queue up both all the individual parts and the run itself for deletion.
                for part in &run.parts {
                    self.add(part);
                }
                self.blob_keys.insert(run_key);
            };
        }
    }
}

/// Returns the total sum of diffs or None if there were no updates.
fn diffs_sum<D: Monoid + Codec64>(updates: &Int64Array) -> Option<D> {
    let mut sum = None;
    for d in updates.values().iter() {
        let d = D::decode(d.to_le_bytes());
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

    use super::*;
    use crate::PersistLocation;
    use crate::cache::PersistClientCache;
    use crate::cfg::BATCH_BUILDER_MAX_OUTSTANDING_PARTS;
    use crate::internal::paths::{BlobKey, PartialBlobKey};
    use crate::tests::{all_ok, new_test_client};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn batch_builder_flushing() {
        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
            (("4".to_owned(), "four".to_owned()), 4, 1),
        ];

        let cache = PersistClientCache::new_no_metrics();

        // Set blob_target_size to 0 so that each row gets forced into its own
        // batch. Set max_outstanding to a small value that's >1 to test various
        // edge cases below.
        cache.cfg.set_config(&BLOB_TARGET_SIZE, 0);
        cache.cfg.set_config(&MAX_RUNS, 3);
        cache
            .cfg
            .set_config(&BATCH_BUILDER_MAX_OUTSTANDING_PARTS, 2);

        let client = cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("client construction failed");
        let (mut write, mut read) = client
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;

        // A new builder has no writing or finished parts.
        let mut builder = write.builder(Antichain::from_elem(0));

        fn assert_writing(
            builder: &BatchBuilder<String, String, u64, i64>,
            expected_finished: &[bool],
        ) {
            let WritingRuns::Compacting(run) = &builder.builder.parts.writing_runs else {
                unreachable!("ordered run!")
            };

            let actual: Vec<_> = run.iter().map(|(_, p)| p.is_finished()).collect();
            assert_eq!(*expected_finished, actual);
        }

        assert_writing(&builder, &[]);

        // We set blob_target_size to 0, so the first update gets forced out
        // into a run.
        let ((k, v), t, d) = &data[0];
        builder.add(k, v, t, d).await.expect("invalid usage");
        assert_writing(&builder, &[false]);

        // We set batch_builder_max_outstanding_parts to 2, so we are allowed to
        // pipeline a second part.
        let ((k, v), t, d) = &data[1];
        builder.add(k, v, t, d).await.expect("invalid usage");
        assert_writing(&builder, &[false, false]);

        // But now that we have 3 parts, the add call back-pressures until the
        // first one finishes.
        let ((k, v), t, d) = &data[2];
        builder.add(k, v, t, d).await.expect("invalid usage");
        assert_writing(&builder, &[true, false, false]);

        // Finally, pushing a fourth part will cause the first three to spill out into
        // a new compacted run.
        let ((k, v), t, d) = &data[3];
        builder.add(k, v, t, d).await.expect("invalid usage");
        assert_writing(&builder, &[false, false]);

        // Finish off the batch and verify that the keys and such get plumbed
        // correctly by reading the data back.
        let batch = builder
            .finish(Antichain::from_elem(5))
            .await
            .expect("invalid usage");
        assert_eq!(batch.batch.runs().count(), 2);
        assert_eq!(batch.batch.part_count(), 4);
        write
            .append_batch(batch, Antichain::from_elem(0), Antichain::from_elem(5))
            .await
            .expect("invalid usage")
            .expect("unexpected upper");
        assert_eq!(read.expect_snapshot_and_fetch(4).await, all_ok(&data, 4));
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn batch_builder_keys() {
        let cache = PersistClientCache::new_no_metrics();
        // Set blob_target_size to 0 so that each row gets forced into its own batch part
        cache.cfg.set_config(&BLOB_TARGET_SIZE, 0);
        // Otherwise fails: expected hollow part!
        cache.cfg.set_config(&STRUCTURED_KEY_LOWER_LEN, 0);
        cache.cfg.set_config(&INLINE_WRITES_SINGLE_MAX_BYTES, 0);
        cache.cfg.set_config(&INLINE_WRITES_TOTAL_MAX_BYTES, 0);
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
            let part = part.expect_hollow_part();
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
    async fn batch_delete() {
        let cache = PersistClientCache::new_no_metrics();
        cache.cfg.set_config(&INLINE_WRITES_SINGLE_MAX_BYTES, 0);
        cache.cfg.set_config(&INLINE_WRITES_TOTAL_MAX_BYTES, 0);
        cache.cfg.set_config(&BATCH_DELETE_ENABLED, true);
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

        assert_eq!(batch.batch.part_count(), 1);
        let part_key = batch.batch.parts[0]
            .expect_hollow_part()
            .key
            .complete(&shard_id);

        let part_bytes = client.blob.get(&part_key).await.expect("invalid usage");
        assert!(part_bytes.is_some());

        batch.delete().await;

        let part_bytes = client.blob.get(&part_key).await.expect("invalid usage");
        assert!(part_bytes.is_none());
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

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn structured_lowers() {
        let cache = PersistClientCache::new_no_metrics();
        // Ensure structured data is calculated, and that we give some budget for a key lower.
        cache.cfg().set_config(&STRUCTURED_KEY_LOWER_LEN, 1024);
        // Otherwise fails: expected hollow part!
        cache.cfg().set_config(&INLINE_WRITES_SINGLE_MAX_BYTES, 0);
        cache.cfg().set_config(&INLINE_WRITES_TOTAL_MAX_BYTES, 0);
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

        assert_eq!(batch.batch.part_count(), 1);
        let [part] = batch.batch.parts.as_slice() else {
            panic!("expected single part")
        };
        // Verifies that the structured key lower is stored and decoded.
        assert!(part.structured_key_lower().is_some());
    }
}
