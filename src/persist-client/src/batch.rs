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
use differential_dataflow::Hashable;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

use bytes::Bytes;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use futures_util::stream::StreamExt;
use futures_util::{stream, FutureExt};
use mz_dyncfg::Config;
use mz_ore::cast::CastFrom;
use mz_ore::{instrument, soft_panic_or_log};
use mz_persist::indexed::columnar::{ColumnarRecords, ColumnarRecordsBuilder};
use mz_persist::indexed::encoding::{BatchColumnarFormat, BlobTraceBatchPart, BlobTraceUpdates};
use mz_persist::location::Blob;
use mz_persist_types::arrow::{ArrayBound, ArrayOrd};
use mz_persist_types::parquet::{CompressionFormat, EncodingConfig};
use mz_persist_types::schema::SchemaId;
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
use crate::cfg::{MiB, BATCH_BUILDER_MAX_OUTSTANDING_PARTS};
use crate::error::InvalidUsage;
use crate::internal::compact::{CompactConfig, Compactor};
use crate::internal::encoding::{LazyInlineBatchPart, LazyPartStats, LazyProto, Schemas};
use crate::internal::machine::retry_external;
use crate::internal::merge::{MergeTree, Pending};
use crate::internal::metrics::{BatchWriteMetrics, Metrics, RetryMetrics, ShardMetrics};
use crate::internal::paths::{PartId, PartialBatchKey, WriterKey};
use crate::internal::state::{
    BatchPart, HollowBatch, HollowBatchPart, HollowRun, HollowRunRef, ProtoInlineBatchPart,
    RunMeta, RunOrder, RunPart, WRITE_DIFFS_SUM,
};
use crate::stats::{
    encode_updates, untrimmable_columns, STATS_BUDGET_BYTES, STATS_COLLECTION_ENABLED,
};
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
                    }) => (updates, ts_rewrite, schema_id),
                    other @ RunPart::Many(_) | other @ RunPart::Single(BatchPart::Hollow(_)) => {
                        parts.push(other.clone());
                        continue;
                    }
                };
                let updates = updates
                    .decode::<T>(&self.metrics.columnar)
                    .expect("valid inline part");
                let key_lower = updates.key_lower().to_vec();
                let diffs_sum =
                    diffs_sum::<D>(updates.updates.records()).expect("inline parts are not empty");
                let mut write_schemas = write_schemas.clone();
                write_schemas.id = *schema_id;

                let write_span =
                    debug_span!("batch::flush_to_blob", shard = %self.shard_metrics.shard_id)
                        .or_current();
                let handle = mz_ore::task::spawn(
                    || "batch::flush_to_blob",
                    BatchParts::write_hollow_part(
                        cfg.clone(),
                        cfg.part_write_columnar_data(),
                        Arc::clone(&self.blob),
                        Arc::clone(&self.metrics),
                        Arc::clone(&self.shard_metrics),
                        batch_metrics.clone(),
                        Arc::clone(isolated_runtime),
                        updates,
                        run_meta.order.unwrap_or(RunOrder::Unordered),
                        key_lower,
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
    pub(crate) batch_columnar_format: BatchColumnarFormat,
    pub(crate) batch_columnar_format_percent: usize,
    pub(crate) inline_writes_single_max_bytes: usize,
    pub(crate) stats_collection_enabled: bool,
    pub(crate) stats_budget: usize,
    pub(crate) stats_untrimmable_columns: Arc<UntrimmableColumns>,
    pub(crate) write_diffs_sum: bool,
    pub(crate) encoding_config: EncodingConfig,
    pub(crate) preferred_order: RunOrder,
    pub(crate) structured_key_lower_len: usize,
    pub(crate) run_length_limit: usize,
    /// The number of runs to cap the built batch at, or None if we should
    /// continue to generate one run per part for unordered batches.
    /// See the config definition for details.
    pub(crate) max_runs: Option<usize>,
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

pub(crate) const BATCH_COLUMNAR_FORMAT_PERCENT: Config<usize> = Config::new(
    "persist_batch_columnar_format_percent",
    0,
    "Percent of parts to write using 'persist_batch_columnar_format', falling back to 'row'.",
);

pub(crate) const ENCODING_ENABLE_DICTIONARY: Config<bool> = Config::new(
    "persist_encoding_enable_dictionary",
    false,
    "A feature flag to enable dictionary encoding for Parquet data (Materialize).",
);

pub(crate) const ENCODING_COMPRESSION_FORMAT: Config<&'static str> = Config::new(
    "persist_encoding_compression_format",
    "none",
    "A feature flag to enable compression of Parquet data (Materialize).",
);

pub(crate) const STRUCTURED_ORDER: Config<bool> = Config::new(
    "persist_batch_structured_order",
    false,
    "If enabled, output compaction batches in structured-data order.",
);

pub(crate) const STRUCTURED_ORDER_UNTIL_SHARD: Config<&'static str> = Config::new(
    "persist_batch_structured_order_from_shard",
    "sz",
    "Restrict shards using structured ordering to those shards with formatted ids less than \
    the given string. (For example, `s0` will disable it for all shards, `s8` will enable it for \
    half of all shards, `s8888` will enable it for slightly more shards, and `sz` will enable it \
    for everyone.)",
);

pub(crate) const STRUCTURED_KEY_LOWER_LEN: Config<usize> = Config::new(
    "persist_batch_structured_key_lower_len",
    0,
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
    pub fn new(value: &PersistConfig, shard_id: ShardId) -> Self {
        let writer_key = WriterKey::for_version(&value.build_version);

        let batch_columnar_format =
            BatchColumnarFormat::from_str(&BATCH_COLUMNAR_FORMAT.get(value));
        let batch_columnar_format_percent = BATCH_COLUMNAR_FORMAT_PERCENT.get(value);

        let structured_order = STRUCTURED_ORDER.get(value) && {
            shard_id.to_string() < STRUCTURED_ORDER_UNTIL_SHARD.get(value)
        };
        let preferred_order = if structured_order {
            RunOrder::Structured
        } else {
            RunOrder::Codec
        };

        BatchBuilderConfig {
            writer_key,
            blob_target_size: BLOB_TARGET_SIZE.get(value).clamp(1, usize::MAX),
            batch_delete_enabled: BATCH_DELETE_ENABLED.get(value),
            batch_builder_max_outstanding_parts: BATCH_BUILDER_MAX_OUTSTANDING_PARTS.get(value),
            batch_columnar_format,
            batch_columnar_format_percent,
            inline_writes_single_max_bytes: INLINE_WRITES_SINGLE_MAX_BYTES.get(value),
            stats_collection_enabled: STATS_COLLECTION_ENABLED.get(value),
            stats_budget: STATS_BUDGET_BYTES.get(value),
            stats_untrimmable_columns: Arc::new(untrimmable_columns(value)),
            write_diffs_sum: WRITE_DIFFS_SUM.get(value),
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
        }
    }

    fn part_write_columnar_data(&self) -> bool {
        // [0, 100)
        let rand = || usize::cast_from(Uuid::new_v4().hashed()) % 100;
        self.batch_columnar_format.is_structured() && self.batch_columnar_format_percent > rand()
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
        self.builder.finish(registered_upper).await
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
        validate_schema(
            &self.builder.write_schemas,
            &self.key_buf,
            &self.val_buf,
            Some(key),
            Some(val),
        );
        let result = self
            .builder
            .add(&self.key_buf, &self.val_buf, ts, diff)
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

    write_schemas: Schemas<K, V>,
    buffer: BatchBuffer,

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
        parts: BatchParts<T>,
        metrics: Arc<Metrics>,
        write_schemas: Schemas<K, V>,
        lower: Antichain<T>,
        blob: Arc<dyn Blob>,
        shard_id: ShardId,
        version: Version,
        since: Antichain<T>,
        inline_upper: Option<Antichain<T>>,
    ) -> Self {
        Self {
            lower,
            inclusive_upper: Antichain::new(),
            blob,
            buffer: BatchBuffer::new(Arc::clone(&metrics), cfg.blob_target_size),
            metrics,
            write_schemas,
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
        self.flush_part(remainder).await;

        let batch_delete_enabled = self.parts.cfg.batch_delete_enabled;
        let shard_metrics = Arc::clone(&self.parts.shard_metrics);
        let runs = self.parts.finish().await;

        let mut run_parts = vec![];
        let mut run_splits = vec![];
        let mut run_meta = vec![];
        for (order, parts) in runs {
            if parts.is_empty() {
                continue;
            }
            if run_parts.len() != 0 {
                run_splits.push(run_parts.len());
            }
            run_meta.push(RunMeta {
                order: Some(order),
                schema: self.write_schemas.id,
            });
            run_parts.extend(parts);
        }
        let desc = Description::new(self.lower, registered_upper, self.since);

        let batch = Batch::new(
            batch_delete_enabled,
            Arc::clone(&self.metrics),
            self.blob,
            shard_metrics,
            self.version,
            HollowBatch::new(desc, run_parts, self.num_updates, run_meta, run_splits),
        );

        Ok(batch)
    }

    /// Adds the given update to the batch.
    ///
    /// The update timestamp must be greater or equal to `lower` that was given
    /// when creating this [BatchBuilder].
    pub async fn add(
        &mut self,
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
                self.flush_part(part_to_flush).await;
                Ok(Added::RecordAndParts)
            }
            None => Ok(Added::Record),
        }
    }

    /// Adds a batch of updates all at once. The caller takes responsibility for ensuring
    /// that the data is appropriately chunked.
    ///
    /// The update timestamps must be greater or equal to `lower` that was given
    /// when creating this [BatchBuilder].
    pub async fn flush_many(&mut self, updates: BlobTraceUpdates) -> Result<(), InvalidUsage<T>> {
        for ts in updates.records().timestamps().iter().flatten() {
            let ts = T::decode(ts.to_le_bytes());
            if !self.lower.less_equal(&ts) {
                return Err(InvalidUsage::UpdateNotBeyondLower {
                    ts,
                    lower: self.lower.clone(),
                });
            }

            self.inclusive_upper.insert(Reverse(ts));
        }

        // If there have been any individual updates added, flush those first.
        // This is a noop if there are no such updates... and at time of writing there
        // never are, since no callers mix these two methods.
        // TODO: consider moving the individual updates to BatchBuilder so we can
        // avoid handling this case.
        let previous = self.buffer.drain();
        self.flush_part(previous).await;

        self.flush_part(updates).await;

        Ok(())
    }

    /// Flushes the current part to Blob storage, first consolidating and then
    /// columnar encoding the updates. It is the caller's responsibility to
    /// chunk `current_part` to be no greater than
    /// [BatchBuilderConfig::blob_target_size], and must absolutely be less than
    /// [mz_persist::indexed::columnar::KEY_VAL_DATA_MAX_LEN]
    async fn flush_part(&mut self, columnar: BlobTraceUpdates) {
        let key_lower = {
            let key_bytes = columnar.records().keys();
            if key_bytes.is_empty() {
                &[]
            } else if self.parts.expected_order() == RunOrder::Codec {
                key_bytes.value(0)
            } else {
                ::arrow::compute::min_binary(key_bytes).expect("min of nonempty array")
            }
        };
        let key_lower = truncate_bytes(key_lower, TRUNCATE_LEN, TruncateBound::Lower)
            .expect("lower bound always exists");

        let num_updates = columnar.len();
        if num_updates == 0 {
            return;
        }
        let diffs_sum = diffs_sum::<D>(columnar.records()).expect("part is non empty");

        let start = Instant::now();
        self.parts
            .write(
                &self.write_schemas,
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

        self.num_updates += num_updates;
    }
}

// Ideally this would be done inside `BatchBuilderInternal::add`, but that seems
// to require plumbing around `Sync` bounds for `K` and `V`, so instead just
// inline it at the two callers.
pub(crate) fn validate_schema<K: Codec, V: Codec>(
    stats_schemas: &Schemas<K, V>,
    key: &[u8],
    val: &[u8],
    decoded_key: Option<&K>,
    decoded_val: Option<&V>,
) {
    // Attempt to catch any bad schema usage in CI. This is probably too
    // expensive to run in prod.
    if !mz_ore::assert::SOFT_ASSERTIONS.load(Ordering::Relaxed) {
        return;
    }
    let key_valid = match decoded_key {
        Some(key) => K::validate(key, &stats_schemas.key),
        None => {
            let key = K::decode(key, &stats_schemas.key).expect("valid encoded key");
            K::validate(&key, &stats_schemas.key)
        }
    };
    let () = key_valid
        .unwrap_or_else(|err| panic!("constructing batch with mismatched key schema: {}", err));
    let val_valid = match decoded_val {
        Some(val) => V::validate(val, &stats_schemas.val),
        None => {
            let val = V::decode(val, &stats_schemas.val).expect("valid encoded val");
            V::validate(&val, &stats_schemas.val)
        }
    };
    let () = val_valid
        .unwrap_or_else(|err| panic!("constructing batch with mismatched val schema: {}", err));
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
    ) -> Option<BlobTraceUpdates> {
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

    fn drain(&mut self) -> BlobTraceUpdates {
        // TODO: we're in a position to do a very good estimate here, instead of using the default.
        let builder = mem::take(&mut self.records_builder);
        let records = builder.finish(&self.metrics.columnar);
        assert_eq!(self.records_builder.len(), 0);
        BlobTraceUpdates::Row(records)
    }
}

#[derive(Debug)]
enum WritingRuns<T> {
    /// Building a single run with the specified ordering. Parts are expected to be internally
    /// sorted and added in order. Merging a vec of parts will shift them out to a hollow run
    /// in blob, bounding the total length of a run in memory.
    Ordered(RunOrder, MergeTree<Pending<RunPart<T>>>),
    /// Building multiple runs which may have different orders. Merging a vec of runs will cause
    /// them to be compacted together, bounding the total number of runs we generate.
    Compacting(MergeTree<(RunOrder, Pending<Vec<RunPart<T>>>)>),
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
        lower: Antichain<T>,
        blob: Arc<dyn Blob>,
        isolated_runtime: Arc<IsolatedRuntime>,
        batch_metrics: &BatchWriteMetrics,
        schemas: Schemas<K, V>,
    ) -> Self
    where
        K: Codec + Debug,
        V: Codec + Debug,
        T: Lattice + Send + Sync,
        D: Semigroup + Ord + Codec64 + Send + Sync,
    {
        let writing_runs = {
            let cfg = cfg.clone();
            let blob = Arc::clone(&blob);
            let metrics = Arc::clone(&metrics);
            let shard_metrics = Arc::clone(&shard_metrics);
            let isolated_runtime = Arc::clone(&isolated_runtime);
            // Clamping to prevent extreme values given weird configs.
            let runs_per_compaction = runs_per_compaction.clamp(2, 1024);

            let merge_fn = move |parts: Vec<(RunOrder, Pending<Vec<RunPart<T>>>)>| {
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
                                (
                                    RunMeta {
                                        order: Some(order),
                                        schema: schemas.id,
                                    },
                                    parts.into_result().await,
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
                        output_batch.parts
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
            lower,
            blob,
            isolated_runtime,
            next_index: 0,
            writing_runs,
            batch_metrics: batch_metrics.clone(),
        }
    }

    pub(crate) fn new_ordered(
        cfg: BatchBuilderConfig,
        order: RunOrder,
        metrics: Arc<Metrics>,
        shard_metrics: Arc<ShardMetrics>,
        shard_id: ShardId,
        lower: Antichain<T>,
        blob: Arc<dyn Blob>,
        isolated_runtime: Arc<IsolatedRuntime>,
        batch_metrics: &BatchWriteMetrics,
    ) -> Self {
        let writing_runs = {
            let cfg = cfg.clone();
            let blob = Arc::clone(&blob);
            let metrics = Arc::clone(&metrics);
            let writer_key = cfg.writer_key.clone();
            let merge_fn = move |parts| {
                let blob = Arc::clone(&blob);
                let writer_key = writer_key.clone();
                let metrics = Arc::clone(&metrics);
                let handle = mz_ore::task::spawn(
                    || "batch::spill_run",
                    async move {
                        let parts = stream::iter(parts)
                            .then(|p: Pending<RunPart<T>>| p.into_result())
                            .collect()
                            .await;
                        let run_ref = HollowRunRef::set(
                            shard_id,
                            blob.as_ref(),
                            &writer_key,
                            HollowRun { parts },
                            &*metrics,
                        )
                        .await;

                        RunPart::Many(run_ref)
                    }
                    .instrument(debug_span!("batch::spill_run")),
                );
                Pending::new(handle)
            };
            WritingRuns::Ordered(order, MergeTree::new(cfg.run_length_limit, merge_fn))
        };
        BatchParts {
            cfg,
            metrics,
            shard_metrics,
            shard_id,
            lower,
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
        key_lower: Vec<u8>,
        mut updates: BlobTraceUpdates,
        upper: Antichain<T>,
        since: Antichain<T>,
        diffs_sum: D,
    ) {
        let desc = Description::new(self.lower.clone(), upper, since);
        let batch_metrics = self.batch_metrics.clone();
        let index = self.next_index;
        self.next_index += 1;
        let ts_rewrite = None;
        let schema_id = write_schemas.id;

        // Decide this once per part and plumb it around as necessary so that we
        // use a consistent answer for things like inline threshold.
        let part_write_columnar_data = self.cfg.part_write_columnar_data();

        // If we're going to encode structured data then halve our limit since we're storing
        // it twice, once as binary encoded and once as structured.
        let inline_threshold = if part_write_columnar_data {
            self.cfg.inline_writes_single_max_bytes.saturating_div(2)
        } else {
            self.cfg.inline_writes_single_max_bytes
        };

        let (name, write_future) = if updates.goodbytes() < inline_threshold {
            let metrics = Arc::clone(&self.metrics);
            let write_schemas = write_schemas.clone();

            let span = debug_span!("batch::inline_part", shard = %self.shard_id).or_current();
            (
                "batch::inline_part",
                async move {
                    let updates = if part_write_columnar_data {
                        let records = updates.records().clone();
                        let structured = metrics
                            .columnar
                            .arrow()
                            .measure_part_build(|| {
                                updates.get_or_make_structured::<K, V>(
                                    write_schemas.key.as_ref(),
                                    write_schemas.val.as_ref(),
                                )
                            })
                            .clone();
                        BlobTraceUpdates::Both(records, structured)
                    } else {
                        let records = updates.records().clone();
                        BlobTraceUpdates::Row(records)
                    };

                    let start = Instant::now();
                    let updates = LazyInlineBatchPart::from(&ProtoInlineBatchPart {
                        desc: Some(desc.into_proto()),
                        index: index.into_proto(),
                        updates: Some(updates.into_proto()),
                    });
                    batch_metrics
                        .step_inline
                        .inc_by(start.elapsed().as_secs_f64());
                    RunPart::Single(BatchPart::Inline {
                        updates,
                        ts_rewrite,
                        schema_id,
                    })
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
            let write_span =
                debug_span!("batch::write_part", shard = %self.shard_metrics.shard_id).or_current();
            (
                "batch::write_part",
                BatchParts::write_hollow_part(
                    self.cfg.clone(),
                    part_write_columnar_data,
                    Arc::clone(&self.blob),
                    Arc::clone(&self.metrics),
                    Arc::clone(&self.shard_metrics),
                    batch_metrics.clone(),
                    Arc::clone(&self.isolated_runtime),
                    part,
                    self.expected_order(),
                    key_lower,
                    ts_rewrite,
                    D::encode(&diffs_sum),
                    write_schemas.clone(),
                )
                .map(RunPart::Single)
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
                let run = Pending::Writing(mz_ore::task::spawn(|| name, async move {
                    vec![write_future.await]
                }));
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
        part_write_columnar_data: bool,
        blob: Arc<dyn Blob>,
        metrics: Arc<Metrics>,
        shard_metrics: Arc<ShardMetrics>,
        batch_metrics: BatchWriteMetrics,
        isolated_runtime: Arc<IsolatedRuntime>,
        mut updates: BlobTraceBatchPart<T>,
        run_order: RunOrder,
        key_lower: Vec<u8>,
        ts_rewrite: Option<Antichain<T>>,
        diffs_sum: [u8; 8],
        write_schemas: Schemas<K, V>,
    ) -> BatchPart<T> {
        let partial_key = PartialBatchKey::new(&cfg.writer_key, &PartId::new());
        let key = partial_key.complete(&shard_metrics.shard_id);
        let goodbytes = updates.updates.records().goodbytes();
        let metrics_ = Arc::clone(&metrics);
        let schema_id = write_schemas.id;

        let (stats, structured_key_lower, (buf, encode_time)) = isolated_runtime
            .spawn_named(|| "batch::encode_part", async move {
                // Only encode our updates in a structured format if required, it's expensive.
                let stats = 'collect_stats: {
                    if cfg.stats_collection_enabled || cfg.batch_columnar_format.is_structured() {
                        let result = metrics_.columnar.arrow().measure_part_build(|| {
                            encode_updates(&write_schemas, &updates.updates)
                        });

                        // We can't collect stats if we failed to encode in a columnar format.
                        let Ok((extended_cols, stats)) = result else {
                            soft_panic_or_log!("failed to encode in columnar format! {:?}", result);
                            break 'collect_stats None;
                        };

                        // Write a structured batch if the dyncfg is enabled and we're the stats
                        // override is not set.
                        if let BlobTraceUpdates::Row(record) = &updates.updates {
                            if let Some(record_ext) = extended_cols {
                                if part_write_columnar_data {
                                    updates.updates =
                                        BlobTraceUpdates::Both(record.clone(), record_ext);
                                }
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
            diffs_sum: cfg.write_diffs_sum.then_some(diffs_sum),
            format: Some(cfg.batch_columnar_format),
            schema_id,
        })
    }

    #[instrument(level = "debug", name = "batch::finish_upload", fields(shard = %self.shard_id))]
    pub(crate) async fn finish(self) -> Vec<(RunOrder, Vec<RunPart<T>>)> {
        match self.writing_runs {
            WritingRuns::Ordered(RunOrder::Unordered, run) => {
                let parts = run.finish();
                let mut output = Vec::with_capacity(parts.len());
                for part in parts {
                    output.push((RunOrder::Unordered, vec![part.into_result().await]));
                }
                output
            }
            WritingRuns::Ordered(order, run) => {
                let parts = run.finish();
                let mut output = Vec::with_capacity(parts.len());
                for part in parts {
                    output.push(part.into_result().await);
                }
                vec![(order, output)]
            }
            WritingRuns::Compacting(batches) => {
                let runs = batches.finish();
                let mut output = Vec::with_capacity(runs.len());
                for (order, run) in runs {
                    let run = run.into_result().await;
                    output.push((order, run))
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
    use crate::cfg::BATCH_BUILDER_MAX_OUTSTANDING_PARTS;
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
        let builder = write.builder(Antichain::from_elem(0));
        let mut builder = builder.builder;

        fn assert_writing(
            builder: &BatchBuilderInternal<String, String, u64, i64>,
            expected_finished: &[bool],
        ) {
            let WritingRuns::Compacting(run) = &builder.parts.writing_runs else {
                unreachable!("ordered run!")
            };

            let actual: Vec<_> = run.iter().map(|(_, p)| p.is_finished()).collect();
            assert_eq!(*expected_finished, actual);
        }

        assert_writing(&builder, &[]);

        // We set blob_target_size to 0, so the first update gets forced out
        // into a run.
        let ((k, v), t, d) = &data[0];
        let key = k.encode_to_vec();
        let val = v.encode_to_vec();
        builder.add(&key, &val, t, d).await.expect("invalid usage");
        assert_writing(&builder, &[false]);

        // We set batch_builder_max_outstanding_parts to 2, so we are allowed to
        // pipeline a second part.
        let ((k, v), t, d) = &data[1];
        let key = k.encode_to_vec();
        let val = v.encode_to_vec();
        builder.add(&key, &val, t, d).await.expect("invalid usage");
        assert_writing(&builder, &[false, false]);

        // But now that we have 3 parts, the add call back-pressures until the
        // first one finishes.
        let ((k, v), t, d) = &data[2];
        let key = k.encode_to_vec();
        let val = v.encode_to_vec();
        builder.add(&key, &val, t, d).await.expect("invalid usage");
        assert_writing(&builder, &[true, false, false]);

        // Finally, pushing a fourth part will cause the first three to spill out into
        // a new compacted run.
        let ((k, v), t, d) = &data[3];
        let key = k.encode_to_vec();
        let val = v.encode_to_vec();
        builder.add(&key, &val, t, d).await.expect("invalid usage");
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
    async fn batch_builder_partial_order() {
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
        cache.cfg().set_config(&BATCH_COLUMNAR_FORMAT, "both_v2");
        cache.cfg().set_config(&BATCH_COLUMNAR_FORMAT_PERCENT, 100);
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
