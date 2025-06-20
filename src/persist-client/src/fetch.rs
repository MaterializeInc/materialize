// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fetching batches of data from persist's backing store

use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int64Array};
use arrow::compute::FilterBuilder;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use itertools::EitherOrBoth;
use mz_dyncfg::{Config, ConfigSet, ConfigValHandle};
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_ore::{soft_panic_no_log, soft_panic_or_log};
use mz_persist::indexed::columnar::arrow::{realloc_any, realloc_array};
use mz_persist::indexed::columnar::{ColumnarRecords, ColumnarRecordsStructuredExt};
use mz_persist::indexed::encoding::{BlobTraceBatchPart, BlobTraceUpdates};
use mz_persist::location::{Blob, SeqNo};
use mz_persist::metrics::ColumnarMetrics;
use mz_persist_types::arrow::ArrayOrd;
use mz_persist_types::columnar::{ColumnDecoder, Schema, data_type};
use mz_persist_types::part::Codec64Mut;
use mz_persist_types::schema::backward_compatible;
use mz_persist_types::stats::PartStats;
use mz_persist_types::{Codec, Codec64};
use mz_proto::RustType;
use serde::{Deserialize, Serialize};
use timely::PartialOrder;
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp};
use tracing::{Instrument, debug, debug_span, trace_span};

use crate::ShardId;
use crate::cfg::PersistConfig;
use crate::error::InvalidUsage;
use crate::internal::compact::CompactConfig;
use crate::internal::encoding::{LazyInlineBatchPart, LazyPartStats, LazyProto, Schemas};
use crate::internal::machine::retry_external;
use crate::internal::metrics::{Metrics, MetricsPermits, ReadMetrics, ShardMetrics};
use crate::internal::paths::BlobKey;
use crate::internal::state::{
    BatchPart, HollowBatchPart, ProtoHollowBatchPart, ProtoInlineBatchPart,
};
use crate::read::LeasedReaderId;
use crate::schema::{PartMigration, SchemaCache};

pub(crate) const FETCH_SEMAPHORE_COST_ADJUSTMENT: Config<f64> = Config::new(
    "persist_fetch_semaphore_cost_adjustment",
    // We use `encoded_size_bytes` as the number of permits, but the parsed size
    // is larger than the encoded one, so adjust it. This default value is from
    // eyeballing graphs in experiments that were run on tpch loadgen data.
    1.2,
    "\
    An adjustment multiplied by encoded_size_bytes to approximate an upper \
    bound on the size in lgalloc, which includes the decoded version.",
);

pub(crate) const FETCH_SEMAPHORE_PERMIT_ADJUSTMENT: Config<f64> = Config::new(
    "persist_fetch_semaphore_permit_adjustment",
    1.0,
    "\
    A limit on the number of outstanding persist bytes being fetched and \
    parsed, expressed as a multiplier of the process's memory limit. This data \
    all spills to lgalloc, so values > 1.0 are safe. Only applied to cc \
    replicas.",
);

pub(crate) const PART_DECODE_FORMAT: Config<&'static str> = Config::new(
    "persist_part_decode_format",
    PartDecodeFormat::default().as_str(),
    "\
    Format we'll use to decode a Persist Part, either 'row', \
    'row_with_validate', or 'arrow' (Materialize).",
);

pub(crate) const OPTIMIZE_IGNORED_DATA_FETCH: Config<bool> = Config::new(
    "persist_optimize_ignored_data_fetch",
    true,
    "CYA to allow opt-out of a performance optimization to skip fetching ignored data",
);

pub(crate) const FETCH_VALIDATE_PART_LOWER_BOUNDS_ON_READ: Config<bool> = Config::new(
    "persist_validate_part_lower_bounds_on_read",
    true,
    "Validate that the lower of the part is less than or equal to the lower of the batch containing that part.",
);

#[derive(Debug, Clone)]
pub(crate) struct FetchConfig {
    pub(crate) validate_lower_bounds_on_read: bool,
}

impl FetchConfig {
    pub fn from_persist_config(cfg: &PersistConfig) -> Self {
        Self {
            validate_lower_bounds_on_read: FETCH_VALIDATE_PART_LOWER_BOUNDS_ON_READ.get(cfg),
        }
    }

    pub fn from_compact_config(cfg: &CompactConfig) -> Self {
        Self {
            validate_lower_bounds_on_read: cfg.fetch_validate_lower_bounds_on_read,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BatchFetcherConfig {
    pub(crate) part_decode_format: ConfigValHandle<String>,
}

impl BatchFetcherConfig {
    pub fn new(value: &PersistConfig) -> Self {
        Self {
            part_decode_format: PART_DECODE_FORMAT.handle(value),
        }
    }

    pub fn part_decode_format(&self) -> PartDecodeFormat {
        PartDecodeFormat::from_str(self.part_decode_format.get().as_str())
    }
}

/// Capable of fetching [`LeasedBatchPart`] while not holding any capabilities.
#[derive(Debug)]
pub struct BatchFetcher<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
    // These are only here so we can use them in the auto-expiring `Drop` impl.
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
{
    pub(crate) cfg: BatchFetcherConfig,
    pub(crate) blob: Arc<dyn Blob>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) shard_metrics: Arc<ShardMetrics>,
    pub(crate) shard_id: ShardId,
    pub(crate) read_schemas: Schemas<K, V>,
    pub(crate) schema_cache: SchemaCache<K, V, T, D>,
    pub(crate) is_transient: bool,

    // Ensures that `BatchFetcher` is of the same type as the `ReadHandle` it's
    // derived from.
    pub(crate) _phantom: PhantomData<fn() -> (K, V, T, D)>,
}

impl<K, V, T, D> BatchFetcher<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Semigroup + Codec64 + Send + Sync,
{
    /// Trade in an exchange-able [LeasedBatchPart] for the data it represents.
    ///
    /// Note to check the `LeasedBatchPart` documentation for how to handle the
    /// returned value.
    pub async fn fetch_leased_part(
        &mut self,
        part: ExchangeableBatchPart<T>,
    ) -> Result<Result<FetchedBlob<K, V, T, D>, BlobKey>, InvalidUsage<T>> {
        let ExchangeableBatchPart {
            shard_id,
            encoded_size_bytes: _,
            desc,
            filter,
            filter_pushdown_audit,
            part,
        } = part;
        let part: BatchPart<T> = part.decode_to().expect("valid part");
        if shard_id != self.shard_id {
            return Err(InvalidUsage::BatchNotFromThisShard {
                batch_shard: shard_id,
                handle_shard: self.shard_id.clone(),
            });
        }

        let migration =
            PartMigration::new(&part, self.read_schemas.clone(), &mut self.schema_cache)
                .await
                .unwrap_or_else(|read_schemas| {
                    panic!(
                        "could not decode part {:?} with schema: {:?}",
                        part.schema_id(),
                        read_schemas
                    )
                });

        let (buf, fetch_permit) = match &part {
            BatchPart::Hollow(x) => {
                let fetch_permit = self
                    .metrics
                    .semaphore
                    .acquire_fetch_permits(x.encoded_size_bytes)
                    .await;
                let read_metrics = if self.is_transient {
                    &self.metrics.read.unindexed
                } else {
                    &self.metrics.read.batch_fetcher
                };
                let buf = fetch_batch_part_blob(
                    &shard_id,
                    self.blob.as_ref(),
                    &self.metrics,
                    &self.shard_metrics,
                    read_metrics,
                    x,
                )
                .await;
                let buf = match buf {
                    Ok(buf) => buf,
                    Err(key) => return Ok(Err(key)),
                };
                let buf = FetchedBlobBuf::Hollow {
                    buf,
                    part: x.clone(),
                };
                (buf, Some(Arc::new(fetch_permit)))
            }
            BatchPart::Inline {
                updates,
                ts_rewrite,
                ..
            } => {
                let buf = FetchedBlobBuf::Inline {
                    desc: desc.clone(),
                    updates: updates.clone(),
                    ts_rewrite: ts_rewrite.clone(),
                };
                (buf, None)
            }
        };
        let fetched_blob = FetchedBlob {
            metrics: Arc::clone(&self.metrics),
            read_metrics: self.metrics.read.batch_fetcher.clone(),
            buf,
            registered_desc: desc.clone(),
            migration,
            filter: filter.clone(),
            filter_pushdown_audit,
            structured_part_audit: self.cfg.part_decode_format(),
            fetch_permit,
            _phantom: PhantomData,
        };
        Ok(Ok(fetched_blob))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum FetchBatchFilter<T> {
    Snapshot {
        as_of: Antichain<T>,
    },
    Listen {
        as_of: Antichain<T>,
        lower: Antichain<T>,
    },
    Compaction {
        since: Antichain<T>,
    },
}

impl<T: Timestamp + Lattice> FetchBatchFilter<T> {
    pub(crate) fn filter_ts(&self, t: &mut T) -> bool {
        match self {
            FetchBatchFilter::Snapshot { as_of } => {
                // This time is covered by a listen
                if as_of.less_than(t) {
                    return false;
                }
                t.advance_by(as_of.borrow());
                true
            }
            FetchBatchFilter::Listen { as_of, lower } => {
                // This time is covered by a snapshot
                if !as_of.less_than(t) {
                    return false;
                }

                // Because of compaction, the next batch we get might also
                // contain updates we've already emitted. For example, we
                // emitted `[1, 2)` and then compaction combined that batch with
                // a `[2, 3)` batch into a new `[1, 3)` batch. If this happens,
                // we just need to filter out anything < the frontier. This
                // frontier was the upper of the last batch (and thus exclusive)
                // so for the == case, we still emit.
                if !lower.less_equal(t) {
                    return false;
                }
                true
            }
            FetchBatchFilter::Compaction { since } => {
                t.advance_by(since.borrow());
                true
            }
        }
    }
}

/// Trade in an exchange-able [LeasedBatchPart] for the data it represents.
///
/// Note to check the `LeasedBatchPart` documentation for how to handle the
/// returned value.
pub(crate) async fn fetch_leased_part<K, V, T, D>(
    cfg: &PersistConfig,
    part: &LeasedBatchPart<T>,
    blob: &dyn Blob,
    metrics: Arc<Metrics>,
    read_metrics: &ReadMetrics,
    shard_metrics: &ShardMetrics,
    reader_id: &LeasedReaderId,
    read_schemas: Schemas<K, V>,
    schema_cache: &mut SchemaCache<K, V, T, D>,
) -> FetchedPart<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Semigroup + Codec64 + Send + Sync,
{
    let fetch_config = FetchConfig::from_persist_config(cfg);
    let encoded_part = EncodedPart::fetch(
        &fetch_config,
        &part.shard_id,
        blob,
        &metrics,
        shard_metrics,
        read_metrics,
        &part.desc,
        &part.part,
    )
    .await
    .unwrap_or_else(|blob_key| {
        // Ideally, readers should never encounter a missing blob. They place a seqno
        // hold as they consume their snapshot/listen, preventing any blobs they need
        // from being deleted by garbage collection, and all blob implementations are
        // linearizable so there should be no possibility of stale reads.
        //
        // If we do have a bug and a reader does encounter a missing blob, the state
        // cannot be recovered, and our best option is to panic and retry the whole
        // process.
        panic!("{} could not fetch batch part: {}", reader_id, blob_key)
    });
    let part_cfg = BatchFetcherConfig::new(cfg);
    let migration = PartMigration::new(&part.part, read_schemas, schema_cache)
        .await
        .unwrap_or_else(|read_schemas| {
            panic!(
                "could not decode part {:?} with schema: {:?}",
                part.part.schema_id(),
                read_schemas
            )
        });
    FetchedPart::new(
        metrics,
        encoded_part,
        migration,
        part.filter.clone(),
        part.filter_pushdown_audit,
        part_cfg.part_decode_format(),
        part.part.stats(),
    )
}

pub(crate) async fn fetch_batch_part_blob<T>(
    shard_id: &ShardId,
    blob: &dyn Blob,
    metrics: &Metrics,
    shard_metrics: &ShardMetrics,
    read_metrics: &ReadMetrics,
    part: &HollowBatchPart<T>,
) -> Result<SegmentedBytes, BlobKey> {
    let now = Instant::now();
    let get_span = debug_span!("fetch_batch::get");
    let blob_key = part.key.complete(shard_id);
    let value = retry_external(&metrics.retries.external.fetch_batch_get, || async {
        shard_metrics.blob_gets.inc();
        blob.get(&blob_key).await
    })
    .instrument(get_span.clone())
    .await
    .ok_or(blob_key)?;

    drop(get_span);

    read_metrics.part_count.inc();
    read_metrics.part_bytes.inc_by(u64::cast_from(value.len()));
    read_metrics.seconds.inc_by(now.elapsed().as_secs_f64());

    Ok(value)
}

pub(crate) fn decode_batch_part_blob<T>(
    cfg: &FetchConfig,
    metrics: &Metrics,
    read_metrics: &ReadMetrics,
    registered_desc: Description<T>,
    part: &HollowBatchPart<T>,
    buf: &SegmentedBytes,
) -> EncodedPart<T>
where
    T: Timestamp + Lattice + Codec64,
{
    trace_span!("fetch_batch::decode").in_scope(|| {
        let parsed = metrics
            .codecs
            .batch
            .decode(|| BlobTraceBatchPart::decode(buf, &metrics.columnar))
            .map_err(|err| anyhow!("couldn't decode batch at key {}: {}", part.key, err))
            // We received a State that we couldn't decode. This could happen if
            // persist messes up backward/forward compatibility, if the durable
            // data was corrupted, or if operations messes up deployment. In any
            // case, fail loudly.
            .expect("internal error: invalid encoded state");
        read_metrics
            .part_goodbytes
            .inc_by(u64::cast_from(parsed.updates.goodbytes()));
        EncodedPart::from_hollow(cfg, read_metrics.clone(), registered_desc, part, parsed)
    })
}

pub(crate) async fn fetch_batch_part<T>(
    cfg: &FetchConfig,
    shard_id: &ShardId,
    blob: &dyn Blob,
    metrics: &Metrics,
    shard_metrics: &ShardMetrics,
    read_metrics: &ReadMetrics,
    registered_desc: &Description<T>,
    part: &HollowBatchPart<T>,
) -> Result<EncodedPart<T>, BlobKey>
where
    T: Timestamp + Lattice + Codec64,
{
    let buf =
        fetch_batch_part_blob(shard_id, blob, metrics, shard_metrics, read_metrics, part).await?;
    let part = decode_batch_part_blob(
        cfg,
        metrics,
        read_metrics,
        registered_desc.clone(),
        part,
        &buf,
    );
    Ok(part)
}

/// This represents the lease of a seqno. It's generally paired with some external state,
/// like a hollow part: holding this lease indicates that we may still want to fetch that part,
/// and should hold back GC to keep it around.
///
/// Generally the state and lease are bundled together, as in [LeasedBatchPart]... but sometimes
/// it's necessary to handle them separately, so this struct is exposed as well. Handle with care.
#[derive(Clone, Debug)]
pub struct Lease(Arc<SeqNo>);

impl Lease {
    /// Creates a new [Lease] that holds the given [SeqNo].
    pub fn new(seqno: SeqNo) -> Self {
        Self(Arc::new(seqno))
    }

    /// Returns the inner [SeqNo] of this [Lease].
    #[cfg(test)]
    pub fn seqno(&self) -> SeqNo {
        *self.0
    }

    /// Returns the number of live copies of this lease, including this one.
    pub fn count(&self) -> usize {
        Arc::strong_count(&self.0)
    }
}

/// A token representing one fetch-able batch part.
///
/// It is tradeable via `crate::fetch::fetch_batch` for the resulting data
/// stored in the part.
///
/// # Exchange
///
/// You can exchange `LeasedBatchPart`:
/// - If `leased_seqno.is_none()`
/// - By converting it to [`ExchangeableBatchPart`] through
///   `Self::into_exchangeable_part`. [`ExchangeableBatchPart`] is exchangeable,
///   including over the network.
///
/// n.b. `Self::into_exchangeable_part` is known to be equivalent to
/// `SerdeLeasedBatchPart::from(self)`, but we want the additional warning message to
/// be visible and sufficiently scary.
///
/// # Panics
/// `LeasedBatchPart` panics when dropped unless a very strict set of invariants are
/// held:
///
/// `LeasedBatchPart` may only be dropped if it:
/// - Does not have a leased `SeqNo (i.e. `self.leased_seqno.is_none()`)
///
/// In any other circumstance, dropping `LeasedBatchPart` panics.
#[derive(Debug)]
pub struct LeasedBatchPart<T> {
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) shard_id: ShardId,
    pub(crate) filter: FetchBatchFilter<T>,
    pub(crate) desc: Description<T>,
    pub(crate) part: BatchPart<T>,
    /// The lease that prevents this part from being GCed. Code should ensure that this lease
    /// lives as long as the part is needed.
    pub(crate) lease: Lease,
    pub(crate) filter_pushdown_audit: bool,
}

impl<T> LeasedBatchPart<T>
where
    T: Timestamp + Codec64,
{
    /// Takes `self` into a [`ExchangeableBatchPart`], which allows `self` to be
    /// exchanged (potentially across the network).
    ///
    /// !!!WARNING!!!
    ///
    /// This method also returns the [Lease] associated with the given part, since
    /// that can't travel across process boundaries. The caller is responsible for
    /// ensuring that the lease is held for as long as the batch part may be in use:
    /// dropping it too early may cause a fetch to fail.
    pub(crate) fn into_exchangeable_part(self) -> (ExchangeableBatchPart<T>, Lease) {
        // If `x` has a lease, we've effectively transferred it to `r`.
        let lease = self.lease.clone();
        let part = ExchangeableBatchPart {
            shard_id: self.shard_id,
            encoded_size_bytes: self.part.encoded_size_bytes(),
            desc: self.desc.clone(),
            filter: self.filter.clone(),
            part: LazyProto::from(&self.part.into_proto()),
            filter_pushdown_audit: self.filter_pushdown_audit,
        };
        (part, lease)
    }

    /// The encoded size of this part in bytes
    pub fn encoded_size_bytes(&self) -> usize {
        self.part.encoded_size_bytes()
    }

    /// The filter has indicated we don't need this part, we can verify the
    /// ongoing end-to-end correctness of corner cases via "audit". This means
    /// we fetch the part like normal and if the MFP keeps anything from it,
    /// then something has gone horribly wrong.
    pub fn request_filter_pushdown_audit(&mut self) {
        self.filter_pushdown_audit = true;
    }

    /// Returns the pushdown stats for this part.
    pub fn stats(&self) -> Option<PartStats> {
        self.part.stats().map(|x| x.decode())
    }

    /// Apply any relevant projection pushdown optimizations, assuming that the data in the part
    /// is equivalent to the provided key and value.
    pub fn maybe_optimize(&mut self, cfg: &ConfigSet, key: ArrayRef, val: ArrayRef) {
        assert_eq!(key.len(), 1, "expect a single-row key array");
        assert_eq!(val.len(), 1, "expect a single-row val array");
        let as_of = match &self.filter {
            FetchBatchFilter::Snapshot { as_of } => as_of,
            FetchBatchFilter::Listen { .. } | FetchBatchFilter::Compaction { .. } => return,
        };
        if !OPTIMIZE_IGNORED_DATA_FETCH.get(cfg) {
            return;
        }
        let (diffs_sum, _stats) = match &self.part {
            BatchPart::Hollow(x) => (x.diffs_sum, x.stats.as_ref()),
            BatchPart::Inline { .. } => return,
        };
        debug!(
            "try_optimize_ignored_data_fetch diffs_sum={:?} as_of={:?} lower={:?} upper={:?}",
            // This is only used for debugging, so hack to assume that D is i64.
            diffs_sum.map(i64::decode),
            as_of.elements(),
            self.desc.lower().elements(),
            self.desc.upper().elements()
        );
        let as_of = match &as_of.elements() {
            &[as_of] => as_of,
            _ => return,
        };
        let eligible = self.desc.upper().less_equal(as_of) && self.desc.since().less_equal(as_of);
        if !eligible {
            return;
        }
        let Some(diffs_sum) = diffs_sum else {
            return;
        };

        debug!(
            "try_optimize_ignored_data_fetch faked {:?} diffs at ts {:?} skipping fetch of {} bytes",
            // This is only used for debugging, so hack to assume that D is i64.
            i64::decode(diffs_sum),
            as_of,
            self.part.encoded_size_bytes(),
        );
        self.metrics.pushdown.parts_faked_count.inc();
        self.metrics
            .pushdown
            .parts_faked_bytes
            .inc_by(u64::cast_from(self.part.encoded_size_bytes()));
        let timestamps = {
            let mut col = Codec64Mut::with_capacity(1);
            col.push(as_of);
            col.finish()
        };
        let diffs = {
            let mut col = Codec64Mut::with_capacity(1);
            col.push_raw(diffs_sum);
            col.finish()
        };
        let updates = BlobTraceUpdates::Structured {
            key_values: ColumnarRecordsStructuredExt { key, val },
            timestamps,
            diffs,
        };
        let faked_data = LazyInlineBatchPart::from(&ProtoInlineBatchPart {
            desc: Some(self.desc.into_proto()),
            index: 0,
            updates: Some(updates.into_proto()),
        });
        self.part = BatchPart::Inline {
            updates: faked_data,
            ts_rewrite: None,
            schema_id: None,
            deprecated_schema_id: None,
        };
    }
}

impl<T> Drop for LeasedBatchPart<T> {
    /// For details, see [`LeasedBatchPart`].
    fn drop(&mut self) {
        self.metrics.lease.dropped_part.inc()
    }
}

/// A [Blob] object that has been fetched, but not at all decoded.
///
/// In contrast to [FetchedPart], this representation hasn't yet done parquet
/// decoding.
#[derive(Debug)]
pub struct FetchedBlob<K: Codec, V: Codec, T, D> {
    metrics: Arc<Metrics>,
    read_metrics: ReadMetrics,
    buf: FetchedBlobBuf<T>,
    registered_desc: Description<T>,
    migration: PartMigration<K, V>,
    filter: FetchBatchFilter<T>,
    filter_pushdown_audit: bool,
    structured_part_audit: PartDecodeFormat,
    fetch_permit: Option<Arc<MetricsPermits>>,
    _phantom: PhantomData<fn() -> D>,
}

#[derive(Debug, Clone)]
enum FetchedBlobBuf<T> {
    Hollow {
        buf: SegmentedBytes,
        part: HollowBatchPart<T>,
    },
    Inline {
        desc: Description<T>,
        updates: LazyInlineBatchPart,
        ts_rewrite: Option<Antichain<T>>,
    },
}

impl<K: Codec, V: Codec, T: Clone, D> Clone for FetchedBlob<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            metrics: Arc::clone(&self.metrics),
            read_metrics: self.read_metrics.clone(),
            buf: self.buf.clone(),
            registered_desc: self.registered_desc.clone(),
            migration: self.migration.clone(),
            filter: self.filter.clone(),
            filter_pushdown_audit: self.filter_pushdown_audit.clone(),
            fetch_permit: self.fetch_permit.clone(),
            structured_part_audit: self.structured_part_audit.clone(),
            _phantom: self._phantom.clone(),
        }
    }
}

/// [FetchedPart] but with an accompanying permit from the fetch mem/disk
/// semaphore.
pub struct ShardSourcePart<K: Codec, V: Codec, T, D> {
    /// The underlying [FetchedPart].
    pub part: FetchedPart<K, V, T, D>,
    fetch_permit: Option<Arc<MetricsPermits>>,
}

impl<K, V, T: Debug, D: Debug> Debug for ShardSourcePart<K, V, T, D>
where
    K: Codec + Debug,
    <K as Codec>::Storage: Debug,
    V: Codec + Debug,
    <V as Codec>::Storage: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ShardSourcePart { part, fetch_permit } = self;
        f.debug_struct("ShardSourcePart")
            .field("part", part)
            .field("fetch_permit", fetch_permit)
            .finish()
    }
}

impl<K: Codec, V: Codec, T: Timestamp + Lattice + Codec64, D> FetchedBlob<K, V, T, D> {
    /// Partially decodes this blob into a [FetchedPart].
    pub fn parse(&self, cfg: PersistConfig) -> ShardSourcePart<K, V, T, D> {
        self.parse_internal(&FetchConfig::from_persist_config(&cfg))
    }

    /// Partially decodes this blob into a [FetchedPart].
    pub(crate) fn parse_internal(&self, cfg: &FetchConfig) -> ShardSourcePart<K, V, T, D> {
        let (part, stats) = match &self.buf {
            FetchedBlobBuf::Hollow { buf, part } => {
                let parsed = decode_batch_part_blob(
                    cfg,
                    &self.metrics,
                    &self.read_metrics,
                    self.registered_desc.clone(),
                    part,
                    buf,
                );
                (parsed, part.stats.as_ref())
            }
            FetchedBlobBuf::Inline {
                desc,
                updates,
                ts_rewrite,
            } => {
                let parsed = EncodedPart::from_inline(
                    cfg,
                    &self.metrics,
                    self.read_metrics.clone(),
                    desc.clone(),
                    updates,
                    ts_rewrite.as_ref(),
                );
                (parsed, None)
            }
        };
        let part = FetchedPart::new(
            Arc::clone(&self.metrics),
            part,
            self.migration.clone(),
            self.filter.clone(),
            self.filter_pushdown_audit,
            self.structured_part_audit,
            stats,
        );
        ShardSourcePart {
            part,
            fetch_permit: self.fetch_permit.clone(),
        }
    }

    /// Decodes and returns the pushdown stats for this part, if known.
    pub fn stats(&self) -> Option<PartStats> {
        match &self.buf {
            FetchedBlobBuf::Hollow { part, .. } => part.stats.as_ref().map(|x| x.decode()),
            FetchedBlobBuf::Inline { .. } => None,
        }
    }
}

/// A [Blob] object that has been fetched, but not yet fully decoded.
///
/// In contrast to [FetchedBlob], this representation has already done parquet
/// decoding.
#[derive(Debug)]
pub struct FetchedPart<K: Codec, V: Codec, T, D> {
    metrics: Arc<Metrics>,
    ts_filter: FetchBatchFilter<T>,
    // If migration is Either, then the columnar one will have already been
    // applied here on the structured data only.
    part: EitherOrBoth<
        ColumnarRecords,
        (
            <K::Schema as Schema<K>>::Decoder,
            <V::Schema as Schema<V>>::Decoder,
        ),
    >,
    timestamps: Int64Array,
    diffs: Int64Array,
    migration: PartMigration<K, V>,
    filter_pushdown_audit: Option<LazyPartStats>,
    peek_stash: Option<((Result<K, String>, Result<V, String>), T, D)>,
    part_cursor: usize,
    key_storage: Option<K::Storage>,
    val_storage: Option<V::Storage>,

    _phantom: PhantomData<fn() -> D>,
}

impl<K: Codec, V: Codec, T: Timestamp + Lattice + Codec64, D> FetchedPart<K, V, T, D> {
    pub(crate) fn new(
        metrics: Arc<Metrics>,
        part: EncodedPart<T>,
        migration: PartMigration<K, V>,
        ts_filter: FetchBatchFilter<T>,
        filter_pushdown_audit: bool,
        part_decode_format: PartDecodeFormat,
        stats: Option<&LazyPartStats>,
    ) -> Self {
        let part_len = u64::cast_from(part.part.updates.len());
        match &migration {
            PartMigration::SameSchema { .. } => metrics.schema.migration_count_same.inc(),
            PartMigration::Schemaless { .. } => {
                metrics.schema.migration_count_codec.inc();
                metrics.schema.migration_len_legacy_codec.inc_by(part_len);
            }
            PartMigration::Either { .. } => {
                metrics.schema.migration_count_either.inc();
                match part_decode_format {
                    PartDecodeFormat::Row {
                        validate_structured: false,
                    } => metrics.schema.migration_len_either_codec.inc_by(part_len),
                    PartDecodeFormat::Row {
                        validate_structured: true,
                    } => {
                        metrics.schema.migration_len_either_codec.inc_by(part_len);
                        metrics.schema.migration_len_either_arrow.inc_by(part_len);
                    }
                    PartDecodeFormat::Arrow => {
                        metrics.schema.migration_len_either_arrow.inc_by(part_len)
                    }
                }
            }
        }

        let filter_pushdown_audit = if filter_pushdown_audit {
            stats.cloned()
        } else {
            None
        };

        let downcast_structured = |structured: ColumnarRecordsStructuredExt,
                                   structured_only: bool| {
            let key_size_before = ArrayOrd::new(&structured.key).goodbytes();

            let structured = match &migration {
                PartMigration::SameSchema { .. } => structured,
                PartMigration::Schemaless { read } if structured_only => {
                    // We don't know the source schema, but we do know the source datatype; migrate it directly.
                    let start = Instant::now();
                    let read_key = data_type::<K>(&*read.key).ok()?;
                    let read_val = data_type::<V>(&*read.val).ok()?;
                    let key_migration = backward_compatible(structured.key.data_type(), &read_key)?;
                    let val_migration = backward_compatible(structured.val.data_type(), &read_val)?;
                    let key = key_migration.migrate(structured.key);
                    let val = val_migration.migrate(structured.val);
                    metrics
                        .schema
                        .migration_migrate_seconds
                        .inc_by(start.elapsed().as_secs_f64());
                    ColumnarRecordsStructuredExt { key, val }
                }
                PartMigration::Schemaless { .. } => return None,
                PartMigration::Either {
                    write: _,
                    read: _,
                    key_migration,
                    val_migration,
                } => {
                    let start = Instant::now();
                    let key = key_migration.migrate(structured.key);
                    let val = val_migration.migrate(structured.val);
                    metrics
                        .schema
                        .migration_migrate_seconds
                        .inc_by(start.elapsed().as_secs_f64());
                    ColumnarRecordsStructuredExt { key, val }
                }
            };

            let read_schema = migration.codec_read();
            let key = K::Schema::decoder_any(&*read_schema.key, &*structured.key);
            let val = V::Schema::decoder_any(&*read_schema.val, &*structured.val);

            match &key {
                Ok(key_decoder) => {
                    let key_size_after = key_decoder.goodbytes();
                    let key_diff = key_size_before.saturating_sub(key_size_after);
                    metrics
                        .pushdown
                        .parts_projection_trimmed_bytes
                        .inc_by(u64::cast_from(key_diff));
                }
                Err(e) => {
                    soft_panic_or_log!("failed to create decoder: {e:#?}");
                }
            }

            Some((key.ok()?, val.ok()?))
        };

        let updates = part.normalize(&metrics.columnar);
        let timestamps = updates.timestamps().clone();
        let diffs = updates.diffs().clone();
        let part = match updates {
            // If only one encoding is available, decode via that encoding.
            BlobTraceUpdates::Row(records) => EitherOrBoth::Left(records),
            BlobTraceUpdates::Structured { key_values, .. } => EitherOrBoth::Right(
                // The structured-only data format was added after schema ids were recorded everywhere,
                // so we expect this data to be present.
                downcast_structured(key_values, true).expect("valid schemas for structured data"),
            ),
            // If both are available, respect the specified part decode format.
            BlobTraceUpdates::Both(records, ext) => match part_decode_format {
                PartDecodeFormat::Row {
                    validate_structured: false,
                } => EitherOrBoth::Left(records),
                PartDecodeFormat::Row {
                    validate_structured: true,
                } => match downcast_structured(ext, false) {
                    Some(decoders) => EitherOrBoth::Both(records, decoders),
                    None => EitherOrBoth::Left(records),
                },
                PartDecodeFormat::Arrow => match downcast_structured(ext, false) {
                    Some(decoders) => EitherOrBoth::Right(decoders),
                    None => EitherOrBoth::Left(records),
                },
            },
        };

        FetchedPart {
            metrics,
            ts_filter,
            part,
            peek_stash: None,
            timestamps,
            diffs,
            migration,
            filter_pushdown_audit,
            part_cursor: 0,
            key_storage: None,
            val_storage: None,
            _phantom: PhantomData,
        }
    }

    /// Returns Some if this part was only fetched as part of a filter pushdown
    /// audit. See [LeasedBatchPart::request_filter_pushdown_audit].
    ///
    /// If set, the value in the Option is for debugging and should be included
    /// in any error messages.
    pub fn is_filter_pushdown_audit(&self) -> Option<impl std::fmt::Debug + use<K, V, T, D>> {
        self.filter_pushdown_audit.clone()
    }
}

/// A [Blob] object that has been fetched, but has no associated decoding
/// logic.
#[derive(Debug)]
pub(crate) struct EncodedPart<T> {
    metrics: ReadMetrics,
    registered_desc: Description<T>,
    part: BlobTraceBatchPart<T>,
    needs_truncation: bool,
    ts_rewrite: Option<Antichain<T>>,
}

impl<K, V, T, D> FetchedPart<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    /// [Self::next] but optionally providing a `K` and `V` for alloc reuse.
    ///
    /// When `result_override` is specified, return it instead of decoding data.
    /// This is used when we know the decoded result will be ignored.
    pub fn next_with_storage(
        &mut self,
        key: &mut Option<K>,
        val: &mut Option<V>,
    ) -> Option<((Result<K, String>, Result<V, String>), T, D)> {
        let mut consolidated = self.peek_stash.take();
        loop {
            // Fetch and decode the next tuple in the sequence. (Or break if there is none.)
            let next = if self.part_cursor < self.timestamps.len() {
                let next_idx = self.part_cursor;
                self.part_cursor += 1;
                // These `to_le_bytes` calls were previously encapsulated by `ColumnarRecords`.
                // TODO(structured): re-encapsulate these once we've finished the structured migration.
                let mut t = T::decode(self.timestamps.values()[next_idx].to_le_bytes());
                if !self.ts_filter.filter_ts(&mut t) {
                    continue;
                }
                let d = D::decode(self.diffs.values()[next_idx].to_le_bytes());
                if d.is_zero() {
                    continue;
                }
                let kv = self.decode_kv(next_idx, key, val);
                (kv, t, d)
            } else {
                break;
            };

            // Attempt to consolidate in the next tuple, stashing it if that's not possible.
            if let Some((kv, t, d)) = &mut consolidated {
                let (kv_next, t_next, d_next) = &next;
                if kv == kv_next && t == t_next {
                    d.plus_equals(d_next);
                    if d.is_zero() {
                        consolidated = None;
                    }
                } else {
                    self.peek_stash = Some(next);
                    break;
                }
            } else {
                consolidated = Some(next);
            }
        }

        let (kv, t, d) = consolidated?;

        Some((kv, t, d))
    }

    fn decode_kv(
        &mut self,
        index: usize,
        key: &mut Option<K>,
        val: &mut Option<V>,
    ) -> (Result<K, String>, Result<V, String>) {
        let decoded = self
            .part
            .as_ref()
            .map_left(|codec| {
                let ((ck, cv), _, _) = codec.get(index).expect("valid index");
                Self::decode_codec(
                    &*self.metrics,
                    self.migration.codec_read(),
                    ck,
                    cv,
                    key,
                    val,
                    &mut self.key_storage,
                    &mut self.val_storage,
                )
            })
            .map_right(|(structured_key, structured_val)| {
                self.decode_structured(index, structured_key, structured_val, key, val)
            });

        match decoded {
            EitherOrBoth::Both((k, v), (k_s, v_s)) => {
                // Purposefully do not trace to prevent blowing up Sentry.
                let is_valid = self
                    .metrics
                    .columnar
                    .arrow()
                    .key()
                    .report_valid(|| k_s == k);
                if !is_valid {
                    soft_panic_no_log!("structured key did not match, {k_s:?} != {k:?}");
                }
                // Purposefully do not trace to prevent blowing up Sentry.
                let is_valid = self
                    .metrics
                    .columnar
                    .arrow()
                    .val()
                    .report_valid(|| v_s == v);
                if !is_valid {
                    soft_panic_no_log!("structured val did not match, {v_s:?} != {v:?}");
                }

                (k, v)
            }
            EitherOrBoth::Left(kv) => kv,
            EitherOrBoth::Right(kv) => kv,
        }
    }

    fn decode_codec(
        metrics: &Metrics,
        read_schemas: &Schemas<K, V>,
        key_buf: &[u8],
        val_buf: &[u8],
        key: &mut Option<K>,
        val: &mut Option<V>,
        key_storage: &mut Option<K::Storage>,
        val_storage: &mut Option<V::Storage>,
    ) -> (Result<K, String>, Result<V, String>) {
        let k = metrics.codecs.key.decode(|| match key.take() {
            Some(mut key) => {
                match K::decode_from(&mut key, key_buf, key_storage, &read_schemas.key) {
                    Ok(()) => Ok(key),
                    Err(err) => Err(err),
                }
            }
            None => K::decode(key_buf, &read_schemas.key),
        });
        let v = metrics.codecs.val.decode(|| match val.take() {
            Some(mut val) => {
                match V::decode_from(&mut val, val_buf, val_storage, &read_schemas.val) {
                    Ok(()) => Ok(val),
                    Err(err) => Err(err),
                }
            }
            None => V::decode(val_buf, &read_schemas.val),
        });
        (k, v)
    }

    fn decode_structured(
        &self,
        idx: usize,
        keys: &<K::Schema as Schema<K>>::Decoder,
        vals: &<V::Schema as Schema<V>>::Decoder,
        key: &mut Option<K>,
        val: &mut Option<V>,
    ) -> (Result<K, String>, Result<V, String>) {
        let mut key = key.take().unwrap_or_default();
        keys.decode(idx, &mut key);

        let mut val = val.take().unwrap_or_default();
        vals.decode(idx, &mut val);

        (Ok(key), Ok(val))
    }
}

impl<K, V, T, D> Iterator for FetchedPart<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    type Item = ((Result<K, String>, Result<V, String>), T, D);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_with_storage(&mut None, &mut None)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // We don't know in advance how restrictive the filter will be.
        let max_len = self.timestamps.len();
        (0, Some(max_len))
    }
}

impl<T> EncodedPart<T>
where
    T: Timestamp + Lattice + Codec64,
{
    pub async fn fetch(
        cfg: &FetchConfig,
        shard_id: &ShardId,
        blob: &dyn Blob,
        metrics: &Metrics,
        shard_metrics: &ShardMetrics,
        read_metrics: &ReadMetrics,
        registered_desc: &Description<T>,
        part: &BatchPart<T>,
    ) -> Result<Self, BlobKey> {
        match part {
            BatchPart::Hollow(x) => {
                fetch_batch_part(
                    cfg,
                    shard_id,
                    blob,
                    metrics,
                    shard_metrics,
                    read_metrics,
                    registered_desc,
                    x,
                )
                .await
            }
            BatchPart::Inline {
                updates,
                ts_rewrite,
                ..
            } => Ok(EncodedPart::from_inline(
                cfg,
                metrics,
                read_metrics.clone(),
                registered_desc.clone(),
                updates,
                ts_rewrite.as_ref(),
            )),
        }
    }

    pub(crate) fn from_inline(
        cfg: &FetchConfig,
        metrics: &Metrics,
        read_metrics: ReadMetrics,
        desc: Description<T>,
        x: &LazyInlineBatchPart,
        ts_rewrite: Option<&Antichain<T>>,
    ) -> Self {
        let parsed = x.decode(&metrics.columnar).expect("valid inline part");
        Self::new(cfg, read_metrics, desc, "inline", ts_rewrite, parsed)
    }

    pub(crate) fn from_hollow(
        cfg: &FetchConfig,
        metrics: ReadMetrics,
        registered_desc: Description<T>,
        part: &HollowBatchPart<T>,
        parsed: BlobTraceBatchPart<T>,
    ) -> Self {
        Self::new(
            cfg,
            metrics,
            registered_desc,
            &part.key.0,
            part.ts_rewrite.as_ref(),
            parsed,
        )
    }

    pub(crate) fn new(
        cfg: &FetchConfig,
        metrics: ReadMetrics,
        registered_desc: Description<T>,
        printable_name: &str,
        ts_rewrite: Option<&Antichain<T>>,
        parsed: BlobTraceBatchPart<T>,
    ) -> Self {
        // There are two types of batches in persist:
        // - Batches written by a persist user (either directly or indirectly
        //   via BatchBuilder). These always have a since of the minimum
        //   timestamp and may be registered in persist state with a tighter set
        //   of bounds than are inline in the batch (truncation). To read one of
        //   these batches, all data physically in the batch but outside of the
        //   truncated bounds must be ignored. Not every user batch is
        //   truncated.
        // - Batches written by compaction. These always have an inline desc
        //   that exactly matches the one they are registered with. The since
        //   can be anything.
        let inline_desc = &parsed.desc;
        let needs_truncation = inline_desc.lower() != registered_desc.lower()
            || inline_desc.upper() != registered_desc.upper();
        if needs_truncation {
            if cfg.validate_lower_bounds_on_read {
                assert!(
                    PartialOrder::less_equal(inline_desc.lower(), registered_desc.lower()),
                    "key={} inline={:?} registered={:?}",
                    printable_name,
                    inline_desc,
                    registered_desc
                );
            }
            if ts_rewrite.is_none() {
                // The ts rewrite feature allows us to advance the registered
                // upper of a batch that's already been staged (the inline
                // upper), so if it's been used, then there's no useful
                // invariant that we can assert here.
                assert!(
                    PartialOrder::less_equal(registered_desc.upper(), inline_desc.upper()),
                    "key={} inline={:?} registered={:?}",
                    printable_name,
                    inline_desc,
                    registered_desc
                );
            }
            // As mentioned above, batches that needs truncation will always have a
            // since of the minimum timestamp. Technically we could truncate any
            // batch where the since is less_than the output_desc's lower, but we're
            // strict here so we don't get any surprises.
            assert_eq!(
                inline_desc.since(),
                &Antichain::from_elem(T::minimum()),
                "key={} inline={:?} registered={:?}",
                printable_name,
                inline_desc,
                registered_desc
            );
        } else {
            assert_eq!(
                inline_desc, &registered_desc,
                "key={} inline={:?} registered={:?}",
                printable_name, inline_desc, registered_desc
            );
        }

        EncodedPart {
            metrics,
            registered_desc,
            part: parsed,
            needs_truncation,
            ts_rewrite: ts_rewrite.cloned(),
        }
    }

    pub(crate) fn maybe_unconsolidated(&self) -> bool {
        // At time of writing, only user parts may be unconsolidated, and they are always
        // written with a since of [T::minimum()].
        self.part.desc.since().borrow() == AntichainRef::new(&[T::minimum()])
    }

    pub(crate) fn updates(&self) -> &BlobTraceUpdates {
        &self.part.updates
    }

    /// Returns the updates with all truncation / timestamp rewriting applied.
    pub(crate) fn normalize(&self, metrics: &ColumnarMetrics) -> BlobTraceUpdates {
        let updates = self.part.updates.clone();
        if !self.needs_truncation && self.ts_rewrite.is_none() {
            return updates;
        }

        let mut codec = updates
            .records()
            .map(|r| (r.keys().clone(), r.vals().clone()));
        let mut structured = updates.structured().cloned();
        let mut timestamps = updates.timestamps().clone();
        let mut diffs = updates.diffs().clone();

        if let Some(rewrite) = self.ts_rewrite.as_ref() {
            timestamps = arrow::compute::unary(&timestamps, |i: i64| {
                let mut t = T::decode(i.to_le_bytes());
                t.advance_by(rewrite.borrow());
                i64::from_le_bytes(T::encode(&t))
            });
        }

        let reallocated = if self.needs_truncation {
            let filter = BooleanArray::from_unary(&timestamps, |i| {
                let t = T::decode(i.to_le_bytes());
                let truncate_t = {
                    !self.registered_desc.lower().less_equal(&t)
                        || self.registered_desc.upper().less_equal(&t)
                };
                !truncate_t
            });
            if filter.false_count() == 0 {
                // If we're not filtering anything in practice, skip filtering and reallocating.
                false
            } else {
                let filter = FilterBuilder::new(&filter).optimize().build();
                let do_filter = |array: &dyn Array| filter.filter(array).expect("valid filter len");
                if let Some((keys, vals)) = codec {
                    codec = Some((
                        realloc_array(do_filter(&keys).as_binary(), metrics),
                        realloc_array(do_filter(&vals).as_binary(), metrics),
                    ));
                }
                if let Some(ext) = structured {
                    structured = Some(ColumnarRecordsStructuredExt {
                        key: realloc_any(do_filter(&*ext.key), metrics),
                        val: realloc_any(do_filter(&*ext.val), metrics),
                    });
                }
                timestamps = realloc_array(do_filter(&timestamps).as_primitive(), metrics);
                diffs = realloc_array(do_filter(&diffs).as_primitive(), metrics);
                true
            }
        } else {
            false
        };

        if self.ts_rewrite.is_some() && !reallocated {
            timestamps = realloc_array(&timestamps, metrics);
        }

        if self.ts_rewrite.is_some() {
            self.metrics
                .ts_rewrite
                .inc_by(u64::cast_from(timestamps.len()));
        }

        match (codec, structured) {
            (Some((key, value)), None) => {
                BlobTraceUpdates::Row(ColumnarRecords::new(key, value, timestamps, diffs))
            }
            (Some((key, value)), Some(ext)) => {
                BlobTraceUpdates::Both(ColumnarRecords::new(key, value, timestamps, diffs), ext)
            }
            (None, Some(ext)) => BlobTraceUpdates::Structured {
                key_values: ext,
                timestamps,
                diffs,
            },
            (None, None) => unreachable!(),
        }
    }
}

/// This represents the serde encoding for [`LeasedBatchPart`]. We expose the struct
/// itself (unlike other encodable structs) to attempt to provide stricter drop
/// semantics on `LeasedBatchPart`, i.e. `SerdeLeasedBatchPart` is exchangeable
/// (including over the network), where `LeasedBatchPart` is not.
///
/// For more details see documentation and comments on:
/// - [`LeasedBatchPart`]
/// - `From<SerdeLeasedBatchPart>` for `LeasedBatchPart<T>`
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExchangeableBatchPart<T> {
    shard_id: ShardId,
    // Duplicated with the one serialized in the proto for use in backpressure.
    encoded_size_bytes: usize,
    desc: Description<T>,
    filter: FetchBatchFilter<T>,
    part: LazyProto<ProtoHollowBatchPart>,
    filter_pushdown_audit: bool,
}

impl<T> ExchangeableBatchPart<T> {
    /// Returns the encoded size of the given part.
    pub fn encoded_size_bytes(&self) -> usize {
        self.encoded_size_bytes
    }
}

/// Format we'll use when decoding a [`Part`].
///
/// [`Part`]: mz_persist_types::part::Part
#[derive(Debug, Copy, Clone)]
pub enum PartDecodeFormat {
    /// Decode from opaque `Codec` data.
    Row {
        /// Will also decode the structured data, and validate it matches.
        validate_structured: bool,
    },
    /// Decode from arrow data
    Arrow,
}

impl PartDecodeFormat {
    /// Returns a default value for [`PartDecodeFormat`].
    pub const fn default() -> Self {
        PartDecodeFormat::Arrow
    }

    /// Parses a [`PartDecodeFormat`] from the provided string, falling back to the default if the
    /// provided value is unrecognized.
    pub fn from_str(s: &str) -> Self {
        match s {
            "row" => PartDecodeFormat::Row {
                validate_structured: false,
            },
            "row_with_validate" => PartDecodeFormat::Row {
                validate_structured: true,
            },
            "arrow" => PartDecodeFormat::Arrow,
            x => {
                let default = PartDecodeFormat::default();
                soft_panic_or_log!("Invalid part decode format: '{x}', falling back to {default}");
                default
            }
        }
    }

    /// Returns a string representation of [`PartDecodeFormat`].
    pub const fn as_str(&self) -> &'static str {
        match self {
            PartDecodeFormat::Row {
                validate_structured: false,
            } => "row",
            PartDecodeFormat::Row {
                validate_structured: true,
            } => "row_with_validate",
            PartDecodeFormat::Arrow => "arrow",
        }
    }
}

impl fmt::Display for PartDecodeFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[mz_ore::test]
fn client_exchange_data() {
    // The whole point of SerdeLeasedBatchPart is that it can be exchanged
    // between timely workers, including over the network. Enforce then that it
    // implements ExchangeData.
    fn is_exchange_data<T: timely::ExchangeData>() {}
    is_exchange_data::<ExchangeableBatchPart<u64>>();
    is_exchange_data::<ExchangeableBatchPart<u64>>();
}
