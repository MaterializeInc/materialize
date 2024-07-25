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
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_dyncfg::{Config, ConfigSet, ConfigValHandle};
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_ore::{soft_panic_no_log, soft_panic_or_log};
use mz_persist::indexed::encoding::{BlobTraceBatchPart, BlobTraceUpdates};
use mz_persist::location::{Blob, SeqNo};
use mz_persist_types::columnar::{ColumnDecoder, Schema2};
use mz_persist_types::stats::PartStats;
use mz_persist_types::{Codec, Codec64};
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use serde::{Deserialize, Serialize};
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::{debug_span, trace_span, Instrument};

use crate::batch::{
    proto_fetch_batch_filter, ProtoFetchBatchFilter, ProtoFetchBatchFilterListen, ProtoLease,
    ProtoLeasedBatchPart,
};
use crate::cfg::PersistConfig;
use crate::error::InvalidUsage;
use crate::internal::encoding::{LazyInlineBatchPart, LazyPartStats, LazyProto, Schemas};
use crate::internal::machine::retry_external;
use crate::internal::metrics::{Metrics, MetricsPermits, ReadMetrics, ShardMetrics};
use crate::internal::paths::BlobKey;
use crate::internal::state::{BatchPart, HollowBatchPart};
use crate::project::ProjectionPushdown;
use crate::read::LeasedReaderId;
use crate::ShardId;

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
    "Format we'll use to decode a Persist Part, either 'row' or 'row_with_validate' (Materialize).",
);

#[derive(Debug, Clone)]
pub(crate) struct BatchFetcherConfig {
    pub(crate) part_decode_format: ConfigValHandle<String>,
}

impl BatchFetcherConfig {
    pub fn new(value: &PersistConfig) -> Self {
        BatchFetcherConfig {
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
    pub(crate) schemas: Schemas<K, V>,

    // Ensures that `BatchFetcher` is of the same type as the `ReadHandle` it's
    // derived from.
    pub(crate) _phantom: PhantomData<(K, V, T, D)>,
}

impl<K, V, T, D> BatchFetcher<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    /// Takes a [`SerdeLeasedBatchPart`] into a [`LeasedBatchPart`].
    pub fn leased_part_from_exchangeable(&self, x: SerdeLeasedBatchPart) -> LeasedBatchPart<T> {
        x.decode(Arc::clone(&self.metrics))
    }

    /// Trade in an exchange-able [LeasedBatchPart] for the data it represents.
    ///
    /// Note to check the `LeasedBatchPart` documentation for how to handle the
    /// returned value.
    pub async fn fetch_leased_part(
        &self,
        part: &LeasedBatchPart<T>,
    ) -> Result<FetchedBlob<K, V, T, D>, InvalidUsage<T>> {
        if &part.shard_id != &self.shard_id {
            let batch_shard = part.shard_id.clone();
            return Err(InvalidUsage::BatchNotFromThisShard {
                batch_shard,
                handle_shard: self.shard_id.clone(),
            });
        }

        let (buf, fetch_permit) = match &part.part {
            BatchPart::Hollow(x) => {
                let fetch_permit = self
                    .metrics
                    .semaphore
                    .acquire_fetch_permits(x.encoded_size_bytes)
                    .await;
                let buf = fetch_batch_part_blob(
                    &part.shard_id,
                    self.blob.as_ref(),
                    &self.metrics,
                    &self.shard_metrics,
                    &self.metrics.read.batch_fetcher,
                    x,
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
                    panic!("batch fetcher could not fetch batch part: {}", blob_key)
                });
                let buf = FetchedBlobBuf::Hollow {
                    buf,
                    part: x.clone(),
                };
                (buf, Some(Arc::new(fetch_permit)))
            }
            BatchPart::Inline {
                updates,
                ts_rewrite,
            } => {
                let buf = FetchedBlobBuf::Inline {
                    desc: part.desc.clone(),
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
            registered_desc: part.desc.clone(),
            schemas: self.schemas.clone(),
            filter: part.filter.clone(),
            filter_pushdown_audit: part.filter_pushdown_audit,
            structured_part_audit: self.cfg.part_decode_format(),
            fetch_permit,
            _phantom: PhantomData,
        };
        Ok(fetched_blob)
    }
}

#[derive(Debug, Clone)]
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

impl<T: Timestamp + Codec64> RustType<ProtoFetchBatchFilter> for FetchBatchFilter<T> {
    fn into_proto(&self) -> ProtoFetchBatchFilter {
        let kind = match self {
            FetchBatchFilter::Snapshot { as_of } => {
                proto_fetch_batch_filter::Kind::Snapshot(as_of.into_proto())
            }
            FetchBatchFilter::Listen { as_of, lower } => {
                proto_fetch_batch_filter::Kind::Listen(ProtoFetchBatchFilterListen {
                    as_of: Some(as_of.into_proto()),
                    lower: Some(lower.into_proto()),
                })
            }
            FetchBatchFilter::Compaction { .. } => unreachable!("not serialized"),
        };
        ProtoFetchBatchFilter { kind: Some(kind) }
    }

    fn from_proto(proto: ProtoFetchBatchFilter) -> Result<Self, TryFromProtoError> {
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoFetchBatchFilter::kind"))?;
        match kind {
            proto_fetch_batch_filter::Kind::Snapshot(as_of) => Ok(FetchBatchFilter::Snapshot {
                as_of: as_of.into_rust()?,
            }),
            proto_fetch_batch_filter::Kind::Listen(ProtoFetchBatchFilterListen {
                as_of,
                lower,
            }) => Ok(FetchBatchFilter::Listen {
                as_of: as_of.into_rust_if_some("ProtoFetchBatchFilterListen::as_of")?,
                lower: lower.into_rust_if_some("ProtoFetchBatchFilterListen::lower")?,
            }),
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
    schemas: Schemas<K, V>,
) -> FetchedPart<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    let encoded_part = EncodedPart::fetch(
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
    FetchedPart::new(
        metrics,
        encoded_part,
        schemas,
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
            .inc_by(u64::cast_from(parsed.updates.records().goodbytes()));
        EncodedPart::from_hollow(read_metrics.clone(), registered_desc, part, parsed)
    })
}

pub(crate) async fn fetch_batch_part<T>(
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
    let part = decode_batch_part_blob(metrics, read_metrics, registered_desc.clone(), part, &buf);
    Ok(part)
}

/// This represents the lease of a seqno. It's generally paired with some external state,
/// like a hollow part: holding this lease indicates that we may still want to fetch that part,
/// and should hold back GC to keep it around.
///
/// Generally the state and lease are bundled together, as in [LeasedBatchPart]... but sometimes
/// it's necessary to handle them separately, so this struct is exposed as well. Handle with care.
#[derive(Clone, Debug, Default)]
pub(crate) struct Lease(Arc<()>);

impl Lease {
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
/// - By converting it to [`SerdeLeasedBatchPart`] through
///   `Self::into_exchangeable_part`. [`SerdeLeasedBatchPart`] is exchangeable,
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
    pub(crate) reader_id: LeasedReaderId,
    pub(crate) filter: FetchBatchFilter<T>,
    pub(crate) desc: Description<T>,
    pub(crate) part: BatchPart<T>,
    /// The `SeqNo` from which this part originated; we track this value as
    /// to ensure the `SeqNo` isn't garbage collected while a
    /// read still depends on it.
    pub(crate) leased_seqno: SeqNo,
    /// The lease that prevents this part from being GCed. Code should ensure that this lease
    /// lives as long as the part is needed.
    pub(crate) lease: Option<Lease>,
    pub(crate) filter_pushdown_audit: bool,
}

impl<T> LeasedBatchPart<T>
where
    T: Timestamp + Codec64,
{
    /// Takes `self` into a [`SerdeLeasedBatchPart`], which allows `self` to be
    /// exchanged (potentially across the network).
    ///
    /// !!!WARNING!!!
    ///
    /// This method also returns the [Lease] associated with the given part, since
    /// that can't travel across process boundaries. The caller is responsible for
    /// ensuring that the lease is held for as long as the batch part may be in use:
    /// dropping it too early may cause a fetch to fail.
    pub(crate) fn into_exchangeable_part(mut self) -> (SerdeLeasedBatchPart, Option<Lease>) {
        let (proto, _metrics) = self.into_proto();
        // If `x` has a lease, we've effectively transferred it to `r`.
        let lease = self.lease.take();
        let part = SerdeLeasedBatchPart {
            encoded_size_bytes: self.part.encoded_size_bytes(),
            proto: LazyProto::from(&proto),
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

    /// Apply any relevant projection pushdown optimizations.
    ///
    /// NB: Until we implement full projection pushdown, this doesn't guarantee
    /// any projection.
    pub fn maybe_optimize(&mut self, cfg: &ConfigSet, project: &ProjectionPushdown) {
        let as_of = match &self.filter {
            FetchBatchFilter::Snapshot { as_of } => as_of,
            FetchBatchFilter::Listen { .. } | FetchBatchFilter::Compaction { .. } => return,
        };
        let faked_part = project.try_optimize_ignored_data_fetch(
            cfg,
            &self.metrics,
            as_of,
            &self.desc,
            &self.part,
        );
        if let Some(faked_part) = faked_part {
            self.part = faked_part;
        }
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
    schemas: Schemas<K, V>,
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
            schemas: self.schemas.clone(),
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

impl<K: Codec, V: Codec, T: Clone, D> Clone for ShardSourcePart<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            part: self.part.clone(),
            fetch_permit: self.fetch_permit.clone(),
        }
    }
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
    pub fn parse(&self) -> ShardSourcePart<K, V, T, D> {
        let (part, stats) = match &self.buf {
            FetchedBlobBuf::Hollow { buf, part } => {
                let parsed = decode_batch_part_blob(
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
            self.schemas.clone(),
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
    part: EncodedPart<T>,
    structured_part: (
        Option<Arc<<K::Schema as Schema2<K>>::Decoder>>,
        Option<Arc<<V::Schema as Schema2<V>>::Decoder>>,
    ),
    schemas: Schemas<K, V>,
    filter_pushdown_audit: Option<LazyPartStats>,
    part_cursor: Cursor,
    key_storage: Option<K::Storage>,
    val_storage: Option<V::Storage>,

    _phantom: PhantomData<fn() -> D>,
}

impl<K: Codec, V: Codec, T: Clone, D> Clone for FetchedPart<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            metrics: Arc::clone(&self.metrics),
            ts_filter: self.ts_filter.clone(),
            part: self.part.clone(),
            structured_part: self.structured_part.clone(),
            schemas: self.schemas.clone(),
            filter_pushdown_audit: self.filter_pushdown_audit.clone(),
            part_cursor: self.part_cursor.clone(),
            key_storage: None,
            val_storage: None,
            _phantom: self._phantom.clone(),
        }
    }
}

impl<K: Codec, V: Codec, T: Timestamp + Lattice + Codec64, D> FetchedPart<K, V, T, D> {
    fn new(
        metrics: Arc<Metrics>,
        part: EncodedPart<T>,
        schemas: Schemas<K, V>,
        ts_filter: FetchBatchFilter<T>,
        filter_pushdown_audit: bool,
        structured_part_audit: PartDecodeFormat,
        stats: Option<&LazyPartStats>,
    ) -> Self {
        let filter_pushdown_audit = if filter_pushdown_audit {
            stats.cloned()
        } else {
            None
        };

        // TODO(parkmycar): We should probably refactor this since these columns are duplicated
        // (via a smart pointer) in EncodedPart.
        //
        // For structured columnar data we need to downcast from `dyn Array`s to concrete types.
        // Downcasting is relatively expensive so we want to do this once, which is why we do it
        // when creating a FetchedPart.
        let structured_part = match (&part.part.updates, structured_part_audit) {
            // Only downcast and create decoders if we have structured data AND
            // an audit of the data is requested.
            (
                BlobTraceUpdates::Both(_codec, structured),
                PartDecodeFormat::Row {
                    validate_structured: true,
                },
            ) => {
                let key = match Schema2::decoder_any(schemas.key.as_ref(), &*structured.key) {
                    Ok(key) => Some(Arc::new(key)),
                    Err(err) => {
                        tracing::error!(?err, "failed to create key decoder");
                        None
                    }
                };

                let val = match Schema2::decoder_any(schemas.val.as_ref(), &*structured.val) {
                    Ok(val) => Some(Arc::new(val)),
                    Err(err) => {
                        tracing::error!(?err, "failed to create val decoder");
                        None
                    }
                };

                (key, val)
            }
            _ => (None, None),
        };

        FetchedPart {
            metrics,
            ts_filter,
            part,
            structured_part,
            schemas,
            filter_pushdown_audit,
            part_cursor: Cursor::default(),
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
    pub fn is_filter_pushdown_audit(&self) -> Option<impl std::fmt::Debug> {
        self.filter_pushdown_audit.clone()
    }
}

/// A [Blob] object that has been fetched, but has no associated decoding
/// logic.
#[derive(Debug, Clone)]
pub(crate) struct EncodedPart<T> {
    metrics: ReadMetrics,
    registered_desc: Description<T>,
    part: Arc<BlobTraceBatchPart<T>>,
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
        result_override: Option<(K, V)>,
    ) -> Option<((Result<K, String>, Result<V, String>), T, D)> {
        while let Some(((k, v, mut t, d), idx)) = self.part_cursor.pop(&self.part) {
            if !self.ts_filter.filter_ts(&mut t) {
                continue;
            }

            let mut d = D::decode(d);

            // If `filter_ts` advances our timestamp, we may end up with the same K, V, T in successive
            // records. If so, opportunistically consolidate those out.
            while let Some((k_next, v_next, mut t_next, d_next)) = self.part_cursor.peek(&self.part)
            {
                if (k, v) != (k_next, v_next) {
                    break;
                }

                if !self.ts_filter.filter_ts(&mut t_next) {
                    break;
                }
                if t != t_next {
                    break;
                }

                // All equal... consolidate!
                self.part_cursor.idx += 1;
                d.plus_equals(&D::decode(d_next));
            }

            // If multiple updates consolidate out entirely, drop the record.
            if d.is_zero() {
                continue;
            }

            if let Some((key, val)) = result_override {
                return Some(((Ok(key), Ok(val)), t, d));
            } else {
                let k = self.metrics.codecs.key.decode(|| match key.take() {
                    Some(mut key) => match K::decode_from(&mut key, k, &mut self.key_storage) {
                        Ok(()) => Ok(key),
                        Err(err) => Err(err),
                    },
                    None => K::decode(k),
                });
                let v = self.metrics.codecs.val.decode(|| match val.take() {
                    Some(mut val) => match V::decode_from(&mut val, v, &mut self.val_storage) {
                        Ok(()) => Ok(val),
                        Err(err) => Err(err),
                    },
                    None => V::decode(v),
                });

                // Note: We only provide structured columns, if they were originally written, and a
                // dyncfg was specified to run validation.
                if let Some(key_structured) = self.structured_part.0.as_ref() {
                    let key_metrics = self.metrics.columnar.arrow().key();

                    let mut k_s = K::default();
                    key_metrics.measure_decoding(|| key_structured.decode(idx, &mut k_s));

                    // Purposefully do not trace to prevent blowing up Sentry.
                    let is_valid = key_metrics.report_valid(|| Ok(&k_s) == k.as_ref());
                    if !is_valid {
                        soft_panic_no_log!("structured key did not match, {k_s:?} != {k:?}");
                    }
                }

                if let Some(val_structured) = self.structured_part.1.as_ref() {
                    let val_metrics = self.metrics.columnar.arrow().val();

                    let mut v_s = V::default();
                    val_metrics.measure_decoding(|| val_structured.decode(idx, &mut v_s));

                    // Purposefully do not trace to prevent blowing up Sentry.
                    let is_valid = val_metrics.report_valid(|| Ok(&v_s) == v.as_ref());
                    if !is_valid {
                        soft_panic_no_log!("structured val did not match, {v_s:?} != {v:?}");
                    }
                }

                return Some(((k, v), t, d));
            }
        }
        None
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
        self.next_with_storage(&mut None, &mut None, None)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // We don't know in advance how restrictive the filter will be.
        let max_len = self.part.part.updates.records().len();
        (0, Some(max_len))
    }
}

impl<T> EncodedPart<T>
where
    T: Timestamp + Lattice + Codec64,
{
    pub async fn fetch(
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
            } => Ok(EncodedPart::from_inline(
                metrics,
                read_metrics.clone(),
                registered_desc.clone(),
                updates,
                ts_rewrite.as_ref(),
            )),
        }
    }

    pub(crate) fn from_inline(
        metrics: &Metrics,
        read_metrics: ReadMetrics,
        desc: Description<T>,
        x: &LazyInlineBatchPart,
        ts_rewrite: Option<&Antichain<T>>,
    ) -> Self {
        let parsed = x.decode(&metrics.columnar).expect("valid inline part");
        Self::new(read_metrics, desc, "inline", ts_rewrite, parsed)
    }

    pub(crate) fn from_hollow(
        metrics: ReadMetrics,
        registered_desc: Description<T>,
        part: &HollowBatchPart<T>,
        parsed: BlobTraceBatchPart<T>,
    ) -> Self {
        Self::new(
            metrics,
            registered_desc,
            &part.key.0,
            part.ts_rewrite.as_ref(),
            parsed,
        )
    }

    pub(crate) fn new(
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
            assert!(
                PartialOrder::less_equal(inline_desc.lower(), registered_desc.lower()),
                "key={} inline={:?} registered={:?}",
                printable_name,
                inline_desc,
                registered_desc
            );
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
            part: Arc::new(parsed),
            needs_truncation,
            ts_rewrite: ts_rewrite.cloned(),
        }
    }

    pub(crate) fn maybe_unconsolidated(&self) -> bool {
        // At time of writing, only user parts may be unconsolidated, and they are always
        // written with a since of [T::minimum()].
        self.part.desc.since().borrow() == AntichainRef::new(&[T::minimum()])
    }
}

/// A pointer into a particular encoded part, with methods for fetching an update and
/// scanning forward to the next. It is an error to use the same cursor for distinct
/// parts.
///
/// We avoid implementing copy to make it hard to accidentally duplicate a cursor. However,
/// clone is very cheap.
#[derive(Debug, Clone, Default)]
pub(crate) struct Cursor {
    idx: usize,
}

impl Cursor {
    /// Get the tuple at the specified pair of indices. If there is no such tuple,
    /// either because we are out of range or because this tuple has been filtered out,
    /// this returns `None`.
    pub fn get<'a, T: Timestamp + Lattice + Codec64>(
        &self,
        encoded: &'a EncodedPart<T>,
    ) -> Option<(&'a [u8], &'a [u8], T, [u8; 8])> {
        let part = encoded.part.updates.records();
        let ((k, v), t, d) = part.get(self.idx)?;

        let mut t = T::decode(t);
        // We assert on the write side that at most one of rewrite or
        // truncation is used, so it shouldn't matter which is run first.
        //
        // That said, my (Dan's) intuition here is that rewrite goes first,
        // though I don't particularly have a justification for it.
        if let Some(ts_rewrite) = encoded.ts_rewrite.as_ref() {
            t.advance_by(ts_rewrite.borrow());
            encoded.metrics.ts_rewrite.inc();
        }

        // This filtering is really subtle, see the comment above for
        // what's going on here.
        let truncated_t = encoded.needs_truncation && {
            !encoded.registered_desc.lower().less_equal(&t)
                || encoded.registered_desc.upper().less_equal(&t)
        };
        if truncated_t {
            return None;
        }
        Some((k, v, t, d))
    }

    /// A cursor points to a particular update in the backing part data.
    /// If the update it points to is not valid, advance it to the next valid update
    /// if there is one, and return the pointed-to data.
    pub fn peek<'a, T: Timestamp + Lattice + Codec64>(
        &mut self,
        part: &'a EncodedPart<T>,
    ) -> Option<(&'a [u8], &'a [u8], T, [u8; 8])> {
        while !self.is_exhausted(part) {
            let current = self.get(part);
            if current.is_some() {
                return current;
            }
            self.advance(part);
        }
        None
    }

    /// Similar to peek, but advance the cursor just past the end of the most recent update.
    /// Returns the update and the `(part_idx, idx)` that is was popped at.
    pub fn pop<'a, T: Timestamp + Lattice + Codec64>(
        &mut self,
        part: &'a EncodedPart<T>,
    ) -> Option<((&'a [u8], &'a [u8], T, [u8; 8]), usize)> {
        while !self.is_exhausted(part) {
            let current = self.get(part);
            let popped_idx = self.idx;
            self.advance(part);
            if current.is_some() {
                return current.map(|p| (p, popped_idx));
            }
        }
        None
    }

    /// Returns true if the cursor is past the end of the part data.
    pub fn is_exhausted<T: Timestamp + Codec64>(&self, part: &EncodedPart<T>) -> bool {
        self.idx >= part.part.updates.records().len()
    }

    /// Advance the cursor just past the end of the most recent update, if there is one.
    pub fn advance<T: Timestamp + Codec64>(&mut self, part: &EncodedPart<T>) {
        if !self.is_exhausted(part) {
            self.idx += 1;
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
pub struct SerdeLeasedBatchPart {
    // Duplicated with the one serialized in the proto for use in backpressure.
    encoded_size_bytes: usize,
    // We wrap this in a LazyProto because it guarantees that we use the proto
    // encoding for the serde impls.
    proto: LazyProto<ProtoLeasedBatchPart>,
}

impl SerdeLeasedBatchPart {
    /// Returns the encoded size of the given part.
    pub fn encoded_size_bytes(&self) -> usize {
        self.encoded_size_bytes
    }

    pub(crate) fn decode<T: Timestamp + Codec64>(
        &self,
        metrics: Arc<Metrics>,
    ) -> LeasedBatchPart<T> {
        let proto = self.proto.decode().expect("valid leased batch part");
        (proto, metrics)
            .into_rust()
            .expect("valid leased batch part")
    }
}

// TODO: The way we're smuggling the metrics through here is a bit odd. Perhaps
// we could refactor `LeasedBatchPart` into some proto-able struct plus the
// metrics for the Drop bit?
impl<T: Timestamp + Codec64> RustType<(ProtoLeasedBatchPart, Arc<Metrics>)> for LeasedBatchPart<T> {
    fn into_proto(&self) -> (ProtoLeasedBatchPart, Arc<Metrics>) {
        let proto = ProtoLeasedBatchPart {
            shard_id: self.shard_id.into_proto(),
            filter: Some(self.filter.into_proto()),
            desc: Some(self.desc.into_proto()),
            part: Some(self.part.into_proto()),
            lease: Some(ProtoLease {
                reader_id: self.reader_id.into_proto(),
                seqno: Some(self.leased_seqno.into_proto()),
            }),
            filter_pushdown_audit: self.filter_pushdown_audit,
        };
        (proto, Arc::clone(&self.metrics))
    }

    fn from_proto(proto: (ProtoLeasedBatchPart, Arc<Metrics>)) -> Result<Self, TryFromProtoError> {
        let (proto, metrics) = proto;
        let lease = proto
            .lease
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoLeasedBatchPart::lease"))?;
        Ok(LeasedBatchPart {
            metrics,
            shard_id: proto.shard_id.into_rust()?,
            filter: proto
                .filter
                .into_rust_if_some("ProtoLeasedBatchPart::filter")?,
            desc: proto.desc.into_rust_if_some("ProtoLeasedBatchPart::desc")?,
            part: proto.part.into_rust_if_some("ProtoLeasedBatchPart::part")?,
            reader_id: lease.reader_id.into_rust()?,
            leased_seqno: lease.seqno.into_rust_if_some("ProtoLease::seqno")?,
            lease: None,
            filter_pushdown_audit: proto.filter_pushdown_audit,
        })
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
}

impl PartDecodeFormat {
    /// Returns a default value for [`PartDecodeFormat`].
    pub const fn default() -> Self {
        // IMPORTANT: By default we will not decode or validate our structured format until it's
        // more stable.
        PartDecodeFormat::Row {
            validate_structured: false,
        }
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
    is_exchange_data::<SerdeLeasedBatchPart>();
    is_exchange_data::<SerdeLeasedBatchPart>();
}
