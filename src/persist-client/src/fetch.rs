// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fetching batches of data from persist's backing store

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::{Blob, SeqNo};
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::{debug_span, trace_span, Instrument};

use crate::error::InvalidUsage;
use crate::internal::encoding::{LazyPartStats, Schemas};
use crate::internal::machine::retry_external;
use crate::internal::metrics::{Metrics, ReadMetrics, ShardMetrics};
use crate::internal::paths::{BlobKey, PartialBatchKey};
use crate::read::LeasedReaderId;
use crate::stats::PartStats;
use crate::ShardId;

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
    pub(crate) blob: Arc<dyn Blob + Send + Sync>,
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
        LeasedBatchPart::from(x, Arc::clone(&self.metrics))
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

        let value = fetch_batch_part_blob(
            &part.shard_id,
            self.blob.as_ref(),
            &self.metrics,
            &self.shard_metrics,
            &self.metrics.read.batch_fetcher,
            &part.key,
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
        let fetched_blob = FetchedBlob {
            key: part.key.0.clone(),
            metrics: Arc::clone(&self.metrics),
            read_metrics: self.metrics.read.batch_fetcher.clone(),
            registered_desc: part.desc.clone(),
            part: value,
            schemas: self.schemas.clone(),
            metadata: part.metadata.clone(),
            filter_pushdown_audit: part.filter_pushdown_audit,
            stats: part.stats.clone(),
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
    pub(crate) fn new(meta: &SerdeLeasedBatchPartMetadata) -> Self
    where
        T: Codec64,
    {
        match &meta {
            SerdeLeasedBatchPartMetadata::Snapshot { as_of } => {
                let as_of = Antichain::from_iter(as_of.iter().map(|x| T::decode(*x)));
                FetchBatchFilter::Snapshot { as_of }
            }
            SerdeLeasedBatchPartMetadata::Listen { as_of, lower } => {
                let as_of = Antichain::from_iter(as_of.iter().map(|x| T::decode(*x)));
                let lower = Antichain::from_iter(lower.iter().map(|x| T::decode(*x)));
                FetchBatchFilter::Listen { as_of, lower }
            }
        }
    }
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
    part: &LeasedBatchPart<T>,
    blob: &(dyn Blob + Send + Sync),
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
    let encoded_part = fetch_batch_part(
        &part.shard_id,
        blob,
        &metrics,
        shard_metrics,
        read_metrics,
        &part.key,
        &part.desc,
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
    FetchedPart::new(
        metrics,
        encoded_part,
        schemas,
        &part.metadata,
        part.filter_pushdown_audit,
        part.stats.as_ref(),
    )
}

pub(crate) async fn fetch_batch_part_blob(
    shard_id: &ShardId,
    blob: &(dyn Blob + Send + Sync),
    metrics: &Metrics,
    shard_metrics: &ShardMetrics,
    read_metrics: &ReadMetrics,
    key: &PartialBatchKey,
) -> Result<SegmentedBytes, BlobKey> {
    let now = Instant::now();
    let get_span = debug_span!("fetch_batch::get");
    let blob_key = key.complete(shard_id);
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
    key: &str,
    registered_desc: Description<T>,
    value: &SegmentedBytes,
) -> EncodedPart<T>
where
    T: Timestamp + Lattice + Codec64,
{
    trace_span!("fetch_batch::decode").in_scope(|| {
        let part = metrics
            .codecs
            .batch
            .decode(|| BlobTraceBatchPart::decode(value, &metrics.columnar))
            .map_err(|err| anyhow!("couldn't decode batch at key {}: {}", key, err))
            // We received a State that we couldn't decode. This could happen if
            // persist messes up backward/forward compatibility, if the durable
            // data was corrupted, or if operations messes up deployment. In any
            // case, fail loudly.
            .expect("internal error: invalid encoded state");
        read_metrics.part_goodbytes.inc_by(u64::cast_from(
            part.updates.iter().map(|x| x.goodbytes()).sum::<usize>(),
        ));
        EncodedPart::new(key, registered_desc, part)
    })
}

pub(crate) async fn fetch_batch_part<T>(
    shard_id: &ShardId,
    blob: &(dyn Blob + Send + Sync),
    metrics: &Metrics,
    shard_metrics: &ShardMetrics,
    read_metrics: &ReadMetrics,
    key: &PartialBatchKey,
    registered_desc: &Description<T>,
) -> Result<EncodedPart<T>, BlobKey>
where
    T: Timestamp + Lattice + Codec64,
{
    let value =
        fetch_batch_part_blob(shard_id, blob, metrics, shard_metrics, read_metrics, key).await?;
    let part = decode_batch_part_blob(metrics, read_metrics, key, registered_desc.clone(), &value);
    Ok(part)
}

/// Propagates metadata from readers alongside a `HollowBatch` to apply the
/// desired semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum SerdeLeasedBatchPartMetadata {
    /// Apply snapshot-style semantics to the fetched batch part.
    Snapshot {
        /// Return all values with time leq `as_of`.
        as_of: Vec<[u8; 8]>,
    },
    /// Apply listen-style semantics to the fetched batch part.
    Listen {
        /// Return all values with time in advance of `as_of`.
        as_of: Vec<[u8; 8]>,
        /// Return all values with `lower` leq time.
        lower: Vec<[u8; 8]>,
    },
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
///   [`Self::into_exchangeable_part`]. [`SerdeLeasedBatchPart`] is exchangeable,
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
    pub(crate) metadata: SerdeLeasedBatchPartMetadata,
    pub(crate) desc: Description<T>,
    pub(crate) key: PartialBatchKey,
    pub(crate) encoded_size_bytes: usize,
    /// The `SeqNo` from which this part originated; we track this value as
    /// long as necessary to ensure the `SeqNo` isn't garbage collected while a
    /// read still depends on it.
    pub(crate) leased_seqno: Option<SeqNo>,
    pub(crate) stats: Option<LazyPartStats>,
    pub(crate) filter_pushdown_audit: bool,
    /// A lower bound on the key. If a tight lower bound is not available, the
    /// empty vec (as the minimum vec) is a conservative choice.
    pub(crate) key_lower: Vec<u8>,
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
    /// This semantically transfers the lease to the returned
    /// SerdeLeasedBatchPart. If `self` has a `leased_seqno`, failing to take
    /// the returned `SerdeLeasedBatchPart` back into a `LeasedBatchPart` will
    /// leak `SeqNo`s and prevent persist GC.
    pub fn into_exchangeable_part(mut self) -> SerdeLeasedBatchPart {
        let r = SerdeLeasedBatchPart {
            shard_id: self.shard_id,
            metadata: self.metadata.clone(),
            lower: self.desc.lower().iter().map(T::encode).collect(),
            upper: self.desc.upper().iter().map(T::encode).collect(),
            since: self.desc.since().iter().map(T::encode).collect(),
            key: self.key.clone(),
            encoded_size_bytes: self.encoded_size_bytes,
            leased_seqno: self.leased_seqno,
            reader_id: self.reader_id.clone(),
            stats: self.stats.clone(),
            filter_pushdown_audit: self.filter_pushdown_audit,
            key_lower: std::mem::take(&mut self.key_lower),
        };
        // If `x` has a lease, we've effectively transferred it to `r`.
        let _ = self.leased_seqno.take();
        r
    }

    /// Because sources get dropped without notice, we need to permit another
    /// operator to safely expire leases.
    ///
    /// The part's `reader_id` is intentionally inaccessible, and should
    /// be obtained from the issuing [`crate::ReadHandle`], or one of its derived
    /// structures, e.g. [`crate::read::Subscribe`].
    ///
    /// # Panics
    /// - If `reader_id` is different than the [`LeasedReaderId`] from
    ///   the part issuer.
    pub(crate) fn return_lease(&mut self, reader_id: &LeasedReaderId) -> Option<SeqNo> {
        assert!(
            &self.reader_id == reader_id,
            "only issuing reader can authorize lease expiration"
        );
        self.leased_seqno.take()
    }

    /// The encoded size of this part in bytes
    pub fn encoded_size_bytes(&self) -> usize {
        self.encoded_size_bytes
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
        self.stats.as_ref().map(|x| x.decode())
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
    key: String,
    metrics: Arc<Metrics>,
    read_metrics: ReadMetrics,
    registered_desc: Description<T>,
    part: SegmentedBytes,
    schemas: Schemas<K, V>,
    metadata: SerdeLeasedBatchPartMetadata,
    filter_pushdown_audit: bool,
    stats: Option<LazyPartStats>,
    _phantom: PhantomData<fn() -> D>,
}

impl<K: Codec, V: Codec, T: Clone, D> Clone for FetchedBlob<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            metrics: Arc::clone(&self.metrics),
            read_metrics: self.read_metrics.clone(),
            registered_desc: self.registered_desc.clone(),
            part: self.part.clone(),
            schemas: self.schemas.clone(),
            metadata: self.metadata.clone(),
            filter_pushdown_audit: self.filter_pushdown_audit.clone(),
            stats: self.stats.clone(),
            _phantom: self._phantom.clone(),
        }
    }
}

impl<K: Codec, V: Codec, T: Timestamp + Lattice + Codec64, D> FetchedBlob<K, V, T, D> {
    /// Partially decodes this blob into a [FetchedPart].
    pub fn parse(&self) -> FetchedPart<K, V, T, D> {
        let part = decode_batch_part_blob(
            &self.metrics,
            &self.read_metrics,
            &self.key,
            self.registered_desc.clone(),
            &self.part,
        );
        FetchedPart::new(
            Arc::clone(&self.metrics),
            part,
            self.schemas.clone(),
            &self.metadata,
            self.filter_pushdown_audit,
            self.stats.as_ref(),
        )
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
        metadata: &SerdeLeasedBatchPartMetadata,
        filter_pushdown_audit: bool,
        stats: Option<&LazyPartStats>,
    ) -> Self {
        let ts_filter = FetchBatchFilter::new(metadata);
        let filter_pushdown_audit = if filter_pushdown_audit {
            stats.cloned()
        } else {
            None
        };
        FetchedPart {
            metrics,
            ts_filter,
            part,
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
    registered_desc: Description<T>,
    part: Arc<BlobTraceBatchPart<T>>,
    needs_truncation: bool,
}

impl<K, V, T, D> FetchedPart<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    /// [Self::next] but optionally providing a `K` and `V` for alloc reuse.
    pub fn next_with_storage(
        &mut self,
        key: &mut Option<K>,
        val: &mut Option<V>,
    ) -> Option<((Result<K, String>, Result<V, String>), T, D)> {
        while let Some((k, v, mut t, d)) = self.part_cursor.pop(&self.part) {
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
            return Some(((k, v), t, d));
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
        self.next_with_storage(&mut None, &mut None)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // We don't know in advance how restrictive the filter will be.
        let max_len = self.part.part.updates.iter().map(|x| x.len()).sum();
        (0, Some(max_len))
    }
}

impl<T> EncodedPart<T>
where
    T: Timestamp + Lattice + Codec64,
{
    pub(crate) fn new(
        key: &str,
        registered_desc: Description<T>,
        part: BlobTraceBatchPart<T>,
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
        let inline_desc = &part.desc;
        let needs_truncation = inline_desc.lower() != registered_desc.lower()
            || inline_desc.upper() != registered_desc.upper();
        if needs_truncation {
            assert!(
                PartialOrder::less_equal(inline_desc.lower(), registered_desc.lower()),
                "key={} inline={:?} registered={:?}",
                key,
                inline_desc,
                registered_desc
            );
            assert!(
                PartialOrder::less_equal(registered_desc.upper(), inline_desc.upper()),
                "key={} inline={:?} registered={:?}",
                key,
                inline_desc,
                registered_desc
            );
            // As mentioned above, batches that needs truncation will always have a
            // since of the minimum timestamp. Technically we could truncate any
            // batch where the since is less_than the output_desc's lower, but we're
            // strict here so we don't get any surprises.
            assert_eq!(
                inline_desc.since(),
                &Antichain::from_elem(T::minimum()),
                "key={} inline={:?} registered={:?}",
                key,
                inline_desc,
                registered_desc
            );
        } else {
            assert_eq!(
                inline_desc, &registered_desc,
                "key={} inline={:?} registered={:?}",
                key, inline_desc, registered_desc
            );
        }

        EncodedPart {
            registered_desc,
            part: Arc::new(part),
            needs_truncation,
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
    part_idx: usize,
    idx: usize,
}

impl Cursor {
    /// A cursor points to a particular update in the backing part data.
    /// If the update it points to is not valid, advance it to the next valid update
    /// if there is one, and return the pointed-to data.
    pub fn peek<'a, T: Timestamp + Codec64>(
        &mut self,
        encoded: &'a EncodedPart<T>,
    ) -> Option<(&'a [u8], &'a [u8], T, [u8; 8])> {
        while let Some(part) = encoded.part.updates.get(self.part_idx) {
            let ((k, v), t, d) = match part.get(self.idx) {
                Some(x) => x,
                None => {
                    self.part_idx += 1;
                    self.idx = 0;
                    continue;
                }
            };

            let t = T::decode(t);

            // This filtering is really subtle, see the comment above for
            // what's going on here.
            let truncated_t = encoded.needs_truncation && {
                !encoded.registered_desc.lower().less_equal(&t)
                    || encoded.registered_desc.upper().less_equal(&t)
            };
            if truncated_t {
                self.idx += 1;
                continue;
            }
            return Some((k, v, t, d));
        }
        None
    }

    /// Similar to peek, but advance the cursor just past the end of the most recent update.
    pub fn pop<'a, T: Timestamp + Codec64>(
        &mut self,
        part: &'a EncodedPart<T>,
    ) -> Option<(&'a [u8], &'a [u8], T, [u8; 8])> {
        let update = self.peek(part);
        if update.is_some() {
            self.idx += 1;
        }
        update
    }

    /// Advance the cursor just past the end of the most recent update, if there is one.
    pub fn advance<'a, T: Timestamp + Codec64>(&mut self, part: &'a EncodedPart<T>) {
        if self.part_idx < part.part.updates.len() {
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
    shard_id: ShardId,
    metadata: SerdeLeasedBatchPartMetadata,
    lower: Vec<[u8; 8]>,
    upper: Vec<[u8; 8]>,
    since: Vec<[u8; 8]>,
    key: PartialBatchKey,
    encoded_size_bytes: usize,
    leased_seqno: Option<SeqNo>,
    reader_id: LeasedReaderId,
    stats: Option<LazyPartStats>,
    filter_pushdown_audit: bool,
    key_lower: Vec<u8>,
}

impl SerdeLeasedBatchPart {
    /// Returns the encoded size of the given part.
    pub fn encoded_size_bytes(&self) -> usize {
        self.encoded_size_bytes
    }
}

impl<T: Timestamp + Codec64> LeasedBatchPart<T> {
    /// Takes a [`SerdeLeasedBatchPart`] into a [`LeasedBatchPart`].
    ///
    /// Note that this process in non-commutative with
    /// [LeasedBatchPart::into_exchangeable_part]. The `LeasedBatchPart` that
    /// this function generates is never droppable. However, the value generated
    /// by `LeasedBatchPart::into_exchangeable_part` inherits the
    /// `LeasedBatchPart`'s droppability.
    ///
    /// For more details, see [`LeasedBatchPart`]'s documentation.
    pub(crate) fn from(x: SerdeLeasedBatchPart, metrics: Arc<Metrics>) -> Self {
        LeasedBatchPart {
            metrics,
            shard_id: x.shard_id,
            metadata: x.metadata,
            desc: Description::new(
                Antichain::from_iter(x.lower.into_iter().map(T::decode)),
                Antichain::from_iter(x.upper.into_iter().map(T::decode)),
                Antichain::from_iter(x.since.into_iter().map(T::decode)),
            ),
            key: x.key,
            encoded_size_bytes: x.encoded_size_bytes,
            leased_seqno: x.leased_seqno,
            reader_id: x.reader_id,
            stats: x.stats,
            filter_pushdown_audit: x.filter_pushdown_audit,
            key_lower: x.key_lower,
        }
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
