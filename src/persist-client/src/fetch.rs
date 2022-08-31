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

use anyhow::anyhow;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::{debug_span, trace_span, Instrument};

use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::{Blob, SeqNo};
use mz_persist_types::{Codec, Codec64};

use crate::error::InvalidUsage;
use crate::internal::machine::retry_external;
use crate::internal::metrics::Metrics;
use crate::internal::paths::PartialBatchKey;
use crate::read::{ReadHandle, ReaderId};
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
    pub(crate) shard_id: ShardId,

    // Ensures that `BatchFetcher` is of the same type as the `ReadHandle` it's
    // derived from.
    _phantom: PhantomData<(K, V, T, D)>,
}

impl<K, V, T, D> BatchFetcher<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    pub(crate) async fn new(handle: ReadHandle<K, V, T, D>) -> Self {
        let b = BatchFetcher {
            blob: Arc::clone(&handle.blob),
            metrics: Arc::clone(&handle.metrics),
            shard_id: handle.machine.shard_id(),
            _phantom: PhantomData,
        };
        handle.expire().await;
        b
    }

    /// Trade in an exchange-able [LeasedBatchPart] for the data it represents.
    ///
    /// Note to check the `LeasedBatchPart` documentation for how to handle the
    /// returned value.
    pub async fn fetch_leased_part(
        &self,
        part: LeasedBatchPart<T>,
    ) -> (
        LeasedBatchPart<T>,
        Result<Vec<((Result<K, String>, Result<V, String>), T, D)>, InvalidUsage<T>>,
    ) {
        if &part.shard_id != &self.shard_id {
            let batch_shard = part.shard_id.clone();
            return (
                part,
                Err(InvalidUsage::BatchNotFromThisShard {
                    batch_shard,
                    handle_shard: self.shard_id.clone(),
                }),
            );
        }

        let (part, res) = fetch_leased_part(part, self.blob.as_ref(), &self.metrics, None).await;
        (part, Ok(res))
    }
}

enum FetchBatchFilter<T> {
    Snapshot {
        as_of: Antichain<T>,
    },
    Listen {
        as_of: Antichain<T>,
        lower: Antichain<T>,
    },
}

impl<T: Timestamp + Lattice> FetchBatchFilter<T> {
    fn filter_ts(&self, t: &mut T) -> bool {
        match self {
            FetchBatchFilter::Snapshot { as_of } => {
                // This time is covered by a listen
                if as_of.less_than(&t) {
                    return false;
                }
                t.advance_by(as_of.borrow());
                true
            }
            FetchBatchFilter::Listen { as_of, lower } => {
                // This time is covered by a snapshot
                if !as_of.less_than(&t) {
                    return false;
                }

                // Because of compaction, the next batch we get might also
                // contain updates we've already emitted. For example, we
                // emitted `[1, 2)` and then compaction combined that batch with
                // a `[2, 3)` batch into a new `[1, 3)` batch. If this happens,
                // we just need to filter out anything < the frontier. This
                // frontier was the upper of the last batch (and thus exclusive)
                // so for the == case, we still emit.
                if !lower.less_equal(&t) {
                    return false;
                }
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
    part: LeasedBatchPart<T>,
    blob: &(dyn Blob + Send + Sync),
    metrics: &Metrics,
    reader_id: Option<&ReaderId>,
) -> (
    LeasedBatchPart<T>,
    Vec<((Result<K, String>, Result<V, String>), T, D)>,
)
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    let mut updates = Vec::new();
    let ts_filter = match &part.metadata {
        SerdeLeasedBatchPartMetadata::Snapshot { as_of } => {
            let as_of = Antichain::from(as_of.iter().map(|x| T::decode(*x)).collect::<Vec<_>>());
            FetchBatchFilter::Snapshot { as_of }
        }
        SerdeLeasedBatchPartMetadata::Listen { as_of, lower } => {
            let as_of = Antichain::from(as_of.iter().map(|x| T::decode(*x)).collect::<Vec<_>>());
            let lower = Antichain::from(lower.iter().map(|x| T::decode(*x)).collect::<Vec<_>>());
            FetchBatchFilter::Listen { as_of, lower }
        }
    };

    fetch_batch_part(
        &part.shard_id,
        blob,
        &metrics,
        &part.key,
        &part.desc,
        |k, v, mut t, d| {
            if !ts_filter.filter_ts(&mut t) {
                return;
            }

            let k = metrics.codecs.key.decode(|| K::decode(k));
            let v = metrics.codecs.val.decode(|| V::decode(v));
            let d = D::decode(d);
            updates.push(((k, v), t, d));
        },
    )
    .await
    .unwrap_or_else(|err| {
        // Ideally, readers should never encounter a missing blob. They place a seqno
        // hold as they consume their snapshot/listen, preventing any blobs they need
        // from being deleted by garbage collection, and all blob implementations are
        // linearizable so there should be no possibility of stale reads.
        //
        // If we do have a bug and a reader does encounter a missing blob, the state
        // cannot be recovered, and our best option is to panic and retry the whole
        // process.
        panic!(
            "{} could not fetch batch part: {}",
            reader_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "batch fetcher".to_string()),
            err
        )
    });

    (part, updates)
}

pub(crate) async fn fetch_batch_part<T, UpdateFn>(
    shard_id: &ShardId,
    blob: &(dyn Blob + Send + Sync),
    metrics: &Metrics,
    key: &PartialBatchKey,
    registered_desc: &Description<T>,
    mut update_fn: UpdateFn,
) -> Result<(), anyhow::Error>
where
    T: Timestamp + Lattice + Codec64,
    UpdateFn: FnMut(&[u8], &[u8], T, [u8; 8]),
{
    let get_span = debug_span!("fetch_batch::get");
    let value = retry_external(&metrics.retries.external.fetch_batch_get, || async {
        blob.get(&key.complete(shard_id)).await
    })
    .instrument(get_span.clone())
    .await;

    let value = match value {
        Some(v) => v,
        None => {
            return Err(anyhow!(
                "unexpected missing blob: {} for shard: {}",
                key,
                shard_id
            ))
        }
    };
    drop(get_span);

    trace_span!("fetch_batch::decode").in_scope(|| {
        let batch = metrics
            .codecs
            .batch
            .decode(|| BlobTraceBatchPart::decode(&value))
            .map_err(|err| anyhow!("couldn't decode batch at key {}: {}", key, err))
            // We received a State that we couldn't decode. This could happen if
            // persist messes up backward/forward compatibility, if the durable
            // data was corrupted, or if operations messes up deployment. In any
            // case, fail loudly.
            .expect("internal error: invalid encoded state");

        // Drop the encoded representation as soon as we can to reclaim memory.
        drop(value);

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
        let inline_desc = decode_inline_desc(&batch.desc);
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
                &inline_desc, registered_desc,
                "key={} inline={:?} registered={:?}",
                key, inline_desc, registered_desc
            );
        }

        for chunk in batch.updates {
            for ((k, v), t, d) in chunk.iter() {
                let t = T::decode(t);

                // This filtering is really subtle, see the comment above for
                // what's going on here.
                if needs_truncation {
                    if !registered_desc.lower().less_equal(&t) {
                        continue;
                    }
                    if registered_desc.upper().less_equal(&t) {
                        continue;
                    }
                }

                update_fn(k, v, t, d);
            }
        }
    });

    Ok(())
}

// TODO: This goes away the desc on BlobTraceBatchPart becomes a Description<T>,
// which should be a straightforward refactor but it touches a decent bit.
fn decode_inline_desc<T: Timestamp + Codec64>(desc: &Description<u64>) -> Description<T> {
    fn decode_antichain<T: Timestamp + Codec64>(x: &Antichain<u64>) -> Antichain<T> {
        Antichain::from(
            x.elements()
                .iter()
                .map(|x| T::decode(x.to_le_bytes()))
                .collect::<Vec<_>>(),
        )
    }
    Description::new(
        decode_antichain(desc.lower()),
        decode_antichain(desc.upper()),
        decode_antichain(desc.since()),
    )
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
/// - Is consumed through `self.get_droppable_part()`
///
/// In any other circumstance, dropping `LeasedBatchPart` panics.
#[derive(Debug)]
pub struct LeasedBatchPart<T>
where
    T: Timestamp + Codec64,
{
    pub(crate) shard_id: ShardId,
    pub(crate) reader_id: ReaderId,
    pub(crate) metadata: SerdeLeasedBatchPartMetadata,
    pub(crate) desc: Description<T>,
    pub(crate) key: PartialBatchKey,
    /// The `SeqNo` from which this part originated; we track this value as
    /// long as necessary to ensure the `SeqNo` isn't garbage collected while a
    /// read still depends on it.
    pub(crate) leased_seqno: Option<SeqNo>,
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
            leased_seqno: self.leased_seqno,
            reader_id: self.reader_id.clone(),
        };
        // If `x` has a lease, we've effectively transferred it to `r`.
        let _ = self.leased_seqno.take();
        r
    }

    /// Because sources get dropped without notice, we need to permit another
    /// operator to safely expire leases.
    ///
    /// The part's `reader_id` is intentionally inaccessible, and should be
    /// obtained from the issuing [`ReadHandle`], or one of its derived
    /// structures, e.g. [`crate::read::Subscribe`].
    ///
    /// # Panics
    /// - If `reader_id` is different than the [`ReaderId`] from the part
    ///   issuer.
    pub(crate) fn return_lease(&mut self, reader_id: &ReaderId) -> Option<SeqNo> {
        assert!(
            &self.reader_id == reader_id,
            "only issuing reader can authorize lease expiration"
        );
        self.leased_seqno.take()
    }
}

impl<T> Drop for LeasedBatchPart<T>
where
    T: Timestamp + Codec64,
{
    /// For details, see [`LeasedBatchPart`].
    fn drop(&mut self) {
        assert!(
            self.leased_seqno.is_none(),
            "LeasedBatchPart cannot be dropped with lease intact: {:?}",
            self
        );
    }
}

/// This represents the serde encoding for [`LeasedBatchPart`]. We expose the struct
/// itself (unlike other encodable structs) to attempt to provide stricter drop
/// semantics on `LeasedBatchPart`, i.e. `SerdeLeasedBatchPart` is exchangeable
/// (including over the network), where `LeasedBatchPart` is not.
///
/// For more details see documentation and comments on:
/// - [`LeasedBatchPart`]
/// - From<SerdeLeasedBatchPart> for LeasedBatchPart<T>
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SerdeLeasedBatchPart {
    shard_id: ShardId,
    metadata: SerdeLeasedBatchPartMetadata,
    lower: Vec<[u8; 8]>,
    upper: Vec<[u8; 8]>,
    since: Vec<[u8; 8]>,
    key: PartialBatchKey,
    leased_seqno: Option<SeqNo>,
    reader_id: ReaderId,
}

impl<T: Timestamp + Codec64> From<SerdeLeasedBatchPart> for LeasedBatchPart<T> {
    /// Takes a [`SerdeLeasedBatchPart`] into a [`LeasedBatchPart`].
    ///
    /// Note that this process in non-commutative with
    /// [LeasedBatchPart::into_exchangeable_part]. The `LeasedBatchPart` that
    /// this function generates is never droppable. However, the value generated
    /// by `LeasedBatchPart::into_exchangeable_part` inherits the
    /// `LeasedBatchPart`'s droppability.
    ///
    /// For more details, see [`LeasedBatchPart`]'s documentation.
    fn from(x: SerdeLeasedBatchPart) -> Self {
        LeasedBatchPart {
            shard_id: x.shard_id,
            metadata: x.metadata,
            desc: Description::new(
                Antichain::from(x.lower.into_iter().map(T::decode).collect::<Vec<_>>()),
                Antichain::from(x.upper.into_iter().map(T::decode).collect::<Vec<_>>()),
                Antichain::from(x.since.into_iter().map(T::decode).collect::<Vec<_>>()),
            ),
            key: x.key,
            leased_seqno: x.leased_seqno,
            reader_id: x.reader_id,
        }
    }
}

#[test]
fn client_exchange_data() {
    // The whole point of SerdeLeasedBatchPart is that it can be exchanged
    // between timely workers, including over the network. Enforce then that it
    // implements ExchangeData.
    fn is_exchange_data<T: timely::ExchangeData>() {}
    is_exchange_data::<SerdeLeasedBatchPart>();
    is_exchange_data::<SerdeLeasedBatchPart>();
}
