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
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::{debug_span, instrument, trace_span, Instrument};

use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::Blob;
use mz_persist_types::{Codec, Codec64};

use crate::error::InvalidUsage;
use crate::internal::machine::retry_external;
use crate::internal::metrics::Metrics;
use crate::internal::paths::PartialBlobKey;
use crate::read::LeasedBatchMetadata;
use crate::ShardId;

use super::{LeasedBatch, ReadHandle};

/// Capable of fetch batches, and appropriately handling the metadata on
/// [`LeasedBatch`]es.
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
    pub(super) async fn new(handle: ReadHandle<K, V, T, D>) -> Self {
        let b = BatchFetcher {
            blob: Arc::clone(&handle.blob),
            metrics: Arc::clone(&handle.metrics),
            shard_id: handle.machine.shard_id(),
            _phantom: PhantomData,
        };
        handle.expire().await;
        b
    }

    /// Trade in an exchange-able [LeasedBatch] for the data it represents.
    ///
    /// Note to check the `LeasedBatch` documentation for how to handle the
    /// returned value.
    #[instrument(level = "debug", skip_all, fields(shard = %self.shard_id))]
    pub async fn fetch_batch(
        &self,
        mut batch: LeasedBatch<T>,
    ) -> (
        LeasedBatch<T>,
        Result<Vec<((Result<K, String>, Result<V, String>), T, D)>, InvalidUsage<T>>,
    ) {
        // We do this first to express that the batch has been handled in some
        // way and must be returned.
        batch.consume_lease();

        if batch.shard_id != self.shard_id {
            let batch_shard = batch.shard_id.clone();
            return (
                batch,
                Err(InvalidUsage::BatchNotFromThisShard {
                    batch_shard,
                    handle_shard: self.shard_id,
                }),
            );
        }

        let mut updates = Vec::new();
        for key in batch.batch.keys.iter() {
            fetch_batch_part(
                &batch.shard_id,
                self.blob.as_ref(),
                &self.metrics,
                &key,
                &batch.batch.desc.clone(),
                |k, v, mut t, d| {
                    match &batch.metadata {
                        LeasedBatchMetadata::Listen { as_of, until } => {
                            // This time is covered by a snapshot
                            if !as_of.less_than(&t) {
                                return;
                            }

                            // Because of compaction, the next batch we get might also
                            // contain updates we've already emitted. For example, we
                            // emitted `[1, 2)` and then compaction combined that batch
                            // with a `[2, 3)` batch into a new `[1, 3)` batch. If this
                            // happens, we just need to filter out anything < the
                            // frontier. This frontier was the upper of the last batch
                            // (and thus exclusive) so for the == case, we still emit.
                            if !until.less_equal(&t) {
                                return;
                            }
                        }
                        LeasedBatchMetadata::Snapshot { as_of } => {
                            // This time is covered by a listen
                            if as_of.less_than(&t) {
                                return;
                            }
                            t.advance_by(as_of.borrow())
                        }
                    }

                    let k = self.metrics.codecs.key.decode(|| K::decode(k));
                    let v = self.metrics.codecs.val.decode(|| V::decode(v));
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
                panic!("batch fetcher could not fetch batch part: {}", err)
            });
        }

        (batch, Ok(updates))
    }
}

pub(crate) async fn fetch_batch_part<T, UpdateFn>(
    shard_id: &ShardId,
    blob: &(dyn Blob + Send + Sync),
    metrics: &Metrics,
    key: &PartialBlobKey,
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
