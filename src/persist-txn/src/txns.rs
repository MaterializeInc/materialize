// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interface for atomic multi-shard writes.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use mz_ore::instrument;
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::{Codec, Codec64, Opaque, StepForward};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tracing::debug;

use crate::metrics::Metrics;
use crate::txn_cache::{TxnsCache, Unapplied};
use crate::txn_write::Txn;
use crate::{TxnsCodec, TxnsCodecDefault, TxnsEntry};

/// An interface for atomic multi-shard writes.
///
/// This handle is acquired through [Self::open]. Any data shards must be
/// registered with [Self::register] before use. Transactions are then started
/// with [Self::begin].
///
/// # Implementation Details
///
/// The structure of the txns shard is `(ShardId, Vec<u8>)` updates.
///
/// The core mechanism is that a txn commits a set of transmittable persist
/// _batch handles_ as `(ShardId, <opaque blob>)` pairs at a single timestamp.
/// This contractually both commits the txn and advances the logical upper of
/// _every_ data shard (not just the ones involved in the txn).
///
/// Example:
///
/// ```text
/// // A txn to only d0 at ts=1
/// (d0, <opaque blob A>, 1, 1)
/// // A txn to d0 (two blobs) and d1 (one blob) at ts=4
/// (d0, <opaque blob B>, 4, 1)
/// (d0, <opaque blob C>, 4, 1)
/// (d1, <opaque blob D>, 4, 1)
/// ```
///
/// However, the new commit is not yet readable until the txn apply has run,
/// which is expected to be promptly done by the committer, except in the event
/// of a crash. This, in ts order, moves the batch handles into the data shards
/// with a [compare_and_append_batch] (similar to how the multi-worker
/// persist_sink works).
///
/// [compare_and_append_batch]:
///     mz_persist_client::write::WriteHandle::compare_and_append_batch
///
/// Once apply is run, we "tidy" the txns shard by retracting the update adding
/// the batch. As a result, the contents of the txns shard at any given
/// timestamp is exactly the set of outstanding apply work (plus registrations,
/// see below).
///
/// Example (building on the above):
///
/// ```text
/// // Tidy for the first txn at ts=3
/// (d0, <opaque blob A>, 3, -1)
/// // Tidy for the second txn (the timestamps can be different for each
/// // retraction in a txn, but don't need to be)
/// (d0, <opaque blob B>, 5, -1)
/// (d0, <opaque blob C>, 6, -1)
/// (d1, <opaque blob D>, 6, -1)
/// ```
///
/// To make it easy to reason about exactly which data shards are registered in
/// the txn set at any given moment, the data shard is added to the set with a
/// `(ShardId, <empty>)` pair. The data may not be read before the timestamp of
/// the update (which starts at the time it was initialized, but it may later be
/// forwarded).
///
/// Example (building on both of the above):
///
/// ```text
/// // d0 and d1 were both initialized before they were used above
/// (d0, <empty>, 0, 1)
/// (d1, <empty>, 2, 1)
/// ```
#[derive(Debug)]
pub struct TxnsHandle<K, V, T, D, O = u64, C = TxnsCodecDefault>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
    O: Opaque + Debug + Codec64,
    C: TxnsCodec,
{
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) txns_cache: TxnsCache<T, C>,
    pub(crate) txns_write: WriteHandle<C::Key, C::Val, T, i64>,
    pub(crate) txns_since: SinceHandle<C::Key, C::Val, T, i64, O>,
    pub(crate) datas: DataHandles<K, V, T, D>,
}

impl<K, V, T, D, O, C> TxnsHandle<K, V, T, D, O, C>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
    O: Opaque + Debug + Codec64,
    C: TxnsCodec,
{
    /// Returns a [TxnsHandle] committing to the given txn shard.
    ///
    /// `txns_id` identifies which shard will be used as the txns WAL. MZ will
    /// likely have one of these per env, used by all processes and the same
    /// across restarts.
    ///
    /// This also does any (idempotent) initialization work: i.e. ensures that
    /// the txn shard is readable at `init_ts` by appending an empty batch, if
    /// necessary.
    pub async fn open(
        init_ts: T,
        client: PersistClient,
        metrics: Arc<Metrics>,
        txns_id: ShardId,
        // TODO(txn): Get rid of these by introducing a SchemalessWriteHandle to
        // persist.
        key_schema: Arc<K::Schema>,
        val_schema: Arc<V::Schema>,
    ) -> Self {
        let (txns_key_schema, txns_val_schema) = C::schemas();
        let (mut txns_write, txns_read) = client
            .open(
                txns_id,
                Arc::new(txns_key_schema),
                Arc::new(txns_val_schema),
                Diagnostics {
                    shard_name: "txns".to_owned(),
                    handle_purpose: "commit txns".to_owned(),
                },
            )
            .await
            .expect("txns schema shouldn't change");
        let txns_since = client
            .open_critical_since(
                txns_id,
                // TODO: We likely need to use a different critical reader id
                // for this if we want to be able to introspect it via SQL.
                PersistClient::CONTROLLER_CRITICAL_SINCE,
                Diagnostics {
                    shard_name: "txns".to_owned(),
                    handle_purpose: "commit txns".to_owned(),
                },
            )
            .await
            .expect("txns schema shouldn't change");
        let txns_cache = TxnsCache::init(init_ts, txns_read, &mut txns_write).await;
        TxnsHandle {
            metrics,
            txns_cache,
            txns_write,
            txns_since,
            datas: DataHandles {
                client,
                data_write: BTreeMap::new(),
                key_schema,
                val_schema,
            },
        }
    }

    /// Returns a new, empty transaction that can involve the data shards
    /// registered with this handle.
    pub fn begin(&self) -> Txn<K, V, D> {
        // TODO: This is a method on the handle because we'll need WriteHandles
        // once we start spilling to s3.
        Txn::new()
    }

    /// Registers data shards for use with this txn set.
    ///
    /// A registration entry is written to the txn shard. If it is not possible
    /// to register the data at the requested time, an Err will be returned with
    /// the minimum time the data shards could be registered.
    ///
    /// This method is idempotent. Data shards currently registered at
    /// `register_ts` will not be registered a second time. Specifically, this
    /// method will return success when the most recent register ts `R` is
    /// less_equal to `register_ts` AND there is no forget ts between `R` and
    /// `register_ts`.
    ///
    /// As a side effect all txns <= register_ts are applied, including the
    /// registration itself.
    ///
    /// **WARNING!** While a data shard is registered to the txn set, writing to
    /// it directly (i.e. using a WriteHandle instead of the TxnHandle,
    /// registering it with another txn shard) will lead to incorrectness,
    /// undefined behavior, and (potentially sticky) panics.
    #[instrument(level = "debug", fields(ts = ?register_ts))]
    pub async fn register(
        &mut self,
        register_ts: T,
        data_writes: impl IntoIterator<Item = WriteHandle<K, V, T, D>>,
    ) -> Result<Tidy, T> {
        let op = &Arc::clone(&self.metrics).register;
        op.run(async {
            let data_writes = data_writes.into_iter().collect::<Vec<_>>();
            let updates = data_writes
                .iter()
                .map(|data_write| {
                    let data_id = data_write.shard_id();
                    let entry = TxnsEntry::Register(data_id, T::encode(&register_ts));
                    (data_id, C::encode(entry))
                })
                .collect::<Vec<_>>();
            let data_ids_debug = || {
                data_writes
                    .iter()
                    .map(|x| format!("{:.9}", x.shard_id().to_string()))
                    .collect::<Vec<_>>()
                    .join(" ")
            };

            let mut txns_upper = self
                .txns_write
                .shared_upper()
                .into_option()
                .expect("txns should not be closed");
            loop {
                self.txns_cache.update_ge(&txns_upper).await;
                // Figure out which are still unregistered as of `txns_upper`. Below
                // we write conditionally on the upper being what we expect so than
                // we can re-run this if anything changes from underneath us.
                let updates = updates
                    .iter()
                    .flat_map(|(data_id, (key, val))| {
                        let registered = self.txns_cache.registered_at(data_id, &txns_upper);
                        (!registered).then_some(((key, val), &register_ts, 1))
                    })
                    .collect::<Vec<_>>();
                // If the txns_upper has passed register_ts, we can no longer write.
                if register_ts < txns_upper {
                    debug!(
                        "txns register {} at {:?} mismatch current={:?}",
                        data_ids_debug(),
                        register_ts,
                        txns_upper,
                    );
                    return Err(txns_upper);
                }

                let res = crate::small_caa(
                    || format!("txns register {}", data_ids_debug()),
                    &mut self.txns_write,
                    &updates,
                    txns_upper,
                    register_ts.step_forward(),
                )
                .await;
                match res {
                    Ok(()) => {
                        debug!(
                            "txns register {} at {:?} success",
                            data_ids_debug(),
                            register_ts
                        );
                        break;
                    }
                    Err(new_txns_upper) => {
                        self.metrics.register.retry_count.inc();
                        txns_upper = new_txns_upper;
                        continue;
                    }
                }
            }
            for data_write in data_writes {
                self.datas
                    .data_write
                    .insert(data_write.shard_id(), data_write);
            }
            let tidy = self.apply_le(&register_ts).await;

            Ok(tidy)
        })
        .await
    }

    /// Removes data shards from use with this txn set.
    ///
    /// The registration entry written to the txn shard is retracted. If it is
    /// not possible to forget the data shard at the requested time, an Err will
    /// be returned with the minimum time the data shards could be forgotten.
    ///
    /// This method is idempotent. Data shards currently forgotten at
    /// `forget_ts` will not be forgotten a second time. Specifically, this
    /// method will return success when the most recent forget ts (if any) `F`
    /// is less_equal to `forget_ts` AND there is no register ts between `F` and
    /// `forget_ts`.
    ///
    /// As a side effect all txns <= forget_ts are applied, including the
    /// forget itself.
    ///
    /// **WARNING!** While a data shard is registered to the txn set, writing to
    /// it directly (i.e. using a WriteHandle instead of the TxnHandle,
    /// registering it with another txn shard) will lead to incorrectness,
    /// undefined behavior, and (potentially sticky) panics.
    #[instrument(level = "debug", fields(ts = ?forget_ts, shard = %data_id))]
    pub async fn forget(&mut self, forget_ts: T, data_id: ShardId) -> Result<Tidy, T> {
        let op = &Arc::clone(&self.metrics).forget;
        op.run(async {
            let mut txns_upper = self
                .txns_write
                .shared_upper()
                .into_option()
                .expect("txns should not be closed");
            loop {
                self.txns_cache.update_ge(&txns_upper).await;

                let (key, val) = C::encode(TxnsEntry::Register(data_id, T::encode(&forget_ts)));
                let updates = if self.txns_cache.registered_at(&data_id, &txns_upper) {
                    vec![((&key, &val), &forget_ts, -1)]
                } else {
                    // Never registered or already forgotten. This could change in
                    // `[txns_upper, forget_ts]` (due to races) so close off that
                    // interval before returning, just don't write any updates.
                    vec![]
                };

                // If the txns_upper has passed forget_ts, we can no longer write.
                if forget_ts < txns_upper {
                    debug!(
                        "txns forget {:.9} at {:?} mismatch current={:?}",
                        data_id.to_string(),
                        forget_ts,
                        txns_upper,
                    );
                    return Err(txns_upper);
                }

                // Ensure the latest write has been applied, so we don't run into
                // any issues trying to apply it later.
                //
                // NB: It's _very_ important for correctness to get this from the
                // unapplied batches (which compact themselves naturally) and not
                // from the writes (which are artificially compacted based on when
                // we need reads for).
                let data_latest_unapplied = self
                    .txns_cache
                    .unapplied_batches
                    .values()
                    .rev()
                    .find(|(x, _, _)| x == &data_id);
                if let Some((_, _, latest_write)) = data_latest_unapplied {
                    debug!(
                        "txns forget {:.9} applying latest write {:?}",
                        data_id.to_string(),
                        latest_write,
                    );
                    let latest_write = latest_write.clone();
                    let _tidy = self.apply_le(&latest_write).await;
                }
                let res = crate::small_caa(
                    || format!("txns forget {:.9}", data_id.to_string()),
                    &mut self.txns_write,
                    &updates,
                    txns_upper,
                    forget_ts.step_forward(),
                )
                .await;
                match res {
                    Ok(()) => {
                        debug!(
                            "txns forget {:.9} at {:?} success",
                            data_id.to_string(),
                            forget_ts
                        );
                        break;
                    }
                    Err(new_txns_upper) => {
                        self.metrics.forget.retry_count.inc();
                        txns_upper = new_txns_upper;
                        continue;
                    }
                }
            }

            // Note: Ordering here matters, we want to generate the Tidy work _before_ removing the
            // handle because the work will create a handle to the shard.
            let tidy = self.apply_le(&forget_ts).await;
            self.datas.data_write.remove(&data_id);

            Ok(tidy)
        })
        .await
    }

    /// Forgets, at the given timestamp, every data shard that is registered.
    /// Returns the ids of the forgotten shards. See [Self::forget].
    #[instrument(level = "debug", fields(ts = ?forget_ts))]
    pub async fn forget_all(&mut self, forget_ts: T) -> Result<(Vec<ShardId>, Tidy), T> {
        let op = &Arc::clone(&self.metrics).forget_all;
        op.run(async {
            let mut txns_upper = self
                .txns_write
                .shared_upper()
                .into_option()
                .expect("txns should not be closed");
            let registered = loop {
                self.txns_cache.update_ge(&txns_upper).await;

                let registered = self.txns_cache.all_registered_at(&txns_upper);
                let data_ids_debug = || {
                    registered
                        .iter()
                        .map(|x| format!("{:.9}", x.to_string()))
                        .collect::<Vec<_>>()
                        .join(" ")
                };
                let updates = registered
                    .iter()
                    .map(|data_id| {
                        C::encode(crate::TxnsEntry::Register(*data_id, T::encode(&forget_ts)))
                    })
                    .collect::<Vec<_>>();
                let updates = updates
                    .iter()
                    .map(|(key, val)| ((key, val), &forget_ts, -1))
                    .collect::<Vec<_>>();

                // If the txns_upper has passed forget_ts, we can no longer write.
                if forget_ts < txns_upper {
                    debug!(
                        "txns forget_all {} at {:?} mismatch current={:?}",
                        data_ids_debug(),
                        forget_ts,
                        txns_upper,
                    );
                    return Err(txns_upper);
                }

                // Ensure the latest write has been applied, so we don't run into
                // any issues trying to apply it later.
                //
                // NB: It's _very_ important for correctness to get this from the
                // unapplied batches (which compact themselves naturally) and not
                // from the writes (which are artificially compacted based on when
                // we need reads for).
                let data_latest_unapplied = self.txns_cache.unapplied_batches.values().last();
                if let Some((_, _, latest_write)) = data_latest_unapplied {
                    debug!(
                        "txns forget_all {} applying latest write {:?}",
                        data_ids_debug(),
                        latest_write,
                    );
                    let latest_write = latest_write.clone();
                    let _tidy = self.apply_le(&latest_write).await;
                }
                let res = crate::small_caa(
                    || format!("txns forget_all {}", data_ids_debug()),
                    &mut self.txns_write,
                    &updates,
                    txns_upper,
                    forget_ts.step_forward(),
                )
                .await;
                match res {
                    Ok(()) => {
                        debug!(
                            "txns forget_all {} at {:?} success",
                            data_ids_debug(),
                            forget_ts
                        );
                        break registered;
                    }
                    Err(new_txns_upper) => {
                        self.metrics.forget_all.retry_count.inc();
                        txns_upper = new_txns_upper;
                        continue;
                    }
                }
            };

            for data_id in registered.iter() {
                self.datas.data_write.remove(data_id);
            }
            let tidy = self.apply_le(&forget_ts).await;

            Ok((registered, tidy))
        })
        .await
    }

    /// "Applies" all committed txns <= the given timestamp, ensuring that reads
    /// at that timestamp will not block.
    ///
    /// In the common case, the txn committer will have done this work and this
    /// method will be a no-op, but it is not guaranteed. In the event of a
    /// crash or race, this does whatever persist writes are necessary (and
    /// returns the resulting maintenance work), which could be significant.
    ///
    /// If the requested timestamp has not yet been written, this could block
    /// for an unbounded amount of time.
    ///
    /// This method is idempotent.
    #[instrument(level = "debug", fields(ts = ?ts))]
    pub async fn apply_le(&mut self, ts: &T) -> Tidy {
        let op = &self.metrics.apply_le;
        op.run(async {
            debug!("apply_le {:?}", ts);
            self.txns_cache.update_gt(ts).await;
            self.txns_cache.update_gauges(&self.metrics);

            let mut unapplied_by_data = BTreeMap::<_, Vec<_>>::new();
            for (data_id, unapplied, unapplied_ts) in self.txns_cache.unapplied() {
                if ts < &unapplied_ts {
                    break;
                }
                unapplied_by_data
                    .entry(*data_id)
                    .or_default()
                    .push((unapplied, unapplied_ts));
            }

            let retractions = FuturesUnordered::new();
            for (data_id, unapplied) in unapplied_by_data {
                let mut data_write = self.datas.take_write(&data_id).await;
                retractions.push(async move {
                    let mut ret = Vec::new();
                    for (unapplied, unapplied_ts) in unapplied {
                        match unapplied {
                            Unapplied::RegisterForget => {
                                let () = crate::empty_caa(
                                    || {
                                        format!(
                                            "data {:.9} register/forget fill",
                                            data_id.to_string()
                                        )
                                    },
                                    &mut data_write,
                                    unapplied_ts.clone(),
                                )
                                .await;
                            }
                            Unapplied::Batch(batch_raw) => {
                                crate::apply_caa(&mut data_write, batch_raw, unapplied_ts.clone())
                                    .await;
                                // NB: Protos are not guaranteed to exactly roundtrip the
                                // encoded bytes, so we intentionally use the raw batch so that
                                // it definitely retracts.
                                ret.push((batch_raw.clone(), (T::encode(unapplied_ts), data_id)));
                            }
                        }
                    }
                    (data_write, ret)
                });
            }
            let retractions = retractions.collect::<Vec<_>>().await;
            let retractions = retractions
                .into_iter()
                .flat_map(|(data_write, retractions)| {
                    self.datas.put_write(data_write);
                    retractions
                })
                .collect();

            // Remove all the applied registers.
            self.txns_cache
                .unapplied_registers
                .retain(|(_, register_ts)| ts < register_ts);

            debug!("apply_le {:?} success", ts);
            Tidy { retractions }
        })
        .await
    }

    /// [Self::apply_le] but also advances the physical upper of every data
    /// shard registered at the timestamp past the timestamp.
    #[instrument(level = "debug", fields(ts = ?ts))]
    pub async fn apply_eager_le(&mut self, ts: &T) -> Tidy {
        let op = &Arc::clone(&self.metrics).apply_eager_le;
        op.run(async {
            // TODO: Ideally the upper advancements would be done concurrently with
            // this apply_le.
            let tidy = self.apply_le(ts).await;

            let data_writes = FuturesUnordered::new();
            for data_id in self.txns_cache.all_registered_at(ts) {
                let mut data_write = self.datas.take_write(&data_id).await;
                let current = data_write.shared_upper();
                let advance_to = ts.step_forward();
                data_writes.push(async move {
                    let empty: &[((K, V), T, D)] = &[];
                    if current.less_than(&advance_to) {
                        let () = data_write
                            .append(empty, current, Antichain::from_elem(advance_to))
                            .await
                            .expect("usage was valid")
                            .expect("nothing before minimum timestamp");
                    }
                    data_write
                });
            }
            for data_write in data_writes.collect::<Vec<_>>().await {
                self.datas.put_write(data_write);
            }

            tidy
        })
        .await
    }

    /// Commits the tidy work at the given time.
    ///
    /// Mostly a helper to make it obvious that we can throw away the apply work
    /// (and not get into an infinite cycle of tidy->apply->tidy).
    #[cfg(test)]
    pub async fn tidy_at(&mut self, tidy_ts: T, tidy: Tidy) -> Result<(), T> {
        debug!("tidy at {:?}", tidy_ts);

        let mut txn = self.begin();
        txn.tidy(tidy);
        // We just constructed this txn, so it couldn't have committed any
        // batches, and thus there's nothing to apply. We're free to throw it
        // away.
        let apply = txn.commit_at(self, tidy_ts.clone()).await?;
        assert!(apply.is_empty());

        debug!("tidy at {:?} success", tidy_ts);
        Ok(())
    }

    /// Allows compaction to the txns shard as well as internal representations,
    /// losing the ability to answer queries about times less_than since_ts.
    ///
    /// In practice, this will likely only be called from the singleton
    /// controller process.
    pub async fn compact_to(&mut self, mut since_ts: T) {
        let op = &self.metrics.compact_to;
        op.run(async {
            tracing::debug!("compact_to {:?}", since_ts);
            // This call to compact the cache only affects the write and
            // registration times, not the unapplied batches. The unapplied batches
            // have a very important correctness invariant to hold, are
            // self-compacting as batches are applied, and are handled below. This
            // means it's always safe to compact the cache past where the txns shard
            // is physically compacted to, so do that regardless of min_unapplied_ts
            // and of whether the maybe_downgrade goes through.
            self.txns_cache.update_gt(&since_ts).await;
            self.txns_cache.compact_to(&since_ts);

            // NB: A critical invariant for how this all works is that we never
            // allow the since of the txns shard to pass any unapplied writes, so
            // reduce it as necessary.
            let min_unapplied_ts = self.txns_cache.min_unapplied_ts();
            if min_unapplied_ts < &since_ts {
                since_ts.clone_from(min_unapplied_ts);
            }
            crate::cads::<T, O, C>(&mut self.txns_since, since_ts).await;
        })
        .await
    }

    /// Returns the [ShardId] of the txns shard.
    pub fn txns_id(&self) -> ShardId {
        self.txns_write.shard_id()
    }

    /// Returns the [TxnsCache] used by this handle.
    pub fn read_cache(&self) -> &TxnsCache<T, C> {
        &self.txns_cache
    }
}

/// A token representing maintenance writes (in particular, retractions) to the
/// txns shard.
///
/// This can be written on its own with `TxnsHandle::tidy_at` or sidecar'd into
/// a normal txn with [Txn::tidy].
#[derive(Debug, Default)]
pub struct Tidy {
    pub(crate) retractions: BTreeMap<Vec<u8>, ([u8; 8], ShardId)>,
}

impl Tidy {
    /// Merges the work represented by the other tidy into this one.
    pub fn merge(&mut self, other: Tidy) {
        self.retractions.extend(other.retractions)
    }
}

/// A helper to make a more targeted mutable borrow of self.
#[derive(Debug)]
pub(crate) struct DataHandles<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    pub(crate) client: PersistClient,
    data_write: BTreeMap<ShardId, WriteHandle<K, V, T, D>>,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
}

impl<K, V, T, D> DataHandles<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    async fn open_data_write(&self, data_id: ShardId) -> WriteHandle<K, V, T, D> {
        self.client
            .open_writer(
                data_id,
                Arc::clone(&self.key_schema),
                Arc::clone(&self.val_schema),
                Diagnostics::from_purpose("txn data"),
            )
            .await
            .expect("schema shouldn't change")
    }

    pub(crate) async fn take_write(&mut self, data_id: &ShardId) -> WriteHandle<K, V, T, D> {
        if let Some(data_write) = self.data_write.remove(data_id) {
            return data_write;
        }
        self.open_data_write(*data_id).await
    }

    pub(crate) fn put_write(&mut self, data_write: WriteHandle<K, V, T, D>) {
        self.data_write.insert(data_write.shard_id(), data_write);
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use differential_dataflow::Hashable;
    use futures::future::BoxFuture;
    use mz_ore::cast::CastFrom;
    use mz_ore::metrics::MetricsRegistry;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::cfg::RetryParameters;
    use mz_persist_client::PersistLocation;
    use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
    use rand::rngs::SmallRng;
    use rand::{RngCore, SeedableRng};
    use timely::progress::Antichain;
    use tokio::sync::oneshot;
    use tracing::{info, info_span, Instrument};

    use crate::operator::DataSubscribe;
    use crate::tests::{reader, write_directly, writer, CommitLog};

    use super::*;

    impl TxnsHandle<String, (), u64, i64, u64, TxnsCodecDefault> {
        pub(crate) async fn expect_open(client: PersistClient) -> Self {
            Self::expect_open_id(client, ShardId::new()).await
        }

        pub(crate) async fn expect_open_id(client: PersistClient, txns_id: ShardId) -> Self {
            Self::open(
                0,
                client,
                Arc::new(Metrics::new(&MetricsRegistry::new())),
                txns_id,
                Arc::new(StringSchema),
                Arc::new(UnitSchema),
            )
            .await
        }

        pub(crate) fn new_log(&self) -> CommitLog {
            CommitLog::new(self.datas.client.clone(), self.txns_id())
        }

        pub(crate) async fn expect_register(&mut self, register_ts: u64) -> ShardId {
            let data_id = ShardId::new();
            self.register(register_ts, [writer(&self.datas.client, data_id).await])
                .await
                .unwrap();
            data_id
        }

        pub(crate) async fn expect_commit_at(
            &mut self,
            commit_ts: u64,
            data_id: ShardId,
            keys: &[&str],
            log: &CommitLog,
        ) -> Tidy {
            let mut txn = self.begin();
            for key in keys {
                txn.write(&data_id, (*key).into(), (), 1).await;
            }
            let tidy = txn
                .commit_at(self, commit_ts)
                .await
                .unwrap()
                .apply(self)
                .await;
            for key in keys {
                log.record((data_id, (*key).into(), commit_ts, 1));
            }
            tidy
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn register_at() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let log = txns.new_log();
        let d0 = txns.expect_register(2).await;

        // Register a second time is a no-op (idempotent).
        txns.register(3, [writer(&client, d0).await]).await.unwrap();

        // Cannot register a new data shard at an already closed off time. An
        // error is returned with the first time that a registration would
        // succeed.
        let d1 = ShardId::new();
        assert_eq!(
            txns.register(2, [writer(&client, d1).await])
                .await
                .unwrap_err(),
            4
        );

        // Can still register after txns have been committed.
        txns.expect_commit_at(4, d0, &["foo"], &log).await;
        txns.register(5, [writer(&client, d1).await]).await.unwrap();

        // We can also register some new and some already registered shards.
        let d2 = ShardId::new();
        txns.register(6, [writer(&client, d0).await, writer(&client, d2).await])
            .await
            .unwrap();

        let () = log.assert_snapshot(d0, 6).await;
        let () = log.assert_snapshot(d1, 6).await;
    }

    /// A sanity check that CommitLog catches an incorrect usage (proxy for a
    /// bug that looks like an incorrect usage).
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    #[should_panic(expected = "left: [(\"foo\", 2, 1)]\n right: [(\"foo\", 2, 2)]")]
    async fn incorrect_usage_register_write_same_time() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let log = txns.new_log();
        let d0 = txns.expect_register(1).await;
        let mut d0_write = writer(&client, d0).await;

        // Commit a write at ts 2...
        let mut txn = txns.begin();
        txn.write(&d0, "foo".into(), (), 1).await;
        let apply = txn.commit_at(&mut txns, 2).await.unwrap();
        log.record_txn(2, &txn);
        // ... and (incorrectly) also write to the shard normally at ts 2.
        let () = d0_write
            .compare_and_append(
                &[(("foo".to_owned(), ()), 2, 1)],
                Antichain::from_elem(2),
                Antichain::from_elem(3),
            )
            .await
            .unwrap()
            .unwrap();
        log.record((d0, "foo".into(), 2, 1));
        apply.apply(&mut txns).await;

        // Verify that CommitLog catches this.
        log.assert_snapshot(d0, 2).await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn forget_at() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let log = txns.new_log();

        // Can forget a data_shard that has not been registered.
        txns.forget(1, ShardId::new()).await.unwrap();

        // Can forget a registered shard.
        let d0 = txns.expect_register(2).await;
        txns.forget(3, d0).await.unwrap();

        // Forget is idempotent.
        txns.forget(4, d0).await.unwrap();

        // Cannot forget at an already closed off time. An error is returned
        // with the first time that a registration would succeed.
        let d1 = txns.expect_register(5).await;
        assert_eq!(txns.forget(5, d1).await.unwrap_err(), 6);

        // Write to txns and to d0 directly.
        let mut d0_write = writer(&client, d0).await;
        txns.expect_commit_at(6, d1, &["d1"], &log).await;
        let updates = [(("d0".to_owned(), ()), 6, 1)];
        d0_write
            .compare_and_append(&updates, d0_write.shared_upper(), Antichain::from_elem(7))
            .await
            .unwrap()
            .unwrap();
        log.record((d0, "d0".into(), 6, 1));

        // Can register and forget an already registered and forgotten shard.
        txns.register(7, [writer(&client, d0).await]).await.unwrap();
        let mut forget_expected = vec![d0, d1];
        forget_expected.sort();
        assert_eq!(txns.forget_all(8).await.unwrap().0, forget_expected);

        // Close shard to writes
        d0_write
            .compare_and_append_batch(&mut [], d0_write.shared_upper(), Antichain::new())
            .await
            .unwrap()
            .unwrap();

        let () = log.assert_snapshot(d0, 8).await;
        let () = log.assert_snapshot(d1, 8).await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn register_forget() {
        async fn step_some_past(subs: &mut Vec<DataSubscribe>, ts: u64) {
            for (idx, sub) in subs.iter_mut().enumerate() {
                // Only step some of them to try to maximize edge cases.
                if usize::cast_from(ts) % (idx + 1) == 0 {
                    async {
                        info!("stepping sub {} past {}", idx, ts);
                        sub.step_past(ts).await;
                    }
                    .instrument(info_span!("sub", idx))
                    .await;
                }
            }
        }

        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let log = txns.new_log();
        let d0 = ShardId::new();
        let mut d0_write = writer(&client, d0).await;
        let mut subs = Vec::new();

        // Loop for a while doing the following:
        // - Write directly to some time before register ts
        // - Register
        // - Write via txns
        // - Forget
        //
        // After each step, make sure that a subscription started at that time
        // works and that all subscriptions can be stepped through the expected
        // timestamp.
        let mut ts = 0;
        while ts < 32 {
            subs.push(txns.read_cache().expect_subscribe(&client, d0, ts));
            ts += 1;
            info!("{} direct", ts);
            txns.begin().commit_at(&mut txns, ts).await.unwrap();
            write_directly(ts, &mut d0_write, &[&format!("d{}", ts)], &log).await;
            step_some_past(&mut subs, ts).await;
            if ts % 11 == 0 {
                txns.compact_to(ts).await;
            }

            subs.push(txns.read_cache().expect_subscribe(&client, d0, ts));
            ts += 1;
            info!("{} register", ts);
            txns.register(ts, [writer(&client, d0).await])
                .await
                .unwrap();
            step_some_past(&mut subs, ts).await;
            if ts % 11 == 0 {
                txns.compact_to(ts).await;
            }

            subs.push(txns.read_cache().expect_subscribe(&client, d0, ts));
            ts += 1;
            info!("{} txns", ts);
            txns.expect_commit_at(ts, d0, &[&format!("t{}", ts)], &log)
                .await;
            step_some_past(&mut subs, ts).await;
            if ts % 11 == 0 {
                txns.compact_to(ts).await;
            }

            subs.push(txns.read_cache().expect_subscribe(&client, d0, ts));
            ts += 1;
            info!("{} forget", ts);
            txns.forget(ts, d0).await.unwrap();
            step_some_past(&mut subs, ts).await;
            if ts % 11 == 0 {
                txns.compact_to(ts).await;
            }
        }

        // Check all the subscribes.
        for mut sub in subs.into_iter() {
            sub.step_past(ts).await;
            log.assert_eq(d0, sub.as_of, sub.progress(), sub.output().clone());
        }
    }

    // Regression test for a bug encountered during initial development:
    // - task 0 commits to a data shard at ts T
    // - before task 0 can unblock T for reads, task 1 tries to register it at
    //   time T (ditto T+X), this does a caa of empty space, advancing the upper
    //   of the data shard to T+1
    // - task 0 attempts to caa in the batch, but finds out the upper is T+1 and
    //   assumes someone else did the work
    // - result: the write is lost
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn race_data_shard_register_and_commit() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let d0 = txns.expect_register(1).await;

        let mut txn = txns.begin();
        txn.write(&d0, "foo".into(), (), 1).await;
        let commit_apply = txn.commit_at(&mut txns, 2).await.unwrap();

        txns.register(3, [writer(&client, d0).await]).await.unwrap();

        // Make sure that we can read empty at the register commit time even
        // before the txn commit apply.
        let actual = txns.txns_cache.expect_snapshot(&client, d0, 1).await;
        assert_eq!(actual, Vec::<String>::new());

        commit_apply.apply(&mut txns).await;
        let actual = txns.txns_cache.expect_snapshot(&client, d0, 2).await;
        assert_eq!(actual, vec!["foo".to_owned()]);
    }

    // A test that applies a batch of writes all at once.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn apply_many_ts() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let log = txns.new_log();
        let d0 = txns.expect_register(1).await;

        for ts in 2..10 {
            let mut txn = txns.begin();
            txn.write(&d0, ts.to_string(), (), 1).await;
            let _apply = txn.commit_at(&mut txns, ts).await.unwrap();
            log.record((d0, ts.to_string(), ts, 1));
        }
        // This automatically runs the apply, which catches up all the previous
        // txns at once.
        txns.expect_commit_at(10, d0, &[], &log).await;

        log.assert_snapshot(d0, 10).await;
    }

    struct StressWorker {
        idx: usize,
        data_ids: Vec<ShardId>,
        txns: TxnsHandle<String, (), u64, i64>,
        log: CommitLog,
        tidy: Tidy,
        ts: u64,
        step: usize,
        rng: SmallRng,
        reads: Vec<(
            oneshot::Sender<u64>,
            ShardId,
            u64,
            mz_ore::task::JoinHandle<Vec<(String, u64, i64)>>,
        )>,
    }

    impl StressWorker {
        pub async fn step(&mut self) {
            debug!(
                "stress {} step {} START ts={}",
                self.idx, self.step, self.ts
            );
            let data_id =
                self.data_ids[usize::cast_from(self.rng.next_u64()) % self.data_ids.len()];
            match self.rng.next_u64() % 5 {
                0 => self.write(data_id).await,
                // The register and forget impls intentionally don't switch on
                // whether it's already registered to stress idempotence.
                1 => self.register(data_id).await,
                2 => self.forget(data_id).await,
                3 => {
                    debug!("stress compact {:.9} to {}", data_id.to_string(), self.ts);
                    self.txns.txns_cache.update_ge(&self.ts).await;
                    self.txns.txns_cache.compact_to(&self.ts)
                }
                4 => self.start_read(data_id),
                _ => unreachable!(""),
            }
            debug!("stress {} step {} DONE ts={}", self.idx, self.step, self.ts);
            self.step += 1;
        }

        fn key(&self) -> String {
            format!("w{}s{}", self.idx, self.step)
        }

        async fn registered_at_ts(&mut self, data_id: ShardId) -> bool {
            self.txns.txns_cache.update_ge(&self.ts).await;
            self.txns.txns_cache.registered_at(&data_id, &self.ts)
        }

        // Writes to the given data shard, either via txns if it's registered or
        // directly if it's not.
        async fn write(&mut self, data_id: ShardId) {
            // Make sure to keep the registered_at_ts call _inside_ the retry
            // loop, because a data shard might switch between registered or not
            // registered as the loop advances through timestamps.
            self.retry_ts_err(&mut |w: &mut StressWorker| {
                Box::pin(async move {
                    if w.registered_at_ts(data_id).await {
                        w.write_via_txns(data_id).await
                    } else {
                        w.write_direct(data_id).await
                    }
                })
            })
            .await
        }

        async fn write_via_txns(&mut self, data_id: ShardId) -> Result<(), u64> {
            debug!(
                "stress write_via_txns {:.9} at {}",
                data_id.to_string(),
                self.ts
            );
            let mut txn = self.txns.begin();
            txn.tidy(std::mem::take(&mut self.tidy));
            txn.write(&data_id, self.key(), (), 1).await;
            let apply = txn.commit_at(&mut self.txns, self.ts).await?;
            debug!(
                "log {:.9} {} at {}",
                data_id.to_string(),
                self.key(),
                self.ts
            );
            self.log.record_txn(self.ts, &txn);
            if self.rng.next_u64() % 3 == 0 {
                self.tidy.merge(apply.apply(&mut self.txns).await);
            }
            Ok(())
        }

        async fn write_direct(&mut self, data_id: ShardId) -> Result<(), u64> {
            debug!(
                "stress write_direct {:.9} at {}",
                data_id.to_string(),
                self.ts
            );
            // First write an empty txn to ensure that the shard isn't
            // registered at this ts by someone else.
            self.txns.begin().commit_at(&mut self.txns, self.ts).await?;

            let mut write = writer(&self.txns.datas.client, data_id).await;
            let mut current = write.shared_upper().into_option().unwrap();
            loop {
                if !(current <= self.ts) {
                    return Err(current);
                }
                let key = self.key();
                let updates = [((&key, &()), &self.ts, 1)];
                let res = crate::small_caa(
                    || format!("data {:.9} direct", data_id.to_string()),
                    &mut write,
                    &updates,
                    current,
                    self.ts + 1,
                )
                .await;
                match res {
                    Ok(()) => {
                        debug!("log {:.9} {} at {}", data_id.to_string(), key, self.ts);
                        self.log.record((data_id, key, self.ts, 1));
                        return Ok(());
                    }
                    Err(new_current) => current = new_current,
                }
            }
        }

        async fn register(&mut self, data_id: ShardId) {
            self.retry_ts_err(&mut |w: &mut StressWorker| {
                debug!("stress register {:.9} at {}", data_id.to_string(), w.ts);
                Box::pin(async move {
                    let data_write = writer(&w.txns.datas.client, data_id).await;
                    let _ = w.txns.register(w.ts, [data_write]).await?;
                    Ok(())
                })
            })
            .await
        }

        async fn forget(&mut self, data_id: ShardId) {
            self.retry_ts_err(&mut |w: &mut StressWorker| {
                debug!("stress forget {:.9} at {}", data_id.to_string(), w.ts);
                Box::pin(async move { w.txns.forget(w.ts, data_id).await.map(|_| ()) })
            })
            .await
        }

        fn start_read(&mut self, data_id: ShardId) {
            debug!(
                "stress start_read {:.9} at {}",
                data_id.to_string(),
                self.ts
            );
            let client = self.txns.datas.client.clone();
            let txns_id = self.txns.txns_id();
            let as_of = self.ts;
            debug!("start_read {:.9} as_of {}", data_id.to_string(), as_of);
            let (tx, mut rx) = oneshot::channel();
            let subscribe = mz_ore::task::spawn_blocking(
                || format!("{:.9}-{}", data_id.to_string(), as_of),
                move || {
                    let mut subscribe = DataSubscribe::new(
                        "test",
                        client,
                        txns_id,
                        data_id,
                        as_of,
                        Antichain::new(),
                    );
                    let data_id = format!("{:.9}", data_id.to_string());
                    let _guard = info_span!("read_worker", %data_id, as_of).entered();
                    loop {
                        subscribe.worker.step_or_park(None);
                        subscribe.capture_output();
                        let until = match rx.try_recv() {
                            Ok(ts) => ts,
                            Err(oneshot::error::TryRecvError::Empty) => {
                                continue;
                            }
                            Err(oneshot::error::TryRecvError::Closed) => 0,
                        };
                        while subscribe.progress() < until {
                            subscribe.worker.step_or_park(None);
                            subscribe.capture_output();
                        }
                        return subscribe.output().clone();
                    }
                },
            );
            self.reads.push((tx, data_id, as_of, subscribe));
        }

        async fn retry_ts_err<W>(&mut self, work_fn: &mut W)
        where
            W: for<'b> FnMut(&'b mut Self) -> BoxFuture<'b, Result<(), u64>>,
        {
            loop {
                match work_fn(self).await {
                    Ok(ret) => return ret,
                    Err(new_ts) => self.ts = new_ts,
                }
            }
        }
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn stress_correctness() {
        const NUM_DATA_SHARDS: usize = 2;
        const NUM_WORKERS: usize = 2;
        const NUM_STEPS_PER_WORKER: usize = 100;
        let seed = UNIX_EPOCH.elapsed().unwrap().hashed();
        eprintln!("using seed {}", seed);

        let mut clients = PersistClientCache::new_no_metrics();
        // We disable pubsub below, so retune the listen retries (pubsub
        // fallback) to keep the test speedy.
        clients
            .cfg()
            .set_next_listen_batch_retryer(RetryParameters {
                fixed_sleep: Duration::ZERO,
                initial_backoff: Duration::from_millis(1),
                multiplier: 1,
                clamp: Duration::from_millis(1),
            });
        let client = clients.open(PersistLocation::new_in_mem()).await.unwrap();
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let log = txns.new_log();
        let data_ids = (0..NUM_DATA_SHARDS)
            .map(|_| ShardId::new())
            .collect::<Vec<_>>();
        let data_writes = data_ids
            .iter()
            .map(|data_id| writer(&client, *data_id))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;
        let _data_sinces = data_ids
            .iter()
            .map(|data_id| reader(&client, *data_id))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;
        let register_ts = 1;
        txns.register(register_ts, data_writes).await.unwrap();

        let mut workers = Vec::new();
        for idx in 0..NUM_WORKERS {
            // Clear the state cache between each client to maximally disconnect
            // them from each other.
            clients.clear_state_cache();
            let client = clients.open(PersistLocation::new_in_mem()).await.unwrap();
            let mut worker = StressWorker {
                idx,
                log: log.clone(),
                txns: TxnsHandle::expect_open_id(client.clone(), txns.txns_id()).await,
                data_ids: data_ids.clone(),
                tidy: Tidy::default(),
                ts: register_ts,
                step: 0,
                rng: SmallRng::seed_from_u64(seed.wrapping_add(u64::cast_from(idx))),
                reads: Vec::new(),
            };
            let worker = async move {
                while worker.step < NUM_STEPS_PER_WORKER {
                    worker.step().await;
                }
                (worker.ts, worker.reads)
            }
            .instrument(info_span!("stress_worker", idx));
            workers.push(mz_ore::task::spawn(|| format!("worker-{}", idx), worker));
        }

        let mut max_ts = 0;
        let mut reads = Vec::new();
        for worker in workers {
            let (t, mut r) = worker.await.unwrap();
            max_ts = std::cmp::max(max_ts, t);
            reads.append(&mut r);
        }

        // Run all of the following in a timeout to make hangs easier to debug.
        tokio::time::timeout(Duration::from_secs(30), async {
            info!("finished with max_ts of {}", max_ts);
            txns.apply_le(&max_ts).await;
            for data_id in data_ids {
                info!("reading data shard {}", data_id);
                log.assert_snapshot(data_id, max_ts)
                    .instrument(info_span!("read_data", data_id = format!("{:.9}", data_id)))
                    .await;
            }
            info!("now waiting for reads {}", max_ts);
            for (tx, data_id, as_of, subscribe) in reads {
                let _ = tx.send(max_ts + 1);
                let output = subscribe.await.unwrap();
                log.assert_eq(data_id, as_of, max_ts + 1, output);
            }
        })
        .await
        .unwrap();
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn advance_physical_uppers_past() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let log = txns.new_log();
        let d0 = txns.expect_register(1).await;
        let mut d0_write = writer(&client, d0).await;
        let d1 = txns.expect_register(2).await;
        let mut d1_write = writer(&client, d1).await;

        assert_eq!(d0_write.fetch_recent_upper().await.elements(), &[2]);
        assert_eq!(d1_write.fetch_recent_upper().await.elements(), &[3]);

        // Normal `apply` (used by expect_commit_at) does not advance the
        // physical upper of data shards that were not involved in the txn (lazy
        // upper). d1 is not involved in this txn so stays where it is.
        txns.expect_commit_at(3, d0, &["0-2"], &log).await;
        assert_eq!(d0_write.fetch_recent_upper().await.elements(), &[4]);
        assert_eq!(d1_write.fetch_recent_upper().await.elements(), &[3]);

        // d0 is not involved in this txn so stays where it is.
        txns.expect_commit_at(4, d1, &["1-3"], &log).await;
        assert_eq!(d0_write.fetch_recent_upper().await.elements(), &[4]);
        assert_eq!(d1_write.fetch_recent_upper().await.elements(), &[5]);

        // But we if use the "eager upper" version of apply, it advances the
        // physical upper of every registered data shard.
        txns.apply_eager_le(&4).await;
        assert_eq!(d0_write.fetch_recent_upper().await.elements(), &[5]);
        assert_eq!(d1_write.fetch_recent_upper().await.elements(), &[5]);

        // This also works even if the txn is empty.
        let apply = txns.begin().commit_at(&mut txns, 5).await.unwrap();
        apply.apply_eager(&mut txns).await;
        assert_eq!(d0_write.fetch_recent_upper().await.elements(), &[6]);
        assert_eq!(d1_write.fetch_recent_upper().await.elements(), &[6]);

        log.assert_snapshot(d0, 5).await;
        log.assert_snapshot(d1, 5).await;
    }
}
