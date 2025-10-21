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
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use mz_dyncfg::{Config, ConfigSet, ConfigValHandle};
use mz_ore::collections::HashSet;
use mz_ore::instrument;
use mz_persist_client::batch::Batch;
use mz_persist_client::cfg::USE_CRITICAL_SINCE_TXN;
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::schema::SchemaId;
use mz_persist_types::txn::{TxnsCodec, TxnsEntry};
use mz_persist_types::{Codec, Codec64, Opaque, StepForward};
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use tracing::debug;

use crate::TxnsCodecDefault;
use crate::metrics::Metrics;
use crate::txn_cache::{TxnsCache, Unapplied};
use crate::txn_write::Txn;

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
pub struct TxnsHandle<K: Codec, V: Codec, T, D, O = u64, C: TxnsCodec = TxnsCodecDefault> {
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
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64 + Sync,
    D: Debug + Semigroup + Ord + Codec64 + Send + Sync,
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
        dyncfgs: ConfigSet,
        metrics: Arc<Metrics>,
        txns_id: ShardId,
    ) -> Self {
        let (txns_key_schema, txns_val_schema) = C::schemas();
        client
            .register_schema::<C::Key, C::Val, T, i64>(
                txns_id,
                &txns_key_schema,
                &txns_val_schema,
                Diagnostics {
                    shard_name: "txns".to_owned(),
                    handle_purpose: "txns register schema".to_owned(),
                },
            )
            .await
            .expect("valid usage")
            .expect("valid schema");

        let (mut txns_write, txns_read) = client
            .open(
                txns_id,
                Arc::new(txns_key_schema),
                Arc::new(txns_val_schema),
                Diagnostics {
                    shard_name: "txns".to_owned(),
                    handle_purpose: "commit txns".to_owned(),
                },
                USE_CRITICAL_SINCE_TXN.get(client.dyncfgs()),
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
                dyncfgs,
                client: Arc::new(client),
                data_write_for_apply: BTreeMap::new(),
                data_write_for_commit: BTreeMap::new(),
            },
        }
    }

    /// Returns a new, empty transaction that can involve the data shards
    /// registered with this handle.
    pub fn begin(&self) -> Txn<K, V, T, D> {
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
    ///
    /// # Panics
    ///
    /// This method requires that all writers in `data_writes` have a registered
    /// write schema, and panics otherwise.
    #[instrument(level = "debug", fields(ts = ?register_ts))]
    pub async fn register(
        &mut self,
        register_ts: T,
        data_writes: impl IntoIterator<Item = WriteHandle<K, V, T, D>>,
    ) -> Result<Tidy, T> {
        let op = &Arc::clone(&self.metrics).register;
        op.run(async {
            let data_writes = data_writes.into_iter().collect::<Vec<_>>();

            // Other parts of the txn system assume that all participating data
            // shards have a registered schema, so sanity-check this here.
            for data_write in &data_writes {
                assert!(
                    data_write.schema_id().is_some(),
                    "data shard {} has no schema registered",
                    data_write.shard_id(),
                );
            }

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
                txns_upper = self.txns_cache.update_ge(&txns_upper).await.clone();
                // Figure out which are still unregistered as of `txns_upper`. Below
                // we write conditionally on the upper being what we expect so than
                // we can re-run this if anything changes from underneath us.
                let updates = updates
                    .iter()
                    .flat_map(|(data_id, (key, val))| {
                        let registered =
                            self.txns_cache.registered_at_progress(data_id, &txns_upper);
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
                // If we already have a write handle for a newer version of a table, don't replace
                // it! Currently we only support adding columns to tables with a default value, so
                // the latest/newest schema will always be the most complete.
                //
                // TODO(alter_table): Revisit when we support dropping columns.
                match self.datas.data_write_for_commit.get(&data_write.shard_id()) {
                    None => {
                        self.datas
                            .data_write_for_commit
                            .insert(data_write.shard_id(), DataWriteCommit(data_write));
                    }
                    Some(previous) => {
                        let new_schema_id = data_write.schema_id().expect("checked above");

                        if let Some(prev_schema_id) = previous.schema_id()
                            && prev_schema_id > new_schema_id
                        {
                            mz_ore::soft_panic_or_log!(
                                "tried registering a WriteHandle with an older SchemaId; \
                                 prev_schema_id: {} new_schema_id: {} shard_id: {}",
                                prev_schema_id,
                                new_schema_id,
                                previous.shard_id(),
                            );
                            continue;
                        } else if previous.schema_id().is_none() {
                            mz_ore::soft_panic_or_log!(
                                "encountered data shard without a schema; shard_id: {}",
                                previous.shard_id(),
                            );
                        }

                        tracing::info!(
                            prev_schema_id = ?previous.schema_id(),
                            ?new_schema_id,
                            shard_id = %previous.shard_id(),
                            "replacing WriteHandle"
                        );
                        self.datas
                            .data_write_for_commit
                            .insert(data_write.shard_id(), DataWriteCommit(data_write));
                    }
                }
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
    #[instrument(level = "debug", fields(ts = ?forget_ts))]
    pub async fn forget(
        &mut self,
        forget_ts: T,
        data_ids: impl IntoIterator<Item = ShardId>,
    ) -> Result<Tidy, T> {
        let op = &Arc::clone(&self.metrics).forget;
        op.run(async {
            let data_ids = data_ids.into_iter().collect::<Vec<_>>();
            let mut txns_upper = self
                .txns_write
                .shared_upper()
                .into_option()
                .expect("txns should not be closed");
            loop {
                txns_upper = self.txns_cache.update_ge(&txns_upper).await.clone();

                let data_ids_debug = || {
                    data_ids
                        .iter()
                        .map(|x| format!("{:.9}", x.to_string()))
                        .collect::<Vec<_>>()
                        .join(" ")
                };
                let updates = data_ids
                    .iter()
                    // Never registered or already forgotten. This could change in
                    // `[txns_upper, forget_ts]` (due to races) so close off that
                    // interval before returning, just don't write any updates.
                    .filter(|data_id| self.txns_cache.registered_at_progress(data_id, &txns_upper))
                    .map(|data_id| C::encode(TxnsEntry::Register(*data_id, T::encode(&forget_ts))))
                    .collect::<Vec<_>>();
                let updates = updates
                    .iter()
                    .map(|(key, val)| ((key, val), &forget_ts, -1))
                    .collect::<Vec<_>>();

                // If the txns_upper has passed forget_ts, we can no longer write.
                if forget_ts < txns_upper {
                    debug!(
                        "txns forget {} at {:?} mismatch current={:?}",
                        data_ids_debug(),
                        forget_ts,
                        txns_upper,
                    );
                    return Err(txns_upper);
                }

                // Ensure the latest writes for each shard has been applied, so we don't run into
                // any issues trying to apply it later.
                {
                    let data_ids: HashSet<_> = data_ids.iter().cloned().collect();
                    let data_latest_unapplied = self
                        .txns_cache
                        .unapplied_batches
                        .values()
                        .rev()
                        .find(|(x, _, _)| data_ids.contains(x));
                    if let Some((_, _, latest_write)) = data_latest_unapplied {
                        debug!(
                            "txns forget {} applying latest write {:?}",
                            data_ids_debug(),
                            latest_write,
                        );
                        let latest_write = latest_write.clone();
                        let _tidy = self.apply_le(&latest_write).await;
                    }
                }
                let res = crate::small_caa(
                    || format!("txns forget {}", data_ids_debug()),
                    &mut self.txns_write,
                    &updates,
                    txns_upper,
                    forget_ts.step_forward(),
                )
                .await;
                match res {
                    Ok(()) => {
                        debug!(
                            "txns forget {} at {:?} success",
                            data_ids_debug(),
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
            for data_id in &data_ids {
                self.datas.data_write_for_commit.remove(data_id);
            }

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
                txns_upper = self.txns_cache.update_ge(&txns_upper).await.clone();

                let registered = self.txns_cache.all_registered_at_progress(&txns_upper);
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
                self.datas.data_write_for_commit.remove(data_id);
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
            let _ = self.txns_cache.update_gt(ts).await;
            self.txns_cache.update_gauges(&self.metrics);

            let mut unapplied_by_data = BTreeMap::<_, Vec<_>>::new();
            for (data_id, unapplied, unapplied_ts) in self.txns_cache.unapplied() {
                if ts < unapplied_ts {
                    break;
                }
                unapplied_by_data
                    .entry(*data_id)
                    .or_default()
                    .push((unapplied, unapplied_ts));
            }

            let retractions = FuturesUnordered::new();
            for (data_id, unapplied) in unapplied_by_data {
                let mut data_write = self.datas.take_write_for_apply(&data_id).await;
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
                            Unapplied::Batch(batch_raws) => {
                                let batch_raws = batch_raws
                                    .into_iter()
                                    .map(|batch_raw| batch_raw.as_slice())
                                    .collect();
                                crate::apply_caa(
                                    &mut data_write,
                                    &batch_raws,
                                    unapplied_ts.clone(),
                                )
                                .await;
                                for batch_raw in batch_raws {
                                    // NB: Protos are not guaranteed to exactly roundtrip the
                                    // encoded bytes, so we intentionally use the raw batch so that
                                    // it definitely retracts.
                                    ret.push((
                                        batch_raw.to_vec(),
                                        (T::encode(unapplied_ts), data_id),
                                    ));
                                }
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
                    self.datas.put_write_for_apply(data_write);
                    retractions
                })
                .collect();

            // Remove all the applied registers.
            self.txns_cache.mark_register_applied(ts);

            debug!("apply_le {:?} success", ts);
            Tidy { retractions }
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
            let _ = self.txns_cache.update_gt(&since_ts).await;

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
pub(crate) struct DataHandles<K: Codec, V: Codec, T, D> {
    pub(crate) dyncfgs: ConfigSet,
    pub(crate) client: Arc<PersistClient>,
    /// See [DataWriteApply].
    ///
    /// This is lazily populated with the set of shards touched by `apply_le`.
    data_write_for_apply: BTreeMap<ShardId, DataWriteApply<K, V, T, D>>,
    /// See [DataWriteCommit].
    ///
    /// This contains the set of data shards registered but not yet forgotten
    /// with this particular write handle.
    ///
    /// NB: In the common case, this and `_for_apply` will contain the same set
    /// of shards, but this is not required. A shard can be in either and not
    /// the other.
    data_write_for_commit: BTreeMap<ShardId, DataWriteCommit<K, V, T, D>>,
}

impl<K, V, T, D> DataHandles<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + Codec64 + Sync,
    D: Semigroup + Ord + Codec64 + Send + Sync,
{
    async fn open_data_write_for_apply(&self, data_id: ShardId) -> DataWriteApply<K, V, T, D> {
        let diagnostics = Diagnostics::from_purpose("txn data");
        let schemas = self
            .client
            .latest_schema::<K, V, T, D>(data_id, diagnostics.clone())
            .await
            .expect("codecs have not changed");
        let (key_schema, val_schema) = match schemas {
            Some((_, key_schema, val_schema)) => (Arc::new(key_schema), Arc::new(val_schema)),
            // We will always have at least one schema registered by the time we reach this point,
            // because that is ensured at txn-registration time.
            None => unreachable!("data shard {} should have a schema", data_id),
        };
        let wrapped = self
            .client
            .open_writer(data_id, key_schema, val_schema, diagnostics)
            .await
            .expect("schema shouldn't change");
        DataWriteApply {
            apply_ensure_schema_match: APPLY_ENSURE_SCHEMA_MATCH.handle(&self.dyncfgs),
            client: Arc::clone(&self.client),
            wrapped,
        }
    }

    pub(crate) async fn take_write_for_apply(
        &mut self,
        data_id: &ShardId,
    ) -> DataWriteApply<K, V, T, D> {
        if let Some(data_write) = self.data_write_for_apply.remove(data_id) {
            return data_write;
        }
        self.open_data_write_for_apply(*data_id).await
    }

    pub(crate) fn put_write_for_apply(&mut self, data_write: DataWriteApply<K, V, T, D>) {
        self.data_write_for_apply
            .insert(data_write.shard_id(), data_write);
    }

    pub(crate) fn take_write_for_commit(
        &mut self,
        data_id: &ShardId,
    ) -> Option<DataWriteCommit<K, V, T, D>> {
        self.data_write_for_commit.remove(data_id)
    }

    pub(crate) fn put_write_for_commit(&mut self, data_write: DataWriteCommit<K, V, T, D>) {
        let prev = self
            .data_write_for_commit
            .insert(data_write.shard_id(), data_write);
        assert!(prev.is_none());
    }
}

/// A newtype wrapper around [WriteHandle] indicating that it has a real schema
/// registered by the user.
///
/// The txn-wal user declares which schema they'd like to use for committing
/// batches by passing it in (as part of the WriteHandle) in the call to
/// register. This must be used to encode any new batches written. The wrapper
/// helps us from accidentally mixing up the WriteHandles that we internally
/// invent for applying the batches (which use a schema matching the one
/// declared in the batch).
#[derive(Debug)]
pub(crate) struct DataWriteCommit<K: Codec, V: Codec, T, D>(pub(crate) WriteHandle<K, V, T, D>);

impl<K: Codec, V: Codec, T, D> Deref for DataWriteCommit<K, V, T, D> {
    type Target = WriteHandle<K, V, T, D>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K: Codec, V: Codec, T, D> DerefMut for DataWriteCommit<K, V, T, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// A newtype wrapper around [WriteHandle] indicating that it can alter the
/// schema its using to match the one in the batches being appended.
///
/// When a batch is committed to txn-wal, it contains metadata about which
/// schemas were used to encode the data in it. Txn-wal then uses this info to
/// make sure that in [TxnsHandle::apply_le], that the `compare_and_append` call
/// happens on a handle with the same schema. This is accomplished by querying
/// the persist schema registry.
#[derive(Debug)]
pub(crate) struct DataWriteApply<K: Codec, V: Codec, T, D> {
    client: Arc<PersistClient>,
    apply_ensure_schema_match: ConfigValHandle<bool>,
    pub(crate) wrapped: WriteHandle<K, V, T, D>,
}

impl<K: Codec, V: Codec, T, D> Deref for DataWriteApply<K, V, T, D> {
    type Target = WriteHandle<K, V, T, D>;

    fn deref(&self) -> &Self::Target {
        &self.wrapped
    }
}

impl<K: Codec, V: Codec, T, D> DerefMut for DataWriteApply<K, V, T, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.wrapped
    }
}

pub(crate) const APPLY_ENSURE_SCHEMA_MATCH: Config<bool> = Config::new(
    "txn_wal_apply_ensure_schema_match",
    true,
    "CYA to skip updating write handle to batch schema in apply",
);

fn at_most_one_schema(
    schemas: impl Iterator<Item = SchemaId>,
) -> Result<Option<SchemaId>, (SchemaId, SchemaId)> {
    let mut schema = None;
    for s in schemas {
        match schema {
            None => schema = Some(s),
            Some(x) if s != x => return Err((s, x)),
            Some(_) => continue,
        }
    }
    Ok(schema)
}

impl<K, V, T, D> DataWriteApply<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + Codec64 + Sync,
    D: Semigroup + Ord + Codec64 + Send + Sync,
{
    pub(crate) async fn maybe_replace_with_batch_schema(&mut self, batches: &[Batch<K, V, T, D>]) {
        // TODO: Remove this once everything is rolled out and we're sure it's
        // not going to cause any issues.
        if !self.apply_ensure_schema_match.get() {
            return;
        }
        let batch_schema = at_most_one_schema(batches.iter().flat_map(|x| x.schemas()));
        let batch_schema = batch_schema.unwrap_or_else(|_| {
            panic!(
                "txn-wal uses at most one schema to commit batches, got: {:?}",
                batches.iter().flat_map(|x| x.schemas()).collect::<Vec<_>>()
            )
        });
        let (batch_schema, handle_schema) = match (batch_schema, self.wrapped.schema_id()) {
            (Some(batch_schema), Some(handle_schema)) if batch_schema != handle_schema => {
                (batch_schema, handle_schema)
            }
            _ => return,
        };

        let data_id = self.shard_id();
        let diagnostics = Diagnostics::from_purpose("txn data");
        let (key_schema, val_schema) = self
            .client
            .get_schema::<K, V, T, D>(data_id, batch_schema, diagnostics.clone())
            .await
            .expect("codecs shouldn't change")
            .expect("id must have been registered to create this batch");
        let new_data_write = self
            .client
            .open_writer(
                self.shard_id(),
                Arc::new(key_schema),
                Arc::new(val_schema),
                diagnostics,
            )
            .await
            .expect("codecs shouldn't change");
        tracing::info!(
            "updated {} write handle from {} to {} to apply batches",
            data_id,
            handle_schema,
            batch_schema
        );
        assert_eq!(new_data_write.schema_id(), Some(batch_schema));
        self.wrapped = new_data_write;
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use differential_dataflow::Hashable;
    use futures::future::BoxFuture;
    use mz_ore::assert_none;
    use mz_ore::cast::CastFrom;
    use mz_ore::collections::CollectionExt;
    use mz_ore::metrics::MetricsRegistry;
    use mz_persist_client::PersistLocation;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::cfg::RetryParameters;
    use rand::rngs::SmallRng;
    use rand::{RngCore, SeedableRng};
    use timely::progress::Antichain;
    use tokio::sync::oneshot;
    use tracing::{Instrument, info, info_span};

    use crate::operator::DataSubscribe;
    use crate::tests::{CommitLog, reader, write_directly, writer};

    use super::*;

    impl TxnsHandle<String, (), u64, i64, u64, TxnsCodecDefault> {
        pub(crate) async fn expect_open(client: PersistClient) -> Self {
            Self::expect_open_id(client, ShardId::new()).await
        }

        pub(crate) async fn expect_open_id(client: PersistClient, txns_id: ShardId) -> Self {
            let dyncfgs = crate::all_dyncfgs(client.dyncfgs().clone());
            Self::open(
                0,
                client,
                dyncfgs,
                Arc::new(Metrics::new(&MetricsRegistry::new())),
                txns_id,
            )
            .await
        }

        pub(crate) fn new_log(&self) -> CommitLog {
            CommitLog::new((*self.datas.client).clone(), self.txns_id())
        }

        pub(crate) async fn expect_register(&mut self, register_ts: u64) -> ShardId {
            self.expect_registers(register_ts, 1).await.into_element()
        }

        pub(crate) async fn expect_registers(
            &mut self,
            register_ts: u64,
            amount: usize,
        ) -> Vec<ShardId> {
            let data_ids: Vec<_> = (0..amount).map(|_| ShardId::new()).collect();
            let mut writers = Vec::new();
            for data_id in &data_ids {
                writers.push(writer(&self.datas.client, *data_id).await);
            }
            self.register(register_ts, writers).await.unwrap();
            data_ids
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
        let mut txn = txns.begin_test();
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
        txns.forget(1, [ShardId::new()]).await.unwrap();

        // Can forget multiple data_shards that have not been registered.
        txns.forget(2, (0..5).map(|_| ShardId::new()))
            .await
            .unwrap();

        // Can forget a registered shard.
        let d0 = txns.expect_register(3).await;
        txns.forget(4, [d0]).await.unwrap();

        // Can forget multiple registered shards.
        let ds = txns.expect_registers(5, 5).await;
        txns.forget(6, ds.clone()).await.unwrap();

        // Forget is idempotent.
        txns.forget(7, [d0]).await.unwrap();
        txns.forget(8, ds.clone()).await.unwrap();

        // Cannot forget at an already closed off time. An error is returned
        // with the first time that a registration would succeed.
        let d1 = txns.expect_register(9).await;
        assert_eq!(txns.forget(9, [d1]).await.unwrap_err(), 10);

        // Write to txns and to d0 directly.
        let mut d0_write = writer(&client, d0).await;
        txns.expect_commit_at(10, d1, &["d1"], &log).await;
        let updates = [(("d0".to_owned(), ()), 10, 1)];
        d0_write
            .compare_and_append(&updates, d0_write.shared_upper(), Antichain::from_elem(11))
            .await
            .unwrap()
            .unwrap();
        log.record((d0, "d0".into(), 10, 1));

        // Can register and forget an already registered and forgotten shard.
        txns.register(11, [writer(&client, d0).await])
            .await
            .unwrap();
        let mut forget_expected = vec![d0, d1];
        forget_expected.sort();
        assert_eq!(txns.forget_all(12).await.unwrap().0, forget_expected);

        // Close shard to writes
        d0_write
            .compare_and_append_batch(&mut [], d0_write.shared_upper(), Antichain::new(), true)
            .await
            .unwrap()
            .unwrap();

        let () = log.assert_snapshot(d0, 12).await;
        let () = log.assert_snapshot(d1, 12).await;

        for di in ds {
            let mut di_write = writer(&client, di).await;

            // Close shards to writes
            di_write
                .compare_and_append_batch(&mut [], di_write.shared_upper(), Antichain::new(), true)
                .await
                .unwrap()
                .unwrap();

            let () = log.assert_snapshot(di, 8).await;
        }
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
            txns.forget(ts, [d0]).await.unwrap();
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
            match self.rng.next_u64() % 6 {
                0 => self.write(data_id).await,
                // The register and forget impls intentionally don't switch on
                // whether it's already registered to stress idempotence.
                1 => self.register(data_id).await,
                2 => self.forget(data_id).await,
                3 => {
                    debug!("stress update {:.9} to {}", data_id.to_string(), self.ts);
                    let _ = self.txns.txns_cache.update_ge(&self.ts).await;
                }
                4 => self.start_read(data_id, true),
                5 => self.start_read(data_id, false),
                _ => unreachable!(""),
            }
            debug!("stress {} step {} DONE ts={}", self.idx, self.step, self.ts);
            self.step += 1;
        }

        fn key(&self) -> String {
            format!("w{}s{}", self.idx, self.step)
        }

        async fn registered_at_progress_ts(&mut self, data_id: ShardId) -> bool {
            self.ts = *self.txns.txns_cache.update_ge(&self.ts).await;
            self.txns
                .txns_cache
                .registered_at_progress(&data_id, &self.ts)
        }

        // Writes to the given data shard, either via txns if it's registered or
        // directly if it's not.
        async fn write(&mut self, data_id: ShardId) {
            // Make sure to keep the registered_at_ts call _inside_ the retry
            // loop, because a data shard might switch between registered or not
            // registered as the loop advances through timestamps.
            self.retry_ts_err(&mut |w: &mut StressWorker| {
                Box::pin(async move {
                    if w.registered_at_progress_ts(data_id).await {
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
            // HACK: Normally, we'd make sure that this particular handle had
            // registered the data shard before writing to it, but that would
            // consume a ts and isn't quite how we want `write_via_txns` to
            // work. Work around that by setting a write handle (with a schema
            // that we promise is correct) in the right place.
            if !self.txns.datas.data_write_for_commit.contains_key(&data_id) {
                let x = writer(&self.txns.datas.client, data_id).await;
                self.txns
                    .datas
                    .data_write_for_commit
                    .insert(data_id, DataWriteCommit(x));
            }
            let mut txn = self.txns.begin_test();
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
                Box::pin(async move { w.txns.forget(w.ts, [data_id]).await.map(|_| ()) })
            })
            .await
        }

        fn start_read(&mut self, data_id: ShardId, use_global_txn_cache: bool) {
            debug!(
                "stress start_read {:.9} at {}",
                data_id.to_string(),
                self.ts
            );
            let client = (*self.txns.datas.client).clone();
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
                        use_global_txn_cache,
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

        log.assert_snapshot(d0, 4).await;
        log.assert_snapshot(d1, 4).await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    #[allow(clippy::unnecessary_get_then_check)] // Makes it less readable.
    async fn schemas() {
        let client = PersistClient::new_for_tests().await;
        let mut txns0 = TxnsHandle::expect_open(client.clone()).await;
        let mut txns1 = TxnsHandle::expect_open_id(client.clone(), txns0.txns_id()).await;
        let log = txns0.new_log();
        let d0 = txns0.expect_register(1).await;

        // The register call happened on txns0, which means it has a real schema
        // and can commit batches.
        assert!(txns0.datas.data_write_for_commit.get(&d0).is_some());
        let mut txn = txns0.begin_test();
        txn.write(&d0, "foo".into(), (), 1).await;
        let apply = txn.commit_at(&mut txns0, 2).await.unwrap();
        log.record_txn(2, &txn);

        // We can use handle without a register call to apply a committed txn.
        assert!(txns1.datas.data_write_for_commit.get(&d0).is_none());
        let _tidy = apply.apply(&mut txns1).await;

        // However, it cannot commit batches.
        assert!(txns1.datas.data_write_for_commit.get(&d0).is_none());
        let res = mz_ore::task::spawn(|| "test", async move {
            let mut txn = txns1.begin();
            txn.write(&d0, "bar".into(), (), 1).await;
            // This panics.
            let _ = txn.commit_at(&mut txns1, 3).await;
        });
        assert!(res.await.is_err());

        // Forgetting the data shard removes it, so we don't leave the schema
        // sitting around.
        assert!(txns0.datas.data_write_for_commit.get(&d0).is_some());
        txns0.forget(3, [d0]).await.unwrap();
        assert_none!(txns0.datas.data_write_for_commit.get(&d0));

        // Forget is idempotent.
        assert_none!(txns0.datas.data_write_for_commit.get(&d0));
        txns0.forget(4, [d0]).await.unwrap();
        assert_none!(txns0.datas.data_write_for_commit.get(&d0));

        // We can register it again and commit again.
        assert_none!(txns0.datas.data_write_for_commit.get(&d0));
        txns0
            .register(5, [writer(&client, d0).await])
            .await
            .unwrap();
        assert!(txns0.datas.data_write_for_commit.get(&d0).is_some());
        txns0.expect_commit_at(6, d0, &["baz"], &log).await;

        log.assert_snapshot(d0, 6).await;
    }
}
