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
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::{Codec, Codec64, Opaque, StepForward};
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use tracing::{debug, instrument};

use crate::txn_read::TxnsCache;
use crate::txn_write::Txn;
use crate::{TxnsCodec, TxnsCodecDefault};

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
    pub(crate) txns_cache: TxnsCache<T, C>,
    pub(crate) txns_write: WriteHandle<C::Key, C::Val, T, i64>,
    #[allow(dead_code)] // TODO(txn): Becomes used in the txns compaction PR.
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
                // TODO(txn): We likely need to use a different critical reader
                // id for this if we want to be able to introspect it via SQL.
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
        // TODO(txn): This is a method on the handle because we'll need
        // WriteHandles once we start spilling to s3.
        Txn::new()
    }

    /// Registers data shards for use with this txn set.
    ///
    /// First, a registration entry is written to the txn shard. If it is not
    /// possible to register the data at the requested time, an Err will be
    /// returned with the minimum time the data shards could be registered.
    /// Second, the data shard will be made readable through `register_ts` by
    /// appending empty updates to it, if necessary.
    ///
    /// This method is idempotent. Data shards registered previously to
    /// `register_ts` will not be registered a second time.
    ///
    /// **WARNING!** While a data shard is registered to the txn set, writing to
    /// it directly (i.e. using a WriteHandle instead of the TxnHandle,
    /// registering it with another txn shard) will lead to incorrectness,
    /// undefined behavior, and (potentially sticky) panics.
    #[instrument(level = "debug", skip_all, fields(ts = ?register_ts))]
    pub async fn register(
        &mut self,
        mut register_ts: T,
        data_writes: impl IntoIterator<Item = WriteHandle<K, V, T, D>>,
    ) -> Result<T, T> {
        let mut max_registered_ts = T::minimum();

        let data_writes = data_writes.into_iter().collect::<Vec<_>>();
        let mut data_ids = data_writes.iter().map(|x| x.shard_id()).collect::<Vec<_>>();
        let data_ids_debug = data_ids
            .iter()
            .map(|x| format!("{:.9}", x.to_string()))
            .collect::<Vec<_>>()
            .join(" ");

        // SUBTLE! We have to write the registration to the txns shard *before*
        // we write empty data to the physical shard. Otherwise we could race
        // with a commit at the same timestamp, which would then not be able to
        // copy the committed batch into the data shard.
        let mut txns_upper = self
            .txns_write
            .shared_upper()
            .into_option()
            .expect("txns should not be closed");
        loop {
            self.txns_cache.update_ge(&txns_upper).await;
            data_ids.retain(|data_id| {
                let Ok(ts) = self.txns_cache.data_since(data_id) else {
                    return true;
                };
                debug!(
                    "txns register {:.9} already done at {:?}",
                    data_id.to_string(),
                    ts
                );
                if max_registered_ts < ts {
                    max_registered_ts = ts;
                }
                false
            });
            if data_ids.is_empty() {
                register_ts = max_registered_ts;
                break;
            } else if register_ts < txns_upper {
                debug!(
                    "txns register {} at {:?} mismatch current={:?}",
                    data_ids_debug, register_ts, txns_upper,
                );
                return Err(txns_upper);
            }

            let updates = data_ids
                .iter()
                .map(|data_id| C::encode(crate::TxnsEntry::Register(*data_id)))
                .collect::<Vec<_>>();
            let updates = updates
                .iter()
                .map(|(key, val)| ((key, val), &register_ts, 1i64))
                .collect::<Vec<_>>();
            let res = crate::small_caa(
                || format!("txns register {}", data_ids_debug),
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
                        data_ids_debug, register_ts
                    );
                    break;
                }
                Err(new_txns_upper) => {
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

        // Now that we've written the initialization into the txns shard, we
        // know it's safe to make the data shard readable at `commit_ts`. We
        // might crash between the txns write and this one, so `apply_le` also
        // has to do the work just in case.
        //
        // TODO(txn): This will have to work differently once we plumb in real
        // schemas.
        self.apply_le(&register_ts).await;

        Ok(register_ts)
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
    #[instrument(level = "debug", skip_all, fields(ts = ?ts))]
    pub async fn apply_le(&mut self, ts: &T) -> Tidy {
        debug!("apply_le {:?}", ts);
        self.txns_cache.update_gt(ts).await;

        // TODO(txn): Oof, figure out a protocol where we don't have to do this
        // every time.
        let inits = FuturesUnordered::new();
        for (data_id, times) in self.txns_cache.datas.iter() {
            let register_ts = times.first().expect("data shard has register ts").clone();
            let mut data_write = self.datas.take_write(data_id).await;
            inits.push(async move {
                let () = crate::empty_caa(
                    || format!("data init {:.9}", data_id.to_string()),
                    &mut data_write,
                    register_ts,
                )
                .await;
                data_write
            });
        }
        let data_writes = inits.collect::<Vec<_>>().await;
        for data_write in data_writes {
            self.datas.put_write(data_write);
        }

        let mut unapplied_batches_by_data = BTreeMap::<_, Vec<_>>::new();
        for (data_id, batch_raw, commit_ts) in self.txns_cache.unapplied_batches() {
            if ts < commit_ts {
                break;
            }
            unapplied_batches_by_data
                .entry(*data_id)
                .or_default()
                .push((batch_raw, commit_ts));
        }

        let retractions = FuturesUnordered::new();
        for (data_id, batches) in unapplied_batches_by_data {
            let mut data_write = self.datas.take_write(&data_id).await;
            retractions.push(async move {
                let mut ret = Vec::new();
                for (batch_raw, commit_ts) in batches {
                    crate::apply_caa(&mut data_write, batch_raw, commit_ts.clone()).await;
                    // NB: Protos are not guaranteed to exactly roundtrip the
                    // encoded bytes, so we intentionally use the raw batch so that
                    // it definitely retracts.
                    ret.push((batch_raw.clone(), data_id));
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

        debug!("apply_le {:?} success", ts);
        Tidy { retractions }
    }

    /// Commits the tidy work at the given time.
    ///
    /// Mostly a helper to make it obvious that we can throw away the apply work
    /// (and not get into an infinite cycle of tidy->apply->tidy).
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
/// This can be written on its own with [TxnsHandle::tidy_at] or sidecar'd into
/// a normal txn with [Txn::tidy].
#[derive(Debug, Default)]
pub struct Tidy {
    pub(crate) retractions: BTreeMap<Vec<u8>, ShardId>,
}

impl Tidy {
    /// Merges the work represented by the other tidy into this one.
    pub fn merge(&mut self, other: Tidy) {
        self.retractions.extend(other.retractions)
    }
}

/// A helper to make [Self::get_write] a more targeted mutable borrow of self.
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

    #[allow(dead_code)]
    pub(crate) async fn get_write<'a>(
        &'a mut self,
        data_id: &ShardId,
    ) -> &'a mut WriteHandle<K, V, T, D> {
        if self.data_write.get(data_id).is_none() {
            let data_write = self.open_data_write(*data_id).await;
            assert!(self.data_write.insert(*data_id, data_write).is_none());
        }
        self.data_write.get_mut(data_id).expect("inserted above")
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
    use std::time::UNIX_EPOCH;

    use differential_dataflow::Hashable;
    use futures::future::BoxFuture;
    use mz_ore::cast::CastFrom;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::PersistLocation;
    use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
    use rand::rngs::SmallRng;
    use rand::{RngCore, SeedableRng};
    use timely::progress::Antichain;
    use tokio::sync::oneshot;
    use tracing::{info, info_span, Instrument};

    use crate::operator::DataSubscribe;
    use crate::tests::{reader, writer, CommitLog};

    use super::*;

    impl TxnsHandle<String, (), u64, i64, u64, TxnsCodecDefault> {
        pub(crate) async fn expect_open(client: PersistClient) -> Self {
            Self::expect_open_id(client, ShardId::new()).await
        }

        pub(crate) async fn expect_open_id(client: PersistClient, txns_id: ShardId) -> Self {
            Self::open(
                0,
                client,
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
        txns.apply_le(&2).await;

        // Register a second time is a no-op and returns the original
        // registration time, regardless of whether the second attempt is for
        // before or after the original time.
        assert_eq!(txns.register(1, [writer(&client, d0).await]).await, Ok(2));
        assert_eq!(txns.register(3, [writer(&client, d0).await]).await, Ok(2));

        // Cannot register a new data shard at an already closed off time. An
        // error is returned with the first time that a registration would
        // succeed.
        let d1 = ShardId::new();
        assert_eq!(txns.register(2, [writer(&client, d1).await]).await, Err(3));

        // Can still register after txns have been committed.
        txns.expect_commit_at(3, d0, &["foo"], &log).await;
        assert_eq!(txns.register(4, [writer(&client, d1).await]).await, Ok(4));

        // Reregistration returns the latest original registration time.
        assert_eq!(txns.register(5, [writer(&client, d0).await]).await, Ok(2));
        assert_eq!(
            txns.register(5, [writer(&client, d0).await, writer(&client, d1).await])
                .await,
            Ok(4)
        );

        // If some are registered but some are not, then register_ts is
        // returned. This also tests that the previous entirely registered calls
        // did not consume the timestamp.
        let d2 = ShardId::new();
        assert_eq!(
            txns.register(5, [writer(&client, d0).await, writer(&client, d2).await])
                .await,
            Ok(5)
        );

        let () = log.assert_snapshot(d0, 5).await;
        let () = log.assert_snapshot(d1, 5).await;
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

        txns.register(2, [writer(&client, d0).await]).await.unwrap();

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
            tokio::task::JoinHandle<Vec<(String, u64, i64)>>,
        )>,
    }

    impl StressWorker {
        pub async fn step(&mut self) {
            debug!("{} step {} START ts={}", self.idx, self.step, self.ts);
            let data_id =
                self.data_ids[usize::cast_from(self.rng.next_u64()) % self.data_ids.len()];
            match self.rng.next_u64() % 3 {
                0 => self.write(data_id).await,
                1 => self.register(data_id).await,
                2 => self.start_read(data_id),
                _ => unreachable!(""),
            }
            debug!("{} step {} DONE ts={}", self.idx, self.step, self.ts);
            self.step += 1;
        }

        fn key(&self) -> String {
            format!("w{}s{}", self.idx, self.step)
        }

        async fn registered_at_ts(&mut self, data_id: ShardId) -> bool {
            self.txns.txns_cache.update_ge(&self.ts).await;
            self.txns.txns_cache.datas.contains_key(&data_id)
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
            let mut txn = self.txns.begin();
            txn.tidy(std::mem::take(&mut self.tidy));
            txn.write(&data_id, self.key(), (), 1).await;
            let apply = txn.commit_at(&mut self.txns, self.ts).await?;
            self.log.record_txn(self.ts, &txn);
            if self.rng.next_u64() % 3 == 0 {
                self.tidy.merge(apply.apply(&mut self.txns).await);
            }
            Ok(())
        }

        async fn write_direct(&mut self, data_id: ShardId) -> Result<(), u64> {
            let mut write = writer(&self.txns.datas.client, data_id).await;
            let mut current = write.shared_upper();
            loop {
                if !current.less_equal(&self.ts) {
                    return Err(current.into_option().unwrap());
                }
                let key = self.key();
                let updates = [((key.clone(), ()), self.ts, 1)];
                let res = write
                    .compare_and_append(&updates, current, Antichain::from_elem(self.ts + 1))
                    .await
                    .unwrap();
                match res {
                    Ok(()) => {
                        self.log.record((data_id, key, self.ts, 1));
                        return Ok(());
                    }
                    Err(err) => current = err.current,
                }
            }
        }

        async fn register(&mut self, data_id: ShardId) {
            self.retry_ts_err(&mut |w: &mut StressWorker| {
                Box::pin(async move {
                    let data_write = writer(&w.txns.datas.client, data_id).await;
                    let _ = w.txns.register(w.ts, [data_write]).await?;
                    Ok(())
                })
            })
            .await
        }

        fn start_read(&mut self, data_id: ShardId) {
            let client = self.txns.datas.client.clone();
            let txns_id = self.txns.txns_id();
            let as_of = self.ts;
            debug!("start_read {:.9} as_of {}", data_id.to_string(), as_of);
            let (tx, mut rx) = oneshot::channel();
            let subscribe = mz_ore::task::spawn_blocking(
                || format!("{:.9}-{}", data_id.to_string(), as_of),
                move || {
                    let _guard = info_span!("read_worker", %data_id, as_of).entered();
                    let mut subscribe = DataSubscribe::new("test", client, txns_id, data_id, as_of);
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
                ts: register_ts + 1,
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

        info!("finished with max_ts of {}", max_ts);
        txns.apply_le(&max_ts).await;
        for data_id in data_ids {
            log.assert_snapshot(data_id, max_ts).await;
        }
        info!("now waiting for reads {}", max_ts);
        for (tx, data_id, as_of, subscribe) in reads {
            let _ = tx.send(max_ts + 1);
            let output = subscribe.await.unwrap();
            log.assert_eq(data_id, as_of, max_ts + 1, output);
        }
    }
}
