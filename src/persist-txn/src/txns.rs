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
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::{Codec, Codec64, StepForward};
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use tracing::debug;

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
pub struct TxnsHandle<K, V, T, D, C = TxnsCodecDefault>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
    C: TxnsCodec,
{
    pub(crate) txns_cache: TxnsCache<T, C>,
    pub(crate) txns_write: WriteHandle<C::Key, C::Val, T, i64>,
    pub(crate) datas: DataHandles<K, V, T, D>,
}

impl<K, V, T, D, C> TxnsHandle<K, V, T, D, C>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
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
        let txns_cache = TxnsCache::init(init_ts, txns_read, &mut txns_write).await;
        TxnsHandle {
            txns_cache,
            txns_write,
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
        let mut txns_upper = recent_upper(&mut self.txns_write).await;
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
    pub async fn apply_le(&mut self, ts: &T) -> Tidy {
        debug!("apply_le {:?}", ts);
        self.txns_cache.update_gt(ts).await;

        // TODO(txn): Oof, figure out a protocol where we don't have to do this
        // every time.
        for (data_id, times) in self.txns_cache.datas.iter() {
            let register_ts = times.first().expect("data shard has register ts").clone();
            let data_write = self.datas.get_write(data_id).await;
            let () = crate::empty_caa(
                || format!("data init {:.9}", data_id.to_string()),
                data_write,
                register_ts,
            )
            .await;
        }

        let mut retractions = BTreeMap::new();
        for (data_id, batch_raw, commit_ts) in self.txns_cache.unapplied_batches() {
            if ts < commit_ts {
                break;
            }

            let write = self.datas.get_write(data_id).await;
            crate::apply_caa(write, batch_raw, commit_ts.clone()).await;
            // NB: Protos are not guaranteed to exactly roundtrip the
            // encoded bytes, so we intentionally use the raw batch so that
            // it definitely retracts.
            retractions.insert(batch_raw.clone(), *data_id);
        }

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
    pub(crate) async fn get_write<'a>(
        &'a mut self,
        data_id: &ShardId,
    ) -> &'a mut WriteHandle<K, V, T, D> {
        if self.data_write.get(data_id).is_none() {
            let data = self
                .client
                .open_writer(
                    *data_id,
                    Arc::clone(&self.key_schema),
                    Arc::clone(&self.val_schema),
                    Diagnostics::from_purpose("txn data"),
                )
                .await
                .expect("schema shouldn't change");
            assert!(self.data_write.insert(*data_id, data).is_none());
        }
        self.data_write.get_mut(data_id).expect("inserted above")
    }
}

pub(crate) async fn recent_upper<K, V, T>(txns_or_data_write: &mut WriteHandle<K, V, T, i64>) -> T
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + Codec64,
{
    // TODO(txn): Replace this fetch_recent_upper call with pubsub in
    // maelstrom.
    txns_or_data_write
        .fetch_recent_upper()
        .await
        .as_option()
        .expect("txns shard should not be closed")
        .clone()
}

#[cfg(test)]
mod tests {
    use mz_persist_types::codec_impls::{StringSchema, UnitSchema};

    use crate::tests::writer;

    use super::*;

    impl TxnsHandle<String, (), u64, i64, TxnsCodecDefault> {
        pub(crate) async fn expect_open(client: PersistClient) -> Self {
            Self::expect_open_id(client, ShardId::new()).await
        }

        pub(crate) async fn expect_open_id(client: PersistClient, txns_id: ShardId) -> Self {
            Self::open(
                0,
                client.clone(),
                txns_id,
                Arc::new(StringSchema),
                Arc::new(UnitSchema),
            )
            .await
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
        ) -> Tidy {
            let mut txn = self.begin();
            for key in keys {
                txn.write(&data_id, (*key).into(), (), 1).await;
            }
            txn.commit_at(self, commit_ts)
                .await
                .unwrap()
                .apply(self)
                .await
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn register_at() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
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
        txns.expect_commit_at(3, d0, &["foo"]).await;
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
}
