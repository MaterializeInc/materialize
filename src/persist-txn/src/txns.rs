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
use mz_persist_types::codec_impls::{StringSchema, TodoSchema};
use mz_persist_types::{Codec, Codec64};
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use tracing::debug;

use crate::txn_read::TxnsCache;
use crate::txn_write::Txn;
use crate::StepForward;

/// An interface for atomic multi-shard writes.
///
/// This handle is acquired through [Self::open]. Any data shards must be
/// registered with [Self::register] before use. Transactions are then started
/// with [Self::begin].
///
/// # Implementation Details
///
/// The structure of the txn shard is `(ShardId, Vec<u8>)` updates.
///
/// The core mechanism is that a txn commits a set of transmittable persist
/// _batch handles_ as `(ShardId, <opaque blob>)` pairs at a single timestamp.
/// This contractually both commits the txn and advances the upper of every data
/// shard.
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
/// which is expected to be promptly run by the committer, except in the event
/// of a crash. This, in ts order, moves the batch handles into the data shards
/// with a [compare_and_append_batch] (similar to how the multi-worker
/// persist_sink works).
///
/// [compare_and_append_batch]:
///     mz_persist_client::write::WriteHandle::compare_and_append_batch
///
/// Once apply is run, this is noted in the txn shard by retracting the update
/// adding the batch. As a result, the contents of the txn shard at any given
/// timestamp is exactly the set of outstanding apply work.
///
/// Example (building on the above):
///
/// ```text
/// // Apply for the first txn at ts=3
/// (d0, <opaque blob A>, 3, -1)
/// // Apply for the second txn (the timestamps can be different for each
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
pub struct TxnsHandle<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    pub(crate) txns_cache: TxnsCache<T>,
    pub(crate) txns_write: WriteHandle<ShardId, String, T, i64>,
    pub(crate) datas: DataHandles<K, V, T, D>,
}

impl<K, V, T, D> TxnsHandle<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
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
        let (mut txns_write, txns_read) = client
            .open(
                txns_id,
                Arc::new(TodoSchema::<ShardId>::default()),
                Arc::new(StringSchema),
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

    /// Registers a data shard for use with this txn set.
    ///
    /// The data shard will be initially readable at `register_ts`. First, a
    /// registration entry is written to the txn shard. If it is not possible to
    /// register the data at that time, an Err will be returned with the minimum
    /// time the data shard could be registered. Second, the data shard will be
    /// made readable at `register_ts` by appending empty updates to it, if
    /// necessary. If the data has already been registered, this method is
    /// idempotent.
    ///
    /// **WARNING!** While a data shard is registered to the txn set, writing to
    /// it directly (i.e. using a WriteHandle instead of the TxnHandle,
    /// registering it with another txn shard) will lead to incorrectness,
    /// undefined behavior, and (potentially sticky) panics.
    pub async fn register(
        &mut self,
        mut register_ts: T,
        data_write: WriteHandle<K, V, T, D>,
    ) -> Result<(), T> {
        let data_id = data_write.shard_id();
        // SUBTLE! We have to write the registration to the txns shard *before*
        // we write empty data to the physical shard. Otherwise we could race
        // with a commit at the same timestamp, which would then not be able to
        // copy the committed batch into the data shard.
        let mut txns_upper = recent_upper(&mut self.txns_write).await;
        loop {
            self.txns_cache.update_ge(&txns_upper).await;
            if let Ok(init_ts) = self.txns_cache.data_since(&data_id) {
                debug!("txns register {:.9} already done at {:?}", data_id, init_ts);
                if register_ts < init_ts {
                    return Err(init_ts);
                }
                register_ts = init_ts;
                break;
            } else if register_ts < txns_upper {
                debug!(
                    "txns register {:.9} at {:?} mismatch current={:?}",
                    data_id, register_ts, txns_upper,
                );
                return Err(txns_upper);
            }

            const TABLE_INIT: &String = &String::new();
            let updates = vec![((&data_id, TABLE_INIT), &register_ts, 1)];
            let res = crate::small_caa(
                || format!("txns register {:.9}", data_id.to_string()),
                &mut self.txns_write,
                &updates,
                txns_upper,
                register_ts.step_forward(),
            )
            .await;
            match res {
                Ok(()) => {
                    break;
                }
                Err(new_txns_upper) => {
                    txns_upper = new_txns_upper;
                    continue;
                }
            }
        }
        self.datas.data_write.insert(data_id, data_write);

        // Now that we've written the initialization into the txns shard, we
        // know it's safe to make the data shard readable at `commit_ts`. We
        // might crash between the txns write and this one, so `apply_le` also
        // has to do the work just in case.
        //
        // TODO(txn): This will have to work differently once we plumb in real
        // schemas.
        self.apply_le(&register_ts).await;

        Ok(())
    }

    /// "Applies" all committed txns <= the given timestamp, ensuring that reads
    /// at that timestamp will not block.
    ///
    /// In the common case, the txn committer will have done this work and this
    /// method will be a no-op, but it is not guaranteed. In the event of a
    /// crash or race, this does whatever persist writes and resulting
    /// maintenance work is necessary, which could be significant.
    ///
    /// If the requested timestamp has not yet been written, this could block
    /// for an unbounded amount of time.
    ///
    /// This method is idempotent.
    pub async fn apply_le(&mut self, ts: &T) {
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

        let mut retractions = Vec::new();
        for (data_id, batch_raw, commit_ts) in self.txns_cache.unapplied_batches() {
            if ts < commit_ts {
                break;
            }

            let write = self.datas.get_write(data_id).await;
            let by_us = crate::apply_caa(write, batch_raw, commit_ts.clone()).await;
            if by_us {
                // NB: Protos are not guaranteed to exactly roundtrip the
                // encoded bytes, so we intentionally use the raw batch so that
                // it definitely retracts.
                retractions.push((data_id, batch_raw));
            }
            // TODO(txn): Commit as we go? This is idempotent but that could
            // save us duplicate work when racing.
        }

        let mut retract_ts = recent_upper(&mut self.txns_write).await;
        loop {
            // TODO(txn): Need to filter here to avoid double retractions.
            // Maelstrom won't catch this bug, so leaving it in to make sure the
            // unit tests do once they're added.
            let retractions = retractions
                .iter()
                .map(|(data_id, batch_desc)| ((*data_id, *batch_desc), &retract_ts, -1))
                .collect::<Vec<_>>();
            if retractions.is_empty() {
                break;
            }
            let res = crate::small_caa(
                || "txns retract",
                &mut self.txns_write,
                &retractions,
                retract_ts.clone(),
                retract_ts.step_forward(),
            )
            .await;
            match res {
                Ok(()) => break,
                Err(current) => {
                    retract_ts = current;
                    continue;
                }
            }
        }

        debug!("apply_le {:?} success", ts);
    }

    /// Returns the [TxnsCache] used by this handle.
    pub fn read_cache(&self) -> &TxnsCache<T> {
        &self.txns_cache
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
    client: PersistClient,
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

pub(crate) async fn recent_upper<T: Timestamp + Lattice + TotalOrder + Codec64>(
    txns_or_data_write: &mut WriteHandle<ShardId, String, T, i64>,
) -> T {
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
    use mz_persist_types::codec_impls::{UnitSchema, VecU8Schema};
    use timely::progress::Antichain;

    use crate::tests::writer;

    use super::*;

    async fn expect_snapshot(
        txns: &mut TxnsHandle<Vec<u8>, (), u64, i64>,
        data_id: ShardId,
        as_of: u64,
    ) -> Vec<(Vec<u8>, u64, i64)> {
        let mut data_read = txns
            .datas
            .client
            .open_leased_reader::<Vec<u8>, (), u64, i64>(
                data_id,
                Arc::new(VecU8Schema),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("schemas shouldn't change");

        let () = txns.apply_le(&as_of).await;
        let translated_as_of = txns
            .read_cache()
            .to_data_inclusive(&data_id, as_of)
            .unwrap();
        let updates = data_read
            .snapshot_and_fetch(Antichain::from_elem(translated_as_of))
            .await
            .unwrap();
        updates
            .into_iter()
            .map(|((k, v), t, d)| {
                let (k, ()) = (k.unwrap(), v.unwrap());
                (k, t, d)
            })
            .collect()
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
        let mut txns = TxnsHandle::<Vec<u8>, (), u64, i64>::open(
            0,
            client.clone(),
            ShardId::new(),
            Arc::new(VecU8Schema),
            Arc::new(UnitSchema),
        )
        .await;
        let d0 = ShardId::new();
        txns.register(1, writer(&client, d0).await).await.unwrap();

        let mut txn = txns.begin();
        txn.write(&d0, "foo".into(), (), 1).await;
        let commit_apply = txn.commit_at(&mut txns, 2).await.unwrap();

        txns.register(2, writer(&client, d0).await).await.unwrap();

        // Make sure that we can read empty at the register commit time even
        // before the txn commit apply.
        let actual = expect_snapshot(&mut txns, d0, 1).await;
        assert_eq!(actual, vec![]);

        commit_apply.apply(&mut txns).await;
        let actual = expect_snapshot(&mut txns, d0, 2).await;
        assert_eq!(actual, vec![("foo".into(), 2, 1)]);
    }
}
