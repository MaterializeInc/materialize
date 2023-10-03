// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Interfaces for reading txn shards as well as data shards.

use std::collections::BTreeMap;
use std::fmt::Debug;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use mz_ore::collections::HashMap;
use mz_persist_client::error::UpperMismatch;
use mz_persist_client::read::{ListenEvent, ReadHandle, Since, Subscribe};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::ShardId;
use mz_persist_types::{Codec, Codec64, StepForward};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tracing::debug;

use crate::error::NotRegistered;
use crate::{TxnsCodec, TxnsCodecDefault, TxnsEntry};

/// A cache of the txn shard contents, optimized for various in-memory
/// operations.
///
/// # Implementation Details
///
/// Reads of data shards are almost as straightforward as writes. A data shard
/// may be read normally, using snapshots, subscriptions, shard_source, etc,
/// through the most recent non-empty write. However, the upper of the txns
/// shard (and thus the logical upper of the data shard) may be arbitrarily far
/// ahead of the physical upper of the data shard. As a result, we have to
/// "translate" timestamps by synthesizing ranges of empty time that have not
/// yet been committed (but are guaranteed to remain empty):
///
/// - To take a snapshot of a data shard, the `as_of` is passed through
///   unchanged if the timestamp of that shard's latest non-empty write is past
///   it. Otherwise, the timestamp of the latest non-empty write is used.
///   Concretely, to read a snapshot as of `T`:
///   - We read the txns shard contents up through and including `T`, blocking
///     until the upper passes `T` if necessary.
///   - We then find, for the requested data shard, the latest non-empty write
///     at a timestamp `T' <= T`.
///   - We then take a normal snapshot on the data shard at `T'`.
/// - To iterate a listen on a data shard, when writes haven't been read yet
///   they are passed through unchanged, otherwise if the txns shard indicates
///   that there are ranges of empty time progress is returned, otherwise
///   progress to the txns shard will indicate when new information is
///   available.
///
/// Note that all of the above can be determined solely by information in the
/// txns shard. In particular, non-empty writes are indicated by updates with
/// positive diffs.
///
/// Also note that the above is structured such that it is possible to write a
/// timely operator with the data shard as an input, passing on all payloads
/// unchanged and simply manipulating capabilities in response to data and txns
/// shard progress. See [crate::operator::txns_progress].
#[derive(Debug)]
pub struct TxnsCache<T: Timestamp + Lattice + Codec64, C: TxnsCodec = TxnsCodecDefault> {
    _txns_id: ShardId,
    pub(crate) progress_exclusive: T,
    txns_subscribe: Subscribe<C::Key, C::Val, T, i64>,

    next_batch_id: usize,
    /// The batches needing application as of the current progress.
    ///
    /// This is indexed by a "batch id" that is internal to this object because
    /// timestamps are not unique.
    ///
    /// Invariant: Values are sorted by timestamp.
    unapplied_batches: BTreeMap<usize, (ShardId, Vec<u8>, T)>,
    /// An index into `unapplied_batches` keyed by the serialized batch.
    batch_idx: HashMap<Vec<u8>, usize>,
    /// The times at which each data shard has been written.
    ///
    /// Invariant: Times in the Vec are in ascending order.
    pub(crate) datas: BTreeMap<ShardId, Vec<T>>,
}

impl<T: Timestamp + Lattice + TotalOrder + StepForward + Codec64, C: TxnsCodec> TxnsCache<T, C> {
    pub(crate) async fn init(
        init_ts: T,
        txns_read: ReadHandle<C::Key, C::Val, T, i64>,
        txns_write: &mut WriteHandle<C::Key, C::Val, T, i64>,
    ) -> Self {
        let () = crate::empty_caa(|| "txns init", txns_write, init_ts.clone()).await;
        let mut ret = Self::open(txns_read).await;
        ret.update_gt(&init_ts).await;
        ret
    }

    pub(crate) async fn open(txns_read: ReadHandle<C::Key, C::Val, T, i64>) -> Self {
        let _txns_id = txns_read.shard_id();
        // TODO(txn): Figure out the compaction story. This might require
        // sorting inserts before retractions within each timestamp.
        let as_of = txns_read.since().clone();
        let subscribe = txns_read
            .subscribe(as_of)
            .await
            .expect("handle holds a capability");
        TxnsCache {
            _txns_id,
            progress_exclusive: T::minimum(),
            txns_subscribe: subscribe,
            next_batch_id: 0,
            unapplied_batches: BTreeMap::new(),
            batch_idx: HashMap::new(),
            datas: BTreeMap::new(),
        }
    }

    /// Returns the minimum timestamp at which the data shard can be read.
    ///
    /// Returns Err if that data has not been registered.
    pub fn data_since(&self, data_id: &ShardId) -> Result<T, NotRegistered<T>> {
        let time = self
            .datas
            .get(data_id)
            .and_then(|times| times.first().cloned());
        time.ok_or(NotRegistered {
            data_id: *data_id,
            ts: self.progress_exclusive.clone(),
        })
    }

    /// Returns the greatest timestamp written to the data shard that is `<=`
    /// the given timestamp.
    ///
    /// A data shard might be definite at times past the physical upper because
    /// of invariants maintained by this txn system. As a result, this method
    /// "translates" an `as_of` of a persist snapshot into the one that will
    /// resolve on the physical shard.
    ///
    /// Callers must first wait for `update_gt` with the same or later timestamp
    /// to return. Panics otherwise.
    ///
    /// Returns Err if that data shard has not been registered at the given ts.
    pub fn data_snapshot(
        &self,
        data_id: &ShardId,
        as_of: T,
    ) -> Result<DataSnapshot<T>, NotRegistered<T>> {
        assert!(self.progress_exclusive > as_of);
        let all = self.datas.get(data_id).ok_or(NotRegistered {
            data_id: *data_id,
            ts: self.progress_exclusive.clone(),
        })?;
        let latest_write = all
            .iter()
            .rev()
            .find(|x| **x <= as_of)
            .unwrap_or(&as_of)
            .clone();
        let empty_to = all
            .iter()
            .find(|x| as_of < **x)
            .unwrap_or(&self.progress_exclusive)
            .clone();
        debug!(
            "translate_snapshot {:.9} latest_write={:?} as_of={:?} empty_to={:?}: all={:?}",
            data_id.to_string(),
            latest_write,
            as_of,
            empty_to,
            all,
        );
        let ret = DataSnapshot {
            data_id: data_id.clone(),
            latest_write,
            as_of,
            empty_to,
        };
        assert_eq!(ret.validate(), Ok(()));
        Ok(ret)
    }

    /// Returns the next action to take when iterating a Listen on a data shard.
    ///
    /// A data shard Listen is executed by repeatedly calling this method with
    /// an exclusive progress frontier. The returned value indicates an action
    /// to take. Some of these actions advance the progress frontier, which
    /// results in calling this method again with a higher timestamp, and thus a
    /// new action. See [DataListenNext] for specifications of the actions.
    ///
    /// Note that this is a state machine on `self.progress_exclusive` and the
    /// listen progress. DataListenNext indicates which state transitions to
    /// take.
    ///
    /// Returns Err if that data shard has not been registered at the given ts.
    #[allow(clippy::unused_async)]
    pub fn data_listen_next(
        &self,
        data_id: &ShardId,
        ts: T,
    ) -> Result<DataListenNext<T>, NotRegistered<T>> {
        assert!(self.progress_exclusive >= ts);
        let all = self.datas.get(data_id).ok_or(NotRegistered {
            data_id: *data_id,
            ts: self.progress_exclusive.clone(),
        })?;
        let ret = all
            .last()
            .filter(|x| ts <= **x)
            .map(|ts| DataListenNext::ReadDataPast(ts.clone()));
        debug!(
            "{:.9} data_listen_next 1 {:?}->{:?}: all={:?}",
            data_id.to_string(),
            ts,
            ret,
            all,
        );
        if let Some(ret) = ret {
            return Ok(ret);
        }

        // No writes were <= ft, look to see if the txns upper has advanced
        // past ft.
        if ts < self.progress_exclusive {
            debug!(
                "{:.9} data_listen_next 2 {:?}->{:?}: all={:?}",
                data_id.to_string(),
                ts,
                self.progress_exclusive,
                all
            );
            return Ok(DataListenNext::EmitLogicalProgress(
                self.progress_exclusive.clone(),
            ));
        }
        // Nope, all caught up, we have to wait.
        debug!(
            "{:.9} data_listen_next {:?} waiting for progress",
            data_id.to_string(),
            ts
        );
        Ok(DataListenNext::WaitForTxnsProgress)
    }

    /// Returns the minimum timestamp not known to be applied by this cache.
    pub fn min_unapplied_ts(&self) -> &T {
        // We maintain an invariant that the values in the unapplied_batches map
        // are sorted by timestamp, thus the first one must be the minimum.
        self.unapplied_batches
            .first_key_value()
            .map(|(_, (_, _, ts))| ts)
            // If we don't have any known unapplied batches, then the next
            // timestamp that could be written must potentially have an
            // unapplied batch.
            .unwrap_or(&self.progress_exclusive)
    }

    /// Returns the batches needing application as of the current progress.
    pub(crate) fn unapplied_batches(&self) -> impl Iterator<Item = &(ShardId, Vec<u8>, T)> {
        self.unapplied_batches.values()
    }

    /// Filters out retractions known to have made it into the txns shard.
    ///
    /// This is called with a set of things that are known to have been applied
    /// and in preparation for retracting them. The caller will attempt to
    /// retract everything not filtered out by this method in a CaA with an
    /// expected upper of `expected_txns_upper`. So, we catch up to that point,
    /// and keep everything that is still outstanding. If the CaA fails with an
    /// expected upper mismatch, then it must call this method again on the next
    /// attempt with the new expected upper (new retractions may have made it
    /// into the txns shard in the meantime).
    ///
    /// Callers must first wait for `update_ge` with the same or later timestamp
    /// to return. Panics otherwise.
    pub(crate) fn filter_retractions<'a>(
        &'a self,
        expected_txns_upper: &T,
        retractions: impl Iterator<Item = (&'a Vec<u8>, &'a ShardId)>,
    ) -> impl Iterator<Item = (&'a Vec<u8>, &'a ShardId)> {
        assert!(&self.progress_exclusive >= expected_txns_upper);
        retractions.filter(|(batch_raw, _)| self.batch_idx.contains_key(*batch_raw))
    }

    /// Invariant: afterward, self.progress_exclusive will be > ts
    pub(crate) async fn update_gt(&mut self, ts: &T) {
        self.update(|progress_exclusive| progress_exclusive > ts)
            .await;
        debug_assert!(&self.progress_exclusive > ts);
        debug_assert_eq!(self.validate(), Ok(()));
    }

    /// Invariant: afterward, self.progress_exclusive will be >= ts
    pub(crate) async fn update_ge(&mut self, ts: &T) {
        self.update(|progress_exclusive| progress_exclusive >= ts)
            .await;
        debug_assert!(&self.progress_exclusive >= ts);
        debug_assert_eq!(self.validate(), Ok(()));
    }

    async fn update<F: Fn(&T) -> bool>(&mut self, done: F) {
        while !done(&self.progress_exclusive) {
            let events = self.txns_subscribe.fetch_next().await;
            for event in events {
                let mut updates = match event {
                    ListenEvent::Progress(frontier) => {
                        self.progress_exclusive = frontier
                            .into_option()
                            .expect("nothing should close the txns shard");
                        continue;
                    }
                    ListenEvent::Updates(updates) => updates,
                };
                // Persist emits the times sorted by little endian encoding,
                // which is not what we want. If we ever expose an interface for
                // registering and committing to a data shard at the same
                // timestamp, this will also have to sort registrations first.
                updates.sort_by(|(_, at, _), (_, bt, _)| at.cmp(bt));
                for ((k, v), t, d) in updates {
                    let k = k.expect("valid key");
                    let v = v.expect("valid val");
                    match C::decode(k, v) {
                        TxnsEntry::Register(data_id) => self.push_register(data_id, t, d),
                        TxnsEntry::Append(data_id, batch) => self.push_append(data_id, batch, t, d),
                    }
                }
            }
        }
        debug!(
            "cache correct before {:?} len={} least_ts={:?}",
            self.progress_exclusive,
            self.unapplied_batches.len(),
            self.unapplied_batches
                .first_key_value()
                .map(|(_, (_, _, ts))| ts),
        );
    }

    fn push_register(&mut self, data_id: ShardId, ts: T, diff: i64) {
        debug!(
            "cache learned {:.9} registered t={:?}",
            data_id.to_string(),
            ts
        );
        debug_assert!(ts >= self.progress_exclusive);
        assert_eq!(diff, 1);

        let prev = self.datas.insert(data_id, [ts.clone()].into());
        if let Some(prev) = prev {
            panic!("{} registered at both {:?} and {:?}", data_id, prev, ts);
        }
    }

    fn push_append(&mut self, data_id: ShardId, batch: Vec<u8>, ts: T, diff: i64) {
        debug!(
            "cache learned {:.9} b={} t={:?} d={}",
            data_id.to_string(),
            batch.hashed(),
            ts,
            diff,
        );
        debug_assert!(ts >= self.progress_exclusive);

        if diff == 1 {
            let idx = self.next_batch_id;
            self.next_batch_id += 1;
            // TODO(txn): Pretty sure we could accidentally end up with a dup.
            // Add some randomness to uncommitted batches before turning this on
            // anywhere important.
            let prev = self.batch_idx.insert(batch.clone(), idx);
            assert_eq!(prev, None);
            let prev = self
                .unapplied_batches
                .insert(idx, (data_id, batch, ts.clone()));
            assert_eq!(prev, None);
            self.datas
                .get_mut(&data_id)
                .expect("data is initialized")
                .push(ts);
        } else if diff == -1 {
            let idx = self
                .batch_idx
                .remove(&batch)
                .expect("invariant violation: batch should exist");
            let prev = self
                .unapplied_batches
                .remove(&idx)
                .expect("invariant violation: batch index should exist");
            debug_assert_eq!(data_id, prev.0);
            debug_assert_eq!(batch, prev.1);
            // Insertion timestamp should be less equal retraction timestamp.
            debug_assert!(prev.2 <= ts);
        } else {
            unreachable!("only +1/-1 diffs are used");
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.batch_idx.len() != self.unapplied_batches.len() {
            return Err(format!(
                "expected index len {} to match what it's indexing {}",
                self.batch_idx.len(),
                self.unapplied_batches.len()
            ));
        }

        let mut prev_ts = T::minimum();
        for (idx, (_, batch, ts)) in self.unapplied_batches.iter() {
            if self.batch_idx.get(batch) != Some(idx) {
                return Err(format!(
                    "expected batch to be indexed at {} got {:?}",
                    idx,
                    self.batch_idx.get(batch)
                ));
            }
            if ts < &prev_ts {
                return Err(format!(
                    "unapplied timestamp {:?} out of order after {:?}",
                    ts, prev_ts
                ));
            }
            prev_ts = ts.clone();
        }

        for (data_id, times) in self.datas.iter() {
            if times.is_empty() {
                return Err(format!(
                    "expected at least a registration time for {}",
                    data_id
                ));
            }
            let mut prev_ts = T::minimum();
            for ts in times.iter() {
                if ts < &prev_ts {
                    return Err(format!(
                        "write timestamp {:?} out of order after {:?}",
                        ts, prev_ts
                    ));
                }
                prev_ts = ts.clone();
            }
        }

        Ok(())
    }
}

/// A token exchangeable for a data shard snapshot.
///
/// - Invariant: `latest_write <= as_of < empty_to`
/// - Invariant: `(latest_write, empty_to)` has no committed writes (which means
///   we can do an empty CaA of those times if we like).
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct DataSnapshot<T> {
    data_id: ShardId,
    latest_write: T,
    as_of: T,
    empty_to: T,
}

impl<T: Timestamp + Lattice + TotalOrder + Codec64> DataSnapshot<T> {
    /// Unblocks reading a snapshot at `self.as_of` by waiting for the latest
    /// write before that time and then running an empty CaA if necessary.
    pub(crate) async fn unblock_read<K, V, D>(&self, mut data_write: WriteHandle<K, V, T, D>)
    where
        K: Debug + Codec,
        V: Debug + Codec,
        D: Semigroup + Codec64 + Send + Sync,
    {
        // First block until the latest write has been applied.
        let () = data_write
            .wait_for_upper_past(&Antichain::from_elem(self.latest_write.clone()))
            .await;

        // Now fill `(latest_write,as_of]` is filled with empty updates, so we
        // can read the shard at as_of normally.
        //
        // It's quite counter-intuitive for reads to involve writes, but I think
        // this is fine. In particular, because writing empty updates to a
        // persist shard is a metadata-only operation. It might result in things
        // like GC maintenance or a CRDB write, but this is also true for
        // registering a reader. On the balance, I think this is a _much_ better
        // set of tradeoffs than the original plan of trying to translate read
        // timestamps to the most recent write and reading that.
        let mut data_upper = data_write
            .shared_upper()
            .into_option()
            .expect("data shard should not be closed");
        while data_upper <= self.as_of {
            // It would be very bad if we accidentally filled any times <=
            // latest_write with empty updates, so defensively assert on each
            // iteration through the loop.
            assert!(self.latest_write < data_upper);
            debug!(
                "CaA data snapshot {:.9} [{:?},{:?})",
                self.data_id.to_string(),
                data_upper,
                self.empty_to,
            );
            assert!(self.latest_write <= self.as_of);
            assert!(self.as_of < self.empty_to);
            let res = data_write
                .compare_and_append_batch(
                    &mut [],
                    Antichain::from_elem(data_upper.clone()),
                    Antichain::from_elem(self.empty_to.clone()),
                )
                .await
                .expect("usage was valid");
            match res {
                Ok(()) => {
                    debug!(
                        "CaA data snapshot {:.9} [{:?},{:?}) success",
                        self.data_id.to_string(),
                        data_upper,
                        self.empty_to,
                    );
                    // Persist registers writes on the first write, so politely
                    // expire the writer we just created, but (as a performance
                    // optimization) only if we actually wrote something.
                    data_write.expire().await;
                    break;
                }
                Err(UpperMismatch { current, .. }) => {
                    let current = current
                        .into_option()
                        .expect("txns shard should not be closed");
                    debug!(
                        "CaA data snapshot {:.9} [{:?},{:?}) mismatch actual={:?}",
                        self.data_id.to_string(),
                        data_upper,
                        self.empty_to,
                        current,
                    );
                    data_upper = current;
                    continue;
                }
            }
        }
    }

    /// See [ReadHandle::snapshot_and_fetch].
    pub async fn snapshot_and_fetch<K, V, D>(
        &self,
        data_read: &mut ReadHandle<K, V, T, D>,
        // TODO(txn): It's quite surprising to require a WriteHandle for reads,
        // see what we can do about making this nicer.
        data_write: WriteHandle<K, V, T, D>,
    ) -> Result<Vec<((Result<K, String>, Result<V, String>), T, D)>, Since<T>>
    where
        K: Debug + Codec + Ord,
        V: Debug + Codec + Ord,
        D: Semigroup + Codec64 + Send + Sync,
    {
        self.unblock_read(data_write).await;
        data_read
            .snapshot_and_fetch(Antichain::from_elem(self.as_of.clone()))
            .await
    }

    fn validate(&self) -> Result<(), String> {
        if !(self.latest_write <= self.as_of) {
            return Err(format!(
                "latest_write {:?} not <= as_of {:?}",
                self.latest_write, self.as_of
            ));
        }
        if !(self.as_of < self.empty_to) {
            return Err(format!(
                "as_of {:?} not < empty_to {:?}",
                self.as_of, self.empty_to
            ));
        }
        Ok(())
    }
}

/// The next action to take in a data shard `Listen`.
///
/// See [TxnsCache::data_listen_next].
#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub enum DataListenNext<T> {
    /// Read the data shard normally, until this timestamp is less than what has
    /// been read.
    ReadDataPast(T),
    /// It is known there there are no writes between the progress given to the
    /// `data_listen_next` call and this timestamp. Advance the data shard
    /// listen progress to this (exclusive) frontier.
    EmitLogicalProgress(T),
    /// The data shard listen has caught up to what has been written to the txns
    /// shard. Wait for it to progress with `update_gt` and call
    /// `data_listen_next` again.
    WaitForTxnsProgress,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mz_persist_client::{Diagnostics, PersistClient, ShardIdSchema};
    use mz_persist_types::codec_impls::VecU8Schema;
    use DataListenNext::*;

    use crate::operator::DataSubscribe;
    use crate::tests::{reader, writer};
    use crate::txns::TxnsHandle;

    use super::*;

    impl TxnsCache<u64, TxnsCodecDefault> {
        pub(crate) async fn expect_open(
            init_ts: u64,
            txns: &TxnsHandle<String, (), u64, i64>,
        ) -> Self {
            let txns_read = txns
                .datas
                .client
                .open_leased_reader(
                    txns.txns_id(),
                    Arc::new(ShardIdSchema),
                    Arc::new(VecU8Schema),
                    Diagnostics::for_tests(),
                )
                .await
                .unwrap();
            let mut ret = TxnsCache::open(txns_read).await;
            ret.update_gt(&init_ts).await;
            ret
        }

        pub(crate) async fn expect_snapshot(
            &mut self,
            client: &PersistClient,
            data_id: ShardId,
            as_of: u64,
        ) -> Vec<String> {
            let mut data_read = reader(client, data_id).await;
            let data_write = writer(client, data_id).await;
            self.update_gt(&as_of).await;
            let mut snapshot = self
                .data_snapshot(&data_read.shard_id(), as_of)
                .unwrap()
                .snapshot_and_fetch(&mut data_read, data_write)
                .await
                .unwrap();
            snapshot.sort();
            snapshot
                .into_iter()
                .flat_map(|((k, v), _t, d)| {
                    let (k, ()) = (k.unwrap(), v.unwrap());
                    std::iter::repeat(k).take(usize::try_from(d).unwrap())
                })
                .collect()
        }

        pub(crate) fn expect_subscribe(
            &self,
            client: &PersistClient,
            data_id: ShardId,
            as_of: u64,
        ) -> DataSubscribe {
            DataSubscribe::new("test", client.clone(), self._txns_id, data_id, as_of)
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn data_snapshot() {
        fn ds(data_id: ShardId, latest_write: u64, as_of: u64, empty_to: u64) -> DataSnapshot<u64> {
            DataSnapshot {
                data_id,
                latest_write,
                as_of,
                empty_to,
            }
        }

        let mut txns = TxnsHandle::expect_open(PersistClient::new_for_tests().await).await;
        let log = txns.new_log();
        let d0 = ShardId::new();
        txns.expect_commit_at(3, d0, &[], &log).await;

        let mut cache = TxnsCache::expect_open(3, &txns).await;
        assert_eq!(cache.progress_exclusive, 4);

        // We can answer inclusive queries at < progress. The data shard isn't
        // registered yet, so it errors.
        assert!(cache.data_snapshot(&d0, 3).is_err());

        // Register a data shard. Registration advances the physical upper, so
        // we're able to read before and at the register ts. We couldn't read at
        // 3 before, but we can now.
        cache.push_register(d0, 4, 1);
        cache.progress_exclusive = 5;
        assert_eq!(cache.data_snapshot(&d0, 3), Ok(ds(d0, 3, 3, 4)));
        assert_eq!(cache.data_snapshot(&d0, 4), Ok(ds(d0, 4, 4, 5)));

        // Advance time. Now we can resolve queries at higher times. Nothing got
        // written at 5, so we still read as_of 4.
        cache.progress_exclusive = 6;
        assert_eq!(cache.data_snapshot(&d0, 5), Ok(ds(d0, 4, 5, 6)));

        // Write a batch. Apply will advance the physical upper, so we read
        // as_of the commit ts.
        cache.push_append(d0, vec![0xA0], 6, 1);
        cache.progress_exclusive = 7;
        assert_eq!(cache.data_snapshot(&d0, 6), Ok(ds(d0, 6, 6, 7)));

        // An unrelated batch doesn't change the answer. Neither does retracting
        // the batch.
        let other = ShardId::new();
        cache.push_register(other, 7, 1);
        cache.push_append(other, vec![0xB0], 7, 1);
        cache.push_append(d0, vec![0xA0], 7, -1);
        cache.progress_exclusive = 8;
        assert_eq!(cache.data_snapshot(&d0, 7), Ok(ds(d0, 6, 7, 8)));

        // All of the previous answers are still the same.
        assert_eq!(cache.data_snapshot(&d0, 3), Ok(ds(d0, 3, 3, 4)));
        assert_eq!(cache.data_snapshot(&d0, 4), Ok(ds(d0, 4, 4, 6)));
        assert_eq!(cache.data_snapshot(&d0, 5), Ok(ds(d0, 4, 5, 6)));
        assert_eq!(cache.data_snapshot(&d0, 6), Ok(ds(d0, 6, 6, 8)));
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn txns_cache_data_listen_next() {
        let mut txns = TxnsHandle::expect_open(PersistClient::new_for_tests().await).await;
        let log = txns.new_log();
        let d0 = ShardId::new();
        txns.expect_commit_at(3, d0, &[], &log).await;

        let mut cache = TxnsCache::expect_open(3, &txns).await;
        assert_eq!(cache.progress_exclusive, 4);

        // We can answer exclusive queries at <= progress. The data shard isn't
        // registered yet, so they error.
        assert!(cache.data_listen_next(&d0, 3).is_err());
        assert!(cache.data_listen_next(&d0, 4).is_err());

        // Register a data shard. Registration advances the physical upper, so
        // we're able to read at the register ts.
        cache.push_register(d0, 4, 1);
        cache.progress_exclusive = 5;
        assert_eq!(cache.data_listen_next(&d0, 4), Ok(ReadDataPast(4)));
        assert_eq!(cache.data_listen_next(&d0, 5), Ok(WaitForTxnsProgress));

        // Advance time. Now we can resolve queries at higher times. Nothing got
        // written at 5, so the physical upper hasn't moved, but we're now
        // guaranteed that `[5, 6)` is empty.
        cache.progress_exclusive = 6;
        assert_eq!(cache.data_listen_next(&d0, 5), Ok(EmitLogicalProgress(6)));

        // Write a batch. Apply will advance the physical upper, so now we need
        // to wait for the data upper to advance past the commit ts.
        cache.push_append(d0, vec![0xA0], 6, 1);
        cache.progress_exclusive = 7;
        assert_eq!(cache.data_listen_next(&d0, 6), Ok(ReadDataPast(6)));

        // An unrelated batch is another empty space guarantee. Ditto retracting
        // the batch.
        let other = ShardId::new();
        cache.push_register(other, 7, 1);
        cache.push_append(other, vec![0xB0], 7, 1);
        cache.push_append(d0, vec![0xA0], 7, -1);
        cache.progress_exclusive = 8;
        assert_eq!(cache.data_listen_next(&d0, 7), Ok(EmitLogicalProgress(8)));

        // Unlike a snapshot, the previous answers have changed! This is because
        // it's silly (inefficient) to walk through the state machine for a big
        // range of time once you know of a later write that advances the
        // physical upper.
        assert_eq!(cache.data_listen_next(&d0, 3), Ok(ReadDataPast(6)));
        assert_eq!(cache.data_listen_next(&d0, 4), Ok(ReadDataPast(6)));
        assert_eq!(cache.data_listen_next(&d0, 5), Ok(ReadDataPast(6)));
        assert_eq!(cache.data_listen_next(&d0, 6), Ok(ReadDataPast(6)));
    }
}
