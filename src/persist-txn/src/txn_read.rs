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

use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use mz_ore::collections::HashMap;
use mz_persist_client::read::{ListenEvent, ReadHandle, Subscribe};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::ShardId;
use mz_persist_types::{Codec64, StepForward};
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
/// shard progress. TODO(txn): Link to operator once it's added.
#[derive(Debug)]
pub struct TxnsCache<T: Timestamp + Lattice + Codec64, C: TxnsCodec = TxnsCodecDefault> {
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
        Self::open(init_ts, txns_read).await
    }

    pub(crate) async fn open(init_ts: T, txns_read: ReadHandle<C::Key, C::Val, T, i64>) -> Self {
        // TODO(txn): Figure out the compaction story. This might require
        // sorting inserts before retractions within each timestamp.
        let subscribe = txns_read
            .subscribe(Antichain::from_elem(T::minimum()))
            .await
            .expect("handle holds a capability");
        let mut ret = TxnsCache {
            progress_exclusive: T::minimum(),
            txns_subscribe: subscribe,
            next_batch_id: 0,
            unapplied_batches: BTreeMap::new(),
            batch_idx: HashMap::new(),
            datas: BTreeMap::new(),
        };
        ret.update_gt(&init_ts).await;
        ret
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
    /// Returns Err if that data has not been registered.
    pub fn to_data_inclusive(&self, data_id: &ShardId, ts: T) -> Result<T, NotRegistered<T>> {
        assert!(self.progress_exclusive > ts);
        self.datas
            .get(data_id)
            .and_then(|all| {
                let ret = all.iter().rev().find(|x| **x <= ts).cloned();
                debug!(
                    "to_data_inclusive {:.9} t={:?} ret={:?}: all={:?}",
                    data_id.to_string(),
                    ts,
                    ret,
                    all,
                );
                ret
            })
            .ok_or(NotRegistered {
                data_id: *data_id,
                ts: self.progress_exclusive.clone(),
            })
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mz_persist_client::{Diagnostics, PersistClient, ShardIdSchema};
    use mz_persist_types::codec_impls::VecU8Schema;

    use crate::tests::reader;
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
            TxnsCache::open(init_ts, txns_read).await
        }

        pub(crate) async fn expect_snapshot(
            &mut self,
            client: &PersistClient,
            data_id: ShardId,
            as_of: u64,
        ) -> Vec<String> {
            let mut data_read = reader(client, data_id).await;
            self.update_gt(&as_of).await;
            let translated_as_of = self
                .to_data_inclusive(&data_read.shard_id(), as_of)
                .unwrap();
            let mut snapshot = data_read
                .snapshot_and_fetch(Antichain::from_elem(translated_as_of))
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
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn txns_cache_to_data_inclusive() {
        let mut txns = TxnsHandle::expect_open(PersistClient::new_for_tests().await).await;
        let log = txns.new_log();
        let d0 = ShardId::new();
        txns.expect_commit_at(3, d0, &[], &log).await;

        let mut cache = TxnsCache::expect_open(3, &txns).await;
        assert_eq!(cache.progress_exclusive, 4);

        // We can answer inclusive queries at < progress. The data shard isn't
        // registered yet, so it errors.
        assert!(cache.to_data_inclusive(&d0, 3).is_err());

        // Register a data shard. Registration advances the physical upper, so
        // we're able to read at the register ts.
        cache.push_register(d0, 4, 1);
        cache.progress_exclusive = 5;
        assert_eq!(cache.to_data_inclusive(&d0, 4), Ok(4));

        // Advance time. Now we can resolve queries at higher times. Nothing got
        // written at 5, so we still read as_of 4.
        cache.progress_exclusive = 6;
        assert_eq!(cache.to_data_inclusive(&d0, 5), Ok(4));

        // Write a batch. Apply will advance the physical upper, so we read
        // as_of the commit ts.
        cache.push_append(d0, vec![0xA0], 6, 1);
        cache.progress_exclusive = 7;
        assert_eq!(cache.to_data_inclusive(&d0, 6), Ok(6));

        // An unrelated batch doesn't change the answer. Neither does retracting
        // the batch.
        let other = ShardId::new();
        cache.push_register(other, 7, 1);
        cache.push_append(other, vec![0xB0], 7, 1);
        cache.push_append(d0, vec![0xA0], 7, -1);
        cache.progress_exclusive = 8;
        assert_eq!(cache.to_data_inclusive(&d0, 7), Ok(6));

        // All of the previous answers are still the same.
        assert!(cache.to_data_inclusive(&d0, 3).is_err());
        assert_eq!(cache.to_data_inclusive(&d0, 4), Ok(4));
        assert_eq!(cache.to_data_inclusive(&d0, 5), Ok(4));
        assert_eq!(cache.to_data_inclusive(&d0, 6), Ok(6));
    }
}
