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
use mz_ore::collections::HashMap;
use mz_persist_client::read::{ListenEvent, ReadHandle, Subscribe};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::ShardId;
use timely::progress::Antichain;
use tracing::debug;

use crate::error::NotRegistered;

/// A cache of the txn shard contents, optimized for various in-memory
/// operations.
///
/// # Implementation Details
///
/// Reads of data shards are almost as straightforward as writes. A data shard
/// may be read normally, using snapshots, subscriptions, shard_source, etc,
/// through the most recent non-empty write. However, the upper of the txn shard
/// (and thus the logical upper of the data shard) may be arbitrarily far ahead
/// of the upper of the data shard. As a result, we have to "translate"
/// timestamps by synthesizing ranges of empty time that have not yet been
/// committed (but eventually will be):
///
/// - To take a snapshot of a data shard, the `as_of` is passed through
///   unchanged if the timestamp of that shard's latest non-empty write is past
///   it. Otherwise, the timestamp of the latest non-empty write is used.
/// - To iterate a listen on a data shard, when writes haven't been read yet
///   they are passed through unchanged, otherwise if the txn shard indicates
///   that there are ranges of empty time progress is returned, otherwise the
///   txn shard will indicate when new information is available.
///
/// Note that all of the above can be determined solely by information in the
/// txn shard. In particular, non-empty writes are indicated by updates with
/// positive diffs.
///
/// Also note that the above is structured such that it is possible to write a
/// timely operator with the data shard as an input, passing on all payloads
/// unchanged and simply manipulating capabilities in response to data and txn
/// shard progress. TODO(txn): Link to operator once it's added.
#[derive(Debug)]
pub struct TxnsCache {
    pub(crate) progress_exclusive: u64,
    txns_subscribe: Subscribe<ShardId, String, u64, i64>,

    next_batch_id: usize,
    /// The batches needing application as of the current progress.
    ///
    /// This is indexed by a "batch id" that is internal to this object because
    /// timestamps are not unique.
    ///
    /// Invariant: Values are sorted by timestamp.
    unapplied_batches: BTreeMap<usize, (ShardId, String, u64)>,
    /// An index into `unapplied_batches` keyed by the serialized batch.
    batch_idx: HashMap<String, usize>,
    /// The times at which each data shard has been written.
    ///
    /// Invariant: Times in the Vec are in ascending order.
    pub(crate) datas: BTreeMap<ShardId, Vec<u64>>,
}

impl TxnsCache {
    pub(crate) async fn init(
        init_ts: u64,
        txns_read: ReadHandle<ShardId, String, u64, i64>,
        txns_write: &mut WriteHandle<ShardId, String, u64, i64>,
    ) -> Self {
        let () = crate::empty_caa(|| "txns init", txns_write, init_ts).await;
        Self::open(init_ts, txns_read).await
    }

    async fn open(init_ts: u64, txns_read: ReadHandle<ShardId, String, u64, i64>) -> Self {
        // TODO(txn): Figure out the compaction story. This might require
        // sorting inserts before retractions within each timestamp.
        let subscribe = txns_read
            .subscribe(Antichain::from_elem(0))
            .await
            .expect("handle holds a capability");
        let mut ret = TxnsCache {
            progress_exclusive: 0,
            txns_subscribe: subscribe,
            next_batch_id: 0,
            unapplied_batches: BTreeMap::new(),
            batch_idx: HashMap::new(),
            datas: BTreeMap::new(),
        };
        ret.update_gt(init_ts).await;
        ret
    }

    /// Returns the minimum timestamp at which the data shard can be read.
    ///
    /// Returns Err if that data has not been registered.
    pub fn data_since(&self, data_id: &ShardId) -> Result<u64, NotRegistered> {
        let time = self
            .datas
            .get(data_id)
            .and_then(|times| times.first().copied());
        time.ok_or(NotRegistered {
            data_id: *data_id,
            ts: self.progress_exclusive,
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
    pub fn to_data_inclusive(&self, data_id: &ShardId, ts: u64) -> Result<u64, NotRegistered> {
        assert!(self.progress_exclusive > ts);
        self.datas
            .get(data_id)
            .and_then(|all| {
                let ret = all.iter().rev().find(|x| **x <= ts).copied();
                debug!(
                    "to_data_inclusive {:.9} t={} ret={:?}: all={:?}",
                    data_id.to_string(),
                    ts,
                    ret,
                    all,
                );
                ret
            })
            .ok_or(NotRegistered {
                data_id: *data_id,
                ts: self.progress_exclusive,
            })
    }

    /// Returns the batches needing application as of the current progress.
    pub(crate) fn unapplied_batches(&self) -> impl Iterator<Item = &(ShardId, String, u64)> {
        self.unapplied_batches.values()
    }

    /// Invariant: afterward, self.progress_exclusive will be > ts
    pub(crate) async fn update_gt(&mut self, ts: u64) {
        self.update(|progress_exclusive| progress_exclusive > ts)
            .await;
        debug_assert!(self.progress_exclusive > ts);
        debug_assert_eq!(self.validate(), Ok(()));
    }

    /// Invariant: afterward, self.progress_exclusive will be >= ts
    pub(crate) async fn update_ge(&mut self, ts: u64) {
        self.update(|progress_exclusive| progress_exclusive >= ts)
            .await;
        debug_assert!(self.progress_exclusive >= ts);
        debug_assert_eq!(self.validate(), Ok(()));
    }

    async fn update<F: Fn(u64) -> bool>(&mut self, done: F) {
        while !done(self.progress_exclusive) {
            let events = self.txns_subscribe.fetch_next().await;
            for event in events {
                let mut updates = match event {
                    ListenEvent::Progress(frontier) => {
                        self.progress_exclusive = *frontier
                            .as_option()
                            .expect("nothing should close the txns shard");
                        continue;
                    }
                    ListenEvent::Updates(updates) => updates,
                };
                // Persist emits the times sorted by little endian encoding,
                // which is not what we want
                updates.sort_by(|(akv, at, _), (bkv, bt, _)| (at, akv).cmp(&(bt, bkv)));
                for ((k, v), t, d) in updates {
                    let data_id = k.expect("valid data_id");
                    let batch = v.expect("valid batch");
                    self.push(data_id, batch, t, d);
                }
            }
        }
        debug!(
            "cache correct before {} len={} least_ts={:?}",
            self.progress_exclusive,
            self.unapplied_batches.len(),
            self.unapplied_batches
                .first_key_value()
                .map(|(_, (_, _, ts))| ts),
        );
    }

    fn push(&mut self, data_id: ShardId, batch: String, ts: u64, diff: i64) {
        if batch.is_empty() {
            assert_eq!(diff, 1);
            // This is just a data registration.
            debug!(
                "cache learned {:.9} registered t={}",
                data_id.to_string(),
                ts
            );
            let prev = self.datas.insert(data_id, vec![ts]);
            if let Some(prev) = prev {
                panic!("{} registered at both {:?} and {}", data_id, prev, ts);
            }
            return;
        }
        debug!(
            "cache learned {:.9} b={} t={} d={}",
            data_id.to_string(),
            batch.hashed(),
            ts,
            diff,
        );
        if diff == 1 {
            let idx = self.next_batch_id;
            self.next_batch_id += 1;
            // TODO(txn): Pretty sure we could accidentally end up with a dup.
            // Add some randomness to uncommitted batches before turning this on
            // anywhere important.
            let prev = self.batch_idx.insert(batch.clone(), idx);
            assert_eq!(prev, None);
            let prev = self.unapplied_batches.insert(idx, (data_id, batch, ts));
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
        // TODO(txn): Replace asserts with return Err.
        assert_eq!(self.unapplied_batches.len(), self.batch_idx.len());

        let mut prev_ts = 0;
        for (idx, (_, batch, ts)) in self.unapplied_batches.iter() {
            assert_eq!(batch.is_empty(), false);
            assert_eq!(self.batch_idx.get(batch), Some(idx));
            assert!(ts >= &prev_ts);
            prev_ts = *ts;
        }

        for (_, times) in self.datas.iter() {
            assert_eq!(times.is_empty(), false);
            let mut prev_ts = 0;
            for ts in times.iter() {
                assert!(prev_ts < *ts);
                prev_ts = *ts;
            }
        }

        Ok(())
    }
}
