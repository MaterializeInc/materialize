// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A cache of the txn shard contents.

use std::cmp::{min, Reverse};
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::collections::HashMap;
use mz_ore::instrument;
use mz_persist_client::fetch::LeasedBatchPart;
use mz_persist_client::metrics::encode_ts_metric;
use mz_persist_client::read::{ListenEvent, ReadHandle, Subscribe};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::{Codec64, StepForward};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tracing::debug;

use crate::metrics::Metrics;
use crate::txn_read::{DataListenNext, DataSnapshot};
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
/// ahead of the physical upper of the data shard. As a result, we do the
/// following:
///
/// - To take a snapshot of a data shard, the `as_of` is passed through
///   unchanged if the timestamp of that shard's latest non-empty write is past
///   it. Otherwise, we know the times between them have no writes and can fill
///   them with empty updates. Concretely, to read a snapshot as of `T`:
///   - We read the txns shard contents up through and including `T`, blocking
///     until the upper passes `T` if necessary.
///   - We then find, for the requested data shard, the latest non-empty write
///     at a timestamp `T' <= T`.
///   - We wait for `T'` to be applied by watching the data shard upper.
///   - We `compare_and_append` empty updates for `(T', T]`, which is known by
///     the txn system to not have writes for this shard (otherwise we'd have
///     picked a different `T'`).
///   - We read the snapshot at `T` as normal.
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
pub struct TxnsCacheState<T: Timestamp + Lattice + Codec64> {
    txns_id: ShardId,
    /// Invariant: <= the minimum unapplied batch.
    /// Invariant: <= the minimum unapplied register.
    since_ts: T,
    pub(crate) progress_exclusive: T,

    next_batch_id: usize,
    /// The batches needing application as of the current progress.
    ///
    /// This is indexed by a "batch id" that is internal to this object because
    /// timestamps are not unique.
    ///
    /// Invariant: Values are sorted by timestamp.
    pub(crate) unapplied_batches: BTreeMap<usize, (ShardId, Vec<u8>, T)>,
    /// An index into `unapplied_batches` keyed by the serialized batch.
    batch_idx: HashMap<Vec<u8>, usize>,
    /// The times at which each data shard has been written.
    pub(crate) datas: BTreeMap<ShardId, DataTimes<T>>,
    /// The registers and forgets needing application as of the current progress.
    ///
    /// Invariant: Values are sorted by timestamp.
    pub(crate) unapplied_registers: VecDeque<(ShardId, T)>,

    /// Invariant: Contains the minimum write time (if any) for each value in
    /// `self.datas`.
    datas_min_write_ts: BinaryHeap<Reverse<(T, ShardId)>>,

    /// If Some, this cache only tracks the indicated data shard as a
    /// performance optimization. When used, only some methods (in particular,
    /// the ones necessary for the txns_progress operator) are supported.
    ///
    /// TODO: It'd be nice to make this a compile time thing. I have some ideas,
    /// but they're decently invasive, so leave it for a followup.
    only_data_id: Option<ShardId>,
}

/// A self-updating [TxnsCacheState].
#[derive(Debug)]
pub struct TxnsCache<T: Timestamp + Lattice + Codec64, C: TxnsCodec = TxnsCodecDefault> {
    pub(crate) txns_subscribe: Subscribe<C::Key, C::Val, T, i64>,
    pub(crate) buf: Vec<(TxnsEntry, T, i64)>,
    state: TxnsCacheState<T>,
}

impl<T: Timestamp + Lattice + TotalOrder + StepForward + Codec64> TxnsCacheState<T> {
    pub(crate) fn new(txns_id: ShardId, since_ts: T, only_data_id: Option<ShardId>) -> Self {
        TxnsCacheState {
            txns_id,
            since_ts,
            progress_exclusive: T::minimum(),
            next_batch_id: 0,
            unapplied_batches: BTreeMap::new(),
            batch_idx: HashMap::new(),
            datas: BTreeMap::new(),
            unapplied_registers: VecDeque::new(),
            datas_min_write_ts: BinaryHeap::new(),
            only_data_id,
        }
    }

    /// Returns the [ShardId] of the txns shard.
    pub fn txns_id(&self) -> ShardId {
        self.txns_id
    }

    /// Returns whether the data shard was registered to the txns set at the
    /// given timestamp.
    ///
    /// Specifically, a data shard is registered at a timestamp `ts` if it has a
    /// `register_ts <= ts` but no `forget_ts >= ts`.
    pub fn registered_at(&self, data_id: &ShardId, ts: &T) -> bool {
        self.assert_only_data_id(data_id);
        let Some(data_times) = self.datas.get(data_id) else {
            return false;
        };
        data_times.registered.iter().any(|x| x.contains(ts))
    }

    /// Returns the set of all data shards registered to the txns set at the
    /// given timestamp. See [Self::registered_at].
    pub(crate) fn all_registered_at(&self, ts: &T) -> Vec<ShardId> {
        assert_eq!(self.only_data_id, None);
        assert!(self.progress_exclusive >= *ts);
        self.datas
            .iter()
            .filter(|(_, data_times)| data_times.registered.iter().any(|x| x.contains(ts)))
            .map(|(data_id, _)| *data_id)
            .collect()
    }

    /// Returns a token exchangeable for a snapshot of a data shard.
    ///
    /// A data shard might be definite at times past the physical upper because
    /// of invariants maintained by this txn system. As a result, this method
    /// discovers the latest write before the `as_of`.
    ///
    /// Callers must first wait for `update_gt` with the same or later timestamp
    /// to return. Panics otherwise.
    ///
    /// Returns Err if that data shard has not been registered at the given ts.
    pub fn data_snapshot(&self, data_id: ShardId, as_of: T) -> DataSnapshot<T> {
        self.assert_only_data_id(&data_id);
        assert!(self.progress_exclusive > as_of);
        let Some(all) = self.datas.get(&data_id) else {
            // Not registered at this time, so we know there are no unapplied
            // writes.
            return DataSnapshot {
                data_id,
                latest_write: None,
                as_of,
                empty_to: self.progress_exclusive.clone(),
            };
        };
        let latest_write = all.writes.iter().rev().find(|x| **x <= as_of).cloned();
        let empty_to = all
            .writes
            .iter()
            .find(|x| as_of < **x)
            .unwrap_or(&self.progress_exclusive)
            .clone();
        debug!(
            "data_snapshot {:.9} latest_write={:?} as_of={:?} empty_to={:?}: all={:?}",
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
        ret
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
    pub fn data_listen_next(&self, data_id: &ShardId, ts: T) -> DataListenNext<T> {
        self.assert_only_data_id(data_id);
        assert!(self.progress_exclusive >= ts);
        use DataListenNext::*;
        let data_times = self.datas.get(data_id);
        debug!(
            "data_listen_next {:.9} {:?}: progress={:?} times={:?}",
            data_id.to_string(),
            ts,
            self.progress_exclusive,
            data_times,
        );

        if ts < self.since_ts {
            return CompactedTo(self.since_ts.clone());
        }
        let Some(data_times) = data_times else {
            // Not registered, maybe it will be in the future? In the meantime,
            // treat it like a normal shard (i.e. pass through reads) and check
            // again later.
            if ts < self.progress_exclusive {
                return ReadDataTo(self.progress_exclusive.clone());
            } else {
                return WaitForTxnsProgress;
            }
        };
        // See if any txns writes are >= our timestamp, if so, we can read all
        // the way past the write.
        if let Some(latest_write) = data_times.writes.back() {
            if &ts <= latest_write {
                return ReadDataTo(latest_write.step_forward());
            }
        }

        // The most recent forget is set, which means it's not registered as of
        // the latest information we have. Read to the current progress point
        // normally.
        let last_reg = data_times.last_reg();
        if last_reg.forget_ts.is_some() {
            if ts < self.progress_exclusive {
                return ReadDataTo(self.progress_exclusive.clone());
            } else {
                // TODO(txn): Make sure to add a regression test for this when
                // we redo the data_listen_next unit test.
                return WaitForTxnsProgress;
            }
        }

        // If we're before the most recent registration, it's always safe to
        // read to that point normally.
        if ts < last_reg.register_ts {
            return ReadDataTo(last_reg.register_ts.clone());
        }

        // No writes were > ts, look to see if the txns upper has advanced
        // past ts.
        if ts < self.progress_exclusive {
            // Emitting logical progress at the wrong time is a correctness bug,
            // so be extra defensive about the necessary conditions: the most
            // recent registration is still active and we're in it.
            assert!(last_reg.forget_ts.is_none() && last_reg.contains(&ts));
            return EmitLogicalProgress(self.progress_exclusive.clone());
        }
        // Nope, all caught up, we have to wait.
        WaitForTxnsProgress
    }

    /// Returns the minimum timestamp not known to be applied by this cache.
    pub fn min_unapplied_ts(&self) -> &T {
        assert_eq!(self.only_data_id, None);
        // We maintain an invariant that the values in the unapplied_batches map
        // are sorted by timestamp, thus the first one must be the minimum.
        let min_batch_ts = self
            .unapplied_batches
            .first_key_value()
            .map(|(_, (_, _, ts))| ts)
            // If we don't have any known unapplied batches, then the next
            // timestamp that could be written must potentially have an
            // unapplied batch.
            .unwrap_or(&self.progress_exclusive);
        let min_register_ts = self
            .unapplied_registers
            .front()
            .map(|(_, ts)| ts)
            .unwrap_or(&self.progress_exclusive);

        min(min_batch_ts, min_register_ts)
    }

    /// Returns the operations needing application as of the current progress.
    pub(crate) fn unapplied(&self) -> impl Iterator<Item = (&ShardId, Unapplied, &T)> {
        assert_eq!(self.only_data_id, None);
        let registers = self
            .unapplied_registers
            .iter()
            .map(|(data_id, ts)| (data_id, Unapplied::RegisterForget, ts));
        let batches = self
            .unapplied_batches
            .values()
            .map(|(data_id, batch, ts)| (data_id, Unapplied::Batch(batch), ts));
        // This will emit registers and forgets before batches at the same timestamp. Currently,
        // this is fine because for a single data shard you can't combine registers, forgets, and
        // batches at the same timestamp. In the future if we allow combining these operations in
        // a single op, then we probably want to emit registers, then batches, then forgets or we
        // can make forget exclusive in which case we'd emit it before batches.
        registers.merge_by(batches, |(_, _, ts1), (_, _, ts2)| ts1 <= ts2)
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
        retractions: impl Iterator<Item = (&'a Vec<u8>, &'a ([u8; 8], ShardId))>,
    ) -> impl Iterator<Item = (&'a Vec<u8>, &'a ([u8; 8], ShardId))> {
        assert_eq!(self.only_data_id, None);
        assert!(&self.progress_exclusive >= expected_txns_upper);
        retractions.filter(|(batch_raw, _)| self.batch_idx.contains_key(*batch_raw))
    }

    /// Allows compaction to internal representations, losing the ability to
    /// answer queries about times less_than since_ts.
    ///
    /// To ensure that this is not `O(data shard)`, we update them lazily as
    /// they are touched in self.update.
    ///
    /// Callers must first wait for `update_ge` with the same or later timestamp
    /// to return. Panics otherwise.
    pub fn compact_to(&mut self, since_ts: &T) {
        // Make things easier on ourselves by allowing update to work on
        // un-compacted data.
        assert!(&self.progress_exclusive >= since_ts);

        // NB: This intentionally does not compact self.unapplied_batches,
        // because we aren't allowed to alter those timestamps. This is fine
        // because it and self.batch_idx are self-compacting, anyway.
        if &self.since_ts < since_ts {
            self.since_ts.clone_from(since_ts);
        } else {
            return;
        }
        self.compact_data_times()
    }

    pub(crate) fn push_entries(&mut self, mut entries: Vec<(TxnsEntry, T, i64)>, progress: T) {
        // Persist emits the times sorted by little endian encoding,
        // which is not what we want. If we ever expose an interface for
        // registering and committing to a data shard at the same
        // timestamp, this will also have to sort registrations first.
        entries.sort_by(|(a, _, _), (b, _, _)| a.ts::<T>().cmp(&b.ts::<T>()));
        for (e, t, d) in entries {
            match e {
                TxnsEntry::Register(data_id, ts) => {
                    let ts = T::decode(ts);
                    debug_assert!(ts <= t);
                    self.push_register(data_id, ts, d, t);
                }
                TxnsEntry::Append(data_id, ts, batch) => {
                    let ts = T::decode(ts);
                    debug_assert!(ts <= t);
                    self.push_append(data_id, batch, ts, d)
                }
            }
        }
        self.progress_exclusive = progress;
    }

    fn push_register(&mut self, data_id: ShardId, ts: T, diff: i64, compacted_ts: T) {
        self.assert_only_data_id(&data_id);
        // Since we keep the original non-advanced timestamp around, retractions
        // necessarily might be for times in the past, so `|| diff < 0`.
        debug_assert!(ts >= self.progress_exclusive || diff < 0);
        if let Some(only_data_id) = self.only_data_id.as_ref() {
            if only_data_id != &data_id {
                return;
            }
        }

        // The shard has not compacted past the register/forget ts, so it may not have been applied.
        if ts == compacted_ts {
            self.unapplied_registers.push_back((data_id, ts.clone()));
        }

        if diff == 1 {
            debug!(
                "cache learned {:.9} registered t={:?}",
                data_id.to_string(),
                ts
            );
            let entry = self.datas.entry(data_id).or_default();
            // Sanity check that if there is a registration, then we've closed
            // it off.
            if let Some(last_reg) = entry.registered.back() {
                assert!(last_reg.forget_ts.is_some())
            }
            entry.registered.push_back(DataRegistered {
                register_ts: ts,
                forget_ts: None,
            });
        } else if diff == -1 {
            debug!(
                "cache learned {:.9} forgotten t={:?}",
                data_id.to_string(),
                ts
            );
            let active_reg = self
                .datas
                .get_mut(&data_id)
                .and_then(|x| x.registered.back_mut())
                .expect("data shard should be registered before forget");
            assert_eq!(active_reg.forget_ts.replace(ts), None);
        } else {
            unreachable!("only +1/-1 diffs are used");
        }
    }

    fn push_append(&mut self, data_id: ShardId, batch: Vec<u8>, ts: T, diff: i64) {
        self.assert_only_data_id(&data_id);
        // Since we keep the original non-advanced timestamp around, retractions
        // necessarily might be for times in the past, so `|| diff < 0`.
        debug_assert!(ts >= self.progress_exclusive || diff < 0);
        if let Some(only_data_id) = self.only_data_id.as_ref() {
            if only_data_id != &data_id {
                return;
            }
        }

        if diff == 1 {
            debug!(
                "cache learned {:.9} committed t={:?} b={}",
                data_id.to_string(),
                ts,
                batch.hashed(),
            );
            let idx = self.next_batch_id;
            self.next_batch_id += 1;
            let prev = self.batch_idx.insert(batch.clone(), idx);
            assert_eq!(prev, None);
            let prev = self
                .unapplied_batches
                .insert(idx, (data_id, batch, ts.clone()));
            assert_eq!(prev, None);
            let times = self.datas.get_mut(&data_id).expect("data is initialized");
            if times.writes.is_empty() {
                self.datas_min_write_ts.push(Reverse((ts.clone(), data_id)));
            }
            times.writes.push_back(ts);
            self.compact_data_times();
        } else if diff == -1 {
            debug!(
                "cache learned {:.9} applied t={:?} b={}",
                data_id.to_string(),
                ts,
                batch.hashed(),
            );
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

    fn compact_data_times(&mut self) {
        debug!(
            "cache compact since={:?} min_writes={:?}",
            self.since_ts, self.datas_min_write_ts
        );
        loop {
            // Repeatedly grab the data shard with the minimum write_ts while it
            // (the minimum since_ts) is less_than the since_ts. If that results
            // in no registration and no writes, forget about the data shard
            // entirely, so it doesn't sit around in the map forever. This will
            // eventually finish because each data shard we compact will not
            // meet the compaction criteria if we see it again.
            //
            // NB: This intentionally doesn't compact the registration
            // timestamps if none of the writes need to be compacted, so that
            // compaction isn't `O(compact calls * data shards)`.
            let data_id = match self.datas_min_write_ts.peek() {
                Some(Reverse((ts, _))) if ts < &self.since_ts => {
                    let Reverse((_, data_id)) = self.datas_min_write_ts.pop().expect("just peeked");
                    data_id
                }
                Some(_) | None => break,
            };
            let times = self
                .datas
                .get_mut(&data_id)
                .expect("datas_min_write_ts should be an index into datas");
            debug!(
                "cache compact {:.9} since={:?} times={:?}",
                data_id.to_string(),
                self.since_ts,
                times
            );

            // Advance the registration times.
            while let Some(x) = times.registered.front_mut() {
                if let Some(forget_ts) = x.forget_ts.as_ref() {
                    if forget_ts < &self.since_ts {
                        times.registered.pop_front();
                        continue;
                    }
                }
                break;
            }

            // Pop any write times before since_ts. We can stop as soon
            // as something is >=.
            while let Some(write_ts) = times.writes.front() {
                if write_ts < &self.since_ts {
                    times.writes.pop_front();
                } else {
                    break;
                }
            }

            // Now re-insert it into datas_min_write_ts if non-empty.
            if let Some(ts) = times.writes.front() {
                self.datas_min_write_ts.push(Reverse((ts.clone(), data_id)));
            }

            if times.registered.is_empty() {
                assert!(times.writes.is_empty());
                self.datas.remove(&data_id);
            }
            debug!(
                "cache compact {:.9} DONE since={:?} times={:?}",
                data_id.to_string(),
                self.since_ts,
                self.datas.get(&data_id),
            );
        }
        debug!(
            "cache compact DONE since={:?} min_writes={:?}",
            self.since_ts, self.datas_min_write_ts
        );
        debug_assert_eq!(self.validate(), Ok(()));
    }

    pub(crate) fn update_gauges(&self, metrics: &Metrics) {
        metrics
            .data_shard_count
            .set(u64::cast_from(self.datas.len()));
        metrics
            .batches
            .unapplied_count
            .set(u64::cast_from(self.unapplied_batches.len()));
        let unapplied_batches_bytes = self
            .unapplied_batches
            .values()
            .map(|(_, x, _)| x.len())
            .sum::<usize>();
        metrics
            .batches
            .unapplied_bytes
            .set(u64::cast_from(unapplied_batches_bytes));
        metrics
            .batches
            .unapplied_min_ts
            .set(encode_ts_metric(&Antichain::from_elem(
                self.min_unapplied_ts().clone(),
            )));
    }

    fn assert_only_data_id(&self, data_id: &ShardId) {
        if let Some(only_data_id) = self.only_data_id.as_ref() {
            assert_eq!(data_id, only_data_id);
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

        let mut prev_batch_ts = T::minimum();
        for (idx, (_, batch, ts)) in self.unapplied_batches.iter() {
            if self.batch_idx.get(batch) != Some(idx) {
                return Err(format!(
                    "expected batch to be indexed at {} got {:?}",
                    idx,
                    self.batch_idx.get(batch)
                ));
            }
            if ts < &prev_batch_ts {
                return Err(format!(
                    "unapplied batch timestamp {:?} out of order after {:?}",
                    ts, prev_batch_ts
                ));
            }
            prev_batch_ts = ts.clone();
        }

        let mut prev_register_ts = T::minimum();
        for (_, ts) in self.unapplied_registers.iter() {
            if ts < &prev_register_ts {
                return Err(format!(
                    "unapplied register timestamp {:?} out of order after {:?}",
                    ts, prev_register_ts
                ));
            }
            prev_register_ts = ts.clone();
        }

        for (data_id, data_times) in self.datas.iter() {
            let () = data_times.validate(&self.since_ts)?;

            if let Some(ts) = data_times.writes.front() {
                assert!(&self.since_ts <= ts);
                // Oof, bummer to do this iteration.
                assert!(
                    self.datas_min_write_ts
                        .iter()
                        .any(|Reverse((t, d))| t == ts && d == data_id),
                    "{:?} {:?} missing from {:?}",
                    data_id,
                    ts,
                    self.datas_min_write_ts
                );
            }
        }

        for Reverse((ts, data_id)) in self.datas_min_write_ts.iter() {
            assert_eq!(
                self.datas.get(data_id).unwrap().writes.front().as_ref(),
                Some(&ts)
            );
        }

        Ok(())
    }
}

impl<T: Timestamp + Lattice + TotalOrder + StepForward + Codec64, C: TxnsCodec> TxnsCache<T, C> {
    pub(crate) async fn init(
        init_ts: T,
        txns_read: ReadHandle<C::Key, C::Val, T, i64>,
        txns_write: &mut WriteHandle<C::Key, C::Val, T, i64>,
    ) -> Self {
        let () = crate::empty_caa(|| "txns init", txns_write, init_ts.clone()).await;
        let as_of = txns_read.since().clone();
        let mut ret = Self::from_read(txns_read, as_of, None).await;
        ret.update_gt(&init_ts).await;
        ret
    }

    /// Returns a [TxnsCache] reading from the given txn shard.
    ///
    /// `txns_id` identifies which shard will be used as the txns WAL. MZ will
    /// likely have one of these per env, used by all processes and the same
    /// across restarts.
    pub async fn open(
        client: &PersistClient,
        txns_id: ShardId,
        only_data_id: Option<ShardId>,
    ) -> Self {
        let (txns_key_schema, txns_val_schema) = C::schemas();
        let txns_read = client
            .open_leased_reader(
                txns_id,
                Arc::new(txns_key_schema),
                Arc::new(txns_val_schema),
                Diagnostics {
                    shard_name: "txns".to_owned(),
                    handle_purpose: "read txns".to_owned(),
                },
            )
            .await
            .expect("txns schema shouldn't change");
        let as_of = txns_read.since().clone();
        Self::from_read(txns_read, as_of, only_data_id).await
    }

    pub(crate) async fn from_read(
        txns_read: ReadHandle<C::Key, C::Val, T, i64>,
        as_of: Antichain<T>,
        only_data_id: Option<ShardId>,
    ) -> Self {
        let txns_id = txns_read.shard_id();
        let since_ts = as_of.as_option().expect("txns shard is not closed").clone();
        let txns_subscribe = txns_read
            .subscribe(as_of)
            .await
            .expect("handle holds a capability");
        let state = TxnsCacheState::new(txns_id, since_ts, only_data_id);
        TxnsCache {
            txns_subscribe,
            buf: Vec::new(),
            state,
        }
    }

    /// Invariant: afterward, self.progress_exclusive will be > ts
    #[instrument(level = "debug", fields(ts = ?ts))]
    pub async fn update_gt(&mut self, ts: &T) {
        self.update(|progress_exclusive| progress_exclusive > ts)
            .await;
        debug_assert!(&self.progress_exclusive > ts);
        debug_assert_eq!(self.validate(), Ok(()));
    }

    /// Invariant: afterward, self.progress_exclusive will be >= ts
    #[instrument(level = "debug", fields(ts = ?ts))]
    pub async fn update_ge(&mut self, ts: &T) {
        self.update(|progress_exclusive| progress_exclusive >= ts)
            .await;
        debug_assert!(&self.progress_exclusive >= ts);
        debug_assert_eq!(self.validate(), Ok(()));
    }

    async fn update<F: Fn(&T) -> bool>(&mut self, done: F) {
        while !done(&self.progress_exclusive) {
            let events = self.txns_subscribe.next(None).await;
            for event in events {
                match event {
                    ListenEvent::Progress(frontier) => {
                        let progress = frontier
                            .into_option()
                            .expect("nothing should close the txns shard");
                        self.state
                            .push_entries(std::mem::take(&mut self.buf), progress);
                    }
                    ListenEvent::Updates(parts) => {
                        Self::fetch_parts(
                            self.only_data_id.clone(),
                            &mut self.txns_subscribe,
                            parts,
                            &mut self.buf,
                        )
                        .await;
                    }
                };
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

    pub(crate) async fn fetch_parts(
        only_data_id: Option<ShardId>,
        txns_subscribe: &mut Subscribe<C::Key, C::Val, T, i64>,
        parts: Vec<LeasedBatchPart<T>>,
        updates: &mut Vec<(TxnsEntry, T, i64)>,
    ) {
        // We filter out unrelated data in two passes. The first is
        // `should_fetch_part`, which allows us to skip entire fetches
        // from s3/Blob. Then, if a part does need to be fetched, it
        // still might contain info about unrelated data shards, and we
        // filter those out before buffering in `updates`.
        for part in parts {
            let should_fetch_part = Self::should_fetch_part(only_data_id.as_ref(), &part);
            debug!(
                "should_fetch_part={} for {:?} {:?}",
                should_fetch_part,
                only_data_id,
                part.stats()
            );
            if !should_fetch_part {
                txns_subscribe.return_leased_part(part);
                continue;
            }
            let part_updates = txns_subscribe.fetch_batch_part(part).await;
            let part_updates = part_updates.map(|((k, v), t, d)| {
                let (k, v) = (k.expect("valid key"), v.expect("valid val"));
                (C::decode(k, v), t, d)
            });
            if let Some(only_data_id) = only_data_id.as_ref() {
                updates.extend(part_updates.filter(|(x, _, _)| x.data_id() == only_data_id));
            } else {
                updates.extend(part_updates);
            }
        }
    }

    fn should_fetch_part(only_data_id: Option<&ShardId>, part: &LeasedBatchPart<T>) -> bool {
        let Some(only_data_id) = only_data_id else {
            return true;
        };
        // This `part.stats()` call involves decoding and the only_data_id=None
        // case is common-ish, so make sure to keep it after that early return.
        let Some(stats) = part.stats() else {
            return true;
        };
        C::should_fetch_part(only_data_id, &stats).unwrap_or(true)
    }
}

impl<T: Timestamp + Lattice + Codec64, C: TxnsCodec> Deref for TxnsCache<T, C> {
    type Target = TxnsCacheState<T>;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<T: Timestamp + Lattice + Codec64, C: TxnsCodec> DerefMut for TxnsCache<T, C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

#[derive(Debug)]
pub(crate) struct DataTimes<T> {
    /// The times at which the data shard was in the txns set.
    ///
    /// Invariants:
    ///
    /// - At least one registration (otherwise we filter this out of the cache).
    /// - These are in increasing order.
    /// - These are non-overlapping intervals.
    /// - Everything in writes is in one of these intervals.
    pub(crate) registered: VecDeque<DataRegistered<T>>,
    /// Invariant: These are in increasing order.
    ///
    /// Invariant: Each of these is >= self.since_ts.
    pub(crate) writes: VecDeque<T>,
}

impl<T> Default for DataTimes<T> {
    fn default() -> Self {
        Self {
            registered: Default::default(),
            writes: Default::default(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct DataRegistered<T> {
    /// The inclusive time at which the data shard was added to the txns set.
    ///
    /// If this time has been advanced by compaction, writes might be at times
    /// equal to it.
    pub(crate) register_ts: T,
    /// The inclusive time at which the data shard was removed from the txns
    /// set, or None if it hasn't yet been removed.
    pub(crate) forget_ts: Option<T>,
}

impl<T: Timestamp + TotalOrder> DataRegistered<T> {
    pub(crate) fn contains(&self, ts: &T) -> bool {
        &self.register_ts <= ts && self.forget_ts.as_ref().map_or(true, |x| ts <= x)
    }
}

impl<T: Timestamp + TotalOrder> DataTimes<T> {
    pub(crate) fn last_reg(&self) -> &DataRegistered<T> {
        self.registered.back().expect("at least one registration")
    }

    pub(crate) fn validate(&self, since_ts: &T) -> Result<(), String> {
        // Writes are sorted
        let mut prev_ts = T::minimum();
        for ts in self.writes.iter() {
            if ts < &prev_ts {
                return Err(format!(
                    "write ts {:?} out of order after {:?}",
                    ts, prev_ts
                ));
            }
            if ts < since_ts {
                return Err(format!(
                    "write ts {:?} not advanced past since ts {:?}",
                    ts, since_ts
                ));
            }
            prev_ts = ts.clone();
        }

        // Registered is sorted and non-overlapping.
        let mut prev_ts = T::minimum();
        let mut writes_idx = 0;
        for x in self.registered.iter() {
            if x.register_ts < prev_ts {
                return Err(format!(
                    "register ts {:?} out of order after {:?}",
                    x.register_ts, prev_ts
                ));
            }
            if let Some(forget_ts) = x.forget_ts.as_ref() {
                if !(&x.register_ts <= forget_ts) {
                    return Err(format!(
                        "register ts {:?} not less_equal forget ts {:?}",
                        x.register_ts, forget_ts
                    ));
                }
                prev_ts.clone_from(forget_ts);
            }
            // Also peel off any writes in this interval.
            while let Some(write_ts) = self.writes.get(writes_idx) {
                if write_ts < &x.register_ts {
                    return Err(format!(
                        "write ts {:?} not in any register interval {:?}",
                        write_ts, self.registered
                    ));
                }
                if let Some(forget_ts) = x.forget_ts.as_ref() {
                    if write_ts <= forget_ts {
                        writes_idx += 1;
                        continue;
                    }
                }
                break;
            }
        }

        // Check for writes after the last interval.
        let Some(reg_back) = self.registered.back() else {
            return Err("registered was empty".into());
        };
        if writes_idx != self.writes.len() && reg_back.forget_ts.is_some() {
            return Err(format!(
                "write ts {:?} not in any register interval {:?}",
                self.writes, self.registered
            ));
        }

        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum Unapplied<'a> {
    RegisterForget,
    Batch(&'a Vec<u8>),
}

#[cfg(test)]
mod tests {
    use mz_persist_client::{PersistClient, ShardIdSchema};
    use mz_persist_types::codec_impls::VecU8Schema;
    use DataListenNext::*;

    use crate::operator::DataSubscribe;
    use crate::tests::reader;
    use crate::txns::TxnsHandle;

    use super::*;

    impl TxnsCache<u64, TxnsCodecDefault> {
        pub(crate) async fn expect_open(
            init_ts: u64,
            txns: &TxnsHandle<String, (), u64, i64>,
        ) -> Self {
            let mut ret = TxnsCache::open(&txns.datas.client, txns.txns_id(), None).await;
            ret.update_gt(&init_ts).await;
            ret.compact_to(&init_ts);
            ret
        }

        pub(crate) async fn expect_snapshot(
            &mut self,
            client: &PersistClient,
            data_id: ShardId,
            as_of: u64,
        ) -> Vec<String> {
            let mut data_read = reader(client, data_id).await;
            self.update_gt(&as_of).await;
            let mut snapshot = self
                .data_snapshot(data_read.shard_id(), as_of)
                .snapshot_and_fetch(&mut data_read)
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
            DataSubscribe::new(
                "test",
                client.clone(),
                self.txns_id,
                data_id,
                as_of,
                Antichain::new(),
            )
        }
    }

    // TODO(txn): Rewrite this test to exercise more edge cases, something like:
    // registrations at `[2,4], [8,9]` and writes at 1, 3, 6, 10, 12.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn data_snapshot() {
        fn ds(
            data_id: ShardId,
            latest_write: Option<u64>,
            as_of: u64,
            empty_to: u64,
        ) -> DataSnapshot<u64> {
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
        // registered at this timestamp, so it's a normal shard and we can read
        // it normally.
        assert_eq!(cache.data_snapshot(d0, 2), ds(d0, None, 2, 4));

        // Register a data shard. We're able to read before and at the register
        // ts.
        cache.push_register(d0, 4, 1, 4);
        cache.progress_exclusive = 5;
        assert_eq!(cache.data_snapshot(d0, 3), ds(d0, None, 3, 5));
        assert_eq!(cache.data_snapshot(d0, 4), ds(d0, None, 4, 5));

        // Advance time. Nothing got written at 5, so we still don't have a
        // previous write.
        cache.progress_exclusive = 6;
        assert_eq!(cache.data_snapshot(d0, 5), ds(d0, None, 5, 6));

        // Write a batch. Apply will advance the physical upper, so we read
        // as_of the commit ts.
        cache.push_append(d0, vec![0xA0], 6, 1);
        cache.progress_exclusive = 7;
        assert_eq!(cache.data_snapshot(d0, 6), ds(d0, Some(6), 6, 7));

        // An unrelated batch doesn't change the answer. Neither does retracting
        // the batch.
        let other = ShardId::new();
        cache.push_register(other, 7, 1, 7);
        cache.push_append(other, vec![0xB0], 7, 1);
        cache.push_append(d0, vec![0xA0], 7, -1);
        cache.progress_exclusive = 8;
        assert_eq!(cache.data_snapshot(d0, 7), ds(d0, Some(6), 7, 8));

        // All of the previous answers are still the same (mod empty_to).
        assert_eq!(cache.data_snapshot(d0, 2), ds(d0, None, 2, 6));
        assert_eq!(cache.data_snapshot(d0, 3), ds(d0, None, 3, 6));
        assert_eq!(cache.data_snapshot(d0, 4), ds(d0, None, 4, 6));
        assert_eq!(cache.data_snapshot(d0, 5), ds(d0, None, 5, 6));
        assert_eq!(cache.data_snapshot(d0, 6), ds(d0, Some(6), 6, 8));
    }

    // TODO(txn): Rewrite this test to exercise more edge cases, something like:
    // registrations at `[2,4], [8,9]` and writes at 1, 3, 6, 10, 12.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn txns_cache_data_listen_next() {
        let mut txns = TxnsHandle::expect_open(PersistClient::new_for_tests().await).await;
        let log = txns.new_log();
        let d0 = ShardId::new();
        txns.expect_commit_at(3, d0, &[], &log).await;

        let mut cache = TxnsCache::expect_open(3, &txns).await;
        assert_eq!(cache.progress_exclusive, 4);

        // We can answer exclusive queries at <= progress. The data shard is not
        // registered yet, so we read it as a normal shard.
        assert_eq!(cache.data_listen_next(&d0, 3), ReadDataTo(4));
        assert_eq!(cache.data_listen_next(&d0, 4), WaitForTxnsProgress);

        // Register a data shard. The paired snapshot would advance the physical
        // upper, so we're able to read at the register ts.
        cache.push_register(d0, 4, 1, 4);
        cache.progress_exclusive = 5;
        // assert_eq!(cache.data_listen_next(&d0, 4), ReadDataTo(5));
        // assert_eq!(cache.data_listen_next(&d0, 5), WaitForTxnsProgress);

        // Advance time. Now we can resolve queries at higher times. Nothing got
        // written at 5, so the physical upper hasn't moved, but we're now
        // guaranteed that `[5, 6)` is empty.
        cache.progress_exclusive = 6;
        assert_eq!(cache.data_listen_next(&d0, 5), EmitLogicalProgress(6));

        // Write a batch. Apply will advance the physical upper, so now we need
        // to wait for the data upper to advance past the commit ts.
        cache.push_append(d0, vec![0xA0], 6, 1);
        cache.progress_exclusive = 7;
        assert_eq!(cache.data_listen_next(&d0, 6), ReadDataTo(7));

        // An unrelated batch is another empty space guarantee. Ditto retracting
        // the batch.
        let other = ShardId::new();
        cache.push_register(other, 7, 1, 7);
        cache.push_append(other, vec![0xB0], 7, 1);
        cache.push_append(d0, vec![0xA0], 7, -1);
        cache.progress_exclusive = 8;
        assert_eq!(cache.data_listen_next(&d0, 7), EmitLogicalProgress(8));

        // Unlike a snapshot, the previous answers have changed! This is because
        // it's silly (inefficient) to walk through the state machine for a big
        // range of time once you know of a later write that advances the
        // physical upper.
        // assert_eq!(cache.data_listen_next(&d0, 3), ReadDataTo(7));
        // assert_eq!(cache.data_listen_next(&d0, 4), ReadDataTo(7));
        assert_eq!(cache.data_listen_next(&d0, 5), ReadDataTo(7));
        assert_eq!(cache.data_listen_next(&d0, 6), ReadDataTo(7));
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn empty_to() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let d0 = txns.expect_register(1).await;

        // During code review, we discussed an alternate implementation of
        // empty_to that was an Option: None when we new about a write > the
        // as_of, and Some when we didn't. The None case would mean that we
        // don't need to CaA empty updates in. This is quite appealing, but
        // would cause an issue with the guarantee that `apply_le(as_of)` is
        // sufficient to unblock a read. Specifically:
        //
        // - Write at 3, but don't apply.
        // - Write at 5, but don't apply.
        // - Catch the cache up past the write at 5.
        // - Run apply_le(4) to unblock a read a 4.
        // - Run a snapshot at 4.
        // - If nothing else applies the write at 5, the snapshot would
        //   deadlock.
        for ts in [3, 5] {
            let mut txn = txns.begin();
            txn.write(&d0, "3".into(), (), 1).await;
            let _apply = txn.commit_at(&mut txns, ts).await.unwrap();
        }
        txns.txns_cache.update_gt(&5).await;
        txns.apply_le(&4).await;
        let snap = txns.txns_cache.data_snapshot(d0, 4);
        let mut data_read = reader(&client, d0).await;
        // This shouldn't deadlock.
        let contents = snap.snapshot_and_fetch(&mut data_read).await.unwrap();
        assert_eq!(contents.len(), 1);

        // Sanity check that the scenario played out like we said above.
        assert_eq!(snap.empty_to, 5);
    }

    #[mz_ore::test]
    fn data_times_validate() {
        fn dt(register_forget_ts: &[u64], write_ts: &[u64]) -> Result<(), ()> {
            let mut dt = DataTimes::default();
            for x in register_forget_ts {
                if let Some(back) = dt.registered.back_mut() {
                    if back.forget_ts == None {
                        back.forget_ts = Some(*x);
                        continue;
                    }
                }
                dt.registered.push_back(DataRegistered {
                    register_ts: *x,
                    forget_ts: None,
                })
            }
            dt.writes = write_ts.into_iter().cloned().collect();
            let since_ts = u64::minimum();
            dt.validate(&since_ts).map_err(|_| ())
        }

        // Valid
        assert_eq!(dt(&[1], &[2, 3]), Ok(()));
        assert_eq!(dt(&[1, 3], &[2]), Ok(()));
        assert_eq!(dt(&[1, 3, 5], &[2, 6, 7]), Ok(()));
        assert_eq!(dt(&[1, 3, 5], &[2, 6, 7]), Ok(()));
        assert_eq!(dt(&[1, 1], &[1]), Ok(()));

        // Invalid
        assert_eq!(dt(&[], &[]), Err(()));
        assert_eq!(dt(&[1], &[0]), Err(()));
        assert_eq!(dt(&[1, 3], &[4]), Err(()));
        assert_eq!(dt(&[1, 3, 5], &[4]), Err(()));
        assert_eq!(dt(&[1, 4], &[3, 2]), Err(()));
    }

    /// Regression test for a bug caught by higher level tests in CI:
    /// - Commit a write at 5
    /// - Apply it and commit the tidy retraction at 20.
    /// - Catch up to both of these in the TxnsHandle and call compact_to(10).
    ///   The TxnsHandle knows the write has been applied and lets it CaDS the
    ///   txns shard since to 10.
    /// - Open a TxnsCache starting at the txns shard since (10) to serve a
    ///   snapshot at 12. Catch it up through 12, but _not_ the tidy at 20.
    /// - This TxnsCache gets the write with a ts compacted forward to 10, but
    ///   no retraction. The snapshot resolves with an incorrect latest_write of
    ///   `Some(10)`.
    /// - The unblock read waits for this write to be applied before doing the
    ///   empty CaA, but this write never existed so it hangs forever.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn regression_compact_latest_write() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let log = txns.new_log();
        let d0 = txns.expect_register(1).await;

        let tidy_5 = txns.expect_commit_at(5, d0, &["5"], &log).await;
        let _ = txns.expect_commit_at(15, d0, &["15"], &log).await;
        txns.tidy_at(20, tidy_5).await.unwrap();
        txns.txns_cache.update_gt(&20).await;
        assert_eq!(txns.txns_cache.min_unapplied_ts(), &15);
        txns.compact_to(10).await;

        let txns_read = client
            .open_leased_reader(
                txns.txns_id(),
                Arc::new(ShardIdSchema),
                Arc::new(VecU8Schema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("txns schema shouldn't change");
        let mut cache =
            TxnsCache::<_, TxnsCodecDefault>::from_read(txns_read, Antichain::from_elem(10), None)
                .await;
        cache.update_gt(&15).await;
        let snap = cache.data_snapshot(d0, 12);
        assert_eq!(snap.latest_write, None);
    }

    // Regression test for a bug where we were sorting TxnEvents by the
    // compacted timestamp instead of the original one when applying them to a
    // cache. This caused them to be applied in a surprising order (e.g. forget
    // before register).
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn regression_ts_sort() {
        let client = PersistClient::new_for_tests().await;
        let txns = TxnsHandle::expect_open(client.clone()).await;
        let mut cache = TxnsCache::expect_open(0, &txns).await;
        let d0 = ShardId::new();

        // With the bug, this panics via an internal sanity assertion.
        cache.push_entries(
            vec![
                (TxnsEntry::Register(d0, u64::encode(&2)), 2, -1),
                (TxnsEntry::Register(d0, u64::encode(&1)), 2, 1),
            ],
            3,
        );
    }
}
