// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A cache of the txn shard contents.

use std::cmp::{max, min};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::collections::HashMap;
use mz_ore::instrument;
use mz_persist_client::cfg::USE_CRITICAL_SINCE_TXN;
use mz_persist_client::fetch::LeasedBatchPart;
use mz_persist_client::metrics::encode_ts_metric;
use mz_persist_client::read::{ListenEvent, ReadHandle, Subscribe};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::txn::{TxnsCodec, TxnsEntry};
use mz_persist_types::{Codec64, StepForward};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tracing::debug;

use crate::metrics::Metrics;
use crate::txn_read::{DataListenNext, DataSnapshot, DataSubscribeBlocked};
use crate::TxnsCodecDefault;

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
    /// The since of the txn_shard when this cache was initialized.
    /// Some writes with a timestamp < than this may have been applied and
    /// tidied, so this cache has no way of learning about them.
    ///
    /// Invariant: never changes.
    pub(crate) init_ts: T,
    /// The contents of this cache are updated up to, but not including, this time.
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
    ///
    /// Invariant: Contains all unapplied writes and registers.
    /// Invariant: Contains the latest write and registertaion >= init_ts for all shards.
    pub(crate) datas: BTreeMap<ShardId, DataTimes<T>>,
    /// The registers and forgets needing application as of the current progress.
    ///
    /// Invariant: Values are sorted by timestamp.
    pub(crate) unapplied_registers: VecDeque<(ShardId, T)>,

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
    /// A subscribe over the txn shard.
    pub(crate) txns_subscribe: Subscribe<C::Key, C::Val, T, i64>,
    /// Pending updates for timestamps that haven't closed.
    pub(crate) buf: Vec<(TxnsEntry, T, i64)>,
    state: TxnsCacheState<T>,
}

impl<T: Timestamp + Lattice + TotalOrder + StepForward + Codec64> TxnsCacheState<T> {
    /// Creates a new empty [`TxnsCacheState`].
    ///
    /// `init_ts` must be == the critical handle's since of the txn shard.
    fn new(txns_id: ShardId, init_ts: T, only_data_id: Option<ShardId>) -> Self {
        TxnsCacheState {
            txns_id,
            init_ts,
            progress_exclusive: T::minimum(),
            next_batch_id: 0,
            unapplied_batches: BTreeMap::new(),
            batch_idx: HashMap::new(),
            datas: BTreeMap::new(),
            unapplied_registers: VecDeque::new(),
            only_data_id,
        }
    }

    /// Creates and initializes a new [`TxnsCacheState`].
    ///
    /// `txns_read` is a [`ReadHandle`] on the txn shard.
    pub(crate) async fn init<C: TxnsCodec>(
        only_data_id: Option<ShardId>,
        txns_read: ReadHandle<C::Key, C::Val, T, i64>,
    ) -> (Self, Subscribe<C::Key, C::Val, T, i64>) {
        let txns_id = txns_read.shard_id();
        let as_of = txns_read.since().clone();
        let since_ts = as_of.as_option().expect("txns shard is not closed").clone();
        let mut txns_subscribe = txns_read
            .subscribe(as_of)
            .await
            .expect("handle holds a capability");
        let mut state = Self::new(txns_id, since_ts.clone(), only_data_id.clone());
        let mut buf = Vec::new();
        // The cache must be updated to `since_ts` to maintain the invariant
        // that `state.since_ts <= state.progress_exclusive`.
        TxnsCache::<T, C>::update(
            &mut state,
            &mut txns_subscribe,
            &mut buf,
            only_data_id,
            |progress_exclusive| progress_exclusive >= &since_ts,
        )
        .await;
        debug_assert_eq!(state.validate(), Ok(()));
        (state, txns_subscribe)
    }

    /// Returns the [ShardId] of the txns shard.
    pub fn txns_id(&self) -> ShardId {
        self.txns_id
    }

    /// Returns whether the data shard was registered to the txns set as of the
    /// current progress.
    ///
    /// Specifically, a data shard is registered if the most recent register
    /// timestamp is set but the most recent forget timestamp is not set.
    ///
    /// This function accepts a timestamp as input, but that timestamp must be
    /// equal to the progress exclusive, or else the function panics. It mainly
    /// acts as a way for the caller to think about the logical time at which
    /// this function executes. Times in the past may have been compacted away,
    /// and we can't always return an accurate answer. If this function isn't
    /// sufficient, you can usually find what you're looking for by inspecting
    /// the times in the most recent registration.
    pub fn registered_at_progress(&self, data_id: &ShardId, ts: &T) -> bool {
        self.assert_only_data_id(data_id);
        assert_eq!(self.progress_exclusive, *ts);
        let Some(data_times) = self.datas.get(data_id) else {
            return false;
        };
        data_times.last_reg().forget_ts.is_none()
    }

    /// Returns the set of all data shards registered to the txns set as of the
    /// current progress. See [Self::registered_at_progress].
    pub(crate) fn all_registered_at_progress(&self, ts: &T) -> Vec<ShardId> {
        assert_eq!(self.only_data_id, None);
        assert_eq!(self.progress_exclusive, *ts);
        self.datas
            .iter()
            .filter(|(_, data_times)| data_times.last_reg().forget_ts.is_none())
            .map(|(data_id, _)| *data_id)
            .collect()
    }

    /// Returns a token exchangeable for a snapshot of a data shard.
    ///
    /// A data shard might be definite at times past the physical upper because
    /// of invariants maintained by this txn system. As a result, this method
    /// discovers the latest potentially unapplied write before the `as_of`.
    ///
    /// Callers must first wait for [`TxnsCache::update_gt`] with the same or
    /// later timestamp to return. Panics otherwise.
    pub fn data_snapshot(&self, data_id: ShardId, as_of: T) -> DataSnapshot<T> {
        self.assert_only_data_id(&data_id);
        assert!(self.progress_exclusive > as_of);
        // `empty_to` will often be used as the input to `data_listen_next`.
        // `data_listen_next` needs a timestamp that is greater than or equal
        // to the init_ts. See the comment above the assert in
        // `data_listen_next` for more details.
        //
        // TODO: Once the txn shard itself always tracks the most recent write
        // for every shard, we can remove this and always use
        // `as_of.step_forward()`.
        let empty_to = max(as_of.step_forward(), self.init_ts.clone());
        let Some(all) = self.datas.get(&data_id) else {
            // Not registered currently, so we know there are no unapplied
            // writes.
            return DataSnapshot {
                data_id,
                latest_write: None,
                as_of,
                empty_to,
            };
        };

        let min_unapplied_ts = self
            .unapplied_batches
            .first_key_value()
            .map(|(_, (_, _, ts))| ts)
            .unwrap_or(&self.progress_exclusive);
        let latest_write = all
            .writes
            .iter()
            .rev()
            .find(|x| **x <= as_of && *x >= min_unapplied_ts)
            .cloned();

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

    // TODO(jkosh44) This method can likely be simplified to return
    // DataRemapEntry directly.
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
    pub fn data_listen_next(&self, data_id: &ShardId, ts: &T) -> DataListenNext<T> {
        self.assert_only_data_id(data_id);
        assert!(
            &self.progress_exclusive >= ts,
            "ts {:?} is past progress_exclusive {:?}",
            ts,
            self.progress_exclusive
        );
        // There may be applied and tidied writes before the init_ts that the
        // cache is unaware of. So if this method is called with a timestamp
        // less than the initial since, it may mistakenly tell the caller to
        // `EmitLogicalProgress(self.progress_exclusive)` instead of the
        // correct answer of `ReadTo(tidied_write_ts)`.
        //
        // We know for a fact that there are no unapplied writes, registers, or
        // forgets before the init_ts because the since of the txn shard is
        // always held back to the earliest unapplied event. There may be some
        // untidied events with a lower timestamp than the init_ts, but they
        // are guaranteed to be applied.
        //
        // TODO: Once the txn shard itself always tracks the most recent write
        // for every shard, we can remove this assert. It will always be
        // correct to return ReadTo(latest_write_ts) if there are any writes,
        // and then `EmitLogicalProgress(self.progress_exclusive)`.
        assert!(
            ts >= &self.init_ts,
            "ts {:?} is not past initial since {:?}",
            ts,
            self.init_ts
        );
        use DataListenNext::*;
        let data_times = self.datas.get(data_id);
        debug!(
            "data_listen_next {:.9} {:?}: progress={:?} times={:?}",
            data_id.to_string(),
            ts,
            self.progress_exclusive,
            data_times,
        );
        let Some(data_times) = data_times else {
            // Not registered, maybe it will be in the future? In the meantime,
            // treat it like a normal shard (i.e. pass through reads) and check
            // again later.
            if ts < &self.progress_exclusive {
                return ReadDataTo(self.progress_exclusive.clone());
            } else {
                return WaitForTxnsProgress;
            }
        };
        let physical_ts = data_times.latest_physical_ts();
        let last_reg = data_times.last_reg();
        if ts >= &self.progress_exclusive {
            // All caught up, we have to wait.
            WaitForTxnsProgress
        } else if ts <= physical_ts {
            // There was some physical write, so read up to that time.
            ReadDataTo(physical_ts.step_forward())
        } else if last_reg.forget_ts.is_none() {
            // Emitting logical progress at the wrong time is a correctness bug,
            // so be extra defensive about the necessary conditions: the most
            // recent registration is still active, and we're in it.
            assert!(last_reg.contains(ts));
            EmitLogicalProgress(self.progress_exclusive.clone())
        } else {
            // The most recent forget is set, which means it's not registered as of
            // the latest information we have. Read to the current progress point
            // normally.

            assert!(ts > &last_reg.register_ts && last_reg.forget_ts.is_some());
            ReadDataTo(self.progress_exclusive.clone())
        }
    }

    /// Returns a token exchangeable for a subscribe of a data shard.
    ///
    /// Callers must first wait for [`TxnsCache::update_gt`] with the same or
    /// later timestamp to return. Panics otherwise.
    pub(crate) fn data_subscribe(&self, data_id: ShardId, as_of: T) -> DataSubscribeBlocked<T> {
        self.assert_only_data_id(&data_id);
        assert!(self.progress_exclusive > as_of);
        let snapshot = self.data_snapshot(data_id, as_of);
        DataSubscribeBlocked(snapshot)
    }

    /// Returns the minimum timestamp not known to be applied by this cache.
    pub fn min_unapplied_ts(&self) -> &T {
        assert_eq!(self.only_data_id, None);
        self.min_unapplied_ts_inner()
    }

    fn min_unapplied_ts_inner(&self) -> &T {
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
            .fold(
                BTreeMap::new(),
                |mut accum: BTreeMap<_, Vec<_>>, (data_id, batch, ts)| {
                    accum.entry((ts, data_id)).or_default().push(batch);
                    accum
                },
            )
            .into_iter()
            .map(|((ts, data_id), batches)| (data_id, Unapplied::Batch(batches), ts));
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
    /// Callers must first wait for [`TxnsCache::update_ge`] with the same or
    /// later timestamp to return. Panics otherwise.
    pub(crate) fn filter_retractions<'a>(
        &'a self,
        expected_txns_upper: &T,
        retractions: impl Iterator<Item = (&'a Vec<u8>, &'a ([u8; 8], ShardId))>,
    ) -> impl Iterator<Item = (&'a Vec<u8>, &'a ([u8; 8], ShardId))> {
        assert_eq!(self.only_data_id, None);
        assert!(&self.progress_exclusive >= expected_txns_upper);
        retractions.filter(|(batch_raw, _)| self.batch_idx.contains_key(*batch_raw))
    }

    /// Update contents with `entries` and mark this cache as progressed up to `progress`.
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
        debug_assert_eq!(self.validate(), Ok(()));
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
        debug_assert_eq!(self.validate(), Ok(()));
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
            // Sanity check that shard is registered.
            assert_eq!(times.last_reg().forget_ts, None);
            times.writes.push_back(ts);
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
        self.compact_data_times(&data_id);
        debug_assert_eq!(self.validate(), Ok(()));
    }

    /// Informs the cache that all registers and forgets less than ts have been
    /// applied.
    pub(crate) fn mark_register_applied(&mut self, ts: &T) {
        self.unapplied_registers
            .retain(|(_, register_ts)| ts < register_ts);
        debug_assert_eq!(self.validate(), Ok(()));
    }

    /// Compact the internal representation for `data_id` by removing all data
    /// that is not needed to maintain the following invariants:
    ///
    ///   - The latest write and registration for each shard are kept in
    ///     `self.datas`.
    ///   - All unapplied writes and registrations are kept in `self.datas`.
    ///   - All writes in `self.datas` are contained by some registration in
    ///     `self.datas`.
    fn compact_data_times(&mut self, data_id: &ShardId) {
        let Some(times) = self.datas.get_mut(data_id) else {
            return;
        };

        debug!("cache compact {:.9} times={:?}", data_id.to_string(), times);

        if let Some(unapplied_write_ts) = self
            .unapplied_batches
            .first_key_value()
            .map(|(_, (_, _, ts))| ts)
        {
            debug!(
                "cache compact {:.9} unapplied_write_ts={:?}",
                data_id.to_string(),
                unapplied_write_ts,
            );
            while let Some(write_ts) = times.writes.front() {
                if times.writes.len() == 1 || write_ts >= unapplied_write_ts {
                    break;
                }
                times.writes.pop_front();
            }
        } else {
            times.writes.drain(..times.writes.len() - 1);
        }
        let unapplied_reg_ts = self.unapplied_registers.front().map(|(_, ts)| ts);
        let min_write_ts = times.writes.front();
        let min_reg_ts = [unapplied_reg_ts, min_write_ts].into_iter().flatten().min();
        if let Some(min_reg_ts) = min_reg_ts {
            debug!(
                "cache compact {:.9} unapplied_reg_ts={:?} min_write_ts={:?} min_reg_ts={:?}",
                data_id.to_string(),
                unapplied_reg_ts,
                min_write_ts,
                min_reg_ts,
            );
            while let Some(reg) = times.registered.front() {
                match &reg.forget_ts {
                    Some(forget_ts) if forget_ts >= min_reg_ts => break,
                    _ if times.registered.len() == 1 => break,
                    _ => {
                        assert!(
                            reg.forget_ts.is_some(),
                            "only the latest reg can have no forget ts"
                        );
                        times.registered.pop_front();
                    }
                }
            }
        } else {
            times.registered.drain(..times.registered.len() - 1);
        }

        debug!(
            "cache compact DONE {:.9} times={:?}",
            data_id.to_string(),
            times
        );
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
        // Unapplied batches are all indexed and sorted.
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

        // Unapplied registers are sorted.
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

        let min_unapplied_ts = self.min_unapplied_ts_inner();

        for (data_id, data_times) in self.datas.iter() {
            let () = data_times.validate()?;

            if let Some(ts) = data_times.writes.front() {
                // Writes are compacted.
                if min_unapplied_ts > ts && data_times.writes.len() > 1 {
                    return Err(format!(
                        "{:?} write ts {:?} not past min unapplied ts {:?}",
                        data_id, ts, min_unapplied_ts
                    ));
                }
            }

            // datas contains all unapplied writes.
            if let Some((_, (_, _, unapplied_ts))) = self
                .unapplied_batches
                .iter()
                .find(|(_, (shard_id, _, _))| shard_id == data_id)
            {
                if let Some(write_ts) = data_times.writes.front() {
                    if write_ts > unapplied_ts {
                        return Err(format!(
                            "{:?} min write ts {:?} past min unapplied batch ts {:?}",
                            data_id, write_ts, unapplied_ts
                        ));
                    }
                }
            }

            // datas contains all unapplied register/forgets.
            if let Some((_, unapplied_ts)) = self
                .unapplied_registers
                .iter()
                .find(|(shard_id, _)| shard_id == data_id)
            {
                let register_ts = &data_times.first_reg().register_ts;
                if register_ts > unapplied_ts {
                    return Err(format!(
                        "{:?} min register ts {:?} past min unapplied register ts {:?}",
                        data_id, register_ts, unapplied_ts
                    ));
                }
            }
        }

        Ok(())
    }
}

impl<T: Timestamp + Lattice + TotalOrder + StepForward + Codec64, C: TxnsCodec> TxnsCache<T, C> {
    /// Initialize the txn shard at `init_ts` and returns a [TxnsCache] reading
    /// from that shard.
    pub(crate) async fn init(
        init_ts: T,
        txns_read: ReadHandle<C::Key, C::Val, T, i64>,
        txns_write: &mut WriteHandle<C::Key, C::Val, T, i64>,
    ) -> Self {
        let () = crate::empty_caa(|| "txns init", txns_write, init_ts.clone()).await;
        let mut ret = Self::from_read(txns_read, None).await;
        let _ = ret.update_gt(&init_ts).await;
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
                USE_CRITICAL_SINCE_TXN.get(client.dyncfgs()),
            )
            .await
            .expect("txns schema shouldn't change");
        Self::from_read(txns_read, only_data_id).await
    }

    async fn from_read(
        txns_read: ReadHandle<C::Key, C::Val, T, i64>,
        only_data_id: Option<ShardId>,
    ) -> Self {
        let (state, txns_subscribe) = TxnsCacheState::init::<C>(only_data_id, txns_read).await;
        TxnsCache {
            txns_subscribe,
            buf: Vec::new(),
            state,
        }
    }

    /// Invariant: afterward, self.progress_exclusive will be > ts
    ///
    /// Returns the `progress_exclusive` of the cache after updating.
    #[must_use]
    #[instrument(level = "debug", fields(ts = ?ts))]
    pub async fn update_gt(&mut self, ts: &T) -> &T {
        let only_data_id = self.only_data_id.clone();
        Self::update(
            &mut self.state,
            &mut self.txns_subscribe,
            &mut self.buf,
            only_data_id,
            |progress_exclusive| progress_exclusive > ts,
        )
        .await;
        debug_assert!(&self.progress_exclusive > ts);
        debug_assert_eq!(self.validate(), Ok(()));
        &self.progress_exclusive
    }

    /// Invariant: afterward, self.progress_exclusive will be >= ts
    ///
    /// Returns the `progress_exclusive` of the cache after updating.
    #[must_use]
    #[instrument(level = "debug", fields(ts = ?ts))]
    pub async fn update_ge(&mut self, ts: &T) -> &T {
        let only_data_id = self.only_data_id.clone();
        Self::update(
            &mut self.state,
            &mut self.txns_subscribe,
            &mut self.buf,
            only_data_id,
            |progress_exclusive| progress_exclusive >= ts,
        )
        .await;
        debug_assert!(&self.progress_exclusive >= ts);
        debug_assert_eq!(self.validate(), Ok(()));
        &self.progress_exclusive
    }

    /// Listen to the txns shard for events until `done` returns true.
    async fn update<F: Fn(&T) -> bool>(
        state: &mut TxnsCacheState<T>,
        txns_subscribe: &mut Subscribe<C::Key, C::Val, T, i64>,
        buf: &mut Vec<(TxnsEntry, T, i64)>,
        only_data_id: Option<ShardId>,
        done: F,
    ) {
        while !done(&state.progress_exclusive) {
            let events = txns_subscribe.next(None).await;
            for event in events {
                match event {
                    ListenEvent::Progress(frontier) => {
                        let progress = frontier
                            .into_option()
                            .expect("nothing should close the txns shard");
                        state.push_entries(std::mem::take(buf), progress);
                    }
                    ListenEvent::Updates(parts) => {
                        Self::fetch_parts(only_data_id, txns_subscribe, parts, buf).await;
                    }
                };
            }
        }
        debug_assert_eq!(state.validate(), Ok(()));
        debug!(
            "cache correct before {:?} len={} least_ts={:?}",
            state.progress_exclusive,
            state.unapplied_batches.len(),
            state
                .unapplied_batches
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
                drop(part);
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

    fn first_reg(&self) -> &DataRegistered<T> {
        self.registered.front().expect("at least one registration")
    }

    /// Returns the latest known physical upper of a data shard.
    fn latest_physical_ts(&self) -> &T {
        let last_reg = self.last_reg();
        let mut physical_ts = &last_reg.register_ts;
        if let Some(forget_ts) = &last_reg.forget_ts {
            physical_ts = max(physical_ts, forget_ts);
        }
        if let Some(latest_write) = self.writes.back() {
            physical_ts = max(physical_ts, latest_write);
        }
        physical_ts
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        // Writes are sorted.
        let mut prev_ts = T::minimum();
        for ts in self.writes.iter() {
            if ts < &prev_ts {
                return Err(format!(
                    "write ts {:?} out of order after {:?}",
                    ts, prev_ts
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
    Batch(Vec<&'a Vec<u8>>),
}

#[cfg(test)]
mod tests {
    use mz_persist_client::PersistClient;
    use mz_persist_types::codec_impls::{ShardIdSchema, VecU8Schema};
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
            let _ = ret.update_gt(&init_ts).await;
            ret
        }

        pub(crate) async fn expect_snapshot(
            &mut self,
            client: &PersistClient,
            data_id: ShardId,
            as_of: u64,
        ) -> Vec<String> {
            let mut data_read = reader(client, data_id).await;
            let _ = self.update_gt(&as_of).await;
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

    #[mz_ore::test]
    fn txns_cache_data_snapshot_and_listen_next() {
        let d0 = ShardId::new();
        let ds = |latest_write: Option<u64>, as_of: u64, empty_to: u64| -> DataSnapshot<u64> {
            DataSnapshot {
                data_id: d0,
                latest_write,
                as_of,
                empty_to,
            }
        };
        #[track_caller]
        fn testcase(
            cache: &mut TxnsCacheState<u64>,
            ts: u64,
            data_id: ShardId,
            snap_expected: DataSnapshot<u64>,
            listen_expected: DataListenNext<u64>,
        ) {
            cache.progress_exclusive = ts + 1;
            assert_eq!(cache.data_snapshot(data_id, ts), snap_expected);
            assert_eq!(cache.data_listen_next(&data_id, &ts), listen_expected);
            assert_eq!(
                cache.data_listen_next(&data_id, &(ts + 1)),
                WaitForTxnsProgress
            );
        }

        // This attempts to exercise all the various interesting edge cases of
        // data_snapshot and data_listen_subscribe using the following sequence
        // of events:
        //
        // - Registrations at: [2,8], [15,16]
        // - Direct writes at: 1, 13
        // - Writes via txns at: 4, 5, 7
        let mut c = TxnsCacheState::new(ShardId::new(), 0, None);

        // empty
        assert_eq!(c.progress_exclusive, 0);
        assert!(mz_ore::panic::catch_unwind(|| c.data_snapshot(d0, 0)).is_err());
        assert_eq!(c.data_listen_next(&d0, &0), WaitForTxnsProgress);

        // ts 0 (never registered)
        testcase(&mut c, 0, d0, ds(None, 0, 1), ReadDataTo(1));

        // ts 1 (direct write)
        // - The cache knows everything < 2.
        // - d0 is not registered in the cache.
        // - We know the shard can't be written to via txn < 2.
        // - So go read the shard normally up to 2.
        testcase(&mut c, 1, d0, ds(None, 1, 2), ReadDataTo(2));

        // ts 2 (register)
        c.push_register(d0, 2, 1, 2);
        testcase(&mut c, 2, d0, ds(None, 2, 3), ReadDataTo(3));

        // ts 3 (registered, not written)
        testcase(&mut c, 3, d0, ds(None, 3, 4), EmitLogicalProgress(4));

        // ts 4 (written via txns)
        c.push_append(d0, vec![4], 4, 1);
        testcase(&mut c, 4, d0, ds(Some(4), 4, 5), ReadDataTo(5));

        // ts 5 (written via txns, write at preceding ts)
        c.push_append(d0, vec![5], 5, 1);
        testcase(&mut c, 5, d0, ds(Some(5), 5, 6), ReadDataTo(6));

        // ts 6 (registered, not written, write at preceding ts)
        testcase(&mut c, 6, d0, ds(Some(5), 6, 7), EmitLogicalProgress(7));

        // ts 7 (written via txns, write at non-preceding ts)
        c.push_append(d0, vec![7], 7, 1);
        testcase(&mut c, 7, d0, ds(Some(7), 7, 8), ReadDataTo(8));

        // ts 8 (apply and tidy write from ts 4)
        c.push_append(d0, vec![4], 8, -1);
        testcase(&mut c, 8, d0, ds(Some(7), 8, 9), EmitLogicalProgress(9));

        // ts 9 (apply and tidy write from ts 5)
        c.push_append(d0, vec![5], 9, -1);
        testcase(&mut c, 9, d0, ds(Some(7), 9, 10), EmitLogicalProgress(10));

        // ts 10 (apply and tidy write from ts 7)
        c.push_append(d0, vec![7], 10, -1);
        testcase(&mut c, 10, d0, ds(None, 10, 11), EmitLogicalProgress(11));

        // ts 11 (forget)
        // Revisit when
        // https://github.com/MaterializeInc/materialize/issues/25992 is fixed,
        // it's unclear how to encode the register timestamp in a forget.
        c.push_register(d0, 11, -1, 11);
        testcase(&mut c, 11, d0, ds(None, 11, 12), ReadDataTo(12));

        // ts 12 (not registered, not written). This ReadDataTo would block until
        // the write happens at ts 13.
        testcase(&mut c, 12, d0, ds(None, 12, 13), ReadDataTo(13));

        // ts 13 (written directly)
        testcase(&mut c, 13, d0, ds(None, 13, 14), ReadDataTo(14));

        // ts 14 (not registered, not written) This ReadDataTo would block until
        // the register happens at 15.
        testcase(&mut c, 14, d0, ds(None, 14, 15), ReadDataTo(15));

        // ts 15 (registered, previously forgotten)
        c.push_register(d0, 15, 1, 15);
        testcase(&mut c, 15, d0, ds(None, 15, 16), ReadDataTo(16));

        // ts 16 (forgotten, registered at preceding ts)
        // Revisit when
        // https://github.com/MaterializeInc/materialize/issues/25992 is fixed,
        // it's unclear how to encode the register timestamp in a forget.
        c.push_register(d0, 16, -1, 16);
        testcase(&mut c, 16, d0, ds(None, 16, 17), ReadDataTo(17));

        // Now that we have more history, some of the old answers change! In
        // particular, we have more information on unapplied writes, empty
        // times, and can ReadDataTo much later times.

        assert_eq!(c.data_snapshot(d0, 0), ds(None, 0, 1));
        assert_eq!(c.data_snapshot(d0, 1), ds(None, 1, 2));
        assert_eq!(c.data_snapshot(d0, 2), ds(None, 2, 3));
        assert_eq!(c.data_snapshot(d0, 3), ds(None, 3, 4));
        assert_eq!(c.data_snapshot(d0, 4), ds(None, 4, 5));
        assert_eq!(c.data_snapshot(d0, 5), ds(None, 5, 6));
        assert_eq!(c.data_snapshot(d0, 6), ds(None, 6, 7));
        assert_eq!(c.data_snapshot(d0, 7), ds(None, 7, 8));
        assert_eq!(c.data_snapshot(d0, 8), ds(None, 8, 9));
        assert_eq!(c.data_snapshot(d0, 9), ds(None, 9, 10));
        assert_eq!(c.data_snapshot(d0, 10), ds(None, 10, 11));
        assert_eq!(c.data_snapshot(d0, 11), ds(None, 11, 12));
        assert_eq!(c.data_snapshot(d0, 12), ds(None, 12, 13));
        assert_eq!(c.data_snapshot(d0, 13), ds(None, 13, 14));
        assert_eq!(c.data_snapshot(d0, 14), ds(None, 14, 15));
        assert_eq!(c.data_snapshot(d0, 15), ds(None, 15, 16));
        assert_eq!(c.data_snapshot(d0, 16), ds(None, 16, 17));

        assert_eq!(c.data_listen_next(&d0, &0), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &1), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &2), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &3), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &4), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &5), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &6), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &7), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &8), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &9), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &10), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &11), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &12), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &13), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &14), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &15), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &16), ReadDataTo(17));
        assert_eq!(c.data_listen_next(&d0, &17), WaitForTxnsProgress);
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn empty_to() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let d0 = txns.expect_register(1).await;

        // During code review, we discussed an alternate implementation of
        // empty_to that was an Option: None when we knew about a write > the
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
        let _ = txns.txns_cache.update_gt(&5).await;
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
            dt.validate().map_err(|_| ())
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
        let _ = txns.txns_cache.update_gt(&20).await;
        assert_eq!(txns.txns_cache.min_unapplied_ts(), &15);
        txns.compact_to(10).await;

        let mut txns_read = client
            .open_leased_reader(
                txns.txns_id(),
                Arc::new(ShardIdSchema),
                Arc::new(VecU8Schema),
                Diagnostics::for_tests(),
                true,
            )
            .await
            .expect("txns schema shouldn't change");
        txns_read.downgrade_since(&Antichain::from_elem(10)).await;
        let mut cache = TxnsCache::<_, TxnsCodecDefault>::from_read(txns_read, None).await;
        let _ = cache.update_gt(&15).await;
        let snap = cache.data_snapshot(d0, 12);
        assert_eq!(snap.latest_write, Some(5));
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

    /// Tests that `data_snapshot` and `data_listen_next` properly handle an
    /// `init_ts` > 0.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn data_compacted() {
        let d0 = ShardId::new();
        let mut c = TxnsCacheState::new(ShardId::new(), 10, None);
        c.progress_exclusive = 20;

        assert!(mz_ore::panic::catch_unwind(|| c.data_listen_next(&d0, &0)).is_err());

        let ds = c.data_snapshot(d0, 0);
        assert_eq!(
            ds,
            DataSnapshot {
                data_id: d0,
                latest_write: None,
                as_of: 0,
                empty_to: 10,
            }
        );
    }
}
