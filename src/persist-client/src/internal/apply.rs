// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of persist command application.

use std::fmt::Debug;
use std::ops::{ControlFlow, ControlFlow::Break, ControlFlow::Continue};
use std::sync::{Arc, Mutex};

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_ore::cast::CastFrom;
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::debug;

use mz_persist::location::{CaSResult, Indeterminate, SeqNo};

use crate::cache::StateCache;
use crate::error::CodecMismatch;
use crate::internal::maintenance::RoutineMaintenance;
use crate::internal::metrics::{CmdMetrics, Metrics, ShardMetrics};
use crate::internal::paths::{PartialRollupKey, RollupId};
use crate::internal::state::{HollowBatch, Since, StateCollections, TypedState, Upper};
use crate::internal::state_diff::StateDiff;
use crate::internal::state_versions::{EncodedRollup, StateVersions};
use crate::internal::trace::FueledMergeReq;
use crate::{PersistConfig, ShardId};

#[derive(Debug, Clone)]
pub struct CachedState<T> {
    pub shard_id: ShardId,
    pub seqno: SeqNo,
    pub upper: Antichain<T>,
    pub since: Antichain<T>,
    pub seqno_since: SeqNo,
    pub is_tombstone: bool,
}

impl<K, V, T: Timestamp + Lattice + Codec64, D> From<&TypedState<K, V, T, D>> for CachedState<T> {
    fn from(value: &TypedState<K, V, T, D>) -> Self {
        CachedState {
            shard_id: value.shard_id,
            seqno: value.seqno,
            upper: value.upper().clone(),
            since: value.since().clone(),
            seqno_since: value.seqno_since(),
            is_tombstone: value.collections.is_tombstone(),
        }
    }
}

impl<T: Timestamp + Lattice + Codec64> CachedState<T> {
    fn validate_cached_version_of<K, V, D>(
        &self,
        state: &TypedState<K, V, T, D>,
    ) -> Result<(), String> {
        // destructure to ensure we validate each field
        let Self {
            shard_id,
            seqno,
            upper,
            since,
            seqno_since,
            is_tombstone,
        } = self;
        if *shard_id != state.shard_id {
            return Err(format!(
                "shard_id didn't match {} vs {}",
                shard_id, state.shard_id
            ));
        }
        if !(*seqno <= state.seqno) {
            return Err(format!(
                "seqno unexpectedly not leq {} vs {}",
                seqno, state.seqno
            ));
        }
        if !(*seqno_since <= state.seqno_since()) {
            return Err(format!(
                "seqno since unexpectedly not leq {} vs {}",
                seqno_since,
                state.seqno_since()
            ));
        }
        if !PartialOrder::less_equal(since, state.since()) {
            return Err(format!(
                "since unexpectedly not leq {:?} vs {:?}",
                since.elements(),
                state.since().elements()
            ));
        }
        if !PartialOrder::less_equal(upper, state.upper()) {
            return Err(format!(
                "upper unexpectedly not leq {:?} vs {:?}",
                upper.elements(),
                state.upper().elements()
            ));
        }
        if *is_tombstone && !state.collections.is_tombstone() {
            return Err(format!(
                "is_tombstone unexpectedly not true {:?} vs {:?}",
                is_tombstone,
                state.collections.is_tombstone()
            ));
        }
        Ok(())
    }

    fn try_update<K, V, D>(&mut self, state: &TypedState<K, V, T, D>) {
        if self.seqno < state.seqno {
            debug_assert_eq!(self.validate_cached_version_of(state), Ok(()));
            // destructure to ensure we set each field
            let Self {
                shard_id: _shard_id,
                seqno,
                upper,
                since,
                seqno_since,
                is_tombstone,
            } = self;

            *seqno = state.seqno;
            if upper != state.upper() {
                upper.clone_from(state.upper());
            }
            if since != state.since() {
                since.clone_from(state.since());
            }
            if *seqno_since != state.seqno_since() {
                *seqno_since = state.seqno_since();
            }
            if *is_tombstone != state.collections.is_tombstone() {
                *is_tombstone = state.collections.is_tombstone();
            }
        }
    }
}

/// An applier of persist commands.
///
/// This struct exists mainly to allow us to very narrowly bound the surface
/// area that directly interacts with state.
#[derive(Debug)]
pub struct Applier<K, V, T, D> {
    pub(crate) cfg: PersistConfig,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) shard_metrics: Arc<ShardMetrics>,
    pub(crate) state_versions: Arc<StateVersions>,

    // Access to the shard's state, shared across all handles created by the same
    // PersistClientCache. The state is wrapped in a std::sync::Mutex, disallowing
    // access across await points. Access should be always be kept brief, and it
    // is expected that other handles may advance the state at any time this Applier
    // is not holding the lock.
    state: Arc<Mutex<TypedState<K, V, T, D>>>,
    // An Applier-local cache of several of the most frequently accessed (and cheapest
    // to copy) values of the latest state, used to reduce lock contention. The cached
    // state should always be updated after attempting to update the shared state.
    cached_state: CachedState<T>,
}

// Impl Clone regardless of the type params.
impl<K, V, T: Clone, D> Clone for Applier<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            metrics: Arc::clone(&self.metrics),
            shard_metrics: Arc::clone(&self.shard_metrics),
            state_versions: Arc::clone(&self.state_versions),
            state: Arc::clone(&self.state),
            cached_state: self.cached_state.clone(),
        }
    }
}

#[derive(Debug)]
struct ExpectationMismatch;

impl<K, V, T, D> Applier<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    pub async fn new(
        cfg: PersistConfig,
        shard_id: ShardId,
        metrics: Arc<Metrics>,
        state_versions: Arc<StateVersions>,
        shared_states: &StateCache,
    ) -> Result<Self, Box<CodecMismatch>> {
        let shard_metrics = metrics.shards.shard(&shard_id);
        let state = shared_states
            .get::<K, V, T, D, _, _>(shard_id, || {
                metrics
                    .cmds
                    .init_state
                    .run_cmd(&shard_metrics, |_cas_mismatch_metric| {
                        // No cas_mismatch retries because we just use the returned
                        // state on a mismatch.
                        state_versions.maybe_init_shard(&shard_metrics)
                    })
            })
            .await?;
        let cached_state = {
            let state = state.lock().expect("lock poisoned");
            CachedState::from(&*state)
        };
        Ok(Applier {
            cfg,
            metrics,
            shard_metrics,
            state_versions,
            state,
            cached_state,
        })
    }

    pub fn state(&self) -> Arc<Mutex<TypedState<K, V, T, D>>> {
        Arc::clone(&self.state)
    }

    pub fn cached_state(&self) -> &CachedState<T> {
        &self.cached_state
    }

    pub fn all_fueled_merge_reqs(&self) -> Vec<FueledMergeReq<T>> {
        self.state
            .lock()
            .expect("lock poisoned")
            .collections
            .trace
            .all_fueled_merge_reqs()
    }

    pub fn snapshot(
        &self,
        as_of: &Antichain<T>,
    ) -> Result<Result<Vec<HollowBatch<T>>, Upper<T>>, Since<T>> {
        self.state.lock().expect("lock poisoned").snapshot(as_of)
    }

    pub fn verify_listen(&self, as_of: &Antichain<T>) -> Result<Result<(), Upper<T>>, Since<T>> {
        self.state
            .lock()
            .expect("lock poisoned")
            .verify_listen(as_of)
    }

    pub fn next_listen_batch(&self, frontier: &Antichain<T>) -> Option<HollowBatch<T>> {
        self.state
            .lock()
            .expect("lock poisoned")
            .next_listen_batch(frontier)
    }

    pub async fn write_rollup_blob(&self, rollup_id: &RollupId) -> EncodedRollup {
        let key = PartialRollupKey::new(self.cached_state.seqno, rollup_id);
        let rollup = self.state_versions.encode_rollup_blob(
            &self.shard_metrics,
            &self.state.lock().expect("lock poisoned"),
            key,
        );
        let () = self.state_versions.write_rollup_blob(&rollup).await;
        rollup
    }

    pub async fn apply_unbatched_cmd<
        R,
        E,
        WorkFn: FnMut(SeqNo, &PersistConfig, &mut StateCollections<T>) -> ControlFlow<E, R>,
    >(
        &mut self,
        cmd: &CmdMetrics,
        mut work_fn: WorkFn,
    ) -> Result<(SeqNo, Result<R, E>, RoutineMaintenance), Indeterminate> {
        loop {
            let ret = Self::apply_unbatched_cmd_locked(
                &self.state,
                cmd,
                &mut work_fn,
                &self.cfg,
                &self.metrics,
                &self.shard_metrics,
                &self.state_versions,
            )
            .await;

            match ret {
                Ok(Ok((seqno, Ok((res, new_state)), maintenance))) => {
                    self.update_state(new_state);
                    return Ok((seqno, Ok(res), maintenance));
                }
                Ok(Ok((seqno, Err(err), maintenance))) => {
                    return Ok((seqno, Err(err), maintenance));
                }
                Ok(Err(err)) => {
                    return Err(err);
                }
                Err(ExpectationMismatch) => {
                    cmd.cas_mismatch.inc();
                    self.fetch_and_update_state().await;
                }
            }
        }
    }

    async fn apply_unbatched_cmd_locked<
        R,
        E,
        WorkFn: FnMut(SeqNo, &PersistConfig, &mut StateCollections<T>) -> ControlFlow<E, R>,
    >(
        state: &Arc<Mutex<TypedState<K, V, T, D>>>,
        cmd: &CmdMetrics,
        work_fn: &mut WorkFn,
        cfg: &PersistConfig,
        metrics: &Metrics,
        shard_metrics: &ShardMetrics,
        state_versions: &StateVersions,
    ) -> Result<
        Result<
            (
                SeqNo,
                Result<(R, TypedState<K, V, T, D>), E>,
                RoutineMaintenance,
            ),
            Indeterminate,
        >,
        ExpectationMismatch,
    > {
        let is_write = cmd.name == metrics.cmds.compare_and_append.name;
        let is_rollup = cmd.name == metrics.cmds.add_and_remove_rollups.name;
        let (expected, diff, new_state, expiry_metrics, garbage_collection, work_ret) = {
            let state = state.lock().expect("lock poisoned");
            let expected = state.seqno;
            let was_tombstone_before = state.collections.is_tombstone();

            let (work_ret, mut new_state) = match state.clone_apply(cfg, work_fn) {
                Continue(x) => x,
                Break(err) => {
                    return Ok(Ok((expected, Err(err), RoutineMaintenance::default())));
                }
            };
            let expiry_metrics = new_state.expire_at((cfg.now)());

            // Sanity check that all state transitions have special case for
            // being a tombstone. The ones that do will return a Break and
            // return out of this method above. The one exception is adding
            // a rollup, because we want to be able to add a rollup for the
            // tombstone state.
            //
            // TODO: Even better would be to write the rollup in the
            // tombstone transition so it's a single terminal state
            // transition, but it'll be tricky to get right.
            if was_tombstone_before && !is_rollup {
                panic!(
                    "cmd {} unexpectedly tried to commit a new state on a tombstone: {:?}",
                    cmd.name, state
                );
            }

            // Find out if this command has been selected to perform gc, so
            // that it will fire off a background request to the
            // GarbageCollector to delete eligible blobs and truncate the
            // state history. This is dependant both on `maybe_gc` returning
            // Some _and_ on this state being successfully compare_and_set.
            //
            // NB: Make sure this overwrites `garbage_collection` on every
            // run though the loop (i.e. no `if let Some` here). When we
            // lose a CaS race, we might discover that the winner got
            // assigned the gc.
            let garbage_collection = new_state.maybe_gc(is_write);

            // NB: Make sure this is the very last thing before the
            // `try_compare_and_set_current` call. (In particular, it needs
            // to come after anything that might modify new_state, such as
            // `maybe_gc`.)
            let diff = StateDiff::from_diff(&state.state, &new_state);
            // Sanity check that our diff logic roundtrips and adds back up
            // correctly.
            #[cfg(any(test, debug_assertions))]
            {
                if let Err(err) = StateDiff::validate_roundtrip(metrics, &state, &diff, &new_state)
                {
                    panic!("validate_roundtrips failed: {}", err);
                }
            }

            (
                expected,
                diff,
                new_state,
                expiry_metrics,
                garbage_collection,
                work_ret,
            )
        };

        // SUBTLE! Unlike the other consensus and blob uses, we can't
        // automatically retry indeterminate ExternalErrors here. However,
        // if the state change itself is _idempotent_, then we're free to
        // retry even indeterminate errors. See
        // [Self::apply_unbatched_idempotent_cmd].
        let cas_res = state_versions
            .try_compare_and_set_current(
                &cmd.name,
                shard_metrics,
                Some(expected),
                &new_state,
                &diff,
            )
            .await;

        match cas_res {
            Ok(CaSResult::Committed) => {
                assert!(
                    expected <= new_state.seqno,
                    "state seqno regressed: {} vs {}",
                    expected,
                    new_state.seqno
                );

                metrics
                    .lease
                    .timeout_read
                    .inc_by(u64::cast_from(expiry_metrics.readers_expired));

                if let Some(gc) = garbage_collection.as_ref() {
                    debug!("Assigned gc request: {:?}", gc);
                }

                let maintenance = RoutineMaintenance {
                    garbage_collection,
                    write_rollup: new_state.need_rollup(),
                };

                Ok(Ok((
                    new_state.seqno,
                    Ok((work_ret, new_state)),
                    maintenance,
                )))
            }
            Ok(CaSResult::ExpectationMismatch) => Err(ExpectationMismatch),
            Err(err) => Ok(Err(err)),
        }
    }

    pub fn update_state(&mut self, new_state: TypedState<K, V, T, D>) {
        let mut state = self.state.lock().expect("lock poisoned");
        let seqno_before = state.seqno;
        state.try_replace_state(new_state);
        self.cached_state.try_update(&state);
        assert!(
            seqno_before <= self.cached_state.seqno,
            "state seqno regressed: {} vs {}",
            seqno_before,
            self.cached_state.seqno
        );
    }

    pub async fn fetch_and_update_state(&mut self) {
        let seqno_before = self.cached_state.seqno;
        self.state_versions
            .fetch_and_update_to_current(&self.state, seqno_before)
            .await
            .expect("shard codecs should not change");
        self.cached_state
            .try_update(&self.state.lock().expect("lock poisoned"));
        assert!(
            seqno_before <= self.cached_state.seqno,
            "state seqno regressed: {} vs {}",
            seqno_before,
            self.cached_state.seqno
        );
    }
}
