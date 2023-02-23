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
use std::sync::Arc;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_ore::cast::CastFrom;
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::sync::Mutex;
use tracing::debug;

use mz_persist::location::{Indeterminate, SeqNo};

use crate::cache::StateCache;
use crate::error::CodecMismatch;
use crate::internal::maintenance::RoutineMaintenance;
use crate::internal::metrics::{CmdMetrics, Metrics, ShardMetrics};
use crate::internal::paths::{PartialRollupKey, RollupId};
use crate::internal::state::{HollowBatch, Since, State, StateCollections, TypedState, Upper};
use crate::internal::state_diff::StateDiff;
use crate::internal::state_versions::{EncodedRollup, StateVersions};
use crate::internal::trace::FueledMergeReq;
use crate::{PersistConfig, ShardId};

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

    // NB: This is very intentionally not pub(crate) so that it's easy to reason
    // very locally about the duration of Mutex holds.
    state: Arc<Mutex<TypedState<K, V, T, D>>>,

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

// WIP probably make this <K, V, T, D>
#[derive(Debug, Clone)]
pub struct CachedState<T> {
    pub shard_id: ShardId,
    pub seqno: SeqNo,
    pub upper: Antichain<T>,
    pub since: Antichain<T>,
    pub seqno_since: SeqNo,
    pub is_tombstone: bool,
}

impl<T: Timestamp + Lattice + Codec64> From<&State<T>> for CachedState<T> {
    fn from(value: &State<T>) -> Self {
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
    fn validate_cached_version_of(&self, state: &State<T>) -> Result<(), String> {
        if self.shard_id != state.shard_id {
            return Err(format!(
                "shard_id didn't match {} vs {}",
                self.shard_id, state.shard_id
            ));
        }
        if !(self.seqno <= state.seqno) {
            return Err(format!(
                "seqno unexpectedly not leq {} vs {}",
                self.seqno, state.seqno
            ));
        }
        if !PartialOrder::less_equal(&self.since, state.since()) {
            return Err(format!(
                "since unexpectedly not leq {:?} vs {:?}",
                self.since.elements(),
                state.since().elements()
            ));
        }
        if !PartialOrder::less_equal(&self.upper, state.upper()) {
            return Err(format!(
                "upper unexpectedly not leq {:?} vs {:?}",
                self.upper.elements(),
                state.upper().elements()
            ));
        }
        Ok(())
    }

    fn update<K, V, D>(&mut self, state: &TypedState<K, V, T, D>) {
        debug_assert_eq!(self.validate_cached_version_of(&state.state), Ok(()));
        self.seqno = state.seqno;
        if &self.upper != state.upper() {
            self.upper.clone_from(state.upper());
        }
        if &self.since != state.since() {
            self.since.clone_from(state.since());
        }
        let is_tombstone = state.collections.is_tombstone();
        if self.is_tombstone != is_tombstone {
            self.is_tombstone = is_tombstone;
        }
    }
}

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
            .get(shard_id, || async {
                metrics
                    .cmds
                    .init_state
                    .run_cmd(&shard_metrics, |_cas_mismatch_metric| {
                        // No cas_mismatch retries because we just use the returned
                        // state on a mismatch.
                        state_versions.maybe_init_shard(&shard_metrics)
                    })
                    .await
            })
            .await?;
        let cached_state = {
            let state = state.lock().await;
            CachedState::from(&state.state)
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

    pub fn cached_state(&self) -> &CachedState<T> {
        &self.cached_state
    }

    pub async fn all_fueled_merge_reqs(&self) -> Vec<FueledMergeReq<T>> {
        self.state
            .lock()
            .await
            .collections
            .trace
            .all_fueled_merge_reqs()
    }

    pub async fn snapshot(
        &self,
        as_of: &Antichain<T>,
    ) -> Result<Result<Vec<HollowBatch<T>>, Upper<T>>, Since<T>> {
        self.state.lock().await.snapshot(as_of)
    }

    pub async fn verify_listen(
        &self,
        as_of: &Antichain<T>,
    ) -> Result<Result<(), Upper<T>>, Since<T>> {
        self.state.lock().await.verify_listen(as_of)
    }

    pub async fn next_listen_batch(&self, frontier: &Antichain<T>) -> Option<HollowBatch<T>> {
        self.state.lock().await.next_listen_batch(frontier)
    }

    pub async fn write_rollup_blob(&self, rollup_id: &RollupId) -> EncodedRollup {
        let rollup = {
            let state = self.state.lock().await;
            let key = PartialRollupKey::new(state.seqno, rollup_id);
            self.state_versions
                .encode_rollup_blob(&self.shard_metrics, &state, key)
        };
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
        work_fn: WorkFn,
    ) -> Result<(SeqNo, Result<R, E>, RoutineMaintenance), Indeterminate> {
        let mut state = self.state.lock().await;
        let ret = Self::apply_unbatched_cmd_locked(
            &mut state,
            cmd,
            work_fn,
            &self.cfg,
            &self.metrics,
            &self.shard_metrics,
            &self.state_versions,
        )
        .await;
        self.cached_state.update(&state);
        ret
    }

    async fn apply_unbatched_cmd_locked<
        R,
        E,
        WorkFn: FnMut(SeqNo, &PersistConfig, &mut StateCollections<T>) -> ControlFlow<E, R>,
    >(
        state: &mut TypedState<K, V, T, D>,
        cmd: &CmdMetrics,
        mut work_fn: WorkFn,
        cfg: &PersistConfig,
        metrics: &Metrics,
        shard_metrics: &ShardMetrics,
        state_versions: &StateVersions,
    ) -> Result<(SeqNo, Result<R, E>, RoutineMaintenance), Indeterminate> {
        let is_write = cmd.name == metrics.cmds.compare_and_append.name;
        let is_rollup = cmd.name == metrics.cmds.add_and_remove_rollups.name;
        cmd.run_cmd(shard_metrics, |cas_mismatch_metric| async move {
            let mut garbage_collection;
            let mut expiry_metrics;

            loop {
                let was_tombstone_before = state.collections.is_tombstone();

                let (work_ret, mut new_state) = match state.clone_apply(cfg, &mut work_fn) {
                    Continue(x) => x,
                    Break(err) => {
                        return Ok((state.seqno(), Err(err), RoutineMaintenance::default()))
                    }
                };
                expiry_metrics = new_state.expire_at((cfg.now)());

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
                garbage_collection = new_state.maybe_gc(is_write);

                // NB: Make sure this is the very last thing before the
                // `try_compare_and_set_current` call. (In particular, it needs
                // to come after anything that might modify new_state, such as
                // `maybe_gc`.)
                let diff = StateDiff::from_diff(state, &new_state);
                // Sanity check that our diff logic roundtrips and adds back up
                // correctly.
                #[cfg(any(test, debug_assertions))]
                {
                    if let Err(err) =
                        StateDiff::validate_roundtrip(metrics, state, &diff, &new_state)
                    {
                        panic!("validate_roundtrips failed: {}", err);
                    }
                }

                // SUBTLE! Unlike the other consensus and blob uses, we can't
                // automatically retry indeterminate ExternalErrors here. However,
                // if the state change itself is _idempotent_, then we're free to
                // retry even indeterminate errors. See
                // [Self::apply_unbatched_idempotent_cmd].
                let expected = state.seqno();
                let cas_res = state_versions
                    .try_compare_and_set_current(
                        &cmd.name,
                        shard_metrics,
                        Some(expected),
                        &new_state,
                        &diff,
                    )
                    .await?;
                match cas_res {
                    Ok(()) => {
                        assert!(
                            state.seqno <= new_state.seqno,
                            "state seqno regressed: {} vs {}",
                            state.seqno,
                            new_state.seqno
                        );
                        *state = new_state;

                        metrics
                            .lease
                            .timeout_read
                            .inc_by(u64::cast_from(expiry_metrics.readers_expired));

                        if let Some(gc) = garbage_collection.as_ref() {
                            debug!("Assigned gc request: {:?}", gc);
                        }

                        let maintenance = RoutineMaintenance {
                            garbage_collection,
                            write_rollup: state.need_rollup(),
                        };

                        return Ok((state.seqno(), Ok(work_ret), maintenance));
                    }
                    Err(diffs_to_current) => {
                        cas_mismatch_metric.0.inc();

                        let seqno_before = state.seqno;
                        let diffs_apply = diffs_to_current
                            .first()
                            .map_or(true, |x| x.seqno == seqno_before.next());
                        if diffs_apply {
                            metrics.state.update_state_fast_path.inc();
                            state.apply_encoded_diffs(cfg, metrics, &diffs_to_current)
                        } else {
                            // Otherwise, we've gc'd the diffs we'd need to
                            // advance self.state to where diffs_to_current
                            // starts so we need a new rollup.
                            metrics.state.update_state_slow_path.inc();
                            debug!(
                                "update_state didn't hit update_state fast path {} {:?}",
                                state.seqno,
                                diffs_to_current.first().map(|x| x.seqno),
                            );

                            // SUBTLE: Consensus::compare_and_set guarantees
                            // that we get back everything in
                            // `[max(expected.next(),earliest),current]` on
                            // expectation mismatch. If `diffs_apply` is false
                            // (this branch), then we know `earliest >
                            // expected.next()` and so `diffs_to_current` is
                            // already the full set of all live diffs. Sanity
                            // check this deduction with an assert and then use
                            // it to `fetch_current_state`.
                            assert!(
                                diffs_to_current
                                    .first()
                                    .map_or(false, |x| x.seqno > expected.next()),
                                "{:?} vs {}",
                                diffs_to_current.first(),
                                expected.next()
                            );
                            let all_live_diffs = diffs_to_current;
                            *state = state_versions
                                .fetch_current_state(&state.shard_id, all_live_diffs)
                                .await
                                .check_codecs(&state.shard_id)
                                .expect("shard codecs should not change");
                        }

                        // Intentionally don't backoff here. It would only make
                        // starvation issues even worse.
                        continue;
                    }
                }
            }
        })
        .await
    }

    pub async fn fetch_and_update_state(&mut self) {
        let mut state = self.state.lock().await;
        let seqno_before = state.seqno;
        self.state_versions
            .fetch_and_update_to_current(&mut state)
            .await
            .expect("shard codecs should not change");
        self.cached_state.update(&state);
        assert!(
            seqno_before <= state.seqno,
            "state seqno regressed: {} vs {}",
            seqno_before,
            state.seqno
        );
    }
}
