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
use std::sync::{Arc, RwLock};

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_ore::cast::CastFrom;
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use tracing::debug;

use mz_persist::location::{CaSResult, Indeterminate, SeqNo};

use crate::cache::StateCache;
use crate::error::CodecMismatch;
use crate::internal::gc::GcReq;
use crate::internal::maintenance::RoutineMaintenance;
use crate::internal::metrics::{CmdMetrics, Metrics, ShardMetrics};
use crate::internal::paths::{PartialRollupKey, RollupId};
use crate::internal::state::{
    ExpiryMetrics, HollowBatch, Since, StateCollections, TypedState, Upper,
};
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

    // Access to the shard's state, shared across all handles created by the same
    // PersistClientCache. The state is wrapped in a std::sync::RwLock, disallowing
    // access across await points. Access should be always be kept brief, and it
    // is expected that other handles may advance the state at any time this Applier
    // is not holding the lock.
    state: Arc<RwLock<TypedState<K, V, T, D>>>,
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
        }
    }
}

#[derive(Debug)]
struct ExpectationMismatch(SeqNo);

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
        Ok(Applier {
            cfg,
            metrics,
            shard_metrics,
            state_versions,
            state,
        })
    }

    pub fn read_locked_state<R, F: Fn(&TypedState<K, V, T, D>) -> R>(&self, f: F) -> R {
        let state = self.state.read().expect("lock poisoned");
        f(&state)
    }

    pub fn all_fueled_merge_reqs(&self) -> Vec<FueledMergeReq<T>> {
        self.read_locked_state(|state| state.collections.trace.all_fueled_merge_reqs())
    }

    pub fn snapshot(
        &self,
        as_of: &Antichain<T>,
    ) -> Result<Result<Vec<HollowBatch<T>>, Upper<T>>, Since<T>> {
        self.read_locked_state(|state| state.snapshot(as_of))
    }

    pub fn verify_listen(&self, as_of: &Antichain<T>) -> Result<Result<(), Upper<T>>, Since<T>> {
        self.read_locked_state(|state| state.verify_listen(as_of))
    }

    pub fn next_listen_batch(&self, frontier: &Antichain<T>) -> Option<HollowBatch<T>> {
        self.read_locked_state(|state| state.next_listen_batch(frontier))
    }

    pub async fn write_rollup_blob(&self, rollup_id: &RollupId) -> EncodedRollup {
        let rollup = self.read_locked_state(|state| {
            let key = PartialRollupKey::new(state.seqno, rollup_id);
            self.state_versions
                .encode_rollup_blob(&self.shard_metrics, &state, key)
        });
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
                ApplyCmdResult::Committed((seqno, new_state, res, maintenance)) => {
                    self.update_state(new_state);
                    return Ok((seqno, Ok(res), maintenance));
                }
                ApplyCmdResult::SkippedStateTransition((seqno, err, maintenance)) => {
                    return Ok((seqno, Err(err), maintenance));
                }
                ApplyCmdResult::Indeterminate(err) => {
                    return Err(err);
                }
                ApplyCmdResult::ExpectationMismatch(seqno) => {
                    cmd.cas_mismatch.inc();
                    self.fetch_and_update_state(Some(seqno)).await;
                }
            }
        }
    }

    async fn apply_unbatched_cmd_locked<
        R,
        E,
        WorkFn: FnMut(SeqNo, &PersistConfig, &mut StateCollections<T>) -> ControlFlow<E, R>,
    >(
        state: &Arc<RwLock<TypedState<K, V, T, D>>>,
        cmd: &CmdMetrics,
        work_fn: &mut WorkFn,
        cfg: &PersistConfig,
        metrics: &Metrics,
        shard_metrics: &ShardMetrics,
        state_versions: &StateVersions,
    ) -> ApplyCmdResult<K, V, T, D, R, E> {
        let next_state = match Self::compute_next_state_locked(state, work_fn, metrics, cmd, cfg) {
            Ok(x) => x,
            Err((seqno, err)) => {
                return ApplyCmdResult::SkippedStateTransition((
                    seqno,
                    err,
                    RoutineMaintenance::default(),
                ))
            }
        };

        let NextState {
            expected,
            diff,
            state,
            expiry_metrics,
            garbage_collection,
            work_ret,
        } = next_state;

        // SUBTLE! Unlike the other consensus and blob uses, we can't
        // automatically retry indeterminate ExternalErrors here. However,
        // if the state change itself is _idempotent_, then we're free to
        // retry even indeterminate errors. See
        // [Self::apply_unbatched_idempotent_cmd].
        let cas_res = state_versions
            .try_compare_and_set_current(&cmd.name, shard_metrics, Some(expected), &state, &diff)
            .await;

        match cas_res {
            Ok(CaSResult::Committed) => {
                assert!(
                    expected <= state.seqno,
                    "state seqno regressed: {} vs {}",
                    expected,
                    state.seqno
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
                    write_rollup: state.need_rollup(),
                };

                ApplyCmdResult::Committed((state.seqno, state, work_ret, maintenance))
            }
            Ok(CaSResult::ExpectationMismatch) => ApplyCmdResult::ExpectationMismatch(expected),
            Err(err) => ApplyCmdResult::Indeterminate(err),
        }
    }

    fn compute_next_state_locked<
        R,
        E,
        WorkFn: FnMut(SeqNo, &PersistConfig, &mut StateCollections<T>) -> ControlFlow<E, R>,
    >(
        state: &Arc<RwLock<TypedState<K, V, T, D>>>,
        work_fn: &mut WorkFn,
        metrics: &Metrics,
        cmd: &CmdMetrics,
        cfg: &PersistConfig,
    ) -> Result<NextState<K, V, T, D, R>, (SeqNo, E)> {
        let is_write = cmd.name == metrics.cmds.compare_and_append.name;
        let is_rollup = cmd.name == metrics.cmds.add_and_remove_rollups.name;

        let state = state.read().expect("lock poisoned");
        let expected = state.seqno;
        let was_tombstone_before = state.collections.is_tombstone();

        let (work_ret, mut new_state) = match state.clone_apply(cfg, work_fn) {
            Continue(x) => x,
            Break(err) => {
                return Err((expected, err));
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
            if let Err(err) = StateDiff::validate_roundtrip(metrics, &state, &diff, &new_state) {
                panic!("validate_roundtrips failed: {}", err);
            }
        }

        Ok(NextState {
            expected,
            diff,
            state: new_state,
            expiry_metrics,
            garbage_collection,
            work_ret,
        })
    }

    pub fn update_state(&mut self, new_state: TypedState<K, V, T, D>) {
        let (seqno_before, seqno_after) = {
            let mut state = self.state.write().expect("lock poisoned");
            let seqno_before = state.seqno;
            state.try_replace_state(new_state);
            let seqno_after = state.seqno;
            (seqno_before, seqno_after)
        };

        assert!(
            seqno_before <= seqno_after,
            "state seqno regressed: {} vs {}",
            seqno_before,
            seqno_after
        );
    }

    /// Fetches and updates to the latest state. Uses an optional hint to early-out if
    /// any more recent version of state is observed (e.g. updated by another handle),
    /// without making any calls to Consensus or Blob.
    pub async fn fetch_and_update_state(&mut self, seqno_hint: Option<SeqNo>) {
        let seqno_before =
            seqno_hint.unwrap_or_else(|| self.read_locked_state(|state| state.seqno));
        let seqno_after = self
            .state_versions
            .fetch_and_update_to_current(&self.state, seqno_before)
            .await
            .expect("shard codecs should not change");
        assert!(
            seqno_before <= seqno_after,
            "state seqno regressed: {} vs {}",
            seqno_before,
            seqno_after
        );
    }
}

enum ApplyCmdResult<K, V, T, D, R, E> {
    Committed((SeqNo, TypedState<K, V, T, D>, R, RoutineMaintenance)),
    SkippedStateTransition((SeqNo, E, RoutineMaintenance)),
    Indeterminate(Indeterminate),
    ExpectationMismatch(SeqNo),
}

struct NextState<K, V, T, D, R> {
    expected: SeqNo,
    diff: StateDiff<T>,
    state: TypedState<K, V, T, D>,
    expiry_metrics: ExpiryMetrics,
    garbage_collection: Option<GcReq>,
    work_ret: R,
}
