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
use std::time::Instant;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_ore::cast::CastFrom;
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use tracing::debug;

use mz_persist::location::{CaSResult, Indeterminate, SeqNo};

use crate::cache::{LockingTypedState, StateCache};
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
use crate::internal::watch::StateWatch;
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
    pub(crate) shard_id: ShardId,

    // Access to the shard's state, shared across all handles created by the same
    // PersistClientCache. The state is wrapped in LockingTypedState, disallowing
    // access across await points. Access should be always be kept brief, and it
    // is expected that other handles may advance the state at any time this Applier
    // is not holding the lock.
    //
    // NB: This is very intentionally not pub(crate) so that it's easy to reason
    //     very locally about the duration of locks.
    state: Arc<LockingTypedState<K, V, T, D>>,
}

// Impl Clone regardless of the type params.
impl<K, V, T: Clone, D> Clone for Applier<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            metrics: Arc::clone(&self.metrics),
            shard_metrics: Arc::clone(&self.shard_metrics),
            state_versions: Arc::clone(&self.state_versions),
            shard_id: self.shard_id,
            state: Arc::clone(&self.state),
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
            .get::<K, V, T, D, _, _>(shard_id, || {
                metrics.cmds.init_state.run_cmd(&shard_metrics, || {
                    state_versions.maybe_init_shard(&shard_metrics)
                })
            })
            .await?;
        Ok(Applier {
            cfg,
            metrics,
            shard_metrics,
            state_versions,
            shard_id,
            state,
        })
    }

    /// Returns a new [StateWatch] for changes to this Applier's State.
    pub fn watch(&self) -> StateWatch<K, V, T, D> {
        StateWatch::new(Arc::clone(&self.state), &self.metrics)
    }

    /// Fetches the latest state from Consensus and passes its `upper` to the provided closure.
    pub async fn fetch_upper<R, F: FnMut(&Antichain<T>) -> R>(&mut self, f: F) -> R {
        self.fetch_and_update_state(None).await;
        self.upper(f)
    }

    /// A point-in-time read/clone of `upper` from the current state.
    ///
    /// Due to sharing state with other handles, successive reads to this fn or any other may
    /// see a different version of state, even if this Applier has not explicitly fetched and
    /// updated to the latest state. Successive calls will always return values such that
    /// `PartialOrder::less_equal(call1, call2)` hold true.
    pub fn clone_upper(&self) -> Antichain<T> {
        self.upper(|upper| upper.clone())
    }

    fn upper<R, F: FnMut(&Antichain<T>) -> R>(&self, mut f: F) -> R {
        self.state
            .read_lock(&self.metrics.locks.applier_read_cacheable, move |state| {
                f(state.upper())
            })
    }

    /// A point-in-time read of `since` from the current state.
    ///
    /// Due to sharing state with other handles, successive reads to this fn or any other may
    /// see a different version of state, even if this Applier has not explicitly fetched and
    /// updated to the latest state. Successive calls will always return values such that
    /// `PartialOrder::less_equal(call1, call2)` hold true.
    #[cfg(test)]
    pub fn since(&self) -> Antichain<T> {
        self.state
            .read_lock(&self.metrics.locks.applier_read_cacheable, |state| {
                state.since().clone()
            })
    }

    /// A point-in-time read of `seqno` from the current state.
    ///
    /// Due to sharing state with other handles, successive reads to this fn or any other may
    /// see a different version of state, even if this Applier has not explicitly fetched and
    /// updated to the latest state. Successive calls will always return values such that
    /// `call1 <= call2` hold true.
    pub fn seqno(&self) -> SeqNo {
        self.state
            .read_lock(&self.metrics.locks.applier_read_cacheable, |state| {
                state.seqno
            })
    }

    /// A point-in-time read of `seqno_since` from the current state.
    ///
    /// Due to sharing state with other handles, successive reads to this fn or any other may
    /// see a different version of state, even if this Applier has not explicitly fetched and
    /// updated to the latest state. Successive calls will always return values such that
    /// `call1 <= call2` hold true.
    pub fn seqno_since(&self) -> SeqNo {
        self.state
            .read_lock(&self.metrics.locks.applier_read_cacheable, |state| {
                state.seqno_since()
            })
    }

    /// A point-in-time read of `is_tombstone` from the current state.
    ///
    /// Due to sharing state with other handles, successive reads to this fn or any other may
    /// see a different version of state, even if this Applier has not explicitly fetched and
    /// updated to the latest state. Once this fn returns true, it will always return true.
    pub fn is_tombstone(&self) -> bool {
        self.state
            .read_lock(&self.metrics.locks.applier_read_cacheable, |state| {
                state.collections.is_tombstone()
            })
    }

    /// Returns whether the current's state `since` and `upper` are both empty.
    ///
    /// Due to sharing state with other handles, successive reads to this fn or any other may
    /// see a different version of state, even if this Applier has not explicitly fetched and
    /// updated to the latest state. Once this fn returns true, it will always return true.
    pub fn since_upper_both_empty(&self) -> bool {
        self.state
            .read_lock(&self.metrics.locks.applier_read_cacheable, |state| {
                state.since().is_empty() && state.upper().is_empty()
            })
    }

    pub fn all_fueled_merge_reqs(&self) -> Vec<FueledMergeReq<T>> {
        self.state
            .read_lock(&self.metrics.locks.applier_read_noncacheable, |state| {
                state.collections.trace.all_fueled_merge_reqs()
            })
    }

    pub fn snapshot(
        &self,
        as_of: &Antichain<T>,
    ) -> Result<Result<Vec<HollowBatch<T>>, Upper<T>>, Since<T>> {
        self.state
            .read_lock(&self.metrics.locks.applier_read_noncacheable, |state| {
                state.snapshot(as_of)
            })
    }

    pub fn verify_listen(&self, as_of: &Antichain<T>) -> Result<Result<(), Upper<T>>, Since<T>> {
        self.state
            .read_lock(&self.metrics.locks.applier_read_noncacheable, |state| {
                state.verify_listen(as_of)
            })
    }

    pub fn next_listen_batch(&self, frontier: &Antichain<T>) -> Result<HollowBatch<T>, SeqNo> {
        self.state
            .read_lock(&self.metrics.locks.applier_read_noncacheable, |state| {
                state.next_listen_batch(frontier)
            })
    }

    pub async fn write_rollup_blob(&self, rollup_id: &RollupId) -> EncodedRollup {
        let rollup = self
            .state
            .read_lock(&self.metrics.locks.applier_read_noncacheable, |state| {
                let key = PartialRollupKey::new(state.seqno, rollup_id);
                self.state_versions
                    .encode_rollup_blob(&self.shard_metrics, state, key)
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
            cmd.started.inc();
            let now = Instant::now();
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
            cmd.seconds.inc_by(now.elapsed().as_secs_f64());

            match ret {
                ApplyCmdResult::Committed((seqno, new_state, res, maintenance)) => {
                    cmd.succeeded.inc();
                    self.shard_metrics.cmd_succeeded.inc();
                    self.update_state(new_state);
                    return Ok((seqno, Ok(res), maintenance));
                }
                ApplyCmdResult::SkippedStateTransition((seqno, err, maintenance)) => {
                    cmd.succeeded.inc();
                    self.shard_metrics.cmd_succeeded.inc();
                    return Ok((seqno, Err(err), maintenance));
                }
                ApplyCmdResult::Indeterminate(err) => {
                    cmd.failed.inc();
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
        state: &LockingTypedState<K, V, T, D>,
        cmd: &CmdMetrics,
        work_fn: &mut WorkFn,
        cfg: &PersistConfig,
        metrics: &Metrics,
        shard_metrics: &ShardMetrics,
        state_versions: &StateVersions,
    ) -> ApplyCmdResult<K, V, T, D, R, E> {
        let computed_next_state = state
            .read_lock(&metrics.locks.applier_read_noncacheable, |state| {
                Self::compute_next_state_locked(state, work_fn, metrics, cmd, cfg)
            });

        let next_state = match computed_next_state {
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
        state: &TypedState<K, V, T, D>,
        work_fn: &mut WorkFn,
        metrics: &Metrics,
        cmd: &CmdMetrics,
        cfg: &PersistConfig,
    ) -> Result<NextState<K, V, T, D, R>, (SeqNo, E)> {
        let is_write = cmd.name == metrics.cmds.compare_and_append.name;
        let is_rollup = cmd.name == metrics.cmds.add_and_remove_rollups.name;

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
            if let Err(err) = StateDiff::validate_roundtrip(metrics, state, &diff, &new_state) {
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
        let (seqno_before, seqno_after) =
            self.state
                .write_lock(&self.metrics.locks.applier_write, |state| {
                    let seqno_before = state.seqno;
                    if seqno_before < new_state.seqno {
                        *state = new_state;
                    }
                    let seqno_after = state.seqno;
                    (seqno_before, seqno_after)
                });

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
        let current_seqno = self.seqno();
        let seqno_before = match seqno_hint {
            None => current_seqno,
            Some(hint) => {
                // state is already more recent than our hint due to
                // advancement by another handle to the same shard.
                if hint < current_seqno {
                    self.metrics.state.update_state_noop_path.inc();
                    return;
                }
                current_seqno
            }
        };

        let diffs_to_current = self
            .state_versions
            .fetch_all_live_diffs_gt_seqno::<K, V, T, D>(&self.shard_id, seqno_before)
            .await;

        // no new diffs past our current seqno, nothing to do
        if diffs_to_current.is_empty() {
            self.metrics.state.update_state_empty_path.inc();
            return;
        }

        let new_seqno = self
            .state
            .write_lock(&self.metrics.locks.applier_write, |state| {
                state.apply_encoded_diffs(&self.cfg, &self.metrics, &diffs_to_current);
                state.seqno
            });

        assert!(
            seqno_before <= new_seqno,
            "state seqno regressed: {} vs {}",
            seqno_before,
            new_seqno
        );

        // whether the seqno advanced from diffs and/or because another handle
        // already updated it, we can assume it is now up-to-date
        if seqno_before < new_seqno {
            self.metrics.state.update_state_fast_path.inc();
            return;
        }

        // our state is so old there aren't any diffs we can use to
        // catch up directly. fall back to fully refetching state.
        // we can reuse the recent diffs we already have as a hint.
        let new_state = self
            .state_versions
            .fetch_current_state(&self.shard_id, diffs_to_current)
            .await
            .check_codecs::<K, V, D>(&self.shard_id)
            .expect("shard codecs should not change");

        let new_seqno = self
            .state
            .write_lock(&self.metrics.locks.applier_write, |state| {
                if state.seqno < new_state.seqno {
                    *state = new_state;
                }
                state.seqno
            });

        self.metrics.state.update_state_slow_path.inc();
        assert!(
            seqno_before <= new_seqno,
            "state seqno regressed: {} vs {}",
            seqno_before,
            new_seqno
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
