// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of the persist state machine.

use std::convert::Infallible;
use std::fmt::Debug;
use std::ops::{ControlFlow, ControlFlow::Break, ControlFlow::Continue};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_ore::cast::CastFrom;
use timely::progress::{Antichain, Timestamp};
use tracing::{debug, info, trace_span, Instrument};

#[allow(unused_imports)] // False positive.
use mz_ore::fmt::FormatBuffer;
use mz_persist::location::{ExternalError, Indeterminate, SeqNo};
use mz_persist::retry::Retry;
use mz_persist_types::{Codec, Codec64};

use crate::error::{CodecMismatch, InvalidUsage};
use crate::internal::compact::CompactReq;
use crate::internal::gc::GcReq;
use crate::internal::maintenance::{LeaseExpiration, RoutineMaintenance, WriterMaintenance};
use crate::internal::metrics::{
    CmdMetrics, Metrics, MetricsRetryStream, RetryMetrics, ShardMetrics,
};
use crate::internal::paths::{PartialRollupKey, RollupId};
use crate::internal::state::{
    HollowBatch, ReaderState, Since, State, StateCollections, Upper, WriterState,
};
use crate::internal::state_diff::StateDiff;
use crate::internal::state_versions::StateVersions;
use crate::internal::trace::FueledMergeRes;
use crate::read::ReaderId;
use crate::write::WriterId;
use crate::{PersistConfig, ShardId};

#[derive(Debug)]
pub struct Machine<K, V, T, D> {
    pub(crate) cfg: PersistConfig,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) shard_metrics: Arc<ShardMetrics>,
    pub(crate) state_versions: Arc<StateVersions>,

    state: State<K, V, T, D>,
}

// Impl Clone regardless of the type params.
impl<K, V, T: Clone, D> Clone for Machine<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            metrics: Arc::clone(&self.metrics),
            shard_metrics: Arc::clone(&self.shard_metrics),
            state_versions: Arc::clone(&self.state_versions),
            state: self.state.clone(self.cfg.build_version.clone()),
        }
    }
}

impl<K, V, T, D> Machine<K, V, T, D>
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
    ) -> Result<Self, CodecMismatch> {
        let shard_metrics = metrics.shards.shard(&shard_id);
        let state = metrics
            .cmds
            .init_state
            .run_cmd(|_cas_mismatch_metric| {
                // No cas_mismatch retries because we just use the returned
                // state on a mismatch.
                state_versions.maybe_init_shard(&shard_metrics)
            })
            .await?;
        Ok(Machine {
            cfg,
            metrics,
            shard_metrics,
            state_versions,
            state,
        })
    }

    pub fn shard_id(&self) -> ShardId {
        self.state.shard_id()
    }

    pub async fn fetch_upper(&mut self) -> &Antichain<T> {
        self.fetch_and_update_state().await;
        self.state.upper()
    }

    pub fn upper(&self) -> &Antichain<T> {
        self.state.upper()
    }

    pub fn seqno(&self) -> SeqNo {
        self.state.seqno()
    }

    #[cfg(test)]
    pub fn seqno_since(&self) -> SeqNo {
        self.state.seqno_since()
    }

    pub async fn add_rollup_for_current_seqno(&mut self) {
        let rollup_seqno = self.state.seqno;
        let rollup_key = PartialRollupKey::new(rollup_seqno, &RollupId::new());
        let () = self
            .state_versions
            .write_rollup_blob(&self.shard_metrics, &self.state, &rollup_key)
            .await;
        let applied = self
            .add_and_remove_rollups((rollup_seqno, &rollup_key), &[])
            .await;
        if !applied {
            // Someone else already wrote a rollup at this seqno, so ours didn't
            // get added. Delete it.
            self.state_versions
                .delete_rollup(&self.state.shard_id, &rollup_key)
                .await;
        }
    }

    pub async fn add_and_remove_rollups(
        &mut self,
        add_rollup: (SeqNo, &PartialRollupKey),
        remove_rollups: &[(SeqNo, PartialRollupKey)],
    ) -> bool {
        // See the big SUBTLE comment in [Self::merge_res] for what's going on
        // here.
        let mut applied_ever_true = false;
        let metrics = Arc::clone(&self.metrics);
        let (_seqno, _applied, _maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.add_and_remove_rollups, |_, state| {
                let ret = state.add_and_remove_rollups(add_rollup, remove_rollups);
                if let Continue(applied) = ret {
                    applied_ever_true = applied_ever_true || applied;
                }
                ret
            })
            .await;
        applied_ever_true
    }

    pub async fn register_reader(
        &mut self,
        reader_id: &ReaderId,
        heartbeat_timestamp_ms: u64,
    ) -> (Upper<T>, ReaderState<T>) {
        let metrics = Arc::clone(&self.metrics);
        let (seqno, (shard_upper, read_cap), _maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.register, |seqno, state| {
                state.register_reader(reader_id, seqno, heartbeat_timestamp_ms)
            })
            .await;
        debug_assert_eq!(seqno, read_cap.seqno);
        (shard_upper, read_cap)
    }

    pub async fn register_writer(
        &mut self,
        writer_id: &WriterId,
        lease_duration: Duration,
        heartbeat_timestamp_ms: u64,
    ) -> (Upper<T>, WriterState) {
        let metrics = Arc::clone(&self.metrics);
        let (_seqno, (shard_upper, writer_state), _maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.register, |_seqno, state| {
                state.register_writer(writer_id, lease_duration, heartbeat_timestamp_ms)
            })
            .await;
        (shard_upper, writer_state)
    }

    pub async fn clone_reader(
        &mut self,
        new_reader_id: &ReaderId,
        heartbeat_timestamp_ms: u64,
    ) -> ReaderState<T> {
        let metrics = Arc::clone(&self.metrics);
        let (seqno, read_cap, _maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.clone_reader, |seqno, state| {
                state.clone_reader(new_reader_id, seqno, heartbeat_timestamp_ms)
            })
            .await;
        debug_assert_eq!(seqno, read_cap.seqno);
        read_cap
    }

    pub async fn compare_and_append(
        &mut self,
        batch: &HollowBatch<T>,
        writer_id: &WriterId,
        heartbeat_timestamp_ms: u64,
    ) -> Result<
        Result<Result<(SeqNo, WriterMaintenance<T>), Upper<T>>, InvalidUsage<T>>,
        Indeterminate,
    > {
        let metrics = Arc::clone(&self.metrics);
        loop {
            let (seqno, res, routine) = self
                .apply_unbatched_cmd(&metrics.cmds.compare_and_append, |_, state| {
                    state.compare_and_append(batch, writer_id, heartbeat_timestamp_ms)
                })
                .await?;

            match res {
                Ok(merge_reqs) => {
                    let mut compact_reqs = Vec::with_capacity(merge_reqs.len());
                    for req in merge_reqs {
                        let req = CompactReq {
                            shard_id: self.shard_id(),
                            desc: req.desc,
                            inputs: req.inputs,
                        };
                        compact_reqs.push(req);
                    }
                    let writer_maintenance = WriterMaintenance {
                        routine,
                        compaction: compact_reqs,
                    };
                    return Ok(Ok(Ok((seqno, writer_maintenance))));
                }
                Err(Ok(_current_upper)) => {
                    // If the state machine thinks that the shard upper is not
                    // far enough along, it could be because the caller of this
                    // method has found out that it advanced via some some
                    // side-channel that didn't update our local cache of the
                    // machine state. So, fetch the latest state and try again
                    // if we indeed get something different.
                    self.fetch_and_update_state().await;
                    let current_upper = self.upper();

                    // We tried to to a compare_and_append with the wrong
                    // expected upper, that won't work.
                    if current_upper != batch.desc.lower() {
                        return Ok(Ok(Err(Upper(current_upper.clone()))));
                    } else {
                        // The upper stored in state was outdated. Retry after
                        // updating.
                    }
                }
                Err(Err(invalid_usage)) => {
                    return Ok(Err(invalid_usage));
                }
            }
        }
    }

    pub async fn merge_res(&mut self, res: &FueledMergeRes<T>) -> bool {
        let metrics = Arc::clone(&self.metrics);

        // SUBTLE! If Machine::merge_res returns false, the blobs referenced in
        // compaction output are deleted so we don't leak them. Naively passing
        // back the value returned by State::apply_merge_res might give a false
        // negative in the presence of retries and Indeterminate errors.
        // Specifically, something like the following:
        //
        // - We try to apply_merge_res, it matches.
        // - When apply_unbatched_cmd goes to commit the new state, the
        //   Consensus::compare_and_set returns an Indeterminate error (but
        //   actually succeeds). The committed State now contains references to
        //   the compaction output blobs.
        // - Machine::apply_unbatched_idempotent_cmd retries the Indeterminate
        //   error. For whatever reason, this time though it doesn't match
        //   (maybe the batches simply get grouped difference when deserialized
        //   from state, or more unavoidably perhaps another compaction
        //   happens).
        // - This now bubbles up applied=false to the caller, which uses it as a
        //   signal that the blobs in the compaction output should be deleted so
        //   that we don't leak them.
        // - We now contain references in committed State to blobs that don't
        //   exist.
        //
        // The fix is to keep track of whether applied ever was true, even for a
        // compare_and_set that returned an Indeterminate error. This has the
        // chance of false positive (leaking a blob) but that's better than a
        // false negative (a blob we can never recover referenced by state). We
        // anyway need a mechanism to clean up leaked blobs because of process
        // crashes.
        let mut applied_ever_true = false;
        let (_seqno, _applied, _maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.merge_res, |_, state| {
                let ret = state.apply_merge_res(&res);
                if let Continue(applied) = ret {
                    applied_ever_true = applied_ever_true || applied;
                }
                ret
            })
            .await;
        applied_ever_true
    }

    pub async fn downgrade_since(
        &mut self,
        reader_id: &ReaderId,
        outstanding_seqno: Option<SeqNo>,
        new_since: &Antichain<T>,
        heartbeat_timestamp_ms: u64,
    ) -> (SeqNo, Since<T>, RoutineMaintenance) {
        let metrics = Arc::clone(&self.metrics);
        self.apply_unbatched_idempotent_cmd(&metrics.cmds.downgrade_since, |seqno, state| {
            state.downgrade_since(
                reader_id,
                seqno,
                outstanding_seqno,
                new_since,
                heartbeat_timestamp_ms,
            )
        })
        .await
    }

    pub async fn heartbeat_reader(
        &mut self,
        reader_id: &ReaderId,
        heartbeat_timestamp_ms: u64,
    ) -> (SeqNo, RoutineMaintenance) {
        let metrics = Arc::clone(&self.metrics);
        let (seqno, _existed, maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.heartbeat_reader, |_, state| {
                state.heartbeat_reader(reader_id, heartbeat_timestamp_ms)
            })
            .await;
        (seqno, maintenance)
    }

    pub async fn heartbeat_writer(
        &mut self,
        writer_id: &WriterId,
        heartbeat_timestamp_ms: u64,
    ) -> (SeqNo, RoutineMaintenance) {
        let metrics = Arc::clone(&self.metrics);
        let (seqno, _existed, maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.heartbeat_writer, |_, state| {
                state.heartbeat_writer(writer_id, heartbeat_timestamp_ms)
            })
            .await;
        (seqno, maintenance)
    }

    pub async fn expire_reader(&mut self, reader_id: &ReaderId) -> SeqNo {
        let metrics = Arc::clone(&self.metrics);
        let (seqno, _existed, _maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.expire_reader, |_, state| {
                state.expire_reader(reader_id)
            })
            .await;
        seqno
    }

    pub async fn expire_writer(&mut self, writer_id: &WriterId) -> SeqNo {
        let metrics = Arc::clone(&self.metrics);
        let (seqno, _existed, _maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.expire_writer, |_, state| {
                state.expire_writer(writer_id)
            })
            .await;
        seqno
    }

    pub async fn snapshot(
        &mut self,
        as_of: &Antichain<T>,
    ) -> Result<Vec<HollowBatch<T>>, Since<T>> {
        let mut retry: Option<MetricsRetryStream> = None;
        loop {
            let upper = match self.state.snapshot(as_of) {
                Ok(Ok(x)) => return Ok(x),
                Ok(Err(Upper(upper))) => {
                    // The upper isn't ready yet, fall through and try again.
                    upper
                }
                Err(Since(since)) => return Err(Since(since)),
            };
            // Only sleep after the first fetch, because the first time through
            // maybe our state was just out of date.
            retry = Some(match retry.take() {
                None => self
                    .metrics
                    .retries
                    .snapshot
                    .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream()),
                Some(retry) => {
                    // Use a duration based threshold here instead of the usual
                    // INFO_MIN_ATTEMPTS because here we're waiting on an
                    // external thing to arrive.
                    if retry.next_sleep() >= Duration::from_millis(64) {
                        info!(
                            "snapshot {} as of {:?} not yet available for upper {:?} retrying in {:?}",
                            self.shard_id(),
                            as_of,
                            upper,
                            retry.next_sleep()
                        );
                    } else {
                        debug!(
                            "snapshot {} as of {:?} not yet available for upper {:?} retrying in {:?}",
                            self.shard_id(),
                            as_of,
                            upper,
                            retry.next_sleep()
                        );
                    }
                    retry.sleep().await
                }
            });
            self.fetch_and_update_state().await;
        }
    }

    // NB: Unlike the other methods here, this one is read-only.
    pub async fn verify_listen(&self, as_of: &Antichain<T>) -> Result<(), Since<T>> {
        match self.state.verify_listen(as_of) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(Upper(_))) => {
                // The upper may not be ready yet (maybe it would be ready if we
                // re-fetched state), but that's okay! One way to think of
                // Listen is as an async stream where creating the stream at any
                // legal as_of does not block but then updates trickle in once
                // they are available.
                Ok(())
            }
            Err(Since(since)) => return Err(Since(since)),
        }
    }

    pub async fn next_listen_batch(&mut self, frontier: &Antichain<T>) -> HollowBatch<T> {
        let mut retry: Option<MetricsRetryStream> = None;
        loop {
            if let Some(b) = self.state.next_listen_batch(frontier) {
                return b;
            }
            // Only sleep after the first fetch, because the first time through
            // maybe our state was just out of date.
            retry = Some(match retry.take() {
                None => self
                    .metrics
                    .retries
                    .next_listen_batch
                    .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream()),
                Some(retry) => {
                    // Wait a bit and try again. Intentionally don't ever log
                    // this at info level.
                    //
                    // TODO: See if we can watch for changes in Consensus to be
                    // more reactive here.
                    debug!(
                        "next_listen_batch didn't find new data, retrying in {:?}",
                        retry.next_sleep()
                    );
                    retry.sleep().instrument(trace_span!("listen::sleep")).await
                }
            });
            self.fetch_and_update_state().await;
        }
    }

    async fn apply_unbatched_idempotent_cmd<
        R,
        WorkFn: FnMut(SeqNo, &mut StateCollections<T>) -> ControlFlow<Infallible, R>,
    >(
        &mut self,
        cmd: &CmdMetrics,
        mut work_fn: WorkFn,
    ) -> (SeqNo, R, RoutineMaintenance) {
        let mut retry = self
            .metrics
            .retries
            .idempotent_cmd
            .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
        loop {
            match self.apply_unbatched_cmd(cmd, &mut work_fn).await {
                Ok((seqno, x, maintenance)) => match x {
                    Ok(x) => return (seqno, x, maintenance),
                    Err(infallible) => match infallible {},
                },
                Err(err) => {
                    if retry.attempt() >= INFO_MIN_ATTEMPTS {
                        info!("apply_unbatched_idempotent_cmd {} received an indeterminate error, retrying in {:?}: {}", cmd.name, retry.next_sleep(), err);
                    } else {
                        debug!("apply_unbatched_idempotent_cmd {} received an indeterminate error, retrying in {:?}: {}", cmd.name, retry.next_sleep(), err);
                    }
                    retry = retry.sleep().await;
                    continue;
                }
            }
        }
    }

    async fn apply_unbatched_cmd<
        R,
        E,
        WorkFn: FnMut(SeqNo, &mut StateCollections<T>) -> ControlFlow<E, R>,
    >(
        &mut self,
        cmd: &CmdMetrics,
        mut work_fn: WorkFn,
    ) -> Result<(SeqNo, Result<R, E>, RoutineMaintenance), Indeterminate> {
        let is_write = cmd.name == self.metrics.cmds.compare_and_append.name;
        cmd.run_cmd(|cas_mismatch_metric| async move {
            let mut garbage_collection;

            loop {
                let (work_ret, mut new_state) = match self
                    .state
                    .clone_apply(&self.cfg.build_version, &mut work_fn)
                {
                    Continue(x) => x,
                    Break(err) => {
                        return Ok((self.state.seqno(), Err(err), RoutineMaintenance::default()))
                    }
                };

                let diff = StateDiff::from_diff(&self.state, &new_state);
                // Sanity check that our diff logic roundtrips and adds back up
                // correctly.
                //
                // TODO: Re-enable this #14490.
                #[cfg(all(TODO, any(test, debug_assertions)))]
                {
                    if let Err(err) =
                        StateDiff::validate_roundtrip(&self.metrics, &self.state, &diff, &new_state)
                    {
                        panic!("validate_roundtrips failed: {}", err);
                    }
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
                garbage_collection =
                    new_state
                        .maybe_gc(is_write)
                        .map(|(old_seqno_since, new_seqno_since)| GcReq {
                            shard_id: self.shard_id(),
                            old_seqno_since,
                            new_seqno_since,
                        });

                // SUBTLE! Unlike the other consensus and blob uses, we can't
                // automatically retry indeterminate ExternalErrors here. However,
                // if the state change itself is _idempotent_, then we're free to
                // retry even indeterminate errors. See
                // [Self::apply_unbatched_idempotent_cmd].
                let expected = self.state.seqno();
                let cas_res = self
                    .state_versions
                    .try_compare_and_set_current(
                        &cmd.name,
                        &self.shard_metrics,
                        Some(expected),
                        &new_state,
                        &diff,
                    )
                    .await?;
                match cas_res {
                    Ok(()) => {
                        assert!(
                            self.state.seqno <= new_state.seqno,
                            "state seqno regressed: {} vs {}",
                            self.state.seqno,
                            new_state.seqno
                        );
                        self.state = new_state;

                        let (expired_readers, expired_writers) =
                            self.state.handles_needing_expiration((self.cfg.now)());
                        self.metrics
                            .lease
                            .timeout_read
                            .inc_by(u64::cast_from(expired_readers.len()));
                        let lease_expiration = Some(LeaseExpiration {
                            readers: expired_readers,
                            writers: expired_writers,
                        });

                        let maintenance = RoutineMaintenance {
                            garbage_collection,
                            lease_expiration,
                            write_rollup: self.state.need_rollup(),
                        };

                        return Ok((self.state.seqno(), Ok(work_ret), maintenance));
                    }
                    Err(diffs_to_current) => {
                        cas_mismatch_metric.0.inc();

                        let seqno_before = self.state.seqno;
                        let diffs_apply = diffs_to_current
                            .first()
                            .map_or(true, |x| x.seqno == seqno_before.next());
                        if diffs_apply {
                            self.metrics.state.update_state_fast_path.inc();
                            self.state.apply_encoded_diffs(
                                &self.cfg,
                                &self.metrics,
                                &diffs_to_current,
                            )
                        } else {
                            // Otherwise, we've gc'd the diffs we'd need to
                            // advance self.state to where diffs_to_current
                            // starts so we need a new rollup.
                            self.metrics.state.update_state_slow_path.inc();
                            debug!(
                                "update_state didn't hit update_state fast path {} {:?}",
                                self.state.seqno,
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
                            self.state = self
                                .state_versions
                                .fetch_current_state(&self.state.shard_id, all_live_diffs)
                                .await
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
        let seqno_before = self.state.seqno;
        self.state_versions
            .fetch_and_update_to_current(&mut self.state)
            .await
            .expect("shard codecs should not change");
        assert!(
            seqno_before <= self.state.seqno,
            "state seqno regressed: {} vs {}",
            seqno_before,
            self.state.seqno
        );
    }
}

pub const INFO_MIN_ATTEMPTS: usize = 3;

pub async fn retry_external<R, F, WorkFn>(metrics: &RetryMetrics, mut work_fn: WorkFn) -> R
where
    F: std::future::Future<Output = Result<R, ExternalError>>,
    WorkFn: FnMut() -> F,
{
    let mut retry = metrics.stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
    loop {
        match work_fn().await {
            Ok(x) => {
                if retry.attempt() > 0 {
                    debug!(
                        "external operation {} succeeded after failing at least once",
                        metrics.name,
                    );
                }
                return x;
            }
            Err(err) => {
                if retry.attempt() >= INFO_MIN_ATTEMPTS {
                    info!(
                        "external operation {} failed, retrying in {:?}: {:#}",
                        metrics.name,
                        retry.next_sleep(),
                        err
                    );
                } else {
                    debug!(
                        "external operation {} failed, retrying in {:?}: {:#}",
                        metrics.name,
                        retry.next_sleep(),
                        err
                    );
                }
                retry = retry.sleep().await;
            }
        }
    }
}

pub async fn retry_determinate<R, F, WorkFn>(
    metrics: &RetryMetrics,
    mut work_fn: WorkFn,
) -> Result<R, Indeterminate>
where
    F: std::future::Future<Output = Result<R, ExternalError>>,
    WorkFn: FnMut() -> F,
{
    let mut retry = metrics.stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
    loop {
        match work_fn().await {
            Ok(x) => {
                if retry.attempt() > 0 {
                    debug!(
                        "external operation {} succeeded after failing at least once",
                        metrics.name,
                    );
                }
                return Ok(x);
            }
            Err(ExternalError::Determinate(err)) => {
                // The determinate "could not serialize access" errors
                // happen often enough in dev (which uses Postgres) that
                // it's impeding people's work. At the same time, it's been
                // a source of confusion for eng. The situation is much
                // better on CRDB and we have metrics coverage in prod, so
                // this is redundant enough that it's more hurtful than
                // helpful. As a result, this intentionally ignores
                // INFO_MIN_ATTEMPTS and always logs at debug.
                debug!(
                    "external operation {} failed, retrying in {:?}: {:#}",
                    metrics.name,
                    retry.next_sleep(),
                    err
                );
                retry = retry.sleep().await;
                continue;
            }
            Err(ExternalError::Indeterminate(x)) => return Err(x),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use differential_dataflow::trace::Description;
    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::cast::CastFrom;
    use mz_ore::now::SYSTEM_TIME;
    use tokio::sync::Mutex;

    use crate::async_runtime::CpuHeavyRuntime;
    use crate::batch::{validate_truncate_batch, BatchBuilder};
    use crate::fetch::fetch_batch_part;
    use crate::internal::compact::{CompactReq, Compactor};
    use crate::read::{Listen, ListenEvent};
    use crate::tests::new_test_client;
    use crate::{GarbageCollector, PersistConfig, ShardId};

    use super::*;

    #[derive(Debug, Default)]
    struct DatadrivenState {
        batches: HashMap<String, HollowBatch<u64>>,
        listens: HashMap<String, Listen<String, (), u64, i64>>,
    }

    #[tokio::test]
    async fn machine_datadriven() {
        fn get_arg<'a>(args: &'a HashMap<String, Vec<String>>, name: &str) -> Option<&'a str> {
            args.get(name).map(|vals| {
                if vals.len() != 1 {
                    panic!("unexpected values for {}: {:?}", name, vals);
                }
                vals[0].as_ref()
            })
        }
        fn get_u64<'a>(args: &'a HashMap<String, Vec<String>>, name: &str) -> Option<u64> {
            get_arg(args, name).map(|x| {
                x.parse::<u64>()
                    .unwrap_or_else(|_| panic!("invalid {}: {}", name, x))
            })
        }

        datadriven::walk_async("tests/machine", |mut f| async {
            let shard_id = ShardId::new();
            let mut client = new_test_client().await;
            // Reset blob_target_size. Individual batch writes and compactions
            // can override it with an arg.
            client.cfg.blob_target_size =
                PersistConfig::new(&DUMMY_BUILD_INFO, client.cfg.now.clone()).blob_target_size;

            let state = Arc::new(Mutex::new(DatadrivenState::default()));
            let cpu_heavy_runtime = Arc::new(CpuHeavyRuntime::new());
            let write = Arc::new(Mutex::new(
                client
                    .open_writer::<String, (), u64, i64>(shard_id)
                    .await
                    .expect("invalid shard types"),
            ));
            let now = SYSTEM_TIME.clone();

            f.run_async(move |tc| {
                let shard_id = shard_id.clone();
                let client = client.clone();
                let state = Arc::clone(&state);
                let cpu_heavy_runtime = Arc::clone(&cpu_heavy_runtime);
                let write = Arc::clone(&write);
                let now = now.clone();
                async move {
                    let mut state = state.lock().await;

                    match tc.directive.as_str() {
                        // Scans consensus and returns all states with their SeqNos
                        // and which batches they reference
                        "consensus-scan" => {
                            let from =
                                SeqNo(get_u64(&tc.args, "from_seqno").expect("missing from_seqno"));

                            let mut s = String::new();

                            let mut states = write
                                .lock()
                                .await
                                .machine
                                .state_versions
                                .fetch_live_states::<String, (), u64, i64>(&shard_id)
                                .await
                                .expect("shard codecs should not change");
                            while let Some(x) = states.next() {
                                if x.seqno < from {
                                    continue;
                                }
                                let mut batches = vec![];
                                x.collections.trace.map_batches(|b| {
                                    for (batch_name, original_batch) in &state.batches {
                                        if original_batch.parts == b.parts {
                                            batches.push(batch_name.to_owned());
                                            break;
                                        }
                                    }
                                });
                                write!(s, "seqno={} batches={}\n", x.seqno, batches.join(","));
                            }
                            s
                        }
                        "write-batch" => {
                            let output = get_arg(&tc.args, "output").expect("missing output");
                            let lower = get_u64(&tc.args, "lower").expect("missing lower");
                            let upper = get_u64(&tc.args, "upper").expect("missing upper");
                            let target_size = get_arg(&tc.args, "target_size")
                                .map(|x| x.parse::<usize>().expect("invalid target_size"));

                            let updates = tc
                                .input
                                .trim()
                                .split('\n')
                                .filter(|x| !x.is_empty())
                                .map(|x| {
                                    let parts = x.split(' ').collect::<Vec<_>>();
                                    if parts.len() != 3 {
                                        panic!("unexpected update: {}", x);
                                    }
                                    let (key, ts, diff) = (parts[0], parts[1], parts[2]);
                                    let ts = ts.parse::<u64>().expect("invalid ts");
                                    let diff = diff.parse::<i64>().expect("invalid diff");
                                    (key.to_owned(), ts, diff)
                                })
                                .collect::<Vec<_>>();

                            let mut cfg = client.cfg.clone();
                            if let Some(target_size) = target_size {
                                cfg.blob_target_size = target_size;
                            };
                            let mut builder = BatchBuilder::new(
                                cfg,
                                Arc::clone(&client.metrics),
                                0,
                                Antichain::from_elem(lower),
                                Arc::clone(&client.blob),
                                Arc::clone(&cpu_heavy_runtime),
                                shard_id.clone(),
                                WriterId::new(),
                            );
                            for (k, t, d) in updates {
                                builder.add(&k, &(), &t, &d).await.expect("invalid batch");
                            }
                            let batch = builder
                                .finish(Antichain::from_elem(upper))
                                .await
                                .expect("invalid batch")
                                .into_hollow_batch();
                            state.batches.insert(output.to_owned(), batch.clone());
                            format!("parts={} len={}\n", batch.parts.len(), batch.len)
                        }
                        "fetch-batch" => {
                            let input = get_arg(&tc.args, "input").expect("missing input");
                            let batch = state.batches.get(input).expect("unknown batch").clone();

                            let mut s = String::new();
                            for (idx, part) in batch.parts.iter().enumerate() {
                                write!(s, "<part {idx}>\n");
                                let blob_batch =
                                    client.blob.get(&part.key.complete(&shard_id)).await;
                                match blob_batch {
                                    Ok(Some(_)) | Err(_) => {}
                                    // don't try to fetch/print the keys of the batch part
                                    // if the blob store no longer has it
                                    Ok(None) => {
                                        s.push_str("<empty>\n");
                                        continue;
                                    }
                                };
                                fetch_batch_part(
                                    &shard_id,
                                    client.blob.as_ref(),
                                    client.metrics.as_ref(),
                                    &part.key,
                                    &batch.desc,
                                    |k, _v, t, d| {
                                        let (k, d) = (String::decode(k).unwrap(), i64::decode(d));
                                        write!(s, "{k} {t} {d}\n");
                                    },
                                )
                                .await
                                .expect("invalid batch part");
                            }
                            if s.is_empty() {
                                s.push_str("<empty>\n");
                            }
                            s
                        }
                        "truncate-batch-desc" => {
                            let input = get_arg(&tc.args, "input").expect("missing input");
                            let output = get_arg(&tc.args, "output").expect("missing output");
                            let lower = get_u64(&tc.args, "lower").expect("missing lower");
                            let upper = get_u64(&tc.args, "upper").expect("missing upper");

                            let mut batch =
                                state.batches.get(input).expect("unknown batch").clone();
                            let truncated_desc = Description::new(
                                Antichain::from_elem(lower),
                                Antichain::from_elem(upper),
                                batch.desc.since().clone(),
                            );
                            match validate_truncate_batch(&batch.desc, &truncated_desc) {
                                Ok(()) => {
                                    batch.desc = truncated_desc;
                                    state.batches.insert(output.to_owned(), batch.clone());
                                    format!("parts={} len={}\n", batch.parts.len(), batch.len)
                                }
                                Err(err) => format!("error: {}\n", err),
                            }
                        }
                        "compact" => {
                            let output = get_arg(&tc.args, "output").expect("missing output");
                            let lower = get_u64(&tc.args, "lower").expect("missing lower");
                            let upper = get_u64(&tc.args, "upper").expect("missing upper");
                            let since = get_u64(&tc.args, "since").expect("missing since");
                            let target_size = get_arg(&tc.args, "target_size")
                                .map(|x| x.parse::<usize>().expect("invalid target_size"));

                            let mut inputs = Vec::new();
                            for input in tc.args.get("inputs").expect("missing inputs") {
                                inputs
                                    .push(state.batches.get(input).expect("unknown batch").clone());
                            }

                            let mut cfg = client.cfg.clone();
                            if let Some(target_size) = target_size {
                                cfg.blob_target_size = target_size;
                            };
                            let req = CompactReq {
                                shard_id,
                                desc: Description::new(
                                    Antichain::from_elem(lower),
                                    Antichain::from_elem(upper),
                                    Antichain::from_elem(since),
                                ),
                                inputs,
                            };
                            let res = Compactor::compact::<u64, i64>(
                                cfg,
                                Arc::clone(&client.blob),
                                Arc::clone(&client.metrics),
                                Arc::clone(&cpu_heavy_runtime),
                                req,
                                WriterId::new(),
                            )
                            .await;
                            match res {
                                Ok(res) => {
                                    state.batches.insert(output.to_owned(), res.output.clone());
                                    format!(
                                        "parts={} len={}\n",
                                        res.output.parts.len(),
                                        res.output.len
                                    )
                                }
                                Err(err) => format!("error: {}\n", err),
                            }
                        }
                        "gc" => {
                            let old_seqno_since =
                                SeqNo(get_u64(&tc.args, "from_seqno").expect("missing from_seqno"));
                            let new_seqno_since =
                                SeqNo(get_u64(&tc.args, "to_seqno").expect("missing to_seqno"));

                            GarbageCollector::gc_and_truncate(
                                &mut write.lock().await.machine,
                                GcReq {
                                    shard_id,
                                    old_seqno_since,
                                    new_seqno_since,
                                },
                            )
                            .await;

                            "ok\n".into()
                        }
                        "register-listen" => {
                            let output = get_arg(&tc.args, "output").expect("missing output");
                            let as_of = get_u64(&tc.args, "as-of").expect("missing as-of");
                            let read = client
                                .open_reader::<String, (), u64, i64>(shard_id)
                                .await
                                .expect("invalid shard types");
                            let listen = read.expect_listen(as_of).await;
                            state.listens.insert(output.to_owned(), listen);
                            "ok\n".into()
                        }
                        "listen-through" => {
                            let input = get_arg(&tc.args, "input").expect("missing input");
                            let frontier = get_u64(&tc.args, "frontier").expect("missing frontier");
                            let listen = state.listens.get_mut(input).expect("unknown listener");
                            let mut s = String::new();
                            'outer: loop {
                                for event in listen.next().await {
                                    match event {
                                        ListenEvent::Updates(x) => {
                                            for ((k, _v), t, d) in x.iter() {
                                                write!(s, "{} {} {}\n", k.as_ref().unwrap(), t, d);
                                            }
                                        }
                                        ListenEvent::Progress(x) => {
                                            if !x.less_than(&frontier) {
                                                break 'outer;
                                            }
                                        }
                                    }
                                }
                            }
                            if s.is_empty() {
                                s.push_str("<empty>\n");
                            }
                            s
                        }
                        "compare-and-append" => {
                            let input = get_arg(&tc.args, "input").expect("missing input");
                            let batch = state.batches.get(input).expect("unknown batch");
                            let mut write = write.lock().await;
                            let writer_id = write.writer_id.clone();
                            let (_, _) = write
                                .machine
                                .compare_and_append(batch, &writer_id, now())
                                .await
                                .expect("indeterminate")
                                .expect("invalid usage")
                                .expect("upper mismatch");
                            format!("ok\n")
                        }
                        "apply-merge-res" => {
                            let input = get_arg(&tc.args, "input").expect("missing input");
                            let batch = state.batches.get(input).expect("unknown batch");
                            let mut write = write.lock().await;
                            let applied = write
                                .machine
                                .merge_res(&FueledMergeRes {
                                    output: batch.clone(),
                                })
                                .await;
                            format!("{}\n", applied)
                        }
                        _ => panic!("unknown directive {:?}", tc),
                    }
                }
            })
            .await;
            f
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn apply_unbatched_cmd_truncate() {
        mz_ore::test::init_logging();

        let (mut write, _) = new_test_client()
            .await
            .expect_open::<String, (), u64, i64>(ShardId::new())
            .await;

        // Write a bunch of batches. This should result in a bounded number of
        // live entries in consensus.
        const NUM_BATCHES: u64 = 100;
        for idx in 0..NUM_BATCHES {
            let batch = write
                .expect_batch(&[((idx.to_string(), ()), idx, 1)], idx, idx + 1)
                .await;
            let (_, writer_maintenance) = write
                .machine
                .compare_and_append(
                    &batch.into_hollow_batch(),
                    &write.writer_id,
                    (write.cfg.now)(),
                )
                .await
                .expect("external durability failed")
                .expect("invalid usage")
                .expect("unexpected upper");
            writer_maintenance
                .perform(&write.machine, &write.gc, write.compact.as_ref())
                .await;
        }
        let live_diffs = write
            .machine
            .state_versions
            .fetch_live_diffs(&write.machine.shard_id())
            .await;
        // Make sure we constructed the key correctly.
        assert!(live_diffs.len() > 0);
        // Make sure the number of entries is bounded. (I think we could work
        // out a tighter bound than this, but the point is only that it's
        // bounded).
        let max_live_diffs = 2 * usize::cast_from(NUM_BATCHES.next_power_of_two().trailing_zeros());
        assert!(
            live_diffs.len() < max_live_diffs,
            "{} vs {}",
            live_diffs.len(),
            max_live_diffs
        );
    }
}
