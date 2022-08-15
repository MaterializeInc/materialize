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
use tracing::{debug, debug_span, info, trace, trace_span, Instrument};

#[allow(unused_imports)] // False positive.
use mz_ore::fmt::FormatBuffer;
use mz_persist::location::{Consensus, ExternalError, Indeterminate, SeqNo, VersionedData};
use mz_persist::retry::Retry;
use mz_persist_types::{Codec, Codec64};

use crate::error::{CodecMismatch, InvalidUsage};
use crate::r#impl::compact::CompactReq;
use crate::r#impl::gc::GcReq;
use crate::r#impl::maintenance::{LeaseExpiration, RoutineMaintenance, WriterMaintenance};
use crate::r#impl::metrics::{
    CmdMetrics, Metrics, MetricsRetryStream, RetriesMetrics, RetryMetrics, ShardMetrics,
};
use crate::r#impl::state::{
    HollowBatch, ReaderState, Since, State, StateCollections, Upper, WriterState,
};
use crate::r#impl::trace::FueledMergeRes;
use crate::read::ReaderId;
use crate::write::WriterId;
use crate::{PersistConfig, ShardId};

#[derive(Debug)]
pub struct Machine<K, V, T, D> {
    // TODO: Remove cfg after we remove the read lease expiry hack.
    cfg: PersistConfig,
    consensus: Arc<dyn Consensus + Send + Sync>,
    metrics: Arc<Metrics>,
    shard_metrics: Arc<ShardMetrics>,

    state: State<K, V, T, D>,
}

// Impl Clone regardless of the type params.
impl<K, V, T: Clone, D> Clone for Machine<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            consensus: Arc::clone(&self.consensus),
            metrics: Arc::clone(&self.metrics),
            shard_metrics: Arc::clone(&self.shard_metrics),
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
        consensus: Arc<dyn Consensus + Send + Sync>,
        metrics: Arc<Metrics>,
    ) -> Result<Self, CodecMismatch> {
        let shard_metrics = metrics.shards.shard(&shard_id);
        let state = metrics
            .cmds
            .init_state
            .run_cmd(|_cas_mismatch_metric| {
                // No cas_mismatch retries because we just use the returned
                // state on a mismatch.
                Self::maybe_init_state(&cfg, consensus.as_ref(), &metrics.retries, shard_id)
            })
            .await?;
        Ok(Machine {
            cfg,
            consensus,
            metrics,
            shard_metrics,
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
        heartbeat_timestamp_ms: u64,
    ) -> (Upper<T>, WriterState) {
        let metrics = Arc::clone(&self.metrics);
        let (_seqno, (shard_upper, writer_state), _maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.register, |_seqno, state| {
                state.register_writer(writer_id, heartbeat_timestamp_ms)
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
    ) -> Result<
        Result<Result<(SeqNo, WriterMaintenance<T>), Upper<T>>, InvalidUsage<T>>,
        Indeterminate,
    > {
        let metrics = Arc::clone(&self.metrics);
        loop {
            let (seqno, res, routine) = self
                .apply_unbatched_cmd(&metrics.cmds.compare_and_append, |_, state| {
                    state.compare_and_append(batch, writer_id)
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
        new_since: &Antichain<T>,
        heartbeat_timestamp_ms: u64,
    ) -> (SeqNo, Since<T>, RoutineMaintenance) {
        let metrics = Arc::clone(&self.metrics);
        self.apply_unbatched_idempotent_cmd(&metrics.cmds.downgrade_since, |seqno, state| {
            state.downgrade_since(reader_id, seqno, new_since, heartbeat_timestamp_ms)
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
            let path = self.shard_id().to_string();
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

                trace!(
                    "apply_unbatched_cmd {} attempting {}\n  new_state={:?}",
                    cmd.name,
                    self.state.seqno(),
                    new_state
                );
                let new = self
                    .metrics
                    .codecs
                    .state
                    .encode(|| VersionedData::from((new_state.seqno(), &new_state)));

                // SUBTLE! Unlike the other consensus and blob uses, we can't
                // automatically retry indeterminate ExternalErrors here. However,
                // if the state change itself is _idempotent_, then we're free to
                // retry even indeterminate errors. See
                // [Self::apply_unbatched_idempotent_cmd].
                let payload_len = new.data.len();
                let cas_res = retry_determinate(
                    &self.metrics.retries.determinate.apply_unbatched_cmd_cas,
                    || async {
                        self.consensus
                            .compare_and_set(&path, Some(self.state.seqno()), new.clone())
                            .await
                    },
                )
                .instrument(debug_span!("apply_unbatched_cmd::cas", payload_len))
                .await
                .map_err(|err| {
                    debug!("apply_unbatched_cmd {} errored: {}", cmd.name, err);
                    err
                })?;
                match cas_res {
                    Ok(()) => {
                        trace!(
                            "apply_unbatched_cmd {} succeeded {}\n  new_state={:?}",
                            cmd.name,
                            new_state.seqno(),
                            new_state
                        );

                        self.shard_metrics.set_since(self.state.since());
                        self.shard_metrics.set_upper(&self.state.upper());
                        self.shard_metrics.set_encoded_state_size(payload_len);
                        self.shard_metrics.set_batch_count(self.state.batch_count());
                        self.shard_metrics.set_seqnos_held(self.state.seqnos_held());

                        let expired_readers =
                            self.state.readers_needing_expiration((self.cfg.now)());
                        self.metrics
                            .lease
                            .timeout_read
                            .inc_by(u64::cast_from(expired_readers.len()));
                        let lease_expiration = Some(LeaseExpiration {
                            readers: expired_readers,
                            writers: vec![],
                        });

                        let maintenance = RoutineMaintenance {
                            garbage_collection,
                            lease_expiration,
                        };

                        self.state = new_state;
                        return Ok((self.state.seqno(), Ok(work_ret), maintenance));
                    }
                    Err(current) => {
                        debug!(
                            "apply_unbatched_cmd {} {} lost the CaS race, retrying: {} vs {:?}",
                            self.shard_id(),
                            cmd.name,
                            self.state.seqno(),
                            current.as_ref().map(|x| x.seqno)
                        );
                        cas_mismatch_metric.0.inc();
                        self.update_state(current).await;

                        // Intentionally don't backoff here. It would only make
                        // starvation issues even worse.
                        continue;
                    }
                }
            }
        })
        .await
    }

    async fn maybe_init_state(
        cfg: &PersistConfig,
        consensus: &(dyn Consensus + Send + Sync),
        retry_metrics: &RetriesMetrics,
        shard_id: ShardId,
    ) -> Result<State<K, V, T, D>, CodecMismatch> {
        let path = shard_id.to_string();
        let mut current = retry_external(&retry_metrics.external.maybe_init_state_head, || async {
            consensus.head(&path).await
        })
        .await;

        loop {
            // First, check if the shard has already been initialized.
            if let Some(current) = current.as_ref() {
                let current_state = match State::decode(&cfg.build_version, &current.data) {
                    Ok(x) => x,
                    Err(err) => return Err(err),
                };
                debug_assert_eq!(current.seqno, current_state.seqno());
                return Ok(current_state);
            }

            // It hasn't been initialized, try initializing it.
            let state = State::new(cfg.build_version.clone(), shard_id);
            let new = VersionedData::from((state.seqno(), &state));
            trace!(
                "maybe_init_state attempting {}\n  state={:?}",
                new.seqno,
                state
            );
            let cas_res = retry_external(&retry_metrics.external.maybe_init_state_cas, || async {
                consensus.compare_and_set(&path, None, new.clone()).await
            })
            .await;
            match cas_res {
                Ok(()) => {
                    trace!(
                        "maybe_init_state succeeded {}\n  state={:?}",
                        state.seqno(),
                        state
                    );
                    return Ok(state);
                }
                Err(x) => {
                    // We lost a CaS race, use the value included in the CaS
                    // expectation error. Because we used None for expected,
                    // this should never be None.
                    debug!(
                        "maybe_init_state lost the CaS race, using current value: {:?}",
                        x.as_ref().map(|x| x.seqno)
                    );
                    debug_assert!(x.is_some());
                    current = x
                }
            }
        }
    }

    pub async fn fetch_and_update_state(&mut self) {
        let shard_id = self.shard_id();
        let current = retry_external(
            &self.metrics.retries.external.fetch_and_update_state_head,
            || async { self.consensus.head(&shard_id.to_string()).await },
        )
        .instrument(trace_span!("fetch_and_update_state::head"))
        .await;
        self.update_state(current).await;
    }

    async fn update_state(&mut self, current: Option<VersionedData>) {
        let current = match current {
            Some(x) => x,
            None => {
                // Machine is only constructed once, we've successfully
                // retrieved state from durable storage, but now it's gone? In
                // the future, maybe this means the shard was deleted or
                // something, but for now it's entirely unexpected.
                panic!("internal error: missing state {}", self.state.shard_id());
            }
        };
        let current_state = self
            .metrics
            .codecs
            .state
            .decode(|| State::decode(&self.cfg.build_version, &current.data))
            // We received a State with different declared codecs than a
            // previous SeqNo of the same State. Fail loudly.
            .expect("internal error: new durable state disagreed with old durable state");
        debug_assert_eq!(current.seqno, current_state.seqno());
        debug_assert!(self.state.seqno() <= current.seqno);
        self.state = current_state;
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
    use tokio::sync::Mutex;

    use crate::async_runtime::CpuHeavyRuntime;
    use crate::batch::{validate_truncate_batch, BatchBuilder};
    use crate::r#impl::compact::{CompactReq, Compactor};
    use crate::r#impl::paths::PartialBlobKey;
    use crate::r#impl::state::ProtoStateRollup;
    use crate::read::{fetch_batch_part, Listen, ListenEvent};
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

            f.run_async(move |tc| {
                let shard_id = shard_id.clone();
                let client = client.clone();
                let state = Arc::clone(&state);
                let cpu_heavy_runtime = Arc::clone(&cpu_heavy_runtime);
                let write = Arc::clone(&write);

                async move {
                    let mut state = state.lock().await;

                    match tc.directive.as_str() {
                        // Scans consensus and returns all states with their SeqNos
                        // and which batches they reference
                        "consensus-scan" => {
                            let from =
                                SeqNo(get_u64(&tc.args, "from_seqno").expect("missing from_seqno"));
                            let res = client.consensus.scan(&shard_id.to_string(), from).await;

                            let mut s = String::new();
                            let states = match res {
                                Ok(states) => states,
                                Err(err) => {
                                    write!(s, "error: {}\n", err);
                                    return s;
                                }
                            };
                            for persisted_state in states {
                                let persisted_state: ProtoStateRollup =
                                    prost::Message::decode(&*persisted_state.data)
                                        .expect("internal error: invalid encoded state");
                                let mut batches = vec![];
                                if let Some(trace) = persisted_state.trace.as_ref() {
                                    for batch in trace.spine.iter() {
                                        let part_keys: Vec<PartialBlobKey> = batch
                                            .keys
                                            .iter()
                                            .map(|k| PartialBlobKey(k.to_owned()))
                                            .collect();

                                        for (batch_name, original_batch) in &state.batches {
                                            if original_batch.keys == part_keys {
                                                batches.push(batch_name.to_owned());
                                                break;
                                            }
                                        }
                                    }
                                }
                                write!(
                                    s,
                                    "seqno={} batches={}\n",
                                    persisted_state.seqno,
                                    batches.join(",")
                                );
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
                            format!("parts={} len={}\n", batch.keys.len(), batch.len)
                        }
                        "fetch-batch" => {
                            let input = get_arg(&tc.args, "input").expect("missing input");
                            let batch = state.batches.get(input).expect("unknown batch").clone();

                            let mut s = String::new();
                            for (idx, key) in batch.keys.iter().enumerate() {
                                write!(s, "<part {idx}>\n");
                                let blob_batch = client.blob.get(&key.complete(&shard_id)).await;
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
                                    key,
                                    &batch.desc,
                                    |k, _v, t, d| {
                                        let (k, d) = (String::decode(k).unwrap(), i64::decode(d));
                                        write!(s, "{k} {t} {d}\n");
                                    },
                                )
                                .await
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
                                    format!("parts={} len={}\n", batch.keys.len(), batch.len)
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
                                        res.output.keys.len(),
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
                                Arc::clone(&client.consensus),
                                Arc::clone(&client.blob),
                                &client.metrics,
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
                                .compare_and_append(batch, &writer_id)
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
        let consensus = Arc::clone(&write.machine.consensus);

        // Write a bunch of batches. This should result in a bounded number of
        // live entries in consensus.
        const NUM_BATCHES: u64 = 100;
        for idx in 0..NUM_BATCHES {
            let batch = write
                .expect_batch(&[((idx.to_string(), ()), idx, 1)], idx, idx + 1)
                .await;
            let (_, writer_maintenance) = write
                .machine
                .compare_and_append(&batch.into_hollow_batch(), &write.writer_id)
                .await
                .expect("external durability failed")
                .expect("invalid usage")
                .expect("unexpected upper");
            writer_maintenance
                .perform_awaitable(&write.machine, &write.gc, write.compact.as_ref())
                .await;
        }
        let key = write.machine.shard_id().to_string();
        let consensus_entries = consensus
            .scan(&key, SeqNo::minimum())
            .await
            .expect("scan failed");
        // Make sure we constructed the key correctly.
        assert!(consensus_entries.len() > 0);
        // Make sure the number of entries is bounded. (I think we could work
        // out a tighter bound than this, but the point is only that it's
        // bounded).
        let max_entries = 2 * usize::cast_from(NUM_BATCHES.next_power_of_two().trailing_zeros());
        assert!(consensus_entries.len() < max_entries);
    }
}
