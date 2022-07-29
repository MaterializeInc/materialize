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
use timely::progress::{Antichain, Timestamp};
use tracing::{debug, debug_span, info, trace, trace_span, Instrument};

#[allow(unused_imports)] // False positive.
use mz_ore::fmt::FormatBuffer;
use mz_persist::location::{Consensus, ExternalError, Indeterminate, SeqNo, VersionedData};
use mz_persist::retry::Retry;
use mz_persist_types::{Codec, Codec64};

use crate::error::{CodecMismatch, InvalidUsage};
use crate::r#impl::gc::{GarbageCollector, GcReq};
use crate::r#impl::metrics::{
    CmdMetrics, Metrics, MetricsRetryStream, RetriesMetrics, RetryMetrics,
};
use crate::r#impl::state::{
    HollowBatch, ReaderState, Since, State, StateCollections, Upper, WriterState,
};
use crate::r#impl::trace::{FueledMergeReq, FueledMergeRes};
use crate::read::ReaderId;
use crate::write::WriterId;
use crate::{PersistConfig, ShardId};

#[derive(Debug)]
pub struct Machine<K, V, T, D> {
    // TODO: Remove cfg after we remove the read lease expiry hack.
    cfg: PersistConfig,
    consensus: Arc<dyn Consensus + Send + Sync>,
    metrics: Arc<Metrics>,
    gc: GarbageCollector,

    state: State<K, V, T, D>,
}

// Impl Clone regardless of the type params.
impl<K, V, T: Clone, D> Clone for Machine<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            consensus: Arc::clone(&self.consensus),
            metrics: Arc::clone(&self.metrics),
            state: self.state.clone(),
            gc: self.gc.clone(),
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
        gc: GarbageCollector,
    ) -> Result<Self, CodecMismatch> {
        let state = metrics
            .cmds
            .init_state
            .run_cmd(|_cas_mismatch_metric| {
                // No cas_mismatch retries because we just use the returned
                // state on a mismatch.
                Self::maybe_init_state(consensus.as_ref(), &metrics.retries, shard_id)
            })
            .await?;
        Ok(Machine {
            cfg,
            consensus,
            metrics,
            state,
            gc,
        })
    }

    pub fn shard_id(&self) -> ShardId {
        self.state.shard_id()
    }

    pub async fn fetch_upper(&mut self) -> Antichain<T> {
        self.fetch_and_update_state().await;
        self.state.upper()
    }

    pub fn upper(&self) -> Antichain<T> {
        self.state.upper()
    }

    pub async fn register_reader(
        &mut self,
        reader_id: &ReaderId,
        heartbeat_timestamp_ms: u64,
    ) -> (Upper<T>, ReaderState<T>) {
        let metrics = Arc::clone(&self.metrics);
        let (seqno, (shard_upper, read_cap)) = self
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
        let (_seqno, (shard_upper, writer_state)) = self
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
        let (seqno, read_cap) = self
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
        Result<Result<(SeqNo, Vec<FueledMergeReq<T>>), Upper<T>>, InvalidUsage<T>>,
        Indeterminate,
    > {
        let metrics = Arc::clone(&self.metrics);
        loop {
            let (seqno, res) = self
                .apply_unbatched_cmd(&metrics.cmds.compare_and_append, |_, state| {
                    state.compare_and_append(batch, writer_id)
                })
                .await?;
            // TODO: This is not a great place to hook this in.
            self.maybe_expire_readers();

            match res {
                Ok(merge_reqs) => {
                    return Ok(Ok(Ok((seqno, merge_reqs))));
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
                    if &current_upper != batch.desc.lower() {
                        return Ok(Ok(Err(Upper(current_upper))));
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
        let (_seqno, applied) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.merge_res, |_, state| {
                state.apply_merge_res(&res)
            })
            .await;
        applied
    }

    pub async fn downgrade_since(
        &mut self,
        reader_id: &ReaderId,
        new_since: &Antichain<T>,
        heartbeat_timestamp_ms: u64,
    ) -> (SeqNo, Since<T>) {
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
    ) -> SeqNo {
        let metrics = Arc::clone(&self.metrics);
        let (seqno, _existed) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.heartbeat_reader, |_, state| {
                state.heartbeat_reader(reader_id, heartbeat_timestamp_ms)
            })
            .await;
        seqno
    }

    pub async fn expire_reader(&mut self, reader_id: &ReaderId) -> SeqNo {
        let metrics = Arc::clone(&self.metrics);
        let (seqno, _existed) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.expire_reader, |_, state| {
                state.expire_reader(reader_id)
            })
            .await;
        seqno
    }

    pub async fn expire_writer(&mut self, writer_id: &WriterId) -> SeqNo {
        let metrics = Arc::clone(&self.metrics);
        let (seqno, _existed) = self
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
    ) -> (SeqNo, R) {
        let mut retry = self
            .metrics
            .retries
            .idempotent_cmd
            .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
        loop {
            match self.apply_unbatched_cmd(cmd, &mut work_fn).await {
                Ok((seqno, x)) => match x {
                    Ok(x) => {
                        // TODO: This is not a great place to hook this in.
                        self.maybe_expire_readers();
                        return (seqno, x);
                    }
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
    ) -> Result<(SeqNo, Result<R, E>), Indeterminate> {
        cmd.run_cmd(|cas_mismatch_metric| async move {
            let path = self.shard_id().to_string();

            loop {
                let (work_ret, new_state) = match self.state.clone_apply(&mut work_fn) {
                    Continue(x) => x,
                    Break(err) => return Ok((self.state.seqno(), Err(err))),
                };
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

                        // If this command has downgraded the seqno_since, fire
                        // off a background request to the GarbageCollector so
                        // it can delete eligible blobs and truncate the state
                        // history.
                        let old_seqno_since = self.state.seqno_since();
                        let new_seqno_since = new_state.seqno_since();
                        if old_seqno_since != new_seqno_since {
                            self.gc.gc_and_truncate_background(GcReq {
                                shard_id: self.shard_id(),
                                old_seqno_since,
                                new_seqno_since,
                            });
                        }

                        self.state = new_state;
                        return Ok((self.state.seqno(), Ok(work_ret)));
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

    // HACK: Temporary impl of read lease expiry because otherwise gc can't work
    // after a restart (the unexpired reader that is never coming back holds
    // back the seqno cap forever) and we leak data like crazy.
    fn maybe_expire_readers(&self) {
        let readers_needing_expiration = self.state.readers_needing_expiration((self.cfg.now)());
        for reader_needing_expiration in readers_needing_expiration {
            self.metrics.lease.timeout_read.inc();
            let mut machine = self.clone();
            let _ = mz_ore::task::spawn(|| "persist::automatic_read_expiration", async move {
                machine.expire_reader(&reader_needing_expiration).await
            });
        }
    }

    async fn maybe_init_state(
        consensus: &(dyn Consensus + Send + Sync),
        retry_metrics: &RetriesMetrics,
        shard_id: ShardId,
    ) -> Result<State<K, V, T, D>, CodecMismatch> {
        debug!("Machine::maybe_init_state shard_id={}", shard_id);

        let path = shard_id.to_string();
        let mut current = retry_external(&retry_metrics.external.maybe_init_state_head, || async {
            consensus.head(&path).await
        })
        .await;

        loop {
            // First, check if the shard has already been initialized.
            if let Some(current) = current.as_ref() {
                let current_state = match State::decode(&current.data) {
                    Ok(x) => x,
                    Err(err) => return Err(err),
                };
                debug_assert_eq!(current.seqno, current_state.seqno());
                return Ok(current_state);
            }

            // It hasn't been initialized, try initializing it.
            let state = State::new(shard_id);
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
            .decode(|| State::decode(&current.data))
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
    use mz_ore::cast::CastFrom;
    use tokio::sync::Mutex;

    use crate::async_runtime::CpuHeavyRuntime;
    use crate::batch::{validate_truncate_batch, BatchBuilder};
    use crate::r#impl::compact::{CompactReq, Compactor};
    use crate::read::{fetch_batch_part, Listen, ListenEvent};
    use crate::tests::new_test_client;
    use crate::{PersistConfig, ShardId};

    use super::*;

    #[derive(Debug, Default)]
    struct DatadrivenState {
        batches: HashMap<String, HollowBatch<u64>>,
        listens: HashMap<String, Listen<String, (), u64, i64>>,
        merge_reqs: Vec<FueledMergeReq<u64>>,
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
                PersistConfig::new(client.cfg.now.clone()).blob_target_size;

            let state = Arc::new(Mutex::new(DatadrivenState::default()));
            let cpu_heavy_runtime = Arc::new(CpuHeavyRuntime::new());

            f.run_async(move |tc| {
                let shard_id = shard_id.clone();
                let client = client.clone();
                let state = Arc::clone(&state);
                let cpu_heavy_runtime = Arc::clone(&cpu_heavy_runtime);
                async move {
                    let mut state = state.lock().await;
                    match tc.directive.as_str() {
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
                            let mut write = client
                                .open_writer::<String, (), u64, i64>(shard_id)
                                .await
                                .expect("invalid shard types");
                            let (_, mut merge_reqs) = write
                                .machine
                                .compare_and_append(batch, &write.writer_id)
                                .await
                                .expect("indeterminate")
                                .expect("invalid usage")
                                .expect("upper mismatch");
                            state.merge_reqs.append(&mut merge_reqs);
                            format!("ok\n")
                        }
                        "apply-merge-res" => {
                            let input = get_arg(&tc.args, "input").expect("missing input");
                            let batch = state.batches.get(input).expect("unknown batch");
                            let mut write = client
                                .open_writer::<String, (), u64, i64>(shard_id)
                                .await
                                .expect("invalid shard types");
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
            write
                .expect_compare_and_append(&[((idx.to_string(), ()), idx, 1)], idx, idx + 1)
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

        // The truncation is done in the background, so wait until the assertion
        // passes, giving up and failing the test if that takes "too much" time.
        let mut retry = Retry::persist_defaults(SystemTime::now()).into_retry_stream();
        loop {
            if consensus_entries.len() <= max_entries {
                break;
            }
            info!(
                "expected at most {} entries got {}",
                max_entries,
                consensus_entries.len()
            );
            if retry.next_sleep() > Duration::from_secs(10) {
                panic!("did not converge in time");
            }
            retry = retry.sleep().await;
        }
    }
}
