// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Timely operators for the crate

use std::fmt::Debug;
use std::future::Future;
use std::sync::mpsc::TryRecvError;
use std::sync::{mpsc, Arc};
use std::time::Duration;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::Hashable;
use mz_ore::cast::CastFrom;
use mz_persist_client::cfg::RetryParameters;
use mz_persist_client::dyn_cfg::{Config, ConfigSet};
use mz_persist_client::operators::shard_source::{shard_source, SnapshotMode};
use mz_persist_client::read::ListenEvent;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
use mz_persist_types::{Codec, Codec64, StepForward};
use mz_timely_util::builder_async::{
    AsyncInputHandle, Disconnected, Event, OperatorBuilder as AsyncOperatorBuilder,
    PressOnDropButton,
};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::EventCore;
use timely::dataflow::operators::{Broadcast, Capture, Leave, Map, Probe};
use timely::dataflow::{ProbeHandle, Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use timely::worker::Worker;
use timely::{Data, PartialOrder, WorkerConfig};
use tracing::{debug, trace};

use crate::txn_cache::{TxnsCache, TxnsCacheState};
use crate::txn_read::DataListenNext;
use crate::{TxnsCodec, TxnsCodecDefault, TxnsEntry};

/// An operator for translating physical data shard frontiers into logical ones.
///
/// A data shard in the txns set logically advances its upper each time a txn is
/// committed, but the upper is not physically advanced unless that data shard
/// was involved in the txn. This means that a shard_source (or any read)
/// pointed at a data shard would appear to stall at the time of the most recent
/// write. We fix this for shard_source by flowing its output through a new
/// `txns_progress` dataflow operator, which ensures that the
/// frontier/capability is advanced as the txns shard progresses, as long as the
/// shard_source is up to date with the latest committed write to that data
/// shard.
///
/// Example:
///
/// - A data shard has most recently been written to at 3.
/// - The txns shard's upper is at 6.
/// - We render a dataflow containing a shard_source with an as_of of 5.
/// - A txn NOT involving the data shard is committed at 7.
/// - A txn involving the data shard is committed at 9.
///
/// How it works:
///
/// - The shard_source operator is rendered. Its single output is hooked up as a
///   _disconnected_ input to txns_progress. The txns_progress single output is
///   a stream of the same type, which is used by downstream operators. This
///   txns_progress operator is targeted at one data_shard; rendering a
///   shard_source for a second data shard requires a second txns_progress
///   operator.
/// - The shard_source operator emits data through 3 and advances the frontier.
/// - The txns_progress operator passes through these writes and frontier
///   advancements unchanged. (Recall that it's always correct to read a data
///   shard "normally", it just might stall.) Because the txns_progress operator
///   knows there are no writes in `[3,5]`, it then downgrades its own
///   capability past 5 (to 6). Because the input is disconnected, this means
///   the overall frontier of the output is downgraded to 6.
/// - The txns_progress operator learns about the write at 7 (the upper is now
///   8). Because it knows that the data shard was not involved in this, it's
///   free to downgrade its capability to 8.
/// - The txns_progress operator learns about the write at 9 (the upper is now
///   10). It knows that the data shard _WAS_ involved in this, so it forwards
///   on data from its input until the input has progressed to 10, at which
///   point it can itself downgrade to 10.
pub fn txns_progress<K, V, T, D, P, C, F, G>(
    passthrough: Stream<G, P>,
    name: &str,
    client_fn: impl Fn() -> F,
    txns_id: ShardId,
    data_id: ShardId,
    as_of: T,
    until: Antichain<T>,
    data_key_schema: Arc<K::Schema>,
    data_val_schema: Arc<V::Schema>,
) -> (Stream<G, P>, Vec<PressOnDropButton>)
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
    D: Data + Semigroup + Codec64 + Send + Sync,
    P: Debug + Data,
    C: TxnsCodec,
    F: Future<Output = PersistClient> + Send + 'static,
    G: Scope<Timestamp = T>,
{
    let unique_id = (name, passthrough.scope().addr()).hashed();
    let (txns, source_button) = txns_progress_source::<K, V, T, D, P, C, G>(
        passthrough.scope(),
        name,
        client_fn(),
        txns_id,
        data_id,
        unique_id,
    );
    // Each of the `txns_frontiers` workers wants the full copy of the txns
    // shard (modulo filtered for data_id).
    let txns = txns.broadcast();
    let (passthrough, frontiers_button) = txns_progress_frontiers::<K, V, T, D, P, C, G>(
        txns,
        passthrough,
        name,
        client_fn(),
        txns_id,
        data_id,
        as_of,
        until,
        data_key_schema,
        data_val_schema,
        unique_id,
    );
    (passthrough, vec![source_button, frontiers_button])
}

fn txns_progress_source<K, V, T, D, P, C, G>(
    scope: G,
    name: &str,
    client: impl Future<Output = PersistClient> + 'static,
    txns_id: ShardId,
    data_id: ShardId,
    unique_id: u64,
) -> (Stream<G, (TxnsEntry, T, i64)>, PressOnDropButton)
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
    D: Data + Semigroup + Codec64 + Send + Sync,
    P: Debug + Data,
    C: TxnsCodec,
    G: Scope<Timestamp = T>,
{
    let worker_idx = scope.index();
    let chosen_worker = usize::cast_from(name.hashed()) % scope.peers();
    let name = format!("txns_progress_source({})", name);
    let mut builder = AsyncOperatorBuilder::new(name.clone(), scope);
    let name = format!("{} [{}]", name, unique_id);
    let (mut txns_output, txns_stream) = builder.new_output();

    let shutdown_button = builder.build(move |capabilities| async move {
        if worker_idx != chosen_worker {
            return;
        }

        let [mut cap]: [_; 1] = capabilities.try_into().expect("one capability per output");
        let client = client.await;
        let mut txns_subscribe = {
            let cache = TxnsCache::<T, C>::open(&client, txns_id, Some(data_id)).await;
            assert!(cache.buf.is_empty());
            cache.txns_subscribe
        };
        loop {
            let events = txns_subscribe.next(None).await;
            for event in events {
                let parts = match event {
                    ListenEvent::Progress(frontier) => {
                        let progress = frontier
                            .into_option()
                            .expect("nothing should close the txns shard");
                        debug!("{} emitting progress {:?}", name, progress);
                        cap.downgrade(&progress);
                        continue;
                    }
                    ListenEvent::Updates(parts) => parts,
                };
                let mut updates = Vec::new();
                TxnsCache::<T, C>::fetch_parts(
                    Some(data_id),
                    &mut txns_subscribe,
                    parts,
                    &mut updates,
                )
                .await;
                if !updates.is_empty() {
                    debug!("{} emitting updates {:?}", name, updates);
                    txns_output.give_container(&cap, &mut updates).await;
                }
            }
        }
    });
    (txns_stream, shutdown_button.press_on_drop())
}

fn txns_progress_frontiers<K, V, T, D, P, C, G>(
    txns: Stream<G, (TxnsEntry, T, i64)>,
    passthrough: Stream<G, P>,
    name: &str,
    client: impl Future<Output = PersistClient> + 'static,
    txns_id: ShardId,
    data_id: ShardId,
    as_of: T,
    until: Antichain<T>,
    data_key_schema: Arc<K::Schema>,
    data_val_schema: Arc<V::Schema>,
    unique_id: u64,
) -> (Stream<G, P>, PressOnDropButton)
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
    D: Data + Semigroup + Codec64 + Send + Sync,
    P: Debug + Data,
    C: TxnsCodec,
    G: Scope<Timestamp = T>,
{
    let name = format!("txns_progress_frontiers({})", name);
    let mut builder = AsyncOperatorBuilder::new(name.clone(), passthrough.scope());
    let name = format!(
        "{} [{}] {}/{}",
        name,
        unique_id,
        passthrough.scope().index(),
        passthrough.scope().peers(),
    );
    let (mut passthrough_output, passthrough_stream) = builder.new_output();
    let txns_input = builder.new_disconnected_input(&txns, Pipeline);
    let mut passthrough_input = builder.new_disconnected_input(&passthrough, Pipeline);

    let shutdown_button = builder.build(move |capabilities| async move {
        let [mut cap]: [_; 1] = capabilities.try_into().expect("one capability per output");
        let client = client.await;
        let state = TxnsCacheState::new(txns_id, T::minimum(), Some(data_id));
        let mut txns_cache = TxnsCacheTimely {
            name: name.clone(),
            state,
            input: txns_input,
            buf: Vec::new(),
        };

        txns_cache.update_gt(&as_of).await;
        let snap = txns_cache.state.data_snapshot(data_id, as_of.clone());
        let data_write = client
            .open_writer::<K, V, T, D>(
                data_id,
                Arc::clone(&data_key_schema),
                Arc::clone(&data_val_schema),
                Diagnostics::from_purpose("data read physical upper"),
            )
            .await
            .expect("schema shouldn't change");
        let empty_to = snap.unblock_read(data_write).await;
        debug!(
            "{} {:.9} starting as_of={:?} empty_to={:?}",
            name,
            data_id.to_string(),
            as_of,
            empty_to.elements()
        );

        // We've ensured that the data shard's physical upper is past as_of, so
        // start by passing through data and frontier updates from the input
        // until it is past the as_of.
        let mut read_data_to = empty_to;
        let mut output_progress_exclusive = T::minimum();
        loop {
            loop {
                // This only returns None when there are no more data left. Turn
                // it into an empty frontier progress so we can re-use the
                // shutdown code below.
                let event = passthrough_input
                    .next()
                    .await
                    .unwrap_or_else(|| Event::Progress(Antichain::new()));
                match event {
                    // NB: Ignore the data_cap because this input is
                    // disconnected.
                    Event::Data(_data_cap, data) => {
                        // NB: Nothing to do here for `until` because the both
                        // `shard_source` (before this operator) and
                        // `mfp_and_decode` (after this operator) do the
                        // necessary filtering.
                        for data in data {
                            debug!(
                                "{} {:.9} emitting data {:?}",
                                name,
                                data_id.to_string(),
                                data
                            );
                            passthrough_output.give(&cap, data).await;
                        }
                    }
                    Event::Progress(progress) => {
                        // If `until.less_equal(progress)`, it means that all
                        // subsequent batches will contain only times greater or
                        // equal to `until`, which means they can be dropped in
                        // their entirety.
                        //
                        // Ideally this check would live in
                        // `txns_progress_source`, but that turns out to be much
                        // more invasive (requires replacing lots of `T`s with
                        // `Antichain<T>`s). Given that we've been thinking
                        // about reworking the operators, do the easy but more
                        // wasteful thing for now.
                        if PartialOrder::less_equal(&until, &progress) {
                            debug!(
                                "{} progress {:?} has passed until {:?}",
                                name,
                                progress.elements(),
                                until.elements()
                            );
                            return;
                        }
                        // We reached the empty frontier! Shut down.
                        let Some(input_progress_exclusive) = progress.as_option() else {
                            return;
                        };

                        // Recall that any reads of the data shard are always
                        // correct, so given that we've passed through any data
                        // from the input, that means we're free to pass through
                        // frontier updates too.
                        if &output_progress_exclusive < input_progress_exclusive {
                            output_progress_exclusive.clone_from(input_progress_exclusive);
                            debug!(
                                "{} {:.9} downgrading cap to {:?}",
                                name,
                                data_id.to_string(),
                                output_progress_exclusive
                            );
                            cap.downgrade(&output_progress_exclusive);
                        }
                        if read_data_to.less_equal(&output_progress_exclusive) {
                            break;
                        }
                    }
                }
            }

            // Any time we hit this point, we've emitted everything known to be
            // physically written to the data shard. Query the txns shard to
            // find out what to do next given our current progress.
            loop {
                txns_cache.update_ge(&output_progress_exclusive).await;
                txns_cache.compact_to(&output_progress_exclusive);
                let data_listen_next = txns_cache
                    .state
                    .data_listen_next(&data_id, output_progress_exclusive.clone());
                debug!(
                    "{} {:.9} data_listen_next at {:?}({:?}): {:?}",
                    name,
                    data_id.to_string(),
                    read_data_to.elements(),
                    output_progress_exclusive,
                    data_listen_next,
                );
                match data_listen_next {
                    // We've caught up to the txns upper and we have to wait for
                    // it to advance before asking again.
                    //
                    // Note that we're asking again with the same input, but
                    // once the cache is past progress_exclusive (as it will be
                    // after this update_gt call), we're guaranteed to get an
                    // answer.
                    DataListenNext::WaitForTxnsProgress => {
                        txns_cache.update_gt(&output_progress_exclusive).await;
                        continue;
                    }
                    // The data shard got a write! Loop back above and pass
                    // through data until we see it.
                    DataListenNext::ReadDataTo(new_target) => {
                        read_data_to = Antichain::from_elem(new_target);
                        // TODO: This is a very strong hint that the data shard
                        // is about to be written to. Because the data shard's
                        // upper advances sparsely (on write, but not on passage
                        // of time) which invalidates the "every 1s" assumption
                        // of the default tuning, we've had to de-tune the
                        // listen sleeps on the paired persist_source. Maybe we
                        // use "one state" to wake it up in case pubsub doesn't
                        // and remove the listen polling entirely? (NB: This
                        // would have to happen in each worker so that it's
                        // guaranteed to happen in each process.)
                        break;
                    }
                    // We know there are no writes in
                    // `[output_progress_exclusive, new_progress)`, so advance
                    // our output frontier.
                    DataListenNext::EmitLogicalProgress(new_progress) => {
                        assert!(output_progress_exclusive < new_progress);
                        output_progress_exclusive = new_progress;
                        trace!(
                            "{} {:.9} downgrading cap to {:?}",
                            name,
                            data_id.to_string(),
                            output_progress_exclusive
                        );
                        cap.downgrade(&output_progress_exclusive);
                        continue;
                    }
                    DataListenNext::CompactedTo(since_ts) => {
                        unreachable!(
                            "internal logic error: {} unexpectedly compacted past {:?} to {:?}",
                            data_id, output_progress_exclusive, since_ts
                        )
                    }
                }
            }
        }
    });
    (passthrough_stream, shutdown_button.press_on_drop())
}

const DATA_SHARD_RETRYER_INITIAL_BACKOFF: Config<Duration> = Config::new(
    "persist_txns_data_shard_retryer_initial_backoff",
    Duration::from_millis(1024),
    "The initial backoff when polling for new batches from a txns data shard persist_source.",
);

const DATA_SHARD_RETRYER_MULTIPLIER: Config<u32> = Config::new(
    "persist_txns_data_shard_retryer_multiplier",
    2,
    "The backoff multiplier when polling for new batches from a txns data shard persist_source.",
);

const DATA_SHARD_RETRYER_CLAMP: Config<Duration> = Config::new(
    "persist_txns_data_shard_retryer_clamp",
    Duration::from_secs(16),
    "The backoff clamp duration when polling for new batches from a txns data shard persist_source.",
);

/// Retry configuration for persist-txns data shard override of
/// `next_listen_batch`.
pub fn txns_data_shard_retry_params(cfg: &ConfigSet) -> RetryParameters {
    RetryParameters {
        initial_backoff: DATA_SHARD_RETRYER_INITIAL_BACKOFF.get(cfg),
        multiplier: DATA_SHARD_RETRYER_MULTIPLIER.get(cfg),
        clamp: DATA_SHARD_RETRYER_CLAMP.get(cfg),
    }
}

// NB: The API of this intentionally mirrors TxnsCache and TxnsRead. Consider
// making them all implement the same trait?
struct TxnsCacheTimely<T: Timestamp + Lattice + Codec64> {
    name: String,
    state: TxnsCacheState<T>,
    input: AsyncInputHandle<T, Vec<(TxnsEntry, T, i64)>, Disconnected>,
    buf: Vec<(TxnsEntry, T, i64)>,
}

impl<T> TxnsCacheTimely<T>
where
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
{
    /// See [TxnsCacheState::compact_to].
    fn compact_to(&mut self, since_ts: &T) {
        self.state.compact_to(since_ts)
    }

    /// See [TxnsCache::update_gt].
    async fn update_gt(&mut self, ts: &T) {
        debug!("{} update_gt {:?}", self.name, ts);
        self.update(|progress_exclusive| progress_exclusive > ts)
            .await;
        debug_assert!(&self.state.progress_exclusive > ts);
        debug_assert_eq!(self.state.validate(), Ok(()));
    }

    /// See [TxnsCache::update_ge].
    async fn update_ge(&mut self, ts: &T) {
        debug!("{} update_ge {:?}", self.name, ts);
        self.update(|progress_exclusive| progress_exclusive >= ts)
            .await;
        debug_assert!(&self.state.progress_exclusive >= ts);
        debug_assert_eq!(self.state.validate(), Ok(()));
    }

    async fn update<F: Fn(&T) -> bool>(&mut self, done: F) {
        while !done(&self.state.progress_exclusive) {
            let Some(event) = self.input.next().await else {
                unreachable!("txns shard unexpectedly closed")
            };
            match event {
                Event::Progress(frontier) => {
                    let progress = frontier
                        .into_option()
                        .expect("nothing should close the txns shard");
                    debug!("{} got progress {:?}", self.name, progress);
                    self.state
                        .push_entries(std::mem::take(&mut self.buf), progress);
                }
                Event::Data(_cap, mut entries) => {
                    debug!("{} got updates {:?}", self.name, entries);
                    self.buf.append(&mut entries);
                }
            }
        }
        debug!(
            "cache correct before {:?} len={} least_ts={:?}",
            self.state.progress_exclusive,
            self.state.unapplied_batches.len(),
            self.state
                .unapplied_batches
                .first_key_value()
                .map(|(_, (_, _, ts))| ts),
        );
    }
}

/// A helper for subscribing to a data shard using the timely operators.
///
/// This could instead be a wrapper around a [Subscribe], but it's only used in
/// tests and maelstrom, so do it by wrapping the timely operators to get
/// additional coverage. For the same reason, hardcode the K, V, T, D types.
///
/// [Subscribe]: mz_persist_client::read::Subscribe
pub struct DataSubscribe {
    pub(crate) as_of: u64,
    pub(crate) worker: Worker<timely::communication::allocator::Thread>,
    data: ProbeHandle<u64>,
    txns: ProbeHandle<u64>,
    capture: mpsc::Receiver<EventCore<u64, Vec<(String, u64, i64)>>>,
    output: Vec<(String, u64, i64)>,

    _tokens: Vec<PressOnDropButton>,
}

impl std::fmt::Debug for DataSubscribe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let DataSubscribe {
            as_of,
            worker: _,
            data,
            txns,
            capture: _,
            output,
            _tokens: _,
        } = self;
        f.debug_struct("DataSubscribe")
            .field("as_of", as_of)
            .field("data", data)
            .field("txns", txns)
            .field("output", output)
            .finish_non_exhaustive()
    }
}

impl DataSubscribe {
    /// Creates a new [DataSubscribe].
    pub fn new(
        name: &str,
        client: PersistClient,
        txns_id: ShardId,
        data_id: ShardId,
        as_of: u64,
        until: Antichain<u64>,
    ) -> Self {
        let mut worker = Worker::new(
            WorkerConfig::default(),
            timely::communication::allocator::Thread::new(),
        );
        let (data, txns, capture, tokens) = worker.dataflow::<u64, _, _>(|scope| {
            let (data_stream, shard_source_token) = scope.scoped::<u64, _, _>("hybrid", |scope| {
                let client = client.clone();
                let (data_stream, token) = shard_source::<String, (), u64, i64, _, _, _, _>(
                    scope,
                    name,
                    move || std::future::ready(client.clone()),
                    data_id,
                    Some(Antichain::from_elem(as_of)),
                    SnapshotMode::Include,
                    until.clone(),
                    false.then_some(|_, _: &_, _| unreachable!()),
                    Arc::new(StringSchema),
                    Arc::new(UnitSchema),
                    |_, _| true,
                    false.then_some(|| unreachable!()),
                );
                (data_stream.leave(), token)
            });
            let (mut data, mut txns) = (ProbeHandle::new(), ProbeHandle::new());
            let data_stream = data_stream.flat_map(|part| {
                part.map(|((k, v), t, d)| {
                    let (k, ()) = (k.unwrap(), v.unwrap());
                    (k, t, d)
                })
            });
            let data_stream = data_stream.probe_with(&mut data);
            let (data_stream, mut txns_progress_token) =
                txns_progress::<String, (), u64, i64, _, TxnsCodecDefault, _, _>(
                    data_stream,
                    name,
                    || std::future::ready(client.clone()),
                    txns_id,
                    data_id,
                    as_of,
                    until,
                    Arc::new(StringSchema),
                    Arc::new(UnitSchema),
                );
            let data_stream = data_stream.probe_with(&mut txns);
            let mut tokens = shard_source_token;
            tokens.append(&mut txns_progress_token);
            (data, txns, data_stream.capture(), tokens)
        });
        Self {
            as_of,
            worker,
            data,
            txns,
            capture,
            output: Vec::new(),
            _tokens: tokens,
        }
    }

    /// Returns the exclusive progress of the dataflow.
    pub fn progress(&self) -> u64 {
        self.txns
            .with_frontier(|f| *f.as_option().unwrap_or(&u64::MAX))
    }

    /// Steps the dataflow, capturing output.
    pub fn step(&mut self) {
        self.worker.step();
        self.capture_output()
    }

    pub(crate) fn capture_output(&mut self) {
        loop {
            let event = match self.capture.try_recv() {
                Ok(x) => x,
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            };
            match event {
                EventCore::Progress(_) => {}
                EventCore::Messages(_, mut msgs) => self.output.append(&mut msgs),
            }
        }
    }

    /// Steps the dataflow past the given time, capturing output.
    #[cfg(test)]
    pub async fn step_past(&mut self, ts: u64) {
        while self.txns.less_equal(&ts) {
            trace!(
                "progress at {:?}",
                self.txns.with_frontier(|x| x.to_owned()).elements()
            );
            self.step();
            tokio::task::yield_now().await;
        }
    }

    /// Returns captured output.
    pub fn output(&self) -> &Vec<(String, u64, i64)> {
        &self.output
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::task::JoinHandleExt;
    use mz_persist_types::Opaque;

    use crate::tests::writer;
    use crate::txns::TxnsHandle;

    use super::*;

    struct DataSubscribeTask {
        /// Carries step requests. A `None` timestamp requests one step, a
        /// `Some(ts)` requests stepping until we progress beyond `ts`.
        tx: std::sync::mpsc::Sender<(Option<u64>, tokio::sync::oneshot::Sender<u64>)>,
        task: mz_ore::task::JoinHandle<Vec<(String, u64, i64)>>,
    }

    impl DataSubscribeTask {
        async fn new(
            client: PersistClient,
            txns_id: ShardId,
            data_id: ShardId,
            as_of: u64,
        ) -> Self {
            let cache = TxnsCache::open(&client, txns_id, Some(data_id)).await;
            let (tx, rx) = std::sync::mpsc::channel();
            let task = mz_ore::task::spawn_blocking(
                || "data_subscribe task",
                move || Self::task(client, cache, data_id, as_of, rx),
            );
            DataSubscribeTask { tx, task }
        }

        async fn step(&self) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.tx.send((None, tx)).expect("task should be running");
            rx.await.expect("task should be running");
        }

        async fn step_past(&self, ts: u64) -> u64 {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.tx
                .send((Some(ts), tx))
                .expect("task should be running");
            rx.await.expect("task should be running")
        }

        async fn finish(self) -> Vec<(String, u64, i64)> {
            // Closing the channel signals the task to exit.
            drop(self.tx);
            self.task.wait_and_assert_finished().await
        }

        fn task(
            client: PersistClient,
            cache: TxnsCache<u64>,
            data_id: ShardId,
            as_of: u64,
            rx: std::sync::mpsc::Receiver<(Option<u64>, tokio::sync::oneshot::Sender<u64>)>,
        ) -> Vec<(String, u64, i64)> {
            let mut subscribe = cache.expect_subscribe(&client, data_id, as_of);
            loop {
                let (ts, tx) = match rx.try_recv() {
                    Ok(x) => x,
                    Err(TryRecvError::Empty) => {
                        // No requests, continue stepping so nothing deadlocks.
                        subscribe.step();
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => {
                        // All done! Return our output.
                        return subscribe.output().clone();
                    }
                };
                // Always step at least once.
                subscribe.step();
                // If we got a ts, make sure to step past it.
                if let Some(ts) = ts {
                    while subscribe.progress() <= ts {
                        subscribe.step();
                    }
                }
                let _ = tx.send(subscribe.progress());
            }
        }
    }

    impl<K, V, T, D, O, C> TxnsHandle<K, V, T, D, O, C>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
        D: Semigroup + Codec64 + Send + Sync,
        O: Opaque + Debug + Codec64,
        C: TxnsCodec,
    {
        async fn subscribe_task(
            &self,
            client: &PersistClient,
            data_id: ShardId,
            as_of: u64,
        ) -> DataSubscribeTask {
            DataSubscribeTask::new(client.clone(), self.txns_id(), data_id, as_of).await
        }
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn data_subscribe() {
        async fn step(subs: &Vec<DataSubscribeTask>) {
            for sub in subs.iter() {
                sub.step().await;
            }
        }

        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let log = txns.new_log();
        let d0 = ShardId::new();

        // Start a subscription before the shard gets registered.
        let mut subs = Vec::new();
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&subs).await;

        // Now register the shard. Also start a new subscription and step the
        // previous one (plus repeat this for ever later step).
        txns.register(1, [writer(&client, d0).await]).await.unwrap();
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&subs).await;

        // Now write something unrelated.
        let d1 = txns.expect_register(2).await;
        txns.expect_commit_at(3, d1, &["nope"], &log).await;
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&subs).await;

        // Now write to our shard before.
        txns.expect_commit_at(4, d0, &["4"], &log).await;
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&subs).await;

        // Now write to our shard at the as_of.
        txns.expect_commit_at(5, d0, &["5"], &log).await;
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&subs).await;

        // Now write to our shard past the as_of.
        txns.expect_commit_at(6, d0, &["6"], &log).await;
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&subs).await;

        // Now write something unrelated again.
        txns.expect_commit_at(7, d1, &["nope"], &log).await;
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&subs).await;

        // Verify that the dataflows can progress to the expected point.
        for sub in subs.iter() {
            let progress = sub.step_past(7).await;
            assert_eq!(progress, 8);
        }

        // Now verify that we read the right thing no matter when the dataflow
        // started.
        for sub in subs {
            sub.step_past(7).await;
            log.assert_eq(d0, 5, 8, sub.finish().await);
        }
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn subscribe_shard_finalize() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let log = txns.new_log();
        let d0 = txns.expect_register(1).await;

        // Start the operator as_of the register ts.
        let mut sub = txns.read_cache().expect_subscribe(&client, d0, 1);
        sub.step_past(1).await;

        // Write to it via txns.
        txns.expect_commit_at(2, d0, &["foo"], &log).await;
        sub.step_past(2).await;

        // Unregister it.
        txns.forget(3, d0).await.unwrap();
        sub.step_past(3).await;

        // TODO: Hard mode, see if we can get the rest of this test to work even
        // _without_ the txns shard advancing.
        txns.begin().commit_at(&mut txns, 7).await.unwrap();

        // The operator should continue to emit data written directly even
        // though it's no longer in the txns set.
        let mut d0_write = writer(&client, d0).await;
        let key = "bar".to_owned();
        crate::small_caa(|| "test", &mut d0_write, &[((&key, &()), &5, 1)], 4, 6)
            .await
            .unwrap();
        log.record((d0, key, 5, 1));
        sub.step_past(4).await;

        // Now finalize the shard to writes.
        let () = d0_write
            .compare_and_append_batch(&mut [], Antichain::from_elem(6), Antichain::new())
            .await
            .unwrap()
            .unwrap();
        while sub.txns.less_than(&u64::MAX) {
            sub.step();
            tokio::task::yield_now().await;
        }

        // Make sure we read the correct things.
        log.assert_eq(d0, 1, u64::MAX, sub.output().clone());

        // Also make sure that we can read the right things if we start up after
        // the forget but before the direct write and ditto after the direct
        // write.
        log.assert_subscribe(d0, 4, u64::MAX).await;
        log.assert_subscribe(d0, 6, u64::MAX).await;
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn subscribe_shard_register_forget() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let d0 = ShardId::new();

        // Start a subscription on the data shard.
        let mut sub = txns.read_cache().expect_subscribe(&client, d0, 0);
        assert_eq!(sub.progress(), 0);

        // Register the shard at 10.
        txns.register(10, [writer(&client, d0).await])
            .await
            .unwrap();
        sub.step_past(10).await;
        assert!(
            sub.progress() > 10,
            "operator should advance past 10 when shard is registered"
        );

        // Forget the shard at 20.
        txns.forget(20, d0).await.unwrap();
        sub.step_past(20).await;
        assert!(
            sub.progress() > 20,
            "operator should advance past 20 when shard is forgotten"
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn as_of_until() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let log = txns.new_log();

        let d0 = txns.expect_register(1).await;
        txns.expect_commit_at(2, d0, &["2"], &log).await;
        txns.expect_commit_at(3, d0, &["3"], &log).await;
        txns.expect_commit_at(4, d0, &["4"], &log).await;
        txns.expect_commit_at(5, d0, &["5"], &log).await;
        txns.expect_commit_at(6, d0, &["6"], &log).await;
        txns.expect_commit_at(7, d0, &["7"], &log).await;

        let mut sub = DataSubscribe::new(
            "as_of_until",
            client,
            txns.txns_id(),
            d0,
            3,
            Antichain::from_elem(5),
        );
        // Manually step the dataflow, instead of going through the
        // `DataSubscribe` helper because we're interested in all captured
        // events.
        while sub.txns.less_equal(&5) {
            sub.worker.step();
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let actual = sub.capture.into_iter().collect::<Vec<_>>();
        let expected = vec![
            EventCore::Messages(
                3,
                vec![
                    ("2".to_owned(), 3, 1),
                    ("3".to_owned(), 3, 1),
                    ("4".to_owned(), 4, 1),
                ],
            ),
            EventCore::Progress(vec![(0, -1), (3, 1)]),
            EventCore::Progress(vec![(3, -1)]),
        ];
        assert_eq!(actual, expected);
    }
}
