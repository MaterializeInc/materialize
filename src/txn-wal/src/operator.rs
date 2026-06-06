// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Timely operators for the crate

use std::any::Any;
use std::fmt::Debug;
use std::future::Future;
use std::sync::mpsc::TryRecvError;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use differential_dataflow::Hashable;
use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use mz_dyncfg::{Config, ConfigSet};
use mz_ore::cast::CastFrom;
use mz_persist_client::cfg::RetryParameters;
use mz_persist_client::operators::shard_source::{
    ErrorHandler, FilterResult, SnapshotMode, shard_source,
};
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
use mz_persist_types::txn::TxnsCodec;
use mz_persist_types::{Codec, Codec64, StepForward};
use mz_timely_util::builder_async::{
    OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton, button,
};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
#[cfg(test)]
use timely::dataflow::operators::Input;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as OperatorBuilderRc;
use timely::dataflow::operators::vec::{Broadcast, Map};
use timely::dataflow::operators::{Capture, Leave, Probe};
use timely::dataflow::{ProbeHandle, Scope, StreamVec};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use timely::worker::Worker;
use timely::{PartialOrder, WorkerConfig};
use tracing::debug;

use crate::TxnsCodecDefault;
use crate::txn_cache::TxnsCache;
use crate::txn_read::{DataRemapEntry, TxnsRead};

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
pub fn txns_progress<'scope, K, V, T, D, P, C, F>(
    passthrough: StreamVec<'scope, T, P>,
    name: &str,
    ctx: &TxnsContext,
    client_fn: impl Fn() -> F,
    txns_id: ShardId,
    data_id: ShardId,
    as_of: T,
    until: Antichain<T>,
    data_key_schema: Arc<K::Schema>,
    data_val_schema: Arc<V::Schema>,
) -> (StreamVec<'scope, T, P>, Vec<PressOnDropButton>)
where
    K: Debug + Codec + Send + Sync,
    V: Debug + Codec + Send + Sync,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64 + Sync,
    D: Debug + Clone + 'static + Monoid + Ord + Codec64 + Send + Sync,
    P: Debug + Clone + 'static,
    C: TxnsCodec + 'static,
    F: Future<Output = PersistClient> + Send + 'static,
{
    let unique_id = (name, passthrough.scope().addr()).hashed();
    let (remap, source_button) = txns_progress_source_global::<K, V, T, D, P, C>(
        passthrough.scope(),
        name,
        ctx.clone(),
        client_fn(),
        txns_id,
        data_id,
        as_of,
        data_key_schema,
        data_val_schema,
        unique_id,
    );
    // Each of the `txns_frontiers` workers wants the full copy of the remap
    // information.
    let remap = remap.broadcast();
    let (passthrough, frontiers_button) = txns_progress_frontiers::<K, V, T, D, P, C>(
        remap,
        passthrough,
        name,
        data_id,
        until,
        unique_id,
    );
    (passthrough, vec![source_button, frontiers_button])
}

/// TODO: I'd much prefer the communication protocol between the two operators
/// to be exactly remap as defined in the [reclocking design doc]. However, we
/// can't quite recover exactly the information necessary to construct that at
/// the moment. Seems worth doing, but in the meantime, intentionally make this
/// look fairly different (`Stream` of `DataRemapEntry` instead of
/// `Collection<FromTime>`) to hopefully minimize confusion. As a performance
/// optimization, we only re-emit this when the _physical_ upper has changed,
/// which means that the frontier of the `Stream<DataRemapEntry<T>>` indicates
/// updates to the logical_upper of the most recent `DataRemapEntry` (i.e. the
/// one with the largest physical_upper).
///
/// [reclocking design doc]:
///     https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210714_reclocking.md
fn txns_progress_source_global<'scope, K, V, T, D, P, C>(
    scope: Scope<'scope, T>,
    name: &str,
    ctx: TxnsContext,
    client: impl Future<Output = PersistClient> + 'static,
    txns_id: ShardId,
    data_id: ShardId,
    as_of: T,
    data_key_schema: Arc<K::Schema>,
    data_val_schema: Arc<V::Schema>,
    unique_id: u64,
) -> (StreamVec<'scope, T, DataRemapEntry<T>>, PressOnDropButton)
where
    K: Debug + Codec + Send + Sync,
    V: Debug + Codec + Send + Sync,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64 + Sync,
    D: Debug + Clone + 'static + Monoid + Ord + Codec64 + Send + Sync,
    P: Debug + Clone + 'static,
    C: TxnsCodec + 'static,
{
    let worker_idx = scope.index();
    let chosen_worker = usize::cast_from(name.hashed()) % scope.peers();
    let name = format!("txns_progress_source({})", name);
    let mut builder = AsyncOperatorBuilder::new(name.clone(), scope);
    let name = format!("{} [{}] {:.9}", name, unique_id, data_id.to_string());
    let (remap_output, remap_stream) = builder.new_output::<CapacityContainerBuilder<Vec<_>>>();

    let shutdown_button = builder.build(move |capabilities| async move {
        if worker_idx != chosen_worker {
            return;
        }

        let [mut cap]: [_; 1] = capabilities.try_into().expect("one capability per output");
        let client = client.await;
        let txns_read = ctx.get_or_init::<T, C>(&client, txns_id).await;

        let _ = txns_read.update_gt(as_of.clone()).await;
        let data_write = client
            .open_writer::<K, V, T, D>(
                data_id,
                Arc::clone(&data_key_schema),
                Arc::clone(&data_val_schema),
                Diagnostics::from_purpose("data read physical upper"),
            )
            .await
            .expect("schema shouldn't change");
        let mut rx = txns_read
            .data_subscribe(data_id, as_of.clone(), data_write)
            .await;
        debug!("{} starting as_of={:?}", name, as_of);

        let mut physical_upper = T::minimum();

        while let Some(remap) = rx.recv().await {
            assert!(physical_upper <= remap.physical_upper);
            assert!(physical_upper < remap.logical_upper);

            let logical_upper = remap.logical_upper.clone();
            // As mentioned in the docs on this function, we only
            // emit updates when the physical upper changes (which
            // happens to makes the protocol a tiny bit more
            // remap-like).
            if remap.physical_upper != physical_upper {
                physical_upper = remap.physical_upper.clone();
                debug!("{} emitting {:?}", name, remap);
                remap_output.give(&cap, remap);
            } else {
                debug!("{} not emitting {:?}", name, remap);
            }
            cap.downgrade(&logical_upper);
        }
    });
    (remap_stream, shutdown_button.press_on_drop())
}

/// The block ordering inside the schedule closure is load-bearing: pending
/// passthrough input is emitted at the pre-activation capability BEFORE any
/// capability downgrade, which keeps the differential invariant `send_time <=
/// record_time` and avoids dropping in-flight rows when the passthrough
/// frontier crosses `until` in the same activation (SQL-299). Do not reorder.
fn txns_progress_frontiers<'scope, K, V, T, D, P, C>(
    remap: StreamVec<'scope, T, DataRemapEntry<T>>,
    passthrough: StreamVec<'scope, T, P>,
    name: &str,
    data_id: ShardId,
    until: Antichain<T>,
    unique_id: u64,
) -> (StreamVec<'scope, T, P>, PressOnDropButton)
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
    D: Clone + 'static + Monoid + Codec64 + Send + Sync,
    P: Debug + Clone + 'static,
    C: TxnsCodec,
{
    let scope = passthrough.scope();
    let name = format!("txns_progress_frontiers({})", name);
    let mut builder = OperatorBuilderRc::new(name.clone(), scope.clone());
    let info = builder.operator_info();
    let name = format!(
        "{} [{}] {}/{} {:.9}",
        name,
        unique_id,
        scope.index(),
        scope.peers(),
        data_id.to_string(),
    );
    let (passthrough_output, passthrough_stream) = builder.new_output::<Vec<P>>();
    let mut passthrough_output = OutputBuilder::from(passthrough_output);
    // Both inputs are disconnected from the output: capability advancement is
    // driven manually based on the remap stream and the passthrough frontier.
    // NB: the output is created BEFORE the inputs on purpose. `new_output`
    // connects to whatever inputs already exist (here, none); the `[]`
    // connection arg below records the input-to-output summary but does not by
    // itself disconnect the output. Creating an input before the output would
    // silently connect them and break the manual capability management.
    let mut remap_input = builder.new_input_connection(remap, Pipeline, []);
    let mut passthrough_input = builder.new_input_connection(passthrough, Pipeline, []);

    let (mut shutdown_handle, shutdown_button) = button(scope, info.address);

    builder.build_reschedule(move |capabilities| {
        // The output capability's time tracks how far we've progressed in
        // copying along the passthrough input. `None` indicates that we've
        // dropped the capability to shut down.
        let [cap]: [_; 1] = capabilities.try_into().expect("one capability per output");
        let mut capability = Some(cap);
        // The most recently observed remap state. Retained even after the remap
        // input closes so we can still advance the output capability to the
        // last known `logical_upper` while the passthrough input is draining.
        // This deliberately diverges from the async impl, which dropped the
        // entry on close and stalled (PER-4).
        let mut remap = DataRemapEntry {
            physical_upper: T::minimum(),
            logical_upper: T::minimum(),
        };
        // Whether the remap input has reached the empty antichain.
        let mut remap_closed = false;

        move |frontiers| {
            // If our worker pressed the button we stop producing data and
            // frontier updates downstream, but mirror `builder_async`: hold the
            // capability and stop draining the inputs until ALL workers have
            // pressed. Dropping the capability on the local press alone would
            // let the downstream frontier advance during cross-worker teardown
            // skew, past times whose data this worker has discarded, while
            // other workers' operator instances still feed downstream.
            if shutdown_handle.local_pressed() {
                return if shutdown_handle.all_pressed() {
                    // All workers pressed: drop the capability and drain the
                    // inputs so teardown does not stall the dataflow.
                    capability = None;
                    remap_input.for_each(|_input_cap, _data| {});
                    passthrough_input.for_each(|_input_cap, _data| {});
                    false
                } else {
                    // Wedge: keep the capability, leave the inputs undrained
                    // (their pending messages hold the frontier), and ask to be
                    // rescheduled until the remaining workers press.
                    true
                };
            }

            // Fold new DataRemapEntries, keeping the one with the largest
            // logical_upper. The ordering of incoming entries is not assumed.
            remap_input.for_each(|_input_cap, data| {
                for x in data.drain(..) {
                    debug!("{} got remap {:?}", name, x);
                    if remap.logical_upper < x.logical_upper {
                        assert!(
                            remap.physical_upper <= x.physical_upper,
                            "previous remap physical upper {:?} is ahead of new remap physical upper {:?}",
                            remap.physical_upper,
                            x.physical_upper,
                        );
                        // TODO: If the physical upper has advanced, that's a very
                        // strong hint that the data shard is about to be written to.
                        // Because the data shard's upper advances sparsely (on write,
                        // but not on passage of time) which invalidates the "every 1s"
                        // assumption of the default tuning, we've had to de-tune the
                        // listen sleeps on the paired persist_source. Maybe we use "one
                        // state" to wake it up in case pubsub doesn't and remove the
                        // listen polling entirely? (NB: This would have to happen in
                        // each worker so that it's guaranteed to happen in each
                        // process.)
                        remap = x;
                    }
                }
            });

            // Apply the remap input's frontier as a `logical_upper` bump. We do
            // not discard `remap` on the empty antichain: the last observed
            // entry remains valid and lets the capability still advance past
            // `physical_upper` while the passthrough input drains.
            if let Some(logical_upper) = frontiers[0].frontier().as_option() {
                if remap.logical_upper < *logical_upper {
                    remap.logical_upper = logical_upper.clone();
                }
            } else {
                remap_closed = true;
            }

            debug!("{} remap {:?} remap_closed={}", name, remap, remap_closed);

            // Pass through any data the passthrough input has pending, at the
            // current (pre-downgrade) capability, BEFORE any downgrade below.
            // `cap.time()` here equals the pre-activation frontier, which is
            // `<=` every pending record's time, so the differential invariant
            // `send_time <= record_time` holds. Doing this before the
            // `until`-driven drop is the SQL-299 fix. NB: nothing to do for
            // `until` because the shard_source (before) and mfp_and_decode
            // (after) filter.
            if let Some(cap) = capability.as_ref() {
                let mut output = passthrough_output.activate();
                passthrough_input.for_each(|_input_cap, data| {
                    debug!("{} emitting data {:?}", name, data);
                    output.session(cap).give_container(data);
                });
            } else {
                // Still drain to avoid stalling the dataflow.
                passthrough_input.for_each(|_input_cap, _data| {});
            }

            // Only consult the passthrough frontier when not waiting on remap to
            // push `physical_upper` past the capability. While `physical_upper
            // <= cap.time()` and the remap input is open, the next expected
            // event is a remap update that jumps `cap` to `logical_upper`, not a
            // passthrough advance. Consulting the passthrough frontier then can
            // drop the capability prematurely (e.g. `SELECT AS OF MAX`, where no
            // remap update ever arrives and the passthrough side reports the
            // empty antichain). Once remap is closed, the passthrough frontier
            // is the only remaining driver.
            let waiting_for_remap = match capability.as_ref() {
                Some(cap) => !remap_closed && remap.physical_upper.less_equal(cap.time()),
                None => false,
            };
            if !waiting_for_remap {
                // Apply the passthrough input's frontier.
                //
                // If `until.less_equal(pass_frontier)`, it means that all
                // subsequent batches will contain only times greater or equal
                // to `until`, which means they can be dropped in their entirety.
                //
                // Ideally this check would live in `txns_progress_source`, but
                // that turns out to be much more invasive (requires replacing
                // lots of `T`s with `Antichain<T>`s). Given that we've been
                // thinking about reworking the operators, do the easy but more
                // wasteful thing for now.
                let pass_frontier = frontiers[1].frontier();
                if PartialOrder::less_equal(&until.borrow(), &pass_frontier) {
                    debug!(
                        "{} progress {:?} has passed until {:?}",
                        name,
                        pass_frontier,
                        until.elements(),
                    );
                    capability = None;
                } else if let Some(new_progress) = pass_frontier.as_option() {
                    // Recall that any reads of the data shard are always
                    // correct, so given that we've passed through any data from
                    // the input, that means we're free to pass through frontier
                    // updates too.
                    if let Some(cap) = capability.as_mut() {
                        if cap.time() < new_progress {
                            debug!("{} downgrading cap to {:?}", name, new_progress);
                            cap.downgrade(new_progress);
                        }
                    }
                } else {
                    // Reached the empty frontier; shut down.
                    capability = None;
                }
            }

            // If we've copied passthrough data to at least `physical_upper`, we
            // can artificially advance the output to `logical_upper`. By the
            // emptiness of `[physical_upper, logical_upper)`, no record still in
            // flight lies below `logical_upper`, so this never strands data.
            if let Some(cap) = capability.as_mut() {
                assert!(remap.physical_upper <= remap.logical_upper);
                let phys_reached = remap.physical_upper.less_equal(cap.time());
                let logical_ahead = cap.time() < &remap.logical_upper;
                if phys_reached && logical_ahead {
                    cap.downgrade(&remap.logical_upper);
                }
            }

            false
        }
    });

    (passthrough_stream, shutdown_button.press_on_drop())
}

/// The process global [`TxnsRead`] that any operator can communicate with.
#[derive(Default, Debug, Clone)]
pub struct TxnsContext {
    read: Arc<tokio::sync::OnceCell<Box<dyn Any + Send + Sync>>>,
}

impl TxnsContext {
    async fn get_or_init<T, C>(&self, client: &PersistClient, txns_id: ShardId) -> TxnsRead<T>
    where
        T: Timestamp + Lattice + Codec64 + TotalOrder + StepForward + Sync,
        C: TxnsCodec + 'static,
    {
        let read = self
            .read
            .get_or_init(|| {
                let client = client.clone();
                async move {
                    let read: Box<dyn Any + Send + Sync> =
                        Box::new(TxnsRead::<T>::start::<C>(client, txns_id).await);
                    read
                }
            })
            .await
            .downcast_ref::<TxnsRead<T>>()
            .expect("timestamp types should match");
        // We initially only have one txns shard in the system.
        assert_eq!(&txns_id, read.txns_id());
        read.clone()
    }
}

// Existing configs use the prefix "persist_txns_" for historical reasons. New
// configs should use the prefix "txn_wal_".

pub(crate) const DATA_SHARD_RETRYER_INITIAL_BACKOFF: Config<Duration> = Config::new(
    "persist_txns_data_shard_retryer_initial_backoff",
    Duration::from_millis(1024),
    "The initial backoff when polling for new batches from a txns data shard persist_source.",
);

pub(crate) const DATA_SHARD_RETRYER_MULTIPLIER: Config<u32> = Config::new(
    "persist_txns_data_shard_retryer_multiplier",
    2,
    "The backoff multiplier when polling for new batches from a txns data shard persist_source.",
);

pub(crate) const DATA_SHARD_RETRYER_CLAMP: Config<Duration> = Config::new(
    "persist_txns_data_shard_retryer_clamp",
    Duration::from_secs(16),
    "The backoff clamp duration when polling for new batches from a txns data shard persist_source.",
);

/// Retry configuration for txn-wal data shard override of
/// `next_listen_batch`.
pub fn txns_data_shard_retry_params(cfg: &ConfigSet) -> RetryParameters {
    RetryParameters {
        fixed_sleep: Duration::ZERO,
        initial_backoff: DATA_SHARD_RETRYER_INITIAL_BACKOFF.get(cfg),
        multiplier: DATA_SHARD_RETRYER_MULTIPLIER.get(cfg),
        clamp: DATA_SHARD_RETRYER_CLAMP.get(cfg),
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
    pub(crate) worker: Worker,
    data: ProbeHandle<u64>,
    txns: ProbeHandle<u64>,
    capture: mpsc::Receiver<Event<u64, Vec<(String, u64, i64)>>>,
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
            timely::communication::Allocator::Thread(
                timely::communication::allocator::Thread::default(),
            ),
            Some(std::time::Instant::now()),
        );
        let (data, txns, capture, tokens) = worker.dataflow::<u64, _, _>(|outer| {
            let (data_stream, shard_source_token) = outer.scoped::<u64, _, _>("hybrid", |scope| {
                let client = client.clone();
                let (data_stream, token) = shard_source::<String, (), u64, i64, _, _, _>(
                    outer,
                    scope,
                    name,
                    move || std::future::ready(client.clone()),
                    data_id,
                    Some(Antichain::from_elem(as_of)),
                    SnapshotMode::Include,
                    until.clone(),
                    false.then_some(|_, _, _| unreachable!()),
                    Arc::new(StringSchema),
                    Arc::new(UnitSchema),
                    FilterResult::keep_all,
                    false.then_some(|| unreachable!()),
                    async {},
                    ErrorHandler::Halt("data_subscribe"),
                );
                (data_stream.leave(outer), token)
            });
            let (data, txns) = (ProbeHandle::new(), ProbeHandle::new());
            let data_stream = data_stream.flat_map(|part| {
                let part = part.parse();
                part.part.map(|((k, ()), t, d)| (k, t, d))
            });
            let data_stream = data_stream.probe_with(&data);
            let (data_stream, mut txns_progress_token) =
                txns_progress::<String, (), u64, i64, _, TxnsCodecDefault, _>(
                    data_stream,
                    name,
                    &TxnsContext::default(),
                    || std::future::ready(client.clone()),
                    txns_id,
                    data_id,
                    as_of,
                    until,
                    Arc::new(StringSchema),
                    Arc::new(UnitSchema),
                );
            let data_stream = data_stream.probe_with(&txns);
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
                Event::Progress(_) => {}
                Event::Messages(_, mut msgs) => self.output.append(&mut msgs),
            }
        }
    }

    /// Steps the dataflow past the given time, capturing output.
    #[cfg(test)]
    pub async fn step_past(&mut self, ts: u64) {
        while self.txns.less_equal(&ts) {
            tracing::trace!(
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

/// A handle to a [DataSubscribe] running in a task.
#[derive(Debug)]
pub struct DataSubscribeTask {
    /// Carries step requests. A `None` timestamp requests one step, a
    /// `Some(ts)` requests stepping until we progress beyond `ts`.
    tx: std::sync::mpsc::Sender<(
        Option<u64>,
        tokio::sync::oneshot::Sender<(Vec<(String, u64, i64)>, u64)>,
    )>,
    task: mz_ore::task::JoinHandle<Vec<(String, u64, i64)>>,
    output: Vec<(String, u64, i64)>,
    progress: u64,
}

impl DataSubscribeTask {
    /// Creates a new [DataSubscribeTask].
    pub async fn new(
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
        DataSubscribeTask {
            tx,
            task,
            output: Vec::new(),
            progress: 0,
        }
    }

    #[cfg(test)]
    async fn step(&mut self) {
        self.send(None).await;
    }

    /// Steps the dataflow past the given time, capturing output.
    pub async fn step_past(&mut self, ts: u64) -> u64 {
        self.send(Some(ts)).await;
        self.progress
    }

    /// Returns captured output.
    pub fn output(&self) -> &Vec<(String, u64, i64)> {
        &self.output
    }

    async fn send(&mut self, ts: Option<u64>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send((ts, tx)).expect("task should be running");
        let (mut new_output, new_progress) = rx.await.expect("task should be running");
        self.output.append(&mut new_output);
        assert!(self.progress <= new_progress);
        self.progress = new_progress;
    }

    /// Signals for the task to exit, and then waits for this to happen.
    ///
    /// _All_ output from the lifetime of the task (not just what was previously
    /// captured) is returned.
    pub async fn finish(self) -> Vec<(String, u64, i64)> {
        // Closing the channel signals the task to exit.
        drop(self.tx);
        self.task.await
    }

    fn task(
        client: PersistClient,
        cache: TxnsCache<u64>,
        data_id: ShardId,
        as_of: u64,
        rx: std::sync::mpsc::Receiver<(
            Option<u64>,
            tokio::sync::oneshot::Sender<(Vec<(String, u64, i64)>, u64)>,
        )>,
    ) -> Vec<(String, u64, i64)> {
        let mut subscribe = DataSubscribe::new(
            "DataSubscribeTask",
            client.clone(),
            cache.txns_id(),
            data_id,
            as_of,
            Antichain::new(),
        );
        let mut output = Vec::new();
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
                    return output;
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
            let new_output = std::mem::take(&mut subscribe.output);
            output.extend(new_output.iter().cloned());
            let _ = tx.send((new_output, subscribe.progress()));
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::{Either, Itertools};

    use crate::tests::writer;
    use crate::txns::TxnsHandle;

    use super::*;

    /// One scripted action applied to the operator's two inputs.
    #[derive(Debug, Clone)]
    enum Action {
        /// Send a `DataRemapEntry` on the remap input.
        Remap {
            physical_upper: u64,
            logical_upper: u64,
        },
        /// Advance the remap input frontier to `ts` (empty antichain if `None`).
        RemapFrontier(Option<u64>),
        /// Send passthrough data records (as `(payload, time)`), then leave them buffered.
        Pass { records: Vec<(i64, u64)> },
        /// Advance the passthrough input frontier to `ts` (empty antichain if `None`).
        PassFrontier(Option<u64>),
        /// Step the worker once.
        Step,
    }

    /// Runs `schedule` against the operator built by `build`, returning the
    /// captured output events and the final exclusive output frontier. Each
    /// event is shaped as `(payload, time, count)`, where `count` is synthesized
    /// as `1` so the output looks like a differential collection.
    fn run_schedule(
        build: impl for<'a> Fn(
            StreamVec<'a, u64, DataRemapEntry<u64>>,
            StreamVec<'a, u64, i64>,
            Antichain<u64>,
        ) -> (StreamVec<'a, u64, i64>, PressOnDropButton),
        until: Antichain<u64>,
        schedule: &[Action],
    ) -> (Vec<(i64, u64, i64)>, u64) {
        let mut worker = Worker::new(
            WorkerConfig::default(),
            timely::communication::Allocator::Thread(
                timely::communication::allocator::Thread::default(),
            ),
            Some(std::time::Instant::now()),
        );

        // The button must outlive the run: dropping it presses the shutdown
        // handle, which makes the operator drop its capability on the next
        // activation. Hold it until after the drain loop completes.
        let (remap_handle, pass_handle, probe, capture, _button) =
            worker.dataflow::<u64, _, _>(|scope| {
                let (remap_handle, remap_stream) = scope.new_input::<Vec<DataRemapEntry<u64>>>();
                let (pass_handle, pass_stream) = scope.new_input::<Vec<i64>>();
                let (out, button) = build(remap_stream, pass_stream, until.clone());
                let probe = ProbeHandle::new();
                let out = out.probe_with(&probe);
                (remap_handle, pass_handle, probe, out.capture(), button)
            });

        // timely input handles can only `advance_to` forward in time. Track the
        // last time used on each input so we can fail loudly with a useful
        // message instead of panicking deep inside timely on a decreasing time.
        let mut last_remap_ts = 0u64;
        let mut last_pass_ts = 0u64;
        // Held in `Option`s so a `*Frontier(None)` action can `take` and drop the
        // handle, which closes the input to the empty antichain. Advancing to
        // `u64::MAX` is NOT equivalent: it leaves the input's frontier at
        // `Some(u64::MAX)`, which the operator (correctly) treats as a finite
        // `logical_upper`/passthrough advance rather than a closed input.
        let mut remap_handle = Some(remap_handle);
        let mut pass_handle = Some(pass_handle);
        for action in schedule {
            match action.clone() {
                // `Remap` is a `send` at the handle's current time, so it carries
                // no explicit time and needs no monotonicity assert.
                Action::Remap {
                    physical_upper,
                    logical_upper,
                } => remap_handle
                    .as_mut()
                    .expect("remap input still open")
                    .send(DataRemapEntry {
                        physical_upper,
                        logical_upper,
                    }),
                Action::RemapFrontier(Some(ts)) => {
                    assert!(
                        ts >= last_remap_ts,
                        "Action::RemapFrontier time {ts} < previous remap time {last_remap_ts}; per-input times must be non-decreasing"
                    );
                    last_remap_ts = ts;
                    remap_handle
                        .as_mut()
                        .expect("remap input still open")
                        .advance_to(ts);
                }
                // Drop the handle to close the input to the empty antichain.
                Action::RemapFrontier(None) => {
                    last_remap_ts = u64::MAX;
                    drop(remap_handle.take());
                }
                Action::Pass { records } => {
                    let handle = pass_handle.as_mut().expect("passthrough input still open");
                    for (payload, time) in records {
                        assert!(
                            time >= last_pass_ts,
                            "Action::Pass time {time} < previous passthrough time {last_pass_ts}; per-input times must be non-decreasing"
                        );
                        last_pass_ts = time;
                        // `advance_to` is what makes each record's time visible to
                        // the operator; the subsequent `send` emits the payload at
                        // that time. Both impls consume the identical schedule, so
                        // the exact send mechanics need only be self-consistent.
                        handle.advance_to(time);
                        handle.send(payload);
                    }
                }
                Action::PassFrontier(Some(ts)) => {
                    assert!(
                        ts >= last_pass_ts,
                        "Action::PassFrontier time {ts} < previous passthrough time {last_pass_ts}; per-input times must be non-decreasing"
                    );
                    last_pass_ts = ts;
                    pass_handle
                        .as_mut()
                        .expect("passthrough input still open")
                        .advance_to(ts);
                }
                // Drop the handle to close the input to the empty antichain.
                Action::PassFrontier(None) => {
                    last_pass_ts = u64::MAX;
                    drop(pass_handle.take());
                }
                Action::Step => {
                    worker.step();
                }
            }
        }
        // Drain: flush inputs and step until the output probe frontier stops
        // advancing. A hard cap PANICS so a buggy operator that never settles
        // fails loudly instead of silently returning partial results.
        if let Some(handle) = remap_handle.as_mut() {
            handle.flush();
        }
        if let Some(handle) = pass_handle.as_mut() {
            handle.flush();
        }
        let mut last = probe.with_frontier(|f| f.to_owned());
        let mut stable = 0;
        for step in 0.. {
            assert!(
                step < 4096,
                "run_schedule did not quiesce within 4096 steps"
            );
            worker.step();
            let now = probe.with_frontier(|f| f.to_owned());
            if now == last {
                stable += 1;
                // Require a few consecutive no-change steps so in-flight messages flush.
                if stable >= 8 {
                    break;
                }
            } else {
                stable = 0;
                last = now;
            }
        }

        let frontier = probe.with_frontier(|f| *f.as_option().unwrap_or(&u64::MAX));
        let mut output = Vec::new();
        while let Ok(event) = capture.try_recv() {
            if let Event::Messages(time, msgs) = event {
                for payload in msgs {
                    output.push((payload, time, 1));
                }
            }
        }
        (output, frontier)
    }

    impl<K, V, T, D, C> TxnsHandle<K, V, T, D, C>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + TotalOrder + StepForward + Codec64 + Sync,
        D: Debug + Monoid + Ord + Codec64 + Send + Sync,
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
        async fn step(subs: &mut Vec<DataSubscribeTask>) {
            for sub in subs.iter_mut() {
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
        step(&mut subs).await;

        // Now register the shard. Also start a new subscription and step the
        // previous one (plus repeat this for every later step).
        txns.register(1, [writer(&client, d0).await]).await.unwrap();
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&mut subs).await;

        // Now write something unrelated.
        let d1 = txns.expect_register(2).await;
        txns.expect_commit_at(3, d1, &["nope"], &log).await;
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&mut subs).await;

        // Now write to our shard before.
        txns.expect_commit_at(4, d0, &["4"], &log).await;
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&mut subs).await;

        // Now write to our shard at the as_of.
        txns.expect_commit_at(5, d0, &["5"], &log).await;
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&mut subs).await;

        // Now write to our shard past the as_of.
        txns.expect_commit_at(6, d0, &["6"], &log).await;
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&mut subs).await;

        // Now write something unrelated again.
        txns.expect_commit_at(7, d1, &["nope"], &log).await;
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&mut subs).await;

        // Verify that the dataflows can progress to the expected point and that
        // we read the right thing no matter when the dataflow started.
        for mut sub in subs {
            let progress = sub.step_past(7).await;
            assert_eq!(progress, 8);
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
        txns.forget(3, [d0]).await.unwrap();
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
            .compare_and_append_batch(&mut [], Antichain::from_elem(6), Antichain::new(), true)
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
        txns.forget(20, [d0]).await.unwrap();
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

        let until = 5;
        let mut sub = DataSubscribe::new(
            "as_of_until",
            client,
            txns.txns_id(),
            d0,
            3,
            Antichain::from_elem(until),
        );
        // Manually step the dataflow, instead of going through the
        // `DataSubscribe` helper because we're interested in all captured
        // events.
        while sub.txns.less_equal(&5) {
            sub.worker.step();
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let (actual_progresses, actual_events): (Vec<_>, Vec<_>) =
            sub.capture.into_iter().partition_map(|event| match event {
                Event::Progress(progress) => Either::Left(progress),
                Event::Messages(ts, data) => Either::Right((ts, data)),
            });
        // Aggregate the captured records, ignoring the stream-level
        // timestamp on each batch. The operator emits each container at
        // whatever capability it currently holds (which is determined by
        // its scheduling cadence and the upstream frontiers it has
        // observed), so the per-batch `ts` is not deterministic and not
        // part of the operator's contract. Per-record `(key, time, diff)`
        // tuples are what callers see, and the differential invariant
        // (stream `ts <= record time`) is checked separately below.
        let mut actual_records: Vec<(String, u64, i64)> = actual_events
            .iter()
            .flat_map(|(_ts, data)| data.iter().cloned())
            .collect();
        actual_records.sort();
        let expected_records: Vec<(String, u64, i64)> = vec![
            ("2".to_owned(), 3, 1),
            ("3".to_owned(), 3, 1),
            ("4".to_owned(), 4, 1),
        ];
        assert_eq!(actual_records, expected_records);

        // Verify the differential invariant: each batch's stream
        // timestamp `ts` must be `<= record_time` for every record it
        // carries. The operator's contract requires this so that
        // downstream differential operators can integrate the records
        // at their declared times.
        for (ts, data) in &actual_events {
            for (_key, record_ts, _diff) in data {
                assert!(
                    ts <= record_ts,
                    "differential invariant violated: stream ts {ts} > record time {record_ts}",
                );
            }
        }

        // The number and contents of progress messages is not guaranteed and
        // depends on the downgrade behavior. The only thing we can assert is
        // the max progress timestamp, if there is one, is less than the until.
        if let Some(max_progress_ts) = actual_progresses
            .into_iter()
            .flatten()
            .map(|(ts, _diff)| ts)
            .max()
        {
            assert!(max_progress_ts < until, "{max_progress_ts} < {until}");
        }
    }

    /// Builds the sync operator for the harness.
    fn build_sync<'a>(
        remap: StreamVec<'a, u64, DataRemapEntry<u64>>,
        pass: StreamVec<'a, u64, i64>,
        until: Antichain<u64>,
    ) -> (StreamVec<'a, u64, i64>, PressOnDropButton) {
        txns_progress_frontiers::<String, (), u64, i64, i64, TxnsCodecDefault>(
            remap,
            pass,
            "test",
            ShardId::new(),
            until,
            0,
        )
    }

    /// Generates a random schedule for the no-data-loss fuzz test. Interleaves
    /// remap entries/frontiers with passthrough data/frontiers. Payloads are
    /// unique and increasing so a single dropped or duplicated record is
    /// detectable; per-input times are non-decreasing (the harness requires
    /// this). The schedule never closes the passthrough input, and the test
    /// uses `until = ∅`, so the operator never has a legitimate reason to shut
    /// down and must pass through every record it is given.
    ///
    /// Schedules are intentionally NOT constrained to respect the remap
    /// "[physical_upper, logical_upper) is empty" contract. The no-data-loss
    /// property must hold under arbitrary interleavings, so feeding
    /// contract-violating schedules only strengthens the test.
    fn gen_schedule(seed: u64) -> Vec<Action> {
        // Simple xorshift RNG for determinism without extra deps.
        let mut state = seed.wrapping_add(0x9E3779B97F4A7C15).max(1);
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        let mut schedule = Vec::new();
        let mut physical = 0u64;
        let mut logical = 0u64;
        let mut pass_frontier = 0u64;
        let mut payload = 0i64;
        let mut remap_closed = false;
        let steps = 8 + (next() % 16);
        for _ in 0..steps {
            match next() % 5 {
                0 if !remap_closed => {
                    physical += next() % 3;
                    logical = logical.max(physical) + (next() % 4);
                    schedule.push(Action::Remap {
                        physical_upper: physical,
                        logical_upper: logical,
                    });
                }
                1 if !remap_closed => {
                    if next() % 8 == 0 {
                        remap_closed = true;
                        schedule.push(Action::RemapFrontier(None));
                    } else {
                        logical += next() % 3;
                        schedule.push(Action::RemapFrontier(Some(logical)));
                    }
                }
                2 => {
                    let t = pass_frontier + (next() % 3);
                    pass_frontier = t;
                    payload += 1;
                    schedule.push(Action::Pass {
                        records: vec![(payload, t)],
                    });
                }
                3 => {
                    pass_frontier += next() % 3;
                    schedule.push(Action::PassFrontier(Some(pass_frontier)));
                }
                _ => schedule.push(Action::Step),
            }
            schedule.push(Action::Step);
        }
        schedule
    }

    /// Fuzz: under any random interleaving, the deasynced operator must emit
    /// every passthrough record it is given (no loss, no duplication) and must
    /// not prematurely shut down. With `until = ∅` and no passthrough close, the
    /// operator never legitimately drops its capability, so the output frontier
    /// must stay finite.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn frontiers_fuzz_no_data_loss() {
        for seed in 0..500u64 {
            let schedule = gen_schedule(seed);
            let mut sent: Vec<i64> = schedule
                .iter()
                .flat_map(|a| match a {
                    Action::Pass { records } => records.iter().map(|(p, _)| *p).collect(),
                    _ => Vec::new(),
                })
                .collect();
            let (out, frontier) = run_schedule(build_sync, Antichain::new(), &schedule);
            let mut emitted: Vec<i64> = out.iter().map(|(p, _, _)| *p).collect();
            sent.sort();
            emitted.sort();
            assert_eq!(
                emitted, sent,
                "seed {seed}: operator lost or duplicated data\nschedule={schedule:?}\nout={out:?}"
            );
            assert_ne!(
                frontier,
                u64::MAX,
                "seed {seed}: operator prematurely shut down (empty output frontier)\nschedule={schedule:?}"
            );
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn frontiers_sql_299_up_to_no_tail_loss() {
        // until = 0. A remap entry with physical_upper = 5 keeps the operator
        // out of the `waiting_for_remap` state (5 > cap.time() = 0), so the
        // until check actually fires. Buffer a record at time 0 (payload 4) and
        // leave it pending. In the single activation, the operator sees both the
        // buffered record and the passthrough frontier at 0, which already
        // satisfies `until <= pass_frontier` and drops the capability. The
        // record must be emitted before that drop, not discarded. Buffering at
        // time 0 (the cap's time) is what makes the record and the
        // until-crossing land in the same activation — with the ordered
        // `new_input` handle, advancing the passthrough frontier past the record
        // would deliver the record in an earlier activation and mask the bug.
        let schedule = vec![
            Action::Remap {
                physical_upper: 5,
                logical_upper: 5,
            },
            Action::RemapFrontier(Some(5)),
            Action::Pass {
                records: vec![(4, 0)],
            },
            Action::PassFrontier(None),
            Action::Step,
        ];
        let (output, _frontier) = run_schedule(build_sync, Antichain::from_elem(0), &schedule);
        let payloads: Vec<i64> = output.iter().map(|(p, _, _)| *p).collect();
        assert!(
            payloads.contains(&4),
            "buffered record at time 0 must be emitted before until-driven shutdown, got {output:?}"
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn frontiers_per4_advance_after_remap_close() {
        // Emit a remap entry whose logical_upper (10) exceeds its physical_upper
        // (5). Close the remap input while the passthrough frontier is still
        // below physical_upper (so the capability has NOT yet advanced to
        // logical_upper), then advance the passthrough frontier up to
        // physical_upper (5). The capability must still advance to logical_upper
        // (10) using the remap entry retained across the close, not stall at the
        // passthrough frontier (5). The async impl dropped the entry on close and
        // stalled here (PER-4).
        let schedule = vec![
            Action::Remap {
                physical_upper: 5,
                logical_upper: 10,
            },
            Action::RemapFrontier(Some(10)),
            Action::Step,
            // Close remap before the passthrough frontier reaches physical_upper.
            Action::RemapFrontier(None),
            Action::Step,
            // Only now does the passthrough frontier reach physical_upper.
            Action::PassFrontier(Some(5)),
            Action::Step,
        ];
        let (_output, frontier) = run_schedule(build_sync, Antichain::new(), &schedule);
        assert_eq!(
            frontier, 10,
            "capability must advance to logical_upper after remap close, got {frontier}"
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn frontiers_select_as_of_max_blocks() {
        // Mimic `SELECT AS OF MAX`: a remap entry exists with physical_upper == 0
        // (so physical_upper <= cap.time() and the operator waits for remap), no
        // further remap update arrives, and the passthrough frontier reaches the
        // empty antichain. The operator must NOT drop its capability (must keep
        // blocking), so the output frontier stays finite (0), not u64::MAX.
        let schedule = vec![
            Action::Remap {
                physical_upper: 0,
                logical_upper: 0,
            },
            Action::RemapFrontier(Some(0)),
            Action::PassFrontier(None),
            Action::Step,
        ];
        let (_output, frontier) = run_schedule(build_sync, Antichain::new(), &schedule);
        assert_eq!(
            frontier, 0,
            "operator must block (retain capability) while waiting for remap, got {frontier}"
        );
    }
}
