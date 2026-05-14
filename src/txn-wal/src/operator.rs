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
use std::collections::VecDeque;
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

/// Re-emit `passthrough` with a translated output frontier, bounded by
/// `until`.
///
/// The operator does not inspect the contents of `passthrough` containers
/// — they are forwarded verbatim. Only the output frontier is computed by
/// this operator.
///
/// # Contract
///
/// Let *T_until* be the first input-stream time at which the `passthrough`
/// frontier satisfies `until.less_equal(pass_frontier)`. If `until` is the
/// empty antichain *T_until* is infinity; the empty `passthrough` frontier
/// is treated as crossing any finite `until`.
///
/// 1. **Lossless forwarding under `until`.** For every container `c`
///    arriving on `passthrough` at input-stream time `t < T_until`, `c` is
///    forwarded on the output verbatim. This holds regardless of how the
///    operator's activations interleave data arrivals and frontier
///    advances: a container that arrives in the same activation as the
///    until-crossing frontier is still forwarded before the output
///    capability is dropped.
/// 2. **No emission past `until`.** Once the `passthrough` frontier
///    crosses `until` (or reaches the empty antichain), the operator
///    drops its output capability and emits nothing further.
/// 3. **Output frontier translation.** The output frontier reports the
///    largest `logical_upper` observed on the remap input whose
///    corresponding `physical_upper` has been crossed by `pass_frontier`,
///    or by an earlier `logical_upper`. Between those bumps, it tracks
///    `pass_frontier`. While waiting on the first remap entry (queue
///    empty and remap input still open), the operator does not advance
///    on `pass_frontier` alone: that prevents `SELECT AS OF MAX` from
///    completing prematurely when the data shard's frontier reaches the
///    empty antichain before the remap source has emitted anything.
/// 4. **Termination.** The output reaches the empty antichain iff the
///    `passthrough` frontier does, the `passthrough` frontier crosses
///    `until`, or the operator is shut down via its `PressOnDropButton`.
///
/// # Inputs
///
/// * `remap` — physical/logical upper pairs from
///   [`txns_progress_source_global`]. `physical_upper` is monotone
///   non-decreasing across entries. The stream may close while
///   `passthrough` is still draining; the last observed entry remains
///   usable for output-frontier advances afterwards.
/// * `passthrough` — opaque records; the operator does not look at them.
/// * `until` — upper bound on the input-stream times that produce
///   output. The contents of records, including any per-record times
///   carried inside `P`, are not consulted.
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
    let mut remap_input = builder.new_input_connection(remap, Pipeline, []);
    let mut passthrough_input = builder.new_input_connection(passthrough, Pipeline, []);

    let (shutdown_handle, shutdown_button) = button(scope, info.address);

    builder.build(move |capabilities| {
        // The output capability's time tracks how far we've progressed in
        // copying along the passthrough input. `None` indicates that we've
        // dropped the capability to shut down.
        let [cap]: [_; 1] = capabilities
            .try_into()
            .expect("one capability per output");
        let mut capability = Some(cap);
        // Pending `DataRemapEntry`s observed from the remap input, in
        // arrival order. Each entry `(phys, log)` asserts that
        // `[phys, log)` is empty on the data shard. The async original
        // consumed remap entries via `passthrough_input.next().await`
        // strictly FIFO, which let it advance the cap stepwise at every
        // `phys_i`. The sync `for_each` flattens that into a single
        // activation, so we have to retain the queue ourselves to recover
        // that stepwise advancement; otherwise the cap can only ever hop
        // to the largest seen `logical_upper`, never to an intermediate
        // one, and stalls when the passthrough frontier sits at an
        // intermediate `physical_upper`.
        //
        // Invariant: `physical_upper` is non-decreasing across the queue.
        // Consecutive entries with equal `physical_upper` are coalesced
        // into a single entry whose `logical_upper` is the max.
        let mut remap_queue: VecDeque<DataRemapEntry<T>> = VecDeque::new();
        // The largest `logical_upper` ever observed on the remap input
        // (whether from an emitted `DataRemapEntry` or from the remap
        // input frontier alone). `txns_progress_source` only emits a new
        // entry when `physical_upper` changes, but downgrades its cap to
        // `logical_upper` every iteration; that means the remap input
        // frontier can advance past the last entry's `logical_upper`
        // without producing a new entry, and we need to remember that
        // bound so the output cap can still advance after the queue has
        // been fully consumed.
        let mut latest_remap_log: T = T::minimum();
        // Whether the remap input has reached the empty antichain. Once
        // closed, no further remap updates can arrive and the passthrough
        // frontier is the only remaining driver of cap advancement.
        let mut remap_closed = false;

        move |frontiers| {
            if shutdown_handle.local_pressed() {
                capability = None;
            }

            // Drain new DataRemapEntries into the queue. Coalesce equal
            // `physical_upper`s and assert monotonicity. Track the
            // largest observed `logical_upper` separately so we don't
            // lose it once the queue is fully consumed.
            remap_input.for_each(|_input_cap, data| {
                for x in data.drain(..) {
                    debug!("{} got remap {:?}", name, x);
                    if latest_remap_log < x.logical_upper {
                        latest_remap_log = x.logical_upper.clone();
                    }
                    if let Some(last) = remap_queue.back_mut() {
                        assert!(
                            last.physical_upper <= x.physical_upper,
                            "previous remap physical upper {:?} is ahead of new remap physical upper {:?}",
                            last.physical_upper,
                            x.physical_upper,
                        );
                        if last.physical_upper == x.physical_upper {
                            if last.logical_upper < x.logical_upper {
                                last.logical_upper = x.logical_upper;
                            }
                            continue;
                        }
                    }
                    remap_queue.push_back(x);
                }
            });

            // Apply the remap input's frontier as a `logical_upper` bump.
            // `txns_progress_source` only emits a `DataRemapEntry` when
            // `physical_upper` changes; when its cap is downgraded for a
            // `logical_upper`-only advance (e.g. a txn at an unrelated
            // data shard advanced the txns shard upper), that shows up
            // here as a frontier advance without a corresponding entry.
            // Track that bump in both the queue tail (if any) and in
            // `latest_remap_log`, so that even after the queue is fully
            // consumed the cap can still advance up to the frontier. We
            // do not discard queued entries when the remap input reaches
            // the empty antichain: each `[phys, log)` claim remains valid
            // and lets the output capability still be advanced past
            // `physical_upper` while the passthrough input is draining.
            if let Some(logical_upper) = frontiers[0].frontier().as_option() {
                if latest_remap_log < *logical_upper {
                    latest_remap_log = logical_upper.clone();
                }
                if let Some(last) = remap_queue.back_mut() {
                    if last.logical_upper < *logical_upper {
                        last.logical_upper = logical_upper.clone();
                    }
                }
            } else {
                remap_closed = true;
            }

            debug!(
                "{} remap_queue.len={} remap_closed={}",
                name,
                remap_queue.len(),
                remap_closed,
            );

            // Pass through any received data using the *current* output
            // capability, before any cap advancement below. This preserves
            // the differential invariant `send_time <= record_time` for
            // buffered data: the cap at this point reflects either
            // `T::minimum()` (first activation) or whatever frontier the
            // previous activation last advanced to, and incoming records
            // were emitted by upstream at times `>= prev pass_frontier`,
            // so `cap.time() <= record_time` holds.
            //
            // Draining before the cap is advanced and *before* the until
            // check below is what fixes the data-loss bug: any record
            // buffered when the passthrough frontier crosses `until` is
            // still emitted at the held cap, instead of being silently
            // discarded after the cap was dropped to `None`.
            //
            // NB: Nothing to do here for `until` because the upstream
            // operator and downstream consumer do per-record filtering;
            // this operator forwards containers unchanged.
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

            // Advance the cap based on the queue head and the passthrough
            // frontier, alternating until neither makes progress. Walking
            // the queue lets the cap hop forward whenever the head's
            // `physical_upper` has been reached (we know `[phys, log)` is
            // empty); applying `pass_frontier` lets it advance into the
            // gap between consecutive queue entries when the frontier has
            // already moved past it. Either step can unblock the other,
            // so we loop until quiescent.
            //
            // "Waiting for remap" means the queue is empty and the remap
            // input is still open: in that state we expect a remap update
            // to tell us the next `logical_upper`, and applying the
            // passthrough frontier alone could drop the cap to the empty
            // antichain prematurely. For example, `SELECT AS OF MAX`
            // causes `txns_progress_source` to block on `update_gt(MAX)`,
            // so no remap update ever arrives and the passthrough side
            // eventually reports the empty antichain; we must not drop
            // the capability on that, or the SELECT would complete
            // instead of blocking until the timestamp exists.
            let pass_frontier = frontiers[1].frontier();
            if let Some(cap) = capability.as_mut() {
                loop {
                    let mut advanced = false;
                    while let Some(head) = remap_queue.front() {
                        if !head.physical_upper.less_equal(cap.time()) {
                            break;
                        }
                        let log = head.logical_upper.clone();
                        remap_queue.pop_front();
                        if cap.time() < &log {
                            debug!("{} downgrading cap to {:?} via queue", name, log);
                            cap.downgrade(&log);
                            advanced = true;
                        }
                    }
                    // Once the queue is fully consumed, the remap input
                    // frontier (captured in `latest_remap_log`) is a
                    // valid further bound: the source's cap has been
                    // downgraded past every emitted entry's
                    // `logical_upper`, and any range between the last
                    // entry's `logical_upper` and `latest_remap_log` is
                    // also empty on the data shard (no entry would have
                    // been emitted otherwise, because `physical_upper`
                    // did not change).
                    if remap_queue.is_empty() && cap.time() < &latest_remap_log {
                        debug!(
                            "{} downgrading cap to {:?} via latest_remap_log",
                            name, latest_remap_log,
                        );
                        cap.downgrade(&latest_remap_log);
                        advanced = true;
                    }
                    let waiting_for_remap = !remap_closed
                        && remap_queue.is_empty()
                        && cap.time() == &latest_remap_log;
                    if !waiting_for_remap {
                        if let Some(new_progress) = pass_frontier.as_option() {
                            if cap.time() < new_progress {
                                debug!(
                                    "{} downgrading cap to {:?} via pass_frontier",
                                    name, new_progress,
                                );
                                cap.downgrade(new_progress);
                                advanced = true;
                            }
                        }
                    }
                    if !advanced {
                        break;
                    }
                }
            }

            // Drop the capability when the passthrough frontier has
            // crossed `until` or reached the empty antichain — but only
            // when we are not still waiting on a remap update, for the
            // `SELECT AS OF MAX` reason above. Doing this after the drain
            // is what makes the data-loss bug impossible.
            let waiting_for_remap = !remap_closed
                && remap_queue.is_empty()
                && capability.as_ref().is_some_and(|c| c.time() == &latest_remap_log);
            if !waiting_for_remap {
                if PartialOrder::less_equal(&until.borrow(), &pass_frontier) {
                    // If `until.less_equal(pass_frontier)`, all subsequent
                    // batches contain only times `>= until`, which can be
                    // dropped in their entirety. Ideally this check would
                    // live in `txns_progress_source`, but that turns out
                    // to be much more invasive (requires replacing lots
                    // of `T`s with `Antichain<T>`s). Given that we've
                    // been thinking about reworking the operators, do
                    // the easy but more wasteful thing for now.
                    debug!(
                        "{} progress {:?} has passed until {:?}",
                        name,
                        pass_frontier,
                        until.elements(),
                    );
                    capability = None;
                } else if pass_frontier.as_option().is_none() {
                    // Reached the empty frontier; shut down.
                    capability = None;
                }
            }
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

    /// Regression test for the case where the remap input closes while the
    /// passthrough input is still draining. The operator must still advance
    /// the output capability to the last observed `logical_upper` once the
    /// passthrough frontier catches up to `physical_upper`. See PER-4.
    #[mz_ore::test]
    fn frontiers_advance_to_logical_after_remap_close() {
        use timely::dataflow::operators::Capture;
        use timely::dataflow::operators::vec::UnorderedInput;

        let captured = timely::execute_directly(|worker| {
            let (mut remap_handle, remap_cap, pass_handle, pass_cap, captured, _button) = worker
                .dataflow::<u64, _, _>(|scope| {
                    let ((remap_handle, remap_cap), remap_stream) =
                        scope.new_unordered_input::<DataRemapEntry<u64>>();
                    let ((pass_handle, pass_cap), pass_stream) = scope.new_unordered_input::<i32>();
                    let (out, button) =
                        txns_progress_frontiers::<String, (), u64, i64, i32, TxnsCodecDefault>(
                            remap_stream,
                            pass_stream,
                            "test",
                            ShardId::new(),
                            Antichain::from_elem(u64::MAX),
                            0,
                        );
                    let captured = out.capture();
                    (
                        remap_handle,
                        remap_cap,
                        pass_handle,
                        pass_cap,
                        captured,
                        button,
                    )
                });

            // Emit a single remap entry whose logical_upper (10) is ahead of
            // its physical_upper (5), then close the remap input. The output
            // capability is still at 0; the operator has not yet had a chance
            // to advance to logical_upper because the passthrough frontier
            // has not reached physical_upper.
            remap_handle
                .activate()
                .session(&remap_cap)
                .give(DataRemapEntry {
                    physical_upper: 5,
                    logical_upper: 10,
                });
            drop(remap_cap);

            // Step the worker so the operator observes the remap entry and
            // the remap input closing.
            for _ in 0..5 {
                worker.step();
            }

            // Now advance the passthrough frontier to 5 — matching
            // physical_upper — without emitting any data. With the fix the
            // operator should then advance its output capability to
            // logical_upper (10).
            let pass_cap_5 = pass_cap.delayed(&5);
            drop(pass_cap);
            for _ in 0..5 {
                worker.step();
            }

            // Drop the remaining passthrough capability so the operator
            // shuts down cleanly.
            drop(pass_cap_5);
            for _ in 0..5 {
                worker.step();
            }
            // pass_handle must outlive the steps above so the unordered
            // input is not dropped prematurely.
            drop(pass_handle);
            drop(remap_handle);

            captured.into_iter().collect::<Vec<_>>()
        });

        let max_progress_ts = captured
            .iter()
            .filter_map(|event| match event {
                Event::Progress(progress) => Some(progress.iter().map(|(ts, _diff)| *ts)),
                Event::Messages(_, _) => None,
            })
            .flatten()
            .max()
            .expect("at least one progress event");
        assert!(
            max_progress_ts >= 10,
            "output capability should have reached logical_upper=10 after remap close, \
             but max progress was {max_progress_ts}",
        );
    }

    /// Regression test for SQL-299: when the passthrough frontier crosses
    /// `until` in the same activation as a buffered passthrough record,
    /// the record must still be emitted before the output capability is
    /// dropped. The pre-fix `txns_progress_frontiers` reacted to the
    /// frontier first and drained-and-discarded the buffer afterwards.
    #[mz_ore::test]
    fn passthrough_drained_before_until_drop() {
        use timely::dataflow::operators::Capture;
        use timely::dataflow::operators::vec::UnorderedInput;

        let captured = timely::execute_directly(|worker| {
            let (mut remap_handle, remap_cap, mut pass_handle, pass_cap, captured, _button) =
                worker.dataflow::<u64, _, _>(|scope| {
                    let ((remap_handle, remap_cap), remap_stream) =
                        scope.new_unordered_input::<DataRemapEntry<u64>>();
                    let ((pass_handle, pass_cap), pass_stream) = scope.new_unordered_input::<i32>();
                    let (out, button) =
                        txns_progress_frontiers::<String, (), u64, i64, i32, TxnsCodecDefault>(
                            remap_stream,
                            pass_stream,
                            "test",
                            ShardId::new(),
                            Antichain::from_elem(5),
                            0,
                        );
                    let captured = out.capture();
                    (
                        remap_handle,
                        remap_cap,
                        pass_handle,
                        pass_cap,
                        captured,
                        button,
                    )
                });

            // Queue a remap entry whose `physical_upper` is above
            // `until`. The operator cannot consume it at the initial
            // cap, so the queue stays non-empty for the activation
            // below — that makes `waiting_for_remap` false, which is
            // the state in which the until check actually fires (the
            // `SELECT AS OF MAX` guard does not apply).
            remap_handle
                .activate()
                .session(&remap_cap)
                .give(DataRemapEntry {
                    physical_upper: 10,
                    logical_upper: 10,
                });

            // Queue a passthrough record at the initial cap (time 0).
            // It must end up on the output before the cap is dropped.
            pass_handle.activate().session(&pass_cap).give(42i32);

            // Advance the passthrough frontier to `until` *without*
            // stepping the worker first. The next activation sees both
            // the buffered record on the passthrough input and the
            // until-crossing frontier, which is exactly the state that
            // the buggy ordering silently drops data in.
            let pass_cap_5 = pass_cap.delayed(&5);
            drop(pass_cap);

            for _ in 0..10 {
                worker.step();
            }

            // Clean up so the dataflow shuts down.
            drop(pass_cap_5);
            drop(remap_cap);
            drop(pass_handle);
            drop(remap_handle);
            for _ in 0..5 {
                worker.step();
            }

            captured.into_iter().collect::<Vec<_>>()
        });

        let total_records: usize = captured
            .iter()
            .filter_map(|event| match event {
                Event::Messages(_, data) => Some(data.len()),
                Event::Progress(_) => None,
            })
            .sum();
        assert_eq!(
            total_records, 1,
            "expected the buffered passthrough record to be emitted before \
             the until-crossing dropped the output capability, got events: \
             {captured:?}",
        );
    }
}
