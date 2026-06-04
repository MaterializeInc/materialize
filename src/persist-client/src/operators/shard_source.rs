// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A source that reads from a persist shard.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, VecDeque};
use std::convert::Infallible;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use arrow::array::ArrayRef;
use differential_dataflow::Hashable;
use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use futures_util::StreamExt;
use mz_ore::cast::CastFrom;
use mz_persist_types::stats::PartStats;
use mz_persist_types::{Codec, Codec64};
use mz_timely_util::activator::ArcActivator;
use mz_timely_util::builder_async::{PressOnDropButton, button};
use timely::PartialOrder;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as OperatorBuilderRc;
use timely::dataflow::operators::{Capability, CapabilitySet, ConnectLoop, Enter, Feedback, Leave};
use timely::dataflow::{Scope, StreamVec};
use timely::order::TotalOrder;
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp, timestamp::Refines};
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{debug, trace};

use crate::batch::BLOB_TARGET_SIZE;
use crate::cfg::{RetryParameters, USE_CRITICAL_SINCE_SOURCE};
use crate::fetch::{ExchangeableBatchPart, FetchedBlob, Lease};
use crate::internal::paths::BlobKey;
use crate::internal::state::BatchPart;
use crate::stats::{STATS_AUDIT_PERCENT, STATS_FILTER_ENABLED};
use crate::{Diagnostics, PersistClient, ShardId};

/// The result of applying an MFP to a part, if we know it.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum FilterResult {
    /// This dataflow may or may not filter out any row in this part.
    #[default]
    Keep,
    /// This dataflow is guaranteed to filter out all records in this part.
    Discard,
    /// This dataflow will keep all the rows, but the values are irrelevant:
    /// include the given single-row KV data instead.
    ReplaceWith {
        /// The single-element key column.
        key: ArrayRef,
        /// The single-element val column.
        val: ArrayRef,
    },
}

impl FilterResult {
    /// The noop filtering function: return the default value for all parts.
    pub fn keep_all<T>(_stats: &PartStats, _frontier: AntichainRef<T>) -> FilterResult {
        Self::Keep
    }
}

/// Many dataflows, including the Persist source, encounter errors that are neither data-plane
/// errors (a la SourceData) nor bugs. This includes:
/// - lease timeouts: the source has failed to heartbeat, the lease timed out, and our inputs are
///   GCed away. (But we'd be able to use the compaction output if we restart.)
/// - external transactions: our Kafka transaction has failed, and we can't re-create it without
///   re-ingesting a bunch of data we no longer have in memory. (But we could do on restart.)
///
/// It would be an error to simply exit from our dataflow operator, since that allows timely
/// frontiers to advance, which signals progress that we haven't made. So we report the error and
/// attempt to trigger a restart: either directly (via a `halt!`) or indirectly with a callback.
#[derive(Clone)]
pub enum ErrorHandler {
    /// Halt the process on error.
    Halt(&'static str),
    /// Signal an error to a higher-level supervisor.
    Signal(Rc<dyn Fn(anyhow::Error) + 'static>),
}

impl Debug for ErrorHandler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorHandler::Halt(name) => f.debug_tuple("ErrorHandler::Halt").field(name).finish(),
            ErrorHandler::Signal(_) => f.write_str("ErrorHandler::Signal"),
        }
    }
}

impl ErrorHandler {
    /// Returns a new error handler that uses the provided function to signal an error.
    pub fn signal(signal_fn: impl Fn(anyhow::Error) + 'static) -> Self {
        Self::Signal(Rc::new(signal_fn))
    }

    /// Signal an error to an error handler from a synchronous operator. For [ErrorHandler::Halt]
    /// this never returns; for [ErrorHandler::Signal] it returns after invoking the callback, and
    /// the caller is responsible for "freezing": retaining its capabilities and doing no further
    /// work, so that no spurious progress is observable while a restart is pending.
    pub fn report_and_freeze(&self, error: anyhow::Error) {
        match self {
            ErrorHandler::Halt(name) => {
                mz_ore::halt!("unhandled error in {name}: {error:#}")
            }
            ErrorHandler::Signal(callback) => callback(error),
        }
    }

    /// Signal an error to an error handler. This function never returns: logically it blocks until
    /// restart, though that restart might be sooner (if halting) or later (if triggering a dataflow
    /// restart, for example).
    pub async fn report_and_stop(&self, error: anyhow::Error) -> ! {
        match self {
            ErrorHandler::Halt(name) => {
                mz_ore::halt!("unhandled error in {name}: {error:#}")
            }
            ErrorHandler::Signal(callback) => {
                let () = callback(error);
                std::future::pending().await
            }
        }
    }
}

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
/// All updates at times greater or equal to `until` will be suppressed.
/// The `map_filter_project` argument, if supplied, may be partially applied,
/// and any un-applied part of the argument will be left behind in the argument.
///
/// The `desc_transformer` interposes an operator in the stream before the
/// chosen data is fetched. This is currently used to provide flow control... see
/// usages for details.
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
pub fn shard_source<'inner, 'outer, K, V, T, D, DT, TOuter, C>(
    outer: Scope<'outer, TOuter>,
    scope: Scope<'inner, T>,
    name: &str,
    client: impl Fn() -> C,
    shard_id: ShardId,
    as_of: Option<Antichain<TOuter>>,
    snapshot_mode: SnapshotMode,
    until: Antichain<TOuter>,
    desc_transformer: Option<DT>,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    filter_fn: impl FnMut(&PartStats, AntichainRef<TOuter>) -> FilterResult + Send + 'static,
    // If Some, an override for the default listen sleep retry parameters.
    listen_sleep: Option<impl Fn() -> RetryParameters + Send + 'static>,
    start_signal: impl Future<Output = ()> + Send + 'static,
    error_handler: ErrorHandler,
) -> (
    StreamVec<'inner, T, FetchedBlob<K, V, TOuter, D>>,
    Vec<PressOnDropButton>,
)
where
    K: Debug + Codec,
    V: Debug + Codec,
    D: Monoid + Codec64 + Send + Sync,
    // TODO: Figure out how to get rid of the TotalOrder bound :(.
    TOuter: Timestamp + Lattice + Codec64 + TotalOrder + Sync,
    T: Refines<TOuter>,
    DT: FnOnce(
        Scope<'inner, T>,
        StreamVec<'inner, T, (usize, ExchangeableBatchPart<TOuter>)>,
        usize,
    ) -> (
        StreamVec<'inner, T, (usize, ExchangeableBatchPart<TOuter>)>,
        Vec<PressOnDropButton>,
    ),
    C: Future<Output = PersistClient> + Send + 'static,
{
    // WARNING! If emulating any of this code, you should read the doc string on
    // [`LeasedBatchPart`] and [`Subscribe`] or will likely run into intentional
    // panics.
    //
    // This source is split as such:
    // 1. Sets up `async_stream`, which only yields data (parts) on one chosen
    //    worker. Generating also generates SeqNo leases on the chosen worker,
    //    ensuring `part`s do not get GCed while in flight.
    // 2. Part distribution: A timely source operator which continuously reads
    //    from that stream, and distributes the data among workers.
    // 3. Part fetcher: A timely operator which downloads the part's contents
    //    from S3, and outputs them to a timely stream. Additionally, the
    //    operator returns the `LeasedBatchPart` to the original worker, so it
    //    can release the SeqNo lease.

    let chosen_worker = usize::cast_from(name.hashed()) % scope.peers();

    let mut tokens = vec![];

    // we can safely pass along a zero summary from this feedback edge,
    // as the input is disconnected from the operator's output
    let (completed_fetches_feedback_handle, completed_fetches_feedback_stream) =
        scope.feedback(T::Summary::default());

    // Sniff out if this is on behalf of a transient dataflow. This doesn't
    // affect the fetch behavior, it just causes us to use a different set of
    // metrics.
    let is_transient = !until.is_empty();

    let (descs, descs_token) = shard_source_descs::<K, V, D, TOuter>(
        outer,
        name,
        client(),
        shard_id.clone(),
        as_of,
        snapshot_mode,
        until,
        completed_fetches_feedback_stream.leave(outer),
        chosen_worker,
        Arc::clone(&key_schema),
        Arc::clone(&val_schema),
        filter_fn,
        listen_sleep,
        start_signal,
        error_handler.clone(),
    );
    tokens.push(descs_token);

    let descs = descs.enter(scope);
    let descs = match desc_transformer {
        Some(desc_transformer) => {
            let (descs, extra_tokens) = desc_transformer(scope, descs, chosen_worker);
            tokens.extend(extra_tokens);
            descs
        }
        None => descs,
    };

    let (parts, completed_fetches_stream, fetch_token) = shard_source_fetch::<K, V, TOuter, D, T>(
        descs,
        name,
        client(),
        shard_id,
        key_schema,
        val_schema,
        is_transient,
        error_handler,
    );
    completed_fetches_stream.connect_loop(completed_fetches_feedback_handle);
    tokens.push(fetch_token);

    (parts, tokens)
}

/// An enum describing whether a snapshot should be emitted
#[derive(Debug, Clone, Copy)]
pub enum SnapshotMode {
    /// The snapshot will be included in the stream
    Include,
    /// The snapshot will not be included in the stream
    Exclude,
}

#[derive(Debug)]
struct LeaseManager<T> {
    leases: BTreeMap<T, Vec<Lease>>,
}

impl<T: Timestamp + Codec64> LeaseManager<T> {
    fn new() -> Self {
        Self {
            leases: BTreeMap::new(),
        }
    }

    /// Track a lease associated with a particular time.
    fn push_at(&mut self, time: T, lease: Lease) {
        self.leases.entry(time).or_default().push(lease);
    }

    /// Discard any leases for data that aren't past the given frontier.
    fn advance_to(&mut self, frontier: AntichainRef<T>)
    where
        // If we allowed partial orders, we'd need to reconsider every key on each advance.
        T: TotalOrder,
    {
        while let Some(first) = self.leases.first_entry() {
            if frontier.less_equal(first.key()) {
                break; // This timestamp is still live!
            }
            drop(first.remove());
        }
    }
}

/// A message from the listen task to the `shard_source_descs` operator.
enum ListenMessage<T> {
    /// The resolved `as_of`; the operator downgrades its capabilities to it.
    AsOf(Antichain<T>),
    /// Parts minted at `ts`, each with the lease that protects it from GC.
    Parts {
        ts: T,
        parts: Vec<(usize, ExchangeableBatchPart<T>, Lease)>,
    },
    /// Listen progress; the operator downgrades its capabilities. The empty
    /// antichain indicates that the listen is complete (`until` was reached).
    Progress(Antichain<T>),
    /// A fatal error; the operator reports it and freezes.
    Error(anyhow::Error),
}

pub(crate) fn shard_source_descs<'outer, K, V, D, TOuter>(
    scope: Scope<'outer, TOuter>,
    name: &str,
    client: impl Future<Output = PersistClient> + Send + 'static,
    shard_id: ShardId,
    as_of: Option<Antichain<TOuter>>,
    snapshot_mode: SnapshotMode,
    until: Antichain<TOuter>,
    completed_fetches_stream: StreamVec<'outer, TOuter, Infallible>,
    chosen_worker: usize,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    mut filter_fn: impl FnMut(&PartStats, AntichainRef<TOuter>) -> FilterResult + Send + 'static,
    // If Some, an override for the default listen sleep retry parameters.
    listen_sleep: Option<impl Fn() -> RetryParameters + Send + 'static>,
    start_signal: impl Future<Output = ()> + Send + 'static,
    error_handler: ErrorHandler,
) -> (
    StreamVec<'outer, TOuter, (usize, ExchangeableBatchPart<TOuter>)>,
    PressOnDropButton,
)
where
    K: Debug + Codec,
    V: Debug + Codec,
    D: Monoid + Codec64 + Send + Sync,
    // TODO: Figure out how to get rid of the TotalOrder bound :(.
    TOuter: Timestamp + Lattice + Codec64 + TotalOrder + Sync,
{
    let worker_index = scope.index();
    let num_workers = scope.peers();

    let name_owned = name.to_owned();

    let mut builder =
        OperatorBuilderRc::new(format!("shard_source_descs({})", name), scope.clone());
    let info = builder.operator_info();
    // NB: create the output before the input, so that the input's explicit
    // empty connection below refers to the right output port.
    let (descs_output, descs_stream) =
        builder.new_output::<Vec<(usize, ExchangeableBatchPart<TOuter>)>>();
    let mut descs_output = OutputBuilder::from(descs_output);
    // The completed fetches input is disconnected from the output: it only
    // drives lease returns, not output progress.
    let mut completed_fetches =
        builder.new_input_connection(completed_fetches_stream, Pipeline, []);

    // Only the chosen worker produces parts. It spawns a listen task that owns
    // all async work (reader, snapshot, listen loop, stats-based filtering)
    // and communicates with the operator over a channel. The other workers
    // build the same operator shape but immediately drop their capabilities.
    let chosen_state = (worker_index == chosen_worker).then(|| {
        let (msg_tx, msg_rx) = tokio::sync::mpsc::unbounded_channel::<ListenMessage<TOuter>>();
        // Fired by the operator once the completed fetches frontier is empty,
        // i.e. all fetches are done. The task holds the listen handle (and
        // with it the reader's seqno hold) until then.
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let (activator, activation_ack) = ArcActivator::new(scope.clone(), &info);

        let task = mz_ore::task::spawn(|| format!("shard_source_descs({})", name_owned), {
            let name = name_owned.clone();
            let error = move |e: anyhow::Error,
                              msg_tx: &tokio::sync::mpsc::UnboundedSender<ListenMessage<TOuter>>,
                              activator: &ArcActivator| {
                let _ = msg_tx.send(ListenMessage::Error(e));
                activator.activate();
            };
            async move {
                // Internally, the `open_leased_reader` call registers a new LeasedReaderId and
                // then fires up a background tokio task to heartbeat it. Since we are already
                // running inside a task here, the heartbeat task is spawned promptly.
                let client = client.await;
                let diagnostics = Diagnostics {
                    handle_purpose: format!("shard_source({})", name),
                    shard_name: name.clone(),
                };
                let mut read = client
                    .open_leased_reader::<K, V, TOuter, D>(
                        shard_id,
                        key_schema,
                        val_schema,
                        diagnostics,
                        USE_CRITICAL_SINCE_SOURCE.get(client.dyncfgs()),
                    )
                    .await
                    .expect("could not open persist shard");

                // Wait for the start signal only after we have obtained a read handle. This
                // makes "cannot serve requested as_of" panics caused by (database-issues#8729)
                // significantly less likely.
                let () = start_signal.await;

                let cfg = read.cfg.clone();
                let metrics = Arc::clone(&read.metrics);

                let as_of = as_of.unwrap_or_else(|| read.since().clone());

                // Eagerly downgrade our frontier to the initial as_of. This makes sure
                // that the output frontier of the `persist_source` closely tracks the
                // `upper` frontier of the persist shard. It might be that the snapshot
                // for `as_of` is not initially available yet, but this makes sure we
                // already downgrade to it.
                //
                // Downstream consumers might rely on close frontier tracking for making
                // progress. For example, the `persist_sink` needs to know the
                // up-to-date upper of the output shard to make progress because it will
                // only write out new data once it knows that earlier writes went
                // through, including the initial downgrade of the shard upper to the
                // `as_of`.
                //
                // NOTE: We have to do this before our `snapshot()` call because that
                // will block when there is no data yet available in the shard.
                if msg_tx.send(ListenMessage::AsOf(as_of.clone())).is_err() {
                    return;
                }
                activator.activate();

                let mut snapshot_parts = match snapshot_mode {
                    SnapshotMode::Include => match read.snapshot(as_of.clone()).await {
                        Ok(parts) => parts,
                        Err(e) => {
                            error(
                                anyhow!(
                                    "{name}: {shard_id} cannot serve requested as_of {as_of:?}: {e:?}"
                                ),
                                &msg_tx,
                                &activator,
                            );
                            return;
                        }
                    },
                    SnapshotMode::Exclude => vec![],
                };

                let mut listen = match read.listen(as_of.clone()).await {
                    Ok(handle) => handle,
                    Err(e) => {
                        error(
                            anyhow!(
                                "{name}: {shard_id} cannot serve requested as_of {as_of:?}: {e:?}"
                            ),
                            &msg_tx,
                            &activator,
                        );
                        return;
                    }
                };

                let listen_retry = listen_sleep.as_ref().map(|retry| retry());

                // The head of the stream is enriched with the snapshot parts if they exist
                let listen_head = if !snapshot_parts.is_empty() {
                    let (mut parts, progress) = listen.next(listen_retry).await;
                    snapshot_parts.append(&mut parts);
                    futures::stream::iter(Some((snapshot_parts, progress)))
                } else {
                    futures::stream::iter(None)
                };

                // The tail of the stream is all subsequent parts
                let listen_tail = futures::stream::unfold(&mut listen, |listen| async move {
                    Some((listen.next(listen_retry).await, listen))
                });

                let mut shard_stream = pin!(listen_head.chain(listen_tail));

                // Ideally, we'd like our audit overhead to be proportional to the actual amount
                // of "real" work we're doing in the source. So: start with a small, constant
                // budget; add to the budget when we do real work; and skip auditing a part if we
                // don't have the budget for it.
                let mut audit_budget_bytes =
                    u64::cast_from(BLOB_TARGET_SIZE.get(&cfg).saturating_mul(2));

                // All future updates will be timestamped after this frontier.
                let mut current_frontier = as_of.clone();

                // If `until.less_equal(current_frontier)`, it means that all subsequent batches
                // will contain only times greater or equal to `until`, which means they can be
                // dropped in their entirety.
                while !PartialOrder::less_equal(&until, &current_frontier) {
                    let (parts, progress) = shard_stream.next().await.expect("infinite stream");

                    // Emit the part at the `(ts, 0)` time. The `granular_backpressure`
                    // operator will refine this further, if its enabled.
                    let current_ts = current_frontier
                        .as_option()
                        .expect("until should always be <= the empty frontier");

                    let mut out = Vec::with_capacity(parts.len());
                    for mut part_desc in parts {
                        // TODO: Push more of this logic into LeasedBatchPart like we've
                        // done for project?
                        if STATS_FILTER_ENABLED.get(&cfg) {
                            let filter_result = match &part_desc.part {
                                BatchPart::Hollow(x) => {
                                    let should_fetch =
                                        x.stats.as_ref().map_or(FilterResult::Keep, |stats| {
                                            filter_fn(&stats.decode(), current_frontier.borrow())
                                        });
                                    should_fetch
                                }
                                BatchPart::Inline { .. } => FilterResult::Keep,
                            };
                            // Apply the filter: discard or substitute the part if required.
                            let bytes = u64::cast_from(part_desc.encoded_size_bytes());
                            match filter_result {
                                FilterResult::Keep => {
                                    audit_budget_bytes = audit_budget_bytes.saturating_add(bytes);
                                }
                                FilterResult::Discard => {
                                    metrics.pushdown.parts_filtered_count.inc();
                                    metrics.pushdown.parts_filtered_bytes.inc_by(bytes);
                                    let should_audit = match &part_desc.part {
                                        BatchPart::Hollow(x) => {
                                            let mut h = DefaultHasher::new();
                                            x.key.hash(&mut h);
                                            usize::cast_from(h.finish()) % 100
                                                < STATS_AUDIT_PERCENT.get(&cfg)
                                        }
                                        BatchPart::Inline { .. } => false,
                                    };
                                    if should_audit && bytes < audit_budget_bytes {
                                        audit_budget_bytes -= bytes;
                                        metrics.pushdown.parts_audited_count.inc();
                                        metrics.pushdown.parts_audited_bytes.inc_by(bytes);
                                        part_desc.request_filter_pushdown_audit();
                                    } else {
                                        debug!(
                                            "skipping part because of stats filter {:?}",
                                            part_desc.part.stats()
                                        );
                                        continue;
                                    }
                                }
                                FilterResult::ReplaceWith { key, val } => {
                                    part_desc.maybe_optimize(&cfg, key, val);
                                    audit_budget_bytes = audit_budget_bytes.saturating_add(bytes);
                                }
                            }
                            let bytes = u64::cast_from(part_desc.encoded_size_bytes());
                            if part_desc.part.is_inline() {
                                metrics.pushdown.parts_inline_count.inc();
                                metrics.pushdown.parts_inline_bytes.inc_by(bytes);
                            } else {
                                metrics.pushdown.parts_fetched_count.inc();
                                metrics.pushdown.parts_fetched_bytes.inc_by(bytes);
                            }
                        }

                        // Give the part to a random worker. This isn't round robin in an attempt
                        // to avoid skew issues: if your parts alternate size large, small, then
                        // you'll end up only using half of your workers.
                        //
                        // There's certainly some other things we could be doing instead here, but
                        // this has seemed to work okay so far. Continue to revisit as necessary.
                        let worker_idx = usize::cast_from(Instant::now().hashed()) % num_workers;
                        let (part, lease) = part_desc.into_exchangeable_part();
                        out.push((worker_idx, part, lease));
                    }

                    if !out.is_empty() {
                        let msg = ListenMessage::Parts {
                            ts: current_ts.clone(),
                            parts: out,
                        };
                        if msg_tx.send(msg).is_err() {
                            return;
                        }
                    }

                    current_frontier.join_assign(&progress);
                    if msg_tx.send(ListenMessage::Progress(progress)).is_err() {
                        return;
                    }
                    activator.activate();
                }

                // Signal completion: all subsequent parts would be filtered by `until`.
                let _ = msg_tx.send(ListenMessage::Progress(Antichain::new()));
                activator.activate();

                // Keep the listen handle (and with it the reader's seqno hold, which protects
                // the leased parts from GC) alive until the operator signals that all fetches
                // have completed; `listen` only drops when this task exits. If the operator is
                // dropped instead, this task is aborted and the handles are dropped with it.
                let _ = shutdown_rx.await;
            }
        })
        .abort_on_drop();

        (msg_rx, Some(shutdown_tx), activation_ack, task)
    });

    let (shutdown_handle, shutdown_button) = button(scope, info.address);

    builder.build(move |capabilities| {
        let [cap]: [_; 1] = capabilities.try_into().expect("one capability per output");
        // Only the chosen worker produces parts; the others hold no
        // capabilities.
        let mut cap_set = if worker_index == chosen_worker {
            CapabilitySet::from_elem(cap)
        } else {
            trace!(
                "We are not the chosen worker ({}), exiting...",
                chosen_worker
            );
            CapabilitySet::new()
        };
        // Leases for parts that have been emitted but whose fetch has not yet
        // completed, keyed by the timestamp they were emitted at. Advanced by
        // the completed fetches frontier.
        let mut leases = LeaseManager::new();
        let mut chosen_state = chosen_state;
        // Set once `error_handler` has been notified: the operator stops doing
        // work but retains its capabilities so the frontier does not advance.
        let mut failed = false;

        move |frontiers| {
            // Drain the completed fetches input. It carries no data
            // (`Infallible`); only its frontier matters.
            completed_fetches.for_each(|_cap, _data| {});

            if shutdown_handle.local_pressed() {
                cap_set = CapabilitySet::new();
                chosen_state = None;
                return;
            }

            let Some((msg_rx, shutdown_tx, activation_ack, task)) = chosen_state.as_mut() else {
                return;
            };
            // Keep the listen task alive for as long as the operator runs.
            let _ = &task;
            activation_ack.ack();

            // Apply the completed fetches frontier to the leases.
            let completed_frontier = frontiers[0].frontier();
            leases.advance_to(completed_frontier);
            if completed_frontier.is_empty() {
                // All fetches have completed; allow the listen task to drop
                // the listen handle and exit.
                if let Some(tx) = shutdown_tx.take() {
                    let _ = tx.send(());
                }
            }

            if failed {
                return;
            }

            // Drain messages from the listen task.
            loop {
                match msg_rx.try_recv() {
                    Ok(ListenMessage::AsOf(as_of)) => {
                        cap_set.downgrade(as_of.iter());
                    }
                    Ok(ListenMessage::Parts { ts, parts }) => {
                        let session_cap = cap_set.delayed(&ts);
                        let mut output = descs_output.activate();
                        let mut session = output.session(&session_cap);
                        for (worker_idx, part, lease) in parts {
                            leases.push_at(ts.clone(), lease);
                            session.give((worker_idx, part));
                        }
                    }
                    Ok(ListenMessage::Progress(progress)) => {
                        cap_set.downgrade(progress.iter());
                    }
                    Ok(ListenMessage::Error(e)) => {
                        // Report the error and freeze: capabilities are
                        // retained so that no spurious progress is observable
                        // while a restart is pending.
                        error_handler.report_and_freeze(e);
                        failed = true;
                        break;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }
        }
    });

    (descs_stream, shutdown_button.press_on_drop())
}

pub(crate) fn shard_source_fetch<'inner, K, V, T, D, TInner>(
    descs: StreamVec<'inner, TInner, (usize, ExchangeableBatchPart<T>)>,
    name: &str,
    client: impl Future<Output = PersistClient> + Send + 'static,
    shard_id: ShardId,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    is_transient: bool,
    error_handler: ErrorHandler,
) -> (
    StreamVec<'inner, TInner, FetchedBlob<K, V, T, D>>,
    StreamVec<'inner, TInner, Infallible>,
    PressOnDropButton,
)
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Monoid + Codec64 + Send + Sync,
    TInner: Timestamp + Refines<T>,
{
    let scope = descs.scope();
    let mut builder =
        OperatorBuilderRc::new(format!("shard_source_fetch({})", name), scope.clone());
    let info = builder.operator_info();
    // NB: create the outputs before the input, so that the input's default
    // connection covers both outputs.
    let (fetched_output, fetched_stream) = builder.new_output::<Vec<FetchedBlob<K, V, T, D>>>();
    let mut fetched_output = OutputBuilder::from(fetched_output);
    let (_completed_fetches_output, completed_fetches_stream) =
        builder.new_output::<Vec<Infallible>>();
    let mut descs_input = builder.new_input(
        descs,
        Exchange::new(|&(i, _): &(usize, _)| u64::cast_from(i)),
    );
    let name_owned = name.to_owned();

    // Channels between the operator and the fetch task: descs flow to the
    // task, fetch results flow back. The task wakes the operator through the
    // activator after each result.
    let (desc_tx, mut desc_rx) = tokio::sync::mpsc::unbounded_channel::<ExchangeableBatchPart<T>>();
    let (blob_tx, blob_rx) =
        tokio::sync::mpsc::unbounded_channel::<Result<FetchedBlob<K, V, T, D>, (BlobKey, String)>>();
    let (activator, activation_ack) = ArcActivator::new(scope.clone(), &info);

    // The fetch task owns the `BatchFetcher` and performs all async work:
    // fetcher creation and the per-part downloads. `fetch_leased_part`
    // internally acquires permits from the persist fetch semaphore before
    // downloading, which is what bounds fetched-but-undecoded bytes; the
    // permit travels inside the returned `FetchedBlob` and is released when
    // decode retires the part.
    let task = mz_ore::task::spawn(|| format!("shard_source_fetch({})", name_owned), {
        let diagnostics = Diagnostics {
            shard_name: name_owned.clone(),
            handle_purpose: format!("shard_source_fetch batch fetcher {}", name_owned),
        };
        async move {
            let mut fetcher = client
                .await
                .create_batch_fetcher::<K, V, T, D>(
                    shard_id,
                    key_schema,
                    val_schema,
                    is_transient,
                    diagnostics,
                )
                .await
                .expect("shard codecs should not change");
            while let Some(part) = desc_rx.recv().await {
                let reader_id = part.reader_id().clone();
                let fetched = fetcher
                    .fetch_leased_part(part)
                    .await
                    .expect("shard_id should match across all workers");
                let fetched = match fetched {
                    Ok(fetched) => Ok(fetched),
                    Err(blob_key) => {
                        // Check the state of the minting reader's lease so the
                        // operator can tell a lease expiry apart from a GC bug.
                        let diagnostics = fetcher.missing_blob_diagnostics(&reader_id).await;
                        Err((blob_key, diagnostics))
                    }
                };
                if blob_tx.send(fetched).is_err() {
                    // The operator is gone; we can stop fetching.
                    return;
                }
                activator.activate();
            }
        }
    })
    .abort_on_drop();

    let (shutdown_handle, shutdown_button) = button(scope, info.address);

    builder.build(move |_capabilities| {
        // Per-flight capabilities, in the order parts were sent to the fetch
        // task (which processes and returns them in the same order). The first
        // capability emits the fetched blob; dropping the second advances the
        // `completed_fetches` frontier, which releases the part's lease on the
        // chosen worker. Holding both until the fetch completes is what keeps
        // the lease alive while the download is in flight.
        let mut inflight_caps: VecDeque<(Capability<TInner>, Capability<TInner>)> = VecDeque::new();
        // Wrapped in `Option` so we can drop the sender to signal the task
        // that no more descs are coming.
        let mut desc_tx = Some(desc_tx);
        let mut blob_rx = Some(blob_rx);
        // Set once `error_handler` has been notified of an error: the operator
        // stops doing work but intentionally retains its capabilities, so the
        // frontier does not advance and downstream consumers do not observe
        // spurious progress while a restart is pending.
        let mut failed = false;

        move |frontiers| {
            // Keep the fetch task alive for as long as the operator runs.
            let _ = &task;
            activation_ack.ack();

            if shutdown_handle.local_pressed() {
                // Shutdown: drop all capabilities and disconnect from the
                // task. In-flight fetches are abandoned; the task exits when
                // it observes the closed desc channel (or is aborted when the
                // operator, and with it the `AbortOnDropHandle`, is dropped).
                inflight_caps.clear();
                desc_tx = None;
                blob_rx = None;
                // Still drain the input to avoid stalling the dataflow.
                descs_input.for_each(|_cap, _data| {});
                return;
            }

            // Forward incoming descs to the fetch task, retaining a capability
            // pair for each.
            descs_input.for_each(|cap, data| {
                for (_idx, part) in data.drain(..) {
                    if failed {
                        continue;
                    }
                    let fetched_cap = cap.delayed(cap.time(), 0);
                    let completed_cap = cap.delayed(cap.time(), 1);
                    inflight_caps.push_back((fetched_cap, completed_cap));
                    desc_tx
                        .as_ref()
                        .expect("desc_tx alive while operator is running")
                        .send(part)
                        .expect("fetch task unexpectedly gone");
                }
            });

            // Drain completed fetches, emitting each at its retained
            // capability.
            if let Some(rx) = blob_rx.as_mut() {
                loop {
                    match rx.try_recv() {
                        Ok(Ok(fetched)) => {
                            let (fetched_cap, _completed_cap) = inflight_caps
                                .pop_front()
                                .expect("capability for every in-flight fetch");
                            fetched_output
                                .activate()
                                .session(&fetched_cap)
                                .give(fetched);
                            // `_completed_cap` drops here, advancing the
                            // `completed_fetches` frontier past this part.
                        }
                        Ok(Err((blob_key, diagnostics))) => {
                            // Ideally, readers should never encounter a missing blob. They place a seqno
                            // hold as they consume their snapshot/listen, preventing any blobs they need
                            // from being deleted by garbage collection, and all blob implementations are
                            // linearizable so there should be no possibility of stale reads.
                            //
                            // However, it is possible for a lease to expire given a sustained period of
                            // downtime, which could allow parts we expect to exist to be deleted...
                            // at which point our best option is to request a restart, with the lease
                            // state of the minting reader attached to tell the two cases apart. We
                            // report the error and freeze: capabilities are retained so the frontier
                            // cannot advance past the missing part.
                            error_handler.report_and_freeze(anyhow!(
                                "batch fetcher could not fetch batch part {}: {}",
                                blob_key,
                                diagnostics
                            ));
                            failed = true;
                            break;
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            // The task exits only after the desc channel closes (which
                            // we haven't done) or a panic; in either case, with fetches
                            // outstanding this is unexpected.
                            assert!(
                                inflight_caps.is_empty(),
                                "fetch task unexpectedly gone with {} fetches in flight",
                                inflight_caps.len()
                            );
                            break;
                        }
                    }
                }
            }

            // Once the input is closed and nothing is in flight, disconnect
            // from the task so it can exit.
            if frontiers[0].frontier().is_empty() && inflight_caps.is_empty() {
                desc_tx = None;
            }
        }
    });

    (
        fetched_stream,
        completed_fetches_stream,
        shutdown_button.press_on_drop(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use mz_persist::location::SeqNo;
    use timely::dataflow::operators::Leave;
    use timely::dataflow::operators::Probe;
    use timely::dataflow::operators::probe::Handle as ProbeHandle;
    use timely::progress::Antichain;

    use crate::operators::shard_source::shard_source;
    use crate::{Diagnostics, ShardId};

    #[mz_ore::test]
    fn test_lease_manager() {
        let lease = Lease::new(SeqNo::minimum());
        let mut manager = LeaseManager::new();
        for t in 0u64..10 {
            manager.push_at(t, lease.clone());
        }
        assert_eq!(lease.count(), 11);
        manager.advance_to(AntichainRef::new(&[5]));
        assert_eq!(lease.count(), 6);
        manager.advance_to(AntichainRef::new(&[3]));
        assert_eq!(lease.count(), 6);
        manager.advance_to(AntichainRef::new(&[9]));
        assert_eq!(lease.count(), 2);
        manager.advance_to(AntichainRef::new(&[10]));
        assert_eq!(lease.count(), 1);
    }

    /// Verifies that a `shard_source` will downgrade it's output frontier to
    /// the `since` of the shard when no explicit `as_of` is given. Even if
    /// there is no data/no snapshot available in the
    /// shard.
    ///
    /// NOTE: This test is weird: if everything is good it will pass. If we
    /// break the assumption that we test this will time out and we will notice.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn test_shard_source_implicit_initial_as_of() {
        let persist_client = PersistClient::new_for_tests().await;

        let expected_frontier = 42;
        let shard_id = ShardId::new();

        initialize_shard(
            &persist_client,
            shard_id,
            Antichain::from_elem(expected_frontier),
        )
        .await;

        let res = timely::execute::execute_directly(move |worker| {
            let until = Antichain::new();

            let (probe, _token) = worker.dataflow::<u64, _, _>(|outer| {
                let (stream, token) = outer.scoped::<u64, _, _>("hybrid", |scope| {
                    let transformer = move |_, descs, _| (descs, vec![]);
                    let (stream, tokens) = shard_source::<String, String, u64, u64, _, _, _>(
                        outer,
                        scope,
                        "test_source",
                        move || std::future::ready(persist_client.clone()),
                        shard_id,
                        None, // No explicit as_of!
                        SnapshotMode::Include,
                        until,
                        Some(transformer),
                        Arc::new(
                            <std::string::String as mz_persist_types::Codec>::Schema::default(),
                        ),
                        Arc::new(
                            <std::string::String as mz_persist_types::Codec>::Schema::default(),
                        ),
                        FilterResult::keep_all,
                        false.then_some(|| unreachable!()),
                        async {},
                        ErrorHandler::Halt("test"),
                    );
                    (stream.leave(outer), tokens)
                });

                let probe = ProbeHandle::new();
                let _stream = stream.probe_with(&probe);

                (probe, token)
            });

            while probe.less_than(&expected_frontier) {
                worker.step();
            }

            let mut probe_frontier = Antichain::new();
            probe.with_frontier(|f| probe_frontier.extend(f.iter().cloned()));

            probe_frontier
        });

        assert_eq!(res, Antichain::from_elem(expected_frontier));
    }

    /// Verifies that a `shard_source` will downgrade it's output frontier to
    /// the given `as_of`. Even if there is no data/no snapshot available in the
    /// shard.
    ///
    /// NOTE: This test is weird: if everything is good it will pass. If we
    /// break the assumption that we test this will time out and we will notice.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn test_shard_source_explicit_initial_as_of() {
        let persist_client = PersistClient::new_for_tests().await;

        let expected_frontier = 42;
        let shard_id = ShardId::new();

        initialize_shard(
            &persist_client,
            shard_id,
            Antichain::from_elem(expected_frontier),
        )
        .await;

        let res = timely::execute::execute_directly(move |worker| {
            let as_of = Antichain::from_elem(expected_frontier);
            let until = Antichain::new();

            let (probe, _token) = worker.dataflow::<u64, _, _>(|outer| {
                let (stream, token) = outer.scoped::<u64, _, _>("hybrid", |scope| {
                    let transformer = move |_, descs, _| (descs, vec![]);
                    let (stream, tokens) = shard_source::<String, String, u64, u64, _, _, _>(
                        outer,
                        scope,
                        "test_source",
                        move || std::future::ready(persist_client.clone()),
                        shard_id,
                        Some(as_of), // We specify the as_of explicitly!
                        SnapshotMode::Include,
                        until,
                        Some(transformer),
                        Arc::new(
                            <std::string::String as mz_persist_types::Codec>::Schema::default(),
                        ),
                        Arc::new(
                            <std::string::String as mz_persist_types::Codec>::Schema::default(),
                        ),
                        FilterResult::keep_all,
                        false.then_some(|| unreachable!()),
                        async {},
                        ErrorHandler::Halt("test"),
                    );
                    (stream.leave(outer), tokens)
                });

                let probe = ProbeHandle::new();
                let _stream = stream.probe_with(&probe);

                (probe, token)
            });

            while probe.less_than(&expected_frontier) {
                worker.step();
            }

            let mut probe_frontier = Antichain::new();
            probe.with_frontier(|f| probe_frontier.extend(f.iter().cloned()));

            probe_frontier
        });

        assert_eq!(res, Antichain::from_elem(expected_frontier));
    }

    async fn initialize_shard(
        persist_client: &PersistClient,
        shard_id: ShardId,
        since: Antichain<u64>,
    ) {
        let mut read_handle = persist_client
            .open_leased_reader::<String, String, u64, u64>(
                shard_id,
                Arc::new(<std::string::String as mz_persist_types::Codec>::Schema::default()),
                Arc::new(<std::string::String as mz_persist_types::Codec>::Schema::default()),
                Diagnostics::for_tests(),
                true,
            )
            .await
            .expect("invalid usage");

        read_handle.downgrade_since(&since).await;
    }
}
