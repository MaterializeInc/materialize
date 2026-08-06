// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A source that reads from a persist shard.
//!
//! # For consumers
//!
//! [shard_source] renders a set of timely operators that continuously read a
//! persist shard and emit its contents as a stream of [FetchedBlob]s: parts
//! that have been downloaded from blob storage but not yet decoded. Callers
//! decode them (e.g. via [FetchedBlob::parse]) downstream, typically on the
//! same worker, so that decode cost scales with the number of workers.
//!
//! The source observes the following contract:
//!
//! * **Times**: all emitted times are advanced by the given `as_of`. With
//!   [SnapshotMode::Include] the shard contents as of `as_of` are emitted at
//!   the `as_of`; with [SnapshotMode::Exclude] only subsequent updates are.
//! * **Frontier**: the output frontier tracks the shard's `upper`, eagerly
//!   downgraded to the `as_of` before the snapshot is available so downstream
//!   consumers (e.g. `persist_sink`) can rely on close frontier tracking. When
//!   `until` is non-empty, parts lying entirely at or beyond `until` are
//!   dropped and the source eventually completes; fine-grained filtering of
//!   individual updates against `until` is the caller's responsibility.
//! * **Distribution**: parts are distributed across all workers, regardless of
//!   which worker coordinates the read.
//! * **Filter pushdown**: `filter_fn` is consulted with each part's stats and
//!   may keep the part, discard it without fetching, or replace its contents
//!   with a single-row constant ([FilterResult]). A random sample of discarded
//!   parts is fetched anyway to audit the decision.
//! * **Errors**: conditions that are neither data-plane errors nor bugs (an
//!   unserveable `as_of`, a missing blob after a lease timeout) are reported to
//!   the given [ErrorHandler]. The source then freezes: it stops doing work but
//!   retains its capabilities, so the frontier never advances past unproduced
//!   data while a halt or dataflow restart is pending.
//! * **Shutdown**: dropping the returned [PressOnDropButton] tokens shuts the
//!   source down, dropping all held capabilities and abandoning in-flight work.
//!
//! # For implementors
//!
//! The source is split into two synchronous timely operators, each paired with
//! a tokio task that owns all async persist work. Operators and tasks
//! communicate over channels; tasks wake their operator through a
//! [SyncActivator]-backed [ArcActivator], which also unparks a parked worker.
//!
//! ```text
//!  tokio: listen task ──(parts+leases, progress, mpsc)──> [shard_source_descs]
//!  (chosen worker only)                                        │ exchange by assigned worker
//!         ▲ oneshot: drop listen                               ▼
//!         └──────────────────────────────────────────── [shard_source_fetch] (per worker)
//!                            completed_fetches feedback        │ ▲
//!                                                       desc   │ │ FetchedBlob
//!                                                       (mpsc) ▼ │ (mpsc)
//!                                                    tokio: fetch task (per worker)
//! ```
//!
//! **`shard_source_descs`** runs on all workers, but only the chosen worker
//! (hash of the name) spawns a listen task and holds capabilities. The task
//! opens a leased reader, waits for the `start_signal`, resolves the `as_of`,
//! and walks snapshot + listen, applying `filter_fn` and the audit budget. It
//! splits each [LeasedBatchPart] into an [ExchangeableBatchPart] plus its
//! [Lease] and sends both to the operator, along with progress updates that
//! drive the operator's capability downgrades. The operator emits
//! `(worker_idx, part)` pairs — exchanged by index — and parks each lease in a
//! `LeaseManager` keyed by the part's time. The lease-return input (formerly
//! the separate `shard_source_descs_return` operator) is merged in as a
//! disconnected `completed_fetches` input.
//!
//! **`shard_source_fetch`** forwards each incoming desc to its fetch task,
//! tagged with the time it was minted at, and retains a capability pair (data
//! output + `completed_fetches` output) per time, counting how many fetches at
//! that time are outstanding. The task echoes the time back with each result;
//! the operator emits the [FetchedBlob] at that time's data capability and,
//! when a time's outstanding count reaches zero, drops both of its capabilities
//! — advancing the data frontier and releasing that time's leases. Keying by
//! time rather than arrival order makes the operator independent of the order
//! results come back in.
//!
//! **Lease lifecycle**: dropping the completed-fetches capability advances a
//! feedback loop back into `shard_source_descs`, whose frontier advances the
//! `LeaseManager` and drops the leases for fetched parts. The listen task — and
//! with it the reader and its SeqNo hold, which is what actually protects
//! unfetched parts' blobs from GC — must outlive all in-flight fetches: the
//! operator releases it through a oneshot only once the completed-fetches
//! frontier is empty. If the dataflow is dropped instead, the tasks are aborted
//! via their [AbortOnDropHandle]s.
//!
//! **Two-phase shutdown**: both operators return a [PressOnDropButton] and so
//! participate in `builder_async`'s coordinated shutdown. Their schedule
//! closures use `build_reschedule` and only drop capabilities / drain inputs
//! once *all* workers have pressed (`all_pressed`). Dropping capabilities on a
//! local-only press would let the downstream frontier advance past times that
//! other workers' instances still feed (cross-worker teardown skew).
//!
//! Subtleties worth knowing before changing this code:
//!
//! * [LeasedBatchPart]s panic on drop while leased; only the lease-split
//!   [ExchangeableBatchPart] representation may cross channels, where it can be
//!   dropped harmlessly on shutdown.
//! * Each result carries its own time, so the operator does not depend on the
//!   order the task returns them. Once a fetch fails the operator freezes and
//!   must STOP draining results — a later, good result would otherwise release
//!   a capability and advance the
//!   frontier past data never emitted.
//! * The fetch task's result channel is bounded: the task downloads ahead of
//!   the operator, so an unbounded channel would let fetched blobs pile up
//!   without limit while Timely is busy. The bound makes the task block in
//!   `send` once the operator falls behind, restoring the implicit limit the
//!   async operator had (its future only advanced when Timely scheduled it).
//! * The `completed_fetches` feedback edge carries no data (`Infallible`); it
//!   exists for its frontier, which signals cross-process fetch completion,
//!   wakes the descs operator, and keeps that operator alive (and thus the
//!   listen task's SeqNo hold) until all fetches finish.
//! * Tests that step workers manually must not park indefinitely while polling
//!   state that does not activate the worker: with the listen in a task, a
//!   caught-up source with no listen retry timer produces no activations.
//!
//! [SyncActivator]: timely::scheduling::SyncActivator
//! [ArcActivator]: mz_timely_util::activator::ArcActivator
//! [LeasedBatchPart]: crate::fetch::LeasedBatchPart
//! [AbortOnDropHandle]: mz_ore::task::AbortOnDropHandle

use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
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
use futures::stream::FuturesUnordered;
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
use crate::cfg::{
    RetryParameters, SOURCE_FETCH_CONCURRENCY, SOURCE_HYDRATION_FRONTIER_COALESCE_BYTES,
    USE_CRITICAL_SINCE_SOURCE,
};
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
    // See the module documentation for the structure of this source: a descs
    // operator (with a listen task minting parts and leases on one chosen
    // worker) distributing parts to a fetch operator (with a fetch task
    // downloading their contents) on each worker, with a feedback loop of
    // completed fetches that releases the leases.

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

/// A message from the listen task to the [shard_source_descs] operator.
enum ListenMessage<T> {
    /// The resolved `as_of`; the operator downgrades its capabilities to it.
    AsOf(Antichain<T>),
    /// Parts minted at `ts`, each with the lease that protects it from GC.
    Parts {
        ts: T,
        parts: Vec<(usize, ExchangeableBatchPart<T>, Lease)>,
    },
    /// Listen progress; the operator downgrades its capabilities. The empty
    /// antichain indicates the listen is complete (`until` was reached).
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
    // empty connection below disconnects it from the right output port.
    let (descs_output, descs_stream) =
        builder.new_output::<Vec<(usize, ExchangeableBatchPart<TOuter>)>>();
    let mut descs_output = OutputBuilder::from(descs_output);
    // The completed fetches input is disconnected from the output: it only
    // drives lease returns (merging in what used to be the separate
    // `shard_source_descs_return` operator), not output progress.
    let mut completed_fetches =
        builder.new_input_connection(completed_fetches_stream, Pipeline, []);

    // Only the chosen worker produces parts. It spawns a listen task that owns
    // all async work (reader, snapshot, listen loop, stats-based filtering) and
    // communicates with the operator over a channel. The other workers build
    // the same operator shape but hold no capabilities.
    let chosen_state = (worker_index == chosen_worker).then(|| {
        let (msg_tx, msg_rx) = tokio::sync::mpsc::unbounded_channel::<ListenMessage<TOuter>>();
        // Fired by the operator once the completed-fetches frontier is empty,
        // i.e. all fetches are done. The task holds the listen handle (and with
        // it the reader's seqno hold) until then.
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let (activator, activation_ack) = ArcActivator::new(scope.clone(), &info);

        let task = mz_ore::task::spawn(|| format!("shard_source_descs({})", name_owned), {
            let name = name_owned.clone();
            // Report a fatal error to the operator and stop the task.
            let error = |e: anyhow::Error,
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

                // Recent shard upper observed at hydration time. While the source is
                // still catching up to it we coalesce frontier downgrades (see the loop
                // below); once `current_frontier` reaches it the source is live and we
                // forward every batch's progress so steady-state frontier tracking stays
                // tight. Read before `listen` consumes `read`.
                let replay_upper = read.shared_upper();

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

                // While catching up to `replay_upper`, coalesce frontier downgrades until
                // at least this many encoded bytes have been emitted at the held
                // capability. This turns a long historical replay (one persist batch ~ one
                // write ~ 1/s) from one progress round per batch into a handful of larger
                // steps, which is what bounds the number of downstream
                // arrangement-maintenance passes. `0` disables it. The budget caps how much
                // the downstream batcher stages before it can seal, so we never trade the
                // per-batch storm for an unbounded single batch.
                let coalesce_target =
                    u64::cast_from(SOURCE_HYDRATION_FRONTIER_COALESCE_BYTES.get(&cfg));
                // Encoded bytes emitted since the last forwarded progress.
                let mut coalesced_bytes: u64 = 0;

                // If `until.less_equal(current_frontier)`, it means that all subsequent batches
                // will contain only times greater or equal to `until`, which means they can be
                // dropped in their entirety.
                while !PartialOrder::less_equal(&until, &current_frontier) {
                    let (parts, progress) = shard_stream.next().await.expect("infinite stream");

                    let current_ts = current_frontier
                        .as_option()
                        .expect("until should always be <= the empty frontier");

                    let mut out = Vec::with_capacity(parts.len());
                    let mut batch_bytes: u64 = 0;
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
                        batch_bytes = batch_bytes
                            .saturating_add(u64::cast_from(part_desc.encoded_size_bytes()));
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
                    coalesced_bytes = coalesced_bytes.saturating_add(batch_bytes);

                    // Coalesce the frontier downgrade while still catching up to
                    // `replay_upper` and below the byte budget. Parts carry their real
                    // timestamps regardless (they were already sent above), so holding the
                    // frontier back only batches downstream progress rounds. Once live
                    // (caught up to `replay_upper`) or with coalescing disabled we forward
                    // every batch, keeping steady-state tracking tight for consumers like
                    // `persist_sink`.
                    let caught_up = PartialOrder::less_equal(&replay_upper, &current_frontier);
                    let coalesce =
                        coalesce_target > 0 && !caught_up && coalesced_bytes < coalesce_target;
                    if !coalesce {
                        coalesced_bytes = 0;
                        if msg_tx
                            .send(ListenMessage::Progress(current_frontier.clone()))
                            .is_err()
                        {
                            return;
                        }
                    }

                    // Activate every iteration so the operator drains and emits parts even
                    // while a progress downgrade is being withheld.
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

    let (mut shutdown_handle, shutdown_button) = button(scope, info.address);

    builder.build_reschedule(move |capabilities| {
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
            // Two-phase shutdown, mirroring `builder_async`: only once *all*
            // workers have pressed do we drop capabilities and drain inputs.
            // Dropping caps on a local-only press would let the downstream
            // frontier advance past times other workers' instances still feed.
            if shutdown_handle.local_pressed() {
                return if shutdown_handle.all_pressed() {
                    cap_set = CapabilitySet::new();
                    chosen_state = None;
                    completed_fetches.for_each(|_cap, _data| {});
                    false
                } else {
                    // Local press only: wedge. Keep capabilities and leave the
                    // input undrained so its pending messages hold the frontier.
                    true
                };
            }

            // Drain the completed fetches input. It carries no data
            // (`Infallible`); only its frontier matters.
            completed_fetches.for_each(|_cap, _data| {});

            let Some((msg_rx, shutdown_tx, activation_ack, task)) = chosen_state.as_mut() else {
                // Non-chosen workers have nothing to do.
                return true;
            };
            // Keep the listen task alive for as long as the operator runs.
            let _ = &task;
            activation_ack.ack();

            // Apply the completed fetches frontier to the leases.
            let completed_frontier = frontiers[0].frontier();
            leases.advance_to(completed_frontier);
            if completed_frontier.is_empty() {
                // All fetches have completed; allow the listen task to drop the
                // listen handle and exit.
                if let Some(tx) = shutdown_tx.take() {
                    let _ = tx.send(());
                }
            }

            if failed {
                // Frozen: retain capabilities so the frontier does not advance.
                // Every error path in the listen task sends `Error` as its final
                // message and returns, so `msg_rx` is empty forever once we get
                // here; no need to keep draining it.
                return true;
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
                        // Report the error and freeze: capabilities are retained
                        // so that no spurious progress is observable while a
                        // restart is pending.
                        error_handler.report_and_freeze(e);
                        failed = true;
                        break;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }

            // Keep the operator alive; it is torn down via the shutdown button.
            true
        }
    });

    (descs_stream, shutdown_button.press_on_drop())
}

/// Capacity of the bounded channel carrying fetch results from the fetch task
/// back to [shard_source_fetch]. Small, to keep fetched-but-undrained blobs
/// bounded when Timely is busy, while leaving room for a little fetch/decode
/// pipelining. See the channel's construction for the full rationale.
const FETCH_RESULT_CHANNEL_CAPACITY: usize = 4;

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

    // Channels between the operator and the fetch task: descs flow to the task,
    // fetch results flow back. On a missing blob the task attaches the minting
    // reader's lease diagnostics so the operator can distinguish a lease expiry
    // from a GC bug. The task wakes the operator through the activator after
    // each result.
    //
    // Each desc is tagged with the time it was minted at; the task echoes that
    // time back with the result so the operator can emit at the right
    // capability without relying on the order results come back in.
    //
    // The result channel is *bounded*: the fetch task downloads parts ahead of
    // the operator (which only drains when Timely schedules it), so an unbounded
    // channel would let fetched blobs — the memory-heavy payloads — pile up
    // without limit whenever Timely is busy and Tokio keeps fetching. The bound
    // makes the task block in `send` once the operator falls behind, restoring
    // the implicit "roughly one in flight" limit the async operator had (its
    // future only advanced when Timely scheduled it) while still allowing a
    // little fetch/decode pipelining. (The persist fetch semaphore bounds total
    // in-flight *bytes*; this is the coarser per-operator count bound.)
    let (desc_tx, mut desc_rx) =
        tokio::sync::mpsc::unbounded_channel::<(TInner, ExchangeableBatchPart<T>)>();
    let (blob_tx, blob_rx) = tokio::sync::mpsc::channel::<(
        TInner,
        Result<FetchedBlob<K, V, T, D>, (BlobKey, String)>,
    )>(FETCH_RESULT_CHANNEL_CAPACITY);
    let (activator, activation_ack) = ArcActivator::new(scope.clone(), &info);

    // The fetch task owns the `BatchFetcher` and performs all async work:
    // fetcher creation and the per-part downloads.
    let task = mz_ore::task::spawn(|| format!("shard_source_fetch({})", name_owned), {
        let diagnostics = Diagnostics {
            shard_name: name_owned.clone(),
            handle_purpose: format!("shard_source_fetch batch fetcher {}", name_owned),
        };
        async move {
            let client = client.await;
            // How many part fetches to run at once so blob-store round-trips
            // overlap. `1` restores the serial behavior. Read once at startup;
            // changing it takes effect on the next restart.
            let max_concurrency = SOURCE_FETCH_CONCURRENCY.get(client.dyncfgs()).max(1);
            let fetcher = client
                .create_batch_fetcher::<K, V, T, D>(
                    shard_id,
                    key_schema,
                    val_schema,
                    is_transient,
                    diagnostics,
                )
                .await
                .expect("shard codecs should not change");

            // Fetch one part on its own `BatchFetcher` clone (cheap: the clone
            // shares the schema cache). Echoes the minting time back with the
            // result so the operator emits at the right capability regardless of
            // completion order.
            let fetch_one = |time: TInner, part: ExchangeableBatchPart<T>| {
                let mut fetcher = fetcher.clone();
                async move {
                    let reader_id = part.reader_id().clone();
                    let fetched = fetcher
                        .fetch_leased_part(part)
                        .await
                        .expect("shard_id should match across all workers");
                    let fetched = match fetched {
                        Ok(fetched) => Ok(fetched),
                        Err(blob_key) => {
                            // Ideally, readers should never encounter a missing blob. They place a
                            // seqno hold as they consume their snapshot/listen, preventing any blobs
                            // they need from being deleted by garbage collection, and all blob
                            // implementations are linearizable so there should be no possibility of
                            // stale reads.
                            //
                            // However, it is possible for a lease to expire given a sustained period
                            // of downtime, which could allow parts we expect to exist to be
                            // deleted... at which point our best option is to request a restart.
                            // Check the state of the minting reader's lease to tell the two cases
                            // apart.
                            let diagnostics = fetcher.missing_blob_diagnostics(&reader_id).await;
                            Err((blob_key, diagnostics))
                        }
                    };
                    (time, fetched)
                }
            };

            // Up to `max_concurrency` fetches run at once. The operator's
            // time-keyed bookkeeping tolerates results completing out of order.
            // In-flight *bytes* stay bounded by the persist fetch semaphore
            // inside `fetch_leased_part`; the bounded result channel bounds
            // fetched-but-undrained blobs.
            let mut in_flight = FuturesUnordered::new();
            let mut input_done = false;
            loop {
                tokio::select! {
                    biased;
                    // Drain a completed fetch and forward it. `send().await`
                    // blocks here once the operator falls behind, which is the
                    // intended backpressure.
                    Some((time, fetched)) = in_flight.next(), if !in_flight.is_empty() => {
                        if blob_tx.send((time, fetched)).await.is_err() {
                            // The operator is gone; stop fetching.
                            return;
                        }
                        activator.activate();
                    }
                    // Start another fetch while there is spare concurrency and
                    // the desc channel is still open.
                    maybe = desc_rx.recv(), if !input_done && in_flight.len() < max_concurrency => {
                        match maybe {
                            Some((time, part)) => in_flight.push(fetch_one(time, part)),
                            None => input_done = true,
                        }
                    }
                    // Input closed and nothing in flight: the task is done.
                    else => return,
                }
            }
        }
    })
    .abort_on_drop();

    let (mut shutdown_handle, shutdown_button) = button(scope, info.address);

    builder.build_reschedule(move |_capabilities| {
        // Outstanding fetches, keyed by the time the part was minted at. For
        // each time we retain a capability on each output (data and
        // completed-fetches) and count how many fetches at that time are in
        // flight. When the count reaches zero we drop both capabilities,
        // advancing the data and completed-fetches frontiers past that time;
        // the latter releases the parts' leases on the chosen worker (whose
        // `LeaseManager` is likewise keyed by time). Keying by time rather than
        // by arrival order makes the operator robust to the task returning
        // results in any order — e.g. if it ever fetches concurrently.
        let mut outstanding: BTreeMap<TInner, (Capability<TInner>, Capability<TInner>, usize)> =
            BTreeMap::new();
        // Wrapped in `Option` so we can drop the sender to signal the task that
        // no more descs are coming.
        let mut desc_tx = Some(desc_tx);
        let mut blob_rx = Some(blob_rx);
        // Set once `error_handler` has been notified of a missing blob: the
        // operator stops doing work but retains its capabilities so the frontier
        // does not advance past data we did not emit.
        let mut failed = false;

        move |frontiers| {
            // Keep the fetch task alive for as long as the operator runs.
            let _ = &task;

            // Two-phase shutdown, mirroring `builder_async`: only once *all*
            // workers have pressed do we drop capabilities and drain the input.
            if shutdown_handle.local_pressed() {
                return if shutdown_handle.all_pressed() {
                    outstanding.clear();
                    desc_tx = None;
                    blob_rx = None;
                    descs_input.for_each(|_cap, _data| {});
                    false
                } else {
                    // Local press only: wedge. Keep capabilities and leave the
                    // input undrained so its pending messages hold the frontier.
                    true
                };
            }

            activation_ack.ack();

            if failed {
                // Frozen: retain every outstanding capability so the frontier
                // stays at the missing part and never advances past data we did
                // not emit. Crucially we must NOT keep draining `blob_rx`: a
                // later, successfully fetched part would otherwise release a
                // capability and let the frontier advance past the missing part.
                // Still drain the input to avoid stalling the dataflow.
                descs_input.for_each(|_cap, _data| {});
                return true;
            }

            // Forward incoming descs to the fetch task, retaining a capability
            // pair per time and counting the fetch as outstanding.
            descs_input.for_each(|cap, data| {
                for (_idx, part) in data.drain(..) {
                    let entry = outstanding.entry(cap.time().clone()).or_insert_with(|| {
                        (cap.delayed(cap.time(), 0), cap.delayed(cap.time(), 1), 0)
                    });
                    entry.2 += 1;
                    desc_tx
                        .as_ref()
                        .expect("desc_tx alive while operator is running")
                        .send((cap.time().clone(), part))
                        .expect("fetch task unexpectedly gone");
                }
            });

            // Drain completed fetches, emitting each at the capability for its
            // time.
            if let Some(rx) = blob_rx.as_mut() {
                loop {
                    match rx.try_recv() {
                        Ok((time, Ok(fetched))) => {
                            let entry = outstanding
                                .get_mut(&time)
                                .expect("capability for every in-flight fetch time");
                            fetched_output.activate().session(&entry.0).give(fetched);
                            entry.2 -= 1;
                            if entry.2 == 0 {
                                // Drops both capabilities, advancing the data and
                                // completed-fetches frontiers past `time`.
                                outstanding.remove(&time);
                            }
                        }
                        Ok((_time, Err((blob_key, diagnostics)))) => {
                            // Report the missing blob and freeze: capabilities are
                            // retained (including this failed part's) so the
                            // frontier cannot advance past the missing part.
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
                            // The task exits only after the desc channel closes
                            // (which we haven't done) or a panic; with fetches
                            // outstanding this is unexpected.
                            assert!(
                                outstanding.is_empty(),
                                "fetch task unexpectedly gone with {} outstanding fetch times",
                                outstanding.len()
                            );
                            break;
                        }
                    }
                }
            }

            // Once the input is closed and nothing is in flight, disconnect from
            // the task so it can exit.
            if frontiers[0].frontier().is_empty() && outstanding.is_empty() {
                desc_tx = None;
            }

            // Keep the operator alive; it is torn down via the shutdown button.
            true
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

    use mz_persist::location::{Blob, SeqNo};
    use mz_persist_types::codec_impls::StringSchema;
    use timely::dataflow::operators::Leave;
    use timely::dataflow::operators::Probe;
    use timely::dataflow::operators::capture::{Capture, Event as CaptureEvent};
    use timely::dataflow::operators::probe::Handle as ProbeHandle;
    use timely::progress::Antichain;

    use crate::batch::{INLINE_WRITES_SINGLE_MAX_BYTES, INLINE_WRITES_TOTAL_MAX_BYTES};
    use crate::cache::PersistClientCache;
    use crate::internal::paths::{BlobKey, PartialBlobKey};
    use crate::operators::shard_source::shard_source;
    use crate::{Diagnostics, PersistLocation, ShardId};

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

    /// Hydrating an index over a shard with many fine-grained batches (the prod
    /// case: a retained-history collection written at ~1/s, whose batches stay
    /// unmerged because the held-back `since` blocks compaction) replays one
    /// progress round per batch. With
    /// `persist_source_hydration_frontier_coalesce_bytes` set, the source holds
    /// those downgrades back while catching up to the hydration-time upper and
    /// forwards them in a few larger steps instead.
    ///
    /// This writes `N_BATCHES` single-timestamp batches (compaction disabled so
    /// they stay distinct, mirroring a held-back `since`) and runs the source
    /// twice over the same shard, counting how many distinct output frontiers it
    /// passes through. Disabled (the default) replays per batch; enabled with a
    /// budget larger than the whole replay collapses it to a single jump. Both
    /// must still reach the same final upper, so coalescing only changes
    /// frontier granularity, not how far the source gets.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn test_shard_source_hydration_frontier_coalesce() {
        const N_BATCHES: u64 = 64;

        // Hydrate the shard from `as_of = 0` with the given coalesce budget and
        // return the largest timestamp whose parts were emitted. The source is
        // bounded at the upper, so a return at all proves it terminated without
        // stalling.
        async fn run(coalesce_bytes: usize) -> Option<u64> {
            let mut cache = PersistClientCache::new_no_metrics();
            // Keep the batches unmerged, so the source sees one batch per write
            // just as a retained-history shard does in prod.
            cache.cfg.compaction_enabled = false;
            cache
                .cfg
                .set_config(&SOURCE_HYDRATION_FRONTIER_COALESCE_BYTES, coalesce_bytes);
            let persist_client = cache
                .open(PersistLocation::new_in_mem())
                .await
                .expect("in-mem location is valid");
            let shard_id = ShardId::new();

            let mut write = persist_client
                .open_writer::<String, String, u64, u64>(
                    shard_id,
                    Arc::new(StringSchema),
                    Arc::new(StringSchema),
                    Diagnostics::for_tests(),
                )
                .await
                .expect("invalid usage");

            // One append per timestamp: `N_BATCHES` distinct batches sealing
            // `[0, N_BATCHES)`.
            for t in 0..N_BATCHES {
                let row = ((format!("k{t}"), format!("v{t}")), t, 1u64);
                write.expect_compare_and_append(&[row], t, t + 1).await;
            }

            timely::execute::execute_directly(move |worker| {
                let (probe, receiver, _token) = worker.dataflow::<u64, _, _>(|outer| {
                    let (stream, token) = outer.scoped::<u64, _, _>("hybrid", |scope| {
                        let (stream, tokens) = shard_source::<String, String, u64, u64, _, _, _>(
                            outer,
                            scope,
                            "test_source",
                            move || std::future::ready(persist_client.clone()),
                            shard_id,
                            Some(Antichain::from_elem(0)),
                            SnapshotMode::Include,
                            // Bound the source at the shard upper so it
                            // terminates once the replay is done.
                            Antichain::from_elem(N_BATCHES),
                            Some(move |_, descs, _| (descs, vec![])),
                            Arc::new(StringSchema),
                            Arc::new(StringSchema),
                            FilterResult::keep_all,
                            false.then_some(|| unreachable!()),
                            async {},
                            ErrorHandler::Halt("test"),
                        );
                        (stream.leave(outer), tokens)
                    });
                    let probe = ProbeHandle::new();
                    // Capture the source's output directly so every progress
                    // message is recorded, independent of how many we drain per
                    // worker step.
                    let receiver = stream.probe_with(&probe).capture();
                    (probe, receiver, token)
                });

                // Step until the source closes its output (until == upper, so it
                // drops its capabilities once the replay completes).
                let deadline = Instant::now() + std::time::Duration::from_secs(60);
                while !probe.with_frontier(|f| f.is_empty()) {
                    assert!(Instant::now() < deadline, "timed out hydrating shard");
                    worker.step_or_park(Some(std::time::Duration::from_millis(1)));
                }

                // The largest `Messages` time is the highest timestamp whose
                // parts were emitted; it must reach `N_BATCHES - 1` regardless of
                // coalescing, because parts always flow per batch and only the
                // frontier downgrades are held back.
                let mut max_msg_time: Option<u64> = None;
                while let Ok(event) = receiver.try_recv() {
                    if let CaptureEvent::Messages(time, _) = event {
                        max_msg_time = max_msg_time.max(Some(time));
                    }
                }
                max_msg_time
            })
        }

        // Coalescing disabled (default) and enabled with a budget larger than
        // the whole replay (a single 0 -> upper jump) must both consume every
        // batch and emit parts through the last timestamp. Coalescing changes
        // only frontier granularity, not the data emitted or how far we get;
        // the round reduction itself is covered by
        // `test_frontier_coalesce_decision`, since timely batches progress
        // across the dataflow edge in a fast in-mem replay.
        assert_eq!(run(0).await, Some(N_BATCHES - 1));
        assert_eq!(run(1 << 30).await, Some(N_BATCHES - 1));
    }

    /// The round-reduction property, tested directly on the forward/coalesce
    /// decision so it is independent of timely's progress batching. Simulates
    /// replaying `n` unit batches from as-of 0 to upper `n`, each `bytes`
    /// encoded bytes, and counts how many progress downgrades get forwarded.
    #[mz_ore::test]
    fn test_frontier_coalesce_decision() {
        fn forwards(coalesce_target: u64, n: u64, bytes_per_batch: u64) -> usize {
            let replay_upper = n;
            let mut current = 0u64;
            let mut coalesced = 0u64;
            let mut forwarded = 0usize;
            for _ in 0..n {
                current += 1;
                coalesced += bytes_per_batch;
                let caught_up = replay_upper <= current;
                // Mirrors the forward/coalesce decision in `shard_source_descs`.
                let coalesce = coalesce_target > 0 && !caught_up && coalesced < coalesce_target;
                if !coalesce {
                    forwarded += 1;
                    coalesced = 0;
                }
            }
            forwarded
        }

        // Disabled: one forward per batch (the per-batch storm).
        assert_eq!(forwards(0, 64, 10), 64);
        // Budget larger than the whole replay: a single forward at the upper.
        assert_eq!(forwards(1 << 30, 64, 10), 1);
        // Mid budget: forwards every ~target/bytes batches plus the final
        // catch-up forward, so strictly between the two extremes.
        let partial = forwards(100, 64, 10);
        assert!(
            partial > 1 && partial < 64,
            "expected partial coalescing, got {partial}"
        );
    }

    /// Verifies that the source fetches and emits actual data: a batch written
    /// before the dataflow starts comes out as at least one `FetchedBlob`, and
    /// the output frontier reaches the shard's upper. Exercises the listen task
    /// -> descs operator -> fetch task -> fetch operator pipeline end-to-end.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn test_shard_source_fetches_data() {
        let persist_client = PersistClient::new_for_tests().await;
        let shard_id = ShardId::new();

        let mut write = persist_client
            .open_writer::<String, String, u64, u64>(
                shard_id,
                Arc::new(StringSchema),
                Arc::new(StringSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("invalid usage");
        let data = [
            (("k1".to_owned(), "v1".to_owned()), 0u64, 1u64),
            (("k2".to_owned(), "v2".to_owned()), 1u64, 1u64),
        ];
        write.expect_compare_and_append(&data[..], 0, 5).await;

        let expected_frontier = 5;
        let (blob_count, frontier) = timely::execute::execute_directly(move |worker| {
            let as_of = Antichain::from_elem(0);
            let until = Antichain::new();

            let (capture, probe, token) = worker.dataflow::<u64, _, _>(|outer| {
                let (stream, token) = outer.scoped::<u64, _, _>("hybrid", |scope| {
                    let transformer = move |_, descs, _| (descs, vec![]);
                    let (stream, tokens) = shard_source::<String, String, u64, u64, _, _, _>(
                        outer,
                        scope,
                        "test_source",
                        move || std::future::ready(persist_client.clone()),
                        shard_id,
                        Some(as_of),
                        SnapshotMode::Include,
                        until,
                        Some(transformer),
                        Arc::new(StringSchema),
                        Arc::new(StringSchema),
                        FilterResult::keep_all,
                        false.then_some(|| unreachable!()),
                        async {},
                        ErrorHandler::Halt("test"),
                    );
                    (stream.leave(outer), tokens)
                });

                let probe = ProbeHandle::new();
                let stream = stream.probe_with(&probe);
                (stream.capture(), probe, token)
            });

            let deadline = Instant::now() + std::time::Duration::from_secs(60);
            while probe.less_than(&expected_frontier) {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for output frontier {expected_frontier}"
                );
                worker.step();
            }
            drop(token);

            let mut blob_count = 0;
            while let Ok(event) = capture.try_recv() {
                if let CaptureEvent::Messages(_, msgs) = event {
                    blob_count += msgs.len();
                }
            }
            let mut frontier = Antichain::new();
            probe.with_frontier(|f| frontier.extend(f.iter().cloned()));
            (blob_count, frontier)
        });

        assert!(blob_count >= 1, "expected at least one fetched blob");
        assert_eq!(frontier, Antichain::from_elem(expected_frontier));
    }

    /// Verifies that dropping the source's tokens while it is running does not
    /// panic or wedge the worker: capabilities are released so the dataflow can
    /// shut down to the empty frontier.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn test_shard_source_shutdown_mid_stream() {
        let persist_client = PersistClient::new_for_tests().await;
        let shard_id = ShardId::new();

        let mut write = persist_client
            .open_writer::<String, String, u64, u64>(
                shard_id,
                Arc::new(StringSchema),
                Arc::new(StringSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("invalid usage");
        let data = [(("k1".to_owned(), "v1".to_owned()), 0u64, 1u64)];
        write.expect_compare_and_append(&data[..], 0, 5).await;

        timely::execute::execute_directly(move |worker| {
            let as_of = Antichain::from_elem(0);
            // An empty `until` means the source would run forever if not shut
            // down by dropping its tokens.
            let until = Antichain::new();

            let (probe, token) = worker.dataflow::<u64, _, _>(|outer| {
                let (stream, token) = outer.scoped::<u64, _, _>("hybrid", |scope| {
                    let transformer = move |_, descs, _| (descs, vec![]);
                    let (stream, tokens) = shard_source::<String, String, u64, u64, _, _, _>(
                        outer,
                        scope,
                        "test_source",
                        move || std::future::ready(persist_client.clone()),
                        shard_id,
                        Some(as_of),
                        SnapshotMode::Include,
                        until,
                        Some(transformer),
                        Arc::new(StringSchema),
                        Arc::new(StringSchema),
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

            // Step until the source has made progress, so shutdown happens while
            // the listen and fetch machinery is live.
            let deadline = Instant::now() + std::time::Duration::from_secs(60);
            while probe.less_than(&1) {
                assert!(Instant::now() < deadline, "timed out waiting for progress");
                worker.step();
            }

            // Shut down and confirm the dataflow drains: with all tokens dropped,
            // the operators must release their capabilities and the frontier must
            // become empty.
            drop(token);
            let deadline = Instant::now() + std::time::Duration::from_secs(60);
            loop {
                assert!(Instant::now() < deadline, "timed out waiting for shutdown");
                worker.step();
                if probe.with_frontier(|f| f.is_empty()) {
                    break;
                }
            }
        });
    }

    /// Verifies that an unserveable `as_of` (the listing path) reports an error
    /// through the `ErrorHandler` and freezes the source: the output frontier
    /// stays at the requested `as_of` and the worker does not panic.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn test_shard_source_error_freeze() {
        let persist_client = PersistClient::new_for_tests().await;
        let shard_id = ShardId::new();

        // Write data so the shard's upper is past the as_of (otherwise
        // `snapshot` blocks waiting for the upper instead of erroring on the
        // since), then advance the since past the as_of we'll request.
        let mut write = persist_client
            .open_writer::<String, String, u64, u64>(
                shard_id,
                Arc::new(StringSchema),
                Arc::new(StringSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("invalid usage");
        let data = [(("k1".to_owned(), "v1".to_owned()), 0u64, 1u64)];
        write.expect_compare_and_append(&data[..], 0, 5).await;
        initialize_shard(&persist_client, shard_id, Antichain::from_elem(3)).await;

        let (errored, frontier) = timely::execute::execute_directly(move |worker| {
            let as_of = Antichain::from_elem(1);
            let until = Antichain::new();

            let errored = Rc::new(std::cell::Cell::new(false));
            let error_handler = ErrorHandler::signal({
                let errored = Rc::clone(&errored);
                move |_err| errored.set(true)
            });

            let (probe, _token) = worker.dataflow::<u64, _, _>(|outer| {
                let (stream, token) = outer.scoped::<u64, _, _>("hybrid", |scope| {
                    let transformer = move |_, descs, _| (descs, vec![]);
                    let (stream, tokens) = shard_source::<String, String, u64, u64, _, _, _>(
                        outer,
                        scope,
                        "test_source",
                        move || std::future::ready(persist_client.clone()),
                        shard_id,
                        Some(as_of),
                        SnapshotMode::Include,
                        until,
                        Some(transformer),
                        Arc::new(StringSchema),
                        Arc::new(StringSchema),
                        FilterResult::keep_all,
                        false.then_some(|| unreachable!()),
                        async {},
                        error_handler,
                    );
                    (stream.leave(outer), tokens)
                });

                let probe = ProbeHandle::new();
                let _stream = stream.probe_with(&probe);
                (probe, token)
            });

            let deadline = Instant::now() + std::time::Duration::from_secs(60);
            while !errored.get() {
                assert!(Instant::now() < deadline, "timed out waiting for error");
                worker.step();
            }
            // Keep stepping; the source must stay frozen at the as_of.
            for _ in 0..100 {
                worker.step();
            }

            let mut frontier = Antichain::new();
            probe.with_frontier(|f| frontier.extend(f.iter().cloned()));
            (errored.get(), frontier)
        });

        assert!(errored);
        assert_eq!(frontier, Antichain::from_elem(1));
    }

    /// Regression test for the `shard_source_fetch` freeze path: a blob that
    /// goes missing while *fetching* (the listing path is covered by
    /// `test_shard_source_error_freeze`) must freeze the output frontier at the
    /// missing part and report the error, never advancing past data never
    /// emitted.
    ///
    /// We delete the first batch's blob, which is read by the snapshot at
    /// `as_of = 0`. Its fetch fails; the later batches (t=1, t=2) fetch fine. We
    /// step until the dataflow quiesces (with brief parks so the tokio fetch
    /// task finishes), so the later results are produced, then assert the error
    /// fired and the frontier stayed at the missing part — the failed time's
    /// capability is held regardless of what other times complete.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn test_shard_source_fetch_error_freeze() {
        // Force writes to real blobs (inline parts have no blob to delete) and
        // disable compaction so the three batches stay distinct and deletable.
        let mut cache = PersistClientCache::new_no_metrics();
        cache.cfg.compaction_enabled = false;
        cache.cfg.set_config(&INLINE_WRITES_SINGLE_MAX_BYTES, 0);
        cache.cfg.set_config(&INLINE_WRITES_TOTAL_MAX_BYTES, 0);
        let persist_client = cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("in-mem location is valid");
        let shard_id = ShardId::new();
        // Clones of a `PersistClient` share the blob `Arc`, so deleting via this
        // handle is visible to the reader the source opens.
        let blob = Arc::clone(&persist_client.blob);

        let mut write = persist_client
            .open_writer::<String, String, u64, u64>(
                shard_id,
                Arc::new(StringSchema),
                Arc::new(StringSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("invalid usage");

        // The data-part (non-rollup) blob keys currently present.
        async fn batch_keys(blob: &dyn Blob) -> std::collections::BTreeSet<String> {
            let mut keys = std::collections::BTreeSet::new();
            blob.list_keys_and_metadata("", &mut |meta| {
                if let Ok((_, PartialBlobKey::Batch(..))) = BlobKey::parse_ids(meta.key) {
                    keys.insert(meta.key.to_owned());
                }
            })
            .await
            .expect("list keys");
            keys
        }

        let row = |t: u64| ((format!("k{t}"), format!("v{t}")), t, 1u64);
        let before = batch_keys(blob.as_ref()).await;
        write.expect_compare_and_append(&[row(0)], 0, 1).await;
        let after = batch_keys(blob.as_ref()).await;
        write.expect_compare_and_append(&[row(1)], 1, 2).await;
        write.expect_compare_and_append(&[row(2)], 2, 3).await;

        // Delete exactly the first (t=0) batch's data part(s); the snapshot at
        // as_of=0 reads it.
        let missing: Vec<_> = after.difference(&before).cloned().collect();
        assert!(!missing.is_empty(), "first batch wrote no blob part");
        for key in &missing {
            blob.delete(key).await.expect("delete");
        }

        let frontier = timely::execute::execute_directly(move |worker| {
            let errored = Rc::new(std::cell::Cell::new(false));
            let error_handler = ErrorHandler::signal({
                let errored = Rc::clone(&errored);
                move |_err| errored.set(true)
            });

            let (probe, _token) = worker.dataflow::<u64, _, _>(|outer| {
                let (stream, token) = outer.scoped::<u64, _, _>("hybrid", |scope| {
                    let (stream, tokens) = shard_source::<String, String, u64, u64, _, _, _>(
                        outer,
                        scope,
                        "test_source",
                        move || std::future::ready(persist_client.clone()),
                        shard_id,
                        Some(Antichain::from_elem(0)),
                        SnapshotMode::Include,
                        Antichain::new(),
                        Some(move |_, descs, _| (descs, vec![])),
                        Arc::new(StringSchema),
                        Arc::new(StringSchema),
                        FilterResult::keep_all,
                        false.then_some(|| unreachable!()),
                        async {},
                        error_handler,
                    );
                    (stream.leave(outer), tokens)
                });
                let probe = ProbeHandle::new();
                stream.probe_with(&probe);
                (probe, token)
            });

            // Step until the fetch error fires, then step until the dataflow
            // quiesces, with brief parks so the tokio fetch task can finish.
            let deadline = Instant::now() + std::time::Duration::from_secs(60);
            while !errored.get() {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for fetch error"
                );
                worker.step_or_park(Some(std::time::Duration::from_millis(1)));
            }
            let mut last = probe.with_frontier(|f| f.to_owned());
            let mut stable = 0;
            while stable < 100 {
                assert!(Instant::now() < deadline, "timed out waiting for quiesce");
                worker.step_or_park(Some(std::time::Duration::from_millis(1)));
                let now = probe.with_frontier(|f| f.to_owned());
                if now == last {
                    stable += 1;
                } else {
                    stable = 0;
                    last = now;
                }
            }
            last
        });

        // Frozen at the missing part (t=0); the bug advanced this past it.
        assert_eq!(frontier, Antichain::from_elem(0));
    }

    /// With `persist_source_fetch_concurrency > 1` the source still fetches
    /// every part and reaches the shard upper. The frontier can only reach the
    /// upper once every part's fetch has completed, so this proves the
    /// concurrent path loses nothing even though results complete out of order.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn test_shard_source_fetch_concurrent() {
        const N_BATCHES: u64 = 16;

        let mut cache = PersistClientCache::new_no_metrics();
        cache.cfg.compaction_enabled = false;
        cache.cfg.set_config(&SOURCE_FETCH_CONCURRENCY, 8);
        let persist_client = cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("in-mem location is valid");
        let shard_id = ShardId::new();

        let mut write = persist_client
            .open_writer::<String, String, u64, u64>(
                shard_id,
                Arc::new(StringSchema),
                Arc::new(StringSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("invalid usage");
        for t in 0..N_BATCHES {
            let row = ((format!("k{t}"), format!("v{t}")), t, 1u64);
            write.expect_compare_and_append(&[row], t, t + 1).await;
        }

        let (blob_count, max_time) = timely::execute::execute_directly(move |worker| {
            let (capture, probe, token) = worker.dataflow::<u64, _, _>(|outer| {
                let (stream, token) = outer.scoped::<u64, _, _>("hybrid", |scope| {
                    let (stream, tokens) = shard_source::<String, String, u64, u64, _, _, _>(
                        outer,
                        scope,
                        "test_source",
                        move || std::future::ready(persist_client.clone()),
                        shard_id,
                        Some(Antichain::from_elem(0)),
                        SnapshotMode::Include,
                        Antichain::from_elem(N_BATCHES),
                        Some(move |_, descs, _| (descs, vec![])),
                        Arc::new(StringSchema),
                        Arc::new(StringSchema),
                        FilterResult::keep_all,
                        false.then_some(|| unreachable!()),
                        async {},
                        ErrorHandler::Halt("test"),
                    );
                    (stream.leave(outer), tokens)
                });
                let probe = ProbeHandle::new();
                let stream = stream.probe_with(&probe);
                (stream.capture(), probe, token)
            });

            // The source is bounded at the upper, so its frontier empties only
            // once every part's fetch has completed; reaching the empty frontier
            // proves the concurrent path lost nothing.
            let deadline = Instant::now() + std::time::Duration::from_secs(60);
            while !probe.with_frontier(|f| f.is_empty()) {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for completion"
                );
                worker.step_or_park(Some(std::time::Duration::from_millis(1)));
            }
            drop(token);

            let mut blob_count = 0;
            let mut max_time: Option<u64> = None;
            while let Ok(event) = capture.try_recv() {
                if let CaptureEvent::Messages(time, msgs) = event {
                    blob_count += msgs.len();
                    max_time = max_time.max(Some(time));
                }
            }
            (blob_count, max_time)
        });

        assert!(blob_count >= 1, "expected at least one fetched blob");
        // Parts were emitted through the last timestamp.
        assert_eq!(max_time, Some(N_BATCHES - 1));
    }

    /// Fetch-path freeze under concurrency: with several fetches in flight, a
    /// missing *middle* batch must still freeze the frontier at that batch, even
    /// though later batches fetch fine and may complete before the error is
    /// observed. This is the out-of-order analogue of
    /// `test_shard_source_fetch_error_freeze`; it exercises the time-keyed
    /// capability bookkeeping the concurrent path relies on.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn test_shard_source_fetch_concurrent_error_freeze() {
        const N_BATCHES: u64 = 12;
        const MISSING_TS: u64 = 5;

        let mut cache = PersistClientCache::new_no_metrics();
        cache.cfg.compaction_enabled = false;
        cache.cfg.set_config(&INLINE_WRITES_SINGLE_MAX_BYTES, 0);
        cache.cfg.set_config(&INLINE_WRITES_TOTAL_MAX_BYTES, 0);
        cache.cfg.set_config(&SOURCE_FETCH_CONCURRENCY, 8);
        let persist_client = cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("in-mem location is valid");
        let shard_id = ShardId::new();
        let blob = Arc::clone(&persist_client.blob);

        let mut write = persist_client
            .open_writer::<String, String, u64, u64>(
                shard_id,
                Arc::new(StringSchema),
                Arc::new(StringSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("invalid usage");

        async fn batch_keys(blob: &dyn Blob) -> std::collections::BTreeSet<String> {
            let mut keys = std::collections::BTreeSet::new();
            blob.list_keys_and_metadata("", &mut |meta| {
                if let Ok((_, PartialBlobKey::Batch(..))) = BlobKey::parse_ids(meta.key) {
                    keys.insert(meta.key.to_owned());
                }
            })
            .await
            .expect("list keys");
            keys
        }

        // Write each batch, snapshotting the blob keys around the missing one so
        // we can delete exactly its part(s).
        let mut missing = Vec::new();
        for t in 0..N_BATCHES {
            let before = batch_keys(blob.as_ref()).await;
            let row = ((format!("k{t}"), format!("v{t}")), t, 1u64);
            write.expect_compare_and_append(&[row], t, t + 1).await;
            if t == MISSING_TS {
                let after = batch_keys(blob.as_ref()).await;
                missing = after.difference(&before).cloned().collect();
            }
        }
        assert!(!missing.is_empty(), "missing batch wrote no blob part");
        for key in &missing {
            blob.delete(key).await.expect("delete");
        }

        let frontier = timely::execute::execute_directly(move |worker| {
            let errored = Rc::new(std::cell::Cell::new(false));
            let error_handler = ErrorHandler::signal({
                let errored = Rc::clone(&errored);
                move |_err| errored.set(true)
            });

            let (probe, _token) = worker.dataflow::<u64, _, _>(|outer| {
                let (stream, token) = outer.scoped::<u64, _, _>("hybrid", |scope| {
                    let (stream, tokens) = shard_source::<String, String, u64, u64, _, _, _>(
                        outer,
                        scope,
                        "test_source",
                        move || std::future::ready(persist_client.clone()),
                        shard_id,
                        Some(Antichain::from_elem(0)),
                        SnapshotMode::Include,
                        Antichain::from_elem(N_BATCHES),
                        Some(move |_, descs, _| (descs, vec![])),
                        Arc::new(StringSchema),
                        Arc::new(StringSchema),
                        FilterResult::keep_all,
                        false.then_some(|| unreachable!()),
                        async {},
                        error_handler,
                    );
                    (stream.leave(outer), tokens)
                });
                let probe = ProbeHandle::new();
                stream.probe_with(&probe);
                (probe, token)
            });

            let deadline = Instant::now() + std::time::Duration::from_secs(60);
            while !errored.get() {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for fetch error"
                );
                worker.step_or_park(Some(std::time::Duration::from_millis(1)));
            }
            let mut last = probe.with_frontier(|f| f.to_owned());
            let mut stable = 0;
            while stable < 100 {
                assert!(Instant::now() < deadline, "timed out waiting for quiesce");
                worker.step_or_park(Some(std::time::Duration::from_millis(1)));
                let now = probe.with_frontier(|f| f.to_owned());
                if now == last {
                    stable += 1;
                } else {
                    stable = 0;
                    last = now;
                }
            }
            last
        });

        // Frozen at the missing batch despite later batches fetching fine
        // concurrently.
        assert_eq!(frontier, Antichain::from_elem(MISSING_TS));
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
