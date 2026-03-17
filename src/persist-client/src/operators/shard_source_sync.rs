// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sync Timely operators (with Tokio tasks) for reading from a persist shard.
//!
//! This module provides an alternative implementation of `shard_source` that uses
//! sync Timely operators communicating with Tokio tasks via channels, instead of
//! async Timely operators. Gated behind the `PERSIST_SHARD_SOURCE_SYNC` dyncfg.
//!
//! The source is split into three Timely operators and two Tokio tasks:
//!
//! * **`shard_source_descs`** (sync operator + Tokio task): On the chosen worker, a Tokio task
//!   opens a leased reader, takes a snapshot, and listens for new batches. It sends `DescsEvent`s
//!   (setup info, part descriptors, and progress frontiers) to the operator via an unbounded
//!   channel. The operator distributes parts to workers and manages the output frontier via a
//!   [`CapabilitySet`]. Non-chosen workers immediately drop their capabilities.
//!
//! * **`shard_source_descs_return`** (sync operator): Receives the `completed_fetches` feedback
//!   stream (as a disconnected input) and advances the `LeaseManager` based on the frontier.
//!   This releases SeqNo leases for parts that have been fully fetched, allowing garbage
//!   collection to proceed.
//!
//! * **`shard_source_fetch`** (sync operator + Tokio task): Each worker spawns a Tokio task that
//!   owns a batch fetcher. The operator sends parts to the task for downloading; the task sends
//!   fetched blobs back. A [`SyncActivator`] wakes the operator when results are available.
//!   The operator's second output (`completed_fetches_stream`) is a phantom output used only
//!   for frontier tracking by `shard_source_descs_return`.

use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::convert::Infallible;
use std::fmt::Debug;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use differential_dataflow::Hashable;
use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use futures_util::StreamExt;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_persist_types::stats::PartStats;
use mz_persist_types::{Codec, Codec64};
use mz_timely_util::builder_async::{PressOnDropButton, button};
use timely::PartialOrder;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as OperatorBuilderRc;
use timely::dataflow::operators::{CapabilitySet, ConnectLoop, Enter, Feedback, Leave};
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, StreamVec};
use timely::order::TotalOrder;
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp, timestamp::Refines};
use timely::scheduling::SyncActivator;
use tokio::sync::mpsc;

use crate::batch::BLOB_TARGET_SIZE;
use crate::cfg::{PersistConfig, RetryParameters, USE_CRITICAL_SINCE_SOURCE};
use crate::fetch::{ExchangeableBatchPart, FetchedBlob, LeasedBatchPart};
use crate::internal::metrics::Metrics;
use crate::internal::state::BatchPart;
use crate::operators::shard_source::{ErrorHandler, FilterResult, LeaseManager, SnapshotMode};
use crate::stats::{STATS_AUDIT_PERCENT, STATS_FILTER_ENABLED};
use crate::{Diagnostics, PersistClient, ShardId};

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// This is the sync Timely operator implementation, using Tokio tasks for I/O.
/// See the [module docs](self) for details.
pub(crate) fn shard_source<'g, K, V, T, D, DT, G, C>(
    scope: &mut Child<'g, G, T>,
    name: &str,
    client: impl Fn() -> C,
    shard_id: ShardId,
    as_of: Option<Antichain<G::Timestamp>>,
    snapshot_mode: SnapshotMode,
    until: Antichain<G::Timestamp>,
    desc_transformer: Option<DT>,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    filter_fn: impl FnMut(&PartStats, AntichainRef<G::Timestamp>) -> FilterResult + 'static,
    listen_sleep: Option<impl Fn() -> RetryParameters + Send + 'static>,
    start_signal: impl Future<Output = ()> + Send + 'static,
    error_handler: ErrorHandler,
) -> (
    StreamVec<Child<'g, G, T>, FetchedBlob<K, V, G::Timestamp, D>>,
    Vec<PressOnDropButton>,
)
where
    K: Debug + Codec,
    V: Debug + Codec,
    D: Monoid + Codec64 + Send + Sync,
    G: Scope,
    G::Timestamp: Timestamp + Lattice + Codec64 + TotalOrder + Sync,
    T: Refines<G::Timestamp>,
    DT: FnOnce(
        Child<'g, G, T>,
        StreamVec<Child<'g, G, T>, (usize, ExchangeableBatchPart<G::Timestamp>)>,
        usize,
    ) -> (
        StreamVec<Child<'g, G, T>, (usize, ExchangeableBatchPart<G::Timestamp>)>,
        Vec<PressOnDropButton>,
    ),
    C: Future<Output = PersistClient> + Send + 'static,
{
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

    let (descs, descs_token) = shard_source_descs::<K, V, D, G>(
        &scope.parent,
        name,
        client(),
        shard_id.clone(),
        as_of,
        snapshot_mode,
        until,
        completed_fetches_feedback_stream.leave(),
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
            let (descs, extra_tokens) = desc_transformer(scope.clone(), descs, chosen_worker);
            tokens.extend(extra_tokens);
            descs
        }
        None => descs,
    };

    let (parts, completed_fetches_stream, fetch_token) = shard_source_fetch(
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

/// Events sent from the descs Tokio task to the descs operator.
enum DescsEvent<T: Timestamp> {
    /// Initial setup: the resolved as_of frontier, config, and metrics from the reader.
    Setup {
        as_of: Antichain<T>,
        cfg: PersistConfig,
        metrics: Arc<Metrics>,
    },
    /// A batch of parts with the progress frontier after this batch.
    Parts {
        parts: Vec<LeasedBatchPart<T>>,
        progress: Antichain<T>,
    },
}

/// Tokio task that performs persist I/O for `shard_source_descs`.
///
/// Opens a leased reader, waits for the start signal, takes a snapshot,
/// and continuously listens for new parts, sending them back to the
/// operator via channel.
async fn descs_task<K, V, D, T>(
    name: String,
    client: impl Future<Output = PersistClient> + Send,
    shard_id: ShardId,
    as_of: Option<Antichain<T>>,
    snapshot_mode: SnapshotMode,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    listen_sleep: Option<impl Fn() -> RetryParameters + Send>,
    start_signal: impl Future<Output = ()> + Send,
    error_handler: ErrorHandler,
    event_tx: mpsc::UnboundedSender<DescsEvent<T>>,
    sync_activator: SyncActivator,
) where
    K: Debug + Codec,
    V: Debug + Codec,
    D: Monoid + Codec64 + Send + Sync,
    T: Timestamp + Lattice + Codec64 + TotalOrder + Sync,
{
    // Internally, the `open_leased_reader` call registers a new LeasedReaderId and then fires
    // up a background tokio task to heartbeat it. It is possible that we might get a
    // particularly adversarial scheduling where the CRDB query to register the id is sent and
    // then our Future is not polled again for a long time, resulting in us never spawning the
    // heartbeat task. Running reader creation in a dedicated task defends against this.
    let mut read = {
        let diagnostics = Diagnostics {
            handle_purpose: format!("shard_source({})", name),
            shard_name: name.clone(),
        };
        let client = client.await;
        client
            .open_leased_reader::<K, V, T, D>(
                shard_id,
                key_schema,
                val_schema,
                diagnostics,
                USE_CRITICAL_SINCE_SOURCE.get(client.dyncfgs()),
            )
            .await
            .expect("could not open persist shard")
    };

    // Wait for the start signal only after we have obtained a read handle. This makes "cannot
    // serve requested as_of" panics caused by (database-issues#8729) significantly less
    // likely.
    let () = start_signal.await;

    let cfg = read.cfg.clone();
    let metrics = Arc::clone(&read.metrics);

    let as_of = as_of.unwrap_or_else(|| read.since().clone());

    // Send the initial setup event so the operator can downgrade its frontier.
    if event_tx
        .send(DescsEvent::Setup {
            as_of: as_of.clone(),
            cfg: cfg.clone(),
            metrics: Arc::clone(&metrics),
        })
        .is_err()
    {
        return; // Operator gone.
    }
    let _ = sync_activator.activate();

    let mut snapshot_parts = match snapshot_mode {
        SnapshotMode::Include => match read.snapshot(as_of.clone()).await {
            Ok(parts) => parts,
            Err(e) => {
                error_handler
                    .report_and_stop(anyhow!(
                        "{name}: {shard_id} cannot serve requested as_of {as_of:?}: {e:?}"
                    ))
                    .await
            }
        },
        SnapshotMode::Exclude => vec![],
    };

    let mut listen = match read.listen(as_of.clone()).await {
        Ok(handle) => handle,
        Err(e) => {
            error_handler
                .report_and_stop(anyhow!(
                    "{name}: {shard_id} cannot serve requested as_of {as_of:?}: {e:?}"
                ))
                .await
        }
    };

    let listen_retry = listen_sleep.as_ref().map(|retry| retry());

    // The head of the stream is enriched with the snapshot parts if they exist.
    let listen_head = if !snapshot_parts.is_empty() {
        let (mut parts, progress) = listen.next(listen_retry).await;
        snapshot_parts.append(&mut parts);
        futures::stream::iter(Some((snapshot_parts, progress)))
    } else {
        futures::stream::iter(None)
    };

    // The tail of the stream is all subsequent parts.
    let listen_tail = futures::stream::unfold(listen, |mut listen| async move {
        Some((listen.next(listen_retry).await, listen))
    });

    let mut shard_stream = std::pin::pin!(listen_head.chain(listen_tail));

    while let Some((parts, progress)) = shard_stream.next().await {
        if event_tx
            .send(DescsEvent::Parts { parts, progress })
            .is_err()
        {
            return; // Operator gone.
        }
        let _ = sync_activator.activate();
    }
}

fn shard_source_descs<K, V, D, G>(
    scope: &G,
    name: &str,
    client: impl Future<Output = PersistClient> + Send + 'static,
    shard_id: ShardId,
    as_of: Option<Antichain<G::Timestamp>>,
    snapshot_mode: SnapshotMode,
    until: Antichain<G::Timestamp>,
    completed_fetches_stream: StreamVec<G, Infallible>,
    chosen_worker: usize,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    mut filter_fn: impl FnMut(&PartStats, AntichainRef<G::Timestamp>) -> FilterResult + 'static,
    listen_sleep: Option<impl Fn() -> RetryParameters + Send + 'static>,
    start_signal: impl Future<Output = ()> + Send + 'static,
    error_handler: ErrorHandler,
) -> (
    StreamVec<G, (usize, ExchangeableBatchPart<G::Timestamp>)>,
    PressOnDropButton,
)
where
    K: Debug + Codec,
    V: Debug + Codec,
    D: Monoid + Codec64 + Send + Sync,
    G: Scope,
    G::Timestamp: Timestamp + Lattice + Codec64 + TotalOrder + Sync,
{
    let worker_index = scope.index();
    let num_workers = scope.peers();
    let name_owned = name.to_owned();

    // Shared lease manager between the descs and descs_return operators.
    // Both operators run on the same Timely thread, so Rc sharing is safe.
    let leases: Rc<RefCell<Option<LeaseManager<G::Timestamp>>>> = Rc::new(RefCell::new(None));

    // -- descs_return operator (sync): advances lease manager based on completed fetches --
    {
        let leases = Rc::clone(&leases);
        let mut builder = OperatorBuilderRc::new(
            format!("shard_source_descs_return({})", name),
            scope.clone(),
        );
        // Disconnected input: does not influence any output frontier.
        // This operator doesn't need to use a token because it naturally exits when its input
        // frontier reaches the empty antichain.
        let mut completed_fetches =
            builder.new_input_connection(completed_fetches_stream, Pipeline, vec![]);
        builder.build(move |_capabilities| {
            move |frontiers| {
                // Must drain input every activation.
                completed_fetches.for_each(|_cap, _data| {});
                if let Some(mgr) = leases.borrow_mut().as_mut() {
                    mgr.advance_to(frontiers[0].frontier());
                }
            }
        });
    }

    // -- descs operator (sync + Tokio task): distributes parts to workers --
    let mut builder =
        OperatorBuilderRc::new(format!("shard_source_descs({})", name), scope.clone());
    let info = builder.operator_info();

    let (descs_output, descs_stream) = builder.new_output();
    let mut descs_output = OutputBuilder::from(descs_output);

    // Shutdown token: caller holds the PressOnDropButton to keep the dataflow alive.
    let (mut shutdown_handle, shutdown_button) =
        button(&mut scope.clone(), Rc::clone(&info.address));

    // Set up the Tokio task and channels on the chosen worker only.
    let mut task_state = None;
    if worker_index == chosen_worker {
        let sync_activator = scope.sync_activator_for(info.address.to_vec());
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let task_handle = mz_ore::task::spawn(
            || format!("shard_source_descs({})", name_owned),
            descs_task::<K, V, D, G::Timestamp>(
                name_owned.clone(),
                client,
                shard_id,
                as_of,
                snapshot_mode,
                key_schema,
                val_schema,
                listen_sleep,
                start_signal,
                error_handler,
                event_tx,
                sync_activator,
            ),
        )
        .abort_on_drop();

        task_state = Some((event_rx, task_handle));
    }

    builder.build(move |capabilities| {
        let mut cap_set = CapabilitySet::from_elem(capabilities.into_element());

        // Non-chosen workers: drop capabilities immediately.
        if task_state.is_none() {
            cap_set = CapabilitySet::new();
        }

        // Chosen worker state. Destructure task_state for ownership.
        let (mut event_rx, task_handle) = match task_state {
            Some((rx, handle)) => (Some(rx), Some(handle)),
            None => (None, None),
        };
        let leases = leases; // Move the shared Rc into the schedule closure.
        let mut cfg: Option<PersistConfig> = None;
        let mut metrics: Option<Arc<Metrics>> = None;
        let mut current_frontier = Antichain::from_elem(G::Timestamp::minimum());
        let mut audit_budget_bytes: u64 = 0;

        move |_frontiers| {
            // Keep the task handle alive for the lifetime of this closure.
            let _ = &task_handle;

            // If the shutdown button was pressed, drop the event receiver to signal the
            // task to exit, and drop capabilities.
            if shutdown_handle.all_pressed() {
                event_rx = None;
                cap_set = CapabilitySet::new();
            }

            let Some(rx) = event_rx.as_mut() else {
                return;
            };

            // Drain events from the Tokio task.
            loop {
                match rx.try_recv() {
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        // Task exited. Drop capabilities to signal completion.
                        cap_set = CapabilitySet::new();
                        break;
                    }
                    Ok(DescsEvent::Setup {
                        as_of,
                        cfg: task_cfg,
                        metrics: task_metrics,
                    }) => {
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
                        cap_set.downgrade(as_of.clone());
                        current_frontier = as_of;
                        // Ideally, we'd like our audit overhead to be proportional to the
                        // actual amount of "real" work we're doing in the source. So: start
                        // with a small, constant budget; add to the budget when we do real
                        // work; and skip auditing a part if we don't have the budget for it.
                        audit_budget_bytes =
                            u64::cast_from(BLOB_TARGET_SIZE.get(&task_cfg).saturating_mul(2));
                        cfg = Some(task_cfg);
                        metrics = Some(task_metrics);

                        // Initialize the lease manager now that we have setup.
                        *leases.borrow_mut() = Some(LeaseManager::new());
                    }
                    Ok(DescsEvent::Parts { parts, progress }) => {
                        let cfg = cfg.as_ref().expect("setup must precede parts");
                        let metrics = metrics.as_ref().expect("setup must precede parts");

                        // If `until.less_equal(current_frontier)`, all subsequent batches
                        // contain only times >= `until`, so they can be dropped entirely.
                        if !PartialOrder::less_equal(&until, &current_frontier) {
                            // Emit the part at the `(ts, 0)` time. The
                            // `granular_backpressure` operator will refine this further,
                            // if it's enabled.
                            let current_ts = current_frontier
                                .as_option()
                                .expect("until should always be <= the empty frontier");
                            let session_cap = cap_set.delayed(current_ts);

                            for mut part_desc in parts {
                                if !apply_stats_filter(
                                    cfg,
                                    metrics,
                                    &mut filter_fn,
                                    &current_frontier,
                                    &mut audit_budget_bytes,
                                    &mut part_desc,
                                ) {
                                    continue;
                                }

                                // Give the part to a random worker. This isn't round
                                // robin in an attempt to avoid skew issues: if your
                                // parts alternate size large, small, then you'll end up
                                // only using half of your workers.
                                //
                                // There's certainly some other things we could be doing
                                // instead here, but this has seemed to work okay so far.
                                // Continue to revisit as necessary.
                                let worker_idx =
                                    usize::cast_from(Instant::now().hashed()) % num_workers;
                                let (part, lease) = part_desc.into_exchangeable_part();
                                if let Some(mgr) = leases.borrow_mut().as_mut() {
                                    mgr.push_at(current_ts.clone(), lease);
                                }
                                descs_output
                                    .activate()
                                    .session(&session_cap)
                                    .give((worker_idx, part));
                            }
                        }

                        current_frontier.join_assign(&progress);
                        cap_set.downgrade(progress.iter());
                    }
                }
            }
        }
    });

    (descs_stream, shutdown_button.press_on_drop())
}

/// Tokio task that creates a batch fetcher and fetches parts received from the operator.
async fn fetch_task<K, V, T, D>(
    name: String,
    client: impl Future<Output = PersistClient>,
    shard_id: ShardId,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    is_transient: bool,
    error_handler: ErrorHandler,
    mut part_rx: mpsc::UnboundedReceiver<ExchangeableBatchPart<T>>,
    fetched_tx: mpsc::UnboundedSender<FetchedBlob<K, V, T, D>>,
    sync_activator: SyncActivator,
) where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Monoid + Codec64 + Send + Sync,
{
    let diagnostics = Diagnostics {
        shard_name: name.clone(),
        handle_purpose: format!("shard_source_fetch batch fetcher {}", name),
    };
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

    while let Some(part) = part_rx.recv().await {
        // `LeasedBatchPart`es cannot be dropped at this point w/o
        // panicking, so swap them to an owned version.
        let fetched = fetcher
            .fetch_leased_part(part)
            .await
            .expect("shard_id should match across all workers");
        let fetched = match fetched {
            Ok(fetched) => fetched,
            Err(blob_key) => {
                // Ideally, readers should never encounter a missing blob. They place a seqno
                // hold as they consume their snapshot/listen, preventing any blobs they need
                // from being deleted by garbage collection, and all blob implementations are
                // linearizable so there should be no possibility of stale reads.
                //
                // However, it is possible for a lease to expire given a sustained period of
                // downtime, which could allow parts we expect to exist to be deleted...
                // at which point our best option is to request a restart.
                error_handler
                    .report_and_stop(anyhow!(
                        "batch fetcher could not fetch batch part {}; lost lease?",
                        blob_key
                    ))
                    .await
            }
        };
        if fetched_tx.send(fetched).is_err() {
            return; // Operator gone.
        }
        let _ = sync_activator.activate();
    }
}

fn shard_source_fetch<K, V, T, D, G>(
    descs: StreamVec<G, (usize, ExchangeableBatchPart<T>)>,
    name: &str,
    client: impl Future<Output = PersistClient> + Send + 'static,
    shard_id: ShardId,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    is_transient: bool,
    error_handler: ErrorHandler,
) -> (
    StreamVec<G, FetchedBlob<K, V, T, D>>,
    StreamVec<G, Infallible>,
    PressOnDropButton,
)
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Monoid + Codec64 + Send + Sync,
    G: Scope,
    G::Timestamp: Refines<T>,
{
    let scope = descs.scope();
    let mut builder =
        OperatorBuilderRc::new(format!("shard_source_fetch({})", name), scope.clone());
    let info = builder.operator_info();

    let (fetched_output, fetched_stream) = builder.new_output();
    let mut fetched_output = OutputBuilder::from(fetched_output);
    // Phantom output: no data is emitted, but its frontier (tracking the input) feeds back
    // to `shard_source_descs_return` to advance the lease manager.
    let (_, completed_fetches_stream) = builder.new_output::<Vec<Infallible>>();

    // Input connected to both outputs with default (identity) summary.
    let mut descs_input = builder.new_input(
        descs,
        Exchange::new(|&(i, _): &(usize, _)| u64::cast_from(i)),
    );

    // Shutdown token.
    let (mut shutdown_handle, shutdown_button) =
        button(&mut scope.clone(), Rc::clone(&info.address));

    // Set up the Tokio task and channels.
    let sync_activator = scope.sync_activator_for(info.address.to_vec());
    let (part_tx, part_rx) = mpsc::unbounded_channel();
    let (fetched_tx, fetched_rx) = mpsc::unbounded_channel();
    let name_owned = name.to_owned();

    let task_handle = mz_ore::task::spawn(
        || format!("shard_source_fetch({})", name_owned),
        fetch_task::<K, V, T, D>(
            name_owned.clone(),
            client,
            shard_id,
            key_schema,
            val_schema,
            is_transient,
            error_handler,
            part_rx,
            fetched_tx,
            sync_activator,
        ),
    )
    .abort_on_drop();

    builder.build(move |_capabilities| {
        let mut fetched_rx = Some(fetched_rx);
        // Track capabilities for in-flight fetches: one per part sent to the task.
        // We retain capabilities for both outputs: output 0 (fetched blobs) is used to
        // emit the fetched data, and output 1 (completed_fetches) prevents the feedback
        // frontier from advancing until fetches complete, keeping leases alive.
        let mut inflight_caps = std::collections::VecDeque::new();
        let mut errored = false;

        move |_frontiers| {
            // Keep the task handle alive for the lifetime of this closure.
            let _ = &task_handle;

            // Always drain input to allow Timely to reclaim buffers and make progress.
            // On shutdown or error, we discard the data instead of forwarding it.
            let shutting_down = shutdown_handle.all_pressed() || errored;
            descs_input.for_each(|cap, data| {
                if shutting_down {
                    return;
                }
                for (_idx, part) in data.drain(..) {
                    inflight_caps.push_back((cap.retain(0), cap.retain(1)));
                    if part_tx.send(part).is_err() {
                        // Task gone — will be handled below when fetched_rx disconnects.
                    }
                }
            });

            if shutdown_handle.all_pressed() {
                inflight_caps.clear();
                fetched_rx = None;
                return;
            }

            if errored {
                return;
            }

            // Receive fetched blobs from the task.
            if let Some(ref mut rx) = fetched_rx {
                loop {
                    match rx.try_recv() {
                        Ok(fetched) => {
                            let (cap, _completed_fetches_cap) = inflight_caps
                                .pop_front()
                                .expect("received more fetched blobs than parts sent");
                            fetched_output.activate().session(&cap).give(fetched);
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            if inflight_caps.is_empty() {
                                // Clean shutdown: all parts were fetched.
                                fetched_rx = None;
                            } else {
                                // Task exited with in-flight parts — error was reported.
                                // Keep capabilities alive to prevent frontier advancement.
                                errored = true;
                            }
                            break;
                        }
                    }
                }
            }
        }
    });

    (
        fetched_stream,
        completed_fetches_stream,
        shutdown_button.press_on_drop(),
    )
}

/// Apply stats-based filtering to a part descriptor.
///
/// Returns `true` if the part should be emitted, `false` if it was filtered out.
fn apply_stats_filter<T: Timestamp + Codec64>(
    cfg: &PersistConfig,
    metrics: &Metrics,
    filter_fn: &mut impl FnMut(&PartStats, AntichainRef<T>) -> FilterResult,
    current_frontier: &Antichain<T>,
    audit_budget_bytes: &mut u64,
    part_desc: &mut LeasedBatchPart<T>,
) -> bool {
    if !STATS_FILTER_ENABLED.get(cfg) {
        return true;
    }

    let filter_result = match &part_desc.part {
        BatchPart::Hollow(x) => x.stats.as_ref().map_or(FilterResult::Keep, |stats| {
            filter_fn(&stats.decode(), current_frontier.borrow())
        }),
        BatchPart::Inline { .. } => FilterResult::Keep,
    };

    let bytes = u64::cast_from(part_desc.encoded_size_bytes());
    match filter_result {
        FilterResult::Keep => {
            *audit_budget_bytes = audit_budget_bytes.saturating_add(bytes);
        }
        FilterResult::Discard => {
            metrics.pushdown.parts_filtered_count.inc();
            metrics.pushdown.parts_filtered_bytes.inc_by(bytes);
            let should_audit = match &part_desc.part {
                BatchPart::Hollow(x) => {
                    let mut h = DefaultHasher::new();
                    x.key.hash(&mut h);
                    usize::cast_from(h.finish()) % 100 < STATS_AUDIT_PERCENT.get(cfg)
                }
                BatchPart::Inline { .. } => false,
            };
            if should_audit && bytes < *audit_budget_bytes {
                *audit_budget_bytes -= bytes;
                metrics.pushdown.parts_audited_count.inc();
                metrics.pushdown.parts_audited_bytes.inc_by(bytes);
                part_desc.request_filter_pushdown_audit();
            } else {
                tracing::debug!(
                    "skipping part because of stats filter {:?}",
                    part_desc.part.stats()
                );
                return false;
            }
        }
        FilterResult::ReplaceWith { key, val } => {
            part_desc.maybe_optimize(cfg, key, val);
            *audit_budget_bytes = audit_budget_bytes.saturating_add(bytes);
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

    true
}
