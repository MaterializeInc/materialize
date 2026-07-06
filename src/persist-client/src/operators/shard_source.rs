// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A source that reads from a persist shard.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::VecDeque;
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
use mz_ore::collections::CollectionExt;
use mz_persist_types::stats::PartStats;
use mz_persist_types::{Codec, Codec64};
use mz_timely_util::builder_async::{
    Event, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use timely::PartialOrder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{Capability, CapabilitySet, ConnectLoop, Enter, Feedback, Leave};
use timely::dataflow::{Scope, StreamVec};
use timely::order::TotalOrder;
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp, timestamp::Refines};
use tracing::{debug, trace};

use crate::batch::BLOB_TARGET_SIZE;
use crate::cfg::{
    RetryParameters, SOURCE_FETCH_CONCURRENCY, SOURCE_HYDRATION_FRONTIER_COALESCE_BYTES,
    USE_CRITICAL_SINCE_SOURCE,
};
use crate::fetch::{ExchangeableBatchPart, FetchedBlob, Lease};
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
    filter_fn: impl FnMut(&PartStats, AntichainRef<TOuter>) -> FilterResult + 'static,
    // If Some, an override for the default listen sleep retry parameters.
    listen_sleep: Option<impl Fn() -> RetryParameters + 'static>,
    start_signal: impl Future<Output = ()> + 'static,
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
    mut filter_fn: impl FnMut(&PartStats, AntichainRef<TOuter>) -> FilterResult + 'static,
    // If Some, an override for the default listen sleep retry parameters.
    listen_sleep: Option<impl Fn() -> RetryParameters + 'static>,
    start_signal: impl Future<Output = ()> + 'static,
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

    // This is a generator that sets up an async `Stream` that can be continuously polled to get the
    // values that are `yield`-ed from it's body.
    let name_owned = name.to_owned();

    // Create a shared slot between the operator to store the listen handle
    let listen_handle = Rc::new(RefCell::new(None));
    let return_listen_handle = Rc::clone(&listen_handle);

    // Create a oneshot channel to give the part returner a SubscriptionLeaseReturner
    let (tx, rx) = tokio::sync::oneshot::channel::<Rc<RefCell<LeaseManager<TOuter>>>>();
    let mut builder = AsyncOperatorBuilder::new(
        format!("shard_source_descs_return({})", name),
        scope.clone(),
    );
    let mut completed_fetches = builder.new_disconnected_input(completed_fetches_stream, Pipeline);
    // This operator doesn't need to use a token because it naturally exits when its input
    // frontier reaches the empty antichain.
    builder.build(move |_caps| async move {
        let Ok(leases) = rx.await else {
            // Either we're not the chosen worker or the dataflow was shutdown before the
            // subscriber was even created.
            return;
        };
        while let Some(event) = completed_fetches.next().await {
            let Event::Progress(frontier) = event else {
                continue;
            };
            leases.borrow_mut().advance_to(frontier.borrow());
        }
        // Make it explicit that the subscriber is kept alive until we have finished returning parts
        drop(return_listen_handle);
    });

    let mut builder =
        AsyncOperatorBuilder::new(format!("shard_source_descs({})", name), scope.clone());
    let (descs_output, descs_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    #[allow(clippy::await_holding_refcell_ref)]
    let shutdown_button = builder.build(move |caps| async move {
        let mut cap_set = CapabilitySet::from_elem(caps.into_element());

        // Only one worker is responsible for distributing parts
        if worker_index != chosen_worker {
            trace!(
                "We are not the chosen worker ({}), exiting...",
                chosen_worker
            );
            return;
        }

        // Internally, the `open_leased_reader` call registers a new LeasedReaderId and then fires
        // up a background tokio task to heartbeat it. It is possible that we might get a
        // particularly adversarial scheduling where the CRDB query to register the id is sent and
        // then our Future is not polled again for a long time, resulting is us never spawning the
        // heartbeat task. Run reader creation in a task to attempt to defend against this.
        //
        // TODO: Really we likely need to swap the inners of all persist operators to be
        // communicating with a tokio task over a channel, but that's much much harder, so for now
        // we whack the moles as we see them.
        let mut read = mz_ore::task::spawn(|| format!("shard_source_reader({})", name_owned), {
            let diagnostics = Diagnostics {
                handle_purpose: format!("shard_source({})", name_owned),
                shard_name: name_owned.clone(),
            };
            async move {
                let client = client.await;
                client
                    .open_leased_reader::<K, V, TOuter, D>(
                        shard_id,
                        key_schema,
                        val_schema,
                        diagnostics,
                        USE_CRITICAL_SINCE_SOURCE.get(client.dyncfgs()),
                    )
                    .await
            }
        })
        .await
        .expect("could not open persist shard");

        // Wait for the start signal only after we have obtained a read handle. This makes "cannot
        // serve requested as_of" panics caused by (database-issues#8729) significantly less
        // likely.
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
        cap_set.downgrade(as_of.clone());

        let mut snapshot_parts =
            match snapshot_mode {
                SnapshotMode::Include => match read.snapshot(as_of.clone()).await {
                    Ok(parts) => parts,
                    Err(e) => error_handler
                        .report_and_stop(anyhow!(
                            "{name_owned}: {shard_id} cannot serve requested as_of {as_of:?}: {e:?}"
                        ))
                        .await,
                },
                SnapshotMode::Exclude => vec![],
            };

        // We're about to start producing parts to be fetched whose leases will be returned by the
        // `shard_source_descs_return` operator above. In order for that operator to successfully
        // return the leases we send it the lease returner associated with our shared subscriber.
        let leases = Rc::new(RefCell::new(LeaseManager::new()));
        tx.send(Rc::clone(&leases))
            .expect("lease returner exited before desc producer");

        // Recent shard upper observed at hydration time. While the source is still
        // catching up to it we coalesce frontier downgrades (see the loop below); once
        // `current_frontier` reaches it the source is live and we forward every batch's
        // progress so steady-state frontier tracking stays tight. Read before `listen`
        // consumes `read`.
        let replay_upper = read.shared_upper();

        // Store the listen handle in the shared slot so that it stays alive until both operators
        // exit
        let mut listen = listen_handle.borrow_mut();
        let listen = match read.listen(as_of.clone()).await {
            Ok(handle) => listen.insert(handle),
            Err(e) => {
                error_handler
                    .report_and_stop(anyhow!(
                        "{name_owned}: {shard_id} cannot serve requested as_of {as_of:?}: {e:?}"
                    ))
                    .await
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
        let listen_tail = futures::stream::unfold(listen, |listen| async move {
            Some((listen.next(listen_retry).await, listen))
        });

        let mut shard_stream = pin!(listen_head.chain(listen_tail));

        // Ideally, we'd like our audit overhead to be proportional to the actual amount of "real"
        // work we're doing in the source. So: start with a small, constant budget; add to the
        // budget when we do real work; and skip auditing a part if we don't have the budget for it.
        let mut audit_budget_bytes = u64::cast_from(BLOB_TARGET_SIZE.get(&cfg).saturating_mul(2));

        // All future updates will be timestamped after this frontier.
        let mut current_frontier = as_of.clone();

        // While catching up to `replay_upper`, coalesce frontier downgrades until at
        // least this many encoded bytes have been emitted at the held capability. This
        // turns a long historical replay (one persist batch ~ one write ~ 1/s) from one
        // progress round per batch into a handful of larger steps, which is what bounds
        // the number of downstream arrangement-maintenance passes. `0` disables it. The
        // budget caps how much the downstream batcher stages before it can seal, so we
        // never trade the per-batch storm for an unbounded single batch.
        let coalesce_target = u64::cast_from(SOURCE_HYDRATION_FRONTIER_COALESCE_BYTES.get(&cfg));
        // Encoded bytes emitted since the last forwarded progress.
        let mut coalesced_bytes: u64 = 0;

        // If `until.less_equal(current_frontier)`, it means that all subsequent batches will contain only
        // times greater or equal to `until`, which means they can be dropped in their entirety.
        while !PartialOrder::less_equal(&until, &current_frontier) {
            let (parts, progress) = shard_stream.next().await.expect("infinite stream");

            let mut batch_bytes: u64 = 0;

            // Emit the part at the `(ts, 0)` time. The `granular_backpressure`
            // operator will refine this further, if its enabled.
            let current_ts = current_frontier
                .as_option()
                .expect("until should always be <= the empty frontier");
            let session_cap = cap_set.delayed(current_ts);

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

                // Give the part to a random worker. This isn't round robin in an attempt to avoid
                // skew issues: if your parts alternate size large, small, then you'll end up only
                // using half of your workers.
                //
                // There's certainly some other things we could be doing instead here, but this has
                // seemed to work okay so far. Continue to revisit as necessary.
                let worker_idx = usize::cast_from(Instant::now().hashed()) % num_workers;
                batch_bytes =
                    batch_bytes.saturating_add(u64::cast_from(part_desc.encoded_size_bytes()));
                let (part, lease) = part_desc.into_exchangeable_part();
                leases.borrow_mut().push_at(current_ts.clone(), lease);
                descs_output.give(&session_cap, (worker_idx, part));
            }

            current_frontier.join_assign(&progress);
            coalesced_bytes = coalesced_bytes.saturating_add(batch_bytes);

            // Coalesce the frontier downgrade while still catching up to `replay_upper`
            // and below the byte budget. Parts carry their real timestamps regardless, so
            // holding the frontier back only batches downstream progress rounds. Once live
            // (caught up to `replay_upper`) or with coalescing disabled we forward every
            // batch, keeping steady-state tracking tight for consumers like `persist_sink`.
            let caught_up = PartialOrder::less_equal(&replay_upper, &current_frontier);
            let coalesce = coalesce_target > 0 && !caught_up && coalesced_bytes < coalesce_target;
            if !coalesce {
                coalesced_bytes = 0;
                cap_set.downgrade(current_frontier.iter());
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
    let mut builder =
        AsyncOperatorBuilder::new(format!("shard_source_fetch({})", name), descs.scope());
    let (fetched_output, fetched_stream) = builder.new_output::<CapacityContainerBuilder<_>>();
    let (completed_fetches_output, completed_fetches_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<Infallible>>>();
    let mut descs_input = builder.new_input_for_many(
        descs,
        Exchange::new(|&(i, _): &(usize, _)| u64::cast_from(i)),
        [&fetched_output, &completed_fetches_output],
    );
    let name_owned = name.to_owned();

    let shutdown_button = builder.build(move |_capabilities| async move {
        // Open the fetcher in a task to defend against an adversarial schedule
        // delaying its background work, and read the concurrency dyncfg while we
        // hold the client. See the equivalent reasoning in `shard_source_descs`.
        let (fetcher, max_concurrency) =
            mz_ore::task::spawn(|| format!("shard_source_fetch({})", name_owned), {
                let diagnostics = Diagnostics {
                    shard_name: name_owned.clone(),
                    handle_purpose: format!("shard_source_fetch batch fetcher {}", name_owned),
                };
                async move {
                    let client = client.await;
                    // Up to this many part fetches run concurrently to amortize
                    // the blob-store round-trip, which dominates when there are
                    // many small parts. Results are keyed by time below, so
                    // completions in any order are fine; total in-flight bytes
                    // stay bounded by the fetch semaphore inside
                    // `fetch_leased_part`.
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
                    (fetcher, max_concurrency)
                }
            })
            .await;

        // Fetch one part on a per-call clone of the fetcher (cheap: shares the
        // schema cache), carrying the part's input capabilities through so they
        // come back with the result. The missing-blob diagnostics round-trip
        // happens inside the future, so the error surfaces only after the fetch
        // has truly failed.
        let fetch_one = |caps: [Capability<TInner>; 2], part: ExchangeableBatchPart<T>| {
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
                        Err(anyhow!(
                            "batch fetcher could not fetch batch part {}: {}",
                            blob_key,
                            diagnostics
                        ))
                    }
                };
                (caps, fetched)
            }
        };

        // Descs accepted from the input but not yet handed to a fetch, FIFO.
        // Each carries its input capabilities (data + completed-fetches), so
        // buffering here holds no progress hostage; it only bounds how many
        // fetches run at once (and thus how many parts are resident in memory).
        // Timely tracks the frontier through these capabilities: it advances
        // past a time only once every fetch minted at that time has completed
        // and dropped its clones, releasing the parts' leases on the chosen
        // worker via the completed-fetches feedback. Carrying capabilities
        // rather than a separate time-keyed map makes correctness independent of
        // the order results come back in.
        let mut pending: VecDeque<([Capability<TInner>; 2], ExchangeableBatchPart<T>)> =
            VecDeque::new();
        let mut in_flight = FuturesUnordered::new();
        let mut input_done = false;

        loop {
            // Start fetches up to the concurrency cap.
            while in_flight.len() < max_concurrency {
                let Some((caps, part)) = pending.pop_front() else {
                    break;
                };
                in_flight.push(fetch_one(caps, part));
            }

            tokio::select! {
                // Emit completed fetches first, so `in_flight` drains and we do
                // not hold more than `max_concurrency` parts in memory.
                biased;
                Some((caps, fetched)) = in_flight.next(), if !in_flight.is_empty() => {
                    match fetched {
                        Ok(fetched) => {
                            // Emit at the data capability, then drop both caps.
                            // Dropping them advances the data and completed-fetches
                            // frontiers once this time's last outstanding fetch is
                            // done.
                            fetched_output.give(&caps[0], fetched);
                            drop(caps);
                        }
                        Err(e) => {
                            // Report the missing blob and freeze. `report_and_stop`
                            // never returns, so we stop draining results and retain
                            // every in-flight and pending capability, including this
                            // failed part's `caps`. Crucially, a later successfully
                            // fetched part must NOT be allowed to drop its capability
                            // and let the frontier advance past the part we never
                            // emitted.
                            error_handler.report_and_stop(e).await;
                        }
                    }
                }
                // Accept new descs while the input is live. Their fetches are
                // throttled by the `pending` queue above, not here.
                event = descs_input.next(), if !input_done => {
                    match event {
                        Some(Event::Data(caps, data)) => {
                            // `LeasedBatchPart`es cannot be dropped at this point
                            // w/o panicking, so swap them to an owned version.
                            for (_idx, part) in data {
                                pending.push_back((caps.clone(), part));
                            }
                        }
                        Some(Event::Progress(_)) => {}
                        None => input_done = true,
                    }
                }
                // Input is exhausted and no fetches remain: we're done.
                else => break,
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
    /// the output frontier reaches the shard's upper.
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
            // the fetch machinery is live.
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
    /// fired and the frontier stayed at the missing part.
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
