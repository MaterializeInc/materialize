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
use timely::dataflow::operators::{CapabilitySet, ConnectLoop, Enter, Feedback, Leave};
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp, timestamp::Refines};
use tracing::{debug, trace};

use crate::batch::BLOB_TARGET_SIZE;
use crate::cfg::{RetryParameters, USE_CRITICAL_SINCE_SOURCE};
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
pub fn shard_source<'g, K, V, T, D, DT, G, C>(
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
    // If Some, an override for the default listen sleep retry parameters.
    listen_sleep: Option<impl Fn() -> RetryParameters + 'static>,
    start_signal: impl Future<Output = ()> + 'static,
    error_handler: ErrorHandler,
) -> (
    Stream<Child<'g, G, T>, FetchedBlob<K, V, G::Timestamp, D>>,
    Vec<PressOnDropButton>,
)
where
    K: Debug + Codec,
    V: Debug + Codec,
    D: Monoid + Codec64 + Send + Sync,
    G: Scope,
    // TODO: Figure out how to get rid of the TotalOrder bound :(.
    G::Timestamp: Timestamp + Lattice + Codec64 + TotalOrder + Sync,
    T: Refines<G::Timestamp>,
    DT: FnOnce(
        Child<'g, G, T>,
        &Stream<Child<'g, G, T>, (usize, ExchangeableBatchPart<G::Timestamp>)>,
        usize,
    ) -> (
        Stream<Child<'g, G, T>, (usize, ExchangeableBatchPart<G::Timestamp>)>,
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
            let (descs, extra_tokens) = desc_transformer(scope.clone(), &descs, chosen_worker);
            tokens.extend(extra_tokens);
            descs
        }
        None => descs,
    };

    let (parts, completed_fetches_stream, fetch_token) = shard_source_fetch(
        &descs,
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

pub(crate) fn shard_source_descs<K, V, D, G>(
    scope: &G,
    name: &str,
    client: impl Future<Output = PersistClient> + Send + 'static,
    shard_id: ShardId,
    as_of: Option<Antichain<G::Timestamp>>,
    snapshot_mode: SnapshotMode,
    until: Antichain<G::Timestamp>,
    completed_fetches_stream: Stream<G, Infallible>,
    chosen_worker: usize,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    mut filter_fn: impl FnMut(&PartStats, AntichainRef<G::Timestamp>) -> FilterResult + 'static,
    // If Some, an override for the default listen sleep retry parameters.
    listen_sleep: Option<impl Fn() -> RetryParameters + 'static>,
    start_signal: impl Future<Output = ()> + 'static,
    error_handler: ErrorHandler,
) -> (
    Stream<G, (usize, ExchangeableBatchPart<G::Timestamp>)>,
    PressOnDropButton,
)
where
    K: Debug + Codec,
    V: Debug + Codec,
    D: Monoid + Codec64 + Send + Sync,
    G: Scope,
    // TODO: Figure out how to get rid of the TotalOrder bound :(.
    G::Timestamp: Timestamp + Lattice + Codec64 + TotalOrder + Sync,
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
    let (tx, rx) = tokio::sync::oneshot::channel::<Rc<RefCell<LeaseManager<G::Timestamp>>>>();
    let mut builder = AsyncOperatorBuilder::new(
        format!("shard_source_descs_return({})", name),
        scope.clone(),
    );
    let mut completed_fetches = builder.new_disconnected_input(&completed_fetches_stream, Pipeline);
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
                    .open_leased_reader::<K, V, G::Timestamp, D>(
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

        // If `until.less_equal(current_frontier)`, it means that all subsequent batches will contain only
        // times greater or equal to `until`, which means they can be dropped in their entirety.
        while !PartialOrder::less_equal(&until, &current_frontier) {
            let (parts, progress) = shard_stream.next().await.expect("infinite stream");

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
                let (part, lease) = part_desc.into_exchangeable_part();
                leases.borrow_mut().push_at(current_ts.clone(), lease);
                descs_output.give(&session_cap, (worker_idx, part));
            }

            current_frontier.join_assign(&progress);
            cap_set.downgrade(progress.iter());
        }
    });

    (descs_stream, shutdown_button.press_on_drop())
}

pub(crate) fn shard_source_fetch<K, V, T, D, G>(
    descs: &Stream<G, (usize, ExchangeableBatchPart<T>)>,
    name: &str,
    client: impl Future<Output = PersistClient> + Send + 'static,
    shard_id: ShardId,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    is_transient: bool,
    error_handler: ErrorHandler,
) -> (
    Stream<G, FetchedBlob<K, V, T, D>>,
    Stream<G, Infallible>,
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
        let mut fetcher = mz_ore::task::spawn(|| format!("shard_source_fetch({})", name_owned), {
            let diagnostics = Diagnostics {
                shard_name: name_owned.clone(),
                handle_purpose: format!("shard_source_fetch batch fetcher {}", name_owned),
            };
            async move {
                client
                    .await
                    .create_batch_fetcher::<K, V, T, D>(
                        shard_id,
                        key_schema,
                        val_schema,
                        is_transient,
                        diagnostics,
                    )
                    .await
            }
        })
        .await
        .expect("shard codecs should not change");

        while let Some(event) = descs_input.next().await {
            if let Event::Data([fetched_cap, _completed_fetches_cap], data) = event {
                // `LeasedBatchPart`es cannot be dropped at this point w/o
                // panicking, so swap them to an owned version.
                for (_idx, part) in data {
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
                    {
                        // Do very fine-grained output activation/session
                        // creation to ensure that we don't hold activated
                        // outputs or sessions across await points, which
                        // would prevent messages from being flushed from
                        // the shared timely output buffer.
                        fetched_output.give(&fetched_cap, fetched);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use mz_persist::location::SeqNo;
    use timely::dataflow::Scope;
    use timely::dataflow::operators::Leave;
    use timely::dataflow::operators::Probe;
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

            let (probe, _token) = worker.dataflow::<u64, _, _>(|scope| {
                let (stream, token) = scope.scoped::<u64, _, _>("hybrid", |scope| {
                    let transformer = move |_, descs: &Stream<_, _>, _| (descs.clone(), vec![]);
                    let (stream, tokens) = shard_source::<String, String, u64, u64, _, _, _>(
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
                    (stream.leave(), tokens)
                });

                let probe = stream.probe();

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

            let (probe, _token) = worker.dataflow::<u64, _, _>(|scope| {
                let (stream, token) = scope.scoped::<u64, _, _>("hybrid", |scope| {
                    let transformer = move |_, descs: &Stream<_, _>, _| (descs.clone(), vec![]);
                    let (stream, tokens) = shard_source::<String, String, u64, u64, _, _, _>(
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
                    (stream.leave(), tokens)
                });

                let probe = stream.probe();

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
