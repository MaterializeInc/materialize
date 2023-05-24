// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A source that reads from a persist shard.

use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::convert::Infallible;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::Hashable;
use futures::StreamExt;
use futures_util::future::Either;
use mz_ore::assert::SOFT_ASSERTIONS;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::vec::VecExt;
use mz_persist_types::{Codec, Codec64};
use mz_timely_util::builder_async::{Event, OperatorBuilder as AsyncOperatorBuilder};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{CapabilitySet, ConnectLoop, Feedback};
use timely::dataflow::{Scope, ScopeParent, Stream};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::Activator;
use timely::PartialOrder;
use tokio::sync::mpsc;
use tracing::{debug, trace};

use crate::cache::PersistClientCache;
use crate::fetch::{FetchedPart, SerdeLeasedBatchPart};
use crate::read::ListenEvent;
use crate::stats::PartStats;
use crate::{PersistLocation, ShardId};

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
/// All updates at times greater or equal to `until` will be suppressed.
/// The `map_filter_project` argument, if supplied, may be partially applied,
/// and any un-applied part of the argument will be left behind in the argument.
///
/// Users of this function have the ability to apply flow control to the output
/// to limit the in-flight data (measured in bytes) it can emit. The flow control
/// input is a timely stream that communicates the frontier at which the data
/// emitted from by this source have been dropped.
///
/// **Note:** Because this function is reading batches from `persist`, it is working
/// at batch granularity. In practice, the source will be overshooting the target
/// flow control upper by an amount that is related to the size of batches.
///
/// If no flow control is desired an empty stream whose frontier immediately advances
/// to the empty antichain can be used. An easy easy of creating such stream is by
/// using [`timely::dataflow::operators::generic::operator::empty`].
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
pub fn shard_source<K, V, D, F, G>(
    scope: &mut G,
    name: &str,
    clients: Arc<PersistClientCache>,
    location: PersistLocation,
    shard_id: ShardId,
    as_of: Option<Antichain<G::Timestamp>>,
    until: Antichain<G::Timestamp>,
    flow_control: Option<FlowControl<G>>,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    should_fetch_part: F,
) -> (Stream<G, FetchedPart<K, V, G::Timestamp, D>>, Rc<dyn Any>)
where
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
    F: FnMut(&PartStats) -> bool + 'static,
    G: Scope,
    // TODO: Figure out how to get rid of the TotalOrder bound :(.
    G::Timestamp: Timestamp + Lattice + Codec64 + TotalOrder,
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

    // we can safely pass along a zero summary from this feedback edge,
    // as the input is disconnected from the operator's output
    let (completed_fetches_feedback_handle, completed_fetches_feedback_stream) =
        scope.feedback(<<G as ScopeParent>::Timestamp as Timestamp>::Summary::default());

    let (descs, descs_token) = shard_source_descs::<K, V, D, _, G>(
        scope,
        name,
        Arc::clone(&clients),
        location.clone(),
        shard_id.clone(),
        as_of,
        until,
        flow_control,
        completed_fetches_feedback_stream,
        chosen_worker,
        Arc::clone(&key_schema),
        Arc::clone(&val_schema),
        should_fetch_part,
    );
    let (parts, completed_fetches_stream, fetch_token) = shard_source_fetch(
        &descs, name, clients, location, shard_id, key_schema, val_schema,
    );
    completed_fetches_stream.connect_loop(completed_fetches_feedback_handle);

    (parts, Rc::new((descs_token, fetch_token)))
}

/// Flow control configuration.
#[derive(Debug)]
pub struct FlowControl<G: Scope> {
    /// Stream providing in-flight frontier updates.
    ///
    /// As implied by its type, this stream never emits data, only progress updates.
    ///
    /// TODO: Replace `Infallible` with `!` once the latter is stabilized.
    pub progress_stream: Stream<G, Infallible>,
    /// Maximum number of in-flight bytes.
    pub max_inflight_bytes: usize,
}

#[derive(Debug)]
struct ActivateOnDrop {
    token_rx: tokio::sync::mpsc::Receiver<()>,
    activator: Activator,
}

impl Drop for ActivateOnDrop {
    fn drop(&mut self) {
        self.token_rx.close();
        self.activator.activate();
    }
}

pub(crate) fn shard_source_descs<K, V, D, F, G>(
    scope: &G,
    name: &str,
    clients: Arc<PersistClientCache>,
    location: PersistLocation,
    shard_id: ShardId,
    as_of: Option<Antichain<G::Timestamp>>,
    until: Antichain<G::Timestamp>,
    flow_control: Option<FlowControl<G>>,
    completed_fetches_stream: Stream<G, SerdeLeasedBatchPart>,
    chosen_worker: usize,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    mut should_fetch_part: F,
) -> (Stream<G, (usize, SerdeLeasedBatchPart)>, Rc<dyn Any>)
where
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
    F: FnMut(&PartStats) -> bool + 'static,
    G: Scope,
    // TODO: Figure out how to get rid of the TotalOrder bound :(.
    G::Timestamp: Timestamp + Lattice + Codec64 + TotalOrder,
{
    let cfg = clients.cfg().clone();
    let metrics = Arc::clone(&clients.metrics);
    let worker_index = scope.index();
    let num_workers = scope.peers();

    // This is a generator that sets up an async `Stream` that can be continuously polled to get the
    // values that are `yield`-ed from it's body.
    let name_owned = name.to_owned();

    let (flow_control_stream, flow_control_bytes) = match flow_control {
        Some(fc) => (fc.progress_stream, Some(fc.max_inflight_bytes)),
        None => (
            timely::dataflow::operators::generic::operator::empty(scope),
            None,
        ),
    };

    let mut builder =
        AsyncOperatorBuilder::new(format!("shard_source_descs({})", name), scope.clone());
    let (mut descs_output, descs_stream) = builder.new_output();

    let mut flow_control_input = builder.new_input_connection(
        &flow_control_stream,
        Pipeline,
        // Disconnect the flow_control_input from the output capabilities of the
        // operator. We could leave it connected without risking deadlock so
        // long as there is a non zero summary on the feedback edge. But it may
        // be less efficient because the pipeline will be moving in increments
        // of SUMMARY even though we have potentially dumped a lot more data in
        // the pipeline because of batch boundaries. Leaving it unconnected
        // means the pipeline will be able to retire bigger chunks of work.
        vec![Antichain::new()],
    );
    let mut completed_fetches = builder.new_input_connection(
        &completed_fetches_stream,
        // We must ensure all completed fetches are fed into
        // the worker responsible for managing part leases
        Exchange::new(move |_| u64::cast_from(chosen_worker)),
        // Disconnect the completed fetches feedback edge from the output
        // capabilities of the operator. The feedback edge influences only
        // when the operator can be safely shut down, and does not affect
        // its output.
        vec![Antichain::new()],
    );

    // NB: It is not safe to use the shutdown button, due to the possibility
    // of the Subscribe handle's SeqNo hold releasing before `shard_source_fetch`
    // has completed fetches to each outstanding part. Instead, we create a
    // channel that we pass along as a token. Closing the Receiver informs this
    // operator that the dataflow is being shutdown, and allows for a graceful
    // termination where we wait for `shard_source_fetch` to complete all of
    // its fetches before expiring our Subscribe handle and its SeqNo hold.
    // We wrap the token with `ActivateOnDrop` to ensure that the operator is
    // activated after the Receiver is closed.
    let (token_tx, token_rx) = mpsc::channel::<()>(1);
    let activate_on_drop = Rc::new(ActivateOnDrop {
        activator: builder.activator().clone(),
        token_rx,
    });

    let _shutdown_button = builder.build(move |caps| async move {
        let mut cap_set = CapabilitySet::from_elem(caps.into_element());

        let mut inflight_bytes = 0;
        let mut inflight_parts: Vec<(Antichain<G::Timestamp>, usize)> = Vec::new();

        let max_inflight_bytes = flow_control_bytes.unwrap_or(usize::MAX);

        // Only one worker is responsible for distributing parts
        if worker_index != chosen_worker {
            trace!(
                "We are not the chosen worker ({}), exiting...",
                chosen_worker
            );
            return;
        }

        let token_is_dropped = token_tx.closed();
        tokio::pin!(token_is_dropped);

        let create_read_handle = async {
            let client = clients
                .open(location)
                .await
                .expect("location should be valid");
            let read = client
                .open_leased_reader::<K, V, G::Timestamp, D>(
                    shard_id,
                    &format!("shard_source({})", name_owned),
                    key_schema,
                    val_schema,
                )
                .await
                .expect("could not open persist shard");

            read
        };

        tokio::pin!(create_read_handle);

        // Creating a ReadHandle can take indefinitely long, so we race its creation with our token
        // to ensure any async work is dropped once the token is dropped.
        //
        // NB: Reading from a channel (token_is_dropped) is cancel-safe. `create_read_handle` is NOT
        // cancel-safe, but we will not retry it if it is dropped.
        let read = match futures::future::select(token_is_dropped, create_read_handle).await {
            Either::Left(_) => {
                // The token dropped before we finished creating our `ReadHandle.
                // We can return immediately, as we could not have emitted any
                // parts to fetch.
                cap_set.downgrade(&[]);
                return;
            }
            Either::Right((read, _)) => {
                read
            }
        };

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
        // NOTE: We have to do this before our `subscribe()` call (which
        // internally calls `snapshot()` because that call will block when there
        // is no data yet available in the shard.
        cap_set.downgrade(as_of.clone());
        let mut current_ts = match as_of.clone().into_option() {
            Some(ts) => ts,
            None => {
                return;
            }
        };

        let create_subscribe = async {
            read.subscribe(as_of.clone()).await
        };
        tokio::pin!(create_subscribe);
        let token_is_dropped = token_tx.closed();
        tokio::pin!(token_is_dropped);

        // creating a Subscribe can take indefinitely long (e.g. as_of in the far future),
        // so we race its creation with our token to ensure any async work is dropped once
        // the token is dropped.
        //
        // NB: reading from a channel (token_is_dropped) is cancel-safe.
        //     create_subscribe is NOT cancel-safe, but we will not retry it if it is dropped.
        let subscription = match futures::future::select(token_is_dropped, create_subscribe).await {
            Either::Left(_) => {
                // the token dropped before we finished creating our Subscribe.
                // we can return immediately, as we could not have emitted any
                // parts to fetch.
                cap_set.downgrade(&[]);
                return;
            }
            Either::Right((subscription, _)) => {
                subscription
            }
        };

        let mut subscription = subscription.unwrap_or_else(|e| {
            panic!(
                "{}: {} cannot serve requested as_of {:?}: {:?}",
                name_owned, shard_id, as_of, e
            )
        });

        let mut batch_parts = vec![];
        let mut lease_returner = subscription.lease_returner().clone();
        let (_keep_subscribe_alive_tx, keep_subscribe_alive_rx) = tokio::sync::oneshot::channel::<()>();
        let subscription_stream = async_stream::stream! {

            let mut done = false;
            while !done {
                for event in subscription.next().await {
                    if let ListenEvent::Progress(ref progress) = event {
                        // If `until.less_equal(progress)`, it means that all subsequent batches will
                        // contain only times greater or equal to `until`, which means they can be
                        // dropped in their entirety. The current batch must be emitted, but we can
                        // stop afterwards.
                        if PartialOrder::less_equal(&until, progress) {
                            done = true;
                        }
                    }
                    yield event;
                }
            }

            // intentionally keep this stream from dropping until the operator
            // is dropped. this ensures our Subscribe handle stays alive for as
            // long as is needed to finish fetching all of its parts.
            let _ = keep_subscribe_alive_rx.await;
        };
        tokio::pin!(subscription_stream);

        'emitting_parts:
        loop {
            // Notes on this select!:
            //
            // We have two mutually exclusive preconditions based on our flow
            // control state to determine whether we emit new batch parts, vs
            // applying flow control and waiting for downstream operators to
            // complete their work.
            //
            // We use a `biased` select, not for correctness, but to minimize
            // the work we need to do (e.g. check if the dataflow has been
            // dropped before reading more data from CRDB).
            tokio::select! {
                biased;
                // If the token has been dropped, we begin a graceful shutdown
                // by downgrading to the empty frontier and proceeding to the
                // next state, where we await our feedback edge's progression.
                //
                // NB: Reading from a channel is cancel safe.
                _ = token_tx.closed() => {
                    cap_set.downgrade(&[]);
                    break 'emitting_parts;
                }
                // Return the leases of any parts that the following stage,
                // `shard_source_fetch` has finished reading. This allows us
                // to advance our SeqNo hold as we continue to emit batches.
                //
                // NB: AsyncInputHandle::next is cancel safe
                completed_fetch = completed_fetches.next_mut() => {
                    match completed_fetch {
                        Some(Event::Data(_cap, data)) => {
                            for part in data.drain(..) {
                                lease_returner.return_leased_part(lease_returner.leased_part_from_exchangeable::<G::Timestamp>(part));
                            }
                        }
                        Some(Event::Progress(frontier)) => {
                            if frontier.is_empty() {
                                return;
                            }
                        }
                        None => {
                            // the downstream operator has been dropped, nothing more to do
                            return;
                        }
                    }
                }
                // While we have budget left for fetching more parts, read from the
                // subscription and pass them on.
                //
                // NB: StreamExt::next is cancel safe
                event = subscription_stream.next(), if inflight_bytes < max_inflight_bytes => {
                    match event {
                        Some(ListenEvent::Updates(mut parts)) => {
                            batch_parts.append(&mut parts);
                        }
                        Some(ListenEvent::Progress(progress)) => {
                            let session_cap = cap_set.delayed(&current_ts);

                            let bytes_emitted = {
                                let mut bytes_emitted = 0;
                                for mut part_desc in std::mem::take(&mut batch_parts) {
                                    // TODO(mfp): Push the filter down into the Subscribe?
                                    if cfg.dynamic.stats_filter_enabled() {
                                        let should_fetch = part_desc.stats.as_ref().map_or(true, |stats| should_fetch_part(stats));
                                        let bytes = u64::cast_from(part_desc.encoded_size_bytes);
                                        if should_fetch {
                                            metrics.pushdown.parts_fetched_count.inc();
                                            metrics.pushdown.parts_fetched_bytes.inc_by(bytes);
                                        } else {
                                            metrics.pushdown.parts_filtered_count.inc();
                                            metrics.pushdown.parts_filtered_bytes.inc_by(bytes);
                                            let should_audit = SOFT_ASSERTIONS.load(Ordering::Relaxed) || {
                                                let mut h = DefaultHasher::new();
                                                part_desc.key.hash(&mut h);
                                                usize::cast_from(h.finish()) % 100 < cfg.dynamic.stats_audit_percent()
                                            };
                                            if should_audit {
                                                metrics.pushdown.parts_audited_count.inc();
                                                metrics.pushdown.parts_audited_bytes.inc_by(bytes);
                                                part_desc.request_filter_pushdown_audit();
                                            } else {
                                                debug!("skipping part because of stats filter {:?}", part_desc.stats);
                                                lease_returner.return_leased_part(part_desc);
                                                continue;
                                            }
                                        }
                                    }

                                    bytes_emitted += part_desc.encoded_size_bytes();
                                    // Give the part to a random worker. This isn't
                                    // round robin in an attempt to avoid skew issues:
                                    // if your parts alternate size large, small, then
                                    // you'll end up only using half of your workers.
                                    //
                                    // There's certainly some other things we could be
                                    // doing instead here, but this has seemed to work
                                    // okay so far. Continue to revisit as necessary.
                                    let worker_idx = usize::cast_from(Instant::now().hashed()) % num_workers;
                                    descs_output.give(&session_cap, (worker_idx, part_desc.into_exchangeable_part())).await;
                                }
                                bytes_emitted
                            };

                            // Only track in-flight parts if flow control is enabled. Otherwise we
                            // would leak memory, as tracked parts would never be drained.
                            if flow_control_bytes.is_some() && bytes_emitted > 0 {
                                inflight_parts.push((progress.clone(), bytes_emitted));
                                inflight_bytes += bytes_emitted;
                                trace!(
                                    "shard {} putting {} bytes inflight. total: {}. batch frontier {:?}",
                                    shard_id,
                                    bytes_emitted,
                                    inflight_bytes,
                                    progress,
                                );
                            }

                            cap_set.downgrade(progress.iter());
                            match progress.into_option() {
                                Some(ts) => {
                                    current_ts = ts;
                                }
                                None => {
                                    cap_set.downgrade(&[]);
                                    break 'emitting_parts;
                                }
                            }
                        }
                        // We never expect any further output from our subscribe,
                        // so propagate that information downstream.
                        None => {
                            cap_set.downgrade(&[]);
                            break 'emitting_parts;
                        }
                    }
                }
                // We've exhausted our budget, listen for updates to the flow_control
                // input's frontier until we free up new budget. Progress ChangeBatches
                // are consumed even if you don't interact with the handle, so even if
                // we never make it to this block (e.g. budget of usize::MAX), because
                // the stream has no data, we don't cause unbounded buffering in timely.
                //
                // NB: AsyncInputHandle::next is cancel safe
                flow_control_upper = flow_control_input.next(), if inflight_bytes >= max_inflight_bytes => {
                    // We can never get here when flow control is disabled, as we are not tracking
                    // in-flight bytes in this case.
                    assert!(flow_control_bytes.is_some());

                    // Get an upper bound until which we should produce data
                    let flow_control_upper = match flow_control_upper {
                        Some(Event::Progress(frontier)) => frontier,
                        Some(Event::Data(_, _)) => unreachable!("flow_control_input should not contain data"),
                        None => Antichain::new(),
                    };

                    let retired_parts = inflight_parts.drain_filter_swapping(|(upper, _size)| {
                        PartialOrder::less_equal(&*upper, &flow_control_upper)
                    });

                    for (_upper, size_in_bytes) in retired_parts {
                        inflight_bytes -= size_in_bytes;
                        trace!(
                            "shard {} returning {} bytes. total: {}. batch frontier {:?} less_equal to {:?}",
                            shard_id,
                            size_in_bytes,
                            inflight_bytes,
                            _upper,
                            flow_control_upper,
                        );
                    }
                }
                else => {
                    break 'emitting_parts;
                }
            }
        }

        // We have finished outputting all leased parts the dataflow will need,
        // but `shard_source_fetches` may still be fetching those parts. We must
        // keep our Subscribe (and its SeqNo hold) alive until all of them have
        // been fetched to avoid racing with GC and panicking on a missing blob.
        // We can drop our handle when the feedback edge from `shard_source_fetches`
        // advances to the empty frontier, indicating that all parts have been read.
        while let Some(completed_fetch) = completed_fetches.next_mut().await {
            match completed_fetch {
                Event::Data(_cap, data) => {
                    for part in data.drain(..) {
                        lease_returner.return_leased_part(lease_returner.leased_part_from_exchangeable::<G::Timestamp>(part));
                    }
                }
                Event::Progress(frontier) => {
                    if frontier.is_empty() {
                        return;
                    }
                }
            }
        }
    });

    (descs_stream, activate_on_drop)
}

pub(crate) fn shard_source_fetch<K, V, T, D, G>(
    descs: &Stream<G, (usize, SerdeLeasedBatchPart)>,
    name: &str,
    clients: Arc<PersistClientCache>,
    location: PersistLocation,
    shard_id: ShardId,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
) -> (
    Stream<G, FetchedPart<K, V, T, D>>,
    Stream<G, SerdeLeasedBatchPart>,
    Rc<dyn Any>,
)
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
    G: Scope<Timestamp = T>,
{
    let mut builder =
        AsyncOperatorBuilder::new(format!("shard_source_fetch({})", name), descs.scope());
    let mut descs_input = builder.new_input(
        descs,
        Exchange::new(|&(i, _): &(usize, _)| u64::cast_from(i)),
    );
    let (mut fetched_output, fetched_stream) = builder.new_output();
    let (mut completed_fetches_output, completed_fetches_stream) = builder.new_output();

    let shutdown_button = builder.build(move |_capabilities| async move {
        let fetcher = {
            let client = clients
                .open(location.clone())
                .await
                .expect("location should be valid");
            client
                .create_batch_fetcher::<K, V, T, D>(shard_id, key_schema, val_schema)
                .await
        };

        while let Some(event) = descs_input.next_mut().await {
            if let Event::Data(cap, data) = event {
                // `LeasedBatchPart`es cannot be dropped at this point w/o
                // panicking, so swap them to an owned version.
                for (_idx, part) in data.drain(..) {
                    let (leased_part, fetched) = fetcher
                        .fetch_leased_part(fetcher.leased_part_from_exchangeable(part))
                        .await;
                    let fetched = fetched.expect("shard_id should match across all workers");
                    {
                        // Do very fine-grained output activation/session
                        // creation to ensure that we don't hold activated
                        // outputs or sessions across await points, which
                        // would prevent messages from being flushed from
                        // the shared timely output buffer.
                        fetched_output.give(&cap, fetched).await;
                        completed_fetches_output
                            .give(&cap, leased_part.into_exchangeable_part())
                            .await;
                    }
                }
            }
        }
    });

    (
        fetched_stream,
        completed_fetches_stream,
        Rc::new(shutdown_button.press_on_drop()),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use timely::dataflow::operators::Probe;
    use timely::progress::Antichain;

    use crate::cache::PersistClientCache;
    use crate::operators::shard_source::shard_source;
    use crate::{PersistLocation, ShardId};

    /// Verifies that a `shard_source` will downgrade it's output frontier to
    /// the `since` of the shard when no explicit `as_of` is given. Even if
    /// there is no data/no snapshot available in the
    /// shard.
    ///
    /// NOTE: This test is weird: if everything is good it will pass. If we
    /// break the assumption that we test this will time out and we will notice.
    #[tokio::test]
    async fn test_shard_source_implicit_initial_as_of() {
        let (persist_clients, location) = new_test_client_cache_and_location();

        let expected_frontier = 42;
        let shard_id = ShardId::new();

        initialize_shard(
            &persist_clients,
            location.clone(),
            shard_id,
            Antichain::from_elem(expected_frontier),
        )
        .await;

        let res = timely::execute::execute_directly(move |worker| {
            let until = Antichain::new();

            let (probe, _token) = worker.dataflow::<u64, _, _>(|scope| {
                let (stream, token) = shard_source::<String, String, u64, _, _>(
                    scope,
                    "test_source",
                    persist_clients,
                    location,
                    shard_id,
                    None, // No explicit as_of!
                    until,
                    None,
                    Arc::new(<std::string::String as mz_persist_types::Codec>::Schema::default()),
                    Arc::new(<std::string::String as mz_persist_types::Codec>::Schema::default()),
                    |_fetch| true,
                );

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
    #[tokio::test]
    async fn test_shard_source_explicit_initial_as_of() {
        let (persist_clients, location) = new_test_client_cache_and_location();

        let expected_frontier = 42;
        let shard_id = ShardId::new();

        initialize_shard(
            &persist_clients,
            location.clone(),
            shard_id,
            Antichain::from_elem(expected_frontier),
        )
        .await;

        let res = timely::execute::execute_directly(move |worker| {
            let as_of = Antichain::from_elem(expected_frontier);
            let until = Antichain::new();

            let (probe, _token) = worker.dataflow::<u64, _, _>(|scope| {
                let (stream, token) = shard_source::<String, String, u64, _, _>(
                    scope,
                    "test_source",
                    persist_clients,
                    location,
                    shard_id,
                    Some(as_of), // We specify the as_of explicitly!
                    until,
                    None,
                    Arc::new(<std::string::String as mz_persist_types::Codec>::Schema::default()),
                    Arc::new(<std::string::String as mz_persist_types::Codec>::Schema::default()),
                    |_fetch| true,
                );

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
        persist_clients: &Arc<PersistClientCache>,
        location: PersistLocation,
        shard_id: ShardId,
        since: Antichain<u64>,
    ) {
        let persist_client = persist_clients
            .open(location.clone())
            .await
            .expect("client construction failed");

        let mut read_handle = persist_client
            .open_leased_reader::<String, String, u64, u64>(
                shard_id,
                "tests",
                Arc::new(<std::string::String as mz_persist_types::Codec>::Schema::default()),
                Arc::new(<std::string::String as mz_persist_types::Codec>::Schema::default()),
            )
            .await
            .expect("invalid usage");

        read_handle.downgrade_since(&since).await;
    }

    fn new_test_client_cache_and_location() -> (Arc<PersistClientCache>, PersistLocation) {
        let persist_clients = PersistClientCache::new_no_metrics();

        let persist_clients = Arc::new(persist_clients);
        let location = PersistLocation {
            blob_uri: "mem://".to_owned(),
            consensus_uri: "mem://".to_owned(),
        };

        (persist_clients, location)
    }
}
