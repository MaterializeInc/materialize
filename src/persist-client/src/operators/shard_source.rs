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
use std::convert::Infallible;
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ShutdownButton;
use differential_dataflow::Hashable;
use futures::StreamExt;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::vec::VecExt;
use mz_persist::location::ExternalError;
use mz_persist_types::{Codec, Codec64};
use mz_timely_util::builder_async::{Event, OperatorBuilder as AsyncOperatorBuilder};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::{Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::sync::{mpsc, Mutex};
use tracing::trace;

use crate::cache::PersistClientCache;
use crate::fetch::{FetchedPart, SerdeLeasedBatchPart};
use crate::read::ListenEvent;
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
pub fn shard_source<K, V, D, G>(
    scope: &G,
    name: &str,
    clients: Arc<Mutex<PersistClientCache>>,
    location: PersistLocation,
    shard_id: ShardId,
    as_of: Option<Antichain<G::Timestamp>>,
    until: Antichain<G::Timestamp>,
    flow_control: Option<FlowControl<G>>,
) -> (Stream<G, FetchedPart<K, V, G::Timestamp, D>>, Rc<dyn Any>)
where
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
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
    // 4. Consumed part collector: A timely operator running only on the
    //    original worker that collects workers' `LeasedBatchPart`s. Internally,
    //    this drops the part's SeqNo lease, allowing GC to occur.

    // Connects the consumed part collector operator with the part-issuing
    // Subscribe.
    let (consumed_part_tx, consumed_part_rx): (
        mpsc::UnboundedSender<SerdeLeasedBatchPart>,
        mpsc::UnboundedReceiver<SerdeLeasedBatchPart>,
    ) = mpsc::unbounded_channel();

    let chosen_worker = usize::cast_from(name.hashed()) % scope.peers();
    let (descs, descs_shutdown) = shard_source_descs::<K, V, D, G>(
        scope,
        name,
        Arc::clone(&clients),
        location.clone(),
        shard_id.clone(),
        as_of,
        until,
        flow_control,
        consumed_part_rx,
        chosen_worker,
    );
    let (parts, tokens) = shard_source_fetch(&descs, name, clients, location, shard_id);
    shard_source_tokens(&tokens, name, consumed_part_tx, chosen_worker);

    let token = Rc::new(descs_shutdown.press_on_drop());
    (parts, token)
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

pub(crate) fn shard_source_descs<K, V, D, G>(
    scope: &G,
    name: &str,
    clients: Arc<Mutex<PersistClientCache>>,
    location: PersistLocation,
    shard_id: ShardId,
    as_of: Option<Antichain<G::Timestamp>>,
    until: Antichain<G::Timestamp>,
    flow_control: Option<FlowControl<G>>,
    mut consumed_part_rx: mpsc::UnboundedReceiver<SerdeLeasedBatchPart>,
    chosen_worker: usize,
) -> (Stream<G, (usize, SerdeLeasedBatchPart)>, ShutdownButton<()>)
where
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
    G: Scope,
    // TODO: Figure out how to get rid of the TotalOrder bound :(.
    G::Timestamp: Timestamp + Lattice + Codec64 + TotalOrder,
{
    let worker_index = scope.index();
    let num_workers = scope.peers();

    // This is a generator that sets up an async `Stream` that can be continuously polled to get the
    // values that are `yield`-ed from it's body.
    let name_owned = name.to_owned();
    let async_stream = async_stream::try_stream!({
        // Only one worker is responsible for distributing parts
        if worker_index != chosen_worker {
            trace!(
                "We are not the chosen worker ({}), exiting...",
                chosen_worker
            );
            return;
        }

        let read = {
            let mut persist_clients = clients.lock().await;
            persist_clients.open(location).await
        };
        let read = read
            .expect("location should be valid")
            .open_leased_reader::<K, V, G::Timestamp, D>(
                shard_id,
                &format!("shard_source({})", name_owned),
            )
            .await
            .expect("could not open persist shard");
        let as_of = as_of.unwrap_or_else(|| read.since().clone());

        // Eagerly yield the initial as_of. This makes sure that the output
        // frontier of the `persist_source` closely tracks the `upper` frontier
        // of the persist shard. It might be that the snapshot for `as_of` is
        // not initially available yet, but this makes sure we already downgrade
        // to it.
        //
        // Downstream consumers might rely on close frontier tracking for making
        // progress. For example, the `persist_sink` needs to know the
        // up-to-date upper of the output shard to make progress because it
        // will only write out new data once it knows that earlier writes went
        // through, including the initial downgrade of the shard upper to the
        // `as_of`.
        yield ListenEvent::Progress(as_of.clone());

        let subscription = read.subscribe(as_of.clone()).await;

        let mut subscription = subscription.unwrap_or_else(|e| {
            panic!(
                "{}: {} cannot serve requested as_of {:?}: {:?}",
                name_owned, shard_id, as_of, e
            )
        });

        let mut done = false;
        while !done {
            while let Ok(leased_part) = consumed_part_rx.try_recv() {
                subscription
                    .return_leased_part(subscription.leased_part_from_exchangeable(leased_part));
            }

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

        // Rather than simply end, we spawn a task that can continue to return
        // leases. This keeps the `ReadHandle` alive until we have confirmed
        // each lease as read.
        //
        // TODO: Replace the channel, this task, and the shard_source_tokens
        // operator with a timely edge between the shard_source_fetch output and
        // an input exchanging to the chosen worker for shard_source_descs.
        mz_ore::task::spawn(|| "LeaseReturner", async move {
            while let Some(leased_part) = consumed_part_rx.recv().await {
                subscription
                    .return_leased_part(subscription.leased_part_from_exchangeable(leased_part));
            }
        });
    });

    // TODO: Now that this operator is async AND the pinned stream does not
    // participate in a select! call, which means its methods never get
    // cancelled, we can get rid of the whole async_stream! macro machinery and
    // just get the value directly with a function call.
    let mut pinned_stream = Box::pin(async_stream);

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

    let shutdown_button = builder.build(move |caps| async move {
        let mut cap_set = CapabilitySet::from_elem(caps.into_element());

        let mut current_ts = timely::progress::Timestamp::minimum();
        let mut inflight_bytes = 0;
        let mut inflight_parts: Vec<(Antichain<G::Timestamp>, usize)> = Vec::new();

        let max_inflight_bytes = flow_control_bytes.unwrap_or(usize::MAX);

        loop {
            // While we have budget left for fetching more parts, read from the
            // subscription and pass them on.
            let mut batch_parts = vec![];
            while inflight_bytes < max_inflight_bytes {
                match pinned_stream.next().await {
                    Some(Ok(ListenEvent::Updates(mut parts))) => {
                        batch_parts.append(&mut parts);
                    }
                    Some(Ok(ListenEvent::Progress(progress))) => {
                        let session_cap = cap_set.delayed(&current_ts);
                        let mut descs_output = descs_output.activate();
                        let mut descs_session = descs_output.session(&session_cap);

                        // NB: in order to play nice with downstream operators whose invariants
                        // depend on seeing the full contents of an individual batch, we must
                        // atomically emit all parts here (e.g. no awaits).
                        let bytes_emitted = {
                            let mut bytes_emitted = 0;
                            for part_desc in std::mem::take(&mut batch_parts) {
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
                                descs_session.give((worker_idx, part_desc.into_exchangeable_part()));
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
                                return;
                            }
                        }
                    }
                    Some(Err::<_, ExternalError>(e)) => {
                        panic!("unexpected error from persist {e}")
                    }
                    // We never expect any further output from
                    // `pinned_stream`, so propagate that information
                    // downstream.
                    None => {
                        cap_set.downgrade(&[]);
                        return;
                    }
                }
            }

            // We've exhausted our budget, listen for updates to the
            // flow_control input's frontier until we free up new budget.
            // Progress ChangeBatches are consumed even if you don't interact
            // with the handle, so even if we never make it to this block (e.g.
            // budget of usize::MAX), because the stream has no data, we don't
            // cause unbounded buffering in timely.
            while inflight_bytes >= max_inflight_bytes {
                // We can never get here when flow control is disabled, as we are not tracking
                // in-flight bytes in this case.
                assert_eq!(flow_control_bytes, None);

                // Get an upper bound until which we should produce data
                let flow_control_upper = match flow_control_input.next().await {
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
        }
    });

    (descs_stream, shutdown_button)
}

pub(crate) fn shard_source_fetch<K, V, T, D, G>(
    descs: &Stream<G, (usize, SerdeLeasedBatchPart)>,
    name: &str,
    clients: Arc<Mutex<PersistClientCache>>,
    location: PersistLocation,
    shard_id: ShardId,
) -> (
    Stream<G, FetchedPart<K, V, T, D>>,
    Stream<G, SerdeLeasedBatchPart>,
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
    let (mut tokens_output, tokens_stream) = builder.new_output();

    // NB: we intentionally do _not_ pass along the shutdown button here so that
    // we can be assured we always emit the full contents of a batch. If we used
    // the shutdown token, on Drop, it is possible we'd only partially emit the
    // contents of a batch which could lead to downstream operators seeing records
    // and collections that never existed, which may break their invariants.
    //
    // The downside of this approach is that we may be left doing (considerable)
    // work if the dataflow is dropped but we have a large numbers of parts left
    // in the batch to fetch and yield.
    //
    // This also means that a pre-requisite to this operator is for the input to
    // atomically provide the parts for each batch.
    //
    // Note that this requirement would not be necessary if we were emitting
    // fully consolidated data: https://github.com/MaterializeInc/materialize/issues/16860#issuecomment-1366094925
    let _shutdown_button = builder.build(move |_capabilities| async move {
        let fetcher = {
            let mut clients = clients.lock().await;

            let client = clients
                .open(location.clone())
                .await
                .expect("location should be valid");

            // Unlock the client cache before we do any async work.
            std::mem::drop(clients);

            client.create_batch_fetcher::<K, V, T, D>(shard_id).await
        };

        let mut buffer = Vec::new();

        while let Some(event) = descs_input.next().await {
            if let Event::Data(cap, data) = event {
                // `LeasedBatchPart`es cannot be dropped at this point w/o
                // panicking, so swap them to an owned version.
                data.swap(&mut buffer);

                for (_idx, part) in buffer.drain(..) {
                    let (token, fetched) = fetcher
                        .fetch_leased_part(fetcher.leased_part_from_exchangeable(part))
                        .await;
                    let fetched = fetched.expect("shard_id should match across all workers");
                    {
                        // Do very fine-grained output activation/session
                        // creation to ensure that we don't hold activated
                        // outputs or sessions across await points, which
                        // would prevent messages from being flushed from
                        // the shared timely output buffer.
                        let mut fetched_output = fetched_output.activate();
                        let mut tokens_output = tokens_output.activate();
                        fetched_output.session(&cap).give(fetched);
                        tokens_output
                            .session(&cap)
                            .give(token.into_exchangeable_part());
                    }
                }
            }
        }
    });

    (fetched_stream, tokens_stream)
}

pub(crate) fn shard_source_tokens<T, G>(
    tokens: &Stream<G, SerdeLeasedBatchPart>,
    name: &str,
    consumed_part_tx: mpsc::UnboundedSender<SerdeLeasedBatchPart>,
    chosen_worker: usize,
) where
    T: Timestamp + Lattice + Codec64,
    G: Scope<Timestamp = T>,
{
    let worker_index = tokens.scope().index();

    // This operator is meant to only run on the chosen worker. All workers will
    // exchange their fetched ("consumed") parts back to the leasor.
    let mut builder =
        OperatorBuilder::new(format!("shard_source_tokens({})", name), tokens.scope());

    // Exchange all "consumed" parts back to the chosen worker/leasor.
    let mut tokens_input = builder.new_input(
        tokens,
        Exchange::new(move |_| u64::cast_from(chosen_worker)),
    );

    let name = name.to_owned();
    builder.build(|_initial_capabilities| {
        let mut buffer = Vec::new();

        move |_frontiers| {
            // The chosen worker is the leasor because it issues batches.
            if worker_index != chosen_worker {
                trace!("We are not the batch leasor for {:?}, exiting...", name);
                return;
            }

            while let Some((_cap, data)) = tokens_input.next() {
                data.swap(&mut buffer);

                for part in buffer.drain(..) {
                    if let Err(mpsc::error::SendError(_part)) = consumed_part_tx.send(part) {
                        // Subscribe loop dropped, which drops its ReadHandle,
                        // which in turn drops all leases, so doing anything
                        // else here is both moot and impossible.
                        //
                        // The parts we tried to send will just continue being
                        // `SerdeLeasedBatchPart`'es.
                    }
                }
            }
        }
    });
}
