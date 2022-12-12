// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A source that reads from an a persist shard.

use std::any::Any;
use std::convert::Infallible;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::Hashable;
use futures::StreamExt;
use mz_ore::collections::CollectionExt;
use mz_ore::vec::VecExt;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::{CapabilitySet, OkErr};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::PartialOrder;
use tokio::sync::{mpsc, Mutex};
use tracing::trace;

use mz_expr::MfpPlan;
use mz_ore::cast::CastFrom;
use mz_persist::location::ExternalError;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::fetch::SerdeLeasedBatchPart;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_timely_util::builder_async::{Event, OperatorBuilder as AsyncOperatorBuilder};

use crate::controller::CollectionMetadata;
use crate::types::errors::DataflowError;
use crate::types::sources::SourceData;

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
pub fn persist_source<G, YFn>(
    scope: &G,
    source_id: GlobalId,
    persist_clients: Arc<Mutex<PersistClientCache>>,
    metadata: CollectionMetadata,
    as_of: Option<Antichain<Timestamp>>,
    until: Antichain<Timestamp>,
    map_filter_project: Option<&mut MfpPlan>,
    // Use Infallible to statically ensure that no data can ever be sent. TODO:
    // Replace Infallible with `!` once the latter is stabilized.
    flow_control_input: &Stream<G, Infallible>,
    flow_control_max_inflight_bytes: usize,
    yield_fn: YFn,
) -> (
    Stream<G, (Row, Timestamp, Diff)>,
    Stream<G, (DataflowError, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    let (stream, token) = persist_source_core(
        scope,
        source_id,
        persist_clients,
        metadata,
        as_of,
        until,
        map_filter_project,
        flow_control_input,
        flow_control_max_inflight_bytes,
        yield_fn,
    );
    let (ok_stream, err_stream) = stream.ok_err(|(d, t, r)| match d {
        Ok(row) => Ok((row, t, r)),
        Err(err) => Err((err, t, r)),
    });
    (ok_stream, err_stream, token)
}

/// Informs a `persist_source` to skip flow control on its output
pub const NO_FLOW_CONTROL: usize = usize::MAX;

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
#[allow(clippy::needless_borrow)]
pub fn persist_source_core<G, YFn>(
    scope: &G,
    source_id: GlobalId,
    persist_clients: Arc<Mutex<PersistClientCache>>,
    metadata: CollectionMetadata,
    as_of: Option<Antichain<Timestamp>>,
    until: Antichain<Timestamp>,
    mut map_filter_project: Option<&mut MfpPlan>,
    // Use Infallible to statically ensure that no data can ever be sent. TODO:
    // Replace Infallible with `!` once the latter is stabilized.
    flow_control_input: &Stream<G, Infallible>,
    flow_control_max_inflight_bytes: usize,
    yield_fn: YFn,
) -> (
    Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    YFn: Fn(Instant, usize) -> bool + 'static,
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
    let worker_index = scope.index();
    let peers = scope.peers();
    let chosen_worker = usize::cast_from(source_id.hashed()) % peers;

    // Extract the MFP if it exists; leave behind an identity MFP in that case.
    let mut map_filter_project = map_filter_project.as_mut().map(|mfp| mfp.take());

    // All of these need to be cloned out here because they're moved into the
    // `try_stream!` generator.
    let persist_clients_stream = Arc::<Mutex<PersistClientCache>>::clone(&persist_clients);
    let persist_location_stream = metadata.persist_location.clone();
    let data_shard = metadata.data_shard.clone();
    let as_of_stream = as_of;

    // Connects the consumed part collector operator with the part-issuing
    // Subscribe.
    let (consumed_part_tx, mut consumed_part_rx): (
        mpsc::UnboundedSender<SerdeLeasedBatchPart>,
        mpsc::UnboundedReceiver<SerdeLeasedBatchPart>,
    ) = mpsc::unbounded_channel();

    // We want our async task to notice if we have dropped the token, as we may have reported
    // the dataflow complete and advanced the read frontier of the persist collection, making
    // it unsafe to attempt to open a subscription at `as_of`, leading to a crash. This token
    // reveals whether we are still live to the async stream.
    let handle_creation_token = Arc::new(());
    let weak_handle_token = Arc::downgrade(&handle_creation_token);

    // This channel functions as a token that can be awaited. We never send a message on this
    // channel, but we rely on the drop behavior. If the transmitter is dropped the receiver will
    // return an error. Before the drop occurs the receiver will never return from await.
    let (drop_tx, drop_rx) = tokio::sync::oneshot::channel::<()>();

    let until_clone = until.clone();
    // This is a generator that sets up an async `Stream` that can be continuously polled to get the
    // values that are `yield`-ed from it's body.
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
            let mut persist_clients = persist_clients_stream.lock().await;
            persist_clients.open(persist_location_stream).await
        };

        // This is a moment where we may have dropped our source if our token
        // has been dropped, but if we still hold it we should be good to go.
        if weak_handle_token.upgrade().is_none() {
            return;
        }

        let read = read
            .expect("could not open persist client")
            .open_leased_reader::<SourceData, (), mz_repr::Timestamp, mz_repr::Diff>(
                data_shard,
                &format!("persist_source data {}", source_id),
            )
            .await;

        // This is a moment where we may have dropped our source if our token
        // has been dropped, but if we still hold it we should be good to go.
        if weak_handle_token.upgrade().is_none() {
            return;
        }

        let read = read.expect("could not open persist shard");

        let as_of_stream = as_of_stream.unwrap_or_else(|| read.since().clone());

        // Eagerly yield the initial as_of. This makes sure that the output
        // frontier of the `persist_source` closely tracks the `upper` frontier
        // of the persist shard. It might be that the snapshot for `as_of` is
        // not initially available yet, but this makes sure we already downgrade
        // to it.
        //
        // Downstream consumers might rely on close frontier tracking for making
        // progress. For example, the `persist_sink` needs to know the
        // up-to-date uppper of the output shard to make progress because it
        // will only write out new data once it knows that earlier writes went
        // through, including the initial downgrade of the shard upper to the
        // `as_of`.
        yield (Vec::new(), as_of_stream.clone(), 0);

        // Always poll drop_rx first. drop_rx returns only if drop_tx has been dropped.
        // In this case, we deliberately do not continue on the subscribe. but terminate this task
        let subscription = tokio::select! {
            biased;
            _ = drop_rx => None,
            x = read.subscribe(as_of_stream.clone()) => Some(x),
        };

        // This is a moment where we may have let persist compact if our token
        // has been dropped, but if we still hold it we should be good to go.
        if weak_handle_token.upgrade().is_none() {
            return;
        }

        // We get here only when the token is still present, hence the subscription select
        // must have returned from the subscribe and hence we can be sure that subscripbtion
        // is Some.
        let subscription = subscription.unwrap();

        let mut subscription = subscription.unwrap_or_else(|e| {
            panic!(
                "{source_id}: {} cannot serve requested as_of {:?}: {:?}",
                data_shard, as_of_stream, e
            )
        });

        let mut done = false;
        while !done {
            while let Ok(leased_part) = consumed_part_rx.try_recv() {
                subscription
                    .return_leased_part(subscription.leased_part_from_exchangeable(leased_part));
            }

            let (parts, progress) = subscription.next().await;
            // If `until.less_equal(progress)`, it means that all subsequent batches will
            // contain only times greater or equal to `until`, which means they can be dropped
            // in their entirety. The current batch must be emitted, but we can stop afterwards.
            if PartialOrder::less_equal(&until_clone, &progress) {
                done = true;
            }

            let parts_size_bytes = parts.iter().map(|x| x.encoded_size_bytes()).sum();
            yield (parts, progress, parts_size_bytes);
        }

        // Rather than simply end, we spawn a task that can continue to return leases.
        // This keeps the `ReadHandle` alive until we have confirmed each lease as read.
        mz_ore::task::spawn(|| "LeaseReturner", async move {
            while let Some(leased_part) = consumed_part_rx.recv().await {
                subscription
                    .return_leased_part(subscription.leased_part_from_exchangeable(leased_part));
            }
        });
    });

    let mut pinned_stream = Box::pin(async_stream);

    let mut builder_dist = AsyncOperatorBuilder::new(
        format!("persist_source {}: part distribution", source_id),
        scope.clone(),
    );
    let mut flow_control_input = builder_dist.new_input(flow_control_input, Pipeline);
    let (mut parts_output, parts_stream) = builder_dist.new_output();
    let shutdown_button = builder_dist.build(move |caps| async move {
        let mut cap_set = CapabilitySet::from_elem(caps.into_element());

        let mut current_ts = timely::progress::Timestamp::minimum();
        let mut inflight_bytes = 0;
        let mut inflight_parts: Vec<(Antichain<Timestamp>, usize)> = Vec::new();

        loop {
            // While we have budget left for fetching more parts, read from the
            // subscription and pass them on.
            while inflight_bytes < flow_control_max_inflight_bytes {
                match pinned_stream.next().await {
                    Some(Ok((parts, progress, size_in_bytes))) => {
                        let session_cap = cap_set.delayed(&current_ts);
                        let mut parts_output = parts_output.activate();
                        let mut session = parts_output.session(&session_cap);

                        if size_in_bytes > 0 {
                            inflight_parts.push((progress.clone(), size_in_bytes));
                            inflight_bytes += size_in_bytes;
                            trace!(
                                "shard {} putting {} bytes inflight. total: {}. batch frontier {:?}",
                                data_shard,
                                size_in_bytes,
                                inflight_bytes,
                                progress,
                            );
                        }

                        for part in parts {
                            // Give the part to a random worker.
                            let worker_idx = usize::cast_from(Instant::now().hashed()) % peers;
                            session.give((worker_idx, part.into_exchangeable_part()));
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
            while inflight_bytes >= flow_control_max_inflight_bytes {
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
                        data_shard,
                        size_in_bytes,
                        inflight_bytes,
                        _upper,
                        flow_control_upper,
                    );
                }
            }
        }
    });

    let mut fetcher_builder = AsyncOperatorBuilder::new(
        format!("persist_source {}: part fetcher", source_id),
        scope.clone(),
    );

    let mut fetcher_input = fetcher_builder.new_input(
        &parts_stream,
        Exchange::new(|&(i, _): &(usize, _)| u64::cast_from(i)),
    );
    let (mut update_output, update_output_stream) = fetcher_builder.new_output();
    let (mut consumed_part_output, consumed_part_output_stream) = fetcher_builder.new_output();

    // Re-used state for processing and building rows.
    let mut datum_vec = mz_repr::DatumVec::new();
    let mut row_builder = Row::default();

    fetcher_builder.build(move |_capabilities| async move {
        let fetcher = {
            let mut persist_clients = persist_clients.lock().await;

            let persist_client = persist_clients
                .open(metadata.persist_location.clone())
                .await
                .expect("could not open persist client");

            // Unlock the client cache before we do any async work.
            std::mem::drop(persist_clients);

            persist_client
                .create_batch_fetcher::<SourceData, (), mz_repr::Timestamp, mz_repr::Diff>(
                    data_shard,
                )
                .await
        };

        let mut buffer = Vec::new();

        while let Some(event) = fetcher_input.next().await {
            if let Event::Data(cap, data) = event {
                // `LeasedBatchPart`es cannot be dropped at this point w/o
                // panicking, so swap them to an owned version.
                data.swap(&mut buffer);

                for (_idx, part) in buffer.drain(..) {
                    let (consumed_part, fetched_part) = fetcher
                        .fetch_leased_part(fetcher.leased_part_from_exchangeable(part))
                        .await;
                    let fetched_part = fetched_part
                        .expect("shard_id generated for sources must match across all workers");
                    // SUBTLE: This operator yields back to timely whenever an await returns a
                    // Pending result from the overall async/await state machine `poll`. Since
                    // this is fetching from remote storage, it will yield and thus we can reset
                    // our yield counters here.
                    let mut decode_start = Instant::now();

                    // Apply as much logic to `updates` as we can, before we emit anything.
                    let (updates_size_hint_min, updates_size_hint_max) = fetched_part.size_hint();
                    let mut updates =
                        Vec::with_capacity(updates_size_hint_max.unwrap_or(updates_size_hint_min));
                    for ((key, val), time, diff) in fetched_part {
                        if !until.less_equal(&time) {
                            match (key, val) {
                                (Ok(SourceData(Ok(row))), Ok(())) => {
                                    if let Some(mfp) = &mut map_filter_project {
                                        let arena = mz_repr::RowArena::new();
                                        let mut datums_local = datum_vec.borrow_with(&row);
                                        for result in mfp.evaluate(
                                            &mut datums_local,
                                            &arena,
                                            time,
                                            diff,
                                            |time| !until.less_equal(time),
                                            &mut row_builder,
                                        ) {
                                            match result {
                                                Ok((row, time, diff)) => {
                                                    // Additional `until` filtering due to temporal filters.
                                                    if !until.less_equal(&time) {
                                                        updates.push((Ok(row), time, diff));
                                                    }
                                                }
                                                Err((err, time, diff)) => {
                                                    // Additional `until` filtering due to temporal filters.
                                                    if !until.less_equal(&time) {
                                                        updates.push((Err(err), time, diff));
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        updates.push((Ok(row), time, diff));
                                    }
                                }
                                (Ok(SourceData(Err(err))), Ok(())) => {
                                    updates.push((Err(err), time, diff));
                                }
                                // TODO(petrosagg): error handling
                                (Err(_), Ok(_)) | (Ok(_), Err(_)) | (Err(_), Err(_)) => {
                                    panic!("decoding failed")
                                }
                            }
                        }
                        if yield_fn(decode_start, updates.len()) {
                            // A large part of the point of yielding is to let later operators
                            // reduce down the data, so emit what we have. Note that this means
                            // we don't get to consolidate everything, but that's part of the
                            // tradeoff in tuning yield_fn.
                            differential_dataflow::consolidation::consolidate_updates(&mut updates);

                            {
                                // Do very fine-grained output activation/session
                                // creation to ensure that we don't hold activated
                                // outputs or sessions across await points, which
                                // would prevent messages from being flushed from
                                // the shared timely output buffer.
                                let mut output_handle = update_output.activate();
                                let mut update_session = output_handle.session(&cap);

                                update_session.give_vec(&mut updates);
                            }

                            // Force a yield to give back the timely thread, reactivating on our
                            // way out.
                            tokio::task::yield_now().await;
                            decode_start = Instant::now();
                        }
                    }
                    differential_dataflow::consolidation::consolidate_updates(&mut updates);

                    // Do very fine-grained output activation/session creation
                    // to ensure that we don't hold activated outputs or
                    // sessions across await points, which would prevent
                    // messages from being flushed from the shared timely output
                    // buffer.
                    let mut output_handle = update_output.activate();
                    let mut update_session = output_handle.session(&cap);
                    update_session.give_vec(&mut updates);

                    // Do very fine-grained output activation/session creation
                    // to ensure that we don't hold activated outputs or
                    // sessions across await points, which would prevent
                    // messages from being flushed from the shared timely output
                    // buffer.
                    let mut consumed_part_output_handle = consumed_part_output.activate();
                    let mut consumed_part_session = consumed_part_output_handle.session(&cap);
                    consumed_part_session.give(consumed_part.into_exchangeable_part());
                }
            }
        }
    });

    // This operator is meant to only run on the chosen worker. All workers will
    // exchange their fetched ("consumed") parts back to the leasor.
    let mut consumed_part_builder = OperatorBuilder::new(
        format!("persist_source {}: consumed part collector", source_id),
        scope.clone(),
    );

    // Exchange all "consumed" parts back to the chosen worker/leasor.
    let mut consumed_part_input = consumed_part_builder.new_input(
        &consumed_part_output_stream,
        Exchange::new(move |_| u64::cast_from(chosen_worker)),
    );

    let last_token = Rc::new(shutdown_button.press_on_drop());
    let token = Rc::clone(&last_token);

    consumed_part_builder.build(|_initial_capabilities| {
        let mut buffer = Vec::new();

        move |_frontiers| {
            // The chosen worker is the leasor because it issues batches.
            if worker_index != chosen_worker {
                trace!(
                    "We are not the batch leasor for {:?}, exiting...",
                    source_id
                );
                return;
            }

            while let Some((_cap, data)) = consumed_part_input.next() {
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

    let token = Rc::new((token, handle_creation_token, drop_tx));

    (update_output_stream, token)
}
