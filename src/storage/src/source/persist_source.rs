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
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use differential_dataflow::Hashable;
use futures::Stream as FuturesStream;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::OkErr;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio::sync::{mpsc, Mutex};
use tracing::trace;

use mz_expr::MfpPlan;
use mz_ore::cast::CastFrom;
use mz_persist::location::ExternalError;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::fetch::SerdeLeasedBatchPart;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

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
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
pub fn persist_source<G>(
    scope: &G,
    source_id: GlobalId,
    persist_clients: Arc<Mutex<PersistClientCache>>,
    metadata: CollectionMetadata,
    as_of: Antichain<Timestamp>,
    until: Antichain<Timestamp>,
    map_filter_project: Option<&mut MfpPlan>,
) -> (
    Stream<G, (Row, Timestamp, Diff)>,
    Stream<G, (DataflowError, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    let (stream, token) = persist_source_core(
        scope,
        source_id,
        persist_clients,
        metadata,
        as_of,
        until,
        map_filter_project,
    );
    let (ok_stream, err_stream) = stream.ok_err(|(d, t, r)| match d {
        Ok(row) => Ok((row, t, r)),
        Err(err) => Err((err, t, r)),
    });
    (ok_stream, err_stream, token)
}

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
pub fn persist_source_core<G>(
    scope: &G,
    source_id: GlobalId,
    persist_clients: Arc<Mutex<PersistClientCache>>,
    metadata: CollectionMetadata,
    as_of: Antichain<Timestamp>,
    until: Antichain<Timestamp>,
    mut map_filter_project: Option<&mut MfpPlan>,
) -> (
    Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
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

        let read = persist_clients_stream
            .lock()
            .await
            .open(persist_location_stream)
            .await
            .expect("could not open persist client")
            .open_reader::<SourceData, (), mz_repr::Timestamp, mz_repr::Diff>(data_shard)
            .await
            .expect("could not open persist shard");

        let mut subscription = read
            .subscribe(as_of_stream)
            .await
            .expect("cannot serve requested as_of");

        let mut done = false;
        while !done {
            while let Ok(leased_part) = consumed_part_rx.try_recv() {
                subscription.return_leased_part(leased_part.into());
            }

            let (parts, progress) = subscription.next().await;
            // If `until.less_equal(progress)`, it means that all subsequent batches will
            // contain only times greater or equal to `until`, which means they can be dropped
            // in their entirety. The current batch must be emitted, but we can stop afterwards.
            if timely::PartialOrder::less_equal(&until_clone, &progress) {
                done = true;
            }
            yield (parts, progress);
        }

        // Rather than simply end, we spawn a task that can continue to return leases.
        // This keeps the `ReadHandle` alive until we have confirmed each lease as read.
        mz_ore::task::spawn(|| "LeaseReturner", async move {
            while let Some(leased_part) = consumed_part_rx.recv().await {
                subscription.return_leased_part(leased_part.into());
            }
        });
    });

    let mut pinned_stream = Box::pin(async_stream);

    let (inner, token) = crate::source::util::source(
        scope,
        format!("persist_source {}: part distribution", source_id),
        move |info| {
            let waker_activator = Arc::new(scope.sync_activator_for(&info.address[..]));
            let waker = futures::task::waker(waker_activator);

            let mut current_ts = timely::progress::Timestamp::minimum();

            move |cap_set, output| {
                let mut context = Context::from_waker(&waker);

                while let Poll::Ready(item) = pinned_stream.as_mut().poll_next(&mut context) {
                    match item {
                        Some(Ok((parts, progress))) => {
                            let session_cap = cap_set.delayed(&current_ts);
                            let mut session = output.session(&session_cap);

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
            }
        },
    );

    let mut fetcher_builder = OperatorBuilder::new(
        format!("persist_source {}: part fetcher", source_id),
        scope.clone(),
    );

    let mut fetcher_input = fetcher_builder.new_input(
        &inner,
        Exchange::new(|&(i, _): &(usize, _)| u64::cast_from(i)),
    );
    let (mut update_output, update_output_stream) = fetcher_builder.new_output();
    let (mut consumed_part_output, consumed_part_output_stream) = fetcher_builder.new_output();

    let update_output_port = update_output_stream.name().port;
    let consumed_part_port = consumed_part_output_stream.name().port;

    // Re-used state for processing and building rows.
    let mut datum_vec = mz_repr::DatumVec::new();
    let mut row_builder = Row::default();

    fetcher_builder.build_async(
        scope.clone(),
        move |mut initial_capabilities, frontiers, scheduler| async move {
            let fetcher = persist_clients
                .lock()
                .await
                .open(metadata.persist_location.clone())
                .await
                .expect("could not open persist client")
                .open_reader::<SourceData, (), mz_repr::Timestamp, mz_repr::Diff>(
                    data_shard.clone(),
                )
                .await
                .expect("could not open persist shard")
                .batch_fetcher()
                .await;

            initial_capabilities.clear();

            let mut buffer = Vec::new();

            while scheduler.notified().await {
                // Re-acquire the output handle on each invocation and drop it
                // when we're done.
                let mut output_handle = update_output.activate();
                let mut consumed_part_output_handle = consumed_part_output.activate();

                while let Some((cap, data)) = fetcher_input.next() {
                    // `LeasedBatchPart`es cannot be dropped at this point w/o
                    // panicking, so swap them to an owned version.
                    data.swap(&mut buffer);

                    let update_cap = cap.delayed_for_output(cap.time(), update_output_port);
                    let mut update_session = output_handle.session(&update_cap);

                    let consumed_part_cap = cap.delayed_for_output(cap.time(), consumed_part_port);
                    let mut consumed_part_session =
                        consumed_part_output_handle.session(&consumed_part_cap);

                    for (_idx, part) in buffer.drain(..) {
                        let (consumed_part, fetched_part) =
                            fetcher.fetch_leased_part(part.into()).await;
                        let fetched_part = fetched_part
                            .expect("shard_id generated for sources must match across all workers");

                        // Apply as much logic to `updates` as we can, before we emit anything.
                        let (updates_size_hint_min, updates_size_hint_max) =
                            fetched_part.size_hint();
                        let mut updates = Vec::with_capacity(
                            updates_size_hint_max.unwrap_or(updates_size_hint_min),
                        );
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
                            // TODO: Figure out how to actually make the operator
                            // yield here if this invocation has spent "too much"
                            // time running.
                        }
                        differential_dataflow::consolidation::consolidate_updates(&mut updates);

                        update_session.give_vec(&mut updates);
                        consumed_part_session.give(consumed_part.into_exchangeable_part());
                    }
                }

                // Quit the loop once we know that we processed all input.
                if frontiers.borrow().iter().all(|f| f.is_empty()) {
                    break;
                }
            }
        },
    );

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

    let last_token = Rc::new(token);
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

    let token = Rc::new(token);

    (update_output_stream, token)
}
