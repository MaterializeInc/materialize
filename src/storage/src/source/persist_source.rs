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

use differential_dataflow::Hashable;
use futures_util::Stream as FuturesStream;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::{Map, OkErr};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio::sync::{mpsc, Mutex};
use tracing::trace;

use mz_ore::cast::CastFrom;
use mz_persist::location::ExternalError;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::fetch::SerdeLeasedBatch;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_timely_util::async_op;
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

use crate::controller::CollectionMetadata;
use crate::types::errors::DataflowError;
use crate::types::sources::SourceData;

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
pub fn persist_source<G>(
    scope: &G,
    source_id: GlobalId,
    persist_clients: Arc<Mutex<PersistClientCache>>,
    metadata: CollectionMetadata,
    as_of: Antichain<Timestamp>,
) -> (
    Stream<G, (Row, Timestamp, Diff)>,
    Stream<G, (DataflowError, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    let (stream, token) = persist_source_core(scope, source_id, persist_clients, metadata, as_of);
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
) -> (
    Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    // WARNING! If emulating any of this code, you should read the doc string
    // on [`LeasedBatch`] and [`Subscribe`] or will likely run into intentional
    // panics.
    //
    // This source is split as such:
    // 1. Sets up `async_stream`, which only yields data (hollow batches) on
    //    one chosen worker. Generating hollow batches also generates
    //    `consumed_batch` leases on the chosen worker, ensuring
    //    `consumed_batch`s do not get GCed while in flight.
    // 2. Batch distribution: A timely source operator which continuously reads
    //    from that stream, and distributes the data among workers.
    // 3. Batch fetcher: A timely operator which downloads the batch's contents
    //    from S3, and outputs them to a timely stream. Additionally, the
    //    operator returns the `ConsumedBatch` to the original worker, so it
    //    can perform internal bookkeeping.
    // 4. Consumed batch collector: A timely operator running only on the
    //    original worker that collects workers' `LeasedBatch`es. Internally,
    //    this drops the batch's `consumed_batch` lease, allowing compaction to
    //    occur.
    let worker_index = scope.index();
    let peers = scope.peers();
    let chosen_worker = usize::cast_from(source_id.hashed()) % peers;

    // All of these need to be cloned out here because they're moved into the
    // `try_stream!` generator.
    let persist_clients_stream = Arc::<Mutex<PersistClientCache>>::clone(&persist_clients);
    let persist_location_stream = metadata.persist_location.clone();
    let data_shard = metadata.data_shard.clone();
    let as_of_stream = as_of;

    // Connects the consumed batch collector operator with the batch-issuing
    // Subscribe.
    let (consumed_batch_tx, mut consumed_batch_rx): (
        mpsc::UnboundedSender<SerdeLeasedBatch>,
        mpsc::UnboundedReceiver<SerdeLeasedBatch>,
    ) = mpsc::unbounded_channel();

    // This is a generator that sets up an async `Stream` that can be continously polled to get the
    // values that are `yield`-ed from it's body.
    let async_stream = async_stream::try_stream!({
        // Only one worker is responsible for distributing batches
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

        loop {
            while let Ok(leased_batch) = consumed_batch_rx.try_recv() {
                subscription.return_leased_batch(leased_batch.into());
            }

            yield subscription.next().await;
        }
    });

    let mut pinned_stream = Box::pin(async_stream);

    let (inner, token) = crate::source::util::source(
        scope,
        format!("persist_source {:?}: batch distribution", source_id),
        move |info| {
            let waker_activator = Arc::new(scope.sync_activator_for(&info.address[..]));
            let waker = futures_util::task::waker(waker_activator);

            let mut current_ts = 0;

            // `i` gets used to round-robin distribution of hollow batches. We
            // start at a different worker for each source, so as to prevent
            // sources started at the same time from distributing sources in
            // lock step with one another.
            let mut i = usize::cast_from(source_id.hashed()) % peers;

            move |cap_set, output| {
                let mut context = Context::from_waker(&waker);

                while let Poll::Ready(item) = pinned_stream.as_mut().poll_next(&mut context) {
                    match item {
                        Some(Ok(batch)) => {
                            let session_cap = cap_set.delayed(&current_ts);
                            let mut session = output.session(&session_cap);

                            let progress = batch.generate_progress();
                            session.give((i, batch.get_droppable_batch()));

                            // Round robin
                            i = (i + 1) % peers;
                            if let Some(frontier) = progress {
                                cap_set.downgrade(frontier.iter());
                                match frontier.into_option() {
                                    Some(ts) => {
                                        current_ts = ts;
                                    }
                                    None => {
                                        cap_set.downgrade(&[]);
                                        return;
                                    }
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
        format!(
            "persist_source {:?}: batch fetcher {}",
            worker_index, source_id
        ),
        scope.clone(),
    );

    let mut fetcher_input = fetcher_builder.new_input(
        &inner,
        Exchange::new(|&(i, _): &(usize, _)| u64::cast_from(i)),
    );
    let (mut update_output, update_output_stream) = fetcher_builder.new_output();
    let (mut consumed_batch_output, consumed_batch_output_stream) = fetcher_builder.new_output();

    let update_output_port = update_output_stream.name().port;
    let consumed_batch_port = consumed_batch_output_stream.name().port;

    fetcher_builder.build_async(
        scope.clone(),
        async_op!(|initial_capabilities, _frontiers| {
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

            let mut output_handle = update_output.activate();
            let mut consumed_batch_output_handle = consumed_batch_output.activate();

            let mut buffer = Vec::new();

            while let Some((cap, data)) = fetcher_input.next() {
                // `LeasedBatch`es cannot be dropped at this point w/o
                // panicking, so swap them to an owned version.
                data.swap(&mut buffer);

                let update_cap = cap.delayed_for_output(cap.time(), update_output_port);
                let mut update_session = output_handle.session(&update_cap);

                let consumed_batch_cap = cap.delayed_for_output(cap.time(), consumed_batch_port);
                let mut consumed_batch_session =
                    consumed_batch_output_handle.session(&consumed_batch_cap);

                for (_idx, batch) in buffer.drain(..) {
                    let (consumed_batch, updates) = fetcher.fetch_batch(batch.into()).await;

                    let mut updates = updates
                        .expect("shard_id generated for sources must match across all workers");

                    update_session.give_vec(&mut updates);
                    consumed_batch_session.give(consumed_batch.get_droppable_batch());
                }
            }
            false
        }),
    );

    // This operator is meant to only run on the chosen worker. All workers will
    // exchange their fetched batches' `consumed_batch` back to the leasor.
    let mut consumed_batch_builder = OperatorBuilder::new(
        format!("persist_source {:?}: ConsumedBatch collector", source_id),
        scope.clone(),
    );

    // Exchange all `consumed_batch`s back to the chosen worker/leasor.
    let mut consumed_batch_input = consumed_batch_builder.new_input(
        &consumed_batch_output_stream,
        Exchange::new(move |_| u64::cast_from(chosen_worker)),
    );

    let last_token = Rc::new(token);
    let token = Rc::clone(&last_token);

    consumed_batch_builder.build_async(
        scope.clone(),
        async_op!(|initial_capabilities, _frontiers| {
            initial_capabilities.clear();

            // The chosen worker is the leasor because it issues batches.
            if worker_index != chosen_worker {
                trace!(
                    "We are not the batch leasor for {:?}, exiting...",
                    source_id
                );
                return false;
            }

            let mut buffer = Vec::new();

            while let Some((_cap, data)) = consumed_batch_input.next() {
                data.swap(&mut buffer);

                for batch in buffer.drain(..) {
                    if let Err(mpsc::error::SendError(_batch)) = consumed_batch_tx.send(batch) {
                        // Subscribe loop dropped, which drops its ReadHandle,
                        // which in turn drops all leases, so doing anything
                        // else here is both moot and impossible.
                        //
                        // The batches we tried to send will just continue being
                        // `SerdeLeasedBatch`'es.
                    }
                }
            }

            false
        }),
    );

    let stream = update_output_stream.map(|x| match x {
        ((Ok(SourceData(Ok(row))), Ok(())), ts, diff) => (Ok(row), ts, diff),
        ((Ok(SourceData(Err(err))), Ok(())), ts, diff) => (Err(err), ts, diff),
        // TODO(petrosagg): error handling
        _ => panic!("decoding failed"),
    });

    let token = Rc::new(token);

    (stream, token)
}
