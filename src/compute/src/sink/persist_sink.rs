// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::pin;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::{Collection, Hashable};
use futures::StreamExt;
use mz_compute_types::dyncfgs::PERSIST_SINK_OBEY_READ_ONLY;
use mz_compute_types::sinks::{ComputeSinkDesc, PersistSinkConnection};
use mz_ore::cast::CastFrom;
use mz_persist_client::batch::{Batch, BatchBuilder, ProtoBatch};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::operators::shard_source::SnapshotMode;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{Event, OperatorBuilder as AsyncOperatorBuilder};
use serde::{Deserialize, Serialize};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{probe, Broadcast, Capability, CapabilitySet, Inspect};
use timely::dataflow::{Scope, Stream};
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use timely::PartialOrder;
use tokio::sync::watch;
use tracing::trace;

use crate::compute_state::ComputeState;
use crate::render::sinks::SinkRender;
use crate::render::StartSignal;
use crate::sink::correction::Correction;
use crate::sink::refresh::apply_refresh;

impl<G> SinkRender<G> for PersistSinkConnection<CollectionMetadata>
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_sink(
        &self,
        compute_state: &mut ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        as_of: Antichain<Timestamp>,
        start_signal: StartSignal,
        mut ok_collection: Collection<G, Row, Diff>,
        mut err_collection: Collection<G, DataflowError, Diff>,
    ) -> Option<Rc<dyn Any>> {
        // Attach a probe reporting the compute frontier.
        // The `apply_refresh` operator can round up frontiers, making it impossible to accurately
        // track the progress of the computation, so we need to attach the probe before it.
        let mut probe = probe::Handle::default();
        ok_collection = ok_collection.probe_with(&mut probe);
        let collection_state = compute_state.expect_collection_mut(sink_id);
        collection_state.compute_probe = Some(probe);

        // If a `RefreshSchedule` was specified, round up timestamps.
        if let Some(refresh_schedule) = &sink.refresh_schedule {
            ok_collection = apply_refresh(ok_collection, refresh_schedule.clone());
            err_collection = apply_refresh(err_collection, refresh_schedule.clone());
        }

        if sink.up_to != Antichain::default() {
            unimplemented!(
                "UP TO is not supported for persist sinks yet, and shouldn't have been accepted during parsing/planning"
            )
        }

        persist_sink(
            sink_id,
            &self.storage_metadata,
            ok_collection,
            err_collection,
            as_of,
            compute_state,
            start_signal,
        )
    }
}

pub(crate) fn persist_sink<G>(
    sink_id: GlobalId,
    target: &CollectionMetadata,
    ok_collection: Collection<G, Row, Diff>,
    err_collection: Collection<G, DataflowError, Diff>,
    as_of: Antichain<Timestamp>,
    compute_state: &mut ComputeState,
    start_signal: StartSignal,
) -> Option<Rc<dyn Any>>
where
    G: Scope<Timestamp = Timestamp>,
{
    let desired_oks = ok_collection.inner;
    let desired_errs = err_collection.inner;

    // There is no guarantee that `as_of` is beyond the persist shard's since. If it isn't,
    // instantiating a `persist_source` with it would panic. So instead we leave it to
    // `persist_source` to select an appropriate `as_of`. We only care about times beyond the
    // current shard upper anyway.
    let source_as_of = None;
    let (persist_oks, persist_errs, token) = mz_storage_operators::persist_source::persist_source(
        &mut desired_oks.scope(),
        sink_id,
        Arc::clone(&compute_state.persist_clients),
        &compute_state.txns_ctx,
        &compute_state.worker_config,
        target.clone(),
        source_as_of,
        SnapshotMode::Include,
        Antichain::new(), // we want all updates
        None,             // no MFP
        compute_state.dataflow_max_inflight_bytes(),
        start_signal,
        |error| panic!("compute_persist_sink: {error}"),
    );

    Some(Rc::new((
        install_desired_into_persist(
            sink_id,
            target,
            desired_oks,
            desired_errs,
            persist_oks,
            persist_errs,
            as_of,
            compute_state,
        ),
        token,
    )))
}

/// Continuously writes the difference between `persist_stream` and
/// `desired_stream` into persist, such that the persist shard is made to
/// contain the same updates as `desired_stream`. This is done via a multi-stage
/// operator graph:
///
/// 1. `mint_batch_descriptions` emits new batch descriptions whenever the
///    frontier of `persist_stream` advances *and `persist_frontier`* is less
///    than `desired_frontier`. A batch description is a pair of `(lower,
///    upper)` that tells write operators which updates to write and in the end
///    tells the append operator what frontiers to use when calling
///    `append`/`compare_and_append`. This is a single-worker operator.
/// 2. `write_batches` writes the difference between `desired_stream` and
///    `persist_stream` to persist as batches and sends those batches along.
///    This does not yet append the batches to the persist shard, the update are
///    only uploaded/prepared to be appended to a shard. Also: we only write
///    updates for batch descriptions that we learned about from
///    `mint_batch_descriptions`.
/// 3. `append_batches` takes as input the minted batch descriptions and written
///    batches. Whenever the frontiers sufficiently advance, we take a batch
///    description and all the batches that belong to it and append it to the
///    persist shard.
///
/// Note that `mint_batch_descriptions` inspects the frontier of
/// `desired_collection`, and passes the data through to `write_batches`.
/// This is done to avoid a clone of the underlying data so that both
/// operators can have the collection as input.
///
fn install_desired_into_persist<G>(
    sink_id: GlobalId,
    target: &CollectionMetadata,
    desired_oks: Stream<G, (Row, Timestamp, Diff)>,
    desired_errs: Stream<G, (DataflowError, Timestamp, Diff)>,
    persist_oks: Stream<G, (Row, Timestamp, Diff)>,
    persist_errs: Stream<G, (DataflowError, Timestamp, Diff)>,
    as_of: Antichain<Timestamp>,
    compute_state: &mut crate::compute_state::ComputeState,
) -> Option<Rc<dyn Any>>
where
    G: Scope<Timestamp = Timestamp>,
{
    let persist_clients = Arc::clone(&compute_state.persist_clients);
    let shard_id = target.data_shard;

    let operator_name = format!("persist_sink {}", sink_id);

    if sink_id.is_user() {
        trace!(
            "persist_sink {sink_id}/{shard_id}: \
            initial as_of: {:?}",
            as_of
        );
    }

    let obey_read_only_mode = PERSIST_SINK_OBEY_READ_ONLY.get(&compute_state.worker_config);

    let (batch_descriptions, desired_oks, desired_errs, mint_token) = mint_batch_descriptions(
        sink_id,
        operator_name.clone(),
        target,
        &desired_oks,
        &desired_errs,
        as_of,
        Arc::clone(&persist_clients),
        compute_state,
    );

    let (written_batches, write_token) = write_batches(
        sink_id.clone(),
        operator_name.clone(),
        target,
        &batch_descriptions,
        &desired_oks,
        &desired_errs,
        &persist_oks,
        &persist_errs,
        Arc::clone(&persist_clients),
        compute_state.read_only_rx.clone(),
        obey_read_only_mode,
    );

    let append_token = append_batches(
        sink_id.clone(),
        operator_name,
        target,
        &batch_descriptions,
        &written_batches,
        persist_clients,
        compute_state.read_only_rx.clone(),
        obey_read_only_mode,
    );

    let token = Rc::new((mint_token, write_token, append_token));

    Some(token)
}

/// Whenever the frontier advances, this mints a new batch description (lower
/// and upper) that writers should use for writing the next set of batches to
/// persist.
///
/// Only one of the workers does this, meaning there will only be one
/// description in the stream, even in case of multiple timely workers. Use
/// `broadcast()` to, ahem, broadcast, the one description to all downstream
/// write operators/workers.
///
/// This also keeps the shared frontier that is stored in `compute_state` in
/// sync with the upper of the persist shard.
fn mint_batch_descriptions<G>(
    sink_id: GlobalId,
    operator_name: String,
    target: &CollectionMetadata,
    desired_oks: &Stream<G, (Row, Timestamp, Diff)>,
    desired_errs: &Stream<G, (DataflowError, Timestamp, Diff)>,
    as_of: Antichain<Timestamp>,
    persist_clients: Arc<PersistClientCache>,
    compute_state: &mut crate::compute_state::ComputeState,
) -> (
    Stream<G, (Antichain<Timestamp>, Antichain<Timestamp>)>,
    Stream<G, (Row, Timestamp, Diff)>,
    Stream<G, (DataflowError, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = desired_oks.scope();

    // Only attempt to write from this frontier onward, as our data are not necessarily
    // correct for times not greater or equal to this frontier.
    let write_lower_bound = as_of;

    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;
    let target_relation_desc = target.relation_desc.clone();

    // Only one worker is responsible for determining batch descriptions. All
    // workers must write batches with the same description, to ensure that they
    // can be combined into one batch that gets appended to Consensus state.
    let hashed_id = sink_id.hashed();
    let active_worker = usize::cast_from(hashed_id) % scope.peers() == scope.index();

    // Only the "active" operator will mint batches. All other workers have an
    // empty frontier. It's necessary to insert all of these into
    // `compute_state.sink_write_frontier` below so we properly clear out
    // default frontiers of non-active workers.
    let shared_frontier = Rc::new(RefCell::new(if active_worker {
        Antichain::from_elem(TimelyTimestamp::minimum())
    } else {
        Antichain::new()
    }));

    let collection = compute_state.expect_collection_mut(sink_id);
    collection.sink_write_frontier = Some(Rc::clone(&shared_frontier));

    let mut mint_op =
        AsyncOperatorBuilder::new(format!("{} mint_batch_descriptions", operator_name), scope);

    let (mut output, output_stream) = mint_op.new_output::<CapacityContainerBuilder<_>>();
    let (mut desired_oks_output, desired_oks_output_stream) =
        mint_op.new_output::<CapacityContainerBuilder<_>>();
    let (mut desired_errs_output, desired_errs_output_stream) =
        mint_op.new_output::<CapacityContainerBuilder<_>>();

    // The `desired` inputs drive both the description output and their respective passthrough
    // output.
    let mut desired_oks_input =
        mint_op.new_input_for_many(desired_oks, Pipeline, [&output, &desired_oks_output]);
    let mut desired_errs_input =
        mint_op.new_input_for_many(desired_errs, Pipeline, [&output, &desired_errs_output]);

    let shutdown_button = mint_op.build(move |capabilities| async move {
        // Non-active workers should just pass the data through.
        if !active_worker {
            // The description output is entirely driven by the active worker, so we drop
            // its capability here. The data-passthrough outputs just uses the data
            // capabilities.
            drop(capabilities);

            loop {
                tokio::select! {
                    Some(event) = desired_oks_input.next() => {
                        if let Event::Data([_, cap], mut data) = event {
                            desired_oks_output
                                .give_container(&cap, &mut data)
                                .await;
                        }
                    }
                    Some(event) = desired_errs_input.next() => {
                        if let Event::Data([_, cap], mut data) = event {
                            desired_errs_output
                                .give_container(&cap, &mut data)
                                .await;
                        }
                    }
                    // All inputs are exhausted, so we can shut down.
                    else => return,
                }
            }
        }

        // The data-passthrough outputs will use the data capabilities, so we drop
        // their capabilities here.
        let [desc_cap, _, _]: [_; 3] = capabilities.try_into().expect("one capability per output");
        let mut cap_set = CapabilitySet::from_elem(desc_cap);

        // TODO(aljoscha): We need to figure out what to do with error
        // results from these calls.
        let persist_client = persist_clients
            .open(persist_location)
            .await
            .expect("could not open persist client");

        let mut write = persist_client
            .open_writer::<SourceData, (), Timestamp, Diff>(
                shard_id,
                Arc::new(target_relation_desc),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: sink_id.to_string(),
                    handle_purpose: format!(
                        "compute::persist_sink::mint_batch_descriptions {}",
                        sink_id
                    ),
                },
            )
            .await
            .expect("could not open persist shard");

        let mut current_persist_frontier = write.upper().clone();

        // Advance the persist shard's upper to at least our write lower
        // bound.
        if PartialOrder::less_than(&current_persist_frontier, &write_lower_bound) {
            if sink_id.is_user() {
                trace!(
                    "persist_sink {sink_id}/{shard_id}: \
                        advancing to write_lower_bound: {:?}",
                    write_lower_bound
                );
            }

            let empty_updates: &[((SourceData, ()), Timestamp, Diff)] = &[];
            // It's fine if we don't succeed here. This just means that
            // someone else already advanced the persist frontier further,
            // which is great!
            let res = write
                .append(
                    empty_updates,
                    current_persist_frontier.clone(),
                    write_lower_bound.clone(),
                )
                .await
                .expect("invalid usage");

            if sink_id.is_user() {
                trace!(
                    "persist_sink {sink_id}/{shard_id}: \
                        advancing to write_lower_bound result: {:?}",
                    res
                );
            }

            current_persist_frontier.clone_from(&write_lower_bound);
        }

        // The current input frontiers.
        let mut desired_oks_frontier = Antichain::from_elem(TimelyTimestamp::minimum());
        let mut desired_errs_frontier = Antichain::from_elem(TimelyTimestamp::minimum());
        let mut persist_frontier = Antichain::from_elem(TimelyTimestamp::minimum());

        // The persist_frontier as it was when we last ran through our minting logic.
        // SUBTLE: As described below, we only mint new batch descriptions
        // when the persist frontier moves. We therefore have to encode this
        // one as an `Option<Antichain<T>>` where the change from `None` to
        // `Some([minimum])` is also a change in the frontier. If we didn't
        // do this, we would be stuck at `[minimum]`.
        let mut emitted_persist_frontier: Option<Antichain<_>> = None;

        // We go straight to the write handle to learn about the current upper
        // and upper advancements. The "persist_oks" stream is lying to us! For
        // example, when starting the persist_source with an as_of, the upper
        // will jump to that as_of.
        let mut current_upper = persist_frontier.clone();
        let upper_stream = async_stream::stream!({
            loop {
                write.wait_for_upper_past(&current_upper).await;
                current_upper = write.shared_upper();

                if current_upper.is_empty() {
                    // We are done! Report the final upper and then break out.
                    yield current_upper;
                    break;
                } else {
                    yield current_upper.clone()
                }
            }
        });
        let mut upper_stream = pin::pin!(upper_stream);

        loop {
            tokio::select! {
                Some(event) = desired_oks_input.next() => {
                    match event {
                        Event::Data([_, cap], mut data) => {
                            // Just passthrough the data.
                            desired_oks_output.give_container(&cap, &mut data).await;
                            continue;
                        }
                        Event::Progress(frontier) => {
                            desired_oks_frontier = frontier;
                        }
                    }
                }
                Some(event) = desired_errs_input.next() => {
                    match event {
                        Event::Data([_, cap], mut data) => {
                            // Just passthrough the data.
                            desired_errs_output.give_container(&cap, &mut data).await;
                            continue;
                        }
                        Event::Progress(frontier) => {
                            desired_errs_frontier = frontier;
                        }
                    }
                }
                // `StreamExt::next()` only borrows the stream, and the next-fut
                // itself doesn't hold state. So the combination is cancel-safe.
                Some(upper) = upper_stream.next() => {
                    persist_frontier = upper;
                }
                else => {
                    // All inputs are exhausted, so we can shut down.
                    return;
                }
            };

            if PartialOrder::less_than(&*shared_frontier.borrow(), &persist_frontier) {
                if sink_id.is_user() {
                    trace!(
                        "persist_sink {sink_id}/{shard_id}: \
                            updating shared_frontier to {:?}",
                        persist_frontier,
                    );
                }

                // Share that we have finished processing all times less than the persist frontier.
                // Advancing the sink upper communicates to the storage controller that it is
                // permitted to compact our target storage collection up to the new upper. So we
                // must be careful to not advance the sink upper beyond our read frontier.
                shared_frontier.borrow_mut().clear();
                shared_frontier
                    .borrow_mut()
                    .extend(persist_frontier.iter().cloned());
            }

            // We only mint new batch desriptions when:
            //  1. the desired frontier is past the persist frontier
            //  2. the persist frontier has moved since we last emitted a
            //     batch
            //
            // That last point is _subtle_: If we emitted new batch
            // descriptions whenever the desired frontier moves but the
            // persist frontier doesn't move, we would mint overlapping
            // batch descriptions, which would lead to errors when trying to
            // appent batches based on them.
            //
            // We never use the same lower frontier twice.
            // We only emit new batches when the persist frontier moves.
            // A batch description that we mint for a given `lower` will
            // either succeed in being appended, in which case the
            // persist frontier moves. Or it will fail because the
            // persist frontier got moved by someone else, in which case
            // we also won't mint a new batch description for the same
            // frontier.
            let desired_frontier = desired_oks_frontier.meet(&desired_errs_frontier);
            if PartialOrder::less_than(&persist_frontier, &desired_frontier)
                && (emitted_persist_frontier.is_none()
                    || PartialOrder::less_than(
                        emitted_persist_frontier.as_ref().unwrap(),
                        &persist_frontier,
                    ))
            {
                let batch_description = (persist_frontier.to_owned(), desired_frontier.to_owned());

                let lower = batch_description.0.first().unwrap();
                let batch_ts = batch_description.0.first().unwrap().clone();

                let cap = cap_set
                    .try_delayed(&batch_ts)
                    .ok_or_else(|| {
                        format!(
                            "minter cannot delay {:?} to {:?}. \
                                Likely because we already emitted a \
                                batch description and delayed.",
                            cap_set, lower
                        )
                    })
                    .unwrap();

                trace!(
                    "persist_sink {sink_id}/{shard_id}: \
                        new batch_description: {:?}",
                    batch_description
                );

                output.give(&cap, batch_description).await;

                // WIP: We downgrade our capability so that downstream
                // operators (writer and appender) can know when all the
                // writers have had a chance to write updates to persist for
                // a given batch. Just stepping forward feels a bit icky,
                // though.
                let new_batch_frontier = Antichain::from_elem(batch_ts.step_forward());
                trace!(
                    "persist_sink {sink_id}/{shard_id}: \
                        downgrading to {:?}",
                    new_batch_frontier
                );
                let res = cap_set.try_downgrade(new_batch_frontier.iter());
                match res {
                    Ok(_) => (),
                    Err(e) => panic!("in minter: {:?}", e),
                }

                emitted_persist_frontier.replace(persist_frontier.clone());
            }
        }
    });

    if sink_id.is_user() {
        output_stream.inspect(|d| trace!("batch_description: {:?}", d));
    }

    let token = Rc::new(shutdown_button.press_on_drop());
    (
        output_stream,
        desired_oks_output_stream,
        desired_errs_output_stream,
        token,
    )
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum BatchOrData {
    Batch(ProtoBatch),
    Data {
        lower: Antichain<Timestamp>,
        upper: Antichain<Timestamp>,
        contents: Vec<(Result<Row, DataflowError>, Timestamp, Diff)>,
    },
}

/// Writes `desired_stream - persist_stream` to persist, but only for updates
/// that fall into batch a description that we get via `batch_descriptions`.
/// This forwards a `HollowBatch` for any batch of updates that was written.
fn write_batches<G>(
    sink_id: GlobalId,
    operator_name: String,
    target: &CollectionMetadata,
    batch_descriptions: &Stream<G, (Antichain<Timestamp>, Antichain<Timestamp>)>,
    desired_oks: &Stream<G, (Row, Timestamp, Diff)>,
    desired_errs: &Stream<G, (DataflowError, Timestamp, Diff)>,
    persist_oks: &Stream<G, (Row, Timestamp, Diff)>,
    persist_errs: &Stream<G, (DataflowError, Timestamp, Diff)>,
    persist_clients: Arc<PersistClientCache>,
    mut read_only: watch::Receiver<bool>,
    obey_read_only: bool,
) -> (Stream<G, BatchOrData>, Rc<dyn Any>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;
    let target_relation_desc = target.relation_desc.clone();

    let scope = desired_oks.scope();
    let worker_index = scope.index();

    let mut write_op = AsyncOperatorBuilder::new(format!("{} write_batches", operator_name), scope);

    let (mut output, output_stream) = write_op.new_output();

    let mut descriptions_input =
        write_op.new_input_for(&batch_descriptions.broadcast(), Pipeline, &output);
    let mut desired_oks_input = write_op.new_input_for(
        desired_oks,
        Exchange::new(move |(row, _, _): &(Row, _, _)| row.hashed()),
        &output,
    );
    let mut desired_errs_input = write_op.new_input_for(
        desired_errs,
        Exchange::new(move |(error, _, _): &(DataflowError, _, _)| error.hashed()),
        &output,
    );
    // These inputs are disconnected so that the persist frontier is not taken into account when
    // determining downstream implications. We're only interested in the frontier to know when we
    // are ready to write out new data (when the corrections have "settled"). But the persist
    // frontier must not hold back the downstream frontier, otherwise the `append_batches` operator
    // would never append batches because it waits for its input frontier to advance before it does
    // so. The input frontier would never advance if we don't write new updates to persist, leading
    // to a Catch-22-type situation.
    let mut persist_oks_input = write_op.new_disconnected_input(
        persist_oks,
        Exchange::new(move |(row, _, _): &(Row, _, _)| row.hashed()),
    );
    let mut persist_errs_input = write_op.new_disconnected_input(
        persist_errs,
        Exchange::new(move |(error, _, _): &(DataflowError, _, _)| error.hashed()),
    );

    // This operator accepts the current and desired update streams for a `persist` shard.
    // It attempts to write out updates, starting from the current's upper frontier, that
    // will cause the changes of desired to be committed to persist.

    let shutdown_button = write_op.build(move |_capabilities| async move {
        // TODO(aljoscha): We need to figure out what to do with error results from these calls.
        let persist_client = persist_clients
            .open(persist_location)
            .await
            .expect("could not open persist client");
        let sink_metrics = persist_client.metrics().sink.clone();
        let sink_worker_metrics = sink_metrics.for_worker(worker_index);

        // Contains `desired - persist`, reflecting the updates we would like to commit
        // to `persist` in order to "correct" it to track `desired`. These collections are
        // only modified by updates received from either the `desired` or `persist` inputs.
        let mut correction_oks = Correction::new(sink_metrics.clone(), sink_worker_metrics.clone());
        let mut correction_errs = Correction::new(sink_metrics, sink_worker_metrics);

        // Contains descriptions of batches for which we know that we can
        // write data. We got these from the "centralized" operator that
        // determines batch descriptions for all writers.
        //
        // `Antichain` does not implement `Ord`, so we cannot use a `BTreeMap`. We need to search
        // through the map, so we cannot use the `mz_ore` wrapper either.
        #[allow(clippy::disallowed_types)]
        let mut in_flight_batches = std::collections::HashMap::<
            (Antichain<Timestamp>, Antichain<Timestamp>),
            Capability<Timestamp>,
        >::new();

        let mut write = persist_client
            .open_writer::<SourceData, (), Timestamp, Diff>(
                shard_id,
                Arc::new(target_relation_desc),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: sink_id.to_string(),
                    handle_purpose: format!("compute::persist_sink::write_batches {}", sink_id),
                },
            )
            .await
            .expect("could not open persist shard");

        // The current input frontiers.
        let mut batch_descriptions_frontier = Antichain::from_elem(TimelyTimestamp::minimum());
        let mut desired_oks_frontier = Antichain::from_elem(TimelyTimestamp::minimum());
        let mut desired_errs_frontier = Antichain::from_elem(TimelyTimestamp::minimum());
        let mut persist_oks_frontier = Antichain::from_elem(TimelyTimestamp::minimum());
        let mut persist_errs_frontier = Antichain::from_elem(TimelyTimestamp::minimum());

        loop {
            tokio::select! {
                Some(event) = descriptions_input.next() => {
                    match event {
                        Event::Data(cap, data) => {
                            // Ingest new batch descriptions.
                            for description in data {
                                if sink_id.is_user() {
                                    trace!(
                                        "persist_sink {sink_id}/{shard_id}: \
                                            write_batches: \
                                            new_description: {:?}, \
                                            desired_frontier: {:?}, \
                                            batch_descriptions_frontier: {:?}, \
                                            persist_frontier: {:?}",
                                        description,
                                        desired_oks_frontier.meet(&desired_errs_frontier),
                                        batch_descriptions_frontier,
                                        persist_oks_frontier.meet(&persist_errs_frontier),
                                    );
                                }
                                let existing = in_flight_batches.insert(
                                    description.clone(),
                                    cap.delayed(description.0.first().unwrap()),
                                );
                                assert!(
                                    existing.is_none(),
                                    "write_batches: sink {} got more than one \
                                        batch for description {:?}, in-flight: {:?}",
                                    sink_id,
                                    description,
                                    in_flight_batches
                                );
                            }

                            continue;
                        }
                        Event::Progress(frontier) => {
                            batch_descriptions_frontier = frontier;
                        }
                    }
                }
                Some(event) = desired_oks_input.next() => {
                    match event {
                        Event::Data(_cap, data) => {
                            // Extract desired rows as positive contributions to `correction_oks`.
                            if sink_id.is_user() && !data.is_empty() {
                                trace!(
                                    "persist_sink {sink_id}/{shard_id}: \
                                        updates: {:?}, \
                                        in-flight-batches: {:?}, \
                                        desired_frontier: {:?}, \
                                        batch_descriptions_frontier: {:?}, \
                                        persist_frontier: {:?}",
                                    data,
                                    in_flight_batches,
                                    desired_oks_frontier.meet(&desired_errs_frontier),
                                    batch_descriptions_frontier,
                                    persist_oks_frontier.meet(&persist_errs_frontier),
                                );
                            }

                            correction_oks.insert(data);

                            continue;
                        }
                        Event::Progress(frontier) => {
                            desired_oks_frontier = frontier;
                        }
                    }
                }
                Some(event) = desired_errs_input.next() => {
                    match event {
                        Event::Data(_cap, data) => {
                            // Extract desired rows as positive contributions to `correction_errs`.
                            if sink_id.is_user() && !data.is_empty() {
                                trace!(
                                    "persist_sink {sink_id}/{shard_id}: \
                                        updates: {:?}, \
                                        in-flight-batches: {:?}, \
                                        desired_frontier: {:?}, \
                                        batch_descriptions_frontier: {:?}, \
                                        persist_frontier: {:?}",
                                    data,
                                    in_flight_batches,
                                    desired_oks_frontier.meet(&desired_errs_frontier),
                                    batch_descriptions_frontier,
                                    persist_oks_frontier.meet(&persist_errs_frontier),
                                );
                            }

                            correction_errs.insert(data);

                            continue;
                        }
                        Event::Progress(frontier) => {
                            desired_errs_frontier = frontier;
                        }
                    }
                }
                Some(event) = persist_oks_input.next() => {
                    match event {
                        Event::Data(_cap, mut data) => {
                            // Extract persist rows as negative contributions to `correction_oks`.
                            let updates = data.drain(..).map(|(d, t, r)| (d, t, -r)).collect();
                            correction_oks.insert(updates);

                            continue;
                        }
                        Event::Progress(frontier) => {
                            persist_oks_frontier = frontier;
                        }
                    }
                }
                Some(event) = persist_errs_input.next() => {
                    match event {
                        Event::Data(_cap, mut data) => {
                            // Extract persist rows as negative contributions to `correction_errs`.
                            let updates = data.drain(..).map(|(d, t, r)| (d, t, -r)).collect();
                            correction_errs.insert(updates);

                            continue;
                        }
                        Event::Progress(frontier) => {
                            persist_errs_frontier = frontier;
                        }
                    }
                }
                _it_changed = read_only.changed() => {
                    // We might have to append some batches that we stashed
                    // while in read-only mode.
                }
                else => {
                    // All inputs are exhausted, so we can shut down.
                    return;
                }
            }

            let desired_frontier = desired_oks_frontier.meet(&desired_errs_frontier);
            let persist_frontier = persist_oks_frontier.meet(&persist_errs_frontier);

            trace!(
                "persist_sink {sink_id}/{shard_id}: \
                    in-flight batches: {:?}, \
                    batch_descriptions_frontier: {:?}, \
                    desired_frontier: {:?} \
                    persist_frontier: {:?}",
                in_flight_batches,
                batch_descriptions_frontier,
                desired_frontier,
                persist_frontier,
            );

            // Advance all updates to `persist`'s frontier.
            correction_oks.advance_by(&persist_frontier);
            correction_errs.advance_by(&persist_frontier);

            // Discard batch descriptions whose upper is already not beyond the
            // persist frontier. Those have no chance of being applied to the
            // shard succesfully.
            let persist_upper = write.shared_upper();
            in_flight_batches
                .retain(|(lower, _upper), _cap| PartialOrder::less_equal(&persist_upper, lower));

            if obey_read_only && read_only.borrow().clone() {
                // We are not allowed to do writes, so go back to the beginning
                // of the loop.
                //
                // We are bailing here and not earlier to make sure that we keep
                // our corrections buffers up to date, which will potentially
                // consolidate things out, and to make sure that we weed out
                // batches that we can never apply.
                continue;
            }

            // We can write updates for a given batch description when
            // a) the batch is not beyond `batch_descriptions_frontier`,
            // and b) we know that we have seen all updates that would
            // fall into the batch, from `desired_frontier`.
            let ready_batches = in_flight_batches
                .keys()
                .filter(|(lower, upper)| {
                    !PartialOrder::less_equal(&batch_descriptions_frontier, lower)
                        && !PartialOrder::less_than(&desired_frontier, upper)
                        && !PartialOrder::less_than(&persist_frontier, lower)
                })
                .cloned()
                .collect::<Vec<_>>();

            trace!(
                "persist_sink {sink_id}/{shard_id}: \
                    ready batches: {:?}",
                ready_batches,
            );

            for batch_description in ready_batches.into_iter() {
                let cap = in_flight_batches.remove(&batch_description).unwrap();

                if sink_id.is_user() {
                    trace!(
                        "persist_sink {sink_id}/{shard_id}: \
                            emitting done batch: {:?}, cap: {:?}",
                        batch_description,
                        cap
                    );
                }

                let (batch_lower, batch_upper) = batch_description;
                let to_append_oks = correction_oks.updates_within(&batch_lower, &batch_upper);
                let to_append_errs = correction_errs.updates_within(&batch_lower, &batch_upper);
                let update_count = to_append_oks.len() + to_append_errs.len();

                if update_count > 0 {
                    let minimum_batch_updates = persist_clients.cfg().sink_minimum_batch_updates();
                    let batch_or_data = if update_count >= minimum_batch_updates {
                        let oks = to_append_oks.map(|(d, t, r)| ((SourceData(Ok(d)), ()), t, r));
                        let errs = to_append_errs.map(|(d, t, r)| ((SourceData(Err(d)), ()), t, r));
                        let batch = write
                            .batch(oks.chain(errs), batch_lower, batch_upper)
                            .await
                            .expect("invalid usage");

                        if sink_id.is_user() {
                            trace!(
                                "persist_sink {sink_id}/{shard_id}: \
                                wrote batch from worker {}: ({:?}, {:?})",
                                worker_index,
                                batch.lower(),
                                batch.upper()
                            );
                        }
                        BatchOrData::Batch(batch.into_transmittable_batch())
                    } else {
                        let oks = to_append_oks.map(|(d, t, r)| (Ok(d), t, r));
                        let errs = to_append_errs.map(|(d, t, r)| (Err(d), t, r));
                        BatchOrData::Data {
                            lower: batch_lower,
                            upper: batch_upper,
                            contents: oks.chain(errs).collect(),
                        }
                    };

                    output.give(&cap, batch_or_data).await;
                }
            }
        }
    });

    if sink_id.is_user() {
        output_stream.inspect(|d| trace!("batch: {:?}", d));
    }

    let token = Rc::new(shutdown_button.press_on_drop());
    (output_stream, token)
}

/// Fuses written batches together and appends them to persist using one
/// `compare_and_append` call. Writing only happens for batch descriptions where
/// we know that no future batches will arrive, that is, for those batch
/// descriptions that are not beyond the frontier of both the
/// `batch_descriptions` and `batches` inputs.
///
/// To avoid contention over the persist shard, we route all batches to a single worker.
/// This worker may also batch up individual records sent by the upstream operator, as
/// a way to coalesce what would otherwise be many tiny batches into fewer, larger ones.
fn append_batches<G>(
    sink_id: GlobalId,
    operator_name: String,
    target: &CollectionMetadata,
    batch_descriptions: &Stream<G, (Antichain<Timestamp>, Antichain<Timestamp>)>,
    batches: &Stream<G, BatchOrData>,
    persist_clients: Arc<PersistClientCache>,
    mut read_only: watch::Receiver<bool>,
    obey_read_only: bool,
) -> Rc<dyn Any>
where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = batch_descriptions.scope();

    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;
    let target_relation_desc = target.relation_desc.clone();

    let operator_name = format!("{} append_batches", operator_name);
    let mut append_op = AsyncOperatorBuilder::new(operator_name, scope.clone());

    let hashed_id = sink_id.hashed();
    let active_worker = usize::cast_from(hashed_id) % scope.peers() == scope.index();

    // This operator wants to completely control the frontier on it's output
    // because it's used to track the latest persist frontier. We update this
    // when we either append to persist successfully or when we learn about a
    // new current frontier because a `compare_and_append` failed. That's why
    // input capability tracking is not connected to the output.
    let mut descriptions_input =
        append_op.new_disconnected_input(batch_descriptions, Exchange::new(move |_| hashed_id));
    let mut batches_input =
        append_op.new_disconnected_input(batches, Exchange::new(move |_| hashed_id));

    // This operator accepts the batch descriptions and tokens that represent
    // written batches. Written batches get appended to persist when we learn
    // from our input frontiers that we have seen all batches for a given batch
    // description.

    let shutdown_button = append_op.build(move |_capabilities| async move {
        if !active_worker {
            return;
        }

        // Contains descriptions of batches for which we know that we can
        // write data. We got these from the "centralized" operator that
        // determines batch descriptions for all writers.
        //
        // `Antichain` does not implement `Ord`, so we cannot use a `BTreeSet`. We need to search
        // through the set, so we cannot use the `mz_ore` wrapper either.
        #[allow(clippy::disallowed_types)]
        let mut in_flight_descriptions = std::collections::HashSet::<
            (Antichain<Timestamp>, Antichain<Timestamp>)
        >::new();

        #[derive(Debug, Default)]
        struct BatchSet {
            finished: Vec<Batch<SourceData, (), Timestamp, Diff>>,
            incomplete: Option<BatchBuilder<SourceData, (), Timestamp, Diff>>,
        }

        // We use iteration only for weeding out batches that no longer have a
        // chance of being applied. Otherwise we only use insertion and
        // deletion. We don't use iteration order for determining what batches
        // get written in which order.
        #[allow(clippy::disallowed_types)]
        let mut in_flight_batches = std::collections::HashMap::<
            (Antichain<Timestamp>, Antichain<Timestamp>),
            BatchSet,
        >::new();

        // TODO(aljoscha): We need to figure out what to do with error results from these calls.
        let persist_client = persist_clients
            .open(persist_location)
            .await
            .expect("could not open persist client");

        let mut write = persist_client
            .open_writer::<SourceData, (), Timestamp, Diff>(
                shard_id,
                Arc::new(target_relation_desc),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: sink_id.to_string(),
                    handle_purpose: format!("persist_sink::append_batches {}", sink_id),
                },
            )
            .await
            .expect("could not open persist shard");

        // The current input frontiers.
        let mut batch_description_frontier = Antichain::from_elem(TimelyTimestamp::minimum());
        let mut batches_frontier = Antichain::from_elem(TimelyTimestamp::minimum());

        loop {
            tokio::select! {
                Some(event) = descriptions_input.next() => {
                    match event {
                        Event::Data(_cap, data) => {
                            // Ingest new batch descriptions.
                            for batch_description in data {
                                if sink_id.is_user() {
                                    trace!(
                                        "persist_sink {sink_id}/{shard_id}: \
                                            append_batches: sink {}, \
                                            new description: {:?}, \
                                            batch_description_frontier: {:?}",
                                        sink_id,
                                        batch_description,
                                        batch_description_frontier
                                    );
                                }

                                let is_new = in_flight_descriptions.insert(batch_description.clone());

                                assert!(
                                    is_new,
                                    "append_batches: sink {} got more than one batch \
                                        for a given description in-flight: {:?}",
                                    sink_id, in_flight_batches
                                );
                            }

                            continue;
                        }
                        Event::Progress(frontier) => {
                            batch_description_frontier = frontier;
                        }
                    }
                }
                Some(event) = batches_input.next() => {
                    match event {
                        Event::Data(_cap, data) => {
                            // Ingest new written batches
                            for batch in data {
                                match batch {
                                    BatchOrData::Batch(batch) => {
                                        let batch = write.batch_from_transmittable_batch(batch);
                                        let batch_description = (batch.lower().clone(), batch.upper().clone());

                                        let batches = in_flight_batches
                                            .entry(batch_description)
                                            .or_default();

                                        batches.finished.push(batch);
                                    }
                                    BatchOrData::Data { lower, upper, contents } => {
                                        let batches = in_flight_batches
                                            .entry((lower.clone(), upper))
                                            .or_default();
                                        let builder = batches.incomplete.get_or_insert_with(|| {
                                            write.builder(lower)
                                        });
                                        for (data, time, diff) in contents {
                                            persist_client.metrics().sink.forwarded_updates.inc();
                                            builder.add(&SourceData(data), &(), &time, &diff).await.expect("invalid usage");
                                        }
                                    }
                                }

                            }

                            continue;
                        }
                        Event::Progress(frontier) => {
                            batches_frontier = frontier;
                        }
                    }
                }
                _it_changed = read_only.changed() => {
                    // We might have to append some batches that we stashed
                    // while in read-only mode.
                }
                else => {
                    // All inputs are exhausted, so we can shut down.
                    return;
                }
            };

            // Only retain descriptions and batches that still have a chance of
            // being applied.
            let persist_upper = write.shared_upper();
            in_flight_descriptions.retain(|(lower, _upper)| {
                PartialOrder::less_equal(&persist_upper, lower)
            });

            for ((lower, upper), batch_set) in in_flight_batches.iter_mut() {
                if PartialOrder::less_equal(&persist_upper, lower) {
                    continue;
                }

                // We're not keeping this batch. Be nice and delete any data
                // that has been written.
                for batch in batch_set.finished.drain(..) {
                    batch.delete().await;
                }
                if let Some(batch) = batch_set.incomplete.take() {
                    batch
                        .finish(upper.clone()).await
                        .expect("invalid usage")
                        .delete().await;
                }
            }
            // It's annoying that we're first iterating and doing the retain,
            // but we can't do the batch deletion inside retain because we need
            // async.
            in_flight_batches.retain(|(lower, _upper), batch_set| {
                if PartialOrder::less_equal(&persist_upper, lower) {
                    return true;
                }

                // We're not keeping this batch, make sure that the above loop
                // cleared and deleted all the batches.
                assert!(batch_set.finished.is_empty());
                assert!(batch_set.incomplete.is_none());

                false
            });

            if obey_read_only && read_only.borrow().clone() {
                // We are not allowed to do writes, so go back to the beginning
                // of the loop.
                //
                // We are bailing here and not earlier to make sure that we weed
                // out batches that we can never apply and that we keep reading
                // our inputs.
                continue;
            }

            // Peel off any batches that are not beyond the frontier
            // anymore.
            //
            // It is correct to consider batches that are not beyond the
            // `batches_frontier` because it is held back by the writer
            // operator as long as a) the `batch_description_frontier` did
            // not advance and b) as long as the `desired_frontier` has not
            // advanced to the `upper` of a given batch description.

            let mut done_batches = in_flight_descriptions
                .iter()
                .filter(|(lower, _upper)| !PartialOrder::less_equal(&batches_frontier, lower))
                .cloned()
                .collect::<Vec<_>>();

            trace!(
                "persist_sink {sink_id}/{shard_id}: \
                    append_batches: in_flight: {:?}, \
                    done: {:?}, \
                    batch_frontier: {:?}, \
                    batch_description_frontier: {:?}",
                in_flight_descriptions,
                done_batches,
                batches_frontier,
                batch_description_frontier
            );

            // Append batches in order, to ensure that their `lower` and
            // `upper` lign up.
            done_batches.sort_by(|a, b| {
                if PartialOrder::less_than(a, b) {
                    Ordering::Less
                } else if PartialOrder::less_than(b, a) {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            });

            for done_batch_metadata in done_batches.into_iter() {
                in_flight_descriptions.remove(&done_batch_metadata);

                let batch_set = in_flight_batches
                    .remove(&done_batch_metadata)
                    .unwrap_or_default();

                let mut batches = batch_set.finished;
                if let Some(builder) = batch_set.incomplete {
                    let (_lower, upper) = &done_batch_metadata;
                    let batch = builder.finish(upper.clone()).await.expect("invalid usage");
                    batches.push(batch);
                    persist_client.metrics().sink.forwarded_batches.inc();
                }

                trace!(
                    "persist_sink {sink_id}/{shard_id}: \
                        done batch: {:?}, {:?}",
                    done_batch_metadata,
                    batches
                );

                let (batch_lower, batch_upper) = done_batch_metadata;

                let mut to_append = batches.iter_mut().collect::<Vec<_>>();

                let result = write
                    .compare_and_append_batch(
                        &mut to_append[..],
                        batch_lower.clone(),
                        batch_upper.clone(),
                    )
                    .await
                    .expect("Invalid usage");

                if sink_id.is_user() {
                    trace!(
                        "persist_sink {sink_id}/{shard_id}: \
                            append result for batch ({:?} -> {:?}): {:?}",
                        batch_lower,
                        batch_upper,
                        result
                    );
                }

                match result {
                    Ok(()) => {
                        // Nothing to do!
                    }
                    Err(mismatch) => {
                        // Clean up in case we didn't manage to append the
                        // batches to persist.
                        for batch in batches {
                            batch.delete().await;
                        }
                        trace!(
                            "persist_sink({}): invalid upper! \
                                Tried to append batch ({:?} -> {:?}) but upper \
                                is {:?}. This is not a problem, it just means \
                                someone else was faster than us. We will try \
                                again with a new batch description.",
                            sink_id,
                            batch_lower,
                            batch_upper,
                            mismatch.current,
                        );
                    }
                }
            }
        }
    });

    let token = Rc::new(shutdown_button.press_on_drop());
    token
}
