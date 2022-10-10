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
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{Collection, Hashable};
use mz_persist_client::batch::Batch;
use mz_persist_client::write::WriterEnrichedHollowBatch;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::{
    Broadcast, Capability, CapabilitySet, ConnectLoop, Feedback, Inspect,
};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use timely::PartialOrder;
use tokio::sync::Mutex;
use tracing::trace;

use mz_compute_client::sinks::{ComputeSinkDesc, PersistSinkConnection};
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage::controller::CollectionMetadata;
use mz_storage::types::errors::DataflowError;
use mz_storage::types::sources::SourceData;
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

use crate::compute_state::ComputeState;
use crate::render::sinks::SinkRender;

impl<G> SinkRender<G> for PersistSinkConnection<CollectionMetadata>
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_continuous_sink(
        &self,
        compute_state: &mut ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        sinked_collection: Collection<G, Row, Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        let desired_collection = sinked_collection.map(Ok).concat(&err_collection.map(Err));

        persist_sink(
            sink_id,
            &self.storage_metadata,
            desired_collection,
            sink.as_of.frontier.clone(),
            compute_state,
        )
    }
}

pub(crate) fn persist_sink<G>(
    sink_id: GlobalId,
    target: &CollectionMetadata,
    desired_collection: Collection<G, Result<Row, DataflowError>, Diff>,
    as_of: Antichain<Timestamp>,
    compute_state: &mut ComputeState,
) -> Option<Rc<dyn Any>>
where
    G: Scope<Timestamp = Timestamp>,
{
    // There is no guarantee that `as_of` is beyond the persist shard's since. If it isn't,
    // instantiating a `persist_source` with it would panic. So instead we leave it to
    // `persist_source` to select an appropriate `as_of`. We only care about times beyond the
    // current shard upper anyway.
    let source_as_of = None;
    let (ok_stream, err_stream, token) = mz_storage::source::persist_source::persist_source(
        &desired_collection.scope(),
        sink_id,
        Arc::clone(&compute_state.persist_clients),
        target.clone(),
        source_as_of,
        Antichain::new(), // we want all updates
        None,             // no MFP
        // Copy the logic in DeltaJoin/Get/Join to start.
        |_timer, count| count > 1_000_000,
    );
    use differential_dataflow::AsCollection;
    let persist_collection = ok_stream
        .as_collection()
        .map(Ok)
        .concat(&err_stream.as_collection().map(Err));

    Some(Rc::new((
        install_desired_into_persist(
            sink_id,
            target,
            desired_collection,
            persist_collection,
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
fn install_desired_into_persist<G>(
    sink_id: GlobalId,
    target: &CollectionMetadata,
    desired_collection: Collection<G, Result<Row, DataflowError>, Diff>,
    persist_collection: Collection<G, Result<Row, DataflowError>, Diff>,
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

    let mut scope = desired_collection.inner.scope();

    // The append operator keeps capabilities that it downgrades to match the
    // current upper frontier of the persist shard. This frontier can be
    // observed on the persist_feedback_stream. This is used by the minter
    // operator to learn about the current persist frontier, driving it's
    // decisions on when to mint new batches.
    //
    // This stream should never carry data, so we don't bother about increasing
    // the timestamp on feeding back using the summary.
    let (persist_feedback_handle, persist_feedback_stream) = scope.feedback(Timestamp::default());

    let (batch_descriptions, mint_token) = mint_batch_descriptions(
        sink_id,
        operator_name.clone(),
        target,
        &desired_collection.inner,
        &persist_feedback_stream,
        as_of,
        Arc::clone(&persist_clients),
        compute_state,
    );

    let (written_batches, write_token) = write_batches(
        sink_id.clone(),
        operator_name.clone(),
        target,
        &batch_descriptions,
        &desired_collection.inner,
        &persist_collection.inner,
        Arc::clone(&persist_clients),
    );

    let (append_frontier_stream, append_token) = append_batches(
        sink_id.clone(),
        operator_name,
        target,
        &batch_descriptions,
        &written_batches,
        persist_clients,
    );

    append_frontier_stream.connect_loop(persist_feedback_handle);

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
    desired_stream: &Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>,
    persist_feedback_stream: &Stream<G, ()>,
    as_of: Antichain<Timestamp>,
    persist_clients: Arc<Mutex<PersistClientCache>>,
    compute_state: &mut crate::compute_state::ComputeState,
) -> (
    Stream<G, (Antichain<Timestamp>, Antichain<Timestamp>)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = desired_stream.scope();

    // Only attempt to write from this frontier onward, as our data are not necessarily
    // correct for times not greater or equal to this frontier.
    let write_lower_bound = as_of;

    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;

    // Only one worker is responsible for determining batch descriptions. All
    // workers must write batches with the same description, to ensure that they
    // can be combined into one batch that gets appended to Consensus state.
    let hashed_id = sink_id.hashed();
    let active_worker = (hashed_id as usize) % scope.peers() == scope.index();

    // Only the "active" operator will mint batches. All other workers have an
    // empty frontier. It's necessary to insert all of these into
    // `compute_state. sink_write_frontier` below so we properly clear out
    // default frontiers of non-active workers.
    let shared_frontier = Rc::new(RefCell::new(if active_worker {
        Antichain::from_elem(TimelyTimestamp::minimum())
    } else {
        Antichain::new()
    }));

    compute_state
        .sink_write_frontiers
        .insert(sink_id, Rc::clone(&shared_frontier));

    let mut mint_op = OperatorBuilder::new(
        format!("{} mint_batch_descriptions", operator_name),
        scope.clone(),
    );

    let (mut output, output_stream) = mint_op.new_output();

    let mut desired_input = mint_op.new_input(desired_stream, Pipeline);
    let mut persist_feedback_input =
        mint_op.new_input_connection(persist_feedback_stream, Pipeline, vec![Antichain::new()]);

    // Dropping this token signals that the operator should shut down cleanly.
    let token = Rc::new(());
    let token_weak = Rc::downgrade(&token);

    mint_op.build_async(
        scope,
        move |mut capabilities, frontiers, scheduler| async move {
            let mut cap_set = if active_worker {
                CapabilitySet::from_elem(capabilities.pop().expect("missing capability"))
            } else {
                // We have to eagerly drop unneeded capabilities!
                capabilities.pop();
                CapabilitySet::new()
            };

            // TODO(aljoscha): We need to figure out what to do with error
            // results from these calls.
            let persist_client = persist_clients
                .lock()
                .await
                .open(persist_location)
                .await
                .expect("could not open persist client");

            let mut write = persist_client
                .open_writer::<SourceData, (), Timestamp, Diff>(shard_id)
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

            // The frontiers as they where when we last ran through our minting
            // logic.
            let mut current_desired_frontier = Antichain::from_elem(TimelyTimestamp::minimum());
            // SUBTLE: As described below, we only mint new batch descriptions
            // when the persist frontier moves. We therefore have to encode this
            // one as an `Option<Antichain<T>>` where the change from `None` to
            // `Some([minimum])` is also a change in the frontier. If we didn't
            // do this, we would be stuck at `[minimum]`.
            let mut current_persist_frontier = None;

            while scheduler.notified().await {
                if token_weak.upgrade().is_none() {
                    return;
                }

                desired_input.for_each(|_cap, _data| {
                    // Just read away data.
                    // WIP: Is this idiomatic timely?
                });
                persist_feedback_input.for_each(|_cap, _data| {
                    // Just read away data.
                    // WIP: Is this idiomatic timely?
                });

                if !active_worker {
                    // SUBTLE: We must not simply return, because this will
                    // de-schedule the operator. Even if we're not active we
                    // must still remain and pump away input updates, otherwise
                    // the one active operator will not have its frontiers
                    // advanced.
                    continue;
                }

                // Capture current frontiers.
                let frontiers = frontiers.borrow().clone();
                let desired_frontier = &frontiers[0];
                let persist_frontier = &frontiers[1];

                if PartialOrder::less_than(&*shared_frontier.borrow(), persist_frontier) {
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
                if PartialOrder::less_than(persist_frontier, desired_frontier)
                    && (current_persist_frontier.is_none()
                        || PartialOrder::less_than(
                            current_persist_frontier.as_ref().unwrap(),
                            persist_frontier,
                        ))
                {
                    let batch_description =
                        (persist_frontier.to_owned(), desired_frontier.to_owned());

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

                    let mut output = output.activate();
                    let mut session = output.session(&cap);
                    session.give(batch_description);

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

                    current_desired_frontier.clone_from(desired_frontier);
                    current_persist_frontier.replace(persist_frontier.clone());
                } else {
                    // WIP: Remove this!
                    if sink_id.is_user() {
                        trace!(
                            "persist_sink {sink_id}/{shard_id}: \
                            NOT MINTING: \
                            desired: {:?}, \
                            persist {:?}, \
                            current_desired: {:?}, \
                            current_persist {:?}, \
                            persist < desired: {}",
                            desired_frontier,
                            persist_frontier,
                            current_desired_frontier,
                            current_persist_frontier,
                            PartialOrder::less_than(persist_frontier, desired_frontier)
                        );
                    }
                    // WIP: Do something smart about this! As it is now, this is
                    // a busy loop that keeps polling for the latest upper from
                    // persist.
                    // We could, for example, pipe the output of
                    // `append_batches` back into this and let it's frontier
                    // (which we have to carefully manage in that operator) tell
                    // us about advances in the shard upper.
                }
            }
        },
    );

    if sink_id.is_user() {
        output_stream.inspect(|d| trace!("batch_description: {:?}", d));
    }

    (output_stream, token)
}

/// Writes `desired_stream - persist_stream` to persist, but only for updates
/// that fall into batch a description that we get via `batch_descriptions`.
/// This forwards a `HollowBatch` for any batch of updates that was written.
fn write_batches<G>(
    sink_id: GlobalId,
    operator_name: String,
    target: &CollectionMetadata,
    batch_descriptions: &Stream<G, (Antichain<Timestamp>, Antichain<Timestamp>)>,
    desired_stream: &Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>,
    persist_stream: &Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>,
    persist_clients: Arc<Mutex<PersistClientCache>>,
) -> (
    Stream<G, WriterEnrichedHollowBatch<Timestamp>>,
    Option<Rc<dyn Any>>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;

    let scope = desired_stream.scope();
    let worker_index = scope.index();

    let mut write_op =
        OperatorBuilder::new(format!("{} write_batches", operator_name), scope.clone());

    let activator = scope.activator_for(&write_op.operator_info().address[..]);

    let (mut output, output_stream) = write_op.new_output();

    let mut descriptions_input = write_op.new_input(&batch_descriptions.broadcast(), Pipeline);
    let mut desired_input = write_op.new_input(
        desired_stream,
        Exchange::new(
            move |(row, _ts, _diff): &(Result<Row, DataflowError>, Timestamp, Diff)| row.hashed(),
        ),
    );
    let mut persist_input = write_op.new_input_connection(
        persist_stream,
        Exchange::new(
            move |(row, _ts, _diff): &(Result<Row, DataflowError>, Timestamp, Diff)| row.hashed(),
        ),
        // This connection specification makes sure that the persist frontier is
        // not taken into account when determining downstream implications.
        // We're only interested in the frontier to know when we are ready to
        // write out new data (when the corrections have "settled"). But the
        // persist frontier must not hold back the downstream frontier,
        // otherwise the `append_batches` operator would never append batches
        // because it waits for its input frontier to advance before it does so.
        // The input frontier would never advance if we don't write new updates
        // to persist, leading to a Catch-22-type situation.
        vec![Antichain::new()],
    );

    // Dropping this token signals that the operator should shut down cleanly.
    let token = Rc::new(());
    let token_weak = Rc::downgrade(&token);

    // This operator accepts the current and desired update streams for a `persist` shard.
    // It attempts to write out updates, starting from the current's upper frontier, that
    // will cause the changes of desired to be committed to persist.

    write_op.build_async(
        scope,
        move |_capabilities, frontiers, scheduler| async move {
            let mut buffer = Vec::new();
            let mut batch_descriptions_buffer = Vec::new();

            // Contains `desired - persist`, reflecting the updates we would like to commit
            // to `persist` in order to "correct" it to track `desired`. This collection is
            // only modified by updates received from either the `desired` or `persist` inputs.
            let mut correction = Vec::new();

            // Contains descriptions of batches for which we know that we can
            // write data. We got these from the "centralized" operator that
            // determines batch descriptions for all writers.
            let mut in_flight_batches: HashMap<
                (Antichain<Timestamp>, Antichain<Timestamp>),
                Capability<Timestamp>,
            > = HashMap::new();

            // TODO(aljoscha): We need to figure out what to do with error results from these calls.
            let persist_client = persist_clients
                .lock()
                .await
                .open(persist_location)
                .await
                .expect("could not open persist client");

            let mut write = persist_client
                .open_writer::<SourceData, (), Timestamp, Diff>(shard_id)
                .await
                .expect("could not open persist shard");

            while scheduler.notified().await {
                if token_weak.upgrade().is_none() {
                    return;
                }

                // Make sure that our write handle lease does not expire!
                //
                // We don't write to _Consensus_ but instead only write batch
                // data to _Blob_ so someone might eventually expire our lease.
                // This could lead to the "leaked blob reaper" eventually
                // removing blobs that have been written but not yet appended to
                // the shard.
                //
                write.maybe_heartbeat_writer().await;
                trace!("persist_sink {sink_id}/{shard_id}: writer heartbeating write handle");
                // NOTE: We schedule ourselves to make sure that we keep
                // heartbeating even in cases where there are no changes in our
                // inputs (updates or frontier changes).
                activator.activate_after(Duration::from_secs(60));

                // Capture current frontiers.
                let frontiers = frontiers.borrow().clone();
                let batch_descriptions_frontier = &frontiers[0];
                let desired_frontier = &frontiers[1];
                let persist_frontier = &frontiers[2];

                // Ingest new batch descriptions.
                descriptions_input.for_each(|cap, data| {
                    data.swap(&mut batch_descriptions_buffer);
                    for description in batch_descriptions_buffer.drain(..) {
                        if sink_id.is_user() {
                            trace!(
                                "persist_sink {sink_id}/{shard_id}: \
                                write_batches: \
                                new_description: {:?}, \
                                desired_frontier: {:?}, \
                                batch_descriptions_frontier: {:?}, \
                                persist_frontier: {:?}",
                                description,
                                desired_frontier,
                                batch_descriptions_frontier,
                                persist_frontier
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
                });

                // Extract desired rows as positive contributions to `correction`.
                desired_input.for_each(|_cap, data| {
                    data.swap(&mut buffer);
                    if sink_id.is_user() && !buffer.is_empty() {
                        trace!(
                            "persist_sink {sink_id}/{shard_id}: \
                            updates: {:?}, \
                            in-flight-batches: {:?}, \
                            desired_frontier: {:?}, \
                            batch_descriptions_frontier: {:?}, \
                            persist_frontier: {:?}",
                            buffer,
                            in_flight_batches,
                            desired_frontier,
                            batch_descriptions_frontier,
                            persist_frontier
                        );
                    }
                    correction.append(&mut buffer);
                });

                // Extract persist rows as negative contributions to `correction`.
                persist_input.for_each(|_cap, data| {
                    data.swap(&mut buffer);
                    correction.extend(buffer.drain(..).map(|(d, t, r)| (d, t, -r)));
                });

                // We may have the opportunity to commit updates.
                if !PartialOrder::less_equal(desired_frontier, persist_frontier) {
                    trace!(
                        "persist_sink {sink_id}/{shard_id}: \
                        CAN emit: \
                        persist_frontier: {:?}, \
                        desired_frontier: {:?}",
                        persist_frontier,
                        desired_frontier
                    );
                    // Advance all updates to `persist`'s frontier.
                    for (row, time, diff) in correction.iter_mut() {
                        let time_before = *time;
                        time.advance_by(persist_frontier.borrow());
                        if sink_id.is_user() && &time_before != time {
                            trace!(
                                "persist_sink {sink_id}/{shard_id}: \
                                advanced {:?}, {}, {} to {}",
                                row,
                                time_before,
                                diff,
                                time
                            );
                        }
                    }
                    // Consolidate updates within.
                    consolidate_updates(&mut correction);

                    if sink_id.is_user() {
                        trace!(
                            "correction: {:?}, in-flight: {:?}",
                            correction,
                            in_flight_batches
                        );
                    }

                    trace!(
                        "persist_sink {sink_id}/{shard_id}: \
                        in-flight batches: {:?}, \
                        batch_descriptions_frontier: {:?}, \
                        desired_frontier: {:?} \
                        persist_frontier: {:?}",
                        in_flight_batches,
                        batch_descriptions_frontier,
                        desired_frontier,
                        persist_frontier
                    );

                    // We can write updates for a given batch description when
                    // a) the batch is not beyond `batch_descriptions_frontier`,
                    // and b) we know that we have seen all updates that would
                    // fall into the batch, from `desired_frontier`.
                    let ready_batches = in_flight_batches
                        .keys()
                        .filter(|(lower, upper)| {
                            !PartialOrder::less_equal(batch_descriptions_frontier, lower)
                                && !PartialOrder::less_than(desired_frontier, upper)
                                && !PartialOrder::less_than(persist_frontier, lower)
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

                        let mut to_append = correction
                            .iter()
                            .filter(|(_, time, _)| {
                                batch_lower.less_equal(time) && !batch_upper.less_equal(time)
                            })
                            .map(|(data, time, diff)| ((SourceData(data.clone()), ()), time, diff))
                            .peekable();

                        let mut batch_tokens = if to_append.peek().is_some() {
                            let batch = write
                                .batch(to_append, batch_lower.clone(), batch_upper.clone())
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

                            vec![batch.into_writer_hollow_batch()]
                        } else {
                            vec![]
                        };

                        let mut output = output.activate();
                        let mut session = output.session(&cap);
                        session.give_vec(&mut batch_tokens);
                    }
                } else {
                    trace!(
                        "persist_sink {sink_id}/{shard_id}: \
                        cannot emit: persist_frontier: {:?}, desired_frontier: {:?}",
                        persist_frontier,
                        desired_frontier
                    );
                }
            }
        },
    );

    if sink_id.is_user() {
        output_stream.inspect(|d| trace!("batch: {:?}", d));
    }

    (output_stream, Some(token))
}

/// Fuses written batches together and appends them to persist using one
/// `compare_and_append` call. Writing only happens for batch descriptions where
/// we know that no future batches will arrive, that is, for those batch
/// descriptions that are not beyond the frontier of both the
/// `batch_descriptions` and `batches` inputs.
fn append_batches<G>(
    sink_id: GlobalId,
    operator_name: String,
    target: &CollectionMetadata,
    batch_descriptions: &Stream<G, (Antichain<Timestamp>, Antichain<Timestamp>)>,
    batches: &Stream<G, WriterEnrichedHollowBatch<Timestamp>>,
    persist_clients: Arc<Mutex<PersistClientCache>>,
) -> (Stream<G, ()>, Option<Rc<dyn Any>>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = batch_descriptions.scope();

    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;

    let operator_name = format!("{} append_batches", operator_name);
    let mut append_op = OperatorBuilder::new(operator_name, scope.clone());

    let activator = scope.activator_for(&append_op.operator_info().address[..]);

    // We never output anything, but we update our capabilities based on the
    // persist frontier we know about. So someone can listen on our output
    // frontier and learn about the persist frontier advancing.
    let (mut _output, output_stream) = append_op.new_output();

    let hashed_id = sink_id.hashed();
    let active_worker = (hashed_id as usize) % scope.peers() == scope.index();

    // This operator wants to completely control the frontier on it's output
    // because it's used to track the latest persist frontier. We update this
    // when we either append to persist successfully or when we learn about a
    // new current frontier because a `compare_and_append` failed. That's why
    // input capability tracking is not connected to the output.
    let mut descriptions_input = append_op.new_input_connection(
        batch_descriptions,
        Exchange::new(move |_| hashed_id),
        vec![Antichain::new()],
    );
    let mut batches_input = append_op.new_input_connection(
        batches,
        Exchange::new(move |_| hashed_id),
        vec![Antichain::new()],
    );

    // Dropping this token signals that the operator should shut down cleanly.
    let token = Rc::new(());
    let token_weak = Rc::downgrade(&token);

    // This operator accepts the batch descriptions and tokens that represent
    // written batches. Written batches get appended to persist when we learn
    // from our input frontiers that we have seen all batches for a given batch
    // description.

    append_op.build_async(
        scope,
        move |mut capabilities, frontiers, scheduler| async move {
            let mut cap_set = if active_worker {
                CapabilitySet::from_elem(capabilities.pop().expect("missing capability"))
            } else {
                // Pop away, because we don't need it.
                capabilities.pop().expect("missing capability");
                CapabilitySet::new()
            };

            let mut description_buffer = Vec::new();
            let mut batch_buffer = Vec::new();

            // Contains descriptions of batches for which we know that we can
            // write data. We got these from the "centralized" operator that
            // determines batch descriptions for all writers.
            let mut in_flight_descriptions: HashSet<(Antichain<Timestamp>, Antichain<Timestamp>)> =
                HashSet::new();

            let mut in_flight_batches: HashMap<
                (Antichain<Timestamp>, Antichain<Timestamp>),
                Vec<Batch<_, _, _, _>>,
            > = HashMap::new();

            // TODO(aljoscha): We need to figure out what to do with error results from these calls.
            let persist_client = persist_clients
                .lock()
                .await
                .open(persist_location)
                .await
                .expect("could not open persist client");

            let mut write = persist_client
                .open_writer::<SourceData, (), Timestamp, Diff>(shard_id)
                .await
                .expect("could not open persist shard");

            while scheduler.notified().await {
                if token_weak.upgrade().is_none() {
                    return;
                }

                if !active_worker {
                    // SUBTLE: This is different from `mint_batch_description`
                    // where the non-active workers have to stay alive and keep
                    // pumping input, to ensure that the one active worker sees
                    // its frontiers advancing.
                    //
                    // In this here operator only the active worker will ever
                    // get messages, so the inactive ones don't have anything
                    // that needs pumping away.
                    return;
                }

                // Make sure that our write handle lease does not expire!
                write.maybe_heartbeat_writer().await;
                trace!("persist_sink {sink_id}/{shard_id}: appender heartbeating write handle");
                // NOTE: We schedule ourselves to make sure that we keep
                // heartbeating even in cases where there are no changes in our
                // inputs (updates or frontier changes).
                activator.activate_after(Duration::from_secs(60));

                // Capture current frontiers.
                let frontiers = frontiers.borrow().clone();
                let batch_description_frontier = &frontiers[0];
                let batches_frontier = &frontiers[1];

                // Ingest new batch descriptions.
                descriptions_input.for_each(|_cap, data| {
                    data.swap(&mut description_buffer);
                    for batch_description in description_buffer.drain(..) {
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
                });

                // Ingest new written batches
                batches_input.for_each(|_cap, data| {
                    data.swap(&mut batch_buffer);
                    for batch in batch_buffer.drain(..) {
                        let batch = write.batch_from_hollow_batch(batch);
                        let batch_description = (batch.lower().clone(), batch.upper().clone());

                        let batches = in_flight_batches
                            .entry(batch_description)
                            .or_insert_with(Vec::new);

                        batches.push(batch);
                    }
                });

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
                    .filter(|(lower, _upper)| !PartialOrder::less_equal(batches_frontier, lower))
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

                    let mut batches = in_flight_batches
                        .remove(&done_batch_metadata)
                        .unwrap_or_else(Vec::new);

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
                        .expect("Indeterminate")
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
                            cap_set.downgrade(batch_upper);
                        }
                        Err(current_upper) => {
                            cap_set.downgrade(current_upper.0.iter());

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
                                current_upper
                            );
                        }
                    }
                }
            }
        },
    );

    (output_stream, Some(token))
}
