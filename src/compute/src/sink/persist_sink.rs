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
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::consolidation::{consolidate, consolidate_updates};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection, Hashable};
use itertools::Itertools;
use mz_compute_types::dyncfgs::ENABLE_PERSIST_SINK_STASH;
use mz_compute_types::sinks::{ComputeSinkDesc, PersistSinkConnection};
use mz_ore::cast::CastFrom;
use mz_ore::collections::HashMap;
use mz_persist_client::batch::{Batch, BatchBuilder, ProtoBatch};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::metrics::{SinkMetrics, SinkWorkerMetrics, UpdateDelta};
use mz_persist_client::operators::shard_source::SnapshotMode;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{Event, OperatorBuilder as AsyncOperatorBuilder};
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{
    Broadcast, Capability, CapabilitySet, ConnectLoop, Feedback, Inspect,
};
use timely::dataflow::{Scope, Stream};
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use timely::PartialOrder;
use tracing::trace;

use crate::compute_state::ComputeState;
use crate::render::sinks::SinkRender;
use crate::render::StartSignal;
use crate::sink::refresh::apply_refresh;

impl<G> SinkRender<G> for PersistSinkConnection<CollectionMetadata>
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_continuous_sink(
        &self,
        compute_state: &mut ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        as_of: Antichain<Timestamp>,
        start_signal: StartSignal,
        sinked_collection: Collection<G, Row, Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        let mut desired_collection = sinked_collection.map(Ok).concat(&err_collection.map(Err));

        // If a `RefreshSchedule` was specified, round up timestamps.
        if let Some(refresh_schedule) = &sink.refresh_schedule {
            desired_collection = apply_refresh(desired_collection, refresh_schedule.clone());
        }

        if sink.up_to != Antichain::default() {
            unimplemented!(
                "UP TO is not supported for persist sinks yet, and shouldn't have been accepted during parsing/planning"
            )
        }

        persist_sink(
            sink_id,
            &self.storage_metadata,
            desired_collection,
            as_of,
            compute_state,
            start_signal,
        )
    }
}

pub(crate) fn persist_sink<G>(
    sink_id: GlobalId,
    target: &CollectionMetadata,
    desired_collection: Collection<G, Result<Row, DataflowError>, Diff>,
    as_of: Antichain<Timestamp>,
    compute_state: &mut ComputeState,
    start_signal: StartSignal,
) -> Option<Rc<dyn Any>>
where
    G: Scope<Timestamp = Timestamp>,
{
    // There is no guarantee that `as_of` is beyond the persist shard's since. If it isn't,
    // instantiating a `persist_source` with it would panic. So instead we leave it to
    // `persist_source` to select an appropriate `as_of`. We only care about times beyond the
    // current shard upper anyway.
    let source_as_of = None;
    let (ok_stream, err_stream, token) = mz_storage_operators::persist_source::persist_source(
        &mut desired_collection.scope(),
        sink_id,
        Arc::clone(&compute_state.persist_clients),
        target.clone(),
        source_as_of,
        SnapshotMode::Include,
        Antichain::new(), // we want all updates
        None,             // no MFP
        compute_state.dataflow_max_inflight_bytes(),
        start_signal,
    );
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
///
/// Note that `mint_batch_descriptions` inspects the frontier of
/// `desired_collection`, and passes the data through to `write_batches`.
/// This is done to avoid a clone of the underlying data so that both
/// operators can have the collection as input.
///
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
    let enable_stash = ENABLE_PERSIST_SINK_STASH.get(&compute_state.worker_config);

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

    let (batch_descriptions, passthrough_desired_stream, mint_token) = mint_batch_descriptions(
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
        &passthrough_desired_stream,
        &persist_collection.inner,
        Arc::clone(&persist_clients),
        enable_stash,
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
    persist_clients: Arc<PersistClientCache>,
    compute_state: &mut crate::compute_state::ComputeState,
) -> (
    Stream<G, (Antichain<Timestamp>, Antichain<Timestamp>)>,
    Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>,
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

    let (mut output, output_stream) = mint_op.new_output();
    let (mut data_output, data_output_stream) = mint_op.new_output();

    // The description and the data-passthrough outputs are both driven by this input, so
    // they use a standard input connection.
    let mut desired_input =
        mint_op.new_input_for_many(desired_stream, Pipeline, [&output, &data_output]);

    // Neither output's capabilities should depend on the feedback input.
    let mut persist_feedback_input =
        mint_op.new_disconnected_input(persist_feedback_stream, Pipeline);

    let shutdown_button = mint_op.build(move |capabilities| async move {
        // Non-active workers should just pass the data through.
        if !active_worker {
            // The description output is entirely driven by the active worker, so we drop
            // its capability here. The data-passthrough output just uses the data
            // capabilities.
            drop(capabilities);
            while let Some(event) = desired_input.next().await {
                match event {
                    Event::Data([_output_cap, data_output_cap], mut data) => {
                        data_output
                            .give_container(&data_output_cap, &mut data)
                            .await;
                    }
                    Event::Progress(_) => {}
                }
            }
            return;
        }

        // The data-passthrough output will use the data capabilities, so we drop
        // its capability here.
        let [desc_cap, _]: [_; 2] = capabilities.try_into().expect("one capability per output");
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
        let mut desired_frontier = Antichain::from_elem(TimelyTimestamp::minimum());
        let mut persist_frontier = Antichain::from_elem(TimelyTimestamp::minimum());

        // The persist_frontier as it was when we last ran through our minting logic.
        // SUBTLE: As described below, we only mint new batch descriptions
        // when the persist frontier moves. We therefore have to encode this
        // one as an `Option<Antichain<T>>` where the change from `None` to
        // `Some([minimum])` is also a change in the frontier. If we didn't
        // do this, we would be stuck at `[minimum]`.
        let mut emitted_persist_frontier: Option<Antichain<_>> = None;

        loop {
            tokio::select! {
                Some(event) = desired_input.next() => {
                    match event {
                        Event::Data([_output_cap, data_output_cap], mut data) => {
                            // Just passthrough the data.
                            data_output.give_container(&data_output_cap, &mut data).await;
                            continue;
                        }
                        Event::Progress(frontier) => {
                            desired_frontier = frontier;
                        }
                    }
                }
                Some(event) = persist_feedback_input.next() => {
                    match event {
                        Event::Data(_cap, _data) => {
                            // This input produces no data.
                            continue;
                        }
                        Event::Progress(frontier) => {
                            persist_frontier = frontier;
                        }
                    }
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
    (output_stream, data_output_stream, token)
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

/// A type to wrap a `correction` buffer taking the difference between
/// existing and desired contents for the sink while collecting metrics
/// on its length and capacity dynamics.
struct CorrectionBuffer<T, R>(Vec<(Result<Row, DataflowError>, T, R)>);

impl<T, R> CorrectionBuffer<T, R> {
    fn with_correction_buffer<F: FnMut(&mut Vec<(Result<Row, DataflowError>, T, R)>) -> O, O>(
        &mut self,
        sink_metrics: &SinkMetrics,
        sink_worker_metrics: &SinkWorkerMetrics,
        mut f: F,
    ) -> O {
        let (old_len, old_capacity) = (self.0.len(), self.0.capacity());
        let output = f(&mut self.0);
        let (new_len, new_capacity) = (self.0.len(), self.0.capacity());
        let (len_delta, capacity_delta) = (
            UpdateDelta::new(new_len, old_len),
            UpdateDelta::new(new_capacity, old_capacity),
        );
        sink_worker_metrics.report_correction_update_totals(new_len, new_capacity);
        sink_metrics.report_correction_update_deltas(len_delta, capacity_delta);
        output
    }
}

/// A stash for storing future updates by time.
#[derive(Default)]
struct UpdateStash<D>(BTreeMap<Timestamp, ConsolidatingVec<D>>);

impl<D: Ord> UpdateStash<D> {
    fn new() -> Self {
        Self(Default::default())
    }

    /// Insert a batch of updates into the stash.
    fn insert(&mut self, mut updates: Vec<(D, Timestamp, Diff)>) {
        consolidate_updates(&mut updates);
        updates.sort_unstable_by_key(|(_, time, _)| *time);

        let mut updates = updates.into_iter().peekable();
        while let Some(&(_, time, _)) = updates.peek() {
            let data = updates
                .peeking_take_while(|(_, t, _)| *t == time)
                .map(|(d, _, r)| (d, r));

            use std::collections::btree_map::Entry;
            match self.0.entry(time) {
                Entry::Vacant(entry) => {
                    entry.insert(data.collect());
                }
                Entry::Occupied(mut entry) => {
                    entry.get_mut().extend(data);
                }
            }
        }
    }

    /// Remove all updates before the given frontier and return an iterator over them.
    fn seal(
        &mut self,
        frontier: &Antichain<Timestamp>,
    ) -> impl Iterator<Item = (D, Timestamp, Diff)> {
        let retain = match frontier.as_option() {
            Some(time) => self.0.split_off(time),
            None => BTreeMap::new(),
        };
        let drain = std::mem::replace(&mut self.0, retain);
        drain
            .into_iter()
            .flat_map(|(t, data)| data.into_iter().map(move |(d, r)| (d, t, r)))
    }
}

/// A vector that consolidates its contents.
///
/// The vector is filled with updates until it reaches capacity. At this point, the updates are
/// consolidated to free up space. This process repeats until the consolidation recovered less than
/// half of the vector's capacity, at which point the capacity is doubled.
struct ConsolidatingVec<D>(Vec<(D, Diff)>);

impl<D: Ord> ConsolidatingVec<D> {
    // Pushes `item` into the vector.
    //
    // If the vector does not have sufficient capacity, we try to consolidate and/or double its
    // capacity.
    //
    // The worst-case cost of this function is O(n log n) in the number of items the vector stores,
    // but amortizes to O(1).
    fn push(&mut self, item: (D, Diff)) {
        let capacity = self.0.capacity();
        if self.0.len() == capacity {
            // The vector is full. First, consolidate to try to recover some space.
            consolidate(&mut self.0);

            // If consolidation didn't free at least half the available capacity, double the
            // capacity. This ensures we won't consolidate over and over again with small gains.
            if self.0.len() > capacity / 2 {
                self.0.reserve(capacity);
            }
        }

        self.0.push(item);
    }
}

impl<D> IntoIterator for ConsolidatingVec<D> {
    type Item = (D, Diff);
    type IntoIter = std::vec::IntoIter<(D, Diff)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<D> FromIterator<(D, Diff)> for ConsolidatingVec<D> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (D, Diff)>,
    {
        Self(Vec::from_iter(iter))
    }
}

impl<D: Ord> Extend<(D, Diff)> for ConsolidatingVec<D> {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (D, Diff)>,
    {
        for item in iter {
            self.push(item);
        }
    }
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
    persist_clients: Arc<PersistClientCache>,
    enable_stash: bool,
) -> (Stream<G, BatchOrData>, Rc<dyn Any>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;
    let target_relation_desc = target.relation_desc.clone();

    let scope = desired_stream.scope();
    let worker_index = scope.index();

    let mut write_op = AsyncOperatorBuilder::new(format!("{} write_batches", operator_name), scope);

    let (mut output, output_stream) = write_op.new_output();

    let mut descriptions_input =
        write_op.new_input_for(&batch_descriptions.broadcast(), Pipeline, &output);
    let mut desired_input = write_op.new_input_for(
        desired_stream,
        Exchange::new(
            move |(row, _ts, _diff): &(Result<Row, DataflowError>, Timestamp, Diff)| row.hashed(),
        ),
        &output,
    );
    // This input is disconnected so that the persist frontier is not taken into account when
    // determining downstream implications. We're only interested in the frontier to know when we
    // are ready to write out new data (when the corrections have "settled"). But the persist
    // frontier must not hold back the downstream frontier, otherwise the `append_batches` operator
    // would never append batches because it waits for its input frontier to advance before it does
    // so. The input frontier would never advance if we don't write new updates to persist, leading
    // to a Catch-22-type situation.
    let mut persist_input = write_op.new_disconnected_input(
        persist_stream,
        Exchange::new(
            move |(row, _ts, _diff): &(Result<Row, DataflowError>, Timestamp, Diff)| row.hashed(),
        ),
    );

    // This operator accepts the current and desired update streams for a `persist` shard.
    // It attempts to write out updates, starting from the current's upper frontier, that
    // will cause the changes of desired to be committed to persist.

    let shutdown_button = write_op.build(move |_capabilities| async move {
        // Contains `desired - persist`, reflecting the updates we would like to commit
        // to `persist` in order to "correct" it to track `desired`. This collection is
        // only modified by updates received from either the `desired` or `persist` inputs.
        let mut correction = CorrectionBuffer(Vec::new());

        // Contains updates from `desired` at times beyond `desired`'s frontier, by time. The idea
        // is to only move updates into `correction` that have a chance of being emitted shortly,
        // to keep the amount of updates we need to consolidate small.
        let mut desired_stash = enable_stash.then(UpdateStash::new);

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

        // TODO(aljoscha): We need to figure out what to do with error results from these calls.
        let persist_client = persist_clients
            .open(persist_location)
            .await
            .expect("could not open persist client");
        let sink_metrics = &persist_client.metrics().sink;
        let sink_worker_metrics = &sink_metrics.for_worker(worker_index);

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
        let mut desired_frontier = Antichain::from_elem(TimelyTimestamp::minimum());
        let mut persist_frontier = Antichain::from_elem(TimelyTimestamp::minimum());

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

                            continue;
                        }
                        Event::Progress(frontier) => {
                            batch_descriptions_frontier = frontier;
                        }
                    }
                }
                Some(event) = desired_input.next() => {
                    match event {
                        Event::Data(_cap, mut data) => {
                            // Extract desired rows into the stash. They are moved into
                            // `correction` in response to frontier advancements.
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
                                    desired_frontier,
                                    batch_descriptions_frontier,
                                    persist_frontier
                                );
                            }

                            if let Some(stash) = &mut desired_stash {
                                stash.insert(data);
                            } else {
                                correction.with_correction_buffer(
                                    sink_metrics,
                                    sink_worker_metrics,
                                    |buffer| buffer.append(&mut data),
                                );
                            }

                            continue;
                        }
                        Event::Progress(frontier) => {
                            // Extract desired rows as positive contributions to `correction`.
                            if let Some(stash) = &mut desired_stash {
                                correction.with_correction_buffer(
                                    sink_metrics,
                                    sink_worker_metrics,
                                    |buffer| buffer.extend(stash.seal(&frontier)),
                                );
                            }

                            desired_frontier = frontier;
                        }
                    }
                }
                Some(event) = persist_input.next() => {
                    match event {
                        Event::Data(_cap, mut data) => {
                            // Extract persist rows as negative contributions to `correction`.
                            correction.with_correction_buffer(
                                sink_metrics,
                                sink_worker_metrics,
                                |buffer| buffer.extend(data.drain(..).map(|(d, t, r)| (d, t, -r))),
                            );

                            continue;
                        }
                        Event::Progress(frontier) => {
                            persist_frontier = frontier;
                        }
                    }
                }
                else => {
                    // All inputs are exhausted, so we can shut down.
                    return;
                }
            }

            // We may have the opportunity to commit updates.
            if !PartialOrder::less_equal(&desired_frontier, &persist_frontier) {
                trace!(
                    "persist_sink {sink_id}/{shard_id}: \
                        CAN emit: \
                        persist_frontier: {:?}, \
                        desired_frontier: {:?}",
                    persist_frontier,
                    desired_frontier
                );
                // Advance all updates to `persist`'s frontier.
                for (row, time, diff) in correction.0.iter_mut() {
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

                if !ready_batches.is_empty() {
                    // Consolidate updates only when they are required by an
                    // attempt to write out new updates. Otherwise, we might
                    // spend a lot of time "consolidating" the same updates
                    // over and over again, with no changes.
                    correction.with_correction_buffer(
                        sink_metrics,
                        sink_worker_metrics,
                        consolidate_updates,
                    );

                    // `correction` starts large as it diffs the initial snapshots,
                    // but in steady state contains substantially fewer updates.
                    // We should regularly shrink it to an appropriate size.
                    // We use a 4x threshold here to ensure that we cannot enter
                    // a resizing cycle without a linear-in-`correction.len()`
                    // number of updates. E.g. a 2x threshold could result in
                    // an allocation that must soon after be re-doubled back to
                    // the current size, then halved, then doubled. We want that
                    // pattern to require a linear number of updates rather than
                    // a constant number.
                    if correction.0.len() < correction.0.capacity() / 4 {
                        correction.with_correction_buffer(
                            sink_metrics,
                            sink_worker_metrics,
                            Vec::shrink_to_fit,
                        );
                    }
                }

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
                        .0
                        .iter()
                        .filter(|(_, time, _)| {
                            batch_lower.less_equal(time) && !batch_upper.less_equal(time)
                        })
                        .peekable();

                    if to_append.peek().is_some() {
                        // We want to pass along the data directly if `to_append` is small, but to avoid
                        // having to iterate through everything twice we'll check if `correction`
                        // is small as a reasonable proxy.
                        let minimum_batch_updates =
                            persist_clients.cfg().sink_minimum_batch_updates();
                        let batch_or_data = if correction.0.len() >= minimum_batch_updates {
                            let batch = write
                                .batch(
                                    to_append.map(|(data, time, diff)| {
                                        ((SourceData(data.clone()), ()), time, diff)
                                    }),
                                    batch_lower.clone(),
                                    batch_upper.clone(),
                                )
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
                            BatchOrData::Data {
                                lower: batch_lower.clone(),
                                upper: batch_upper.clone(),
                                contents: to_append.cloned().collect(),
                            }
                        };

                        output.give(&cap, batch_or_data).await;
                    }
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
) -> (Stream<G, ()>, Rc<dyn Any>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = batch_descriptions.scope();

    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;
    let target_relation_desc = target.relation_desc.clone();

    let operator_name = format!("{} append_batches", operator_name);
    let mut append_op = AsyncOperatorBuilder::new(operator_name, scope.clone());

    // We never output anything, but we update our capabilities based on the
    // persist frontier we know about. So someone can listen on our output
    // frontier and learn about the persist frontier advancing.
    let (mut _output, output_stream) = append_op.new_output();

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

    let shutdown_button = append_op.build(move |mut capabilities| async move {
        if !active_worker {
            return;
        }

        let mut cap_set = CapabilitySet::from_elem(capabilities.pop().expect("missing capability"));

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

        let mut in_flight_batches = HashMap::<
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
                else => {
                    // All inputs are exhausted, so we can shut down.
                    return;
                }
            };

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
                        cap_set.downgrade(batch_upper);
                    }
                    Err(mismatch) => {
                        cap_set.downgrade(mismatch.current.iter());

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
    (output_stream, token)
}
