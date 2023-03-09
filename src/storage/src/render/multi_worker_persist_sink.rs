// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Render an operator that persists a source collection.
//!
//! ## Implementation
//!
//! This module defines the `persist_sink` operator, that writes
//! a collection produced by source rendering into a persist shard.
//!
//! It attempts to use all workers to write data to persist, and uses
//! single-instance workers to coordinate work. The below diagram
//! is an overview how it it shaped. There is more information
//! in the doc comments of the top-level functions of this module.
//!
//!```text
//!
//!                                       ,------------.
//!                                       | source     |
//!                                       | collection |
//!                                       +---+--------+
//!                                       /   |
//!                                      /    |
//!                                     /     |
//!                                    /      |
//!                                   /       |
//!                                  /        |
//!                                 /         |
//!                                /          |
//!                               /     ,-+-----------------------.
//!                              /      | mint_batch_descriptions |
//!                             /       | one arbitrary worker    |
//!                            |        +-,--,--------+----+------+
//!                           ,----------´.-´         |     \
//!                       _.-´ |       .-´            |      \
//!                   _.-´     |    .-´               |       \
//!                .-´  .------+----|-------+---------|--------\-----.
//!               /    /            |       |         |         \     \
//!        ,--------------.   ,-----------------.     |     ,-----------------.
//!        | write_batches|   |  write_batches  |     |     |  write_batches  |
//!        | worker 0     |   | worker 1        |     |     | worker N        |
//!        +-----+--------+   +-+---------------+     |     +--+--------------+
//!               \              \                    |        /
//!                `-.            `,                  |       /
//!                   `-._          `-.               |      /
//!                       `-._         `-.            |     /
//!                           `---------. `-.         |    /
//!                                     +`---`---+-------------,
//!                                     | append_batches       |
//!                                     | one arbitrary worker |
//!                                     +------+---------------+
//!```
//!
//! ## Similarities with `mz_compute::sink::persist_sink`
//!
//! This module has many similarities with the compute version of
//! the same concept, and in fact, is entirely derived from it.
//!
//! Compute requires that its `persist_sink` is _self-correcting_;
//! that is, it corrects what the collection in persist
//! accumulates to if the collection has values changed at
//! previous timestamps. It does this by continually comparing
//! the input stream with the collection as read back from persist.
//!
//! Source collections, while definite, cannot be reliably by
//! re-produced once written down, which means compute's
//! `persist_sink`'s self-correction mechanism would need to be
//! skipped on operator startup, and would cause unnecessary read
//! load on persist.
//!
//! Additionally, persisting sources requires we use bounded
//! amounts of memory, even if a single timestamp represents
//! a huge amount of data. This is not (currently) possible
//! to guarantee while also performing self-correction.
//!
//! Because of this, we have ripped out the self-correction
//! mechanism, and aggressively simplified the sub-operators.
//! Some, particularly `append_batches` could be merged with
//! the compute version, but that requires some amount of
//! onerous refactoring that we have chosen to skip for now.
//!
// TODO(guswynn): merge at least the `append_batches` operator`

use std::any::Any;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::AddAssign;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::{Collection, Hashable};
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{Broadcast, Capability, CapabilitySet, Inspect};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use timely::PartialOrder;
use tracing::trace;

use mz_ore::cast::CastFrom;
use mz_ore::collections::HashMap;
use mz_persist_client::batch::Batch;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::write::WriterEnrichedHollowBatch;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::types::errors::DataflowError;
use mz_storage_client::types::sources::SourceData;
use mz_timely_util::builder_async::{Event, OperatorBuilder as AsyncOperatorBuilder};

use crate::source::types::SourcePersistSinkMetrics;
use crate::storage_state::StorageState;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct BatchMetrics {
    inserts: u64,
    retractions: u64,
    error_inserts: u64,
    error_retractions: u64,
}

impl AddAssign<&BatchMetrics> for BatchMetrics {
    fn add_assign(&mut self, rhs: &BatchMetrics) {
        self.inserts += rhs.inserts;
        self.retractions += rhs.retractions;
        self.error_inserts += rhs.error_inserts;
        self.error_retractions += rhs.error_retractions;
    }
}

impl BatchMetrics {
    fn is_empty(&self) -> bool {
        self.inserts == 0
            && self.retractions == 0
            && self.error_inserts == 0
            && self.error_retractions == 0
    }
}

struct BatchBuilderAndMetdata<K, V, T, D>
where
    T: timely::progress::Timestamp
        + differential_dataflow::lattice::Lattice
        + mz_persist_types::Codec64,
{
    builder: mz_persist_client::batch::BatchBuilder<K, V, T, D>,
    metrics: BatchMetrics,
}

impl<K, V, T, D> BatchBuilderAndMetdata<K, V, T, D>
where
    T: timely::progress::Timestamp
        + differential_dataflow::lattice::Lattice
        + mz_persist_types::Codec64,
{
    fn new(bb: mz_persist_client::batch::BatchBuilder<K, V, T, D>) -> Self {
        BatchBuilderAndMetdata {
            builder: bb,
            metrics: Default::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct HollowBatchAndMetadata {
    lower: Antichain<Timestamp>,
    upper: Antichain<Timestamp>,
    batch: WriterEnrichedHollowBatch<Timestamp>,
    metrics: BatchMetrics,
}

/// Continuously writes the `desired_stream` into persist
/// This is done via a multi-stage operator graph:
///
/// 1. `mint_batch_descriptions` emits new batch descriptions whenever the
///    frontier of `desired_collection` advances. A batch description is
///    a pair of `(lower, upper)` that tells write operators
///    which updates to write and in the end tells the append operator
///    what frontiers to use when calling `append`/`compare_and_append`.
///    This is a single-worker operator.
/// 2. `write_batches` writes the `desired_collection` to persist as
///    batches and sends those batches along.
///    This does not yet append the batches to the persist shard, the update are
///    only uploaded/prepared to be appended to a shard. Also: we only write
///    updates for batch descriptions that we learned about from
///    `mint_batch_descriptions`.
/// 3. `append_batches` takes as input the minted batch descriptions and written
///    batches. Whenever the frontiers sufficiently advance, we take a batch
///    description and all the batches that belong to it and append it to the
///    persist shard.
pub(crate) fn render<G>(
    scope: &mut G,
    collection_id: GlobalId,
    target: CollectionMetadata,
    desired_collection: Collection<G, Result<Row, DataflowError>, Diff>,
    storage_state: &mut StorageState,
    metrics: SourcePersistSinkMetrics,
    output_index: usize,
) -> Rc<dyn Any>
where
    G: Scope<Timestamp = Timestamp>,
{
    let persist_clients = Arc::clone(&storage_state.persist_clients);
    let shard_id = target.data_shard;

    let operator_name = format!("persist_sink({})", shard_id);

    let (batch_descriptions, mint_token) = mint_batch_descriptions(
        scope,
        collection_id,
        operator_name.clone(),
        &target,
        &desired_collection.inner,
        Arc::clone(&persist_clients),
    );

    let (written_batches, write_token) = write_batches(
        scope,
        collection_id.clone(),
        operator_name.clone(),
        &target,
        &batch_descriptions,
        &desired_collection.inner,
        Arc::clone(&persist_clients),
        storage_state,
    );

    let append_token = append_batches(
        scope,
        collection_id.clone(),
        operator_name,
        &target,
        &batch_descriptions,
        &written_batches,
        persist_clients,
        storage_state,
        output_index,
        metrics,
    );

    Rc::new((mint_token, write_token, append_token))
}

/// Whenever the frontier advances, this mints a new batch description (lower
/// and upper) that writers should use for writing the next set of batches to
/// persist.
///
/// Only one of the workers does this, meaning there will only be one
/// description in the stream, even in case of multiple timely workers. Use
/// `broadcast()` to, ahem, broadcast, the one description to all downstream
/// write operators/workers.
fn mint_batch_descriptions<G>(
    scope: &mut G,
    collection_id: GlobalId,
    operator_name: String,
    target: &CollectionMetadata,
    desired_stream: &Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>,
    persist_clients: Arc<PersistClientCache>,
) -> (
    Stream<G, (Antichain<Timestamp>, Antichain<Timestamp>)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;
    let target_relation_desc = target.relation_desc.clone();

    // Only one worker is responsible for determining batch descriptions. All
    // workers must write batches with the same description, to ensure that they
    // can be combined into one batch that gets appended to Consensus state.
    let hashed_id = collection_id.hashed();
    let active_worker = usize::cast_from(hashed_id) % scope.peers() == scope.index();

    // Only the "active" operator will mint batches. All other workers have an
    // empty frontier. It's necessary to insert all of these into
    // `compute_state.sink_write_frontier` below so we properly clear out
    // default frontiers of non-active workers.

    let mut mint_op = AsyncOperatorBuilder::new(
        format!("{} mint_batch_descriptions", operator_name),
        scope.clone(),
    );

    let (mut output, output_stream) = mint_op.new_output();

    let mut desired_input = mint_op.new_input(desired_stream, Pipeline);

    let shutdown_button = mint_op.build(move |mut capabilities| async move {
        let mut cap_set = CapabilitySet::from_elem(capabilities.pop().expect("missing capability"));

        if !active_worker {
            // The non-active workers report that they are done snapshotting.
            return;
        }

        // Initialize this operators's `upper` to the `upper` of the persist shard we are writing
        // to. Data from the source not beyond this time will be dropped, as it has already
        // been persisted.
        // In the future, sources will avoid passing through data not beyond this upper
        let mut current_upper = {
            // TODO(aljoscha): We need to figure out what to do with error
            // results from these calls.
            let persist_client = persist_clients
                .open(persist_location)
                .await
                .expect("could not open persist client");

            let write = persist_client
                .open_writer::<SourceData, (), Timestamp, Diff>(
                    shard_id,
                    &format!(
                        "compute::persist_sink::mint_batch_descriptions {}",
                        collection_id
                    ),
                    Arc::new(target_relation_desc),
                    Arc::new(UnitSchema),
                )
                .await
                .expect("could not open persist shard");

            write.upper().clone()
        };

        // The current input frontiers.
        let mut desired_frontier;

        loop {
            if let Some(event) = desired_input.next().await {
                match event {
                    Event::Data(_cap, _data) => {
                        // Just read away data.
                        continue;
                    }
                    Event::Progress(frontier) => {
                        desired_frontier = frontier;
                    }
                }
            } else {
                // Input is exhausted, so we can shut down.
                return;
            };

            // If the new frontier for the data input has progressed, produce a batch description.
            if PartialOrder::less_than(&current_upper, &desired_frontier) {
                // The maximal description range we can produce.
                let batch_description = (current_upper.to_owned(), desired_frontier.to_owned());

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
                    "persist_sink {collection_id}/{shard_id}: \
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
                    "persist_sink {collection_id}/{shard_id}: \
                        downgrading to {:?}",
                    new_batch_frontier
                );
                let res = cap_set.try_downgrade(new_batch_frontier.iter());
                match res {
                    Ok(_) => (),
                    Err(e) => panic!("in minter: {:?}", e),
                }

                // After successfully emitting a new description, we can update the upper for the
                // operator.
                current_upper = desired_frontier.to_owned();
            }
        }
    });

    if collection_id.is_user() {
        output_stream.inspect(|d| trace!("batch_description: {:?}", d));
    }

    let token = Rc::new(shutdown_button.press_on_drop());
    (output_stream, token)
}

/// Writes `desired_stream` to persist, but only for updates
/// that fall into batch a description that we get via `batch_descriptions`.
/// This forwards a `HollowBatch` (with additional metadata)
/// for any batch of updates that was written.
///
/// This also and updates various metrics.
fn write_batches<G>(
    scope: &mut G,
    collection_id: GlobalId,
    operator_name: String,
    target: &CollectionMetadata,
    batch_descriptions: &Stream<G, (Antichain<Timestamp>, Antichain<Timestamp>)>,
    desired_stream: &Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>,
    persist_clients: Arc<PersistClientCache>,
    storage_state: &mut StorageState,
) -> (Stream<G, HollowBatchAndMetadata>, Rc<dyn Any>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let worker_index = scope.index();

    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;
    let target_relation_desc = target.relation_desc.clone();

    let source_statistics = storage_state
        .source_statistics
        .get(&collection_id)
        .expect("statistics initialized")
        .clone();

    let mut write_op =
        AsyncOperatorBuilder::new(format!("{} write_batches", operator_name), scope.clone());

    let (mut output, output_stream) = write_op.new_output();

    let mut descriptions_input = write_op.new_input(&batch_descriptions.broadcast(), Pipeline);
    let mut desired_input = write_op.new_input(desired_stream, Pipeline);

    // This operator accepts the current and desired update streams for a `persist` shard.
    // It attempts to write out updates, starting from the current's upper frontier, that
    // will cause the changes of desired to be committed to persist, _but only those also past the
    // upper_.

    let shutdown_button = write_op.build(move |_capabilities| async move {
        // In-progress batches of data, keyed by timestamp.
        let mut stashed_batches = BTreeMap::new();

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

        let mut write = persist_client
            .open_writer::<SourceData, (), Timestamp, Diff>(
                shard_id,
                &format!("compute::persist_sink::write_batches {}", collection_id),
                Arc::new(target_relation_desc),
                Arc::new(UnitSchema),
            )
            .await
            .expect("could not open persist shard");

        // The current input frontiers.
        let mut batch_descriptions_frontier = Antichain::from_elem(TimelyTimestamp::minimum());
        let mut desired_frontier = Antichain::from_elem(TimelyTimestamp::minimum());

        let mut current_upper = Antichain::from_elem(TimelyTimestamp::minimum());

        loop {
            tokio::select! {
                Some(event) = descriptions_input.next_mut() => {
                    match event {
                        Event::Data(cap, data) => {
                            // Ingest new batch descriptions.
                            for description in data.drain(..) {
                                if collection_id.is_user() {
                                    trace!(
                                        "persist_sink {collection_id}/{shard_id}: \
                                            write_batches: \
                                            new_description: {:?}, \
                                            desired_frontier: {:?}, \
                                            batch_descriptions_frontier: {:?}",
                                        description,
                                        desired_frontier,
                                        batch_descriptions_frontier,
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
                                    collection_id,
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
                Some(event) = desired_input.next_mut() => {
                    match event {
                        Event::Data(_cap, data) => {
                            // Extract desired rows as positive contributions to `correction`.
                            if collection_id.is_user() && !data.is_empty() {
                                trace!(
                                    "persist_sink {collection_id}/{shard_id}: \
                                        updates: {:?}, \
                                        in-flight-batches: {:?}, \
                                        desired_frontier: {:?}, \
                                        batch_descriptions_frontier: {:?}",
                                    data,
                                    in_flight_batches,
                                    desired_frontier,
                                    batch_descriptions_frontier,
                                );
                            }

                            // TODO: come up with a better default batch size here
                            // (100 was chosen arbitrarily), and avoid having to make a batch
                            // per-timestamp.
                            for (row, ts, diff) in data.drain(..) {
                                if write.upper().less_equal(&ts){
                                    let builder = stashed_batches.entry(ts).or_insert_with(|| {
                                        BatchBuilderAndMetdata::new(
                                            write.builder(
                                                // SUBTLE:
                                                // The choice of the minimum antichain here
                                                // over one that contains `ts` may seem odd, but
                                                // currently, in persist, appended batches must
                                                // be supersets of the of the `compare_and_append`
                                                // description. This means we must choose a lower
                                                // that is not greater than any batch description
                                                // that may be grouped with this batch later on.
                                                Antichain::from_elem(
                                                    Timestamp::minimum()
                                                ),
                                            ),
                                        )
                                    });

                                    let is_value = row.is_ok();
                                    builder
                                        .builder
                                        .add(&SourceData(row), &(), &ts, &diff)
                                        .await
                                        .expect("invalid usage");

                                    source_statistics.inc_updates_staged_by(1);

                                    // Note that we assume `diff` is either +1 or -1 here, being anything
                                    // else is a logic bug we can't handle at the metric layer. We also
                                    // assume this addition doesn't overflow.
                                    match (is_value, diff.is_positive()) {
                                        (true, true) => {
                                            builder.metrics.inserts += diff.unsigned_abs()
                                        },
                                        (true, false) => {
                                            builder.metrics.retractions += diff.unsigned_abs()
                                        },
                                        (false, true) => {
                                            builder.metrics.error_inserts += diff.unsigned_abs()
                                        },
                                        (false, false) => {
                                            builder.metrics.error_retractions += diff.unsigned_abs()
                                        },
                                    }
                                }
                            }

                            continue;
                        }
                        Event::Progress(frontier) => {
                            desired_frontier = frontier;
                        }
                    }
                }
                else => {
                    // All inputs are exhausted, so we can shut down.
                    return;
                }
            }

            // We may have the opportunity to commit updates.
            if PartialOrder::less_equal(&current_upper, &desired_frontier) {
                trace!(
                    "persist_sink {collection_id}/{shard_id}: \
                        CAN emit: \
                        current_upper: {:?}, \
                        desired_frontier: {:?}",
                    current_upper,
                    desired_frontier
                );

                trace!(
                    "persist_sink {collection_id}/{shard_id}: \
                        in-flight batches: {:?}, \
                        batch_descriptions_frontier: {:?}, \
                        desired_frontier: {:?}",
                    in_flight_batches,
                    batch_descriptions_frontier,
                    desired_frontier,
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
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                trace!(
                    "persist_sink {collection_id}/{shard_id}: \
                        ready batches: {:?}",
                    ready_batches,
                );

                for batch_description in ready_batches.into_iter() {
                    let cap = in_flight_batches.remove(&batch_description).unwrap();

                    if collection_id.is_user() {
                        trace!(
                            "persist_sink {collection_id}/{shard_id}: \
                                emitting done batch: {:?}, cap: {:?}",
                            batch_description,
                            cap
                        );
                    }

                    let (batch_lower, batch_upper) = batch_description;

                    let mut finalized_timestamps: Vec<_> = stashed_batches
                        .keys()
                        .filter(|time| {
                            batch_lower.less_equal(time) && !batch_upper.less_equal(time)
                        })
                        .copied()
                        .collect();

                    // TODO(guswynn): is this sorting required?
                    finalized_timestamps.sort_unstable();

                    let mut batch_tokens = vec![];
                    for ts in finalized_timestamps {
                        let batch_builder = stashed_batches.remove(&ts).unwrap();
                        let batch = batch_builder
                            .builder
                            .finish(batch_upper.clone())
                            .await
                            .expect("invalid usage");

                        if collection_id.is_user() {
                            trace!(
                                "persist_sink {collection_id}/{shard_id}: \
                                    wrote batch from worker {}: ({:?}, {:?}),
                                    containing {:?}",
                                worker_index,
                                batch_lower,
                                batch_upper,
                                batch_builder.metrics
                            );
                        }
                        batch_tokens.push(HollowBatchAndMetadata {
                            lower: batch_lower.clone(),
                            upper: batch_upper.clone(),
                            batch: batch.into_writer_hollow_batch(),
                            metrics: batch_builder.metrics,
                        })
                    }

                    let mut output = output.activate();
                    let mut session = output.session(&cap);
                    session.give_vec(&mut batch_tokens);

                    current_upper = desired_frontier.clone();
                }
            } else {
                trace!(
                    "persist_sink {collection_id}/{shard_id}: \
                        cannot emit: current_upper: {:?}, \
                        desired_frontier: {:?}",
                    current_upper,
                    desired_frontier
                );
            }
        }
    });

    if collection_id.is_user() {
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
/// This also keeps the shared frontier that is stored in `compute_state` in
/// sync with the upper of the persist shard, and updates various metrics
/// and statistics objects.
fn append_batches<G>(
    scope: &mut G,
    collection_id: GlobalId,
    operator_name: String,
    target: &CollectionMetadata,
    batch_descriptions: &Stream<G, (Antichain<Timestamp>, Antichain<Timestamp>)>,
    batches: &Stream<G, HollowBatchAndMetadata>,
    persist_clients: Arc<PersistClientCache>,
    storage_state: &mut StorageState,
    output_index: usize,
    metrics: SourcePersistSinkMetrics,
) -> Rc<dyn Any>
where
    G: Scope<Timestamp = Timestamp>,
{
    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;
    let target_relation_desc = target.relation_desc.clone();

    let operator_name = format!("{} append_batches", operator_name);
    let mut append_op = AsyncOperatorBuilder::new(operator_name, scope.clone());

    let hashed_id = collection_id.hashed();
    let active_worker = usize::cast_from(hashed_id) % scope.peers() == scope.index();

    let mut descriptions_input = append_op.new_input_connection(
        batch_descriptions,
        Exchange::new(move |_| hashed_id),
        vec![],
    );
    let mut batches_input =
        append_op.new_input_connection(batches, Exchange::new(move |_| hashed_id), vec![]);

    let current_upper = Rc::clone(&storage_state.source_uppers[&collection_id]);
    if !active_worker {
        // This worker is not writing, so make sure it's "taken out" of the
        // calculation by advancing to the empty frontier.
        current_upper.borrow_mut().clear();
    }

    let source_statistics = storage_state
        .source_statistics
        .get(&collection_id)
        .expect("statistics initialized")
        .clone();

    // This operator accepts the batch descriptions and tokens that represent
    // written batches. Written batches get appended to persist when we learn
    // from our input frontiers that we have seen all batches for a given batch
    // description.

    let shutdown_button = append_op.build(move |_| async move {
        // This may SEEM unnecessary, but metrics contains extra
        // `DeleteOnDrop`-wrapped fields that will NOT be moved into this
        // closure otherwise, dropping and destroying
        // those metrics. This is because rust now only moves the
        // explicitly-referenced fields into closures.
        let metrics = metrics;

        // Contains descriptions of batches for which we know that we can
        // write data. We got these from the "centralized" operator that
        // determines batch descriptions for all writers.
        //
        // `Antichain` does not implement `Ord`, so we cannot use a `BTreeSet`. We need to search
        // through the set, so we cannot use the `mz_ore` wrapper either.
        #[allow(clippy::disallowed_types)]
        let mut in_flight_descriptions =
            std::collections::HashSet::<(Antichain<Timestamp>, Antichain<Timestamp>)>::new();

        // In flight batches that haven't been `compare_and_append`'d yet, plus metrics about
        // the batch.
        let mut in_flight_batches = HashMap::<
            (Antichain<Timestamp>, Antichain<Timestamp>),
            Vec<(Batch<_, _, _, _>, BatchMetrics)>,
        >::new();

        if !active_worker {
            // The non-active workers report that they are done snapshotting.
            source_statistics.initialize_snapshot_committed(&Antichain::<Timestamp>::new());
            return;
        }

        // TODO(aljoscha): We need to figure out what to do with error results from these calls.
        let persist_client = persist_clients
            .open(persist_location)
            .await
            .expect("could not open persist client");

        let mut write = persist_client
            .open_writer::<SourceData, (), Timestamp, Diff>(
                shard_id,
                &format!("persist_sink::append_batches {}", collection_id),
                Arc::new(target_relation_desc),
                Arc::new(UnitSchema),
            )
            .await
            .expect("could not open persist shard");

        // Initialize this sink's `upper` to the `upper` of the persist shard we are writing
        // to. Data from the source not beyond this time will be dropped, as it has already
        // been persisted.
        // In the future, sources will avoid passing through data not beyond this upper
        // VERY IMPORTANT: Only the active write worker must change the
        // shared upper. All other workers have already cleared this
        // upper above.
        current_upper.borrow_mut().clone_from(write.upper());
        source_statistics.initialize_snapshot_committed(write.upper());

        // The current input frontiers.
        let mut batch_description_frontier = Antichain::from_elem(TimelyTimestamp::minimum());
        let mut batches_frontier = Antichain::from_elem(TimelyTimestamp::minimum());

        // Pause the source to prevent committing the snapshot,
        // if the failpoint is configured
        let mut pg_snapshot_pause = false;
        (|| {
            fail::fail_point!("pg_snapshot_pause", |val| {
                pg_snapshot_pause = val.map_or(false, |index| {
                    let index: usize = index.parse().unwrap();
                    index == output_index
                });
            });
        })();

        loop {
            tokio::select! {
                Some(event) = descriptions_input.next_mut() => {
                    match event {
                        Event::Data(_cap, data) => {
                            // Ingest new batch descriptions.
                            for batch_description in data.drain(..) {
                                if collection_id.is_user() {
                                    trace!(
                                        "persist_sink {collection_id}/{shard_id}: \
                                            append_batches: sink {}, \
                                            new description: {:?}, \
                                            batch_description_frontier: {:?}",
                                        collection_id,
                                        batch_description,
                                        batch_description_frontier
                                    );
                                }

                                // This line has to be broken up, or
                                // rustfmt fails in the whole function :(
                                let is_new = in_flight_descriptions.insert(
                                    batch_description.clone()
                                );

                                assert!(
                                    is_new,
                                    "append_batches: sink {} got more than one batch \
                                        for a given description in-flight: {:?}",
                                    collection_id, in_flight_batches
                                );
                            }

                            continue;
                        }
                        Event::Progress(frontier) => {
                            batch_description_frontier = frontier;
                        }
                    }
                }
                Some(event) = batches_input.next_mut() => {
                    match event {
                        Event::Data(_cap, data) => {
                            // Ingest new written batches
                            for batch_and_metadata in data.drain(..) {
                                let batch = write.batch_from_hollow_batch(
                                    batch_and_metadata.batch
                                );
                                let batch_description = (
                                    batch_and_metadata.lower,
                                    batch_and_metadata.upper
                                );

                                let batches = in_flight_batches
                                    .entry(batch_description)
                                    .or_insert_with(Vec::new);

                                batches.push((batch, batch_and_metadata.metrics));
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
                "persist_sink {collection_id}/{shard_id}: \
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
                    "persist_sink {collection_id}/{shard_id}: \
                        done batch: {:?}, {:?}",
                    done_batch_metadata,
                    batches
                );

                let (batch_lower, batch_upper) = done_batch_metadata;

                let mut batch_metrics = BatchMetrics::default();
                let mut to_append = batches
                    .iter_mut()
                    .map(|(batch, metrics)| {
                        batch_metrics += metrics;
                        batch
                    })
                    .collect::<Vec<_>>();

                // We evaluate this above to avoid checking an environment variable
                // in a hot loop. Note that we only pause before we emit
                // non-empty batches, because we do want to bump the upper
                // with empty ones before we start ingesting the snapshot.
                //
                // This is a fairly complex failure case we need to check
                // see `test/cluster/pg-snapshot-partial-failure` for more
                // information.
                if pg_snapshot_pause && !to_append.is_empty() && !batch_metrics.is_empty() {
                    futures::future::pending().await
                }

                let result = write
                    .compare_and_append_batch(
                        &mut to_append[..],
                        batch_lower.clone(),
                        batch_upper.clone(),
                    )
                    .await
                    .expect("Invalid usage");

                source_statistics
                    .inc_updates_committed_by(batch_metrics.inserts + batch_metrics.retractions);
                source_statistics.update_snapshot_committed(&batch_upper);

                metrics.processed_batches.inc();
                metrics.row_inserts.inc_by(batch_metrics.inserts);
                metrics.row_retractions.inc_by(batch_metrics.retractions);
                metrics.error_inserts.inc_by(batch_metrics.error_inserts);
                metrics
                    .error_retractions
                    .inc_by(batch_metrics.error_retractions);
                metrics
                    .progress
                    .set(mz_persist_client::metrics::encode_ts_metric(&batch_upper));

                if collection_id.is_user() {
                    trace!(
                        "persist_sink {collection_id}/{shard_id}: \
                            append result for batch ({:?} -> {:?}): {:?}",
                        batch_lower,
                        batch_upper,
                        result
                    );
                }

                match result {
                    Ok(()) => {
                        current_upper.borrow_mut().clone_from(&batch_upper);
                    }
                    Err(mismatch) => {
                        // _Best effort_ Clean up in case we didn't manage to append the
                        // batches to persist.
                        for (batch, _) in batches {
                            batch.delete().await;
                        }
                        panic!(
                            "persist_sink({}): invalid upper! \
                                Tried to append batch ({:?} -> {:?}) but upper \
                                is {:?}. This is not a problem, it just means \
                                someone else was faster than us. We will try \
                                again with a new batch description.",
                            collection_id, batch_lower, batch_upper, mismatch.current,
                        );
                    }
                }
            }
        }
    });

    Rc::new(shutdown_button.press_on_drop())
}
