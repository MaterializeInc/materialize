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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::AddAssign;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection, Hashable};
use futures::{StreamExt, future};
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::collections::HashMap;
use mz_persist_client::Diagnostics;
use mz_persist_client::batch::{Batch, BatchBuilder, ProtoBatch};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::error::UpperMismatch;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::{Codec, Codec64};
use mz_repr::{Diff, GlobalId, Row};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_storage_types::{StorageDiff, dyncfgs};
use mz_timely_util::builder_async::{
    Event, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use serde::{Deserialize, Serialize};
use timely::PartialOrder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{Broadcast, Capability, CapabilitySet, Inspect};
use timely::dataflow::{Scope, Stream};
use timely::progress::{Antichain, Timestamp};
use tokio::sync::Semaphore;
use tracing::trace;

use crate::metrics::source::SourcePersistSinkMetrics;
use crate::storage_state::StorageState;

/// Metrics about batches.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct BatchMetrics {
    inserts: u64,
    retractions: u64,
    error_inserts: u64,
    error_retractions: u64,
}

impl AddAssign<&BatchMetrics> for BatchMetrics {
    fn add_assign(&mut self, rhs: &BatchMetrics) {
        let BatchMetrics {
            inserts: self_inserts,
            retractions: self_retractions,
            error_inserts: self_error_inserts,
            error_retractions: self_error_retractions,
        } = self;
        let BatchMetrics {
            inserts: rhs_inserts,
            retractions: rhs_retractions,
            error_inserts: rhs_error_inserts,
            error_retractions: rhs_error_retractions,
        } = rhs;
        *self_inserts += rhs_inserts;
        *self_retractions += rhs_retractions;
        *self_error_inserts += rhs_error_inserts;
        *self_error_retractions += rhs_error_retractions;
    }
}

/// Manages batches and metrics.
struct BatchBuilderAndMetadata<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
{
    builder: BatchBuilder<K, V, T, D>,
    data_ts: T,
    metrics: BatchMetrics,
}

impl<K, V, T, D> BatchBuilderAndMetadata<K, V, T, D>
where
    K: Codec + Debug,
    V: Codec + Debug,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    /// Creates a new batch.
    ///
    /// NOTE(benesch): temporary restriction: all updates added to the batch
    /// must be at the specified timestamp `data_ts`.
    fn new(builder: BatchBuilder<K, V, T, D>, data_ts: T) -> Self {
        BatchBuilderAndMetadata {
            builder,
            data_ts,
            metrics: Default::default(),
        }
    }

    /// Adds an update to the batch.
    ///
    /// NOTE(benesch): temporary restriction: all updates added to the batch
    /// must be at the timestamp specified during creation.
    async fn add(&mut self, k: &K, v: &V, t: &T, d: &D) {
        assert_eq!(
            self.data_ts, *t,
            "BatchBuilderAndMetadata::add called with a timestamp {t:?} that does not match creation timestamp {:?}",
            self.data_ts
        );

        self.builder.add(k, v, t, d).await.expect("invalid usage");
    }

    async fn finish(self, lower: Antichain<T>, upper: Antichain<T>) -> HollowBatchAndMetadata<T> {
        let batch = self
            .builder
            .finish(upper.clone())
            .await
            .expect("invalid usage");
        HollowBatchAndMetadata {
            lower,
            upper,
            data_ts: self.data_ts,
            batch: batch.into_transmittable_batch(),
            metrics: self.metrics,
        }
    }
}

/// A batch or data + metrics moved from `write_batches` to `append_batches`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(bound(
    serialize = "T: Timestamp + Codec64",
    deserialize = "T: Timestamp + Codec64"
))]
struct HollowBatchAndMetadata<T> {
    lower: Antichain<T>,
    upper: Antichain<T>,
    data_ts: T,
    batch: ProtoBatch,
    metrics: BatchMetrics,
}

/// Holds finished batches for `append_batches`.
#[derive(Debug, Default)]
struct BatchSet {
    finished: Vec<FinishedBatch>,
    batch_metrics: BatchMetrics,
}

#[derive(Debug)]
struct FinishedBatch {
    batch: Batch<SourceData, (), mz_repr::Timestamp, StorageDiff>,
    data_ts: mz_repr::Timestamp,
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
///
/// This operator assumes that the `desired_collection` comes pre-sharded.
///
/// Note that `mint_batch_descriptions` inspects the frontier of
/// `desired_collection`, and passes the data through to `write_batches`.
/// This is done to avoid a clone of the underlying data so that both
/// operators can have the collection as input.
pub(crate) fn render<G>(
    scope: &G,
    collection_id: GlobalId,
    target: CollectionMetadata,
    desired_collection: Collection<G, Result<Row, DataflowError>, Diff>,
    storage_state: &StorageState,
    metrics: SourcePersistSinkMetrics,
    busy_signal: Arc<Semaphore>,
) -> (
    Stream<G, ()>,
    Stream<G, Rc<anyhow::Error>>,
    Vec<PressOnDropButton>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    let persist_clients = Arc::clone(&storage_state.persist_clients);

    let operator_name = format!("persist_sink({})", collection_id);

    let (batch_descriptions, passthrough_desired_stream, mint_token) = mint_batch_descriptions(
        scope,
        collection_id,
        &operator_name,
        &target,
        &desired_collection,
        Arc::clone(&persist_clients),
    );

    let (written_batches, write_token) = write_batches(
        scope,
        collection_id.clone(),
        &operator_name,
        &target,
        &batch_descriptions,
        &passthrough_desired_stream.as_collection(),
        Arc::clone(&persist_clients),
        storage_state,
        Arc::clone(&busy_signal),
    );

    let (upper_stream, append_errors, append_token) = append_batches(
        scope,
        collection_id.clone(),
        operator_name,
        &target,
        &batch_descriptions,
        &written_batches,
        persist_clients,
        storage_state,
        metrics,
        Arc::clone(&busy_signal),
    );

    (
        upper_stream,
        append_errors,
        vec![mint_token, write_token, append_token],
    )
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
    scope: &G,
    collection_id: GlobalId,
    operator_name: &str,
    target: &CollectionMetadata,
    desired_collection: &Collection<G, Result<Row, DataflowError>, Diff>,
    persist_clients: Arc<PersistClientCache>,
) -> (
    Stream<G, (Antichain<mz_repr::Timestamp>, Antichain<mz_repr::Timestamp>)>,
    Stream<G, (Result<Row, DataflowError>, mz_repr::Timestamp, Diff)>,
    PressOnDropButton,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
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

    let (output, output_stream) = mint_op.new_output();
    let (data_output, data_output_stream) = mint_op.new_output::<CapacityContainerBuilder<_>>();

    // The description and the data-passthrough outputs are both driven by this input, so
    // they use a standard input connection.
    let mut desired_input =
        mint_op.new_input_for_many(&desired_collection.inner, Pipeline, [&output, &data_output]);

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
                        data_output.give_container(&data_output_cap, &mut data);
                    }
                    Event::Progress(_) => {}
                }
            }
            return;
        }
        // The data-passthrough output should will use the data capabilities, so we drop
        // its capability here.
        let [desc_cap, _]: [_; 2] = capabilities.try_into().expect("one capability per output");
        let mut cap_set = CapabilitySet::from_elem(desc_cap);

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

            let mut write = persist_client
                .open_writer::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                    shard_id,
                    Arc::new(target_relation_desc),
                    Arc::new(UnitSchema),
                    Diagnostics {
                        shard_name: collection_id.to_string(),
                        handle_purpose: format!(
                            "storage::persist_sink::mint_batch_descriptions {}",
                            collection_id
                        ),
                    },
                )
                .await
                .expect("could not open persist shard");

            // TODO: this sink currently cannot tolerate a stale upper... which is bad because the
            // upper can become stale as soon as it is read. (For example, if another concurrent
            // instance of the sink has updated it.) Fetching a recent upper helps to mitigate this,
            // but ideally we would just skip ahead if we discover that our upper is stale.
            let upper = write.fetch_recent_upper().await.clone();
            // explicitly expire the once-used write handle.
            write.expire().await;
            upper
        };

        // The current input frontiers.
        let mut desired_frontier;

        loop {
            if let Some(event) = desired_input.next().await {
                match event {
                    Event::Data([_output_cap, data_output_cap], mut data) => {
                        // Just passthrough the data.
                        data_output.give_container(&data_output_cap, &mut data);
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

                let lower = batch_description.0.as_option().copied().unwrap();

                let cap = cap_set
                    .try_delayed(&lower)
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

                output.give(&cap, batch_description);

                // We downgrade our capability to the batch
                // description upper, as there will never be
                // any overlapping descriptions.
                trace!(
                    "persist_sink {collection_id}/{shard_id}: \
                        downgrading to {:?}",
                    desired_frontier
                );
                cap_set.downgrade(desired_frontier.iter());

                // After successfully emitting a new description, we can update the upper for the
                // operator.
                current_upper.clone_from(&desired_frontier);
            }
        }
    });

    (
        output_stream,
        data_output_stream,
        shutdown_button.press_on_drop(),
    )
}

/// Writes `desired_collection` to persist, but only for updates
/// that fall into batch a description that we get via `batch_descriptions`.
/// This forwards a `HollowBatch` (with additional metadata)
/// for any batch of updates that was written.
///
/// This operator assumes that the `desired_collection` comes pre-sharded.
///
/// This also and updates various metrics.
fn write_batches<G>(
    scope: &G,
    collection_id: GlobalId,
    operator_name: &str,
    target: &CollectionMetadata,
    batch_descriptions: &Stream<G, (Antichain<mz_repr::Timestamp>, Antichain<mz_repr::Timestamp>)>,
    desired_collection: &Collection<G, Result<Row, DataflowError>, Diff>,
    persist_clients: Arc<PersistClientCache>,
    storage_state: &StorageState,
    busy_signal: Arc<Semaphore>,
) -> (
    Stream<G, HollowBatchAndMetadata<mz_repr::Timestamp>>,
    PressOnDropButton,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    let worker_index = scope.index();

    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;
    let target_relation_desc = target.relation_desc.clone();

    let source_statistics = storage_state
        .aggregated_statistics
        .get_source(&collection_id)
        .expect("statistics initialized")
        .clone();

    let mut write_op =
        AsyncOperatorBuilder::new(format!("{} write_batches", operator_name), scope.clone());

    let (output, output_stream) = write_op.new_output::<CapacityContainerBuilder<_>>();

    let mut descriptions_input =
        write_op.new_input_for(&batch_descriptions.broadcast(), Pipeline, &output);
    let mut desired_input = write_op.new_disconnected_input(&desired_collection.inner, Pipeline);

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
            (Antichain<mz_repr::Timestamp>, Antichain<mz_repr::Timestamp>),
            Capability<mz_repr::Timestamp>,
        >::new();

        // TODO(aljoscha): We need to figure out what to do with error results from these calls.
        let persist_client = persist_clients
            .open(persist_location)
            .await
            .expect("could not open persist client");

        let write = persist_client
            .open_writer::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                shard_id,
                Arc::new(target_relation_desc),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: collection_id.to_string(),
                    handle_purpose: format!(
                        "storage::persist_sink::write_batches {}",
                        collection_id
                    ),
                },
            )
            .await
            .expect("could not open persist shard");

        // The current input frontiers.
        let mut batch_descriptions_frontier = Antichain::from_elem(Timestamp::minimum());
        let mut desired_frontier = Antichain::from_elem(Timestamp::minimum());

        // The frontiers of the inputs we have processed, used to avoid redoing work
        let mut processed_desired_frontier = Antichain::from_elem(Timestamp::minimum());
        let mut processed_descriptions_frontier = Antichain::from_elem(Timestamp::minimum());

        // A "safe" choice for the lower of new batches we are creating.
        let mut operator_batch_lower = Antichain::from_elem(Timestamp::minimum());

        while !(batch_descriptions_frontier.is_empty() && desired_frontier.is_empty()) {
            // Wait for either inputs to become ready
            tokio::select! {
                _ = descriptions_input.ready() => {},
                _ = desired_input.ready() => {},
            }

            // Collect ready work from both inputs
            while let Some(event) = descriptions_input.next_sync() {
                match event {
                    Event::Data(cap, data) => {
                        // Ingest new batch descriptions.
                        for description in data {
                            if collection_id.is_user() {
                                trace!(
                                    "persist_sink {collection_id}/{shard_id}: \
                                        write_batches: \
                                        new_description: {:?}, \
                                        desired_frontier: {:?}, \
                                        batch_descriptions_frontier: {:?}",
                                    description, desired_frontier, batch_descriptions_frontier,
                                );
                            }
                            match in_flight_batches.entry(description) {
                                std::collections::hash_map::Entry::Vacant(v) => {
                                    // This _should_ be `.retain`, but rust
                                    // currently thinks we can't use `cap`
                                    // as an owned value when using the
                                    // match guard `Some(event)`
                                    v.insert(cap.delayed(cap.time()));
                                }
                                std::collections::hash_map::Entry::Occupied(o) => {
                                    let (description, _) = o.remove_entry();
                                    panic!(
                                        "write_batches: sink {} got more than one \
                                            batch for description {:?}, in-flight: {:?}",
                                        collection_id, description, in_flight_batches
                                    );
                                }
                            }
                        }
                    }
                    Event::Progress(frontier) => {
                        batch_descriptions_frontier = frontier;
                    }
                }
            }

            let ready_events = std::iter::from_fn(|| desired_input.next_sync()).collect_vec();

            // We know start the async work for the input we received. Until we finish the dataflow
            // should be marked as busy.
            let permit = busy_signal.acquire().await;

            for event in ready_events {
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

                        for (row, ts, diff) in data {
                            if write.upper().less_equal(&ts) {
                                let builder = stashed_batches.entry(ts).or_insert_with(|| {
                                    BatchBuilderAndMetadata::new(
                                        write.builder(operator_batch_lower.clone()),
                                        ts,
                                    )
                                });

                                let is_value = row.is_ok();

                                builder
                                    .add(&SourceData(row), &(), &ts, &diff.into_inner())
                                    .await;

                                source_statistics.inc_updates_staged_by(1);

                                // Note that we assume `diff` is either +1 or -1 here, being anything
                                // else is a logic bug we can't handle at the metric layer. We also
                                // assume this addition doesn't overflow.
                                match (is_value, diff.is_positive()) {
                                    (true, true) => builder.metrics.inserts += diff.unsigned_abs(),
                                    (true, false) => {
                                        builder.metrics.retractions += diff.unsigned_abs()
                                    }
                                    (false, true) => {
                                        builder.metrics.error_inserts += diff.unsigned_abs()
                                    }
                                    (false, false) => {
                                        builder.metrics.error_retractions += diff.unsigned_abs()
                                    }
                                }
                            }
                        }
                    }
                    Event::Progress(frontier) => {
                        desired_frontier = frontier;
                    }
                }
            }
            // We may have the opportunity to commit updates, if either frontier
            // has moved
            if PartialOrder::less_equal(&processed_desired_frontier, &desired_frontier)
                || PartialOrder::less_equal(
                    &processed_descriptions_frontier,
                    &batch_descriptions_frontier,
                )
            {
                trace!(
                    "persist_sink {collection_id}/{shard_id}: \
                        CAN emit: \
                        processed_desired_frontier: {:?}, \
                        processed_descriptions_frontier: {:?}, \
                        desired_frontier: {:?}, \
                        batch_descriptions_frontier: {:?}",
                    processed_desired_frontier,
                    processed_descriptions_frontier,
                    desired_frontier,
                    batch_descriptions_frontier,
                );

                trace!(
                    "persist_sink {collection_id}/{shard_id}: \
                        in-flight batches: {:?}, \
                        batch_descriptions_frontier: {:?}, \
                        desired_frontier: {:?}",
                    in_flight_batches, batch_descriptions_frontier, desired_frontier,
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

                for batch_description in ready_batches {
                    let cap = in_flight_batches.remove(&batch_description).unwrap();

                    if collection_id.is_user() {
                        trace!(
                            "persist_sink {collection_id}/{shard_id}: \
                                emitting done batch: {:?}, cap: {:?}",
                            batch_description, cap
                        );
                    }

                    let (batch_lower, batch_upper) = batch_description;

                    let finalized_timestamps: Vec<_> = stashed_batches
                        .keys()
                        .filter(|time| {
                            batch_lower.less_equal(time) && !batch_upper.less_equal(time)
                        })
                        .copied()
                        .collect();

                    let mut batch_tokens = vec![];
                    for ts in finalized_timestamps {
                        let batch_builder = stashed_batches.remove(&ts).unwrap();

                        if collection_id.is_user() {
                            trace!(
                                "persist_sink {collection_id}/{shard_id}: \
                                    wrote batch from worker {}: ({:?}, {:?}),
                                    containing {:?}",
                                worker_index, batch_lower, batch_upper, batch_builder.metrics
                            );
                        }

                        let batch = batch_builder
                            .finish(batch_lower.clone(), batch_upper.clone())
                            .await;

                        // The next "safe" lower for batches is the meet (max) of all the emitted
                        // batches. These uppers all are not beyond the `desired_frontier`, which
                        // means all updates received by this operator will be beyond this lower.
                        // Additionally, the `mint_batch_descriptions` operator ensures that
                        // later-received batch descriptions will start beyond these uppers as
                        // well.
                        //
                        // It is impossible to emit a batch description that is
                        // beyond a not-yet emitted description in `in_flight_batches`, as
                        // a that description would also have been chosen as ready above.
                        operator_batch_lower = operator_batch_lower.join(&batch_upper);
                        batch_tokens.push(batch);
                    }

                    output.give_container(&cap, &mut batch_tokens);

                    processed_desired_frontier.clone_from(&desired_frontier);
                    processed_descriptions_frontier.clone_from(&batch_descriptions_frontier);
                }
            } else {
                trace!(
                    "persist_sink {collection_id}/{shard_id}: \
                        cannot emit: processed_desired_frontier: {:?}, \
                        processed_descriptions_frontier: {:?}, \
                        desired_frontier: {:?}",
                    processed_desired_frontier, processed_descriptions_frontier, desired_frontier
                );
            }
            drop(permit);
        }
    });

    if collection_id.is_user() {
        output_stream.inspect(|d| trace!("batch: {:?}", d));
    }

    (output_stream, shutdown_button.press_on_drop())
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
    scope: &G,
    collection_id: GlobalId,
    operator_name: String,
    target: &CollectionMetadata,
    batch_descriptions: &Stream<G, (Antichain<mz_repr::Timestamp>, Antichain<mz_repr::Timestamp>)>,
    batches: &Stream<G, HollowBatchAndMetadata<mz_repr::Timestamp>>,
    persist_clients: Arc<PersistClientCache>,
    storage_state: &StorageState,
    metrics: SourcePersistSinkMetrics,
    busy_signal: Arc<Semaphore>,
) -> (
    Stream<G, ()>,
    Stream<G, Rc<anyhow::Error>>,
    PressOnDropButton,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;
    let target_relation_desc = target.relation_desc.clone();

    // We can only be lenient with concurrent modifications when we know that
    // this source pipeline is using the feedback upsert operator, which works
    // correctly when multiple instances of an ingestion pipeline produce
    // different updates, because of concurrency/non-determinism.
    let use_continual_feedback_upsert = dyncfgs::STORAGE_USE_CONTINUAL_FEEDBACK_UPSERT
        .get(storage_state.storage_configuration.config_set());
    let bail_on_concurrent_modification = !use_continual_feedback_upsert;

    let mut read_only_rx = storage_state.read_only_rx.clone();

    let operator_name = format!("{} append_batches", operator_name);
    let mut append_op = AsyncOperatorBuilder::new(operator_name, scope.clone());

    let hashed_id = collection_id.hashed();
    let active_worker = usize::cast_from(hashed_id) % scope.peers() == scope.index();
    let worker_id = scope.index();

    // Both of these inputs are disconnected from the output capabilities of this operator, as
    // any output of this operator is entirely driven by the `compare_and_append`s. Currently
    // this operator has no outputs, but they may be added in the future, when merging with
    // the compute `persist_sink`.
    let mut descriptions_input =
        append_op.new_disconnected_input(batch_descriptions, Exchange::new(move |_| hashed_id));
    let mut batches_input =
        append_op.new_disconnected_input(batches, Exchange::new(move |_| hashed_id));

    let current_upper = Rc::clone(&storage_state.source_uppers[&collection_id]);
    if !active_worker {
        // This worker is not writing, so make sure it's "taken out" of the
        // calculation by advancing to the empty frontier.
        current_upper.borrow_mut().clear();
    }

    let source_statistics = storage_state
        .aggregated_statistics
        .get_source(&collection_id)
        .expect("statistics initialized")
        .clone();

    // An output whose frontier tracks the last successful compare and append of this operator
    let (_upper_output, upper_stream) = append_op.new_output::<CapacityContainerBuilder<_>>();

    // This operator accepts the batch descriptions and tokens that represent
    // written batches. Written batches get appended to persist when we learn
    // from our input frontiers that we have seen all batches for a given batch
    // description.

    let (shutdown_button, errors) = append_op.build_fallible(move |caps| Box::pin(async move {
        let [upper_cap_set]: &mut [_; 1] = caps.try_into().unwrap();

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
        let mut in_flight_descriptions = std::collections::HashSet::<(
            Antichain<mz_repr::Timestamp>,
            Antichain<mz_repr::Timestamp>,
        )>::new();

        // In flight batches that haven't been `compare_and_append`'d yet, plus metrics about
        // the batch.
        let mut in_flight_batches = HashMap::<
            (Antichain<mz_repr::Timestamp>, Antichain<mz_repr::Timestamp>),
            BatchSet,
        >::new();

        source_statistics.initialize_rehydration_latency_ms();
        if !active_worker {
            // The non-active workers report that they are done snapshotting and hydrating.
            let empty_frontier = Antichain::new();
            source_statistics.initialize_snapshot_committed(&empty_frontier);
            source_statistics.update_rehydration_latency_ms(&empty_frontier);
            return Ok(());
        }

        let persist_client = persist_clients
            .open(persist_location)
            .await?;

        let mut write = persist_client
            .open_writer::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                shard_id,
                Arc::new(target_relation_desc),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name:collection_id.to_string(),
                    handle_purpose: format!("persist_sink::append_batches {}", collection_id)
                },
            )
            .await?;

        // Initialize this sink's `upper` to the `upper` of the persist shard we are writing
        // to. Data from the source not beyond this time will be dropped, as it has already
        // been persisted.
        // In the future, sources will avoid passing through data not beyond this upper
        // VERY IMPORTANT: Only the active write worker must change the
        // shared upper. All other workers have already cleared this
        // upper above.
        current_upper.borrow_mut().clone_from(write.upper());
        upper_cap_set.downgrade(current_upper.borrow().iter());
        source_statistics.initialize_snapshot_committed(write.upper());

        // The current input frontiers.
        let mut batch_description_frontier = Antichain::from_elem(Timestamp::minimum());
        let mut batches_frontier = Antichain::from_elem(Timestamp::minimum());

        loop {
            // Drain both input channels as of resumption and advance frontier.
            tokio::select! {
                Some(event) = descriptions_input.next() => {
                    match event {
                        Event::Data(_cap, data) => {
                            // Ingest new batch descriptions.
                            for batch_description in data {
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
                Some(event) = batches_input.next() => {
                    match event {
                        Event::Data(_cap, data) => {
                            for batch in data {
                                let batch_description = (batch.lower.clone(), batch.upper.clone());

                                let batches = in_flight_batches
                                    .entry(batch_description)
                                    .or_default();

                                batches.finished.push(FinishedBatch {
                                    batch: write.batch_from_transmittable_batch(batch.batch),
                                    data_ts: batch.data_ts,
                                });
                                batches.batch_metrics += &batch.metrics;
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
                    return Ok(());
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
            // `upper` line up.
            done_batches.sort_by(|a, b| {
                if PartialOrder::less_than(a, b) {
                    Ordering::Less
                } else if PartialOrder::less_than(b, a) {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            });

            let mut done_batches_iter = done_batches.iter();

            let Some(first_batch_description) = done_batches_iter.next() else {
                continue;
            };

            let batch_upper = first_batch_description.1.clone();

            let batch_lower = if let Some(last_batch_description) = done_batches_iter.last() {
                last_batch_description.0.clone()
            } else {
                first_batch_description.0.clone()
            };

            let mut batches: Vec<FinishedBatch> = vec![];
            let mut batch_metrics: Vec<BatchMetrics> = vec![];

            for batch_metadata in done_batches.drain(..) {
                let batch = in_flight_batches.remove(&batch_metadata).unwrap_or_default();
                batches.append(&mut std::mem::replace(&mut vec![], batch.finished));
                batch_metrics.push(batch.batch_metrics);
            }

            let mut to_append = batches.iter_mut().map(|b| &mut b.batch).collect::<Vec<_>>();

            let result = {
                let maybe_err = if *read_only_rx.borrow() {

                    // We have to wait for either us coming out of read-only
                    // mode or someone else applying a write that covers our
                    // batch.
                    //
                    // If we didn't wait for the latter here, and just go
                    // around the loop again, we might miss a moment where
                    // _we_ have to write down a batch. For example when our
                    // input frontier advances to a state where we can
                    // write, and the read-write instance sees the same
                    // update but then crashes before it can append a batch.

                    let maybe_err = loop {
                        if collection_id.is_user() {
                            tracing::debug!(
                                %worker_id,
                                %collection_id,
                                %shard_id,
                                ?batch_lower,
                                ?batch_upper,
                                ?current_upper,
                                "persist_sink is in read-only mode, waiting until we come out of it or the shard upper advances"
                            );
                        }

                        // We don't try to be smart here, and for example
                        // use `wait_for_upper_past()`. We'd have to use a
                        // select!, which would require cancel safety of
                        // `wait_for_upper_past()`, which it doesn't
                        // advertise.
                        let _ = tokio::time::timeout(Duration::from_secs(1), read_only_rx.changed()).await;

                        if !*read_only_rx.borrow() {
                            if collection_id.is_user() {
                                tracing::debug!(
                                    %worker_id,
                                    %collection_id,
                                    %shard_id,
                                    ?batch_lower,
                                    ?batch_upper,
                                    ?current_upper,
                                    "persist_sink has come out of read-only mode"
                                );
                            }

                            // It's okay to write now.
                            break Ok(());
                        }

                        let current_upper = write.fetch_recent_upper().await;

                        if PartialOrder::less_than(&batch_upper, current_upper) {
                            // We synthesize an `UpperMismatch` so that we can go
                            // through the same logic below for trimming down our
                            // batches.
                            //
                            // Notably, we are not trying to be smart, and teach the
                            // write operator about read-only mode. Writing down
                            // those batches does not append anything to the persist
                            // shard, and it would be a hassle to figure out in the
                            // write workers how to trim down batches in read-only
                            // mode, when the shard upper advances.
                            //
                            // Right here, in the logic below, we have all we need
                            // for figuring out how to trim our batches.

                            if collection_id.is_user() {
                                tracing::debug!(
                                    %worker_id,
                                    %collection_id,
                                    %shard_id,
                                    ?batch_lower,
                                    ?batch_upper,
                                    ?current_upper,
                                    "persist_sink not appending in read-only mode"
                                );
                            }

                            break Err(UpperMismatch {
                                current: current_upper.clone(),
                                expected: batch_lower.clone()}
                            );
                        }
                    };

                    maybe_err
                } else {
                    // It's okay to proceed with the write.
                    Ok(())
                };

                match maybe_err {
                    Ok(()) => {
                        let _permit = busy_signal.acquire().await;

                        write.compare_and_append_batch(
                            &mut to_append[..],
                            batch_lower.clone(),
                            batch_upper.clone(),
                        )
                        .await
                        .expect("Invalid usage")
                    },
                    Err(e) => {
                        // We forward the synthesize error message, so that
                        // we go though the batch cleanup logic below.
                        Err(e)
                    }
                }
            };


            // These metrics are independent of whether it was _us_ or
            // _someone_ that managed to commit a batch that advanced the
            // upper.
            source_statistics.update_snapshot_committed(&batch_upper);
            source_statistics.update_rehydration_latency_ms(&batch_upper);
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
                    // Only update these metrics when we know that _we_ were
                    // successful.
                    /* let batch_metrics = batch_metrics.drain(..).reduce();
                    source_statistics
                        .inc_updates_committed_by(batch_metrics.inserts + batch_metrics.retractions);
                    metrics.processed_batches.inc_by(batch_metrics.len());
                    metrics.row_inserts.inc_by(batch_metrics.inserts);
                    metrics.row_retractions.inc_by(batch_metrics.retractions);
                    metrics.error_inserts.inc_by(batch_metrics.error_inserts);
                    metrics
                        .error_retractions
                        .inc_by(batch_metrics.error_retractions); */

                    current_upper.borrow_mut().clone_from(&batch_upper);
                    upper_cap_set.downgrade(current_upper.borrow().iter());
                }
                Err(mismatch) => {
                    // We tried to do a non-contiguous append, that won't work.
                    if PartialOrder::less_than(&mismatch.current, &batch_lower) {
                        // Best-effort attempt to delete unneeded batches.
                        future::join_all(batches.into_iter().map(|b| b.batch.delete())).await;

                        // We always bail when this happens, regardless of
                        // `bail_on_concurrent_modification`.
                        tracing::warn!(
                            "persist_sink({}): invalid upper! \
                                Tried to append batch ({:?} -> {:?}) but upper \
                                is {:?}. This is surpising and likely indicates \
                                a bug in the persist sink, but we'll restart the \
                                dataflow and try again.",
                            collection_id, batch_lower, batch_upper, mismatch.current,
                        );
                        anyhow::bail!("collection concurrently modified. Ingestion dataflow will be restarted");
                    } else if PartialOrder::less_than(&mismatch.current, &batch_upper) {
                        // The shard's upper was ahead of our batch's lower
                        // but not ahead of our upper. Cut down the
                        // description by advancing its lower to the current
                        // shard upper and try again. IMPORTANT: We can only
                        // advance the lower, meaning we cut updates away,
                        // we must not "extend" the batch by changing to a
                        // lower that is not beyond the current lower. This
                        // invariant is checked by the first if branch: if
                        // `!(current_upper < lower)` then it holds that
                        // `lower <= current_upper`.

                        // First, construct a new batch description with the
                        // lower advanced to the current shard upper.
                        let new_batch_lower = mismatch.current.clone();
                        let new_done_batch_metadata = (new_batch_lower.clone(), batch_upper.clone());

                        // Re-add the new batch to the list of batches to
                        // process.
                        done_batches.push(new_done_batch_metadata.clone());

                        // Retain any batches that are still in advance of
                        // the new lower, and delete any batches that are
                        // not.
                        //
                        // Temporary measure: this bookkeeping is made
                        // possible by the fact that each batch only
                        // contains data at a single timestamp, even though
                        // it might declare a larger lower or upper. In the
                        // future, we'll want to use persist's `append` API
                        // and let persist handle the truncation internally.
                        let new_batch_set = in_flight_batches.entry(new_done_batch_metadata).or_default();
                        let mut batch_delete_futures = vec![];
                        for batch in batches {
                            if new_batch_lower.less_equal(&batch.data_ts) {
                                new_batch_set.finished.push(batch);
                            } else {
                                batch_delete_futures.push(batch.batch.delete());
                            }
                        }

                        // Best-effort attempt to delete unneeded batches.
                        future::join_all(batch_delete_futures).await;
                    } else {
                        // Best-effort attempt to delete unneeded batches.
                        future::join_all(batches.into_iter().map(|b| b.batch.delete())).await;
                    }

                    if bail_on_concurrent_modification {
                        tracing::warn!(
                            "persist_sink({}): invalid upper! \
                                Tried to append batch ({:?} -> {:?}) but upper \
                                is {:?}. This is not a problem, it just means \
                                someone else was faster than us. We will try \
                                again with a new batch description.",
                            collection_id, batch_lower, batch_upper, mismatch.current,
                        );
                        anyhow::bail!("collection concurrently modified. Ingestion dataflow will be restarted");
                    }
                }
            }
        }
    }));

    (upper_stream, errors, shutdown_button.press_on_drop())
}
