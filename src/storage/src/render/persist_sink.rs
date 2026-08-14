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

use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::ops::AddAssign;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::consolidation::consolidate;
use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Hashable, VecCollection};
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
use timely::dataflow::operators::vec::Broadcast;
use timely::dataflow::operators::{Capability, CapabilitySet, InspectCore};
use timely::dataflow::{Scope, Stream, StreamVec};
use timely::progress::{Antichain, Timestamp};
use tokio::sync::Semaphore;
use tracing::trace;

use crate::metrics::source::SourcePersistSinkMetrics;
use crate::statistics::SourceStatistics;
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
    /// Largest update timestamp staged so far, `None` while empty.
    ///
    /// `append_batches` needs this to decide, after an `UpperMismatch`, whether a batch lies
    /// entirely below a raised append lower and so holds nothing this sink still owes. Such a
    /// batch is deleted rather than appended, which keeps parts that would be truncated away in
    /// their entirety out of shard state.
    data_max_ts: Option<T>,
    metrics: BatchMetrics,
}

impl<K, V, T, D> BatchBuilderAndMetadata<K, V, T, D>
where
    K: Codec + Debug,
    V: Codec + Debug,
    T: Timestamp + Lattice + Codec64,
    D: Monoid + Codec64,
{
    /// Creates a new batch. Updates at any timestamp at or beyond the builder's lower may be
    /// added, in any order.
    fn new(builder: BatchBuilder<K, V, T, D>) -> Self {
        BatchBuilderAndMetadata {
            builder,
            data_max_ts: None,
            metrics: Default::default(),
        }
    }

    /// Adds an update to the batch.
    async fn add(&mut self, k: &K, v: &V, t: &T, d: &D) {
        self.data_max_ts = Some(match self.data_max_ts.take() {
            Some(max) => max.join(t),
            None => t.clone(),
        });

        self.builder.add(k, v, t, d).await.expect("invalid usage");
    }

    /// Finishes the batch, registering it under `lower` and `upper`.
    ///
    /// Panics if no update was ever added, since an empty batch has no largest timestamp. Callers
    /// open a builder on the first update rather than up front, so reaching this is a bug.
    async fn finish(self, lower: Antichain<T>, upper: Antichain<T>) -> HollowBatchAndMetadata<T> {
        let data_max_ts = self.data_max_ts.expect("finishing an empty builder");
        // `BatchBuilder::finish` rejects an update at or beyond `upper`, so a builder that was
        // handed updates outside the description it is being finished under fails here rather
        // than producing a batch whose parts reach past their registered bounds.
        let batch = self
            .builder
            .finish(upper.clone())
            .await
            .expect("invalid usage");
        HollowBatchAndMetadata {
            lower,
            upper,
            data_max_ts,
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
    data_max_ts: T,
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
    data_max_ts: mz_repr::Timestamp,
}

/// The batch builder the source sink writes with.
type SourceBatchBuilder = BatchBuilderAndMetadata<SourceData, (), mz_repr::Timestamp, StorageDiff>;

/// Updates staged at one timestamp, along with their accounted size.
#[derive(Debug, Default)]
struct RawStashEntry {
    updates: Vec<(Result<Row, DataflowError>, Diff)>,
    bytes: usize,
    /// `updates.len()` as of the last consolidation.
    consolidated_len: usize,
}

impl RawStashEntry {
    /// Adds an update, charging its size to the entry.
    fn push(&mut self, row: Result<Row, DataflowError>, diff: Diff) -> usize {
        let bytes = stashed_bytes(&row);
        self.updates.push((row, diff));
        self.bytes += bytes;
        bytes
    }

    /// Consolidates the entry, returning the bytes this freed.
    fn consolidate(&mut self) -> usize {
        consolidate(&mut self.updates);
        self.consolidated_len = self.updates.len();

        let bytes = self.updates.iter().map(|(row, _)| stashed_bytes(row)).sum();
        let freed = self.bytes.saturating_sub(bytes);
        self.bytes = bytes;
        freed
    }

    /// Consolidates the entry only once it has doubled since the last attempt.
    ///
    /// Used on the memory-pressure path, where the same entry can be revisited on every arrival.
    /// The doubling keeps the total work amortized linear instead of re-sorting a growing entry
    /// each time the stash is over budget.
    fn maybe_consolidate(&mut self) -> usize {
        if self.updates.len() < self.consolidated_len.max(1) * 2 {
            return 0;
        }
        self.consolidate()
    }

    /// Consolidates and returns the entry's updates, for staging into a builder.
    fn drain(mut self) -> Vec<(Result<Row, DataflowError>, Diff)> {
        // Every update is about to be visited anyway, so the sort is nearly free here, and
        // whatever cancels is a row that never reaches blob storage.
        self.consolidate();
        self.updates
    }
}

/// The size charged against the raw stash budget for one staged update.
///
/// This tracks the retained row payload rather than the true allocation, so it is an estimate
/// used only to decide when to start writing batches out.
fn stashed_bytes(row: &Result<Row, DataflowError>) -> usize {
    let payload = match row {
        Ok(row) => row.byte_len(),
        Err(_) => size_of::<DataflowError>(),
    };
    payload + size_of::<Diff>()
}

/// Adds one update to `builder`, keeping the batch metrics in step.
async fn stage_update(
    builder: &mut SourceBatchBuilder,
    row: Result<Row, DataflowError>,
    ts: mz_repr::Timestamp,
    diff: Diff,
) {
    let is_value = row.is_ok();

    builder
        .add(&SourceData(row), &(), &ts, &diff.into_inner())
        .await;

    // Note that we assume `diff` is either +1 or -1 here, being anything else is a logic bug we
    // can't handle at the metric layer. We also assume this addition doesn't overflow.
    match (is_value, diff.is_positive()) {
        (true, true) => builder.metrics.inserts += diff.unsigned_abs(),
        (true, false) => builder.metrics.retractions += diff.unsigned_abs(),
        (false, true) => builder.metrics.error_inserts += diff.unsigned_abs(),
        (false, false) => builder.metrics.error_retractions += diff.unsigned_abs(),
    }
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
pub(crate) fn render<'scope>(
    scope: Scope<'scope, mz_repr::Timestamp>,
    collection_id: GlobalId,
    target: CollectionMetadata,
    desired_collection: VecCollection<'scope, mz_repr::Timestamp, Result<Row, DataflowError>, Diff>,
    storage_state: &StorageState,
    metrics: SourcePersistSinkMetrics,
    busy_signal: Arc<Semaphore>,
) -> (
    StreamVec<'scope, mz_repr::Timestamp, ()>,
    StreamVec<'scope, mz_repr::Timestamp, Rc<anyhow::Error>>,
    Vec<PressOnDropButton>,
) {
    let persist_clients = Arc::clone(&storage_state.persist_clients);

    let operator_name = format!("persist_sink({})", collection_id);

    let (batch_descriptions, passthrough_desired_stream, mint_token) = mint_batch_descriptions(
        scope,
        collection_id,
        &operator_name,
        &target,
        desired_collection,
        Arc::clone(&persist_clients),
    );

    let source_statistics = storage_state
        .aggregated_statistics
        .get_source(&collection_id)
        .expect("statistics initialized")
        .clone();

    let (written_batches, write_token) = write_batches(
        scope,
        collection_id.clone(),
        &operator_name,
        &target,
        batch_descriptions.clone(),
        passthrough_desired_stream.as_collection(),
        Arc::clone(&persist_clients),
        source_statistics,
        dyncfgs::STORAGE_PERSIST_SINK_MAX_RAW_STASH_BYTES
            .get(storage_state.storage_configuration.config_set()),
        Arc::clone(&busy_signal),
    );

    let (upper_stream, append_errors, append_token) = append_batches(
        scope,
        collection_id.clone(),
        operator_name,
        &target,
        batch_descriptions,
        written_batches,
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
fn mint_batch_descriptions<'scope>(
    scope: Scope<'scope, mz_repr::Timestamp>,
    collection_id: GlobalId,
    operator_name: &str,
    target: &CollectionMetadata,
    desired_collection: VecCollection<'scope, mz_repr::Timestamp, Result<Row, DataflowError>, Diff>,
    persist_clients: Arc<PersistClientCache>,
) -> (
    StreamVec<
        'scope,
        mz_repr::Timestamp,
        (Antichain<mz_repr::Timestamp>, Antichain<mz_repr::Timestamp>),
    >,
    StreamVec<'scope, mz_repr::Timestamp, (Result<Row, DataflowError>, mz_repr::Timestamp, Diff)>,
    PressOnDropButton,
) {
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

    let (output, output_stream) = mint_op.new_output::<CapacityContainerBuilder<Vec<_>>>();
    let (data_output, data_output_stream) =
        mint_op.new_output::<CapacityContainerBuilder<Vec<_>>>();

    // The description and the data-passthrough outputs are both driven by this input, so
    // they use a standard input connection.
    let mut desired_input =
        mint_op.new_input_for_many(desired_collection.inner, Pipeline, [&output, &data_output]);

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
/// Emits one batch per description, spanning however many timestamps that description covers.
/// Updates wait as raw rows until their description arrives, which is what keeps the batch count
/// independent of how many timestamps a stalled frontier accumulates. Past `max_raw_stash_bytes`
/// the heaviest timestamps are written out early, one batch each.
///
/// This operator assumes that the `desired_collection` comes pre-sharded.
///
/// This also and updates various metrics.
fn write_batches<'scope>(
    scope: Scope<'scope, mz_repr::Timestamp>,
    collection_id: GlobalId,
    operator_name: &str,
    target: &CollectionMetadata,
    batch_descriptions: Stream<
        'scope,
        mz_repr::Timestamp,
        Vec<(Antichain<mz_repr::Timestamp>, Antichain<mz_repr::Timestamp>)>,
    >,
    desired_collection: VecCollection<'scope, mz_repr::Timestamp, Result<Row, DataflowError>, Diff>,
    persist_clients: Arc<PersistClientCache>,
    source_statistics: SourceStatistics,
    max_raw_stash_bytes: usize,
    busy_signal: Arc<Semaphore>,
) -> (
    StreamVec<'scope, mz_repr::Timestamp, HollowBatchAndMetadata<mz_repr::Timestamp>>,
    PressOnDropButton,
) {
    let worker_index = scope.index();

    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;
    let target_relation_desc = target.relation_desc.clone();

    let mut write_op =
        AsyncOperatorBuilder::new(format!("{} write_batches", operator_name), scope.clone());

    let (output, output_stream) = write_op.new_output::<CapacityContainerBuilder<Vec<_>>>();

    let mut descriptions_input =
        write_op.new_input_for(batch_descriptions.broadcast(), Pipeline, &output);
    let mut desired_input = write_op.new_disconnected_input(desired_collection.inner, Pipeline);

    // This operator accepts the current and desired update streams for a `persist` shard.
    // It attempts to write out updates, starting from the current's upper frontier, that
    // will cause the changes of desired to be committed to persist, _but only those also past the
    // upper_.

    let shutdown_button = write_op.build(move |_capabilities| async move {
        // Updates staged as raw rows, keyed by timestamp.
        //
        // A batch builder cannot be split, so updates may only enter one once it is known which
        // description will cover them. A description is only acted on when the desired frontier
        // has reached its upper, which means every update it covers has already arrived, so the
        // builder can be created then and take all of them at once. Holding the rows until that
        // point is what keeps the batch count proportional to descriptions rather than to
        // timestamps.
        let mut raw_stash: BTreeMap<mz_repr::Timestamp, RawStashEntry> = BTreeMap::new();
        let mut raw_stash_bytes: usize = 0;

        // Builders for timestamps evicted from `raw_stash` to stay under the byte budget.
        //
        // Each holds exactly one timestamp, which is what makes evicting safe without knowing the
        // descriptions yet: a description either covers a timestamp entirely or not at all, so a
        // single-timestamp builder can never straddle one.
        let mut spilled: BTreeMap<mz_repr::Timestamp, SourceBatchBuilder> = BTreeMap::new();

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
                                // Every description this operator has emitted was covered by the
                                // desired frontier at the time, so no update below
                                // `operator_batch_lower` can still be in flight. An update that
                                // arrives anyway belongs to a description that is already gone: it
                                // matches no later description and would sit in the stash unwritten
                                // and unnoticed. Not a `debug_assert!`, which compiles out of the
                                // optimized and release profiles and would leave the loss silent
                                // everywhere it matters.
                                assert!(
                                    operator_batch_lower.less_equal(&ts),
                                    "persist_sink {collection_id}/{shard_id}: update at {ts:?} \
                                    arrived below the emitted batch lower {operator_batch_lower:?}",
                                );

                                // Counted on arrival rather than when the update reaches a
                                // builder, so a stalled frontier does not make the sink look
                                // like it is receiving nothing.
                                source_statistics.inc_updates_staged_by(1);

                                // A timestamp already evicted from the stash keeps its own
                                // builder, so later updates at that time join it rather than
                                // starting the stash growing again.
                                if let Some(builder) = spilled.get_mut(&ts) {
                                    stage_update(builder, row, ts, diff).await;
                                } else {
                                    raw_stash_bytes +=
                                        raw_stash.entry(ts).or_default().push(row, diff);
                                }
                            }
                        }
                    }
                    Event::Progress(frontier) => {
                        desired_frontier = frontier;
                    }
                }
            }

            // Consolidate before writing anything out. Updates that cancel cost nothing to
            // drop and everything to keep: at a pinned timestamp the snapshot's rows and the
            // rewind retractions that supersede them are both staged here, and they annihilate
            // exactly. That is the heaviest entry and so the first eviction candidate.
            if raw_stash_bytes > max_raw_stash_bytes {
                for entry in raw_stash.values_mut() {
                    raw_stash_bytes -= entry.maybe_consolidate();
                }
                raw_stash.retain(|_, entry| !entry.updates.is_empty());
            }

            // Evict the heaviest timestamps until the stash fits its budget. Heaviest first so
            // that each eviction buys as much headroom as possible, which keeps the number of
            // single-timestamp batches down.
            while raw_stash_bytes > max_raw_stash_bytes {
                let Some(ts) = raw_stash
                    .iter()
                    .max_by_key(|(_, entry)| entry.bytes)
                    .map(|(ts, _)| *ts)
                else {
                    break;
                };
                let entry = raw_stash.remove(&ts).expect("just looked up");
                raw_stash_bytes -= entry.bytes;
                let updates = entry.drain();
                // The entry can consolidate to nothing, in which case there is no builder to
                // open. Its bytes are already off the budget, so the loop still makes progress.
                if updates.is_empty() {
                    continue;
                }
                let builder = spilled.entry(ts).or_insert_with(|| {
                    BatchBuilderAndMetadata::new(write.builder(operator_batch_lower.clone()))
                });
                for (row, diff) in updates {
                    stage_update(builder, row, ts, diff).await;
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
                    let covered = |time: &mz_repr::Timestamp| {
                        batch_lower.less_equal(time) && !batch_upper.less_equal(time)
                    };

                    let mut batch_tokens = vec![];

                    // This description is only ready once the desired frontier reached its upper,
                    // so every update it covers has arrived. Whatever is still stashed for it is
                    // all of it, and one builder can take the lot.
                    //
                    // The builder is opened on the first surviving update rather than up front,
                    // because every stashed timestamp can consolidate to nothing and a batch with
                    // no updates has no data bounds to register.
                    let stashed_timestamps: Vec<_> =
                        raw_stash.keys().copied().filter(covered).collect();
                    let mut coalesced: Option<SourceBatchBuilder> = None;
                    for ts in stashed_timestamps {
                        let entry = raw_stash.remove(&ts).expect("just looked up");
                        raw_stash_bytes -= entry.bytes;
                        for (row, diff) in entry.drain() {
                            let builder = coalesced.get_or_insert_with(|| {
                                BatchBuilderAndMetadata::new(
                                    write.builder(operator_batch_lower.clone()),
                                )
                            });
                            stage_update(builder, row, ts, diff).await;
                        }
                    }

                    if let Some(builder) = coalesced {
                        if collection_id.is_user() {
                            trace!(
                                "persist_sink {collection_id}/{shard_id}: \
                                    wrote coalesced batch from worker {}: ({:?}, {:?}), \
                                    containing {:?}",
                                worker_index, batch_lower, batch_upper, builder.metrics
                            );
                        }

                        batch_tokens.push(
                            builder
                                .finish(batch_lower.clone(), batch_upper.clone())
                                .await,
                        );
                    }

                    // Timestamps evicted under memory pressure already have builders, one apiece.
                    let spilled_timestamps: Vec<_> =
                        spilled.keys().copied().filter(covered).collect();
                    for ts in spilled_timestamps {
                        let builder = spilled.remove(&ts).expect("just looked up");

                        if collection_id.is_user() {
                            trace!(
                                "persist_sink {collection_id}/{shard_id}: \
                                    wrote spilled batch from worker {}: ({:?}, {:?}) at {ts}, \
                                    containing {:?}",
                                worker_index, batch_lower, batch_upper, builder.metrics
                            );
                        }

                        batch_tokens.push(
                            builder
                                .finish(batch_lower.clone(), batch_upper.clone())
                                .await,
                        );
                    }

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

    // Use `InspectCore::inspect_container` instead of `Inspect::inspect`.
    // `Inspect` carries a `where for<'a> &'a C: IntoIterator` bound, and on
    // macOS the solver can satisfy that bound by chasing objc2's
    // `&Retained<T>: IntoIterator` blanket impl into an endless
    // `Retained<Retained<…>>` chain, overflowing the recursion limit.
    // `InspectCore` has no such bound, so the cascade never starts. We
    // iterate the container by hand to recover the per-item callback.
    let output_stream = if collection_id.is_user() {
        InspectCore::inspect_container(output_stream, |event| {
            if let Ok((_, data)) = event {
                for d in data {
                    trace!("batch: {:?}", d);
                }
            }
        })
    } else {
        output_stream
    };

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
fn append_batches<'scope>(
    scope: Scope<'scope, mz_repr::Timestamp>,
    collection_id: GlobalId,
    operator_name: String,
    target: &CollectionMetadata,
    batch_descriptions: Stream<
        'scope,
        mz_repr::Timestamp,
        Vec<(Antichain<mz_repr::Timestamp>, Antichain<mz_repr::Timestamp>)>,
    >,
    batches: StreamVec<'scope, mz_repr::Timestamp, HollowBatchAndMetadata<mz_repr::Timestamp>>,
    persist_clients: Arc<PersistClientCache>,
    storage_state: &StorageState,
    metrics: SourcePersistSinkMetrics,
    busy_signal: Arc<Semaphore>,
) -> (
    StreamVec<'scope, mz_repr::Timestamp, ()>,
    StreamVec<'scope, mz_repr::Timestamp, Rc<anyhow::Error>>,
    PressOnDropButton,
) {
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
    let (_upper_output, upper_stream) = append_op.new_output::<CapacityContainerBuilder<Vec<_>>>();

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
                                    data_max_ts: batch.data_max_ts,
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

            let validate_part_bounds_on_write = write.validate_part_bounds_on_write();
            let mut todo = VecDeque::new();

            if validate_part_bounds_on_write {
                // Persist will expect each batch's bounds to match the append-time bounds; write them separately.
                for done_batch_metadata in done_batches.drain(..) {
                    in_flight_descriptions.remove(&done_batch_metadata);
                    let batch_set = in_flight_batches
                        .remove(&done_batch_metadata)
                        .unwrap_or_default();
                    todo.push_back((done_batch_metadata, batch_set));
                }
            } else {
                // Persist should allow batches to be written as part of a single append even when the bounds don't
                // match exactly; group all eligible batches together.
                let mut combined_batch_metadata = None;
                let mut combined_batch_set = BatchSet::default();
                for done_batch_metadata in done_batches.drain(..) {
                    in_flight_descriptions.remove(&done_batch_metadata);
                    let mut batch_set = in_flight_batches
                        .remove(&done_batch_metadata)
                        .unwrap_or_default();
                    match combined_batch_metadata.as_mut() {
                        Some((_, upper)) => *upper = done_batch_metadata.1,
                        None => combined_batch_metadata = Some(done_batch_metadata),
                    }
                    combined_batch_set.batch_metrics += &batch_set.batch_metrics;
                    combined_batch_set.finished.append(&mut batch_set.finished);
                }
                if let Some(done_batch_metadata) = combined_batch_metadata {
                    todo.push_back((done_batch_metadata, combined_batch_set))
                }
            };

            while let Some((done_batch_metadata, batch_set)) = todo.pop_front() {
                in_flight_descriptions.remove(&done_batch_metadata);

                let mut batches = batch_set.finished;

                trace!(
                    "persist_sink {collection_id}/{shard_id}: \
                        done batch: {:?}, {:?}",
                    done_batch_metadata,
                    batches
                );

                let (batch_lower, batch_upper) = done_batch_metadata;

                let batch_metrics = batch_set.batch_metrics;

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
                            let _ = tokio::time::timeout(
                                Duration::from_secs(1),
                                read_only_rx.changed(),
                            )
                            .await;

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
                                validate_part_bounds_on_write,
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
                        let committed =
                            batch_metrics.inserts + batch_metrics.retractions;
                        source_statistics
                            .inc_updates_committed_by(committed);
                        metrics.processed_batches.inc();
                        metrics.row_inserts.inc_by(batch_metrics.inserts);
                        metrics.row_retractions.inc_by(batch_metrics.retractions);
                        metrics.error_inserts.inc_by(batch_metrics.error_inserts);
                        metrics
                            .error_retractions
                            .inc_by(batch_metrics.error_retractions);

                        current_upper.borrow_mut().clone_from(&batch_upper);
                        upper_cap_set.downgrade(current_upper.borrow().iter());
                    }
                    Err(mismatch) => {
                        // We tried to to a non-contiguous append, that won't work.
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
                            let new_done_batch_metadata =
                                (new_batch_lower.clone(), batch_upper.clone());

                            // Re-append every batch that still holds something we owe, under the
                            // narrowed description. A batch may hold data on both sides of the new
                            // lower: persist registers it truncated and filters the updates
                            // outside the registered bounds on read, so the ones the concurrent
                            // writer already committed do not come back. A batch entirely below
                            // the new lower owes nothing and is deleted instead, to keep parts
                            // that would be truncated away in full out of shard state.
                            let mut batch_delete_futures = vec![];
                            let mut new_batch_set = BatchSet::default();
                            for batch in batches {
                                if new_batch_lower.less_equal(&batch.data_max_ts) {
                                    new_batch_set.finished.push(batch);
                                } else {
                                    batch_delete_futures.push(batch.batch.delete());
                                }
                            }

                            // Re-add the new batch to the list of batches to process.
                            todo.push_front((new_done_batch_metadata, new_batch_set));

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
        }
    }));

    (upper_stream, errors, shutdown_button.press_on_drop())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::str::FromStr;

    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_dyncfg::{ConfigUpdates, ConfigVal};
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::SYSTEM_TIME;
    use mz_ore::url::SensitiveUrl;
    use mz_persist_client::PersistLocation;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::rpc::PubSubClientConnection;
    use mz_persist_types::ShardId;
    use mz_repr::{Datum, RelationDesc, SqlScalarType};
    use mz_storage_types::sources::SourceEnvelope;
    use mz_storage_types::sources::envelope::{KeyEnvelope, NoneEnvelope};
    use timely::dataflow::operators::Input;

    use crate::statistics::SourceStatisticsMetricDefs;

    use super::*;

    fn ts(t: u64) -> mz_repr::Timestamp {
        t.into()
    }

    fn frontier(t: u64) -> Antichain<mz_repr::Timestamp> {
        Antichain::from_elem(ts(t))
    }

    /// One step of a `write_batches` script.
    enum Step {
        /// Deliver a batch description, as `mint_batch_descriptions` would.
        Description(u64, u64),
        /// Deliver `count` updates at time `at`.
        Updates(u64, usize),
        /// Deliver `count` updates at time `at` with negated diffs, as the rewind of a snapshot
        /// does for rows the replication stream redelivers at their true offset.
        Retractions(u64, usize),
        /// Advance both input frontiers.
        AdvanceTo(u64),
    }

    /// What a batch emitted by `write_batches` carries, flattened for assertions.
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct EmittedBatch {
        lower: u64,
        upper: u64,
        data_max_ts: u64,
        inserts: u64,
    }

    /// Drives `write_batches` through `script` and returns the batches it emitted, along with a
    /// handle to the shard so callers can append them and read the result back.
    fn run_write_batches(
        target: CollectionMetadata,
        persist_clients: Arc<PersistClientCache>,
        max_raw_stash_bytes: usize,
        script: Vec<Step>,
    ) -> Vec<(EmittedBatch, ProtoBatch)> {
        timely::execute_directly(move |worker| {
            // `ProtoBatch` is not `Ord`, so the captured stream is summarized on the way out
            // rather than going through `Capture`.
            let emitted = Rc::new(RefCell::new(Vec::new()));

            let (mut descs_input, mut data_input, button) = worker
                .dataflow::<mz_repr::Timestamp, _, _>(|scope| {
                    let (descs_input, descs) = scope.new_input();
                    let (data_input, data) = scope.new_input();

                    let source_id = GlobalId::User(0);
                    let stats_defs =
                        SourceStatisticsMetricDefs::register_with(&MetricsRegistry::new());
                    let source_statistics = SourceStatistics::new(
                        source_id,
                        0,
                        &stats_defs,
                        source_id,
                        &target.data_shard,
                        SourceEnvelope::None(NoneEnvelope {
                            key_envelope: KeyEnvelope::None,
                            key_arity: 0,
                        }),
                        Antichain::from_elem(Timestamp::minimum()),
                    );

                    let (batches, button) = write_batches(
                        scope,
                        source_id,
                        "test",
                        &target,
                        descs,
                        data.as_collection(),
                        persist_clients,
                        source_statistics,
                        max_raw_stash_bytes,
                        Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
                    );
                    let sink = Rc::clone(&emitted);
                    InspectCore::inspect_container(batches, move |event| {
                        if let Ok((_, data)) = event {
                            for b in data {
                                sink.borrow_mut().push((
                                    EmittedBatch {
                                        lower: b.lower.as_option().expect("single lower").into(),
                                        upper: b.upper.as_option().expect("single upper").into(),
                                        data_max_ts: b.data_max_ts.into(),
                                        inserts: b.metrics.inserts,
                                    },
                                    b.batch.clone(),
                                ));
                            }
                        }
                    });

                    (descs_input, data_input, button)
                });

            // The operator waits on persist off the timely scheduler, so a plain `step` can find
            // the worker idle while the operator is still starting up. Parking hands the thread
            // over until its waker fires, which is what lets the operator keep up with the script.
            fn pump(worker: &mut timely::worker::Worker) {
                for _ in 0..32 {
                    worker.step_or_park(Some(Duration::from_millis(1)));
                }
            }

            // Twice, so the operator is past opening its persist handles before the script runs.
            pump(worker);
            pump(worker);

            for step in script {
                match step {
                    Step::Description(lower, upper) => {
                        descs_input.send((frontier(lower), frontier(upper)));
                    }
                    Step::Updates(at, count) => {
                        for i in 0..i64::try_from(count).expect("small count") {
                            let row = Row::pack_slice(&[Datum::Int64(i)]);
                            data_input.send((Ok(row), ts(at), Diff::ONE));
                        }
                    }
                    Step::Retractions(at, count) => {
                        for i in 0..i64::try_from(count).expect("small count") {
                            let row = Row::pack_slice(&[Datum::Int64(i)]);
                            data_input.send((Ok(row), ts(at), -Diff::ONE));
                        }
                    }
                    Step::AdvanceTo(t) => {
                        descs_input.advance_to(ts(t));
                        data_input.advance_to(ts(t));
                    }
                }
                pump(worker);
            }

            descs_input.close();
            data_input.close();
            for _ in 0..1_000 {
                if !worker.step_or_park(Some(Duration::from_millis(1))) {
                    break;
                }
            }

            drop(button);
            while worker.step() {}

            let mut emitted = emitted.borrow().clone();
            emitted.sort_by(|a, b| a.0.cmp(&b.0));
            emitted
        })
    }

    fn test_target() -> CollectionMetadata {
        CollectionMetadata {
            persist_location: PersistLocation {
                blob_uri: SensitiveUrl::from_str("mem://").expect("invalid URL"),
                consensus_uri: SensitiveUrl::from_str("mem://").expect("invalid URL"),
            },
            data_shard: ShardId::new(),
            relation_desc: RelationDesc::builder()
                .with_column("a", SqlScalarType::Int64.nullable(false))
                .finish(),
            txns_shard: None,
        }
    }

    /// Persist clients with part bounds validation on. Both settings default off in code but are
    /// turned on in production, so an append has to run under them to say anything about the
    /// bounds the sink writes.
    fn test_persist_clients() -> Arc<PersistClientCache> {
        let persist_cfg =
            PersistConfig::new_default_configs(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone());
        let mut updates = ConfigUpdates::default();
        updates.add_dynamic(
            "persist_validate_part_bounds_on_write",
            ConfigVal::Bool(true),
        );
        updates.add_dynamic(
            "persist_validate_part_bounds_on_read",
            ConfigVal::Bool(true),
        );
        updates.apply(&persist_cfg.configs);
        Arc::new(PersistClientCache::new(
            persist_cfg,
            &MetricsRegistry::new(),
            |_, _| PubSubClientConnection::noop(),
        ))
    }

    /// Appends every emitted batch in one `compare_and_append` over `[lower, upper)`, then reads
    /// the shard back and returns the summed diffs.
    ///
    /// This is where a batch whose parts reach outside their registered bounds is caught, so the
    /// tests append for real rather than stopping at what `write_batches` emitted. A `lower` above
    /// the batches' own lower registers them truncated, which is what the sink relies on when a
    /// concurrent writer has already claimed part of the range.
    async fn append_and_read_back(
        target: &CollectionMetadata,
        persist_clients: &PersistClientCache,
        emitted: Vec<(EmittedBatch, ProtoBatch)>,
        lower: u64,
        upper: u64,
    ) -> i64 {
        let persist_client = persist_clients
            .open(target.persist_location.clone())
            .await
            .expect("could not open persist client");
        let mut write = persist_client
            .open_writer::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                target.data_shard,
                Arc::new(target.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("could not open persist shard");

        assert!(
            write.validate_part_bounds_on_write(),
            "part bounds validation is off, so this append proves nothing about batch bounds"
        );

        let mut batches: Vec<_> = emitted
            .into_iter()
            .map(|(_, proto)| write.batch_from_transmittable_batch(proto))
            .collect();
        let mut to_append: Vec<_> = batches.iter_mut().collect();
        write
            .compare_and_append_batch(&mut to_append[..], frontier(lower), frontier(upper), true)
            .await
            .expect("invalid usage")
            .expect("upper mismatch");

        assert_eq!(write.fetch_recent_upper().await, &frontier(upper));

        let mut read = persist_client
            .open_leased_reader::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                target.data_shard,
                Arc::new(target.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
                true,
            )
            .await
            .expect("invalid usage");
        let contents = read
            .snapshot_and_fetch(frontier(upper - 1))
            .await
            .expect("since <= as_of");

        contents.iter().map(|(_, _, d)| *d).sum()
    }

    /// Advances the shard upper to `upper` without writing data, standing in for a concurrent
    /// writer that reached part of the range first.
    async fn advance_shard_upper(
        target: &CollectionMetadata,
        persist_clients: &PersistClientCache,
        upper: u64,
    ) {
        let persist_client = persist_clients
            .open(target.persist_location.clone())
            .await
            .expect("could not open persist client");
        let mut write = persist_client
            .open_writer::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                target.data_shard,
                Arc::new(target.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("could not open persist shard");

        let empty: Vec<((SourceData, ()), mz_repr::Timestamp, StorageDiff)> = Vec::new();
        write
            .compare_and_append(
                &empty,
                Antichain::from_elem(Timestamp::minimum()),
                frontier(upper),
            )
            .await
            .expect("invalid usage")
            .expect("upper mismatch");
    }

    /// A batch spanning many timestamps stays usable when a concurrent writer has raised the shard
    /// upper into the middle of it. Persist registers the batch under the narrowed description and
    /// filters the updates outside those bounds on read, so the sink can hand a straddling batch
    /// over as is rather than discarding it and rebuilding from the new upper.
    ///
    /// This is the property that lets `write_batches` coalesce a stalled frontier into one batch
    /// without giving up the ability to recover from a concurrent append.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait
    async fn a_straddling_batch_is_usable_under_a_raised_lower() {
        const TIMES: u64 = 10;
        const DONE: u64 = TIMES + 1;
        // Inside the batch's data range, so the batch holds updates on both sides of it.
        const RAISED_LOWER: u64 = 5;

        let persist_clients = test_persist_clients();
        let target = test_target();

        // One update at each of times 1..=TIMES, all covered by a single description, so the
        // stash coalesces them into one batch whose data spans the whole range.
        let mut script = vec![];
        for at in 1..=TIMES {
            script.push(Step::Updates(at, 1));
        }
        script.push(Step::Description(0, DONE));
        script.push(Step::AdvanceTo(DONE));

        let emitted = run_write_batches(
            target.clone(),
            Arc::clone(&persist_clients),
            1 << 20,
            script,
        );
        assert_eq!(
            emitted.len(),
            1,
            "expected one coalesced batch, got {:?}",
            emitted.iter().map(|(b, _)| b).collect::<Vec<_>>()
        );

        advance_shard_upper(&target, &persist_clients, RAISED_LOWER).await;

        // Append the straddling batch under the raised lower, as the sink does after an
        // `UpperMismatch` cuts the description down.
        let total =
            append_and_read_back(&target, &persist_clients, emitted, RAISED_LOWER, DONE).await;

        assert_eq!(
            total,
            i64::try_from(TIMES - RAISED_LOWER + 1).expect("small"),
            "only the updates at or above the raised lower should be readable, and all of them"
        );
    }

    /// The rewind mechanism retracts the snapshot's copy of every row the replication stream
    /// redelivers at its true offset, and both land at the pinned timestamp. Those pairs cancel,
    /// so the stash collapses them rather than evicting to make room.
    ///
    /// NOTE: this only reclaims pairs that are in the stash at the same time. Once a timestamp
    /// has been evicted its updates live in a builder, where a later retraction cannot reach
    /// them, and the cancellation is left to persist compaction.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait
    async fn write_batches_consolidates_the_stash_before_evicting() {
        const ROWS: usize = 512;
        const DONE: u64 = 4;

        let persist_clients = test_persist_clients();
        let target = test_target();

        // The snapshot's rows, then the rewind retracting all but one of them, all at the pinned
        // timestamp while the frontier is stalled.
        let script = vec![
            Step::Updates(1, ROWS),
            Step::Retractions(1, ROWS - 1),
            Step::Description(0, DONE),
            Step::AdvanceTo(DONE),
        ];

        // Big enough to hold the snapshot's rows, too small to also hold their retractions.
        // Cancelling pairs only collapse while both sides are still in the stash, so a budget
        // that evicted the rows before their retractions arrived would prove nothing.
        let unit = stashed_bytes(&Ok(Row::pack_slice(&[Datum::Int64(0)])));
        let budget = unit * (ROWS + ROWS / 2);

        let emitted =
            run_write_batches(target.clone(), Arc::clone(&persist_clients), budget, script);

        assert_eq!(
            emitted.len(),
            1,
            "cancelling updates should consolidate away rather than evict, got {:?}",
            emitted.iter().map(|(b, _)| b).collect::<Vec<_>>()
        );
        assert_eq!(
            emitted[0].0.inserts, 1,
            "only the surviving row should reach the batch"
        );

        let total = append_and_read_back(&target, &persist_clients, emitted, 0, DONE).await;
        assert_eq!(total, 1, "the shard should hold exactly the surviving row");
    }

    /// A row inserted and deleted at the same timestamp consolidates to nothing, which can leave
    /// a description with no updates at all to write. That must emit no batch rather than open a
    /// builder that has no data bounds to register.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait
    async fn write_batches_emits_nothing_when_a_description_fully_consolidates() {
        const DONE: u64 = 4;

        let persist_clients = test_persist_clients();

        // A budget the stash never reaches, so the updates sit unconsolidated until the
        // description drains them, and one that forces eviction to handle the same entry.
        for budget in [1 << 20, 0] {
            // Everything the description covers cancels out.
            let script = vec![
                Step::Updates(1, 8),
                Step::Retractions(1, 8),
                Step::Description(0, DONE),
                Step::AdvanceTo(DONE),
            ];

            let emitted =
                run_write_batches(test_target(), Arc::clone(&persist_clients), budget, script);

            assert!(
                emitted.is_empty(),
                "a fully consolidated description should produce no batch at budget {budget}, \
                got {:?}",
                emitted.iter().map(|(b, _)| b).collect::<Vec<_>>()
            );
        }
    }

    /// A snapshot at time 1 pinning the frontier while replication delivers one update at each of
    /// times 2..=`stall_times`+1, then the description that covers the whole stall.
    fn pinned_frontier_script(snapshot_rows: usize, stall_times: u64, done: u64) -> Vec<Step> {
        let mut script = vec![Step::Updates(1, snapshot_rows)];
        for t in 2..=stall_times + 1 {
            script.push(Step::Updates(t, 1));
        }
        // The minter holds a capability at the shard upper for the whole stall, so its one
        // description is emitted there, and the frontier then jumps past everything staged.
        script.push(Step::Description(0, done));
        script.push(Step::AdvanceTo(done));
        script
    }

    /// A snapshot pins the export's frontier at its as_of while concurrent replication keeps
    /// delivering updates at later times. No description is minted for the duration, so the
    /// updates stage as raw rows, and the single description minted once the snapshot finishes
    /// takes all of them into one batch regardless of how many timestamps they span.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait
    async fn write_batches_coalesces_a_pinned_frontier_into_one_batch() {
        const SNAPSHOT_ROWS: usize = 4;
        const STALL_TIMES: u64 = 16;
        const DONE: u64 = STALL_TIMES + 2;

        let persist_clients = test_persist_clients();
        let target = test_target();

        // A budget far above what this script stages, so nothing is evicted.
        let emitted = run_write_batches(
            target.clone(),
            Arc::clone(&persist_clients),
            1 << 20,
            pinned_frontier_script(SNAPSHOT_ROWS, STALL_TIMES, DONE),
        );

        assert_eq!(
            emitted.len(),
            1,
            "the stall should coalesce into a single batch, got {:?}",
            emitted.iter().map(|(b, _)| b).collect::<Vec<_>>()
        );
        assert_eq!(
            emitted[0].0,
            EmittedBatch {
                lower: 0,
                upper: DONE,
                data_max_ts: STALL_TIMES + 1,
                inserts: u64::cast_from(SNAPSHOT_ROWS) + STALL_TIMES,
            }
        );

        let total = append_and_read_back(&target, &persist_clients, emitted, 0, DONE).await;
        assert_eq!(
            total,
            i64::try_from(SNAPSHOT_ROWS).expect("small")
                + i64::try_from(STALL_TIMES).expect("small"),
            "every staged update should be readable exactly once"
        );
    }

    /// Under a budget the stash cannot meet, updates are evicted into per-timestamp batches
    /// rather than held in memory. Those batches are still appended and read back correctly,
    /// which is what makes the eviction a graceful degradation rather than a failure.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait
    async fn write_batches_spills_single_timestamp_batches_when_over_budget() {
        const SNAPSHOT_ROWS: usize = 4;
        const STALL_TIMES: u64 = 16;
        const DONE: u64 = STALL_TIMES + 2;

        let persist_clients = test_persist_clients();
        let target = test_target();

        // A zero budget evicts every timestamp as soon as it is staged.
        let emitted = run_write_batches(
            target.clone(),
            Arc::clone(&persist_clients),
            0,
            pinned_frontier_script(SNAPSHOT_ROWS, STALL_TIMES, DONE),
        );

        assert_eq!(
            emitted.len(),
            usize::cast_from(STALL_TIMES) + 1,
            "every timestamp should have been evicted to its own batch, got {:?}",
            emitted.iter().map(|(b, _)| b).collect::<Vec<_>>()
        );
        for (batch, _) in &emitted {
            assert_eq!(
                (batch.lower, batch.upper),
                (0, DONE),
                "an evicted batch still carries the description bounds"
            );
        }
        // One batch per staged timestamp. That is what makes evicting safe before the covering
        // description is known, since a single-timestamp batch cannot span a description boundary.
        let data_times: Vec<_> = emitted.iter().map(|(b, _)| b.data_max_ts).collect();
        assert_eq!(data_times, (1..=STALL_TIMES + 1).collect::<Vec<_>>());

        let total = append_and_read_back(&target, &persist_clients, emitted, 0, DONE).await;
        assert_eq!(
            total,
            i64::try_from(SNAPSHOT_ROWS).expect("small")
                + i64::try_from(STALL_TIMES).expect("small"),
            "eviction must not change what the shard ends up holding"
        );
    }
}
