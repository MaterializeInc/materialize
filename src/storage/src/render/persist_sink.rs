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
//!                                 /   ,-+-----------------------.
//!                                /    | capture max_seen_time   |
//!                               /     | one arbitrary worker    |
//!                              /      +-,--,--------+----+------+
//!                             /             |
//!                            |              |
//!                            |        ,-+-----------------------.
//!                            |        | mint_batch_descriptions |
//!                            |        | one arbitrary worker    |
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
//! Ahead of `mint_batch_descriptions` sits `max_seen_timestamps`, which passes the source
//! collection through and reports each worker's largest timestamp to the minting worker. The
//! minter also broadcasts a committed ceiling to the writers, which `append_batches` does not see.
//! Both are left out of the diagram above to keep the shape of the three main operators readable.
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
    /// entirely below a raised append lower. A batch completely below the append lower is
    /// deleted. A batch whose data straddles an append lower has its bounds adjusted instead.
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

/// How far past the data the minter commits its ceiling while a snapshot pins the frontier, in
/// milliseconds, the unit of `mz_repr::Timestamp`. `None` turns committing ahead off.
///
/// Floored at `timestamp_interval`, since a ceiling that does not clear a timestamp tick is behind
/// the data by the time it reaches a writer.
fn description_lookahead(lookahead: Duration, timestamp_interval: Duration) -> Option<u64> {
    let to_millis = |d: Duration| {
        u64::try_from(d.as_millis()).unwrap_or_else(|_| panic!("{:?} cannot fit in u64", d))
    };
    let lookahead = to_millis(lookahead);
    // The floor is applied past the zero test, which is the only value that turns committing
    // ahead off.
    (lookahead > 0).then(|| lookahead.max(to_millis(timestamp_interval)))
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
///
/// `snapshotting` says whether this incarnation will snapshot the collection, which is what lets
/// the sink group updates behind the pinned frontier a snapshot holds. See
/// [`mint_batch_descriptions`].
pub(crate) fn render<'scope>(
    scope: Scope<'scope, mz_repr::Timestamp>,
    collection_id: GlobalId,
    target: CollectionMetadata,
    desired_collection: VecCollection<'scope, mz_repr::Timestamp, Result<Row, DataflowError>, Diff>,
    storage_state: &StorageState,
    metrics: SourcePersistSinkMetrics,
    busy_signal: Arc<Semaphore>,
    snapshotting: bool,
    timestamp_interval: Duration,
) -> (
    StreamVec<'scope, mz_repr::Timestamp, ()>,
    StreamVec<'scope, mz_repr::Timestamp, Rc<anyhow::Error>>,
    Vec<PressOnDropButton>,
) {
    let persist_clients = Arc::clone(&storage_state.persist_clients);

    let operator_name = format!("persist_sink({})", collection_id);

    let config_set = storage_state.storage_configuration.config_set();
    let lookahead = description_lookahead(
        dyncfgs::STORAGE_PERSIST_SINK_DESCRIPTION_LOOKAHEAD.get(config_set),
        timestamp_interval,
    );

    let (desired_stream, max_seen_stream, max_seen_token) =
        max_seen_timestamps(desired_collection.inner, &operator_name);

    let (batch_descriptions, commitments, passthrough_desired_stream, mint_token) =
        mint_batch_descriptions(
            scope,
            collection_id,
            &operator_name,
            &target,
            desired_stream.as_collection(),
            max_seen_stream,
            Arc::clone(&persist_clients),
            lookahead,
            snapshotting,
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
        commitments,
        passthrough_desired_stream.as_collection(),
        Arc::clone(&persist_clients),
        source_statistics,
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
        vec![max_seen_token, mint_token, write_token, append_token],
    )
}

/// Passes `desired` through, and on a second output reports the largest timestamp this worker's
/// share has reached.
///
/// `desired` is pre-sharded, so any one worker sees a share of it. The batch description minter
/// establishes the committed based the data timestamps, which has to account for every share.
/// Not every source round-robins messages, and a lack of visibility (e.g. data on one worker,
/// minter on another) would result in steady state behavior (batch-per-timestamp).
///
/// Reported only when the largest timestamp grows, so this carries one timestamp per worker per
/// distinct time rather than one per batch.
fn max_seen_timestamps<'scope>(
    desired: StreamVec<
        'scope,
        mz_repr::Timestamp,
        (Result<Row, DataflowError>, mz_repr::Timestamp, Diff),
    >,
    operator_name: &str,
) -> (
    StreamVec<'scope, mz_repr::Timestamp, (Result<Row, DataflowError>, mz_repr::Timestamp, Diff)>,
    StreamVec<'scope, mz_repr::Timestamp, mz_repr::Timestamp>,
    PressOnDropButton,
) {
    let mut op = AsyncOperatorBuilder::new(
        format!("{} max_seen_timestamps", operator_name),
        desired.scope(),
    );

    let (data_output, data_stream) = op.new_output::<CapacityContainerBuilder<Vec<_>>>();
    let (max_output, max_stream) = op.new_output::<CapacityContainerBuilder<Vec<_>>>();
    let mut input = op.new_input_for_many(desired, Pipeline, [&data_output, &max_output]);

    let button = op.build(move |capabilities| async move {
        // Both outputs are driven by the input, so they use its data capabilities.
        drop(capabilities);

        let mut max_seen: Option<mz_repr::Timestamp> = None;
        while let Some(event) = input.next().await {
            let Event::Data([data_cap, max_cap], mut data) = event else {
                continue;
            };
            if let Some(next_max) = data.iter().map(|(_, ts, _)| *ts).max()
                && max_seen < Some(next_max)
            {
                max_seen = Some(next_max);
                max_output.give(&max_cap, next_max);
            }
            data_output.give_container(&data_cap, &mut data);
        }
    });

    (data_stream, max_stream, button.press_on_drop())
}

/// Whenever the frontier advances, this mints a new batch description (lower
/// and upper) that writers should use for writing the next set of batches to
/// persist.
///
/// With a `lookahead`, and while a `snapshotting` export holds the frontier pinned, it also commits
/// to a ceiling that far past the data and broadcasts it on the second output. A ceiling is not a
/// description: it gives the writers a bound to group updates under before the frontier certifies
/// anything, and it binds this operator, which mints nothing below an outstanding ceiling. So the
/// whole snapshot and the catch-up behind it become one description, emitted when the frontier
/// reaches the ceiling. See [`next_mint`].
///
/// Only one of the workers does this, meaning there will only be one
/// description in the stream, even in case of multiple timely workers. Use
/// `broadcast()` to, ahem, broadcast, the one description to all downstream
/// write operators/workers.
///
/// `max_seen` carries every worker's largest timestamp, which paces the commitments. It is routed
/// here from [`max_seen_timestamps`], so it is the same collection `desired_collection` is a share
/// of.
fn mint_batch_descriptions<'scope>(
    scope: Scope<'scope, mz_repr::Timestamp>,
    collection_id: GlobalId,
    operator_name: &str,
    target: &CollectionMetadata,
    desired_collection: VecCollection<'scope, mz_repr::Timestamp, Result<Row, DataflowError>, Diff>,
    max_seen: StreamVec<'scope, mz_repr::Timestamp, mz_repr::Timestamp>,
    persist_clients: Arc<PersistClientCache>,
    lookahead: Option<u64>,
    snapshotting: bool,
) -> (
    StreamVec<
        'scope,
        mz_repr::Timestamp,
        (Antichain<mz_repr::Timestamp>, Antichain<mz_repr::Timestamp>),
    >,
    StreamVec<'scope, mz_repr::Timestamp, Commitment>,
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
    let (ceiling_output, ceiling_output_stream) =
        mint_op.new_output::<CapacityContainerBuilder<Vec<_>>>();
    let (data_output, data_output_stream) =
        mint_op.new_output::<CapacityContainerBuilder<Vec<_>>>();

    // The description, ceiling and data-passthrough outputs are all driven by this input, so
    // they use a standard input connection.
    let mut desired_input = mint_op.new_input_for_many(
        desired_collection.inner,
        Pipeline,
        [&output, &ceiling_output, &data_output],
    );

    // Every worker's share of the data reports its largest timestamp here. This is a disconnected
    // input because it doesn't drive the outputs, it only influences the bounds of descriptions.
    let mut max_seen_input =
        mint_op.new_disconnected_input(max_seen, Exchange::new(move |_| hashed_id));

    let shutdown_button = mint_op.build(move |capabilities| async move {
        // Non-active workers should just pass the data through.
        if !active_worker {
            // The description and ceiling outputs are entirely driven by the active worker, so we
            // drop their capabilities here. The data-passthrough output just uses the data
            // capabilities.
            drop(capabilities);
            while let Some(event) = desired_input.next().await {
                match event {
                    Event::Data([_output_cap, _ceiling_cap, data_output_cap], mut data) => {
                        data_output.give_container(&data_output_cap, &mut data);
                    }
                    Event::Progress(_) => {}
                }
            }
            return;
        }
        // The data-passthrough output should will use the data capabilities, so we drop
        // its capability here.
        let [desc_cap, ceiling_cap, _]: [_; 3] =
            capabilities.try_into().expect("one capability per output");
        let mut cap_set = CapabilitySet::from_elem(desc_cap);
        let mut ceiling_cap_set = CapabilitySet::from_elem(ceiling_cap);

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

        // The current input frontier.
        let mut desired_frontier = Antichain::from_elem(mz_repr::Timestamp::minimum());

        // The largest timestamp any worker's share of the data has reached, which is what the next
        // commitment is timed against. See [`max_seen_timestamps`].
        let mut max_seen_ts: Option<mz_repr::Timestamp> = None;

        // The first non-minimum frontier the collection takes, tracked only while `snapshotting`.
        // A source emits its snapshot at the minimum from-time, so the whole snapshot occupies the
        // single time that reclocks to, and the frontier moving off that value is the snapshot
        // ending. Frontiers only advance, so this can never re-arm. Nothing is committed before it
        // arms: timely does not order progress ahead of data, and a row seen under the minimum
        // frontier would otherwise anchor the ceiling at the shard upper, which on a fresh shard
        // is the whole gap from zero to the wall clock.
        let mut first_frontier: Option<Antichain<mz_repr::Timestamp>> = None;

        // The outstanding ceiling, if any. While one is held nothing below it is minted, which is
        // what makes it binding.
        let mut committed: Option<mz_repr::Timestamp> = None;

        loop {
            tokio::select! {
                event = desired_input.next() => match event {
                    Some(Event::Data([_output_cap, _ceiling_cap, data_output_cap], mut data)) => {
                        // Just passthrough the data.
                        data_output.give_container(&data_output_cap, &mut data);
                    }
                    Some(Event::Progress(frontier)) => {
                        if snapshotting
                            && first_frontier.is_none()
                            && frontier != Antichain::from_elem(mz_repr::Timestamp::minimum())
                        {
                            first_frontier = Some(frontier.clone());
                        }
                        desired_frontier = frontier;
                    }
                    // Input is exhausted, so we can shut down.
                    None => return,
                },
                // Reports arrive on their own edge, so one can trail the data it summarizes by a
                // round. That only delays a commitment, and the first one is timed against the
                // frontier rather than the data.
                Some(event) = max_seen_input.next() => {
                    if let Event::Data(_cap, data) = event {
                        max_seen_ts = std::cmp::max(max_seen_ts, data.into_iter().max());
                    }
                }
            }

            let snapshot_in_progress = snapshotting
                && first_frontier
                    .as_ref()
                    .is_some_and(|first| *first == desired_frontier);

            while let Some(mint) = next_mint(
                &current_upper,
                &desired_frontier,
                max_seen_ts,
                committed,
                lookahead.filter(|_| snapshot_in_progress),
            ) {
                let lower = current_upper
                    .as_option()
                    .copied()
                    .expect("a non-empty current upper, or nothing is minted");

                let upper = match mint {
                    Mint::Ceiling(ceiling) => {
                        let cap = ceiling_cap_set
                            .try_delayed(&lower)
                            .expect("ceiling capability holds the current upper");
                        trace!(
                            "persist_sink {collection_id}/{shard_id}: \
                                committing ceiling: {:?}",
                            ceiling
                        );
                        ceiling_output.give(&cap, Commitment { lower, ceiling });
                        committed = Some(ceiling);
                        continue;
                    }
                    Mint::Description(upper) => upper,
                };

                let batch_description = (current_upper.to_owned(), upper.to_owned());

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
                    upper
                );
                cap_set.downgrade(upper.iter());
                ceiling_cap_set.downgrade(upper.iter());

                // The description that retires a ceiling covers everything the writers grouped
                // under it, so the next one starts fresh.
                committed = None;
                current_upper = upper;
            }
        }
    });

    (
        output_stream,
        ceiling_output_stream,
        data_output_stream,
        shutdown_button.press_on_drop(),
    )
}

/// A bound the minter commits ahead of the frontier, broadcast to the writers.
///
/// `lower` is the lower of the description that will end at or past `ceiling`, which the writers
/// need before that description exists, since a builder declares its lower when it opens.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
struct Commitment {
    lower: mz_repr::Timestamp,
    ceiling: mz_repr::Timestamp,
}

/// What [`mint_batch_descriptions`] does with the frontier and the data it has seen.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Mint {
    /// Emit a description ending here, the unit `append_batches` appends.
    Description(Antichain<mz_repr::Timestamp>),
    /// Commit to this ceiling and broadcast it to the writers. Not a description: it gives them a
    /// bound to group under, and binds the minter to mint nothing below it.
    Ceiling(mz_repr::Timestamp),
}

/// The next thing the minter emits, or `None` when there is nothing to do.
///
/// A description is derived from the frontier, which certifies that everything below it has
/// arrived. An outstanding `committed` ceiling suppresses that until the frontier reaches the
/// ceiling, which is what collapses a whole snapshot and the catch-up behind it into one
/// description.
///
/// A `lookahead` is given only while a snapshot pins the frontier, and commits a ceiling that far
/// past the largest timestamp the data has reached. The lead is what lets the ceiling reach the
/// writers before the updates it covers, since a builder only takes updates at times it was opened
/// for. Before any data the snapshot's rows are about to land at the pinned time itself, so that
/// stands in for the data.
///
/// Committing is confined to a snapshot because a ceiling is binding. While it is outstanding no
/// description is derived from the frontier either, so once the snapshot ends the shard upper waits
/// for the frontier to reach the ceiling rather than advancing to where the frontier actually is. A
/// collection that is keeping up lags the data by about one timestamp and so has nothing to group,
/// and would pay that wait for nothing.
fn next_mint(
    current_upper: &Antichain<mz_repr::Timestamp>,
    desired_frontier: &Antichain<mz_repr::Timestamp>,
    max_seen_ts: Option<mz_repr::Timestamp>,
    committed: Option<mz_repr::Timestamp>,
    lookahead: Option<u64>,
) -> Option<Mint> {
    let frontier_reached = |ts: mz_repr::Timestamp| {
        PartialOrder::less_equal(&Antichain::from_elem(ts), desired_frontier)
    };
    if committed.is_none_or(frontier_reached)
        && PartialOrder::less_than(current_upper, desired_frontier)
    {
        return Some(Mint::Description(desired_frontier.clone()));
    }

    let lookahead = lookahead?;
    let lower = *current_upper.as_option()?;
    // Before any data the snapshot's rows are about to land at the pinned time itself, so that
    // stands in for the data.
    let ceiling = max_seen_ts.unwrap_or(lower).checked_add(lookahead)?;
    (lower < ceiling && committed.is_none_or(|c| c < ceiling)).then_some(Mint::Ceiling(ceiling))
}

/// Writes `desired_collection` to persist, but only for updates
/// that fall into batch a description that we get via `batch_descriptions`.
/// This forwards a `HollowBatch` (with additional metadata)
/// for any batch of updates that was written.
///
/// Every update below the ceiling `mint_batch_descriptions` commits goes into one open builder,
/// whatever its timestamp, so a pinned frontier costs one batch rather than one per timestamp. An
/// update that outruns the ceiling, or arrives while none is committed, has no bound to be grouped
/// under and writes a batch of its own timestamp, which is what the sink does for every update when
/// nothing is ever committed ahead of the frontier.
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
    commitments: StreamVec<'scope, mz_repr::Timestamp, Commitment>,
    desired_collection: VecCollection<'scope, mz_repr::Timestamp, Result<Row, DataflowError>, Diff>,
    persist_clients: Arc<PersistClientCache>,
    source_statistics: SourceStatistics,
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
    // Commitments only route updates into builders, so this input is disconnected: holding the
    // output back on it would stall every batch behind the minter's own progress.
    let mut commitments_input = write_op.new_disconnected_input(commitments.broadcast(), Pipeline);
    let mut desired_input = write_op.new_disconnected_input(desired_collection.inner, Pipeline);

    // This operator accepts the current and desired update streams for a `persist` shard.
    // It attempts to write out updates, starting from the current's upper frontier, that
    // will cause the changes of desired to be committed to persist, _but only those also past the
    // upper_.

    let shutdown_button = write_op.build(move |_capabilities| async move {
        // Builders for timestamps no ceiling covers, keyed by timestamp.
        //
        // A batch builder cannot be split, so an update may only join updates at other timestamps
        // once a bound they all fall below is known. Until then a timestamp gets a builder to
        // itself, which is safe without knowing the descriptions because a description covers a
        // timestamp entirely or not at all. Such a builder stays below `persist_blob_target_size`
        // and so holds its rows in memory, so it is finished as soon as a description covers it
        // rather than held until that description is ready.
        let mut uncovered_builders: BTreeMap<mz_repr::Timestamp, SourceBatchBuilder> =
            BTreeMap::new();

        // The outstanding commitment: the bounds the open builder takes updates in, and the lower
        // it declares. The ceiling is raised to the description's own upper once that arrives, so
        // rows landing between arrival and readiness still join. `None` until the first commitment,
        // which leaves every timestamp writing its own batch.
        let mut commitment: Option<Commitment> = None;

        // The one open builder, holding every update inside the commitment regardless of
        // timestamp. It reaches `persist_blob_target_size` and spills its parts to blob, so what it
        // holds resident is one unflushed part however long the frontier stays pinned. Only ever
        // one, because the minter mints nothing below an outstanding ceiling, so there is exactly
        // one description in flight for it to be finished under.
        let mut open_builder: Option<SourceBatchBuilder> = None;

        // Batches finished before their description became ready, keyed by the description's
        // lower. A description can take any number of batches, so an uncovered builder is closed
        // as soon as a description covers it rather than held open.
        let mut description_batches: BTreeMap<
            mz_repr::Timestamp,
            Vec<HollowBatchAndMetadata<mz_repr::Timestamp>>,
        > = BTreeMap::new();

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
                _ = commitments_input.ready() => {},
                _ = desired_input.ready() => {},
            }

            // Collect ready work from all three inputs. Commitments and descriptions are processed
            // before the data of the same round so that an update whose bound arrived alongside it
            // can go straight into the open builder instead of writing a batch of its own.
            let ready_commitments =
                std::iter::from_fn(|| commitments_input.next_sync()).collect_vec();
            let ready_descriptions =
                std::iter::from_fn(|| descriptions_input.next_sync()).collect_vec();
            let ready_events = std::iter::from_fn(|| desired_input.next_sync()).collect_vec();

            // We now start the async work for the input we received. Until we finish the dataflow
            // should be marked as busy.
            let permit = busy_signal.acquire().await;

            for event in ready_commitments {
                let Event::Data(_cap, data) = event else {
                    continue;
                };
                for next in data {
                    // A commitment for a different lower belongs to a later description, so it
                    // cannot widen the open builder, which is finished under its own.
                    match commitment {
                        Some(held) if held.lower == next.lower => {
                            commitment = Some(Commitment {
                                ceiling: held.ceiling.max(next.ceiling),
                                ..held
                            })
                        }
                        Some(_) => {}
                        None => commitment = Some(next),
                    }
                }
            }

            for event in ready_descriptions {
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

                            let (lower, upper) = (&description.0, &description.1);
                            let lower_ts = *lower
                                .as_option()
                                .expect("minted descriptions have a single-element lower");

                            // The description that retires a commitment ends at or past its
                            // ceiling, so rows landing between its arrival and its readiness can
                            // still join the builder it will be finished under.
                            if let Some(held) = commitment.filter(|held| held.lower == lower_ts)
                                && let Some(upper_ts) = upper.as_option()
                            {
                                commitment = Some(Commitment {
                                    ceiling: held.ceiling.max(*upper_ts),
                                    ..held
                                });
                            }

                            // Finish any per-timestamp builders when a description arrives to avoid
                            // keeping things in memory. This isn't ideal as we're creating a batch
                            // with a potentially small number of rows, but it should be rare. This
                            // can happen when data arrives ahead of the description, so if this
                            // becomes a problem, we should look to get descriptions to this
                            // operator sooner.
                            let uncovered_timestamps: Vec<_> = uncovered_builders
                                .keys()
                                .filter(|ts| lower.less_equal(ts) && !upper.less_equal(ts))
                                .copied()
                                .collect();
                            for ts in uncovered_timestamps {
                                let builder =
                                    uncovered_builders.remove(&ts).expect("just looked up");
                                let batch = builder.finish(lower.clone(), upper.clone()).await;
                                description_batches.entry(lower_ts).or_default().push(batch);
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
                                // matches no later description, so its batch would never be
                                // appended and the update would be lost unnoticed. Not a
                                // `debug_assert!`, which compiles out of the optimized and release
                                // profiles and would leave the loss silent everywhere it matters.
                                assert!(
                                    operator_batch_lower.less_equal(&ts),
                                    "persist_sink {collection_id}/{shard_id}: update at {ts:?} \
                                    arrived below the emitted batch lower {operator_batch_lower:?}",
                                );

                                let inside =
                                    commitment.filter(|held| held.lower <= ts && ts < held.ceiling);
                                let builder = if let Some(held) = inside {
                                    // The description retiring the commitment is known to contain
                                    // it, so the update joins the one open builder whatever its
                                    // timestamp.
                                    open_builder.get_or_insert_with(|| {
                                        BatchBuilderAndMetadata::new(
                                            write.builder(Antichain::from_elem(held.lower)),
                                        )
                                    })
                                } else {
                                    // Nothing to group under, so the only lower this builder can
                                    // declare is the operator's own, the one lower at or below
                                    // every description that could come to cover it. That
                                    // declaration is what registers the batch truncated once it is
                                    // appended under a description's narrower bounds.
                                    uncovered_builders.entry(ts).or_insert_with(|| {
                                        BatchBuilderAndMetadata::new(
                                            write.builder(operator_batch_lower.clone()),
                                        )
                                    })
                                };
                                stage_update(builder, row, ts, diff).await;
                                source_statistics.inc_updates_staged_by(1);
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
                    let lower = *batch_lower
                        .as_option()
                        .expect("minted descriptions have a single-element lower");

                    let mut batch_tokens = description_batches.remove(&lower).unwrap_or_default();

                    // Updates that arrived after this description did, with no commitment to group
                    // them, are still in builders of their own.
                    let covered: Vec<_> = uncovered_builders
                        .keys()
                        .copied()
                        .filter(|ts| batch_lower.less_equal(ts) && !batch_upper.less_equal(ts))
                        .collect();
                    for ts in covered {
                        let builder = uncovered_builders.remove(&ts).expect("just looked up");
                        batch_tokens.push(
                            builder
                                .finish(batch_lower.clone(), batch_upper.clone())
                                .await,
                        );
                    }

                    if commitment.is_some_and(|held| held.lower == lower)
                        && let Some(builder) = open_builder.take()
                    {
                        commitment = None;
                        if collection_id.is_user() {
                            trace!(
                                "persist_sink {collection_id}/{shard_id}: \
                                    wrote batch from worker {}: ({:?}, {:?}), containing {:?}",
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
    #[derive(Clone)]
    enum Step {
        /// Deliver a batch description, as `mint_batch_descriptions` would.
        Description(u64, u64),
        /// Deliver a commitment, as `mint_batch_descriptions` would.
        Commit(u64, u64),
        /// Deliver `count` updates at time `at`.
        Updates(u64, usize),
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

    /// Runs a timely worker to completion on a blocking thread.
    ///
    /// The test body is a single poll of the runtime's `block_on` future, so it runs under one
    /// tokio cooperative budget. A worker driven inline spends that budget on the operators'
    /// `select!` and semaphore polls, and once it is gone every such poll returns `Pending` and
    /// re-wakes itself, parking the operator for good with no error. Blocking threads have no
    /// budget.
    async fn run_worker<T: Send + 'static>(
        worker: impl FnOnce(&mut timely::worker::Worker) -> T + Send + Sync + 'static,
    ) -> T {
        mz_ore::task::spawn_blocking(
            || "persist_sink_test_worker",
            move || timely::execute_directly(worker),
        )
        .await
    }

    /// Drives `write_batches` through `script` and returns the batches it emitted, along with a
    /// handle to the shard so callers can append them and read the result back.
    async fn run_write_batches(
        target: CollectionMetadata,
        persist_clients: Arc<PersistClientCache>,
        script: Vec<Step>,
    ) -> Vec<(EmittedBatch, ProtoBatch)> {
        run_worker(move |worker| {
            // `ProtoBatch` is not `Ord`, so the captured stream is summarized on the way out
            // rather than going through `Capture`.
            let emitted = Rc::new(RefCell::new(Vec::new()));

            let (mut descs_input, mut ceilings_input, mut data_input, button) =
                worker.dataflow::<mz_repr::Timestamp, _, _>(|scope| {
                    let (descs_input, descs) = scope.new_input();
                    let (ceilings_input, ceilings) = scope.new_input();
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
                        ceilings,
                        data.as_collection(),
                        persist_clients,
                        source_statistics,
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

                    (descs_input, ceilings_input, data_input, button)
                });

            // The operator waits on persist off the timely scheduler, so a plain `step` can find
            // the worker idle while the operator is still starting up. Parking hands the thread
            // over until its waker fires, which is what lets the operator keep up with the script.
            fn pump(worker: &mut timely::worker::Worker) {
                // no solid reason for this number, it's a selection that seems high enough to
                // work reliably, but not create a ton of delay (~32ms parked).
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
                    Step::Commit(lower, ceiling) => ceilings_input.send(Commitment {
                        lower: ts(lower),
                        ceiling: ts(ceiling),
                    }),
                    Step::Updates(at, count) => {
                        for i in 0..i64::try_from(count).expect("small count") {
                            let row = Row::pack_slice(&[Datum::Int64(i)]);
                            data_input.send((Ok(row), ts(at), Diff::ONE));
                        }
                    }
                    Step::AdvanceTo(t) => {
                        descs_input.advance_to(ts(t));
                        ceilings_input.advance_to(ts(t));
                        data_input.advance_to(ts(t));
                    }
                }
                pump(worker);
            }

            descs_input.close();
            ceilings_input.close();
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
        .await
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

    /// A single `compare_and_append` over `[lower, upper)` carrying every emitted batch.
    fn one_append(
        emitted: Vec<(EmittedBatch, ProtoBatch)>,
        lower: u64,
        upper: u64,
    ) -> Vec<(u64, u64, Vec<ProtoBatch>)> {
        vec![(lower, upper, emitted.into_iter().map(|(_, p)| p).collect())]
    }

    /// One `compare_and_append` per description the batches were written for, ascending by lower.
    fn append_per_description(
        emitted: Vec<(EmittedBatch, ProtoBatch)>,
    ) -> Vec<(u64, u64, Vec<ProtoBatch>)> {
        let mut by_desc: BTreeMap<(u64, u64), Vec<ProtoBatch>> = BTreeMap::new();
        for (batch, proto) in emitted {
            by_desc
                .entry((batch.lower, batch.upper))
                .or_default()
                .push(proto);
        }
        by_desc
            .into_iter()
            .map(|((lower, upper), protos)| (lower, upper, protos))
            .collect()
    }

    /// Applies each entry in `appends` as one `compare_and_append` over `[lower, upper)`, in order,
    /// then reads the shard back as of `as_of` and returns the summed diffs.
    ///
    /// Batches written for different descriptions need separate entries, because persist rejects a
    /// batch whose upper is below the append upper. A `lower` above a batch's own lower registers
    /// it truncated, which is what the sink relies on when a concurrent writer has already claimed
    /// part of the range.
    ///
    /// Part bounds validation is what catches a batch whose parts reach outside their registered
    /// bounds, so the tests append for real rather than stopping at what `write_batches` emitted.
    async fn append_and_read_back(
        target: &CollectionMetadata,
        persist_clients: &PersistClientCache,
        appends: Vec<(u64, u64, Vec<ProtoBatch>)>,
        as_of: u64,
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

        for (lower, upper, protos) in appends {
            let mut batches: Vec<_> = protos
                .into_iter()
                .map(|proto| write.batch_from_transmittable_batch(proto))
                .collect();
            let mut to_append: Vec<_> = batches.iter_mut().collect();
            write
                .compare_and_append_batch(
                    &mut to_append[..],
                    frontier(lower),
                    frontier(upper),
                    true,
                )
                .await
                .expect("invalid usage")
                .expect("upper mismatch");

            assert_eq!(write.fetch_recent_upper().await, &frontier(upper));
        }

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
            .snapshot_and_fetch(frontier(as_of))
            .await
            .expect("since <= as_of");

        contents.iter().map(|(_, _, d)| *d).sum()
    }

    /// Several descriptions can become ready in the same pass. Each is written under its own
    /// bounds, so every batch holds exactly the updates its description covers however that ready
    /// set happens to be ordered.
    ///
    /// NOTE: `in_flight_batches` is a `HashMap`, so the ready set comes out in no particular order.
    /// Enough descriptions are used here that an all-ascending pass is unlikely.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait
    async fn write_batches_handles_descriptions_ready_in_one_pass() {
        const DESCRIPTIONS: u64 = 6;
        const DONE: u64 = DESCRIPTIONS * 2;

        let persist_clients = test_persist_clients();
        let target = test_target();

        // One update inside each of the tiling descriptions [0,2), [2,4), ... None of them is ready
        // until the frontier passes every upper, so they all come due together.
        let mut script = vec![];
        for i in 0..DESCRIPTIONS {
            script.push(Step::Updates(i * 2 + 1, 1));
        }
        for i in 0..DESCRIPTIONS {
            script.push(Step::Description(i * 2, i * 2 + 2));
        }
        script.push(Step::AdvanceTo(DONE));

        let emitted = run_write_batches(target.clone(), Arc::clone(&persist_clients), script).await;

        assert_eq!(
            emitted.len(),
            usize::cast_from(DESCRIPTIONS),
            "one batch per description, got {:?}",
            emitted.iter().map(|(b, _)| b).collect::<Vec<_>>()
        );
        for (batch, _) in &emitted {
            assert!(
                batch.lower <= batch.data_max_ts && batch.data_max_ts < batch.upper,
                "batch {batch:?} holds data outside the description it was written for"
            );
        }

        let total = append_and_read_back(
            &target,
            &persist_clients,
            append_per_description(emitted),
            DONE - 1,
        )
        .await;
        assert_eq!(
            total,
            i64::try_from(DESCRIPTIONS).expect("small"),
            "every update should be readable exactly once"
        );
    }

    /// A description that covers no updates must emit no batch, rather than open a builder that
    /// has no data bounds to register.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait
    async fn write_batches_emits_nothing_for_a_description_with_no_updates() {
        const SPLIT: u64 = 4;
        const DONE: u64 = 8;

        // Two descriptions in hand, with data only in the second.
        let emitted = run_write_batches(
            test_target(),
            test_persist_clients(),
            vec![
                Step::Description(0, SPLIT),
                Step::Description(SPLIT, DONE),
                Step::AdvanceTo(SPLIT),
                Step::Updates(SPLIT, 8),
                Step::AdvanceTo(DONE),
            ],
        )
        .await;

        assert_eq!(
            emitted
                .iter()
                .map(|(b, _)| (b.lower, b.upper))
                .collect::<Vec<_>>(),
            vec![(SPLIT, DONE)],
            "only the description holding data should produce a batch",
        );
    }

    /// A snapshot at time 1 pinning the frontier while replication delivers one update at each of
    /// times 2..=`pinned_times`+1, with the description that covers the whole snapshot arriving
    /// only at the end.
    fn pinned_frontier_script(snapshot_rows: usize, pinned_times: u64, done: u64) -> Vec<Step> {
        let mut script = vec![Step::Updates(1, snapshot_rows)];
        for t in 2..=pinned_times + 1 {
            script.push(Step::Updates(t, 1));
        }
        // The minter holds a capability at the shard upper for the whole snapshot, so its one
        // description is emitted there, and the frontier then jumps past everything staged.
        script.push(Step::Description(0, done));
        script.push(Step::AdvanceTo(done));
        script
    }

    /// A snapshot pins the export's frontier at its as_of while concurrent replication keeps
    /// delivering updates at later times. Each timestamp writes a batch of its own, all finished
    /// under the one description that arrives when the snapshot finishes.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait
    async fn write_batches_writes_one_batch_per_timestamp() {
        const SNAPSHOT_ROWS: usize = 4;
        const PINNED_TIMES: u64 = 16;
        const DONE: u64 = PINNED_TIMES + 2;

        let persist_clients = test_persist_clients();
        let target = test_target();

        let emitted = run_write_batches(
            target.clone(),
            Arc::clone(&persist_clients),
            pinned_frontier_script(SNAPSHOT_ROWS, PINNED_TIMES, DONE),
        )
        .await;

        // Every batch carries the description's bounds, since that is what they are finished
        // under, and holds a single timestamp's updates.
        let expected: Vec<_> = std::iter::once(EmittedBatch {
            lower: 0,
            upper: DONE,
            data_max_ts: 1,
            inserts: u64::cast_from(SNAPSHOT_ROWS),
        })
        .chain((2..=PINNED_TIMES + 1).map(|ts| EmittedBatch {
            lower: 0,
            upper: DONE,
            data_max_ts: ts,
            inserts: 1,
        }))
        .collect();
        assert_eq!(
            emitted.iter().map(|(b, _)| b.clone()).collect::<Vec<_>>(),
            expected,
        );

        let total = append_and_read_back(
            &target,
            &persist_clients,
            one_append(emitted, 0, DONE),
            DONE - 1,
        )
        .await;
        assert_eq!(
            total,
            i64::try_from(SNAPSHOT_ROWS).expect("small")
                + i64::try_from(PINNED_TIMES).expect("small"),
            "the same updates should be readable however they were batched"
        );
    }

    /// The same snapshot with a ceiling committed first, which is what the minter does behind a
    /// frontier that is not moving. The description itself only arrives once the frontier reaches
    /// the ceiling, which is what ends the script.
    fn committed_ceiling_script(snapshot_rows: usize, pinned_times: u64, done: u64) -> Vec<Step> {
        let mut script = vec![
            Step::Commit(0, done),
            Step::AdvanceTo(1),
            Step::Updates(1, snapshot_rows),
        ];
        for t in 2..=pinned_times + 1 {
            script.push(Step::Updates(t, 1));
        }
        script.push(Step::Description(0, done));
        script.push(Step::AdvanceTo(done));
        script
    }

    /// A ceiling committed ahead of the frontier gives arriving updates a bound, so a pinned
    /// frontier writes one batch rather than one per timestamp, and the rows sit in a builder that
    /// fills to the blob target instead of many single-timestamp builders that each stay under it
    /// and hold their rows in memory.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait
    async fn write_batches_routes_updates_below_the_ceiling_into_one_builder() {
        const SNAPSHOT_ROWS: usize = 4;
        const PINNED_TIMES: u64 = 16;
        const DONE: u64 = PINNED_TIMES + 2;

        let persist_clients = test_persist_clients();
        let target = test_target();

        let emitted = run_write_batches(
            target.clone(),
            Arc::clone(&persist_clients),
            committed_ceiling_script(SNAPSHOT_ROWS, PINNED_TIMES, DONE),
        )
        .await;

        assert_eq!(
            emitted.len(),
            1,
            "the whole snapshot should share the one open builder, got {:?}",
            emitted.iter().map(|(b, _)| b).collect::<Vec<_>>()
        );
        assert_eq!(
            emitted[0].0,
            EmittedBatch {
                lower: 0,
                upper: DONE,
                data_max_ts: PINNED_TIMES + 1,
                inserts: u64::cast_from(SNAPSHOT_ROWS) + PINNED_TIMES,
            }
        );

        let total = append_and_read_back(
            &target,
            &persist_clients,
            one_append(emitted, 0, DONE),
            DONE - 1,
        )
        .await;
        assert_eq!(
            total,
            i64::try_from(SNAPSHOT_ROWS).expect("small")
                + i64::try_from(PINNED_TIMES).expect("small"),
            "grouping must not change what the shard ends up holding"
        );
    }
}
