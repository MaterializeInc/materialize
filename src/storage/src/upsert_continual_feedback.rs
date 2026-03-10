// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of feedback UPSERT operator and associated helpers. See
//! [`upsert_inner`] for a description of how the operator works and why.

use std::cmp::Reverse;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::{AsCollection, VecCollection};
use indexmap::map::Entry;
use mz_ore::cast::CastFrom;
use mz_repr::{Diff, GlobalId, Row};
use mz_storage_types::errors::{DataflowError, EnvelopeError};
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use std::convert::Infallible;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Capability, CapabilitySet};
use timely::dataflow::{Scope, StreamVec};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use crate::healthcheck::HealthStatusUpdate;
use crate::metrics::upsert::UpsertMetrics;
use crate::upsert::UpsertConfig;
use crate::upsert::UpsertErrorEmitter;
use crate::upsert::UpsertKey;
use crate::upsert::UpsertValue;
use crate::upsert::types::UpsertValueAndSize;
use crate::upsert::types::{self as upsert_types};
use crate::upsert::types::{UpsertState, UpsertStateBackend};

/// An operator that transforms an input stream of upserts (updates to key-value
/// pairs), which represents an imaginary key-value state, into a differential
/// collection. It keeps an internal map-like state which keeps the latest value
/// for each key, such that it can emit the retractions and additions implied by
/// a new update for a given key.
///
/// This operator is intended to be used in an ingestion pipeline that reads
/// from an external source, and the output of this operator is eventually
/// written to persist.
///
/// The operator has two inputs: a) the source input, of upserts, and b) a
/// persist input that feeds back the upsert state to the operator. Below, there
/// is a section for each input that describes how and why we process updates
/// from each input.
///
/// An important property of this operator is that it does _not_ update the
/// map-like state that it keeps for translating the stream of upserts into a
/// differential collection when it processes source input. It _only_ updates
/// the map-like state based on updates from the persist (feedback) input. We do
/// this because the operator is expected to be used in cases where there are
/// multiple concurrent instances of the same ingestion pipeline, and the
/// different instances might see different input because of concurrency and
/// non-determinism. All instances of the upsert operator must produce output
/// that is consistent with the current state of the output (that all instances
/// produce "collaboratively"). This global state is what the operator
/// continually learns about via updates from the persist input.
///
/// ## Processing the Source Input
///
/// Updates on the source input are stashed/staged until they can be processed.
/// Whether or not an update can be processed depends both on the upper frontier
/// of the source input and on the upper frontier of the persist input:
///
///  - Input updates are only processed once their timestamp is "done", that is
///  the input upper is no longer `less_equal` their timestamp.
///
///  - Input updates are only processed once they are at the persist upper, that
///  is we have emitted and written down updates for all previous times and we
///  have updated our map-like state to the latest global state of the output of
///  the ingestion pipeline. We know this is the case when the persist upper is
///  no longer `less_than` their timestamp.
///
/// ## Processing the Persist Input
///
/// We continually ingest updates from the persist input into our state using
/// `UpsertState::consolidate_chunk`. We might be ingesting updates from the
/// initial snapshot (when starting the operator) that are not consolidated.
/// `consolidate_chunk` is able to handle that.
pub fn upsert_inner<G: Scope, FromTime, F, Fut, US, SF, SFut>(
    input: VecCollection<G, (UpsertKey, Option<UpsertValue>, FromTime), Diff>,
    key_indices: Vec<usize>,
    resume_upper: Antichain<G::Timestamp>,
    persist_input: VecCollection<G, Result<Row, DataflowError>, Diff>,
    mut persist_token: Option<Vec<PressOnDropButton>>,
    upsert_metrics: UpsertMetrics,
    source_config: crate::source::SourceExportCreationConfig,
    state_fn: F,
    stash_fn: SF,
    upsert_config: UpsertConfig,
) -> (
    VecCollection<G, Result<Row, DataflowError>, Diff>,
    StreamVec<G, (Option<GlobalId>, HealthStatusUpdate)>,
    StreamVec<G, Infallible>,
    PressOnDropButton,
)
where
    G::Timestamp: Refines<mz_repr::Timestamp> + TotalOrder + Sync,
    F: FnOnce() -> Fut + 'static,
    Fut: std::future::Future<Output = US>,
    US: UpsertStateBackend<G::Timestamp, FromTime>,
    SF: FnOnce() -> SFut + 'static,
    SFut: std::future::Future<Output = crate::upsert::stash::UpsertStash<G::Timestamp, FromTime>>,
    FromTime: Debug + timely::ExchangeData + Clone + Ord + Sync,
{
    let mut builder = AsyncOperatorBuilder::new("Upsert".to_string(), input.scope());

    // We only care about UpsertValueError since this is the only error that we can retract
    let persist_input = persist_input.flat_map(move |result| {
        let value = match result {
            Ok(ok) => Ok(ok),
            Err(DataflowError::EnvelopeError(err)) => match *err {
                EnvelopeError::Upsert(err) => Err(Box::new(err)),
                _ => return None,
            },
            Err(_) => return None,
        };
        let value_ref = match value {
            Ok(ref row) => Ok(row),
            Err(ref err) => Err(&**err),
        };
        Some((UpsertKey::from_value(value_ref, &key_indices), value))
    });
    let (output_handle, output) = builder.new_output::<CapacityContainerBuilder<_>>();

    // An output that just reports progress of the snapshot consolidation process upstream to the
    // persist source to ensure that backpressure is applied
    let (_snapshot_handle, snapshot_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<Infallible>>>();

    let (mut health_output, health_stream) = builder.new_output();
    let mut input = builder.new_input_for(
        input.inner,
        Exchange::new(move |((key, _, _), _, _)| UpsertKey::hashed(key)),
        &output_handle,
    );

    let mut persist_input = builder.new_disconnected_input(
        persist_input.inner,
        Exchange::new(|((key, _), _, _)| UpsertKey::hashed(key)),
    );

    let upsert_shared_metrics = Arc::clone(&upsert_metrics.shared);

    let shutdown_button = builder.build(move |caps| async move {
        let [output_cap, snapshot_cap, health_cap]: [_; 3] = caps.try_into().unwrap();
        drop(output_cap);
        let mut snapshot_cap = CapabilitySet::from_elem(snapshot_cap);

        let mut state = UpsertState::<_, G::Timestamp, FromTime>::new(
            state_fn().await,
            upsert_shared_metrics,
            &upsert_metrics,
            source_config.source_statistics.clone(),
            upsert_config.shrink_upsert_unused_buffers_by_ratio,
        );

        // True while we're still reading the initial "snapshot" (a whole bunch
        // of updates, all at the same initial timestamp) from our persist
        // input or while we're reading the initial snapshot from the upstream
        // source.
        let mut hydrating = true;

        // A re-usable buffer of changes, per key. This is an `IndexMap`
        // because it has to be `drain`-able and have a consistent iteration
        // order.
        let mut commands_state: indexmap::IndexMap<
            _,
            upsert_types::UpsertValueAndSize<G::Timestamp, FromTime>,
        > = indexmap::IndexMap::new();
        let mut multi_get_scratch = Vec::new();

        // For stashing source input while it's not eligible for processing.
        let mut stash = stash_fn().await;
        // A capability suitable for emitting any updates based on stash. No capability is held
        // when the stash is empty.
        let mut stash_cap: Option<Capability<G::Timestamp>> = None;
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());

        // For our persist/feedback input, both of these.
        let mut persist_stash = vec![];
        let mut persist_upper = Antichain::from_elem(Timestamp::minimum());

        // We keep track of the largest timestamp seen on the persist input so
        // that we can block processing source input while that timestamp is
        // beyond the persist frontier. While ingesting updates of a timestamp,
        // our upsert state is in a consolidating state, and trying to read it
        // at that time would yield a panic.
        //
        // NOTE(aljoscha): You would think that it cannot happen that we even
        // attempt to process source updates while the state is in a
        // consolidating state, because we always wait until the persist
        // frontier "catches up" with the timestamp of the source input. If
        // there is only this here UPSERT operator and no concurrent instances,
        // this is true. But with concurrent instances it can happen that an
        // operator that is faster than us makes it so updates get written to
        // persist. And we would then be ingesting them.
        let mut largest_seen_persist_ts: Option<G::Timestamp> = None;

        // A buffer for our output.
        let mut output_updates = vec![];

        let mut error_emitter = (&mut health_output, &health_cap);

        loop {
            tokio::select! {
                _ = persist_input.ready() => {
                    // Read away as much input as we can.
                    while let Some(persist_event) = persist_input.next_sync() {
                        match persist_event {
                            AsyncEvent::Data(time, data) => {
                                tracing::trace!(
                                    worker_id = %source_config.worker_id,
                                    source_id = %source_config.id,
                                    time=?time,
                                    updates=%data.len(),
                                    "received persist data");

                                persist_stash.extend(data.into_iter().map(
                                    |((key, value), ts, diff)| {
                                        largest_seen_persist_ts =
                                            std::cmp::max(
                                                largest_seen_persist_ts
                                                    .clone(),
                                                Some(ts.clone()),
                                            );
                                        (key, value, ts, diff)
                                    },
                                ));
                            }
                            AsyncEvent::Progress(upper) => {
                                tracing::trace!(
                                    worker_id = %source_config.worker_id,
                                    source_id = %source_config.id,
                                    ?upper,
                                    "received persist progress");
                                persist_upper = upper;
                            }
                        }
                    }

                    let last_rehydration_chunk =
                        hydrating && PartialOrder::less_equal(&resume_upper, &persist_upper);

                    tracing::debug!(
                        worker_id = %source_config.worker_id,
                        source_id = %source_config.id,
                        persist_stash = %persist_stash.len(),
                        %hydrating,
                        %last_rehydration_chunk,
                        ?resume_upper,
                        ?persist_upper,
                        "ingesting persist snapshot chunk");

                    let persist_stash_iter = persist_stash
                        .drain(..)
                        .map(|(key, val, _ts, diff)| (key, val, diff));

                    match state
                        .consolidate_chunk(
                            persist_stash_iter,
                            last_rehydration_chunk,
                        )
                        .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            // Make sure our persist source can shut down.
                            persist_token.take();
                            snapshot_cap.downgrade(&[]);
                            UpsertErrorEmitter::<G>::emit(
                                &mut error_emitter,
                                "Failed to rehydrate state".to_string(),
                                e,
                            )
                            .await;
                        }
                    }

                    tracing::debug!(
                        worker_id = %source_config.worker_id,
                        source_id = %source_config.id,
                        ?resume_upper,
                        ?persist_upper,
                        "downgrading snapshot cap",
                    );

                    // Only downgrade this _after_ ingesting the data, because
                    // that can actually take quite some time, and we don't want
                    // to announce that we're done ingesting the initial
                    // snapshot too early.
                    //
                    // When we finish ingesting our initial persist snapshot,
                    // during "re-hydration", we downgrade this to the empty
                    // frontier, so we need to be lenient to this failing from
                    // then on.
                    let _ = snapshot_cap.try_downgrade(persist_upper.iter());



                    if last_rehydration_chunk {
                        hydrating = false;

                        tracing::info!(
                            worker_id = %source_config.worker_id,
                            source_id = %source_config.id,
                            "upsert source finished rehydration",
                        );

                        snapshot_cap.downgrade(&[]);
                    }

                }
                _ = input.ready() => {
                    while let Some(event) = input.next_sync() {
                        match event {
                            AsyncEvent::Data(cap, mut data) => {
                                tracing::trace!(
                                    worker_id = %source_config.worker_id,
                                    source_id = %source_config.id,
                                    time=?cap.time(),
                                    updates=%data.len(),
                                    "received data");

                                if PartialOrder::less_equal(&input_upper, &resume_upper) {
                                    data.retain(|(_, ts, _)| resume_upper.less_equal(ts));
                                }
                                let batch: Vec<_> = data.drain(..)
                                    .map(|((key, value, order), time, diff)| {
                                        assert!(diff.is_positive(), "invalid upsert input");
                                        (time, key, order, value)
                                    }).collect();
                                let batch_len = batch.len();
                                let stage_start = Instant::now();
                                match stash.stage_batch(batch).await {
                                    Ok(()) => {
                                        upsert_metrics.shared.stash_stage_latency
                                            .observe(stage_start.elapsed().as_secs_f64());
                                        upsert_metrics.stash_stage_size
                                            .inc_by(u64::cast_from(batch_len));
                                    }
                                    Err(e) => {
                                        UpsertErrorEmitter::<G>::emit(
                                            &mut error_emitter,
                                            "Failed to stage input to stash".to_string(),
                                            e,
                                        ).await;
                                    }
                                }
                                if !stash.is_empty() {
                                    // Update the stashed capability to the minimum
                                    stash_cap = match stash_cap {
                                        Some(stash_cap) => {
                                            if cap.time() < stash_cap.time() {
                                                Some(cap)
                                            } else {
                                                Some(stash_cap)
                                            }
                                        }
                                        None => Some(cap)
                                    };
                                }

                            }
                            AsyncEvent::Progress(upper) => {
                                tracing::trace!(
                                    worker_id = %source_config.worker_id,
                                    source_id = %source_config.id,
                                    ?upper,
                                    "received progress");

                                // Ignore progress updates before the `resume_upper`, which is our initial
                                // capability post-snapshotting.
                                if PartialOrder::less_than(&upper, &resume_upper) {
                                    tracing::trace!(
                                        worker_id = %source_config.worker_id,
                                        source_id = %source_config.id,
                                        ?upper,
                                        ?resume_upper,
                                        "ignoring progress updates before resume_upper");
                                    continue;
                                }

                                input_upper = upper;
                            }
                        }

                    }
                }
            };

            // While we have partially ingested updates of a timestamp our state
            // is in an inconsistent/consolidating state and accessing it would
            // panic.
            if let Some(largest_seen_persist_ts) = largest_seen_persist_ts.as_ref() {
                let largest_seen_outer_persist_ts = largest_seen_persist_ts.clone().to_outer();
                let outer_persist_upper = persist_upper.iter().map(|ts| ts.clone().to_outer());
                let outer_persist_upper = Antichain::from_iter(outer_persist_upper);
                if outer_persist_upper.less_equal(&largest_seen_outer_persist_ts) {
                    continue;
                }
            }

            // We try and drain from our stash every time we go through the
            // loop. More of our stash can become eligible for draining both
            // when the source-input frontier advances or when the persist
            // frontier advances.
            if !stash.is_empty() {
                let cap = stash_cap
                    .as_mut()
                    .expect("missing capability for non-empty stash");

                tracing::trace!(
                    worker_id = %source_config.worker_id,
                    source_id = %source_config.id,
                    ?cap,
                    stash_len = stash.len(),
                    "stashed updates");

                let drain_start = Instant::now();
                let eligible_updates = match stash
                    .drain(|ts| !input_upper.less_equal(ts) && !persist_upper.less_than(ts))
                    .await
                {
                    Ok(updates) => {
                        upsert_metrics
                            .shared
                            .stash_drain_latency
                            .observe(drain_start.elapsed().as_secs_f64());
                        upsert_metrics
                            .stash_drain_size
                            .inc_by(u64::cast_from(updates.len()));
                        updates
                    }
                    Err(e) => {
                        UpsertErrorEmitter::<G>::emit(
                            &mut error_emitter,
                            "Failed to drain stash".to_string(),
                            e,
                        )
                        .await;
                        vec![]
                    }
                };

                // Discard entries in the past of persist_upper -- the downstream
                // persist_sink would filter them out anyway.
                let eligible_updates: Vec<_> = eligible_updates
                    .into_iter()
                    .filter(|(ts, _, _, _)| persist_upper.less_equal(ts))
                    .collect();

                drain_staged_input::<_, G, _, _, _>(
                    eligible_updates,
                    &mut commands_state,
                    &mut output_updates,
                    &mut multi_get_scratch,
                    &mut error_emitter,
                    &mut state,
                    &source_config,
                )
                .await;

                tracing::trace!(
                    worker_id = %source_config.worker_id,
                    source_id = %source_config.id,
                    output_updates = %output_updates.len(),
                    "output updates for complete timestamp");

                for (update, ts, diff) in output_updates.drain(..) {
                    output_handle.give(cap, (update, ts, diff));
                }

                if !stash.is_empty() {
                    let min_remaining_time = stash
                        .min_time()
                        .cloned()
                        .expect("non-empty stash must have a min time");
                    cap.downgrade(&min_remaining_time);
                } else {
                    stash_cap = None;
                }
            }

            if input_upper.is_empty() {
                tracing::debug!(
                    worker_id = %source_config.worker_id,
                    source_id = %source_config.id,
                    "input exhausted, shutting down");
                break;
            };
        }
    });

    (
        output
            .as_collection()
            .map(|result: UpsertValue| match result {
                Ok(ok) => Ok(ok),
                Err(err) => Err(DataflowError::from(EnvelopeError::Upsert(*err))),
            }),
        health_stream,
        snapshot_stream,
        shutdown_button.press_on_drop(),
    )
}

/// Helper method for [`upsert_inner`] used to process eligible updates drained
/// from the stash and emit the corresponding output updates.
///
/// The `eligible_updates` are already deduplicated by the stash's merge operator
/// (one entry per `(time, key)` with the largest `from_time`).
async fn drain_staged_input<S, G, T, FromTime, E>(
    eligible_updates: Vec<(T, UpsertKey, Reverse<FromTime>, Option<UpsertValue>)>,
    commands_state: &mut indexmap::IndexMap<UpsertKey, UpsertValueAndSize<T, FromTime>>,
    output_updates: &mut Vec<(UpsertValue, T, Diff)>,
    multi_get_scratch: &mut Vec<UpsertKey>,
    error_emitter: &mut E,
    state: &mut UpsertState<'_, S, T, FromTime>,
    source_config: &crate::source::SourceExportCreationConfig,
) where
    S: UpsertStateBackend<T, FromTime>,
    G: Scope,
    T: TotalOrder + timely::ExchangeData + Clone + Debug + Ord + Sync,
    FromTime: timely::ExchangeData + Clone + Ord + Sync,
    E: UpsertErrorEmitter<G>,
{
    tracing::debug!(
        worker_id = %source_config.worker_id,
        source_id = %source_config.id,
        eligible = eligible_updates.len(),
        "draining stash");

    // Read the previous values _per key_ out of `state`, recording it
    // along with the value with the _latest timestamp for that key_.
    commands_state.clear();
    for (_, key, _, _) in eligible_updates.iter() {
        commands_state.entry(*key).or_default();
    }

    // These iterators iterate in the same order because `commands_state`
    // is an `IndexMap`.
    multi_get_scratch.clear();
    multi_get_scratch.extend(commands_state.iter().map(|(k, _)| *k));
    match state
        .multi_get(multi_get_scratch.drain(..), commands_state.values_mut())
        .await
    {
        Ok(_) => {}
        Err(e) => {
            error_emitter
                .emit("Failed to fetch records from state".to_string(), e)
                .await;
        }
    }

    let bincode_opts = upsert_types::upsert_bincode_opts();
    // Upsert the values into `commands_state`, by recording the latest
    // value (or deletion). These will be synced at the end to the `state`.
    //
    // Note that we are effectively doing "mini-upsert" here, using
    // `command_state`. This "mini-upsert" is seeded with data from `state`, using
    // a single `multi_get` above. This simplifies `UpsertStateBackend`
    // implementations, and reduces the number of reads we need to do.
    //
    // No state writes are performed here -- state is only updated via the
    // persist feedback path.
    for (ts, key, _from_time, value) in eligible_updates {
        let mut command_state = if let Entry::Occupied(command_state) = commands_state.entry(key) {
            command_state
        } else {
            panic!("key missing from commands_state");
        };

        let existing_state_cell = &mut command_state.get_mut().value;

        if let Some(cs) = existing_state_cell.as_mut() {
            cs.ensure_decoded(bincode_opts, source_config.id);
        }

        match value {
            Some(value) => {
                if let Some(old_value) = existing_state_cell.as_ref() {
                    if let Some(old_value) = old_value.finalized_value_ref() {
                        output_updates.push((old_value.clone(), ts.clone(), Diff::MINUS_ONE));
                    }
                }
                output_updates.push((value, ts, Diff::ONE));
            }
            None => {
                if let Some(old_value) = existing_state_cell.as_ref() {
                    if let Some(old_value) = old_value.finalized_value_ref() {
                        output_updates.push((old_value.clone(), ts, Diff::MINUS_ONE));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::mpsc;

    use mz_ore::metrics::MetricsRegistry;
    use mz_persist_types::ShardId;
    use mz_repr::{Datum, Timestamp as MzTimestamp};
    use mz_rocksdb::{RocksDBConfig, ValueIterator};
    use mz_storage_operators::persist_source::Subtime;
    use mz_storage_types::sources::SourceEnvelope;
    use mz_storage_types::sources::envelope::{KeyEnvelope, UpsertEnvelope, UpsertStyle};
    use rocksdb::Env;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, Input, Probe};
    use timely::progress::Timestamp;

    use crate::metrics::StorageMetrics;
    use crate::metrics::upsert::UpsertMetricDefs;
    use crate::source::SourceExportCreationConfig;
    use crate::statistics::{SourceStatistics, SourceStatisticsMetricDefs};
    use crate::upsert::memory::InMemoryHashMap;
    use crate::upsert::types::{
        BincodeOpts, StateValue, consolidating_merge_function, upsert_bincode_opts,
    };

    use super::*;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn gh_9160_repro() {
        // Helper to wrap timestamps in the appropriate types
        let new_ts = |ts| (MzTimestamp::new(ts), Subtime::minimum());

        let stash_dir = tempfile::tempdir().unwrap();
        let stash_path = stash_dir.path().to_path_buf();

        let output_handle = timely::execute_directly(move |worker| {
            let (mut input_handle, mut persist_handle, output_probe, output_handle) =
                worker.dataflow::<MzTimestamp, _, _>(|scope| {
                    // Enter a subscope since the upsert operator expects to work a backpressure
                    // enabled scope.
                    scope.scoped::<(MzTimestamp, Subtime), _, _>("upsert", |scope| {
                        let (input_handle, input) = scope.new_input();
                        let (persist_handle, persist_input) = scope.new_input();
                        let upsert_config = UpsertConfig {
                            shrink_upsert_unused_buffers_by_ratio: 0,
                        };
                        let source_id = GlobalId::User(0);
                        let metrics_registry = MetricsRegistry::new();
                        let upsert_metrics_defs =
                            UpsertMetricDefs::register_with(&metrics_registry);
                        let upsert_metrics =
                            UpsertMetrics::new(&upsert_metrics_defs, source_id, 0, None);

                        let metrics_registry = MetricsRegistry::new();
                        let storage_metrics = StorageMetrics::register_with(&metrics_registry);

                        let metrics_registry = MetricsRegistry::new();
                        let source_statistics_defs =
                            SourceStatisticsMetricDefs::register_with(&metrics_registry);
                        let envelope = SourceEnvelope::Upsert(UpsertEnvelope {
                            source_arity: 2,
                            style: UpsertStyle::Default(KeyEnvelope::Flattened),
                            key_indices: vec![0],
                        });
                        let source_statistics = SourceStatistics::new(
                            source_id,
                            0,
                            &source_statistics_defs,
                            source_id,
                            &ShardId::new(),
                            envelope,
                            Antichain::from_elem(Timestamp::minimum()),
                        );

                        let source_config = SourceExportCreationConfig {
                            id: GlobalId::User(0),
                            worker_id: 0,
                            metrics: storage_metrics,
                            source_statistics,
                        };

                        let stash_shared = Arc::clone(&upsert_metrics.rocksdb_shared);
                        let stash_instance = Arc::clone(&upsert_metrics.rocksdb_instance_metrics);
                        let stash_path = stash_path.clone();
                        let stash_fn = || async move {
                            use crate::upsert::stash::{
                                StashValue, UpsertStash, stash_merge_function,
                            };
                            let env = Env::mem_env().unwrap();
                            let merge_op = Some((
                                "test_stash_merge".to_string(),
                                |k: &[u8], vs: ValueIterator<BincodeOpts, StashValue<i32>>| {
                                    stash_merge_function(k, vs)
                                },
                            ));
                            UpsertStash::new(
                                mz_rocksdb::RocksDBInstance::new(
                                    &stash_path,
                                    mz_rocksdb::InstanceOptions::new(
                                        env,
                                        1,
                                        merge_op,
                                        upsert_bincode_opts(),
                                    ),
                                    RocksDBConfig::new(Default::default(), None),
                                    stash_shared,
                                    stash_instance,
                                )
                                .unwrap(),
                            )
                        };

                        let (output, _, _, button) = upsert_inner(
                            input.as_collection(),
                            vec![0],
                            Antichain::from_elem(Timestamp::minimum()),
                            persist_input.as_collection(),
                            None,
                            upsert_metrics,
                            source_config,
                            || async { InMemoryHashMap::default() },
                            stash_fn,
                            upsert_config,
                        );
                        std::mem::forget(button);

                        let (probe, stream) = output.inner.probe();
                        (input_handle, persist_handle, probe, stream.capture())
                    })
                });

            // We work with a hypothetical schema of (key int, value int).

            // The input will contain records for two keys, 0 and 1.
            let key0 = UpsertKey::from_key(Ok(&Row::pack_slice(&[Datum::Int64(0)])));
            let key1 = UpsertKey::from_key(Ok(&Row::pack_slice(&[Datum::Int64(1)])));

            // We will assume that the kafka topic contains the following messages with their
            // associated reclocked timestamp:
            //  1. {offset=1, key=0, value=0}    @ mz_time = 0
            //  2. {offset=2, key=1, value=NULL} @ mz_time = 2  // <- deletion of unrelated key. Causes the operator
            //                                                  //    to maintain the associated cap to time 2
            //  3. {offset=3, key=0, value=1}    @ mz_time = 3
            //  4. {offset=4, key=0, value=2}    @ mz_time = 3  // <- messages 2 and 3 are reclocked to time 3
            let value1 = Row::pack_slice(&[Datum::Int64(0), Datum::Int64(0)]);
            let value3 = Row::pack_slice(&[Datum::Int64(0), Datum::Int64(1)]);
            let value4 = Row::pack_slice(&[Datum::Int64(0), Datum::Int64(2)]);
            let msg1 = (key0, Some(Ok(value1.clone())), 1);
            let msg2 = (key1, None, 2);
            let msg3 = (key0, Some(Ok(value3)), 3);
            let msg4 = (key0, Some(Ok(value4)), 4);

            // The first message will initialize the upsert state such that key 0 has value 0 and
            // produce an output update to that effect.
            input_handle.send((msg1, new_ts(0), Diff::ONE));
            input_handle.advance_to(new_ts(2));
            while output_probe.less_than(&new_ts(1)) {
                worker.step_or_park(None);
            }

            // We assume this worker succesfully CAAs the update to the shard so we send it back
            // through the persist_input
            persist_handle.send((Ok(value1), new_ts(0), Diff::ONE));
            persist_handle.advance_to(new_ts(1));
            worker.step();

            // Then, messages 2 and 3 are sent as one batch with capability = 2
            input_handle.send_batch(&mut vec![
                (msg2, new_ts(2), Diff::ONE),
                (msg3, new_ts(3), Diff::ONE),
            ]);
            // Advance our capability to 3
            input_handle.advance_to(new_ts(3));
            // Message 4 is sent with capability 3
            input_handle.send_batch(&mut vec![(msg4, new_ts(3), Diff::ONE)]);
            // Advance our capability to 4
            input_handle.advance_to(new_ts(4));
            worker.step();

            // We now assume that another replica raced us and processed msg1 at time 2, which in
            // this test is a no-op so the persist frontier advances to time 3 without new data.
            persist_handle.advance_to(new_ts(3));
            // We now step the worker until the output has advanced past time 3, meaning the
            // operator has processed the stash entries. The RocksDB-backed stash requires
            // async I/O so multiple steps may be needed.
            while output_probe.less_than(&new_ts(4)) {
                worker.step_or_park(None);
            }

            output_handle
        });

        let mut actual_output = output_handle
            .extract()
            .into_iter()
            .flat_map(|(_cap, container)| container)
            .collect();
        differential_dataflow::consolidation::consolidate_updates(&mut actual_output);

        // The expected consolidated output contains only updates for key 0 which has the value 0
        // at timestamp 0 and the value 2 at timestamp 3
        let value1 = Row::pack_slice(&[Datum::Int64(0), Datum::Int64(0)]);
        let value4 = Row::pack_slice(&[Datum::Int64(0), Datum::Int64(2)]);
        let expected_output: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(value1.clone()), new_ts(0), Diff::ONE),
            (Ok(value1), new_ts(3), Diff::MINUS_ONE),
            (Ok(value4), new_ts(3), Diff::ONE),
        ];
        assert_eq!(actual_output, expected_output);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn gh_9540_repro() {
        // Helper to wrap timestamps in the appropriate types
        let mz_ts = |ts| (MzTimestamp::new(ts), Subtime::minimum());
        let (tx, rx) = mpsc::channel::<std::thread::JoinHandle<()>>();

        let rocksdb_dir = tempfile::tempdir().unwrap();
        let stash_dir = tempfile::tempdir().unwrap();
        let stash_path = stash_dir.path().to_path_buf();
        let output_handle = timely::execute_directly(move |worker| {
            let tx = tx.clone();
            let (mut input_handle, mut persist_handle, output_probe, output_handle) =
                worker.dataflow::<MzTimestamp, _, _>(|scope| {
                    // Enter a subscope since the upsert operator expects to work a backpressure
                    // enabled scope.
                    scope.scoped::<(MzTimestamp, Subtime), _, _>("upsert", |scope| {
                        let (input_handle, input) = scope.new_input();
                        let (persist_handle, persist_input) = scope.new_input();
                        let upsert_config = UpsertConfig {
                            shrink_upsert_unused_buffers_by_ratio: 0,
                        };
                        let source_id = GlobalId::User(0);
                        let metrics_registry = MetricsRegistry::new();
                        let upsert_metrics_defs =
                            UpsertMetricDefs::register_with(&metrics_registry);
                        let upsert_metrics =
                            UpsertMetrics::new(&upsert_metrics_defs, source_id, 0, None);
                        let rocksdb_shared_metrics = Arc::clone(&upsert_metrics.rocksdb_shared);
                        let rocksdb_instance_metrics =
                            Arc::clone(&upsert_metrics.rocksdb_instance_metrics);

                        let metrics_registry = MetricsRegistry::new();
                        let storage_metrics = StorageMetrics::register_with(&metrics_registry);

                        let metrics_registry = MetricsRegistry::new();
                        let source_statistics_defs =
                            SourceStatisticsMetricDefs::register_with(&metrics_registry);
                        let envelope = SourceEnvelope::Upsert(UpsertEnvelope {
                            source_arity: 2,
                            style: UpsertStyle::Default(KeyEnvelope::Flattened),
                            key_indices: vec![0],
                        });
                        let source_statistics = SourceStatistics::new(
                            source_id,
                            0,
                            &source_statistics_defs,
                            source_id,
                            &ShardId::new(),
                            envelope,
                            Antichain::from_elem(Timestamp::minimum()),
                        );

                        let source_config = SourceExportCreationConfig {
                            id: GlobalId::User(0),
                            worker_id: 0,
                            metrics: storage_metrics,
                            source_statistics,
                        };

                        // A closure that will initialize and return a configured RocksDB instance
                        let rocksdb_init_fn = move || async move {
                            let merge_operator = Some((
                                "upsert_state_snapshot_merge_v1".to_string(),
                                |a: &[u8],
                                 b: ValueIterator<
                                    BincodeOpts,
                                    StateValue<(MzTimestamp, Subtime), u64>,
                                >| {
                                    consolidating_merge_function::<(MzTimestamp, Subtime), u64>(
                                        a.into(),
                                        b,
                                    )
                                },
                            ));
                            let rocksdb_cleanup_tries = 5;
                            let tuning = RocksDBConfig::new(Default::default(), None);
                            let mut rocksdb_inst = mz_rocksdb::RocksDBInstance::new(
                                rocksdb_dir.path(),
                                mz_rocksdb::InstanceOptions::new(
                                    Env::mem_env().unwrap(),
                                    rocksdb_cleanup_tries,
                                    merge_operator,
                                    // For now, just use the same config as the one used for
                                    // merging snapshots.
                                    upsert_bincode_opts(),
                                ),
                                tuning,
                                rocksdb_shared_metrics,
                                rocksdb_instance_metrics,
                            )
                            .unwrap();

                            let handle = rocksdb_inst.take_core_loop_handle().expect("join handle");
                            tx.send(handle).expect("sent joinhandle");
                            crate::upsert::rocksdb::RocksDB::new(rocksdb_inst)
                        };

                        let stash_shared = Arc::clone(&upsert_metrics.rocksdb_shared);
                        let stash_instance = Arc::clone(&upsert_metrics.rocksdb_instance_metrics);
                        let stash_path = stash_path.clone();
                        let stash_fn = || async move {
                            use crate::upsert::stash::{
                                StashValue, UpsertStash, stash_merge_function,
                            };
                            let env = Env::mem_env().unwrap();
                            let merge_op = Some((
                                "test_stash_merge".to_string(),
                                |k: &[u8], vs: ValueIterator<BincodeOpts, StashValue<u64>>| {
                                    stash_merge_function(k, vs)
                                },
                            ));
                            UpsertStash::new(
                                mz_rocksdb::RocksDBInstance::new(
                                    &stash_path,
                                    mz_rocksdb::InstanceOptions::new(
                                        env,
                                        1,
                                        merge_op,
                                        upsert_bincode_opts(),
                                    ),
                                    RocksDBConfig::new(Default::default(), None),
                                    stash_shared,
                                    stash_instance,
                                )
                                .unwrap(),
                            )
                        };

                        let (output, _, _, button) = upsert_inner(
                            input.as_collection(),
                            vec![0],
                            Antichain::from_elem(Timestamp::minimum()),
                            persist_input.as_collection(),
                            None,
                            upsert_metrics,
                            source_config,
                            rocksdb_init_fn,
                            stash_fn,
                            upsert_config,
                        );
                        std::mem::forget(button);

                        let (probe, stream) = output.inner.probe();
                        (input_handle, persist_handle, probe, stream.capture())
                    })
                });

            // We work with a hypothetical schema of (key int, value int).

            // The input will contain records for one key.
            let key0 = UpsertKey::from_key(Ok(&Row::pack_slice(&[Datum::Int64(0)])));

            // Test scenario: two messages at the same timestamp where the higher
            // offset (from_time) should win via dedup after sorting.
            //  1. {offset=1, key=0, value=0}    @ mz_time = 0
            //  2. {offset=2, key=0, value=NULL} @ mz_time = 1
            //  3. {offset=3, key=0, value=0}    @ mz_time = 2
            //  4. {offset=4, key=0, value=NULL} @ mz_time = 2  // <- msg3 and msg4 at same time
            let value1 = Row::pack_slice(&[Datum::Int64(0), Datum::Int64(0)]);
            let msg1 = ((key0, Some(Ok(value1.clone())), 1), mz_ts(0), Diff::ONE);
            let msg2 = ((key0, None, 2), mz_ts(1), Diff::ONE);
            let msg3 = ((key0, Some(Ok(value1.clone())), 3), mz_ts(2), Diff::ONE);
            let msg4 = ((key0, None, 4), mz_ts(2), Diff::ONE);

            // Process msg1 at time 0.
            input_handle.send(msg1);
            input_handle.advance_to(mz_ts(1));
            while output_probe.less_than(&mz_ts(1)) {
                worker.step_or_park(None);
            }
            // Feedback the produced output..
            persist_handle.send((Ok(value1.clone()), mz_ts(0), Diff::ONE));
            persist_handle.advance_to(mz_ts(1));
            // ..and send the next upsert command that deletes the key.
            input_handle.send(msg2);
            input_handle.advance_to(mz_ts(2));
            while output_probe.less_than(&mz_ts(2)) {
                worker.step_or_park(None);
            }

            // Feedback the produced output..
            persist_handle.send((Ok(value1), mz_ts(1), Diff::MINUS_ONE));
            persist_handle.advance_to(mz_ts(2));

            // Send both msg3 and msg4 at time 2 (out of order: msg4 first).
            // With ToUpper semantics, both are stashed until the frontier
            // advances past time 2. The dedup picks the highest from_time
            // (msg4, the delete), so the net result is a deletion.
            input_handle.send(msg4);
            input_handle.send(msg3);
            input_handle.flush();
            input_handle.advance_to(mz_ts(3));

            output_handle
        });

        let mut actual_output = output_handle
            .extract()
            .into_iter()
            .flat_map(|(_cap, container)| container)
            .collect();
        differential_dataflow::consolidation::consolidate_updates(&mut actual_output);

        // The expected consolidated output: key 0 has value 0 at time 0, deleted at time 1.
        // At time 2, msg4 (delete, offset=4) wins over msg3 (insert, offset=3), so no
        // net change at time 2 (delete of already-deleted key is a no-op).
        let value1 = Row::pack_slice(&[Datum::Int64(0), Datum::Int64(0)]);
        let expected_output: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(value1.clone()), mz_ts(0), Diff::ONE),
            (Ok(value1), mz_ts(1), Diff::MINUS_ONE),
        ];
        assert_eq!(actual_output, expected_output);

        while let Ok(handle) = rx.recv() {
            handle.join().expect("threads completed successfully");
        }
    }
}
