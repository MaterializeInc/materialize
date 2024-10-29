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

use differential_dataflow::consolidation;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::{AsCollection, Collection};
use indexmap::map::Entry;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::vec::VecExt;
use mz_repr::{Diff, Row};
use mz_storage_types::errors::{DataflowError, EnvelopeError, UpsertError};
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use std::convert::Infallible;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Capability, CapabilitySet};
use timely::dataflow::{Scope, Stream};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::{Antichain, Timestamp};

use crate::healthcheck::HealthStatusUpdate;
use crate::metrics::upsert::UpsertMetrics;
use crate::render::sources::OutputIndex;
use crate::upsert::types as upsert_types;
use crate::upsert::types::PutStats;
use crate::upsert::types::UpsertValueAndSize;
use crate::upsert::types::{StateValue, UpsertState, UpsertStateBackend, Value};
use crate::upsert::UpsertConfig;
use crate::upsert::UpsertErrorEmitter;
use crate::upsert::UpsertKey;
use crate::upsert::UpsertValue;

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
/// Processing of persist input happens differently based on whether we are
/// still hydrating or not. The operator is hydrating when it is: a) ingesting
/// the initial source snapshot, or b) ingesting a snapshot of previously
/// written updates. We know we are ingesting the initial source snapshot when
/// the `resume_upper` is `[MIN]`.
///
/// We need these different phases because while reading an initial snapshot,
/// updates are not guaranteed to be physically consolidated, that is there
/// might be multiple updates with varying diffs for a given key. The snapshot
/// is still logically consolidated, that is when summing up all the diffs for a
/// given key, the diff is either `1` (the key-value pair exists) or `0` the
/// (key-value pair doesn't exist).
///
/// While hydrating, we therefore use a different method for ingesting updates
/// into our map-like state, look for
/// [`UpsertState::consolidate_snapshot_chunk`]. After hydration we can use
/// [`UpsertState::multi_put_with_stats`], to ingest updates into our state as
/// they come in.
///
/// Based on whether we are ingesting the initial source snapshot or not, the
/// logic for determining when we are done hydrating is slightly different. It's
/// explained inline in the code, look for `last_snapshot_chunk`, and the long
/// comment above it.
pub fn upsert_inner<G: Scope, FromTime, F, Fut, US>(
    input: &Collection<G, (UpsertKey, Option<UpsertValue>, FromTime), Diff>,
    key_indices: Vec<usize>,
    resume_upper: Antichain<G::Timestamp>,
    persist_input: Collection<G, Result<Row, DataflowError>, Diff>,
    mut persist_token: Option<Vec<PressOnDropButton>>,
    upsert_metrics: UpsertMetrics,
    source_config: crate::source::RawSourceCreationConfig,
    state_fn: F,
    upsert_config: UpsertConfig,
    prevent_snapshot_buffering: bool,
    snapshot_buffering_max: Option<usize>,
) -> (
    Collection<G, Result<Row, DataflowError>, Diff>,
    Stream<G, (OutputIndex, HealthStatusUpdate)>,
    Stream<G, Infallible>,
    PressOnDropButton,
)
where
    G::Timestamp: TotalOrder,
    F: FnOnce() -> Fut + 'static,
    Fut: std::future::Future<Output = US>,
    US: UpsertStateBackend<Option<FromTime>>,
    FromTime: Debug + timely::ExchangeData + Ord,
{
    let mut builder = AsyncOperatorBuilder::new("Upsert".to_string(), input.scope());

    // We only care about UpsertValueError since this is the only error that we can retract
    let persist_input = persist_input.flat_map(move |result| {
        let value = match result {
            Ok(ok) => Ok(ok),
            Err(DataflowError::EnvelopeError(err)) => match *err {
                EnvelopeError::Upsert(err) => Err(err),
                _ => return None,
            },
            Err(_) => return None,
        };
        Some((UpsertKey::from_value(value.as_ref(), &key_indices), value))
    });
    let (output_handle, output) = builder.new_output();

    // An output that just reports progress of the snapshot consolidation process upstream to the
    // persist source to ensure that backpressure is applied
    let (_snapshot_handle, snapshot_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<Infallible>>>();

    let (mut health_output, health_stream) = builder.new_output();
    let mut input = builder.new_input_for(
        &input.inner,
        Exchange::new(move |((key, _, _), _, _)| UpsertKey::hashed(key)),
        &output_handle,
    );

    let mut persist_input = builder.new_disconnected_input(
        &persist_input.inner,
        Exchange::new(|((key, _), _, _)| UpsertKey::hashed(key)),
    );

    let upsert_shared_metrics = Arc::clone(&upsert_metrics.shared);

    let shutdown_button = builder.build(move |caps| async move {
        let [mut output_cap, snapshot_cap, health_cap]: [_; 3] = caps.try_into().unwrap();
        let mut snapshot_cap = CapabilitySet::from_elem(snapshot_cap);

        // The order key of the `UpsertState` is `Option<FromTime>`, which implements `Default`
        // (as required for `consolidate_snapshot_chunk`), with slightly more efficient serialization
        // than a default `Partitioned`.

        let mut state = UpsertState::<_, Option<FromTime>>::new(
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

        // A re-usable buffer of changes, per key. This is an `IndexMap` because it has to be `drain`-able
        // and have a consistent iteration order.
        let mut commands_state: indexmap::IndexMap<_, upsert_types::UpsertValueAndSize<Option<FromTime>>> =
            indexmap::IndexMap::new();
        let mut multi_get_scratch = Vec::new();

        // For our source input, both of these.
        let mut stash = vec![];
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());


        // For our persist/feedback input, both of these.
        let mut persist_stash = vec![];
        let mut persist_upper = Antichain::from_elem(Timestamp::minimum());

        // A buffer for our output.
        let mut output_updates = vec![];

        let mut error_emitter = (&mut health_output, &health_cap);
        let mut legacy_errors_to_correct = vec![];


        loop {
            tokio::select! {
                _ = persist_input.ready() => {
                    // Read away as much input as we can.
                    while let Some(persist_event) = persist_input.next_sync() {
                        match persist_event {
                            AsyncEvent::Data(_cap, data) => {
                                tracing::trace!(
                                    worker_id = %source_config.worker_id,
                                    source_id = %source_config.id,
                                    time=?_cap,
                                    updates=%data.len(),
                                    "received persist data");
                                persist_stash.extend(data.into_iter().map(|((key, value), ts, diff)| {
                                    (key, value, ts, diff)
                                }))
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

                    if hydrating {

                        // Determine if this is the last time we call
                        // consolidate_snapshot_chunk, and update our
                        // `hydrating` state.
                        //
                        // There are two situations in which we're reading a
                        // snapshot:
                        //
                        // 1. Reading the initial snapshot from the source: in
                        //    this case there is no state in our output yet that
                        //    we could read back in and our resume_upper is [0].
                        //    We know we're done once the persist_upper is
                        //    "past" that.
                        // 2. Re-hydrating our state from persist: some instance
                        //    of the source has already read/ingested/written
                        //    down the initial snapshot from the source. We are
                        //    reading a snapshot of our own upsert state back in
                        //    from persist. In this case we read the snapshot at
                        //    a timestamp that is "before" the resume upper, and
                        //    we know that we're done when the persist_upper is
                        //    "at or past" the resume_upper.
                        //
                        // It's unfortunate that we have to differentiate
                        // between these cases, both here and in code below. The
                        // root of the problem is that when reading the initial
                        // source snapshot, the `resume_upper` is `[0]`, but
                        // we're also reading in those updates at time `0`. When
                        // starting the operator at any other `resume_upper`,
                        // updates are read at a timestamp before that.
                        let last_snapshot_chunk =  if resume_upper.less_equal(&G::Timestamp::minimum()) {
                            PartialOrder::less_than(&resume_upper, &persist_upper)
                        } else {
                            PartialOrder::less_equal(&resume_upper, &persist_upper)
                        };

                        tracing::debug!(
                            worker_id = %source_config.worker_id,
                            source_id = %source_config.id,
                            persist_stash = %persist_stash.len(),
                            %last_snapshot_chunk,
                            ?resume_upper,
                            ?persist_upper,
                            "ingesting persist snapshot chunk");

                        hydrating = !last_snapshot_chunk;

                        let ready_updates = persist_stash.drain_filter_swapping(|(_key, value, ts, diff)| {
                            let first_source_snapshot = ts == &G::Timestamp::minimum();
                            let rehydration_snapshot = !resume_upper.less_equal(ts);
                            let ready = first_source_snapshot || rehydration_snapshot;

                            // Also collect any legacy errors as they're flying
                            // by.
                            if ready {
                                if let Err(UpsertError::Value(ref mut err)) = value {
                                    // If we receive a legacy error in the snapshot we will keep a note of it but
                                    // insert a non-legacy error in our state. This is so that if this error is
                                    // ever retracted we will correctly retract the non-legacy version because by
                                    // that time we will have emitted the error correction, which happens before
                                    // processing any of the new source input.
                                    if err.is_legacy_dont_touch_it {
                                        legacy_errors_to_correct.push((err.clone(), diff.clone()));
                                        err.is_legacy_dont_touch_it = false;
                                    }
                                }
                            }
                            ready
                        });

                        // We have to collect into a vec because
                        // consolidate_snapshot_chunk needs a ExactSizeIterator
                        // iterator. And I (aljoscha) don't want to go down the
                        // rabbit hole of changing that right now.
                        let ready_updates = ready_updates.map(|(key, val, _ts, diff)| (key, val, diff)).collect_vec();

                        match state
                            .consolidate_snapshot_chunk(
                                ready_updates.into_iter(),
                                last_snapshot_chunk,
                            )
                            .await
                        {
                            Ok(_) => {
                                snapshot_cap.downgrade(persist_upper.iter());
                            }
                            Err(e) => {
                                // Make sure our persist source can shut down.
                                persist_token.take();
                                UpsertErrorEmitter::<G>::emit(
                                    &mut error_emitter,
                                    "Failed to rehydrate state".to_string(),
                                    e,
                                )
                                .await;
                            }
                        }


                        if last_snapshot_chunk {
                            // Now it's time to emit the error corrections. It doesn't matter at what timestamp we emit
                            // them at because all they do is change the representation. The error count at any
                            // timestamp remains constant.
                            upsert_metrics
                                .legacy_value_errors
                                .set(u64::cast_from(legacy_errors_to_correct.len()));
                            if !legacy_errors_to_correct.is_empty() {
                                tracing::error!(
                                    "unexpected legacy error representation. Found {} occurences",
                                    legacy_errors_to_correct.len()
                                );
                            }
                            consolidation::consolidate(&mut legacy_errors_to_correct);
                            for (mut error, diff) in legacy_errors_to_correct.drain(..) {
                                assert!(
                                    error.is_legacy_dont_touch_it,
                                    "attempted to correct non-legacy error"
                                );
                                tracing::info!(
                                    worker_id = %source_config.worker_id,
                                    source_id = %source_config.id,
                                    "correcting legacy error {error:?} with diff {diff}");
                                let time = output_cap.time().clone();
                                let retraction = Err(UpsertError::Value(error.clone()));
                                error.is_legacy_dont_touch_it = false;
                                let insertion = Err(UpsertError::Value(error));
                                output_handle.give(&output_cap, (retraction, time.clone(), -diff));
                                output_handle.give(&output_cap, (insertion, time, diff));
                            }

                            tracing::info!(
                                worker_id = %source_config.worker_id,
                                source_id = %source_config.id,
                                "upsert source finished rehydration",
                            );

                            snapshot_cap.downgrade(&[]);
                        }

                    }

                    // We don't have an else here, because we might notice we're
                    // done snapshotting but already have some more persist
                    // updates staged. We have to work them off eagerly here.
                    if !hydrating {
                        tracing::debug!(
                            worker_id = %source_config.worker_id,
                            source_id = %source_config.id,
                            persist_stash = %persist_stash.len(),
                            ?persist_upper,
                            "ingesting state updates from persist");

                        ingest_state_updates::<_, G, _, _, _>(
                            &mut persist_stash,
                            &persist_upper,
                            &mut error_emitter,
                            &mut state,
                            &source_config,
                            ).await;
                    }
                }
                _ = input.ready() => {
                    let mut events_processed = 0;
                    while let Some(event) = input.next_sync() {
                        match event {
                            AsyncEvent::Data(cap, mut data) => {
                                tracing::trace!(
                                    worker_id = %source_config.worker_id,
                                    source_id = %source_config.id,
                                    time=?cap.time(),
                                    updates=%data.len(),
                                    "received data");

                                let event_time = cap.time().clone();

                                stage_input(
                                    &mut stash,
                                    cap,
                                    &mut data,
                                    &input_upper,
                                    &resume_upper,
                                    upsert_config.shrink_upsert_unused_buffers_by_ratio,
                                );

                                // For the very first snapshot from the source,
                                // we allow emitting partial updates without yet
                                // seeing all updates for that timestamp. For
                                // this, we use an ephemeral upsert state to run
                                // our upserting logic that we throw away once
                                // we receive the first updates on the persist
                                // input.
                                //
                                // We can only do this for the very first
                                // snapshot, because all subsequent emitted
                                // updates need to have a consistent view of
                                // upsert state as ingested from the persist
                                // input.
                                //
                                // This is a load-bearing optimization, as it is
                                // required to avoid buffering the entire source
                                // snapshot in the `stash`.
                                if prevent_snapshot_buffering && resume_upper.less_equal(&G::Timestamp::minimum()) && event_time == G::Timestamp::minimum() {
                                    tracing::debug!(
                                        worker_id = %source_config.worker_id,
                                        source_id = %source_config.id,
                                        ?event_time,
                                        ?resume_upper,
                                        ?output_cap, "partial drain not implemented, yet");
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

                                if let Some(ts) = upper.as_option() {
                                    tracing::trace!(
                                        worker_id = %source_config.worker_id,
                                        source_id = %source_config.id,
                                        ?ts,
                                        "downgrading output capability");
                                    let _ = output_cap.try_downgrade(ts);
                                }
                                input_upper = upper;
                            }
                        }

                        events_processed += 1;
                        if let Some(max) = snapshot_buffering_max {
                            if events_processed >= max {
                                break;
                            }
                        }
                    }
                }
            };

            // We try and drain from our stash every time we go through the
            // loop. More of our stash can become eligible for draining both
            // when the source-input frontier advances or when the persist
            // frontier advances.

            drain_staged_input::<_, G, _, _, _>(
                &mut stash,
                &mut commands_state,
                &mut output_updates,
                &mut multi_get_scratch,
                DrainStyle::ToUpper{input_upper: &input_upper, persist_upper: &persist_upper},
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

            for (update, cap, diff) in output_updates.drain(..) {
                let ts = cap.time().clone();
                output_handle.give(&cap, (update, ts, diff));
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
        output.as_collection().map(|result| match result {
            Ok(ok) => Ok(ok),
            Err(err) => Err(DataflowError::from(EnvelopeError::Upsert(err))),
        }),
        health_stream,
        snapshot_stream,
        shutdown_button.press_on_drop(),
    )
}

/// Helper method for `upsert_inner` used to stage `data` updates
/// from the input timely edge.
fn stage_input<T, FromTime>(
    stash: &mut Vec<(
        Capability<T>,
        UpsertKey,
        Reverse<FromTime>,
        Option<UpsertValue>,
    )>,
    cap: Capability<T>,
    data: &mut Vec<((UpsertKey, Option<UpsertValue>, FromTime), T, Diff)>,
    input_upper: &Antichain<T>,
    resume_upper: &Antichain<T>,
    storage_shrink_upsert_unused_buffers_by_ratio: usize,
) where
    T: PartialOrder + timely::progress::Timestamp,
    FromTime: Ord,
{
    if PartialOrder::less_equal(input_upper, resume_upper) {
        data.retain(|(_, ts, _)| resume_upper.less_equal(ts));
    }

    stash.extend(data.drain(..).map(|((key, value, order), time, diff)| {
        assert!(diff > 0, "invalid upsert input");
        // TODO: Don't retain a cap per update but instead bunch them up.
        let cap = cap.delayed(&time);
        (cap, key, Reverse(order), value)
    }));

    if storage_shrink_upsert_unused_buffers_by_ratio > 0 {
        let reduced_capacity = stash.capacity() / storage_shrink_upsert_unused_buffers_by_ratio;
        if reduced_capacity > stash.len() {
            stash.shrink_to(reduced_capacity);
        }
    }
}

/// The style of drain we are performing on the stash. `AtTime`-drains cannot
/// assume that all values have been seen, and must leave tombstones behind for deleted values.
#[derive(Debug)]
enum DrainStyle<'a, T> {
    ToUpper {
        input_upper: &'a Antichain<T>,
        persist_upper: &'a Antichain<T>,
    },
    // TODO: For partial draining when taking the source snapshot.
    #[allow(unused)]
    AtTime(T),
}

/// Helper method for `upsert_inner` used to stage `data` updates
/// from the input timely edge.
async fn drain_staged_input<S, G, T, FromTime, E>(
    stash: &mut Vec<(
        Capability<T>,
        UpsertKey,
        Reverse<FromTime>,
        Option<UpsertValue>,
    )>,
    commands_state: &mut indexmap::IndexMap<UpsertKey, UpsertValueAndSize<Option<FromTime>>>,
    output_updates: &mut Vec<(Result<Row, UpsertError>, Capability<T>, Diff)>,
    multi_get_scratch: &mut Vec<UpsertKey>,
    drain_style: DrainStyle<'_, T>,
    error_emitter: &mut E,
    state: &mut UpsertState<'_, S, Option<FromTime>>,
    source_config: &crate::source::RawSourceCreationConfig,
) where
    S: UpsertStateBackend<Option<FromTime>>,
    G: Scope,
    T: PartialOrder + Ord + Clone + Debug + timely::progress::Timestamp,
    FromTime: timely::ExchangeData + Ord,
    E: UpsertErrorEmitter<G>,
{
    // Sort by (key, time, Reverse(from_time)) so that deduping by (key, time) gives
    // the latest change for that key.
    stash.sort_unstable_by(|a, b| {
        let (ts1, key1, from_ts1, val1) = a;
        let (ts2, key2, from_ts2, val2) = b;
        Ord::cmp(
            &(ts1.time(), key1, from_ts1, val1),
            &(ts2.time(), key2, from_ts2, val2),
        )
    });

    // Find the prefix that we can emit
    let idx = stash.partition_point(|(ts, _, _, _)| match &drain_style {
        DrainStyle::ToUpper {
            input_upper,
            persist_upper,
        } => {
            // We make sure that a) we only process updates when we know their
            // timestamp is complete, that is there will be no more updates for
            // that timestamp, and b) that "previous" times in the persist
            // output are complete. The latter makes sure that we emit updates
            // for the next timestamp that are consistent with the global state
            // in the output persist shard, which also serves as a persistent
            // copy of our in-memory/on-disk upsert state.
            !input_upper.less_equal(ts.time()) && !persist_upper.less_than(ts.time())
        }
        DrainStyle::AtTime(time) => ts.time() <= time,
    });

    tracing::debug!(
        worker_id = %source_config.worker_id,
        source_id = %source_config.id,
        ?drain_style,
        updates = idx,
        "draining stash");

    // Read the previous values _per key_ out of `state`, recording it
    // along with the value with the _latest timestamp for that key_.
    commands_state.clear();
    for (_, key, _, _) in stash.iter().take(idx) {
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

    // From the prefix that can be emitted we can deduplicate based on (ts, key) in
    // order to only process the command with the maximum order within the (ts,
    // key) group. This is achieved by wrapping order in `Reverse(FromTime)` above.;
    let mut commands = stash.drain(..idx).dedup_by(|a, b| {
        let ((a_ts, a_key, _, _), (b_ts, b_key, _, _)) = (a, b);
        a_ts == b_ts && a_key == b_key
    });

    let bincode_opts = upsert_types::upsert_bincode_opts();
    // Upsert the values into `commands_state`, by recording the latest
    // value (or deletion). These will be synced at the end to the `state`.
    //
    // Note that we are effectively doing "mini-upsert" here, using
    // `command_state`. This "mini-upsert" is seeded with data from `state`, using
    // a single `multi_get` above, and the final state is written out into
    // `state` using a single `multi_put`. This simplifies `UpsertStateBackend`
    // implementations, and reduces the number of reads and write we need to do.
    //
    // This "mini-upsert" technique is actually useful in `UpsertState`'s
    // `consolidate_snapshot_read_write_inner` implementation, minimizing gets and puts on
    // the `UpsertStateBackend` implementations. In some sense, its "upsert all the way down".
    while let Some((ts, key, from_time, value)) = commands.next() {
        let mut command_state = if let Entry::Occupied(command_state) = commands_state.entry(key) {
            command_state
        } else {
            panic!("key missing from commands_state");
        };

        let existing_value = &mut command_state.get_mut().value;

        if let Some(cs) = existing_value.as_mut() {
            cs.ensure_decoded(bincode_opts);
        }

        // Skip this command if its order key is below the one in the upsert state.
        // Note that the existing order key may be `None` if the existing value
        // is from snapshotting, which always sorts below new values/deletes.
        let existing_order = existing_value.as_ref().and_then(|cs| cs.order().as_ref());
        if existing_order >= Some(&from_time.0) {
            // Skip this update. If no later updates adjust this key, then we just
            // end up writing the same value back to state. If there
            // is nothing in the state, `existing_order` is `None`, and this
            // does not occur.
            continue;
        }

        match value {
            Some(value) => {
                if let Some(old_value) = existing_value
                    .replace(StateValue::value(value.clone(), Some(from_time.0.clone())))
                {
                    if let Value::Value(old_value, _) = old_value.into_decoded() {
                        output_updates.push((old_value, ts.clone(), -1));
                    }
                }
                output_updates.push((value, ts, 1));
            }
            None => {
                if let Some(old_value) = existing_value.take() {
                    if let Value::Value(old_value, _) = old_value.into_decoded() {
                        output_updates.push((old_value, ts, -1));
                    }
                }

                // Record a tombstone for deletes.
                *existing_value = Some(StateValue::tombstone(Some(from_time.0.clone())));
            }
        }
    }
}

/// Helper method for `upsert_inner` used to ingest state updates from the
/// persist input.
async fn ingest_state_updates<S, G, T, FromTime, E>(
    updates: &mut Vec<(UpsertKey, UpsertValue, T, Diff)>,
    persist_upper: &Antichain<T>,
    error_emitter: &mut E,
    state: &mut UpsertState<'_, S, Option<FromTime>>,
    source_config: &crate::source::RawSourceCreationConfig,
) where
    S: UpsertStateBackend<Option<FromTime>>,
    G: Scope,
    T: PartialOrder + Ord + Clone + Debug + timely::progress::Timestamp,
    FromTime: timely::ExchangeData + Ord,
    E: UpsertErrorEmitter<G>,
{
    // Sort by (key, diff) and make sure additions sort before retractions,
    // which allows us to de-duplicate below, and only keep the latest addition,
    // if any.
    updates.sort_unstable_by(|a, b| {
        let (key1, _val1, ts1, diff1) = a;
        let (key2, _val2, ts2, diff2) = b;
        Ord::cmp(&(ts1, key1, Reverse(diff1)), &(ts2, key2, Reverse(diff2)))
    });

    // Find the prefix that we can ingest.
    let idx = updates.partition_point(|(_, _, ts, _)| !persist_upper.less_equal(ts));

    tracing::debug!(
        worker_id = %source_config.worker_id,
        source_id = %source_config.id,
        ?persist_upper,
        updates = idx,
        "ingesting state updates");

    let mut precomputed_putstats = PutStats::default();

    // It's not ideal that we iterate once before ingesting, but we're doing it
    // to precalculate our stats. We might want to rework how we keep stats in
    // the future.
    for (key, value, ts, diff) in &updates[0..idx] {
        match diff {
            1 => {
                let value = StateValue::value(value.clone(), None::<FromTime>);
                let size: i64 = value.memory_size().try_into().expect("less than i64 size");
                precomputed_putstats.size_diff += size;
                precomputed_putstats.values_diff += 1;
            }
            -1 => {
                let value = StateValue::value(value.clone(), None::<FromTime>);
                let size: i64 = value.memory_size().try_into().expect("less than i64 size");
                precomputed_putstats.size_diff -= size;
                precomputed_putstats.values_diff -= 1;
            }
            invalid_diff => {
                panic!(
                    "unexpected diff for update to upsert state: {:?}",
                    (key, value, ts, invalid_diff)
                );
            }
        }
    }

    let eligible_commands = updates.drain(..idx).dedup_by(|a, b| {
        let ((a_key, _, a_ts, _), (b_key, _, b_ts, _)) = (a, b);
        a_ts == b_ts && a_key == b_key
    });

    let commands = eligible_commands
        .map(|(key, value, ts, diff)| match diff {
            1 => {
                let value = StateValue::value(value, None::<FromTime>);
                (
                    key,
                    upsert_types::PutValue {
                        value: Some(value),
                        previous_value_metadata: None,
                    },
                )
            }
            -1 => (
                key.clone(),
                upsert_types::PutValue {
                    value: None,
                    previous_value_metadata: None,
                },
            ),
            invalid_diff => {
                panic!(
                    "unexpected diff for update to upsert state: {:?}",
                    (key, value, ts, invalid_diff)
                );
            }
        })
        .collect_vec();

    match state
        .multi_put_with_stats(commands, precomputed_putstats)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            error_emitter
                .emit("Failed to update records in state".to_string(), e)
                .await;
        }
    }
}
