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

use differential_dataflow::hashable::Hashable;
use differential_dataflow::{AsCollection, Collection};
use indexmap::map::Entry;
use itertools::Itertools;
use mz_ore::vec::VecExt;
use mz_repr::{Diff, GlobalId, Row};
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
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use crate::healthcheck::HealthStatusUpdate;
use crate::metrics::upsert::UpsertMetrics;
use crate::upsert::UpsertConfig;
use crate::upsert::UpsertErrorEmitter;
use crate::upsert::UpsertKey;
use crate::upsert::UpsertValue;
use crate::upsert::types::UpsertValueAndSize;
use crate::upsert::types::{self as upsert_types, ValueMetadata};
use crate::upsert::types::{StateValue, UpsertState, UpsertStateBackend};

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
/// As an optimization, we allow processing input updates when they are right at
/// the input frontier. This is called _partial emission_ because we are
/// emitting updates that might be retracted when processing more updates from
/// the same timestamp. In order to be able to process these updates we keep
/// _provisional values_ in our upsert state. These will be overwritten when we
/// get the final upsert values on the persist input.
///
/// ## Processing the Persist Input
///
/// We continually ingest updates from the persist input into our state using
/// `UpsertState::consolidate_chunk`. We might be ingesting updates from the
/// initial snapshot (when starting the operator) that are not consolidated or
/// we might be ingesting updates from a partial emission (see above). In either
/// case, our input might not be consolidated and `consolidate_chunk` is able to
/// handle that.
pub fn upsert_inner<G: Scope, FromTime, F, Fut, US>(
    input: &Collection<G, (UpsertKey, Option<UpsertValue>, FromTime), Diff>,
    key_indices: Vec<usize>,
    resume_upper: Antichain<G::Timestamp>,
    persist_input: Collection<G, Result<Row, DataflowError>, Diff>,
    mut persist_token: Option<Vec<PressOnDropButton>>,
    upsert_metrics: UpsertMetrics,
    source_config: crate::source::SourceExportCreationConfig,
    state_fn: F,
    upsert_config: UpsertConfig,
    prevent_snapshot_buffering: bool,
    snapshot_buffering_max: Option<usize>,
) -> (
    Collection<G, Result<Row, DataflowError>, Diff>,
    Stream<G, (Option<GlobalId>, HealthStatusUpdate)>,
    Stream<G, Infallible>,
    PressOnDropButton,
)
where
    G::Timestamp: Refines<mz_repr::Timestamp> + TotalOrder + Sync,
    F: FnOnce() -> Fut + 'static,
    Fut: std::future::Future<Output = US>,
    US: UpsertStateBackend<G::Timestamp, FromTime>,
    FromTime: Debug + timely::ExchangeData + Ord + Sync,
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

        // A re-usable buffer of changes, per key. This is an `IndexMap` because it has to be `drain`-able
        // and have a consistent iteration order.
        let mut commands_state: indexmap::IndexMap<_, upsert_types::UpsertValueAndSize<G::Timestamp, FromTime>> =
            indexmap::IndexMap::new();
        let mut multi_get_scratch = Vec::new();

        // For stashing source input while it's not eligible for processing.
        let mut stash = vec![];
        // A capability suitable for emitting any updates based on stash. No capability is held
        // when the stash is empty.
        let mut stash_cap: Option<Capability<G::Timestamp>> = None;
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());
        let mut partial_drain_time = None;

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

                                persist_stash.extend(data.into_iter().map(|((key, value), ts, diff)| {
                                    largest_seen_persist_ts = std::cmp::max(largest_seen_persist_ts.clone(), Some(ts.clone()));
                                    (key, value, ts, diff)
                                }));
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
                                    &mut data,
                                    &input_upper,
                                    &resume_upper,
                                );
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

                                if prevent_snapshot_buffering && input_upper.as_option() == Some(&event_time) {
                                    tracing::debug!(
                                        worker_id = %source_config.worker_id,
                                        source_id = %source_config.id,
                                        ?event_time,
                                        ?resume_upper,
                                        ?input_upper,
                                        "allowing partial drain");
                                    partial_drain_time = Some(event_time.clone());
                                } else {
                                    tracing::debug!(
                                        worker_id = %source_config.worker_id,
                                        source_id = %source_config.id,
                                        %prevent_snapshot_buffering,
                                        ?event_time,
                                        ?resume_upper,
                                        ?input_upper,
                                        "not allowing partial drain");
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

                                // Disable partial drain, because this progress
                                // update has moved the frontier. We might allow
                                // it again once we receive data right at the
                                // frontier again.
                                partial_drain_time = None;
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
                let cap = stash_cap.as_mut().expect("missing capability for non-empty stash");

                tracing::trace!(
                    worker_id = %source_config.worker_id,
                    source_id = %source_config.id,
                    ?cap,
                    ?stash,
                    "stashed updates");

                let mut min_remaining_time = drain_staged_input::<_, G, _, _, _>(
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

                for (update, ts, diff) in output_updates.drain(..) {
                    output_handle.give(cap, (update, ts, diff));
                }

                if !stash.is_empty() {
                    let min_remaining_time = min_remaining_time.take().expect("we still have updates left");
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

            // If there were staged events that occurred at the capability time, drain
            // them. This is safe because out-of-order updates to the same key that are
            // drained in separate calls to `drain_staged_input` are correctly ordered by
            // their `FromTime` in `drain_staged_input`.
            //
            // Note also that this may result in more updates in the output collection than
            // the minimum. However, because the frontier only advances on `Progress` updates,
            // the collection always accumulates correctly for all keys.
            if let Some(partial_drain_time) = &partial_drain_time {
                if !stash.is_empty() {
                    let cap = stash_cap.as_mut().expect("missing capability for non-empty stash");

                    tracing::trace!(
                        worker_id = %source_config.worker_id,
                        source_id = %source_config.id,
                        ?cap,
                        ?stash,
                        "stashed updates");

                    let mut min_remaining_time = drain_staged_input::<_, G, _, _, _>(
                        &mut stash,
                        &mut commands_state,
                        &mut output_updates,
                        &mut multi_get_scratch,
                        DrainStyle::AtTime{
                            time: partial_drain_time.clone(),
                            persist_upper: &persist_upper
                        },
                        &mut error_emitter,
                        &mut state,
                        &source_config,
                    )
                    .await;

                    tracing::trace!(
                        worker_id = %source_config.worker_id,
                        source_id = %source_config.id,
                        output_updates = %output_updates.len(),
                        "output updates for partial timestamp");

                    for (update, ts, diff) in output_updates.drain(..) {
                        output_handle.give(cap, (update, ts, diff));
                    }

                    if !stash.is_empty() {
                        let min_remaining_time = min_remaining_time.take().expect("we still have updates left");
                        cap.downgrade(&min_remaining_time);
                    } else {
                        stash_cap = None;
                    }
                }
            }
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

/// Helper method for [`upsert_inner`] used to stage `data` updates
/// from the input/source timely edge.
#[allow(clippy::disallowed_types)]
fn stage_input<T, FromTime>(
    stash: &mut Vec<(T, UpsertKey, Reverse<FromTime>, Option<UpsertValue>)>,
    data: &mut Vec<((UpsertKey, Option<UpsertValue>, FromTime), T, Diff)>,
    input_upper: &Antichain<T>,
    resume_upper: &Antichain<T>,
) where
    T: PartialOrder + timely::progress::Timestamp,
    FromTime: Ord,
{
    if PartialOrder::less_equal(input_upper, resume_upper) {
        data.retain(|(_, ts, _)| resume_upper.less_equal(ts));
    }

    stash.extend(data.drain(..).map(|((key, value, order), time, diff)| {
        assert!(diff.is_positive(), "invalid upsert input");
        (time, key, Reverse(order), value)
    }));
}

/// The style of drain we are performing on the stash. `AtTime`-drains cannot
/// assume that all values have been seen, and must leave tombstones behind for deleted values.
#[derive(Debug)]
enum DrainStyle<'a, T> {
    ToUpper {
        input_upper: &'a Antichain<T>,
        persist_upper: &'a Antichain<T>,
    },
    // For partial draining when taking the source snapshot.
    AtTime {
        time: T,
        persist_upper: &'a Antichain<T>,
    },
}

/// Helper method for [`upsert_inner`] used to stage `data` updates
/// from the input timely edge.
///
/// Returns the minimum observed time across the updates that remain in the
/// stash or `None` if none are left.
///
/// ## Correctness
///
/// It is safe to call this function multiple times with the same `persist_upper` provided that the
/// drain style is `AtTime`, which updates the state such that past actions are remembered and can
/// be undone in subsequent calls.
///
/// It is *not* safe to call this function more than once with the same `persist_upper` and a
/// `ToUpper` drain style. Doing so causes all calls except the first one to base their work on
/// stale state, since in this drain style no modifications to the state are made.
async fn drain_staged_input<S, G, T, FromTime, E>(
    stash: &mut Vec<(T, UpsertKey, Reverse<FromTime>, Option<UpsertValue>)>,
    commands_state: &mut indexmap::IndexMap<UpsertKey, UpsertValueAndSize<T, FromTime>>,
    output_updates: &mut Vec<(Result<Row, UpsertError>, T, Diff)>,
    multi_get_scratch: &mut Vec<UpsertKey>,
    drain_style: DrainStyle<'_, T>,
    error_emitter: &mut E,
    state: &mut UpsertState<'_, S, T, FromTime>,
    source_config: &crate::source::SourceExportCreationConfig,
) -> Option<T>
where
    S: UpsertStateBackend<T, FromTime>,
    G: Scope,
    T: TotalOrder + timely::ExchangeData + Debug + Ord + Sync,
    FromTime: timely::ExchangeData + Ord + Sync,
    E: UpsertErrorEmitter<G>,
{
    let mut min_remaining_time = Antichain::new();

    let mut eligible_updates = stash
        .drain_filter_swapping(|(ts, _, _, _)| {
            let eligible = match &drain_style {
                DrainStyle::ToUpper {
                    input_upper,
                    persist_upper,
                } => {
                    // We make sure that a) we only process updates when we know their
                    // timestamp is complete, that is there will be no more updates for
                    // that timestamp, and b) that "previous" times in the persist
                    // input are complete. The latter makes sure that we emit updates
                    // for the next timestamp that are consistent with the global state
                    // in the output persist shard, which also serves as a persistent
                    // copy of our in-memory/on-disk upsert state.
                    !input_upper.less_equal(ts) && !persist_upper.less_than(ts)
                }
                DrainStyle::AtTime {
                    time,
                    persist_upper,
                } => {
                    // Even when emitting partial updates, we still need to wait
                    // until "previous" times in the persist input are complete.
                    *ts <= *time && !persist_upper.less_than(ts)
                }
            };

            if !eligible {
                min_remaining_time.insert(ts.clone());
            }

            eligible
        })
        .filter(|(ts, _, _, _)| {
            let persist_upper = match &drain_style {
                DrainStyle::ToUpper {
                    input_upper: _,
                    persist_upper,
                } => persist_upper,
                DrainStyle::AtTime {
                    time: _,
                    persist_upper,
                } => persist_upper,
            };

            // Any update that is "in the past" of the persist upper is not
            // relevant anymore. We _can_ emit changes for it, but the
            // downstream persist_sink would filter these updates out because
            // the shard upper is already further ahead.
            //
            // Plus, our upsert state is up-to-date to the persist_upper, so we
            // wouldn't be able to emit correct retractions for incoming
            // commands whose `ts` is in the past of that.
            let relevant = persist_upper.less_equal(ts);
            relevant
        })
        .collect_vec();

    tracing::debug!(
        worker_id = %source_config.worker_id,
        source_id = %source_config.id,
        ?drain_style,
        remaining = %stash.len(),
        eligible = eligible_updates.len(),
        "draining stash");

    // Sort the eligible updates by (key, time, Reverse(from_time)) so that
    // deduping by (key, time) gives the latest change for that key.
    eligible_updates.sort_unstable_by(|a, b| {
        let (ts1, key1, from_ts1, val1) = a;
        let (ts2, key2, from_ts2, val2) = b;
        Ord::cmp(&(ts1, key1, from_ts1, val1), &(ts2, key2, from_ts2, val2))
    });

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

    // From the prefix that can be emitted we can deduplicate based on (ts, key) in
    // order to only process the command with the maximum order within the (ts,
    // key) group. This is achieved by wrapping order in `Reverse(FromTime)` above.;
    let mut commands = eligible_updates.into_iter().dedup_by(|a, b| {
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

        let existing_state_cell = &mut command_state.get_mut().value;

        if let Some(cs) = existing_state_cell.as_mut() {
            cs.ensure_decoded(bincode_opts, source_config.id);
        }

        // Skip this command if its order key is below the one in the upsert state.
        // Note that the existing order key may be `None` if the existing value
        // is from snapshotting, which always sorts below new values/deletes.
        let existing_order = existing_state_cell
            .as_ref()
            .and_then(|cs| cs.provisional_order(&ts));
        if existing_order >= Some(&from_time.0) {
            // Skip this update. If no later updates adjust this key, then we just
            // end up writing the same value back to state. If there
            // is nothing in the state, `existing_order` is `None`, and this
            // does not occur.
            continue;
        }

        match value {
            Some(value) => {
                if let Some(old_value) = existing_state_cell.as_ref() {
                    if let Some(old_value) = old_value.provisional_value_ref(&ts) {
                        output_updates.push((old_value.clone(), ts.clone(), Diff::MINUS_ONE));
                    }
                }

                match &drain_style {
                    DrainStyle::AtTime { .. } => {
                        let existing_value = existing_state_cell.take();

                        let new_value = match existing_value {
                            Some(existing_value) => existing_value.clone().into_provisional_value(
                                value.clone(),
                                ts.clone(),
                                from_time.0.clone(),
                            ),
                            None => StateValue::new_provisional_value(
                                value.clone(),
                                ts.clone(),
                                from_time.0.clone(),
                            ),
                        };

                        existing_state_cell.replace(new_value);
                    }
                    DrainStyle::ToUpper { .. } => {
                        // Not writing down provisional values, or anything.
                    }
                };

                output_updates.push((value, ts, Diff::ONE));
            }
            None => {
                if let Some(old_value) = existing_state_cell.as_ref() {
                    if let Some(old_value) = old_value.provisional_value_ref(&ts) {
                        output_updates.push((old_value.clone(), ts.clone(), Diff::MINUS_ONE));
                    }
                }

                match &drain_style {
                    DrainStyle::AtTime { .. } => {
                        let existing_value = existing_state_cell.take();

                        let new_value = match existing_value {
                            Some(existing_value) => existing_value
                                .into_provisional_tombstone(ts.clone(), from_time.0.clone()),
                            None => StateValue::new_provisional_tombstone(
                                ts.clone(),
                                from_time.0.clone(),
                            ),
                        };

                        existing_state_cell.replace(new_value);
                    }
                    DrainStyle::ToUpper { .. } => {
                        // Not writing down provisional values, or anything.
                    }
                }
            }
        }
    }

    match &drain_style {
        DrainStyle::AtTime { .. } => {
            match state
                .multi_put(
                    // We don't want to update per-record stats, like size of
                    // records indexed or count of records indexed.
                    //
                    // We only add provisional values and these will be
                    // overwritten once we receive updates for state from the
                    // persist input. And the merge functionality cannot know
                    // what was in state before merging, so it cannot correctly
                    // retract/update stats added here.
                    //
                    // Mostly, the merge functionality can't update those stats
                    // because merging happens in a function that we pass to
                    // rocksdb which doesn't have access to any external
                    // context. And in general, with rocksdb we do blind writes
                    // rather than inspect what was there before when
                    // updating/inserting.
                    false,
                    commands_state.drain(..).map(|(k, cv)| {
                        (
                            k,
                            upsert_types::PutValue {
                                value: cv.value.map(|cv| cv.into_decoded()),
                                previous_value_metadata: cv.metadata.map(|v| ValueMetadata {
                                    size: v.size.try_into().expect("less than i64 size"),
                                    is_tombstone: v.is_tombstone,
                                }),
                            },
                        )
                    }),
                )
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
        style => {
            tracing::trace!(
                worker_id = %source_config.worker_id,
                source_id = %source_config.id,
                "not doing state update for drain style {:?}", style);
        }
    }

    min_remaining_time.into_option()
}

#[cfg(test)]
mod test {
    use mz_ore::metrics::MetricsRegistry;
    use mz_persist_types::ShardId;
    use mz_repr::{Datum, Timestamp as MzTimestamp};
    use mz_storage_operators::persist_source::Subtime;
    use mz_storage_types::sources::SourceEnvelope;
    use mz_storage_types::sources::envelope::{KeyEnvelope, UpsertEnvelope, UpsertStyle};
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, Input};
    use timely::progress::Timestamp;

    use crate::metrics::StorageMetrics;
    use crate::metrics::upsert::UpsertMetricDefs;
    use crate::source::SourceExportCreationConfig;
    use crate::statistics::{SourceStatistics, SourceStatisticsMetricDefs};
    use crate::upsert::memory::InMemoryHashMap;

    use super::*;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn gh_9160_repro() {
        // Helper to wrap timestamps in the appropriate types
        let new_ts = |ts| (MzTimestamp::new(ts), Subtime::minimum());

        let output_handle = timely::execute_directly(move |worker| {
            let (mut input_handle, mut persist_handle, output_handle) = worker
                .dataflow::<MzTimestamp, _, _>(|scope| {
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

                        let (output, _, _, button) = upsert_inner(
                            &input.as_collection(),
                            vec![0],
                            Antichain::from_elem(Timestamp::minimum()),
                            persist_input.as_collection(),
                            None,
                            upsert_metrics,
                            source_config,
                            || async { InMemoryHashMap::default() },
                            upsert_config,
                            true,
                            None,
                        );
                        std::mem::forget(button);

                        (input_handle, persist_handle, output.inner.capture())
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
            worker.step();

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
            // We now step the worker so that the pending data is received. This causes the
            // operator to store internally the following map from capabilities to updates:
            // cap=2 => [ msg2, msg3 ]
            // cap=3 => [ msg4 ]
            worker.step();

            // We now assume that another replica raced us and processed msg1 at time 2, which in
            // this test is a no-op so the persist frontier advances to time 3 without new data.
            persist_handle.advance_to(new_ts(3));
            // We now step this worker again, which will notice that the persist upper is {3} and
            // wlil attempt to process msg3 and msg4 *separately*, causing it to produce a double
            // retraction.
            worker.step();

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
}
