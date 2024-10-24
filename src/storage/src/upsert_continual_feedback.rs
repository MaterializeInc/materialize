// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::fmt::Debug;
use std::sync::Arc;

use differential_dataflow::consolidation;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::{AsCollection, Collection};
use futures::future::FutureExt;
use futures::StreamExt;
use indexmap::map::Entry;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_repr::{Diff, Row};
use mz_storage_types::errors::{DataflowError, EnvelopeError, UpsertError};
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use std::convert::Infallible;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::{Antichain, Timestamp};

use crate::healthcheck::HealthStatusUpdate;
use crate::metrics::upsert::UpsertMetrics;
use crate::render::sources::OutputIndex;
use crate::upsert::types as upsert_types;
use crate::upsert::types::{StateValue, UpsertState, UpsertStateBackend, Value};
use crate::upsert::UpsertConfig;
use crate::upsert::UpsertErrorEmitter;
use crate::upsert::UpsertKey;
use crate::upsert::UpsertValue;

use upsert_types::ValueMetadata;

pub(crate) fn upsert_inner<G: Scope, FromTime, F, Fut, US>(
    input: &Collection<G, (UpsertKey, Option<UpsertValue>, FromTime), Diff>,
    key_indices: Vec<usize>,
    resume_upper: Antichain<G::Timestamp>,
    previous: Collection<G, Result<Row, DataflowError>, Diff>,
    previous_token: Option<Vec<PressOnDropButton>>,
    upsert_metrics: UpsertMetrics,
    source_config: crate::source::RawSourceCreationConfig,
    state: F,
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
    FromTime: timely::ExchangeData + Ord,
{
    let mut builder = AsyncOperatorBuilder::new("Upsert".to_string(), input.scope());

    // We only care about UpsertValueError since this is the only error that we can retract
    let previous = previous.flat_map(move |result| {
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

    let mut previous = builder.new_input_for(
        &previous.inner,
        Exchange::new(|((key, _), _, _)| UpsertKey::hashed(key)),
        &output_handle,
    );

    let upsert_shared_metrics = Arc::clone(&upsert_metrics.shared);
    let shutdown_button = builder.build(move |caps| async move {
        let [mut output_cap, mut snapshot_cap, health_cap]: [_; 3] = caps.try_into().unwrap();

        // The order key of the `UpsertState` is `Option<FromTime>`, which implements `Default`
        // (as required for `consolidate_snapshot_chunk`), with slightly more efficient serialization
        // than a default `Partitioned`.
        let mut state = UpsertState::<_, Option<FromTime>>::new(
            state().await,
            upsert_shared_metrics,
            &upsert_metrics,
            source_config.source_statistics,
            upsert_config.shrink_upsert_unused_buffers_by_ratio,
        );
        let mut events = vec![];
        let mut snapshot_upper = Antichain::from_elem(Timestamp::minimum());

        let mut stash = vec![];
        let mut legacy_errors_to_correct = vec![];

        let mut error_emitter = (&mut health_output, &health_cap);

        tracing::info!(
            ?resume_upper,
            ?snapshot_upper,
            "timely-{} upsert source {} starting rehydration",
            source_config.worker_id,
            source_config.id
        );
        // Read and consolidate the snapshot from the 'previous' input until it
        // reaches the `resume_upper`.
        while !PartialOrder::less_equal(&resume_upper, &snapshot_upper) {
            previous.ready().await;
            while let Some(event) = previous.next_sync() {
                match event {
                    AsyncEvent::Data(_cap, data) => {
                        events.extend(data.into_iter().filter_map(|((key, value), ts, diff)| {
                            if !resume_upper.less_equal(&ts) {
                                Some((key, value, diff))
                            } else {
                                None
                            }
                        }))
                    }
                    AsyncEvent::Progress(upper) => {
                        snapshot_upper = upper;
                    }
                };
            }

            for (_, value, diff) in events.iter_mut() {
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

            match state
                .consolidate_snapshot_chunk(
                    events.drain(..),
                    PartialOrder::less_equal(&resume_upper, &snapshot_upper),
                )
                .await
            {
                Ok(_) => {
                    if let Some(ts) = snapshot_upper.clone().into_option() {
                        // As we shutdown, we could ostensibly get data from later than the
                        // `resume_upper`, which we ignore above. We don't want our output capability to make
                        // it further than the `resume_upper`.
                        if !resume_upper.less_equal(&ts) {
                            snapshot_cap.downgrade(&ts);
                            output_cap.downgrade(&ts);
                        }
                    }
                }
                Err(e) => {
                    UpsertErrorEmitter::<G>::emit(
                        &mut error_emitter,
                        "Failed to rehydrate state".to_string(),
                        e,
                    )
                    .await;
                }
            }
        }

        drop(events);
        drop(previous_token);
        drop(snapshot_cap);

        // Exchaust the previous input. It is expected to immediately reach the empty
        // antichain since we have dropped its token.
        //
        // Note that we do not need to also process the `input` during this, as the dropped token
        // will shutdown the `backpressure` operator
        while let Some(_event) = previous.next().await {}

        // After snapshotting, our output frontier is exactly the `resume_upper`
        if let Some(ts) = resume_upper.as_option() {
            output_cap.downgrade(ts);
        }

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
        for (mut error, diff) in legacy_errors_to_correct {
            assert!(
                error.is_legacy_dont_touch_it,
                "attempted to correct non-legacy error"
            );
            tracing::info!("correcting legacy error {error:?} with diff {diff}");
            let time = output_cap.time().clone();
            let retraction = Err(UpsertError::Value(error.clone()));
            error.is_legacy_dont_touch_it = false;
            let insertion = Err(UpsertError::Value(error));
            output_handle.give(&output_cap, (retraction, time.clone(), -diff));
            output_handle.give(&output_cap, (insertion, time, diff));
        }

        tracing::info!(
            "timely-{} upsert source {} finished rehydration",
            source_config.worker_id,
            source_config.id
        );

        // A re-usable buffer of changes, per key. This is an `IndexMap` because it has to be `drain`-able
        // and have a consistent iteration order.
        let mut commands_state: indexmap::IndexMap<
            _,
            upsert_types::UpsertValueAndSize<Option<FromTime>>,
        > = indexmap::IndexMap::new();
        let mut multi_get_scratch = Vec::new();

        // Now can can resume consuming the collection
        let mut output_updates = vec![];
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());

        while let Some(event) = input.next().await {
            // Buffer as many events as possible. This should be bounded, as new data can't be
            // produced in this worker until we yield to timely.
            let events = [event]
                .into_iter()
                .chain(std::iter::from_fn(|| input.next().now_or_never().flatten()))
                .enumerate();

            let mut partial_drain_time = None;
            for (i, event) in events {
                match event {
                    AsyncEvent::Data(cap, mut data) => {
                        tracing::trace!(
                            time=?cap.time(),
                            updates=%data.len(),
                            "received data in upsert"
                        );
                        stage_input(
                            &mut stash,
                            &mut data,
                            &input_upper,
                            &resume_upper,
                            upsert_config.shrink_upsert_unused_buffers_by_ratio,
                        );

                        let event_time = cap.time();
                        // If the data is at _exactly_ the output frontier, we can preemptively drain it into the state.
                        // Data within this set events strictly beyond this time are staged as
                        // normal.
                        //
                        // This is a load-bearing optimization, as it is required to avoid buffering
                        // the entire source snapshot in the `stash`.
                        if prevent_snapshot_buffering && output_cap.time() == event_time {
                            partial_drain_time = Some(event_time.clone());
                        }
                    }
                    AsyncEvent::Progress(upper) => {
                        tracing::trace!(?upper, "received progress in upsert");
                        // Ignore progress updates before the `resume_upper`, which is our initial
                        // capability post-snapshotting.
                        if PartialOrder::less_than(&upper, &resume_upper) {
                            continue;
                        }

                        // Disable the partial drain as this progress event covers
                        // the `output_cap` time.
                        partial_drain_time = None;
                        drain_staged_input::<_, G, _, _, _>(
                            &mut stash,
                            &mut commands_state,
                            &mut output_updates,
                            &mut multi_get_scratch,
                            DrainStyle::ToUpper(&upper),
                            &mut error_emitter,
                            &mut state,
                        )
                        .await;

                        output_handle.give_container(&output_cap, &mut output_updates);

                        if let Some(ts) = upper.as_option() {
                            output_cap.downgrade(ts);
                        }
                        input_upper = upper;
                    }
                }
                let events_processed = i + 1;
                if let Some(max) = snapshot_buffering_max {
                    if events_processed >= max {
                        break;
                    }
                }
            }

            // If there were staged events that occurred at the capability time, drain
            // them. This is safe because out-of-order updates to the same key that are
            // drained in separate calls to `drain_staged_input` are correctly ordered by
            // their `FromTime` in `drain_staged_input`.
            //
            // Note also that this may result in more updates in the output collection than
            // the minimum. However, because the frontier only advances on `Progress` updates,
            // the collection always accumulates correctly for all keys.
            if let Some(partial_drain_time) = partial_drain_time {
                drain_staged_input::<_, G, _, _, _>(
                    &mut stash,
                    &mut commands_state,
                    &mut output_updates,
                    &mut multi_get_scratch,
                    DrainStyle::AtTime(partial_drain_time),
                    &mut error_emitter,
                    &mut state,
                )
                .await;

                output_handle.give_container(&output_cap, &mut output_updates);
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

/// Helper method for `upsert_inner` used to stage `data` updates
/// from the input timely edge.
fn stage_input<T, FromTime>(
    stash: &mut Vec<(T, UpsertKey, Reverse<FromTime>, Option<UpsertValue>)>,
    data: &mut Vec<((UpsertKey, Option<UpsertValue>, FromTime), T, Diff)>,
    input_upper: &Antichain<T>,
    resume_upper: &Antichain<T>,
    storage_shrink_upsert_unused_buffers_by_ratio: usize,
) where
    T: PartialOrder,
    FromTime: Ord,
{
    if PartialOrder::less_equal(input_upper, resume_upper) {
        data.retain(|(_, ts, _)| resume_upper.less_equal(ts));
    }

    stash.extend(data.drain(..).map(|((key, value, order), time, diff)| {
        assert!(diff > 0, "invalid upsert input");
        (time, key, Reverse(order), value)
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
    ToUpper(&'a Antichain<T>),
    AtTime(T),
}

/// Helper method for `upsert_inner` used to stage `data` updates
/// from the input timely edge.
async fn drain_staged_input<S, G, T, FromTime, E>(
    stash: &mut Vec<(T, UpsertKey, Reverse<FromTime>, Option<UpsertValue>)>,
    commands_state: &mut indexmap::IndexMap<
        UpsertKey,
        upsert_types::UpsertValueAndSize<Option<FromTime>>,
    >,
    output_updates: &mut Vec<(Result<Row, UpsertError>, T, Diff)>,
    multi_get_scratch: &mut Vec<UpsertKey>,
    drain_style: DrainStyle<'_, T>,
    error_emitter: &mut E,
    state: &mut UpsertState<'_, S, Option<FromTime>>,
) where
    S: UpsertStateBackend<Option<FromTime>>,
    G: Scope,
    T: PartialOrder + Ord + Clone + Debug,
    FromTime: timely::ExchangeData + Ord,
    E: UpsertErrorEmitter<G>,
{
    stash.sort_unstable();

    // Find the prefix that we can emit
    let idx = stash.partition_point(|(ts, _, _, _)| match &drain_style {
        DrainStyle::ToUpper(upper) => !upper.less_equal(ts),
        DrainStyle::AtTime(time) => ts <= time,
    });

    tracing::trace!(?drain_style, updates = idx, "draining stash in upsert");

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

    match state
        .multi_put(commands_state.drain(..).map(|(k, cv)| {
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
        }))
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
