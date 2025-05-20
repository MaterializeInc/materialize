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
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::{AsCollection, Collection};
use itertools::Itertools;
use mz_kv_cache::KeyValueReadHandle;
use mz_ore::vec::VecExt;
use mz_persist_client::Diagnostics;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, Row};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::{DataflowError, EnvelopeError, UpsertError};
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use std::convert::Infallible;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use crate::healthcheck::HealthStatusUpdate;
use crate::metrics::upsert::{UpsertMetrics, UpsertSharedMetrics};
use crate::upsert::UpsertConfig;
use crate::upsert::UpsertErrorEmitter;
use crate::upsert::UpsertKey;
use crate::upsert::UpsertValue;
use crate::upsert::types::UpsertStateBackend;

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
    persist_token: Option<Vec<PressOnDropButton>>,
    upsert_metrics: UpsertMetrics,
    source_config: crate::source::SourceExportCreationConfig,
    _state_fn: F,
    _upsert_config: UpsertConfig,
    _prevent_snapshot_buffering: bool,
    snapshot_buffering_max: Option<usize>,
    persist_clients: Arc<PersistClientCache>,
    storage_metadata: CollectionMetadata,
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
    US: UpsertStateBackend<G::Timestamp, Option<FromTime>>,
    FromTime: Debug + timely::ExchangeData + Ord + Sync,
{
    let mut builder = AsyncOperatorBuilder::new("Upsert".to_string(), input.scope());

    // We only care about UpsertValueError since this is the only error that we can retract
    let key_indices_clone = key_indices.clone();
    let persist_input = persist_input.flat_map(move |result| {
        let value = match result {
            Ok(ok) => Ok(ok),
            Err(DataflowError::EnvelopeError(err)) => match *err {
                EnvelopeError::Upsert(err) => Err(err),
                _ => return None,
            },
            Err(_) => return None,
        };
        Some((
            UpsertKey::from_value(value.as_ref(), &key_indices_clone),
            value,
        ))
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

        let persist_client = persist_clients
            .open(storage_metadata.persist_location.clone())
            .await
            .unwrap();

        // We create a new read handle every time someone requests a snapshot
        // and then immediately expire it instead of keeping a read handle
        // permanently in our state to avoid having it heartbeat continually.
        // The assumption is that calls to snapshot are rare and therefore worth
        // it to always create a new handle.
        let read_handle = persist_client
            .open_leased_reader::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                storage_metadata.data_shard,
                Arc::new(storage_metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: source_config.id.to_string(),
                    handle_purpose: format!("upsert kv read handle {}", source_config.id),
                },
                false,
            )
            .await
            .expect("invalid persist usage");

        let mut state = KeyValueReadHandle::new(read_handle);

        // We're always "hydrated", so no need for upstream to wait on us.
        drop(snapshot_cap);

        // For stashing source input while it's not eligible for processing.
        let mut stash = vec![];
        // A capability suitable for emitting any updates based on stash. No capability is held
        // when the stash is empty.
        let mut stash_cap: Option<Capability<G::Timestamp>> = None;
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());

        let mut persist_upper = Antichain::from_elem(Timestamp::minimum());

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

                                // WIP: Throw it away for now, but later feed it
                                // into our KV cache.
                                drop(data);
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
            if !stash.is_empty() {
                let cap = stash_cap
                    .as_mut()
                    .expect("missing capability for non-empty stash");

                tracing::trace!(
                    worker_id = %source_config.worker_id,
                    source_id = %source_config.id,
                    ?cap,
                    ?stash,
                    "stashed updates");

                let mut min_remaining_time = drain_staged_input::<G, _, _, _>(
                    &mut stash,
                    &key_indices,
                    &mut output_updates,
                    &input_upper,
                    &persist_upper,
                    &mut error_emitter,
                    &mut state,
                    &source_config,
                    &upsert_shared_metrics,
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
                    let min_remaining_time = min_remaining_time
                        .take()
                        .expect("we still have updates left");
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

        drop(persist_token);
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
async fn drain_staged_input<G, T, FromTime, E>(
    stash: &mut Vec<(T, UpsertKey, Reverse<FromTime>, Option<UpsertValue>)>,
    key_indices: &[usize],
    output_updates: &mut Vec<(Result<Row, UpsertError>, T, Diff)>,
    input_upper: &Antichain<T>,
    persist_upper: &Antichain<T>,
    _error_emitter: &mut E,
    state: &mut KeyValueReadHandle<mz_repr::Timestamp>,
    source_config: &crate::source::SourceExportCreationConfig,
    shared_metrics: &Arc<UpsertSharedMetrics>,
) -> Option<T>
where
    G: Scope,
    T: TotalOrder + timely::ExchangeData + Debug + Ord + Sync,
    FromTime: timely::ExchangeData + Ord + Sync,
    E: UpsertErrorEmitter<G>,
    T: Refines<mz_repr::Timestamp> + TotalOrder + Sync,
{
    let mut min_remaining_time = Antichain::new();

    let query_ts = persist_upper
        .as_option()
        .cloned()
        .expect("source must be shut down by now")
        .to_outer()
        .step_back()
        .unwrap_or(mz_repr::Timestamp::minimum());

    let mut eligible_updates = stash
        .drain_filter_swapping(|(ts, _, _, _)| {
            // We make sure that a) we only process updates when we know their
            // timestamp is complete, that is there will be no more updates for
            // that timestamp, and b) that "previous" times in the persist input
            // are complete. The latter makes sure that we emit updates for the
            // next timestamp that are consistent with the global state in the
            // output persist shard, which also serves as a persistent copy of
            // our in-memory/on-disk upsert state.
            let eligible = !input_upper.less_equal(ts) && !persist_upper.less_than(ts);

            if !eligible {
                min_remaining_time.insert(ts.clone());
            }

            eligible
        })
        .filter(|(ts, _, _, _)| {
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
        ?input_upper,
        ?persist_upper,
        remaining = %stash.len(),
        eligible = eligible_updates.len(),
        ?query_ts,
        "draining stash");

    // Sort the eligible updates by (key, time, Reverse(from_time)) so that
    // deduping by (key, time) gives the latest change for that key.
    eligible_updates.sort_unstable_by(|a, b| {
        let (ts1, key1, from_ts1, val1) = a;
        let (ts2, key2, from_ts2, val2) = b;
        Ord::cmp(&(ts1, key1, from_ts1, val1), &(ts2, key2, from_ts2, val2))
    });

    // From the prefix that can be emitted we can deduplicate based on (ts, key) in
    // order to only process the command with the maximum order within the (ts,
    // key) group. This is achieved by wrapping order in `Reverse(FromTime)` above.;
    let eligible_updates = eligible_updates
        .into_iter()
        .dedup_by(|a, b| {
            let ((a_ts, a_key, _, _), (b_ts, b_key, _, _)) = (a, b);
            a_ts == b_ts && a_key == b_key
        })
        .collect_vec();

    let keys = eligible_updates
        .iter()
        .map(|(_ts, upsert_key, _from_time, _value)| upsert_key.key_columns.clone())
        .collect_vec();

    let existing_values = if query_ts != mz_repr::Timestamp::minimum() {
        let now = Instant::now();
        let existing_values: BTreeMap<Row, Row> = state
            .get_multi(key_indices, keys, query_ts)
            .await
            .into_iter()
            .collect();

        let latency = now.elapsed();

        shared_metrics
            .multi_get_latency
            .observe(latency.as_secs_f64());

        existing_values
    } else {
        BTreeMap::new()
    };

    tracing::trace!(?existing_values, "got kv results");

    for (ts, key, _from_time, value) in eligible_updates.into_iter() {
        let existing_value = existing_values.get(&key.key_columns);

        match value {
            Some(new_value) => {
                output_updates.push((new_value, ts.clone(), Diff::ONE));
            }
            None => todo!(),
        }

        match existing_value {
            Some(existing_value) => {
                output_updates.push((Ok(existing_value.clone()), ts.clone(), Diff::MINUS_ONE));
            }
            None => {
                // nothing to do!
            }
        }
    }

    min_remaining_time.into_option()
}
