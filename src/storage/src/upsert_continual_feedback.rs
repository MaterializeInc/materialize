// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of feedback UPSERT operator. See [`upsert_inner`] for how it
//! works.

use std::cmp::Reverse;
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::trace::implementations::Vector;
use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, OrdValBuilder};
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
use differential_dataflow::trace::{Builder, Cursor, Description, Trace, TraceReader};
use differential_dataflow::{AsCollection, VecCollection};
use itertools::Itertools;
use mz_repr::{Diff, GlobalId, Row};
use mz_storage_types::errors::{DataflowError, EnvelopeError};
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use std::convert::Infallible;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::OperatorInfo;
use timely::dataflow::operators::{Capability, CapabilitySet};
use timely::dataflow::{Scope, StreamVec};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use crate::healthcheck::HealthStatusUpdate;
use crate::metrics::upsert::UpsertMetrics;
use crate::upsert::UpsertKey;
use crate::upsert::UpsertValue;

// ── Spine type aliases ──────────────────────────────────────────────────────
// We use a differential Spine as the backing KV store. This gives us:
// - Contiguous batch storage (no per-Row heap allocations in long-lived state)
// - Automatic consolidation via the spine's merge tree
// - Efficient cursor-based point lookups
//
// We use a simple u64 counter as the "timestamp" — the spine doesn't care about
// real timestamps, it just needs a totally ordered time for batch boundaries.

type SpineTime = u64;
type UpsertUpdate = ((UpsertKey, UpsertValue), SpineTime, Diff);
type UpsertLayout = Vector<UpsertUpdate>;
type UpsertBatch = Rc<OrdValBatch<UpsertLayout>>;
type UpsertSpine = Spine<UpsertBatch>;
type UpsertBuilder = RcBuilder<OrdValBuilder<UpsertLayout, Vec<UpsertUpdate>>>;

/// Look up the current value for `key` in the spine using the cursor.
/// Returns `Some(value)` if the key exists (net diff = 1), `None` otherwise.
fn spine_lookup<'a>(
    cursor: &mut <UpsertSpine as TraceReader>::Cursor,
    storage: &<UpsertSpine as TraceReader>::Storage,
    key: &UpsertKey,
) -> Option<UpsertValue> {
    cursor.seek_key(storage, key);
    if cursor.get_key(storage) != Some(key) {
        return None;
    }
    let mut result = None;
    while let Some(val) = cursor.get_val(storage) {
        let mut count: isize = 0;
        cursor.map_times(storage, |_time, diff| {
            count += diff.into_inner() as isize;
        });
        debug_assert!(count == 0 || count == 1, "unexpected diff count {count} for key {key:?}");
        if count == 1 {
            debug_assert!(result.is_none(), "multiple values with count=1 for key {key:?}");
            result = Some(val.clone());
        }
        cursor.step_val(storage);
    }
    result
}

/// Build a batch from sorted updates and insert it into the spine.
/// `batch_time` is the artificial timestamp for this batch.
fn insert_batch(
    trace: &mut UpsertSpine,
    updates: &mut Vec<UpsertUpdate>,
    batch_time: SpineTime,
) {
    if updates.is_empty() {
        return;
    }

    // Consolidate before building: the builder does not handle cancelling
    // diffs, so (key, val, +1) and (key, val, -1) must cancel here.
    differential_dataflow::consolidation::consolidate_updates(updates);

    // Always build and insert the batch, even if empty after consolidation.
    // The spine requires batch.lower() == trace.upper, so skipping would
    // desync batch_time from the trace's upper frontier.
    let len = updates.len();
    let mut builder = UpsertBuilder::with_capacity(len, len, len);
    builder.push(updates);

    let lower = Antichain::from_elem(batch_time);
    let upper = Antichain::from_elem(batch_time + 1);
    let since = Antichain::from_elem(SpineTime::minimum());
    let batch = builder.done(Description::new(lower, upper, since));

    trace.insert(batch);

    // Allow the spine to compact everything up to this time.
    trace.set_logical_compaction(Antichain::from_elem(batch_time).borrow());
    trace.set_physical_compaction(Antichain::from_elem(batch_time).borrow());
}

/// Transforms a stream of upserts (key-value updates) into a differential
/// collection. State is backed by a differential Spine for contiguous memory
/// layout.
///
/// Has two inputs:
///   1. **Source input** — upsert commands from the external source.
///   2. **Persist input** — feedback of the operator's own output, read back
///      from persist.  This is the single source of truth for state.
///
/// The operator only updates its internal state from the persist feedback,
/// never from source input directly.
pub fn upsert_inner<G: Scope, FromTime>(
    input: VecCollection<G, (UpsertKey, Option<UpsertValue>, FromTime), Diff>,
    key_indices: Vec<usize>,
    resume_upper: Antichain<G::Timestamp>,
    persist_input: VecCollection<G, Result<Row, DataflowError>, Diff>,
    persist_token: Option<Vec<PressOnDropButton>>,
    upsert_metrics: UpsertMetrics,
    source_config: crate::source::SourceExportCreationConfig,
) -> (
    VecCollection<G, Result<Row, DataflowError>, Diff>,
    StreamVec<G, (Option<GlobalId>, HealthStatusUpdate)>,
    StreamVec<G, Infallible>,
    PressOnDropButton,
)
where
    G::Timestamp: Refines<mz_repr::Timestamp> + TotalOrder + Sync,
    FromTime: Debug + timely::ExchangeData + Clone + Ord + Sync,
{
    let mut builder = AsyncOperatorBuilder::new("Upsert".to_string(), input.scope());

    // We only care about UpsertValueError since this is the only error that we can retract.
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
    let (_snapshot_handle, snapshot_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<Infallible>>>();
    let (_health_output, health_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<(Option<GlobalId>, HealthStatusUpdate)>>>();

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
    let _ = upsert_shared_metrics;

    let shutdown_button = builder.build(move |caps| async move {
        // Hold the persist token alive for the lifetime of the operator.
        // Dropping it would signal the persist source to shut down.
        let _persist_token = persist_token;

        let [output_cap, snapshot_cap, _health_cap]: [_; 3] = caps.try_into().unwrap();
        drop(output_cap);
        let mut snapshot_cap = CapabilitySet::from_elem(snapshot_cap);

        // ── State: a differential Spine ─────────────────────────────────
        let op_info = OperatorInfo::new(0, 0, [].into());
        let mut trace = UpsertSpine::new(op_info, None, None);
        let mut batch_time: SpineTime = 0;
        let mut hydrating = true;

        let mut stash = vec![];
        let mut stash_cap: Option<Capability<G::Timestamp>> = None;
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());

        let mut persist_stash = vec![];
        let mut persist_upper = Antichain::from_elem(Timestamp::minimum());
        let mut largest_seen_persist_ts: Option<G::Timestamp> = None;

        let mut output_updates = vec![];
        let snapshot_start = std::time::Instant::now();

        loop {
            // ── Read from whichever input is ready ──────────────────────
            tokio::select! {
                _ = persist_input.ready() => {
                    while let Some(event) = persist_input.next_sync() {
                        match event {
                            AsyncEvent::Data(_time, data) => {
                                persist_stash.extend(data.into_iter().map(
                                    |((key, value), ts, diff)| {
                                        largest_seen_persist_ts = std::cmp::max(
                                            largest_seen_persist_ts.clone(),
                                            Some(ts.clone()),
                                        );
                                        (key, value, diff)
                                    },
                                ));
                            }
                            AsyncEvent::Progress(upper) => {
                                persist_upper = upper;
                            }
                        }
                    }

                    let last_rehydration_chunk =
                        hydrating && PartialOrder::less_equal(&resume_upper, &persist_upper);

                    // Build a batch from persist feedback and insert into
                    // the spine. Same codepath for rehydration and
                    // steady-state.
                    if !persist_stash.is_empty() {
                        let mut updates: Vec<UpsertUpdate> = persist_stash
                            .drain(..)
                            .map(|(k, v, diff)| ((k, v), batch_time, diff))
                            .collect();
                        insert_batch(&mut trace, &mut updates, batch_time);
                        batch_time += 1;
                    }

                    if last_rehydration_chunk {
                        hydrating = false;

                        upsert_metrics.rehydration_latency.set(
                            snapshot_start.elapsed().as_secs_f64(),
                        );
                        tracing::info!(
                            worker_id = %source_config.worker_id,
                            source_id = %source_config.id,
                            "upsert finished rehydration",
                        );
                        snapshot_cap.downgrade(&[]);
                    }

                    let _ = snapshot_cap.try_downgrade(persist_upper.iter());
                }
                _ = input.ready() => {
                    while let Some(event) = input.next_sync() {
                        match event {
                            AsyncEvent::Data(cap, mut data) => {
                                stage_input(&mut stash, &mut data, &input_upper, &resume_upper);
                                if !stash.is_empty() {
                                    stash_cap = Some(match stash_cap {
                                        Some(prev) if cap.time() < prev.time() => cap,
                                        Some(prev) => prev,
                                        None => cap,
                                    });
                                }
                            }
                            AsyncEvent::Progress(upper) => {
                                if PartialOrder::less_than(&upper, &resume_upper) {
                                    continue;
                                }
                                input_upper = upper;
                            }
                        }
                    }
                }
            };

            // ── Guard: wait until persist has caught up ─────────────────
            if let Some(ref ts) = largest_seen_persist_ts {
                let outer = ts.clone().to_outer();
                let outer_upper = Antichain::from_iter(
                    persist_upper.iter().map(|t| t.clone().to_outer()),
                );
                if outer_upper.less_equal(&outer) {
                    continue;
                }
            }

            // ── Drain eligible source updates ───────────────────────────
            if !stash.is_empty() {
                let cap = stash_cap
                    .as_mut()
                    .expect("capability for non-empty stash");

                drain_staged_input(
                    &mut stash,
                    &mut output_updates,
                    &input_upper,
                    &persist_upper,
                    &mut trace,
                    &source_config,
                );

                for (update, ts, diff) in output_updates.drain(..) {
                    output_handle.give(cap, (update, ts, diff));
                }

                if stash.is_empty() {
                    stash_cap = None;
                } else {
                    let min_ts = stash.iter().map(|(ts, ..)| ts).min().unwrap().clone();
                    cap.downgrade(&min_ts);
                }
            }

            if input_upper.is_empty() {
                break;
            }
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

/// Buffer incoming source updates into the stash, filtering out anything
/// before the resume upper.
fn stage_input<T: PartialOrder + Timestamp, FromTime: Ord>(
    stash: &mut Vec<(T, UpsertKey, Reverse<FromTime>, Option<UpsertValue>)>,
    data: &mut Vec<((UpsertKey, Option<UpsertValue>, FromTime), T, Diff)>,
    input_upper: &Antichain<T>,
    resume_upper: &Antichain<T>,
) {
    if PartialOrder::less_equal(input_upper, resume_upper) {
        data.retain(|(_, ts, _)| resume_upper.less_equal(ts));
    }
    stash.extend(data.drain(..).map(|((key, value, order), time, diff)| {
        assert!(diff.is_positive(), "invalid upsert input");
        (time, key, Reverse(order), value)
    }));
}

/// Drain source updates whose timestamp is complete and whose persist state is
/// caught up. Uses cursor-based lookups on the spine for previous values.
fn drain_staged_input<T, FromTime>(
    stash: &mut Vec<(T, UpsertKey, Reverse<FromTime>, Option<UpsertValue>)>,
    output: &mut Vec<(UpsertValue, T, Diff)>,
    input_upper: &Antichain<T>,
    persist_upper: &Antichain<T>,
    trace: &mut UpsertSpine,
    source_config: &crate::source::SourceExportCreationConfig,
) where
    T: TotalOrder + timely::ExchangeData + Clone + Debug + Ord + Sync,
    FromTime: timely::ExchangeData + Clone + Ord + Sync,
{
    let mut eligible: Vec<_> = stash
        .extract_if(.., |(ts, ..)| {
            !input_upper.less_equal(ts) && !persist_upper.less_than(ts)
        })
        .filter(|(ts, ..)| persist_upper.less_equal(ts))
        .collect();

    tracing::debug!(
        worker_id = %source_config.worker_id,
        source_id = %source_config.id,
        remaining = stash.len(),
        eligible = eligible.len(),
        "draining stash",
    );

    if eligible.is_empty() {
        return;
    }

    // Sort by (ts, key, Reverse(from_time)) so dedup keeps the highest
    // from_time per (ts, key).
    eligible.sort_unstable();
    let commands: Vec<_> = eligible
        .into_iter()
        .dedup_by(|a, b| a.0 == b.0 && a.1 == b.1)
        .collect();

    // Phase 1: Collect unique keys and look them up in key-sorted order.
    // The spine cursor only moves forward, so we must traverse keys in order.
    let mut prev_values: std::collections::BTreeMap<UpsertKey, Option<UpsertValue>> =
        std::collections::BTreeMap::new();
    for (_, key, _, _) in &commands {
        prev_values.entry(*key).or_default();
    }

    let (mut cursor, storage) = trace.cursor();
    for (key, slot) in prev_values.iter_mut() {
        *slot = spine_lookup(&mut cursor, &storage, key);
    }

    // Phase 2: Process commands using the looked-up values.
    for (ts, key, _from_time, value) in commands {
        let old = prev_values.get(&key).and_then(|v| v.as_ref());
        match value {
            Some(new_val) => {
                if let Some(old_val) = old {
                    output.push((old_val.clone(), ts.clone(), Diff::MINUS_ONE));
                }
                output.push((new_val, ts, Diff::ONE));
            }
            None => {
                if let Some(old_val) = old {
                    output.push((old_val.clone(), ts, Diff::MINUS_ONE));
                }
            }
        }
    }
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

    use super::*;

    type Ts = (MzTimestamp, Subtime);

    fn new_ts(ts: u64) -> Ts {
        (MzTimestamp::new(ts), Subtime::minimum())
    }

    fn key(k: i64) -> UpsertKey {
        UpsertKey::from_key(Ok(&Row::pack_slice(&[Datum::Int64(k)])))
    }

    fn row(k: i64, v: i64) -> Row {
        Row::pack_slice(&[Datum::Int64(k), Datum::Int64(v)])
    }

    /// Each test sets up its own dataflow. This macro reduces the boilerplate.
    macro_rules! upsert_test {
        (|$input:ident, $persist:ident, $worker:ident| $body:block) => {{
            let output_handle = timely::execute_directly(move |$worker| {
                let (mut $input, mut $persist, output_handle) = $worker
                    .dataflow::<MzTimestamp, _, _>(|scope| {
                        scope.scoped::<Ts, _, _>("upsert", |scope| {
                            let (input_handle, input) = scope.new_input();
                            let (persist_handle, persist_input) = scope.new_input();
                            let source_id = GlobalId::User(0);

                            let reg = MetricsRegistry::new();
                            let upsert_defs = UpsertMetricDefs::register_with(&reg);
                            let upsert_metrics =
                                UpsertMetrics::new(&upsert_defs, source_id, 0, None);

                            let reg2 = MetricsRegistry::new();
                            let storage_metrics = StorageMetrics::register_with(&reg2);

                            let reg3 = MetricsRegistry::new();
                            let stats_defs =
                                SourceStatisticsMetricDefs::register_with(&reg3);
                            let envelope = SourceEnvelope::Upsert(UpsertEnvelope {
                                source_arity: 2,
                                style: UpsertStyle::Default(KeyEnvelope::Flattened),
                                key_indices: vec![0],
                            });
                            let source_statistics = SourceStatistics::new(
                                source_id, 0, &stats_defs, source_id, &ShardId::new(),
                                envelope, Antichain::from_elem(Timestamp::minimum()),
                            );
                            let source_config = SourceExportCreationConfig {
                                id: source_id,
                                worker_id: 0,
                                metrics: storage_metrics,
                                source_statistics,
                            };

                            let (output, _, _, button) = upsert_inner(
                                input.as_collection(),
                                vec![0],
                                Antichain::from_elem(Timestamp::minimum()),
                                persist_input.as_collection(),
                                None,
                                upsert_metrics,
                                source_config,
                            );
                            std::mem::forget(button);
                            (input_handle, persist_handle, output.inner.capture())
                        })
                    });

                $body

                output_handle
            });

            let mut actual: Vec<_> = output_handle
                .extract()
                .into_iter()
                .flat_map(|(_cap, container)| container)
                .collect();
            differential_dataflow::consolidation::consolidate_updates(&mut actual);
            actual
        }};
    }

    /// Regression test: two updates to the same key at different timestamps
    /// must produce correct retractions even when deduped across timestamps.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn gh_9160_repro() {
        let actual = upsert_test!(|input, persist, worker| {
            let key0 = key(0);
            let key1 = key(1);
            let value1 = row(0, 0);
            let value3 = row(0, 1);
            let value4 = row(0, 2);

            input.send(((key0, Some(Ok(value1.clone())), 1), new_ts(0), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();

            persist.send((Ok(value1), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            input.send_batch(&mut vec![
                ((key1, None, 2), new_ts(2), Diff::ONE),
                ((key0, Some(Ok(value3)), 3), new_ts(3), Diff::ONE),
            ]);
            input.advance_to(new_ts(3));
            input.send_batch(&mut vec![((key0, Some(Ok(value4)), 4), new_ts(3), Diff::ONE)]);
            input.advance_to(new_ts(4));
            worker.step();

            persist.advance_to(new_ts(3));
            worker.step();
        });

        let value1 = row(0, 0);
        let value4 = row(0, 2);
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(value1.clone()), new_ts(0), Diff::ONE),
            (Ok(value1), new_ts(3), Diff::MINUS_ONE),
            (Ok(value4), new_ts(3), Diff::ONE),
        ];
        assert_eq!(actual, expected);
    }

    /// Cursor ordering: keys at the same timestamp must all be found in the
    /// spine regardless of their key sort order vs. timestamp order.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn out_of_order_keys_across_timestamps() {
        let actual = upsert_test!(|input, persist, worker| {
            let key_high = key(99);
            let key_low = key(1);
            let val_a = row(99, 1);
            let val_b = row(1, 2);

            // key_high at T=0
            input.send(((key_high, Some(Ok(val_a.clone())), 1), new_ts(0), Diff::ONE));
            input.advance_to(new_ts(1));
            worker.step();

            persist.send((Ok(val_a.clone()), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            // key_low at T=1
            input.send(((key_low, Some(Ok(val_b.clone())), 2), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();

            persist.send((Ok(val_b.clone()), new_ts(1), Diff::ONE));
            persist.advance_to(new_ts(2));
            worker.step();

            // BOTH keys updated at T=2. Exercises cursor key-order traversal.
            let val_a2 = row(99, 10);
            let val_b2 = row(1, 20);
            input.send_batch(&mut vec![
                ((key_high, Some(Ok(val_a2.clone())), 3), new_ts(2), Diff::ONE),
                ((key_low, Some(Ok(val_b2.clone())), 4), new_ts(2), Diff::ONE),
            ]);
            input.advance_to(new_ts(3));
            worker.step();

            persist.advance_to(new_ts(3));
            worker.step();
        });

        let val_a = row(99, 1);
        let val_b = row(1, 2);
        let val_a2 = row(99, 10);
        let val_b2 = row(1, 20);
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(val_b.clone()), new_ts(1), Diff::ONE),
            (Ok(val_b), new_ts(2), Diff::MINUS_ONE),
            (Ok(val_b2), new_ts(2), Diff::ONE),
            (Ok(val_a.clone()), new_ts(0), Diff::ONE),
            (Ok(val_a), new_ts(2), Diff::MINUS_ONE),
            (Ok(val_a2), new_ts(2), Diff::ONE),
        ];
        let mut actual_sorted = actual;
        let mut expected_sorted = expected;
        actual_sorted.sort();
        expected_sorted.sort();
        assert_eq!(actual_sorted, expected_sorted);
    }

    /// Rehydration: persist feedback arrives before source input. Subsequent
    /// source updates should retract the rehydrated values correctly.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn rehydration_then_update() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(42);
            let old_val = row(42, 100);
            let new_val = row(42, 200);

            persist.send((Ok(old_val), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            input.send(((k, Some(Ok(new_val)), 1), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();

            persist.advance_to(new_ts(2));
            worker.step();
        });

        let old_val = row(42, 100);
        let new_val = row(42, 200);
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(old_val), new_ts(1), Diff::MINUS_ONE),
            (Ok(new_val), new_ts(1), Diff::ONE),
        ];
        assert_eq!(actual, expected);
    }

    /// Delete a key that exists in the spine.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn delete_existing_key() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(7);
            let val = row(7, 77);

            input.send(((k, Some(Ok(val.clone())), 1), new_ts(0), Diff::ONE));
            input.advance_to(new_ts(1));
            worker.step();

            persist.send((Ok(val), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            input.send(((k, None, 2), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();

            persist.advance_to(new_ts(2));
            worker.step();
        });

        let val = row(7, 77);
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(val.clone()), new_ts(0), Diff::ONE),
            (Ok(val), new_ts(1), Diff::MINUS_ONE),
        ];
        assert_eq!(actual, expected);
    }

    /// Multiple persist feedback batches (unconsolidated snapshot). The spine
    /// consolidates across batches automatically.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn multi_batch_rehydration() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(5);
            let old_val = row(5, 10);
            let new_val = row(5, 20);
            let updated_val = row(5, 30);

            // Unconsolidated persist data: old_val +1, old_val -1, new_val +1
            // All sent before frontier advance, arriving in one batch.
            persist.send((Ok(old_val.clone()), new_ts(0), Diff::ONE));
            persist.send((Ok(old_val), new_ts(0), Diff::MINUS_ONE));
            persist.send((Ok(new_val), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            input.send(((k, Some(Ok(updated_val)), 1), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();

            persist.advance_to(new_ts(2));
            worker.step();
        });

        let new_val = row(5, 20);
        let updated_val = row(5, 30);
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(new_val), new_ts(1), Diff::MINUS_ONE),
            (Ok(updated_val), new_ts(1), Diff::ONE),
        ];
        assert_eq!(actual, expected);
    }

    /// Deleting a key that doesn't exist should produce no output.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn delete_nonexistent_key() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(99);

            // Persist has no data, frontier advances.
            persist.advance_to(new_ts(1));
            worker.step();

            // Source deletes a key that was never inserted.
            input.send(((k, None, 1), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();

            persist.advance_to(new_ts(2));
            worker.step();
        });

        assert!(actual.is_empty(), "expected empty output, got: {actual:?}");
    }

    /// Full lifecycle: insert → confirm → delete → confirm → re-insert.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn reinsert_after_delete() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(3);
            let val_a = row(3, 10);
            let val_b = row(3, 20);

            // Insert val_a at T=0.
            input.send(((k, Some(Ok(val_a.clone())), 1), new_ts(0), Diff::ONE));
            input.advance_to(new_ts(1));
            worker.step();

            persist.send((Ok(val_a.clone()), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            // Delete at T=1.
            input.send(((k, None, 2), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();

            // Persist confirms the delete.
            persist.send((Ok(val_a), new_ts(1), Diff::MINUS_ONE));
            persist.advance_to(new_ts(2));
            worker.step();

            // Re-insert with a different value at T=2.
            input.send(((k, Some(Ok(val_b.clone())), 3), new_ts(2), Diff::ONE));
            input.advance_to(new_ts(3));
            worker.step();

            persist.advance_to(new_ts(3));
            worker.step();
        });

        let val_a = row(3, 10);
        let val_b = row(3, 20);
        let mut expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(val_a.clone()), new_ts(0), Diff::ONE),
            (Ok(val_a), new_ts(1), Diff::MINUS_ONE),
            (Ok(val_b), new_ts(2), Diff::ONE),
        ];
        expected.sort();
        let mut actual = actual;
        actual.sort();
        assert_eq!(actual, expected);
    }

    /// Sending the same value that's already in state should produce a
    /// retraction + insertion that consolidates to zero net change.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn idempotent_update() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(11);
            let val = row(11, 50);

            input.send(((k, Some(Ok(val.clone())), 1), new_ts(0), Diff::ONE));
            input.advance_to(new_ts(1));
            worker.step();

            persist.send((Ok(val.clone()), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            // "Update" to the same value at T=1.
            input.send(((k, Some(Ok(val.clone())), 2), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();

            persist.advance_to(new_ts(2));
            worker.step();
        });

        let val = row(11, 50);
        // retract(val) + insert(val) at T=1 cancel after consolidation.
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(val), new_ts(0), Diff::ONE),
        ];
        assert_eq!(actual, expected);
    }
}
