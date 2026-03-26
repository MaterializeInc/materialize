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

use std::fmt::Debug;
use std::sync::Arc;

use differential_dataflow::difference::{IsZero, Semigroup};
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::agent::TraceAgent;
use differential_dataflow::trace::implementations::ValSpine;
use differential_dataflow::trace::{Cursor, TraceReader};
use differential_dataflow::{AsCollection, VecCollection};
use mz_repr::{Diff, GlobalId, Row};
use mz_storage_types::errors::{DataflowError, EnvelopeError};
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use std::convert::Infallible;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{Capability, CapabilitySet};
use timely::dataflow::{Scope, StreamVec};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use crate::healthcheck::HealthStatusUpdate;
use crate::metrics::upsert::UpsertMetrics;
use crate::upsert::UpsertKey;
use crate::upsert::UpsertValue;

// ── Source stash diff type ───────────────────────────────────────────────────
// The source stash uses a custom diff type that carries the upsert payload.
// The Semigroup implementation does "max FromTime wins" — when two updates for
// the same (key, time) are consolidated, the one with the higher FromTime
// (latest Kafka offset) is kept.

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct UpsertDiff<O> {
    from_time: O,
    value: Option<UpsertValue>,
}

impl<O> IsZero for UpsertDiff<O> {
    fn is_zero(&self) -> bool {
        false
    }
}

impl<O: Ord + Clone> Semigroup for UpsertDiff<O> {
    fn plus_equals(&mut self, rhs: &Self) {
        if rhs.from_time > self.from_time {
            *self = rhs.clone();
        }
    }
}

/// Transforms a stream of upserts (key-value updates) into a differential
/// collection.
///
/// Persist feedback is arranged into a differential trace (DD manages the
/// spine lifecycle). Source input is stashed with a custom `UpsertDiff`
/// Semigroup that deduplicates by keeping the highest FromTime per (key, time).
///
/// Has two inputs:
///   1. **Source input** — upsert commands from the external source.
///   2. **Persist input** — feedback of the operator's own output, read back
///      from persist.  Arranged into a trace for cursor-based lookups.
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
    G::Timestamp: Refines<mz_repr::Timestamp> + TotalOrder + Lattice + Sync,
    FromTime: Debug + timely::ExchangeData + Clone + Ord + Sync,
{
    // ── Arrange persist feedback ────────────────────────────────────────
    // Extract (UpsertKey, UpsertValue) from the persist feedback collection
    // and arrange it. DD manages the spine, batching, and compaction.
    let persist_keyed = persist_input.flat_map(move |result| {
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
    let persist_arranged = persist_keyed.arrange_by_key();
    let mut persist_trace = persist_arranged.trace.clone();

    // Probe the persist arrangement's stream for frontier tracking.
    // This replaces receiving the batch stream as an input — we just
    // read the probe frontier to know when persist has caught up.
    use timely::dataflow::operators::Probe;
    let (persist_probe, _persist_probe_stream) = persist_arranged.stream.probe();

    // ── Build the async processing operator ─────────────────────────────
    let mut builder = AsyncOperatorBuilder::new("Upsert".to_string(), input.scope());

    let (output_handle, output) = builder.new_output::<CapacityContainerBuilder<_>>();
    let (_snapshot_handle, snapshot_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<Infallible>>>();
    let (_health_output, health_stream) = builder
        .new_output::<CapacityContainerBuilder<Vec<(Option<GlobalId>, HealthStatusUpdate)>>>();

    let mut input = builder.new_input_for(
        input.inner,
        Exchange::new(move |((key, _, _), _, _)| UpsertKey::hashed(key)),
        &output_handle,
    );

    // We still need the persist stream as an input so the operator wakes
    // when the persist arrangement produces batches (frontier advances).
    // We read the actual frontier from the probe though.
    let mut persist_wakeup = builder.new_disconnected_input(_persist_probe_stream, Pipeline);

    let upsert_shared_metrics = Arc::clone(&upsert_metrics.shared);
    let _ = upsert_shared_metrics;

    let shutdown_button = builder.build(move |caps| async move {
        let _persist_token = persist_token;

        let [output_cap, snapshot_cap, _health_cap]: [_; 3] = caps.try_into().unwrap();
        drop(output_cap);
        let mut snapshot_cap = CapabilitySet::from_elem(snapshot_cap);

        let mut hydrating = true;

        // Source stash with UpsertDiff Semigroup for dedup.
        let mut stash: Vec<(UpsertKey, G::Timestamp, UpsertDiff<FromTime>)> = vec![];
        let mut stash_cap: Option<Capability<G::Timestamp>> = None;
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());

        let mut output_updates = vec![];
        let snapshot_start = std::time::Instant::now();
        let mut prev_persist_upper = Antichain::from_elem(Timestamp::minimum());

        loop {
            // Wait for either source input or persist frontier changes.
            tokio::select! {
                _ = input.ready() => {}
                _ = persist_wakeup.ready() => {
                    // Drain events so the input doesn't accumulate.
                    while persist_wakeup.next_sync().is_some() {}
                }
            }

            // ── Read source input ───────────────────────────────────────
            while let Some(event) = input.next_sync() {
                match event {
                    AsyncEvent::Data(cap, data) => {
                        for ((key, value, from_time), ts, diff) in data {
                            assert!(diff.is_positive(), "invalid upsert input");
                            if PartialOrder::less_equal(&input_upper, &resume_upper)
                                && !resume_upper.less_equal(&ts)
                            {
                                continue;
                            }
                            stash.push((key, ts, UpsertDiff { from_time, value }));
                        }
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

            if stash.len() > 10_000 {
                differential_dataflow::consolidation::consolidate_updates(&mut stash);
            }

            // ── Read persist frontier from probe ────────────────────────
            let persist_upper = persist_probe.with_frontier(|f| f.to_owned());

            if persist_upper != prev_persist_upper {
                let last_rehydration_chunk =
                    hydrating && PartialOrder::less_equal(&resume_upper, &persist_upper);

                if last_rehydration_chunk {
                    hydrating = false;
                    upsert_metrics
                        .rehydration_latency
                        .set(snapshot_start.elapsed().as_secs_f64());
                    tracing::info!(
                        worker_id = %source_config.worker_id,
                        source_id = %source_config.id,
                        "upsert finished rehydration",
                    );
                    snapshot_cap.downgrade(&[]);
                }

                let _ = snapshot_cap.try_downgrade(persist_upper.iter());

                // Compact the trace so the spine can merge old batches.
                persist_trace.set_logical_compaction(persist_upper.borrow());
                persist_trace.set_physical_compaction(persist_upper.borrow());

                prev_persist_upper = persist_upper.clone();
            }

            // ── Drain eligible source updates ───────────────────────────
            if !stash.is_empty() {
                let cap = stash_cap.as_mut().expect("capability for non-empty stash");

                differential_dataflow::consolidation::consolidate_updates(&mut stash);

                drain_staged_input(
                    &mut stash,
                    &mut output_updates,
                    &input_upper,
                    &persist_upper,
                    &mut persist_trace,
                    &source_config,
                );

                for (update, ts, diff) in output_updates.drain(..) {
                    output_handle.give(cap, (update, ts, diff));
                }

                if stash.is_empty() {
                    stash_cap = None;
                } else {
                    let min_ts = stash.iter().map(|(_, ts, _)| ts).min().unwrap().clone();
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

/// Drain source updates whose timestamp is complete and whose persist state is
/// caught up. Uses cursor-based lookups on the persist trace.
///
/// The stash must be pre-consolidated (sorted by (key, time) with the
/// UpsertDiff Semigroup having deduped by FromTime).
fn drain_staged_input<T, FromTime>(
    stash: &mut Vec<(UpsertKey, T, UpsertDiff<FromTime>)>,
    output: &mut Vec<(UpsertValue, T, Diff)>,
    input_upper: &Antichain<T>,
    persist_upper: &Antichain<T>,
    trace: &mut TraceAgent<ValSpine<UpsertKey, UpsertValue, T, Diff>>,
    source_config: &crate::source::SourceExportCreationConfig,
) where
    T: TotalOrder + Lattice + timely::ExchangeData + Timestamp + Clone + Debug + Ord + Sync,
    FromTime: timely::ExchangeData + Clone + Ord + Sync,
{
    let eligible: Vec<_> = stash
        .extract_if(.., |(_, ts, _)| {
            !input_upper.less_equal(ts) && !persist_upper.less_than(ts)
        })
        .filter(|(_, ts, _)| persist_upper.less_equal(ts))
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

    // Eligible entries are sorted by (key, time) from consolidation.
    // The trace cursor moves forward through keys, matching this order.
    let (mut cursor, storage) = trace.cursor();

    for (key, ts, upsert_diff) in eligible {
        // Look up the current value for this key in the persist trace.
        // For ValSpine with Vector layout, Key<'a> = &'a UpsertKey.
        cursor.seek_key(&storage, &key);
        let old_value = if cursor.get_key(&storage) == Some(&key) {
            let mut result = None;
            while let Some(val) = cursor.get_val(&storage) {
                let mut count = Diff::ZERO;
                cursor.map_times(&storage, |_time, diff| {
                    count += diff.clone();
                });
                if count.is_positive() {
                    assert!(
                        count == 1.into(),
                        "unexpected multiple entries for the same key in persist trace"
                    );
                    result = Some(val.clone());
                }
                cursor.step_val(&storage);
            }
            result
        } else {
            None
        };

        match upsert_diff.value {
            Some(new_val) => {
                if let Some(old_val) = old_value {
                    output.push((old_val, ts.clone(), Diff::MINUS_ONE));
                }
                output.push((new_val, ts, Diff::ONE));
            }
            None => {
                if let Some(old_val) = old_value {
                    output.push((old_val, ts, Diff::MINUS_ONE));
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
            input.send_batch(&mut vec![(
                (key0, Some(Ok(value4)), 4),
                new_ts(3),
                Diff::ONE,
            )]);
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

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn out_of_order_keys_across_timestamps() {
        let actual = upsert_test!(|input, persist, worker| {
            let key_high = key(99);
            let key_low = key(1);
            let val_a = row(99, 1);
            let val_b = row(1, 2);

            input.send(((key_high, Some(Ok(val_a.clone())), 1), new_ts(0), Diff::ONE));
            input.advance_to(new_ts(1));
            worker.step();
            persist.send((Ok(val_a.clone()), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            input.send(((key_low, Some(Ok(val_b.clone())), 2), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();
            persist.send((Ok(val_b.clone()), new_ts(1), Diff::ONE));
            persist.advance_to(new_ts(2));
            worker.step();

            let val_a2 = row(99, 10);
            let val_b2 = row(1, 20);
            input.send_batch(&mut vec![
                (
                    (key_high, Some(Ok(val_a2.clone())), 3),
                    new_ts(2),
                    Diff::ONE,
                ),
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
        let mut expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
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

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn multi_batch_rehydration() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(5);
            let old_val = row(5, 10);
            let new_val = row(5, 20);
            let updated_val = row(5, 30);

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

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn delete_nonexistent_key() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(99);

            persist.advance_to(new_ts(1));
            worker.step();

            input.send(((k, None, 1), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();
            persist.advance_to(new_ts(2));
            worker.step();
        });

        assert!(actual.is_empty(), "expected empty output, got: {actual:?}");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn reinsert_after_delete() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(3);
            let val_a = row(3, 10);
            let val_b = row(3, 20);

            input.send(((k, Some(Ok(val_a.clone())), 1), new_ts(0), Diff::ONE));
            input.advance_to(new_ts(1));
            worker.step();
            persist.send((Ok(val_a.clone()), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            input.send(((k, None, 2), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();
            persist.send((Ok(val_a), new_ts(1), Diff::MINUS_ONE));
            persist.advance_to(new_ts(2));
            worker.step();

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

            input.send(((k, Some(Ok(val.clone())), 2), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();
            persist.advance_to(new_ts(2));
            worker.step();
        });

        let val = row(11, 50);
        let expected: Vec<(Result<Row, DataflowError>, _, _)> =
            vec![(Ok(val), new_ts(0), Diff::ONE)];
        assert_eq!(actual, expected);
    }
}
