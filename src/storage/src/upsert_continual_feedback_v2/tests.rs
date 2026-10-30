// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! No test drives errors through the persist feedback, so every harness below closes
//! the error input by dropping its handle.

use mz_ore::cast::CastLossy;
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

// The tests drive the operator with a plain integer `FromTime` standing in
// for a Kafka offset; project it to itself so dedup orders by it directly.
impl UpsertSourceTime for i32 {
    type Order = i32;
    fn upsert_order(&self) -> i32 {
        *self
    }
}

type Ts = (MzTimestamp, Subtime);

fn new_ts(ts: u64) -> Ts {
    (MzTimestamp::new(ts), Subtime::minimum())
}

/// `ts` refined to its `sub`-th subtime.
fn sub_ts(ts: u64, sub: u64) -> Ts {
    use timely::progress::PathSummary;
    let sub = (0..sub).fold(Subtime::minimum(), |acc, _| {
        Subtime::least_summary()
            .results_in(&acc)
            .expect("no overflow")
    });
    (MzTimestamp::new(ts), sub)
}

fn key(k: i64) -> UpsertKey {
    UpsertKey::from_key(Ok(&Row::pack_slice(&[Datum::Int64(k)])))
}

fn row(k: i64, v: i64) -> Row {
    Row::pack_slice(&[Datum::Int64(k), Datum::Int64(v)])
}

// Runs the test body once per stash flavor and asserts the two flavors
// produce identical (consolidated) output, so every scenario covers both
// operator arms. Returns one flavor's output for the caller's own
// expected-value assertion.
macro_rules! upsert_test {
    (|$input:ident, $persist:ident, $worker:ident| $body:block) => {
        upsert_test!(|$input, $persist, $worker, _registry| $body)
    };
    // `$registry` holds the operator's upsert metrics, for `$body` to read.
    (|$input:ident, $persist:ident, $worker:ident, $registry:ident| $body:block) => {{
        let run = |flavor: UpsertStashFlavor| {
            let $registry = MetricsRegistry::new();
            let output_handle = timely::execute_directly(move |$worker| {
                let (mut $input, mut $persist, output_handle) = $worker
                    .dataflow::<MzTimestamp, _, _>(|scope| {
                        scope.scoped::<Ts, _, _>("upsert", |scope| {
                            let (input_handle, input) = scope.new_input();
                            let (persist_handle, persist_input) = scope.new_input();
                            let (_persist_err_handle, persist_err_input) = scope.new_input();
                            let source_id = GlobalId::User(0);

                            let upsert_defs = UpsertMetricDefs::register_with(&$registry);
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
                                flavor,
                                input.as_collection(),
                                vec![0],
                                Antichain::from_elem(Timestamp::minimum()),
                                persist_input.as_collection(),
                                persist_err_input.as_collection(),
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
        };

        let paged = run(UpsertStashFlavor::Paged);
        let chunked = run(UpsertStashFlavor::Chunked);
        assert_eq!(paged, chunked, "stash flavors must produce equal output");
        chunked
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

        persist.send((value1, new_ts(0), Diff::ONE));
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
        persist.send((val_a.clone(), new_ts(0), Diff::ONE));
        persist.advance_to(new_ts(1));
        worker.step();

        input.send(((key_low, Some(Ok(val_b.clone())), 2), new_ts(1), Diff::ONE));
        input.advance_to(new_ts(2));
        worker.step();
        persist.send((val_b.clone(), new_ts(1), Diff::ONE));
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

/// A source tail spanning several timestamps ahead of persist drains one
/// timestamp per persist advance, each against the prior round's output.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn source_tail_ahead_of_persist() {
    let actual = upsert_test!(|input, persist, worker| {
        let k = key(7);
        let (v0, v1, v2) = (row(7, 0), row(7, 1), row(7, 2));

        input.send_batch(&mut vec![
            ((k, Some(Ok(v0.clone())), 1), new_ts(0), Diff::ONE),
            ((k, Some(Ok(v1.clone())), 2), new_ts(1), Diff::ONE),
            ((k, Some(Ok(v2.clone())), 3), new_ts(2), Diff::ONE),
        ]);
        input.advance_to(new_ts(3));
        worker.step();

        // The operator reads the persist frontier when input progress
        // wakes it, so each round advances both.
        persist.send((v0.clone(), new_ts(0), Diff::ONE));
        persist.advance_to(new_ts(1));
        worker.step();
        input.advance_to(new_ts(4));
        worker.step();

        persist.send((v0, new_ts(1), Diff::MINUS_ONE));
        persist.send((v1.clone(), new_ts(1), Diff::ONE));
        persist.advance_to(new_ts(2));
        worker.step();
        input.advance_to(new_ts(5));
        worker.step();

        persist.send((v1, new_ts(2), Diff::MINUS_ONE));
        persist.send((v2, new_ts(2), Diff::ONE));
        persist.advance_to(new_ts(3));
        worker.step();
        input.advance_to(new_ts(6));
        worker.step();
    });

    let (v0, v1, v2) = (row(7, 0), row(7, 1), row(7, 2));
    let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
        (Ok(v0.clone()), new_ts(0), Diff::ONE),
        (Ok(v0), new_ts(1), Diff::MINUS_ONE),
        (Ok(v1.clone()), new_ts(1), Diff::ONE),
        (Ok(v1), new_ts(2), Diff::MINUS_ONE),
        (Ok(v2), new_ts(2), Diff::ONE),
    ];
    assert_eq!(actual, expected);
}

/// A tail that arrives over several input frontier advances, so over
/// several batcher chains, drains in time order per key.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn source_tail_across_rounds() {
    let actual = upsert_test!(|input, persist, worker| {
        let (ka, kb) = (key(1), key(2));
        let (a0, a2, b1, b2) = (row(1, 0), row(1, 2), row(2, 1), row(2, 2));

        input.send(((ka, Some(Ok(a0.clone())), 1), new_ts(0), Diff::ONE));
        input.advance_to(new_ts(1));
        worker.step();
        input.send(((kb, Some(Ok(b1.clone())), 2), new_ts(1), Diff::ONE));
        input.advance_to(new_ts(2));
        worker.step();
        input.send_batch(&mut vec![
            ((ka, Some(Ok(a2.clone())), 3), new_ts(2), Diff::ONE),
            ((kb, Some(Ok(b2.clone())), 4), new_ts(2), Diff::ONE),
        ]);
        input.advance_to(new_ts(3));
        worker.step();

        // The operator reads the persist frontier when input progress
        // wakes it, so each round advances both.
        persist.send((a0.clone(), new_ts(0), Diff::ONE));
        persist.advance_to(new_ts(1));
        worker.step();
        input.advance_to(new_ts(4));
        worker.step();

        persist.send((b1.clone(), new_ts(1), Diff::ONE));
        persist.advance_to(new_ts(2));
        worker.step();
        input.advance_to(new_ts(5));
        worker.step();

        persist.send((a0, new_ts(2), Diff::MINUS_ONE));
        persist.send((a2, new_ts(2), Diff::ONE));
        persist.send((b1, new_ts(2), Diff::MINUS_ONE));
        persist.send((b2, new_ts(2), Diff::ONE));
        persist.advance_to(new_ts(3));
        worker.step();
        input.advance_to(new_ts(6));
        worker.step();
    });

    let (a0, a2, b1, b2) = (row(1, 0), row(1, 2), row(2, 1), row(2, 2));
    let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
        (Ok(a0.clone()), new_ts(0), Diff::ONE),
        (Ok(a0), new_ts(2), Diff::MINUS_ONE),
        (Ok(a2), new_ts(2), Diff::ONE),
        (Ok(b1.clone()), new_ts(1), Diff::ONE),
        (Ok(b1), new_ts(2), Diff::MINUS_ONE),
        (Ok(b2), new_ts(2), Diff::ONE),
    ];
    assert_eq!(actual, expected);
}

/// Two updates for one key at a time still open at the input frontier,
/// arriving in separate operator rounds, consolidate to the later one.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn open_time_across_rounds_consolidates() {
    let actual = upsert_test!(|input, persist, worker| {
        let k = key(1);
        // `send` buffers until `flush` or `advance_to`, and the operator
        // takes several steps to react to each change.
        input.advance_to(new_ts(1));
        input.send(((k, Some(Ok(row(1, 1))), 1), new_ts(1), Diff::ONE));
        input.flush();
        for _ in 0..5 {
            worker.step();
        }
        input.send(((k, Some(Ok(row(1, 2))), 2), new_ts(1), Diff::ONE));
        input.flush();
        for _ in 0..5 {
            worker.step();
        }
        input.advance_to(new_ts(2));
        for _ in 0..5 {
            worker.step();
        }
        persist.advance_to(new_ts(1));
        for _ in 0..5 {
            worker.step();
        }
        input.advance_to(new_ts(3));
        for _ in 0..5 {
            worker.step();
        }
    });
    let expected: Vec<(Result<Row, DataflowError>, _, _)> =
        vec![(Ok(row(1, 2)), new_ts(1), Diff::ONE)];
    assert_eq!(actual, expected);
}

/// Two updates for one key at a time the source sent ahead of the input
/// frontier, arriving in separate operator rounds, consolidate to the
/// later one.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn time_ahead_of_frontier_across_rounds_consolidates() {
    let actual = upsert_test!(|input, persist, worker| {
        let k = key(1);
        input.send(((k, Some(Ok(row(1, 1))), 1), new_ts(1), Diff::ONE));
        input.flush();
        for _ in 0..5 {
            worker.step();
        }
        input.advance_to(new_ts(1));
        for _ in 0..5 {
            worker.step();
        }
        input.send(((k, Some(Ok(row(1, 2))), 2), new_ts(1), Diff::ONE));
        input.flush();
        for _ in 0..5 {
            worker.step();
        }
        input.advance_to(new_ts(2));
        for _ in 0..5 {
            worker.step();
        }
        persist.advance_to(new_ts(1));
        for _ in 0..5 {
            worker.step();
        }
        input.advance_to(new_ts(3));
        for _ in 0..5 {
            worker.step();
        }
    });
    let expected: Vec<(Result<Row, DataflowError>, _, _)> =
        vec![(Ok(row(1, 2)), new_ts(1), Diff::ONE)];
    assert_eq!(actual, expected);
}

/// An input frontier inside a timestamp finalizes its earlier subtimes,
/// which drain one persist advance at a time.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn refined_input_frontier_drains_finalized_subtimes() {
    let actual = upsert_test!(|input, persist, worker| {
        let (a, b) = (row(1, 1), row(2, 1));
        input.send(((key(1), Some(Ok(a.clone())), 1), sub_ts(1, 0), Diff::ONE));
        input.send(((key(2), Some(Ok(b)), 2), sub_ts(1, 1), Diff::ONE));
        input.advance_to(sub_ts(1, 2));
        for _ in 0..5 {
            worker.step();
        }
        persist.advance_to(sub_ts(1, 0));
        for _ in 0..5 {
            worker.step();
        }
        persist.send((a, sub_ts(1, 0), Diff::ONE));
        persist.advance_to(sub_ts(1, 1));
        for _ in 0..5 {
            worker.step();
        }
        input.advance_to(new_ts(3));
        for _ in 0..5 {
            worker.step();
        }
    });
    let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
        (Ok(row(1, 1)), sub_ts(1, 0), Diff::ONE),
        (Ok(row(2, 1)), sub_ts(1, 1), Diff::ONE),
    ];
    assert_eq!(actual, expected);
}

/// Data far ahead of the frontier stays stashed through earlier rounds
/// and drains once persist reaches it.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn far_ahead_data_drains() {
    let actual = upsert_test!(|input, persist, worker| {
        input.send(((key(1), Some(Ok(row(1, 1))), 1), new_ts(2), Diff::ONE));
        input.send(((key(2), Some(Ok(row(2, 1))), 2), new_ts(5000), Diff::ONE));
        input.advance_to(new_ts(5001));
        for _ in 0..5 {
            worker.step();
        }
        persist.advance_to(new_ts(2));
        for _ in 0..5 {
            worker.step();
        }
        persist.advance_to(new_ts(5000));
        for _ in 0..5 {
            worker.step();
        }
        input.advance_to(new_ts(5002));
        for _ in 0..5 {
            worker.step();
        }
    });
    let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
        (Ok(row(1, 1)), new_ts(2), Diff::ONE),
        (Ok(row(2, 1)), new_ts(5000), Diff::ONE),
    ];
    assert_eq!(actual, expected);
}

/// An update at a later subtime of the persist frontier's timestamp is
/// extracted with the eligible one, pushed back, and drained once persist
/// reaches it.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn later_subtime_is_pushed_back_and_drained() {
    let actual = upsert_test!(|input, persist, worker, registry| {
        let (k, a, b) = (key(1), row(1, 1), row(1, 2));
        input.send(((k, Some(Ok(a.clone())), 1), sub_ts(1, 0), Diff::ONE));
        input.send(((k, Some(Ok(b)), 2), sub_ts(1, 1), Diff::ONE));
        input.advance_to(new_ts(2));
        for _ in 0..5 {
            worker.step();
        }
        persist.advance_to(sub_ts(1, 0));
        for _ in 0..5 {
            worker.step();
        }
        persist.send((a, sub_ts(1, 0), Diff::ONE));
        persist.advance_to(sub_ts(1, 1));
        for _ in 0..5 {
            worker.step();
        }
        input.advance_to(new_ts(3));
        for _ in 0..5 {
            worker.step();
        }

        let restashed: u64 = registry
            .gather()
            .iter()
            .filter(|family| family.name() == "mz_storage_upsert_restashed_total")
            .flat_map(|family| family.get_metric())
            .map(|metric| u64::cast_lossy(metric.get_counter().value()))
            .sum();
        assert_eq!(restashed, 1, "the later subtime is pushed back once");
    });

    let (a, b) = (row(1, 1), row(1, 2));
    let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
        (Ok(a.clone()), sub_ts(1, 0), Diff::ONE),
        (Ok(a), sub_ts(1, 1), Diff::MINUS_ONE),
        (Ok(b), sub_ts(1, 1), Diff::ONE),
    ];
    assert_eq!(actual, expected);
}

/// A source tail far ahead of persist, drained one timestamp per persist
/// advance, extracts only the timestamp being drained. Extracting the
/// whole tail each round would push it back every time, making catch-up
/// quadratic in the tail.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn tail_catch_up_restashes_nothing() {
    const TIMESTAMPS: u64 = 20;
    const PER_TS: i64 = 10;
    let actual = upsert_test!(|input, persist, worker, registry| {
        let mut offset = 0;
        for t in 0..TIMESTAMPS {
            let mut batch = Vec::new();
            for k in 0..PER_TS {
                offset += 1;
                let v = row(k, i64::from(offset));
                batch.push(((key(k), Some(Ok(v)), offset), new_ts(t), Diff::ONE));
            }
            input.send_batch(&mut batch);
            input.advance_to(new_ts(t + 1));
            worker.step();
        }

        // The operator reads the persist frontier when input progress
        // wakes it, so each round advances both.
        for t in 1..=TIMESTAMPS {
            persist.advance_to(new_ts(t));
            worker.step();
            input.advance_to(new_ts(TIMESTAMPS + t));
            worker.step();
        }

        let restashed: u64 = registry
            .gather()
            .iter()
            .filter(|family| family.name() == "mz_storage_upsert_restashed_total")
            .flat_map(|family| family.get_metric())
            .map(|metric| u64::cast_lossy(metric.get_counter().value()))
            .sum();
        assert_eq!(restashed, 0, "updates pushed back during catch-up");
    });

    // No feedback reaches persist, so every update drains as an insert.
    let inserts = actual.iter().filter(|(_, _, diff)| *diff == Diff::ONE);
    assert_eq!(
        u64::cast_from(inserts.count()),
        TIMESTAMPS * PER_TS.unsigned_abs()
    );
}

/// A persist frontier that jumps into the middle of a stashed tail drops
/// the entries it passed, drains the one it lands on, and keeps the rest
/// in the stash for later rounds without extracting them.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn persist_jump_into_tail() {
    let actual = upsert_test!(|input, persist, worker, registry| {
        for t in 2..=8 {
            let k = i64::try_from(t).expect("small");
            input.send(((key(k), Some(Ok(row(k, k))), 1), new_ts(t), Diff::ONE));
        }
        input.advance_to(new_ts(10));
        for _ in 0..5 {
            worker.step();
        }
        persist.advance_to(new_ts(5));
        for _ in 0..5 {
            worker.step();
        }
        for t in 6..=9 {
            persist.advance_to(new_ts(t));
            input.advance_to(new_ts(10 + t));
            for _ in 0..5 {
                worker.step();
            }
        }

        let restashed: u64 = registry
            .gather()
            .iter()
            .filter(|family| family.name() == "mz_storage_upsert_restashed_total")
            .flat_map(|family| family.get_metric())
            .map(|metric| u64::cast_lossy(metric.get_counter().value()))
            .sum();
        assert_eq!(restashed, 0, "the kept tail was extracted and pushed back");
    });
    let expected: Vec<(Result<Row, DataflowError>, _, _)> = (5..=8)
        .map(|t| {
            let k = i64::try_from(t).expect("small");
            (Ok(row(k, k)), new_ts(t), Diff::ONE)
        })
        .collect();
    assert_eq!(actual, expected);
}

/// A tail of several timestamps, each larger than one committed chunk,
/// drains correctly when the chunked stash spills, and each seal passes
/// the spilled chunks wholly past its upper through. The paged flavor
/// stays resident and is the reference.
///
/// Each timestamp's 25k updates fill one to two committed chunks. Loading
/// commits each chunk once and ages it into a compressed body once, and
/// a seal re-commits at most the two pieces of the chunk it splits, so
/// the pool sees about 3 inserts per timestamp (34 here). A seal that
/// rewrote every remaining chunk would add about one per remaining
/// timestamp per round, roughly 100 more over 12 timestamps.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn spilled_tail_drains_across_split_chunks() {
    use mz_ore::pool::Pool;
    use mz_timely_util::columnar::chunk::set_spill_override;

    let pool = Pool::new().expect("pool creation");
    set_spill_override(Some(pool.clone()));

    const TIMESTAMPS: u64 = 12;
    const KEYS: i64 = 25_000;
    let actual = upsert_test!(|input, persist, worker, registry| {
        let mut batch = Vec::new();
        let mut offset = 0;
        for t in 0..TIMESTAMPS {
            for k in 0..KEYS {
                offset += 1;
                let v = row(k, i64::from(offset));
                batch.push(((key(k), Some(Ok(v)), offset), new_ts(t), Diff::ONE));
            }
        }
        input.send_batch(&mut batch);
        input.advance_to(new_ts(TIMESTAMPS));
        for _ in 0..5 {
            worker.step();
        }
        for t in 1..=TIMESTAMPS {
            persist.advance_to(new_ts(t));
            input.advance_to(new_ts(TIMESTAMPS + t));
            for _ in 0..5 {
                worker.step();
            }
        }

        let restashed: u64 = registry
            .gather()
            .iter()
            .filter(|family| family.name() == "mz_storage_upsert_restashed_total")
            .flat_map(|family| family.get_metric())
            .map(|metric| u64::cast_lossy(metric.get_counter().value()))
            .sum();
        assert_eq!(restashed, 0, "updates pushed back during catch-up");
    });

    set_spill_override(None);
    let inserts = pool.stats().inserts;
    assert!(inserts > 0, "chunks should have spilled through the pool");
    assert!(
        inserts <= 5 * TIMESTAMPS,
        "{inserts} pool inserts: seals rewrote chunks past their upper"
    );

    // No feedback reaches persist, so every update drains as an insert.
    let mut expected: Vec<(Result<Row, DataflowError>, _, _)> = Vec::new();
    let mut offset = 0;
    for t in 0..TIMESTAMPS {
        for k in 0..KEYS {
            offset += 1;
            expected.push((Ok(row(k, i64::from(offset))), new_ts(t), Diff::ONE));
        }
    }
    let mut actual = actual;
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}

/// Drives an arm's stash batcher through a pseudo-random sequence of
/// pushes, seals and push-backs, checking it against a model that maps
/// each `(time, key)` to the greatest order pushed for it, which is what
/// consolidation keeps. Every seal must extract exactly the model's
/// entries below the upper, and the least time held, the batcher's
/// frontier plus the pushed-back entries, must match the model's.
fn stash_matches_model<A>(
    decode: impl Fn(Vec<<A::Batcher as Batcher>::Output>) -> Vec<(UpsertKey, Ts, i32)>,
) where
    A: UpsertStashArm<Ts, i32>,
{
    use std::collections::BTreeMap;

    let mut batcher = A::new_batcher();
    let mut chunker = A::Chunker::default();
    let mut model: BTreeMap<(Ts, UpsertKey), i32> = BTreeMap::new();
    let mut state: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next = move |n: u64| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) % n
    };
    let mut upper = sub_ts(0, 0);
    let mut order = 0;
    for _ in 0..2000 {
        match next(3) {
            0 | 1 => {
                // Source input arrives at or past the seal upper.
                let mut updates = Vec::new();
                for _ in 0..next(20) {
                    let outer = u64::from(upper.0) + next(40);
                    let ts = std::cmp::max(sub_ts(outer, next(3)), upper);
                    let k = key(i64::try_from(next(8)).expect("small"));
                    order += 1;
                    let kept = model.entry((ts, k)).or_insert(order);
                    *kept = std::cmp::max(*kept, order);
                    let diff = UpsertDiff {
                        from_time: order,
                        value: None,
                    };
                    updates.push((k, ts, diff));
                }
                A::flush(&mut updates, &mut chunker, &mut batcher);
            }
            _ => {
                let outer = u64::from(upper.0) + next(5);
                upper = std::cmp::max(sub_ts(outer, next(3)), upper);
                let (sealed, _description) = batcher.seal(Antichain::from_elem(upper));
                let mut least = batcher.frontier().to_owned();
                let mut extracted: Vec<_> = decode(sealed)
                    .into_iter()
                    .map(|(k, ts, order)| (ts, k, order))
                    .collect();
                extracted.sort();
                let mut expected = Vec::new();
                while let Some(entry) = model.first_entry()
                    && entry.key().0 < upper
                {
                    let ((ts, k), order) = entry.remove_entry();
                    expected.push((ts, k, order));
                }
                assert_eq!(extracted, expected, "seal at {upper:?}");
                // Push some back, as the drain does with ineligible updates.
                let mut pushed_back = Vec::new();
                for (ts, k, order) in extracted {
                    if next(3) == 0 {
                        model.insert((ts, k), order);
                        let diff = UpsertDiff {
                            from_time: order,
                            value: None,
                        };
                        pushed_back.push((k, ts, diff));
                        least.insert(ts);
                    }
                }
                A::flush(&mut pushed_back, &mut chunker, &mut batcher);
                assert_eq!(
                    least.as_option(),
                    model.keys().next().map(|(ts, _)| ts),
                    "least time held"
                );
            }
        }
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn paged_stash_matches_model() {
    stash_matches_model::<PagedArm>(|sealed| {
        use columnar::Index;
        let mut out = Vec::new();
        for column in &sealed {
            for (k, ts, diff) in column.borrow().into_index_iter() {
                let diff = <UpsertDiff<i32> as columnar::Columnar>::into_owned(diff);
                out.push((
                    *k,
                    <Ts as columnar::Columnar>::into_owned(ts),
                    diff.from_time,
                ));
            }
        }
        out
    });
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn chunked_stash_matches_model() {
    stash_matches_model::<ChunkedArm>(|sealed| {
        use columnar::{Index, Len};
        let mut out = Vec::new();
        for chunk in sealed {
            let body = chunk.into_body();
            let view = body.borrow();
            for index in 0..view.len() {
                let ((_, k), ts, diff) = view.get(index);
                let diff = <UpsertDiff<i32> as columnar::Columnar>::into_owned(diff);
                out.push((
                    *k,
                    <Ts as columnar::Columnar>::into_owned(ts),
                    diff.from_time,
                ));
            }
        }
        assert!(
            out.is_sorted_by_key(|(k, ts, _)| (*ts, *k)),
            "sealed chunks are time-major"
        );
        out
    });
}

#[mz_ore::test]
fn seal_upper_stops_past_persist_upper() {
    use timely::progress::PathSummary;
    let at = |t: u64, s: u64| {
        let sub = (0..s).fold(Subtime::minimum(), |acc, _| {
            Subtime::least_summary()
                .results_in(&acc)
                .expect("no overflow")
        });
        Antichain::from_elem((MzTimestamp::new(t), sub))
    };
    // The next outer timestamp, when the input frontier is past it.
    assert_eq!(seal_upper(&at(5, 3), &at(9, 0)), at(6, 0));
    // Capped at the input frontier, which may sit inside the outer timestamp.
    assert_eq!(seal_upper(&at(5, 0), &at(5, 7)), at(5, 7));
    assert_eq!(seal_upper(&at(5, 0), &at(6, 0)), at(6, 0));
    // An empty input frontier caps nothing. Times past the next outer
    // timestamp stay stashed, as they cannot be eligible this round.
    assert_eq!(seal_upper(&at(5, 0), &Antichain::new()), at(6, 0));
    // At the maximum outer timestamp there is no next one to stop at.
    assert_eq!(
        seal_upper(&at(u64::MAX, 2), &at(u64::MAX, 7)),
        at(u64::MAX, 7)
    );
    assert_eq!(
        seal_upper(&at(u64::MAX, 2), &Antichain::new()),
        Antichain::new()
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn rehydration_then_update() {
    let actual = upsert_test!(|input, persist, worker| {
        let k = key(42);
        let old_val = row(42, 100);
        let new_val = row(42, 200);

        persist.send((old_val, new_ts(0), Diff::ONE));
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
fn drain_crosses_probe_window() {
    // More distinct keys than one drain probe window holds, all eligible
    // at the same timestamp. The records fit one sealed chunk (the ship
    // threshold is ~2 MiB), so the drain must close a probe window
    // mid-chunk and carry keys correctly across the boundary.
    const KEYS: i64 = 1500;
    let actual = upsert_test!(|input, persist, worker| {
        for k in 0..KEYS {
            persist.send((row(k, k), new_ts(0), Diff::ONE));
        }
        persist.advance_to(new_ts(1));
        worker.step();

        for k in 0..KEYS {
            input.send(((key(k), Some(Ok(row(k, k + 1))), 1), new_ts(1), Diff::ONE));
        }
        input.advance_to(new_ts(2));
        worker.step();
        persist.advance_to(new_ts(2));
        worker.step();
    });

    let mut expected: Vec<(Result<Row, DataflowError>, _, _)> = Vec::new();
    for k in 0..KEYS {
        expected.push((Ok(row(k, k)), new_ts(1), Diff::MINUS_ONE));
        expected.push((Ok(row(k, k + 1)), new_ts(1), Diff::ONE));
    }
    let mut actual_sorted = actual;
    actual_sorted.sort();
    expected.sort();
    assert_eq!(actual_sorted, expected);
}

/// The probe-window scenario with committed chunks forced through a
/// private buffer pool, so the chunked flavor's seal and drain read
/// spilled bodies back through the pool codec rather than resident
/// memory. The override is thread-scoped and `execute_directly` runs the
/// worker on this thread, so it reaches the operator's batchers. The
/// paged flavor (which the harness also runs) routes through the column
/// pager rather than the chunk override, so it stays resident and serves
/// as the reference.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn drain_reads_spilled_chunks() {
    use mz_ore::pool::Pool;
    use mz_timely_util::columnar::chunk::set_spill_override;

    let pool = Pool::new().expect("pool creation");
    set_spill_override(Some(pool.clone()));

    const KEYS: i64 = 1500;
    let actual = upsert_test!(|input, persist, worker| {
        for k in 0..KEYS {
            persist.send((row(k, k), new_ts(0), Diff::ONE));
        }
        persist.advance_to(new_ts(1));
        worker.step();

        for k in 0..KEYS {
            input.send(((key(k), Some(Ok(row(k, k + 1))), 1), new_ts(1), Diff::ONE));
        }
        input.advance_to(new_ts(2));
        worker.step();
        persist.advance_to(new_ts(2));
        worker.step();
    });

    set_spill_override(None);
    assert!(
        pool.stats().inserts > 0,
        "chunks should have spilled through the pool"
    );

    let mut expected: Vec<(Result<Row, DataflowError>, _, _)> = Vec::new();
    for k in 0..KEYS {
        expected.push((Ok(row(k, k)), new_ts(1), Diff::MINUS_ONE));
        expected.push((Ok(row(k, k + 1)), new_ts(1), Diff::ONE));
    }
    let mut actual_sorted = actual;
    actual_sorted.sort();
    expected.sort();
    assert_eq!(actual_sorted, expected);
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
        persist.send((val, new_ts(0), Diff::ONE));
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

        persist.send((old_val.clone(), new_ts(0), Diff::ONE));
        persist.send((old_val, new_ts(0), Diff::MINUS_ONE));
        persist.send((new_val, new_ts(0), Diff::ONE));
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
        persist.send((val_a.clone(), new_ts(0), Diff::ONE));
        persist.advance_to(new_ts(1));
        worker.step();

        input.send(((k, None, 2), new_ts(1), Diff::ONE));
        input.advance_to(new_ts(2));
        worker.step();
        persist.send((val_a, new_ts(1), Diff::MINUS_ONE));
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
        persist.send((val.clone(), new_ts(0), Diff::ONE));
        persist.advance_to(new_ts(1));
        worker.step();

        input.send(((k, Some(Ok(val.clone())), 2), new_ts(1), Diff::ONE));
        input.advance_to(new_ts(2));
        worker.step();
        persist.advance_to(new_ts(2));
        worker.step();
    });

    let val = row(11, 50);
    let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![(Ok(val), new_ts(0), Diff::ONE)];
    assert_eq!(actual, expected);
}

/// Operator-level repro of the 0dt read-only-handoff stranding bug.
///
/// Models a lagging replacement generation: the external (old) writer has
/// already advanced the shard — and therefore the feedback `persist_upper`
/// — to `T = 10`, while the operator itself has emitted nothing. The
/// lagging replacement now produces source data at timestamps BELOW that
/// upper (`ts = 5, 7`), i.e. data the external writer has already persisted.
///
/// The drain DROPS such already-persisted data (it satisfies
/// neither `ts == persist_upper` nor `ts > persist_upper`), mirroring v1's
/// `relevant = persist_upper.less_equal(ts)`. Were it instead re-stashed,
/// the data would be stranded forever — `persist_upper` only advances, so
/// `ts == persist_upper` could never again hold — and `min_ineligible_ts`
/// would pin the operator's output capability at `ts = 5`, BELOW the shard
/// upper, where it would stay for good. Dropping it lets the frontier
/// advance freely.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn lagging_replacement_below_upper_strands_data() {
    for flavor in [UpsertStashFlavor::Paged, UpsertStashFlavor::Chunked] {
        let (frontier, emitted) = run_below_upper_scenario_v2(flavor);

        // The below-upper data is discarded (no output) and the output
        // frontier is not pinned below the shard upper (10); it advances
        // to the input upper (11), matching v1's behavior.
        assert!(
            emitted.is_empty(),
            "below-upper data should be dropped, not emitted; got {emitted:?} ({flavor:?})"
        );
        assert_eq!(
            frontier,
            vec![new_ts(11)],
            "v2 output frontier should advance to the input upper, not pin below \
             persist_upper ({flavor:?})"
        );
        assert!(
            frontier[0] >= new_ts(10),
            "v2 output frontier {frontier:?} should reach at least persist_upper (10) \
             ({flavor:?})"
        );
    }
}

/// Shared driver for the lagging-replacement scenario against v2. Returns
/// `(output_frontier, consolidated_emitted_updates)`.
fn run_below_upper_scenario_v2(
    flavor: UpsertStashFlavor,
) -> (Vec<Ts>, Vec<(Result<Row, DataflowError>, Ts, Diff)>) {
    use timely::dataflow::operators::Probe;

    let (frontier, capture) = timely::execute_directly(move |worker| {
        let (mut input, mut persist, probe, capture) =
            worker.dataflow::<MzTimestamp, _, _>(|scope| {
                scope.scoped::<Ts, _, _>("upsert", |scope| {
                    let (input_handle, input) = scope.new_input();
                    let (persist_handle, persist_input) = scope.new_input();
                    let (_persist_err_handle, persist_err_input) = scope.new_input();
                    let source_id = GlobalId::User(0);

                    let reg = MetricsRegistry::new();
                    let upsert_defs = UpsertMetricDefs::register_with(&reg);
                    let upsert_metrics = UpsertMetrics::new(&upsert_defs, source_id, 0, None);

                    let reg2 = MetricsRegistry::new();
                    let storage_metrics = StorageMetrics::register_with(&reg2);

                    let reg3 = MetricsRegistry::new();
                    let stats_defs = SourceStatisticsMetricDefs::register_with(&reg3);
                    let envelope = SourceEnvelope::Upsert(UpsertEnvelope {
                        source_arity: 2,
                        style: UpsertStyle::Default(KeyEnvelope::Flattened),
                        key_indices: vec![0],
                    });
                    let source_statistics = SourceStatistics::new(
                        source_id,
                        0,
                        &stats_defs,
                        source_id,
                        &ShardId::new(),
                        envelope,
                        Antichain::from_elem(Timestamp::minimum()),
                    );
                    let source_config = SourceExportCreationConfig {
                        id: source_id,
                        worker_id: 0,
                        metrics: storage_metrics,
                        source_statistics,
                    };

                    let (output, _, _, button) = upsert_inner(
                        flavor,
                        input.as_collection(),
                        vec![0],
                        Antichain::from_elem(Timestamp::minimum()),
                        persist_input.as_collection(),
                        persist_err_input.as_collection(),
                        None,
                        upsert_metrics,
                        source_config,
                    );
                    std::mem::forget(button);
                    let (probe, stream) = output.inner.probe();
                    (input_handle, persist_handle, probe, stream.capture())
                })
            });

        // The external writer has advanced the shard (feedback persist_upper)
        // to T = 10 WITHOUT the operator emitting anything itself.
        persist.advance_to(new_ts(10));
        for _ in 0..20 {
            worker.step();
        }

        // The lagging replacement produces source data at ts BELOW the
        // current persist_upper (5 and 7 while persist_upper = 10).
        input.send(((key(0), Some(Ok(row(0, 1))), 1), new_ts(5), Diff::ONE));
        input.send(((key(1), Some(Ok(row(1, 2))), 2), new_ts(7), Diff::ONE));
        input.advance_to(new_ts(11));
        for _ in 0..20 {
            worker.step();
        }

        (probe.with_frontier(|f| f.to_vec()), capture)
    });

    let mut emitted: Vec<_> = capture
        .extract()
        .into_iter()
        .flat_map(|(_cap, c)| c)
        .collect();
    differential_dataflow::consolidation::consolidate_updates(&mut emitted);
    (frontier, emitted)
}
