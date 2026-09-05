// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Micro-benchmark for the MV sink `Correction` buffer on hydration-style workloads,
//! i.e., workloads where the input catches up with the current time by passing through
//! many distinct timestamps.
//!
//! The scenario this models: an MV sink restarts with an old as-of and the desired
//! input replays through `T` distinct timestamps while persist writes (and thus
//! `advance_since`/`updates_before` calls) trail behind. Reads and since advancement
//! must only do work proportional to the drained slice, otherwise the catch-up
//! degenerates into quadratic behavior in `T`.
//!
//! Run with:
//!
//! ```text
//! cargo bench -p mz-compute --features bench --bench correction
//! ```

use std::hint::black_box;
use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main,
};
use mz_compute::sink::correction::Correction;
use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::metrics::{Metrics, SinkMetrics};
use mz_repr::{Datum, Diff, Row, Timestamp};
use timely::progress::Antichain;

/// Default value of `compute_correction_v2_chain_proportionality`.
const CHAIN_PROPORTIONALITY: f64 = 3.0;
/// Default value of `compute_correction_v2_chunk_size`.
const CHUNK_SIZE: usize = 8 * 1024;

/// Number of updates inserted per distinct timestamp.
const UPDATES_PER_TS: u64 = 16;

/// Time offset of far-future retractions in the temporal-filter pattern.
const TEMPORAL_OFFSET: u64 = 1 << 40;

/// The shape of the update stream fed into the correction buffer.
#[derive(Clone, Copy)]
enum Pattern {
    /// Every timestamp appends new, distinct rows. Nothing consolidates away.
    Append,
    /// Every timestamp updates the same set of keys: an addition for the new value and a
    /// retraction of the previous one. Retraction-heavy, consolidates down to a small set.
    Upsert,
    /// Every timestamp appends new rows accompanied by their far-future retractions, and deletes
    /// the previous timestamp's rows, retracting now and re-adding the far-future retraction at a
    /// slightly different future time. Models an MV behind a temporal filter (e.g. a last-30-days
    /// view): an ever-growing mass of far-future updates that never participates in reads.
    TemporalFilter,
}

impl Pattern {
    fn name(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Upsert => "upsert",
            Self::TemporalFilter => "temporal_filter",
        }
    }
}

fn sink_metrics() -> SinkMetrics {
    let registry = MetricsRegistry::new();
    let metrics = Metrics::new(&PersistConfig::new_for_tests(), &registry);
    metrics.sink.clone()
}

fn make_correction(metrics: &SinkMetrics) -> Correction<Row> {
    Correction::with_params(
        metrics.clone(),
        metrics.for_worker(0),
        None,
        CHAIN_PROPORTIONALITY,
        CHUNK_SIZE,
    )
}

fn row(key: u64, value: u64) -> Row {
    let payload = format!("payload-{value:016}");
    Row::pack_slice(&[Datum::UInt64(key), Datum::String(&payload)])
}

/// Generate one batch of updates per distinct timestamp `0..num_ts`.
fn make_batches(num_ts: u64, pattern: Pattern) -> Vec<Vec<(Row, Timestamp, Diff)>> {
    (0..num_ts)
        .map(|t| {
            let time = Timestamp::from(t);
            match pattern {
                Pattern::Append => (0..UPDATES_PER_TS)
                    .map(|i| (row(t * UPDATES_PER_TS + i, t), time, Diff::ONE))
                    .collect(),
                Pattern::Upsert => (0..UPDATES_PER_TS / 2)
                    .flat_map(|key| {
                        let addition = (row(key, t), time, Diff::ONE);
                        let retraction = t
                            .checked_sub(1)
                            .map(|prev| (row(key, prev), time, -Diff::ONE));
                        std::iter::once(addition).chain(retraction)
                    })
                    .collect(),
                Pattern::TemporalFilter => (0..UPDATES_PER_TS / 4)
                    .flat_map(|i| {
                        let key = t * (UPDATES_PER_TS / 4) + i;
                        // New row, plus its retraction when the temporal filter window closes.
                        let this = [
                            (row(key, t), time, Diff::ONE),
                            (
                                row(key, t),
                                Timestamp::from(t + TEMPORAL_OFFSET),
                                -Diff::ONE,
                            ),
                        ];
                        // Delete the previous timestamp's row: retract it now and cancel its
                        // window-close retraction. The cancellation lands at a different future
                        // time than the original retraction, so the far-future mass grows.
                        let prev = t.checked_sub(1).map(|p| {
                            let key = p * (UPDATES_PER_TS / 4) + i;
                            [
                                (row(key, p), time, -Diff::ONE),
                                (row(key, p), Timestamp::from(t + TEMPORAL_OFFSET), Diff::ONE),
                            ]
                        });
                        this.into_iter().chain(prev.into_iter().flatten())
                    })
                    .collect(),
            }
        })
        .collect()
}

/// Fill a fresh correction buffer with all batches, mimicking desired input that has
/// run far ahead of persist writes.
fn filled_correction(
    metrics: &SinkMetrics,
    batches: &[Vec<(Row, Timestamp, Diff)>],
) -> Correction<Row> {
    let mut correction = make_correction(metrics);
    for batch in batches {
        correction.insert(&mut batch.clone());
    }
    correction
}

/// Benchmark helpers modelling the `write_batches` operator's use of the buffer.
///
/// A trait rather than an inherent `impl`, because `Correction` is defined in another crate.
trait WriteSteps {
    /// Emulate one `write_batches` write: read the updates before `upper` and feed back their
    /// negations, like the persist input does.
    fn write_step(&mut self, upper: &Antichain<Timestamp>) -> usize;
    /// Read and count the updates before `upper`, without persist feedback.
    fn read_count(&mut self, upper: &Antichain<Timestamp>) -> usize;
}

impl WriteSteps for Correction<Row> {
    fn write_step(&mut self, upper: &Antichain<Timestamp>) -> usize {
        let mut written: Vec<_> = self.updates_before(upper).collect();
        let count = written.len();
        self.insert_negated(&mut written);
        count
    }

    fn read_count(&mut self, upper: &Antichain<Timestamp>) -> usize {
        self.updates_before(upper).count()
    }
}

/// Drain the correction buffer one timestamp at a time, like the `write_batches`
/// operator does when persist writes catch up step by step.
///
/// `updates_before` does not remove updates from the buffer. In the real sink, written
/// updates come back through the persist input and are removed by `insert_negated`. We
/// model that feedback here, otherwise the buffer re-emits all previous updates on every
/// step and clone costs drown out the structure-management costs we want to measure.
fn drain_stepwise(mut correction: Correction<Row>, num_ts: u64) -> Correction<Row> {
    for t in 0..num_ts {
        let upper = Antichain::from_elem(Timestamp::from(t + 1));
        let count = correction.write_step(&upper);
        black_box(count);
        correction.advance_since(upper);
    }
    correction
}

/// Advance the since across all buffered times at once, then consolidate and drain.
/// This exercises `Cursor::advance_by` with many distinct times below the since.
fn advance_jump(mut correction: Correction<Row>, num_ts: u64) -> Correction<Row> {
    correction.advance_since(Antichain::from_elem(Timestamp::from(num_ts)));
    correction.consolidate_at_since();
    let upper = Antichain::from_elem(Timestamp::from(num_ts + 1));
    let count = correction.read_count(&upper);
    black_box(count);
    correction
}

fn bench_scenario<F>(
    group: &mut BenchmarkGroup<WallTime>,
    metrics: &SinkMetrics,
    pattern: Pattern,
    num_ts: u64,
    routine: F,
) where
    F: Fn(Correction<Row>, u64) -> Correction<Row>,
{
    let batches = make_batches(num_ts, pattern);
    group.bench_function(BenchmarkId::from_parameter(num_ts), |b| {
        b.iter_batched(
            || filled_correction(metrics, &batches),
            |correction| routine(correction, num_ts),
            BatchSize::PerIteration,
        )
    });
}

/// Configure a benchmark group for short wall-clock time.
///
/// The interesting effects here are order-of-magnitude differences across problem sizes, so we
/// trade statistical rigor for execution speed.
fn configure(group: &mut BenchmarkGroup<WallTime>) {
    group
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
}

fn bench_correction(c: &mut Criterion) {
    let metrics = sink_metrics();
    let num_ts_values = [1024, 4096, 16384];

    for pattern in [Pattern::Append, Pattern::Upsert, Pattern::TemporalFilter] {
        let mut group = c.benchmark_group(format!("correction_drain_stepwise/{}", pattern.name()));
        configure(&mut group);
        for num_ts in num_ts_values {
            bench_scenario(&mut group, &metrics, pattern, num_ts, drain_stepwise);
        }
        group.finish();

        let mut group = c.benchmark_group(format!("correction_advance_jump/{}", pattern.name()));
        configure(&mut group);
        for num_ts in num_ts_values {
            bench_scenario(&mut group, &metrics, pattern, num_ts, advance_jump);
        }
        group.finish();

        let mut group = c.benchmark_group(format!("correction_insert/{}", pattern.name()));
        configure(&mut group);
        for num_ts in num_ts_values {
            let batches = make_batches(num_ts, pattern);
            group.bench_function(BenchmarkId::from_parameter(num_ts), |b| {
                b.iter_batched(
                    || (make_correction(&metrics), batches.clone()),
                    |(mut correction, mut batches)| {
                        for batch in &mut batches {
                            correction.insert(batch);
                        }
                        correction
                    },
                    BatchSize::PerIteration,
                )
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_correction);
criterion_main!(benches);
