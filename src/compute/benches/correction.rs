// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Micro-benchmark comparing `CorrectionV1` and `CorrectionV2` on the hydration-style workloads
//! from `mz_compute::sink::correction_workload`.
//!
//! Wall-clock is the measurement here. The deterministic complexity guard for the same workloads
//! lives in the `correction_v2` unit tests, which count structural work instead of time.
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
use mz_compute::sink::correction::CorrectionV1;
use mz_compute::sink::correction_v2::CorrectionV2;
use mz_compute::sink::correction_workload::{Pattern, make_batches};
use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::metrics::{Metrics, SinkMetrics};
use mz_repr::{Diff, Row, Timestamp};
use timely::progress::Antichain;

/// Default value of `compute_correction_v2_chain_proportionality`.
const CHAIN_PROPORTIONALITY: f64 = 3.0;
/// Default value of `compute_correction_v2_chunk_size`.
const CHUNK_SIZE: usize = 8 * 1024;
/// Default value of `consolidating_vec_growth_dampener`.
const GROWTH_DAMPENER: usize = 1;

#[derive(Clone, Copy)]
enum Version {
    V1,
    V2,
}

impl Version {
    fn name(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }
}

/// Local dispatch over the correction buffer implementations.
///
/// The production `Correction` enum only contains v1 and v2, so the bench carries its own.
enum Corr {
    V1(CorrectionV1<Row>),
    V2(CorrectionV2<Row>),
}

impl Corr {
    fn insert(&mut self, updates: &mut Vec<(Row, Timestamp, Diff)>) {
        match self {
            Self::V1(c) => c.insert(updates),
            Self::V2(c) => c.insert(updates),
        }
    }

    fn insert_negated(&mut self, updates: &mut Vec<(Row, Timestamp, Diff)>) {
        match self {
            Self::V1(c) => c.insert_negated(updates),
            Self::V2(c) => c.insert_negated(updates),
        }
    }

    fn updates_before(
        &mut self,
        upper: &Antichain<Timestamp>,
    ) -> Box<dyn Iterator<Item = (Row, Timestamp, Diff)> + '_> {
        match self {
            Self::V1(c) => Box::new(c.updates_before(upper)),
            Self::V2(c) => Box::new(c.updates_before(upper)),
        }
    }

    fn advance_since(&mut self, since: Antichain<Timestamp>) {
        match self {
            Self::V1(c) => c.advance_since(since),
            Self::V2(c) => c.advance_since(since),
        }
    }

    fn consolidate_at_since(&mut self) {
        match self {
            Self::V1(c) => c.consolidate_at_since(),
            Self::V2(c) => c.consolidate_at_since(),
        }
    }
}

fn sink_metrics() -> SinkMetrics {
    let registry = MetricsRegistry::new();
    let metrics = Metrics::new(&PersistConfig::new_for_tests(), &registry);
    metrics.sink.clone()
}

fn make_correction(version: Version, metrics: &SinkMetrics) -> Corr {
    let worker_metrics = metrics.for_worker(0);
    match version {
        Version::V1 => Corr::V1(CorrectionV1::new(
            metrics.clone(),
            worker_metrics,
            GROWTH_DAMPENER,
        )),
        Version::V2 => Corr::V2(CorrectionV2::new(
            metrics.clone(),
            worker_metrics,
            None,
            CHAIN_PROPORTIONALITY,
            CHUNK_SIZE,
        )),
    }
}

/// Fill a fresh correction buffer with all batches, mimicking desired input that has
/// run far ahead of persist writes.
fn filled_correction(
    version: Version,
    metrics: &SinkMetrics,
    batches: &[Vec<(Row, Timestamp, Diff)>],
) -> Corr {
    let mut correction = make_correction(version, metrics);
    for batch in batches {
        correction.insert(&mut batch.clone());
    }
    correction
}

impl Corr {
    /// Emulate one `write_batches` write: read the updates before `upper` and feed back their
    /// negations, like the persist input does.
    fn write_step(&mut self, upper: &Antichain<Timestamp>) -> usize {
        let mut written: Vec<_> = self.updates_before(upper).collect();
        let count = written.len();
        self.insert_negated(&mut written);
        count
    }

    /// Read and count the updates before `upper`, without persist feedback.
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
fn drain_stepwise(mut correction: Corr, num_ts: u64) -> Corr {
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
fn advance_jump(mut correction: Corr, num_ts: u64) -> Corr {
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
    F: Fn(Corr, u64) -> Corr,
{
    let batches = make_batches(num_ts, pattern);
    for version in [Version::V1, Version::V2] {
        group.bench_function(BenchmarkId::new(version.name(), num_ts), |b| {
            b.iter_batched(
                || filled_correction(version, metrics, &batches),
                |correction| routine(correction, num_ts),
                BatchSize::PerIteration,
            )
        });
    }
}

/// Configure a benchmark group for short wall-clock time.
///
/// The interesting effects here are order-of-magnitude differences between implementations, so we
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

    for pattern in Pattern::ALL {
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
            for version in [Version::V1, Version::V2] {
                group.bench_function(BenchmarkId::new(version.name(), num_ts), |b| {
                    b.iter_batched(
                        || (make_correction(version, &metrics), batches.clone()),
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
        }
        group.finish();
    }
}

criterion_group!(benches, bench_correction);
criterion_main!(benches);
