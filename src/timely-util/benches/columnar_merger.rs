// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Microbenchmark comparing the columnation-backed `ColInternalMerger`
//! against the column-backed `ColumnMerger` on the merge-batcher's hot path.
//!
//! Each iteration drives a single 2-input merge through the framework's
//! `InternalMerger::merge`, so we measure the same code path either merger
//! ends up traveling at the trace level. Three input regimes:
//!
//! - **mixed**: identical key range; left and right interleave throughout.
//! - **collisions**: small key range; lots of equal-key consolidation.
//! - **disjoint**: non-overlapping key ranges; one side dominates locally,
//!   so the column merger's galloping should pay off.
//!
//! TODO: add a `Row`-keyed variant once `Rows` (the columnar container for
//! `Row`) is reachable from a dev-dep — currently it lives in a private
//! module inside `mz-repr` and isn't re-exported. The expected payoff there
//! is significant: variable-length keys amortize per-leaf trait dispatch
//! over more per-row work, where parallel-array layout becomes more
//! competitive with `ColumnationStack`'s flat-slice layout.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use differential_dataflow::trace::implementations::merge_batcher::Merger;
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::batcher::ColumnMerger;
use mz_timely_util::columnation::{ColInternalMerger, ColumnationStack};
use rand::{Rng, SeedableRng, rngs::StdRng};
use timely::container::PushInto;

type Data = (u64, u64);
type Time = u64;
type Diff = i64;
type Tuple = (Data, Time, Diff);

/// Generate a sorted+consolidated `Vec<Tuple>` of approximately `n` records,
/// with keys drawn uniformly from `[0, key_range)` and times from
/// `[0, time_range)`. The exact length will typically be smaller than `n` due
/// to consolidation.
fn make(seed: u64, n: usize, key_range: u64, time_range: u64) -> Vec<Tuple> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut raw: Vec<Tuple> = (0..n)
        .map(|_| {
            (
                (
                    rng.random_range(0..key_range),
                    rng.random_range(0..key_range),
                ),
                rng.random_range(0..time_range),
                rng.random_range(-3i64..=3),
            )
        })
        .collect();
    raw.sort();
    let mut out: Vec<Tuple> = Vec::new();
    for (d, t, r) in raw {
        if let Some(last) = out.last_mut() {
            if last.0 == d && last.1 == t {
                last.2 += r;
                continue;
            }
        }
        out.push((d, t, r));
    }
    out.retain(|x| x.2 != 0);
    out
}

fn build_columnation(data: &[Tuple]) -> ColumnationStack<Tuple> {
    let mut stack: ColumnationStack<Tuple> = ColumnationStack::with_capacity(data.len());
    for &tup in data {
        stack.push_into(tup);
    }
    stack
}

fn build_column(data: &[Tuple]) -> Column<Tuple> {
    let mut col: Column<Tuple> = Default::default();
    for &tup in data {
        col.push_into(tup);
    }
    col
}

fn bench_merge(c: &mut Criterion) {
    let n = 10_000usize;
    let configs: [(&str, Vec<Tuple>, Vec<Tuple>); 3] = [
        // Same wide key range on both sides → records interleave.
        (
            "mixed",
            make(1, n, 2 * n as u64, 4),
            make(2, n, 2 * n as u64, 4),
        ),
        // Tight key + time ranges → many records map to the same `(d, t)`,
        // exercising the equal-key diff-consolidation branch.
        (
            "collisions",
            make(3, n, (n / 4) as u64, 2),
            make(4, n, (n / 4) as u64, 2),
        ),
        // Left in [0, n), right in [n, 2n) → no overlap. Each `Less`/`Greater`
        // run extends to the end of its side; galloping should win here.
        (
            "disjoint",
            make(5, n, n as u64, 4),
            make(6, n, n as u64, 4)
                .into_iter()
                .map(|((k1, k2), t, r)| ((k1 + n as u64, k2 + n as u64), t, r))
                .collect(),
        ),
    ];

    let mut group = c.benchmark_group("merge_two_sorted");

    for (name, a, b) in &configs {
        group.throughput(Throughput::Elements((a.len() + b.len()) as u64));

        group.bench_with_input(BenchmarkId::new("columnation", name), &(), |bencher, _| {
            bencher.iter_batched(
                || (build_columnation(a), build_columnation(b)),
                |(ca, cb)| {
                    let mut merger: ColInternalMerger<Data, Time, Diff> = Default::default();
                    let mut output = Vec::new();
                    let mut stash = Vec::new();
                    merger.merge(vec![ca], vec![cb], &mut output, &mut stash);
                    output
                },
                BatchSize::LargeInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("column", name), &(), |bencher, _| {
            bencher.iter_batched(
                || (build_column(a), build_column(b)),
                |(ca, cb)| {
                    let mut merger: ColumnMerger<Data, Time, Diff> = Default::default();
                    let mut output = Vec::new();
                    let mut stash = Vec::new();
                    merger.merge(vec![ca], vec![cb], &mut output, &mut stash);
                    output
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_merge);
criterion_main!(benches);
