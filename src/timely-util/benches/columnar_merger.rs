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
//! ends up traveling at the trace level.
//!
//! Two axes:
//!
//! - **regime** — the input distribution shape:
//!   - *mixed*: identical key range; left and right interleave throughout.
//!   - *collisions*: small key range; lots of equal-key consolidation.
//!   - *disjoint*: non-overlapping key ranges; one side dominates locally,
//!     so the column merger's galloping should pay off.
//!
//! - **size** — the per-side heap footprint, swept across targets that
//!   bracket the cache hierarchy. Each merger's curve crosses the cache
//!   tiers at its own size, and a single fixed `n` would only show one
//!   point on each curve. Stating size in bytes-per-side (rather than
//!   element count) keeps the comparison meaningful when the record type
//!   changes — a future `Row`-keyed variant exercises the same heap budget
//!   even though its element count is smaller.
//!
//! TODO: add a `Row`-keyed variant once `Rows` (the columnar container for
//! `Row`) is reachable from a dev-dep — currently it lives in a private
//! module inside `mz-repr` and isn't re-exported.

use std::mem::size_of;

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

/// Per-side heap footprints to sweep across. Targets one point inside each
/// cache tier on Apple Silicon (L1d ≈ 64–128 KiB per core, shared L2 a few
/// MiB, then DRAM); on x86 the same picks straddle similar boundaries.
/// Element counts are derived in `bench_merge` from `size_of::<Tuple>()`.
const SIZES: &[(&str, usize)] = &[
    ("32K", 32 * 1024),
    ("512K", 512 * 1024),
    ("8M", 8 * 1024 * 1024),
    ("128M", 128 * 1024 * 1024),
];

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

/// Build the three regime input pairs at the given per-side element count.
/// Key/time ranges scale with `n` so the regime properties (interleaving,
/// collision density, disjoint-ness) hold across sizes.
fn configs(n: usize) -> [(&'static str, Vec<Tuple>, Vec<Tuple>); 3] {
    [
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
        // Left in `[0, n)`, right in `[n, 2n)` → no overlap. Each
        // `Less`/`Greater` run extends to the end of its side; galloping
        // should win here.
        (
            "disjoint",
            make(5, n, n as u64, 4),
            make(6, n, n as u64, 4)
                .into_iter()
                .map(|((k1, k2), t, r)| ((k1 + n as u64, k2 + n as u64), t, r))
                .collect(),
        ),
    ]
}

fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_two_sorted");

    let bytes_per_record = size_of::<Tuple>();

    for (size_label, bytes_per_side) in SIZES {
        let n = bytes_per_side / bytes_per_record;
        let cfgs = configs(n);

        for (regime, a, b) in &cfgs {
            // Throughput in bytes; criterion converts to elements/s when we
            // divide by record size at report time. Bytes is the more
            // meaningful unit when comparing across record types.
            let bytes = u64::try_from((a.len() + b.len()) * bytes_per_record).unwrap();
            group.throughput(Throughput::Bytes(bytes));

            let id = format!("{regime}/{size_label}");

            group.bench_with_input(
                BenchmarkId::new("columnation", &id),
                &(),
                |bencher, _| {
                    bencher.iter_batched(
                        || (build_columnation(a), build_columnation(b)),
                        |(ca, cb)| {
                            let mut merger: ColInternalMerger<Data, Time, Diff> =
                                Default::default();
                            let mut output = Vec::new();
                            let mut stash = Vec::new();
                            merger.merge(vec![ca], vec![cb], &mut output, &mut stash);
                            output
                        },
                        BatchSize::LargeInput,
                    );
                },
            );

            group.bench_with_input(BenchmarkId::new("column", &id), &(), |bencher, _| {
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
    }

    group.finish();
}

criterion_group!(benches, bench_merge);
criterion_main!(benches);
