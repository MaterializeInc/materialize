// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Benchmarks for `OffsetOptimized::index_pair()` and `DatumContainer::index()`.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use differential_dataflow::trace::implementations::BatchContainer;
use timely::container::PushInto;

use mz_compute::row_spine::{DatumContainer, OffsetOptimized};
use mz_repr::{Datum, Row};

/// Benchmark OffsetOptimized: two index() calls vs one index_pair() call.
fn bench_offset_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("offset_index");

    // Build an OffsetOptimized with uniform stride (common case: all rows same size)
    let n = 10_000usize;
    let stride = 48usize; // typical row size in bytes

    let mut offsets = OffsetOptimized::with_capacity(n + 1);
    for i in 0..=n {
        offsets.push_into(stride * i);
    }

    // Benchmark: two separate index() calls (old pattern)
    group.bench_function("two_index_calls_uniform", |b| {
        b.iter(|| {
            let mut sum = 0usize;
            for i in 0..n {
                let lower = offsets.index(black_box(i));
                let upper = offsets.index(black_box(i + 1));
                sum += upper - lower;
            }
            black_box(sum)
        })
    });

    // Benchmark: one index_pair() call (new pattern)
    group.bench_function("index_pair_uniform", |b| {
        b.iter(|| {
            let mut sum = 0usize;
            for i in 0..n {
                let (lower, upper) = offsets.index_pair(black_box(i));
                sum += upper - lower;
            }
            black_box(sum)
        })
    });

    // Build an OffsetOptimized with variable sizes (some rows spill to OffsetList)
    let mut offsets_var = OffsetOptimized::with_capacity(n + 1);
    let mut offset = 0usize;
    for i in 0..=n {
        offsets_var.push_into(offset);
        // Variable row sizes: 16-80 bytes
        offset += 16 + (i % 5) * 16;
    }

    // Variable sizes: two index() calls
    group.bench_function("two_index_calls_variable", |b| {
        b.iter(|| {
            let mut sum = 0usize;
            for i in 0..n {
                let lower = offsets_var.index(black_box(i));
                let upper = offsets_var.index(black_box(i + 1));
                sum += upper - lower;
            }
            black_box(sum)
        })
    });

    // Variable sizes: index_pair()
    group.bench_function("index_pair_variable", |b| {
        b.iter(|| {
            let mut sum = 0usize;
            for i in 0..n {
                let (lower, upper) = offsets_var.index_pair(black_box(i));
                sum += upper - lower;
            }
            black_box(sum)
        })
    });

    group.finish();
}

/// Benchmark DatumContainer::index() throughput (end-to-end).
fn bench_datum_container_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("datum_container_index");

    let n = 10_000usize;

    // Build container with uniform int rows (5 columns)
    let mut container_int5 = DatumContainer::with_capacity(n);
    for i in 0..n {
        let row = Row::pack(&[
            Datum::Int64(i as i64),
            Datum::Int64(i as i64 * 7),
            Datum::Int64(i as i64 * 13),
            Datum::Int64(i as i64 * 31),
            Datum::Int64(i as i64 * 97),
        ]);
        container_int5.push_own(&row);
    }

    group.bench_function("sequential_int5", |b| {
        b.iter(|| {
            for i in 0..n {
                black_box(container_int5.index(black_box(i)));
            }
        })
    });

    // Build container with mixed-type rows
    let mut container_mixed = DatumContainer::with_capacity(n);
    for i in 0..n {
        let row = Row::pack(&[
            Datum::Int64(i as i64),
            Datum::String("hello world"),
            Datum::Float64((i as f64 * 3.14).into()),
            Datum::True,
            Datum::Int32(i as i32),
        ]);
        container_mixed.push_own(&row);
    }

    group.bench_function("sequential_mixed5", |b| {
        b.iter(|| {
            for i in 0..n {
                black_box(container_mixed.index(black_box(i)));
            }
        })
    });

    // Build container with many small rows (single int datum)
    let mut container_narrow = DatumContainer::with_capacity(n);
    for i in 0..n {
        let row = Row::pack(&[Datum::Int64(i as i64)]);
        container_narrow.push_own(&row);
    }

    group.bench_function("sequential_narrow1", |b| {
        b.iter(|| {
            for i in 0..n {
                black_box(container_narrow.index(black_box(i)));
            }
        })
    });

    // Random access pattern
    let indices: Vec<usize> = (0..n).map(|i| (i * 7919) % n).collect();

    group.bench_function("random_int5", |b| {
        b.iter(|| {
            for &i in &indices {
                black_box(container_int5.index(black_box(i)));
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_offset_index, bench_datum_container_index);
criterion_main!(benches);
