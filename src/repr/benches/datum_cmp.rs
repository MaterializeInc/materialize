// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for Datum comparison (Ord/PartialEq) with Numeric values.
//!
//! Measures the improvement from the manual Datum::Ord implementation that
//! uses fast_numeric_cmp() instead of OrderedDecimal::cmp (3 FFI calls).

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use dec::OrderedDecimal;
use mz_repr::adt::numeric::Numeric;
use mz_repr::Datum;

fn make_numeric(s: &str) -> OrderedDecimal<Numeric> {
    let mut cx = dec::Context::<Numeric>::default();
    let n = cx.parse(s).unwrap();
    OrderedDecimal(n)
}

/// Baseline: use OrderedDecimal::cmp directly (the old FFI path).
/// This simulates what the derived Datum::Ord used to do.
#[inline(never)]
fn old_ordered_decimal_cmp(
    a: &OrderedDecimal<Numeric>,
    b: &OrderedDecimal<Numeric>,
) -> std::cmp::Ordering {
    a.cmp(b)
}

fn bench_datum_cmp(c: &mut Criterion) {
    let mut group = c.benchmark_group("datum_cmp");

    let n42 = make_numeric("42");
    let n99 = make_numeric("99");
    let money_a = make_numeric("12345.67");
    let money_b = make_numeric("98765.43");
    let equal_a = make_numeric("42.00");
    let equal_b = make_numeric("42.00");
    let neg_a = make_numeric("-500.25");
    let neg_b = make_numeric("-100.50");
    let large_a = make_numeric("123456789012345678");
    let large_b = make_numeric("987654321098765432");

    // Old: OrderedDecimal::cmp (FFI path)
    group.bench_function("old_numeric_small_int", |b| {
        b.iter(|| black_box(old_ordered_decimal_cmp(&n42, &n99)));
    });
    group.bench_function("old_numeric_money", |b| {
        b.iter(|| black_box(old_ordered_decimal_cmp(&money_a, &money_b)));
    });
    group.bench_function("old_numeric_equal", |b| {
        b.iter(|| black_box(old_ordered_decimal_cmp(&equal_a, &equal_b)));
    });
    group.bench_function("old_numeric_negative", |b| {
        b.iter(|| black_box(old_ordered_decimal_cmp(&neg_a, &neg_b)));
    });
    group.bench_function("old_numeric_large_coeff", |b| {
        b.iter(|| black_box(old_ordered_decimal_cmp(&large_a, &large_b)));
    });
    group.bench_function("old_numeric_eq", |b| {
        b.iter(|| black_box(old_ordered_decimal_cmp(&n42, &n99) == std::cmp::Ordering::Equal));
    });
    group.bench_function("old_numeric_eq_equal", |b| {
        b.iter(|| black_box(old_ordered_decimal_cmp(&equal_a, &equal_b) == std::cmp::Ordering::Equal));
    });

    // New: Datum::cmp (fast_numeric_cmp path via manual Ord)
    group.bench_function("new_numeric_small_int", |b| {
        let da = Datum::Numeric(n42);
        let db = Datum::Numeric(n99);
        b.iter(|| black_box(da.cmp(&db)));
    });
    group.bench_function("new_numeric_money", |b| {
        let da = Datum::Numeric(money_a);
        let db = Datum::Numeric(money_b);
        b.iter(|| black_box(da.cmp(&db)));
    });
    group.bench_function("new_numeric_equal", |b| {
        let da = Datum::Numeric(equal_a);
        let db = Datum::Numeric(equal_b);
        b.iter(|| black_box(da.cmp(&db)));
    });
    group.bench_function("new_numeric_negative", |b| {
        let da = Datum::Numeric(neg_a);
        let db = Datum::Numeric(neg_b);
        b.iter(|| black_box(da.cmp(&db)));
    });
    group.bench_function("new_numeric_large_coeff", |b| {
        let da = Datum::Numeric(large_a);
        let db = Datum::Numeric(large_b);
        b.iter(|| black_box(da.cmp(&db)));
    });
    group.bench_function("new_numeric_eq", |b| {
        let da = Datum::Numeric(n42);
        let db = Datum::Numeric(n99);
        b.iter(|| black_box(da == db));
    });
    group.bench_function("new_numeric_eq_equal", |b| {
        let da = Datum::Numeric(equal_a);
        let db = Datum::Numeric(equal_b);
        b.iter(|| black_box(da == db));
    });

    // Non-Numeric baseline (should be unchanged)
    group.bench_function("int64_baseline", |b| {
        let da = Datum::Int64(42);
        let db = Datum::Int64(99);
        b.iter(|| black_box(da.cmp(&db)));
    });
    group.bench_function("string_baseline", |b| {
        let da = Datum::String("hello world");
        let db = Datum::String("hello xorld");
        b.iter(|| black_box(da.cmp(&db)));
    });
    group.bench_function("cross_variant", |b| {
        let da = Datum::Int64(42);
        let db = Datum::String("hello");
        b.iter(|| black_box(da.cmp(&db)));
    });

    group.finish();

    // Batch benchmarks
    let mut batch_group = c.benchmark_group("datum_cmp_batch");

    // Batch: 10k Numeric comparisons - old OrderedDecimal::cmp
    batch_group.bench_function("old_10k_numeric", |b| {
        let values: Vec<OrderedDecimal<Numeric>> = (0..10_000)
            .map(|i| make_numeric(&format!("{}.{:02}", i * 100 + 1, i % 100)))
            .collect();
        b.iter(|| {
            let mut count = 0u64;
            for i in 1..values.len() {
                if old_ordered_decimal_cmp(&values[i - 1], &values[i]) == std::cmp::Ordering::Less {
                    count += 1;
                }
                black_box(count);
            }
            count
        });
    });

    // Batch: 10k Numeric comparisons - new Datum::cmp
    batch_group.bench_function("new_10k_numeric", |b| {
        let values: Vec<OrderedDecimal<Numeric>> = (0..10_000)
            .map(|i| make_numeric(&format!("{}.{:02}", i * 100 + 1, i % 100)))
            .collect();
        let datums: Vec<Datum> = values.iter().map(|n| Datum::Numeric(*n)).collect();
        b.iter(|| {
            let mut count = 0u64;
            for i in 1..datums.len() {
                if datums[i - 1] < datums[i] {
                    count += 1;
                }
                black_box(count);
            }
            count
        });
    });

    // Batch: 10k Int64 comparisons (baseline, should be unchanged)
    batch_group.bench_function("10k_int64_baseline", |b| {
        let datums: Vec<Datum> = (0..10_000).map(|i| Datum::Int64(i)).collect();
        b.iter(|| {
            let mut count = 0u64;
            for i in 1..datums.len() {
                if datums[i - 1] < datums[i] {
                    count += 1;
                }
                black_box(count);
            }
            count
        });
    });

    batch_group.finish();
}

criterion_group!(benches, bench_datum_cmp);
criterion_main!(benches);
