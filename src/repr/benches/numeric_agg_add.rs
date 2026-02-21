// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for NumericAgg (Decimal<27>) accumulator addition,
//! comparing the old FFI path (cx_agg.add + cx_agg.reduce) vs the
//! new fast path (try_add_numeric_agg).

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mz_repr::adt::numeric::{self, NumericAgg};

/// Old path: cx_agg.add() + cx_agg.reduce() = 2 C FFI calls.
fn add_agg_ffi(a: &mut NumericAgg, b: &NumericAgg) {
    let mut cx = numeric::cx_agg();
    cx.add(a, b);
    cx.reduce(a);
}

/// New path: try fast path first, fall back to FFI.
fn add_agg_fast(a: &mut NumericAgg, b: &NumericAgg) {
    if !numeric::try_add_numeric_agg(a, b) {
        let mut cx = numeric::cx_agg();
        cx.add(a, b);
        cx.reduce(a);
    }
}

fn bench_single_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_agg_add_single");
    let mut cx = numeric::cx_agg();

    // Small integers (exponent=0)
    let a_small: NumericAgg = cx.parse("42").unwrap();
    let b_small: NumericAgg = cx.parse("58").unwrap();

    group.bench_function("old_small_int", |bench| {
        bench.iter(|| {
            let mut a = a_small.clone();
            add_agg_ffi(black_box(&mut a), black_box(&b_small));
            a
        })
    });
    group.bench_function("new_small_int", |bench| {
        bench.iter(|| {
            let mut a = a_small.clone();
            add_agg_fast(black_box(&mut a), black_box(&b_small));
            a
        })
    });

    // Decimal values (same scale, 2 decimal places)
    let a_money: NumericAgg = cx.parse("1234567.89").unwrap();
    let b_money: NumericAgg = cx.parse("9876543.21").unwrap();

    group.bench_function("old_money", |bench| {
        bench.iter(|| {
            let mut a = a_money.clone();
            add_agg_ffi(black_box(&mut a), black_box(&b_money));
            a
        })
    });
    group.bench_function("new_money", |bench| {
        bench.iter(|| {
            let mut a = a_money.clone();
            add_agg_fast(black_box(&mut a), black_box(&b_money));
            a
        })
    });

    // Large coefficient (18 significant digits)
    let a_large: NumericAgg = cx.parse("999999999999999999").unwrap();
    let b_large: NumericAgg = cx.parse("1").unwrap();

    group.bench_function("old_large_coeff", |bench| {
        bench.iter(|| {
            let mut a = a_large.clone();
            add_agg_ffi(black_box(&mut a), black_box(&b_large));
            a
        })
    });
    group.bench_function("new_large_coeff", |bench| {
        bench.iter(|| {
            let mut a = a_large.clone();
            add_agg_fast(black_box(&mut a), black_box(&b_large));
            a
        })
    });

    group.finish();
}

fn bench_sum_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_agg_sum_batch");
    let mut cx = numeric::cx_agg();

    // Simulate SUM aggregation: accumulate 10k money-like values
    let values: Vec<NumericAgg> = (0..10000)
        .map(|i| {
            let s = format!("{}.{:02}", i * 3 - 5000, i % 100);
            cx.parse(&*s).unwrap()
        })
        .collect();

    group.bench_function("old_sum_money_10k", |bench| {
        bench.iter(|| {
            let mut acc = values[0].clone();
            for v in &values[1..] {
                add_agg_ffi(&mut acc, v);
            }
            black_box(acc)
        })
    });
    group.bench_function("new_sum_money_10k", |bench| {
        bench.iter(|| {
            let mut acc = values[0].clone();
            for v in &values[1..] {
                add_agg_fast(&mut acc, v);
            }
            black_box(acc)
        })
    });

    // Integer-only batch (best case - exponent=0)
    let int_values: Vec<NumericAgg> = (0..10000)
        .map(|i| {
            let s = format!("{}", i * 7 - 3000);
            cx.parse(&*s).unwrap()
        })
        .collect();

    group.bench_function("old_sum_ints_10k", |bench| {
        bench.iter(|| {
            let mut acc = int_values[0].clone();
            for v in &int_values[1..] {
                add_agg_ffi(&mut acc, v);
            }
            black_box(acc)
        })
    });
    group.bench_function("new_sum_ints_10k", |bench| {
        bench.iter(|| {
            let mut acc = int_values[0].clone();
            for v in &int_values[1..] {
                add_agg_fast(&mut acc, v);
            }
            black_box(acc)
        })
    });

    group.finish();
}

criterion_group!(benches, bench_single_add, bench_sum_batch);
criterion_main!(benches);
