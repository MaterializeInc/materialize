// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for numeric (decimal) arithmetic operations.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mz_repr::adt::numeric::{self, Numeric};

/// FFI-only addition (the old path): clone Context + C FFI call.
fn add_numeric_ffi(a: &Numeric, b: &Numeric) -> Numeric {
    let mut cx = numeric::cx_datum();
    let mut result = a.clone();
    cx.add(&mut result, b);
    result
}

/// FFI-only subtraction (the old path).
fn sub_numeric_ffi(a: &Numeric, b: &Numeric) -> Numeric {
    let mut cx = numeric::cx_datum();
    let mut result = a.clone();
    cx.sub(&mut result, b);
    result
}

/// Fast-path addition with FFI fallback (the new path).
fn add_numeric_fast(a: &Numeric, b: &Numeric) -> Numeric {
    if let Some(result) = numeric::try_add_fast(a, b) {
        return result;
    }
    add_numeric_ffi(a, b)
}

/// Fast-path subtraction with FFI fallback (the new path).
fn sub_numeric_fast(a: &Numeric, b: &Numeric) -> Numeric {
    if let Some(result) = numeric::try_sub_fast(a, b) {
        return result;
    }
    sub_numeric_ffi(a, b)
}

fn bench_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_add");
    let mut cx = numeric::cx_datum();

    // Small integers (same exponent=0)
    let a_small: Numeric = cx.parse("42").unwrap();
    let b_small: Numeric = cx.parse("58").unwrap();

    group.bench_function("old_small_int", |b| {
        b.iter(|| add_numeric_ffi(black_box(&a_small), black_box(&b_small)))
    });
    group.bench_function("new_small_int", |b| {
        b.iter(|| add_numeric_fast(black_box(&a_small), black_box(&b_small)))
    });

    // Typical decimal values (same scale)
    let a_dec: Numeric = cx.parse("123.45").unwrap();
    let b_dec: Numeric = cx.parse("678.90").unwrap();

    group.bench_function("old_decimal", |b| {
        b.iter(|| add_numeric_ffi(black_box(&a_dec), black_box(&b_dec)))
    });
    group.bench_function("new_decimal", |b| {
        b.iter(|| add_numeric_fast(black_box(&a_dec), black_box(&b_dec)))
    });

    // Large coefficient (18 significant digits, same exponent)
    let a_large: Numeric = cx.parse("999999999999999999").unwrap();
    let b_large: Numeric = cx.parse("1").unwrap();

    group.bench_function("old_large_coeff", |b| {
        b.iter(|| add_numeric_ffi(black_box(&a_large), black_box(&b_large)))
    });
    group.bench_function("new_large_coeff", |b| {
        b.iter(|| add_numeric_fast(black_box(&a_large), black_box(&b_large)))
    });

    // Money-like values (2 decimal places)
    let a_money: Numeric = cx.parse("1234567.89").unwrap();
    let b_money: Numeric = cx.parse("9876543.21").unwrap();

    group.bench_function("old_money", |b| {
        b.iter(|| add_numeric_ffi(black_box(&a_money), black_box(&b_money)))
    });
    group.bench_function("new_money", |b| {
        b.iter(|| add_numeric_fast(black_box(&a_money), black_box(&b_money)))
    });

    // Different exponents (fast path falls back to FFI)
    let a_diff: Numeric = cx.parse("1.5").unwrap();
    let b_diff: Numeric = cx.parse("2.50").unwrap();

    group.bench_function("old_diff_scale", |b| {
        b.iter(|| add_numeric_ffi(black_box(&a_diff), black_box(&b_diff)))
    });
    group.bench_function("new_diff_scale", |b| {
        b.iter(|| add_numeric_fast(black_box(&a_diff), black_box(&b_diff)))
    });

    group.finish();
}

fn bench_sub(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_sub");
    let mut cx = numeric::cx_datum();

    let a: Numeric = cx.parse("1234567.89").unwrap();
    let b: Numeric = cx.parse("123456.78").unwrap();

    group.bench_function("old_money", |bench| {
        bench.iter(|| sub_numeric_ffi(black_box(&a), black_box(&b)))
    });
    group.bench_function("new_money", |bench| {
        bench.iter(|| sub_numeric_fast(black_box(&a), black_box(&b)))
    });

    group.finish();
}

fn bench_add_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_add_batch");
    let mut cx = numeric::cx_datum();

    // Generate 10k pairs with same scale (typical SUM aggregation pattern)
    let values: Vec<Numeric> = (0..10000)
        .map(|i| {
            let s = format!("{}.{:02}", i * 3 - 5000, i % 100);
            cx.parse(&*s).unwrap()
        })
        .collect();

    group.bench_function("old_sum_10k", |b| {
        b.iter(|| {
            let mut acc = values[0].clone();
            for v in &values[1..] {
                acc = add_numeric_ffi(&acc, v);
            }
            black_box(acc)
        })
    });

    group.bench_function("new_sum_10k", |b| {
        b.iter(|| {
            let mut acc = values[0].clone();
            for v in &values[1..] {
                acc = add_numeric_fast(&acc, v);
            }
            black_box(acc)
        })
    });

    // Integer-only batch (best case)
    let int_values: Vec<Numeric> = (0..10000)
        .map(|i| {
            let s = format!("{}", i * 7 - 3000);
            cx.parse(&*s).unwrap()
        })
        .collect();

    group.bench_function("old_sum_ints_10k", |b| {
        b.iter(|| {
            let mut acc = int_values[0].clone();
            for v in &int_values[1..] {
                acc = add_numeric_ffi(&acc, v);
            }
            black_box(acc)
        })
    });

    group.bench_function("new_sum_ints_10k", |b| {
        b.iter(|| {
            let mut acc = int_values[0].clone();
            for v in &int_values[1..] {
                acc = add_numeric_fast(&acc, v);
            }
            black_box(acc)
        })
    });

    group.finish();
}

criterion_group!(benches, bench_add, bench_sub, bench_add_batch);
criterion_main!(benches);
