// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for numeric (decimal) comparison operations.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dec::OrderedDecimal;
use mz_repr::adt::numeric::{self, Numeric};

/// Compare via OrderedDecimal::cmp (the old path: 2×reduce + partial_cmp = 3 FFI calls).
#[inline(never)]
fn cmp_numeric_ffi(a: &OrderedDecimal<Numeric>, b: &OrderedDecimal<Numeric>) -> std::cmp::Ordering {
    a.cmp(b)
}

/// Compare via fast_numeric_cmp (the new path: pure Rust for same-exponent).
#[inline(never)]
fn cmp_numeric_fast(a: &Numeric, b: &Numeric) -> std::cmp::Ordering {
    numeric::fast_numeric_cmp(a, b)
}

fn bench_numeric_cmp(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_cmp");

    // Small integers with same exponent (0).
    let a_small = OrderedDecimal(Numeric::from(42i32));
    let b_small = OrderedDecimal(Numeric::from(99i32));
    group.bench_function("ffi_small_int", |bench| {
        bench.iter(|| cmp_numeric_ffi(black_box(&a_small), black_box(&b_small)))
    });
    group.bench_function("fast_small_int", |bench| {
        bench.iter(|| cmp_numeric_fast(black_box(&a_small.0), black_box(&b_small.0)))
    });

    // Money-like values (2 decimal places, same exponent -2).
    let mut cx = numeric::cx_datum();
    let a_money: Numeric = cx.parse("12345.67").unwrap();
    let b_money: Numeric = cx.parse("98765.43").unwrap();
    let a_money_od = OrderedDecimal(a_money);
    let b_money_od = OrderedDecimal(b_money);
    group.bench_function("ffi_money", |bench| {
        bench.iter(|| cmp_numeric_ffi(black_box(&a_money_od), black_box(&b_money_od)))
    });
    group.bench_function("fast_money", |bench| {
        bench.iter(|| cmp_numeric_fast(black_box(&a_money.clone()), black_box(&b_money.clone())))
    });

    // Large coefficients (18 significant digits, same exponent).
    let a_large: Numeric = cx.parse("123456789012345678").unwrap();
    let b_large: Numeric = cx.parse("987654321098765432").unwrap();
    let a_large_od = OrderedDecimal(a_large);
    let b_large_od = OrderedDecimal(b_large);
    group.bench_function("ffi_large_coeff", |bench| {
        bench.iter(|| cmp_numeric_ffi(black_box(&a_large_od), black_box(&b_large_od)))
    });
    group.bench_function("fast_large_coeff", |bench| {
        bench.iter(|| cmp_numeric_fast(black_box(&a_large.clone()), black_box(&b_large.clone())))
    });

    // Equal values (same exponent).
    let a_eq: Numeric = cx.parse("42.00").unwrap();
    let b_eq: Numeric = cx.parse("42.00").unwrap();
    let a_eq_od = OrderedDecimal(a_eq);
    let b_eq_od = OrderedDecimal(b_eq);
    group.bench_function("ffi_equal", |bench| {
        bench.iter(|| cmp_numeric_ffi(black_box(&a_eq_od), black_box(&b_eq_od)))
    });
    group.bench_function("fast_equal", |bench| {
        bench.iter(|| cmp_numeric_fast(black_box(&a_eq.clone()), black_box(&b_eq.clone())))
    });

    // Different exponents (fast path uses adjusted exponent).
    let a_diff: Numeric = cx.parse("100").unwrap();
    let b_diff: Numeric = cx.parse("99.99").unwrap();
    let a_diff_od = OrderedDecimal(a_diff);
    let b_diff_od = OrderedDecimal(b_diff);
    group.bench_function("ffi_diff_exp", |bench| {
        bench.iter(|| cmp_numeric_ffi(black_box(&a_diff_od), black_box(&b_diff_od)))
    });
    group.bench_function("fast_diff_exp", |bench| {
        bench.iter(|| cmp_numeric_fast(black_box(&a_diff.clone()), black_box(&b_diff.clone())))
    });

    // Negative values (same exponent).
    let a_neg: Numeric = cx.parse("-500.25").unwrap();
    let b_neg: Numeric = cx.parse("-100.50").unwrap();
    let a_neg_od = OrderedDecimal(a_neg);
    let b_neg_od = OrderedDecimal(b_neg);
    group.bench_function("ffi_negative", |bench| {
        bench.iter(|| cmp_numeric_ffi(black_box(&a_neg_od), black_box(&b_neg_od)))
    });
    group.bench_function("fast_negative", |bench| {
        bench.iter(|| cmp_numeric_fast(black_box(&a_neg.clone()), black_box(&b_neg.clone())))
    });

    group.finish();
}

fn bench_numeric_cmp_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_cmp_batch");
    let mut cx = numeric::cx_datum();

    // Batch of 10k money-like comparisons (same exponent -2).
    let values: Vec<Numeric> = (0..10000)
        .map(|i| {
            let s = format!("{}.{:02}", i / 100, i % 100);
            cx.parse(s.as_str()).unwrap()
        })
        .collect();
    let values_od: Vec<OrderedDecimal<Numeric>> = values.iter().map(|v| OrderedDecimal(*v)).collect();

    group.bench_function("ffi_10k_money", |bench| {
        bench.iter(|| {
            let mut count = 0u32;
            for pair in values_od.windows(2) {
                if pair[0] < pair[1] {
                    count += 1;
                }
            }
            black_box(count)
        })
    });

    group.bench_function("fast_10k_money", |bench| {
        bench.iter(|| {
            let mut count = 0u32;
            for pair in values.windows(2) {
                if numeric::fast_numeric_cmp(&pair[0], &pair[1]) == std::cmp::Ordering::Less {
                    count += 1;
                }
            }
            black_box(count)
        })
    });

    group.finish();
}

criterion_group!(benches, bench_numeric_cmp, bench_numeric_cmp_batch);
criterion_main!(benches);
