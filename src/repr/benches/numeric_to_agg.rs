// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for Numeric → NumericAgg conversion (to_width) and
//! NumericAgg × i64 multiplication, comparing the old FFI path vs
//! the new fast path.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mz_repr::adt::numeric::{self, Numeric, NumericAgg};

/// Old path: cx_agg.to_width() = C FFI call.
fn to_width_ffi(n: &Numeric) -> NumericAgg {
    let mut cx = numeric::cx_agg();
    cx.to_width(*n)
}

/// New path: direct coefficient copy, no FFI.
fn to_width_fast(n: &Numeric) -> NumericAgg {
    numeric::numeric_to_agg_direct(n)
}

/// Old path: cx_agg.mul() = 2 FFI calls (from_i64 + mul).
fn mul_agg_ffi(a: &NumericAgg, factor: i64) -> NumericAgg {
    let mut cx = numeric::cx_agg();
    let mut f = NumericAgg::from(factor);
    cx.mul(&mut f, a);
    f
}

/// New path: try fast path first, fall back to FFI.
fn mul_agg_fast(a: &NumericAgg, factor: i64) -> NumericAgg {
    numeric::try_mul_numeric_agg_i64(a, factor).unwrap_or_else(|| {
        let mut cx = numeric::cx_agg();
        let mut f = NumericAgg::from(factor);
        cx.mul(&mut f, a);
        f
    })
}

fn bench_to_width(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_to_agg");
    let mut cx = dec::Context::<Numeric>::default();
    cx.set_max_exponent(isize::from(numeric::NUMERIC_DATUM_MAX_PRECISION - 1))
        .unwrap();
    cx.set_min_exponent(-isize::from(numeric::NUMERIC_DATUM_MAX_PRECISION))
        .unwrap();

    // Zero
    let zero: Numeric = cx.parse("0").unwrap();
    group.bench_function("old_zero", |b| {
        b.iter(|| to_width_ffi(black_box(&zero)))
    });
    group.bench_function("new_zero", |b| {
        b.iter(|| to_width_fast(black_box(&zero)))
    });

    // Small integer
    let small: Numeric = cx.parse("42").unwrap();
    group.bench_function("old_small_int", |b| {
        b.iter(|| to_width_ffi(black_box(&small)))
    });
    group.bench_function("new_small_int", |b| {
        b.iter(|| to_width_fast(black_box(&small)))
    });

    // Money value (2 decimal places)
    let money: Numeric = cx.parse("12345.67").unwrap();
    group.bench_function("old_money", |b| {
        b.iter(|| to_width_ffi(black_box(&money)))
    });
    group.bench_function("new_money", |b| {
        b.iter(|| to_width_fast(black_box(&money)))
    });

    // Large coefficient (18 digits)
    let large: Numeric = cx.parse("999999999999999999").unwrap();
    group.bench_function("old_large_18dig", |b| {
        b.iter(|| to_width_ffi(black_box(&large)))
    });
    group.bench_function("new_large_18dig", |b| {
        b.iter(|| to_width_fast(black_box(&large)))
    });

    // Negative value
    let neg: Numeric = cx.parse("-98765.4321").unwrap();
    group.bench_function("old_negative", |b| {
        b.iter(|| to_width_ffi(black_box(&neg)))
    });
    group.bench_function("new_negative", |b| {
        b.iter(|| to_width_fast(black_box(&neg)))
    });

    // Max precision (38 digits)
    let max_prec: Numeric = cx.parse("12345678901234567890123456789012345678").unwrap();
    group.bench_function("old_max_precision", |b| {
        b.iter(|| to_width_ffi(black_box(&max_prec)))
    });
    group.bench_function("new_max_precision", |b| {
        b.iter(|| to_width_fast(black_box(&max_prec)))
    });

    // Small exponent
    let small_exp: Numeric = cx.parse("0.000001").unwrap();
    group.bench_function("old_small_exp", |b| {
        b.iter(|| to_width_ffi(black_box(&small_exp)))
    });
    group.bench_function("new_small_exp", |b| {
        b.iter(|| to_width_fast(black_box(&small_exp)))
    });

    group.finish();
}

fn bench_to_width_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_to_agg_batch");
    let mut cx = dec::Context::<Numeric>::default();
    cx.set_max_exponent(isize::from(numeric::NUMERIC_DATUM_MAX_PRECISION - 1))
        .unwrap();
    cx.set_min_exponent(-isize::from(numeric::NUMERIC_DATUM_MAX_PRECISION))
        .unwrap();

    // 10k mixed numeric values
    let values: Vec<Numeric> = (0..10_000)
        .map(|i| {
            let s = match i % 5 {
                0 => format!("{}", i),
                1 => format!("{}.{:02}", i / 100, i % 100),
                2 => format!("-{}.{:04}", i, i % 10000),
                3 => format!("0.{:06}", i % 1000000),
                _ => format!("{}", i * 1000 + 42),
            };
            cx.parse(&*s).unwrap()
        })
        .collect();

    group.bench_function("old_10k_mixed", |b| {
        b.iter(|| {
            let mut sum = NumericAgg::zero();
            let mut cx_agg = numeric::cx_agg();
            for n in &values {
                let wide = cx_agg.to_width(*n);
                cx_agg.add(&mut sum, &wide);
            }
            sum
        })
    });
    group.bench_function("new_10k_mixed", |b| {
        b.iter(|| {
            let mut sum = NumericAgg::zero();
            let mut cx_agg = numeric::cx_agg();
            for n in &values {
                let wide = numeric::numeric_to_agg_direct(n);
                cx_agg.add(&mut sum, &wide);
            }
            sum
        })
    });

    group.finish();
}

fn bench_mul_agg(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_agg_mul");
    let mut cx = numeric::cx_agg();

    // Factor = 1 (most common: no retraction)
    let money: NumericAgg = cx.parse("12345.67").unwrap();
    group.bench_function("old_factor1_money", |b| {
        b.iter(|| mul_agg_ffi(black_box(&money), black_box(1)))
    });
    group.bench_function("new_factor1_money", |b| {
        b.iter(|| mul_agg_fast(black_box(&money), black_box(1)))
    });

    // Factor = -1 (retraction)
    group.bench_function("old_factor_neg1_money", |b| {
        b.iter(|| mul_agg_ffi(black_box(&money), black_box(-1)))
    });
    group.bench_function("new_factor_neg1_money", |b| {
        b.iter(|| mul_agg_fast(black_box(&money), black_box(-1)))
    });

    // Factor = 5 (consolidation)
    let small: NumericAgg = cx.parse("42").unwrap();
    group.bench_function("old_factor5_small", |b| {
        b.iter(|| mul_agg_ffi(black_box(&small), black_box(5)))
    });
    group.bench_function("new_factor5_small", |b| {
        b.iter(|| mul_agg_fast(black_box(&small), black_box(5)))
    });

    // Large coefficient (18 digits) × factor
    let large: NumericAgg = cx.parse("999999999999999999.99").unwrap();
    group.bench_function("old_factor2_large", |b| {
        b.iter(|| mul_agg_ffi(black_box(&large), black_box(2)))
    });
    group.bench_function("new_factor2_large", |b| {
        b.iter(|| mul_agg_fast(black_box(&large), black_box(2)))
    });

    group.finish();
}

criterion_group!(benches, bench_to_width, bench_to_width_batch, bench_mul_agg);
criterion_main!(benches);
