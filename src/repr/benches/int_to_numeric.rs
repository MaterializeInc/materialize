// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for integer-to-Numeric casting.
//!
//! Compares:
//! - Old: FFI via `Numeric::from(i32/i64)` + `rescale()` (C decNumber library)
//! - New: Pure Rust via `numeric_from_i64_coeff` / `numeric_from_u64_coeff`

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_repr::adt::numeric::{self, Numeric};

fn bench_i32_to_numeric(c: &mut Criterion) {
    let values: Vec<i32> = (-5000..5000).collect();

    let mut group = c.benchmark_group("i32_to_numeric");

    group.bench_function("ffi", |b| {
        b.iter(|| {
            let mut sum = Numeric::from(0i32);
            let mut cx = numeric::cx_datum();
            for &v in &values {
                let n = Numeric::from(v);
                cx.add(&mut sum, &n);
            }
            black_box(sum)
        })
    });

    group.bench_function("fast_path", |b| {
        b.iter(|| {
            let mut sum = Numeric::from(0i32);
            let mut cx = numeric::cx_datum();
            for &v in &values {
                let n = numeric::numeric_from_i64_coeff(i64::from(v), 0);
                cx.add(&mut sum, &n);
            }
            black_box(sum)
        })
    });

    group.finish();
}

fn bench_i64_to_numeric(c: &mut Criterion) {
    let values: Vec<i64> = (1_000_000_000..1_000_010_000).collect();

    let mut group = c.benchmark_group("i64_to_numeric");

    group.bench_function("ffi", |b| {
        b.iter(|| {
            let mut sum = Numeric::from(0i32);
            let mut cx = numeric::cx_datum();
            for &v in &values {
                let n = Numeric::from(v);
                cx.add(&mut sum, &n);
            }
            black_box(sum)
        })
    });

    group.bench_function("fast_path", |b| {
        b.iter(|| {
            let mut sum = Numeric::from(0i32);
            let mut cx = numeric::cx_datum();
            for &v in &values {
                let n = numeric::numeric_from_i64_coeff(v, 0);
                cx.add(&mut sum, &n);
            }
            black_box(sum)
        })
    });

    group.finish();
}

fn bench_i32_to_numeric_scaled(c: &mut Criterion) {
    let values: Vec<i32> = (-5000..5000).collect();

    let mut group = c.benchmark_group("i32_to_numeric_scale6");

    group.bench_function("ffi", |b| {
        b.iter(|| {
            let mut sum = Numeric::from(0i32);
            let mut cx = numeric::cx_datum();
            for &v in &values {
                let mut n = Numeric::from(v);
                numeric::rescale(&mut n, 6).unwrap();
                cx.add(&mut sum, &n);
            }
            black_box(sum)
        })
    });

    group.bench_function("fast_path", |b| {
        b.iter(|| {
            let mut sum = Numeric::from(0i32);
            let mut cx = numeric::cx_datum();
            for &v in &values {
                let n = numeric::try_numeric_from_i64(i64::from(v), Some(6)).unwrap();
                cx.add(&mut sum, &n);
            }
            black_box(sum)
        })
    });

    group.finish();
}

fn bench_u64_to_numeric(c: &mut Criterion) {
    let values: Vec<u64> = (0..10_000).collect();

    let mut group = c.benchmark_group("u64_to_numeric");

    group.bench_function("ffi", |b| {
        b.iter(|| {
            let mut sum = Numeric::from(0i32);
            let mut cx = numeric::cx_datum();
            for &v in &values {
                let n = Numeric::from(v);
                cx.add(&mut sum, &n);
            }
            black_box(sum)
        })
    });

    group.bench_function("fast_path", |b| {
        b.iter(|| {
            let mut sum = Numeric::from(0i32);
            let mut cx = numeric::cx_datum();
            for &v in &values {
                let n = numeric::numeric_from_u64_coeff(v, 0);
                cx.add(&mut sum, &n);
            }
            black_box(sum)
        })
    });

    group.finish();
}

fn bench_per_value_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("int_to_numeric_single");

    let test_cases: Vec<(&str, i64)> = vec![
        ("zero", 0),
        ("small_42", 42),
        ("medium_123456", 123456),
        ("large_1B", 1_000_000_000),
        ("max_i32", i32::MAX as i64),
        ("negative", -999_999),
        ("min_i64", i64::MIN),
    ];

    for (name, val) in &test_cases {
        group.bench_with_input(BenchmarkId::new("ffi_i64", name), val, |b, &val| {
            b.iter(|| black_box(Numeric::from(val)))
        });
        group.bench_with_input(BenchmarkId::new("fast_path_i64", name), val, |b, &val| {
            b.iter(|| black_box(numeric::numeric_from_i64_coeff(val, 0)))
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_per_value_overhead,
    bench_i32_to_numeric,
    bench_i64_to_numeric,
    bench_i32_to_numeric_scaled,
    bench_u64_to_numeric,
);
criterion_main!(benches);
