// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for Numeric multiplication.
//!
//! Compares:
//! - Old: FFI via `cx.mul()` (C decNumber library)
//! - New: Pure Rust via `try_mul_fast()`

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_repr::adt::numeric::{self, Numeric};

fn bench_mul_per_value(c: &mut Criterion) {
    let mut cx = numeric::cx_datum();

    let cases: Vec<(&str, Numeric, Numeric)> = vec![
        ("small_int", Numeric::from(42i32), Numeric::from(7i32)),
        (
            "money_x_qty",
            cx.parse("12345.67").unwrap(),
            cx.parse("99").unwrap(),
        ),
        (
            "same_scale",
            cx.parse("100.00").unwrap(),
            cx.parse("50.00").unwrap(),
        ),
        (
            "large_coeff",
            cx.parse("999999999999.99").unwrap(),
            cx.parse("123456").unwrap(),
        ),
        (
            "neg_x_pos",
            cx.parse("-500.25").unwrap(),
            cx.parse("200.50").unwrap(),
        ),
        (
            "tiny",
            cx.parse("0.000001").unwrap(),
            cx.parse("0.000002").unwrap(),
        ),
        (
            "one_x_price",
            Numeric::from(1i32),
            cx.parse("12345.67").unwrap(),
        ),
        (
            "zero_x_val",
            Numeric::from(0i32),
            cx.parse("12345.67").unwrap(),
        ),
    ];

    let mut group = c.benchmark_group("numeric_mul_per_value");

    for (name, a, b) in &cases {
        group.bench_with_input(BenchmarkId::new("ffi", name), &(a, b), |bench, (a, b)| {
            bench.iter(|| {
                let mut cx = numeric::cx_datum();
                let mut result = **a;
                cx.mul(&mut result, b);
                black_box(result)
            })
        });

        group.bench_with_input(
            BenchmarkId::new("fast_path", name),
            &(a, b),
            |bench, (a, b)| {
                bench.iter(|| {
                    if let Some(r) = numeric::try_mul_fast(a, b) {
                        black_box(r)
                    } else {
                        let mut cx = numeric::cx_datum();
                        let mut result = **a;
                        cx.mul(&mut result, b);
                        black_box(result)
                    }
                })
            },
        );
    }

    group.finish();
}

fn bench_mul_batch(c: &mut Criterion) {
    let mut cx = numeric::cx_datum();

    // Simulate TPC-H: revenue = price * quantity for 10k rows
    let prices: Vec<Numeric> = (0..10_000)
        .map(|i| {
            let val = format!("{}.{:02}", 100 + (i % 9000), (i * 7) % 100);
            cx.parse(val).unwrap()
        })
        .collect();
    let quantities: Vec<Numeric> = (0..10_000)
        .map(|i| {
            let val = format!("{}", 1 + (i % 50));
            cx.parse(val).unwrap()
        })
        .collect();

    let mut group = c.benchmark_group("numeric_mul_batch");

    group.bench_function("ffi_10k_revenue", |b| {
        b.iter(|| {
            let mut sum = Numeric::from(0i32);
            let mut cx = numeric::cx_datum();
            for i in 0..prices.len() {
                let mut product = prices[i];
                cx.mul(&mut product, &quantities[i]);
                cx.add(&mut sum, &product);
            }
            black_box(sum)
        })
    });

    group.bench_function("fast_10k_revenue", |b| {
        b.iter(|| {
            let mut sum = Numeric::from(0i32);
            let mut cx = numeric::cx_datum();
            for i in 0..prices.len() {
                let product = if let Some(r) = numeric::try_mul_fast(&prices[i], &quantities[i]) {
                    r
                } else {
                    let mut p = prices[i];
                    cx.mul(&mut p, &quantities[i]);
                    p
                };
                if let Some(s) = numeric::try_add_fast(&sum, &product) {
                    sum = s;
                } else {
                    cx.add(&mut sum, &product);
                }
            }
            black_box(sum)
        })
    });

    // Same-scale multiplication (DECIMAL(15,2) × DECIMAL(15,2))
    let a_vals: Vec<Numeric> = (0..10_000)
        .map(|i| {
            let val = format!("{}.{:02}", 50 + (i % 950), (i * 3) % 100);
            cx.parse(val).unwrap()
        })
        .collect();
    let b_vals: Vec<Numeric> = (0..10_000)
        .map(|i| {
            let val = format!("{}.{:02}", 1 + (i % 99), (i * 11) % 100);
            cx.parse(val).unwrap()
        })
        .collect();

    group.bench_function("ffi_10k_same_scale", |b| {
        b.iter(|| {
            for i in 0..a_vals.len() {
                let mut product = a_vals[i];
                let mut cx = numeric::cx_datum();
                cx.mul(&mut product, &b_vals[i]);
                black_box(product);
            }
        })
    });

    group.bench_function("fast_10k_same_scale", |b| {
        b.iter(|| {
            for i in 0..a_vals.len() {
                let product = if let Some(r) = numeric::try_mul_fast(&a_vals[i], &b_vals[i]) {
                    r
                } else {
                    let mut p = a_vals[i];
                    let mut cx = numeric::cx_datum();
                    cx.mul(&mut p, &b_vals[i]);
                    p
                };
                black_box(product);
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_mul_per_value, bench_mul_batch);
criterion_main!(benches);
