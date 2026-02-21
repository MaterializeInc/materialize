// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for numeric_to_twos_complement_be (Avro encoding path).

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mz_repr::adt::numeric::{self, Numeric};

fn bench_twos_complement_be(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_twos_complement_be");

    let mut cx = numeric::cx_datum();

    // Integer (fast path: zero exponent, small coefficient)
    let int_val = Numeric::from(42i32);
    group.bench_function("integer_42", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_be(black_box(int_val)))
    });

    // Money-like decimal (fast path: negative exponent, small coefficient)
    let money: Numeric = cx.parse("12345.67").unwrap();
    group.bench_function("money_12345_67", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_be(black_box(money)))
    });

    // After rescale to scale=6 (simulating Avro column encoding)
    let mut rescaled: Numeric = cx.parse("123.45").unwrap();
    let _ = numeric::rescale(&mut rescaled, 6);
    group.bench_function("rescaled_scale6", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_be(black_box(rescaled)))
    });

    // Large coefficient that still fits in i64 (18 digits)
    let large: Numeric = cx.parse("999999999999999999").unwrap();
    group.bench_function("large_18_digits", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_be(black_box(large)))
    });

    // Negative value
    let neg: Numeric = cx.parse("-98765.4321").unwrap();
    group.bench_function("negative", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_be(black_box(neg)))
    });

    // Zero
    let zero = Numeric::from(0i32);
    group.bench_function("zero", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_be(black_box(zero)))
    });

    // Large coefficient (> 18 digits, must use FFI path)
    let huge: Numeric = cx.parse("12345678901234567890.12").unwrap();
    group.bench_function("huge_gt18_digits", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_be(black_box(huge)))
    });

    group.finish();
}

criterion_group!(benches, bench_twos_complement_be);
criterion_main!(benches);
