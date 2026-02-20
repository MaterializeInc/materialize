// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for integer and numeric value parsing.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mz_repr::adt::numeric;
use mz_repr::strconv;

/// General parser for int32 (the old .trim().parse() path).
fn parse_int32_general(s: &str) -> i32 {
    s.trim().parse().unwrap()
}

/// General parser for int64 (the old .trim().parse() path).
fn parse_int64_general(s: &str) -> i64 {
    s.trim().parse().unwrap()
}

/// General parser for numeric (the old cx.parse() path).
fn parse_numeric_general(s: &str) -> numeric::Numeric {
    let mut cx = numeric::cx_datum();
    let mut n: numeric::Numeric = cx.parse(s.trim()).unwrap();
    let _ = numeric::munge_numeric(&mut n);
    n
}

fn bench_parse_int32(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_int32");

    let cases: &[(&str, &str)] = &[
        ("zero", "0"),
        ("one", "1"),
        ("small", "42"),
        ("hundred", "100"),
        ("thousand", "9999"),
        ("typical_id", "1234567890"),
        ("negative", "-42"),
        ("min", "-2147483648"),
        ("max", "2147483647"),
    ];

    for (name, input) in cases {
        group.bench_function(format!("old_{}", name), |b| {
            b.iter(|| parse_int32_general(black_box(input)))
        });
        group.bench_function(format!("new_{}", name), |b| {
            b.iter(|| strconv::parse_int32(black_box(input)).unwrap())
        });
    }
    group.finish();
}

fn bench_parse_int64(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_int64");

    let cases: &[(&str, &str)] = &[
        ("zero", "0"),
        ("small", "42"),
        ("typical_id", "1234567890"),
        ("large", "9223372036854775807"),
        ("negative", "-9223372036854775808"),
    ];

    for (name, input) in cases {
        group.bench_function(format!("old_{}", name), |b| {
            b.iter(|| parse_int64_general(black_box(input)))
        });
        group.bench_function(format!("new_{}", name), |b| {
            b.iter(|| strconv::parse_int64(black_box(input)).unwrap())
        });
    }
    group.finish();
}

fn bench_parse_numeric(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_numeric");

    let cases: &[(&str, &str)] = &[
        ("zero", "0"),
        ("one", "1"),
        ("small", "42"),
        ("hundred", "100"),
        ("thousand", "9999"),
        ("large", "123456789"),
        ("very_large", "999999999999999999"),
        ("negative", "-42"),
        ("neg_large", "-123456789"),
        ("decimal", "123.456"),
        ("decimal_small", "0.001"),
        ("decimal_long", "123456.789012345678"),
        ("neg_decimal", "-99.99"),
        ("leading_zeros", "00042"),
    ];

    for (name, input) in cases {
        group.bench_function(format!("old_{}", name), |b| {
            b.iter(|| parse_numeric_general(black_box(input)))
        });
        group.bench_function(format!("new_{}", name), |b| {
            b.iter(|| strconv::parse_numeric(black_box(input)).unwrap())
        });
    }
    group.finish();
}

fn bench_parse_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_batch_10k");

    // Generate 10k integer values
    let int_values: Vec<String> = (0..10000).map(|i| format!("{}", i * 3 - 5000)).collect();
    let int_refs: Vec<&str> = int_values.iter().map(|s| s.as_str()).collect();

    group.bench_function("old_int32_10k", |b| {
        b.iter(|| {
            for s in &int_refs {
                black_box(parse_int32_general(black_box(s)));
            }
        });
    });

    group.bench_function("new_int32_10k", |b| {
        b.iter(|| {
            for s in &int_refs {
                black_box(strconv::parse_int32(black_box(s)).unwrap());
            }
        });
    });

    // Generate 10k numeric values (mix of ints and decimals)
    let numeric_values: Vec<String> = (0..10000)
        .map(|i| {
            if i % 3 == 0 {
                format!("{}", i * 7 - 3000)
            } else if i % 3 == 1 {
                format!("{}.{}", i, i % 1000)
            } else {
                format!("-{}.{:03}", i / 10, i % 1000)
            }
        })
        .collect();
    let numeric_refs: Vec<&str> = numeric_values.iter().map(|s| s.as_str()).collect();

    group.bench_function("old_numeric_mixed_10k", |b| {
        b.iter(|| {
            for s in &numeric_refs {
                black_box(parse_numeric_general(black_box(s)));
            }
        });
    });

    group.bench_function("new_numeric_mixed_10k", |b| {
        b.iter(|| {
            for s in &numeric_refs {
                black_box(strconv::parse_numeric(black_box(s)).unwrap());
            }
        });
    });

    // Batch of numeric integer-only values (best case for fast path)
    let numeric_int_values: Vec<String> = (0..10000).map(|i| format!("{}", i * 3 - 5000)).collect();
    let numeric_int_refs: Vec<&str> = numeric_int_values.iter().map(|s| s.as_str()).collect();

    group.bench_function("old_numeric_ints_10k", |b| {
        b.iter(|| {
            for s in &numeric_int_refs {
                black_box(parse_numeric_general(black_box(s)));
            }
        });
    });

    group.bench_function("new_numeric_ints_10k", |b| {
        b.iter(|| {
            for s in &numeric_int_refs {
                black_box(strconv::parse_numeric(black_box(s)).unwrap());
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_parse_int32,
    bench_parse_int64,
    bench_parse_numeric,
    bench_parse_batch,
);
criterion_main!(benches);
