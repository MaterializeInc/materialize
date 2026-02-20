// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for float value parsing.
//!
//! Compares two approaches:
//! - Old: trim() + FromStr::parse() + regex overflow/underflow check
//! - New: fast-path byte-level parsing for clean ASCII values

use std::num::FpCategory;
use std::sync::LazyLock;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mz_repr::strconv;
use regex::bytes::Regex;

/// General parser for f64 (the old trim + FromStr + regex path).
fn parse_float64_general(s: &str) -> f64 {
    static ZERO_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"(?i-u)^[-+]?(0+(\.0*)?|\.0+)(e|$)"#).unwrap());
    static INF_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new("(?i-u)^[-+]?inf").unwrap());

    let buf = s.trim();
    let f: f64 = buf.parse().unwrap();
    match f.classify() {
        FpCategory::Infinite if !INF_RE.is_match(buf.as_bytes()) => panic!("overflow"),
        FpCategory::Zero if !ZERO_RE.is_match(buf.as_bytes()) => panic!("underflow"),
        _ => f,
    }
}

/// General parser for f32 (the old trim + FromStr + regex path).
fn parse_float32_general(s: &str) -> f32 {
    static ZERO_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"(?i-u)^[-+]?(0+(\.0*)?|\.0+)(e|$)"#).unwrap());
    static INF_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new("(?i-u)^[-+]?inf").unwrap());

    let buf = s.trim();
    let f: f32 = buf.parse().unwrap();
    match f.classify() {
        FpCategory::Infinite if !INF_RE.is_match(buf.as_bytes()) => panic!("overflow"),
        FpCategory::Zero if !ZERO_RE.is_match(buf.as_bytes()) => panic!("underflow"),
        _ => f,
    }
}

fn bench_parse_float64(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_float64");

    let cases: &[(&str, &str)] = &[
        // Pure integers (fast path: u64 → f64 cast)
        ("zero", "0"),
        ("one", "1"),
        ("small_int", "42"),
        ("hundred", "100"),
        ("thousand", "9999"),
        ("million", "1000000"),
        ("typical_id", "1234567890"),
        ("negative_int", "-42"),
        ("neg_large_int", "-123456789"),
        ("max_15digit", "999999999999999"),
        // Simple decimals (fast path: u64 / 10^n)
        ("pi", "3.14159265358979"),
        ("typical_decimal", "3.14"),
        ("small_decimal", "0.001"),
        ("price", "99.99"),
        ("negative_dec", "-42.5"),
        ("neg_pi", "-3.14159265358979"),
        ("tiny_frac", "0.000001"),
        ("long_decimal", "1234567890.12345"),
        // Falls back to general parser
        ("sci_pos", "1.5e10"),
        ("sci_neg", "1.5e-5"),
    ];

    for (name, input) in cases {
        group.bench_function(format!("old_{}", name), |b| {
            b.iter(|| parse_float64_general(black_box(input)))
        });
        group.bench_function(format!("new_{}", name), |b| {
            b.iter(|| strconv::parse_float64(black_box(input)).unwrap())
        });
    }
    group.finish();
}

fn bench_parse_float32(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_float32");

    let cases: &[(&str, &str)] = &[
        ("zero", "0"),
        ("one", "1"),
        ("small_int", "42"),
        ("hundred", "100"),
        ("pi", "3.14159"),
        ("typical", "3.14"),
        ("small_decimal", "0.001"),
        ("negative", "-42.5"),
    ];

    for (name, input) in cases {
        group.bench_function(format!("old_{}", name), |b| {
            b.iter(|| parse_float32_general(black_box(input)))
        });
        group.bench_function(format!("new_{}", name), |b| {
            b.iter(|| strconv::parse_float32(black_box(input)).unwrap())
        });
    }
    group.finish();
}

fn bench_parse_float_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_float_batch_10k");

    // Generate 10k f64 values - mix of integers and decimals
    let mixed_values: Vec<String> = (0..10000)
        .map(|i| {
            if i % 3 == 0 {
                format!("{}", i * 7 - 3000) // integer
            } else if i % 3 == 1 {
                format!("{}.{}", i, i % 1000) // decimal
            } else {
                format!("-{}.{:03}", i / 10, i % 1000) // negative decimal
            }
        })
        .collect();
    let mixed_refs: Vec<&str> = mixed_values.iter().map(|s| s.as_str()).collect();

    group.bench_function("old_f64_mixed_10k", |b| {
        b.iter(|| {
            for s in &mixed_refs {
                black_box(parse_float64_general(black_box(s)));
            }
        });
    });

    group.bench_function("new_f64_mixed_10k", |b| {
        b.iter(|| {
            for s in &mixed_refs {
                black_box(strconv::parse_float64(black_box(s)).unwrap());
            }
        });
    });

    // Integer-only float values (best case for fast path)
    let int_values: Vec<String> = (0..10000).map(|i| format!("{}", i * 3 - 5000)).collect();
    let int_refs: Vec<&str> = int_values.iter().map(|s| s.as_str()).collect();

    group.bench_function("old_f64_ints_10k", |b| {
        b.iter(|| {
            for s in &int_refs {
                black_box(parse_float64_general(black_box(s)));
            }
        });
    });

    group.bench_function("new_f64_ints_10k", |b| {
        b.iter(|| {
            for s in &int_refs {
                black_box(strconv::parse_float64(black_box(s)).unwrap());
            }
        });
    });

    // Decimal-only float values
    let dec_values: Vec<String> = (0..10000)
        .map(|i| format!("{}.{:04}", i / 10, i % 10000))
        .collect();
    let dec_refs: Vec<&str> = dec_values.iter().map(|s| s.as_str()).collect();

    group.bench_function("old_f64_decimals_10k", |b| {
        b.iter(|| {
            for s in &dec_refs {
                black_box(parse_float64_general(black_box(s)));
            }
        });
    });

    group.bench_function("new_f64_decimals_10k", |b| {
        b.iter(|| {
            for s in &dec_refs {
                black_box(strconv::parse_float64(black_box(s)).unwrap());
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_parse_float64,
    bench_parse_float32,
    bench_parse_float_batch,
);
criterion_main!(benches);
