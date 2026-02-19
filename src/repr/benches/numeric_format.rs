// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for numeric (decimal) formatting.
//!
//! Compares two approaches:
//! - Old: `to_standard_notation_string()` which allocates Vec<u8> + String
//! - New: `write_numeric_standard_notation()` which uses zero heap allocations

use std::fmt::Write;
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dec::Context;
use mz_repr::adt::numeric::{self, Numeric};

fn parse_numeric(s: &str) -> Numeric {
    let mut cx = Context::<Numeric>::default();
    cx.parse(s).unwrap()
}

fn make_test_values() -> Vec<(&'static str, Numeric)> {
    vec![
        ("zero", parse_numeric("0")),
        ("one", parse_numeric("1")),
        ("small_int", parse_numeric("42")),
        ("decimal", parse_numeric("123.456789")),
        ("negative", parse_numeric("-3.14159265358979323846264338327950288")),
        ("tiny", parse_numeric("0.000001")),
        ("large_int", parse_numeric("999999999999999999999999999999999999999")),
        ("large_dec", parse_numeric("99999.999999999999999999999999999999999")),
        ("max_precision", parse_numeric("123456789012345678901234567890123456789")),
        ("small_exp", parse_numeric("1E+10")),
        ("neg_exp", parse_numeric("1E-10")),
        ("many_decimals", parse_numeric("0.123456789012345678901234567890123456789")),
    ]
}

/// Old approach: heap-allocating `to_standard_notation_string`.
fn bench_old(c: &mut Criterion) {
    let values = make_test_values();

    let mut group = c.benchmark_group("numeric_format_old");
    for (name, val) in &values {
        group.bench_with_input(BenchmarkId::from_parameter(name), val, |b, v| {
            let mut buf = String::with_capacity(64);
            b.iter(|| {
                buf.clear();
                let s = black_box(v).to_standard_notation_string();
                buf.push_str(&s);
                black_box(&buf);
            });
        });
    }
    group.finish();
}

/// New approach: zero-allocation `write_numeric_standard_notation`.
fn bench_new(c: &mut Criterion) {
    let values = make_test_values();

    let mut group = c.benchmark_group("numeric_format_new");
    for (name, val) in &values {
        group.bench_with_input(BenchmarkId::from_parameter(name), val, |b, v| {
            let mut buf = String::with_capacity(64);
            b.iter(|| {
                buf.clear();
                numeric::write_numeric_standard_notation(&mut buf, black_box(v)).unwrap();
                black_box(&buf);
            });
        });
    }
    group.finish();
}

/// Write-only: measures pure write cost to a pre-allocated buffer.
fn bench_write_only(c: &mut Criterion) {
    let values = make_test_values();

    let mut group = c.benchmark_group("numeric_format_write_only");
    for (name, val) in &values {
        group.bench_with_input(
            BenchmarkId::new("old", name),
            val,
            |b, v| {
                let mut buf = String::with_capacity(128);
                b.iter(|| {
                    buf.clear();
                    write!(buf, "{}", black_box(v).to_standard_notation_string()).unwrap();
                    black_box(&buf);
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("new", name),
            val,
            |b, v| {
                let mut buf = String::with_capacity(128);
                b.iter(|| {
                    buf.clear();
                    numeric::write_numeric_standard_notation(&mut buf, black_box(v)).unwrap();
                    black_box(&buf);
                });
            },
        );
    }
    group.finish();
}

/// Batch: format 10k values to a shared buffer (simulates pgwire encoding).
fn bench_batch(c: &mut Criterion) {
    // Create a realistic mix of values
    let all_values = make_test_values();
    let batch: Vec<_> = (0..10_000)
        .map(|i| all_values[i % all_values.len()].1.clone())
        .collect();

    let mut group = c.benchmark_group("numeric_format_batch");

    group.bench_function("old_10k", |b| {
        let mut buf = String::with_capacity(10_000 * 20);
        b.iter(|| {
            buf.clear();
            for v in &batch {
                buf.push_str(&black_box(v).to_standard_notation_string());
                buf.push(',');
            }
            black_box(&buf);
        });
    });

    group.bench_function("new_10k", |b| {
        let mut buf = String::with_capacity(10_000 * 20);
        b.iter(|| {
            buf.clear();
            for v in &batch {
                numeric::write_numeric_standard_notation(&mut buf, black_box(v)).unwrap();
                buf.push(',');
            }
            black_box(&buf);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_write_only, bench_old, bench_new, bench_batch);
criterion_main!(benches);
