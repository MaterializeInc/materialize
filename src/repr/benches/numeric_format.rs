// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for numeric formatting (standard notation output).
//!
//! Compares two approaches:
//! - Old: `to_standard_notation_string()` → heap-allocated String (+ Vec for coefficient digits)
//! - New: `write_numeric_standard_notation()` → writes directly to buffer, zero heap allocations

use std::fmt::Write;
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_repr::adt::numeric::{self, Numeric, write_numeric_standard_notation};

fn make_test_numerics() -> Vec<(&'static str, Numeric)> {
    let mut cx = numeric::cx_datum();
    vec![
        ("zero", cx.parse("0").unwrap()),
        ("one", cx.parse("1").unwrap()),
        ("small_int", cx.parse("42").unwrap()),
        ("decimal", cx.parse("123.456789").unwrap()),
        ("negative", cx.parse("-3.14159265358979").unwrap()),
        ("tiny", cx.parse("0.000001").unwrap()),
        ("large_int", cx.parse("99999999999999").unwrap()),
        ("large_dec", cx.parse("99999999999.999999").unwrap()),
        ("max_precision", cx.parse("999999999999999999999999999999999999999").unwrap()),
        ("small_exp", cx.parse("1e10").unwrap()),
        ("neg_exp", cx.parse("1e-10").unwrap()),
        (
            "many_decimals",
            cx.parse("0.123456789012345678901234567890123456789").unwrap(),
        ),
    ]
}

fn format_via_to_standard_notation_string(n: &Numeric) -> String {
    n.to_standard_notation_string()
}

fn format_via_write_numeric(n: &Numeric) -> String {
    let mut s = String::new();
    write_numeric_standard_notation(&mut s, n).unwrap();
    s
}

/// Benchmark: measure per-value formatting time for both approaches.
fn bench_per_value(c: &mut Criterion) {
    let test_values = make_test_numerics();

    let mut group = c.benchmark_group("numeric_format");
    for (name, val) in &test_values {
        group.bench_with_input(
            BenchmarkId::new("to_standard_notation_string", name),
            val,
            |b, val| b.iter(|| black_box(format_via_to_standard_notation_string(val))),
        );
        group.bench_with_input(
            BenchmarkId::new("write_numeric_standard_notation", name),
            val,
            |b, val| b.iter(|| black_box(format_via_write_numeric(val))),
        );
    }
    group.finish();
}

/// Benchmark: simulate formatting a batch of numeric values to a single output
/// buffer (like pgwire would do). This measures the benefit of reusing the
/// buffer across values.
fn bench_batch_to_buffer(c: &mut Criterion) {
    let test_values: Vec<Numeric> = make_test_numerics().into_iter().map(|(_, v)| v).collect();
    let batch: Vec<Numeric> = test_values.iter().copied().cycle().take(10000).collect();

    let mut group = c.benchmark_group("numeric_format_batch_10k");
    group.bench_function("to_standard_notation_string", |b| {
        b.iter(|| {
            let mut buf = String::with_capacity(8192);
            for val in &batch {
                buf.push_str(&val.to_standard_notation_string());
                buf.push(',');
            }
            black_box(buf.len())
        })
    });
    group.bench_function("write_numeric_standard_notation", |b| {
        b.iter(|| {
            let mut buf = String::with_capacity(8192);
            for val in &batch {
                write_numeric_standard_notation(&mut buf, val).unwrap();
                buf.push(',');
            }
            black_box(buf.len())
        })
    });
    group.finish();
}

/// Benchmark: write to a pre-allocated buffer without returning a String.
/// This measures the "pure write" cost without any allocation for the output.
fn bench_write_only(c: &mut Criterion) {
    let test_values = make_test_numerics();

    let mut group = c.benchmark_group("numeric_format_write_only");
    for (name, val) in &test_values {
        group.bench_with_input(
            BenchmarkId::new("to_standard_notation_string", name),
            val,
            |b, val| {
                let mut buf = String::with_capacity(128);
                b.iter(|| {
                    buf.clear();
                    buf.push_str(&val.to_standard_notation_string());
                    black_box(buf.len())
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("write_numeric_standard_notation", name),
            val,
            |b, val| {
                let mut buf = String::with_capacity(128);
                b.iter(|| {
                    buf.clear();
                    write_numeric_standard_notation(&mut buf, val).unwrap();
                    black_box(buf.len())
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_per_value, bench_batch_to_buffer, bench_write_only);
criterion_main!(benches);
