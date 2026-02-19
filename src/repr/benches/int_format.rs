// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for integer formatting.
//!
//! Compares two approaches:
//! - Old: `write!(buf, "{}", i)` which goes through Rust's fmt machinery
//! - New: direct digit extraction into a stack buffer with single `write_str`

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_repr::strconv;

/// Old approach: format via write! macro (Rust's fmt machinery).
fn format_int64_old(buf: &mut String, i: i64) {
    std::fmt::Write::write_fmt(buf, format_args!("{}", i)).unwrap();
}

fn format_uint64_old(buf: &mut String, u: u64) {
    std::fmt::Write::write_fmt(buf, format_args!("{}", u)).unwrap();
}

fn make_i64_test_values() -> Vec<(&'static str, i64)> {
    vec![
        ("zero", 0),
        ("one", 1),
        ("small", 42),
        ("hundred", 100),
        ("thousand", 9999),
        ("million", 1_000_000),
        ("typical_id", 1234567890),
        ("negative", -42),
        ("neg_large", -1234567890),
        ("i32_max", i32::MAX as i64),
        ("i32_min", i32::MIN as i64),
        ("i64_max", i64::MAX),
        ("i64_min", i64::MIN),
    ]
}

fn make_u64_test_values() -> Vec<(&'static str, u64)> {
    vec![
        ("zero", 0),
        ("one", 1),
        ("small", 42),
        ("thousand", 9999),
        ("typical_id", 1234567890),
        ("u32_max", u32::MAX as u64),
        ("u64_max", u64::MAX),
    ]
}

fn bench_i64_per_value(c: &mut Criterion) {
    let values = make_i64_test_values();
    let mut group = c.benchmark_group("i64_per_value");

    for (name, val) in &values {
        group.bench_with_input(BenchmarkId::new("old_write_macro", name), val, |b, &v| {
            let mut buf = String::with_capacity(32);
            b.iter(|| {
                buf.clear();
                format_int64_old(&mut buf, black_box(v));
                black_box(&buf);
            });
        });
        group.bench_with_input(BenchmarkId::new("new_stack_buf", name), val, |b, &v| {
            let mut buf = String::with_capacity(32);
            b.iter(|| {
                buf.clear();
                strconv::format_int64(&mut buf, black_box(v));
                black_box(&buf);
            });
        });
    }
    group.finish();
}

fn bench_u64_per_value(c: &mut Criterion) {
    let values = make_u64_test_values();
    let mut group = c.benchmark_group("u64_per_value");

    for (name, val) in &values {
        group.bench_with_input(BenchmarkId::new("old_write_macro", name), val, |b, &v| {
            let mut buf = String::with_capacity(32);
            b.iter(|| {
                buf.clear();
                format_uint64_old(&mut buf, black_box(v));
                black_box(&buf);
            });
        });
        group.bench_with_input(BenchmarkId::new("new_stack_buf", name), val, |b, &v| {
            let mut buf = String::with_capacity(32);
            b.iter(|| {
                buf.clear();
                strconv::format_uint64(&mut buf, black_box(v));
                black_box(&buf);
            });
        });
    }
    group.finish();
}

fn bench_batch_i64(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_i64_10k");

    // Mix of values: typical database IDs, small values, negatives
    let values: Vec<i64> = (0..10_000)
        .map(|i| match i % 5 {
            0 => i as i64,             // small positive
            1 => -(i as i64),          // small negative
            2 => 1_000_000 + i as i64, // typical ID range
            3 => i as i64 * 12345,     // medium values
            _ => i64::MAX - i as i64,  // large values
        })
        .collect();

    group.bench_function("old_write_macro", |b| {
        let mut buf = String::with_capacity(200_000);
        b.iter(|| {
            buf.clear();
            for &v in &values {
                format_int64_old(&mut buf, black_box(v));
                buf.push(',');
            }
            black_box(&buf);
        });
    });

    group.bench_function("new_stack_buf", |b| {
        let mut buf = String::with_capacity(200_000);
        b.iter(|| {
            buf.clear();
            for &v in &values {
                strconv::format_int64(&mut buf, black_box(v));
                buf.push(',');
            }
            black_box(&buf);
        });
    });

    group.finish();
}

fn bench_batch_i32(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_i32_10k");

    let values: Vec<i32> = (0..10_000)
        .map(|i| match i % 4 {
            0 => i as i32,
            1 => -(i as i32),
            2 => 1_000_000 + i as i32,
            _ => i32::MAX - i as i32,
        })
        .collect();

    group.bench_function("old_write_macro", |b| {
        let mut buf = String::with_capacity(200_000);
        b.iter(|| {
            buf.clear();
            for &v in &values {
                use std::fmt::Write;
                std::fmt::Write::write_fmt(&mut buf, format_args!("{}", black_box(v))).unwrap();
                buf.push(',');
            }
            black_box(&buf);
        });
    });

    group.bench_function("new_stack_buf", |b| {
        let mut buf = String::with_capacity(200_000);
        b.iter(|| {
            buf.clear();
            for &v in &values {
                strconv::format_int32(&mut buf, black_box(v));
                buf.push(',');
            }
            black_box(&buf);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_i64_per_value,
    bench_u64_per_value,
    bench_batch_i64,
    bench_batch_i32,
);
criterion_main!(benches);
