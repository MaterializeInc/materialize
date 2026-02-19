// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks comparing interval formatting approaches:
//!
//! - Old: `write!(buf, "{}", iv)` dispatches through `Interval::Display`
//!   which uses multiple `write!` and `write_char` calls through the fmt
//!   machinery.
//!
//! - New: Direct formatting with stack buffers, the DIGIT_PAIRS lookup table,
//!   and single `write_str` calls. Zero heap allocations; bypasses fmt entirely.

use std::fmt::Write;
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_repr::adt::interval::Interval;
use mz_repr::strconv;

/// Old format_interval: uses write!(buf, "{}", iv) through Display trait.
fn format_interval_old(buf: &mut String, iv: Interval) {
    write!(buf, "{}", iv).unwrap();
}

fn bench_interval_format(c: &mut Criterion) {
    let test_intervals: Vec<(&str, Interval)> = vec![
        ("zero", Interval::new(0, 0, 0)),
        ("one_hour", Interval::new(0, 0, 3_600_000_000)),
        ("hms", Interval::new(0, 0, 3_723_000_000)),
        ("one_year", Interval::new(12, 0, 0)),
        ("years_months", Interval::new(14, 0, 0)),
        ("one_day", Interval::new(0, 1, 0)),
        ("days", Interval::new(0, 5, 0)),
        ("neg_months", Interval::new(-14, 0, 0)),
        ("neg_days", Interval::new(0, -3, 0)),
        ("with_micros", Interval::new(0, 0, 1_500_000)),
        ("complex", Interval::new(14, 3, 3_723_456_789)),
        ("neg_time", Interval::new(0, 0, -3_723_000_000)),
        ("all_parts", Interval::new(25, 10, 45_296_123_456)),
    ];

    // Per-value benchmarks
    let mut group = c.benchmark_group("interval_format_write");
    for (name, iv) in &test_intervals {
        group.bench_with_input(BenchmarkId::new("old_display", name), iv, |b, iv| {
            let mut buf = String::with_capacity(128);
            b.iter(|| {
                buf.clear();
                format_interval_old(&mut buf, black_box(*iv));
                black_box(&buf);
            });
        });
        group.bench_with_input(BenchmarkId::new("new_direct", name), iv, |b, iv| {
            let mut buf = String::with_capacity(128);
            b.iter(|| {
                buf.clear();
                strconv::format_interval(&mut buf, black_box(*iv));
                black_box(&buf);
            });
        });
    }
    group.finish();

    // Batch benchmark: 10k intervals
    let batch_intervals: Vec<Interval> = (0..10_000i64)
        .map(|i| {
            Interval::new(
                (i % 48) as i32,
                (i % 365) as i32,
                i * 1_000_000 + (i % 1000) * 1000,
            )
        })
        .collect();

    let mut group = c.benchmark_group("interval_format_batch");
    group.bench_function("old_display_10k", |b| {
        let mut buf = String::with_capacity(500_000);
        b.iter(|| {
            buf.clear();
            for iv in &batch_intervals {
                format_interval_old(&mut buf, black_box(*iv));
            }
            black_box(&buf);
        });
    });
    group.bench_function("new_direct_10k", |b| {
        let mut buf = String::with_capacity(500_000);
        b.iter(|| {
            buf.clear();
            for iv in &batch_intervals {
                strconv::format_interval(&mut buf, black_box(*iv));
            }
            black_box(&buf);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_interval_format);
criterion_main!(benches);
