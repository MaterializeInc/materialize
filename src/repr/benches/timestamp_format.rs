// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for timestamp/date/time formatting.
//!
//! Compares two approaches:
//! - Old: chrono's `ts.format("%m-%d %H:%M:%S")` which parses the format string each call
//! - New: direct field extraction with stack-buffer single-write

use std::hint::black_box;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_ore::fmt::FormatBuffer;
use mz_repr::adt::date::Date;
use mz_repr::strconv;

fn make_test_timestamps() -> Vec<(&'static str, NaiveDateTime)> {
    vec![
        (
            "epoch",
            NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            "typical",
            NaiveDate::from_ymd_opt(2024, 6, 15)
                .unwrap()
                .and_hms_opt(14, 30, 45)
                .unwrap(),
        ),
        (
            "with_micros",
            NaiveDate::from_ymd_opt(2024, 12, 31)
                .unwrap()
                .and_hms_micro_opt(23, 59, 59, 123456)
                .unwrap(),
        ),
        (
            "small_micros",
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_micro_opt(0, 0, 0, 1)
                .unwrap(),
        ),
        (
            "year_0001",
            NaiveDate::from_ymd_opt(1, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            "year_9999",
            NaiveDate::from_ymd_opt(9999, 12, 31)
                .unwrap()
                .and_hms_micro_opt(23, 59, 59, 999999)
                .unwrap(),
        ),
    ]
}

fn make_test_dates() -> Vec<(&'static str, Date)> {
    vec![
        (
            "epoch",
            Date::try_from(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).unwrap(),
        ),
        (
            "typical",
            Date::try_from(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()).unwrap(),
        ),
        (
            "year_end",
            Date::try_from(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap()).unwrap(),
        ),
    ]
}

fn make_test_times() -> Vec<(&'static str, NaiveTime)> {
    vec![
        ("midnight", NaiveTime::from_hms_opt(0, 0, 0).unwrap()),
        ("typical", NaiveTime::from_hms_opt(14, 30, 45).unwrap()),
        (
            "with_micros",
            NaiveTime::from_hms_micro_opt(23, 59, 59, 123456).unwrap(),
        ),
    ]
}

// ---- Old implementations using chrono's format() ----

fn old_format_nanos_to_micros<F: FormatBuffer>(buf: &mut F, nanos: u32) {
    if nanos >= 500 {
        let mut micros = nanos / 1000;
        let rem = nanos % 1000;
        if rem >= 500 {
            micros += 1;
        }
        let mut width = 6;
        while micros % 10 == 0 {
            width -= 1;
            micros /= 10;
        }
        write!(buf, ".{:0width$}", micros, width = width);
    }
}

fn old_format_timestamp(buf: &mut String, ts: &NaiveDateTime) {
    let (year_ad, year) = ts.year_ce();
    write!(buf, "{:04}-{}", year, ts.format("%m-%d %H:%M:%S"));
    old_format_nanos_to_micros(buf, ts.and_utc().timestamp_subsec_nanos());
    if !year_ad {
        write!(buf, " BC");
    }
}

fn old_format_timestamptz(buf: &mut String, ts: &DateTime<Utc>) {
    let (year_ad, year) = ts.year_ce();
    write!(buf, "{:04}-{}", year, ts.format("%m-%d %H:%M:%S"));
    old_format_nanos_to_micros(buf, ts.timestamp_subsec_nanos());
    write!(buf, "+00");
    if !year_ad {
        write!(buf, " BC");
    }
}

fn old_format_date(buf: &mut String, d: Date) {
    let d: NaiveDate = d.into();
    let (year_ad, year) = d.year_ce();
    write!(buf, "{:04}-{}", year, d.format("%m-%d"));
    if !year_ad {
        write!(buf, " BC");
    }
}

fn old_format_time(buf: &mut String, t: NaiveTime) {
    write!(buf, "{}", t.format("%H:%M:%S"));
    old_format_nanos_to_micros(buf, t.nanosecond());
}

// ---- Benchmarks ----

fn bench_format_timestamp(c: &mut Criterion) {
    let timestamps = make_test_timestamps();
    let mut group = c.benchmark_group("format_timestamp");

    for (name, ts) in &timestamps {
        group.bench_with_input(BenchmarkId::new("old_chrono", name), ts, |b, ts| {
            let mut buf = String::with_capacity(64);
            b.iter(|| {
                buf.clear();
                old_format_timestamp(black_box(&mut buf), black_box(ts));
                black_box(&buf);
            });
        });
        group.bench_with_input(BenchmarkId::new("new_direct", name), ts, |b, ts| {
            let mut buf = String::with_capacity(64);
            b.iter(|| {
                buf.clear();
                strconv::format_timestamp(black_box(&mut buf), black_box(ts));
                black_box(&buf);
            });
        });
    }
    group.finish();
}

fn bench_format_timestamptz(c: &mut Criterion) {
    let timestamps = make_test_timestamps();
    let mut group = c.benchmark_group("format_timestamptz");

    for (name, ts) in &timestamps {
        let tstz: DateTime<Utc> = DateTime::from_naive_utc_and_offset(*ts, Utc);
        group.bench_with_input(BenchmarkId::new("old_chrono", name), &tstz, |b, ts| {
            let mut buf = String::with_capacity(64);
            b.iter(|| {
                buf.clear();
                old_format_timestamptz(black_box(&mut buf), black_box(ts));
                black_box(&buf);
            });
        });
        group.bench_with_input(BenchmarkId::new("new_direct", name), &tstz, |b, ts| {
            let mut buf = String::with_capacity(64);
            b.iter(|| {
                buf.clear();
                strconv::format_timestamptz(black_box(&mut buf), black_box(ts));
                black_box(&buf);
            });
        });
    }
    group.finish();
}

fn bench_format_date(c: &mut Criterion) {
    let dates = make_test_dates();
    let mut group = c.benchmark_group("format_date");

    for (name, d) in &dates {
        group.bench_with_input(BenchmarkId::new("old_chrono", name), d, |b, d| {
            let mut buf = String::with_capacity(64);
            b.iter(|| {
                buf.clear();
                old_format_date(black_box(&mut buf), black_box(*d));
                black_box(&buf);
            });
        });
        group.bench_with_input(BenchmarkId::new("new_direct", name), d, |b, d| {
            let mut buf = String::with_capacity(64);
            b.iter(|| {
                buf.clear();
                strconv::format_date(black_box(&mut buf), black_box(*d));
                black_box(&buf);
            });
        });
    }
    group.finish();
}

fn bench_format_time(c: &mut Criterion) {
    let times = make_test_times();
    let mut group = c.benchmark_group("format_time");

    for (name, t) in &times {
        group.bench_with_input(BenchmarkId::new("old_chrono", name), t, |b, t| {
            let mut buf = String::with_capacity(64);
            b.iter(|| {
                buf.clear();
                old_format_time(black_box(&mut buf), black_box(*t));
                black_box(&buf);
            });
        });
        group.bench_with_input(BenchmarkId::new("new_direct", name), t, |b, t| {
            let mut buf = String::with_capacity(64);
            b.iter(|| {
                buf.clear();
                strconv::format_time(black_box(&mut buf), black_box(*t));
                black_box(&buf);
            });
        });
    }
    group.finish();
}

fn bench_batch_timestamp(c: &mut Criterion) {
    let timestamps = make_test_timestamps();
    let mut group = c.benchmark_group("batch_timestamp");

    // Simulate pgwire: format 10k timestamps to a shared buffer
    let batch: Vec<_> = timestamps
        .iter()
        .cycle()
        .take(10_000)
        .map(|(_, ts)| *ts)
        .collect();

    group.bench_function("old_chrono_10k", |b| {
        let mut buf = String::with_capacity(32 * 10_000);
        b.iter(|| {
            buf.clear();
            for ts in &batch {
                old_format_timestamp(black_box(&mut buf), black_box(ts));
                buf.push('\n');
            }
            black_box(&buf);
        });
    });

    group.bench_function("new_direct_10k", |b| {
        let mut buf = String::with_capacity(32 * 10_000);
        b.iter(|| {
            buf.clear();
            for ts in &batch {
                strconv::format_timestamp(black_box(&mut buf), black_box(ts));
                buf.push('\n');
            }
            black_box(&buf);
        });
    });

    let batch_tz: Vec<_> = batch
        .iter()
        .map(|ts| DateTime::from_naive_utc_and_offset(*ts, Utc))
        .collect();

    group.bench_function("old_chrono_10k_tz", |b| {
        let mut buf = String::with_capacity(32 * 10_000);
        b.iter(|| {
            buf.clear();
            for ts in &batch_tz {
                old_format_timestamptz(black_box(&mut buf), black_box(ts));
                buf.push('\n');
            }
            black_box(&buf);
        });
    });

    group.bench_function("new_direct_10k_tz", |b| {
        let mut buf = String::with_capacity(32 * 10_000);
        b.iter(|| {
            buf.clear();
            for ts in &batch_tz {
                strconv::format_timestamptz(black_box(&mut buf), black_box(ts));
                buf.push('\n');
            }
            black_box(&buf);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_format_timestamp,
    bench_format_timestamptz,
    bench_format_date,
    bench_format_time,
    bench_batch_timestamp,
);
criterion_main!(benches);
