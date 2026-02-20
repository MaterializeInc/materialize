// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for timestamp/date/time parsing: fast-path vs general parser.

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use mz_repr::strconv;

fn bench_parse_timestamp(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_timestamp");

    let cases = vec![
        ("no_frac", "2024-06-15 14:30:25"),
        ("micros", "2024-06-15 14:30:25.123456"),
        ("millis", "2024-06-15 14:30:25.123"),
        ("nanos", "2024-06-15 14:30:25.123456789"),
        ("epoch_ts", "1970-01-01 00:00:00"),
        ("y2k", "2000-01-01 00:00:00"),
        ("t_separator", "2024-06-15T14:30:25.123456"),
    ];

    for (name, s) in &cases {
        group.bench_function(format!("old_{}", name), |b| {
            b.iter(|| strconv::parse_timestamp_general(black_box(s)).unwrap())
        });
        group.bench_function(format!("new_{}", name), |b| {
            b.iter(|| strconv::parse_timestamp(black_box(s)).unwrap())
        });
    }

    group.finish();
}

fn bench_parse_timestamptz(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_timestamptz");

    let cases = vec![
        ("utc_short", "2024-06-15 14:30:25+00"),
        ("utc_full", "2024-06-15 14:30:25+00:00"),
        ("neg_offset", "2024-06-15 14:30:25-05:30"),
        ("micros_utc", "2024-06-15 14:30:25.123456+00"),
        ("micros_offset", "2024-06-15 14:30:25.123456-05:30"),
    ];

    for (name, s) in &cases {
        group.bench_function(format!("old_{}", name), |b| {
            b.iter(|| strconv::parse_timestamptz_general(black_box(s)).unwrap())
        });
        group.bench_function(format!("new_{}", name), |b| {
            b.iter(|| strconv::parse_timestamptz(black_box(s)).unwrap())
        });
    }

    group.finish();
}

fn bench_parse_date(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_date");

    let cases = vec![
        ("typical", "2024-06-15"),
        ("epoch", "1970-01-01"),
        ("year_end", "2024-12-31"),
    ];

    for (name, s) in &cases {
        group.bench_function(format!("old_{}", name), |b| {
            b.iter(|| strconv::parse_date_general(black_box(s)).unwrap())
        });
        group.bench_function(format!("new_{}", name), |b| {
            b.iter(|| strconv::parse_date(black_box(s)).unwrap())
        });
    }

    group.finish();
}

fn bench_parse_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_time");

    let cases = vec![
        ("no_frac", "14:30:25"),
        ("micros", "14:30:25.123456"),
        ("midnight", "00:00:00"),
    ];

    for (name, s) in &cases {
        group.bench_function(format!("old_{}", name), |b| {
            b.iter(|| strconv::parse_time_general(black_box(s)).unwrap())
        });
        group.bench_function(format!("new_{}", name), |b| {
            b.iter(|| strconv::parse_time(black_box(s)).unwrap())
        });
    }

    group.finish();
}

fn bench_parse_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_batch");

    // 10K timestamps with microseconds
    let timestamps: Vec<String> = (0..10_000)
        .map(|i| {
            let year = 2000 + (i % 25);
            let month = 1 + (i % 12);
            let day = 1 + (i % 28);
            let hour = i % 24;
            let minute = i % 60;
            let second = i % 60;
            let micros = i * 37 % 1_000_000;
            format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                year, month, day, hour, minute, second, micros
            )
        })
        .collect();

    group.bench_function("old_10k_timestamps", |b| {
        b.iter(|| {
            for s in &timestamps {
                black_box(strconv::parse_timestamp_general(black_box(s)).unwrap());
            }
        })
    });

    group.bench_function("new_10k_timestamps", |b| {
        b.iter(|| {
            for s in &timestamps {
                black_box(strconv::parse_timestamp(black_box(s)).unwrap());
            }
        })
    });

    // 10K timestamptz
    let timestamptzs: Vec<String> = (0..10_000)
        .map(|i| {
            let year = 2000 + (i % 25);
            let month = 1 + (i % 12);
            let day = 1 + (i % 28);
            let hour = i % 24;
            let minute = i % 60;
            let second = i % 60;
            let micros = i * 37 % 1_000_000;
            let tz_hour = (i % 13) as i32 - 6;
            format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}{:+03}:00",
                year, month, day, hour, minute, second, micros, tz_hour
            )
        })
        .collect();

    group.bench_function("old_10k_timestamptz", |b| {
        b.iter(|| {
            for s in &timestamptzs {
                black_box(strconv::parse_timestamptz_general(black_box(s)).unwrap());
            }
        })
    });

    group.bench_function("new_10k_timestamptz", |b| {
        b.iter(|| {
            for s in &timestamptzs {
                black_box(strconv::parse_timestamptz(black_box(s)).unwrap());
            }
        })
    });

    // 10K dates
    let dates: Vec<String> = (0..10_000)
        .map(|i| {
            let year = 2000 + (i % 25);
            let month = 1 + (i % 12);
            let day = 1 + (i % 28);
            format!("{:04}-{:02}-{:02}", year, month, day)
        })
        .collect();

    group.bench_function("old_10k_dates", |b| {
        b.iter(|| {
            for s in &dates {
                black_box(strconv::parse_date_general(black_box(s)).unwrap());
            }
        })
    });

    group.bench_function("new_10k_dates", |b| {
        b.iter(|| {
            for s in &dates {
                black_box(strconv::parse_date(black_box(s)).unwrap());
            }
        })
    });

    // 10K times
    let times: Vec<String> = (0..10_000)
        .map(|i| {
            let hour = i % 24;
            let minute = i % 60;
            let second = i % 60;
            let micros = i * 37 % 1_000_000;
            format!("{:02}:{:02}:{:02}.{:06}", hour, minute, second, micros)
        })
        .collect();

    group.bench_function("old_10k_times", |b| {
        b.iter(|| {
            for s in &times {
                black_box(strconv::parse_time_general(black_box(s)).unwrap());
            }
        })
    });

    group.bench_function("new_10k_times", |b| {
        b.iter(|| {
            for s in &times {
                black_box(strconv::parse_time(black_box(s)).unwrap());
            }
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_parse_timestamp,
    bench_parse_timestamptz,
    bench_parse_date,
    bench_parse_time,
    bench_parse_batch,
);
criterion_main!(benches);
