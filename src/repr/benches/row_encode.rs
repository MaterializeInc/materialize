// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for Row::push_datum encoding throughput.
//!
//! Measures the performance of encoding various datum types into Row storage,
//! testing the combined sign-check + single-write optimization for integers,
//! combined tag+value buffers for fixed-size types, and inlined length encoding
//! for strings/bytes.

use std::hint::black_box;

use chrono::NaiveDate;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Row};
use ordered_float::OrderedFloat;

const NUM_ROWS: usize = 100_000;
const COLS_PER_ROW: usize = 6;

fn bench_encode_int64(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_int64");
    group.throughput(Throughput::Elements((NUM_ROWS * COLS_PER_ROW) as u64));

    // Mix of small and large values to exercise different byte widths
    let datums_per_row: Vec<Vec<Datum>> = (0..NUM_ROWS)
        .map(|i| {
            vec![
                Datum::Int64(i as i64),                 // small positive
                Datum::Int64(-(i as i64) - 1),          // small negative
                Datum::Int64(i as i64 * 100_000),       // medium
                Datum::Int64(i as i64 * 1_000_000_000), // large
                Datum::Int64(127 - (i as i64 % 255)),   // around 1-byte boundary
                Datum::Int64(32000 + i as i64),         // around 2-byte boundary
            ]
        })
        .collect();

    group.bench_function("pack", |b| {
        b.iter(|| {
            let mut row = Row::default();
            for datums in &datums_per_row {
                row.packer().extend(datums.iter());
                black_box(&row);
            }
        })
    });

    group.finish();
}

fn bench_encode_int32(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_int32");
    group.throughput(Throughput::Elements((NUM_ROWS * COLS_PER_ROW) as u64));

    let datums_per_row: Vec<Vec<Datum>> = (0..NUM_ROWS)
        .map(|i| {
            vec![
                Datum::Int32(i as i32),
                Datum::Int32(-(i as i32) - 1),
                Datum::Int32((i as i32) * 1000),
                Datum::Int32((i as i32) * 100_000),
                Datum::Int32(127 - (i as i32 % 255)),
                Datum::Int32(32000 + i as i32),
            ]
        })
        .collect();

    group.bench_function("pack", |b| {
        b.iter(|| {
            let mut row = Row::default();
            for datums in &datums_per_row {
                row.packer().extend(datums.iter());
                black_box(&row);
            }
        })
    });

    group.finish();
}

fn bench_encode_float64(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_float64");
    group.throughput(Throughput::Elements((NUM_ROWS * COLS_PER_ROW) as u64));

    let datums_per_row: Vec<Vec<Datum>> = (0..NUM_ROWS)
        .map(|i| {
            let f = i as f64;
            vec![
                Datum::Float64(OrderedFloat(f * 1.1)),
                Datum::Float64(OrderedFloat(-f * 0.5)),
                Datum::Float64(OrderedFloat(f * 1000.0)),
                Datum::Float64(OrderedFloat(0.001 * f)),
                Datum::Float64(OrderedFloat(f64::MAX / (f + 1.0))),
                Datum::Float64(OrderedFloat(f64::MIN / (f + 1.0))),
            ]
        })
        .collect();

    group.bench_function("pack", |b| {
        b.iter(|| {
            let mut row = Row::default();
            for datums in &datums_per_row {
                row.packer().extend(datums.iter());
                black_box(&row);
            }
        })
    });

    group.finish();
}

fn bench_encode_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_string");

    // Short strings (< TINY = 256 bytes) — most common case
    let short_strings: Vec<String> = (0..NUM_ROWS).map(|i| format!("row_{}_value", i)).collect();
    let datums_per_row: Vec<Vec<Datum>> = short_strings
        .iter()
        .map(|s| {
            vec![
                Datum::String(s.as_str()),
                Datum::String("hello"),
                Datum::String("world"),
                Datum::String(s.as_str()),
                Datum::String("short"),
                Datum::String("test"),
            ]
        })
        .collect();

    group.throughput(Throughput::Elements((NUM_ROWS * COLS_PER_ROW) as u64));

    group.bench_function("short/pack", |b| {
        b.iter(|| {
            let mut row = Row::default();
            for datums in &datums_per_row {
                row.packer().extend(datums.iter());
                black_box(&row);
            }
        })
    });

    // Medium strings (around TINY boundary)
    let medium_string = "x".repeat(200);
    let medium_datums: Vec<Datum> = vec![Datum::String(&medium_string); COLS_PER_ROW];

    group.bench_function("medium/pack", |b| {
        b.iter(|| {
            let mut row = Row::default();
            for _ in 0..NUM_ROWS {
                row.packer().extend(medium_datums.iter());
                black_box(&row);
            }
        })
    });

    group.finish();
}

fn bench_encode_timestamp(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_timestamp");
    group.throughput(Throughput::Elements((NUM_ROWS * COLS_PER_ROW) as u64));

    let base = NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let ts = CheckedTimestamp::from_timestamplike(base).unwrap();
    let datum = Datum::Timestamp(ts);
    let datums: Vec<Datum> = vec![datum; COLS_PER_ROW];

    group.bench_function("pack", |b| {
        b.iter(|| {
            let mut row = Row::default();
            for _ in 0..NUM_ROWS {
                row.packer().extend(datums.iter());
                black_box(&row);
            }
        })
    });

    group.finish();
}

fn bench_encode_date(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_date");
    group.throughput(Throughput::Elements((NUM_ROWS * COLS_PER_ROW) as u64));

    let datums_per_row: Vec<Vec<Datum>> = (0..NUM_ROWS)
        .map(|i| {
            let day = (i % 365) as i32;
            vec![Datum::Date(Date::from_pg_epoch(day).unwrap()); COLS_PER_ROW]
        })
        .collect();

    group.bench_function("pack", |b| {
        b.iter(|| {
            let mut row = Row::default();
            for datums in &datums_per_row {
                row.packer().extend(datums.iter());
                black_box(&row);
            }
        })
    });

    group.finish();
}

fn bench_encode_interval(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_interval");
    group.throughput(Throughput::Elements((NUM_ROWS * COLS_PER_ROW) as u64));

    let datums_per_row: Vec<Vec<Datum>> = (0..NUM_ROWS)
        .map(|i| {
            vec![
                Datum::Interval(Interval::new(i as i32, i as i32 % 30, i as i64 * 1_000_000)),
                Datum::Interval(Interval::new(0, 0, i as i64 * 60_000_000)),
                Datum::Interval(Interval::new(12, 0, 0)),
                Datum::Interval(Interval::new(i as i32, i as i32, i as i64)),
                Datum::Interval(Interval::new(0, 1, 86400_000_000)),
                Datum::Interval(Interval::new(-(i as i32), 0, -(i as i64))),
            ]
        })
        .collect();

    group.bench_function("pack", |b| {
        b.iter(|| {
            let mut row = Row::default();
            for datums in &datums_per_row {
                row.packer().extend(datums.iter());
                black_box(&row);
            }
        })
    });

    group.finish();
}

fn bench_encode_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_mixed");
    // 7 datums per row in the mixed case
    group.throughput(Throughput::Elements((NUM_ROWS * 7) as u64));

    let base_ts = NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_opt(10, 30, 0)
        .unwrap();
    let ts = CheckedTimestamp::from_timestamplike(base_ts).unwrap();

    let strings: Vec<String> = (0..NUM_ROWS).map(|i| format!("user_{}", i)).collect();

    let datums_per_row: Vec<Vec<Datum>> = (0..NUM_ROWS)
        .map(|i| {
            vec![
                Datum::Int64(i as i64),
                Datum::String(strings[i].as_str()),
                Datum::Float64(OrderedFloat(i as f64 * 1.5)),
                Datum::Timestamp(ts),
                Datum::Date(Date::from_pg_epoch((i % 365) as i32).unwrap()),
                Datum::Int32(i as i32),
                Datum::Interval(Interval::new(0, 0, i as i64 * 1_000_000)),
            ]
        })
        .collect();

    group.bench_function("pack", |b| {
        b.iter(|| {
            let mut row = Row::default();
            for datums in &datums_per_row {
                row.packer().extend(datums.iter());
                black_box(&row);
            }
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_encode_int64,
    bench_encode_int32,
    bench_encode_float64,
    bench_encode_string,
    bench_encode_timestamp,
    bench_encode_date,
    bench_encode_interval,
    bench_encode_mixed,
);
criterion_main!(benches);
