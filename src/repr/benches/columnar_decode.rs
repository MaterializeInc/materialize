// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for persist columnar decode (Arrow → Row).
//!
//! Measures the throughput of DatumColumnDecoder::get() which decodes Arrow
//! columnar data back into Materialize's Row format. The direct-push
//! optimization bypasses Datum construction for scalar types, eliminating
//! the double enum dispatch in the decode path.

use std::hint::black_box;

use chrono::NaiveDate;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mz_persist_types::columnar::{ColumnDecoder, ColumnEncoder, Schema};
use mz_repr::adt::date::Date;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, RelationDesc, Row, SqlScalarType};
use ordered_float::OrderedFloat;

const NUM_ROWS: usize = 10_000;

/// Benchmark decoding rows with Int64 columns from columnar format.
fn bench_decode_int64(c: &mut Criterion) {
    let mut group = c.benchmark_group("columnar_decode_int64");
    let cols = 6;
    group.throughput(Throughput::Elements((NUM_ROWS * cols) as u64));

    let desc = RelationDesc::builder()
        .with_column("a", SqlScalarType::Int64.nullable(true))
        .with_column("b", SqlScalarType::Int64.nullable(true))
        .with_column("c", SqlScalarType::Int64.nullable(true))
        .with_column("d", SqlScalarType::Int64.nullable(true))
        .with_column("e", SqlScalarType::Int64.nullable(true))
        .with_column("f", SqlScalarType::Int64.nullable(true))
        .finish();

    // Encode rows into columnar format
    let mut encoder = <RelationDesc as Schema<Row>>::encoder(&desc).unwrap();
    let mut row = Row::default();
    for i in 0..NUM_ROWS {
        let mut packer = row.packer();
        packer.push(Datum::Int64(i as i64));
        packer.push(Datum::Int64(-(i as i64) - 1));
        packer.push(Datum::Int64(i as i64 * 100_000));
        packer.push(Datum::Int64(i as i64 * 1_000_000_000));
        packer.push(Datum::Int64(127 - (i as i64 % 255)));
        packer.push(Datum::Int64(32000 + i as i64));
        encoder.append(&row);
    }
    let col = encoder.finish();
    let decoder = <RelationDesc as Schema<Row>>::decoder(&desc, col).unwrap();

    group.bench_function("decode", |b| {
        b.iter(|| {
            let mut out = Row::default();
            for idx in 0..NUM_ROWS {
                decoder.decode(idx, &mut out);
                black_box(&out);
            }
        })
    });

    group.finish();
}

/// Benchmark decoding rows with String columns from columnar format.
fn bench_decode_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("columnar_decode_string");
    let cols = 4;
    group.throughput(Throughput::Elements((NUM_ROWS * cols) as u64));

    let desc = RelationDesc::builder()
        .with_column("a", SqlScalarType::String.nullable(true))
        .with_column("b", SqlScalarType::String.nullable(true))
        .with_column("c", SqlScalarType::String.nullable(true))
        .with_column("d", SqlScalarType::String.nullable(true))
        .finish();

    let strings: Vec<String> = (0..NUM_ROWS).map(|i| format!("row_{}_value", i)).collect();
    let mut encoder = <RelationDesc as Schema<Row>>::encoder(&desc).unwrap();
    let mut row = Row::default();
    for i in 0..NUM_ROWS {
        let mut packer = row.packer();
        packer.push(Datum::String(&strings[i]));
        packer.push(Datum::String("hello"));
        packer.push(Datum::String("world"));
        packer.push(Datum::String(&strings[i]));
        encoder.append(&row);
    }
    let col = encoder.finish();
    let decoder = <RelationDesc as Schema<Row>>::decoder(&desc, col).unwrap();

    group.bench_function("decode", |b| {
        b.iter(|| {
            let mut out = Row::default();
            for idx in 0..NUM_ROWS {
                decoder.decode(idx, &mut out);
                black_box(&out);
            }
        })
    });

    group.finish();
}

/// Benchmark decoding rows with Float64 columns from columnar format.
fn bench_decode_float64(c: &mut Criterion) {
    let mut group = c.benchmark_group("columnar_decode_float64");
    let cols = 4;
    group.throughput(Throughput::Elements((NUM_ROWS * cols) as u64));

    let desc = RelationDesc::builder()
        .with_column("a", SqlScalarType::Float64.nullable(true))
        .with_column("b", SqlScalarType::Float64.nullable(true))
        .with_column("c", SqlScalarType::Float64.nullable(true))
        .with_column("d", SqlScalarType::Float64.nullable(true))
        .finish();

    let mut encoder = <RelationDesc as Schema<Row>>::encoder(&desc).unwrap();
    let mut row = Row::default();
    for i in 0..NUM_ROWS {
        let f = i as f64;
        let mut packer = row.packer();
        packer.push(Datum::Float64(OrderedFloat(f * 1.1)));
        packer.push(Datum::Float64(OrderedFloat(-f * 0.5)));
        packer.push(Datum::Float64(OrderedFloat(f * 1000.0)));
        packer.push(Datum::Float64(OrderedFloat(0.001 * f)));
        encoder.append(&row);
    }
    let col = encoder.finish();
    let decoder = <RelationDesc as Schema<Row>>::decoder(&desc, col).unwrap();

    group.bench_function("decode", |b| {
        b.iter(|| {
            let mut out = Row::default();
            for idx in 0..NUM_ROWS {
                decoder.decode(idx, &mut out);
                black_box(&out);
            }
        })
    });

    group.finish();
}

/// Benchmark decoding a mixed-type row (Int64, String, Float64, Bool, Date)
/// from columnar format. This is the most realistic scenario.
fn bench_decode_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("columnar_decode_mixed");
    let cols = 6;
    group.throughput(Throughput::Elements((NUM_ROWS * cols) as u64));

    let desc = RelationDesc::builder()
        .with_column("id", SqlScalarType::Int64.nullable(true))
        .with_column("name", SqlScalarType::String.nullable(true))
        .with_column("score", SqlScalarType::Float64.nullable(true))
        .with_column("active", SqlScalarType::Bool.nullable(true))
        .with_column("day", SqlScalarType::Date.nullable(true))
        .with_column("seq", SqlScalarType::Int32.nullable(true))
        .finish();

    let base_ts = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let _ = base_ts; // just to keep chrono import used
    let strings: Vec<String> = (0..NUM_ROWS).map(|i| format!("user_{}", i)).collect();

    let mut encoder = <RelationDesc as Schema<Row>>::encoder(&desc).unwrap();
    let mut row = Row::default();
    for i in 0..NUM_ROWS {
        let mut packer = row.packer();
        packer.push(Datum::Int64(i as i64));
        packer.push(Datum::String(&strings[i]));
        packer.push(Datum::Float64(OrderedFloat(i as f64 * 1.5)));
        packer.push(if i % 3 == 0 { Datum::True } else { Datum::False });
        packer.push(Datum::Date(
            Date::from_pg_epoch((i % 365) as i32).unwrap(),
        ));
        packer.push(Datum::Int32(i as i32));
        encoder.append(&row);
    }
    let col = encoder.finish();
    let decoder = <RelationDesc as Schema<Row>>::decoder(&desc, col).unwrap();

    group.bench_function("decode", |b| {
        b.iter(|| {
            let mut out = Row::default();
            for idx in 0..NUM_ROWS {
                decoder.decode(idx, &mut out);
                black_box(&out);
            }
        })
    });

    group.finish();
}

/// Benchmark decoding rows with nullable columns (50% null) from columnar format.
fn bench_decode_nullable(c: &mut Criterion) {
    let mut group = c.benchmark_group("columnar_decode_nullable");
    let cols = 4;
    group.throughput(Throughput::Elements((NUM_ROWS * cols) as u64));

    let desc = RelationDesc::builder()
        .with_column("a", SqlScalarType::Int64.nullable(true))
        .with_column("b", SqlScalarType::String.nullable(true))
        .with_column("c", SqlScalarType::Float64.nullable(true))
        .with_column("d", SqlScalarType::Bool.nullable(true))
        .finish();

    let strings: Vec<String> = (0..NUM_ROWS).map(|i| format!("val_{}", i)).collect();
    let mut encoder = <RelationDesc as Schema<Row>>::encoder(&desc).unwrap();
    let mut row = Row::default();
    for i in 0..NUM_ROWS {
        let mut packer = row.packer();
        if i % 2 == 0 {
            packer.push(Datum::Int64(i as i64));
        } else {
            packer.push(Datum::Null);
        }
        if i % 3 == 0 {
            packer.push(Datum::String(&strings[i]));
        } else {
            packer.push(Datum::Null);
        }
        if i % 2 == 1 {
            packer.push(Datum::Float64(OrderedFloat(i as f64)));
        } else {
            packer.push(Datum::Null);
        }
        packer.push(if i % 4 == 0 { Datum::True } else { Datum::Null });
        encoder.append(&row);
    }
    let col = encoder.finish();
    let decoder = <RelationDesc as Schema<Row>>::decoder(&desc, col).unwrap();

    group.bench_function("decode", |b| {
        b.iter(|| {
            let mut out = Row::default();
            for idx in 0..NUM_ROWS {
                decoder.decode(idx, &mut out);
                black_box(&out);
            }
        })
    });

    group.finish();
}

/// Benchmark decoding with Timestamp columns (complex type that still goes
/// through the Datum path, for comparison).
fn bench_decode_timestamp(c: &mut Criterion) {
    let mut group = c.benchmark_group("columnar_decode_timestamp");
    let cols = 4;
    group.throughput(Throughput::Elements((NUM_ROWS * cols) as u64));

    let desc = RelationDesc::builder()
        .with_column("a", SqlScalarType::Timestamp { precision: None }.nullable(true))
        .with_column("b", SqlScalarType::Timestamp { precision: None }.nullable(true))
        .with_column("c", SqlScalarType::Timestamp { precision: None }.nullable(true))
        .with_column("d", SqlScalarType::Timestamp { precision: None }.nullable(true))
        .finish();

    let base = NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let ts = CheckedTimestamp::from_timestamplike(base).unwrap();

    let mut encoder = <RelationDesc as Schema<Row>>::encoder(&desc).unwrap();
    let mut row = Row::default();
    for _i in 0..NUM_ROWS {
        let mut packer = row.packer();
        packer.push(Datum::Timestamp(ts));
        packer.push(Datum::Timestamp(ts));
        packer.push(Datum::Timestamp(ts));
        packer.push(Datum::Timestamp(ts));
        encoder.append(&row);
    }
    let col = encoder.finish();
    let decoder = <RelationDesc as Schema<Row>>::decoder(&desc, col).unwrap();

    group.bench_function("decode", |b| {
        b.iter(|| {
            let mut out = Row::default();
            for idx in 0..NUM_ROWS {
                decoder.decode(idx, &mut out);
                black_box(&out);
            }
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_decode_int64,
    bench_decode_string,
    bench_decode_float64,
    bench_decode_mixed,
    bench_decode_nullable,
    bench_decode_timestamp,
);
criterion_main!(benches);
