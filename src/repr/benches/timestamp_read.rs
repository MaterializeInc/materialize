// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for timestamp read_datum optimization.
//!
//! Measures the cost of reading timestamp datums from Row storage,
//! which was optimized by:
//! 1. Skipping redundant CheckedTimestamp range validation on read
//!    (data was already validated on write)
//! 2. Avoiding a second integer division for nanosecond extraction
//!    (compute remainder from quotient instead)

use std::convert::TryInto;
use std::hint::black_box;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use criterion::{Criterion, criterion_group, criterion_main};
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Row};

/// Simulate the OLD read_datum path for CheapTimestamp:
/// - Uses rem_euclid (second integer division) for nanoseconds
/// - Calls from_timestamplike which validates date range
#[inline(never)]
fn old_decode_timestamp(nanos: i64) -> CheckedTimestamp<NaiveDateTime> {
    let secs = nanos.div_euclid(1_000_000_000);
    let nsecs: u32 = nanos.rem_euclid(1_000_000_000).try_into().unwrap();
    let ndt = DateTime::from_timestamp(secs, nsecs)
        .expect("We only write round-trippable timestamps")
        .naive_utc();
    CheckedTimestamp::from_timestamplike(ndt).expect("unexpected timestamp")
}

/// NEW optimized read_datum path for CheapTimestamp:
/// - Computes remainder from quotient (avoids second division)
/// - Uses unchecked constructor (skips redundant validation)
#[inline(never)]
fn new_decode_timestamp(nanos: i64) -> CheckedTimestamp<NaiveDateTime> {
    let secs = nanos.div_euclid(1_000_000_000);
    let nsecs = (nanos - secs * 1_000_000_000) as u32;
    let ndt = DateTime::from_timestamp(secs, nsecs)
        .expect("We only write round-trippable timestamps")
        .naive_utc();
    CheckedTimestamp::from_timestamplike_unchecked(ndt)
}

/// OLD path for CheapTimestampTz
#[inline(never)]
fn old_decode_timestamptz(nanos: i64) -> CheckedTimestamp<DateTime<Utc>> {
    let secs = nanos.div_euclid(1_000_000_000);
    let nsecs: u32 = nanos.rem_euclid(1_000_000_000).try_into().unwrap();
    let dt = DateTime::from_timestamp(secs, nsecs)
        .expect("We only write round-trippable timestamps");
    CheckedTimestamp::from_timestamplike(dt).expect("unexpected timestamp")
}

/// NEW path for CheapTimestampTz
#[inline(never)]
fn new_decode_timestamptz(nanos: i64) -> CheckedTimestamp<DateTime<Utc>> {
    let secs = nanos.div_euclid(1_000_000_000);
    let nsecs = (nanos - secs * 1_000_000_000) as u32;
    let dt = DateTime::from_timestamp(secs, nsecs)
        .expect("We only write round-trippable timestamps");
    CheckedTimestamp::from_timestamplike_unchecked(dt)
}

/// Build a row with N CheapTimestamp columns (typical case: epoch nanos fit in i64).
fn make_timestamp_row(ncols: usize) -> Row {
    let ts = NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_micro_opt(14, 30, 45, 123456)
        .unwrap();
    let checked = CheckedTimestamp::from_timestamplike(ts).unwrap();
    let datums: Vec<Datum> = (0..ncols).map(|_| Datum::Timestamp(checked)).collect();
    Row::pack_slice(&datums)
}

/// Build a row with N CheapTimestampTz columns.
fn make_timestamptz_row(ncols: usize) -> Row {
    let dt = DateTime::from_timestamp(1_700_000_000, 500_000_000).unwrap();
    let checked = CheckedTimestamp::from_timestamplike(dt).unwrap();
    let datums: Vec<Datum> = (0..ncols).map(|_| Datum::TimestampTz(checked)).collect();
    Row::pack_slice(&datums)
}

/// Benchmark: isolated decode function comparison (old vs new).
fn bench_decode_isolated(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_isolated");

    // Typical timestamp: 2024-06-15T14:30:45.123456Z
    let typical_nanos: i64 = 1_718_459_445_123_456_000;
    // Epoch timestamp: 1970-01-01T00:00:00Z
    let epoch_nanos: i64 = 0;
    // Pre-epoch: 1969-07-20T20:17:00Z (Apollo 11)
    let pre_epoch_nanos: i64 = -14_182_980_000_000_000;
    // Y2K: 2000-01-01T00:00:00Z
    let y2k_nanos: i64 = 946_684_800_000_000_000;

    for (name, nanos) in [
        ("typical", typical_nanos),
        ("epoch", epoch_nanos),
        ("pre_epoch", pre_epoch_nanos),
        ("y2k", y2k_nanos),
    ] {
        group.bench_function(&format!("old_timestamp_{}", name), |b| {
            b.iter(|| black_box(old_decode_timestamp(black_box(nanos))))
        });
        group.bench_function(&format!("new_timestamp_{}", name), |b| {
            b.iter(|| black_box(new_decode_timestamp(black_box(nanos))))
        });
    }

    // TimestampTz variants
    for (name, nanos) in [
        ("typical", typical_nanos),
        ("epoch", epoch_nanos),
        ("pre_epoch", pre_epoch_nanos),
    ] {
        group.bench_function(&format!("old_timestamptz_{}", name), |b| {
            b.iter(|| black_box(old_decode_timestamptz(black_box(nanos))))
        });
        group.bench_function(&format!("new_timestamptz_{}", name), |b| {
            b.iter(|| black_box(new_decode_timestamptz(black_box(nanos))))
        });
    }

    group.finish();
}

/// Benchmark: iterate and read all datums in a timestamp row.
/// Uses the optimized read_datum path (from row.iter()).
fn bench_read_timestamps(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_timestamp");

    // Single timestamp read
    {
        let row = make_timestamp_row(1);
        group.bench_function("single_timestamp", |b| {
            b.iter(|| {
                let val = row.iter().next().unwrap();
                black_box(val);
            })
        });
    }

    // Single timestamptz read
    {
        let row = make_timestamptz_row(1);
        group.bench_function("single_timestamptz", |b| {
            b.iter(|| {
                let val = row.iter().next().unwrap();
                black_box(val);
            })
        });
    }

    // Read all datums from 5-column timestamp row
    {
        let row = make_timestamp_row(5);
        group.bench_function("iter_5col_timestamp", |b| {
            b.iter(|| {
                for datum in row.iter() {
                    black_box(datum);
                }
            })
        });
    }

    // Read all datums from 10-column timestamp row
    {
        let row = make_timestamp_row(10);
        group.bench_function("iter_10col_timestamp", |b| {
            b.iter(|| {
                for datum in row.iter() {
                    black_box(datum);
                }
            })
        });
    }

    // Read all datums from 10-column timestamptz row
    {
        let row = make_timestamptz_row(10);
        group.bench_function("iter_10col_timestamptz", |b| {
            b.iter(|| {
                for datum in row.iter() {
                    black_box(datum);
                }
            })
        });
    }

    // unpack() - measures count() + full iteration
    {
        let row = make_timestamp_row(10);
        group.bench_function("unpack_10col_timestamp", |b| {
            b.iter(|| {
                let datums = row.unpack();
                black_box(datums);
            })
        });
    }

    group.finish();
}

/// Benchmark: batch of 10k rows (old decode vs new decode).
fn bench_batch_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_decode");

    let typical_nanos: i64 = 1_718_459_445_123_456_000;

    // 10k decodes, simulating iterating over timestamp column
    let nanos_values: Vec<i64> = (0..10_000)
        .map(|i| typical_nanos + i * 1_000_000_000)
        .collect();

    group.bench_function("old_10k_timestamp", |b| {
        b.iter(|| {
            for &nanos in &nanos_values {
                black_box(old_decode_timestamp(nanos));
            }
        })
    });

    group.bench_function("new_10k_timestamp", |b| {
        b.iter(|| {
            for &nanos in &nanos_values {
                black_box(new_decode_timestamp(nanos));
            }
        })
    });

    group.bench_function("old_10k_timestamptz", |b| {
        b.iter(|| {
            for &nanos in &nanos_values {
                black_box(old_decode_timestamptz(nanos));
            }
        })
    });

    group.bench_function("new_10k_timestamptz", |b| {
        b.iter(|| {
            for &nanos in &nanos_values {
                black_box(new_decode_timestamptz(nanos));
            }
        })
    });

    // Full row iteration: 10k rows x 5 timestamp columns
    {
        let rows: Vec<Row> = (0..10_000).map(|_| make_timestamp_row(5)).collect();
        group.bench_function("10k_5col_row_iter", |b| {
            b.iter(|| {
                for row in &rows {
                    for datum in row.iter() {
                        black_box(datum);
                    }
                }
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_decode_isolated,
    bench_read_timestamps,
    bench_batch_decode,
);
criterion_main!(benches);
