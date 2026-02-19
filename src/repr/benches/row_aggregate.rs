// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for row datum access patterns used in accumulable reductions.
//!
//! Compares the old enumerate+while pattern (read_datum for every column)
//! against the new nth() pattern (skip_datum for skipped columns).

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mz_repr::{Datum, Row};

/// Build a row with N int64 columns
fn make_int_row(ncols: usize) -> Row {
    let mut row = Row::default();
    let mut packer = row.packer();
    for i in 0..ncols {
        packer.push(Datum::Int64(i as i64 * 100));
    }
    row
}

/// Build a row with N timestamp columns
fn make_ts_row(ncols: usize) -> Row {
    let mut row = Row::default();
    let mut packer = row.packer();
    for i in 0..ncols {
        let ts = chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_micro_opt(12, 30, i as u32 % 60, 0)
            .unwrap();
        let checked =
            mz_repr::adt::timestamp::CheckedTimestamp::from_timestamplike(ts).unwrap();
        packer.push(Datum::Timestamp(checked));
    }
    row
}

/// Build a mixed row: int, string, timestamp, int, string, bool, ...
fn make_mixed_row(ncols: usize) -> Row {
    let mut row = Row::default();
    let mut packer = row.packer();
    for i in 0..ncols {
        match i % 6 {
            0 => packer.push(Datum::Int64(i as i64 * 42)),
            1 => packer.push(Datum::String("hello_world")),
            2 => {
                let ts = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                let checked =
                    mz_repr::adt::timestamp::CheckedTimestamp::from_timestamplike(ts).unwrap();
                packer.push(Datum::Timestamp(checked));
            }
            3 => packer.push(Datum::Int32(42)),
            4 => packer.push(Datum::String("some_longer_string_value")),
            5 => packer.push(Datum::True),
            _ => unreachable!(),
        }
    }
    row
}

/// Old pattern: enumerate + while loop (uses read_datum for every column)
fn extract_enumerate_while<'a>(row: &'a Row, aggr_indices: &[usize]) -> Vec<Datum<'a>> {
    let mut results = Vec::with_capacity(aggr_indices.len());
    let mut row_iter = row.iter().enumerate();
    for datum_index in aggr_indices {
        let mut datum = row_iter.next().unwrap();
        while datum_index != &datum.0 {
            datum = row_iter.next().unwrap();
        }
        results.push(datum.1);
    }
    results
}

/// New pattern: nth() calls (uses skip_datum for skipped columns)
fn extract_nth<'a>(row: &'a Row, aggr_indices: &[usize]) -> Vec<Datum<'a>> {
    let mut results = Vec::with_capacity(aggr_indices.len());
    let mut row_iter = row.iter();
    let mut prev_index: usize = 0;
    for (i, datum_index) in aggr_indices.iter().enumerate() {
        let skip = if i == 0 {
            *datum_index
        } else {
            *datum_index - prev_index - 1
        };
        let datum = row_iter.nth(skip).unwrap();
        prev_index = *datum_index;
        results.push(datum);
    }
    results
}

fn bench_aggregate_access(c: &mut Criterion) {
    // --- Single aggregation: extract one column from various positions ---

    // int64, 10 cols, aggregate on col 0 (first)
    let row = make_int_row(10);
    c.bench_function("agg_int10_col0_old", |b| {
        b.iter(|| extract_enumerate_while(black_box(&row), black_box(&[0])))
    });
    c.bench_function("agg_int10_col0_new", |b| {
        b.iter(|| extract_nth(black_box(&row), black_box(&[0])))
    });

    // int64, 10 cols, aggregate on col 9 (last)
    c.bench_function("agg_int10_col9_old", |b| {
        b.iter(|| extract_enumerate_while(black_box(&row), black_box(&[9])))
    });
    c.bench_function("agg_int10_col9_new", |b| {
        b.iter(|| extract_nth(black_box(&row), black_box(&[9])))
    });

    // int64, 20 cols, aggregate on col 15
    let row20 = make_int_row(20);
    c.bench_function("agg_int20_col15_old", |b| {
        b.iter(|| extract_enumerate_while(black_box(&row20), black_box(&[15])))
    });
    c.bench_function("agg_int20_col15_new", |b| {
        b.iter(|| extract_nth(black_box(&row20), black_box(&[15])))
    });

    // timestamp, 10 cols, aggregate on col 8
    let ts_row = make_ts_row(10);
    c.bench_function("agg_ts10_col8_old", |b| {
        b.iter(|| extract_enumerate_while(black_box(&ts_row), black_box(&[8])))
    });
    c.bench_function("agg_ts10_col8_new", |b| {
        b.iter(|| extract_nth(black_box(&ts_row), black_box(&[8])))
    });

    // --- Multiple aggregations: extract several columns ---

    // int64, 20 cols, aggregate on cols 3, 7, 15
    c.bench_function("agg_int20_cols_3_7_15_old", |b| {
        b.iter(|| extract_enumerate_while(black_box(&row20), black_box(&[3, 7, 15])))
    });
    c.bench_function("agg_int20_cols_3_7_15_new", |b| {
        b.iter(|| extract_nth(black_box(&row20), black_box(&[3, 7, 15])))
    });

    // mixed, 20 cols, aggregate on cols 0, 6, 12, 18
    let mixed_row = make_mixed_row(20);
    c.bench_function("agg_mixed20_cols_0_6_12_18_old", |b| {
        b.iter(|| {
            extract_enumerate_while(black_box(&mixed_row), black_box(&[0, 6, 12, 18]))
        })
    });
    c.bench_function("agg_mixed20_cols_0_6_12_18_new", |b| {
        b.iter(|| extract_nth(black_box(&mixed_row), black_box(&[0, 6, 12, 18])))
    });

    // timestamp, 10 cols, aggregate on cols 2, 5, 8
    c.bench_function("agg_ts10_cols_2_5_8_old", |b| {
        b.iter(|| extract_enumerate_while(black_box(&ts_row), black_box(&[2, 5, 8])))
    });
    c.bench_function("agg_ts10_cols_2_5_8_new", |b| {
        b.iter(|| extract_nth(black_box(&ts_row), black_box(&[2, 5, 8])))
    });

    // --- Batch: 10k rows ---

    let rows: Vec<Row> = (0..10_000).map(|_| make_int_row(20)).collect();
    c.bench_function("batch_10k_int20_col15_old", |b| {
        b.iter(|| {
            let mut sum = 0i64;
            for row in &rows {
                let datums = extract_enumerate_while(black_box(row), &[15]);
                sum += datums[0].unwrap_int64();
            }
            black_box(sum)
        })
    });
    c.bench_function("batch_10k_int20_col15_new", |b| {
        b.iter(|| {
            let mut sum = 0i64;
            for row in &rows {
                let datums = extract_nth(black_box(row), &[15]);
                sum += datums[0].unwrap_int64();
            }
            black_box(sum)
        })
    });

    let ts_rows: Vec<Row> = (0..10_000).map(|_| make_ts_row(10)).collect();
    c.bench_function("batch_10k_ts10_col8_old", |b| {
        b.iter(|| {
            let mut count = 0u64;
            for row in &ts_rows {
                let datums = extract_enumerate_while(black_box(row), &[8]);
                black_box(datums[0]);
                count += 1;
            }
            black_box(count)
        })
    });
    c.bench_function("batch_10k_ts10_col8_new", |b| {
        b.iter(|| {
            let mut count = 0u64;
            for row in &ts_rows {
                let datums = extract_nth(black_box(row), &[8]);
                black_box(datums[0]);
                count += 1;
            }
            black_box(count)
        })
    });

    let mixed_rows: Vec<Row> = (0..10_000).map(|_| make_mixed_row(20)).collect();
    c.bench_function("batch_10k_mixed20_cols_0_6_12_18_old", |b| {
        b.iter(|| {
            let mut count = 0u64;
            for row in &mixed_rows {
                let datums =
                    extract_enumerate_while(black_box(row), &[0, 6, 12, 18]);
                black_box(&datums);
                count += 1;
            }
            black_box(count)
        })
    });
    c.bench_function("batch_10k_mixed20_cols_0_6_12_18_new", |b| {
        b.iter(|| {
            let mut count = 0u64;
            for row in &mixed_rows {
                let datums = extract_nth(black_box(row), &[0, 6, 12, 18]);
                black_box(&datums);
                count += 1;
            }
            black_box(count)
        })
    });
}

criterion_group!(benches, bench_aggregate_access);
criterion_main!(benches);
