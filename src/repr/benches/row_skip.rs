// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for DatumListIter::nth() with skip_datum optimization.
//!
//! Compares the new skip_datum-based nth() against the old approach of calling
//! next() in a loop (which constructs and discards Datum values for each skipped element).

use std::hint::black_box;

use chrono::NaiveDate;
use criterion::{Criterion, criterion_group, criterion_main};
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Row};
use ordered_float::OrderedFloat;

/// Build a row with N int64 columns (values 1..=N).
fn make_int_row(ncols: usize) -> Row {
    let datums: Vec<Datum> = (0..ncols).map(|i| Datum::Int64(i as i64 + 1)).collect();
    Row::pack_slice(&datums)
}

/// Build a row with N timestamp columns.
fn make_timestamp_row(ncols: usize) -> Row {
    let ts = NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_micro_opt(14, 30, 45, 123456)
        .unwrap();
    let checked = CheckedTimestamp::from_timestamplike(ts).unwrap();
    let datums: Vec<Datum> = (0..ncols).map(|_| Datum::Timestamp(checked)).collect();
    Row::pack_slice(&datums)
}

/// Build a row with N string columns of given length.
fn make_string_row(ncols: usize, strlen: usize) -> Row {
    let s: String = (0..strlen).map(|_| 'x').collect();
    let datums: Vec<Datum> = (0..ncols).map(|_| Datum::String(&s)).collect();
    Row::pack_slice(&datums)
}

/// Build a mixed-type row: int64, float64, string, timestamp, bool, repeated.
fn make_mixed_row(ncols: usize) -> Row {
    let ts = NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_opt(14, 30, 45)
        .unwrap();
    let checked = CheckedTimestamp::from_timestamplike(ts).unwrap();
    let s = "hello world";
    let mut datums = Vec::with_capacity(ncols);
    for i in 0..ncols {
        match i % 5 {
            0 => datums.push(Datum::Int64(i as i64)),
            1 => datums.push(Datum::Float64(OrderedFloat(3.14))),
            2 => datums.push(Datum::String(s)),
            3 => datums.push(Datum::Timestamp(checked)),
            4 => datums.push(Datum::True),
            _ => unreachable!(),
        }
    }
    Row::pack_slice(&datums)
}

/// Old approach: simulate Iterator::nth default (call next() in a loop)
#[inline(never)]
fn old_nth<'a>(row: &'a mz_repr::RowRef, n: usize) -> Option<Datum<'a>> {
    let mut iter = row.iter();
    // Simulate the default Iterator::nth: call next() n times, then one more
    for _ in 0..n {
        iter.next()?;
    }
    iter.next()
}

fn bench_nth(c: &mut Criterion) {
    let mut group = c.benchmark_group("nth");

    // int64 rows
    for (ncols, target_idx) in [(5, 4), (10, 9), (20, 19), (20, 10)] {
        let row = make_int_row(ncols);
        group.bench_function(
            &format!("old_int64_{}col_nth_{}", ncols, target_idx),
            |b| {
                b.iter(|| {
                    let val = old_nth(&row, black_box(target_idx)).unwrap();
                    black_box(val);
                })
            },
        );
        group.bench_function(
            &format!("new_int64_{}col_nth_{}", ncols, target_idx),
            |b| {
                b.iter(|| {
                    let val = row.iter().nth(black_box(target_idx)).unwrap();
                    black_box(val);
                })
            },
        );
    }

    // timestamp rows
    for (ncols, target_idx) in [(5, 4), (10, 9), (20, 19)] {
        let row = make_timestamp_row(ncols);
        group.bench_function(
            &format!("old_timestamp_{}col_nth_{}", ncols, target_idx),
            |b| {
                b.iter(|| {
                    let val = old_nth(&row, black_box(target_idx)).unwrap();
                    black_box(val);
                })
            },
        );
        group.bench_function(
            &format!("new_timestamp_{}col_nth_{}", ncols, target_idx),
            |b| {
                b.iter(|| {
                    let val = row.iter().nth(black_box(target_idx)).unwrap();
                    black_box(val);
                })
            },
        );
    }

    // string rows
    for (ncols, target_idx) in [(10, 9), (20, 19)] {
        let row = make_string_row(ncols, 20);
        group.bench_function(
            &format!("old_string_{}col_nth_{}", ncols, target_idx),
            |b| {
                b.iter(|| {
                    let val = old_nth(&row, black_box(target_idx)).unwrap();
                    black_box(val);
                })
            },
        );
        group.bench_function(
            &format!("new_string_{}col_nth_{}", ncols, target_idx),
            |b| {
                b.iter(|| {
                    let val = row.iter().nth(black_box(target_idx)).unwrap();
                    black_box(val);
                })
            },
        );
    }

    // mixed-type rows
    for (ncols, target_idx) in [(10, 9), (20, 19), (20, 10)] {
        let row = make_mixed_row(ncols);
        group.bench_function(
            &format!("old_mixed_{}col_nth_{}", ncols, target_idx),
            |b| {
                b.iter(|| {
                    let val = old_nth(&row, black_box(target_idx)).unwrap();
                    black_box(val);
                })
            },
        );
        group.bench_function(
            &format!("new_mixed_{}col_nth_{}", ncols, target_idx),
            |b| {
                b.iter(|| {
                    let val = row.iter().nth(black_box(target_idx)).unwrap();
                    black_box(val);
                })
            },
        );
    }

    group.finish();
}

fn bench_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("count");

    // Old count: use iterator next() in a loop (simulate old behavior)
    #[inline(never)]
    fn old_count(row: &mz_repr::RowRef) -> usize {
        let mut iter = row.iter();
        let mut count = 0;
        while iter.next().is_some() {
            count += 1;
        }
        count
    }

    for ncols in [5, 10, 20, 50] {
        let int_row = make_int_row(ncols);
        group.bench_function(&format!("old_int64_{}col", ncols), |b| {
            b.iter(|| black_box(old_count(&int_row)))
        });
        group.bench_function(&format!("new_int64_{}col", ncols), |b| {
            b.iter(|| black_box(int_row.iter().count()))
        });
    }

    for ncols in [5, 10, 20] {
        let ts_row = make_timestamp_row(ncols);
        group.bench_function(&format!("old_timestamp_{}col", ncols), |b| {
            b.iter(|| black_box(old_count(&ts_row)))
        });
        group.bench_function(&format!("new_timestamp_{}col", ncols), |b| {
            b.iter(|| black_box(ts_row.iter().count()))
        });
    }

    for ncols in [10, 20] {
        let mixed_row = make_mixed_row(ncols);
        group.bench_function(&format!("old_mixed_{}col", ncols), |b| {
            b.iter(|| black_box(old_count(&mixed_row)))
        });
        group.bench_function(&format!("new_mixed_{}col", ncols), |b| {
            b.iter(|| black_box(mixed_row.iter().count()))
        });
    }

    group.finish();
}

fn bench_batch_nth(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_nth");

    let nrows = 10_000;

    // 20-column int row, extract column 15
    let int_rows: Vec<Row> = (0..nrows).map(|_| make_int_row(20)).collect();
    group.bench_function("old_10k_int64_20col_nth_15", |b| {
        b.iter(|| {
            let mut sum = 0i64;
            for row in &int_rows {
                if let Datum::Int64(v) = old_nth(row, black_box(15)).unwrap() {
                    sum += v;
                }
            }
            black_box(sum);
        })
    });
    group.bench_function("new_10k_int64_20col_nth_15", |b| {
        b.iter(|| {
            let mut sum = 0i64;
            for row in &int_rows {
                if let Datum::Int64(v) = row.iter().nth(black_box(15)).unwrap() {
                    sum += v;
                }
            }
            black_box(sum);
        })
    });

    // 20-column mixed row, extract column 15
    let mixed_rows: Vec<Row> = (0..nrows).map(|_| make_mixed_row(20)).collect();
    group.bench_function("old_10k_mixed_20col_nth_15", |b| {
        b.iter(|| {
            let mut count = 0usize;
            for row in &mixed_rows {
                let val = old_nth(row, black_box(15)).unwrap();
                if !val.is_null() {
                    count += 1;
                }
            }
            black_box(count);
        })
    });
    group.bench_function("new_10k_mixed_20col_nth_15", |b| {
        b.iter(|| {
            let mut count = 0usize;
            for row in &mixed_rows {
                let val = row.iter().nth(black_box(15)).unwrap();
                if !val.is_null() {
                    count += 1;
                }
            }
            black_box(count);
        })
    });

    // 10-column timestamp row, extract column 8
    let ts_rows: Vec<Row> = (0..nrows).map(|_| make_timestamp_row(10)).collect();
    group.bench_function("old_10k_timestamp_10col_nth_8", |b| {
        b.iter(|| {
            let mut count = 0usize;
            for row in &ts_rows {
                let val = old_nth(row, black_box(8)).unwrap();
                if !val.is_null() {
                    count += 1;
                }
            }
            black_box(count);
        })
    });
    group.bench_function("new_10k_timestamp_10col_nth_8", |b| {
        b.iter(|| {
            let mut count = 0usize;
            for row in &ts_rows {
                let val = row.iter().nth(black_box(8)).unwrap();
                if !val.is_null() {
                    count += 1;
                }
            }
            black_box(count);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_nth, bench_count, bench_batch_nth);
criterion_main!(benches);
