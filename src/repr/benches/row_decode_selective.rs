// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for selective datum decoding vs full row decoding.
//!
//! Compares `DatumVec::borrow_with()` (full decode) against
//! `DatumVec::borrow_with_selective()` (skip unneeded columns).

use criterion::{Criterion, criterion_group, criterion_main};
use mz_repr::adt::numeric::{Dec, Numeric};
use mz_repr::{Datum, DatumVec, Row};

fn make_int_row(ncols: usize) -> Row {
    let datums: Vec<Datum> = (0..ncols).map(|i| Datum::Int64(i as i64 * 1000)).collect();
    Row::pack_slice(&datums)
}

fn make_timestamp_row(ncols: usize) -> Row {
    use chrono::NaiveDate;
    use mz_repr::adt::timestamp::CheckedTimestamp;
    let base = NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_micro_opt(14, 30, 0, 123456)
        .unwrap();
    let datums: Vec<Datum> = (0..ncols)
        .map(|_| {
            Datum::Timestamp(
                CheckedTimestamp::from_timestamplike(base).unwrap(),
            )
        })
        .collect();
    Row::pack_slice(&datums)
}

fn make_mixed_row(ncols: usize) -> Row {
    use chrono::NaiveDate;
    use mz_repr::adt::timestamp::CheckedTimestamp;
    let ts = NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_micro_opt(14, 30, 0, 123456)
        .unwrap();
    let datums: Vec<Datum> = (0..ncols)
        .map(|i| match i % 5 {
            0 => Datum::Int64(i as i64 * 1000),
            1 => Datum::String("hello world"),
            2 => Datum::Float64(3.14159.into()),
            3 => Datum::Timestamp(
                CheckedTimestamp::from_timestamplike(ts).unwrap(),
            ),
            _ => Datum::True,
        })
        .collect();
    Row::pack_slice(&datums)
}

fn make_numeric_row(ncols: usize) -> Row {
    let datums: Vec<Datum> = (0..ncols)
        .map(|_| {
            let mut cx = Numeric::context();
            let n = cx.parse("123456789.987654321").unwrap();
            Datum::from(n)
        })
        .collect();
    Row::pack_slice(&datums)
}

fn needed_mask(ncols: usize, needed_indices: &[usize]) -> Vec<bool> {
    let mut mask = vec![false; ncols];
    for &i in needed_indices {
        if i < ncols {
            mask[i] = true;
        }
    }
    mask
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_selective");

    // --- 10-column int64 rows, need 2 columns ---
    {
        let row = make_int_row(10);
        let needed = needed_mask(10, &[0, 9]);
        group.bench_function("int10_need2_full", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with(&row);
                std::hint::black_box(&*borrow);
            })
        });
        group.bench_function("int10_need2_selective", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with_selective(&row, &needed);
                std::hint::black_box(&*borrow);
            })
        });
    }

    // --- 20-column int64 rows, need 3 columns ---
    {
        let row = make_int_row(20);
        let needed = needed_mask(20, &[0, 10, 19]);
        group.bench_function("int20_need3_full", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with(&row);
                std::hint::black_box(&*borrow);
            })
        });
        group.bench_function("int20_need3_selective", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with_selective(&row, &needed);
                std::hint::black_box(&*borrow);
            })
        });
    }

    // --- 10-column timestamp rows, need 2 columns ---
    {
        let row = make_timestamp_row(10);
        let needed = needed_mask(10, &[0, 9]);
        group.bench_function("ts10_need2_full", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with(&row);
                std::hint::black_box(&*borrow);
            })
        });
        group.bench_function("ts10_need2_selective", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with_selective(&row, &needed);
                std::hint::black_box(&*borrow);
            })
        });
    }

    // --- 20-column timestamp rows, need 3 columns ---
    {
        let row = make_timestamp_row(20);
        let needed = needed_mask(20, &[0, 10, 19]);
        group.bench_function("ts20_need3_full", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with(&row);
                std::hint::black_box(&*borrow);
            })
        });
        group.bench_function("ts20_need3_selective", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with_selective(&row, &needed);
                std::hint::black_box(&*borrow);
            })
        });
    }

    // --- 20-column mixed rows, need 4 columns ---
    {
        let row = make_mixed_row(20);
        let needed = needed_mask(20, &[0, 5, 10, 15]);
        group.bench_function("mixed20_need4_full", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with(&row);
                std::hint::black_box(&*borrow);
            })
        });
        group.bench_function("mixed20_need4_selective", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with_selective(&row, &needed);
                std::hint::black_box(&*borrow);
            })
        });
    }

    // --- 10-column numeric rows, need 2 columns ---
    {
        let row = make_numeric_row(10);
        let needed = needed_mask(10, &[0, 9]);
        group.bench_function("numeric10_need2_full", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with(&row);
                std::hint::black_box(&*borrow);
            })
        });
        group.bench_function("numeric10_need2_selective", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with_selective(&row, &needed);
                std::hint::black_box(&*borrow);
            })
        });
    }

    // --- 50-column int64 rows, need 3 columns ---
    {
        let row = make_int_row(50);
        let needed = needed_mask(50, &[0, 25, 49]);
        group.bench_function("int50_need3_full", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with(&row);
                std::hint::black_box(&*borrow);
            })
        });
        group.bench_function("int50_need3_selective", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with_selective(&row, &needed);
                std::hint::black_box(&*borrow);
            })
        });
    }

    // --- Need ALL columns (should be roughly same speed) ---
    {
        let row = make_int_row(10);
        let needed = vec![true; 10];
        group.bench_function("int10_needALL_full", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with(&row);
                std::hint::black_box(&*borrow);
            })
        });
        group.bench_function("int10_needALL_selective", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                let borrow = dv.borrow_with_selective(&row, &needed);
                std::hint::black_box(&*borrow);
            })
        });
    }

    group.finish();

    // --- Batch benchmarks (10k rows) ---
    let mut batch_group = c.benchmark_group("decode_selective_batch");

    {
        let rows: Vec<Row> = (0..10_000).map(|_| make_timestamp_row(20)).collect();
        let needed = needed_mask(20, &[0, 10, 19]);
        batch_group.bench_function("10k_ts20_need3_full", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                for row in &rows {
                    let borrow = dv.borrow_with(row);
                    std::hint::black_box(&*borrow);
                }
            })
        });
        batch_group.bench_function("10k_ts20_need3_selective", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                for row in &rows {
                    let borrow = dv.borrow_with_selective(row, &needed);
                    std::hint::black_box(&*borrow);
                }
            })
        });
    }

    {
        let rows: Vec<Row> = (0..10_000).map(|_| make_mixed_row(20)).collect();
        let needed = needed_mask(20, &[0, 5, 10, 15]);
        batch_group.bench_function("10k_mixed20_need4_full", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                for row in &rows {
                    let borrow = dv.borrow_with(row);
                    std::hint::black_box(&*borrow);
                }
            })
        });
        batch_group.bench_function("10k_mixed20_need4_selective", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                for row in &rows {
                    let borrow = dv.borrow_with_selective(row, &needed);
                    std::hint::black_box(&*borrow);
                }
            })
        });
    }

    {
        let rows: Vec<Row> = (0..10_000).map(|_| make_int_row(50)).collect();
        let needed = needed_mask(50, &[0, 25, 49]);
        batch_group.bench_function("10k_int50_need3_full", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                for row in &rows {
                    let borrow = dv.borrow_with(row);
                    std::hint::black_box(&*borrow);
                }
            })
        });
        batch_group.bench_function("10k_int50_need3_selective", |b| {
            let mut dv = DatumVec::new();
            b.iter(|| {
                for row in &rows {
                    let borrow = dv.borrow_with_selective(row, &needed);
                    std::hint::black_box(&*borrow);
                }
            })
        });
    }

    batch_group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
