// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmark for arrangement key/value formation.
//!
//! Compares the old approach (decode all datums, index for key, index+push for value)
//! with the new approach (selective decode for key, byte-project for value).

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mz_repr::{Datum, DatumVec, Row, RowArena};

/// Build test rows with the given column count, using a mix of Int64 values.
fn build_int_rows(num_rows: usize, num_cols: usize) -> Vec<Row> {
    (0..num_rows)
        .map(|i| {
            let mut row = Row::default();
            let mut packer = row.packer();
            for c in 0..num_cols {
                packer.push(Datum::Int64((i * num_cols + c) as i64));
            }
            row
        })
        .collect()
}

/// Build test rows with mixed types (Int64, String, Float64, Bool, Timestamp).
fn build_mixed_rows(num_rows: usize) -> Vec<Row> {
    use chrono::NaiveDate;
    use mz_repr::adt::timestamp::CheckedTimestamp;
    (0..num_rows)
        .map(|i| {
            let mut row = Row::default();
            let mut packer = row.packer();
            // 10 columns: 3 int, 3 string, 2 float, 1 bool, 1 timestamp
            packer.push(Datum::Int64(i as i64));
            packer.push(Datum::Int64((i * 2) as i64));
            packer.push(Datum::Int64((i * 3) as i64));
            packer.push(Datum::String(&format!("name_{}", i)));
            packer.push(Datum::String(&format!("desc_{}", i)));
            packer.push(Datum::String(&format!("category_{}", i % 100)));
            packer.push(Datum::Float64(((i as f64) * 1.5).into()));
            packer.push(Datum::Float64(((i as f64) * 0.01).into()));
            packer.push(Datum::from(i % 2 == 0));
            let ndt = NaiveDate::from_ymd_opt(2024, 6, 15)
                .unwrap()
                .and_hms_micro_opt(12, 30, 45, (i % 1_000_000) as u32)
                .unwrap();
            packer.push(Datum::Timestamp(
                CheckedTimestamp::from_timestamplike(ndt).unwrap(),
            ));
            row
        })
        .collect()
}

/// Build test rows with Numeric columns.
fn build_numeric_rows(num_rows: usize, num_cols: usize) -> Vec<Row> {
    use mz_repr::adt::numeric::Numeric;
    (0..num_rows)
        .map(|i| {
            let mut row = Row::default();
            let mut packer = row.packer();
            for c in 0..num_cols {
                let val = Numeric::from((i * num_cols + c) as i64);
                packer.push(Datum::from(val));
            }
            row
        })
        .collect()
}

/// Old approach: decode all datums, index for key, index+push for value.
fn old_arrange_kv(
    rows: &[Row],
    key_cols: &[usize],
    thinning: &[usize],
    key_buf: &mut Row,
    val_buf: &mut Row,
    datums: &mut DatumVec,
    temp_storage: &mut RowArena,
) {
    for row in rows {
        temp_storage.clear();
        let datums = datums.borrow_with(row);
        // Key: index into datums for key columns
        {
            let mut packer = key_buf.packer();
            for &c in key_cols {
                packer.push(datums[c]);
            }
        }
        // Value: index into datums for thinning columns
        {
            let val_datum_iter = thinning.iter().map(|c| datums[*c]);
            val_buf.packer().extend(val_datum_iter);
        }
        black_box((&*key_buf, &*val_buf));
    }
}

/// New approach: selective decode for key, byte-project for value.
fn new_arrange_kv(
    rows: &[Row],
    key_cols: &[usize],
    key_needed: &[bool],
    thinning: &[usize],
    key_buf: &mut Row,
    val_buf: &mut Row,
    datums: &mut DatumVec,
    temp_storage: &mut RowArena,
) {
    for row in rows {
        temp_storage.clear();
        // Phase 1: selectively decode only key-needed columns
        {
            let datums = datums.borrow_with_selective(row, key_needed);
            let mut packer = key_buf.packer();
            for &c in key_cols {
                packer.push(datums[c]);
            }
        } // DatumVecBorrow dropped, releasing borrow on row
        // Phase 2: byte-project value columns directly from source row
        row.project_onto(thinning, val_buf);
        black_box((&*key_buf, &*val_buf));
    }
}

fn bench_arrange_kv(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrange_kv");

    // Scenario 1: 10 Int64 columns, key=col0, thinning=cols 1-9
    {
        let rows = build_int_rows(10_000, 10);
        let key_cols = vec![0usize];
        let thinning: Vec<usize> = (1..10).collect();
        let key_needed = vec![
            true, false, false, false, false, false, false, false, false, false,
        ];
        let mut key_buf = Row::default();
        let mut val_buf = Row::default();
        let mut datums = DatumVec::new();
        let mut temp_storage = RowArena::new();

        group.bench_function("int10_key1_old", |b| {
            b.iter(|| {
                old_arrange_kv(
                    &rows,
                    &key_cols,
                    &thinning,
                    &mut key_buf,
                    &mut val_buf,
                    &mut datums,
                    &mut temp_storage,
                )
            })
        });
        group.bench_function("int10_key1_new", |b| {
            b.iter(|| {
                new_arrange_kv(
                    &rows,
                    &key_cols,
                    &key_needed,
                    &thinning,
                    &mut key_buf,
                    &mut val_buf,
                    &mut datums,
                    &mut temp_storage,
                )
            })
        });
    }

    // Scenario 2: 20 Int64 columns, key=cols 0,1, thinning=cols 2-19
    {
        let rows = build_int_rows(10_000, 20);
        let key_cols = vec![0usize, 1];
        let thinning: Vec<usize> = (2..20).collect();
        let mut key_needed = vec![false; 20];
        key_needed[0] = true;
        key_needed[1] = true;
        let mut key_buf = Row::default();
        let mut val_buf = Row::default();
        let mut datums = DatumVec::new();
        let mut temp_storage = RowArena::new();

        group.bench_function("int20_key2_old", |b| {
            b.iter(|| {
                old_arrange_kv(
                    &rows,
                    &key_cols,
                    &thinning,
                    &mut key_buf,
                    &mut val_buf,
                    &mut datums,
                    &mut temp_storage,
                )
            })
        });
        group.bench_function("int20_key2_new", |b| {
            b.iter(|| {
                new_arrange_kv(
                    &rows,
                    &key_cols,
                    &key_needed,
                    &thinning,
                    &mut key_buf,
                    &mut val_buf,
                    &mut datums,
                    &mut temp_storage,
                )
            })
        });
    }

    // Scenario 3: 10 mixed columns, key=col0 (int), thinning=cols 1-9
    {
        let rows = build_mixed_rows(10_000);
        let key_cols = vec![0usize];
        let thinning: Vec<usize> = (1..10).collect();
        let key_needed = vec![
            true, false, false, false, false, false, false, false, false, false,
        ];
        let mut key_buf = Row::default();
        let mut val_buf = Row::default();
        let mut datums = DatumVec::new();
        let mut temp_storage = RowArena::new();

        group.bench_function("mixed10_key1_old", |b| {
            b.iter(|| {
                old_arrange_kv(
                    &rows,
                    &key_cols,
                    &thinning,
                    &mut key_buf,
                    &mut val_buf,
                    &mut datums,
                    &mut temp_storage,
                )
            })
        });
        group.bench_function("mixed10_key1_new", |b| {
            b.iter(|| {
                new_arrange_kv(
                    &rows,
                    &key_cols,
                    &key_needed,
                    &thinning,
                    &mut key_buf,
                    &mut val_buf,
                    &mut datums,
                    &mut temp_storage,
                )
            })
        });
    }

    // Scenario 4: 50 Int64 columns, key=col0, thinning=cols 1-49
    {
        let rows = build_int_rows(10_000, 50);
        let key_cols = vec![0usize];
        let thinning: Vec<usize> = (1..50).collect();
        let mut key_needed = vec![false; 50];
        key_needed[0] = true;
        let mut key_buf = Row::default();
        let mut val_buf = Row::default();
        let mut datums = DatumVec::new();
        let mut temp_storage = RowArena::new();

        group.bench_function("int50_key1_old", |b| {
            b.iter(|| {
                old_arrange_kv(
                    &rows,
                    &key_cols,
                    &thinning,
                    &mut key_buf,
                    &mut val_buf,
                    &mut datums,
                    &mut temp_storage,
                )
            })
        });
        group.bench_function("int50_key1_new", |b| {
            b.iter(|| {
                new_arrange_kv(
                    &rows,
                    &key_cols,
                    &key_needed,
                    &thinning,
                    &mut key_buf,
                    &mut val_buf,
                    &mut datums,
                    &mut temp_storage,
                )
            })
        });
    }

    // Scenario 5: 10 Numeric columns, key=col0, thinning=cols 1-9
    {
        let rows = build_numeric_rows(10_000, 10);
        let key_cols = vec![0usize];
        let thinning: Vec<usize> = (1..10).collect();
        let key_needed = vec![
            true, false, false, false, false, false, false, false, false, false,
        ];
        let mut key_buf = Row::default();
        let mut val_buf = Row::default();
        let mut datums = DatumVec::new();
        let mut temp_storage = RowArena::new();

        group.bench_function("numeric10_key1_old", |b| {
            b.iter(|| {
                old_arrange_kv(
                    &rows,
                    &key_cols,
                    &thinning,
                    &mut key_buf,
                    &mut val_buf,
                    &mut datums,
                    &mut temp_storage,
                )
            })
        });
        group.bench_function("numeric10_key1_new", |b| {
            b.iter(|| {
                new_arrange_kv(
                    &rows,
                    &key_cols,
                    &key_needed,
                    &thinning,
                    &mut key_buf,
                    &mut val_buf,
                    &mut datums,
                    &mut temp_storage,
                )
            })
        });
    }

    // Scenario 6: 5 Int64 columns, key=cols 0,1,2, thinning=cols 3,4
    // (many key columns, few value columns - smaller benefit)
    {
        let rows = build_int_rows(10_000, 5);
        let key_cols = vec![0usize, 1, 2];
        let thinning: Vec<usize> = vec![3, 4];
        let key_needed = vec![true, true, true, false, false];
        let mut key_buf = Row::default();
        let mut val_buf = Row::default();
        let mut datums = DatumVec::new();
        let mut temp_storage = RowArena::new();

        group.bench_function("int5_key3_old", |b| {
            b.iter(|| {
                old_arrange_kv(
                    &rows,
                    &key_cols,
                    &thinning,
                    &mut key_buf,
                    &mut val_buf,
                    &mut datums,
                    &mut temp_storage,
                )
            })
        });
        group.bench_function("int5_key3_new", |b| {
            b.iter(|| {
                new_arrange_kv(
                    &rows,
                    &key_cols,
                    &key_needed,
                    &thinning,
                    &mut key_buf,
                    &mut val_buf,
                    &mut datums,
                    &mut temp_storage,
                )
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_arrange_kv);
criterion_main!(benches);
