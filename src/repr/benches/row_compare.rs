// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Benchmarks for row comparison patterns:
//! - Old: Row::unpack() allocates Vec<Datum> per comparison (Top1Monoid pattern)
//! - New (DatumVec): DatumVec::borrow_with() reuses allocation across comparisons
//! - New (selective): nth() to extract only ORDER BY columns, skip_datum for others

use std::cell::RefCell;
use std::cmp::Ordering;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mz_repr::{Datum, DatumVec, Row};

/// Old Top1Monoid::cmp() pattern: allocates 2 Vec<Datum> per comparison.
fn compare_rows_unpack(left: &Row, right: &Row, order_columns: &[usize]) -> Ordering {
    let left_datums: Vec<_> = left.unpack();
    let right_datums: Vec<_> = right.unpack();
    for &col in order_columns {
        let cmp = left_datums[col].cmp(&right_datums[col]);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    left_datums.cmp(&right_datums)
}

/// New pattern: selective comparison using nth() (skip_datum for skipped columns).
/// Only reads the columns needed for ORDER BY. Falls back to DatumVec for tiebreaker.
fn compare_rows_selective(left: &Row, right: &Row, order_columns: &[usize]) -> Ordering {
    // Fast path: compare only the ORDER BY columns using nth()
    for &col in order_columns {
        let left_datum = left.iter().nth(col).unwrap();
        let right_datum = right.iter().nth(col).unwrap();
        let cmp = left_datum.cmp(&right_datum);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    // Tiebreaker: full comparison with thread-local DatumVec
    thread_local! {
        static CMP_LEFT: RefCell<DatumVec> = const { RefCell::new(DatumVec::new()) };
        static CMP_RIGHT: RefCell<DatumVec> = const { RefCell::new(DatumVec::new()) };
    }
    CMP_LEFT.with(|left_cell| {
        CMP_RIGHT.with(|right_cell| {
            let mut left_vec = left_cell.borrow_mut();
            let mut right_vec = right_cell.borrow_mut();
            let left_datums = left_vec.borrow_with(left);
            let right_datums = right_vec.borrow_with(right);
            left_datums.cmp(&right_datums)
        })
    })
}

fn make_int_row(vals: &[i64]) -> Row {
    Row::pack(vals.iter().map(|v| Datum::Int64(*v)))
}

fn make_mixed_row(id: i64, name: &str, score: f64) -> Row {
    Row::pack([
        Datum::Int64(id),
        Datum::String(name),
        Datum::Float64(score.into()),
        Datum::Int64(id * 100),
        Datum::String("filler text for padding"),
    ])
}

fn make_timestamp_row(vals: &[i64]) -> Row {
    use chrono::NaiveDateTime;
    use mz_repr::adt::timestamp::CheckedTimestamp;
    Row::pack(vals.iter().map(|v| {
        let ts = NaiveDateTime::from_timestamp_opt(*v, 0).unwrap();
        Datum::Timestamp(CheckedTimestamp::from_timestamplike(ts).unwrap())
    }))
}

fn bench_per_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("per_comparison");

    // 5-column int rows, compare by column 0
    let left = make_int_row(&[1, 2, 3, 4, 5]);
    let right = make_int_row(&[2, 2, 3, 4, 5]);
    let order = vec![0];
    group.bench_function("int5_col0_unpack", |b| {
        b.iter(|| compare_rows_unpack(black_box(&left), black_box(&right), &order))
    });
    group.bench_function("int5_col0_selective", |b| {
        b.iter(|| compare_rows_selective(black_box(&left), black_box(&right), &order))
    });

    // 10-column int rows, compare by column 0 (early column in wide row)
    let left = make_int_row(&[100, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let right = make_int_row(&[200, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let order = vec![0];
    group.bench_function("int10_col0_unpack", |b| {
        b.iter(|| compare_rows_unpack(black_box(&left), black_box(&right), &order))
    });
    group.bench_function("int10_col0_selective", |b| {
        b.iter(|| compare_rows_selective(black_box(&left), black_box(&right), &order))
    });

    // 10-column int rows, compare by column 8
    let left = make_int_row(&[1, 2, 3, 4, 5, 6, 7, 8, 100, 10]);
    let right = make_int_row(&[1, 2, 3, 4, 5, 6, 7, 8, 200, 10]);
    let order = vec![8];
    group.bench_function("int10_col8_unpack", |b| {
        b.iter(|| compare_rows_unpack(black_box(&left), black_box(&right), &order))
    });
    group.bench_function("int10_col8_selective", |b| {
        b.iter(|| compare_rows_selective(black_box(&left), black_box(&right), &order))
    });

    // 20-column int rows, compare by column 0 (BIG win expected)
    let vals20: Vec<i64> = (0..20).collect();
    let mut vals20_diff = vals20.clone();
    vals20_diff[0] = 999;
    let left = make_int_row(&vals20);
    let right = make_int_row(&vals20_diff);
    let order = vec![0];
    group.bench_function("int20_col0_unpack", |b| {
        b.iter(|| compare_rows_unpack(black_box(&left), black_box(&right), &order))
    });
    group.bench_function("int20_col0_selective", |b| {
        b.iter(|| compare_rows_selective(black_box(&left), black_box(&right), &order))
    });

    // 20-column int rows, compare by column 15
    let mut vals20_diff2 = vals20.clone();
    vals20_diff2[15] = 999;
    let left = make_int_row(&vals20);
    let right = make_int_row(&vals20_diff2);
    let order = vec![15];
    group.bench_function("int20_col15_unpack", |b| {
        b.iter(|| compare_rows_unpack(black_box(&left), black_box(&right), &order))
    });
    group.bench_function("int20_col15_selective", |b| {
        b.iter(|| compare_rows_selective(black_box(&left), black_box(&right), &order))
    });

    // Mixed-type rows (int, string, float), compare by column 2 (float score)
    let left = make_mixed_row(1, "alice", 95.5);
    let right = make_mixed_row(2, "bob", 88.0);
    let order = vec![2];
    group.bench_function("mixed5_col2_unpack", |b| {
        b.iter(|| compare_rows_unpack(black_box(&left), black_box(&right), &order))
    });
    group.bench_function("mixed5_col2_selective", |b| {
        b.iter(|| compare_rows_selective(black_box(&left), black_box(&right), &order))
    });

    // Timestamp rows, compare by column 3
    let left = make_timestamp_row(&[1000000, 2000000, 3000000, 4000000, 5000000]);
    let right = make_timestamp_row(&[1000000, 2000000, 3000000, 5000000, 5000000]);
    let order = vec![3];
    group.bench_function("ts5_col3_unpack", |b| {
        b.iter(|| compare_rows_unpack(black_box(&left), black_box(&right), &order))
    });
    group.bench_function("ts5_col3_selective", |b| {
        b.iter(|| compare_rows_selective(black_box(&left), black_box(&right), &order))
    });

    // 10-column timestamp rows, compare by column 0 (big win: skip expensive ts reads)
    let left = make_timestamp_row(&[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]);
    let right = make_timestamp_row(&[200, 200, 300, 400, 500, 600, 700, 800, 900, 1000]);
    let order = vec![0];
    group.bench_function("ts10_col0_unpack", |b| {
        b.iter(|| compare_rows_unpack(black_box(&left), black_box(&right), &order))
    });
    group.bench_function("ts10_col0_selective", |b| {
        b.iter(|| compare_rows_selective(black_box(&left), black_box(&right), &order))
    });

    // Equal rows (triggers tiebreaker path - selective is slower here)
    let left = make_int_row(&[1, 2, 3, 4, 5]);
    let right = make_int_row(&[1, 2, 3, 4, 5]);
    let order = vec![0, 2];
    group.bench_function("int5_equal_tiebreak_unpack", |b| {
        b.iter(|| compare_rows_unpack(black_box(&left), black_box(&right), &order))
    });
    group.bench_function("int5_equal_tiebreak_selective", |b| {
        b.iter(|| compare_rows_selective(black_box(&left), black_box(&right), &order))
    });

    group.finish();
}

fn bench_batch_semigroup(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_semigroup");

    let n = 10_000;

    // Integer rows: 10 columns, order by column 0 (common LIMIT 1 pattern)
    let rows: Vec<Row> = (0..n)
        .map(|i| {
            make_int_row(&[
                i % 100,
                i * 7,
                i * 13,
                i * 31,
                i * 97,
                i,
                i + 1,
                i + 2,
                i + 3,
                i + 4,
            ])
        })
        .collect();
    let order = vec![0];

    group.bench_function("int10_10k_unpack", |b| {
        b.iter(|| {
            let mut best = &rows[0];
            for row in &rows[1..] {
                if compare_rows_unpack(best, row, &order) == Ordering::Greater {
                    best = row;
                }
            }
            black_box(best);
        })
    });

    group.bench_function("int10_10k_selective", |b| {
        b.iter(|| {
            let mut best = &rows[0];
            for row in &rows[1..] {
                if compare_rows_selective(best, row, &order) == Ordering::Greater {
                    best = row;
                }
            }
            black_box(best);
        })
    });

    // 20-column int rows, order by column 0
    let wide_rows: Vec<Row> = (0..n)
        .map(|i| {
            let mut vals: Vec<i64> = (0..20).map(|j| i * 7 + j).collect();
            vals[0] = i % 100;
            make_int_row(&vals)
        })
        .collect();
    let order = vec![0];

    group.bench_function("int20_10k_unpack", |b| {
        b.iter(|| {
            let mut best = &wide_rows[0];
            for row in &wide_rows[1..] {
                if compare_rows_unpack(best, row, &order) == Ordering::Greater {
                    best = row;
                }
            }
            black_box(best);
        })
    });

    group.bench_function("int20_10k_selective", |b| {
        b.iter(|| {
            let mut best = &wide_rows[0];
            for row in &wide_rows[1..] {
                if compare_rows_selective(best, row, &order) == Ordering::Greater {
                    best = row;
                }
            }
            black_box(best);
        })
    });

    // 10-column timestamp rows, order by column 0 (timestamps expensive to read)
    let ts_rows: Vec<Row> = (0..n)
        .map(|i| {
            let vals: Vec<i64> = (0..10).map(|j| (i * 86400 + j * 3600) as i64).collect();
            make_timestamp_row(&vals)
        })
        .collect();
    let order = vec![0];

    group.bench_function("ts10_10k_unpack", |b| {
        b.iter(|| {
            let mut best = &ts_rows[0];
            for row in &ts_rows[1..] {
                if compare_rows_unpack(best, row, &order) == Ordering::Greater {
                    best = row;
                }
            }
            black_box(best);
        })
    });

    group.bench_function("ts10_10k_selective", |b| {
        b.iter(|| {
            let mut best = &ts_rows[0];
            for row in &ts_rows[1..] {
                if compare_rows_selective(best, row, &order) == Ordering::Greater {
                    best = row;
                }
            }
            black_box(best);
        })
    });

    // Mixed rows: 5 columns, order by column 2 (float)
    let names = ["alice", "bob", "charlie", "diana", "eve"];
    let mixed_rows: Vec<Row> = (0..n)
        .map(|i| {
            make_mixed_row(
                i,
                names[i as usize % names.len()],
                (i as f64) * 0.7,
            )
        })
        .collect();
    let order = vec![2];

    group.bench_function("mixed5_10k_unpack", |b| {
        b.iter(|| {
            let mut best = &mixed_rows[0];
            for row in &mixed_rows[1..] {
                if compare_rows_unpack(best, row, &order) == Ordering::Greater {
                    best = row;
                }
            }
            black_box(best);
        })
    });

    group.bench_function("mixed5_10k_selective", |b| {
        b.iter(|| {
            let mut best = &mixed_rows[0];
            for row in &mixed_rows[1..] {
                if compare_rows_selective(best, row, &order) == Ordering::Greater {
                    best = row;
                }
            }
            black_box(best);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_per_comparison, bench_batch_semigroup);
criterion_main!(benches);
