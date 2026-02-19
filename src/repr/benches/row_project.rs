// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Benchmarks for byte-level row projection vs traditional unpack+repack.
//!
//! The `project_onto` method copies encoded column bytes directly from the
//! source row, avoiding per-datum type matching and re-encoding.

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use mz_repr::adt::numeric::Numeric;
use mz_repr::{Datum, DatumVec, Row, RowArena};

const NUM_ROWS: u64 = 10_000;

/// Build rows with `num_cols` Int64 columns.
fn build_int_rows(num_rows: usize, num_cols: usize) -> Vec<Row> {
    (0..num_rows)
        .map(|i| {
            let datums: Vec<Datum> = (0..num_cols)
                .map(|c| Datum::Int64((i * num_cols + c) as i64))
                .collect();
            Row::pack(&datums)
        })
        .collect()
}

/// Build rows with mixed types: Int64, String, Float64, Bool.
fn build_mixed_rows(num_rows: usize) -> Vec<Row> {
    (0..num_rows)
        .map(|i| {
            Row::pack(&[
                Datum::Int64(i as i64),
                Datum::String("hello world, this is a medium length string"),
                Datum::Float64(1.23456789f64.into()),
                Datum::True,
                Datum::Int64((i * 7) as i64),
                Datum::String("another column with text data inside"),
                Datum::Float64(9.87654321f64.into()),
                Datum::Int64((i * 13) as i64),
                Datum::False,
                Datum::String("short"),
            ])
        })
        .collect()
}

/// Build rows with Numeric columns (expensive to decode/encode).
fn build_numeric_rows(num_rows: usize) -> Vec<Row> {
    (0..num_rows)
        .map(|i| {
            Row::pack(&[
                Datum::from(Numeric::from(i as i64 * 12345)),
                Datum::from(Numeric::from(99999.99999f64)),
                Datum::from(Numeric::from(i as i64)),
                Datum::Int64(i as i64),
                Datum::from(Numeric::from(123456789.0f64)),
            ])
        })
        .collect()
}

/// Traditional projection: unpack all datums, repack selected ones.
fn project_via_unpack(row: &Row, projection: &[usize], dest: &mut Row) {
    let datums = row.unpack();
    dest.packer().extend(projection.iter().map(|&c| datums[c]));
}

fn bench_project_int_identity(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_project/int_10col_identity");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_int_rows(NUM_ROWS as usize, 10);
    let projection: Vec<usize> = (0..10).collect();
    let mut dest = Row::default();

    group.bench_function("unpack_repack", |b| {
        b.iter(|| {
            for row in &rows {
                project_via_unpack(black_box(row), &projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.bench_function("byte_project", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(row).project_onto(&projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.finish();
}

fn bench_project_int_subset(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_project/int_20col_select5");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_int_rows(NUM_ROWS as usize, 20);
    // Select columns 0, 3, 7, 12, 19 (spread across the row)
    let projection = vec![0, 3, 7, 12, 19];
    let mut dest = Row::default();

    group.bench_function("unpack_repack", |b| {
        b.iter(|| {
            for row in &rows {
                project_via_unpack(black_box(row), &projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.bench_function("byte_project", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(row).project_onto(&projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.finish();
}

fn bench_project_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_project/mixed_10col_select4");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_mixed_rows(NUM_ROWS as usize);
    // Select: Int64(0), String(1), Float64(2), String(5)
    let projection = vec![0, 1, 2, 5];
    let mut dest = Row::default();

    group.bench_function("unpack_repack", |b| {
        b.iter(|| {
            for row in &rows {
                project_via_unpack(black_box(row), &projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.bench_function("byte_project", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(row).project_onto(&projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.finish();
}

fn bench_project_numeric(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_project/numeric_5col_select3");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_numeric_rows(NUM_ROWS as usize);
    // Select: Numeric(0), Int64(3), Numeric(4)
    let projection = vec![0, 3, 4];
    let mut dest = Row::default();

    group.bench_function("unpack_repack", |b| {
        b.iter(|| {
            for row in &rows {
                project_via_unpack(black_box(row), &projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.bench_function("byte_project", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(row).project_onto(&projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.finish();
}

fn bench_project_wide_row(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_project/int_50col_select3");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_int_rows(NUM_ROWS as usize, 50);
    // Select just 3 columns from a wide row
    let projection = vec![5, 25, 45];
    let mut dest = Row::default();

    group.bench_function("unpack_repack", |b| {
        b.iter(|| {
            for row in &rows {
                project_via_unpack(black_box(row), &projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.bench_function("byte_project", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(row).project_onto(&projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.finish();
}

/// Simulates the production MFP evaluation path: selective decode + pack projected
/// datums. This is what `SafeMfpPlan::evaluate_into` does for a pure projection.
fn mfp_selective_project(
    row: &Row,
    needed: &[bool],
    projection: &[usize],
    dest: &mut Row,
    datum_vec: &mut DatumVec,
) {
    let datums_local = datum_vec.borrow_with_selective(row, needed);
    dest.packer()
        .extend(projection.iter().map(|&c| datums_local[c]));
}

/// Build a `needed` bitmask from a projection (marks projected columns as true).
fn needed_from_projection(num_cols: usize, projection: &[usize]) -> Vec<bool> {
    let mut needed = vec![false; num_cols];
    for &col in projection {
        needed[col] = true;
    }
    needed
}

fn bench_mfp_vs_project_int20(c: &mut Criterion) {
    let mut group = c.benchmark_group("mfp_vs_project/int_20col_select5");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_int_rows(NUM_ROWS as usize, 20);
    let projection = vec![0, 3, 7, 12, 19];
    let needed = needed_from_projection(20, &projection);
    let mut dest = Row::default();
    let mut datum_vec = DatumVec::new();

    group.bench_function("mfp_selective_decode", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_selective_project(
                    black_box(row),
                    &needed,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.bench_function("byte_project_onto", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(row).project_onto(&projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.finish();
}

fn bench_mfp_vs_project_mixed10(c: &mut Criterion) {
    let mut group = c.benchmark_group("mfp_vs_project/mixed_10col_select4");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_mixed_rows(NUM_ROWS as usize);
    let projection = vec![0, 1, 2, 5];
    let needed = needed_from_projection(10, &projection);
    let mut dest = Row::default();
    let mut datum_vec = DatumVec::new();

    group.bench_function("mfp_selective_decode", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_selective_project(
                    black_box(row),
                    &needed,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.bench_function("byte_project_onto", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(row).project_onto(&projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.finish();
}

fn bench_mfp_vs_project_numeric5(c: &mut Criterion) {
    let mut group = c.benchmark_group("mfp_vs_project/numeric_5col_select3");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_numeric_rows(NUM_ROWS as usize);
    let projection = vec![0, 3, 4];
    let needed = needed_from_projection(5, &projection);
    let mut dest = Row::default();
    let mut datum_vec = DatumVec::new();

    group.bench_function("mfp_selective_decode", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_selective_project(
                    black_box(row),
                    &needed,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.bench_function("byte_project_onto", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(row).project_onto(&projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.finish();
}

fn bench_mfp_vs_project_int50(c: &mut Criterion) {
    let mut group = c.benchmark_group("mfp_vs_project/int_50col_select3");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_int_rows(NUM_ROWS as usize, 50);
    let projection = vec![5, 25, 45];
    let needed = needed_from_projection(50, &projection);
    let mut dest = Row::default();
    let mut datum_vec = DatumVec::new();

    group.bench_function("mfp_selective_decode", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_selective_project(
                    black_box(row),
                    &needed,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.bench_function("byte_project_onto", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(row).project_onto(&projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.finish();
}

/// Simulates the OLD MFP evaluate_into path: selective decode of all needed columns
/// (predicates + projection), evaluate predicate, then push_datum for projected columns.
fn mfp_eval_old_path(
    row: &Row,
    needed_all: &[bool],
    predicate_col: usize,
    projection: &[usize],
    dest: &mut Row,
    datum_vec: &mut DatumVec,
) -> bool {
    let datums_local = datum_vec.borrow_with_selective(row, needed_all);
    // Simulate predicate: check that predicate_col > 0
    if datums_local[predicate_col] == Datum::Int64(0) {
        return false;
    }
    dest.packer()
        .extend(projection.iter().map(|&c| datums_local[c]));
    true
}

/// Simulates the NEW evaluate_into_project path: selective decode of ONLY predicate
/// columns, evaluate predicate, then byte-level project_onto from source row.
fn mfp_eval_new_path(
    row: &Row,
    needed_pred_only: &[bool],
    predicate_col: usize,
    projection: &[usize],
    dest: &mut Row,
    datum_vec: &mut DatumVec,
) -> bool {
    let datums_local = datum_vec.borrow_with_selective(row, needed_pred_only);
    // Simulate predicate: check that predicate_col > 0
    if datums_local[predicate_col] == Datum::Int64(0) {
        return false;
    }
    drop(datums_local);
    row.project_onto(projection, dest);
    true
}

/// Build needed bitmask for predicate + projection columns (old path).
fn needed_all(num_cols: usize, predicate_cols: &[usize], projection: &[usize]) -> Vec<bool> {
    let mut needed = vec![false; num_cols];
    for &col in predicate_cols {
        needed[col] = true;
    }
    for &col in projection {
        needed[col] = true;
    }
    needed
}

/// Build needed bitmask for predicate columns only (new path).
fn needed_pred_only(num_cols: usize, predicate_cols: &[usize]) -> Vec<bool> {
    let mut needed = vec![false; num_cols];
    for &col in predicate_cols {
        needed[col] = true;
    }
    needed
}

fn bench_eval_project_int20(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_then_project/int_20col_pred1_proj5");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_int_rows(NUM_ROWS as usize, 20);
    let predicate_col = 10;
    let predicate_cols = vec![10];
    let projection = vec![0, 3, 7, 12, 19];
    let needed_old = needed_all(20, &predicate_cols, &projection);
    let needed_new = needed_pred_only(20, &predicate_cols);
    let mut dest = Row::default();
    let mut datum_vec = DatumVec::new();

    group.bench_function("old_selective_decode_all", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_eval_old_path(
                    black_box(row),
                    &needed_old,
                    predicate_col,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.bench_function("new_eval_then_byte_project", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_eval_new_path(
                    black_box(row),
                    &needed_new,
                    predicate_col,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.finish();
}

fn bench_eval_project_mixed10(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_then_project/mixed_10col_pred1_proj4");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_mixed_rows(NUM_ROWS as usize);
    // Predicate on Int64 col 4, project String+Float+Int cols
    let predicate_col = 4;
    let predicate_cols = vec![4];
    let projection = vec![0, 1, 2, 5];
    let needed_old = needed_all(10, &predicate_cols, &projection);
    let needed_new = needed_pred_only(10, &predicate_cols);
    let mut dest = Row::default();
    let mut datum_vec = DatumVec::new();

    group.bench_function("old_selective_decode_all", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_eval_old_path(
                    black_box(row),
                    &needed_old,
                    predicate_col,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.bench_function("new_eval_then_byte_project", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_eval_new_path(
                    black_box(row),
                    &needed_new,
                    predicate_col,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.finish();
}

fn bench_eval_project_int50(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_then_project/int_50col_pred1_proj3");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_int_rows(NUM_ROWS as usize, 50);
    // Predicate on col 30, project cols spread across the row
    let predicate_col = 30;
    let predicate_cols = vec![30];
    let projection = vec![5, 25, 45];
    let needed_old = needed_all(50, &predicate_cols, &projection);
    let needed_new = needed_pred_only(50, &predicate_cols);
    let mut dest = Row::default();
    let mut datum_vec = DatumVec::new();

    group.bench_function("old_selective_decode_all", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_eval_old_path(
                    black_box(row),
                    &needed_old,
                    predicate_col,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.bench_function("new_eval_then_byte_project", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_eval_new_path(
                    black_box(row),
                    &needed_new,
                    predicate_col,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.finish();
}

fn bench_eval_project_numeric5(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_then_project/numeric_5col_pred1_proj3");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_numeric_rows(NUM_ROWS as usize);
    // Predicate on Int64 col 3, project Numeric cols 0, 2, 4
    let predicate_col = 3;
    let predicate_cols = vec![3];
    let projection = vec![0, 2, 4];
    let needed_old = needed_all(5, &predicate_cols, &projection);
    let needed_new = needed_pred_only(5, &predicate_cols);
    let mut dest = Row::default();
    let mut datum_vec = DatumVec::new();

    group.bench_function("old_selective_decode_all", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_eval_old_path(
                    black_box(row),
                    &needed_old,
                    predicate_col,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.bench_function("new_eval_then_byte_project", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_eval_new_path(
                    black_box(row),
                    &needed_new,
                    predicate_col,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.finish();
}

// === Unordered (unsorted) projection benchmarks ===

fn bench_project_unordered_int20_reversed(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_project_unordered/int_20col_reverse5");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_int_rows(NUM_ROWS as usize, 20);
    // Reversed order: [19, 12, 7, 3, 0]
    let projection = vec![19, 12, 7, 3, 0];
    let mut dest = Row::default();

    group.bench_function("unpack_repack", |b| {
        b.iter(|| {
            for row in &rows {
                project_via_unpack(black_box(row), &projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.bench_function("byte_project_unordered", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(row).project_onto_unordered(&projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.finish();
}

fn bench_project_unordered_int20_shuffled(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_project_unordered/int_20col_shuffle5");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_int_rows(NUM_ROWS as usize, 20);
    // Shuffled order: [12, 3, 19, 0, 7]
    let projection = vec![12, 3, 19, 0, 7];
    let mut dest = Row::default();

    group.bench_function("unpack_repack", |b| {
        b.iter(|| {
            for row in &rows {
                project_via_unpack(black_box(row), &projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.bench_function("byte_project_unordered", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(row).project_onto_unordered(&projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.finish();
}

fn bench_project_unordered_mixed10(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_project_unordered/mixed_10col_shuffle4");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_mixed_rows(NUM_ROWS as usize);
    // Unordered: [5, 0, 9, 2] - String, Int64, String, Float64
    let projection = vec![5, 0, 9, 2];
    let mut dest = Row::default();

    group.bench_function("unpack_repack", |b| {
        b.iter(|| {
            for row in &rows {
                project_via_unpack(black_box(row), &projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.bench_function("byte_project_unordered", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(row).project_onto_unordered(&projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.finish();
}

fn bench_project_unordered_int50_wide(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_project_unordered/int_50col_shuffle3");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_int_rows(NUM_ROWS as usize, 50);
    // Unordered from wide row: [45, 5, 25]
    let projection = vec![45, 5, 25];
    let mut dest = Row::default();

    group.bench_function("unpack_repack", |b| {
        b.iter(|| {
            for row in &rows {
                project_via_unpack(black_box(row), &projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.bench_function("byte_project_unordered", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(row).project_onto_unordered(&projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.finish();
}

/// Compare unordered byte project against MFP selective decode for unsorted projections
fn bench_mfp_vs_unordered_int20(c: &mut Criterion) {
    let mut group = c.benchmark_group("mfp_vs_unordered/int_20col_shuffle5");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_int_rows(NUM_ROWS as usize, 20);
    let projection = vec![12, 3, 19, 0, 7];
    let needed = needed_from_projection(20, &projection);
    let mut dest = Row::default();
    let mut datum_vec = DatumVec::new();

    group.bench_function("mfp_selective_decode", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_selective_project(
                    black_box(row),
                    &needed,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.bench_function("byte_project_unordered", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(row).project_onto_unordered(&projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.finish();
}

fn bench_mfp_vs_unordered_mixed10(c: &mut Criterion) {
    let mut group = c.benchmark_group("mfp_vs_unordered/mixed_10col_shuffle4");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_mixed_rows(NUM_ROWS as usize);
    let projection = vec![5, 0, 9, 2];
    let needed = needed_from_projection(10, &projection);
    let mut dest = Row::default();
    let mut datum_vec = DatumVec::new();

    group.bench_function("mfp_selective_decode", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_selective_project(
                    black_box(row),
                    &needed,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.bench_function("byte_project_unordered", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(row).project_onto_unordered(&projection, &mut dest);
                black_box(&dest);
            }
        })
    });

    group.finish();
}

/// Eval + unordered project: predicate check then byte-level unsorted projection
fn mfp_eval_new_path_unordered(
    row: &Row,
    needed_pred_only: &[bool],
    predicate_col: usize,
    projection: &[usize],
    dest: &mut Row,
    datum_vec: &mut DatumVec,
) -> bool {
    let datums_local = datum_vec.borrow_with_selective(row, needed_pred_only);
    if datums_local[predicate_col] == Datum::Int64(0) {
        return false;
    }
    drop(datums_local);
    row.project_onto_unordered(projection, dest);
    true
}

fn bench_eval_unordered_int20(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_then_unordered/int_20col_pred1_shuffle5");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_int_rows(NUM_ROWS as usize, 20);
    let predicate_col = 10;
    let predicate_cols = vec![10];
    let projection = vec![12, 3, 19, 0, 7];
    let needed_old = needed_all(20, &predicate_cols, &projection);
    let needed_new = needed_pred_only(20, &predicate_cols);
    let mut dest = Row::default();
    let mut datum_vec = DatumVec::new();

    group.bench_function("old_selective_decode_all", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_eval_old_path(
                    black_box(row),
                    &needed_old,
                    predicate_col,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.bench_function("new_eval_then_byte_unordered", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_eval_new_path_unordered(
                    black_box(row),
                    &needed_new,
                    predicate_col,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.finish();
}

fn bench_eval_unordered_mixed10(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_then_unordered/mixed_10col_pred1_shuffle4");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let rows = build_mixed_rows(NUM_ROWS as usize);
    let predicate_col = 4;
    let predicate_cols = vec![4];
    let projection = vec![5, 0, 9, 2];
    let needed_old = needed_all(10, &predicate_cols, &projection);
    let needed_new = needed_pred_only(10, &predicate_cols);
    let mut dest = Row::default();
    let mut datum_vec = DatumVec::new();

    group.bench_function("old_selective_decode_all", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_eval_old_path(
                    black_box(row),
                    &needed_old,
                    predicate_col,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.bench_function("new_eval_then_byte_unordered", |b| {
        b.iter(|| {
            for row in &rows {
                mfp_eval_new_path_unordered(
                    black_box(row),
                    &needed_new,
                    predicate_col,
                    &projection,
                    &mut dest,
                    &mut datum_vec,
                );
                black_box(&dest);
            }
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_project_int_identity,
    bench_project_int_subset,
    bench_project_mixed,
    bench_project_numeric,
    bench_project_wide_row,
    bench_mfp_vs_project_int20,
    bench_mfp_vs_project_mixed10,
    bench_mfp_vs_project_numeric5,
    bench_mfp_vs_project_int50,
    bench_eval_project_int20,
    bench_eval_project_mixed10,
    bench_eval_project_int50,
    bench_eval_project_numeric5,
    // Unordered projection benchmarks
    bench_project_unordered_int20_reversed,
    bench_project_unordered_int20_shuffled,
    bench_project_unordered_mixed10,
    bench_project_unordered_int50_wide,
    bench_mfp_vs_unordered_int20,
    bench_mfp_vs_unordered_mixed10,
    bench_eval_unordered_int20,
    bench_eval_unordered_mixed10,
);
criterion_main!(benches);
