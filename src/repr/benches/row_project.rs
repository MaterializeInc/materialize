// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Benchmarks for byte-level row projection vs traditional unpack+repack.
//!
//! The `project_onto` method copies encoded column bytes directly from the
//! source row, avoiding per-datum type matching and re-encoding.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use mz_repr::adt::numeric::Numeric;
use mz_repr::{Datum, Row};

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

criterion_group!(
    benches,
    bench_project_int_identity,
    bench_project_int_subset,
    bench_project_mixed,
    bench_project_numeric,
    bench_project_wide_row,
);
criterion_main!(benches);
