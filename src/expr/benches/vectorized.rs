// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks comparing vectorized (columnar) vs scalar (row-at-a-time)
//! evaluation of MFP expressions on integer columns.

use std::hint::black_box;

use columnar::{Columnar, Index as _};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_expr::func::AddInt64;
use mz_expr::vectorized::{VectorizedSafeMfpPlan, rows_to_columns};
use mz_expr::{MapFilterProject, MirScalarExpr};
use mz_ore::cast::CastFrom;
use mz_repr::{Datum, Diff, Row, RowArena, Timestamp};
use mz_timely_util::columnar::Column;
use timely::container::PushInto;

/// Build `n` rows of (i64, i64) with values (i, i*2).
fn make_rows(n: usize) -> Vec<Row> {
    (0..n)
        .map(|i| {
            let i = i64::cast_from(u32::try_from(i).expect("bench size fits in u32"));
            Row::pack_slice(&[Datum::Int64(i), Datum::Int64(i * 2)])
        })
        .collect()
}

/// Build a `Column<(Row, Timestamp, Diff)>` from rows, all at time 0 with diff +1.
fn make_column(rows: &[Row]) -> Column<(Row, Timestamp, Diff)> {
    let mut col = Column::<(Row, Timestamp, Diff)>::default();
    let ts = Timestamp::from(0u64);
    let diff = Diff::from(1i64);
    for row in rows {
        col.push_into((row, &ts, &diff));
    }
    col
}

/// Build a MapFilterProject that computes `col0 + col1` with input arity 2,
/// projecting to [col0, col1, col0+col1].
fn make_add_mfp() -> MapFilterProject {
    let add_expr = MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), AddInt64);
    MapFilterProject::new(2)
        .map(vec![add_expr])
        .project(vec![0, 1, 2])
}

fn bench_vectorized_vs_scalar(c: &mut Criterion) {
    let mut group = c.benchmark_group("vectorized_mfp");

    for &batch_size in &[100, 1_000, 10_000, 100_000] {
        let rows = make_rows(batch_size);
        let mfp = make_add_mfp();

        // Vectorized path: pre-convert once, then evaluate.
        let vectorized_plan = VectorizedSafeMfpPlan::from_mfp(&mfp);
        let columns = rows_to_columns(rows.iter(), 2);

        group.bench_with_input(
            BenchmarkId::new("vectorized", batch_size),
            &batch_size,
            |b, &n| {
                b.iter(|| {
                    black_box(vectorized_plan.evaluate_batch(&columns, n));
                });
            },
        );

        // Scalar path: row-at-a-time via SafeMfpPlan.
        let plan = mfp.clone().into_plan().unwrap();
        let safe_plan = plan.into_nontemporal().unwrap();

        group.bench_with_input(
            BenchmarkId::new("scalar", batch_size),
            &batch_size,
            |b, &_n| {
                b.iter(|| {
                    let arena = RowArena::new();
                    let mut row_buf = Row::default();
                    for row in &rows {
                        let mut datums = row.unpack();
                        black_box(
                            safe_plan
                                .evaluate_into(&mut datums, &arena, &mut row_buf)
                                .unwrap(),
                        );
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_rows_to_columns(c: &mut Criterion) {
    let mut group = c.benchmark_group("rows_to_columns");

    for &batch_size in &[1_000, 10_000, 100_000] {
        let rows = make_rows(batch_size);

        group.bench_with_input(
            BenchmarkId::new("convert", batch_size),
            &batch_size,
            |b, &_n| {
                b.iter(|| {
                    black_box(rows_to_columns(rows.iter(), 2));
                });
            },
        );
    }

    group.finish();
}

/// Benchmark the end-to-end path: read from a `Column<(Row, Timestamp, Diff)>`,
/// evaluate an MFP (vectorized vs scalar), and encode results back into a Column.
///
/// This mirrors how persist_source processes batches of data.
fn bench_column_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_roundtrip");

    for &batch_size in &[1_000, 10_000, 100_000] {
        let rows = make_rows(batch_size);
        let column = make_column(&rows);
        let mfp = make_add_mfp();
        let vectorized_plan = VectorizedSafeMfpPlan::from_mfp(&mfp);
        let plan = mfp.clone().into_plan().unwrap();
        let safe_plan = plan.into_nontemporal().unwrap();

        // Vectorized: read Column -> transpose to DatumColumns -> eval batch -> encode Column
        group.bench_with_input(
            BenchmarkId::new("vectorized", batch_size),
            &batch_size,
            |b, &_n| {
                b.iter(|| {
                    // Step 1: Extract rows from the columnar container.
                    let borrowed = column.borrow();
                    let mut extracted_rows: Vec<Row> = Vec::with_capacity(batch_size);
                    for i in 0..batch_size {
                        let (row_ref, _ts, _diff) = borrowed.get(i);
                        extracted_rows.push(Row::into_owned(row_ref));
                    }

                    // Step 2: Transpose to columnar form.
                    let datum_columns =
                        rows_to_columns(extracted_rows.iter(), vectorized_plan.input_arity);

                    // Step 3: Evaluate MFP in batch.
                    let results = vectorized_plan.evaluate_batch(&datum_columns, batch_size);

                    // Step 4: Encode results back into a Column.
                    let mut output = Column::<(Row, Timestamp, Diff)>::default();
                    let ts = Timestamp::from(0u64);
                    let diff = Diff::from(1i64);
                    for result in results {
                        if let Ok(Some(row)) = result {
                            output.push_into((&row, &ts, &diff));
                        }
                    }
                    black_box(&output);
                });
            },
        );

        // Scalar: read Column -> eval row-at-a-time -> encode Column
        group.bench_with_input(
            BenchmarkId::new("scalar", batch_size),
            &batch_size,
            |b, &_n| {
                b.iter(|| {
                    let borrowed = column.borrow();
                    let arena = RowArena::new();
                    let mut row_buf = Row::default();
                    let mut output = Column::<(Row, Timestamp, Diff)>::default();
                    let ts = Timestamp::from(0u64);
                    let diff = Diff::from(1i64);
                    for i in 0..batch_size {
                        let (row_ref, _ts, _diff) = borrowed.get(i);
                        let row = Row::into_owned(row_ref);
                        let mut datums = row.unpack();
                        if let Ok(Some(result)) =
                            safe_plan.evaluate_into(&mut datums, &arena, &mut row_buf)
                        {
                            output.push_into((result, &ts, &diff));
                        }
                    }
                    black_box(&output);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_vectorized_vs_scalar,
    bench_rows_to_columns,
    bench_column_roundtrip,
);
criterion_main!(benches);
