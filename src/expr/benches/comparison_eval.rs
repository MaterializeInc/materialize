// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for MirScalarExpr comparison evaluation.
//!
//! Tests the fast-path comparison evaluator that bypasses the EagerBinaryFunc
//! ceremony for Eq/NotEq/Lt/Lte/Gt/Gte operators.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_expr::MirScalarExpr;
use mz_expr::func::{AddInt32, Eq, Gt, Lt, Lte, NotEq};
use mz_ore::treat_as_equal::TreatAsEqual;
use mz_repr::{Datum, Row, RowArena, SqlColumnType, SqlScalarType};

/// Build a comparison predicate: Column(col_idx) op Literal(literal_row)
fn make_comparison(
    func: mz_expr::BinaryFunc,
    col_idx: usize,
    literal: Row,
    col_type: SqlColumnType,
) -> MirScalarExpr {
    MirScalarExpr::CallBinary {
        func,
        expr1: Box::new(MirScalarExpr::Column(col_idx, TreatAsEqual(None))),
        expr2: Box::new(MirScalarExpr::Literal(Ok(literal), col_type)),
    }
}

fn bench_comparison_eval(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison_eval");

    // Common setup: a row with [Int32(42), Int64(1000000), String("hello"), Float64(3.14)]
    let arena = RowArena::new();
    let int32_type = SqlColumnType {
        scalar_type: SqlScalarType::Int32,
        nullable: false,
    };
    let int64_type = SqlColumnType {
        scalar_type: SqlScalarType::Int64,
        nullable: false,
    };
    let float64_type = SqlColumnType {
        scalar_type: SqlScalarType::Float64,
        nullable: false,
    };
    let string_type = SqlColumnType {
        scalar_type: SqlScalarType::String,
        nullable: false,
    };

    // Datums for each row (pre-unpacked, as they would be in evaluate_inner)
    let datums_mixed: Vec<Datum> = vec![
        Datum::Int32(42),
        Datum::Int64(1_000_000),
        Datum::String("hello"),
        Datum::Float64(3.14.into()),
    ];

    // --- Benchmark: Int32 column > literal ---
    let gt_int32 = make_comparison(
        mz_expr::BinaryFunc::Gt(Gt),
        0,
        Row::pack_slice(&[Datum::Int32(5)]),
        int32_type.clone(),
    );

    group.bench_function("int32_gt_literal", |b| {
        b.iter(|| black_box(gt_int32.eval(black_box(&datums_mixed), &arena)))
    });

    // --- Benchmark: Int64 column = literal ---
    let eq_int64 = make_comparison(
        mz_expr::BinaryFunc::Eq(Eq),
        1,
        Row::pack_slice(&[Datum::Int64(1_000_000)]),
        int64_type.clone(),
    );

    group.bench_function("int64_eq_literal", |b| {
        b.iter(|| black_box(eq_int64.eval(black_box(&datums_mixed), &arena)))
    });

    // --- Benchmark: Float64 column < literal ---
    let lt_float64 = make_comparison(
        mz_expr::BinaryFunc::Lt(Lt),
        3,
        Row::pack_slice(&[Datum::Float64(100.0.into())]),
        float64_type.clone(),
    );

    group.bench_function("float64_lt_literal", |b| {
        b.iter(|| black_box(lt_float64.eval(black_box(&datums_mixed), &arena)))
    });

    // --- Benchmark: String column != literal ---
    let neq_string = make_comparison(
        mz_expr::BinaryFunc::NotEq(NotEq),
        2,
        Row::pack_slice(&[Datum::String("world")]),
        string_type.clone(),
    );

    group.bench_function("string_neq_literal", |b| {
        b.iter(|| black_box(neq_string.eval(black_box(&datums_mixed), &arena)))
    });

    // --- Benchmark: Column op Column ---
    let gt_col_col = MirScalarExpr::CallBinary {
        func: mz_expr::BinaryFunc::Gt(Gt),
        expr1: Box::new(MirScalarExpr::Column(0, TreatAsEqual(None))),
        expr2: Box::new(MirScalarExpr::Column(0, TreatAsEqual(None))),
    };

    group.bench_function("int32_gt_column", |b| {
        b.iter(|| black_box(gt_col_col.eval(black_box(&datums_mixed), &arena)))
    });

    // --- Benchmark: Null propagation ---
    let datums_with_null: Vec<Datum> = vec![
        Datum::Null,
        Datum::Int64(1_000_000),
        Datum::String("hello"),
        Datum::Float64(3.14.into()),
    ];

    group.bench_function("null_propagation", |b| {
        b.iter(|| black_box(gt_int32.eval(black_box(&datums_with_null), &arena)))
    });

    // --- Benchmark: Non-comparison binary op (AddInt32) for overhead measurement ---
    let add_int32 = MirScalarExpr::CallBinary {
        func: mz_expr::BinaryFunc::AddInt32(AddInt32),
        expr1: Box::new(MirScalarExpr::Column(0, TreatAsEqual(None))),
        expr2: Box::new(MirScalarExpr::Literal(
            Ok(Row::pack_slice(&[Datum::Int32(10)])),
            int32_type.clone(),
        )),
    };

    group.bench_function("add_int32_baseline", |b| {
        b.iter(|| black_box(add_int32.eval(black_box(&datums_mixed), &arena)))
    });

    // --- Batch benchmark: 10k evaluations of int32 > literal ---
    let n = 10_000;
    let mut datums_batch: Vec<Vec<Datum>> = Vec::with_capacity(n);
    for i in 0..n {
        datums_batch.push(vec![
            Datum::Int32(i as i32),
            Datum::Int64(i as i64),
            Datum::String("test"),
            Datum::Float64((i as f64).into()),
        ]);
    }

    group.bench_function(BenchmarkId::new("batch_int32_gt", "10k"), |b| {
        b.iter(|| {
            let mut count = 0u32;
            for datums in &datums_batch {
                if let Ok(Datum::True) = gt_int32.eval(datums, &arena) {
                    count += 1;
                }
            }
            black_box(count)
        })
    });

    // --- Batch benchmark: 10k evaluations of multiple predicates ---
    let lte_int64 = make_comparison(
        mz_expr::BinaryFunc::Lte(Lte),
        1,
        Row::pack_slice(&[Datum::Int64(5000)]),
        int64_type.clone(),
    );

    group.bench_function(BenchmarkId::new("batch_two_predicates", "10k"), |b| {
        b.iter(|| {
            let mut count = 0u32;
            for datums in &datums_batch {
                if let Ok(Datum::True) = gt_int32.eval(datums, &arena) {
                    if let Ok(Datum::True) = lte_int64.eval(datums, &arena) {
                        count += 1;
                    }
                }
            }
            black_box(count)
        })
    });

    group.finish();
}

criterion_group!(benches, bench_comparison_eval);
criterion_main!(benches);
