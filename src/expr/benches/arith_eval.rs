// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for MirScalarExpr integer arithmetic evaluation.
//!
//! Tests the fast-path arithmetic evaluator that bypasses the EagerBinaryFunc
//! ceremony for Add/Sub/Mul on Int16/Int32/Int64.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_expr::MirScalarExpr;
use mz_expr::func::{AddInt32, AddInt64, MulInt64, SubInt32};
use mz_ore::treat_as_equal::TreatAsEqual;
use mz_repr::{Datum, Row, RowArena, SqlColumnType, SqlScalarType};

/// Build a binary expression: Column(col_a) op Column(col_b)
fn make_col_op_col(func: mz_expr::BinaryFunc, col_a: usize, col_b: usize) -> MirScalarExpr {
    MirScalarExpr::CallBinary {
        func,
        expr1: Box::new(MirScalarExpr::Column(col_a, TreatAsEqual(None))),
        expr2: Box::new(MirScalarExpr::Column(col_b, TreatAsEqual(None))),
    }
}

/// Build a binary expression: Column(col_idx) op Literal(value)
fn make_col_op_lit(
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

fn bench_arith_eval(c: &mut Criterion) {
    let mut group = c.benchmark_group("arith_eval");
    let arena = RowArena::new();

    let int32_type = SqlColumnType {
        scalar_type: SqlScalarType::Int32,
        nullable: false,
    };
    let int64_type = SqlColumnType {
        scalar_type: SqlScalarType::Int64,
        nullable: false,
    };

    // Datums: [Int32(42), Int32(10), Int64(1000000), Int64(7)]
    let datums: Vec<Datum> = vec![
        Datum::Int32(42),
        Datum::Int32(10),
        Datum::Int64(1_000_000),
        Datum::Int64(7),
    ];

    // --- Single eval benchmarks ---

    // AddInt32: col + literal
    let add_i32 = make_col_op_lit(
        mz_expr::BinaryFunc::AddInt32(AddInt32),
        0,
        Row::pack_slice(&[Datum::Int32(10)]),
        int32_type.clone(),
    );
    group.bench_function("add_int32_col_lit", |b| {
        b.iter(|| black_box(add_i32.eval(black_box(&datums), &arena)))
    });

    // SubInt32: col - col
    let sub_i32 = make_col_op_col(mz_expr::BinaryFunc::SubInt32(SubInt32), 0, 1);
    group.bench_function("sub_int32_col_col", |b| {
        b.iter(|| black_box(sub_i32.eval(black_box(&datums), &arena)))
    });

    // AddInt64: col + literal
    let add_i64 = make_col_op_lit(
        mz_expr::BinaryFunc::AddInt64(AddInt64),
        2,
        Row::pack_slice(&[Datum::Int64(999)]),
        int64_type.clone(),
    );
    group.bench_function("add_int64_col_lit", |b| {
        b.iter(|| black_box(add_i64.eval(black_box(&datums), &arena)))
    });

    // MulInt64: col * col
    let mul_i64 = make_col_op_col(mz_expr::BinaryFunc::MulInt64(MulInt64), 2, 3);
    group.bench_function("mul_int64_col_col", |b| {
        b.iter(|| black_box(mul_i64.eval(black_box(&datums), &arena)))
    });

    // --- Batch benchmark: 10k AddInt64 ---
    let n = 10_000;
    let mut datums_batch: Vec<Vec<Datum>> = Vec::with_capacity(n);
    for i in 0..n {
        datums_batch.push(vec![
            Datum::Int32(i as i32),
            Datum::Int32(10),
            Datum::Int64(i as i64 * 1000),
            Datum::Int64(7),
        ]);
    }

    group.bench_function(BenchmarkId::new("batch_add_int64", "10k"), |b| {
        b.iter(|| {
            let mut sum = 0i64;
            for datums in &datums_batch {
                if let Ok(Datum::Int64(v)) = add_i64.eval(datums, &arena) {
                    sum = sum.wrapping_add(v);
                }
            }
            black_box(sum)
        })
    });

    // --- Batch: compound expression col0 * col1 + col2 ---
    let compound = MirScalarExpr::CallBinary {
        func: mz_expr::BinaryFunc::AddInt64(AddInt64),
        expr1: Box::new(MirScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::MulInt64(MulInt64),
            expr1: Box::new(MirScalarExpr::Column(2, TreatAsEqual(None))),
            expr2: Box::new(MirScalarExpr::Column(3, TreatAsEqual(None))),
        }),
        expr2: Box::new(MirScalarExpr::Column(2, TreatAsEqual(None))),
    };

    group.bench_function(BenchmarkId::new("batch_compound_int64", "10k"), |b| {
        b.iter(|| {
            let mut sum = 0i64;
            for datums in &datums_batch {
                if let Ok(Datum::Int64(v)) = compound.eval(datums, &arena) {
                    sum = sum.wrapping_add(v);
                }
            }
            black_box(sum)
        })
    });

    // --- Null propagation ---
    let datums_with_null: Vec<Datum> =
        vec![Datum::Int32(42), Datum::Null, Datum::Int64(1_000_000), Datum::Null];
    group.bench_function("add_int64_null", |b| {
        b.iter(|| black_box(add_i64.eval(black_box(&datums_with_null), &arena)))
    });

    group.finish();
}

criterion_group!(benches, bench_arith_eval);
criterion_main!(benches);
