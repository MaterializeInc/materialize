// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for MirScalarExpr float arithmetic and integer div/mod evaluation.
//!
//! Tests the fast-path evaluator that bypasses the EagerBinaryFunc
//! ceremony for float Add/Sub/Mul/Div and integer Div/Mod.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_expr::MirScalarExpr;
use mz_expr::func::{
    AddFloat64, DivFloat64, DivInt64, ModInt64, MulFloat64, SubFloat64,
};
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

fn bench_float_arith_eval(c: &mut Criterion) {
    let mut group = c.benchmark_group("float_arith_eval");
    let arena = RowArena::new();

    let float64_type = SqlColumnType {
        scalar_type: SqlScalarType::Float64,
        nullable: false,
    };
    // Datums: [Float64(3.14), Float64(2.71), Int64(1000000), Int64(7)]
    let datums: Vec<Datum> = vec![
        Datum::Float64(3.14.into()),
        Datum::Float64(2.71.into()),
        Datum::Int64(1_000_000),
        Datum::Int64(7),
    ];

    // --- Single eval benchmarks ---

    // AddFloat64: col + col
    let add_f64 = make_col_op_col(mz_expr::BinaryFunc::AddFloat64(AddFloat64), 0, 1);
    group.bench_function("add_f64_col_col", |b| {
        b.iter(|| black_box(add_f64.eval(black_box(&datums), &arena)))
    });

    // SubFloat64: col - col
    let sub_f64 = make_col_op_col(mz_expr::BinaryFunc::SubFloat64(SubFloat64), 0, 1);
    group.bench_function("sub_f64_col_col", |b| {
        b.iter(|| black_box(sub_f64.eval(black_box(&datums), &arena)))
    });

    // MulFloat64: col * col
    let mul_f64 = make_col_op_col(mz_expr::BinaryFunc::MulFloat64(MulFloat64), 0, 1);
    group.bench_function("mul_f64_col_col", |b| {
        b.iter(|| black_box(mul_f64.eval(black_box(&datums), &arena)))
    });

    // DivFloat64: col / literal
    let div_f64 = make_col_op_lit(
        mz_expr::BinaryFunc::DivFloat64(DivFloat64),
        0,
        Row::pack_slice(&[Datum::Float64(2.0.into())]),
        float64_type.clone(),
    );
    group.bench_function("div_f64_col_lit", |b| {
        b.iter(|| black_box(div_f64.eval(black_box(&datums), &arena)))
    });

    // DivInt64: col / col
    let div_i64 = make_col_op_col(mz_expr::BinaryFunc::DivInt64(DivInt64), 2, 3);
    group.bench_function("div_i64_col_col", |b| {
        b.iter(|| black_box(div_i64.eval(black_box(&datums), &arena)))
    });

    // ModInt64: col % col
    let mod_i64 = make_col_op_col(mz_expr::BinaryFunc::ModInt64(ModInt64), 2, 3);
    group.bench_function("mod_i64_col_col", |b| {
        b.iter(|| black_box(mod_i64.eval(black_box(&datums), &arena)))
    });

    // --- Batch benchmark: 10k float64 operations ---
    let n = 10_000;
    let mut datums_batch: Vec<Vec<Datum>> = Vec::with_capacity(n);
    for i in 0..n {
        datums_batch.push(vec![
            Datum::Float64((i as f64 * 1.1).into()),
            Datum::Float64((i as f64 * 0.7 + 1.0).into()),
            Datum::Int64(i as i64 * 1000 + 1),
            Datum::Int64(7),
        ]);
    }

    group.bench_function(BenchmarkId::new("batch_add_f64", "10k"), |b| {
        b.iter(|| {
            let mut sum = 0.0f64;
            for datums in &datums_batch {
                if let Ok(Datum::Float64(v)) = add_f64.eval(datums, &arena) {
                    sum += *v;
                }
            }
            black_box(sum)
        })
    });

    group.bench_function(BenchmarkId::new("batch_div_f64", "10k"), |b| {
        b.iter(|| {
            let mut sum = 0.0f64;
            for datums in &datums_batch {
                if let Ok(Datum::Float64(v)) = div_f64.eval(datums, &arena) {
                    sum += *v;
                }
            }
            black_box(sum)
        })
    });

    group.bench_function(BenchmarkId::new("batch_div_i64", "10k"), |b| {
        b.iter(|| {
            let mut sum = 0i64;
            for datums in &datums_batch {
                if let Ok(Datum::Int64(v)) = div_i64.eval(datums, &arena) {
                    sum = sum.wrapping_add(v);
                }
            }
            black_box(sum)
        })
    });

    // --- Compound expression: (a * b) + (a / b) ---
    let compound = MirScalarExpr::CallBinary {
        func: mz_expr::BinaryFunc::AddFloat64(AddFloat64),
        expr1: Box::new(MirScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::MulFloat64(MulFloat64),
            expr1: Box::new(MirScalarExpr::Column(0, TreatAsEqual(None))),
            expr2: Box::new(MirScalarExpr::Column(1, TreatAsEqual(None))),
        }),
        expr2: Box::new(MirScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::DivFloat64(DivFloat64),
            expr1: Box::new(MirScalarExpr::Column(0, TreatAsEqual(None))),
            expr2: Box::new(MirScalarExpr::Column(1, TreatAsEqual(None))),
        }),
    };

    group.bench_function(BenchmarkId::new("batch_compound_f64", "10k"), |b| {
        b.iter(|| {
            let mut sum = 0.0f64;
            for datums in &datums_batch {
                if let Ok(Datum::Float64(v)) = compound.eval(datums, &arena) {
                    sum += *v;
                }
            }
            black_box(sum)
        })
    });

    group.finish();
}

criterion_group!(benches, bench_float_arith_eval);
criterion_main!(benches);
