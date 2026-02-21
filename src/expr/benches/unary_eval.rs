// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for MirScalarExpr unary function evaluation.
//!
//! Tests the fast-path evaluator that bypasses the EagerUnaryFunc
//! ceremony for IsNull, Not, NegInt32/64, CastInt32ToInt64, etc.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_expr::MirScalarExpr;
use mz_expr::func;
use mz_ore::treat_as_equal::TreatAsEqual;
use mz_repr::{Datum, RowArena};

/// Build a unary expression: func(Column(col_idx))
fn make_unary_col(func: mz_expr::UnaryFunc, col_idx: usize) -> MirScalarExpr {
    MirScalarExpr::CallUnary {
        func,
        expr: Box::new(MirScalarExpr::Column(col_idx, TreatAsEqual(None))),
    }
}

fn bench_unary_eval(c: &mut Criterion) {
    let mut group = c.benchmark_group("unary_eval");
    let arena = RowArena::new();

    // Datums: [Int32(42), Int64(1000000), True, Null, Int16(99), False, Int32(-5)]
    let datums: Vec<Datum> = vec![
        Datum::Int32(42),
        Datum::Int64(1_000_000),
        Datum::True,
        Datum::Null,
        Datum::Int16(99),
        Datum::False,
        Datum::Int32(-5),
    ];

    // --- IsNull ---
    let is_null_nonnull = make_unary_col(
        mz_expr::UnaryFunc::IsNull(func::IsNull),
        0, // Int32(42) -> false
    );
    group.bench_function("is_null_nonnull", |b| {
        b.iter(|| black_box(is_null_nonnull.eval(black_box(&datums), &arena)))
    });

    let is_null_null = make_unary_col(
        mz_expr::UnaryFunc::IsNull(func::IsNull),
        3, // Null -> true
    );
    group.bench_function("is_null_null", |b| {
        b.iter(|| black_box(is_null_null.eval(black_box(&datums), &arena)))
    });

    // --- Not ---
    let not_true = make_unary_col(
        mz_expr::UnaryFunc::Not(func::Not),
        2, // True -> False
    );
    group.bench_function("not_true", |b| {
        b.iter(|| black_box(not_true.eval(black_box(&datums), &arena)))
    });

    let not_false = make_unary_col(
        mz_expr::UnaryFunc::Not(func::Not),
        5, // False -> True
    );
    group.bench_function("not_false", |b| {
        b.iter(|| black_box(not_false.eval(black_box(&datums), &arena)))
    });

    let not_null = make_unary_col(
        mz_expr::UnaryFunc::Not(func::Not),
        3, // Null -> Null
    );
    group.bench_function("not_null", |b| {
        b.iter(|| black_box(not_null.eval(black_box(&datums), &arena)))
    });

    // --- NegInt32 ---
    let neg_i32 = make_unary_col(
        mz_expr::UnaryFunc::NegInt32(func::NegInt32),
        0, // Int32(42) -> Int32(-42)
    );
    group.bench_function("neg_int32", |b| {
        b.iter(|| black_box(neg_i32.eval(black_box(&datums), &arena)))
    });

    // --- NegInt64 ---
    let neg_i64 = make_unary_col(
        mz_expr::UnaryFunc::NegInt64(func::NegInt64),
        1, // Int64(1000000) -> Int64(-1000000)
    );
    group.bench_function("neg_int64", |b| {
        b.iter(|| black_box(neg_i64.eval(black_box(&datums), &arena)))
    });

    // --- CastInt32ToInt64 ---
    let cast_i32_i64 = make_unary_col(
        mz_expr::UnaryFunc::CastInt32ToInt64(func::CastInt32ToInt64),
        0, // Int32(42) -> Int64(42)
    );
    group.bench_function("cast_int32_to_int64", |b| {
        b.iter(|| black_box(cast_i32_i64.eval(black_box(&datums), &arena)))
    });

    // --- CastInt16ToInt32 ---
    let cast_i16_i32 = make_unary_col(
        mz_expr::UnaryFunc::CastInt16ToInt32(func::CastInt16ToInt32),
        4, // Int16(99) -> Int32(99)
    );
    group.bench_function("cast_int16_to_int32", |b| {
        b.iter(|| black_box(cast_i16_i32.eval(black_box(&datums), &arena)))
    });

    // --- CastBoolToInt32 ---
    let cast_bool_i32 = make_unary_col(
        mz_expr::UnaryFunc::CastBoolToInt32(func::CastBoolToInt32),
        2, // True -> Int32(1)
    );
    group.bench_function("cast_bool_to_int32", |b| {
        b.iter(|| black_box(cast_bool_i32.eval(black_box(&datums), &arena)))
    });

    // --- Batch benchmark: 10k IsNull ---
    let n = 10_000;
    let mut datums_batch: Vec<Vec<Datum>> = Vec::with_capacity(n);
    for i in 0..n {
        // 5% nulls, rest are Int64
        datums_batch.push(vec![
            if i % 20 == 0 {
                Datum::Null
            } else {
                Datum::Int64(i as i64)
            },
            Datum::True,
            Datum::Int32(i as i32),
            Datum::Int16(i as i16),
        ]);
    }

    let is_null_batch = make_unary_col(
        mz_expr::UnaryFunc::IsNull(func::IsNull),
        0,
    );
    group.bench_function(BenchmarkId::new("batch_is_null", "10k"), |b| {
        b.iter(|| {
            let mut count = 0u32;
            for datums in &datums_batch {
                if let Ok(Datum::True) = is_null_batch.eval(datums, &arena) {
                    count += 1;
                }
            }
            black_box(count)
        })
    });

    // --- Batch benchmark: 10k Not ---
    let not_batch = make_unary_col(
        mz_expr::UnaryFunc::Not(func::Not),
        1,
    );
    group.bench_function(BenchmarkId::new("batch_not", "10k"), |b| {
        b.iter(|| {
            let mut count = 0u32;
            for datums in &datums_batch {
                if let Ok(Datum::True) = not_batch.eval(datums, &arena) {
                    count += 1;
                }
            }
            black_box(count)
        })
    });

    // --- Batch benchmark: 10k CastInt32ToInt64 ---
    let cast_batch = make_unary_col(
        mz_expr::UnaryFunc::CastInt32ToInt64(func::CastInt32ToInt64),
        2,
    );
    group.bench_function(BenchmarkId::new("batch_cast_int32_to_int64", "10k"), |b| {
        b.iter(|| {
            let mut sum = 0i64;
            for datums in &datums_batch {
                if let Ok(Datum::Int64(v)) = cast_batch.eval(datums, &arena) {
                    sum = sum.wrapping_add(v);
                }
            }
            black_box(sum)
        })
    });

    // --- Batch benchmark: 10k NegInt32 ---
    let neg_batch = make_unary_col(
        mz_expr::UnaryFunc::NegInt32(func::NegInt32),
        2,
    );
    group.bench_function(BenchmarkId::new("batch_neg_int32", "10k"), |b| {
        b.iter(|| {
            let mut sum = 0i32;
            for datums in &datums_batch {
                if let Ok(Datum::Int32(v)) = neg_batch.eval(datums, &arena) {
                    sum = sum.wrapping_add(v);
                }
            }
            black_box(sum)
        })
    });

    // --- Compound: NOT(IsNull(col)) ---
    let not_is_null = MirScalarExpr::CallUnary {
        func: mz_expr::UnaryFunc::Not(func::Not),
        expr: Box::new(MirScalarExpr::CallUnary {
            func: mz_expr::UnaryFunc::IsNull(func::IsNull),
            expr: Box::new(MirScalarExpr::Column(0, TreatAsEqual(None))),
        }),
    };
    group.bench_function(BenchmarkId::new("batch_not_is_null", "10k"), |b| {
        b.iter(|| {
            let mut count = 0u32;
            for datums in &datums_batch {
                if let Ok(Datum::True) = not_is_null.eval(datums, &arena) {
                    count += 1;
                }
            }
            black_box(count)
        })
    });

    group.finish();
}

criterion_group!(benches, bench_unary_eval);
criterion_main!(benches);
