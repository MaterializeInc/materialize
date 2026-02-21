// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for MirScalarExpr::contains_temporal and contains_unmaterializable.
//!
//! Tests the direct recursive implementations vs the old visit_pre-based
//! approach that allocated a Vec on every call.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_expr::MirScalarExpr;
use mz_expr::func;
use mz_ore::treat_as_equal::TreatAsEqual;

/// Build a Column leaf
fn col(idx: usize) -> MirScalarExpr {
    MirScalarExpr::Column(idx, TreatAsEqual(None))
}

/// Build a Literal leaf
fn lit_int32(v: i32) -> MirScalarExpr {
    use mz_repr::{Datum, Row, SqlColumnType, SqlScalarType};
    MirScalarExpr::Literal(
        Ok(Row::pack_slice(&[Datum::Int32(v)])),
        SqlColumnType {
            scalar_type: SqlScalarType::Int32,
            nullable: false,
        },
    )
}

/// Build: col(0) > col(1) — a simple binary expression with no temporal
fn simple_binary() -> MirScalarExpr {
    MirScalarExpr::CallBinary {
        func: func::Gt.into(),
        expr1: Box::new(col(0)),
        expr2: Box::new(col(1)),
    }
}

/// Build: col(0) > 10 AND col(1) < 20 — a compound expression with no temporal
fn compound_no_temporal() -> MirScalarExpr {
    let gt = MirScalarExpr::CallBinary {
        func: func::Gt.into(),
        expr1: Box::new(col(0)),
        expr2: Box::new(lit_int32(10)),
    };
    let lt = MirScalarExpr::CallBinary {
        func: func::Lt.into(),
        expr1: Box::new(col(1)),
        expr2: Box::new(lit_int32(20)),
    };
    MirScalarExpr::CallVariadic {
        func: mz_expr::VariadicFunc::And,
        exprs: vec![gt, lt],
    }
}

/// Build a deep chain: NOT(NOT(NOT(... col(0) ...))) with n levels
fn deep_unary_chain(n: usize) -> MirScalarExpr {
    let mut expr = col(0);
    for _ in 0..n {
        expr = MirScalarExpr::CallUnary {
            func: mz_expr::UnaryFunc::IsNull(func::IsNull),
            expr: Box::new(expr),
        };
    }
    expr
}

fn bench_contains_temporal(c: &mut Criterion) {
    let mut group = c.benchmark_group("contains_temporal");

    // Leaf node (Column) — the most common case in coverage
    let leaf = col(0);
    group.bench_function("leaf_column", |b| {
        b.iter(|| black_box(&leaf).contains_temporal())
    });

    // Literal leaf
    let literal = lit_int32(42);
    group.bench_function("leaf_literal", |b| {
        b.iter(|| black_box(&literal).contains_temporal())
    });

    // Simple binary: col(0) > col(1)
    let binary = simple_binary();
    group.bench_function("binary_2_leaves", |b| {
        b.iter(|| black_box(&binary).contains_temporal())
    });

    // Compound: AND(col(0) > 10, col(1) < 20) — 7 nodes total
    let compound = compound_no_temporal();
    group.bench_function("compound_7_nodes", |b| {
        b.iter(|| black_box(&compound).contains_temporal())
    });

    // Deep chain: 10 levels of IsNull(IsNull(...col(0)...)) — 11 nodes
    let deep = deep_unary_chain(10);
    group.bench_function("deep_chain_11_nodes", |b| {
        b.iter(|| black_box(&deep).contains_temporal())
    });

    // Batch: 10k calls on a compound expression
    let compound = compound_no_temporal();
    group.bench_with_input(
        BenchmarkId::new("batch_compound", "10k"),
        &10_000usize,
        |b, &n| {
            b.iter(|| {
                let mut total = 0u32;
                for _ in 0..n {
                    if black_box(&compound).contains_temporal() {
                        total += 1;
                    }
                }
                total
            })
        },
    );

    group.finish();
}

fn bench_contains_unmaterializable(c: &mut Criterion) {
    let mut group = c.benchmark_group("contains_unmaterializable");

    let leaf = col(0);
    group.bench_function("leaf_column", |b| {
        b.iter(|| black_box(&leaf).contains_unmaterializable())
    });

    let binary = simple_binary();
    group.bench_function("binary_2_leaves", |b| {
        b.iter(|| black_box(&binary).contains_unmaterializable())
    });

    let compound = compound_no_temporal();
    group.bench_function("compound_7_nodes", |b| {
        b.iter(|| black_box(&compound).contains_unmaterializable())
    });

    group.finish();
}

criterion_group!(benches, bench_contains_temporal, bench_contains_unmaterializable);
criterion_main!(benches);
