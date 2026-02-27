// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use mz_expr::MirScalarExpr;
use mz_expr::func::{BinaryFunc, Eq};
use mz_repr::{Datum, ReprColumnType, ReprRelationType, ReprScalarType, RowArena};
use mz_transform::Transform;
use mz_transform::case_literal::CaseLiteralTransform;

/// Build `Eq(lhs, rhs)` as a `MirScalarExpr`.
fn eq(lhs: MirScalarExpr, rhs: MirScalarExpr) -> MirScalarExpr {
    MirScalarExpr::CallBinary {
        func: BinaryFunc::Eq(Eq),
        expr1: Box::new(lhs),
        expr2: Box::new(rhs),
    }
}

/// Build `If(cond, then, els)`.
fn if_then_else(cond: MirScalarExpr, then: MirScalarExpr, els: MirScalarExpr) -> MirScalarExpr {
    cond.if_then_else(then, els)
}

/// Build an i64 literal.
fn lit_i64(v: i64) -> MirScalarExpr {
    MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64)
}

/// Column reference.
fn col(i: usize) -> MirScalarExpr {
    MirScalarExpr::column(i)
}

/// Build a nested If chain with `n` arms:
/// CASE #0 WHEN 0 THEN 0 WHEN 1 THEN 100 ... WHEN n-1 THEN 100*(n-1) ELSE -1 END
fn build_if_chain(n: usize) -> MirScalarExpr {
    let mut expr = lit_i64(-1);
    for i in (0..n).rev() {
        expr = if_then_else(eq(col(0), lit_i64(i as i64)), lit_i64(100 * i as i64), expr);
    }
    expr
}

/// Apply `CaseLiteralTransform` to the If chain and return the resulting scalar.
fn build_case_literal(n: usize) -> MirScalarExpr {
    let scalar = build_if_chain(n);
    let mut relation = mz_expr::MirRelationExpr::Map {
        input: Box::new(mz_expr::MirRelationExpr::constant(
            vec![vec![Datum::Int64(0)]],
            ReprRelationType::new(vec![ReprColumnType {
                scalar_type: ReprScalarType::Int64,
                nullable: false,
            }]),
        )),
        scalars: vec![scalar],
    };
    let mut features = mz_repr::optimize::OptimizerFeatures::default();
    features.enable_case_literal_transform = true;
    let typecheck_ctx = mz_transform::typecheck::empty_typechecking_context();
    let mut df_meta = mz_transform::dataflow::DataflowMetainfo::default();
    let mut transform_ctx =
        mz_transform::TransformCtx::local(&features, &typecheck_ctx, &mut df_meta, None, None);
    CaseLiteralTransform
        .transform(&mut relation, &mut transform_ctx)
        .unwrap();
    match relation {
        mz_expr::MirRelationExpr::Map { scalars, .. } => scalars.into_iter().next().unwrap(),
        other => panic!("expected Map, got {other:?}"),
    }
}

fn bench_case_literal(c: &mut Criterion) {
    let mut group = c.benchmark_group("case_literal");

    for n in [4, 16, 64, 256] {
        let if_chain = build_if_chain(n);
        let case_lit = build_case_literal(n);
        let arena = RowArena::new();

        let scenarios: &[(&str, i64)] = &[
            ("first", 0),
            ("middle", (n / 2) as i64),
            ("last", (n - 1) as i64),
            ("miss", -999),
        ];

        for &(scenario, input) in scenarios {
            let datums = [Datum::Int64(input)];

            group.bench_function(format!("if_chain/{n}_arms/{scenario}"), |b| {
                b.iter(|| black_box(&if_chain).eval(black_box(&datums), black_box(&arena)))
            });

            group.bench_function(format!("case_literal/{n}_arms/{scenario}"), |b| {
                b.iter(|| black_box(&case_lit).eval(black_box(&datums), black_box(&arena)))
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_case_literal);
criterion_main!(benches);
