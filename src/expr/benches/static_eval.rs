// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks comparing tree-based `MirScalarExpr::eval` with compiled
//! `CompiledMirScalarExpr::eval`.

use bencher::{benchmark_group, benchmark_main, Bencher};

use mz_repr::{Datum, RowArena, SqlScalarType};

use mz_expr::func::{AddInt32, IsNull, MulInt32, NegInt32, Not};
use mz_expr::{CompiledMirScalarExpr, MirScalarExpr, VariadicFunc};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn lit_bool(b: bool) -> MirScalarExpr {
    MirScalarExpr::literal_ok(Datum::from(b), SqlScalarType::Bool)
}

fn lit_null_bool() -> MirScalarExpr {
    MirScalarExpr::literal_ok(Datum::Null, SqlScalarType::Bool)
}

fn lit_i32(v: i32) -> MirScalarExpr {
    MirScalarExpr::literal_ok(Datum::Int32(v), SqlScalarType::Int32)
}

fn lit_null_i32() -> MirScalarExpr {
    MirScalarExpr::literal_ok(Datum::Null, SqlScalarType::Int32)
}

fn col(c: usize) -> MirScalarExpr {
    MirScalarExpr::column(c)
}

// ---------------------------------------------------------------------------
// Expression builders
// ---------------------------------------------------------------------------

/// Column(0) — trivial baseline
fn expr_column() -> MirScalarExpr {
    col(0)
}

/// Column(0) + Column(1)
fn expr_add() -> MirScalarExpr {
    col(0).call_binary(col(1), AddInt32)
}

/// (Column(0) + Column(1)) * Column(2)
fn expr_arith_3() -> MirScalarExpr {
    col(0)
        .call_binary(col(1), AddInt32)
        .call_binary(col(2), MulInt32)
}

/// NOT(NOT(IS_NULL(Column(0))))
fn expr_unary_chain() -> MirScalarExpr {
    col(0).call_unary(IsNull).call_unary(Not).call_unary(Not)
}

/// IF Column(0) > 0 THEN Column(0) ELSE -Column(0)
/// Approximated as: IF NOT(IS_NULL(Column(0))) THEN Column(0) ELSE -Column(0)
fn expr_if() -> MirScalarExpr {
    MirScalarExpr::If {
        cond: Box::new(col(0).call_unary(IsNull).call_unary(Not)),
        then: Box::new(col(0)),
        els: Box::new(col(0).call_unary(NegInt32)),
    }
}

/// AND(Column(0), Column(1), Column(2), Column(3))
fn expr_and_4() -> MirScalarExpr {
    MirScalarExpr::CallVariadic {
        func: VariadicFunc::And,
        exprs: vec![col(0), col(1), col(2), col(3)],
    }
}

/// OR(Column(0), Column(1), Column(2), Column(3))
fn expr_or_4() -> MirScalarExpr {
    MirScalarExpr::CallVariadic {
        func: VariadicFunc::Or,
        exprs: vec![col(0), col(1), col(2), col(3)],
    }
}

/// COALESCE(Column(0), Column(1), Column(2), Literal(42))
fn expr_coalesce() -> MirScalarExpr {
    MirScalarExpr::CallVariadic {
        func: VariadicFunc::Coalesce,
        exprs: vec![col(0), col(1), col(2), lit_i32(42)],
    }
}

/// GREATEST(Column(0), Column(1), Column(2), Column(3))
fn expr_greatest() -> MirScalarExpr {
    MirScalarExpr::CallVariadic {
        func: VariadicFunc::Greatest,
        exprs: vec![col(0), col(1), col(2), col(3)],
    }
}

/// Nested: IF AND(Col0, Col1) THEN COALESCE(Col2, Col3, 0) ELSE Greatest(Col2, Col3)
fn expr_nested() -> MirScalarExpr {
    MirScalarExpr::If {
        cond: Box::new(MirScalarExpr::CallVariadic {
            func: VariadicFunc::And,
            exprs: vec![col(0), col(1)],
        }),
        then: Box::new(MirScalarExpr::CallVariadic {
            func: VariadicFunc::Coalesce,
            exprs: vec![col(2), col(3), lit_i32(0)],
        }),
        els: Box::new(MirScalarExpr::CallVariadic {
            func: VariadicFunc::Greatest,
            exprs: vec![col(2), col(3)],
        }),
    }
}

// ---------------------------------------------------------------------------
// Benchmark runner
// ---------------------------------------------------------------------------

/// Benchmark tree eval over many rows.
fn bench_tree(bencher: &mut Bencher, expr: &MirScalarExpr, rows: &[Vec<Datum<'static>>]) {
    let arena = RowArena::new();
    bencher.iter(|| {
        for row in rows {
            bencher::black_box(expr.eval(row, &arena));
        }
    });
}

/// Benchmark compiled eval over many rows (compilation cost excluded).
/// Uses `eval_with_stack` to amortize stack allocation across rows.
fn bench_compiled(
    bencher: &mut Bencher,
    compiled: &CompiledMirScalarExpr,
    rows: &[Vec<Datum<'static>>],
) {
    let arena = RowArena::new();
    let mut stack = Vec::new();
    bencher.iter(|| {
        for row in rows {
            bencher::black_box(compiled.eval_with_stack(row, &arena, &mut stack));
        }
    });
}

// ---------------------------------------------------------------------------
// Row data
// ---------------------------------------------------------------------------

const N: usize = 1024;

fn int_rows() -> Vec<Vec<Datum<'static>>> {
    (0..N)
        .map(|i| {
            vec![
                Datum::Int32(i as i32),
                Datum::Int32((i * 7 + 3) as i32),
                Datum::Int32((i * 13 + 1) as i32),
                Datum::Int32((i * 31) as i32),
            ]
        })
        .collect()
}

fn bool_rows() -> Vec<Vec<Datum<'static>>> {
    (0..N)
        .map(|i| {
            vec![
                Datum::from(i % 2 == 0),
                Datum::from(i % 3 == 0),
                Datum::from(i % 5 == 0),
                Datum::from(i % 7 == 0),
            ]
        })
        .collect()
}

fn nullable_int_rows() -> Vec<Vec<Datum<'static>>> {
    (0..N)
        .map(|i| {
            vec![
                if i % 3 == 0 {
                    Datum::Null
                } else {
                    Datum::Int32(i as i32)
                },
                if i % 5 == 0 {
                    Datum::Null
                } else {
                    Datum::Int32((i * 7) as i32)
                },
                if i % 7 == 0 {
                    Datum::Null
                } else {
                    Datum::Int32((i * 13) as i32)
                },
                Datum::Int32((i * 31) as i32),
            ]
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Individual benchmarks
// ---------------------------------------------------------------------------

fn column_tree(b: &mut Bencher) {
    let expr = expr_column();
    bench_tree(b, &expr, &int_rows());
}
fn column_compiled(b: &mut Bencher) {
    let expr = expr_column();
    let compiled = CompiledMirScalarExpr::from(&expr);
    bench_compiled(b, &compiled, &int_rows());
}

fn add_tree(b: &mut Bencher) {
    let expr = expr_add();
    bench_tree(b, &expr, &int_rows());
}
fn add_compiled(b: &mut Bencher) {
    let expr = expr_add();
    let compiled = CompiledMirScalarExpr::from(&expr);
    bench_compiled(b, &compiled, &int_rows());
}

fn arith3_tree(b: &mut Bencher) {
    let expr = expr_arith_3();
    bench_tree(b, &expr, &int_rows());
}
fn arith3_compiled(b: &mut Bencher) {
    let expr = expr_arith_3();
    let compiled = CompiledMirScalarExpr::from(&expr);
    bench_compiled(b, &compiled, &int_rows());
}

fn unary_chain_tree(b: &mut Bencher) {
    let expr = expr_unary_chain();
    bench_tree(b, &expr, &int_rows());
}
fn unary_chain_compiled(b: &mut Bencher) {
    let expr = expr_unary_chain();
    let compiled = CompiledMirScalarExpr::from(&expr);
    bench_compiled(b, &compiled, &int_rows());
}

fn if_tree(b: &mut Bencher) {
    let expr = expr_if();
    bench_tree(b, &expr, &int_rows());
}
fn if_compiled(b: &mut Bencher) {
    let expr = expr_if();
    let compiled = CompiledMirScalarExpr::from(&expr);
    bench_compiled(b, &compiled, &int_rows());
}

fn and4_tree(b: &mut Bencher) {
    let expr = expr_and_4();
    bench_tree(b, &expr, &bool_rows());
}
fn and4_compiled(b: &mut Bencher) {
    let expr = expr_and_4();
    let compiled = CompiledMirScalarExpr::from(&expr);
    bench_compiled(b, &compiled, &bool_rows());
}

fn or4_tree(b: &mut Bencher) {
    let expr = expr_or_4();
    bench_tree(b, &expr, &bool_rows());
}
fn or4_compiled(b: &mut Bencher) {
    let expr = expr_or_4();
    let compiled = CompiledMirScalarExpr::from(&expr);
    bench_compiled(b, &compiled, &bool_rows());
}

fn coalesce_tree(b: &mut Bencher) {
    let expr = expr_coalesce();
    bench_tree(b, &expr, &nullable_int_rows());
}
fn coalesce_compiled(b: &mut Bencher) {
    let expr = expr_coalesce();
    let compiled = CompiledMirScalarExpr::from(&expr);
    bench_compiled(b, &compiled, &nullable_int_rows());
}

fn greatest_tree(b: &mut Bencher) {
    let expr = expr_greatest();
    bench_tree(b, &expr, &nullable_int_rows());
}
fn greatest_compiled(b: &mut Bencher) {
    let expr = expr_greatest();
    let compiled = CompiledMirScalarExpr::from(&expr);
    bench_compiled(b, &compiled, &nullable_int_rows());
}

fn nested_tree(b: &mut Bencher) {
    let expr = expr_nested();
    bench_tree(b, &expr, &bool_rows());
}
fn nested_compiled(b: &mut Bencher) {
    let expr = expr_nested();
    let compiled = CompiledMirScalarExpr::from(&expr);
    bench_compiled(b, &compiled, &bool_rows());
}

// ---------------------------------------------------------------------------
// Groups & main
// ---------------------------------------------------------------------------

benchmark_group!(
    tree,
    column_tree,
    add_tree,
    arith3_tree,
    unary_chain_tree,
    if_tree,
    and4_tree,
    or4_tree,
    coalesce_tree,
    greatest_tree,
    nested_tree,
);

benchmark_group!(
    compiled,
    column_compiled,
    add_compiled,
    arith3_compiled,
    unary_chain_compiled,
    if_compiled,
    and4_compiled,
    or4_compiled,
    coalesce_compiled,
    greatest_compiled,
    nested_compiled,
);

benchmark_main!(tree, compiled);
