// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks comparing compiled (WASM) vs interpreted MFP evaluation.
//!
//! Each benchmark constructs a [`MapFilterProject`] with expressions and/or predicates,
//! pre-generates a batch of input rows, then measures per-row evaluation throughput
//! for the interpreted ([`SafeMfpPlan`]), compiled per-row ([`CompiledMfp`]),
//! and compiled batch ([`CompiledExprSession::eval_batch`]) paths.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use mz_expr::{BinaryFunc, EvalError, MapFilterProject, MirScalarExpr, UnaryFunc, VariadicFunc};
use mz_repr::{Datum, ReprColumnType, ReprScalarType, Row, RowArena, SqlScalarType};

use mz_expr_compiler::engine::ExprEngine;
use mz_expr_compiler::eval::{CompiledExprSession, CompiledMfp};

// ---------------------------------------------------------------------------
// Expression builders
// ---------------------------------------------------------------------------

fn col(idx: usize) -> MirScalarExpr {
    MirScalarExpr::Column(idx, Default::default())
}

fn lit_i64(v: i64) -> MirScalarExpr {
    MirScalarExpr::Literal(
        Ok(Row::pack_slice(&[Datum::Int64(v)])),
        ReprColumnType {
            scalar_type: ReprScalarType::Int64,
            nullable: false,
        },
    )
}

fn add_i64(a: MirScalarExpr, b: MirScalarExpr) -> MirScalarExpr {
    MirScalarExpr::CallBinary {
        func: BinaryFunc::AddInt64(mz_expr::func::AddInt64),
        expr1: Box::new(a),
        expr2: Box::new(b),
    }
}

fn mul_i64(a: MirScalarExpr, b: MirScalarExpr) -> MirScalarExpr {
    MirScalarExpr::CallBinary {
        func: BinaryFunc::MulInt64(mz_expr::func::MulInt64),
        expr1: Box::new(a),
        expr2: Box::new(b),
    }
}

fn sub_i64(a: MirScalarExpr, b: MirScalarExpr) -> MirScalarExpr {
    MirScalarExpr::CallBinary {
        func: BinaryFunc::SubInt64(mz_expr::func::SubInt64),
        expr1: Box::new(a),
        expr2: Box::new(b),
    }
}

fn lt(a: MirScalarExpr, b: MirScalarExpr) -> MirScalarExpr {
    MirScalarExpr::CallBinary {
        func: BinaryFunc::Lt(mz_expr::func::Lt),
        expr1: Box::new(a),
        expr2: Box::new(b),
    }
}

fn gt(a: MirScalarExpr, b: MirScalarExpr) -> MirScalarExpr {
    MirScalarExpr::CallBinary {
        func: BinaryFunc::Gt(mz_expr::func::Gt),
        expr1: Box::new(a),
        expr2: Box::new(b),
    }
}

fn eq(a: MirScalarExpr, b: MirScalarExpr) -> MirScalarExpr {
    MirScalarExpr::CallBinary {
        func: BinaryFunc::Eq(mz_expr::func::Eq),
        expr1: Box::new(a),
        expr2: Box::new(b),
    }
}

fn and(children: Vec<MirScalarExpr>) -> MirScalarExpr {
    MirScalarExpr::CallVariadic {
        func: VariadicFunc::And(mz_expr::func::variadic::And),
        exprs: children,
    }
}

fn or(children: Vec<MirScalarExpr>) -> MirScalarExpr {
    MirScalarExpr::CallVariadic {
        func: VariadicFunc::Or(mz_expr::func::variadic::Or),
        exprs: children,
    }
}

fn not(a: MirScalarExpr) -> MirScalarExpr {
    MirScalarExpr::CallUnary {
        func: UnaryFunc::Not(mz_expr::func::Not),
        expr: Box::new(a),
    }
}

fn is_null(a: MirScalarExpr) -> MirScalarExpr {
    MirScalarExpr::CallUnary {
        func: UnaryFunc::IsNull(mz_expr::func::IsNull),
        expr: Box::new(a),
    }
}

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------

/// Generates `n` rows of `num_cols` Int64 columns with deterministic values.
/// About 10% of values are NULL.
fn generate_rows(n: usize, num_cols: usize) -> Vec<Row> {
    let mut rows = Vec::with_capacity(n);
    for i in 0..n {
        let datums: Vec<Datum<'_>> = (0..num_cols)
            .map(|c| {
                // Deterministic pseudo-random: mix row and column index.
                let v = i64::try_from(i)
                    .unwrap()
                    .wrapping_mul(31)
                    .wrapping_add(i64::try_from(c).unwrap() * 97);
                if v % 10 == 0 {
                    Datum::Null
                } else {
                    Datum::Int64(v)
                }
            })
            .collect();
        rows.push(Row::pack_slice(&datums));
    }
    rows
}

// ---------------------------------------------------------------------------
// Benchmark scenarios
// ---------------------------------------------------------------------------

/// Scenario descriptor for a benchmark case.
struct Scenario {
    name: &'static str,
    mfp: MapFilterProject,
    /// The key expression to batch-evaluate (either the MFP expression or predicate).
    bench_expr: MirScalarExpr,
    input_types: Vec<SqlScalarType>,
    num_cols: usize,
}

/// Simple: `col(0) + col(1)`
fn scenario_simple_add() -> Scenario {
    let expr = add_i64(col(0), col(1));
    Scenario {
        name: "add_two_cols",
        mfp: MapFilterProject {
            expressions: vec![expr.clone()],
            predicates: vec![],
            projection: vec![0, 1, 2],
            input_arity: 2,
        },
        bench_expr: expr,
        input_types: vec![SqlScalarType::Int64, SqlScalarType::Int64],
        num_cols: 2,
    }
}

/// Arithmetic chain: `(col(0) + col(1)) * (col(2) - col(0))`
fn scenario_arithmetic_chain() -> Scenario {
    let expr = mul_i64(add_i64(col(0), col(1)), sub_i64(col(2), col(0)));
    Scenario {
        name: "arith_chain",
        mfp: MapFilterProject {
            expressions: vec![expr.clone()],
            predicates: vec![],
            projection: vec![0, 1, 2, 3],
            input_arity: 3,
        },
        bench_expr: expr,
        input_types: vec![
            SqlScalarType::Int64,
            SqlScalarType::Int64,
            SqlScalarType::Int64,
        ],
        num_cols: 3,
    }
}

/// Simple predicate: `WHERE col(0) > 42`
fn scenario_simple_predicate() -> Scenario {
    let pred = gt(col(0), lit_i64(42));
    Scenario {
        name: "where_gt_42",
        mfp: MapFilterProject {
            expressions: vec![],
            predicates: vec![(1, pred.clone())],
            projection: vec![0],
            input_arity: 1,
        },
        bench_expr: pred,
        input_types: vec![SqlScalarType::Int64],
        num_cols: 1,
    }
}

/// Compound predicate: `WHERE col(0) > 0 AND col(1) < 1000`
fn scenario_compound_predicate() -> Scenario {
    let pred = and(vec![gt(col(0), lit_i64(0)), lt(col(1), lit_i64(1000))]);
    Scenario {
        name: "where_and",
        mfp: MapFilterProject {
            expressions: vec![],
            predicates: vec![(2, pred.clone())],
            projection: vec![0, 1],
            input_arity: 2,
        },
        bench_expr: pred,
        input_types: vec![SqlScalarType::Int64, SqlScalarType::Int64],
        num_cols: 2,
    }
}

/// Map + filter: `SELECT col(0) + col(1) WHERE col(0) > 0 AND col(1) > 0`
fn scenario_map_and_filter() -> Scenario {
    let expr = add_i64(col(0), col(1));
    let pred = and(vec![gt(col(0), lit_i64(0)), gt(col(1), lit_i64(0))]);
    Scenario {
        name: "map_and_filter",
        mfp: MapFilterProject {
            expressions: vec![expr.clone()],
            predicates: vec![(2, pred)],
            projection: vec![2],
            input_arity: 2,
        },
        bench_expr: expr,
        input_types: vec![SqlScalarType::Int64, SqlScalarType::Int64],
        num_cols: 2,
    }
}

/// Complex: `WHERE (col(0) + col(1) > col(2)) OR (col(0) = 0 AND NOT(col(1) < col(2)))`
fn scenario_complex_bool() -> Scenario {
    let lhs = gt(add_i64(col(0), col(1)), col(2));
    let rhs = and(vec![eq(col(0), lit_i64(0)), not(lt(col(1), col(2)))]);
    let pred = or(vec![lhs, rhs]);
    Scenario {
        name: "complex_bool",
        mfp: MapFilterProject {
            expressions: vec![],
            predicates: vec![(3, pred.clone())],
            projection: vec![0, 1, 2],
            input_arity: 3,
        },
        bench_expr: pred,
        input_types: vec![
            SqlScalarType::Int64,
            SqlScalarType::Int64,
            SqlScalarType::Int64,
        ],
        num_cols: 3,
    }
}

/// IsNull predicate: `WHERE col(0) IS NOT NULL AND col(1) IS NOT NULL`
fn scenario_is_null_filter() -> Scenario {
    let pred = and(vec![not(is_null(col(0))), not(is_null(col(1)))]);
    Scenario {
        name: "is_null_filter",
        mfp: MapFilterProject {
            expressions: vec![],
            predicates: vec![(2, pred.clone())],
            projection: vec![0, 1],
            input_arity: 2,
        },
        bench_expr: pred,
        input_types: vec![SqlScalarType::Int64, SqlScalarType::Int64],
        num_cols: 2,
    }
}

// ---------------------------------------------------------------------------
// Benchmark harness
// ---------------------------------------------------------------------------

fn bench_mfp(c: &mut Criterion) {
    let scenarios: Vec<Scenario> = vec![
        scenario_simple_add(),
        scenario_arithmetic_chain(),
        scenario_simple_predicate(),
        scenario_compound_predicate(),
        scenario_map_and_filter(),
        scenario_complex_bool(),
        scenario_is_null_filter(),
    ];

    let engine = ExprEngine::new().unwrap();

    for num_rows in [100usize, 1_000, 10_000] {
        let mut group = c.benchmark_group(format!("mfp_{num_rows}_rows"));
        group.throughput(Throughput::Elements(u64::try_from(num_rows).unwrap()));

        for scenario in &scenarios {
            let rows = generate_rows(num_rows, scenario.num_cols);

            // Pre-unpack rows for batch evaluation (done once, outside benchmark loop).
            let unpacked: Vec<Vec<Datum<'_>>> = rows.iter().map(|r| r.unpack()).collect();
            let datum_slices: Vec<&[Datum<'_>]> = unpacked.iter().map(|v| v.as_slice()).collect();

            // --- Interpreted path (full MFP, per-row) ---
            {
                let mfp = scenario.mfp.clone();
                let plan = mfp.into_plan().unwrap();
                let safe_plan = plan.non_temporal();

                group.bench_with_input(
                    BenchmarkId::new(format!("{}/interpreted", scenario.name), num_rows),
                    &rows,
                    |b, rows| {
                        b.iter(|| {
                            let arena = RowArena::new();
                            let mut row_buf = Row::default();
                            for row in rows {
                                let mut datums: Vec<Datum<'_>> = row.unpack();
                                let _ = safe_plan.evaluate_into(&mut datums, &arena, &mut row_buf);
                            }
                        });
                    },
                );
            }

            // --- Compiled path (full MFP, per-row WASM calls) ---
            {
                let mfp = scenario.mfp.clone();
                let plan = mfp.into_plan().unwrap();
                let input_types = scenario.input_types.clone();
                let mut compiled = match CompiledMfp::try_new(plan, &input_types) {
                    Ok(c) => c,
                    Err(_) => {
                        // If nothing compiled, skip compiled benchmarks for this scenario.
                        continue;
                    }
                };

                group.bench_with_input(
                    BenchmarkId::new(format!("{}/compiled", scenario.name), num_rows),
                    &rows,
                    |b, rows| {
                        b.iter(|| {
                            let arena = RowArena::new();
                            let mut row_buf = Row::default();
                            for row in rows {
                                let mut datums: Vec<Datum<'_>> = row.unpack();
                                let _: Vec<Result<_, (EvalError, _, _)>> = compiled.evaluate(
                                    &mut datums,
                                    &arena,
                                    0u64.into(),
                                    mz_repr::Diff::from(1),
                                    |_| true,
                                    &mut row_buf,
                                );
                            }
                        });
                    },
                );
            }

            // --- Compiled batch (single WASM call for all rows) ---
            {
                let compile_types: Vec<(SqlScalarType, bool)> = scenario
                    .input_types
                    .iter()
                    .map(|st| (st.clone(), true))
                    .collect();
                let compiled_expr = match engine.compile(&scenario.bench_expr, &compile_types) {
                    Ok(Some(c)) => c,
                    _ => continue,
                };
                let mut session = match CompiledExprSession::new(&compiled_expr) {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                group.bench_with_input(
                    BenchmarkId::new(format!("{}/compiled_batch", scenario.name), num_rows),
                    &datum_slices,
                    |b, slices| {
                        b.iter(|| {
                            let _ = session.eval_batch(slices);
                        });
                    },
                );
            }
        }

        group.finish();
    }
}

criterion_group!(benches, bench_mfp);
criterion_main!(benches);
