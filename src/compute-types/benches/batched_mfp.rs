// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compares ways of evaluating an MFP over a batch of rows.
//!
//! * `per_row`: one row at a time, as compute's `CollectionFlatMap` does.
//! * `batched`: [`MfpPlan::evaluate_batch`] on rows borrowed from the input,
//!   as the batched operators call it. It evaluates a column at a time where
//!   the plan has typed kernels, and a row at a time otherwise.
//!
//! The scenarios cover both paths: plans of `Int64` arithmetic and
//! comparisons, and plans with string functions or text predicates.
//! `batched` produces the same output as `per_row`, which `bench` checks
//! before measuring.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mz_compute_types::plan::scalar::{LirScalarExpr, mfp_mir_to_lir_plan};
use mz_expr::func::{AddInt64, DivInt64, Eq as EqFunc, Gt, Lower, MulInt64, SubInt64};
use mz_expr::{BatchInput, EvalError, MapFilterProject, MfpPlan, MirScalarExpr};
use mz_ore::cast::CastFrom;
use mz_repr::{Datum, DatumVec, Diff, ReprScalarType, Row, RowArena, Timestamp};

const ROWS: usize = 4096;

type Output = Result<(Row, Timestamp, Diff), (EvalError, Timestamp, Diff)>;

struct Scenario {
    name: &'static str,
    plan: MfpPlan<LirScalarExpr>,
    rows: Vec<Row>,
}

fn int(i: i64) -> MirScalarExpr {
    MirScalarExpr::literal_ok(Datum::Int64(i), ReprScalarType::Int64)
}

fn col(i: usize) -> MirScalarExpr {
    MirScalarExpr::column(i)
}

/// Deterministic values in `0..1000`.
fn values(seed: u64) -> impl Iterator<Item = i64> {
    let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    std::iter::repeat_with(move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        i64::try_from((state >> 33) % 1000).expect("fits")
    })
}

/// Rows of `arity` integers, one in `null_every` of them null (none if 0).
fn int_rows(arity: usize, null_every: i64) -> Vec<Row> {
    let mut values = values(u64::cast_from(arity));
    (0..ROWS)
        .map(|_| {
            Row::pack((0..arity).map(|_| {
                let value = values.next().unwrap();
                if null_every > 0 && value % null_every == 0 {
                    Datum::Null
                } else {
                    Datum::Int64(value)
                }
            }))
        })
        .collect()
}

fn scenarios() -> Vec<Scenario> {
    let plan = mfp_mir_to_lir_plan;
    let arith = || {
        MapFilterProject::new(4)
            .map([
                col(0)
                    .call_binary(col(1), AddInt64)
                    .call_binary(int(3), MulInt64),
                col(2).call_binary(col(3), SubInt64),
            ])
            .filter([col(4).call_binary(int(1000), Gt)])
            .project([4, 5])
    };
    let mut scenarios = Vec::new();

    // A cheap predicate that keeps half the rows, all columns projected.
    scenarios.push(Scenario {
        name: "filter_int",
        plan: plan(MapFilterProject::new(4).filter([col(0).call_binary(int(500), Gt)])),
        rows: int_rows(4, 0),
    });

    // Arithmetic maps and a predicate on a mapped column.
    scenarios.push(Scenario {
        name: "arith",
        plan: plan(arith()),
        rows: int_rows(4, 0),
    });

    // The same, with one input in ten null.
    scenarios.push(Scenario {
        name: "arith_nullable",
        plan: plan(arith()),
        rows: int_rows(4, 10),
    });

    // An allocating string function.
    let mut ints = values(7);
    let string_rows = (0..ROWS)
        .map(|i| {
            let text = format!("Value{i}ABCdef");
            Row::pack([Datum::Int64(ints.next().unwrap()), Datum::String(&text)])
        })
        .collect();
    scenarios.push(Scenario {
        name: "string_lower",
        plan: plan(
            MapFilterProject::new(2)
                .map([col(1).call_unary(Lower)])
                .filter([col(0).call_binary(int(100), Gt)])
                .project([0, 2]),
        ),
        rows: string_rows,
    });

    // A predicate on text, which no kernel covers.
    let mut ints = values(13);
    let text_rows = (0..ROWS)
        .map(|_| {
            let value = ints.next().unwrap();
            let text = format!("key{}", value % 16);
            Row::pack([Datum::String(&text), Datum::Int64(value)])
        })
        .collect();
    scenarios.push(Scenario {
        name: "text_filter",
        plan: plan(
            MapFilterProject::new(2)
                .filter([col(0).call_binary(
                    MirScalarExpr::literal_ok(Datum::String("key3"), ReprScalarType::String),
                    EqFunc,
                )])
                .project([1]),
        ),
        rows: text_rows,
    });

    // A division guarded by `CASE`; one row in eight divides by zero.
    let mut ints = values(11);
    let guarded_rows = (0..ROWS)
        .map(|i| {
            let divisor = if i % 8 == 0 {
                0
            } else {
                ints.next().unwrap() + 1
            };
            Row::pack([Datum::Int64(ints.next().unwrap()), Datum::Int64(divisor)])
        })
        .collect();
    scenarios.push(Scenario {
        name: "case_guarded_div",
        plan: plan(
            MapFilterProject::new(2)
                .map([col(1)
                    .call_binary(int(0), EqFunc)
                    .if_then_else(int(0), col(0).call_binary(col(1), DivInt64))])
                .project([2]),
        ),
        rows: guarded_rows,
    });

    // A wide row passed through a cheap predicate.
    scenarios.push(Scenario {
        name: "wide_filter",
        plan: plan(MapFilterProject::new(32).filter([col(0).call_binary(int(500), Gt)])),
        rows: int_rows(32, 0),
    });

    scenarios
}

fn per_row(plan: &MfpPlan<LirScalarExpr>, rows: &[Row], out: &mut Vec<Output>) {
    let mut datum_vec = DatumVec::new();
    let mut row_builder = Row::default();
    for row in rows {
        let arena = RowArena::new();
        let mut datums = datum_vec.borrow_with(row);
        out.extend(plan.evaluate(
            &mut datums,
            &arena,
            Timestamp::MIN,
            Diff::ONE,
            |_| true,
            &mut row_builder,
        ));
    }
}

/// Input rows of a batch, borrowed.
struct Rows<'a>(&'a [Row]);

impl<'a> BatchInput<'a> for Rows<'a> {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn read(&self, index: usize, datums: &mut Vec<Datum<'a>>) {
        datums.clear();
        datums.extend(self.0[index].iter());
    }
}

fn batched(plan: &MfpPlan<LirScalarExpr>, rows: &[Row], out: &mut Vec<Output>) {
    let arena = RowArena::new();
    let mut row_builder = Row::default();
    plan.evaluate_batch(&Rows(rows), &arena, |_, outcome, datums| {
        out.extend(plan.finish_batch_row(
            outcome,
            datums,
            &arena,
            Timestamp::MIN,
            Diff::ONE,
            |_| true,
            &mut row_builder,
        ));
    });
}

type Variant = fn(&MfpPlan<LirScalarExpr>, &[Row], &mut Vec<Output>);

const VARIANTS: [(&str, Variant); 2] = [("per_row", per_row), ("batched", batched)];

fn bench(c: &mut Criterion) {
    for scenario in scenarios() {
        let mut expected = Vec::new();
        per_row(&scenario.plan, &scenario.rows, &mut expected);
        for (name, variant) in VARIANTS {
            let mut out = Vec::new();
            variant(&scenario.plan, &scenario.rows, &mut out);
            assert_eq!(
                out, expected,
                "{} disagrees with per_row on {}",
                name, scenario.name
            );
        }

        let mut group = c.benchmark_group(format!("batched_mfp/{}", scenario.name));
        group.throughput(Throughput::Elements(u64::cast_from(ROWS)));
        for (name, variant) in VARIANTS {
            let mut out = Vec::with_capacity(ROWS);
            group.bench_function(name, |b| {
                b.iter(|| {
                    out.clear();
                    variant(
                        black_box(&scenario.plan),
                        black_box(&scenario.rows),
                        &mut out,
                    );
                })
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
