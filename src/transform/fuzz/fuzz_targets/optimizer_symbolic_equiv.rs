// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: the logical optimizer must preserve results over *symbolic*
//! inputs. `full_optimizer_equiv` builds plans rooted at `Constant`s, so the
//! optimizer constant-folds everything away before the interesting relational
//! planning (join ordering/implementation, predicate and projection pushdown
//! through `Get`s, key inference) ever runs. This target instead roots the plan
//! at `Get`s, opaque relations, exactly what the optimizer sees when planning a
//! real query against catalog objects, so that planning actually happens, while
//! still retaining a ground-truth oracle.
//!
//! Each `Get` is bound (in a side table) to a concrete, fuzzed constant
//! collection. The oracle:
//!
//!   1. `baseline = collapse(substitute(plan))`. Inline each `Get`'s data, then
//!      fold to the actual result rows.
//!   2. `optimized = optimize(plan)`. Run the full logical optimizer with the
//!      `Get`s still symbolic, so join/pushdown/key planning runs for real.
//!   3. `after = collapse(substitute(optimized))`. Inline the same data into the
//!      optimized plan and fold.
//!   4. assert `baseline == after`.
//!
//! `substitute` replaces only the global `Get`s we created. The optimizer's own
//! `Let`/local `Get` bindings (e.g. from CSE) are collapsed by `collapse`, which
//! iterates `FoldConstants` + `NormalizeLets` until the plan reduces to a
//! `Constant`. The comparison is conservative (only asserted when both sides
//! fold, a `Typecheck`/optimizer error is a skip), so a surviving divergence or
//! an optimizer panic is a genuine finding. It covers the symbolic-input
//! planning that the constant-rooted target cannot reach.

#![no_main]

use std::collections::BTreeMap;

use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::{
    AggregateExpr, AggregateFunc, ColumnOrder, EvalError, Id, MirRelationExpr, MirScalarExpr, func,
};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{Datum, Diff, GlobalId, ReprColumnType, ReprRelationType, ReprScalarType, Row};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::fold_constants::FoldConstants;
use mz_transform::normalize_lets::NormalizeLets;
use mz_transform::{Optimizer, TransformCtx, typecheck};

#[derive(Clone, Copy, PartialEq)]
enum Ty {
    Int32,
    Int64,
    Bool,
}

fn scalar_ty(ty: Ty) -> ReprScalarType {
    match ty {
        Ty::Int32 => ReprScalarType::Int32,
        Ty::Int64 => ReprScalarType::Int64,
        Ty::Bool => ReprScalarType::Bool,
    }
}

fn rand_ty(u: &mut Unstructured) -> arbitrary::Result<Ty> {
    Ok(match u.int_in_range(0u8..=2)? {
        0 => Ty::Int32,
        1 => Ty::Int64,
        _ => Ty::Bool,
    })
}

fn gen_datum(u: &mut Unstructured, ty: Ty) -> arbitrary::Result<Datum<'static>> {
    if u.ratio(1u8, 5u8)? {
        return Ok(Datum::Null);
    }
    Ok(match ty {
        Ty::Int32 => Datum::Int32(i32::arbitrary(u)?),
        Ty::Int64 => Datum::Int64(i64::arbitrary(u)?),
        Ty::Bool => {
            if bool::arbitrary(u)? {
                Datum::True
            } else {
                Datum::False
            }
        }
    })
}

fn cols_of(schema: &[Ty], ty: Ty) -> Vec<usize> {
    schema
        .iter()
        .enumerate()
        .filter(|(_, t)| **t == ty)
        .map(|(i, _)| i)
        .collect()
}

/// A shallow, well-typed scalar expression of type `ty` over `schema`.
fn gen_scalar(
    u: &mut Unstructured,
    ty: Ty,
    schema: &[Ty],
    depth: u32,
) -> arbitrary::Result<MirScalarExpr> {
    let st = scalar_ty(ty);
    if depth == 0 || u.ratio(1u8, 2u8)? {
        let cols = cols_of(schema, ty);
        if !cols.is_empty() && bool::arbitrary(u)? {
            let idx = u.int_in_range(0..=cols.len() - 1)?;
            return Ok(MirScalarExpr::column(cols[idx]));
        }
        return Ok(match u.int_in_range(0u8..=2)? {
            0 => MirScalarExpr::literal_ok(gen_datum(u, ty)?, st),
            1 => MirScalarExpr::literal_null(st),
            _ => MirScalarExpr::literal(Err(EvalError::DivisionByZero), st),
        });
    }
    let d = depth - 1;
    Ok(match ty {
        Ty::Int32 => {
            let a = gen_scalar(u, Ty::Int32, schema, d)?;
            let b = gen_scalar(u, Ty::Int32, schema, d)?;
            match u.int_in_range(0u8..=2)? {
                0 => a.call_binary(b, func::AddInt32),
                1 => a.call_binary(b, func::SubInt32),
                _ => a.call_binary(b, func::MulInt32),
            }
        }
        Ty::Int64 => {
            let a = gen_scalar(u, Ty::Int64, schema, d)?;
            let b = gen_scalar(u, Ty::Int64, schema, d)?;
            match u.int_in_range(0u8..=2)? {
                0 => a.call_binary(b, func::AddInt64),
                1 => a.call_binary(b, func::SubInt64),
                _ => a.call_binary(b, func::MulInt64),
            }
        }
        Ty::Bool => match u.int_in_range(0u8..=3)? {
            0 => gen_scalar(u, Ty::Bool, schema, d)?.and(gen_scalar(u, Ty::Bool, schema, d)?),
            1 => gen_scalar(u, Ty::Bool, schema, d)?.or(gen_scalar(u, Ty::Bool, schema, d)?),
            2 => gen_scalar(u, Ty::Bool, schema, d)?.not(),
            _ => {
                let t = rand_ty(u)?;
                let a = gen_scalar(u, t, schema, d)?;
                let b = gen_scalar(u, t, schema, d)?;
                a.call_binary(b, func::Eq)
            }
        },
    })
}

/// A symbolic `Get` leaf bound (in `data`) to a fresh constant collection.
fn gen_get(
    u: &mut Unstructured,
    next_id: &mut u64,
    data: &mut BTreeMap<u64, MirRelationExpr>,
) -> arbitrary::Result<(MirRelationExpr, Vec<Ty>)> {
    let ncols = u.int_in_range(1usize..=3)?;
    let schema: Vec<Ty> = (0..ncols)
        .map(|_| rand_ty(u))
        .collect::<arbitrary::Result<_>>()?;
    let col_types: Vec<ReprColumnType> = schema
        .iter()
        .map(|t| scalar_ty(*t).nullable(true))
        .collect();
    let typ = ReprRelationType::new(col_types);
    let nrows = u.int_in_range(0usize..=4)?;
    let mut rows = Vec::with_capacity(nrows);
    for _ in 0..nrows {
        let mut row = Vec::with_capacity(ncols);
        for t in &schema {
            row.push(gen_datum(u, *t)?);
        }
        rows.push(row);
    }
    let constant = MirRelationExpr::constant(rows, typ.clone());

    let id = *next_id;
    *next_id += 1;
    data.insert(id, constant);

    Ok((MirRelationExpr::global_get(GlobalId::User(id), typ), schema))
}

/// One aggregate over `schema`, plus the scalar type of its output column.
fn gen_aggregate(u: &mut Unstructured, schema: &[Ty]) -> arbitrary::Result<(AggregateExpr, Ty)> {
    let mut opts: Vec<(AggregateFunc, usize, Ty)> = Vec::new();
    for &c in &cols_of(schema, Ty::Int32) {
        opts.push((AggregateFunc::MaxInt32, c, Ty::Int32));
        opts.push((AggregateFunc::MinInt32, c, Ty::Int32));
        opts.push((AggregateFunc::SumInt32, c, Ty::Int64));
    }
    for &c in &cols_of(schema, Ty::Int64) {
        opts.push((AggregateFunc::MaxInt64, c, Ty::Int64));
        opts.push((AggregateFunc::MinInt64, c, Ty::Int64));
    }
    for &c in &cols_of(schema, Ty::Bool) {
        opts.push((AggregateFunc::Any, c, Ty::Bool));
        opts.push((AggregateFunc::All, c, Ty::Bool));
    }
    opts.push((AggregateFunc::Count, 0, Ty::Int64));

    let idx = u.int_in_range(0..=opts.len() - 1)?;
    let (func, col, out) = opts[idx].clone();
    Ok((
        AggregateExpr {
            func,
            expr: MirScalarExpr::column(col),
            distinct: bool::arbitrary(u)?,
        },
        out,
    ))
}

fn gen_rel(
    u: &mut Unstructured,
    depth: u32,
    next_id: &mut u64,
    data: &mut BTreeMap<u64, MirRelationExpr>,
) -> arbitrary::Result<(MirRelationExpr, Vec<Ty>)> {
    if depth == 0 || u.ratio(2u8, 5u8)? {
        return gen_get(u, next_id, data);
    }
    let (inner, schema) = gen_rel(u, depth - 1, next_id, data)?;
    let arity = schema.len();
    Ok(match u.int_in_range(0u8..=9)? {
        0 => {
            let n = u.int_in_range(1usize..=2)?;
            let preds = (0..n)
                .map(|_| gen_scalar(u, Ty::Bool, &schema, 2))
                .collect::<arbitrary::Result<Vec<_>>>()?;
            (inner.filter(preds), schema)
        }
        1 => {
            let ty = rand_ty(u)?;
            let e = gen_scalar(u, ty, &schema, 2)?;
            let mut s = schema.clone();
            s.push(ty);
            (inner.map(vec![e]), s)
        }
        2 => {
            let k = u.int_in_range(1usize..=arity)?;
            let mut outputs = Vec::with_capacity(k);
            for _ in 0..k {
                outputs.push(u.int_in_range(0..=arity - 1)?);
            }
            let s = outputs.iter().map(|&i| schema[i]).collect();
            (inner.project(outputs), s)
        }
        3 => (inner.negate(), schema),
        4 => (inner.distinct(), schema),
        5 => (inner.threshold(), schema),
        6 => {
            let other = if bool::arbitrary(u)? {
                inner.clone()
            } else {
                inner.clone().negate()
            };
            (inner.union(other), schema)
        }
        // Join 2-4 symbolic inputs with multiple equi-join equivalence classes
        // chaining inputs together (e.g. `in0.x = in1.x` and `in1.y = in2.y`).
        // With the `Get`s left symbolic, this is what exercises join
        // ordering/implementation selection, equality propagation across inputs,
        // and predicate/projection pushdown into each `Get`.
        7 => {
            let n_extra = u.int_in_range(1usize..=3)?;
            let mut inputs = vec![inner];
            let mut input_schemas = vec![schema.clone()];
            for _ in 0..n_extra {
                let (other, oschema) = gen_rel(u, depth - 1, next_id, data)?;
                input_schemas.push(oschema);
                inputs.push(other);
            }
            let mut variables: Vec<Vec<(usize, usize)>> = Vec::new();
            for r in 1..inputs.len() {
                for ty in [Ty::Int32, Ty::Int64, Ty::Bool] {
                    let rc = cols_of(&input_schemas[r], ty);
                    if rc.is_empty() || !bool::arbitrary(u)? {
                        continue;
                    }
                    let candidates: Vec<usize> = (0..r)
                        .filter(|&l| !cols_of(&input_schemas[l], ty).is_empty())
                        .collect();
                    if candidates.is_empty() {
                        continue;
                    }
                    let l = candidates[u.int_in_range(0..=candidates.len() - 1)?];
                    let lc = cols_of(&input_schemas[l], ty);
                    let li = lc[u.int_in_range(0..=lc.len() - 1)?];
                    let rj = rc[u.int_in_range(0..=rc.len() - 1)?];
                    variables.push(vec![(l, li), (r, rj)]);
                }
            }
            let mut s = schema.clone();
            for os in &input_schemas[1..] {
                s.extend(os.iter().copied());
            }
            (MirRelationExpr::join(inputs, variables), s)
        }
        8 => {
            let mut group_key = Vec::new();
            for c in 0..arity {
                if bool::arbitrary(u)? {
                    group_key.push(c);
                }
            }
            let n_agg = u.int_in_range(0usize..=2)?;
            let mut aggregates = Vec::with_capacity(n_agg);
            let mut out: Vec<Ty> = group_key.iter().map(|&k| schema[k]).collect();
            for _ in 0..n_agg {
                let (a, t) = gen_aggregate(u, &schema)?;
                aggregates.push(a);
                out.push(t);
            }
            if group_key.is_empty() && aggregates.is_empty() {
                aggregates.push(AggregateExpr {
                    func: AggregateFunc::Count,
                    expr: MirScalarExpr::column(0),
                    distinct: false,
                });
                out.push(Ty::Int64);
            }
            (inner.reduce(group_key, aggregates, None), out)
        }
        _ => {
            let mut group_key = Vec::new();
            for c in 0..arity {
                if u.ratio(1u8, 3u8)? {
                    group_key.push(c);
                }
            }
            // Total order (every column) so LIMIT/OFFSET is deterministic.
            let mut order_key = Vec::with_capacity(arity);
            for column in 0..arity {
                order_key.push(ColumnOrder {
                    column,
                    desc: bool::arbitrary(u)?,
                    nulls_last: bool::arbitrary(u)?,
                });
            }
            let limit = if bool::arbitrary(u)? {
                Some(MirScalarExpr::literal_ok(
                    Datum::Int64(u.int_in_range(0i64..=3)?),
                    ReprScalarType::Int64,
                ))
            } else {
                None
            };
            let offset = u.int_in_range(0usize..=2)?;
            (
                inner.top_k(group_key, order_key, limit, offset, None),
                schema,
            )
        }
    })
}

/// Replace every global `Get` we created with its bound constant collection.
/// Local `Get`s (introduced by the optimizer's `Let`s) are left for `collapse`.
fn substitute(mut rel: MirRelationExpr, data: &BTreeMap<u64, MirRelationExpr>) -> MirRelationExpr {
    rel.visit_pre_mut(|e| {
        let replacement = match e {
            MirRelationExpr::Get {
                id: Id::Global(GlobalId::User(uid)),
                ..
            } => data.get(&*uid).cloned(),
            _ => None,
        };
        if let Some(c) = replacement {
            *e = c;
        }
    });
    rel
}

/// Outcome of trying to fold a (`Get`-free) plan all the way to a `Constant`.
enum Collapse {
    /// Reduced to a `Constant` of `Ok` rows. The consolidated `(row, diff)`
    /// multiset is the actual result.
    Const(BTreeMap<Row, Diff>),
    /// Reached a fixpoint of `FoldConstants` + `NormalizeLets` (applying them no
    /// longer changes the plan) that is *not* a constant, e.g. the plan errors,
    /// or folding genuinely cannot evaluate it. This is a legitimate
    /// fold-limitation skip, not a coverage gap.
    StuckFixpoint,
    /// Hit the iteration budget without reaching either a constant or a
    /// fixpoint. The plan was still simplifying when we ran out of passes. Kept
    /// distinct from `StuckFixpoint` only to name the two skip reasons.
    /// `FoldConstants` does not promise a constant input collapses to a
    /// `Constant` within any limit, so this is a conservative skip too.
    BudgetExhausted,
}

/// Fold a (now `Get`-free) plan to a `Constant` by iterating `FoldConstants` +
/// `NormalizeLets` (to collapse any `Let`s the optimizer's CSE introduced) until
/// it either becomes a `Constant`, reaches a fixpoint, or exhausts the budget.
///
/// This loops to a genuine fixpoint (stops only when a pass leaves the plan
/// unchanged), so a plan that just needs a few more passes converges rather than
/// being dropped. The budget is a generous guard against a non-terminating
/// rewrite.
fn collapse(mut rel: MirRelationExpr) -> Collapse {
    let features = OptimizerFeatures::default();
    const BUDGET: usize = 64;
    for _ in 0..BUDGET {
        let before = rel.clone();
        let mut typ = rel.typ();
        if (FoldConstants { limit: None })
            .action(&mut rel, &mut typ)
            .is_err()
        {
            return Collapse::StuckFixpoint;
        }
        if rel.as_const().is_some() {
            break;
        }
        if NormalizeLets::new(true)
            .action(&mut rel, &features)
            .is_err()
        {
            return Collapse::StuckFixpoint;
        }
        // A full pass that changed nothing means we will never reach a constant.
        if rel == before {
            return Collapse::StuckFixpoint;
        }
    }
    let Some(constant) = rel.as_const() else {
        // Still simplifying when the budget ran out.
        return Collapse::BudgetExhausted;
    };
    let (Ok(rows), _) = constant else {
        return Collapse::StuckFixpoint;
    };
    let mut multiset: BTreeMap<Row, Diff> = BTreeMap::new();
    for (row, diff) in rows {
        *multiset.entry(row.clone()).or_insert(Diff::ZERO) += *diff;
    }
    multiset.retain(|_, d| *d != Diff::ZERO);
    Collapse::Const(multiset)
}

/// Run the full logical optimizer. `None` if it errors (e.g. `Typecheck`).
#[allow(deprecated)]
fn optimize(rel: MirRelationExpr) -> Option<MirRelationExpr> {
    let features = OptimizerFeatures::default();
    let typecheck_ctx = typecheck::empty_typechecking_context();
    let mut df_meta = DataflowMetainfo::default();
    let mut ctx = TransformCtx::local(
        &features,
        &typecheck_ctx,
        &mut df_meta,
        None,
        Some(GlobalId::Transient(1)),
    );
    let optimizer = Optimizer::logical_optimizer(&mut ctx);
    optimizer
        .optimize(rel, &mut ctx)
        .ok()
        .map(|o| o.into_inner())
}

fn run(u: &mut Unstructured) -> arbitrary::Result<()> {
    let mut next_id = 0u64;
    let mut data = BTreeMap::new();
    let (plan, _schema) = gen_rel(u, 3, &mut next_id, &mut data)?;

    // Ground truth: inline the data into the input plan and fold. Only proceed
    // when the *input* (which has no optimizer-introduced `Let`s) folds to a
    // constant, that is what gives us a result to compare against.
    let baseline = match collapse(substitute(plan.clone(), &data)) {
        Collapse::Const(b) => b,
        Collapse::StuckFixpoint | Collapse::BudgetExhausted => return Ok(()),
    };

    // Optimize with the Gets still symbolic, then inline the same data and fold.
    let Some(optimized) = optimize(plan.clone()) else {
        return Ok(());
    };
    match collapse(substitute(optimized, &data)) {
        Collapse::Const(after) => assert_eq!(
            baseline, after,
            "optimizer changed the result over symbolic inputs\nplan = {plan:?}\ndata = {data:?}"
        ),
        // The optimized plan did not fold to a constant: either a non-constant
        // fixpoint (an operator `FoldConstants` cannot evaluate) or still
        // simplifying when the 64-pass budget ran out. `FoldConstants` does not
        // promise a constant input reduces to a `Constant` within a limit, and
        // the optimizer legitimately reshapes plans (CSE into `Let` nesting)
        // into forms this two-pass loop may not drive to a fixpoint here. Both
        // are conservative skips, not divergences, matching `full_optimizer_equiv`.
        Collapse::StuckFixpoint | Collapse::BudgetExhausted => {}
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
