// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: the *entire* logical optimizer must preserve a relation's
//! result. Where `mir_relation_transforms` checks individual transforms in
//! isolation, this target runs the full `Optimizer::logical_optimizer` pipeline
//! — every transform, in the real order, with all their interactions — and
//! checks that the optimized plan produces the same rows as the input.
//!
//! We build a random, well-typed plan rooted at `Constant` collections over an
//! `int4`/`int8`/`bool` schema, using the bug-rich relational operators the
//! per-transform target omits: `Join` (over 2-4 inputs, with multiple equi-join
//! equivalences chaining several inputs together), `Reduce` (group keys +
//! min/max/sum/any/all/count aggregates over *computed* inputs, not just bare
//! column refs), `TopK`, `Threshold`, and
//! `Union`/`Negate`/`Distinct`/`Map`/`Filter`/`Project`. Because every leaf is
//! constant and `FoldConstants` evaluates all of these operators, both the input
//! and the optimized output fold to actual result rows.
//!
//! The multi-input joins with several equivalence classes (e.g. `a.x = b.x` and
//! `b.y = c.y`) are what drive the join-ordering/implementation planner, equality
//! propagation, and predicate pushdown through `Get`s — the parts of the optimizer
//! a 2-way, at-most-one-equivalence join barely touches. Computed aggregate inputs
//! likewise exercise aggregate-expression simplification and the reduction MFP.
//!
//! Oracle: fold the input to its `(row, diff)` multiset; run the optimizer; fold
//! the result. When both fold to a constant, the multisets must be equal — a
//! divergence is a miscompile. The comparison is conservative (we only assert
//! when both sides fold, and skip when the optimizer returns an error, e.g. the
//! `Typecheck` pass rejecting a plan shape), so a surviving assertion failure or
//! a panic inside the optimizer is a genuine finding.

#![no_main]

use std::collections::BTreeMap;

use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::{
    AggregateExpr, AggregateFunc, ColumnOrder, EvalError, MirRelationExpr, MirScalarExpr, func,
};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{Datum, Diff, GlobalId, ReprColumnType, ReprRelationType, ReprScalarType, Row};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::fold_constants::FoldConstants;
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

fn gen_constant(u: &mut Unstructured) -> arbitrary::Result<(MirRelationExpr, Vec<Ty>)> {
    let ncols = u.int_in_range(1usize..=3)?;
    let schema: Vec<Ty> = (0..ncols)
        .map(|_| rand_ty(u))
        .collect::<arbitrary::Result<_>>()?;
    let col_types: Vec<ReprColumnType> = schema
        .iter()
        .map(|t| scalar_ty(*t).nullable(true))
        .collect();
    let nrows = u.int_in_range(0usize..=4)?;
    let mut rows = Vec::with_capacity(nrows);
    for _ in 0..nrows {
        let mut row = Vec::with_capacity(ncols);
        for t in &schema {
            row.push(gen_datum(u, *t)?);
        }
        rows.push(row);
    }
    Ok((
        MirRelationExpr::constant(rows, ReprRelationType::new(col_types)),
        schema,
    ))
}

/// One aggregate over `schema`, plus the scalar type of its output column.
///
/// The aggregated input is a freshly generated scalar expression of the
/// function's required input type (not just a bare column reference), so the
/// reduction sees `max(a + b)`, `sum(if p then x else y)`, etc. — exercising
/// aggregate-input simplification and the reduce MFP.
fn gen_aggregate(u: &mut Unstructured, schema: &[Ty]) -> arbitrary::Result<(AggregateExpr, Ty)> {
    // (func, required input type, output type).
    let opts: &[(AggregateFunc, Ty, Ty)] = &[
        (AggregateFunc::MaxInt32, Ty::Int32, Ty::Int32),
        (AggregateFunc::MinInt32, Ty::Int32, Ty::Int32),
        (AggregateFunc::SumInt32, Ty::Int32, Ty::Int64),
        (AggregateFunc::MaxInt64, Ty::Int64, Ty::Int64),
        (AggregateFunc::MinInt64, Ty::Int64, Ty::Int64),
        (AggregateFunc::Any, Ty::Bool, Ty::Bool),
        (AggregateFunc::All, Ty::Bool, Ty::Bool),
        (AggregateFunc::Count, Ty::Int32, Ty::Int64),
    ];
    let idx = u.int_in_range(0..=opts.len() - 1)?;
    let (func, in_ty, out) = opts[idx].clone();
    // A computed input of the required type. Since the input may be any type, the
    // aggregate `expr` no longer has to be a column — depth keeps it bounded.
    let expr = gen_scalar(u, in_ty, schema, 2)?;
    Ok((
        AggregateExpr {
            func,
            expr,
            distinct: bool::arbitrary(u)?,
        },
        out,
    ))
}

fn gen_rel(u: &mut Unstructured, depth: u32) -> arbitrary::Result<(MirRelationExpr, Vec<Ty>)> {
    if depth == 0 || u.ratio(2u8, 5u8)? {
        return gen_constant(u);
    }
    let (inner, schema) = gen_rel(u, depth - 1)?;
    let arity = schema.len();
    Ok(match u.int_in_range(0u8..=9)? {
        // Filter
        0 => {
            let n = u.int_in_range(1usize..=2)?;
            let preds = (0..n)
                .map(|_| gen_scalar(u, Ty::Bool, &schema, 2))
                .collect::<arbitrary::Result<Vec<_>>>()?;
            (inner.filter(preds), schema)
        }
        // Map one column
        1 => {
            let ty = rand_ty(u)?;
            let e = gen_scalar(u, ty, &schema, 2)?;
            let mut s = schema.clone();
            s.push(ty);
            (inner.map(vec![e]), s)
        }
        // Project a (reordered/duplicated) subset
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
        // Union with a same-schema relation (self, or self negated).
        6 => {
            let other = if bool::arbitrary(u)? {
                inner.clone()
            } else {
                inner.clone().negate()
            };
            (inner.union(other), schema)
        }
        // Join 2-4 relations with multiple equi-join equivalence classes that
        // chain inputs together (e.g. `in0.x = in1.x` and `in1.y = in2.y`). This
        // is what makes join ordering/implementation planning and equality
        // propagation actually run, unlike a 2-way single-equivalence join.
        7 => {
            let n_extra = u.int_in_range(1usize..=3)?;
            let mut inputs = vec![inner];
            // Per-input absolute schema, used only to find type-matching join cols.
            let mut input_schemas = vec![schema.clone()];
            for _ in 0..n_extra {
                let (other, oschema) = gen_rel(u, depth - 1)?;
                input_schemas.push(oschema);
                inputs.push(other);
            }
            // For each newly added input `r`, try to add one equivalence per type
            // linking it to some earlier input `l < r` with a column of that type.
            let mut variables: Vec<Vec<(usize, usize)>> = Vec::new();
            for r in 1..inputs.len() {
                for ty in [Ty::Int32, Ty::Int64, Ty::Bool] {
                    let rc = cols_of(&input_schemas[r], ty);
                    if rc.is_empty() || !bool::arbitrary(u)? {
                        continue;
                    }
                    // Pick an earlier input that also has a column of this type.
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
        // Reduce: a distinct subset group key plus 0..=2 aggregates.
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
        // TopK over the input.
        _ => {
            let mut group_key = Vec::new();
            for c in 0..arity {
                if u.ratio(1u8, 3u8)? {
                    group_key.push(c);
                }
            }
            // Order by *every* column (in a random direction each) so the order
            // is total: distinct rows never tie, hence which rows a LIMIT/OFFSET
            // keeps is unambiguous and the result multiset is deterministic. (A
            // partial order would let the optimizer legitimately keep different
            // tied rows, a spurious divergence rather than a bug.)
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

/// Fold `rel`. If it reduces to a `Constant` of `Ok` rows, return the
/// consolidated `(row, diff)` multiset; otherwise `None`.
fn fold_to_multiset(mut rel: MirRelationExpr) -> Option<BTreeMap<Row, Diff>> {
    let mut typ = rel.typ();
    (FoldConstants { limit: None })
        .action(&mut rel, &mut typ)
        .ok()?;
    let (Ok(rows), _) = rel.as_const()? else {
        return None;
    };
    let mut multiset: BTreeMap<Row, Diff> = BTreeMap::new();
    for (row, diff) in rows {
        *multiset.entry(row.clone()).or_insert(Diff::ZERO) += *diff;
    }
    multiset.retain(|_, d| *d != Diff::ZERO);
    Some(multiset)
}

/// Run the full logical optimizer. Returns `None` if it errors (e.g. the
/// `Typecheck` pass rejects the plan) — only a panic is a finding here.
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
    let (rel, _schema) = gen_rel(u, 4)?;

    // The input must fold to actual rows for there to be anything to compare.
    let Some(baseline) = fold_to_multiset(rel.clone()) else {
        return Ok(());
    };

    let Some(optimized) = optimize(rel.clone()) else {
        return Ok(());
    };

    // The optimizer is semantics-preserving: the optimized plan must fold to the
    // same multiset. We only assert when the optimized plan also folds (it should,
    // since all leaves are constant), staying conservative about fold limitations.
    if let Some(after) = fold_to_multiset(optimized) {
        assert_eq!(
            baseline, after,
            "the optimizer changed the result multiset\n{rel:?}"
        );
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
