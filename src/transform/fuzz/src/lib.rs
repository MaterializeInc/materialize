// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared generators and oracle helpers for the `mz-transform` fuzz targets.
//!
//! The three targets (`mir_relation_transforms`, `full_optimizer_equiv`,
//! `optimizer_symbolic_equiv`) all build random, well-typed `MirRelationExpr`
//! plans over an `int4`/`int8`/`bool` schema and check result preservation with
//! a `FoldConstants`-based oracle. They differ only in what they root the plan
//! at (literal `Constant`s vs opaque `Get`s) and which transforms they drive.
//! Everything they have in common lives here:
//!
//!  * [`gen_scalar`] builds well-typed scalar expressions (arithmetic per width,
//!    the boolean connectives, `Eq`, `If`, and the int/bool casts).
//!  * [`gen_constant`] builds a random literal `Constant` collection.
//!  * [`gen_rel`] builds a random relation over the bug-rich relational
//!    operators (`Join`, `Reduce`, `TopK`, `Threshold`, `Union`, and the
//!    map/filter/project/negate/distinct set), parameterized by a `leaf` closure
//!    so a caller can root it at `Constant`s or symbolic `Get`s.
//!  * [`apply_recursively`], [`fold_to_multiset`], and [`optimize`] are the
//!    oracle machinery.

use std::collections::BTreeMap;

use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use mz_expr::{
    AggregateExpr, AggregateFunc, ColumnOrder, EvalError, MirRelationExpr, MirScalarExpr, func,
};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{Datum, Diff, GlobalId, ReprColumnType, ReprRelationType, ReprScalarType, Row};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::fold_constants::FoldConstants;
use mz_transform::{Optimizer, Transform, TransformCtx, TransformError, typecheck};

/// The scalar types the fuzz targets generate over.
#[derive(Clone, Copy, PartialEq)]
pub enum Ty {
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

/// Pick a random column type.
pub fn rand_ty(u: &mut Unstructured) -> arbitrary::Result<Ty> {
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

/// A well-typed scalar expression of type `ty` over a relation with column types
/// `schema`. Column references only target columns of the requested type, and
/// every leaf may also be a literal (constant, null, or a poison error to
/// exercise error-propagation paths). Includes `Add`/`Sub`/`Mul`/`Mod` per
/// integer width, the boolean connectives, `Eq` across a random type, `If`, and
/// the `int4`<->`int8`/`int4`<->`bool` casts so neither integer width is a leaf.
pub fn gen_scalar(
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
    // An `If`/`then`/`else` of the requested type, available for every `ty`.
    let gen_if = |u: &mut Unstructured| -> arbitrary::Result<MirScalarExpr> {
        let c = gen_scalar(u, Ty::Bool, schema, d)?;
        let t = gen_scalar(u, ty, schema, d)?;
        let f = gen_scalar(u, ty, schema, d)?;
        Ok(c.if_then_else(t, f))
    };
    Ok(match ty {
        Ty::Int32 => match u.int_in_range(0u8..=5)? {
            0 => gen_scalar(u, Ty::Int32, schema, d)?
                .call_binary(gen_scalar(u, Ty::Int32, schema, d)?, func::AddInt32),
            1 => gen_scalar(u, Ty::Int32, schema, d)?
                .call_binary(gen_scalar(u, Ty::Int32, schema, d)?, func::SubInt32),
            2 => gen_scalar(u, Ty::Int32, schema, d)?
                .call_binary(gen_scalar(u, Ty::Int32, schema, d)?, func::MulInt32),
            3 => gen_scalar(u, Ty::Int32, schema, d)?
                .call_binary(gen_scalar(u, Ty::Int32, schema, d)?, func::ModInt32),
            // Narrowing cast from int8 (may error on overflow, folds to an error).
            4 => gen_scalar(u, Ty::Int64, schema, d)?.call_unary(func::CastInt64ToInt32),
            _ => gen_if(u)?,
        },
        Ty::Int64 => match u.int_in_range(0u8..=5)? {
            0 => gen_scalar(u, Ty::Int64, schema, d)?
                .call_binary(gen_scalar(u, Ty::Int64, schema, d)?, func::AddInt64),
            1 => gen_scalar(u, Ty::Int64, schema, d)?
                .call_binary(gen_scalar(u, Ty::Int64, schema, d)?, func::SubInt64),
            2 => gen_scalar(u, Ty::Int64, schema, d)?
                .call_binary(gen_scalar(u, Ty::Int64, schema, d)?, func::MulInt64),
            3 => gen_scalar(u, Ty::Int64, schema, d)?
                .call_binary(gen_scalar(u, Ty::Int64, schema, d)?, func::ModInt64),
            // Widening cast from int4.
            4 => gen_scalar(u, Ty::Int32, schema, d)?.call_unary(func::CastInt32ToInt64),
            _ => gen_if(u)?,
        },
        Ty::Bool => match u.int_in_range(0u8..=5)? {
            0 => gen_scalar(u, Ty::Bool, schema, d)?.and(gen_scalar(u, Ty::Bool, schema, d)?),
            1 => gen_scalar(u, Ty::Bool, schema, d)?.or(gen_scalar(u, Ty::Bool, schema, d)?),
            2 => gen_scalar(u, Ty::Bool, schema, d)?.not(),
            3 => {
                let t = rand_ty(u)?;
                let a = gen_scalar(u, t, schema, d)?;
                let b = gen_scalar(u, t, schema, d)?;
                a.call_binary(b, func::Eq)
            }
            // Cast int4 -> bool (nonzero is true).
            4 => gen_scalar(u, Ty::Int32, schema, d)?.call_unary(func::CastInt32ToBool),
            _ => gen_if(u)?,
        },
    })
}

/// A random literal `Constant` collection (1-3 columns, 0-4 rows), returned with
/// its column schema. All columns are declared nullable.
pub fn gen_constant(u: &mut Unstructured) -> arbitrary::Result<(MirRelationExpr, Vec<Ty>)> {
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
/// reduction sees `max(a + b)`, `sum(if p then x else y)`, etc., exercising
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
    // A computed input of the required type. The aggregate `expr` can be any
    // well-typed scalar, not just a column. Depth keeps it bounded.
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

/// Generate a random relation, returning it, its column schema, and whether it
/// is guaranteed to have non-negative multiplicities.
///
/// `leaf` produces the base relations the plan is rooted at. The constant-rooted
/// targets return a literal `Constant`; the symbolic target returns an opaque
/// `Get` (and records its backing data on the side). Either way `leaf` returns a
/// relation and its column schema; leaves are assumed non-negative.
///
/// The non-negativity flag is the contract `TopK` (and every dataflow reduction)
/// requires of its input, so we only place a `TopK` directly over a non-negative
/// subtree. See the `TopK` arm.
pub fn gen_rel<F>(
    u: &mut Unstructured,
    depth: u32,
    leaf: &mut F,
) -> arbitrary::Result<(MirRelationExpr, Vec<Ty>, bool)>
where
    F: FnMut(&mut Unstructured) -> arbitrary::Result<(MirRelationExpr, Vec<Ty>)>,
{
    if depth == 0 || u.ratio(2u8, 5u8)? {
        let (rel, schema) = leaf(u)?;
        return Ok((rel, schema, true));
    }
    let (inner, schema, inner_nn) = gen_rel(u, depth - 1, leaf)?;
    let arity = schema.len();
    Ok(match u.int_in_range(0u8..=9)? {
        // Filter
        0 => {
            let n = u.int_in_range(1usize..=2)?;
            let preds = (0..n)
                .map(|_| gen_scalar(u, Ty::Bool, &schema, 2))
                .collect::<arbitrary::Result<Vec<_>>>()?;
            (inner.filter(preds), schema, inner_nn)
        }
        // Map one column
        1 => {
            let ty = rand_ty(u)?;
            let e = gen_scalar(u, ty, &schema, 2)?;
            let mut s = schema.clone();
            s.push(ty);
            (inner.map(vec![e]), s, inner_nn)
        }
        // Project a (reordered/duplicated) subset
        2 => {
            let k = u.int_in_range(1usize..=arity)?;
            let mut outputs = Vec::with_capacity(k);
            for _ in 0..k {
                outputs.push(u.int_in_range(0..=arity - 1)?);
            }
            let s = outputs.iter().map(|&i| schema[i]).collect();
            (inner.project(outputs), s, inner_nn)
        }
        3 => (inner.negate(), schema, false),
        4 => (inner.distinct(), schema, true),
        5 => (inner.threshold(), schema, true),
        // Union with a same-schema relation (self, or self negated).
        6 => {
            let (other, union_nn) = if bool::arbitrary(u)? {
                // `inner + inner`: non-negative exactly when `inner` is.
                (inner.clone(), inner_nn)
            } else {
                // `inner + (-inner)` cancels to an empty (hence non-negative)
                // collection regardless of `inner`'s sign.
                (inner.clone().negate(), true)
            };
            (inner.union(other), schema, union_nn)
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
            // A join's multiplicities are the product of its inputs', so the
            // result is non-negative exactly when every input is.
            let mut join_nn = inner_nn;
            for _ in 0..n_extra {
                let (other, oschema, other_nn) = gen_rel(u, depth - 1, leaf)?;
                join_nn &= other_nn;
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
            (MirRelationExpr::join(inputs, variables), s, join_nn)
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
            (inner.reduce(group_key, aggregates, None), out, true)
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
            // `TopK`, like every dataflow reduction, is only defined over
            // non-negative collections. When `inner` can be net-negative, wrap
            // it in a `Threshold` to drop the negative-diff rows. Without this a
            // no-op `TopK` over a `Negate` diverges: the unguarded `TopKElision`
            // removes it in the optimized plan, exposing negatives that the input
            // plan's `fold_topk_constant` had zeroed. `Threshold` folds to a real
            // non-negative constant (a `Reduce`/`distinct` would instead error on
            // negatives), and its `ThresholdElision` is guarded by the
            // `NonNegative` analysis, so it is not elided over this
            // not-provably-non-negative input. Both fold paths thus agree.
            let input = if inner_nn { inner } else { inner.threshold() };
            (
                input.top_k(group_key, order_key, limit, offset, None),
                schema,
                true,
            )
        }
    })
}

/// Apply `transform` over the whole plan through its recursive driver
/// (`Transform::transform` -> `actually_perform_transform`), not `action`.
///
/// NOTE: Some transforms' `action` (e.g. `FoldConstants`, `UnionBranchCancellation`)
/// only rewrites the single node it is handed, expecting its caller to have
/// already handled the children. Calling `action` on the plan root therefore
/// folds/cancels nothing below the root, which would leave the
/// result-equivalence oracle inert on every plan deeper than one operator.
pub fn apply_recursively<T: Transform>(
    transform: T,
    rel: &mut MirRelationExpr,
) -> Result<(), TransformError> {
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
    transform.transform(rel, &mut ctx)
}

/// Fold `rel`. If it reduces to a `Constant` of `Ok` rows, return the
/// consolidated `(row, diff)` multiset, otherwise `None`.
pub fn fold_to_multiset(mut rel: MirRelationExpr) -> Option<BTreeMap<Row, Diff>> {
    apply_recursively(FoldConstants { limit: None }, &mut rel).ok()?;
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
/// `Typecheck` pass rejects the plan). Only a panic is a finding here.
#[allow(deprecated)]
pub fn optimize(rel: MirRelationExpr) -> Option<MirRelationExpr> {
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
