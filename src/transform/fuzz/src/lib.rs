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
use mz_transform::normalize_lets::NormalizeLets;
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
/// `leaf` must return **at least one column**. A zero-arity leaf makes the
/// `Project` arm's `int_in_range(1..=arity)` an empty range and underflows
/// `arity - 1`, and the `Reduce` fallback references `column(0)`. Worse than
/// either, `MirRelationExpr::join_scalars` drops an arity-0 single-row input
/// *after* `join` has computed the equivalences' global column offsets over the
/// full input list, so both the equivalences and the schema returned here would
/// silently point at the wrong columns.
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

/// The `OptimizerFeatures` every target here plans with: production's defaults.
///
/// NOTE: `OptimizerFeatures::default()` is all-`false`, which is *not* what any
/// deployment runs. Several transforms branch on these, and
/// `EquivalencePropagation` reads three of them, so planning with the derived
/// default exercises only the legacy paths. `enable_eq_classes_withholding_errors`
/// is the pointed one: it exists to stop equivalence propagation suppressing
/// errors, and `gen_scalar` deliberately seeds error literals, so the one feature
/// built for this generator's input class was the one turned off.
///
/// Only the flags whose production default is `true` are listed; the rest come
/// from `Default`. A newly added flag therefore arrives here as `false`, which is
/// wrong if it ships enabled, but the tripwire for that lives where it belongs:
/// `mz_sql`'s `optimizer_features_no_enable_for_item_parsing` destructures
/// `OptimizerFeatures` exhaustively, so a new field fails a fast unit test rather
/// than this crate's multi-minute sanitizer build. Source of truth for the values
/// is each flag's `default:` in `mz_sql::session::vars::definitions`.
pub fn fuzz_features() -> OptimizerFeatures {
    OptimizerFeatures {
        enable_new_outer_join_lowering: true,
        enable_reduce_mfp_fusion: true,
        enable_variadic_left_join_lowering: true,
        enable_letrec_fixpoint_analysis: true,
        enable_projection_pushdown_after_relation_cse: true,
        enable_less_reduce_in_eqprop: true,
        enable_dequadratic_eqprop_map: true,
        enable_eq_classes_withholding_errors: true,
        enable_cast_elimination: true,
        enable_simplify_quantified_comparisons: true,
        enable_simplify_from_less_existence: true,
        enable_coalesce_case_transform: true,
        enable_will_distinct_propagation: true,
        enable_fixed_correlated_cte_lowering: true,
        persist_fast_path_limit: 25,
        ..Default::default()
    }
}

/// Row cap for the oracles' constant folding.
///
/// `FoldConstants`' join arm materializes the full cross product *before*
/// applying equivalences, and `limit: None` disables its only size check, so an
/// unbounded fold is the harness asking for a `Vec<(Row, Diff)>` bounded only by
/// the product of every leaf's row count. Generated join trees nest, so that is
/// reachable in principle and would surface as an `oom-*`/`timeout-*` artifact
/// blamed on the optimizer.
///
/// Declining to fold is already a benign skip on both oracles, so a cap costs
/// nothing: it is well above the largest product measured over these generators
/// (~5e5 rows) while keeping peak allocation in the hundreds of MB against the
/// runner's `-rss_limit_mb=4096`.
pub const FOLD_ROW_LIMIT: usize = 1_000_000;

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
    let features = fuzz_features();
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
///
/// `None` covers two benign cases: the plan did not fold all the way down to a
/// `Constant`, and it folded to an `EvalError`. The latter is not a blind spot
/// but a required tolerance. The optimizer is knowingly imprecise about error
/// semantics, `predicate_pushdown` will push a predicate that can error into a
/// join input and manufacture an error the unoptimized plan never raises (see
/// the comment there and database-issues#6258), so one side folding to an error
/// while the other yields rows is accepted behaviour and would otherwise fire
/// roughly once in every 1,500 executions.
///
/// A `TransformError` is a different matter and panics, see [`optimize`].
pub fn fold_to_multiset(mut rel: MirRelationExpr) -> Option<BTreeMap<Row, Diff>> {
    if let Err(e) = apply_recursively(
        FoldConstants {
            limit: Some(FOLD_ROW_LIMIT),
        },
        &mut rel,
    ) {
        panic!("FoldConstants returned an error: {e}");
    }
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

/// True if `rel` has an erroring operand under a non-strict `AND`/`OR`/
/// `error_if_null`, the shape of the open bug CLU-137.
///
/// Those three swallow an operand's error once another operand fixes the result:
/// `Or::eval` returns `true` the moment it sees a true operand and drops any
/// error it collected, `And::eval` does the same for `false`, and
/// `error_if_null` evaluates its message operand only when the first operand is
/// NULL. `reduce`'s generic variadic fold nonetheless replaces the whole call
/// with an operand's literal error, and `undistribute_and_or` can recombine an
/// erroring operand across the short-circuit boundary. Either way the optimizer
/// can turn a row the plan should emit into an error, and, once the folded
/// literal is typed non-nullable, into a *different* row: that is how the count
/// of a nullable aggregate becomes the count of a non-nullable one.
///
/// CLU-137 tracks the fix (see the closed PR #37299 for a full one). Until it
/// lands, the equivalence oracles skip these plans rather than rediscover it on
/// every run.
///
/// Deliberately conservative. It asks whether an operand *could* error rather
/// than whether it already holds a literal error, because `reduce` folds a
/// column-free fallible operand (`9223372036854775807 + 1`, `1 / 0`) to a
/// literal error first and absorbs it after. The cost is that a plan whose
/// AND/OR operands merely *might* error is skipped even where the fold could not
/// have fired: measured at 3.6% of `gen_rel(depth = 4)` plans.
pub fn hits_non_strict_error_fold(rel: &MirRelationExpr) -> bool {
    let mut hit = false;
    rel.visit_scalars(&mut |scalar| {
        // One definition of the shape, in `mz_expr` next to the fold it describes,
        // so the several oracles that skip CLU-137 cannot drift apart on which
        // functions count as non-strict. They already had: this predicate covered
        // `ErrorIfNull` while `mir_scalar_reduce`'s copy did not.
        hit |= scalar.could_hit_nonstrict_error_fold();
    });
    hit
}

/// Run the full logical optimizer.
///
/// NOTE: a `TransformError` from here is itself a finding, so this panics rather
/// than reporting one. The tempting reading, that a plan shape the optimizer
/// rejects comes back as an error to skip, is wrong in both directions.
/// `Typecheck` returns `Ok(())` on every path and routes a type error through
/// `type_error!(true, ..)` -> `soft_panic_or_log!`, and `Fixpoint`
/// non-convergence does the same. Soft assertions default to
/// `cfg!(debug_assertions)`, which cargo-fuzz enables, so a rejected plan is
/// already a crash before it can reach us. What is left that can error are
/// optimizer invariant violations: a `Let`/`Get` on an unbound local id, a `Let`
/// whose type changed under it, an ANF rebinding that lost an identifier. Those
/// surface to users as `internal error`, so swallowing them would leave this
/// harness green through exactly the `normalize_lets`/`cse` regressions it is
/// best placed to catch.
#[allow(deprecated)]
pub fn optimize(rel: MirRelationExpr) -> MirRelationExpr {
    let features = fuzz_features();
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
    match optimizer.optimize(rel, &mut ctx) {
        Ok(optimized) => optimized.into_inner(),
        Err(e) => panic!("logical optimizer returned an error: {e}"),
    }
}

/// Outcome of trying to fold a (`Get`-free) plan all the way to a `Constant`.
pub enum Collapse {
    /// Reduced to a `Constant` of `Ok` rows. The consolidated `(row, diff)`
    /// multiset is the actual result.
    Const(BTreeMap<Row, Diff>),
    /// Either folding reached a fixpoint that is not a constant (a legitimate
    /// fold limitation), or it folded all the way to an `Err` constant.
    ///
    /// NOTE: those two are not the same thing, and the second is not a fold
    /// limitation at all: an `EvalError` constant is a fully determined result.
    /// They share a variant because neither yields a `(row, diff)` multiset to
    /// compare. Splitting them would let an oracle notice a baseline of
    /// `Ok(rows)` becoming an `Err` after optimization, but only in that
    /// direction: `Err -> Ok` is legitimate, since `Demand` can drop an unused
    /// erroring `Map` column. Even the `Ok -> Err` direction is not assertable
    /// today, because `predicate_pushdown` knowingly manufactures errors the
    /// unoptimized plan never raises (database-issues#6258).
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
pub fn collapse(mut rel: MirRelationExpr) -> Collapse {
    let features = fuzz_features();
    const BUDGET: usize = 64;
    for _ in 0..BUDGET {
        let before = rel.clone();
        // A `TransformError` here is an optimizer invariant violation, not a
        // reason to give up on the plan. See `mz_transform_fuzz::optimize`.
        if let Err(e) = apply_recursively(
            FoldConstants {
                limit: Some(FOLD_ROW_LIMIT),
            },
            &mut rel,
        ) {
            panic!("FoldConstants returned an error: {e}");
        }
        if rel.as_const().is_some() {
            break;
        }
        if let Err(e) = NormalizeLets::new(true).action(&mut rel, &features) {
            panic!("NormalizeLets returned an error: {e}");
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
