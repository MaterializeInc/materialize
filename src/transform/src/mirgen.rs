// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Random well-typed `MirRelationExpr` generation and the `FoldConstants`
//! result-equivalence oracle.
//!
//! Two consumers share this module, and they test different halves of the
//! pipeline over the same generated plans:
//!
//!  * The `mz-transform` fuzz targets check MIR-to-MIR equivalence: generate a
//!    plan, run transforms, assert the result multiset is unchanged. They never
//!    execute anything.
//!  * The compute surface suite checks MIR-to-dataflow equivalence: generate a
//!    plan, render it on a real replica, assert the rendered output matches
//!    [`fold_to_multiset`] of the same plan.
//!
//! # Entropy
//!
//! Generation draws from an [`Entropy`] source rather than a concrete RNG, so a
//! fuzz target can drive it from libFuzzer's coverage-guided byte string while
//! the corpus generator drives it from a seeded PRNG.
//!
//! An [`Entropy`] implementation wrapping libFuzzer's `Unstructured` must
//! delegate each method one-to-one, in call order. The generators' byte
//! consumption is what maps a corpus entry to a plan, and the accumulated fuzz
//! corpus is carried between release-qualification runs. Changing how many bytes
//! a generator draws silently remaps every stored entry to a different plan and
//! discards that accumulated coverage.

use std::collections::BTreeMap;
use std::ops::RangeInclusive;

use mz_expr::{
    AggregateExpr, AggregateFunc, ColumnOrder, EvalError, MirRelationExpr, MirScalarExpr, func,
};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{Datum, Diff, GlobalId, ReprRelationType, ReprScalarType, Row};

use crate::dataflow::DataflowMetainfo;
use crate::fold_constants::FoldConstants;
use crate::{Optimizer, Transform, TransformCtx, TransformError, typecheck};

/// A source of choices for the generators.
///
/// The methods mirror exactly the operations the generators need, so an
/// implementation over libFuzzer's `Unstructured` is a one-to-one delegation and
/// preserves byte consumption (see the module docs).
pub trait Entropy {
    /// Why a draw failed. A byte-backed source runs out of input; a PRNG-backed
    /// source cannot fail and uses [`std::convert::Infallible`].
    type Error;

    /// A `u8` in `range`, inclusive.
    fn int_in_range_u8(&mut self, range: RangeInclusive<u8>) -> Result<u8, Self::Error>;

    /// A `usize` in `range`, inclusive.
    fn int_in_range_usize(&mut self, range: RangeInclusive<usize>) -> Result<usize, Self::Error>;

    /// An `i64` in `range`, inclusive.
    fn int_in_range_i64(&mut self, range: RangeInclusive<i64>) -> Result<i64, Self::Error>;

    /// True with probability `numerator / denominator`.
    fn ratio(&mut self, numerator: u8, denominator: u8) -> Result<bool, Self::Error>;

    /// An arbitrary `bool`.
    fn any_bool(&mut self) -> Result<bool, Self::Error>;

    /// An arbitrary `i32`, spanning the full range.
    fn any_i32(&mut self) -> Result<i32, Self::Error>;

    /// An arbitrary `i64`, spanning the full range.
    fn any_i64(&mut self) -> Result<i64, Self::Error>;
}

/// The result of a generator draw.
pub type Gen<T, E> = Result<T, <E as Entropy>::Error>;

/// The scalar types the generators produce.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Ty {
    /// `int4`.
    Int32,
    /// `int8`.
    Int64,
    /// `boolean`.
    Bool,
}

impl Ty {
    /// The `ReprScalarType` this generator type denotes.
    pub fn scalar_type(self) -> ReprScalarType {
        match self {
            Ty::Int32 => ReprScalarType::Int32,
            Ty::Int64 => ReprScalarType::Int64,
            Ty::Bool => ReprScalarType::Bool,
        }
    }
}

/// Pick a random column type.
pub fn rand_ty<E: Entropy>(u: &mut E) -> Gen<Ty, E> {
    Ok(match u.int_in_range_u8(0..=2)? {
        0 => Ty::Int32,
        1 => Ty::Int64,
        _ => Ty::Bool,
    })
}

/// A random datum of type `ty`, null one time in five.
pub fn gen_datum<E: Entropy>(u: &mut E, ty: Ty) -> Gen<Datum<'static>, E> {
    if u.ratio(1, 5)? {
        return Ok(Datum::Null);
    }
    Ok(match ty {
        Ty::Int32 => Datum::Int32(u.any_i32()?),
        Ty::Int64 => Datum::Int64(u.any_i64()?),
        Ty::Bool => {
            if u.any_bool()? {
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
pub fn gen_scalar<E: Entropy>(
    u: &mut E,
    ty: Ty,
    schema: &[Ty],
    depth: u32,
) -> Gen<MirScalarExpr, E> {
    let st = ty.scalar_type();
    if depth == 0 || u.ratio(1, 2)? {
        let cols = cols_of(schema, ty);
        if !cols.is_empty() && u.any_bool()? {
            let idx = u.int_in_range_usize(0..=cols.len() - 1)?;
            return Ok(MirScalarExpr::column(cols[idx]));
        }
        return Ok(match u.int_in_range_u8(0..=2)? {
            0 => MirScalarExpr::literal_ok(gen_datum(u, ty)?, st),
            1 => MirScalarExpr::literal_null(st),
            _ => MirScalarExpr::literal(Err(EvalError::DivisionByZero), st),
        });
    }
    let d = depth - 1;
    Ok(match ty {
        Ty::Int32 => match u.int_in_range_u8(0..=5)? {
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
            _ => gen_if(u, ty, schema, d)?,
        },
        Ty::Int64 => match u.int_in_range_u8(0..=5)? {
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
            _ => gen_if(u, ty, schema, d)?,
        },
        Ty::Bool => match u.int_in_range_u8(0..=5)? {
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
            _ => gen_if(u, ty, schema, d)?,
        },
    })
}

/// An `If`/`then`/`else` of type `ty`, available for every `ty`.
fn gen_if<E: Entropy>(u: &mut E, ty: Ty, schema: &[Ty], d: u32) -> Gen<MirScalarExpr, E> {
    let c = gen_scalar(u, Ty::Bool, schema, d)?;
    let t = gen_scalar(u, ty, schema, d)?;
    let f = gen_scalar(u, ty, schema, d)?;
    Ok(c.if_then_else(t, f))
}

/// A random schema of 1-3 columns.
pub fn gen_schema<E: Entropy>(u: &mut E) -> Gen<Vec<Ty>, E> {
    let ncols = u.int_in_range_usize(1..=3)?;
    (0..ncols).map(|_| rand_ty(u)).collect()
}

/// Random rows (0-4 of them) matching `schema`, as unpacked datums.
pub fn gen_rows<E: Entropy>(u: &mut E, schema: &[Ty]) -> Gen<Vec<Vec<Datum<'static>>>, E> {
    let nrows = u.int_in_range_usize(0..=4)?;
    let mut rows = Vec::with_capacity(nrows);
    for _ in 0..nrows {
        let mut row = Vec::with_capacity(schema.len());
        for t in schema {
            row.push(gen_datum(u, *t)?);
        }
        rows.push(row);
    }
    Ok(rows)
}

/// The relation type of `schema`, with every column declared nullable.
pub fn nullable_relation_type(schema: &[Ty]) -> ReprRelationType {
    ReprRelationType::new(
        schema
            .iter()
            .map(|t| t.scalar_type().nullable(true))
            .collect(),
    )
}

/// A random literal `Constant` collection (1-3 columns, 0-4 rows), returned with
/// its column schema. All columns are declared nullable.
pub fn gen_constant<E: Entropy>(u: &mut E) -> Gen<(MirRelationExpr, Vec<Ty>), E> {
    let schema = gen_schema(u)?;
    let rows = gen_rows(u, &schema)?;
    Ok((
        MirRelationExpr::constant(rows, nullable_relation_type(&schema)),
        schema,
    ))
}

/// One aggregate over `schema`, plus the scalar type of its output column.
///
/// The aggregated input is a freshly generated scalar expression of the
/// function's required input type (not just a bare column reference), so the
/// reduction sees `max(a + b)`, `sum(if p then x else y)`, etc., exercising
/// aggregate-input simplification and the reduce MFP.
pub fn gen_aggregate<E: Entropy>(u: &mut E, schema: &[Ty]) -> Gen<(AggregateExpr, Ty), E> {
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
    let idx = u.int_in_range_usize(0..=opts.len() - 1)?;
    let (func, in_ty, out) = opts[idx].clone();
    // A computed input of the required type. The aggregate `expr` can be any
    // well-typed scalar, not just a column. Depth keeps it bounded.
    let expr = gen_scalar(u, in_ty, schema, 2)?;
    Ok((
        AggregateExpr {
            func,
            expr,
            distinct: u.any_bool()?,
        },
        out,
    ))
}

/// Generate a random relation, returning it, its column schema, and whether it
/// is guaranteed to have non-negative multiplicities.
///
/// `leaf` produces the base relations the plan is rooted at. The constant-rooted
/// fuzz targets return a literal `Constant`; the symbolic target returns an opaque
/// `Get` (and records its backing data on the side); the compute surface suite
/// returns a `Get` of a persist-backed source import. Either way `leaf` returns a
/// relation and its column schema; leaves are assumed non-negative.
///
/// The non-negativity flag is the contract `TopK` (and every dataflow reduction)
/// requires of its input, so we only place a `TopK` directly over a non-negative
/// subtree. See the `TopK` arm.
pub fn gen_rel<E: Entropy, F>(
    u: &mut E,
    depth: u32,
    leaf: &mut F,
) -> Gen<(MirRelationExpr, Vec<Ty>, bool), E>
where
    F: FnMut(&mut E) -> Gen<(MirRelationExpr, Vec<Ty>), E>,
{
    if depth == 0 || u.ratio(2, 5)? {
        let (rel, schema) = leaf(u)?;
        return Ok((rel, schema, true));
    }
    let (inner, schema, inner_nn) = gen_rel(u, depth - 1, leaf)?;
    let arity = schema.len();
    Ok(match u.int_in_range_u8(0..=9)? {
        // Filter
        0 => {
            let n = u.int_in_range_usize(1..=2)?;
            let preds = (0..n)
                .map(|_| gen_scalar(u, Ty::Bool, &schema, 2))
                .collect::<Result<Vec<_>, _>>()?;
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
            let k = u.int_in_range_usize(1..=arity)?;
            let mut outputs = Vec::with_capacity(k);
            for _ in 0..k {
                outputs.push(u.int_in_range_usize(0..=arity - 1)?);
            }
            let s = outputs.iter().map(|&i| schema[i]).collect();
            (inner.project(outputs), s, inner_nn)
        }
        3 => (inner.negate(), schema, false),
        4 => (inner.distinct(), schema, true),
        5 => (inner.threshold(), schema, true),
        // Union with a same-schema relation (self, or self negated).
        6 => {
            let (other, union_nn) = if u.any_bool()? {
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
            let n_extra = u.int_in_range_usize(1..=3)?;
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
                    if rc.is_empty() || !u.any_bool()? {
                        continue;
                    }
                    // Pick an earlier input that also has a column of this type.
                    let candidates: Vec<usize> = (0..r)
                        .filter(|&l| !cols_of(&input_schemas[l], ty).is_empty())
                        .collect();
                    if candidates.is_empty() {
                        continue;
                    }
                    let l = candidates[u.int_in_range_usize(0..=candidates.len() - 1)?];
                    let lc = cols_of(&input_schemas[l], ty);
                    let li = lc[u.int_in_range_usize(0..=lc.len() - 1)?];
                    let rj = rc[u.int_in_range_usize(0..=rc.len() - 1)?];
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
                if u.any_bool()? {
                    group_key.push(c);
                }
            }
            let n_agg = u.int_in_range_usize(0..=2)?;
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
                if u.ratio(1, 3)? {
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
                    desc: u.any_bool()?,
                    nulls_last: u.any_bool()?,
                });
            }
            let limit = if u.any_bool()? {
                Some(MirScalarExpr::literal_ok(
                    Datum::Int64(u.int_in_range_i64(0..=3)?),
                    ReprScalarType::Int64,
                ))
            } else {
                None
            };
            let offset = u.int_in_range_usize(0..=2)?;
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

/// What folding a plan produced.
///
/// The three cases must be distinguished by anything using folding as an oracle.
/// Collapsing "folded to an error" into "could not fold" is the dangerous
/// conflation: an erroring plan has a perfectly good expected result (the same
/// error), while an unfoldable plan has none, and treating both as "no verdict"
/// silently drops the error cases from the oracle's reach.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoldOutcome {
    /// The plan reduced to rows. An empty multiset is a real answer, not a
    /// missing one.
    Rows(BTreeMap<Row, Diff>),
    /// The plan reduced to an evaluation error. The expected result is that
    /// error.
    Error(EvalError),
    /// The plan did not reduce to a constant, so folding has no verdict.
    Unfoldable,
}

/// Fold `rel` and report what it produced. See [`FoldOutcome`].
pub fn fold_outcome(mut rel: MirRelationExpr) -> FoldOutcome {
    if apply_recursively(FoldConstants { limit: None }, &mut rel).is_err() {
        return FoldOutcome::Unfoldable;
    }
    match rel.as_const() {
        Some((Ok(rows), _)) => {
            let mut multiset: BTreeMap<Row, Diff> = BTreeMap::new();
            for (row, diff) in rows {
                *multiset.entry(row.clone()).or_insert(Diff::ZERO) += *diff;
            }
            multiset.retain(|_, d| *d != Diff::ZERO);
            FoldOutcome::Rows(multiset)
        }
        Some((Err(err), _)) => FoldOutcome::Error(err.clone()),
        None => FoldOutcome::Unfoldable,
    }
}

/// Fold `rel`. If it reduces to a `Constant` of `Ok` rows, return the
/// consolidated `(row, diff)` multiset, otherwise `None`.
///
/// `None` covers both an unfoldable plan and one that folds to an error. Use
/// [`fold_outcome`] where that difference matters, which it does for any use as an
/// oracle: an empty result is `Some(<empty map>)`, so `None` never means "empty".
pub fn fold_to_multiset(rel: MirRelationExpr) -> Option<BTreeMap<Row, Diff>> {
    match fold_outcome(rel) {
        FoldOutcome::Rows(multiset) => Some(multiset),
        FoldOutcome::Error(_) | FoldOutcome::Unfoldable => None,
    }
}

/// A deterministic [`Entropy`] source over a seeded ChaCha PRNG.
///
/// For callers that want reproducible-by-seed generation rather than
/// coverage-guided bytes: the same seed always yields the same plan, which is
/// what makes a generated corpus regenerable and a failure replayable from its
/// seed alone.
#[derive(Debug)]
pub struct SeededEntropy {
    rng: rand_chacha::ChaCha8Rng,
}

impl SeededEntropy {
    /// A source seeded by `seed`.
    pub fn new(seed: u64) -> Self {
        use rand::SeedableRng;
        SeededEntropy {
            rng: rand_chacha::ChaCha8Rng::seed_from_u64(seed),
        }
    }
}

impl Entropy for SeededEntropy {
    // A PRNG never runs out of entropy, so no draw can fail.
    type Error = std::convert::Infallible;

    fn int_in_range_u8(&mut self, range: RangeInclusive<u8>) -> Result<u8, Self::Error> {
        use rand::Rng;
        Ok(self.rng.random_range(range))
    }

    fn int_in_range_usize(&mut self, range: RangeInclusive<usize>) -> Result<usize, Self::Error> {
        use rand::Rng;
        Ok(self.rng.random_range(range))
    }

    fn int_in_range_i64(&mut self, range: RangeInclusive<i64>) -> Result<i64, Self::Error> {
        use rand::Rng;
        Ok(self.rng.random_range(range))
    }

    fn ratio(&mut self, numerator: u8, denominator: u8) -> Result<bool, Self::Error> {
        use rand::Rng;
        Ok(self.rng.random_range(0..denominator) < numerator)
    }

    fn any_bool(&mut self) -> Result<bool, Self::Error> {
        use rand::Rng;
        Ok(self.rng.random())
    }

    fn any_i32(&mut self) -> Result<i32, Self::Error> {
        use rand::Rng;
        Ok(self.rng.random())
    }

    fn any_i64(&mut self) -> Result<i64, Self::Error> {
        use rand::Rng;
        Ok(self.rng.random())
    }
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
