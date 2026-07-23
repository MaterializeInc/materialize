// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `MapFilterProject::optimize` must preserve evaluation.
//!
//! A `MapFilterProject` (MFP) is the linear map/filter/project pipeline that
//! runs in essentially every dataflow operator. `optimize` fuses and reorders
//! maps and predicates, drops unused map expressions, and canonicalizes the
//! projection. A miscompile here silently corrupts query results, so it is a
//! high-value correctness target.
//!
//! We build a random, well-typed MFP over an `int4`/`int8`/`bool` input schema
//! (map expressions can reference earlier columns, and predicates and the
//! projection reference any column), with a scalar vocabulary that includes
//! `int4`/`int8` arithmetic and the `int4`↔`int8` casts (the latter is fallible,
//! so it feeds optimize's error-handling). The target runs in one of two modes:
//!
//!  * **Non-temporal preservation.** Evaluate the raw, *unoptimized* MFP and the
//!    once-`optimize`d plan on a batch of random input rows, and compare. Neither
//!    side is routed through `into_plan`, because `MfpPlan::create_from`
//!    unconditionally runs `optimize` first. The raw reference is evaluated
//!    directly (`eval_raw`), and the optimized side is wrapped straight from the
//!    once-optimized MFP (`SafeMfpPlan::from_mfp`). Routing the reference through
//!    `into_plan` would compare `optimize` against `optimize`-of-`optimize`,
//!    testing only idempotence and silently passing any miscompile stable under a
//!    second pass. Routing the optimized side through it would compare against a
//!    twice-optimized plan, which can repair a first-pass miscompile that
//!    production `into_plan`-on-raw callers still execute. `optimize` only
//!    iterates while the expression size strictly decreases, so a fresh second
//!    call is not guaranteed to be a no-op. The oracle is one-directional,
//!    mirroring the contract optimize actually owes: optimize is allowed to
//!    *drop* an error or a row that the raw MFP would reject, because it removes
//!    unused map expressions and reorders predicates. But for every input row the
//!    raw MFP passes through cleanly with output `out`, the optimized plan must
//!    also pass it through with the byte-identical `out`. (When the raw MFP errors
//!    or filters a row we assert nothing.)
//!
//!  * **Temporal lowering.** Add predicates of the form `mz_now() <cmp> e` (and
//!    conjunctions of them) over a bounded `mz_timestamp` expression `e`, then
//!    lower the MFP to a temporal `MfpPlan` (`into_plan`, which runs `optimize`
//!    and `extract_temporal_bounds`, the operator/`StepMzTimestamp` translation
//!    that the non-temporal path bails on). The plan defines a per-row validity
//!    interval `[lower, upper)`. We read that interval off a single `evaluate`
//!    and check, for a batch of concrete logical times `T`, that "the row is live
//!    at `T`" (`lower <= T < upper`, with the non-temporal predicates also
//!    passing) agrees with a substitution reference: the same MFP with every
//!    `mz_now()` replaced by the literal `mz_timestamp` `T`, evaluated
//!    non-temporally. The compared timestamps are kept well below `u64::MAX` so
//!    `StepMzTimestamp` (the `+1` the lowering inserts for `<=`/`>`/`=`) never
//!    overflows, which keeps the substitution equivalence exact. As above the
//!    oracle is one-directional: if the reference errors at `T` we assert
//!    nothing.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::{
    func, Eval, EvalError, MapFilterProject, MirScalarExpr, SafeMfpPlan, UnmaterializableFunc,
};
use mz_repr::{Datum, Diff, ReprScalarType, Row, RowArena, Timestamp};

// Input schema: int4 columns, then int8 columns, then bool columns.
const N_IN_INT: usize = 2;
const N_IN_LONG: usize = 1;
const N_IN_BOOL: usize = 2;
const N_INPUT: usize = N_IN_INT + N_IN_LONG + N_IN_BOOL;
const MAX_MAPS: usize = 4;
const MAX_FILTERS: usize = 3;
const SCALAR_DEPTH: u32 = 4;
const ROWS_PER_MFP: usize = 8;
// Concrete logical times probed against the temporal plan's validity interval.
const TIMES_PER_MFP: usize = 6;

#[derive(Clone, Copy, PartialEq)]
enum Ty {
    Int,
    Long,
    Bool,
}

fn scalar_ty(ty: Ty) -> ReprScalarType {
    match ty {
        Ty::Int => ReprScalarType::Int32,
        Ty::Long => ReprScalarType::Int64,
        Ty::Bool => ReprScalarType::Bool,
    }
}

fn input_types() -> Vec<Ty> {
    let mut v = vec![Ty::Int; N_IN_INT];
    v.extend(std::iter::repeat(Ty::Long).take(N_IN_LONG));
    v.extend(std::iter::repeat(Ty::Bool).take(N_IN_BOOL));
    v
}

fn rand_ty(u: &mut Unstructured) -> arbitrary::Result<Ty> {
    Ok(match u.int_in_range(0u8..=2)? {
        0 => Ty::Int,
        1 => Ty::Long,
        _ => Ty::Bool,
    })
}

fn datum_of(u: &mut Unstructured, ty: Ty, nullable: bool) -> arbitrary::Result<Datum<'static>> {
    if nullable && u.ratio(1u8, 4u8)? {
        return Ok(Datum::Null);
    }
    Ok(match ty {
        Ty::Int => Datum::Int32(i32::arbitrary(u)?),
        Ty::Long => Datum::Int64(i64::arbitrary(u)?),
        Ty::Bool => {
            if bool::arbitrary(u)? {
                Datum::True
            } else {
                Datum::False
            }
        }
    })
}

/// Pick the index of an available column whose type is `want`, if any exists.
fn pick_col(u: &mut Unstructured, cols: &[Ty], want: Ty) -> arbitrary::Result<Option<usize>> {
    let matching: Vec<usize> = cols
        .iter()
        .enumerate()
        .filter(|(_, t)| **t == want)
        .map(|(i, _)| i)
        .collect();
    if matching.is_empty() {
        return Ok(None);
    }
    let i = u.int_in_range(0..=matching.len() - 1)?;
    Ok(Some(matching[i]))
}

fn gen_leaf(u: &mut Unstructured, want: Ty, cols: &[Ty]) -> arbitrary::Result<MirScalarExpr> {
    let st = scalar_ty(want);
    // Prefer a column reference when one of the right type is available. This is
    // what makes optimize's unused-map / projection reasoning interesting.
    if u.ratio(1u8, 2u8)? {
        if let Some(col) = pick_col(u, cols, want)? {
            return Ok(MirScalarExpr::column(col));
        }
    }
    Ok(match u.int_in_range(0u8..=1)? {
        0 => MirScalarExpr::literal_ok(datum_of(u, want, false)?, st),
        _ => MirScalarExpr::literal_null(st),
    })
}

fn gen_scalar(
    u: &mut Unstructured,
    want: Ty,
    cols: &[Ty],
    depth: u32,
) -> arbitrary::Result<MirScalarExpr> {
    if depth == 0 || u.ratio(2u8, 5u8)? {
        return gen_leaf(u, want, cols);
    }
    let d = depth - 1;
    match want {
        Ty::Int => match u.int_in_range(0u8..=5)? {
            0 => {
                let cond = gen_scalar(u, Ty::Bool, cols, d)?;
                let then = gen_scalar(u, Ty::Int, cols, d)?;
                let els = gen_scalar(u, Ty::Int, cols, d)?;
                Ok(cond.if_then_else(then, els))
            }
            1 => Ok(gen_scalar(u, Ty::Int, cols, d)?
                .call_binary(gen_scalar(u, Ty::Int, cols, d)?, func::AddInt32)),
            2 => Ok(gen_scalar(u, Ty::Int, cols, d)?
                .call_binary(gen_scalar(u, Ty::Int, cols, d)?, func::SubInt32)),
            3 => Ok(gen_scalar(u, Ty::Int, cols, d)?
                .call_binary(gen_scalar(u, Ty::Int, cols, d)?, func::MulInt32)),
            4 => Ok(gen_scalar(u, Ty::Int, cols, d)?
                .call_binary(gen_scalar(u, Ty::Int, cols, d)?, func::ModInt32)),
            // int8 -> int4 (fallible: out-of-range overflows).
            _ => Ok(gen_scalar(u, Ty::Long, cols, d)?.call_unary(func::CastInt64ToInt32)),
        },
        Ty::Long => match u.int_in_range(0u8..=4)? {
            0 => {
                let cond = gen_scalar(u, Ty::Bool, cols, d)?;
                let then = gen_scalar(u, Ty::Long, cols, d)?;
                let els = gen_scalar(u, Ty::Long, cols, d)?;
                Ok(cond.if_then_else(then, els))
            }
            1 => Ok(gen_scalar(u, Ty::Long, cols, d)?
                .call_binary(gen_scalar(u, Ty::Long, cols, d)?, func::AddInt64)),
            2 => Ok(gen_scalar(u, Ty::Long, cols, d)?
                .call_binary(gen_scalar(u, Ty::Long, cols, d)?, func::SubInt64)),
            3 => Ok(gen_scalar(u, Ty::Long, cols, d)?
                .call_binary(gen_scalar(u, Ty::Long, cols, d)?, func::MulInt64)),
            // int4 -> int8 (infallible widening).
            _ => Ok(gen_scalar(u, Ty::Int, cols, d)?.call_unary(func::CastInt32ToInt64)),
        },
        Ty::Bool => match u.int_in_range(0u8..=5)? {
            0 => {
                let cond = gen_scalar(u, Ty::Bool, cols, d)?;
                let then = gen_scalar(u, Ty::Bool, cols, d)?;
                let els = gen_scalar(u, Ty::Bool, cols, d)?;
                Ok(cond.if_then_else(then, els))
            }
            1 => Ok(gen_scalar(u, Ty::Bool, cols, d)?.and(gen_scalar(u, Ty::Bool, cols, d)?)),
            2 => Ok(gen_scalar(u, Ty::Bool, cols, d)?.or(gen_scalar(u, Ty::Bool, cols, d)?)),
            3 => Ok(gen_scalar(u, Ty::Bool, cols, d)?.not()),
            4 => {
                let t = rand_ty(u)?;
                let a = gen_scalar(u, t, cols, d)?;
                let b = gen_scalar(u, t, cols, d)?;
                Ok(match u.int_in_range(0u8..=4)? {
                    0 => a.call_binary(b, func::Eq),
                    1 => a.call_binary(b, func::Lt),
                    2 => a.call_binary(b, func::Gt),
                    3 => a.call_binary(b, func::Lte),
                    _ => a.call_binary(b, func::Gte),
                })
            }
            _ => {
                let t = rand_ty(u)?;
                Ok(gen_scalar(u, t, cols, d)?.call_is_null())
            }
        },
    }
}

fn gen_input_row(u: &mut Unstructured, types: &[Ty]) -> arbitrary::Result<Row> {
    let mut row = Row::default();
    let mut packer = row.packer();
    for ty in types {
        packer.push(datum_of(u, *ty, true)?);
    }
    drop(packer);
    Ok(row)
}

/// `mz_now()`.
fn mz_now() -> MirScalarExpr {
    MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow)
}

/// An `mz_timestamp` literal.
fn ts_literal(t: u64) -> MirScalarExpr {
    MirScalarExpr::literal_ok(
        Datum::MzTimestamp(Timestamp::new(t)),
        ReprScalarType::MzTimestamp,
    )
}

/// A bounded `mz_timestamp`-typed expression to compare `mz_now()` against.
/// Sometimes a bare literal, sometimes derived from an `int8` column via
/// `CastInt64ToMzTimestamp` so the bound depends on the row. Either way the
/// value is kept well below `u64::MAX` so the `StepMzTimestamp` (`+1`) that the
/// lowering inserts for `<=`/`>`/`=` cannot overflow, keeping the substitution
/// oracle exact.
fn gen_ts_expr(u: &mut Unstructured, cols: &[Ty]) -> arbitrary::Result<MirScalarExpr> {
    if u.ratio(1u8, 2u8)? {
        if let Some(col) = pick_col(u, cols, Ty::Long)? {
            // The column holds an arbitrary i64. Clamp it into [0, BOUND) with a
            // modulo so the cast can't go out of range or near the step overflow.
            let masked = MirScalarExpr::column(col)
                .call_binary(ts_bound_i64(), func::ModInt64)
                .call_unary(func::AbsInt64)
                .call_unary(func::CastInt64ToMzTimestamp);
            return Ok(masked);
        }
    }
    let t = u.int_in_range(0u64..=TS_BOUND)?;
    Ok(ts_literal(t))
}

/// Upper bound (exclusive) on generated timestamp magnitudes, far from
/// `u64::MAX` so `StepMzTimestamp` never overflows.
const TS_BOUND: u64 = 1_000_000;

fn ts_bound_i64() -> MirScalarExpr {
    MirScalarExpr::literal_ok(
        Datum::Int64(i64::try_from(TS_BOUND).unwrap()),
        ReprScalarType::Int64,
    )
}

/// A single temporal predicate `mz_now() <cmp> e`.
fn gen_temporal_pred(u: &mut Unstructured, cols: &[Ty]) -> arbitrary::Result<MirScalarExpr> {
    let e = gen_ts_expr(u, cols)?;
    Ok(match u.int_in_range(0u8..=4)? {
        0 => mz_now().call_binary(e, func::Eq),
        1 => mz_now().call_binary(e, func::Lt),
        2 => mz_now().call_binary(e, func::Lte),
        3 => mz_now().call_binary(e, func::Gt),
        _ => mz_now().call_binary(e, func::Gte),
    })
}

/// Build the reference plan for a temporal MFP at a concrete logical time `t`:
/// take the original MFP, replace every `mz_now()` with the literal
/// `mz_timestamp` `t`, and lower it. After substitution there are no temporal
/// expressions, so the plan is non-temporal by construction. Returns `None` if
/// lowering rejects it (treated as "assert nothing at this `t`"). The result
/// depends only on `t`, not on any row, so callers build it once per `t`.
fn reference_plan(mfp: &MapFilterProject, t: u64) -> Option<mz_expr::SafeMfpPlan> {
    let mut substituted = mfp.clone();
    let subst = |e: &mut MirScalarExpr| {
        e.visit_pre_mut(|node| {
            if let MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow) = node {
                *node = ts_literal(t);
            }
        });
    };
    for expr in &mut substituted.expressions {
        subst(expr);
    }
    for (_, pred) in &mut substituted.predicates {
        subst(pred);
    }
    substituted.into_plan().ok()?.into_nontemporal().ok()
}

/// Evaluate a reference plan on a row: `Some(pass)` for a clean pass/fail, `None`
/// on an evaluation error.
fn reference_present(plan: &mz_expr::SafeMfpPlan, row: &Row) -> Option<bool> {
    let arena = RowArena::new();
    let mut datums: Vec<Datum> = row.iter().collect();
    let mut buf = Row::default();
    match plan.evaluate_into(&mut datums, &arena, &mut buf) {
        Ok(Some(_)) => Some(true),
        Ok(None) => Some(false),
        Err(_) => None,
    }
}

/// Read the validity interval a temporal plan assigns to `row`, evaluating once
/// with `time = 0`, `diff = +1`, and an always-valid frontier. Returns:
///  * `Err(())` if evaluation errored,
///  * `Ok(None)` if the (non-temporal part of the) plan rejected the row,
///  * `Ok(Some((lower, upper)))` for the half-open interval `[lower, upper)`
///    (`upper == None` means unbounded above).
type Interval = Option<(Timestamp, Option<Timestamp>)>;
fn temporal_interval(plan: &mz_expr::MfpPlan, row: &Row) -> Result<Interval, ()> {
    let arena = RowArena::new();
    let mut datums: Vec<Datum> = row.iter().collect();
    let mut row_builder = Row::default();
    let mut lower: Option<Timestamp> = None;
    let mut upper: Option<Timestamp> = None;
    let mut errored = false;
    let mut produced = false;
    for result in plan.evaluate::<EvalError, _>(
        &mut datums,
        &arena,
        Timestamp::new(0),
        Diff::ONE,
        |_t| true,
        &mut row_builder,
    ) {
        produced = true;
        match result {
            Ok((_, t, diff)) => {
                if diff == Diff::ONE {
                    lower = Some(t);
                } else {
                    upper = Some(t);
                }
            }
            Err(_) => errored = true,
        }
    }
    if errored {
        return Err(());
    }
    if !produced {
        return Ok(None);
    }
    Ok(Some((lower.unwrap_or_else(|| Timestamp::new(0)), upper)))
}

/// Evaluate the raw, *unoptimized* MFP against `row`, replicating
/// `SafeMfpPlan::evaluate_inner` plus the projection directly. This is the
/// ground truth `optimize` must preserve.
///
/// We deliberately do not build the reference through `into_plan`:
/// `MfpPlan::create_from` unconditionally runs `optimize` first, so a reference
/// routed through it would compare `optimize` against `optimize`-of-`optimize`
/// and only test idempotence.
///
/// Returns `Some(Some(out))` for a clean pass with projected output `out`,
/// `Some(None)` for a cleanly filtered row, and `None` on an evaluation error
/// in any map or predicate.
fn eval_raw(mfp: &MapFilterProject, row: &Row) -> Option<Option<Row>> {
    let arena = RowArena::new();
    let mut datums: Vec<Datum> = row.iter().collect();
    // Interleave map evaluation with predicate checks exactly as
    // `evaluate_inner`: a predicate positioned at `support` runs once the maps
    // producing columns below `support` have been appended.
    let mut expression = 0;
    for (support, predicate) in &mfp.predicates {
        while mfp.input_arity + expression < *support {
            datums.push(mfp.expressions[expression].eval(&datums[..], &arena).ok()?);
            expression += 1;
        }
        match predicate.eval(&datums[..], &arena) {
            Ok(Datum::True) => {}
            Ok(_) => return Some(None),
            Err(_) => return None,
        }
    }
    while expression < mfp.expressions.len() {
        datums.push(mfp.expressions[expression].eval(&datums[..], &arena).ok()?);
        expression += 1;
    }
    let mut out = Row::default();
    out.packer()
        .extend(mfp.projection.iter().map(|c| datums[*c]));
    Some(Some(out))
}

/// Non-temporal preservation: raw unoptimized MFP semantics vs. the `optimize`d
/// plan's output.
fn run_nontemporal(
    u: &mut Unstructured,
    mfp: MapFilterProject,
    types: &[Ty],
) -> arbitrary::Result<()> {
    let mut optimized = mfp.clone();
    optimized.optimize();

    // Evaluate the once-optimized MFP directly, not through `into_plan`.
    // `into_plan` reaches `MfpPlan::create_from`, which runs `optimize` a second
    // time, so the compared side would reflect two passes instead of the single
    // pass a production `into_plan` caller on the raw MFP executes. Non-temporal
    // mode never generates `mz_now()` predicates, so there is nothing for
    // `into_plan`'s temporal extraction to do here.
    let safe_opt = SafeMfpPlan::from_mfp(optimized);

    for _ in 0..ROWS_PER_MFP {
        let row = gen_input_row(u, types)?;

        // Only a row the raw MFP passes through cleanly constrains the optimized
        // plan. An error or a filtered row lets optimize legitimately differ.
        let Some(Some(out_ref)) = eval_raw(&mfp, &row) else {
            continue;
        };

        let arena = RowArena::new();
        let mut datums_p: Vec<Datum> = row.iter().collect();
        let mut buf_p = Row::default();
        match safe_opt.evaluate_into(&mut datums_p, &arena, &mut buf_p) {
            Ok(Some(out)) => assert_eq!(
                &out_ref, out,
                "optimize changed the projected output\n  row = {row:?}\n  out_ref = {out_ref:?}\n  out_opt = {out:?}"
            ),
            Ok(None) => panic!("optimize filtered out a row the raw MFP passed\n  row = {row:?}"),
            Err(e) => panic!(
                "optimize errored on a row the raw MFP passed cleanly\n  row = {row:?}\n  err = {e:?}"
            ),
        }
    }
    Ok(())
}

/// Temporal lowering: validity interval from the lowered plan vs. the
/// `mz_now()`-substitution reference.
fn run_temporal(
    u: &mut Unstructured,
    mfp: MapFilterProject,
    types: &[Ty],
) -> arbitrary::Result<()> {
    // `into_plan` runs `optimize` and `extract_temporal_bounds`. An Err means an
    // unsupported temporal shape, which is a legitimate rejection, not a bug.
    let Ok(plan) = mfp.clone().into_plan() else {
        return Ok(());
    };

    // Sample the logical times to probe, and build each substitution reference
    // plan once (it depends only on `t`, not on the row). A reference plan that
    // fails to lower is dropped: we simply assert nothing at that time.
    let mut times: Vec<(u64, mz_expr::SafeMfpPlan)> = Vec::with_capacity(TIMES_PER_MFP);
    for _ in 0..TIMES_PER_MFP {
        let t = u.int_in_range(0u64..=TS_BOUND + 2)?;
        if let Some(ref_plan) = reference_plan(&mfp, t) {
            times.push((t, ref_plan));
        }
    }

    for _ in 0..ROWS_PER_MFP {
        let row = gen_input_row(u, types)?;
        let interval = temporal_interval(&plan, &row);
        // A plan that errors on this row lets the lowered plan legitimately
        // differ from the substitution reference, so assert nothing.
        let interval = match interval {
            Err(()) => continue,
            Ok(iv) => iv,
        };

        for (t, ref_plan) in &times {
            // One-directional: only constrain the plan when the reference is a
            // clean pass/fail. A reference error lets the lowered plan differ.
            let Some(present_ref) = reference_present(ref_plan, &row) else {
                continue;
            };
            let present_plan = match &interval {
                // Plan rejects the row at every time (non-temporal predicate
                // failed): it is absent at every `t`.
                None => false,
                Some((lower, upper)) => {
                    let tt = Timestamp::new(*t);
                    *lower <= tt && upper.map(|up| tt < up).unwrap_or(true)
                }
            };
            assert_eq!(
                present_plan, present_ref,
                "temporal lowering disagrees with substitution at t={t}\n  interval = {interval:?}\n  row = {row:?}\n  mfp = {mfp:?}"
            );
        }
    }
    Ok(())
}

fn run(u: &mut Unstructured) -> arbitrary::Result<()> {
    let types = input_types();
    // Columns available to later expressions: input columns plus appended maps.
    let mut cols = types.clone();

    let temporal = u.ratio(1u8, 2u8)?;

    let n_maps = u.int_in_range(0..=MAX_MAPS)?;
    let mut maps = Vec::with_capacity(n_maps);
    for _ in 0..n_maps {
        let t = rand_ty(u)?;
        maps.push(gen_scalar(u, t, &cols, SCALAR_DEPTH)?);
        cols.push(t);
    }

    let n_filters = u.int_in_range(0..=MAX_FILTERS)?;
    let mut filters = Vec::with_capacity(n_filters);
    for _ in 0..n_filters {
        // In temporal mode, mix in `mz_now()` predicates alongside ordinary ones.
        if temporal && u.ratio(3u8, 5u8)? {
            filters.push(gen_temporal_pred(u, &cols)?);
        } else {
            filters.push(gen_scalar(u, Ty::Bool, &cols, SCALAR_DEPTH)?);
        }
    }
    // Guarantee at least one temporal predicate in temporal mode so the lowering
    // path is actually exercised.
    if temporal && !filters.iter().any(|f| f.contains_temporal()) {
        filters.push(gen_temporal_pred(u, &cols)?);
    }

    let n_proj = u.int_in_range(0..=cols.len())?;
    let mut projection = Vec::with_capacity(n_proj);
    for _ in 0..n_proj {
        projection.push(u.int_in_range(0..=cols.len() - 1)?);
    }

    let mfp = MapFilterProject::new(N_INPUT)
        .map(maps)
        .filter(filters)
        .project(projection);

    if temporal {
        run_temporal(u, mfp, &types)
    } else {
        run_nontemporal(u, mfp, &types)
    }
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
