// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `MirScalarExpr::reduce` must preserve successful evaluations.
//! The optimizer folds constants, simplifies `If`, and propagates nulls via
//! `reduce`, and downstream passes trust the reduced expression to evaluate to
//! the same value as the original.
//!
//! We generate a random, well-typed expression over a fixed `int4`/`int8`/
//! `bool`/`text` schema, tracking the type of every subexpression during
//! generation so the result is well-typed by construction and `eval` can't
//! panic on a type mismatch. The function vocabulary spans the folder's
//! trickiest areas: integer arithmetic (`+`/`-`/`*`/`%`, negate, abs) on both
//! `int4` and `int8`, the boolean connectives `AND`/`OR`/`NOT`, comparisons and
//! `IS NULL` over every type, text concatenation/length, the `If` rewrites, and
//! a cast matrix (`int4`↔`int8`, `int`/`bool`→`text`, `text`→`int`, `int`→
//! `bool`) — many of which are *fallible* (overflow, parse failure, division by
//! zero), exercising reduce's error propagation.
//!
//! Crucially, alongside the binary `a AND b` / `a OR b` shapes, we also emit
//! *n-ary* `CallVariadic(And/Or, ..)` nodes with 3+ operands that nest other
//! n-ary And/Or directly inside (an `And` of `Or`s of `And`s, …). This is the
//! input shape that reaches reduce's variadic AND/OR machinery —
//! `flatten_associative` (collapsing same-func nests), `reduce_and_canonicalize`
//! (sort/dedup/short-circuit), `demorgans`, and especially `undistribute_and_or`
//! (factoring `(a&&b)||(a&&c)` into `a&&(b||c)`), which only fires on wide,
//! repeated-operand AND/OR trees that a purely binary generator essentially
//! never produces.
//!
//! We reduce a clone with the accurate column types and check evaluation on a
//! batch of random rows. The check is one-directional: reduce is allowed to
//! *eliminate* a runtime error (e.g. `If(c, x, x)` becomes `x`, dropping `c`,
//! and `x AND false` becomes `false`), so we only require that a successful
//! `Ok(v)` result is preserved exactly — reduce must never alter a value or turn
//! a success into an error. Every value type compared is exact (no float/numeric
//! normalization), so equality is the right oracle.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::func::variadic::{And, Or};
use mz_expr::{func, Eval, EvalError, MirScalarExpr};
use mz_repr::{Datum, ReprColumnType, ReprScalarType, Row, RowArena};

// Column layout: a contiguous block per type. Columns are nullable.
const N_INT: usize = 2; // int4   [COL_INT0, COL_LONG0)
const N_LONG: usize = 1; // int8  [COL_LONG0, COL_BOOL0)
const N_BOOL: usize = 2; // bool  [COL_BOOL0, COL_STR0)
const N_STR: usize = 1; // text   [COL_STR0, N_COLS)
const COL_INT0: usize = 0;
const COL_LONG0: usize = COL_INT0 + N_INT;
const COL_BOOL0: usize = COL_LONG0 + N_LONG;
const COL_STR0: usize = COL_BOOL0 + N_BOOL;
const N_COLS: usize = COL_STR0 + N_STR;
const MAX_DEPTH: u32 = 6;
const ROWS_PER_EXPR: usize = 8;

#[derive(Clone, Copy)]
enum Ty {
    Int,
    Long,
    Bool,
    Str,
}

fn scalar_ty(ty: Ty) -> ReprScalarType {
    match ty {
        Ty::Int => ReprScalarType::Int32,
        Ty::Long => ReprScalarType::Int64,
        Ty::Bool => ReprScalarType::Bool,
        Ty::Str => ReprScalarType::String,
    }
}

fn col_ty(col: usize) -> Ty {
    if col < COL_LONG0 {
        Ty::Int
    } else if col < COL_BOOL0 {
        Ty::Long
    } else if col < COL_STR0 {
        Ty::Bool
    } else {
        Ty::Str
    }
}

fn col_types() -> Vec<ReprColumnType> {
    (0..N_COLS)
        .map(|c| scalar_ty(col_ty(c)).nullable(true))
        .collect()
}

fn rand_ty(u: &mut Unstructured) -> arbitrary::Result<Ty> {
    Ok(match u.int_in_range(0u8..=3)? {
        0 => Ty::Int,
        1 => Ty::Long,
        2 => Ty::Bool,
        _ => Ty::Str,
    })
}

/// A short, bounded string drawn from an alphabet that sometimes parses as an
/// integer (so the `text`→`int` casts hit both the success and error paths).
fn gen_string(u: &mut Unstructured) -> arbitrary::Result<String> {
    const ALPHABET: &[u8] = b"01239-+ aZ";
    let n = u.int_in_range(0usize..=5)?;
    let mut s = String::with_capacity(n);
    for _ in 0..n {
        let i = u.int_in_range(0usize..=ALPHABET.len() - 1)?;
        s.push(ALPHABET[i] as char);
    }
    Ok(s)
}

/// A `'static` datum for the non-text types (text needs borrowed backing
/// storage and is handled inline at each call site).
fn nonstr_datum(u: &mut Unstructured, ty: Ty) -> arbitrary::Result<Datum<'static>> {
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
        Ty::Str => unreachable!("text datums are built from borrowed storage"),
    })
}

fn gen_leaf(u: &mut Unstructured, ty: Ty) -> arbitrary::Result<MirScalarExpr> {
    let st = scalar_ty(ty);
    Ok(match u.int_in_range(0u8..=3)? {
        0 => {
            let col = match ty {
                Ty::Int => COL_INT0 + u.int_in_range(0..=N_INT - 1)?,
                Ty::Long => COL_LONG0 + u.int_in_range(0..=N_LONG - 1)?,
                Ty::Bool => COL_BOOL0 + u.int_in_range(0..=N_BOOL - 1)?,
                Ty::Str => COL_STR0 + u.int_in_range(0..=N_STR - 1)?,
            };
            MirScalarExpr::column(col)
        }
        1 => match ty {
            Ty::Str => {
                let s = gen_string(u)?;
                MirScalarExpr::literal_ok(Datum::String(&s), st)
            }
            _ => MirScalarExpr::literal_ok(nonstr_datum(u, ty)?, st),
        },
        2 => MirScalarExpr::literal_null(st),
        // An error literal exercises reduce's error propagation/ordering.
        _ => MirScalarExpr::literal(Err(EvalError::DivisionByZero), st),
    })
}

/// Generate a small pool of bool "atoms" — bounded subexpressions that the
/// wide-And/Or builder draws operands from *with repetition*. Reuse is the whole
/// point: `undistribute_and_or` only fires when the same operand appears across
/// several branches (e.g. `(a&&b)||(a&&c)`), and `reduce_and_canonicalize`'s
/// sort/dedup/short-circuit paths likewise need duplicates and unit/zero
/// literals to bite. A purely independent recursive generator almost never
/// repeats a subterm, so we seed the pool explicitly.
fn gen_bool_atoms(u: &mut Unstructured, depth: u32) -> arbitrary::Result<Vec<MirScalarExpr>> {
    let n = u.int_in_range(2usize..=4)?;
    let mut atoms = Vec::with_capacity(n + 2);
    for _ in 0..n {
        atoms.push(gen_expr(u, Ty::Bool, depth)?);
    }
    // Seed unit/zero literals so canonicalization's true/false handling fires.
    atoms.push(MirScalarExpr::literal_ok(Datum::True, scalar_ty(Ty::Bool)));
    atoms.push(MirScalarExpr::literal_ok(Datum::False, scalar_ty(Ty::Bool)));
    Ok(atoms)
}

/// Generate an n-ary `CallVariadic(And/Or, ..)` with 3+ operands, nesting other
/// n-ary And/Or directly inside so reduce's `flatten_associative` and
/// `undistribute_and_or` paths are reached. Operands are drawn (with repetition)
/// from a shared atom pool, which is what lets the undistribution and
/// dedup/short-circuit rewrites actually match.
fn gen_wide_and_or(u: &mut Unstructured, depth: u32) -> arbitrary::Result<MirScalarExpr> {
    let outer_is_and = bool::arbitrary(u)?;
    let atoms = gen_bool_atoms(u, depth.saturating_sub(1))?;
    let pick =
        |u: &mut Unstructured, atoms: &[MirScalarExpr]| -> arbitrary::Result<MirScalarExpr> {
            let i = u.int_in_range(0..=atoms.len() - 1)?;
            Ok(atoms[i].clone())
        };

    let n_operands = u.int_in_range(3usize..=6)?;
    let mut operands = Vec::with_capacity(n_operands);
    for _ in 0..n_operands {
        // With some probability nest an inner And/Or (often the *opposite*
        // connective, the `(a&&b)||(a&&c)` distribution shape; sometimes the
        // same one to exercise `flatten_associative`).
        if depth > 0 && u.ratio(2u8, 5u8)? {
            let inner_is_and = if u.ratio(3u8, 4u8)? {
                !outer_is_and
            } else {
                outer_is_and
            };
            let n_inner = u.int_in_range(2usize..=4)?;
            let mut inner = Vec::with_capacity(n_inner);
            for _ in 0..n_inner {
                inner.push(pick(u, &atoms)?);
            }
            operands.push(if inner_is_and {
                MirScalarExpr::call_variadic(And, inner)
            } else {
                MirScalarExpr::call_variadic(Or, inner)
            });
        } else {
            operands.push(pick(u, &atoms)?);
        }
    }
    Ok(if outer_is_and {
        MirScalarExpr::call_variadic(And, operands)
    } else {
        MirScalarExpr::call_variadic(Or, operands)
    })
}

fn gen_expr(u: &mut Unstructured, ty: Ty, depth: u32) -> arbitrary::Result<MirScalarExpr> {
    if depth == 0 || u.ratio(2u8, 5u8)? {
        return gen_leaf(u, ty);
    }
    let d = depth - 1;
    match ty {
        Ty::Int => match u.int_in_range(0u8..=7)? {
            // If { cond: bool, then: int, els: int }
            0 => {
                let cond = gen_expr(u, Ty::Bool, d)?;
                let then = gen_expr(u, Ty::Int, d)?;
                let els = gen_expr(u, Ty::Int, d)?;
                Ok(cond.if_then_else(then, els))
            }
            // int4 arithmetic — `+`/`-`/`*`/`%` may overflow or divide by zero,
            // producing an EvalError tolerated by the one-directional oracle.
            1 => Ok(gen_expr(u, Ty::Int, d)?.call_binary(gen_expr(u, Ty::Int, d)?, func::AddInt32)),
            2 => Ok(gen_expr(u, Ty::Int, d)?.call_binary(gen_expr(u, Ty::Int, d)?, func::SubInt32)),
            3 => Ok(gen_expr(u, Ty::Int, d)?.call_binary(gen_expr(u, Ty::Int, d)?, func::MulInt32)),
            4 => Ok(gen_expr(u, Ty::Int, d)?.call_binary(gen_expr(u, Ty::Int, d)?, func::ModInt32)),
            5 => Ok(gen_expr(u, Ty::Int, d)?.call_unary(func::NegInt32)),
            6 => Ok(gen_expr(u, Ty::Int, d)?.call_unary(func::AbsInt32)),
            // Casts that produce an int4.
            _ => match u.int_in_range(0u8..=2)? {
                0 => Ok(gen_expr(u, Ty::Long, d)?.call_unary(func::CastInt64ToInt32)),
                1 => Ok(gen_expr(u, Ty::Str, d)?.call_unary(func::CastStringToInt32)),
                _ => Ok(gen_expr(u, Ty::Str, d)?.call_unary(func::ByteLengthString)),
            },
        },
        Ty::Long => {
            match u.int_in_range(0u8..=4)? {
                0 => {
                    let cond = gen_expr(u, Ty::Bool, d)?;
                    let then = gen_expr(u, Ty::Long, d)?;
                    let els = gen_expr(u, Ty::Long, d)?;
                    Ok(cond.if_then_else(then, els))
                }
                1 => Ok(gen_expr(u, Ty::Long, d)?
                    .call_binary(gen_expr(u, Ty::Long, d)?, func::AddInt64)),
                2 => Ok(gen_expr(u, Ty::Long, d)?
                    .call_binary(gen_expr(u, Ty::Long, d)?, func::SubInt64)),
                3 => Ok(gen_expr(u, Ty::Long, d)?
                    .call_binary(gen_expr(u, Ty::Long, d)?, func::MulInt64)),
                // Casts that produce an int8.
                _ => match u.int_in_range(0u8..=1)? {
                    0 => Ok(gen_expr(u, Ty::Int, d)?.call_unary(func::CastInt32ToInt64)),
                    _ => Ok(gen_expr(u, Ty::Str, d)?.call_unary(func::CastStringToInt64)),
                },
            }
        }
        Ty::Bool => match u.int_in_range(0u8..=7)? {
            0 => {
                let cond = gen_expr(u, Ty::Bool, d)?;
                let then = gen_expr(u, Ty::Bool, d)?;
                let els = gen_expr(u, Ty::Bool, d)?;
                Ok(cond.if_then_else(then, els))
            }
            1 => Ok(gen_expr(u, Ty::Bool, d)?.and(gen_expr(u, Ty::Bool, d)?)),
            2 => Ok(gen_expr(u, Ty::Bool, d)?.or(gen_expr(u, Ty::Bool, d)?)),
            3 => Ok(gen_expr(u, Ty::Bool, d)?.not()),
            // A wide, nested n-ary And/Or with shared operands — the shape that
            // reaches flatten_associative / undistribute_and_or.
            7 => gen_wide_and_or(u, d),
            // A comparison of two operands of a common type yields a bool.
            4 => {
                let t = rand_ty(u)?;
                let a = gen_expr(u, t, d)?;
                let b = gen_expr(u, t, d)?;
                Ok(match u.int_in_range(0u8..=4)? {
                    0 => a.call_binary(b, func::Eq),
                    1 => a.call_binary(b, func::Lt),
                    2 => a.call_binary(b, func::Gt),
                    3 => a.call_binary(b, func::Lte),
                    _ => a.call_binary(b, func::Gte),
                })
            }
            // IS NULL of any-typed operand yields a bool.
            5 => {
                let t = rand_ty(u)?;
                Ok(gen_expr(u, t, d)?.call_is_null())
            }
            // int4 -> bool cast.
            _ => Ok(gen_expr(u, Ty::Int, d)?.call_unary(func::CastInt32ToBool)),
        },
        Ty::Str => match u.int_in_range(0u8..=3)? {
            0 => {
                let cond = gen_expr(u, Ty::Bool, d)?;
                let then = gen_expr(u, Ty::Str, d)?;
                let els = gen_expr(u, Ty::Str, d)?;
                Ok(cond.if_then_else(then, els))
            }
            1 => Ok(gen_expr(u, Ty::Str, d)?
                .call_binary(gen_expr(u, Ty::Str, d)?, func::TextConcatBinary)),
            2 => Ok(gen_expr(u, Ty::Int, d)?.call_unary(func::CastInt32ToString)),
            _ => Ok(gen_expr(u, Ty::Bool, d)?.call_unary(func::CastBoolToString)),
        },
    }
}

fn gen_row(u: &mut Unstructured) -> arbitrary::Result<Row> {
    let mut row = Row::default();
    let mut packer = row.packer();
    for c in 0..N_COLS {
        if u.ratio(1u8, 4u8)? {
            packer.push(Datum::Null);
            continue;
        }
        match col_ty(c) {
            Ty::Str => {
                let s = gen_string(u)?;
                packer.push(Datum::String(&s));
            }
            ty => packer.push(nonstr_datum(u, ty)?),
        }
    }
    drop(packer);
    Ok(row)
}

fn run(u: &mut Unstructured) -> arbitrary::Result<()> {
    let types = col_types();
    let top_ty = rand_ty(u)?;
    let expr = gen_expr(u, top_ty, MAX_DEPTH)?;

    let mut reduced = expr.clone();
    reduced.reduce(&types);

    for _ in 0..ROWS_PER_EXPR {
        let row = gen_row(u)?;
        let datums: Vec<Datum> = row.iter().collect();
        let arena = RowArena::new();
        let original = expr.eval(&datums, &arena);
        let folded = reduced.eval(&datums, &arena);
        // The invariant is one-directional. `reduce` is *permitted* to eliminate
        // a runtime error: e.g. `reduce_if` collapses `If(c, x, x)` to `x`, which
        // drops `c` (and any error it would raise). So an `Err` original may be
        // turned into anything. But it must never change a *successful* value, and
        // must never introduce an error where evaluation succeeded: when the
        // original is `Ok(v)`, the reduced expression must also be exactly `Ok(v)`.
        if original.is_ok() {
            assert_eq!(
                original, folded,
                "reduce changed a successful evaluation\n  expr     = {expr:?}\n  reduced  = {reduced:?}\n  row      = {row:?}"
            );
        }
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
