// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Post-order construction of `CaseLiteral` from `If(Eq(x, lit), r, ...)`
//! chains. Runs as part of `reduce` after `if_then::reduce_if`, so it only
//! sees `If` nodes that survived boolean-algebra rewrites. The fold/create
//! split mirrors the bottom-up traversal: `try_create` builds a CaseLiteral at
//! the deepest 2-arm point, `try_fold` accretes each outer arm as the post-order
//! visitor unwinds.

use std::collections::BTreeSet;

use mz_repr::{ReprColumnType, ReprScalarType, Row, SqlColumnType};

use crate::scalar::func::CaseLiteral;
use crate::{BinaryFunc, MirScalarExpr, VariadicFunc};

/// Entry point invoked from `reduce_post` for surviving `If` nodes.
pub(super) fn try_build(expr: &mut MirScalarExpr, column_types: &[ReprColumnType]) {
    try_fold_into_case_literal(expr, column_types);
    try_create_case_literal(expr, column_types);
}

/// Fold rule: if node is `If(Eq(x, lit), res, CallVariadic(CaseLiteral{..}, [x, ...]))`
/// where the CaseLiteral's input (`exprs[0]`) structurally equals `x`, insert (or
/// overwrite) `res` into the existing CaseLiteral.
///
/// Because we traverse bottom-up, the current If is an *earlier* arm than anything
/// already in the CaseLiteral. For duplicates, the outer/earlier arm wins per SQL
/// CASE semantics, so we overwrite the existing entry.
fn try_fold_into_case_literal(expr: &mut MirScalarExpr, column_types: &[ReprColumnType]) {
    let MirScalarExpr::If { cond, then, els } = expr else {
        return;
    };
    let Some((common_candidate, literal_row)) = peek_eq_literal(cond, column_types) else {
        return;
    };
    let MirScalarExpr::CallVariadic {
        func: VariadicFunc::CaseLiteral(cl),
        exprs,
    } = els.as_mut()
    else {
        return;
    };

    // Check that the CaseLiteral's input matches the If's common expression.
    if exprs[0] != *common_candidate {
        return;
    }

    if let Some(existing_idx) = cl.get(literal_row) {
        // Duplicate literal: overwrite with the earlier arm's result (this If).
        exprs[existing_idx] = then.take();
    } else {
        // New literal: insert before the fallback (last position).
        let new_idx = exprs.len() - 1;
        exprs.insert(new_idx, then.take());
        cl.insert(literal_row.clone(), new_idx);
    }

    // Recompute the return type over the result arms and the fallback
    // (`exprs[1..]`; `exprs[0]` is the input). The folded-in arm's type — in
    // particular its nullability — must be unioned in, exactly as
    // `try_create_case_literal` does, otherwise the CaseLiteral under-reports
    // its type and downstream rules (e.g. IsNull folding) misfire.
    let mut return_type: Option<ReprColumnType> = None;
    for result in &exprs[1..] {
        let t = result.typ(column_types);
        return_type = Some(match return_type {
            None => t,
            Some(prev) => prev.union(&t).expect("incompatible branch types"),
        });
    }
    cl.return_type =
        SqlColumnType::from_repr(&return_type.expect("CaseLiteral has at least a fallback"));

    // Replace the If with the CaseLiteral.
    *expr = els.take();
}

/// Chain-walk rule: if node is an If-chain with >= 2 consecutive arms matching
/// `Eq(same_expr, literal)`, create a new CaseLiteral.
fn try_create_case_literal(expr: &mut MirScalarExpr, column_types: &[ReprColumnType]) {
    if !has_at_least_two_arms(expr, column_types) {
        return;
    }

    // Take the expression and dismantle it.
    let chain = expr.take();
    let (collected_cases, common, els) = collect_if_chain_arms(chain, column_types);

    let common = common.expect("common expr must be set when arm_count >= 2");

    // Compute the return type as the union of all branch types and els type.
    let mut return_type: Option<ReprColumnType> = None;
    for (_, result) in &collected_cases {
        let t = result.typ(column_types);
        return_type = Some(match return_type {
            None => t,
            Some(prev) => prev.union(&t).expect("incompatible branch types"),
        });
    }
    let els_type = els.typ(column_types);
    let return_type = match return_type {
        Some(prev) => prev.union(&els_type).expect("incompatible else type"),
        None => els_type,
    };
    let sql_return_type = SqlColumnType::from_repr(&return_type);

    // Build the exprs vector: [input, result1, result2, ..., els]
    let mut exprs = Vec::with_capacity(collected_cases.len() + 2);
    exprs.push(common);
    let mut cl = CaseLiteral {
        lookup: Vec::with_capacity(collected_cases.len()),
        return_type: sql_return_type,
    };
    for (row, result_expr) in collected_cases {
        let idx = exprs.len();
        cl.insert(row, idx);
        exprs.push(result_expr);
    }
    exprs.push(els);

    *expr = MirScalarExpr::CallVariadic {
        func: VariadicFunc::CaseLiteral(cl),
        exprs,
    };
}

/// Returns `true` if the If-chain has at least 2 arms matching `Eq(same_expr, literal)`.
/// Bails early once 2 are found to avoid unnecessary traversal.
fn has_at_least_two_arms(expr: &MirScalarExpr, column_types: &[ReprColumnType]) -> bool {
    let mut count = 0;
    let mut common_candidate: Option<&MirScalarExpr> = None;
    let mut current = expr;

    loop {
        match current {
            MirScalarExpr::If { cond, then: _, els } => {
                if let Some((expr_side, _literal_row)) = peek_eq_literal(cond, column_types) {
                    match common_candidate {
                        None => {
                            common_candidate = Some(expr_side);
                        }
                        Some(existing) => {
                            if existing != expr_side {
                                break;
                            }
                        }
                    }
                    count += 1;
                    if count >= 2 {
                        return true;
                    }
                    current = els;
                } else {
                    break;
                }
            }
            _ => break,
        }
    }

    false
}

/// A CaseLiteral lookup probes a BTreeMap keyed on Row total-order, which
/// disagrees with SQL `=` for floating point (`-0.0 = 0.0` is true, but they
/// sort distinct). Folding such a chain is therefore unsound (database-issues#11317).
/// Returns true if `t` is or transitively contains a floating-point type, or is
/// jsonb (which can hold f64).
fn type_is_float_bearing(t: &ReprScalarType) -> bool {
    match t {
        // Floats themselves, and jsonb which can store an f64.
        ReprScalarType::Float32 | ReprScalarType::Float64 | ReprScalarType::Jsonb => true,
        // Composite types: float-bearing iff any contained type is.
        ReprScalarType::Array(element_type)
        | ReprScalarType::List { element_type }
        | ReprScalarType::Map {
            value_type: element_type,
        }
        | ReprScalarType::Range { element_type } => type_is_float_bearing(element_type),
        ReprScalarType::Record { fields } => fields
            .iter()
            .any(|f| type_is_float_bearing(&f.scalar_type)),
        // Lookup-safe scalar types: their Row total-order agrees with SQL `=`.
        // Listed explicitly (no catch-all) so a new scalar type forces a
        // conscious decision here.
        ReprScalarType::Bool
        | ReprScalarType::Int16
        | ReprScalarType::Int32
        | ReprScalarType::Int64
        | ReprScalarType::UInt8
        | ReprScalarType::UInt16
        | ReprScalarType::UInt32
        | ReprScalarType::UInt64
        | ReprScalarType::Numeric
        | ReprScalarType::Date
        | ReprScalarType::Time
        | ReprScalarType::Timestamp
        | ReprScalarType::TimestampTz
        | ReprScalarType::MzTimestamp
        | ReprScalarType::Interval
        | ReprScalarType::Bytes
        | ReprScalarType::String
        | ReprScalarType::Uuid
        // Int2Vector is a vector of int2 (no floats).
        | ReprScalarType::Int2Vector
        | ReprScalarType::MzAclItem
        | ReprScalarType::AclItem => false,
    }
}

/// Inspects an `Eq(expr, literal)` condition and returns references to the
/// non-literal expression and the literal `Row`.
/// Returns `(non_literal_expr_ref, literal_row_ref)`.
///
/// Returns `None` for a comparison whose non-literal side has a float-bearing
/// type: the CaseLiteral lookup uses Row total-order, which disagrees with SQL
/// `=` for floats (database-issues#11317), so such chains must not fold.
fn peek_eq_literal<'a>(
    cond: &'a MirScalarExpr,
    column_types: &[ReprColumnType],
) -> Option<(&'a MirScalarExpr, &'a Row)> {
    let MirScalarExpr::CallBinary {
        func: BinaryFunc::Eq(_),
        expr1,
        expr2,
    } = cond
    else {
        return None;
    };

    let result = if let Some(row) = expr1.as_literal_non_null_row() {
        if !expr2.is_literal() {
            Some((expr2.as_ref(), row))
        } else {
            None
        }
    } else if let Some(row) = expr2.as_literal_non_null_row() {
        if !expr1.is_literal() {
            Some((expr1.as_ref(), row))
        } else {
            None
        }
    } else {
        None
    };

    let (x, row) = result?;
    if type_is_float_bearing(&x.typ(column_types).scalar_type) {
        return None;
    }
    Some((x, row))
}

/// Walks an If-chain and collects `(literal_row, result_expr)` pairs.
///
/// The input `chain` is consumed and dismantled.
/// Returns `(cases, common_candidate, els)`.
fn collect_if_chain_arms(
    chain: MirScalarExpr,
    column_types: &[ReprColumnType],
) -> (
    Vec<(Row, MirScalarExpr)>,
    Option<MirScalarExpr>,
    MirScalarExpr,
) {
    let mut cases = Vec::new();
    let mut seen = BTreeSet::new();
    let mut common_candidate: Option<MirScalarExpr> = None;
    let mut remaining = chain;

    loop {
        match remaining {
            MirScalarExpr::If { cond, then, els } => {
                if let Some((expr_side, literal_row)) = peek_eq_literal(&cond, column_types) {
                    match &common_candidate {
                        None => {
                            common_candidate = Some(expr_side.clone());
                        }
                        Some(existing) => {
                            if existing != expr_side {
                                remaining = MirScalarExpr::If { cond, then, els };
                                break;
                            }
                        }
                    }

                    // First occurrence of each literal wins (SQL CASE semantics).
                    if seen.insert(literal_row.clone()) {
                        cases.push((literal_row.clone(), *then));
                    }

                    remaining = *els;
                } else {
                    remaining = MirScalarExpr::If { cond, then, els };
                    break;
                }
            }
            _ => break,
        }
    }

    (cases, common_candidate, remaining)
}

#[cfg(test)]
mod tests {
    use mz_repr::{Datum, ReprColumnType, ReprScalarType, RowArena};
    use proptest::prelude::*;

    use crate::scalar::func::Eq;
    use crate::{BinaryFunc, Eval, MirScalarExpr, VariadicFunc};

    fn lit_i64(v: i64) -> MirScalarExpr {
        MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64)
    }
    fn lit_f64(v: f64) -> MirScalarExpr {
        MirScalarExpr::literal_ok(
            Datum::Float64(ordered_float::OrderedFloat(v)),
            ReprScalarType::Float64,
        )
    }
    fn eq(l: MirScalarExpr, r: MirScalarExpr) -> MirScalarExpr {
        l.call_binary(r, BinaryFunc::Eq(Eq))
    }

    #[mz_ore::test]
    fn float_chain_is_not_folded() {
        // CASE WHEN x = 0.0 THEN 1 WHEN x = 1.0 THEN 2 ELSE 0 END over a float8 column.
        // Folding is unsound for floats (-0.0 vs +0.0), so it must stay an If.
        let col = MirScalarExpr::column(0);
        let mut expr = MirScalarExpr::If {
            cond: Box::new(eq(col.clone(), lit_f64(0.0))),
            then: Box::new(lit_i64(1)),
            els: Box::new(MirScalarExpr::If {
                cond: Box::new(eq(col.clone(), lit_f64(1.0))),
                then: Box::new(lit_i64(2)),
                els: Box::new(lit_i64(0)),
            }),
        };
        let col_types = vec![ReprColumnType {
            scalar_type: ReprScalarType::Float64,
            nullable: true,
        }];
        crate::scalar::reduce::reduce(&mut expr, &col_types);
        assert!(
            matches!(expr, MirScalarExpr::If { .. }),
            "float chain must NOT fold, got {expr:?}"
        );
    }

    #[mz_ore::test]
    fn two_arm_chain_becomes_case_literal() {
        let col = MirScalarExpr::column(0);
        // If(col=1, 10, If(col=2, 20, 0))
        let mut expr = MirScalarExpr::If {
            cond: Box::new(eq(col.clone(), lit_i64(1))),
            then: Box::new(lit_i64(10)),
            els: Box::new(MirScalarExpr::If {
                cond: Box::new(eq(col.clone(), lit_i64(2))),
                then: Box::new(lit_i64(20)),
                els: Box::new(lit_i64(0)),
            }),
        };
        let col_types = vec![ReprColumnType {
            scalar_type: ReprScalarType::Int64,
            nullable: true,
        }];
        crate::scalar::reduce::reduce(&mut expr, &col_types);
        assert!(
            matches!(
                expr,
                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::CaseLiteral(_),
                    ..
                }
            ),
            "expected CaseLiteral, got {expr:?}"
        );
    }

    #[mz_ore::test]
    fn fold_preserves_nullability() {
        // CASE WHEN c=1 THEN NULL WHEN c=2 THEN 20 WHEN c=3 THEN 30 ELSE 0 END.
        // The c=2/c=3 arms form the inner CaseLiteral (via try_create); the c=1
        // NULL arm is folded in (via try_fold). The folded NULL must keep the
        // overall result type nullable.
        let col = MirScalarExpr::column(0);
        let null_i64 = MirScalarExpr::literal_null(mz_repr::ReprScalarType::Int64);
        let mut expr = MirScalarExpr::If {
            cond: Box::new(eq(col.clone(), lit_i64(1))),
            then: Box::new(null_i64),
            els: Box::new(MirScalarExpr::If {
                cond: Box::new(eq(col.clone(), lit_i64(2))),
                then: Box::new(lit_i64(20)),
                els: Box::new(MirScalarExpr::If {
                    cond: Box::new(eq(col.clone(), lit_i64(3))),
                    then: Box::new(lit_i64(30)),
                    els: Box::new(lit_i64(0)),
                }),
            }),
        };
        let col_types = vec![mz_repr::ReprColumnType {
            scalar_type: mz_repr::ReprScalarType::Int64,
            nullable: true,
        }];
        let original_type = expr.typ(&col_types);
        crate::scalar::reduce::reduce(&mut expr, &col_types);
        // Sanity: it actually folded into a CaseLiteral.
        assert!(
            matches!(
                expr,
                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::CaseLiteral(_),
                    ..
                }
            ),
            "expected CaseLiteral, got {expr:?}"
        );
        // The bug: folded NULL arm lost nullability.
        assert_eq!(
            expr.typ(&col_types),
            original_type,
            "reduced CaseLiteral type must match the original If-chain type (nullability preserved)"
        );
    }

    /// Evaluate `expr` against a single optional-i64 input column.
    /// Returns Ok(Some(v)) for an i64 result, Ok(None) for NULL, Err(()) for an
    /// eval error. Collapsing errors to a unit lets us compare originals and
    /// reduced forms for equivalence including the error case.
    fn eval_one(expr: &MirScalarExpr, input: Option<i64>) -> Result<Option<i64>, ()> {
        let datums = vec![input.map_or(Datum::Null, Datum::Int64)];
        let arena = RowArena::new();
        match expr.eval(&datums, &arena) {
            Ok(d) if d.is_null() => Ok(None),
            Ok(Datum::Int64(v)) => Ok(Some(v)),
            Ok(other) => panic!("unexpected datum {other:?}"),
            Err(_) => Err(()),
        }
    }

    proptest! {
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow under miri
        fn reduce_preserves_eval(
            arms in prop::collection::vec((-5i64..5, proptest::option::of(-100i64..100)), 2..6),
            fallback in proptest::option::of(-100i64..100),
            probes in prop::collection::vec(-8i64..8, 1..6),
        ) {
            // Map a generated optional result to a literal: `Some(v)` -> integer
            // literal, `None` -> typed NULL literal. Exercising NULL results lets
            // the type-equality assertion below catch under-reported nullability.
            let result_lit = |v: &Option<i64>| match v {
                Some(v) => lit_i64(*v),
                None => MirScalarExpr::literal_null(mz_repr::ReprScalarType::Int64),
            };
            let col = MirScalarExpr::column(0);
            // Build the chain bottom-up so earlier arms end up outermost
            // (outer arm wins for duplicate literals, matching SQL CASE).
            let mut chain = result_lit(&fallback);
            for (lit, res) in arms.iter().rev() {
                chain = MirScalarExpr::If {
                    cond: Box::new(eq(col.clone(), lit_i64(*lit))),
                    then: Box::new(result_lit(res)),
                    els: Box::new(chain),
                };
            }
            let original = chain.clone();
            let mut reduced = chain;
            let col_types = vec![ReprColumnType {
                scalar_type: ReprScalarType::Int64,
                nullable: true,
            }];
            crate::scalar::reduce::reduce(&mut reduced, &col_types);

            // Probe NULL, every literal used in the chain, and random values.
            let mut inputs: Vec<Option<i64>> = vec![None];
            for (lit, _) in &arms {
                inputs.push(Some(*lit));
            }
            for p in &probes {
                inputs.push(Some(*p));
            }

            // Soundness of the reduced type's nullability: if any reachable input
            // makes the (original) expression evaluate to NULL, the reduced form
            // must report a nullable type. The original bug folded a NULL arm
            // without unioning its nullability, so the reduced type claimed
            // `nullable: false` while a reachable arm still produced NULL — which
            // is exactly what this catches. We compare against eval rather than
            // the original's static type because `typ` over-reports nullability
            // for dead duplicate-literal arms that reduce legitimately drops.
            let reduced_type = reduced.typ(&col_types);
            for input in &inputs {
                if eval_one(&original, *input) == Ok(None) {
                    prop_assert!(
                        reduced_type.nullable,
                        "reduced type must be nullable: input {:?} evaluates to NULL but \
                         reduced type is {:?}",
                        input,
                        reduced_type
                    );
                }
            }

            for input in inputs {
                prop_assert_eq!(
                    eval_one(&original, input),
                    eval_one(&reduced, input),
                    "mismatch at input {:?}: original chain vs reduced", input
                );
            }
        }
    }
}
