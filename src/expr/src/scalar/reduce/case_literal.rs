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

use mz_repr::{ReprColumnType, Row, SqlColumnType};

use crate::scalar::func::CaseLiteral;
use crate::{BinaryFunc, MirScalarExpr, VariadicFunc};

/// Entry point invoked from `reduce_post` for surviving `If` nodes.
pub(super) fn try_build(expr: &mut MirScalarExpr, column_types: &[ReprColumnType]) {
    try_fold_into_case_literal(expr);
    try_create_case_literal(expr, column_types);
}

/// Fold rule: if node is `If(Eq(x, lit), res, CallVariadic(CaseLiteral{..}, [x, ...]))`
/// where the CaseLiteral's input (`exprs[0]`) structurally equals `x`, insert (or
/// overwrite) `res` into the existing CaseLiteral.
///
/// Because we traverse bottom-up, the current If is an *earlier* arm than anything
/// already in the CaseLiteral. For duplicates, the outer/earlier arm wins per SQL
/// CASE semantics, so we overwrite the existing entry.
fn try_fold_into_case_literal(expr: &mut MirScalarExpr) {
    let MirScalarExpr::If { cond, then, els } = expr else {
        return;
    };
    let Some((common_candidate, literal_row)) = peek_eq_literal(cond) else {
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

    // Replace the If with the CaseLiteral.
    *expr = els.take();
}

/// Chain-walk rule: if node is an If-chain with >= 2 consecutive arms matching
/// `Eq(same_expr, literal)`, create a new CaseLiteral.
fn try_create_case_literal(expr: &mut MirScalarExpr, column_types: &[ReprColumnType]) {
    if !has_at_least_two_arms(expr) {
        return;
    }

    // Take the expression and dismantle it.
    let chain = expr.take();
    let (collected_cases, common, els) = collect_if_chain_arms(chain);

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
fn has_at_least_two_arms(expr: &MirScalarExpr) -> bool {
    let mut count = 0;
    let mut common_candidate: Option<&MirScalarExpr> = None;
    let mut current = expr;

    loop {
        match current {
            MirScalarExpr::If { cond, then: _, els } => {
                if let Some((expr_side, _literal_row)) = peek_eq_literal(cond) {
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

/// Inspects an `Eq(expr, literal)` condition and returns references to the
/// non-literal expression and the literal `Row`.
/// Returns `(non_literal_expr_ref, literal_row_ref)`.
fn peek_eq_literal(cond: &MirScalarExpr) -> Option<(&MirScalarExpr, &Row)> {
    let MirScalarExpr::CallBinary {
        func: BinaryFunc::Eq(_),
        expr1,
        expr2,
    } = cond
    else {
        return None;
    };

    if let Some(row) = expr1.as_literal_non_null_row() {
        if !expr2.is_literal() {
            return Some((expr2.as_ref(), row));
        }
    }
    if let Some(row) = expr2.as_literal_non_null_row() {
        if !expr1.is_literal() {
            return Some((expr1.as_ref(), row));
        }
    }
    None
}

/// Walks an If-chain and collects `(literal_row, result_expr)` pairs.
///
/// The input `chain` is consumed and dismantled.
/// Returns `(cases, common_candidate, els)`.
fn collect_if_chain_arms(
    chain: MirScalarExpr,
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
                if let Some((expr_side, literal_row)) = peek_eq_literal(&cond) {
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
    use mz_repr::{Datum, ReprColumnType, ReprScalarType};

    use crate::scalar::func::Eq;
    use crate::{BinaryFunc, MirScalarExpr, VariadicFunc};

    fn lit_i64(v: i64) -> MirScalarExpr {
        MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64)
    }
    fn eq(l: MirScalarExpr, r: MirScalarExpr) -> MirScalarExpr {
        l.call_binary(r, BinaryFunc::Eq(Eq))
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
}
