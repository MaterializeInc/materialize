// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rewrites chains of `If(Eq(expr, literal), result, If(...))` into
//! `CaseLiteral { cases, els }` for O(log n) evaluation via `BTreeMap` lookup.

use std::collections::BTreeMap;

use mz_expr::visit::Visit;
use mz_expr::{BinaryFunc, MirRelationExpr, MirScalarExpr, UnaryFunc};
use mz_repr::{ReprColumnType, Row, SqlColumnType};

use crate::TransformCtx;

/// Rewrites If-chains matching a single expression against literals
/// into a `CaseLiteral` unary function with `BTreeMap` lookup.
#[derive(Debug)]
pub struct CaseLiteralTransform;

impl crate::Transform for CaseLiteralTransform {
    fn name(&self) -> &'static str {
        "CaseLiteralTransform"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "case_literal")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        _ctx: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        relation.try_visit_mut_post::<_, crate::TransformError>(
            &mut |expr: &mut MirRelationExpr| {
                rewrite_scalars_in_relation(expr);
                Ok(())
            },
        )?;
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

/// Rewrites scalar expressions within a single `MirRelationExpr` node.
///
/// Collects column types from the input relation to compute return types
/// for the generated `CaseLiteral` nodes.
fn rewrite_scalars_in_relation(expr: &mut MirRelationExpr) {
    match expr {
        MirRelationExpr::Map { input, scalars } => {
            let mut column_types = input.typ().column_types;
            for scalar in scalars.iter_mut() {
                rewrite_if_chain(scalar, &column_types);
                let new_type = scalar.typ(&column_types);
                column_types.push(new_type);
            }
        }
        MirRelationExpr::Filter { input, predicates } => {
            let column_types = input.typ().column_types;
            for predicate in predicates.iter_mut() {
                rewrite_if_chain(predicate, &column_types);
            }
        }
        MirRelationExpr::Reduce {
            input, aggregates, ..
        } => {
            let column_types = input.typ().column_types;
            for agg in aggregates.iter_mut() {
                rewrite_if_chain(&mut agg.expr, &column_types);
            }
        }
        MirRelationExpr::FlatMap { input, exprs, .. } => {
            let column_types = input.typ().column_types;
            for e in exprs.iter_mut() {
                rewrite_if_chain(e, &column_types);
            }
        }
        _ => {}
    }
}

/// Rewrites a scalar expression tree, replacing If-chains of
/// `If(Eq(common_expr, literal), result, ...)` with `CaseLiteral`.
///
/// Uses a pre-order approach for If-chains: the outermost If is processed
/// first so it can greedily consume the entire chain. After rewriting,
/// the transform recurses into the result subexpressions.
fn rewrite_if_chain(expr: &mut MirScalarExpr, column_types: &[ReprColumnType]) {
    // Try to rewrite the current node first (greedy top-down for If-chains).
    try_rewrite_if_chain(expr, column_types);

    // Then recurse into children.
    match expr {
        MirScalarExpr::Column(..) | MirScalarExpr::Literal(..) => {}
        MirScalarExpr::CallUnmaterializable(_) => {}
        MirScalarExpr::CallUnary { expr: inner, .. } => {
            rewrite_if_chain(inner, column_types);
        }
        MirScalarExpr::CallBinary { expr1, expr2, .. } => {
            rewrite_if_chain(expr1, column_types);
            rewrite_if_chain(expr2, column_types);
        }
        MirScalarExpr::CallVariadic { exprs, .. } => {
            for e in exprs {
                rewrite_if_chain(e, column_types);
            }
        }
        MirScalarExpr::If { cond, then, els } => {
            rewrite_if_chain(cond, column_types);
            rewrite_if_chain(then, column_types);
            rewrite_if_chain(els, column_types);
        }
    }
}

/// Attempts to rewrite a single `If` node as a `CaseLiteral`.
///
/// Returns without modification if the pattern doesn't match or
/// fewer than 2 arms are collected.
fn try_rewrite_if_chain(expr: &mut MirScalarExpr, column_types: &[ReprColumnType]) {
    // First pass: read-only scan to determine if the pattern matches with >= 2 arms.
    let arm_count = count_if_chain_arms(expr);
    if arm_count < 2 {
        return;
    }

    // Second pass: take the expression and dismantle it.
    let chain = std::mem::replace(expr, MirScalarExpr::literal_false());
    let (collected_cases, common, els) = collect_if_chain_arms(chain);

    let mut common = common.expect("common expr must be set when arm_count >= 2");
    let mut els = els;

    // Compute the return type as the union of all branch types and els type.
    let mut return_type: Option<ReprColumnType> = None;
    for (_, result) in &collected_cases {
        let t = result.typ(column_types);
        return_type = Some(match return_type {
            None => t,
            Some(prev) => prev.union(&t).unwrap_or(t),
        });
    }
    let els_type = els.typ(column_types);
    let return_type = match return_type {
        Some(prev) => prev.union(&els_type).unwrap_or(els_type),
        None => els_type,
    };
    let sql_return_type = SqlColumnType::from_repr(&return_type);

    // Recurse into subexpressions that may themselves contain rewritable If-chains.
    rewrite_if_chain(&mut common, column_types);
    rewrite_if_chain(&mut els, column_types);
    let cases_map: BTreeMap<Row, MirScalarExpr> = collected_cases
        .into_iter()
        .map(|(row, mut result_expr)| {
            rewrite_if_chain(&mut result_expr, column_types);
            (row, result_expr)
        })
        .collect();

    *expr = MirScalarExpr::CallUnary {
        func: UnaryFunc::CaseLiteral(mz_expr::func::CaseLiteral {
            cases: cases_map,
            els: Box::new(els),
            return_type: sql_return_type,
        }),
        expr: Box::new(common),
    };
}

/// Counts matching If-chain arms without modifying the expression.
fn count_if_chain_arms(expr: &MirScalarExpr) -> usize {
    let mut count = 0;
    let mut common_expr: Option<&MirScalarExpr> = None;
    let mut current = expr;

    loop {
        match current {
            MirScalarExpr::If { cond, then: _, els } => {
                if let Some((expr_side, _literal_row)) = peek_eq_literal(cond) {
                    match common_expr {
                        None => {
                            common_expr = Some(expr_side);
                        }
                        Some(existing) => {
                            if existing != expr_side {
                                break;
                            }
                        }
                    }
                    count += 1;
                    current = els;
                } else {
                    break;
                }
            }
            _ => break,
        }
    }

    count
}

/// Like `extract_eq_literal` but returns references instead of clones.
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

    if let Some(row) = peek_non_null_literal(expr1) {
        if !is_literal(expr2) {
            return Some((expr2.as_ref(), row));
        }
    }
    if let Some(row) = peek_non_null_literal(expr2) {
        if !is_literal(expr1) {
            return Some((expr1.as_ref(), row));
        }
    }
    None
}

/// Returns a reference to the `Row` if the expression is a non-NULL `Ok` literal.
fn peek_non_null_literal(expr: &MirScalarExpr) -> Option<&Row> {
    if let MirScalarExpr::Literal(Ok(row), _) = expr {
        if !row.unpack_first().is_null() {
            return Some(row);
        }
    }
    None
}

/// Walks an If-chain and collects `(literal_row, result_expr)` pairs.
///
/// The input `chain` is consumed and dismantled.
/// Returns `(cases, common_expr, els)`.
fn collect_if_chain_arms(
    chain: MirScalarExpr,
) -> (
    Vec<(Row, MirScalarExpr)>,
    Option<MirScalarExpr>,
    MirScalarExpr,
) {
    let mut cases = Vec::new();
    let mut common_expr: Option<MirScalarExpr> = None;
    let mut remaining = chain;

    loop {
        match remaining {
            MirScalarExpr::If { cond, then, els } => {
                if let Some((expr_side, literal_row)) = extract_eq_literal(&cond) {
                    match &common_expr {
                        None => {
                            common_expr = Some(expr_side);
                        }
                        Some(existing) => {
                            if *existing != expr_side {
                                remaining = MirScalarExpr::If { cond, then, els };
                                break;
                            }
                        }
                    }

                    // First occurrence of each literal wins (SQL CASE semantics).
                    if !cases
                        .iter()
                        .any(|(row, _): &(Row, MirScalarExpr)| *row == literal_row)
                    {
                        cases.push((literal_row, *then));
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

    (cases, common_expr, remaining)
}

/// Extracts `(non_literal_expr, literal_row)` from an `Eq(expr, literal)` condition.
fn extract_eq_literal(cond: &MirScalarExpr) -> Option<(MirScalarExpr, Row)> {
    let MirScalarExpr::CallBinary {
        func: BinaryFunc::Eq(_),
        expr1,
        expr2,
    } = cond
    else {
        return None;
    };

    if let Some(row) = peek_non_null_literal(expr1) {
        if !is_literal(expr2) {
            return Some((expr2.as_ref().clone(), row.clone()));
        }
    }
    if let Some(row) = peek_non_null_literal(expr2) {
        if !is_literal(expr1) {
            return Some((expr1.as_ref().clone(), row.clone()));
        }
    }
    None
}

/// Returns true if the expression is any `Literal`.
fn is_literal(expr: &MirScalarExpr) -> bool {
    matches!(expr, MirScalarExpr::Literal(..))
}

#[cfg(test)]
mod tests {
    use mz_expr::func::Eq;
    use mz_expr::{BinaryFunc, MirRelationExpr, MirScalarExpr, UnaryFunc};
    use mz_repr::{Datum, ReprColumnType, ReprRelationType, ReprScalarType};

    use super::*;

    /// Helper: build `Eq(lhs, rhs)` as a `MirScalarExpr`.
    fn eq(lhs: MirScalarExpr, rhs: MirScalarExpr) -> MirScalarExpr {
        MirScalarExpr::CallBinary {
            func: BinaryFunc::Eq(Eq),
            expr1: Box::new(lhs),
            expr2: Box::new(rhs),
        }
    }

    /// Helper: build `If(cond, then, els)`.
    fn if_then_else(cond: MirScalarExpr, then: MirScalarExpr, els: MirScalarExpr) -> MirScalarExpr {
        cond.if_then_else(then, els)
    }

    /// Helper: build an i64 literal.
    fn lit_i64(v: i64) -> MirScalarExpr {
        MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64)
    }

    /// Column reference.
    fn col(i: usize) -> MirScalarExpr {
        MirScalarExpr::column(i)
    }

    /// Wrap a scalar expression in a `Map` over a `Get` to allow applying the transform.
    fn wrap_in_map(scalar: MirScalarExpr) -> MirRelationExpr {
        MirRelationExpr::Map {
            input: Box::new(MirRelationExpr::constant(
                vec![vec![Datum::Int64(0)]],
                ReprRelationType::new(vec![ReprColumnType {
                    scalar_type: ReprScalarType::Int64,
                    nullable: false,
                }]),
            )),
            scalars: vec![scalar],
        }
    }

    /// Apply the CaseLiteralTransform to a relation and return the first scalar from the Map.
    fn apply_transform(scalar: MirScalarExpr) -> MirScalarExpr {
        let mut relation = wrap_in_map(scalar);
        let features = mz_repr::optimize::OptimizerFeatures::default();
        let typecheck_ctx = crate::typecheck::empty_typechecking_context();
        let mut df_meta = crate::dataflow::DataflowMetainfo::default();
        let mut transform_ctx =
            crate::TransformCtx::local(&features, &typecheck_ctx, &mut df_meta, None, None);
        crate::Transform::transform(&CaseLiteralTransform, &mut relation, &mut transform_ctx)
            .unwrap();
        match relation {
            MirRelationExpr::Map { scalars, .. } => scalars.into_iter().next().unwrap(),
            other => panic!("expected Map, got {other:?}"),
        }
    }

    /// Verify that the result is a CaseLiteral with the expected number of cases.
    fn assert_case_literal(expr: &MirScalarExpr, expected_cases: usize) {
        match expr {
            MirScalarExpr::CallUnary {
                func: UnaryFunc::CaseLiteral(cl),
                ..
            } => {
                assert_eq!(
                    cl.cases.len(),
                    expected_cases,
                    "expected {expected_cases} cases, got {}",
                    cl.cases.len()
                );
            }
            other => panic!("expected CaseLiteral, got {other:?}"),
        }
    }

    // Build a CASE-like If-chain: CASE #0 WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 0 END
    fn build_2_arm_chain() -> MirScalarExpr {
        if_then_else(
            eq(col(0), lit_i64(1)),
            lit_i64(10),
            if_then_else(eq(col(0), lit_i64(2)), lit_i64(20), lit_i64(0)),
        )
    }

    #[mz_ore::test]
    fn test_2_arm_case() {
        let expr = build_2_arm_chain();
        let result = apply_transform(expr);
        assert_case_literal(&result, 2);
    }

    #[mz_ore::test]
    fn test_multi_arm_case() {
        // CASE #0 WHEN 1 THEN 10 WHEN 2 THEN 20 WHEN 3 THEN 30
        //         WHEN 4 THEN 40 WHEN 5 THEN 50 ELSE 0 END
        let mut expr = lit_i64(0);
        for i in (1..=5).rev() {
            expr = if_then_else(eq(col(0), lit_i64(i)), lit_i64(i * 10), expr);
        }
        let result = apply_transform(expr);
        assert_case_literal(&result, 5);
    }

    #[mz_ore::test]
    fn test_single_arm_no_conversion() {
        // CASE #0 WHEN 1 THEN 10 ELSE 0 END — only 1 arm, should NOT convert.
        let expr = if_then_else(eq(col(0), lit_i64(1)), lit_i64(10), lit_i64(0));
        let result = apply_transform(expr);
        // Should still be an If, not a CaseLiteral.
        assert!(
            matches!(result, MirScalarExpr::If { .. }),
            "single arm should not be converted, got {result:?}"
        );
    }

    #[mz_ore::test]
    fn test_different_exprs_no_conversion() {
        // If(Eq(#0, 1), 10, If(Eq(#1, 2), 20, 0)) — different columns, should NOT convert.
        let expr = if_then_else(
            eq(col(0), lit_i64(1)),
            lit_i64(10),
            if_then_else(
                eq(MirScalarExpr::column(1), lit_i64(2)),
                lit_i64(20),
                lit_i64(0),
            ),
        );
        // This has two arms but different common expressions, so only one arm matches
        // before we see a different expr. That gives us < 2 arms → no conversion.
        let result = apply_transform(expr);
        assert!(
            matches!(result, MirScalarExpr::If { .. }),
            "different expressions should not be converted, got {result:?}"
        );
    }

    #[mz_ore::test]
    fn test_partial_extraction() {
        // 3 arms where first 2 match #0 and 3rd matches #1:
        // If(Eq(#0, 1), 10, If(Eq(#0, 2), 20, If(Eq(#1, 3), 30, 0)))
        // Should extract 2 arms for #0, with els = If(Eq(#1, 3), 30, 0).
        let inner = if_then_else(
            eq(MirScalarExpr::column(1), lit_i64(3)),
            lit_i64(30),
            lit_i64(0),
        );
        let expr = if_then_else(
            eq(col(0), lit_i64(1)),
            lit_i64(10),
            if_then_else(eq(col(0), lit_i64(2)), lit_i64(20), inner),
        );

        // We need 2 columns for this test.
        let mut relation = MirRelationExpr::Map {
            input: Box::new(MirRelationExpr::constant(
                vec![vec![Datum::Int64(0), Datum::Int64(0)]],
                ReprRelationType::new(vec![
                    ReprColumnType {
                        scalar_type: ReprScalarType::Int64,
                        nullable: false,
                    },
                    ReprColumnType {
                        scalar_type: ReprScalarType::Int64,
                        nullable: false,
                    },
                ]),
            )),
            scalars: vec![expr],
        };

        let features = mz_repr::optimize::OptimizerFeatures::default();
        let typecheck_ctx = crate::typecheck::empty_typechecking_context();
        let mut df_meta = crate::dataflow::DataflowMetainfo::default();
        let mut transform_ctx =
            crate::TransformCtx::local(&features, &typecheck_ctx, &mut df_meta, None, None);
        crate::Transform::transform(&CaseLiteralTransform, &mut relation, &mut transform_ctx)
            .unwrap();

        let result = match relation {
            MirRelationExpr::Map { scalars, .. } => scalars.into_iter().next().unwrap(),
            other => panic!("expected Map, got {other:?}"),
        };

        // Should be a CaseLiteral with 2 arms (#0 matches), and the els
        // should be an If (the #1 comparison).
        match &result {
            MirScalarExpr::CallUnary {
                func: UnaryFunc::CaseLiteral(cl),
                ..
            } => {
                assert_eq!(cl.cases.len(), 2);
                assert!(matches!(*cl.els, MirScalarExpr::If { .. }));
            }
            other => panic!("expected CaseLiteral, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn test_null_literal_skipped() {
        // If(Eq(#0, NULL::int64), 10, If(Eq(#0, 2), 20, If(Eq(#0, 3), 30, 0)))
        // The NULL arm should break the chain, leaving us with the whole thing
        // as an If since arm1 has NULL, so we get 0 valid arms before the break.
        // Actually: extract_eq_literal skips NULL literals, so arm1 doesn't match,
        // and we get 0 arms → no conversion.
        let null_lit = MirScalarExpr::literal(Ok(Datum::Null), ReprScalarType::Int64);
        let expr = if_then_else(
            eq(col(0), null_lit),
            lit_i64(10),
            if_then_else(
                eq(col(0), lit_i64(2)),
                lit_i64(20),
                if_then_else(eq(col(0), lit_i64(3)), lit_i64(30), lit_i64(0)),
            ),
        );
        let result = apply_transform(expr);
        // The NULL arm breaks the chain at the top level. The inner 2 arms
        // (comparing #0 to 2 and 3) should still be converted.
        // The result should be If(Eq(#0, NULL), 10, CaseLiteral(...))
        match &result {
            MirScalarExpr::If { els, .. } => {
                assert_case_literal(els, 2);
            }
            other => panic!("expected If with CaseLiteral in els, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn test_eval_basic() {
        // Verify that evaluating the CaseLiteral produces correct results.
        let expr = build_2_arm_chain();
        let result = apply_transform(expr);

        let arena = mz_repr::RowArena::new();

        // Input = 1 → output should be 10
        let out = result.eval(&[Datum::Int64(1)], &arena).unwrap();
        assert_eq!(out, Datum::Int64(10));

        // Input = 2 → output should be 20
        let out = result.eval(&[Datum::Int64(2)], &arena).unwrap();
        assert_eq!(out, Datum::Int64(20));

        // Input = 99 → output should be 0 (els)
        let out = result.eval(&[Datum::Int64(99)], &arena).unwrap();
        assert_eq!(out, Datum::Int64(0));

        // Input = NULL → output should be 0 (els, since NULL = x is falsy)
        let out = result.eval(&[Datum::Null], &arena).unwrap();
        assert_eq!(out, Datum::Int64(0));
    }
}
