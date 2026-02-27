// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rewrites chains of `If(Eq(expr, literal), result, If(...))` into
//! `CallVariadic { func: CaseLiteral { lookup, return_type }, exprs }` for
//! O(log n) evaluation via `BTreeMap` lookup.
//!
//! Uses the `ReprRelationType` analysis to obtain column types in O(n),
//! avoiding repeated `input.typ()` calls. Each scalar is then visited
//! bottom-up so inner CaseLiterals are created first, then outer If nodes
//! fold into them.

use std::collections::BTreeMap;

use itertools::Itertools;
use mz_expr::visit::Visit;
use mz_expr::{BinaryFunc, MirRelationExpr, MirScalarExpr, VariadicFunc};
use mz_repr::{ReprColumnType, Row, SqlColumnType};

use crate::TransformCtx;
use crate::analysis::{DerivedBuilder, ReprRelationType};

/// Rewrites If-chains matching a single expression against literals
/// into a `CaseLiteral` variadic function with `BTreeMap` lookup.
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
        ctx: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        // Pre-compute column types for all nodes in a single pass.
        let mut builder = DerivedBuilder::new(ctx.features);
        builder.require(ReprRelationType);
        let derived = builder.visit(&*relation);

        let mut todo = vec![(&mut *relation, derived.as_view())];
        while let Some((expr, view)) = todo.pop() {
            match expr {
                MirRelationExpr::Map { scalars, .. } => {
                    // Use the output type (includes scalars' types).
                    let output_type: &Vec<ReprColumnType> = view
                        .value::<ReprRelationType>()
                        .expect("ReprRelationType required")
                        .as_ref()
                        .unwrap();
                    let input_arity = output_type.len() - scalars.len();
                    for (index, scalar) in scalars.iter_mut().enumerate() {
                        Self::rewrite_scalar(scalar, &output_type[..input_arity + index])?;
                    }
                }
                MirRelationExpr::Filter { predicates, .. } => {
                    let input_type: &Vec<ReprColumnType> = view
                        .last_child()
                        .value::<ReprRelationType>()
                        .expect("ReprRelationType required")
                        .as_ref()
                        .unwrap();
                    for predicate in predicates.iter_mut() {
                        Self::rewrite_scalar(predicate, input_type)?;
                    }
                }
                MirRelationExpr::Reduce { aggregates, .. } => {
                    let input_type: &Vec<ReprColumnType> = view
                        .last_child()
                        .value::<ReprRelationType>()
                        .expect("ReprRelationType required")
                        .as_ref()
                        .unwrap();
                    for agg in aggregates.iter_mut() {
                        Self::rewrite_scalar(&mut agg.expr, input_type)?;
                    }
                }
                MirRelationExpr::FlatMap { exprs, .. } => {
                    let input_type: &Vec<ReprColumnType> = view
                        .last_child()
                        .value::<ReprRelationType>()
                        .expect("ReprRelationType required")
                        .as_ref()
                        .unwrap();
                    for e in exprs.iter_mut() {
                        Self::rewrite_scalar(e, input_type)?;
                    }
                }
                _ => {}
            }
            todo.extend(expr.children_mut().rev().zip_eq(view.children_rev()));
        }

        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl CaseLiteralTransform {
    /// Rewrites a scalar expression tree bottom-up, replacing If-chains of
    /// `If(Eq(common_expr, literal), result, ...)` with `CaseLiteral`.
    fn rewrite_scalar(
        expr: &mut MirScalarExpr,
        column_types: &[ReprColumnType],
    ) -> Result<(), crate::TransformError> {
        expr.try_visit_mut_post(&mut |node: &mut MirScalarExpr| {
            try_fold_into_case_literal(node);
            try_create_case_literal(node, column_types);
            Ok(())
        })
    }
}

/// Fold rule: if node is `If(Eq(x, lit), res, CallVariadic(CaseLiteral{..}, [x, ...]))`
/// where the CaseLiteral's input (`exprs[0]`) structurally equals `x` and `lit` is
/// not already in `lookup`, insert `res` into the existing CaseLiteral.
fn try_fold_into_case_literal(expr: &mut MirScalarExpr) {
    let MirScalarExpr::If { cond, then, els } = expr else {
        return;
    };
    let Some((common_expr, literal_row)) = peek_eq_literal(cond) else {
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
    if exprs[0] != *common_expr {
        return;
    }

    // Don't fold if the literal is already present (first occurrence wins per SQL CASE).
    if cl.lookup.contains_key(literal_row) {
        return;
    }

    // Insert: push `then` before `els` (which is the last element), and update lookup.
    let new_idx = exprs.len() - 1;
    let then_expr = std::mem::replace(then.as_mut(), MirScalarExpr::literal_false());
    // Insert the new result before the fallback (last position).
    exprs.insert(new_idx, then_expr);
    cl.lookup.insert(literal_row.clone(), new_idx);

    // Replace the If with the CaseLiteral.
    let inner = std::mem::replace(els.as_mut(), MirScalarExpr::literal_false());
    *expr = inner;
}

/// Chain-walk rule: if node is an If-chain with >= 2 consecutive arms matching
/// `Eq(same_expr, literal)`, create a new CaseLiteral.
fn try_create_case_literal(expr: &mut MirScalarExpr, column_types: &[ReprColumnType]) {
    let arm_count = count_if_chain_arms(expr);
    if arm_count < 2 {
        return;
    }

    // Take the expression and dismantle it.
    let chain = std::mem::replace(expr, MirScalarExpr::literal_false());
    let (collected_cases, common, els) = collect_if_chain_arms(chain);

    let common = common.expect("common expr must be set when arm_count >= 2");
    let els = els;

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

    // Build the exprs vector: [input, result1, result2, ..., els]
    let mut exprs = Vec::with_capacity(collected_cases.len() + 2);
    exprs.push(common);
    let mut lookup = BTreeMap::new();
    for (row, result_expr) in collected_cases {
        let idx = exprs.len();
        lookup.insert(row, idx);
        exprs.push(result_expr);
    }
    exprs.push(els);

    *expr = MirScalarExpr::CallVariadic {
        func: VariadicFunc::CaseLiteral(mz_expr::func::CaseLiteral {
            lookup,
            return_type: sql_return_type,
        }),
        exprs,
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
    use mz_expr::{BinaryFunc, MirRelationExpr, MirScalarExpr, VariadicFunc};
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
        let mut features = mz_repr::optimize::OptimizerFeatures::default();
        features.enable_case_literal_transform = true;
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
            MirScalarExpr::CallVariadic {
                func: VariadicFunc::CaseLiteral(cl),
                ..
            } => {
                assert_eq!(
                    cl.lookup.len(),
                    expected_cases,
                    "expected {expected_cases} cases, got {}",
                    cl.lookup.len()
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
        // With bottom-up, the inner chain becomes a CaseLiteral first.
        // Then the outer If(Eq(#0, NULL), 10, CaseLiteral) has a CaseLiteral
        // in els, but the cond is Eq(#0, NULL) which is not a valid literal
        // (NULL is skipped), so the fold rule doesn't fire.
        // Result: If(Eq(#0, NULL), 10, CaseLiteral(...))
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
