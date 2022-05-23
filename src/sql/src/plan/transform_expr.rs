// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformations of SQL IR, before decorrelation.

use std::collections::BTreeMap;
use std::mem;

use once_cell::sync::Lazy;

use mz_expr::func;
use mz_repr::{ColumnType, RelationType, ScalarType};

use crate::plan::expr::{
    AbstractExpr, AggregateFunc, BinaryFunc, HirRelationExpr, HirScalarExpr, UnaryFunc,
};

/// Rewrites predicates that contain subqueries so that the subqueries
/// appear in their own later predicate when possible.
///
/// For example, this function rewrites this expression
///
/// ```text
/// Filter {
///     predicates: [a = b AND EXISTS (<subquery 1>) AND c = d AND (<subquery 2>) = e]
/// }
/// ```
///
/// like so:
///
/// ```text
/// Filter {
///     predicates: [
///         a = b AND c = d,
///         EXISTS (<subquery>),
///         (<subquery 2>) = e,
///     ]
/// }
/// ```
///
/// The rewrite causes decorrelation to incorporate prior predicates into
/// the outer relation upon which the subquery is evaluated. In the above
/// rewritten example, the `EXISTS (<subquery>)` will only be evaluated for
/// outer rows where `a = b AND c = d`. The second subquery, `(<subquery 2>)
/// = e`, will be further restricted to outer rows that match `A = b AND c =
/// d AND EXISTS(<subquery>)`. This can vastly reduce the cost of the
/// subquery, especially when the original conjunction contains join keys.
pub fn split_subquery_predicates(expr: &mut HirRelationExpr) {
    fn walk_relation(expr: &mut HirRelationExpr) {
        expr.visit_mut(0, &mut |expr, _| match expr {
            HirRelationExpr::Map { scalars, .. } => {
                for scalar in scalars {
                    walk_scalar(scalar);
                }
            }
            HirRelationExpr::CallTable { exprs, .. } => {
                for expr in exprs {
                    walk_scalar(expr);
                }
            }
            HirRelationExpr::Filter { predicates, .. } => {
                let mut subqueries = vec![];
                for predicate in &mut *predicates {
                    walk_scalar(predicate);
                    extract_conjuncted_subqueries(predicate, &mut subqueries);
                }
                // TODO(benesch): we could be smarter about the order in which
                // we emit subqueries. At the moment we just emit in the order
                // we discovered them, but ideally we'd emit them in an order
                // that accounted for their cost/selectivity. E.g., low-cost,
                // high-selectivity subqueries should go first.
                for subquery in subqueries {
                    predicates.push(subquery);
                }
            }
            _ => (),
        });
    }

    fn walk_scalar(expr: &mut HirScalarExpr) {
        expr.visit_mut(&mut |expr| match expr {
            HirScalarExpr::Exists(input) | HirScalarExpr::Select(input) => walk_relation(input),
            _ => (),
        })
    }

    fn contains_subquery(expr: &HirScalarExpr) -> bool {
        let mut found = false;
        expr.visit(&mut |expr| match expr {
            HirScalarExpr::Exists(_) | HirScalarExpr::Select(_) => found = true,
            _ => (),
        });
        found
    }

    /// Extracts subqueries from a conjunction into `out`.
    ///
    /// For example, given an expression like
    ///
    /// ```text
    /// a = b AND EXISTS (<subquery 1>) AND c = d AND (<subquery 2>) = e
    /// ```
    ///
    /// this function rewrites the expression to
    ///
    /// ```text
    /// a = b AND true AND c = d AND true
    /// ```
    ///
    /// and returns the expression fragments `EXISTS (<subquery 1>)` and
    //// `(<subquery 2>) = e` in the `out` vector.
    fn extract_conjuncted_subqueries(expr: &mut HirScalarExpr, out: &mut Vec<HirScalarExpr>) {
        match expr {
            HirScalarExpr::CallBinary {
                func: BinaryFunc::And,
                expr1,
                expr2,
            } => {
                extract_conjuncted_subqueries(expr1, out);
                extract_conjuncted_subqueries(expr2, out);
            }
            expr if contains_subquery(expr) => {
                out.push(mem::replace(expr, HirScalarExpr::literal_true()))
            }
            _ => (),
        }
    }

    walk_relation(expr)
}

/// Rewrites quantified comparisons into simpler EXISTS operators.
///
/// Note that this transformation is only valid when the expression is
/// used in a context where the distinction between `FALSE` and `NULL`
/// is immaterial, e.g., in a `WHERE` clause or a `CASE` condition, or
/// when the inputs to the comparison are non-nullable. This function is careful
/// to only apply the transformation when it is valid to do so.
///
/// WHERE (SELECT any(<pred>) FROM <rel>)
/// =>
/// WHERE EXISTS(SELECT * FROM <rel> WHERE <pred>)
///
/// WHERE (SELECT all(<pred>) FROM <rel>)
/// =>
/// WHERE NOT EXISTS(SELECT * FROM <rel> WHERE (NOT <pred>) OR <pred> IS NULL)
///
/// See Section 3.5 of "Execution Strategies for SQL Subqueries" by
/// M. Elhemali, et al.
pub fn try_simplify_quantified_comparisons(expr: &mut HirRelationExpr) {
    fn walk_relation(expr: &mut HirRelationExpr, outers: &[RelationType]) {
        match expr {
            HirRelationExpr::Map { scalars, input } => {
                walk_relation(input, outers);
                let mut outers = outers.to_vec();
                outers.insert(0, input.typ(&outers, &NO_PARAMS));
                for scalar in scalars {
                    walk_scalar(scalar, &outers, false);
                    let (inner, outers) = outers
                        .split_first_mut()
                        .expect("outers known to have at least one element");
                    let scalar_type = scalar.typ(&outers, inner, &NO_PARAMS);
                    inner.column_types.push(scalar_type);
                }
            }
            HirRelationExpr::Filter { predicates, input } => {
                walk_relation(input, outers);
                let mut outers = outers.to_vec();
                outers.insert(0, input.typ(&outers, &NO_PARAMS));
                for pred in predicates {
                    walk_scalar(pred, &outers, true);
                }
            }
            HirRelationExpr::CallTable { exprs, .. } => {
                let mut outers = outers.to_vec();
                outers.insert(0, RelationType::empty());
                for scalar in exprs {
                    walk_scalar(scalar, &outers, false);
                }
            }
            HirRelationExpr::Join { left, right, .. } => {
                walk_relation(left, outers);
                let mut outers = outers.to_vec();
                outers.insert(0, left.typ(&outers, &NO_PARAMS));
                walk_relation(right, &outers);
            }
            expr => {
                let _ = expr.visit1_mut(0, &mut |expr, _| -> Result<(), ()> {
                    walk_relation(expr, outers);
                    Ok(())
                });
            }
        }
    }

    fn walk_scalar(expr: &mut HirScalarExpr, outers: &[RelationType], mut in_filter: bool) {
        expr.visit_mut_pre(&mut |e| match e {
            HirScalarExpr::Exists(input) => walk_relation(input, outers),
            HirScalarExpr::Select(input) => {
                walk_relation(input, outers);

                // We're inside of a `(SELECT ...)` subquery. Now let's see if
                // it has the form `(SELECT <any|all>(...) FROM <input>)`.
                // Ideally we could do this with one pattern, but Rust's pattern
                // matching engine is not powerful enough, so we have to do this
                // in stages; the early returns avoid brutal nesting.

                let (func, expr, input) = match &mut **input {
                    HirRelationExpr::Reduce {
                        group_key,
                        aggregates,
                        input,
                        expected_group_size: _,
                    } if group_key.is_empty() && aggregates.len() == 1 => {
                        let agg = &mut aggregates[0];
                        (&agg.func, &mut agg.expr, input)
                    }
                    _ => return,
                };

                if !in_filter && column_type(outers, input, expr).nullable {
                    // Unless we're directly inside of a WHERE, this
                    // transformation is only valid if the expression involved
                    // is non-nullable.
                    return;
                }

                match func {
                    AggregateFunc::Any => {
                        // Found `(SELECT any(<expr>) FROM <input>)`. Rewrite to
                        // `EXISTS(SELECT 1 FROM <input> WHERE <expr>)`.
                        *e = input.take().filter(vec![expr.take()]).exists();
                    }
                    AggregateFunc::All => {
                        // Found `(SELECT all(<expr>) FROM <input>)`. Rewrite to
                        // `NOT EXISTS(SELECT 1 FROM <input> WHERE NOT <expr> OR <expr> IS NULL)`.
                        //
                        // Note that negation of <expr> alone is insufficient.
                        // Consider that `WHERE <pred>` filters out rows if
                        // `<pred>` is false *or* null. To invert the test, we
                        // need `NOT <pred> OR <pred> IS NULL`.
                        let expr = expr.take();
                        let filter = expr
                            .clone()
                            .call_unary(UnaryFunc::Not(func::Not))
                            .call_binary(
                                expr.call_unary(UnaryFunc::IsNull(func::IsNull)),
                                BinaryFunc::Or,
                            );
                        *e = input
                            .take()
                            .filter(vec![filter])
                            .exists()
                            .call_unary(UnaryFunc::Not(func::Not));
                    }
                    _ => (),
                }
            }
            _ => {
                // As soon as we see *any* scalar expression, we are no longer
                // directly inside of a filter.
                in_filter = false;
            }
        })
    }

    walk_relation(expr, &[])
}

/// An empty parameter type map.
///
/// These transformations are expected to run after parameters are bound, so
/// there is no need to provide any parameter type information.
static NO_PARAMS: Lazy<BTreeMap<usize, ScalarType>> = Lazy::new(BTreeMap::new);

fn column_type(
    outers: &[RelationType],
    inner: &HirRelationExpr,
    expr: &HirScalarExpr,
) -> ColumnType {
    let inner_type = inner.typ(&outers, &NO_PARAMS);
    expr.typ(&outers, &inner_type, &NO_PARAMS)
}
