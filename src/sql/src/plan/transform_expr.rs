// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformations of SQL IR, before decorrelation.

use std::mem;

use crate::plan::expr::{BinaryFunc, RelationExpr, ScalarExpr};

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
pub fn split_subquery_predicates(expr: &mut RelationExpr) {
    fn walk_relation(expr: &mut RelationExpr) {
        expr.visit_mut(&mut |expr| match expr {
            RelationExpr::Map { scalars, .. } => {
                for scalar in scalars {
                    walk_scalar(scalar);
                }
            }
            RelationExpr::FlatMap { exprs, .. } => {
                for expr in exprs {
                    walk_scalar(expr);
                }
            }
            RelationExpr::Filter { predicates, .. } => {
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
        })
    }

    fn walk_scalar(expr: &mut ScalarExpr) {
        expr.visit_mut(&mut |expr| match expr {
            ScalarExpr::Exists(input) | ScalarExpr::Select(input) => walk_relation(input),
            _ => (),
        })
    }

    fn contains_subquery(expr: &ScalarExpr) -> bool {
        let mut found = false;
        expr.visit(&mut |expr| match expr {
            ScalarExpr::Exists(_) | ScalarExpr::Select(_) => found = true,
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
    fn extract_conjuncted_subqueries(expr: &mut ScalarExpr, out: &mut Vec<ScalarExpr>) {
        match expr {
            ScalarExpr::CallBinary {
                func: BinaryFunc::And,
                expr1,
                expr2,
            } => {
                extract_conjuncted_subqueries(expr1, out);
                extract_conjuncted_subqueries(expr2, out);
            }
            expr if contains_subquery(expr) => {
                out.push(mem::replace(expr, ScalarExpr::literal_true()))
            }
            _ => (),
        }
    }

    walk_relation(expr)
}
