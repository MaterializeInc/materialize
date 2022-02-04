// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines the [`RejectedNulls`] attribute.
//!
//! The attribute value is a set of column references associated with
//! each `QueryBox`. If any of the references is `NULL`, there is at
//! least one predicate in that box that will be evaluated to `NULL`
//! or `FALSE` (that is, a row with that column will be filtered
//! away). For boxes without predicates, the attribute value is
//! always the empty set.
//!
//! Besides "predicate p rejects nulls in a set of columns C", in the
//! literature this property is also stated as "predicate p is strong
//! with respect to C".

use super::propagated_nulls::propagated_nulls;
use crate::query_model::attribute::core::{Attribute, AttributeKey};
use crate::query_model::model::{
    BoxId, BoxScalarExpr, BoxType, ColumnReference, Model, QuantifierType,
};
use std::collections::HashSet;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct RejectedNulls;

impl AttributeKey for RejectedNulls {
    type Value = HashSet<ColumnReference>;
}

impl Attribute for RejectedNulls {
    fn attr_id(&self) -> &'static str {
        "RejectedNulls"
    }

    fn requires(&self) -> Vec<Box<dyn Attribute>> {
        vec![]
    }

    fn derive(&self, model: &mut Model, box_id: BoxId) {
        let mut r#box = model.get_mut_box(box_id);
        let mut value = HashSet::new();

        match r#box.box_type {
            BoxType::Select(ref select) => {
                for p in select.predicates.iter() {
                    rejected_nulls(p, &mut value);
                }
            }
            BoxType::OuterJoin(ref outerjoin) => {
                for p in outerjoin.predicates.iter() {
                    rejected_nulls(p, &mut value);
                }
                // By definition, preserved sides in outer joins don't filter
                // anything, so columns from the corresponding sides cannot
                // reject nulls and need to be removed from the result value.
                for q in r#box
                    .input_quantifiers()
                    .filter(|q| q.quantifier_type == QuantifierType::PreservedForeach)
                {
                    value.retain(|c| c.quantifier_id != q.id);
                }
            }
            _ => (),
        }

        r#box.attributes.set::<RejectedNulls>(value);
    }
}

/// Returns all columns that *must* be non-NULL for the boolean `expr`
/// to be `NULL` or `FALSE`.
///
/// An expression `expr` rejects nulls in a set of column references
/// `C` if it evaluates to either `FALSE` or `NULL` whenever some
/// `c` in `C` is null.
///
/// An expression `expr` propagates nulls in a set of column references
/// `C` if it evaluates to `NULL` whenever some `c` in `C` is null.
///
/// Consequently, results returned by [`propagated_nulls`] must be
/// included in [`rejected_nulls`].
///
/// Unfortuantely, boolean functions such as "and" and "or" are not
/// propagating nulls in their inputs, but we still need to handle
/// them here, as they are used quite frequently in predicates.
/// The procedure for doing this is derived below.
///
/// Observe the truth values for the following terms:
///
/// For `AND(A, B)`:
///
/// |   | F | N | T |
/// |   |:-:|:-:|:-:|
/// | F | F | F | F |
/// | N | F | N | N |
/// | T | F | N | T |
///
/// For `OR(A, B)`:
///
/// |   | F | N | T |
/// |   |:-:|:-:|:-:|
/// | F | F | N | T |
/// | N | N | N | T |
/// | T | T | T | T |
///
/// For `NOT(AND(A, B))`:
///
/// |   | F | N | T |
/// |   |:-:|:-:|:-:|
/// | F | T | T | T |
/// | N | T | N | N |
/// | T | T | N | F |
///
/// For `NOT(OR(A, B))`:
///
/// |   | F | N | T |
/// |   |:-:|:-:|:-:|
/// | F | T | N | F |
/// | N | N | N | F |
/// | T | F | F | F |
///
/// Based on the above truth tables, we can establish the following
/// statements are always true:
/// 1. If either `A` or `B` rejects nulls in `C`,
///    then `AND(A, B)` rejects nulls in `C`.
/// 2. If both `A` and `B` reject nulls in `C`,
///    then `OR(A, B)` rejects nulls in `C`.
/// 3. If both `A` and `B` propagate nulls in `C`,
///    then `NOT(AND(A, B))` rejects nulls in `C`.
/// 4. If either `A` or `B` propagates nulls in `C`,
///    then `NOT(OR(A, B))` rejects nulls in `C`.
///
/// Based on the above statements, the algorithm implemented by
/// this function can be described by the following pseudo-code:
///
/// ```text
/// def rejected_nulls(expr: Expr, sign: bool = true) -> Set[Expr]:
///     match expr:
///         case NOT(ISNULL(c)):
///             { c }
///         case NOT(expr):
///             rejected_nulls(expr, !sign)
///         case AND(lhs, rhs):
///             if sign > 0:
///                 rejected_nulls(lhs, sign) ∪ rejected_nulls(rhs, sign)
///             else:
///                 propagated_nulls(lhs) ∩ propagated_nulls(rhs)
///         case OR(lhs, rhs):
///             if sign > 0:
///                 rejected_nulls(lhs, sign) ∩ rejected_nulls(rhs, sign)
///             else:
///                 propagated_nulls(lhs) ∪ propagated_nulls(rhs)
///         case expr:
///             propagated_nulls(expr)
/// ```
pub(crate) fn rejected_nulls(expr: &BoxScalarExpr, set: &mut HashSet<ColumnReference>) {
    /// Define an inner function needed in order to pass around the `sign`.
    fn rejected_nulls(expr: &BoxScalarExpr, sign: bool) -> HashSet<ColumnReference> {
        ore::stack::maybe_grow(|| {
            if let Some(c) = case_not_isnull(expr) {
                HashSet::from([c.clone()])
            } else if let Some(expr) = case_not(expr) {
                rejected_nulls(expr, !sign)
            } else if let Some((lhs, rhs)) = case_and(expr) {
                if sign {
                    union(rejected_nulls(lhs, sign), rejected_nulls(rhs, sign))
                } else {
                    intersect(propagated_nulls(lhs), propagated_nulls(rhs))
                }
            } else if let Some((lhs, rhs)) = case_or(expr) {
                if sign {
                    intersect(rejected_nulls(lhs, sign), rejected_nulls(rhs, sign))
                } else {
                    union(propagated_nulls(lhs), propagated_nulls(rhs))
                }
            } else {
                propagated_nulls(expr)
            }
        })
    }

    set.extend(rejected_nulls(expr, true))
}

/// Computes the union of two sets, consuming both sides
/// and mutating and returning `lhs`.
fn union<T>(mut lhs: HashSet<T>, rhs: HashSet<T>) -> HashSet<T>
where
    T: Clone + Eq + std::hash::Hash,
{
    lhs.extend(rhs);
    lhs
}

/// Computes the intersection of two sets, consuming both sides
/// and mutating and returning `lhs`.
fn intersect<T>(mut lhs: HashSet<T>, rhs: HashSet<T>) -> HashSet<T>
where
    T: Clone + Eq + std::hash::Hash,
{
    lhs.retain(|item| rhs.contains(item));
    lhs
}

/// Active pattern match for `NOT(ISNULL(c))` fragments.
fn case_not_isnull(expr: &BoxScalarExpr) -> Option<&ColumnReference> {
    use BoxScalarExpr::*;

    if let CallUnary {
        func: expr::UnaryFunc::Not(expr::func::Not),
        expr,
    } = expr
    {
        if let CallUnary {
            func: expr::UnaryFunc::IsNull(expr::func::IsNull),
            expr,
        } = &**expr
        {
            if let ColumnReference(c) = &**expr {
                return Some(c);
            }
        }
    }

    None
}

/// Active pattern match for `NOT(expr)` fragments.
fn case_not(expr: &BoxScalarExpr) -> Option<&BoxScalarExpr> {
    use BoxScalarExpr::*;

    if let CallUnary {
        func: expr::UnaryFunc::Not(expr::func::Not),
        expr,
    } = expr
    {
        return Some(expr);
    }

    None
}

/// Active pattern match for `NOT(expr)` fragments.
fn case_or(expr: &BoxScalarExpr) -> Option<(&BoxScalarExpr, &BoxScalarExpr)> {
    use BoxScalarExpr::*;

    if let CallBinary {
        func: expr::BinaryFunc::Or,
        expr1,
        expr2,
    } = expr
    {
        return Some((expr1, expr2));
    }

    None
}

/// Active pattern match for `NOT(expr)` fragments.
fn case_and(expr: &BoxScalarExpr) -> Option<(&BoxScalarExpr, &BoxScalarExpr)> {
    use BoxScalarExpr::*;

    if let CallBinary {
        func: expr::BinaryFunc::And,
        expr1,
        expr2,
    } = expr
    {
        return Some((expr1, expr2));
    }

    None
}
