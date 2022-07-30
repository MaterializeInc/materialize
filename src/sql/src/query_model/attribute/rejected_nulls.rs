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
/// Unfortunately, boolean functions such as "and" and "or" are not
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
        mz_ore::stack::maybe_grow(|| {
            if let Some(c) = case_not_isnull(expr) {
                HashSet::from([c.clone()])
            } else if let Some(expr) = case_not(expr) {
                rejected_nulls(expr, !sign)
            } else if let Some(exprs) = case_and(expr) {
                if sign {
                    union_variadic(exprs.iter().map(|e| rejected_nulls(e, sign)).collect())
                } else {
                    intersect_variadic(exprs.iter().map(propagated_nulls).collect())
                }
            } else if let Some(exprs) = case_or(expr) {
                if sign {
                    intersect_variadic(exprs.iter().map(|e| rejected_nulls(e, sign)).collect())
                } else {
                    union_variadic(exprs.iter().map(propagated_nulls).collect())
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

/// Computes the union of a vector of sets.
fn union_variadic<T>(sets: Vec<HashSet<T>>) -> HashSet<T>
where
    T: Clone + Eq + std::hash::Hash,
{
    sets.into_iter().reduce(|s1, s2| union(s1, s2)).unwrap()
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

/// Computes the intersection of a vector of sets.
fn intersect_variadic<T>(sets: Vec<HashSet<T>>) -> HashSet<T>
where
    T: Clone + Eq + std::hash::Hash,
{
    sets.into_iter().reduce(|s1, s2| intersect(s1, s2)).unwrap()
}

/// Active pattern match for `NOT(ISNULL(c))` fragments.
fn case_not_isnull(expr: &BoxScalarExpr) -> Option<&ColumnReference> {
    use BoxScalarExpr::*;

    if let CallUnary {
        func: mz_expr::UnaryFunc::Not(mz_expr::func::Not),
        expr,
    } = expr
    {
        if let CallUnary {
            func: mz_expr::UnaryFunc::IsNull(mz_expr::func::IsNull),
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
        func: mz_expr::UnaryFunc::Not(mz_expr::func::Not),
        expr,
    } = expr
    {
        return Some(expr);
    }

    None
}

/// Active pattern match for `expr1 OR expr2 OR ...` fragments.
fn case_or(expr: &BoxScalarExpr) -> Option<&Vec<BoxScalarExpr>> {
    use BoxScalarExpr::*;

    if let CallVariadic {
        func: mz_expr::VariadicFunc::Or,
        exprs,
    } = expr
    {
        return Some(exprs);
    }

    None
}

/// Active pattern match for `expr1 AND expr2 AND ...` fragments.
fn case_and(expr: &BoxScalarExpr) -> Option<&Vec<BoxScalarExpr>> {
    use BoxScalarExpr::*;

    if let CallVariadic {
        func: mz_expr::VariadicFunc::And,
        exprs,
    } = expr
    {
        return Some(exprs);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_model::model::*;
    use crate::query_model::test::util::*;

    #[test]
    fn test_select_1() {
        let mut model = Model::default();
        let g_id = add_get(&mut model);
        let b_id = add_select(&mut model, g_id, |input| {
            vec![
                // P0: (#0 - #1) + (#2 - #3)
                exp::add(
                    exp::sub(exp::cref(input, 0), exp::cref(input, 1)), // {#0, #1}
                    exp::sub(exp::cref(input, 2), exp::cref(input, 3)), // {#2, #3}
                ), // {#0, #1, #2, #3}
            ]
        });

        assert_derived_attribute(&mut model, b_id, |r#box| {
            HashSet::from([
                cref(input(&r#box, 0), 0),
                cref(input(&r#box, 0), 1),
                cref(input(&r#box, 0), 2),
                cref(input(&r#box, 0), 3),
            ]) // {#0, #1, #2, #3}
        });
    }

    #[test]
    fn test_select_2() {
        let mut model = Model::default();
        let g_id = add_get(&mut model);
        let b_id = add_select(&mut model, g_id, |input| {
            vec![
                // P0: (#0 > #1) || (#2 < #3)
                exp::and(
                    exp::gt(exp::cref(input, 0), exp::cref(input, 1)), // {#0, #1}
                    exp::lt(exp::cref(input, 2), exp::cref(input, 3)), // {#2, #3}
                ), // {#0, #1, #2, #3}
            ]
        });

        assert_derived_attribute(&mut model, b_id, |r#box| {
            HashSet::from([
                cref(input(&r#box, 0), 0),
                cref(input(&r#box, 0), 1),
                cref(input(&r#box, 0), 2),
                cref(input(&r#box, 0), 3),
            ]) // {#0, #1, #2, #3}
        });
    }

    #[test]
    fn test_select_3() {
        let mut model = Model::default();
        let g_id = add_get(&mut model);
        let b_id = add_select(&mut model, g_id, |input| {
            vec![
                // P0: OR(NOT(OR(#0 < #1, #1 < #2)), AND(!isnull(#1), AND(#0 = #2, #2 = #3))
                exp::or(
                    exp::not(exp::or(
                        exp::lt(exp::cref(input, 0), exp::cref(input, 1)), // { #0, #1 }
                        exp::lt(exp::cref(input, 1), exp::cref(input, 2)), // { #1, #2 }
                    )), // { #0, #1, #2 }
                    exp::and(
                        exp::not(exp::isnull(exp::cref(input, 1))), // { #1 }
                        exp::and(
                            exp::eq(exp::cref(input, 0), exp::cref(input, 2)), // { #0, #2 }
                            exp::eq(exp::cref(input, 2), exp::cref(input, 3)), // { #2, #3 }
                        ), // { #0, #2, #3 }
                    ), // { #0, #1, #2, #3 }
                ), // { #0, #1, #2 }
                // P1: !isnull(#3)
                exp::not(exp::isnull(exp::cref(input, 3))), // { #3 }
            ]
        });

        assert_derived_attribute(&mut model, b_id, |r#box| {
            HashSet::from([
                cref(input(&r#box, 0), 0),
                cref(input(&r#box, 0), 1),
                cref(input(&r#box, 0), 2),
                cref(input(&r#box, 0), 3),
            ]) // {#0, #1, #2, #3}
        });
    }

    #[test]
    fn test_select_4() {
        let mut model = Model::default();
        let g_id = add_get(&mut model);
        let b_id = add_select(&mut model, g_id, |input| {
            vec![
                // P0: AND(NOT(AND(#0 < #1, #1 < #2)), OR(!isnull(#2), OR(#0 = #2, #2 = #3))
                exp::and(
                    exp::not(exp::and(
                        exp::lt(exp::cref(input, 0), exp::cref(input, 1)), // { #0, #1 }
                        exp::lt(exp::cref(input, 1), exp::cref(input, 2)), // { #1, #2 }
                    )), // { #1 }
                    exp::or(
                        exp::not(exp::isnull(exp::cref(input, 2))), // { #2 }
                        exp::or(
                            exp::eq(exp::cref(input, 0), exp::cref(input, 2)), // { #0, #2 }
                            exp::eq(exp::cref(input, 2), exp::cref(input, 3)), // { #2, #3 }
                        ), // { #2 }
                    ), // { #2 }
                ), // { #1, #2 }
                // P1: !isnull(#3)
                exp::not(exp::isnull(exp::cref(input, 3))), // { #3 }
            ]
        });

        assert_derived_attribute(&mut model, b_id, |r#box| {
            HashSet::from([
                cref(input(&r#box, 0), 1),
                cref(input(&r#box, 0), 2),
                cref(input(&r#box, 0), 3),
            ]) // {#0, #2, #3}
        });
    }

    #[test]
    fn test_left_outer_join_1() {
        let mut model = Model::default();
        let g_id = add_get(&mut model);
        let b_id = add_left_outer_join(&mut model, g_id, g_id, |lhs, rhs| {
            vec![
                // P0: (lhs.#0 > rhs.#0)
                exp::gt(exp::cref(lhs, 0), exp::cref(rhs, 0)),
                // P1: (lhs.#1 < rhs.#1)
                exp::lt(exp::cref(lhs, 1), exp::cref(rhs, 1)),
                // P2: (lhs.#2 == rhs.#2)
                exp::eq(exp::cref(lhs, 2), exp::cref(rhs, 2)),
            ]
        });

        assert_derived_attribute(&mut model, b_id, |r#box| {
            HashSet::from([
                cref(input(r#box, 1), 0),
                cref(input(r#box, 1), 1),
                cref(input(r#box, 1), 2),
            ]) // {rhs.#0, rhs.#1, rhs.#2}
        });
    }

    #[test]
    fn test_full_outer_join_1() {
        let mut model = Model::default();
        let g_id = add_get(&mut model);
        let b_id = add_full_outer_join(&mut model, g_id, g_id, |lhs, rhs| {
            vec![
                // P0: (lhs.#0 >= rhs.#0)
                exp::gte(exp::cref(lhs, 0), exp::cref(rhs, 0)),
                // P1: (lhs.#1 <= rhs.#1)
                exp::lte(exp::cref(lhs, 1), exp::cref(rhs, 1)),
                // P2: (lhs.#2 != rhs.#2)
                exp::not_eq(exp::cref(lhs, 2), exp::cref(rhs, 2)),
            ]
        });

        assert_derived_attribute(&mut model, b_id, |_| {
            HashSet::from([]) // {}
        });
    }

    /// Adds a get box to the given model with a schema consisting of
    /// for 32-bit nullable integers.
    fn add_get(model: &mut Model) -> BoxId {
        let get_id = model.make_box(qgm::get(0).into());

        let mut b = model.get_mut_box(get_id);
        b.add_column(exp::base(0, typ::int32(true)));
        b.add_column(exp::base(1, typ::int32(true)));
        b.add_column(exp::base(2, typ::int32(true)));
        b.add_column(exp::base(3, typ::int32(true)));

        get_id
    }

    /// Adds a select join to the model and attaches the given `predicates`.
    /// The select box has a single input connected to the `src_id` box.
    fn add_select<F>(model: &mut Model, src_id: BoxId, predicates: F) -> BoxId
    where
        F: FnOnce(QuantifierId) -> Vec<BoxScalarExpr>,
    {
        let tgt_id = model.make_box(Select::default().into());
        let inp_id = model.make_quantifier(QuantifierType::Foreach, src_id, tgt_id);

        if let BoxType::Select(ref mut b) = model.get_mut_box(tgt_id).box_type {
            b.predicates.extend_from_slice(&predicates(inp_id));
        }

        tgt_id
    }

    /// Adds a full outer join to the model and attaches the given `predicates`.
    /// Both inputs are connected to the `lhs_id` and `rhs_id` boxes.
    fn add_full_outer_join<F>(
        model: &mut Model,
        lhs_id: BoxId,
        rhs_id: BoxId,
        predicates: F,
    ) -> BoxId
    where
        F: FnOnce(QuantifierId, QuantifierId) -> Vec<BoxScalarExpr>,
    {
        let tgt_id = model.make_box(OuterJoin::default().into());
        let lhs_id = model.make_quantifier(QuantifierType::PreservedForeach, lhs_id, tgt_id);
        let rhs_id = model.make_quantifier(QuantifierType::PreservedForeach, rhs_id, tgt_id);

        if let BoxType::OuterJoin(ref mut b) = model.get_mut_box(tgt_id).box_type {
            b.predicates.extend_from_slice(&predicates(lhs_id, rhs_id));
        }

        tgt_id
    }

    /// Adds a left outer join to the model and attaches the given `predicates`.
    /// Both inputs are connected to the `lhs_id` and `rhs_id` boxes.
    fn add_left_outer_join<F>(
        model: &mut Model,
        lhs_id: BoxId,
        rhs_id: BoxId,
        predicates: F,
    ) -> BoxId
    where
        F: FnOnce(QuantifierId, QuantifierId) -> Vec<BoxScalarExpr>,
    {
        let tgt_id = model.make_box(OuterJoin::default().into());
        let lhs_id = model.make_quantifier(QuantifierType::PreservedForeach, lhs_id, tgt_id);
        let rhs_id = model.make_quantifier(QuantifierType::Foreach, rhs_id, tgt_id);

        if let BoxType::OuterJoin(ref mut b) = model.get_mut_box(tgt_id).box_type {
            b.predicates.extend_from_slice(&predicates(lhs_id, rhs_id));
        }

        tgt_id
    }

    /// Derives the `RejectedNulls` for the given `b_id` and asserts its value.
    fn assert_derived_attribute<F>(model: &mut Model, b_id: BoxId, exp_value: F)
    where
        F: FnOnce(&QueryBox) -> HashSet<ColumnReference>,
    {
        RejectedNulls.derive(model, b_id);

        let r#box = model.get_box(b_id);
        let act_value = r#box.attributes.get::<RejectedNulls>();
        let exp_value = &exp_value(&r#box);

        assert_eq!(act_value, exp_value);
    }

    fn input(b: &QueryBox, i: usize) -> QuantifierId {
        b.quantifiers.iter().nth(i).unwrap().clone()
    }
}
