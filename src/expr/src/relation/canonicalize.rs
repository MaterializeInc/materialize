// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utility functions to transform parts of a single `MirRelationExpr`
//! into canonical form.

use crate::{BinaryFunc, MirScalarExpr, UnaryFunc};
use repr::{Datum, RelationType, ScalarType};

/// Canonicalize equivalence classes of a join.
///
/// This function makes it so that the same expression appears in only one
/// equivalence class. It also sorts and dedups the equivalence classes.
///
/// ```rust
/// use expr::MirScalarExpr;
/// use expr::canonicalize::canonicalize_equivalences;
///
/// let mut equivalences = vec![
///     vec![MirScalarExpr::Column(1), MirScalarExpr::Column(4)],
///     vec![MirScalarExpr::Column(3), MirScalarExpr::Column(5)],
///     vec![MirScalarExpr::Column(0), MirScalarExpr::Column(3)],
///     vec![MirScalarExpr::Column(2), MirScalarExpr::Column(2)],
/// ];
/// let expected = vec![
///     vec![MirScalarExpr::Column(0),
///         MirScalarExpr::Column(3),
///         MirScalarExpr::Column(5)],
///     vec![MirScalarExpr::Column(1), MirScalarExpr::Column(4)],
/// ];
/// canonicalize_equivalences(&mut equivalences);
/// assert_eq!(expected, equivalences)
/// ````
pub fn canonicalize_equivalences(equivalences: &mut Vec<Vec<MirScalarExpr>>) {
    for index in 1..equivalences.len() {
        for inner in 0..index {
            if equivalences[index]
                .iter()
                .any(|pair| equivalences[inner].contains(pair))
            {
                let to_extend = std::mem::replace(&mut equivalences[index], Vec::new());
                equivalences[inner].extend(to_extend);
            }
        }
    }
    for equivalence in equivalences.iter_mut() {
        equivalence.sort();
        equivalence.dedup();
    }
    equivalences.retain(|es| es.len() > 1);
    equivalences.sort();
}

/// Canonicalize predicates of a filter.
///
/// This function reduces and canonicalizes the structure of each individual
/// predicate. Then, it transforms predicates of the form "A and B" into two: "A"
/// and "B". Aftewards, it reduces predicates based on information from other
/// predicates in the set. Finally, it sorts and deduplicates the predicates.
///
/// Additionally, it also removes IS NOT NULL predicates if there is another
/// null rejecting predicate for the same sub-expression.
pub fn canonicalize_predicates(predicates: &mut Vec<MirScalarExpr>, input_type: &RelationType) {
    // 1) Reduce each individual predicate.
    let mut pending_predicates = predicates
        .drain(..)
        .map(|mut p| {
            p.reduce(&input_type);
            return p;
        })
        .collect::<Vec<MirScalarExpr>>();

    // 2) Split "A and B" into two predicates: "A" and "B"
    while let Some(expr) = pending_predicates.pop() {
        if let MirScalarExpr::CallBinary {
            func: BinaryFunc::And,
            expr1,
            expr2,
        } = expr
        {
            pending_predicates.push(*expr1);
            pending_predicates.push(*expr2);
        } else {
            predicates.push(expr);
        }
    }

    // 3) Reduce across `predicates`.
    // If a predicate `p` cannot be null, and `f(p)` is a nullable bool
    // then the predicate `p & f(p)` is equal to `p & f(true)`, and
    // `!p & f(p)` is equal to `!p & f(false)`. For any index i, the `Vec` of
    // predicates `[p1, ... pi, ... pn]` is equivalent to the single predicate
    // `pi & (p1 & ... & p(i-1) & p(i+1) ... & pn)`. Thus, if `pi`
    // (resp. `!pi`) cannot be null, it is valid to replace with `true` (resp.
    // `false`) every subexpression in `(p1 & ... & p(i-1) & p(i+1) ... & pn)`
    // that is equal to `pi`.

    // If `p` is null and `q` is a nullable bool, then `p & q` can be either
    // `null` or `false` depending on what `q`. Our rendering pipeline treats
    // both as "remove this row." Thus, in the specific context of filter
    // predicates, it is acceptable to make the aforementioned substitution
    // even if `pi` can be null.

    // Note that this does some dedupping of predicates since if `p1 = p2`
    // then this reduction process will replace `p1` with true.

    // Maintain respectively:
    // 1) A list of predicates for which we have checked for matching
    // subexpressions
    // 2) A list of predicates for which we have yet to do so.
    let mut completed = Vec::new();
    let mut todo = Vec::new();
    // Seed `todo` with all predicates.
    std::mem::swap(&mut todo, predicates);

    while let Some(predicate_to_apply) = todo.pop() {
        // Remove redundant !isnull(x) predicates if there is another predicate
        // that evaluates to NULL when `x` is NULL.
        if let Some(operand) = is_not_null(&predicate_to_apply) {
            if todo
                .iter_mut()
                .any(|p| is_null_rejecting_predicate(p, &operand))
                || completed
                    .iter_mut()
                    .any(|p| is_null_rejecting_predicate(p, &operand))
            {
                // skip this predicate
                continue;
            }
        }
        // Helper method: for each predicate `p`, see if all other predicates
        // (a.k.a. the union of todo & completed) contains `p` as a
        // subexpression, and replace the subexpression accordingly.
        // This method lives inside the loop because in order to comply with
        // Rust rules that only one mutable reference to `todo` can be held at a
        // time.
        let mut replace_subexpr_other_predicates =
            |expr: &MirScalarExpr, constant_bool: &MirScalarExpr| {
                // Do not replace subexpressions equal to `expr` if `expr` is a
                // literal to avoid infinite looping.
                if !expr.is_literal() {
                    for other_predicate in todo.iter_mut() {
                        replace_subexpr_and_reduce(
                            other_predicate,
                            expr,
                            constant_bool,
                            input_type,
                        );
                    }
                    for other_idx in (0..completed.len()).rev() {
                        if replace_subexpr_and_reduce(
                            &mut completed[other_idx],
                            expr,
                            constant_bool,
                            input_type,
                        ) {
                            // If a predicate in the `completed` list has
                            // been simplified, stick it back into the `todo` list.
                            todo.push(completed.remove(other_idx));
                        }
                    }
                }
            };
        // Meat of loop starts here. If a predicate p is of the form `!q`, replace
        // every instance of `q` in every other predicate with `false.`
        // Otherwise, replace every instance of `p` in every other predicate
        // with `true`.
        if let MirScalarExpr::CallUnary {
            func: UnaryFunc::Not,
            expr,
        } = &predicate_to_apply
        {
            replace_subexpr_other_predicates(
                expr,
                &MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool),
            )
        } else {
            replace_subexpr_other_predicates(
                &predicate_to_apply,
                &MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool),
            );
        }
        completed.push(predicate_to_apply);
    }
    // Remove any predicates that have been reduced to "true"
    completed.retain(|p| !p.is_literal_true());
    *predicates = completed;

    // 4) Sort and dedup predicates.
    predicates.sort();
    predicates.dedup();
}

/// Replace any matching subexpressions in `predicate`, and if `predicate` has
/// changed, reduce it. Return whether `predicate` has changed.
fn replace_subexpr_and_reduce(
    predicate: &mut MirScalarExpr,
    replace_if_equal_to: &MirScalarExpr,
    replace_with: &MirScalarExpr,
    input_type: &RelationType,
) -> bool {
    let mut changed = false;
    predicate.visit_mut_pre_post(
        &mut |e| {
            // The `cond` of an if statement is not visited to prevent `then`
            // or `els` from being evaluated before `cond`, resulting in a
            // correctness error.
            if let MirScalarExpr::If { then, els, .. } = e {
                return Some(vec![then, els]);
            }
            None
        },
        &mut |e| {
            if e == replace_if_equal_to {
                *e = replace_with.clone();
                changed = true;
            } else if let MirScalarExpr::CallBinary {
                func: r_func,
                expr1: r_expr1,
                expr2: r_expr2,
            } = replace_if_equal_to
            {
                if let Some(negation) = r_func.negate() {
                    if let MirScalarExpr::CallBinary {
                        func: l_func,
                        expr1: l_expr1,
                        expr2: l_expr2,
                    } = e
                    {
                        if negation == *l_func && l_expr1 == r_expr1 && l_expr2 == r_expr2 {
                            *e = MirScalarExpr::CallUnary {
                                func: UnaryFunc::Not,
                                expr: Box::new(replace_with.clone()),
                            };
                            changed = true;
                        }
                    }
                }
            }
        },
    );
    if changed {
        predicate.reduce(input_type);
    }
    changed
}

/// Returns the inner operand if the given predicate is an IS NOT NULL expression.
fn is_not_null(predicate: &MirScalarExpr) -> Option<MirScalarExpr> {
    if let MirScalarExpr::CallUnary {
        func: UnaryFunc::Not,
        expr,
    } = &predicate
    {
        if let MirScalarExpr::CallUnary {
            func: UnaryFunc::IsNull,
            expr,
        } = &**expr
        {
            return Some((**expr).clone());
        }
    }
    None
}

/// Whether the given predicate evaluates to NULL when the given operand expression is NULL.
fn is_null_rejecting_predicate(predicate: &MirScalarExpr, operand: &MirScalarExpr) -> bool {
    propagates_null_from_subexpression(predicate, operand)
}

fn propagates_null_from_subexpression(expr: &MirScalarExpr, operand: &MirScalarExpr) -> bool {
    if operand == expr {
        true
    } else if let MirScalarExpr::CallBinary { func, expr1, expr2 } = &expr {
        func.propagates_nulls()
            && (propagates_null_from_subexpression(&expr1, operand)
                || propagates_null_from_subexpression(&expr2, operand))
    } else if let MirScalarExpr::CallUnary { func, expr } = &expr {
        func.propagates_nulls() && propagates_null_from_subexpression(expr, operand)
    } else {
        false
    }
}
