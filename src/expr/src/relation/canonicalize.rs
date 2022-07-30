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

use std::cmp::Ordering;
use std::collections::HashSet;

use mz_repr::{Datum, RelationType, ScalarType};

use crate::visit::Visit;
use crate::{func, MirScalarExpr, UnaryFunc, VariadicFunc};

/// Canonicalize equivalence classes of a join and expressions contained in them.
///
/// `input_types` can be the `RelationType` of the join or the `RelationType` of
/// the individual inputs of the join in order.
///
/// This function:
/// * simplifies expressions to involve the least number of non-literal nodes.
///   This ensures that we only replace expressions by "even simpler"
///   expressions and that repeated substitutions reduce the complexity of
///   expressions and a fixed point is certain to be reached. Without this
///   rule, we might repeatedly replace a simple expression with an equivalent
///   complex expression containing that (or another replaceable) simple
///   expression, and repeat indefinitely.
/// * reduces all expressions contained in `equivalences`.
/// * Does everything that [canonicalize_equivalence_classes] does.
pub fn canonicalize_equivalences(
    equivalences: &mut Vec<Vec<MirScalarExpr>>,
    input_types: &[RelationType],
) {
    // This only aggregates the column types of each input, not the
    // keys of the inputs. It is unnecessary to aggregate the keys
    // of the inputs since input keys are unnecessary for reducing
    // `MirScalarExpr`s.
    let input_typ = input_types
        .iter()
        .fold(RelationType::empty(), |mut typ, i| {
            typ.column_types.extend_from_slice(&i.column_types[..]);
            typ
        });
    // Calculate the number of non-leaves for each expression.
    let mut to_reduce = equivalences
        .drain(..)
        .filter_map(|mut cls| {
            let mut result = cls
                .drain(..)
                .map(|expr| (rank_complexity(&expr), expr))
                .collect::<Vec<_>>();
            result.sort();
            result.dedup();
            if result.len() > 1 {
                Some(result)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    let mut expressions_rewritten = true;
    while expressions_rewritten {
        expressions_rewritten = false;
        for i in 0..to_reduce.len() {
            // `to_reduce` will be borrowed as immutable, so in order to modify
            // elements of `to_reduce[i]`, we are going to pop them out of
            // `to_reduce[i]` and put the modified version in `new_equivalence`,
            // which will then replace `to_reduce[i]`.
            let mut new_equivalence = Vec::with_capacity(to_reduce[i].len());
            while let Some((_, mut popped_expr)) = to_reduce[i].pop() {
                #[allow(deprecated)]
                popped_expr.visit_mut_post_nolimit(&mut |e: &mut MirScalarExpr| {
                    // If a simpler expression can be found that is equivalent
                    // to e,
                    if let Some(simpler_e) = to_reduce.iter().find_map(|cls| {
                        if cls.iter().skip(1).position(|(_, expr)| e == expr).is_some() {
                            Some(cls[0].1.clone())
                        } else {
                            None
                        }
                    }) {
                        // Replace e with the simpler expression.
                        *e = simpler_e;
                        expressions_rewritten = true;
                    }
                });
                popped_expr.reduce(&input_typ);
                new_equivalence.push((rank_complexity(&popped_expr), popped_expr));
            }
            new_equivalence.sort();
            new_equivalence.dedup();
            to_reduce[i] = new_equivalence;
        }
    }

    // Map away the complexity rating.
    *equivalences = to_reduce
        .drain(..)
        .map(|mut cls| cls.drain(..).map(|(_, expr)| expr).collect::<Vec<_>>())
        .collect::<Vec<_>>();

    canonicalize_equivalence_classes(equivalences);
}

/// Canonicalize only the equivalence classes of a join.
///
/// This function:
/// * ensures the same expression appears in only one equivalence class.
/// * ensures the equivalence classes are sorted and dedupped.
/// ```rust
/// use mz_expr::MirScalarExpr;
/// use mz_expr::canonicalize::canonicalize_equivalence_classes;
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
/// canonicalize_equivalence_classes(&mut equivalences);
/// assert_eq!(expected, equivalences)
/// ````
pub fn canonicalize_equivalence_classes(equivalences: &mut Vec<Vec<MirScalarExpr>>) {
    // Fuse equivalence classes containing the same expression.
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

/// Gives a relative complexity ranking for an expression. Higher numbers mean
/// greater complexity.
///
/// Currently, this method weighs literals as the least complex and weighs all
/// other expressions by the number of non-literals. In the future, we can
/// change how complexity is ranked so that repeated substitutions would result
/// in arriving at "better" fixed points. For example, we could try to improve
/// performance by ranking expressions by their estimated computation time.
///
/// To ensure we arrive at a fixed point after repeated substitutions, valid
/// complexity rankings must fulfill the following property:
/// For any expression `e`, there does not exist a SQL function `f` such
/// that `complexity(e) >= complexity(f(e))`.
///
/// For ease of intuiting the fixed point that we will arrive at after
/// repeated substitutions, it is nice but not required that complexity
/// rankings additionally fulfill the following property:
/// If expressions `e1` and `e2` are such that
/// `complexity(e1) < complexity(e2)` then for all SQL functions `f`,
/// `complexity(f(e1)) < complexity(f(e2))`.
fn rank_complexity(expr: &MirScalarExpr) -> usize {
    if expr.is_literal() {
        // literals are the least complex
        return 0;
    }
    let mut non_literal_count = 1;
    #[allow(deprecated)]
    expr.visit_post_nolimit(&mut |e| {
        if !e.is_literal() {
            non_literal_count += 1
        }
    });
    non_literal_count
}

/// Applies a flat_map on a Vec, and overwrites the vec with the result.
fn flat_map_modify<T, I, F>(v: &mut Vec<T>, f: F)
where
    F: FnMut(T) -> I,
    I: IntoIterator<Item = T>,
{
    let mut xx = v.drain(..).flat_map(f).collect();
    v.append(&mut xx);
}

/// Canonicalize predicates of a filter.
///
/// This function reduces and canonicalizes the structure of each individual
/// predicate. Then, it transforms predicates of the form "A and B" into two: "A"
/// and "B". Afterwards, it reduces predicates based on information from other
/// predicates in the set. Finally, it sorts and deduplicates the predicates.
///
/// Additionally, it also removes IS NOT NULL predicates if there is another
/// null rejecting predicate for the same sub-expression.
pub fn canonicalize_predicates(predicates: &mut Vec<MirScalarExpr>, input_type: &RelationType) {
    // 1) Reduce each individual predicate.
    predicates.iter_mut().for_each(|p| p.reduce(&input_type));

    // 2) Split "A and B" into two predicates: "A" and "B"
    // Relies on the `reduce` above having flattened nested ANDs.
    flat_map_modify(predicates, |p| {
        if let MirScalarExpr::CallVariadic {
            func: VariadicFunc::And,
            exprs,
        } = p
        {
            exprs
        } else {
            vec![p]
        }
    });

    // 3) Make non-null requirements explicit as predicates in order for
    // step 4) to be able to simplify AND/OR expressions with IS NULL
    // sub-predicates. This redundancy is removed later by step 5).
    let mut non_null_columns = HashSet::new();
    for p in predicates.iter() {
        p.non_null_requirements(&mut non_null_columns);
    }
    predicates.extend(non_null_columns.iter().map(|c| {
        MirScalarExpr::column(*c)
            .call_unary(UnaryFunc::IsNull(func::IsNull))
            .call_unary(UnaryFunc::Not(func::Not))
    }));

    // 4) Reduce across `predicates`.
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
            func: UnaryFunc::Not(func::Not),
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

    // 5) Remove redundant !isnull/isnull predicates after performing the replacements
    // in the loop above.
    std::mem::swap(&mut todo, &mut completed);
    while let Some(predicate_to_apply) = todo.pop() {
        // Remove redundant !isnull(x) predicates if there is another predicate
        // that evaluates to NULL when `x` is NULL.
        if let Some(operand) = is_not_null(&predicate_to_apply) {
            if todo
                .iter_mut()
                .chain(completed.iter_mut())
                .any(|p| is_null_rejecting_predicate(p, &operand))
            {
                // skip this predicate
                continue;
            }
        } else if let MirScalarExpr::CallUnary {
            func: UnaryFunc::IsNull(func::IsNull),
            expr,
        } = &predicate_to_apply
        {
            if todo
                .iter_mut()
                .chain(completed.iter_mut())
                .any(|p| is_null_rejecting_predicate(p, expr))
            {
                completed.push(MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool));
                break;
            }
        }
        completed.push(predicate_to_apply);
    }

    if completed
        .iter()
        .any(|p| p.is_literal_false() || p.is_literal_null())
    {
        // all rows get filtered away if any predicate is null or false.
        *predicates = vec![MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool)]
    } else {
        // Remove any predicates that have been reduced to "true"
        completed.retain(|p| !p.is_literal_true());
        *predicates = completed;
    }

    // 4) Sort and dedup predicates.
    predicates.sort_by(compare_predicates);
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
    #[allow(deprecated)]
    predicate.visit_mut_pre_post_nolimit(
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
                                func: UnaryFunc::Not(func::Not),
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
        func: UnaryFunc::Not(func::Not),
        expr,
    } = &predicate
    {
        if let MirScalarExpr::CallUnary {
            func: UnaryFunc::IsNull(func::IsNull),
            expr,
        } = &**expr
        {
            return Some((**expr).clone());
        }
    }
    None
}

/// Whether the given predicate evaluates to NULL when the given operand expression is NULL.
#[inline(always)]
fn is_null_rejecting_predicate(predicate: &MirScalarExpr, operand: &MirScalarExpr) -> bool {
    propagates_null_from_subexpression(predicate, operand)
}

fn propagates_null_from_subexpression(expr: &MirScalarExpr, operand: &MirScalarExpr) -> bool {
    if operand == expr {
        true
    } else if let MirScalarExpr::CallVariadic { func, exprs } = &expr {
        func.propagates_nulls()
            && (exprs
                .iter()
                .any(|e| propagates_null_from_subexpression(&e, operand)))
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

/// Comparison method for sorting predicates by their complexity, measured by the total
/// number of non-literal expression nodes within the expression.
fn compare_predicates(x: &MirScalarExpr, y: &MirScalarExpr) -> Ordering {
    (rank_complexity(x), x).cmp(&(rank_complexity(y), y))
}
