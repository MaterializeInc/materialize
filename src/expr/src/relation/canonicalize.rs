// Copyright Materialize, Inc. All rights reserved.
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
    // If a predicate `p` cannot be null, and `f` is a function from bool to
    // bool, then the predicate `p & f(p)` is equal to `p & f(true)`, and
    // `!p & f(p)` is equal to `!p & f(false)`.
    // For any index i, the `Vec` of predicates `[p1, ... pi, ... pn]` is
    // equivalent to the single predicate `pi & (p1 & ... & p(i-1) & p(i+1)
    // ... & pn)`.
    // Thus, if `pi` (resp. `!pi`) cannot be null, it is valid to replace with
    // `true` (resp. `false`) every subexpression in
    // `(p1 & ... & p(i-1) & p(i+1) ... & pn)` that is equal to `pi`.

    // Pop out a predicate `p` from `predicates_to_apply`. Check all the other
    // predicates (this is the union of `predicates_to_apply` and
    // `applied_predicates`), and if `p` is a subexpression of any of those
    // predicates, replace the subexpression with `true`. Then stick `p` into
    // `applied_predicates`. If a predicate `q` in `applied_predicates` was
    // altered in the aforementioned process, remove `q` from
    // `applied_predicates` and put it in `predicates_to_apply` so we can reduce
    // the predicates even further.
    let mut predicates_to_apply = Vec::new();
    let mut applied_predicates = Vec::<MirScalarExpr>::new();
    // Seed `predicates_to_apply` with the `Vec` of all predicates.
    std::mem::swap(&mut predicates_to_apply, predicates);

    while let Some(predicate_to_apply) = predicates_to_apply.pop() {
        let mut reduce_other_instances_of_expr =
            |expr: &MirScalarExpr, constant_bool: &MirScalarExpr| {
                // Do not replace subexpressions equal to `expr` if `expr` is a
                // literal to avoid infinite looping.
                if !expr.is_literal() && !expr.typ(input_type).nullable {
                    for other_predicate in predicates_to_apply.iter_mut() {
                        let mut changed = false;
                        other_predicate.visit_mut_pre(&mut |e: &mut MirScalarExpr| {
                            if e == expr {
                                *e = constant_bool.clone();
                                changed = true;
                                true
                            } else {
                                false
                            }
                        });
                        if changed {
                            other_predicate.reduce(input_type);
                        }
                    }
                    for other_idx in (0..applied_predicates.len()).rev() {
                        let mut changed = false;
                        applied_predicates[other_idx].visit_mut_pre(
                            &mut |e: &mut MirScalarExpr| {
                                if e == expr {
                                    *e = constant_bool.clone();
                                    changed = true;
                                    true
                                } else {
                                    false
                                }
                            },
                        );
                        if changed {
                            applied_predicates[other_idx].reduce(input_type);
                            predicates_to_apply.push(applied_predicates.remove(other_idx));
                        }
                    }
                }
            };

        if let MirScalarExpr::CallUnary {
            func: UnaryFunc::Not,
            expr,
        } = &predicate_to_apply
        {
            reduce_other_instances_of_expr(
                expr,
                &MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool),
            )
        } else {
            reduce_other_instances_of_expr(
                &predicate_to_apply,
                &MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool),
            );
        }
        applied_predicates.push(predicate_to_apply);
    }
    *predicates = applied_predicates;

    // 4) Sort and dedup predicates.
    predicates.sort();
    predicates.dedup();
}
