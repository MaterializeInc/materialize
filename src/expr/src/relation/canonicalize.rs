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
use std::collections::{BTreeMap, BTreeSet};

use mz_ore::soft_assert_or_log;
use mz_repr::{ColumnType, Datum, ScalarType};

use crate::visit::Visit;
use crate::{func, MirScalarExpr, UnaryFunc, VariadicFunc};

/// Canonicalize equivalence classes of a join and expressions contained in them.
///
/// `input_types` can be the [ColumnType]s of the join or the [ColumnType]s of
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
pub fn canonicalize_equivalences<'a, I>(
    equivalences: &mut Vec<Vec<MirScalarExpr>>,
    input_column_types: I,
) where
    I: Iterator<Item = &'a Vec<ColumnType>>,
{
    let column_types = input_column_types
        .flat_map(|f| f.clone())
        .collect::<Vec<_>>();

    let mut equivs = EquivalenceClasses::default();
    std::mem::swap(&mut equivs.classes, equivalences);
    equivs.minimize(&Some(column_types));
    std::mem::swap(&mut equivs.classes, equivalences);
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
    expr.visit_pre(|e| {
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
pub fn canonicalize_predicates(predicates: &mut Vec<MirScalarExpr>, column_types: &[ColumnType]) {
    soft_assert_or_log!(
        predicates
            .iter()
            .all(|p| p.typ(column_types).scalar_type == ScalarType::Bool),
        "cannot canonicalize predicates that are not of type bool"
    );

    // 1) Reduce each individual predicate.
    predicates.iter_mut().for_each(|p| p.reduce(column_types));

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
    let mut non_null_columns = BTreeSet::new();
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
                            column_types,
                        );
                    }
                    for other_idx in (0..completed.len()).rev() {
                        if replace_subexpr_and_reduce(
                            &mut completed[other_idx],
                            expr,
                            constant_bool,
                            column_types,
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

    if completed.iter().any(|p| {
        (p.is_literal_false() || p.is_literal_null()) &&
        // This extra check is only needed if we determine that the soft-assert
        // at the top of this function would ever fail for a good reason.
        p.typ(column_types).scalar_type == ScalarType::Bool
    }) {
        // all rows get filtered away if any predicate is null or false.
        *predicates = vec![MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool)]
    } else {
        // Remove any predicates that have been reduced to "true"
        completed.retain(|p| !p.is_literal_true());
        *predicates = completed;
    }

    // 6) Sort and dedup predicates.
    predicates.sort_by(compare_predicates);
    predicates.dedup();
}

/// Replace any matching subexpressions in `predicate`, and if `predicate` has
/// changed, reduce it. Return whether `predicate` has changed.
fn replace_subexpr_and_reduce(
    predicate: &mut MirScalarExpr,
    replace_if_equal_to: &MirScalarExpr,
    replace_with: &MirScalarExpr,
    column_types: &[ColumnType],
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
        predicate.reduce(column_types);
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
                .any(|e| propagates_null_from_subexpression(e, operand)))
    } else if let MirScalarExpr::CallBinary { func, expr1, expr2 } = &expr {
        func.propagates_nulls()
            && (propagates_null_from_subexpression(expr1, operand)
                || propagates_null_from_subexpression(expr2, operand))
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

/// For each equivalence class, it finds the simplest expression, which will be the canonical one.
/// Returns a Map that maps from each expression in each equivalence class to the canonical
/// expression in the same equivalence class.
pub fn get_canonicalizer_map(
    equivalences: &Vec<Vec<MirScalarExpr>>,
) -> BTreeMap<MirScalarExpr, MirScalarExpr> {
    let mut canonicalizer_map = BTreeMap::new();
    for equivalence in equivalences {
        // The unwrap is ok, because a join equivalence class can't be empty.
        let canonical_expr = equivalence
            .iter()
            .min_by(|a, b| compare_predicates(*a, *b))
            .unwrap();
        for e in equivalence {
            if e != canonical_expr {
                canonicalizer_map.insert(e.clone(), canonical_expr.clone());
            }
        }
    }
    canonicalizer_map
}

pub use equivalence_classes::EquivalenceClasses;

/// Logic for maintaining equivalence classes of expressions.
///
/// "Equivalent" expressions can be freely substituted for one another, and are "equal"
/// in the Rust sense that their results are always `Datum::eq`, although they are not
/// necessarily equal in the SQL sense, as they may equivalently produce `Datum::Null`.
mod equivalence_classes {

    use std::collections::BTreeMap;

    use mz_repr::ColumnType;
    use crate::{MirScalarExpr, BinaryFunc, UnaryFunc};

    /// A compact representation of classes of expressions that must be equivalent.
    ///
    /// Each "class" contains a list of expressions, each of which must be `Eq::eq` equal.
    /// Ideally, the first element is the "simplest", e.g. a literal or column reference,
    /// and any other element of that list can be replaced by it.
    ///
    /// The classes are meant to be minimized, with each expression as reduced as it can be,
    /// and all classes sharing an element merged.
    #[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Default, Debug)]
    pub struct EquivalenceClasses {
        /// Multiple lists of equivalent expressions, each representing an equivalence class.
        ///
        /// The first element should be the "canonical" simplest element, that any other element
        /// can be replaced by.
        /// These classes are unified whenever possible, to minimize the number of classes.
        pub classes: Vec<Vec<MirScalarExpr>>,
    }

    impl EquivalenceClasses {
        /// Sorts and deduplicates each class, and the classes themselves.
        fn tidy(&mut self) {
            for class in self.classes.iter_mut() {
                // Remove all literal errors, as they cannot be equated to other things.
                class.retain(|e| !e.is_literal_err());
                class.sort_by(|e1, e2| match (e1, e2) {
                    (MirScalarExpr::Literal(_, _), MirScalarExpr::Literal(_, _)) => e1.cmp(e2),
                    (MirScalarExpr::Literal(_, _), _) => std::cmp::Ordering::Less,
                    (_, MirScalarExpr::Literal(_, _)) => std::cmp::Ordering::Greater,
                    (MirScalarExpr::Column(_), MirScalarExpr::Column(_)) => e1.cmp(e2),
                    (MirScalarExpr::Column(_), _) => std::cmp::Ordering::Less,
                    (_, MirScalarExpr::Column(_)) => std::cmp::Ordering::Greater,
                    (x, y) => {
                        // General expressions should be ordered by their size,
                        // to ensure we only simplify expressions by substitution.
                        let x_size = x.size();
                        let y_size = y.size();
                        if x_size == y_size {
                            x.cmp(y)
                        } else {
                            x_size.cmp(&y_size)
                        }
                    }
                });
                class.dedup();
            }
            self.classes.retain(|c| c.len() > 1);
            self.classes.sort();
            self.classes.dedup();
        }

        /// Update `self` to maintain the same equivalences which potentially reducing along `Ord::le`.
        ///
        /// Informally this means simplifying constraints, removing redundant constraints, and unifying equivalence classes.
        pub fn minimize(&mut self, columns: &Option<Vec<ColumnType>>) {
            // Repeatedly, we reduce each of the classes themselves, then unify the classes.
            // This should strictly reduce complexity, and reach a fixed point.
            // Ideally it is *confluent*, arriving at the same fixed point no matter the order of operations.
            self.tidy();

            // We continue as long as any simplification has occurred.
            // An expression can be simplified, a duplication found, or two classes unified.
            let mut stable = false;
            while !stable {
                stable = self.minimize_once(columns);
            }

            // TODO: remove these measures once we are more confident about idempotence.
            let prev = self.clone();
            self.minimize_once(columns);
            mz_ore::soft_assert_eq_or_log!(self, &prev, "Equivalences::minimize() not idempotent");
        }

        /// A single iteration of minimization, which we expect to repeat but benefit from factoring out.
        fn minimize_once(&mut self, columns: &Option<Vec<ColumnType>>) -> bool {
            // We are complete unless we experience an expression simplification, or an equivalence class unification.
            let mut stable = true;

            // 0. Reduce each expression
            //
            // This is optional in that `columns` may not be provided (`reduce` requires type information).
            if let Some(columns) = columns {
                for class in self.classes.iter_mut() {
                    for expr in class.iter_mut() {
                        let prev_expr = expr.clone();
                        expr.reduce_safely(columns);
                        if &prev_expr != expr {
                            stable = false;
                        }
                    }
                }
            }

            // 1. Reduce each class.
            //    Each class can be reduced in the context of *other* classes, which are available for substitution.
            for class_index in 0..self.classes.len() {
                for index in 0..self.classes[class_index].len() {
                    let mut cloned = self.classes[class_index][index].clone();
                    // Use `reduce_child` rather than `reduce_expr` to avoid entire expression replacement.
                    let reduced = self.reduce_child(&mut cloned);
                    if reduced {
                        self.classes[class_index][index] = cloned;
                        stable = false;
                    }
                }
            }

            // 2. Unify classes.
            //    If the same expression is in two classes, we can unify the classes.
            //    This element may not be the representative.
            //    TODO: If all lists are sorted, this could be a linear merge among all.
            //          They stop being sorted as soon as we make any modification, though.
            //          But, it would be a fast rejection when faced with lots of data.
            for index1 in 0..self.classes.len() {
                for index2 in 0..index1 {
                    if self.classes[index1]
                        .iter()
                        .any(|x| self.classes[index2].iter().any(|y| x == y))
                    {
                        let prior = std::mem::take(&mut self.classes[index2]);
                        self.classes[index1].extend(prior);
                        stable = false;
                    }
                }
            }

            // 3. Identify idioms
            //    E.g. If Eq(x, y) must be true, we can introduce classes `[x, y]` and `[false, IsNull(x), IsNull(y)]`.
            let mut to_add = Vec::new();
            for class in self.classes.iter_mut() {
                if class.iter().any(|c| c.is_literal_true()) {
                    for expr in class.iter() {
                        // If Eq(x, y) must be true, we can introduce classes `[x, y]` and `[false, IsNull(x), IsNull(y)]`.
                        // This substitution replaces a complex expression with several smaller expressions, and cannot
                        // cycle if we follow that practice.
                        if let MirScalarExpr::CallBinary {
                            func: BinaryFunc::Eq,
                            expr1,
                            expr2,
                        } = expr
                        {
                            to_add.push(vec![*expr1.clone(), *expr2.clone()]);
                            to_add.push(vec![
                                MirScalarExpr::literal_false(),
                                expr1.clone().call_is_null(),
                                expr2.clone().call_is_null(),
                            ]);
                            stable = false;
                        }
                    }
                    // Remove the more complex form of the expression.
                    class.retain(|expr| {
                        if let MirScalarExpr::CallBinary {
                            func: BinaryFunc::Eq,
                            ..
                        } = expr
                        {
                            false
                        } else {
                            true
                        }
                    });
                    for expr in class.iter() {
                        // If TRUE == NOT(X) then FALSE == X is a simpler form.
                        if let MirScalarExpr::CallUnary {
                            func: UnaryFunc::Not(_),
                            expr: e,
                        } = expr
                        {
                            to_add.push(vec![MirScalarExpr::literal_false(), (**e).clone()]);
                            stable = false;
                        }
                    }
                    class.retain(|expr| {
                        if let MirScalarExpr::CallUnary {
                            func: UnaryFunc::Not(_),
                            ..
                        } = expr
                        {
                            false
                        } else {
                            true
                        }
                    });
                }
                if class.iter().any(|c| c.is_literal_false()) {
                    for expr in class.iter() {
                        // If FALSE == NOT(X) then TRUE == X is a simpler form.
                        if let MirScalarExpr::CallUnary {
                            func: UnaryFunc::Not(_),
                            expr: e,
                        } = expr
                        {
                            to_add.push(vec![MirScalarExpr::literal_true(), (**e).clone()]);
                            stable = false;
                        }
                    }
                    class.retain(|expr| {
                        if let MirScalarExpr::CallUnary {
                            func: UnaryFunc::Not(_),
                            ..
                        } = expr
                        {
                            false
                        } else {
                            true
                        }
                    });
                }
            }
            self.classes.extend(to_add);

            // Tidy up classes, restore representative.
            self.tidy();

            stable
        }

        /// Produce the equivalences present in both inputs.
        pub fn union(&self, other: &Self) -> Self {
            // TODO: seems like this could be extended, with similar concepts to localization:
            //       We may removed non-shared constraints, but ones that remain could take over
            //       and substitute in to retain more equivalences.

            // For each pair of equivalence classes, their intersection.
            let mut equivalences = EquivalenceClasses {
                classes: Vec::new(),
            };
            for class1 in self.classes.iter() {
                for class2 in other.classes.iter() {
                    let class = class1
                        .iter()
                        .filter(|e1| class2.iter().any(|e2| e1 == &e2))
                        .cloned()
                        .collect::<Vec<_>>();
                    if class.len() > 1 {
                        equivalences.classes.push(class);
                    }
                }
            }
            equivalences.minimize(&None);
            equivalences
        }

        /// Permutes each expression, looking up each column reference in `permutation` and replacing with what it finds.
        pub fn permute(&mut self, permutation: &[usize]) {
            for class in self.classes.iter_mut() {
                for expr in class.iter_mut() {
                    expr.permute(permutation);
                }
            }
        }

        /// Subject the constraints to the column projection, reworking and removing equivalences.
        ///
        /// This method should also introduce equivalences representing any repeated columns.
        pub fn project<I>(&mut self, output_columns: I)
        where
            I: IntoIterator<Item = usize> + Clone,
        {
            // Retain the first instance of each column, and record subsequent instances as duplicates.
            let mut dupes = Vec::new();
            let mut remap = BTreeMap::default();
            for (idx, col) in output_columns.into_iter().enumerate() {
                if let Some(pos) = remap.get(&col) {
                    dupes.push((*pos, idx));
                } else {
                    remap.insert(col, idx);
                }
            }

            // Some expressions may be "localized" in that they only reference columns in `output_columns`.
            // Many expressions may not be localized, but may reference canonical non-localized expressions
            // for classes that contain a localized expression; in that case we can "backport" the localized
            // expression to give expressions referencing the canonical expression a shot at localization.
            //
            // Expressions should only contain instances of canonical expressions, and so we shouldn't need
            // to look any further than backporting those. Backporting should have the property that the simplest
            // localized expression in each class does not contain any non-localized canonical expressions
            // (as that would make it non-localized); our backporting of non-localized canonicals with localized
            // expressions should never fire a second

            // Let's say an expression is "localized" once we are able to rewrite its support in terms of `output_columns`.
            // Not all expressions can be localized, although some of them may be equivalent to localized expressions.
            // As we find localized expressions, we can replace uses of their equivalent representative with them,
            // which may allow further expression localization.
            // We continue the process until no further classes can be localized.

            // A map from representatives to our first localization of their equivalence class.
            let mut localized = false;
            while !localized {
                localized = true;
                for class in self.classes.iter_mut() {
                    if !class[0].support().iter().all(|c| remap.contains_key(c)) {
                        if let Some(pos) = class
                            .iter()
                            .position(|e| e.support().iter().all(|c| remap.contains_key(c)))
                        {
                            class.swap(0, pos);
                            localized = false;
                        }
                    }
                }
                // attempt to replace representatives with equivalent localizeable expressions.
                for class_index in 0..self.classes.len() {
                    for index in 0..self.classes[class_index].len() {
                        let mut cloned = self.classes[class_index][index].clone();
                        // Use `reduce_child` rather than `reduce_expr` to avoid entire expression replacement.
                        let reduced = self.reduce_child(&mut cloned);
                        if reduced {
                            self.classes[class_index][index] = cloned;
                        }
                    }
                }
                // NB: Do *not* `self.minimize()`, as we are developing localizable rather than canonical representatives.
            }

            // Localize all localizable expressions and discard others.
            for class in self.classes.iter_mut() {
                class.retain(|e| e.support().iter().all(|c| remap.contains_key(c)));
                for expr in class.iter_mut() {
                    expr.permute_map(&remap);
                }
            }
            self.classes.retain(|c| c.len() > 1);
            // If column repetitions, introduce them as equivalences.
            // We introduce only the equivalence to the first occurrence, and rely on minimization to collect them.
            for (col1, col2) in dupes {
                self.classes.push(vec![
                    MirScalarExpr::Column(col1),
                    MirScalarExpr::Column(col2),
                ]);
            }
            self.minimize(&None);
        }

        /// Perform any exact replacement for `expr`, report if it had an effect.
        fn replace(&self, expr: &mut MirScalarExpr) -> bool {
            for class in self.classes.iter() {
                // TODO: If `class` is sorted We only need to iterate through "simpler" expressions;
                // we can stop once x > expr.
                // We need to be careful with that reasoning, as it interferes with self-improvement;
                // if we are modifying `self` we must restore the invariant before relying on it again.
                if class[1..].iter().any(|x| x == expr) {
                    expr.clone_from(&class[0]);
                    return true;
                }
            }
            false
        }

        /// Perform any simplification, report if effective.
        pub fn reduce_expr(&self, expr: &mut MirScalarExpr) -> bool {
            let mut simplified = false;
            simplified = simplified || self.reduce_child(expr);
            simplified = simplified || self.replace(expr);
            simplified
        }

        /// Perform any simplification on children, report if effective.
        pub fn reduce_child(&self, expr: &mut MirScalarExpr) -> bool {
            let mut simplified = false;
            match expr {
                MirScalarExpr::CallBinary { expr1, expr2, .. } => {
                    simplified = self.reduce_expr(expr1) || simplified;
                    simplified = self.reduce_expr(expr2) || simplified;
                }
                MirScalarExpr::CallUnary { expr, .. } => {
                    simplified = self.reduce_expr(expr) || simplified;
                }
                MirScalarExpr::CallVariadic { exprs, .. } => {
                    for expr in exprs.iter_mut() {
                        simplified = self.reduce_expr(expr) || simplified;
                    }
                }
                MirScalarExpr::If { cond: _, then, els } => {
                    // Do not simplify `cond`, as we cannot ensure the simplification
                    // continues to hold as expressions migrate around.
                    simplified = self.reduce_expr(then) || simplified;
                    simplified = self.reduce_expr(els) || simplified;
                }
                _ => {}
            }
            simplified
        }

        /// True if any equivalence class contains two distinct non-error literals.
        pub fn unsatisfiable(&self) -> bool {
            for class in self.classes.iter() {
                let mut literal_ok = None;
                for expr in class.iter() {
                    if let MirScalarExpr::Literal(Ok(row), _) = expr {
                        if literal_ok.is_some() && literal_ok != Some(row) {
                            return true;
                        } else {
                            literal_ok = Some(row);
                        }
                    }
                }
            }
            false
        }
    }
}