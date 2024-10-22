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
                popped_expr.reduce(&column_types);
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

    // 4) Self-reduce predicates based on potential equivalences.
    // This includes noticing that predicates imply things about other predicates, 
    // but also that predicates like `a == b` may enable further simplification.
    let mut preds = predicates.clone();
    preds.push(MirScalarExpr::literal_true());
    let mut equivalences = equivalences::EquivalenceClasses::default();
    equivalences.classes.push(preds);
    equivalences.minimize(&Some(column_types.to_vec()));
    let mut completed = equivalences.into_predicates();
    completed.sort();
    completed.dedup();

    // 5) Remove redundant !isnull/isnull predicates after performing the replacements
    // in the loop above.
    let mut todo = std::mem::take(&mut completed);
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

/// Types and traits related to `MirScalarExpr` equivalence determination.
pub mod equivalences {

    use crate::{BinaryFunc, MirScalarExpr, UnaryFunc};
    use mz_repr::ColumnType;
    use std::collections::BTreeMap;

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
        /// They are only guaranteed to form an equivalence relation after a call to `minimimize`,
        /// which refreshes both `self.classes` and `self.remap`.
        pub classes: Vec<Vec<MirScalarExpr>>,

        /// An expression simplification map.
        ///
        /// This map reflects an equivalence relation based on a prior version of `self.classes`.
        /// As users may add to `self.classes`, `self.remap` may become stale. We refresh `remap`
        /// only in `self.refresh()`, to the equivalence relation that derives from `self.classes`.
        remap: BTreeMap<MirScalarExpr, MirScalarExpr>,
    }

    impl EquivalenceClasses {
        /// Comparator function for the complexity of scalar expressions. Simpler expressions are
        /// smaller. Can be used when we need to decide which of several equivalent expressions to use.
        pub fn mir_scalar_expr_complexity(
            e1: &MirScalarExpr,
            e2: &MirScalarExpr,
        ) -> std::cmp::Ordering {
            use crate::MirScalarExpr::*;
            use std::cmp::Ordering::*;
            match (e1, e2) {
                (Literal(_, _), Literal(_, _)) => e1.cmp(e2),
                (Literal(_, _), _) => Less,
                (_, Literal(_, _)) => Greater,
                (Column(_), Column(_)) => e1.cmp(e2),
                (Column(_), _) => Less,
                (_, Column(_)) => Greater,
                (x, y) => {
                    // General expressions should be ordered by their size,
                    // to ensure we only simplify expressions by substitution.
                    // If same size, then fall back to the expressions' Ord.
                    match x.size().cmp(&y.size()) {
                        Equal => x.cmp(y),
                        other => other,
                    }
                }
            }
        }

        /// Sorts and deduplicates each class, removing literal errors.
        ///
        /// This method does not ensure equivalence relation structure, but instead performs
        /// only minimal structural clean-up.
        fn tidy(&mut self) {
            for class in self.classes.iter_mut() {
                // Remove all literal errors, as they cannot be equated to other things.
                class.retain(|e| !e.is_literal_err());
                class.sort_by(Self::mir_scalar_expr_complexity);
                class.dedup();
            }
            self.classes.retain(|c| c.len() > 1);
            self.classes.sort();
            self.classes.dedup();
        }

        /// Restore equivalence relation structure to `self.classes` and refresh `self.remap`.
        ///
        /// This method takes roughly linear time, and returns true iff `self.remap` has changed.
        /// This is the only method that changes `self.remap`, and is a perfect place to decide
        /// whether the equivalence classes it represents have experienced any changes since the
        /// last refresh.
        fn refresh(&mut self) -> bool {
            self.tidy();

            // remap may already be the correct answer, and if so we should avoid the work of rebuilding it.
            // If it contains the same number of expressions as `self.classes`, and for every expression in
            // `self.classes` the two agree on the representative, they are identical.
            if self.remap.len() == self.classes.iter().map(|c| c.len()).sum::<usize>()
                && self
                    .classes
                    .iter()
                    .all(|c| c.iter().all(|e| self.remap.get(e) == Some(&c[0])))
            {
                // No change, so return false.
                return false;
            }

            // Optimistically build the `remap` we would want.
            // Note if any unions would be required, in which case we have further work to do,
            // including re-forming `self.classes`.
            let mut union_find = BTreeMap::default();
            let mut dirtied = false;
            for class in self.classes.iter() {
                for expr in class.iter() {
                    if let Some(other) = union_find.insert(expr.clone(), class[0].clone()) {
                        // A merge is required, but have the more complex expression point at the simpler one.
                        // This allows `union_find` to end as the `remap` for the new `classes` we form, with
                        // the only required work being compressing all the paths.
                        if Self::mir_scalar_expr_complexity(&other, &class[0])
                            == std::cmp::Ordering::Less
                        {
                            union_find.union(&class[0], &other);
                        } else {
                            union_find.union(&other, &class[0]);
                        }
                        dirtied = true;
                    }
                }
            }
            if dirtied {
                let mut classes: BTreeMap<_, Vec<_>> = BTreeMap::default();
                for class in self.classes.drain(..) {
                    for expr in class {
                        let root: MirScalarExpr = union_find.find(&expr).unwrap().clone();
                        classes.entry(root).or_default().push(expr);
                    }
                }
                self.classes = classes.into_values().collect();
                self.tidy();
            }

            let changed = self.remap != union_find;
            self.remap = union_find;
            changed
        }

        /// Update `self` to maintain the same equivalences which potentially reducing along `Ord::le`.
        ///
        /// Informally this means simplifying constraints, removing redundant constraints, and unifying equivalence classes.
        pub fn minimize(&mut self, columns: &Option<Vec<ColumnType>>) {
            // Repeatedly, we reduce each of the classes themselves, then unify the classes.
            // This should strictly reduce complexity, and reach a fixed point.
            // Ideally it is *confluent*, arriving at the same fixed point no matter the order of operations.

            // Ensure `self.classes` and `self.remap` are equivalence relations.
            // Users are allowed to mutate `self.classes`, so we must perform this normalization at least once.
            self.refresh();

            // We should not rely on nullability information present in `column_types`. (Doing this
            // every time just before calling `reduce` was found to be a bottleneck during incident-217,
            // so now we do this nullability tweaking only once here.)
            let columns = columns.as_ref().map(|columns| {
                columns
                    .iter()
                    .map(|col| {
                        let mut col = col.clone();
                        col.nullable = true;
                        col
                    })
                    .collect()
            });

            // We continue as long as any simplification has occurred.
            // An expression can be simplified, a duplication found, or two classes unified.
            let mut stable = false;
            while !stable {
                stable = !self.minimize_once(&columns);
            }
        }

        /// A single iteration of minimization, which we expect to repeat but benefit from factoring out.
        ///
        /// This invocation should take roughly linear time.
        /// It starts with equivalence class invariants maintained (closed under transitivity), and then
        ///   1. Performs per-expression reduction, including the class structure to replace subexpressions.
        ///   2. Applies idiom detection to e.g. unpack expressions equivalence to literal true or false.
        ///   3. Restores the equivalence class invariants.
        fn minimize_once(&mut self, columns: &Option<Vec<ColumnType>>) -> bool {
            // 1. Reduce each expression
            //
            // This reduction first looks for subexpression substitutions that can be performed,
            // and then applies expression reduction if column type information is provided.
            for class in self.classes.iter_mut() {
                for expr in class.iter_mut() {
                    self.remap.reduce_child(expr);
                    if let Some(columns) = columns {
                        expr.reduce(columns);
                    }
                }
            }

            // 2. Identify idioms
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

            // 3. Restore equivalence relation structure and observe if any changes result.
            self.refresh()
        }

        /// Produce the equivalences present in both inputs.
        pub fn union(&self, other: &Self) -> Self {
            // TODO: seems like this could be extended, with similar concepts to localization:
            //       We may removed non-shared constraints, but ones that remain could take over
            //       and substitute in to retain more equivalences.

            // For each pair of equivalence classes, their intersection.
            let mut equivalences = EquivalenceClasses {
                classes: Vec::new(),
                remap: Default::default(),
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
        ///
        /// This method is only used internal to `Equivalences`, and should eventually be replaced
        /// by the use of `self.remap` or something akin to it. The linear scan is not appropriate.
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
        ///
        /// This method is only used internal to `Equivalences`, and should eventually be replaced
        /// by the use of `self.remap` or something akin to it. The linear scan is not appropriate.
        fn reduce_expr(&self, expr: &mut MirScalarExpr) -> bool {
            let mut simplified = false;
            simplified = simplified || self.reduce_child(expr);
            simplified = simplified || self.replace(expr);
            simplified
        }

        /// Perform any simplification on children, report if effective.
        ///
        /// This method is only used internal to `Equivalences`, and should eventually be replaced
        /// by the use of `self.remap` or something akin to it. The linear scan is not appropriate.
        fn reduce_child(&self, expr: &mut MirScalarExpr) -> bool {
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

        /// Returns a map that can be used to replace (sub-)expressions.
        pub fn reducer(&self) -> &BTreeMap<MirScalarExpr, MirScalarExpr> {
            &self.remap
        }

        /// Extracts the information in `self` into a list of predicates.
        ///
        /// Equivalence classes including `true` become predicates, classes
        /// including `false` become negated predicates, and all other classes
        /// become a sequence of `(x = y) OR (x IS NULL AND y IS NULL)`.
        pub fn into_predicates(self) -> Vec<MirScalarExpr> {
            let mut results = Vec::new();
            for class in self.classes {
                if class.contains(&MirScalarExpr::literal_true()) {
                    results.extend(
                        class
                            .into_iter()
                            .filter(|x| x != &MirScalarExpr::literal_true()),
                    );
                } else if class.contains(&MirScalarExpr::literal_false()) {
                    results.extend(
                        class
                            .into_iter()
                            .filter(|x| x != &MirScalarExpr::literal_false())
                            .map(|e| e.not()),
                    );
                } else {
                    let mut iter = class.into_iter();
                    if let Some(expr0) = iter.next() {
                        // expr0 may be known non-null, which simplifies our output; check.
                        let mut nullable = expr0.clone().call_is_null();
                        self.remap.reduce_expr(&mut nullable);
                        let is_nullable = nullable != MirScalarExpr::literal_false();

                        for expr in iter {
                            results.push({
                                let mut result =
                                    expr.clone().call_binary(expr0.clone(), BinaryFunc::Eq);
                                if is_nullable {
                                    result = result
                                        .or(expr.call_is_null().and(expr0.clone().call_is_null()));
                                }
                                result
                            })
                        }
                    }
                }
            }
            results.sort();
            results.dedup();
            results
        }
    }

    /// A type capable of simplifying `MirScalarExpr`s.
    pub trait ExpressionReducer {
        /// Attempt to replace `expr` itself with another expression.
        /// Returns true if it does so.
        fn replace(&self, expr: &mut MirScalarExpr) -> bool;
        /// Attempt to replace any subexpressions of `expr` with other expressions.
        /// Returns true if it does so.
        fn reduce_expr(&self, expr: &mut MirScalarExpr) -> bool {
            let mut simplified = false;
            simplified = simplified || self.reduce_child(expr);
            simplified = simplified || self.replace(expr);
            simplified
        }
        /// Attempt to replace any subexpressions of `expr`'s children with other expressions.
        /// Returns true if it does so.
        fn reduce_child(&self, expr: &mut MirScalarExpr) -> bool {
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
    }

    impl ExpressionReducer for BTreeMap<&MirScalarExpr, &MirScalarExpr> {
        /// Perform any exact replacement for `expr`, report if it had an effect.
        fn replace(&self, expr: &mut MirScalarExpr) -> bool {
            if let Some(other) = self.get(expr) {
                if other != &expr {
                    expr.clone_from(other);
                    return true;
                }
            }
            false
        }
    }

    impl ExpressionReducer for BTreeMap<MirScalarExpr, MirScalarExpr> {
        /// Perform any exact replacement for `expr`, report if it had an effect.
        fn replace(&self, expr: &mut MirScalarExpr) -> bool {
            if let Some(other) = self.get(expr) {
                if other != expr {
                    expr.clone_from(other);
                    return true;
                }
            }
            false
        }
    }

    trait UnionFind<T> {
        /// Sets `self[x]` to the root from `x`, and returns a reference to the root.
        fn find<'a>(&'a mut self, x: &T) -> Option<&'a T>;
        /// Ensures that `x` and `y` have the same root.
        fn union(&mut self, x: &T, y: &T);
    }

    impl<T: Clone + Ord> UnionFind<T> for BTreeMap<T, T> {
        fn find<'a>(&'a mut self, x: &T) -> Option<&'a T> {
            if !self.contains_key(x) {
                None
            } else {
                if self[x] != self[&self[x]] {
                    // Path halving
                    let mut y = self[x].clone();
                    while y != self[&y] {
                        let grandparent = self[&self[&y]].clone();
                        *self.get_mut(&y).unwrap() = grandparent;
                        y.clone_from(&self[&y]);
                    }
                    *self.get_mut(x).unwrap() = y;
                }
                Some(&self[x])
            }
        }

        fn union(&mut self, x: &T, y: &T) {
            match (self.find(x).is_some(), self.find(y).is_some()) {
                (true, true) => {
                    if self[x] != self[y] {
                        let root_x = self[x].clone();
                        let root_y = self[y].clone();
                        self.insert(root_x, root_y);
                    }
                }
                (false, true) => {
                    self.insert(x.clone(), self[y].clone());
                }
                (true, false) => {
                    self.insert(y.clone(), self[x].clone());
                }
                (false, false) => {
                    self.insert(x.clone(), x.clone());
                    self.insert(y.clone(), x.clone());
                }
            }
        }
    }
}
