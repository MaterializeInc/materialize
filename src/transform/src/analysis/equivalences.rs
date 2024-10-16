// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An analysis that reports all known-equivalent expressions for each relation.
//!
//! Expressions are equivalent at a relation if they are certain to evaluate to
//! the same `Datum` for all records in the relation.
//!
//! Equivalences are recorded in an `EquivalenceClasses`, which lists all known
//! equivalences classes, each a list of equivalent expressions.

use std::collections::BTreeMap;

use mz_expr::{Id, MirRelationExpr, MirScalarExpr};
use mz_repr::{ColumnType, Datum};

use crate::analysis::{Analysis, Lattice};
use crate::analysis::{Arity, RelationType};
use crate::analysis::{Derived, DerivedBuilder};

/// Pulls up and pushes down predicate information represented as equivalences
#[derive(Debug, Default)]
pub struct Equivalences;

impl Analysis for Equivalences {
    // A `Some(list)` indicates a list of classes of equivalent expressions.
    // A `None` indicates all expressions are equivalent, including contradictions;
    // this is only possible for the empty collection, and as an initial result for
    // unconstrained recursive terms.
    type Value = Option<EquivalenceClasses>;

    fn announce_dependencies(builder: &mut DerivedBuilder) {
        builder.require(Arity);
        builder.require(RelationType); // needed for expression reduction.
    }

    fn derive(
        &self,
        expr: &MirRelationExpr,
        index: usize,
        results: &[Self::Value],
        depends: &Derived,
    ) -> Self::Value {
        let mut equivalences = match expr {
            MirRelationExpr::Constant { rows, typ } => {
                // Trawl `rows` for any constant information worth recording.
                // Literal columns may be valuable; non-nullability could be too.
                let mut equivalences = EquivalenceClasses::default();
                if let Ok([(row, _cnt), rows @ ..]) = rows.as_deref() {
                    // Vector of `Option<Datum>` which becomes `None` once a column has a second datum.
                    let len = row.iter().count();
                    let mut common = Vec::with_capacity(len);
                    common.extend(row.iter().map(Some));
                    // Prep initial nullability information.
                    let mut nullable_cols = common
                        .iter()
                        .map(|datum| datum == &Some(Datum::Null))
                        .collect::<Vec<_>>();

                    for (row, _cnt) in rows.iter() {
                        for ((datum, common), nullable) in row
                            .iter()
                            .zip(common.iter_mut())
                            .zip(nullable_cols.iter_mut())
                        {
                            if Some(datum) != *common {
                                *common = None;
                            }
                            if datum == Datum::Null {
                                *nullable = true;
                            }
                        }
                    }
                    for (index, common) in common.into_iter().enumerate() {
                        if let Some(datum) = common {
                            equivalences.classes.push(vec![
                                MirScalarExpr::Column(index),
                                MirScalarExpr::literal_ok(
                                    datum,
                                    typ.column_types[index].scalar_type.clone(),
                                ),
                            ]);
                        }
                    }
                    // If any columns are non-null, introduce this fact.
                    if nullable_cols.iter().any(|x| !*x) {
                        let mut class = vec![MirScalarExpr::literal_false()];
                        for (index, nullable) in nullable_cols.iter().enumerate() {
                            if !*nullable {
                                class.push(MirScalarExpr::column(index).call_is_null());
                            }
                        }
                        equivalences.classes.push(class);
                    }
                }
                Some(equivalences)
            }
            MirRelationExpr::Get { id, typ, .. } => {
                let mut equivalences = Some(EquivalenceClasses::default());
                // Find local identifiers, but nothing for external identifiers.
                if let Id::Local(id) = id {
                    if let Some(offset) = depends.bindings().get(id) {
                        // It is possible we have derived nothing for a recursive term
                        if let Some(result) = results.get(*offset) {
                            equivalences.clone_from(result);
                        } else {
                            // No top element was prepared.
                            // This means we are executing pessimistically,
                            // but perhaps we must because optimism is off.
                        }
                    }
                }
                // Incorporate statements about column nullability.
                let mut non_null_cols = vec![MirScalarExpr::literal_false()];
                for (index, col_type) in typ.column_types.iter().enumerate() {
                    if !col_type.nullable {
                        non_null_cols.push(MirScalarExpr::column(index).call_is_null());
                    }
                }
                if non_null_cols.len() > 1 {
                    if let Some(equivalences) = equivalences.as_mut() {
                        equivalences.classes.push(non_null_cols);
                    }
                }

                equivalences
            }
            MirRelationExpr::Let { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::LetRec { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Project { outputs, .. } => {
                // restrict equivalences, and introduce equivalences for repeated outputs.
                let mut equivalences = results.get(index - 1).unwrap().clone();
                equivalences
                    .as_mut()
                    .map(|e| e.project(outputs.iter().cloned()));
                equivalences
            }
            MirRelationExpr::Map { scalars, .. } => {
                // introduce equivalences for new columns and expressions that define them.
                let mut equivalences = results.get(index - 1).unwrap().clone();
                if let Some(equivalences) = &mut equivalences {
                    let input_arity = depends.results::<Arity>().unwrap()[index - 1];
                    for (pos, expr) in scalars.iter().enumerate() {
                        equivalences
                            .classes
                            .push(vec![MirScalarExpr::Column(input_arity + pos), expr.clone()]);
                    }
                }
                equivalences
            }
            MirRelationExpr::FlatMap { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Filter { predicates, .. } => {
                let mut equivalences = results.get(index - 1).unwrap().clone();
                if let Some(equivalences) = &mut equivalences {
                    let mut class = predicates.clone();
                    class.push(MirScalarExpr::literal_ok(
                        Datum::True,
                        mz_repr::ScalarType::Bool,
                    ));
                    equivalences.classes.push(class);
                }
                equivalences
            }
            MirRelationExpr::Join { equivalences, .. } => {
                // Collect equivalences from all inputs;
                let expr_index = index;
                let mut children = depends
                    .children_of_rev(expr_index, expr.children().count())
                    .collect::<Vec<_>>();
                children.reverse();

                let arity = depends.results::<Arity>().unwrap();
                let mut columns = 0;
                let mut result = Some(EquivalenceClasses::default());
                for child in children.into_iter() {
                    let input_arity = arity[child];
                    let equivalences = results[child].clone();
                    if let Some(mut equivalences) = equivalences {
                        let permutation = (columns..(columns + input_arity)).collect::<Vec<_>>();
                        equivalences.permute(&permutation);
                        result
                            .as_mut()
                            .map(|e| e.classes.extend(equivalences.classes));
                    } else {
                        result = None;
                    }
                    columns += input_arity;
                }

                // Fold join equivalences into our results.
                result
                    .as_mut()
                    .map(|e| e.classes.extend(equivalences.iter().cloned()));
                result
            }
            MirRelationExpr::Reduce {
                group_key,
                aggregates: _,
                ..
            } => {
                let input_arity = depends.results::<Arity>().unwrap()[index - 1];
                let mut equivalences = results.get(index - 1).unwrap().clone();
                if let Some(equivalences) = &mut equivalences {
                    // Introduce keys column equivalences as a map, then project to them as a projection.
                    for (pos, expr) in group_key.iter().enumerate() {
                        equivalences
                            .classes
                            .push(vec![MirScalarExpr::Column(input_arity + pos), expr.clone()]);
                    }

                    // Having added classes to `equivalences`, we should minimize the classes to fold the
                    // information in before applying the `project`, to set it up for success.
                    equivalences.minimize(&None);

                    // TODO: MIN, MAX, ANY, ALL aggregates pass through all certain properties of their columns.
                    // They also pass through equivalences of them and other constant columns (e.g. key columns).
                    // However, it is not correct to simply project onto these columns, as relationships amongst
                    // aggregate columns may no longer be preserved. MAX(col) != MIN(col) even though col = col.
                    // TODO: COUNT ensures a non-null value.
                    equivalences.project(input_arity..(input_arity + group_key.len()));
                }
                equivalences
            }
            MirRelationExpr::TopK { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Negate { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Threshold { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Union { .. } => {
                // TODO: `EquivalenceClasses::union` takes references, and we could probably skip much of this cloning.
                let expr_index = index;
                depends
                    .children_of_rev(expr_index, expr.children().count())
                    .flat_map(|c| results[c].clone())
                    .reduce(|e1, e2| e1.union(&e2))
            }
            MirRelationExpr::ArrangeBy { .. } => results.get(index - 1).unwrap().clone(),
        };

        let expr_type = depends.results::<RelationType>().unwrap()[index].clone();
        equivalences.as_mut().map(|e| e.minimize(&expr_type));
        equivalences
    }

    fn lattice() -> Option<Box<dyn Lattice<Self::Value>>> {
        Some(Box::new(EQLattice))
    }
}

struct EQLattice;

impl Lattice<Option<EquivalenceClasses>> for EQLattice {
    fn top(&self) -> Option<EquivalenceClasses> {
        None
    }

    fn meet_assign(
        &self,
        a: &mut Option<EquivalenceClasses>,
        b: Option<EquivalenceClasses>,
    ) -> bool {
        match (&mut *a, b) {
            (_, None) => false,
            (None, b) => {
                *a = b;
                true
            }
            (Some(a), Some(b)) => {
                let mut c = a.union(&b);
                std::mem::swap(a, &mut c);
                a != &mut c
            }
        }
    }
}

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
    /// Comparator function for the complexity of scalar expressions. Simpler expressions are
    /// smaller. Can be used when we need to decide which of several equivalent expressions to use.
    pub fn mir_scalar_expr_complexity(
        e1: &MirScalarExpr,
        e2: &MirScalarExpr,
    ) -> std::cmp::Ordering {
        use std::cmp::Ordering::*;
        use MirScalarExpr::*;
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

    /// Sorts and deduplicates each class, and the classes themselves.
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

    /// Update `self` to maintain the same equivalences which potentially reducing along `Ord::le`.
    ///
    /// Informally this means simplifying constraints, removing redundant constraints, and unifying equivalence classes.
    pub fn minimize(&mut self, columns: &Option<Vec<ColumnType>>) {
        // Repeatedly, we reduce each of the classes themselves, then unify the classes.
        // This should strictly reduce complexity, and reach a fixed point.
        // Ideally it is *confluent*, arriving at the same fixed point no matter the order of operations.
        self.tidy();

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
            stable = self.minimize_once(&columns);
        }

        // TODO: remove these measures once we are more confident about idempotence.
        let prev = self.clone();
        self.minimize_once(&columns);
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
                    expr.reduce(columns);
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
        //    `expr_to_class_index` tells us for each expression the class index where we last saw
        //    it.
        //    `to_merge` has pairs of classes to be merged. The first element of the pair should be
        //    an earlier class than the second, and `to_merge` should be in sorted order. (These
        //    invariants are important when handling expressions that appear in more than two
        //    classes: we'll first merge the first two into the second, and then all this into the
        //    third, and so on.)
        let mut expr_to_class_index = BTreeMap::new();
        let mut to_merge = Vec::new();
        for (index, class) in self.classes.iter().enumerate() {
            for expr in class {
                if let Some(other_index) = expr_to_class_index.get(expr) {
                    to_merge.push((*other_index, index));
                }
            }
            for expr in class {
                expr_to_class_index.insert(expr, index);
            }
        }
        for (from, to) in to_merge {
            let prior = std::mem::take(&mut self.classes[from]);
            self.classes[to].extend(prior);
            stable = false;
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
                        func: mz_expr::BinaryFunc::Eq,
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
                        func: mz_expr::BinaryFunc::Eq,
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
                        func: mz_expr::UnaryFunc::Not(_),
                        expr: e,
                    } = expr
                    {
                        to_add.push(vec![MirScalarExpr::literal_false(), (**e).clone()]);
                        stable = false;
                    }
                }
                class.retain(|expr| {
                    if let MirScalarExpr::CallUnary {
                        func: mz_expr::UnaryFunc::Not(_),
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
                        func: mz_expr::UnaryFunc::Not(_),
                        expr: e,
                    } = expr
                    {
                        to_add.push(vec![MirScalarExpr::literal_true(), (**e).clone()]);
                        stable = false;
                    }
                }
                class.retain(|expr| {
                    if let MirScalarExpr::CallUnary {
                        func: mz_expr::UnaryFunc::Not(_),
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
