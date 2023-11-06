// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Propagates expression equivalence from leaves to root, and back down again.
//! 
//! Expression equivalences are `MirScalarExpr` replacements by simpler expressions.
//! These equivalences derive from
//!   Filters:  predicates must evaluate to `Datum::True`.
//!   Maps:     new columns equal the expressions that define them.
//!   Joins:    equated columns must be equal.
//!   Others:   lots of other predicates we might learn (range constraints on aggregates; non-negativity)
//!
//! From leaf to root the equivalences are *enforced*, and communicate that the expression will not produce rows that do not satisfy the equivalence.
//! From root to leaf the equivalences are *advised*, and communicate that the expression may discard any outputs that do not satisfy the equivalence.
//!
//! Importantly, in descent the operator *may not* assume any equivalence filtering will be applied to its results.
//! It cannot therefore produce rows it would otherwise not, even rows that do not satisfy the equivalence.
//! Operators *may* introduce filtering in descent, and they must do so to take further advantage of the equivalences. 
//!
//! The subtlety is due to the expressions themselves causing the equivalences, and allowing more rows may invalidate equivalences.
//! For example, we might learn that `Column(7)` equals `Literal(3)`, but must refrain from introducing that substitution in descent,
//! because it is possible that the equivalence derives from restrictions in the expression we are visiting. Were we certain that the
//! equivalence was independent of the expression (e.g. through a more nuanced expression traversal) we could imaging relaxing this.

use std::collections::BTreeMap;

use mz_expr::{Id, MirRelationExpr, MirScalarExpr};
use mz_repr::Datum;

use crate::{TransformCtx, TransformError};

/// Pulls up and pushes down predicate information represented as equivalences
#[derive(Debug, Default)]
pub struct EquivalencePropagation;

impl crate::Transform for EquivalencePropagation {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "equivalence_propagation")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        let mut empty = BTreeMap::new();
        // let pre = relation.clone();
        self.pull(relation, &mut empty);
        mz_repr::explain::trace_plan(&*relation);
        // if pre != *relation {
        //     println!("PREV: {:?}", pre);
        //     println!("NEXT: {:?}", relation);
        // }
        relation.typ();
        Ok(())
    }
}

impl EquivalencePropagation {

    /// Returns equivalence classes that are ensured by `relation`.
    pub fn pull(
        &self,
        relation: &mut MirRelationExpr,
        get_equivalences: &mut BTreeMap<Id, EquivalenceClasses>,
    ) -> EquivalenceClasses {
        match relation {
            MirRelationExpr::Constant { rows, typ } => {
                // Trawl `rows` for any constant information worth recording.
                // Literal columns may be valuable; non-nullability could be too.
                let mut equivalences = EquivalenceClasses::default();
                if let Ok([(row, _cnt), rows @ ..]) = rows.as_deref() {
                    // Vector of `Option<Datum>` which becomes `None` once a column has a second datum.
                    let len = row.iter().count();
                    let mut common = Vec::with_capacity(len);
                    common.extend(row.iter().map(|d| Some(d)));

                    for (row, _cnt) in rows.iter() {
                        for (datum, common) in row.iter().zip(common.iter_mut()) {
                            if Some(datum) != *common {
                                *common = None;
                            }
                        }
                    }
                    for (index, common) in common.into_iter().enumerate() {
                        if let Some(datum) = common {
                            equivalences.classes.push(vec![
                                MirScalarExpr::Column(index), 
                                MirScalarExpr::literal_ok(datum, typ.column_types[index].scalar_type.clone())
                            ]);
                        }
                    }
                }
                equivalences.minimize();
                equivalences
            }
            MirRelationExpr::Get { id, .. } => {
                // Find local identifiers, but nothing for external identifiers.
                if let Some(equivalences) = get_equivalences.get(id) {
                    equivalences.clone()
                } else {
                    EquivalenceClasses::default()
                }
            }
            MirRelationExpr::Let { id, body, value } => {
                // Determine any equivalences for `value`, then use in `body`.
                let value_equivalences = self.pull(value, get_equivalences);
                get_equivalences.insert(Id::Local(*id), value_equivalences);
                self.pull(body, get_equivalences)
            }
            MirRelationExpr::LetRec {
                ids,
                values,
                limits: _,
                body,
            } => {
                // Start with trivial (empty) equivalence classes for each identifier.
                get_equivalences.extend(ids.iter().map(|id| (Id::Local(*id), EquivalenceClasses::default())));

                // It is inappropriate to iterate through a method that may mutate operators,
                // as the mutation may invalidate its enforcement. If we install equivalence
                // enforces atop each `Let` binding, and ensure they advance monotonically, 
                // this might be fine. 

                // let mut improvement = true;
                // while improvement {
                //     improvement = false;
                    for (id, value) in ids.iter().zip(values) {
                        let value_equivalences = self.pull(value, get_equivalences);
                        if get_equivalences[&Id::Local(*id)] != value_equivalences {
                            get_equivalences.insert(Id::Local(*id), value_equivalences);
                            // improvement = true;
                        }
                    }
                // }

                self.pull(body, get_equivalences)
            }
            MirRelationExpr::Project { input, outputs } => { 
                // restrict equivalences, and introduce equivalences for repeated outputs.
                let mut equivalences = self.pull(input, get_equivalences);
                equivalences.project(outputs);
                equivalences
            }
            MirRelationExpr::Map { input, scalars } => { 
                // introduce equivalences for new columns and expressions that define them.
                let mut equivalences = self.pull(input, get_equivalences);
                // Reduce `scalars` based on the promises of `input` equivalences.
                for expr in scalars.iter_mut() {
                    equivalences.reduce_expr(expr);
                }

                let input_arity = input.arity();
                for (pos, expr) in scalars.iter().enumerate() {
                    equivalences.classes.push(vec![MirScalarExpr::Column(input_arity + pos), expr.clone()]);
                }
                equivalences.minimize();
                equivalences
            }
            MirRelationExpr::FlatMap { input, exprs, .. } => { 
                // not sure we can do anything here.
                let equivalences = self.pull(input, get_equivalences);
                for expr in exprs.iter_mut() {
                    equivalences.reduce_expr(expr);
                }
                equivalences
            }
            MirRelationExpr::Filter { input, predicates } => { 
                let mut equivalences = self.pull(input, get_equivalences);
                // Reduce `predicates` based on the promises of `input` equivalences.
                for expr in predicates.iter_mut() {
                    equivalences.reduce_expr(expr);
                }
                predicates.sort();
                predicates.dedup();
                // TODO: further simplify `predicates`
                // introduce equivalences to `Datum::True`.
                let mut class = predicates.clone();
                class.push(MirScalarExpr::literal_ok(Datum::True, mz_repr::ScalarType::Bool));
                equivalences.classes.push(class);
                equivalences.minimize();
                equivalences
            }

            MirRelationExpr::Join {
                inputs,
                equivalences,
                ..
            } => {
                // Collect equivalences from all inputs;
                let mut columns = 0;
                let mut result = EquivalenceClasses::default();
                for input in inputs.iter_mut() {
                    let input_arity = input.arity();
                    let mut equivalences = self.pull(input, get_equivalences);
                    let permutation = (columns .. (columns + input_arity)).collect::<Vec<_>>();
                    equivalences.permute(&permutation);
                    result.classes.extend(equivalences.classes);
                    columns += input_arity;
                }

                // We can apply the equivalences to the join equivalences.
                // This is acceptable to do, but it is perhaps harmful, in that we may remove
                // valuable signals for what we must join on. E.g. if we simplify `Col(3)` to `Col(1)`,
                // without introducing their equivalence as a constraint, we may miss an index on `Col(3)`
                // and instead build one on `Col(1)`.
                for class in equivalences.iter_mut() {
                    for expr in class.iter_mut() {
                        result.reduce_expr(expr);
                    }
                }
                // We could plausibly *introduce* the equivalences to the join equivalences,
                // but this has the potential to introduce work we are not compelled to perform,
                // for uncertain benefit. We should always be able to re-derive the equivalences,
                // and optimize them away, in the future if we do insert them here.

                // Fold join equivalences into our results.
                result.classes.extend(equivalences.iter().cloned());
                result.minimize();

                // TODO: This is an opportunity to press what we know now back to children.
                //       Ideally we would do it in combination with information from parents.

                result
            }
            MirRelationExpr::Reduce { input, group_key, aggregates, .. } => {
                let mut equivalences = self.pull(input, get_equivalences);
                // Reduce group key and aggregate expressions based on promises of `input` equivalences.
                for expr in group_key.iter_mut() {
                    equivalences.reduce_expr(expr);
                }
                for aggr in aggregates.iter_mut() {
                    equivalences.reduce_expr(&mut aggr.expr);
                }
                // Introduce keys column equivalences as a map, then project to them as a projection.
                let input_arity = input.arity();
                for (pos, expr) in group_key.iter().enumerate() {
                    equivalences.classes.push(vec![MirScalarExpr::Column(input_arity + pos), expr.clone()]);
                }
                equivalences.minimize();
                let outputs = (input_arity .. (input_arity + group_key.len())).collect::<Vec<_>>();
                equivalences.project(&outputs[..]);

                // TODO: There are cases where equivalences can make their way through,
                //       such as `MIN`, `MAX`, `ANY`, `ALL` aggregates that surface columns.

                equivalences
            }
            MirRelationExpr::TopK { input, .. } => {
                // Nothing to do.
                self.pull(input, get_equivalences)
            }
            MirRelationExpr::Negate { input } => {
                // Nothing to do.
                self.pull(input, get_equivalences)
            }
            MirRelationExpr::Threshold { input } => {
                // Nothing to do.
                self.pull(input, get_equivalences)
            }
            MirRelationExpr::Union { base, inputs } => {
                // Restrict equivalences to those common in all inputs.
                let mut equivalences = self.pull(base, get_equivalences);
                for input in inputs.iter_mut() {
                    equivalences = equivalences.union(&self.pull(input, get_equivalences))
                }
                equivalences
            }
            MirRelationExpr::ArrangeBy { input, .. } => {
                // Nothing to do.
                self.pull(input, get_equivalences)
            }
        } 
    }

    // /// Impresses upon `relation` equivalences that can further restrict the records `relation` produces.
    // ///
    // /// These equivalences are not *enforced* atop `relation`, and it is incorrect for `relation` to emit 
    // /// additional records under the premise that they will be filtered out (they may not be). However, if
    // /// `relation` fails to produce any records that do not satisfy these equivalences, the ultimate results
    // /// will remain correct.
    // ///
    // /// Essentially, `relation` may optionally install a `Filter` containing any subset of these equivalences.
    // /// If this improves a `Join` by providing more equivalences, great! If this allows additional predicate 
    // /// pushdown through a `Let` binding, great!
    // pub fn push(
    //     &self,
    //     relation: &mut MirRelationExpr,
    //     outer_equivalences: EquivalenceClasses,
    //     get_equivalences: &mut BTreeMap<Id, EquivalenceClasses>,
    // ) {
    // }
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
    classes: Vec<Vec<MirScalarExpr>>,
}

impl EquivalenceClasses {
    /// Update `self` to maintain the same equivalences which potentially reducing along `Ord::le`.
    /// 
    /// Informally this means simplifying constraints, removing redundant constraints, and unifying equivalence classes.
    fn minimize(&mut self) {
        // Repeatedly, we reduce each of the classes themselves, then unify the classes.
        // This should strictly reduce complexity, and reach a fixed point.
        // Ideally it is *confluent*, arriving at the same fixed point no matter the order of operations.

        for class in self.classes.iter_mut() {
            class.sort_by(|e1,e2| {
                match (e1, e2) {
                    (MirScalarExpr::Literal(_,_), MirScalarExpr::Literal(_,_)) => e1.cmp(e2),
                    (MirScalarExpr::Literal(_,_), _) => std::cmp::Ordering::Less,
                    (_, MirScalarExpr::Literal(_,_)) => std::cmp::Ordering::Greater,
                    (x, y) => x.cmp(y),
                }
            });
            class.dedup();
        }
        self.classes.retain(|c| c.len() > 1);
        self.classes.sort();

        // We continue as long as any simplification has occurred.
        // An expression can be simplified, a duplication found, or two classes unified.
        let mut complete = false;
        while !complete {
            // We are complete unless we experience an expression simplification, or an equivalence class unification.
            complete = true;

            // 1. Reduce each class.
            //    Each class can be reduced in the context of *other* classes, which are available for substitution.
            for class_index in 0 .. self.classes.len() {
                for index in 0 .. self.classes[class_index].len() {
                    let mut cloned = self.classes[class_index][index].clone();
                    // Use `reduce_child` rather than `reduce_expr` to avoid entire expression replacement.
                    let reduced = self.reduce_child(&mut cloned);
                    if reduced {
                        self.classes[class_index][index] = cloned;
                        complete = false;
                    }
                }
            }

            // 2. Unify classes.
            //    If the same expression is in two classes, we can unify the classes.
            //    This element may not be the representative.
            //    TODO: If all lists are sorted, this could be a linear merge among all.
            //          They stop being sorted as soon as we make any modification, though.
            //          But, it would be a fast rejection when faced with lots of data.
            for index1 in 0 .. self.classes.len() {
                for index2 in 0 .. index1 {
                    if self.classes[index1].iter().any(|x| self.classes[index2].iter().any(|y| x == y)) {
                        let prior = std::mem::take(&mut self.classes[index2]);
                        self.classes[index1].extend(prior);
                        complete = false;
                    }
                }
            }

            // Tidy up classes, restore representative.
            for class in self.classes.iter_mut() {
                // TODO: Ideally we put literals as representatives, to reduce dependencies on rows.
                class.sort_by(|e1,e2| {
                    match (e1, e2) {
                        (MirScalarExpr::Literal(_,_), MirScalarExpr::Literal(_,_)) => e1.cmp(e2),
                        (MirScalarExpr::Literal(_,_), _) => std::cmp::Ordering::Less,
                        (_, MirScalarExpr::Literal(_,_)) => std::cmp::Ordering::Greater,
                        (x, y) => x.cmp(y),
                    }
                });
                class.dedup();
            }

            // Discard trivial equivalence classes.
            self.classes.retain(|class| class.len() > 1);
            self.classes.sort();
        }
    }

    /// Produce the equivalences present in both inputs.
    fn union(&self, other: &Self) -> Self {

        // TODO: seems like this could be extended, with similar concepts to localization:
        //       We may removed non-shared constraints, but ones that remain could take over
        //       and substitute in to retain more equivalences.

        // For each pair of equivalence clasess, their intersection.
        let mut equivalences = EquivalenceClasses { classes: Vec::new() };
        for class1 in self.classes.iter() {
            for class2 in other.classes.iter() {
                let class = class1.iter().filter(|e1| class2.iter().any(|e2| e1 == &e2)).cloned().collect::<Vec<_>>();
                if class.len() > 1 {
                    equivalences.classes.push(class);
                }
            }
        }
        equivalences.minimize();
        equivalences
    }

    fn permute(&mut self, permutation: &[usize]) {
        for class in self.classes.iter_mut() {
            for expr in class.iter_mut() {
                expr.permute(permutation);
            }
        }
    }

    /// Subject the constraints to the column projection, reworking and removing equivalences.
    ///
    /// This method should also introduce equivalences representing any repeated columns.
    fn project(&mut self, output_columns: &[usize]) {
        let remap = output_columns.iter().enumerate().map(|(idx, col)| (*col, idx)).collect::<BTreeMap<_,_>>();

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
                if !class[0].support().iter().all(|c| output_columns.contains(c)) {
                    if let Some(pos) = class.iter().position(|e| e.support().iter().all(|c| output_columns.contains(c))) {
                        class.swap(0, pos);
                        localized = false;
                    }
                }
            }
            // attempt to replace representatives with equivalent localizeable expressions.
            for class_index in 0 .. self.classes.len() {
                for index in 0 .. self.classes[class_index].len() {
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
            class.retain(|e| e.support().iter().all(|c| output_columns.contains(c)));
            for expr in class.iter_mut() {
                expr.permute_map(&remap);
            }
        }
        self.classes.retain(|c| c.len() > 1);
        // If column repetitions, introduce them as equivalences.
        // We introduce only the equivalence to the first occurrence, and rely on minimization to collect them.
        for c in 0 .. output_columns.len() {
            if let Some(pos) = output_columns[..c].iter().position(|x| x == &output_columns[c]) {
                self.classes.push(vec![MirScalarExpr::Column(pos), MirScalarExpr::Column(c)]);
            }
        }

        self.minimize();
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
    fn reduce_expr(&self, expr: &mut MirScalarExpr) -> bool {
        let mut simplified = false;
        simplified = simplified || self.reduce_child(expr);
        simplified = simplified || self.replace(expr);
        simplified
    }
    
    /// Perform any simplification on children, report if effective.
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
            MirScalarExpr::If { cond, then, els} => {
                simplified = self.reduce_expr(cond) || simplified;
                simplified = self.reduce_expr(then) || simplified;
                simplified = self.reduce_expr(els) || simplified;
            }
            _ => { }
        }
        simplified
    }
}