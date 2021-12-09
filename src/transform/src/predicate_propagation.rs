// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Lifts information about predicates towards the expression root.
//!
//! This transform produces a collection of predicates that could be harmlessly
//! applied to the argument expression, in the sense that they would neither
//! discard any records nor produce errors.
//!
//! This transform does not move the predicates, which remain in force
//! in their current location, but it is able to simplify expressions
//! as it moves from inputs toward the query root.
//!
//! Importantly, any simplifications performed must all be robust to the possibility
//! that the filter might move elsewhere. In particular, we should perform
//! no simplifications that could result in errors that could not otherwise
//! occur (e.g. by optimizing the `cond` of an `If` expression).

use std::collections::{HashMap, HashSet};

use crate::TransformArgs;
use expr::{
    BinaryFunc, JoinInputMapper, MirRelationExpr, MirScalarExpr, UnaryFunc, RECURSION_LIMIT,
};
use itertools::Itertools;
use ore::stack::{CheckedRecursion, RecursionGuard};
use repr::Datum;
use repr::ScalarType;

/// Harvest and act upon per-column information.
#[derive(Debug)]
pub struct PredicateKnowledge {
    recursion_guard: RecursionGuard,
}

impl Default for PredicateKnowledge {
    fn default() -> PredicateKnowledge {
        PredicateKnowledge {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl CheckedRecursion for PredicateKnowledge {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl crate::Transform for PredicateKnowledge {
    fn transform(
        &self,
        expr: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let _predicates = self.action(expr, &mut HashMap::new())?;
        Ok(())
    }
}

impl PredicateKnowledge {
    /// Harvest predicates known to be true of the columns in scope.
    ///
    /// The `let_knowledge` argument contains predicates known to be true for a let binding.
    ///
    /// The list of scalar expressions returned is meant to have some structure. Any column
    /// equivalence is expressed by a single `#col1 = #col2` and should inherit the
    /// implications of the first column's role in any predicates.
    pub fn action(
        &self,
        expr: &mut MirRelationExpr,
        let_knowledge: &mut HashMap<expr::Id, Vec<Constraint>>,
    ) -> Result<Vec<Constraint>, crate::TransformError> {
        self.checked_recur(|_| {
            let self_type = expr.typ();
            let mut materialize_predicates = false;
            let mut predicates = match expr {
                MirRelationExpr::ArrangeBy { input, .. } => self.action(input, let_knowledge)?,
                MirRelationExpr::DeclareKeys { input, .. } => self.action(input, let_knowledge)?,
                MirRelationExpr::Get { id, typ: _ } => {
                    // If we fail to find bound knowledge, use at least the nullability of the type
                    // (added later for all expression types).
                    let_knowledge.get(id).cloned().unwrap_or_else(Vec::new)
                }
                MirRelationExpr::Constant { rows, typ } => {
                    // Each column could 1. be equal to a literal, 2. known to be non-null.
                    // They could be many other things too, but these are the ones we can handle.
                    if let Ok([(row, _cnt), rows @ ..]) = rows.as_deref_mut() {
                        // An option for each column that contains a common value.
                        let mut literal = row.iter().map(Some).collect::<Vec<_>>();
                        // True for each column that is never `Datum::Null`.
                        let mut non_null = literal
                            .iter()
                            .map(|x| x != &Some(Datum::Null))
                            .collect::<Vec<_>>();
                        for (row, _cnt) in rows.iter() {
                            for (index, datum) in row.iter().enumerate() {
                                if literal[index] != Some(datum) {
                                    literal[index] = None;
                                }
                                if datum == Datum::Null {
                                    non_null[index] = false;
                                }
                            }
                        }
                        // Load up a return predicate list with everything we have learned.
                        let mut predicates: Vec<Constraint> = Vec::new();
                        for column in 0..literal.len() {
                            if let Some(datum) = literal[column] {
                                // Having found a literal, if it is Null use `IsNull` and otherwise use `Eq`.
                                if datum == Datum::Null {
                                    predicates.push(Constraint::Predicate(
                                        MirScalarExpr::Column(column).call_unary(
                                            expr::UnaryFunc::IsNull(expr::func::IsNull),
                                        ),
                                    ));
                                } else {
                                    predicates.push(Constraint::Equivalence(vec![
                                        MirScalarExpr::Column(column),
                                        MirScalarExpr::literal(
                                            Ok(datum),
                                            typ.column_types[column].scalar_type.clone(),
                                        ),
                                    ]));
                                }
                            }
                            if non_null[column] {
                                predicates.push(Constraint::Predicate(
                                    MirScalarExpr::column(column)
                                        .call_unary(UnaryFunc::IsNull(expr::func::IsNull))
                                        .call_unary(UnaryFunc::Not(expr::func::Not)),
                                ));
                            }
                        }
                        predicates
                    } else {
                        // Everything is true about an empty collection, but let's have other rules
                        // optimize them away.

                        Vec::new()
                    }
                }
                MirRelationExpr::Let { id, value, body } => {
                    // This deals with shadowed let bindings, but perhaps we should just complain instead?
                    let value_knowledge = self.action(value, let_knowledge)?;
                    let prior_knowledge =
                        let_knowledge.insert(expr::Id::Local(id.clone()), value_knowledge);
                    let body_knowledge = self.action(body, let_knowledge)?;
                    let_knowledge.remove(&expr::Id::Local(id.clone()));
                    if let Some(prior_knowledge) = prior_knowledge {
                        let_knowledge.insert(expr::Id::Local(id.clone()), prior_knowledge);
                    }
                    body_knowledge
                }
                MirRelationExpr::Project { input, outputs } => {
                    let mut input_knowledge =
                        denormalize_predicates(self.action(input, let_knowledge)?);
                    // Need to
                    // 1. restrict to supported columns,
                    // 2. rewrite constraints using new column identifiers,
                    // 3. add equivalence constraints for repeated columns.
                    let mut remap = HashMap::new();
                    for (index, column) in outputs.iter().enumerate() {
                        if !remap.contains_key(column) {
                            remap.insert(*column, index);
                        }
                    }

                    // 1. restrict to supported columns,
                    input_knowledge
                        .retain(|expr| expr.support().into_iter().all(|c| remap.contains_key(&c)));

                    // 2. rewrite constraints using new column identifiers,
                    for predicate in input_knowledge.iter_mut() {
                        predicate.permute_map(&remap);
                    }

                    // 3. add equivalence constraints for repeated columns.
                    for (index, column) in outputs.iter().enumerate() {
                        if remap[column] != index {
                            input_knowledge.push(Constraint::Equivalence(vec![
                                MirScalarExpr::Column(remap[column]),
                                MirScalarExpr::Column(index),
                            ]));
                        }
                    }
                    input_knowledge
                }
                MirRelationExpr::Map { input, scalars } => {
                    let input_knowledge = self.action(input, let_knowledge)?;
                    let input_arity = input.arity();
                    let structured = PredicateStructure::new(&input_knowledge);
                    let mut output_knowledge = input_knowledge.clone();
                    // Scalars could be simplified based on known predicates.
                    for (index, scalar) in scalars.iter_mut().enumerate() {
                        optimize(scalar, &structured);
                        output_knowledge.push(Constraint::Equivalence(vec![
                            MirScalarExpr::Column(input_arity + index),
                            scalar.clone(),
                        ]));
                    }
                    output_knowledge
                }
                MirRelationExpr::FlatMap {
                    input,
                    func: _,
                    exprs,
                } => {
                    let input_knowledge = self.action(input, let_knowledge)?;
                    let structured = PredicateStructure::new(&input_knowledge);
                    for expr in exprs {
                        optimize(expr, &structured);
                    }
                    input_knowledge
                }
                MirRelationExpr::Filter { input, predicates } => {
                    let input_knowledge = self.action(input, let_knowledge)?;
                    let structured = PredicateStructure::new(&input_knowledge);
                    let mut output_knowledge = input_knowledge.clone();

                    for predicate in predicates.iter_mut() {
                        optimize(predicate, &structured);
                        append_constraint(&mut output_knowledge, predicate.clone());
                    }
                    output_knowledge
                }
                MirRelationExpr::Join {
                    inputs,
                    equivalences,
                    ..
                } => {
                    materialize_predicates = true;
                    let input_mapper = JoinInputMapper::new(inputs);

                    let mut knowledge = Vec::new();

                    // Collect input knowledge, but update column references.
                    for (input_idx, input) in inputs.iter_mut().enumerate() {
                        for predicate in self.action(input, let_knowledge)? {
                            knowledge.push(predicate.map_to_global(&input_mapper, input_idx));
                        }
                    }

                    let structured = PredicateStructure::new(&knowledge);
                    let mut new_predicates = Vec::new();

                    for equivalence in equivalences.iter_mut() {
                        for expr in equivalence.iter_mut() {
                            optimize(expr, &structured);
                        }

                        new_predicates.push(Constraint::Equivalence(equivalence.clone()));
                    }

                    knowledge.extend(new_predicates);
                    knowledge
                }
                MirRelationExpr::Reduce {
                    input,
                    group_key,
                    aggregates,
                    ..
                } => {
                    let input_type = input.typ();
                    let input_knowledge = self.action(input, let_knowledge)?;
                    let structured = PredicateStructure::new(&input_knowledge);
                    for key in group_key.iter_mut() {
                        optimize(key, &structured);
                    }
                    for aggregate in aggregates.iter_mut() {
                        optimize(&mut aggregate.expr, &structured);
                    }

                    // List of predicates we will return.
                    let mut predicates = Vec::new();

                    // Predicates that depend only on group keys should remain true of those columns.
                    // 0. Form a map from key expressions to the columns they will become.
                    let mut key_exprs = HashMap::new();
                    for (index, expr) in group_key.iter().enumerate() {
                        let key = expr.clone();
                        let new_column = MirScalarExpr::Column(index);
                        let existing_column =
                            key_exprs.entry(key).or_insert_with(|| new_column.clone());
                        // Report duplicated keys as equivalences
                        predicates.push(Constraint::Equivalence(vec![
                            existing_column.clone(),
                            new_column,
                        ]));
                    }
                    // 1. Visit each predicate, and collect those that reach no `ScalarExpr::Column` when substituting per `key_exprs`.
                    //    They will be preserved and presented as output.
                    for mut predicate in denormalize_predicates(input_knowledge.clone()) {
                        if predicate.substitute(&key_exprs) {
                            predicates.push(predicate);
                        }
                    }

                    // TODO: Predicates about columns that are certain aggregates can be preserved.
                    // For example, if a column Not(IsNull) or (= 7) then Min/Max of that column
                    // will have the same property.
                    for (index, aggregate) in aggregates.iter_mut().enumerate() {
                        let column = group_key.len() + index;
                        if let Some(Ok(literal)) = aggregate.as_literal() {
                            if literal == Datum::Null {
                                predicates.push(Constraint::Predicate(
                                    MirScalarExpr::Column(column)
                                        .call_unary(expr::UnaryFunc::IsNull(expr::func::IsNull)),
                                ));
                            } else {
                                predicates.push(Constraint::Equivalence(vec![
                                    MirScalarExpr::Column(column),
                                    MirScalarExpr::literal(
                                        Ok(literal),
                                        self_type.column_types[column].scalar_type.clone(),
                                    ),
                                ]));
                                predicates.push(Constraint::Predicate(
                                    MirScalarExpr::column(column)
                                        .call_unary(UnaryFunc::IsNull(expr::func::IsNull))
                                        .call_unary(UnaryFunc::Not(expr::func::Not)),
                                ));
                            }
                        } else {
                            // Aggregates that are non-literals may still be non-null.
                            if let expr::AggregateFunc::Count = aggregate.func {
                                // Replace counts of nonnull, nondistinct expressions with `true`.
                                if let MirScalarExpr::Column(c) = aggregate.expr {
                                    if !input_type.column_types[c].nullable && !aggregate.distinct {
                                        aggregate.expr = MirScalarExpr::literal_ok(
                                            Datum::True,
                                            ScalarType::Bool,
                                        );
                                    }
                                }
                                predicates.push(Constraint::Predicate(
                                    MirScalarExpr::column(column)
                                        .call_unary(UnaryFunc::IsNull(expr::func::IsNull))
                                        .call_unary(UnaryFunc::Not(expr::func::Not)),
                                ));
                            } else {
                                // TODO: Something more sophisticated using `input_knowledge` too.
                            }
                        }
                    }

                    predicates
                }
                MirRelationExpr::TopK { input, .. } => self.action(input, let_knowledge)?,
                MirRelationExpr::Negate { input } => self.action(input, let_knowledge)?,
                MirRelationExpr::Threshold { input } => self.action(input, let_knowledge)?,
                MirRelationExpr::Union { base, inputs } => {
                    let mut know1 = denormalize_predicates(self.action(base, let_knowledge)?);
                    for input in inputs.iter_mut() {
                        let know2 = denormalize_predicates(self.action(input, let_knowledge)?);
                        know1.retain(|predicate| know2.contains(predicate));
                    }
                    know1
                }
            };

            // Propagate the nullability flags of the projected columns as predicates
            predicates.extend(self_type.column_types.iter().enumerate().flat_map(
                |(col_idx, col_typ)| {
                    if !col_typ.nullable {
                        Some(Constraint::Predicate(
                            MirScalarExpr::column(col_idx)
                                .call_unary(UnaryFunc::IsNull(expr::func::IsNull))
                                .call_unary(UnaryFunc::Not(expr::func::Not)),
                        ))
                    } else {
                        None
                    }
                },
            ));

            normalize_predicates(&mut predicates, &self_type);
            if predicates
                .iter()
                .any(|p| p.is_literal_false() || p.is_literal_null())
            {
                expr.take_safely();
            } else if materialize_predicates && !predicates.is_empty() {
                *expr = expr
                    .take_dangerous()
                    .filter(predicates.iter().flat_map(|c| {
                        if let Constraint::Predicate(p) = &c {
                            Some(p.clone())
                        } else {
                            None
                        }
                    }));
            }
            Ok(predicates)
        })
    }
}

/// Models a constraint.
///
/// Used to lift knowledge through the relation graph.
#[derive(PartialOrd, Ord, PartialEq, Eq, Clone, Debug)]
pub enum Constraint {
    /// Represents a value equivalence, rather than a SQL equality.
    Equivalence(Vec<MirScalarExpr>),
    /// A constraint expressed as an expression.
    Predicate(MirScalarExpr),
}

impl Constraint {
    /// Returns an iterator over the expressions contained in the constraint.
    fn iter_expr<'a>(&'a self) -> Box<dyn Iterator<Item = &'a MirScalarExpr> + 'a> {
        match self {
            Self::Equivalence(class) => Box::new(class.iter()),
            Self::Predicate(p) => Box::new(std::iter::once(p)),
        }
    }

    /// Returns an iterator over mutable references of the expressions contained
    /// in the constraint.
    fn iter_expr_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut MirScalarExpr> + 'a> {
        match self {
            Self::Equivalence(class) => Box::new(class.iter_mut()),
            Self::Predicate(p) => Box::new(std::iter::once(p)),
        }
    }

    /// Returns a set with all the input columns referenced in the constraint.
    fn support(&self) -> HashSet<usize> {
        self.iter_expr()
            .map(|e| e.support())
            .fold(Default::default(), |mut acc, support| {
                acc.extend(support);
                acc
            })
    }

    /// Updates column references in the constraint.
    fn permute_map(&mut self, permutation: &HashMap<usize, usize>) {
        for expr in self.iter_expr_mut() {
            expr.permute_map(permutation);
        }
    }

    fn is_literal_true(&self) -> bool {
        if let Self::Predicate(p) = self {
            p.is_literal_true()
        } else {
            false
        }
    }

    fn is_literal_false(&self) -> bool {
        if let Self::Predicate(p) = self {
            p.is_literal_false()
        } else {
            false
        }
    }

    fn is_literal_null(&self) -> bool {
        if let Self::Predicate(p) = self {
            p.is_literal_null()
        } else {
            false
        }
    }

    /// Simplifies all the expressions within the Constraint.
    fn reduce(&mut self, relation_type: &repr::RelationType) {
        for expr in self.iter_expr_mut() {
            expr.reduce(relation_type);
        }
    }

    /// Maps all the expressions within the Constraint.
    fn map_to_global(mut self, input_mapper: &JoinInputMapper, index: usize) -> Self {
        for expr in self.iter_expr_mut() {
            *expr = input_mapper.map_expr_to_global(expr.to_owned(), index);
        }
        self
    }

    /// Attempts to perform substitutions via `map` in all the expressions contained
    /// in the constraint and returns `false` if the constraint cannot be lifted.
    fn substitute(&mut self, map: &HashMap<MirScalarExpr, MirScalarExpr>) -> bool {
        match self {
            Self::Equivalence(class) => {
                // Keep the class members that can find substitutions
                *class = class
                    .drain(..)
                    .flat_map(|mut expr| {
                        if substitute(&mut expr, map) {
                            Some(expr)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                class.len() > 1
            }
            Self::Predicate(p) => substitute(p, map),
        }
    }

    /// Optimizes all the expressions in the constraint by performing replacements.
    /// Returns true if any replacement took place.
    fn optimize(&mut self, predicates: &PredicateStructure) -> bool {
        self.iter_expr_mut()
            .map(|e| optimize(e, predicates))
            .fold(false, |acc, v| acc || v)
    }
}

impl std::fmt::Display for Constraint {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            Self::Equivalence(class) => {
                write!(f, "Equivalence[")?;
                for (index, expr) in class.iter().enumerate() {
                    write!(f, "{}{}", if index > 0 { " " } else { "" }, expr)?;
                }
                write!(f, "]")
            }
            Self::Predicate(p) => p.fmt(f),
        }
    }
}

/// Adds a constraint, expressed as a predicate, to the collection.
///
/// Equality predicates are converted into an equivalence and an IS NOT NULL predicate.
fn append_constraint(constraints: &mut Vec<Constraint>, predicate: MirScalarExpr) {
    if let MirScalarExpr::CallBinary {
        expr1,
        expr2,
        func: BinaryFunc::Eq,
    } = predicate
    {
        constraints.push(Constraint::Predicate(
            expr1
                .clone()
                .call_unary(UnaryFunc::IsNull(expr::func::IsNull))
                .call_unary(UnaryFunc::Not(expr::func::Not)),
        ));
        constraints.push(Constraint::Equivalence(vec![*expr1, *expr2]));
    } else {
        constraints.push(Constraint::Predicate(predicate));
    }
}

/// Denormalizes the predicates in the given list for easily computing the
/// intersection with another list of constraints.
fn denormalize_predicates(constraints: Vec<Constraint>) -> Vec<Constraint> {
    let mut result = Vec::new();
    let mut representatives = Vec::new();
    let mut other_predicates = Vec::new();
    for constraint in constraints {
        if let Constraint::Equivalence(class) = constraint {
            for (idx1, expr1) in class.iter().enumerate() {
                for (_, expr2) in class.iter().enumerate().filter(|(idx2, _)| *idx2 != idx1) {
                    result.push(Constraint::Equivalence(vec![expr1.clone(), expr2.clone()]));
                }
            }

            let mut iter = class.into_iter();
            if let Some(representative) = iter.next() {
                representatives.push((representative, iter.collect_vec()));
            }
        } else {
            other_predicates.push(constraint);
        }
    }

    // Rewrite all non-equivalence predicates in terms of every other
    // member of the equivalences.
    for (representative, others) in representatives.iter() {
        let mut rewritten_predicates = Vec::new();
        for constraint in other_predicates.iter() {
            if let Constraint::Predicate(p) = &constraint {
                for other in others {
                    let mut structured = PredicateStructure::default();
                    structured.replacements.insert(representative, other);

                    let mut cloned_p = p.clone();
                    if optimize(&mut cloned_p, &structured) {
                        rewritten_predicates.push(Constraint::Predicate(cloned_p));
                    } else {
                        break;
                    }
                }
            }
        }
        other_predicates.extend(rewritten_predicates);
    }
    result.extend(other_predicates);
    result
}

/// Put `predicates` into a normal form that minimizes redundancy and exploits equality.
///
/// The method extracts equality statements, chooses representatives for each equivalence relation,
/// and substitutes the representatives.
fn normalize_predicates(predicates: &mut Vec<Constraint>, input_type: &repr::RelationType) {
    // Remove duplicates
    predicates.sort();
    predicates.dedup();

    // Establish equivalence classes of expressions, where we choose a representative
    // that is the "simplest", ideally a literal, then a column reference, and then
    // more complicated stuff from there. We then replace all expressions in the class
    // with the simplest representative. Folks looking up expressions should also use
    // an expression with substitutions performed.

    let mut classes = Vec::new();
    let mut other_predicates = Vec::new();
    for mut predicate in predicates.drain(..) {
        match predicate {
            Constraint::Equivalence(input_class) => {
                let mut class = Vec::new();
                for expr in input_class {
                    class.extend(
                        classes
                            .iter()
                            .position(|c: &Vec<MirScalarExpr>| c.contains(&expr))
                            .map(|p| classes.remove(p))
                            .unwrap_or_else(|| vec![expr]),
                    );
                }
                if class.len() > 1 {
                    classes.push(class);
                }
            }
            Constraint::Predicate(_) => {
                predicate.reduce(input_type);
                other_predicates.push(predicate);
            }
        }
    }

    *predicates = other_predicates;

    'retry_loop: loop {
        // Order each class so that literals come first, then column references, then weird things.
        for class in classes.iter_mut() {
            // Reduce the expressions within the class
            for expr in class.iter_mut() {
                expr.reduce(input_type);
            }
            class.sort_by(cmp_expr);
            class.dedup();
            // If we have a second literal, not equal to the first, we have a contradiction and can
            // just replace everything with false.
            if let Some(second) = class.get(1) {
                if second.is_literal_ok() {
                    predicates.clear();
                    predicates.push(Constraint::Predicate(MirScalarExpr::literal_ok(
                        Datum::False,
                        ScalarType::Bool,
                    )));
                    return;
                }
            }
        }
        // Sort by complexity of representative, and perform substitutions within predicates.
        classes.sort_by(|x, y| cmp_expr(&x[0], &y[0]));

        let mut structured = PredicateStructure::default();
        for class in classes.iter_mut() {
            if class
                .iter_mut()
                .map(|expr| optimize(expr, &structured))
                .fold(false, |acc, v| acc || v)
            {
                // Optimizing a member of the current class may result in a change in the
                // order of the equivalente expression within the class and among the classes.
                continue 'retry_loop;
            }
            let mut iter = class.iter();
            if let Some(representative) = iter.next() {
                for other in iter {
                    structured.replacements.insert(other, representative);
                }
            }
        }

        // Visit each predicate and rewrite using the representative from each class.
        for predicate in predicates.iter_mut() {
            predicate.optimize(&structured);
            // Simplify the predicate after the replacements
            predicate.reduce(input_type);
            // Add the optimized predicate to the knowledge base
            structured.add_predicate(predicate);
        }

        break;
    }

    // Re-introduce equality constraints using the representative.
    for class in classes.into_iter() {
        if class.len() > 1 {
            predicates.push(Constraint::Equivalence(class));
        }
    }

    predicates.sort();
    predicates.dedup();
    predicates.retain(|p| !p.is_literal_true());

    use std::cmp::Ordering;
    // Compares two expressions. Literals come first, then column references, then weird things.
    fn cmp_expr(x: &MirScalarExpr, y: &MirScalarExpr) -> Ordering {
        match (x, y) {
            (MirScalarExpr::Literal { .. }, MirScalarExpr::Literal { .. }) => x.cmp(y),
            (MirScalarExpr::Literal { .. }, _) => Ordering::Less,
            (_, MirScalarExpr::Literal { .. }) => Ordering::Greater,
            (MirScalarExpr::Column(_), MirScalarExpr::Column(_)) => x.cmp(y),
            (MirScalarExpr::Column(_), _) => Ordering::Less,
            (_, MirScalarExpr::Column(_)) => Ordering::Greater,
            // This last class could be problematic if a complex expression sorts lower than
            // expressions it contains (e.g. in x + y = x, if x + y comes before x then x would
            // repeatedly be replaced by x + y).
            _ => (nodes(x), x).cmp(&(nodes(y), y)),
        }
    }
}

/// Attempts to perform substitutions via `map` and returns `false` if an unreplaced column reference is reached.
fn substitute(expression: &mut MirScalarExpr, map: &HashMap<MirScalarExpr, MirScalarExpr>) -> bool {
    if let Some(expr) = map.get(expression) {
        *expression = expr.clone();
        true
    } else {
        match expression {
            MirScalarExpr::Column(_) => false,
            MirScalarExpr::Literal(_, _) => true,
            MirScalarExpr::CallNullary(_) => true,
            MirScalarExpr::CallUnary { expr, .. } => substitute(expr, map),
            MirScalarExpr::CallBinary { expr1, expr2, .. } => {
                substitute(expr1, map) && substitute(expr2, map)
            }
            MirScalarExpr::CallVariadic { exprs, .. } => {
                exprs.iter_mut().all(|expr| substitute(expr, map))
            }
            MirScalarExpr::If { cond, then, els } => {
                substitute(cond, map) && substitute(then, map) && substitute(els, map)
            }
        }
    }
}

/// Replaces subexpressions of `expr` that have a value in `map` with the value, not including
/// the `If { cond, .. }` field.
///
/// Returns whether any replacement was performed.
fn optimize(expr: &mut MirScalarExpr, predicates: &PredicateStructure) -> bool {
    let mut any_replacement = false;
    expr.visit_mut_pre_post(
        &mut |e| {
            // The `cond` of an if statement is not visited to prevent `then`
            // or `els` from being evaluated before `cond`, resulting in a
            // correctness error.
            if let MirScalarExpr::If { then, els, .. } = e {
                Some(vec![then, els])
            } else {
                None
            }
        },
        &mut |e| {
            if let Some(replacement) = predicates.replacements.get(e) {
                *e = (*replacement).clone();
                any_replacement = true;
            } else if predicates.known_true.contains(e) {
                *e = MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool);
                any_replacement = true;
            } else if predicates.known_false.contains(e) {
                *e = MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool);
                any_replacement = true;
            }
        },
    );

    any_replacement
}

/// The number of nodes in the expression.
fn nodes(expr: &MirScalarExpr) -> usize {
    let mut count = 0;
    expr.visit_post(&mut |_e| count += 1);
    count
}

#[derive(Default)]
struct PredicateStructure<'predicates> {
    /// Each instance of the former should be replaced with an instance of the latter.
    pub replacements: HashMap<&'predicates MirScalarExpr, &'predicates MirScalarExpr>,
    /// These expressions are known to equal `Datum::True` for all records that will eventually be produced.
    pub known_true: HashSet<&'predicates MirScalarExpr>,
    /// These expressions are known to be false, because `not(expr)` is known to be true.
    pub known_false: HashSet<&'predicates MirScalarExpr>,
}

impl<'a> PredicateStructure<'a> {
    /// Creates a structured form of predicates that can be randomly accessed.
    ///
    /// The predicates are assumed normalized in the sense that all instances of `x = y` imply that
    /// `y` can and should be replaced by `x`.
    pub fn new(predicates: &'a [Constraint]) -> Self {
        let mut structured = Self::default();
        for predicate in predicates.iter() {
            structured.add_predicate(predicate);
        }
        structured
    }

    /// Adds the knowledge provided by the given predicate to the structure.
    ///
    /// The predicate is assumed to be normalized in the sense that it's been optimized
    /// with the knowledge of the predicates already in this structure.
    pub fn add_predicate(&mut self, predicate: &'a Constraint) {
        match predicate {
            Constraint::Equivalence(input_class) => {
                let mut it = input_class.iter();
                if let Some(first) = it.next() {
                    while let Some(other) = it.next() {
                        self.replacements.insert(&other, &first);
                    }
                }
            }
            Constraint::Predicate(predicate) => match predicate {
                MirScalarExpr::CallUnary {
                    expr,
                    func: UnaryFunc::Not(expr::func::Not),
                } => {
                    self.known_false.insert(expr);
                }
                _ => {
                    self.known_true.insert(predicate);
                }
            },
        }
    }
}
