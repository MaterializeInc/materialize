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
//! applied to the argument expression, in the sense that the would neither
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
use expr::{BinaryFunc, MirRelationExpr, MirScalarExpr, UnaryFunc};
use repr::Datum;
use repr::ScalarType;

/// Harvest and act upon per-column information.
#[derive(Debug)]
pub struct PredicateKnowledge;

impl crate::Transform for PredicateKnowledge {
    fn transform(
        &self,
        expr: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let _predicates = PredicateKnowledge::action(expr, &mut HashMap::new())?;
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
    fn action(
        expr: &mut MirRelationExpr,
        let_knowledge: &mut HashMap<expr::Id, Vec<MirScalarExpr>>,
    ) -> Result<Vec<MirScalarExpr>, crate::TransformError> {
        let self_type = expr.typ();
        let mut predicates = match expr {
            MirRelationExpr::ArrangeBy { input, .. } => {
                PredicateKnowledge::action(input, let_knowledge)?
            }
            MirRelationExpr::DeclareKeys { input, .. } => {
                PredicateKnowledge::action(input, let_knowledge)?
            }
            MirRelationExpr::Get { id, typ } => {
                // If we fail to find bound knowledge, use the nullability of the type.
                let_knowledge.get(id).cloned().unwrap_or_else(|| {
                    typ.column_types
                        .iter()
                        .enumerate()
                        .filter(|(_index, ct)| !ct.nullable)
                        .map(|(index, _ct)| {
                            MirScalarExpr::column(index)
                                .call_unary(UnaryFunc::IsNull(expr::func::IsNull))
                                .call_unary(UnaryFunc::Not(expr::func::Not))
                        })
                        .collect()
                })
            }
            MirRelationExpr::Constant { rows, typ } => {
                // Each column could 1. be equal to a literal, 2. known to be non-null.
                // They could be many other things too, but these are the ones we can handle.
                if let Ok([(row, _cnt), rows @ ..]) = rows.as_deref_mut() {
                    // An option for each column that contains a common value.
                    let mut literal = row.iter().map(|x| Some(x)).collect::<Vec<_>>();
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
                    let mut predicates = Vec::new();
                    for column in 0..literal.len() {
                        if let Some(datum) = literal[column] {
                            // Having found a literal, if it is Null use `IsNull` and otherwise use `Eq`.
                            if datum == Datum::Null {
                                predicates.push(
                                    MirScalarExpr::Column(column)
                                        .call_unary(expr::UnaryFunc::IsNull(expr::func::IsNull)),
                                );
                            } else {
                                predicates.push(MirScalarExpr::Column(column).call_binary(
                                    MirScalarExpr::literal(
                                        Ok(datum),
                                        typ.column_types[column].scalar_type.clone(),
                                    ),
                                    expr::BinaryFunc::Eq,
                                ));
                            }
                        }
                        if non_null[column] {
                            predicates.push(
                                MirScalarExpr::column(column)
                                    .call_unary(UnaryFunc::IsNull(expr::func::IsNull))
                                    .call_unary(UnaryFunc::Not(expr::func::Not)),
                            );
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
                let value_knowledge = PredicateKnowledge::action(value, let_knowledge)?;
                let prior_knowledge =
                    let_knowledge.insert(expr::Id::Local(id.clone()), value_knowledge);
                let body_knowledge = PredicateKnowledge::action(body, let_knowledge)?;
                let_knowledge.remove(&expr::Id::Local(id.clone()));
                if let Some(prior_knowledge) = prior_knowledge {
                    let_knowledge.insert(expr::Id::Local(id.clone()), prior_knowledge);
                }
                body_knowledge
            }
            MirRelationExpr::Project { input, outputs } => {
                let mut input_knowledge = PredicateKnowledge::action(input, let_knowledge)?;
                // Need to
                // 1. restrict to supported columns,
                // 2. rewrite constraints using new column identifiers,
                // 3. add equivalence constraints for repeated columns.
                let mut remap = HashMap::new();
                for (index, column) in outputs.iter().enumerate() {
                    if !remap.contains_key(column) {
                        remap.insert(column, index);
                    }
                }

                // 1. restrict to supported columns,
                input_knowledge
                    .retain(|expr| expr.support().into_iter().all(|c| remap.contains_key(&c)));

                // 2. rewrite constraints using new column identifiers,
                for predicate in input_knowledge.iter_mut() {
                    predicate.visit_mut(&mut |e| {
                        if let MirScalarExpr::Column(c) = e {
                            *c = remap[c];
                        }
                    })
                }

                // 3. add equivalence constraints for repeated columns.
                for (index, column) in outputs.iter().enumerate() {
                    if remap[column] != index {
                        input_knowledge.push(
                            MirScalarExpr::Column(remap[column])
                                .call_binary(MirScalarExpr::Column(*column), expr::BinaryFunc::Eq),
                        );
                    }
                }
                input_knowledge
            }
            MirRelationExpr::Map { input, scalars } => {
                let input_knowledge = PredicateKnowledge::action(input, let_knowledge)?;
                let structured = PredicateStructure::new(&input_knowledge);
                // Scalars could be simplified based on known predicates.
                for scalar in scalars.iter_mut() {
                    optimize(scalar, &structured);
                }
                // TODO: present literal columns (and non-null?) as predicates.
                input_knowledge
            }
            MirRelationExpr::FlatMap {
                input,
                func: _,
                exprs,
            } => {
                let input_knowledge = PredicateKnowledge::action(input, let_knowledge)?;
                let structured = PredicateStructure::new(&input_knowledge);
                for expr in exprs {
                    optimize(expr, &structured);
                }
                input_knowledge
            }
            MirRelationExpr::Filter { input, predicates } => {
                let input_knowledge = PredicateKnowledge::action(input, let_knowledge)?;
                let structured = PredicateStructure::new(&input_knowledge);
                let mut output_knowledge = input_knowledge.clone();

                for predicate in predicates.iter_mut() {
                    optimize(predicate, &structured);
                    output_knowledge.push(predicate.clone());
                }
                output_knowledge
            }
            MirRelationExpr::Join {
                inputs,
                equivalences,
                ..
            } => {
                let mut knowledge = Vec::new();

                // Collect input knowledge, but update column references.
                let mut prior_arity = 0;
                for input in inputs.iter_mut() {
                    for mut predicate in PredicateKnowledge::action(input, let_knowledge)? {
                        predicate.visit_mut(&mut |expr| {
                            if let MirScalarExpr::Column(c) = expr {
                                *c += prior_arity;
                            }
                        });
                        knowledge.push(predicate);
                    }
                    prior_arity += input.arity();
                }

                let structured = PredicateStructure::new(&knowledge);
                let mut new_predicates = Vec::new();

                for equivalence in equivalences.iter_mut() {
                    for expr in equivalence.iter_mut() {
                        optimize(expr, &structured);
                    }

                    if let Some(first) = equivalence.get(0) {
                        for other in equivalence[1..].iter() {
                            new_predicates
                                .push(first.clone().call_binary(other.clone(), BinaryFunc::Eq));
                        }
                    }
                }

                knowledge.extend(new_predicates);
                normalize_predicates(&mut knowledge);
                knowledge
            }
            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                ..
            } => {
                let input_type = input.typ();
                let input_knowledge = PredicateKnowledge::action(input, let_knowledge)?;
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
                    key_exprs.insert(expr.clone(), MirScalarExpr::Column(index));
                }
                // 1. Visit each predicate, and collect those that reach no `ScalarExpr::Column` when substituting per `key_exprs`.
                //    They will be preserved and presented as output.
                for mut predicate in input_knowledge.iter().cloned() {
                    if substitute(&mut predicate, &key_exprs) {
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
                            predicates.push(
                                MirScalarExpr::Column(column)
                                    .call_unary(expr::UnaryFunc::IsNull(expr::func::IsNull)),
                            );
                        } else {
                            predicates.push(MirScalarExpr::Column(column).call_binary(
                                MirScalarExpr::literal(
                                    Ok(literal),
                                    self_type.column_types[column].scalar_type.clone(),
                                ),
                                expr::BinaryFunc::Eq,
                            ));
                            predicates.push(
                                MirScalarExpr::column(column)
                                    .call_unary(UnaryFunc::IsNull(expr::func::IsNull))
                                    .call_unary(UnaryFunc::Not(expr::func::Not)),
                            );
                        }
                    } else {
                        // Aggregates that are non-literals may still be non-null.
                        if let expr::AggregateFunc::Count = aggregate.func {
                            // Replace counts of nonnull, nondistinct expressions with `true`.
                            if let MirScalarExpr::Column(c) = aggregate.expr {
                                if !input_type.column_types[c].nullable && !aggregate.distinct {
                                    aggregate.expr =
                                        MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool);
                                }
                            }
                            predicates.push(
                                MirScalarExpr::column(column)
                                    .call_unary(UnaryFunc::IsNull(expr::func::IsNull))
                                    .call_unary(UnaryFunc::Not(expr::func::Not)),
                            );
                        } else {
                            // TODO: Something more sophisticated using `input_knowledge` too.
                            if !self_type.column_types[column].nullable {
                                predicates.push(
                                    MirScalarExpr::column(column)
                                        .call_unary(UnaryFunc::IsNull(expr::func::IsNull))
                                        .call_unary(UnaryFunc::Not(expr::func::Not)),
                                );
                            }
                        }
                    }
                }

                predicates
            }
            MirRelationExpr::TopK { input, .. } => {
                PredicateKnowledge::action(input, let_knowledge)?
            }
            MirRelationExpr::Negate { input } => PredicateKnowledge::action(input, let_knowledge)?,
            MirRelationExpr::Threshold { input } => {
                PredicateKnowledge::action(input, let_knowledge)?
            }
            MirRelationExpr::Union { base, inputs } => {
                let mut know1 = PredicateKnowledge::action(base, let_knowledge)?;
                for input in inputs.iter_mut() {
                    let know2 = PredicateKnowledge::action(input, let_knowledge)?;
                    know1.retain(|predicate| know2.contains(predicate));
                }
                know1
            }
        };

        normalize_predicates(&mut predicates);
        if predicates
            .iter()
            .any(|p| p.is_literal_false() || p.is_literal_null())
        {
            expr.take_safely();
        }
        Ok(predicates)
    }
}

/// Put `predicates` into a normal form that minimizes redundancy and exploits equality.
///
/// The method extracts equality statements, chooses representatives for each equivalence relation,
/// and substitutes the representatives.
fn normalize_predicates(predicates: &mut Vec<MirScalarExpr>) {
    // Remove duplicates
    predicates.sort();
    predicates.dedup();

    // Establish equivalence classes of expressions, where we choose a representative
    // that is the "simplest", ideally a literal, then a column reference, and then
    // more complicated stuff from there. We then replace all expressions in the class
    // with the simplest representative. Folks looking up expressions should also use
    // an expression with substitutions performed.

    let mut classes = Vec::new();
    for predicate in predicates.iter() {
        if let MirScalarExpr::CallBinary {
            expr1,
            expr2,
            func: BinaryFunc::Eq,
        } = predicate
        {
            let mut class = Vec::new();
            class.extend(
                classes
                    .iter()
                    .position(|c: &Vec<MirScalarExpr>| c.contains(&**expr1))
                    .map(|p| classes.remove(p))
                    .unwrap_or_else(|| vec![(**expr1).clone()]),
            );
            class.extend(
                classes
                    .iter()
                    .position(|c: &Vec<MirScalarExpr>| c.contains(&**expr2))
                    .map(|p| classes.remove(p))
                    .unwrap_or_else(|| vec![(**expr2).clone()]),
            );
            classes.push(class);
        }
    }

    // Order each class so that literals come first, then column references, then weird things.
    for class in classes.iter_mut() {
        use std::cmp::Ordering;
        class.sort_by(|x, y| match (x, y) {
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
        });
        class.dedup();
        // If we have a second literal, not equal to the first, we have a contradiction and can
        // just replaces everything with false.
        if let Some(second) = class.get(1) {
            if second.is_literal_ok() {
                predicates.push(MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool));
            }
        }
    }
    // TODO: Sort by complexity of representative, and perform substitutions within predicates.
    classes.sort();

    // let mut normalization = HashMap::<&MirScalarExpr, &MirScalarExpr>::new();
    let mut structured = PredicateStructure::default();
    for class in classes.iter() {
        let mut iter = class.iter();
        if let Some(representative) = iter.next() {
            for other in iter {
                structured.replacements.insert(other, representative);
            }
        }
    }

    // Visit each predicate and rewrite using the representative from each class.
    // Feel welcome to remove all equality tests and re-introduce canonical tests.
    for predicate in predicates.iter_mut() {
        optimize(predicate, &structured);
    }
    // Re-introduce equality constraints using the representative.
    for class in classes.iter() {
        for expr in class[1..].iter() {
            predicates.push(class[0].clone().call_binary(expr.clone(), BinaryFunc::Eq));
        }
    }

    predicates.sort();
    predicates.dedup();
    predicates.retain(|p| !p.is_literal_true());
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
fn optimize(expr: &mut MirScalarExpr, predicates: &PredicateStructure) {
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
            } else if predicates.known_true.contains(e) {
                *e = MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool);
            } else if predicates.known_false.contains(e) {
                *e = MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool);
            }
        },
    );
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
    pub fn new(predicates: &'a [MirScalarExpr]) -> Self {
        let mut structured = Self::default();
        for predicate in predicates.iter() {
            match predicate {
                MirScalarExpr::CallBinary {
                    expr1,
                    expr2,
                    func: BinaryFunc::Eq,
                } => {
                    structured.replacements.insert(&**expr2, &**expr1);
                }
                MirScalarExpr::CallUnary {
                    expr,
                    func: UnaryFunc::Not(expr::func::Not),
                } => {
                    structured.known_false.insert(expr);
                }
                _ => {
                    structured.known_true.insert(predicate);
                }
            }
        }
        structured
    }
}
