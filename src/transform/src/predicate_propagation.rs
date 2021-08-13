// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Lift predicate information towards the root.
//!
//! This transform does not move the predicates, which remain in force
//! in their current location, but it is able to simplify expressions
//! as it moves from inputs toward the query root.

use std::collections::HashMap;

use crate::TransformArgs;
use expr::{BinaryFunc, MirRelationExpr, MirScalarExpr, UnaryFunc};
use repr::Datum;
use repr::{RelationType, ScalarType};

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
                                .call_unary(UnaryFunc::IsNull)
                                .call_unary(UnaryFunc::Not)
                        })
                        .collect()
                })
            }
            MirRelationExpr::Constant { rows: _, typ: _ } => {
                // TODO(frank): sort out what things we could do here that we can't do in literal hoisting.
                Vec::new()
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
                // Scalars could be simplified based on known predicates.
                for scalar in scalars.iter_mut() {
                    optimize(scalar, &input.typ(), &input_knowledge[..])?;
                }
                input_knowledge
            }
            MirRelationExpr::FlatMap {
                input,
                func: _,
                exprs,
                demand: _,
            } => {
                let input_knowledge = PredicateKnowledge::action(input, let_knowledge)?;
                for expr in exprs {
                    optimize(expr, &input.typ(), &input_knowledge[..])?;
                }
                input_knowledge
            }
            MirRelationExpr::Filter { input, predicates } => {
                let mut input_knowledge = PredicateKnowledge::action(input, let_knowledge)?;

                // Add predicates to the knowledge as we go.
                for predicate in predicates.iter_mut() {
                    optimize(predicate, &input.typ(), &input_knowledge[..])?;
                    input_knowledge.push(predicate.clone());
                }
                input_knowledge
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

                for equivalence in equivalences.iter_mut() {
                    if let Some(first) = equivalence.get(0) {
                        for other in equivalence[1..].iter() {
                            // TODO: Canonicalize information to refer to first column / something.
                            knowledge
                                .push(first.clone().call_binary(other.clone(), BinaryFunc::Eq));
                        }
                    }
                }

                normalize_predicates(&mut knowledge);
                knowledge
            }
            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                ..
            } => {
                let input_knowledge = PredicateKnowledge::action(input, let_knowledge)?;
                for key in group_key.iter_mut() {
                    optimize(key, &input.typ(), &input_knowledge[..])?;
                }
                for aggregate in aggregates {
                    optimize(&mut aggregate.expr, &input.typ(), &input_knowledge[..])?;
                }
                // TODO: Predicates about columns that are certain aggregates can be preserved.
                // For example, if a column Not(IsNull) or (= 7) then Min/Max of that column
                // will have the same property.
                Vec::new()
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
            _ => x.cmp(y),
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

    // Visit each predicate and rewrite using the representative from each class.
    // Feel welcome to remove all equality tests and re-introduce canonical tests.
    for predicate in predicates.iter_mut() {
        if let MirScalarExpr::CallBinary {
            expr1: _,
            expr2: _,
            func: BinaryFunc::Eq,
        } = predicate
        {
            *predicate = MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool);
        } else {
            predicate.visit_mut(&mut |e| {
                if let Some(class) = classes.iter().find(|c| c.contains(e)) {
                    *e = class[0].clone();
                }
            });
        }
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

/// Attempts to optimize a supplied expression.
///
/// Naively traverse the expression looking for instances of statements that are known
/// to be true, or known to be false. This is restricted to exact matches in the predicate,
/// and is not clever enough to reason about various forms of inequality.
pub fn optimize(
    expr: &mut MirScalarExpr,
    input_type: &RelationType,
    predicates: &[MirScalarExpr],
) -> Result<(), crate::TransformError> {
    // To simplify things, we'll build a map from complex expressions to the simpler ones
    // that should replace them.
    let mut normalizing_map = HashMap::new();
    for predicate in predicates.iter() {
        if let MirScalarExpr::CallBinary {
            expr1,
            expr2,
            func: BinaryFunc::Eq,
        } = predicate
        {
            normalizing_map.insert(expr2.clone(), expr1.clone());
        }
    }
    // Replace all expressions with a normalized representative.
    // Ideally we would do a pre-order traversal here, but that
    // method doesn't seem to exist.
    expr.visit_mut(&mut |e| {
        if let Some(expr) = normalizing_map.get(e) {
            *e = (**expr).clone();
        }
    });

    expr.visit_mut(&mut |expr| {
        if predicates.contains(expr) {
            assert!(expr.typ(input_type).scalar_type == ScalarType::Bool);
            *expr = MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool);
            expr.reduce(input_type);
        } else if predicates.contains(&expr.clone().call_unary(UnaryFunc::Not)) {
            assert!(expr.typ(input_type).scalar_type == ScalarType::Bool);
            *expr = MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool);
            expr.reduce(input_type);
        } else {
            for predicate in predicates {
                if let MirScalarExpr::CallBinary {
                    expr1,
                    expr2,
                    func: BinaryFunc::Eq,
                } = predicate
                {
                    if &**expr1 == expr && expr2.is_literal() {
                        *expr = (**expr2).clone();
                    } else if &**expr2 == expr && expr1.is_literal() {
                        *expr = (**expr1).clone();
                    }
                }
            }
        }
    });

    Ok(())
}
