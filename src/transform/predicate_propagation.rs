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
use expr::{BinaryFunc, RelationExpr, ScalarExpr, UnaryFunc};
use repr::Datum;
use repr::{ColumnType, RelationType, ScalarType};

/// Harvest and act upon per-column information.
#[derive(Debug)]
pub struct PredicateKnowledge;

impl crate::Transform for PredicateKnowledge {
    fn transform(
        &self,
        expr: &mut RelationExpr,
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
        expr: &mut RelationExpr,
        let_knowledge: &mut HashMap<expr::Id, Vec<ScalarExpr>>,
    ) -> Result<Vec<ScalarExpr>, crate::TransformError> {
        let mut predicates = match expr {
            RelationExpr::ArrangeBy { input, .. } => {
                PredicateKnowledge::action(input, let_knowledge)?
            }
            RelationExpr::Get { id, typ } => {
                // If we fail to find bound knowledge, use the nullability of the type.
                let_knowledge.get(id).cloned().unwrap_or_else(|| {
                    typ.column_types
                        .iter()
                        .enumerate()
                        .filter(|(_index, ct)| !ct.nullable)
                        .map(|(index, _ct)| {
                            ScalarExpr::column(index)
                                .call_unary(UnaryFunc::IsNull)
                                .call_unary(UnaryFunc::Not)
                        })
                        .collect()
                })
            }
            RelationExpr::Constant { rows: _, typ: _ } => {
                // TODO(frank): sort out what things we could do here that we can't do in literal hoisting.
                Vec::new()
            }
            RelationExpr::Let { id, value, body } => {
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
            RelationExpr::Project { input, outputs } => {
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
                        if let ScalarExpr::Column(c) = e {
                            *c = remap[c];
                        }
                    })
                }

                // 3. add equivalence constraints for repeated columns.
                for (index, column) in outputs.iter().enumerate() {
                    if remap[column] != index {
                        input_knowledge.push(
                            ScalarExpr::Column(remap[column])
                                .call_binary(ScalarExpr::Column(*column), expr::BinaryFunc::Eq),
                        );
                    }
                }
                input_knowledge
            }
            RelationExpr::Map { input, scalars } => {
                let input_knowledge = PredicateKnowledge::action(input, let_knowledge)?;
                // Scalars could be simplified based on known predicates.
                for scalar in scalars.iter_mut() {
                    optimize(scalar, &input.typ(), &input_knowledge[..])?;
                }
                input_knowledge
            }
            RelationExpr::FlatMap {
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
            RelationExpr::Filter { input, predicates } => {
                let mut input_knowledge = PredicateKnowledge::action(input, let_knowledge)?;
                // Add predicates to the knowledge as we go.
                for predicate in predicates.iter_mut() {
                    optimize(predicate, &input.typ(), &input_knowledge[..])?;
                    input_knowledge.push(predicate.clone());
                }
                input_knowledge
            }
            RelationExpr::Join {
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
                            if let ScalarExpr::Column(c) = expr {
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

                knowledge
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
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
            RelationExpr::TopK { input, .. } => PredicateKnowledge::action(input, let_knowledge)?,
            RelationExpr::Negate { input } => PredicateKnowledge::action(input, let_knowledge)?,
            RelationExpr::Threshold { input } => PredicateKnowledge::action(input, let_knowledge)?,
            RelationExpr::Union { left, right } => {
                let mut know1 = PredicateKnowledge::action(left, let_knowledge)?;
                let know2 = PredicateKnowledge::action(right, let_knowledge)?;

                know1.retain(|predicate| know2.contains(predicate));
                know1
            }
        };

        predicates.sort();
        predicates.dedup();
        if predicates
            .iter()
            .any(|p| p.is_literal_false() || p.is_literal_null())
        {
            expr.take_safely();
        }
        Ok(predicates)
    }
}

/// Attempts to optimize
pub fn optimize(
    expr: &mut ScalarExpr,
    input_type: &RelationType,
    predicates: &[ScalarExpr],
) -> Result<(), crate::TransformError> {
    // Naively traverse the expression looking for instances of known-true statements.
    // Note: this does not find known false statements that are consequences of negation,
    // such as `x != y` implying that `x = y` must be false.

    expr.visit_mut(&mut |expr| {
        if predicates.contains(expr) {
            assert!(expr.typ(input_type).scalar_type == ScalarType::Bool);
            *expr = ScalarExpr::literal_ok(Datum::True, ColumnType::new(ScalarType::Bool));
            expr.reduce(input_type);
        } else if predicates.contains(&expr.clone().call_unary(UnaryFunc::Not)) {
            assert!(expr.typ(input_type).scalar_type == ScalarType::Bool);
            *expr = ScalarExpr::literal_ok(Datum::False, ColumnType::new(ScalarType::Bool));
            expr.reduce(input_type);
        } else {
            for predicate in predicates {
                if let ScalarExpr::CallBinary {
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
