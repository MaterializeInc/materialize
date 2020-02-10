// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::{EvalEnv, GlobalId, Id, RelationExpr, ScalarExpr};

/// Hoist projections wherever possible, in order to minimize structural limitations on transformations.
/// Projections can be re-introduced in the physical planning stage.
#[derive(Debug)]
pub struct ProjectionLifting;

impl crate::transform::Transform for ProjectionLifting {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation)
    }
}

impl ProjectionLifting {
    pub fn transform(&self, relation: &mut RelationExpr) {
        self.action(relation, &mut HashMap::new());
    }
    // Lift projections out from under `relation`.
    pub fn action(
        &self,
        relation: &mut RelationExpr,
        // Map from names to new get type and projection required at use.
        gets: &mut HashMap<Id, (repr::RelationType, Vec<usize>)>,
    ) {
        match relation {
            RelationExpr::Constant { .. } => {}
            RelationExpr::Get { id, .. } => {
                if let Some((typ, columns)) = gets.get(id) {
                    *relation = RelationExpr::Get {
                        id: *id,
                        typ: typ.clone(),
                    }
                    .project(columns.clone());
                }
            }
            RelationExpr::Let { id, value, body } => {
                self.action(value, gets);
                let id = Id::Local(*id);
                if let RelationExpr::Project { input, outputs } = &mut **value {
                    let typ = input.typ();
                    let prior = gets.insert(id, (typ, outputs.clone()));
                    assert!(!prior.is_some());
                    **value = input.take_dangerous();
                }

                self.action(body, gets);
                gets.remove(&id);
            }
            RelationExpr::Project { input, outputs } => {
                self.action(input, gets);
                if let RelationExpr::Project {
                    input: inner,
                    outputs: inner_outputs,
                } = &mut **input
                {
                    for output in outputs.iter_mut() {
                        *output = inner_outputs[*output];
                    }
                    **input = inner.take_dangerous();
                }
            }
            RelationExpr::Map { input, scalars } => {
                self.action(input, gets);
                if let RelationExpr::Project {
                    input: inner,
                    outputs,
                } = &mut **input
                {
                    // Retain projected columns and scalar columns.
                    let mut new_outputs = outputs.clone();
                    let inner_arity = inner.arity();
                    new_outputs.extend(inner_arity..(inner_arity + scalars.len()));

                    // Rewrite scalar expressions using inner columns.
                    for scalar in scalars.iter_mut() {
                        scalar.permute(&new_outputs);
                    }

                    *relation = inner
                        .take_dangerous()
                        .map(scalars.clone())
                        .project(new_outputs);
                }
            }
            RelationExpr::FlatMapUnary {
                input,
                func,
                expr,
                demand,
            } => {
                self.action(input, gets);
                if let RelationExpr::Project {
                    input: inner,
                    outputs,
                } = &mut **input
                {
                    // TODO: Preserve demand.
                    *demand = None;
                    // Retain projected columns and scalar columns.
                    let mut new_outputs = outputs.clone();
                    let inner_arity = inner.arity();
                    new_outputs.extend(inner_arity..(inner_arity + func.output_arity()));

                    // Rewrite scalar expression using inner columns.
                    expr.permute(&new_outputs);

                    *relation = inner
                        .take_dangerous()
                        .flat_map_unary(func.clone(), expr.clone())
                        .project(new_outputs);
                }
            }
            RelationExpr::Filter { input, predicates } => {
                self.action(input, gets);
                if let RelationExpr::Project {
                    input: inner,
                    outputs,
                } = &mut **input
                {
                    // Rewrite scalar expressions using inner columns.
                    for predicate in predicates.iter_mut() {
                        predicate.permute(outputs);
                    }
                    *relation = inner
                        .take_dangerous()
                        .filter(predicates.clone())
                        .project(outputs.clone());
                }
            }
            RelationExpr::Join {
                inputs,
                variables,
                demand,
                implementation,
            } => {
                for input in inputs.iter_mut() {
                    self.action(input, gets);
                }

                let mut projection = Vec::new();
                let mut temp_arity = 0;

                for (index, join_input) in inputs.iter_mut().enumerate() {
                    if let RelationExpr::Project { input, outputs } = join_input {
                        for variable in variables.iter_mut() {
                            for (rel, col) in variable.iter_mut() {
                                if *rel == index {
                                    *col = outputs[*col];
                                }
                            }
                        }
                        for output in outputs.iter() {
                            projection.push(temp_arity + *output);
                        }

                        if let Some(demand) = demand {
                            for col in demand[index].iter_mut() {
                                *col = outputs[*col];
                            }
                        }

                        temp_arity += input.arity();
                        *join_input = input.take_dangerous();
                    } else {
                        let arity = join_input.arity();
                        projection.extend(temp_arity..(temp_arity + arity));
                        temp_arity += arity;
                    }
                }

                *implementation = crate::relation::JoinImplementation::Unimplemented;

                if projection.len() != temp_arity || (0..temp_arity).any(|i| projection[i] != i) {
                    *relation = relation.take_dangerous().project(projection);
                }
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                // Reduce *absorbs* projections, which is amazing!
                self.action(input, gets);
                if let RelationExpr::Project {
                    input: inner,
                    outputs,
                } = &mut **input
                {
                    for key in group_key.iter_mut() {
                        key.permute(outputs);
                    }
                    for aggregate in aggregates.iter_mut() {
                        aggregate.expr.permute(outputs);
                    }
                    **input = inner.take_dangerous();
                }
            }
            RelationExpr::TopK {
                input,
                group_key,
                order_key,
                limit,
                offset,
            } => {
                self.action(input, gets);
                if let RelationExpr::Project {
                    input: inner,
                    outputs,
                } = &mut **input
                {
                    for key in group_key.iter_mut() {
                        *key = outputs[*key];
                    }
                    for key in order_key.iter_mut() {
                        key.column = outputs[key.column];
                    }
                    *relation = inner
                        .take_dangerous()
                        .top_k(
                            group_key.clone(),
                            order_key.clone(),
                            limit.clone(),
                            offset.clone(),
                        )
                        .project(outputs.clone());
                }
            }
            RelationExpr::Negate { input } => {
                self.action(input, gets);
                if let RelationExpr::Project {
                    input: inner,
                    outputs,
                } = &mut **input
                {
                    *relation = inner.take_dangerous().negate().project(outputs.clone());
                }
            }
            RelationExpr::Threshold { input } => {
                // We cannot, in general, lift projections out of threshold.
                // If we could reason that the input cannot be negative, we
                // would be able to lift the projection, but otherwise our
                // action on weights need to accumulate the restricted rows.
                self.action(input, gets);
            }
            RelationExpr::Union { left, right } => {
                // We cannot, in general, lift projections out of unions.
                self.action(left, gets);
                self.action(right, gets);

                if let (
                    RelationExpr::Project {
                        input: input1,
                        outputs: output1,
                    },
                    RelationExpr::Project {
                        input: input2,
                        outputs: output2,
                    },
                ) = (&mut **left, &mut **right)
                {
                    if output1 == output2 && input1.typ() == input2.typ() {
                        let outputs = output1.clone();
                        **left = input1.take_dangerous();
                        **right = input2.take_dangerous();
                        *relation = relation.take_dangerous().project(outputs);
                    }
                }
            }
            RelationExpr::ArrangeBy { input, keys } => {
                self.action(input, gets);
                if let RelationExpr::Project {
                    input: inner,
                    outputs,
                } = &mut **input
                {
                    for key_set in keys.iter_mut() {
                        for key in key_set.iter_mut() {
                            key.permute(outputs);
                        }
                    }
                    *relation = inner
                        .take_dangerous()
                        .arrange_by(keys)
                        .project(outputs.clone());
                }
            }
        }
    }
}
