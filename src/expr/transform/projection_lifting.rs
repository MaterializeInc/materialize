// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

// Prefering higher-order methods to equivalent lower complexity methods is wrong.
#![allow(clippy::or_fun_call)]

use crate::RelationExpr;
use std::collections::HashMap;

/// Hoist projections wherever possible, in order to minimize structural limitations on transformations.
/// Projections can be re-introduced in the physical planning stage.
#[derive(Debug)]
pub struct ProjectionLifting;

impl crate::transform::Transform for ProjectionLifting {
    fn transform(&self, relation: &mut RelationExpr) {
        self.transform(relation)
    }
}

impl ProjectionLifting {
    pub fn transform(&self, relation: &mut RelationExpr) {
        self.action(relation, &mut HashMap::new());
    }
    /// Columns that must be non-null.
    pub fn action(
        &self,
        relation: &mut RelationExpr,
        // Map from names to new get type and projection required at use.
        gets: &mut HashMap<String, (repr::RelationType, Vec<usize>)>,
    ) {
        match relation {
            RelationExpr::Constant { .. } => {}
            RelationExpr::Get { name, .. } => {
                if let Some((typ, columns)) = gets.get(name) {
                    *relation = RelationExpr::Get {
                        name: name.to_string(),
                        typ: typ.clone(),
                    }
                    .project(columns.clone());
                }
            }
            RelationExpr::Let { name, value, body } => {
                self.action(value, gets);
                if let RelationExpr::Project { input, outputs } = &mut **value {
                    let typ = input.typ();
                    let prior = gets.insert(name.to_string(), (typ, outputs.clone()));
                    assert!(!prior.is_some());
                    **value = input.take_dangerous();
                }

                self.action(body, gets);
                gets.remove(name);
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
                    // Rewrite scalar expressions using inner columns.
                    for scalar in scalars.iter_mut() {
                        scalar.permute(outputs);
                    }
                    // Retain projected columns and scalar columns.
                    let mut new_outputs = outputs.clone();
                    new_outputs.extend(outputs.len()..(outputs.len() + scalars.len()));
                    *relation = inner
                        .take_dangerous()
                        .map(scalars.clone())
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
            RelationExpr::Join { inputs, variables } => {
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
                        temp_arity += input.arity();
                        *join_input = input.take_dangerous();
                    } else {
                        let arity = join_input.arity();
                        projection.extend(temp_arity..(temp_arity + arity));
                        temp_arity += arity;
                    }
                }

                *relation = relation.take_dangerous().project(projection);
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
                        *key = outputs[*key];
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
                self.action(input, gets);
                if let RelationExpr::Project {
                    input: inner,
                    outputs,
                } = &mut **input
                {
                    *relation = inner.take_dangerous().threshold().project(outputs.clone());
                }
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
                    if output1 == output2 && input1.arity() == input2.arity() {
                        let outputs = output1.clone();
                        **left = input1.take_dangerous();
                        **right = input2.take_dangerous();
                        *relation = relation.take_dangerous().project(outputs);
                    }
                }
            }
        }
    }
}
