// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Hoist projections through operators.
//!
//! Projections can be re-introduced in the physical planning stage.

use std::collections::HashMap;
use std::mem;

use crate::TransformArgs;
use expr::{Id, MirRelationExpr};

/// Hoist projections through operators.
#[derive(Debug)]
pub struct ProjectionLifting;

impl crate::Transform for ProjectionLifting {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.action(relation, &mut HashMap::new());
        Ok(())
    }
}

impl ProjectionLifting {
    /// Hoist projections through operators.
    pub fn action(
        &self,
        relation: &mut MirRelationExpr,
        // Map from names to new get type and projection required at use.
        gets: &mut HashMap<Id, (repr::RelationType, Vec<usize>)>,
    ) {
        match relation {
            MirRelationExpr::Constant { .. } => {}
            MirRelationExpr::Get { id, .. } => {
                if let Some((typ, columns)) = gets.get(id) {
                    *relation = MirRelationExpr::Get {
                        id: *id,
                        typ: typ.clone(),
                    }
                    .project(columns.clone());
                }
            }
            MirRelationExpr::Let { id, value, body } => {
                self.action(value, gets);
                let id = Id::Local(*id);
                if let MirRelationExpr::Project { input, outputs } = &mut **value {
                    let typ = input.typ();
                    let prior = gets.insert(id, (typ, outputs.clone()));
                    assert!(!prior.is_some());
                    **value = input.take_dangerous();
                }

                self.action(body, gets);
                gets.remove(&id);
            }
            MirRelationExpr::Project { input, outputs } => {
                self.action(input, gets);
                if let MirRelationExpr::Project {
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
            MirRelationExpr::Map { input, scalars } => {
                self.action(input, gets);
                if let MirRelationExpr::Project {
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
            MirRelationExpr::FlatMap {
                input,
                func,
                exprs,
                demand,
            } => {
                self.action(input, gets);
                if let MirRelationExpr::Project {
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
                    for expr in exprs.iter_mut() {
                        expr.permute(&new_outputs);
                    }

                    *relation = inner
                        .take_dangerous()
                        .flat_map(func.clone(), exprs.clone())
                        .project(new_outputs);
                }
            }
            MirRelationExpr::Filter { input, predicates } => {
                self.action(input, gets);
                if let MirRelationExpr::Project {
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
            MirRelationExpr::Join {
                inputs,
                equivalences,
                demand,
                implementation,
            } => {
                for input in inputs.iter_mut() {
                    self.action(input, gets);
                }

                // Track the location of the projected columns in the un-projected join.
                let mut projection = Vec::new();
                let mut temp_arity = 0;

                for join_input in inputs.iter_mut() {
                    if let MirRelationExpr::Project { input, outputs } = join_input {
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

                if projection.len() != temp_arity || (0..temp_arity).any(|i| projection[i] != i) {
                    // Update equivalences, demand, and implementation.
                    for equivalence in equivalences.iter_mut() {
                        for expr in equivalence {
                            expr.permute(&projection[..]);
                        }
                    }
                    if let Some(demand) = demand {
                        for column in demand.iter_mut() {
                            *column = projection[*column];
                        }
                    }

                    *implementation = expr::JoinImplementation::Unimplemented;

                    *relation = relation.take_dangerous().project(projection);
                }
            }
            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                monotonic: _,
                expected_group_size: _,
            } => {
                // Reduce *absorbs* projections, which is amazing!
                self.action(input, gets);
                if let MirRelationExpr::Project {
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
            MirRelationExpr::TopK {
                input,
                group_key,
                order_key,
                limit,
                offset,
                monotonic: _,
            } => {
                self.action(input, gets);
                if let MirRelationExpr::Project {
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
            MirRelationExpr::Negate { input } => {
                self.action(input, gets);
                if let MirRelationExpr::Project {
                    input: inner,
                    outputs,
                } = &mut **input
                {
                    *relation = inner.take_dangerous().negate().project(outputs.clone());
                }
            }
            MirRelationExpr::Threshold { input } => {
                // We cannot, in general, lift projections out of threshold.
                // If we could reason that the input cannot be negative, we
                // would be able to lift the projection, but otherwise our
                // action on weights need to accumulate the restricted rows.
                self.action(input, gets);
            }
            MirRelationExpr::DeclareKeys { input, .. } => {
                self.action(input, gets);
            }
            MirRelationExpr::Union { base, inputs } => {
                // We cannot, in general, lift projections out of unions.
                self.action(base, gets);
                for input in &mut *inputs {
                    self.action(input, gets);
                }

                if let MirRelationExpr::Project {
                    input: base_input,
                    outputs: base_outputs,
                } = &mut **base
                {
                    let base_typ = base_input.typ();

                    let mut can_lift = true;
                    for input in &mut *inputs {
                        match input {
                            MirRelationExpr::Project { input, outputs }
                                if input.typ() == base_typ && outputs == base_outputs => {}
                            _ => {
                                can_lift = false;
                                break;
                            }
                        }
                    }

                    if can_lift {
                        let base_outputs = mem::take(base_outputs);
                        **base = base_input.take_dangerous();
                        for inp in inputs {
                            match inp {
                                MirRelationExpr::Project { input, .. } => {
                                    *inp = input.take_dangerous();
                                }
                                _ => unreachable!(),
                            }
                        }
                        *relation = relation.take_dangerous().project(base_outputs);
                    }
                }
            }
            MirRelationExpr::ArrangeBy { input, keys } => {
                self.action(input, gets);
                if let MirRelationExpr::Project {
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
