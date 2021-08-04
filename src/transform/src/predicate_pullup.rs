// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Lift predicates through the graph
//!
//! The main purpose of this transform is to get predicates out of the
//! way before `RedundantJoin` transform that may prevent the removal
//! of redundant join operands. `PredicatePushdown` is required
//! as a cleanup step afterwards.
//!
//! It only lifts predicates from `Filter` operators, leaving join
//! equivalences untouched, since lifting them would prevent
//! `RedundantJoin` from doing its job.

use std::collections::{HashMap, HashSet};

use crate::TransformArgs;
use expr::{Id, MirRelationExpr, MirScalarExpr};

/// Lift predicates through the graph
#[derive(Debug)]
pub struct PredicatePullup;

impl crate::Transform for PredicatePullup {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let mut empty = HashMap::new();
        self.action(relation, &mut empty);
        Ok(())
    }
}

impl PredicatePullup {
    fn action(
        &self,
        relation: &mut MirRelationExpr,
        get_predicates: &mut HashMap<Id, HashSet<MirScalarExpr>>,
    ) {
        match relation {
            MirRelationExpr::Get { id, .. } => {
                if let Some(predicates) = get_predicates.get(id) {
                    if !predicates.is_empty() {
                        *relation = relation.take_dangerous().filter(predicates.clone());
                    }
                }
            }

            MirRelationExpr::Let { id, value, body } => {
                self.action(value, get_predicates);

                if let MirRelationExpr::Filter {
                    input: inner_input,
                    predicates: inner_predicates,
                } = &mut **value
                {
                    get_predicates.insert(
                        Id::Local(*id),
                        inner_predicates.drain(..).collect::<HashSet<_>>(),
                    );
                    *value = Box::new(inner_input.take_dangerous());
                }

                self.action(body, get_predicates);
            }

            MirRelationExpr::Filter { input, .. } => {
                self.action(input, get_predicates);

                // Two nested Filters must be fused so that the predicates
                // from both are lifted.
                crate::fusion::filter::Filter.action(relation);
            }

            MirRelationExpr::Map { input, scalars } => {
                self.action(input, get_predicates);

                if scalars.iter().all(|x| !x.is_literal_err()) {
                    if let MirRelationExpr::Filter {
                        input: inner_input,
                        predicates,
                    } = &mut **input
                    {
                        if predicates.iter().all(|x| !x.is_literal_err()) {
                            *relation = inner_input
                                .take_dangerous()
                                .map(scalars.to_owned())
                                .filter(predicates.to_owned());
                        }
                    }
                }
            }

            MirRelationExpr::Project { input, outputs } => {
                self.action(input, get_predicates);

                if let MirRelationExpr::Filter {
                    input: inner_input,
                    predicates,
                } = &mut **input
                {
                    if predicates.iter().all(|x| !x.is_literal_err()) {
                        // lift all predicates which required columns are projected
                        let projection = outputs
                            .iter()
                            .enumerate()
                            .map(|(i, c)| (*c, i))
                            .collect::<HashMap<usize, usize>>();

                        let (mut liftable, non_liftable): (Vec<MirScalarExpr>, Vec<MirScalarExpr>) =
                            predicates.drain(..).partition(|e| {
                                e.support().iter().all(|c| projection.contains_key(c))
                            });

                        if non_liftable.is_empty() {
                            **input = inner_input.take_dangerous();
                        } else {
                            **input = inner_input.take_dangerous().filter(non_liftable);
                        }

                        if !liftable.is_empty() {
                            // apply the projection
                            for pred in liftable.iter_mut() {
                                pred.permute_map(&projection);
                            }

                            *relation = relation.take_dangerous().filter(liftable);
                        }
                    }
                }
            }

            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates: _,
                monotonic: _,
                expected_group_size: _,
            } => {
                self.action(input, get_predicates);

                if let MirRelationExpr::Filter {
                    input: inner_input,
                    predicates,
                } = &mut **input
                {
                    if predicates.iter().all(|x| !x.is_literal_err()) {
                        // lift all predicates which required columns are projected
                        let projection = group_key
                            .iter()
                            .enumerate()
                            .filter_map(|(i, e)| {
                                if let MirScalarExpr::Column(c) = e {
                                    Some((*c, i))
                                } else {
                                    None
                                }
                            })
                            .collect::<HashMap<usize, usize>>();
                        let (mut liftable, non_liftable): (Vec<MirScalarExpr>, Vec<MirScalarExpr>) =
                            predicates.drain(..).partition(|e| {
                                e.support().iter().all(|c| projection.contains_key(c))
                            });

                        if non_liftable.is_empty() {
                            **input = inner_input.take_dangerous();
                        } else {
                            **input = inner_input.take_dangerous().filter(non_liftable);
                        }

                        if !liftable.is_empty() {
                            // apply the projection
                            for pred in liftable.iter_mut() {
                                pred.permute_map(&projection);
                            }

                            *relation = relation.take_dangerous().filter(liftable);
                        }
                    }
                }
            }

            MirRelationExpr::Join {
                inputs,
                equivalences: _,
                demand,
                implementation,
            } => {
                for input in inputs.iter_mut() {
                    self.action(input, get_predicates);
                }

                let input_mapper = expr::JoinInputMapper::new(inputs);
                let mut lifted_predicates = Vec::new();
                for (input_index, input) in inputs.iter_mut().enumerate() {
                    if let MirRelationExpr::Filter {
                        input: inner_input,
                        predicates,
                    } = input
                    {
                        if predicates.iter().all(|x| !x.is_literal_err()) {
                            for mut expr in predicates.drain(..) {
                                expr.visit_mut(&mut |e| {
                                    if let MirScalarExpr::Column(c) = e {
                                        *c = input_mapper.map_column_to_global(*c, input_index);
                                    }
                                });
                                lifted_predicates.push(expr);
                            }

                            *input = inner_input.take_dangerous();
                        }
                    }
                }

                *demand = None;
                *implementation = expr::JoinImplementation::Unimplemented;

                if !lifted_predicates.is_empty() {
                    *relation = relation.take_dangerous().filter(lifted_predicates);
                }
            }

            MirRelationExpr::Union { base, inputs } => {
                self.action(base, get_predicates);
                for input in inputs.iter_mut() {
                    self.action(input, get_predicates);
                }

                // We can lift the predicates that are common to all branches.
                if let MirRelationExpr::Filter {
                    input: base_input,
                    predicates: base_predicates,
                } = &mut **base
                {
                    if base_predicates.iter().all(|x| !x.is_literal_err()) {
                        let mut lifted_predicates = base_predicates.clone();

                        for input in inputs.iter() {
                            if lifted_predicates.is_empty() {
                                break;
                            }
                            if let MirRelationExpr::Filter {
                                input: _,
                                predicates,
                            } = input
                            {
                                lifted_predicates.retain(|p| predicates.contains(p));
                            } else {
                                lifted_predicates.clear();
                            }
                        }

                        if !lifted_predicates.is_empty() {
                            let remaining_base_predicates = base_predicates
                                .iter()
                                .filter(|p| !lifted_predicates.contains(p))
                                .cloned()
                                .collect::<Vec<_>>();
                            if remaining_base_predicates.is_empty() {
                                **base = base_input.take_dangerous();
                            } else {
                                **base = base_input
                                    .take_dangerous()
                                    .filter(remaining_base_predicates);
                            }
                            for input in inputs.iter_mut() {
                                if let MirRelationExpr::Filter {
                                    input: inner_input,
                                    predicates,
                                } = input
                                {
                                    let remaining_predicates = predicates
                                        .iter()
                                        .filter(|p| !lifted_predicates.contains(p))
                                        .cloned()
                                        .collect::<Vec<_>>();
                                    if remaining_predicates.is_empty() {
                                        *input = inner_input.take_dangerous();
                                    } else {
                                        *input = inner_input
                                            .take_dangerous()
                                            .filter(remaining_predicates);
                                    }
                                }
                            }
                            *relation = relation.take_dangerous().filter(lifted_predicates);
                        }
                    }
                }
            }

            MirRelationExpr::Negate { input } => {
                self.action(input, get_predicates);

                if let MirRelationExpr::Filter {
                    input: inner_input,
                    predicates,
                } = &mut **input
                {
                    if predicates.iter().all(|x| !x.is_literal_err()) {
                        *relation = inner_input
                            .take_dangerous()
                            .negate()
                            .filter(predicates.clone());
                    }
                }
            }

            // TODO We currently don't lift predicates from the input of FlatMaps since
            // predicate pushdown won't push them back down. Enable when #7617 is fixed.
            // MirRelationExpr::FlatMap {
            //     input,
            //     func: _,
            //     exprs: _,
            //     demand: _,
            // } => {
            //     self.action(input, get_predicates);

            //     if let MirRelationExpr::Filter {
            //         input: inner_input,
            //         predicates,
            //     } = &mut **input
            //     {
            //         if predicates.iter().all(|x| !x.is_literal_err()) {
            //             let liftable_predictes = predicates.clone();
            //             **input = inner_input.take_dangerous();
            //             *relation = relation.take_dangerous().filter(liftable_predictes);
            //         }
            //     }
            // }
            x => {
                // Lifting predicates on top of the remaining non-covered operators
                // is not correct, so we just process their content here
                x.visit1_mut(|e| self.action(e, get_predicates));
            }
        }
    }
}
