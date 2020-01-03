// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

// Using references ensures code doesn't break if the other argument
// ceases to be Copy.
#![allow(clippy::op_ref)]
use crate::{EvalEnv, RelationExpr};

#[derive(Debug)]
pub struct ReductionPushdown;

impl super::Transform for ReductionPushdown {
    fn transform(&self, relation: &mut RelationExpr, env: &EvalEnv) {
        self.transform(relation, env)
    }
}

impl ReductionPushdown {
    pub fn transform(&self, relation: &mut RelationExpr, env: &EvalEnv) {
        relation.visit_mut(&mut |e| {
            self.action(e, env);
        });
    }

    pub fn action(&self, relation: &mut RelationExpr, _env: &EvalEnv) {
        if let RelationExpr::Reduce {
            input,
            group_key,
            aggregates,
        } = relation
        {
            // Map expressions can be absorbed into the Reduce at no cost.
            if let RelationExpr::Map {
                input: inner,
                scalars,
            } = &mut **input
            {
                let arity = inner.arity();

                // Normalize the scalars to not be self-referential.
                let mut scalars = scalars.clone();
                for index in 0..scalars.len() {
                    let (lower, upper) = scalars.split_at_mut(index);
                    upper[0].visit_mut(&mut |e| {
                        if let crate::ScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = lower[*c - arity].clone();
                            }
                        }
                    });
                }
                for key in group_key.iter_mut() {
                    key.visit_mut(&mut |e| {
                        if let crate::ScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = scalars[*c - arity].clone();
                            }
                        }
                    });
                }
                for agg in aggregates.iter_mut() {
                    agg.expr.visit_mut(&mut |e| {
                        if let crate::ScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = scalars[*c - arity].clone();
                            }
                        }
                    });
                }

                **input = inner.take_dangerous()
            }
            // else if let RelationExpr::Join { inputs, variables, .. } = &**input {

            //     let types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
            //     let uniques = types.iter().map(|t| t.keys.clone()).collect::<Vec<_>>();
            //     let input_arities = types
            //         .iter()
            //         .map(|t| t.column_types.len())
            //         .collect::<Vec<_>>();

            //     let mut offset = 0;
            //     let mut prior_arities = Vec::new();
            //     for input in 0..inputs.len() {
            //         prior_arities.push(offset);
            //         offset += input_arities[input];
            //     }

            //     let input_relation = input_arities
            //         .iter()
            //         .enumerate()
            //         .flat_map(|(r, a)| std::iter::repeat(r).take(*a))
            //         .collect::<Vec<_>>();

            //     let mut relations = std::collections::HashSet::new();
            //     for aggr in aggregates.iter() {
            //         relations.extend(aggr.expr.support().iter().map(|c| input_relation[*c]));
            //     }

            //     if relations.len() <= 1 {
            //         let start = relations.into_iter().next().unwrap_or(0);
            //         let order = super::join_order::order_on_keys(inputs.len(), start, &variables, &uniques);
            //         if order.is_some() {
            //             // Each constrained key from the start relation needs to appear in the grouping key.
            //             if variables.iter().all(|vs|
            //                 vs.iter().any(|(r,c)| group_key.contains(&(c + prior_arities[*r]))) ||
            //                 vs.iter().all(|(r,c)| (r != &start))) {
            //                 println!("ReducePushdown opportunity (for relation {}): {}", start, relation.pretty());
            //             }
            //         }
            //     }
            // }
        }
    }
}
