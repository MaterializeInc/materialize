// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// If statements seem a bit clearer in this case. Specialized methods
// that replace simple and common alternatives frustrate developers.
#![allow(clippy::comparison_chain, clippy::filter_next)]
use std::collections::HashMap;

use crate::scalar::EvalEnv;
use crate::{GlobalId, RelationExpr, ScalarExpr};

#[derive(Debug)]
pub struct RedundantJoin;

impl super::Transform for RedundantJoin {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        env: &EvalEnv,
    ) {
        self.transform(relation, env)
    }
}

impl RedundantJoin {
    pub fn transform(&self, relation: &mut RelationExpr, _env: &EvalEnv) {
        relation.visit_mut(&mut |e| {
            self.action(e);
        });
    }

    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Join {
            inputs,
            variables,
            demand,
            implementation,
        } = relation
        {
            let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
            let input_arities = input_types
                .iter()
                .map(|i| i.column_types.len())
                .collect::<Vec<_>>();

            let mut offset = 0;
            let mut prior_arities = Vec::new();
            for input in 0..inputs.len() {
                prior_arities.push(offset);
                offset += input_arities[input];
            }

            // It is possible that two inputs are the same, and joined on columns that form a key for them.
            // If so, we can remove one of them, and replace references to it with corresponding references
            // to the other.
            let mut columns = 0;
            let mut projection = Vec::new();
            let mut to_remove = Vec::new();
            for (index, input) in inputs.iter().enumerate() {
                let keys = input.typ().keys;
                if let Some(prior) = (0..index)
                    .filter(|prior| {
                        &inputs[*prior] == input
                            && keys.iter().any(|key| {
                                key.iter().all(|k| {
                                    variables.iter().any(|v| {
                                        v.contains(&(index, *k)) && v.contains(&(*prior, *k))
                                    })
                                })
                            })
                    })
                    .next()
                {
                    projection.extend(
                        prior_arities[prior]..(prior_arities[prior] + input_arities[prior]),
                    );
                    to_remove.push((index, prior));
                // TODO: check for relation repetition in any variable.
                } else {
                    projection.extend(columns..(columns + input_arities[index]));
                    columns += input_arities[index];
                }
            }

            // Update constraints to reference `prior`. Shift subsequent references.
            while let Some((index, prior)) = to_remove.pop() {
                inputs.remove(index);
                for variable in variables.iter_mut() {
                    for (rel, _col) in variable.iter_mut() {
                        if *rel == index {
                            *rel = prior;
                        } else if *rel > index {
                            *rel -= 1;
                        }
                    }
                    variable.sort();
                    variable.dedup();
                }
                variables.retain(|v| v.len() > 1);
                *demand = None;
            }
            if projection.iter().enumerate().any(|(i, p)| i != *p) {
                *implementation = crate::relation::JoinImplementation::Unimplemented;
                *relation = relation.take_dangerous().project(projection);
            }
        }
    }
}
