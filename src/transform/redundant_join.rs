// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Remove redundant collections of distinct elements from joins.

// If statements seem a bit clearer in this case. Specialized methods
// that replace simple and common alternatives frustrate developers.
#![allow(clippy::comparison_chain, clippy::filter_next)]

use crate::{RelationExpr, ScalarExpr, TransformState};

/// Remove redundant collections of distinct elements from joins.
#[derive(Debug)]
pub struct RedundantJoin;

impl crate::Transform for RedundantJoin {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &mut TransformState,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut(&mut |e| {
            self.action(e);
        });
        Ok(())
    }
}

impl RedundantJoin {
    /// Remove redundant collections of distinct elements from joins.
    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Join {
            inputs,
            equivalences,
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
                                    equivalences.iter().any(|e| {
                                        e.contains(&ScalarExpr::Column(prior_arities[index] + *k))
                                            && e.contains(&ScalarExpr::Column(
                                                prior_arities[*prior] + *k,
                                            ))
                                    })
                                })
                            })
                    })
                    .next()
                {
                    projection.extend(
                        prior_arities[prior]..(prior_arities[prior] + input_arities[prior]),
                    );
                    to_remove.push(index);
                // TODO: check for relation repetition in any variable.
                } else {
                    projection.extend(columns..(columns + input_arities[index]));
                    columns += input_arities[index];
                }
            }

            // Update constraints to reference `prior`. Shift subsequent references.
            if !to_remove.is_empty() {
                // remove in reverse order.
                while let Some(index) = to_remove.pop() {
                    inputs.remove(index);
                }
                for equivalence in equivalences.iter_mut() {
                    for expr in equivalence.iter_mut() {
                        expr.permute(&projection[..]);
                    }
                    equivalence.sort();
                    equivalence.dedup();
                }
                equivalences.retain(|v| v.len() > 1);
                *demand = None;
            }

            // Implement a projection if the projection removed any columns.
            let orig_arity = input_arities.iter().sum::<usize>();
            if projection.len() != orig_arity || projection.iter().enumerate().any(|(i, p)| i != *p)
            {
                *implementation = expr::JoinImplementation::Unimplemented;
                *relation = relation.take_dangerous().project(projection);
            }
        }
    }
}
