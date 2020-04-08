// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::{GlobalId, RelationExpr, ScalarExpr};

#[derive(Debug)]
pub struct Join;

impl crate::transform::Transform for Join {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
    ) -> Result<(), crate::transform::TransformError> {
        self.transform(relation);
        Ok(())
    }
}

impl Join {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut(&mut |e| {
            self.action(e);
        });
    }

    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Join {
            inputs,
            equivalences,
            demand,
            implementation,
        } = relation
        {
            let mut new_inputs = Vec::new();
            let mut new_equivalences = Vec::new();
            let mut new_columns = 0;

            // We scan through each input, digesting any joins that we find and updating their equivalence classes.
            // We retain any existing equivalence classes, as they are already with respect to the cross product.
            for input in inputs.drain(..) {
                if let RelationExpr::Join {
                    mut inputs,
                    mut equivalences,
                    ..
                } = input
                {
                    // Update and push all of the variables.
                    for mut equivalence in equivalences.drain(..) {
                        for expr in equivalence.iter_mut() {
                            expr.visit_mut(&mut |e| {
                                if let ScalarExpr::Column(c) = e {
                                    *c += new_columns;
                                }
                            });
                        }
                        new_equivalences.push(equivalence);
                    }
                    // Add all of the inputs.
                    for input in inputs.drain(..) {
                        new_columns += input.arity();
                        new_inputs.push(input);
                    }
                } else {
                    // Retain the input.
                    new_columns += input.arity();
                    new_inputs.push(input);
                }
            }

            new_equivalences.extend(equivalences.drain(..));

            *inputs = new_inputs;
            *equivalences = new_equivalences;
            *demand = None;
            *implementation = crate::JoinImplementation::Unimplemented;

            // Join variables may not be an equivalence class. Better ensure!
            for index in 1..equivalences.len() {
                for inner in 0..index {
                    if equivalences[index]
                        .iter()
                        .any(|pair| equivalences[inner].contains(pair))
                    {
                        let to_extend = std::mem::replace(&mut equivalences[index], Vec::new());
                        equivalences[inner].extend(to_extend);
                    }
                }
            }
            equivalences.retain(|v| !v.is_empty());

            // put join constraints in a canonical format.
            for equivalence in equivalences.iter_mut() {
                equivalence.sort();
                equivalence.dedup();
            }
        }
    }
}
