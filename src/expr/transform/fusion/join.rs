// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::{EvalEnv, GlobalId, RelationExpr, ScalarExpr};

#[derive(Debug)]
pub struct Join;

impl crate::transform::Transform for Join {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation)
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
            inputs, variables, ..
        } = relation
        {
            let mut new_inputs = Vec::new();
            let mut new_variables = Vec::new();
            let mut new_relation = Vec::new();

            for input in inputs.drain(..) {
                let mut columns = Vec::new();
                if let RelationExpr::Join {
                    mut inputs,
                    mut variables,
                    ..
                } = input
                {
                    // Update and push all of the variables.
                    for mut variable in variables.drain(..) {
                        for (rel, _col) in variable.iter_mut() {
                            *rel += new_inputs.len();
                        }
                        new_variables.push(variable);
                    }
                    // Add all of the inputs.
                    for input in inputs.drain(..) {
                        let new_inputs_len = new_inputs.len();
                        let arity = input.arity();
                        columns.extend((0..arity).map(|c| (new_inputs_len, c)));
                        new_inputs.push(input);
                    }
                } else {
                    // Retain the input.
                    let new_inputs_len = new_inputs.len();
                    let arity = input.arity();
                    columns.extend((0..arity).map(|c| (new_inputs_len, c)));
                    new_inputs.push(input);
                }
                new_relation.push(columns);
            }

            for mut variable in variables.drain(..) {
                for (rel, col) in variable.iter_mut() {
                    let (rel2, col2) = new_relation[*rel][*col];
                    *rel = rel2;
                    *col = col2;
                }
                new_variables.push(variable);
            }

            *inputs = new_inputs;
            *variables = new_variables;

            // Join variables may not be an equivalance class. Better ensure!
            for index in 1..variables.len() {
                for inner in 0..index {
                    if variables[index]
                        .iter()
                        .any(|pair| variables[inner].contains(pair))
                    {
                        let to_extend = std::mem::replace(&mut variables[index], Vec::new());
                        variables[inner].extend(to_extend);
                    }
                }
            }
            variables.retain(|v| !v.is_empty());

            // put join constraints in a canonical format.
            for variable in variables.iter_mut() {
                variable.sort();
                variable.dedup();
            }
            //disable variable sort if any indexes are being used
            if !inputs.iter().any(|input| match input {
                RelationExpr::ArrangeBy { .. } => true,
                _ => false,
            }) {
                variables.sort();
            }
        }
    }
}
