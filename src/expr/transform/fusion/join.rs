// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::RelationExpr;
use repr::RelationType;

#[derive(Debug)]
pub struct Join;

impl crate::transform::Transform for Join {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl Join {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut(&mut |e| {
            self.action(e, &e.typ());
        });
    }

    pub fn action(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        if let RelationExpr::Join { inputs, variables } = relation {
            let mut new_inputs = Vec::new();
            let mut new_variables = Vec::new();

            let mut new_relation = Vec::new();

            for input in inputs.drain(..) {
                let mut columns = Vec::new();
                if let RelationExpr::Join {
                    mut inputs,
                    mut variables,
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
        }
    }
}
