// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::RelationExpr;
use repr::RelationType;

/// Removes singleton constants from joins, and removes joins with
/// single input relations.
#[derive(Debug)]
pub struct JoinElision;

impl super::Transform for JoinElision {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl JoinElision {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut(&mut |e| {
            self.action(e, &e.typ());
        });
    }
    // Tuples have lengths, which may be zero; they are not "empty".
    #[allow(clippy::len_zero)]
    pub fn action(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        if let RelationExpr::Join { inputs, variables } = relation {
            // We re-accumulate `inputs` into `new_inputs` in order to perform
            // some clean-up logic as we go. Mainly, we need to update the key
            // equivalence classes which contain relation indices which should
            // be decremented appropriately.
            let mut new_inputs = Vec::new();
            for (position, expression) in inputs.drain(..).enumerate() {
                let is_vacuous = if let RelationExpr::Constant { rows, .. } = &expression {
                    rows.len() == 1 && rows[0].0.len() == 0 && rows[0].1 == 1
                } else {
                    false
                };

                if is_vacuous {
                    for group in variables.iter_mut() {
                        for (index, _) in group {
                            if *index >= position {
                                *index -= 1;
                            }
                        }
                    }
                } else {
                    new_inputs.push(expression);
                }
            }

            // If `new_inputs` is empty or a singleton (without constraints) we can remove the join.
            *inputs = new_inputs;
            match inputs.len() {
                0 => {
                    *relation = RelationExpr::constant(vec![vec![]], metadata.clone());
                }
                1 => {
                    // if there are constraints, they probably should have
                    // been pushed down by predicate pushdown, but .. let's
                    // not re-write that code here.
                    if variables.is_empty() {
                        *relation = inputs.pop().unwrap();
                    }
                }
                _ => {}
            }
        }
    }
}
