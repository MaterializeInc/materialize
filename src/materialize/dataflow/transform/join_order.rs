// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::dataflow::RelationExpr;
use crate::repr::RelationType;

/// Re-order relations in a join to process them in an order that makes sense.
///
/// ```rust
/// use materialize::dataflow::RelationExpr;
/// use materialize::repr::{ColumnType, RelationType, ScalarType};
/// use materialize::dataflow::transform::join_order::JoinOrder;
///
/// let input1 = RelationExpr::constant(vec![], RelationType::new(vec![
///     ColumnType::new(ScalarType::Bool),
/// ]));
/// let input2 = RelationExpr::constant(vec![], RelationType::new(vec![
///     ColumnType::new(ScalarType::Bool),
/// ]));
/// let input3 = RelationExpr::constant(vec![], RelationType::new(vec![
///     ColumnType::new(ScalarType::Bool),
/// ]));
/// let mut expr = RelationExpr::join(
///     vec![input1, input2, input3],
///     vec![vec![(0, 0), (2, 0)]],
/// );
/// let typ = RelationType::new(vec![
///     ColumnType::new(ScalarType::Bool),
///     ColumnType::new(ScalarType::Bool),
///     ColumnType::new(ScalarType::Bool),
/// ]);
///
/// JoinOrder.transform(&mut expr, &typ);
///
/// if let RelationExpr::Project { input, outputs } = expr {
///     assert_eq!(outputs, vec![0, 2, 1]);
/// }
/// ```
pub struct JoinOrder;

impl super::Transform for JoinOrder {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl JoinOrder {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut(&mut |e| {
            self.action(e, &e.typ());
        });
    }
    pub fn action(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        if let RelationExpr::Join { inputs, variables } = relation {
            let arities = inputs.iter().map(|i| i.arity()).collect::<Vec<_>>();

            // Step 1: determine a relation_expr order starting from `inputs[0]`.
            let mut relation_expr_order = vec![0];
            while relation_expr_order.len() < inputs.len() {
                let mut candidates = (0..inputs.len())
                    .filter(|i| !relation_expr_order.contains(i))
                    .map(|i| {
                        (
                            variables
                                .iter()
                                .filter(|vars| {
                                    vars.iter().any(|(idx, _)| &i == idx)
                                        && vars
                                            .iter()
                                            .any(|(idx, _)| relation_expr_order.contains(idx))
                                })
                                .count(),
                            i,
                        )
                    })
                    .collect::<Vec<_>>();

                candidates.sort();
                relation_expr_order.push(candidates.pop().expect("Candidate expected").1);
            }

            // Step 2: rewrite `variables`.
            let mut positions = vec![0; relation_expr_order.len()];
            for (index, input) in relation_expr_order.iter().enumerate() {
                positions[*input] = index;
            }

            let mut new_variables = Vec::new();
            for variable in variables.iter() {
                let mut new_set = Vec::new();
                for (rel, col) in variable.iter() {
                    new_set.push((positions[*rel], *col));
                }
                new_variables.push(new_set);
            }

            // Step 3: prepare `Project`.
            // We want to present as if in the order we promised, so we need to permute.
            // In particular, for each (rel, col) in order, we want to figure out where
            // it lives in our weird local order, and build an expr that picks it out.
            let mut offset = 0;
            let mut offsets = vec![0; relation_expr_order.len()];
            for input in relation_expr_order.iter() {
                offsets[*input] = offset;
                offset += arities[*input];
            }

            let mut projection = Vec::new();
            for rel in 0..inputs.len() {
                for col in 0..arities[rel] {
                    let position = offsets[rel] + col;
                    projection.push(position);
                }
            }

            // Step 4: prepare output
            let mut new_inputs = Vec::new();
            for rel in relation_expr_order.into_iter() {
                new_inputs.push(inputs[rel].clone()); // TODO: Extract from `inputs`.
            }

            let join = RelationExpr::Join {
                inputs: new_inputs,
                variables: new_variables,
            };

            // Output projection
            *relation = join.project(projection);
            // (output, metadata)
        }
        // else {
        //     (relation, metadata)
        // }
    }
}
