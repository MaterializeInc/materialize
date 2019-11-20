// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::RelationExpr;

/// Re-order relations in a join to minimize the size of intermediate results.
/// To minimize intermediate results, ensure each additional relation
/// added to the join shares an equality constraint with a column of a relation
/// that is already present in the intermediate results.
///
/// For example:
/// Join relations A, B, C given the equality constraints A.x = C.x and B.y = C.y.
///
/// In the given ordering, we join A and B first with no equality constraints. This
/// means we have to do a full cross join of A and B, creating large intermediate results.
///
/// Instead, if we reordered to A, C, B, we could use the shared equality constraint
/// between A and C to perform an equijoin, creating smaller intermediate results.
/// ```rust
/// use expr::RelationExpr;
/// use expr::transform::join_order::JoinOrder;
/// use repr::{ColumnType, RelationType, ScalarType};
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
///
/// JoinOrder.transform(&mut expr);
///
/// if let RelationExpr::Project { input, outputs } = expr {
///     assert_eq!(outputs, vec![0, 1, 2]);
/// }
/// ```
#[derive(Debug)]
pub struct JoinOrder;

impl super::Transform for JoinOrder {
    fn transform(&self, relation: &mut RelationExpr) {
        self.transform(relation)
    }
}

impl JoinOrder {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut(&mut |e| {
            self.action(e);
        });
    }
    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Join {
            inputs,
            variables,
            demand,
        } = relation
        {
            let types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
            let uniques = types.iter().map(|t| t.keys.clone()).collect::<Vec<_>>();
            let arities = types
                .iter()
                .map(|t| t.column_types.len())
                .collect::<Vec<_>>();

            // Step 1: determine a relation_expr order starting from `inputs[0]`.
            let relation_expr_order = order_join(inputs.len(), &variables[..], &uniques[..]);

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
            for rel in relation_expr_order.iter() {
                new_inputs.push(inputs[*rel].clone()); // TODO: Extract from `inputs`.
            }

            // put join constraints in a canonical format.
            for variable in new_variables.iter_mut() {
                variable.sort();
            }
            new_variables.sort();

            let join = if let Some(demand) = demand {
                let mut new_demand = Vec::new();
                for rel in relation_expr_order.into_iter() {
                    new_demand.push(demand[rel].clone());
                }
                RelationExpr::Join {
                    inputs: new_inputs,
                    variables: new_variables,
                    demand: Some(new_demand),
                }
            } else {
                RelationExpr::join(new_inputs, new_variables)
            };

            // Output projection
            *relation = join.project(projection);
        }
    }
}

fn order_join(
    relations: usize,
    constraints: &[Vec<(usize, usize)>],
    unique_keys: &[Vec<Vec<usize>>],
) -> Vec<usize> {
    // First attempt to order so as to exploit uniqueness constraints.
    // This attempts to restrict the intermediate state, a proxy for cost-based optimization.
    for i in 0..relations {
        if let Some(order) = order_on_keys(relations, i, constraints, unique_keys) {
            return order;
        }
    }

    // Attempt to order relations so that each is at least constrained by columns in prior relations.
    let mut relation_expr_order = vec![0];
    while relation_expr_order.len() < relations {
        let mut candidates = (0..relations)
            .filter(|i| !relation_expr_order.contains(i))
            .map(|i| {
                (
                    constraints
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
    relation_expr_order
}

/// Attempt to order relations to join on unique keys.
///
/// This method attempts to produce an order on relations so that each join will involve at least a
/// unique key, which would ensure that the number of records does not increase along the join. The
/// attempt starts from a specified `start` relation, and greedily adds relations as long as any have
/// unique keys that must be equal to some bound column.
fn order_on_keys(
    relations: usize,
    start: usize,
    constraints: &[Vec<(usize, usize)>],
    unique_keys: &[Vec<Vec<usize>>],
) -> Option<Vec<usize>> {
    let mut order = vec![start];
    while order.len() < relations {
        // Attempt to find a next relation, not yet in `order` and whose unique keys are all bound
        // by columns of relations that are present in `order`.
        let candidate = (0..relations).filter(|i| !order.contains(i)).find(|i| {
            unique_keys[*i].iter().any(|keys| {
                keys.iter().all(|key| {
                    constraints.iter().any(|variables| {
                        let contains_key = variables.contains(&(*i, *key));
                        let contains_bound = variables.iter().any(|(idx, _)| order.contains(idx));
                        contains_key && contains_bound
                    })
                })
            })
        });

        order.push(candidate?);
    }
    Some(order)
}
