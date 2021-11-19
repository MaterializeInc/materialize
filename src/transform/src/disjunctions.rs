// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Converts a disjunction into a union if that avoids a cross-join.

use std::collections::HashSet;

use crate::TransformArgs;
use expr::{BinaryFunc, JoinInputMapper, MirRelationExpr, MirScalarExpr, UnaryFunc};
use itertools::Itertools;
use repr::RelationType;

/// Converts a disjunction into a union if that avoids a cross-join.
#[derive(Debug)]
pub struct DisjunctionToUnion;

impl crate::Transform for DisjunctionToUnion {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
        Ok(())
    }
}

impl DisjunctionToUnion {
    /// Converts a disjunction into a union if that avoids a cross-join.
    pub fn action(&self, relation: &mut MirRelationExpr) {
        if let MirRelationExpr::Filter { input, predicates } = relation {
            if let MirRelationExpr::Join {
                inputs,
                equivalences,
                implementation: _,
            } = &**input
            {
                let input_mapper = JoinInputMapper::new(inputs);
                let cross_joined_inputs = inputs.iter().enumerate().flat_map(|(input_idx, _)| {
                    if equivalences.iter().any(|equivalence| {
                        // This assumes that join equivalences only referencing a single
                        // input are pushed down by predicate pushdown transform.
                        equivalence
                            .iter()
                            .any(|expr| input_mapper.lookup_inputs(expr).any(|i| i == input_idx))
                    }) {
                        None
                    } else {
                        Some(input_idx)
                    }
                });

                // Find the first cross-joined input for which there is a disjunction predicate
                // that binds at least of its columns in all the disjunctive branches.
                if let Some((predicate_idx, disjunction_terms)) = cross_joined_inputs
                    .map(|cross_joined_input| {
                        predicates
                            .iter()
                            .enumerate()
                            .flat_map(|(predicate_idx, predicate)| {
                                let disjunction_terms = extract_disjunction_terms(predicate);
                                if disjunction_terms.len() > 1
                                    && all_branches_bind_input(
                                        &disjunction_terms,
                                        cross_joined_input,
                                        &input_mapper,
                                    )
                                {
                                    Some((predicate_idx, disjunction_terms))
                                } else {
                                    None
                                }
                            })
                            .collect_vec()
                    })
                    .flatten()
                    .next()
                {
                    let mut branches = Vec::new();
                    for term_idx in 0..disjunction_terms.len() {
                        let branch_predicates = (0..term_idx)
                            .map(|p| {
                                disjunction_terms[p]
                                    .clone()
                                    .call_unary(UnaryFunc::Not(expr::func::Not))
                            })
                            .chain(std::iter::once(disjunction_terms[term_idx].clone()));
                        branches.push(input.clone().filter(branch_predicates));
                    }

                    **input = MirRelationExpr::union_many(branches, RelationType::empty());
                    predicates.remove(predicate_idx);
                }
            }
        }
    }
}

fn extract_disjunction_terms(expr: &MirScalarExpr) -> Vec<&MirScalarExpr> {
    extract_terms(BinaryFunc::Or, expr)
}

fn extract_terms(op: BinaryFunc, expr: &MirScalarExpr) -> Vec<&MirScalarExpr> {
    let mut stack = vec![expr];
    let mut terms = Vec::new();
    while let Some(expr) = stack.pop() {
        match expr {
            MirScalarExpr::CallBinary { func, expr1, expr2 } if *func == op => {
                stack.push(expr1);
                stack.push(expr2);
            }
            _ => terms.push(expr),
        }
    }
    terms
}

/// True if there is an equality predicate binding a column from input in all
/// expression in `branches`.
fn all_branches_bind_input(
    branches: &[&MirScalarExpr],
    input: usize,
    input_mapper: &JoinInputMapper,
) -> bool {
    branches.iter().all(|expr| {
        let terms = extract_terms(BinaryFunc::And, *expr);
        terms.iter().any(|expr| {
            if let MirScalarExpr::CallBinary {
                func: BinaryFunc::Eq,
                expr1,
                expr2,
            } = expr
            {
                match (&**expr1, &**expr2) {
                    (MirScalarExpr::Column(c1), MirScalarExpr::Column(c2)) => {
                        input_mapper.map_column_to_local(*c1).1 == input
                            && input_mapper.map_column_to_local(*c2).1 != input
                            || input_mapper.map_column_to_local(*c1).1 != input
                                && input_mapper.map_column_to_local(*c2).1 == input
                    }
                    (MirScalarExpr::Column(c), e) | (e, MirScalarExpr::Column(c))
                        if input_mapper.map_column_to_local(*c).1 == input =>
                    {
                        let lookup_inputs = input_mapper.lookup_inputs(e).collect::<HashSet<_>>();
                        (lookup_inputs.len() == 1 && !lookup_inputs.contains(&input))
                            || lookup_inputs.len() > 1
                    }
                    _ => false,
                }
            } else {
                false
            }
        })
    })
}
