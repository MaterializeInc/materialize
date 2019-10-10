// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Write docs.
//!
//! ```rust
//! println!("write test");
//! ```

use crate::{BinaryFunc, RelationExpr, ScalarExpr};
use repr::{ColumnType, RelationType, ScalarType};
use std::mem;

#[derive(Debug)]
pub struct SimplifyJoinEqualities;

impl super::Transform for SimplifyJoinEqualities {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl SimplifyJoinEqualities {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut_pre(&mut |e| {
            self.generate_simplified_relation(e, &e.typ());
        });
    }

    pub fn generate_simplified_relation(
        &self,
        relation: &mut RelationExpr,
        _metadata: &RelationType,
    ) {
        let mut updated = false;
        let mut original_outputs = 0;
        if let RelationExpr::Filter { input, predicates } = relation {
            if let RelationExpr::Join {
                inputs,
                variables: _,
            } = &mut **input
            {
                let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
                let mut input_arities = input_types
                    .iter()
                    .map(|i| i.column_types.len())
                    .collect::<Vec<_>>();

                let mut offset = 0;
                let mut prior_arities = Vec::new();
                for input in 0..inputs.len() {
                    prior_arities.push(offset);
                    offset += input_arities[input];
                }

                let mut input_relation = input_arities
                    .iter()
                    .enumerate()
                    .flat_map(|(r, a)| std::iter::repeat(r).take(*a))
                    .collect::<Vec<_>>();
                original_outputs = input_relation.len();

                for predicate in predicates {
                    let updated_predicate = match &predicate {
                        ScalarExpr::CallBinary {
                            func: BinaryFunc::Eq,
                            expr1,
                            expr2,
                        } => {
                            let updated_expr1 = simplify_expr_if_complex_equality(
                                &**expr1,
                                inputs,
                                &mut input_relation,
                                &mut input_arities,
                                &mut prior_arities,
                            );
                            let updated_expr2 = simplify_expr_if_complex_equality(
                                &**expr2,
                                inputs,
                                &mut input_relation,
                                &mut input_arities,
                                &mut prior_arities,
                            );
                            match (updated_expr1, updated_expr2) {
                                (Some(updated_val1), Some(updated_val2)) => {
                                    Some(ScalarExpr::CallBinary {
                                        func: BinaryFunc::Eq,
                                        expr1: Box::from(updated_val1),
                                        expr2: Box::from(updated_val2),
                                    })
                                }
                                (Some(updated_val1), None) => Some(ScalarExpr::CallBinary {
                                    func: BinaryFunc::Eq,
                                    expr1: Box::from(updated_val1),
                                    expr2: Box::new(*expr2.clone()),
                                }),
                                (None, Some(updated_val2)) => Some(ScalarExpr::CallBinary {
                                    func: BinaryFunc::Eq,
                                    expr1: Box::new(*expr1.clone()),
                                    expr2: Box::from(updated_val2),
                                }),
                                (None, None) => None,
                            }
                        }
                        _ => None,
                    };
                    if let Some(updated_predicate_value) = updated_predicate {
                        mem::replace(predicate, updated_predicate_value);
                        updated = true;
                    }
                }
            }
        }
        if updated {
            mem::replace(
                relation,
                RelationExpr::Project {
                    input: Box::from(relation.to_owned()),
                    outputs: (0..original_outputs).collect(),
                },
            );
        }
    }
}

/// Given a &ScalarExpr, determine if it contains a complex
/// equality that supports exactly one input RelationExpr.
/// If the above is true, simplify the &ScalarExpr by turning it
/// from a:
///     Filter {
///         input: Join { inputs: [...], ... },
///         predicates: [complex_equality, ...],
///     }
/// to a :
///     Filter {
///         input: Join { inputs: [..., Map {}], ... }
///         predicates: [simple equality that points to column from newly generated Map {}, ...]
///     }
fn simplify_expr_if_complex_equality(
    expr: &ScalarExpr,
    inputs: &mut Vec<RelationExpr>,
    input_relation: &mut Vec<usize>,
    input_arities: &mut Vec<usize>,
    prior_arities: &mut Vec<usize>,
) -> Option<ScalarExpr> {
    match expr {
        ScalarExpr::Literal(_data) => None,
        ScalarExpr::Column(_usize) => None,
        _ => {
            // Check how many relations the expr supports.
            let mut support = Vec::new();
            expr.visit(&mut |e| {
                if let ScalarExpr::Column(i) = e {
                    support.push(input_relation[*i]);
                }
            });
            support.sort();
            support.dedup();

            match support.len() {
                0 => {
                    // ScalarExpr doesn't support any relations, nothing to simplify.
                    None
                }
                1 => {
                    // ScalarExpr supports exactly one relation. Simplify!
                    let input_index_to_update = support[0];
                    let column_offset_to_subtract = prior_arities[input_index_to_update];
                    let new_expr = create_new_expr_with_updated_column_references(
                        &expr,
                        column_offset_to_subtract,
                    );
                    let map = RelationExpr::Map {
                        input: Box::from(inputs[input_index_to_update].clone()),
                        scalars: vec![(
                            new_expr.clone(),
                            ColumnType {
                                name: Some(String::from("throwaway")),
                                nullable: false,
                                scalar_type: ScalarType::Null,
                            },
                        )],
                    };
                    inputs.push(map);

                    // Add new column for new mapped input.
                    input_arities.push(1);
                    prior_arities.push(prior_arities[prior_arities.len() - 1] + 1);
                    *input_relation = input_arities
                        .iter()
                        .enumerate()
                        .flat_map(|(r, a)| std::iter::repeat(r).take(*a))
                        .collect::<Vec<_>>();
                    // the last column of the input.
                    Some(ScalarExpr::Column(input_relation.len() - 1))
                }
                _ => {
                    // ScalarExpr supports >1 relation. Will need to actually join.
                    None
                }
            }
        }
    }
}

/// To simplify, we need to:
///     1. Pull complex equalities out of the ScalarExpr::Filter predicate
///        and turn them into new ScalarExpr::Map()s over copies of the
///        appropriate input.
///     2. Update the ScalarExpr::Filter predicate to remove the complex
///        equality.
///
/// This function tackles #2 in that it creates a NEW ScalarExpr predicate
/// to put into ScalarExpr::Filter that reflects the change from #1.
/// This is what prevents us from "optimizing" in an infinite loop.
fn create_new_expr_with_updated_column_references(
    expr: &ScalarExpr,
    column_offset_to_subtract: usize,
) -> ScalarExpr {
    match expr {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            let new_expr1 = match **expr1 {
                ScalarExpr::Column(c) => ScalarExpr::Column(c - column_offset_to_subtract),
                _ => expr1.as_ref().clone(),
            };
            let new_expr_2 = match **expr2 {
                ScalarExpr::Column(c) => ScalarExpr::Column(c - column_offset_to_subtract),
                _ => expr2.as_ref().clone(),
            };
            ScalarExpr::CallBinary {
                func: *func,
                expr1: Box::from(new_expr1),
                expr2: Box::from(new_expr_2),
            }
        }
        _ => {
            // todo: Rest of the ScalarExpr options!
            ScalarExpr::Column(0) // placeholder
        }
    }
}
