// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Simplifies some complex equalities in Joins to reduce
//! the amount of data volume produced intermediate relations.
//!
//! ```rust
//! use expr::{RelationExpr, ScalarExpr, BinaryFunc};
//! use repr::{RelationType, ColumnType, Datum, ScalarType};
//! use expr::transform::simplify::SimplifyJoinEqualities;
//!
//! let mut relation = RelationExpr::Filter {
//!    input: Box::from(RelationExpr::Join {
//!        inputs: vec![
//!            RelationExpr::Get {
//!                name: String::from("aggdata"),
//!                typ: RelationType {
//!                    column_types: vec![
//!                        ColumnType {
//!                            nullable: false,
//!                            scalar_type: ScalarType::Int64,
//!                        },
//!                        ColumnType {
//!                            nullable: false,
//!                            scalar_type: ScalarType::Int64,
//!                        },
//!                    ],
//!                    keys: vec![
//!                        vec![],
//!                    ],
//!                },
//!            },
//!            RelationExpr::Get {
//!                name: String::from("aggdata"),
//!                typ: RelationType {
//!                    column_types: vec![
//!                        ColumnType {
//!                            nullable: false,
//!                            scalar_type: ScalarType::Int64,
//!                        },
//!                        ColumnType {
//!                            nullable: false,
//!                            scalar_type: ScalarType::Int64,
//!                        },
//!                    ],
//!                    keys: vec![
//!                        vec![],
//!                    ],
//!                },
//!            },
//!        ].to_owned(),
//!        variables: vec![],
//!    }),
//!    predicates: vec![
//!        ScalarExpr::CallBinary {
//!            func: BinaryFunc::Eq,
//!            expr1: Box::from(ScalarExpr::Column(
//!                2,
//!            )),
//!            expr2: Box::from(ScalarExpr::CallBinary {
//!                func: BinaryFunc::MulInt64,
//!                expr1: Box::from(ScalarExpr::Column(
//!                    0,
//!                )),
//!                expr2: Box::from(ScalarExpr::Column(
//!                    0,
//!                )),
//!            }),
//!        },
//!    ].to_owned(),
//!};
//!
//!SimplifyJoinEqualities.transform(&mut relation);
//!
//!let expected = RelationExpr::Project {
//!    input: Box::from(RelationExpr::Filter {
//!        input: Box::from(RelationExpr::Join {
//!            inputs: vec![
//!                RelationExpr::Map {
//!                    input: Box::from(RelationExpr::Get {
//!                        name: String::from("aggdata"),
//!                        typ: RelationType {
//!                            column_types: vec![
//!                                ColumnType {
//!                                    nullable: false,
//!                                    scalar_type: ScalarType::Int64,
//!                                },
//!                                ColumnType {
//!                                    nullable: false,
//!                                    scalar_type: ScalarType::Int64,
//!                                },
//!                            ],
//!                            keys: vec![
//!                                vec![],
//!                            ],
//!                        },
//!                    }),
//!                    scalars: vec![
//!                        (
//!                           ScalarExpr::CallBinary {
//!                                func: BinaryFunc::MulInt64,
//!                                expr1: Box::from(ScalarExpr::Column(
//!                                    0,
//!                                )),
//!                                expr2: Box::from(ScalarExpr::Column(
//!                                    0,
//!                                )),
//!                            },
//!                            ColumnType {
//!                                nullable: false,
//!                                scalar_type: ScalarType::Null,
//!                            },
//!                        ),
//!                    ],
//!                },
//!                RelationExpr::Get {
//!                    name: String::from("aggdata"),
//!                    typ: RelationType {
//!                        column_types: vec![
//!                            ColumnType {
//!                                nullable: false,
//!                                scalar_type: ScalarType::Int64,
//!                            },
//!                            ColumnType {
//!                                nullable: false,
//!                                scalar_type: ScalarType::Int64,
//!                            },
//!                        ],
//!                        keys: vec![
//!                            vec![],
//!                        ],
//!                    },
//!                },
//!            ].to_owned(),
//!            variables: vec![],
//!        }),
//!        predicates: vec![
//!            ScalarExpr::CallBinary {
//!                func: BinaryFunc::Eq,
//!                expr1: Box::from(ScalarExpr::Column(
//!                    3,
//!                )),
//!                expr2: Box::from(ScalarExpr::Column(
//!                    2,
//!                )),
//!            },
//!        ].to_owned(),
//!    }),
//!    outputs: vec![
//!        0,
//!        1,
//!        3,
//!        4,
//!    ],
//!};
//!
//! assert_eq!(relation, expected);
//! ```
use crate::{BinaryFunc, RelationExpr, ScalarExpr};
use repr::{ColumnType, ScalarType};
use std::mem;

#[derive(Debug)]
pub struct SimplifyJoinEqualities;

impl super::Transform for SimplifyJoinEqualities {
    fn transform(&self, relation: &mut RelationExpr) {
        self.transform(relation);
    }
}

impl SimplifyJoinEqualities {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut_pre(&mut |e| {
            self.generate_simplified_relation(e);
        });
    }

    pub fn generate_simplified_relation(&self, relation: &mut RelationExpr) {
        let mut columns_to_drop = Vec::new();
        if let RelationExpr::Filter { input, predicates } = relation {
            if let RelationExpr::Join {
                inputs,
                variables: _,
            } = &mut **input
            {
                let mut input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
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

                for i in 0..predicates.len() {
                    let new_predicate = if let ScalarExpr::CallBinary {
                        func: BinaryFunc::Eq,
                        expr1,
                        expr2,
                    } = &predicates[i]
                    {
                        match (
                            supports_complex_equality_on_input(&*expr1, &input_relation),
                            supports_complex_equality_on_input(&*expr2, &input_relation),
                        ) {
                            (Some(input_index_1), Some(input_index_2)) => {
                                // Predicates like `expr1 = expr2` where expr1, expr2 are both complex
                                // equalities and on the same input will already have been simplified.
                                // Will only get here if they are on different inputs.
                                let map_1 = create_map_over_input(
                                    inputs[input_index_1].take_dangerous(),
                                    expr1,
                                    prior_arities[input_index_1],
                                );
                                let map_1_arity = map_1.arity();
                                inputs[input_index_1] = map_1;

                                let mut new_column_input_1_index =
                                    prior_arities[input_index_1] + map_1_arity - 1;

                                let map_2 = create_map_over_input(
                                    inputs[input_index_2].take_dangerous(),
                                    expr2,
                                    prior_arities[input_index_2],
                                );
                                let map_2_arity = map_2.arity();
                                inputs[input_index_2] = map_2;

                                let mut new_column_input_2_index =
                                    prior_arities[input_index_2] + map_2_arity - 1;
                                if new_column_input_1_index <= new_column_input_2_index {
                                    new_column_input_2_index += 1;
                                } else {
                                    new_column_input_1_index += 1;
                                }
                                columns_to_drop.push(new_column_input_1_index);
                                columns_to_drop.push(new_column_input_2_index);

                                Some((
                                    ScalarExpr::CallBinary {
                                        func: BinaryFunc::Eq,
                                        expr1: Box::from(ScalarExpr::Column(
                                            new_column_input_1_index,
                                        )),
                                        expr2: Box::from(ScalarExpr::Column(
                                            new_column_input_2_index,
                                        )),
                                    },
                                    i,
                                    vec![new_column_input_1_index, new_column_input_2_index],
                                ))
                            }
                            (Some(input_index_1), None) => {
                                let map = create_map_over_input(
                                    inputs[input_index_1].take_dangerous(),
                                    expr1,
                                    prior_arities[input_index_1],
                                );
                                let map_arity = map.arity();
                                inputs[input_index_1] = map;

                                let new_column_index = prior_arities[input_index_1] + map_arity - 1;
                                columns_to_drop.push(new_column_index);

                                Some((
                                    ScalarExpr::CallBinary {
                                        func: BinaryFunc::Eq,
                                        expr1: Box::from(ScalarExpr::Column(new_column_index)),
                                        expr2: Box::from(match **expr2 {
                                            ScalarExpr::Column(index) => {
                                                if index >= new_column_index {
                                                    ScalarExpr::Column(index + 1)
                                                } else {
                                                    ScalarExpr::Column(index)
                                                }
                                            }
                                            _ => *expr2.clone(),
                                        }),
                                    },
                                    i,
                                    vec![new_column_index],
                                ))
                            }
                            (None, Some(input_index_2)) => {
                                let map = create_map_over_input(
                                    inputs[input_index_2].take_dangerous(),
                                    expr2,
                                    prior_arities[input_index_2],
                                );
                                let map_arity = map.arity();
                                inputs[input_index_2] = map;

                                let new_column_index = prior_arities[input_index_2] + map_arity - 1;
                                columns_to_drop.push(new_column_index);

                                Some((
                                    ScalarExpr::CallBinary {
                                        func: BinaryFunc::Eq,
                                        expr1: Box::from(match **expr1 {
                                            ScalarExpr::Column(index) => {
                                                if index >= new_column_index {
                                                    ScalarExpr::Column(index + 1)
                                                } else {
                                                    ScalarExpr::Column(index)
                                                }
                                            }
                                            _ => *expr1.clone(),
                                        }), // this column needs to be updated.
                                        expr2: Box::from(ScalarExpr::Column(new_column_index)),
                                    },
                                    i,
                                    vec![new_column_index],
                                ))
                            }
                            (None, None) => None,
                        }
                    } else {
                        None
                    };
                    if let Some((predicate, predicate_index, mut new_column_indices)) =
                        new_predicate
                    {
                        // Update all local variables tracking columns.
                        input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
                        input_arities = input_types
                            .iter()
                            .map(|i| i.column_types.len())
                            .collect::<Vec<_>>();

                        let mut offset = 0;
                        prior_arities = Vec::new();
                        for input in 0..inputs.len() {
                            prior_arities.push(offset);
                            offset += input_arities[input];
                        }

                        input_relation = input_arities
                            .iter()
                            .enumerate()
                            .flat_map(|(r, a)| std::iter::repeat(r).take(*a))
                            .collect::<Vec<_>>();

                        // Update all ScalarExpr::Column values if necessary.
                        if new_column_indices.len() == 1 {
                            let new_column_index = new_column_indices[0];
                            for i in 0..predicates.len() {
                                predicates[i].visit_mut(&mut |e| {
                                    if let ScalarExpr::Column(index) = e {
                                        if *index >= new_column_index {
                                            *index += 1;
                                        }
                                    }
                                });
                            }
                        } else {
                            new_column_indices.sort();
                            for i in 0..predicates.len() {
                                predicates[i].visit_mut(&mut |e| {
                                    if let ScalarExpr::Column(index) = e {
                                        if *index >= new_column_indices[0]
                                            && *index < new_column_indices[1]
                                        {
                                            *index += 1;
                                        } else if *index >= new_column_indices[0]
                                            && *index >= new_column_indices[1]
                                        {
                                            *index += 2;
                                        }
                                    }
                                });
                            }
                        }
                        predicates[predicate_index] = predicate;
                    };
                }
            }
        }
        if !columns_to_drop.is_empty() {
            let all_columns: Vec<usize> = (0..relation.arity()).collect();
            let columns_to_keep = all_columns
                .iter()
                .filter(|x| !columns_to_drop.contains(x))
                .copied()
                .collect();
            let full_new_relation = RelationExpr::Project {
                input: Box::from(relation.take_dangerous()),
                outputs: columns_to_keep,
            };
            mem::replace(relation, full_new_relation.clone());
        }
    }
}

/// A ScalarExpr supports a complex equality we can simplify
/// iff 1) it is not a ScalarExpr::Literal or Column and 2) it
/// only supports one relation.
/// A ScalarExpr supports a relation iff it requires using
/// values from that relation.
fn supports_complex_equality_on_input(
    expr: &ScalarExpr,
    input_relation: &[usize],
) -> Option<usize> {
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
                1 => Some(support[0]),
                _ => {
                    // If ScalarExpr doesn't support any relations (support.len() == 0):
                    // there's nothing to simplify.
                    // If ScalarExpr supports >1 relations: will have to actually join,
                    // can't simplify.
                    None
                }
            }
        }
    }
}

fn create_map_over_input(
    input: RelationExpr,
    expr: &ScalarExpr,
    column_offset: usize,
) -> RelationExpr {
    RelationExpr::Map {
        input: Box::from(input),
        scalars: vec![(
            adjust_expr_column_references(expr, column_offset),
            ColumnType {
                nullable: false,
                scalar_type: ScalarType::Null,
            },
        )],
    }
}

/// When moving a complex equality ScalarExpr from a RelationExpr::Join
/// predicate to a RelationExpr::Map scalar, we need to adjust the column
/// references to align with the inner Map, not the outer join.
fn adjust_expr_column_references(
    expr: &ScalarExpr,
    column_offset_to_subtract: usize,
) -> ScalarExpr {
    let mut expr = expr.clone();
    expr.visit_mut(&mut |e| {
        if let ScalarExpr::Column(column) = e {
            *column -= column_offset_to_subtract;
        }
    });
    expr
}
