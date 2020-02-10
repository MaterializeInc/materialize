// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Simplifies some complex equalities in Joins to reduce
//! the amount of data volume produced intermediate relations.
//!
//! ```rust
//! use expr::{RelationExpr, ScalarExpr, BinaryFunc};
//! use repr::{RelationType, ColumnType, Datum, ScalarType};
//! use expr::transform::simplify::SimplifyFilterPredicates;
//!
//! // Common schema for each input.
//! let schema = RelationType::new(vec![
//!    ColumnType::new(ScalarType::Int32),
//!    ColumnType::new(ScalarType::Int32),
//! ]);
//!
//! // One piece of arbitrary data!
//! let data = vec![Datum::Int32(1), Datum::Int32(1)];
//!
//! // Two arbitrary inputs!
//! let input0 = RelationExpr::constant(vec![data.clone()], schema.clone());
//! let input1 = RelationExpr::constant(vec![data.clone()], schema.clone());
//!
//! // We will simplify Filter { Join {} } RelationExprs iff the Filter has a predicate
//! // that:
//! //      a) is a BinaryFunc::Eq of two ScalarExprs
//! //      b) one or both sides of the ScalarExprs are not Literals or Columns and
//! //         relies on exactly one input relation to be computed
//! //      c) neither side of the Eq relies on more that one input relation
//! // This tests an arbitrary example of such a Filter { Join {} }.
//! let join = RelationExpr::join(
//!     vec![input0.clone(), input1.clone()],
//!     vec![vec![]]
//! );
//! let complex_equality = ScalarExpr::Column(0).call_binary(ScalarExpr::Column(0), BinaryFunc::Eq);
//! let mut relation = RelationExpr::filter(
//!     join,
//!     vec![ScalarExpr::Column(1).call_binary(complex_equality.clone(), BinaryFunc::Eq)]
//! );
//!
//! SimplifyFilterPredicates.transform(&mut relation);
//!
//! // We expect:
//! //     1) The complex equality to be:
//! //          a) applied as a Map {} over the correct input relation
//! //          b) replaces in the list of Filter's predicates by a ScalarExpr::Column
//! //             pointing to the newly Map-ped column
//! //     2) The whole RelationExpr to be wrapped in a Project to remove the newly Map-ped column
//! let expected_join = RelationExpr::join(vec![input0.map(vec![complex_equality]), input1], vec![vec![]]);
//! let updated_equality = ScalarExpr::Column(1).call_binary(ScalarExpr::Column(2), BinaryFunc::Eq);
//! let expected_relation = expected_join.filter(vec![updated_equality]).project(vec![0, 1, 3, 4]);
//!
//! assert_eq!(relation, expected_relation);
//! ```

use std::collections::HashMap;
use std::mem;

use crate::{BinaryFunc, EvalEnv, GlobalId, RelationExpr, ScalarExpr};

#[derive(Debug)]
pub struct SimplifyFilterPredicates;

impl super::Transform for SimplifyFilterPredicates {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation);
    }
}

impl SimplifyFilterPredicates {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut_pre(&mut |e| {
            self.generate_simplified_relation(e);
        });
    }

    /// If the relation is a Filter { Join {} } and one of the Filter predicates
    /// contains an expression that is 1) complex 2) only depends on one input relation,
    /// we want to simplify the parameter relation.
    /// We simplify by:
    ///     1) Moving the complex predicate into a new Map{} over the correct input relation
    ///     2) Creating a new ScalarExpr::Column to replace the complex expression in the
    ///        Filter predicate that points to the output of the new Map{}.
    ///     3) Adding a Project{} on the outside of the RelationExpr to remove the newly
    ///        mapped column.
    pub fn generate_simplified_relation(&self, relation: &mut RelationExpr) {
        let mut columns_to_drop = Vec::new();
        if let RelationExpr::Filter { input, predicates } = relation {
            if let RelationExpr::Join { inputs, .. } = &mut **input {
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
            mem::replace(relation, full_new_relation);
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
        ScalarExpr::Literal(_data, _column_type) => None,
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
                    // TODO: #761 Don't simplify if either expr supports > 1 input_relation.
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
        scalars: vec![adjust_expr_column_references(expr, column_offset)],
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
