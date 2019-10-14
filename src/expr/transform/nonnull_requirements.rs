// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::RelationExpr;
use std::collections::HashSet;

/// Drive non-null requirements to `RelationExpr::Constant` collections.
///
/// This analysis derives NonNull requirements on the arguments to predicates.
/// These requirements exist because most functions with Null arguments are
/// themselves Null, and a predicate that evaluates to Null will not pass.
///
/// These requirements are not here introduced as constraints, but rather flow
/// to sources of data and restrict any constant collections to those rows that
/// satisfy the constraint. The main consequence is when Null values are added
/// in support of outer-joins and subqueries, we can occasionally remove that
/// branch when we observe that Null values would be subjected to predicates.
///
/// This analysis relies on a careful understanding of `ScalarExpr` and the
/// semantics of various functions, *some of which may be non-Null even with
/// Null arguments*.
#[derive(Debug)]
pub struct NonNullRequirements;

impl crate::transform::Transform for NonNullRequirements {
    fn transform(&self, relation: &mut RelationExpr) {
        self.transform(relation)
    }
}

impl NonNullRequirements {
    pub fn transform(&self, relation: &mut RelationExpr) {
        self.action(relation, HashSet::new());
    }
    /// Columns that must be non-null.
    pub fn action(&self, relation: &mut RelationExpr, mut columns: HashSet<usize>) {
        match relation {
            RelationExpr::Constant { rows, .. } => {
                rows.retain(|(row, _)| columns.iter().all(|c| row[*c] != repr::Datum::Null))
            }
            RelationExpr::Get { .. } => {}
            RelationExpr::Let { body, .. } => {
                self.action(body, columns);
            }
            RelationExpr::Project { input, outputs } => {
                self.action(input, columns.into_iter().map(|c| outputs[c]).collect());
            }
            RelationExpr::Map { input, scalars } => {
                let arity = input.arity();
                let mut new_columns = HashSet::new();
                for column in columns {
                    if column < arity {
                        new_columns.insert(column);
                    } else {
                        scalars[column - arity].non_null_requirements(&mut new_columns);
                    }
                }
                self.action(input, new_columns);
            }
            RelationExpr::Filter { input, predicates } => {
                for predicate in predicates {
                    predicate.non_null_requirements(&mut columns);
                    // TODO: Equality constraints should smear around requirements!
                    // TODO: Not(IsNull) should add a constraint!
                }
                self.action(input, columns);
            }
            RelationExpr::Join { inputs, variables } => {
                let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
                let input_arities = input_types
                    .iter()
                    .map(|i| i.column_types.len())
                    .collect::<Vec<_>>();

                let mut offset = 0;
                let mut prior_arities = Vec::new();
                for input in 0..inputs.len() {
                    prior_arities.push(offset);
                    offset += input_arities[input];
                }

                let input_relation = input_arities
                    .iter()
                    .enumerate()
                    .flat_map(|(r, a)| std::iter::repeat(r).take(*a))
                    .collect::<Vec<_>>();

                let mut new_columns = vec![HashSet::new(); inputs.len()];
                for column in columns {
                    let input = input_relation[column];
                    new_columns[input].insert(column - prior_arities[input]);
                }

                // `variable` smears constraints around.
                for variable in variables {
                    if variable.iter().any(|(r, c)| new_columns[*r].contains(c)) {
                        for (r, c) in variable {
                            new_columns[*r].insert(*c);
                        }
                    }
                }

                for (input, columns) in inputs.iter_mut().zip(new_columns) {
                    self.action(input, columns);
                }
            }
            RelationExpr::Reduce {
                input, group_key, ..
            } => {
                let mut new_columns = HashSet::new();
                for column in columns {
                    // No obvious requirements on aggregate columns.
                    // A "non-empty" requirement, I guess?
                    if column < group_key.len() {
                        new_columns.insert(group_key[column]);
                    }
                }
                self.action(input, new_columns);
            }
            RelationExpr::TopK { input, .. } => {
                self.action(input, columns);
            }
            RelationExpr::Negate { input } => {
                self.action(input, columns);
            }
            RelationExpr::Threshold { input } => {
                self.action(input, columns);
            }
            RelationExpr::Union { left, right } => {
                self.action(left, columns.clone());
                self.action(right, columns);
            }
        }
    }
}
