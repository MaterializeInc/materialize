// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

// Prefering higher-order methods to equivalent lower complexity methods is wrong.
#![allow(clippy::or_fun_call)]

use crate::RelationExpr;
use std::collections::{HashMap, HashSet};

/// Drive demand from the root through operators.
///
/// This transformation primarily informs the `Join` operator, which can
/// simplify its intermediate state with
#[derive(Debug)]
pub struct Demand;

impl crate::transform::Transform for Demand {
    fn transform(&self, relation: &mut RelationExpr) {
        self.transform(relation)
    }
}

impl Demand {
    pub fn transform(&self, relation: &mut RelationExpr) {
        self.action(relation, HashSet::new(), &mut HashMap::new());
    }
    /// Columns be produced.
    pub fn action(
        &self,
        relation: &mut RelationExpr,
        mut columns: HashSet<usize>,
        gets: &mut HashMap<String, HashSet<usize>>,
    ) {
        match relation {
            RelationExpr::Constant { .. } => {
                // Nothing clever to do with constants, that I can think of.
            }
            RelationExpr::Get { name, .. } => {
                gets.entry(name.to_string())
                    .or_insert(HashSet::new())
                    .extend(columns);
            }
            RelationExpr::Let { name, value, body } => {
                // Let harvests any requirements of get from its body,
                // and pushes the union of the requirements at its value.
                let prior = gets.insert(name.to_string(), HashSet::new());
                self.action(body, columns, gets);
                let needs = gets.remove(name).unwrap();
                if let Some(prior) = prior {
                    gets.insert(name.to_string(), prior);
                }

                self.action(value, needs, gets);
            }
            RelationExpr::Project { input, outputs } => {
                self.action(
                    input,
                    columns.into_iter().map(|c| outputs[c]).collect(),
                    gets,
                );
            }
            RelationExpr::Map { input, scalars } => {
                let arity = input.arity();
                let mut new_columns = HashSet::new();
                for column in columns {
                    if column < arity {
                        new_columns.insert(column);
                    } else {
                        new_columns.extend(scalars[column - arity].support());
                    }
                }
                self.action(input, new_columns, gets);
            }
            RelationExpr::Filter { input, predicates } => {
                for predicate in predicates {
                    for column in predicate.support() {
                        columns.insert(column);
                    }
                }
                self.action(input, columns, gets);
            }
            RelationExpr::Join {
                inputs,
                variables,
                projection,
            } => {
                // Record column demands as an optional projection.
                *projection = Some(columns.iter().cloned().collect());

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

                // We certainly need each required column from its input.
                let mut new_columns = vec![HashSet::new(); inputs.len()];
                for column in columns {
                    let input = input_relation[column];
                    new_columns[input].insert(column - prior_arities[input]);
                }

                // We also need any columns that participate in constraints.
                for variable in variables {
                    for (rel, col) in variable {
                        new_columns[*rel].insert(*col);
                    }
                }

                // Recursively indicate the requirements.
                for (input, columns) in inputs.iter_mut().zip(new_columns) {
                    self.action(input, columns, gets);
                }
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let mut new_columns = HashSet::new();
                for column in columns {
                    // Group keys determine aggregation granularity and are
                    // each crucial in determining aggregates and even the
                    // multiplicities of other keys.
                    new_columns.extend(group_key.iter().cloned());
                    // No obvious requirements on aggregate columns.
                    // A "non-empty" requirement, I guess?
                    if column >= group_key.len() {
                        new_columns.extend(aggregates[column - group_key.len()].expr.support());
                    }
                }
                self.action(input, new_columns, gets);
            }
            RelationExpr::TopK {
                input,
                group_key,
                order_key,
                ..
            } => {
                // Group and order keys must be retained, as they define
                // which rows are retained.
                columns.extend(group_key.iter().cloned());
                columns.extend(order_key.iter().map(|o| o.column));
                self.action(input, columns, gets);
            }
            RelationExpr::Negate { input } => {
                self.action(input, columns, gets);
            }
            RelationExpr::Threshold { input } => {
                self.action(input, columns, gets);
            }
            RelationExpr::Union { left, right } => {
                self.action(left, columns.clone(), gets);
                self.action(right, columns, gets);
            }
        }
    }
}
