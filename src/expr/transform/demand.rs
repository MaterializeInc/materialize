// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

// Prefering higher-order methods to equivalent lower complexity methods is wrong.
#![allow(clippy::or_fun_call)]

use std::collections::{HashMap, HashSet};

use crate::{Id, RelationExpr};

/// Drive demand from the root through operators.
///
/// This transformation primarily informs the `Join` operator, which can
/// simplify its intermediate state when it knows that certain columns are
/// not observed in its output. Internal arrangements need not maintain
/// columns that are no longer required in the join pipeline, which are
/// those columns not required by the output nor any further equalities.
#[derive(Debug)]
pub struct Demand;

impl crate::transform::Transform for Demand {
    fn transform(&self, relation: &mut RelationExpr) {
        self.transform(relation)
    }
}

impl Demand {
    pub fn transform(&self, relation: &mut RelationExpr) {
        self.action(
            relation,
            (0..relation.typ().column_types.len()).collect(),
            &mut HashMap::new(),
        );
    }

    /// Columns be produced.
    pub fn action(
        &self,
        relation: &mut RelationExpr,
        mut columns: HashSet<usize>,
        gets: &mut HashMap<Id, HashSet<usize>>,
    ) {
        match relation {
            RelationExpr::Constant { .. } => {
                // Nothing clever to do with constants, that I can think of.
            }
            RelationExpr::Get { id, .. } => {
                gets.entry(*id).or_insert(HashSet::new()).extend(columns);
            }
            RelationExpr::Let { id, value, body } => {
                // Let harvests any requirements of get from its body,
                // and pushes the union of the requirements at its value.
                let id = Id::Local(*id);
                let prior = gets.insert(id, HashSet::new());
                self.action(body, columns, gets);
                let needs = gets.remove(&id).unwrap();
                if let Some(prior) = prior {
                    gets.insert(id, prior);
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
                demand,
            } => {
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

                // We want to keep only one key from its equivalence class and permute
                // duplicate keys to their equivalent.
                let mut permutation: Vec<usize> = (0..input_arities.iter().sum()).collect();
                // Assumes the same column does not appear in the two different equivalence classes
                for variable in variables.iter() {
                    let (min_rel, min_col) = variable.iter().min().unwrap();
                    for (rel, col) in variable {
                        permutation[prior_arities[*rel] + col] = prior_arities[*min_rel] + *min_col;
                    }
                }

                // What the upstream relation demands from the join
                // organized by the input from which the demand will be fulfilled
                let mut demand_vec = vec![Vec::new(); inputs.len()];
                // What the join demands from each input
                let mut new_columns = vec![HashSet::new(); inputs.len()];

                // Permute each required column to its new location
                // and record it as demanded of both the input and the join
                for column in columns {
                    let projected_column = permutation[column];
                    let rel = input_relation[projected_column];
                    let col = projected_column - prior_arities[rel];
                    demand_vec[rel].push(col);
                    new_columns[rel].insert(col);
                }

                // Record column demands as an optional projection.
                *demand = Some(demand_vec);

                // The join also demands from each input any columns that
                // participate in constraints.
                for variable in variables.iter() {
                    for (rel, col) in variable {
                        new_columns[*rel].insert(*col);
                    }
                }

                // Recursively indicate the requirements.
                for (input, columns) in inputs.iter_mut().zip(new_columns) {
                    self.action(input, columns, gets);
                }

                *relation = relation.take_dangerous().project(permutation);
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let mut new_columns = HashSet::new();
                // Group keys determine aggregation granularity and are
                // each crucial in determining aggregates and even the
                // multiplicities of other keys.
                new_columns.extend(group_key.iter().cloned());
                for column in columns {
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
            RelationExpr::ArrangeBy { input, keys } => {
                columns.extend(keys.iter().cloned());
                self.action(input, columns, gets);
            }
        }
    }
}
