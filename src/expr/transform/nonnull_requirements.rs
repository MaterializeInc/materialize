// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};

use crate::{EvalEnv, GlobalId, Id, RelationExpr, ScalarExpr, UnaryTableFunc};

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
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation)
    }
}

impl NonNullRequirements {
    pub fn transform(&self, relation: &mut RelationExpr) {
        self.action(relation, HashSet::new(), &mut HashMap::new());
    }
    /// Columns that must be non-null.
    pub fn action(
        &self,
        relation: &mut RelationExpr,
        mut columns: HashSet<usize>,
        gets: &mut HashMap<Id, Vec<HashSet<usize>>>,
    ) {
        match relation {
            RelationExpr::Constant { rows, .. } => rows.retain(|(row, _)| {
                let datums = row.unpack();
                columns.iter().all(|c| datums[*c] != repr::Datum::Null)
            }),
            RelationExpr::Get { id, .. } => {
                gets.entry(*id).or_insert_with(|| Vec::new()).push(columns);
            }
            RelationExpr::Let { id, value, body } => {
                // Let harvests any non-null requirements from its body,
                // and acts on the intersection of the requirements for
                // each corresponding Get, pushing them at its value.
                let id = Id::Local(*id);
                let prior = gets.insert(id, Vec::new());
                self.action(body, columns, gets);
                let mut needs = gets.remove(&id).unwrap();
                if let Some(prior) = prior {
                    gets.insert(id, prior);
                }
                if let Some(mut need) = needs.pop() {
                    while let Some(x) = needs.pop() {
                        need.retain(|col| x.contains(col))
                    }
                    self.action(value, need, gets);
                }
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
                        scalars[column - arity].non_null_requirements(&mut new_columns);
                    }
                }
                self.action(input, new_columns, gets);
            }
            RelationExpr::FlatMapUnary {
                input,
                func,
                expr,
                demand: _,
            } => {
                match func {
                    // outputs zero rows if input is null
                    UnaryTableFunc::JsonbEach
                    | UnaryTableFunc::JsonbObjectKeys
                    | UnaryTableFunc::JsonbArrayElements
                    | UnaryTableFunc::RegexpExtract(_)
                    | UnaryTableFunc::CsvExtract(_) => {
                        expr.non_null_requirements(&mut columns);
                    }
                }
                self.action(input, columns, gets);
            }
            RelationExpr::Filter { input, predicates } => {
                for predicate in predicates {
                    predicate.non_null_requirements(&mut columns);
                    // TODO: Not(IsNull) should add a constraint!
                }
                self.action(input, columns, gets);
            }
            RelationExpr::Join {
                inputs, variables, ..
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

                let mut new_columns = vec![HashSet::new(); inputs.len()];
                for column in columns {
                    let input = input_relation[column];
                    new_columns[input].insert(column - prior_arities[input]);
                }

                // `variable` smears constraints around.
                // Also, any non-nullable columns impose constraints on their equivalence class.
                for variable in variables {
                    let exists_constraint =
                        variable.iter().any(|(r, c)| new_columns[*r].contains(c));
                    let nonnull_columns = variable
                        .iter()
                        .any(|(r, c)| !input_types[*r].column_types[*c].nullable);
                    if exists_constraint || nonnull_columns {
                        for (r, c) in variable {
                            new_columns[*r].insert(*c);
                        }
                    }
                }

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
                    // No obvious requirements on aggregate columns.
                    // A "non-empty" requirement, I guess?
                    if column < group_key.len() {
                        group_key[column].non_null_requirements(&mut new_columns);
                    }
                    if column == group_key.len()
                        && aggregates.len() == 1
                        && aggregates[0].func != crate::AggregateFunc::CountAll
                    {
                        aggregates[0].expr.non_null_requirements(&mut new_columns);
                    }
                }
                self.action(input, new_columns, gets);
            }
            RelationExpr::TopK { input, .. } => {
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
            RelationExpr::ArrangeBy { input, .. } => {
                self.action(input, columns, gets);
            }
        }
    }
}
