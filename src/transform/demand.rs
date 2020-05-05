// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};

use expr::{GlobalId, Id, RelationExpr, ScalarExpr};

/// Drive demand from the root through operators.
///
/// This transform alerts operators to their columns that influence the
/// ultimate output of the expression, and gives them permission to swap
/// other columns for dummy values. As part of this, operators should not
/// actually use any of these dummy values, lest they run-time error.
///
/// This transformation primarily informs the `Join` operator, which can
/// simplify its intermediate state when it knows that certain columns are
/// not observed in its output. Internal arrangements need not maintain
/// columns that are no longer required in the join pipeline, which are
/// those columns not required by the output nor any further equalities.
#[derive(Debug)]
pub struct Demand;

impl crate::Transform for Demand {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
    ) -> Result<(), crate::TransformError> {
        self.transform(relation);
        Ok(())
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

    /// Columns to be produced.
    pub fn action(
        &self,
        relation: &mut RelationExpr,
        mut columns: HashSet<usize>,
        gets: &mut HashMap<Id, HashSet<usize>>,
    ) {
        let relation_type = relation.typ();
        match relation {
            RelationExpr::Constant { .. } => {
                // Nothing clever to do with constants, that I can think of.
            }
            RelationExpr::Get { id, .. } => {
                gets.entry(*id).or_insert_with(HashSet::new).extend(columns);
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
                // contains columns whose supports have yet to be explored
                let mut new_columns = columns.clone();
                new_columns.retain(|c| *c >= arity);
                while !new_columns.is_empty() {
                    // explore supports
                    new_columns = new_columns
                        .iter()
                        .flat_map(|c| scalars[*c - arity].support())
                        .filter(|c| !columns.contains(c))
                        .collect();
                    // add those columns to the seen list
                    columns.extend(new_columns.clone());
                    new_columns.retain(|c| *c >= arity);
                }

                // Replace un-read expressions with literals to prevent evaluation.
                for (index, scalar) in scalars.iter_mut().enumerate() {
                    if !columns.contains(&(arity + index)) {
                        // Leave literals as they are, to benefit explain.
                        if !scalar.is_literal() {
                            let typ = relation_type.column_types[arity + index].clone();
                            let datum = if typ.nullable {
                                repr::Datum::Null
                            } else {
                                typ.scalar_type.dummy_datum()
                            };
                            *scalar = ScalarExpr::Literal(Ok(repr::Row::pack(Some(datum))), typ);
                        }
                    }
                }

                columns.retain(|c| *c < arity);
                self.action(input, columns, gets);
            }
            RelationExpr::FlatMap {
                input,
                func: _,
                exprs,
                demand,
            } => {
                let mut sorted = columns.iter().cloned().collect::<Vec<_>>();
                sorted.sort();
                *demand = Some(sorted);
                // A FlatMap which returns zero rows acts like a filter
                // so we always need to execute it
                for expr in exprs {
                    columns.extend(expr.support());
                }
                columns.retain(|c| *c < input.arity());
                self.action(input, columns, gets);
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
                equivalences,
                demand,
                implementation: _,
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

                // Each produced column that is equivalent to a prior column should be remapped
                // so that upstream uses depend only on the first column, simplifying the demand
                // analysis. In principle we could choose any representative, if it turns out
                // that some other column would have been more helpful, but we don't have a great
                // reason to do that at the moment.
                let mut permutation: Vec<usize> = (0..input_arities.iter().sum()).collect();
                for equivalence in equivalences.iter() {
                    let mut first_column = None;
                    for expr in equivalence.iter() {
                        if let ScalarExpr::Column(c) = expr {
                            if let Some(prior) = &first_column {
                                permutation[*c] = *prior;
                            } else {
                                first_column = Some(*c);
                            }
                        }
                    }
                }

                // Capture the external demand for the join. Use the permutation to intervene
                // when an externally demanded column will be replaced with a copy of another.
                let mut demand_vec = columns.iter().map(|c| permutation[*c]).collect::<Vec<_>>();
                demand_vec.sort();
                *demand = Some(demand_vec);
                let should_permute = columns.iter().any(|c| permutation[*c] != *c);

                // Each equivalence class imposes internal demand for columns.
                for equivalence in equivalences.iter() {
                    for expr in equivalence.iter() {
                        columns.extend(expr.support());
                    }
                }

                // Populate child demands from external and internal demands.
                let mut new_columns = vec![HashSet::new(); inputs.len()];
                for column in columns.iter() {
                    let input = input_relation[*column];
                    new_columns[input].insert(*column - prior_arities[input]);
                }

                // Recursively indicate the requirements.
                for (input, columns) in inputs.iter_mut().zip(new_columns) {
                    self.action(input, columns, gets);
                }

                // Install a permutation if any demanded column is not the
                // canonical column.
                if should_permute {
                    *relation = relation.take_dangerous().project(permutation);
                }
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
                new_columns.extend(group_key.iter().flat_map(|e| e.support()));
                for column in columns.iter() {
                    // No obvious requirements on aggregate columns.
                    // A "non-empty" requirement, I guess?
                    if *column >= group_key.len() {
                        new_columns.extend(aggregates[*column - group_key.len()].expr.support());
                    }
                }

                // Replace un-demanded aggregations with literals.
                let input_type = input.typ();
                for index in (0..aggregates.len()).rev() {
                    if !columns.contains(&(group_key.len() + index)) {
                        if !aggregates[index].expr.is_literal() {
                            let typ = aggregates[index].expr.typ(&input_type);
                            let datum = if typ.nullable {
                                repr::Datum::Null
                            } else {
                                typ.scalar_type.dummy_datum()
                            };
                            aggregates[index].expr =
                                ScalarExpr::Literal(Ok(repr::Row::pack(Some(datum))), typ);
                        }
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
                // Threshold requires all columns, as collapsing any distinct values
                // has the potential to change how it thresholds counts. This could
                // be improved with reasoning about distinctness or non-negativity.
                let arity = input.arity();
                self.action(input, (0..arity).collect(), gets);
            }
            RelationExpr::Union { left, right } => {
                self.action(left, columns.clone(), gets);
                self.action(right, columns, gets);
            }
            RelationExpr::ArrangeBy { input, keys } => {
                for key_set in keys {
                    for key in key_set {
                        columns.extend(key.support());
                    }
                }
                self.action(input, columns, gets);
            }
        }
    }
}
