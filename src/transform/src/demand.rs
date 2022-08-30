// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformation based on pushing demand information about columns toward sources.

use std::collections::{HashMap, HashSet};

use mz_expr::{
    AggregateExpr, AggregateFunc, Id, JoinInputMapper, MirRelationExpr, MirScalarExpr,
    RECURSION_LIMIT,
};
use mz_ore::stack::{CheckedRecursion, RecursionGuard};
use mz_repr::{Datum, Row};

use crate::TransformArgs;

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
pub struct Demand {
    recursion_guard: RecursionGuard,
}

impl Default for Demand {
    fn default() -> Demand {
        Demand {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl CheckedRecursion for Demand {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl crate::Transform for Demand {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.action(
            relation,
            (0..relation.arity()).collect(),
            &mut HashMap::new(),
        )
    }
}

impl Demand {
    /// Columns to be produced.
    fn action(
        &self,
        relation: &mut MirRelationExpr,
        mut columns: HashSet<usize>,
        gets: &mut HashMap<Id, HashSet<usize>>,
    ) -> Result<(), crate::TransformError> {
        self.checked_recur(|_| {
            // A valid relation type is only needed for Maps, but we can't borrow
            // the relation in the corresponding branch of the match statement, since
            // it is already borrowed mutably.
            let relation_type = if matches!(relation, MirRelationExpr::Map { .. }) {
                Some(relation.typ())
            } else {
                None
            };
            match relation {
                MirRelationExpr::Constant { .. } => {
                    // Nothing clever to do with constants, that I can think of.
                    Ok(())
                }
                MirRelationExpr::Get { id, .. } => {
                    gets.entry(*id).or_insert_with(HashSet::new).extend(columns);
                    Ok(())
                }
                MirRelationExpr::Let { id, value, body } => {
                    // Let harvests any requirements of get from its body,
                    // and pushes the union of the requirements at its value.
                    let id = Id::Local(*id);
                    let prior = gets.insert(id, HashSet::new());
                    self.action(body, columns, gets)?;
                    let needs = gets.remove(&id).unwrap();
                    if let Some(prior) = prior {
                        gets.insert(id, prior);
                    }

                    self.action(value, needs, gets)
                }
                MirRelationExpr::Project { input, outputs } => self.action(
                    input,
                    columns.into_iter().map(|c| outputs[c]).collect(),
                    gets,
                ),
                MirRelationExpr::Map { input, scalars } => {
                    let relation_type = relation_type.as_ref().unwrap();
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
                                *scalar = MirScalarExpr::Literal(
                                    Ok(Row::pack_slice(&[Datum::Dummy])),
                                    typ,
                                );
                            }
                        }
                    }

                    columns.retain(|c| *c < arity);
                    self.action(input, columns, gets)
                }
                MirRelationExpr::FlatMap {
                    input,
                    func: _,
                    exprs,
                } => {
                    // A FlatMap which returns zero rows acts like a filter
                    // so we always need to execute it
                    for expr in exprs {
                        columns.extend(expr.support());
                    }
                    columns.retain(|c| *c < input.arity());
                    self.action(input, columns, gets)
                }
                MirRelationExpr::Filter { input, predicates } => {
                    for predicate in predicates {
                        for column in predicate.support() {
                            columns.insert(column);
                        }
                    }
                    self.action(input, columns, gets)
                }
                MirRelationExpr::Join {
                    inputs,
                    equivalences,
                    implementation: _,
                } => {
                    let input_mapper = JoinInputMapper::new(inputs);

                    // Each produced column that is equivalent to a prior column should be remapped
                    // so that upstream uses depend only on the first column, simplifying the demand
                    // analysis. In principle we could choose any representative, if it turns out
                    // that some other column would have been more helpful, but we don't have a great
                    // reason to do that at the moment.
                    let mut permutation: Vec<usize> = (0..input_mapper.total_columns()).collect();
                    for equivalence in equivalences.iter() {
                        let mut first_column = None;
                        for expr in equivalence.iter() {
                            if let MirScalarExpr::Column(c) = expr {
                                if let Some(prior) = &first_column {
                                    permutation[*c] = *prior;
                                } else {
                                    first_column = Some(*c);
                                }
                            }
                        }
                    }

                    let should_permute = columns.iter().any(|c| permutation[*c] != *c);

                    // Each equivalence class imposes internal demand for columns.
                    for equivalence in equivalences.iter() {
                        for expr in equivalence.iter() {
                            columns.extend(expr.support());
                        }
                    }

                    // Populate child demands from external and internal demands.
                    let new_columns = input_mapper.split_column_set_by_input(columns.iter());

                    // Recursively indicate the requirements.
                    for (input, columns) in inputs.iter_mut().zip(new_columns) {
                        self.action(input, columns, gets)?;
                    }

                    // Install a permutation if any demanded column is not the
                    // canonical column.
                    if should_permute {
                        *relation = relation.take_dangerous().project(permutation);
                    }

                    Ok(())
                }
                MirRelationExpr::Reduce {
                    input,
                    group_key,
                    aggregates,
                    monotonic: _,
                    expected_group_size: _,
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
                            new_columns
                                .extend(aggregates[*column - group_key.len()].expr.support());
                        }
                    }

                    // Replace un-demanded aggregations with dummies.
                    let input_type = input.typ();
                    for index in (0..aggregates.len()).rev() {
                        if !columns.contains(&(group_key.len() + index)) {
                            let typ = aggregates[index].typ(&input_type.column_types);
                            aggregates[index] = AggregateExpr {
                                func: AggregateFunc::Dummy,
                                expr: MirScalarExpr::literal_ok(Datum::Dummy, typ.scalar_type),
                                distinct: false,
                            };
                        }
                    }

                    self.action(input, new_columns, gets)
                }
                MirRelationExpr::TopK {
                    input,
                    group_key,
                    order_key,
                    ..
                } => {
                    // Group and order keys must be retained, as they define
                    // which rows are retained.
                    columns.extend(group_key.iter().cloned());
                    columns.extend(order_key.iter().map(|o| o.column));
                    self.action(input, columns, gets)
                }
                MirRelationExpr::Negate { input } => self.action(input, columns, gets),
                MirRelationExpr::Threshold { input } => {
                    // Threshold requires all columns, as collapsing any distinct values
                    // has the potential to change how it thresholds counts. This could
                    // be improved with reasoning about distinctness or non-negativity.
                    let arity = input.arity();
                    self.action(input, (0..arity).collect(), gets)
                }
                MirRelationExpr::Union { base, inputs } => {
                    self.action(base, columns.clone(), gets)?;
                    for input in inputs {
                        self.action(input, columns.clone(), gets)?;
                    }
                    Ok(())
                }
                MirRelationExpr::ArrangeBy { input, keys } => {
                    for key_set in keys {
                        for key in key_set {
                            columns.extend(key.support());
                        }
                    }
                    self.action(input, columns, gets)
                }
            }
        })
    }
}
