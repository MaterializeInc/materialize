// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Pushes projections containing unique columns down through other operators.
//!
//! This action improves the quality of the query, in that projections without
//! repeated columns reduce the width of data in the dataflow.
//!
//! Some comments have been inherited from the `Demand` transform.

use std::collections::{HashMap, HashSet};

use expr::{Id, JoinInputMapper, MirRelationExpr, MirScalarExpr};

use crate::TransformArgs;

/// Pushes projections down through other operators.
#[derive(Debug)]
pub struct ProjectionPushdown;

impl crate::Transform for ProjectionPushdown {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.action(
            relation,
            (0..relation.arity()).collect(),
            &mut HashMap::new(),
        );
        Ok(())
    }
}

impl ProjectionPushdown {
    /// Pushes the `desired_projection` down through `relation`.
    ///
    /// This action transforms `relation` to a `MirRelationExpr` equivalent to
    /// `relation.project(desired_projection)`.
    ///
    /// `desired_projection` is expected to consist of unique columns.
    fn action(
        &self,
        relation: &mut MirRelationExpr,
        mut desired_projection: Vec<usize>,
        gets: &mut HashMap<Id, HashSet<usize>>,
    ) {
        // Try to push the desired projection down through `relation`.
        // In the process `relation` is transformed to a ` MirRelationExpr`
        // equivalent to `relation.project(actual_projection)`.
        let actual_projection = match relation {
            // Nothing can be pushed through leaf nodes.
            MirRelationExpr::Constant { .. } => (0..relation.arity()).collect(),
            MirRelationExpr::Get { id, .. } => {
                gets.entry(*id)
                    .or_insert_with(HashSet::new)
                    .extend(desired_projection.clone());
                (0..relation.arity()).collect()
            }
            MirRelationExpr::Let { id, value, body } => {
                // Let harvests any requirements of get from its body,
                // and pushes the sorted union of the requirements at its value.
                let id = Id::Local(*id);
                let prior = gets.insert(id, HashSet::new());
                self.action(body, desired_projection.clone(), gets);
                let desired_value_projection = gets.remove(&id).unwrap();
                if let Some(prior) = prior {
                    gets.insert(id, prior);
                }
                let desired_value_projection = get_sorted_vec(desired_value_projection);
                body.visit_mut_pre(&mut |e| {
                    // When we push the `desired_value_projection` at `value`,
                    // the columns returned by `Get(id)` will change, so we need
                    // to permute `Project`s around `Get(id)`.
                    if let MirRelationExpr::Project { input, outputs } = e {
                        if let MirRelationExpr::Get { id: inner_id, .. } = &**input {
                            if *inner_id == id {
                                reverse_permute_columns(
                                    outputs.iter_mut(),
                                    desired_value_projection.iter(),
                                );
                                if outputs.len() == desired_value_projection.len()
                                    && (0..desired_value_projection.len())
                                        .zip(outputs.iter())
                                        .all(|(i, o)| i == *o)
                                {
                                    *e = input.take_dangerous();
                                }
                            }
                        }
                    }
                });
                self.action(value, desired_value_projection, gets);
                desired_projection.clone()
            }
            MirRelationExpr::Join {
                inputs,
                equivalences,
                ..
            } => {
                let input_mapper = JoinInputMapper::new(inputs);

                let mut columns_to_pushdown =
                    desired_projection.iter().cloned().collect::<HashSet<_>>();
                // Each equivalence class imposes internal demand for columns.
                for equivalence in equivalences.iter() {
                    for expr in equivalence.iter() {
                        columns_to_pushdown.extend(expr.support());
                    }
                }

                // Populate child demands from external and internal demands.
                let new_columns = input_mapper.split_column_set_by_input(&columns_to_pushdown);

                // Recursively indicate the requirements.
                for (input, inp_columns) in inputs.iter_mut().zip(new_columns) {
                    let inp_columns = get_sorted_vec(inp_columns);
                    self.action(input, inp_columns, gets);
                }

                let actual_projection = get_sorted_vec(columns_to_pushdown);

                reverse_permute(
                    equivalences.iter_mut().flat_map(|e| e.iter_mut()),
                    actual_projection.iter(),
                );

                actual_projection
            }
            MirRelationExpr::FlatMap {
                input,
                func,
                exprs,
                demand: _,
            } => {
                let inner_arity = input.arity();
                // A FlatMap which returns zero rows acts like a filter
                // so we always need to execute it
                let mut columns_to_pushdown =
                    desired_projection.iter().cloned().collect::<HashSet<_>>();
                for expr in exprs.iter() {
                    columns_to_pushdown.extend(expr.support());
                }
                let mut columns_to_pushdown = get_sorted_vec(columns_to_pushdown);
                columns_to_pushdown.retain(|c| *c < inner_arity);
                // The actual projection always has the newly-created columns at
                // the end.
                let mut actual_projection = columns_to_pushdown.clone();
                for c in 0..func.output_type().arity() {
                    actual_projection.push(inner_arity + c);
                }
                reverse_permute(exprs.iter_mut(), columns_to_pushdown.iter());
                self.action(input, columns_to_pushdown, gets);
                actual_projection
            }
            MirRelationExpr::Filter { input, predicates } => {
                let mut columns_to_pushdown =
                    desired_projection.iter().cloned().collect::<HashSet<_>>();
                for predicate in predicates.iter() {
                    columns_to_pushdown.extend(predicate.support());
                }
                let columns_to_pushdown = get_sorted_vec(columns_to_pushdown);
                reverse_permute(predicates.iter_mut(), columns_to_pushdown.iter());
                self.action(input, columns_to_pushdown.clone(), gets);
                columns_to_pushdown
            }
            MirRelationExpr::Project { input, outputs } => {
                // Combine `outputs` with `desired_projection`.
                *outputs = desired_projection.iter().map(|c| outputs[*c]).collect();

                let unique_outputs = outputs.iter().map(|i| *i).collect::<HashSet<_>>();
                if outputs.len() == unique_outputs.len() {
                    // Push down the project as is.
                    self.action(input, outputs.to_vec(), gets);
                    *relation = input.take_dangerous();
                } else {
                    // Push down only the unique elems in `outputs`.
                    let columns_to_pushdown = get_sorted_vec(unique_outputs);
                    reverse_permute_columns(outputs.iter_mut(), columns_to_pushdown.iter());
                    self.action(input, columns_to_pushdown, gets);
                }

                desired_projection.clone()
            }
            MirRelationExpr::Map { input, scalars } => {
                let arity = input.arity();
                // contains columns whose supports have yet to be explored
                let mut actual_projection =
                    desired_projection.iter().cloned().collect::<HashSet<_>>();
                let mut unexplored_supports = actual_projection.clone();
                unexplored_supports.retain(|c| *c >= arity);
                while !unexplored_supports.is_empty() {
                    // explore supports
                    unexplored_supports = unexplored_supports
                        .iter()
                        .flat_map(|c| scalars[*c - arity].support())
                        .filter(|c| !actual_projection.contains(c))
                        .collect();
                    // add those columns to the seen list
                    actual_projection.extend(unexplored_supports.clone());
                    unexplored_supports.retain(|c| *c >= arity);
                }
                *scalars = (0..scalars.len())
                    .filter_map(|i| {
                        if actual_projection.contains(&(i + arity)) {
                            Some(scalars[i].clone())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                let actual_projection = get_sorted_vec(actual_projection);
                reverse_permute(scalars.iter_mut(), actual_projection.iter());
                self.action(
                    input,
                    actual_projection
                        .iter()
                        .filter(|c| **c < arity)
                        .map(|c| *c)
                        .collect(),
                    gets,
                );
                actual_projection
            }
            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                monotonic: _,
                expected_group_size: _,
            } => {
                let mut columns_to_pushdown = HashSet::new();
                // Group keys determine aggregation granularity and are
                // each crucial in determining aggregates and even the
                // multiplicities of other keys.
                columns_to_pushdown.extend(group_key.iter().flat_map(|e| e.support()));

                for index in (0..aggregates.len()).rev() {
                    if !desired_projection.contains(&(group_key.len() + index)) {
                        aggregates.remove(index);
                    } else {
                        // No obvious requirements on aggregate columns.
                        // A "non-empty" requirement, I guess?
                        columns_to_pushdown.extend(aggregates[index].expr.support())
                    }
                }

                let columns_to_pushdown = get_sorted_vec(columns_to_pushdown);
                reverse_permute(
                    group_key
                        .iter_mut()
                        .chain(aggregates.iter_mut().map(|a| &mut a.expr)),
                    columns_to_pushdown.iter(),
                );

                self.action(input, columns_to_pushdown, gets);
                let mut actual_projection =
                    desired_projection.iter().cloned().collect::<HashSet<_>>();
                actual_projection.extend(0..group_key.len());
                get_sorted_vec(actual_projection)
            }
            MirRelationExpr::TopK {
                input,
                group_key,
                order_key,
                ..
            } => {
                // Group and order keys must be retained, as they define
                // which rows are retained.
                let mut columns_to_pushdown =
                    desired_projection.iter().cloned().collect::<HashSet<_>>();
                columns_to_pushdown.extend(group_key.iter().cloned());
                columns_to_pushdown.extend(order_key.iter().map(|o| o.column));
                // If the `TopK` does not have any new column demand, just push
                // down the desired projection. Otherwise, push down the sorted
                // column demand.
                let columns_to_pushdown = if columns_to_pushdown.len() == desired_projection.len() {
                    desired_projection.clone()
                } else {
                    get_sorted_vec(columns_to_pushdown)
                };
                reverse_permute_columns(
                    group_key
                        .iter_mut()
                        .chain(order_key.iter_mut().map(|o| &mut o.column)),
                    columns_to_pushdown.iter(),
                );
                self.action(input, columns_to_pushdown.clone(), gets);
                columns_to_pushdown
            }
            MirRelationExpr::Negate { input } => {
                self.action(input, desired_projection.clone(), gets);
                desired_projection.clone()
            }
            MirRelationExpr::Union { base, inputs } => {
                self.action(base, desired_projection.clone(), gets);
                for input in inputs {
                    self.action(input, desired_projection.clone(), gets);
                }
                desired_projection.clone()
            }
            MirRelationExpr::Threshold { input } => {
                // Threshold requires all columns, as collapsing any distinct values
                // has the potential to change how it thresholds counts. This could
                // be improved with reasoning about distinctness or non-negativity.
                let arity = input.arity();
                self.action(input, (0..arity).collect(), gets);
                (0..arity).collect()
            }
            MirRelationExpr::ArrangeBy { input, keys } => {
                // If a key set contains keys that are not demanded upstream,
                // delete the key set.
                // If `keys` becomes empty, delete the arrangeby, and keep
                // pushing the projection. Otherwise, do not push the project past
                // the ArrangeBy
                for i in (0..keys.len()).rev() {
                    if keys[i]
                        .iter()
                        .any(|k| k.support().iter().any(|c| !desired_projection.contains(c)))
                    {
                        keys.remove(i);
                    }
                }
                if keys.is_empty() {
                    self.action(input, desired_projection.clone(), gets);
                    *relation = input.take_dangerous();
                    desired_projection.clone()
                } else {
                    let arity = input.arity();
                    self.action(input, (0..arity).collect(), gets);
                    (0..arity).collect()
                }
            }
            MirRelationExpr::DeclareKeys { input, keys } => {
                // TODO[btv] - If and when we add a "debug mode" that asserts whether this is truly a key,
                // we will probably need to add the key to the set of demanded
                // columns.

                // Current behavior is that if a key is not contained with the
                // desired_projection, then it is not relevant to the query plan
                // and can be removed.
                for i in (0..keys.len()).rev() {
                    if keys[i].iter().any(|k| !desired_projection.contains(k)) {
                        keys.remove(i);
                    }
                }
                self.action(input, desired_projection.clone(), gets);
                if keys.is_empty() {
                    *relation = input.take_dangerous();
                }
                desired_projection.clone()
            }
        };
        let add_project = desired_projection != actual_projection;
        if add_project {
            reverse_permute_columns(desired_projection.iter_mut(), actual_projection.iter());
            *relation = relation.take_dangerous().project(desired_projection);
        }
    }
}

fn get_sorted_vec(set: HashSet<usize>) -> Vec<usize> {
    let mut result = set.into_iter().collect::<Vec<usize>>();
    result.sort();
    result
}

fn reverse_permute<'a, I, J>(iter_mut: I, permutation: J)
where
    I: Iterator<Item = &'a mut MirScalarExpr>,
    J: Iterator<Item = &'a usize>,
{
    let reverse_col_map = permutation
        .enumerate()
        .map(|(idx, c)| (*c, idx))
        .collect::<HashMap<_, _>>();
    for expr in iter_mut {
        expr.permute_map(&reverse_col_map);
    }
}

fn reverse_permute_columns<'a, I, J>(iter_mut: I, permutation: J)
where
    I: Iterator<Item = &'a mut usize>,
    J: Iterator<Item = &'a usize>,
{
    let reverse_col_map = permutation
        .enumerate()
        .map(|(idx, c)| (*c, idx))
        .collect::<HashMap<_, _>>();
    for c in iter_mut {
        *c = reverse_col_map[c];
    }
}
