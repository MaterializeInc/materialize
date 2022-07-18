// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Pushes column removal down through other operators.
//!
//! This action improves the quality of the query by
//! reducing the width of data in the dataflow. It determines the unique
//! columns an expression depends on, and pushes a projection onto only
//! those columns down through child operators.
//!
//! A `MirRelationExpr::Project` node is actually three transformations in one.
//! 1) Projection - removes columns.
//! 2) Permutation - reorders columns.
//! 3) Repetition - duplicates columns.
//!
//! This action handles these three transformations like so:
//! 1) Projections are pushed as far down as possible.
//! 2) Permutations are pushed as far down as is convenient.
//! 3) Repetitions are not pushed down at all.
//!
//! Some comments have been inherited from the `Demand` transform.
//!
//! Note that this transform is one that can operate across views in a dataflow
//! and thus currently exists outside of both the physical and logical
//! optimizers.

use std::collections::{BTreeSet, HashMap};

use mz_expr::visit::Visit;
use mz_expr::{Id, JoinInputMapper, MirRelationExpr, MirScalarExpr};

use crate::{TransformArgs, TransformError};

/// Pushes projections down through other operators.
#[derive(Debug)]
pub struct ProjectionPushdown;

impl crate::Transform for ProjectionPushdown {
    // This method is only used during unit testing.
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.action(
            relation,
            &(0..relation.arity()).collect(),
            &mut HashMap::new(),
        )?;
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
    pub fn action(
        &self,
        relation: &mut MirRelationExpr,
        desired_projection: &Vec<usize>,
        gets: &mut HashMap<Id, BTreeSet<usize>>,
    ) -> Result<(), TransformError> {
        // First, try to push the desired projection down through `relation`.
        // In the process `relation` is transformed to a `MirRelationExpr`
        // equivalent to `relation.project(actual_projection)`.
        // There are three reasons why `actual_projection` may differ from
        // `desired_projection`:
        // 1) `relation` may need one or more columns that is not contained in
        //    `desired_projection`.
        // 2) `relation` may not be able to accommodate certain permutations.
        //    For example, `MirRelationExpr::Map` always appends all
        //    newly-created columns to the end.
        // 3) Nothing can be pushed through a leaf node. If `relation` is a leaf
        //    node, `actual_projection` will always be `(0..relation.arity())`.
        // Then, if `actual_projection` and `desired_projection` differ, we will
        // add a project around `relation`.
        let actual_projection = match relation {
            MirRelationExpr::Constant { .. } => (0..relation.arity()).collect(),
            MirRelationExpr::Get { id, .. } => {
                gets.entry(*id)
                    .or_insert_with(BTreeSet::new)
                    .extend(desired_projection.iter().cloned());
                (0..relation.arity()).collect()
            }
            MirRelationExpr::Let { id, value, body } => {
                // Let harvests any requirements of get from its body,
                // and pushes the sorted union of the requirements at its value.
                let id = Id::Local(*id);
                let prior = gets.insert(id, BTreeSet::new());
                self.action(body, desired_projection, gets)?;
                let desired_value_projection = gets.remove(&id).unwrap();
                if let Some(prior) = prior {
                    gets.insert(id, prior);
                }
                let desired_value_projection =
                    desired_value_projection.into_iter().collect::<Vec<_>>();
                self.action(value, &desired_value_projection, gets)?;
                self.update_projection_around_get(
                    body,
                    &HashMap::from_iter(std::iter::once((id, desired_value_projection))),
                )?;
                desired_projection.clone()
            }
            MirRelationExpr::Join {
                inputs,
                equivalences,
                ..
            } => {
                let input_mapper = JoinInputMapper::new(inputs);

                let mut columns_to_pushdown =
                    desired_projection.iter().cloned().collect::<BTreeSet<_>>();
                // Each equivalence class imposes internal demand for columns.
                for equivalence in equivalences.iter() {
                    for expr in equivalence.iter() {
                        columns_to_pushdown.extend(expr.support());
                    }
                }

                // Populate child demands from external and internal demands.
                let new_columns =
                    input_mapper.split_column_set_by_input(columns_to_pushdown.iter());

                // Recursively indicate the requirements.
                for (input, inp_columns) in inputs.iter_mut().zip(new_columns) {
                    let mut inp_columns = inp_columns.into_iter().collect::<Vec<_>>();
                    inp_columns.sort();
                    self.action(input, &inp_columns, gets)?;
                }

                reverse_permute(
                    equivalences.iter_mut().flat_map(|e| e.iter_mut()),
                    columns_to_pushdown.iter(),
                );

                columns_to_pushdown.into_iter().collect()
            }
            MirRelationExpr::FlatMap { input, func, exprs } => {
                let inner_arity = input.arity();
                // A FlatMap which returns zero rows acts like a filter
                // so we always need to execute it
                let mut columns_to_pushdown =
                    desired_projection.iter().cloned().collect::<BTreeSet<_>>();
                for expr in exprs.iter() {
                    columns_to_pushdown.extend(expr.support());
                }
                columns_to_pushdown.retain(|c| *c < inner_arity);

                reverse_permute(exprs.iter_mut(), columns_to_pushdown.iter());
                let columns_to_pushdown = columns_to_pushdown.into_iter().collect::<Vec<_>>();
                self.action(input, &columns_to_pushdown, gets)?;
                // The actual projection always has the newly-created columns at
                // the end.
                let mut actual_projection = columns_to_pushdown;
                for c in 0..func.output_type().arity() {
                    actual_projection.push(inner_arity + c);
                }
                actual_projection
            }
            MirRelationExpr::Filter { input, predicates } => {
                let mut columns_to_pushdown =
                    desired_projection.iter().cloned().collect::<BTreeSet<_>>();
                for predicate in predicates.iter() {
                    columns_to_pushdown.extend(predicate.support());
                }
                reverse_permute(predicates.iter_mut(), columns_to_pushdown.iter());
                let columns_to_pushdown = columns_to_pushdown.into_iter().collect::<Vec<_>>();
                self.action(input, &columns_to_pushdown, gets)?;
                columns_to_pushdown
            }
            MirRelationExpr::Project { input, outputs } => {
                // Combine `outputs` with `desired_projection`.
                *outputs = desired_projection.iter().map(|c| outputs[*c]).collect();

                let unique_outputs = outputs.iter().map(|i| *i).collect::<BTreeSet<_>>();
                if outputs.len() == unique_outputs.len() {
                    // Push down the project as is.
                    self.action(input, &outputs, gets)?;
                    *relation = input.take_dangerous();
                } else {
                    // Push down only the unique elems in `outputs`.
                    let columns_to_pushdown = unique_outputs.into_iter().collect::<Vec<_>>();
                    reverse_permute_columns(outputs.iter_mut(), columns_to_pushdown.iter());
                    self.action(input, &columns_to_pushdown, gets)?;
                }

                desired_projection.clone()
            }
            MirRelationExpr::Map { input, scalars } => {
                let arity = input.arity();
                // contains columns whose supports have yet to be explored
                let mut actual_projection =
                    desired_projection.iter().cloned().collect::<BTreeSet<_>>();
                for (i, scalar) in scalars.iter().enumerate().rev() {
                    if actual_projection.contains(&(i + arity)) {
                        actual_projection.extend(scalar.support());
                    }
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
                reverse_permute(scalars.iter_mut(), actual_projection.iter());
                self.action(
                    input,
                    &actual_projection
                        .iter()
                        .filter(|c| **c < arity)
                        .map(|c| *c)
                        .collect(),
                    gets,
                )?;
                actual_projection.into_iter().collect()
            }
            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                monotonic: _,
                expected_group_size: _,
            } => {
                let mut columns_to_pushdown = BTreeSet::new();
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

                reverse_permute(
                    group_key
                        .iter_mut()
                        .chain(aggregates.iter_mut().map(|a| &mut a.expr)),
                    columns_to_pushdown.iter(),
                );

                self.action(
                    input,
                    &columns_to_pushdown.into_iter().collect::<Vec<_>>(),
                    gets,
                )?;
                let mut actual_projection =
                    desired_projection.iter().cloned().collect::<BTreeSet<_>>();
                actual_projection.extend(0..group_key.len());
                actual_projection.into_iter().collect()
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
                    desired_projection.iter().cloned().collect::<BTreeSet<_>>();
                columns_to_pushdown.extend(group_key.iter().cloned());
                columns_to_pushdown.extend(order_key.iter().map(|o| o.column));
                // If the `TopK` does not have any new column demand, just push
                // down the desired projection. Otherwise, push down the sorted
                // column demand.
                let columns_to_pushdown = if columns_to_pushdown.len() == desired_projection.len() {
                    desired_projection.clone()
                } else {
                    columns_to_pushdown.into_iter().collect::<Vec<_>>()
                };
                reverse_permute_columns(
                    group_key
                        .iter_mut()
                        .chain(order_key.iter_mut().map(|o| &mut o.column)),
                    columns_to_pushdown.iter(),
                );
                self.action(input, &columns_to_pushdown, gets)?;
                columns_to_pushdown
            }
            MirRelationExpr::Negate { input } => {
                self.action(input, desired_projection, gets)?;
                desired_projection.clone()
            }
            MirRelationExpr::Union { base, inputs } => {
                self.action(base, desired_projection, gets)?;
                for input in inputs {
                    self.action(input, desired_projection, gets)?;
                }
                desired_projection.clone()
            }
            MirRelationExpr::Threshold { input } => {
                // Threshold requires all columns, as collapsing any distinct values
                // has the potential to change how it thresholds counts. This could
                // be improved with reasoning about distinctness or non-negativity.
                let arity = input.arity();
                self.action(input, &(0..arity).collect(), gets)?;
                (0..arity).collect()
            }
            MirRelationExpr::ArrangeBy { input, keys: _ } => {
                // Do not push the project past the ArrangeBy.
                // TODO: how do we handle key sets containing column references
                // that are not demanded upstream?
                let arity = input.arity();
                self.action(input, &(0..arity).collect(), gets)?;
                (0..arity).collect()
            }
        };
        let add_project = desired_projection != &actual_projection;
        if add_project {
            let mut projection_to_add = desired_projection.to_owned();
            reverse_permute_columns(projection_to_add.iter_mut(), actual_projection.iter());
            *relation = relation.take_dangerous().project(projection_to_add);
        }
        Ok(())
    }

    /// When we push the `desired_value_projection` at `value`,
    /// the columns returned by `Get(get_id)` will change, so we need
    /// to permute `Project`s around `Get(get_id)`.
    pub fn update_projection_around_get(
        &self,
        relation: &mut MirRelationExpr,
        applied_projections: &HashMap<Id, Vec<usize>>,
    ) -> Result<(), TransformError> {
        relation.visit_mut_pre(&mut |e| {
            if let MirRelationExpr::Project { input, outputs } = e {
                if let MirRelationExpr::Get { id: inner_id, .. } = &**input {
                    if let Some(new_projection) = applied_projections.get(inner_id) {
                        reverse_permute_columns(outputs.iter_mut(), new_projection.iter());
                        if outputs.len() == new_projection.len()
                            && outputs.iter().enumerate().all(|(i, o)| i == *o)
                        {
                            *e = input.take_dangerous();
                        }
                    }
                }
            }
            // If there is no `Project` around a Get, all columns of
            // `Get(get_id)` are required. Thus, the columns returned by
            // `Get(get_id)` will not have changed, so no action
            // is necessary.
        })?;
        Ok(())
    }
}

/// Applies the reverse of [MirScalarExpr.permute] on each expression.
///
/// `permutation` can be thought of as a mapping of column references from
/// `stateA` to `stateB`. [MirScalarExpr.permute] assumes that the column
/// references of the expression are in `stateA` and need to be remapped to
/// their `stateB` counterparts. This methods assumes that the column
/// references are in `stateB` and need to be remapped to `stateA`.
///
/// The `outputs` field of [MirRelationExpr::Project] is a mapping from "after"
/// to "before". Thus, when lifting projections, you would permute on `outputs`,
/// but you need to reverse permute when pushdown projections down.
fn reverse_permute<'a, I, J>(exprs: I, permutation: J)
where
    I: Iterator<Item = &'a mut MirScalarExpr>,
    J: Iterator<Item = &'a usize>,
{
    let reverse_col_map = permutation
        .enumerate()
        .map(|(idx, c)| (*c, idx))
        .collect::<HashMap<_, _>>();
    for expr in exprs {
        expr.permute_map(&reverse_col_map);
    }
}

/// Same as [reverse_permute], but takes column numbers as input
fn reverse_permute_columns<'a, I, J>(columns: I, permutation: J)
where
    I: Iterator<Item = &'a mut usize>,
    J: Iterator<Item = &'a usize>,
{
    let reverse_col_map = permutation
        .enumerate()
        .map(|(idx, c)| (*c, idx))
        .collect::<HashMap<_, _>>();
    for c in columns {
        *c = reverse_col_map[c];
    }
}
