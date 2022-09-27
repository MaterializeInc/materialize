// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Push non-null requirements toward sources.
//!
//! This analysis derives NonNull requirements on the arguments to predicates.
//! These requirements exist because most functions with Null arguments are
//! themselves Null, and a predicate that evaluates to Null will not pass.
//!
//! These requirements are not here introduced as constraints, but rather flow
//! to sources of data and restrict any constant collections to those rows that
//! satisfy the constraint. The main consequence is when Null values are added
//! in support of outer-joins and subqueries, we can occasionally remove that
//! branch when we observe that Null values would be subjected to predicates.
//!
//! This analysis relies on a careful understanding of `ScalarExpr` and the
//! semantics of various functions, *some of which may be non-Null even with
//! Null arguments*.
use std::collections::{HashMap, HashSet};

use crate::TransformArgs;
use itertools::{Either, Itertools};
use mz_expr::{Id, JoinInputMapper, MirRelationExpr, MirScalarExpr, RECURSION_LIMIT};
use mz_ore::stack::{CheckedRecursion, RecursionGuard};

/// Push non-null requirements toward sources.
#[derive(Debug)]
pub struct NonNullRequirements {
    recursion_guard: RecursionGuard,
}

impl Default for NonNullRequirements {
    fn default() -> NonNullRequirements {
        NonNullRequirements {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl CheckedRecursion for NonNullRequirements {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl crate::Transform for NonNullRequirements {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "non_null_requirements")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let result = self.action(relation, HashSet::new(), &mut HashMap::new());
        mz_repr::explain_new::trace_plan(&*relation);
        result
    }
}

impl NonNullRequirements {
    /// Push non-null requirements toward sources.
    pub fn action(
        &self,
        relation: &mut MirRelationExpr,
        mut columns: HashSet<usize>,
        gets: &mut HashMap<Id, Vec<HashSet<usize>>>,
    ) -> Result<(), crate::TransformError> {
        self.checked_recur(|_| {
            match relation {
                MirRelationExpr::Constant { rows, .. } => {
                    if let Ok(rows) = rows {
                        let mut datum_vec = mz_repr::DatumVec::new();
                        rows.retain(|(row, _)| {
                            let datums = datum_vec.borrow_with(&row);
                            columns.iter().all(|c| datums[*c] != mz_repr::Datum::Null)
                        })
                    }
                    Ok(())
                }
                MirRelationExpr::Get { id, .. } => {
                    gets.entry(*id).or_insert_with(Vec::new).push(columns);
                    Ok(())
                }
                MirRelationExpr::Let { id, value, body } => {
                    // Let harvests any non-null requirements from its body,
                    // and acts on the intersection of the requirements for
                    // each corresponding Get, pushing them at its value.
                    let id = Id::Local(*id);
                    let prior = gets.insert(id, Vec::new());
                    self.action(body, columns, gets)?;
                    let mut needs = gets.remove(&id).unwrap();
                    if let Some(prior) = prior {
                        gets.insert(id, prior);
                    }
                    if let Some(mut need) = needs.pop() {
                        while let Some(x) = needs.pop() {
                            need.retain(|col| x.contains(col))
                        }
                        self.action(value, need, gets)?;
                    }
                    Ok(())
                }
                MirRelationExpr::Project { input, outputs } => self.action(
                    input,
                    columns.into_iter().map(|c| outputs[c]).collect(),
                    gets,
                ),
                MirRelationExpr::Map { input, scalars } => {
                    let arity = input.arity();
                    if columns
                        .iter()
                        .any(|c| *c >= arity && scalars[*c - arity].is_literal_null())
                    {
                        // A null value was introduced in a marked column;
                        // the entire expression can be zeroed out.
                        relation.take_safely();
                        Ok(())
                    } else {
                        // For each column, if it must be non-null, extract the expression's
                        // non-null requirements and include them too. We go in reverse order
                        // to ensure we squeegee down all requirements even for references to
                        // other columns produced in this operator.
                        for column in (arity..(arity + scalars.len())).rev() {
                            if columns.contains(&column) {
                                scalars[column - arity].non_null_requirements(&mut columns);
                            }
                            columns.remove(&column);
                        }
                        self.action(input, columns, gets)
                    }
                }
                MirRelationExpr::FlatMap { input, func, exprs } => {
                    // Columns whose number is smaller than arity refer to
                    // columns of `input`. Columns whose number is
                    // greater than or equal to the arity refer to columns created
                    // by the FlatMap. The latter group of columns cannot be
                    // propagated down.
                    let arity = input.arity();
                    columns.retain(|c| *c < arity);

                    if func.empty_on_null_input() {
                        // we can safely disregard rows where any of the exprs
                        // evaluate to null
                        for expr in exprs {
                            expr.non_null_requirements(&mut columns);
                        }
                    }

                    // TODO: if `!func.empty_on_null_input()` and there are members
                    // of `columns` that refer to columns created by the FlatMap, we
                    // may be able to propagate some non-null requirements based on
                    // which columns created by the FlatMap cannot be null. However,
                    // we have been too lazy to handle this so far.

                    self.action(input, columns, gets)
                }
                MirRelationExpr::Filter { input, predicates } => {
                    for predicate in predicates {
                        predicate.non_null_requirements(&mut columns);
                        // TODO: Not(IsNull) should add a constraint!
                    }
                    self.action(input, columns, gets)
                }
                MirRelationExpr::Join {
                    inputs,
                    equivalences,
                    ..
                } => {
                    let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();

                    let input_mapper = JoinInputMapper::new_from_input_types(&input_types);

                    let mut new_columns = input_mapper.split_column_set_by_input(columns.iter());

                    // `variable` smears constraints around.
                    // Also, any non-nullable columns impose constraints on their equivalence class.
                    for equivalence in equivalences {
                        let exists_constraint = equivalence.iter().any(|expr| {
                            if let MirScalarExpr::Column(c) = expr {
                                let (col, rel) = input_mapper.map_column_to_local(*c);
                                new_columns[rel].contains(&col)
                                    || !input_types[rel].column_types[col].nullable
                            } else {
                                false
                            }
                        });

                        if exists_constraint {
                            for expr in equivalence.iter() {
                                if let MirScalarExpr::Column(c) = expr {
                                    let (col, rel) = input_mapper.map_column_to_local(*c);
                                    new_columns[rel].insert(col);
                                }
                            }
                        }
                    }

                    for (input, columns) in inputs.iter_mut().zip(new_columns) {
                        self.action(input, columns, gets)?;
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
                    let (group_key_columns, aggr_columns): (Vec<usize>, Vec<usize>) =
                        columns.iter().partition(|c| **c < group_key.len());
                    for column in group_key_columns {
                        group_key[column].non_null_requirements(&mut new_columns);
                    }

                    if !aggr_columns.is_empty() {
                        let (
                            mut inferred_nonnull_constraints,
                            mut ignored_nulls_by_remaining_aggregates,
                        ): (Vec<HashSet<usize>>, Vec<HashSet<usize>>) =
                            aggregates.iter().enumerate().partition_map(|(pos, aggr)| {
                                let mut ignores_nulls_on_columns = HashSet::new();
                                if let mz_repr::Datum::Null = aggr.func.identity_datum() {
                                    aggr.expr
                                        .non_null_requirements(&mut ignores_nulls_on_columns);
                                }
                                if aggr.func.propagates_nonnull_constraint()
                                    && aggr_columns.contains(&(group_key.len() + pos))
                                {
                                    Either::Left(ignores_nulls_on_columns)
                                } else {
                                    Either::Right(ignores_nulls_on_columns)
                                }
                            });

                        // Compute the intersection of all pushable non constraints inferred from
                        // the non-null constraints on aggregate columns and the nulls ignored by
                        // the remaining aggregates. Example:
                        // - SUM(#0 + #2), MAX(#0 + #1), non-null requirements on both aggs => implies !isnull(#0)
                        //  We don't want to push down a !isnull(#2) because deleting a row like (1,1, null) would
                        //  make the MAX wrong.
                        // - SUM(#0 + #2), MAX(#0 + #1), non-null requirements only on the MAX => implies !isnull(#0).
                        let mut pushable_nonnull_constraints: Option<HashSet<usize>> = None;
                        if !inferred_nonnull_constraints.is_empty() {
                            for column_set in inferred_nonnull_constraints
                                .drain(..)
                                .chain(ignored_nulls_by_remaining_aggregates.drain(..))
                            {
                                if let Some(previous) = pushable_nonnull_constraints {
                                    pushable_nonnull_constraints =
                                        Some(column_set.intersection(&previous).cloned().collect());
                                } else {
                                    pushable_nonnull_constraints = Some(column_set);
                                }
                            }
                        }

                        if let Some(pushable_nonnull_constraints) = pushable_nonnull_constraints {
                            new_columns.extend(pushable_nonnull_constraints);
                        }
                    }

                    self.action(input, new_columns, gets)
                }
                MirRelationExpr::TopK {
                    input, group_key, ..
                } => {
                    // We can only allow rows to be discarded if their key columns are
                    // NULL, as discarding rows based on other columns can change the
                    // result set, based on how NULL is ordered.
                    columns.retain(|c| group_key.contains(c));
                    // TODO(mcsherry): bind NULL ordering and apply the transformation
                    // to all columns if the correct ASC/DESC ordering is observed
                    // (with some care about orderings on multiple columns).
                    self.action(input, columns, gets)
                }
                MirRelationExpr::Negate { input } => self.action(input, columns, gets),
                MirRelationExpr::Threshold { input } => self.action(input, columns, gets),
                MirRelationExpr::Union { base, inputs } => {
                    self.action(base, columns.clone(), gets)?;
                    for input in inputs {
                        self.action(input, columns.clone(), gets)?;
                    }
                    Ok(())
                }
                MirRelationExpr::ArrangeBy { input, .. } => self.action(input, columns, gets),
            }
        })
    }
}
