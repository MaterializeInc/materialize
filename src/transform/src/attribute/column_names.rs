// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Definition and helper structs for the [`ColumnNames`] attribute.

use std::ops::Range;

use mz_expr::{Id, MirRelationExpr, MirScalarExpr};
use mz_repr::explain::ExprHumanizer;

use crate::attribute::subtree_size::SubtreeSize;
use crate::attribute::{Attribute, DerivedAttributes, DerivedAttributesBuilder, Env};

/// Compute the column types of each subtree of a [MirRelationExpr] from the
/// bottom-up.
#[allow(missing_debug_implementations)]
pub struct ColumnNames<'c> {
    humanizer: &'c dyn ExprHumanizer,
    /// Environment of computed values for this attribute
    env: Env<Self>,
    /// A vector of results for all nodes in the visited tree in
    /// post-visit order. An empty string denotes a missing value.
    pub results: Vec<Vec<String>>,
}

impl<'c> ColumnNames<'c> {
    /// Construct a new attribute instance.
    pub fn new(humanizer: &'c dyn ExprHumanizer) -> Self {
        Self {
            humanizer,
            env: Env::empty(),
            results: Default::default(),
        }
    }

    fn infer<'a, I>(&self, expr: &MirRelationExpr, mut input_results: I) -> Vec<String>
    where
        I: Iterator<Item = &'a Vec<String>>,
    {
        use MirRelationExpr::*;

        match expr {
            Constant { rows: _, typ } => {
                // Fallback to an anonymous schema for constants.
                ColumnNames::anonymous(0..typ.arity()).collect()
            }
            Get {
                id: Id::Global(id),
                typ,
                access_strategy: _,
            } => {
                if let Some(column_names) = self.humanizer.column_names_for_id(*id) {
                    column_names
                } else {
                    // Possible as some ExprHumanizer impls still return None.
                    ColumnNames::anonymous(0..typ.arity()).collect()
                }
            }
            Get {
                id: Id::Local(id),
                typ,
                access_strategy: _,
            } => {
                if let Some(column_names) = self.env.get(id) {
                    column_names.clone()
                } else {
                    // Possible because we infer LetRec bindings in order. This
                    // can be improved by introducing a fixpoint loop in the
                    // Env<A>::schedule_tasks LetRec handling block.
                    ColumnNames::anonymous(0..typ.arity()).collect()
                }
            }
            Let {
                id: _,
                value: _,
                body: _,
            } => {
                // Return the column names of the `body`.
                input_results.last().unwrap().clone()
            }
            LetRec {
                ids: _,
                values: _,
                limits: _,
                body: _,
            } => {
                // Return the column names of the `body`.
                input_results.last().unwrap().clone()
            }
            Project { input: _, outputs } => {
                // Permute the column names of the input.
                let input_column_names = input_results.next().unwrap();
                let mut column_names = vec![];
                for col in outputs {
                    column_names.push(input_column_names[*col].clone());
                }
                column_names
            }
            Map { input: _, scalars } => {
                // Extend the column names of the input with anonymous columns.
                let mut column_names = input_results.next().unwrap().clone();
                ColumnNames::extend_with_scalars(&mut column_names, scalars);
                column_names
            }
            FlatMap {
                input: _,
                func,
                exprs: _,
            } => {
                // Extend the column names of the input with anonymous columns.
                let mut column_names = input_results.next().unwrap().clone();
                let func_output_start = column_names.len();
                let func_output_end = column_names.len() + func.output_arity();
                column_names.extend(ColumnNames::anonymous(func_output_start..func_output_end));
                column_names
            }
            Filter {
                input: _,
                predicates: _,
            } => {
                // Return the column names of the `input`.
                input_results.next().unwrap().clone()
            }
            Join {
                inputs: _,
                equivalences: _,
                implementation: _,
            } => {
                let mut column_names = vec![];
                for input_column_names in input_results {
                    column_names.extend(input_column_names.iter().cloned());
                }
                column_names
            }
            Reduce {
                input: _,
                group_key,
                aggregates,
                monotonic: _,
                expected_group_size: _,
            } => {
                // We clone and extend the input vector and then remove the part
                // associated with the input at the end.
                let mut column_names = input_results.next().unwrap().clone();
                let input_arity = column_names.len();

                // Infer the group key part.
                ColumnNames::extend_with_scalars(&mut column_names, group_key);
                // Infer the aggregates part.
                let aggs_start = group_key.len();
                let aggs_end = group_key.len() + aggregates.len();
                column_names.extend(ColumnNames::anonymous(aggs_start..aggs_end));
                // Remove the prefix associated with the input
                column_names.drain(0..input_arity);

                column_names
            }
            TopK {
                input: _,
                group_key: _,
                order_key: _,
                limit: _,
                offset: _,
                monotonic: _,
                expected_group_size: _,
            } => {
                // Return the column names of the `input`.
                input_results.next().unwrap().clone()
            }
            Negate { input: _ } => {
                // Return the column names of the `input`.
                input_results.next().unwrap().clone()
            }
            Threshold { input: _ } => {
                // Return the column names of the `input`.
                input_results.next().unwrap().clone()
            }
            Union { base: _, inputs: _ } => {
                // Use the first non-empty column across all inputs.
                let mut column_names = vec![];

                let base_results = input_results.next().unwrap();
                let inputs_results = input_results.collect::<Vec<_>>();

                for (i, mut column_name) in base_results.iter().cloned().enumerate() {
                    for input_results in inputs_results.iter() {
                        if column_name.is_empty() && !input_results[i].is_empty() {
                            column_name = input_results[i].clone();
                            break;
                        }
                    }
                    column_names.push(column_name);
                }

                column_names
            }
            ArrangeBy { input: _, keys: _ } => {
                // Return the column names of the `input`.
                input_results.next().unwrap().clone()
            }
        }
    }

    /// fallback schema consisting of ordinal column names: #0, #1, ...
    fn anonymous(range: Range<usize>) -> impl Iterator<Item = String> {
        range.map(|_| String::new())
    }

    /// fallback schema consisting of ordinal column names: #0, #1, ...
    fn extend_with_scalars(column_names: &mut Vec<String>, scalars: &Vec<MirScalarExpr>) {
        for scalar in scalars {
            column_names.push(match scalar {
                MirScalarExpr::Column(c) => column_names[*c].clone(),
                _ => String::new(),
            });
        }
    }
}

impl<'a> Attribute for ColumnNames<'a> {
    type Value = Vec<String>;

    fn derive(&mut self, expr: &MirRelationExpr, deps: &DerivedAttributes) {
        let n = self.results.len();
        let mut offsets = Vec::new();
        let mut offset = 1;
        for _ in 0..expr.num_inputs() {
            offsets.push(n - offset);
            offset += &deps.get_results::<SubtreeSize>()[n - offset];
        }
        let input_schemas = offsets.into_iter().rev().map(|o| &self.results[o]);
        self.results.push(self.infer(expr, input_schemas));
    }

    fn schedule_env_tasks(&mut self, expr: &MirRelationExpr) {
        self.env.schedule_tasks(expr);
    }

    fn handle_env_tasks(&mut self) {
        self.env.handle_tasks(&self.results);
    }

    fn add_dependencies(builder: &mut DerivedAttributesBuilder)
    where
        Self: Sized,
    {
        builder.require(SubtreeSize::default());
    }

    fn get_results(&self) -> &Vec<Self::Value> {
        &self.results
    }

    fn get_results_mut(&mut self) -> &mut Vec<Self::Value> {
        &mut self.results
    }

    fn take(self) -> Vec<Self::Value> {
        self.results
    }
}
