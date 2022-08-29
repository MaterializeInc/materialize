// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Definition and helper structs for the [`NonNegative`] attribute.

use mz_expr::Id;
use mz_expr::MirRelationExpr;
use typemap_rev::TypeMap;
use typemap_rev::TypeMapKey;

use super::subtree_size::SubtreeSize;
use super::AttributeBuilder;
use super::{Attribute, Env};

/// Traverses a [`MirRelationExpr`] tree and figures out whether for subtree
/// the sum of all diffs up to a specific time for any record can be a
/// negative value.
///
/// The results for each subtree are accumulated in a bottom-up order
/// in [`NonNegative::results`].
///
/// This method is a conservative approximation and is known to miss not-hard
/// cases.
///
/// It assumes that all `Get` bindings correspond to collections without negative
/// multiplicities. Local let bindings present in `safe_lets` are relied on to have
/// no non-negative multiplicities by the `env` field.
#[derive(Default)]
#[allow(missing_debug_implementations)]
pub struct NonNegative {
    /// Environment of computed values for this attribute.
    env: Env<Self>,
    /// A vector of results for all nodes in the visited tree in
    /// post-visit order.
    pub results: Vec<bool>,
}

impl TypeMapKey for NonNegative {
    type Value = NonNegative;
}

impl Attribute for NonNegative {
    type Value = bool;

    fn derive(&mut self, expr: &MirRelationExpr, deps: &TypeMap) {
        use MirRelationExpr::*;
        let n = self.results.len();
        match expr {
            Constant { rows, .. } => {
                if let Ok(rows) = rows {
                    let has_negative_rows = rows.iter().any(|(_data, diff)| diff < &0);
                    self.results.push(!has_negative_rows);
                } else {
                    self.results.push(true); // constant errors are considered "non-negative"
                }
            }
            Get { id, .. } => match id {
                Id::Local(id) => match self.env.get(id) {
                    Some(value) => self.results.push(value.clone()),
                    None => self.results.push(false),
                },
                Id::Global(_) => {
                    self.results.push(true);
                }
            },
            Let {
                value: _, body: _, ..
            } => {
                let body = self.results[n - 1];
                self.results.push(body);
            }
            Project { input: _, .. } => {
                let input = self.results[n - 1];
                self.results.push(input);
            }
            Map { input: _, .. } => {
                let input = self.results[n - 1];
                self.results.push(input);
            }
            FlatMap { input: _, .. } => {
                let input = self.results[n - 1];
                self.results.push(input);
            }
            Filter { input: _, .. } => {
                let input = self.results[n - 1];
                self.results.push(input);
            }
            Join { inputs, .. } => {
                let mut result = true;
                let mut offset = 1;
                for _ in 0..inputs.len() {
                    result &= &self.results[n - offset];
                    offset += &deps.get::<SubtreeSize>().unwrap().results[n - offset];
                }
                self.results.push(result); // can be refined
            }
            Reduce { input: _, .. } => {
                let input = self.results[n - 1];
                self.results.push(input);
            }
            TopK { input: _, .. } => {
                let input = self.results[n - 1];
                self.results.push(input);
            }
            Negate { input: _ } => {
                self.results.push(false); // can be refined
            }
            Threshold { input: _ } => {
                self.results.push(true);
            }
            Union { base: _, inputs } => {
                let mut result = true;
                let mut offset = 1;
                for _ in 0..inputs.len() {
                    result &= &self.results[n - offset];
                    offset += &deps.get::<SubtreeSize>().unwrap().results[n - offset];
                }
                result &= &self.results[n - offset]; // include the base result
                self.results.push(result); // can be refined
            }
            ArrangeBy { input: _, .. } => {
                let input = self.results[n - 1];
                self.results.push(input);
            }
        }
    }

    fn schedule_env_tasks(&mut self, expr: &MirRelationExpr) {
        self.env.schedule_tasks(expr);
    }

    fn handle_env_tasks(&mut self) {
        self.env.handle_tasks(&self.results);
    }

    fn add_dependencies(builder: &mut AttributeBuilder)
    where
        Self: Sized,
    {
        builder.add_attribute::<SubtreeSize>();
    }

    fn get_results(&self) -> &Vec<Self::Value> {
        &self.results
    }

    fn get_results_mut(&mut self) -> &mut Vec<Self::Value> {
        &mut self.results
    }
}
