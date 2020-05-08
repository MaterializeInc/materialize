// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Removes unit collections from joins, and joins with fewer than two inputs.
//!
//! Unit collections have no columns and a count of one, and a join with such
//! a collection act as the identity operator on collections. Once removed,
//! we may find joins with zero or one input, which can be further simplified.

use repr::RelationType;

use crate::{RelationExpr, TransformState};

/// Removes unit collections from joins, and joins with fewer than two inputs.
#[derive(Debug)]
pub struct JoinElision;

impl crate::Transform for JoinElision {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &mut TransformState,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut(&mut |e| {
            self.action(e);
        });
        Ok(())
    }
}

impl JoinElision {
    /// Removes unit collections from joins, and joins with fewer than two inputs.
    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Join {
            inputs,
            equivalences,
            ..
        } = relation
        {
            // We re-accumulate `inputs` into `new_inputs` in order to perform
            // some clean-up logic as we go. Mainly, we need to update the key
            // equivalence classes which contain relation indices which should
            // be decremented appropriately.

            // For each input relation, is it trivial and should be removed?
            let is_vacuous = inputs
                .iter()
                .map(|expression| {
                    if let RelationExpr::Constant { rows, typ } = &expression {
                        rows.len() == 1 && typ.column_types.len() == 0 && rows[0].1 == 1
                    } else {
                        false
                    }
                })
                .collect::<Vec<_>>();

            let new_inputs = inputs
                .drain(..)
                .enumerate()
                .filter(|(index, _)| !is_vacuous[*index])
                .map(|(_, expression)| expression)
                .collect::<Vec<_>>();

            // If `new_inputs` is empty or a singleton (without constraints) we can remove the join.
            *inputs = new_inputs;

            match inputs.len() {
                0 => {
                    // The identity for join is the collection containing a single 0-ary row.
                    *relation = RelationExpr::constant(vec![vec![]], RelationType::empty());
                }
                1 => {
                    // if there are constraints, they probably should have
                    // been pushed down by predicate pushdown, but .. let's
                    // not re-write that code here.
                    if equivalences.is_empty() {
                        *relation = inputs.pop().unwrap();
                    }
                }
                _ => {}
            }
        }
    }
}
