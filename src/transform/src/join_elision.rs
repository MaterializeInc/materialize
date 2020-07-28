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

use crate::{RelationExpr, TransformArgs};

/// Removes unit collections from joins, and joins with fewer than two inputs.
#[derive(Debug)]
pub struct JoinElision;

impl crate::Transform for JoinElision {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: TransformArgs,
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
            inputs.retain(|input| {
                if let RelationExpr::Constant { rows, typ } = &input {
                    !(rows.len() == 1 && typ.column_types.len() == 0 && rows[0].1 == 1)
                } else {
                    true
                }
            });

            // If `inputs` is now empty or a singleton (without constraints),
            // we can remove the join.
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
