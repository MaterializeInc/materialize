// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuses multiple `Join` operators into one `Join` operator.
//!
//! Multiway join planning relies on a broad view of the involved relations,
//! and chains of binary joins can make this challenging to reason about.
//! Collecting multiple joins together with their constraints improves
//! our ability to plan these joins, and reason about other operators' motion
//! around them.
//!
//! Also removes unit collections from joins, and joins with fewer than two inputs.
//!
//! Unit collections have no columns and a count of one, and a join with such
//! a collection act as the identity operator on collections. Once removed,
//! we may find joins with zero or one input, which can be further simplified.

use crate::TransformArgs;
use expr::{MirRelationExpr, MirScalarExpr};
use repr::RelationType;

/// Fuses multiple `Join` operators into one `Join` operator.
///
/// Removes unit collections from joins, and joins with fewer than two inputs.
#[derive(Debug)]
pub struct Join;

impl crate::Transform for Join {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut(&mut |e| {
            self.action(e);
        });
        Ok(())
    }
}

impl Join {
    /// Fuses multiple `Join` operators into one `Join` operator.
    pub fn action(&self, relation: &mut MirRelationExpr) {
        if let MirRelationExpr::Join {
            inputs,
            equivalences,
            demand,
            implementation,
        } = relation
        {
            let mut new_inputs = Vec::new();
            let mut new_equivalences = Vec::new();
            let mut new_columns = 0;

            // We scan through each input, digesting any joins that we find and updating their equivalence classes.
            // We retain any existing equivalence classes, as they are already with respect to the cross product.
            for input in inputs.drain(..) {
                if let MirRelationExpr::Join {
                    mut inputs,
                    mut equivalences,
                    ..
                } = input
                {
                    // Update and push all of the variables.
                    for mut equivalence in equivalences.drain(..) {
                        for expr in equivalence.iter_mut() {
                            expr.visit_mut(&mut |e| {
                                if let MirScalarExpr::Column(c) = e {
                                    *c += new_columns;
                                }
                            });
                        }
                        new_equivalences.push(equivalence);
                    }
                    // Add all of the inputs.
                    for input in inputs.drain(..) {
                        new_columns += input.arity();
                        new_inputs.push(input);
                    }
                } else {
                    // Retain the input.
                    new_columns += input.arity();
                    new_inputs.push(input);
                }
            }

            // Filter join identities out of the inputs.
            // The join identity is a single 0-ary row constant expression.
            new_inputs.retain(|input| {
                if let MirRelationExpr::Constant {
                    rows: Ok(rows),
                    typ,
                } = &input
                {
                    !(rows.len() == 1 && typ.column_types.len() == 0 && rows[0].1 == 1)
                } else {
                    true
                }
            });

            new_equivalences.extend(equivalences.drain(..));
            expr::canonicalize::canonicalize_equivalences(&mut new_equivalences);

            *inputs = new_inputs;
            *equivalences = new_equivalences;
            *demand = None;
            *implementation = expr::JoinImplementation::Unimplemented;

            // If `inputs` is now empty or a singleton (without constraints),
            // we can remove the join.
            match inputs.len() {
                0 => {
                    // The identity for join is the collection containing a single 0-ary row.
                    *relation = MirRelationExpr::constant(vec![vec![]], RelationType::empty());
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
