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
//! our ability to plan these joins, and reason about other operators motion
//! aroud them.

use crate::TransformArgs;
use expr::{MirRelationExpr, MirScalarExpr};

/// Fuses multiple `Join` operators into one `Join` operator.
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

            new_equivalences.extend(equivalences.drain(..));

            *inputs = new_inputs;
            *equivalences = new_equivalences;
            *demand = None;
            *implementation = expr::JoinImplementation::Unimplemented;

            expr::canonicalize::canonicalize_equivalences(equivalences);
        }
    }
}
