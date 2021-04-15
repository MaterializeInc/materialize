// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuses multiple `Union` operators into one.
//!
//! Nested negated unions are merged into the parent one by pushing
//! the Negate to all their branches.

use std::iter;

use crate::TransformArgs;
use expr::MirRelationExpr;

/// Fuses multiple `Union` operators into one.
#[derive(Debug)]
pub struct Union;

impl crate::Transform for Union {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
        Ok(())
    }
}

impl Union {
    /// Fuses multiple `Union` operators into one.
    /// Nested negated unions are merged into the parent one by pushing
    /// the Negate to all their branches.
    pub fn action(&self, relation: &mut MirRelationExpr) {
        let relation_type = relation.typ();
        if let MirRelationExpr::Union { base, inputs } = relation {
            let can_fuse = iter::once(&**base).chain(&*inputs).any(|input| -> bool {
                match input {
                    MirRelationExpr::Union { .. } => true,
                    MirRelationExpr::Negate { input: inner_input } => {
                        if let MirRelationExpr::Union { .. } = **inner_input {
                            true
                        } else {
                            false
                        }
                    }
                    _ => false,
                }
            });
            if can_fuse {
                let mut new_inputs: Vec<MirRelationExpr> = vec![];
                for input in iter::once(&mut **base).chain(inputs) {
                    let outer_input = input.take_dangerous();
                    match outer_input {
                        MirRelationExpr::Union { base, inputs } => {
                            new_inputs.push(*base);
                            new_inputs.extend(inputs);
                        }
                        MirRelationExpr::Negate {
                            input: ref inner_input,
                        } => {
                            if let MirRelationExpr::Union { base, inputs } = &**inner_input {
                                new_inputs.push(base.to_owned().negate());
                                new_inputs.extend(inputs.into_iter().map(|x| x.clone().negate()));
                            } else {
                                new_inputs.push(outer_input);
                            }
                        }
                        _ => new_inputs.push(outer_input),
                    }
                }
                *relation = MirRelationExpr::union_many(new_inputs, relation_type);
            }
        }
    }
}
