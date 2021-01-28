// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuses multiple `Union` operators into one.

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
    pub fn action(&self, relation: &mut MirRelationExpr) {
        let relation_type = relation.typ();
        if let MirRelationExpr::Union { base, inputs } = relation {
            let can_fuse = iter::once(&**base)
                .chain(&*inputs)
                .any(|input| matches!(input, MirRelationExpr::Union { .. }));
            if can_fuse {
                let mut new_inputs: Vec<MirRelationExpr> = vec![];
                for input in iter::once(&mut **base).chain(inputs) {
                    match input.take_dangerous() {
                        MirRelationExpr::Union { base, inputs } => {
                            new_inputs.push(*base);
                            new_inputs.extend(inputs);
                        }
                        input => new_inputs.push(input),
                    }
                }
                *relation = MirRelationExpr::union_many(new_inputs, relation_type);
            }
        }
    }
}
