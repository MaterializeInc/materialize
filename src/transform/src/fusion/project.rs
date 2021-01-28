// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuses Project operators with parent operators when possible.

// TODO(frank): evaluate for redundancy with projection hoisting.

use crate::TransformArgs;
use expr::MirRelationExpr;

/// Fuses Project operators with parent operators when possible.
#[derive(Debug)]
pub struct Project;

impl crate::Transform for Project {
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

impl Project {
    /// Fuses Project operators with parent operators when possible.
    pub fn action(&self, relation: &mut MirRelationExpr) {
        if let MirRelationExpr::Project { input, outputs } = relation {
            while let MirRelationExpr::Project {
                input: inner,
                outputs: outputs2,
            } = &mut **input
            {
                *outputs = outputs.iter().map(|i| outputs2[*i]).collect();
                **input = inner.take_dangerous();
            }
            if outputs.iter().enumerate().all(|(a, b)| a == *b) && outputs.len() == input.arity() {
                *relation = input.take_dangerous();
            }
        }

        // Any reduce will absorb any project. Also, this happens often.
        if let MirRelationExpr::Reduce {
            input,
            group_key,
            aggregates,
            monotonic: _,
            expected_group_size: _,
        } = relation
        {
            if let MirRelationExpr::Project {
                input: inner,
                outputs,
            } = &mut **input
            {
                // Rewrite the group key using `inner` columns.
                for key in group_key.iter_mut() {
                    key.permute(&outputs[..]);
                }
                for aggregate in aggregates.iter_mut() {
                    aggregate.expr.permute(&outputs[..]);
                }
                *input = Box::new(inner.take_dangerous());
            }
        }
    }
}
