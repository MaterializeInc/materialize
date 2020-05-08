// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transform column references in a `Map` into a `Project`.

use crate::{RelationExpr, ScalarExpr, TransformState};

/// Transform column references in a `Map` into a `Project`.
#[derive(Debug)]
pub struct ProjectionExtraction;

impl crate::Transform for ProjectionExtraction {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: TransformState,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut(&mut |e| {
            self.action(e);
        });
        Ok(())
    }
}

impl ProjectionExtraction {
    /// Transform column references in a `Map` into a `Project`.
    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Map { input, scalars } = relation {
            if scalars.iter().any(|s| {
                if let ScalarExpr::Column(_) = s {
                    true
                } else {
                    false
                }
            }) {
                let input_arity = input.arity();
                let mut outputs: Vec<_> = (0..input_arity).collect();
                let mut dropped = 0;
                scalars.retain(|scalar| {
                    if let ScalarExpr::Column(col) = scalar {
                        dropped += 1;
                        // We may need to chase down a few levels of indirection;
                        // find the original input column in `outputs[*col]`.
                        outputs.push(outputs[*col]);
                        false // don't retain
                    } else {
                        outputs.push(outputs.len() - dropped);
                        true // retain
                    }
                });
                if dropped > 0 {
                    for scalar in scalars {
                        scalar.permute(&outputs);
                    }
                    *relation = relation.take_dangerous().project(outputs);
                }
            }
        }
    }
}
