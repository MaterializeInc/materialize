// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transform column references in a `Map` into a `Project`.

use crate::{RelationExpr, ScalarExpr, TransformArgs};

/// Transform column references in a `Map` into a `Project`, or repeated
/// aggregations in a `Reduce` into a `Project`.
#[derive(Debug)]
pub struct ProjectionExtraction;

impl crate::Transform for ProjectionExtraction {
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
        } else if let RelationExpr::Reduce {
            input: _,
            group_key,
            aggregates,
        } = relation
        {
            // If any entry of aggregates exists earlier in aggregates, we can remove it
            // and replace it with a projection that points to the first instance of it.
            let mut projection = Vec::new();
            projection.extend(0..group_key.len());
            let mut finger = 0;
            let mut must_project = false;
            while finger < aggregates.len() {
                if let Some(position) = aggregates[..finger]
                    .iter()
                    .position(|x| x == &aggregates[finger])
                {
                    projection.push(group_key.len() + position);
                    aggregates.remove(finger);
                    must_project = true;
                } else {
                    projection.push(group_key.len() + finger);
                    finger += 1;
                }
            }
            if must_project {
                *relation = relation.take_dangerous().project(projection);
            }
        }
    }
}
