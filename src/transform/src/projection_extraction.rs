// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transform column references in a `Map` into a `Project`.

use crate::TransformArgs;
use mz_expr::visit::Visit;
use mz_expr::{MirRelationExpr, MirScalarExpr};

/// Transform column references in a `Map` into a `Project`, or repeated
/// aggregations in a `Reduce` into a `Project`.
#[derive(Debug)]
pub struct ProjectionExtraction;

impl crate::Transform for ProjectionExtraction {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "projection_extraction")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let result = relation.try_visit_mut_post(&mut |e| self.action(e));
        mz_repr::explain_new::trace_plan(&*relation);
        result
    }
}

impl ProjectionExtraction {
    /// Transform column references in a `Map` into a `Project`.
    pub fn action(&self, relation: &mut MirRelationExpr) -> Result<(), crate::TransformError> {
        if let MirRelationExpr::Map { input, scalars } = relation {
            if scalars
                .iter()
                .any(|s| matches!(s, MirScalarExpr::Column(_)))
            {
                let input_arity = input.arity();
                let mut outputs: Vec<_> = (0..input_arity).collect();
                let mut dropped = 0;
                scalars.retain(|scalar| {
                    if let MirScalarExpr::Column(col) = scalar {
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
        } else if let MirRelationExpr::Reduce {
            input: _,
            group_key,
            aggregates,
            monotonic: _,
            expected_group_size: _,
        } = relation
        {
            let mut projection = Vec::new();

            // If any key is an exact duplicate, we can remove it and use a projection.
            let mut finger = 0;
            while finger < group_key.len() {
                if let Some(position) = group_key[..finger]
                    .iter()
                    .position(|x| x == &group_key[finger])
                {
                    projection.push(position);
                    group_key.remove(finger);
                } else {
                    projection.push(finger);
                    finger += 1;
                }
            }

            // If any entry of aggregates exists earlier in aggregates, we can remove it
            // and replace it with a projection that points to the first instance of it.
            let mut finger = 0;
            while finger < aggregates.len() {
                if let Some(position) = aggregates[..finger]
                    .iter()
                    .position(|x| x == &aggregates[finger])
                {
                    projection.push(group_key.len() + position);
                    aggregates.remove(finger);
                } else {
                    projection.push(group_key.len() + finger);
                    finger += 1;
                }
            }
            if projection.iter().enumerate().any(|(i, p)| i != *p) {
                *relation = relation.take_dangerous().project(projection);
            }
        }
        Ok(())
    }
}
