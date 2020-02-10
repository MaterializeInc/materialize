// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::{EvalEnv, GlobalId, RelationExpr, ScalarExpr};

/// Extracts simple projections from the scalar expressions in a `Map` operator
/// into a `Project`, so they can be subjected to other optimizations.
#[derive(Debug)]
pub struct ProjectionExtraction;

impl super::Transform for ProjectionExtraction {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation)
    }
}

impl ProjectionExtraction {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut(&mut |e| {
            self.action(e);
        });
    }

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
                        outputs.push(*col);
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
