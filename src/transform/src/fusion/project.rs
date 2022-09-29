// Copyright Materialize, Inc. and contributors. All rights reserved.
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
use mz_expr::visit::Visit;
use mz_expr::MirRelationExpr;

/// Fuses Project operators with parent operators when possible.
#[derive(Debug)]
pub struct Project;

impl crate::Transform for Project {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "project_fusion")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let result = relation.try_visit_mut_pre(&mut |e| Ok(self.action(e)));
        mz_repr::explain_new::trace_plan(&*relation);
        result
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
    }
}
