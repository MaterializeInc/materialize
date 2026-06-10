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

use mz_expr::MirRelationExpr;

use crate::TransformCtx;

/// Fuses Project operators with parent operators when possible.
#[derive(Debug)]
pub struct Project;

impl crate::Transform for Project {
    fn name(&self) -> &'static str {
        "ProjectFusion"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "project_fusion")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        _: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        relation.visit_pre_mut(Self::action);
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl Project {
    /// Fuses Project operators with parent operators when possible.
    pub fn action(relation: &mut MirRelationExpr) {
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
