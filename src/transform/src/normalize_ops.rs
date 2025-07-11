// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Normalize the structure of various operators.

use mz_expr::MirRelationExpr;
use mz_expr::visit::Visit;

use crate::TransformCtx;

/// Normalize the structure of various operators.
#[derive(Debug)]
pub struct NormalizeOps;

impl crate::Transform for NormalizeOps {
    fn name(&self) -> &'static str {
        "NormalizeOps"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "normalize_ops")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        _ctx: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        // Canonicalize and fuse various operators as a bottom-up transforms.
        relation.try_visit_mut_post::<_, crate::TransformError>(
            &mut |expr: &mut MirRelationExpr| {
                // (a) Might enable fusion in the next step.
                crate::canonicalization::FlatMapToMap::action(expr);
                crate::canonicalization::TopKElision::action(expr);
                // (b) Fuse various like-kinded operators. Might enable further canonicalization.
                crate::fusion::Fusion::action(expr);
                // (c) Fuse join trees (might lift in-between MFPs).
                crate::fusion::join::Join::action(expr)?;
                // (d) Extract column references in Map as Project.
                crate::canonicalization::ProjectionExtraction::action(expr);
                // Done!
                Ok(())
            },
        )?;
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}
