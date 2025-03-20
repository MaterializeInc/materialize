// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Remove TopK operators with both an offset of zero and no limit.

use mz_expr::visit::Visit;
use mz_expr::MirRelationExpr;

use crate::TransformCtx;

/// Remove TopK operators with both an offset of zero and no limit.
#[derive(Debug)]
pub struct TopKElision;

impl crate::Transform for TopKElision {
    fn name(&self) -> &'static str {
        "TopKElision"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "topk_elision")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        _: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut_post(&mut Self::action)?;
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl TopKElision {
    /// Remove TopK operators with both an offset of zero and no limit.
    pub fn action(relation: &mut MirRelationExpr) {
        if let MirRelationExpr::TopK {
            input,
            group_key: _,
            order_key: _,
            limit,
            offset,
            monotonic: _,
            expected_group_size: _,
        } = relation
        {
            // The limit is not set if it either `None` or literal `Null`.
            if limit.as_ref().map_or(true, |l| l.is_literal_null()) && *offset == 0 {
                *relation = input.take_dangerous();
            } else if limit.as_ref().and_then(|l| l.as_literal_int64()) == Some(0) {
                relation.take_safely(None);
            }
        }
    }
}
