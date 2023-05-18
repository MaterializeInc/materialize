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

use crate::TransformArgs;

/// Remove TopK operators with both an offset of zero and no limit.
#[derive(Debug)]
pub struct TopKElision;

impl crate::Transform for TopKElision {
    fn recursion_safe(&self) -> bool {
        true
    }

    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "topk_elision")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
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
            if limit.is_none() && *offset == 0 {
                *relation = input.take_dangerous();
            } else if limit == &Some(0) {
                relation.take_safely();
            }
        }
    }
}
