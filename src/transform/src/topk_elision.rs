// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Remove TopK operators with both an offset of zero and no limit.

use crate::TransformArgs;
use mz_expr::visit::Visit;
use mz_expr::MirRelationExpr;

/// Remove TopK operators with both an offset of zero and no limit.
#[derive(Debug)]
pub struct TopKElision;

impl crate::Transform for TopKElision {
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
        let result = relation.try_visit_mut_post(&mut |e| self.action(e));
        mz_repr::explain_new::trace_plan(&*relation);
        result
    }
}

impl TopKElision {
    /// Remove TopK operators with both an offset of zero and no limit.
    pub fn action(&self, relation: &mut MirRelationExpr) -> Result<(), crate::TransformError> {
        if let MirRelationExpr::TopK {
            input,
            group_key: _,
            order_key: _,
            limit,
            offset,
            monotonic: _,
        } = relation
        {
            if limit.is_none() && *offset == 0 {
                *relation = input.take_dangerous();
            } else if limit == &Some(0) {
                relation.take_safely();
            }
        }
        Ok(())
    }
}
