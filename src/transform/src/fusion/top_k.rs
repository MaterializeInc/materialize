// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuses a sequence of `TopK` operators in to one `TopK` operator

use crate::TransformArgs;
use mz_expr::visit::Visit;
use mz_expr::MirRelationExpr;

/// Fuses a sequence of `TopK` operators in to one `TopK` operator if
/// they happen to share the same grouping and ordering key.
#[derive(Debug)]
pub struct TopK;

impl crate::Transform for TopK {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "topk_fusion")
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

impl TopK {
    /// Fuses a sequence of `TopK` operators in to one `TopK` operator.
    pub fn action(&self, relation: &mut MirRelationExpr) {
        if let MirRelationExpr::TopK {
            input,
            group_key,
            order_key,
            limit,
            offset,
            monotonic,
        } = relation
        {
            while let MirRelationExpr::TopK {
                input: inner_input,
                group_key: inner_group_key,
                order_key: inner_order_key,
                limit: inner_limit,
                offset: inner_offset,
                monotonic: inner_monotonic,
            } = &mut **input
            {
                // We can fuse two chained TopK operators as long as they share the
                // same grouping and ordering key.
                if *group_key == *inner_group_key && *order_key == *inner_order_key {
                    // Given the following limit/offset pairs:
                    //
                    // inner_offset          inner_limit
                    // |------------|xxxxxxxxxxxxxxxxxx|
                    //              |------------|xxxxxxxxxxxx|
                    //              outer_offset    outer_limit
                    //
                    // the limit/offset pair of the fused TopK operator is computed
                    // as:
                    //
                    // offset = inner_offset + outer_offset
                    // limit = min(max(inner_limit - outer_offset, 0), outer_limit)
                    if let Some(inner_limit) = inner_limit {
                        let inner_limit_minus_outer_offset = inner_limit.saturating_sub(*offset);
                        if let Some(limit) = limit {
                            *limit = std::cmp::min(*limit, inner_limit_minus_outer_offset);
                        } else {
                            *limit = Some(inner_limit_minus_outer_offset);
                        }
                    }

                    if let Some(0) = limit {
                        relation.take_safely();
                        break;
                    }

                    *offset += *inner_offset;
                    *monotonic = *inner_monotonic;
                    **input = inner_input.take_dangerous();
                } else {
                    break;
                }
            }
        }
    }
}
