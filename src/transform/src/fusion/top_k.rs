// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::TransformArgs;
use expr::MirRelationExpr;

/// Fuses a sequence of `TopK` operators in to one `TopK` operator.
#[derive(Debug)]
pub struct TopK;

impl crate::Transform for TopK {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
        Ok(())
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
                if *group_key == *inner_group_key && *order_key == *inner_order_key {
                    if let Some(inner_limit) = inner_limit {
                        if *offset >= *inner_limit {
                            relation.take_safely();
                            break;
                        }
                        *inner_limit -= *offset;
                        if let Some(limit) = limit {
                            *limit = std::cmp::min(*limit, *inner_limit);
                        } else {
                            *limit = Some(*inner_limit);
                        }
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
