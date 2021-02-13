// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transforms predicates of the form "A and B" into two: "A" and "B".

use crate::TransformArgs;
use expr::{BinaryFunc, MirRelationExpr, MirScalarExpr};

/// Transforms predicates of the form "A and B" into two: "A" and "B".
#[derive(Debug)]
pub struct SplitPredicates;

impl crate::Transform for SplitPredicates {
    /// Transforms predicates of the form "A and B" into two: "A" and "B".
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut(&mut |expr| {
            if let MirRelationExpr::Filter { predicates, .. } = expr {
                let mut pending_predicates = predicates.drain(..).collect::<Vec<_>>();
                while let Some(expr) = pending_predicates.pop() {
                    if let MirScalarExpr::CallBinary {
                        func: BinaryFunc::And,
                        expr1,
                        expr2,
                    } = expr
                    {
                        pending_predicates.push(*expr1);
                        pending_predicates.push(*expr2);
                    } else {
                        predicates.push(expr);
                    }
                }
            }
        });
        Ok(())
    }
}
