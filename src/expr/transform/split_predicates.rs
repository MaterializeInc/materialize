// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::{BinaryFunc, EvalEnv, GlobalId, RelationExpr, ScalarExpr};

#[derive(Debug)]
pub struct SplitPredicates;

impl super::Transform for SplitPredicates {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        relation.visit_mut(&mut |expr| {
            if let RelationExpr::Filter { predicates, .. } = expr {
                let mut pending_predicates = predicates.drain(..).collect::<Vec<_>>();
                while let Some(expr) = pending_predicates.pop() {
                    if let ScalarExpr::CallBinary {
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
    }
}
