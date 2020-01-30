// Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

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
