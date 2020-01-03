// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

// Using references ensures code doesn't break if the other argument
// ceases to be Copy.
#[allow(clippy::op_ref)]
use crate::{EvalEnv, RelationExpr};

#[derive(Debug)]
pub struct ReductionPushdown;

impl super::Transform for ReductionPushdown {
    fn transform(&self, relation: &mut RelationExpr, env: &EvalEnv) {
        self.transform(relation, env)
    }
}

impl ReductionPushdown {
    pub fn transform(&self, relation: &mut RelationExpr, env: &EvalEnv) {
        relation.visit_mut(&mut |e| {
            self.action(e, env);
        });
    }

    pub fn action(&self, relation: &mut RelationExpr, _env: &EvalEnv) {
        if let RelationExpr::Reduce {
            input,
            group_key,
            aggregates,
        } = relation
        {
            // Map expressions can possibly be absorbed into the Reduce at no cost.
            if let RelationExpr::Map {
                input: inner,
                scalars,
            } = &mut **input
            {
                if scalars.iter().all(|e| e.is_literal()) {
                    let arity = inner.arity();
                    let keys_check = group_key.iter().all(|k| k < &arity);
                    let agg_check = aggregates
                        .iter()
                        .all(|e| e.expr.support().iter().all(|c| c < &arity));
                    if keys_check && agg_check {
                        **input = inner.take_dangerous();
                    }
                }
            } else if let RelationExpr::Join { .. } = &**input {
                // println!("ReducePushdown opportunity: {}", relation.pretty());
            }
        }
    }
}
