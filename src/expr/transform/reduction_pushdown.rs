// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::collections::HashMap;

use crate::{EvalEnv, GlobalId, RelationExpr, ScalarExpr};

#[derive(Debug)]
pub struct ReductionPushdown;

impl super::Transform for ReductionPushdown {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        env: &EvalEnv,
    ) {
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
            // Map expressions can be absorbed into the Reduce at no cost.
            if let RelationExpr::Map {
                input: inner,
                scalars,
            } = &mut **input
            {
                let arity = inner.arity();

                // Normalize the scalars to not be self-referential.
                let mut scalars = scalars.clone();
                for index in 0..scalars.len() {
                    let (lower, upper) = scalars.split_at_mut(index);
                    upper[0].visit_mut(&mut |e| {
                        if let crate::ScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = lower[*c - arity].clone();
                            }
                        }
                    });
                }
                for key in group_key.iter_mut() {
                    key.visit_mut(&mut |e| {
                        if let crate::ScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = scalars[*c - arity].clone();
                            }
                        }
                    });
                }
                for agg in aggregates.iter_mut() {
                    agg.expr.visit_mut(&mut |e| {
                        if let crate::ScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = scalars[*c - arity].clone();
                            }
                        }
                    });
                }

                **input = inner.take_dangerous()
            }
        }
    }
}
