// Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::collections::HashMap;

use crate::{EvalEnv, GlobalId, RelationExpr, ScalarExpr};

#[derive(Debug)]
pub struct Project;

impl crate::transform::Transform for Project {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation)
    }
}

impl Project {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
    }
    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Project { input, outputs } = relation {
            while let RelationExpr::Project {
                input: inner,
                outputs: outputs2,
            } = &mut **input
            {
                *outputs = outputs.iter().map(|i| outputs2[*i]).collect();
                **input = inner.take_dangerous();
            }
            if outputs.iter().enumerate().all(|(a, b)| a == *b) && outputs.len() == input.arity() {
                *relation = input.take_dangerous();
            }
        }

        // Any reduce will absorb any project. Also, this happens often.
        if let RelationExpr::Reduce {
            input,
            group_key,
            aggregates,
        } = relation
        {
            if let RelationExpr::Project {
                input: inner,
                outputs,
            } = &mut **input
            {
                // Rewrite the group key using `inner` columns.
                for key in group_key.iter_mut() {
                    key.permute(&outputs[..]);
                }
                for aggregate in aggregates.iter_mut() {
                    aggregate.expr.permute(&outputs[..]);
                }
                *input = Box::new(inner.take_dangerous());
            }
        }
    }
}
