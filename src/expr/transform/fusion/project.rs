// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::RelationExpr;

#[derive(Debug)]
pub struct Project;

impl crate::transform::Transform for Project {
    fn transform(&self, relation: &mut RelationExpr) {
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
    }
}
