// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::RelationExpr;
use repr::RelationType;

#[derive(Debug)]
pub struct Project;

impl crate::transform::Transform for Project {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl Project {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e, &e.typ());
        });
    }
    pub fn action(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        if let RelationExpr::Project { input, outputs } = relation {
            while let RelationExpr::Project {
                input: inner,
                outputs: outputs2,
            } = &mut **input
            {
                *outputs = outputs.iter().map(|i| outputs2[*i]).collect();
                **input = inner.take();
            }

            if outputs.is_empty() {
                *relation = input.take();
            }
        }
    }
}
