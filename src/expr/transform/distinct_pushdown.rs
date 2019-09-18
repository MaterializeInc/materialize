// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::RelationExpr;
use repr::RelationType;

/// Pushes distinct through various operators, towards their sources.
#[derive(Debug)]
pub struct DistinctPushdown;

impl super::Transform for DistinctPushdown {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl DistinctPushdown {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut(&mut |e| {
            self.action(e, &e.typ());
        });
    }
    pub fn action(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        if let RelationExpr::Distinct { input } = relation {
            match &mut **input {
                RelationExpr::Distinct { .. } => {
                    // Duplicate distincts can be suppressed.
                    *relation = input.take();
                }
                RelationExpr::Join { inputs, .. } => {
                    // We distinct the inputs to joins.
                    // This could cause us to miss on arrangement opportunities.
                    for input in inputs.iter_mut() {
                        *input = input.take().distinct();
                    }
                    *relation = input.take();
                }
                RelationExpr::Reduce { .. } => {
                    // We believe reduce operators will not output duplicates.
                    *relation = input.take();
                }
                _ => {}
            }
        }
    }
}
