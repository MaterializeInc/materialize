// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::{RelationExpr, ScalarExpr};
use repr::{RelationType};
use std::mem;

/// Extracts simple projections from the scalar expressions in a `Map` operator
/// into a `Project`, so they can be subjected to other optimizations.
#[derive(Debug)]
pub struct ProjectionExtraction;

impl super::Transform for ProjectionExtraction {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl ProjectionExtraction {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut(&mut |e| {
            self.action(e, &e.typ());
        });
    }

    pub fn action(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        if let RelationExpr::Map { input, scalars } = relation {
            let input_arity = input.arity();
            let mut outputs: Vec<_> = (0..input_arity).collect();
            let mut dropped = 0;
            scalars.retain(|scalar| {
                if let ScalarExpr::Column(col) = scalar.0 {
                    dropped += 1;
                    outputs.push(col);
                    false // don't retain
                } else {
                    outputs.push(outputs.len() - dropped);
                    true // retain
                }
            });
            if dropped > 0 {
                let map = relation.take();
                mem::swap(relation, &mut map.project(outputs));
            }
        }
    }
}
