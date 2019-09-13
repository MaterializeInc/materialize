// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::RelationExpr;
use repr::RelationType;

/// Removes distincts under a union if the result is also made distinct.
#[derive(Debug)]
pub struct DistinctUnion;

impl super::Transform for DistinctUnion {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl DistinctUnion {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e, &e.typ());
        });
    }
    pub fn action(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        if let RelationExpr::Distinct { input } = relation {
            if let RelationExpr::Union { left, right } = &mut **input {
                if let RelationExpr::Distinct { input } = &mut **left {
                    let empty = Box::new(RelationExpr::Constant {
                        rows: vec![],
                        typ: metadata.to_owned(),
                    });
                    *left = std::mem::replace(input, empty);
                }
                if let RelationExpr::Distinct { input } = &mut **right {
                    let empty = Box::new(RelationExpr::Constant {
                        rows: vec![],
                        typ: metadata.to_owned(),
                    });
                    *right = std::mem::replace(input, empty);
                }
            }
        }
    }
}
