// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::RelationExpr;
use repr::RelationType;

#[derive(Debug)]
pub struct EmptyMap;

impl super::Transform for EmptyMap {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl EmptyMap {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e, &e.typ());
        });
    }
    pub fn action(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        if let RelationExpr::Map { input, scalars } = relation {
            if scalars.is_empty() {
                let empty = RelationExpr::Constant {
                    rows: vec![],
                    typ: metadata.to_owned(),
                };
                *relation = std::mem::replace(input, empty);
            }
        }
    }
}
