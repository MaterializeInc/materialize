// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::{EvalEnv, RelationExpr};

#[derive(Debug)]
pub struct EmptyMap;

impl super::Transform for EmptyMap {
    fn transform(&self, relation: &mut RelationExpr, _: &EvalEnv) {
        self.transform(relation)
    }
}

impl EmptyMap {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
    }
    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Map { input, scalars } = relation {
            if scalars.is_empty() {
                *relation = input.take_dangerous();
            }
        }
    }
}
