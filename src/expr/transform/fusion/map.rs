// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::mem;

use crate::{EvalEnv, GlobalId, RelationExpr, ScalarExpr};

#[derive(Debug)]
pub struct Map;

impl crate::transform::Transform for Map {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation)
    }
}

impl Map {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
    }
    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Map { input, scalars } = relation {
            while let RelationExpr::Map {
                input: inner_input,
                scalars: inner_scalars,
            } = &mut **input
            {
                inner_scalars.append(scalars);
                mem::swap(scalars, inner_scalars);
                **input = inner_input.take_dangerous();
            }
        }
    }
}
