// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::{EvalEnv, GlobalId, Id, RelationExpr, ScalarExpr};

/// Remove TopK operators with both an offset of zero and no limit.
#[derive(Debug)]
pub struct TopKElision;

impl crate::transform::Transform for TopKElision {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) -> Result<(), super::TransformError> {
        self.transform(relation);
        Ok(())
    }
}

impl TopKElision {
    pub fn transform(&self, relation: &mut RelationExpr) {
        self.action(relation, &mut HashMap::new());
    }

    pub fn action(
        &self,
        relation: &mut RelationExpr,
        _gets: &mut HashMap<Id, (repr::RelationType, Vec<usize>)>,
    ) {
        if let RelationExpr::TopK {
            input,
            group_key: _,
            order_key: _,
            limit,
            offset,
        } = relation
        {
            if limit.is_none() && *offset == 0 {
                *relation = input.take_dangerous();
            }
        }
    }
}
