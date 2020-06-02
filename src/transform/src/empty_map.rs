// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Remove empty `Map` operators.

use crate::{RelationExpr, TransformArgs};

/// Remove empty `Map` operators.
#[derive(Debug)]
pub struct EmptyMap;

impl crate::Transform for EmptyMap {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
        Ok(())
    }
}

impl EmptyMap {
    /// Remove empty `Map` operators.
    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Map { input, scalars } = relation {
            if scalars.is_empty() {
                *relation = input.take_dangerous();
            }
        }
    }
}
