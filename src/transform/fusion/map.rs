// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuses a sequence of `Map` operators in to one `Map` operator.
//!
//! This transform introduces the complexity that max expressions can
//! refer to the results of prior map expressions. This is an important
//! detail that is often overlooked and leads to bugs. However, it is
//! important to coalesce these operators so that we can more easily
//! move them around other operators together.

use std::collections::HashMap;
use std::mem;

use expr::{GlobalId, RelationExpr, ScalarExpr};

/// Fuses a sequence of `Map` operators in to one `Map` operator.
#[derive(Debug)]
pub struct Map;

impl crate::Transform for Map {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
        Ok(())
    }
}

impl Map {
    /// Fuses a sequence of `Map` operators in to one `Map` operator.
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
