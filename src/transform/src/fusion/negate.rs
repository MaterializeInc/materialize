// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuses a sequence of `Negate` operators in to one or zero `Negate` operators.

use crate::TransformArgs;
use expr::MirRelationExpr;

/// Fuses a sequence of `Negate` operators in to one or zero `Negate` operators.
#[derive(Debug)]
pub struct Negate;

impl crate::Transform for Negate {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
        Ok(())
    }
}

impl Negate {
    /// Fuses a sequence of `Negate` operators in to one or zero `Negate` operators.
    pub fn action(&self, relation: &mut MirRelationExpr) {
        if let MirRelationExpr::Negate { input } = relation {
            let mut require_negate = true;
            while let MirRelationExpr::Negate { input: inner_input } = &mut **input {
                **input = inner_input.take_dangerous();
                require_negate = !require_negate;
            }

            if !require_negate {
                *relation = input.take_dangerous();
            }
        }
    }
}
