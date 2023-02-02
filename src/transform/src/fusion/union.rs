// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuses multiple `Union` operators into one.
//!
//! Nested negated unions are merged into the parent one by pushing
//! the Negate to all their branches.

use mz_expr::visit::Visit;
use mz_expr::MirRelationExpr;

/// Fuses multiple `Union` operators into one.
#[derive(Debug)]
pub struct Union;

impl crate::Transform for Union {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "union")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: crate::TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut_post(&mut Self::action)?;
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl Union {
    /// Fuses multiple `Union` operators into one.
    ///
    /// The order among children is maintained, and the action should be idempotent.
    /// This action works best if other operators such as `Negate` and other linear
    /// operators are pushed down through other `Union` operators.
    pub fn action(relation: &mut MirRelationExpr) {
        if let MirRelationExpr::Union { inputs, .. } = relation {
            let mut list: Vec<MirRelationExpr> = Vec::with_capacity(1 + inputs.len());
            Self::unfold_unions_into(relation.take_dangerous(), &mut list);
            *relation = MirRelationExpr::Union {
                base: Box::new(list.remove(0)),
                inputs: list,
            }
        }
    }

    /// Unfolds `self` and children into a list of expressions that can be unioned.
    fn unfold_unions_into(expr: MirRelationExpr, list: &mut Vec<MirRelationExpr>) {
        let mut stack = vec![expr];
        while let Some(expr) = stack.pop() {
            if let MirRelationExpr::Union { base, inputs } = expr {
                stack.extend(inputs.into_iter().rev());
                stack.push(*base);
            } else {
                list.push(expr);
            }
        }
    }
}
