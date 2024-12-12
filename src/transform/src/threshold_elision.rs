// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Remove Threshold operators when we are certain no records have negative multiplicity.
//!
//! If we have Threshold(A - Subset(A)) and we believe that A has no negative multiplicities,
//! then we can replace this with A - Subset(A).
//!
//! The Subset(X) notation means that the collection is a multiset subset of X:
//! multiplicities of each record in Subset(X) are at most that of X.

use mz_expr::MirRelationExpr;

use crate::analysis::{DerivedBuilder, NonNegative, SubtreeSize};
use crate::TransformCtx;

/// Remove Threshold operators that have no effect.
#[derive(Debug)]
pub struct ThresholdElision;

impl crate::Transform for ThresholdElision {
    fn name(&self) -> &'static str {
        "ThresholdElision"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "threshold_elision")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        let mut builder = DerivedBuilder::new(ctx.features);
        builder.require(NonNegative);
        builder.require(SubtreeSize);
        let derived = builder.visit(&*relation);

        // Descend the AST, removing `Threshold` operators whose inputs are non-negative.
        let mut todo = vec![(&mut *relation, derived.as_view())];
        while let Some((expr, mut view)) = todo.pop() {
            if let MirRelationExpr::Threshold { input } = expr {
                if *view
                    .last_child()
                    .value::<NonNegative>()
                    .expect("NonNegative required")
                {
                    *expr = input.take_dangerous();
                    view = view.last_child();
                }
            }
            todo.extend(expr.children_mut().rev().zip(view.children_rev()))
        }

        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}
