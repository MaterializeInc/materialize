// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Analysis to identify monotonic collections, especially TopK inputs.

use std::collections::BTreeSet;

use mz_expr::MirRelationExpr;
use mz_repr::GlobalId;

use crate::analysis::monotonic::Monotonic;
use crate::analysis::DerivedBuilder;
use crate::TransformCtx;

/// A struct to apply expression optimizations based on the [`Monotonic`] analysis.
#[derive(Debug, Default)]
pub struct MonotonicFlag;

impl MonotonicFlag {
    /// Transforms `expr` by propagating information about monotonicity through operators,
    /// using [`mz_repr::optimize::OptimizerFeatures`] from `ctx` and global monotonic ids.
    pub fn transform(
        &self,
        expr: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
        global_monotonic_ids: &BTreeSet<GlobalId>,
    ) -> Result<(), crate::RecursionLimitError> {
        let mut builder = DerivedBuilder::new(ctx.features);
        builder.require(Monotonic::new(global_monotonic_ids.clone()));
        let derived = builder.visit(&*expr);

        let mut todo = vec![(&mut *expr, derived.as_view())];
        while let Some((expr, view)) = todo.pop() {
            match expr {
                MirRelationExpr::Reduce { monotonic, .. }
                | MirRelationExpr::TopK { monotonic, .. } => {
                    *monotonic = *view
                        .last_child()
                        .value::<Monotonic>()
                        .expect("Monotonic required");
                }
                _ => {}
            }
            todo.extend(expr.children_mut().rev().zip(view.children_rev()))
        }

        mz_repr::explain::trace_plan(&*expr);
        Ok(())
    }
}
