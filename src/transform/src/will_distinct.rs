// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transform that pushes down the information that a collection will be subjected to a `Distinct` on specific columns.

use mz_expr::MirRelationExpr;

use crate::analysis::{DerivedView, NonNegative};

use crate::{TransformCtx, TransformError};

/// Pushes down the information that a collection will be subjected to a `Distinct` on specific columns.
///
/// This intends to recognize idiomatic stacks of `UNION` from SQL, which look like `Distinct` over `Union`, potentially
/// over other `Distinct` expressions. It is only able to see patterns of `DISTINCT UNION DISTINCT, ..` and other operators
/// in between will prevent the necessary insight to remove the second `DISTINCT`.
///
/// There are other potential optimizations, for example fusing multiple reads of the same source, where distinctness may
/// mean they could be a single read (with additional requirements).
#[derive(Debug, Default)]
pub struct WillDistinct;

impl crate::Transform for WillDistinct {
    #[mz_ore::instrument(
        target = "optimizer"
        level = "trace",
        fields(path.segment = "will_distinct")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        // Perform bottom-up equivalence class analysis.
        use crate::analysis::DerivedBuilder;
        let mut builder = DerivedBuilder::new(ctx.features);
        builder.require(NonNegative);
        let derived = builder.visit(relation);
        let derived = derived.as_view();

        self.apply(relation, derived);

        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl WillDistinct {
    fn apply(&self, expr: &mut MirRelationExpr, derived: DerivedView) {
        // Record a list of expressions paired with optional "will distinct by" columns.
        let mut todo = vec![(expr, derived, false)];
        while let Some((expr, derived, distinct_by)) = todo.pop() {
            // If we find a `Distinct` expression in the shadow of another `Distinct` that will apply to its key columns,
            // we can remove this `Distinct` operator as the distinctness will be enforced by the other expression.
            if let (
                MirRelationExpr::Reduce {
                    input,
                    group_key,
                    aggregates,
                    ..
                },
                true,
            ) = (&mut *expr, distinct_by)
            {
                if aggregates.is_empty() {
                    let arity = input.arity();
                    *expr = input
                        .take_dangerous()
                        .map(group_key.clone())
                        .project((arity..arity + group_key.len()).collect::<Vec<_>>());

                    // Bail out of traversal at this point, on account of we've lost track of the shape of the expression.
                } else {
                    todo.extend(
                        expr.children_mut()
                            .rev()
                            .zip(derived.children_rev())
                            .map(|(x, y)| (x, y, false)),
                    );
                }
            } else {
                match (expr, distinct_by) {
                    (
                        MirRelationExpr::Reduce {
                            input, aggregates, ..
                        },
                        _,
                    ) => {
                        if aggregates.is_empty() {
                            todo.push((input, derived.last_child(), true));
                        } else {
                            todo.push((input, derived.last_child(), false))
                        }
                    }
                    // If all inputs to the union are non-negative, any distinct enforced above the expression can be
                    // communicated on to each input.
                    (MirRelationExpr::Union { base, inputs }, will_distinct) => {
                        if derived
                            .children_rev()
                            .all(|v| *v.value::<NonNegative>().unwrap())
                        {
                            let children_rev = inputs.iter_mut().rev().chain(Some(&mut **base));
                            todo.extend(
                                children_rev
                                    .zip(derived.children_rev())
                                    .map(|(x, y)| (x, y, will_distinct)),
                            );
                        } else {
                            let children_rev = inputs.iter_mut().rev().chain(Some(&mut **base));
                            todo.extend(
                                children_rev
                                    .zip(derived.children_rev())
                                    .map(|(x, y)| (x, y, false)),
                            );
                        }
                    }
                    (x, _) => {
                        todo.extend(
                            x.children_mut()
                                .rev()
                                .zip(derived.children_rev())
                                .map(|(x, y)| (x, y, false)),
                        );
                    }
                }
            }
        }
    }
}
