// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Removes `Reduce` when the input has as unique keys the keys of the reduce.
//!
//! When a reduce has grouping keys that are contained within a
//! set of columns that form unique keys for the input, the reduce
//! can be simplified to a map operation.

use itertools::Itertools;
use mz_expr::MirRelationExpr;

use crate::TransformCtx;
use crate::analysis::{DerivedBuilder, DerivedView};
use crate::analysis::{SqlRelationType, UniqueKeys};

/// Removes `Reduce` when the input has as unique keys the keys of the reduce.
#[derive(Debug)]
pub struct ReduceElision;

impl crate::Transform for ReduceElision {
    fn name(&self) -> &'static str {
        "ReduceElision"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "reduce_elision")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        // Assemble type information once for the whole expression.
        let mut builder = DerivedBuilder::new(ctx.features);
        builder.require(SqlRelationType);
        builder.require(UniqueKeys);
        let derived = builder.visit(relation);
        let derived_view = derived.as_view();

        self.action(relation, derived_view);

        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl ReduceElision {
    /// Removes `Reduce` when the input has as unique keys the keys of the reduce.
    pub fn action(&self, relation: &mut MirRelationExpr, derived: DerivedView) {
        let mut todo = vec![(relation, derived)];
        while let Some((expr, view)) = todo.pop() {
            let mut replaced = false;
            if let MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                monotonic: _,
                expected_group_size: _,
            } = expr
            {
                let input_type = view
                    .last_child()
                    .value::<SqlRelationType>()
                    .expect("SqlRelationType required")
                    .as_ref()
                    .expect("Expression not well-typed");
                let input_keys = view
                    .last_child()
                    .value::<UniqueKeys>()
                    .expect("UniqueKeys required");

                if input_keys.iter().any(|keys| {
                    keys.iter()
                        .all(|k| group_key.iter().any(|gk| gk.as_column() == Some(*k)))
                }) {
                    let map_scalars = aggregates
                        .iter()
                        .map(|a| a.on_unique(input_type))
                        .collect_vec();

                    let mut result = input.take_dangerous();

                    let input_arity = input_type.len();

                    // Append the group keys, then any `map_scalars`, then project
                    // to put them all in the right order.
                    let mut new_scalars = group_key.clone();
                    new_scalars.extend(map_scalars);
                    result = result.map(new_scalars).project(
                        (input_arity..(input_arity + (group_key.len() + aggregates.len())))
                            .collect(),
                    );

                    *expr = result;
                    replaced = true;

                    // // NB: The following is borked because of smart builders.
                    // if let MirRelationExpr::Project { input, .. } = expr {
                    //     if let MirRelationExpr::Map { input, .. } = &mut **input {
                    //         todo.push((&mut **input, view.last_child()))
                    //     }
                    // }
                }
            }

            // This gets around an awkward borrow of both `expr` and `input` above.
            if !replaced {
                todo.extend(expr.children_mut().rev().zip(view.children_rev()));
            }
        }
    }
}
