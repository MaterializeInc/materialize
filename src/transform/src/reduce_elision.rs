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

use crate::TransformArgs;
use itertools::Itertools;
use mz_expr::visit::Visit;
use mz_expr::MirRelationExpr;

/// Removes `Reduce` when the input has as unique keys the keys of the reduce.
#[derive(Debug)]
pub struct ReduceElision;

impl crate::Transform for ReduceElision {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.try_visit_mut_post(&mut |e| self.action(e))
    }
}

impl ReduceElision {
    /// Removes `Reduce` when the input has as unique keys the keys of the reduce.
    pub fn action(&self, relation: &mut MirRelationExpr) -> Result<(), crate::TransformError> {
        if let MirRelationExpr::Reduce {
            input,
            group_key,
            aggregates,
            monotonic: _,
            expected_group_size: _,
        } = relation
        {
            let input_type = input.typ();
            if input_type.keys.iter().any(|keys| {
                keys.iter()
                    .all(|k| group_key.contains(&mz_expr::MirScalarExpr::Column(*k)))
            }) {
                let map_scalars = aggregates
                    .iter()
                    .map(|a| a.on_unique(&input_type.column_types))
                    .collect_vec();

                let mut result = input.take_dangerous();

                // Append the group keys, then any `map_scalars`, then project
                // to put them all in the right order.
                let mut new_scalars = group_key.clone();
                new_scalars.extend(map_scalars);
                result = result.map(new_scalars).project(
                    (input_type.column_types.len()
                        ..(input_type.column_types.len() + (group_key.len() + aggregates.len())))
                        .collect(),
                );

                *relation = result;
            }
        }
        Ok(())
    }
}
