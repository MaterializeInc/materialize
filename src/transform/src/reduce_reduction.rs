// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Breaks complex `Reduce` variants into a join of simpler variants.
//!
//! Specifically, any `Reduce` that contains two different "types" of aggregation,
//! in the sense of `ReductionType`, will be broken in to one `Reduce` for each
//! type of aggregation, each containing the aggregations of that type,
//! and the results are then joined back together.

use crate::TransformCtx;
use mz_compute_types::plan::reduce::reduction_type;
use mz_expr::MirRelationExpr;

/// Breaks complex `Reduce` variants into a join of simpler variants.
#[derive(Debug)]
pub struct ReduceReduction;

impl crate::Transform for ReduceReduction {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "reduce_reduction")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        relation.visit_pre_mut(&mut Self::action);
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl ReduceReduction {
    /// Breaks complex `Reduce` variants into a join of simpler variants.
    pub fn action(relation: &mut MirRelationExpr) {
        if let MirRelationExpr::Reduce {
            input,
            group_key,
            aggregates,
            monotonic,
            expected_group_size,
        } = relation
        {
            // Segment the aggregates by reduction type.
            let mut segmented_aggregates = std::collections::BTreeMap::default();
            for (index, aggr) in aggregates.iter().enumerate() {
                let (aggrs, indexes) = segmented_aggregates
                    .entry(reduction_type(&aggr.func))
                    .or_insert_with(|| (Vec::default(), Vec::default()));
                indexes.push(group_key.len() + index);
                aggrs.push(aggr.clone());
            }

            // Do nothing unless there are at least two distinct types of aggregations.
            if segmented_aggregates.len() < 2 {
                return;
            }

            // For each type of aggregation we'll plan the corresponding `Reduce`,
            // and then join the at-least-two `Reduce` stages together.
            // TODO: Perhaps we should introduce a `Let` stage rather than clone the input?
            let mut reduces = Vec::with_capacity(segmented_aggregates.keys().count());
            // Track the current and intended locations of each output column.
            let mut columns = Vec::new();

            for (_aggr_type, (aggrs, indexes)) in segmented_aggregates {
                columns.extend(0..group_key.len());
                columns.extend(indexes);

                reduces.push(MirRelationExpr::Reduce {
                    input: input.clone(),
                    group_key: group_key.clone(),
                    aggregates: aggrs,
                    monotonic: *monotonic,
                    expected_group_size: *expected_group_size,
                });
            }

            // Now build a `Join` of the reduces, on their keys, followed by a permutation of their aggregates.
            // Equate all `group_key` columns in all inputs.
            let mut equivalences = vec![Vec::with_capacity(reduces.len()); group_key.len()];
            for column in 0..group_key.len() {
                for input in 0..reduces.len() {
                    equivalences[column].push((input, column));
                }
            }

            // Determine projection that puts aggregate columns in their intended locations,
            // and projects away repeated key columns.
            let max_column = columns.iter().max().expect("Non-empty aggregates expected");
            let mut projection = Vec::with_capacity(max_column + 1);
            for column in 0..max_column + 1 {
                projection.push(columns.iter().position(|c| *c == column).unwrap())
            }

            // Now make the join.
            *relation = MirRelationExpr::join(reduces, equivalences).project(projection);
        }
    }
}
