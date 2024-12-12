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
    fn name(&self) -> &'static str {
        "ReduceReduction"
    }

    /// Transforms an expression through accumulated knowledge.
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "reduce_reduction")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        if ctx.features.enable_reduce_reduction {
            relation.visit_pre_mut(&mut Self::action);
            mz_repr::explain::trace_plan(&*relation);
        }
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
            // We start by segmenting the aggregates into those that should be rendered independently.
            // Each element of this list is a pair of lists describing a bundle of aggregations that
            // should be applied independently. Each pair of lists correspond to the aggregaties and
            // the column positions in which they should appear in the output.
            // Perhaps these should be lists of pairs, to ensure they align, but their subsequent use
            // is as the shredded lists.
            let mut segmented_aggregates: Vec<(Vec<mz_expr::AggregateExpr>, Vec<usize>)> =
                Vec::new();

            // Our rendering currently produces independent dataflow paths for 1. all accumulable aggregations,
            // 2. all hierarchical aggregations, and 3. *each* basic aggregation.
            // We'll form groups for accumulable, hierarchical, and a list of basic aggregates.
            let mut accumulable = (Vec::new(), Vec::new());
            let mut hierarchical = (Vec::new(), Vec::new());

            use mz_compute_types::plan::reduce::ReductionType;
            for (index, aggr) in aggregates.iter().enumerate() {
                match reduction_type(&aggr.func) {
                    ReductionType::Accumulable => {
                        accumulable.0.push(aggr.clone());
                        accumulable.1.push(group_key.len() + index);
                    }
                    ReductionType::Hierarchical => {
                        hierarchical.0.push(aggr.clone());
                        hierarchical.1.push(group_key.len() + index);
                    }
                    ReductionType::Basic => segmented_aggregates
                        .push((vec![aggr.clone()], vec![group_key.len() + index])),
                }
            }

            // Fold in hierarchical and accumulable aggregates.
            if !hierarchical.0.is_empty() {
                segmented_aggregates.push(hierarchical);
            }
            if !accumulable.0.is_empty() {
                segmented_aggregates.push(accumulable);
            }
            segmented_aggregates.sort();

            // Do nothing unless there are at least two distinct types of aggregations.
            if segmented_aggregates.len() < 2 {
                return;
            }

            // For each type of aggregation we'll plan the corresponding `Reduce`,
            // and then join the at-least-two `Reduce` stages together.
            // TODO: Perhaps we should introduce a `Let` stage rather than clone the input?
            let mut reduces = Vec::with_capacity(segmented_aggregates.len());
            // Track the current and intended locations of each output column.
            let mut columns = Vec::new();

            for (aggrs, indexes) in segmented_aggregates {
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
