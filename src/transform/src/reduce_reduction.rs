// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Breaks complex `Reduce` variants into a join of simpler variants.

use crate::TransformArgs;
use mz_compute_client::plan::reduce::{reduction_type, ReductionType};
use mz_expr::MirRelationExpr;

/// Fuses multiple `Filter` operators into one and deduplicates predicates.
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
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.visit_pre_mut(&mut Self::action);
        mz_repr::explain_new::trace_plan(&*relation);
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
            // Do nothing if `aggregates` contains 1. nothing, 2. a single aggregate, or 3. only accumulable aggregates.
            // Otherwise, rip apart `aggregates` into accumulable aggregates and each other aggregate.
            let mut accumulable = Vec::new();
            let mut all_others = Vec::new();
            for (index, aggregate) in aggregates.iter().enumerate() {
                if reduction_type(&aggregate.func) == ReductionType::Accumulable {
                    accumulable.push((index, aggregate));
                } else {
                    all_others.push((index, aggregate));
                }
            }

            // We only leap in to action if there are things to break apart.
            if all_others.len() > 1 || (!accumulable.is_empty() && !all_others.is_empty()) {
                let mut reduces = Vec::new();
                for (_index, aggr) in all_others.iter() {
                    reduces.push(MirRelationExpr::Reduce {
                        input: input.clone(),
                        group_key: group_key.clone(),
                        aggregates: vec![(*aggr).clone()],
                        monotonic: *monotonic,
                        expected_group_size: *expected_group_size,
                    });
                }
                if !accumulable.is_empty() {
                    reduces.push(MirRelationExpr::Reduce {
                        input: input.clone(),
                        group_key: group_key.clone(),
                        aggregates: accumulable
                            .iter()
                            .map(|(_index, aggr)| (*aggr).clone())
                            .collect(),
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
                // Project the group key and then each of the aggregates in order.
                let mut projection =
                    Vec::with_capacity(group_key.len() + all_others.len() + accumulable.len());
                projection.extend(0..group_key.len());
                let mut accumulable_count = 0;
                let mut all_others_count = 0;
                for aggr in aggregates.iter() {
                    if reduction_type(&aggr.func) == ReductionType::Accumulable {
                        projection.push(
                            (group_key.len() + 1) * all_others.len()
                                + group_key.len()
                                + accumulable_count,
                        );
                        accumulable_count += 1;
                    } else {
                        projection.push((group_key.len() + 1) * all_others_count + group_key.len());
                        all_others_count += 1;
                    }
                }
                // Now make the join.
                *relation = MirRelationExpr::join(reduces, equivalences).project(projection);
            }
        }
    }
}
