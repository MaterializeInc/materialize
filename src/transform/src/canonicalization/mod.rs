// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformations that bring relation expressions to their canonical form.
//!
//! This is achieved  by:
//! 1. Bringing enclosed scalar expressions to a canonical form,
//! 2. Converting / peeling off part of the enclosing relation expression into
//!    another relation expression that can represent the same concept.

mod flatmap_to_map;
mod projection_extraction;
mod topk_elision;

pub use flatmap_to_map::FlatMapToMap;
pub use projection_extraction::ProjectionExtraction;
pub use topk_elision::TopKElision;

use crate::TransformCtx;
use crate::TransformError;
use mz_expr::MirRelationExpr;

/// Orders the keys in a `Reduce`.
///
/// This ordering is best thought of as a "logical" transformation, in that
/// it aims to canonicalize the representation without (yet) worrying about
/// physical properties like the order of arrangement keys (which can help
/// to avoid re-arranging data).
#[derive(Debug)]
pub struct ReduceOrdering;

impl crate::Transform for ReduceOrdering {
    #[tracing::instrument(
        target = "optimizer",
        level = "debug",
        skip_all,
        fields(path.segment = "reduce_ordering")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        // Visit looking for `Reduce` expressions with keys or aggregates not in canonical order.
        relation.visit_pre_mut(|expr| {
            if let MirRelationExpr::Reduce {
                group_key: keys,
                aggregates: aggs,
                ..
            } = expr
            {
                let old_keys = keys.clone();
                let old_aggs = aggs.clone();
                keys.sort();
                keys.dedup();
                aggs.sort();
                aggs.dedup();

                let mut projection = Vec::with_capacity(old_keys.len() + old_aggs.len());
                // Need to find each of `old_keys` in `keys`, and install a projection.
                projection.extend(
                    old_keys
                        .iter()
                        .map(|ok| keys.iter().position(|k| ok == k).unwrap()),
                );
                // Need to find each of `old_aggs` in `aggs`, and install a projection.
                projection.extend(
                    old_aggs
                        .iter()
                        .map(|oa| keys.len() + aggs.iter().position(|a| oa == a).unwrap()),
                );

                if !projection
                    .iter()
                    .cloned()
                    .eq(0..(old_keys.len() + old_aggs.len()))
                {
                    *expr = expr.take_dangerous().project(projection);
                }
            }
        });
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}
