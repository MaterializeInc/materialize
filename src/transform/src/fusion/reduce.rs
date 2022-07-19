// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuses reduce operators with parent operators if possible.
use crate::{TransformArgs, TransformError};
use mz_expr::visit::Visit;
use mz_expr::{MirRelationExpr, MirScalarExpr};

/// Fuses reduce operators with parent operators if possible.
#[derive(Debug)]
pub struct Reduce;

impl crate::Transform for Reduce {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), TransformError> {
        relation.try_visit_mut_pre(&mut |e| self.action(e))
    }
}

impl Reduce {
    /// Fuses reduce operators with parent operators if possible.
    pub fn action(&self, relation: &mut MirRelationExpr) -> Result<(), TransformError> {
        if let MirRelationExpr::Reduce {
            input,
            group_key,
            aggregates,
            monotonic: _,
            expected_group_size: _,
        } = relation
        {
            if let MirRelationExpr::Reduce {
                input: inner_input,
                group_key: inner_group_key,
                aggregates: inner_aggregates,
                monotonic: _,
                expected_group_size: _,
            } = &mut **input
            {
                // Collect all columns referenced by outer
                let mut outer_cols = vec![];
                for expr in group_key.iter() {
                    expr.visit_post(&mut |e| {
                        if let MirScalarExpr::Column(i) = e {
                            outer_cols.push(*i);
                        }
                    })?;
                }

                // We can fuse reduce operators as long as the outer one doesn't
                // group by an aggregation performed by the inner one.
                if outer_cols.iter().any(|c| *c >= inner_group_key.len()) {
                    return Ok(());
                }

                if aggregates.is_empty() && inner_aggregates.is_empty() {
                    // Replace inner reduce with map + project (no grouping)
                    let mut outputs = vec![];
                    let mut scalars = vec![];

                    let arity = inner_input.arity();
                    for e in inner_group_key {
                        if let MirScalarExpr::Column(i) = e {
                            outputs.push(*i);
                        } else {
                            outputs.push(arity + scalars.len());
                            scalars.push(e.clone());
                        }
                    }

                    **input = inner_input.take_dangerous().map(scalars).project(outputs);
                }
            }
        }
        Ok(())
    }
}
