// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Preliminary work on reduction push-down.
//!
//! At the moment, this only absorbs Map operators into Reduce operators.

use crate::TransformArgs;
use expr::MirRelationExpr;

/// Pushes Reduce operators toward sources.
#[derive(Debug)]
pub struct ReductionPushdown;

impl crate::Transform for ReductionPushdown {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.try_visit_mut_post(&mut |e| self.action(e))
    }
}

impl ReductionPushdown {
    /// Pushes Reduce operators toward sources.
    pub fn action(&self, relation: &mut MirRelationExpr) -> Result<(), crate::TransformError> {
        if let MirRelationExpr::Reduce {
            input,
            group_key,
            aggregates,
            monotonic: _,
            expected_group_size: _,
        } = relation
        {
            // Map expressions can be absorbed into the Reduce at no cost.
            if let MirRelationExpr::Map {
                input: inner,
                scalars,
            } = &mut **input
            {
                let arity = inner.arity();

                // Normalize the scalars to not be self-referential.
                let mut scalars = scalars.clone();
                for index in 0..scalars.len() {
                    let (lower, upper) = scalars.split_at_mut(index);
                    upper[0].visit_mut(&mut |e| {
                        if let expr::MirScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = lower[*c - arity].clone();
                            }
                        }
                    });
                }
                for key in group_key.iter_mut() {
                    key.visit_mut(&mut |e| {
                        if let expr::MirScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = scalars[*c - arity].clone();
                            }
                        }
                    });
                }
                for agg in aggregates.iter_mut() {
                    agg.expr.visit_mut(&mut |e| {
                        if let expr::MirScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = scalars[*c - arity].clone();
                            }
                        }
                    });
                }

                **input = inner.take_dangerous()
            }
        }
        Ok(())
    }
}
