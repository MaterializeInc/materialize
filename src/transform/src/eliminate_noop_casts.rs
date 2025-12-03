// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL types distinguish between `varchar` and `varchar(n)` and `text` as
//! types, but they are all represented using `Datum::String` at runtime.
//! This transform eliminates noop casts between values of equivalent
//! representation types. s

use mz_expr::MirRelationExpr;
use mz_ore::soft_assert_or_log;

use crate::{TransformCtx, TransformError};

/// A transform that eliminates noop casts between values of equivalent representation types.
#[derive(Debug)]
pub struct EliminateNoopCasts;

impl crate::Transform for EliminateNoopCasts {
    fn name(&self) -> &'static str {
        "EliminateNoopCasts"
    }

    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        soft_assert_or_log!(
            ctx.features.enable_cast_elimination,
            "cast elimination is not enabled but the pass ran anyway"
        );

        // Descend the AST, reducing scalar expressions.
        let mut todo = vec![&mut *relation];
        while let Some(expr) = todo.pop() {
            match expr {
                MirRelationExpr::Constant { .. }
                | MirRelationExpr::Get { .. }
                | MirRelationExpr::Let { .. }
                | MirRelationExpr::LetRec { .. }
                | MirRelationExpr::Project { .. }
                | MirRelationExpr::Union { .. }
                | MirRelationExpr::Threshold { .. }
                | MirRelationExpr::Negate { .. } => {
                    // No expressions to reduce
                }
                MirRelationExpr::Map { scalars: exprs, .. }
                | MirRelationExpr::FlatMap { exprs, .. }
                | MirRelationExpr::Filter {
                    predicates: exprs, ..
                } => {
                    for e in exprs.iter_mut() {
                        e.elimimate_noop_casts().map_err(TransformError::from)?;
                    }
                }
                MirRelationExpr::Join {
                    equivalences: vecexprs,
                    ..
                }
                | MirRelationExpr::ArrangeBy { keys: vecexprs, .. } => {
                    for exprs in vecexprs.iter_mut() {
                        for e in exprs.iter_mut() {
                            e.elimimate_noop_casts().map_err(TransformError::from)?;
                        }
                    }
                }
                MirRelationExpr::Reduce {
                    group_key: exprs,
                    aggregates,
                    ..
                } => {
                    for e in exprs.iter_mut() {
                        e.elimimate_noop_casts().map_err(TransformError::from)?;
                    }

                    for agg in aggregates.iter_mut() {
                        agg.expr
                            .elimimate_noop_casts()
                            .map_err(TransformError::from)?;
                    }
                }
                MirRelationExpr::TopK { limit, .. } => {
                    if let Some(limit) = limit {
                        limit.elimimate_noop_casts().map_err(TransformError::from)?;
                    }
                }
            }

            todo.extend(expr.children_mut())
        }

        Ok(())
    }
}
