// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Canonicalize types in MRE by converting them to repr types and back.
//!
//! This will not be a long-lived transform---we will eliminate it once
//! MRE tracks repr types internally.

use mz_expr::MirRelationExpr;
use mz_expr::visit::Visit;

use crate::{TransformCtx, TransformError};

/// Canonicalize types in MRE by converting them to repr types and back.
#[derive(Debug)]
pub struct ReprizeSqlTypes;

impl crate::Transform for ReprizeSqlTypes {
    fn name(&self) -> &'static str {
        "reprize-sql-types"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "reprize_sql_types")
    )]
    fn actually_perform_transform(
        &self,
        expr: &mut MirRelationExpr,
        _ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        expr.visit_mut_post(&mut |expr| {
            self.repr_canonicalize_relation(expr);
        })
        .map_err(TransformError::from)?;
        mz_repr::explain::trace_plan(&*expr);
        Ok(())
    }
}

impl ReprizeSqlTypes {
    fn repr_canonicalize_relation(&self, expr: &mut MirRelationExpr) {
        use MirRelationExpr::*;
        match expr {
            // no nested scalar expressions
            Let { .. }
            | LetRec { .. }
            | Project { .. }
            | Negate { .. }
            | Threshold { .. }
            | Union { .. } => (),
            // stored types
            Constant { typ, .. } | Get { typ, .. } => {
                typ.repr_canonicalize();
            }
            // a single vector of scalar expressions
            Map {
                scalars: scalar_exprs,
                ..
            }
            | Filter {
                predicates: scalar_exprs,
                ..
            } => {
                for scalar_expr in scalar_exprs.iter_mut() {
                    scalar_expr.repr_canonicalize();
                }
            }
            // custom business
            FlatMap { func, exprs, .. } => {
                func.repr_canonicalize();
                for scalar_expr in exprs.iter_mut() {
                    scalar_expr.repr_canonicalize();
                }
            }
            Join {
                equivalences,
                implementation,
                ..
            } => {
                for exprs in equivalences.iter_mut() {
                    for expr in exprs.iter_mut() {
                        expr.repr_canonicalize();
                    }
                }
                implementation.repr_canonicalize();
            }
            Reduce {
                group_key,
                aggregates,
                ..
            } => {
                for key in group_key.iter_mut() {
                    key.repr_canonicalize();
                }
                for aggregate in aggregates.iter_mut() {
                    aggregate.repr_canonicalize();
                }
            }
            TopK { limit, .. } => {
                if let Some(limit) = limit {
                    limit.repr_canonicalize();
                }
            }
            ArrangeBy { keys, .. } => {
                for exprs in keys.iter_mut() {
                    for expr in exprs.iter_mut() {
                        expr.repr_canonicalize();
                    }
                }
            }
        }
    }
}
