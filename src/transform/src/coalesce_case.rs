// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Turn `COALESCE` of `NULL`-returning `CASE` into just a `CASE`

use mz_expr::func::variadic::Coalesce;
use mz_expr::visit::Visit;
use mz_expr::{AggregateExpr, JoinImplementation, MirRelationExpr, MirScalarExpr, VariadicFunc};
use mz_ore::soft_assert_eq_or_log;

use crate::{TransformCtx, TransformError};

/// Replace operators on constant collections with constant collections.
#[derive(Debug)]
pub struct CoalesceCase {}

impl Default for CoalesceCase {
    fn default() -> Self {
        Self {}
    }
}

impl crate::Transform for CoalesceCase {
    fn name(&self) -> &'static str {
        "FoldConstants"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "fold_constants")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        _: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        relation.try_visit_mut_post(&mut |e| self.action(e))?;
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl CoalesceCase {
    fn action(&self, relation: &mut MirRelationExpr) -> Result<(), TransformError> {
        match relation {
            MirRelationExpr::Constant { .. } |
            MirRelationExpr::Get { .. } |
            MirRelationExpr::Let { .. } |
            MirRelationExpr::LetRec { .. } |
            MirRelationExpr::Project { .. } |
            MirRelationExpr::Negate { .. } |
            MirRelationExpr::Threshold { .. } |
            MirRelationExpr::Union { .. } |
            // don't mess with arrangements, even though their keys have MSEs in them
            // these _mostly_ shouldn't occur, since we run before join implementation, but some may be inserted earlier for us
            MirRelationExpr::ArrangeBy { .. } => (),
            MirRelationExpr::Map { scalars: exprs, .. } |
            MirRelationExpr::Filter { predicates: exprs, .. } |
            MirRelationExpr::FlatMap { exprs, .. } => {
                // NB TableFunc doesn't ever hold an MSE
                for expr in exprs.iter_mut() {
                    self.rewrite_scalar(expr)?;
                }
            }
            MirRelationExpr::Join { equivalences, implementation, .. } => {
                if implementation.is_implemented() {
                    soft_assert_eq_or_log!(*implementation, JoinImplementation::Unimplemented, "unexpected implemented Join when optimizing coalesce/case, skipping");
                    return Ok(());
                }

                for equivalence in equivalences.iter_mut() {
                    for expr in equivalence {
                        self.rewrite_scalar(expr)?;
                    }
                }
            }
            MirRelationExpr::Reduce { group_key, aggregates, .. } => {
                for expr in group_key.iter_mut() {
                    self.rewrite_scalar(expr)?;
                }

                for agg in aggregates.iter_mut() {
                    self.rewrite_aggreagte(agg)?;
                }
            }
            MirRelationExpr::TopK { limit, .. } => {
                if let Some(expr) = limit {
                    self.rewrite_scalar(expr)?;
                }
            }
        }

        Ok(())
    }

    fn rewrite_scalar(&self, expr: &mut MirScalarExpr) -> Result<(), TransformError> {
        expr.visit_mut_post(&mut |e| self.try_combine_coalesce_case(e))
            .map_err(TransformError::from)
    }

    fn rewrite_aggreagte(&self, agg: &mut AggregateExpr) -> Result<(), TransformError> {
        // NB AggregateFunc doesn't contain any MSEs
        self.rewrite_scalar(&mut agg.expr)
    }

    fn try_combine_coalesce_case(&self, expr: &mut MirScalarExpr) {
        // COALESCE(CASE WHEN e_cond THEN NULL ELSE e_else END, ...)
        // ->
        // CASE WHEN e_cond THEN COALESCE(...) ELSE e_else END
        //
        // ... and flipped
        expr.flatten_associative();

        if let MirScalarExpr::CallVariadic { func, exprs } = expr
            && *func == VariadicFunc::Coalesce(Coalesce)
        {
            if let MirScalarExpr::If { cond: _, then, els } = &exprs[0] {
                if then.is_literal_null() {
                    let MirScalarExpr::If {
                        mut cond,
                        then: _,
                        mut els,
                    } = exprs.remove(0)
                    else {
                        unreachable!();
                    };
                    *expr = MirScalarExpr::if_then_else(
                        cond.take(),
                        MirScalarExpr::CallVariadic {
                            func: VariadicFunc::Coalesce(Coalesce),
                            exprs: std::mem::take(exprs),
                        },
                        els.take(),
                    );
                } else if els.is_literal_null() {
                    let MirScalarExpr::If {
                        mut cond,
                        mut then,
                        els: _,
                    } = exprs.remove(0)
                    else {
                        unreachable!();
                    };
                    *expr = MirScalarExpr::if_then_else(
                        cond.take(),
                        then.take(),
                        MirScalarExpr::CallVariadic {
                            func: VariadicFunc::Coalesce(Coalesce),
                            exprs: std::mem::take(exprs),
                        },
                    );
                }
            }
        }
    }
}
