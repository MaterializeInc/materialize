// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Pushes `COALESCE` into `CASE WHEN`

use mz_expr::func::variadic::Coalesce;
use mz_expr::visit::Visit;
use mz_expr::{AggregateExpr, MirRelationExpr, MirScalarExpr, VariadicFunc};
use mz_ore::soft_panic_or_log;

use crate::{TransformCtx, TransformError};

/// Push `COALESCE` into `CASE WHEN` as possible.
#[derive(Debug)]
pub struct CoalesceCase;

impl Default for CoalesceCase {
    fn default() -> Self {
        Self {}
    }
}

impl crate::Transform for CoalesceCase {
    fn name(&self) -> &'static str {
        "CoalesceCase"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "coalesce_case")
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
                    soft_panic_or_log!("unexpected implemented Join when optimizing coalesce/case, skipping: {implementation:?}");
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
        // Visiting in pre-order means that when we push a `COALESCE` down, we'll keep pushing if the `CASE` chain continues.
        expr.visit_mut_pre(&mut |e| self.try_combine_coalesce_case(e))
            .map_err(TransformError::from)
    }

    fn rewrite_aggreagte(&self, agg: &mut AggregateExpr) -> Result<(), TransformError> {
        // NB AggregateFunc doesn't contain any MSEs
        self.rewrite_scalar(&mut agg.expr)
    }

    fn try_combine_coalesce_case(&self, expr: &mut MirScalarExpr) {
        // COALESCE(CASE WHEN e_cond THEN e_then ELSE e_else END, ...)
        // ->
        // CASE WHEN e_cond THEN COALESCE(e_then, ...) ELSE COALESCE(e_else, ...) END
        expr.flatten_associative();

        if let MirScalarExpr::CallVariadic { func, exprs } = expr
            && *func == VariadicFunc::Coalesce(Coalesce)
        {
            if let MirScalarExpr::If { .. } = &exprs[0] {
                let mut exprs = std::mem::take(exprs);
                if let MirScalarExpr::If {
                    mut cond,
                    mut then,
                    mut els,
                } = exprs.remove(0)
                {
                    let cond = cond.take();

                    let mut then_exprs = Vec::with_capacity(exprs.len() + 1);
                    then_exprs.push(then.take());
                    then_exprs.extend(exprs.iter().cloned());

                    let mut else_exprs = Vec::with_capacity(exprs.len() + 1);
                    else_exprs.push(els.take());
                    else_exprs.append(&mut exprs);

                    let t =
                        MirScalarExpr::call_variadic(VariadicFunc::Coalesce(Coalesce), then_exprs);
                    let f =
                        MirScalarExpr::call_variadic(VariadicFunc::Coalesce(Coalesce), else_exprs);
                    *expr = cond.if_then_else(t, f);
                } else {
                    unreachable!();
                };
            }
        }
    }
}
