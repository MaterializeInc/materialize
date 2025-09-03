// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An Optimizer that
//! 1. Optimistically calls `optimize_mir_constant`.
//! 2. Then, if we haven't arrived at a constant, it does real optimization:
//!    - calls `prep_relation_expr` an `ExprPrepStyle` was given.
//!    - calls `optimize_mir_local`, i.e., the logical optimizer.
//!
//! This is used for `CREATE VIEW` statements and in various other situations where no physical
//! optimization is needed, such as for `INSERT` statements.
//!
//! TODO: We should split this into an optimizer that is just for views, and another optimizer
//! for various other ad hoc things, such as `INSERT`, `COPY FROM`, etc.

use std::time::Instant;

use mz_expr::OptimizedMirRelationExpr;
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::HirRelationExpr;
use mz_transform::TransformCtx;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::typecheck::{SharedContext as TypecheckContext, empty_context};

use crate::optimize::dataflows::{ExprPrepStyle, prep_relation_expr};
use crate::optimize::{
    Optimize, OptimizerConfig, OptimizerError, optimize_mir_constant, optimize_mir_local,
    trace_plan,
};

pub struct Optimizer<'a> {
    /// A typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: TypecheckContext,
    /// Optimizer config.
    config: OptimizerConfig,
    /// Optimizer metrics.
    ///
    /// Allowed to be `None` for cases where view optimization is invoked outside of the
    /// coordinator context and the metrics are not available.
    metrics: Option<OptimizerMetrics>,
    /// If present, the optimizer will call `prep_relation_expr` using the given `ExprPrepStyle`.
    expr_prep_style: Option<ExprPrepStyle<'a>>,
    /// Whether to call `FoldConstants` with a size limit, or try to fold constants of any size.
    fold_constants_limit: bool,
}

impl<'a> Optimizer<'a> {
    pub fn new(config: OptimizerConfig, metrics: Option<OptimizerMetrics>) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            config,
            metrics,
            expr_prep_style: None,
            fold_constants_limit: true,
        }
    }

    /// Creates an optimizer instance that also calls `prep_relation_expr` with the given
    /// `ExprPrepStyle`, so that unmaterializable functions are resolved.
    /// Additionally, this instance calls constant folding without a size limit.
    pub fn new_with_prep_no_limit(
        config: OptimizerConfig,
        metrics: Option<OptimizerMetrics>,
        expr_prep_style: ExprPrepStyle<'a>,
    ) -> Optimizer<'a> {
        Self {
            typecheck_ctx: empty_context(),
            config,
            metrics,
            expr_prep_style: Some(expr_prep_style),
            fold_constants_limit: false,
        }
    }
}

impl Optimize<HirRelationExpr> for Optimizer<'_> {
    type To = OptimizedMirRelationExpr;

    fn optimize(&mut self, expr: HirRelationExpr) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        // Trace the pipeline input under `optimize/raw`.
        trace_plan!(at: "raw", &expr);

        // HIR ⇒ MIR lowering and decorrelation
        let mut expr = expr.lower(&self.config, self.metrics.as_ref())?;

        let mut df_meta = DataflowMetainfo::default();
        let mut transform_ctx = TransformCtx::local(
            &self.config.features,
            &self.typecheck_ctx,
            &mut df_meta,
            self.metrics.as_ref(),
            None,
        );

        // First, we run a very simple optimizer pipeline, which only folds constants. This takes
        // care of constant INSERTs. (This optimizer is also used for INSERTs, not just VIEWs.)
        expr = optimize_mir_constant(expr, &mut transform_ctx, self.fold_constants_limit)?;

        // MIR ⇒ MIR optimization (local)
        let expr = if expr.as_const().is_some() {
            // No need to optimize further, because we already have a constant.
            // But trace this at "local", so that `EXPLAIN LOCALLY OPTIMIZED PLAN` can pick it up.
            trace_plan!(at: "local", &expr);
            OptimizedMirRelationExpr(expr)
        } else {
            // Do the real optimization (starting with `prep_relation_expr` if needed).
            if let Some(expr_prep_style) = &self.expr_prep_style {
                let mut opt_expr = OptimizedMirRelationExpr(expr);
                prep_relation_expr(&mut opt_expr, expr_prep_style.clone())?;
                expr = opt_expr.into_inner();
            }
            optimize_mir_local(expr, &mut transform_ctx)?
        };

        if let Some(metrics) = &self.metrics {
            metrics.observe_e2e_optimization_time("view", time.elapsed());
        }

        // Return the resulting OptimizedMirRelationExpr.
        Ok(expr)
    }
}
