// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `CREATE VIEW` statements.

use std::time::Instant;

use mz_expr::{OptimizedMirRelationExpr, StatisticsOracle};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::HirRelationExpr;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use mz_transform::TransformCtx;

use crate::optimize::{optimize_mir_local, trace_plan, Optimize, OptimizerConfig, OptimizerError};

pub struct Optimizer {
    /// A typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: TypecheckContext,
    /// Optimizer config.
    config: OptimizerConfig,
    /// Optimizer metrics.
    ///
    /// Allowed to be `None` for cases where view optimization is invoked outside of the
    /// coordinator context and the metrics are not available.
    metrics: Option<OptimizerMetrics>,
}

impl Optimizer {
    pub fn new(config: OptimizerConfig, metrics: Option<OptimizerMetrics>) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            config,
            metrics,
        }
    }
}

impl Optimize<HirRelationExpr> for Optimizer {
    type To = OptimizedMirRelationExpr;

    fn optimize(&mut self, expr: HirRelationExpr) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        // Trace the pipeline input under `optimize/raw`.
        trace_plan!(at: "raw", &expr);

        // HIR ⇒ MIR lowering and decorrelation
        let expr = expr.lower(&self.config, self.metrics.as_ref())?;

        // MIR ⇒ MIR optimization (local)
        let stats = StatisticsOracle::default();
        let mut df_meta = DataflowMetainfo::default();
        let mut transform_ctx = TransformCtx::local(
            &self.config.features,
            &self.typecheck_ctx,
            &stats,
            &mut df_meta,
        );
        let expr = optimize_mir_local(expr, &mut transform_ctx)?;

        if let Some(metrics) = &self.metrics {
            metrics.observe_e2e_optimization_time("view", time.elapsed());
        }

        // Return the resulting OptimizedMirRelationExpr.
        Ok(expr)
    }
}
