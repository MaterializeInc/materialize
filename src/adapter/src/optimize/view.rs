// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `CREATE VIEW` statements.

use mz_expr::OptimizedMirRelationExpr;
use mz_repr::explain::trace_plan;
use mz_sql::plan::HirRelationExpr;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use mz_transform::Optimizer as TransformOptimizer;
use tracing::{span, Level};

use crate::optimize::{Optimize, OptimizerConfig, OptimizerError};

pub struct OptimizeView {
    /// A typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: TypecheckContext,
    // Optimizer config.
    config: OptimizerConfig,
}

impl OptimizeView {
    pub fn new(config: OptimizerConfig) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            config,
        }
    }
}

impl Optimize<HirRelationExpr> for OptimizeView {
    type To = OptimizedMirRelationExpr;

    fn optimize(&mut self, expr: HirRelationExpr) -> Result<Self::To, OptimizerError> {
        // HIR ⇒ MIR lowering and decorrelation
        let expr = expr.lower(&self.config)?;

        // MIR ⇒ MIR optimization (local)
        let expr = span!(target: "optimizer", Level::DEBUG, "local").in_scope(|| {
            let optimizer = TransformOptimizer::logical_optimizer(&self.typecheck_ctx);
            let expr = optimizer.optimize(expr)?;

            // Trace the result of this phase.
            trace_plan(expr.as_inner());

            Ok::<_, OptimizerError>(expr)
        })?;

        // Return the resulting OptimizedMirRelationExpr.
        Ok(expr)
    }
}
