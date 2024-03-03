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
use mz_sql::plan::HirRelationExpr;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};

use crate::optimize::{optimize_mir_local, trace_plan, Optimize, OptimizerConfig, OptimizerError};

pub struct Optimizer {
    /// A typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: TypecheckContext,
    // Optimizer config.
    config: OptimizerConfig,
}

impl Optimizer {
    pub fn new(config: OptimizerConfig) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            config,
        }
    }
}

impl Optimize<HirRelationExpr> for Optimizer {
    type To = OptimizedMirRelationExpr;

    fn optimize(&mut self, expr: HirRelationExpr) -> Result<Self::To, OptimizerError> {
        // Trace the pipeline input under `optimize/raw`.
        trace_plan!(at: "raw", &expr);

        // HIR ⇒ MIR lowering and decorrelation
        let expr = expr.lower(&self.config)?;

        // MIR ⇒ MIR optimization (local)
        let expr = optimize_mir_local(expr, &self.typecheck_ctx)?;

        // Return the resulting OptimizedMirRelationExpr.
        Ok(expr)
    }
}
