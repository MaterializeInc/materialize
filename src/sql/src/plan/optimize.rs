// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

///! This module defines the API and logic for running optimization pipelines.
use crate::plan::expr::HirRelationExpr;
use crate::query_model::{Model, QGMError};

use super::StatementContext;

/// Feature flags for the [`HirRelationExpr::optimize_and_lower()`] logic.
#[derive(Debug)]
pub struct OptimizerConfig {
    pub qgm_optimizations: bool,
}

/// Convert a reference to a [`StatementContext`] to an [`OptimizerConfig`].
///
/// This picks up feature flag values such as `qgm_optimizations` from the `PlanContext` if this is present in
/// the [`StatementContext`], otherwise uses sensible defaults.
impl<'a> From<&StatementContext<'a>> for OptimizerConfig {
    fn from(scx: &StatementContext) -> Self {
        match scx.pcx() {
            Ok(pcx) => OptimizerConfig {
                qgm_optimizations: pcx.qgm_optimizations,
            },
            Err(..) => OptimizerConfig {
                qgm_optimizations: false,
            },
        }
    }
}

impl HirRelationExpr {
    /// Perform optimizing algebraic rewrites on this [`HirRelationExpr`] and lower it to a [`mz_expr::MirRelationExpr`].
    ///
    /// The optimization path is fully-determined by the values of the feature flag defined in the [`OptimizerConfig`].
    pub fn optimize_and_lower(
        self,
        config: &OptimizerConfig,
    ) -> Result<mz_expr::MirRelationExpr, QGMError> {
        if config.qgm_optimizations {
            // try to go through the QGM path
            self.try_qgm_path()
        } else {
            // directly decorrelate and lower into a MirRelationExpr
            Ok(self.lower())
        }
    }

    /// Attempt an optimization path from HIR to MIR that goes through a QGM representation.
    ///
    /// Return `Result::Err` if the path is not possible.
    fn try_qgm_path(self) -> Result<mz_expr::MirRelationExpr, QGMError> {
        // create a query graph model from this HirRelationExpr
        let mut model = Model::try_from(self)?;

        // perform optimizing algebraic rewrites on the qgm
        model.optimize();

        // decorrelate and lower the optimized query graph model into a MirRelationExpr
        model.try_into()
    }
}
