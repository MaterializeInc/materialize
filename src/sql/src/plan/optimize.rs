// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module defines the API and logic for running optimization pipelines.

use crate::plan::expr::HirRelationExpr;
use crate::plan::{PlanError, StatementContext};

/// Feature flags for the [`HirRelationExpr::optimize_and_lower()`] logic.
#[derive(Debug)]
pub struct OptimizerConfig {
    // TODO: add parameters driving the optimization pass here
}

/// Convert a reference to a [`StatementContext`] to an [`OptimizerConfig`].
impl<'a> From<&StatementContext<'a>> for OptimizerConfig {
    fn from(_scx: &StatementContext) -> Self {
        OptimizerConfig {}
    }
}

impl HirRelationExpr {
    /// Perform optimizing algebraic rewrites on this [`HirRelationExpr`] and lower it to a [`mz_expr::MirRelationExpr`].
    ///
    /// The optimization path is fully-determined by the values of the feature flag defined in the [`OptimizerConfig`].
    pub fn optimize_and_lower(
        self,
        _config: &OptimizerConfig,
    ) -> Result<mz_expr::MirRelationExpr, PlanError> {
        self.lower()
    }
}
