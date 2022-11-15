// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_expr::visit::{Visit, VisitChildren};

///! This module defines the API and logic for running optimization pipelines.
use crate::plan::expr::HirRelationExpr;
use crate::plan::HirScalarExpr;
use crate::query_model::{Model, QGMError};

use super::{PlanError, StatementContext};

/// Feature flags for the [`HirRelationExpr::optimize_and_lower()`] logic.
#[derive(Debug)]
pub struct OptimizerConfig {
    pub qgm_optimizations: bool,
    pub window_functions: bool,
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
                window_functions: scx.catalog.window_functions(),
            },
            Err(..) => OptimizerConfig {
                qgm_optimizations: false,
                window_functions: scx.catalog.window_functions(),
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
    ) -> Result<mz_expr::MirRelationExpr, PlanError> {
        if !config.window_functions {
            // None - there are no Get(GlobalId) nodes / Some - all Get(GlobalId) nodes are for system IDs
            let mut all_system_global_ids = None;
            // there are no windowing expressions
            let mut has_windowing_exprs = false;

            // look for Get(GlobalId) or HirScalarExpr::Windowig in the entire tree
            self.try_visit_pre(&mut |expr: &HirRelationExpr| {
                use mz_expr::Id::Global;
                // update all_global_ids_system accumulator for each encountered Get(GlobalId) node
                if let HirRelationExpr::Get { id: Global(id), .. } = expr {
                    let v = all_system_global_ids.get_or_insert(true);
                    *v &= id.is_system();
                }
                // update has_windowing_exprs accumulator for each encountered HirScalarExpr::Windowig node
                expr.try_visit_children(|expr: &HirScalarExpr| {
                    expr.visit_pre(&mut |expr: &HirScalarExpr| {
                        if let HirScalarExpr::Windowing(_) = expr {
                            has_windowing_exprs = true;
                        }
                    })
                })
            })?;

            if has_windowing_exprs && !all_system_global_ids.unwrap_or(false) {
                bail_unsupported!("window functions");
            }
        }

        if config.qgm_optimizations {
            // try to go through the QGM path
            self.try_qgm_path().map_err(Into::into)
        } else {
            // directly decorrelate and lower into a MirRelationExpr
            Ok(self.lower())
        }
    }

    /// Attempt an optimization path from HIR to MIR that goes through a QGM representation.
    ///
    /// Return `Result::Err` if the path is not possible.
    #[tracing::instrument(target = "optimizer", level = "debug", name = "qgm", skip_all)]
    fn try_qgm_path(self) -> Result<mz_expr::MirRelationExpr, QGMError> {
        // create a query graph model from this HirRelationExpr
        let mut model = Model::try_from(self)?;

        tracing::span!(tracing::Level::INFO, "raw").in_scope(|| {
            // TODO: this requires to implement Clone for Model
            // mz_repr::explain_new::trace_plan(model);
        });

        // perform optimizing algebraic rewrites on the qgm
        model.optimize();

        tracing::span!(tracing::Level::INFO, "raw").in_scope(|| {
            // TODO: this requires to implement Clone for Model
            // mz_repr::explain_new::trace_plan(model);
        });

        // decorrelate and lower the optimized query graph model into a MirRelationExpr
        model.try_into()
    }
}
