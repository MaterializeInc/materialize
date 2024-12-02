// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for MIR structures.
//!
//! The specialized [`Explain`] implementation for an [`MirRelationExpr`]
//! wrapped in an [`Explainable`] newtype struct allows us to interpret more
//! [`mz_repr::explain::ExplainConfig`] options. This is the case because
//! Analysis derivation and Let normalization are defined in [`mz_transform`]
//! and consequently are not available for the default [`Explain`]
//! implementation for [`MirRelationExpr`] in [`mz_expr`].

use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::explain::export_ids_for;
use mz_expr::explain::{
    enforce_linear_chains, ExplainContext, ExplainMultiPlan, ExplainSinglePlan, ExplainSource,
};
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
use mz_repr::explain::{Explain, ExplainError, UnsupportedFormat};
use mz_transform::analysis::annotate_plan;
use mz_transform::normalize_lets::normalize_lets;

use crate::explain::Explainable;

impl<'a> Explain<'a> for Explainable<'a, MirRelationExpr> {
    type Context = ExplainContext<'a>;

    type Text = ExplainSinglePlan<'a, MirRelationExpr>;

    type Json = ExplainSinglePlan<'a, MirRelationExpr>;

    type Dot = UnsupportedFormat;

    fn explain_text(&'a mut self, context: &'a Self::Context) -> Result<Self::Text, ExplainError> {
        self.as_explain_single_plan(context)
    }

    fn explain_json(&'a mut self, context: &'a Self::Context) -> Result<Self::Json, ExplainError> {
        self.as_explain_single_plan(context)
    }
}

impl<'a> Explainable<'a, MirRelationExpr> {
    fn as_explain_single_plan(
        &'a mut self,
        context: &'a ExplainContext<'a>,
    ) -> Result<ExplainSinglePlan<'a, MirRelationExpr>, ExplainError> {
        // normalize the representation as linear chains
        // (this implies !context.config.raw_plans by construction)
        if context.config.linear_chains {
            enforce_linear_chains(self.0)?;
        };
        // unless raw plans are explicitly requested
        // normalize the representation of nested Let bindings
        // and enforce sequential Let binding IDs
        if !context.config.raw_plans {
            normalize_lets(self.0, context.features)
                .map_err(|e| ExplainError::UnknownError(e.to_string()))?;
        }

        Ok(ExplainSinglePlan {
            context,
            plan: annotate_plan(self.0, context)?,
        })
    }
}

impl<'a> Explain<'a> for Explainable<'a, DataflowDescription<OptimizedMirRelationExpr>> {
    type Context = ExplainContext<'a>;

    type Text = ExplainMultiPlan<'a, MirRelationExpr>;

    type Json = ExplainMultiPlan<'a, MirRelationExpr>;

    type Dot = UnsupportedFormat;

    fn explain_text(&'a mut self, context: &'a Self::Context) -> Result<Self::Text, ExplainError> {
        self.as_explain_multi_plan(context)
    }

    fn explain_json(&'a mut self, context: &'a Self::Context) -> Result<Self::Text, ExplainError> {
        self.as_explain_multi_plan(context)
    }
}

impl<'a> Explainable<'a, DataflowDescription<OptimizedMirRelationExpr>> {
    fn as_explain_multi_plan(
        &'a mut self,
        context: &'a ExplainContext<'a>,
    ) -> Result<ExplainMultiPlan<'a, MirRelationExpr>, ExplainError> {
        let export_ids = export_ids_for(self.0);
        let plans = self
            .0
            .objects_to_build
            .iter_mut()
            .rev()
            .map(|build_desc| {
                let plan = build_desc.plan.as_inner_mut();

                // normalize the representation as linear chains
                // (this implies !context.config.raw_plans by construction)
                if context.config.linear_chains {
                    enforce_linear_chains(plan)?;
                };
                // unless raw plans are explicitly requested
                // normalize the representation of nested Let bindings
                // and enforce sequential Let binding IDs
                if !context.config.raw_plans {
                    normalize_lets(plan, context.features)
                        .map_err(|e| ExplainError::UnknownError(e.to_string()))?;
                }

                let public_id = export_ids
                    .get(&build_desc.id)
                    .unwrap_or(&build_desc.id)
                    .clone();
                let id = context
                    .humanizer
                    .humanize_id(public_id)
                    .unwrap_or_else(|| public_id.to_string());

                Ok((id, annotate_plan(plan, context)?))
            })
            .collect::<Result<Vec<_>, ExplainError>>()?;

        let sources = self
            .0
            .source_imports
            .iter_mut()
            .map(|(id, (source_desc, _))| {
                let op = source_desc.arguments.operators.as_ref();
                ExplainSource::new(*id, op, context.config.filter_pushdown)
            })
            .collect::<Vec<_>>();

        Ok(ExplainMultiPlan {
            context,
            sources,
            plans,
        })
    }
}
