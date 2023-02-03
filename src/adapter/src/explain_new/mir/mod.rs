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
//! [`ExplainConfig`] options. This is the case because attribute derivation and
//! let normalization are defined in [`mz_transform`] and conssequently are not
//! available for the default [`Explain`] implementation for [`MirRelationExpr`]
//! in [`mz_expr`].

use std::collections::BTreeMap;

use mz_compute_client::types::dataflows::DataflowDescription;
use mz_expr::{
    explain_new::{enforce_linear_chains, ExplainContext, ExplainMultiPlan, ExplainSinglePlan},
    visit::Visit,
    MirRelationExpr, OptimizedMirRelationExpr,
};
use mz_ore::{stack::RecursionLimitError, str::bracketed, str::separated};
use mz_repr::explain_new::{
    AnnotatedPlan, Attributes, Explain, ExplainConfig, ExplainError, UnsupportedFormat,
};
use mz_transform::attribute::{
    Arity, DerivedAttributes, NonNegative, RelationType, SubtreeSize, UniqueKeys,
};

use super::Explainable;

impl<'a> Explain<'a> for Explainable<'a, MirRelationExpr> {
    type Context = ExplainContext<'a>;

    type Text = ExplainSinglePlan<'a, MirRelationExpr>;

    type Json = ExplainSinglePlan<'a, MirRelationExpr>;

    type Dot = UnsupportedFormat;

    fn explain_text(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Text, ExplainError> {
        self.as_explain_single_plan(config, context)
    }

    fn explain_json(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Json, ExplainError> {
        self.as_explain_single_plan(config, context)
    }
}

impl<'a> Explainable<'a, MirRelationExpr> {
    fn as_explain_single_plan(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a ExplainContext<'a>,
    ) -> Result<ExplainSinglePlan<'a, MirRelationExpr>, ExplainError> {
        // normalize the representation as linear chains
        // (this implies !config.raw_plans by construction)
        if config.linear_chains {
            enforce_linear_chains(self.0)?;
        };
        // unless raw plans are explicitly requested
        // normalize the representation of nested Let bindings
        // and enforce sequential Let binding IDs
        if !config.raw_plans {
            mz_transform::normalize_lets::normalize_lets(self.0)
                .map_err(|e| ExplainError::UnknownError(e.to_string()))?;
        }

        let plan = try_from(context, self.0)?;
        Ok(ExplainSinglePlan { context, plan })
    }
}

impl<'a> Explain<'a> for Explainable<'a, DataflowDescription<OptimizedMirRelationExpr>> {
    type Context = ExplainContext<'a>;

    type Text = ExplainMultiPlan<'a, MirRelationExpr>;

    type Json = ExplainMultiPlan<'a, MirRelationExpr>;

    type Dot = UnsupportedFormat;

    fn explain_text(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Text, ExplainError> {
        self.as_explain_multi_plan(config, context)
    }

    fn explain_json(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Text, ExplainError> {
        self.as_explain_multi_plan(config, context)
    }
}

impl<'a> Explainable<'a, DataflowDescription<OptimizedMirRelationExpr>> {
    fn as_explain_multi_plan(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a ExplainContext<'a>,
    ) -> Result<ExplainMultiPlan<'a, MirRelationExpr>, ExplainError> {
        let plans = self
            .0
            .objects_to_build
            .iter_mut()
            .rev()
            .map(|build_desc| {
                // normalize the representation as linear chains
                // (this implies !config.raw_plans by construction)
                if config.linear_chains {
                    enforce_linear_chains(build_desc.plan.as_inner_mut())?;
                };
                // unless raw plans are explicitly requested
                // normalize the representation of nested Let bindings
                // and enforce sequential Let binding IDs
                if !config.raw_plans {
                    mz_transform::normalize_lets::normalize_lets(build_desc.plan.as_inner_mut())
                        .map_err(|e| ExplainError::UnknownError(e.to_string()))?;
                }

                let id = context
                    .humanizer
                    .humanize_id(build_desc.id)
                    .unwrap_or_else(|| build_desc.id.to_string());
                let plan = try_from(context, build_desc.plan.as_inner())?;
                Ok((id, plan))
            })
            .collect::<Result<Vec<_>, ExplainError>>()?;

        let sources = self
            .0
            .source_imports
            .iter_mut()
            .filter_map(|(id, (source_desc, _))| {
                source_desc.arguments.operators.as_ref().map(|op| {
                    let id = context
                        .humanizer
                        .humanize_id(*id)
                        .unwrap_or_else(|| id.to_string());
                    (id, op)
                })
            })
            .collect::<Vec<_>>();

        Ok(ExplainMultiPlan {
            context,
            sources,
            plans,
        })
    }
}

fn try_from<'a>(
    context: &'a ExplainContext,
    plan: &'a MirRelationExpr,
) -> Result<AnnotatedPlan<'a, MirRelationExpr>, RecursionLimitError> {
    let mut annotations = BTreeMap::<&MirRelationExpr, Attributes>::default();
    let config = context.config;

    if config.requires_attributes() {
        // get the annotation keys
        let subtree_refs = plan.post_order_vec();
        // get the annotation values
        let mut explain_attrs = DerivedAttributes::from(config);
        plan.visit(&mut explain_attrs)?;

        if config.subtree_size {
            for (expr, attr) in std::iter::zip(
                subtree_refs.iter(),
                explain_attrs.remove_results::<SubtreeSize>().into_iter(),
            ) {
                let attrs = annotations.entry(expr).or_default();
                attrs.subtree_size = Some(attr);
            }
        }
        if config.non_negative {
            for (expr, attr) in std::iter::zip(
                subtree_refs.iter(),
                explain_attrs.remove_results::<NonNegative>().into_iter(),
            ) {
                let attrs = annotations.entry(expr).or_default();
                attrs.non_negative = Some(attr);
            }
        }

        if config.arity {
            for (expr, attr) in std::iter::zip(
                subtree_refs.iter(),
                explain_attrs.remove_results::<Arity>().into_iter(),
            ) {
                let attrs = annotations.entry(expr).or_default();
                attrs.arity = Some(attr);
            }
        }

        if config.types {
            for (expr, types) in std::iter::zip(
                subtree_refs.iter(),
                explain_attrs.remove_results::<RelationType>().into_iter(),
            ) {
                let humanized_columns = types
                    .into_iter()
                    .map(|c| context.humanizer.humanize_column_type(&c))
                    .collect::<Vec<_>>();
                let attr = bracketed("(", ")", separated(", ", humanized_columns)).to_string();
                let attrs = annotations.entry(expr).or_default();
                attrs.types = Some(attr);
            }
        }

        if config.keys {
            for (expr, keys) in std::iter::zip(
                subtree_refs.iter(),
                explain_attrs.remove_results::<UniqueKeys>().into_iter(),
            ) {
                let formatted_keys = keys
                    .into_iter()
                    .map(|key_set| bracketed("[", "]", separated(", ", key_set)).to_string());
                let attr = bracketed("(", ")", separated(", ", formatted_keys)).to_string();
                let attrs = annotations.entry(expr).or_default();
                attrs.keys = Some(attr);
            }
        }
    }

    Ok(AnnotatedPlan { plan, annotations })
}
