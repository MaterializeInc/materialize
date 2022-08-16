// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for `Mir` structures.

pub(crate) mod text;

use std::collections::HashMap;

use mz_compute_client::command::DataflowDescription;
use mz_expr::{
    visit::{Visit, Visitor},
    MirRelationExpr, OptimizedMirRelationExpr,
};
use mz_ore::stack::RecursionLimitError;
use mz_repr::explain_new::{Explain, ExplainConfig, ExplainError, UnsupportedFormat};
use mz_transform::attribute::{non_negative::NonNegative, subtree_size::SubtreeSize, Attribute};

use super::{
    AnnotatedPlan, Attributes, ExplainContext, ExplainMultiPlan, ExplainSinglePlan, Explainable,
};

impl<'a> Explain<'a> for Explainable<'a, MirRelationExpr> {
    type Context = ExplainContext<'a>;

    type Text = ExplainSinglePlan<'a, MirRelationExpr>;

    type Json = UnsupportedFormat;

    type Dot = UnsupportedFormat;

    #[allow(unused_variables)] // TODO (#13299)
    fn explain_text(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Text, ExplainError> {
        let plan = AnnotatedPlan::try_from(config, self.0)?;
        Ok(ExplainSinglePlan { context, plan })
    }
}

impl<'a> Explain<'a> for Explainable<'a, DataflowDescription<OptimizedMirRelationExpr>> {
    type Context = ExplainContext<'a>;

    type Text = ExplainMultiPlan<'a, MirRelationExpr>;

    type Json = UnsupportedFormat;

    type Dot = UnsupportedFormat;

    #[allow(unused_variables)] // TODO (#13299)
    fn explain_text(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Text, ExplainError> {
        let plans = self
            .0
            .objects_to_build
            .iter_mut()
            .rev()
            .map(|build_desc| {
                let id = context
                    .humanizer
                    .humanize_id(build_desc.id)
                    .unwrap_or_else(|| build_desc.id.to_string());
                let plan = AnnotatedPlan::try_from(config, build_desc.plan.as_inner())?;
                Ok((id, plan))
            })
            .collect::<Result<Vec<_>, RecursionLimitError>>()?;

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

struct ExplainAttributes<'a> {
    config: &'a ExplainConfig,
    non_negative: NonNegative,
    subtree_size: SubtreeSize,
}

impl<'a> From<&'a ExplainConfig> for ExplainAttributes<'a> {
    fn from(config: &'a ExplainConfig) -> ExplainAttributes<'a> {
        ExplainAttributes {
            config,
            non_negative: NonNegative::default(),
            subtree_size: SubtreeSize::default(),
        }
    }
}

impl<'a> AnnotatedPlan<'a, MirRelationExpr> {
    fn try_from(
        config: &'a ExplainConfig,
        plan: &'a MirRelationExpr,
    ) -> Result<Self, RecursionLimitError> {
        let mut annotations = HashMap::<&MirRelationExpr, Attributes>::default();

        if config.requires_attributes() {
            // get the annotation keys
            let subtree_refs = plan.post_order_vec();
            // get the annotation values
            let mut explain_attrs = ExplainAttributes::from(config);
            plan.visit(&mut explain_attrs)?;

            if config.subtree_size {
                for (expr, attr) in std::iter::zip(
                    subtree_refs.iter(),
                    explain_attrs.subtree_size.results.into_iter(),
                ) {
                    let attrs = annotations.entry(expr).or_default();
                    attrs.subtree_size = Some(attr);
                }
            }
            if config.non_negative {
                for (expr, attr) in std::iter::zip(
                    subtree_refs.iter(),
                    explain_attrs.non_negative.results.into_iter(),
                ) {
                    let attrs = annotations.entry(expr).or_default();
                    attrs.non_negative = Some(attr);
                }
            }
        }

        Ok(AnnotatedPlan { plan, annotations })
    }
}

// TODO: Model dependencies as part of the core attributes framework.
impl<'a> Visitor<MirRelationExpr> for ExplainAttributes<'a> {
    fn pre_visit(&mut self, expr: &MirRelationExpr) {
        if self.config.subtree_size || self.config.non_negative {
            self.subtree_size.schedule_env_tasks(expr);
        }
        if self.config.non_negative {
            self.non_negative.schedule_env_tasks(expr);
        }
    }

    fn post_visit(&mut self, expr: &MirRelationExpr) {
        // Derive attributes and handle environment maintenance tasks
        // in reverse dependency order.
        if self.config.subtree_size || self.config.non_negative {
            self.subtree_size.derive(expr, &());
            self.subtree_size.handle_env_tasks();
        }
        if self.config.non_negative {
            self.non_negative.derive(expr, &self.subtree_size);
            self.non_negative.handle_env_tasks();
        }
    }
}
