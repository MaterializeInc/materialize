// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for structures defined in this crate.

pub(crate) mod text;

use std::collections::BTreeMap;

use mz_expr::explain::{enforce_linear_chains, ExplainContext, ExplainMultiPlan, ExplainSource};
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
use mz_repr::explain::{AnnotatedPlan, Explain, ExplainError, UnsupportedFormat};

use crate::plan::Plan;
use crate::types::dataflows::DataflowDescription;

impl<'a> Explain<'a> for DataflowDescription<Plan> {
    type Context = ExplainContext<'a>;

    type Text = ExplainMultiPlan<'a, Plan>;

    type Json = ExplainMultiPlan<'a, Plan>;

    type Dot = UnsupportedFormat;

    fn explain_text(&'a mut self, context: &'a Self::Context) -> Result<Self::Text, ExplainError> {
        self.as_explain_multi_plan(context)
    }

    fn explain_json(&'a mut self, context: &'a Self::Context) -> Result<Self::Text, ExplainError> {
        self.as_explain_multi_plan(context)
    }
}

impl<'a> DataflowDescription<Plan> {
    fn as_explain_multi_plan(
        &'a mut self,
        context: &'a ExplainContext<'a>,
    ) -> Result<ExplainMultiPlan<'a, Plan>, ExplainError> {
        let plans = self
            .objects_to_build
            .iter_mut()
            .rev()
            .map(|build_desc| {
                let id = context
                    .humanizer
                    .humanize_id(build_desc.id)
                    .unwrap_or_else(|| build_desc.id.to_string());
                let plan = AnnotatedPlan {
                    plan: &build_desc.plan,
                    annotations: BTreeMap::default(),
                };
                (id, plan)
            })
            .collect::<Vec<_>>();

        let sources = self
            .source_imports
            .iter_mut()
            .filter_map(|(id, (source_desc, _))| {
                source_desc.arguments.operators.as_ref().map(|op| {
                    let id = context
                        .humanizer
                        .humanize_id(*id)
                        .unwrap_or_else(|| id.to_string());
                    ExplainSource::new(id, op, context)
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

impl<'a> Explain<'a> for DataflowDescription<OptimizedMirRelationExpr> {
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

impl<'a> DataflowDescription<OptimizedMirRelationExpr> {
    fn as_explain_multi_plan(
        &'a mut self,
        context: &'a ExplainContext<'a>,
    ) -> Result<ExplainMultiPlan<'a, MirRelationExpr>, ExplainError> {
        let plans = self
            .objects_to_build
            .iter_mut()
            .rev()
            .map(|build_desc| {
                // normalize the representation as linear chains
                // (this implies !context.config.raw_plans by construction)
                if context.config.linear_chains {
                    enforce_linear_chains(build_desc.plan.as_inner_mut())?;
                };

                let id = context
                    .humanizer
                    .humanize_id(build_desc.id)
                    .unwrap_or_else(|| build_desc.id.to_string());
                let plan = AnnotatedPlan {
                    plan: build_desc.plan.as_inner(),
                    annotations: BTreeMap::default(),
                };
                Ok((id, plan))
            })
            .collect::<Result<Vec<_>, ExplainError>>()?;

        let sources = self
            .source_imports
            .iter_mut()
            .filter_map(|(id, (source_desc, _))| {
                source_desc.arguments.operators.as_ref().map(|op| {
                    let id = context
                        .humanizer
                        .humanize_id(*id)
                        .unwrap_or_else(|| id.to_string());
                    ExplainSource::new(id, op, context)
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
