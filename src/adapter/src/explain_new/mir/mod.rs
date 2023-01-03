// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for MIR structures.

pub(crate) mod text;

use std::collections::HashMap;

use mz_compute_client::types::dataflows::DataflowDescription;
use mz_expr::{visit::Visit, Id, LocalId, MirRelationExpr, OptimizedMirRelationExpr};
use mz_ore::{stack::RecursionLimitError, str::bracketed, str::separated};
use mz_repr::explain_new::{Explain, ExplainConfig, ExplainError, UnsupportedFormat};
use mz_transform::attribute::{
    Arity, DerivedAttributes, NonNegative, RelationType, SubtreeSize, UniqueKeys,
};

use super::{
    AnnotatedPlan, Attributes, ExplainContext, ExplainMultiPlan, ExplainSinglePlan, Explainable,
};

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

        let plan = AnnotatedPlan::try_from(context, self.0)?;
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
                let plan = AnnotatedPlan::try_from(context, build_desc.plan.as_inner())?;
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

impl<'a> AnnotatedPlan<'a, MirRelationExpr> {
    fn try_from(
        context: &'a ExplainContext,
        plan: &'a MirRelationExpr,
    ) -> Result<Self, RecursionLimitError> {
        let mut annotations = HashMap::<&MirRelationExpr, Attributes>::default();
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
}

/// Normalize the way inputs of multi-input variants are rendered.
///
/// After the transform is applied, non-trival inputs `$input` of variants with
/// more than one input are wrapped in a `let $x = $input in $x` blocks.
///
/// If these blocks are subsequently pulled up by `NormalizeLets`,
/// the rendered version of the resulting tree will only have linear chains.
fn enforce_linear_chains(expr: &mut MirRelationExpr) -> Result<(), RecursionLimitError> {
    use MirRelationExpr::{Constant, Get, Join, Union};

    // helper struct: a generator of fresh local ids
    let mut id_gen = id_gen(expr)?.peekable();

    let mut wrap_in_let = |input: &mut MirRelationExpr| {
        match input {
            Constant { .. } | Get { .. } => (),
            input => {
                // generate fresh local id
                // let id = id_cnt
                //     .next()
                //     .map(|id| LocalId::new(1000_u64 + u64::cast_from(id_map.len()) + id))
                //     .unwrap();
                let id = id_gen.next().unwrap();
                let value = input.take_safely();
                // generate a `let $fresh_id = $body in $fresh_id` to replace this input
                let mut binding = MirRelationExpr::Let {
                    id,
                    value: Box::new(value),
                    body: Box::new(Get {
                        id: Id::Local(id.clone()),
                        typ: input.typ(),
                    }),
                };
                // swap the current body with the replacement
                std::mem::swap(input, &mut binding);
            }
        }
    };

    expr.try_visit_mut_post(&mut |expr: &mut MirRelationExpr| {
        match expr {
            Join { inputs, .. } => {
                for input in inputs {
                    wrap_in_let(input);
                }
            }
            Union { base, inputs } => {
                wrap_in_let(base);
                for input in inputs {
                    wrap_in_let(input);
                }
            }
            _ => (),
        }
        Ok(())
    })
}

// Create an [`Iterator`] for [`LocalId`] values that are guaranteed to be
// fresh within the scope of the given [`MirRelationExpr`].
fn id_gen(expr: &MirRelationExpr) -> Result<impl Iterator<Item = LocalId>, RecursionLimitError> {
    let mut max_id = 0_u64;

    expr.visit_post(&mut |expr| {
        match expr {
            MirRelationExpr::Let { id, .. } => max_id = std::cmp::max(max_id, id.into()),
            _ => (),
        };
    })?;

    Ok((max_id + 1..).map(LocalId::new))
}
