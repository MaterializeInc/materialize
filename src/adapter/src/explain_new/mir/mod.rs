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
use std::iter::once;

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
        // unless raw plans are explicitly requested
        // normalize the representation of nested Let bindings
        if !config.raw_plans {
            normalize_lets_in_tree(self.0)?;
        }

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
                // unless raw plans are explicitly requested
                // normalize the representation of nested Let bindings
                if !config.raw_plans {
                    normalize_lets_in_tree(build_desc.plan.as_inner_mut())?;
                }

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

/// Normalizes occurrences of nested `Let` bindings in the entire tree
/// rooted at the given `expr`.
///
/// This just applies [`normalize_lets_at_root`] bottom up once.
fn normalize_lets_in_tree<'a>(expr: &'a mut MirRelationExpr) -> Result<(), RecursionLimitError> {
    expr.visit_mut_post(&mut |expr: &mut MirRelationExpr| {
        let mut normalized_root = normalize_lets_at_root(expr.take_dangerous());
        std::mem::swap(expr, &mut normalized_root);
    })
}

/// Normalizes occurrences of nested `Let` bindings at the root of the
/// given `expr`.
///
/// The general form of a `Let` binding is
/// ```text
/// let
///   ${id} = ${value}
/// in
///   ${body}
/// ```
/// Let ⟦-〛 denote a translated expression.
/// There are three basic cases for implementing ⟦-〛 that depend
/// on the surrounding context of a matched `Let` binding.
///
/// ## Case 1: An inner `Let` appearing in a relational operator `op`
/// Rewrite
/// ```text
/// 〚 op(
///     ${prefix},
///     let
///       ${id} = ${value}
///     in
///       ${body},
///     ${suffix}
///   ) 〛
/// ```
/// as
/// ```text
/// let
///   ${id} = ${value}
/// in
///   ⟦ op(${prefix}, ${body}, ${suffix}) 〛
/// ```
///
/// ## Case 2: An inner `Let` appearing in the `value` of an enclosing outer `Let`
/// Rewrite
/// ```text
/// 〚 let
///     ${id_1} =
///       let
///         ${id_2} = ${value_2}
///       in
///         ${body_2}
///   in
///     ${body_1} 〛
/// ```
/// as
/// ```text
/// let
///   ${id_2} = ${value_2}
/// in
///   〚 let
///       ${id_1} = ${body_2}
///     in
///       ${body_1} 〛
/// ```
///
/// ## Case 3: An inner `Let` appearing in the `body` of an enclosing outer `Let`
/// ```text
/// 〚 let
///     ${id_1} = ${value_1}
///   in
///     let
///       ${lhs_2} = ${value_2}
///     in
///       ${body_2} 〛
/// ```
/// Do nothing.
fn normalize_lets_at_root(expr: MirRelationExpr) -> MirRelationExpr {
    use MirRelationExpr::*;
    match expr {
        // base cases: do nothing
        result @ Constant { rows: _, typ: _ } => result,
        result @ Get { id: _, typ: _ } => result,
        Let {
            id: id_1,
            value: value_1,
            body: body_1,
        } => {
            // Case 2
            if let Let {
                id: id_2,
                value: value_2,
                body: body_2,
            } = *value_1
            {
                Let {
                    id: id_2,
                    value: value_2,
                    body: Box::new(normalize_lets_at_root(Let {
                        id: id_1,
                        value: body_2,
                        body: body_1,
                    })),
                }
            } else {
                Let {
                    id: id_1,
                    value: value_1,
                    body: body_1,
                }
            }
        }
        // Case 1
        Project { input, outputs } => {
            if let Let { id, value, body } = *input {
                Let {
                    id,
                    value,
                    body: Box::new(normalize_lets_at_root(Project {
                        input: body,
                        outputs,
                    })),
                }
            } else {
                Project { input, outputs }
            }
        }
        Map { input, scalars } => {
            if let Let { id, value, body } = *input {
                Let {
                    id,
                    value,
                    body: Box::new(normalize_lets_at_root(Map {
                        input: body,
                        scalars,
                    })),
                }
            } else {
                Map { input, scalars }
            }
        }
        FlatMap { input, func, exprs } => {
            if let Let { id, value, body } = *input {
                Let {
                    id,
                    value,
                    body: Box::new(normalize_lets_at_root(FlatMap {
                        input: body,
                        func,
                        exprs,
                    })),
                }
            } else {
                FlatMap { input, func, exprs }
            }
        }
        Filter { input, predicates } => {
            if let Let { id, value, body } = *input {
                Let {
                    id,
                    value,
                    body: Box::new(normalize_lets_at_root(Filter {
                        input: body,
                        predicates,
                    })),
                }
            } else {
                Filter { input, predicates }
            }
        }
        Join {
            inputs,
            equivalences,
            implementation,
        } => {
            let mut bindings = vec![];

            let inputs = inputs
                .into_iter()
                .map(|input| {
                    if let Let { id, value, body } = input {
                        bindings.push((id, value));
                        *body
                    } else {
                        input
                    }
                })
                .collect::<Vec<_>>();

            let mut result = Join {
                inputs,
                equivalences,
                implementation,
            };
            for (id, value) in bindings.drain(..).rev() {
                result = Let {
                    id,
                    value,
                    body: Box::new(normalize_lets_at_root(result)),
                };
            }

            result
        }
        Reduce {
            input,
            group_key,
            aggregates,
            monotonic,
            expected_group_size,
        } => {
            if let Let { id, value, body } = *input {
                Let {
                    id,
                    value,
                    body: Box::new(normalize_lets_at_root(Reduce {
                        input: body,
                        group_key,
                        aggregates,
                        monotonic,
                        expected_group_size,
                    })),
                }
            } else {
                Reduce {
                    input,
                    group_key,
                    aggregates,
                    monotonic,
                    expected_group_size,
                }
            }
        }
        TopK {
            input,
            group_key,
            order_key,
            limit,
            offset,
            monotonic,
        } => {
            if let Let { id, value, body } = *input {
                Let {
                    id,
                    value,
                    body: Box::new(normalize_lets_at_root(TopK {
                        input: body,
                        group_key,
                        order_key,
                        limit,
                        offset,
                        monotonic,
                    })),
                }
            } else {
                TopK {
                    input,
                    group_key,
                    order_key,
                    limit,
                    offset,
                    monotonic,
                }
            }
        }
        Negate { input } => {
            if let Let { id, value, body } = *input {
                Let {
                    id,
                    value,
                    body: Box::new(normalize_lets_at_root(Negate { input: body })),
                }
            } else {
                Negate { input }
            }
        }
        Threshold { input } => {
            if let Let { id, value, body } = *input {
                Let {
                    id,
                    value,
                    body: Box::new(normalize_lets_at_root(Threshold { input: body })),
                }
            } else {
                Threshold { input }
            }
        }
        Union { base, inputs } => {
            let mut bindings = vec![];

            let mut inputs = once(*base)
                .chain(inputs)
                .into_iter()
                .map(|input| {
                    if let Let { id, value, body } = input {
                        bindings.push((id, value));
                        *body
                    } else {
                        input
                    }
                })
                .collect::<Vec<_>>();
            let base = Box::new(inputs.remove(0));

            let mut result = Union { base, inputs };
            for (id, value) in bindings.drain(..).rev() {
                result = Let {
                    id,
                    value,
                    body: Box::new(normalize_lets_at_root(result)),
                };
            }

            result
        }
        ArrangeBy { input, keys } => {
            if let Let { id, value, body } = *input {
                Let {
                    id,
                    value,
                    body: Box::new(normalize_lets_at_root(ArrangeBy { input: body, keys })),
                }
            } else {
                ArrangeBy { input, keys }
            }
        }
    }
}
