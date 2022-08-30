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
use mz_ore::{stack::RecursionLimitError, str::bracketed, str::separated};
use mz_repr::explain_new::{Explain, ExplainConfig, ExplainError, UnsupportedFormat};
use mz_transform::attribute::{
    arity::Arity, non_negative::NonNegative, relation_type::RelationType,
    subtree_size::SubtreeSize, unique_keys::UniqueKeys, AttributeBuilder, AttributeDeriver,
};

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

        let plan = AnnotatedPlan::try_from(context, self.0)?;
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
                let plan = AnnotatedPlan::try_from(context, build_desc.plan.as_inner())?;
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
struct ExplainAttributes {
    deriver: AttributeDeriver,
}

impl From<&ExplainConfig> for ExplainAttributes {
    fn from(config: &ExplainConfig) -> ExplainAttributes {
        let mut builder = AttributeBuilder::default();
        if config.subtree_size {
            builder.add_attribute::<SubtreeSize>();
        }
        if config.non_negative {
            builder.add_attribute::<NonNegative>();
        }
        if config.types {
            builder.add_attribute::<RelationType>();
        }
        if config.arity {
            builder.add_attribute::<Arity>();
        }
        if config.keys {
            builder.add_attribute::<UniqueKeys>();
        }
        ExplainAttributes {
            deriver: builder.finish(),
        }
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
            let mut explain_attrs = ExplainAttributes::from(config);
            plan.visit(&mut explain_attrs)?;
            let mut attribute_map = explain_attrs.deriver.take();

            if config.subtree_size {
                for (expr, attr) in std::iter::zip(
                    subtree_refs.iter(),
                    attribute_map
                        .remove::<SubtreeSize>()
                        .unwrap()
                        .results
                        .into_iter(),
                ) {
                    let attrs = annotations.entry(expr).or_default();
                    attrs.subtree_size = Some(attr);
                }
            }
            if config.non_negative {
                for (expr, attr) in std::iter::zip(
                    subtree_refs.iter(),
                    attribute_map
                        .remove::<NonNegative>()
                        .unwrap()
                        .results
                        .into_iter(),
                ) {
                    let attrs = annotations.entry(expr).or_default();
                    attrs.non_negative = Some(attr);
                }
            }

            if config.arity {
                for (expr, attr) in std::iter::zip(
                    subtree_refs.iter(),
                    attribute_map.remove::<Arity>().unwrap().results.into_iter(),
                ) {
                    let attrs = annotations.entry(expr).or_default();
                    attrs.arity = Some(attr);
                }
            }

            if config.types {
                for (expr, types) in std::iter::zip(
                    subtree_refs.iter(),
                    attribute_map
                        .remove::<RelationType>()
                        .unwrap()
                        .results
                        .into_iter(),
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
                    attribute_map
                        .remove::<UniqueKeys>()
                        .unwrap()
                        .results
                        .into_iter(),
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

// TODO: Model dependencies as part of the core attributes framework.
impl Visitor<MirRelationExpr> for ExplainAttributes {
    fn pre_visit(&mut self, expr: &MirRelationExpr) {
        self.deriver.pre_visit(expr);
    }

    fn post_visit(&mut self, expr: &MirRelationExpr) {
        self.deriver.post_visit(expr);
    }
}

/// Normalizes occurrences of nested `Let` bindings in the entire tree
/// rooted at the given `expr`.
///
/// This just applies [`normalize_lets_at_root`] bottom up once.
fn normalize_lets_in_tree<'a>(expr: &'a mut MirRelationExpr) -> Result<(), RecursionLimitError> {
    expr.visit_mut_post(&mut normalize_lets_at_root)
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
///
/// Note that the implementation of this spec is not recursive,
/// so 〚-〛 calls in the resulting term are avoided. This ensures
/// that the entire tree can be rewritten in a single bottom-up pass.
fn normalize_lets_at_root(expr: &mut MirRelationExpr) {
    use MirRelationExpr::*;
    let normalized_expr = match expr {
        Constant { rows: _, typ: _ } => None,
        Get { id: _, typ: _ } => None,
        Let {
            id: id_1,
            value: value_1,
            body: body_1,
        } => {
            let is_modified = at_inner_most_let_body(value_1, |body_2| {
                *body_2 = Let {
                    id: id_1.clone(),
                    value: Box::new(body_2.take_dangerous()),
                    body: Box::new(body_1.take_dangerous()),
                }
            });
            if is_modified {
                Some(value_1.take_dangerous())
            } else {
                None
            }
        }
        Project { input, outputs } => {
            let is_modified = at_inner_most_let_body(input, |body| {
                *body = Project {
                    input: Box::new(body.take_dangerous()),
                    outputs: outputs.split_off(0),
                }
            });
            if is_modified {
                Some(input.take_dangerous())
            } else {
                None
            }
        }
        Map { input, scalars } => {
            let is_modified = at_inner_most_let_body(input, |body| {
                *body = Map {
                    input: Box::new(body.take_dangerous()),
                    scalars: scalars.split_off(0),
                }
            });
            if is_modified {
                Some(input.take_dangerous())
            } else {
                None
            }
        }
        FlatMap { input, func, exprs } => {
            let is_modified = at_inner_most_let_body(input, |body| {
                *body = FlatMap {
                    input: Box::new(body.take_dangerous()),
                    func: func.clone(),
                    exprs: exprs.split_off(0),
                }
            });
            if is_modified {
                Some(input.take_dangerous())
            } else {
                None
            }
        }
        Filter { input, predicates } => {
            let is_modified = at_inner_most_let_body(input, |body| {
                *body = Filter {
                    input: Box::new(body.take_dangerous()),
                    predicates: predicates.split_off(0),
                }
            });
            if is_modified {
                Some(input.take_dangerous())
            } else {
                None
            }
        }
        Join {
            inputs,
            equivalences,
            implementation,
        } => {
            let mut bindings = vec![];

            let inputs = inputs
                .drain(..)
                .map(|input| {
                    let mut residual = input;
                    while let Let { id, value, body } = residual {
                        bindings.push((id, value));
                        residual = *body;
                    }
                    residual
                })
                .collect::<Vec<_>>();

            let mut result = Join {
                inputs,
                equivalences: equivalences.split_off(0),
                implementation: implementation.to_owned(),
            };
            for (id, value) in bindings.drain(..).rev() {
                result = Let {
                    id,
                    value,
                    body: Box::new(result),
                };
            }

            Some(result)
        }
        Reduce {
            input,
            group_key,
            aggregates,
            monotonic,
            expected_group_size,
        } => {
            let is_modified = at_inner_most_let_body(input, |body| {
                *body = Reduce {
                    input: Box::new(body.take_dangerous()),
                    group_key: group_key.split_off(0),
                    aggregates: aggregates.split_off(0),
                    monotonic: monotonic.clone(),
                    expected_group_size: expected_group_size.clone(),
                }
            });
            if is_modified {
                Some(input.take_dangerous())
            } else {
                None
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
            let is_modified = at_inner_most_let_body(input, |body| {
                *body = TopK {
                    input: Box::new(body.take_dangerous()),
                    group_key: group_key.split_off(0),
                    order_key: order_key.split_off(0),
                    limit: limit.clone(),
                    offset: offset.clone(),
                    monotonic: monotonic.clone(),
                }
            });
            if is_modified {
                Some(input.take_dangerous())
            } else {
                None
            }
        }
        Negate { input } => {
            let is_modified = at_inner_most_let_body(input, |body| {
                *body = Negate {
                    input: Box::new(body.take_dangerous()),
                }
            });
            if is_modified {
                Some(input.take_dangerous())
            } else {
                None
            }
        }
        Threshold { input } => {
            let is_modified = at_inner_most_let_body(input, |body| {
                *body = Threshold {
                    input: Box::new(body.take_dangerous()),
                }
            });
            if is_modified {
                Some(input.take_dangerous())
            } else {
                None
            }
        }
        Union { base, inputs } => {
            let mut bindings = vec![];

            let mut inputs = once(base.take_dangerous())
                .chain(inputs.split_off(0))
                .into_iter()
                .map(|input| {
                    let mut residual = input;
                    while let Let { id, value, body } = residual {
                        bindings.push((id, value));
                        residual = *body;
                    }
                    residual
                })
                .collect::<Vec<_>>();
            let base = Box::new(inputs.remove(0));

            let mut result = Union { base, inputs };
            for (id, value) in bindings.drain(..).rev() {
                result = Let {
                    id,
                    value,
                    body: Box::new(result),
                };
            }

            Some(result)
        }
        ArrangeBy { input, keys } => {
            let is_modified = at_inner_most_let_body(input, |body| {
                *body = ArrangeBy {
                    input: Box::new(body.take_dangerous()),
                    keys: keys.split_off(0),
                }
            });
            if is_modified {
                Some(input.take_dangerous())
            } else {
                None
            }
        }
    };
    if let Some(mut normalized_expr) = normalized_expr {
        std::mem::swap(expr, &mut normalized_expr);
    }
}

/// Try to match `expr`, assuming it is a chain of nested `Let` nodes,
/// until the inner-most `body` is reached, and apply `f` to that `body`.
/// Return `true` iff the match was successful and `f` has been applied.
fn at_inner_most_let_body<F>(expr: &mut MirRelationExpr, mut f: F) -> bool
where
    F: FnMut(&mut MirRelationExpr),
{
    use MirRelationExpr::Let;
    match expr {
        Let { body: next, .. } => {
            let mut body = next.as_mut();
            while let Let {
                id: _,
                value: _,
                body: next,
            } = body
            {
                body = next.as_mut();
            }
            f(body);
            true
        }
        _ => false,
    }
}
