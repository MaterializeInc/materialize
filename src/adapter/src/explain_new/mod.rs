// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for various intermediate representations.
//!
//! Ideally, the `EXPLAIN` support for each IR should be in the
//! crate where this IR is defined. However, due to the use of
//! some generic structs with fields specific to LIR in the current
//! explain paths, and the dependency chain between the crates where
//! the various IRs live, this is not possible. Consequencly, we
//! currently resort to using a wrapper type.

use std::collections::HashMap;
use std::fmt;

use mz_expr::explain::Indices;
use mz_expr::{MapFilterProject, RowSetFinishing};
use mz_ore::str::{Indent, IndentLike};
use mz_repr::explain_new::{
    separated_text, DisplayJson, DisplayText, ExplainConfig, ExprHumanizer, RenderingContext,
};
use mz_repr::GlobalId;

use crate::coord::peek::{self, FastPathPlan};

pub(crate) mod hir;
pub(crate) mod lir;
pub(crate) mod mir;
pub(crate) mod qgm;

/// Newtype struct for wrapping types that should
/// implement the [`mz_repr::explain_new::Explain`] trait.
pub(crate) struct Explainable<'a, T>(&'a mut T);

impl<'a, T> Explainable<'a, T> {
    pub(crate) fn new(t: &'a mut T) -> Explainable<'a, T> {
        Explainable(t)
    }
}

/// Newtype struct for wrapping types that should implement one
/// of the `Display$Format` traits.
///
/// While explainable wraps a mutable reference passed to the
/// `explain*` methods in [`mz_repr::explain_new::Explain`],
/// [`Displayable`] wraps a shared reference passed to the
/// `fmt_$format` methods in `Display$Format`.
pub(crate) struct Displayable<'a, T>(&'a T);

impl<'a, T> From<&'a T> for Displayable<'a, T> {
    fn from(t: &'a T) -> Self {
        Displayable(t)
    }
}

/// Explain context shared by all [`mz_repr::explain_new::Explain`]
/// implementations in this crate.
#[derive(Debug)]
#[allow(dead_code)] // TODO (#13299)
pub(crate) struct ExplainContext<'a> {
    pub(crate) config: &'a ExplainConfig,
    pub(crate) humanizer: &'a dyn ExprHumanizer,
    pub(crate) used_indexes: UsedIndexes,
    pub(crate) finishing: Option<RowSetFinishing>,
    pub(crate) fast_path_plan: Option<peek::FastPathPlan>,
}

/// A somewhat ad-hoc way to keep carry a plan with a set
/// of attributes derived for each node in that plan.
#[allow(dead_code)] // TODO (#13299)
pub(crate) struct AnnotatedPlan<'a, T> {
    pub(crate) plan: &'a T,
    pub(crate) annotations: HashMap<&'a T, Attributes>,
}

/// A container for derived attributes.
#[derive(Clone, Default, Debug)]
pub struct Attributes {
    non_negative: Option<bool>,
    subtree_size: Option<usize>,
    arity: Option<usize>,
    types: Option<String>,
    keys: Option<String>,
}

impl fmt::Display for Attributes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("//");
        if let Some(subtree_size) = &self.subtree_size {
            builder.field("subtree_size", subtree_size);
        }
        if let Some(non_negative) = &self.non_negative {
            builder.field("non_negative", non_negative);
        }
        if let Some(arity) = &self.arity {
            builder.field("arity", arity);
        }
        if let Some(types) = &self.types {
            builder.field("types", types);
        }
        if let Some(keys) = &self.keys {
            builder.field("keys", keys);
        }
        builder.finish()
    }
}

/// A set of indexes that are used in the physical plan
/// derived  from a plan wrapped in an [`ExplainSinglePlan`]
/// or an [`ExplainMultiPlan`].
#[derive(Debug)]
pub(crate) struct UsedIndexes(Vec<GlobalId>);

impl UsedIndexes {
    pub(crate) fn new(values: Vec<GlobalId>) -> UsedIndexes {
        UsedIndexes(values)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<'a, C> DisplayText<C> for UsedIndexes
where
    C: AsMut<Indent> + AsRef<&'a dyn ExprHumanizer>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        writeln!(f, "{}Used Indexes:", ctx.as_mut())?;
        *ctx.as_mut() += 1;
        for id in &self.0 {
            let index_name = ctx
                .as_ref()
                .humanize_id(*id)
                .unwrap_or_else(|| id.to_string());
            writeln!(f, "{}- {}", ctx.as_mut(), index_name)?;
        }
        *ctx.as_mut() -= 1;
        Ok(())
    }
}

/// A structure produced by the `explain_$format` methods in
/// [`mz_repr::explain_new::Explain`] implementations for points
/// in the optimization pipeline identified with a single plan of
/// type `T`.
#[allow(dead_code)] // TODO (#13299)
pub(crate) struct ExplainSinglePlan<'a, T> {
    context: &'a ExplainContext<'a>,
    plan: AnnotatedPlan<'a, T>,
}

impl<'a, T: 'a> DisplayText<()> for ExplainSinglePlan<'a, T>
where
    Displayable<'a, T>: DisplayText<PlanRenderingContext<'a, T>>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        let mut ctx = PlanRenderingContext::new(
            Indent::default(),
            self.context.humanizer,
            self.plan.annotations.clone(),
            self.context.config,
        );

        fn fmt_plan<'a, T>(
            ctx: &mut PlanRenderingContext<'a, T>,
            f: &mut fmt::Formatter<'_>,
            fast_path_plan: &Option<FastPathPlan>,
            plan: &'a T,
        ) -> fmt::Result
        where
            Displayable<'a, T>: DisplayText<PlanRenderingContext<'a, T>>,
        {
            match fast_path_plan {
                Some(fast_path_plan) if !ctx.config.no_fast_path => fast_path_plan.fmt_text(f, ctx),
                _ => Displayable::from(plan).fmt_text(f, ctx),
            }
        }

        if let Some(finishing) = &self.context.finishing {
            finishing.fmt_text(f, &mut ctx.indent)?;
            ctx.indented(|ctx| fmt_plan(ctx, f, &self.context.fast_path_plan, self.plan.plan))?;
        } else {
            fmt_plan(&mut ctx, f, &self.context.fast_path_plan, self.plan.plan)?;
        }

        if !self.context.used_indexes.is_empty() {
            writeln!(f, "")?;
            self.context.used_indexes.fmt_text(f, &mut ctx)?;
        }

        Ok(())
    }
}

impl<'a, T: 'a> DisplayJson for ExplainSinglePlan<'a, T>
where
    T: serde::Serialize,
{
    fn to_serde_value(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(&self.plan.plan)
    }
}

/// A structure produced by the `explain_$format` methods in
/// [`mz_repr::explain_new::Explain`] implementations at points
/// in the optimization pipeline identified with a
/// `DataflowDescription` instance with plans of type `T`.
pub(crate) struct ExplainMultiPlan<'a, T> {
    pub(crate) context: &'a ExplainContext<'a>,
    // Maps the names of the sources to the linear operators that will be
    // on them.
    pub(crate) sources: Vec<(String, &'a MapFilterProject)>,
    // elements of the vector are in topological order
    pub(crate) plans: Vec<(String, AnnotatedPlan<'a, T>)>,
}

impl<'a, T: 'a> DisplayText<()> for ExplainMultiPlan<'a, T>
where
    Displayable<'a, T>: DisplayText<PlanRenderingContext<'a, T>>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        let mut ctx = RenderingContext::new(Indent::default(), self.context.humanizer);

        match &self.context.fast_path_plan {
            Some(fast_path_plan) if !self.context.config.no_fast_path => {
                writeln!(f, "{}{} (fast path)", ctx.indent, GlobalId::Explain)?;
                ctx.indented(|ctx| {
                    // if present, a RowSetFinishing is applied on top of the fast path plan
                    match &self.context.finishing {
                        Some(finishing) => {
                            finishing.fmt_text(f, &mut ctx.indent)?;
                            ctx.indented(|ctx| fast_path_plan.fmt_text(f, ctx))?;
                        }
                        _ => {
                            fast_path_plan.fmt_text(f, ctx)?;
                        }
                    }
                    Ok(())
                })?;
            }
            _ => {
                // render plans
                for (no, (id, plan)) in self.plans.iter().enumerate() {
                    let mut ctx = PlanRenderingContext::new(
                        ctx.indent.clone(),
                        ctx.humanizer,
                        plan.annotations.clone(),
                        self.context.config,
                    );

                    writeln!(f, "{}{}", ctx.indent, id)?;
                    ctx.indented(|ctx| {
                        match &self.context.finishing {
                            // if present, a RowSetFinishing always applies to the first rendered plan
                            Some(finishing) if no == 0 => {
                                finishing.fmt_text(f, &mut ctx.indent)?;
                                ctx.indented(|ctx| Displayable(plan.plan).fmt_text(f, ctx))?;
                            }
                            // all other plans are rendered without a RowSetFinishing
                            _ => {
                                Displayable(plan.plan).fmt_text(f, ctx)?;
                            }
                        }
                        Ok(())
                    })?;
                }
                if !self.sources.is_empty() {
                    // render one blank line between the plans and sources
                    writeln!(f, "")?;
                    // render sources
                    for (id, operator) in self.sources.iter() {
                        writeln!(f, "{}Source {}", ctx.indent, id)?;
                        ctx.indented(|ctx| Displayable(*operator).fmt_text(f, ctx))?;
                    }
                }
            }
        }

        if !self.context.used_indexes.is_empty() {
            writeln!(f, "")?;
            self.context.used_indexes.fmt_text(f, &mut ctx)?;
        }

        Ok(())
    }
}

impl<'a, T: 'a> DisplayJson for ExplainMultiPlan<'a, T>
where
    T: serde::Serialize,
{
    fn to_serde_value(&self) -> serde_json::Result<serde_json::Value> {
        let plans = self
            .plans
            .iter()
            .map(|(id, plan)| {
                // TODO: fix plans with Constants
                serde_json::json!({
                    "id": id,
                    "plan": &plan.plan
                })
            })
            .collect::<Vec<_>>();

        let sources = self
            .sources
            .iter()
            .map(|(id, op)| {
                serde_json::json!({
                    "id": id,
                    "op": op
                })
            })
            .collect::<Vec<_>>();

        let result = serde_json::json!({ "plans": plans, "sources": sources });

        Ok(result)
    }
}

impl<'a> DisplayText<RenderingContext<'a>> for Displayable<'a, MapFilterProject> {
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut RenderingContext<'a>) -> fmt::Result {
        let (scalars, predicates, outputs, input_arity) = (
            &self.0.expressions,
            &self.0.predicates,
            &self.0.projection,
            &self.0.input_arity,
        );

        // render `project` field iff not the identity projection
        if &outputs.len() != input_arity || outputs.iter().enumerate().any(|(i, p)| i != *p) {
            let outputs = Indices(&outputs);
            writeln!(f, "{}project=({})", ctx.indent, outputs)?;
        }
        // render `filter` field iff predicates are present
        if !predicates.is_empty() {
            let predicates = predicates.iter().map(|(_, p)| Displayable::from(p));
            let predicates = separated_text(" AND ", predicates);
            writeln!(f, "{}filter=({})", ctx.indent, predicates)?;
        }
        // render `map` field iff scalars are present
        if !scalars.is_empty() {
            let scalars = scalars.iter().map(Displayable::from);
            let scalars = separated_text(", ", scalars);
            writeln!(f, "{}map=({})", ctx.indent, scalars)?;
        }

        Ok(())
    }
}

#[allow(dead_code)] // TODO (#13299)
#[allow(missing_debug_implementations)]
pub(crate) struct PlanRenderingContext<'a, T> {
    pub(crate) indent: Indent, // TODO: can this be a ref?
    pub(crate) humanizer: &'a dyn ExprHumanizer,
    pub(crate) annotations: HashMap<&'a T, Attributes>, // TODO: can this be a ref?
    pub(crate) config: &'a ExplainConfig,
}

impl<'a, T> PlanRenderingContext<'a, T> {
    pub fn new(
        indent: Indent,
        humanizer: &'a dyn ExprHumanizer,
        annotations: HashMap<&'a T, Attributes>,
        config: &'a ExplainConfig,
    ) -> PlanRenderingContext<'a, T> {
        PlanRenderingContext {
            indent,
            humanizer,
            annotations,
            config,
        }
    }
}

impl<'a, T> AsMut<Indent> for PlanRenderingContext<'a, T> {
    fn as_mut(&mut self) -> &mut Indent {
        &mut self.indent
    }
}

impl<'a, T> AsRef<&'a dyn ExprHumanizer> for PlanRenderingContext<'a, T> {
    fn as_ref(&self) -> &&'a dyn ExprHumanizer {
        &self.humanizer
    }
}
