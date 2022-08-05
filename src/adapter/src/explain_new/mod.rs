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

use mz_expr::RowSetFinishing;
use mz_ore::str::{Indent, IndentLike};
use mz_repr::explain_new::{DisplayText, ExplainConfig, ExprHumanizer, RenderingContext};
use mz_repr::GlobalId;
use mz_storage::types::transforms::LinearOperator;

use crate::coord::peek::{self, FastPathPlan};

pub(crate) mod common;
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

#[derive(Clone)]
pub(crate) struct Attributes;

/// A somewhat ad-hoc way to keep carry a plan with a set
/// of attributes derived for each node in that plan.
#[allow(dead_code)] // TODO (#13299)
pub(crate) struct AnnotatedPlan<'a, T> {
    pub(crate) plan: &'a T,
    pub(crate) annotations: HashMap<&'a T, Attributes>,
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
}

impl<'a, C> DisplayText<C> for UsedIndexes
where
    C: AsMut<Indent> + AsRef<&'a dyn ExprHumanizer>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        write!(f, "{}used_indexes:", ctx.as_mut())?;
        *ctx.as_mut() += 1;
        for id in &self.0 {
            let index_name = ctx
                .as_ref()
                .humanize_id(*id)
                .unwrap_or_else(|| id.to_string());
            write!(f, "{}- {}", ctx.as_mut(), index_name)?;
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
            if let Some(fast_path_plan) = fast_path_plan {
                fast_path_plan.fmt_text(f, ctx)
            } else {
                Displayable::from(plan).fmt_text(f, ctx)
            }
        }

        if let Some(finishing) = &self.context.finishing {
            finishing.fmt_text(f, &mut ctx.indent)?;
            ctx.indented(|ctx| fmt_plan(ctx, f, &self.context.fast_path_plan, self.plan.plan))?;
        } else {
            fmt_plan(&mut ctx, f, &self.context.fast_path_plan, self.plan.plan)?;
        }

        if !self.context.used_indexes.0.is_empty() {
            writeln!(f, "")?;
            self.context.used_indexes.fmt_text(f, &mut ctx)?;
        }

        Ok(())
    }
}

/// A structure produced by the `explain_$format` methods in
/// [`mz_repr::explain_new::Explain`] implementations at points
/// in the optimization pipeline identified with a
/// `DataflowDescription` instance with plans of type `T`.
#[allow(dead_code)] // TODO (#13299)
pub(crate) struct ExplainMultiPlan<'a, T> {
    pub(crate) context: ExplainContext<'a>,
    // Maps the names of the sources to the linear operators that will be
    // on them.
    // TODO: implement DisplayText and DisplayJson for LinearOperator
    // there are plans to replace LinearOperator with MFP struct (#6657)
    pub(crate) sources: Vec<(String, LinearOperator)>,
    // elements of the vector are in topological order
    pub(crate) plans: Vec<AnnotatedPlan<'a, T>>,
}

impl<'a, T: 'a> DisplayText<()> for ExplainMultiPlan<'a, T>
where
    Displayable<'a, T>: DisplayText<PlanRenderingContext<'a, T>>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        // TODO (#13472)
        let mut ctx = RenderingContext::new(Indent::default(), self.context.humanizer);

        fn fmt_plan<'a, T>(
            ctx: &mut RenderingContext<'a>,
            f: &mut fmt::Formatter<'_>,
            fast_path_plan: &Option<FastPathPlan>,
            _plans: &Vec<AnnotatedPlan<'a, T>>,
            _sources: &Vec<(String, LinearOperator)>,
        ) -> fmt::Result
        where
            Displayable<'a, T>: DisplayText<PlanRenderingContext<'a, T>>,
        {
            if let Some(fast_path_plan) = fast_path_plan {
                fast_path_plan.fmt_text(f, ctx)?;
            } else {
                // self.plans.fmt_text(..., f)?;
                // writeln!(f, "")?;
                // self.sources.fmt_text(..., f)?;
            }
            Ok(())
        }

        if let Some(finishing) = &self.context.finishing {
            finishing.fmt_text(f, &mut ctx.indent)?;
            ctx.indented(|ctx| {
                fmt_plan(
                    ctx,
                    f,
                    &self.context.fast_path_plan,
                    &self.plans,
                    &self.sources,
                )
            })?;
        } else {
            fmt_plan(
                &mut ctx,
                f,
                &self.context.fast_path_plan,
                &self.plans,
                &self.sources,
            )?;
        }
        if !self.context.used_indexes.0.is_empty() {
            writeln!(f, "")?;
            self.context.used_indexes.fmt_text(f, &mut ctx)?;
        }

        Ok(())
    }
}

#[allow(dead_code)] // TODO (#13299)
#[allow(missing_debug_implementations)]
pub(crate) struct PlanRenderingContext<'a, T> {
    pub(crate) indent: Indent,
    pub(crate) humanizer: &'a dyn ExprHumanizer,
    pub(crate) annotations: HashMap<&'a T, Attributes>, // TODO: can this be a ref
    pub(crate) config: &'a ExplainConfig,
}

impl<'a, T> PlanRenderingContext<'a, T> {
    #[allow(dead_code)] // TODO (#13299)
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
