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

use mz_expr::{ExprHumanizer, RowSetFinishing};
use mz_repr::{
    explain_new::{DisplayText, Explain, Indent, UnsupportedFormat},
    GlobalId,
};
use mz_storage::client::transforms::LinearOperator;

use crate::coord::fast_path_peek;

pub(crate) mod common;
pub(crate) mod hir;
pub(crate) mod lir;
pub(crate) mod mir;
pub(crate) mod qgm;

/// Newtype struct for wrapping types that should
/// implement the `Explain` trait.
pub(crate) struct Explainable<'a, T>(&'a mut T);

impl<'a, T> Explainable<'a, T> {
    pub(crate) fn new(t: &'a mut T) -> Explainable<'a, T> {
        Explainable(t)
    }
}

/// Explain context shared by all `Explain` implementations
/// in this crate.
#[derive(Debug)]
pub struct ExplainContext<'a> {
    pub humanizer: &'a dyn ExprHumanizer,
    pub used_indexes: Vec<GlobalId>,
    pub finishing: Option<RowSetFinishing>,
    pub fast_path_plan: Option<fast_path_peek::Plan>,
}

pub struct Attributes;

/// A somewhat hacky way to annotate the nodes of a plan with
/// arbitrary attributes based on the pre-order of the items
/// in the associated plan.
pub struct AnnotatedPlan<T> {
    pub plan: T,
    pub annotations: HashMap<usize, Attributes>,
}

pub struct ExplainSinglePlan<'a, T> {
    pub context: ExplainContext<'a>,
    pub plan: AnnotatedPlan<T>,
}

impl<'a, T> DisplayText for ExplainSinglePlan<'a, T>
where
    T: DisplayText,
{
    type Context = ();

    fn fmt_text(&self, _: &mut Self::Context, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut indent = Indent::default();

        self.context.finishing.fmt_text(&mut indent, f)?;
        // self.plans.fmt_text(..., f)?;
        // self.context.used_indexes.fmt_text(..., f)?;
        Ok(())
    }
}

pub struct ExplainMultiPlan<'a, T> {
    pub context: ExplainContext<'a>,
    // Maps the names of the sources to the linear operators that will be
    // on them.
    // TODO: implement DisplayText and DisplayJson for LinearOperator
    // there are plans to replace LinearOperator with MFP struct (#6657)
    pub sources: Vec<(String, LinearOperator)>,
    // elements of the vector are in topological order
    pub plans: Vec<AnnotatedPlan<T>>,
}

impl<'a, T> DisplayText for ExplainMultiPlan<'a, T>
where
    T: DisplayText,
{
    type Context = ();

    fn fmt_text(&self, _: &mut Self::Context, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut indent = Indent::default();

        self.context.finishing.fmt_text(&mut indent, f)?;
        // self.plans.fmt_text(..., f)?;
        // self.sources.used_indexes.fmt_text(..., f)?;
        // self.context.used_indexes.fmt_text(..., f)?;
        Ok(())
    }
}
