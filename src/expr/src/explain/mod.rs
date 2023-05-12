// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for structures defined in this crate.

use std::collections::BTreeMap;
use std::fmt::Formatter;
use std::time::Duration;

use itertools::Itertools;

use mz_ore::stack::RecursionLimitError;
use mz_ore::str::Indent;
use mz_repr::explain::text::DisplayText;
use mz_repr::explain::ExplainError::LinearChainsPlusRecursive;
use mz_repr::explain::{
    AnnotatedPlan, Explain, ExplainConfig, ExplainError, ExprHumanizer, ScalarOps,
    UnsupportedFormat, UsedIndexes,
};

use crate::interpret::{Interpreter, Pushdownable, RelationTrace, Trace};
use crate::{
    visit::Visit, Id, LocalId, MapFilterProject, MirRelationExpr, MirScalarExpr, RowSetFinishing,
};

mod json;
mod text;

/// Explain context shared by all [`mz_repr::explain::Explain`]
/// implementations in this crate.
#[derive(Debug)]
pub struct ExplainContext<'a> {
    pub config: &'a ExplainConfig,
    pub humanizer: &'a dyn ExprHumanizer,
    pub used_indexes: UsedIndexes,
    pub finishing: Option<RowSetFinishing>,
    pub duration: Duration,
}

/// A structure produced by the `explain_$format` methods in
/// [`mz_repr::explain::Explain`] implementations for points
/// in the optimization pipeline identified with a single plan of
/// type `T`.
#[allow(missing_debug_implementations)]
pub struct ExplainSinglePlan<'a, T> {
    pub context: &'a ExplainContext<'a>,
    pub plan: AnnotatedPlan<'a, T>,
}

/// Carries metadata about the possibility of MFP pushdown for a source.
/// (Likely to change, and only emitted when a context flag is enabled.)
#[allow(missing_debug_implementations)]
pub struct PushdownInfo {
    /// Pushdown-able columns in the source.
    pub trace: RelationTrace,
}

impl<C: AsMut<Indent>> DisplayText<C> for PushdownInfo {
    fn fmt_text(&self, f: &mut Formatter<'_>, ctx: &mut C) -> std::fmt::Result {
        if !self.trace.0.is_empty() {
            writeln!(
                f,
                "{}pushdown=({})",
                ctx.as_mut(),
                self.trace
                    .0
                    .iter()
                    .enumerate()
                    .filter_map(|(id, p)| match p {
                        Pushdownable::No => None,
                        Pushdownable::Maybe => Some(format!("#{id}?")),
                        Pushdownable::Yes => Some(format!("#{id}")),
                    })
                    .join(", ")
            )?;
        }
        Ok(())
    }
}

#[allow(missing_debug_implementations)]
pub struct ExplainSource<'a> {
    pub id: String,
    pub op: &'a MapFilterProject,
    pub pushdown_info: Option<PushdownInfo>,
}

impl<'a> ExplainSource<'a> {
    pub fn new(
        id: String,
        op: &'a MapFilterProject,
        context: &ExplainContext<'a>,
    ) -> ExplainSource<'a> {
        let pushdown_info = if context.config.mfp_pushdown {
            Some(PushdownInfo {
                trace: Trace.mfp_filter(op),
            })
        } else {
            None
        };

        ExplainSource {
            id,
            op,
            pushdown_info,
        }
    }
}

/// A structure produced by the `explain_$format` methods in
/// [`mz_repr::explain::Explain`] implementations at points
/// in the optimization pipeline identified with a
/// `DataflowDescription` instance with plans of type `T`.
#[allow(missing_debug_implementations)]
pub struct ExplainMultiPlan<'a, T> {
    pub context: &'a ExplainContext<'a>,
    // Maps the names of the sources to the linear operators that will be
    // on them.
    pub sources: Vec<ExplainSource<'a>>,
    // elements of the vector are in topological order
    pub plans: Vec<(String, AnnotatedPlan<'a, T>)>,
}

impl<'a> Explain<'a> for MirRelationExpr {
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

impl<'a> MirRelationExpr {
    fn as_explain_single_plan(
        &'a mut self,
        context: &'a ExplainContext<'a>,
    ) -> Result<ExplainSinglePlan<'a, MirRelationExpr>, ExplainError> {
        // normalize the representation as linear chains
        // (this implies !context.config.raw_plans by construction)
        if context.config.linear_chains {
            enforce_linear_chains(self)?;
        };

        let plan = AnnotatedPlan {
            plan: self,
            annotations: BTreeMap::default(),
        };

        Ok(ExplainSinglePlan { context, plan })
    }
}

/// Normalize the way inputs of multi-input variants are rendered.
///
/// After the transform is applied, non-trival inputs `$input` of variants with
/// more than one input are wrapped in a `let $x = $input in $x` blocks.
///
/// If these blocks are subsequently pulled up by `NormalizeLets`,
/// the rendered version of the resulting tree will only have linear chains.
pub fn enforce_linear_chains(expr: &mut MirRelationExpr) -> Result<(), ExplainError> {
    use MirRelationExpr::{Constant, Get, Join, Union};

    if expr.is_recursive() {
        // `linear_chains` is not implemented for WMR, see
        // https://github.com/MaterializeInc/materialize/issues/19012
        return Err(LinearChainsPlusRecursive);
    }

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

impl ScalarOps for MirScalarExpr {
    fn match_col_ref(&self) -> Option<usize> {
        match self {
            MirScalarExpr::Column(c) => Some(*c),
            _ => None,
        }
    }

    fn references(&self, column: usize) -> bool {
        match self {
            MirScalarExpr::Column(c) => *c == column,
            _ => false,
        }
    }
}
