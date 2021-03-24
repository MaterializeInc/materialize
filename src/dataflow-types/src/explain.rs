// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module houses a pretty printer for the parts of a
//! [`DataflowDesc`] that are relevant to dataflow rendering.
//!
//! Format details:
//!
//!   * Sources that have [`LinearOperator`]s come first.
//!     The format is "Source <name> (<id>):" followed by the `predicates` of
//!     the [`LinearOperator`] and then the `projection`.
//!   * Intermediate views in the dataflow come next.
//!     The format is "View <name> (<id>):" followed by the output of
//!     [`expr::explain::ViewExplanation`].
//!   * Last is the view or query being explained. The format is "Query:"
//!     followed by the output of [`expr::explain::ViewExplanation`].
//!   * If there are no sources with some [`LinearOperator`] and no intermediate
//!     views, then the format is identical to the format of
//!     [`expr::explain::ViewExplanation`].
//!
//! It's important to avoid trailing whitespace everywhere, as plans may be
//! printed in contexts where trailing whitespace is unacceptable, like
//! sqllogictest files.

use std::fmt;

use crate::{DataflowDesc, LinearOperator};

use expr::explain::*;
use expr::{ExprHumanizer, GlobalId, MirRelationExpr, RowSetFinishing};

/// An `Explanation` facilitates pretty-printing of the parts of a
/// [`DataflowDesc`] that are relevant to dataflow rendering.
///
/// By default, the [`fmt::Display`] implementation renders the expression as
/// described in the module docs. Additional information may be attached to the
/// explanation via the other public methods on the type.
#[derive(Debug)]
pub struct Explanation<'a> {
    expr_humanizer: &'a dyn ExprHumanizer,
    /// Each source that has some [`LinearOperator`].
    sources: Vec<(GlobalId, &'a LinearOperator)>,
    /// One `ViewExplanation` per view in the dataflow.
    views: Vec<(GlobalId, ViewExplanation<'a>)>,
    /// An optional `RowSetFinishing` to mention at the end.
    finishing: Option<RowSetFinishing>,
}

impl<'a> Explanation<'a> {
    /// Creates an explanation for a [`MirRelationExpr`].
    pub fn new(
        expr: &'a MirRelationExpr,
        expr_humanizer: &'a dyn ExprHumanizer,
    ) -> Explanation<'a> {
        Explanation {
            expr_humanizer,
            sources: vec![],
            views: vec![(
                GlobalId::Explain,
                ViewExplanation::new(expr, expr_humanizer),
            )],
            finishing: None,
        }
    }

    pub fn new_from_dataflow(
        dataflow: &'a DataflowDesc,
        expr_humanizer: &'a dyn ExprHumanizer,
    ) -> Explanation<'a> {
        let sources = dataflow
            .source_imports
            .iter()
            .filter_map(|(id, source_desc)| {
                if let Some(operator) = &source_desc.0.operators {
                    Some((*id, operator))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let views = dataflow
            .objects_to_build
            .iter()
            .map(|build_desc| {
                (
                    build_desc.id,
                    ViewExplanation::new(build_desc.relation_expr.as_ref(), expr_humanizer),
                )
            })
            .collect::<Vec<_>>();
        Explanation {
            expr_humanizer,
            sources,
            views,
            finishing: None,
        }
    }

    /// Attach type information into the explanation.
    pub fn explain_types(&mut self) {
        for (_, view) in &mut self.views {
            view.explain_types();
        }
    }

    /// Attach a `RowSetFinishing` to the explanation.
    pub fn explain_row_set_finishing(&mut self, finishing: RowSetFinishing) {
        self.finishing = Some(finishing);
    }
}

impl<'a> fmt::Display for Explanation<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (id, operator) in &self.sources {
            writeln!(
                f,
                "Source {} ({}):",
                self.expr_humanizer
                    .humanize_id(*id)
                    .unwrap_or_else(|| "?".to_owned()),
                id,
            )?;
            writeln!(
                f,
                "| Filter {}",
                separated(", ", operator.predicates.iter())
            )?;
            writeln!(
                f,
                "| Project {}",
                bracketed("(", ")", Indices(&operator.projection))
            )?;
            writeln!(f)?;
        }
        for (view_num, (id, view)) in self.views.iter().enumerate() {
            if view_num > 0 {
                writeln!(f)?;
            }
            if self.sources.len() > 0 || self.views.len() > 1 {
                match id {
                    GlobalId::Explain => writeln!(f, "Query:")?,
                    _ => writeln!(
                        f,
                        "View {} ({}):",
                        self.expr_humanizer
                            .humanize_id(*id)
                            .unwrap_or_else(|| "?".to_owned()),
                        id
                    )?,
                }
            }
            view.fmt(f)?;
        }

        if let Some(finishing) = &self.finishing {
            writeln!(
                f,
                "\nFinish order_by={} limit={} offset={} project={}",
                bracketed("(", ")", separated(", ", &finishing.order_by)),
                match finishing.limit {
                    Some(limit) => limit.to_string(),
                    None => "none".to_owned(),
                },
                finishing.offset,
                bracketed("(", ")", Indices(&finishing.project))
            )?;
        }

        Ok(())
    }
}
