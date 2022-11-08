// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module houses a pretty printer for the parts of a
//! [`DataflowDescription`] that are relevant to dataflow rendering.
//!
//! Format details:
//!
//!   * Sources that have [`MapFilterProject`]s come first.
//!     The format is "Source <name> (<id>):" followed by the [`MapFilterProject`].
//!   * Intermediate views in the dataflow come next.
//!     The format is "View <name> (<id>):" followed by the output of
//!     [`mz_expr::explain::ViewExplanation`].
//!   * Last is the view or query being explained. The format is "Query:"
//!     followed by the output of [`mz_expr::explain::ViewExplanation`].
//!   * If there are no sources with some [`MapFilterProject`] and no intermediate
//!     views, then the format is identical to the format of
//!     [`mz_expr::explain::ViewExplanation`].
//!
//! It's important to avoid trailing whitespace everywhere, as plans may be
//! printed in contexts where trailing whitespace is unacceptable, like
//! sqllogictest files.

use std::fmt;

use chrono::NaiveDateTime;
use mz_expr::explain::{Indices, ViewExplanation};
use mz_expr::MapFilterProject;
use mz_expr::{OptimizedMirRelationExpr, RowSetFinishing};
use mz_ore::result::ResultExt;
use mz_ore::str::{bracketed, separated};
use mz_repr::explain_new::ExprHumanizer;
use mz_repr::GlobalId;
use mz_storage_client::types::sources::Timeline;

use crate::command::DataflowDescription;

pub trait ViewFormatter<ViewExpr> {
    fn fmt_source_body(&self, f: &mut fmt::Formatter, operator: &MapFilterProject) -> fmt::Result;
    fn fmt_view(&self, f: &mut fmt::Formatter, view: &ViewExpr) -> fmt::Result;
}

/// An `Explanation` facilitates pretty-printing of the parts of a
/// [`DataflowDescription`] that are relevant to dataflow rendering.
///
/// By default, the [`fmt::Display`] implementation renders the expression as
/// described in the module docs. Additional information may be attached to the
/// explanation via the other public methods on the type.
#[derive(Debug)]
pub struct Explanation<'a, Formatter, ViewExpr>
where
    Formatter: ViewFormatter<ViewExpr>,
{
    /// Determines how sources and views are formatted
    formatter: &'a Formatter,
    expr_humanizer: &'a dyn ExprHumanizer,
    /// Each source that has some [`MapFilterProject`].
    sources: Vec<(GlobalId, &'a MapFilterProject)>,
    /// One `ViewExplanation` per view in the dataflow.
    views: Vec<(GlobalId, &'a ViewExpr)>,
    /// An optional `RowSetFinishing` to mention at the end.
    finishing: Option<RowSetFinishing>,
}

impl<'a, Formatter, ViewExpr> Explanation<'a, Formatter, ViewExpr>
where
    Formatter: ViewFormatter<ViewExpr>,
{
    pub fn new(
        expr: &'a ViewExpr,
        expr_humanizer: &'a dyn ExprHumanizer,
        formatter: &'a Formatter,
    ) -> Self {
        Self {
            formatter,
            expr_humanizer,
            sources: vec![],
            views: vec![(GlobalId::Explain, expr)],
            finishing: None,
        }
    }

    pub fn new_from_dataflow(
        dataflow: &'a DataflowDescription<ViewExpr>,
        expr_humanizer: &'a dyn ExprHumanizer,
        formatter: &'a Formatter,
    ) -> Self {
        let sources = dataflow
            .source_imports
            .iter()
            .filter_map(|(id, (source, _monotonic))| {
                if let Some(operator) = &source.arguments.operators {
                    Some((*id, operator))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let views = dataflow
            .objects_to_build
            .iter()
            .map(|build_desc| (build_desc.id, &build_desc.plan))
            .collect::<Vec<_>>();
        Self {
            formatter,
            expr_humanizer,
            sources,
            views,
            finishing: None,
        }
    }

    /// Attach a `RowSetFinishing` to the explanation.
    pub fn explain_row_set_finishing(&mut self, finishing: RowSetFinishing) {
        self.finishing = Some(finishing);
    }
}

impl<'a, Formatter, ViewExpr> fmt::Display for Explanation<'a, Formatter, ViewExpr>
where
    Formatter: ViewFormatter<ViewExpr>,
{
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
            self.formatter.fmt_source_body(f, operator)?;
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
            self.formatter.fmt_view(f, view)?;
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

pub struct JsonViewFormatter {}

impl<ViewExpr: serde::Serialize> ViewFormatter<ViewExpr> for JsonViewFormatter {
    fn fmt_source_body(&self, f: &mut fmt::Formatter, operator: &MapFilterProject) -> fmt::Result {
        let operator_str = match serde_json::to_string_pretty(operator).map_err_to_string() {
            Ok(o) => o,
            Err(e) => e,
        };
        writeln!(f, "{}", operator_str)
    }

    fn fmt_view(&self, f: &mut fmt::Formatter, view: &ViewExpr) -> fmt::Result {
        let view_str = match serde_json::to_string_pretty(view).map_err_to_string() {
            Ok(o) => o,
            Err(e) => e,
        };
        writeln!(f, "{}", view_str)
    }
}

pub struct DataflowGraphFormatter<'a> {
    expr_humanizer: &'a dyn ExprHumanizer,
    typed: bool,
}

impl<'a> DataflowGraphFormatter<'a> {
    pub fn new(expr_humanizer: &'a dyn ExprHumanizer, typed: bool) -> Self {
        Self {
            expr_humanizer,
            typed,
        }
    }
}

impl<'a> ViewFormatter<OptimizedMirRelationExpr> for DataflowGraphFormatter<'a> {
    fn fmt_source_body(&self, f: &mut fmt::Formatter, operator: &MapFilterProject) -> fmt::Result {
        let (map, filter, project) = operator.as_map_filter_project();
        if !map.is_empty() {
            writeln!(f, "| Map {}", separated(", ", map.iter()))?;
        }
        if !filter.is_empty() {
            writeln!(f, "| Filter {}", separated(", ", filter.iter()))?;
        }
        writeln!(f, "| Project {}", bracketed("(", ")", Indices(&project)))?;
        Ok(())
    }

    fn fmt_view(&self, f: &mut fmt::Formatter, view: &OptimizedMirRelationExpr) -> fmt::Result {
        let mut explain = ViewExplanation::new(view, self.expr_humanizer);
        if self.typed {
            explain.explain_types();
        }
        fmt::Display::fmt(&explain, f)
    }
}

/// Information used when determining the timestamp for a query.
pub struct TimestampExplanation<T> {
    /// The chosen timestamp from `determine_timestamp`.
    pub timestamp: T,
    /// The timeline that the timestamp corresponds to.
    pub timeline: Option<Timeline>,
    /// The read frontier of all involved sources.
    pub since: Vec<T>,
    /// The write frontier of all involved sources.
    pub upper: Vec<T>,
    /// Whether the query can responded immediately or if it has to block.
    pub respond_immediately: bool,
    /// The current value of the global timestamp.
    pub global_timestamp: T,
    /// Details about each source.
    pub sources: Vec<TimestampSource<T>>,
}

pub struct TimestampSource<T> {
    pub name: String,
    pub read_frontier: Vec<T>,
    pub write_frontier: Vec<T>,
}

pub trait DisplayableInTimeline {
    fn fmt(&self, timeline: Option<&Timeline>, f: &mut fmt::Formatter) -> fmt::Result;
    fn display<'a>(&'a self, timeline: Option<&'a Timeline>) -> DisplayInTimeline<'a, Self> {
        DisplayInTimeline { t: self, timeline }
    }
}

impl DisplayableInTimeline for mz_repr::Timestamp {
    fn fmt(&self, timeline: Option<&Timeline>, f: &mut fmt::Formatter) -> fmt::Result {
        match timeline {
            Some(Timeline::EpochMilliseconds) => {
                let ts_ms: u64 = self.into();
                let ts = ts_ms / 1000;
                let nanos = ((ts_ms % 1000) as u32) * 1000000;
                let ndt = NaiveDateTime::from_timestamp(ts as i64, nanos);
                write!(f, "{:13} ({})", self, ndt.format("%Y-%m-%d %H:%M:%S%.3f"))
            }
            None | Some(_) => {
                write!(f, "{:13}", self)
            }
        }
    }
}

pub struct DisplayInTimeline<'a, T: ?Sized> {
    t: &'a T,
    timeline: Option<&'a Timeline>,
}
impl<'a, T> fmt::Display for DisplayInTimeline<'a, T>
where
    T: DisplayableInTimeline,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.t.fmt(self.timeline, f)
    }
}

impl<'a, T> fmt::Debug for DisplayInTimeline<'a, T>
where
    T: DisplayableInTimeline,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self, f)
    }
}

impl<T: fmt::Display + fmt::Debug + DisplayableInTimeline> fmt::Display
    for TimestampExplanation<T>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let timeline = self.timeline.as_ref();
        writeln!(
            f,
            "          query timestamp: {}",
            self.timestamp.display(timeline)
        )?;
        writeln!(
            f,
            "                    since:{:?}",
            self.since
                .iter()
                .map(|t| t.display(timeline))
                .collect::<Vec<_>>()
        )?;
        writeln!(
            f,
            "                    upper:{:?}",
            self.upper
                .iter()
                .map(|t| t.display(timeline))
                .collect::<Vec<_>>()
        )?;
        writeln!(
            f,
            "         global timestamp: {}",
            self.global_timestamp.display(timeline)
        )?;
        writeln!(f, "  can respond immediately: {}", self.respond_immediately)?;
        for source in &self.sources {
            writeln!(f, "")?;
            writeln!(f, "source {}:", source.name)?;
            writeln!(
                f,
                "            read frontier:{:?}",
                source
                    .read_frontier
                    .iter()
                    .map(|t| t.display(timeline))
                    .collect::<Vec<_>>()
            )?;
            writeln!(
                f,
                "           write frontier:{:?}",
                source
                    .write_frontier
                    .iter()
                    .map(|t| t.display(timeline))
                    .collect::<Vec<_>>()
            )?;
        }
        Ok(())
    }
}
