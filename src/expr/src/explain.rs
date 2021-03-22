// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module houses a pretty printer for [`MirRelationExpr`]s.
//!
//! Format details:
//!
//!   * The nodes in a `MirRelationExpr` are printed in post-order, left to right.
//!   * Nodes which only have a single input are grouped together
//!     into "chains."
//!   * Each chain of `MirRelationExpr`s is referred to by ID, e.g. %4.
//!   * Nodes may be followed by additional, indented annotations.
//!   * Columns are referred to by position, e.g. #4.
//!   * Collections of columns are written as ranges where possible,
//!     e.g. "#2..#5".
//!
//! It's important to avoid trailing whitespace everywhere, as plans may be
//! printed in contexts where trailing whitespace is unacceptable, like
//! sqllogictest files.

use std::collections::HashMap;
use std::fmt;
use std::iter;

use ore::str::StrExt;
use repr::RelationType;

use crate::{
    ExprHumanizer, GlobalId, Id, JoinImplementation, LocalId, MirRelationExpr, MirScalarExpr,
    RowSetFinishing,
};

/// An `ViewExplanation` facilitates pretty-printing of a [`MirRelationExpr`].
#[derive(Debug)]
struct ViewExplanation<'a> {
    /// One `ExplanationNode` for each `MirRelationExpr` in the plan, in
    /// left-to-right post-order.
    nodes: Vec<ExplanationNode<'a>>,
    /// Records the chain ID that was assigned to each expression.
    expr_chains: HashMap<*const MirRelationExpr, usize>,
    /// Records the chain ID that was assigned to each let.
    local_id_chains: HashMap<LocalId, usize>,
    /// Records the local ID that corresponds to a chain ID, if any.
    chain_local_ids: HashMap<usize, LocalId>,
    /// The ID of the current chain. Incremented while constructing the
    /// `Explanation`.
    chain: usize,
}

/// An `Explanation` facilitates pretty-printing of a `DataflowDesc`.
///
/// Only the parts of the `DataflowDesc` that are relevant to seeing how a
/// view/query is planned are printed. Because the `dataflow-types` crate
/// depends on the `expr` crate and not the other way around, only components of
/// the `DataflowDesc` visible to the `expr` crate are stored in this struct.
///
/// By default, the [`fmt::Display`] implementation renders the expression as
/// described in the module docs. Additional information may be attached to the
/// explanation via the other public methods on the type.
#[derive(Debug)]
pub struct Explanation<'a> {
    expr_humanizer: &'a dyn ExprHumanizer,
    /// Each source with some `LinearOperator`.
    /// The tuple is (id, predicates, projection)
    sources: Vec<(GlobalId, &'a Vec<MirScalarExpr>, &'a Vec<usize>)>,
    /// One `ViewExplanation` per view in the dataflow.
    views: Vec<(GlobalId, ViewExplanation<'a>)>,
    /// An optional `RowSetFinishing` to mention at the end.
    finishing: Option<RowSetFinishing>,
    /// The view currently being explained.
    current_view: ViewExplanation<'a>,
}

#[derive(Debug)]
pub struct ExplanationNode<'a> {
    /// The expression being explained.
    pub expr: &'a MirRelationExpr,
    /// The type of the expression, if desired.
    pub typ: Option<RelationType>,
    /// The ID of the linear chain to which this node belongs.
    pub chain: usize,
}

impl<'a> fmt::Display for Explanation<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut prev_chain = usize::max_value();
        for (id, predicates, projection) in &self.sources {
            writeln!(
                f,
                "Source {} ({}):",
                self.expr_humanizer
                    .humanize_id(*id)
                    .unwrap_or_else(|| "?".to_owned()),
                id,
            )?;
            writeln!(f, "| Filter {}", separated(", ", predicates.iter()))?;
            writeln!(f, "| Project {}", bracketed("(", ")", Indices(projection)))?;
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
            for node in view.nodes.iter() {
                if node.chain != prev_chain {
                    if node.chain != 0 {
                        writeln!(f)?;
                    }
                    write!(f, "%{} =", node.chain)?;
                    if let Some(local_id) = view.chain_local_ids.get(&node.chain) {
                        write!(f, " Let {} =", local_id)?;
                    }
                    writeln!(f)?;
                }
                prev_chain = node.chain;

                view.fmt_node(f, node, self.expr_humanizer)?;
            }
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

impl<'a> Explanation<'a> {
    /// Creates an explanation for a [`MirRelationExpr`].
    pub fn new(
        expr: &'a MirRelationExpr,
        expr_humanizer: &'a dyn ExprHumanizer,
    ) -> Explanation<'a> {
        let mut explanation = Explanation {
            expr_humanizer,
            sources: vec![],
            views: vec![],
            finishing: None,
            current_view: ViewExplanation::new(),
        };
        explanation.current_view.walk(expr);
        explanation.views.push((
            GlobalId::Explain,
            std::mem::replace(&mut explanation.current_view, ViewExplanation::new()),
        ));
        explanation
    }

    pub fn new_from_dataflow(
        sources: impl Iterator<Item = (GlobalId, &'a Vec<MirScalarExpr>, &'a Vec<usize>)>,
        views: impl Iterator<Item = (GlobalId, &'a MirRelationExpr)>,
        expr_humanizer: &'a dyn ExprHumanizer,
    ) -> Explanation<'a> {
        let mut explanation = Explanation {
            expr_humanizer,
            sources: sources.collect::<Vec<_>>(),
            views: vec![],
            finishing: None,
            current_view: ViewExplanation::new(),
        };
        for (id, expr) in views {
            explanation.current_view.walk(expr);
            explanation.views.push((
                id,
                std::mem::replace(&mut explanation.current_view, ViewExplanation::new()),
            ));
        }
        explanation
    }

    /// Attach type information into the explanation.
    pub fn explain_types(&mut self) {
        for (_, view) in &mut self.views {
            for node in view.nodes.iter_mut() {
                // TODO(jamii) `typ` is itself recursive, so this is quadratic :(
                node.typ = Some(node.expr.typ());
            }
        }
    }

    /// Attach a `RowSetFinishing` to the explanation.
    pub fn explain_row_set_finishing(&mut self, finishing: RowSetFinishing) {
        self.finishing = Some(finishing);
    }
}

impl<'a> ViewExplanation<'a> {
    fn new() -> ViewExplanation<'a> {
        ViewExplanation {
            nodes: vec![],
            expr_chains: HashMap::new(),
            local_id_chains: HashMap::new(),
            chain_local_ids: HashMap::new(),
            chain: 0,
        }
    }

    // Do a post-order traversal of the expression, grouping "chains" of
    // views together as we go. We have to break the chain whenever we
    // encounter a node with multiple inputs, like a join.
    fn walk(&mut self, expr: &'a MirRelationExpr) {
        use MirRelationExpr::*;
        // First, walk the children, in order to perform a post-order
        // traversal.
        match expr {
            // Leaf expressions. Nothing more to visit.
            // TODO [btv] Explain complex sources.
            Constant { .. } | Get { .. } => (),
            // Single-input expressions continue the chain.
            Project { input, .. }
            | Map { input, .. }
            | FlatMap { input, .. }
            | Filter { input, .. }
            | Reduce { input, .. }
            | TopK { input, .. }
            | Negate { input, .. }
            | Threshold { input, .. }
            | DeclareKeys { input, .. }
            | ArrangeBy { input, .. } => self.walk(input),
            // For join and union, each input may need to go in its own
            // chain.
            Join { inputs, .. } => self.walk_many(inputs),
            Union { base, inputs, .. } => self.walk_many(iter::once(&**base).chain(inputs)),
            Let { id, body, value } => {
                // Similarly the definition of a let goes in its own chain.
                self.walk(value);
                self.chain += 1;

                // Keep track of the chain ID <-> local ID correspondence.
                let value_chain = self.expr_chain(value);
                self.local_id_chains.insert(*id, value_chain);
                self.chain_local_ids.insert(value_chain, *id);

                self.walk(body);
            }
        }

        // Then record the node.
        self.nodes.push(ExplanationNode {
            expr,
            typ: None,
            chain: self.chain,
        });
        self.expr_chains
            .insert(expr as *const MirRelationExpr, self.chain);
    }

    fn walk_many<E>(&mut self, exprs: E)
    where
        E: IntoIterator<Item = &'a MirRelationExpr>,
    {
        for expr in exprs {
            // Elide chains that would consist only a of single Get node.
            if let MirRelationExpr::Get {
                id: Id::Local(id), ..
            } = expr
            {
                self.expr_chains
                    .insert(expr as *const MirRelationExpr, self.local_id_chains[id]);
            } else {
                self.walk(expr);
                self.chain += 1;
            }
        }
    }

    fn fmt_node(
        &self,
        f: &mut fmt::Formatter,
        node: &ExplanationNode,
        expr_humanizer: &'a dyn ExprHumanizer,
    ) -> fmt::Result {
        use MirRelationExpr::*;

        match node.expr {
            Constant { rows, .. } => {
                write!(f, "| Constant")?;
                match rows {
                    Ok(rows) if !rows.is_empty() => writeln!(
                        f,
                        " {}",
                        separated(
                            " ",
                            rows.iter()
                                .flat_map(|(row, count)| (0..*count).map(move |_| row))
                        )
                    )?,
                    Ok(_) => writeln!(f)?,
                    Err(e) => writeln!(f, " Err({})", e.to_string().quoted())?,
                }
            }
            Get { id, .. } => match id {
                Id::Local(local_id) => writeln!(
                    f,
                    "| Get %{} ({})",
                    self.local_id_chains
                        .get(local_id)
                        .map_or_else(|| "?".to_owned(), |i| i.to_string()),
                    local_id,
                )?,
                Id::Global(id) => writeln!(
                    f,
                    "| Get {} ({})",
                    expr_humanizer
                        .humanize_id(*id)
                        .unwrap_or_else(|| "?".to_owned()),
                    id,
                )?,
                Id::LocalBareSource => writeln!(f, "| Get Bare Source for This Source")?,
            },
            // Lets are annotated on the chain ID that they correspond to.
            Let { .. } => (),
            Project { outputs, .. } => {
                writeln!(f, "| Project {}", bracketed("(", ")", Indices(outputs)))?
            }
            Map { scalars, .. } => writeln!(f, "| Map {}", separated(", ", scalars))?,
            FlatMap {
                func,
                exprs,
                demand,
                ..
            } => {
                writeln!(f, "| FlatMap {}({})", func, separated(", ", exprs))?;
                if let Some(demand) = demand {
                    writeln!(f, "| | demand = {}", bracketed("(", ")", Indices(demand)))?;
                }
            }
            Filter { predicates, .. } => writeln!(f, "| Filter {}", separated(", ", predicates))?,
            Join {
                inputs,
                equivalences,
                demand,
                implementation,
            } => {
                write!(
                    f,
                    "| Join {}",
                    separated(
                        " ",
                        inputs
                            .iter()
                            .map(|input| bracketed("%", "", self.expr_chain(input)))
                    ),
                )?;
                if !equivalences.is_empty() {
                    write!(
                        f,
                        " {}",
                        separated(
                            " ",
                            equivalences.iter().map(|equivalence| bracketed(
                                "(= ",
                                ")",
                                separated(" ", equivalence)
                            ))
                        )
                    )?;
                }
                writeln!(f)?;
                write!(f, "| | implementation = ")?;
                self.fmt_join_implementation(f, inputs, implementation)?;
                if let Some(demand) = demand {
                    writeln!(f, "| | demand = {}", bracketed("(", ")", Indices(demand)))?;
                }
            }
            Reduce {
                group_key,
                aggregates,
                ..
            } => {
                if aggregates.is_empty() {
                    writeln!(
                        f,
                        "| Distinct group={}",
                        bracketed("(", ")", separated(", ", group_key)),
                    )?
                } else {
                    writeln!(
                        f,
                        "| Reduce group={}",
                        bracketed("(", ")", separated(", ", group_key)),
                    )?;
                    for agg in aggregates {
                        writeln!(f, "| | agg {}", agg)?;
                    }
                }
            }
            TopK {
                group_key,
                order_key,
                limit,
                offset,
                ..
            } => {
                write!(
                    f,
                    "| TopK group={} order={}",
                    bracketed("(", ")", Indices(group_key)),
                    bracketed("(", ")", separated(", ", order_key)),
                )?;
                if let Some(limit) = limit {
                    write!(f, " limit={}", limit)?;
                }
                writeln!(f, " offset={}", offset)?
            }
            Negate { .. } => writeln!(f, "| Negate")?,
            Threshold { .. } => write!(f, "| Threshold")?,
            DeclareKeys { input: _, keys } => write!(
                f,
                "| Declare primary keys {}",
                separated(
                    " ",
                    keys.iter()
                        .map(|key| bracketed("(", ")", separated(", ", key)))
                )
            )?,
            Union { base, inputs } => writeln!(
                f,
                "| Union %{} {}",
                self.expr_chain(base),
                separated(
                    " ",
                    inputs
                        .iter()
                        .map(|input| bracketed("%", "", self.expr_chain(input)))
                )
            )?,
            ArrangeBy { keys, .. } => writeln!(
                f,
                "| ArrangeBy {}",
                separated(
                    " ",
                    keys.iter()
                        .map(|key| bracketed("(", ")", separated(", ", key)))
                ),
            )?,
        }

        if let Some(RelationType { column_types, keys }) = &node.typ {
            let column_types: Vec<_> = column_types
                .iter()
                .map(|c| expr_humanizer.humanize_column_type(c))
                .collect();
            writeln!(f, "| | types = ({})", separated(", ", column_types))?;
            writeln!(
                f,
                "| | keys = ({})",
                separated(
                    ", ",
                    keys.iter().map(|key| bracketed("(", ")", Indices(key)))
                )
            )?;
        }

        Ok(())
    }

    fn fmt_join_implementation(
        &self,
        f: &mut fmt::Formatter,
        join_inputs: &[MirRelationExpr],
        implementation: &JoinImplementation,
    ) -> fmt::Result {
        match implementation {
            JoinImplementation::Differential((pos, first_arr), inputs) => writeln!(
                f,
                "Differential %{}{} {}",
                self.expr_chain(&join_inputs[*pos]),
                if let Some(arr) = first_arr {
                    format!(".({})", separated(", ", arr))
                } else {
                    "".to_string()
                },
                separated(
                    " ",
                    inputs.iter().map(|(pos, input)| {
                        format!(
                            "%{}.({})",
                            self.expr_chain(&join_inputs[*pos]),
                            separated(", ", input)
                        )
                    })
                ),
            ),
            JoinImplementation::DeltaQuery(inputs) => {
                writeln!(f, "DeltaQuery")?;
                for (pos, inputs) in inputs.iter().enumerate() {
                    writeln!(
                        f,
                        "| |   delta %{} {}",
                        self.expr_chain(&join_inputs[pos]),
                        separated(
                            " ",
                            inputs.iter().map(|(pos, input)| {
                                format!(
                                    "%{}.({})",
                                    self.expr_chain(&join_inputs[*pos]),
                                    separated(", ", input)
                                )
                            })
                        )
                    )?;
                }
                Ok(())
            }
            JoinImplementation::Unimplemented => writeln!(f, "Unimplemented"),
        }
    }

    /// Retrieves the chain ID for the specified expression.
    ///
    /// The `ExplanationNode` for `expr` must have already been inserted into
    /// the explanation.
    fn expr_chain(&self, expr: &MirRelationExpr) -> usize {
        self.expr_chains[&(expr as *const MirRelationExpr)]
    }
}

/// Creates a type whose [`fmt::Display`] implementation outputs each item in
/// `iter` separated by `separator`.
pub fn separated<'a, I>(separator: &'a str, iter: I) -> impl fmt::Display + 'a
where
    I: IntoIterator,
    I::IntoIter: Clone + 'a,
    I::Item: fmt::Display + 'a,
{
    struct Separated<'a, I> {
        separator: &'a str,
        iter: I,
    }

    impl<'a, I> fmt::Display for Separated<'a, I>
    where
        I: Iterator + Clone,
        I::Item: fmt::Display,
    {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            for (i, item) in self.iter.clone().enumerate() {
                if i != 0 {
                    write!(f, "{}", self.separator)?;
                }
                write!(f, "{}", item)?;
            }
            Ok(())
        }
    }

    Separated {
        separator,
        iter: iter.into_iter(),
    }
}

/// Creates a type whose [`fmt::Display`] implementation outputs item preceded
/// by `open` and followed by `close`.
pub fn bracketed<'a, D>(open: &'a str, close: &'a str, contents: D) -> impl fmt::Display + 'a
where
    D: fmt::Display + 'a,
{
    struct Bracketed<'a, D> {
        open: &'a str,
        close: &'a str,
        contents: D,
    }

    impl<'a, D> fmt::Display for Bracketed<'a, D>
    where
        D: fmt::Display,
    {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{}{}{}", self.open, self.contents, self.close)
        }
    }

    Bracketed {
        open,
        close,
        contents,
    }
}

/// Pretty-prints a list of indices.
#[derive(Debug)]
pub struct Indices<'a>(pub &'a [usize]);

impl<'a> fmt::Display for Indices<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut is_first = true;
        let mut slice = self.0;
        while !slice.is_empty() {
            if !is_first {
                write!(f, ", ")?;
            }
            is_first = false;
            let lead = &slice[0];
            if slice.len() > 2 && slice[1] == lead + 1 && slice[2] == lead + 2 {
                let mut last = 3;
                while slice.get(last) == Some(&(lead + last)) {
                    last += 1;
                }
                write!(f, "#{}..#{}", lead, lead + last - 1)?;
                slice = &slice[last..];
            } else {
                write!(f, "#{}", slice[0])?;
                slice = &slice[1..];
            }
        }
        Ok(())
    }
}
