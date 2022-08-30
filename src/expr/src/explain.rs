// Copyright Materialize, Inc. and contributors. All rights reserved.
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

use mz_ore::str::{bracketed, separated, StrExt};
use mz_repr::explain_new::ExprHumanizer;
use mz_repr::RelationType;

use crate::{Id, JoinImplementation, LocalId, MirRelationExpr};

/// An `ViewExplanation` facilitates pretty-printing of a [`MirRelationExpr`].
///
/// By default, the [`fmt::Display`] implementation renders the expression as
/// described in the module docs. Additional information may be attached to the
/// explanation via the other public methods on the type.
#[derive(Debug)]
pub struct ViewExplanation<'a> {
    expr_humanizer: &'a dyn ExprHumanizer,
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

#[derive(Debug)]
pub struct ExplanationNode<'a> {
    /// The expression being explained.
    pub expr: &'a MirRelationExpr,
    /// The type of the expression, if desired.
    pub typ: Option<RelationType>,
    /// The ID of the linear chain to which this node belongs.
    pub chain: usize,
}

impl<'a> fmt::Display for ViewExplanation<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut prev_chain = usize::max_value();
        for node in &self.nodes {
            if node.chain != prev_chain {
                if node.chain != 0 {
                    writeln!(f)?;
                }
                write!(f, "%{} =", node.chain)?;
                if let Some(local_id) = self.chain_local_ids.get(&node.chain) {
                    write!(f, " Let {} =", local_id)?;
                }
                writeln!(f)?;
            }
            prev_chain = node.chain;

            self.fmt_node(f, node)?;
        }
        Ok(())
    }
}

impl<'a> ViewExplanation<'a> {
    pub fn new(
        expr: &'a MirRelationExpr,
        expr_humanizer: &'a dyn ExprHumanizer,
    ) -> ViewExplanation<'a> {
        use MirRelationExpr::*;

        // Do a post-order traversal of the expression, grouping "chains" of
        // nodes together as we go. We have to break the chain whenever we
        // encounter a node with multiple inputs, like a join.

        fn walk<'a>(expr: &'a MirRelationExpr, explanation: &mut ViewExplanation<'a>) {
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
                | ArrangeBy { input, .. } => walk(input, explanation),
                // For join and union, each input may need to go in its own
                // chain.
                Join { inputs, .. } => walk_many(inputs, explanation),
                Union { base, inputs, .. } => {
                    walk_many(iter::once(&**base).chain(inputs), explanation)
                }
                Let { id, body, value } => {
                    // Similarly the definition of a let goes in its own chain.
                    walk(value, explanation);
                    explanation.chain += 1;

                    // Keep track of the chain ID <-> local ID correspondence.
                    let value_chain = explanation.expr_chain(value);
                    explanation.local_id_chains.insert(*id, value_chain);
                    explanation.chain_local_ids.insert(value_chain, *id);

                    walk(body, explanation);
                }
            }

            // Then record the node.
            explanation.nodes.push(ExplanationNode {
                expr,
                typ: None,
                chain: explanation.chain,
            });
            explanation
                .expr_chains
                .insert(expr as *const MirRelationExpr, explanation.chain);
        }

        fn walk_many<'a, E>(exprs: E, explanation: &mut ViewExplanation<'a>)
        where
            E: IntoIterator<Item = &'a MirRelationExpr>,
        {
            for expr in exprs {
                // Elide chains that would consist only a of single Get node.
                if let MirRelationExpr::Get {
                    id: Id::Local(id), ..
                } = expr
                {
                    explanation.expr_chains.insert(
                        expr as *const MirRelationExpr,
                        explanation.local_id_chains[id],
                    );
                } else {
                    walk(expr, explanation);
                    explanation.chain += 1;
                }
            }
        }

        let mut explanation = ViewExplanation {
            expr_humanizer,
            nodes: vec![],
            expr_chains: HashMap::new(),
            local_id_chains: HashMap::new(),
            chain_local_ids: HashMap::new(),
            chain: 0,
        };
        walk(expr, &mut explanation);
        explanation
    }

    /// Attach type information into the explanation.
    pub fn explain_types(&mut self) {
        for node in &mut self.nodes {
            if let MirRelationExpr::Let { .. } = &node.expr {
                // Skip.
                // Since we don't print out Let nodes in the explanation,
                // types of Let nodes should not be attached to the
                // explanation. The type information of a Let is always the
                // same as the the type of the body.
            } else {
                // TODO(jamii) `typ` is itself recursive, so this is quadratic :(
                node.typ = Some(node.expr.typ());
            }
        }
    }

    fn fmt_node(&self, f: &mut fmt::Formatter, node: &ExplanationNode) -> fmt::Result {
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
                            rows.iter().map(|(row, count)| if *count == 1 {
                                format!("{row}")
                            } else {
                                format!("({row} x {count})")
                            })
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
                    self.expr_humanizer
                        .humanize_id(*id)
                        .unwrap_or_else(|| "?".to_owned()),
                    id,
                )?,
            },
            // Lets are annotated on the chain ID that they correspond to.
            Let { .. } => (),
            Project { outputs, .. } => {
                writeln!(f, "| Project {}", bracketed("(", ")", Indices(outputs)))?
            }
            Map { scalars, .. } => writeln!(f, "| Map {}", separated(", ", scalars))?,
            FlatMap { func, exprs, .. } => {
                writeln!(f, "| FlatMap {}({})", func, separated(", ", exprs))?;
            }
            Filter { predicates, .. } => writeln!(f, "| Filter {}", separated(", ", predicates))?,
            Join {
                inputs,
                equivalences,
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
            Threshold { .. } => writeln!(f, "| Threshold")?,
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
                .map(|c| self.expr_humanizer.humanize_column_type(c))
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
            JoinImplementation::IndexedFilter(_, key, vals) => {
                writeln!(
                    f,
                    "IndexedFilter {}",
                    separated(
                        " OR ",
                        vals.iter().map(|row| format!(
                            "({})",
                            separated(
                                " AND ",
                                key.iter()
                                    .zip(row.unpack().into_iter())
                                    .map(|(k, v)| format!("{} = {}", k, v))
                            )
                        ))
                    )
                )
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
                write!(f, "#{}..=#{}", lead, lead + last - 1)?;
                slice = &slice[last..];
            } else {
                write!(f, "#{}", slice[0])?;
                slice = &slice[1..];
            }
        }
        Ok(())
    }
}
