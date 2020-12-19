// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module houses a pretty printer for [`RelationExpr`]s.
//!
//! Format details:
//!
//!   * The nodes in a `RelationExpr` are printed in post-order, left to right.
//!   * Nodes which only have a single input are grouped together
//!     into "chains."
//!   * Each chain of `RelationExpr`s is referred to by ID, e.g. %4.
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

use repr::RelationType;

use crate::{Id, IdHumanizer, JoinImplementation, LocalId, RelationExpr, RowSetFinishing};

/// An `Explanation` facilitates pretty-printing of a [`RelationExpr`].
///
/// By default, the [`fmt::Display`] implementation renders the expression as
/// described in the module docs. Additional information may be attached to the
/// explanation via the other public methods on the type.
#[derive(Debug)]
pub struct Explanation<'a> {
    id_humanizer: &'a dyn IdHumanizer,
    /// One `ExplanationNode` for each `RelationExpr` in the plan, in
    /// left-to-right post-order.
    nodes: Vec<ExplanationNode<'a>>,
    /// An optional `RowSetFinishing` to mention at the end.
    finishing: Option<RowSetFinishing>,
    /// Records the chain ID that was assigned to each expression.
    expr_chains: HashMap<*const RelationExpr, usize>,
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
    pub expr: &'a RelationExpr,
    /// The type of the expression, if desired.
    pub typ: Option<RelationType>,
    /// The ID of the linear chain to which this node belongs.
    pub chain: usize,
}

impl<'a> fmt::Display for Explanation<'a> {
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

        if let Some(finishing) = &self.finishing {
            writeln!(
                f,
                "\nFinish order_by={} limit={} offset={} project={}",
                Bracketed("(", ")", Separated(", ", finishing.order_by.clone())),
                match finishing.limit {
                    Some(limit) => limit.to_string(),
                    None => "none".to_owned(),
                },
                finishing.offset,
                Bracketed("(", ")", Indices(&finishing.project))
            )?;
        }

        Ok(())
    }
}

impl<'a> Explanation<'a> {
    /// Creates an explanation for a [`RelationExpr`].
    pub fn new(expr: &'a RelationExpr, id_humanizer: &'a dyn IdHumanizer) -> Explanation<'a> {
        use RelationExpr::*;

        // Do a post-order traversal of the expression, grouping "chains" of
        // nodes together as we go. We have to break the chain whenever we
        // encounter a node with multiple inputs, like a join.

        fn walk<'a>(expr: &'a RelationExpr, explanation: &mut Explanation<'a>) {
            // First, walk the children, in order to perform a post-order
            // traversal.
            match expr {
                // Leaf expressions. Nothing more to visit.
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
                .insert(expr as *const RelationExpr, explanation.chain);
        }

        fn walk_many<'a, E>(exprs: E, explanation: &mut Explanation<'a>)
        where
            E: IntoIterator<Item = &'a RelationExpr>,
        {
            for expr in exprs {
                // Elide chains that would consist only a of single Get node.
                if let RelationExpr::Get {
                    id: Id::Local(id), ..
                } = expr
                {
                    explanation
                        .expr_chains
                        .insert(expr as *const RelationExpr, explanation.local_id_chains[id]);
                } else {
                    walk(expr, explanation);
                    explanation.chain += 1;
                }
            }
        }

        let mut explanation = Explanation {
            id_humanizer,
            nodes: vec![],
            finishing: None,
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
            // TODO(jamii) `typ` is itself recursive, so this is quadratic :(
            node.typ = Some(node.expr.typ());
        }
    }

    /// Attach a `RowSetFinishing` to the explanation.
    pub fn explain_row_set_finishing(&mut self, finishing: RowSetFinishing) {
        self.finishing = Some(finishing);
    }

    fn fmt_node(&self, f: &mut fmt::Formatter, node: &ExplanationNode) -> fmt::Result {
        use RelationExpr::*;

        match node.expr {
            Constant { rows, .. } => writeln!(
                f,
                "| Constant {}",
                Separated(
                    " ",
                    rows.iter()
                        .flat_map(|(row, count)| (0..*count).map(move |_| row))
                        .collect::<Vec<_>>()
                )
            )?,
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
                    self.id_humanizer
                        .humanize_id(*id)
                        .unwrap_or_else(|| "?".to_owned()),
                    id,
                )?,
            },
            // Lets are annotated on the chain ID that they correspond to.
            Let { .. } => (),
            Project { outputs, .. } => {
                writeln!(f, "| Project {}", Bracketed("(", ")", Indices(outputs)))?
            }
            Map { scalars, .. } => writeln!(f, "| Map {}", Separated(", ", scalars.clone()))?,
            FlatMap {
                func,
                exprs,
                demand,
                ..
            } => {
                writeln!(f, "| FlatMap {}({})", func, Separated(", ", exprs.clone()))?;
                if let Some(demand) = demand {
                    writeln!(f, "| | demand = {}", Bracketed("(", ")", Indices(demand)))?;
                }
            }
            Filter { predicates, .. } => {
                writeln!(f, "| Filter {}", Separated(", ", predicates.clone()))?
            }
            Join {
                inputs,
                equivalences,
                demand,
                implementation,
            } => {
                write!(
                    f,
                    "| Join {}",
                    Separated(
                        " ",
                        inputs
                            .iter()
                            .map(|input| Bracketed("%", "", self.expr_chain(input)))
                            .collect()
                    ),
                )?;
                if !equivalences.is_empty() {
                    write!(
                        f,
                        " {}",
                        Separated(
                            " ",
                            equivalences
                                .iter()
                                .map(|equivalence| Bracketed(
                                    "(= ",
                                    ")",
                                    Separated(" ", equivalence.clone())
                                ))
                                .collect()
                        )
                    )?;
                }
                writeln!(f)?;
                write!(f, "| | implementation = ")?;
                self.fmt_join_implementation(f, inputs, implementation)?;
                if let Some(demand) = demand {
                    writeln!(f, "| | demand = {}", Bracketed("(", ")", Indices(demand)))?;
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
                        Bracketed("(", ")", Separated(", ", group_key.clone())),
                    )?
                } else {
                    writeln!(
                        f,
                        "| Reduce group={}",
                        Bracketed("(", ")", Separated(", ", group_key.clone())),
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
                    Bracketed("(", ")", Indices(group_key)),
                    Bracketed("(", ")", Separated(", ", order_key.clone())),
                )?;
                if let Some(limit) = limit {
                    write!(f, " limit={}", limit)?;
                }
                writeln!(f, " offset={}", offset)?
            }
            Negate { .. } => writeln!(f, "| Negate")?,
            Threshold { .. } => write!(f, "| Threshold")?,
            Union { base, inputs } => {
                let input_chains: Vec<_> = inputs
                    .iter()
                    .map(|input| Bracketed("%", "", self.expr_chain(input)))
                    .collect();
                writeln!(
                    f,
                    "| Union %{} {}",
                    self.expr_chain(base),
                    Separated(" ", input_chains)
                )?
            }
            ArrangeBy { keys, .. } => writeln!(
                f,
                "| ArrangeBy {}",
                Separated(
                    " ",
                    keys.iter()
                        .map(|key| Bracketed("(", ")", Separated(", ", key.clone())))
                        .collect::<Vec<_>>()
                ),
            )?,
        }

        if let Some(RelationType { column_types, keys }) = &node.typ {
            writeln!(f, "| | types = ({})", Separated(", ", column_types.clone()))?;
            writeln!(
                f,
                "| | keys = ({})",
                Separated(", ", keys.iter().map(|key| Indices(key)).collect())
            )?;
        }

        Ok(())
    }

    fn fmt_join_implementation(
        &self,
        f: &mut fmt::Formatter,
        join_inputs: &[RelationExpr],
        implementation: &JoinImplementation,
    ) -> fmt::Result {
        match implementation {
            JoinImplementation::Differential((pos, first_arr), inputs) => writeln!(
                f,
                "Differential %{}{} {}",
                self.expr_chain(&join_inputs[*pos]),
                if let Some(arr) = first_arr {
                    format!(".({})", Separated(", ", arr.clone()))
                } else {
                    "".to_string()
                },
                Separated(
                    " ",
                    inputs
                        .iter()
                        .map(|(pos, input)| {
                            format!(
                                "%{}.({})",
                                self.expr_chain(&join_inputs[*pos]),
                                Separated(", ", input.clone())
                            )
                        })
                        .collect()
                ),
            ),
            JoinImplementation::DeltaQuery(inputs) => {
                writeln!(f, "DeltaQuery")?;
                for (pos, inputs) in inputs.iter().enumerate() {
                    writeln!(
                        f,
                        "| |   delta %{} {}",
                        self.expr_chain(&join_inputs[pos]),
                        Separated(
                            " ",
                            inputs
                                .iter()
                                .map(|(pos, input)| {
                                    format!(
                                        "%{}.({})",
                                        self.expr_chain(&join_inputs[*pos]),
                                        Separated(", ", input.clone())
                                    )
                                })
                                .collect()
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
    fn expr_chain(&self, expr: &RelationExpr) -> usize {
        self.expr_chains[&(expr as *const RelationExpr)]
    }
}

#[derive(Debug)]
pub struct Separated<'a, T>(pub &'a str, pub Vec<T>);

impl<'a, T> fmt::Display for Separated<'a, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        for (i, elem) in self.1.iter().enumerate() {
            if i != 0 {
                write!(f, "{}", self.0)?;
            }
            write!(f, "{}", elem)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Bracketed<'a, T>(pub &'a str, pub &'a str, pub T);

impl<'a, T> fmt::Display for Bracketed<'a, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}{}{}", self.0, self.2, self.1)
    }
}

#[derive(Debug)]
pub struct Indices<'a>(pub &'a [usize]);

impl<'a> fmt::Display for Indices<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
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
