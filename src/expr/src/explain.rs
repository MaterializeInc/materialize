// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This is the implementation for the EXPLAIN (DECORRELATED | OPTIMIZED) PLAN command.
//!
//! Conventions:
//! * RelationExprs are printed in post-order, left to right
//! * RelationExprs which only have a single input are grouped together
//! * Each group of RelationExprs is referred by id eg %4
//! * RelationExprs may be followed by additional annotations on lines starting with | |
//! * Columns are referred to by position eg #4
//! * Collections of columns are written as ranges where possible eg "#2..#5"
//!
//! It's important to avoid trailing whitespace everywhere, because it plays havoc with SLT

use std::fmt;

use super::{
    AggregateExpr, Id, IdHumanizer, JoinImplementation, LocalId, RelationExpr, RowSetFinishing,
    ScalarExpr,
};
use repr::RelationType;
use std::collections::HashMap;

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
                writeln!(f, "%{} =", node.chain)?;
            }
            prev_chain = node.chain;

            // skip Let
            if let RelationExpr::Let { .. } = node.expr {
                continue;
            }

            node.fmt(self, f)?;
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

impl<'a> ExplanationNode<'a> {
    fn fmt(&self, explanation: &Explanation, f: &mut fmt::Formatter) -> fmt::Result {
        use RelationExpr::*;

        match self.expr {
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
                    explanation
                        .local_id_chains
                        .get(local_id)
                        .map_or_else(|| "?".to_owned(), |i| i.to_string()),
                    local_id,
                )?,
                Id::Global(id) => writeln!(
                    f,
                    "| Get {} ({})",
                    explanation
                        .id_humanizer
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
                let input_chains = inputs
                    .iter()
                    .map(|inp| explanation.expr_chain(inp))
                    .collect::<Vec<_>>();
                write!(
                    f,
                    "| Join {}",
                    Separated(
                        " ",
                        inputs
                            .iter()
                            .map(|input| Bracketed("%", "", explanation.expr_chain(input)))
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
                for line in implementation.fmt_with(&input_chains) {
                    writeln!(f, "| | {}", line)?;
                }
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
                    .map(|input| Bracketed("%", "", explanation.expr_chain(input)))
                    .collect();
                writeln!(
                    f,
                    "| Union %{} {}",
                    explanation.expr_chain(base),
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

        if let Some(RelationType { column_types, keys }) = &self.typ {
            writeln!(f, "| | types = ({})", Separated(", ", column_types.clone()))?;
            writeln!(
                f,
                "| | keys = ({})",
                Separated(", ", keys.iter().map(|key| Indices(key)).collect())
            )?;
        }

        Ok(())
    }
}

impl RelationExpr {
    /// Create an Explanation, to which annotations can be added before printing
    pub fn explain<'a>(&'a self, id_humanizer: &'a dyn IdHumanizer) -> Explanation<'a> {
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
                Join { inputs, .. } => {
                    // For join, each of the inputs goes in its own chain.
                    for input in inputs {
                        walk(input, explanation);
                        explanation.chain += 1;
                    }
                }
                Union { base, inputs, .. } => {
                    // Ditto for union.
                    walk(base, explanation);
                    explanation.chain += 1;
                    for input in inputs {
                        walk(input, explanation);
                        explanation.chain += 1;
                    }
                }
                Let { id, body, value } => {
                    // Similarly the definition of a let goes in its own chain.
                    walk(value, explanation);
                    explanation.chain += 1;
                    walk(body, explanation);
                    explanation
                        .local_id_chains
                        .insert(*id, explanation.expr_chain(value));
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

        let mut explanation = Explanation {
            id_humanizer,
            nodes: vec![],
            finishing: None,
            expr_chains: HashMap::new(),
            local_id_chains: HashMap::new(),
            chain: 0,
        };
        walk(self, &mut explanation);
        explanation
    }
}

impl<'a> Explanation<'a> {
    /// Retrieves the chain ID for the specified expression.
    ///
    /// The `ExplanationNode` for `expr` must have already been inserted into
    /// the explanation.
    fn expr_chain(&self, expr: &RelationExpr) -> usize {
        self.expr_chains[&(expr as *const RelationExpr)]
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
}

impl fmt::Display for ScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        use ScalarExpr::*;
        match self {
            Column(i) => write!(f, "#{}", i)?,
            Literal(Ok(row), _) => write!(f, "{}", row.unpack_first())?,
            Literal(Err(e), _) => write!(f, "(err: {})", e)?,
            CallNullary(func) => write!(f, "{}()", func)?,
            CallUnary { func, expr } => {
                write!(f, "{}({})", func, expr)?;
            }
            CallBinary { func, expr1, expr2 } => {
                if func.is_infix_op() {
                    write!(f, "({} {} {})", expr1, func, expr2)?;
                } else {
                    write!(f, "{}({}, {})", func, expr1, expr2)?;
                }
            }
            CallVariadic { func, exprs } => {
                write!(f, "{}({})", func, Separated(", ", exprs.clone()))?;
            }
            If { cond, then, els } => {
                write!(f, "if {} then {{{}}} else {{{}}}", cond, then, els)?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for AggregateExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}({}{})",
            self.func,
            if self.distinct { "distinct " } else { "" },
            self.expr
        )
    }
}

impl JoinImplementation {
    fn fmt_with(&self, input_chains: &[usize]) -> Vec<String> {
        use JoinImplementation::*;
        let mut result = match self {
            Differential((pos, first_arr), inputs) => vec![format!(
                "Differential %{}{} {}",
                input_chains[*pos],
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
                                input_chains[*pos],
                                Separated(", ", input.clone())
                            )
                        })
                        .collect()
                )
            )],
            DeltaQuery(inputss) => {
                let mut result = vec!["DeltaQuery".to_owned()];
                result.extend(inputss.iter().enumerate().map(|(pos, inputs)| {
                    format!(
                        "  delta %{} {}",
                        input_chains[pos],
                        Separated(
                            " ",
                            inputs
                                .iter()
                                .map(|(pos, input)| {
                                    format!(
                                        "%{}.({})",
                                        input_chains[*pos],
                                        Separated(", ", input.clone())
                                    )
                                })
                                .collect()
                        )
                    )
                }));
                result
            }
            Unimplemented => vec!["Unimplemented".to_owned()],
        };
        result[0] = format!("implementation = {}", result[0]);
        result
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
