// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This is the implementation for the EXPLAIN RAW PLAN command.
//!
//! Conventions:
//! * RelationExprs are printed in post-order, left to right
//! * RelationExprs which only have a single input are grouped together
//! * Each group of RelationExprs is referred by id eg %4
//! * RelationExprs may be followed by additional annotations on lines starting with | |
//! * Columns are referred by position eg #4
//! * References to columns in outer scopes are indicated by adding a ^ per level of nesting eg #^^4
//! * Collections of columns are written as ranges where possible eg "#2..#5"
//!
//! It's important to avoid trailing whitespace everywhere, because it plays havoc with SLT

use crate::expr::{AggregateExpr, JoinKind, RelationExpr, ScalarExpr};
use expr::{Id, IdHumanizer};
use repr::{RelationType, ScalarType};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug)]
pub struct Explanation<'a> {
    /// One ExplanationNode for each RelationExpr in the plan, in left-to-right post-order
    pub nodes: Vec<ExplanationNode<'a>>,
}

#[derive(Debug)]
pub struct ExplanationNode<'a> {
    /// The expr being explained
    pub expr: &'a RelationExpr,
    /// The parent of expr
    pub parent_expr: Option<&'a RelationExpr>,
    /// A pretty-printed representation of the expr
    pub pretty: String,
    /// A list of annotations containing extra information about this expr
    pub annotations: Vec<String>,
    /// Nodes are grouped into chains of linear operations for easy printing
    pub chain: usize,
}

impl<'a> std::fmt::Display for Explanation<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        let mut prev_chain = usize::max_value();
        for node in &self.nodes {
            if node.chain != prev_chain {
                if node.chain != 0 {
                    writeln!(f)?;
                }
                writeln!(f, "{} =", node.chain)?;
            }
            prev_chain = node.chain;

            // explain output shows up in SLT where the linter will not allow trailing whitespace, so trim stuff
            writeln!(f, "| {}", node.pretty.trim())?;
            for annotation in &node.annotations {
                writeln!(f, "| | {}", annotation.trim())?;
            }
        }

        Ok(())
    }
}

impl RelationExpr {
    /// Create an Explanation, to which annotations can be added before printing
    pub fn explain(&self, id_humanizer: &impl IdHumanizer) -> Explanation {
        use RelationExpr::*;

        // get nodes in post-order
        let mut nodes = vec![];
        let mut stack: Vec<(Option<&RelationExpr>, &RelationExpr)> = vec![(None, self)];
        while let Some((parent_expr, expr)) = stack.pop() {
            nodes.push(ExplanationNode {
                expr,
                parent_expr,
                pretty: String::new(),
                annotations: vec![],
                chain: 0, // will fix this up later
            });
            expr.visit1(&mut |child_expr| {
                stack.push((Some(expr), child_expr));
            });
        }
        nodes.reverse();

        // map from RelationExpr nodes to chain used in the explanation
        // keyed by expr identity, not by expr value
        let mut expr_chain: HashMap<*const RelationExpr, usize> = HashMap::new();

        // group into linear chains of exprs
        let mut chain = 0;
        for node in &mut nodes {
            node.chain = chain;
            let breaks_chain = match &node.parent_expr {
                None => true,
                Some(parent_expr) => match parent_expr {
                    Project { .. }
                    | Map { .. }
                    | FlatMapUnary { .. }
                    | Filter { .. }
                    | Reduce { .. }
                    | TopK { .. }
                    | Negate { .. }
                    | Threshold { .. }
                    | Distinct { .. } => false,
                    Join { .. } | Union { .. } => true,
                    Constant { .. } | Get { .. } => unreachable!(), // these don't have children
                },
            };
            if breaks_chain {
                expr_chain.insert(node.expr as *const RelationExpr, chain);
                chain += 1;
            }
        }

        // slighly easier to use
        let expr_chain = |expr: &RelationExpr| expr_chain[&(expr as *const RelationExpr)];

        for ExplanationNode {
            expr,
            pretty,
            annotations,
            ..
        } in nodes.iter_mut()
        {
            use std::fmt::Write;

            // write the expr
            match expr {
                Constant { rows, .. } => {
                    write!(pretty, "Constant {}", Separated(" ", rows.clone())).unwrap();
                }
                Get { id, .. } => match id {
                    Id::Local(_) => {
                        unimplemented!("sql::RelationExpr::Get can't contain LocalId yet")
                    }
                    Id::Global(_) => write!(
                        pretty,
                        "Get {} ({})",
                        id_humanizer
                            .humanize_id(*id)
                            .unwrap_or_else(|| "?".to_owned()),
                        id,
                    )
                    .unwrap(),
                },
                Project { outputs, .. } => {
                    write!(pretty, "Project {}", Bracketed("(", ")", Indices(outputs))).unwrap()
                }
                Map { scalars, .. } => {
                    write!(pretty, "Map {}", Separated(" ", scalars.clone())).unwrap();
                }
                FlatMapUnary { func, expr, .. } => {
                    write!(pretty, "FlatMapUnary {}({})", func, expr).unwrap();
                }
                Filter { predicates, .. } => {
                    write!(pretty, "Filter {}", Separated(" ", predicates.clone())).unwrap();
                }
                Join {
                    left,
                    right,
                    on,
                    kind,
                } => {
                    write!(
                        pretty,
                        "{}Join {} {} on {}",
                        kind,
                        expr_chain(left),
                        expr_chain(right),
                        on
                    )
                    .unwrap();
                }
                Reduce {
                    group_key,
                    aggregates,
                    ..
                } => {
                    write!(
                        pretty,
                        "Reduce group={} {}",
                        Bracketed("(", ")", Separated(", ", group_key.clone())),
                        Separated(" ", aggregates.clone())
                    )
                    .unwrap();
                }
                Distinct { .. } => {
                    write!(pretty, "Distinct").unwrap();
                }
                TopK {
                    group_key,
                    order_key,
                    limit,
                    offset,
                    ..
                } => {
                    write!(
                        pretty,
                        "TopK group={} order={}",
                        Bracketed("(", ")", Indices(group_key)),
                        Bracketed("(", ")", Separated(", ", order_key.clone())),
                    )
                    .unwrap();
                    if let Some(limit) = limit {
                        write!(pretty, " limit={}", limit).unwrap();
                    }
                    write!(pretty, " offset={}", offset).unwrap();
                }
                Negate { .. } => {
                    write!(pretty, "Negate").unwrap();
                }
                Threshold { .. } => {
                    write!(pretty, "Threshold").unwrap();
                }
                Union { left, right } => {
                    write!(pretty, "Union %{} %{}", expr_chain(left), expr_chain(right)).unwrap();
                }
            }
        }

        Explanation { nodes }
    }
}

impl<'a> Explanation<'a> {
    pub fn explain_types(&mut self, params: &BTreeMap<usize, ScalarType>) {
        for node in &mut self.nodes {
            // TODO(jamii) `typ` is itself recursive, so this is quadratic :(
            let RelationType { column_types, keys } = node.expr.typ(&[], params);
            node.annotations
                .push(format!("types = ({})", Separated(", ", column_types)));
            node.annotations.push(format!(
                "keys = ({})",
                Separated(", ", keys.iter().map(|key| Indices(key)).collect())
            ));
        }
    }
}

impl std::fmt::Display for ScalarExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        use ScalarExpr::*;
        match self {
            Column(i) => write!(
                f,
                "#{}{}",
                (0..i.level).map(|_| '^').collect::<String>(),
                i.column
            )?,
            Parameter(i) => write!(f, "?{}", i)?,
            Literal(row, _) => write!(f, "{}", row.unpack_first())?,
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
            // TODO(jamii)
            Exists(..) => write!(f, "exists(..)")?,
            Select(..) => write!(f, "select(..)")?,
        }
        Ok(())
    }
}

impl std::fmt::Display for AggregateExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{}({}{})",
            self.func,
            if self.distinct { "distinct " } else { "" },
            self.expr
        )
    }
}

#[derive(Debug)]
pub struct Separated<'a, T>(&'a str, Vec<T>);

impl<'a, T> std::fmt::Display for Separated<'a, T>
where
    T: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
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
pub struct Bracketed<'a, T>(&'a str, &'a str, T);

impl<'a, T> std::fmt::Display for Bracketed<'a, T>
where
    T: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}{}{}", self.0, self.2, self.1)
    }
}

#[derive(Debug)]
pub struct Indices<'a>(&'a [usize]);

impl<'a> std::fmt::Display for Indices<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
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

impl std::fmt::Display for JoinKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{}",
            match self {
                JoinKind::Inner => "Inner",
                JoinKind::LeftOuter => "LeftOuter",
                JoinKind::RightOuter => "RightOuter",
                JoinKind::FullOuter => "FullOuter",
            }
        )
    }
}
