// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module houses a pretty printer for [`HirRelationExpr`],
//! which is the SQL-specific relation expression (as opposed to [`expr::MirRelationExpr`]).
//! See also [`expr::explain`].
//!
//! The format is the same, except for the following extensions:
//!
//!   * References to columns in outer scopes are indicated by adding a ^ per
//!     level of nesting, e.g. #^^4.
//!   * Subqueries can be embedded in scalar expressions.
//!

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use expr::explain::Indices;
use expr::{ExprHumanizer, Id, IdGen, RowSetFinishing};
use ore::str::{bracketed, separated};
use repr::{RelationType, ScalarType};

use crate::plan::expr::{AggregateExpr, HirRelationExpr, HirScalarExpr, WindowExprType};

/// An `Explanation` facilitates pretty-printing of a [`HirRelationExpr`].
///
/// By default, the [`fmt::Display`] implementation renders the expression as
/// described in the module docs. Additional information may be attached to the
/// explanation via the other public methods on the type.
#[derive(Debug)]
pub struct Explanation<'a> {
    expr_humanizer: &'a dyn ExprHumanizer,
    /// One `ExplanationNode` for each `HirRelationExpr` in the plan, in
    /// left-to-right post-order.
    nodes: Vec<ExplanationNode<'a>>,
    /// An optional `RowSetFinishing` to mention at the end.
    finishing: Option<RowSetFinishing>,
    /// Records the chain ID that was assigned to each expression.
    expr_chains: HashMap<*const HirRelationExpr, u64>,
    /// The ID of the current chain. Incremented while constructing the
    /// `Explanation`.
    chain: u64,
}

#[derive(Debug)]
pub struct ExplanationNode<'a> {
    /// The expression being explained.
    pub expr: &'a HirRelationExpr,
    /// The type of the expression, if desired.
    pub typ: Option<RelationType>,
    /// The ID of the linear chain to which this node belongs.
    pub chain: u64,
    /// Nexted explanations for any subqueries in the node.
    pub subqueries: Vec<Explanation<'a>>,
}

impl<'a> fmt::Display for Explanation<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut prev_chain = u64::max_value();
        for node in &self.nodes {
            if node.chain != prev_chain {
                if node.chain != 0 {
                    writeln!(f)?;
                }
                write!(f, "%{} =", node.chain)?;
                writeln!(f)?;
            }
            prev_chain = node.chain;

            self.fmt_node(f, node)?;
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
    /// Creates an explanation for a [`HirRelationExpr`].
    pub fn new(
        expr: &'a HirRelationExpr,
        expr_humanizer: &'a dyn ExprHumanizer,
    ) -> Explanation<'a> {
        Self::new_internal(expr, expr_humanizer, &mut IdGen::default())
    }

    pub fn new_internal(
        expr: &'a HirRelationExpr,
        expr_humanizer: &'a dyn ExprHumanizer,
        id_gen: &mut IdGen,
    ) -> Explanation<'a> {
        use HirRelationExpr::*;

        // Do a post-order traversal of the expression, grouping "chains" of
        // nodes together as we go. We have to break the chain whenever we
        // encounter a node with multiple inputs, like a join.

        fn walk<'a>(
            expr: &'a HirRelationExpr,
            explanation: &mut Explanation<'a>,
            id_gen: &mut IdGen,
        ) {
            // First, walk the children, in order to perform a post-order
            // traversal.
            match expr {
                // Leaf expressions. Nothing more to visit.
                Constant { .. } | Get { .. } | CallTable { .. } => (),
                // Single-input expressions continue the chain.
                Project { input, .. }
                | Map { input, .. }
                | Filter { input, .. }
                | Reduce { input, .. }
                | Distinct { input }
                | TopK { input, .. }
                | Negate { input, .. }
                | DeclareKeys { input, .. }
                | Threshold { input, .. } => walk(input, explanation, id_gen),
                // For join and union, each input needs to go in its own chain.
                Join { left, right, .. } => {
                    walk(left, explanation, id_gen);
                    explanation.chain = id_gen.allocate_id();
                    walk(right, explanation, id_gen);
                    explanation.chain = id_gen.allocate_id();
                }
                Union { base, inputs, .. } => {
                    walk(base, explanation, id_gen);
                    explanation.chain = id_gen.allocate_id();
                    for input in inputs {
                        walk(input, explanation, id_gen);
                        explanation.chain = id_gen.allocate_id();
                    }
                }
            }

            // Then collect subqueries.
            let mut scalars = vec![];
            match expr {
                Constant { .. }
                | Get { .. }
                | Project { .. }
                | Distinct { .. }
                | Negate { .. }
                | Threshold { .. }
                | Union { .. }
                | DeclareKeys { .. }
                | TopK { .. } => (),
                Map { scalars: exprs, .. }
                | Filter {
                    predicates: exprs, ..
                }
                | CallTable { exprs, .. } => scalars.extend(exprs),
                Join { on, .. } => scalars.push(on),
                Reduce { aggregates, .. } => {
                    for agg in aggregates {
                        scalars.push(&agg.expr);
                    }
                }
            }
            let mut subqueries = vec![];
            for scalar in scalars {
                scalar.visit(&mut |scalar| match scalar {
                    HirScalarExpr::Exists(expr) | HirScalarExpr::Select(expr) => {
                        let subquery =
                            Explanation::new_internal(expr, explanation.expr_humanizer, id_gen);
                        explanation.expr_chains.insert(
                            &**expr as *const HirRelationExpr,
                            subquery.nodes.last().unwrap().chain,
                        );
                        subqueries.push(subquery);
                    }
                    _ => (),
                })
            }

            // Finally, record the node.
            explanation.nodes.push(ExplanationNode {
                expr,
                typ: None,
                chain: explanation.chain,
                subqueries,
            });
            explanation
                .expr_chains
                .insert(expr as *const HirRelationExpr, explanation.chain);
        }

        let mut explanation = Explanation {
            expr_humanizer,
            nodes: vec![],
            finishing: None,
            expr_chains: HashMap::new(),
            chain: id_gen.allocate_id(),
        };
        walk(expr, &mut explanation, id_gen);
        explanation
    }

    /// Attach type information into the explanation.
    pub fn explain_types(&mut self, params: &BTreeMap<usize, ScalarType>) {
        self.explain_types_internal(&[], params)
    }

    fn explain_types_internal(
        &mut self,
        outers: &[RelationType],
        params: &BTreeMap<usize, ScalarType>,
    ) {
        for node in &mut self.nodes {
            // TODO(jamii) `typ` is itself recursive, so this is quadratic :(
            let typ = node.expr.typ(outers, params);
            let mut outers = outers.to_vec();
            outers.push(typ);
            for subquery in &mut node.subqueries {
                subquery.explain_types_internal(&outers, params);
            }
            node.typ = outers.pop();
        }
    }

    /// Attach a `RowSetFinishing` to the explanation.
    pub fn explain_row_set_finishing(&mut self, finishing: RowSetFinishing) {
        self.finishing = Some(finishing);
    }

    fn fmt_node(&self, f: &mut fmt::Formatter, node: &ExplanationNode) -> fmt::Result {
        use HirRelationExpr::*;

        match node.expr {
            Constant { rows, .. } => {
                write!(f, "| Constant")?;
                for row in rows {
                    write!(f, " {}", row)?;
                }
                writeln!(f)?;
            }
            Get { id, .. } => match id {
                Id::Local(_) => {
                    unreachable!("SQL expressions do not support Lets yet")
                }
                Id::LocalBareSource => writeln!(f, "| Get Local Bare Source")?,
                Id::Global(id) => writeln!(
                    f,
                    "| Get {} ({})",
                    self.expr_humanizer
                        .humanize_id(*id)
                        .unwrap_or_else(|| "?".to_owned()),
                    id,
                )?,
            },
            Project { outputs, .. } => {
                writeln!(f, "| Project {}", bracketed("(", ")", Indices(outputs)))?
            }
            Map { scalars, .. } => {
                write!(f, "| Map")?;
                for (i, e) in scalars.iter().enumerate() {
                    write!(f, "{}", if i == 0 { " " } else { ", " })?;
                    self.fmt_scalar_expr(f, e)?;
                }
                writeln!(f)?;
            }
            CallTable { func, exprs } => {
                write!(f, "| CallTable {}(", func,)?;
                for (i, e) in exprs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    self.fmt_scalar_expr(f, e)?;
                }
                writeln!(f, ")")?;
            }
            Filter { predicates, .. } => {
                write!(f, "| Filter")?;
                for (i, e) in predicates.iter().enumerate() {
                    write!(f, "{}", if i == 0 { " " } else { ", " })?;
                    self.fmt_scalar_expr(f, e)?;
                }
                writeln!(f)?;
            }
            Join {
                left,
                right,
                on,
                kind,
            } => {
                write!(
                    f,
                    "| {}Join %{} %{} on ",
                    kind,
                    self.expr_chain(left),
                    self.expr_chain(right),
                )?;
                self.fmt_scalar_expr(f, on)?;
                writeln!(f)?;
            }
            Reduce {
                group_key,
                aggregates,
                ..
            } => {
                write!(
                    f,
                    "| Reduce group={}",
                    bracketed("(", ")", separated(", ", group_key)),
                )?;
                for agg in aggregates {
                    write!(f, " ")?;
                    self.fmt_aggregate_expr(f, agg)?;
                }
                writeln!(f)?;
            }
            Distinct { .. } => writeln!(f, "| Distinct")?,
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
                separated(", ", keys.iter().map(|key| Indices(key)))
            )?;
        }

        for subquery in &node.subqueries {
            for line in subquery.to_string().split('\n') {
                if line.is_empty() {
                    writeln!(f, "| |")?;
                } else {
                    writeln!(f, "| | {}", line)?;
                }
            }
        }

        Ok(())
    }

    fn fmt_scalar_expr(&self, f: &mut fmt::Formatter, expr: &HirScalarExpr) -> fmt::Result {
        use HirScalarExpr::*;

        match expr {
            Column(i) => write!(
                f,
                "#{}{}",
                (0..i.level).map(|_| '^').collect::<String>(),
                i.column
            ),
            Parameter(i) => write!(f, "${}", i),
            Literal(row, _) => write!(f, "{}", row.unpack_first()),
            CallNullary(func) => write!(f, "{}()", func),
            CallUnary { func, expr } => {
                write!(f, "{}(", func)?;
                self.fmt_scalar_expr(f, expr)?;
                write!(f, ")")
            }
            CallBinary { func, expr1, expr2 } => {
                if func.is_infix_op() {
                    write!(f, "(")?;
                    self.fmt_scalar_expr(f, expr1)?;
                    write!(f, " {} ", func)?;
                    self.fmt_scalar_expr(f, expr2)?;
                    write!(f, ")")
                } else {
                    write!(f, "{}", func)?;
                    write!(f, "(")?;
                    self.fmt_scalar_expr(f, expr1)?;
                    write!(f, ", ")?;
                    self.fmt_scalar_expr(f, expr2)?;
                    write!(f, ")")
                }
            }
            CallVariadic { func, exprs } => {
                write!(f, "{}(", func)?;
                for (i, expr) in exprs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    self.fmt_scalar_expr(f, expr)?;
                }
                write!(f, ")")
            }
            If { cond, then, els } => {
                write!(f, "if ")?;
                self.fmt_scalar_expr(f, cond)?;
                write!(f, " then {{")?;
                self.fmt_scalar_expr(f, then)?;
                write!(f, "}} els {{")?;
                self.fmt_scalar_expr(f, els)?;
                write!(f, "}}")
            }
            Exists(expr) => write!(f, "exists(%{})", self.expr_chain(expr)),
            Select(expr) => write!(f, "select(%{})", self.expr_chain(expr)),
            Windowing(expr) => {
                match &expr.func {
                    WindowExprType::Scalar(scalar) => {
                        write!(f, "{}()", scalar.clone().into_expr())?
                    }
                }
                write!(f, " over (")?;
                for (i, e) in expr.partition.iter().enumerate() {
                    if i > 0 {
                        write!(f, "{}", ", ")?;
                    }
                    self.fmt_scalar_expr(f, e)?;
                }
                write!(f, ")")?;

                if !expr.order_by.is_empty() {
                    write!(f, " order by (")?;
                    for (i, e) in expr.order_by.iter().enumerate() {
                        if i > 0 {
                            write!(f, "{}", ", ")?;
                        }
                        self.fmt_scalar_expr(f, e)?;
                    }
                    write!(f, ")")?;
                }
                Ok(())
            }
        }
    }

    fn fmt_aggregate_expr(&self, f: &mut fmt::Formatter, expr: &AggregateExpr) -> fmt::Result {
        write!(f, "{}(", expr.func.clone().into_expr())?;
        if expr.distinct {
            write!(f, "distinct ")?;
        }
        self.fmt_scalar_expr(f, &expr.expr)?;
        write!(f, ")")
    }

    /// Retrieves the chain ID for the specified expression.
    ///
    /// The `ExplanationNode` for `expr` must have already been inserted into
    /// the explanation.
    fn expr_chain(&self, expr: &HirRelationExpr) -> u64 {
        self.expr_chains[&(expr as *const HirRelationExpr)]
    }
}
