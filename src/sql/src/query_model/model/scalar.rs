// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Parts of the Query Graph Model that involve scalar expressions.
//!
//! All types in this module are crate-private.

use std::collections::HashSet;
use std::fmt;

use mz_ore::str::separated;
use mz_repr::*;

use crate::plan::expr::{BinaryFunc, UnaryFunc, UnmaterializableFunc, VariadicFunc};
use crate::query_model::model::{QuantifierId, QuantifierSet};
use mz_expr::AggregateFunc;

/// Representation for scalar expressions within a query graph model.
///
/// Similar to HirScalarExpr but:
/// * subqueries are represented as column references to the subquery
///   quantifiers within the same box the expression belongs to,
/// * aggregate expressions are considered scalar expressions here
///   even though they are only valid in the context of a Grouping box,
/// * column references are represented by a pair (quantifier ID, column
///   position),
/// * BaseColumn is used to represent leaf columns, only allowed in
///   the projection of BaseTables and TableFunctions.
///
/// Scalar expressions only make sense within the context of a
/// [`super::graph::QueryBox`], and hence, their name.
#[derive(Debug, PartialEq, Clone)]
pub(crate) enum BoxScalarExpr {
    /// A reference to a column from a quantifier that either lives in
    /// the same box as the expression or is a sibling quantifier of
    /// an ascendent box of the box that contains the expression.
    ColumnReference(ColumnReference),
    /// A leaf column. Only allowed as standalone expressions in the
    /// projection of `BaseTable` and `TableFunction` boxes.
    BaseColumn(BaseColumn),
    /// A literal value.
    /// (A single datum stored as a row, because we can't own a Datum)
    Literal(Row, ColumnType),
    CallUnmaterializable(UnmaterializableFunc),
    CallUnary {
        func: UnaryFunc,
        expr: Box<BoxScalarExpr>,
    },
    CallBinary {
        func: BinaryFunc,
        expr1: Box<BoxScalarExpr>,
        expr2: Box<BoxScalarExpr>,
    },
    CallVariadic {
        func: VariadicFunc,
        exprs: Vec<BoxScalarExpr>,
    },
    If {
        cond: Box<BoxScalarExpr>,
        then: Box<BoxScalarExpr>,
        els: Box<BoxScalarExpr>,
    },
    Aggregate {
        /// Names the aggregation function.
        func: AggregateFunc,
        /// An expression which extracts from each row the input to `func`.
        expr: Box<BoxScalarExpr>,
        /// Should the aggregation be applied only to distinct results in each group.
        distinct: bool,
    },
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct ColumnReference {
    pub quantifier_id: QuantifierId,
    pub position: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct BaseColumn {
    pub position: usize,
    pub column_type: mz_repr::ColumnType,
}

impl fmt::Display for BoxScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            BoxScalarExpr::ColumnReference(c) => {
                write!(f, "Q{}.C{}", c.quantifier_id, c.position)
            }
            BoxScalarExpr::BaseColumn(c) => {
                write!(f, "C{}", c.position)
            }
            BoxScalarExpr::Literal(row, _) => {
                write!(f, "{}", row.unpack_first())
            }
            BoxScalarExpr::CallUnmaterializable(func) => {
                write!(f, "{}()", func)
            }
            BoxScalarExpr::CallUnary { func, expr } => {
                if let UnaryFunc::Not(_) = *func {
                    if let BoxScalarExpr::CallUnary {
                        func,
                        expr: inner_expr,
                    } = &**expr
                    {
                        if let Some(is) = func.is() {
                            write!(f, "({}) IS NOT {}", inner_expr, is)?;
                            return Ok(());
                        }
                    }
                }
                if let Some(is) = func.is() {
                    write!(f, "({}) IS {}", expr, is)
                } else {
                    write!(f, "{}({})", func, expr)
                }
            }
            BoxScalarExpr::CallBinary { func, expr1, expr2 } => {
                if func.is_infix_op() {
                    write!(f, "({} {} {})", expr1, func, expr2)
                } else {
                    write!(f, "{}({}, {})", func, expr1, expr2)
                }
            }
            BoxScalarExpr::CallVariadic { func, exprs } => {
                write!(f, "{}({})", func, separated(", ", exprs.clone()))
            }
            BoxScalarExpr::If { cond, then, els } => {
                write!(f, "if {} then {{{}}} else {{{}}}", cond, then, els)
            }
            BoxScalarExpr::Aggregate {
                func,
                expr,
                distinct,
            } => {
                write!(
                    f,
                    "{}({}{})",
                    *func,
                    if *distinct { "distinct " } else { "" },
                    expr
                )
            }
        }
    }
}

impl BoxScalarExpr {
    pub fn visit_children<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a Self),
    {
        use BoxScalarExpr::*;
        match self {
            ColumnReference(..) | BaseColumn(..) | Literal(..) | CallUnmaterializable(..) => (),
            CallUnary { expr, .. } => f(expr),
            CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
            Aggregate { expr, .. } => {
                f(expr);
            }
        }
    }

    /// A generalization of `visit`. The function `pre` runs on a
    /// `BoxScalarExpr` before it runs on any of the child `BoxScalarExpr`s.
    /// The function `post` runs on child `BoxScalarExpr`s first before the
    /// parent. Optionally, `pre` can return which child `BoxScalarExpr`s, if
    /// any, should be visited (default is to visit all children).
    pub fn try_visit_pre_post<F1, F2>(&self, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&Self) -> Option<Vec<&Self>>,
        F2: FnMut(&Self),
    {
        let to_visit = pre(self);
        if let Some(to_visit) = to_visit {
            for e in to_visit {
                e.try_visit_pre_post(pre, post);
            }
        } else {
            self.visit_children(|e| e.try_visit_pre_post(pre, post));
        }
        post(self);
    }

    pub fn visit_mut_children<'a, F>(&'a mut self, mut f: F)
    where
        F: FnMut(&'a mut Self),
    {
        use BoxScalarExpr::*;
        match self {
            ColumnReference(..) | BaseColumn(..) | Literal(..) | CallUnmaterializable(..) => (),
            CallUnary { expr, .. } => f(expr),
            CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
            Aggregate { expr, .. } => {
                f(expr);
            }
        }
    }

    pub fn visit_post<F>(&self, f: &mut F)
    where
        F: FnMut(&Self),
    {
        self.visit_children(|e| e.visit_post(f));
        f(self);
    }

    pub fn visit_mut_post<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit_mut_children(|e| e.visit_mut_post(f));
        f(self);
    }

    pub fn collect_column_references_from_context(
        &self,
        context: &QuantifierSet,
        column_refs: &mut HashSet<ColumnReference>,
    ) {
        self.try_visit_pre_post(&mut |_| None, &mut |expr| {
            if let BoxScalarExpr::ColumnReference(c) = expr {
                if context.contains(&c.quantifier_id) {
                    column_refs.insert(c.clone());
                }
            }
        })
    }
}
