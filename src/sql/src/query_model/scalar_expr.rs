// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::fmt;

use repr::*;

use crate::query_model::{QuantifierId, QuantifierSet};

pub use expr::BinaryFunc;

/// Representation for scalar expressions within a query graph model.
/// Similar to HirScalarExpr but:
/// * subqueries are represented as column references to the subquery
///   quantifiers within the same box the expression belongs to,
/// * aggregate expressions are considered scalar expressions here
///   even though they are only valid in the context of a Grouping box,
/// * column references are represented by a pair (quantifier ID, column
///   position),
/// * BaseColumn is used to represent leaf columns, only allowed in
///   the projection of BaseTables and TableFunctions.
#[derive(Debug, PartialEq, Clone)]
pub enum Expr {
    ColumnReference(ColumnReference),
    BaseColumn(BaseColumn),
    Literal(Row, ColumnType),
    CallBinary {
        func: BinaryFunc,
        expr1: Box<Expr>,
        expr2: Box<Expr>,
    },
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Expr::ColumnReference(c) => {
                write!(f, "Q{}.C{}", c.quantifier_id, c.position)
            }
            Expr::BaseColumn(c) => {
                write!(f, "C{}", c.position)
            }
            Expr::Literal(row, _) => {
                write!(f, "{}", row.unpack_first())
            }
            Expr::CallBinary { func, expr1, expr2 } => {
                if func.is_infix_op() {
                    write!(f, "({} {} {})", expr1, func, expr2)
                } else {
                    write!(f, "{}({}, {})", func, expr1, expr2)
                }
            }
        }
    }
}

impl Expr {
    // @todo use a generic visit method
    pub fn collect_column_references_from_context(
        &self,
        context: &QuantifierSet,
        column_refs: &mut HashSet<ColumnReference>,
    ) {
        match &self {
            Expr::ColumnReference(c) => {
                if context.contains(&c.quantifier_id) {
                    column_refs.insert(c.clone());
                }
            }
            Expr::CallBinary { expr1, expr2, .. } => {
                expr1.collect_column_references_from_context(context, column_refs);
                expr2.collect_column_references_from_context(context, column_refs);
            }
            Expr::Literal(..) | Expr::BaseColumn(_) => {}
        }
    }

    /// True if the expression doesn't reference any column from the given set of
    /// quantifiers, even though it may contain other column references from other
    /// contexts.
    pub fn is_constaint_within_context(&self, context: &QuantifierSet) -> bool {
        let mut column_refs = HashSet::new();
        self.collect_column_references_from_context(context, &mut column_refs);
        column_refs.is_empty()
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct ColumnReference {
    pub quantifier_id: QuantifierId,
    pub position: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct BaseColumn {
    pub position: usize,
}
