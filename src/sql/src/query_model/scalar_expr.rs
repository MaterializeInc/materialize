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
/// [`crate::query_model::QueryBox`], and hence, their name.
#[derive(Debug, PartialEq, Clone)]
pub enum BoxScalarExpr {
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
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct ColumnReference {
    pub quantifier_id: QuantifierId,
    pub position: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct BaseColumn {
    pub position: usize,
    pub column_type: repr::ColumnType,
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
        }
    }
}

impl BoxScalarExpr {
    // @todo use a generic visit method
    pub fn collect_column_references_from_context(
        &self,
        context: &QuantifierSet,
        column_refs: &mut HashSet<ColumnReference>,
    ) {
        match &self {
            BoxScalarExpr::ColumnReference(c) => {
                if context.contains(&c.quantifier_id) {
                    column_refs.insert(c.clone());
                }
            }
            BoxScalarExpr::Literal(..) | BoxScalarExpr::BaseColumn(_) => {}
        }
    }
}
