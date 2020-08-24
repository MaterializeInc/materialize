// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::mem;

use crate::ast::display::{self, AstDisplay, AstFormatter};
use crate::ast::{Expr, FunctionArgs, Ident, ObjectName};

/// The most complete variant of a `SELECT` query expression, optionally
/// including `WITH`, `UNION` / other set operations, and `ORDER BY`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Query {
    /// WITH (common table expressions, or CTEs)
    pub ctes: Vec<Cte>,
    /// SELECT or UNION / EXCEPT / INTECEPT
    pub body: SetExpr,
    /// ORDER BY
    pub order_by: Vec<OrderByExpr>,
    /// `LIMIT { <N> | ALL }`
    pub limit: Option<Expr>,
    /// `OFFSET <N> { ROW | ROWS }`
    pub offset: Option<Expr>,
    /// `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`
    pub fetch: Option<Fetch>,
}

impl AstDisplay for Query {
    fn fmt(&self, f: &mut AstFormatter) {
        if !self.ctes.is_empty() {
            f.write_str("WITH ");
            f.write_node(&display::comma_separated(&self.ctes));
            f.write_str(" ");
        }
        f.write_node(&self.body);
        if !self.order_by.is_empty() {
            f.write_str(" ORDER BY ");
            f.write_node(&display::comma_separated(&self.order_by));
        }
        if let Some(ref limit) = self.limit {
            f.write_str(" LIMIT ");
            f.write_node(limit);
        }
        if let Some(ref offset) = self.offset {
            f.write_str(" OFFSET ");
            f.write_node(offset);
            f.write_str(" ROWS");
        }
        if let Some(ref fetch) = self.fetch {
            f.write_str(" ");
            f.write_node(fetch);
        }
    }
}
impl_display!(Query);

impl Query {
    pub fn select(select: Select) -> Query {
        Query {
            ctes: vec![],
            body: SetExpr::Select(Box::new(select)),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }
    }

    pub fn take(&mut self) -> Query {
        mem::replace(
            self,
            Query {
                ctes: vec![],
                order_by: vec![],
                body: SetExpr::Values(Values(vec![])),
                limit: None,
                offset: None,
                fetch: None,
            },
        )
    }
}

/// A node in a tree, representing a "query body" expression, roughly:
/// `SELECT ... [ {UNION|EXCEPT|INTERSECT} SELECT ...]`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SetExpr {
    /// Restricted SELECT .. FROM .. HAVING (no ORDER BY or set operations)
    Select(Box<Select>),
    /// Parenthesized SELECT subquery, which may include more set operations
    /// in its body and an optional ORDER BY / LIMIT.
    Query(Box<Query>),
    /// UNION/EXCEPT/INTERSECT of two queries
    SetOperation {
        op: SetOperator,
        all: bool,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
    Values(Values),
    // TODO: ANSI SQL supports `TABLE` here.
}

impl AstDisplay for SetExpr {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            SetExpr::Select(s) => f.write_node(s),
            SetExpr::Query(q) => {
                f.write_str("(");
                f.write_node(q);
                f.write_str(")")
            }
            SetExpr::Values(v) => f.write_node(v),
            SetExpr::SetOperation {
                left,
                right,
                op,
                all,
            } => {
                f.write_node(left);
                f.write_str(" ");
                f.write_node(op);
                f.write_str(" ");
                if *all {
                    f.write_str("ALL ");
                }
                f.write_node(right);
            }
        }
    }
}
impl_display!(SetExpr);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SetOperator {
    Union,
    Except,
    Intersect,
}

impl AstDisplay for SetOperator {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str(match self {
            SetOperator::Union => "UNION",
            SetOperator::Except => "EXCEPT",
            SetOperator::Intersect => "INTERSECT",
        })
    }
}
impl_display!(SetOperator);

/// A restricted variant of `SELECT` (without CTEs/`ORDER BY`), which may
/// appear either as the only body item of an `SQLQuery`, or as an operand
/// to a set operation like `UNION`.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Select {
    pub distinct: bool,
    /// projection expressions
    pub projection: Vec<SelectItem>,
    /// FROM
    pub from: Vec<TableWithJoins>,
    /// WHERE
    pub selection: Option<Expr>,
    /// GROUP BY
    pub group_by: Vec<Expr>,
    /// HAVING
    pub having: Option<Expr>,
}

impl AstDisplay for Select {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SELECT ");
        if self.distinct {
            f.write_str("DISTINCT ");
        }
        f.write_node(&display::comma_separated(&self.projection));
        if !self.from.is_empty() {
            f.write_str(" FROM ");
            f.write_node(&display::comma_separated(&self.from));
        }
        if let Some(ref selection) = self.selection {
            f.write_str(" WHERE ");
            f.write_node(selection);
        }
        if !self.group_by.is_empty() {
            f.write_str(" GROUP BY ");
            f.write_node(&display::comma_separated(&self.group_by));
        }
        if let Some(ref having) = self.having {
            f.write_str(" HAVING ");
            f.write_node(having);
        }
    }
}
impl_display!(Select);

impl Select {
    pub fn from(mut self, twj: TableWithJoins) -> Select {
        self.from.push(twj);
        self
    }

    pub fn project(mut self, select_item: SelectItem) -> Select {
        self.projection.push(select_item);
        self
    }

    pub fn selection(mut self, selection: Option<Expr>) -> Select {
        self.selection = selection;
        self
    }
}

/// A single CTE (used after `WITH`): `alias [(col1, col2, ...)] AS ( query )`
/// The names in the column list before `AS`, when specified, replace the names
/// of the columns returned by the query. The parser does not validate that the
/// number of columns in the query matches the number of columns in the query.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Cte {
    pub alias: TableAlias,
    pub query: Query,
}

impl AstDisplay for Cte {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_node(&self.alias);
        f.write_str(" AS (");
        f.write_node(&self.query);
        f.write_str(")");
    }
}
impl_display!(Cte);

/// One item of the comma-separated list following `SELECT`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SelectItem {
    /// An expression, optionally followed by `[ AS ] alias`.
    Expr { expr: Expr, alias: Option<Ident> },
    /// An unqualified `*`.
    Wildcard,
}

impl AstDisplay for SelectItem {
    fn fmt(&self, f: &mut AstFormatter) {
        match &self {
            SelectItem::Expr { expr, alias } => {
                f.write_node(expr);
                if let Some(alias) = alias {
                    f.write_str(" AS ");
                    f.write_node(alias);
                }
            }
            SelectItem::Wildcard => f.write_str("*"),
        }
    }
}
impl_display!(SelectItem);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
}

impl AstDisplay for TableWithJoins {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_node(&self.relation);
        for join in &self.joins {
            f.write_node(join)
        }
    }
}
impl_display!(TableWithJoins);

impl TableWithJoins {
    pub fn subquery(query: Query, alias: TableAlias) -> TableWithJoins {
        TableWithJoins {
            relation: TableFactor::Derived {
                lateral: false,
                subquery: Box::new(query),
                alias: Some(alias),
            },
            joins: vec![],
        }
    }
}

/// A table name or a parenthesized subquery with an optional alias
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableFactor {
    Table {
        name: ObjectName,
        alias: Option<TableAlias>,
    },
    Function {
        name: ObjectName,
        args: FunctionArgs,
        alias: Option<TableAlias>,
    },
    Derived {
        lateral: bool,
        subquery: Box<Query>,
        alias: Option<TableAlias>,
    },
    /// Represents a parenthesized join expression, such as
    /// `(foo <JOIN> bar [ <JOIN> baz ... ])`.
    /// The inner `TableWithJoins` can have no joins only if its
    /// `relation` is itself a `TableFactor::NestedJoin`.
    NestedJoin {
        join: Box<TableWithJoins>,
        alias: Option<TableAlias>,
    },
}

impl AstDisplay for TableFactor {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            TableFactor::Table { name, alias } => {
                f.write_node(name);
                if let Some(alias) = alias {
                    f.write_str(" AS ");
                    f.write_node(alias);
                }
            }
            TableFactor::Function { name, args, alias } => {
                f.write_node(name);
                f.write_str("(");
                f.write_node(args);
                f.write_str(")");
                if let Some(alias) = alias {
                    f.write_str(" AS ");
                    f.write_node(alias);
                }
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if *lateral {
                    f.write_str("LATERAL ");
                }
                f.write_str("(");
                f.write_node(subquery);
                f.write_str(")");
                if let Some(alias) = alias {
                    f.write_str(" AS ");
                    f.write_node(alias);
                }
            }
            TableFactor::NestedJoin { join, alias } => {
                f.write_str("(");
                f.write_node(join);
                f.write_str(")");
                if let Some(alias) = alias {
                    f.write_str(" AS ");
                    f.write_node(alias);
                }
            }
        }
    }
}
impl_display!(TableFactor);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableAlias {
    pub name: Ident,
    pub columns: Vec<Ident>,
    /// Whether the number of aliased columns must exactly match the number of
    /// columns in the underlying table.
    ///
    /// TODO(benesch): this shouldn't really live in the AST (it's a HIR
    /// concern), but it will have to do for now.
    pub strict: bool,
}

impl AstDisplay for TableAlias {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_node(&self.name);
        if !self.columns.is_empty() {
            f.write_str(" (");
            f.write_node(&display::comma_separated(&self.columns));
            f.write_str(")");
        }
    }
}
impl_display!(TableAlias);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Join {
    pub relation: TableFactor,
    pub join_operator: JoinOperator,
}

impl AstDisplay for Join {
    fn fmt(&self, f: &mut AstFormatter) {
        fn prefix(constraint: &JoinConstraint) -> &'static str {
            match constraint {
                JoinConstraint::Natural => "NATURAL ",
                _ => "",
            }
        }
        fn suffix<'a>(constraint: &'a JoinConstraint) -> impl AstDisplay + 'a {
            struct Suffix<'a>(&'a JoinConstraint);
            impl<'a> AstDisplay for Suffix<'a> {
                fn fmt(&self, f: &mut AstFormatter) {
                    match self.0 {
                        JoinConstraint::On(expr) => {
                            f.write_str(" ON ");
                            f.write_node(expr);
                        }
                        JoinConstraint::Using(attrs) => {
                            f.write_str(" USING(");
                            f.write_node(&display::comma_separated(attrs));
                            f.write_str(")");
                        }
                        _ => {}
                    }
                }
            }
            Suffix(constraint)
        }
        match &self.join_operator {
            JoinOperator::Inner(constraint) => {
                f.write_str(" ");
                f.write_str(prefix(constraint));
                f.write_str("JOIN ");
                f.write_node(&self.relation);
                f.write_node(&suffix(constraint));
            }
            JoinOperator::LeftOuter(constraint) => {
                f.write_str(" ");
                f.write_str(prefix(constraint));
                f.write_str("LEFT JOIN ");
                f.write_node(&self.relation);
                f.write_node(&suffix(constraint));
            }
            JoinOperator::RightOuter(constraint) => {
                f.write_str(" ");
                f.write_str(prefix(constraint));
                f.write_str("RIGHT JOIN ");
                f.write_node(&self.relation);
                f.write_node(&suffix(constraint));
            }
            JoinOperator::FullOuter(constraint) => {
                f.write_str(" ");
                f.write_str(prefix(constraint));
                f.write_str("FULL JOIN ");
                f.write_node(&self.relation);
                f.write_node(&suffix(constraint));
            }
            JoinOperator::CrossJoin => {
                f.write_str(" CROSS JOIN ");
                f.write_node(&self.relation);
            }
        }
    }
}
impl_display!(Join);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
    RightOuter(JoinConstraint),
    FullOuter(JoinConstraint),
    CrossJoin,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<Ident>),
    Natural,
}

/// SQL ORDER BY expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: Option<bool>,
}

impl AstDisplay for OrderByExpr {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_node(&self.expr);
        match self.asc {
            Some(true) => f.write_str(" ASC"),
            Some(false) => f.write_str(" DESC"),
            None => {}
        }
    }
}
impl_display!(OrderByExpr);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fetch {
    pub with_ties: bool,
    pub percent: bool,
    pub quantity: Option<Expr>,
}

impl AstDisplay for Fetch {
    fn fmt(&self, f: &mut AstFormatter) {
        let extension = if self.with_ties { "WITH TIES" } else { "ONLY" };
        if let Some(ref quantity) = self.quantity {
            let percent = if self.percent { " PERCENT" } else { "" };
            f.write_str("FETCH FIRST ");
            f.write_node(quantity);
            f.write_str(percent);
            f.write_str(" ROWS ");
            f.write_str(extension);
        } else {
            f.write_str("FETCH FIRST ROWS ");
            f.write_str(extension);
        }
    }
}
impl_display!(Fetch);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Values(pub Vec<Vec<Expr>>);

impl AstDisplay for Values {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("VALUES ");
        let mut delim = "";
        for row in &self.0 {
            f.write_str(delim);
            delim = ", ";
            f.write_str("(");
            f.write_node(&display::comma_separated(row));
            f.write_str(")");
        }
    }
}
impl_display!(Values);
