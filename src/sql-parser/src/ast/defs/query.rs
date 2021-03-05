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

use std::fmt::Debug;
use std::hash::Hash;

use crate::ast::display::{self, AstDisplay, AstFormatter};
use crate::ast::{Expr, FunctionArgs, Ident, SqlOption, UnresolvedObjectName};

// This represents the metadata that lives next to an AST, as we take it through
// various stages in the planning process.
//
// Conceptually, when we first receive an AST from the parsing process, it only
// represents the syntax that the user input, and has no semantic information
// embedded in it. Later in this process, we want to be able to walk the tree
// and add additional information to it piecemeal, perhaps without going down
// the full planning pipeline. AstInfo represents various bits of information
// that get stored in the tree: for instance, at first, table names are only
// represented by the names the user input (in the `Raw` implementor of this
// trait), but later on, we replace them with both the name along with the ID
// that it gets resolved to.
//
// Currently this process brings an Ast<Raw> to Ast<Aug>, and lives in
// sql/src/plan/query.rs:resolve_names.
pub trait AstInfo: Clone {
    // The type used for table references.
    type ObjectName: AstDisplay + Clone + Hash + Debug + Eq;
    // The type stored next to CTEs for their assigned ID.
    type Id: Clone + Hash + Debug + Eq;
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Default)]
pub struct Raw;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum RawName {
    Name(UnresolvedObjectName),
    Id(String, UnresolvedObjectName),
}

impl RawName {
    pub fn name(&self) -> &UnresolvedObjectName {
        match self {
            RawName::Name(name) => name,
            RawName::Id(_, name) => name,
        }
    }
}

impl AstDisplay for RawName {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            RawName::Name(o) => f.write_node(o),
            RawName::Id(id, o) => {
                f.write_str(format!("[{} AS ", id));
                f.write_node(o);
                f.write_str("]");
            }
        }
    }
}
impl_display!(RawName);

impl AstInfo for Raw {
    type ObjectName = RawName;
    type Id = ();
}

/// The most complete variant of a `SELECT` query expression, optionally
/// including `WITH`, `UNION` / other set operations, and `ORDER BY`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Query<T: AstInfo> {
    /// WITH (common table expressions, or CTEs)
    pub ctes: Vec<Cte<T>>,
    /// SELECT or UNION / EXCEPT / INTECEPT
    pub body: SetExpr<T>,
    /// ORDER BY
    pub order_by: Vec<OrderByExpr<T>>,
    /// `LIMIT { <N> | ALL }`
    /// `FETCH { FIRST | NEXT } <N> { ROW | ROWS } | { ONLY | WITH TIES }`
    pub limit: Option<Limit<T>>,
    /// `OFFSET <N> { ROW | ROWS }`
    pub offset: Option<Expr<T>>,
}

impl<T: AstInfo> AstDisplay for Query<T> {
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

        let write_offset = |f: &mut AstFormatter| {
            if let Some(offset) = &self.offset {
                f.write_str(" OFFSET ");
                f.write_node(offset);
            }
        };

        if let Some(limit) = &self.limit {
            if limit.with_ties {
                write_offset(f);
                f.write_str(" FETCH FIRST ");
                f.write_node(&limit.quantity);
                f.write_str(" ROWS WITH TIES");
            } else {
                f.write_str(" LIMIT ");
                f.write_node(&limit.quantity);
                write_offset(f);
            }
        } else {
            write_offset(f);
        }
    }
}
impl_display_t!(Query);

impl<T: AstInfo> Query<T> {
    pub fn select(select: Select<T>) -> Query<T> {
        Query {
            ctes: vec![],
            body: SetExpr::Select(Box::new(select)),
            order_by: vec![],
            limit: None,
            offset: None,
        }
    }

    pub fn take(&mut self) -> Query<T> {
        mem::replace(
            self,
            Query::<T> {
                ctes: vec![],
                order_by: vec![],
                body: SetExpr::Values(Values(vec![])),
                limit: None,
                offset: None,
            },
        )
    }
}

/// A node in a tree, representing a "query body" expression, roughly:
/// `SELECT ... [ {UNION|EXCEPT|INTERSECT} SELECT ...]`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SetExpr<T: AstInfo> {
    /// Restricted SELECT .. FROM .. HAVING (no ORDER BY or set operations)
    Select(Box<Select<T>>),
    /// Parenthesized SELECT subquery, which may include more set operations
    /// in its body and an optional ORDER BY / LIMIT.
    Query(Box<Query<T>>),
    /// UNION/EXCEPT/INTERSECT of two queries
    SetOperation {
        op: SetOperator,
        all: bool,
        left: Box<SetExpr<T>>,
        right: Box<SetExpr<T>>,
    },
    Values(Values<T>),
    // TODO: ANSI SQL supports `TABLE` here.
}

impl<T: AstInfo> AstDisplay for SetExpr<T> {
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
impl_display_t!(SetExpr);

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
pub struct Select<T: AstInfo> {
    pub distinct: Option<Distinct<T>>,
    /// projection expressions
    pub projection: Vec<SelectItem<T>>,
    /// FROM
    pub from: Vec<TableWithJoins<T>>,
    /// WHERE
    pub selection: Option<Expr<T>>,
    /// GROUP BY
    pub group_by: Vec<Expr<T>>,
    /// HAVING
    pub having: Option<Expr<T>>,
    /// OPTION
    pub options: Vec<SqlOption<T>>,
}

impl<T: AstInfo> AstDisplay for Select<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SELECT");
        if let Some(distinct) = &self.distinct {
            f.write_str(" ");
            f.write_node(distinct);
        }
        if !self.projection.is_empty() {
            f.write_str(" ");
            f.write_node(&display::comma_separated(&self.projection));
        }
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
        if !self.options.is_empty() {
            f.write_str(" OPTION (");
            f.write_node(&display::comma_separated(&self.options));
            f.write_str(")");
        }
    }
}
impl_display_t!(Select);

impl<T: AstInfo> Select<T> {
    pub fn from(mut self, twj: TableWithJoins<T>) -> Select<T> {
        self.from.push(twj);
        self
    }

    pub fn project(mut self, select_item: SelectItem<T>) -> Select<T> {
        self.projection.push(select_item);
        self
    }

    pub fn selection(mut self, selection: Option<Expr<T>>) -> Select<T> {
        self.selection = selection;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Distinct<T: AstInfo> {
    EntireRow,
    On(Vec<Expr<T>>),
}
impl_display_t!(Distinct);

impl<T: AstInfo> AstDisplay for Distinct<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Distinct::EntireRow => f.write_str("DISTINCT"),
            Distinct::On(cols) => {
                f.write_str("DISTINCT ON (");
                f.write_node(&display::comma_separated(cols));
                f.write_str(")");
            }
        }
    }
}

/// A single CTE (used after `WITH`): `alias [(col1, col2, ...)] AS ( query )`
/// The names in the column list before `AS`, when specified, replace the names
/// of the columns returned by the query. The parser does not validate that the
/// number of columns in the query matches the number of columns in the query.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Cte<T: AstInfo> {
    pub alias: TableAlias,
    pub id: T::Id,
    pub query: Query<T>,
}

impl<T: AstInfo> AstDisplay for Cte<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_node(&self.alias);
        f.write_str(" AS (");
        f.write_node(&self.query);
        f.write_str(")");
    }
}
impl_display_t!(Cte);

/// One item of the comma-separated list following `SELECT`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SelectItem<T: AstInfo> {
    /// An expression, optionally followed by `[ AS ] alias`.
    Expr { expr: Expr<T>, alias: Option<Ident> },
    /// An unqualified `*`.
    Wildcard,
}

impl<T: AstInfo> AstDisplay for SelectItem<T> {
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
impl_display_t!(SelectItem);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableWithJoins<T: AstInfo> {
    pub relation: TableFactor<T>,
    pub joins: Vec<Join<T>>,
}

impl<T: AstInfo> AstDisplay for TableWithJoins<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_node(&self.relation);
        for join in &self.joins {
            f.write_node(join)
        }
    }
}
impl_display_t!(TableWithJoins);

impl<T: AstInfo> TableWithJoins<T> {
    pub fn subquery(query: Query<T>, alias: TableAlias) -> TableWithJoins<T> {
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
pub enum TableFactor<T: AstInfo> {
    Table {
        name: T::ObjectName,
        alias: Option<TableAlias>,
    },
    Function {
        name: UnresolvedObjectName,
        args: FunctionArgs<T>,
        alias: Option<TableAlias>,
    },
    Derived {
        lateral: bool,
        subquery: Box<Query<T>>,
        alias: Option<TableAlias>,
    },
    /// Represents a parenthesized join expression, such as
    /// `(foo <JOIN> bar [ <JOIN> baz ... ])`.
    /// The inner `TableWithJoins` can have no joins only if its
    /// `relation` is itself a `TableFactor::NestedJoin`.
    NestedJoin {
        join: Box<TableWithJoins<T>>,
        alias: Option<TableAlias>,
    },
}

impl<T: AstInfo> AstDisplay for TableFactor<T> {
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
impl_display_t!(TableFactor);

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
pub struct Join<T: AstInfo> {
    pub relation: TableFactor<T>,
    pub join_operator: JoinOperator<T>,
}

impl<T: AstInfo> AstDisplay for Join<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        fn prefix<T: AstInfo>(constraint: &JoinConstraint<T>) -> &'static str {
            match constraint {
                JoinConstraint::Natural => "NATURAL ",
                _ => "",
            }
        }
        fn suffix<'a, T: AstInfo>(constraint: &'a JoinConstraint<T>) -> impl AstDisplay + 'a {
            struct Suffix<'a, T: AstInfo>(&'a JoinConstraint<T>);
            impl<'a, T: AstInfo> AstDisplay for Suffix<'a, T> {
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
impl_display_t!(Join);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinOperator<T: AstInfo> {
    Inner(JoinConstraint<T>),
    LeftOuter(JoinConstraint<T>),
    RightOuter(JoinConstraint<T>),
    FullOuter(JoinConstraint<T>),
    CrossJoin,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinConstraint<T: AstInfo> {
    On(Expr<T>),
    Using(Vec<Ident>),
    Natural,
}

/// SQL ORDER BY expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderByExpr<T: AstInfo> {
    pub expr: Expr<T>,
    pub asc: Option<bool>,
}

impl<T: AstInfo> AstDisplay for OrderByExpr<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_node(&self.expr);
        match self.asc {
            Some(true) => f.write_str(" ASC"),
            Some(false) => f.write_str(" DESC"),
            None => {}
        }
    }
}
impl_display_t!(OrderByExpr);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Limit<T: AstInfo> {
    pub with_ties: bool,
    pub quantity: Expr<T>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Values<T: AstInfo>(pub Vec<Vec<Expr<T>>>);

impl<T: AstInfo> AstDisplay for Values<T> {
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
impl_display_t!(Values);
