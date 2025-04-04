// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. and contributors. All rights reserved.
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

use std::{fmt, mem};

use mz_ore::soft_assert_eq_or_log;
use mz_sql_lexer::keywords::*;

use crate::ast::display::{self, AstDisplay, AstFormatter};
use crate::ast::{AstInfo, Ident, OrderByExpr, Query, UnresolvedItemName, Value};

/// An SQL expression of any type.
///
/// The parser does not distinguish between expressions of different types
/// (e.g. boolean vs string), so the caller must handle expressions of
/// inappropriate type, like `WHERE 1` or `SELECT 1=1`, as necessary.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Expr<T: AstInfo> {
    /// Identifier e.g. table name or column name
    Identifier(Vec<Ident>),
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`.
    QualifiedWildcard(Vec<Ident>),
    /// A field access, like `(expr).foo`.
    FieldAccess {
        expr: Box<Expr<T>>,
        field: Ident,
    },
    /// A wildcard field access, like `(expr).*`.
    ///
    /// Note that this is different from `QualifiedWildcard` in that the
    /// wildcard access occurs on an arbitrary expression, rather than a
    /// qualified name. The distinction is important for PostgreSQL
    /// compatibility.
    WildcardAccess(Box<Expr<T>>),
    /// A positional parameter, e.g., `$1` or `$42`
    Parameter(usize),
    /// Boolean negation
    Not {
        expr: Box<Expr<T>>,
    },
    /// Boolean and
    And {
        left: Box<Expr<T>>,
        right: Box<Expr<T>>,
    },
    /// Boolean or
    Or {
        left: Box<Expr<T>>,
        right: Box<Expr<T>>,
    },
    /// `IS {NULL, TRUE, FALSE, UNKNOWN}` expression
    IsExpr {
        expr: Box<Expr<T>>,
        construct: IsExprConstruct<T>,
        negated: bool,
    },
    /// `[ NOT ] IN (val1, val2, ...)`
    InList {
        expr: Box<Expr<T>>,
        list: Vec<Expr<T>>,
        negated: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        expr: Box<Expr<T>>,
        subquery: Box<Query<T>>,
        negated: bool,
    },
    /// `<expr> [ NOT ] {LIKE, ILIKE} <pattern> [ ESCAPE <escape> ]`
    Like {
        expr: Box<Expr<T>>,
        pattern: Box<Expr<T>>,
        escape: Option<Box<Expr<T>>>,
        case_insensitive: bool,
        negated: bool,
    },
    /// `<expr> [ NOT ] BETWEEN <low> AND <high>`
    Between {
        expr: Box<Expr<T>>,
        negated: bool,
        low: Box<Expr<T>>,
        high: Box<Expr<T>>,
    },
    /// Unary or binary operator
    Op {
        op: Op,
        expr1: Box<Expr<T>>,
        expr2: Option<Box<Expr<T>>>,
    },
    /// CAST an expression to a different data type e.g. `CAST(foo AS VARCHAR(123))`
    Cast {
        expr: Box<Expr<T>>,
        data_type: T::DataType,
    },
    /// `expr COLLATE collation`
    Collate {
        expr: Box<Expr<T>>,
        collation: UnresolvedItemName,
    },
    /// `COALESCE(<expr>, ...)` or `GREATEST(<expr>, ...)` or `LEAST(<expr>`, ...)
    ///
    /// While COALESCE/GREATEST/LEAST have the same syntax as a function call,
    /// their semantics are extremely unusual, and are better captured with a
    /// dedicated AST node.
    HomogenizingFunction {
        function: HomogenizingFunction,
        exprs: Vec<Expr<T>>,
    },
    /// NULLIF(expr, expr)
    ///
    /// While NULLIF has the same syntax as a function call, it is not evaluated
    /// as a function within Postgres.
    NullIf {
        l_expr: Box<Expr<T>>,
        r_expr: Box<Expr<T>>,
    },
    /// Nested expression e.g. `(foo > bar)` or `(1)`
    Nested(Box<Expr<T>>),
    /// A row constructor like `ROW(<expr>...)` or `(<expr>, <expr>...)`.
    Row {
        exprs: Vec<Expr<T>>,
    },
    /// A literal value, such as string, number, date or NULL
    Value(Value),
    /// Scalar function call e.g. `LEFT(foo, 5)`
    Function(Function<T>),
    /// `CASE [<operand>] WHEN <condition> THEN <result> ... [ELSE <result>] END`
    ///
    /// Note we only recognize a complete single expression as `<condition>`,
    /// not `< 0` nor `1, 2, 3` as allowed in a `<simple when clause>` per
    /// <https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#simple-when-clause>
    Case {
        operand: Option<Box<Expr<T>>>,
        conditions: Vec<Expr<T>>,
        results: Vec<Expr<T>>,
        else_result: Option<Box<Expr<T>>>,
    },
    /// An exists expression `EXISTS(SELECT ...)`, used in expressions like
    /// `WHERE EXISTS (SELECT ...)`.
    Exists(Box<Query<T>>),
    /// A parenthesized subquery `(SELECT ...)`, used in expression like
    /// `SELECT (subquery) AS x` or `WHERE (subquery) = x`
    Subquery(Box<Query<T>>),
    /// `<expr> <op> ANY/SOME (<query>)`
    AnySubquery {
        left: Box<Expr<T>>,
        op: Op,
        right: Box<Query<T>>,
    },
    /// `<expr> <op> ANY (<array_expr>)`
    AnyExpr {
        left: Box<Expr<T>>,
        op: Op,
        right: Box<Expr<T>>,
    },
    /// `<expr> <op> ALL (<query>)`
    AllSubquery {
        left: Box<Expr<T>>,
        op: Op,
        right: Box<Query<T>>,
    },
    /// `<expr> <op> ALL (<array_expr>)`
    AllExpr {
        left: Box<Expr<T>>,
        op: Op,
        right: Box<Expr<T>>,
    },
    /// `ARRAY[<expr>*]`
    Array(Vec<Expr<T>>),
    ArraySubquery(Box<Query<T>>),
    /// `LIST[<expr>*]`
    List(Vec<Expr<T>>),
    ListSubquery(Box<Query<T>>),
    /// `MAP[<expr>*]`
    Map(Vec<MapEntry<T>>),
    MapSubquery(Box<Query<T>>),
    /// `<expr>([<expr>(:<expr>)?])+`
    Subscript {
        expr: Box<Expr<T>>,
        positions: Vec<SubscriptPosition<T>>,
    },
}

impl<T: AstInfo> AstDisplay for Expr<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Expr::Identifier(s) => f.write_node(&display::separated(s, ".")),
            Expr::QualifiedWildcard(q) => {
                f.write_node(&display::separated(q, "."));
                f.write_str(".*");
            }
            Expr::FieldAccess { expr, field } => {
                f.write_node(expr);
                f.write_str(".");
                f.write_node(field);
            }
            Expr::WildcardAccess(expr) => {
                f.write_node(expr);
                f.write_str(".*");
            }
            Expr::Parameter(n) => f.write_str(&format!("${}", n)),
            Expr::Not { expr } => {
                f.write_str("NOT ");
                f.write_node(expr);
            }
            Expr::And { left, right } => {
                f.write_node(left);
                f.write_str(" AND ");
                f.write_node(right);
            }
            Expr::Or { left, right } => {
                f.write_node(left);
                f.write_str(" OR ");
                f.write_node(right);
            }
            Expr::IsExpr {
                expr,
                negated,
                construct,
            } => {
                f.write_node(&expr);
                f.write_str(" IS ");
                if *negated {
                    f.write_str("NOT ");
                }
                f.write_node(construct);
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                f.write_node(&expr);
                f.write_str(" ");
                if *negated {
                    f.write_str("NOT ");
                }
                f.write_str("IN (");
                f.write_node(&display::comma_separated(list));
                f.write_str(")");
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                f.write_node(&expr);
                f.write_str(" ");
                if *negated {
                    f.write_str("NOT ");
                }
                f.write_str("IN (");
                f.write_node(&subquery);
                f.write_str(")");
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => {
                f.write_node(&expr);
                f.write_str(" ");
                if *negated {
                    f.write_str("NOT ");
                }
                if *case_insensitive {
                    f.write_str("I");
                }
                f.write_str("LIKE ");
                f.write_node(&pattern);
                if let Some(escape) = escape {
                    f.write_str(" ESCAPE ");
                    f.write_node(escape);
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                f.write_node(&expr);
                if *negated {
                    f.write_str(" NOT");
                }
                f.write_str(" BETWEEN ");
                f.write_node(&low);
                f.write_str(" AND ");
                f.write_node(&high);
            }
            Expr::Op { op, expr1, expr2 } => {
                if let Some(expr2) = expr2 {
                    f.write_node(&expr1);
                    f.write_str(" ");
                    f.write_str(op);
                    f.write_str(" ");
                    f.write_node(&expr2);
                } else {
                    f.write_str(op);
                    f.write_str(" ");
                    f.write_node(&expr1);
                }
            }
            Expr::Cast { expr, data_type } => {
                f.write_node(&expr);
                f.write_str("::");
                f.write_node(data_type);
            }
            Expr::Collate { expr, collation } => {
                f.write_node(&expr);
                f.write_str(" COLLATE ");
                f.write_node(&collation);
            }
            Expr::HomogenizingFunction { function, exprs } => {
                f.write_node(function);
                f.write_str("(");
                f.write_node(&display::comma_separated(exprs));
                f.write_str(")");
            }
            Expr::NullIf { l_expr, r_expr } => {
                f.write_str("NULLIF(");
                f.write_node(&display::comma_separated(&[l_expr, r_expr]));
                f.write_str(")");
            }
            Expr::Nested(ast) => {
                f.write_str("(");
                f.write_node(&ast);
                f.write_str(")");
            }
            Expr::Row { exprs } => {
                f.write_str("ROW(");
                f.write_node(&display::comma_separated(exprs));
                f.write_str(")");
            }
            Expr::Value(v) => {
                f.write_node(v);
            }
            Expr::Function(fun) => {
                f.write_node(fun);
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                f.write_str("CASE");
                if let Some(operand) = operand {
                    f.write_str(" ");
                    f.write_node(&operand);
                }
                for (c, r) in conditions.iter().zip(results) {
                    f.write_str(" WHEN ");
                    f.write_node(c);
                    f.write_str(" THEN ");
                    f.write_node(r);
                }

                if let Some(else_result) = else_result {
                    f.write_str(" ELSE ");
                    f.write_node(&else_result);
                }
                f.write_str(" END")
            }
            Expr::Exists(s) => {
                f.write_str("EXISTS (");
                f.write_node(&s);
                f.write_str(")");
            }
            Expr::Subquery(s) => {
                f.write_str("(");
                f.write_node(&s);
                f.write_str(")");
            }
            Expr::AnySubquery { left, op, right } => {
                f.write_node(&left);
                f.write_str(" ");
                f.write_str(op);
                f.write_str(" ANY (");
                f.write_node(&right);
                f.write_str(")");
            }
            Expr::AnyExpr { left, op, right } => {
                f.write_node(&left);
                f.write_str(" ");
                f.write_str(op);
                f.write_str(" ANY (");
                f.write_node(&right);
                f.write_str(")");
            }
            Expr::AllSubquery { left, op, right } => {
                f.write_node(&left);
                f.write_str(" ");
                f.write_str(op);
                f.write_str(" ALL (");
                f.write_node(&right);
                f.write_str(")");
            }
            Expr::AllExpr { left, op, right } => {
                f.write_node(&left);
                f.write_str(" ");
                f.write_str(op);
                f.write_str(" ALL (");
                f.write_node(&right);
                f.write_str(")");
            }
            Expr::Array(exprs) => {
                f.write_str("ARRAY[");
                f.write_node(&display::comma_separated(exprs));
                f.write_str("]");
            }
            Expr::ArraySubquery(s) => {
                f.write_str("ARRAY(");
                f.write_node(&s);
                f.write_str(")");
            }
            Expr::List(exprs) => {
                f.write_str("LIST[");
                f.write_node(&display::comma_separated(exprs));
                f.write_str("]");
            }
            Expr::ListSubquery(s) => {
                f.write_str("LIST(");
                f.write_node(&s);
                f.write_str(")");
            }
            Expr::Map(exprs) => {
                f.write_str("MAP[");
                f.write_node(&display::comma_separated(exprs));
                f.write_str("]");
            }
            Expr::MapSubquery(s) => {
                f.write_str("MAP(");
                f.write_node(&s);
                f.write_str(")");
            }
            Expr::Subscript { expr, positions } => {
                f.write_node(&expr);
                f.write_str("[");

                let mut first = true;

                for p in positions {
                    if first {
                        first = false
                    } else {
                        f.write_str("][");
                    }
                    f.write_node(p);
                }

                f.write_str("]");
            }
        }
    }
}
impl_display_t!(Expr);

impl<T: AstInfo> Expr<T> {
    pub fn null() -> Expr<T> {
        Expr::Value(Value::Null)
    }

    pub fn number<S>(n: S) -> Expr<T>
    where
        S: Into<String>,
    {
        Expr::Value(Value::Number(n.into()))
    }

    pub fn negate(self) -> Expr<T> {
        Expr::Not {
            expr: Box::new(self),
        }
    }

    pub fn and(self, right: Expr<T>) -> Expr<T> {
        Expr::And {
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    pub fn or(self, right: Expr<T>) -> Expr<T> {
        Expr::Or {
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    pub fn binop(self, op: Op, right: Expr<T>) -> Expr<T> {
        Expr::Op {
            op,
            expr1: Box::new(self),
            expr2: Some(Box::new(right)),
        }
    }

    pub fn lt(self, right: Expr<T>) -> Expr<T> {
        self.binop(Op::bare("<"), right)
    }

    pub fn lt_eq(self, right: Expr<T>) -> Expr<T> {
        self.binop(Op::bare("<="), right)
    }

    pub fn gt(self, right: Expr<T>) -> Expr<T> {
        self.binop(Op::bare(">"), right)
    }

    pub fn gt_eq(self, right: Expr<T>) -> Expr<T> {
        self.binop(Op::bare(">="), right)
    }

    pub fn equals(self, right: Expr<T>) -> Expr<T> {
        self.binop(Op::bare("="), right)
    }

    pub fn not_equals(self, right: Expr<T>) -> Expr<T> {
        self.binop(Op::bare("<>"), right)
    }

    pub fn minus(self, right: Expr<T>) -> Expr<T> {
        self.binop(Op::bare("-"), right)
    }

    pub fn multiply(self, right: Expr<T>) -> Expr<T> {
        self.binop(Op::bare("*"), right)
    }

    pub fn modulo(self, right: Expr<T>) -> Expr<T> {
        self.binop(Op::bare("%"), right)
    }

    pub fn divide(self, right: Expr<T>) -> Expr<T> {
        self.binop(Op::bare("/"), right)
    }

    pub fn cast(self, data_type: T::DataType) -> Expr<T> {
        Expr::Cast {
            expr: Box::new(self),
            data_type,
        }
    }

    pub fn call(name: T::ItemName, args: Vec<Expr<T>>) -> Expr<T> {
        Expr::Function(Function {
            name,
            args: FunctionArgs::args(args),
            filter: None,
            over: None,
            distinct: false,
        })
    }

    pub fn call_nullary(name: T::ItemName) -> Expr<T> {
        Expr::call(name, vec![])
    }

    pub fn call_unary(self, name: T::ItemName) -> Expr<T> {
        Expr::call(name, vec![self])
    }

    pub fn take(&mut self) -> Expr<T> {
        mem::replace(self, Expr::Identifier(vec![]))
    }
}

/// A reference to an operator.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Op {
    /// Any namespaces that preceded the operator.
    pub namespace: Option<Vec<Ident>>,
    /// The operator itself.
    pub op: String,
}

impl AstDisplay for Op {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        if let Some(namespace) = &self.namespace {
            f.write_str("OPERATOR(");
            for name in namespace {
                f.write_node(name);
                f.write_str(".");
            }
            f.write_str(&self.op);
            f.write_str(")");
        } else {
            f.write_str(&self.op)
        }
    }
}
impl_display!(Op);

impl Op {
    /// Constructs a new unqualified operator reference.
    pub fn bare<S>(op: S) -> Op
    where
        S: Into<String>,
    {
        Op {
            namespace: None,
            op: op.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HomogenizingFunction {
    Coalesce,
    Greatest,
    Least,
}

impl AstDisplay for HomogenizingFunction {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            HomogenizingFunction::Coalesce => f.write_str("COALESCE"),
            HomogenizingFunction::Greatest => f.write_str("GREATEST"),
            HomogenizingFunction::Least => f.write_str("LEAST"),
        }
    }
}
impl_display!(HomogenizingFunction);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MapEntry<T: AstInfo> {
    pub key: Expr<T>,
    pub value: Expr<T>,
}

impl<T: AstInfo> AstDisplay for MapEntry<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.key);
        f.write_str(" => ");
        f.write_node(&self.value);
    }
}
impl_display_t!(MapEntry);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriptPosition<T: AstInfo> {
    pub start: Option<Expr<T>>,
    pub end: Option<Expr<T>>,
    // i.e. did this subscript include a colon
    pub explicit_slice: bool,
}

impl<T: AstInfo> AstDisplay for SubscriptPosition<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        if let Some(start) = &self.start {
            f.write_node(start);
        }
        if self.explicit_slice {
            f.write_str(":");
            if let Some(end) = &self.end {
                f.write_node(end);
            }
        }
    }
}
impl_display_t!(SubscriptPosition);

/// A window specification (i.e. `OVER (PARTITION BY .. ORDER BY .. etc.)`)
/// Includes potential IGNORE NULLS or RESPECT NULLS from before the OVER clause.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WindowSpec<T: AstInfo> {
    pub partition_by: Vec<Expr<T>>,
    pub order_by: Vec<OrderByExpr<T>>,
    pub window_frame: Option<WindowFrame>,
    // Note that IGNORE NULLS and RESPECT NULLS are mutually exclusive. We validate that not both
    // are present during HIR planning.
    pub ignore_nulls: bool,
    pub respect_nulls: bool,
}

impl<T: AstInfo> AstDisplay for WindowSpec<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        if self.ignore_nulls {
            f.write_str(" IGNORE NULLS");
        }
        if self.respect_nulls {
            f.write_str(" RESPECT NULLS");
        }
        f.write_str(" OVER (");
        let mut delim = "";
        if !self.partition_by.is_empty() {
            delim = " ";
            f.write_str("PARTITION BY ");
            f.write_node(&display::comma_separated(&self.partition_by));
        }
        if !self.order_by.is_empty() {
            f.write_str(delim);
            delim = " ";
            f.write_str("ORDER BY ");
            f.write_node(&display::comma_separated(&self.order_by));
        }
        if let Some(window_frame) = &self.window_frame {
            if let Some(end_bound) = &window_frame.end_bound {
                f.write_str(delim);
                f.write_node(&window_frame.units);
                f.write_str(" BETWEEN ");
                f.write_node(&window_frame.start_bound);
                f.write_str(" AND ");
                f.write_node(&*end_bound);
            } else {
                f.write_str(delim);
                f.write_node(&window_frame.units);
                f.write_str(" ");
                f.write_node(&window_frame.start_bound);
            }
        }
        f.write_str(")");
    }
}
impl_display_t!(WindowSpec);

/// Specifies the data processed by a window function, e.g.
/// `RANGE UNBOUNDED PRECEDING` or `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW`.
///
/// Note: The parser does not validate the specified bounds; the caller should
/// reject invalid bounds like `ROWS UNBOUNDED FOLLOWING` before execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start_bound: WindowFrameBound,
    /// The right bound of the `BETWEEN .. AND` clause. The end bound of `None`
    /// indicates the shorthand form (e.g. `ROWS 1 PRECEDING`), which must
    /// behave the same as `end_bound = WindowFrameBound::CurrentRow`.
    pub end_bound: Option<WindowFrameBound>,
    // TBD: EXCLUDE
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

impl AstDisplay for WindowFrameUnits {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
        })
    }
}
impl_display!(WindowFrameUnits);

/// Specifies [WindowFrame]'s `start_bound` and `end_bound`
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WindowFrameBound {
    /// `CURRENT ROW`
    CurrentRow,
    /// `<N> PRECEDING` or `UNBOUNDED PRECEDING`
    Preceding(Option<u64>),
    /// `<N> FOLLOWING` or `UNBOUNDED FOLLOWING`.
    Following(Option<u64>),
}

impl AstDisplay for WindowFrameBound {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            WindowFrameBound::CurrentRow => f.write_str("CURRENT ROW"),
            WindowFrameBound::Preceding(None) => f.write_str("UNBOUNDED PRECEDING"),
            WindowFrameBound::Following(None) => f.write_str("UNBOUNDED FOLLOWING"),
            WindowFrameBound::Preceding(Some(n)) => {
                f.write_str(n);
                f.write_str(" PRECEDING");
            }
            WindowFrameBound::Following(Some(n)) => {
                f.write_str(n);
                f.write_str(" FOLLOWING");
            }
        }
    }
}
impl_display!(WindowFrameBound);

/// A function call
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Function<T: AstInfo> {
    pub name: T::ItemName,
    pub args: FunctionArgs<T>,
    // aggregate functions may specify e.g. `COUNT(DISTINCT X) FILTER (WHERE ...)`
    pub filter: Option<Box<Expr<T>>>,
    pub over: Option<WindowSpec<T>>,
    // aggregate functions may specify eg `COUNT(DISTINCT x)`
    pub distinct: bool,
}

impl<T: AstInfo> AstDisplay for Function<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        // This block handles printing function calls that have special parsing. In stable mode, the
        // name is quoted and so won't get the special parsing. We only need to print the special
        // formats in non-stable mode.
        if !f.stable() {
            let special: Option<(&str, &[Option<Keyword>])> =
                match self.name.to_ast_string_stable().as_str() {
                    r#""extract""# if self.args.len() == Some(2) => {
                        Some(("extract", &[None, Some(FROM)]))
                    }
                    r#""position""# if self.args.len() == Some(2) => {
                        Some(("position", &[None, Some(IN)]))
                    }

                    // "trim" doesn't need to appear here because it changes the function name (to
                    // "btrim", "ltrim", or "rtrim"), but only "trim" is parsed specially. "substring"
                    // supports comma-delimited arguments, so doesn't need to be here.
                    _ => None,
                };
            if let Some((name, kws)) = special {
                f.write_str(name);
                f.write_str("(");
                self.args.intersperse_function_argument_keywords(f, kws);
                f.write_str(")");
                return;
            }
        }

        f.write_node(&self.name);
        f.write_str("(");
        if self.distinct {
            f.write_str("DISTINCT ")
        }
        f.write_node(&self.args);
        f.write_str(")");
        if let Some(filter) = &self.filter {
            f.write_str(" FILTER (WHERE ");
            f.write_node(&filter);
            f.write_str(")");
        }
        if let Some(o) = &self.over {
            f.write_node(o);
        }
    }
}
impl_display_t!(Function);

/// Arguments for a function call.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FunctionArgs<T: AstInfo> {
    /// The special star argument, as in `count(*)`.
    Star,
    /// A normal list of arguments.
    Args {
        args: Vec<Expr<T>>,
        order_by: Vec<OrderByExpr<T>>,
    },
}

impl<T: AstInfo> FunctionArgs<T> {
    pub fn args(args: Vec<Expr<T>>) -> Self {
        Self::Args {
            args,
            order_by: vec![],
        }
    }

    /// Returns the number of arguments. Star (`*`) is None.
    pub fn len(&self) -> Option<usize> {
        match self {
            FunctionArgs::Star => None,
            FunctionArgs::Args { args, .. } => Some(args.len()),
        }
    }

    /// Prints associated keywords before each argument
    fn intersperse_function_argument_keywords<W: fmt::Write>(
        &self,
        f: &mut AstFormatter<W>,
        kws: &[Option<Keyword>],
    ) {
        let args = match self {
            FunctionArgs::Star => unreachable!(),
            FunctionArgs::Args { args, .. } => args,
        };
        soft_assert_eq_or_log!(args.len(), kws.len());
        let mut delim = "";
        for (arg, kw) in args.iter().zip(kws) {
            if let Some(kw) = kw {
                f.write_str(delim);
                f.write_str(kw.as_str());
                delim = " ";
            }
            f.write_str(delim);
            f.write_node(arg);
            delim = " ";
        }
    }
}

impl<T: AstInfo> AstDisplay for FunctionArgs<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            FunctionArgs::Star => f.write_str("*"),
            FunctionArgs::Args { args, order_by } => {
                f.write_node(&display::comma_separated(args));
                if !order_by.is_empty() {
                    f.write_str(" ORDER BY ");
                    f.write_node(&display::comma_separated(order_by));
                }
            }
        }
    }
}
impl_display_t!(FunctionArgs);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum IsExprConstruct<T: AstInfo> {
    Null,
    True,
    False,
    Unknown,
    DistinctFrom(Box<Expr<T>>),
}

impl<T: AstInfo> AstDisplay for IsExprConstruct<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            IsExprConstruct::Null => f.write_str("NULL"),
            IsExprConstruct::True => f.write_str("TRUE"),
            IsExprConstruct::False => f.write_str("FALSE"),
            IsExprConstruct::Unknown => f.write_str("UNKNOWN"),
            IsExprConstruct::DistinctFrom(e) => {
                f.write_str("DISTINCT FROM ");
                e.fmt(f);
            }
        }
    }
}
impl_display_t!(IsExprConstruct);
