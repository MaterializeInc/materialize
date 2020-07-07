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
use crate::ast::{
    BinaryOperator, DataType, Ident, ObjectName, OrderByExpr, Query, UnaryOperator, Value,
};

/// An SQL expression of any type.
///
/// The parser does not distinguish between expressions of different types
/// (e.g. boolean vs string), so the caller must handle expressions of
/// inappropriate type, like `WHERE 1` or `SELECT 1=1`, as necessary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expr {
    /// Identifier e.g. table name or column name
    Identifier(Vec<Ident>),
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`.
    QualifiedWildcard(Vec<Ident>),
    /// A field access, like `(expr).foo`.
    FieldAccess { expr: Box<Expr>, field: Ident },
    /// A wildcard field access, like `(expr).*`.
    ///
    /// Note that this is different from `QualifiedWildcard` in that the
    /// wildcard access occurs on an arbitrary expression, rather than a
    /// qualified name. The distinction is important for PostgreSQL
    /// compatibility.
    WildcardAccess(Box<Expr>),
    /// A positional parameter, e.g., `$1` or `$42`
    Parameter(usize),
    /// `IS NULL` expression
    IsNull { expr: Box<Expr>, negated: bool },
    /// `[ NOT ] IN (val1, val2, ...)`
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    /// `<expr> [ NOT ] BETWEEN <low> AND <high>`
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// Binary operation e.g. `1 + 1` or `foo > bar`
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Unary operation e.g. `NOT foo`
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },
    /// CAST an expression to a different data type e.g. `CAST(foo AS VARCHAR(123))`
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// `expr COLLATE collation`
    Collate {
        expr: Box<Expr>,
        collation: ObjectName,
    },
    /// COALESCE(<expr>, ...)
    ///
    /// While COALESCE has the same syntax as a function call, its semantics are
    /// extremely unusual, and are better captured with a dedicated AST node.
    Coalesce { exprs: Vec<Expr> },
    /// Nested expression e.g. `(foo > bar)` or `(1)`
    Nested(Box<Expr>),
    /// A row constructor like `ROW(<expr>...)` or `(<expr>, <expr>...)`.
    Row { exprs: Vec<Expr> },
    /// A literal value, such as string, number, date or NULL
    Value(Value),
    /// Scalar function call e.g. `LEFT(foo, 5)`
    Function(Function),
    /// `CASE [<operand>] WHEN <condition> THEN <result> ... [ELSE <result>] END`
    ///
    /// Note we only recognize a complete single expression as `<condition>`,
    /// not `< 0` nor `1, 2, 3` as allowed in a `<simple when clause>` per
    /// <https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#simple-when-clause>
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },
    /// An exists expression `EXISTS(SELECT ...)`, used in expressions like
    /// `WHERE EXISTS (SELECT ...)`.
    Exists(Box<Query>),
    /// A parenthesized subquery `(SELECT ...)`, used in expression like
    /// `SELECT (subquery) AS x` or `WHERE (subquery) = x`
    Subquery(Box<Query>),
    /// `<expr> <op> ANY/SOME (<query>)`
    Any {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Query>,
    },
    /// `<expr> <op> ALL (<query>)`
    All {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Query>,
    },
    /// `LIST[<expr>*]`
    List(Vec<Expr>),
    /// `<expr>[<expr>?(:<expr>?(, <expr>?:<expr>?)*)?]`
    Subscript {
        expr: Box<Expr>,
        is_slice: bool,
        positions: Vec<SubscriptPosition>,
    },
}

impl AstDisplay for Expr {
    fn fmt(&self, f: &mut AstFormatter) {
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
            Expr::IsNull { expr, negated } => {
                f.write_node(&expr);
                f.write_str(" IS");
                if *negated {
                    f.write_str(" NOT");
                }
                f.write_str(" NULL");
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
            Expr::BinaryOp { left, op, right } => {
                f.write_node(&left);
                f.write_str(" ");
                f.write_str(op);
                f.write_str(" ");
                f.write_node(&right);
            }
            Expr::UnaryOp { op, expr } => {
                f.write_str(op);
                f.write_str(" ");
                f.write_node(&expr);
            }
            Expr::Cast { expr, data_type } => {
                // We are potentially rewriting an expression like
                //     CAST(<expr> OP <expr> AS <type>)
                // to
                //     <expr> OP <expr>::<type>
                // which could incorrectly change the meaning of the expression
                // as the `::` binds tightly. To be safe, we wrap the inner
                // expression in parentheses
                //    (<expr> OP <expr>)::<type>
                // unless the inner expression is of a type that we know is
                // safe to follow with a `::` to without wrapping.
                let needs_wrap = match **expr {
                    Expr::Nested(_)
                    | Expr::Value(_)
                    | Expr::Cast { .. }
                    | Expr::Function { .. }
                    | Expr::Identifier { .. }
                    | Expr::Collate { .. }
                    | Expr::Coalesce { .. } => false,
                    _ => true,
                };
                if needs_wrap {
                    f.write_str('(');
                }
                f.write_node(&expr);
                if needs_wrap {
                    f.write_str(')');
                }
                f.write_str("::");
                f.write_node(data_type);
            }
            Expr::Collate { expr, collation } => {
                f.write_node(&expr);
                f.write_str(" COLLATE ");
                f.write_node(&collation);
            }
            Expr::Coalesce { exprs } => {
                f.write_str("COALESCE(");
                f.write_node(&display::comma_separated(&exprs));
                f.write_str(")");
            }
            Expr::Nested(ast) => {
                f.write_str("(");
                f.write_node(&ast);
                f.write_str(")");
            }
            Expr::Row { exprs } => {
                f.write_str("ROW(");
                f.write_node(&display::comma_separated(&exprs));
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
            Expr::Any { left, op, right } => {
                f.write_node(&left);
                f.write_str(" ");
                f.write_str(op);
                f.write_str("ANY (");
                f.write_node(&right);
                f.write_str(")");
            }
            Expr::All { left, op, right } => {
                f.write_node(&left);
                f.write_str(" ");
                f.write_str(op);
                f.write_str(" ALL (");
                f.write_node(&right);
                f.write_str(")");
            }
            Expr::List(exprs) => {
                let mut exprs = exprs.iter().peekable();
                f.write_str("LIST[");
                while let Some(expr) = exprs.next() {
                    f.write_node(expr);
                    if exprs.peek().is_some() {
                        f.write_str(", ");
                    }
                }
                f.write_str("]");
            }
            Expr::Subscript {
                expr,
                is_slice,
                positions,
            } => {
                f.write_node(&expr);
                f.write_str("[");
                if *is_slice {
                    itertools::join(positions, ",");
                } else {
                    f.write_node(positions[0].start.as_ref().unwrap());
                }
                f.write_str("]");
            }
        }
    }
}
impl_display!(Expr);

impl Expr {
    pub fn is_string_literal(&self) -> bool {
        if let Expr::Value(Value::String(_)) = self {
            true
        } else {
            false
        }
    }

    pub fn null() -> Expr {
        Expr::Value(Value::Null)
    }

    pub fn number<S>(n: S) -> Expr
    where
        S: Into<String>,
    {
        Expr::Value(Value::Number(n.into()))
    }

    pub fn negate(self) -> Expr {
        Expr::UnaryOp {
            expr: Box::new(self),
            op: UnaryOperator::Not,
        }
    }

    pub fn binop(self, op: BinaryOperator, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op,
            right: Box::new(right),
        }
    }

    pub fn and(self, right: Expr) -> Expr {
        self.binop(BinaryOperator::And, right)
    }

    pub fn or(self, right: Expr) -> Expr {
        self.binop(BinaryOperator::Or, right)
    }

    pub fn lt(self, right: Expr) -> Expr {
        self.binop(BinaryOperator::Lt, right)
    }

    pub fn lt_eq(self, right: Expr) -> Expr {
        self.binop(BinaryOperator::LtEq, right)
    }

    pub fn gt(self, right: Expr) -> Expr {
        self.binop(BinaryOperator::Gt, right)
    }

    pub fn gt_eq(self, right: Expr) -> Expr {
        self.binop(BinaryOperator::GtEq, right)
    }

    pub fn equals(self, right: Expr) -> Expr {
        self.binop(BinaryOperator::Eq, right)
    }

    pub fn minus(self, right: Expr) -> Expr {
        self.binop(BinaryOperator::Minus, right)
    }

    pub fn multiply(self, right: Expr) -> Expr {
        self.binop(BinaryOperator::Multiply, right)
    }

    pub fn modulo(self, right: Expr) -> Expr {
        self.binop(BinaryOperator::Modulus, right)
    }

    pub fn divide(self, right: Expr) -> Expr {
        self.binop(BinaryOperator::Divide, right)
    }

    pub fn call(name: &str, args: Vec<Expr>) -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec![name.into()]),
            args: FunctionArgs::Args(args),
            filter: None,
            over: None,
            distinct: false,
        })
    }

    pub fn call_nullary(name: &str) -> Expr {
        Expr::call(name, vec![])
    }

    pub fn call_unary(self, name: &str) -> Expr {
        Expr::call(name, vec![self])
    }

    pub fn take(&mut self) -> Expr {
        mem::replace(self, Expr::Identifier(vec![]))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptPosition {
    pub start: Option<Expr>,
    pub end: Option<Expr>,
}

impl AstDisplay for SubscriptPosition {
    fn fmt(&self, f: &mut AstFormatter) {
        if let Some(start) = &self.start {
            f.write_node(start);
        }
        f.write_str(":");
        if let Some(end) = &self.end {
            f.write_node(end);
        }
    }
}
impl_display!(SubscriptPosition);

/// A window specification (i.e. `OVER (PARTITION BY .. ORDER BY .. etc.)`)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub window_frame: Option<WindowFrame>,
}

impl AstDisplay for WindowSpec {
    fn fmt(&self, f: &mut AstFormatter) {
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
    }
}
impl_display!(WindowSpec);

/// Specifies the data processed by a window function, e.g.
/// `RANGE UNBOUNDED PRECEDING` or `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW`.
///
/// Note: The parser does not validate the specified bounds; the caller should
/// reject invalid bounds like `ROWS UNBOUNDED FOLLOWING` before execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start_bound: WindowFrameBound,
    /// The right bound of the `BETWEEN .. AND` clause. The end bound of `None`
    /// indicates the shorthand form (e.g. `ROWS 1 PRECEDING`), which must
    /// behave the same as `end_bound = WindowFrameBound::CurrentRow`.
    pub end_bound: Option<WindowFrameBound>,
    // TBD: EXCLUDE
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

impl AstDisplay for WindowFrameUnits {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str(match self {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
        })
    }
}
impl_display!(WindowFrameUnits);

/// Specifies [WindowFrame]'s `start_bound` and `end_bound`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFrameBound {
    /// `CURRENT ROW`
    CurrentRow,
    /// `<N> PRECEDING` or `UNBOUNDED PRECEDING`
    Preceding(Option<u64>),
    /// `<N> FOLLOWING` or `UNBOUNDED FOLLOWING`.
    Following(Option<u64>),
}

impl AstDisplay for WindowFrameBound {
    fn fmt(&self, f: &mut AstFormatter) {
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Function {
    pub name: ObjectName,
    pub args: FunctionArgs,
    // aggregate functions may specify e.g. `COUNT(DISTINCT X) FILTER (WHERE ...)`
    pub filter: Option<Box<Expr>>,
    pub over: Option<WindowSpec>,
    // aggregate functions may specify eg `COUNT(DISTINCT x)`
    pub distinct: bool,
}

impl AstDisplay for Function {
    fn fmt(&self, f: &mut AstFormatter) {
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
            f.write_str(" OVER (");
            f.write_node(o);
            f.write_str(")");
        }
    }
}
impl_display!(Function);

/// Arguments for a function call.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FunctionArgs {
    /// The special star argument, as in `count(*)`.
    Star,
    /// A normal list of arguments.
    Args(Vec<Expr>),
}

impl AstDisplay for FunctionArgs {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            FunctionArgs::Star => f.write_str("*"),
            FunctionArgs::Args(args) => f.write_node(&display::comma_separated(&args)),
        }
    }
}
impl_display!(FunctionArgs);
