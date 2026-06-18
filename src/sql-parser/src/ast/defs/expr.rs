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

use itertools::Itertools;
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
    /// Identifier e.g. table name or column name. The parser always
    /// constructs this with a non-empty `Vec`.
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
                write_dot_receiver(f, expr);
                f.write_str(".");
                f.write_node(field);
            }
            Expr::WildcardAccess(expr) => {
                write_dot_receiver(f, expr);
                f.write_str(".*");
            }
            Expr::Parameter(n) => f.write_str(&format!("${}", n)),
            Expr::Not { expr } => {
                f.write_str("NOT ");
                // `NOT` binds tighter than `AND`/`OR`, so an operand exposing a
                // looser operator on its left spine (`NOT (a OR b)`) must keep its
                // parens once `Nested` is stripped — use `left_edge`, since the
                // `NOT` sits to the operand's left.
                write_binary_operand(f, expr, left_edge(expr) < prec::NOT);
            }
            Expr::And { left, right } => {
                write_binary_operand(f, left, right_edge(left) < prec::AND);
                f.write_str(" AND ");
                write_binary_operand(f, right, left_edge(right) <= prec::AND);
            }
            Expr::Or { left, right } => {
                write_binary_operand(f, left, right_edge(left) < prec::OR);
                f.write_str(" OR ");
                write_binary_operand(f, right, left_edge(right) <= prec::OR);
            }
            Expr::IsExpr {
                expr,
                negated,
                construct,
            } => {
                write_binary_operand(f, expr, right_edge(expr) < prec::IS);
                f.write_str(" IS ");
                if *negated {
                    f.write_str("NOT ");
                }
                // `IS DISTINCT FROM <rhs>` parses the RHS at the `IS` precedence
                // (see `Parser::parse_is`), so a RHS whose left spine binds at or
                // below `IS` re-associates out of the `IS` unless parenthesized
                // (`a IS DISTINCT FROM b OR c` is `(a IS DISTINCT FROM b) OR c`).
                // The other constructs (`NULL`/`TRUE`/…) are bare keywords.
                if let IsExprConstruct::DistinctFrom(rhs) = construct {
                    f.write_str("DISTINCT FROM ");
                    write_binary_operand(f, rhs, left_edge(rhs) <= prec::IS);
                } else {
                    f.write_node(construct);
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                write_binary_operand(f, expr, right_edge(expr) < prec::LIKE);
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
                write_binary_operand(f, expr, right_edge(expr) < prec::LIKE);
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
                write_binary_operand(f, expr, right_edge(expr) < prec::LIKE);
                f.write_str(" ");
                if *negated {
                    f.write_str("NOT ");
                }
                if *case_insensitive {
                    f.write_str("I");
                }
                f.write_str("LIKE ");
                // The pattern and escape parse at `Like` precedence and sit to the
                // right of the keyword, so an operand exposing a precedence at or
                // below `Like` on its left spine (e.g. an `IN`/`LIKE`/`BETWEEN` at
                // equal precedence) re-associates unless parenthesized —
                // `a LIKE b IN (q)` parses as `(a LIKE b) IN (q)`. When an `ESCAPE`
                // follows, the pattern is *also* immediately left of `ESCAPE`: a
                // `[I]LIKE` exposed on the pattern's right spine would steal the
                // `ESCAPE` as its own (`a LIKE NOT b LIKE c ESCAPE d` parses the
                // escape onto the inner `b LIKE c`), so guard the right edge too.
                let pattern_parens = left_edge(pattern) <= prec::LIKE
                    || (escape.is_some() && right_edge(pattern) <= prec::LIKE);
                write_binary_operand(f, pattern, pattern_parens);
                if let Some(escape) = escape {
                    f.write_str(" ESCAPE ");
                    write_binary_operand(f, escape, left_edge(escape) <= prec::LIKE);
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                // The subject is the LHS of the `BETWEEN` infix (parsed at
                // `Like`); a spine exposed at or below `Like` on its right would
                // pull `BETWEEN` inside it (`a OR b BETWEEN …` is `a OR (b BETWEEN
                // …)`), so parenthesize via `right_edge`. Parser ASTs wrap such a
                // subject in `Nested` (`ATOM`), so they're unaffected.
                write_binary_operand(f, expr, right_edge(expr) < prec::LIKE);
                if *negated {
                    f.write_str(" NOT");
                }
                f.write_str(" BETWEEN ");
                write_between_bound(f, low);
                f.write_str(" AND ");
                write_between_bound(f, high);
            }
            Expr::Op { op, expr1, expr2 } => {
                if let Some(expr2) = expr2 {
                    // Binary operators are left-associative, so an operand that
                    // re-associates on reparse must be parenthesized. The left
                    // operand needs parens when the operator (printed to its
                    // right) reaches into its right spine at a strictly looser
                    // precedence (`right_edge`); the right operand needs parens
                    // when the operator (printed to its left) reaches into its
                    // left spine at an equal-or-looser precedence (`left_edge`,
                    // `<=` because equal precedence re-associates left, so
                    // `a - (b - c)` must keep its parens). The parser wraps such
                    // operands in `Expr::Nested` (which ranks `ATOM` on both
                    // edges, so it never re-parenthesizes), leaving parser-produced
                    // ASTs unchanged; this re-adds parens for ASTs that lost them.
                    let p = binary_op_precedence(op);
                    write_binary_operand(f, expr1, right_edge(expr1) < p);
                    f.write_str(" ");
                    f.write_str(op);
                    f.write_str(" ");
                    write_binary_operand(f, expr2, left_edge(expr2) <= p);
                } else {
                    f.write_str(op);
                    f.write_str(" ");
                    // A prefix operator binds tighter than `COLLATE` and the
                    // binary operators but looser than the postfix `::`/`[…]`
                    // forms, and `- <number>` lexes as a negative literal, so a
                    // low-precedence or numeric-leftmost operand must be
                    // parenthesized to keep the prefix operator's scope.
                    if prefix_operand_needs_parens(expr1.as_ref()) {
                        f.write_str("(");
                        f.write_node(&expr1);
                        f.write_str(")");
                    } else {
                        f.write_node(&expr1);
                    }
                }
            }
            Expr::Cast { expr, data_type } => {
                // `::` binds very tightly, so a non-self-delimiting operand must
                // be parenthesized or the cast re-associates into its spine —
                // `CAST(-0 AS int4)` (i.e. `Cast(- 0)`) would otherwise print as
                // `- 0::int4` and reparse as `- (0::int4)`. The parser wraps such
                // operands in `Expr::Nested`, but `normalize` strips those, so the
                // printer must re-add them (mirrors the `Collate` arm; `Nested` is
                // itself self-delimiting, so parser-produced ASTs don't double up).
                if prints_self_delimiting(expr) {
                    f.write_node(&expr);
                } else {
                    f.write_str("(");
                    f.write_node(&expr);
                    f.write_str(")");
                }
                f.write_str("::");
                f.write_node(data_type);
            }
            Expr::Collate { expr, collation } => {
                // `COLLATE` binds very tightly (`PostfixCollateAt`), so a
                // low-precedence operand must be parenthesized or the collation
                // re-associates onto its rightmost sub-operand — `a + b COLLATE c`
                // would reparse as `a + (b COLLATE c)`. (Round-trip parens are
                // stripped by `normalize`, so the printer must re-add them.)
                if prints_self_delimiting(expr) {
                    f.write_node(&expr);
                } else {
                    f.write_str("(");
                    f.write_node(&expr);
                    f.write_str(")");
                }
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
                for (c, r) in conditions.iter().zip_eq(results) {
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
                write_quantified_left(f, left);
                f.write_str(" ");
                f.write_str(op);
                f.write_str(" ANY (");
                f.write_node(&right);
                f.write_str(")");
            }
            Expr::AnyExpr { left, op, right } => {
                write_quantified_left(f, left);
                f.write_str(" ");
                f.write_str(op);
                f.write_str(" ANY (");
                f.write_node(&right);
                f.write_str(")");
            }
            Expr::AllSubquery { left, op, right } => {
                write_quantified_left(f, left);
                f.write_str(" ");
                f.write_str(op);
                f.write_str(" ALL (");
                f.write_node(&right);
                f.write_str(")");
            }
            Expr::AllExpr { left, op, right } => {
                write_quantified_left(f, left);
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
                write_subscript_receiver(f, expr);
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

/// Write `expr` as the receiver of a `.` operator (used by `FieldAccess` and
/// `WildcardAccess`), parenthesizing when the receiver could re-bind the
/// trailing dot on reparse. The `.` token has very high precedence and both
/// the lexer and parser greedily extend adjacent tokens: `1.x` tokenizes the
/// number `1.` and leaves `x` as an alias, and `'a'::T.x` consumes `T.x` as a
/// qualified type name. The whitelist below covers receivers that print as
/// self-terminating syntax (parenthesized exprs, function calls, bracketed
/// collections, etc.); anything else gets explicit parens.
///
/// A bare `Identifier`/`QualifiedWildcard` receiver is *not* safe: `a` then
/// `.b`/`.*` prints as `a.b`/`a.*`, which reparses as the qualified identifier
/// `Identifier([a, b])` / `QualifiedWildcard([a])` rather than a field/wildcard
/// access. The parser only ever builds those accesses over a parenthesized
/// receiver (`(a).b`), so it wraps the name in `Expr::Nested`; a bare name here
/// is a `Nested`-stripped AST and must be re-parenthesized. A `FieldAccess` /
/// `WildcardAccess` receiver *is* safe, because its own printing already
/// parenthesizes a bare-name base (`(a).b.c`), so the chain stays self-delimiting.
///
/// The quantified-subquery forms (`AnySubquery`/`AllSubquery`, printed
/// `<expr> <op> ANY (<query>)`) are likewise *not* safe: they end in a `(query)`
/// that is only a sub-part, so a trailing `.x`/`.*` binds to that inner subquery
/// rather than the whole expression. (Contrast `Subquery`/`ArraySubquery`/… which
/// are a single `(…)`/`ARRAY(…)` primary, so a trailing dot attaches to the whole
/// thing.)
fn write_dot_receiver<W: fmt::Write, T: AstInfo>(f: &mut AstFormatter<W>, expr: &Expr<T>) {
    let safe = matches!(
        expr,
        Expr::FieldAccess { .. }
            | Expr::WildcardAccess(_)
            | Expr::Parameter(_)
            | Expr::Nested(_)
            | Expr::Row { .. }
            | Expr::Function(_)
            | Expr::Case { .. }
            | Expr::Exists(_)
            | Expr::Subquery(_)
            | Expr::Array(_)
            | Expr::ArraySubquery(_)
            | Expr::List(_)
            | Expr::ListSubquery(_)
            | Expr::Map(_)
            | Expr::MapSubquery(_)
            | Expr::Subscript { .. }
            | Expr::HomogenizingFunction { .. }
            | Expr::NullIf { .. }
            | Expr::Value(
                Value::String(_)
                    | Value::Boolean(_)
                    | Value::Null
                    | Value::HexString(_)
                    | Value::Interval(_)
            )
    );
    if safe {
        f.write_node(expr);
    } else {
        f.write_str("(");
        f.write_node(expr);
        f.write_str(")");
    }
}

/// Write `left` as the LHS of `<left> <op> ANY/ALL (...)`. The parser parses the
/// left at `Cmp`, and the `<op>` printed to its right is a binary infix that will
/// reach into any spine exposed at or below `Like` and reparse it as part of the
/// quantified expression's left rather than as a wrapper around it. Use
/// [`right_edge`] so a low-precedence spine hidden under right-transparent
/// prefixes (`- NOT a IN (b) = ANY (...)`, which exposes the `NOT`'s `IN`) is
/// caught; a normal tight infix `Op` prints bare.
fn write_quantified_left<W: fmt::Write, T: AstInfo>(f: &mut AstFormatter<W>, expr: &Expr<T>) {
    let needs_parens = right_edge(expr) <= prec::LIKE;
    if needs_parens {
        f.write_str("(");
        f.write_node(expr);
        f.write_str(")");
    } else {
        f.write_node(expr);
    }
}

/// Write `bound` as a `BETWEEN … AND …` bound. The parser parses both bounds with
/// `parse_subexpr(Precedence::Like)` (see `Parser::parse_between`), starting fresh
/// with nothing to the bound's left, so it walks the bound's *left spine* and
/// stops at the first operator binding at or below `Like` — leaving that operator
/// outside the bound (`x BETWEEN 1 IS NULL AND y` parses `1` as the bound, then
/// expects `AND` but finds `IS`). A bound is therefore safe bare only when its
/// left edge binds strictly above `Like`; use [`left_edge`] (not [`right_edge`],
/// which closes at `ATOM` for the right-closing `IS NULL`/`= ANY (…)`/`IN (…)`
/// forms whose looseness is on the left). The parser wraps these bounds in
/// `Expr::Nested` (which is `ATOM`, so it prints bare); this re-adds the parens
/// for ASTs where that wrapper is absent.
fn write_between_bound<W: fmt::Write, T: AstInfo>(f: &mut AstFormatter<W>, bound: &Expr<T>) {
    let needs_parens = left_edge(bound) <= prec::LIKE;
    if needs_parens {
        f.write_str("(");
        f.write_node(bound);
        f.write_str(")");
    } else {
        f.write_node(bound);
    }
}

/// Output-precedence ranks, ordered like `Parser::get_next_precedence` /
/// `Parser::Precedence` (higher binds tighter). They classify the *top* operator
/// an expr prints with, so the binary-operator printer can parenthesize an
/// operand that would otherwise re-associate on reparse. Keep in sync with the
/// parser. `ATOM` covers the self-delimiting primaries — they never need parens.
mod prec {
    pub const OR: u8 = 1;
    pub const AND: u8 = 2;
    pub const NOT: u8 = 3;
    pub const IS: u8 = 4;
    pub const CMP: u8 = 5;
    pub const LIKE: u8 = 6;
    pub const OTHER: u8 = 7;
    pub const PLUS_MINUS: u8 = 8;
    pub const MULTIPLY_DIVIDE: u8 = 9;
    // The `COLLATE` and postfix (`::`/`[…]`) parser levels (10 and 12) live
    // between `MULTIPLY_DIVIDE` and `ATOM`, but neither edge function ever
    // *returns* them: those forms are self-delimiting (their own operand is
    // parenthesized when it isn't), so both their edges rank `ATOM`. The holes
    // are kept so `PREFIX` keeps the parser's relative position.
    #[allow(dead_code)]
    pub const COLLATE: u8 = 10;
    pub const PREFIX: u8 = 11;
    #[allow(dead_code)]
    pub const POSTFIX: u8 = 12;
    pub const ATOM: u8 = 13;
}

/// The precedence of a binary operator, mirroring `Parser::get_next_precedence`.
/// A namespaced `OPERATOR(...)` binds at `OTHER`, like the parser.
fn binary_op_precedence(op: &Op) -> u8 {
    if op.namespace.is_some() {
        return prec::OTHER;
    }
    match op.op.as_str() {
        "=" | "<" | "<=" | "<>" | "!=" | ">" | ">=" => prec::CMP,
        "+" | "-" => prec::PLUS_MINUS,
        "*" | "/" | "%" => prec::MULTIPLY_DIVIDE,
        _ => prec::OTHER,
    }
}

/// The precedence at which a prefix operator (`Op` with no second operand)
/// parses its operand, mirroring `Parser::parse_prefix`: `-`/`+` at
/// `PrefixPlusMinus`, but `~` (and namespaced prefixes) at `Other`, so `~ a + b`
/// parses as `~ (a + b)` — `~` binds looser than `+`/`-`/`*`.
fn unary_prec(op: &Op) -> u8 {
    if op.namespace.is_none() && (op.op == "-" || op.op == "+") {
        prec::PREFIX
    } else {
        prec::OTHER
    }
}

/// The loosest precedence exposed on `expr`'s *right spine* — the precedence at
/// which an operator printed immediately to its right would bind *into* it
/// rather than wrap it. For a left operand / subject of a construct that prints
/// to its right, this is what decides parenthesization (its mirror, [`left_edge`],
/// decides right operands), because a prefix operator and the right operand of a
/// binary/`BETWEEN`/`LIKE`/`IS DISTINCT FROM` are right-transparent:
/// `- NOT a IN (b)` exposes the `NOT`'s `IN` on the right even though its top node
/// is unary `-`. Forms that close with a bracket on the right (`(…)`, `[…]`,
/// `::type`, `IS NULL`) are `ATOM`.
fn right_edge<T: AstInfo>(expr: &Expr<T>) -> u8 {
    match expr {
        // Right-transparent binary infixes: an operator tighter than this one
        // binds into the right operand, which itself may expose a looser spine.
        Expr::Or { right, .. } => prec::OR.min(right_edge(right)),
        Expr::And { right, .. } => prec::AND.min(right_edge(right)),
        Expr::Op {
            op, expr2: Some(r), ..
        } => binary_op_precedence(op).min(right_edge(r)),
        // Prefix operators expose their operand's right spine.
        Expr::Op {
            op,
            expr1,
            expr2: None,
        } => unary_prec(op).min(right_edge(expr1)),
        Expr::Not { expr } => prec::NOT.min(right_edge(expr)),
        // `IS DISTINCT FROM x` exposes `x`; `IS NULL`/`TRUE`/… close.
        Expr::IsExpr {
            construct: IsExprConstruct::DistinctFrom(x),
            ..
        } => prec::IS.min(right_edge(x)),
        // `… BETWEEN low AND high` exposes `high`; `… [I]LIKE pat [ESCAPE esc]`
        // exposes the rightmost of `esc`/`pat`.
        Expr::Between { high, .. } => prec::LIKE.min(right_edge(high)),
        Expr::Like {
            pattern, escape, ..
        } => {
            let rightmost = escape.as_deref().unwrap_or_else(|| pattern.as_ref());
            prec::LIKE.min(right_edge(rightmost))
        }
        // Everything else closes on the right (a bracket, a keyword, a literal,
        // or `IS NULL`-style), so nothing binds into it.
        _ => prec::ATOM,
    }
}

/// The loosest precedence exposed on `expr`'s *left spine* — the mirror of
/// [`right_edge`]. For a *right* operand (an operator on its left), this is what
/// decides parenthesization: a left-associative operator printed to its left
/// reaches into the left spine and re-associates if that spine exposes a
/// precedence at or below the operator's. The top operator alone is not enough,
/// because a left-nested chain can bury a looser operator down its left edge:
/// `387 = ANY (...) LIKE a IN (...)` has a top `IN` (`Like`) but exposes the
/// `= ANY` (`Cmp`) on its left, so a tighter `<>` to its left
/// (`48 <> 387 = ANY (...) ...`) would steal the `<>` into the `= ANY`'s left
/// rather than leave it as the `<>`'s right operand. Forms that open with their
/// own token on the left (a prefix operator, a keyword, `(…)`, a literal) are
/// `ATOM`.
fn left_edge<T: AstInfo>(expr: &Expr<T>) -> u8 {
    match expr {
        // Left-transparent infixes / postfix-keyword constructs: the subject (or
        // left operand) sits on the left spine, so descend into it.
        Expr::Or { left, .. } => prec::OR.min(left_edge(left)),
        Expr::And { left, .. } => prec::AND.min(left_edge(left)),
        Expr::Op {
            op,
            expr1,
            expr2: Some(_),
        } => binary_op_precedence(op).min(left_edge(expr1)),
        Expr::IsExpr { expr, .. } => prec::IS.min(left_edge(expr)),
        Expr::AnyExpr { left, .. }
        | Expr::AllExpr { left, .. }
        | Expr::AnySubquery { left, .. }
        | Expr::AllSubquery { left, .. } => prec::CMP.min(left_edge(left)),
        Expr::Like { expr, .. }
        | Expr::Between { expr, .. }
        | Expr::InList { expr, .. }
        | Expr::InSubquery { expr, .. } => prec::LIKE.min(left_edge(expr)),
        // Everything else leads with its own token on the left — a prefix
        // operator (`-`/`+`/`~`/`NOT`), a keyword, `(…)`, `ARRAY[…]`, a literal,
        // or a `COLLATE`/`::`/`[…]` whose own operand the printer parenthesizes
        // when it isn't self-delimiting — so nothing to the left binds into it.
        _ => prec::ATOM,
    }
}

/// Write `operand` for a binary operator, parenthesizing it iff `needs_parens`.
fn write_binary_operand<W: fmt::Write, T: AstInfo>(
    f: &mut AstFormatter<W>,
    operand: &Expr<T>,
    needs_parens: bool,
) {
    if needs_parens {
        f.write_str("(");
        f.write_node(operand);
        f.write_str(")");
    } else {
        f.write_node(operand);
    }
}

/// Whether `expr` prints in a *self-delimiting* form — atomic, or wrapped in its
/// own brackets/parens (`name(...)`, `(…)`, `ARRAY[…]`, `CASE … END`, …) — so it
/// is safe to print immediately to the left of a tight postfix operator (`::`,
/// `COLLATE`, or the `IN` delimiter of the `position(<needle> IN …)` special
/// form) without the operator re-associating into the expression's spine.
///
/// Anything with an exposed operator spine is *not* self-delimiting: a tight
/// postfix would bind to its rightmost sub-operand (`a + b COLLATE c` parses as
/// `a + (b COLLATE c)`), and the `position` `IN` delimiter would split on an
/// inner `IN`/comparison (`a IN (q) ->> b`). Callers must parenthesize / fall
/// back for those. Postfix forms (`::`/`COLLATE`/`[…]`) are self-delimiting only
/// when their own inner operand is.
fn prints_self_delimiting<T: AstInfo>(expr: &Expr<T>) -> bool {
    match expr {
        Expr::Value(_)
        | Expr::Identifier(_)
        | Expr::QualifiedWildcard(_)
        | Expr::Parameter(_)
        | Expr::Function(_)
        | Expr::HomogenizingFunction { .. }
        | Expr::NullIf { .. }
        | Expr::Subquery(_)
        | Expr::Exists(_)
        | Expr::Nested(_)
        | Expr::Array(_)
        | Expr::ArraySubquery(_)
        | Expr::List(_)
        | Expr::ListSubquery(_)
        | Expr::Map(_)
        | Expr::MapSubquery(_)
        | Expr::Case { .. }
        | Expr::Row { .. } => true,
        // The postfix `::` / `COLLATE` / `[…]` forms print as `<inner><suffix>`,
        // so they are safe only when their inner operand is.
        Expr::Cast { expr, .. } | Expr::Collate { expr, .. } | Expr::Subscript { expr, .. } => {
            prints_self_delimiting(expr)
        }
        _ => false,
    }
}

/// Whether the operand of a prefix operator (`-`/`+`/`~`) must be parenthesized
/// to round-trip. A prefix op binds *tighter* than `COLLATE`/`AT TIME ZONE` and
/// the binary/comparison operators, but *looser* than the postfix `::`/`[…]`
/// forms — and `- <number>` additionally lexes as a negative literal. So peel
/// the tight postfixes (`::`/`[…]`); if the chain bottoms out at a numeric
/// literal the sign would fold into it, and if it bottoms out at anything other
/// than a self-delimiting non-`COLLATE` primary (a `COLLATE`, a binary op, …) the
/// prefix op would re-associate — both need parens. (`a + b COLLATE c` reparses
/// as `a + (b COLLATE c)`; `- x COLLATE c` as `(- x) COLLATE c`.)
fn prefix_operand_needs_parens<T: AstInfo>(operand: &Expr<T>) -> bool {
    let mut e = operand;
    let mut saw_postfix = false;
    loop {
        match e {
            Expr::Cast { expr, .. } | Expr::Subscript { expr, .. } => {
                saw_postfix = true;
                e = expr.as_ref();
            }
            Expr::Value(Value::Number(_)) => return saw_postfix,
            // Another prefix operator (`+ + x`, `- ~ x`, `NOT NOT x`) stacks
            // directly: prefix operators don't re-associate, and the inner
            // operator symbol sits between the outer one and any digit so there
            // is no `- <number>` fold. Always safe — and crucially, NOT adding
            // parens here keeps deep unary chains from exploding the nesting
            // depth (and overflowing the stack) on reparse.
            Expr::Op { expr2: None, .. } | Expr::Not { .. } => return false,
            // Self-delimiting, but a top-level `COLLATE` binds looser than the
            // prefix op, so it (unlike `::`/`[…]`) is not safe here.
            _ => return !(prints_self_delimiting(e) && !matches!(e, Expr::Collate { .. })),
        }
    }
}

/// Write `expr` as the receiver of a `[…]` subscript. An unparenthesized
/// `Identifier(["map"])` reparses as `Token::Keyword(MAP)` followed by `[`,
/// which dispatches to `parse_map` (the map-literal grammar) instead of a
/// regular subscript. Parenthesize identifiers whose last component is a
/// context-sensitive keyword so the round trip stays an identifier subscript.
fn write_subscript_receiver<W: fmt::Write, T: AstInfo>(f: &mut AstFormatter<W>, expr: &Expr<T>) {
    let needs_parens = match expr {
        // A bare keyword identifier (`map`, `list`, …) dispatches to the
        // map/list-literal grammar before `[`, so it needs parens even though
        // identifiers are otherwise safe receivers.
        Expr::Identifier(idents) => idents
            .last()
            .and_then(|id| id.as_keyword())
            .map(|kw| kw.is_context_sensitive_keyword())
            .unwrap_or(false),
        // Self-delimiting primaries, the bracketed collections, and the postfix
        // forms that end in an identifier or `)` are safe: a following `[…]`
        // attaches to the whole receiver as a fresh subscript.
        Expr::QualifiedWildcard(_)
        | Expr::Parameter(_)
        | Expr::Value(_)
        | Expr::Function(_)
        | Expr::HomogenizingFunction { .. }
        | Expr::NullIf { .. }
        | Expr::Nested(_)
        | Expr::Subquery(_)
        | Expr::Exists(_)
        | Expr::Case { .. }
        | Expr::Row { .. }
        | Expr::Array(_)
        | Expr::ArraySubquery(_)
        | Expr::List(_)
        | Expr::ListSubquery(_)
        | Expr::Map(_)
        | Expr::MapSubquery(_)
        | Expr::FieldAccess { .. }
        | Expr::WildcardAccess(_)
        | Expr::Collate { .. } => false,
        // `Cast`: the type parser swallows a following `[…]` as an array suffix
        // (`a::int4[1]` is `a` cast to `int4[]`, not a subscript of `a::int4`).
        // `Subscript`: consecutive `[…]` flatten into one node (`a[1][2]` is a
        // single subscript), so a nested subscript receiver must be parenthesized
        // to stay nested. Everything else (operators, `IS`/`LIKE`/… constructs)
        // binds looser than `[` and would re-associate, so parenthesize by default.
        _ => true,
    };
    if needs_parens {
        f.write_str("(");
        f.write_node(expr);
        f.write_str(")");
    } else {
        f.write_node(expr);
    }
}

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
        self.fmt_call(f, true);
    }
}

impl<T: AstInfo> Function<T> {
    /// Render this call in table-function position (`FROM f(...)`, `ROWS FROM
    /// (f(...))`), where the `extract(a FROM b)` / `position(a IN b)` special
    /// forms are *not* valid syntax — only the scalar-expression parser
    /// dispatches to them. Forces the plain comma form (with the name quoted
    /// to dodge the special grammar) so the round trip stays stable.
    pub(crate) fn fmt_table_call<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        self.fmt_call(f, false);
    }

    fn fmt_call<W: fmt::Write>(&self, f: &mut AstFormatter<W>, allow_special_form: bool) {
        // This block handles printing function calls that have special parsing. In stable mode, the
        // name is quoted and so won't get the special parsing. We only need to print the special
        // formats in non-stable mode.
        //
        // The special forms (`position(a IN b)`, `extract(field FROM source)`)
        // have no syntax for `DISTINCT`, a within-group `ORDER BY`, a `FILTER`,
        // or an `OVER` window. A call literally named `"position"`/`"extract"`
        // that carries any of those modifiers (only reachable via the quoted
        // name — the real special grammar doesn't accept them) must therefore
        // fall through to the plain quoted-call form, or the special form
        // silently drops them on display.
        let has_call_modifiers = self.distinct
            || self.filter.is_some()
            || self.over.is_some()
            || matches!(&self.args, FunctionArgs::Args { order_by, .. } if !order_by.is_empty());
        if allow_special_form && !f.stable() && !has_call_modifiers {
            let special: Option<(&str, &[Option<Keyword>])> =
                match self.name.to_ast_string_stable().as_str() {
                    // `extract(field FROM source)` parses `field` into a string
                    // literal, so the special form only round-trips when arg0 is
                    // a string. A generic `"extract"(a, b)` with a non-string
                    // first arg must use the plain (quoted) call form.
                    r#""extract""#
                        if self.args.len() == Some(2)
                            && matches!(self.args.first(), Some(Expr::Value(Value::String(_)))) =>
                    {
                        Some(("extract", &[None, Some(FROM)]))
                    }
                    // `position(<needle> IN <haystack>)` parses the needle at
                    // `Precedence::Like`, so a low-precedence needle (`NOT`, a
                    // comparison, `IS`, a boolean connective, a quantified
                    // comparison, ...) printed bare before the `IN` would swallow
                    // or stop short of the delimiter. Only use the special form
                    // with a needle that's safe to sit left of `IN`.
                    r#""position""#
                        if self.args.len() == Some(2)
                            && self.args.first().is_some_and(prints_self_delimiting) =>
                    {
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

        // If the function name clashes with a keyword that has its own special
        // parser form, an unquoted name on reparse would trigger the
        // special-grammar parser instead of a regular function call. Emit the
        // always-quoted stable form so the regular function-call path is
        // preserved. The list tracks the `(Token::Keyword(KW), Some(Token::LParen))`
        // dispatch in `parse_prefix` (array, coalesce, ...); add a new entry
        // whenever a keyword grows special-grammar parens. (The `ANY`/`ALL`/`SOME`
        // quantifier keywords are handled more generally by `can_be_printed_bare`,
        // since they're also unsafe as bare identifiers, e.g. `0 # some`.)
        let name_stable = self.name.to_ast_string_stable();
        let needs_quote_to_disambiguate = matches!(
            name_stable.as_str(),
            r#""array""#
                | r#""coalesce""#
                | r#""exists""#
                | r#""extract""#
                | r#""greatest""#
                | r#""least""#
                | r#""list""#
                | r#""map""#
                | r#""normalize""#
                | r#""nullif""#
                | r#""position""#
                | r#""row""#
                | r#""substring""#
                | r#""trim""#
        );
        if needs_quote_to_disambiguate {
            f.write_str(&name_stable);
        } else {
            f.write_node(&self.name);
        }
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

    /// The first positional argument, if any (the `*` form has none).
    pub fn first(&self) -> Option<&Expr<T>> {
        match self {
            FunctionArgs::Star => None,
            FunctionArgs::Args { args, .. } => args.first(),
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
        for (arg, kw) in args.iter().zip_eq(kws) {
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
