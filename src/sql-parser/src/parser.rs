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

//! SQL Parser

use std::error::Error;
use std::fmt;

use itertools::Itertools;
use tracing::warn;

use mz_ore::collections::CollectionExt;
use mz_ore::option::OptionExt;
use mz_ore::stack::{CheckedRecursion, RecursionGuard, RecursionLimitError};

use crate::ast::*;
use crate::keywords::*;
use crate::lexer::{self, Token};

// NOTE(benesch): this recursion limit was chosen based on the maximum amount of
// nesting I've ever seen in a production SQL query (i.e., about a dozen) times
// a healthy factor to be conservative.
const RECURSION_LIMIT: usize = 128;

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($parser:expr, $pos:expr, $MSG:expr) => {
        Err($parser.error($pos, $MSG.to_string()))
    };
    ($parser:expr, $pos:expr, $($arg:tt)*) => {
        Err($parser.error($pos, format!($($arg)*)))
    };
}

/// Parses a SQL string containing zero or more SQL statements.
#[tracing::instrument(target = "compiler", level = "trace", name = "sql_to_ast")]
pub fn parse_statements(sql: &str) -> Result<Vec<Statement<Raw>>, ParserError> {
    let tokens = lexer::lex(sql)?;
    Parser::new(sql, tokens).parse_statements()
}

/// Parses a SQL string containing one SQL expression.
pub fn parse_expr(sql: &str) -> Result<Expr<Raw>, ParserError> {
    let tokens = lexer::lex(sql)?;
    let mut parser = Parser::new(sql, tokens);
    let expr = parser.parse_expr()?;
    if parser.next_token().is_some() {
        parser_err!(
            parser,
            parser.peek_prev_pos(),
            "extra token after expression"
        )
    } else {
        Ok(expr)
    }
}

/// Parses a SQL string containing a single data type.
pub fn parse_data_type(sql: &str) -> Result<RawDataType, ParserError> {
    let tokens = lexer::lex(sql)?;
    let mut parser = Parser::new(sql, tokens);
    let data_type = parser.parse_data_type()?;
    if parser.next_token().is_some() {
        parser_err!(
            parser,
            parser.peek_prev_pos(),
            "extra token after data type"
        )
    } else {
        Ok(data_type)
    }
}

macro_rules! maybe {
    ($e:expr) => {{
        if let Some(v) = $e {
            return Ok(v);
        }
    }};
}

#[derive(PartialEq)]
enum IsOptional {
    Optional,
    Mandatory,
}
use IsOptional::*;

enum IsLateral {
    Lateral,
    NotLateral,
}
use IsLateral::*;

#[derive(Debug, Clone, PartialEq)]
pub struct ParserError {
    /// The error message.
    pub message: String,
    /// The byte position with which the error is associated.
    pub pos: usize,
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for ParserError {}

impl From<RecursionLimitError> for ParserError {
    fn from(_: RecursionLimitError) -> ParserError {
        ParserError {
            pos: 0,
            message: format!(
                "statement exceeds nested expression limit of {}",
                RECURSION_LIMIT
            ),
        }
    }
}

impl ParserError {
    /// Constructs an error with the provided message at the provided position.
    pub(crate) fn new<S>(pos: usize, message: S) -> ParserError
    where
        S: Into<String>,
    {
        ParserError {
            pos,
            message: message.into(),
        }
    }
}

/// SQL Parser
struct Parser<'a> {
    sql: &'a str,
    tokens: Vec<(Token, usize)>,
    /// The index of the first unprocessed token in `self.tokens`
    index: usize,
    recursion_guard: RecursionGuard,
}

/// Defines a number of precedence classes operators follow. Since this enum derives Ord, the
/// precedence classes are ordered from weakest binding at the top to tightest binding at the
/// bottom.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
enum Precedence {
    Zero,
    Or,
    And,
    PrefixNot,
    Is,
    Cmp,
    Like,
    Other,
    PlusMinus,
    MultiplyDivide,
    PostfixCollateAt,
    PrefixPlusMinus,
    PostfixSubscriptCast,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
enum SetPrecedence {
    Zero,
    UnionExcept,
    Intersect,
}

impl<'a> Parser<'a> {
    /// Parse the specified tokens
    fn new(sql: &'a str, tokens: Vec<(Token, usize)>) -> Self {
        Parser {
            sql,
            tokens,
            index: 0,
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }

    fn error(&self, pos: usize, message: String) -> ParserError {
        ParserError { pos, message }
    }

    fn parse_statements(&mut self) -> Result<Vec<Statement<Raw>>, ParserError> {
        let mut stmts = Vec::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while self.consume_token(&Token::Semicolon) {
                expecting_statement_delimiter = false;
            }

            if self.peek_token().is_none() {
                break;
            } else if expecting_statement_delimiter {
                return self.expected(self.peek_pos(), "end of statement", self.peek_token());
            }

            let statement = self.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Parse a single top-level statement (such as SELECT, INSERT, CREATE, etc.),
    /// stopping before the statement separator, if any.
    fn parse_statement(&mut self) -> Result<Statement<Raw>, ParserError> {
        match self.next_token() {
            Some(t) => match t {
                Token::Keyword(SELECT) | Token::Keyword(WITH) | Token::Keyword(VALUES) => {
                    self.prev_token();
                    Ok(Statement::Select(SelectStatement {
                        query: self.parse_query()?,
                        as_of: self.parse_optional_as_of()?,
                    }))
                }
                Token::Keyword(CREATE) => Ok(self.parse_create()?),
                Token::Keyword(DISCARD) => Ok(self.parse_discard()?),
                Token::Keyword(DROP) => Ok(self.parse_drop()?),
                Token::Keyword(DELETE) => Ok(self.parse_delete()?),
                Token::Keyword(INSERT) => Ok(self.parse_insert()?),
                Token::Keyword(UPDATE) => Ok(self.parse_update()?),
                Token::Keyword(ALTER) => Ok(self.parse_alter()?),
                Token::Keyword(COPY) => Ok(self.parse_copy()?),
                Token::Keyword(SET) => Ok(self.parse_set()?),
                Token::Keyword(RESET) => Ok(self.parse_reset()?),
                Token::Keyword(SHOW) => Ok(self.parse_show()?),
                Token::Keyword(START) => Ok(self.parse_start_transaction()?),
                // `BEGIN` is a nonstandard but common alias for the
                // standard `START TRANSACTION` statement. It is supported
                // by at least PostgreSQL and MySQL.
                Token::Keyword(BEGIN) => Ok(self.parse_begin()?),
                Token::Keyword(COMMIT) => Ok(self.parse_commit()?),
                Token::Keyword(ROLLBACK) => Ok(self.parse_rollback()?),
                Token::Keyword(TAIL) => Ok(self.parse_tail()?),
                Token::Keyword(EXPLAIN) => Ok(self.parse_explain()?),
                Token::Keyword(DECLARE) => Ok(self.parse_declare()?),
                Token::Keyword(FETCH) => Ok(self.parse_fetch()?),
                Token::Keyword(CLOSE) => Ok(self.parse_close()?),
                Token::Keyword(PREPARE) => Ok(self.parse_prepare()?),
                Token::Keyword(EXECUTE) => Ok(self.parse_execute()?),
                Token::Keyword(DEALLOCATE) => Ok(self.parse_deallocate()?),
                Token::Keyword(RAISE) => Ok(self.parse_raise()?),
                Token::Keyword(kw) => parser_err!(
                    self,
                    self.peek_prev_pos(),
                    format!("Unexpected keyword {} at the beginning of a statement", kw)
                ),
                Token::LParen => {
                    self.prev_token();
                    Ok(Statement::Select(SelectStatement {
                        query: self.parse_query()?,
                        as_of: None, // Only the outermost SELECT may have an AS OF clause.
                    }))
                }
                unexpected => self.expected(
                    self.peek_prev_pos(),
                    "a keyword at the beginning of a statement",
                    Some(unexpected),
                ),
            },
            None => self.expected(self.peek_prev_pos(), "SQL statement", None),
        }
    }

    /// Parse a new expression
    fn parse_expr(&mut self) -> Result<Expr<Raw>, ParserError> {
        self.parse_subexpr(Precedence::Zero)
    }

    /// Parse tokens until the precedence changes
    fn parse_subexpr(&mut self, precedence: Precedence) -> Result<Expr<Raw>, ParserError> {
        let expr = self.checked_recur_mut(|parser| parser.parse_prefix())?;
        self.parse_subexpr_seeded(precedence, expr)
    }

    fn parse_subexpr_seeded(
        &mut self,
        precedence: Precedence,
        mut expr: Expr<Raw>,
    ) -> Result<Expr<Raw>, ParserError> {
        self.checked_recur_mut(|parser| {
            loop {
                let next_precedence = parser.get_next_precedence();
                if precedence >= next_precedence {
                    break;
                }

                expr = parser.parse_infix(expr, next_precedence)?;
            }
            Ok(expr)
        })
    }

    /// Parse an expression prefix
    fn parse_prefix(&mut self) -> Result<Expr<Raw>, ParserError> {
        // PostgreSQL allows any string literal to be preceded by a type name,
        // indicating that the string literal represents a literal of that type.
        // Some examples:
        //
        //     DATE '2020-05-20'
        //     TIMESTAMP WITH TIME ZONE '2020-05-20 7:43:54'
        //     BOOL 'true'
        //
        // The first two are standard SQL, while the latter is a PostgreSQL
        // extension. Complicating matters is the fact that INTERVAL string
        // literals may optionally be followed by some special keywords, e.g.:
        //
        //     INTERVAL '7' DAY
        //
        // Note also that naively `SELECT date` looks like a syntax error
        // because the `date` type name is not followed by a string literal, but
        // in fact is a valid expression that should parse as the column name
        // "date".
        maybe!(self.maybe_parse(|parser| {
            let data_type = parser.parse_data_type()?;
            if data_type.to_string().as_str() == "interval" {
                parser.parse_literal_interval()
            } else {
                Ok(Expr::Cast {
                    expr: Box::new(Expr::Value(Value::String(parser.parse_literal_string()?))),
                    data_type,
                })
            }
        }));

        let tok = self
            .next_token()
            .ok_or_else(|| self.error(self.peek_prev_pos(), "Unexpected EOF".to_string()))?;
        let expr = match tok {
            Token::Keyword(TRUE) | Token::Keyword(FALSE) | Token::Keyword(NULL) => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            Token::Keyword(ARRAY) => self.parse_array(),
            Token::Keyword(LIST) => self.parse_list(),
            Token::Keyword(CASE) => self.parse_case_expr(),
            Token::Keyword(CAST) => self.parse_cast_expr(),
            Token::Keyword(COALESCE) => {
                self.parse_homogenizing_function(HomogenizingFunction::Coalesce)
            }
            Token::Keyword(GREATEST) => {
                self.parse_homogenizing_function(HomogenizingFunction::Greatest)
            }
            Token::Keyword(LEAST) => self.parse_homogenizing_function(HomogenizingFunction::Least),
            Token::Keyword(NULLIF) => self.parse_nullif_expr(),
            Token::Keyword(EXISTS) => self.parse_exists_expr(),
            Token::Keyword(EXTRACT) => self.parse_extract_expr(),
            Token::Keyword(INTERVAL) => self.parse_literal_interval(),
            Token::Keyword(NOT) => Ok(Expr::Not {
                expr: Box::new(self.parse_subexpr(Precedence::PrefixNot)?),
            }),
            Token::Keyword(ROW) => self.parse_row_expr(),
            Token::Keyword(TRIM) => self.parse_trim_expr(),
            Token::Keyword(POSITION) if self.peek_token() == Some(Token::LParen) => {
                self.parse_position_expr()
            }
            Token::Keyword(SUBSTRING) => self.parse_substring_expr(),
            Token::Keyword(kw) if kw.is_reserved() => {
                return Err(self.error(
                    self.peek_prev_pos(),
                    "expected expression, but found reserved keyword".into(),
                ));
            }
            Token::Keyword(id) => self.parse_qualified_identifier(id.into_ident()),
            Token::Ident(id) => self.parse_qualified_identifier(Ident::new(id)),
            Token::Op(op) if op == "-" => {
                if let Some(Token::Number(n)) = self.peek_token() {
                    let n = match n.parse::<f64>() {
                        Ok(n) => n,
                        Err(_) => {
                            return Err(
                                self.error(self.peek_prev_pos(), format!("invalid number {}", n))
                            )
                        }
                    };
                    if n != 0.0 {
                        self.prev_token();
                        return Ok(Expr::Value(self.parse_value()?));
                    }
                }

                Ok(Expr::Op {
                    op: Op::bare(op),
                    expr1: Box::new(self.parse_subexpr(Precedence::PrefixPlusMinus)?),
                    expr2: None,
                })
            }
            Token::Op(op) if op == "+" => Ok(Expr::Op {
                op: Op::bare(op),
                expr1: Box::new(self.parse_subexpr(Precedence::PrefixPlusMinus)?),
                expr2: None,
            }),
            Token::Op(op) if op == "~" => Ok(Expr::Op {
                op: Op::bare(op),
                expr1: Box::new(self.parse_subexpr(Precedence::Other)?),
                expr2: None,
            }),
            Token::Number(_) | Token::String(_) | Token::HexString(_) => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            Token::Parameter(n) => Ok(Expr::Parameter(n)),
            Token::LParen => {
                let expr = self.parse_parenthesized_expr()?;
                self.expect_token(&Token::RParen)?;
                Ok(expr)
            }
            unexpected => self.expected(self.peek_prev_pos(), "an expression", Some(unexpected)),
        }?;

        Ok(expr)
    }

    /// Parses an expression that appears in parentheses, like `(1 + 1)` or
    /// `(SELECT 1)`. Assumes that the opening parenthesis has already been
    /// parsed. Parses up to the closing parenthesis without consuming it.
    fn parse_parenthesized_expr(&mut self) -> Result<Expr<Raw>, ParserError> {
        // The SQL grammar has an irritating ambiguity that presents here.
        // Consider these two expression fragments:
        //
        //     SELECT (((SELECT 2)) + 3)
        //     SELECT (((SELECT 2)) UNION SELECT 2)
        //             ^           ^
        //            (1)         (2)
        // When we see the parenthesis marked (1), we have no way to know ahead
        // of time whether that parenthesis is part of a `SetExpr::Query` inside
        // of an `Expr::Subquery` or whether it introduces an `Expr::Nested`.
        // The approach taken here avoids backtracking by deferring the decision
        // of whether to parse as a subquery or a nested expression until we get
        // to the point marked (2) above. Once there, we know that the presence
        // of a set operator implies that the parentheses belonged to a the
        // subquery; otherwise, they belonged to the expression.
        //
        // See also PostgreSQL's comments on the matter:
        // https://github.com/postgres/postgres/blob/42c63ab/src/backend/parser/gram.y#L11125-L11136

        enum Either {
            Query(Query<Raw>),
            Expr(Expr<Raw>),
        }

        impl Either {
            fn into_expr(self) -> Expr<Raw> {
                match self {
                    Either::Query(query) => Expr::Subquery(Box::new(query)),
                    Either::Expr(expr) => expr,
                }
            }

            fn nest(self) -> Either {
                // Need to be careful to maintain expression nesting to preserve
                // operator precedence. But there are no precedence concerns for
                // queries, so we flatten to match parse_query's behavior.
                match self {
                    Either::Expr(expr) => Either::Expr(Expr::Nested(Box::new(expr))),
                    Either::Query(_) => self,
                }
            }
        }

        // Recursive helper for parsing parenthesized expressions. Each call
        // handles one layer of parentheses. Before every call, the parser must
        // be positioned after an opening parenthesis; upon non-error return,
        // the parser will be positioned before the corresponding close
        // parenthesis. Somewhat weirdly, the returned expression semantically
        // includes the opening/closing parentheses, even though this function
        // is not responsible for parsing them.
        fn parse(parser: &mut Parser) -> Result<Either, ParserError> {
            if parser.peek_keyword(SELECT) || parser.peek_keyword(WITH) {
                // Easy case one: unambiguously a subquery.
                Ok(Either::Query(parser.parse_query()?))
            } else if !parser.consume_token(&Token::LParen) {
                // Easy case two: unambiguously an expression.
                let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
                if exprs.len() == 1 {
                    Ok(Either::Expr(Expr::Nested(Box::new(exprs.into_element()))))
                } else {
                    Ok(Either::Expr(Expr::Row { exprs }))
                }
            } else {
                // Hard case: we have an open parenthesis, and we need to decide
                // whether it belongs to the query or the expression.

                // Parse to the closing parenthesis.
                let either = parser.checked_recur_mut(parse)?;
                parser.expect_token(&Token::RParen)?;

                // Decide if we need to associate any tokens after the closing
                // parenthesis with what we've parsed so far.
                match (either, parser.peek_token()) {
                    // The next token is another closing parenthesis. Can't
                    // resolve the ambiguity yet. Return to let our caller
                    // handle it.
                    (either, Some(Token::RParen)) => Ok(either.nest()),

                    // The next token is a comma, which means `either` was the
                    // first expression in an implicit row constructor.
                    (either, Some(Token::Comma)) => {
                        let mut exprs = vec![either.into_expr()];
                        while parser.consume_token(&Token::Comma) {
                            exprs.push(parser.parse_expr()?);
                        }
                        Ok(Either::Expr(Expr::Row { exprs }))
                    }

                    // We have a subquery and the next token is a set operator.
                    // That implies we have a partially-parsed subquery (or a
                    // syntax error). Hop into parsing a set expression where
                    // our subquery is the LHS of the set operator.
                    (Either::Query(query), Some(Token::Keyword(kw)))
                        if matches!(kw, UNION | INTERSECT | EXCEPT) =>
                    {
                        let query = SetExpr::Query(Box::new(query));
                        let ctes = vec![];
                        let body = parser.parse_query_body_seeded(SetPrecedence::Zero, query)?;
                        Ok(Either::Query(parser.parse_query_tail(ctes, body)?))
                    }

                    // The next token is something else. That implies we have a
                    // partially-parsed expression (or a syntax error). Hop into
                    // parsing an expression where `either` is the expression
                    // prefix.
                    (either, _) => {
                        let prefix = either.into_expr();
                        let expr = parser.parse_subexpr_seeded(Precedence::Zero, prefix)?;
                        Ok(Either::Expr(expr).nest())
                    }
                }
            }
        }

        Ok(parse(self)?.into_expr())
    }

    fn parse_function(&mut self, name: UnresolvedObjectName) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let distinct = matches!(
            self.parse_at_most_one_keyword(&[ALL, DISTINCT], &format!("function: {}", name))?,
            Some(DISTINCT),
        );
        let args = self.parse_optional_args(true)?;

        if distinct && matches!(args, FunctionArgs::Star) {
            return Err(self.error(
                self.peek_prev_pos() - 1,
                "DISTINCT * not supported as function args".to_string(),
            ));
        }

        let filter = if self.parse_keyword(FILTER) {
            self.expect_token(&Token::LParen)?;
            self.expect_keyword(WHERE)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Some(Box::new(expr))
        } else {
            None
        };
        let over = if self.parse_keyword(OVER) {
            // TBD: support window names (`OVER mywin`) in place of inline specification
            self.expect_token(&Token::LParen)?;
            let partition_by = if self.parse_keywords(&[PARTITION, BY]) {
                // a list of possibly-qualified column names
                self.parse_comma_separated(Parser::parse_expr)?
            } else {
                vec![]
            };
            let order_by = if self.parse_keywords(&[ORDER, BY]) {
                self.parse_comma_separated(Parser::parse_order_by_expr)?
            } else {
                vec![]
            };
            let window_frame = if !self.consume_token(&Token::RParen) {
                let window_frame = self.parse_window_frame()?;
                self.expect_token(&Token::RParen)?;
                Some(window_frame)
            } else {
                None
            };

            Some(WindowSpec {
                partition_by,
                order_by,
                window_frame,
            })
        } else {
            None
        };

        Ok(Expr::Function(Function {
            name,
            args,
            filter,
            over,
            distinct,
        }))
    }

    fn parse_window_frame(&mut self) -> Result<WindowFrame, ParserError> {
        let units = match self.expect_one_of_keywords(&[ROWS, RANGE, GROUPS])? {
            ROWS => WindowFrameUnits::Rows,
            RANGE => WindowFrameUnits::Range,
            GROUPS => WindowFrameUnits::Groups,
            _ => unreachable!(),
        };
        let (start_bound, end_bound) = if self.parse_keyword(BETWEEN) {
            let start_bound = self.parse_window_frame_bound()?;
            self.expect_keyword(AND)?;
            let end_bound = Some(self.parse_window_frame_bound()?);
            (start_bound, end_bound)
        } else {
            (self.parse_window_frame_bound()?, None)
        };
        Ok(WindowFrame {
            units,
            start_bound,
            end_bound,
        })
    }

    /// Parse `CURRENT ROW` or `{ <positive number> | UNBOUNDED } { PRECEDING | FOLLOWING }`
    fn parse_window_frame_bound(&mut self) -> Result<WindowFrameBound, ParserError> {
        if self.parse_keywords(&[CURRENT, ROW]) {
            Ok(WindowFrameBound::CurrentRow)
        } else {
            let rows = if self.parse_keyword(UNBOUNDED) {
                None
            } else {
                Some(self.parse_literal_uint()?)
            };
            if self.parse_keyword(PRECEDING) {
                Ok(WindowFrameBound::Preceding(rows))
            } else if self.parse_keyword(FOLLOWING) {
                Ok(WindowFrameBound::Following(rows))
            } else {
                self.expected(self.peek_pos(), "PRECEDING or FOLLOWING", self.peek_token())
            }
        }
    }

    fn parse_case_expr(&mut self) -> Result<Expr<Raw>, ParserError> {
        let mut operand = None;
        if !self.parse_keyword(WHEN) {
            operand = Some(Box::new(self.parse_expr()?));
            self.expect_keyword(WHEN)?;
        }
        let mut conditions = vec![];
        let mut results = vec![];
        loop {
            conditions.push(self.parse_expr()?);
            self.expect_keyword(THEN)?;
            results.push(self.parse_expr()?);
            if !self.parse_keyword(WHEN) {
                break;
            }
        }
        let else_result = if self.parse_keyword(ELSE) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.expect_keyword(END)?;
        Ok(Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        })
    }

    /// Parse a SQL CAST function e.g. `CAST(expr AS FLOAT)`
    fn parse_cast_expr(&mut self) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword(AS)?;
        let data_type = self.parse_data_type()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Cast {
            expr: Box::new(expr),
            data_type,
        })
    }

    /// Parse a SQL EXISTS expression e.g. `WHERE EXISTS(SELECT ...)`.
    fn parse_exists_expr(&mut self) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let exists_node = Expr::Exists(Box::new(self.parse_query()?));
        self.expect_token(&Token::RParen)?;
        Ok(exists_node)
    }

    fn parse_homogenizing_function(
        &mut self,
        function: HomogenizingFunction,
    ) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let exprs = self.parse_comma_separated(Parser::parse_expr)?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::HomogenizingFunction { function, exprs })
    }

    fn parse_nullif_expr(&mut self) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let l_expr = Box::new(self.parse_expr()?);
        self.expect_token(&Token::Comma)?;
        let r_expr = Box::new(self.parse_expr()?);
        self.expect_token(&Token::RParen)?;
        Ok(Expr::NullIf { l_expr, r_expr })
    }

    fn parse_extract_expr(&mut self) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let field = match self.next_token() {
            Some(Token::Keyword(kw)) => kw.into_ident().into_string(),
            Some(Token::Ident(id)) => Ident::new(id).into_string(),
            Some(Token::String(s)) => s,
            t => self.expected(self.peek_prev_pos(), "extract field token", t)?,
        };
        self.expect_keyword(FROM)?;
        let expr = self.parse_expr()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Function(Function {
            name: UnresolvedObjectName::unqualified("extract"),
            args: FunctionArgs::args(vec![Expr::Value(Value::String(field)), expr]),
            filter: None,
            over: None,
            distinct: false,
        }))
    }

    fn parse_row_expr(&mut self) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        if self.consume_token(&Token::RParen) {
            Ok(Expr::Row { exprs: vec![] })
        } else {
            let exprs = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::Row { exprs })
        }
    }

    fn parse_composite_type_definition(&mut self) -> Result<Vec<ColumnDef<Raw>>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let fields = self.parse_comma_separated(|parser| {
            Ok(ColumnDef {
                name: parser.parse_identifier()?,
                data_type: parser.parse_data_type()?,
                collation: None,
                options: vec![],
            })
        })?;
        self.expect_token(&Token::RParen)?;
        Ok(fields)
    }

    // Parse calls to trim(), which can take the form:
    // - trim(side 'chars' from 'string')
    // - trim('chars' from 'string')
    // - trim(side from 'string')
    // - trim(from 'string')
    // - trim('string')
    // - trim(side 'string')
    fn parse_trim_expr(&mut self) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let name = match self.parse_one_of_keywords(&[BOTH, LEADING, TRAILING]) {
            None | Some(BOTH) => "btrim",
            Some(LEADING) => "ltrim",
            Some(TRAILING) => "rtrim",
            _ => unreachable!(),
        };
        let mut exprs = Vec::new();
        if self.parse_keyword(FROM) {
            // 'string'
            exprs.push(self.parse_expr()?);
        } else {
            // Either 'chars' or 'string'
            exprs.push(self.parse_expr()?);
            if self.parse_keyword(FROM) {
                // 'string'; previous must be 'chars'
                // Swap 'chars' and 'string' for compatibility with btrim, ltrim, and rtrim.
                exprs.insert(0, self.parse_expr()?);
            }
        }
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Function(Function {
            name: UnresolvedObjectName::unqualified(name),
            args: FunctionArgs::args(exprs),
            filter: None,
            over: None,
            distinct: false,
        }))
    }

    // Parse calls to position(), which has the special form position('string' in 'string').
    fn parse_position_expr(&mut self) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        // we must be greater-equal the precedence of IN, which is Like to avoid
        // parsing away the IN as part of the sub expression
        let needle = self.parse_subexpr(Precedence::Like)?;
        self.expect_token(&Token::Keyword(IN))?;
        let haystack = self.parse_expr()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Function(Function {
            name: UnresolvedObjectName::unqualified("position"),
            args: FunctionArgs::args(vec![needle, haystack]),
            filter: None,
            over: None,
            distinct: false,
        }))
    }

    /// Parse an INTERVAL literal.
    ///
    /// Some syntactically valid intervals:
    ///
    ///   1. `INTERVAL '1' DAY`
    ///   2. `INTERVAL '1-1' YEAR TO MONTH`
    ///   3. `INTERVAL '1' SECOND`
    ///   4. `INTERVAL '1:1' MINUTE TO SECOND
    ///   5. `INTERVAL '1:1:1.1' HOUR TO SECOND (5)`
    ///   6. `INTERVAL '1.111' SECOND (2)`
    ///
    fn parse_literal_interval(&mut self) -> Result<Expr<Raw>, ParserError> {
        // The first token in an interval is a string literal which specifies
        // the duration of the interval.
        let value = self.parse_literal_string()?;

        // Determine the range of TimeUnits, whether explicit (`INTERVAL ... DAY TO MINUTE`) or
        // implicit (in which all date fields are eligible).
        let (precision_high, precision_low, fsec_max_precision) =
            match self.expect_one_of_keywords(&[
                YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, YEARS, MONTHS, DAYS, HOURS, MINUTES,
                SECONDS,
            ]) {
                Ok(d) => {
                    let d_pos = self.peek_prev_pos();
                    if self.parse_keyword(TO) {
                        let e = self.expect_one_of_keywords(&[
                            YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, YEARS, MONTHS, DAYS, HOURS,
                            MINUTES, SECONDS,
                        ])?;

                        let high: DateTimeField = d
                            .as_str()
                            .parse()
                            .map_err(|e| self.error(self.peek_prev_pos(), e))?;
                        let low: DateTimeField = e
                            .as_str()
                            .parse()
                            .map_err(|e| self.error(self.peek_prev_pos(), e))?;

                        // Check for invalid ranges, i.e. precision_high is the same
                        // as or a less significant DateTimeField than
                        // precision_low.
                        if high >= low {
                            return parser_err!(
                                self,
                                d_pos,
                                "Invalid field range in INTERVAL '{}' {} TO {}; the value in the \
                                 position of {} should be more significant than {}.",
                                value,
                                d,
                                e,
                                d,
                                e,
                            );
                        }

                        let fsec_max_precision = if low == DateTimeField::Second {
                            self.parse_optional_precision()?
                        } else {
                            None
                        };

                        (high, low, fsec_max_precision)
                    } else {
                        let low: DateTimeField = d
                            .as_str()
                            .parse()
                            .map_err(|e| self.error(self.peek_prev_pos(), e))?;
                        let fsec_max_precision = if low == DateTimeField::Second {
                            self.parse_optional_precision()?
                        } else {
                            None
                        };

                        (DateTimeField::Year, low, fsec_max_precision)
                    }
                }
                Err(_) => (DateTimeField::Year, DateTimeField::Second, None),
            };

        Ok(Expr::Value(Value::Interval(IntervalValue {
            value,
            precision_high,
            precision_low,
            fsec_max_precision,
        })))
    }

    /// Parse an operator following an expression
    fn parse_infix(
        &mut self,
        expr: Expr<Raw>,
        precedence: Precedence,
    ) -> Result<Expr<Raw>, ParserError> {
        let tok = self.next_token().unwrap(); // safe as EOF's precedence is the lowest

        let regular_binary_operator = match &tok {
            Token::Op(s) => Some(Op::bare(s)),
            Token::Eq => Some(Op::bare("=")),
            Token::Star => Some(Op::bare("*")),
            Token::Keyword(OPERATOR) => {
                self.expect_token(&Token::LParen)?;
                let op = self.parse_operator()?;
                self.expect_token(&Token::RParen)?;
                Some(op)
            }
            _ => None,
        };

        if let Some(op) = regular_binary_operator {
            if let Some(kw) = self.parse_one_of_keywords(&[ANY, SOME, ALL]) {
                self.expect_token(&Token::LParen)?;

                let expr = if self.parse_one_of_keywords(&[SELECT, VALUES]).is_some() {
                    self.prev_token();
                    let subquery = self.parse_query()?;

                    if kw == ALL {
                        Expr::AllSubquery {
                            left: Box::new(expr),
                            op,
                            right: Box::new(subquery),
                        }
                    } else {
                        Expr::AnySubquery {
                            left: Box::new(expr),
                            op,
                            right: Box::new(subquery),
                        }
                    }
                } else {
                    let right = self.parse_expr()?;

                    if kw == ALL {
                        Expr::AllExpr {
                            left: Box::new(expr),
                            op,
                            right: Box::new(right),
                        }
                    } else {
                        Expr::AnyExpr {
                            left: Box::new(expr),
                            op,
                            right: Box::new(right),
                        }
                    }
                };
                self.expect_token(&Token::RParen)?;

                Ok(expr)
            } else {
                Ok(Expr::Op {
                    op,
                    expr1: Box::new(expr),
                    expr2: Some(Box::new(self.parse_subexpr(precedence)?)),
                })
            }
        } else if let Token::Keyword(kw) = tok {
            match kw {
                IS => {
                    let negated = self.parse_keyword(NOT);
                    if let Some(construct) =
                        self.parse_one_of_keywords(&[NULL, TRUE, FALSE, UNKNOWN])
                    {
                        Ok(Expr::IsExpr {
                            expr: Box::new(expr),
                            negated,
                            construct: match construct {
                                NULL => IsExprConstruct::Null,
                                TRUE => IsExprConstruct::True,
                                FALSE => IsExprConstruct::False,
                                UNKNOWN => IsExprConstruct::Unknown,
                                _ => unreachable!(),
                            },
                        })
                    } else {
                        self.expected(
                            self.peek_pos(),
                            "NULL, NOT NULL, TRUE, NOT TRUE, FALSE, NOT FALSE, UNKNOWN, NOT UNKNOWN after IS",
                            self.peek_token(),
                        )
                    }
                }
                ISNULL => Ok(Expr::IsExpr {
                    expr: Box::new(expr),
                    negated: false,
                    construct: IsExprConstruct::Null,
                }),
                NOT | IN | LIKE | ILIKE | BETWEEN => {
                    self.prev_token();
                    let negated = self.parse_keyword(NOT);
                    if self.parse_keyword(IN) {
                        self.parse_in(expr, negated)
                    } else if self.parse_keyword(BETWEEN) {
                        self.parse_between(expr, negated)
                    } else if self.parse_keyword(LIKE) {
                        self.parse_like(expr, false, negated)
                    } else if self.parse_keyword(ILIKE) {
                        self.parse_like(expr, true, negated)
                    } else {
                        self.expected(
                            self.peek_pos(),
                            "IN, BETWEEN, LIKE, or ILIKE after NOT",
                            self.peek_token(),
                        )
                    }
                }
                AND => Ok(Expr::And {
                    left: Box::new(expr),
                    right: Box::new(self.parse_subexpr(precedence)?),
                }),
                OR => Ok(Expr::Or {
                    left: Box::new(expr),
                    right: Box::new(self.parse_subexpr(precedence)?),
                }),
                AT => {
                    self.expect_keywords(&[TIME, ZONE])?;
                    Ok(Expr::Function(Function {
                        name: UnresolvedObjectName(vec!["timezone".into()]),
                        args: FunctionArgs::args(vec![self.parse_subexpr(precedence)?, expr]),
                        filter: None,
                        over: None,
                        distinct: false,
                    }))
                }
                COLLATE => Ok(Expr::Collate {
                    expr: Box::new(expr),
                    collation: self.parse_object_name()?,
                }),
                // Can only happen if `get_next_precedence` got out of sync with this function
                _ => panic!("No infix parser for token {:?}", tok),
            }
        } else if Token::DoubleColon == tok {
            self.parse_pg_cast(expr)
        } else if Token::LBracket == tok {
            self.prev_token();
            self.parse_subscript(expr)
        } else if Token::Dot == tok {
            match self.next_token() {
                Some(Token::Ident(id)) => Ok(Expr::FieldAccess {
                    expr: Box::new(expr),
                    field: Ident::new(id),
                }),
                // Per PostgreSQL, even reserved keywords are ok after a field
                // access operator.
                Some(Token::Keyword(kw)) => Ok(Expr::FieldAccess {
                    expr: Box::new(expr),
                    field: kw.into_ident(),
                }),
                Some(Token::Star) => Ok(Expr::WildcardAccess(Box::new(expr))),
                unexpected => self.expected(
                    self.peek_prev_pos(),
                    "an identifier or a '*' after '.'",
                    unexpected,
                ),
            }
        } else {
            // Can only happen if `get_next_precedence` got out of sync with this function
            panic!("No infix parser for token {:?}", tok)
        }
    }

    /// Parse subscript expression, i.e. either an index value or slice range.
    fn parse_subscript(&mut self, expr: Expr<Raw>) -> Result<Expr<Raw>, ParserError> {
        let mut positions = Vec::new();

        while self.consume_token(&Token::LBracket) {
            let start = if self.peek_token() == Some(Token::Colon) {
                None
            } else {
                Some(self.parse_expr()?)
            };

            let (end, explicit_slice) = if self.consume_token(&Token::Colon) {
                // Presence of a colon means these positions were explicit
                (
                    // Terminated expr
                    if self.peek_token() == Some(Token::RBracket) {
                        None
                    } else {
                        Some(self.parse_expr()?)
                    },
                    true,
                )
            } else {
                (None, false)
            };

            assert!(
                start.is_some() || explicit_slice,
                "user typed something between brackets"
            );

            assert!(
                explicit_slice || end.is_none(),
                "if end is some, must have an explicit slice"
            );

            positions.push(SubscriptPosition {
                start,
                end,
                explicit_slice,
            });
            self.expect_token(&Token::RBracket)?;
        }

        Ok(Expr::Subscript {
            expr: Box::new(expr),
            positions,
        })
    }

    // Parse calls to substring(), which can take the form:
    // - substring('string', 'int')
    // - substring('string', 'int', 'int')
    // - substring('string' FROM 'int')
    // - substring('string' FROM 'int' FOR 'int')
    // - substring('string' FOR 'int')
    fn parse_substring_expr(&mut self) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let mut exprs = vec![self.parse_expr()?];
        if self.parse_keyword(FROM) {
            // 'string' FROM 'int'
            exprs.push(self.parse_expr()?);
            if self.parse_keyword(FOR) {
                // 'string' FROM 'int' FOR 'int'
                exprs.push(self.parse_expr()?);
            }
        } else if self.parse_keyword(FOR) {
            // 'string' FOR 'int'
            exprs.push(Expr::Value(Value::Number(String::from("1"))));
            exprs.push(self.parse_expr()?);
        } else {
            // 'string', 'int'
            // or
            // 'string', 'int', 'int'
            self.expect_token(&Token::Comma)?;
            exprs.extend(self.parse_comma_separated(Parser::parse_expr)?);
        }

        self.expect_token(&Token::RParen)?;
        Ok(Expr::Function(Function {
            name: UnresolvedObjectName::unqualified("substring"),
            args: FunctionArgs::args(exprs),
            filter: None,
            over: None,
            distinct: false,
        }))
    }

    /// Parse an operator reference.
    ///
    /// Examples:
    ///   * `+`
    ///   * `OPERATOR(schema.+)`
    ///   * `OPERATOR("foo"."bar"."baz".@>)`
    fn parse_operator(&mut self) -> Result<Op, ParserError> {
        let mut namespace = vec![];
        loop {
            match self.next_token() {
                Some(Token::Keyword(kw)) => namespace.push(kw.into_ident()),
                Some(Token::Ident(id)) => namespace.push(Ident::new(id)),
                Some(Token::Op(op)) => return Ok(Op { namespace, op }),
                Some(Token::Star) => {
                    let op = String::from("*");
                    return Ok(Op { namespace, op });
                }
                tok => self.expected(self.peek_prev_pos(), "operator", tok)?,
            }
            self.expect_token(&Token::Dot)?;
        }
    }

    /// Parses the parens following the `[ NOT ] IN` operator
    fn parse_in(&mut self, expr: Expr<Raw>, negated: bool) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let in_op = if self
            .parse_one_of_keywords(&[SELECT, VALUES, WITH])
            .is_some()
        {
            self.prev_token();
            Expr::InSubquery {
                expr: Box::new(expr),
                subquery: Box::new(self.parse_query()?),
                negated,
            }
        } else {
            Expr::InList {
                expr: Box::new(expr),
                list: self.parse_comma_separated(Parser::parse_expr)?,
                negated,
            }
        };
        self.expect_token(&Token::RParen)?;
        Ok(in_op)
    }

    /// Parses `BETWEEN <low> AND <high>`, assuming the `BETWEEN` keyword was already consumed
    fn parse_between(&mut self, expr: Expr<Raw>, negated: bool) -> Result<Expr<Raw>, ParserError> {
        // Stop parsing subexpressions for <low> and <high> on tokens with
        // precedence lower than that of `BETWEEN`, such as `AND`, `IS`, etc.
        let low = self.parse_subexpr(Precedence::Like)?;
        self.expect_keyword(AND)?;
        let high = self.parse_subexpr(Precedence::Like)?;
        Ok(Expr::Between {
            expr: Box::new(expr),
            negated,
            low: Box::new(low),
            high: Box::new(high),
        })
    }

    /// Parses `LIKE <pattern> [ ESCAPE <char> ]`, assuming the `LIKE` keyword was already consumed
    fn parse_like(
        &mut self,
        expr: Expr<Raw>,
        case_insensitive: bool,
        negated: bool,
    ) -> Result<Expr<Raw>, ParserError> {
        let pattern = self.parse_subexpr(Precedence::Like)?;
        let escape = if self.parse_keyword(ESCAPE) {
            Some(Box::new(self.parse_subexpr(Precedence::Like)?))
        } else {
            None
        };
        Ok(Expr::Like {
            expr: Box::new(expr),
            pattern: Box::new(pattern),
            escape,
            case_insensitive,
            negated,
        })
    }

    /// Parse a postgresql casting style which is in the form of `expr::datatype`
    fn parse_pg_cast(&mut self, expr: Expr<Raw>) -> Result<Expr<Raw>, ParserError> {
        Ok(Expr::Cast {
            expr: Box::new(expr),
            data_type: self.parse_data_type()?,
        })
    }

    /// Get the precedence of the next token
    fn get_next_precedence(&self) -> Precedence {
        if let Some(token) = self.peek_token() {
            match &token {
                Token::Keyword(OR) => Precedence::Or,
                Token::Keyword(AND) => Precedence::And,
                Token::Keyword(NOT) => match &self.peek_nth_token(1) {
                    // The precedence of NOT varies depending on keyword that
                    // follows it. If it is followed by IN, BETWEEN, or LIKE,
                    // it takes on the precedence of those tokens. Otherwise it
                    // is not an infix operator, and therefore has zero
                    // precedence.
                    Some(Token::Keyword(IN)) => Precedence::Like,
                    Some(Token::Keyword(BETWEEN)) => Precedence::Like,
                    Some(Token::Keyword(ILIKE)) => Precedence::Like,
                    Some(Token::Keyword(LIKE)) => Precedence::Like,
                    _ => Precedence::Zero,
                },
                Token::Keyword(IS) | Token::Keyword(ISNULL) => Precedence::Is,
                Token::Keyword(IN) => Precedence::Like,
                Token::Keyword(BETWEEN) => Precedence::Like,
                Token::Keyword(ILIKE) => Precedence::Like,
                Token::Keyword(LIKE) => Precedence::Like,
                Token::Keyword(OPERATOR) => Precedence::Other,
                Token::Op(s) => match s.as_str() {
                    "<" | "<=" | "<>" | "!=" | ">" | ">=" => Precedence::Cmp,
                    "+" | "-" => Precedence::PlusMinus,
                    "/" | "%" => Precedence::MultiplyDivide,
                    _ => Precedence::Other,
                },
                Token::Eq => Precedence::Cmp,
                Token::Star => Precedence::MultiplyDivide,
                Token::Keyword(COLLATE) | Token::Keyword(AT) => Precedence::PostfixCollateAt,
                Token::DoubleColon | Token::LBracket | Token::Dot => {
                    Precedence::PostfixSubscriptCast
                }
                _ => Precedence::Zero,
            }
        } else {
            Precedence::Zero
        }
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file)
    fn peek_token(&self) -> Option<Token> {
        self.peek_nth_token(0)
    }

    fn peek_keyword(&mut self, kw: Keyword) -> bool {
        match self.peek_token() {
            Some(Token::Keyword(k)) => k == kw,
            _ => false,
        }
    }

    fn peek_keywords(&mut self, keywords: &[Keyword]) -> bool {
        for (i, keyword) in keywords.iter().enumerate() {
            match self.peek_nth_token(i) {
                Some(Token::Keyword(k)) => {
                    if k != *keyword {
                        return false;
                    }
                }
                _ => return false,
            }
        }
        true
    }

    /// Return the nth token that has not yet been processed.
    fn peek_nth_token(&self, n: usize) -> Option<Token> {
        self.tokens.get(self.index + n).map(|(t, _)| t.clone())
    }

    /// Return the next token that has not yet been processed, or None if
    /// reached end-of-file, and mark it as processed. OK to call repeatedly
    /// after reaching EOF.
    fn next_token(&mut self) -> Option<Token> {
        let token = self
            .tokens
            .get(self.index)
            .map(|(token, _range)| token.clone());
        self.index += 1;
        token
    }

    /// Push back the last one non-whitespace token. Must be called after
    /// `next_token()`, otherwise might panic. OK to call after
    /// `next_token()` indicates an EOF.
    fn prev_token(&mut self) {
        assert!(self.index > 0);
        self.index -= 1;
    }

    /// Return the byte position within the query string at which the
    /// next token starts.
    fn peek_pos(&self) -> usize {
        match self.tokens.get(self.index) {
            Some((_token, pos)) => *pos,
            None => self.sql.len(),
        }
    }

    /// Return the byte position within the query string at which the previous
    /// token starts.
    ///
    /// Must be called after `next_token()`, otherwise might panic.
    /// OK to call after `next_token()` indicates an EOF.
    fn peek_prev_pos(&self) -> usize {
        assert!(self.index > 0);
        match self.tokens.get(self.index - 1) {
            Some((_token, pos)) => *pos,
            None => self.sql.len(),
        }
    }

    /// Report unexpected token
    fn expected<D, T>(
        &self,
        pos: usize,
        expected: D,
        found: Option<Token>,
    ) -> Result<T, ParserError>
    where
        D: fmt::Display,
    {
        parser_err!(
            self,
            pos,
            "Expected {}, found {}",
            expected,
            found.display_or("EOF"),
        )
    }

    /// Look for an expected keyword and consume it if it exists
    #[must_use]
    fn parse_keyword(&mut self, kw: Keyword) -> bool {
        if self.peek_keyword(kw) {
            self.next_token();
            true
        } else {
            false
        }
    }

    /// Look for an expected sequence of keywords and consume them if they exist
    #[must_use]
    fn parse_keywords(&mut self, keywords: &[Keyword]) -> bool {
        if self.peek_keywords(keywords) {
            self.index += keywords.len();
            true
        } else {
            false
        }
    }

    fn parse_at_most_one_keyword(
        &mut self,
        keywords: &[Keyword],
        location: &str,
    ) -> Result<Option<Keyword>, ParserError> {
        match self.parse_one_of_keywords(keywords) {
            Some(first) => {
                let remaining_keywords = keywords
                    .iter()
                    .cloned()
                    .filter(|k| *k != first)
                    .collect::<Vec<_>>();
                if let Some(second) = self.parse_one_of_keywords(remaining_keywords.as_slice()) {
                    let second_pos = self.peek_prev_pos();
                    parser_err!(
                        self,
                        second_pos,
                        "Cannot specify both {} and {} in {}",
                        first,
                        second,
                        location,
                    )
                } else {
                    Ok(Some(first))
                }
            }
            None => Ok(None),
        }
    }

    /// Look for one of the given keywords and return the one that matches.
    #[must_use]
    fn parse_one_of_keywords(&mut self, kws: &[Keyword]) -> Option<Keyword> {
        match self.peek_token() {
            Some(Token::Keyword(k)) => kws.iter().find(|kw| **kw == k).map(|kw| {
                self.next_token();
                *kw
            }),
            _ => None,
        }
    }

    /// Bail out if the current token is not one of the expected keywords, or consume it if it is
    fn expect_one_of_keywords(&mut self, keywords: &[Keyword]) -> Result<Keyword, ParserError> {
        if let Some(keyword) = self.parse_one_of_keywords(keywords) {
            Ok(keyword)
        } else {
            self.expected(
                self.peek_pos(),
                &format!("one of {}", keywords.iter().join(" or ")),
                self.peek_token(),
            )
        }
    }

    /// Bail out if the current token is not an expected keyword, or consume it if it is
    fn expect_keyword(&mut self, expected: Keyword) -> Result<(), ParserError> {
        if self.parse_keyword(expected) {
            Ok(())
        } else {
            self.expected(self.peek_pos(), expected, self.peek_token())
        }
    }

    /// Bail out if the following tokens are not the expected sequence of
    /// keywords, or consume them if they are.
    fn expect_keywords(&mut self, expected: &[Keyword]) -> Result<(), ParserError> {
        for kw in expected {
            self.expect_keyword(*kw)?;
        }
        Ok(())
    }

    /// Consume the next token if it matches the expected token, otherwise return false
    #[must_use]
    fn consume_token(&mut self, expected: &Token) -> bool {
        match &self.peek_token() {
            Some(t) if *t == *expected => {
                self.next_token();
                true
            }
            _ => false,
        }
    }

    /// Bail out if the current token is not an expected keyword, or consume it if it is
    fn expect_token(&mut self, expected: &Token) -> Result<(), ParserError> {
        if self.consume_token(expected) {
            Ok(())
        } else {
            self.expected(self.peek_pos(), expected, self.peek_token())
        }
    }

    /// Parse a comma-separated list of 1+ items accepted by `F`
    fn parse_comma_separated<T, F>(&mut self, mut f: F) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Self) -> Result<T, ParserError>,
    {
        let mut values = vec![];
        loop {
            values.push(f(self)?);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(values)
    }

    #[must_use]
    fn maybe_parse<T, F>(&mut self, mut f: F) -> Option<T>
    where
        F: FnMut(&mut Self) -> Result<T, ParserError>,
    {
        let index = self.index;
        if let Ok(t) = f(self) {
            Some(t)
        } else {
            self.index = index;
            None
        }
    }

    /// Parse a SQL CREATE statement
    fn parse_create(&mut self) -> Result<Statement<Raw>, ParserError> {
        if self.peek_keyword(DATABASE) {
            self.parse_create_database()
        } else if self.peek_keyword(SCHEMA) {
            self.parse_create_schema()
        } else if self.peek_keyword(SINK) {
            self.parse_create_sink()
        } else if self.peek_keyword(TYPE) {
            self.parse_create_type()
        } else if self.peek_keyword(ROLE) || self.peek_keyword(USER) {
            self.parse_create_role()
        } else if self.peek_keyword(CLUSTER) {
            self.next_token();
            if self.peek_keyword(REPLICA) {
                self.parse_create_cluster_replica()
            } else {
                self.parse_create_cluster()
            }
        } else if self.peek_keyword(INDEX) || self.peek_keywords(&[DEFAULT, INDEX]) {
            self.parse_create_index()
        } else if self.peek_keyword(SOURCE) {
            self.parse_create_source()
        } else if self.peek_keyword(TABLE)
            || self.peek_keywords(&[TEMP, TABLE])
            || self.peek_keywords(&[TEMPORARY, TABLE])
        {
            self.parse_create_table()
        } else if self.peek_keyword(SECRET) {
            self.parse_create_secret()
        } else if self.peek_keyword(CONNECTION) {
            self.parse_create_connection()
        } else if self.peek_keywords(&[MATERIALIZED, VIEW])
            || self.peek_keywords(&[OR, REPLACE, MATERIALIZED, VIEW])
        {
            self.parse_create_materialized_view()
        } else {
            let index = self.index;

            // go over optional modifiers
            let _ = self.parse_keywords(&[OR, REPLACE]);
            let _ = self.parse_one_of_keywords(&[TEMP, TEMPORARY]);

            if self.parse_keyword(VIEW) {
                self.index = index;
                self.parse_create_view()
            } else if self.parse_keyword(VIEWS) {
                self.index = index;
                self.parse_create_views()
            } else {
                self.expected(
                    self.peek_pos(),
                    "DATABASE, SCHEMA, ROLE, USER, TYPE, INDEX, SINK, SOURCE, TABLE, SECRET or [OR REPLACE] [TEMPORARY] VIEW or VIEWS after CREATE",
                    self.peek_token(),
                )
            }
        }
    }

    fn parse_create_database(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(DATABASE)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_database_name()?;
        Ok(Statement::CreateDatabase(CreateDatabaseStatement {
            name,
            if_not_exists,
        }))
    }

    fn parse_create_schema(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(SCHEMA)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_schema_name()?;
        Ok(Statement::CreateSchema(CreateSchemaStatement {
            name,
            if_not_exists,
        }))
    }

    fn parse_format(&mut self) -> Result<Format<Raw>, ParserError> {
        let format = if self.parse_keyword(AVRO) {
            self.expect_keyword(USING)?;
            Format::Avro(self.parse_avro_schema()?)
        } else if self.parse_keyword(PROTOBUF) {
            Format::Protobuf(self.parse_protobuf_schema()?)
        } else if self.parse_keyword(REGEX) {
            let regex = self.parse_literal_string()?;
            Format::Regex(regex)
        } else if self.parse_keyword(CSV) {
            self.expect_keyword(WITH)?;
            let columns = if self.parse_keyword(HEADER) || self.parse_keyword(HEADERS) {
                CsvColumns::Header {
                    names: self.parse_parenthesized_column_list(Mandatory)?,
                }
            } else {
                let n_cols = self.parse_literal_uint()? as usize;
                self.expect_keyword(COLUMNS)?;
                CsvColumns::Count(n_cols)
            };
            let delimiter = if self.parse_keywords(&[DELIMITED, BY]) {
                let s = self.parse_literal_string()?;
                match s.len() {
                    1 => Ok(s.chars().next().unwrap()),
                    _ => self.expected(self.peek_pos(), "one-character string", self.peek_token()),
                }?
            } else {
                ','
            };
            Format::Csv { columns, delimiter }
        } else if self.parse_keyword(JSON) {
            Format::Json
        } else if self.parse_keyword(TEXT) {
            Format::Text
        } else if self.parse_keyword(BYTES) {
            Format::Bytes
        } else {
            return self.expected(
                self.peek_pos(),
                "AVRO, PROTOBUF, REGEX, CSV, JSON, TEXT, or BYTES",
                self.peek_token(),
            );
        };
        Ok(format)
    }

    fn parse_avro_schema(&mut self) -> Result<AvroSchema<Raw>, ParserError> {
        let avro_schema = if self.parse_keywords(&[CONFLUENT, SCHEMA, REGISTRY]) {
            let csr_connection = self.parse_csr_connection_avro()?;
            AvroSchema::Csr { csr_connection }
        } else if self.parse_keyword(SCHEMA) {
            self.prev_token();
            let schema = self.parse_schema()?;
            // Look ahead to avoid erroring on `WITH SNAPSHOT`; we only want to
            // accept `WITH (...)` here.
            let with_options = if self.peek_keyword(WITH)
                && self.peek_nth_token(1) != Some(Token::Keyword(SNAPSHOT))
            {
                self.parse_kw_options(Parser::parse_avro_schema_options)?
            } else {
                vec![]
            };
            AvroSchema::InlineSchema {
                schema,
                with_options,
            }
        } else {
            return self.expected(
                self.peek_pos(),
                "CONFLUENT SCHEMA REGISTRY or SCHEMA",
                self.peek_token(),
            );
        };
        Ok(avro_schema)
    }

    fn parse_avro_schema_options(&mut self) -> Result<AvroSchemaOption<Raw>, ParserError> {
        self.expect_keywords(&[CONFLUENT, WIRE, FORMAT])?;
        let _ = self.consume_token(&Token::Eq);
        Ok(AvroSchemaOption {
            name: AvroSchemaOptionName::ConfluentWireFormat,
            value: self.parse_opt_with_option_value(false)?,
        })
    }

    fn parse_protobuf_schema(&mut self) -> Result<ProtobufSchema<Raw>, ParserError> {
        if self.parse_keywords(&[USING, CONFLUENT, SCHEMA, REGISTRY]) {
            let csr_connection = self.parse_csr_connection_proto()?;
            Ok(ProtobufSchema::Csr { csr_connection })
        } else if self.parse_keyword(MESSAGE) {
            let message_name = self.parse_literal_string()?;
            self.expect_keyword(USING)?;
            let schema = self.parse_schema()?;
            Ok(ProtobufSchema::InlineSchema {
                message_name,
                schema,
            })
        } else {
            return self.expected(
                self.peek_pos(),
                "CONFLUENT SCHEMA REGISTRY or MESSAGE",
                self.peek_token(),
            );
        }
    }

    fn parse_csr_connection_avro(&mut self) -> Result<CsrConnectionAvro<Raw>, ParserError> {
        let connection = if self.parse_keyword(CONNECTION) {
            CsrConnection::Reference {
                connection: self.parse_raw_name()?,
            }
        } else {
            CsrConnection::Inline {
                url: self.parse_literal_string()?,
            }
        };
        let seed = if self.parse_keyword(SEED) {
            let key_schema = if self.parse_keyword(KEY) {
                self.expect_keyword(SCHEMA)?;
                Some(self.parse_literal_string()?)
            } else {
                None
            };
            self.expect_keywords(&[VALUE, SCHEMA])?;
            let value_schema = self.parse_literal_string()?;
            Some(CsrSeedAvro {
                key_schema,
                value_schema,
            })
        } else {
            None
        };

        let mut parse_schema_strategy =
            |kws| -> Result<Option<ReaderSchemaSelectionStrategy>, ParserError> {
                if self.parse_keywords(kws) {
                    Ok(Some(
                        match self.expect_one_of_keywords(&[ID, LATEST, INLINE])? {
                            ID => {
                                let pos = self.index;
                                ReaderSchemaSelectionStrategy::ById(
                                    self.parse_literal_int()?.try_into().map_err(|_| {
                                        ParserError::new(pos, "Expected a 32-bit integer")
                                    })?,
                                )
                            }
                            LATEST => ReaderSchemaSelectionStrategy::Latest,
                            INLINE => {
                                ReaderSchemaSelectionStrategy::Inline(self.parse_literal_string()?)
                            }
                            _ => unreachable!(),
                        },
                    ))
                } else {
                    Ok(None)
                }
            };

        let key_strategy = parse_schema_strategy(&[KEY, STRATEGY])?;
        let value_strategy = parse_schema_strategy(&[VALUE, STRATEGY])?;

        // Look ahead to avoid erroring on `WITH SNAPSHOT`; we only want to
        // accept `WITH (...)` here.
        let with_options = if self.peek_nth_token(1) == Some(Token::LParen) {
            self.parse_opt_with_options()?
        } else {
            vec![]
        };
        Ok(CsrConnectionAvro {
            connection,
            seed,
            with_options,
            key_strategy,
            value_strategy,
        })
    }

    fn parse_csr_connection_proto(&mut self) -> Result<CsrConnectionProtobuf<Raw>, ParserError> {
        let connection = if self.parse_keyword(CONNECTION) {
            CsrConnection::Reference {
                connection: self.parse_raw_name()?,
            }
        } else {
            CsrConnection::Inline {
                url: self.parse_literal_string()?,
            }
        };

        let seed = if self.parse_keyword(SEED) {
            let key = if self.parse_keyword(KEY) {
                self.expect_keyword(SCHEMA)?;
                let schema = self.parse_literal_string()?;
                self.expect_keyword(MESSAGE)?;
                let message_name = self.parse_literal_string()?;
                Some(CsrSeedProtobufSchema {
                    schema,
                    message_name,
                })
            } else {
                None
            };
            self.expect_keywords(&[VALUE, SCHEMA])?;
            let value_schema = self.parse_literal_string()?;
            self.expect_keyword(MESSAGE)?;
            let value_message_name = self.parse_literal_string()?;
            Some(CsrSeedProtobuf {
                value: CsrSeedProtobufSchema {
                    schema: value_schema,
                    message_name: value_message_name,
                },
                key,
            })
        } else {
            None
        };

        // Look ahead to avoid erroring on `WITH SNAPSHOT`; we only want to
        // accept `WITH (...)` here.
        let with_options = if self.peek_nth_token(1) == Some(Token::LParen) {
            self.parse_opt_with_options()?
        } else {
            vec![]
        };

        Ok(CsrConnectionProtobuf {
            connection,
            seed,
            with_options,
        })
    }

    fn parse_schema(&mut self) -> Result<Schema, ParserError> {
        self.expect_keyword(SCHEMA)?;
        let schema = if self.parse_keyword(FILE) {
            Schema::File(self.parse_literal_string()?.into())
        } else {
            Schema::Inline(self.parse_literal_string()?)
        };
        Ok(schema)
    }

    fn parse_envelope(&mut self) -> Result<Envelope<Raw>, ParserError> {
        let envelope = if self.parse_keyword(NONE) {
            Envelope::None
        } else if self.parse_keyword(DEBEZIUM) {
            let debezium_mode = if self.parse_keyword(UPSERT) {
                DbzMode::Upsert
            } else {
                let tx_metadata = if self.consume_token(&Token::LParen) {
                    self.expect_keywords(&[TRANSACTION, METADATA])?;
                    self.expect_token(&Token::LParen)?;
                    let options = self.parse_comma_separated(|parser| {
                        match parser.expect_one_of_keywords(&[SOURCE, COLLECTION])? {
                            SOURCE => {
                                let _ = parser.consume_token(&Token::Eq);
                                Ok(DbzTxMetadataOption::Source(parser.parse_raw_name()?))
                            }
                            COLLECTION => {
                                let _ = parser.consume_token(&Token::Eq);
                                Ok(DbzTxMetadataOption::Collection(
                                    parser.parse_with_option_value()?,
                                ))
                            }
                            _ => unreachable!(),
                        }
                    })?;
                    self.expect_token(&Token::RParen)?;
                    self.expect_token(&Token::RParen)?;
                    options
                } else {
                    vec![]
                };
                DbzMode::Plain { tx_metadata }
            };
            Envelope::Debezium(debezium_mode)
        } else if self.parse_keyword(UPSERT) {
            Envelope::Upsert
        } else if self.parse_keyword(MATERIALIZE) {
            Envelope::CdcV2
        } else {
            return self.expected(
                self.peek_pos(),
                "NONE, DEBEZIUM, UPSERT, or MATERIALIZE",
                self.peek_token(),
            );
        };
        Ok(envelope)
    }

    fn parse_compression(&mut self) -> Result<Compression, ParserError> {
        let compression = if self.parse_keyword(NONE) {
            Compression::None
        } else if self.parse_keyword(GZIP) {
            Compression::Gzip
        } else {
            return self.expected(self.peek_pos(), "NONE or GZIP", self.peek_token());
        };
        Ok(compression)
    }

    fn parse_create_connection(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(CONNECTION)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_object_name()?;
        self.expect_keyword(FOR)?;
        let connection = match self.expect_one_of_keywords(&[KAFKA, CONFLUENT, POSTGRES, SSH])? {
            KAFKA => {
                let with_options =
                    self.parse_comma_separated(Parser::parse_kafka_connection_options)?;
                CreateConnection::Kafka { with_options }
            }
            CONFLUENT => {
                self.expect_keywords(&[SCHEMA, REGISTRY])?;
                let with_options =
                    self.parse_comma_separated(Parser::parse_csr_connection_options)?;
                CreateConnection::Csr { with_options }
            }
            POSTGRES => {
                let with_options =
                    self.parse_comma_separated(Parser::parse_postgres_connection_options)?;
                CreateConnection::Postgres { with_options }
            }
            SSH => {
                self.expect_keyword(TUNNEL)?;
                let with_options =
                    self.parse_comma_separated(Parser::parse_ssh_connection_options)?;
                CreateConnection::Ssh { with_options }
            }
            _ => unreachable!(),
        };
        Ok(Statement::CreateConnection(CreateConnectionStatement {
            name,
            connection,
            if_not_exists,
        }))
    }

    fn parse_kafka_connection_options(
        &mut self,
    ) -> Result<KafkaConnectionOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[BROKER, BROKERS, SASL, SSL])? {
            BROKER => KafkaConnectionOptionName::Broker,
            BROKERS => KafkaConnectionOptionName::Brokers,
            SASL => match self.expect_one_of_keywords(&[MECHANISMS, PASSWORD, USERNAME])? {
                MECHANISMS => KafkaConnectionOptionName::SaslMechanisms,
                PASSWORD => KafkaConnectionOptionName::SaslPassword,
                USERNAME => KafkaConnectionOptionName::SaslUsername,
                _ => unreachable!(),
            },
            SSL => match self.expect_one_of_keywords(&[KEY, CERTIFICATE])? {
                KEY => KafkaConnectionOptionName::SslKey,
                CERTIFICATE => {
                    if self.parse_keyword(AUTHORITY) {
                        KafkaConnectionOptionName::SslCertificateAuthority
                    } else {
                        KafkaConnectionOptionName::SslCertificate
                    }
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };

        let _ = self.consume_token(&Token::Eq);
        Ok(KafkaConnectionOption {
            name,
            value: self.parse_opt_with_option_value(false)?,
        })
    }

    fn parse_csr_connection_options(&mut self) -> Result<CsrConnectionOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[SSL, URL, USERNAME, PASSWORD])? {
            SSL => match self.expect_one_of_keywords(&[KEY, CERTIFICATE])? {
                KEY => CsrConnectionOptionName::SslKey,
                CERTIFICATE => {
                    if self.parse_keyword(AUTHORITY) {
                        CsrConnectionOptionName::SslCertificateAuthority
                    } else {
                        CsrConnectionOptionName::SslCertificate
                    }
                }
                _ => unreachable!(),
            },
            URL => CsrConnectionOptionName::Url,
            USERNAME => CsrConnectionOptionName::Username,
            PASSWORD => CsrConnectionOptionName::Password,
            _ => unreachable!(),
        };

        let _ = self.consume_token(&Token::Eq);
        Ok(CsrConnectionOption {
            name,
            value: self.parse_opt_with_option_value(false)?,
        })
    }

    fn parse_postgres_connection_options(
        &mut self,
    ) -> Result<PostgresConnectionOption<Raw>, ParserError> {
        let name = match self
            .expect_one_of_keywords(&[DATABASE, HOST, PASSWORD, PORT, SSH, SSL, USER, USERNAME])?
        {
            DATABASE => PostgresConnectionOptionName::Database,
            HOST => PostgresConnectionOptionName::Host,
            PASSWORD => PostgresConnectionOptionName::Password,
            PORT => PostgresConnectionOptionName::Port,
            SSH => {
                self.expect_keyword(TUNNEL)?;
                PostgresConnectionOptionName::SshTunnel
            }
            SSL => match self.expect_one_of_keywords(&[CERTIFICATE, MODE, KEY])? {
                CERTIFICATE => {
                    if self.parse_keyword(AUTHORITY) {
                        PostgresConnectionOptionName::SslCertificateAuthority
                    } else {
                        PostgresConnectionOptionName::SslCertificate
                    }
                }
                KEY => PostgresConnectionOptionName::SslKey,
                MODE => PostgresConnectionOptionName::SslMode,
                _ => unreachable!(),
            },
            USER | USERNAME => PostgresConnectionOptionName::User,
            _ => unreachable!(),
        };

        let _ = self.consume_token(&Token::Eq);
        Ok(PostgresConnectionOption {
            name,
            value: self.parse_opt_with_option_value(false)?,
        })
    }

    fn parse_ssh_connection_options(&mut self) -> Result<SshConnectionOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[HOST, PORT, USER])? {
            HOST => SshConnectionOptionName::Host,
            PORT => SshConnectionOptionName::Port,
            USER => SshConnectionOptionName::User,
            _ => unreachable!(),
        };

        let _ = self.consume_token(&Token::Eq);
        Ok(SshConnectionOption {
            name,
            value: self.parse_opt_with_option_value(false)?,
        })
    }

    fn parse_create_source(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(SOURCE)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_object_name()?;
        let (col_names, key_constraint) = self.parse_source_columns()?;
        self.expect_keyword(FROM)?;
        let connection = self.parse_create_source_connection()?;
        let with_options = self.parse_opt_with_options()?;
        let format = match self.parse_one_of_keywords(&[KEY, FORMAT]) {
            Some(KEY) => {
                self.expect_keyword(FORMAT)?;
                let key = self.parse_format()?;
                self.expect_keywords(&[VALUE, FORMAT])?;
                let value = self.parse_format()?;
                CreateSourceFormat::KeyValue { key, value }
            }
            Some(FORMAT) => CreateSourceFormat::Bare(self.parse_format()?),
            Some(_) => unreachable!("parse_one_of_keywords returns None for this"),
            None => CreateSourceFormat::None,
        };
        let include_metadata = self.parse_source_include_metadata()?;

        let envelope = if self.parse_keyword(ENVELOPE) {
            Some(self.parse_envelope()?)
        } else {
            None
        };

        let remote = if self.parse_keyword(REMOTE) {
            Some(self.parse_literal_string()?)
        } else {
            None
        };

        Ok(Statement::CreateSource(CreateSourceStatement {
            name,
            col_names,
            connection,
            with_options,
            format,
            include_metadata,
            envelope,
            if_not_exists,
            key_constraint,
            remote,
        }))
    }

    /// Parses the column section of a CREATE SOURCE statement which can be
    /// empty or a comma-separated list of column identifiers and a single key
    /// constraint, e.g.
    ///
    /// (col_0, col_i, ..., col_n, key_constraint)
    fn parse_source_columns(&mut self) -> Result<(Vec<Ident>, Option<KeyConstraint>), ParserError> {
        if self.consume_token(&Token::LParen) {
            let mut columns = vec![];
            let mut key_constraints = vec![];
            loop {
                let pos = self.peek_pos();
                if let Some(key_constraint) = self.parse_key_constraint()? {
                    if !key_constraints.is_empty() {
                        return parser_err!(self, pos, "Multiple key constraints not allowed");
                    }
                    key_constraints.push(key_constraint);
                } else {
                    columns.push(self.parse_identifier()?);
                }
                if !self.consume_token(&Token::Comma) {
                    break;
                }
            }
            self.expect_token(&Token::RParen)?;
            Ok((columns, key_constraints.into_iter().next()))
        } else {
            Ok((vec![], None))
        }
    }

    /// Parses a key constraint.
    fn parse_key_constraint(&mut self) -> Result<Option<KeyConstraint>, ParserError> {
        // PRIMARY KEY (col_1, ..., col_n) NOT ENFORCED
        if self.parse_keywords(&[PRIMARY, KEY]) {
            let columns = self.parse_parenthesized_column_list(Mandatory)?;
            self.expect_keywords(&[NOT, ENFORCED])?;
            Ok(Some(KeyConstraint::PrimaryKeyNotEnforced { columns }))
        } else {
            Ok(None)
        }
    }

    fn parse_create_sink(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(SINK)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_object_name()?;
        let in_cluster = self.parse_optional_in_cluster()?;
        self.expect_keyword(FROM)?;
        let from = self.parse_raw_name()?;
        self.expect_keyword(INTO)?;
        let connection = self.parse_create_sink_connection()?;
        let mut with_options = vec![];
        if self.parse_keyword(WITH) {
            if let Some(Token::LParen) = self.next_token() {
                self.prev_token();
                self.prev_token();
                with_options = self.parse_opt_with_options()?;
            }
        }
        let format = if self.parse_keyword(FORMAT) {
            Some(self.parse_format()?)
        } else {
            None
        };
        let envelope = if self.parse_keyword(ENVELOPE) {
            Some(self.parse_envelope()?)
        } else {
            None
        };
        let with_snapshot = if self.parse_keyword(WITH) {
            self.expect_keyword(SNAPSHOT)?;
            true
        } else if self.parse_keyword(WITHOUT) {
            self.expect_keyword(SNAPSHOT)?;
            false
        } else {
            // If neither WITH nor WITHOUT SNAPSHOT is provided,
            // default to WITH SNAPSHOT.
            true
        };
        let as_of = self.parse_optional_as_of()?;
        Ok(Statement::CreateSink(CreateSinkStatement {
            name,
            in_cluster,
            from,
            connection,
            with_options,
            format,
            envelope,
            with_snapshot,
            as_of,
            if_not_exists,
        }))
    }

    fn parse_create_source_connection(
        &mut self,
    ) -> Result<CreateSourceConnection<Raw>, ParserError> {
        match self.expect_one_of_keywords(&[KAFKA, KINESIS, S3, POSTGRES, PUBNUB, LOAD])? {
            PUBNUB => {
                self.expect_keywords(&[SUBSCRIBE, KEY])?;
                let subscribe_key = self.parse_literal_string()?;
                self.expect_keyword(CHANNEL)?;
                let channel = self.parse_literal_string()?;

                Ok(CreateSourceConnection::PubNub {
                    subscribe_key,
                    channel,
                })
            }
            POSTGRES => {
                self.expect_keyword(CONNECTION)?;
                let connection = self.parse_raw_name()?;
                self.expect_keyword(PUBLICATION)?;
                let publication = self.parse_literal_string()?;
                let details = if self.parse_keyword(DETAILS) {
                    Some(self.parse_literal_string()?)
                } else {
                    None
                };

                Ok(CreateSourceConnection::Postgres {
                    connection,
                    publication,
                    details,
                })
            }
            KAFKA => {
                let connection = match self.expect_one_of_keywords(&[BROKER, CONNECTION])? {
                    BROKER => KafkaConnection::Inline {
                        broker: self.parse_literal_string()?,
                    },
                    CONNECTION => KafkaConnection::Reference {
                        connection: self.parse_raw_name()?,
                    },
                    _ => unreachable!(),
                };
                self.expect_keyword(TOPIC)?;
                let topic = self.parse_literal_string()?;
                // one token of lookahead:
                // * `KEY (` means we're parsing a list of columns for the key
                // * `KEY FORMAT` means there is no key, we'll parse a KeyValueFormat later
                let key = if self.peek_keyword(KEY)
                    && self.peek_nth_token(1) != Some(Token::Keyword(FORMAT))
                {
                    let _ = self.expect_keyword(KEY);
                    Some(self.parse_parenthesized_column_list(Mandatory)?)
                } else {
                    None
                };
                Ok(CreateSourceConnection::Kafka(KafkaSourceConnection {
                    connection,
                    topic,
                    key,
                }))
            }
            KINESIS => {
                self.expect_keyword(ARN)?;
                let arn = self.parse_literal_string()?;
                Ok(CreateSourceConnection::Kinesis { arn })
            }
            S3 => {
                // FROM S3 DISCOVER OBJECTS
                // (MATCHING '<pattern>')?
                // USING
                // (BUCKET SCAN '<bucket>' | SQS NOTIFICATIONS '<channel>')+
                self.expect_keywords(&[DISCOVER, OBJECTS])?;
                let pattern = if self.parse_keyword(MATCHING) {
                    Some(self.parse_literal_string()?)
                } else {
                    None
                };
                self.expect_keyword(USING)?;
                let mut key_sources = Vec::new();
                while let Some(keyword) = self.parse_one_of_keywords(&[BUCKET, SQS]) {
                    match keyword {
                        BUCKET => {
                            self.expect_keyword(SCAN)?;
                            let bucket = self.parse_literal_string()?;
                            key_sources.push(S3KeySource::Scan { bucket });
                        }
                        SQS => {
                            self.expect_keyword(NOTIFICATIONS)?;
                            let queue = self.parse_literal_string()?;
                            key_sources.push(S3KeySource::SqsNotifications { queue });
                        }
                        key => unreachable!(
                            "Keyword {} is not expected after DISCOVER OBJECTS USING",
                            key
                        ),
                    }
                    if !self.consume_token(&Token::Comma) {
                        break;
                    }
                }
                let compression = if self.parse_keyword(COMPRESSION) {
                    self.parse_compression()?
                } else {
                    Compression::None
                };
                Ok(CreateSourceConnection::S3 {
                    key_sources,
                    pattern,
                    compression,
                })
            }
            LOAD => {
                self.expect_keyword(GENERATOR)?;
                let generator = match self.expect_one_of_keywords(&[COUNTER, AUCTION])? {
                    COUNTER => LoadGenerator::Counter,
                    AUCTION => LoadGenerator::Auction,
                    _ => unreachable!(),
                };
                let options = if matches!(self.peek_token(), Some(Token::Semicolon) | None) {
                    vec![]
                } else {
                    self.parse_comma_separated(Parser::parse_load_generator_options)?
                };
                Ok(CreateSourceConnection::LoadGenerator { generator, options })
            }
            _ => unreachable!(),
        }
    }

    fn parse_load_generator_options(&mut self) -> Result<LoadGeneratorOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[TICK])? {
            TICK => {
                self.expect_keyword(INTERVAL)?;
                LoadGeneratorOptionName::TickInterval
            }
            _ => unreachable!(),
        };

        let _ = self.consume_token(&Token::Eq);
        Ok(LoadGeneratorOption {
            name,
            value: self.parse_opt_with_option_value(false)?,
        })
    }

    fn parse_create_sink_connection(&mut self) -> Result<CreateSinkConnection<Raw>, ParserError> {
        self.expect_keyword(KAFKA)?;
        self.expect_keyword(BROKER)?;
        let broker = self.parse_literal_string()?;
        self.expect_keyword(TOPIC)?;
        let topic = self.parse_literal_string()?;
        // one token of lookahead:
        // * `KEY (` means we're parsing a list of columns for the key
        // * `KEY FORMAT` means there is no key, we'll parse a KeyValueFormat later
        let key =
            if self.peek_keyword(KEY) && self.peek_nth_token(1) != Some(Token::Keyword(FORMAT)) {
                let _ = self.expect_keyword(KEY);
                let key_columns = self.parse_parenthesized_column_list(Mandatory)?;

                let not_enforced = if self.peek_keywords(&[NOT, ENFORCED]) {
                    let _ = self.expect_keywords(&[NOT, ENFORCED])?;
                    true
                } else {
                    false
                };
                Some(KafkaSinkKey {
                    key_columns,
                    not_enforced,
                })
            } else {
                None
            };
        let consistency = self.parse_kafka_consistency()?;
        Ok(CreateSinkConnection::Kafka {
            broker,
            topic,
            key,
            consistency,
        })
    }

    fn parse_kafka_consistency(&mut self) -> Result<Option<KafkaConsistency<Raw>>, ParserError> {
        if self.parse_keyword(CONSISTENCY) {
            // We would prefer for all consistency parameters to be
            // parenthesized, but for backwards compatibility we support an
            // unparenthesized syntax that has some ambiguity issues
            // (see #8231).
            //
            // Bad:
            //     CONSISTENCY TOPIC 'foo' CONSISTENCY FORMAT 'bar' WITH (format_option = 'blah')
            // Good:
            //     CONSISTENCY (TOPIC 'foo' FORMAT 'bar' WITH (format_option = 'blah'))
            let parenthesized = self.consume_token(&Token::LParen);
            self.expect_keyword(TOPIC)?;
            let topic = self.parse_literal_string()?;
            let topic_format = if (parenthesized && self.parse_keyword(FORMAT))
                || (!parenthesized && self.parse_keywords(&[CONSISTENCY, FORMAT]))
            {
                Some(self.parse_format()?)
            } else {
                None
            };
            if parenthesized {
                self.expect_token(&Token::RParen)?;
            }
            Ok(Some(KafkaConsistency {
                topic,
                topic_format,
            }))
        } else {
            Ok(None)
        }
    }

    fn parse_create_view(&mut self) -> Result<Statement<Raw>, ParserError> {
        let mut if_exists = if self.parse_keyword(OR) {
            self.expect_keyword(REPLACE)?;
            IfExistsBehavior::Replace
        } else {
            IfExistsBehavior::Error
        };
        let temporary = self.parse_keyword(TEMPORARY) | self.parse_keyword(TEMP);
        self.expect_keyword(VIEW)?;
        if if_exists == IfExistsBehavior::Error && self.parse_if_not_exists()? {
            if_exists = IfExistsBehavior::Skip;
        }

        let definition = self.parse_view_definition()?;
        Ok(Statement::CreateView(CreateViewStatement {
            temporary,
            if_exists,
            definition,
        }))
    }

    fn parse_view_definition(&mut self) -> Result<ViewDefinition<Raw>, ParserError> {
        // ANSI SQL and Postgres support RECURSIVE here, but we don't.
        let name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        // Postgres supports WITH options here, but we don't.
        self.expect_keyword(AS)?;
        let query = self.parse_query()?;
        // Optional `WITH [ CASCADED | LOCAL ] CHECK OPTION` is widely supported here.
        Ok(ViewDefinition {
            name,
            columns,
            query,
        })
    }

    fn parse_create_views(&mut self) -> Result<Statement<Raw>, ParserError> {
        let mut if_exists = if self.parse_keyword(OR) {
            self.expect_keyword(REPLACE)?;
            IfExistsBehavior::Replace
        } else {
            IfExistsBehavior::Error
        };
        let temporary = self.parse_keyword(TEMPORARY) | self.parse_keyword(TEMP);
        self.expect_keyword(VIEWS)?;
        if if_exists == IfExistsBehavior::Error && self.parse_if_not_exists()? {
            if_exists = IfExistsBehavior::Skip;
        }

        self.expect_keywords(&[FROM, SOURCE])?;
        let source = self.parse_raw_name()?;
        let targets = if self.consume_token(&Token::LParen) {
            let targets = self.parse_comma_separated(|parser| {
                let name = parser.parse_object_name()?;
                let alias = if parser.parse_keyword(AS) {
                    Some(parser.parse_object_name()?)
                } else {
                    None
                };
                Ok(CreateViewsSourceTarget { name, alias })
            })?;
            self.expect_token(&Token::RParen)?;
            Some(targets)
        } else {
            None
        };

        Ok(Statement::CreateViews(CreateViewsStatement {
            temporary,
            if_exists,
            source,
            targets,
        }))
    }

    fn parse_create_materialized_view(&mut self) -> Result<Statement<Raw>, ParserError> {
        let mut if_exists = if self.parse_keyword(OR) {
            self.expect_keyword(REPLACE)?;
            IfExistsBehavior::Replace
        } else {
            IfExistsBehavior::Error
        };
        self.expect_keywords(&[MATERIALIZED, VIEW])?;
        if if_exists == IfExistsBehavior::Error && self.parse_if_not_exists()? {
            if_exists = IfExistsBehavior::Skip;
        }

        let name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        let in_cluster = self.parse_optional_in_cluster()?;

        self.expect_keyword(AS)?;
        let query = self.parse_query()?;

        Ok(Statement::CreateMaterializedView(
            CreateMaterializedViewStatement {
                if_exists,
                name,
                columns,
                in_cluster,
                query,
            },
        ))
    }

    fn parse_create_index(&mut self) -> Result<Statement<Raw>, ParserError> {
        let default_index = self.parse_keyword(DEFAULT);
        self.expect_keyword(INDEX)?;

        let if_not_exists = self.parse_if_not_exists()?;
        let name = if self.peek_keyword(IN) || self.peek_keyword(ON) {
            if if_not_exists && !default_index {
                return self.expected(self.peek_pos(), "index name", self.peek_token());
            }
            None
        } else {
            Some(self.parse_identifier()?)
        };
        let in_cluster = self.parse_optional_in_cluster()?;
        self.expect_keyword(ON)?;
        let on_name = self.parse_raw_name()?;

        // Arrangements are the only index type we support, so we can just ignore this
        if self.parse_keyword(USING) {
            self.expect_keyword(ARRANGEMENT)?;
        }

        let key_parts = if default_index {
            None
        } else {
            self.expect_token(&Token::LParen)?;
            if self.consume_token(&Token::RParen) {
                Some(vec![])
            } else {
                let key_parts = self
                    .parse_comma_separated(Parser::parse_order_by_expr)?
                    .into_iter()
                    .map(|x| x.expr)
                    .collect::<Vec<_>>();
                self.expect_token(&Token::RParen)?;
                Some(key_parts)
            }
        };

        let with_options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            let o = if matches!(self.peek_token(), Some(Token::RParen)) {
                vec![]
            } else {
                self.parse_comma_separated(Parser::parse_index_option)?
            };
            self.expect_token(&Token::RParen)?;
            o
        } else {
            vec![]
        };

        Ok(Statement::CreateIndex(CreateIndexStatement {
            name,
            in_cluster,
            on_name,
            key_parts,
            with_options,
            if_not_exists,
        }))
    }

    fn parse_index_option_name(&mut self) -> Result<IndexOptionName, ParserError> {
        self.expect_keywords(&[LOGICAL, COMPACTION, WINDOW])?;
        Ok(IndexOptionName::LogicalCompactionWindow)
    }

    fn parse_index_option(&mut self) -> Result<IndexOption<Raw>, ParserError> {
        let name = self.parse_index_option_name()?;
        let _ = self.consume_token(&Token::Eq);
        let value = self.parse_opt_with_option_value(false)?;
        Ok(IndexOption { name, value })
    }

    fn parse_raw_ident(&mut self) -> Result<RawClusterName, ParserError> {
        if self.consume_token(&Token::LBracket) {
            let id = match self.next_token() {
                Some(Token::Ident(id)) => id,
                Some(Token::Number(n)) => n,
                _ => return parser_err!(self, self.peek_prev_pos(), "expected id"),
            };
            self.expect_token(&Token::RBracket)?;
            Ok(RawClusterName::Resolved(id))
        } else {
            Ok(RawClusterName::Unresolved(self.parse_identifier()?))
        }
    }

    fn parse_optional_in_cluster(&mut self) -> Result<Option<RawClusterName>, ParserError> {
        if self.parse_keywords(&[IN, CLUSTER]) {
            Ok(Some(self.parse_raw_ident()?))
        } else {
            Ok(None)
        }
    }

    fn parse_create_role(&mut self) -> Result<Statement<Raw>, ParserError> {
        let is_user = match self.expect_one_of_keywords(&[ROLE, USER])? {
            ROLE => false,
            USER => true,
            _ => unreachable!(),
        };
        let name = self.parse_identifier()?;
        let _ = self.parse_keyword(WITH);
        let mut options = vec![];
        loop {
            match self.parse_one_of_keywords(&[SUPERUSER, NOSUPERUSER, LOGIN, NOLOGIN]) {
                None => break,
                Some(SUPERUSER) => options.push(CreateRoleOption::SuperUser),
                Some(NOSUPERUSER) => options.push(CreateRoleOption::NoSuperUser),
                Some(LOGIN) => options.push(CreateRoleOption::Login),
                Some(NOLOGIN) => options.push(CreateRoleOption::NoLogin),
                Some(_) => unreachable!(),
            }
        }
        Ok(Statement::CreateRole(CreateRoleStatement {
            is_user,
            name,
            options,
        }))
    }

    fn parse_create_secret(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(SECRET)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_object_name()?;
        self.expect_keyword(AS)?;
        let value = self.parse_expr()?;
        Ok(Statement::CreateSecret(CreateSecretStatement {
            name,
            if_not_exists,
            value,
        }))
    }

    fn parse_create_type(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(TYPE)?;
        let name = self.parse_object_name()?;
        self.expect_keyword(AS)?;

        match self.parse_one_of_keywords(&[LIST, MAP]) {
            Some(as_type) => {
                self.expect_token(&Token::LParen)?;
                let with_options = self.parse_comma_separated(Parser::parse_data_type_option)?;
                self.expect_token(&Token::RParen)?;

                let as_type = match as_type {
                    LIST => CreateTypeAs::List { with_options },
                    MAP => CreateTypeAs::Map { with_options },
                    _ => unreachable!(),
                };

                Ok(Statement::CreateType(CreateTypeStatement { name, as_type }))
            }
            None => {
                let column_defs = self.parse_composite_type_definition()?;

                Ok(Statement::CreateType(CreateTypeStatement {
                    name,
                    as_type: CreateTypeAs::Record { column_defs },
                }))
            }
        }
    }

    fn parse_data_type_option(&mut self) -> Result<WithOption<Raw>, ParserError> {
        let key = self.parse_identifier()?;
        self.expect_token(&Token::Eq)?;
        Ok(WithOption {
            key,
            value: Some(WithOptionValue::DataType(self.parse_data_type()?)),
        })
    }

    fn parse_create_cluster(&mut self) -> Result<Statement<Raw>, ParserError> {
        let name = self.parse_identifier()?;

        let options = if matches!(self.peek_token(), Some(Token::Semicolon) | None) {
            vec![]
        } else {
            self.parse_comma_separated(Parser::parse_cluster_option)?
        };

        Ok(Statement::CreateCluster(CreateClusterStatement {
            name,
            options,
        }))
    }

    fn parse_replica_option(&mut self) -> Result<ReplicaOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[AVAILABILITY, REMOTE, SIZE])? {
            AVAILABILITY => {
                self.expect_keyword(ZONE)?;
                ReplicaOptionName::AvailabilityZone
            }
            REMOTE => ReplicaOptionName::Remote,
            SIZE => ReplicaOptionName::Size,
            _ => unreachable!(),
        };
        let value = self.parse_opt_with_option_value(false)?;
        Ok(ReplicaOption { name, value })
    }

    fn parse_cluster_option(&mut self) -> Result<ClusterOption<Raw>, ParserError> {
        match self.expect_one_of_keywords(&[REPLICAS, INTROSPECTION])? {
            REPLICAS => {
                self.expect_token(&Token::LParen)?;

                let replicas = if self.peek_token() == Some(Token::RParen) {
                    vec![]
                } else {
                    self.parse_comma_separated(|parser| {
                        let name = parser.parse_identifier()?;
                        parser.expect_token(&Token::LParen)?;
                        let options = parser.parse_comma_separated(Parser::parse_replica_option)?;
                        parser.expect_token(&Token::RParen)?;
                        Ok(ReplicaDefinition { name, options })
                    })?
                };

                self.expect_token(&Token::RParen)?;
                Ok(ClusterOption::Replicas(replicas))
            }
            INTROSPECTION => match self.expect_one_of_keywords(&[DEBUGGING, GRANULARITY])? {
                DEBUGGING => {
                    let _ = self.consume_token(&Token::Eq);
                    Ok(ClusterOption::IntrospectionDebugging(
                        self.parse_with_option_value()?,
                    ))
                }
                GRANULARITY => {
                    let _ = self.consume_token(&Token::Eq);
                    Ok(ClusterOption::IntrospectionGranularity(
                        self.parse_with_option_value()?,
                    ))
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    fn parse_create_cluster_replica(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.next_token();
        let of_cluster = self.parse_identifier()?;
        self.expect_token(&Token::Dot)?;
        let name = self.parse_identifier()?;

        let options = self.parse_comma_separated(Parser::parse_replica_option)?;
        Ok(Statement::CreateClusterReplica(
            CreateClusterReplicaStatement {
                of_cluster,
                definition: ReplicaDefinition { name, options },
            },
        ))
    }

    fn parse_if_exists(&mut self) -> Result<bool, ParserError> {
        if self.parse_keyword(IF) {
            self.expect_keyword(EXISTS)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn parse_if_not_exists(&mut self) -> Result<bool, ParserError> {
        if self.parse_keyword(IF) {
            self.expect_keywords(&[NOT, EXISTS])?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn parse_source_include_metadata(&mut self) -> Result<Vec<SourceIncludeMetadata>, ParserError> {
        if self.parse_keyword(INCLUDE) {
            self.parse_comma_separated(|parser| {
                let ty = match parser
                    .expect_one_of_keywords(&[KEY, TIMESTAMP, PARTITION, TOPIC, OFFSET, HEADERS])?
                {
                    KEY => SourceIncludeMetadataType::Key,
                    TIMESTAMP => SourceIncludeMetadataType::Timestamp,
                    PARTITION => SourceIncludeMetadataType::Partition,
                    TOPIC => SourceIncludeMetadataType::Topic,
                    OFFSET => SourceIncludeMetadataType::Offset,
                    HEADERS => SourceIncludeMetadataType::Headers,
                    _ => unreachable!("only explicitly allowed items can be parsed"),
                };
                let alias = parser
                    .parse_keyword(AS)
                    .then(|| parser.parse_identifier())
                    .transpose()?;
                Ok(SourceIncludeMetadata { ty, alias })
            })
        } else {
            Ok(vec![])
        }
    }

    fn parse_discard(&mut self) -> Result<Statement<Raw>, ParserError> {
        let target = match self.expect_one_of_keywords(&[ALL, PLANS, SEQUENCES, TEMP, TEMPORARY])? {
            ALL => DiscardTarget::All,
            PLANS => DiscardTarget::Plans,
            SEQUENCES => DiscardTarget::Sequences,
            TEMP | TEMPORARY => DiscardTarget::Temp,
            _ => unreachable!(),
        };
        Ok(Statement::Discard(DiscardStatement { target }))
    }

    fn parse_drop(&mut self) -> Result<Statement<Raw>, ParserError> {
        let object_type = match self.expect_one_of_keywords(&[
            CONNECTION,
            CLUSTER,
            DATABASE,
            INDEX,
            MATERIALIZED,
            ROLE,
            SECRET,
            SCHEMA,
            SINK,
            SOURCE,
            TABLE,
            TYPE,
            USER,
            VIEW,
        ])? {
            DATABASE => {
                let if_exists = self.parse_if_exists()?;
                let name = self.parse_database_name()?;
                let restrict = matches!(
                    self.parse_at_most_one_keyword(&[CASCADE, RESTRICT], "DROP")?,
                    Some(RESTRICT),
                );
                return Ok(Statement::DropDatabase(DropDatabaseStatement {
                    if_exists,
                    name,
                    restrict,
                }));
            }
            SCHEMA => {
                let if_exists = self.parse_if_exists()?;
                let name = self.parse_schema_name()?;
                let cascade = matches!(
                    self.parse_at_most_one_keyword(&[CASCADE, RESTRICT], "DROP")?,
                    Some(CASCADE),
                );
                return Ok(Statement::DropSchema(DropSchemaStatement {
                    name,
                    if_exists,
                    cascade,
                }));
            }
            ROLE | USER => {
                let if_exists = self.parse_if_exists()?;
                let names = self.parse_comma_separated(Parser::parse_object_name)?;
                return Ok(Statement::DropRoles(DropRolesStatement {
                    if_exists,
                    names,
                }));
            }
            CLUSTER => {
                return if self.peek_keyword(REPLICA) {
                    self.parse_drop_cluster_replicas()
                } else {
                    self.parse_drop_clusters()
                };
            }
            INDEX => ObjectType::Index,
            SINK => ObjectType::Sink,
            SOURCE => ObjectType::Source,
            TABLE => ObjectType::Table,
            TYPE => ObjectType::Type,
            VIEW => ObjectType::View,
            MATERIALIZED => {
                self.expect_keyword(VIEW)?;
                ObjectType::MaterializedView
            }
            SECRET => ObjectType::Secret,
            CONNECTION => ObjectType::Connection,
            _ => unreachable!(),
        };

        let if_exists = self.parse_if_exists()?;
        let names = self.parse_comma_separated(Parser::parse_object_name)?;
        let cascade = matches!(
            self.parse_at_most_one_keyword(&[CASCADE, RESTRICT], "DROP")?,
            Some(CASCADE),
        );
        Ok(Statement::DropObjects(DropObjectsStatement {
            object_type,
            if_exists,
            names,
            cascade,
        }))
    }

    fn parse_drop_clusters(&mut self) -> Result<Statement<Raw>, ParserError> {
        let if_exists = self.parse_if_exists()?;
        let names = self.parse_comma_separated(Parser::parse_object_name)?;
        let cascade = matches!(
            self.parse_at_most_one_keyword(&[CASCADE, RESTRICT], "DROP")?,
            Some(CASCADE),
        );
        return Ok(Statement::DropClusters(DropClustersStatement {
            if_exists,
            names,
            cascade,
        }));
    }

    fn parse_drop_cluster_replicas(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(REPLICA).unwrap();
        let if_exists = self.parse_if_exists()?;
        let names = self.parse_comma_separated(|p| {
            let cluster = p.parse_identifier()?;
            p.expect_token(&Token::Dot)?;
            let replica = p.parse_identifier()?;
            Ok(QualifiedReplica { cluster, replica })
        })?;
        Ok(Statement::DropClusterReplicas(
            DropClusterReplicasStatement { if_exists, names },
        ))
    }

    fn parse_create_table(&mut self) -> Result<Statement<Raw>, ParserError> {
        let temporary = self.parse_keyword(TEMPORARY) | self.parse_keyword(TEMP);
        self.expect_keyword(TABLE)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let table_name = self.parse_object_name()?;
        // parse optional column list (schema)
        let (columns, constraints) = self.parse_columns(Mandatory)?;
        let with_options = self.parse_opt_with_options()?;

        Ok(Statement::CreateTable(CreateTableStatement {
            name: table_name,
            columns,
            constraints,
            with_options,
            if_not_exists,
            temporary,
        }))
    }

    fn parse_columns(
        &mut self,
        optional: IsOptional,
    ) -> Result<(Vec<ColumnDef<Raw>>, Vec<TableConstraint<Raw>>), ParserError> {
        let mut columns = vec![];
        let mut constraints = vec![];

        if !self.consume_token(&Token::LParen) {
            if optional == Optional {
                return Ok((columns, constraints));
            } else {
                return self.expected(
                    self.peek_pos(),
                    "a list of columns in parentheses",
                    self.peek_token(),
                );
            }
        }
        if self.consume_token(&Token::RParen) {
            // Tables with zero columns are a PostgreSQL extension.
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Some(column_name) = self.consume_identifier() {
                let data_type = self.parse_data_type()?;
                let collation = if self.parse_keyword(COLLATE) {
                    Some(self.parse_object_name()?)
                } else {
                    None
                };
                let mut options = vec![];
                loop {
                    match self.peek_token() {
                        None | Some(Token::Comma) | Some(Token::RParen) => break,
                        _ => options.push(self.parse_column_option_def()?),
                    }
                }

                columns.push(ColumnDef {
                    name: column_name,
                    data_type,
                    collation,
                    options,
                });
            } else {
                return self.expected(
                    self.peek_pos(),
                    "column name or constraint definition",
                    self.peek_token(),
                );
            }
            if self.consume_token(&Token::Comma) {
                // Continue.
            } else if self.consume_token(&Token::RParen) {
                break;
            } else {
                return self.expected(
                    self.peek_pos(),
                    "',' or ')' after column definition",
                    self.peek_token(),
                );
            }
        }

        Ok((columns, constraints))
    }

    fn parse_column_option_def(&mut self) -> Result<ColumnOptionDef<Raw>, ParserError> {
        let name = if self.parse_keyword(CONSTRAINT) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        let option = if self.parse_keywords(&[NOT, NULL]) {
            ColumnOption::NotNull
        } else if self.parse_keyword(NULL) {
            ColumnOption::Null
        } else if self.parse_keyword(DEFAULT) {
            ColumnOption::Default(self.parse_expr()?)
        } else if self.parse_keywords(&[PRIMARY, KEY]) {
            ColumnOption::Unique { is_primary: true }
        } else if self.parse_keyword(UNIQUE) {
            ColumnOption::Unique { is_primary: false }
        } else if self.parse_keyword(REFERENCES) {
            let foreign_table = self.parse_object_name()?;
            let referred_columns = self.parse_parenthesized_column_list(Mandatory)?;
            ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
            }
        } else if self.parse_keyword(CHECK) {
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            ColumnOption::Check(expr)
        } else {
            return self.expected(self.peek_pos(), "column option", self.peek_token());
        };

        Ok(ColumnOptionDef { name, option })
    }

    fn parse_optional_table_constraint(
        &mut self,
    ) -> Result<Option<TableConstraint<Raw>>, ParserError> {
        let name = if self.parse_keyword(CONSTRAINT) {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        match self.next_token() {
            Some(Token::Keyword(kw @ PRIMARY)) | Some(Token::Keyword(kw @ UNIQUE)) => {
                let is_primary = kw == PRIMARY;
                if is_primary {
                    self.expect_keyword(KEY)?;
                }
                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    columns,
                    is_primary,
                }))
            }
            Some(Token::Keyword(FOREIGN)) => {
                self.expect_keyword(KEY)?;
                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                self.expect_keyword(REFERENCES)?;
                let foreign_table = self.parse_raw_name()?;
                let referred_columns = self.parse_parenthesized_column_list(Mandatory)?;
                Ok(Some(TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                }))
            }
            Some(Token::Keyword(CHECK)) => {
                self.expect_token(&Token::LParen)?;
                let expr = Box::new(self.parse_expr()?);
                self.expect_token(&Token::RParen)?;
                Ok(Some(TableConstraint::Check { name, expr }))
            }
            unexpected => {
                if name.is_some() {
                    self.expected(
                        self.peek_prev_pos(),
                        "PRIMARY, UNIQUE, FOREIGN, or CHECK",
                        unexpected,
                    )
                } else {
                    self.prev_token();
                    Ok(None)
                }
            }
        }
    }

    fn parse_kw_options<T, F>(&mut self, f: F) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Self) -> Result<T, ParserError>,
    {
        Ok(if self.parse_keyword(WITH) {
            let expect_rparen = self.consume_token(&Token::LParen);
            let o = self.parse_comma_separated(f)?;
            if expect_rparen {
                self.expect_token(&Token::RParen)?;
            }
            o
        } else {
            vec![]
        })
    }

    fn parse_opt_with_options(&mut self) -> Result<Vec<WithOption<Raw>>, ParserError> {
        if self.parse_keyword(WITH) {
            self.parse_with_options(true)
        } else {
            Ok(vec![])
        }
    }

    fn parse_with_options(
        &mut self,
        require_equals: bool,
    ) -> Result<Vec<WithOption<Raw>>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let options =
            self.parse_comma_separated(|parser| parser.parse_with_option(require_equals))?;
        self.expect_token(&Token::RParen)?;
        Ok(options)
    }

    /// If require_equals is true, parse options of the form `KEY = VALUE` or just
    /// `KEY`. If require_equals is false, additionally support `KEY VALUE` (but still the others).
    fn parse_with_option(&mut self, require_equals: bool) -> Result<WithOption<Raw>, ParserError> {
        let key = self.parse_identifier()?;
        let value = self.parse_opt_with_option_value(require_equals)?;
        Ok(WithOption { key, value })
    }

    fn parse_opt_with_option_value(
        &mut self,
        require_equals: bool,
    ) -> Result<Option<WithOptionValue<Raw>>, ParserError> {
        let has_eq = self.consume_token(&Token::Eq);
        // No = was encountered and require_equals is false, so the next token
        // might be a value and might not. The only valid things that indicate
        // no value would be `)` for end-of-options , `,` for another-option, or
        // ';'/nothing for end-of-statement. Either of those means there's no
        // value, anything else means we expect a valid value.
        let has_value = !matches!(
            self.peek_token(),
            Some(Token::RParen) | Some(Token::Comma) | Some(Token::Semicolon) | None
        );
        if has_value && !has_eq && require_equals {
            return self.expected(self.peek_pos(), Token::Eq, self.peek_token());
        }
        Ok(if has_value {
            Some(self.parse_with_option_value()?)
        } else {
            None
        })
    }

    fn parse_with_option_value(&mut self) -> Result<WithOptionValue<Raw>, ParserError> {
        if self.parse_keyword(SECRET) {
            // HACK(benesch): temporarily allow secret references of the form
            // `KEY = SECRET db.schema.item`. `KEY = SECRET` is still allowed
            // for backwards copmatibility and parses as the ident `secret`.
            // Once we have connections with explicit fields for secret
            // references, we can remove this hack.
            if let Some(secret) = self.maybe_parse(Parser::parse_raw_name) {
                Ok(WithOptionValue::Secret(secret))
            } else {
                Ok(WithOptionValue::Ident(Ident::new("secret")))
            }
        } else if let Some(value) = self.maybe_parse(Parser::parse_value) {
            Ok(WithOptionValue::Value(value))
        } else if let Some(ident) = self.maybe_parse(Parser::parse_identifier) {
            Ok(WithOptionValue::Ident(ident))
        } else {
            return self.expected(self.peek_pos(), "option value", self.peek_token());
        }
    }

    fn parse_alter(&mut self) -> Result<Statement<Raw>, ParserError> {
        let object_type = match self.expect_one_of_keywords(&[
            SINK,
            SOURCE,
            VIEW,
            MATERIALIZED,
            TABLE,
            INDEX,
            SECRET,
        ])? {
            SINK => ObjectType::Sink,
            SOURCE => ObjectType::Source,
            VIEW => ObjectType::View,
            MATERIALIZED => {
                self.expect_keyword(VIEW)?;
                ObjectType::MaterializedView
            }
            TABLE => ObjectType::Table,
            INDEX => return self.parse_alter_index(),
            SECRET => return self.parse_alter_secret(),
            _ => unreachable!(),
        };

        let if_exists = self.parse_if_exists()?;
        let name = self.parse_object_name()?;

        self.expect_keywords(&[RENAME, TO])?;
        let to_item_name = self.parse_identifier()?;

        Ok(Statement::AlterObjectRename(AlterObjectRenameStatement {
            object_type,
            if_exists,
            name,
            to_item_name,
        }))
    }

    fn parse_alter_index(&mut self) -> Result<Statement<Raw>, ParserError> {
        let if_exists = self.parse_if_exists()?;
        let name = self.parse_object_name()?;

        Ok(match self.expect_one_of_keywords(&[RESET, SET, RENAME])? {
            RESET => {
                self.expect_token(&Token::LParen)?;
                let reset_options = self.parse_comma_separated(Parser::parse_index_option_name)?;
                self.expect_token(&Token::RParen)?;

                Statement::AlterIndex(AlterIndexStatement {
                    index_name: name,
                    if_exists,
                    action: AlterIndexAction::ResetOptions(reset_options),
                })
            }
            SET => {
                self.expect_token(&Token::LParen)?;
                let set_options = self.parse_comma_separated(Parser::parse_index_option)?;
                self.expect_token(&Token::RParen)?;
                Statement::AlterIndex(AlterIndexStatement {
                    index_name: name,
                    if_exists,
                    action: AlterIndexAction::SetOptions(set_options),
                })
            }
            RENAME => {
                self.expect_keyword(TO)?;
                let to_item_name = self.parse_identifier()?;

                Statement::AlterObjectRename(AlterObjectRenameStatement {
                    object_type: ObjectType::Index,
                    if_exists,
                    name,
                    to_item_name,
                })
            }
            _ => unreachable!(),
        })
    }

    fn parse_alter_secret(&mut self) -> Result<Statement<Raw>, ParserError> {
        let if_exists = self.parse_if_exists()?;
        let name = self.parse_object_name()?;

        Ok(match self.expect_one_of_keywords(&[AS, RENAME])? {
            AS => {
                let value = self.parse_expr()?;
                Statement::AlterSecret(AlterSecretStatement {
                    name,
                    if_exists,
                    value,
                })
            }
            RENAME => {
                self.expect_keyword(TO)?;
                let to_item_name = self.parse_identifier()?;

                Statement::AlterObjectRename(AlterObjectRenameStatement {
                    object_type: ObjectType::Secret,
                    if_exists,
                    name,
                    to_item_name,
                })
            }
            _ => unreachable!(),
        })
    }

    /// Parse a copy statement
    fn parse_copy(&mut self) -> Result<Statement<Raw>, ParserError> {
        let relation = if self.consume_token(&Token::LParen) {
            let query = self.parse_statement()?;
            self.expect_token(&Token::RParen)?;
            match query {
                Statement::Select(stmt) => CopyRelation::Select(stmt),
                Statement::Tail(stmt) => CopyRelation::Tail(stmt),
                _ => return parser_err!(self, self.peek_prev_pos(), "unsupported query in COPY"),
            }
        } else {
            let name = self.parse_raw_name()?;
            let columns = self.parse_parenthesized_column_list(Optional)?;
            CopyRelation::Table { name, columns }
        };
        let (direction, target) = match self.expect_one_of_keywords(&[FROM, TO])? {
            FROM => {
                if let CopyRelation::Table { .. } = relation {
                    // Ok.
                } else {
                    return parser_err!(
                        self,
                        self.peek_prev_pos(),
                        "queries not allowed in COPY FROM"
                    );
                }
                self.expect_keyword(STDIN)?;
                (CopyDirection::From, CopyTarget::Stdin)
            }
            TO => {
                self.expect_keyword(STDOUT)?;
                (CopyDirection::To, CopyTarget::Stdout)
            }
            _ => unreachable!(),
        };
        // WITH must be followed by LParen. The WITH in COPY is optional for backward
        // compat with Postgres but is required elsewhere, which is why we don't use
        // parse_with_options here.
        let has_options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            true
        } else {
            self.consume_token(&Token::LParen)
        };
        let options = if has_options {
            let o = self.parse_comma_separated(Parser::parse_copy_options)?;
            self.expect_token(&Token::RParen)?;
            o
        } else {
            vec![]
        };
        Ok(Statement::Copy(CopyStatement {
            relation,
            direction,
            target,
            options,
        }))
    }

    fn parse_copy_options(&mut self) -> Result<CopyOption<Raw>, ParserError> {
        let name =
            match self.expect_one_of_keywords(&[FORMAT, DELIMITER, NULL, ESCAPE, QUOTE, HEADER])? {
                FORMAT => CopyOptionName::Format,
                DELIMITER => CopyOptionName::Delimiter,
                NULL => CopyOptionName::Null,
                ESCAPE => CopyOptionName::Escape,
                QUOTE => CopyOptionName::Quote,
                HEADER => CopyOptionName::Header,
                _ => unreachable!(),
            };
        let _ = self.consume_token(&Token::Eq);
        let value = self.parse_opt_with_option_value(false)?;
        Ok(CopyOption { name, value })
    }

    /// Parse a literal value (numbers, strings, date/time, booleans)
    fn parse_value(&mut self) -> Result<Value, ParserError> {
        match self.next_token() {
            Some(t) => match t {
                Token::Keyword(TRUE) => Ok(Value::Boolean(true)),
                Token::Keyword(FALSE) => Ok(Value::Boolean(false)),
                Token::Keyword(NULL) => Ok(Value::Null),
                Token::Keyword(kw) => {
                    return parser_err!(
                        self,
                        self.peek_prev_pos(),
                        format!("No value parser for keyword {}", kw)
                    );
                }
                Token::Op(ref op) if op == "-" => match self.next_token() {
                    Some(Token::Number(n)) => Ok(Value::Number(format!("-{}", n))),
                    other => self.expected(self.peek_prev_pos(), "literal int", other),
                },
                Token::Number(ref n) => Ok(Value::Number(n.to_string())),
                Token::String(ref s) => Ok(Value::String(s.to_string())),
                Token::HexString(ref s) => Ok(Value::HexString(s.to_string())),
                Token::LBracket => self.parse_value_array(),
                _ => parser_err!(
                    self,
                    self.peek_prev_pos(),
                    format!("Unsupported value: {:?}", t)
                ),
            },
            None => parser_err!(
                self,
                self.peek_prev_pos(),
                "Expecting a value, but found EOF"
            ),
        }
    }

    fn parse_boolean_value(&mut self) -> Result<bool, ParserError> {
        match self.next_token() {
            Some(t) => match t {
                Token::Keyword(TRUE) => Ok(true),
                Token::Keyword(FALSE) => Ok(false),
                _ => self.expected(self.peek_prev_pos(), "boolean value", Some(t)),
            },
            None => self.expected(self.peek_prev_pos(), "boolean value", None),
        }
    }

    fn parse_value_array(&mut self) -> Result<Value, ParserError> {
        let mut values = vec![];
        loop {
            if let Some(Token::RBracket) = self.peek_token() {
                break;
            }
            values.push(self.parse_value()?);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        self.expect_token(&Token::RBracket)?;
        Ok(Value::Array(values))
    }

    fn parse_array(&mut self) -> Result<Expr<Raw>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let subquery = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::ArraySubquery(Box::new(subquery)))
        } else {
            self.parse_sequence(Self::parse_array).map(Expr::Array)
        }
    }

    fn parse_list(&mut self) -> Result<Expr<Raw>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let subquery = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::ListSubquery(Box::new(subquery)))
        } else {
            self.parse_sequence(Self::parse_list).map(Expr::List)
        }
    }

    fn parse_sequence<F>(&mut self, mut f: F) -> Result<Vec<Expr<Raw>>, ParserError>
    where
        F: FnMut(&mut Self) -> Result<Expr<Raw>, ParserError>,
    {
        self.expect_token(&Token::LBracket)?;
        let mut exprs = vec![];
        loop {
            if let Some(Token::RBracket) = self.peek_token() {
                break;
            }
            let expr = if let Some(Token::LBracket) = self.peek_token() {
                f(self)?
            } else {
                self.parse_expr()?
            };
            exprs.push(expr);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        self.expect_token(&Token::RBracket)?;
        Ok(exprs)
    }

    fn parse_number_value(&mut self) -> Result<Value, ParserError> {
        match self.parse_value()? {
            v @ Value::Number(_) => Ok(v),
            _ => {
                self.prev_token();
                self.expected(self.peek_pos(), "literal number", self.peek_token())
            }
        }
    }

    /// Parse a signed literal integer.
    fn parse_literal_int(&mut self) -> Result<i64, ParserError> {
        match self.next_token() {
            Some(Token::Number(s)) => s.parse::<i64>().map_err(|e| {
                self.error(
                    self.peek_prev_pos(),
                    format!("Could not parse '{}' as i64: {}", s, e),
                )
            }),
            other => self.expected(self.peek_prev_pos(), "literal integer", other),
        }
    }

    /// Parse an unsigned literal integer.
    fn parse_literal_uint(&mut self) -> Result<u64, ParserError> {
        match self.next_token() {
            Some(Token::Number(s)) => s.parse::<u64>().map_err(|e| {
                self.error(
                    self.peek_prev_pos(),
                    format!("Could not parse '{}' as u64: {}", s, e),
                )
            }),
            other => self.expected(self.peek_prev_pos(), "literal unsigned integer", other),
        }
    }

    /// Parse a literal string
    fn parse_literal_string(&mut self) -> Result<String, ParserError> {
        match self.next_token() {
            Some(Token::String(ref s)) => Ok(s.clone()),
            other => self.expected(self.peek_prev_pos(), "literal string", other),
        }
    }

    /// Parse a SQL datatype (in the context of a CREATE TABLE statement for example)
    fn parse_data_type(&mut self) -> Result<RawDataType, ParserError> {
        let other = |name: &str| RawDataType::Other {
            name: RawObjectName::Name(UnresolvedObjectName::unqualified(name)),
            typ_mod: vec![],
        };

        let mut data_type = match self.next_token() {
            Some(Token::Keyword(kw)) => match kw {
                // Text-like types
                CHAR | CHARACTER => {
                    let name = if self.parse_keyword(VARYING) {
                        "varchar"
                    } else {
                        "bpchar"
                    };
                    RawDataType::Other {
                        name: RawObjectName::Name(UnresolvedObjectName::unqualified(name)),
                        typ_mod: self.parse_typ_mod()?,
                    }
                }
                BPCHAR => RawDataType::Other {
                    name: RawObjectName::Name(UnresolvedObjectName::unqualified("bpchar")),
                    typ_mod: self.parse_typ_mod()?,
                },
                VARCHAR => RawDataType::Other {
                    name: RawObjectName::Name(UnresolvedObjectName::unqualified("varchar")),
                    typ_mod: self.parse_typ_mod()?,
                },
                STRING => other("text"),

                // Number-like types
                BIGINT => other("int8"),
                SMALLINT => other("int2"),
                DEC | DECIMAL => RawDataType::Other {
                    name: RawObjectName::Name(UnresolvedObjectName::unqualified("numeric")),
                    typ_mod: self.parse_typ_mod()?,
                },
                DOUBLE => {
                    let _ = self.parse_keyword(PRECISION);
                    other("float8")
                }
                FLOAT => match self.parse_optional_precision()?.unwrap_or(53) {
                    v if v == 0 || v > 53 => {
                        return Err(self.error(
                            self.peek_prev_pos(),
                            "precision for type float must be within ([1-53])".into(),
                        ))
                    }
                    v if v < 25 => other("float4"),
                    _ => other("float8"),
                },
                INT | INTEGER => other("int4"),
                REAL => other("float4"),

                // Time-like types
                TIME => {
                    if self.parse_keyword(WITH) {
                        self.expect_keywords(&[TIME, ZONE])?;
                        other("timetz")
                    } else {
                        if self.parse_keyword(WITHOUT) {
                            self.expect_keywords(&[TIME, ZONE])?;
                        }
                        other("time")
                    }
                }
                TIMESTAMP => {
                    if self.parse_keyword(WITH) {
                        self.expect_keywords(&[TIME, ZONE])?;
                        other("timestamptz")
                    } else {
                        if self.parse_keyword(WITHOUT) {
                            self.expect_keywords(&[TIME, ZONE])?;
                        }
                        other("timestamp")
                    }
                }

                // MZ "proprietary" types
                MAP => {
                    return self.parse_map();
                }

                // Misc.
                BOOLEAN => other("bool"),
                BYTES => other("bytea"),
                JSON => other("jsonb"),
                _ => {
                    self.prev_token();
                    RawDataType::Other {
                        name: RawObjectName::Name(self.parse_object_name()?),
                        typ_mod: self.parse_typ_mod()?,
                    }
                }
            },
            Some(Token::Ident(_) | Token::LBracket) => {
                self.prev_token();
                RawDataType::Other {
                    name: self.parse_raw_name()?,
                    typ_mod: self.parse_typ_mod()?,
                }
            }
            other => self.expected(self.peek_prev_pos(), "a data type name", other)?,
        };

        loop {
            match self.peek_token() {
                Some(Token::Keyword(LIST)) => {
                    self.next_token();
                    data_type = RawDataType::List(Box::new(data_type));
                }
                Some(Token::LBracket) => {
                    // Handle array suffixes. Note that `int[]`, `int[][][]`,
                    // and `int[2][2]` all parse to the same "int array" type.
                    self.next_token();
                    let _ = self.maybe_parse(|parser| parser.parse_number_value());
                    self.expect_token(&Token::RBracket)?;
                    if !matches!(data_type, RawDataType::Array(_)) {
                        data_type = RawDataType::Array(Box::new(data_type));
                    }
                }
                _ => break,
            }
        }
        Ok(data_type)
    }

    fn parse_typ_mod(&mut self) -> Result<Vec<i64>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let typ_mod = self.parse_comma_separated(Parser::parse_literal_int)?;
            self.expect_token(&Token::RParen)?;
            Ok(typ_mod)
        } else {
            Ok(vec![])
        }
    }

    /// Parse `AS identifier` (or simply `identifier` if it's not a reserved keyword)
    /// Some examples with aliases: `SELECT 1 foo`, `SELECT COUNT(*) AS cnt`,
    /// `SELECT ... FROM t1 foo, t2 bar`, `SELECT ... FROM (...) AS bar`
    fn parse_optional_alias<F>(&mut self, is_reserved: F) -> Result<Option<Ident>, ParserError>
    where
        F: FnOnce(Keyword) -> bool,
    {
        let after_as = self.parse_keyword(AS);
        match self.next_token() {
            // Do not accept `AS OF`, which is reserved for providing timestamp information
            // to queries.
            Some(Token::Keyword(OF)) => {
                self.prev_token();
                self.prev_token();
                Ok(None)
            }
            // Accept any other identifier after `AS` (though many dialects have restrictions on
            // keywords that may appear here). If there's no `AS`: don't parse keywords,
            // which may start a construct allowed in this position, to be parsed as aliases.
            // (For example, in `FROM t1 JOIN` the `JOIN` will always be parsed as a keyword,
            // not an alias.)
            Some(Token::Keyword(kw)) if after_as || !is_reserved(kw) => Ok(Some(kw.into_ident())),
            Some(Token::Ident(id)) => Ok(Some(Ident::new(id))),
            not_an_ident => {
                if after_as {
                    return self.expected(
                        self.peek_prev_pos(),
                        "an identifier after AS",
                        not_an_ident,
                    );
                }
                self.prev_token();
                Ok(None) // no alias found
            }
        }
    }

    /// Parse `AS identifier` when the AS is describing a table-valued object,
    /// like in `... FROM generate_series(1, 10) AS t (col)`. In this case
    /// the alias is allowed to optionally name the columns in the table, in
    /// addition to the table itself.
    fn parse_optional_table_alias(&mut self) -> Result<Option<TableAlias>, ParserError> {
        match self.parse_optional_alias(Keyword::is_reserved_in_table_alias)? {
            Some(name) => {
                let columns = self.parse_parenthesized_column_list(Optional)?;
                Ok(Some(TableAlias {
                    name,
                    columns,
                    strict: false,
                }))
            }
            None => Ok(None),
        }
    }

    fn parse_raw_name(&mut self) -> Result<RawObjectName, ParserError> {
        if self.consume_token(&Token::LBracket) {
            let id = match self.next_token() {
                Some(Token::Ident(id)) => id,
                _ => return parser_err!(self, self.peek_prev_pos(), "expected id"),
            };
            self.expect_keyword(AS)?;
            let name = self.parse_object_name()?;
            // TODO(justin): is there a more idiomatic way to detect a fully-qualified name?
            if name.0.len() < 2 {
                return parser_err!(
                    self,
                    self.peek_prev_pos(),
                    "table name in square brackets must be fully qualified"
                );
            }
            self.expect_token(&Token::RBracket)?;
            Ok(RawObjectName::Id(id, name))
        } else {
            Ok(RawObjectName::Name(self.parse_object_name()?))
        }
    }

    /// Parse a possibly quoted database identifier, e.g.
    /// `foo` or `"mydatabase"`
    fn parse_database_name(&mut self) -> Result<UnresolvedDatabaseName, ParserError> {
        Ok(UnresolvedDatabaseName(self.parse_identifier()?))
    }

    /// Parse a possibly qualified, possibly quoted schema identifier, e.g.
    /// `foo` or `mydatabase."schema"`
    fn parse_schema_name(&mut self) -> Result<UnresolvedSchemaName, ParserError> {
        Ok(UnresolvedSchemaName(self.parse_identifiers()?))
    }

    /// Parse a possibly qualified, possibly quoted object identifier, e.g.
    /// `foo` or `myschema."table"`
    fn parse_object_name(&mut self) -> Result<UnresolvedObjectName, ParserError> {
        Ok(UnresolvedObjectName(self.parse_identifiers()?))
    }

    ///Parse one or more simple one-word identifiers separated by a '.'
    fn parse_identifiers(&mut self) -> Result<Vec<Ident>, ParserError> {
        let mut idents = vec![];
        loop {
            idents.push(self.parse_identifier()?);
            if !self.consume_token(&Token::Dot) {
                break;
            }
        }
        Ok(idents)
    }

    /// Parse a simple one-word identifier (possibly quoted, possibly a keyword)
    fn parse_identifier(&mut self) -> Result<Ident, ParserError> {
        match self.consume_identifier() {
            Some(id) => {
                if id.as_str().is_empty() {
                    return parser_err!(
                        self,
                        self.peek_prev_pos(),
                        "zero-length delimited identifier",
                    );
                }
                Ok(id)
            }
            None => self.expected(self.peek_pos(), "identifier", self.peek_token()),
        }
    }

    fn consume_identifier(&mut self) -> Option<Ident> {
        match self.peek_token() {
            Some(Token::Keyword(kw)) => {
                self.next_token();
                Some(kw.into_ident())
            }
            Some(Token::Ident(id)) => {
                self.next_token();
                Some(Ident::new(id))
            }
            _ => None,
        }
    }

    fn parse_qualified_identifier(&mut self, id: Ident) -> Result<Expr<Raw>, ParserError> {
        let mut id_parts = vec![id];
        match self.peek_token() {
            Some(Token::LParen) | Some(Token::Dot) => {
                let mut ends_with_wildcard = false;
                while self.consume_token(&Token::Dot) {
                    match self.next_token() {
                        Some(Token::Keyword(kw)) => id_parts.push(kw.into_ident()),
                        Some(Token::Ident(id)) => id_parts.push(Ident::new(id)),
                        Some(Token::Star) => {
                            ends_with_wildcard = true;
                            break;
                        }
                        unexpected => {
                            return self.expected(
                                self.peek_prev_pos(),
                                "an identifier or a '*' after '.'",
                                unexpected,
                            );
                        }
                    }
                }
                if ends_with_wildcard {
                    Ok(Expr::QualifiedWildcard(id_parts))
                } else if self.consume_token(&Token::LParen) {
                    self.prev_token();
                    self.parse_function(UnresolvedObjectName(id_parts))
                } else {
                    Ok(Expr::Identifier(id_parts))
                }
            }
            _ => Ok(Expr::Identifier(id_parts)),
        }
    }

    /// Parse a parenthesized comma-separated list of unqualified, possibly quoted identifiers
    fn parse_parenthesized_column_list(
        &mut self,
        optional: IsOptional,
    ) -> Result<Vec<Ident>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let cols = self.parse_comma_separated(Parser::parse_identifier)?;
            self.expect_token(&Token::RParen)?;
            Ok(cols)
        } else if optional == Optional {
            Ok(vec![])
        } else {
            self.expected(
                self.peek_pos(),
                "a list of columns in parentheses",
                self.peek_token(),
            )
        }
    }

    fn parse_optional_precision(&mut self) -> Result<Option<u64>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let n = self.parse_literal_uint()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(n))
        } else {
            Ok(None)
        }
    }

    fn parse_map(&mut self) -> Result<RawDataType, ParserError> {
        self.expect_token(&Token::LBracket)?;
        let key_type = Box::new(self.parse_data_type()?);
        self.expect_token(&Token::Op("=>".to_owned()))?;
        let value_type = Box::new(self.parse_data_type()?);
        self.expect_token(&Token::RBracket)?;
        Ok(RawDataType::Map {
            key_type,
            value_type,
        })
    }

    fn parse_delete(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(FROM)?;
        let table_name = RawObjectName::Name(self.parse_object_name()?);
        let alias = self.parse_optional_table_alias()?;
        let using = if self.parse_keyword(USING) {
            self.parse_comma_separated(Parser::parse_table_and_joins)?
        } else {
            vec![]
        };
        let selection = if self.parse_keyword(WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Delete(DeleteStatement {
            table_name,
            alias,
            using,
            selection,
        }))
    }

    /// Parse a query expression, i.e. a `SELECT` statement optionally
    /// preceeded with some `WITH` CTE declarations and optionally followed
    /// by `ORDER BY`. Unlike some other parse_... methods, this one doesn't
    /// expect the initial keyword to be already consumed
    fn parse_query(&mut self) -> Result<Query<Raw>, ParserError> {
        self.checked_recur_mut(|parser| {
            let ctes = if parser.parse_keyword(WITH) {
                // TODO: optional RECURSIVE
                parser.parse_comma_separated(Parser::parse_cte)?
            } else {
                vec![]
            };

            let body = parser.parse_query_body(SetPrecedence::Zero)?;

            parser.parse_query_tail(ctes, body)
        })
    }

    fn parse_query_tail(
        &mut self,
        ctes: Vec<Cte<Raw>>,
        body: SetExpr<Raw>,
    ) -> Result<Query<Raw>, ParserError> {
        let (inner_ctes, inner_order_by, inner_limit, inner_offset, body) = match body {
            SetExpr::Query(query) => {
                let Query {
                    ctes,
                    body,
                    order_by,
                    limit,
                    offset,
                } = *query;
                (ctes, order_by, limit, offset, body)
            }
            _ => (vec![], vec![], None, None, body),
        };

        let ctes = if ctes.is_empty() {
            inner_ctes
        } else if !inner_ctes.is_empty() {
            return parser_err!(self, self.peek_pos(), "multiple WITH clauses not allowed");
        } else {
            ctes
        };

        let order_by = if self.parse_keywords(&[ORDER, BY]) {
            if !inner_order_by.is_empty() {
                return parser_err!(
                    self,
                    self.peek_prev_pos(),
                    "multiple ORDER BY clauses not allowed"
                );
            }
            self.parse_comma_separated(Parser::parse_order_by_expr)?
        } else {
            inner_order_by
        };

        let mut limit = if self.parse_keyword(LIMIT) {
            if inner_limit.is_some() {
                return parser_err!(
                    self,
                    self.peek_prev_pos(),
                    "multiple LIMIT/FETCH clauses not allowed"
                );
            }
            if self.parse_keyword(ALL) {
                None
            } else {
                Some(Limit {
                    with_ties: false,
                    quantity: self.parse_expr()?,
                })
            }
        } else {
            inner_limit
        };

        let offset = if self.parse_keyword(OFFSET) {
            if inner_offset.is_some() {
                return parser_err!(
                    self,
                    self.peek_prev_pos(),
                    "multiple OFFSET clauses not allowed"
                );
            }
            let value = self.parse_expr()?;
            let _ = self.parse_one_of_keywords(&[ROW, ROWS]);
            Some(value)
        } else {
            inner_offset
        };

        if limit.is_none() && self.parse_keyword(FETCH) {
            self.expect_one_of_keywords(&[FIRST, NEXT])?;
            let quantity = if self.parse_one_of_keywords(&[ROW, ROWS]).is_some() {
                Expr::Value(Value::Number('1'.into()))
            } else {
                let quantity = self.parse_expr()?;
                self.expect_one_of_keywords(&[ROW, ROWS])?;
                quantity
            };
            let with_ties = if self.parse_keyword(ONLY) {
                false
            } else if self.parse_keywords(&[WITH, TIES]) {
                true
            } else {
                return self.expected(
                    self.peek_pos(),
                    "one of ONLY or WITH TIES",
                    self.peek_token(),
                );
            };
            limit = Some(Limit {
                with_ties,
                quantity,
            });
        }

        Ok(Query {
            ctes,
            body,
            order_by,
            limit,
            offset,
        })
    }

    /// Parse a CTE (`alias [( col1, col2, ... )] AS (subquery)`)
    fn parse_cte(&mut self) -> Result<Cte<Raw>, ParserError> {
        let alias = TableAlias {
            name: self.parse_identifier()?,
            columns: self.parse_parenthesized_column_list(Optional)?,
            strict: false,
        };
        self.expect_keyword(AS)?;
        self.expect_token(&Token::LParen)?;
        let query = self.parse_query()?;
        self.expect_token(&Token::RParen)?;
        Ok(Cte {
            alias,
            query,
            id: (),
        })
    }

    /// Parse a "query body", which is an expression with roughly the
    /// following grammar:
    /// ```text
    ///   query_body ::= restricted_select | '(' subquery ')' | set_operation
    ///   restricted_select ::= 'SELECT' [expr_list] [ from ] [ where ] [ groupby_having ]
    ///   subquery ::= query_body [ order_by_limit ]
    ///   set_operation ::= query_body { 'UNION' | 'EXCEPT' | 'INTERSECT' } [ 'ALL' ] query_body
    /// ```
    fn parse_query_body(&mut self, precedence: SetPrecedence) -> Result<SetExpr<Raw>, ParserError> {
        // We parse the expression using a Pratt parser, as in `parse_expr()`.
        // Start by parsing a restricted SELECT or a `(subquery)`:
        let expr = if self.parse_keyword(SELECT) {
            SetExpr::Select(Box::new(self.parse_select()?))
        } else if self.consume_token(&Token::LParen) {
            // CTEs are not allowed here, but the parser currently accepts them
            let subquery = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            SetExpr::Query(Box::new(subquery))
        } else if self.parse_keyword(VALUES) {
            SetExpr::Values(self.parse_values()?)
        } else {
            return self.expected(
                self.peek_pos(),
                "SELECT, VALUES, or a subquery in the query body",
                self.peek_token(),
            );
        };

        self.parse_query_body_seeded(precedence, expr)
    }

    fn parse_query_body_seeded(
        &mut self,
        precedence: SetPrecedence,
        mut expr: SetExpr<Raw>,
    ) -> Result<SetExpr<Raw>, ParserError> {
        loop {
            // The query can be optionally followed by a set operator:
            let next_token = self.peek_token();
            let op = self.parse_set_operator(&next_token);
            let next_precedence = match op {
                // UNION and EXCEPT have the same binding power and evaluate left-to-right
                Some(SetOperator::Union) | Some(SetOperator::Except) => SetPrecedence::UnionExcept,
                // INTERSECT has higher precedence than UNION/EXCEPT
                Some(SetOperator::Intersect) => SetPrecedence::Intersect,
                // Unexpected token or EOF => stop parsing the query body
                None => break,
            };
            if precedence >= next_precedence {
                break;
            }
            self.next_token(); // skip past the set operator

            let all = self.parse_keyword(ALL);
            let distinct = self.parse_keyword(DISTINCT);
            if all && distinct {
                return parser_err!(
                    self,
                    self.peek_prev_pos(),
                    "Cannot specify both ALL and DISTINCT in set operation"
                );
            }
            expr = SetExpr::SetOperation {
                left: Box::new(expr),
                op: op.unwrap(),
                all,
                right: Box::new(self.parse_query_body(next_precedence)?),
            };
        }

        Ok(expr)
    }

    fn parse_set_operator(&mut self, token: &Option<Token>) -> Option<SetOperator> {
        match token {
            Some(Token::Keyword(UNION)) => Some(SetOperator::Union),
            Some(Token::Keyword(EXCEPT)) => Some(SetOperator::Except),
            Some(Token::Keyword(INTERSECT)) => Some(SetOperator::Intersect),
            _ => None,
        }
    }

    /// Parse a restricted `SELECT` statement (no CTEs / `UNION` / `ORDER BY`),
    /// assuming the initial `SELECT` was already consumed
    fn parse_select(&mut self) -> Result<Select<Raw>, ParserError> {
        let all = self.parse_keyword(ALL);
        let distinct = self.parse_keyword(DISTINCT);
        if all && distinct {
            return parser_err!(
                self,
                self.peek_prev_pos(),
                "Cannot specify both ALL and DISTINCT in SELECT"
            );
        }
        let distinct = if distinct && self.parse_keyword(ON) {
            self.expect_token(&Token::LParen)?;
            let exprs = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
            Some(Distinct::On(exprs))
        } else if distinct {
            Some(Distinct::EntireRow)
        } else {
            None
        };

        let projection = match self.peek_token() {
            // An empty target list is permissible to match PostgreSQL, which
            // permits these for symmetry with zero column tables.
            Some(Token::Keyword(kw)) if kw.is_reserved() => vec![],
            Some(Token::Semicolon) | Some(Token::RParen) | None => vec![],
            _ => self.parse_comma_separated(Parser::parse_select_item)?,
        };

        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_COLUMN_ALIAS` / `RESERVED_FOR_TABLE_ALIAS`,
        // otherwise they may be parsed as an alias as part of the `projection`
        // or `from`.

        let from = if self.parse_keyword(FROM) {
            self.parse_comma_separated(Parser::parse_table_and_joins)?
        } else {
            vec![]
        };

        let selection = if self.parse_keyword(WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.parse_keywords(&[GROUP, BY]) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![]
        };

        let having = if self.parse_keyword(HAVING) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let options = if self.parse_keyword(OPTION) {
            self.parse_with_options(true)?
        } else {
            vec![]
        };

        Ok(Select {
            distinct,
            projection,
            from,
            selection,
            group_by,
            having,
            options,
        })
    }

    fn parse_set(&mut self) -> Result<Statement<Raw>, ParserError> {
        let modifier = self.parse_one_of_keywords(&[SESSION, LOCAL]);
        let mut variable = self.parse_identifier()?;
        let mut normal = self.consume_token(&Token::Eq) || self.parse_keyword(TO);
        if !normal {
            match variable.as_str().parse() {
                Ok(TIME) => {
                    self.expect_keyword(ZONE)?;
                    variable = Ident::new("timezone");
                    normal = true;
                }
                Ok(NAMES) => {
                    variable = Ident::new("client_encoding");
                    normal = true;
                }
                Ok(SCHEMA) => {
                    variable = Ident::new("search_path");
                    normal = true;
                }
                _ => {}
            }
        }
        if normal {
            let token = self.peek_token();
            let value = match (self.parse_value(), token) {
                (Ok(value), _) => SetVariableValue::Literal(value),
                (Err(_), Some(Token::Keyword(DEFAULT))) => SetVariableValue::Default,
                (Err(_), Some(Token::Keyword(kw))) => SetVariableValue::Ident(kw.into_ident()),
                (Err(_), Some(Token::Ident(id))) => SetVariableValue::Ident(Ident::new(id)),
                (Err(_), other) => self.expected(self.peek_pos(), "variable value", other)?,
            };
            Ok(Statement::SetVariable(SetVariableStatement {
                local: modifier == Some(LOCAL),
                variable,
                value,
            }))
        } else if
        // SET TRANSACTION transaction_mode
        (variable.as_str().parse() == Ok(TRANSACTION) && modifier.is_none())
            ||
            // SET SESSION CHARACTERISTICS AS TRANSACTION transaction_mode
            (modifier == Some(SESSION)
                && variable.as_str().parse() == Ok(CHARACTERISTICS)
                && self.parse_keywords(&[AS, TRANSACTION]))
        {
            Ok(Statement::SetTransaction(SetTransactionStatement {
                modes: self.parse_transaction_modes()?,
            }))
        } else {
            self.expected(self.peek_pos(), "equals sign or TO", self.peek_token())
        }
    }

    fn parse_reset(&mut self) -> Result<Statement<Raw>, ParserError> {
        let variable = self.parse_identifier()?;
        Ok(Statement::ResetVariable(ResetVariableStatement {
            variable,
        }))
    }

    fn parse_show(&mut self) -> Result<Statement<Raw>, ParserError> {
        if self.parse_keyword(DATABASES) {
            return Ok(Statement::ShowDatabases(ShowDatabasesStatement {
                filter: self.parse_show_statement_filter()?,
            }));
        }

        let extended = self.parse_keyword(EXTENDED);
        if extended {
            self.expect_one_of_keywords(&[
                COLUMNS,
                CONNECTIONS,
                FULL,
                INDEX,
                INDEXES,
                KEYS,
                OBJECTS,
                SCHEMAS,
                TABLES,
                TYPES,
            ])?;
            self.prev_token();
        }

        let full = self.parse_keyword(FULL);
        if full {
            if extended {
                self.expect_one_of_keywords(&[COLUMNS, OBJECTS, SCHEMAS, TABLES, TYPES])?;
            } else {
                let kw = self.expect_one_of_keywords(&[
                    COLUMNS,
                    CONNECTIONS,
                    MATERIALIZED,
                    OBJECTS,
                    ROLES,
                    SCHEMAS,
                    SINKS,
                    SOURCES,
                    TABLES,
                    TYPES,
                    VIEWS,
                ])?;
                if kw == MATERIALIZED {
                    self.expect_keyword(VIEWS)?;
                    self.prev_token();
                }
            }
            self.prev_token();
        }

        if self.parse_one_of_keywords(&[COLUMNS, FIELDS]).is_some() {
            self.parse_show_columns(extended, full)
        } else if self.parse_keyword(SCHEMAS) {
            let from = if self.parse_keyword(FROM) {
                Some(self.parse_database_name()?)
            } else {
                None
            };
            Ok(Statement::ShowSchemas(ShowSchemasStatement {
                from,
                extended,
                full,
                filter: self.parse_show_statement_filter()?,
            }))
        } else if let Some(object_type) = self.parse_one_of_keywords(&[
            OBJECTS,
            ROLES,
            CLUSTER,
            CLUSTERS,
            SINKS,
            SOURCES,
            TABLES,
            TYPES,
            USERS,
            VIEWS,
            MATERIALIZED,
            SECRETS,
            CONNECTIONS,
        ]) {
            let object_type = match object_type {
                OBJECTS => ObjectType::Object,
                ROLES | USERS => ObjectType::Role,
                CLUSTER => {
                    if self.parse_keyword(REPLICAS) {
                        ObjectType::ClusterReplica
                    } else {
                        return Ok(Statement::ShowVariable(ShowVariableStatement {
                            variable: Ident::from("cluster"),
                        }));
                    }
                }
                CLUSTERS => ObjectType::Cluster,
                SINKS => ObjectType::Sink,
                SOURCES => ObjectType::Source,
                TABLES => ObjectType::Table,
                TYPES => ObjectType::Type,
                VIEWS => ObjectType::View,
                MATERIALIZED => {
                    self.expect_keyword(VIEWS)?;
                    ObjectType::MaterializedView
                }
                SECRETS => ObjectType::Secret,
                CONNECTIONS => ObjectType::Connection,
                _ => unreachable!(),
            };

            let (from, in_cluster) = match self.parse_one_of_keywords(&[FROM, IN]) {
                Some(kw) => {
                    if kw == IN && self.peek_keyword(CLUSTER) {
                        if matches!(object_type, ObjectType::Sink | ObjectType::MaterializedView) {
                            // put `IN` back
                            self.prev_token();
                            (None, self.parse_optional_in_cluster()?)
                        } else {
                            return parser_err!(
                                self,
                                self.peek_pos(),
                                "expected object name, found 'cluster'"
                            );
                        }
                    } else {
                        let from = self.parse_schema_name()?;
                        let in_cluster = self.parse_optional_in_cluster()?;
                        (Some(from), in_cluster)
                    }
                }
                None => (None, None),
            };

            Ok(Statement::ShowObjects(ShowObjectsStatement {
                object_type,
                extended,
                full,
                from,
                in_cluster,
                filter: self.parse_show_statement_filter()?,
            }))
        } else if self
            .parse_one_of_keywords(&[INDEX, INDEXES, KEYS])
            .is_some()
        {
            let kw = self.parse_one_of_keywords(&[FROM, IN, ON]);
            let (table_name, in_cluster) = if kw == Some(IN) && self.peek_keyword(CLUSTER) {
                // put `IN` back
                self.prev_token();
                (None, self.parse_optional_in_cluster()?)
            } else if kw.is_some() {
                let table_name = self.parse_raw_name()?;
                let in_cluster = self.parse_optional_in_cluster()?;
                (Some(table_name), in_cluster)
            } else {
                (None, None)
            };

            let filter = if self.parse_keyword(WHERE) {
                Some(ShowStatementFilter::Where(self.parse_expr()?))
            } else {
                None
            };
            Ok(Statement::ShowIndexes(ShowIndexesStatement {
                table_name,
                in_cluster,
                extended,
                filter,
            }))
        } else if self.parse_keywords(&[CREATE, VIEW]) {
            Ok(Statement::ShowCreateView(ShowCreateViewStatement {
                view_name: self.parse_raw_name()?,
            }))
        } else if self.parse_keywords(&[CREATE, MATERIALIZED, VIEW]) {
            Ok(Statement::ShowCreateMaterializedView(
                ShowCreateMaterializedViewStatement {
                    materialized_view_name: self.parse_raw_name()?,
                },
            ))
        } else if self.parse_keywords(&[CREATE, SOURCE]) {
            Ok(Statement::ShowCreateSource(ShowCreateSourceStatement {
                source_name: self.parse_raw_name()?,
            }))
        } else if self.parse_keywords(&[CREATE, TABLE]) {
            Ok(Statement::ShowCreateTable(ShowCreateTableStatement {
                table_name: self.parse_raw_name()?,
            }))
        } else if self.parse_keywords(&[CREATE, SINK]) {
            Ok(Statement::ShowCreateSink(ShowCreateSinkStatement {
                sink_name: self.parse_raw_name()?,
            }))
        } else if self.parse_keywords(&[CREATE, INDEX]) {
            Ok(Statement::ShowCreateIndex(ShowCreateIndexStatement {
                index_name: self.parse_raw_name()?,
            }))
        } else if self.parse_keywords(&[CREATE, CONNECTION]) {
            Ok(Statement::ShowCreateConnection(
                ShowCreateConnectionStatement {
                    connection_name: self.parse_raw_name()?,
                },
            ))
        } else {
            let variable = if self.parse_keywords(&[TRANSACTION, ISOLATION, LEVEL]) {
                Ident::new("transaction_isolation")
            } else if self.parse_keywords(&[TIME, ZONE]) {
                Ident::new("timezone")
            } else {
                self.parse_identifier()?
            };
            Ok(Statement::ShowVariable(ShowVariableStatement { variable }))
        }
    }

    fn parse_show_columns(
        &mut self,
        extended: bool,
        full: bool,
    ) -> Result<Statement<Raw>, ParserError> {
        self.expect_one_of_keywords(&[FROM, IN])?;
        let table_name = self.parse_raw_name()?;
        // MySQL also supports FROM <database> here. In other words, MySQL
        // allows both FROM <table> FROM <database> and FROM <database>.<table>,
        // while we only support the latter for now.
        let filter = self.parse_show_statement_filter()?;
        Ok(Statement::ShowColumns(ShowColumnsStatement {
            extended,
            full,
            table_name,
            filter,
        }))
    }

    fn parse_show_statement_filter(
        &mut self,
    ) -> Result<Option<ShowStatementFilter<Raw>>, ParserError> {
        if self.parse_keyword(LIKE) {
            Ok(Some(ShowStatementFilter::Like(
                self.parse_literal_string()?,
            )))
        } else if self.parse_keyword(WHERE) {
            Ok(Some(ShowStatementFilter::Where(self.parse_expr()?)))
        } else {
            Ok(None)
        }
    }

    fn parse_table_and_joins(&mut self) -> Result<TableWithJoins<Raw>, ParserError> {
        let relation = self.parse_table_factor()?;

        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_TABLE_ALIAS`, otherwise they may be parsed as
        // a table alias.
        let mut joins = vec![];
        loop {
            let join = if self.parse_keyword(CROSS) {
                self.expect_keyword(JOIN)?;
                Join {
                    relation: self.parse_table_factor()?,
                    join_operator: JoinOperator::CrossJoin,
                }
            } else {
                let natural = self.parse_keyword(NATURAL);
                let peek_keyword = if let Some(Token::Keyword(kw)) = self.peek_token() {
                    Some(kw)
                } else {
                    None
                };

                let join_operator_type = match peek_keyword {
                    Some(INNER) | Some(JOIN) => {
                        let _ = self.parse_keyword(INNER);
                        self.expect_keyword(JOIN)?;
                        JoinOperator::Inner
                    }
                    Some(kw @ LEFT) | Some(kw @ RIGHT) | Some(kw @ FULL) => {
                        let _ = self.next_token();
                        let _ = self.parse_keyword(OUTER);
                        self.expect_keyword(JOIN)?;
                        match kw {
                            LEFT => JoinOperator::LeftOuter,
                            RIGHT => JoinOperator::RightOuter,
                            FULL => JoinOperator::FullOuter,
                            _ => unreachable!(),
                        }
                    }
                    Some(OUTER) => {
                        return self.expected(
                            self.peek_pos(),
                            "LEFT, RIGHT, or FULL",
                            self.peek_token(),
                        )
                    }
                    None if natural => {
                        return self.expected(
                            self.peek_pos(),
                            "a join type after NATURAL",
                            self.peek_token(),
                        );
                    }
                    _ => break,
                };
                let relation = self.parse_table_factor()?;
                let join_constraint = self.parse_join_constraint(natural)?;
                Join {
                    relation,
                    join_operator: join_operator_type(join_constraint),
                }
            };
            joins.push(join);
        }
        Ok(TableWithJoins { relation, joins })
    }

    /// A table name or a parenthesized subquery, followed by optional `[AS] alias`
    fn parse_table_factor(&mut self) -> Result<TableFactor<Raw>, ParserError> {
        if self.parse_keyword(LATERAL) {
            // LATERAL must always be followed by a subquery or table function.
            if self.consume_token(&Token::LParen) {
                return self.parse_derived_table_factor(Lateral);
            } else if self.parse_keywords(&[ROWS, FROM]) {
                return Ok(self.parse_rows_from()?);
            } else {
                let name = self.parse_object_name()?;
                self.expect_token(&Token::LParen)?;
                let args = self.parse_optional_args(false)?;
                let alias = self.parse_optional_table_alias()?;
                let with_ordinality = self.parse_keywords(&[WITH, ORDINALITY]);
                return Ok(TableFactor::Function {
                    function: TableFunction { name, args },
                    alias,
                    with_ordinality,
                });
            }
        }

        if self.consume_token(&Token::LParen) {
            // A left paren introduces either a derived table (i.e., a subquery)
            // or a nested join. It's nearly impossible to determine ahead of
            // time which it is... so we just try to parse both.
            //
            // Here's an example that demonstrates the complexity:
            //                     /-------------------------------------------------------\
            //                     | /-----------------------------------\                 |
            //     SELECT * FROM ( ( ( (SELECT 1) UNION (SELECT 2) ) AS t1 NATURAL JOIN t2 ) )
            //                   ^ ^ ^ ^
            //                   | | | |
            //                   | | | |
            //                   | | | (4) belongs to a SetExpr::Query inside the subquery
            //                   | | (3) starts a derived table (subquery)
            //                   | (2) starts a nested join
            //                   (1) an additional set of parens around a nested join
            //

            // Check if the recently consumed '(' started a derived table, in
            // which case we've parsed the subquery, followed by the closing
            // ')', and the alias of the derived table. In the example above
            // this is case (3), and the next token would be `NATURAL`.
            maybe!(self.maybe_parse(|parser| parser.parse_derived_table_factor(NotLateral)));

            // The '(' we've recently consumed does not start a derived table.
            // For valid input this can happen either when the token following
            // the paren can't start a query (e.g. `foo` in `FROM (foo NATURAL
            // JOIN bar)`, or when the '(' we've consumed is followed by another
            // '(' that starts a derived table, like (3), or another nested join
            // (2).
            //
            // Ignore the error and back up to where we were before. Either
            // we'll be able to parse a valid nested join, or we won't, and
            // we'll return that error instead.
            let table_and_joins = self.parse_table_and_joins()?;
            match table_and_joins.relation {
                TableFactor::NestedJoin { .. } => (),
                _ => {
                    if table_and_joins.joins.is_empty() {
                        // The SQL spec prohibits derived tables and bare
                        // tables from appearing alone in parentheses.
                        self.expected(self.peek_pos(), "joined table", self.peek_token())?
                    }
                }
            }
            self.expect_token(&Token::RParen)?;
            Ok(TableFactor::NestedJoin {
                join: Box::new(table_and_joins),
                alias: self.parse_optional_table_alias()?,
            })
        } else if self.parse_keywords(&[ROWS, FROM]) {
            Ok(self.parse_rows_from()?)
        } else {
            let name = self.parse_raw_name()?;
            match name {
                RawObjectName::Name(name) if self.consume_token(&Token::LParen) => {
                    let args = self.parse_optional_args(false)?;
                    let alias = self.parse_optional_table_alias()?;
                    let with_ordinality = self.parse_keywords(&[WITH, ORDINALITY]);
                    Ok(TableFactor::Function {
                        function: TableFunction { name, args },
                        alias,
                        with_ordinality,
                    })
                }
                _ => Ok(TableFactor::Table {
                    name,
                    alias: self.parse_optional_table_alias()?,
                }),
            }
        }
    }

    fn parse_rows_from(&mut self) -> Result<TableFactor<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let functions = self.parse_comma_separated(Parser::parse_table_function)?;
        self.expect_token(&Token::RParen)?;
        let alias = self.parse_optional_table_alias()?;
        let with_ordinality = self.parse_keywords(&[WITH, ORDINALITY]);
        Ok(TableFactor::RowsFrom {
            functions,
            alias,
            with_ordinality,
        })
    }

    fn parse_table_function(&mut self) -> Result<TableFunction<Raw>, ParserError> {
        let name = self.parse_object_name()?;
        self.expect_token(&Token::LParen)?;
        Ok(TableFunction {
            name,
            args: self.parse_optional_args(false)?,
        })
    }

    fn parse_derived_table_factor(
        &mut self,
        lateral: IsLateral,
    ) -> Result<TableFactor<Raw>, ParserError> {
        let subquery = Box::new(self.parse_query()?);
        self.expect_token(&Token::RParen)?;
        let alias = self.parse_optional_table_alias()?;
        Ok(TableFactor::Derived {
            lateral: match lateral {
                Lateral => true,
                NotLateral => false,
            },
            subquery,
            alias,
        })
    }

    fn parse_join_constraint(&mut self, natural: bool) -> Result<JoinConstraint<Raw>, ParserError> {
        if natural {
            Ok(JoinConstraint::Natural)
        } else if self.parse_keyword(ON) {
            let constraint = self.parse_expr()?;
            Ok(JoinConstraint::On(constraint))
        } else if self.parse_keyword(USING) {
            let columns = self.parse_parenthesized_column_list(Mandatory)?;
            Ok(JoinConstraint::Using(columns))
        } else {
            self.expected(
                self.peek_pos(),
                "ON, or USING after JOIN",
                self.peek_token(),
            )
        }
    }

    /// Parse an INSERT statement
    fn parse_insert(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(INTO)?;
        let table_name = self.parse_raw_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        let source = if self.parse_keywords(&[DEFAULT, VALUES]) {
            InsertSource::DefaultValues
        } else {
            InsertSource::Query(self.parse_query()?)
        };
        let returning = self.parse_returning()?;
        Ok(Statement::Insert(InsertStatement {
            table_name,
            columns,
            source,
            returning,
        }))
    }

    fn parse_returning(&mut self) -> Result<Vec<SelectItem<Raw>>, ParserError> {
        Ok(if self.parse_keyword(RETURNING) {
            self.parse_comma_separated(Parser::parse_select_item)?
        } else {
            Vec::new()
        })
    }

    fn parse_update(&mut self) -> Result<Statement<Raw>, ParserError> {
        let table_name = RawObjectName::Name(self.parse_object_name()?);

        self.expect_keyword(SET)?;
        let assignments = self.parse_comma_separated(Parser::parse_assignment)?;
        let selection = if self.parse_keyword(WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Update(UpdateStatement {
            table_name,
            assignments,
            selection,
        }))
    }

    /// Parse a `var = expr` assignment, used in an UPDATE statement
    fn parse_assignment(&mut self) -> Result<Assignment<Raw>, ParserError> {
        let id = self.parse_identifier()?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_expr()?;
        Ok(Assignment { id, value })
    }

    fn parse_optional_args(
        &mut self,
        allow_order_by: bool,
    ) -> Result<FunctionArgs<Raw>, ParserError> {
        if self.consume_token(&Token::Star) {
            self.expect_token(&Token::RParen)?;
            Ok(FunctionArgs::Star)
        } else if self.consume_token(&Token::RParen) {
            Ok(FunctionArgs::args(vec![]))
        } else {
            let args = self.parse_comma_separated(Parser::parse_expr)?;
            // ORDER BY can only appear after at least one argument, and not after a
            // star. We can ignore checking for it in the other branches. See:
            // https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-AGGREGATES
            let order_by = if allow_order_by && self.parse_keywords(&[ORDER, BY]) {
                self.parse_comma_separated(Parser::parse_order_by_expr)?
            } else {
                vec![]
            };
            self.expect_token(&Token::RParen)?;
            Ok(FunctionArgs::Args { args, order_by })
        }
    }

    /// Parse `AS OF`, if present.
    fn parse_optional_as_of(&mut self) -> Result<Option<AsOf<Raw>>, ParserError> {
        if self.parse_keyword(AS) {
            self.expect_keyword(OF)?;
            if self.parse_keywords(&[AT, LEAST]) {
                match self.parse_expr() {
                    Ok(expr) => Ok(Some(AsOf::AtLeast(expr))),
                    Err(e) => self.expected(
                        e.pos,
                        "a timestamp value after 'AS OF AT LEAST'",
                        self.peek_token(),
                    ),
                }
            } else {
                match self.parse_expr() {
                    Ok(expr) => Ok(Some(AsOf::At(expr))),
                    Err(e) => {
                        self.expected(e.pos, "a timestamp value after 'AS OF'", self.peek_token())
                    }
                }
            }
        } else {
            Ok(None)
        }
    }

    /// Parse a comma-delimited list of projections after SELECT
    fn parse_select_item(&mut self) -> Result<SelectItem<Raw>, ParserError> {
        if self.consume_token(&Token::Star) {
            return Ok(SelectItem::Wildcard);
        }
        Ok(SelectItem::Expr {
            expr: self.parse_expr()?,
            alias: self.parse_optional_alias(Keyword::is_reserved_in_column_alias)?,
        })
    }

    /// Parse an expression, optionally followed by ASC or DESC,
    /// and then `[NULLS { FIRST | LAST }]` (used in ORDER BY)
    fn parse_order_by_expr(&mut self) -> Result<OrderByExpr<Raw>, ParserError> {
        let expr = self.parse_expr()?;

        let asc = if self.parse_keyword(ASC) {
            Some(true)
        } else if self.parse_keyword(DESC) {
            Some(false)
        } else {
            None
        };

        let nulls_last = if self.parse_keyword(NULLS) {
            let last = self.expect_one_of_keywords(&[FIRST, LAST])? == LAST;
            Some(last)
        } else {
            None
        };

        Ok(OrderByExpr {
            expr,
            asc,
            nulls_last,
        })
    }

    fn parse_values(&mut self) -> Result<Values<Raw>, ParserError> {
        let values = self.parse_comma_separated(|parser| {
            parser.expect_token(&Token::LParen)?;
            let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
            parser.expect_token(&Token::RParen)?;
            Ok(exprs)
        })?;
        Ok(Values(values))
    }

    fn parse_start_transaction(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(TRANSACTION)?;
        Ok(Statement::StartTransaction(StartTransactionStatement {
            modes: self.parse_transaction_modes()?,
        }))
    }

    fn parse_begin(&mut self) -> Result<Statement<Raw>, ParserError> {
        let _ = self.parse_one_of_keywords(&[TRANSACTION, WORK]);
        Ok(Statement::StartTransaction(StartTransactionStatement {
            modes: self.parse_transaction_modes()?,
        }))
    }

    fn parse_transaction_modes(&mut self) -> Result<Vec<TransactionMode>, ParserError> {
        let mut modes = vec![];
        let mut required = false;
        loop {
            let mode = if self.parse_keywords(&[ISOLATION, LEVEL]) {
                let iso_level = if self.parse_keywords(&[READ, UNCOMMITTED]) {
                    TransactionIsolationLevel::ReadUncommitted
                } else if self.parse_keywords(&[READ, COMMITTED]) {
                    TransactionIsolationLevel::ReadCommitted
                } else if self.parse_keywords(&[REPEATABLE, READ]) {
                    TransactionIsolationLevel::RepeatableRead
                } else if self.parse_keyword(SERIALIZABLE) {
                    TransactionIsolationLevel::Serializable
                } else {
                    self.expected(self.peek_pos(), "isolation level", self.peek_token())?
                };
                TransactionMode::IsolationLevel(iso_level)
            } else if self.parse_keywords(&[READ, ONLY]) {
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly)
            } else if self.parse_keywords(&[READ, WRITE]) {
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite)
            } else if required {
                self.expected(self.peek_pos(), "transaction mode", self.peek_token())?
            } else {
                break;
            };
            modes.push(mode);
            // ANSI requires a comma after each transaction mode, but
            // PostgreSQL, for historical reasons, does not. We follow
            // PostgreSQL in making the comma optional, since that is strictly
            // more general.
            required = self.consume_token(&Token::Comma);
        }
        Ok(modes)
    }

    fn parse_commit(&mut self) -> Result<Statement<Raw>, ParserError> {
        Ok(Statement::Commit(CommitStatement {
            chain: self.parse_commit_rollback_chain()?,
        }))
    }

    fn parse_rollback(&mut self) -> Result<Statement<Raw>, ParserError> {
        Ok(Statement::Rollback(RollbackStatement {
            chain: self.parse_commit_rollback_chain()?,
        }))
    }

    fn parse_commit_rollback_chain(&mut self) -> Result<bool, ParserError> {
        let _ = self.parse_one_of_keywords(&[TRANSACTION, WORK]);
        if self.parse_keyword(AND) {
            let chain = !self.parse_keyword(NO);
            self.expect_keyword(CHAIN)?;
            Ok(chain)
        } else {
            Ok(false)
        }
    }

    fn parse_tail(&mut self) -> Result<Statement<Raw>, ParserError> {
        let relation = if self.consume_token(&Token::LParen) {
            let query = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            TailRelation::Query(query)
        } else {
            TailRelation::Name(self.parse_raw_name()?)
        };
        let options = self.parse_kw_options(Parser::parse_tail_options)?;
        let as_of = self.parse_optional_as_of()?;
        Ok(Statement::Tail(TailStatement {
            relation,
            options,
            as_of,
        }))
    }

    fn parse_tail_options(&mut self) -> Result<TailOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[PROGRESS, SNAPSHOT])? {
            PROGRESS => TailOptionName::Progress,
            SNAPSHOT => TailOptionName::Snapshot,
            _ => unreachable!(),
        };

        let _ = self.consume_token(&Token::Eq);

        Ok(TailOption {
            name,
            value: self.parse_opt_with_option_value(false)?,
        })
    }

    /// Parse an `EXPLAIN` statement, assuming that the `EXPLAIN` token
    /// has already been consumed.
    fn parse_explain(&mut self) -> Result<Statement<Raw>, ParserError> {
        if let Some(parse) = self.maybe_parse(Self::parse_explain_new) {
            Ok(parse)
        } else {
            self.parse_explain_old()
        }
    }

    /// Parse an `EXPLAIN` statement, assuming that the `EXPLAIN` token
    /// has already been consumed.
    fn parse_explain_new(&mut self) -> Result<Statement<Raw>, ParserError> {
        let stage = match self.parse_one_of_keywords(&[
            RAW,
            DECORRELATED,
            OPTIMIZED,
            PHYSICAL,
            OPTIMIZER,
            QUERY,
        ]) {
            Some(RAW) => {
                self.expect_keyword(PLAN)?;
                ExplainStageNew::RawPlan
            }
            Some(QUERY) => {
                self.expect_keyword(GRAPH)?;
                ExplainStageNew::QueryGraph
            }
            Some(DECORRELATED) => {
                self.expect_keyword(PLAN)?;
                ExplainStageNew::DecorrelatedPlan
            }
            Some(OPTIMIZED) => {
                if self.parse_keyword(QUERY) {
                    self.expect_keyword(GRAPH)?;
                    ExplainStageNew::OptimizedQueryGraph
                } else {
                    self.expect_keyword(PLAN)?;
                    ExplainStageNew::OptimizedPlan
                }
            }
            Some(PHYSICAL) => {
                self.expect_keyword(PLAN)?;
                ExplainStageNew::PhysicalPlan
            }
            Some(OPTIMIZER) => {
                self.expect_keyword(TRACE)?;
                ExplainStageNew::Trace
            }
            None => ExplainStageNew::OptimizedPlan,
            _ => unreachable!(),
        };

        let config_flags = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            let config_flags = self.parse_comma_separated(Self::parse_identifier)?;
            self.expect_token(&Token::RParen)?;
            config_flags
        } else {
            vec![]
        };

        // TODO (#13299): Make specifying the format optional upon getting rid
        // of the old explain syntax
        self.expect_keyword(AS)?;
        let format = match self.parse_one_of_keywords(&[TEXT, JSON, DOT]) {
            Some(TEXT) => ExplainFormat::Text,
            Some(JSON) => ExplainFormat::Json,
            Some(DOT) => ExplainFormat::Dot,
            None => return Err(ParserError::new(self.index, "expected a format")),
            _ => unreachable!(),
        };

        self.expect_keyword(FOR)?;

        // VIEW name | MATERIALIZED VIEW name | query
        let explainee = if self.parse_keyword(VIEW) {
            Explainee::View(self.parse_raw_name()?)
        } else if self.parse_keywords(&[MATERIALIZED, VIEW]) {
            Explainee::MaterializedView(self.parse_raw_name()?)
        } else {
            Explainee::Query(self.parse_query()?)
        };

        Ok(Statement::Explain(ExplainStatement::New(
            ExplainStatementNew {
                stage,
                config_flags,
                format,
                explainee,
            },
        )))
    }

    /// Parse an `EXPLAIN` statement, assuming that the `EXPLAIN` token
    /// has already been consumed (old code path).
    fn parse_explain_old(&mut self) -> Result<Statement<Raw>, ParserError> {
        // (TYPED)?
        let typed = self.parse_keyword(TYPED);
        let mut timing = false;

        // options: ( '(' TIMING (true|false) ')' )?
        if let Some(Token::LParen) = self.peek_token() {
            // Check whether a valid option is after the parentheses, since the
            // parentheses may belong to the actual query to be explained.
            match self.peek_nth_token(1) {
                Some(Token::Keyword(TIMING)) => {
                    self.next_token(); // Consume the LParen
                    self.parse_comma_separated(|s| match s.expect_one_of_keywords(&[TIMING])? {
                        TIMING => {
                            timing = s.parse_boolean_value()?;
                            Ok(())
                        }
                        _ => unreachable!(),
                    })?;
                    self.expect_token(&Token::RParen)?;
                }
                _ => {}
            }
        }

        // (RAW | DECORRELATED | OPTIMIZED | PHYSICAL)? PLAN
        let stage = match self.parse_one_of_keywords(&[
            RAW,
            DECORRELATED,
            OPTIMIZED,
            PHYSICAL,
            PLAN,
            QUERY,
            TIMESTAMP,
        ]) {
            Some(RAW) => {
                self.expect_keywords(&[PLAN, FOR])?;
                ExplainStageOld::RawPlan
            }
            Some(QUERY) => {
                self.expect_keywords(&[GRAPH, FOR])?;
                ExplainStageOld::QueryGraph
            }
            Some(DECORRELATED) => {
                self.expect_keywords(&[PLAN, FOR])?;
                ExplainStageOld::DecorrelatedPlan
            }
            Some(OPTIMIZED) => {
                if self.parse_keyword(QUERY) {
                    self.expect_keywords(&[GRAPH, FOR])?;
                    ExplainStageOld::OptimizedQueryGraph
                } else {
                    self.expect_keywords(&[PLAN, FOR])?;
                    ExplainStageOld::OptimizedPlan
                }
            }
            Some(PLAN) => {
                self.expect_keyword(FOR)?;
                ExplainStageOld::OptimizedPlan
            }
            Some(PHYSICAL) => {
                self.expect_keywords(&[PLAN, FOR])?;
                ExplainStageOld::PhysicalPlan
            }
            Some(TIMESTAMP) => {
                self.expect_keywords(&[FOR])?;
                ExplainStageOld::Timestamp
            }
            None => ExplainStageOld::OptimizedPlan,
            _ => unreachable!(),
        };

        // VIEW name | MATERIALIZED VIEW name | query
        let explainee = if self.parse_keyword(VIEW) {
            Explainee::View(self.parse_raw_name()?)
        } else if self.parse_keywords(&[MATERIALIZED, VIEW]) {
            Explainee::MaterializedView(self.parse_raw_name()?)
        } else {
            Explainee::Query(self.parse_query()?)
        };

        let options = ExplainOptions { typed, timing };
        Ok(Statement::Explain(ExplainStatement::Old(
            ExplainStatementOld {
                stage,
                explainee,
                options,
            },
        )))
    }

    /// Parse a `DECLARE` statement, assuming that the `DECLARE` token
    /// has already been consumed.
    fn parse_declare(&mut self) -> Result<Statement<Raw>, ParserError> {
        let name = self.parse_identifier()?;
        self.expect_keyword(CURSOR)?;
        // WITHOUT HOLD is optional and the default behavior so we can ignore it.
        let _ = self.parse_keywords(&[WITHOUT, HOLD]);
        self.expect_keyword(FOR)?;
        let stmt = self.parse_statement()?;
        Ok(Statement::Declare(DeclareStatement {
            name,
            stmt: Box::new(stmt),
        }))
    }

    /// Parse a `CLOSE` statement, assuming that the `CLOSE` token
    /// has already been consumed.
    fn parse_close(&mut self) -> Result<Statement<Raw>, ParserError> {
        let name = self.parse_identifier()?;
        Ok(Statement::Close(CloseStatement { name }))
    }

    /// Parse a `PREPARE` statement, assuming that the `PREPARE` token
    /// has already been consumed.
    fn parse_prepare(&mut self) -> Result<Statement<Raw>, ParserError> {
        let name = self.parse_identifier()?;
        self.expect_keyword(AS)?;
        let pos = self.peek_pos();
        //
        let stmt = match self.parse_statement()? {
            stmt @ Statement::Select(_)
            | stmt @ Statement::Insert(_)
            | stmt @ Statement::Delete(_)
            | stmt @ Statement::Update(_) => stmt,
            _ => return parser_err!(self, pos, "unpreparable statement"),
        };
        Ok(Statement::Prepare(PrepareStatement {
            name,
            stmt: Box::new(stmt),
        }))
    }

    /// Parse a `EXECUTE` statement, assuming that the `EXECUTE` token
    /// has already been consumed.
    fn parse_execute(&mut self) -> Result<Statement<Raw>, ParserError> {
        let name = self.parse_identifier()?;
        let params = if self.consume_token(&Token::LParen) {
            let params = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
            params
        } else {
            Vec::new()
        };
        Ok(Statement::Execute(ExecuteStatement { name, params }))
    }

    /// Parse a `DEALLOCATE` statement, assuming that the `DEALLOCATE` token
    /// has already been consumed.
    fn parse_deallocate(&mut self) -> Result<Statement<Raw>, ParserError> {
        let _ = self.parse_keyword(PREPARE);
        let name = if self.parse_keyword(ALL) {
            None
        } else {
            Some(self.parse_identifier()?)
        };
        Ok(Statement::Deallocate(DeallocateStatement { name }))
    }

    /// Parse a `FETCH` statement, assuming that the `FETCH` token
    /// has already been consumed.
    fn parse_fetch(&mut self) -> Result<Statement<Raw>, ParserError> {
        let _ = self.parse_keyword(FORWARD);
        let count = if let Some(count) = self.maybe_parse(Parser::parse_literal_uint) {
            Some(FetchDirection::ForwardCount(count))
        } else if self.parse_keyword(ALL) {
            Some(FetchDirection::ForwardAll)
        } else {
            None
        };
        let _ = self.parse_keyword(FROM);
        let name = self.parse_identifier()?;
        let options = self.parse_kw_options(Parser::parse_fetch_option)?;
        Ok(Statement::Fetch(FetchStatement {
            name,
            count,
            options,
        }))
    }

    fn parse_fetch_option(&mut self) -> Result<FetchOption<Raw>, ParserError> {
        self.expect_keyword(TIMEOUT)?;
        let _ = self.consume_token(&Token::Eq);
        Ok(FetchOption {
            name: FetchOptionName::Timeout,
            value: self.parse_opt_with_option_value(false)?,
        })
    }

    /// Parse a `RAISE` statement, assuming that the `RAISE` token
    /// has already been consumed.
    fn parse_raise(&mut self) -> Result<Statement<Raw>, ParserError> {
        let severity = match self.parse_one_of_keywords(&[DEBUG, INFO, LOG, NOTICE, WARNING]) {
            Some(DEBUG) => NoticeSeverity::Debug,
            Some(INFO) => NoticeSeverity::Info,
            Some(LOG) => NoticeSeverity::Log,
            Some(NOTICE) => NoticeSeverity::Notice,
            Some(WARNING) => NoticeSeverity::Warning,
            Some(_) => unreachable!(),
            None => self.expected(self.peek_pos(), "severity level", self.peek_token())?,
        };

        Ok(Statement::Raise(RaiseStatement { severity }))
    }
}

impl CheckedRecursion for Parser<'_> {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}
