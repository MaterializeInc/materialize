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

//! SQL Parser

use std::error::Error;
use std::fmt;

use itertools::Itertools;
use log::debug;

use ore::collections::CollectionExt;
use repr::adt::datetime::DateTimeField;

use crate::ast::*;
use crate::keywords::*;
use crate::lexer::{self, Token};

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

/// Parses a SQL string containing zero or more SQL columns.
pub fn parse_columns(
    sql: &str,
) -> Result<(Vec<ColumnDef<Raw>>, Vec<TableConstraint<Raw>>), ParserError> {
    let tokens = lexer::lex(sql)?;
    let mut parser = Parser::new(sql, tokens);
    let columns = parser.parse_columns(Optional)?;
    if parser.next_token().is_some() {
        parser_err!(parser, parser.peek_prev_pos(), "extra token after columns")
    } else {
        Ok(columns)
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
    /// Tracks recursion depth.
    depth: usize,
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
            depth: 0,
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
        let expr = self.check_descent(|parser| parser.parse_prefix())?;
        self.parse_subexpr_seeded(precedence, expr)
    }

    fn parse_subexpr_seeded(
        &mut self,
        precedence: Precedence,
        mut expr: Expr<Raw>,
    ) -> Result<Expr<Raw>, ParserError> {
        self.check_descent(|parser| {
            debug!("prefix: {:?}", expr);
            loop {
                let next_precedence = parser.get_next_precedence();
                debug!("next precedence: {:?}", next_precedence);
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
            Token::Keyword(COALESCE) => self.parse_coalesce_expr(),
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
                    op,
                    expr1: Box::new(self.parse_subexpr(Precedence::PrefixPlusMinus)?),
                    expr2: None,
                })
            }
            Token::Op(op) if op == "+" => Ok(Expr::Op {
                op,
                expr1: Box::new(self.parse_subexpr(Precedence::PrefixPlusMinus)?),
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
            if let Some(SELECT) | Some(WITH) = parser.peek_keyword() {
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
                let either = parser.check_descent(parse)?;
                parser.expect_token(&Token::RParen)?;

                // Decide if we need to associate any tokens after the closing
                // parenthesis with what we've parsed so far.
                match (either, parser.peek_token()) {
                    // The next token is another closing parenthesis. Can't
                    // resolve the amibiguity yet. Return to let our caller
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
        let all = self.parse_keyword(ALL);
        let distinct = self.parse_keyword(DISTINCT);
        if all && distinct {
            return parser_err!(
                self,
                self.peek_prev_pos(),
                format!("Cannot specify both ALL and DISTINCT in function: {}", name)
            );
        }
        let args = self.parse_optional_args()?;
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

    fn parse_coalesce_expr(&mut self) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let exprs = self.parse_comma_separated(Parser::parse_expr)?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Coalesce { exprs })
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
            name: UnresolvedObjectName::unqualified("date_part"),
            args: FunctionArgs::Args(vec![Expr::Value(Value::String(field)), expr]),
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
            args: FunctionArgs::Args(exprs),
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
            args: FunctionArgs::Args(vec![needle, haystack]),
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
    ///   4. `INTERVAL '1:1:1.1' HOUR TO SECOND (5)`
    ///   5. `INTERVAL '1.111' SECOND (2)`
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
        debug!("parsing infix");
        let tok = self.next_token().unwrap(); // safe as EOF's precedence is the lowest

        let regular_binary_operator = match &tok {
            Token::Op(s) => Some(s.as_str()),
            Token::Eq => Some("="),
            Token::Star => Some("*"),
            Token::Keyword(ILIKE) => Some("~~*"),
            Token::Keyword(LIKE) => Some("~~"),
            Token::Keyword(NOT) => {
                if self.parse_keyword(LIKE) {
                    Some("!~~")
                } else if self.parse_keyword(ILIKE) {
                    Some("!~~*")
                } else {
                    None
                }
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
                            op: op.into(),
                            right: Box::new(subquery),
                        }
                    } else {
                        Expr::AnySubquery {
                            left: Box::new(expr),
                            op: op.into(),
                            right: Box::new(subquery),
                        }
                    }
                } else {
                    let right = self.parse_expr()?;

                    if kw == ALL {
                        Expr::AllExpr {
                            left: Box::new(expr),
                            op: op.into(),
                            right: Box::new(right),
                        }
                    } else {
                        Expr::AnyExpr {
                            left: Box::new(expr),
                            op: op.into(),
                            right: Box::new(right),
                        }
                    }
                };
                self.expect_token(&Token::RParen)?;

                Ok(expr)
            } else {
                Ok(Expr::Op {
                    op: op.into(),
                    expr1: Box::new(expr),
                    expr2: Some(Box::new(self.parse_subexpr(precedence)?)),
                })
            }
        } else if let Token::Keyword(kw) = tok {
            match kw {
                IS => {
                    if self.parse_keyword(NULL) {
                        Ok(Expr::IsNull {
                            expr: Box::new(expr),
                            negated: false,
                        })
                    } else if self.parse_keywords(&[NOT, NULL]) {
                        Ok(Expr::IsNull {
                            expr: Box::new(expr),
                            negated: true,
                        })
                    } else {
                        self.expected(
                            self.peek_pos(),
                            "NULL or NOT NULL after IS",
                            self.peek_token(),
                        )
                    }
                }
                ISNULL => Ok(Expr::IsNull {
                    expr: Box::new(expr),
                    negated: false,
                }),
                NOT | IN | BETWEEN => {
                    self.prev_token();
                    let negated = self.parse_keyword(NOT);
                    if self.parse_keyword(IN) {
                        self.parse_in(expr, negated)
                    } else if self.parse_keyword(BETWEEN) {
                        self.parse_between(expr, negated)
                    } else {
                        self.expected(
                            self.peek_pos(),
                            "IN or BETWEEN after NOT",
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
                        args: FunctionArgs::Args(vec![self.parse_subexpr(precedence)?, expr]),
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
            self.parse_subscript(expr)
        } else if Token::Dot == tok {
            match self.next_token() {
                Some(Token::Ident(id)) => Ok(Expr::FieldAccess {
                    expr: Box::new(expr),
                    field: Ident::new(id),
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
        let mut is_slice = false;
        let mut positions = Vec::new();
        loop {
            let start = if self.consume_token(&Token::Colon) {
                is_slice = true;
                None
            } else {
                let e = Some(self.parse_expr()?);
                if is_slice {
                    self.expect_token(&Token::Colon)?;
                } else {
                    is_slice = self.consume_token(&Token::Colon);
                }
                e
            };

            let end = if is_slice
                && (Some(Token::RBracket) != self.peek_token()
                    && Some(Token::Comma) != self.peek_token())
            {
                Some(self.parse_expr()?)
            } else {
                None
            };

            positions.push(SubscriptPosition { start, end });

            if !is_slice || !self.consume_token(&Token::Comma) {
                break;
            }
        }

        self.expect_token(&Token::RBracket)?;

        Ok(if is_slice {
            Expr::SubscriptSlice {
                expr: Box::new(expr),
                positions,
            }
        } else {
            assert!(
                positions.len() == 1 && positions[0].start.is_some() && positions[0].end.is_none(),
            );
            Expr::SubscriptIndex {
                expr: Box::new(expr),
                subscript: Box::new(positions.remove(0).start.unwrap()),
            }
        })
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
            debug!("get_next_precedence() {:?}", token);

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

    fn peek_keyword(&self) -> Option<Keyword> {
        match self.peek_token() {
            Some(Token::Keyword(kw)) => Some(kw),
            _ => None,
        }
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
            "Expected {}, found {}{}",
            expected,
            found.as_ref().map(|t| t.name()).unwrap_or("EOF"),
            found
                .as_ref()
                .filter(|t| t.has_value())
                .map(|t| format!(" '{}'", t.value()))
                .unwrap_or_else(String::new)
        )
    }

    /// Look for an expected keyword and consume it if it exists
    #[must_use]
    fn parse_keyword(&mut self, kw: Keyword) -> bool {
        match self.peek_token() {
            Some(Token::Keyword(k)) if k == kw => {
                self.next_token();
                true
            }
            _ => false,
        }
    }

    /// Look for an expected sequence of keywords and consume them if they exist
    #[must_use]
    fn parse_keywords(&mut self, keywords: &[Keyword]) -> bool {
        let index = self.index;
        for keyword in keywords {
            if !self.parse_keyword(*keyword) {
                self.index = index;
                return false;
            }
        }
        true
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
    #[must_use = "must match against result to determine what keyword was parsed"]
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
            self.expected(self.peek_pos(), expected.name(), self.peek_token())
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
        if self.parse_keyword(DATABASE) {
            self.parse_create_database()
        } else if self.parse_keyword(SCHEMA) {
            self.parse_create_schema()
        } else if self.parse_keyword(TABLE) {
            self.prev_token();
            self.parse_create_table()
        } else if self.parse_keyword(OR) || self.parse_keyword(VIEW) {
            self.prev_token();
            self.parse_create_view()
        } else if self.parse_keyword(TEMP) || self.parse_keyword(TEMPORARY) {
            if self.parse_keyword(VIEW) {
                self.prev_token();
                self.prev_token();
                self.parse_create_view()
            } else if self.parse_keyword(MATERIALIZED) && self.parse_keyword(VIEW) {
                self.prev_token();
                self.prev_token();
                self.prev_token();
                self.parse_create_view()
            } else if self.parse_keyword(TABLE) {
                self.prev_token();
                self.prev_token();
                self.parse_create_table()
            } else {
                self.expected(
                    self.peek_pos(),
                    "VIEW or MATERIALIZED VIEW after CREATE TEMPORARY",
                    self.peek_token(),
                )
            }
        } else if self.parse_keyword(MATERIALIZED) {
            if self.parse_keyword(VIEW) {
                self.prev_token();
                self.prev_token();
                self.parse_create_view()
            } else if self.parse_keyword(SOURCE) {
                self.prev_token();
                self.prev_token();
                self.parse_create_source()
            } else {
                self.expected(
                    self.peek_pos(),
                    "VIEW or SOURCE after CREATE MATERIALIZED",
                    self.peek_token(),
                )
            }
        } else if self.parse_keyword(SOURCE) {
            self.prev_token();
            self.parse_create_source()
        } else if self.parse_keyword(SOURCES) {
            self.parse_create_sources()
        } else if self.parse_keyword(SINK) {
            self.parse_create_sink()
        } else if self.parse_keyword(DEFAULT) {
            self.expect_keyword(INDEX)?;
            self.prev_token();
            self.prev_token();
            self.parse_create_index()
        } else if self.parse_keyword(INDEX) {
            self.prev_token();
            self.parse_create_index()
        } else if self.parse_keyword(ROLE) || self.parse_keyword(USER) {
            self.prev_token();
            self.parse_create_role()
        } else if self.parse_keyword(TYPE) {
            self.parse_create_type()
        } else {
            self.expected(
                self.peek_pos(),
                "DATABASE, INDEX, ROLE, SCHEMA, SINK, SOURCE, TYPE, USER, or [MATERIALIZED] VIEW after CREATE",
                self.peek_token(),
            )
        }
    }

    fn parse_create_database(&mut self) -> Result<Statement<Raw>, ParserError> {
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_identifier()?;
        Ok(Statement::CreateDatabase(CreateDatabaseStatement {
            name,
            if_not_exists,
        }))
    }

    fn parse_create_schema(&mut self) -> Result<Statement<Raw>, ParserError> {
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_object_name()?;
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
            self.expect_keyword(MESSAGE)?;
            let message_name = self.parse_literal_string()?;
            self.expect_keyword(USING)?;
            let schema = self.parse_schema()?;
            Format::Protobuf {
                message_name,
                schema,
            }
        } else if self.parse_keyword(REGEX) {
            let regex = self.parse_literal_string()?;
            Format::Regex(regex)
        } else if self.parse_keyword(CSV) {
            self.expect_keyword(WITH)?;
            let (header_row, n_cols) = if self.parse_keyword(HEADER) || self.parse_keyword(HEADERS)
            {
                (true, None)
            } else {
                let n_cols = self.parse_literal_uint()? as usize;
                self.expect_keyword(COLUMNS)?;
                (false, Some(n_cols))
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
            Format::Csv {
                header_row,
                n_cols,
                delimiter,
            }
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
            let url = self.parse_literal_string()?;

            let seed = if self.parse_keyword(SEED) {
                let key_schema = if self.parse_keyword(KEY) {
                    self.expect_keyword(SCHEMA)?;
                    Some(self.parse_literal_string()?)
                } else {
                    None
                };
                self.expect_keywords(&[VALUE, SCHEMA])?;
                let value_schema = self.parse_literal_string()?;
                Some(CsrSeed {
                    key_schema,
                    value_schema,
                })
            } else {
                None
            };

            // Look ahead to avoid erroring on `WITH SNAPSHOT`; we only want to
            // accept `WITH (...)` here.
            let with_options = if self.peek_nth_token(1) == Some(Token::LParen) {
                self.parse_opt_with_sql_options()?
            } else {
                vec![]
            };

            AvroSchema::CsrUrl {
                url,
                seed,
                with_options,
            }
        } else if self.parse_keyword(SCHEMA) {
            self.prev_token();
            let schema = self.parse_schema()?;
            // Look ahead to avoid erroring on `WITH SNAPSHOT`; we only want to
            // accept `WITH (...)` here.
            let with_options = if self.peek_nth_token(1) == Some(Token::LParen) {
                self.expect_keyword(WITH)?;
                self.parse_with_options(false)?
            } else {
                vec![]
            };
            AvroSchema::Schema {
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
                DbzMode::Plain
            };
            Envelope::Debezium(debezium_mode)
        } else if self.parse_keyword(UPSERT) {
            let format = if self.parse_keyword(FORMAT) {
                Some(self.parse_format()?)
            } else {
                None
            };
            Envelope::Upsert(format)
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

    fn parse_create_source(&mut self) -> Result<Statement<Raw>, ParserError> {
        let materialized = self.parse_keyword(MATERIALIZED);
        self.expect_keyword(SOURCE)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_object_name()?;
        let col_names = self.parse_parenthesized_column_list(Optional)?;
        self.expect_keyword(FROM)?;
        let connector = self.parse_connector()?;
        let with_options = self.parse_opt_with_sql_options()?;
        let format = if self.parse_keyword(FORMAT) {
            Some(self.parse_format()?)
        } else {
            None
        };
        let envelope = if self.parse_keyword(ENVELOPE) {
            self.parse_envelope()?
        } else {
            Default::default()
        };

        Ok(Statement::CreateSource(CreateSourceStatement {
            name,
            col_names,
            connector,
            with_options,
            format,
            envelope,
            if_not_exists,
            materialized,
        }))
    }

    fn parse_create_sources(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(FROM)?;
        let connector = self.parse_multi_connector()?;

        Ok(Statement::CreateSources(CreateSourcesStatement {
            connector,
            stmts: vec![],
        }))
    }

    fn parse_create_sink(&mut self) -> Result<Statement<Raw>, ParserError> {
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_object_name()?;
        self.expect_keyword(FROM)?;
        let from = self.parse_object_name()?;
        self.expect_keyword(INTO)?;
        let connector = self.parse_connector()?;
        let mut with_options = vec![];
        if self.parse_keyword(WITH) {
            if let Some(Token::LParen) = self.next_token() {
                self.prev_token();
                self.prev_token();
                with_options = self.parse_opt_with_sql_options()?;
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
            from,
            connector,
            with_options,
            format,
            envelope,
            with_snapshot,
            as_of,
            if_not_exists,
        }))
    }

    fn parse_connector(&mut self) -> Result<Connector<Raw>, ParserError> {
        match self.expect_one_of_keywords(&[FILE, KAFKA, KINESIS, AVRO, S3, POSTGRES, PUBNUB])? {
            PUBNUB => {
                self.expect_keywords(&[SUBSCRIBE, KEY])?;
                let subscribe_key = self.parse_literal_string()?;
                self.expect_keyword(CHANNEL)?;
                let channel = self.parse_literal_string()?;

                Ok(Connector::PubNub {
                    subscribe_key,
                    channel,
                })
            }
            POSTGRES => {
                self.expect_keyword(HOST)?;
                let conn = self.parse_literal_string()?;
                self.expect_keyword(PUBLICATION)?;
                let publication = self.parse_literal_string()?;
                self.expect_keyword(NAMESPACE)?;
                let namespace = self.parse_literal_string()?;
                self.expect_keyword(TABLE)?;
                let table = self.parse_literal_string()?;

                let (columns, constraints) = self.parse_columns(Optional)?;

                if !constraints.is_empty() {
                    return parser_err!(
                        self,
                        self.peek_prev_pos(),
                        "Cannot specify constraints in Postgres table definition"
                    );
                }

                Ok(Connector::Postgres {
                    conn,
                    publication,
                    namespace,
                    table,
                    columns,
                })
            }
            FILE => {
                let path = self.parse_literal_string()?;
                let compression = if self.parse_keyword(COMPRESSION) {
                    self.parse_compression()?
                } else {
                    Compression::None
                };
                Ok(Connector::File { path, compression })
            }
            KAFKA => {
                self.expect_keyword(BROKER)?;
                let broker = self.parse_literal_string()?;
                self.expect_keyword(TOPIC)?;
                let topic = self.parse_literal_string()?;
                let key = if self.parse_keyword(KEY) {
                    Some(self.parse_parenthesized_column_list(Mandatory)?)
                } else {
                    None
                };
                Ok(Connector::Kafka { broker, topic, key })
            }
            KINESIS => {
                self.expect_keyword(ARN)?;
                let arn = self.parse_literal_string()?;
                Ok(Connector::Kinesis { arn })
            }
            AVRO => {
                self.expect_keyword(OCF)?;
                let path = self.parse_literal_string()?;
                Ok(Connector::AvroOcf { path })
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
                Ok(Connector::S3 {
                    key_sources,
                    pattern,
                    compression,
                })
            }
            _ => unreachable!(),
        }
    }

    fn parse_multi_connector(&mut self) -> Result<MultiConnector<Raw>, ParserError> {
        match self.expect_one_of_keywords(&[POSTGRES])? {
            POSTGRES => {
                self.expect_keyword(HOST)?;
                let conn = self.parse_literal_string()?;
                self.expect_keyword(PUBLICATION)?;
                let publication = self.parse_literal_string()?;
                self.expect_keyword(NAMESPACE)?;
                let namespace = self.parse_literal_string()?;
                self.expect_keyword(TABLES)?;
                self.expect_token(&Token::LParen)?;
                let tables = self.parse_postgres_tables()?;

                Ok(MultiConnector::Postgres {
                    conn,
                    publication,
                    namespace,
                    tables,
                })
            }
            _ => unreachable!(),
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
        let materialized = self.parse_keyword(MATERIALIZED);
        self.expect_keyword(VIEW)?;
        if if_exists == IfExistsBehavior::Error && self.parse_if_not_exists()? {
            if_exists = IfExistsBehavior::Skip;
        }

        // Many dialects support `OR REPLACE` | `OR ALTER` right after `CREATE`, but we don't (yet).
        // ANSI SQL and Postgres support RECURSIVE here, but we don't support it either.
        let name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        let with_options = self.parse_opt_with_sql_options()?;
        self.expect_keyword(AS)?;
        let query = self.parse_query()?;
        // Optional `WITH [ CASCADED | LOCAL ] CHECK OPTION` is widely supported here.
        Ok(Statement::CreateView(CreateViewStatement {
            name,
            columns,
            query,
            temporary,
            materialized,
            if_exists,
            with_options,
        }))
    }

    fn parse_create_index(&mut self) -> Result<Statement<Raw>, ParserError> {
        let default_index = self.parse_keyword(DEFAULT);
        self.expect_keyword(INDEX)?;

        let if_not_exists = self.parse_if_not_exists()?;
        let name = if self.parse_keyword(ON) {
            if if_not_exists && !default_index {
                self.prev_token();
                return self.expected(self.peek_pos(), "index name", self.peek_token());
            }
            None
        } else {
            let name = self.parse_identifier()?;
            self.expect_keyword(ON)?;
            Some(name)
        };
        let on_name = self.parse_object_name()?;

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

        let with_options = self.parse_opt_with_options()?;

        Ok(Statement::CreateIndex(CreateIndexStatement {
            name,
            on_name,
            key_parts,
            with_options,
            if_not_exists,
        }))
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

    fn parse_create_type(&mut self) -> Result<Statement<Raw>, ParserError> {
        let name = self.parse_object_name()?;
        self.expect_keyword(AS)?;
        let as_type = match self.expect_one_of_keywords(&[LIST, MAP])? {
            LIST => CreateTypeAs::List,
            MAP => CreateTypeAs::Map,
            _ => unreachable!(),
        };

        self.expect_token(&Token::LParen)?;
        let with_options = self.parse_comma_separated(Parser::parse_data_type_option)?;
        self.expect_token(&Token::RParen)?;

        Ok(Statement::CreateType(CreateTypeStatement {
            name,
            as_type,
            with_options,
        }))
    }

    fn parse_data_type_option(&mut self) -> Result<SqlOption<Raw>, ParserError> {
        let name = self.parse_identifier()?;
        self.expect_token(&Token::Eq)?;
        Ok(SqlOption::DataType {
            name,
            data_type: self.parse_data_type()?,
        })
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
        let object_type = match self.parse_one_of_keywords(&[
            DATABASE, INDEX, ROLE, SCHEMA, SINK, SOURCE, TABLE, TYPE, USER, VIEW,
        ]) {
            Some(DATABASE) => {
                return Ok(Statement::DropDatabase(DropDatabaseStatement {
                    if_exists: self.parse_if_exists()?,
                    name: self.parse_identifier()?,
                }));
            }
            Some(INDEX) => ObjectType::Index,
            Some(ROLE) | Some(USER) => ObjectType::Role,
            Some(SCHEMA) => ObjectType::Schema,
            Some(SINK) => ObjectType::Sink,
            Some(SOURCE) => ObjectType::Source,
            Some(TABLE) => ObjectType::Table,
            Some(TYPE) => ObjectType::Type,
            Some(VIEW) => ObjectType::View,
            _ => return self.expected(
                self.peek_pos(),
                "DATABASE, INDEX, ROLE, SCHEMA, SINK, SOURCE, TABLE, TYPE, USER, VIEW after DROP",
                self.peek_token(),
            ),
        };

        let if_exists = self.parse_if_exists()?;
        let names = self.parse_comma_separated(Parser::parse_object_name)?;
        let cascade = self.parse_keyword(CASCADE);
        let restrict = self.parse_keyword(RESTRICT);
        let restrict_pos = self.peek_prev_pos();
        if cascade && restrict {
            return parser_err!(
                self,
                restrict_pos,
                "Cannot specify both CASCADE and RESTRICT in DROP"
            );
        }
        Ok(Statement::DropObjects(DropObjectsStatement {
            object_type,
            if_exists,
            names,
            cascade,
        }))
    }

    fn parse_create_table(&mut self) -> Result<Statement<Raw>, ParserError> {
        let temporary = self.parse_keyword(TEMPORARY) | self.parse_keyword(TEMP);
        self.expect_keyword(TABLE)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let table_name = self.parse_object_name()?;
        // parse optional column list (schema)
        let (columns, constraints) = self.parse_columns(Mandatory)?;
        let with_options = self.parse_opt_with_sql_options()?;

        Ok(Statement::CreateTable(CreateTableStatement {
            name: table_name,
            columns,
            constraints,
            with_options,
            if_not_exists,
            temporary,
        }))
    }

    fn parse_postgres_tables(&mut self) -> Result<Vec<PgTable<Raw>>, ParserError> {
        let mut tables = vec![];
        loop {
            let name = self.parse_literal_string()?;
            self.expect_keyword(AS)?;
            let alias = RawName::Name(self.parse_object_name()?);
            let (columns, constraints) = self.parse_columns(Optional)?;
            if !constraints.is_empty() {
                return parser_err!(
                    self,
                    self.peek_prev_pos(),
                    "Cannot specify constraints in Postgres table definition"
                );
            }
            tables.push(PgTable {
                name,
                alias,
                columns,
            });

            if self.consume_token(&Token::Comma) {
                // Continue.
            } else if self.consume_token(&Token::RParen) {
                break;
            } else {
                return self.expected(
                    self.peek_pos(),
                    "',' or ')' after table definition",
                    self.peek_token(),
                );
            }
        }
        Ok(tables)
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
                let foreign_table = self.parse_object_name()?;
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

    fn parse_opt_with_sql_options(&mut self) -> Result<Vec<SqlOption<Raw>>, ParserError> {
        if self.parse_keyword(WITH) {
            self.parse_options()
        } else {
            Ok(vec![])
        }
    }

    fn parse_options(&mut self) -> Result<Vec<SqlOption<Raw>>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let options = self.parse_comma_separated(Parser::parse_sql_option)?;
        self.expect_token(&Token::RParen)?;
        Ok(options)
    }

    fn parse_sql_option(&mut self) -> Result<SqlOption<Raw>, ParserError> {
        let name = self.parse_identifier()?;
        self.expect_token(&Token::Eq)?;
        let token = self.peek_token();
        let option = if let Ok(value) = self.parse_value() {
            SqlOption::Value { name, value }
        } else {
            self.prev_token();
            if let Ok(object_name) = self.parse_object_name() {
                SqlOption::ObjectName { name, object_name }
            } else {
                self.expected(self.peek_pos(), "option value", token)?
            }
        };
        Ok(option)
    }

    fn parse_opt_with_options(&mut self) -> Result<Vec<WithOption>, ParserError> {
        if self.parse_keyword(WITH) {
            self.parse_with_options(true)
        } else {
            Ok(vec![])
        }
    }

    fn parse_with_options(&mut self, require_equals: bool) -> Result<Vec<WithOption>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let options =
            self.parse_comma_separated(|parser| parser.parse_with_option(require_equals))?;
        self.expect_token(&Token::RParen)?;
        Ok(options)
    }

    /// If require_equals is true, parse options of the form `KEY = VALUE` or just
    /// `KEY`. If require_equals is false, additionally support `KEY VALUE` (but still the others).
    fn parse_with_option(&mut self, require_equals: bool) -> Result<WithOption, ParserError> {
        let key = self.parse_identifier()?;
        let has_eq = self.consume_token(&Token::Eq);
        // No = was encountered and require_equals is false, so the next token might be
        // a value and might not. The only valid things that indicate no value would
        // be `)` for end-of-options or `,` for another-option. Either of those means
        // there's no value, anything else means we expect a valid value.
        let has_value = !matches!(self.peek_token(), Some(Token::RParen) | Some(Token::Comma));
        if has_value && !has_eq && require_equals {
            return self.expected(self.peek_pos(), Token::Eq.name(), self.peek_token());
        }
        let value = if has_value {
            if let Some(value) = self.maybe_parse(Parser::parse_value) {
                Some(WithOptionValue::Value(value))
            } else if let Some(object_name) = self.maybe_parse(Parser::parse_object_name) {
                Some(WithOptionValue::ObjectName(object_name))
            } else {
                return self.expected(self.peek_pos(), "option value", self.peek_token());
            }
        } else {
            None
        };
        Ok(WithOption { key, value })
    }

    fn parse_alter(&mut self) -> Result<Statement<Raw>, ParserError> {
        let object_type = match self.expect_one_of_keywords(&[INDEX, SINK, SOURCE, VIEW, TABLE])? {
            INDEX => ObjectType::Index,
            SINK => ObjectType::Sink,
            SOURCE => ObjectType::Source,
            VIEW => ObjectType::View,
            TABLE => ObjectType::Table,
            _ => unreachable!(),
        };

        let if_exists = self.parse_if_exists()?;
        let name = self.parse_object_name()?;

        // We support `ALTER INDEX ... {RESET, SET} and `ALTER <object type> RENAME
        if object_type == ObjectType::Index {
            let options = match self.parse_one_of_keywords(&[RESET, SET]) {
                Some(RESET) => {
                    self.expect_token(&Token::LParen)?;
                    let reset_options = self.parse_comma_separated(Parser::parse_identifier)?;
                    self.expect_token(&Token::RParen)?;

                    Some(AlterIndexOptionsList::Reset(reset_options))
                }
                Some(SET) => {
                    let set_options = self.parse_with_options(true)?;

                    Some(AlterIndexOptionsList::Set(set_options))
                }
                Some(_) => unreachable!(),
                None => None,
            };

            if let Some(options) = options {
                return Ok(Statement::AlterIndexOptions(AlterIndexOptionsStatement {
                    index_name: name,
                    if_exists,
                    options,
                }));
            }
        }

        self.expect_keywords(&[RENAME, TO])?;
        let to_item_name = self.parse_identifier()?;

        Ok(Statement::AlterObjectRename(AlterObjectRenameStatement {
            object_type,
            if_exists,
            name,
            to_item_name,
        }))
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
            let name = self.parse_object_name()?;
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
        let mut options = vec![];
        // WITH must be followed by LParen. The WITH in COPY is optional for backward
        // compat with Postgres but is required elsewhere, which is why we don't use
        // parse_with_options here.
        let has_options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            true
        } else {
            self.consume_token(&Token::LParen)
        };
        if has_options {
            self.prev_token();
            options = self.parse_with_options(false)?;
        }
        Ok(Statement::Copy(CopyStatement {
            relation,
            direction,
            target,
            options,
        }))
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
        Ok(Expr::Array(self.parse_sequence(Self::parse_array)?))
    }

    fn parse_list(&mut self) -> Result<Expr<Raw>, ParserError> {
        Ok(Expr::List(self.parse_sequence(Self::parse_list)?))
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

    /// Parse an unsigned literal integer/long
    fn parse_literal_uint(&mut self) -> Result<u64, ParserError> {
        match self.next_token() {
            Some(Token::Number(s)) => s.parse::<u64>().map_err(|e| {
                self.error(
                    self.peek_prev_pos(),
                    format!("Could not parse '{}' as u64: {}", s, e),
                )
            }),
            other => self.expected(self.peek_prev_pos(), "literal int", other),
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
    fn parse_data_type(&mut self) -> Result<DataType<Raw>, ParserError> {
        let other = |name: &str| DataType::Other {
            name: RawName::Name(UnresolvedObjectName::unqualified(name)),
            typ_mod: vec![],
        };

        let mut data_type = match self.next_token() {
            Some(Token::Keyword(kw)) => match kw {
                // Text-like types
                CHAR | CHARACTER => {
                    let name = if self.parse_keyword(VARYING) {
                        "varchar"
                    } else {
                        "char"
                    };
                    DataType::Other {
                        name: RawName::Name(UnresolvedObjectName::unqualified(name)),
                        typ_mod: match self.parse_optional_precision()? {
                            Some(u) => vec![u],
                            None => vec![],
                        },
                    }
                }
                VARCHAR => DataType::Other {
                    name: RawName::Name(UnresolvedObjectName::unqualified("varchar")),
                    typ_mod: match self.parse_optional_precision()? {
                        Some(u) => vec![u],
                        None => vec![],
                    },
                },
                STRING => other("text"),

                // Number-like types
                BIGINT => other("int8"),
                SMALLINT => other("int2"),
                DEC | DECIMAL => DataType::Other {
                    name: RawName::Name(UnresolvedObjectName::unqualified("numeric")),
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
                REGCLASS => other("oid"),

                _ => {
                    self.prev_token();
                    DataType::Other {
                        name: RawName::Name(self.parse_object_name()?),
                        typ_mod: self.parse_typ_mod()?,
                    }
                }
            },
            Some(Token::Ident(_)) => {
                self.prev_token();
                DataType::Other {
                    name: RawName::Name(self.parse_object_name()?),
                    typ_mod: self.parse_typ_mod()?,
                }
            }
            other => self.expected(self.peek_prev_pos(), "a data type name", other)?,
        };

        loop {
            match self.peek_token() {
                Some(Token::Keyword(LIST)) => {
                    self.next_token();
                    data_type = DataType::List(Box::new(data_type));
                }
                Some(Token::LBracket) => {
                    // Handle array suffixes. Note that `int[]`, `int[][][]`,
                    // and `int[2][2]` all parse to the same "int array" type.
                    self.next_token();
                    let _ = self.maybe_parse(|parser| parser.parse_number_value());
                    self.expect_token(&Token::RBracket)?;
                    if !matches!(data_type, DataType::Array(_)) {
                        data_type = DataType::Array(Box::new(data_type));
                    }
                }
                _ => break,
            }
        }
        Ok(data_type)
    }

    fn parse_typ_mod(&mut self) -> Result<Vec<u64>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let typ_mod = self.parse_comma_separated(Parser::parse_literal_uint)?;
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

    /// Parse a possibly qualified, possibly quoted identifier, e.g.
    /// `foo` or `myschema."table"`
    fn parse_object_name(&mut self) -> Result<UnresolvedObjectName, ParserError> {
        let mut idents = vec![];
        loop {
            idents.push(self.parse_identifier()?);
            if !self.consume_token(&Token::Dot) {
                break;
            }
        }
        Ok(UnresolvedObjectName(idents))
    }

    /// Parse a simple one-word identifier (possibly quoted, possibly a keyword)
    fn parse_identifier(&mut self) -> Result<Ident, ParserError> {
        match self.consume_identifier() {
            Some(id) => Ok(id),
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

    fn parse_map(&mut self) -> Result<DataType<Raw>, ParserError> {
        self.expect_token(&Token::LBracket)?;
        let key_type = Box::new(self.parse_data_type()?);
        self.expect_token(&Token::Op("=>".to_owned()))?;
        let value_type = Box::new(self.parse_data_type()?);
        self.expect_token(&Token::RBracket)?;
        Ok(DataType::Map {
            key_type,
            value_type,
        })
    }

    fn parse_delete(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(FROM)?;
        let table_name = self.parse_object_name()?;
        let selection = if self.parse_keyword(WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Delete(DeleteStatement {
            table_name,
            selection,
        }))
    }

    /// Parse a query expression, i.e. a `SELECT` statement optionally
    /// preceeded with some `WITH` CTE declarations and optionally followed
    /// by `ORDER BY`. Unlike some other parse_... methods, this one doesn't
    /// expect the initial keyword to be already consumed
    fn parse_query(&mut self) -> Result<Query<Raw>, ParserError> {
        self.check_descent(|parser| {
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
            Some(Token::Semicolon) | None => vec![],
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
            self.parse_options()?
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
        if !normal && variable.as_str().parse() == Ok(TIME) {
            self.expect_keyword(ZONE)?;
            variable = Ident::new("timezone");
            normal = true;
        }
        if normal {
            let token = self.peek_token();
            let value = match (self.parse_value(), token) {
                (Ok(value), _) => SetVariableValue::Literal(value),
                (Err(_), Some(Token::Keyword(kw))) => SetVariableValue::Ident(kw.into_ident()),
                (Err(_), Some(Token::Ident(id))) => SetVariableValue::Ident(Ident::new(id)),
                (Err(_), other) => self.expected(self.peek_pos(), "variable value", other)?,
            };
            Ok(Statement::SetVariable(SetVariableStatement {
                local: modifier == Some(LOCAL),
                variable,
                value,
            }))
        } else if variable.as_str().parse() == Ok(TRANSACTION) && modifier.is_none() {
            Ok(Statement::SetTransaction(SetTransactionStatement {
                modes: self.parse_transaction_modes()?,
            }))
        } else {
            self.expected(self.peek_pos(), "equals sign or TO", self.peek_token())
        }
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
                COLUMNS, FULL, INDEX, INDEXES, KEYS, OBJECTS, SCHEMAS, TABLES, TYPES,
            ])?;
            self.prev_token();
        }

        let full = self.parse_keyword(FULL);
        if full {
            if extended {
                self.expect_one_of_keywords(&[COLUMNS, OBJECTS, SCHEMAS, TABLES, TYPES])?;
            } else {
                self.expect_one_of_keywords(&[
                    COLUMNS,
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
            }
            self.prev_token();
        }

        let materialized = self.parse_keyword(MATERIALIZED);
        if materialized {
            self.expect_one_of_keywords(&[SOURCES, VIEWS])?;
            self.prev_token();
        }

        if self.parse_one_of_keywords(&[COLUMNS, FIELDS]).is_some() {
            self.parse_show_columns(extended, full)
        } else if let Some(object_type) = self.parse_one_of_keywords(&[
            OBJECTS, ROLES, SCHEMAS, SINKS, SOURCES, TABLES, TYPES, USERS, VIEWS,
        ]) {
            Ok(Statement::ShowObjects(ShowObjectsStatement {
                object_type: match object_type {
                    OBJECTS => ObjectType::Object,
                    ROLES | USERS => ObjectType::Role,
                    SCHEMAS => ObjectType::Schema,
                    SINKS => ObjectType::Sink,
                    SOURCES => ObjectType::Source,
                    TABLES => ObjectType::Table,
                    TYPES => ObjectType::Type,
                    VIEWS => ObjectType::View,
                    val => panic!(
                        "`parse_one_of_keywords` returned an impossible value: {}",
                        val
                    ),
                },
                extended,
                full,
                materialized,
                from: if self.parse_one_of_keywords(&[FROM, IN]).is_some() {
                    Some(self.parse_object_name()?)
                } else {
                    None
                },
                filter: self.parse_show_statement_filter()?,
            }))
        } else if self
            .parse_one_of_keywords(&[INDEX, INDEXES, KEYS])
            .is_some()
        {
            match self.parse_one_of_keywords(&[FROM, IN]) {
                Some(_) => {
                    let table_name = self.parse_object_name()?;
                    let filter = if self.parse_keyword(WHERE) {
                        Some(ShowStatementFilter::Where(self.parse_expr()?))
                    } else {
                        None
                    };
                    Ok(Statement::ShowIndexes(ShowIndexesStatement {
                        table_name,
                        extended,
                        filter,
                    }))
                }
                None => self.expected(
                    self.peek_pos(),
                    "FROM or IN after SHOW INDEXES",
                    self.peek_token(),
                ),
            }
        } else if self.parse_keywords(&[CREATE, VIEW]) {
            Ok(Statement::ShowCreateView(ShowCreateViewStatement {
                view_name: self.parse_object_name()?,
            }))
        } else if self.parse_keywords(&[CREATE, SOURCE]) {
            Ok(Statement::ShowCreateSource(ShowCreateSourceStatement {
                source_name: self.parse_object_name()?,
            }))
        } else if self.parse_keywords(&[CREATE, TABLE]) {
            Ok(Statement::ShowCreateTable(ShowCreateTableStatement {
                table_name: self.parse_object_name()?,
            }))
        } else if self.parse_keywords(&[CREATE, SINK]) {
            Ok(Statement::ShowCreateSink(ShowCreateSinkStatement {
                sink_name: self.parse_object_name()?,
            }))
        } else if self.parse_keywords(&[CREATE, INDEX]) {
            Ok(Statement::ShowCreateIndex(ShowCreateIndexStatement {
                index_name: self.parse_object_name()?,
            }))
        } else {
            let variable = if self.parse_keywords(&[TRANSACTION, ISOLATION, LEVEL]) {
                Ident::new("transaction_isolation")
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
        let table_name = self.parse_object_name()?;
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
            } else {
                let name = self.parse_object_name()?;
                self.expect_token(&Token::LParen)?;
                return Ok(TableFactor::Function {
                    name,
                    args: self.parse_optional_args()?,
                    alias: self.parse_optional_table_alias()?,
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
        } else if self.consume_token(&Token::LBracket) {
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

            Ok(TableFactor::Table {
                name: RawName::Id(id, name),
                alias: self.parse_optional_table_alias()?,
            })
        } else {
            let name = self.parse_object_name()?;
            if self.consume_token(&Token::LParen) {
                Ok(TableFactor::Function {
                    name,
                    args: self.parse_optional_args()?,
                    alias: self.parse_optional_table_alias()?,
                })
            } else {
                Ok(TableFactor::Table {
                    name: RawName::Name(name),
                    alias: self.parse_optional_table_alias()?,
                })
            }
        }
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
        let table_name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        let source = if self.parse_keywords(&[DEFAULT, VALUES]) {
            InsertSource::DefaultValues
        } else {
            InsertSource::Query(self.parse_query()?)
        };
        Ok(Statement::Insert(InsertStatement {
            table_name,
            columns,
            source,
        }))
    }

    fn parse_update(&mut self) -> Result<Statement<Raw>, ParserError> {
        let table_name = self.parse_object_name()?;
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

    fn parse_optional_args(&mut self) -> Result<FunctionArgs<Raw>, ParserError> {
        if self.consume_token(&Token::Star) {
            self.expect_token(&Token::RParen)?;
            Ok(FunctionArgs::Star)
        } else if self.consume_token(&Token::RParen) {
            Ok(FunctionArgs::Args(vec![]))
        } else {
            let args = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
            Ok(FunctionArgs::Args(args))
        }
    }

    /// Parse `AS OF`, if present.
    fn parse_optional_as_of(&mut self) -> Result<Option<Expr<Raw>>, ParserError> {
        if self.parse_keyword(AS) {
            self.expect_keyword(OF)?;
            match self.parse_expr() {
                Ok(expr) => Ok(Some(expr)),
                Err(e) => {
                    self.expected(e.pos, "a timestamp value after 'AS OF'", self.peek_token())
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

    /// Parse an expression, optionally followed by ASC or DESC (used in ORDER BY)
    fn parse_order_by_expr(&mut self) -> Result<OrderByExpr<Raw>, ParserError> {
        let expr = self.parse_expr()?;

        let asc = if self.parse_keyword(ASC) {
            Some(true)
        } else if self.parse_keyword(DESC) {
            Some(false)
        } else {
            None
        };
        Ok(OrderByExpr { expr, asc })
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
        let name = self.parse_object_name()?;
        let options = self.parse_opt_with_options()?;
        let as_of = self.parse_optional_as_of()?;
        Ok(Statement::Tail(TailStatement {
            name,
            options,
            as_of,
        }))
    }

    /// Parse an `EXPLAIN` statement, assuming that the `EXPLAIN` token
    /// has already been consumed.
    fn parse_explain(&mut self) -> Result<Statement<Raw>, ParserError> {
        // (TYPED)?
        let options = ExplainOptions {
            typed: self.parse_keyword(TYPED),
        };

        // (RAW | DECORRELATED | OPTIMIZED)? PLAN
        let stage = match self.parse_one_of_keywords(&[RAW, DECORRELATED, OPTIMIZED, PLAN]) {
            Some(RAW) => {
                self.expect_keywords(&[PLAN, FOR])?;
                ExplainStage::RawPlan
            }
            Some(DECORRELATED) => {
                self.expect_keywords(&[PLAN, FOR])?;
                ExplainStage::DecorrelatedPlan
            }
            Some(OPTIMIZED) => {
                self.expect_keywords(&[PLAN, FOR])?;
                ExplainStage::OptimizedPlan
            }
            Some(PLAN) => {
                self.expect_keyword(FOR)?;
                ExplainStage::OptimizedPlan
            }
            None => ExplainStage::OptimizedPlan,
            _ => unreachable!(),
        };

        // VIEW view_name | query
        let explainee = if self.parse_keyword(VIEW) {
            Explainee::View(self.parse_object_name()?)
        } else {
            Explainee::Query(self.parse_query()?)
        };

        Ok(Statement::Explain(ExplainStatement {
            stage,
            explainee,
            options,
        }))
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
        let options = self.parse_opt_with_options()?;
        Ok(Statement::Fetch(FetchStatement {
            name,
            count,
            options,
        }))
    }

    /// Checks whether it is safe to descend another layer of nesting in the
    /// parse tree, and calls `f` if so.
    ///
    /// The nature of a recursive descent parser, like this SQL parser, is that
    /// deeply nested queries can easily cause a stack overflow, as each level
    /// of nesting requires a new stack frame of a few dozen bytes, and those
    /// bytes add up over time. That means that user input can trivially cause a
    /// process crash, which does not make for a good user experience.
    ///
    /// This method uses the [`stacker`] crate to automatically grow the stack
    /// as necessary. It also enforces a hard but arbitrary limit on the maximum
    /// depth, as it's good practice to have *some* limit. Real-world queries
    /// tend not to have more than a dozen or so layers of nesting.
    ///
    /// Calls to this function must be manually inserted in the parser at any
    /// point that mutual recursion occurs; i.e., whenever parsing of a nested
    /// expression or nested SQL query begins.
    fn check_descent<F, R>(&mut self, f: F) -> Result<R, ParserError>
    where
        F: FnOnce(&mut Parser) -> Result<R, ParserError>,
    {
        // NOTE(benesch): this recursion limit was chosen based on the maximum
        // amount of nesting I've ever seen in a production SQL query (i.e.,
        // about a dozen) times a healthy factor to be conservative.
        const RECURSION_LIMIT: usize = 128;

        // The red zone is the amount of stack space that must be available on
        // the current stack in order to call `f` without allocating a new
        // stack.
        const STACK_RED_ZONE: usize = 32 << 10; // 32KiB

        // The size of any freshly-allocated stacks. It was chosen to match the
        // default stack size for threads in Rust.
        const STACK_SIZE: usize = 2 << 20; // 2MiB

        if self.depth > RECURSION_LIMIT {
            return parser_err!(
                self,
                self.peek_prev_pos(),
                "query exceeds nested expression limit of {}",
                RECURSION_LIMIT
            );
        }

        self.depth += 1;
        let out = stacker::maybe_grow(STACK_RED_ZONE, STACK_SIZE, || f(self));
        self.depth -= 1;

        out
    }
}
