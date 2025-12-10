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

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use IsLateral::*;
use IsOptional::*;
use bytesize::ByteSize;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::option::OptionExt;
use mz_ore::stack::{CheckedRecursion, RecursionGuard, RecursionLimitError};
use mz_sql_lexer::keywords::*;
use mz_sql_lexer::lexer::{self, IdentString, LexerError, PosToken, Token};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::ast::display::AstDisplay;
use crate::ast::*;
use crate::ident;

// NOTE(benesch): this recursion limit was chosen based on the maximum amount of
// nesting I've ever seen in a production SQL query (i.e., about a dozen) times
// a healthy factor to be conservative.
const RECURSION_LIMIT: usize = 128;

/// Maximum allowed size for a batch of statements in bytes: 1MB.
pub const MAX_STATEMENT_BATCH_SIZE: usize = 1_000_000;

/// Keywords that indicate the start of a (sub)query.
const QUERY_START_KEYWORDS: &[Keyword] = &[WITH, SELECT, SHOW, TABLE, VALUES];

/// Keywords that indicate the start of an `ANY` or `ALL` subquery operation.
const ANY_ALL_KEYWORDS: &[Keyword] = &[ANY, ALL, SOME];

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($parser:expr, $pos:expr, $MSG:expr) => {
        Err($parser.error($pos, $MSG.to_string()))
    };
    ($parser:expr, $pos:expr, $($arg:tt)*) => {
        Err($parser.error($pos, format!($($arg)*)))
    };
}

/// The result of successfully parsing a statement:
/// both the AST and the SQL text that it corresponds to
#[derive(Debug, Clone)]
pub struct StatementParseResult<'a> {
    pub ast: Statement<Raw>,
    pub sql: &'a str,
}

impl<'a> StatementParseResult<'a> {
    pub fn new(ast: Statement<Raw>, sql: &'a str) -> Self {
        Self { ast, sql }
    }
}

trait ParserStatementErrorMapper<T> {
    /// Wrap a `ParserError` within a `ParserStatementError` alongside the provided `StatementKind`
    fn map_parser_err(self, statement_kind: StatementKind) -> Result<T, ParserStatementError>;

    /// Wrap a `ParserError` within a `ParserStatementError` without an accompanying
    /// `StatementKind`.
    ///
    /// This should be used when we do not know what specific statement is being parsed.
    fn map_no_statement_parser_err(self) -> Result<T, ParserStatementError>;
}

impl<T> ParserStatementErrorMapper<T> for Result<T, ParserError> {
    fn map_parser_err(self, statement: StatementKind) -> Result<T, ParserStatementError> {
        self.map_err(|error| ParserStatementError {
            error,
            statement: Some(statement),
        })
    }

    fn map_no_statement_parser_err(self) -> Result<T, ParserStatementError> {
        self.map_err(|error| ParserStatementError {
            error,
            statement: None,
        })
    }
}

/// Parses a SQL string containing zero or more SQL statements.
/// Statements larger than [`MAX_STATEMENT_BATCH_SIZE`] are rejected.
///
/// The outer Result is for errors related to the statement size. The inner Result is for
/// errors during the parsing.
#[mz_ore::instrument(target = "compiler", level = "trace", name = "sql_to_ast")]
pub fn parse_statements_with_limit(
    sql: &str,
) -> Result<Result<Vec<StatementParseResult<'_>>, ParserStatementError>, String> {
    if sql.bytes().count() > MAX_STATEMENT_BATCH_SIZE {
        return Err(format!(
            "statement batch size cannot exceed {}",
            ByteSize::b(u64::cast_from(MAX_STATEMENT_BATCH_SIZE))
        ));
    }
    Ok(parse_statements(sql))
}

/// Parses a SQL string containing zero or more SQL statements.
#[mz_ore::instrument(target = "compiler", level = "trace", name = "sql_to_ast")]
pub fn parse_statements(sql: &str) -> Result<Vec<StatementParseResult<'_>>, ParserStatementError> {
    let tokens = lexer::lex(sql).map_err(|error| ParserStatementError {
        error: error.into(),
        statement: None,
    })?;
    let res = Parser::new(sql, tokens).parse_statements();
    // Don't trace sensitive raw sql, so we can only trace after parsing, and then can only output
    // redacted statements.
    debug!("{:?}", {
        match &res {
            Ok(stmts) => stmts
                .iter()
                .map(|stmt| stmt.ast.to_ast_string_redacted())
                .join("; "),
            // Errors can leak sensitive SQL.
            Err(_) => "parse error".to_string(),
        }
    });
    res
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

/// Parses a string containing a comma-separated list of identifiers and
/// returns their underlying string values.
///
/// This is analogous to the `SplitIdentifierString` function in PostgreSQL.
pub fn split_identifier_string(s: &str) -> Result<Vec<String>, ParserError> {
    // SplitIdentifierString ignores leading and trailing whitespace, and
    // accepts empty input as a 0-length result.
    if s.trim().is_empty() {
        Ok(vec![])
    } else {
        let tokens = lexer::lex(s)?;
        let mut parser = Parser::new(s, tokens);
        let values = parser.parse_comma_separated(Parser::parse_set_variable_value)?;
        Ok(values
            .into_iter()
            .map(|v| v.into_unquoted_value())
            .collect())
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

enum IsLateral {
    Lateral,
    NotLateral,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParserStatementError {
    /// The underlying error
    pub error: ParserError,
    /// The kind of statement erroring
    pub statement: Option<StatementKind>,
}

impl Error for ParserStatementError {}

impl fmt::Display for ParserStatementError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.error.to_string())
    }
}

impl From<LexerError> for ParserError {
    fn from(value: LexerError) -> Self {
        ParserError {
            message: value.message,
            pos: value.pos,
        }
    }
}

impl From<Keyword> for Ident {
    fn from(value: Keyword) -> Ident {
        // Note: all keywords are known to be less than our max length.
        Ident::new_unchecked(value.as_str().to_lowercase())
    }
}

/// SQL Parser
struct Parser<'a> {
    sql: &'a str,
    tokens: Vec<PosToken>,
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
    fn new(sql: &'a str, tokens: Vec<PosToken>) -> Self {
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

    fn parse_statements(&mut self) -> Result<Vec<StatementParseResult<'a>>, ParserStatementError> {
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
                return self
                    .expected(self.peek_pos(), "end of statement", self.peek_token())
                    .map_no_statement_parser_err();
            }

            let s = self.parse_statement()?;
            stmts.push(s);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }
    /// Parse a single top-level statement (such as SELECT, INSERT, CREATE, etc.),
    /// stopping before the statement separator, if any. Returns the parsed statement and the SQL
    /// fragment corresponding to it.
    fn parse_statement(&mut self) -> Result<StatementParseResult<'a>, ParserStatementError> {
        let before = self.peek_pos();
        let statement = self.parse_statement_inner()?;
        let after = self.peek_pos();
        Ok(StatementParseResult::new(
            statement,
            self.sql[before..after].trim(),
        ))
    }

    /// Parse a single top-level statement (such as SELECT, INSERT, CREATE, etc.),
    /// stopping before the statement separator, if any.
    fn parse_statement_inner(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        match self.next_token() {
            Some(t) => match t {
                Token::Keyword(CREATE) => Ok(self.parse_create()?),
                Token::Keyword(DISCARD) => Ok(self
                    .parse_discard()
                    .map_parser_err(StatementKind::Discard)?),
                Token::Keyword(DROP) => Ok(self.parse_drop()?),
                Token::Keyword(DELETE) => {
                    Ok(self.parse_delete().map_parser_err(StatementKind::Delete)?)
                }
                Token::Keyword(INSERT) => {
                    Ok(self.parse_insert().map_parser_err(StatementKind::Insert)?)
                }
                Token::Keyword(UPDATE) => {
                    Ok(self.parse_update().map_parser_err(StatementKind::Update)?)
                }
                Token::Keyword(ALTER) => Ok(self.parse_alter()?),
                Token::Keyword(COPY) => Ok(self.parse_copy()?),
                Token::Keyword(SET) => Ok(self.parse_set()?),
                Token::Keyword(RESET) => Ok(self
                    .parse_reset()
                    .map_parser_err(StatementKind::ResetVariable)?),
                Token::Keyword(SHOW) => Ok(Statement::Show(
                    self.parse_show().map_parser_err(StatementKind::Show)?,
                )),
                Token::Keyword(START) => Ok(self
                    .parse_start_transaction()
                    .map_parser_err(StatementKind::StartTransaction)?),
                // `BEGIN` is a nonstandard but common alias for the
                // standard `START TRANSACTION` statement. It is supported
                // by at least PostgreSQL and MySQL.
                Token::Keyword(BEGIN) => Ok(self
                    .parse_begin()
                    .map_parser_err(StatementKind::StartTransaction)?),
                Token::Keyword(COMMIT) => {
                    Ok(self.parse_commit().map_parser_err(StatementKind::Commit)?)
                }
                Token::Keyword(ROLLBACK) => Ok(self
                    .parse_rollback()
                    .map_parser_err(StatementKind::Rollback)?),
                Token::Keyword(TAIL) => {
                    Ok(self.parse_tail().map_parser_err(StatementKind::Subscribe)?)
                }
                Token::Keyword(SUBSCRIBE) => Ok(self
                    .parse_subscribe()
                    .map_parser_err(StatementKind::Subscribe)?),
                Token::Keyword(EXPLAIN) => Ok(self.parse_explain()?),
                Token::Keyword(DECLARE) => Ok(self.parse_declare()?),
                Token::Keyword(FETCH) => {
                    Ok(self.parse_fetch().map_parser_err(StatementKind::Fetch)?)
                }
                Token::Keyword(CLOSE) => {
                    Ok(self.parse_close().map_parser_err(StatementKind::Close)?)
                }
                Token::Keyword(PREPARE) => Ok(self.parse_prepare()?),
                Token::Keyword(EXECUTE) => Ok(self
                    .parse_execute()
                    .map_parser_err(StatementKind::Execute)?),
                Token::Keyword(DEALLOCATE) => Ok(self
                    .parse_deallocate()
                    .map_parser_err(StatementKind::Deallocate)?),
                Token::Keyword(RAISE) => {
                    Ok(self.parse_raise().map_parser_err(StatementKind::Raise)?)
                }
                Token::Keyword(GRANT) => Ok(self.parse_grant()?),
                Token::Keyword(REVOKE) => Ok(self.parse_revoke()?),
                Token::Keyword(REASSIGN) => Ok(self
                    .parse_reassign_owned()
                    .map_parser_err(StatementKind::ReassignOwned)?),
                Token::Keyword(INSPECT) => Ok(Statement::Show(
                    self.parse_inspect().map_no_statement_parser_err()?,
                )),
                Token::Keyword(VALIDATE) => Ok(self
                    .parse_validate()
                    .map_parser_err(StatementKind::ValidateConnection)?),
                Token::Keyword(COMMENT) => Ok(self
                    .parse_comment()
                    .map_parser_err(StatementKind::Comment)?),
                Token::Keyword(k) if QUERY_START_KEYWORDS.contains(&k) => {
                    self.prev_token();
                    Ok(Statement::Select(
                        self.parse_select_statement()
                            .map_parser_err(StatementKind::Select)?,
                    ))
                }
                Token::Keyword(kw) => parser_err!(
                    self,
                    self.peek_prev_pos(),
                    format!("Unexpected keyword {} at the beginning of a statement", kw)
                )
                .map_no_statement_parser_err(),
                Token::LParen => {
                    self.prev_token();
                    Ok(Statement::Select(SelectStatement {
                        query: self.parse_query().map_parser_err(StatementKind::Select)?,
                        as_of: None, // Only the outermost SELECT may have an AS OF clause.
                    }))
                }
                unexpected => self
                    .expected(
                        self.peek_prev_pos(),
                        "a keyword at the beginning of a statement",
                        Some(unexpected),
                    )
                    .map_no_statement_parser_err(),
            },
            None => self
                .expected(self.peek_prev_pos(), "SQL statement", None)
                .map_no_statement_parser_err(),
        }
    }

    /// Parse a new expression
    fn parse_expr(&mut self) -> Result<Expr<Raw>, ParserError> {
        self.parse_subexpr(Precedence::Zero)
    }

    /// Parse tokens until the precedence decreases
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
        //
        // Note: the maybe! block here does swallow valid parsing errors
        // See <https://github.com/MaterializeInc/incidents-and-escalations/issues/90> for more details
        maybe!(self.maybe_parse(|parser| {
            let data_type = parser.parse_data_type()?;
            if data_type.to_string().as_str() == "interval" {
                Ok(Expr::Value(Value::Interval(parser.parse_interval_value()?)))
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
            Token::LBracket => {
                self.prev_token();
                let function = self.parse_named_function()?;
                Ok(Expr::Function(function))
            }
            Token::Keyword(TRUE) | Token::Keyword(FALSE) | Token::Keyword(NULL) => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            Token::Keyword(ARRAY) => self.parse_array(),
            Token::Keyword(LIST) => self.parse_list(),
            Token::Keyword(MAP) => self.parse_map(),
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
            Token::Keyword(NOT) => Ok(Expr::Not {
                expr: Box::new(self.parse_subexpr(Precedence::PrefixNot)?),
            }),
            Token::Keyword(ROW) if self.peek_token() == Some(Token::LParen) => {
                self.parse_row_expr()
            }
            Token::Keyword(TRIM) => self.parse_trim_expr(),
            Token::Keyword(POSITION) if self.peek_token() == Some(Token::LParen) => {
                self.parse_position_expr()
            }
            Token::Keyword(NORMALIZE) => self.parse_normalize_expr(),
            Token::Keyword(SUBSTRING) => self.parse_substring_expr(),
            Token::Keyword(kw) if kw.is_always_reserved() => {
                return Err(self.error(
                    self.peek_prev_pos(),
                    format!("expected expression, but found reserved keyword: {kw}"),
                ));
            }
            Token::Keyword(id) => self.parse_qualified_identifier(id.into()),
            Token::Ident(id) => self.parse_qualified_identifier(self.new_identifier(id)?),
            Token::Op(op) if op == "-" => {
                if let Some(Token::Number(n)) = self.peek_token() {
                    let n = match n.parse::<f64>() {
                        Ok(n) => n,
                        Err(_) => {
                            return Err(
                                self.error(self.peek_prev_pos(), format!("invalid number {}", n))
                            );
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
                let expr = self.parse_parenthesized_fragment()?.into_expr();
                self.expect_token(&Token::RParen)?;
                Ok(expr)
            }
            unexpected => self.expected(self.peek_prev_pos(), "an expression", Some(unexpected)),
        }?;

        Ok(expr)
    }

    /// Parses an expression list that appears in parentheses, like `(1 + 1)`,
    /// `(SELECT 1)`, or `(1, 2)`. Assumes that the opening parenthesis has
    /// already been parsed. Parses up to the closing parenthesis without
    /// consuming it.
    fn parse_parenthesized_fragment(&mut self) -> Result<ParenthesizedFragment, ParserError> {
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
        // of a set operator implies that the parentheses belonged to the
        // subquery; otherwise, they belonged to the expression.
        //
        // See also PostgreSQL's comments on the matter:
        // https://github.com/postgres/postgres/blob/42c63ab/src/backend/parser/gram.y#L11125-L11136
        //
        // Each call of this function handles one layer of parentheses. Before
        // every call, the parser must be positioned after an opening
        // parenthesis; upon non-error return, the parser will be positioned
        // before the corresponding close parenthesis. Somewhat weirdly, the
        // returned expression semantically includes the opening/closing
        // parentheses, even though this function is not responsible for parsing
        // them.

        if self.peek_one_of_keywords(QUERY_START_KEYWORDS) {
            // Easy case one: unambiguously a subquery.
            Ok(ParenthesizedFragment::Query(self.parse_query()?))
        } else if !self.consume_token(&Token::LParen) {
            // Easy case two: unambiguously an expression.
            let exprs = self.parse_comma_separated(Parser::parse_expr)?;
            Ok(ParenthesizedFragment::Exprs(exprs))
        } else {
            // Hard case: we have an open parenthesis, and we need to decide
            // whether it belongs to the inner expression or the outer
            // expression.

            // Parse to the closing parenthesis.
            let fragment = self.checked_recur_mut(Parser::parse_parenthesized_fragment)?;
            self.expect_token(&Token::RParen)?;

            // Decide if we need to associate any tokens after the closing
            // parenthesis with what we've parsed so far.
            match (fragment, self.peek_token()) {
                // We have a subquery and the next token is a set operator or a
                // closing parenthesis. That implies we have a partially-parsed
                // subquery (or a syntax error). Hop into parsing a set
                // expression where our subquery is the LHS of the set operator.
                (
                    ParenthesizedFragment::Query(query),
                    Some(Token::RParen | Token::Keyword(UNION | INTERSECT | EXCEPT)),
                ) => {
                    let query = SetExpr::Query(Box::new(query));
                    let ctes = CteBlock::empty();
                    let body = self.parse_query_body_seeded(SetPrecedence::Zero, query)?;
                    Ok(ParenthesizedFragment::Query(
                        self.parse_query_tail(ctes, body)?,
                    ))
                }

                // Otherwise, we have an expression. It may be only partially
                // parsed. Hop into parsing an expression where `fragment` is
                // the expression prefix. Then parse any additional
                // comma-separated expressions.
                (fragment, _) => {
                    let prefix = fragment.into_expr();
                    let expr = self.parse_subexpr_seeded(Precedence::Zero, prefix)?;
                    let mut exprs = vec![expr];
                    while self.consume_token(&Token::Comma) {
                        exprs.push(self.parse_expr()?);
                    }
                    Ok(ParenthesizedFragment::Exprs(exprs))
                }
            }
        }
    }

    fn parse_function(&mut self, name: RawItemName) -> Result<Function<Raw>, ParserError> {
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
        let over =
            if self.peek_keyword(OVER) || self.peek_keyword(IGNORE) || self.peek_keyword(RESPECT) {
                // TBD: support window names (`OVER mywin`) in place of inline specification
                // https://github.com/MaterializeInc/database-issues/issues/5882

                let ignore_nulls = self.parse_keywords(&[IGNORE, NULLS]);
                let respect_nulls = self.parse_keywords(&[RESPECT, NULLS]);
                self.expect_keyword(OVER)?;

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
                    ignore_nulls,
                    respect_nulls,
                })
            } else {
                None
            };

        Ok(Function {
            name,
            args,
            filter,
            over,
            distinct,
        })
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
        // We are potentially rewriting an expression like
        //     CAST(<expr> OP <expr> AS <type>)
        // to
        //     <expr> OP <expr>::<type>
        // (because we print Expr::Cast always as a Postgres-style cast, i.e. `::`)
        // which could incorrectly change the meaning of the expression
        // as the `::` binds tightly. To be safe, we wrap the inner
        // expression in parentheses
        //    (<expr> OP <expr>)::<type>
        // unless the inner expression is of a kind that we know is
        // safe to follow with a `::` without wrapping.
        if !matches!(
            expr,
            Expr::Nested(_)
                | Expr::Value(_)
                | Expr::Cast { .. }
                | Expr::Function { .. }
                | Expr::Identifier { .. }
                | Expr::Collate { .. }
                | Expr::HomogenizingFunction { .. }
                | Expr::NullIf { .. }
                | Expr::Subquery { .. }
                | Expr::Parameter(..)
        ) {
            Ok(Expr::Cast {
                expr: Box::new(Expr::Nested(Box::new(expr))),
                data_type,
            })
        } else {
            Ok(Expr::Cast {
                expr: Box::new(expr),
                data_type,
            })
        }
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

    // Parse calls to extract(), which can take the form:
    // - extract(field from 'interval')
    fn parse_extract_expr(&mut self) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let field = match self.next_token() {
            Some(Token::Keyword(kw)) => Ident::from(kw).into_string(),
            Some(Token::Ident(id)) => self.new_identifier(id)?.into_string(),
            Some(Token::String(s)) => s,
            t => self.expected(self.peek_prev_pos(), "extract field token", t)?,
        };
        self.expect_keyword(FROM)?;
        let expr = self.parse_expr()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Function(Function {
            name: RawItemName::Name(UnresolvedItemName::unqualified(ident!("extract"))),
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
            None | Some(BOTH) => ident!("btrim"),
            Some(LEADING) => ident!("ltrim"),
            Some(TRAILING) => ident!("rtrim"),
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
            name: RawItemName::Name(UnresolvedItemName::unqualified(name)),
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
            name: RawItemName::Name(UnresolvedItemName::unqualified(ident!("position"))),
            args: FunctionArgs::args(vec![needle, haystack]),
            filter: None,
            over: None,
            distinct: false,
        }))
    }

    /// Parse calls to normalize(), which can take the form:
    /// - normalize('string')
    /// - normalize('string', NFC)
    /// - normalize('string', NFD)
    /// - normalize('string', NFKC)
    /// - normalize('string', NFKD)
    fn parse_normalize_expr(&mut self) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;

        let args = if self.consume_token(&Token::Comma) {
            let form = self
                .expect_one_of_keywords(&[NFC, NFD, NFKC, NFKD])?
                .as_str();
            vec![expr, Expr::Value(Value::String(form.to_owned()))]
        } else {
            vec![expr, Expr::Value(Value::String("NFC".to_owned()))]
        };

        self.expect_token(&Token::RParen)?;
        Ok(Expr::Function(Function {
            name: RawItemName::Name(UnresolvedItemName::unqualified(ident!("normalize"))),
            args: FunctionArgs::args(args),
            filter: None,
            over: None,
            distinct: false,
        }))
    }

    /// Parse an INTERVAL literal.
    ///
    /// Some syntactically valid intervals:
    ///
    ///   - `INTERVAL '1' DAY`
    ///   - `INTERVAL '1-1' YEAR TO MONTH`
    ///   - `INTERVAL '1' SECOND`
    ///   - `INTERVAL '1:1' MINUTE TO SECOND
    ///   - `INTERVAL '1:1:1.1' HOUR TO SECOND (5)`
    ///   - `INTERVAL '1.111' SECOND (2)`
    ///
    fn parse_interval_value(&mut self) -> Result<IntervalValue, ParserError> {
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
        Ok(IntervalValue {
            value,
            precision_high,
            precision_low,
            fsec_max_precision,
        })
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
            if let Some(kw) = self.parse_one_of_keywords(ANY_ALL_KEYWORDS) {
                self.parse_any_all(expr, op, kw)
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
                        self.parse_one_of_keywords(&[NULL, TRUE, FALSE, UNKNOWN, DISTINCT])
                    {
                        Ok(Expr::IsExpr {
                            expr: Box::new(expr),
                            negated,
                            construct: match construct {
                                NULL => IsExprConstruct::Null,
                                TRUE => IsExprConstruct::True,
                                FALSE => IsExprConstruct::False,
                                UNKNOWN => IsExprConstruct::Unknown,
                                DISTINCT => {
                                    self.expect_keyword(FROM)?;
                                    let expr = self.parse_expr()?;
                                    IsExprConstruct::DistinctFrom(Box::new(expr))
                                }
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
                        name: RawItemName::Name(UnresolvedItemName::unqualified(ident!(
                            "timezone"
                        ))),
                        args: FunctionArgs::args(vec![self.parse_subexpr(precedence)?, expr]),
                        filter: None,
                        over: None,
                        distinct: false,
                    }))
                }
                COLLATE => Ok(Expr::Collate {
                    expr: Box::new(expr),
                    collation: self.parse_item_name()?,
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
                    field: self.new_identifier(id)?,
                }),
                // Per PostgreSQL, even reserved keywords are ok after a field
                // access operator.
                Some(Token::Keyword(kw)) => Ok(Expr::FieldAccess {
                    expr: Box::new(expr),
                    field: kw.into(),
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

        // If the expression that is being cast can end with a type name, then let's parenthesize
        // it. Otherwise, the `[...]` would melt into the type name (making it an array type).
        // Specifically, the only expressions whose printing can end with a type name are casts, so
        // check for that.
        if matches!(expr, Expr::Cast { .. }) {
            Ok(Expr::Subscript {
                expr: Box::new(Expr::Nested(Box::new(expr))),
                positions,
            })
        } else {
            Ok(Expr::Subscript {
                expr: Box::new(expr),
                positions,
            })
        }
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
            name: RawItemName::Name(UnresolvedItemName::unqualified(ident!("substring"))),
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
        let op = loop {
            match self.next_token() {
                Some(Token::Keyword(kw)) => namespace.push(kw.into()),
                Some(Token::Ident(id)) => namespace.push(self.new_identifier(id)?),
                Some(Token::Op(op)) => break op,
                Some(Token::Star) => break "*".to_string(),
                tok => self.expected(self.peek_prev_pos(), "operator", tok)?,
            }
            self.expect_token(&Token::Dot)?;
        };
        Ok(Op {
            namespace: Some(namespace),
            op,
        })
    }

    /// Parses an `ANY`, `ALL`, or `SOME` operation, starting after the `ANY`,
    /// `ALL`, or `SOME` keyword.
    fn parse_any_all(
        &mut self,
        left: Expr<Raw>,
        op: Op,
        kw: Keyword,
    ) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;

        let expr = match self.parse_parenthesized_fragment()? {
            ParenthesizedFragment::Exprs(exprs) => {
                if exprs.len() > 1 {
                    return parser_err!(
                        self,
                        self.peek_pos(),
                        "{kw} requires a single expression or subquery, not an expression list",
                    );
                }
                let right = exprs.into_element();
                if kw == ALL {
                    Expr::AllExpr {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    }
                } else {
                    Expr::AnyExpr {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    }
                }
            }
            ParenthesizedFragment::Query(subquery) => {
                if kw == ALL {
                    Expr::AllSubquery {
                        left: Box::new(left),
                        op,
                        right: Box::new(subquery),
                    }
                } else {
                    Expr::AnySubquery {
                        left: Box::new(left),
                        op,
                        right: Box::new(subquery),
                    }
                }
            }
        };

        self.expect_token(&Token::RParen)?;

        Ok(expr)
    }

    /// Parses the parens following the `[ NOT ] IN` operator
    fn parse_in(&mut self, expr: Expr<Raw>, negated: bool) -> Result<Expr<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let in_op = match self.parse_parenthesized_fragment()? {
            ParenthesizedFragment::Exprs(list) => Expr::InList {
                expr: Box::new(expr),
                list,
                negated,
            },
            ParenthesizedFragment::Query(subquery) => Expr::InSubquery {
                expr: Box::new(expr),
                subquery: Box::new(subquery),
                negated,
            },
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
        if let Some(kw) = self.parse_one_of_keywords(ANY_ALL_KEYWORDS) {
            let op = match (case_insensitive, negated) {
                (false, false) => "~~",
                (false, true) => "!~~",
                (true, false) => "~~*",
                (true, true) => "!~~*",
            };
            return self.parse_any_all(expr, Op::bare(op), kw);
        }
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

    fn peek_keyword(&self, kw: Keyword) -> bool {
        match self.peek_token() {
            Some(Token::Keyword(k)) => k == kw,
            _ => false,
        }
    }

    fn peek_keywords(&self, keywords: &[Keyword]) -> bool {
        self.peek_keywords_from(0, keywords)
    }

    fn peek_keywords_from(&self, start: usize, keywords: &[Keyword]) -> bool {
        for (i, keyword) in keywords.iter().enumerate() {
            match self.peek_nth_token(start + i) {
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

    fn peek_one_of_keywords(&self, kws: &[Keyword]) -> bool {
        match self.peek_token() {
            Some(Token::Keyword(k)) => kws.contains(&k),
            _ => false,
        }
    }

    /// Returns whether the sequence of keywords is found at any point before
    /// the end of the unprocessed tokens.
    fn peek_keywords_lookahead(&self, keywords: &[Keyword]) -> bool {
        let mut index = 0;
        while index < self.tokens.len() {
            if self.peek_keywords_from(index, keywords) {
                return true;
            }
            index += 1;
        }
        false
    }

    /// Return the nth token that has not yet been processed.
    fn peek_nth_token(&self, n: usize) -> Option<Token> {
        self.tokens
            .get(self.index + n)
            .map(|token| token.kind.clone())
    }

    /// Return the next token that has not yet been processed, or None if
    /// reached end-of-file, and mark it as processed. OK to call repeatedly
    /// after reaching EOF.
    fn next_token(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.index).map(|token| token.kind.clone());
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
            Some(token) => token.offset,
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
            Some(token) => token.offset,
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
            Some(Token::Keyword(k)) if kws.contains(&k) => {
                self.next_token();
                Some(k)
            }
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
                format!("one of {}", keywords.iter().join(" or ")),
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

    /// Bail out if the current token is not an expected token, or consume it if it is
    fn expect_token(&mut self, expected: &Token) -> Result<(), ParserError> {
        if self.consume_token(expected) {
            Ok(())
        } else {
            self.expected(self.peek_pos(), expected, self.peek_token())
        }
    }

    /// Bail out if the current token is not one of the expected tokens, or consume it if it is
    fn expect_one_of_tokens(&mut self, tokens: &[Token]) -> Result<Token, ParserError> {
        match self.peek_token() {
            Some(t) if tokens.iter().find(|token| t == **token).is_some() => {
                let _ = self.next_token();
                Ok(t)
            }
            _ => self.expected(
                self.peek_pos(),
                format!("one of {}", tokens.iter().join(" or ")),
                self.peek_token(),
            ),
        }
    }

    /// Bail out if the current token is not an expected keyword or token, or consume it if it is
    fn expect_keyword_or_token(
        &mut self,
        expected_keyword: Keyword,
        expected_token: &Token,
    ) -> Result<(), ParserError> {
        if self.parse_keyword(expected_keyword) || self.consume_token(expected_token) {
            Ok(())
        } else {
            self.expected(
                self.peek_pos(),
                format!("{expected_keyword} or {expected_token}"),
                self.peek_token(),
            )
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
    fn parse_create(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        if self.peek_keyword(DATABASE) {
            self.parse_create_database()
                .map_parser_err(StatementKind::CreateDatabase)
        } else if self.peek_keyword(SCHEMA) {
            self.parse_create_schema()
                .map_parser_err(StatementKind::CreateSchema)
        } else if self.peek_keyword(SINK) {
            self.parse_create_sink()
                .map_parser_err(StatementKind::CreateSink)
        } else if self.peek_keyword(TYPE) {
            self.parse_create_type()
                .map_parser_err(StatementKind::CreateType)
        } else if self.peek_keyword(ROLE) {
            self.parse_create_role()
                .map_parser_err(StatementKind::CreateRole)
        } else if self.peek_keyword(CLUSTER) {
            self.next_token();
            if self.peek_keyword(REPLICA) {
                self.parse_create_cluster_replica()
                    .map_parser_err(StatementKind::CreateClusterReplica)
            } else {
                self.parse_create_cluster()
                    .map_parser_err(StatementKind::CreateCluster)
            }
        } else if self.peek_keyword(INDEX) || self.peek_keywords(&[DEFAULT, INDEX]) {
            self.parse_create_index()
                .map_parser_err(StatementKind::CreateIndex)
        } else if self.peek_keyword(SOURCE) {
            self.parse_create_source()
                .map_parser_err(StatementKind::CreateSource)
        } else if self.peek_keyword(SUBSOURCE) {
            self.parse_create_subsource()
                .map_parser_err(StatementKind::CreateSubsource)
        } else if self.peek_keyword(TABLE)
            || self.peek_keywords(&[TEMP, TABLE])
            || self.peek_keywords(&[TEMPORARY, TABLE])
        {
            if self.peek_keywords_lookahead(&[FROM, SOURCE])
                || self.peek_keywords_lookahead(&[FROM, WEBHOOK])
            {
                self.parse_create_table_from_source()
                    .map_parser_err(StatementKind::CreateTableFromSource)
            } else {
                self.parse_create_table()
                    .map_parser_err(StatementKind::CreateTable)
            }
        } else if self.peek_keyword(SECRET) {
            self.parse_create_secret()
                .map_parser_err(StatementKind::CreateSecret)
        } else if self.peek_keyword(CONNECTION) {
            self.parse_create_connection()
                .map_parser_err(StatementKind::CreateConnection)
        } else if self.peek_keywords(&[MATERIALIZED, VIEW])
            || self.peek_keywords(&[OR, REPLACE, MATERIALIZED, VIEW])
        {
            self.parse_create_materialized_view()
                .map_parser_err(StatementKind::CreateMaterializedView)
        } else if self.peek_keywords(&[CONTINUAL, TASK]) {
            if self.peek_keywords_lookahead(&[FROM, TRANSFORM]) {
                self.parse_create_continual_task_from_transform()
                    .map_parser_err(StatementKind::CreateContinualTask)
            } else if self.peek_keywords_lookahead(&[FROM, RETAIN]) {
                self.parse_create_continual_task_from_retain()
                    .map_parser_err(StatementKind::CreateContinualTask)
            } else {
                self.parse_create_continual_task()
                    .map_parser_err(StatementKind::CreateContinualTask)
            }
        } else if self.peek_keywords(&[USER]) {
            parser_err!(
                self,
                self.peek_pos(),
                "CREATE USER is not supported, for more information consult the documentation at https://materialize.com/docs/sql/create-role/#details"
            ).map_parser_err(StatementKind::CreateRole)
        } else if self.peek_keywords(&[NETWORK, POLICY]) {
            self.parse_create_network_policy()
                .map_parser_err(StatementKind::CreateNetworkPolicy)
        } else {
            let index = self.index;

            // go over optional modifiers
            let parsed_or_replace = self.parse_keywords(&[OR, REPLACE]);
            let parsed_temporary = self.parse_one_of_keywords(&[TEMP, TEMPORARY]).is_some();

            if self.parse_keyword(VIEW) {
                self.index = index;
                self.parse_create_view()
                    .map_parser_err(StatementKind::CreateView)
            } else {
                let expected_msg = match (parsed_or_replace, parsed_temporary) {
                    (true, true) => "VIEW after CREATE OR REPLACE TEMPORARY",
                    (true, false) => {
                        "[TEMPORARY] VIEW, or MATERIALIZED VIEW after CREATE OR REPLACE"
                    }
                    (false, true) => "TABLE, or VIEW after CREATE TEMPORARY",
                    (false, false) => {
                        "DATABASE, SCHEMA, ROLE, TYPE, INDEX, SINK, SOURCE, [TEMPORARY] TABLE, \
                        SECRET, [OR REPLACE] [TEMPORARY] VIEW, or [OR REPLACE] MATERIALIZED VIEW \
                        after CREATE"
                    }
                };
                self.expected(self.peek_pos(), expected_msg, self.peek_token())
                    .map_no_statement_parser_err()
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
                let n_cols = self.parse_literal_uint()?;
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
            let array = self.parse_keyword(ARRAY);
            Format::Json { array }
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
            self.expect_keyword(SCHEMA)?;
            let schema = Schema {
                schema: self.parse_literal_string()?,
            };
            let with_options = if self.consume_token(&Token::LParen) {
                let with_options = self.parse_comma_separated(Parser::parse_avro_schema_option)?;
                self.expect_token(&Token::RParen)?;
                with_options
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

    fn parse_avro_schema_option(&mut self) -> Result<AvroSchemaOption<Raw>, ParserError> {
        self.expect_keywords(&[CONFLUENT, WIRE, FORMAT])?;
        Ok(AvroSchemaOption {
            name: AvroSchemaOptionName::ConfluentWireFormat,
            value: self.parse_optional_option_value()?,
        })
    }

    fn parse_protobuf_schema(&mut self) -> Result<ProtobufSchema<Raw>, ParserError> {
        if self.parse_keywords(&[USING, CONFLUENT, SCHEMA, REGISTRY]) {
            let csr_connection = self.parse_csr_connection_proto()?;
            Ok(ProtobufSchema::Csr { csr_connection })
        } else if self.parse_keyword(MESSAGE) {
            let message_name = self.parse_literal_string()?;
            self.expect_keyword(USING)?;
            self.expect_keyword(SCHEMA)?;
            let schema = Schema {
                schema: self.parse_literal_string()?,
            };
            Ok(ProtobufSchema::InlineSchema {
                message_name,
                schema,
            })
        } else {
            self.expected(
                self.peek_pos(),
                "CONFLUENT SCHEMA REGISTRY or MESSAGE",
                self.peek_token(),
            )
        }
    }

    fn parse_csr_connection_reference(&mut self) -> Result<CsrConnection<Raw>, ParserError> {
        self.expect_keyword(CONNECTION)?;
        let connection = self.parse_raw_name()?;

        let options = if self.consume_token(&Token::LParen) {
            let options = self.parse_comma_separated(Parser::parse_csr_config_option)?;
            self.expect_token(&Token::RParen)?;
            options
        } else {
            vec![]
        };

        Ok(CsrConnection {
            connection,
            options,
        })
    }

    fn parse_csr_config_option(&mut self) -> Result<CsrConfigOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[AVRO, NULL, KEY, VALUE, DOC])? {
            AVRO => {
                let name = match self.expect_one_of_keywords(&[KEY, VALUE])? {
                    KEY => CsrConfigOptionName::AvroKeyFullname,
                    VALUE => CsrConfigOptionName::AvroValueFullname,
                    _ => unreachable!(),
                };
                self.expect_keyword(FULLNAME)?;
                name
            }
            NULL => {
                self.expect_keyword(DEFAULTS)?;
                CsrConfigOptionName::NullDefaults
            }
            KEY => match self.expect_one_of_keywords(&[DOC, COMPATIBILITY])? {
                DOC => {
                    self.expect_keyword(ON)?;
                    let doc_on_identifier = self.parse_avro_doc_on_option_name()?;
                    CsrConfigOptionName::AvroDocOn(AvroDocOn {
                        identifier: doc_on_identifier,
                        for_schema: DocOnSchema::KeyOnly,
                    })
                }
                COMPATIBILITY => {
                    self.expect_keyword(LEVEL)?;
                    CsrConfigOptionName::KeyCompatibilityLevel
                }
                _ => unreachable!(),
            },
            VALUE => match self.expect_one_of_keywords(&[DOC, COMPATIBILITY])? {
                DOC => {
                    self.expect_keyword(ON)?;
                    let doc_on_identifier = self.parse_avro_doc_on_option_name()?;
                    CsrConfigOptionName::AvroDocOn(AvroDocOn {
                        identifier: doc_on_identifier,
                        for_schema: DocOnSchema::ValueOnly,
                    })
                }
                COMPATIBILITY => {
                    self.expect_keyword(LEVEL)?;
                    CsrConfigOptionName::ValueCompatibilityLevel
                }
                _ => unreachable!(),
            },
            DOC => {
                self.expect_keyword(ON)?;
                let doc_on_identifier = self.parse_avro_doc_on_option_name()?;
                CsrConfigOptionName::AvroDocOn(AvroDocOn {
                    identifier: doc_on_identifier,
                    for_schema: DocOnSchema::All,
                })
            }
            _ => unreachable!(),
        };
        Ok(CsrConfigOption {
            name,
            value: self.parse_optional_option_value()?,
        })
    }

    fn parse_avro_doc_on_option_name(&mut self) -> Result<DocOnIdentifier<Raw>, ParserError> {
        match self.expect_one_of_keywords(&[TYPE, COLUMN])? {
            TYPE => Ok(DocOnIdentifier::Type(self.parse_raw_name()?)),
            COLUMN => Ok(DocOnIdentifier::Column(self.parse_column_name()?)),
            _ => unreachable!(),
        }
    }

    fn parse_csr_connection_avro(&mut self) -> Result<CsrConnectionAvro<Raw>, ParserError> {
        let connection = self.parse_csr_connection_reference()?;
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

        Ok(CsrConnectionAvro {
            connection,
            seed,
            key_strategy,
            value_strategy,
        })
    }

    fn parse_csr_connection_proto(&mut self) -> Result<CsrConnectionProtobuf<Raw>, ParserError> {
        let connection = self.parse_csr_connection_reference()?;

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

        Ok(CsrConnectionProtobuf { connection, seed })
    }

    fn parse_source_error_policy_option(&mut self) -> Result<SourceErrorPolicy, ParserError> {
        match self.expect_one_of_keywords(&[INLINE])? {
            INLINE => Ok(SourceErrorPolicy::Inline {
                alias: self.parse_alias()?,
            }),
            _ => unreachable!(),
        }
    }

    fn parse_source_envelope(&mut self) -> Result<SourceEnvelope, ParserError> {
        let envelope = if self.parse_keyword(NONE) {
            SourceEnvelope::None
        } else if self.parse_keyword(DEBEZIUM) {
            SourceEnvelope::Debezium
        } else if self.parse_keyword(UPSERT) {
            let value_decode_err_policy = if self.consume_token(&Token::LParen) {
                // We only support the `VALUE DECODING ERRORS` option for now, but if we add another
                // we should extract this into a helper function.
                self.expect_keywords(&[VALUE, DECODING, ERRORS])?;
                let _ = self.consume_token(&Token::Eq);
                let open_inner = self.consume_token(&Token::LParen);
                let value_decode_err_policy =
                    self.parse_comma_separated(Parser::parse_source_error_policy_option)?;
                if open_inner {
                    self.expect_token(&Token::RParen)?;
                }
                self.expect_token(&Token::RParen)?;
                value_decode_err_policy
            } else {
                vec![]
            };

            SourceEnvelope::Upsert {
                value_decode_err_policy,
            }
        } else if self.parse_keyword(MATERIALIZE) {
            SourceEnvelope::CdcV2
        } else {
            return self.expected(
                self.peek_pos(),
                "NONE, UPSERT, or MATERIALIZE",
                self.peek_token(),
            );
        };
        Ok(envelope)
    }

    fn parse_sink_envelope(&mut self) -> Result<SinkEnvelope, ParserError> {
        if self.parse_keyword(UPSERT) {
            Ok(SinkEnvelope::Upsert)
        } else if self.parse_keyword(DEBEZIUM) {
            Ok(SinkEnvelope::Debezium)
        } else {
            self.expected(self.peek_pos(), "UPSERT, DEBEZIUM", self.peek_token())
        }
    }
    /// Parse a `VALIDATE` statement
    fn parse_validate(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(CONNECTION)?;
        let name = self.parse_raw_name()?;
        Ok(Statement::ValidateConnection(ValidateConnectionStatement {
            name,
        }))
    }

    fn parse_create_connection(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(CONNECTION)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_item_name()?;
        let expect_paren = match self.expect_one_of_keywords(&[FOR, TO])? {
            FOR => false,
            TO => true,
            _ => unreachable!(),
        };
        let connection_type = match self
            .expect_one_of_keywords(&[AWS, KAFKA, CONFLUENT, POSTGRES, SSH, SQL, MYSQL, ICEBERG])?
        {
            AWS => {
                if self.parse_keyword(PRIVATELINK) {
                    CreateConnectionType::AwsPrivatelink
                } else {
                    CreateConnectionType::Aws
                }
            }
            KAFKA => CreateConnectionType::Kafka,
            CONFLUENT => {
                self.expect_keywords(&[SCHEMA, REGISTRY])?;
                CreateConnectionType::Csr
            }
            POSTGRES => CreateConnectionType::Postgres,
            SSH => {
                self.expect_keyword(TUNNEL)?;
                CreateConnectionType::Ssh
            }
            SQL => {
                self.expect_keyword(SERVER)?;
                CreateConnectionType::SqlServer
            }
            MYSQL => CreateConnectionType::MySql,
            ICEBERG => {
                self.expect_keyword(CATALOG)?;
                CreateConnectionType::IcebergCatalog
            }
            _ => unreachable!(),
        };
        if expect_paren {
            self.expect_token(&Token::LParen)?;
        }
        let values = self.parse_comma_separated(Parser::parse_connection_option_unified)?;
        if expect_paren {
            self.expect_token(&Token::RParen)?;
        }

        let with_options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_create_connection_option)?;
            self.expect_token(&Token::RParen)?;
            options
        } else {
            vec![]
        };

        Ok(Statement::CreateConnection(CreateConnectionStatement {
            name,
            connection_type,
            values,
            if_not_exists,
            with_options,
        }))
    }

    fn parse_create_connection_option_name(
        &mut self,
    ) -> Result<CreateConnectionOptionName, ParserError> {
        let name = match self.expect_one_of_keywords(&[VALIDATE])? {
            VALIDATE => CreateConnectionOptionName::Validate,
            _ => unreachable!(),
        };
        Ok(name)
    }

    /// Parses a single valid option in the WITH block of a create source
    fn parse_create_connection_option(
        &mut self,
    ) -> Result<CreateConnectionOption<Raw>, ParserError> {
        let name = self.parse_create_connection_option_name()?;
        Ok(CreateConnectionOption {
            name,
            value: self.parse_optional_option_value()?,
        })
    }

    fn parse_default_aws_privatelink(&mut self) -> Result<WithOptionValue<Raw>, ParserError> {
        let _ = self.consume_token(&Token::Eq);
        let connection = self.parse_raw_name()?;
        let port = if self.consume_token(&Token::LParen) {
            self.expect_keyword(PORT)?;
            let pos = self.peek_pos();
            let Ok(port) = u16::try_from(self.parse_literal_int()?) else {
                return parser_err!(self, pos, "Could not parse value into port");
            };
            self.expect_token(&Token::RParen)?;
            Some(port)
        } else {
            None
        };
        Ok(WithOptionValue::ConnectionAwsPrivatelink(
            ConnectionDefaultAwsPrivatelink { connection, port },
        ))
    }

    fn parse_kafka_broker(&mut self) -> Result<WithOptionValue<Raw>, ParserError> {
        let _ = self.consume_token(&Token::Eq);
        let address = self.parse_literal_string()?;
        let tunnel = if self.parse_keyword(USING) {
            match self.expect_one_of_keywords(&[AWS, SSH])? {
                AWS => {
                    self.expect_keywords(&[PRIVATELINK])?;
                    let connection = self.parse_raw_name()?;
                    let options = if self.consume_token(&Token::LParen) {
                        let options = self.parse_comma_separated(
                            Parser::parse_kafka_broker_aws_privatelink_option,
                        )?;
                        self.expect_token(&Token::RParen)?;
                        options
                    } else {
                        vec![]
                    };
                    KafkaBrokerTunnel::AwsPrivatelink(KafkaBrokerAwsPrivatelink {
                        connection,
                        options,
                    })
                }
                SSH => {
                    self.expect_keywords(&[TUNNEL])?;
                    KafkaBrokerTunnel::SshTunnel(self.parse_raw_name()?)
                }
                _ => unreachable!(),
            }
        } else {
            KafkaBrokerTunnel::Direct
        };

        Ok(WithOptionValue::ConnectionKafkaBroker(KafkaBroker {
            address,
            tunnel,
        }))
    }

    fn parse_kafka_broker_aws_privatelink_option(
        &mut self,
    ) -> Result<KafkaBrokerAwsPrivatelinkOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[AVAILABILITY, PORT])? {
            AVAILABILITY => {
                self.expect_keywords(&[ZONE])?;
                KafkaBrokerAwsPrivatelinkOptionName::AvailabilityZone
            }
            PORT => KafkaBrokerAwsPrivatelinkOptionName::Port,
            _ => unreachable!(),
        };
        let value = self.parse_optional_option_value()?;
        Ok(KafkaBrokerAwsPrivatelinkOption { name, value })
    }

    fn parse_kafka_source_config_option(
        &mut self,
    ) -> Result<KafkaSourceConfigOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[GROUP, START, TOPIC])? {
            GROUP => {
                self.expect_keywords(&[ID, PREFIX])?;
                KafkaSourceConfigOptionName::GroupIdPrefix
            }
            START => match self.expect_one_of_keywords(&[OFFSET, TIMESTAMP])? {
                OFFSET => KafkaSourceConfigOptionName::StartOffset,
                TIMESTAMP => KafkaSourceConfigOptionName::StartTimestamp,
                _ => unreachable!(),
            },
            TOPIC => {
                if self.parse_keyword(METADATA) {
                    self.expect_keywords(&[REFRESH, INTERVAL])?;
                    KafkaSourceConfigOptionName::TopicMetadataRefreshInterval
                } else {
                    KafkaSourceConfigOptionName::Topic
                }
            }
            _ => unreachable!(),
        };
        Ok(KafkaSourceConfigOption {
            name,
            value: self.parse_optional_option_value()?,
        })
    }

    fn parse_iceberg_sink_config_option(
        &mut self,
    ) -> Result<IcebergSinkConfigOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[NAMESPACE, TABLE])? {
            NAMESPACE => IcebergSinkConfigOptionName::Namespace,
            TABLE => IcebergSinkConfigOptionName::Table,
            _ => unreachable!(),
        };
        Ok(IcebergSinkConfigOption {
            name,
            value: self.parse_optional_option_value()?,
        })
    }

    fn parse_kafka_sink_config_option(
        &mut self,
    ) -> Result<KafkaSinkConfigOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[
            COMPRESSION,
            PARTITION,
            PROGRESS,
            TOPIC,
            LEGACY,
            TRANSACTIONAL,
        ])? {
            COMPRESSION => {
                self.expect_keyword(TYPE)?;
                KafkaSinkConfigOptionName::CompressionType
            }
            PARTITION => {
                self.expect_keyword(BY)?;
                let _ = self.consume_token(&Token::Eq);
                return Ok(KafkaSinkConfigOption {
                    name: KafkaSinkConfigOptionName::PartitionBy,
                    value: Some(WithOptionValue::Expr(self.parse_expr()?)),
                });
            }
            PROGRESS => {
                self.expect_keywords(&[GROUP, ID, PREFIX])?;
                KafkaSinkConfigOptionName::ProgressGroupIdPrefix
            }
            TOPIC => {
                match self.parse_one_of_keywords(&[METADATA, PARTITION, REPLICATION, CONFIG]) {
                    None => KafkaSinkConfigOptionName::Topic,
                    Some(METADATA) => {
                        self.expect_keywords(&[REFRESH, INTERVAL])?;
                        KafkaSinkConfigOptionName::TopicMetadataRefreshInterval
                    }
                    Some(PARTITION) => {
                        self.expect_keyword(COUNT)?;
                        KafkaSinkConfigOptionName::TopicPartitionCount
                    }
                    Some(REPLICATION) => {
                        self.expect_keyword(FACTOR)?;
                        KafkaSinkConfigOptionName::TopicReplicationFactor
                    }
                    Some(CONFIG) => KafkaSinkConfigOptionName::TopicConfig,
                    Some(other) => {
                        return parser_err!(
                            self,
                            self.peek_prev_pos(),
                            "unexpected keyword {}",
                            other
                        );
                    }
                }
            }
            TRANSACTIONAL => {
                self.expect_keywords(&[ID, PREFIX])?;
                KafkaSinkConfigOptionName::TransactionalIdPrefix
            }
            LEGACY => {
                self.expect_keywords(&[IDS])?;
                KafkaSinkConfigOptionName::LegacyIds
            }
            _ => unreachable!(),
        };
        Ok(KafkaSinkConfigOption {
            name,
            value: self.parse_optional_option_value()?,
        })
    }

    fn parse_connection_option_name(&mut self) -> Result<ConnectionOptionName, ParserError> {
        Ok(
            match self.expect_one_of_keywords(&[
                ACCESS,
                ASSUME,
                AVAILABILITY,
                AWS,
                BROKER,
                BROKERS,
                CATALOG,
                CREDENTIAL,
                DATABASE,
                ENDPOINT,
                HOST,
                PASSWORD,
                PORT,
                PUBLIC,
                PROGRESS,
                REGION,
                ROLE,
                SASL,
                SCOPE,
                SECRET,
                SECURITY,
                SERVICE,
                SESSION,
                SSH,
                SSL,
                URL,
                USER,
                USERNAME,
                WAREHOUSE,
            ])? {
                ACCESS => {
                    self.expect_keywords(&[KEY, ID])?;
                    ConnectionOptionName::AccessKeyId
                }
                ASSUME => {
                    self.expect_keyword(ROLE)?;
                    match self.expect_one_of_keywords(&[ARN, SESSION])? {
                        ARN => ConnectionOptionName::AssumeRoleArn,
                        SESSION => {
                            self.expect_keyword(NAME)?;
                            ConnectionOptionName::AssumeRoleSessionName
                        }
                        _ => unreachable!(),
                    }
                }
                AVAILABILITY => {
                    self.expect_keyword(ZONES)?;
                    ConnectionOptionName::AvailabilityZones
                }
                AWS => match self.expect_one_of_keywords(&[CONNECTION, PRIVATELINK])? {
                    CONNECTION => ConnectionOptionName::AwsConnection,
                    PRIVATELINK => ConnectionOptionName::AwsPrivatelink,
                    _ => unreachable!(),
                },
                BROKER => ConnectionOptionName::Broker,
                BROKERS => ConnectionOptionName::Brokers,
                CATALOG => {
                    self.expect_keyword(TYPE)?;
                    ConnectionOptionName::CatalogType
                }
                CREDENTIAL => ConnectionOptionName::Credential,
                DATABASE => ConnectionOptionName::Database,
                ENDPOINT => ConnectionOptionName::Endpoint,
                HOST => ConnectionOptionName::Host,
                PASSWORD => ConnectionOptionName::Password,
                PORT => ConnectionOptionName::Port,
                PUBLIC => {
                    self.expect_keyword(KEY)?;
                    match self.next_token() {
                        Some(Token::Number(n)) if n == "1" => ConnectionOptionName::PublicKey1,
                        Some(Token::Number(n)) if n == "2" => ConnectionOptionName::PublicKey2,
                        t => self.expected(self.peek_prev_pos(), "1 or 2 after PUBLIC KEY", t)?,
                    }
                }
                PROGRESS => {
                    self.expect_keyword(TOPIC)?;
                    match self.parse_keywords(&[REPLICATION, FACTOR]) {
                        true => ConnectionOptionName::ProgressTopicReplicationFactor,
                        false => ConnectionOptionName::ProgressTopic,
                    }
                }
                SECURITY => {
                    self.expect_keyword(PROTOCOL)?;
                    ConnectionOptionName::SecurityProtocol
                }
                REGION => ConnectionOptionName::Region,
                SASL => match self.expect_one_of_keywords(&[MECHANISMS, PASSWORD, USERNAME])? {
                    MECHANISMS => ConnectionOptionName::SaslMechanisms,
                    PASSWORD => ConnectionOptionName::SaslPassword,
                    USERNAME => ConnectionOptionName::SaslUsername,
                    _ => unreachable!(),
                },
                SCOPE => ConnectionOptionName::Scope,
                SECRET => {
                    self.expect_keywords(&[ACCESS, KEY])?;
                    ConnectionOptionName::SecretAccessKey
                }
                SERVICE => {
                    self.expect_keyword(NAME)?;
                    ConnectionOptionName::ServiceName
                }
                SESSION => {
                    self.expect_keyword(TOKEN)?;
                    ConnectionOptionName::SessionToken
                }
                SSH => {
                    self.expect_keyword(TUNNEL)?;
                    ConnectionOptionName::SshTunnel
                }
                SSL => match self.expect_one_of_keywords(&[CERTIFICATE, MODE, KEY])? {
                    CERTIFICATE => {
                        if self.parse_keyword(AUTHORITY) {
                            ConnectionOptionName::SslCertificateAuthority
                        } else {
                            ConnectionOptionName::SslCertificate
                        }
                    }
                    KEY => ConnectionOptionName::SslKey,
                    MODE => ConnectionOptionName::SslMode,
                    _ => unreachable!(),
                },
                URL => ConnectionOptionName::Url,
                // TYPE => ConnectionOptionName::CatalogType,
                WAREHOUSE => ConnectionOptionName::Warehouse,
                USER | USERNAME => ConnectionOptionName::User,
                _ => unreachable!(),
            },
        )
    }

    fn parse_connection_option_unified(&mut self) -> Result<ConnectionOption<Raw>, ParserError> {
        let name = match self.parse_connection_option_name()? {
            ConnectionOptionName::AwsConnection => {
                return Ok(ConnectionOption {
                    name: ConnectionOptionName::AwsConnection,
                    value: Some(self.parse_object_option_value()?),
                });
            }
            ConnectionOptionName::AwsPrivatelink => {
                return Ok(ConnectionOption {
                    name: ConnectionOptionName::AwsPrivatelink,
                    value: Some(self.parse_default_aws_privatelink()?),
                });
            }
            ConnectionOptionName::Broker => {
                return Ok(ConnectionOption {
                    name: ConnectionOptionName::Broker,
                    value: Some(self.parse_kafka_broker()?),
                });
            }
            ConnectionOptionName::Brokers => {
                let _ = self.consume_token(&Token::Eq);
                let delimiter = self.expect_one_of_tokens(&[Token::LParen, Token::LBracket])?;
                let brokers = self.parse_comma_separated(Parser::parse_kafka_broker)?;
                self.expect_token(&match delimiter {
                    Token::LParen => Token::RParen,
                    Token::LBracket => Token::RBracket,
                    _ => unreachable!(),
                })?;
                return Ok(ConnectionOption {
                    name: ConnectionOptionName::Brokers,
                    value: Some(WithOptionValue::Sequence(brokers)),
                });
            }
            ConnectionOptionName::SshTunnel => {
                return Ok(ConnectionOption {
                    name: ConnectionOptionName::SshTunnel,
                    value: Some(self.parse_object_option_value()?),
                });
            }
            name => name,
        };
        Ok(ConnectionOption {
            name,
            value: self.parse_optional_option_value()?,
        })
    }

    fn parse_create_subsource(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(SUBSOURCE)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_item_name()?;

        let (columns, constraints) = self.parse_columns(Mandatory)?;

        let of_source = if self.parse_keyword(OF) {
            self.expect_keyword(SOURCE)?;
            Some(self.parse_raw_name()?)
        } else {
            None
        };

        let with_options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_create_subsource_option)?;
            self.expect_token(&Token::RParen)?;
            options
        } else {
            vec![]
        };

        Ok(Statement::CreateSubsource(CreateSubsourceStatement {
            name,
            if_not_exists,
            columns,
            of_source,
            constraints,
            with_options,
        }))
    }

    fn parse_create_subsource_option(&mut self) -> Result<CreateSubsourceOption<Raw>, ParserError> {
        let option = match self
            .expect_one_of_keywords(&[EXTERNAL, PROGRESS, TEXT, EXCLUDE, IGNORE, DETAILS, RETAIN])?
        {
            EXTERNAL => {
                self.expect_keyword(REFERENCE)?;
                CreateSubsourceOption {
                    name: CreateSubsourceOptionName::ExternalReference,
                    value: self.parse_optional_option_value()?,
                }
            }
            PROGRESS => CreateSubsourceOption {
                name: CreateSubsourceOptionName::Progress,
                value: self.parse_optional_option_value()?,
            },
            ref keyword @ (TEXT | EXCLUDE | IGNORE) => {
                self.expect_keyword(COLUMNS)?;

                let _ = self.consume_token(&Token::Eq);

                let value = self
                    .parse_option_sequence(Parser::parse_identifier)?
                    .map(|inner| {
                        WithOptionValue::Sequence(
                            inner.into_iter().map(WithOptionValue::Ident).collect_vec(),
                        )
                    });

                CreateSubsourceOption {
                    name: match *keyword {
                        TEXT => CreateSubsourceOptionName::TextColumns,
                        // IGNORE is historical syntax for this option.
                        EXCLUDE | IGNORE => CreateSubsourceOptionName::ExcludeColumns,
                        _ => unreachable!(),
                    },
                    value,
                }
            }
            DETAILS => CreateSubsourceOption {
                name: CreateSubsourceOptionName::Details,
                value: self.parse_optional_option_value()?,
            },
            RETAIN => {
                self.expect_keyword(HISTORY)?;
                CreateSubsourceOption {
                    name: CreateSubsourceOptionName::RetainHistory,
                    value: self.parse_option_retain_history()?,
                }
            }
            _ => unreachable!(),
        };
        Ok(option)
    }

    fn parse_create_source(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(SOURCE)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_item_name()?;

        let (col_names, key_constraint) = self.parse_source_columns()?;

        let in_cluster = self.parse_optional_in_cluster()?;
        self.expect_keyword(FROM)?;

        // Webhook Source, which works differently than all other sources.
        if self.parse_keyword(WEBHOOK) {
            return self.parse_create_webhook_source(name, if_not_exists, in_cluster, false);
        }

        let connection = self.parse_create_source_connection()?;
        let format = match self.parse_one_of_keywords(&[KEY, FORMAT]) {
            Some(KEY) => {
                self.expect_keyword(FORMAT)?;
                let key = self.parse_format()?;
                self.expect_keywords(&[VALUE, FORMAT])?;
                let value = self.parse_format()?;
                Some(FormatSpecifier::KeyValue { key, value })
            }
            Some(FORMAT) => Some(FormatSpecifier::Bare(self.parse_format()?)),
            Some(_) => unreachable!("parse_one_of_keywords returns None for this"),
            None => None,
        };
        let include_metadata = self.parse_source_include_metadata()?;

        let envelope = if self.parse_keyword(ENVELOPE) {
            Some(self.parse_source_envelope()?)
        } else {
            None
        };

        let referenced_subsources = if self.parse_keywords(&[FOR, TABLES]) {
            self.expect_token(&Token::LParen)?;
            let subsources = self.parse_comma_separated(Parser::parse_subsource_references)?;
            self.expect_token(&Token::RParen)?;
            Some(ExternalReferences::SubsetTables(subsources))
        } else if self.parse_keywords(&[FOR, SCHEMAS]) {
            self.expect_token(&Token::LParen)?;
            let schemas = self.parse_comma_separated(Parser::parse_identifier)?;
            self.expect_token(&Token::RParen)?;
            Some(ExternalReferences::SubsetSchemas(schemas))
        } else if self.parse_keywords(&[FOR, ALL, TABLES]) {
            Some(ExternalReferences::All)
        } else {
            None
        };

        let progress_subsource = if self.parse_keywords(&[EXPOSE, PROGRESS, AS]) {
            Some(self.parse_deferred_item_name()?)
        } else {
            None
        };

        // New WITH block
        let with_options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_source_option)?;
            self.expect_token(&Token::RParen)?;
            options
        } else {
            vec![]
        };

        Ok(Statement::CreateSource(CreateSourceStatement {
            name,
            in_cluster,
            col_names,
            connection,
            format,
            include_metadata,
            envelope,
            if_not_exists,
            key_constraint,
            external_references: referenced_subsources,
            progress_subsource,
            with_options,
        }))
    }

    fn parse_subsource_references(&mut self) -> Result<ExternalReferenceExport, ParserError> {
        let reference = self.parse_item_name()?;
        let subsource = if self.parse_one_of_keywords(&[AS, INTO]).is_some() {
            Some(self.parse_item_name()?)
        } else {
            None
        };

        Ok(ExternalReferenceExport {
            reference,
            alias: subsource,
        })
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

    fn parse_source_option_name(&mut self) -> Result<CreateSourceOptionName, ParserError> {
        let name = match self.expect_one_of_keywords(&[TIMESTAMP, RETAIN])? {
            TIMESTAMP => {
                self.expect_keyword(INTERVAL)?;
                CreateSourceOptionName::TimestampInterval
            }
            RETAIN => {
                self.expect_keyword(HISTORY)?;
                CreateSourceOptionName::RetainHistory
            }
            _ => unreachable!(),
        };
        Ok(name)
    }

    /// Parses a single valid option in the WITH block of a create source
    fn parse_source_option(&mut self) -> Result<CreateSourceOption<Raw>, ParserError> {
        let name = self.parse_source_option_name()?;
        if name == CreateSourceOptionName::RetainHistory {
            let _ = self.consume_token(&Token::Eq);
            return Ok(CreateSourceOption {
                name,
                value: self.parse_option_retain_history()?,
            });
        }
        Ok(CreateSourceOption {
            name,
            value: self.parse_optional_option_value()?,
        })
    }

    fn parse_create_webhook_source(
        &mut self,
        name: UnresolvedItemName,
        if_not_exists: bool,
        in_cluster: Option<RawClusterName>,
        is_table: bool,
    ) -> Result<Statement<Raw>, ParserError> {
        self.expect_keywords(&[BODY, FORMAT])?;

        // Note: we don't use `parse_format()` here because we support fewer formats than other
        // sources, and the user gets better errors if we reject the formats here.
        let body_format = match self.expect_one_of_keywords(&[JSON, TEXT, BYTES])? {
            JSON => {
                let array = self.parse_keyword(ARRAY);
                Format::Json { array }
            }
            TEXT => Format::Text,
            BYTES => Format::Bytes,
            _ => unreachable!(),
        };

        let mut include_headers = CreateWebhookSourceIncludeHeaders::default();
        while self.parse_keyword(INCLUDE) {
            match self.expect_one_of_keywords(&[HEADER, HEADERS])? {
                HEADER => {
                    let header_name = self.parse_literal_string()?;
                    self.expect_keyword(AS)?;
                    let column_name = self.parse_identifier()?;
                    let use_bytes = self.parse_keyword(BYTES);

                    include_headers.mappings.push(CreateWebhookSourceMapHeader {
                        header_name,
                        column_name,
                        use_bytes,
                    });
                }
                HEADERS => {
                    let header_filters = include_headers.column.get_or_insert_with(Vec::default);
                    if self.consume_token(&Token::LParen) {
                        let filters = self.parse_comma_separated(|f| {
                            let block = f.parse_keyword(NOT);
                            let header_name = f.parse_literal_string()?;
                            Ok(CreateWebhookSourceFilterHeader { block, header_name })
                        })?;
                        header_filters.extend(filters);

                        self.expect_token(&Token::RParen)?;
                    }
                }
                k => unreachable!("programming error, didn't expect {k}"),
            }
        }

        let validate_using = if self.parse_keyword(CHECK) {
            self.expect_token(&Token::LParen)?;

            let options = if self.parse_keyword(WITH) {
                self.expect_token(&Token::LParen)?;
                let options = self.parse_create_webhook_check_options()?;
                self.expect_token(&Token::RParen)?;

                Some(options)
            } else {
                None
            };

            let using = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;

            Some(CreateWebhookSourceCheck { options, using })
        } else {
            None
        };

        Ok(Statement::CreateWebhookSource(
            CreateWebhookSourceStatement {
                name,
                is_table,
                if_not_exists,
                body_format,
                include_headers,
                validate_using,
                in_cluster,
            },
        ))
    }

    fn parse_create_webhook_check_options(
        &mut self,
    ) -> Result<CreateWebhookSourceCheckOptions<Raw>, ParserError> {
        let mut secrets = vec![];
        let mut headers = vec![];
        let mut bodies = vec![];

        fn parse_alias(parser: &mut Parser<'_>) -> Result<Option<Ident>, ParserError> {
            parser
                .parse_keyword(AS)
                .then(|| parser.parse_identifier())
                .transpose()
        }

        self.parse_comma_separated(|f| {
            match f.expect_one_of_keywords(&[SECRET, HEADERS, BODY])? {
                SECRET => {
                    let secret = f.parse_raw_name()?;
                    let alias = parse_alias(f)?;
                    let use_bytes = f.parse_keyword(Keyword::Bytes);

                    secrets.push(CreateWebhookSourceSecret {
                        secret,
                        alias,
                        use_bytes,
                    });

                    Ok(())
                }
                HEADERS => {
                    // TODO(parkmycar): Support filtering down to specific headers.
                    let alias = parse_alias(f)?;
                    let use_bytes = f.parse_keyword(Keyword::Bytes);
                    headers.push(CreateWebhookSourceHeader { alias, use_bytes });

                    Ok(())
                }
                BODY => {
                    let alias = parse_alias(f)?;
                    let use_bytes = f.parse_keyword(Keyword::Bytes);
                    bodies.push(CreateWebhookSourceBody { alias, use_bytes });

                    Ok(())
                }
                k => unreachable!("Unexpected keyword! {k}"),
            }
        })?;

        Ok(CreateWebhookSourceCheckOptions {
            secrets,
            headers,
            bodies,
        })
    }

    fn parse_create_iceberg_sink(
        &mut self,
        name: Option<UnresolvedItemName>,
        in_cluster: Option<RawClusterName>,
        from: RawItemName,
        if_not_exists: bool,
        connection: CreateSinkConnection<Raw>,
    ) -> Result<CreateSinkStatement<Raw>, ParserError> {
        let envelope = if self.parse_keyword(ENVELOPE) {
            Some(self.parse_sink_envelope()?)
        } else {
            None
        };

        let with_options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_create_sink_option)?;
            self.expect_token(&Token::RParen)?;
            options
        } else {
            vec![]
        };

        Ok(CreateSinkStatement {
            name,
            in_cluster,
            from,
            connection,
            format: None,
            envelope,
            if_not_exists,
            with_options,
        })
    }

    fn parse_create_kafka_sink(
        &mut self,
        name: Option<UnresolvedItemName>,
        in_cluster: Option<RawClusterName>,
        from: RawItemName,
        if_not_exists: bool,
        connection: CreateSinkConnection<Raw>,
    ) -> Result<CreateSinkStatement<Raw>, ParserError> {
        let format = match &self.parse_one_of_keywords(&[KEY, FORMAT]) {
            Some(KEY) => {
                self.expect_keyword(FORMAT)?;
                let key = self.parse_format()?;
                self.expect_keywords(&[VALUE, FORMAT])?;
                let value = self.parse_format()?;
                Some(FormatSpecifier::KeyValue { key, value })
            }
            Some(FORMAT) => Some(FormatSpecifier::Bare(self.parse_format()?)),
            Some(_) => unreachable!("parse_one_of_keywords returns None for this"),
            None => None,
        };
        let envelope = if self.parse_keyword(ENVELOPE) {
            Some(self.parse_sink_envelope()?)
        } else {
            None
        };

        let with_options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_create_sink_option)?;
            self.expect_token(&Token::RParen)?;
            options
        } else {
            vec![]
        };

        Ok(CreateSinkStatement {
            name,
            in_cluster,
            from,
            connection,
            format,
            envelope,
            if_not_exists,
            with_options,
        })
    }

    fn parse_create_sink(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(SINK)?;
        let if_not_exists = self.parse_if_not_exists()?;

        let mut name = Some(self.parse_item_name()?);

        // Sniff out `CREATE SINK IN CLUSTER <c> ...` and `CREATE SINK FROM
        // <view>...`  and ensure they are parsed as nameless `CREATE SINK`
        // commands.
        //
        // This is a bit gross, but we didn't have the foresight to make
        // `IN` and `FROM` reserved keywords for sink names.
        if (name == Some(UnresolvedItemName::unqualified(ident!("in")))
            && self.peek_keyword(CLUSTER))
            || (name == Some(UnresolvedItemName::unqualified(ident!("from")))
                && !self.peek_keyword(FROM))
        {
            name = None;
            self.prev_token();
        }

        let in_cluster = self.parse_optional_in_cluster()?;
        self.expect_keyword(FROM)?;
        let from = self.parse_raw_name()?;
        self.expect_keyword(INTO)?;
        let connection = self.parse_create_sink_connection()?;

        let statement = match connection {
            conn @ CreateSinkConnection::Kafka { .. } => {
                self.parse_create_kafka_sink(name, in_cluster, from, if_not_exists, conn)
            }
            conn @ CreateSinkConnection::Iceberg { .. } => {
                self.parse_create_iceberg_sink(name, in_cluster, from, if_not_exists, conn)
            }
        }?;

        Ok(Statement::CreateSink(statement))
    }

    /// Parse the name of a CREATE SINK optional parameter
    fn parse_create_sink_option_name(&mut self) -> Result<CreateSinkOptionName, ParserError> {
        let name = match self.expect_one_of_keywords(&[PARTITION, SNAPSHOT, VERSION, COMMIT])? {
            SNAPSHOT => CreateSinkOptionName::Snapshot,
            VERSION => CreateSinkOptionName::Version,
            PARTITION => {
                self.expect_keyword(STRATEGY)?;
                CreateSinkOptionName::PartitionStrategy
            }
            COMMIT => {
                self.expect_keyword(INTERVAL)?;
                CreateSinkOptionName::CommitInterval
            }
            _ => unreachable!(),
        };
        Ok(name)
    }

    /// Parse a NAME = VALUE parameter for CREATE SINK
    fn parse_create_sink_option(&mut self) -> Result<CreateSinkOption<Raw>, ParserError> {
        Ok(CreateSinkOption {
            name: self.parse_create_sink_option_name()?,
            value: self.parse_optional_option_value()?,
        })
    }

    fn parse_create_source_connection(
        &mut self,
    ) -> Result<CreateSourceConnection<Raw>, ParserError> {
        match self.expect_one_of_keywords(&[KAFKA, POSTGRES, SQL, MYSQL, LOAD])? {
            POSTGRES => {
                self.expect_keyword(CONNECTION)?;
                let connection = self.parse_raw_name()?;

                let options = if self.consume_token(&Token::LParen) {
                    let options = self.parse_comma_separated(Parser::parse_pg_connection_option)?;
                    self.expect_token(&Token::RParen)?;
                    options
                } else {
                    vec![]
                };

                Ok(CreateSourceConnection::Postgres {
                    connection,
                    options,
                })
            }
            SQL => {
                self.expect_keywords(&[SERVER, CONNECTION])?;
                let connection = self.parse_raw_name()?;

                let options = if self.consume_token(&Token::LParen) {
                    let options =
                        self.parse_comma_separated(Parser::parse_sql_server_connection_option)?;
                    self.expect_token(&Token::RParen)?;
                    options
                } else {
                    vec![]
                };

                Ok(CreateSourceConnection::SqlServer {
                    connection,
                    options,
                })
            }
            MYSQL => {
                self.expect_keyword(CONNECTION)?;
                let connection = self.parse_raw_name()?;

                let options = if self.consume_token(&Token::LParen) {
                    let options =
                        self.parse_comma_separated(Parser::parse_mysql_connection_option)?;
                    self.expect_token(&Token::RParen)?;
                    options
                } else {
                    vec![]
                };

                Ok(CreateSourceConnection::MySql {
                    connection,
                    options,
                })
            }
            KAFKA => {
                self.expect_keyword(CONNECTION)?;
                let connection = self.parse_raw_name()?;

                let options = if self.consume_token(&Token::LParen) {
                    let options =
                        self.parse_comma_separated(Parser::parse_kafka_source_config_option)?;
                    self.expect_token(&Token::RParen)?;
                    options
                } else {
                    vec![]
                };

                Ok(CreateSourceConnection::Kafka {
                    connection,
                    options,
                })
            }
            LOAD => {
                self.expect_keyword(GENERATOR)?;
                let generator = match self.expect_one_of_keywords(&[
                    CLOCK, COUNTER, MARKETING, AUCTION, TPCH, DATUMS, KEY,
                ])? {
                    CLOCK => LoadGenerator::Clock,
                    COUNTER => LoadGenerator::Counter,
                    AUCTION => LoadGenerator::Auction,
                    TPCH => LoadGenerator::Tpch,
                    DATUMS => LoadGenerator::Datums,
                    MARKETING => LoadGenerator::Marketing,
                    KEY => {
                        self.expect_keyword(VALUE)?;
                        LoadGenerator::KeyValue
                    }
                    _ => unreachable!(),
                };
                let options = if self.consume_token(&Token::LParen) {
                    let options =
                        self.parse_comma_separated(Parser::parse_load_generator_option)?;
                    self.expect_token(&Token::RParen)?;
                    options
                } else {
                    vec![]
                };
                Ok(CreateSourceConnection::LoadGenerator { generator, options })
            }
            _ => unreachable!(),
        }
    }

    fn parse_pg_connection_option(&mut self) -> Result<PgConfigOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[DETAILS, PUBLICATION, TEXT, EXCLUDE])? {
            DETAILS => PgConfigOptionName::Details,
            PUBLICATION => PgConfigOptionName::Publication,
            TEXT => {
                self.expect_keyword(COLUMNS)?;

                let _ = self.consume_token(&Token::Eq);

                let value = self
                    .parse_option_sequence(Parser::parse_item_name)?
                    .map(|inner| {
                        WithOptionValue::Sequence(
                            inner
                                .into_iter()
                                .map(WithOptionValue::UnresolvedItemName)
                                .collect_vec(),
                        )
                    });

                return Ok(PgConfigOption {
                    name: PgConfigOptionName::TextColumns,
                    value,
                });
            }
            EXCLUDE => {
                self.expect_keyword(COLUMNS)?;

                let _ = self.consume_token(&Token::Eq);

                let value = self
                    .parse_option_sequence(Parser::parse_item_name)?
                    .map(|inner| {
                        WithOptionValue::Sequence(
                            inner
                                .into_iter()
                                .map(WithOptionValue::UnresolvedItemName)
                                .collect_vec(),
                        )
                    });

                return Ok(PgConfigOption {
                    name: PgConfigOptionName::ExcludeColumns,
                    value,
                });
            }
            _ => unreachable!(),
        };
        Ok(PgConfigOption {
            name,
            value: self.parse_optional_option_value()?,
        })
    }

    fn parse_mysql_connection_option(&mut self) -> Result<MySqlConfigOption<Raw>, ParserError> {
        match self.expect_one_of_keywords(&[DETAILS, TEXT, EXCLUDE, IGNORE])? {
            DETAILS => Ok(MySqlConfigOption {
                name: MySqlConfigOptionName::Details,
                value: self.parse_optional_option_value()?,
            }),
            TEXT => {
                self.expect_keyword(COLUMNS)?;

                let _ = self.consume_token(&Token::Eq);

                let value = self
                    .parse_option_sequence(Parser::parse_item_name)?
                    .map(|inner| {
                        WithOptionValue::Sequence(
                            inner
                                .into_iter()
                                .map(WithOptionValue::UnresolvedItemName)
                                .collect_vec(),
                        )
                    });

                Ok(MySqlConfigOption {
                    name: MySqlConfigOptionName::TextColumns,
                    value,
                })
            }
            // IGNORE is historical syntax for the option.
            EXCLUDE | IGNORE => {
                self.expect_keyword(COLUMNS)?;

                let _ = self.consume_token(&Token::Eq);

                let value = self
                    .parse_option_sequence(Parser::parse_item_name)?
                    .map(|inner| {
                        WithOptionValue::Sequence(
                            inner
                                .into_iter()
                                .map(WithOptionValue::UnresolvedItemName)
                                .collect_vec(),
                        )
                    });

                Ok(MySqlConfigOption {
                    name: MySqlConfigOptionName::ExcludeColumns,
                    value,
                })
            }
            _ => unreachable!(),
        }
    }

    fn parse_sql_server_connection_option(
        &mut self,
    ) -> Result<SqlServerConfigOption<Raw>, ParserError> {
        match self.expect_one_of_keywords(&[DETAILS, TEXT, EXCLUDE])? {
            DETAILS => Ok(SqlServerConfigOption {
                name: SqlServerConfigOptionName::Details,
                value: self.parse_optional_option_value()?,
            }),
            TEXT => {
                self.expect_keyword(COLUMNS)?;

                let _ = self.consume_token(&Token::Eq);

                let value = self
                    .parse_option_sequence(Parser::parse_item_name)?
                    .map(|inner| {
                        WithOptionValue::Sequence(
                            inner
                                .into_iter()
                                .map(WithOptionValue::UnresolvedItemName)
                                .collect_vec(),
                        )
                    });

                Ok(SqlServerConfigOption {
                    name: SqlServerConfigOptionName::TextColumns,
                    value,
                })
            }
            EXCLUDE => {
                self.expect_keyword(COLUMNS)?;

                let _ = self.consume_token(&Token::Eq);

                let value = self
                    .parse_option_sequence(Parser::parse_item_name)?
                    .map(|inner| {
                        WithOptionValue::Sequence(
                            inner
                                .into_iter()
                                .map(WithOptionValue::UnresolvedItemName)
                                .collect_vec(),
                        )
                    });

                Ok(SqlServerConfigOption {
                    name: SqlServerConfigOptionName::ExcludeColumns,
                    value,
                })
            }
            _ => unreachable!(),
        }
    }

    fn parse_load_generator_option(&mut self) -> Result<LoadGeneratorOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[
            AS,
            UP,
            SCALE,
            TICK,
            MAX,
            KEYS,
            SNAPSHOT,
            TRANSACTIONAL,
            VALUE,
            SEED,
            PARTITIONS,
            BATCH,
        ])? {
            AS => {
                self.expect_keyword(OF)?;
                LoadGeneratorOptionName::AsOf
            }
            UP => {
                self.expect_keyword(TO)?;
                LoadGeneratorOptionName::UpTo
            }
            SCALE => {
                self.expect_keyword(FACTOR)?;
                LoadGeneratorOptionName::ScaleFactor
            }
            TICK => {
                self.expect_keyword(INTERVAL)?;
                LoadGeneratorOptionName::TickInterval
            }
            MAX => {
                self.expect_keyword(CARDINALITY)?;
                LoadGeneratorOptionName::MaxCardinality
            }
            KEYS => LoadGeneratorOptionName::Keys,
            SNAPSHOT => {
                self.expect_keyword(ROUNDS)?;
                LoadGeneratorOptionName::SnapshotRounds
            }
            TRANSACTIONAL => {
                self.expect_keyword(SNAPSHOT)?;
                LoadGeneratorOptionName::TransactionalSnapshot
            }
            VALUE => {
                self.expect_keyword(SIZE)?;
                LoadGeneratorOptionName::ValueSize
            }
            SEED => LoadGeneratorOptionName::Seed,
            PARTITIONS => LoadGeneratorOptionName::Partitions,
            BATCH => {
                self.expect_keyword(SIZE)?;
                LoadGeneratorOptionName::BatchSize
            }
            _ => unreachable!(),
        };

        let _ = self.consume_token(&Token::Eq);
        Ok(LoadGeneratorOption {
            name,
            value: self.parse_optional_option_value()?,
        })
    }

    fn parse_create_kafka_sink_connection(
        &mut self,
    ) -> Result<CreateSinkConnection<Raw>, ParserError> {
        self.expect_keyword(CONNECTION)?;

        let connection = self.parse_raw_name()?;

        let options = if self.consume_token(&Token::LParen) {
            let options = self.parse_comma_separated(Parser::parse_kafka_sink_config_option)?;
            self.expect_token(&Token::RParen)?;
            options
        } else {
            vec![]
        };

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
                Some(SinkKey {
                    key_columns,
                    not_enforced,
                })
            } else {
                None
            };

        let headers = if self.parse_keyword(HEADERS) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(CreateSinkConnection::Kafka {
            connection,
            options,
            key,
            headers,
        })
    }

    fn parse_create_iceberg_sink_connection(
        &mut self,
    ) -> Result<CreateSinkConnection<Raw>, ParserError> {
        self.expect_keyword(CONNECTION)?;
        let connection = self.parse_raw_name()?;

        let options = if self.consume_token(&Token::LParen) {
            let options = self.parse_comma_separated(Parser::parse_iceberg_sink_config_option)?;
            self.expect_token(&Token::RParen)?;
            options
        } else {
            vec![]
        };

        self.expect_keywords(&[USING, AWS, CONNECTION])?;
        let aws_connection = self.parse_raw_name()?;

        let key = if self.parse_keyword(KEY) {
            let key_columns = self.parse_parenthesized_column_list(Mandatory)?;

            let not_enforced = self.parse_keywords(&[NOT, ENFORCED]);
            Some(SinkKey {
                key_columns,
                not_enforced,
            })
        } else {
            None
        };

        Ok(CreateSinkConnection::Iceberg {
            connection,
            aws_connection,
            key,
            options,
        })
    }

    fn parse_create_sink_connection(&mut self) -> Result<CreateSinkConnection<Raw>, ParserError> {
        match self.expect_one_of_keywords(&[KAFKA, ICEBERG])? {
            KAFKA => self.parse_create_kafka_sink_connection(),
            ICEBERG => {
                self.expect_keyword(CATALOG)?;
                self.parse_create_iceberg_sink_connection()
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
        let name = self.parse_item_name()?;
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

        let name = self.parse_item_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        let replacing = self
            .parse_keyword(REPLACING)
            .then(|| self.parse_raw_name())
            .transpose()?;
        let in_cluster = self.parse_optional_in_cluster()?;

        let with_options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_materialized_view_option)?;
            self.expect_token(&Token::RParen)?;
            options
        } else {
            vec![]
        };

        self.expect_keyword(AS)?;
        let query = self.parse_query()?;
        let as_of = self.parse_optional_internal_as_of()?;

        Ok(Statement::CreateMaterializedView(
            CreateMaterializedViewStatement {
                if_exists,
                name,
                columns,
                replacing,
                in_cluster,
                query,
                as_of,
                with_options,
            },
        ))
    }

    fn parse_create_continual_task(&mut self) -> Result<Statement<Raw>, ParserError> {
        // TODO(ct3): OR REPLACE/IF NOT EXISTS.
        self.expect_keywords(&[CONTINUAL, TASK])?;

        // TODO(ct3): Multiple outputs.
        let name = RawItemName::Name(self.parse_item_name()?);
        let columns = match self.consume_token(&Token::LParen) {
            true => {
                let columns = self.parse_comma_separated(|parser| {
                    Ok(CteMutRecColumnDef {
                        name: parser.parse_identifier()?,
                        data_type: parser.parse_data_type()?,
                    })
                })?;
                self.expect_token(&Token::RParen)?;
                Some(columns)
            }
            false => None,
        };
        let in_cluster = self.parse_optional_in_cluster()?;
        let with_options = self.parse_create_continual_task_with_options()?;

        // TODO(ct3): Multiple inputs.
        self.expect_keywords(&[ON, INPUT])?;
        let input = self.parse_raw_name()?;
        // TODO(ct3): Allow renaming the inserts/deletes so that we can use
        // something as both an "input" and a "reference".

        // Also try to parse WITH options in the old location. We never exposed
        // this to users, so this can be removed once the CI upgrade tests have
        // moved past these old versions.
        let legacy_with_options = self.parse_create_continual_task_with_options()?;
        let with_options = match (!with_options.is_empty(), !legacy_with_options.is_empty()) {
            (_, false) => with_options,
            (false, true) => legacy_with_options,
            (true, true) => {
                return parser_err!(
                    self,
                    self.peek_prev_pos(),
                    "CREATE CONTINUAL TASK with options in both new and legacy locations"
                );
            }
        };

        self.expect_keyword(AS)?;

        let mut stmts = Vec::new();
        let mut expecting_statement_delimiter = false;
        self.expect_token(&Token::LParen)?;
        // TODO(ct2): Dedup this with parse_statements?
        loop {
            // ignore empty statements (between successive statement delimiters)
            while self.consume_token(&Token::Semicolon) {
                expecting_statement_delimiter = false;
            }

            if self.consume_token(&Token::RParen) {
                break;
            } else if expecting_statement_delimiter {
                self.expected(self.peek_pos(), "end of statement", self.peek_token())?
            }

            let stmt = self.parse_statement().map_err(|err| err.error)?.ast;
            match stmt {
                Statement::Delete(stmt) => stmts.push(ContinualTaskStmt::Delete(stmt)),
                Statement::Insert(stmt) => stmts.push(ContinualTaskStmt::Insert(stmt)),
                _ => {
                    return parser_err!(
                        self,
                        self.peek_prev_pos(),
                        "unsupported query in CREATE CONTINUAL TASK"
                    );
                }
            }
            expecting_statement_delimiter = true;
        }

        let as_of = self.parse_optional_internal_as_of()?;

        Ok(Statement::CreateContinualTask(
            CreateContinualTaskStatement {
                name,
                columns,
                in_cluster,
                with_options,
                input,
                stmts,
                as_of,
                sugar: None,
            },
        ))
    }

    fn parse_create_continual_task_from_transform(
        &mut self,
    ) -> Result<Statement<Raw>, ParserError> {
        self.expect_keywords(&[CONTINUAL, TASK])?;
        let name = RawItemName::Name(self.parse_item_name()?);
        let in_cluster = self.parse_optional_in_cluster()?;
        let with_options = self.parse_create_continual_task_with_options()?;

        self.expect_keywords(&[FROM, TRANSFORM])?;
        let input = self.parse_raw_name()?;

        self.expect_keyword(USING)?;
        self.expect_token(&Token::LParen)?;
        let transform = self.parse_query()?;
        self.expect_token(&Token::RParen)?;

        let as_of = self.parse_optional_internal_as_of()?;

        // `INSERT INTO name SELECT * FROM <transform>`
        let insert = InsertStatement {
            table_name: name.clone(),
            columns: Vec::new(),
            source: InsertSource::Query(transform.clone()),
            returning: Vec::new(),
        };

        // Desugar into a normal CT body.
        let stmts = vec![ContinualTaskStmt::Insert(insert)];

        Ok(Statement::CreateContinualTask(
            CreateContinualTaskStatement {
                name,
                columns: None,
                in_cluster,
                with_options,
                input,
                stmts,
                as_of,
                sugar: Some(CreateContinualTaskSugar::Transform { transform }),
            },
        ))
    }

    fn parse_create_continual_task_from_retain(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keywords(&[CONTINUAL, TASK])?;
        let name = RawItemName::Name(self.parse_item_name()?);
        let in_cluster = self.parse_optional_in_cluster()?;
        let with_options = self.parse_create_continual_task_with_options()?;

        self.expect_keywords(&[FROM, RETAIN])?;
        let input = self.parse_raw_name()?;

        self.expect_keyword(WHILE)?;
        self.expect_token(&Token::LParen)?;
        let retain = self.parse_expr()?;
        self.expect_token(&Token::RParen)?;

        let as_of = self.parse_optional_internal_as_of()?;

        // `INSERT INTO name SELECT * FROM input WHERE <retain>`
        let insert = InsertStatement {
            table_name: name.clone(),
            columns: Vec::new(),
            source: InsertSource::Query(Query {
                ctes: CteBlock::Simple(Vec::new()),
                body: SetExpr::Select(Box::new(Select {
                    from: vec![TableWithJoins {
                        relation: TableFactor::Table {
                            name: input.clone(),
                            alias: None,
                        },
                        joins: Vec::new(),
                    }],
                    selection: Some(retain.clone()),
                    distinct: None,
                    projection: vec![SelectItem::Wildcard],
                    group_by: Vec::new(),
                    having: None,
                    qualify: None,
                    options: Vec::new(),
                })),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            }),
            returning: Vec::new(),
        };

        // `DELETE FROM name WHERE NOT <retain>`
        let delete = DeleteStatement {
            table_name: name.clone(),
            alias: None,
            using: Vec::new(),
            selection: Some(retain.clone().negate()),
        };

        // Desugar into a normal CT body.
        let stmts = vec![
            ContinualTaskStmt::Insert(insert),
            ContinualTaskStmt::Delete(delete),
        ];

        Ok(Statement::CreateContinualTask(
            CreateContinualTaskStatement {
                name,
                columns: None,
                in_cluster,
                with_options,
                input,
                stmts,
                as_of,
                sugar: Some(CreateContinualTaskSugar::Retain { retain }),
            },
        ))
    }

    fn parse_materialized_view_option_name(
        &mut self,
    ) -> Result<MaterializedViewOptionName, ParserError> {
        let option = self.expect_one_of_keywords(&[ASSERT, PARTITION, RETAIN, REFRESH])?;
        let name = match option {
            ASSERT => {
                self.expect_keywords(&[NOT, NULL])?;
                MaterializedViewOptionName::AssertNotNull
            }
            PARTITION => {
                self.expect_keyword(BY)?;
                MaterializedViewOptionName::PartitionBy
            }
            RETAIN => {
                self.expect_keyword(HISTORY)?;
                MaterializedViewOptionName::RetainHistory
            }
            REFRESH => MaterializedViewOptionName::Refresh,
            _ => unreachable!(),
        };
        Ok(name)
    }

    fn parse_materialized_view_option(
        &mut self,
    ) -> Result<MaterializedViewOption<Raw>, ParserError> {
        let name = self.parse_materialized_view_option_name()?;
        let value = match name {
            MaterializedViewOptionName::RetainHistory => self.parse_option_retain_history()?,
            MaterializedViewOptionName::Refresh => {
                Some(self.parse_materialized_view_refresh_option_value()?)
            }
            _ => self.parse_optional_option_value()?,
        };
        Ok(MaterializedViewOption { name, value })
    }

    fn parse_option_retain_history(&mut self) -> Result<Option<WithOptionValue<Raw>>, ParserError> {
        Ok(Some(self.parse_retain_history()?))
    }

    fn parse_retain_history(&mut self) -> Result<WithOptionValue<Raw>, ParserError> {
        let _ = self.consume_token(&Token::Eq);
        self.expect_keyword(FOR)?;
        let value = self.parse_value()?;
        Ok(WithOptionValue::RetainHistoryFor(value))
    }

    fn parse_materialized_view_refresh_option_value(
        &mut self,
    ) -> Result<WithOptionValue<Raw>, ParserError> {
        let _ = self.consume_token(&Token::Eq);

        match self.expect_one_of_keywords(&[ON, AT, EVERY])? {
            ON => {
                self.expect_keyword(COMMIT)?;
                Ok(WithOptionValue::Refresh(RefreshOptionValue::OnCommit))
            }
            AT => {
                if self.parse_keyword(CREATION) {
                    Ok(WithOptionValue::Refresh(RefreshOptionValue::AtCreation))
                } else {
                    Ok(WithOptionValue::Refresh(RefreshOptionValue::At(
                        RefreshAtOptionValue {
                            time: self.parse_expr()?,
                        },
                    )))
                }
            }
            EVERY => {
                let interval = self.parse_interval_value()?;
                let aligned_to = if self.parse_keywords(&[ALIGNED, TO]) {
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                Ok(WithOptionValue::Refresh(RefreshOptionValue::Every(
                    RefreshEveryOptionValue {
                        interval,
                        aligned_to,
                    },
                )))
            }
            _ => unreachable!(),
        }
    }

    fn parse_create_continual_task_with_options(
        &mut self,
    ) -> Result<Vec<ContinualTaskOption<Raw>>, ParserError> {
        if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_continual_task_option)?;
            self.expect_token(&Token::RParen)?;
            Ok(options)
        } else {
            Ok(vec![])
        }
    }

    fn parse_continual_task_option_name(&mut self) -> Result<ContinualTaskOptionName, ParserError> {
        let option = self.expect_one_of_keywords(&[SNAPSHOT])?;
        let name = match option {
            SNAPSHOT => ContinualTaskOptionName::Snapshot,
            _ => unreachable!(),
        };
        Ok(name)
    }

    fn parse_continual_task_option(&mut self) -> Result<ContinualTaskOption<Raw>, ParserError> {
        let name = self.parse_continual_task_option_name()?;
        let value = self.parse_optional_option_value()?;
        Ok(ContinualTaskOption { name, value })
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

    fn parse_table_option_name(&mut self) -> Result<TableOptionName, ParserError> {
        // this is only so we can test redacted values, of which no other
        // examples exist as of its introduction.
        if self.parse_keyword(REDACTED) {
            return Ok(TableOptionName::RedactedTest);
        }
        let name = match self.expect_one_of_keywords(&[PARTITION, RETAIN])? {
            PARTITION => {
                self.expect_keyword(BY)?;
                TableOptionName::PartitionBy
            }
            RETAIN => {
                self.expect_keyword(HISTORY)?;
                TableOptionName::RetainHistory
            }
            _ => unreachable!(),
        };
        Ok(name)
    }

    fn parse_table_option(&mut self) -> Result<TableOption<Raw>, ParserError> {
        let name = self.parse_table_option_name()?;
        let value = match name {
            TableOptionName::PartitionBy => self.parse_optional_option_value(),
            TableOptionName::RetainHistory => self.parse_option_retain_history(),
            TableOptionName::RedactedTest => self.parse_optional_option_value(),
        }?;
        Ok(TableOption { name, value })
    }

    fn parse_index_option_name(&mut self) -> Result<IndexOptionName, ParserError> {
        self.expect_keywords(&[RETAIN, HISTORY])?;
        Ok(IndexOptionName::RetainHistory)
    }

    fn parse_index_option(&mut self) -> Result<IndexOption<Raw>, ParserError> {
        let name = self.parse_index_option_name()?;
        let value = match name {
            IndexOptionName::RetainHistory => self.parse_option_retain_history(),
        }?;
        Ok(IndexOption { name, value })
    }

    fn parse_raw_ident(&mut self) -> Result<RawClusterName, ParserError> {
        if self.consume_token(&Token::LBracket) {
            let id = self.parse_raw_ident_str()?;
            self.expect_token(&Token::RBracket)?;
            Ok(RawClusterName::Resolved(id))
        } else {
            Ok(RawClusterName::Unresolved(self.parse_identifier()?))
        }
    }

    fn parse_raw_network_policy_name(&mut self) -> Result<RawNetworkPolicyName, ParserError> {
        if self.consume_token(&Token::LBracket) {
            let id = self.parse_raw_ident_str()?;
            self.expect_token(&Token::RBracket)?;
            Ok(RawNetworkPolicyName::Resolved(id))
        } else {
            Ok(RawNetworkPolicyName::Unresolved(self.parse_identifier()?))
        }
    }

    fn parse_raw_ident_str(&mut self) -> Result<String, ParserError> {
        match self.next_token() {
            Some(Token::Ident(id)) => Ok(id.into_inner()),
            Some(Token::Number(n)) => Ok(n),
            _ => parser_err!(self, self.peek_prev_pos(), "expected id"),
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
        self.expect_keyword(ROLE)?;
        let name = self.parse_identifier()?;
        let _ = self.parse_keyword(WITH);
        let options = self.parse_role_attributes()?;
        Ok(Statement::CreateRole(CreateRoleStatement { name, options }))
    }

    fn parse_role_attributes(&mut self) -> Result<Vec<RoleAttribute>, ParserError> {
        let mut options = vec![];
        loop {
            match self.parse_one_of_keywords(&[
                SUPERUSER,
                NOSUPERUSER,
                LOGIN,
                NOLOGIN,
                INHERIT,
                NOINHERIT,
                CREATECLUSTER,
                NOCREATECLUSTER,
                CREATEDB,
                NOCREATEDB,
                CREATEROLE,
                NOCREATEROLE,
                PASSWORD,
            ]) {
                None => break,
                Some(SUPERUSER) => options.push(RoleAttribute::SuperUser),
                Some(NOSUPERUSER) => options.push(RoleAttribute::NoSuperUser),
                Some(LOGIN) => options.push(RoleAttribute::Login),
                Some(NOLOGIN) => options.push(RoleAttribute::NoLogin),
                Some(INHERIT) => options.push(RoleAttribute::Inherit),
                Some(NOINHERIT) => options.push(RoleAttribute::NoInherit),
                Some(CREATECLUSTER) => options.push(RoleAttribute::CreateCluster),
                Some(NOCREATECLUSTER) => options.push(RoleAttribute::NoCreateCluster),
                Some(CREATEDB) => options.push(RoleAttribute::CreateDB),
                Some(NOCREATEDB) => options.push(RoleAttribute::NoCreateDB),
                Some(CREATEROLE) => options.push(RoleAttribute::CreateRole),
                Some(NOCREATEROLE) => options.push(RoleAttribute::NoCreateRole),
                Some(PASSWORD) => {
                    if self.parse_keyword(NULL) {
                        options.push(RoleAttribute::Password(None));
                        continue;
                    }
                    let password = self.parse_literal_string()?;
                    options.push(RoleAttribute::Password(Some(password)));
                }
                Some(_) => unreachable!(),
            }
        }
        Ok(options)
    }

    fn parse_create_secret(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(SECRET)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_item_name()?;
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
        let name = self.parse_item_name()?;
        self.expect_keyword(AS)?;

        match self.parse_one_of_keywords(&[LIST, MAP]) {
            Some(LIST) => {
                self.expect_token(&Token::LParen)?;
                let options = self.parse_comma_separated(Parser::parse_create_type_list_option)?;
                self.expect_token(&Token::RParen)?;
                Ok(Statement::CreateType(CreateTypeStatement {
                    name,
                    as_type: CreateTypeAs::List { options },
                }))
            }
            Some(MAP) => {
                self.expect_token(&Token::LParen)?;
                let options = self.parse_comma_separated(Parser::parse_create_type_map_option)?;
                self.expect_token(&Token::RParen)?;
                Ok(Statement::CreateType(CreateTypeStatement {
                    name,
                    as_type: CreateTypeAs::Map { options },
                }))
            }
            None => {
                let column_defs = self.parse_composite_type_definition()?;

                Ok(Statement::CreateType(CreateTypeStatement {
                    name,
                    as_type: CreateTypeAs::Record { column_defs },
                }))
            }
            _ => unreachable!(),
        }
    }

    fn parse_create_type_list_option(&mut self) -> Result<CreateTypeListOption<Raw>, ParserError> {
        self.expect_keywords(&[ELEMENT, TYPE])?;
        let name = CreateTypeListOptionName::ElementType;
        Ok(CreateTypeListOption {
            name,
            value: Some(self.parse_data_type_option_value()?),
        })
    }

    fn parse_create_type_map_option(&mut self) -> Result<CreateTypeMapOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[KEY, VALUE])? {
            KEY => {
                self.expect_keyword(TYPE)?;
                CreateTypeMapOptionName::KeyType
            }
            VALUE => {
                self.expect_keyword(TYPE)?;
                CreateTypeMapOptionName::ValueType
            }
            _ => unreachable!(),
        };
        Ok(CreateTypeMapOption {
            name,
            value: Some(self.parse_data_type_option_value()?),
        })
    }

    fn parse_create_cluster(&mut self) -> Result<Statement<Raw>, ParserError> {
        let name = self.parse_identifier()?;
        // For historical reasons, the parentheses around the options can be
        // omitted.
        let paren = self.consume_token(&Token::LParen);
        let options = self.parse_comma_separated(Parser::parse_cluster_option)?;
        if paren {
            self.expect_token(&Token::RParen)?;
        }

        let features = if self.parse_keywords(&[FEATURES]) {
            self.expect_token(&Token::LParen)?;
            let features = self.parse_comma_separated(Parser::parse_cluster_feature)?;
            self.expect_token(&Token::RParen)?;
            features
        } else {
            Vec::new()
        };

        Ok(Statement::CreateCluster(CreateClusterStatement {
            name,
            options,
            features,
        }))
    }

    fn parse_cluster_option_name(&mut self) -> Result<ClusterOptionName, ParserError> {
        let option = self.expect_one_of_keywords(&[
            AVAILABILITY,
            DISK,
            INTROSPECTION,
            MANAGED,
            REPLICAS,
            REPLICATION,
            SIZE,
            SCHEDULE,
            WORKLOAD,
        ])?;
        let name = match option {
            AVAILABILITY => {
                self.expect_keyword(ZONES)?;
                ClusterOptionName::AvailabilityZones
            }
            DISK => ClusterOptionName::Disk,
            INTROSPECTION => match self.expect_one_of_keywords(&[DEBUGGING, INTERVAL])? {
                DEBUGGING => ClusterOptionName::IntrospectionDebugging,
                INTERVAL => ClusterOptionName::IntrospectionInterval,
                _ => unreachable!(),
            },
            MANAGED => ClusterOptionName::Managed,
            REPLICAS => ClusterOptionName::Replicas,
            REPLICATION => {
                self.expect_keyword(FACTOR)?;
                ClusterOptionName::ReplicationFactor
            }
            SIZE => ClusterOptionName::Size,
            SCHEDULE => ClusterOptionName::Schedule,
            WORKLOAD => {
                self.expect_keyword(CLASS)?;
                ClusterOptionName::WorkloadClass
            }
            _ => unreachable!(),
        };
        Ok(name)
    }

    fn parse_cluster_option(&mut self) -> Result<ClusterOption<Raw>, ParserError> {
        let name = self.parse_cluster_option_name()?;

        match name {
            ClusterOptionName::Replicas => self.parse_cluster_option_replicas(),
            ClusterOptionName::Schedule => self.parse_cluster_option_schedule(),
            _ => {
                let value = self.parse_optional_option_value()?;
                Ok(ClusterOption { name, value })
            }
        }
    }

    fn parse_alter_cluster_option(&mut self) -> Result<ClusterAlterOption<Raw>, ParserError> {
        let (name, value) = match self.expect_one_of_keywords(&[WAIT])? {
            WAIT => {
                let _ = self.consume_token(&Token::Eq);
                let v = match self.expect_one_of_keywords(&[FOR, UNTIL])? {
                    FOR => Some(WithOptionValue::ClusterAlterStrategy(
                        ClusterAlterOptionValue::For(self.parse_value()?),
                    )),
                    UNTIL => {
                        self.expect_keyword(READY)?;
                        let _ = self.consume_token(&Token::Eq);
                        let _ = self.expect_token(&Token::LParen)?;
                        let opts = Some(WithOptionValue::ClusterAlterStrategy(
                            ClusterAlterOptionValue::UntilReady(self.parse_comma_separated(
                                Parser::parse_cluster_alter_until_ready_option,
                            )?),
                        ));
                        let _ = self.expect_token(&Token::RParen)?;
                        opts
                    }
                    _ => unreachable!(),
                };
                (ClusterAlterOptionName::Wait, v)
            }
            _ => unreachable!(),
        };
        Ok(ClusterAlterOption { name, value })
    }

    fn parse_cluster_alter_until_ready_option(
        &mut self,
    ) -> Result<ClusterAlterUntilReadyOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[TIMEOUT, ON])? {
            ON => {
                self.expect_keywords(&[TIMEOUT])?;
                ClusterAlterUntilReadyOptionName::OnTimeout
            }
            TIMEOUT => ClusterAlterUntilReadyOptionName::Timeout,
            _ => unreachable!(),
        };
        let value = self.parse_optional_option_value()?;
        Ok(ClusterAlterUntilReadyOption { name, value })
    }

    fn parse_cluster_option_replicas(&mut self) -> Result<ClusterOption<Raw>, ParserError> {
        let _ = self.consume_token(&Token::Eq);
        self.expect_token(&Token::LParen)?;
        let replicas = if self.consume_token(&Token::RParen) {
            vec![]
        } else {
            let replicas = self.parse_comma_separated(|parser| {
                let name = parser.parse_identifier()?;
                parser.expect_token(&Token::LParen)?;
                let options = parser.parse_comma_separated(Parser::parse_replica_option)?;
                parser.expect_token(&Token::RParen)?;
                Ok(ReplicaDefinition { name, options })
            })?;
            self.expect_token(&Token::RParen)?;
            replicas
        };
        Ok(ClusterOption {
            name: ClusterOptionName::Replicas,
            value: Some(WithOptionValue::ClusterReplicas(replicas)),
        })
    }

    fn parse_cluster_option_schedule(&mut self) -> Result<ClusterOption<Raw>, ParserError> {
        let _ = self.consume_token(&Token::Eq);
        let kw = self.expect_one_of_keywords(&[MANUAL, ON])?;
        let value = match kw {
            MANUAL => ClusterScheduleOptionValue::Manual,
            ON => {
                self.expect_keyword(REFRESH)?;
                // Parse optional `(HYDRATION TIME ESTIMATE ...)`
                let hydration_time_estimate = if self.consume_token(&Token::LParen) {
                    self.expect_keywords(&[HYDRATION, TIME, ESTIMATE])?;
                    let _ = self.consume_token(&Token::Eq);
                    let interval = self.parse_interval_value()?;
                    self.expect_token(&Token::RParen)?;
                    Some(interval)
                } else {
                    None
                };
                ClusterScheduleOptionValue::Refresh {
                    hydration_time_estimate,
                }
            }
            _ => unreachable!(),
        };
        Ok(ClusterOption {
            name: ClusterOptionName::Schedule,
            value: Some(WithOptionValue::ClusterScheduleOptionValue(value)),
        })
    }

    fn parse_replica_option(&mut self) -> Result<ReplicaOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[
            AVAILABILITY,
            BILLED,
            COMPUTE,
            COMPUTECTL,
            DISK,
            INTERNAL,
            INTROSPECTION,
            SIZE,
            STORAGE,
            STORAGECTL,
            WORKERS,
        ])? {
            AVAILABILITY => {
                self.expect_keyword(ZONE)?;
                ReplicaOptionName::AvailabilityZone
            }
            BILLED => {
                self.expect_keyword(AS)?;
                ReplicaOptionName::BilledAs
            }
            COMPUTE => {
                self.expect_keyword(ADDRESSES)?;
                ReplicaOptionName::ComputeAddresses
            }
            COMPUTECTL => {
                self.expect_keyword(ADDRESSES)?;
                ReplicaOptionName::ComputectlAddresses
            }
            DISK => ReplicaOptionName::Disk,
            INTERNAL => ReplicaOptionName::Internal,
            INTROSPECTION => match self.expect_one_of_keywords(&[DEBUGGING, INTERVAL])? {
                DEBUGGING => ReplicaOptionName::IntrospectionDebugging,
                INTERVAL => ReplicaOptionName::IntrospectionInterval,
                _ => unreachable!(),
            },
            SIZE => ReplicaOptionName::Size,
            STORAGE => {
                self.expect_keyword(ADDRESSES)?;
                ReplicaOptionName::StorageAddresses
            }
            STORAGECTL => {
                self.expect_keyword(ADDRESSES)?;
                ReplicaOptionName::StoragectlAddresses
            }
            WORKERS => ReplicaOptionName::Workers,
            _ => unreachable!(),
        };
        let value = self.parse_optional_option_value()?;
        Ok(ReplicaOption { name, value })
    }

    fn parse_cluster_feature(&mut self) -> Result<ClusterFeature<Raw>, ParserError> {
        Ok(ClusterFeature {
            name: self.parse_cluster_feature_name()?,
            value: self.parse_optional_option_value()?,
        })
    }

    fn parse_create_cluster_replica(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.next_token();
        let of_cluster = self.parse_identifier()?;
        self.expect_token(&Token::Dot)?;
        let name = self.parse_identifier()?;
        // For historical reasons, the parentheses around the options can be
        // omitted.
        let paren = self.consume_token(&Token::LParen);
        let options = self.parse_comma_separated(Parser::parse_replica_option)?;
        if paren {
            let _ = self.consume_token(&Token::RParen);
        }
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

    fn parse_alias(&mut self) -> Result<Option<Ident>, ParserError> {
        self.parse_keyword(AS)
            .then(|| self.parse_identifier())
            .transpose()
    }

    fn parse_source_include_metadata(&mut self) -> Result<Vec<SourceIncludeMetadata>, ParserError> {
        if self.parse_keyword(INCLUDE) {
            self.parse_comma_separated(|parser| {
                let metadata = match parser
                    .expect_one_of_keywords(&[KEY, TIMESTAMP, PARTITION, OFFSET, HEADERS, HEADER])?
                {
                    KEY => SourceIncludeMetadata::Key {
                        alias: parser.parse_alias()?,
                    },
                    TIMESTAMP => SourceIncludeMetadata::Timestamp {
                        alias: parser.parse_alias()?,
                    },
                    PARTITION => SourceIncludeMetadata::Partition {
                        alias: parser.parse_alias()?,
                    },
                    OFFSET => SourceIncludeMetadata::Offset {
                        alias: parser.parse_alias()?,
                    },
                    HEADERS => SourceIncludeMetadata::Headers {
                        alias: parser.parse_alias()?,
                    },
                    HEADER => {
                        let key: String = parser.parse_literal_string()?;
                        parser.expect_keyword(AS)?;
                        let alias = parser.parse_identifier()?;
                        let use_bytes = parser.parse_keyword(BYTES);
                        SourceIncludeMetadata::Header {
                            alias,
                            key,
                            use_bytes,
                        }
                    }
                    _ => unreachable!("only explicitly allowed items can be parsed"),
                };
                Ok(metadata)
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

    fn parse_drop(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        if self.parse_keyword(OWNED) {
            self.parse_drop_owned()
                .map_parser_err(StatementKind::DropOwned)
        } else {
            self.parse_drop_objects()
                .map_parser_err(StatementKind::DropObjects)
        }
    }

    fn parse_drop_objects(&mut self) -> Result<Statement<Raw>, ParserError> {
        let object_type = self.expect_object_type()?;
        let if_exists = self.parse_if_exists()?;
        match object_type {
            ObjectType::Database => {
                let name = UnresolvedObjectName::Database(self.parse_database_name()?);
                let restrict = matches!(
                    self.parse_at_most_one_keyword(&[CASCADE, RESTRICT], "DROP")?,
                    Some(RESTRICT),
                );
                Ok(Statement::DropObjects(DropObjectsStatement {
                    object_type: ObjectType::Database,
                    if_exists,
                    names: vec![name],
                    cascade: !restrict,
                }))
            }
            ObjectType::Schema => {
                let names = self.parse_comma_separated(|parser| {
                    Ok(UnresolvedObjectName::Schema(parser.parse_schema_name()?))
                })?;

                let cascade = matches!(
                    self.parse_at_most_one_keyword(&[CASCADE, RESTRICT], "DROP")?,
                    Some(CASCADE),
                );
                Ok(Statement::DropObjects(DropObjectsStatement {
                    object_type: ObjectType::Schema,
                    if_exists,
                    names,
                    cascade,
                }))
            }
            ObjectType::Role => {
                let names = self.parse_comma_separated(|parser| {
                    Ok(UnresolvedObjectName::Role(parser.parse_identifier()?))
                })?;
                Ok(Statement::DropObjects(DropObjectsStatement {
                    object_type: ObjectType::Role,
                    if_exists,
                    names,
                    cascade: false,
                }))
            }
            ObjectType::NetworkPolicy => {
                let names = self.parse_comma_separated(|parser| {
                    Ok(UnresolvedObjectName::NetworkPolicy(
                        parser.parse_identifier()?,
                    ))
                })?;
                Ok(Statement::DropObjects(DropObjectsStatement {
                    object_type: ObjectType::NetworkPolicy,
                    if_exists,
                    names,
                    cascade: false,
                }))
            }
            ObjectType::Cluster => self.parse_drop_clusters(if_exists),
            ObjectType::ClusterReplica => self.parse_drop_cluster_replicas(if_exists),
            ObjectType::Table
            | ObjectType::View
            | ObjectType::MaterializedView
            | ObjectType::Source
            | ObjectType::Sink
            | ObjectType::Index
            | ObjectType::Type
            | ObjectType::Secret
            | ObjectType::Connection
            | ObjectType::ContinualTask => {
                let names = self.parse_comma_separated(|parser| {
                    Ok(UnresolvedObjectName::Item(parser.parse_item_name()?))
                })?;
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
            ObjectType::Func | ObjectType::Subsource => parser_err!(
                self,
                self.peek_prev_pos(),
                format!("Unsupported DROP on {object_type}")
            ),
        }
    }

    fn parse_drop_clusters(&mut self, if_exists: bool) -> Result<Statement<Raw>, ParserError> {
        let names = self.parse_comma_separated(|parser| {
            Ok(UnresolvedObjectName::Cluster(parser.parse_identifier()?))
        })?;
        let cascade = matches!(
            self.parse_at_most_one_keyword(&[CASCADE, RESTRICT], "DROP")?,
            Some(CASCADE),
        );
        Ok(Statement::DropObjects(DropObjectsStatement {
            object_type: ObjectType::Cluster,
            if_exists,
            names,
            cascade,
        }))
    }

    fn parse_drop_cluster_replicas(
        &mut self,
        if_exists: bool,
    ) -> Result<Statement<Raw>, ParserError> {
        let names = self.parse_comma_separated(|p| {
            Ok(UnresolvedObjectName::ClusterReplica(
                p.parse_cluster_replica_name()?,
            ))
        })?;
        Ok(Statement::DropObjects(DropObjectsStatement {
            object_type: ObjectType::ClusterReplica,
            if_exists,
            names,
            cascade: false,
        }))
    }

    fn parse_drop_owned(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(BY)?;
        let role_names = self.parse_comma_separated(Parser::parse_identifier)?;
        let cascade = if self.parse_keyword(CASCADE) {
            Some(true)
        } else if self.parse_keyword(RESTRICT) {
            Some(false)
        } else {
            None
        };
        Ok(Statement::DropOwned(DropOwnedStatement {
            role_names,
            cascade,
        }))
    }

    fn parse_cluster_replica_name(&mut self) -> Result<QualifiedReplica, ParserError> {
        let cluster = self.parse_identifier()?;
        self.expect_token(&Token::Dot)?;
        let replica = self.parse_identifier()?;
        Ok(QualifiedReplica { cluster, replica })
    }

    fn parse_alter_network_policy(&mut self) -> Result<Statement<Raw>, ParserError> {
        let name = self.parse_identifier()?;
        self.expect_keyword(SET)?;
        self.expect_token(&Token::LParen)?;
        let options = self.parse_comma_separated(Parser::parse_network_policy_option)?;
        self.expect_token(&Token::RParen)?;
        Ok(Statement::AlterNetworkPolicy(AlterNetworkPolicyStatement {
            name,
            options,
        }))
    }

    fn parse_create_network_policy(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keywords(&[NETWORK, POLICY])?;
        let name = self.parse_identifier()?;
        self.expect_token(&Token::LParen)?;
        let options = self.parse_comma_separated(Parser::parse_network_policy_option)?;
        self.expect_token(&Token::RParen)?;
        Ok(Statement::CreateNetworkPolicy(
            CreateNetworkPolicyStatement { name, options },
        ))
    }

    fn parse_network_policy_option(&mut self) -> Result<NetworkPolicyOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[RULES])? {
            RULES => NetworkPolicyOptionName::Rules,
            v => panic!("found unreachable keyword {}", v),
        };
        match name {
            NetworkPolicyOptionName::Rules => self.parse_network_policy_option_rules(),
        }
    }

    fn parse_network_policy_option_rules(
        &mut self,
    ) -> Result<NetworkPolicyOption<Raw>, ParserError> {
        let _ = self.consume_token(&Token::Eq);
        self.expect_token(&Token::LParen)?;
        let rules = if self.consume_token(&Token::RParen) {
            vec![]
        } else {
            let rules = self.parse_comma_separated(|parser| {
                let name = parser.parse_identifier()?;
                parser.expect_token(&Token::LParen)?;
                let options =
                    parser.parse_comma_separated(Parser::parse_network_policy_rule_option)?;
                parser.expect_token(&Token::RParen)?;
                Ok(NetworkPolicyRuleDefinition { name, options })
            })?;
            self.expect_token(&Token::RParen)?;
            rules
        };
        Ok(NetworkPolicyOption {
            name: NetworkPolicyOptionName::Rules,
            value: Some(WithOptionValue::NetworkPolicyRules(rules)),
        })
    }

    fn parse_network_policy_rule_option(
        &mut self,
    ) -> Result<NetworkPolicyRuleOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[ACTION, ADDRESS, DIRECTION])? {
            ACTION => NetworkPolicyRuleOptionName::Action,
            ADDRESS => NetworkPolicyRuleOptionName::Address,
            DIRECTION => NetworkPolicyRuleOptionName::Direction,
            v => panic!("found unreachable keyword {}", v),
        };
        let value = self.parse_optional_option_value()?;
        Ok(NetworkPolicyRuleOption { name, value })
    }

    fn parse_create_table(&mut self) -> Result<Statement<Raw>, ParserError> {
        let temporary = self.parse_keyword(TEMPORARY) | self.parse_keyword(TEMP);
        self.expect_keyword(TABLE)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let table_name = self.parse_item_name()?;
        // parse optional column list (schema)
        let (columns, constraints) = self.parse_columns(Mandatory)?;

        let with_options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            let o = if matches!(self.peek_token(), Some(Token::RParen)) {
                vec![]
            } else {
                self.parse_comma_separated(Parser::parse_table_option)?
            };
            self.expect_token(&Token::RParen)?;
            o
        } else {
            vec![]
        };

        Ok(Statement::CreateTable(CreateTableStatement {
            name: table_name,
            columns,
            constraints,
            if_not_exists,
            temporary,
            with_options,
        }))
    }

    fn parse_create_table_from_source(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(TABLE)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let table_name = self.parse_item_name()?;

        if self.parse_keywords(&[FROM, WEBHOOK]) {
            // Webhook Source, which works differently than all other sources.
            return self.parse_create_webhook_source(table_name, if_not_exists, None, true);
        }

        let (columns, constraints) = self.parse_table_from_source_columns()?;

        self.expect_keywords(&[FROM, SOURCE])?;

        let source = self.parse_raw_name()?;

        let external_reference = if self.consume_token(&Token::LParen) {
            self.expect_keyword(REFERENCE)?;
            let _ = self.consume_token(&Token::Eq);
            let external_reference = self.parse_item_name()?;
            self.expect_token(&Token::RParen)?;
            Some(external_reference)
        } else {
            None
        };

        let format = match self.parse_one_of_keywords(&[KEY, FORMAT]) {
            Some(KEY) => {
                self.expect_keyword(FORMAT)?;
                let key = self.parse_format()?;
                self.expect_keywords(&[VALUE, FORMAT])?;
                let value = self.parse_format()?;
                Some(FormatSpecifier::KeyValue { key, value })
            }
            Some(FORMAT) => Some(FormatSpecifier::Bare(self.parse_format()?)),
            Some(_) => unreachable!("parse_one_of_keywords returns None for this"),
            None => None,
        };
        let include_metadata = self.parse_source_include_metadata()?;

        let envelope = if self.parse_keyword(ENVELOPE) {
            Some(self.parse_source_envelope()?)
        } else {
            None
        };

        let with_options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_table_from_source_option)?;
            self.expect_token(&Token::RParen)?;
            options
        } else {
            vec![]
        };

        Ok(Statement::CreateTableFromSource(
            CreateTableFromSourceStatement {
                name: table_name,
                columns,
                constraints,
                if_not_exists,
                source,
                external_reference,
                format,
                include_metadata,
                envelope,
                with_options,
            },
        ))
    }

    fn parse_table_from_source_option(
        &mut self,
    ) -> Result<TableFromSourceOption<Raw>, ParserError> {
        let option = match self
            .expect_one_of_keywords(&[TEXT, EXCLUDE, IGNORE, DETAILS, PARTITION, RETAIN])?
        {
            ref keyword @ (TEXT | EXCLUDE) => {
                self.expect_keyword(COLUMNS)?;

                let _ = self.consume_token(&Token::Eq);

                let value = self
                    .parse_option_sequence(Parser::parse_identifier)?
                    .map(|inner| {
                        WithOptionValue::Sequence(
                            inner.into_iter().map(WithOptionValue::Ident).collect_vec(),
                        )
                    });

                TableFromSourceOption {
                    name: match *keyword {
                        TEXT => TableFromSourceOptionName::TextColumns,
                        EXCLUDE => TableFromSourceOptionName::ExcludeColumns,
                        _ => unreachable!(),
                    },
                    value,
                }
            }
            DETAILS => TableFromSourceOption {
                name: TableFromSourceOptionName::Details,
                value: self.parse_optional_option_value()?,
            },
            IGNORE => {
                self.expect_keyword(COLUMNS)?;
                let _ = self.consume_token(&Token::Eq);

                let value = self
                    .parse_option_sequence(Parser::parse_identifier)?
                    .map(|inner| {
                        WithOptionValue::Sequence(
                            inner.into_iter().map(WithOptionValue::Ident).collect_vec(),
                        )
                    });
                TableFromSourceOption {
                    // IGNORE is historical syntax for this option.
                    name: TableFromSourceOptionName::ExcludeColumns,
                    value,
                }
            }
            PARTITION => {
                self.expect_keyword(BY)?;
                TableFromSourceOption {
                    name: TableFromSourceOptionName::PartitionBy,
                    value: self.parse_optional_option_value()?,
                }
            }
            RETAIN => {
                self.expect_keyword(HISTORY)?;
                TableFromSourceOption {
                    name: TableFromSourceOptionName::RetainHistory,
                    value: self.parse_option_retain_history()?,
                }
            }
            _ => unreachable!(),
        };
        Ok(option)
    }

    fn parse_table_from_source_columns(
        &mut self,
    ) -> Result<(TableFromSourceColumns<Raw>, Vec<TableConstraint<Raw>>), ParserError> {
        let mut constraints = vec![];

        if !self.consume_token(&Token::LParen) {
            return Ok((TableFromSourceColumns::NotSpecified, constraints));
        }
        if self.consume_token(&Token::RParen) {
            // Tables with zero columns are a PostgreSQL extension.
            return Ok((TableFromSourceColumns::NotSpecified, constraints));
        }

        let mut column_names = vec![];
        let mut column_defs = vec![];
        loop {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Some(column_name) = self.consume_identifier()? {
                let next_token = self.peek_token();
                match next_token {
                    Some(Token::Comma) | Some(Token::RParen) => {
                        column_names.push(column_name);
                    }
                    _ => {
                        let data_type = self.parse_data_type()?;
                        let collation = if self.parse_keyword(COLLATE) {
                            Some(self.parse_item_name()?)
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

                        column_defs.push(ColumnDef {
                            name: column_name,
                            data_type,
                            collation,
                            options,
                        });
                    }
                }
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
        if !column_defs.is_empty() && !column_names.is_empty() {
            return parser_err!(
                self,
                self.peek_prev_pos(),
                "cannot mix column definitions and column names"
            );
        }

        let columns = match column_defs.is_empty() {
            true => match column_names.is_empty() {
                true => TableFromSourceColumns::NotSpecified,
                false => TableFromSourceColumns::Named(column_names),
            },
            false => TableFromSourceColumns::Defined(column_defs),
        };

        Ok((columns, constraints))
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
            } else if let Some(column_name) = self.consume_identifier()? {
                let data_type = self.parse_data_type()?;
                let collation = if self.parse_keyword(COLLATE) {
                    Some(self.parse_item_name()?)
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
            let foreign_table = self.parse_item_name()?;
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
        } else if self.parse_keyword(VERSION) {
            self.expect_keyword(ADDED)?;
            let action = ColumnVersioned::Added;
            let version = self.parse_version()?;

            ColumnOption::Versioned { action, version }
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
            Some(Token::Keyword(PRIMARY)) => {
                self.expect_keyword(KEY)?;
                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    columns,
                    is_primary: true,
                    nulls_not_distinct: false,
                }))
            }
            Some(Token::Keyword(UNIQUE)) => {
                let nulls_not_distinct = if self.parse_keyword(NULLS) {
                    self.expect_keywords(&[NOT, DISTINCT])?;
                    true
                } else {
                    false
                };

                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    columns,
                    is_primary: false,
                    nulls_not_distinct,
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

    fn parse_object_option_value(&mut self) -> Result<WithOptionValue<Raw>, ParserError> {
        let _ = self.consume_token(&Token::Eq);
        Ok(WithOptionValue::Item(self.parse_raw_name()?))
    }

    fn parse_optional_option_value(&mut self) -> Result<Option<WithOptionValue<Raw>>, ParserError> {
        // The next token might be a value and might not. The only valid things
        // that indicate no value would be `)` for end-of-options , `,` for
        // another-option, or ';'/nothing for end-of-statement. Either of those
        // means there's no value, anything else means we expect a valid value.
        match self.peek_token() {
            Some(Token::RParen) | Some(Token::Comma) | Some(Token::Semicolon) | None => Ok(None),
            _ => {
                let _ = self.consume_token(&Token::Eq);
                Ok(Some(self.parse_option_value()?))
            }
        }
    }

    fn parse_option_sequence<T, F>(&mut self, f: F) -> Result<Option<Vec<T>>, ParserError>
    where
        F: FnMut(&mut Self) -> Result<T, ParserError>,
    {
        Ok(if self.consume_token(&Token::LParen) {
            let options = if self.consume_token(&Token::RParen) {
                vec![]
            } else {
                let options = self.parse_comma_separated(f)?;
                self.expect_token(&Token::RParen)?;
                options
            };

            Some(options)
        } else if self.consume_token(&Token::LBracket) {
            let options = if self.consume_token(&Token::RBracket) {
                vec![]
            } else {
                let options = self.parse_comma_separated(f)?;

                self.expect_token(&Token::RBracket)?;
                options
            };

            Some(options)
        } else {
            None
        })
    }

    fn parse_option_map(
        &mut self,
    ) -> Result<Option<BTreeMap<String, WithOptionValue<Raw>>>, ParserError> {
        Ok(if self.parse_keyword(MAP) {
            self.expect_token(&Token::LBracket)?;
            let mut map = BTreeMap::new();
            loop {
                if let Some(Token::RBracket) = self.peek_token() {
                    break;
                }
                let key = match self.next_token() {
                    Some(Token::String(s)) => s,
                    token => return self.expected(self.peek_pos(), "string", token),
                };
                self.expect_token(&Token::Arrow)?;
                let value = Parser::parse_option_value(self)?;
                map.insert(key, value);
                if !self.consume_token(&Token::Comma) {
                    break;
                }
            }
            self.expect_token(&Token::RBracket)?;
            Some(map)
        } else {
            None
        })
    }

    fn parse_option_value(&mut self) -> Result<WithOptionValue<Raw>, ParserError> {
        if let Some(seq) = self.parse_option_sequence(Parser::parse_option_value)? {
            Ok(WithOptionValue::Sequence(seq))
        } else if let Some(map) = self.parse_option_map()? {
            Ok(WithOptionValue::Map(map))
        } else if self.parse_keyword(SECRET) {
            if let Some(secret) = self.maybe_parse(Parser::parse_raw_name) {
                Ok(WithOptionValue::Secret(secret))
            } else {
                Ok(WithOptionValue::UnresolvedItemName(UnresolvedItemName(
                    vec![ident!("secret")],
                )))
            }
        } else if let Some(value) = self.maybe_parse(Parser::parse_value) {
            Ok(WithOptionValue::Value(value))
        } else if let Some(item_name) = self.maybe_parse(Parser::parse_item_name) {
            Ok(WithOptionValue::UnresolvedItemName(item_name))
        } else {
            self.expected(self.peek_pos(), "option value", self.peek_token())
        }
    }

    fn parse_data_type_option_value(&mut self) -> Result<WithOptionValue<Raw>, ParserError> {
        let _ = self.consume_token(&Token::Eq);
        Ok(WithOptionValue::DataType(self.parse_data_type()?))
    }

    fn parse_alter(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        if self.parse_keyword(SYSTEM) {
            self.parse_alter_system()
        } else if self.parse_keywords(&[DEFAULT, PRIVILEGES]) {
            self.parse_alter_default_privileges()
                .map_parser_err(StatementKind::AlterDefaultPrivileges)
        } else {
            self.parse_alter_object()
        }
    }

    fn parse_alter_object(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        let object_type = self.expect_object_type().map_no_statement_parser_err()?;

        match object_type {
            ObjectType::Role => self
                .parse_alter_role()
                .map_parser_err(StatementKind::AlterRole),
            ObjectType::Sink => self.parse_alter_sink(),
            ObjectType::Source => self.parse_alter_source(),
            ObjectType::Index => self.parse_alter_index(),
            ObjectType::Secret => self.parse_alter_secret(),
            ObjectType::Connection => self.parse_alter_connection(),
            ObjectType::View
            | ObjectType::MaterializedView
            | ObjectType::Table
            | ObjectType::ContinualTask => self.parse_alter_views(object_type),
            ObjectType::Type => {
                let if_exists = self
                    .parse_if_exists()
                    .map_parser_err(StatementKind::AlterOwner)?;
                let name = UnresolvedObjectName::Item(
                    self.parse_item_name()
                        .map_parser_err(StatementKind::AlterOwner)?,
                );
                self.expect_keywords(&[OWNER, TO])
                    .map_parser_err(StatementKind::AlterOwner)?;
                let new_owner = self
                    .parse_identifier()
                    .map_parser_err(StatementKind::AlterOwner)?;
                Ok(Statement::AlterOwner(AlterOwnerStatement {
                    object_type,
                    if_exists,
                    name,
                    new_owner,
                }))
            }
            ObjectType::Cluster => self.parse_alter_cluster(object_type),
            ObjectType::ClusterReplica => {
                let if_exists = self.parse_if_exists().map_no_statement_parser_err()?;
                let name = UnresolvedObjectName::ClusterReplica(
                    self.parse_cluster_replica_name()
                        .map_no_statement_parser_err()?,
                );
                let action = self
                    .expect_one_of_keywords(&[OWNER, RENAME])
                    .map_no_statement_parser_err()?;
                self.expect_keyword(TO).map_no_statement_parser_err()?;
                match action {
                    OWNER => {
                        let new_owner = self
                            .parse_identifier()
                            .map_parser_err(StatementKind::AlterOwner)?;
                        Ok(Statement::AlterOwner(AlterOwnerStatement {
                            object_type,
                            if_exists,
                            name,
                            new_owner,
                        }))
                    }
                    RENAME => {
                        let to_item_name = self
                            .parse_identifier()
                            .map_parser_err(StatementKind::AlterObjectRename)?;
                        Ok(Statement::AlterObjectRename(AlterObjectRenameStatement {
                            object_type,
                            if_exists,
                            name,
                            to_item_name,
                        }))
                    }
                    _ => unreachable!(),
                }
            }
            ObjectType::Database => {
                let if_exists = self
                    .parse_if_exists()
                    .map_parser_err(StatementKind::AlterOwner)?;
                let name = UnresolvedObjectName::Database(
                    self.parse_database_name()
                        .map_parser_err(StatementKind::AlterOwner)?,
                );
                self.expect_keywords(&[OWNER, TO])
                    .map_parser_err(StatementKind::AlterOwner)?;
                let new_owner = self
                    .parse_identifier()
                    .map_parser_err(StatementKind::AlterOwner)?;
                Ok(Statement::AlterOwner(AlterOwnerStatement {
                    object_type,
                    if_exists,
                    name,
                    new_owner,
                }))
            }
            ObjectType::Schema => self.parse_alter_schema(object_type),
            ObjectType::NetworkPolicy => self
                .parse_alter_network_policy()
                .map_parser_err(StatementKind::AlterNetworkPolicy),
            ObjectType::Func | ObjectType::Subsource => parser_err!(
                self,
                self.peek_prev_pos(),
                format!("Unsupported ALTER on {object_type}")
            )
            .map_no_statement_parser_err(),
        }
    }

    fn parse_alter_cluster(
        &mut self,
        object_type: ObjectType,
    ) -> Result<Statement<Raw>, ParserStatementError> {
        let if_exists = self.parse_if_exists().map_no_statement_parser_err()?;
        let name = self.parse_identifier().map_no_statement_parser_err()?;
        let action = self
            .expect_one_of_keywords(&[OWNER, RENAME, RESET, SET, SWAP])
            .map_no_statement_parser_err()?;
        match action {
            OWNER => {
                self.expect_keyword(TO)
                    .map_parser_err(StatementKind::AlterOwner)?;
                let new_owner = self
                    .parse_identifier()
                    .map_parser_err(StatementKind::AlterOwner)?;
                let name = UnresolvedObjectName::Cluster(name);
                Ok(Statement::AlterOwner(AlterOwnerStatement {
                    object_type,
                    if_exists,
                    name,
                    new_owner,
                }))
            }
            RENAME => {
                self.expect_keyword(TO)
                    .map_parser_err(StatementKind::AlterObjectRename)?;
                let to_item_name = self
                    .parse_identifier()
                    .map_parser_err(StatementKind::AlterObjectRename)?;
                let name = UnresolvedObjectName::Cluster(name);
                Ok(Statement::AlterObjectRename(AlterObjectRenameStatement {
                    object_type,
                    if_exists,
                    name,
                    to_item_name,
                }))
            }
            RESET => {
                self.expect_token(&Token::LParen)
                    .map_parser_err(StatementKind::AlterCluster)?;
                let names = self
                    .parse_comma_separated(Parser::parse_cluster_option_name)
                    .map_parser_err(StatementKind::AlterCluster)?;
                self.expect_token(&Token::RParen)
                    .map_parser_err(StatementKind::AlterCluster)?;
                Ok(Statement::AlterCluster(AlterClusterStatement {
                    if_exists,
                    name,
                    action: AlterClusterAction::ResetOptions(names),
                }))
            }
            SET => {
                self.expect_token(&Token::LParen)
                    .map_parser_err(StatementKind::AlterCluster)?;
                let options = self
                    .parse_comma_separated(Parser::parse_cluster_option)
                    .map_parser_err(StatementKind::AlterCluster)?;
                self.expect_token(&Token::RParen)
                    .map_parser_err(StatementKind::AlterCluster)?;
                let with_options = if self.parse_keyword(WITH) {
                    self.expect_token(&Token::LParen)
                        .map_parser_err(StatementKind::AlterCluster)?;
                    let options = self
                        .parse_comma_separated(Parser::parse_alter_cluster_option)
                        .map_parser_err(StatementKind::AlterCluster)?;
                    self.expect_token(&Token::RParen)
                        .map_parser_err(StatementKind::AlterCluster)?;
                    options
                } else {
                    vec![]
                };
                Ok(Statement::AlterCluster(AlterClusterStatement {
                    if_exists,
                    name,
                    action: AlterClusterAction::SetOptions {
                        options,
                        with_options,
                    },
                }))
            }
            SWAP => {
                self.expect_keyword(WITH)
                    .map_parser_err(StatementKind::AlterObjectSwap)?;
                let name_b = self
                    .parse_identifier()
                    .map_parser_err(StatementKind::AlterObjectSwap)?;

                Ok(Statement::AlterObjectSwap(AlterObjectSwapStatement {
                    object_type,
                    name_a: UnresolvedObjectName::Cluster(name),
                    name_b,
                }))
            }
            _ => unreachable!(),
        }
    }

    fn parse_alter_source(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        let if_exists = self.parse_if_exists().map_no_statement_parser_err()?;
        let source_name = self.parse_item_name().map_no_statement_parser_err()?;

        Ok(
            match self
                .expect_one_of_keywords(&[ADD, DROP, RESET, SET, RENAME, OWNER, REFRESH])
                .map_no_statement_parser_err()?
            {
                ADD => {
                    self.expect_one_of_keywords(&[SUBSOURCE, TABLE])
                        .map_parser_err(StatementKind::AlterSource)?;

                    // TODO: Add IF NOT EXISTS?
                    let subsources = self
                        .parse_comma_separated(Parser::parse_subsource_references)
                        .map_parser_err(StatementKind::AlterSource)?;

                    let options = if self.parse_keyword(WITH) {
                        self.expect_token(&Token::LParen)
                            .map_parser_err(StatementKind::AlterSource)?;
                        let options = self
                            .parse_comma_separated(Parser::parse_alter_source_add_subsource_option)
                            .map_parser_err(StatementKind::AlterSource)?;
                        self.expect_token(&Token::RParen)
                            .map_parser_err(StatementKind::AlterSource)?;
                        options
                    } else {
                        vec![]
                    };

                    Statement::AlterSource(AlterSourceStatement {
                        source_name,
                        if_exists,
                        action: AlterSourceAction::AddSubsources {
                            external_references: subsources,
                            options,
                        },
                    })
                }
                DROP => {
                    self.expect_one_of_keywords(&[SUBSOURCE, TABLE])
                        .map_parser_err(StatementKind::AlterSource)?;

                    let if_exists_inner = self
                        .parse_if_exists()
                        .map_parser_err(StatementKind::AlterSource)?;

                    let names = self
                        .parse_comma_separated(Parser::parse_item_name)
                        .map_parser_err(StatementKind::AlterSource)?;

                    let cascade = matches!(
                        self.parse_at_most_one_keyword(&[CASCADE, RESTRICT], "ALTER SOURCE...DROP")
                            .map_parser_err(StatementKind::AlterSource)?,
                        Some(CASCADE),
                    );

                    Statement::AlterSource(AlterSourceStatement {
                        source_name,
                        if_exists,
                        action: AlterSourceAction::DropSubsources {
                            if_exists: if_exists_inner,
                            cascade,
                            names,
                        },
                    })
                }
                RESET => {
                    self.expect_token(&Token::LParen)
                        .map_parser_err(StatementKind::AlterSource)?;
                    let reset_options = self
                        .parse_comma_separated(Parser::parse_source_option_name)
                        .map_parser_err(StatementKind::AlterSource)?;
                    self.expect_token(&Token::RParen)
                        .map_parser_err(StatementKind::AlterSource)?;

                    Statement::AlterSource(AlterSourceStatement {
                        source_name,
                        if_exists,
                        action: AlterSourceAction::ResetOptions(reset_options),
                    })
                }
                SET => {
                    if let Some(stmt) = self.maybe_parse_alter_set_cluster(
                        if_exists,
                        &source_name,
                        ObjectType::Source,
                    ) {
                        return stmt;
                    }
                    self.expect_token(&Token::LParen)
                        .map_parser_err(StatementKind::AlterSource)?;
                    let set_options = self
                        .parse_comma_separated(Parser::parse_source_option)
                        .map_parser_err(StatementKind::AlterSource)?;
                    self.expect_token(&Token::RParen)
                        .map_parser_err(StatementKind::AlterSource)?;
                    Statement::AlterSource(AlterSourceStatement {
                        source_name,
                        if_exists,
                        action: AlterSourceAction::SetOptions(set_options),
                    })
                }
                RENAME => {
                    self.expect_keyword(TO)
                        .map_parser_err(StatementKind::AlterObjectRename)?;
                    let to_item_name = self
                        .parse_identifier()
                        .map_parser_err(StatementKind::AlterObjectRename)?;

                    Statement::AlterObjectRename(AlterObjectRenameStatement {
                        object_type: ObjectType::Source,
                        if_exists,
                        name: UnresolvedObjectName::Item(source_name),
                        to_item_name,
                    })
                }
                OWNER => {
                    self.expect_keyword(TO)
                        .map_parser_err(StatementKind::AlterOwner)?;
                    let new_owner = self
                        .parse_identifier()
                        .map_parser_err(StatementKind::AlterOwner)?;

                    Statement::AlterOwner(AlterOwnerStatement {
                        object_type: ObjectType::Source,
                        if_exists,
                        name: UnresolvedObjectName::Item(source_name),
                        new_owner,
                    })
                }
                REFRESH => {
                    self.expect_keyword(REFERENCES)
                        .map_parser_err(StatementKind::AlterSource)?;
                    Statement::AlterSource(AlterSourceStatement {
                        source_name,
                        if_exists,
                        action: AlterSourceAction::RefreshReferences,
                    })
                }
                _ => unreachable!(),
            },
        )
    }

    fn parse_alter_source_add_subsource_option(
        &mut self,
    ) -> Result<AlterSourceAddSubsourceOption<Raw>, ParserError> {
        match self.expect_one_of_keywords(&[TEXT, EXCLUDE, IGNORE])? {
            ref keyword @ (TEXT | EXCLUDE | IGNORE) => {
                self.expect_keyword(COLUMNS)?;

                let _ = self.consume_token(&Token::Eq);

                let value = self
                    .parse_option_sequence(Parser::parse_item_name)?
                    .map(|inner| {
                        WithOptionValue::Sequence(
                            inner
                                .into_iter()
                                .map(WithOptionValue::UnresolvedItemName)
                                .collect_vec(),
                        )
                    });

                Ok(AlterSourceAddSubsourceOption {
                    name: match *keyword {
                        TEXT => AlterSourceAddSubsourceOptionName::TextColumns,
                        // IGNORE is historical syntax for this option.
                        EXCLUDE | IGNORE => AlterSourceAddSubsourceOptionName::ExcludeColumns,
                        _ => unreachable!(),
                    },
                    value,
                })
            }
            _ => unreachable!(),
        }
    }

    fn parse_alter_index(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        let if_exists = self.parse_if_exists().map_no_statement_parser_err()?;
        let name = self.parse_item_name().map_no_statement_parser_err()?;

        Ok(
            match self
                .expect_one_of_keywords(&[RESET, SET, RENAME, OWNER])
                .map_no_statement_parser_err()?
            {
                RESET => {
                    self.expect_token(&Token::LParen)
                        .map_parser_err(StatementKind::AlterIndex)?;
                    let reset_options = self
                        .parse_comma_separated(Parser::parse_index_option_name)
                        .map_parser_err(StatementKind::AlterIndex)?;
                    self.expect_token(&Token::RParen)
                        .map_parser_err(StatementKind::AlterIndex)?;

                    Statement::AlterIndex(AlterIndexStatement {
                        index_name: name,
                        if_exists,
                        action: AlterIndexAction::ResetOptions(reset_options),
                    })
                }
                SET => {
                    self.expect_token(&Token::LParen)
                        .map_parser_err(StatementKind::AlterIndex)?;
                    let set_options = self
                        .parse_comma_separated(Parser::parse_index_option)
                        .map_parser_err(StatementKind::AlterIndex)?;
                    self.expect_token(&Token::RParen)
                        .map_parser_err(StatementKind::AlterIndex)?;
                    Statement::AlterIndex(AlterIndexStatement {
                        index_name: name,
                        if_exists,
                        action: AlterIndexAction::SetOptions(set_options),
                    })
                }
                RENAME => {
                    self.expect_keyword(TO)
                        .map_parser_err(StatementKind::AlterObjectRename)?;
                    let to_item_name = self
                        .parse_identifier()
                        .map_parser_err(StatementKind::AlterObjectRename)?;

                    Statement::AlterObjectRename(AlterObjectRenameStatement {
                        object_type: ObjectType::Index,
                        if_exists,
                        name: UnresolvedObjectName::Item(name),
                        to_item_name,
                    })
                }
                OWNER => {
                    self.expect_keyword(TO)
                        .map_parser_err(StatementKind::AlterOwner)?;
                    let new_owner = self
                        .parse_identifier()
                        .map_parser_err(StatementKind::AlterOwner)?;

                    Statement::AlterOwner(AlterOwnerStatement {
                        object_type: ObjectType::Index,
                        if_exists,
                        name: UnresolvedObjectName::Item(name),
                        new_owner,
                    })
                }
                _ => unreachable!(),
            },
        )
    }

    fn parse_alter_secret(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        let if_exists = self.parse_if_exists().map_no_statement_parser_err()?;
        let name = self.parse_item_name().map_no_statement_parser_err()?;

        Ok(
            match self
                .expect_one_of_keywords(&[AS, RENAME, OWNER])
                .map_no_statement_parser_err()?
            {
                AS => {
                    let value = self
                        .parse_expr()
                        .map_parser_err(StatementKind::AlterSecret)?;
                    Statement::AlterSecret(AlterSecretStatement {
                        name,
                        if_exists,
                        value,
                    })
                }
                RENAME => {
                    self.expect_keyword(TO)
                        .map_parser_err(StatementKind::AlterObjectRename)?;
                    let to_item_name = self
                        .parse_identifier()
                        .map_parser_err(StatementKind::AlterObjectRename)?;

                    Statement::AlterObjectRename(AlterObjectRenameStatement {
                        object_type: ObjectType::Secret,
                        if_exists,
                        name: UnresolvedObjectName::Item(name),
                        to_item_name,
                    })
                }
                OWNER => {
                    self.expect_keyword(TO)
                        .map_parser_err(StatementKind::AlterOwner)?;
                    let new_owner = self
                        .parse_identifier()
                        .map_parser_err(StatementKind::AlterOwner)?;

                    Statement::AlterOwner(AlterOwnerStatement {
                        object_type: ObjectType::Secret,
                        if_exists,
                        name: UnresolvedObjectName::Item(name),
                        new_owner,
                    })
                }
                _ => unreachable!(),
            },
        )
    }

    /// Parse an ALTER SINK statement.
    fn parse_alter_sink(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        let if_exists = self.parse_if_exists().map_no_statement_parser_err()?;
        let name = self.parse_item_name().map_no_statement_parser_err()?;

        Ok(
            match self
                .expect_one_of_keywords(&[RESET, SET, RENAME, OWNER])
                .map_no_statement_parser_err()?
            {
                RESET => {
                    self.expect_token(&Token::LParen)
                        .map_parser_err(StatementKind::AlterSink)?;
                    let reset_options = self
                        .parse_comma_separated(Parser::parse_create_sink_option_name)
                        .map_parser_err(StatementKind::AlterSink)?;
                    self.expect_token(&Token::RParen)
                        .map_parser_err(StatementKind::AlterSink)?;

                    Statement::AlterSink(AlterSinkStatement {
                        sink_name: name,
                        if_exists,
                        action: AlterSinkAction::ResetOptions(reset_options),
                    })
                }
                SET => {
                    if let Some(result) =
                        self.maybe_parse_alter_set_cluster(if_exists, &name, ObjectType::Sink)
                    {
                        return result;
                    }

                    if self.parse_keyword(FROM) {
                        let from = self
                            .parse_raw_name()
                            .map_parser_err(StatementKind::AlterSink)?;

                        Statement::AlterSink(AlterSinkStatement {
                            sink_name: name,
                            if_exists,
                            action: AlterSinkAction::ChangeRelation(from),
                        })
                    } else {
                        self.expect_token(&Token::LParen)
                            .map_parser_err(StatementKind::AlterSink)?;
                        let set_options = self
                            .parse_comma_separated(Parser::parse_create_sink_option)
                            .map_parser_err(StatementKind::AlterSink)?;
                        self.expect_token(&Token::RParen)
                            .map_parser_err(StatementKind::AlterSink)?;
                        Statement::AlterSink(AlterSinkStatement {
                            sink_name: name,
                            if_exists,
                            action: AlterSinkAction::SetOptions(set_options),
                        })
                    }
                }
                RENAME => {
                    self.expect_keyword(TO)
                        .map_parser_err(StatementKind::AlterObjectRename)?;
                    let to_item_name = self
                        .parse_identifier()
                        .map_parser_err(StatementKind::AlterObjectRename)?;

                    Statement::AlterObjectRename(AlterObjectRenameStatement {
                        object_type: ObjectType::Sink,
                        if_exists,
                        name: UnresolvedObjectName::Item(name),
                        to_item_name,
                    })
                }
                OWNER => {
                    self.expect_keyword(TO)
                        .map_parser_err(StatementKind::AlterOwner)?;
                    let new_owner = self
                        .parse_identifier()
                        .map_parser_err(StatementKind::AlterOwner)?;

                    Statement::AlterOwner(AlterOwnerStatement {
                        object_type: ObjectType::Sink,
                        if_exists,
                        name: UnresolvedObjectName::Item(name),
                        new_owner,
                    })
                }
                _ => unreachable!(),
            },
        )
    }

    /// Parse an ALTER SYSTEM statement.
    fn parse_alter_system(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        match self
            .expect_one_of_keywords(&[SET, RESET])
            .map_no_statement_parser_err()?
        {
            SET => {
                let name = self
                    .parse_identifier()
                    .map_parser_err(StatementKind::AlterSystemSet)?;
                self.expect_keyword_or_token(TO, &Token::Eq)
                    .map_parser_err(StatementKind::AlterSystemSet)?;
                let to = self
                    .parse_set_variable_to()
                    .map_parser_err(StatementKind::AlterSystemSet)?;
                Ok(Statement::AlterSystemSet(AlterSystemSetStatement {
                    name,
                    to,
                }))
            }
            RESET => {
                if self.parse_keyword(ALL) {
                    Ok(Statement::AlterSystemResetAll(
                        AlterSystemResetAllStatement {},
                    ))
                } else {
                    let name = self
                        .parse_identifier()
                        .map_parser_err(StatementKind::AlterSystemReset)?;
                    Ok(Statement::AlterSystemReset(AlterSystemResetStatement {
                        name,
                    }))
                }
            }
            _ => unreachable!(),
        }
    }

    fn parse_alter_connection(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        let if_exists = self.parse_if_exists().map_no_statement_parser_err()?;
        let name = self.parse_item_name().map_no_statement_parser_err()?;

        Ok(
            match self
                .expect_one_of_keywords(&[RENAME, OWNER, ROTATE, SET, RESET, DROP])
                .map_no_statement_parser_err()?
            {
                RENAME => {
                    self.expect_keyword(TO)
                        .map_parser_err(StatementKind::AlterObjectRename)?;
                    let to_item_name = self
                        .parse_identifier()
                        .map_parser_err(StatementKind::AlterObjectRename)?;

                    Statement::AlterObjectRename(AlterObjectRenameStatement {
                        object_type: ObjectType::Connection,
                        if_exists,
                        name: UnresolvedObjectName::Item(name),
                        to_item_name,
                    })
                }
                OWNER => {
                    self.expect_keyword(TO)
                        .map_parser_err(StatementKind::AlterOwner)?;
                    let new_owner = self
                        .parse_identifier()
                        .map_parser_err(StatementKind::AlterOwner)?;

                    Statement::AlterOwner(AlterOwnerStatement {
                        object_type: ObjectType::Connection,
                        if_exists,
                        name: UnresolvedObjectName::Item(name),
                        new_owner,
                    })
                }
                _ => {
                    self.prev_token();
                    let actions = self
                        .parse_comma_separated(Parser::parse_alter_connection_action)
                        .map_parser_err(StatementKind::AlterConnection)?;

                    let with_options = if self.parse_keyword(WITH) {
                        self.expect_token(&Token::LParen)
                            .map_parser_err(StatementKind::AlterConnection)?;
                        let options = self
                            .parse_comma_separated(Parser::parse_alter_connection_option)
                            .map_parser_err(StatementKind::AlterConnection)?;
                        self.expect_token(&Token::RParen)
                            .map_parser_err(StatementKind::AlterConnection)?;
                        options
                    } else {
                        vec![]
                    };

                    Statement::AlterConnection(AlterConnectionStatement {
                        name,
                        if_exists,
                        actions,
                        with_options,
                    })
                }
            },
        )
    }

    fn parse_alter_connection_action(&mut self) -> Result<AlterConnectionAction<Raw>, ParserError> {
        let r = match self.expect_one_of_keywords(&[ROTATE, SET, RESET, DROP])? {
            ROTATE => {
                self.expect_keyword(KEYS)?;
                AlterConnectionAction::RotateKeys
            }
            SET => {
                self.expect_token(&Token::LParen)?;
                let option = self.parse_connection_option_unified()?;
                self.expect_token(&Token::RParen)?;
                AlterConnectionAction::SetOption(option)
            }
            DROP | RESET => {
                self.expect_token(&Token::LParen)?;
                let option = self.parse_connection_option_name()?;
                self.expect_token(&Token::RParen)?;
                AlterConnectionAction::DropOption(option)
            }
            _ => unreachable!(),
        };

        Ok(r)
    }

    /// Parses a single valid option in the WITH block of a create source
    fn parse_alter_connection_option(&mut self) -> Result<AlterConnectionOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[VALIDATE])? {
            VALIDATE => AlterConnectionOptionName::Validate,
            _ => unreachable!(),
        };

        Ok(AlterConnectionOption {
            name,
            value: self.parse_optional_option_value()?,
        })
    }

    fn parse_alter_role(&mut self) -> Result<Statement<Raw>, ParserError> {
        let name = self.parse_identifier()?;

        let option = match self.parse_one_of_keywords(&[SET, RESET, WITH]) {
            Some(SET) => {
                let name = self.parse_identifier()?;
                self.expect_keyword_or_token(TO, &Token::Eq)?;
                let value = self.parse_set_variable_to()?;
                let var = SetRoleVar::Set { name, value };
                AlterRoleOption::Variable(var)
            }
            Some(RESET) => {
                let name = self.parse_identifier()?;
                let var = SetRoleVar::Reset { name };
                AlterRoleOption::Variable(var)
            }
            Some(WITH) | None => {
                let _ = self.parse_keyword(WITH);
                let attrs = self.parse_role_attributes()?;
                AlterRoleOption::Attributes(attrs)
            }
            Some(k) => unreachable!("unmatched keyword: {k}"),
        };

        Ok(Statement::AlterRole(AlterRoleStatement { name, option }))
    }

    fn parse_alter_default_privileges(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(FOR)?;
        let target_roles = match self.expect_one_of_keywords(&[ROLE, USER, ALL])? {
            ROLE | USER => TargetRoleSpecification::Roles(
                self.parse_comma_separated(Parser::parse_identifier)?,
            ),
            ALL => {
                self.expect_keyword(ROLES)?;
                TargetRoleSpecification::AllRoles
            }
            _ => unreachable!(),
        };
        let target_objects = if self.parse_keyword(IN) {
            match self.expect_one_of_keywords(&[SCHEMA, DATABASE])? {
                SCHEMA => GrantTargetAllSpecification::AllSchemas {
                    schemas: self.parse_comma_separated(Parser::parse_schema_name)?,
                },
                DATABASE => GrantTargetAllSpecification::AllDatabases {
                    databases: self.parse_comma_separated(Parser::parse_database_name)?,
                },
                _ => unreachable!(),
            }
        } else {
            GrantTargetAllSpecification::All
        };
        let is_grant = self.expect_one_of_keywords(&[GRANT, REVOKE])? == GRANT;
        let privileges = self.parse_privilege_specification().ok_or_else(|| {
            self.expected::<_, PrivilegeSpecification>(
                self.peek_pos(),
                "ALL or INSERT or SELECT or UPDATE or DELETE or USAGE or CREATE",
                self.peek_token(),
            )
            .expect_err("only returns errors")
        })?;
        self.expect_keyword(ON)?;
        let object_type =
            self.expect_grant_revoke_plural_object_type(if is_grant { "GRANT" } else { "REVOKE" })?;
        if is_grant {
            self.expect_keyword(TO)?;
        } else {
            self.expect_keyword(FROM)?;
        }
        let grantees = self.parse_comma_separated(Parser::expect_role_specification)?;

        let grant_or_revoke = if is_grant {
            AbbreviatedGrantOrRevokeStatement::Grant(AbbreviatedGrantStatement {
                privileges,
                object_type,
                grantees,
            })
        } else {
            AbbreviatedGrantOrRevokeStatement::Revoke(AbbreviatedRevokeStatement {
                privileges,
                object_type,
                revokees: grantees,
            })
        };

        Ok(Statement::AlterDefaultPrivileges(
            AlterDefaultPrivilegesStatement {
                target_roles,
                target_objects,
                grant_or_revoke,
            },
        ))
    }

    fn parse_alter_views(
        &mut self,
        object_type: ObjectType,
    ) -> Result<Statement<Raw>, ParserStatementError> {
        let if_exists = self.parse_if_exists().map_no_statement_parser_err()?;
        let name = self.parse_item_name().map_no_statement_parser_err()?;
        let keywords: &[_] = match object_type {
            ObjectType::Table => &[SET, RENAME, OWNER, RESET, ADD],
            ObjectType::MaterializedView => &[SET, RENAME, OWNER, RESET, APPLY],
            _ => &[SET, RENAME, OWNER, RESET],
        };

        let action = self
            .expect_one_of_keywords(keywords)
            .map_no_statement_parser_err()?;
        match action {
            RENAME => {
                self.expect_keyword(TO).map_no_statement_parser_err()?;
                let to_item_name = self
                    .parse_identifier()
                    .map_parser_err(StatementKind::AlterObjectRename)?;
                Ok(Statement::AlterObjectRename(AlterObjectRenameStatement {
                    object_type,
                    if_exists,
                    name: UnresolvedObjectName::Item(name),
                    to_item_name,
                }))
            }
            SET => {
                if self.parse_keyword(CLUSTER) {
                    self.parse_alter_set_cluster(if_exists, name, object_type)
                } else {
                    self.expect_token(&Token::LParen)
                        .map_no_statement_parser_err()?;
                    self.expect_keywords(&[RETAIN, HISTORY])
                        .map_parser_err(StatementKind::AlterRetainHistory)?;
                    let history = self
                        .parse_retain_history()
                        .map_parser_err(StatementKind::AlterRetainHistory)?;
                    self.expect_token(&Token::RParen)
                        .map_parser_err(StatementKind::AlterCluster)?;
                    Ok(Statement::AlterRetainHistory(AlterRetainHistoryStatement {
                        object_type,
                        if_exists,
                        name: UnresolvedObjectName::Item(name),
                        history: Some(history),
                    }))
                }
            }
            RESET => {
                self.expect_token(&Token::LParen)
                    .map_no_statement_parser_err()?;
                self.expect_keywords(&[RETAIN, HISTORY])
                    .map_parser_err(StatementKind::AlterRetainHistory)?;
                self.expect_token(&Token::RParen)
                    .map_no_statement_parser_err()?;
                Ok(Statement::AlterRetainHistory(AlterRetainHistoryStatement {
                    object_type,
                    if_exists,
                    name: UnresolvedObjectName::Item(name),
                    history: None,
                }))
            }
            OWNER => {
                self.expect_keyword(TO).map_no_statement_parser_err()?;
                let new_owner = self
                    .parse_identifier()
                    .map_parser_err(StatementKind::AlterOwner)?;
                Ok(Statement::AlterOwner(AlterOwnerStatement {
                    object_type,
                    if_exists,
                    name: UnresolvedObjectName::Item(name),
                    new_owner,
                }))
            }
            ADD => {
                assert_eq!(object_type, ObjectType::Table, "checked object_type above");

                self.expect_keyword(COLUMN)
                    .map_parser_err(StatementKind::AlterTableAddColumn)?;
                let if_col_not_exist = self
                    .parse_if_not_exists()
                    .map_parser_err(StatementKind::AlterTableAddColumn)?;
                let column_name = self
                    .parse_identifier()
                    .map_parser_err(StatementKind::AlterTableAddColumn)?;
                let data_type = self
                    .parse_data_type()
                    .map_parser_err(StatementKind::AlterTableAddColumn)?;

                Ok(Statement::AlterTableAddColumn(
                    AlterTableAddColumnStatement {
                        if_exists,
                        name,
                        if_col_not_exist,
                        column_name,
                        data_type,
                    },
                ))
            }
            APPLY => {
                assert_eq!(
                    object_type,
                    ObjectType::MaterializedView,
                    "checked object_type above",
                );

                self.expect_keyword(REPLACEMENT)
                    .map_parser_err(StatementKind::AlterMaterializedViewApplyReplacement)?;

                let replacement_name = self
                    .parse_item_name()
                    .map_parser_err(StatementKind::AlterMaterializedViewApplyReplacement)?;

                Ok(Statement::AlterMaterializedViewApplyReplacement(
                    AlterMaterializedViewApplyReplacementStatement {
                        if_exists,
                        name,
                        replacement_name,
                    },
                ))
            }
            _ => unreachable!(),
        }
    }

    fn parse_alter_schema(
        &mut self,
        object_type: ObjectType,
    ) -> Result<Statement<Raw>, ParserStatementError> {
        let if_exists = self.parse_if_exists().map_no_statement_parser_err()?;
        let name = self.parse_schema_name().map_no_statement_parser_err()?;
        let name = UnresolvedObjectName::Schema(name);
        let action = self
            .expect_one_of_keywords(&[OWNER, RENAME, SWAP])
            .map_no_statement_parser_err()?;

        match action {
            OWNER => {
                self.expect_keyword(TO)
                    .map_parser_err(StatementKind::AlterOwner)?;
                let new_owner = self
                    .parse_identifier()
                    .map_parser_err(StatementKind::AlterOwner)?;

                Ok(Statement::AlterOwner(AlterOwnerStatement {
                    object_type,
                    if_exists,
                    name,
                    new_owner,
                }))
            }
            RENAME => {
                self.expect_keyword(TO)
                    .map_parser_err(StatementKind::AlterObjectRename)?;
                let to_item_name = self
                    .parse_identifier()
                    .map_parser_err(StatementKind::AlterObjectRename)?;

                Ok(Statement::AlterObjectRename(AlterObjectRenameStatement {
                    object_type,
                    if_exists,
                    name,
                    to_item_name,
                }))
            }
            SWAP => {
                self.expect_keyword(WITH)
                    .map_parser_err(StatementKind::AlterObjectSwap)?;
                let name_b = self
                    .parse_identifier()
                    .map_parser_err(StatementKind::AlterObjectSwap)?;

                Ok(Statement::AlterObjectSwap(AlterObjectSwapStatement {
                    object_type,
                    name_a: name,
                    name_b,
                }))
            }
            k => unreachable!("programming error, unmatched {k}"),
        }
    }

    /// Parses `CLUSTER name` fragments into a [`AlterSetClusterStatement`] if `CLUSTER` is found.
    fn maybe_parse_alter_set_cluster(
        &mut self,
        if_exists: bool,
        name: &UnresolvedItemName,
        object_type: ObjectType,
    ) -> Option<Result<Statement<Raw>, ParserStatementError>> {
        if self.parse_keyword(CLUSTER) {
            Some(self.parse_alter_set_cluster(if_exists, name.clone(), object_type))
        } else {
            None
        }
    }

    /// Parses `IN CLUSTER name` fragments into a [`AlterSetClusterStatement`].
    fn parse_alter_set_cluster(
        &mut self,
        if_exists: bool,
        name: UnresolvedItemName,
        object_type: ObjectType,
    ) -> Result<Statement<Raw>, ParserStatementError> {
        let set_cluster = self
            .parse_raw_ident()
            .map_parser_err(StatementKind::AlterSetCluster)?;
        Ok(Statement::AlterSetCluster(AlterSetClusterStatement {
            name,
            if_exists,
            set_cluster,
            object_type,
        }))
    }

    /// Parse a copy statement
    fn parse_copy(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        // We support an optional "INTO" keyword for COPY INTO <table> FROM
        let maybe_into_pos = if self.parse_keyword(Keyword::Into) {
            Some(self.peek_prev_pos())
        } else {
            None
        };

        let relation = if self.consume_token(&Token::LParen) {
            let query = self.parse_statement()?.ast;
            self.expect_token(&Token::RParen)
                .map_parser_err(StatementKind::Copy)?;
            match query {
                Statement::Select(stmt) => CopyRelation::Select(stmt),
                Statement::Subscribe(stmt) => CopyRelation::Subscribe(stmt),
                _ => {
                    return parser_err!(self, self.peek_prev_pos(), "unsupported query in COPY")
                        .map_parser_err(StatementKind::Copy);
                }
            }
        } else {
            let name = self.parse_raw_name().map_parser_err(StatementKind::Copy)?;
            let columns = self
                .parse_parenthesized_column_list(Optional)
                .map_parser_err(StatementKind::Copy)?;
            CopyRelation::Named { name, columns }
        };
        let (direction, target) = match self
            .expect_one_of_keywords(&[FROM, TO])
            .map_parser_err(StatementKind::Copy)?
        {
            FROM => {
                if let CopyRelation::Named { .. } = relation {
                    // Ok.
                } else {
                    return parser_err!(
                        self,
                        self.peek_prev_pos(),
                        "queries not allowed in COPY FROM"
                    )
                    .map_no_statement_parser_err();
                }
                if self.parse_keyword(STDIN) {
                    (CopyDirection::From, CopyTarget::Stdin)
                } else {
                    let url_expr = self.parse_expr().map_parser_err(StatementKind::Copy)?;
                    (CopyDirection::From, CopyTarget::Expr(url_expr))
                }
            }
            TO => {
                // We only support the INTO keyword for 'COPY FROM'.
                if let Some(into_pos) = maybe_into_pos {
                    return self
                        .expected(into_pos, "identifier", Some(Token::Keyword(Keyword::Into)))
                        .map_parser_err(StatementKind::Copy);
                }

                if self.parse_keyword(STDOUT) {
                    (CopyDirection::To, CopyTarget::Stdout)
                } else {
                    let url_expr = self.parse_expr().map_parser_err(StatementKind::Copy)?;
                    (CopyDirection::To, CopyTarget::Expr(url_expr))
                }
            }
            _ => unreachable!(),
        };
        // WITH must be followed by LParen. The WITH in COPY is optional for backward
        // compat with Postgres but is required elsewhere, which is why we don't use
        // parse_with_options here.
        let has_options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)
                .map_parser_err(StatementKind::Copy)?;
            true
        } else {
            self.consume_token(&Token::LParen)
        };
        let options = if has_options {
            let o = self
                .parse_comma_separated(Parser::parse_copy_option)
                .map_parser_err(StatementKind::Copy)?;
            self.expect_token(&Token::RParen)
                .map_parser_err(StatementKind::Copy)?;
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

    fn parse_copy_option(&mut self) -> Result<CopyOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[
            FORMAT, DELIMITER, NULL, ESCAPE, QUOTE, HEADER, AWS, MAX, FILES, PATTERN,
        ])? {
            FORMAT => CopyOptionName::Format,
            DELIMITER => CopyOptionName::Delimiter,
            NULL => CopyOptionName::Null,
            ESCAPE => CopyOptionName::Escape,
            QUOTE => CopyOptionName::Quote,
            HEADER => CopyOptionName::Header,
            AWS => {
                self.expect_keyword(CONNECTION)?;
                return Ok(CopyOption {
                    name: CopyOptionName::AwsConnection,
                    value: Some(self.parse_object_option_value()?),
                });
            }
            MAX => {
                self.expect_keywords(&[FILE, SIZE])?;
                CopyOptionName::MaxFileSize
            }
            FILES => CopyOptionName::Files,
            PATTERN => CopyOptionName::Pattern,
            _ => unreachable!(),
        };
        Ok(CopyOption {
            name,
            value: self.parse_optional_option_value()?,
        })
    }

    /// Parse a literal value (numbers, strings, date/time, booleans)
    fn parse_value(&mut self) -> Result<Value, ParserError> {
        match self.next_token() {
            Some(t) => match t {
                Token::Keyword(TRUE) => Ok(Value::Boolean(true)),
                Token::Keyword(FALSE) => Ok(Value::Boolean(false)),
                Token::Keyword(NULL) => Ok(Value::Null),
                Token::Keyword(INTERVAL) => Ok(Value::Interval(self.parse_interval_value()?)),
                Token::Keyword(kw) => {
                    parser_err!(
                        self,
                        self.peek_prev_pos(),
                        format!("No value parser for keyword {}", kw)
                    )
                }
                Token::Op(ref op) if op == "-" => match self.next_token() {
                    Some(Token::Number(n)) => Ok(Value::Number(format!("-{}", n))),
                    other => self.expected(self.peek_prev_pos(), "literal int", other),
                },
                Token::Number(ref n) => Ok(Value::Number(n.to_string())),
                Token::String(ref s) => Ok(Value::String(s.to_string())),
                Token::HexString(ref s) => Ok(Value::HexString(s.to_string())),
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

    fn parse_map(&mut self) -> Result<Expr<Raw>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let subquery = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            return Ok(Expr::MapSubquery(Box::new(subquery)));
        }

        self.expect_token(&Token::LBracket)?;
        let mut exprs = vec![];
        loop {
            if let Some(Token::RBracket) = self.peek_token() {
                break;
            }
            let key = self.parse_expr()?;
            self.expect_token(&Token::Arrow)?;
            let value = if let Some(Token::LBracket) = self.peek_token() {
                self.parse_map()?
            } else {
                self.parse_expr()?
            };
            exprs.push(MapEntry { key, value });
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        self.expect_token(&Token::RBracket)?;
        Ok(Expr::Map(exprs))
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

    fn parse_version(&mut self) -> Result<Version, ParserError> {
        let version = self.parse_literal_uint()?;
        Ok(Version(version))
    }

    /// Parse a signed literal integer.
    fn parse_literal_int(&mut self) -> Result<i64, ParserError> {
        let negative = self.consume_token(&Token::Op("-".into()));
        match self.next_token() {
            Some(Token::Number(s)) => {
                let n = s.parse::<i64>().map_err(|e| {
                    self.error(
                        self.peek_prev_pos(),
                        format!("Could not parse '{}' as i64: {}", s, e),
                    )
                })?;
                if negative {
                    n.checked_neg().ok_or_else(|| {
                        self.error(
                            self.peek_prev_pos(),
                            format!("Could not parse '{}' as i64: overflows i64", s),
                        )
                    })
                } else {
                    Ok(n)
                }
            }
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
        let other = |ident| RawDataType::Other {
            name: RawItemName::Name(UnresolvedItemName::unqualified(ident)),
            typ_mod: vec![],
        };

        let mut data_type = match self.next_token() {
            Some(Token::Keyword(kw)) => match kw {
                // Text-like types
                CHAR | CHARACTER => {
                    let name = if self.parse_keyword(VARYING) {
                        ident!("varchar")
                    } else {
                        ident!("bpchar")
                    };
                    RawDataType::Other {
                        name: RawItemName::Name(UnresolvedItemName::unqualified(name)),
                        typ_mod: self.parse_typ_mod()?,
                    }
                }
                BPCHAR => RawDataType::Other {
                    name: RawItemName::Name(UnresolvedItemName::unqualified(ident!("bpchar"))),
                    typ_mod: self.parse_typ_mod()?,
                },
                VARCHAR => RawDataType::Other {
                    name: RawItemName::Name(UnresolvedItemName::unqualified(ident!("varchar"))),
                    typ_mod: self.parse_typ_mod()?,
                },
                STRING => other(ident!("text")),

                // Number-like types
                BIGINT => other(ident!("int8")),
                SMALLINT => other(ident!("int2")),
                DEC | DECIMAL => RawDataType::Other {
                    name: RawItemName::Name(UnresolvedItemName::unqualified(ident!("numeric"))),
                    typ_mod: self.parse_typ_mod()?,
                },
                DOUBLE => {
                    let _ = self.parse_keyword(PRECISION);
                    other(ident!("float8"))
                }
                FLOAT => match self.parse_optional_precision()?.unwrap_or(53) {
                    v if v == 0 || v > 53 => {
                        return Err(self.error(
                            self.peek_prev_pos(),
                            "precision for type float must be within ([1-53])".into(),
                        ));
                    }
                    v if v < 25 => other(ident!("float4")),
                    _ => other(ident!("float8")),
                },
                INT | INTEGER => other(ident!("int4")),
                REAL => other(ident!("float4")),

                // Time-like types
                TIME => {
                    if self.parse_keyword(WITH) {
                        self.expect_keywords(&[TIME, ZONE])?;
                        other(ident!("timetz"))
                    } else {
                        if self.parse_keyword(WITHOUT) {
                            self.expect_keywords(&[TIME, ZONE])?;
                        }
                        other(ident!("time"))
                    }
                }
                TIMESTAMP => {
                    let typ_mod = self.parse_timestamp_precision()?;
                    if self.parse_keyword(WITH) {
                        self.expect_keywords(&[TIME, ZONE])?;
                        RawDataType::Other {
                            name: RawItemName::Name(UnresolvedItemName::unqualified(ident!(
                                "timestamptz"
                            ))),
                            typ_mod,
                        }
                    } else {
                        if self.parse_keyword(WITHOUT) {
                            self.expect_keywords(&[TIME, ZONE])?;
                        }
                        RawDataType::Other {
                            name: RawItemName::Name(UnresolvedItemName::unqualified(ident!(
                                "timestamp"
                            ))),
                            typ_mod,
                        }
                    }
                }
                TIMESTAMPTZ => {
                    let typ_mod = self.parse_timestamp_precision()?;
                    RawDataType::Other {
                        name: RawItemName::Name(UnresolvedItemName::unqualified(ident!(
                            "timestamptz"
                        ))),
                        typ_mod,
                    }
                }

                // MZ "proprietary" types
                MAP => {
                    return self.parse_map_type();
                }

                // Misc.
                BOOLEAN => other(ident!("bool")),
                BYTES => other(ident!("bytea")),
                JSON => other(ident!("jsonb")),

                // We do not want any reserved keywords to be parsed as data type names,
                // eg "CASE 'foo' WHEN ... END" should not parse as "CAST 'foo' AS CASE"
                kw if kw.is_sometimes_reserved() => {
                    return self.expected(
                        self.peek_prev_pos(),
                        "a data type name",
                        Some(Token::Keyword(kw)),
                    );
                }
                _ => {
                    self.prev_token();
                    RawDataType::Other {
                        name: RawItemName::Name(self.parse_item_name()?),
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

    // parses the precision in timestamp(<precision>) and timestamptz(<precision>)
    fn parse_timestamp_precision(&mut self) -> Result<Vec<i64>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let typ_mod = self.parse_literal_int()?;
            self.expect_token(&Token::RParen)?;
            Ok(vec![typ_mod])
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
                if after_as {
                    self.prev_token();
                }
                Ok(None)
            }
            // Accept any other identifier after `AS` (though many dialects have restrictions on
            // keywords that may appear here). If there's no `AS`: don't parse keywords,
            // which may start a construct allowed in this position, to be parsed as aliases.
            // (For example, in `FROM t1 JOIN` the `JOIN` will always be parsed as a keyword,
            // not an alias.)
            Some(Token::Keyword(kw)) if after_as || !is_reserved(kw) => Ok(Some(kw.into())),
            Some(Token::Ident(id)) => Ok(Some(self.new_identifier(id)?)),
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

    fn parse_deferred_item_name(&mut self) -> Result<DeferredItemName<Raw>, ParserError> {
        Ok(match self.parse_raw_name()? {
            named @ RawItemName::Id(..) => DeferredItemName::Named(named),
            RawItemName::Name(name) => DeferredItemName::Deferred(name),
        })
    }

    fn parse_raw_name(&mut self) -> Result<RawItemName, ParserError> {
        if self.consume_token(&Token::LBracket) {
            let id = match self.next_token() {
                Some(Token::Ident(id)) => id.into_inner(),
                _ => return parser_err!(self, self.peek_prev_pos(), "expected id"),
            };
            self.expect_keyword(AS)?;
            let name = self.parse_item_name()?;
            // TODO(justin): is there a more idiomatic way to detect a fully-qualified name?
            if name.0.len() < 2 {
                return parser_err!(
                    self,
                    self.peek_prev_pos(),
                    "table name in square brackets must be fully qualified"
                );
            }

            let version = if self.parse_keywords(&[VERSION]) {
                let version = self.parse_version()?;
                Some(version)
            } else {
                None
            };

            self.expect_token(&Token::RBracket)?;
            Ok(RawItemName::Id(id, name, version))
        } else {
            Ok(RawItemName::Name(self.parse_item_name()?))
        }
    }

    fn parse_column_name(&mut self) -> Result<ColumnName<Raw>, ParserError> {
        let start = self.peek_pos();
        let mut item_name = self.parse_raw_name()?;
        let column_name = match &mut item_name {
            RawItemName::Name(UnresolvedItemName(identifiers)) => {
                if identifiers.len() < 2 {
                    return Err(ParserError::new(
                        start,
                        "need to specify an object and a column",
                    ));
                }
                identifiers.pop().unwrap()
            }
            RawItemName::Id(_, _, _) => {
                self.expect_token(&Token::Dot)?;
                self.parse_identifier()?
            }
        };

        Ok(ColumnName {
            relation: item_name,
            column: column_name,
        })
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
    fn parse_item_name(&mut self) -> Result<UnresolvedItemName, ParserError> {
        Ok(UnresolvedItemName(self.parse_identifiers()?))
    }

    /// Parse an object name.
    fn parse_object_name(
        &mut self,
        object_type: ObjectType,
    ) -> Result<UnresolvedObjectName, ParserError> {
        Ok(match object_type {
            ObjectType::Table
            | ObjectType::View
            | ObjectType::MaterializedView
            | ObjectType::Source
            | ObjectType::Subsource
            | ObjectType::Sink
            | ObjectType::Index
            | ObjectType::Type
            | ObjectType::Secret
            | ObjectType::Connection
            | ObjectType::Func
            | ObjectType::ContinualTask => UnresolvedObjectName::Item(self.parse_item_name()?),
            ObjectType::Role => UnresolvedObjectName::Role(self.parse_identifier()?),
            ObjectType::Cluster => UnresolvedObjectName::Cluster(self.parse_identifier()?),
            ObjectType::ClusterReplica => {
                UnresolvedObjectName::ClusterReplica(self.parse_cluster_replica_name()?)
            }
            ObjectType::Database => UnresolvedObjectName::Database(self.parse_database_name()?),
            ObjectType::Schema => UnresolvedObjectName::Schema(self.parse_schema_name()?),
            ObjectType::NetworkPolicy => {
                UnresolvedObjectName::NetworkPolicy(self.parse_identifier()?)
            }
        })
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
        match self.consume_identifier()? {
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

    fn consume_identifier(&mut self) -> Result<Option<Ident>, ParserError> {
        match self.peek_token() {
            Some(Token::Keyword(kw)) => {
                self.next_token();
                Ok(Some(kw.into()))
            }
            Some(Token::Ident(id)) => {
                self.next_token();
                Ok(Some(self.new_identifier(id)?))
            }
            _ => Ok(None),
        }
    }

    fn parse_qualified_identifier(&mut self, id: Ident) -> Result<Expr<Raw>, ParserError> {
        let mut id_parts = vec![id];
        match self.peek_token() {
            Some(Token::LParen) | Some(Token::Dot) => {
                let mut ends_with_wildcard = false;
                while self.consume_token(&Token::Dot) {
                    match self.next_token() {
                        Some(Token::Keyword(kw)) => id_parts.push(kw.into()),
                        Some(Token::Ident(id)) => id_parts.push(self.new_identifier(id)?),
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
                } else if self.peek_token() == Some(Token::LParen) {
                    let function =
                        self.parse_function(RawItemName::Name(UnresolvedItemName(id_parts)))?;
                    Ok(Expr::Function(function))
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

    fn parse_map_type(&mut self) -> Result<RawDataType, ParserError> {
        self.expect_token(&Token::LBracket)?;
        let key_type = Box::new(self.parse_data_type()?);
        self.expect_token(&Token::Arrow)?;
        let value_type = Box::new(self.parse_data_type()?);
        self.expect_token(&Token::RBracket)?;
        Ok(RawDataType::Map {
            key_type,
            value_type,
        })
    }

    fn parse_delete(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(FROM)?;
        let table_name = RawItemName::Name(self.parse_item_name()?);
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

    /// Parses a SELECT (or WITH, VALUES, TABLE) statement with optional AS OF.
    fn parse_select_statement(&mut self) -> Result<SelectStatement<Raw>, ParserError> {
        Ok(SelectStatement {
            query: self.parse_query()?,
            as_of: self.parse_optional_as_of()?,
        })
    }

    /// Parse a query expression, i.e. a `SELECT` statement optionally
    /// preceded with some `WITH` CTE declarations and optionally followed
    /// by `ORDER BY`. Unlike some other parse_... methods, this one doesn't
    /// expect the initial keyword to be already consumed
    fn parse_query(&mut self) -> Result<Query<Raw>, ParserError> {
        self.checked_recur_mut(|parser| {
            let cte_block = if parser.parse_keyword(WITH) {
                if parser.parse_keyword(MUTUALLY) {
                    parser.expect_keyword(RECURSIVE)?;
                    let options = if parser.consume_token(&Token::LParen) {
                        let options =
                            parser.parse_comma_separated(Self::parse_mut_rec_block_option)?;
                        parser.expect_token(&Token::RParen)?;
                        options
                    } else {
                        vec![]
                    };
                    CteBlock::MutuallyRecursive(MutRecBlock {
                        options,
                        ctes: parser.parse_comma_separated(Parser::parse_cte_mut_rec)?,
                    })
                } else {
                    // TODO: optional RECURSIVE
                    CteBlock::Simple(parser.parse_comma_separated(Parser::parse_cte)?)
                }
            } else {
                CteBlock::empty()
            };

            let body = parser.parse_query_body(SetPrecedence::Zero)?;

            parser.parse_query_tail(cte_block, body)
        })
    }

    fn parse_mut_rec_block_option(&mut self) -> Result<MutRecBlockOption<Raw>, ParserError> {
        match self.expect_one_of_keywords(&[RECURSION, RETURN, ERROR])? {
            RECURSION => {
                self.expect_keyword(LIMIT)?;
                Ok(MutRecBlockOption {
                    name: MutRecBlockOptionName::RecursionLimit,
                    value: self.parse_optional_option_value()?,
                })
            }
            RETURN => {
                self.expect_keywords(&[AT, RECURSION, LIMIT])?;
                Ok(MutRecBlockOption {
                    name: MutRecBlockOptionName::ReturnAtRecursionLimit,
                    value: self.parse_optional_option_value()?,
                })
            }
            ERROR => {
                self.expect_keywords(&[AT, RECURSION, LIMIT])?;
                Ok(MutRecBlockOption {
                    name: MutRecBlockOptionName::ErrorAtRecursionLimit,
                    value: self.parse_optional_option_value()?,
                })
            }
            _ => unreachable!(),
        }
    }

    fn parse_query_tail(
        &mut self,
        ctes: CteBlock<Raw>,
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
            _ => (CteBlock::empty(), vec![], None, None, body),
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

        // Parse LIMIT, FETCH, OFFSET in any order, but:
        // - Only at most one of LIMIT or FETCH is allowed.
        // - Only at most one occurrence is allowed from each of these.
        let mut limit = inner_limit;
        let mut offset = inner_offset;
        while let Some(parsed_keyword) = self.parse_one_of_keywords(&[LIMIT, OFFSET, FETCH]) {
            match parsed_keyword {
                LIMIT => {
                    if limit.is_some() {
                        return parser_err!(
                            self,
                            self.peek_prev_pos(),
                            "multiple LIMIT/FETCH clauses not allowed"
                        );
                    }
                    limit = if self.parse_keyword(ALL) {
                        None
                    } else {
                        Some(Limit {
                            with_ties: false,
                            quantity: self.parse_expr()?,
                        })
                    };
                }
                OFFSET => {
                    if offset.is_some() {
                        return parser_err!(
                            self,
                            self.peek_prev_pos(),
                            "multiple OFFSET clauses not allowed"
                        );
                    }
                    let value = self.parse_expr()?;
                    let _ = self.parse_one_of_keywords(&[ROW, ROWS]);
                    offset = Some(value);
                }
                FETCH => {
                    if limit.is_some() {
                        return parser_err!(
                            self,
                            self.peek_prev_pos(),
                            "multiple LIMIT/FETCH clauses not allowed"
                        );
                    }
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
                _ => unreachable!(),
            }
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

    /// Parse a mutually recursive CTE (`alias ( col1: typ1, col2: typ2, ... ) AS (subquery)`).
    ///
    /// The main distinction from `parse_cte` is that the column names and types are mandatory.
    /// This is not how SQL works for `WITH RECURSIVE`, but we are doing it for now to make the
    /// query interpretation that much easier.
    fn parse_cte_mut_rec(&mut self) -> Result<CteMutRec<Raw>, ParserError> {
        let name = self.parse_identifier()?;
        self.expect_token(&Token::LParen)?;
        let columns = self.parse_comma_separated(|parser| {
            Ok(CteMutRecColumnDef {
                name: parser.parse_identifier()?,
                data_type: parser.parse_data_type()?,
            })
        })?;
        self.expect_token(&Token::RParen)?;
        self.expect_keyword(AS)?;
        self.expect_token(&Token::LParen)?;
        let query = self.parse_query()?;
        self.expect_token(&Token::RParen)?;
        Ok(CteMutRec {
            name,
            columns,
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
        } else if self.parse_keyword(SHOW) {
            SetExpr::Show(self.parse_show()?)
        } else if self.parse_keyword(TABLE) {
            SetExpr::Table(self.parse_raw_name()?)
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
                // UNION and EXCEPT have the same precedence and evaluate left-to-right
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

    fn parse_set_operator(&self, token: &Option<Token>) -> Option<SetOperator> {
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
            // permits these for symmetry with zero column tables. We need
            // to sniff out `AS` here specially to support `SELECT AS OF ...`.
            Some(Token::Keyword(kw)) if kw.is_always_reserved() || kw == AS => vec![],
            Some(Token::Semicolon) | Some(Token::RParen) | None => vec![],
            _ => {
                let mut projection = vec![];
                loop {
                    projection.push(self.parse_select_item()?);
                    if !self.consume_token(&Token::Comma) {
                        break;
                    }
                    if self.peek_keyword(FROM) {
                        return parser_err!(
                            self,
                            self.peek_prev_pos(),
                            "invalid trailing comma in SELECT list",
                        );
                    }
                }
                projection
            }
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

        let qualify = if self.parse_keyword(QUALIFY) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let options = if self.parse_keyword(OPTIONS) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Self::parse_select_option)?;
            self.expect_token(&Token::RParen)?;
            options
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
            qualify,
            options,
        })
    }

    fn parse_select_option(&mut self) -> Result<SelectOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[EXPECTED, AGGREGATE, DISTINCT, LIMIT])? {
            EXPECTED => {
                self.expect_keywords(&[GROUP, SIZE])?;
                SelectOptionName::ExpectedGroupSize
            }
            AGGREGATE => {
                self.expect_keywords(&[INPUT, GROUP, SIZE])?;
                SelectOptionName::AggregateInputGroupSize
            }
            DISTINCT => {
                self.expect_keywords(&[ON, INPUT, GROUP, SIZE])?;
                SelectOptionName::DistinctOnInputGroupSize
            }
            LIMIT => {
                self.expect_keywords(&[INPUT, GROUP, SIZE])?;
                SelectOptionName::LimitInputGroupSize
            }
            _ => unreachable!(),
        };
        Ok(SelectOption {
            name,
            value: self.parse_optional_option_value()?,
        })
    }

    fn parse_set(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        let modifier = self.parse_one_of_keywords(&[SESSION, LOCAL]);
        let mut variable = self.parse_identifier().map_no_statement_parser_err()?;
        let mut normal = self.consume_token(&Token::Eq) || self.parse_keyword(TO);
        if !normal {
            match variable.as_str().parse() {
                Ok(TIME) => {
                    self.expect_keyword(ZONE).map_no_statement_parser_err()?;
                    variable = ident!("timezone");
                    normal = true;
                }
                Ok(NAMES) => {
                    variable = ident!("client_encoding");
                    normal = true;
                }
                _ => {}
            }
        }
        if variable.as_str().parse() == Ok(SCHEMA) {
            variable = ident!("search_path");
            let to = self
                .parse_set_schema_to()
                .map_parser_err(StatementKind::SetVariable)?;
            Ok(Statement::SetVariable(SetVariableStatement {
                local: modifier == Some(LOCAL),
                variable,
                to,
            }))
        } else if normal {
            let to = self
                .parse_set_variable_to()
                .map_parser_err(StatementKind::SetVariable)?;
            Ok(Statement::SetVariable(SetVariableStatement {
                local: modifier == Some(LOCAL),
                variable,
                to,
            }))
        } else if variable.as_str().parse() == Ok(TRANSACTION) && modifier.is_none() {
            // SET TRANSACTION transaction_mode
            Ok(Statement::SetTransaction(SetTransactionStatement {
                local: true,
                modes: self
                    .parse_transaction_modes(true)
                    .map_parser_err(StatementKind::SetTransaction)?,
            }))
        } else if modifier == Some(SESSION)
            && variable.as_str().parse() == Ok(CHARACTERISTICS)
            && self.parse_keywords(&[AS, TRANSACTION])
        {
            // SET SESSION CHARACTERISTICS AS TRANSACTION transaction_mode
            Ok(Statement::SetTransaction(SetTransactionStatement {
                local: false,
                modes: self
                    .parse_transaction_modes(true)
                    .map_parser_err(StatementKind::SetTransaction)?,
            }))
        } else {
            self.expected(self.peek_pos(), "equals sign or TO", self.peek_token())
                .map_no_statement_parser_err()
        }
    }

    fn parse_set_schema_to(&mut self) -> Result<SetVariableTo, ParserError> {
        if self.parse_keyword(DEFAULT) {
            Ok(SetVariableTo::Default)
        } else {
            let to = self.parse_set_variable_value()?;
            Ok(SetVariableTo::Values(vec![to]))
        }
    }

    fn parse_set_variable_to(&mut self) -> Result<SetVariableTo, ParserError> {
        if self.parse_keyword(DEFAULT) {
            Ok(SetVariableTo::Default)
        } else {
            Ok(SetVariableTo::Values(
                self.parse_comma_separated(Parser::parse_set_variable_value)?,
            ))
        }
    }

    fn parse_set_variable_value(&mut self) -> Result<SetVariableValue, ParserError> {
        if let Some(value) = self.maybe_parse(Parser::parse_value) {
            Ok(SetVariableValue::Literal(value))
        } else if let Some(ident) = self.maybe_parse(Parser::parse_identifier) {
            Ok(SetVariableValue::Ident(ident))
        } else {
            self.expected(self.peek_pos(), "variable value", self.peek_token())
        }
    }

    fn parse_reset(&mut self) -> Result<Statement<Raw>, ParserError> {
        let mut variable = self.parse_identifier()?;
        if variable.as_str().parse() == Ok(SCHEMA) {
            variable = ident!("search_path");
        }
        Ok(Statement::ResetVariable(ResetVariableStatement {
            variable,
        }))
    }

    fn parse_show(&mut self) -> Result<ShowStatement<Raw>, ParserError> {
        let redacted = self.parse_keyword(REDACTED);
        if redacted && !self.peek_keyword(CREATE) {
            return parser_err!(
                self,
                self.peek_pos(),
                "SHOW REDACTED is only supported for SHOW REDACTED CREATE ..."
            );
        }
        if self.parse_one_of_keywords(&[COLUMNS, FIELDS]).is_some() {
            self.parse_show_columns()
        } else if self.parse_keyword(OBJECTS) {
            let from = if self.parse_keywords(&[FROM]) {
                Some(self.parse_schema_name()?)
            } else {
                None
            };
            Ok(ShowStatement::ShowObjects(ShowObjectsStatement {
                object_type: ShowObjectType::Object,
                from,
                filter: self.parse_show_statement_filter()?,
            }))
        } else if let Some(object_type) = self.parse_plural_object_type() {
            let from = if object_type.lives_in_schema() {
                if self.parse_keywords(&[FROM]) {
                    Some(self.parse_schema_name()?)
                } else {
                    None
                }
            } else {
                None
            };

            let show_object_type = match object_type {
                ObjectType::Database => ShowObjectType::Database,
                ObjectType::Schema => {
                    let from = if self.parse_keyword(FROM) {
                        Some(self.parse_database_name()?)
                    } else {
                        None
                    };
                    ShowObjectType::Schema { from }
                }
                ObjectType::Table => {
                    let on_source = if self.parse_one_of_keywords(&[ON]).is_some() {
                        Some(self.parse_raw_name()?)
                    } else {
                        None
                    };
                    ShowObjectType::Table { on_source }
                }
                ObjectType::View => ShowObjectType::View,
                ObjectType::Source => {
                    let in_cluster = self.parse_optional_in_cluster()?;
                    ShowObjectType::Source { in_cluster }
                }
                ObjectType::Subsource => {
                    let on_source = if self.parse_one_of_keywords(&[ON]).is_some() {
                        Some(self.parse_raw_name()?)
                    } else {
                        None
                    };

                    if from.is_some() && on_source.is_some() {
                        return parser_err!(
                            self,
                            self.peek_prev_pos(),
                            "Cannot specify both FROM and ON"
                        );
                    }

                    ShowObjectType::Subsource { on_source }
                }
                ObjectType::Sink => {
                    let in_cluster = self.parse_optional_in_cluster()?;
                    ShowObjectType::Sink { in_cluster }
                }
                ObjectType::Type => ShowObjectType::Type,
                ObjectType::Role => ShowObjectType::Role,
                ObjectType::ClusterReplica => ShowObjectType::ClusterReplica,
                ObjectType::Secret => ShowObjectType::Secret,
                ObjectType::Connection => ShowObjectType::Connection,
                ObjectType::Cluster => ShowObjectType::Cluster,
                ObjectType::NetworkPolicy => ShowObjectType::NetworkPolicy,
                ObjectType::MaterializedView => {
                    let in_cluster = self.parse_optional_in_cluster()?;
                    ShowObjectType::MaterializedView { in_cluster }
                }
                ObjectType::ContinualTask => {
                    let in_cluster = self.parse_optional_in_cluster()?;
                    ShowObjectType::ContinualTask { in_cluster }
                }
                ObjectType::Index => {
                    let on_object = if self.parse_one_of_keywords(&[ON]).is_some() {
                        Some(self.parse_raw_name()?)
                    } else {
                        None
                    };

                    if from.is_some() && on_object.is_some() {
                        return parser_err!(
                            self,
                            self.peek_prev_pos(),
                            "Cannot specify both FROM and ON"
                        );
                    }

                    let in_cluster = self.parse_optional_in_cluster()?;
                    ShowObjectType::Index {
                        in_cluster,
                        on_object,
                    }
                }
                ObjectType::Func => {
                    return parser_err!(
                        self,
                        self.peek_prev_pos(),
                        format!("Unsupported SHOW on {object_type}")
                    );
                }
            };
            Ok(ShowStatement::ShowObjects(ShowObjectsStatement {
                object_type: show_object_type,
                from,
                filter: self.parse_show_statement_filter()?,
            }))
        } else if self.parse_keyword(CLUSTER) {
            Ok(ShowStatement::ShowVariable(ShowVariableStatement {
                variable: ident!("cluster"),
            }))
        } else if self.parse_keyword(PRIVILEGES) {
            self.parse_show_privileges()
        } else if self.parse_keywords(&[DEFAULT, PRIVILEGES]) {
            self.parse_show_default_privileges()
        } else if self.parse_keyword(ROLE) {
            self.expect_keyword(MEMBERSHIP)?;
            let role = if self.parse_keyword(FOR) {
                Some(self.parse_identifier()?)
            } else {
                None
            };
            Ok(ShowStatement::ShowObjects(ShowObjectsStatement {
                object_type: ShowObjectType::RoleMembership { role },
                from: None,
                filter: self.parse_show_statement_filter()?,
            }))
        } else if self.parse_keywords(&[CREATE, VIEW]) {
            Ok(ShowStatement::ShowCreateView(ShowCreateViewStatement {
                view_name: self.parse_raw_name()?,
                redacted,
            }))
        } else if self.parse_keywords(&[CREATE, MATERIALIZED, VIEW]) {
            Ok(ShowStatement::ShowCreateMaterializedView(
                ShowCreateMaterializedViewStatement {
                    materialized_view_name: self.parse_raw_name()?,
                    redacted,
                },
            ))
        } else if self.parse_keywords(&[CREATE, SOURCE]) {
            Ok(ShowStatement::ShowCreateSource(ShowCreateSourceStatement {
                source_name: self.parse_raw_name()?,
                redacted,
            }))
        } else if self.parse_keywords(&[CREATE, TABLE]) {
            Ok(ShowStatement::ShowCreateTable(ShowCreateTableStatement {
                table_name: self.parse_raw_name()?,
                redacted,
            }))
        } else if self.parse_keywords(&[CREATE, SINK]) {
            Ok(ShowStatement::ShowCreateSink(ShowCreateSinkStatement {
                sink_name: self.parse_raw_name()?,
                redacted,
            }))
        } else if self.parse_keywords(&[CREATE, INDEX]) {
            Ok(ShowStatement::ShowCreateIndex(ShowCreateIndexStatement {
                index_name: self.parse_raw_name()?,
                redacted,
            }))
        } else if self.parse_keywords(&[CREATE, CONNECTION]) {
            Ok(ShowStatement::ShowCreateConnection(
                ShowCreateConnectionStatement {
                    connection_name: self.parse_raw_name()?,
                    redacted,
                },
            ))
        } else if self.parse_keywords(&[CREATE, CLUSTER]) {
            if redacted {
                return parser_err!(
                    self,
                    self.peek_prev_pos(),
                    "SHOW REDACTED CREATE CLUSTER is not supported"
                );
            }
            Ok(ShowStatement::ShowCreateCluster(
                ShowCreateClusterStatement {
                    cluster_name: RawClusterName::Unresolved(self.parse_identifier()?),
                },
            ))
        } else if self.parse_keywords(&[CREATE, TYPE]) {
            Ok(ShowStatement::ShowCreateType(ShowCreateTypeStatement {
                type_name: self.parse_data_type()?,
                redacted,
            }))
        } else {
            let variable = if self.parse_keywords(&[TRANSACTION, ISOLATION, LEVEL]) {
                ident!("transaction_isolation")
            } else if self.parse_keywords(&[TIME, ZONE]) {
                ident!("timezone")
            } else {
                self.parse_identifier()?
            };
            Ok(ShowStatement::ShowVariable(ShowVariableStatement {
                variable,
            }))
        }
    }

    fn parse_show_columns(&mut self) -> Result<ShowStatement<Raw>, ParserError> {
        self.expect_one_of_keywords(&[FROM, IN])?;
        let table_name = self.parse_raw_name()?;
        // MySQL also supports FROM <database> here. In other words, MySQL
        // allows both FROM <table> FROM <database> and FROM <database>.<table>,
        // while we only support the latter for now.
        let filter = self.parse_show_statement_filter()?;
        Ok(ShowStatement::ShowColumns(ShowColumnsStatement {
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

    fn parse_show_privileges(&mut self) -> Result<ShowStatement<Raw>, ParserError> {
        let object_type = if self.parse_keyword(ON) {
            Some(self.expect_plural_system_object_type_for_privileges()?)
        } else {
            None
        };
        let role = if self.parse_keyword(FOR) {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        Ok(ShowStatement::ShowObjects(ShowObjectsStatement {
            object_type: ShowObjectType::Privileges { object_type, role },
            from: None,
            filter: self.parse_show_statement_filter()?,
        }))
    }

    fn parse_show_default_privileges(&mut self) -> Result<ShowStatement<Raw>, ParserError> {
        let object_type = if self.parse_keyword(ON) {
            Some(self.expect_plural_object_type_for_privileges()?)
        } else {
            None
        };
        let role = if self.parse_keyword(FOR) {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        Ok(ShowStatement::ShowObjects(ShowObjectsStatement {
            object_type: ShowObjectType::DefaultPrivileges { object_type, role },
            from: None,
            filter: self.parse_show_statement_filter()?,
        }))
    }

    fn parse_inspect(&mut self) -> Result<ShowStatement<Raw>, ParserError> {
        self.expect_keyword(SHARD)?;
        let id = self.parse_literal_string()?;
        Ok(ShowStatement::InspectShard(InspectShardStatement { id }))
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
                        );
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
                return self.parse_rows_from();
            } else {
                let name = self.parse_raw_name()?;
                self.expect_token(&Token::LParen)?;
                let args = self.parse_optional_args(false)?;
                let (with_ordinality, alias) = self.parse_table_function_suffix()?;
                return Ok(TableFactor::Function {
                    function: Function {
                        name,
                        args,
                        filter: None,
                        over: None,
                        distinct: false,
                    },
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
            if self.consume_token(&Token::LParen) {
                let args = self.parse_optional_args(false)?;
                let (with_ordinality, alias) = self.parse_table_function_suffix()?;
                Ok(TableFactor::Function {
                    function: Function {
                        name,
                        args,
                        filter: None,
                        over: None,
                        distinct: false,
                    },
                    alias,
                    with_ordinality,
                })
            } else {
                Ok(TableFactor::Table {
                    name,
                    alias: self.parse_optional_table_alias()?,
                })
            }
        }
    }

    fn parse_rows_from(&mut self) -> Result<TableFactor<Raw>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let functions = self.parse_comma_separated(Parser::parse_named_function)?;
        self.expect_token(&Token::RParen)?;
        let (with_ordinality, alias) = self.parse_table_function_suffix()?;
        Ok(TableFactor::RowsFrom {
            functions,
            alias,
            with_ordinality,
        })
    }

    /// Parses the things that can come after the argument list of a table function call. These are
    /// - optional WITH ORDINALITY
    /// - optional table alias
    /// - optional WITH ORDINALITY again! This is allowed just to keep supporting our earlier buggy
    ///   order where we allowed WITH ORDINALITY only after the table alias. (Postgres and other
    ///   systems support it only before the table alias.)
    fn parse_table_function_suffix(&mut self) -> Result<(bool, Option<TableAlias>), ParserError> {
        let with_ordinality_1 = self.parse_keywords(&[WITH, ORDINALITY]);
        let alias = self.parse_optional_table_alias()?;
        let with_ordinality_2 = self.parse_keywords(&[WITH, ORDINALITY]);
        if with_ordinality_1 && with_ordinality_2 {
            return parser_err!(
                self,
                self.peek_prev_pos(),
                "WITH ORDINALITY specified twice"
            );
        }
        Ok((with_ordinality_1 || with_ordinality_2, alias))
    }

    fn parse_named_function(&mut self) -> Result<Function<Raw>, ParserError> {
        let name = self.parse_raw_name()?;
        self.parse_function(name)
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
            let alias = self.parse_optional_alias(Keyword::is_reserved_in_table_alias)?;

            Ok(JoinConstraint::Using { columns, alias })
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
        let table_name = RawItemName::Name(self.parse_item_name()?);
        // The alias here doesn't support columns, so don't use parse_optional_table_alias.
        let alias = self.parse_optional_alias(Keyword::is_reserved_in_table_alias)?;
        let alias = alias.map(|name| TableAlias {
            name,
            columns: Vec::new(),
            strict: false,
        });

        self.expect_keyword(SET)?;
        let assignments = self.parse_comma_separated(Parser::parse_assignment)?;
        let selection = if self.parse_keyword(WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Update(UpdateStatement {
            table_name,
            alias,
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

    /// Parse `UP TO`, if present
    fn parse_optional_up_to(&mut self) -> Result<Option<Expr<Raw>>, ParserError> {
        if self.parse_keyword(UP) {
            self.expect_keyword(TO)?;
            self.parse_expr().map(Some)
        } else {
            Ok(None)
        }
    }

    /// Parse `AS OF`, if present.
    ///
    /// In contrast to `parse_optional_as_of`, this parser only supports `AS OF <time>` syntax and
    /// directly returns an `u64`. It is only meant to be used for internal SQL syntax.
    fn parse_optional_internal_as_of(&mut self) -> Result<Option<u64>, ParserError> {
        fn try_parse_u64(parser: &mut Parser) -> Option<u64> {
            let value = parser.parse_value().ok()?;
            let Value::Number(s) = value else { return None };
            s.parse().ok()
        }

        if self.parse_keywords(&[AS, OF]) {
            match try_parse_u64(self) {
                Some(time) => Ok(Some(time)),
                None => {
                    self.prev_token();
                    self.expected(self.peek_pos(), "`u64` literal", self.peek_token())
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
            modes: self.parse_transaction_modes(false)?,
        }))
    }

    fn parse_begin(&mut self) -> Result<Statement<Raw>, ParserError> {
        let _ = self.parse_one_of_keywords(&[TRANSACTION, WORK]);
        Ok(Statement::StartTransaction(StartTransactionStatement {
            modes: self.parse_transaction_modes(false)?,
        }))
    }

    fn parse_transaction_modes(
        &mut self,
        mut required: bool,
    ) -> Result<Vec<TransactionMode>, ParserError> {
        let mut modes = vec![];
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
                } else if self.parse_keywords(&[STRONG, SESSION, SERIALIZABLE]) {
                    TransactionIsolationLevel::StrongSessionSerializable
                } else if self.parse_keywords(&[STRICT, SERIALIZABLE]) {
                    TransactionIsolationLevel::StrictSerializable
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

    fn parse_tail(&self) -> Result<Statement<Raw>, ParserError> {
        parser_err!(
            self,
            self.peek_prev_pos(),
            "TAIL has been renamed to SUBSCRIBE"
        )
    }

    fn parse_subscribe(&mut self) -> Result<Statement<Raw>, ParserError> {
        let _ = self.parse_keyword(TO);
        let relation = if self.consume_token(&Token::LParen) {
            let query = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            SubscribeRelation::Query(query)
        } else {
            SubscribeRelation::Name(self.parse_raw_name()?)
        };
        let mut output = self.parse_subscribe_output()?;
        let options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Self::parse_subscribe_option)?;
            self.expect_token(&Token::RParen)?;
            options
        } else {
            vec![]
        };
        let as_of = self.parse_optional_as_of()?;
        let up_to = self.parse_optional_up_to()?;
        // For backwards compatibility, we allow parsing output options
        // (`ENVELOPE`, `WITHIN TIMESTAMP ORDER BY`) at the end of the
        // statement, if they haven't already been specified where we prefer
        // them, before the `WITH` options and before the `AS OF`/`UP TO`
        // options. Our preferred syntax better aligns with the option ordering
        // for `CREATE SINK` and `SELECT`.
        if output == SubscribeOutput::Diffs {
            output = self.parse_subscribe_output()?;
        }
        Ok(Statement::Subscribe(SubscribeStatement {
            relation,
            options,
            as_of,
            up_to,
            output,
        }))
    }

    fn parse_subscribe_option(&mut self) -> Result<SubscribeOption<Raw>, ParserError> {
        let name = match self.expect_one_of_keywords(&[PROGRESS, SNAPSHOT])? {
            PROGRESS => SubscribeOptionName::Progress,
            SNAPSHOT => SubscribeOptionName::Snapshot,
            _ => unreachable!(),
        };
        Ok(SubscribeOption {
            name,
            value: self.parse_optional_option_value()?,
        })
    }

    fn parse_subscribe_output(&mut self) -> Result<SubscribeOutput<Raw>, ParserError> {
        if self.parse_keywords(&[ENVELOPE]) {
            let keyword = self.expect_one_of_keywords(&[UPSERT, DEBEZIUM])?;
            self.expect_token(&Token::LParen)?;
            self.expect_keyword(KEY)?;
            let key_columns = self.parse_parenthesized_column_list(Mandatory)?;
            let output = match keyword {
                UPSERT => SubscribeOutput::EnvelopeUpsert { key_columns },
                DEBEZIUM => SubscribeOutput::EnvelopeDebezium { key_columns },
                _ => unreachable!("no other keyword allowed"),
            };
            self.expect_token(&Token::RParen)?;
            Ok(output)
        } else if self.parse_keywords(&[WITHIN, TIMESTAMP, ORDER, BY]) {
            Ok(SubscribeOutput::WithinTimestampOrderBy {
                order_by: self.parse_comma_separated(Parser::parse_order_by_expr)?,
            })
        } else {
            Ok(SubscribeOutput::Diffs)
        }
    }

    /// Parse an `EXPLAIN` statement, assuming that the `EXPLAIN` token
    /// has already been consumed.
    fn parse_explain(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        if self.parse_keyword(TIMESTAMP) {
            self.parse_explain_timestamp()
                .map_parser_err(StatementKind::ExplainTimestamp)
        } else if self.parse_keywords(&[FILTER, PUSHDOWN]) {
            self.parse_explain_pushdown()
                .map_parser_err(StatementKind::ExplainPushdown)
        } else if self.parse_keyword(ANALYZE) || self.parse_keyword(ANALYSE) {
            self.parse_explain_analyze()
                .map_parser_err(StatementKind::ExplainAnalyzeObject)
        } else if self.peek_keyword(KEY) || self.peek_keyword(VALUE) {
            self.parse_explain_schema()
                .map_parser_err(StatementKind::ExplainSinkSchema)
        } else {
            self.parse_explain_plan()
                .map_parser_err(StatementKind::ExplainPlan)
        }
    }

    fn parse_explainee(&mut self) -> Result<Explainee<Raw>, ParserError> {
        let explainee = if self.parse_keyword(VIEW) {
            // Parse: `VIEW name`
            Explainee::View(self.parse_raw_name()?)
        } else if self.parse_keywords(&[MATERIALIZED, VIEW]) {
            // Parse: `MATERIALIZED VIEW name`
            Explainee::MaterializedView(self.parse_raw_name()?)
        } else if self.parse_keyword(INDEX) {
            // Parse: `INDEX name`
            Explainee::Index(self.parse_raw_name()?)
        } else if self.parse_keywords(&[REPLAN, VIEW]) {
            // Parse: `REPLAN VIEW name`
            Explainee::ReplanView(self.parse_raw_name()?)
        } else if self.parse_keywords(&[REPLAN, MATERIALIZED, VIEW]) {
            // Parse: `REPLAN MATERIALIZED VIEW name`
            Explainee::ReplanMaterializedView(self.parse_raw_name()?)
        } else if self.parse_keywords(&[REPLAN, INDEX]) {
            // Parse: `REPLAN INDEX name`
            Explainee::ReplanIndex(self.parse_raw_name()?)
        } else {
            let broken = self.parse_keyword(BROKEN);

            if self.peek_keywords(&[CREATE, VIEW])
                || self.peek_keywords(&[CREATE, OR, REPLACE, VIEW])
            {
                // Parse: `BROKEN? CREATE [OR REPLACE] VIEW ...`
                let _ = self.parse_keyword(CREATE); // consume CREATE token
                let stmt = match self.parse_create_view()? {
                    Statement::CreateView(stmt) => stmt,
                    _ => panic!("Unexpected statement type return after parsing"),
                };

                Explainee::CreateView(Box::new(stmt), broken)
            } else if self.peek_keywords(&[CREATE, MATERIALIZED, VIEW])
                || self.peek_keywords(&[CREATE, OR, REPLACE, MATERIALIZED, VIEW])
            {
                // Parse: `BROKEN? CREATE [OR REPLACE] MATERIALIZED VIEW ...`
                let _ = self.parse_keyword(CREATE); // consume CREATE token
                let stmt = match self.parse_create_materialized_view()? {
                    Statement::CreateMaterializedView(stmt) => stmt,
                    _ => panic!("Unexpected statement type return after parsing"),
                };

                Explainee::CreateMaterializedView(Box::new(stmt), broken)
            } else if self.peek_keywords(&[CREATE, INDEX])
                || self.peek_keywords(&[CREATE, DEFAULT, INDEX])
            {
                // Parse: `BROKEN? CREATE INDEX ...`
                let _ = self.parse_keyword(CREATE); // consume CREATE token
                let stmt = match self.parse_create_index()? {
                    Statement::CreateIndex(stmt) => stmt,
                    _ => panic!("Unexpected statement type return after parsing"),
                };

                Explainee::CreateIndex(Box::new(stmt), broken)
            } else {
                // Parse: `BROKEN? query`
                let query = self.parse_select_statement()?;
                Explainee::Select(Box::new(query), broken)
            }
        };
        Ok(explainee)
    }

    /// Parse an `EXPLAIN ... PLAN` statement, assuming that the `EXPLAIN` token
    /// has already been consumed.
    fn parse_explain_plan(&mut self) -> Result<Statement<Raw>, ParserError> {
        let start = self.peek_pos();
        let (has_stage, stage) = match self.parse_one_of_keywords(&[
            RAW,
            DECORRELATED,
            LOCALLY,
            OPTIMIZED,
            PHYSICAL,
            OPTIMIZER,
            PLAN,
        ]) {
            Some(RAW) => {
                self.expect_keyword(PLAN)?;
                (true, Some(ExplainStage::RawPlan))
            }
            Some(DECORRELATED) => {
                self.expect_keyword(PLAN)?;
                (true, Some(ExplainStage::DecorrelatedPlan))
            }
            Some(LOCALLY) => {
                self.expect_keywords(&[OPTIMIZED, PLAN])?;
                (true, Some(ExplainStage::LocalPlan))
            }
            Some(OPTIMIZED) => {
                self.expect_keyword(PLAN)?;
                (true, Some(ExplainStage::GlobalPlan))
            }
            Some(PHYSICAL) => {
                self.expect_keyword(PLAN)?;
                (true, Some(ExplainStage::PhysicalPlan))
            }
            Some(OPTIMIZER) => {
                self.expect_keyword(TRACE)?;
                (true, Some(ExplainStage::Trace))
            }
            Some(PLAN) => {
                if self.parse_keyword(INSIGHTS) {
                    (true, Some(ExplainStage::PlanInsights))
                } else {
                    // Use the default plan for the explainee.
                    (true, None)
                }
            }
            None => {
                // Use the default plan for the explainee.
                (false, None)
            }
            _ => unreachable!(),
        };

        let with_options = if self.parse_keyword(WITH) {
            if self.consume_token(&Token::LParen) {
                let options = self.parse_comma_separated(Parser::parse_explain_plan_option)?;
                self.expect_token(&Token::RParen)?;
                options
            } else {
                self.prev_token(); // push back WITH in case it's actually a CTE
                vec![]
            }
        } else {
            vec![]
        };

        let format = if self.parse_keyword(AS) {
            match self.parse_one_of_keywords(&[TEXT, JSON, DOT, VERBOSE]) {
                Some(TEXT) => Some(ExplainFormat::Text),
                Some(JSON) => Some(ExplainFormat::Json),
                Some(DOT) => Some(ExplainFormat::Dot),
                Some(VERBOSE) => {
                    self.expect_keyword(TEXT)?;
                    Some(ExplainFormat::VerboseText)
                }
                None => return Err(ParserError::new(self.index, "expected a format")),
                _ => unreachable!(),
            }
        } else if has_stage && stage == Some(ExplainStage::PhysicalPlan) {
            // if EXPLAIN PHYSICAL PLAN is explicitly specified without AS, default to VERBOSE TEXT
            Some(ExplainFormat::VerboseText)
        } else {
            None
        };

        if has_stage {
            self.expect_keyword(FOR)?;
        }

        let explainee = self.parse_explainee()?;

        // Explainees that represent a view only work in association with an
        // explicitly defined stage.
        if matches!((explainee.is_view(), &stage), (true, None)) {
            let msg = "EXPLAIN statement for a view needs an explicit stage".to_string();
            return Err(self.error(start, msg));
        }

        Ok(Statement::ExplainPlan(ExplainPlanStatement {
            stage,
            with_options,
            format,
            explainee,
        }))
    }

    fn parse_explain_plan_option(&mut self) -> Result<ExplainPlanOption<Raw>, ParserError> {
        Ok(ExplainPlanOption {
            name: self.parse_explain_plan_option_name()?,
            value: self.parse_optional_option_value()?,
        })
    }

    /// Parse an `EXPLAIN FILTER PUSHDOWN` statement, assuming that the `EXPLAIN
    /// PUSHDOWN` tokens have already been consumed.
    fn parse_explain_pushdown(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(FOR)?;

        let explainee = self.parse_explainee()?;

        Ok(Statement::ExplainPushdown(ExplainPushdownStatement {
            explainee,
        }))
    }

    fn parse_explain_analyze(&mut self) -> Result<Statement<Raw>, ParserError> {
        // EXPLAIN ANALYZE CLUSTER (MEMORY | CPU) [WITH SKEW] [AS SQL]
        if self.parse_keyword(CLUSTER) {
            let properties = self.parse_explain_analyze_computation_properties()?;
            let as_sql = self.parse_keywords(&[AS, SQL]);
            return Ok(Statement::ExplainAnalyzeCluster(
                ExplainAnalyzeClusterStatement { properties, as_sql },
            ));
        }

        // EXPLAIN ANALYZE ((MEMORY | CPU) [WITH SKEW] | HINTS) FOR (INDEX ... | MATERIALIZED VIEW ...) [AS SQL]

        let properties = if self.parse_keyword(HINTS) {
            ExplainAnalyzeProperty::Hints
        } else {
            ExplainAnalyzeProperty::Computation(
                self.parse_explain_analyze_computation_properties()?,
            )
        };

        self.expect_keyword(FOR)?;

        let explainee = match self.expect_one_of_keywords(&[INDEX, MATERIALIZED])? {
            INDEX => Explainee::Index(self.parse_raw_name()?),
            MATERIALIZED => {
                self.expect_keyword(VIEW)?;
                Explainee::MaterializedView(self.parse_raw_name()?)
            }
            _ => unreachable!(),
        };

        let as_sql = self.parse_keywords(&[AS, SQL]);

        Ok(Statement::ExplainAnalyzeObject(
            ExplainAnalyzeObjectStatement {
                properties,
                explainee,
                as_sql,
            },
        ))
    }

    fn parse_explain_analyze_computation_properties(
        &mut self,
    ) -> Result<ExplainAnalyzeComputationProperties, ParserError> {
        let mut computation_properties = vec![CPU, MEMORY];
        let (kw, property) =
            self.parse_explain_analyze_computation_property(&computation_properties)?;
        let mut properties = vec![property];
        computation_properties.retain(|p| p != &kw);

        while self.consume_token(&Token::Comma) {
            let (kw, property) =
                self.parse_explain_analyze_computation_property(&computation_properties)?;
            computation_properties.retain(|p| p != &kw);
            properties.push(property);
        }

        let skew = self.parse_keywords(&[WITH, SKEW]);

        Ok(ExplainAnalyzeComputationProperties { properties, skew })
    }

    fn parse_explain_analyze_computation_property(
        &mut self,
        properties: &[Keyword],
    ) -> Result<(Keyword, ExplainAnalyzeComputationProperty), ParserError> {
        if properties.is_empty() {
            return Err(ParserError::new(
                self.peek_pos(),
                "both CPU and MEMORY were specified, expected WITH SKEW or FOR",
            ));
        }

        match self.expect_one_of_keywords(properties)? {
            CPU => Ok((CPU, ExplainAnalyzeComputationProperty::Cpu)),
            MEMORY => Ok((MEMORY, ExplainAnalyzeComputationProperty::Memory)),
            _ => unreachable!(),
        }
    }

    /// Parse an `EXPLAIN TIMESTAMP` statement, assuming that the `EXPLAIN
    /// TIMESTAMP` tokens have already been consumed.
    fn parse_explain_timestamp(&mut self) -> Result<Statement<Raw>, ParserError> {
        let format = if self.parse_keyword(AS) {
            match self.parse_one_of_keywords(&[TEXT, JSON, DOT]) {
                Some(TEXT) => Some(ExplainFormat::Text),
                Some(JSON) => Some(ExplainFormat::Json),
                None => return Err(ParserError::new(self.index, "expected a format")),
                _ => unreachable!(),
            }
        } else {
            None
        };

        self.expect_keyword(FOR)?;

        let query = self.parse_select_statement()?;

        Ok(Statement::ExplainTimestamp(ExplainTimestampStatement {
            format,
            select: query,
        }))
    }
    /// Parse an `EXPLAIN [KEY|VALUE] SCHEMA` statement assuming that the `EXPLAIN` token
    /// have already been consumed
    fn parse_explain_schema(&mut self) -> Result<Statement<Raw>, ParserError> {
        let schema_for = match self.expect_one_of_keywords(&[KEY, VALUE])? {
            KEY => ExplainSinkSchemaFor::Key,
            VALUE => ExplainSinkSchemaFor::Value,
            _ => unreachable!(),
        };

        self.expect_keyword(SCHEMA)?;

        let format = if self.parse_keyword(AS) {
            // only json format is supported
            self.expect_keyword(JSON)?;
            Some(ExplainFormat::Json)
        } else {
            None
        };

        self.expect_keywords(&[FOR, CREATE])?;

        if let Statement::CreateSink(statement) = self.parse_create_sink()? {
            Ok(Statement::ExplainSinkSchema(ExplainSinkSchemaStatement {
                schema_for,
                format,
                statement,
            }))
        } else {
            unreachable!("only create sink can be returned here");
        }
    }

    /// Parse a `DECLARE` statement, assuming that the `DECLARE` token
    /// has already been consumed.
    fn parse_declare(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        let name = self
            .parse_identifier()
            .map_parser_err(StatementKind::Declare)?;
        self.expect_keyword(CURSOR)
            .map_parser_err(StatementKind::Declare)?;
        if self.parse_keyword(WITH) {
            let err = parser_err!(
                self,
                self.peek_prev_pos(),
                format!("WITH HOLD is unsupported for cursors")
            )
            .map_parser_err(StatementKind::Declare);
            self.expect_keyword(HOLD)
                .map_parser_err(StatementKind::Declare)?;
            return err;
        }
        // WITHOUT HOLD is optional and the default behavior so we can ignore it.
        let _ = self.parse_keywords(&[WITHOUT, HOLD]);
        self.expect_keyword(FOR)
            .map_parser_err(StatementKind::Declare)?;
        let StatementParseResult { ast, sql } = self.parse_statement()?;
        Ok(Statement::Declare(DeclareStatement {
            name,
            stmt: Box::new(ast),
            sql: sql.to_string(),
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
    fn parse_prepare(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        let name = self
            .parse_identifier()
            .map_parser_err(StatementKind::Prepare)?;
        self.expect_keyword(AS)
            .map_parser_err(StatementKind::Prepare)?;
        let pos = self.peek_pos();
        //
        let StatementParseResult { ast, sql } = self.parse_statement()?;
        if !matches!(
            ast,
            Statement::Select(_)
                | Statement::Insert(_)
                | Statement::Delete(_)
                | Statement::Update(_)
                | Statement::Fetch(_),
        ) {
            return parser_err!(self, pos, "unpreparable statement").map_no_statement_parser_err();
        }
        Ok(Statement::Prepare(PrepareStatement {
            name,
            stmt: Box::new(ast),
            sql: sql.to_string(),
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
        let options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Self::parse_fetch_option)?;
            self.expect_token(&Token::RParen)?;
            options
        } else {
            vec![]
        };
        Ok(Statement::Fetch(FetchStatement {
            name,
            count,
            options,
        }))
    }

    fn parse_fetch_option(&mut self) -> Result<FetchOption<Raw>, ParserError> {
        self.expect_keyword(TIMEOUT)?;
        Ok(FetchOption {
            name: FetchOptionName::Timeout,
            value: self.parse_optional_option_value()?,
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

    /// Parse a `GRANT` statement, assuming that the `GRANT` token
    /// has already been consumed.
    fn parse_grant(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        match self.parse_privilege_specification() {
            Some(privileges) => self
                .parse_grant_privilege(privileges)
                .map_parser_err(StatementKind::GrantPrivileges),
            None => self
                .parse_grant_role()
                .map_parser_err(StatementKind::GrantRole),
        }
    }

    /// Parse a `GRANT PRIVILEGE` statement, assuming that the `GRANT` token
    /// and all privileges have already been consumed.
    fn parse_grant_privilege(
        &mut self,
        privileges: PrivilegeSpecification,
    ) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(ON)?;
        let target = self.expect_grant_target_specification("GRANT")?;
        self.expect_keyword(TO)?;
        let roles = self.parse_comma_separated(Parser::expect_role_specification)?;
        Ok(Statement::GrantPrivileges(GrantPrivilegesStatement {
            privileges,
            target,
            roles,
        }))
    }

    /// Parse a `GRANT ROLE` statement, assuming that the `GRANT` token
    /// has already been consumed.
    fn parse_grant_role(&mut self) -> Result<Statement<Raw>, ParserError> {
        let role_names = self.parse_comma_separated(Parser::parse_identifier)?;
        self.expect_keyword(TO)?;
        let member_names = self.parse_comma_separated(Parser::expect_role_specification)?;
        Ok(Statement::GrantRole(GrantRoleStatement {
            role_names,
            member_names,
        }))
    }

    /// Parse a `REVOKE` statement, assuming that the `REVOKE` token
    /// has already been consumed.
    fn parse_revoke(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        match self.parse_privilege_specification() {
            Some(privileges) => self
                .parse_revoke_privilege(privileges)
                .map_parser_err(StatementKind::RevokePrivileges),
            None => self
                .parse_revoke_role()
                .map_parser_err(StatementKind::RevokeRole),
        }
    }

    /// Parse a `REVOKE PRIVILEGE` statement, assuming that the `REVOKE` token
    /// and all privileges have already been consumed.
    fn parse_revoke_privilege(
        &mut self,
        privileges: PrivilegeSpecification,
    ) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(ON)?;
        let target = self.expect_grant_target_specification("REVOKE")?;
        self.expect_keyword(FROM)?;
        let roles = self.parse_comma_separated(Parser::expect_role_specification)?;
        Ok(Statement::RevokePrivileges(RevokePrivilegesStatement {
            privileges,
            target,
            roles,
        }))
    }

    /// Parse a `REVOKE ROLE` statement, assuming that the `REVOKE` token
    /// has already been consumed.
    fn parse_revoke_role(&mut self) -> Result<Statement<Raw>, ParserError> {
        let role_names = self.parse_comma_separated(Parser::parse_identifier)?;
        self.expect_keyword(FROM)?;
        let member_names = self.parse_comma_separated(Parser::expect_role_specification)?;
        Ok(Statement::RevokeRole(RevokeRoleStatement {
            role_names,
            member_names,
        }))
    }

    fn expect_grant_target_specification(
        &mut self,
        statement_type: &str,
    ) -> Result<GrantTargetSpecification<Raw>, ParserError> {
        if self.parse_keyword(SYSTEM) {
            return Ok(GrantTargetSpecification::System);
        }

        let (object_type, object_spec_inner) = if self.parse_keyword(ALL) {
            let object_type = self.expect_grant_revoke_plural_object_type(statement_type)?;
            let object_spec_inner = if self.parse_keyword(IN) {
                if !object_type.lives_in_schema() && object_type != ObjectType::Schema {
                    return parser_err!(
                        self,
                        self.peek_prev_pos(),
                        format!("IN invalid for {object_type}S")
                    );
                }
                match self.expect_one_of_keywords(&[DATABASE, SCHEMA])? {
                    DATABASE => GrantTargetSpecificationInner::All(
                        GrantTargetAllSpecification::AllDatabases {
                            databases: self.parse_comma_separated(Parser::parse_database_name)?,
                        },
                    ),
                    SCHEMA => {
                        if object_type == ObjectType::Schema {
                            self.prev_token();
                            self.expected(self.peek_pos(), DATABASE, self.peek_token())?;
                        }
                        GrantTargetSpecificationInner::All(
                            GrantTargetAllSpecification::AllSchemas {
                                schemas: self.parse_comma_separated(Parser::parse_schema_name)?,
                            },
                        )
                    }
                    _ => unreachable!(),
                }
            } else {
                GrantTargetSpecificationInner::All(GrantTargetAllSpecification::All)
            };
            (object_type, object_spec_inner)
        } else {
            let object_type = self.expect_grant_revoke_object_type(statement_type)?;
            let object_spec_inner = GrantTargetSpecificationInner::Objects {
                names: self
                    .parse_comma_separated(|parser| parser.parse_object_name(object_type))?,
            };
            (object_type, object_spec_inner)
        };

        Ok(GrantTargetSpecification::Object {
            object_type,
            object_spec_inner,
        })
    }

    /// Bail out if the current token is not an object type suitable for a GRANT/REVOKE, or consume
    /// and return it if it is.
    fn expect_grant_revoke_object_type(
        &mut self,
        statement_type: &str,
    ) -> Result<ObjectType, ParserError> {
        // If the object type is omitted, then it is assumed to be a table.
        let object_type = self.parse_object_type().unwrap_or(ObjectType::Table);
        self.expect_grant_revoke_object_type_inner(statement_type, object_type)
    }

    /// Bail out if the current token is not a plural object type suitable for a GRANT/REVOKE, or consume
    /// and return it if it is.
    fn expect_grant_revoke_plural_object_type(
        &mut self,
        statement_type: &str,
    ) -> Result<ObjectType, ParserError> {
        let object_type = self.expect_plural_object_type().map_err(|_| {
            // Limit the error message to allowed object types.
            self.expected::<_, ObjectType>(
                self.peek_pos(),
                "one of TABLES or TYPES or SECRETS or CONNECTIONS or SCHEMAS or DATABASES or CLUSTERS",
                self.peek_token(),
            )
                .unwrap_err()
        })?;
        self.expect_grant_revoke_object_type_inner(statement_type, object_type)?;
        Ok(object_type)
    }

    fn expect_grant_revoke_object_type_inner(
        &self,
        statement_type: &str,
        object_type: ObjectType,
    ) -> Result<ObjectType, ParserError> {
        match object_type {
            ObjectType::View
            | ObjectType::MaterializedView
            | ObjectType::Source
            | ObjectType::ContinualTask => {
                parser_err!(
                    self,
                    self.peek_prev_pos(),
                    format!(
                        "For object type {object_type}, you must specify 'TABLE' or omit the object type"
                    )
                )
            }
            ObjectType::Sink
            | ObjectType::Index
            | ObjectType::ClusterReplica
            | ObjectType::Role
            | ObjectType::Func
            | ObjectType::Subsource => {
                parser_err!(
                    self,
                    self.peek_prev_pos(),
                    format!("Unsupported {statement_type} on {object_type}")
                )
            }
            ObjectType::Table
            | ObjectType::Type
            | ObjectType::Cluster
            | ObjectType::Secret
            | ObjectType::Connection
            | ObjectType::Database
            | ObjectType::Schema
            | ObjectType::NetworkPolicy => Ok(object_type),
        }
    }

    /// Bail out if the current token is not an object type, or consume and return it if it is.
    fn expect_object_type(&mut self) -> Result<ObjectType, ParserError> {
        Ok(
            match self.expect_one_of_keywords(&[
                TABLE,
                VIEW,
                MATERIALIZED,
                SOURCE,
                SINK,
                INDEX,
                TYPE,
                ROLE,
                USER,
                CLUSTER,
                SECRET,
                CONNECTION,
                DATABASE,
                SCHEMA,
                FUNCTION,
                CONTINUAL,
                NETWORK,
            ])? {
                TABLE => ObjectType::Table,
                VIEW => ObjectType::View,
                MATERIALIZED => {
                    if let Err(e) = self.expect_keyword(VIEW) {
                        self.prev_token();
                        return Err(e);
                    }
                    ObjectType::MaterializedView
                }
                SOURCE => ObjectType::Source,
                SINK => ObjectType::Sink,
                INDEX => ObjectType::Index,
                TYPE => ObjectType::Type,
                ROLE | USER => ObjectType::Role,
                CLUSTER => {
                    if self.parse_keyword(REPLICA) {
                        ObjectType::ClusterReplica
                    } else {
                        ObjectType::Cluster
                    }
                }
                SECRET => ObjectType::Secret,
                CONNECTION => ObjectType::Connection,
                DATABASE => ObjectType::Database,
                SCHEMA => ObjectType::Schema,
                FUNCTION => ObjectType::Func,
                CONTINUAL => {
                    if let Err(e) = self.expect_keyword(TASK) {
                        self.prev_token();
                        return Err(e);
                    }
                    ObjectType::ContinualTask
                }
                NETWORK => {
                    if let Err(e) = self.expect_keyword(POLICY) {
                        self.prev_token();
                        return Err(e);
                    }
                    ObjectType::NetworkPolicy
                }
                _ => unreachable!(),
            },
        )
    }

    /// Look for an object type and return it if it matches.
    fn parse_object_type(&mut self) -> Option<ObjectType> {
        Some(
            match self.parse_one_of_keywords(&[
                TABLE,
                VIEW,
                MATERIALIZED,
                SOURCE,
                SINK,
                INDEX,
                TYPE,
                ROLE,
                USER,
                CLUSTER,
                SECRET,
                CONNECTION,
                DATABASE,
                SCHEMA,
                FUNCTION,
            ])? {
                TABLE => ObjectType::Table,
                VIEW => ObjectType::View,
                MATERIALIZED => {
                    if self.parse_keyword(VIEW) {
                        ObjectType::MaterializedView
                    } else {
                        self.prev_token();
                        return None;
                    }
                }
                SOURCE => ObjectType::Source,
                SINK => ObjectType::Sink,
                INDEX => ObjectType::Index,
                TYPE => ObjectType::Type,
                ROLE | USER => ObjectType::Role,
                CLUSTER => {
                    if self.parse_keyword(REPLICA) {
                        ObjectType::ClusterReplica
                    } else {
                        ObjectType::Cluster
                    }
                }
                SECRET => ObjectType::Secret,
                CONNECTION => ObjectType::Connection,
                DATABASE => ObjectType::Database,
                SCHEMA => ObjectType::Schema,
                FUNCTION => ObjectType::Func,
                _ => unreachable!(),
            },
        )
    }

    /// Bail out if the current token is not an object type in the plural form, or consume and return it if it is.
    fn expect_plural_object_type(&mut self) -> Result<ObjectType, ParserError> {
        Ok(
            match self.expect_one_of_keywords(&[
                TABLES,
                VIEWS,
                MATERIALIZED,
                SOURCES,
                SINKS,
                INDEXES,
                TYPES,
                ROLES,
                USERS,
                CLUSTER,
                CLUSTERS,
                SECRETS,
                CONNECTIONS,
                DATABASES,
                SCHEMAS,
                POLICIES,
            ])? {
                TABLES => ObjectType::Table,
                VIEWS => ObjectType::View,
                MATERIALIZED => {
                    if let Err(e) = self.expect_keyword(VIEWS) {
                        self.prev_token();
                        return Err(e);
                    }
                    ObjectType::MaterializedView
                }
                SOURCES => ObjectType::Source,
                SINKS => ObjectType::Sink,
                INDEXES => ObjectType::Index,
                TYPES => ObjectType::Type,
                ROLES | USERS => ObjectType::Role,
                CLUSTER => {
                    if let Err(e) = self.expect_keyword(REPLICAS) {
                        self.prev_token();
                        return Err(e);
                    }
                    ObjectType::ClusterReplica
                }
                CLUSTERS => ObjectType::Cluster,
                SECRETS => ObjectType::Secret,
                CONNECTIONS => ObjectType::Connection,
                DATABASES => ObjectType::Database,
                SCHEMAS => ObjectType::Schema,
                POLICIES => ObjectType::NetworkPolicy,
                _ => unreachable!(),
            },
        )
    }

    /// Look for an object type in the plural form and return it if it matches.
    fn parse_plural_object_type(&mut self) -> Option<ObjectType> {
        Some(
            match self.parse_one_of_keywords(&[
                TABLES,
                VIEWS,
                MATERIALIZED,
                SOURCES,
                SINKS,
                INDEXES,
                TYPES,
                ROLES,
                USERS,
                CLUSTER,
                CLUSTERS,
                SECRETS,
                CONNECTIONS,
                DATABASES,
                SCHEMAS,
                SUBSOURCES,
                CONTINUAL,
                NETWORK,
            ])? {
                TABLES => ObjectType::Table,
                VIEWS => ObjectType::View,
                MATERIALIZED => {
                    if self.parse_keyword(VIEWS) {
                        ObjectType::MaterializedView
                    } else {
                        self.prev_token();
                        return None;
                    }
                }
                SOURCES => ObjectType::Source,
                SINKS => ObjectType::Sink,
                INDEXES => ObjectType::Index,
                TYPES => ObjectType::Type,
                ROLES | USERS => ObjectType::Role,
                CLUSTER => {
                    if self.parse_keyword(REPLICAS) {
                        ObjectType::ClusterReplica
                    } else {
                        self.prev_token();
                        return None;
                    }
                }
                CLUSTERS => ObjectType::Cluster,
                SECRETS => ObjectType::Secret,
                CONNECTIONS => ObjectType::Connection,
                DATABASES => ObjectType::Database,
                SCHEMAS => ObjectType::Schema,
                SUBSOURCES => ObjectType::Subsource,
                CONTINUAL => {
                    if self.parse_keyword(TASKS) {
                        ObjectType::ContinualTask
                    } else {
                        self.prev_token();
                        return None;
                    }
                }
                NETWORK => {
                    if self.parse_keyword(POLICIES) {
                        ObjectType::NetworkPolicy
                    } else {
                        self.prev_token();
                        return None;
                    }
                }
                _ => unreachable!(),
            },
        )
    }

    /// Bail out if the current token is not a privilege object type, or consume and
    /// return it if it is.
    fn expect_plural_object_type_for_privileges(&mut self) -> Result<ObjectType, ParserError> {
        if let Some(object_type) = self.parse_one_of_keywords(&[VIEWS, SOURCES]) {
            return parser_err!(
                self,
                self.peek_prev_pos(),
                format!("For object type {object_type}, you must specify 'TABLES'")
            );
        }
        if self.parse_keywords(&[MATERIALIZED, VIEWS]) {
            self.prev_token();
            return parser_err!(
                self,
                self.peek_prev_pos(),
                format!("For object type MATERIALIZED VIEWS, you must specify 'TABLES'")
            );
        }

        Ok(
            match self.expect_one_of_keywords(&[
                TABLES,
                TYPES,
                CLUSTERS,
                SECRETS,
                CONNECTIONS,
                DATABASES,
                SCHEMAS,
            ])? {
                TABLES => ObjectType::Table,
                TYPES => ObjectType::Type,
                CLUSTERS => ObjectType::Cluster,
                SECRETS => ObjectType::Secret,
                CONNECTIONS => ObjectType::Connection,
                DATABASES => ObjectType::Database,
                SCHEMAS => ObjectType::Schema,
                _ => unreachable!(),
            },
        )
    }

    /// Bail out if the current token is not a privilege object type in the plural form, or consume and
    /// return it if it is.
    fn expect_plural_system_object_type_for_privileges(
        &mut self,
    ) -> Result<SystemObjectType, ParserError> {
        if let Some(object_type) = self.parse_one_of_keywords(&[VIEWS, SOURCES]) {
            return parser_err!(
                self,
                self.peek_prev_pos(),
                format!("For object type {object_type}, you must specify 'TABLES'")
            );
        }
        if self.parse_keywords(&[MATERIALIZED, VIEWS]) {
            self.prev_token();
            return parser_err!(
                self,
                self.peek_prev_pos(),
                format!("For object type MATERIALIZED VIEWS, you must specify 'TABLES'")
            );
        }

        Ok(
            match self.expect_one_of_keywords(&[
                SYSTEM,
                TABLES,
                TYPES,
                CLUSTERS,
                SECRETS,
                CONNECTIONS,
                DATABASES,
                SCHEMAS,
            ])? {
                SYSTEM => SystemObjectType::System,
                TABLES => SystemObjectType::Object(ObjectType::Table),
                TYPES => SystemObjectType::Object(ObjectType::Type),
                CLUSTERS => SystemObjectType::Object(ObjectType::Cluster),
                SECRETS => SystemObjectType::Object(ObjectType::Secret),
                CONNECTIONS => SystemObjectType::Object(ObjectType::Connection),
                DATABASES => SystemObjectType::Object(ObjectType::Database),
                SCHEMAS => SystemObjectType::Object(ObjectType::Schema),
                _ => unreachable!(),
            },
        )
    }

    /// Look for a privilege and return it if it matches.
    fn parse_privilege(&mut self) -> Option<Privilege> {
        Some(
            match self.parse_one_of_keywords(&[
                INSERT,
                SELECT,
                UPDATE,
                DELETE,
                USAGE,
                CREATE,
                CREATEROLE,
                CREATEDB,
                CREATECLUSTER,
                CREATENETWORKPOLICY,
            ])? {
                INSERT => Privilege::INSERT,
                SELECT => Privilege::SELECT,
                UPDATE => Privilege::UPDATE,
                DELETE => Privilege::DELETE,
                USAGE => Privilege::USAGE,
                CREATE => Privilege::CREATE,
                CREATEROLE => Privilege::CREATEROLE,
                CREATEDB => Privilege::CREATEDB,
                CREATECLUSTER => Privilege::CREATECLUSTER,
                CREATENETWORKPOLICY => Privilege::CREATENETWORKPOLICY,
                _ => unreachable!(),
            },
        )
    }

    /// Parse one or more privileges separated by a ','.
    fn parse_privilege_specification(&mut self) -> Option<PrivilegeSpecification> {
        if self.parse_keyword(ALL) {
            let _ = self.parse_keyword(PRIVILEGES);
            return Some(PrivilegeSpecification::All);
        }

        let mut privileges = Vec::new();
        while let Some(privilege) = self.parse_privilege() {
            privileges.push(privilege);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }

        if privileges.is_empty() {
            None
        } else {
            Some(PrivilegeSpecification::Privileges(privileges))
        }
    }

    /// Bail out if the current token is not a role specification, or consume and return it if it is.
    fn expect_role_specification(&mut self) -> Result<Ident, ParserError> {
        let _ = self.parse_keyword(GROUP);
        self.parse_identifier()
    }

    /// Parse a `REASSIGN OWNED` statement, assuming that the `REASSIGN` token
    /// has already been consumed.
    fn parse_reassign_owned(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keywords(&[OWNED, BY])?;
        let old_roles = self.parse_comma_separated(Parser::parse_identifier)?;
        self.expect_keyword(TO)?;
        let new_role = self.parse_identifier()?;
        Ok(Statement::ReassignOwned(ReassignOwnedStatement {
            old_roles,
            new_role,
        }))
    }

    fn parse_comment(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(ON)?;

        let object = match self.expect_one_of_keywords(&[
            TABLE,
            VIEW,
            COLUMN,
            MATERIALIZED,
            SOURCE,
            SINK,
            INDEX,
            FUNCTION,
            CONNECTION,
            TYPE,
            SECRET,
            ROLE,
            DATABASE,
            SCHEMA,
            CLUSTER,
            CONTINUAL,
            NETWORK,
        ])? {
            TABLE => {
                let name = self.parse_raw_name()?;
                CommentObjectType::Table { name }
            }
            VIEW => {
                let name = self.parse_raw_name()?;
                CommentObjectType::View { name }
            }
            MATERIALIZED => {
                self.expect_keyword(VIEW)?;
                let name = self.parse_raw_name()?;
                CommentObjectType::MaterializedView { name }
            }
            SOURCE => {
                let name = self.parse_raw_name()?;
                CommentObjectType::Source { name }
            }
            SINK => {
                let name = self.parse_raw_name()?;
                CommentObjectType::Sink { name }
            }
            INDEX => {
                let name = self.parse_raw_name()?;
                CommentObjectType::Index { name }
            }
            FUNCTION => {
                let name = self.parse_raw_name()?;
                CommentObjectType::Func { name }
            }
            CONNECTION => {
                let name = self.parse_raw_name()?;
                CommentObjectType::Connection { name }
            }
            TYPE => {
                let ty = self.parse_data_type()?;
                CommentObjectType::Type { ty }
            }
            SECRET => {
                let name = self.parse_raw_name()?;
                CommentObjectType::Secret { name }
            }
            ROLE => {
                let name = self.parse_identifier()?;
                CommentObjectType::Role { name }
            }
            DATABASE => {
                let name = self.parse_database_name()?;
                CommentObjectType::Database { name }
            }
            SCHEMA => {
                let name = self.parse_schema_name()?;
                CommentObjectType::Schema { name }
            }
            CLUSTER => {
                if self.parse_keyword(REPLICA) {
                    let name = self.parse_cluster_replica_name()?;
                    CommentObjectType::ClusterReplica { name }
                } else {
                    let name = self.parse_raw_ident()?;
                    CommentObjectType::Cluster { name }
                }
            }
            COLUMN => {
                let name = self.parse_column_name()?;
                CommentObjectType::Column { name }
            }
            CONTINUAL => {
                self.expect_keyword(TASK)?;
                let name = self.parse_raw_name()?;
                CommentObjectType::ContinualTask { name }
            }
            NETWORK => {
                self.expect_keyword(POLICY)?;
                let name = self.parse_raw_network_policy_name()?;
                CommentObjectType::NetworkPolicy { name }
            }
            _ => unreachable!(),
        };

        self.expect_keyword(IS)?;
        let comment = match self.next_token() {
            Some(Token::Keyword(NULL)) => None,
            Some(Token::String(s)) => Some(s),
            other => return self.expected(self.peek_prev_pos(), "NULL or literal string", other),
        };

        Ok(Statement::Comment(CommentStatement { object, comment }))
    }

    pub fn new_identifier<S>(&self, s: S) -> Result<Ident, ParserError>
    where
        S: TryInto<IdentString>,
        <S as TryInto<IdentString>>::Error: fmt::Display,
    {
        Ident::new(s).map_err(|e| ParserError {
            pos: self.peek_prev_pos(),
            message: e.to_string(),
        })
    }
}

impl CheckedRecursion for Parser<'_> {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

/// Represents an expression or query (a "fragment") with parentheses around it,
/// when it is unknown whether the fragment belongs to a larger expression or
/// query.
enum ParenthesizedFragment {
    Query(Query<Raw>),
    Exprs(Vec<Expr<Raw>>),
}

impl ParenthesizedFragment {
    /// Force the fragment into an expression.
    fn into_expr(self) -> Expr<Raw> {
        match self {
            ParenthesizedFragment::Exprs(exprs) => {
                // The `ParenthesizedFragment` represents that there were
                // parentheses surrounding `exprs`, so...
                if exprs.len() == 1 {
                    // ...if there was only one expression, the parentheses
                    // were simple nesting...
                    Expr::Nested(Box::new(exprs.into_element()))
                } else {
                    // ...and if there were multiple expressions, the
                    // parentheses formed an implicit row constructor.
                    Expr::Row { exprs }
                }
            }
            // Queries become subquery expressions.
            ParenthesizedFragment::Query(query) => Expr::Subquery(Box::new(query)),
        }
    }
}

// Include the `Parser::parse_~` implementations for simple options derived by
// the crate's build.rs script.
include!(concat!(env!("OUT_DIR"), "/parse.simple_options.rs"));
