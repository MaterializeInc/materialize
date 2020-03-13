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
use std::ops::Range;
use std::str::FromStr;

use log::debug;

use repr::datetime::DateTimeField;

use crate::ast::*;
use crate::keywords;
use crate::tokenizer::*;

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($parser:expr, $range:expr, $MSG:expr) => {
        Err($parser.error($range, $MSG.to_string()))
    };
    ($parser:expr, $range:expr, $($arg:tt)*) => {
        Err($parser.error($range, format!($($arg)*)))
    };
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParserError {
    /// Original query (so we can easily print an error)
    pub sql: String,
    /// Part of the query at which the error occurred
    pub range: Range<usize>,
    pub message: String,
}

#[derive(PartialEq)]
pub enum IsOptional {
    Optional,
    Mandatory,
}
use IsOptional::*;

pub enum IsLateral {
    Lateral,
    NotLateral,
}
use IsLateral::*;

impl<'a> fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // be careful with edge cases
        // 0 <= self.range.start <= self.sql.len()
        // 1 <= self.range.end <= self.sql.len()+1
        // 0 <= line_start <= self.range.start
        // 1 <= safe_end <= self.sql.len()-1
        // safe_end <= line_end <= self.sql.len()
        let line_start = self.sql[..self.range.start]
            .rfind('\n')
            .map(|start| start + 1)
            .unwrap_or(0);
        let safe_end = self.range.end.min(self.sql.len() - 1);
        let line_end = self.sql[safe_end..]
            .find('\n')
            .map(|end| safe_end + end)
            .unwrap_or_else(|| self.sql.len());
        let line = &self.sql[line_start..line_end.max(line_start)];
        let underline = std::iter::repeat(' ')
            .take(self.range.start - line_start)
            .chain(std::iter::repeat('^').take(self.range.end - self.range.start))
            .collect::<String>();
        write!(f, "Parse error:\n{}\n{}\n{}", line, underline, self.message,)
    }
}

impl<'a> Error for ParserError {}

/// SQL Parser
pub struct Parser {
    sql: String,
    tokens: Vec<(Token, Range<usize>)>,
    /// The index of the first unprocessed token in `self.tokens`
    index: usize,
}

impl Parser {
    /// Parse the specified tokens
    pub fn new(sql: String, tokens: Vec<(Token, Range<usize>)>) -> Self {
        Parser {
            sql,
            tokens,
            index: 0,
        }
    }

    fn error(&self, range: Range<usize>, message: String) -> ParserError {
        ParserError {
            sql: self.sql.clone(),
            range,
            message,
        }
    }

    /// Parse a SQL statement and produce an Abstract Syntax Tree (AST)
    pub fn parse_sql(sql: String) -> Result<Vec<Statement>, ParserError> {
        debug!("Parsing sql '{}'...", &sql);
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize()?;
        let mut parser = Parser::new(sql, tokens);
        let mut stmts = Vec::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.peek_token().is_none() {
                break;
            } else if expecting_statement_delimiter {
                return parser.expected(
                    parser.peek_range(),
                    "end of statement",
                    parser.peek_token(),
                );
            }

            let statement = parser.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Parse a single top-level statement (such as SELECT, INSERT, CREATE, etc.),
    /// stopping before the statement separator, if any.
    pub fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        match self.next_token() {
            Some(t) => match t {
                Token::Word(ref w) if w.keyword != "" => match w.keyword.as_ref() {
                    "SELECT" | "WITH" | "VALUES" => {
                        self.prev_token();
                        Ok(Statement::Query(Box::new(self.parse_query()?)))
                    }
                    "CREATE" => Ok(self.parse_create()?),
                    "DROP" => Ok(self.parse_drop()?),
                    "DELETE" => Ok(self.parse_delete()?),
                    "INSERT" => Ok(self.parse_insert()?),
                    "UPDATE" => Ok(self.parse_update()?),
                    "ALTER" => Ok(self.parse_alter()?),
                    "COPY" => Ok(self.parse_copy()?),
                    "SET" => Ok(self.parse_set()?),
                    "SHOW" => Ok(self.parse_show()?),
                    "START" => Ok(self.parse_start_transaction()?),
                    // `BEGIN` is a nonstandard but common alias for the
                    // standard `START TRANSACTION` statement. It is supported
                    // by at least PostgreSQL and MySQL.
                    "BEGIN" => Ok(self.parse_begin()?),
                    "COMMIT" => Ok(self.parse_commit()?),
                    "ROLLBACK" => Ok(self.parse_rollback()?),
                    "TAIL" => Ok(Statement::Tail {
                        name: self.parse_object_name()?,
                    }),
                    "EXPLAIN" => Ok(self.parse_explain()?),
                    _ => parser_err!(
                        self,
                        self.peek_prev_range(),
                        format!(
                            "Unexpected keyword {:?} at the beginning of a statement",
                            w.to_string()
                        )
                    ),
                },
                Token::LParen => {
                    self.prev_token();
                    Ok(Statement::Query(Box::new(self.parse_query()?)))
                }
                unexpected => self.expected(
                    self.peek_prev_range(),
                    "a keyword at the beginning of a statement",
                    Some(unexpected),
                ),
            },
            None => self.expected(self.peek_prev_range(), "SQL statement", None),
        }
    }

    /// Parse a new expression
    pub fn parse_expr(&mut self) -> Result<Expr, ParserError> {
        self.parse_subexpr(0)
    }

    /// Parse tokens until the precedence changes
    pub fn parse_subexpr(&mut self, precedence: u8) -> Result<Expr, ParserError> {
        debug!("parsing expr");
        let mut expr = self.parse_prefix()?;
        debug!("prefix: {:?}", expr);
        loop {
            let next_precedence = self.get_next_precedence()?;
            debug!("next precedence: {:?}", next_precedence);
            if precedence >= next_precedence {
                break;
            }

            expr = self.parse_infix(expr, next_precedence)?;
        }
        Ok(expr)
    }

    /// Parse an expression prefix
    pub fn parse_prefix(&mut self) -> Result<Expr, ParserError> {
        let tok = self
            .next_token()
            .ok_or_else(|| self.error(self.peek_prev_range(), "Unexpected EOF".to_string()))?;
        let expr = match tok {
            Token::Word(w) => match w.keyword.as_ref() {
                "TRUE" | "FALSE" | "NULL" => {
                    self.prev_token();
                    Ok(Expr::Value(self.parse_value()?))
                }
                "ARRAY" => {
                    self.prev_token();
                    Ok(Expr::Value(self.parse_value()?))
                }
                "CASE" => self.parse_case_expr(),
                "CAST" => self.parse_cast_expr(),
                "DATE" => Ok(Expr::Value(self.parse_date()?)),
                "EXISTS" => self.parse_exists_expr(),
                "EXTRACT" => self.parse_extract_expr(),
                "INTERVAL" => self.parse_literal_interval(),
                "NOT" => Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(self.parse_subexpr(Self::UNARY_NOT_PREC)?),
                }),
                "TIME" => Ok(Expr::Value(self.parse_time()?)),
                "TIMESTAMP" => self.parse_timestamp(),
                "TIMESTAMPTZ" => self.parse_timestamptz(),
                // Here `w` is a word, check if it's a part of a multi-part
                // identifier, a function call, or a simple identifier:
                _ => match self.peek_token() {
                    Some(Token::LParen) | Some(Token::Period) => {
                        let mut id_parts: Vec<Ident> = vec![w.to_ident()];
                        let mut ends_with_wildcard = false;
                        while self.consume_token(&Token::Period) {
                            match self.next_token() {
                                Some(Token::Word(w)) => id_parts.push(w.to_ident()),
                                Some(Token::Mult) => {
                                    ends_with_wildcard = true;
                                    break;
                                }
                                unexpected => {
                                    return self.expected(
                                        self.peek_prev_range(),
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
                            self.parse_function(ObjectName(id_parts))
                        } else {
                            Ok(Expr::CompoundIdentifier(id_parts))
                        }
                    }
                    _ => Ok(Expr::Identifier(w.to_ident())),
                },
            }, // End of Token::Word
            Token::Mult => Ok(Expr::Wildcard),
            tok @ Token::Minus | tok @ Token::Plus => {
                let op = if tok == Token::Plus {
                    UnaryOperator::Plus
                } else {
                    UnaryOperator::Minus
                };
                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(self.parse_subexpr(45)?),
                })
            }
            Token::Number(_) | Token::SingleQuotedString(_) | Token::HexStringLiteral(_) => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            Token::Parameter(s) => Ok(Expr::Parameter(match s.parse() {
                Ok(n) => n,
                Err(err) => {
                    return parser_err!(
                        self,
                        self.peek_prev_range(),
                        "unable to parse parameter: {}",
                        err
                    )
                }
            })),
            Token::LParen => {
                let expr = if self.parse_keyword("SELECT") || self.parse_keyword("WITH") {
                    self.prev_token();
                    Expr::Subquery(Box::new(self.parse_query()?))
                } else {
                    Expr::Nested(Box::new(self.parse_expr()?))
                };
                self.expect_token(&Token::RParen)?;
                Ok(expr)
            }
            unexpected => self.expected(self.peek_prev_range(), "an expression", Some(unexpected)),
        }?;

        if self.parse_keyword("COLLATE") {
            Ok(Expr::Collate {
                expr: Box::new(expr),
                collation: self.parse_object_name()?,
            })
        } else {
            Ok(expr)
        }
    }

    pub fn parse_function(&mut self, name: ObjectName) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let all = self.parse_keyword("ALL");
        let all_range = self.peek_prev_range();
        let distinct = self.parse_keyword("DISTINCT");
        let distinct_range = self.peek_prev_range();
        if all && distinct {
            return parser_err!(
                self,
                all_range.start..distinct_range.end,
                format!(
                    "Cannot specify both ALL and DISTINCT in function: {}",
                    name.to_string(),
                )
            );
        }
        let args = self.parse_optional_args()?;
        let over = if self.parse_keyword("OVER") {
            // TBD: support window names (`OVER mywin`) in place of inline specification
            self.expect_token(&Token::LParen)?;
            let partition_by = if self.parse_keywords(vec!["PARTITION", "BY"]) {
                // a list of possibly-qualified column names
                self.parse_comma_separated(Parser::parse_expr)?
            } else {
                vec![]
            };
            let order_by = if self.parse_keywords(vec!["ORDER", "BY"]) {
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
            over,
            distinct,
        }))
    }

    pub fn parse_window_frame(&mut self) -> Result<WindowFrame, ParserError> {
        let units = match self.next_token() {
            Some(Token::Word(w)) => w
                .keyword
                .parse::<WindowFrameUnits>()
                .map_err(|e| self.error(self.peek_prev_range(), e))?,
            unexpected => {
                return self.expected(self.peek_prev_range(), "ROWS, RANGE, GROUPS", unexpected)
            }
        };
        let (start_bound, end_bound) = if self.parse_keyword("BETWEEN") {
            let start_bound = self.parse_window_frame_bound()?;
            self.expect_keyword("AND")?;
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
    pub fn parse_window_frame_bound(&mut self) -> Result<WindowFrameBound, ParserError> {
        if self.parse_keywords(vec!["CURRENT", "ROW"]) {
            Ok(WindowFrameBound::CurrentRow)
        } else {
            let rows = if self.parse_keyword("UNBOUNDED") {
                None
            } else {
                Some(self.parse_literal_uint()?)
            };
            if self.parse_keyword("PRECEDING") {
                Ok(WindowFrameBound::Preceding(rows))
            } else if self.parse_keyword("FOLLOWING") {
                Ok(WindowFrameBound::Following(rows))
            } else {
                self.expected(
                    self.peek_range(),
                    "PRECEDING or FOLLOWING",
                    self.peek_token(),
                )
            }
        }
    }

    pub fn parse_case_expr(&mut self) -> Result<Expr, ParserError> {
        let mut operand = None;
        if !self.parse_keyword("WHEN") {
            operand = Some(Box::new(self.parse_expr()?));
            self.expect_keyword("WHEN")?;
        }
        let mut conditions = vec![];
        let mut results = vec![];
        loop {
            conditions.push(self.parse_expr()?);
            self.expect_keyword("THEN")?;
            results.push(self.parse_expr()?);
            if !self.parse_keyword("WHEN") {
                break;
            }
        }
        let else_result = if self.parse_keyword("ELSE") {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.expect_keyword("END")?;
        Ok(Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        })
    }

    /// Parse a SQL CAST function e.g. `CAST(expr AS FLOAT)`
    pub fn parse_cast_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword("AS")?;
        let data_type = self.parse_data_type()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Cast {
            expr: Box::new(expr),
            data_type,
        })
    }

    /// Parse a SQL EXISTS expression e.g. `WHERE EXISTS(SELECT ...)`.
    pub fn parse_exists_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let exists_node = Expr::Exists(Box::new(self.parse_query()?));
        self.expect_token(&Token::RParen)?;
        Ok(exists_node)
    }

    pub fn parse_extract_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let field = self.parse_extract_field()?;
        self.expect_keyword("FROM")?;
        let expr = self.parse_expr()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Extract {
            field,
            expr: Box::new(expr),
        })
    }

    /// Parse the kinds of things that can be fed to EXTRACT and DATE_TRUNC
    pub fn parse_extract_field(&mut self) -> Result<ExtractField, ParserError> {
        let tok = self.next_token();
        let field: Result<ExtractField, _> = match tok {
            Some(Token::Word(ref k)) => k.keyword.parse(),
            Some(Token::SingleQuotedString(ref s)) => s.parse(),
            _ => return self.expected(self.peek_prev_range(), "extract field token", tok),
        };
        match field {
            Ok(f) => Ok(f),
            Err(_) => self.expected(self.peek_prev_range(), "valid extract field", tok)?,
        }
    }

    fn parse_date(&mut self) -> Result<Value, ParserError> {
        let value = self.parse_literal_string()?;
        Ok(Value::Date(value))
    }

    fn parse_time(&mut self) -> Result<Value, ParserError> {
        let value = self.parse_literal_string()?;
        Ok(Value::Time(value))
    }

    fn parse_timestamp(&mut self) -> Result<Expr, ParserError> {
        if self.parse_keyword("WITH") {
            self.expect_keywords(&["TIME", "ZONE"])?;
            let value = self.parse_literal_string()?;
            return Ok(Expr::Value(Value::TimestampTz(value)));
        } else if self.parse_keyword("WITHOUT") {
            self.expect_keywords(&["TIME", "ZONE"])?;
        }
        let value = self.parse_literal_string()?;
        Ok(Expr::Value(Value::Timestamp(value)))
    }

    fn parse_timestamptz(&mut self) -> Result<Expr, ParserError> {
        let value = self.parse_literal_string()?;
        Ok(Expr::Value(Value::TimestampTz(value)))
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
    pub fn parse_literal_interval(&mut self) -> Result<Expr, ParserError> {
        // The first token in an interval is a string literal which specifies
        // the duration of the interval.
        let value = self.parse_literal_string()?;

        // Determine the range of TimeUnits, whether explicit (`INTERVAL ... DAY TO MINUTE`) or
        // implicit (in which all date fields are eligible).
        let (precision_high, precision_low, fsec_max_precision) =
            match self.expect_one_of_keywords(&[
                "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "YEARS", "MONTHS", "DAYS",
                "HOURS", "MINUTES", "SECONDS",
            ]) {
                Ok(d) => {
                    let d_range = self.peek_prev_range();
                    if self.parse_keyword("TO") {
                        let e = self.expect_one_of_keywords(&[
                            "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "YEARS", "MONTHS",
                            "DAYS", "HOURS", "MINUTES", "SECONDS",
                        ])?;
                        let e_range = self.peek_prev_range();

                        let high = DateTimeField::from_str(d)
                            .map_err(|e| self.error(self.peek_prev_range(), e))?;
                        let low = DateTimeField::from_str(e)
                            .map_err(|e| self.error(self.peek_prev_range(), e))?;

                        // Check for invalid ranges, i.e. precision_high is the same
                        // as or a less significant DateTimeField than
                        // precision_low.
                        if high >= low {
                            return parser_err!(
                                self,
                                d_range.start..e_range.end,
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
                        let low = DateTimeField::from_str(d)
                            .map_err(|e| self.error(self.peek_prev_range(), e))?;
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
    pub fn parse_infix(&mut self, expr: Expr, precedence: u8) -> Result<Expr, ParserError> {
        debug!("parsing infix");
        let tok = self.next_token().unwrap(); // safe as EOF's precedence is the lowest

        let regular_binary_operator = match tok {
            Token::Eq => Some(BinaryOperator::Eq),
            Token::Neq => Some(BinaryOperator::NotEq),
            Token::Gt => Some(BinaryOperator::Gt),
            Token::GtEq => Some(BinaryOperator::GtEq),
            Token::Lt => Some(BinaryOperator::Lt),
            Token::LtEq => Some(BinaryOperator::LtEq),
            Token::Plus => Some(BinaryOperator::Plus),
            Token::Minus => Some(BinaryOperator::Minus),
            Token::Mult => Some(BinaryOperator::Multiply),
            Token::Mod => Some(BinaryOperator::Modulus),
            Token::Div => Some(BinaryOperator::Divide),
            Token::JsonGet => Some(BinaryOperator::JsonGet),
            Token::JsonGetAsText => Some(BinaryOperator::JsonGetAsText),
            Token::JsonGetPath => Some(BinaryOperator::JsonGetPath),
            Token::JsonGetPathAsText => Some(BinaryOperator::JsonGetPathAsText),
            Token::JsonContainsJson => Some(BinaryOperator::JsonContainsJson),
            Token::JsonContainedInJson => Some(BinaryOperator::JsonContainedInJson),
            Token::JsonContainsField => Some(BinaryOperator::JsonContainsField),
            Token::JsonContainsAnyFields => Some(BinaryOperator::JsonContainsAnyFields),
            Token::JsonContainsAllFields => Some(BinaryOperator::JsonContainsAllFields),
            Token::JsonConcat => Some(BinaryOperator::JsonConcat),
            Token::JsonDeletePath => Some(BinaryOperator::JsonDeletePath),
            Token::JsonContainsPath => Some(BinaryOperator::JsonContainsPath),
            Token::JsonApplyPathPredicate => Some(BinaryOperator::JsonApplyPathPredicate),
            Token::Word(ref k) => match k.keyword.as_ref() {
                "AND" => Some(BinaryOperator::And),
                "OR" => Some(BinaryOperator::Or),
                "LIKE" => Some(BinaryOperator::Like),
                "NOT" => {
                    if self.parse_keyword("LIKE") {
                        Some(BinaryOperator::NotLike)
                    } else {
                        None
                    }
                }
                _ => None,
            },
            _ => None,
        };
        let op_range = self.peek_prev_range();

        if let Some(op) = regular_binary_operator {
            let any = self.parse_keyword("ANY");
            let some = !any && self.parse_keyword("SOME");
            let all = !any && !some && self.parse_keyword("ALL");
            if any || some || all {
                use BinaryOperator::*;
                match op {
                    Eq | NotEq | Gt | GtEq | Lt | LtEq => (),
                    _ => self.expected(op_range, "comparison operator", Some(tok))?,
                }
                self.expect_token(&Token::LParen)?;
                let query = self.parse_query()?;
                self.expect_token(&Token::RParen)?;
                if any || some {
                    Ok(Expr::Any {
                        left: Box::new(expr),
                        op,
                        right: Box::new(query),
                        some,
                    })
                } else {
                    Ok(Expr::All {
                        left: Box::new(expr),
                        op,
                        right: Box::new(query),
                    })
                }
            } else {
                Ok(Expr::BinaryOp {
                    left: Box::new(expr),
                    op,
                    right: Box::new(self.parse_subexpr(precedence)?),
                })
            }
        } else if let Token::Word(ref k) = tok {
            match k.keyword.as_ref() {
                "IS" => {
                    if self.parse_keyword("NULL") {
                        Ok(Expr::IsNull(Box::new(expr)))
                    } else if self.parse_keywords(vec!["NOT", "NULL"]) {
                        Ok(Expr::IsNotNull(Box::new(expr)))
                    } else {
                        self.expected(
                            self.peek_range(),
                            "NULL or NOT NULL after IS",
                            self.peek_token(),
                        )
                    }
                }
                "NOT" | "IN" | "BETWEEN" => {
                    self.prev_token();
                    let negated = self.parse_keyword("NOT");
                    if self.parse_keyword("IN") {
                        self.parse_in(expr, negated)
                    } else if self.parse_keyword("BETWEEN") {
                        self.parse_between(expr, negated)
                    } else {
                        self.expected(
                            self.peek_range(),
                            "IN or BETWEEN after NOT",
                            self.peek_token(),
                        )
                    }
                }
                // Can only happen if `get_next_precedence` got out of sync with this function
                _ => panic!("No infix parser for token {:?}", tok),
            }
        } else if Token::DoubleColon == tok {
            self.parse_pg_cast(expr)
        } else {
            // Can only happen if `get_next_precedence` got out of sync with this function
            panic!("No infix parser for token {:?}", tok)
        }
    }

    /// Parses the parens following the `[ NOT ] IN` operator
    pub fn parse_in(&mut self, expr: Expr, negated: bool) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let in_op = if self.parse_keyword("SELECT") || self.parse_keyword("WITH") {
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
    pub fn parse_between(&mut self, expr: Expr, negated: bool) -> Result<Expr, ParserError> {
        // Stop parsing subexpressions for <low> and <high> on tokens with
        // precedence lower than that of `BETWEEN`, such as `AND`, `IS`, etc.
        let low = self.parse_subexpr(Self::BETWEEN_PREC)?;
        self.expect_keyword("AND")?;
        let high = self.parse_subexpr(Self::BETWEEN_PREC)?;
        Ok(Expr::Between {
            expr: Box::new(expr),
            negated,
            low: Box::new(low),
            high: Box::new(high),
        })
    }

    /// Parse a postgresql casting style which is in the form of `expr::datatype`
    pub fn parse_pg_cast(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        Ok(Expr::Cast {
            expr: Box::new(expr),
            data_type: self.parse_data_type()?,
        })
    }

    const UNARY_NOT_PREC: u8 = 15;
    const BETWEEN_PREC: u8 = 20;

    /// Get the precedence of the next token
    pub fn get_next_precedence(&self) -> Result<u8, ParserError> {
        if let Some(token) = self.peek_token() {
            debug!("get_next_precedence() {:?}", token);

            match &token {
                Token::Word(k) if k.keyword == "OR" => Ok(5),
                Token::Word(k) if k.keyword == "AND" => Ok(10),
                Token::Word(k) if k.keyword == "NOT" => match &self.peek_nth_token(1) {
                    // The precedence of NOT varies depending on keyword that
                    // follows it. If it is followed by IN, BETWEEN, or LIKE,
                    // it takes on the precedence of those tokens. Otherwise it
                    // is not an infix operator, and therefore has zero
                    // precedence.
                    Some(Token::Word(k)) if k.keyword == "IN" => Ok(Self::BETWEEN_PREC),
                    Some(Token::Word(k)) if k.keyword == "BETWEEN" => Ok(Self::BETWEEN_PREC),
                    Some(Token::Word(k)) if k.keyword == "LIKE" => Ok(Self::BETWEEN_PREC),
                    _ => Ok(0),
                },
                Token::Word(k) if k.keyword == "IS" => Ok(17),
                Token::Word(k) if k.keyword == "IN" => Ok(Self::BETWEEN_PREC),
                Token::Word(k) if k.keyword == "BETWEEN" => Ok(Self::BETWEEN_PREC),
                Token::Word(k) if k.keyword == "LIKE" => Ok(Self::BETWEEN_PREC),
                Token::Eq | Token::Lt | Token::LtEq | Token::Neq | Token::Gt | Token::GtEq => {
                    Ok(20)
                }
                Token::Plus | Token::Minus => Ok(30),
                Token::Mult | Token::Div | Token::Mod => Ok(40),
                Token::DoubleColon => Ok(50),
                // TODO(jamii) it's not clear what precedence postgres gives to json operators
                Token::JsonGet
                | Token::JsonGetAsText
                | Token::JsonGetPath
                | Token::JsonGetPathAsText
                | Token::JsonContainsJson
                | Token::JsonContainedInJson
                | Token::JsonContainsField
                | Token::JsonContainsAnyFields
                | Token::JsonContainsAllFields
                | Token::JsonConcat
                | Token::JsonDeletePath
                | Token::JsonContainsPath
                | Token::JsonApplyPathPredicate => Ok(1),
                _ => Ok(0),
            }
        } else {
            Ok(0)
        }
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file)
    pub fn peek_token(&self) -> Option<Token> {
        self.peek_nth_token(0)
    }

    /// Return nth non-whitespace token that has not yet been processed
    pub fn peek_nth_token(&self, mut n: usize) -> Option<Token> {
        let mut index = self.index;
        loop {
            index += 1;
            match self.tokens.get(index - 1) {
                Some((Token::Whitespace(_), _)) => continue,
                non_whitespace => {
                    if n == 0 {
                        return non_whitespace.map(|(token, _range)| token.clone());
                    }
                    n -= 1;
                }
            }
        }
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file) and mark it as processed. OK to call
    /// repeatedly after reaching EOF.
    pub fn next_token(&mut self) -> Option<Token> {
        loop {
            self.index += 1;
            match self.tokens.get(self.index - 1) {
                Some((Token::Whitespace(_), _)) => continue,
                token => return token.map(|(token, _range)| token.clone()),
            }
        }
    }

    /// Return the first unprocessed token, possibly whitespace.
    pub fn next_token_no_skip(&mut self) -> Option<&Token> {
        self.index += 1;
        self.tokens.get(self.index - 1).map(|(token, _range)| token)
    }

    /// Push back the last one non-whitespace token. Must be called after
    /// `next_token()`, otherwise might panic. OK to call after
    /// `next_token()` indicates an EOF.
    pub fn prev_token(&mut self) {
        loop {
            assert!(self.index > 0);
            self.index -= 1;
            if let Some((Token::Whitespace(_), _)) = self.tokens.get(self.index) {
                continue;
            }
            return;
        }
    }

    /// Return the range within the query string at which the next non-whitespace token occurs
    fn peek_range(&self) -> Range<usize> {
        let mut index = self.index;
        loop {
            index += 1;
            match self.tokens.get(index - 1) {
                Some((Token::Whitespace(_), _)) => continue,
                Some((_token, range)) => return range.clone(),
                #[allow(clippy::range_plus_one)]
                None => return self.sql.len()..self.sql.len() + 1,
            }
        }
    }

    /// Return the range within the query string at which the previous non-whitespace token occurs
    ///
    /// Must be called after `next_token()`, otherwise might panic.
    /// OK to call after `next_token()` indicates an EOF.
    fn peek_prev_range(&self) -> Range<usize> {
        let mut index = self.index;
        loop {
            assert!(index > 0);
            index -= 1;
            match self.tokens.get(index) {
                Some((Token::Whitespace(_), _)) => continue,
                Some((_token, range)) => return range.clone(),
                #[allow(clippy::range_plus_one)]
                None => return self.sql.len()..self.sql.len() + 1,
            }
        }
    }

    /// Report unexpected token
    fn expected<T>(
        &self,
        range: Range<usize>,
        expected: &str,
        found: Option<Token>,
    ) -> Result<T, ParserError> {
        parser_err!(
            self,
            range,
            "Expected {}, found: {}",
            expected,
            found.map_or_else(|| "EOF".to_string(), |t| format!("{}", t))
        )
    }

    /// Look for an expected keyword and consume it if it exists
    #[must_use]
    pub fn parse_keyword(&mut self, expected: &'static str) -> bool {
        // Ideally, we'd accept a enum variant, not a string, but since
        // it's not trivial to maintain the enum without duplicating all
        // the keywords three times, we'll settle for a run-time check that
        // the string actually represents a known keyword...
        assert!(keywords::ALL_KEYWORDS.contains(&expected));
        match self.peek_token() {
            Some(Token::Word(ref k)) if expected.eq_ignore_ascii_case(&k.keyword) => {
                self.next_token();
                true
            }
            _ => false,
        }
    }

    /// Look for an expected sequence of keywords and consume them if they exist
    #[must_use]
    pub fn parse_keywords(&mut self, keywords: Vec<&'static str>) -> bool {
        let index = self.index;
        for keyword in keywords {
            if !self.parse_keyword(&keyword) {
                //println!("parse_keywords aborting .. did not find {}", keyword);
                // reset index and return immediately
                self.index = index;
                return false;
            }
        }
        true
    }

    /// Look for one of the given keywords and return the one that matches.
    #[must_use]
    pub fn parse_one_of_keywords(&mut self, keywords: &[&'static str]) -> Option<&'static str> {
        for keyword in keywords {
            assert!(
                keywords::ALL_KEYWORDS.contains(keyword),
                "{} is not contained in keyword list",
                keyword
            );
        }
        match self.peek_token() {
            Some(Token::Word(ref k)) => keywords
                .iter()
                .find(|keyword| keyword.eq_ignore_ascii_case(&k.keyword))
                .map(|keyword| {
                    self.next_token();
                    *keyword
                }),
            _ => None,
        }
    }

    /// Bail out if the current token is not one of the expected keywords, or consume it if it is
    #[must_use = "must match against result to determine what keyword was parsed"]
    pub fn expect_one_of_keywords(
        &mut self,
        keywords: &[&'static str],
    ) -> Result<&'static str, ParserError> {
        if let Some(keyword) = self.parse_one_of_keywords(keywords) {
            Ok(keyword)
        } else {
            self.expected(
                self.peek_range(),
                &format!("one of {}", keywords.join(" or ")),
                self.peek_token(),
            )
        }
    }

    /// Bail out if the current token is not an expected keyword, or consume it if it is
    pub fn expect_keyword(&mut self, expected: &'static str) -> Result<(), ParserError> {
        if self.parse_keyword(expected) {
            Ok(())
        } else {
            self.expected(self.peek_range(), expected, self.peek_token())
        }
    }

    /// Bail out if the following tokens are not the expected sequence of
    /// keywords, or consume them if they are.
    pub fn expect_keywords(&mut self, expected: &[&'static str]) -> Result<(), ParserError> {
        for kw in expected {
            self.expect_keyword(kw)?;
        }
        Ok(())
    }

    /// Consume the next token if it matches the expected token, otherwise return false
    #[must_use]
    pub fn consume_token(&mut self, expected: &Token) -> bool {
        match &self.peek_token() {
            Some(t) if *t == *expected => {
                self.next_token();
                true
            }
            _ => false,
        }
    }

    /// Bail out if the current token is not an expected keyword, or consume it if it is
    pub fn expect_token(&mut self, expected: &Token) -> Result<(), ParserError> {
        if self.consume_token(expected) {
            Ok(())
        } else {
            self.expected(self.peek_range(), &expected.to_string(), self.peek_token())
        }
    }

    /// Parse a comma-separated list of 1+ items accepted by `F`
    pub fn parse_comma_separated<T, F>(&mut self, mut f: F) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser) -> Result<T, ParserError>,
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

    /// Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<Statement, ParserError> {
        if self.parse_keyword("DATABASE") {
            self.parse_create_database()
        } else if self.parse_keyword("SCHEMA") {
            self.parse_create_schema()
        } else if self.parse_keyword("TABLE") {
            self.parse_create_table()
        } else if self.parse_keyword("OR") || self.parse_keyword("VIEW") {
            self.prev_token();
            self.parse_create_view()
        } else if self.parse_keyword("MATERIALIZED") {
            if self.parse_keyword("VIEW") {
                self.prev_token();
                self.prev_token();
                self.parse_create_view()
            } else if self.parse_keyword("SOURCE") {
                self.prev_token();
                self.prev_token();
                self.parse_create_source()
            } else {
                self.expected(
                    self.peek_range(),
                    "VIEW or SOURCE after CREATE MATERIALIZED",
                    self.peek_token(),
                )
            }
        } else if self.parse_keyword("SOURCE") {
            self.prev_token();
            self.parse_create_source()
        } else if self.parse_keyword("SINK") {
            self.parse_create_sink()
        } else if self.parse_keyword("INDEX") {
            self.parse_create_index()
        } else {
            self.expected(
                self.peek_range(),
                "DATABASE, SCHEMA, [MATERIALIZED] VIEW, SOURCE, SINK, or INDEX after CREATE",
                self.peek_token(),
            )
        }
    }

    pub fn parse_create_database(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_identifier()?;
        Ok(Statement::CreateDatabase {
            name,
            if_not_exists,
        })
    }

    pub fn parse_create_schema(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_object_name()?;
        Ok(Statement::CreateSchema {
            name,
            if_not_exists,
        })
    }

    pub fn parse_format(&mut self) -> Result<Format, ParserError> {
        let format = if self.parse_keyword("AVRO") {
            self.expect_keyword("USING")?;
            Format::Avro(self.parse_avro_schema()?)
        } else if self.parse_keyword("PROTOBUF") {
            self.expect_keyword("MESSAGE")?;
            let message_name = self.parse_literal_string()?;
            self.expect_keyword("USING")?;
            let schema = self.parse_schema()?;
            Format::Protobuf {
                message_name,
                schema,
            }
        } else if self.parse_keyword("REGEX") {
            let regex = self.parse_literal_string()?;
            Format::Regex(regex)
        } else if self.parse_keyword("CSV") {
            self.expect_keyword("WITH")?;
            let n_cols = self.parse_literal_uint()? as usize;
            self.expect_keyword("COLUMNS")?;
            let delimiter = if self.parse_keywords(vec!["DELIMITED", "BY"]) {
                let s = self.parse_literal_string()?;
                match s.len() {
                    1 => Ok(s.chars().next().unwrap()),
                    _ => {
                        self.expected(self.peek_range(), "one-character string", self.peek_token())
                    }
                }?
            } else {
                ','
            };
            Format::Csv { n_cols, delimiter }
        } else if self.parse_keyword("JSON") {
            Format::Json
        } else if self.parse_keyword("TEXT") {
            Format::Text
        } else if self.parse_keyword("BYTES") {
            Format::Bytes
        } else {
            return self.expected(
                self.peek_range(),
                "AVRO, PROTOBUF, REGEX, CSV, JSON, TEXT, or BYTES",
                self.peek_token(),
            );
        };
        Ok(format)
    }

    pub fn parse_avro_schema(&mut self) -> Result<AvroSchema, ParserError> {
        let avro_schema = if self.parse_keywords(vec!["CONFLUENT", "SCHEMA", "REGISTRY"]) {
            let url = self.parse_literal_string()?;
            let seed = if self.parse_keyword("SEED") {
                let key_schema = if self.parse_keyword("KEY") {
                    self.expect_keyword("SCHEMA")?;
                    Some(self.parse_literal_string()?)
                } else {
                    None
                };
                self.expect_keywords(&["VALUE", "SCHEMA"])?;
                let value_schema = self.parse_literal_string()?;
                Some(CsrSeed {
                    key_schema,
                    value_schema,
                })
            } else {
                None
            };
            AvroSchema::CsrUrl { url, seed }
        } else if self.parse_keyword("SCHEMA") {
            self.prev_token();
            AvroSchema::Schema(self.parse_schema()?)
        } else {
            return self.expected(
                self.peek_range(),
                "CONFLUENT SCHEMA REGISTRY or SCHEMA",
                self.peek_token(),
            );
        };
        Ok(avro_schema)
    }

    pub fn parse_schema(&mut self) -> Result<Schema, ParserError> {
        self.expect_keyword("SCHEMA")?;
        let schema = if self.parse_keyword("FILE") {
            Schema::File(self.parse_literal_string()?.into())
        } else {
            Schema::Inline(self.parse_literal_string()?)
        };
        Ok(schema)
    }

    pub fn parse_envelope(&mut self) -> Result<Envelope, ParserError> {
        let envelope = if self.parse_keyword("NONE") {
            Envelope::None
        } else if self.parse_keyword("DEBEZIUM") {
            Envelope::Debezium
        } else {
            return self.expected(self.peek_range(), "NONE or DEBEZIUM", self.peek_token());
        };
        Ok(envelope)
    }

    pub fn parse_create_source(&mut self) -> Result<Statement, ParserError> {
        let materialized = self.parse_keyword("MATERIALIZED");
        self.expect_keyword("SOURCE")?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_object_name()?;
        self.expect_keyword("FROM")?;
        let connector = self.parse_connector()?;
        let with_options = self.parse_with_options()?;
        let format = if self.parse_keyword("FORMAT") {
            Some(self.parse_format()?)
        } else {
            None
        };
        let envelope = if self.parse_keyword("ENVELOPE") {
            self.parse_envelope()?
        } else {
            Default::default()
        };

        Ok(Statement::CreateSource {
            name,
            connector,
            with_options,
            format,
            envelope,
            if_not_exists,
            materialized,
        })
    }

    pub fn parse_create_sink(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_object_name()?;
        self.expect_keyword("FROM")?;
        let from = self.parse_object_name()?;
        self.expect_keyword("INTO")?;
        let connector = self.parse_connector()?;
        self.expect_keyword("FORMAT")?;
        let format = self.parse_format()?;
        Ok(Statement::CreateSink {
            name,
            from,
            connector,
            format,
            if_not_exists,
        })
    }

    pub fn parse_connector(&mut self) -> Result<Connector, ParserError> {
        match self.expect_one_of_keywords(&["FILE", "KAFKA", "KINESIS", "AVRO"])? {
            "FILE" => {
                let path = self.parse_literal_string()?;
                Ok(Connector::File { path })
            }
            "KAFKA" => {
                self.expect_keyword("BROKER")?;
                let broker = self.parse_literal_string()?;
                self.expect_keyword("TOPIC")?;
                let topic = self.parse_literal_string()?;
                Ok(Connector::Kafka { broker, topic })
            }
            "KINESIS" => {
                self.expect_keyword("ARN")?;
                let arn = self.parse_literal_string()?;
                Ok(Connector::Kinesis { arn })
            }
            "AVRO" => {
                self.expect_keyword("OCF")?;
                let path = self.parse_literal_string()?;
                Ok(Connector::AvroOcf { path })
            }
            _ => unreachable!(),
        }
    }

    pub fn parse_create_view(&mut self) -> Result<Statement, ParserError> {
        let mut if_exists = if self.parse_keyword("OR") {
            self.expect_keyword("REPLACE")?;
            IfExistsBehavior::Replace
        } else {
            IfExistsBehavior::Error
        };
        let materialized = self.parse_keyword("MATERIALIZED");
        self.expect_keyword("VIEW")?;
        if if_exists == IfExistsBehavior::Error && self.parse_if_not_exists()? {
            if_exists = IfExistsBehavior::Skip;
        }

        // Many dialects support `OR REPLACE` | `OR ALTER` right after `CREATE`, but we don't (yet).
        // ANSI SQL and Postgres support RECURSIVE here, but we don't support it either.
        let name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        let with_options = self.parse_with_options()?;
        self.expect_keyword("AS")?;
        let query = Box::new(self.parse_query()?);
        // Optional `WITH [ CASCADED | LOCAL ] CHECK OPTION` is widely supported here.
        Ok(Statement::CreateView {
            name,
            columns,
            query,
            materialized,
            if_exists,
            with_options,
        })
    }

    pub fn parse_create_index(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_identifier()?;
        self.expect_keyword("ON")?;
        let on_name = self.parse_object_name()?;
        self.expect_token(&Token::LParen)?;
        let key_parts = if self.consume_token(&Token::RParen) {
            vec![]
        } else {
            let key_parts = self
                .parse_comma_separated(Parser::parse_order_by_expr)?
                .into_iter()
                .map(|x| x.expr)
                .collect::<Vec<_>>();
            self.expect_token(&Token::RParen)?;
            key_parts
        };
        Ok(Statement::CreateIndex {
            name,
            on_name,
            key_parts,
            if_not_exists,
        })
    }

    fn parse_if_exists(&mut self) -> Result<bool, ParserError> {
        if self.parse_keyword("IF") {
            self.expect_keyword("EXISTS")?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn parse_if_not_exists(&mut self) -> Result<bool, ParserError> {
        if self.parse_keyword("IF") {
            self.expect_keywords(&["NOT", "EXISTS"])?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn parse_drop(&mut self) -> Result<Statement, ParserError> {
        let object_type = match self.parse_one_of_keywords(&[
            "DATABASE", "SCHEMA", "TABLE", "VIEW", "SOURCE", "SINK", "INDEX",
        ]) {
            Some("DATABASE") => {
                return Ok(Statement::DropDatabase {
                    if_exists: self.parse_if_exists()?,
                    name: self.parse_identifier()?,
                });
            }
            Some("SCHEMA") => ObjectType::Schema,
            Some("TABLE") => ObjectType::Table,
            Some("VIEW") => ObjectType::View,
            Some("SOURCE") => ObjectType::Source,
            Some("SINK") => ObjectType::Sink,
            Some("INDEX") => ObjectType::Index,
            _ => {
                return self.expected(
                    self.peek_range(),
                    "DATABASE, SCHEMA, TABLE, VIEW, SOURCE, SINK, or INDEX after DROP",
                    self.peek_token(),
                )
            }
        };

        let if_exists = self.parse_if_exists()?;
        let names = self.parse_comma_separated(Parser::parse_object_name)?;
        let cascade = self.parse_keyword("CASCADE");
        let cascade_range = self.peek_prev_range();
        let restrict = self.parse_keyword("RESTRICT");
        let restrict_range = self.peek_prev_range();
        if cascade && restrict {
            return parser_err!(
                self,
                cascade_range.start..restrict_range.end,
                "Cannot specify both CASCADE and RESTRICT in DROP"
            );
        }
        Ok(Statement::DropObjects {
            object_type,
            if_exists,
            names,
            cascade,
        })
    }

    pub fn parse_create_table(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_if_not_exists()?;
        let table_name = self.parse_object_name()?;
        // parse optional column list (schema)
        let (columns, constraints) = self.parse_columns()?;
        let with_options = self.parse_with_options()?;

        Ok(Statement::CreateTable {
            name: table_name,
            columns,
            constraints,
            with_options,
            if_not_exists,
        })
    }

    fn parse_columns(&mut self) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>), ParserError> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Some(Token::Word(column_name)) = self.peek_token() {
                self.next_token();
                let data_type = self.parse_data_type()?;
                let collation = if self.parse_keyword("COLLATE") {
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
                    name: column_name.to_ident(),
                    data_type,
                    collation,
                    options,
                });
            } else {
                return self.expected(
                    self.peek_range(),
                    "column name or constraint definition",
                    self.peek_token(),
                );
            }
            let comma = self.consume_token(&Token::Comma);
            if self.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    self.peek_range(),
                    "',' or ')' after column definition",
                    self.peek_token(),
                );
            }
        }

        Ok((columns, constraints))
    }

    pub fn parse_column_option_def(&mut self) -> Result<ColumnOptionDef, ParserError> {
        let name = if self.parse_keyword("CONSTRAINT") {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        let option = if self.parse_keywords(vec!["NOT", "NULL"]) {
            ColumnOption::NotNull
        } else if self.parse_keyword("NULL") {
            ColumnOption::Null
        } else if self.parse_keyword("DEFAULT") {
            ColumnOption::Default(self.parse_expr()?)
        } else if self.parse_keywords(vec!["PRIMARY", "KEY"]) {
            ColumnOption::Unique { is_primary: true }
        } else if self.parse_keyword("UNIQUE") {
            ColumnOption::Unique { is_primary: false }
        } else if self.parse_keyword("REFERENCES") {
            let foreign_table = self.parse_object_name()?;
            let referred_columns = self.parse_parenthesized_column_list(Mandatory)?;
            ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
            }
        } else if self.parse_keyword("CHECK") {
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            ColumnOption::Check(expr)
        } else {
            return self.expected(self.peek_range(), "column option", self.peek_token());
        };

        Ok(ColumnOptionDef { name, option })
    }

    pub fn parse_optional_table_constraint(
        &mut self,
    ) -> Result<Option<TableConstraint>, ParserError> {
        let name = if self.parse_keyword("CONSTRAINT") {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        match self.next_token() {
            Some(Token::Word(ref k)) if k.keyword == "PRIMARY" || k.keyword == "UNIQUE" => {
                let is_primary = k.keyword == "PRIMARY";
                if is_primary {
                    self.expect_keyword("KEY")?;
                }
                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    columns,
                    is_primary,
                }))
            }
            Some(Token::Word(ref k)) if k.keyword == "FOREIGN" => {
                self.expect_keyword("KEY")?;
                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                self.expect_keyword("REFERENCES")?;
                let foreign_table = self.parse_object_name()?;
                let referred_columns = self.parse_parenthesized_column_list(Mandatory)?;
                Ok(Some(TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                }))
            }
            Some(Token::Word(ref k)) if k.keyword == "CHECK" => {
                self.expect_token(&Token::LParen)?;
                let expr = Box::new(self.parse_expr()?);
                self.expect_token(&Token::RParen)?;
                Ok(Some(TableConstraint::Check { name, expr }))
            }
            unexpected => {
                if name.is_some() {
                    self.expected(
                        self.peek_prev_range(),
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

    pub fn parse_with_options(&mut self) -> Result<Vec<SqlOption>, ParserError> {
        if self.parse_keyword("WITH") {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_sql_option)?;
            self.expect_token(&Token::RParen)?;
            Ok(options)
        } else {
            Ok(vec![])
        }
    }

    pub fn parse_sql_option(&mut self) -> Result<SqlOption, ParserError> {
        let name = self.parse_identifier()?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_value()?;
        Ok(SqlOption { name, value })
    }

    pub fn parse_alter(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword("TABLE")?;
        let _ = self.parse_keyword("ONLY");
        let table_name = self.parse_object_name()?;
        let operation = if self.parse_keyword("ADD") {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                AlterTableOperation::AddConstraint(constraint)
            } else {
                return self.expected(
                    self.peek_range(),
                    "a constraint in ALTER TABLE .. ADD",
                    self.peek_token(),
                );
            }
        } else {
            return self.expected(
                self.peek_range(),
                "ADD after ALTER TABLE",
                self.peek_token(),
            );
        };
        Ok(Statement::AlterTable {
            name: table_name,
            operation,
        })
    }

    /// Parse a copy statement
    pub fn parse_copy(&mut self) -> Result<Statement, ParserError> {
        let table_name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        self.expect_keywords(&["FROM", "STDIN"])?;
        self.expect_token(&Token::SemiColon)?;
        let values = self.parse_tsv()?;
        Ok(Statement::Copy {
            table_name,
            columns,
            values,
        })
    }

    /// Parse a tab separated values in
    /// COPY payload
    fn parse_tsv(&mut self) -> Result<Vec<Option<String>>, ParserError> {
        let values = self.parse_tab_value()?;
        Ok(values)
    }

    fn parse_tab_value(&mut self) -> Result<Vec<Option<String>>, ParserError> {
        let mut values = vec![];
        let mut content = String::from("");
        while let Some(t) = self.next_token_no_skip() {
            match t {
                Token::Whitespace(Whitespace::Tab) => {
                    values.push(Some(content.to_string()));
                    content.clear();
                }
                Token::Whitespace(Whitespace::Newline) => {
                    values.push(Some(content.to_string()));
                    content.clear();
                }
                Token::Backslash => {
                    if self.consume_token(&Token::Period) {
                        return Ok(values);
                    }
                    if let Some(token) = self.next_token() {
                        if let Token::Word(Word { value: v, .. }) = token {
                            if v == "N" {
                                values.push(None);
                            }
                        }
                    } else {
                        continue;
                    }
                }
                _ => {
                    content.push_str(&t.to_string());
                }
            }
        }
        Ok(values)
    }

    /// Parse a literal value (numbers, strings, date/time, booleans)
    fn parse_value(&mut self) -> Result<Value, ParserError> {
        match self.next_token() {
            Some(t) => match t {
                Token::Word(k) => match k.keyword.as_ref() {
                    "TRUE" => Ok(Value::Boolean(true)),
                    "FALSE" => Ok(Value::Boolean(false)),
                    "NULL" => Ok(Value::Null),
                    "ARRAY" => self.parse_array(),
                    _ => {
                        return parser_err!(
                            self,
                            self.peek_prev_range(),
                            format!("No value parser for keyword {}", k.keyword)
                        );
                    }
                },
                Token::Number(ref n) => Ok(Value::Number(n.to_string())),
                Token::SingleQuotedString(ref s) => Ok(Value::SingleQuotedString(s.to_string())),
                Token::HexStringLiteral(ref s) => Ok(Value::HexStringLiteral(s.to_string())),
                _ => parser_err!(
                    self,
                    self.peek_prev_range(),
                    format!("Unsupported value: {:?}", t)
                ),
            },
            None => parser_err!(
                self,
                self.peek_prev_range(),
                "Expecting a value, but found EOF"
            ),
        }
    }

    fn parse_array(&mut self) -> Result<Value, ParserError> {
        self.expect_token(&Token::LBracket)?;
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

    pub fn parse_number_value(&mut self) -> Result<Value, ParserError> {
        match self.parse_value()? {
            v @ Value::Number(_) => Ok(v),
            _ => {
                self.prev_token();
                self.expected(self.peek_range(), "literal number", self.peek_token())
            }
        }
    }

    /// Parse an unsigned literal integer/long
    pub fn parse_literal_uint(&mut self) -> Result<u64, ParserError> {
        match self.next_token() {
            Some(Token::Number(s)) => s.parse::<u64>().map_err(|e| {
                self.error(
                    self.peek_prev_range(),
                    format!("Could not parse '{}' as u64: {}", s, e),
                )
            }),
            other => self.expected(self.peek_prev_range(), "literal int", other),
        }
    }

    /// Parse a literal string
    pub fn parse_literal_string(&mut self) -> Result<String, ParserError> {
        match self.next_token() {
            Some(Token::SingleQuotedString(ref s)) => Ok(s.clone()),
            other => self.expected(self.peek_prev_range(), "literal string", other),
        }
    }

    /// Parse a SQL datatype (in the context of a CREATE TABLE statement for example)
    pub fn parse_data_type(&mut self) -> Result<DataType, ParserError> {
        let mut data_type = match self.next_token() {
            Some(Token::Word(k)) => match k.keyword.as_ref() {
                "BOOL" | "BOOLEAN" => DataType::Boolean,
                "FLOAT" | "FLOAT4" => DataType::Float(self.parse_optional_precision()?),
                "REAL" => DataType::Real,
                "DOUBLE" | "FLOAT8" => {
                    let _ = self.parse_keyword("PRECISION");
                    DataType::Double
                }
                "SMALLINT" => DataType::SmallInt,
                "INT" | "INTEGER" | "INT4" => DataType::Int,
                "INT8" | "BIGINT" => DataType::BigInt,
                "VARCHAR" => DataType::Varchar(self.parse_optional_precision()?),
                "CHAR" | "CHARACTER" => {
                    if self.parse_keyword("VARYING") {
                        DataType::Varchar(self.parse_optional_precision()?)
                    } else {
                        DataType::Char(self.parse_optional_precision()?)
                    }
                }
                "UUID" => DataType::Uuid,
                "DATE" => DataType::Date,
                "TIMESTAMP" => {
                    if self.parse_keyword("WITH") {
                        self.expect_keywords(&["TIME", "ZONE"])?;
                        DataType::TimestampTz
                    } else {
                        if self.parse_keyword("WITHOUT") {
                            self.expect_keywords(&["TIME", "ZONE"])?;
                        }
                        DataType::Timestamp
                    }
                }
                "TIMESTAMPTZ" => DataType::TimestampTz,
                "TIME" => {
                    if self.parse_keyword("WITH") {
                        self.expect_keywords(&["TIME", "ZONE"])?;
                        DataType::TimeTz
                    } else {
                        if self.parse_keyword("WITHOUT") {
                            self.expect_keywords(&["TIME", "ZONE"])?;
                        }
                        DataType::Time
                    }
                }
                // Interval types can be followed by a complicated interval
                // qualifier that we don't currently support. See
                // parse_interval_literal for a taste.
                "INTERVAL" => DataType::Interval,
                "REGCLASS" => DataType::Regclass,
                "TEXT" | "STRING" => DataType::Text,
                "BYTEA" => DataType::Bytea,
                "NUMERIC" | "DECIMAL" | "DEC" => {
                    let (precision, scale) = self.parse_optional_precision_scale()?;
                    DataType::Decimal(precision, scale)
                }
                "JSON" | "JSONB" => DataType::Jsonb,
                _ => self.expected(
                    self.peek_prev_range(),
                    "a known data type",
                    Some(Token::Word(k)),
                )?,
            },
            other => self.expected(self.peek_prev_range(), "a data type name", other)?,
        };
        match &self.peek_token() {
            Some(Token::LBracket) => {
                while self.consume_token(&Token::LBracket) {
                    // Note: this is postgresql-specific
                    self.expect_token(&Token::RBracket)?;
                    data_type = DataType::Array(Box::new(data_type));
                }
            }
            Some(Token::Word(k)) if &k.keyword == "ARRAY" => {
                self.next_token();
                data_type = DataType::Array(Box::new(data_type));
            }
            _ => (),
        }
        Ok(data_type)
    }

    /// Parse `AS identifier` (or simply `identifier` if it's not a reserved keyword)
    /// Some examples with aliases: `SELECT 1 foo`, `SELECT COUNT(*) AS cnt`,
    /// `SELECT ... FROM t1 foo, t2 bar`, `SELECT ... FROM (...) AS bar`
    pub fn parse_optional_alias(
        &mut self,
        reserved_kwds: &[&str],
    ) -> Result<Option<Ident>, ParserError> {
        let after_as = self.parse_keyword("AS");
        match self.next_token() {
            // Accept any identifier after `AS` (though many dialects have restrictions on
            // keywords that may appear here). If there's no `AS`: don't parse keywords,
            // which may start a construct allowed in this position, to be parsed as aliases.
            // (For example, in `FROM t1 JOIN` the `JOIN` will always be parsed as a keyword,
            // not an alias.)
            Some(Token::Word(ref w))
                if after_as || !reserved_kwds.contains(&w.keyword.as_str()) =>
            {
                Ok(Some(w.to_ident()))
            }
            not_an_ident => {
                if after_as {
                    return self.expected(
                        self.peek_prev_range(),
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
    pub fn parse_optional_table_alias(
        &mut self,
        reserved_kwds: &[&str],
    ) -> Result<Option<TableAlias>, ParserError> {
        match self.parse_optional_alias(reserved_kwds)? {
            Some(name) => {
                let columns = self.parse_parenthesized_column_list(Optional)?;
                Ok(Some(TableAlias { name, columns }))
            }
            None => Ok(None),
        }
    }

    /// Parse a possibly qualified, possibly quoted identifier, e.g.
    /// `foo` or `myschema."table"`
    pub fn parse_object_name(&mut self) -> Result<ObjectName, ParserError> {
        let mut idents = vec![];
        loop {
            idents.push(self.parse_identifier()?);
            if !self.consume_token(&Token::Period) {
                break;
            }
        }
        Ok(ObjectName(idents))
    }

    /// Parse a simple one-word identifier (possibly quoted, possibly a keyword)
    pub fn parse_identifier(&mut self) -> Result<Ident, ParserError> {
        match self.next_token() {
            Some(Token::Word(w)) => Ok(w.to_ident()),
            unexpected => self.expected(self.peek_prev_range(), "identifier", unexpected),
        }
    }

    /// Parse a parenthesized comma-separated list of unqualified, possibly quoted identifiers
    pub fn parse_parenthesized_column_list(
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
                self.peek_range(),
                "a list of columns in parentheses",
                self.peek_token(),
            )
        }
    }

    pub fn parse_optional_precision(&mut self) -> Result<Option<u64>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let n = self.parse_literal_uint()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(n))
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_precision_scale(
        &mut self,
    ) -> Result<(Option<u64>, Option<u64>), ParserError> {
        if self.consume_token(&Token::LParen) {
            let n = self.parse_literal_uint()?;
            let scale = if self.consume_token(&Token::Comma) {
                Some(self.parse_literal_uint()?)
            } else {
                None
            };
            self.expect_token(&Token::RParen)?;
            Ok((Some(n), scale))
        } else {
            Ok((None, None))
        }
    }

    pub fn parse_delete(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword("FROM")?;
        let table_name = self.parse_object_name()?;
        let selection = if self.parse_keyword("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Delete {
            table_name,
            selection,
        })
    }

    /// Parse a query expression, i.e. a `SELECT` statement optionally
    /// preceeded with some `WITH` CTE declarations and optionally followed
    /// by `ORDER BY`. Unlike some other parse_... methods, this one doesn't
    /// expect the initial keyword to be already consumed
    pub fn parse_query(&mut self) -> Result<Query, ParserError> {
        let ctes = if self.parse_keyword("WITH") {
            // TODO: optional RECURSIVE
            self.parse_comma_separated(Parser::parse_cte)?
        } else {
            vec![]
        };

        let body = self.parse_query_body(0)?;

        let order_by = if self.parse_keywords(vec!["ORDER", "BY"]) {
            self.parse_comma_separated(Parser::parse_order_by_expr)?
        } else {
            vec![]
        };

        let limit = if self.parse_keyword("LIMIT") {
            self.parse_limit()?
        } else {
            None
        };

        let offset = if self.parse_keyword("OFFSET") {
            Some(self.parse_offset()?)
        } else {
            None
        };

        let fetch = if self.parse_keyword("FETCH") {
            Some(self.parse_fetch()?)
        } else {
            None
        };

        Ok(Query {
            ctes,
            body,
            limit,
            order_by,
            offset,
            fetch,
        })
    }

    /// Parse a CTE (`alias [( col1, col2, ... )] AS (subquery)`)
    fn parse_cte(&mut self) -> Result<Cte, ParserError> {
        let alias = TableAlias {
            name: self.parse_identifier()?,
            columns: self.parse_parenthesized_column_list(Optional)?,
        };
        self.expect_keyword("AS")?;
        self.expect_token(&Token::LParen)?;
        let query = self.parse_query()?;
        self.expect_token(&Token::RParen)?;
        Ok(Cte { alias, query })
    }

    /// Parse a "query body", which is an expression with roughly the
    /// following grammar:
    /// ```text
    ///   query_body ::= restricted_select | '(' subquery ')' | set_operation
    ///   restricted_select ::= 'SELECT' [expr_list] [ from ] [ where ] [ groupby_having ]
    ///   subquery ::= query_body [ order_by_limit ]
    ///   set_operation ::= query_body { 'UNION' | 'EXCEPT' | 'INTERSECT' } [ 'ALL' ] query_body
    /// ```
    fn parse_query_body(&mut self, precedence: u8) -> Result<SetExpr, ParserError> {
        // We parse the expression using a Pratt parser, as in `parse_expr()`.
        // Start by parsing a restricted SELECT or a `(subquery)`:
        let mut expr = if self.parse_keyword("SELECT") {
            SetExpr::Select(Box::new(self.parse_select()?))
        } else if self.consume_token(&Token::LParen) {
            // CTEs are not allowed here, but the parser currently accepts them
            let subquery = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            SetExpr::Query(Box::new(subquery))
        } else if self.parse_keyword("VALUES") {
            SetExpr::Values(self.parse_values()?)
        } else {
            return self.expected(
                self.peek_range(),
                "SELECT, VALUES, or a subquery in the query body",
                self.peek_token(),
            );
        };

        loop {
            // The query can be optionally followed by a set operator:
            let next_token = self.peek_token();
            let op = self.parse_set_operator(&next_token);
            let next_precedence = match op {
                // UNION and EXCEPT have the same binding power and evaluate left-to-right
                Some(SetOperator::Union) | Some(SetOperator::Except) => 10,
                // INTERSECT has higher precedence than UNION/EXCEPT
                Some(SetOperator::Intersect) => 20,
                // Unexpected token or EOF => stop parsing the query body
                None => break,
            };
            if precedence >= next_precedence {
                break;
            }
            self.next_token(); // skip past the set operator

            let all = self.parse_keyword("ALL");
            let all_range = self.peek_prev_range();
            let distinct = self.parse_keyword("DISTINCT");
            let distinct_range = self.peek_prev_range();
            if all && distinct {
                return parser_err!(
                    self,
                    all_range.start..distinct_range.end,
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
            Some(Token::Word(w)) if w.keyword == "UNION" => Some(SetOperator::Union),
            Some(Token::Word(w)) if w.keyword == "EXCEPT" => Some(SetOperator::Except),
            Some(Token::Word(w)) if w.keyword == "INTERSECT" => Some(SetOperator::Intersect),
            _ => None,
        }
    }

    /// Parse a restricted `SELECT` statement (no CTEs / `UNION` / `ORDER BY`),
    /// assuming the initial `SELECT` was already consumed
    pub fn parse_select(&mut self) -> Result<Select, ParserError> {
        let all = self.parse_keyword("ALL");
        let all_range = self.peek_prev_range();
        let distinct = self.parse_keyword("DISTINCT");
        let distinct_range = self.peek_prev_range();
        if all && distinct {
            return parser_err!(
                self,
                all_range.start..distinct_range.end,
                "Cannot specify both ALL and DISTINCT in SELECT"
            );
        }
        let projection = self.parse_comma_separated(Parser::parse_select_item)?;

        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_COLUMN_ALIAS` / `RESERVED_FOR_TABLE_ALIAS`,
        // otherwise they may be parsed as an alias as part of the `projection`
        // or `from`.

        let from = if self.parse_keyword("FROM") {
            self.parse_comma_separated(Parser::parse_table_and_joins)?
        } else {
            vec![]
        };

        let selection = if self.parse_keyword("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.parse_keywords(vec!["GROUP", "BY"]) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![]
        };

        let having = if self.parse_keyword("HAVING") {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Select {
            distinct,
            projection,
            from,
            selection,
            group_by,
            having,
        })
    }

    pub fn parse_set(&mut self) -> Result<Statement, ParserError> {
        let modifier = self.parse_one_of_keywords(&["SESSION", "LOCAL"]);
        let variable = self.parse_identifier()?;
        if self.consume_token(&Token::Eq) || self.parse_keyword("TO") {
            let token = self.peek_token();
            let value = match (self.parse_value(), token) {
                (Ok(value), _) => SetVariableValue::Literal(value),
                (Err(_), Some(Token::Word(ident))) => SetVariableValue::Ident(ident.to_ident()),
                (Err(_), other) => self.expected(self.peek_range(), "variable value", other)?,
            };
            Ok(Statement::SetVariable {
                local: modifier == Some("LOCAL"),
                variable,
                value,
            })
        } else if variable.value == "TRANSACTION" && modifier.is_none() {
            Ok(Statement::SetTransaction {
                modes: self.parse_transaction_modes()?,
            })
        } else {
            self.expected(self.peek_range(), "equals sign or TO", self.peek_token())
        }
    }

    pub fn parse_show(&mut self) -> Result<Statement, ParserError> {
        if self.parse_keyword("DATABASES") {
            return Ok(Statement::ShowDatabases {
                filter: self.parse_show_statement_filter()?,
            });
        }

        let extended = self.parse_keyword("EXTENDED");
        if extended {
            self.expect_one_of_keywords(&[
                "SCHEMAS", "INDEX", "INDEXES", "KEYS", "TABLES", "COLUMNS", "FULL",
            ])?;
            self.prev_token();
        }

        let full = self.parse_keyword("FULL");
        if full {
            if extended {
                self.expect_one_of_keywords(&["SCHEMAS", "COLUMNS", "TABLES"])?;
            } else {
                self.expect_one_of_keywords(&[
                    "SCHEMAS",
                    "COLUMNS",
                    "TABLES",
                    "VIEWS",
                    "SINKS",
                    "SOURCES",
                    "MATERIALIZED",
                ])?;
            }
            self.prev_token();
        }

        let materialized = self.parse_keyword("MATERIALIZED");
        if materialized {
            self.expect_one_of_keywords(&["SOURCES", "VIEWS"])?;
            self.prev_token();
        }

        if self.parse_one_of_keywords(&["COLUMNS", "FIELDS"]).is_some() {
            self.parse_show_columns(extended, full)
        } else if let Some(object_type) =
            self.parse_one_of_keywords(&["SCHEMAS", "SOURCES", "VIEWS", "SINKS", "TABLES"])
        {
            Ok(Statement::ShowObjects {
                object_type: match object_type {
                    "SCHEMAS" => ObjectType::Schema,
                    "SOURCES" => ObjectType::Source,
                    "VIEWS" => ObjectType::View,
                    "SINKS" => ObjectType::Sink,
                    "TABLES" => ObjectType::Table,
                    val => panic!(
                        "`parse_one_of_keywords` returned an impossible value: {}",
                        val
                    ),
                },
                extended,
                full,
                materialized,
                from: if self.parse_one_of_keywords(&["FROM", "IN"]).is_some() {
                    Some(self.parse_object_name()?)
                } else {
                    None
                },
                filter: self.parse_show_statement_filter()?,
            })
        } else if self
            .parse_one_of_keywords(&["INDEX", "INDEXES", "KEYS"])
            .is_some()
        {
            match self.parse_one_of_keywords(&["FROM", "IN"]) {
                Some(_) => {
                    let table_name = self.parse_object_name()?;
                    let filter = if self.parse_keyword("WHERE") {
                        Some(ShowStatementFilter::Where(self.parse_expr()?))
                    } else {
                        None
                    };
                    Ok(Statement::ShowIndexes {
                        table_name,
                        extended,
                        filter,
                    })
                }
                None => self.expected(
                    self.peek_range(),
                    "FROM or IN after SHOW INDEXES",
                    self.peek_token(),
                ),
            }
        } else if self.parse_keywords(vec!["CREATE", "VIEW"]) {
            Ok(Statement::ShowCreateView {
                view_name: self.parse_object_name()?,
            })
        } else if self.parse_keywords(vec!["CREATE", "SOURCE"]) {
            Ok(Statement::ShowCreateSource {
                source_name: self.parse_object_name()?,
            })
        } else if self.parse_keywords(vec!["CREATE", "SINK"]) {
            Ok(Statement::ShowCreateSink {
                sink_name: self.parse_object_name()?,
            })
        } else {
            Ok(Statement::ShowVariable {
                variable: self.parse_identifier()?,
            })
        }
    }

    fn parse_show_columns(&mut self, extended: bool, full: bool) -> Result<Statement, ParserError> {
        self.expect_one_of_keywords(&["FROM", "IN"])?;
        let table_name = self.parse_object_name()?;
        // MySQL also supports FROM <database> here. In other words, MySQL
        // allows both FROM <table> FROM <database> and FROM <database>.<table>,
        // while we only support the latter for now.
        let filter = self.parse_show_statement_filter()?;
        Ok(Statement::ShowColumns {
            extended,
            full,
            table_name,
            filter,
        })
    }

    fn parse_show_statement_filter(&mut self) -> Result<Option<ShowStatementFilter>, ParserError> {
        if self.parse_keyword("LIKE") {
            Ok(Some(ShowStatementFilter::Like(
                self.parse_literal_string()?,
            )))
        } else if self.parse_keyword("WHERE") {
            Ok(Some(ShowStatementFilter::Where(self.parse_expr()?)))
        } else {
            Ok(None)
        }
    }

    pub fn parse_table_and_joins(&mut self) -> Result<TableWithJoins, ParserError> {
        let relation = self.parse_table_factor()?;

        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_TABLE_ALIAS`, otherwise they may be parsed as
        // a table alias.
        let mut joins = vec![];
        loop {
            let join = if self.parse_keyword("CROSS") {
                self.expect_keyword("JOIN")?;
                Join {
                    relation: self.parse_table_factor()?,
                    join_operator: JoinOperator::CrossJoin,
                }
            } else {
                let natural = self.parse_keyword("NATURAL");
                let peek_keyword = if let Some(Token::Word(kw)) = self.peek_token() {
                    kw.keyword
                } else {
                    String::default()
                };

                let join_operator_type = match peek_keyword.as_ref() {
                    "INNER" | "JOIN" => {
                        let _ = self.parse_keyword("INNER");
                        self.expect_keyword("JOIN")?;
                        JoinOperator::Inner
                    }
                    kw @ "LEFT" | kw @ "RIGHT" | kw @ "FULL" => {
                        let _ = self.next_token();
                        let _ = self.parse_keyword("OUTER");
                        self.expect_keyword("JOIN")?;
                        match kw {
                            "LEFT" => JoinOperator::LeftOuter,
                            "RIGHT" => JoinOperator::RightOuter,
                            "FULL" => JoinOperator::FullOuter,
                            _ => unreachable!(),
                        }
                    }
                    "OUTER" => {
                        return self.expected(
                            self.peek_range(),
                            "LEFT, RIGHT, or FULL",
                            self.peek_token(),
                        )
                    }
                    _ if natural => {
                        return self.expected(
                            self.peek_range(),
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
    pub fn parse_table_factor(&mut self) -> Result<TableFactor, ParserError> {
        if self.parse_keyword("LATERAL") {
            // LATERAL must always be followed by a subquery.
            if !self.consume_token(&Token::LParen) {
                self.expected(
                    self.peek_range(),
                    "subquery after LATERAL",
                    self.peek_token(),
                )?;
            }
            return self.parse_derived_table_factor(Lateral);
        }

        if self.consume_token(&Token::LParen) {
            let index = self.index;
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
            match self.parse_derived_table_factor(NotLateral) {
                // The recently consumed '(' started a derived table, and we've
                // parsed the subquery, followed by the closing ')', and the
                // alias of the derived table. In the example above this is
                // case (3), and the next token would be `NATURAL`.
                Ok(table_factor) => Ok(table_factor),
                Err(_) => {
                    // The '(' we've recently consumed does not start a derived
                    // table. For valid input this can happen either when the
                    // token following the paren can't start a query (e.g. `foo`
                    // in `FROM (foo NATURAL JOIN bar)`, or when the '(' we've
                    // consumed is followed by another '(' that starts a
                    // derived table, like (3), or another nested join (2).
                    //
                    // Ignore the error and back up to where we were before.
                    // Either we'll be able to parse a valid nested join, or
                    // we won't, and we'll return that error instead.
                    self.index = index;
                    let table_and_joins = self.parse_table_and_joins()?;
                    match table_and_joins.relation {
                        TableFactor::NestedJoin { .. } => (),
                        _ => {
                            if table_and_joins.joins.is_empty() {
                                // The SQL spec prohibits derived tables and bare
                                // tables from appearing alone in parentheses.
                                self.expected(self.peek_range(), "joined table", self.peek_token())?
                            }
                        }
                    }
                    self.expect_token(&Token::RParen)?;
                    Ok(TableFactor::NestedJoin(Box::new(table_and_joins)))
                }
            }
        } else {
            let name = self.parse_object_name()?;
            // Postgres, MSSQL: table-valued functions:
            let args = if self.consume_token(&Token::LParen) {
                self.parse_optional_args()?
            } else {
                vec![]
            };
            let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
            // MSSQL-specific table hints:
            let mut with_hints = vec![];
            if self.parse_keyword("WITH") {
                if self.consume_token(&Token::LParen) {
                    with_hints = self.parse_comma_separated(Parser::parse_expr)?;
                    self.expect_token(&Token::RParen)?;
                } else {
                    // rewind, as WITH may belong to the next statement's CTE
                    self.prev_token();
                }
            };
            Ok(TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
            })
        }
    }

    pub fn parse_derived_table_factor(
        &mut self,
        lateral: IsLateral,
    ) -> Result<TableFactor, ParserError> {
        let subquery = Box::new(self.parse_query()?);
        self.expect_token(&Token::RParen)?;
        let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
        Ok(TableFactor::Derived {
            lateral: match lateral {
                Lateral => true,
                NotLateral => false,
            },
            subquery,
            alias,
        })
    }

    fn parse_join_constraint(&mut self, natural: bool) -> Result<JoinConstraint, ParserError> {
        if natural {
            Ok(JoinConstraint::Natural)
        } else if self.parse_keyword("ON") {
            let constraint = self.parse_expr()?;
            Ok(JoinConstraint::On(constraint))
        } else if self.parse_keyword("USING") {
            let columns = self.parse_parenthesized_column_list(Mandatory)?;
            Ok(JoinConstraint::Using(columns))
        } else {
            self.expected(
                self.peek_range(),
                "ON, or USING after JOIN",
                self.peek_token(),
            )
        }
    }

    /// Parse an INSERT statement
    pub fn parse_insert(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword("INTO")?;
        let table_name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        let source = Box::new(self.parse_query()?);
        Ok(Statement::Insert {
            table_name,
            columns,
            source,
        })
    }

    pub fn parse_update(&mut self) -> Result<Statement, ParserError> {
        let table_name = self.parse_object_name()?;
        self.expect_keyword("SET")?;
        let assignments = self.parse_comma_separated(Parser::parse_assignment)?;
        let selection = if self.parse_keyword("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Update {
            table_name,
            assignments,
            selection,
        })
    }

    /// Parse a `var = expr` assignment, used in an UPDATE statement
    pub fn parse_assignment(&mut self) -> Result<Assignment, ParserError> {
        let id = self.parse_identifier()?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_expr()?;
        Ok(Assignment { id, value })
    }

    pub fn parse_optional_args(&mut self) -> Result<Vec<Expr>, ParserError> {
        if self.consume_token(&Token::RParen) {
            Ok(vec![])
        } else {
            let args = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
            Ok(args)
        }
    }

    /// Parse a comma-delimited list of projections after SELECT
    pub fn parse_select_item(&mut self) -> Result<SelectItem, ParserError> {
        let expr = self.parse_expr()?;
        if let Expr::Wildcard = expr {
            Ok(SelectItem::Wildcard)
        } else if let Expr::QualifiedWildcard(prefix) = expr {
            Ok(SelectItem::QualifiedWildcard(ObjectName(prefix)))
        } else {
            // `expr` is a regular SQL expression and can be followed by an alias
            if let Some(alias) = self.parse_optional_alias(keywords::RESERVED_FOR_COLUMN_ALIAS)? {
                Ok(SelectItem::ExprWithAlias { expr, alias })
            } else {
                Ok(SelectItem::UnnamedExpr(expr))
            }
        }
    }

    /// Parse an expression, optionally followed by ASC or DESC (used in ORDER BY)
    pub fn parse_order_by_expr(&mut self) -> Result<OrderByExpr, ParserError> {
        let expr = self.parse_expr()?;

        let asc = if self.parse_keyword("ASC") {
            Some(true)
        } else if self.parse_keyword("DESC") {
            Some(false)
        } else {
            None
        };
        Ok(OrderByExpr { expr, asc })
    }

    /// Parse a LIMIT clause
    pub fn parse_limit(&mut self) -> Result<Option<Expr>, ParserError> {
        if self.parse_keyword("ALL") {
            Ok(None)
        } else {
            Ok(Some(Expr::Value(self.parse_number_value()?)))
        }
    }

    /// Parse an OFFSET clause
    pub fn parse_offset(&mut self) -> Result<Expr, ParserError> {
        let value = Expr::Value(self.parse_number_value()?);
        let _ = self.parse_one_of_keywords(&["ROW", "ROWS"]);
        Ok(value)
    }

    /// Parse a FETCH clause
    pub fn parse_fetch(&mut self) -> Result<Fetch, ParserError> {
        self.expect_one_of_keywords(&["FIRST", "NEXT"])?;
        let (quantity, percent) = if self.parse_one_of_keywords(&["ROW", "ROWS"]).is_some() {
            (None, false)
        } else {
            let quantity = Expr::Value(self.parse_value()?);
            let percent = self.parse_keyword("PERCENT");
            self.expect_one_of_keywords(&["ROW", "ROWS"])?;
            (Some(quantity), percent)
        };
        let with_ties = if self.parse_keyword("ONLY") {
            false
        } else if self.parse_keywords(vec!["WITH", "TIES"]) {
            true
        } else {
            return self.expected(
                self.peek_range(),
                "one of ONLY or WITH TIES",
                self.peek_token(),
            );
        };
        Ok(Fetch {
            with_ties,
            percent,
            quantity,
        })
    }

    pub fn parse_values(&mut self) -> Result<Values, ParserError> {
        let values = self.parse_comma_separated(|parser| {
            parser.expect_token(&Token::LParen)?;
            let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
            parser.expect_token(&Token::RParen)?;
            Ok(exprs)
        })?;
        Ok(Values(values))
    }

    pub fn parse_start_transaction(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword("TRANSACTION")?;
        Ok(Statement::StartTransaction {
            modes: self.parse_transaction_modes()?,
        })
    }

    pub fn parse_begin(&mut self) -> Result<Statement, ParserError> {
        let _ = self.parse_one_of_keywords(&["TRANSACTION", "WORK"]);
        Ok(Statement::StartTransaction {
            modes: self.parse_transaction_modes()?,
        })
    }

    pub fn parse_transaction_modes(&mut self) -> Result<Vec<TransactionMode>, ParserError> {
        let mut modes = vec![];
        let mut required = false;
        loop {
            let mode = if self.parse_keywords(vec!["ISOLATION", "LEVEL"]) {
                let iso_level = if self.parse_keywords(vec!["READ", "UNCOMMITTED"]) {
                    TransactionIsolationLevel::ReadUncommitted
                } else if self.parse_keywords(vec!["READ", "COMMITTED"]) {
                    TransactionIsolationLevel::ReadCommitted
                } else if self.parse_keywords(vec!["REPEATABLE", "READ"]) {
                    TransactionIsolationLevel::RepeatableRead
                } else if self.parse_keyword("SERIALIZABLE") {
                    TransactionIsolationLevel::Serializable
                } else {
                    self.expected(self.peek_range(), "isolation level", self.peek_token())?
                };
                TransactionMode::IsolationLevel(iso_level)
            } else if self.parse_keywords(vec!["READ", "ONLY"]) {
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly)
            } else if self.parse_keywords(vec!["READ", "WRITE"]) {
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite)
            } else if required || self.peek_token().is_some() {
                self.expected(self.peek_range(), "transaction mode", self.peek_token())?
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

    pub fn parse_commit(&mut self) -> Result<Statement, ParserError> {
        Ok(Statement::Commit {
            chain: self.parse_commit_rollback_chain()?,
        })
    }

    pub fn parse_rollback(&mut self) -> Result<Statement, ParserError> {
        Ok(Statement::Rollback {
            chain: self.parse_commit_rollback_chain()?,
        })
    }

    pub fn parse_commit_rollback_chain(&mut self) -> Result<bool, ParserError> {
        let _ = self.parse_one_of_keywords(&["TRANSACTION", "WORK"]);
        if self.parse_keyword("AND") {
            let chain = !self.parse_keyword("NO");
            self.expect_keyword("CHAIN")?;
            Ok(chain)
        } else {
            Ok(false)
        }
    }

    /// Parse an `EXPLAIN [DATAFLOW | PLAN] FOR` statement, assuming that the `EXPLAIN` token
    /// has already been consumed.
    pub fn parse_explain(&mut self) -> Result<Statement, ParserError> {
        let stage = if self.parse_keyword("DATAFLOW") {
            Stage::Dataflow
        } else if self.parse_keyword("PLAN") {
            Stage::Plan
        } else {
            self.expected(self.peek_range(), "DATAFLOW or PLAN", self.peek_token())?
        };
        self.expect_keyword("FOR")?;

        Ok(Statement::Explain {
            stage,
            query: Box::new(self.parse_query()?),
        })
    }
}

impl Word {
    pub fn to_ident(&self) -> Ident {
        Ident {
            value: self.value.clone(),
            quote_style: self.quote_style,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prev_index() {
        let sql = "SELECT version";
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(sql.to_string(), tokens);
        assert_eq!(parser.peek_token(), Some(Token::make_keyword("SELECT")));
        assert_eq!(parser.next_token(), Some(Token::make_keyword("SELECT")));
        parser.prev_token();
        assert_eq!(parser.next_token(), Some(Token::make_keyword("SELECT")));
        assert_eq!(parser.next_token(), Some(Token::make_word("version", None)));
        parser.prev_token();
        assert_eq!(parser.peek_token(), Some(Token::make_word("version", None)));
        assert_eq!(parser.next_token(), Some(Token::make_word("version", None)));
        assert_eq!(parser.peek_token(), None);
        parser.prev_token();
        assert_eq!(parser.next_token(), Some(Token::make_word("version", None)));
        assert_eq!(parser.next_token(), None);
        assert_eq!(parser.next_token(), None);
        parser.prev_token();
    }
}
