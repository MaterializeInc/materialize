// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright 2019 Materialize, Inc. All rights reserved.
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

use std::fmt::Debug;

use super::ast::*;
use super::dialect::*;
use super::parser::{Parser, ParserError};
use super::tokenizer::Tokenizer;

/// Tests use the methods on this struct to invoke the parser on one or
/// multiple dialects.
pub struct TestedDialects {
    pub dialects: Vec<Box<dyn Dialect>>,
}

impl TestedDialects {
    /// Run the given function for all of `self.dialects`, assert that they
    /// return the same result, and return that result.
    pub fn one_of_identical_results<F, T: Debug + PartialEq>(&self, f: F) -> T
    where
        F: Fn(&dyn Dialect) -> T,
    {
        let parse_results = self.dialects.iter().map(|dialect| (dialect, f(&**dialect)));
        parse_results
            .fold(None, |s, (dialect, parsed)| {
                if let Some((prev_dialect, prev_parsed)) = s {
                    assert_eq!(
                        prev_parsed, parsed,
                        "Parse results with {:?} are different from {:?}",
                        prev_dialect, dialect
                    );
                }
                Some((dialect, parsed))
            })
            .unwrap()
            .1
    }

    pub fn run_parser_method<F, T: Debug + PartialEq>(&self, sql: &str, f: F) -> T
    where
        F: Fn(&mut Parser) -> T,
    {
        self.one_of_identical_results(|dialect| {
            let mut tokenizer = Tokenizer::new(dialect, sql);
            let tokens = tokenizer.tokenize().unwrap();
            f(&mut Parser::new(tokens))
        })
    }

    pub fn parse_sql_statements(&self, sql: &str) -> Result<Vec<Statement>, ParserError> {
        self.one_of_identical_results(|dialect| Parser::parse_sql(dialect, sql.to_string()))
        // To fail the `ensure_multiple_dialects_are_tested` test:
        // Parser::parse_sql(&**self.dialects.first().unwrap(), sql.to_string())
    }

    /// Ensures that `sql` parses as a single statement, optionally checking
    /// that converting AST back to string equals to `canonical` (unless an
    /// empty canonical string is provided).
    pub fn one_statement_parses_to(&self, sql: &str, canonical: &str) -> Statement {
        let mut statements = self.parse_sql_statements(&sql).unwrap();
        assert_eq!(statements.len(), 1);

        let only_statement = statements.pop().unwrap();
        if !canonical.is_empty() {
            assert_eq!(canonical, only_statement.to_string())
        }
        only_statement
    }

    /// Ensures that `sql` parses as a single [Statement], and is not modified
    /// after a serialization round-trip.
    pub fn verified_stmt(&self, query: &str) -> Statement {
        self.one_statement_parses_to(query, query)
    }

    /// Parse `sql` to a single [Statement] without checking for modifications
    pub fn unverified_stmt(&self, query: &str) -> Statement {
        self.one_statement_parses_to(query, "")
    }

    /// Ensures that `sql` parses as a single [Query], and is not modified
    /// after a serialization round-trip.
    pub fn verified_query(&self, sql: &str) -> Query {
        match self.verified_stmt(sql) {
            Statement::Query(query) => *query,
            _ => panic!("Expected Query"),
        }
    }

    /// Ensures that `sql` parses as a single [Query]
    /// Does not check for modifications
    pub fn unverified_query(&self, sql: &str) -> Query {
        match self.unverified_stmt(sql) {
            Statement::Query(query) => *query,
            _ => panic!("Expected Query"),
        }
    }

    /// Ensures that `sql` parses as a single [Select], and is not modified
    /// after a serialization round-trip.
    pub fn verified_only_select(&self, query: &str) -> Select {
        match self.verified_query(query).body {
            SetExpr::Select(s) => *s,
            _ => panic!("Expected SetExpr::Select"),
        }
    }

    /// Ensures that `sql` parses as a single [Select]
    pub fn unverified_only_select(&self, query: &str) -> Select {
        match self.unverified_query(query).body {
            SetExpr::Select(s) => *s,
            _ => panic!("Expected SetExpr::Select"),
        }
    }

    /// Ensures that `sql` parses as an expression, and is not modified
    /// after a serialization round-trip.
    pub fn verified_expr(&self, sql: &str) -> Expr {
        let ast = self.run_parser_method(sql, Parser::parse_expr).unwrap();
        dbg!(&ast);
        assert_eq!(sql, &ast.to_string(), "round-tripping without changes");
        ast
    }
}

pub fn all_dialects() -> TestedDialects {
    TestedDialects {
        dialects: vec![
            Box::new(GenericDialect {}),
            Box::new(PostgreSqlDialect {}),
            Box::new(MsSqlDialect {}),
            Box::new(AnsiDialect {}),
        ],
    }
}

pub fn only<T>(v: impl IntoIterator<Item = T>) -> T {
    let mut iter = v.into_iter();
    if let (Some(item), None) = (iter.next(), iter.next()) {
        item
    } else {
        panic!("only called on collection without exactly one item")
    }
}

pub fn expr_from_projection(item: &SelectItem) -> &Expr {
    match item {
        SelectItem::UnnamedExpr(expr) => expr,
        _ => panic!("Expected UnnamedExpr"),
    }
}

pub fn number(n: &'static str) -> Value {
    Value::Number(n.parse().unwrap())
}
