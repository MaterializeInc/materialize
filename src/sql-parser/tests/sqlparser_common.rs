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

//! Test SQL syntax.

use std::fmt;

use sql_parser::ast::display::AstDisplay;
use sql_parser::ast::visit_mut::VisitMut;
use sql_parser::ast::*;
use sql_parser::parser::*;

use datadriven::walk;

fn trim_one<'a>(s: &'a str) -> &'a str {
    if s.ends_with('\n') {
        &s[..s.len() - 1]
    } else {
        s
    }
}

pub fn run_parser_method<F, T>(sql: &str, f: F) -> T
where
    F: Fn(&mut Parser) -> T,
    T: fmt::Debug + PartialEq,
{
    let mut tokenizer = sql_parser::tokenizer::Tokenizer::new(sql);
    let tokens = tokenizer.tokenize().unwrap();
    f(&mut Parser::new(sql.to_string(), tokens))
}

fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, ParserError> {
    Parser::parse_sql(sql.to_string())
}

#[test]
fn datadriven() {
    walk("tests/testdata", |f| {
        f.run(|test_case| -> String {
            match test_case.directive.as_str() {
                "parse-statement" => {
                    match parse_sql_statements(trim_one(&test_case.input)) {
                        Ok(s) => {
                            if s.len() != 1 {
                                "expected exactly one statement".to_string()
                            } else if test_case.args.get("roundtrip").is_some() {
                                format!("{}\n", s.iter().next().unwrap().to_string())
                            } else {
                                let stmt = s.iter().next().unwrap();
                                // TODO(justin): it would be nice to have a middle-ground between this
                                // all-on-one-line and {:#?}'s huge number of lines.
                                format!("{}\n=>\n{:?}\n", stmt.to_string(), stmt)
                            }
                        }
                        Err(e) => format!("error:\n{}\n", e),
                    }
                }
                "parse-scalar" => {
                    match run_parser_method(&test_case.input.trim(), Parser::parse_expr) {
                        Ok(s) => {
                            if test_case.args.get("roundtrip").is_some() {
                                format!("{}\n", s.to_string())
                            } else {
                                // TODO(justin): it would be nice to have a middle-ground between this
                                // all-on-one-line and {:#?}'s huge number of lines.
                                format!("{:?}\n", s)
                            }
                        }
                        Err(e) => format!("error:\n{}\n", e),
                    }
                }
                dir => {
                    panic!("unhandled directive {}", dir);
                }
            }
        })
    });
}

#[test]
fn op_precedence() {
    struct RemoveParens;

    impl<'a> VisitMut<'a> for RemoveParens {
        fn visit_expr(&mut self, expr: &'a mut Expr) {
            if let Expr::Nested(e) = expr {
                *expr = (**e).clone();
            }
            visit_mut::visit_expr(self, expr);
        }
    }

    for (actual, expected) in &[
        ("a + b + c", "(a + b) + c"),
        ("a - b + c", "(a - b) + c"),
        ("a + b * c", "a + (b * c)"),
        ("true = 'foo' like 'foo'", "true = ('foo' like 'foo')"),
        ("a->b = c->d", "(a->b) = (c->d)"),
        ("a @> b = c @> d", "(a @> b) = (c @> d)"),
        ("a = b is null", "(a = b) is null"),
        ("a and b or c and d", "(a and b) or (c and d)"),
        ("+ a / b", "(+ a) / b"),
        ("NOT true OR true", "(NOT true) OR true"),
        ("NOT a IS NULL", "NOT (a IS NULL)"),
        ("NOT 1 NOT BETWEEN 1 AND 2", "NOT (1 NOT BETWEEN 1 AND 2)"),
        ("NOT a NOT LIKE b", "NOT (a NOT LIKE b)"),
        ("NOT a NOT IN ('a')", "NOT (a NOT IN ('a'))"),
    ] {
        let left = Parser::parse_sql(format!("SELECT {}", actual)).unwrap();
        let mut right = Parser::parse_sql(format!("SELECT {}", expected)).unwrap();
        RemoveParens.visit_statement(&mut right[0]);

        assert_eq!(left, right);
    }
}

#[test]
fn parse_invalid_table_name() {
    let ast = run_parser_method("db.public..customer", Parser::parse_object_name);
    assert!(ast.is_err());
}

#[test]
fn parse_no_table_name() {
    let ast = run_parser_method("", Parser::parse_object_name);
    assert!(ast.is_err());
}

#[test]
fn format_ident() {
    let cases = vec![
        ("foo", "foo", "\"foo\""),
        ("_foo", "_foo", "\"_foo\""),
        ("foo_bar", "foo_bar", "\"foo_bar\""),
        ("foo1", "foo1", "\"foo1\""),
        // Contains disallowed character.
        ("Foo", "\"Foo\"", "\"Foo\""),
        ("a b", "\"a b\"", "\"a b\""),
        ("\"", "\"\"\"\"", "\"\"\"\""),
        ("foo\"bar", "\"foo\"\"bar\"", "\"foo\"\"bar\""),
        ("foo$bar", "\"foo$bar\"", "\"foo$bar\""),
        // Digit at the beginning.
        ("1foo", "\"1foo\"", "\"1foo\""),
        // Non-reserved keyword.
        ("floor", "floor", "\"floor\""),
        // Reserved keyword.
        ("order", "\"order\"", "\"order\""),
        // Empty string allowed by Materialize but not PG.
        // TODO(justin): disallow this!
        ("", "\"\"", "\"\""),
    ];
    for (name, formatted, forced_quotation) in cases {
        assert_eq!(formatted, format!("{}", Ident::new(name)));
        assert_eq!(forced_quotation, Ident::new(name).to_ast_string_stable());
    }
}
