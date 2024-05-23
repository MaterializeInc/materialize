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

//! SQL parser.
//!
//! This crate provides an SQL lexer and parser for Materialize's dialect of
//! SQL.
//!
//! ```
//! use mz_sql_parser::parser;
//!
//! let sql = "SELECT a, b, 123, myfunc(b) \
//!            FROM table_1 \
//!            WHERE a > b AND b < 100 \
//!            ORDER BY a DESC, b";
//!
//! let ast = parser::parse_statements(sql).unwrap();
//! println!("AST: {:?}", ast);
//! ```

pub mod ast;
pub mod parser;

mod ident;

#[cfg(feature = "test")]
pub fn datadriven_testcase(tc: &datadriven::TestCase) -> String {
    use crate::ast::display::AstDisplay;
    use crate::ast::{Expr, Statement};
    use datadriven::TestCase;
    use mz_ore::collections::CollectionExt;
    use mz_ore::fmt::FormatBuffer;
    use unicode_width::UnicodeWidthStr;

    fn render_error(sql: &str, e: parser::ParserError) -> String {
        let mut s = format!("error: {}\n", e.message);

        // Do our best to emulate psql in rendering a caret pointing at the
        // offending character in the query. This makes it possible to detect
        // incorrect error positions by visually scanning the test files.
        let end = sql.len();
        let line_start = sql[..e.pos].rfind('\n').map(|p| p + 1).unwrap_or(0);
        let line_end = sql[e.pos..].find('\n').map(|p| e.pos + p).unwrap_or(end);
        writeln!(s, "{}", &sql[line_start..line_end]);
        for _ in 0..sql[line_start..e.pos].width() {
            write!(s, " ");
        }
        writeln!(s, "^");

        s
    }

    fn parse_statement(tc: &TestCase) -> String {
        let input = tc.input.strip_suffix('\n').unwrap_or(&tc.input);
        match parser::parse_statements(input) {
            Ok(s) => {
                let stmt = s.into_element().ast;
                for printed in [stmt.to_ast_string(), stmt.to_ast_string_stable()] {
                    let mut parsed = match parser::parse_statements(&printed) {
                        Ok(parsed) => parsed.into_element().ast,
                        Err(err) => panic!("reparse failed: {}: {}\n", stmt, err),
                    };
                    match (&mut parsed, &stmt) {
                        // DECLARE remembers the original SQL. Erase that here so it can differ if
                        // needed (for example, quoting identifiers vs not). This is ok because we
                        // still compare that the resulting ASTs are identical, and it's valid for
                        // those to come from different original strings.
                        (Statement::Declare(parsed), Statement::Declare(stmt)) => {
                            parsed.sql.clone_from(&stmt.sql);
                        }
                        _ => {}
                    }
                    if parsed != stmt {
                        panic!(
                            "reparse comparison failed:\n{:?}\n!=\n{:?}\n{printed}\n",
                            stmt, parsed
                        );
                    }
                }
                if tc.args.contains_key("roundtrip") {
                    format!("{}\n", stmt)
                } else {
                    // TODO(justin): it would be nice to have a middle-ground between this
                    // all-on-one-line and {:#?}'s huge number of lines.
                    format!("{}\n=>\n{:?}\n", stmt, stmt)
                }
            }
            Err(e) => render_error(input, e.error),
        }
    }

    fn parse_scalar(tc: &TestCase) -> String {
        let input = tc.input.trim();
        match parser::parse_expr(input) {
            Ok(s) => {
                for printed in [s.to_ast_string(), s.to_ast_string_stable()] {
                    match parser::parse_expr(&printed) {
                        Ok(parsed) => {
                            // TODO: We always coerce the double colon operator into a Cast expr instead
                            // of keeping it as an Op (see parse_pg_cast). Expr::Cast always prints
                            // itself as double colon. We're thus unable to perfectly roundtrip
                            // `CAST(..)`. We could fix this by keeping "::" as a binary operator and
                            // teaching func.rs how to handle it, similar to how that file handles "~~"
                            // (without the parser converting that operator directly into an
                            // Expr::Like).
                            if !matches!(parsed, Expr::Cast { .. }) {
                                if parsed != s {
                                    panic!(
                                  "reparse comparison failed: {input} != {s}\n{:?}\n!=\n{:?}\n{printed}\n",
                                  s, parsed
                              );
                                }
                            }
                        }
                        Err(err) => panic!("reparse failed: {printed}: {err}\n{s:?}"),
                    }
                }

                if tc.args.contains_key("roundtrip") {
                    format!("{}\n", s)
                } else {
                    // TODO(justin): it would be nice to have a middle-ground between this
                    // all-on-one-line and {:#?}'s huge number of lines.
                    format!("{:?}\n", s)
                }
            }
            Err(e) => render_error(input, e),
        }
    }

    match tc.directive.as_str() {
        "parse-statement" => parse_statement(tc),
        "parse-scalar" => parse_scalar(tc),
        dir => panic!("unhandled directive {}", dir),
    }
}
