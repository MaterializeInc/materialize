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

use std::error::Error;

use unicode_width::UnicodeWidthStr;

use mz_ore::collections::CollectionExt;
use mz_ore::fmt::FormatBuffer;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::visit::Visit;
use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{AstInfo, Expr, Ident, Raw, RawDataType, RawObjectName};
use mz_sql_parser::parser::{self, ParserError};

#[test]
fn datadriven() {
    use datadriven::{walk, TestCase};

    fn render_error(sql: &str, e: ParserError) -> String {
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
                if s.len() != 1 {
                    return "expected exactly one statement\n".to_string();
                }
                let stmt = s.into_element();
                let parsed = match parser::parse_statements(&stmt.to_string()) {
                    Ok(parsed) => parsed.into_element(),
                    Err(err) => return format!("reparse failed: {}\n", err),
                };
                if parsed != stmt {
                    return format!("reparse comparison failed:\n{:?}\n!=\n{:?}\n", stmt, parsed);
                }
                if tc.args.get("roundtrip").is_some() {
                    format!("{}\n", stmt)
                } else {
                    // TODO(justin): it would be nice to have a middle-ground between this
                    // all-on-one-line and {:#?}'s huge number of lines.
                    format!("{}\n=>\n{:?}\n", stmt, stmt)
                }
            }
            Err(e) => render_error(input, e),
        }
    }

    fn parse_scalar(tc: &TestCase) -> String {
        let input = tc.input.trim();
        match parser::parse_expr(input) {
            Ok(s) => {
                if tc.args.get("roundtrip").is_some() {
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

    walk("tests/testdata", |f| {
        f.run(|test_case| -> String {
            match test_case.directive.as_str() {
                "parse-statement" => parse_statement(test_case),
                "parse-scalar" => parse_scalar(test_case),
                dir => panic!("unhandled directive {}", dir),
            }
        })
    });
}

#[test]
fn op_precedence() -> Result<(), Box<dyn Error>> {
    struct RemoveParens;

    impl<'a> VisitMut<'a, Raw> for RemoveParens {
        fn visit_expr_mut(&mut self, expr: &'a mut Expr<Raw>) {
            if let Expr::Nested(e) = expr {
                *expr = (**e).clone();
            }
            visit_mut::visit_expr_mut(self, expr);
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
        ("+ a / b COLLATE coll", "(+a) / (b COLLATE coll)"),
        ("- ts AT TIME ZONE 'tz'", "(-ts) AT TIME ZONE 'tz'"),
        ("a[b].c::d", "((a[b]).c)::d"),
    ] {
        let left = parser::parse_expr(actual)?;
        let mut right = parser::parse_expr(expected)?;
        RemoveParens.visit_expr_mut(&mut right);

        assert_eq!(left, right);
    }
    Ok(())
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

#[test]
fn test_basic_visitor() -> Result<(), Box<dyn Error>> {
    struct Visitor<'a> {
        seen_idents: Vec<&'a str>,
    }

    impl<'a> Visit<'a, Raw> for Visitor<'a> {
        fn visit_ident(&mut self, ident: &'a Ident) {
            self.seen_idents.push(ident.as_str());
        }
        fn visit_object_name(&mut self, object_name: &'a <Raw as AstInfo>::ObjectName) {
            if let RawObjectName::Name(name) = object_name {
                for ident in &name.0 {
                    self.seen_idents.push(ident.as_str());
                }
            }
        }
        fn visit_data_type(&mut self, data_type: &'a <Raw as AstInfo>::DataType) {
            if let RawDataType::Other { name, .. } = data_type {
                self.visit_object_name(name)
            }
        }
    }

    let stmts = parser::parse_statements(
        r#"
        WITH a01 AS (SELECT 1)
            SELECT *, a02.*, a03 AS a04
            FROM (SELECT * FROM a05) a06 (a07)
            JOIN a08 ON a09.a10 = a11.a12
            WHERE a13
            GROUP BY a14
            HAVING a15
        UNION ALL
            SELECT a16 IS NULL
                AND a17 IS NOT NULL
                AND a18 IN (a19)
                AND a20 IN (SELECT * FROM a21)
                AND CAST(a22 AS int)
                AND (a23)
                AND NOT a24
                AND a25(a26)
                AND CASE a27 WHEN a28 THEN a29 ELSE a30 END
                AND a31 BETWEEN a32 AND a33
                AND a34 COLLATE a35 = a36
                AND DATE_PART('YEAR', a37)
                AND (SELECT a38)
                AND EXISTS (SELECT a39)
            FROM a40(a41) AS a42
            LEFT JOIN a43 ON false
            RIGHT JOIN a44 ON false
            FULL JOIN a45 ON false
            JOIN a46 (a47) USING (a48)
            NATURAL JOIN (a49 NATURAL JOIN a50)
        EXCEPT
            (SELECT a51(a52) OVER (PARTITION BY a53 ORDER BY a54 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING))
        ORDER BY a55
        LIMIT 1;
        UPDATE b01 SET b02 = b03 WHERE b04;
        INSERT INTO c01 (c02) VALUES (c03);
        INSERT INTO c04 SELECT * FROM c05;
        DELETE FROM d01 WHERE d02;
        CREATE TABLE e01 (
            e02 INT PRIMARY KEY DEFAULT e03 CHECK (e04),
            CHECK (e05)
        ) WITH (e06 = 1);
        CREATE VIEW f01 (f02) AS SELECT * FROM f03;
        DROP TABLE j01;
        DROP VIEW k01;
        COPY l01 (l02) FROM stdin;
        START TRANSACTION READ ONLY;
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
        COMMIT;
        ROLLBACK;
"#,
    )?;

    #[rustfmt::skip]  // rustfmt loses the structure of the expected vector by wrapping all lines

    let expected = vec![
        "a01", "a02", "a03", "a04", "a05", "a06", "a07", "a08", "a09", "a10", "a11", "a12",
        "a13", "a14", "a15", "a16", "a17", "a18", "a19", "a20", "a21", "a22", "int4", "a23", "a24",
        "a25", "a26", "a27", "a28", "a29", "a30", "a31", "a32", "a33", "a34", "a35", "a36",
        "date_part",
        "a37", "a38", "a39", "a40", "a41", "a42", "a43", "a44", "a45", "a46", "a47", "a48",
        "a49", "a50", "a51", "a52", "a53", "a54", "a55",
        "b01", "b02", "b03", "b04",
        "c01", "c02", "c03", "c04", "c05",
        "d01", "d02",
        "e01", "e02", "int4", "e03", "e04", "e05", "e06",
        "f01", "f02", "f03",
        "j01",
        "k01",
        "l01", "l02",
    ];

    let mut visitor = Visitor {
        seen_idents: Vec::new(),
    };
    for stmt in &stmts {
        Visit::visit_statement(&mut visitor, stmt);
    }
    assert_eq!(visitor.seen_idents, expected);

    Ok(())
}
