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

use std::error::Error;

use datadriven::walk;

use sql_parser::ast::display::AstDisplay;
use sql_parser::ast::visit::Visit;
use sql_parser::ast::visit_mut::{self, VisitMut};
use sql_parser::ast::{Expr, Ident};
use sql_parser::parser;

fn trim_one<'a>(s: &'a str) -> &'a str {
    if s.ends_with('\n') {
        &s[..s.len() - 1]
    } else {
        s
    }
}

#[test]
fn datadriven() {
    walk("tests/testdata", |f| {
        f.run(|test_case| -> String {
            match test_case.directive.as_str() {
                "parse-statement" => {
                    let sql = trim_one(&test_case.input).to_owned();
                    match parser::parse_statements(sql) {
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
                    let sql = test_case.input.trim().to_owned();
                    match parser::parse_expr(sql) {
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
fn op_precedence() -> Result<(), Box<dyn Error>> {
    struct RemoveParens;

    impl<'a> VisitMut<'a> for RemoveParens {
        fn visit_expr_mut(&mut self, expr: &'a mut Expr) {
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
    ] {
        let left = parser::parse_expr((*actual).to_owned())?;
        let mut right = parser::parse_expr((*expected).to_owned())?;
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

    impl<'a> Visit<'a> for Visitor<'a> {
        fn visit_ident(&mut self, ident: &'a Ident) {
            self.seen_idents.push(ident.as_str());
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
                AND EXTRACT(YEAR FROM a37)
                AND (SELECT a38)
                AND EXISTS (SELECT a39)
            FROM a40(a41) AS a42 WITH (a43)
            LEFT JOIN a44 ON false
            RIGHT JOIN a45 ON false
            FULL JOIN a46 ON false
            JOIN a47 (a48) USING (a49)
            NATURAL JOIN (a50 NATURAL JOIN a51)
        EXCEPT
            (SELECT a52(a53) OVER (PARTITION BY a54 ORDER BY a55 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING))
        ORDER BY a56
        LIMIT 1;
        UPDATE b01 SET b02 = b03 WHERE b04;
        INSERT INTO c01 (c02) VALUES (c03);
        INSERT INTO c04 SELECT * FROM c05;
        DELETE FROM d01 WHERE d02;
        CREATE TABLE e01 (
            e02 INT PRIMARY KEY DEFAULT e03 CHECK (e04),
            CHECK (e05)
        ) WITH (e06 = 1);
        CREATE VIEW f01 (f02) WITH (f03 = 1) AS SELECT * FROM f04;
        ALTER TABLE g01 ADD CONSTRAINT g02 PRIMARY KEY (g03);
        ALTER TABLE h01 ADD CONSTRAINT h02 FOREIGN KEY (h03) REFERENCES h04 (h05);
        ALTER TABLE i01 ADD CONSTRAINT i02 UNIQUE (i03);
        DROP TABLE j01;
        DROP VIEW k01;
        COPY l01 (l02) FROM stdin;
        START TRANSACTION READ ONLY;
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
        COMMIT;
        ROLLBACK;
"#
        .into(),
    )?;

    #[rustfmt::skip]  // rustfmt loses the structure of the expected vector by wrapping all lines
    let expected = vec![
        "a01", "a02", "a03", "a04", "a05", "a06", "a07", "a08", "a09", "a10", "a11", "a12",
        "a13", "a14", "a15", "a16", "a17", "a18", "a19", "a20", "a21", "a22", "a23", "a24",
        "a25", "a26", "a27", "a28", "a29", "a30", "a31", "a32", "a33", "a34", "a35", "a36",
        "date_part",
        "a37", "a38", "a39", "a40", "a41", "a42", "a43", "a44", "a45", "a46", "a47", "a48",
        "a49", "a50", "a51", "a52", "a53", "a54", "a55", "a56",
        "b01", "b02", "b03", "b04",
        "c01", "c02", "c03", "c04", "c05",
        "d01", "d02",
        "e01", "e02", "e03", "e04", "e05", "e06",
        "f01", "f02", "f03", "f04",
        "g01", "g02", "g03",
        "h01", "h02", "h03", "h04", "h05",
        "i01", "i02", "i03",
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
