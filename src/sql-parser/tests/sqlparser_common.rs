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
use std::iter;

use datadriven::walk;
use itertools::Itertools;
use mz_ore::assert_ok;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::visit::Visit;
use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{AstInfo, Expr, Ident, Raw, RawDataType, RawItemName};
use mz_sql_parser::datadriven_testcase;
use mz_sql_parser::parser::{
    self, MAX_STATEMENT_BATCH_SIZE, parse_statements, parse_statements_with_limit,
};

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn datadriven() {
    walk("tests/testdata", |f| {
        f.run(|tc| -> String { datadriven_testcase(tc) })
    });
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
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
        ("2 OPERATOR(*) 2 + 2", "2 OPERATOR(*) (2 + 2)"),
    ] {
        let left = parser::parse_expr(actual)?;
        let mut right = parser::parse_expr(expected)?;
        RemoveParens.visit_expr_mut(&mut right);

        assert_eq!(left, right);
    }
    Ok(())
}

#[mz_ore::test]
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
        // Query-body-starting keywords must be quoted as identifiers, or a
        // parenthesized `(table & x)` re-parses as a `TABLE`-query. Regression
        // for the parse_expr_roundtrip cargo-fuzz finding.
        ("table", "\"table\"", "\"table\""),
        ("values", "\"values\"", "\"values\""),
        ("show", "\"show\"", "\"show\""),
        // Empty string allowed by Materialize but not PG.
        // TODO(justin): disallow this!
        ("", "\"\"", "\"\""),
    ];
    for (name, formatted, forced_quotation) in cases {
        assert_eq!(formatted, format!("{}", Ident::new(name).unwrap()));
        assert_eq!(
            forced_quotation,
            Ident::new(name).unwrap().to_ast_string_stable()
        );
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn test_basic_visitor() -> Result<(), Box<dyn Error>> {
    struct Visitor<'a> {
        seen_idents: Vec<&'a str>,
    }

    impl<'a> Visit<'a, Raw> for Visitor<'a> {
        fn visit_ident(&mut self, ident: &'a Ident) {
            self.seen_idents.push(ident.as_str());
        }
        fn visit_item_name(&mut self, item_name: &'a <Raw as AstInfo>::ItemName) {
            if let RawItemName::Name(name) = item_name {
                for ident in &name.0 {
                    self.seen_idents.push(ident.as_str());
                }
            }
        }
        fn visit_data_type(&mut self, data_type: &'a <Raw as AstInfo>::DataType) {
            if let RawDataType::Other { name, .. } = data_type {
                self.visit_item_name(name)
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
        );
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
        "e01", "e02", "int4", "e03", "e04", "e05",
        "f01", "f02", "f03",
        "j01",
        "k01",
        "l01", "l02",
    ];

    let mut visitor = Visitor {
        seen_idents: Vec::new(),
    };
    for stmt in &stmts {
        Visit::visit_statement(&mut visitor, &stmt.ast);
    }
    assert_eq!(visitor.seen_idents, expected);

    Ok(())
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_max_statement_batch_size() {
    let statement = "SELECT 1;";
    let size = statement.bytes().count();
    let max_statement_count = MAX_STATEMENT_BATCH_SIZE / size;
    let statements = iter::repeat(statement).take(max_statement_count).join("");

    assert_ok!(parse_statements_with_limit(&statements));
    let statements = format!("{statements}{statement}");
    let err = parse_statements_with_limit(&statements).expect_err("statements should be too big");
    assert!(err.contains("statement batch size cannot exceed "));
    assert_ok!(parse_statements(&statements));
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn test_nested_table_factor_recursion_limit() {
    // Deeply nested parens in table-factor position (`FROM ((((…`) recurse
    // through parse_table_factor -> parse_table_and_joins; they must hit the
    // parser's recursion limit and error out rather than overflow the stack or
    // balloon memory. Regression for the cargo-fuzz parse_display_roundtrip
    // stack-overflow and parse_expr_roundtrip OOM findings.
    let nested = format!("SELECT * FROM {}", "(".repeat(500));
    let err = parse_statements(&nested).expect_err("deeply nested table factor should error");
    assert!(
        err.to_string().contains("exceeds nested expression limit"),
        "unexpected error: {err}"
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn test_expr_chain_recursion_limit() {
    // Left-associative operators (`a + a`) and field access on a non-identifier
    // receiver (`(a).f`) are parsed in a loop, so a long *flat* chain builds AST
    // depth one node per step. Unbounded, the resulting AST overflows the stack
    // when it is later displayed/dropped/visited recursively. The chain is
    // capped at the recursion limit. Regression for the parse_expr_roundtrip
    // stack overflow (`a.ff.cX.*.G…`). (`a.f.f…` on a bare identifier is a flat
    // qualified name, not nesting, so it is intentionally unaffected.)
    for chain in [
        format!("a{}", " + a".repeat(500)),
        format!("a{}", " * a".repeat(500)),
        format!("(a){}", ".f".repeat(500)),
    ] {
        let err = parser::parse_expr(&chain).expect_err("deep expression chain should error");
        assert!(
            err.to_string().contains("exceeds nested expression limit"),
            "unexpected error for {:.20}…: {err}",
            chain
        );
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn test_show_query_body_display_roundtrip() {
    // A parenthesized SHOW carrying ORDER BY/LIMIT/OFFSET can't be unwrapped
    // into a top-level `Statement::Show` (which takes no modifiers), so it
    // survives as a `SelectStatement` whose query body is a bare `SHOW`.
    // Display must keep the parens, or reparsing the bare `SHOW … ORDER BY …`
    // fails. Regression for the parse_display_roundtrip fuzz finding.
    for sql in [
        "(SHOW foo ORDER BY bar)",
        "(SHOW foo LIMIT 1)",
        "(SHOW foo OFFSET 1)",
    ] {
        assert_display_roundtrips(sql);
    }
}

/// Asserts `parse -> AstDisplay (simple) -> parse` is stable for a single
/// statement (the `parse_display_roundtrip` cargo-fuzz invariant).
fn assert_display_roundtrips(sql: &str) {
    let ast = parse_statements(sql)
        .unwrap_or_else(|e| panic!("{sql:?} should parse: {e}"))
        .into_iter()
        .next()
        .expect("one statement")
        .ast;
    let displayed = ast.to_ast_string_simple();
    let reparsed = parse_statements(&displayed)
        .unwrap_or_else(|e| panic!("display {displayed:?} should reparse: {e}"))
        .into_iter()
        .next()
        .expect("one statement")
        .ast;
    assert_eq!(
        ast.to_ast_string_stable(),
        reparsed.to_ast_string_stable(),
        "display round trip drifted for {sql:?} (displayed {displayed:?})"
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn test_table_function_special_name_display_roundtrip() {
    // `extract`/`position` carry a special `extract(a FROM b)` / `position(a IN
    // b)` display that only reparses in scalar-expression position. As table
    // functions they must fall back to the plain (quoted) comma form.
    // Regression for the parse_pretty_roundtrip fuzz finding.
    for sql in [
        "SELECT a FROM extract(b, c)",
        "SELECT a FROM position(b, c)",
    ] {
        assert_display_roundtrips(sql);
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn test_quoted_special_grammar_function_name_display_roundtrip() {
    // A special-grammar keyword (`list`/`array`/`map`/…) quoted as a function
    // name parses to a plain `Function`, so display must keep it quoted or the
    // bare name dispatches to the keyword's special grammar on reparse
    // (`list(x)` -> a LIST expr). Regression for the parse_display_roundtrip
    // `"list"(c4)` finding.
    for sql in [
        "SELECT \"list\"(c4)",
        "SELECT \"array\"(c2)",
        "SELECT \"true\", \"array\"(c2), \"array\"(c2), \"list\"(c4) s",
    ] {
        assert_display_roundtrips(sql);
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn test_resolved_cluster_name_empty_id_rejected() {
    // A resolved cluster name renders as `[id]`; an empty id (`[""]`) would
    // display as `[]` and fail to reparse, so it must be rejected at parse time.
    // A non-empty resolved id still round-trips. Regression for the
    // parse_display_roundtrip finding `SHOW SINKS IN CLUSTER[""]`.
    assert!(parse_statements("SHOW SINKS IN CLUSTER[\"\"]").is_err());
    assert_display_roundtrips("SHOW SINKS IN CLUSTER[u1]");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn test_extract_generic_call_display_roundtrip() {
    // `extract` renders via the special `extract(field FROM src)` form only
    // when the field is a string literal (what EXTRACT's grammar produces). A
    // generic `"extract"(ident/number, x)` call must round-trip through the
    // plain (quoted) form instead. Regression for the parse_expr_roundtrip
    // finding (`"extract"(a, b)` drifted to `extract(a FROM b)`).
    for sql in [
        "SELECT \"extract\"(a, b)",
        "SELECT \"extract\"(1, b)",
        "SELECT extract('yr' FROM b)",
    ] {
        assert_display_roundtrips(sql);
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn test_as_keyword_as_identifier_display_roundtrip() {
    // A bare `as` at the start of a SELECT item is consumed as the `AS OF`
    // timestamp keyword, so an `as` identifier / function name must stay quoted
    // on display. Regression for the parse_display_roundtrip `"as"(…)` finding.
    for sql in ["SELECT \"as\"", "SELECT \"as\"(1)"] {
        assert_display_roundtrips(sql);
    }
}
