// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datadriven::walk;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::datadriven_testcase;
use mz_sql_parser::parser::{parse_expr, parse_statements};
use mz_sql_pretty::{doc_expr, to_pretty};

// Use the parser's datadriven tests to get a comprehensive set of SQL statements. Assert they all
// generate identical ASTs when pretty printed. Output the same output as the parser so datadriven
// is happy. (Having the datadriven parser be exported would be nice here too.)
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn test_parser() {
    walk("../sql-parser/tests/testdata", |f| {
        f.run(|tc| -> String {
            match tc.directive.as_str() {
                "parse-statement" => {
                    verify_pretty_statement(&tc.input);
                }
                "parse-scalar" => {
                    verify_pretty_expr(&tc.input);
                }
                _ => {}
            }
            datadriven_testcase(tc)
        })
    });
}

fn verify_pretty_expr(expr: &str) {
    let Ok(original) = parse_expr(expr) else {
        return;
    };
    for n in &[1, 40, 1000000] {
        let n = *n;
        let pretty1 = format!("{}", doc_expr(&original).pretty(n));
        let prettied = parse_expr(&pretty1)
            .unwrap_or_else(|_| panic!("could not parse: {pretty1}, original: {expr}"));
        let pretty2 = format!("{}", doc_expr(&prettied).pretty(n));
        assert_eq!(pretty1, pretty2);
        assert_eq!(
            original.to_ast_string_stable(),
            prettied.to_ast_string_stable(),
            "\noriginal: {expr}",
        );
    }
}

fn verify_pretty_statement(stmt: &str) {
    let original = match parse_statements(stmt) {
        Ok(stmt) => match stmt.into_iter().next() {
            Some(stmt) => stmt,
            None => return,
        },
        Err(_) => return,
    };
    for n in &[1, 40, 1000000] {
        let n = *n;
        let pretty1 = to_pretty(&original.ast, n);
        let prettied = parse_statements(&pretty1)
            .unwrap_or_else(|_| panic!("could not parse: {pretty1}, original: {stmt}"))
            .into_iter()
            .next()
            .unwrap();
        let pretty2 = to_pretty(&prettied.ast, n);
        assert_eq!(pretty1, pretty2);
        assert_eq!(
            original.ast.to_ast_string_stable(),
            prettied.ast.to_ast_string_stable(),
            "\noriginal: {stmt}",
        );
        // It'd be nice to assert that this squashes to a single line at high Ns, but literals and
        // idents can contain newlines so that's not always possible.
    }
}
