// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Round-trip tests asserting that pretty-printing preserves AST nodes that the
//! generic `AstDisplay` redacts or omits. Unlike `parser.rs`, which compares
//! `to_ast_string_stable()` (and so drops passwords on both sides), this compares
//! the full parsed AST, proving the value actually survives the round trip.

#![cfg(not(target_arch = "wasm32"))]

use mz_sql_parser::parser::parse_statements;
use mz_sql_pretty::pretty_str_simple;

/// Asserts that parsing `sql`, pretty-printing it (at several widths), and
/// re-parsing yields an identical AST.
fn assert_lossless(sql: &str) {
    let original = parse_statements(sql)
        .expect("original parses")
        .into_iter()
        .next()
        .expect("one statement")
        .ast;
    for width in [1, 40, 1_000_000] {
        let pretty = pretty_str_simple(sql, width).expect("pretty-prints");
        let reparsed = parse_statements(&pretty)
            .unwrap_or_else(|e| panic!("could not re-parse pretty output {pretty:?}: {e}"))
            .into_iter()
            .next()
            .expect("one statement")
            .ast;
        assert_eq!(
            original, reparsed,
            "\nast changed at width {width}\nsql:    {sql}\npretty: {pretty}"
        );
    }
}

#[mz_ore::test]
fn create_role_password_preserved() {
    assert_lossless("CREATE ROLE foo PASSWORD 'secret'");
    assert_lossless("CREATE ROLE foo WITH INHERIT PASSWORD 'secret'");
    assert_lossless("CREATE ROLE foo PASSWORD 'sup3r''secret'");
    assert_lossless("CREATE ROLE foo PASSWORD NULL");
}

#[mz_ore::test]
fn alter_role_password_preserved() {
    assert_lossless("ALTER ROLE foo WITH PASSWORD 'secret'");
    assert_lossless("ALTER ROLE foo PASSWORD NULL");
    assert_lossless("ALTER ROLE foo SET cluster TO 'quickstart'");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn materialized_view_as_of_preserved() {
    assert_lossless("CREATE MATERIALIZED VIEW mv AS SELECT 1 AS OF 12345");
}
