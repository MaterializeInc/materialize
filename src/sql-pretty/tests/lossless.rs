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
fn declare_inner_secret_not_redacted() {
    // DECLARE/PREPARE wrap an inner statement and used to fall back to the
    // redacting AstDisplay for the whole wrapper, turning a secret in the inner
    // statement into `'<REDACTED>'`. They now recurse into the inner statement's
    // pretty doc, which prints the secret. (A full-AST round trip can't be used
    // here: DECLARE/PREPARE capture the raw input in a `sql` field that always
    // differs after reformatting, so we check the printed output directly.)
    // Found by parse_pretty_roundtrip.
    let pretty = pretty_str_simple("DECLARE c CURSOR FOR ALTER ROLE adb PASSWORD '2'", 100)
        .expect("pretty-prints");
    assert!(pretty.contains("PASSWORD '2'"), "secret redacted: {pretty}");
    assert!(!pretty.contains("REDACTED"), "secret redacted: {pretty}");

    // And the inner statement is still printed via the recursive doc printer.
    let pretty = pretty_str_simple("DECLARE c CURSOR FOR SELECT 1", 100).expect("pretty-prints");
    assert!(
        pretty.starts_with("DECLARE c CURSOR FOR SELECT"),
        "{pretty}"
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn materialized_view_as_of_preserved() {
    assert_lossless("CREATE MATERIALIZED VIEW mv AS SELECT 1 AS OF 12345");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn subscribe_relation_named_to_preserved() {
    // `to` is the optional `SUBSCRIBE TO` keyword, so a relation literally
    // named `to` must keep emitting the keyword or the name is dropped on
    // reparse (displayed as `SUBSCRIBE to` -> `SUBSCRIBE TO <missing>`).
    assert_lossless("SUBSCRIBE TO to");
    assert_lossless("SUBSCRIBE t");
    assert_lossless("SUBSCRIBE (SELECT 1)");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn parenthesized_show_with_modifiers_preserved() {
    // A parenthesized SHOW carrying ORDER BY/LIMIT/OFFSET survives as a query
    // body (it can't be unwrapped into a top-level `Statement::Show`, which
    // takes no modifiers), so it must keep its parens to round-trip.
    assert_lossless("(SHOW foo ORDER BY bar)");
    assert_lossless("(SHOW foo LIMIT 1)");
    assert_lossless("(SHOW foo OFFSET 1)");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn as_keyword_as_identifier_preserved() {
    // A bare `as` at the start of a SELECT item is consumed as the `AS OF`
    // timestamp keyword, so an `as` identifier / function name must stay quoted
    // to round-trip. Regression for the parse_display_roundtrip /
    // parse_pretty_roundtrip `"as"(…)` finding.
    assert_lossless("SELECT \"as\"");
    assert_lossless("SELECT \"as\"(1)");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn quoted_special_grammar_function_name_preserved() {
    // A special-grammar keyword (`list`/`array`/`map`/…) quoted as a function
    // name must stay quoted, or the bare name dispatches to its special grammar
    // on reparse (`list(x)` -> a LIST expr). Regression for the
    // parse_display_roundtrip / parse_pretty_roundtrip `"list"(…)` findings.
    assert_lossless("SELECT \"list\"(c4)");
    assert_lossless("SELECT \"true\", \"array\"(c2), \"array\"(c2), \"list\"(c4) s");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn table_function_special_name_preserved() {
    // `extract`/`position` table functions must not use the scalar-only
    // `extract(a FROM b)` / `position(a IN b)` special display, which doesn't
    // reparse in table-function position.
    assert_lossless("SELECT a FROM extract(b, c)");
    assert_lossless("SELECT a FROM position(b, c)");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn csr_seed_protobuf_message_name_preserved() {
    // A protobuf MESSAGE name containing a single quote must be escaped on
    // display, or it produces an unterminated string literal on reparse.
    assert_lossless(
        "CREATE SOURCE s FROM KAFKA CONNECTION c (TOPIC 'baz') \
         FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION csr \
         SEED VALUE SCHEMA 'sch' MESSAGE 'a''b'",
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn create_index_in_name_preserved() {
    // A bare `in` index name re-lexes as the start of the optional `IN CLUSTER`
    // clause, so the pretty-printer must quote it. Found by grammar_roundtrip.
    assert_lossless(r#"CREATE INDEX "in" ON t (a)"#);
}
