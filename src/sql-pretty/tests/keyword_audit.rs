// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Exhaustive audit of the *pretty* renderer: every keyword used as a bare
//! identifier must survive `parse -> pretty_str_simple -> parse`. The pretty
//! printer renders identifiers through `AstDisplay`, so this shares the
//! quoting logic with `sql-parser`'s `keyword_audit`, but it guards the pretty
//! path explicitly (the `parse_pretty_roundtrip` cargo-fuzz target lives here)
//! against any future node that renders an identifier without quoting.

#![cfg(not(target_arch = "wasm32"))]

use mz_sql_lexer::keywords::KEYWORDS;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::parser::parse_statements;
use mz_sql_pretty::pretty_str_simple;

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // slow: ~519 keywords x N templates x widths
fn keyword_identifier_pretty_roundtrip_audit() {
    // `%k%` becomes each keyword as a bare identifier, including positions that
    // make the printer add disambiguating parens (`%k%(...)`, `~ ANY (...)`) —
    // the shape behind the parse_pretty_roundtrip `table` finding.
    let templates = [
        "SELECT %k%",
        "SELECT %k%(1)",
        "SELECT %k% & 1 = 2",
        "SELECT %k% ~ ANY (VALUES ('a'))",
        // Nested `~ ANY` parenthesizes the LHS on display, putting `%k%` as the
        // leading token inside parens — the exact shape behind the
        // parse_pretty_roundtrip `table` finding.
        "SELECT %k%(1) ~ ANY (VALUES ('a')) ~ ANY (VALUES ('a'))",
        "SELECT %k%.c FROM t",
        "SELECT f(%k%)",
        "SELECT * FROM %k%",
        "SELECT * FROM t WHERE %k% IS NULL",
        "CREATE VIEW v AS SELECT %k% FROM t",
    ];
    let carveouts = [
        "Expected left square bracket",
        "Expected left parenthesis",
        "Expected IN, found",
        "Expected arrow, found",
    ];

    let mut failures = vec![];
    for kw in KEYWORDS.values() {
        let k = kw.as_str().to_lowercase();
        for tmpl in templates {
            let sql = tmpl.replace("%k%", &k);
            let Ok(orig) = parse_statements(&sql) else {
                continue;
            };
            if orig.len() != 1 {
                continue;
            }
            let orig = orig.into_iter().next().unwrap().ast;
            // A couple of widths, since layout (and thus parenthesization
            // context) can vary with the target width.
            for width in [1, 100] {
                let pretty = match pretty_str_simple(&sql, width) {
                    Ok(p) => p,
                    Err(e) => {
                        failures.push(format!("{k:>20}  {sql:?}  [pretty failed: {e}]"));
                        continue;
                    }
                };
                match parse_statements(&pretty) {
                    Ok(re) if re.len() == 1 => {
                        let re = re.into_iter().next().unwrap().ast;
                        if orig.to_ast_string_stable() != re.to_ast_string_stable() {
                            failures.push(format!("{k:>20}  {sql:?} -> {pretty:?}  [drift]"));
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        let msg = e.to_string();
                        if !carveouts.iter().any(|c| msg.contains(c)) {
                            failures
                                .push(format!("{k:>20}  {sql:?} -> {pretty:?}  [reparse: {msg}]"));
                        }
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} keyword-identifier pretty round-trip gap(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}
