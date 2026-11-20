// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Exhaustive audit: every keyword used as a bare identifier must round-trip
//! through `AstDisplay`. If a keyword parses as a bare identifier in some
//! position but the displayed SQL fails to re-parse (or re-parses to a
//! different statement), the `Ident` display must quote it.

#![cfg(not(target_arch = "wasm32"))]

use mz_sql_lexer::keywords::KEYWORDS;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::parser::{parse_expr, parse_statements};

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // slow: every keyword in each bracketed id position
fn bracketed_id_roundtrip_audit() {
    let templates = [
        "SELECT * FROM [%id% AS db.public.t VERSION 1]",
        "SELECT [%id% AS db.public.f](1)",
        "SELECT 'a'::[%id% AS db.public.ty]",
        "CREATE INDEX i IN CLUSTER [%id%] ON t (a)",
        "COMMENT ON NETWORK POLICY [%id%] IS 'x'",
    ];
    let ids = KEYWORDS
        .values()
        .map(|kw| kw.as_str().to_lowercase())
        .chain(
            [
                "u1",
                "1",
                "001",
                "1.5",
                "1e2",
                "with space",
                "a\"b",
                "Mixed",
                "ü1",
            ]
            .map(str::to_owned),
        );
    for id in ids {
        let quoted = format!("\"{}\"", id.replace('"', "\"\""));
        for template in templates {
            let sql = template.replace("%id%", &quoted);
            let original = parse_statements(&sql).expect("original parses");
            for printed in [
                original[0].ast.to_ast_string_simple(),
                original[0].ast.to_ast_string_stable(),
            ] {
                let reparsed = parse_statements(&printed)
                    .unwrap_or_else(|e| panic!("could not reparse {printed:?}: {e}"));
                assert_eq!(reparsed.len(), 1, "{printed}");
                assert_eq!(original[0].ast, reparsed[0].ast, "{sql} -> {printed}");
            }
        }
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // slow: ~519 keywords x N templates
fn keyword_identifier_roundtrip_audit() {
    // `%k%` is replaced by each keyword as a bare identifier, in the positions
    // where identifiers appear. We only check templates that parse: a keyword
    // that's reserved in a position fails the initial parse (correctly forcing
    // quoting), so it's skipped.
    let templates = [
        "SELECT %k%",
        "SELECT %k% + 1",
        "SELECT %k%.c FROM t",
        "SELECT f(%k%)",
        "SELECT * FROM %k%",
        "SELECT * FROM t WHERE %k% = 1",
        "CREATE TABLE %k% (a int4)",
        "CREATE VIEW v AS SELECT %k% FROM t",
        "SELECT %k% IS NULL",
        "SELECT -%k%",
    ];
    // Known printer/parser asymmetries for context-sensitive keywords used bare
    // where the parser greedily dispatches to a special grammar (`map[...]`,
    // `position(... IN ...)`); identical carve-out to the cargo-fuzz targets.
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
            let displayed = orig.to_ast_string_simple();
            match parse_statements(&displayed) {
                Ok(re) if re.len() == 1 => {
                    let re = re.into_iter().next().unwrap().ast;
                    if orig.to_ast_string_stable() != re.to_ast_string_stable() {
                        failures.push(format!("{k:>20}  {sql:?} -> {displayed:?}  [drift]"));
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    let msg = e.to_string();
                    if !carveouts.iter().any(|c| msg.contains(c)) {
                        failures.push(format!(
                            "{k:>20}  {sql:?} -> {displayed:?}  [reparse: {msg}]"
                        ));
                    }
                }
            }
        }
    }
    // Expression positions, where `AstDisplay` adds disambiguating parens (the
    // class that produced the `table`/field-access cargo-fuzz finding). These
    // parse a scalar expression directly. The display can introduce `Expr::Nested`
    // (parentheses), so compare the *stable* strings, which normalize them.
    let expr_templates = [
        "%k% & false is false.x", // field access on a binary expr -> parens
        "%k%[1]",                 // subscript receiver
        "%k%.x",                  // qualified / field access
        "not %k%",
        "%k% is null",
        "%k% + 1 = 2",
    ];
    for kw in KEYWORDS.values() {
        let k = kw.as_str().to_lowercase();
        for tmpl in expr_templates {
            let sql = tmpl.replace("%k%", &k);
            let Ok(orig) = parse_expr(&sql) else {
                continue;
            };
            let displayed = orig.to_ast_string_simple();
            match parse_expr(&displayed) {
                Ok(re) => {
                    if orig.to_ast_string_stable() != re.to_ast_string_stable() {
                        failures.push(format!("{k:>20}  expr {sql:?} -> {displayed:?}  [drift]"));
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    if !carveouts.iter().any(|c| msg.contains(c)) {
                        failures.push(format!(
                            "{k:>20}  expr {sql:?} -> {displayed:?}  [reparse: {msg}]"
                        ));
                    }
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} keyword-identifier round-trip gap(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}
