// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! One-shot converter from the lowertest-based `tests/testdata` files to the
//! spec-based `tests/spec` files consumed by `test_scalar.rs`.
//!
//! Inputs are parsed with the old lowertest machinery and printed in the spec
//! syntax. Each converted expression is parsed back and asserted equal to the
//! original, so function variants and literal types cannot silently drift.
//! Expected outputs are carried over verbatim, so running `test_scalar.rs`
//! without REWRITE verifies that the new harness reproduces the old results.
//!
//! Run with `cargo test -p mz-expr --test convert_spec -- --ignored`.

use std::fmt::Write;

use mz_expr::MirScalarExpr;
use mz_expr_parser::{print_scalar, try_parse_column_types, try_parse_scalar};
use mz_expr_test_util::MirScalarExprDeserializeContext;
use mz_lowertest::{MzReflect, deserialize, deserialize_optional, tokenize};
use mz_repr::{SqlColumnType, SqlScalarType};
use serde::{Deserialize, Serialize};

/// A single datadriven test case, with surrounding comments and blank lines
/// preserved verbatim.
struct Case {
    preamble: String,
    directive: String,
    input: String,
    expected: String,
}

fn split_cases(contents: &str) -> Vec<Case> {
    let mut cases = vec![];
    let mut lines = contents.lines().peekable();
    loop {
        let mut preamble = String::new();
        while let Some(line) = lines.peek() {
            if line.trim().is_empty() || line.trim_start().starts_with('#') {
                writeln!(preamble, "{}", line).unwrap();
                lines.next();
            } else {
                break;
            }
        }
        let Some(directive) = lines.next() else {
            // Trailing preamble (end-of-file comments).
            if !preamble.is_empty() {
                cases.push(Case {
                    preamble,
                    directive: String::new(),
                    input: String::new(),
                    expected: String::new(),
                });
            }
            break;
        };
        let mut input = String::new();
        for line in lines.by_ref() {
            if line.trim() == "----" {
                break;
            }
            writeln!(input, "{}", line).unwrap();
        }
        let mut expected = String::new();
        for line in lines.by_ref() {
            if line.trim().is_empty() {
                break;
            }
            writeln!(expected, "{}", line).unwrap();
        }
        cases.push(Case {
            preamble,
            directive: directive.to_string(),
            input,
            expected,
        });
    }
    cases
}

fn render_types(types: &[SqlColumnType]) -> String {
    let parts = types
        .iter()
        .map(|t| {
            let name = match &t.scalar_type {
                SqlScalarType::Bool => "boolean",
                SqlScalarType::Int16 => "smallint",
                SqlScalarType::Int32 => "integer",
                SqlScalarType::Int64 => "bigint",
                SqlScalarType::Float64 => "double precision",
                SqlScalarType::Numeric { .. } => "numeric",
                SqlScalarType::String => "text",
                SqlScalarType::Jsonb => "jsonb",
                other => panic!("cannot render type {other:?}"),
            };
            format!("{name}{}", if t.nullable { "?" } else { "" })
        })
        .collect::<Vec<_>>();
    let rendered = parts.join(", ");
    // The spec type list must parse back to the very same types.
    let reparsed = try_parse_column_types(&format!("({rendered})")).unwrap();
    assert_eq!(&reparsed, types, "type list drift for ({rendered})");
    rendered
}

/// Prints `expr` in spec syntax and asserts that parsing it back yields the
/// original expression.
fn print_checked(expr: &MirScalarExpr) -> String {
    let printed = print_scalar(expr).unwrap();
    let reparsed = try_parse_scalar(&printed)
        .unwrap_or_else(|err| panic!("cannot reparse `{printed}`: {err}"));
    assert_eq!(&reparsed, expr, "roundtrip drift for `{printed}`");
    printed
}

fn print_checked_list(exprs: &[MirScalarExpr]) -> String {
    exprs
        .iter()
        .map(print_checked)
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Deserialize, Serialize, MzReflect)]
enum MFPTestCommand {
    Map(Vec<MirScalarExpr>),
    Filter(Vec<MirScalarExpr>),
    Project(Vec<usize>),
    Optimize,
}

/// Converts the input of one old test case to (directive line, input block).
fn convert_case(directive: &str, input: &str) -> Result<(String, String), String> {
    let mut stream = tokenize(input)?.into_iter();
    let mut ctx = MirScalarExprDeserializeContext::default();
    match directive {
        "reduce" => {
            let scalar: MirScalarExpr = deserialize(&mut stream, "MirScalarExpr", &mut ctx)?;
            let types: Vec<SqlColumnType> =
                deserialize(&mut stream, "Vec<SqlColumnType>", &mut ctx)?;
            Ok((
                "reduce".to_string(),
                format!(
                    "types ({})\n{}\n",
                    render_types(&types),
                    print_checked(&scalar)
                ),
            ))
        }
        "canonicalize" => {
            let predicates: Vec<MirScalarExpr> =
                deserialize(&mut stream, "Vec<MirScalarExpr>", &mut ctx)?;
            let types: Vec<SqlColumnType> =
                deserialize(&mut stream, "Vec<SqlColumnType>", &mut ctx)?;
            let mut body = format!("types ({})\n", render_types(&types));
            for p in &predicates {
                writeln!(body, "{}", print_checked(p)).unwrap();
            }
            Ok(("canonicalize".to_string(), body))
        }
        "canonicalize-join" => {
            let equivalences: Vec<Vec<MirScalarExpr>> =
                deserialize(&mut stream, "Vec<Vec<MirScalarExpr>>", &mut ctx)?;
            let types: Vec<SqlColumnType> =
                deserialize(&mut stream, "Vec<SqlColumnType>", &mut ctx)?;
            let mut body = format!("types ({})\n", render_types(&types));
            for class in &equivalences {
                writeln!(body, "{}", print_checked_list(class)).unwrap();
            }
            Ok(("canonicalize-join".to_string(), body))
        }
        "mfp" => {
            let arity = stream
                .next()
                .unwrap()
                .to_string()
                .parse::<usize>()
                .map_err(|e| e.to_string())?;
            let mut body = format!("arity {arity}\n");
            while let Some(command) = deserialize_optional::<MFPTestCommand, _, _>(
                &mut stream,
                "MFPTestCommand",
                &mut ctx,
            )? {
                match command {
                    MFPTestCommand::Map(exprs) => {
                        writeln!(body, "map ({})", print_checked_list(&exprs)).unwrap()
                    }
                    MFPTestCommand::Filter(exprs) => {
                        writeln!(body, "filter ({})", print_checked_list(&exprs)).unwrap()
                    }
                    MFPTestCommand::Project(cols) => {
                        let cols = cols.iter().map(|c| c.to_string()).collect::<Vec<_>>();
                        writeln!(body, "project ({})", cols.join(", ")).unwrap()
                    }
                    MFPTestCommand::Optimize => writeln!(body, "optimize").unwrap(),
                }
            }
            Ok(("mfp".to_string(), body))
        }
        "interpret" => {
            let types: Vec<SqlColumnType> =
                deserialize(&mut stream, "Vec<SqlColumnType>", &mut ctx)?;
            let values: Vec<Vec<MirScalarExpr>> =
                deserialize(&mut stream, "Vec<Vec<MirScalarExpr>>", &mut ctx)?;
            let expr: MirScalarExpr = deserialize(&mut stream, "MirScalarExpr", &mut ctx)?;
            let tests: Vec<MirScalarExpr> =
                deserialize(&mut stream, "Vec<MirScalarExpr>", &mut ctx)?;
            let mut body = format!("types ({})\n", render_types(&types));
            for column in &values {
                writeln!(body, "values ({})", print_checked_list(column)).unwrap();
            }
            writeln!(body, "expr {}", print_checked(&expr)).unwrap();
            writeln!(body, "test ({})", print_checked_list(&tests)).unwrap();
            Ok(("interpret".to_string(), body))
        }
        directive => Err(format!("unknown directive: {directive}")),
    }
}

#[mz_ore::test]
#[ignore = "one-shot conversion tool"]
fn convert() {
    for file in ["reduce", "mfp", "interpret"] {
        let contents = std::fs::read_to_string(format!("tests/testdata/{file}")).unwrap();
        let mut out = String::new();
        for case in split_cases(&contents) {
            out.push_str(&case.preamble);
            if case.directive.is_empty() {
                continue;
            }
            let (directive, input) = convert_case(&case.directive, &case.input)
                .unwrap_or_else(|err| panic!("{file}: cannot convert case: {err}"));
            writeln!(out, "{directive}").unwrap();
            out.push_str(&input);
            writeln!(out, "----").unwrap();
            out.push_str(&case.expected);
            writeln!(out).unwrap();
        }
        // Drop a trailing blank line if the original file did not end with one.
        if !contents.ends_with("\n\n") && out.ends_with("\n\n") {
            out.pop();
        }
        std::fs::write(format!("tests/spec/{file}.spec"), out).unwrap();
    }
}
