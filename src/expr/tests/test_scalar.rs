// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Spec tests for [`MirScalarExpr`] level transforms.
//!
//! Expressions use the `mz-expr-parser` syntax. Column types are supplied via
//! a leading `types (...)` input line. Set the REWRITE environment variable to
//! rewrite expected outputs.

use itertools::Itertools;
use mz_expr::canonicalize::{canonicalize_equivalences, canonicalize_predicates};
use mz_expr::{ColumnSpecs, Eval, Interpreter, MapFilterProject, MirScalarExpr};
use mz_expr_parser::{try_parse_column_types, try_parse_scalar, try_parse_scalars};
use mz_ore::str::separated;
use mz_repr::{ReprColumnType, ReprRelationType, RowArena, SqlColumnType};

/// Splits the required leading `types (...)` line off the directive input and
/// parses it, returning the declared column types and the remaining input.
fn parse_types_line(input: &str) -> Result<(Vec<SqlColumnType>, &str), String> {
    let input = input.trim_start_matches('\n');
    let Some((line, rest)) = input.split_once('\n') else {
        return Err("missing input after `types (...)` line".to_string());
    };
    let Some(types) = line.trim().strip_prefix("types") else {
        return Err(format!("expected leading `types (...)` line, got `{line}`"));
    };
    Ok((try_parse_column_types(types.trim())?, rest))
}

/// Splits the directive input into (command, rest) lines, where the command is
/// the first word of each non-empty line.
fn command_lines(input: &str) -> impl Iterator<Item = (&str, &str)> {
    input.lines().filter(|l| !l.trim().is_empty()).map(|l| {
        let l = l.trim();
        match l.split_once(char::is_whitespace) {
            Some((command, rest)) => (command, rest.trim()),
            None => (l, ""),
        }
    })
}

/// Strips the enclosing parentheses of a command argument list.
fn strip_parens(s: &str) -> Result<&str, String> {
    let s = s.trim();
    if s.starts_with('(') && s.ends_with(')') {
        Ok(&s[1..s.len() - 1])
    } else {
        Err(format!("expected parenthesized arguments, got `{s}`"))
    }
}

fn reduce(input: &str) -> Result<MirScalarExpr, String> {
    let (typ, input) = parse_types_line(input)?;
    let mut scalar = try_parse_scalar(input.trim())?;
    let repr_typ: Vec<ReprColumnType> = typ.iter().map(ReprColumnType::from).collect();
    let before = scalar.sql_typ(&typ);
    scalar.reduce(&repr_typ);
    let after = scalar.sql_typ(&typ);
    // Verify that `reduce` did not change the type of the scalar.
    if before.scalar_type != after.scalar_type {
        return Err(format!(
            "FAIL: Type of scalar has changed:\nbefore: {:?}\nafter: {:?}\n",
            before, after
        ));
    }
    Ok(scalar)
}

fn test_canonicalize_pred(input: &str) -> Result<Vec<MirScalarExpr>, String> {
    let (typ, input) = parse_types_line(input)?;
    let repr_typ: Vec<ReprColumnType> = typ.iter().map(ReprColumnType::from).collect();
    let input_predicates: Vec<MirScalarExpr> = input
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| try_parse_scalar(l.trim()))
        .collect::<Result<_, _>>()?;
    // predicate canonicalization is meant to produce the same output regardless of the
    // order of the input predicates.
    let mut predicates1 = input_predicates.clone();
    canonicalize_predicates(&mut predicates1, &repr_typ);
    let mut predicates2 = input_predicates.clone();
    predicates2.sort();
    canonicalize_predicates(&mut predicates2, &repr_typ);
    let mut predicates3 = input_predicates;
    predicates3.sort();
    predicates3.reverse();
    canonicalize_predicates(&mut predicates3, &repr_typ);
    if predicates1 != predicates2 || predicates1 != predicates3 {
        Err(format!(
            "predicate canonicalization resulted in unrealiable output: [{}] vs [{}] vs [{}]",
            separated(", ", predicates1.iter().map(|p| p.to_string())),
            separated(", ", predicates2.iter().map(|p| p.to_string())),
            separated(", ", predicates3.iter().map(|p| p.to_string())),
        ))
    } else {
        Ok(predicates1)
    }
}

fn test_canonicalize_equiv(input: &str) -> Result<Vec<Vec<MirScalarExpr>>, String> {
    let (input_type, input) = parse_types_line(input)?;
    let input_repr_type: Vec<ReprColumnType> =
        input_type.iter().map(ReprColumnType::from).collect();
    // One equivalence class per line, as a comma-separated expression list.
    let mut equivalences: Vec<Vec<MirScalarExpr>> = input
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| try_parse_scalars(l.trim()))
        .collect::<Result<_, _>>()?;
    canonicalize_equivalences(&mut equivalences, std::iter::once(&input_repr_type));
    Ok(equivalences)
}

/// Builds a [MapFilterProject], then modifies it with one command per input
/// line: `arity N` (required first), `map (...)`, `filter (...)`,
/// `project (...)`, or `optimize`.
fn test_mfp(input: &str) -> Result<MapFilterProject, String> {
    let mut mfp: Option<MapFilterProject> = None;
    for (command, rest) in command_lines(input) {
        if command == "arity" {
            let arity = rest.parse::<usize>().map_err(|e| e.to_string())?;
            mfp = Some(MapFilterProject::new(arity));
            continue;
        }
        let cur = mfp.ok_or_else(|| "expected leading `arity N` line".to_string())?;
        mfp = Some(match command {
            "map" => cur.map(try_parse_scalars(strip_parens(rest)?)?),
            "filter" => cur.filter(try_parse_scalars(strip_parens(rest)?)?),
            "project" => {
                let cols = strip_parens(rest)?
                    .split(',')
                    .map(|c| c.trim().parse::<usize>().map_err(|e| e.to_string()))
                    .collect::<Result<Vec<_>, _>>()?;
                cur.project(cols)
            }
            "optimize" => {
                let mut cur = cur;
                cur.optimize();
                cur
            }
            command => return Err(format!("unknown mfp command `{command}`")),
        });
    }
    mfp.ok_or_else(|| "expected leading `arity N` line".to_string())
}

/// An interpret test case specifies:
/// - The column types, via the leading `types (...)` line.
/// - One `values (...)` line per column with literal values. Our spec for
///   that column will be the union of the specs of the column's values.
/// - An `expr <expr>` line. We'll interpret the expression to get an output
///   spec.
/// - A `test (...)` line with literal values to test the output spec against.
fn test_interpret(input: &str) -> Result<Vec<String>, String> {
    let (types, input) = parse_types_line(input)?;
    let mut values: Vec<Vec<MirScalarExpr>> = vec![];
    let mut expr: Option<MirScalarExpr> = None;
    let mut tests: Vec<MirScalarExpr> = vec![];
    for (command, rest) in command_lines(input) {
        match command {
            "values" => values.push(try_parse_scalars(strip_parens(rest)?)?),
            "expr" => expr = Some(try_parse_scalar(rest)?),
            "test" => tests = try_parse_scalars(strip_parens(rest)?)?,
            command => return Err(format!("unknown interpret command `{command}`")),
        }
    }
    let expr = expr.ok_or_else(|| "missing `expr` line".to_string())?;

    let arena = RowArena::new();
    let relation = ReprRelationType::new(types.iter().map(ReprColumnType::from).collect());
    let mut interpreter = ColumnSpecs::new(&relation, &arena);

    let specs: Vec<_> = values
        .into_iter()
        .map(|col_exprs| {
            col_exprs
                .into_iter()
                .map(|expr| interpreter.expr(&expr).range)
                .reduce(|a, b| a.union(b))
                .expect("at least one literal")
        })
        .collect();

    for (id, spec) in specs.into_iter().enumerate() {
        interpreter.push_column(id, spec);
    }
    let output = interpreter.expr(&expr);

    let mut may_contain: Vec<_> = tests
        .iter()
        .map(|t| t.eval(&[], &arena).expect("literal datum"))
        .filter(|d| output.range.may_contain(*d))
        .map(|d| d.to_string())
        .collect();

    if output.range.may_fail() {
        may_contain.push("<err>".into())
    }

    Ok(may_contain)
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
fn run() {
    datadriven::walk("tests/spec", |f| {
        f.run(move |s| -> String {
            match s.directive.as_str() {
                // tests simplification of scalars
                "reduce" => match reduce(&s.input) {
                    Ok(scalar) => {
                        format!("{}\n", scalar)
                    }
                    Err(err) => format!("error: {}\n", err),
                },
                "canonicalize" => match test_canonicalize_pred(&s.input) {
                    Ok(preds) => {
                        format!("{}\n", separated("\n", preds.iter().map(|p| p.to_string())))
                    }
                    Err(err) => format!("error: {}\n", err),
                },
                "mfp" => match test_mfp(&s.input) {
                    Ok(mfp) => {
                        let (map, filter, project) = mfp.as_map_filter_project();
                        format!(
                            "[{}]\n[{}]\n[{}]\n",
                            separated(" ", map.iter()),
                            separated(" ", filter.iter()),
                            separated(" ", project.iter())
                        )
                    }
                    Err(err) => format!("error: {}\n", err),
                },
                "interpret" => match test_interpret(&s.input) {
                    Ok(contains) => {
                        format!("may contain: [{}]\n", contains.into_iter().join(" "))
                    }
                    Err(err) => format!("error: {}\n", err),
                },
                "canonicalize-join" => match test_canonicalize_equiv(&s.input) {
                    Ok(equivalences) => {
                        format!(
                            "{}\n",
                            separated(
                                "\n",
                                equivalences.iter().map(|e| format!(
                                    "[{}]",
                                    separated(" ", e.iter().map(|expr| format!("{}", expr)))
                                ))
                            )
                        )
                    }
                    Err(err) => format!("error: {}\n", err),
                },
                _ => panic!("unknown directive: {}", s.directive),
            }
        })
    });
}
