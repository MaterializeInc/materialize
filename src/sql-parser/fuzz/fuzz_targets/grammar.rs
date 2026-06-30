// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: the SQL round-trip oracle (`parse -> AstDisplay/pretty ->
//! reparse` must preserve the AST), driven by a **declarative grammar file**
//! (`grammar/sql.g`) rather than a hand-written generator.
//!
//! `build.rs` compiles `grammar/sql.g` into a rule table (`Item`/`Rule`/`RULES`/
//! `START`, included below). This target walks that table from the start rule,
//! consuming the libFuzzer byte stream as a sequence of production choices, to
//! emit a syntactically plausible statement. Coverage feedback steers the
//! byte->choice mapping. Each generated statement that parses is checked against
//! the same two oracles as the hand-written `sql_roundtrip` target: full-AST
//! equality through `pretty_str_simple` at two line widths, and stable-string
//! equality through `AstDisplay`.
//!
//! The grammar file is the single source of truth for the input language: extend
//! the SQL surface by editing `grammar/sql.g`, with no generator code to touch.
//! Non-parsing output is a harmless no-op (parsing must merely never panic). The
//! grammar is written so the bulk of output is valid and exercises the printer.

#![no_main]

use libfuzzer_sys::arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{AstInfo, Expr, Op, Raw, Statement, Value};
use mz_sql_parser::parser::parse_statements;
use mz_sql_pretty::pretty_str_simple;

// The grammar rule table compiled from `grammar/sql.g` by `build.rs`. Provides
// `enum Item`, `struct Rule`, `static RULES: &[Rule]`, and `static START`.
include!(concat!(env!("OUT_DIR"), "/grammar.rs"));

// ---------------------------------------------------------------------------
// Round-trip oracle (mirrors sql_roundtrip / parse_pretty_roundtrip).
// ---------------------------------------------------------------------------

/// Strip syntactic noise so AST equality reflects *semantic* fidelity:
/// `Declare`/`Prepare` capture raw text, `Expr::Nested` records parens the
/// printer is free to add or drop, and a negative numeric literal is the same
/// value whether the parser folded the sign in (`Number("-1")`) or left a unary
/// op (`- 1`). The parser chooses by *context* (a leading `- 1` folds, `a + - 1`
/// does not), so the two forms must compare equal.
fn normalize(stmt: &mut Statement<Raw>) {
    match stmt {
        Statement::Declare(d) => {
            d.sql.clear();
            normalize(&mut d.stmt);
        }
        Statement::Prepare(p) => {
            p.sql.clear();
            normalize(&mut p.stmt);
        }
        _ => {}
    }
    RemoveParens.visit_statement_mut(stmt);
}

struct RemoveParens;

impl<'a, T: AstInfo> VisitMut<'a, T> for RemoveParens {
    fn visit_expr_mut(&mut self, expr: &'a mut Expr<T>) {
        visit_mut::visit_expr_mut(self, expr);
        if let Expr::Nested(inner) = expr {
            *expr = (**inner).clone();
        }
        // Canonicalize a negative numeric literal to a unary minus over the bare
        // number, so it compares equal to the unfolded `- <number>` form the
        // parser produces in non-leading position. (Positive literals are never
        // sign-prefixed by the parser, so only `-` needs handling.)
        if let Expr::Value(Value::Number(n)) = expr {
            if let Some(rest) = n.strip_prefix('-') {
                *expr = Expr::Op {
                    op: Op::bare("-"),
                    expr1: Box::new(Expr::Value(Value::Number(rest.to_string()))),
                    expr2: None,
                };
            }
        }
    }
}

/// Reparse errors that are a known printer/parser asymmetry rather than a bug.
fn benign_reparse_error(msg: &str) -> bool {
    msg.contains("exceeds nested expression limit")
        || msg.contains("Expected left square bracket")
        || msg.contains("Expected left parenthesis")
        || msg.contains("Expected IN, found")
        || msg.contains("Expected arrow, found")
}

fn check_pretty(sql: &str, orig_ast: &Statement<Raw>) {
    for width in [100, 1] {
        let pretty = match pretty_str_simple(sql, width) {
            Ok(p) => p,
            Err(e) => panic!("parsed but pretty failed: input={sql:?} width={width} err={e}"),
        };
        let reparsed = match parse_statements(&pretty) {
            Ok(r) => r,
            Err(e) => {
                if benign_reparse_error(&e.to_string()) {
                    continue;
                }
                panic!("pretty output failed to reparse: pretty={pretty:?} width={width} err={e}");
            }
        };
        let Some(stmt) = reparsed.into_iter().next() else {
            continue;
        };
        let mut reparsed_ast = stmt.ast;
        normalize(&mut reparsed_ast);
        assert_eq!(
            *orig_ast, reparsed_ast,
            "AST changed through pretty roundtrip\ninput:  {sql:?}\nwidth:  {width}\npretty: {pretty:?}"
        );
    }
}

fn check_display(orig_ast: &Statement<Raw>) {
    let displayed = orig_ast.to_ast_string_simple();
    let reparsed = match parse_statements(&displayed) {
        Ok(r) => r,
        Err(e) => {
            if benign_reparse_error(&e.to_string()) {
                return;
            }
            panic!("AstDisplay output failed to reparse: displayed={displayed:?} err={e}");
        }
    };
    if reparsed.len() != 1 {
        return;
    }
    let mut reparsed_ast = reparsed.into_iter().next().unwrap().ast;
    // Normalize the reparse too (mirroring `check_pretty`): the parser may
    // re-insert a semantically-redundant `Expr::Nested` (e.g. it parenthesizes a
    // cast under unary minus), and per the oracle's contract those parens are
    // free to add or drop. Stripping them from both sides leaves a genuine
    // *structural* drift to still trip the assert.
    normalize(&mut reparsed_ast);
    // Compare ASTs *structurally*, not by re-printed string. A printer that drops
    // a needed paren can map two distinct ASTs onto the same string (e.g.
    // `IsExpr(a, DistinctFrom(Or(b, c)))` and `Or(IsExpr(a, DistinctFrom(b)), c)`
    // both print `a IS DISTINCT FROM b OR c`). A stable-string comparison is blind
    // to those collisions, but the structural comparison catches them. The stable
    // strings are still shown for a readable diff.
    assert_eq!(
        *orig_ast,
        reparsed_ast,
        "AstDisplay roundtrip drifted\ndisplayed: {displayed:?}\norig:     {}\nreparsed: {}",
        orig_ast.to_ast_string_stable(),
        reparsed_ast.to_ast_string_stable(),
    );
}

// ---------------------------------------------------------------------------
// Grammar walker.
// ---------------------------------------------------------------------------

/// Identifiers for `@ident`, weighted toward the cases that exercise the
/// printer's quoting decision (bare names, quoted keyword collisions, names that
/// only round-trip when quoted).
const IDENTS: &[&str] = &[
    "a", "b", "c", "x", "y", "col", "foo", "bar", "t1", "t2", "\"select\"", "\"from\"", "\"any\"",
    "\"Mixed\"", "\"with space\"", "\"a.b\"", "\"1col\"", "\"qu\"\"ote\"",
];

/// String literals for `@str`, weighted toward lexing/escaping edge cases.
const STRINGS: &[&str] = &[
    "'a'", "''", "'foo bar'", "'it''s'", "'a\"b'", "'%'", "'_'", "'100'",
];

/// Every keyword the lexer knows, for `@kw` (a bare keyword used as an
/// identifier, the printer-quoting property nearly every round-trip bug we've
/// found violated). Read from the lexer's list so it stays complete.
fn keywords() -> &'static [&'static str] {
    use std::sync::OnceLock;
    static KW: OnceLock<Vec<&'static str>> = OnceLock::new();
    KW.get_or_init(|| {
        include_str!("../../../sql-lexer/src/keywords.txt")
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect()
    })
}

/// Recursion-depth budget for one generated statement.
const MAX_DEPTH: u32 = 6;
/// Absolute recursion guard, independent of `MAX_DEPTH`, so a cyclic "leaf"
/// alternative in the grammar can never spin forever.
const MAX_CALLS: u32 = 400;
/// Hard cap on generated length: structure-aware generation can otherwise blow
/// up through nested subqueries.
const MAX_OUTPUT: usize = 2000;

struct Gen<'a, 'u> {
    u: &'u mut Unstructured<'a>,
    out: String,
}

impl<'a, 'u> Gen<'a, 'u> {
    /// A uniform choice in `0..n` (0 when the byte stream is exhausted, so the
    /// generator deterministically winds down at the end of input).
    fn pick(&mut self, n: usize) -> usize {
        if n <= 1 {
            return 0;
        }
        self.u
            .int_in_range(0..=(n as u64 - 1))
            .map(|v| v as usize)
            .unwrap_or(0)
    }

    fn one_of(&mut self, opts: &[&str]) {
        let i = self.pick(opts.len());
        self.out.push_str(opts[i]);
    }

    /// Expand rule `idx`. `depth` is the grammar recursion budget, `calls` is the
    /// absolute call guard. Once either is spent (or the output cap is hit), the
    /// rule's precomputed `leaf_alt` is used and rule references stop recursing.
    fn gen(&mut self, idx: usize, depth: u32, calls: u32) {
        if self.out.len() >= MAX_OUTPUT || calls >= MAX_CALLS {
            return;
        }
        let rule = &RULES[idx];
        let exhausted = depth == 0;
        let alt = if exhausted {
            rule.alts[rule.leaf_alt]
        } else {
            rule.alts[self.pick(rule.alts.len())]
        };
        for item in alt {
            match item {
                Item::Lit(s) => self.out.push_str(s),
                Item::Rule(i) => self.gen(*i, depth.saturating_sub(1), calls + 1),
                Item::Ident => self.one_of(IDENTS),
                Item::Kw => {
                    let kw = keywords();
                    let i = self.pick(kw.len());
                    self.out.push_str(kw[i]);
                }
                Item::Int => {
                    let n = self.pick(1000);
                    self.out.push_str(&n.to_string());
                }
                Item::Str => self.one_of(STRINGS),
            }
        }
    }
}

fn run(data: &[u8]) {
    let mut u = Unstructured::new(data);
    let mut g = Gen {
        u: &mut u,
        out: String::new(),
    };
    g.gen(START, MAX_DEPTH, 0);
    let sql = g.out;

    // Parsing must never panic. Only a single parseable statement is checked
    // against the round-trip oracle (matching `sql_roundtrip`).
    let Ok(parsed) = parse_statements(&sql) else {
        return;
    };
    if parsed.len() != 1 {
        return;
    }
    let mut ast = parsed.into_iter().next().unwrap().ast;
    normalize(&mut ast);
    check_pretty(&sql, &ast);
    check_display(&ast);
}

fuzz_target!(|data: &[u8]| {
    run(data);
});
