// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: the SQL round-trip oracle. `parse -> AstDisplay/pretty ->
//! reparse` must preserve the AST. It consumes the libFuzzer byte stream as a
//! sequence of *grammar choices* (not raw text, which mostly fails to parse so
//! the round-trip is a no-op) and, on most runs, emits a syntactically plausible
//! SQL query that reaches deep into the query/expression/DDL grammar. Byte
//! mutation rarely assembles a valid instance of those print paths. Coverage
//! feedback steers the byte→choice mapping.
//!
//! A minority of runs (`Gen::soup`) instead emit *full-vocabulary soup*: a
//! random sequence drawn from every keyword, identifier, literal form, operator/
//! symbol, and raw characters. Almost none parses, but the rare soup that does
//! reaches query shapes the hand-written productions never assemble (this is how
//! the `COPY into …` round-trip bug surfaced: a bare `into` relation re-lexed as
//! `COPY`'s optional `INTO` keyword), and the rest exercises the parser/lexer
//! error paths (which must never panic/hang). Same byte→choice model, same
//! oracle, so soup and structured generation share one target.
//!
//! The generator is deliberately biased toward the constructs that stress the
//! printer's quoting and parenthesization logic, which is where every round-trip
//! bug we've found lives: identifiers/function names that collide with keywords
//! (forcing quoting), varied operator-precedence groupings (forcing
//! re-parenthesization), casts under unary operators, quantified comparisons
//! against both a subquery (`op ANY/ALL/SOME (SELECT …)`) and an array value
//! (`op ANY/ALL/SOME (ARRAY[…])`, the distinct `Expr::AnyExpr`/`AllExpr`
//! nodes), the special-grammar forms (`EXTRACT`, `POSITION`, `SUBSTRING`,
//! `TRIM`, `CAST`, `MAP`, `LIST[…]`/`LIST(SELECT …)`, `ARRAY`, `ROW`), the
//! dedicated-node forms `GREATEST`/`LEAST` (`Expr::HomogenizingFunction`),
//! `NULLIF` (`Expr::NullIf`), `NORMALIZE` (a re-quoted `normalize(...)` call),
//! `LIKE`/`ILIKE … ESCAPE <expr>` (the optional `escape` of `Expr::Like`), the
//! `$N` placeholder (`Expr::Parameter`), window frames, CTEs, and set
//! operations.
//!
//! It also assembles the **connector DDL family** the query-centric arms can't
//! reach (`connector_ddl`): `CREATE TABLE … FROM SOURCE` (the subsource
//! REFERENCE / column-or-constraint spec / `WITH (TEXT COLUMNS|EXCLUDE
//! COLUMNS|DETAILS …)`), `CREATE SOURCE` (load-generator and connection-backed
//! Kafka with FORMAT/ENVELOPE), `CREATE SINK … INTO KAFKA`, `CREATE CONNECTION`,
//! `CREATE … FROM WEBHOOK`, and the surrounding `SHOW CREATE` / `EXPLAIN … FOR`
//! / `ALTER SOURCE … ADD SUBSOURCE` / `VALIDATE CONNECTION` forms. These are the
//! option-heavy printers (formats, envelopes, source/sink options) the
//! structured generator otherwise never exercises. Only round-trip-proven
//! shapes are emitted. The generator varies the names and embedded queries.
//! (Note this is the `CREATE TABLE … FROM SOURCE` family from database-issues
//! #10034: that bug round-trips through parse/print cleanly, its defect is in
//! re-execution, so it is out of reach for *any* parser-level oracle. The
//! statement's printer is still covered for the bugs this target can catch.)
//!
//! Each generated query that parses is checked against two oracles, both
//! comparing the normalized AST structurally: `check_pretty` (full-AST equality
//! through `pretty_str_simple`, verified at two line widths so wrapping can't
//! perturb the AST) and `check_display` (full-AST equality through `AstDisplay`).
//! Structural comparison is required because stable-string rendering is not
//! injective: a printer precedence bug can map two different trees to the same
//! text, so comparing re-rendered strings would pass a changed statement.

#![no_main]

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{AstInfo, Expr, Raw, Statement};
use mz_sql_parser::parser::parse_statements;
use mz_sql_pretty::pretty_str_simple;

// ---------------------------------------------------------------------------
// Round-trip oracle (mirrors parse_pretty_roundtrip / parse_display_roundtrip).
// ---------------------------------------------------------------------------

/// Strip syntactic noise so AST equality reflects *semantic* fidelity:
/// `Declare`/`Prepare` capture raw text, and `Expr::Nested` records parens that
/// the printer is free to add or drop. See `parse_pretty_roundtrip` for detail.
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
    // The line width must not affect the AST: wrapping is purely cosmetic, so
    // both a wide layout (everything on one line) and a narrow one (maximally
    // wrapped) must reparse to the same AST. Checking two widths catches a
    // wrapping that drops/adds a token only on the path it takes at one width.
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
    // One statement must print as exactly one statement. Any other count means
    // the printer emitted text that reparses to a different number of
    // statements, itself a round-trip violation, so it must fail rather than
    // silently pass.
    assert_eq!(
        reparsed.len(),
        1,
        "AstDisplay output reparsed to {} statements, expected 1\ndisplayed: {displayed:?}",
        reparsed.len(),
    );
    let mut reparsed_ast = reparsed.into_iter().next().unwrap().ast;
    // Normalize the reparse too (mirroring `check_pretty`): the parser may
    // re-insert a semantically-redundant `Expr::Nested` (e.g. it parenthesizes a
    // cast under unary minus), and per the oracle's contract those parens are
    // free to add or drop. Stripping them from both sides leaves a genuine
    // structural drift to still trip the assert.
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
// Grammar generator.
// ---------------------------------------------------------------------------

/// Identifiers, weighted toward the cases that exercise the printer's quoting
/// decision: bare names, quoted keyword collisions (parse as identifiers but
/// the printer must keep them quoted or they re-lex as keywords), and names
/// that only round-trip when quoted (case, spaces, dots, embedded quotes,
/// leading digit).
const IDENTS: &[&str] = &[
    "a",
    "b",
    "c",
    "x",
    "y",
    "z",
    "t",
    "u",
    "col",
    "foo",
    "bar",
    "t1",
    "t2",
    "\"some\"",
    "\"any\"",
    "\"all\"",
    "\"select\"",
    "\"from\"",
    "\"where\"",
    "\"map\"",
    "\"list\"",
    "\"array\"",
    "\"position\"",
    "\"trim\"",
    "\"as\"",
    "\"order\"",
    "\"group\"",
    "\"row\"",
    "\"exists\"",
    "\"coalesce\"",
    "\"in\"",
    "\"Mixed\"",
    "\"with space\"",
    "\"a.b\"",
    "\"1col\"",
    "\"qu\"\"ote\"",
    "\"\"",
];

/// Every keyword the lexer knows, read at compile time from the lexer's keyword
/// list (so it stays complete as keywords are added). `ident()` emits these
/// *bare*: whether the printer keeps a keyword-as-identifier unambiguous
/// (quoting it, or emitting a disambiguating keyword) is the round-trip property
/// nearly every printer bug we've found violated, such as `AS`, `LIST`,
/// `ANY`/`ALL`/`SOME`, and the `DEALLOCATE … PREPARE` finding. The pre-baked
/// `IDENTS` only has *quoted* collisions, which are already safe by
/// construction, so they never exercise that decision.
fn keywords() -> &'static [&'static str] {
    static KW: OnceLock<Vec<&'static str>> = OnceLock::new();
    KW.get_or_init(|| {
        include_str!("../../../sql-lexer/src/keywords.txt")
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect()
    })
}

/// Bare scalar type names (no recursive array/list/map wrappers).
const SCALAR_TYPES: &[&str] = &[
    "int2",
    "int4",
    "int8",
    "integer",
    "smallint",
    "bigint",
    "real",
    "double precision",
    "float",
    "numeric",
    "numeric(10, 2)",
    "decimal(5)",
    "boolean",
    "bool",
    "text",
    "varchar(10)",
    "char(5)",
    "bytea",
    "date",
    "time",
    "timestamp",
    "timestamp(3)",
    "timestamptz",
    "interval",
    "jsonb",
    "uuid",
    "oid",
];

/// Scalar/value literals, including ones with tricky lexing (escaped quote,
/// leading/trailing dot, exponent) and typed literals.
const VALUES: &[&str] = &[
    "0",
    "1",
    "2",
    "42",
    "3.14",
    "0.5",
    ".5",
    "1.",
    "1e10",
    "1.5e-3",
    "'a'",
    "''",
    "'foo bar'",
    "'it''s'",
    "'%'",
    "'_'",
    "'100'",
    "true",
    "false",
    "null",
    "INTERVAL '1' DAY",
    "INTERVAL '1-2' YEAR TO MONTH",
    "INTERVAL '1 2:03:04' DAY TO SECOND",
    "DATE '2020-01-01'",
    "TIMESTAMP '2020-01-01 00:00:00'",
    "b'010'",
    "x'deadbeef'",
];

/// Binary operators spanning precedence levels and the custom jsonb/range ops.
const BIN_OPS: &[&str] = &[
    "+", "-", "*", "/", "%", "||", "=", "<>", "!=", "<", ">", "<=", ">=", "AND", "OR", "->", "->>",
    "#>", "#>>", "@>", "<@", "#", "&", "|", "<<", ">>", "~", "~*", "!~", "!~*",
];

/// "Noise" tokens spliced into output at random points (see `Gen::maybe_noise`).
/// Mostly punctuation/operators/odd literals plus comment and dollar-quote
/// starts: emitting them, alongside bare keywords and arbitrary characters,
/// produces mostly-invalid SQL that exercises the parser/lexer's error paths
/// (which must never panic on any input), and occasionally a valid-but-unusual
/// statement the structured grammar wouldn't assemble.
const NOISE: &[&str] = &[
    "(", ")", "[", "]", "{", "}", ",", ";", ".", "::", ":", "*", "@", "?", "!", "\\", "\"", "'",
    "->", "->>", "#>>", "||", "<>", "=>", "%", "~", "&", "|", "$1", "$$", "''", "\"\"", "/*", "*/",
    "--", "  ", "\t", "\n", "1e999", "0x1", "-0", ".", "e", "E'\\x41'", "U&'\\0041'",
];

// The parser's AST source, embedded so the fuzzed connector option space stays
// complete as options are ADDED, the same self-syncing trick `keywords()` uses
// for the lexer's keyword list. A hardcoded option-name snapshot would silently
// rot: a new `KafkaSinkConfigOptionName` variant would go un-fuzzed until
// someone remembered to update the fuzzer. `option_names` instead reads each
// enum's display phrases straight from here at first use.
const DDL_SRC: &str = include_str!("../../src/ast/defs/ddl.rs");
const STMT_SRC: &str = include_str!("../../src/ast/defs/statement.rs");

/// The display phrases of every variant of the option-name enum `enum_name`,
/// scraped from `impl AstDisplay for <enum_name>` in the embedded AST source
/// (every string literal in that impl body is an option phrase). Returns
/// 'static slices into the source, the phrases aren't allocated. New options
/// are picked up automatically. An option whose value grammar isn't the generic
/// one just falls through `config_option`'s generic arm (a harmless no-op parse
/// for the special-grammar ones, a valid clause for the rest) until a dedicated
/// case is added here.
fn option_names(enum_name: &'static str) -> &'static [&'static str] {
    static CACHE: OnceLock<Mutex<BTreeMap<&'static str, &'static [&'static str]>>> =
        OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(BTreeMap::new()));
    if let Some(v) = cache.lock().unwrap().get(enum_name) {
        return v;
    }
    let v: &'static [&'static str] =
        Box::leak(extract_option_phrases(enum_name).into_boxed_slice());
    cache.lock().unwrap().insert(enum_name, v);
    v
}

fn extract_option_phrases(enum_name: &str) -> Vec<&'static str> {
    let needle = format!("impl AstDisplay for {enum_name}");
    for src in [DDL_SRC, STMT_SRC] {
        let Some(start) = src.find(&needle) else {
            continue;
        };
        // Reject a match that's only a prefix of a longer enum name.
        if !src[start + needle.len()..].starts_with([' ', '\n']) {
            continue;
        }
        let Some(rel) = src[start..].find('{') else {
            continue;
        };
        let brace = start + rel;
        // Brace-match to the end of the `impl … { … }` body.
        let mut depth = 0i32;
        let mut end = brace;
        for (k, c) in src[brace..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = brace + k;
                        break;
                    }
                }
                _ => {}
            }
        }
        let mut out = Vec::new();
        let mut rest = &src[brace..=end];
        while let Some(q) = rest.find('"') {
            let tail = &rest[q + 1..];
            match tail.find('"') {
                Some(e) => {
                    out.push(&tail[..e]);
                    rest = &tail[e + 1..];
                }
                None => break,
            }
        }
        return out;
    }
    Vec::new()
}

/// Recursion depth budget for a generated query.
const MAX_DEPTH: u32 = 5;
/// Hard cap on generated query length. Structure-aware generation can otherwise
/// blow up: depth recursion compounds through subqueries/derived tables, and a
/// 2GB string OOMs the fuzzer. Once we cross this, `expr`/`from_item` collapse
/// to leaves so output overshoots by at most one in-flight expansion.
const MAX_OUTPUT: usize = 2000;

struct Gen<'a, 'u> {
    u: &'u mut Unstructured<'a>,
    out: String,
}

impl<'a, 'u> Gen<'a, 'u> {
    fn budget(&self) -> bool {
        self.out.len() < MAX_OUTPUT
    }

    /// A uniform choice in `0..n` (0 when the byte stream is exhausted, so the
    /// generator deterministically winds down to leaves at the end of input).
    fn pick(&mut self, n: usize) -> usize {
        if n <= 1 {
            return 0;
        }
        self.u
            .int_in_range(0..=(n as u64 - 1))
            .map(|v| v as usize)
            .unwrap_or(0)
    }

    /// `true` with probability `num/den` (false on exhaustion).
    fn chance(&mut self, num: u32, den: u32) -> bool {
        self.u.ratio(num, den).unwrap_or(false)
    }

    fn one_of(&mut self, opts: &[&str]) {
        let i = self.pick(opts.len());
        self.out.push_str(opts[i]);
    }

    fn ident(&mut self) {
        // 1 in 5: a *bare* keyword used as an identifier. The parser accepts the
        // non-reserved ones, and whether the printer keeps them unambiguous is
        // exactly the property under test (see `keywords`). Reserved keywords
        // just make the statement fail to parse, a harmless no-op.
        if self.chance(1, 5) {
            let kw = keywords();
            let i = self.pick(kw.len());
            self.out.push_str(kw[i]);
        } else {
            self.one_of(IDENTS);
        }
    }

    /// Splice a bit of "noise" into the output: a noise token, a bare keyword,
    /// or a short run of arbitrary characters. See `NOISE`.
    fn inject_noise(&mut self) {
        match self.pick(3) {
            0 => self.one_of(NOISE),
            1 => {
                let kw = keywords();
                let i = self.pick(kw.len());
                self.out.push_str(kw[i]);
            }
            _ => {
                // A short run of arbitrary characters. `data: &str` only ever
                // delivers valid UTF-8, so `char` (a scalar value) is the right
                // granularity for "random bytes" the parser could actually see.
                let n = 1 + self.pick(4);
                for _ in 0..n {
                    if let Ok(c) = char::arbitrary(self.u) {
                        self.out.push(c);
                    }
                }
            }
        }
    }

    /// With low probability, splice noise into the output. Called at generation
    /// boundaries so most statements stay valid (and exercise the round trip)
    /// while a steady minority are perturbed (and exercise parser robustness).
    fn maybe_noise(&mut self) {
        if self.budget() && self.chance(1, 12) {
            self.inject_noise();
        }
    }

    /// Full-vocabulary "soup": a random sequence drawn from every keyword, the
    /// identifier set, every literal form, the operators/symbols, and raw
    /// characters (via `inject_noise`). Almost none parses, but the rare soup
    /// that does reaches query shapes the structured productions never assemble
    /// (e.g. `COPY into …`, where a bare `into` relation re-lexed as the optional
    /// `INTO` keyword), and the rest stresses the parser/lexer error paths. The
    /// fraction of soup runs is tuned low (see `generate`) so the structured
    /// path still does the bulk of the round-trip work.
    fn soup(&mut self) {
        let n = 1 + self.pick(40);
        for _ in 0..n {
            if !self.budget() {
                break;
            }
            match self.pick(8) {
                // Keywords are the bulk of the "words" and the richest
                // parser-dispatch surface, so weight them up.
                0..=2 => {
                    let kw = keywords();
                    let i = self.pick(kw.len());
                    self.out.push_str(kw[i]);
                }
                3 => self.ident(),
                4 => self.value(),
                // Structural punctuation and operators.
                5 => self.one_of(&["(", ")", ",", ".", "::", ":", ";", "[", "]"]),
                6 => self.one_of(BIN_OPS),
                // Noise tokens, bare keywords, or raw characters.
                _ => self.inject_noise(),
            }
            // Usually a space, sometimes nothing, to test adjacent-token lexing.
            if self.pick(4) != 0 {
                self.out.push(' ');
            }
        }
    }

    /// A possibly-qualified name: `a`, `a.b`, or `a.b.c`.
    fn qualified_name(&mut self) {
        let parts = 1 + self.pick(3);
        for i in 0..parts {
            if i > 0 {
                self.out.push('.');
            }
            self.ident();
        }
    }

    fn data_type(&mut self, depth: u32) {
        if depth > 0 && self.chance(1, 3) {
            // Recursive array / list / map wrappers.
            match self.pick(3) {
                0 => {
                    self.data_type(depth - 1);
                    self.out.push_str("[]");
                }
                1 => {
                    self.data_type(depth - 1);
                    self.out.push_str(" list");
                }
                _ => {
                    self.out.push_str("map[text => ");
                    self.data_type(depth - 1);
                    self.out.push(']');
                }
            }
        } else {
            self.one_of(SCALAR_TYPES);
        }
    }

    /// An expression, optionally wrapped in parentheses to force a non-default
    /// grouping (the parens become `Nested`, which the oracle strips, so what's
    /// actually under test is whether the printer reproduces the grouping).
    fn expr_grouped(&mut self, depth: u32) {
        if depth > 0 && self.chance(2, 5) {
            self.out.push('(');
            self.expr(depth);
            self.out.push(')');
        } else {
            self.expr(depth);
        }
    }

    fn expr(&mut self, depth: u32) {
        if depth == 0 || !self.budget() {
            self.leaf_expr();
            return;
        }
        let d = depth - 1;
        match self.pick(19) {
            // Binary operator chain.
            0 | 1 => {
                self.expr_grouped(d);
                self.out.push(' ');
                self.one_of(BIN_OPS);
                self.out.push(' ');
                self.expr_grouped(d);
            }
            // Unary operators (incl. the negated-cast precedence trap).
            2 => {
                self.one_of(&["-", "+", "NOT ", "~"]);
                self.expr_grouped(d);
            }
            // `::` cast.
            3 => {
                self.expr_grouped(d);
                self.out.push_str("::");
                self.data_type(2);
            }
            // CAST(e AS t).
            4 => {
                self.out.push_str("CAST(");
                self.expr(d);
                self.out.push_str(" AS ");
                self.data_type(2);
                self.out.push(')');
            }
            // IS [NOT] {NULL|TRUE|FALSE|UNKNOWN} / IS [NOT] DISTINCT FROM.
            5 => {
                self.expr_grouped(d);
                self.out.push_str(" IS ");
                if self.chance(1, 2) {
                    self.out.push_str("NOT ");
                }
                if self.chance(1, 2) {
                    self.one_of(&["NULL", "TRUE", "FALSE", "UNKNOWN"]);
                } else {
                    self.out.push_str("DISTINCT FROM ");
                    self.expr_grouped(d);
                }
            }
            // BETWEEN.
            6 => {
                self.expr_grouped(d);
                if self.chance(1, 3) {
                    self.out.push_str(" NOT");
                }
                self.out.push_str(" BETWEEN ");
                self.expr_grouped(d);
                self.out.push_str(" AND ");
                self.expr_grouped(d);
            }
            // [NOT] LIKE / ILIKE [ESCAPE <expr>] / SIMILAR TO.
            7 => {
                self.expr_grouped(d);
                if self.chance(1, 3) {
                    self.out.push_str(" NOT");
                }
                // SIMILAR TO has no ESCAPE in this AST. LIKE/ILIKE
                // (`Expr::Like`) carries an optional `escape` the printer emits.
                let kind = self.pick(3);
                self.out
                    .push_str([" LIKE ", " ILIKE ", " SIMILAR TO "][kind]);
                self.expr_grouped(d);
                if kind != 2 && self.chance(1, 2) {
                    self.out.push_str(" ESCAPE ");
                    self.expr_grouped(d);
                }
            }
            // Function call (plain / aggregate / window).
            8 | 9 => self.func_call(d),
            // CASE.
            10 => self.case_expr(d),
            // Quantified comparison: e op {ANY|ALL|SOME} (...). A subquery in
            // the parens yields `Expr::AnySubquery`/`AllSubquery`. A single
            // non-subquery expression yields the array-valued
            // `Expr::AnyExpr`/`AllExpr` (a distinct printer branch). Both LHS
            // forms go through `write_quantified_left`'s paren logic.
            11 => {
                self.expr_grouped(d);
                self.out.push(' ');
                self.one_of(&["=", "<>", "<", ">", "<=", ">="]);
                self.out.push(' ');
                self.one_of(&["ANY", "ALL", "SOME"]);
                self.out.push_str(" (");
                if self.chance(1, 2) {
                    self.query(d);
                } else if self.chance(1, 2) {
                    // Array-shaped operand keeps the comparison well-typed.
                    self.out.push_str("ARRAY[");
                    self.expr_list(d, 1, 3);
                    self.out.push(']');
                } else {
                    self.expr(d);
                }
                self.out.push(')');
            }
            // [NOT] IN (list) or IN (subquery), and EXISTS / scalar subquery.
            12 => {
                if self.chance(1, 2) {
                    self.expr_grouped(d);
                    if self.chance(1, 3) {
                        self.out.push_str(" NOT");
                    }
                    self.out.push_str(" IN (");
                    if self.chance(1, 2) {
                        self.query(d);
                    } else {
                        self.expr_list(d, 1, 3);
                    }
                    self.out.push(')');
                } else if self.chance(1, 2) {
                    self.out.push_str("EXISTS (");
                    self.query(d);
                    self.out.push(')');
                } else {
                    self.out.push('(');
                    self.query(d);
                    self.out.push(')');
                }
            }
            // Collection literals: ARRAY[...]/ARRAY(subquery), LIST[...], MAP[k=>v], ROW(...).
            13 => self.collection_expr(d),
            // Special grammar forms.
            14 => self.special_form(d),
            // `COLLATE`, postfix form (`<expr> COLLATE <name>`), binds tightly.
            15 => {
                self.expr_grouped(d);
                self.out.push_str(" COLLATE ");
                self.ident();
            }
            // `AT TIME ZONE`, postfix form, desugars to the `timezone(...)` function.
            16 => {
                self.expr_grouped(d);
                self.out.push_str(" AT TIME ZONE ");
                self.expr_grouped(d);
            }
            // Namespaced operator `OPERATOR(schema.op)`, distinct display path.
            17 => {
                self.expr_grouped(d);
                self.out.push_str(" OPERATOR(pg_catalog.");
                self.one_of(&["+", "-", "*", "=", "<", ">", "@>", "->"]);
                self.out.push_str(") ");
                self.expr_grouped(d);
            }
            // Subscript / field access / tuple.
            _ => match self.pick(4) {
                0 => {
                    self.expr_grouped(d);
                    self.out.push('[');
                    self.value();
                    if self.chance(1, 2) {
                        self.out.push(':');
                        self.value();
                    }
                    self.out.push(']');
                }
                1 => {
                    // Subscript with full-expression bounds (not just literals).
                    self.expr_grouped(d);
                    self.out.push('[');
                    self.expr(d);
                    if self.chance(1, 2) {
                        self.out.push(':');
                        self.expr(d);
                    }
                    self.out.push(']');
                }
                2 => {
                    self.out.push('(');
                    self.expr(d);
                    self.out.push_str(").");
                    self.ident();
                }
                _ => {
                    self.out.push('(');
                    self.expr_list(d, 2, 3);
                    self.out.push(')');
                }
            },
        }
    }

    fn leaf_expr(&mut self) {
        match self.pick(4) {
            0 => self.qualified_name(),
            1 => self.value(),
            // `$N` placeholder, `Expr::Parameter`, with its own `${n}` printer.
            2 => {
                let n = 1 + self.pick(9);
                self.out.push('$');
                self.out.push_str(&n.to_string());
            }
            _ => {
                self.out.push('*');
                // `*` alone is only valid as a projection, fall back to a column
                // so a leaf is always a valid scalar.
                self.out.pop();
                self.qualified_name();
            }
        }
    }

    fn value(&mut self) {
        self.one_of(VALUES);
    }

    /// A string literal, weighted toward the lexing/escaping edge cases (empty,
    /// embedded escaped quote, embedded double quote, percent) so connector
    /// option values that must be re-escaped on display are exercised, the
    /// class of the CSR `MESSAGE 'a''b'` round-trip finding.
    fn string_value(&mut self) {
        self.one_of(&[
            "'t'",
            "''",
            "'foo bar'",
            "'it''s'",
            "'a\"b'",
            "'%'",
            "'1s'",
            "'localhost:9092'",
        ]);
    }

    fn expr_list(&mut self, depth: u32, min: usize, max: usize) {
        let n = min + self.pick(max - min + 1);
        for i in 0..n {
            if i > 0 {
                self.out.push_str(", ");
            }
            self.expr(depth);
        }
    }

    fn func_call(&mut self, depth: u32) {
        // Names that collide with special grammar / quantifier keywords stress
        // the printer's disambiguating quoting.
        self.one_of(&[
            "count",
            "sum",
            "max",
            "min",
            "abs",
            "coalesce",
            "\"some\"",
            "\"any\"",
            "\"coalesce\"",
            "\"position\"",
            "\"trim\"",
            "\"array\"",
            "\"row\"",
            "generate_series",
            "lower",
            "f",
        ]);
        self.out.push('(');
        if self.chance(1, 5) {
            self.out.push('*');
        } else {
            if self.chance(1, 5) {
                self.out.push_str("DISTINCT ");
            }
            self.expr_list(depth, 0, 3);
            if self.chance(1, 6) {
                self.out.push_str(" ORDER BY ");
                self.expr(depth);
            }
        }
        self.out.push(')');
        if self.chance(1, 8) {
            // Ordered-set aggregate: `f(args) WITHIN GROUP (ORDER BY …)`.
            self.out.push_str(" WITHIN GROUP (ORDER BY ");
            self.expr(depth);
            if self.chance(1, 2) {
                self.one_of(&[" ASC", " DESC"]);
            }
            self.out.push(')');
        }
        if self.chance(1, 6) {
            self.out.push_str(" FILTER (WHERE ");
            self.expr(depth);
            self.out.push(')');
        }
        if self.chance(1, 4) {
            if self.chance(1, 3) {
                // A named-window reference (`OVER w`), resolves against a
                // `WINDOW` clause. Parses fine on its own for round-trip.
                self.out.push_str(" OVER ");
                self.ident();
            } else {
                self.window_spec(depth);
            }
        }
    }

    fn window_spec(&mut self, depth: u32) {
        self.out.push_str(" OVER ");
        self.window_def(depth);
    }

    fn window_def(&mut self, depth: u32) {
        self.out.push('(');
        if self.chance(1, 2) {
            self.out.push_str("PARTITION BY ");
            self.expr_list(depth, 1, 2);
            self.out.push(' ');
        }
        if self.chance(2, 3) {
            self.out.push_str("ORDER BY ");
            self.expr(depth);
            if self.chance(1, 2) {
                self.one_of(&[" ASC", " DESC"]);
            }
            if self.chance(1, 2) {
                self.one_of(&[" NULLS FIRST", " NULLS LAST"]);
            }
            // Frame.
            if self.chance(1, 2) {
                self.out.push(' ');
                self.one_of(&["ROWS", "RANGE", "GROUPS"]);
                self.out.push_str(" BETWEEN ");
                self.frame_bound();
                self.out.push_str(" AND ");
                self.frame_bound();
            }
        }
        self.out.push(')');
    }

    fn frame_bound(&mut self) {
        match self.pick(5) {
            0 => self.out.push_str("UNBOUNDED PRECEDING"),
            1 => self.out.push_str("UNBOUNDED FOLLOWING"),
            2 => self.out.push_str("CURRENT ROW"),
            3 => {
                self.value();
                self.out.push_str(" PRECEDING");
            }
            _ => {
                self.value();
                self.out.push_str(" FOLLOWING");
            }
        }
    }

    fn case_expr(&mut self, depth: u32) {
        self.out.push_str("CASE");
        // Optional operand (simple CASE).
        if self.chance(1, 2) {
            self.out.push(' ');
            self.expr(depth);
        }
        let arms = 1 + self.pick(2);
        for _ in 0..arms {
            self.out.push_str(" WHEN ");
            self.expr(depth);
            self.out.push_str(" THEN ");
            self.expr(depth);
        }
        if self.chance(1, 2) {
            self.out.push_str(" ELSE ");
            self.expr(depth);
        }
        self.out.push_str(" END");
    }

    fn collection_expr(&mut self, depth: u32) {
        match self.pick(4) {
            0 => {
                if self.chance(1, 2) {
                    self.out.push_str("ARRAY[");
                    self.expr_list(depth, 0, 3);
                    self.out.push(']');
                } else {
                    self.out.push_str("ARRAY(");
                    self.query(depth);
                    self.out.push(')');
                }
            }
            1 => {
                if self.chance(1, 2) {
                    self.out.push_str("LIST[");
                    self.expr_list(depth, 0, 3);
                    self.out.push(']');
                } else {
                    // `LIST(<subquery>)`, `Expr::ListSubquery`, the subquery
                    // sibling of the `LIST[...]` literal.
                    self.out.push_str("LIST(");
                    self.query(depth);
                    self.out.push(')');
                }
            }
            2 => {
                self.out.push_str("MAP[");
                let n = self.pick(3);
                for i in 0..n {
                    if i > 0 {
                        self.out.push_str(", ");
                    }
                    self.value();
                    self.out.push_str(" => ");
                    self.expr(depth);
                }
                self.out.push(']');
            }
            _ => {
                self.out.push_str("ROW(");
                self.expr_list(depth, 0, 3);
                self.out.push(')');
            }
        }
    }

    fn special_form(&mut self, depth: u32) {
        match self.pick(9) {
            0 => {
                self.out.push_str("EXTRACT(");
                self.one_of(&["YEAR", "MONTH", "DAY", "HOUR", "EPOCH"]);
                self.out.push_str(" FROM ");
                self.expr(depth);
                self.out.push(')');
            }
            1 => {
                self.out.push_str("POSITION(");
                self.expr(depth);
                self.out.push_str(" IN ");
                self.expr(depth);
                self.out.push(')');
            }
            2 => {
                self.out.push_str("SUBSTRING(");
                self.expr(depth);
                self.out.push_str(" FROM ");
                self.value();
                if self.chance(1, 2) {
                    self.out.push_str(" FOR ");
                    self.value();
                }
                self.out.push(')');
            }
            3 => {
                self.out.push_str("TRIM(");
                self.one_of(&["", "LEADING ", "TRAILING ", "BOTH "]);
                self.expr(depth);
                if self.chance(1, 2) {
                    self.out.push_str(" FROM ");
                    self.expr(depth);
                }
                self.out.push(')');
            }
            4 => {
                self.out.push_str("COALESCE(");
                self.expr_list(depth, 1, 3);
                self.out.push(')');
            }
            // GREATEST / LEAST, `Expr::HomogenizingFunction`, a dedicated AST
            // node distinct from COALESCE with its own printer branch.
            5 => {
                self.one_of(&["GREATEST(", "LEAST("]);
                self.expr_list(depth, 1, 3);
                self.out.push(')');
            }
            // NULLIF(a, b), `Expr::NullIf`, also a dedicated node, not a call.
            6 => {
                self.out.push_str("NULLIF(");
                self.expr(depth);
                self.out.push_str(", ");
                self.expr(depth);
                self.out.push(')');
            }
            // NORMALIZE(e [, FORM]) desugars to a `normalize(...)` function
            // call whose name the printer must re-quote to avoid re-triggering
            // the NORMALIZE special grammar.
            _ => {
                self.out.push_str("NORMALIZE(");
                self.expr(depth);
                if self.chance(1, 2) {
                    self.out.push_str(", ");
                    self.one_of(&["NFC", "NFD", "NFKC", "NFKD"]);
                }
                self.out.push(')');
            }
        }
    }

    // --- Statement structure -----------------------------------------------

    /// A top-level statement. Mostly bare queries (the richest surface), but
    /// also the statement forms that wrap a query, exercising those statements'
    /// own `AstDisplay`/pretty paths (`CREATE [MATERIALIZED] VIEW`, `INSERT`,
    /// `EXPLAIN`, `SUBSCRIBE`, `DECLARE … CURSOR`) and the row-mutation DML.
    fn statement(&mut self, depth: u32) {
        self.maybe_noise();
        // ~1/4: the connector DDL family. CREATE {SOURCE,SINK,CONNECTION},
        // CREATE TABLE … FROM SOURCE, and the SHOW CREATE / EXPLAIN / ALTER /
        // VALIDATE forms around them. These option-heavy statements are the
        // richest printer surface the query-centric arms below never reach.
        if self.chance(1, 4) {
            self.connector_ddl(depth);
            self.maybe_noise();
            return;
        }
        // A third of the remainder: a non-query statement form (DDL, the
        // prepared-statement protocol, cursors, session commands), so the
        // statement-level printers, and bare-keyword names in their special
        // positions, get exercised too, not just queries.
        if self.chance(1, 3) {
            self.rare_statement(depth);
            self.maybe_noise();
            return;
        }
        match self.pick(15) {
            0..=3 => self.query(depth),
            10 => {
                // CREATE TABLE exercises the column-def + data-type printers.
                self.out.push_str("CREATE TABLE ");
                self.ident();
                self.out.push_str(" (");
                let cols = 1 + self.pick(3);
                for i in 0..cols {
                    if i > 0 {
                        self.out.push_str(", ");
                    }
                    self.ident();
                    self.out.push(' ');
                    self.data_type(2);
                    match self.pick(4) {
                        0 => self.out.push_str(" NOT NULL"),
                        1 => {
                            self.out.push_str(" DEFAULT ");
                            self.value();
                        }
                        _ => {}
                    }
                }
                self.out.push(')');
            }
            4 => {
                self.out.push_str("CREATE VIEW ");
                self.ident();
                self.out.push_str(" AS ");
                self.query(depth);
            }
            5 => {
                self.out.push_str("CREATE MATERIALIZED VIEW ");
                self.ident();
                self.out.push_str(" AS ");
                self.query(depth);
            }
            6 => {
                self.out.push_str("INSERT INTO ");
                self.qualified_name();
                self.out.push(' ');
                self.query(depth);
            }
            7 => {
                self.out.push_str("EXPLAIN ");
                self.query(depth);
            }
            8 => {
                self.out.push_str("SUBSCRIBE (");
                self.query(depth);
                self.out.push(')');
            }
            12 => {
                // `TABLE <name>`, a bare table query (distinct printer path).
                self.out.push_str("TABLE ");
                self.qualified_name();
            }
            13 => {
                // COPY … TO STDOUT, the COPY statement printer.
                self.out.push_str("COPY ");
                self.qualified_name();
                self.out.push_str(" TO STDOUT");
            }
            14 => {
                // SHOW COLUMNS, a simple SHOW-statement printer.
                self.out.push_str("SHOW COLUMNS FROM ");
                self.qualified_name();
            }
            _ => {
                // Row-mutation DML reuses the WHERE/SET expression generators.
                if self.chance(1, 2) {
                    self.out.push_str("DELETE FROM ");
                    self.qualified_name();
                    self.out.push_str(" WHERE ");
                    self.expr(depth);
                } else {
                    self.out.push_str("UPDATE ");
                    self.qualified_name();
                    self.out.push_str(" SET ");
                    self.ident();
                    self.out.push_str(" = ");
                    self.expr(depth);
                    self.out.push_str(" WHERE ");
                    self.expr(depth);
                }
            }
        }
    }

    // --- Connector DDL (sources / sinks / connections / table-from-source) ---

    /// A comma-separated list of identifiers (so bare-keyword and quoted
    /// collisions are exercised in option-list positions too).
    fn ident_list(&mut self, min: usize, max: usize) {
        let n = min + self.pick(max - min + 1);
        for i in 0..n {
            if i > 0 {
                self.out.push_str(", ");
            }
            self.ident();
        }
    }

    /// A simple, round-trip-proven `FORMAT <enc>` clause.
    fn format_clause(&mut self) {
        self.out.push_str("FORMAT ");
        self.one_of(&["BYTES", "TEXT", "JSON"]);
    }

    /// A generic option value spanning the `WithOptionValue` variants the
    /// permissive `parse_option_value` accepts: a string literal (with the
    /// escaping edge cases), a number, a bool, an ident/item-name, or
    /// `SECRET <name>`. Any of these round-trips for a config option.
    fn option_value(&mut self) {
        match self.pick(6) {
            0 | 1 => self.string_value(),
            2 => {
                let n = self.pick(1000);
                self.out.push_str(&n.to_string());
            }
            3 => self.one_of(&["true", "false"]),
            4 => self.ident(),
            _ => {
                self.out.push_str("SECRET ");
                self.qualified_name();
            }
        }
    }

    /// One `NAME [= value]` config-option clause. Most options take the generic
    /// `option_value`. The handful with a dedicated value grammar the parser
    /// dispatches by name are special-cased: `PARTITION BY` (an expression),
    /// `RETAIN HISTORY` (`FOR '<interval>'`), `TEXT`/`EXCLUDE COLUMNS` (an ident
    /// sequence), `BROKER` (a broker string), and the `… CONNECTION` / `SSH
    /// TUNNEL` object references (an item name).
    fn config_option(&mut self, name: &str) {
        self.out.push_str(name);
        match name {
            // PARTITION BY has two value grammars by context: Kafka sink takes a
            // full expression, table-from-source a generic value. A bare number
            // is valid in both, a richer expression exercises the Kafka-sink
            // printer (and just no-ops for table-from-source).
            "PARTITION BY" => {
                self.out.push_str(" = ");
                if self.chance(1, 2) {
                    let n = self.pick(100);
                    self.out.push_str(&n.to_string());
                } else {
                    self.expr(2);
                }
            }
            "RETAIN HISTORY" => self.out.push_str(" FOR '1s'"),
            "TEXT COLUMNS" | "EXCLUDE COLUMNS" => {
                self.out.push_str(" = (");
                self.ident_list(1, 3);
                self.out.push(')');
            }
            "BROKER" => self.out.push_str(" 'localhost:9092'"),
            "AWS CONNECTION" | "GCP CONNECTION" | "SSH TUNNEL" => {
                self.out.push_str(" = ");
                self.ident();
            }
            // Generic: usually a value, occasionally none (exercising the
            // value-less `value: None` print path, which omits the ` = …`).
            _ => {
                if self.chance(9, 10) {
                    self.out.push_str(" = ");
                    self.option_value();
                }
            }
        }
    }

    /// ` (opt, opt, …)` for an order-stable distinct subset of `names` (stable
    /// order ⇒ the printed option order matches, so it reparses identically).
    /// Emits nothing when the subset is empty unless `require_one` (some
    /// statements allow an absent list, others require a non-empty one).
    fn config_option_list(&mut self, names: &[&str], require_one: bool) {
        // Defensive: if source-scraping ever yields no names, emit no list
        // rather than indexing an empty slice below.
        if names.is_empty() {
            return;
        }
        let mut included: Vec<usize> = (0..names.len())
            .filter(|_| self.budget() && self.chance(1, 2))
            .collect();
        if included.is_empty() {
            if require_one {
                included.push(self.pick(names.len()));
            } else {
                return;
            }
        }
        self.out.push_str(" (");
        for (j, &i) in included.iter().enumerate() {
            if j > 0 {
                self.out.push_str(", ");
            }
            self.config_option(names[i]);
        }
        self.out.push(')');
    }

    /// The multi-output source table-selection clause.
    fn for_tables(&mut self) {
        match self.pick(3) {
            0 => self.out.push_str(" FOR ALL TABLES"),
            1 => {
                self.out.push_str(" FOR TABLES (");
                self.qualified_name();
                if self.chance(1, 2) {
                    self.out.push_str(", ");
                    self.qualified_name();
                }
                self.out.push(')');
            }
            _ => {
                self.out.push_str(" FOR SCHEMAS (");
                self.ident();
                self.out.push(')');
            }
        }
    }

    /// An optional `INCLUDE <metadata>, …` clause (distinct, order-stable).
    fn include_metadata(&mut self) {
        let opts = ["KEY", "PARTITION", "OFFSET", "TIMESTAMP", "HEADERS"];
        let included: Vec<usize> = (0..opts.len()).filter(|_| self.chance(1, 3)).collect();
        if included.is_empty() {
            return;
        }
        self.out.push_str(" INCLUDE ");
        for (j, &i) in included.iter().enumerate() {
            if j > 0 {
                self.out.push_str(", ");
            }
            self.out.push_str(opts[i]);
            // KEY / PARTITION can carry an optional alias.
            if (opts[i] == "KEY" || opts[i] == "PARTITION") && self.chance(1, 2) {
                self.out.push_str(" AS ");
                self.ident();
            }
        }
    }

    /// The option-heavy connector statements plus the SHOW CREATE / EXPLAIN /
    /// ALTER / VALIDATE forms around them. Every shape here is one the
    /// parser+printer round-trip is *proven* to preserve (it mirrors the
    /// canonical forms in `sql-parser/tests/testdata`). The generator varies the
    /// names (`ident`/`qualified_name`, so bare-keyword and quoted collisions
    /// are exercised in each position) and the embedded queries (`query`), which
    /// is where the novel coverage comes from.
    fn connector_ddl(&mut self, depth: u32) {
        match self.pick(9) {
            0 | 1 => self.create_table_from_source(),
            2 => self.create_source(),
            3 => self.create_sink(),
            4 => self.create_connection(),
            5 => {
                // CREATE … FROM WEBHOOK.
                self.out.push_str("CREATE SOURCE ");
                self.ident();
                self.out.push_str(" IN CLUSTER ");
                self.ident();
                self.out.push_str(" FROM WEBHOOK BODY FORMAT ");
                self.one_of(&["JSON", "BYTES", "TEXT"]);
            }
            6 => {
                // SHOW CREATE <object>.
                self.out.push_str("SHOW CREATE ");
                self.one_of(&[
                    "SOURCE ",
                    "SINK ",
                    "TABLE ",
                    "VIEW ",
                    "MATERIALIZED VIEW ",
                    "INDEX ",
                    "CONNECTION ",
                ]);
                self.qualified_name();
            }
            7 => {
                // EXPLAIN <plan> FOR <query>, the EXPLAIN-statement printer.
                self.out.push_str("EXPLAIN ");
                self.one_of(&["OPTIMIZED PLAN FOR ", "TIMESTAMP FOR "]);
                self.query(depth);
            }
            _ => {
                // ALTER SOURCE … ADD SUBSOURCE, and VALIDATE CONNECTION.
                if self.chance(1, 2) {
                    self.out.push_str("ALTER SOURCE ");
                    self.qualified_name();
                    self.out.push_str(" ADD SUBSOURCE ");
                    self.ident_list(1, 3);
                    if self.chance(1, 2) {
                        self.out.push_str(" WITH");
                        self.config_option_list(option_names("AlterSourceAddSubsourceOptionName"), true);
                    }
                } else {
                    self.out.push_str("VALIDATE CONNECTION ");
                    self.qualified_name();
                }
            }
        }
    }

    /// `CREATE TABLE … FROM SOURCE`, the subsource statement (database-issues
    /// #10034 family). Exercises the optional column-or-constraint spec, the
    /// `REFERENCE` external reference, and the `WITH (…)` purification options.
    fn create_table_from_source(&mut self) {
        self.out.push_str("CREATE TABLE ");
        if self.chance(1, 6) {
            self.out.push_str("IF NOT EXISTS ");
        }
        self.qualified_name();
        // Optional column / constraint spec. A typed column makes the spec
        // `Defined`, a bare column makes it `Named`. Both print and reparse.
        if self.chance(2, 3) {
            self.out.push('(');
            let cols = self.pick(3);
            // Columns are either all bare (`Named`) or all typed (`Defined`).
            // Mixing the two is a parse error ("cannot mix column definitions
            // and column names").
            let typed = self.chance(1, 2);
            let mut wrote = false;
            for _ in 0..cols {
                if wrote {
                    self.out.push_str(", ");
                }
                self.ident();
                if typed {
                    self.out.push(' ');
                    self.data_type(2);
                }
                wrote = true;
            }
            if self.chance(1, 3) {
                if wrote {
                    self.out.push_str(", ");
                }
                self.out.push_str("PRIMARY KEY (");
                self.ident();
                self.out.push(')');
                wrote = true;
            }
            // `()` is not valid, guarantee at least one element.
            if !wrote {
                self.ident();
            }
            self.out.push(')');
        }
        self.out.push_str(" FROM SOURCE ");
        self.qualified_name();
        if self.chance(3, 4) {
            self.out.push_str(" (REFERENCE = ");
            self.qualified_name();
            self.out.push(')');
        }
        if self.chance(1, 2) {
            self.out.push_str(" WITH");
            self.config_option_list(option_names("TableFromSourceOptionName"), true);
        }
    }

    /// `CREATE SOURCE` over every connector kind (load generator, Kafka,
    /// Postgres, MySQL, SQL Server), each with its full config-option space, and
    /// the source-level `WITH (…)` options.
    fn create_source(&mut self) {
        self.out.push_str("CREATE SOURCE ");
        if self.chance(1, 6) {
            self.out.push_str("IF NOT EXISTS ");
        }
        self.qualified_name();
        if self.chance(1, 2) {
            self.out.push_str(" IN CLUSTER ");
            self.ident();
        }
        match self.pick(5) {
            0 => {
                // Load generator, self-contained, needs no connection. Option
                // and generator-kind validity is a planning concern, any pairing
                // parses and round-trips.
                self.out.push_str(" FROM LOAD GENERATOR ");
                self.one_of(&[
                    "COUNTER",
                    "CLOCK",
                    "AUCTION",
                    "MARKETING",
                    "TPCH",
                    "KEY VALUE",
                ]);
                self.config_option_list(option_names("LoadGeneratorOptionName"), false);
            }
            1 => {
                // Kafka, connection-backed, with FORMAT / INCLUDE / ENVELOPE.
                self.out.push_str(" FROM KAFKA CONNECTION ");
                self.qualified_name();
                self.config_option_list(option_names("KafkaSourceConfigOptionName"), false);
                if self.chance(2, 3) {
                    self.out.push(' ');
                    self.format_clause();
                }
                self.include_metadata();
                if self.chance(1, 2) {
                    self.out.push_str(" ENVELOPE ");
                    self.one_of(&["NONE", "UPSERT", "DEBEZIUM"]);
                }
            }
            2 => {
                self.out.push_str(" FROM POSTGRES CONNECTION ");
                self.qualified_name();
                self.config_option_list(option_names("PgConfigOptionName"), true);
                self.for_tables();
            }
            3 => {
                self.out.push_str(" FROM MYSQL CONNECTION ");
                self.qualified_name();
                self.config_option_list(option_names("MySqlConfigOptionName"), false);
                self.for_tables();
            }
            _ => {
                self.out.push_str(" FROM SQL SERVER CONNECTION ");
                self.qualified_name();
                self.config_option_list(option_names("SqlServerConfigOptionName"), false);
                self.for_tables();
            }
        }
        if self.chance(1, 3) {
            self.out.push_str(" WITH");
            self.config_option_list(option_names("CreateSourceOptionName"), true);
        }
    }

    /// `CREATE SINK … INTO KAFKA`, the full sink config-option space, optional
    /// KEY, FORMAT, ENVELOPE, and the sink-level `WITH (…)` options.
    fn create_sink(&mut self) {
        self.out.push_str("CREATE SINK ");
        if self.chance(1, 6) {
            self.out.push_str("IF NOT EXISTS ");
        }
        // The sink name is optional in the grammar, emit it most of the time.
        if self.chance(3, 4) {
            self.qualified_name();
        }
        if self.chance(1, 2) {
            self.out.push_str(" IN CLUSTER ");
            self.ident();
        }
        self.out.push_str(" FROM ");
        self.qualified_name();
        self.out.push_str(" INTO KAFKA CONNECTION ");
        self.qualified_name();
        self.config_option_list(option_names("KafkaSinkConfigOptionName"), false);
        if self.chance(1, 3) {
            self.out.push_str(" KEY (");
            self.ident_list(1, 2);
            self.out.push(')');
        }
        self.out.push(' ');
        self.format_clause();
        if self.chance(1, 2) {
            self.out.push_str(" ENVELOPE ");
            self.one_of(&["UPSERT", "DEBEZIUM"]);
        }
        if self.chance(1, 3) {
            self.out.push_str(" WITH");
            self.config_option_list(option_names("CreateSinkOptionName"), true);
        }
    }

    /// `CREATE CONNECTION … TO <type> (…)` over the full connection option
    /// space. The parser's option grammar is unified across connection types
    /// (type/option compatibility is a planning concern), so any option subset
    /// parses and round-trips under any `TO <type>`.
    fn create_connection(&mut self) {
        self.out.push_str("CREATE CONNECTION ");
        if self.chance(1, 6) {
            self.out.push_str("IF NOT EXISTS ");
        }
        self.qualified_name();
        self.out.push_str(" TO ");
        self.one_of(&[
            "KAFKA",
            "CONFLUENT SCHEMA REGISTRY",
            "POSTGRES",
            "MYSQL",
            "SQL SERVER",
            "AWS",
            "SSH TUNNEL",
        ]);
        self.config_option_list(option_names("ConnectionOptionName"), true);
        if self.chance(1, 8) {
            self.out.push_str(" WITH (VALIDATE = ");
            self.one_of(&["true", "false"]);
            self.out.push(')');
        }
    }

    /// Non-query statement forms the query-centric `statement()` arm doesn't
    /// reach: the prepared-statement protocol, cursors, session/transaction
    /// commands, and assorted DDL. Names go through `ident()`/`qualified_name()`
    /// so a bare-keyword collision is tested in each statement's special
    /// position (where the optional keyword / clause makes quoting matter).
    fn rare_statement(&mut self, depth: u32) {
        match self.pick(38) {
            0 => {
                self.out.push_str("DEALLOCATE ");
                if self.chance(1, 2) {
                    self.out.push_str("PREPARE ");
                }
                if self.chance(1, 5) {
                    self.out.push_str("ALL");
                } else {
                    self.ident();
                }
            }
            1 => {
                self.out.push_str("PREPARE ");
                self.ident();
                self.out.push_str(" AS ");
                self.query(depth);
            }
            2 => {
                self.out.push_str("EXECUTE ");
                self.ident();
                if self.chance(1, 2) {
                    self.out.push_str(" (");
                    self.expr(depth);
                    self.out.push(')');
                }
            }
            3 => {
                self.out.push_str("DECLARE ");
                self.ident();
                self.out.push_str(" CURSOR FOR ");
                self.query(depth);
            }
            4 => {
                self.out.push_str("FETCH ");
                if self.chance(1, 2) {
                    self.out.push_str("ALL ");
                }
                self.out.push_str("FROM ");
                self.ident();
            }
            5 => {
                self.out.push_str("CLOSE ");
                self.ident();
            }
            6 => {
                self.out.push_str("SET ");
                self.ident();
                self.out.push_str(" TO ");
                self.value();
            }
            7 => {
                self.out.push_str("RESET ");
                self.ident();
            }
            8 => {
                self.out.push_str("SHOW ");
                self.ident();
            }
            9 => {
                self.out.push_str("DROP TABLE ");
                self.qualified_name();
            }
            10 => {
                self.out.push_str("DROP VIEW ");
                self.qualified_name();
            }
            11 => {
                self.out.push_str("COMMENT ON TABLE ");
                self.qualified_name();
                self.out.push_str(" IS 'c'");
            }
            12 => {
                self.out.push_str("GRANT SELECT ON TABLE ");
                self.qualified_name();
                self.out.push_str(" TO ");
                self.ident();
            }
            13 => {
                self.out.push_str("REVOKE SELECT ON TABLE ");
                self.qualified_name();
                self.out.push_str(" FROM ");
                self.ident();
            }
            14 => {
                self.out.push_str("ALTER TABLE ");
                self.qualified_name();
                self.out.push_str(" RENAME TO ");
                self.ident();
            }
            15 => {
                self.out.push_str("CREATE INDEX ");
                self.ident();
                self.out.push_str(" ON ");
                self.qualified_name();
                self.out.push_str(" (");
                self.ident();
                self.out.push(')');
            }
            16 => self.out.push_str("BEGIN"),
            17 => self.out.push_str("COMMIT"),
            18 => self.out.push_str("ROLLBACK"),
            19 => {
                self.out.push_str("CREATE DATABASE ");
                self.ident();
            }
            20 => {
                self.out.push_str("CREATE SCHEMA ");
                self.qualified_name();
            }
            21 => {
                self.out.push_str("CREATE ROLE ");
                self.ident();
            }
            22 => {
                self.out.push_str("DROP DATABASE ");
                self.ident();
            }
            23 => {
                self.out.push_str("DROP SCHEMA ");
                self.qualified_name();
            }
            24 => {
                self.out.push_str("DROP CLUSTER ");
                self.ident();
            }
            25 => {
                self.out.push_str("ALTER TABLE ");
                self.qualified_name();
                self.out.push_str(" OWNER TO ");
                self.ident();
            }
            26 => {
                self.out.push_str("ALTER TABLE ");
                self.qualified_name();
                self.out.push_str(" ADD COLUMN ");
                self.ident();
                self.out.push(' ');
                self.data_type(2);
            }
            27 => {
                self.out.push_str("SHOW CREATE TABLE ");
                self.qualified_name();
            }
            28 => {
                self.out.push_str("CREATE SECRET ");
                self.ident();
                self.out.push_str(" AS 'secret'");
            }
            29 => {
                self.out.push_str("ALTER SYSTEM SET ");
                self.ident();
                self.out.push_str(" = ");
                self.value();
            }
            30 => {
                self.out.push_str("ALTER SYSTEM RESET ");
                self.ident();
            }
            31 => {
                // Role-membership grant/revoke (distinct from the privilege form).
                self.out.push_str("GRANT ");
                self.ident();
                self.out.push_str(" TO ");
                self.ident();
            }
            32 => {
                self.out.push_str("REVOKE ");
                self.ident();
                self.out.push_str(" FROM ");
                self.ident();
            }
            33 => {
                self.out.push_str("DROP TABLE IF EXISTS ");
                self.qualified_name();
            }
            34 => {
                self.out.push_str("CREATE TYPE ");
                self.ident();
                self.out.push_str(" AS LIST (ELEMENT TYPE = ");
                self.data_type(1);
                self.out.push(')');
            }
            35 => {
                self.out.push_str("CREATE TYPE ");
                self.ident();
                self.out.push_str(" AS MAP (KEY TYPE = text, VALUE TYPE = ");
                self.data_type(1);
                self.out.push(')');
            }
            36 => {
                self.out.push_str("ALTER INDEX ");
                self.qualified_name();
                self.out.push_str(" RENAME TO ");
                self.ident();
            }
            _ => {
                self.out.push_str("CREATE OR REPLACE VIEW ");
                self.ident();
                self.out.push_str(" AS ");
                self.query(depth);
            }
        }
    }

    // --- Query / SELECT structure ------------------------------------------

    fn query(&mut self, depth: u32) {
        // Optional CTEs, plain `WITH` or mz's `WITH MUTUALLY RECURSIVE`
        // (which declares each CTE's output column types: a distinct grammar).
        if depth > 0 && self.chance(1, 4) {
            let mutually_recursive = self.chance(1, 3);
            self.out.push_str(if mutually_recursive {
                "WITH MUTUALLY RECURSIVE "
            } else {
                "WITH "
            });
            let n = 1 + self.pick(2);
            for i in 0..n {
                if i > 0 {
                    self.out.push_str(", ");
                }
                self.ident();
                if mutually_recursive {
                    self.out.push('(');
                    let cols = 1 + self.pick(2);
                    for j in 0..cols {
                        if j > 0 {
                            self.out.push_str(", ");
                        }
                        self.ident();
                        self.out.push(' ');
                        self.data_type(2);
                    }
                    self.out.push(')');
                }
                self.out.push_str(" AS (");
                self.set_expr(depth - 1);
                self.out.push(')');
            }
            self.out.push(' ');
        }
        self.set_expr(depth);
        // ORDER BY / LIMIT / OFFSET.
        if self.chance(1, 2) {
            self.out.push_str(" ORDER BY ");
            let n = 1 + self.pick(2);
            for i in 0..n {
                if i > 0 {
                    self.out.push_str(", ");
                }
                self.expr(depth.saturating_sub(1));
                if self.chance(1, 2) {
                    self.one_of(&[" ASC", " DESC"]);
                }
            }
        }
        if self.chance(1, 3) {
            self.out.push_str(" LIMIT ");
            self.value();
        }
        if self.chance(1, 4) {
            self.out.push_str(" OFFSET ");
            self.value();
        }
    }

    fn set_expr(&mut self, depth: u32) {
        if depth > 0 && self.chance(1, 4) {
            // Set operation between two query bodies.
            self.select(depth - 1);
            self.out.push(' ');
            self.one_of(&["UNION", "INTERSECT", "EXCEPT"]);
            if self.chance(1, 2) {
                self.out.push_str(" ALL");
            }
            self.out.push(' ');
            self.select(depth - 1);
        } else if self.chance(1, 6) {
            // VALUES.
            self.out.push_str("VALUES ");
            let rows = 1 + self.pick(2);
            for i in 0..rows {
                if i > 0 {
                    self.out.push_str(", ");
                }
                self.out.push('(');
                self.expr_list(depth.saturating_sub(1), 1, 3);
                self.out.push(')');
            }
        } else {
            self.select(depth);
        }
    }

    fn select(&mut self, depth: u32) {
        let d = depth.saturating_sub(1);
        self.out.push_str("SELECT ");
        if self.chance(1, 6) {
            self.out.push_str("DISTINCT ");
            if self.chance(1, 2) {
                self.out.push_str("ON (");
                self.expr_list(d, 1, 2);
                self.out.push_str(") ");
            }
        }
        // Projection.
        let cols = 1 + self.pick(3);
        for i in 0..cols {
            if i > 0 {
                self.out.push_str(", ");
            }
            if self.chance(1, 6) {
                self.out.push('*');
            } else {
                self.expr(d);
                if self.chance(1, 3) {
                    self.out.push_str(" AS ");
                    self.ident();
                }
            }
        }
        // FROM.
        if self.chance(3, 4) {
            self.out.push_str(" FROM ");
            self.from_item(d);
            // Comma joins (bounded so abundant input can't explode the width).
            let extra = self.pick(3);
            for _ in 0..extra {
                if !self.budget() {
                    break;
                }
                self.out.push_str(", ");
                self.from_item(d);
            }
        }
        if self.chance(1, 2) {
            self.out.push_str(" WHERE ");
            self.expr(d);
        }
        if self.chance(1, 3) {
            self.out.push_str(" GROUP BY ");
            self.group_by(d);
        }
        if self.chance(1, 4) {
            self.out.push_str(" HAVING ");
            self.expr(d);
        }
        // Named WINDOW clause (`WINDOW w AS (…)`), referenced by `OVER w`.
        if self.chance(1, 6) {
            self.out.push_str(" WINDOW ");
            let n = 1 + self.pick(2);
            for i in 0..n {
                if i > 0 {
                    self.out.push_str(", ");
                }
                self.ident();
                self.out.push_str(" AS ");
                self.window_def(d);
            }
        }
    }

    fn group_by(&mut self, depth: u32) {
        match self.pick(4) {
            0 | 1 => self.expr_list(depth, 1, 3),
            2 => {
                self.out.push_str("GROUPING SETS (");
                self.out.push('(');
                self.expr_list(depth, 1, 2);
                self.out.push_str("), (");
                self.expr_list(depth, 1, 2);
                self.out.push_str("))");
            }
            _ => {
                self.one_of(&["ROLLUP", "CUBE"]);
                self.out.push_str(" (");
                self.expr_list(depth, 1, 3);
                self.out.push(')');
            }
        }
    }

    fn from_item(&mut self, depth: u32) {
        if !self.budget() {
            self.qualified_name();
            return;
        }
        if self.chance(1, 3) {
            self.out.push_str("LATERAL ");
        }
        match self.pick(4) {
            0 | 1 => {
                self.qualified_name();
            }
            2 if depth > 0 => {
                // Derived table.
                self.out.push('(');
                self.query(depth - 1);
                self.out.push(')');
                self.alias();
            }
            _ => {
                // Table function.
                self.one_of(&["generate_series", "unnest", "\"row\"", "f"]);
                self.out.push('(');
                self.expr_list(depth.saturating_sub(1), 0, 2);
                self.out.push(')');
            }
        }
        // Optional alias for the simple cases too.
        if self.chance(1, 3) {
            self.alias();
        }
        // Optional join.
        if depth > 0 && self.chance(1, 3) {
            self.join_op();
            self.from_item(depth - 1);
            match self.pick(3) {
                0 => {
                    self.out.push_str(" ON ");
                    self.expr(depth - 1);
                }
                1 => {
                    self.out.push_str(" USING (");
                    self.ident();
                    self.out.push(')');
                }
                _ => {}
            }
        }
    }

    fn join_op(&mut self) {
        self.one_of(&[
            " JOIN ",
            " INNER JOIN ",
            " LEFT JOIN ",
            " RIGHT JOIN ",
            " FULL OUTER JOIN ",
            " CROSS JOIN ",
        ]);
    }

    fn alias(&mut self) {
        self.out.push_str(" AS ");
        self.ident();
        if self.chance(1, 3) {
            self.out.push_str(" (");
            let n = 1 + self.pick(2);
            for i in 0..n {
                if i > 0 {
                    self.out.push_str(", ");
                }
                self.ident();
            }
            self.out.push(')');
        }
    }
}

/// Build a query string from the byte stream.
fn generate(data: &[u8]) -> String {
    let mut u = Unstructured::new(data);
    let mut g = Gen {
        u: &mut u,
        out: String::new(),
    };
    // Mostly a structured statement (deep + valid, the round-trip the printer
    // must preserve). ~1/6 of runs are full-vocabulary soup (reaching shapes the
    // grammar doesn't enumerate + the parser/lexer error paths). One oracle for
    // both.
    if g.chance(1, 6) {
        g.soup();
    } else {
        g.statement(MAX_DEPTH);
    }
    // Trailing noise exercises the parser's handling of unexpected tokens
    // after an otherwise-complete statement.
    g.maybe_noise();
    g.out
}

fuzz_target!(|data: &[u8]| {
    let sql = generate(data);
    let Ok(stmts) = parse_statements(&sql) else {
        return;
    };
    if stmts.len() != 1 {
        return;
    }
    let mut orig_ast = stmts.into_iter().next().unwrap().ast;
    normalize(&mut orig_ast);

    // Both oracles compare the normalized AST against a normalized reparse.
    // Normalizing before the display oracle prints it makes the printer
    // re-derive precedence parens: the generator's explicit `Nested` parens
    // would otherwise print literally and mask a dropped-paren bug, the very
    // collision the structural comparison is there to catch.
    check_display(&orig_ast);
    check_pretty(&sql, &orig_ast);
});
