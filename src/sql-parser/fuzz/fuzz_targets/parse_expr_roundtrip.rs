// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `parse_expr -> AstDisplay (Simple) -> parse_expr` must
//! preserve the full expression AST. Expressions don't have the intentional
//! redactions that statements do (no inline secret leaves), so this can
//! assert strict AST equality.
//!
//! Like `grammar_roundtrip`, this *generates* its input from the byte stream as
//! grammar choices rather than mutating raw text: random bytes rarely parse as
//! an expression, so byte mutation leaves the printer's quoting/parenthesization
//! logic — where every round-trip bug lives — barely exercised. Here the
//! generator is focused on a *bare* expression (the `parse_expr` entry point, as
//! used by index keys, defaults, and check constraints), so it omits the
//! subquery-bearing forms that need full query generation. It is biased toward
//! the printer stress points: identifiers that collide with keywords (forcing
//! quoting), operator-precedence groupings (forcing re-parenthesization), casts
//! under unary operators, and the special grammar forms. It also emits the
//! dedicated-AST-node expression forms whose printer branches a function-call
//! generator never reaches: `NULLIF(a, b)` (`Expr::NullIf`),
//! `GREATEST`/`LEAST(...)` (`Expr::HomogenizingFunction`, distinct from
//! `COALESCE`), `NORMALIZE(e [, FORM])` (a `normalize(...)` call the printer must
//! re-quote), the `$N` placeholder (`Expr::Parameter`), `LIKE`/`ILIKE … ESCAPE
//! <expr>` (the optional `escape` of `Expr::Like`), and the array-valued
//! quantified comparison `e op ANY/ALL/SOME (<array_expr>)`
//! (`Expr::AnyExpr`/`Expr::AllExpr`, distinct from the subquery-valued forms).
//! A minority of inputs get noise spliced in to keep the parser's error paths
//! covered.

#![no_main]

use std::sync::OnceLock;

use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{AstInfo, Expr};
use mz_sql_parser::parser::parse_expr;

// ---------------------------------------------------------------------------
// Round-trip oracle.
// ---------------------------------------------------------------------------

/// Strip `Expr::Nested` wrappers so AST equality is insensitive to parens
/// added or removed during display (e.g. disambiguating a `(1).field`
/// receiver against the lexer's greedy number rule).
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
/// Identical carve-out to `grammar_roundtrip`.
fn benign_reparse_error(msg: &str) -> bool {
    // Disambiguating display (e.g. `(num).field`) adds one paren of nesting
    // depth; an input right at the recursion limit can fail to reparse — a
    // consequence of the depth limit, not a printer bug.
    msg.contains("exceeds nested expression limit")
        // Context-sensitive keywords (MAP, POSITION, EXTRACT, ALL, ...) used as
        // bare identifiers in positions where the parser greedily dispatches to
        // a special grammar (`map[...]`, `position(... IN ...)`, etc.).
        || msg.contains("Expected left square bracket")
        || msg.contains("Expected left parenthesis")
        || msg.contains("Expected IN, found")
        || msg.contains("Expected arrow, found")
}

// ---------------------------------------------------------------------------
// Grammar generator (bare expressions).
// ---------------------------------------------------------------------------

/// Identifiers, weighted toward the printer's quoting decision: bare names and
/// quoted keyword collisions that must stay quoted to round-trip.
const IDENTS: &[&str] = &[
    "a", "b", "c", "x", "col", "foo", "t1", "\"some\"", "\"any\"", "\"all\"", "\"select\"",
    "\"map\"", "\"list\"", "\"array\"", "\"position\"", "\"as\"", "\"in\"", "\"Mixed\"",
    "\"with space\"", "\"a.b\"", "\"1col\"", "\"qu\"\"ote\"", "\"\"",
];

/// Bare scalar type names for casts.
const SCALAR_TYPES: &[&str] = &[
    "int4",
    "bigint",
    "double precision",
    "numeric(10, 2)",
    "boolean",
    "text",
    "varchar(10)",
    "bytea",
    "timestamp",
    "interval",
    "jsonb",
    "uuid",
];

/// Scalar/value literals, including ones with tricky lexing and typed literals.
const VALUES: &[&str] = &[
    "0",
    "1",
    "42",
    "3.14",
    ".5",
    "1.",
    "1e10",
    "1.5e-3",
    "'a'",
    "''",
    "'it''s'",
    "true",
    "false",
    "null",
    "INTERVAL '1' DAY",
    "INTERVAL '1-2' YEAR TO MONTH",
    "DATE '2020-01-01'",
    "x'deadbeef'",
];

/// Binary operators spanning precedence levels and the custom jsonb/range ops.
const BIN_OPS: &[&str] = &[
    "+", "-", "*", "/", "%", "||", "=", "<>", "<", ">", "<=", ">=", "AND", "OR", "->", "->>", "#>",
    "@>", "<@", "&", "|", "<<", ">>", "~",
];

/// Noise tokens spliced in to exercise the parser's error paths.
const NOISE: &[&str] = &[
    "(", ")", "[", "]", ",", "::", ".", "*", "@", "?", "->", "||", "=>", "~", "$1", "/*", "*/",
    "1e999", ".",
];

const MAX_DEPTH: u32 = 5;
const MAX_OUTPUT: usize = 2000;

struct Gen<'a, 'u> {
    u: &'u mut Unstructured<'a>,
    out: String,
}

impl<'a, 'u> Gen<'a, 'u> {
    fn budget(&self) -> bool {
        self.out.len() < MAX_OUTPUT
    }

    fn pick(&mut self, n: usize) -> usize {
        if n <= 1 {
            return 0;
        }
        self.u
            .int_in_range(0..=(n as u64 - 1))
            .map(|v| v as usize)
            .unwrap_or(0)
    }

    fn chance(&mut self, num: u32, den: u32) -> bool {
        self.u.ratio(num, den).unwrap_or(false)
    }

    fn one_of(&mut self, opts: &[&str]) {
        let i = self.pick(opts.len());
        self.out.push_str(opts[i]);
    }

    fn ident(&mut self) {
        // 1 in 5: a bare keyword as an identifier — whether the printer keeps it
        // unambiguous is exactly the property under test.
        if self.chance(1, 5) {
            let kw = keywords();
            let i = self.pick(kw.len());
            self.out.push_str(kw[i]);
        } else {
            self.one_of(IDENTS);
        }
    }

    fn inject_noise(&mut self) {
        match self.pick(3) {
            0 => self.one_of(NOISE),
            1 => {
                let kw = keywords();
                let i = self.pick(kw.len());
                self.out.push_str(kw[i]);
            }
            _ => {
                let n = 1 + self.pick(4);
                for _ in 0..n {
                    if let Ok(c) = char::arbitrary(self.u) {
                        self.out.push(c);
                    }
                }
            }
        }
    }

    fn maybe_noise(&mut self) {
        if self.budget() && self.chance(1, 12) {
            self.inject_noise();
        }
    }

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

    fn value(&mut self) {
        self.one_of(VALUES);
    }

    fn leaf_expr(&mut self) {
        match self.pick(5) {
            0 | 1 => self.qualified_name(),
            2 | 3 => self.value(),
            // `$N` placeholder — `Expr::Parameter`, with its own `${n}` printer.
            _ => {
                let n = 1 + self.pick(9);
                self.out.push('$');
                self.out.push_str(&n.to_string());
            }
        }
    }

    fn expr_grouped(&mut self, depth: u32) {
        if depth > 0 && self.chance(2, 5) {
            self.out.push('(');
            self.expr(depth);
            self.out.push(')');
        } else {
            self.expr(depth);
        }
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

    fn expr(&mut self, depth: u32) {
        if depth == 0 || !self.budget() {
            self.leaf_expr();
            return;
        }
        let d = depth - 1;
        match self.pick(17) {
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
                // SIMILAR TO has no ESCAPE in this AST; LIKE/ILIKE
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
            // Function call.
            8 | 9 => self.func_call(d),
            // CASE.
            10 => self.case_expr(d),
            // [NOT] IN (value list) — no subquery (this is a bare expression).
            11 => {
                self.expr_grouped(d);
                if self.chance(1, 3) {
                    self.out.push_str(" NOT");
                }
                self.out.push_str(" IN (");
                self.expr_list(d, 1, 3);
                self.out.push(')');
            }
            // Collection literals.
            12 => self.collection_expr(d),
            // Special grammar forms.
            13 => self.special_form(d),
            // Postfix forms: COLLATE, AT TIME ZONE.
            14 => {
                self.expr_grouped(d);
                if self.chance(1, 2) {
                    self.out.push_str(" COLLATE ");
                    self.ident();
                } else {
                    self.out.push_str(" AT TIME ZONE ");
                    self.expr_grouped(d);
                }
            }
            // Array-valued quantified comparison: `e op {ANY|ALL|SOME}
            // (<array_expr>)`. A single non-subquery expression inside the
            // parens parses as `Expr::AnyExpr`/`Expr::AllExpr` (the array-valued
            // forms), distinct from the subquery-valued `AnySubquery`/
            // `AllSubquery` that need full query generation.
            15 => {
                self.expr_grouped(d);
                self.out.push(' ');
                self.one_of(&["=", "<>", "<", ">", "<=", ">="]);
                self.out.push(' ');
                self.one_of(&["ANY", "ALL", "SOME"]);
                self.out.push_str(" (");
                // An array-shaped operand keeps the comparison well-typed and
                // exercises the `write_quantified_left` LHS paren logic.
                if self.chance(1, 2) {
                    self.out.push_str("ARRAY[");
                    self.expr_list(d, 1, 3);
                    self.out.push(']');
                } else {
                    self.expr(d);
                }
                self.out.push(')');
            }
            // Subscript / field access / tuple.
            _ => match self.pick(3) {
                0 => {
                    self.expr_grouped(d);
                    self.out.push('[');
                    self.expr(d);
                    if self.chance(1, 2) {
                        self.out.push(':');
                        self.expr(d);
                    }
                    self.out.push(']');
                }
                1 => {
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

    fn func_call(&mut self, depth: u32) {
        self.one_of(&[
            "count",
            "sum",
            "max",
            "abs",
            "coalesce",
            "\"some\"",
            "\"position\"",
            "\"array\"",
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
        }
        self.out.push(')');
        if self.chance(1, 6) {
            self.out.push_str(" FILTER (WHERE ");
            self.expr(depth);
            self.out.push(')');
        }
        if self.chance(1, 4) {
            // A window spec — needs no subquery.
            self.out.push_str(" OVER (");
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
                self.out.push_str("ARRAY[");
                self.expr_list(depth, 0, 3);
                self.out.push(']');
            }
            1 => {
                self.out.push_str("LIST[");
                self.expr_list(depth, 0, 3);
                self.out.push(']');
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
            // GREATEST / LEAST — `Expr::HomogenizingFunction`, a dedicated AST
            // node distinct from COALESCE with its own printer branch.
            5 => {
                self.one_of(&["GREATEST(", "LEAST("]);
                self.expr_list(depth, 1, 3);
                self.out.push(')');
            }
            // NULLIF(a, b) — `Expr::NullIf`, also a dedicated node, not a call.
            6 => {
                self.out.push_str("NULLIF(");
                self.expr(depth);
                self.out.push_str(", ");
                self.expr(depth);
                self.out.push(')');
            }
            // NORMALIZE(e [, FORM]) — desugars to a `normalize(...)` function
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
}

/// Every keyword the lexer knows, read at compile time so the set stays
/// complete as keywords are added (see `ident`/`inject_noise`).
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

/// Build a bare expression string from the byte stream.
fn generate(data: &[u8]) -> String {
    let mut u = Unstructured::new(data);
    let mut g = Gen {
        u: &mut u,
        out: String::new(),
    };
    g.maybe_noise();
    g.expr(MAX_DEPTH);
    g.maybe_noise();
    g.out
}

fuzz_target!(|data: &[u8]| {
    let sql = generate(data);
    let Ok(mut orig) = parse_expr(&sql) else {
        return;
    };
    let displayed = orig.to_ast_string_simple();

    let mut reparsed = match parse_expr(&displayed) {
        Ok(r) => r,
        Err(e) => {
            if benign_reparse_error(&e.to_string()) {
                return;
            }
            panic!("expr AstDisplay output failed to reparse: displayed={displayed:?} err={e}")
        }
    };
    RemoveParens.visit_expr_mut(&mut orig);
    RemoveParens.visit_expr_mut(&mut reparsed);

    assert_eq!(
        orig, reparsed,
        "expr AST changed through AstDisplay roundtrip\ndisplayed: {displayed:?}"
    );
});
