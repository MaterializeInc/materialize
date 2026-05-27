// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Redacts literals from SQL using Materialize's own parser.
//!
//! This is a helper for `bin/mz-workload-anonymize`. Doing literal redaction
//! with the real parser (rather than a regex) handles every literal form the
//! dialect supports — quoted strings with `''` escapes, escape strings,
//! dollar-quoted strings, numbers, hex strings, and intervals — and reuses the
//! exact `'<REDACTED>'` placeholder the rest of Materialize uses to turn
//! "customer data" into "usage data".
//!
//! Protocol: reads a JSON array of SQL strings on stdin and writes a JSON
//! array of the same length on stdout. Each output element is either the
//! redacted SQL or `null` when the input could not be parsed, in which case
//! the caller should fall back to its own redaction for that element.

use std::io::{self, Read, Write};

use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::parser;

/// Redacts every literal in `sql`, or returns `None` if it does not parse.
///
/// A workload may hold more than one statement per entry, so we redact each
/// parsed statement and rejoin them with `; `.
fn redact(sql: &str) -> Option<String> {
    let stmts = parser::parse_statements(sql).ok()?;
    let redacted: Vec<String> = stmts
        .iter()
        .map(|s| s.ast.to_ast_string_redacted())
        .collect();
    Some(redacted.join("; "))
}

fn main() -> io::Result<()> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;

    let sqls: Vec<String> =
        serde_json::from_str(&input).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let redacted: Vec<Option<String>> = sqls.iter().map(|sql| redact(sql)).collect();

    let out = serde_json::to_string(&redacted)?;
    io::stdout().write_all(out.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::redact;

    #[test]
    fn redacts_strings_and_numbers() {
        let redacted = redact("SELECT 'secret', 42 FROM t WHERE name = 'alice'").expect("parses");
        assert!(!redacted.contains("secret"), "{redacted}");
        assert!(!redacted.contains("alice"), "{redacted}");
        assert!(!redacted.contains("42"), "{redacted}");
        assert!(redacted.contains("<REDACTED>"), "{redacted}");
        // Identifiers must be preserved; only literals are redacted here.
        assert!(redacted.contains("name"), "{redacted}");
    }

    #[test]
    fn preserves_quoted_identifier_that_looks_like_a_literal() {
        // A column named with embedded spaces is double-quoted, not a literal,
        // and must survive redaction.
        let redacted = redact(r#"SELECT "my col" FROM t"#).expect("parses");
        assert!(redacted.contains(r#""my col""#), "{redacted}");
    }

    #[test]
    fn returns_none_on_parse_error() {
        assert_eq!(redact("SELEC not valid sql"), None);
    }

    #[test]
    fn redacts_each_statement_in_a_multi_statement_entry() {
        let redacted = redact("SELECT 'a'; SELECT 'b'").expect("parses");
        assert!(!redacted.contains("'a'"), "{redacted}");
        assert!(!redacted.contains("'b'"), "{redacted}");
    }
}
