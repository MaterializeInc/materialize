//! psql-style variable resolution for SQL files.
//!
//! Resolves `:foo`, `:'foo'`, and `:"foo"` syntax in raw SQL text before
//! it reaches the SQL parser. Variables are defined per-profile in
//! `[profiles.<name>.variables]` in `project.toml`.
//!
//! ## Variable Syntax
//!
//! | Syntax | Semantics | Example output |
//! |--------|-----------|----------------|
//! | `:name` | Raw substitution — value inserted verbatim | `analytics` |
//! | `:'name'` | SQL literal — value wrapped in single quotes, `'` doubled | `'it''s-a-host'` |
//! | `:"name"` | SQL identifier — value wrapped in double quotes, `"` doubled | `"my""col"` |
//!
//! ## Resolution Rules
//!
//! 1. Variables are looked up in the `BTreeMap<String, String>` passed to
//!    [`resolve_variables`]. This map is populated from `project.toml`'s
//!    `[profiles.<name>.variables]` section.
//! 2. If a referenced variable has no definition, it is left as-is in the
//!    output and an [`UnresolvedVariable`] (with byte offset and length) is
//!    recorded in `ResolvedSql::unresolved`. Each occurrence is tracked
//!    separately (not deduplicated) so the LSP can highlight every reference.
//! 3. Every resolved substitution is recorded as a [`Substitution`] in
//!    `ResolvedSql::substitutions`, enabling [`resolved_to_original`] to map
//!    byte offsets in the resolved text back to the original source positions.
//! 4. The caller decides whether unresolved variables are errors or warnings
//!    based on the `PRAGMA WARN_ON_MISSING_VARIABLES;` directive.
//!
//! ## Context Awareness
//!
//! Variable references are **not** resolved inside:
//! - Single-quoted string literals (`'...'`)
//! - Double-quoted identifiers (`"..."`)
//! - Line comments (`-- ...`)
//! - Block comments (`/* ... */`), including nested blocks
//! - Dollar-quoted strings (`$$...$$` or `$tag$...$tag$`)
//!
//! The `::` token (PostgreSQL type cast) is never interpreted as a variable
//! reference.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// An unresolved variable reference with its location in the original SQL.
#[derive(Debug, Clone)]
pub struct UnresolvedVariable {
    /// The variable name (without the leading `:` or surrounding quotes).
    pub name: String,
    /// Byte offset of the `:` in the original SQL.
    pub byte_offset: usize,
    /// Byte length of the full reference (`:foo` = 4, `:'foo'` = 6).
    pub byte_len: usize,
}

/// A resolved variable substitution, for mapping offsets between original and resolved text.
#[derive(Debug, Clone)]
pub struct Substitution {
    /// Byte offset of the `:` in the original SQL.
    pub original_start: usize,
    /// Byte length of the variable reference in the original SQL.
    pub original_len: usize,
    /// Byte length of the replacement in the resolved SQL.
    pub resolved_len: usize,
}

/// Error returned when SQL contains variable references that have no definition.
#[derive(Debug)]
pub struct VariableError {
    pub unresolved: Vec<UnresolvedVariable>,
    pub path: PathBuf,
}

/// Result of resolving psql-style variables in SQL text.
#[derive(Debug)]
pub struct ResolvedSql<'a> {
    /// The SQL text with resolved variables (unresolved ones left as-is).
    pub sql: Cow<'a, str>,
    /// Variable references that had no definition, with their positions.
    pub unresolved: Vec<UnresolvedVariable>,
    /// Substitutions performed, for mapping offsets between resolved and original text.
    pub substitutions: Vec<Substitution>,
    /// Whether the file contains a `PRAGMA WARN_ON_MISSING_VARIABLES;` directive.
    pub has_warn_pragma: bool,
}

/// Map a byte offset in resolved text back to the corresponding offset in original text.
///
/// Walks `substitutions` in order, tracking the cumulative delta between original
/// and resolved positions. If `offset` falls before the next substitution in resolved
/// space, applies the running delta. If inside a substitution's resolved span, clamps
/// to that substitution's original start. Otherwise continues accumulating delta.
#[allow(clippy::as_conversions)]
pub fn resolved_to_original(offset: usize, substitutions: &[Substitution]) -> usize {
    let mut delta: isize = 0; // original - resolved cumulative shift

    for sub in substitutions {
        // Where this substitution starts in resolved-text space.
        let resolved_start = (sub.original_start as isize - delta) as usize;

        if offset < resolved_start {
            // Before this substitution — apply accumulated delta.
            return (offset as isize + delta) as usize;
        }

        let resolved_end = resolved_start + sub.resolved_len;
        if offset < resolved_end {
            // Inside this substitution's resolved span — clamp to original start.
            return sub.original_start;
        }

        // Past this substitution — accumulate delta.
        delta += sub.original_len as isize - sub.resolved_len as isize;
    }

    // After all substitutions.
    (offset as isize + delta) as usize
}

const PRAGMA: &str = "PRAGMA WARN_ON_MISSING_VARIABLES;";

/// Check whether the first non-whitespace content in `sql` is a comment
/// containing the warn-on-missing-variables pragma.
fn detect_warn_pragma(sql: &str) -> bool {
    let trimmed = sql.trim_start();

    if let Some(rest) = trimmed.strip_prefix("--") {
        // Line comment: check the text up to the first newline.
        let line = match rest.find('\n') {
            Some(pos) => &rest[..pos],
            None => rest,
        };
        line.contains(PRAGMA)
    } else if let Some(rest) = trimmed.strip_prefix("/*") {
        // Block comment: check the text up to the closing `*/`.
        match rest.find("*/") {
            Some(pos) => rest[..pos].contains(PRAGMA),
            None => rest.contains(PRAGMA),
        }
    } else {
        false
    }
}

/// The kind of variable reference found in the SQL text.
enum VarKind {
    /// `:name` — substitute raw value
    Raw,
    /// `:'name'` — wrap in single quotes with escaping
    SqlLiteral,
    /// `:"name"` — wrap in double quotes with escaping
    SqlIdentifier,
}

/// Check if `bytes[i..]` starts with `needle`.
fn starts_with(bytes: &[u8], i: usize, needle: &[u8]) -> bool {
    bytes[i..].starts_with(needle)
}

/// Push `value` into `out`, doubling any occurrence of `quote`.
fn push_sql_escaped(out: &mut String, value: &str, quote: char) {
    for ch in value.chars() {
        if ch == quote {
            out.push(ch);
        }
        out.push(ch);
    }
}

/// Try to read a variable reference starting at position `i` (which must be `:`).
///
/// Returns `(name, kind, end_position)` or `None` if this isn't a variable.
fn try_read_variable<'a>(
    sql: &'a str,
    bytes: &[u8],
    i: usize,
) -> Option<(&'a str, VarKind, usize)> {
    let len = bytes.len();
    if i + 1 >= len {
        return None;
    }

    // :: is a type cast
    if bytes[i + 1] == b':' {
        return None;
    }

    // :'name'
    if bytes[i + 1] == b'\'' {
        let name_start = i + 2;
        let mut j = name_start;
        while j < len && bytes[j] != b'\'' {
            j += 1;
        }
        if j >= len {
            return None; // unterminated
        }
        let name = &sql[name_start..j];
        return Some((name, VarKind::SqlLiteral, j + 1));
    }

    // :"name"
    if bytes[i + 1] == b'"' {
        let name_start = i + 2;
        let mut j = name_start;
        while j < len && bytes[j] != b'"' {
            j += 1;
        }
        if j >= len {
            return None; // unterminated
        }
        let name = &sql[name_start..j];
        return Some((name, VarKind::SqlIdentifier, j + 1));
    }

    // :name (bare identifier)
    if bytes[i + 1].is_ascii_alphabetic() || bytes[i + 1] == b'_' {
        let name_start = i + 1;
        let mut j = name_start;
        while j < len && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
            j += 1;
        }
        let name = &sql[name_start..j];
        return Some((name, VarKind::Raw, j));
    }

    None
}

/// Consume a single-quoted string. `i` is the position after the opening `'`.
/// Returns the position after the closing `'`.
fn consume_single_quoted(bytes: &[u8], mut i: usize, len: usize) -> usize {
    while i < len {
        if bytes[i] == b'\'' {
            if i + 1 < len && bytes[i + 1] == b'\'' {
                i += 2; // escaped quote
            } else {
                return i + 1; // closing quote
            }
        } else {
            i += 1;
        }
    }
    i
}

/// Consume a double-quoted identifier. `i` is the position after the opening `"`.
/// Returns the position after the closing `"`.
fn consume_double_quoted(bytes: &[u8], mut i: usize, len: usize) -> usize {
    while i < len {
        if bytes[i] == b'"' {
            return i + 1;
        }
        i += 1;
    }
    i
}

/// Consume a line comment. `i` is the position after `--`.
/// Returns the position after `\n` (or end of input).
fn consume_line_comment(bytes: &[u8], mut i: usize, len: usize) -> usize {
    while i < len {
        if bytes[i] == b'\n' {
            return i + 1;
        }
        i += 1;
    }
    i
}

/// Consume a block comment (with nesting). `i` is the position after `/*`.
/// Returns the position after the final `*/`.
fn consume_block_comment(bytes: &[u8], mut i: usize, len: usize) -> usize {
    let mut depth: u32 = 1;
    while i < len && depth > 0 {
        if starts_with(bytes, i, b"/*") {
            depth += 1;
            i += 2;
        } else if starts_with(bytes, i, b"*/") {
            depth -= 1;
            i += 2;
        } else {
            i += 1;
        }
    }
    i
}

/// Check if `$` at position `i` starts a dollar-quote tag.
/// Returns `(end_pos, tag_bytes)` where `end_pos` is the position after the closing `$` of the tag.
fn try_dollar_tag<'a>(bytes: &'a [u8], i: usize, len: usize) -> Option<(usize, &'a [u8])> {
    // Must start with $
    if bytes[i] != b'$' {
        return None;
    }

    // $$ case: tag is empty
    if i + 1 < len && bytes[i + 1] == b'$' {
        return Some((i + 2, &bytes[i..i + 2]));
    }

    // $tag$ case: tag is [a-zA-Z_][a-zA-Z0-9_]*
    let tag_start = i + 1;
    if tag_start < len && (bytes[tag_start].is_ascii_alphabetic() || bytes[tag_start] == b'_') {
        let mut j = tag_start + 1;
        while j < len && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
            j += 1;
        }
        if j < len && bytes[j] == b'$' {
            return Some((j + 1, &bytes[i..j + 1]));
        }
    }

    None
}

/// Consume a dollar-quoted string. `i` is the position after the opening tag.
/// Scans for the matching closing tag.
fn consume_dollar_quoted(bytes: &[u8], mut i: usize, len: usize, tag: &[u8]) -> usize {
    while i < len {
        if bytes[i] == b'$' && bytes[i..].starts_with(tag) {
            return i + tag.len();
        }
        i += 1;
    }
    i
}

/// Find the variable reference (if any) that contains the given byte offset.
///
/// Scans `sql` using the same context-awareness rules as [`resolve_variables`]
/// (skipping strings, comments, dollar-quotes, type casts) and checks whether
/// `offset` falls inside a variable reference.
///
/// # Returns
/// `(name, byte_offset_of_colon, byte_len)` if the offset is inside a
/// `:name`, `:'name'`, or `:"name"` reference outside of strings/comments.
/// `None` otherwise.
pub fn find_variable_at_position(sql: &str, offset: usize) -> Option<(String, usize, usize)> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        if bytes[i] == b'\'' {
            i = consume_single_quoted(bytes, i + 1, len);
        } else if bytes[i] == b'"' {
            i = consume_double_quoted(bytes, i + 1, len);
        } else if starts_with(bytes, i, b"--") {
            i = consume_line_comment(bytes, i + 2, len);
        } else if starts_with(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i + 2, len);
        } else if bytes[i] == b'$' {
            if let Some((end, tag)) = try_dollar_tag(bytes, i, len) {
                i = consume_dollar_quoted(bytes, end, len, tag);
            } else {
                i += 1;
            }
        } else if starts_with(bytes, i, b"::") {
            i += 2;
        } else if bytes[i] == b':' {
            if let Some((name, _kind, end)) = try_read_variable(sql, bytes, i) {
                let var_start = i;
                let var_len = end - var_start;
                if offset >= var_start && offset < end {
                    return Some((name.to_string(), var_start, var_len));
                }
                i = end;
            } else {
                i += 1;
            }
        } else {
            i += 1;
        }
    }

    None
}

/// Resolve psql-style variables (`:foo`, `:'foo'`, `:"foo"`) in SQL text.
///
/// Always returns `ResolvedSql` with the SQL text (unresolved variables left as-is),
/// a list of unresolved variable names, and whether the pragma was detected.
/// The caller decides whether unresolved variables are errors or warnings.
pub fn resolve_variables<'a>(sql: &'a str, vars: &BTreeMap<String, String>) -> ResolvedSql<'a> {
    let bytes = sql.as_bytes();
    let len = bytes.len();

    if len == 0 {
        return ResolvedSql {
            sql: Cow::Borrowed(sql),
            unresolved: Vec::new(),
            substitutions: Vec::new(),
            has_warn_pragma: false,
        };
    }

    let has_warn_pragma = detect_warn_pragma(sql);

    let mut i = 0;
    let mut output: Option<String> = None;
    let mut copy_from: usize = 0;
    let mut unresolved: Vec<UnresolvedVariable> = Vec::new();
    let mut substitutions: Vec<Substitution> = Vec::new();

    while i < len {
        if bytes[i] == b'\'' {
            i = consume_single_quoted(bytes, i + 1, len);
        } else if bytes[i] == b'"' {
            i = consume_double_quoted(bytes, i + 1, len);
        } else if starts_with(bytes, i, b"--") {
            i = consume_line_comment(bytes, i + 2, len);
        } else if starts_with(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i + 2, len);
        } else if bytes[i] == b'$' {
            if let Some((end, tag)) = try_dollar_tag(bytes, i, len) {
                i = consume_dollar_quoted(bytes, end, len, tag);
            } else {
                i += 1;
            }
        } else if starts_with(bytes, i, b"::") {
            // Type cast — skip both colons
            i += 2;
        } else if bytes[i] == b':' {
            if let Some((name, kind, end)) = try_read_variable(sql, bytes, i) {
                // Flush pending text and perform substitution
                let var_start = i;
                let buf = match output {
                    Some(ref mut buf) => {
                        buf.push_str(&sql[copy_from..var_start]);
                        buf
                    }
                    None => {
                        let mut buf = String::with_capacity(sql.len());
                        buf.push_str(&sql[copy_from..var_start]);
                        output = Some(buf);
                        output.as_mut().unwrap()
                    }
                };

                if let Some(value) = vars.get(name) {
                    let before = buf.len();
                    match kind {
                        VarKind::Raw => buf.push_str(value),
                        VarKind::SqlLiteral => {
                            buf.push('\'');
                            push_sql_escaped(buf, value, '\'');
                            buf.push('\'');
                        }
                        VarKind::SqlIdentifier => {
                            buf.push('"');
                            push_sql_escaped(buf, value, '"');
                            buf.push('"');
                        }
                    }
                    let after = buf.len();
                    substitutions.push(Substitution {
                        original_start: var_start,
                        original_len: end - var_start,
                        resolved_len: after - before,
                    });
                } else {
                    unresolved.push(UnresolvedVariable {
                        name: name.to_string(),
                        byte_offset: var_start,
                        byte_len: end - var_start,
                    });
                    buf.push_str(&sql[var_start..end]);
                }

                copy_from = end;
                i = end;
            } else {
                i += 1;
            }
        } else {
            i += 1;
        }
    }

    let sql = match output {
        Some(mut buf) => {
            buf.push_str(&sql[copy_from..]);
            Cow::Owned(buf)
        }
        None => Cow::Borrowed(sql),
    };

    ResolvedSql {
        sql,
        unresolved,
        substitutions,
        has_warn_pragma,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Extract just the names from unresolved variables for easy assertion.
    fn unresolved_names<'a>(result: &'a ResolvedSql<'a>) -> Vec<&'a str> {
        result.unresolved.iter().map(|v| v.name.as_str()).collect()
    }

    #[test]
    fn no_variables_returns_borrowed() {
        let sql = "SELECT 1 FROM t WHERE x = 'hello'";
        let result = resolve_variables(sql, &BTreeMap::new());
        assert!(matches!(result.sql, Cow::Borrowed(_)));
        assert_eq!(result.sql.as_ref(), sql);
        assert!(result.unresolved.is_empty());
    }

    #[test]
    fn bare_variable_substitution() {
        let v = vars(&[("cluster", "analytics")]);
        let result = resolve_variables(
            "CREATE MATERIALIZED VIEW mv IN CLUSTER :cluster AS SELECT 1",
            &v,
        );
        assert_eq!(
            result.sql.as_ref(),
            "CREATE MATERIALIZED VIEW mv IN CLUSTER analytics AS SELECT 1"
        );
        assert!(result.unresolved.is_empty());
    }

    #[test]
    fn single_quoted_variable_with_escaping() {
        let v = vars(&[("pg_host", "it's-a-host")]);
        let result = resolve_variables("CREATE CONNECTION pg TO POSTGRES (HOST :'pg_host')", &v);
        assert_eq!(
            result.sql.as_ref(),
            "CREATE CONNECTION pg TO POSTGRES (HOST 'it''s-a-host')"
        );
        assert!(result.unresolved.is_empty());
    }

    #[test]
    fn double_quoted_variable_with_escaping() {
        let v = vars(&[("col", "my\"col")]);
        let result = resolve_variables("SELECT :\"col\" FROM t", &v);
        assert_eq!(result.sql.as_ref(), "SELECT \"my\"\"col\" FROM t");
        assert!(result.unresolved.is_empty());
    }

    #[test]
    fn type_cast_preserved() {
        let result = resolve_variables("SELECT x::int FROM t", &BTreeMap::new());
        assert!(matches!(result.sql, Cow::Borrowed(_)));
        assert_eq!(result.sql.as_ref(), "SELECT x::int FROM t");
        assert!(result.unresolved.is_empty());
    }

    #[test]
    fn variable_inside_string_literal_not_resolved() {
        let v = vars(&[("foo", "bar")]);
        let result = resolve_variables("SELECT ':foo' FROM t", &v);
        assert!(matches!(result.sql, Cow::Borrowed(_)));
        assert_eq!(result.sql.as_ref(), "SELECT ':foo' FROM t");
    }

    #[test]
    fn variable_in_line_comment_not_resolved() {
        let v = vars(&[("foo", "bar")]);
        let result = resolve_variables("-- :foo\nSELECT 1", &v);
        assert!(matches!(result.sql, Cow::Borrowed(_)));
        assert_eq!(result.sql.as_ref(), "-- :foo\nSELECT 1");
    }

    #[test]
    fn variable_in_block_comment_not_resolved() {
        let v = vars(&[("foo", "bar")]);
        let result = resolve_variables("/* :foo */ SELECT 1", &v);
        assert!(matches!(result.sql, Cow::Borrowed(_)));
        assert_eq!(result.sql.as_ref(), "/* :foo */ SELECT 1");
    }

    #[test]
    fn nested_block_comment_not_resolved() {
        let v = vars(&[("foo", "bar")]);
        let result = resolve_variables("/* /* :foo */ */ SELECT 1", &v);
        assert!(matches!(result.sql, Cow::Borrowed(_)));
        assert_eq!(result.sql.as_ref(), "/* /* :foo */ */ SELECT 1");
    }

    #[test]
    fn multiple_variables() {
        let v = vars(&[("a", "1"), ("b", "2")]);
        let result = resolve_variables("SELECT :a, :b", &v);
        assert_eq!(result.sql.as_ref(), "SELECT 1, 2");
        assert!(result.unresolved.is_empty());
    }

    #[test]
    fn unresolved_variable_reported() {
        let result = resolve_variables("SELECT :missing", &BTreeMap::new());
        assert_eq!(unresolved_names(&result), vec!["missing"]);
        assert_eq!(result.sql.as_ref(), "SELECT :missing");
    }

    #[test]
    fn multiple_unresolved_lists_all() {
        let result = resolve_variables("SELECT :a, :b, :a", &BTreeMap::new());
        assert_eq!(unresolved_names(&result), vec!["a", "b", "a"]);
    }

    #[test]
    fn empty_vars_no_syntax_borrowed() {
        let result = resolve_variables("SELECT 1", &BTreeMap::new());
        assert!(matches!(result.sql, Cow::Borrowed(_)));
    }

    #[test]
    fn empty_vars_with_syntax_reports_unresolved() {
        let result = resolve_variables("SELECT :foo", &BTreeMap::new());
        assert!(!result.unresolved.is_empty());
    }

    #[test]
    fn variable_at_end_of_input() {
        let v = vars(&[("foo", "bar")]);
        let result = resolve_variables("SELECT :foo", &v);
        assert_eq!(result.sql.as_ref(), "SELECT bar");
    }

    #[test]
    fn adjacent_syntax() {
        let v = vars(&[("foo", "1"), ("bar", "2")]);
        let result = resolve_variables("(:foo, :bar)", &v);
        assert_eq!(result.sql.as_ref(), "(1, 2)");
    }

    #[test]
    fn single_quoted_no_escaping_needed() {
        let v = vars(&[("host", "simple-host")]);
        let result = resolve_variables("HOST :'host'", &v);
        assert_eq!(result.sql.as_ref(), "HOST 'simple-host'");
    }

    #[test]
    fn double_quoted_no_escaping_needed() {
        let v = vars(&[("col", "simple_col")]);
        let result = resolve_variables("SELECT :\"col\"", &v);
        assert_eq!(result.sql.as_ref(), "SELECT \"simple_col\"");
    }

    #[test]
    fn double_quoted_identifier_not_resolved() {
        let v = vars(&[("id", "bar")]);
        let result = resolve_variables("SELECT \"user:id\" FROM t", &v);
        assert!(matches!(result.sql, Cow::Borrowed(_)));
        assert_eq!(result.sql.as_ref(), "SELECT \"user:id\" FROM t");
    }

    #[test]
    fn dollar_quoted_not_resolved() {
        let v = vars(&[("foo", "bar")]);
        let result = resolve_variables("SELECT $$:foo$$ FROM t", &v);
        assert!(matches!(result.sql, Cow::Borrowed(_)));
        assert_eq!(result.sql.as_ref(), "SELECT $$:foo$$ FROM t");
    }

    #[test]
    fn dollar_tagged_not_resolved() {
        let v = vars(&[("foo", "bar")]);
        let result = resolve_variables("SELECT $tag$:foo$tag$ FROM t", &v);
        assert!(matches!(result.sql, Cow::Borrowed(_)));
        assert_eq!(result.sql.as_ref(), "SELECT $tag$:foo$tag$ FROM t");
    }

    #[test]
    fn dollar_sign_alone_no_crash() {
        let result = resolve_variables("SELECT $ FROM t", &BTreeMap::new());
        assert!(matches!(result.sql, Cow::Borrowed(_)));
        assert_eq!(result.sql.as_ref(), "SELECT $ FROM t");
    }

    // --- Pragma tests ---

    #[test]
    fn pragma_line_comment() {
        let result = resolve_variables(
            "-- PRAGMA WARN_ON_MISSING_VARIABLES;\nSELECT :foo",
            &BTreeMap::new(),
        );
        assert!(result.has_warn_pragma);
        assert_eq!(unresolved_names(&result), vec!["foo"]);
    }

    #[test]
    fn pragma_block_comment() {
        let result = resolve_variables(
            "/* PRAGMA WARN_ON_MISSING_VARIABLES; */\nSELECT :foo",
            &BTreeMap::new(),
        );
        assert!(result.has_warn_pragma);
        assert_eq!(unresolved_names(&result), vec!["foo"]);
    }

    #[test]
    fn pragma_with_leading_whitespace() {
        let result = resolve_variables(
            "  \t\n  -- PRAGMA WARN_ON_MISSING_VARIABLES;\nSELECT :foo",
            &BTreeMap::new(),
        );
        assert!(result.has_warn_pragma);
    }

    #[test]
    fn pragma_not_on_first_comment() {
        let result = resolve_variables(
            "SELECT 1;\n-- PRAGMA WARN_ON_MISSING_VARIABLES;\nSELECT :foo",
            &BTreeMap::new(),
        );
        assert!(!result.has_warn_pragma);
    }

    #[test]
    fn pragma_missing() {
        let result = resolve_variables("SELECT :foo", &BTreeMap::new());
        assert!(!result.has_warn_pragma);
    }

    #[test]
    fn pragma_partial_match() {
        let result = resolve_variables("-- PRAGMA WARN_ON_MISSING\nSELECT :foo", &BTreeMap::new());
        assert!(!result.has_warn_pragma);
    }

    // --- Unresolved variable position tests ---

    #[test]
    fn unresolved_variable_has_correct_offset() {
        // "SELECT :missing" — `:` is at byte 7
        let result = resolve_variables("SELECT :missing", &BTreeMap::new());
        assert_eq!(result.unresolved.len(), 1);
        assert_eq!(result.unresolved[0].name, "missing");
        assert_eq!(result.unresolved[0].byte_offset, 7);
        assert_eq!(result.unresolved[0].byte_len, 8); // `:missing` = 8 bytes
    }

    #[test]
    fn unresolved_quoted_variable_has_correct_len() {
        // "SELECT :'missing'" — `:` at 7, len = 10 (:'missing')
        let result = resolve_variables("SELECT :'missing'", &BTreeMap::new());
        assert_eq!(result.unresolved.len(), 1);
        assert_eq!(result.unresolved[0].byte_offset, 7);
        assert_eq!(result.unresolved[0].byte_len, 10);
    }

    #[test]
    fn multiple_unresolved_tracks_each_occurrence() {
        let result = resolve_variables("SELECT :a, :b, :a", &BTreeMap::new());
        assert_eq!(result.unresolved.len(), 3);
        assert_eq!(result.unresolved[0].name, "a");
        assert_eq!(result.unresolved[0].byte_offset, 7);
        assert_eq!(result.unresolved[1].name, "b");
        assert_eq!(result.unresolved[1].byte_offset, 11);
        assert_eq!(result.unresolved[2].name, "a");
        assert_eq!(result.unresolved[2].byte_offset, 15);
    }

    // --- find_variable_at_position tests ---

    #[test]
    fn find_var_bare_variable() {
        // "SELECT :foo FROM t" — `:foo` at bytes 7..11
        let sql = "SELECT :foo FROM t";
        let result = find_variable_at_position(sql, 7);
        assert_eq!(result, Some(("foo".to_string(), 7, 4)));
        // Middle of variable
        assert_eq!(
            find_variable_at_position(sql, 9),
            Some(("foo".to_string(), 7, 4))
        );
    }

    #[test]
    fn find_var_single_quoted() {
        // "HOST :'host'" — `:'host'` at bytes 5..13
        let sql = "HOST :'host'";
        let result = find_variable_at_position(sql, 5);
        assert_eq!(result, Some(("host".to_string(), 5, 7)));
        assert_eq!(
            find_variable_at_position(sql, 11),
            Some(("host".to_string(), 5, 7))
        );
    }

    #[test]
    fn find_var_double_quoted() {
        // "SELECT :\"col\"" — `:"col"` at bytes 7..13
        let sql = "SELECT :\"col\"";
        let result = find_variable_at_position(sql, 7);
        assert_eq!(result, Some(("col".to_string(), 7, 6)));
    }

    #[test]
    fn find_var_between_variables_returns_none() {
        // "SELECT :a, :b" — space at byte 10 is between variables
        let sql = "SELECT :a, :b";
        assert_eq!(find_variable_at_position(sql, 10), None);
    }

    #[test]
    fn find_var_in_string_literal_returns_none() {
        let sql = "SELECT ':foo' FROM t";
        // Byte 8 is inside the string literal
        assert_eq!(find_variable_at_position(sql, 8), None);
    }

    #[test]
    fn find_var_type_cast_returns_none() {
        let sql = "SELECT x::int FROM t";
        // Byte 9 is the second colon of ::
        assert_eq!(find_variable_at_position(sql, 9), None);
        assert_eq!(find_variable_at_position(sql, 8), None);
    }

    #[test]
    fn find_var_in_comment_returns_none() {
        let sql = "-- :foo\nSELECT 1";
        assert_eq!(find_variable_at_position(sql, 3), None);
    }

    #[test]
    fn find_var_offset_past_end_returns_none() {
        let sql = "SELECT :foo";
        assert_eq!(find_variable_at_position(sql, 11), None);
    }

    // --- Substitution tracking tests ---

    #[test]
    fn resolved_variable_records_substitution() {
        let v = vars(&[("cluster", "analytics")]);
        let result = resolve_variables("IN CLUSTER :cluster AS", &v);
        assert_eq!(result.substitutions.len(), 1);
        assert_eq!(result.substitutions[0].original_start, 11); // `:cluster` starts at 11
        assert_eq!(result.substitutions[0].original_len, 8); // `:cluster` = 8 bytes
        assert_eq!(result.substitutions[0].resolved_len, 9); // `analytics` = 9 bytes
    }

    // --- resolved_to_original tests ---

    #[test]
    fn resolved_to_original_no_substitutions() {
        assert_eq!(resolved_to_original(5, &[]), 5);
        assert_eq!(resolved_to_original(0, &[]), 0);
    }

    #[test]
    fn resolved_to_original_shorter_replacement() {
        // Original: "IN CLUSTER :cluster AS" (22 bytes)
        //                        ^11     ^19
        // Resolved: "IN CLUSTER ab AS" (16 bytes)  — `:cluster` (8) → `ab` (2)
        //                        ^11 ^13
        let subs = vec![Substitution {
            original_start: 11,
            original_len: 8,
            resolved_len: 2,
        }];
        // Before substitution
        assert_eq!(resolved_to_original(5, &subs), 5);
        // Inside substitution (resolved offset 11 or 12) → clamp to original start
        assert_eq!(resolved_to_original(11, &subs), 11);
        assert_eq!(resolved_to_original(12, &subs), 11);
        // After substitution: resolved 13 → original 19 (delta = 8 - 2 = 6)
        assert_eq!(resolved_to_original(13, &subs), 19);
    }

    #[test]
    fn resolved_to_original_longer_replacement() {
        // Original: "X :a Y" — `:a` at 2, len 2
        // Resolved: "X longvalue Y" — `longvalue` len 9
        let subs = vec![Substitution {
            original_start: 2,
            original_len: 2,
            resolved_len: 9,
        }];
        // Before
        assert_eq!(resolved_to_original(0, &subs), 0);
        // Inside (2..11) → clamp to 2
        assert_eq!(resolved_to_original(5, &subs), 2);
        // After: resolved 11 → original 4 (delta = 2 - 9 = -7)
        assert_eq!(resolved_to_original(11, &subs), 4);
    }

    #[test]
    fn resolved_to_original_multiple_substitutions() {
        // Original: "A :x B :y C" — :x at 2 (len 2), :y at 7 (len 2)
        // Resolved: "A val1 B val2 C" — val1 (len 4), val2 (len 4)
        let subs = vec![
            Substitution {
                original_start: 2,
                original_len: 2,
                resolved_len: 4,
            },
            Substitution {
                original_start: 7,
                original_len: 2,
                resolved_len: 4,
            },
        ];
        // Before first sub
        assert_eq!(resolved_to_original(0, &subs), 0);
        // Inside first sub (resolved 2..6)
        assert_eq!(resolved_to_original(3, &subs), 2);
        // Between subs: resolved 6 → original 4, delta = 2 - 4 = -2
        assert_eq!(resolved_to_original(6, &subs), 4);
        // Inside second sub: resolved 9 (second sub starts at resolved 7 + delta(-2) → 9)
        // Actually: second sub original_start=7, delta after first = 2-4 = -2
        // resolved_start = 7 - (-2) = 9, resolved_end = 9 + 4 = 13
        assert_eq!(resolved_to_original(10, &subs), 7);
        // After both: resolved 13 → original 9, cumulative delta = (2-4) + (2-4) = -4
        assert_eq!(resolved_to_original(13, &subs), 9);
    }
}
