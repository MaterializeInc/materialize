//! psql-style variable resolution for SQL files.
//!
//! Resolves `:foo`, `:'foo'`, and `:"foo"` syntax in raw SQL text before
//! it reaches the SQL parser. Variables are defined per-profile in
//! `[profiles.<name>.variables]` in `project.toml`.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::Path;

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

/// Resolve psql-style variables (`:foo`, `:'foo'`, `:"foo"`) in SQL text.
///
/// Returns `Cow::Borrowed` when no variables are present (zero allocation).
/// Type casts (`::`) are preserved. Variables inside string literals and
/// comments are not resolved.
pub fn resolve_variables<'a>(
    sql: &'a str,
    vars: &BTreeMap<String, String>,
    path: &Path,
) -> Cow<'a, str> {
    let bytes = sql.as_bytes();
    let len = bytes.len();

    if len == 0 {
        return Cow::Borrowed(sql);
    }

    let mut i = 0;
    let mut output: Option<String> = None;
    let mut copy_from: usize = 0;

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
                } else {
                    eprintln!(
                        "warning: potential unresolved variable :{name} in {}",
                        path.display()
                    );
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

    match output {
        Some(mut buf) => {
            buf.push_str(&sql[copy_from..]);
            Cow::Owned(buf)
        }
        None => Cow::Borrowed(sql),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn path() -> PathBuf {
        PathBuf::from("test.sql")
    }

    fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn no_variables_returns_borrowed() {
        let sql = "SELECT 1 FROM t WHERE x = 'hello'";
        let result = resolve_variables(sql, &BTreeMap::new(), &path());
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.as_ref(), sql);
    }

    #[test]
    fn bare_variable_substitution() {
        let v = vars(&[("cluster", "analytics")]);
        let result = resolve_variables(
            "CREATE MATERIALIZED VIEW mv IN CLUSTER :cluster AS SELECT 1",
            &v,
            &path(),
        );
        assert_eq!(
            result.as_ref(),
            "CREATE MATERIALIZED VIEW mv IN CLUSTER analytics AS SELECT 1"
        );
    }

    #[test]
    fn single_quoted_variable_with_escaping() {
        let v = vars(&[("pg_host", "it's-a-host")]);
        let result = resolve_variables(
            "CREATE CONNECTION pg TO POSTGRES (HOST :'pg_host')",
            &v,
            &path(),
        );
        assert_eq!(
            result.as_ref(),
            "CREATE CONNECTION pg TO POSTGRES (HOST 'it''s-a-host')"
        );
    }

    #[test]
    fn double_quoted_variable_with_escaping() {
        let v = vars(&[("col", "my\"col")]);
        let result = resolve_variables("SELECT :\"col\" FROM t", &v, &path());
        assert_eq!(result.as_ref(), "SELECT \"my\"\"col\" FROM t");
    }

    #[test]
    fn type_cast_preserved() {
        let result = resolve_variables("SELECT x::int FROM t", &BTreeMap::new(), &path());
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.as_ref(), "SELECT x::int FROM t");
    }

    #[test]
    fn variable_inside_string_literal_not_resolved() {
        let v = vars(&[("foo", "bar")]);
        let result = resolve_variables("SELECT ':foo' FROM t", &v, &path());
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.as_ref(), "SELECT ':foo' FROM t");
    }

    #[test]
    fn variable_in_line_comment_not_resolved() {
        let v = vars(&[("foo", "bar")]);
        let result = resolve_variables("-- :foo\nSELECT 1", &v, &path());
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.as_ref(), "-- :foo\nSELECT 1");
    }

    #[test]
    fn variable_in_block_comment_not_resolved() {
        let v = vars(&[("foo", "bar")]);
        let result = resolve_variables("/* :foo */ SELECT 1", &v, &path());
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.as_ref(), "/* :foo */ SELECT 1");
    }

    #[test]
    fn nested_block_comment_not_resolved() {
        let v = vars(&[("foo", "bar")]);
        let result = resolve_variables("/* /* :foo */ */ SELECT 1", &v, &path());
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.as_ref(), "/* /* :foo */ */ SELECT 1");
    }

    #[test]
    fn multiple_variables() {
        let v = vars(&[("a", "1"), ("b", "2")]);
        let result = resolve_variables("SELECT :a, :b", &v, &path());
        assert_eq!(result.as_ref(), "SELECT 1, 2");
    }

    #[test]
    fn unresolved_variable_passes_through() {
        let sql = "SELECT :missing";
        let result = resolve_variables(sql, &BTreeMap::new(), &path());
        assert_eq!(result.as_ref(), sql);
    }

    #[test]
    fn multiple_unresolved_pass_through() {
        let sql = "SELECT :a, :b, :a";
        let result = resolve_variables(sql, &BTreeMap::new(), &path());
        assert_eq!(result.as_ref(), sql);
    }

    #[test]
    fn empty_vars_no_syntax_borrowed() {
        let result = resolve_variables("SELECT 1", &BTreeMap::new(), &path());
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn empty_vars_passes_through() {
        let sql = "SELECT :foo";
        let result = resolve_variables(sql, &BTreeMap::new(), &path());
        assert_eq!(result.as_ref(), sql);
    }

    #[test]
    fn array_slice_not_treated_as_variable() {
        let sql = "SELECT arr[1:list_length(arr) - 1]";
        let result = resolve_variables(sql, &BTreeMap::new(), &path());
        assert_eq!(result.as_ref(), sql);
    }

    #[test]
    fn variable_at_end_of_input() {
        let v = vars(&[("foo", "bar")]);
        let result = resolve_variables("SELECT :foo", &v, &path());
        assert_eq!(result.as_ref(), "SELECT bar");
    }

    #[test]
    fn adjacent_syntax() {
        let v = vars(&[("foo", "1"), ("bar", "2")]);
        let result = resolve_variables("(:foo, :bar)", &v, &path());
        assert_eq!(result.as_ref(), "(1, 2)");
    }

    #[test]
    fn single_quoted_no_escaping_needed() {
        let v = vars(&[("host", "simple-host")]);
        let result = resolve_variables("HOST :'host'", &v, &path());
        assert_eq!(result.as_ref(), "HOST 'simple-host'");
    }

    #[test]
    fn double_quoted_no_escaping_needed() {
        let v = vars(&[("col", "simple_col")]);
        let result = resolve_variables("SELECT :\"col\"", &v, &path());
        assert_eq!(result.as_ref(), "SELECT \"simple_col\"");
    }

    #[test]
    fn double_quoted_identifier_not_resolved() {
        let v = vars(&[("id", "bar")]);
        let result = resolve_variables("SELECT \"user:id\" FROM t", &v, &path());
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.as_ref(), "SELECT \"user:id\" FROM t");
    }

    #[test]
    fn dollar_quoted_not_resolved() {
        let v = vars(&[("foo", "bar")]);
        let result = resolve_variables("SELECT $$:foo$$ FROM t", &v, &path());
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.as_ref(), "SELECT $$:foo$$ FROM t");
    }

    #[test]
    fn dollar_tagged_not_resolved() {
        let v = vars(&[("foo", "bar")]);
        let result = resolve_variables("SELECT $tag$:foo$tag$ FROM t", &v, &path());
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.as_ref(), "SELECT $tag$:foo$tag$ FROM t");
    }

    #[test]
    fn dollar_sign_alone_no_crash() {
        let result = resolve_variables("SELECT $ FROM t", &BTreeMap::new(), &path());
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.as_ref(), "SELECT $ FROM t");
    }
}
