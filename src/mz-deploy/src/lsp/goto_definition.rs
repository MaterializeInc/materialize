//! Two-phase go-to-definition for SQL identifiers.
//!
//! Go-to-definition resolves a cursor position in a `.sql` file to the source
//! file that defines the referenced object. The algorithm has two phases:
//!
//! ## Phase A — Find identifier at cursor
//!
//! [`find_reference_at_position()`] uses the lexer ([`mz_sql_lexer::lexer::lex()`])
//! to tokenize the text, finds the token containing the cursor byte offset, and
//! collects dot-separated identifier chains. For example, in `FROM myschema.orders`,
//! clicking on either `myschema` or `orders` returns `["myschema", "orders"]`.
//!
//! Only [`Token::Ident`] tokens are considered identifiers. [`Token::Keyword`]
//! tokens (e.g., `SELECT`, `FROM`) are not resolved. Tokens inside string
//! literals are also ignored.
//!
//! ## Phase B — Resolve to file location
//!
//! [`resolve_reference()`] takes the identifier parts and the project model,
//! constructs an [`ObjectId`] using the same 1/2/3-part resolution as
//! [`ObjectId::from_item_name()`], looks up the object in the project, and
//! returns the file path from [`typed::DatabaseObject::path`].
//!
//! ## Examples
//!
//! ```text
//! -- File: models/mydb/public/bar.sql
//! CREATE VIEW bar AS SELECT * FROM foo;
//!                                  ^^^
//!                            cursor here
//!
//! Phase A: ["foo"]
//! Phase B: default_db="mydb", default_schema="public"
//!        → ObjectId("mydb.public.foo")
//!        → models/mydb/public/foo.sql
//! ```

use crate::project::object_id::ObjectId;
use crate::project::planned;
use mz_sql_lexer::lexer::{self, Token};
use std::path::Path;
use tower_lsp::lsp_types::{Location, Range, Url};

/// Find the dot-qualified identifier chain at the given byte offset.
///
/// Returns `None` if the cursor is not on an identifier token (e.g., on a
/// keyword, operator, string literal, dot, or whitespace).
///
/// # Arguments
/// * `text` — SQL source text.
/// * `byte_offset` — Cursor position as a byte offset into `text`.
///
/// # Returns
/// A vec of identifier parts (e.g., `["schema", "object"]`) or `None`.
pub fn find_reference_at_position(text: &str, byte_offset: usize) -> Option<Vec<String>> {
    let tokens = lexer::lex(text).ok()?;
    if tokens.is_empty() {
        return None;
    }

    // Find the index of the token containing byte_offset.
    let token_idx = find_token_at_offset(&tokens, byte_offset, text.len())?;

    // The token under the cursor must be an identifier or a keyword usable as identifier.
    let token = &tokens[token_idx];
    let ident_text = extract_ident_text(token)?;

    // Collect the full dot-qualified chain by scanning backward and forward.
    let mut parts = vec![ident_text];

    // Scan backward: expect alternating Dot, Ident/Keyword
    let mut i = token_idx;
    while i >= 2 {
        if matches!(tokens[i - 1].kind, Token::Dot) {
            if let Some(s) = extract_ident_text(&tokens[i - 2]) {
                parts.insert(0, s);
                i -= 2;
            } else {
                break;
            }
        } else {
            break;
        }
    }

    // Scan forward: expect alternating Dot, Ident/Keyword
    let mut j = token_idx;
    while j + 2 < tokens.len() {
        if matches!(tokens[j + 1].kind, Token::Dot) {
            if let Some(s) = extract_ident_text(&tokens[j + 2]) {
                parts.push(s);
                j += 2;
            } else {
                break;
            }
        } else {
            break;
        }
    }

    // Only return chains that are part of a dot-qualified name, or a single
    // ident (not a bare keyword like SELECT/FROM).
    if parts.len() == 1 && matches!(token.kind, Token::Keyword(_)) {
        return None;
    }

    Some(parts)
}

/// Extract identifier text from a token.
///
/// Returns the identifier string for `Token::Ident`, and the keyword string
/// for `Token::Keyword` (since SQL keywords can be used as identifiers in
/// dot-qualified names like `mydb.schema.t`). Returns `None` for all other
/// token types.
fn extract_ident_text(token: &lexer::PosToken) -> Option<String> {
    match &token.kind {
        Token::Ident(s) => Some(s.to_string()),
        Token::Keyword(kw) => Some(kw.as_str().to_lowercase()),
        _ => None,
    }
}

/// Find the token index containing the given byte offset.
fn find_token_at_offset(
    tokens: &[lexer::PosToken],
    byte_offset: usize,
    text_len: usize,
) -> Option<usize> {
    for (i, token) in tokens.iter().enumerate() {
        let start = token.offset;
        let end = if i + 1 < tokens.len() {
            tokens[i + 1].offset
        } else {
            text_len
        };
        if byte_offset >= start && byte_offset < end {
            return Some(i);
        }
    }
    None
}

/// Resolve identifier parts to an [`ObjectId`] using the file's path context.
///
/// Derives the default database/schema from the file's path relative to the
/// project root (expects `models/<database>/<schema>/` structure), then
/// constructs an [`ObjectId`] using 1/2/3-part name resolution.
pub fn resolve_object_id(parts: &[String], file_uri: &Url, root: &Path) -> Option<ObjectId> {
    let (default_db, default_schema) = ObjectId::default_db_schema_from_uri(file_uri, root)?;

    match parts.len() {
        1 => Some(ObjectId::new(default_db, default_schema, parts[0].clone())),
        2 => Some(ObjectId::new(
            default_db,
            parts[0].clone(),
            parts[1].clone(),
        )),
        3 => Some(ObjectId::new(
            parts[0].clone(),
            parts[1].clone(),
            parts[2].clone(),
        )),
        _ => None,
    }
}

/// Resolve identifier parts to a file location using the project model.
///
/// Derives the default database/schema from the file's path relative to the
/// project root (expects `models/<database>/<schema>/` structure), then
/// constructs an [`ObjectId`] and looks it up in the project.
///
/// # Returns
/// A [`Location`] pointing to the start of the defining file, or `None` if the
/// reference cannot be resolved (unknown object, external dependency, etc.).
pub fn resolve_reference(
    parts: &[String],
    file_uri: &Url,
    root: &Path,
    project: &planned::Project,
) -> Option<Location> {
    let id = resolve_object_id(parts, file_uri, root)?;
    let obj = project.find_object(&id)?;
    let file_path = root.join("models").join(&obj.typed_object.path);
    let uri = Url::from_file_path(&file_path).ok()?;

    Some(Location {
        uri,
        range: Range::default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Phase A tests ─────────────────────────────────────────────

    #[test]
    fn unqualified_identifier() {
        let text = "SELECT * FROM foo";
        // "foo" starts at byte 14
        let parts = find_reference_at_position(text, 14).unwrap();
        assert_eq!(parts, vec!["foo"]);
    }

    #[test]
    fn schema_qualified_identifier() {
        let text = "SELECT * FROM myschema.orders";
        // cursor on "orders" (byte 23)
        let parts = find_reference_at_position(text, 23).unwrap();
        assert_eq!(parts, vec!["myschema", "orders"]);
    }

    #[test]
    fn schema_qualified_cursor_on_schema() {
        let text = "SELECT * FROM myschema.orders";
        // cursor on "myschema" (byte 14)
        let parts = find_reference_at_position(text, 14).unwrap();
        assert_eq!(parts, vec!["myschema", "orders"]);
    }

    #[test]
    fn fully_qualified_identifier() {
        let text = "SELECT * FROM db.schema.t";
        // cursor on "t" (byte 24)
        let parts = find_reference_at_position(text, 24).unwrap();
        assert_eq!(parts, vec!["db", "schema", "t"]);
    }

    #[test]
    fn cursor_on_dot_returns_none() {
        let text = "SELECT * FROM myschema.orders";
        // The dot is at byte 22
        let result = find_reference_at_position(text, 22);
        assert!(result.is_none());
    }

    #[test]
    fn cursor_on_string_literal_returns_none() {
        let text = "SELECT 'hello' FROM foo";
        // cursor inside string literal (byte 8)
        let result = find_reference_at_position(text, 8);
        assert!(result.is_none());
    }

    #[test]
    fn cursor_on_keyword_returns_none() {
        let text = "SELECT * FROM foo";
        // cursor on "SELECT" (byte 0)
        let result = find_reference_at_position(text, 0);
        assert!(result.is_none());
    }

    #[test]
    fn cursor_at_end_of_file_returns_none() {
        let text = "SELECT 1";
        let result = find_reference_at_position(text, text.len());
        assert!(result.is_none());
    }

    #[test]
    fn empty_file_returns_none() {
        let result = find_reference_at_position("", 0);
        assert!(result.is_none());
    }

    #[test]
    fn quoted_identifier() {
        let text = r#"SELECT * FROM "My Table""#;
        // cursor inside quoted ident
        let parts = find_reference_at_position(text, 15).unwrap();
        assert_eq!(parts, vec!["My Table"]);
    }

    // ── Phase B tests ─────────────────────────────────────────────

    #[test]
    fn resolve_one_part_name() {
        let (root, project) = build_test_project();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let location =
            resolve_reference(&["foo".to_string()], &file_uri, root.path(), &project).unwrap();
        let expected_path = root.path().join("models/mydb/public/foo.sql");
        assert_eq!(location.uri, Url::from_file_path(expected_path).unwrap());
    }

    #[test]
    fn resolve_two_part_name() {
        let (root, project) = build_test_project();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let location = resolve_reference(
            &["public".to_string(), "foo".to_string()],
            &file_uri,
            root.path(),
            &project,
        )
        .unwrap();
        let expected_path = root.path().join("models/mydb/public/foo.sql");
        assert_eq!(location.uri, Url::from_file_path(expected_path).unwrap());
    }

    #[test]
    fn resolve_three_part_name() {
        let (root, project) = build_test_project();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let location = resolve_reference(
            &["mydb".to_string(), "public".to_string(), "foo".to_string()],
            &file_uri,
            root.path(),
            &project,
        )
        .unwrap();
        let expected_path = root.path().join("models/mydb/public/foo.sql");
        assert_eq!(location.uri, Url::from_file_path(expected_path).unwrap());
    }

    #[test]
    fn resolve_unknown_name_returns_none() {
        let (root, project) = build_test_project();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let result = resolve_reference(
            &["nonexistent".to_string()],
            &file_uri,
            root.path(),
            &project,
        );
        assert!(result.is_none());
    }

    #[test]
    fn resolve_cross_schema_reference() {
        let (root, project) = build_test_project_cross_schema();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/other/baz.sql")).unwrap();

        let location = resolve_reference(
            &["public".to_string(), "foo".to_string()],
            &file_uri,
            root.path(),
            &project,
        )
        .unwrap();
        let expected_path = root.path().join("models/mydb/public/foo.sql");
        assert_eq!(location.uri, Url::from_file_path(expected_path).unwrap());
    }

    // ── Test helpers ──────────────────────────────────────────────

    fn build_test_project() -> (tempfile::TempDir, planned::Project) {
        let root = tempfile::tempdir().unwrap();
        let models = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::write(models.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;").unwrap();
        std::fs::write(
            models.join("bar.sql"),
            "CREATE VIEW bar AS SELECT * FROM foo;",
        )
        .unwrap();
        write_project_toml(root.path());

        let project = crate::project::plan_sync(root.path(), "default", None, &Default::default())
            .expect("project should compile");
        (root, project)
    }

    fn build_test_project_cross_schema() -> (tempfile::TempDir, planned::Project) {
        let root = tempfile::tempdir().unwrap();

        let storage = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();

        let other = root.path().join("models/mydb/other");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(
            other.join("baz.sql"),
            "CREATE VIEW baz AS SELECT * FROM mydb.public.foo;",
        )
        .unwrap();
        write_project_toml(root.path());

        let project = crate::project::plan_sync(root.path(), "default", None, &Default::default())
            .expect("project should compile");
        (root, project)
    }

    fn write_project_toml(root: &Path) {
        std::fs::write(root.join("project.toml"), "[project]\nname = \"test\"\n").unwrap();
    }
}
