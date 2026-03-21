//! Context-aware keyword and object name completion for the LSP server.
//!
//! Two kinds of completion items are produced:
//!
//! - **Keywords** — A static list built once at startup from the lexer's keyword
//!   map ([`mz_sql_lexer::keywords::KEYWORDS`]). Returned verbatim for every
//!   completion request; the editor's fuzzy filter handles prefix narrowing.
//!   Excluded when the user is typing a qualified name (i.e., the prefix
//!   contains one or more dots).
//!
//! - **Object names** — Dynamic completions derived from the project model and
//!   external dependencies. Computed per-request because the correct label
//!   depends on both the project state (changes on save) and the file URI
//!   (different file → different default database/schema → different
//!   qualification level).
//!
//! ## Prefix-Aware Qualification and Label Stripping
//!
//! The qualification level of object labels adapts to the dot-qualified prefix
//! the user has already typed. When the prefix contains dots, the label is
//! stripped so the editor's default word-replacement (which replaces the word
//! after the last dot) inserts the correct completion.
//!
//! | Prefix dots | Label                                                                 |
//! |-------------|-----------------------------------------------------------------------|
//! | 0           | Minimum qualification: bare if same-schema, `schema.object` if        |
//! |             | cross-schema, `db.schema.object` if cross-database                    |
//! | 1+          | Remainder after the last dot in the prefix (see below)                |
//!
//! ## Disambiguation
//!
//! A single-dot prefix like `mydb.` is ambiguous — it could be a database or
//! schema prefix. Each object is matched against candidates at increasing
//! qualification levels (`schema.object`, then `db.schema.object`). The first
//! candidate whose lowercase form starts with the prefix text wins. The label
//! is then the portion of the matched candidate after the last dot in the
//! prefix, so the editor's word-replacement produces the correct full name.
//!
//! Examples:
//! - Prefix `public.f` → matches `public.foo` → label `foo`
//! - Prefix `mydb.p` → matches `mydb.public.foo` → label `public.foo`
//! - Prefix `mydb.public.f` → matches `mydb.public.foo` → label `foo`
//!
//! ## Prefix Filtering
//!
//! Objects are filtered server-side: only items whose qualified name starts
//! with the typed prefix (case-insensitive) are returned. For `dots == 0` all
//! objects are returned and the editor handles fuzzy filtering.
//!
//! ## Sort Order
//!
//! Closer objects sort first: same-schema (`"1_"`), cross-schema (`"2_"`),
//! cross-database (`"3_"`). Keywords have no sort prefix and appear after
//! object names by default.

use crate::project::object_id::ObjectId;
use crate::project::planned;
use crate::types::{ObjectKind, Types};
use mz_sql_lexer::keywords::KEYWORDS;
use std::path::Path;
use tower_lsp::lsp_types::{CompletionItem, CompletionItemKind, Position, Url};

/// Describes the dot-qualified prefix at the cursor position.
pub struct PrefixContext<'a> {
    /// Number of dots in the typed prefix (0, 1, or 2+).
    pub dots: usize,
    /// The raw prefix text the user has typed (e.g., `"public.f"`).
    pub text: &'a str,
}

/// Find the dot-qualified identifier prefix at the cursor position.
///
/// Scans backward from `position` through identifier characters (alphanumeric,
/// underscore) and dots to determine what the user has typed so far. Returns
/// a [`PrefixContext`] with the dot count and a borrowed slice of the prefix.
pub fn prefix_context(text: &str, position: Position) -> PrefixContext<'_> {
    // Convert Position (line/character) to byte offset.
    let pos_line = usize::try_from(position.line).unwrap_or(0);
    let pos_char = usize::try_from(position.character).unwrap_or(0);
    let mut byte_offset = 0;
    for (i, line) in text.split('\n').enumerate() {
        if i == pos_line {
            byte_offset += pos_char;
            break;
        }
        // +1 for the newline character.
        byte_offset += line.len() + 1;
    }

    // Scan backward through identifier chars and dots.
    let prefix_bytes = &text.as_bytes()[..byte_offset.min(text.len())];
    let mut start = prefix_bytes.len();
    while start > 0 {
        let ch = char::from(prefix_bytes[start - 1]);
        if ch.is_alphanumeric() || ch == '_' || ch == '.' {
            start -= 1;
        } else {
            break;
        }
    }

    let prefix = &text[start..byte_offset.min(text.len())];
    let dots = prefix.chars().filter(|&c| c == '.').count();

    PrefixContext { dots, text: prefix }
}

/// Build completion items for all SQL keywords known to the lexer.
pub fn keyword_completions() -> Vec<CompletionItem> {
    KEYWORDS
        .entries()
        .map(|(_, kw)| CompletionItem {
            label: kw.as_str().to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        })
        .collect()
}

/// Build completion items for project objects and external dependencies.
///
/// Returns an empty vec if the file is not under `models/<database>/<schema>/`
/// (i.e., the default database/schema cannot be determined). The `prefix`
/// controls qualification level and filtering — see module docs.
pub fn object_completions(
    project: &planned::Project,
    types_cache: Option<&Types>,
    file_uri: &Url,
    root: &Path,
    prefix: &PrefixContext<'_>,
) -> Vec<CompletionItem> {
    let (default_db, default_schema) = match ObjectId::default_db_schema_from_uri(file_uri, root) {
        Some(pair) => pair,
        None => return Vec::new(),
    };

    let mut items = Vec::new();

    // Project objects.
    for obj in project.iter_objects() {
        let id = &obj.id;
        let kind = obj.typed_object.stmt.kind();
        if let Some((label, sort_prefix)) =
            qualify_and_filter(id, &default_db, &default_schema, prefix)
        {
            items.push(CompletionItem {
                label,
                kind: Some(object_kind_to_completion_kind(kind)),
                detail: Some(kind.to_string()),
                sort_text: Some(sort_prefix),
                ..Default::default()
            });
        }
    }

    // External dependencies.
    for id in &project.external_dependencies {
        let kind = types_cache
            .map(|tc| tc.get_kind(&id.to_string()))
            .unwrap_or_default();
        if let Some((label, sort_prefix)) =
            qualify_and_filter(id, &default_db, &default_schema, prefix)
        {
            items.push(CompletionItem {
                label,
                kind: Some(object_kind_to_completion_kind(kind)),
                detail: Some(format!("{} (external)", kind)),
                sort_text: Some(sort_prefix),
                ..Default::default()
            });
        }
    }

    items
}

/// Compute the label and sort prefix for an object, filtered by the typed prefix.
///
/// For `dots == 0`, returns minimum qualification (bare name if same-schema,
/// `schema.object` if cross-schema, `db.schema.object` if cross-database).
/// No filtering is applied; the editor handles fuzzy matching.
///
/// For `dots >= 1`, tries matching the object against `schema.object` and
/// `db.schema.object` candidates (in order). Returns `None` if neither
/// candidate starts with the prefix text (case-insensitive). On match, the
/// label is the portion of the candidate after the last dot in the prefix —
/// this is what the editor inserts when replacing the word at cursor.
fn qualify_and_filter(
    id: &ObjectId,
    default_db: &str,
    default_schema: &str,
    prefix: &PrefixContext<'_>,
) -> Option<(String, String)> {
    let sort_key = if id.database() == default_db && id.schema() == default_schema {
        "1"
    } else if id.database() == default_db {
        "2"
    } else {
        "3"
    };

    if prefix.dots == 0 {
        // Minimum qualification, no filtering.
        let label = if id.database() == default_db && id.schema() == default_schema {
            id.object().to_string()
        } else if id.database() == default_db {
            format!("{}.{}", id.schema(), id.object())
        } else {
            format!("{}.{}.{}", id.database(), id.schema(), id.object())
        };
        return Some((label.clone(), format!("{}_{}", sort_key, label)));
    }

    // dots >= 1: try candidates from least to most qualified.
    let candidates = [
        format!("{}.{}", id.schema(), id.object()),
        format!("{}.{}.{}", id.database(), id.schema(), id.object()),
    ];

    let prefix_lower = prefix.text.to_lowercase();
    for candidate in &candidates {
        if candidate.to_lowercase().starts_with(&prefix_lower) {
            // The label is everything after the last dot in the prefix.
            // Since the candidate starts with the prefix (case-insensitive)
            // and identifiers are ASCII, byte positions correspond directly.
            let last_dot = prefix.text.rfind('.').expect("dots >= 1 guarantees a dot");
            let label = candidate[last_dot + 1..].to_string();
            return Some((label, format!("{}_{}", sort_key, candidate)));
        }
    }

    None
}

/// Map an [`ObjectKind`] to the corresponding LSP [`CompletionItemKind`].
fn object_kind_to_completion_kind(kind: ObjectKind) -> CompletionItemKind {
    match kind {
        ObjectKind::Table | ObjectKind::View | ObjectKind::MaterializedView => {
            CompletionItemKind::STRUCT
        }
        ObjectKind::Source | ObjectKind::Sink => CompletionItemKind::EVENT,
        ObjectKind::Secret => CompletionItemKind::CONSTANT,
        ObjectKind::Connection => CompletionItemKind::INTERFACE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// An empty prefix with no dots — the default for tests that don't care
    /// about prefix context.
    fn no_prefix() -> PrefixContext<'static> {
        PrefixContext { dots: 0, text: "" }
    }

    fn write_project_toml(root: &Path) {
        std::fs::write(root.join("project.toml"), "[project]\nname = \"test\"\n").unwrap();
    }

    fn build_project(root: &tempfile::TempDir) -> planned::Project {
        write_project_toml(root.path());
        crate::project::plan_sync(root.path(), "default", None, &Default::default())
            .expect("project should compile")
    }

    // ── prefix_context tests ──────────────────────────────────────────

    #[test]
    fn prefix_no_prefix() {
        let text = "SELECT ";
        let ctx = prefix_context(text, Position::new(0, 7));
        assert_eq!(ctx.dots, 0);
        assert_eq!(ctx.text, "");
    }

    #[test]
    fn prefix_bare_ident() {
        let text = "SELECT foo";
        let ctx = prefix_context(text, Position::new(0, 10));
        assert_eq!(ctx.dots, 0);
        assert_eq!(ctx.text, "foo");
    }

    #[test]
    fn prefix_one_dot() {
        let text = "SELECT schema.foo";
        let ctx = prefix_context(text, Position::new(0, 17));
        assert_eq!(ctx.dots, 1);
        assert_eq!(ctx.text, "schema.foo");
    }

    #[test]
    fn prefix_two_dots() {
        let text = "SELECT db.schema.foo";
        let ctx = prefix_context(text, Position::new(0, 20));
        assert_eq!(ctx.dots, 2);
        assert_eq!(ctx.text, "db.schema.foo");
    }

    #[test]
    fn prefix_mid_line() {
        let text = "SELECT * FROM schema.f";
        let ctx = prefix_context(text, Position::new(0, 22));
        assert_eq!(ctx.dots, 1);
        assert_eq!(ctx.text, "schema.f");
    }

    #[test]
    fn prefix_text_stored() {
        let text = "SELECT public.f";
        let ctx = prefix_context(text, Position::new(0, 15));
        assert_eq!(ctx.dots, 1);
        assert_eq!(ctx.text, "public.f");
    }

    // ── object_completions tests ──────────────────────────────────────

    #[test]
    fn same_schema_bare_name() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;").unwrap();
        std::fs::write(dir.join("bar.sql"), "CREATE VIEW bar AS SELECT * FROM foo;").unwrap();
        let project = build_project(&root);

        let uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();
        let items = object_completions(&project, None, &uri, root.path(), &no_prefix());

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(
            labels.contains(&"foo"),
            "expected bare 'foo', got: {:?}",
            labels
        );
        assert!(
            labels.contains(&"bar"),
            "expected bare 'bar', got: {:?}",
            labels
        );
    }

    #[test]
    fn cross_schema_qualified() {
        let root = tempfile::tempdir().unwrap();
        let public = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&public).unwrap();
        std::fs::write(public.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();

        let other = root.path().join("models/mydb/other");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(
            other.join("baz.sql"),
            "CREATE VIEW baz AS SELECT * FROM mydb.public.foo;",
        )
        .unwrap();
        let project = build_project(&root);

        // URI is in "other" schema, so "foo" from "public" should be schema-qualified.
        let uri = Url::from_file_path(root.path().join("models/mydb/other/baz.sql")).unwrap();
        let items = object_completions(&project, None, &uri, root.path(), &no_prefix());

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(
            labels.contains(&"public.foo"),
            "expected 'public.foo', got: {:?}",
            labels
        );
        // "baz" is same-schema, so bare.
        assert!(
            labels.contains(&"baz"),
            "expected bare 'baz', got: {:?}",
            labels
        );
    }

    #[test]
    fn cross_database_fully_qualified() {
        let root = tempfile::tempdir().unwrap();
        let db1 = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&db1).unwrap();
        std::fs::write(db1.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;").unwrap();

        let db2 = root.path().join("models/otherdb/public");
        std::fs::create_dir_all(&db2).unwrap();
        std::fs::write(db2.join("bar.sql"), "CREATE VIEW bar AS SELECT 1 AS id;").unwrap();
        let project = build_project(&root);

        // URI is in otherdb, so "foo" from mydb should be fully qualified.
        let uri = Url::from_file_path(root.path().join("models/otherdb/public/bar.sql")).unwrap();
        let items = object_completions(&project, None, &uri, root.path(), &no_prefix());

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(
            labels.contains(&"mydb.public.foo"),
            "expected 'mydb.public.foo', got: {:?}",
            labels
        );
    }

    #[test]
    fn external_deps_included() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("foo.sql"),
            "CREATE VIEW foo AS SELECT * FROM mydb.ext.src;",
        )
        .unwrap();

        // Write a types.lock (TOML) that declares the external dep.
        std::fs::write(
            root.path().join("types.lock"),
            "version = 1\n\n\
             [[table]]\n\
             database = \"mydb\"\n\
             schema = \"ext\"\n\
             name = \"src\"\n\
             kind = \"source\"\n\
             \n\
             [[table.columns]]\n\
             name = \"id\"\n\
             type = \"integer\"\n\
             nullable = false\n",
        )
        .unwrap();
        let project = build_project(&root);

        let types_cache = crate::types::load_types_lock(root.path()).unwrap();
        let uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let items = object_completions(
            &project,
            Some(&types_cache),
            &uri,
            root.path(),
            &no_prefix(),
        );

        let ext_items: Vec<_> = items
            .iter()
            .filter(|i| i.detail.as_deref() == Some("source (external)"))
            .collect();
        assert_eq!(ext_items.len(), 1, "expected one external source");
        assert_eq!(ext_items[0].label, "ext.src");
    }

    #[test]
    fn kind_mapping() {
        let root = tempfile::tempdir().unwrap();
        // Storage and computation objects must be in separate schemas.
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("t.sql"), "CREATE TABLE t (id INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM mydb.storage.t;",
        )
        .unwrap();
        let project = build_project(&root);

        let uri = Url::from_file_path(root.path().join("models/mydb/storage/t.sql")).unwrap();
        let items = object_completions(&project, None, &uri, root.path(), &no_prefix());

        let table_item = items.iter().find(|i| i.label == "t").unwrap();
        assert_eq!(table_item.detail.as_deref(), Some("table"));
        assert_eq!(table_item.kind, Some(CompletionItemKind::STRUCT));

        let view_item = items.iter().find(|i| i.label.ends_with("v")).unwrap();
        assert_eq!(view_item.detail.as_deref(), Some("view"));
        assert_eq!(view_item.kind, Some(CompletionItemKind::STRUCT));
    }

    #[test]
    fn file_outside_models_returns_empty() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;").unwrap();
        let project = build_project(&root);

        // URI is outside models/
        let uri = Url::from_file_path(root.path().join("random/file.sql")).unwrap();
        let items = object_completions(&project, None, &uri, root.path(), &no_prefix());
        assert!(items.is_empty());
    }

    #[test]
    fn schema_prefix_strips_label_to_bare_name() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;").unwrap();
        std::fs::write(dir.join("bar.sql"), "CREATE VIEW bar AS SELECT * FROM foo;").unwrap();
        let project = build_project(&root);

        let prefix = PrefixContext {
            dots: 1,
            text: "public.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();
        let items = object_completions(&project, None, &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        // Labels should be bare names since "public." prefix is stripped.
        assert!(
            labels.contains(&"foo"),
            "expected bare 'foo', got: {:?}",
            labels
        );
        assert!(
            labels.contains(&"bar"),
            "expected bare 'bar', got: {:?}",
            labels
        );
    }

    #[test]
    fn db_prefix_disambiguates_to_schema_dot_object() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;").unwrap();
        let project = build_project(&root);

        // User typed "mydb." — a database prefix, not a schema prefix.
        let prefix = PrefixContext {
            dots: 1,
            text: "mydb.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let items = object_completions(&project, None, &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        // Label should be "public.foo" — the remainder after "mydb.".
        assert!(
            labels.contains(&"public.foo"),
            "expected 'public.foo', got: {:?}",
            labels
        );
    }

    #[test]
    fn full_qualification_strips_to_bare_name() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;").unwrap();
        let project = build_project(&root);

        let prefix = PrefixContext {
            dots: 2,
            text: "mydb.public.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let items = object_completions(&project, None, &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(
            labels.contains(&"foo"),
            "expected bare 'foo', got: {:?}",
            labels
        );
    }

    #[test]
    fn prefix_filters_non_matching_objects() {
        let root = tempfile::tempdir().unwrap();
        let public = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&public).unwrap();
        std::fs::write(public.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();

        let other = root.path().join("models/mydb/other");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(
            other.join("baz.sql"),
            "CREATE VIEW baz AS SELECT * FROM mydb.public.foo;",
        )
        .unwrap();
        let project = build_project(&root);

        // Prefix "other." should only match objects in the "other" schema.
        let prefix = PrefixContext {
            dots: 1,
            text: "other.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/other/baz.sql")).unwrap();
        let items = object_completions(&project, None, &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"baz"), "expected 'baz', got: {:?}", labels);
        // "foo" is in "public" schema — should be filtered out.
        assert!(
            !labels.iter().any(|l| l.contains("foo")),
            "expected no 'foo' items, got: {:?}",
            labels
        );
    }
}
