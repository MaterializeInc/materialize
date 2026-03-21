//! Hover information for SQL identifiers and variable references.
//!
//! ## Variable Hover
//!
//! When the cursor is on a psql-style variable reference (`:foo`, `:'foo'`,
//! `:"foo"`), [`resolve_variable_hover`] returns a tooltip showing the variable
//! name, its resolved value (or "undefined"), and the active profile name.
//!
//! ## Object Hover
//!
//! When the cursor hovers over an identifier that references a database object,
//! [`resolve_hover`] resolves the identifier to its output schema (column names
//! and types) using the `types.cache` and `types.lock` data. The result is
//! formatted as a Markdown table for display in the editor.
//!
//! ### Resolution
//!
//! 1. Identifier parts are resolved to an [`ObjectId`] using the same
//!    1/2/3-part convention as [`goto_definition::resolve_reference()`].
//! 2. The object is looked up in the project model to confirm existence and
//!    determine its kind (view, materialized view, table, etc.).
//! 3. Column schemas are retrieved from the [`Types`] cache.
//!
//! ### Output
//!
//! - **Object with cached columns** — Shows the object kind, fully-qualified
//!   name, and a column table (name, type, nullable). If the object has a
//!   `COMMENT ON` description, it appears as a paragraph after the header.
//!   If any column has a `COMMENT ON COLUMN` description, a `Description`
//!   column is added to the table. If the object has constraints (PRIMARY KEY,
//!   UNIQUE, FOREIGN KEY), they are rendered as a bullet list after the table.
//! - **Object without cached columns** — Shows just the object kind, name,
//!   and source file path. Constraints are shown after the path if present.
//! - **Unknown identifier** — Returns `None`.

use crate::project::constraint::default_constraint_name;
use crate::project::planned;
use crate::project::variables::find_variable_at_position;
use crate::types::Types;
use mz_sql_parser::ast::{CommentObjectType, CommentStatement, CreateConstraintStatement, Raw};
use std::collections::BTreeMap;
use std::path::Path;
use tower_lsp::lsp_types::{Hover, HoverContents, MarkupContent, MarkupKind, Url};

use super::goto_definition;

/// Resolve hover information for a variable reference at the given byte offset.
///
/// If `offset` falls inside a resolved variable reference (`:name`, `:'name'`,
/// or `:"name"`), returns a [`Hover`] showing the variable name, its value,
/// and the active profile. Undefined variables return `None` — their diagnostic
/// already covers the error.
///
/// Returns `None` if the offset is not inside a variable reference, or if the
/// variable is undefined.
pub fn resolve_variable_hover(
    text: &str,
    offset: usize,
    variables: &BTreeMap<String, String>,
    profile_name: &str,
) -> Option<Hover> {
    let (name, _start, _len) = find_variable_at_position(text, offset)?;
    let value = variables.get(&name)?;

    let markdown = format!(
        "**variable** `:{name}`\n\n\
         **Value:** `{value}`\n\
         **Profile:** `{profile_name}`"
    );

    Some(Hover {
        contents: HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value: markdown,
        }),
        range: None,
    })
}

/// Resolve hover information for an identifier.
///
/// Takes the dot-qualified identifier `parts` (from
/// [`goto_definition::find_reference_at_position()`]), resolves it against the
/// project model, and formats the output schema as Markdown.
///
/// # Returns
/// `Some(Hover)` with Markdown content if the identifier resolves to a known
/// object, `None` otherwise.
pub fn resolve_hover(
    parts: &[String],
    file_uri: &Url,
    root: &Path,
    project: &planned::Project,
    types_cache: &Types,
) -> Option<Hover> {
    let id = goto_definition::resolve_object_id(parts, file_uri, root)?;
    let obj = project.find_object(&id)?;
    let kind = obj.typed_object.stmt.kind();
    let fqn = id.to_string();

    let description = extract_description(&obj.typed_object.comments);
    let column_comments = extract_column_comments(&obj.typed_object.comments);
    let columns = types_cache.get_table(&fqn);

    let constraints_md = format_constraints(&obj.typed_object.constraints, &id.object);

    let markdown = match columns {
        Some(cols) if !cols.is_empty() => {
            let mut md = format!("**{kind}** `{fqn}`\n\n");
            if let Some(desc) = &description {
                md.push_str(&format!("{desc}\n\n"));
            }
            let has_any_comment = cols.keys().any(|name| column_comments.contains_key(name));
            if has_any_comment {
                md.push_str("| Column | Type | Description |\n");
                md.push_str("|--------|------|-------------|\n");
                for (name, col_type) in cols {
                    let nullable = if col_type.nullable { "" } else { "not null " };
                    let comment = column_comments.get(name).map(|s| s.as_str()).unwrap_or("");
                    md.push_str(&format!(
                        "| {} | {} {}| {} |\n",
                        name, col_type.r#type, nullable, comment
                    ));
                }
            } else {
                md.push_str("| Column | Type |\n");
                md.push_str("|--------|------|\n");
                for (name, col_type) in cols {
                    let nullable = if col_type.nullable { "" } else { "not null " };
                    md.push_str(&format!("| {} | {} {}|\n", name, col_type.r#type, nullable));
                }
            }
            if let Some(c) = &constraints_md {
                md.push_str(&format!("\n**Constraints**\n\n{c}"));
            }
            md
        }
        _ => {
            let path = &obj.typed_object.path;
            let mut md = format!("**{kind}** `{fqn}`\n\n*{path}*", path = path.display());
            if let Some(c) = &constraints_md {
                md.push_str(&format!("\n\n**Constraints**\n\n{c}"));
            }
            md
        }
    };

    Some(Hover {
        contents: HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value: markdown,
        }),
        range: None,
    })
}

/// Format constraints as a Markdown bullet list.
///
/// Returns `None` when the constraints vec is empty. Each constraint is
/// rendered as `- **{kind}** \`{name}\` ({columns}) — enforced/not enforced`,
/// with an additional `→ \`{ref_obj} ({ref_cols})\`` for foreign keys.
fn format_constraints(
    constraints: &[CreateConstraintStatement<Raw>],
    object_name: &str,
) -> Option<String> {
    if constraints.is_empty() {
        return None;
    }
    let mut md = String::new();
    for c in constraints {
        let name = c
            .name
            .as_ref()
            .map(|n| n.to_string())
            .unwrap_or_else(|| default_constraint_name(object_name, &c.columns, &c.kind));
        let cols: Vec<String> = c.columns.iter().map(|col| col.to_string()).collect();
        let cols_str = cols.join(", ");
        let enforced = if c.enforced {
            "enforced"
        } else {
            "not enforced"
        };

        md.push_str(&format!("- **{}** `{}` ({})", c.kind, name, cols_str));
        if let Some(refs) = &c.references {
            let ref_cols: Vec<String> = refs.columns.iter().map(|col| col.to_string()).collect();
            md.push_str(&format!(" → `{} ({})`", refs.object, ref_cols.join(", ")));
        }
        md.push_str(&format!(" — {enforced}\n"));
    }
    Some(md)
}

/// Extract the object-level description from comment statements.
///
/// Returns the text of the first non-column `COMMENT ON` statement, if any.
fn extract_description(comments: &[CommentStatement<Raw>]) -> Option<String> {
    for c in comments {
        if !matches!(c.object, CommentObjectType::Column { .. }) {
            return c.comment.clone();
        }
    }
    None
}

/// Build a map of column name → comment text from `COMMENT ON COLUMN` statements.
fn extract_column_comments(comments: &[CommentStatement<Raw>]) -> BTreeMap<String, String> {
    comments
        .iter()
        .filter_map(|c| {
            if let CommentObjectType::Column { name } = &c.object {
                c.comment
                    .as_ref()
                    .map(|text| (name.column.to_string(), text.clone()))
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColumnType;
    use std::collections::BTreeMap;

    // ── Variable hover tests ─────────────────────────────────────────

    fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn variable_hover_resolved() {
        let variables = vars(&[("cluster", "analytics")]);
        let sql = "IN CLUSTER :cluster AS";
        let hover = resolve_variable_hover(sql, 11, &variables, "staging").unwrap();
        let text = extract_markdown(&hover);
        assert!(text.contains("**variable** `:cluster`"));
        assert!(text.contains("**Value:** `analytics`"));
        assert!(text.contains("**Profile:** `staging`"));
    }

    #[test]
    fn variable_hover_unresolved_returns_none() {
        let sql = "IN CLUSTER :cluster AS";
        assert!(resolve_variable_hover(sql, 11, &BTreeMap::new(), "staging").is_none());
    }

    #[test]
    fn variable_hover_not_on_variable() {
        let sql = "SELECT 1 FROM t";
        assert!(resolve_variable_hover(sql, 5, &BTreeMap::new(), "default").is_none());
    }

    // ── Object hover tests ──────────────────────────────────────────

    #[test]
    fn hover_with_cached_columns() {
        let (root, project, types_cache) = build_test_project_with_cache();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let hover = resolve_hover(
            &["foo".to_string()],
            &file_uri,
            root.path(),
            &project,
            &types_cache,
        )
        .unwrap();

        let text = extract_markdown(&hover);
        assert!(text.contains("**view** `mydb.public.foo`"));
        assert!(text.contains("| id | integer not null |"));
        assert!(text.contains("| name | text |"));
    }

    #[test]
    fn hover_without_cache_shows_kind_and_path() {
        let (root, project) = build_test_project();
        let empty_cache = Types::default();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let hover = resolve_hover(
            &["foo".to_string()],
            &file_uri,
            root.path(),
            &project,
            &empty_cache,
        )
        .unwrap();

        let text = extract_markdown(&hover);
        assert!(text.contains("**view** `mydb.public.foo`"));
        assert!(text.contains("mydb/public/foo.sql"));
        // Should not contain a table header
        assert!(!text.contains("| Column |"));
    }

    #[test]
    fn hover_unknown_identifier_returns_none() {
        let (root, project) = build_test_project();
        let empty_cache = Types::default();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let result = resolve_hover(
            &["nonexistent".to_string()],
            &file_uri,
            root.path(),
            &project,
            &empty_cache,
        );
        assert!(result.is_none());
    }

    #[test]
    fn hover_cross_schema_reference() {
        let (root, project, types_cache) = build_test_project_cross_schema_with_cache();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/other/baz.sql")).unwrap();

        let hover = resolve_hover(
            &["public".to_string(), "foo".to_string()],
            &file_uri,
            root.path(),
            &project,
            &types_cache,
        )
        .unwrap();

        let text = extract_markdown(&hover);
        assert!(text.contains("**table** `mydb.public.foo`"));
        assert!(text.contains("| id | integer |"));
    }

    #[test]
    fn hover_with_description_and_column_comments() {
        let (root, project, types_cache) = build_test_project_with_comments(
            "CREATE VIEW foo AS SELECT 1 AS id, 'x' AS name;\n\
             COMMENT ON VIEW foo IS 'All incoming customer orders';\n\
             COMMENT ON COLUMN foo.id IS 'Primary key';",
        );
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let hover = resolve_hover(
            &["foo".to_string()],
            &file_uri,
            root.path(),
            &project,
            &types_cache,
        )
        .unwrap();

        let text = extract_markdown(&hover);
        assert!(text.contains("**view** `mydb.public.foo`"));
        assert!(text.contains("All incoming customer orders"));
        assert!(text.contains("| Column | Type | Description |"));
        assert!(text.contains("| id | integer not null | Primary key |"));
        // name has no column comment — empty description cell
        assert!(text.contains("| name | text |  |"));
    }

    #[test]
    fn hover_with_description_only() {
        let (root, project, types_cache) = build_test_project_with_comments(
            "CREATE VIEW foo AS SELECT 1 AS id, 'x' AS name;\n\
             COMMENT ON VIEW foo IS 'A helpful description';",
        );
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let hover = resolve_hover(
            &["foo".to_string()],
            &file_uri,
            root.path(),
            &project,
            &types_cache,
        )
        .unwrap();

        let text = extract_markdown(&hover);
        assert!(text.contains("A helpful description"));
        // No Description column since no column comments
        assert!(!text.contains("| Description |"));
        assert!(text.contains("| Column | Type |"));
    }

    #[test]
    fn hover_no_comments_unchanged() {
        let (root, project, types_cache) = build_test_project_with_cache();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let hover = resolve_hover(
            &["foo".to_string()],
            &file_uri,
            root.path(),
            &project,
            &types_cache,
        )
        .unwrap();

        let text = extract_markdown(&hover);
        // No description paragraph
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], "**view** `mydb.public.foo`");
        assert_eq!(lines[1], "");
        assert!(lines[2].starts_with("| Column | Type |"));
        // No Description column
        assert!(!text.contains("Description"));
    }

    #[test]
    fn hover_with_enforced_pk_constraint() {
        let (root, project, types_cache) = build_test_project_with_comments(
            "CREATE VIEW foo AS SELECT 1 AS id;\n\
             CREATE PRIMARY KEY NOT ENFORCED foo_pk ON foo (id);",
        );
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let hover = resolve_hover(
            &["foo".to_string()],
            &file_uri,
            root.path(),
            &project,
            &types_cache,
        )
        .unwrap();

        let text = extract_markdown(&hover);
        assert!(
            text.contains("**Constraints**"),
            "missing constraints header"
        );
        assert!(text.contains("**PRIMARY KEY**"), "missing PK kind");
        assert!(text.contains("`foo_pk`"), "missing constraint name");
        assert!(text.contains("(id)"), "missing columns");
    }

    #[test]
    fn hover_with_not_enforced_constraint() {
        let (root, project, types_cache) = build_test_project_with_comments(
            "CREATE VIEW foo AS SELECT 1 AS id;\n\
             CREATE PRIMARY KEY NOT ENFORCED foo_pk ON foo (id);",
        );
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let hover = resolve_hover(
            &["foo".to_string()],
            &file_uri,
            root.path(),
            &project,
            &types_cache,
        )
        .unwrap();

        let text = extract_markdown(&hover);
        assert!(
            text.contains("— not enforced"),
            "missing 'not enforced' status"
        );
    }

    #[test]
    fn hover_with_fk_constraint() {
        let (root, project, types_cache) = build_test_project_with_fk();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let hover = resolve_hover(
            &["orders".to_string()],
            &file_uri,
            root.path(),
            &project,
            &types_cache,
        )
        .unwrap();

        let text = extract_markdown(&hover);
        assert!(text.contains("**FOREIGN KEY**"), "missing FK kind");
        assert!(text.contains("→"), "missing arrow for FK reference");
        assert!(text.contains("foo"), "missing referenced object");
    }

    #[test]
    fn hover_no_constraints_omits_section() {
        let (root, project, types_cache) = build_test_project_with_cache();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let hover = resolve_hover(
            &["foo".to_string()],
            &file_uri,
            root.path(),
            &project,
            &types_cache,
        )
        .unwrap();

        let text = extract_markdown(&hover);
        assert!(
            !text.contains("Constraints"),
            "should not have constraints section"
        );
    }

    // ── Helpers ──────────────────────────────────────────────────────

    fn extract_markdown(hover: &Hover) -> &str {
        match &hover.contents {
            HoverContents::Markup(m) => &m.value,
            _ => panic!("expected markup content"),
        }
    }

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

    fn build_test_project_with_cache() -> (tempfile::TempDir, planned::Project, Types) {
        let (root, project) = build_test_project();

        let mut types_cache = Types::default();
        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: false,
            },
        );
        columns.insert(
            "name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
            },
        );
        types_cache
            .tables
            .insert("mydb.public.foo".to_string(), columns);

        (root, project, types_cache)
    }

    fn build_test_project_cross_schema_with_cache() -> (tempfile::TempDir, planned::Project, Types)
    {
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

        let mut types_cache = Types::default();
        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: true,
            },
        );
        types_cache
            .tables
            .insert("mydb.public.foo".to_string(), columns);

        (root, project, types_cache)
    }

    fn build_test_project_with_comments(
        foo_sql: &str,
    ) -> (tempfile::TempDir, planned::Project, Types) {
        let root = tempfile::tempdir().unwrap();
        let models = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::write(models.join("foo.sql"), foo_sql).unwrap();
        std::fs::write(
            models.join("bar.sql"),
            "CREATE VIEW bar AS SELECT * FROM foo;",
        )
        .unwrap();
        write_project_toml(root.path());

        let project = crate::project::plan_sync(root.path(), "default", None, &Default::default())
            .expect("project should compile");

        let mut types_cache = Types::default();
        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: false,
            },
        );
        columns.insert(
            "name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
            },
        );
        types_cache
            .tables
            .insert("mydb.public.foo".to_string(), columns);

        (root, project, types_cache)
    }

    fn build_test_project_with_fk() -> (tempfile::TempDir, planned::Project, Types) {
        let root = tempfile::tempdir().unwrap();
        let models = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::write(models.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;").unwrap();
        std::fs::write(
            models.join("orders.sql"),
            "CREATE VIEW orders AS SELECT 1 AS id, 1 AS user_id;\n\
             CREATE FOREIGN KEY NOT ENFORCED orders_fk ON orders (user_id) REFERENCES foo (id);",
        )
        .unwrap();
        std::fs::write(
            models.join("bar.sql"),
            "CREATE VIEW bar AS SELECT * FROM orders;",
        )
        .unwrap();
        write_project_toml(root.path());

        let project = crate::project::plan_sync(root.path(), "default", None, &Default::default())
            .expect("project should compile");

        let mut types_cache = Types::default();
        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: false,
            },
        );
        columns.insert(
            "user_id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: true,
            },
        );
        types_cache
            .tables
            .insert("mydb.public.orders".to_string(), columns);

        (root, project, types_cache)
    }

    fn write_project_toml(root: &Path) {
        std::fs::write(root.join("project.toml"), "[project]\nname = \"test\"\n").unwrap();
    }
}
