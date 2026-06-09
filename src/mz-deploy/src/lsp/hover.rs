// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
//! and types) using the build artifact database and `types.lock` data. The result is
//! formatted as a Markdown table for display in the editor.
//!
//! ## Function Hover
//!
//! If the identifier is not a project object, [`resolve_hover`] falls back to
//! the function registry ([`functions::lookup`]), which is derived from
//! `mz_sql::func`. Single unqualified names that match a built-in show the
//! function kind and one line per overload signature.
//!
//! ### Resolution
//!
//! 1. Identifier parts are resolved to an `ObjectId` using the same
//!    1/2/3-part convention as [`goto_definition::resolve_reference()`].
//! 2. The object is looked up in the `ProjectCache` (SQLite) to confirm
//!    existence and determine its kind (view, materialized view, table, etc.).
//! 3. Column schemas are retrieved via two-tier lookup: `ProjectCache`
//!    (SQLite) first, then `Types` (types.lock) as fallback.
//!
//! ### Output
//!
//! - **Object with cached columns** — Shows the object kind, fully-qualified
//!   name, and a column table (name, type, nullable). If the object has a
//!   `COMMENT ON` description, it appears as a paragraph after the header.
//!   If any column has a `COMMENT ON COLUMN` description, a `Description`
//!   column is added to the table.
//! - **Object without cached columns** — Shows just the object kind, name,
//!   and source file path.
//! - **Unknown identifier** — Returns `None`.

use super::{functions, goto_definition};
use crate::project::compiler::cache::ProjectCache;
use crate::project::syntax::variables::find_variable_at_position;
use crate::types::Types;
use std::collections::BTreeMap;
use std::path::Path;
use tower_lsp::lsp_types::{Hover, HoverContents, MarkupContent, MarkupKind, Url};

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
) -> Option<Hover> {
    let (name, _start, _len) = find_variable_at_position(text, offset)?;
    let value = variables.get(&name)?.to_string();

    Some(Hover {
        contents: HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value,
        }),
        range: None,
    })
}

/// Resolve hover information for an identifier.
///
/// Takes the dot-qualified identifier `parts` (from
/// [`goto_definition::find_reference_at_position()`]), resolves it against the
/// `ProjectCache` (SQLite), and formats the output schema as Markdown.
///
/// Column schemas are retrieved via two-tier lookup: `ProjectCache` first
/// (typecheck columns), then `Types` (types.lock) as fallback.
///
/// # Returns
/// `Some(Hover)` with Markdown content if the identifier resolves to a known
/// object, `None` otherwise.
pub fn resolve_hover(
    parts: &[String],
    file_uri: &Url,
    root: &Path,
    project_cache: &ProjectCache,
    types_lock: &Types,
) -> Option<Hover> {
    let id = goto_definition::resolve_object_id(parts, file_uri, root);

    let cached_obj = id.as_ref().and_then(|id| project_cache.get_object(id));

    // If not a project object, try function lookup (single unqualified name)
    if cached_obj.is_none() {
        if let Some(func_hover) = resolve_function_hover(parts) {
            return Some(func_hover);
        }
        return None;
    }

    let id = id.unwrap();
    let cached_obj = cached_obj.unwrap();
    let kind = cached_obj.kind;
    let fqn = id.to_string();

    let comments = &cached_obj.comments;
    let description = comments
        .iter()
        .find(|c| c.target_column.is_none())
        .map(|c| c.text.clone())
        .or_else(|| types_lock.comments.get(&id).cloned());
    let comments = &cached_obj.comments;
    let mut column_comments: BTreeMap<_, _> = comments
        .iter()
        .filter_map(|c| {
            c.target_column
                .as_ref()
                .map(|col| (col.clone(), c.text.clone()))
        })
        .collect();
    let columns = project_cache
        .get_columns(&id)
        .or_else(|| types_lock.get_table(&id).cloned());

    // Fall back to types.lock column comments when no project cache comments exist
    if column_comments.is_empty() {
        if let Some(cols) = &columns {
            for (name, col_type) in cols {
                if let Some(comment) = &col_type.comment {
                    column_comments.insert(name.clone(), comment.clone());
                }
            }
        }
    }

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
                for (name, col_type) in &cols {
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
                for (name, col_type) in &cols {
                    let nullable = if col_type.nullable { "" } else { "not null " };
                    md.push_str(&format!("| {} | {} {}|\n", name, col_type.r#type, nullable));
                }
            }
            md
        }
        _ => {
            let file_path = &cached_obj.file_path;
            format!("**{kind}** `{fqn}`\n\n*{file_path}*")
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

/// Resolve hover for a SQL function name.
///
/// Matches single unqualified names against the function registry. Returns a
/// Markdown tooltip showing the function kind and every overload signature,
/// one per line in a code block.
fn resolve_function_hover(parts: &[String]) -> Option<Hover> {
    if parts.len() != 1 {
        return None;
    }
    let name = &parts[0];
    let func = functions::lookup(name)?;
    let kind = match func.kind {
        functions::FunctionKind::Scalar => "scalar function",
        functions::FunctionKind::Aggregate => "aggregate function",
        functions::FunctionKind::Window => "window function",
        functions::FunctionKind::Table => "table function",
    };

    let sigs = func.signatures.join("\n");
    let markdown = format!("**{kind}**\n\n```\n{sigs}\n```");

    Some(Hover {
        contents: HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value: markdown,
        }),
        range: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::compiler::cache::ProjectCache;
    use crate::project::ir::object_id::ObjectId;
    use crate::types::ColumnType;
    use std::collections::BTreeMap;

    fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[mz_ore::test]
    fn variable_hover_resolved() {
        let variables = vars(&[("cluster", "ontology")]);
        let sql = "IN CLUSTER :cluster AS";
        let hover = resolve_variable_hover(sql, 11, &variables).unwrap();
        let text = extract_markdown(&hover);
        assert_eq!(text, "ontology");
    }

    #[mz_ore::test]
    fn variable_hover_unresolved_returns_none() {
        let sql = "IN CLUSTER :cluster AS";
        assert!(resolve_variable_hover(sql, 11, &BTreeMap::new()).is_none());
    }

    #[mz_ore::test]
    fn variable_hover_not_on_variable() {
        let sql = "SELECT 1 FROM t";
        assert!(resolve_variable_hover(sql, 5, &BTreeMap::new()).is_none());
    }

    #[mz_ore::test]
    fn hover_with_cached_columns() {
        let (root, cache, types_lock) = build_test_project_with_types_lock();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let hover = resolve_hover(
            &["foo".to_string()],
            &file_uri,
            root.path(),
            &cache,
            &types_lock,
        )
        .unwrap();

        let text = extract_markdown(&hover);
        assert!(text.contains("**view** `mydb.public.foo`"));
        assert!(text.contains("| id | integer not null |"));
        assert!(text.contains("| name | text |"));
    }

    #[mz_ore::test]
    fn hover_without_cache_shows_kind_and_path() {
        let (root, cache) = build_test_project_cache();
        let empty_types = Types::default();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let hover = resolve_hover(
            &["foo".to_string()],
            &file_uri,
            root.path(),
            &cache,
            &empty_types,
        )
        .unwrap();

        let text = extract_markdown(&hover);
        assert!(text.contains("**view** `mydb.public.foo`"));
        assert!(text.contains("mydb/public/foo.sql"));
        // Should not contain a table header
        assert!(!text.contains("| Column |"));
    }

    #[mz_ore::test]
    fn hover_unknown_identifier_returns_none() {
        let (root, cache) = build_test_project_cache();
        let empty_types = Types::default();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let result = resolve_hover(
            &["nonexistent".to_string()],
            &file_uri,
            root.path(),
            &cache,
            &empty_types,
        );
        assert!(result.is_none());
    }

    #[mz_ore::test]
    fn hover_cross_schema_reference() {
        let (root, cache, types_lock) = build_test_project_cross_schema_with_types_lock();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/other/baz.sql")).unwrap();

        let hover = resolve_hover(
            &["public".to_string(), "foo".to_string()],
            &file_uri,
            root.path(),
            &cache,
            &types_lock,
        )
        .unwrap();

        let text = extract_markdown(&hover);
        assert!(text.contains("**table** `mydb.public.foo`"));
        assert!(text.contains("| id | integer |"));
    }

    #[mz_ore::test]
    fn hover_with_description_and_column_comments() {
        let (root, cache, types_lock) = build_test_project_with_comments_and_types_lock(
            "CREATE VIEW foo AS SELECT 1 AS id, 'x' AS name;\n\
             COMMENT ON VIEW foo IS 'All incoming customer orders';\n\
             COMMENT ON COLUMN foo.id IS 'Primary key';",
        );
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let hover = resolve_hover(
            &["foo".to_string()],
            &file_uri,
            root.path(),
            &cache,
            &types_lock,
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

    #[mz_ore::test]
    fn hover_with_description_only() {
        let (root, cache, types_lock) = build_test_project_with_comments_and_types_lock(
            "CREATE VIEW foo AS SELECT 1 AS id, 'x' AS name;\n\
             COMMENT ON VIEW foo IS 'A helpful description';",
        );
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let hover = resolve_hover(
            &["foo".to_string()],
            &file_uri,
            root.path(),
            &cache,
            &types_lock,
        )
        .unwrap();

        let text = extract_markdown(&hover);
        assert!(text.contains("A helpful description"));
        // No Description column since no column comments
        assert!(!text.contains("| Description |"));
        assert!(text.contains("| Column | Type |"));
    }

    #[mz_ore::test]
    fn hover_no_comments_unchanged() {
        let (root, cache, types_lock) = build_test_project_with_types_lock();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let hover = resolve_hover(
            &["foo".to_string()],
            &file_uri,
            root.path(),
            &cache,
            &types_lock,
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

    #[mz_ore::test]
    fn hover_types_lock_comments_on_external_dep() {
        let (root, cache) = build_test_project_cache();
        let mut types_lock = Types::default();

        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: false,
                position: 0,
                comment: Some("Primary key".to_string()),
            },
        );
        columns.insert(
            "name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
                position: 1,
                comment: None,
            },
        );
        types_lock
            .tables
            .insert("mydb.public.foo".parse::<ObjectId>().unwrap(), columns);
        types_lock.comments.insert(
            "mydb.public.foo".parse::<ObjectId>().unwrap(),
            "External orders table".to_string(),
        );

        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let hover = resolve_hover(
            &["foo".to_string()],
            &file_uri,
            root.path(),
            &cache,
            &types_lock,
        )
        .unwrap();

        let text = extract_markdown(&hover);
        assert!(
            text.contains("External orders table"),
            "should show object comment from types.lock"
        );
        assert!(
            text.contains("| Column | Type | Description |"),
            "should have Description column"
        );
        assert!(
            text.contains("Primary key"),
            "should show column comment from types.lock"
        );
        // name has no comment — empty description cell
        assert!(text.contains("| name | text |  |"));
    }

    fn extract_markdown(hover: &Hover) -> &str {
        match &hover.contents {
            HoverContents::Markup(m) => &m.value,
            _ => panic!("expected markup content"),
        }
    }

    /// Compile a project and open a ProjectCache from its SQLite DB.
    fn build_test_project_cache() -> (tempfile::TempDir, ProjectCache) {
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

        let _project = crate::project::plan_sync(
            &crate::fs::FileSystem::new(),
            root.path(),
            None,
            None,
            &Default::default(),
        )
        .expect("project should compile");
        let cache = ProjectCache::open(root.path(), "", None, &Default::default())
            .expect("cache should open")
            .expect("cache DB should exist");
        (root, cache)
    }

    /// Build project cache with a types.lock providing column schemas for foo.
    fn build_test_project_with_types_lock() -> (tempfile::TempDir, ProjectCache, Types) {
        let (root, cache) = build_test_project_cache();

        let mut types_lock = Types::default();
        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: false,
                position: 0,
                comment: None,
            },
        );
        columns.insert(
            "name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
                position: 1,
                comment: None,
            },
        );
        types_lock
            .tables
            .insert("mydb.public.foo".parse::<ObjectId>().unwrap(), columns);

        (root, cache, types_lock)
    }

    fn build_test_project_cross_schema_with_types_lock() -> (tempfile::TempDir, ProjectCache, Types)
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

        let _project = crate::project::plan_sync(
            &crate::fs::FileSystem::new(),
            root.path(),
            None,
            None,
            &Default::default(),
        )
        .expect("project should compile");
        let cache = ProjectCache::open(root.path(), "", None, &Default::default())
            .expect("cache should open")
            .expect("cache DB should exist");

        let mut types_lock = Types::default();
        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: true,
                position: 0,
                comment: None,
            },
        );
        types_lock
            .tables
            .insert("mydb.public.foo".parse::<ObjectId>().unwrap(), columns);

        (root, cache, types_lock)
    }

    fn build_test_project_with_comments_and_types_lock(
        foo_sql: &str,
    ) -> (tempfile::TempDir, ProjectCache, Types) {
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

        let _project = crate::project::plan_sync(
            &crate::fs::FileSystem::new(),
            root.path(),
            None,
            None,
            &Default::default(),
        )
        .expect("project should compile");
        let cache = ProjectCache::open(root.path(), "", None, &Default::default())
            .expect("cache should open")
            .expect("cache DB should exist");

        let mut types_lock = Types::default();
        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: false,
                position: 0,
                comment: None,
            },
        );
        columns.insert(
            "name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
                position: 1,
                comment: None,
            },
        );
        types_lock
            .tables
            .insert("mydb.public.foo".parse::<ObjectId>().unwrap(), columns);

        (root, cache, types_lock)
    }

    fn write_project_toml(root: &Path) {
        std::fs::write(root.join("project.toml"), "[project]\nname = \"test\"\n").unwrap();
    }
}
