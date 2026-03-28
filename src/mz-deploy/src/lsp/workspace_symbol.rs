//! Workspace symbol search across all project objects.
//!
//! Provides fuzzy-find over every object in the project, enabling editor
//! commands like "Go to Symbol in Workspace" (`Ctrl+T` / `Cmd+T`). Each
//! result carries the object name, kind, file location, and a container
//! label (`database.schema`) for grouping context.
//!
//! ## Filtering
//!
//! - An empty query returns all project objects.
//! - A non-empty query matches by case-insensitive substring on the
//!   fully-qualified name (`database.schema.object`).
//! - External dependencies and constraint MVs are excluded (no file path).

use crate::project::planned;
use std::path::Path;
use tower_lsp::lsp_types::{Location, Range, SymbolInformation, Url};

use super::symbol_kind::object_kind_to_symbol_kind;

/// Search project objects by name.
///
/// Returns a [`SymbolInformation`] for each matching object. Constraint MVs
/// and external dependencies are excluded.
#[allow(deprecated)] // SymbolInformation::deprecated field is deprecated but required
pub fn workspace_symbols(
    query: &str,
    project: &planned::Project,
    root: &Path,
) -> Vec<SymbolInformation> {
    let query_lower = query.to_lowercase();
    let models_dir = root.join("models");

    project
        .iter_objects()
        .filter(|obj| !obj.is_constraint_mv)
        .filter(|obj| {
            query_lower.is_empty() || obj.id.to_string().to_lowercase().contains(&query_lower)
        })
        .filter_map(|obj| {
            let full_path = models_dir.join(&obj.typed_object.path);
            let uri = Url::from_file_path(&full_path).ok()?;
            let kind = object_kind_to_symbol_kind(obj.typed_object.stmt.kind());
            let container = format!("{}.{}", obj.id.database(), obj.id.schema());

            Some(SymbolInformation {
                name: obj.id.object().to_string(),
                kind,
                tags: None,
                deprecated: None,
                location: Location {
                    uri,
                    range: Range::default(),
                },
                container_name: Some(container),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn empty_query_returns_all_objects() {
        let (root, project) = build_test_project();
        let symbols = workspace_symbols("", &project, root.path());
        assert_eq!(symbols.len(), 2); // foo and bar
    }

    #[test]
    fn query_matches_by_substring() {
        let (root, project) = build_test_project();
        let symbols = workspace_symbols("foo", &project, root.path());
        assert_eq!(symbols.len(), 1);
        assert_eq!(symbols[0].name, "foo");
    }

    #[test]
    fn query_case_insensitive() {
        let (root, project) = build_test_project();
        let symbols = workspace_symbols("FOO", &project, root.path());
        assert_eq!(symbols.len(), 1);
        assert_eq!(symbols[0].name, "foo");
    }

    #[test]
    fn query_matches_schema_in_fqn() {
        let (root, project) = build_test_project();
        let symbols = workspace_symbols("public", &project, root.path());
        // Both "mydb.public.foo" and "mydb.public.bar" match "public"
        assert_eq!(symbols.len(), 2);
    }

    #[test]
    fn no_matches_returns_empty() {
        let (root, project) = build_test_project();
        let symbols = workspace_symbols("nonexistent", &project, root.path());
        assert!(symbols.is_empty());
    }

    #[test]
    fn symbols_have_container_name() {
        let (root, project) = build_test_project();
        let symbols = workspace_symbols("foo", &project, root.path());
        assert_eq!(symbols[0].container_name.as_deref(), Some("mydb.public"));
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

    fn write_project_toml(root: &Path) {
        std::fs::write(root.join("project.toml"), "[project]\nname = \"test\"\n").unwrap();
    }
}
