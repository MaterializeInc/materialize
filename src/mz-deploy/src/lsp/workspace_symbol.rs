// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
//! - External dependencies are excluded (no file path).

use crate::project::compiler::cache::ProjectCache;
use std::path::Path;
use tower_lsp::lsp_types::{Location, Range, SymbolInformation, Url};

use super::symbol_kind::object_kind_to_symbol_kind;

/// Search project objects by name.
///
/// Returns a [`SymbolInformation`] for each matching object. External
/// dependencies are excluded.
#[allow(deprecated)] // SymbolInformation::deprecated field is deprecated but required
pub(super) fn workspace_symbols(
    query: &str,
    project_cache: &ProjectCache,
    root: &Path,
) -> Vec<SymbolInformation> {
    let query_lower = query.to_lowercase();

    project_cache
        .list_objects()
        .into_iter()
        .filter(|summary| {
            query_lower.is_empty() || summary.fqn.to_lowercase().contains(&query_lower)
        })
        .filter_map(|summary| {
            let full_path = root.join(&summary.file_path);
            let uri = Url::from_file_path(&full_path).ok()?;
            let kind = object_kind_to_symbol_kind(summary.kind);
            let container = format!("{}.{}", summary.database, summary.schema);

            Some(SymbolInformation {
                name: summary.name,
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
    use crate::project::compiler::cache::ProjectCache;
    use std::path::Path;

    #[mz_ore::test]
    fn empty_query_returns_all_objects() {
        let (root, cache) = build_test_project_cache();
        let symbols = workspace_symbols("", &cache, root.path());
        assert_eq!(symbols.len(), 2); // foo and bar
    }

    #[mz_ore::test]
    fn query_matches_by_substring() {
        let (root, cache) = build_test_project_cache();
        let symbols = workspace_symbols("foo", &cache, root.path());
        assert_eq!(symbols.len(), 1);
        assert_eq!(symbols[0].name, "foo");
    }

    #[mz_ore::test]
    fn query_case_insensitive() {
        let (root, cache) = build_test_project_cache();
        let symbols = workspace_symbols("FOO", &cache, root.path());
        assert_eq!(symbols.len(), 1);
        assert_eq!(symbols[0].name, "foo");
    }

    #[mz_ore::test]
    fn query_matches_schema_in_fqn() {
        let (root, cache) = build_test_project_cache();
        let symbols = workspace_symbols("public", &cache, root.path());
        // Both "mydb.public.foo" and "mydb.public.bar" match "public"
        assert_eq!(symbols.len(), 2);
    }

    #[mz_ore::test]
    fn no_matches_returns_empty() {
        let (root, cache) = build_test_project_cache();
        let symbols = workspace_symbols("nonexistent", &cache, root.path());
        assert!(symbols.is_empty());
    }

    #[mz_ore::test]
    fn symbols_have_container_name() {
        let (root, cache) = build_test_project_cache();
        let symbols = workspace_symbols("foo", &cache, root.path());
        assert_eq!(symbols[0].container_name.as_deref(), Some("mydb.public"));
    }

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

    fn write_project_toml(root: &Path) {
        std::fs::write(root.join("project.toml"), "[project]\nname = \"test\"\n").unwrap();
    }
}
