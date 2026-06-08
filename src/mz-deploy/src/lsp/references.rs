// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Find-references for SQL identifiers.
//!
//! Given a cursor position on an identifier, finds all project objects that
//! **depend on** the referenced object. This is the inverse of go-to-definition:
//! where go-to-definition answers "where is this defined?", find-references
//! answers "who uses this?"
//!
//! ## Algorithm
//!
//! 1. Resolve identifier parts to an [`ObjectId`] (reuses
//!    [`goto_definition::resolve_object_id`]).
//! 2. Query [`ProjectCache::get_dependents`] to find all objects that depend on
//!    the target.
//! 3. For each dependent, look up its source file path via
//!    [`ProjectCache::get_object`] and return an LSP [`Location`].
//!
//! ## Includes the definition
//!
//! When `include_declaration` is true (standard LSP behavior), the defining
//! file is prepended to the results so the user sees the full picture.

use crate::project::compiler::cache::ProjectCache;
use std::path::Path;
use tower_lsp::lsp_types::{Location, Range, Url};

use super::goto_definition;

/// Find all project objects that reference the identified object.
///
/// Queries the [`ProjectCache`] for dependents of the resolved object. Returns
/// a [`Location`] for each dependent's source file. If `include_declaration` is
/// true, the defining file itself is included as the first result.
///
/// Returns an empty vec if the identifier cannot be resolved or has no
/// dependents.
pub(super) fn find_references(
    parts: &[String],
    file_uri: &Url,
    root: &Path,
    project_cache: &ProjectCache,
    include_declaration: bool,
) -> Vec<Location> {
    let id = match goto_definition::resolve_object_id(parts, file_uri, root) {
        Some(id) => id,
        None => return Vec::new(),
    };

    let mut locations = Vec::new();

    if include_declaration {
        if let Some(cached_obj) = project_cache.get_object(&id) {
            if let Some(loc) = file_location(root, &cached_obj.file_path) {
                locations.push(loc);
            }
        }
    }

    for dep_id in project_cache.get_dependents(&id) {
        if let Some(cached_obj) = project_cache.get_object(&dep_id) {
            if let Some(loc) = file_location(root, &cached_obj.file_path) {
                locations.push(loc);
            }
        }
    }

    locations
}

/// Build a [`Location`] pointing to the start of a source file.
fn file_location(root: &Path, file_path: &str) -> Option<Location> {
    let full_path = root.join(file_path);
    let uri = Url::from_file_path(&full_path).ok()?;
    Some(Location {
        uri,
        range: Range::default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::compiler::cache::ProjectCache;
    use std::path::Path;

    #[test]
    fn object_with_dependents() {
        let (root, cache) = build_test_project_cache();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        // "foo" is referenced by "bar"
        let locations =
            find_references(&["foo".to_string()], &file_uri, root.path(), &cache, false);
        assert_eq!(locations.len(), 1);
        let expected = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();
        assert_eq!(locations[0].uri, expected);
    }

    #[test]
    fn object_with_dependents_include_declaration() {
        let (root, cache) = build_test_project_cache();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let locations = find_references(&["foo".to_string()], &file_uri, root.path(), &cache, true);
        assert_eq!(locations.len(), 2);
        // First result is the definition itself.
        let def_uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();
        assert_eq!(locations[0].uri, def_uri);
    }

    #[test]
    fn object_with_no_dependents() {
        let (root, cache) = build_test_project_cache();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();

        // "bar" has no dependents
        let locations =
            find_references(&["bar".to_string()], &file_uri, root.path(), &cache, false);
        assert!(locations.is_empty());
    }

    #[test]
    fn unknown_identifier_returns_empty() {
        let (root, cache) = build_test_project_cache();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();

        let locations = find_references(
            &["nonexistent".to_string()],
            &file_uri,
            root.path(),
            &cache,
            false,
        );
        assert!(locations.is_empty());
    }

    #[test]
    fn transitive_dependents() {
        let (root, cache) = build_chain_project_cache();
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/c.sql")).unwrap();

        // "a" is depended on by "b" (directly), not "c" (c depends on b, not a)
        let locations = find_references(&["a".to_string()], &file_uri, root.path(), &cache, false);
        assert_eq!(locations.len(), 1);
        let expected = Url::from_file_path(root.path().join("models/mydb/public/b.sql")).unwrap();
        assert_eq!(locations[0].uri, expected);
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

    /// Compile a chain project (a -> b -> c) and open a ProjectCache.
    fn build_chain_project_cache() -> (tempfile::TempDir, ProjectCache) {
        let root = tempfile::tempdir().unwrap();
        let models = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::write(models.join("a.sql"), "CREATE VIEW a AS SELECT 1 AS id;").unwrap();
        std::fs::write(models.join("b.sql"), "CREATE VIEW b AS SELECT * FROM a;").unwrap();
        std::fs::write(models.join("c.sql"), "CREATE VIEW c AS SELECT * FROM b;").unwrap();
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
