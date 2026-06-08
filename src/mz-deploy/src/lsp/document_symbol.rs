// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Document symbol provider for `.sql` files.
//!
//! Returns the structural outline of a `.sql` file: the main CREATE statement
//! as the root symbol, with supporting statements (indexes, grants, comments,
//! unit tests) as children. This powers the editor's "Outline" view and
//! breadcrumb navigation.
//!
//! ## Symbol hierarchy
//!
//! ```text
//! CREATE VIEW orders (root)
//!   ├─ INDEX orders_id_idx
//!   ├─ GRANT SELECT TO analyst
//!   ├─ COMMENT ON VIEW orders
//!   └─ TEST test_no_nulls
//! ```
//!
//! ## Range handling
//!
//! The root symbol spans the entire document. Child symbols use zero-width
//! ranges at the start of the file since exact byte offsets for individual
//! supporting statements are not tracked in the typed IR. The hierarchy is
//! the primary value — range precision can be refined later.

use crate::project::compiler::cache::ProjectCache;
use crate::project::ir::object_id::ObjectId;
use std::path::Path;
use tower_lsp::lsp_types::{DocumentSymbol, Range, SymbolKind, Url};

use super::symbol_kind::object_kind_to_symbol_kind;

/// Build the document symbol outline for a `.sql` file.
///
/// Returns a single root symbol (the main CREATE statement) with children for
/// each supporting statement, or an empty vec if the file doesn't correspond
/// to a known project object.
#[allow(deprecated)] // DocumentSymbol::deprecated field is deprecated but required
pub(super) fn document_symbols(
    file_uri: &Url,
    root: &Path,
    project_cache: &ProjectCache,
) -> Vec<DocumentSymbol> {
    let (default_db, default_schema) = match ObjectId::default_db_schema_from_uri(file_uri, root) {
        Some(pair) => pair,
        None => return Vec::new(),
    };

    let file_path = match file_uri.to_file_path() {
        Ok(p) => p,
        Err(_) => return Vec::new(),
    };
    let object_name = match file_path.file_stem().and_then(|s| s.to_str()) {
        Some(name) => name.to_string(),
        None => return Vec::new(),
    };

    let id = ObjectId::new(default_db, default_schema, object_name);
    let fqn = id.to_string();
    let cached_obj = match project_cache.get_object(&id) {
        Some(o) => o,
        None => return Vec::new(),
    };

    let kind = object_kind_to_symbol_kind(cached_obj.kind);

    let mut children = Vec::new();

    // Indexes
    for idx in &cached_obj.indexes {
        let name = if idx.name.is_empty() {
            "index".to_string()
        } else {
            idx.name.clone()
        };
        children.push(child_symbol(format!("INDEX {name}"), SymbolKind::KEY));
    }

    // Grants
    for g in &cached_obj.grants {
        children.push(child_symbol(
            format!("GRANT {} TO {}", g.privilege, g.grantee),
            SymbolKind::EVENT,
        ));
    }

    // Comments
    for c in &cached_obj.comments {
        let label = if let Some(col) = &c.target_column {
            format!("COMMENT ON COLUMN {col}")
        } else {
            "COMMENT".to_string()
        };
        children.push(child_symbol(label, SymbolKind::STRING));
    }

    // Unit tests
    let tests = project_cache.get_tests(&id);
    for t in &tests {
        children.push(child_symbol(format!("TEST {}", t.name), SymbolKind::METHOD));
    }

    vec![DocumentSymbol {
        name: fqn,
        detail: Some(format!("{}", cached_obj.kind)),
        kind,
        tags: None,
        deprecated: None,
        range: Range::default(),
        selection_range: Range::default(),
        children: if children.is_empty() {
            None
        } else {
            Some(children)
        },
    }]
}

/// Create a child symbol with zero-width range.
#[allow(deprecated)] // DocumentSymbol::deprecated field is deprecated but required
fn child_symbol(name: String, kind: SymbolKind) -> DocumentSymbol {
    DocumentSymbol {
        name,
        detail: None,
        kind,
        tags: None,
        deprecated: None,
        range: Range::default(),
        selection_range: Range::default(),
        children: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn simple_view_single_root_symbol() {
        let (root, cache) = build_test_cache("CREATE VIEW foo AS SELECT 1 AS id;");
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();

        let symbols = document_symbols(&file_uri, root.path(), &cache);
        assert_eq!(symbols.len(), 1);
        assert_eq!(symbols[0].name, "mydb.public.foo");
        assert_eq!(symbols[0].detail.as_deref(), Some("view"));
        assert!(symbols[0].children.is_none());
    }

    #[test]
    fn view_with_index_and_comment() {
        let (root, cache) = build_test_cache(
            "CREATE VIEW foo AS SELECT 1 AS id;\n\
             CREATE DEFAULT INDEX IN CLUSTER default ON foo;\n\
             COMMENT ON VIEW foo IS 'A test view';",
        );
        let file_uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();

        let symbols = document_symbols(&file_uri, root.path(), &cache);
        assert_eq!(symbols.len(), 1);
        let children = symbols[0].children.as_ref().unwrap();
        assert_eq!(children.len(), 2);
        // Index child
        assert!(children[0].name.contains("INDEX"));
        assert_eq!(children[0].kind, SymbolKind::KEY);
        // Comment child
        assert_eq!(children[1].name, "COMMENT");
        assert_eq!(children[1].kind, SymbolKind::STRING);
    }

    #[test]
    fn unknown_file_returns_empty() {
        let (root, cache) = build_test_cache("CREATE VIEW foo AS SELECT 1 AS id;");
        let file_uri =
            Url::from_file_path(root.path().join("models/mydb/public/unknown.sql")).unwrap();

        let symbols = document_symbols(&file_uri, root.path(), &cache);
        assert!(symbols.is_empty());
    }

    fn build_test_cache(foo_sql: &str) -> (tempfile::TempDir, ProjectCache) {
        let root = tempfile::tempdir().unwrap();
        let models = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::write(models.join("foo.sql"), foo_sql).unwrap();
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
