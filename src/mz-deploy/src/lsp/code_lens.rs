// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code lenses for unit tests and explain plans.
//!
//! Places clickable links above SQL statements:
//!
//! - **"Run Test"** above each `EXECUTE UNIT TEST` statement — dispatches
//!   `mz-deploy.runTest` with the test filter string.
//! - **"Explain"** above `CREATE MATERIALIZED VIEW` statements and named
//!   `CREATE INDEX` statements — dispatches `mz-deploy.runExplain` with the
//!   fully qualified target (`database.schema.object` or
//!   `database.schema.object#index_name`).

use crate::project::compiler::cache::ProjectCache;
use crate::project::ir::object_id::ObjectId;
use crate::types::ObjectKind;
use std::path::Path;
use tower_lsp::lsp_types::*;

/// Build code lenses for tests and explain targets in the file.
///
/// Returns an empty vec if the file is not under `models/<db>/<schema>/` or
/// the object is not found in the project cache.
pub(super) fn code_lenses(
    file_uri: &Url,
    file_text: &str,
    root: &Path,
    project_cache: Option<&ProjectCache>,
) -> Vec<CodeLens> {
    let project_cache = match project_cache {
        Some(c) => c,
        None => return Vec::new(),
    };

    let (default_db, default_schema) = match ObjectId::default_db_schema_from_uri(file_uri, root) {
        Some(pair) => pair,
        None => return Vec::new(),
    };

    let object_name = file_uri
        .to_file_path()
        .ok()
        .and_then(|p| p.file_stem().map(|s| s.to_string_lossy().into_owned()));
    let object_name = match object_name {
        Some(name) => name,
        None => return Vec::new(),
    };

    let file_object_id = ObjectId::new(default_db.clone(), default_schema.clone(), object_name);
    let fqn = file_object_id.to_string();

    let cached_obj = match project_cache.get_object(&file_object_id) {
        Some(obj) => obj,
        None => return Vec::new(),
    };

    let mut lenses = Vec::new();

    // Explain lens for materialized views
    if cached_obj.kind == ObjectKind::MaterializedView {
        if let Some(line) = find_statement_line(file_text, "create materialized view") {
            lenses.push(CodeLens {
                range: Range::new(Position::new(line, 0), Position::new(line, 0)),
                command: Some(Command {
                    title: "\u{25b6} Explain".to_string(),
                    command: "mz-deploy.runExplain".to_string(),
                    arguments: Some(vec![serde_json::Value::String(fqn.clone())]),
                }),
                data: None,
            });
        }
    }

    // Explain lenses for named indexes
    for index in &cached_obj.indexes {
        if index.name.is_empty() {
            continue;
        }
        let index_name = &index.name;
        if let Some(line) = find_index_line(file_text, index_name) {
            let target = format!("{}#{}", fqn, index_name);
            lenses.push(CodeLens {
                range: Range::new(Position::new(line, 0), Position::new(line, 0)),
                command: Some(Command {
                    title: "\u{25b6} Explain".to_string(),
                    command: "mz-deploy.runExplain".to_string(),
                    arguments: Some(vec![serde_json::Value::String(target)]),
                }),
                data: None,
            });
        }
    }

    // Test lenses
    let tests = project_cache.get_tests(&file_object_id);
    for test in &tests {
        let test_name = &test.name;
        if let Some(line) = find_test_line(file_text, test_name) {
            let filter = format!("{}#{}", fqn, test_name);
            lenses.push(CodeLens {
                range: Range::new(Position::new(line, 0), Position::new(line, 0)),
                command: Some(Command {
                    title: "\u{25b6} Run Test".to_string(),
                    command: "mz-deploy.runTest".to_string(),
                    arguments: Some(vec![serde_json::Value::String(filter)]),
                }),
                data: None,
            });
        }
    }

    lenses
}

/// Find the 0-based line number where a statement keyword appears.
///
/// Case-insensitive scan for lines starting with `keyword` (e.g.,
/// `"create materialized view"`).
fn find_statement_line(file_text: &str, keyword: &str) -> Option<u32> {
    for (i, line) in file_text.lines().enumerate() {
        if line.trim().to_lowercase().starts_with(keyword) {
            return u32::try_from(i).ok();
        }
    }
    None
}

/// Find the 0-based line number where `CREATE INDEX <name>` appears.
fn find_index_line(file_text: &str, index_name: &str) -> Option<u32> {
    let target = format!("create index {}", index_name);
    let target_lower = target.to_lowercase();

    for (i, line) in file_text.lines().enumerate() {
        if line.trim().to_lowercase().starts_with(&target_lower) {
            return u32::try_from(i).ok();
        }
    }
    None
}

/// Find the 0-based line number where `EXECUTE UNIT TEST <name>` appears.
fn find_test_line(file_text: &str, test_name: &str) -> Option<u32> {
    let target = format!("execute unit test {}", test_name);
    let target_lower = target.to_lowercase();

    for (i, line) in file_text.lines().enumerate() {
        let trimmed = line.trim().to_lowercase();
        if trimmed.starts_with(&target_lower) {
            return u32::try_from(i).ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_project_toml(root: &Path) {
        std::fs::write(root.join("project.toml"), "[project]\nname = \"test\"\n").unwrap();
    }

    fn build_cache(root: &tempfile::TempDir) -> ProjectCache {
        write_project_toml(root.path());
        let _project = crate::project::plan_sync(
            &crate::fs::FileSystem::new(),
            root.path(),
            None,
            None,
            &Default::default(),
        )
        .expect("project should compile");
        ProjectCache::open(root.path(), "", None, &Default::default())
            .expect("cache should open")
            .expect("cache DB should exist")
    }

    #[test]
    fn test_find_test_line_basic() {
        let text = "CREATE VIEW foo AS SELECT 1;\n\nEXECUTE UNIT TEST my_test\nAS SELECT 1;\n";
        assert_eq!(find_test_line(text, "my_test"), Some(2));
    }

    #[test]
    fn test_find_test_line_case_insensitive() {
        let text = "execute unit test my_test\nAS SELECT 1;\n";
        assert_eq!(find_test_line(text, "my_test"), Some(0));
    }

    #[test]
    fn test_find_test_line_not_found() {
        let text = "CREATE VIEW foo AS SELECT 1;\n";
        assert_eq!(find_test_line(text, "nonexistent"), None);
    }

    #[test]
    fn test_single_test() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("foo.sql"),
            "CREATE VIEW foo AS SELECT 1 AS id;\n\nEXECUTE UNIT TEST basic_test\nFOR mydb.public.foo\nEXPECTED(id integer) AS (\n    SELECT 1\n);\n",
        )
        .unwrap();
        let cache = build_cache(&root);

        let uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let file_text =
            std::fs::read_to_string(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let lenses = code_lenses(&uri, &file_text, root.path(), Some(&cache));

        assert_eq!(lenses.len(), 1);
        let lens = &lenses[0];
        assert_eq!(lens.range.start.line, 2);
        let cmd = lens.command.as_ref().unwrap();
        assert_eq!(cmd.command, "mz-deploy.runTest");
        assert_eq!(
            cmd.arguments.as_ref().unwrap()[0],
            serde_json::Value::String("mydb.public.foo#basic_test".to_string())
        );
    }

    #[test]
    fn test_multiple_tests() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("foo.sql"),
            "CREATE VIEW foo AS SELECT 1 AS id;\n\nEXECUTE UNIT TEST test_one\nFOR mydb.public.foo\nEXPECTED(id integer) AS (\n    SELECT 1\n);\n\nEXECUTE UNIT TEST test_two\nFOR mydb.public.foo\nEXPECTED(id integer) AS (\n    SELECT 1\n);\n",
        )
        .unwrap();
        let cache = build_cache(&root);

        let uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let file_text =
            std::fs::read_to_string(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let lenses = code_lenses(&uri, &file_text, root.path(), Some(&cache));

        assert_eq!(lenses.len(), 2);

        let filters: Vec<String> = lenses
            .iter()
            .filter_map(|l| {
                l.command
                    .as_ref()
                    .and_then(|c| c.arguments.as_ref())
                    .and_then(|a| a[0].as_str().map(String::from))
            })
            .collect();
        assert!(filters.contains(&"mydb.public.foo#test_one".to_string()));
        assert!(filters.contains(&"mydb.public.foo#test_two".to_string()));

        // Different lines
        assert_ne!(lenses[0].range.start.line, lenses[1].range.start.line);
    }

    #[test]
    fn test_no_tests() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;\n").unwrap();
        let cache = build_cache(&root);

        let uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let file_text =
            std::fs::read_to_string(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let lenses = code_lenses(&uri, &file_text, root.path(), Some(&cache));

        assert!(lenses.is_empty());
    }

    #[test]
    fn test_file_not_in_project() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;\n").unwrap();
        let cache = build_cache(&root);

        // URI points to a file outside models/
        let outside = root.path().join("random/foo.sql");
        std::fs::create_dir_all(root.path().join("random")).unwrap();
        std::fs::write(&outside, "SELECT 1;").unwrap();
        let uri = Url::from_file_path(&outside).unwrap();
        let file_text = std::fs::read_to_string(&outside).unwrap();
        let lenses = code_lenses(&uri, &file_text, root.path(), Some(&cache));

        assert!(lenses.is_empty());
    }
}
