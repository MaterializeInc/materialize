//! Code lenses for unit tests, explain plans, and worksheet execution.
//!
//! Places clickable links above SQL statements:
//!
//! - **"Run Test"** above each `EXECUTE UNIT TEST` statement — dispatches
//!   `mz-deploy.runTest` with the test filter string.
//! - **"Explain"** above `CREATE MATERIALIZED VIEW` statements and named
//!   `CREATE INDEX` statements — dispatches `mz-deploy.runExplain` with the
//!   fully qualified target (`database.schema.object` or
//!   `database.schema.object#index_name`).
//! - **"Execute"** or **"Run"** above each statement in `worksheets/`
//!   files — "Execute" for queries (SELECT, SHOW, EXPLAIN) and SUBSCRIBE,
//!   "Run" for DML/DDL. Both include the cluster name when available
//!   (e.g., "▶ Execute on quickstart"). Dispatches
//!   `mz-deploy.executeStatement` with the statement SQL text. The
//!   extension routes SUBSCRIBE to the `mz-deploy/subscribe` endpoint;
//!   all other statements go through `mz-deploy/execute-query`.
//!
//! ## Worksheet Detection
//!
//! Files inside `<project_root>/worksheets/` are treated as worksheet files.
//! They get "Execute" / "Run" code lenses instead of the project-based
//! "Run Test" / "Explain" lenses.

use crate::project::ast::Statement;
use crate::project::object_id::ObjectId;
use crate::project::planned;
use std::path::Path;
use tower_lsp::lsp_types::*;

/// Returns true if the file is inside the `worksheets/` directory.
pub fn is_worksheet_file(file_uri: &Url, root: &Path) -> bool {
    match file_uri.to_file_path() {
        Ok(path) => path.starts_with(root.join("worksheets")),
        Err(_) => false,
    }
}

/// Build code lenses for each statement in a worksheet file.
///
/// Parses the SQL and emits one lens per statement at its starting line.
/// Queries (SELECT, SHOW, EXPLAIN, SUBSCRIBE) get "Execute"; DML/DDL
/// statements get "Run". The statement's SQL text is passed as the
/// command argument so the extension can send it to the
/// `mz-deploy/execute-query` endpoint.
fn worksheet_code_lenses(file_text: &str, cluster: Option<&str>) -> Vec<CodeLens> {
    let stmts = match mz_sql_parser::parser::parse_statements(file_text) {
        Ok(stmts) => stmts,
        Err(_) => return Vec::new(),
    };

    let mut lenses = Vec::new();
    for stmt in &stmts {
        // Compute byte offset of this statement within the source text.
        // stmt.sql is a slice of file_text, so pointer arithmetic gives the offset.
        #[allow(clippy::as_conversions)]
        let byte_offset = stmt.sql.as_ptr() as usize - file_text.as_ptr() as usize;
        let line = file_text[..byte_offset].matches('\n').count();

        lenses.push(CodeLens {
            range: Range::new(
                Position::new(u32::try_from(line).unwrap_or(0), 0),
                Position::new(u32::try_from(line).unwrap_or(0), 0),
            ),
            command: Some(Command {
                title: {
                    let verb = if super::worksheet::is_dml_statement(&stmt.ast) {
                        "Run"
                    } else {
                        "Execute"
                    };
                    match cluster {
                        Some(c) => format!("\u{25b6} {verb} on {c}"),
                        None => format!("\u{25b6} {verb}"),
                    }
                },
                command: "mz-deploy.executeStatement".to_string(),
                arguments: Some(vec![serde_json::Value::String(stmt.sql.to_string())]),
            }),
            data: None,
        });
    }
    lenses
}

/// Build code lenses for tests and explain targets in the file, or
/// "Execute" lenses for worksheet files.
///
/// Returns an empty vec if the file is not under `models/<db>/<schema>/` or
/// the object is not found in the project (and the file is not a worksheet).
pub fn code_lenses(
    file_uri: &Url,
    file_text: &str,
    root: &Path,
    project: Option<&planned::Project>,
    worksheet_cluster: Option<&str>,
) -> Vec<CodeLens> {
    // Worksheet files get "Execute" lenses instead of project lenses.
    // This works even when the project fails to build.
    if is_worksheet_file(file_uri, root) {
        return worksheet_code_lenses(file_text, worksheet_cluster);
    }

    // Non-worksheet lenses require a successfully built project.
    let project = match project {
        Some(p) => p,
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

    let obj = match project.find_object(&file_object_id) {
        Some(obj) => obj,
        None => return Vec::new(),
    };

    let fqn = file_object_id.to_string();
    let mut lenses = Vec::new();

    // Explain lens for materialized views
    if matches!(obj.typed_object.stmt, Statement::CreateMaterializedView(_)) {
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
    for index in &obj.typed_object.indexes {
        let index_name = match &index.name {
            Some(name) => name.to_string(),
            None => continue,
        };
        if let Some(line) = find_index_line(file_text, &index_name) {
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
    for test in &obj.typed_object.tests {
        let test_name = test.name.to_string();
        if let Some(line) = find_test_line(file_text, &test_name) {
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

    fn write_project_toml(root: &std::path::Path) {
        std::fs::write(root.join("project.toml"), "[project]\nname = \"test\"\n").unwrap();
    }

    fn build_project(root: &tempfile::TempDir) -> planned::Project {
        write_project_toml(root.path());
        crate::project::plan_sync(root.path(), "default", None, &Default::default())
            .expect("project should compile")
    }

    // ── find_test_line tests ─────────────────────────────────────────

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

    // ── code_lenses integration tests ────────────────────────────────

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
        let project = build_project(&root);

        let uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let file_text =
            std::fs::read_to_string(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let lenses = code_lenses(&uri, &file_text, root.path(), Some(&project), None);

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
        let project = build_project(&root);

        let uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let file_text =
            std::fs::read_to_string(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let lenses = code_lenses(&uri, &file_text, root.path(), Some(&project), None);

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
        let project = build_project(&root);

        let uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let file_text =
            std::fs::read_to_string(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let lenses = code_lenses(&uri, &file_text, root.path(), Some(&project), None);

        assert!(lenses.is_empty());
    }

    #[test]
    fn test_file_not_in_project() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;\n").unwrap();
        let project = build_project(&root);

        // URI points to a file outside models/
        let outside = root.path().join("random/foo.sql");
        std::fs::create_dir_all(root.path().join("random")).unwrap();
        std::fs::write(&outside, "SELECT 1;").unwrap();
        let uri = Url::from_file_path(&outside).unwrap();
        let file_text = std::fs::read_to_string(&outside).unwrap();
        let lenses = code_lenses(&uri, &file_text, root.path(), Some(&project), None);

        assert!(lenses.is_empty());
    }
}
