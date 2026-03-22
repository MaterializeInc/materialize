//! Code lenses for inline unit tests.
//!
//! Places a clickable "Run Test" link above each `EXECUTE UNIT TEST` statement
//! in a SQL file. When clicked, the editor dispatches a `mz-deploy.runTest`
//! command with the test filter string as argument.
//!
//! ## Algorithm
//!
//! 1. Derive the file's `ObjectId` from its URI path (same `models/<db>/<schema>/`
//!    convention used by completions and go-to-definition).
//! 2. Look up the object in the project model to get its `typed_object.tests`.
//! 3. For each test, scan the file text for the `EXECUTE UNIT TEST <name>` line
//!    and emit a `CodeLens` with the `mz-deploy.runTest` command.
//!
//! ## Filter format
//!
//! The command argument is `database.schema.object#test_name`, matching the
//! filter syntax accepted by `mz-deploy test --filter`.

use crate::project::object_id::ObjectId;
use crate::project::planned;
use std::path::Path;
use tower_lsp::lsp_types::*;

/// Build code lenses for all unit tests in the file identified by `file_uri`.
///
/// Returns an empty vec if the file is not under `models/<db>/<schema>/`, the
/// object is not found in the project, or the object has no tests.
pub fn code_lenses(
    file_uri: &Url,
    file_text: &str,
    root: &Path,
    project: &planned::Project,
) -> Vec<CodeLens> {
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

    obj.typed_object
        .tests
        .iter()
        .filter_map(|test| {
            let test_name = test.name.to_string();
            let line = find_test_line(file_text, &test_name)?;
            let filter = format!(
                "{}.{}.{}#{}",
                file_object_id.database(),
                file_object_id.schema(),
                file_object_id.object(),
                test_name,
            );

            Some(CodeLens {
                range: Range::new(Position::new(line, 0), Position::new(line, 0)),
                command: Some(Command {
                    title: "\u{25b6} Run Test".to_string(),
                    command: "mz-deploy.runTest".to_string(),
                    arguments: Some(vec![serde_json::Value::String(filter)]),
                }),
                data: None,
            })
        })
        .collect()
}

/// Find the 0-based line number where `EXECUTE UNIT TEST <name>` appears.
///
/// Performs a case-insensitive scan of `file_text` line-by-line, looking for a
/// line that contains the tokens `execute`, `unit`, `test`, followed by
/// `test_name` (after stripping surrounding quotes if present).
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
        let lenses = code_lenses(&uri, &file_text, root.path(), &project);

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
        let lenses = code_lenses(&uri, &file_text, root.path(), &project);

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
        let lenses = code_lenses(&uri, &file_text, root.path(), &project);

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
        let lenses = code_lenses(&uri, &file_text, root.path(), &project);

        assert!(lenses.is_empty());
    }
}
