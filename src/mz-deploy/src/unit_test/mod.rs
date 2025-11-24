//! Unit test parsing and desugaring for SQL views.
//!
//! This module provides functionality to parse custom unit test syntax and desugar it
//! into executable SQL statements that create temporary views and run test assertions.
//!
//! # Syntax
//!
//! ```sql
//! EXECUTE UNIT TEST test_name
//! FOR database.schema.view_name
//! WITH database.schema.mock1(col1 TYPE1, col2 TYPE2) AS (
//!   SELECT * FROM VALUES (...)
//! ),
//! database.schema.mock2(col TYPE) AS (
//!   SELECT * FROM VALUES (...)
//! ),
//! expected(col1 TYPE1, col2 TYPE2) AS (
//!   SELECT * FROM VALUES (...)
//! );
//! ```
//!
//! # Output
//!
//! The test is desugared into:
//! 1. CREATE TEMPORARY VIEW for each mock
//! 2. CREATE TEMPORARY VIEW for expected results
//! 3. CREATE TEMPORARY VIEW for the target (using flattened naming)
//! 4. Test query that returns rows with status column indicating failures

use crate::project::ast::Statement;
use crate::project::hir::FullyQualifiedName;
use crate::project::normalize::NormalizingVisitor;
use mz_sql_parser::ast::{CreateViewStatement, IfExistsBehavior, ViewDefinition};
use std::fs;
use std::path::{Path, PathBuf};

/// Represents a parsed unit test definition.
#[derive(Debug, Clone)]
pub struct UnitTest {
    /// Name of the test (e.g., "test_flippers")
    pub name: String,
    /// Fully qualified name of the target view being tested
    pub target_view: String,
    /// Mock views to create for dependencies
    pub mocks: Vec<MockView>,
    /// Expected results definition
    pub expected: ExpectedResult,
}

/// A mock view definition that replaces a real dependency.
#[derive(Debug, Clone)]
pub struct MockView {
    /// Fully qualified name (e.g., "materialize.public.flipper_activity")
    pub fqn: String,
    /// Column definitions as (name, type) pairs
    pub columns: Vec<(String, String)>,
    /// SQL query body (the part after AS)
    pub query: String,
}

/// Expected results for the test.
#[derive(Debug, Clone)]
pub struct ExpectedResult {
    /// Column definitions as (name, type) pairs
    pub columns: Vec<(String, String)>,
    /// SQL query body (the part after AS)
    pub query: String,
}

/// Find all test files in the test directory.
///
/// Recursively searches for all .sql files in the given directory.
pub fn find_test_files(test_dir: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut test_files = Vec::new();

    if !test_dir.exists() {
        return Ok(test_files); // Return empty vec if test directory doesn't exist
    }

    fn visit_dir(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), std::io::Error> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                visit_dir(&path, files)?;
            } else if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                files.push(path);
            }
        }
        Ok(())
    }

    visit_dir(test_dir, &mut test_files)?;
    test_files.sort(); // Consistent ordering
    Ok(test_files)
}

/// Parse a test file that may contain multiple unit tests.
///
/// Splits the file by "EXECUTE UNIT TEST" keywords and parses each test individually.
pub fn parse_test_file(content: &str) -> Result<Vec<UnitTest>, String> {
    let mut tests = Vec::new();
    let content = content.trim();

    if content.is_empty() {
        return Ok(tests);
    }

    // Split by "EXECUTE UNIT TEST" but keep the delimiter
    let parts: Vec<&str> = content.split("EXECUTE UNIT TEST").collect();

    // First part before any "EXECUTE UNIT TEST" is discarded (comments, etc.)
    for part in parts.iter().skip(1) {
        if part.trim().is_empty() {
            continue;
        }

        // Reconstruct the test SQL with "EXECUTE UNIT TEST" prefix
        let test_sql = format!("EXECUTE UNIT TEST{}", part);
        let test = parse_unit_test(&test_sql)?;
        tests.push(test);
    }

    Ok(tests)
}

/// Parse unit test SQL syntax into structured format.
///
/// This is a simple parser that assumes well-formed input (happy path only).
///
/// # Example
///
/// ```ignore
/// let test = parse_unit_test(r#"
///     EXECUTE UNIT TEST test_flippers
///     FOR flippers
///     MOCK flip_activities(flipper_id BIGINT) AS (
///       VALUES (1), (1)
///     )
///     EXPECTED(flipper_id BIGINT) AS (
///       VALUES (1)
///     );
/// "#);
/// ```
pub fn parse_unit_test(sql: &str) -> Result<UnitTest, String> {
    let sql = sql.trim();

    // Extract test name: EXECUTE UNIT TEST <name>
    let name_start = sql
        .find("EXECUTE UNIT TEST")
        .ok_or("Missing 'EXECUTE UNIT TEST'")?
        + "EXECUTE UNIT TEST".len();
    let for_pos = sql.find("FOR").ok_or("Missing 'FOR' clause")?;
    let name = sql[name_start..for_pos].trim().to_string();

    // Extract target view name: FOR <target>
    // Find first MOCK or EXPECTED after FOR
    let after_for = &sql[for_pos + 3..];
    let mock_pos = after_for.find("MOCK");
    let expected_pos = after_for.find("EXPECTED");

    let content_start = match (mock_pos, expected_pos) {
        (Some(m), Some(e)) => m.min(e),
        (Some(m), None) => m,
        (None, Some(e)) => e,
        (None, None) => return Err("Missing 'MOCK' or 'EXPECTED' clause".to_string()),
    };

    let target_view = after_for[..content_start].trim().to_string();

    // Extract content from first MOCK/EXPECTED to semicolon
    let content = &after_for[content_start..];
    let semicolon_pos = content.find(';').ok_or("Missing terminating ';'")?;
    let content = &content[..semicolon_pos];

    // Parse MOCK and EXPECTED statements
    let (mocks, expected) = parse_mock_and_expected(content)?;

    Ok(UnitTest {
        name,
        target_view,
        mocks,
        expected,
    })
}

/// Parse MOCK and EXPECTED statements.
///
/// Expects content like:
/// ```text
/// MOCK name(...) AS (...)
/// MOCK name2(...) AS (...)
/// EXPECTED(...) AS (...)
/// ```
fn parse_mock_and_expected(content: &str) -> Result<(Vec<MockView>, ExpectedResult), String> {
    let mut mocks = Vec::new();
    let mut expected = None;

    // Find all MOCK and EXPECTED positions
    let mut positions: Vec<(usize, bool)> = Vec::new(); // (position, is_mock)

    // Find all MOCK keywords
    let mut search_pos = 0;
    while let Some(pos) = content[search_pos..].find("MOCK ") {
        let absolute_pos = search_pos + pos;
        // Check if it's at start or after whitespace
        if absolute_pos == 0
            || content
                .chars()
                .nth(absolute_pos - 1)
                .unwrap()
                .is_whitespace()
        {
            positions.push((absolute_pos, true));
        }
        search_pos = absolute_pos + 1;
    }

    // Find EXPECTED keyword
    search_pos = 0;
    while let Some(pos) = content[search_pos..].find("EXPECTED") {
        let absolute_pos = search_pos + pos;
        // Check if it's at start or after whitespace
        if absolute_pos == 0
            || content
                .chars()
                .nth(absolute_pos - 1)
                .unwrap()
                .is_whitespace()
        {
            positions.push((absolute_pos, false));
            break; // Only one EXPECTED
        }
        search_pos = absolute_pos + 1;
    }

    // Sort positions
    positions.sort_by_key(|(pos, _)| *pos);

    // Extract each section
    for i in 0..positions.len() {
        let (start_pos, is_mock) = positions[i];
        let end_pos = if i + 1 < positions.len() {
            positions[i + 1].0
        } else {
            content.len()
        };

        let section = &content[start_pos..end_pos].trim();

        if is_mock {
            let after_mock = &section[5..]; // Skip "MOCK "
            mocks.push(parse_mock(after_mock.trim())?);
        } else {
            let after_expected = &section[8..]; // Skip "EXPECTED"
            expected = Some(parse_expected(after_expected.trim())?);
        }
    }

    let expected = expected.ok_or("Missing 'EXPECTED' definition")?;
    Ok((mocks, expected))
}

/// Parse a mock view definition.
fn parse_mock(item: &str) -> Result<MockView, String> {
    // Format: fqn(col1 TYPE1, col2 TYPE2) AS (query)
    let as_pos = item
        .find(" AS ")
        .ok_or("Missing ' AS ' in mock definition")?;
    let signature = &item[..as_pos].trim();
    let query = &item[as_pos + 4..].trim();

    // Parse FQN and columns
    let paren_pos = signature
        .find('(')
        .ok_or("Missing '(' in mock definition")?;
    let fqn = signature[..paren_pos].trim().to_string();
    let columns_str = &signature[paren_pos + 1..];
    let columns_str = columns_str.trim_end_matches(')').trim();

    let columns = parse_columns(columns_str)?;

    // Remove outer parentheses from query
    let query = query
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim()
        .to_string();

    Ok(MockView {
        fqn,
        columns,
        query,
    })
}

/// Parse expected result definition.
fn parse_expected(item: &str) -> Result<ExpectedResult, String> {
    // Format: (col1 TYPE1, col2 TYPE2) AS (query)
    let as_pos = item
        .find(" AS ")
        .ok_or("Missing ' AS ' in expected definition")?;
    let signature = &item[..as_pos].trim();
    let query = &item[as_pos + 4..].trim();

    // Parse columns
    let paren_pos = signature
        .find('(')
        .ok_or("Missing '(' in expected definition")?;
    let columns_str = &signature[paren_pos + 1..];
    let columns_str = columns_str.trim_end_matches(')').trim();

    let columns = parse_columns(columns_str)?;

    // Remove outer parentheses from query
    let query = query
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim()
        .to_string();

    Ok(ExpectedResult { columns, query })
}

/// Parse column definitions "col1 TYPE1, col2 TYPE2" into Vec<(name, type)>.
fn parse_columns(columns_str: &str) -> Result<Vec<(String, String)>, String> {
    let mut columns = Vec::new();

    // Split by commas, handling nested parentheses in types
    let parts = split_columns(columns_str);

    for part in parts {
        let part = part.trim();
        // Find first whitespace to separate name from type
        let space_pos = part
            .find(char::is_whitespace)
            .ok_or_else(|| format!("Invalid column definition: {}", part))?;
        let name = part[..space_pos].trim().to_string();
        let type_str = part[space_pos..].trim().to_string();
        columns.push((name, type_str));
    }

    Ok(columns)
}

/// Split column definitions by commas (respecting parentheses in types).
fn split_columns(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0;

    for ch in s.chars() {
        match ch {
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                paren_depth -= 1;
                current.push(ch);
            }
            ',' if paren_depth == 0 => {
                parts.push(current.trim().to_string());
                current = String::new();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }

    parts
}

/// Desugar unit test into executable SQL statements.
///
/// Returns a vector of SQL strings in order:
/// 1. CREATE TEMPORARY VIEW for each mock
/// 2. CREATE TEMPORARY VIEW for expected
/// 3. CREATE TEMPORARY VIEW for the target (flattened)
/// 4. Test query with status column
///
/// # Arguments
///
/// * `test` - The parsed unit test
/// * `target_stmt` - The statement defining the target view
/// * `target_fqn` - Fully qualified name of the target view
pub fn desugar_unit_test(
    test: &UnitTest,
    target_stmt: &Statement,
    target_fqn: &FullyQualifiedName,
) -> Vec<String> {
    let mut statements = Vec::new();

    // 1. Create temporary views for mocks
    // Qualify mock names with target's database and schema if not already qualified
    for mock in &test.mocks {
        let qualified_mock = qualify_mock_name(mock, target_fqn);
        statements.push(create_mock_view_sql(&qualified_mock));
    }

    // 2. Create temporary view for expected
    statements.push(create_expected_view_sql(&test.expected));

    // 3. Create temporary view for target (flattened)
    statements.push(create_target_view_sql(target_stmt, target_fqn));

    // 4. Create test query
    let target_fqn_str = format!(
        "{}.{}.{}",
        target_fqn.database(), target_fqn.schema(), target_fqn.object()
    );
    let flattened_target_name = flatten_fqn(&target_fqn_str);
    statements.push(create_test_query_sql(&flattened_target_name));

    statements
}

/// Flatten a fully qualified name by replacing dots with underscores.
///
/// # Example
///
/// ```ignore
/// assert_eq!(flatten_fqn("materialize.public.flippers"), "materialize_public_flippers");
/// ```
fn flatten_fqn(fqn: &str) -> String {
    fqn.replace('.', "_")
}

/// Qualify a mock name with the target's FQN context if it's not already qualified.
fn qualify_mock_name(mock: &MockView, target_fqn: &FullyQualifiedName) -> MockView {
    // Count the number of parts in the FQN (parts = dots + 1)
    // 1 part: object
    // 2 parts: schema.object
    // 3 parts: database.schema.object
    let parts = mock.fqn.matches('.').count() + 1;

    let qualified_fqn = match parts {
        1 => {
            // Unqualified: object only
            // Prepend database.schema
            format!("{}.{}.{}", target_fqn.database(), target_fqn.schema(), mock.fqn)
        }
        2 => {
            // Partially qualified: schema.object
            // Prepend database
            format!("{}.{}", target_fqn.database(), mock.fqn)
        }
        _ => {
            // Fully qualified (3+ parts)
            mock.fqn.clone()
        }
    };

    MockView {
        fqn: qualified_fqn,
        columns: mock.columns.clone(),
        query: mock.query.clone(),
    }
}

/// Create SQL for a mock temporary view.
fn create_mock_view_sql(mock: &MockView) -> String {
    let flattened_name = flatten_fqn(&mock.fqn);
    let columns_def = mock
        .columns
        .iter()
        .map(|(name, typ)| format!("{} {}", name, typ))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "CREATE TEMPORARY VIEW {} AS\nWITH MUTUALLY RECURSIVE data({}) AS (\n  {}\n)\nSELECT * FROM data;",
        flattened_name, columns_def, mock.query
    )
}

/// Create SQL for the expected temporary view.
fn create_expected_view_sql(expected: &ExpectedResult) -> String {
    let columns_def = expected
        .columns
        .iter()
        .map(|(name, typ)| format!("{} {}", name, typ))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "CREATE TEMPORARY VIEW expected AS\nWITH MUTUALLY RECURSIVE data({}) AS (\n  {}\n)\nSELECT * FROM data;",
        columns_def, expected.query
    )
}

/// Create SQL for the target view as a temporary view with flattened naming.
fn create_target_view_sql(stmt: &Statement, fqn: &FullyQualifiedName) -> String {
    let visitor = NormalizingVisitor::flattening(fqn);
    let transformed_stmt = stmt
        .clone()
        .normalize_name_with(&visitor, &fqn.to_item_name())
        .normalize_dependencies_with(&visitor);

    match transformed_stmt {
        Statement::CreateView(view) => {
            let stmt = CreateViewStatement {
                if_exists: IfExistsBehavior::Error,
                temporary: true,
                definition: view.definition.clone(),
            };

            stmt.to_string()
        }
        Statement::CreateMaterializedView(materialized_view) => {
            let stmt = CreateViewStatement {
                if_exists: IfExistsBehavior::Error,
                temporary: true,
                definition: ViewDefinition {
                    name: materialized_view.name,
                    columns: materialized_view.columns,
                    query: materialized_view.query,
                },
            };

            stmt.to_string()
        }
        _ => unimplemented!(),
    }
}

/// Create the test assertion query that returns failures.
///
/// Returns rows with a 'status' column that indicates the type of failure:
/// - 'MISSING': Expected rows not found in actual results
/// - 'UNEXPECTED': Actual rows not found in expected results
///
/// Empty result means the test passed.
fn create_test_query_sql(flattened_target_name: &str) -> String {
    format!(
        r#"SELECT 'MISSING' as status, * FROM expected
EXCEPT
SELECT 'MISSING', * FROM {}

UNION ALL

SELECT 'UNEXPECTED' as status, * FROM {}
EXCEPT
SELECT 'UNEXPECTED', * FROM expected;"#,
        flattened_target_name, flattened_target_name
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flatten_fqn() {
        assert_eq!(
            flatten_fqn("materialize.public.flippers"),
            "materialize_public_flippers"
        );
        assert_eq!(flatten_fqn("a.b.c"), "a_b_c");
        assert_eq!(flatten_fqn("single"), "single");
    }

    #[test]
    fn test_split_columns() {
        let result = split_columns("col1 INT, col2 TEXT");
        assert_eq!(result, vec!["col1 INT", "col2 TEXT"]);

        let result = split_columns("col1 NUMERIC(10,2), col2 VARCHAR(255)");
        assert_eq!(result, vec!["col1 NUMERIC(10,2)", "col2 VARCHAR(255)"]);

        let result = split_columns("single_col BIGINT");
        assert_eq!(result, vec!["single_col BIGINT"]);
    }

    #[test]
    fn test_parse_columns() {
        let result = parse_columns("col1 INT, col2 TEXT").unwrap();
        assert_eq!(
            result,
            vec![
                ("col1".to_string(), "INT".to_string()),
                ("col2".to_string(), "TEXT".to_string())
            ]
        );

        let result = parse_columns("amount NUMERIC(10,2), name VARCHAR(255)").unwrap();
        assert_eq!(
            result,
            vec![
                ("amount".to_string(), "NUMERIC(10,2)".to_string()),
                ("name".to_string(), "VARCHAR(255)".to_string())
            ]
        );
    }

    #[test]
    fn test_parse_mock() {
        let mock_str = "materialize.public.users(id BIGINT, name TEXT) AS (\n  SELECT * FROM VALUES ((1, 'alice'))\n)";
        let result = parse_mock(mock_str).unwrap();

        assert_eq!(result.fqn, "materialize.public.users");
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0], ("id".to_string(), "BIGINT".to_string()));
        assert_eq!(result.columns[1], ("name".to_string(), "TEXT".to_string()));
        assert!(result.query.contains("SELECT * FROM VALUES"));
    }

    #[test]
    fn test_parse_expected() {
        let expected_str =
            "expected(id BIGINT, count INT) AS (\n  SELECT * FROM VALUES ((1, 5))\n)";
        let result = parse_expected(expected_str).unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0], ("id".to_string(), "BIGINT".to_string()));
        assert_eq!(result.columns[1], ("count".to_string(), "INT".to_string()));
        assert!(result.query.contains("SELECT * FROM VALUES"));
    }

    #[test]
    fn test_parse_unit_test_simple() {
        let test_sql = r#"
            EXECUTE UNIT TEST test_simple
            FOR materialize.public.my_view
            MOCK materialize.public.source_table(id BIGINT) AS (
              SELECT * FROM VALUES ((1))
            )
            EXPECTED(id BIGINT) AS (
              SELECT * FROM VALUES ((1))
            );
        "#;

        let result = parse_unit_test(test_sql).unwrap();

        assert_eq!(result.name, "test_simple");
        assert_eq!(result.target_view, "materialize.public.my_view");
        assert_eq!(result.mocks.len(), 1);
        assert_eq!(result.mocks[0].fqn, "materialize.public.source_table");
        assert_eq!(result.expected.columns.len(), 1);
    }

    #[test]
    fn test_parse_unit_test_multiple_mocks() {
        let test_sql = r#"
            EXECUTE UNIT TEST test_flippers
            FOR materialize.public.flippers
            MOCK materialize.public.flip_activities(flipper_id BIGINT, count INT) AS (
              SELECT * FROM VALUES ((1, 5))
            )
            MOCK materialize.public.known_flippers(flipper_id BIGINT) AS (
              SELECT * FROM VALUES ((1))
            )
            EXPECTED(flipper_id BIGINT) AS (
              SELECT * FROM VALUES ((1))
            );
        "#;

        let result = parse_unit_test(test_sql).unwrap();

        assert_eq!(result.name, "test_flippers");
        assert_eq!(result.target_view, "materialize.public.flippers");
        assert_eq!(result.mocks.len(), 2);
        assert_eq!(result.mocks[0].fqn, "materialize.public.flip_activities");
        assert_eq!(result.mocks[1].fqn, "materialize.public.known_flippers");
        assert_eq!(result.expected.columns.len(), 1);
    }

    #[test]
    fn test_create_mock_view_sql() {
        let mock = MockView {
            fqn: "materialize.public.users".to_string(),
            columns: vec![
                ("id".to_string(), "BIGINT".to_string()),
                ("name".to_string(), "TEXT".to_string()),
            ],
            query: "SELECT * FROM VALUES ((1, 'alice'))".to_string(),
        };

        let sql = create_mock_view_sql(&mock);

        assert!(sql.contains("CREATE TEMPORARY VIEW materialize_public_users"));
        assert!(sql.contains("WITH MUTUALLY RECURSIVE data(id BIGINT, name TEXT)"));
        assert!(sql.contains("SELECT * FROM VALUES ((1, 'alice'))"));
        assert!(sql.contains("SELECT * FROM data"));
    }

    #[test]
    fn test_create_expected_view_sql() {
        let expected = ExpectedResult {
            columns: vec![
                ("id".to_string(), "BIGINT".to_string()),
                ("count".to_string(), "INT".to_string()),
            ],
            query: "SELECT * FROM VALUES ((1, 10))".to_string(),
        };

        let sql = create_expected_view_sql(&expected);

        assert!(sql.contains("CREATE TEMPORARY VIEW expected"));
        assert!(sql.contains("WITH MUTUALLY RECURSIVE data(id BIGINT, count INT)"));
        assert!(sql.contains("SELECT * FROM VALUES ((1, 10))"));
        assert!(sql.contains("SELECT * FROM data"));
    }

    #[test]
    fn test_create_test_query_sql() {
        let sql = create_test_query_sql("materialize_public_my_view");

        assert!(sql.contains("SELECT 'MISSING' as status, * FROM expected"));
        assert!(sql.contains("SELECT 'MISSING', * FROM materialize_public_my_view"));
        assert!(sql.contains("SELECT 'UNEXPECTED' as status, * FROM materialize_public_my_view"));
        assert!(sql.contains("SELECT 'UNEXPECTED', * FROM expected"));
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("EXCEPT"));
    }

    #[test]
    fn test_parse_columns_with_complex_types() {
        let result = parse_columns("amount NUMERIC(10,2), data MAP[TEXT => INT]").unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0],
            ("amount".to_string(), "NUMERIC(10,2)".to_string())
        );
        assert_eq!(
            result[1],
            ("data".to_string(), "MAP[TEXT => INT]".to_string())
        );
    }

    #[test]
    fn test_parse_unit_test_with_newlines_and_whitespace() {
        let test_sql = r#"
EXECUTE UNIT TEST   test_with_spaces
FOR   materialize.public.my_view
MOCK   materialize.public.table1  (  id   BIGINT  )   AS   (
  SELECT * FROM VALUES ((1))
)
EXPECTED  (  result   INT  )   AS   (
  SELECT * FROM VALUES ((100))
)  ;
        "#;

        let result = parse_unit_test(test_sql).unwrap();

        assert_eq!(result.name, "test_with_spaces");
        assert_eq!(result.target_view, "materialize.public.my_view");
        assert_eq!(result.mocks.len(), 1);
    }
}
