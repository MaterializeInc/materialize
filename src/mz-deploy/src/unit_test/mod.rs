//! Unit test parsing and desugaring for SQL views.
//!
//! This module provides functionality to parse custom unit test syntax and desugar it
//! into executable SQL statements that create temporary views and run test assertions.
//!
//! Tests are defined inline within the same SQL file as the view definition using
//! the EXECUTE UNIT TEST syntax.
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
use crate::project::typed::FullyQualifiedName;
use crate::project::normalize::NormalizingVisitor;
use mz_sql_parser::ast::{CreateViewStatement, IfExistsBehavior, ViewDefinition};

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

impl UnitTest {
    /// Convert an ExecuteUnitTestStatement from the AST into a UnitTest.
    pub fn from_execute_statement(
        stmt: &mz_sql_parser::ast::ExecuteUnitTestStatement<mz_sql_parser::ast::Raw>,
        _object_id: &crate::project::object_id::ObjectId,
    ) -> Self {
        use mz_sql_parser::ast::display::{AstDisplay, FormatMode};

        let name = stmt.name.to_string();
        let target_view = stmt.target.to_ast_string(FormatMode::Simple);

        // Convert mocks
        let mocks = stmt
            .mocks
            .iter()
            .map(|mock| {
                let fqn = mock.name.to_ast_string(FormatMode::Simple);
                let columns = mock
                    .columns
                    .iter()
                    .map(|col| {
                        (
                            col.name.to_string(),
                            col.data_type.to_ast_string(FormatMode::Simple),
                        )
                    })
                    .collect();
                let query = mock.query.to_ast_string(FormatMode::Simple);
                MockView {
                    fqn,
                    columns,
                    query,
                }
            })
            .collect();

        // Convert expected
        let expected = ExpectedResult {
            columns: stmt
                .expected
                .columns
                .iter()
                .map(|col| {
                    (
                        col.name.to_string(),
                        col.data_type.to_ast_string(FormatMode::Simple),
                    )
                })
                .collect(),
            query: stmt.expected.query.to_ast_string(FormatMode::Simple),
        };

        UnitTest {
            name,
            target_view,
            mocks,
            expected,
        }
    }
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
        target_fqn.database(),
        target_fqn.schema(),
        target_fqn.object()
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
            format!(
                "{}.{}.{}",
                target_fqn.database(),
                target_fqn.schema(),
                mock.fqn
            )
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
}
