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
use crate::project::normalize::NormalizingVisitor;
use crate::project::object_id::ObjectId;
use crate::project::typed::FullyQualifiedName;
use crate::types::Types;
use mz_sql_parser::ast::{CreateViewStatement, IfExistsBehavior, ViewDefinition};
use owo_colors::OwoColorize;
use std::collections::BTreeSet;
use std::fmt;
use thiserror::Error;

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

// =============================================================================
// Test Validation
// =============================================================================

/// Errors that can occur during unit test validation.
#[derive(Debug, Error)]
pub enum TestValidationError {
    /// A required dependency is not mocked
    #[error("unmocked dependency")]
    UnmockedDependency(UnmockedDependencyError),

    /// A mock is missing required columns
    #[error("mock schema mismatch")]
    MockSchemaMismatch(MockSchemaMismatchError),

    /// Expected output doesn't match target view schema
    #[error("expected output schema mismatch")]
    ExpectedSchemaMismatch(ExpectedSchemaMismatchError),

    /// Types cache is missing or stale
    #[error("types cache unavailable: {reason}")]
    TypesCacheUnavailable { reason: String },
}

/// Error: A dependency of the target view is not mocked.
#[derive(Debug)]
pub struct UnmockedDependencyError {
    /// Test name
    pub test_name: String,
    /// The target view being tested
    pub target_view: String,
    /// Dependencies that are not mocked
    pub missing_mocks: Vec<String>,
}

impl fmt::Display for UnmockedDependencyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{}: test '{}' has unmocked dependencies",
            "error".bright_red().bold(),
            self.test_name.cyan()
        )?;
        writeln!(
            f,
            " {} target view: {}",
            "-->".bright_blue().bold(),
            self.target_view.yellow()
        )?;
        writeln!(f)?;
        writeln!(
            f,
            "  {} The following dependencies must be mocked:",
            "|".bright_blue().bold()
        )?;
        for dep in &self.missing_mocks {
            writeln!(f, "  {}   - {}", "|".bright_blue().bold(), dep.yellow())?;
        }
        writeln!(f)?;
        writeln!(
            f,
            "  {} Add mocks for these dependencies in the WITH clause of the test",
            "=".bright_blue().bold()
        )?;
        Ok(())
    }
}

impl std::error::Error for UnmockedDependencyError {}

/// Error: A mock's columns don't match the actual schema.
#[derive(Debug)]
pub struct MockSchemaMismatchError {
    /// Test name
    pub test_name: String,
    /// The mock that has mismatched columns
    pub mock_fqn: String,
    /// Columns in mock that don't exist in actual schema
    pub extra_columns: Vec<String>,
    /// Columns in actual schema missing from mock (name, type)
    pub missing_columns: Vec<(String, String)>,
    /// Columns with wrong types (column_name, mock_type, actual_type)
    pub type_mismatches: Vec<(String, String, String)>,
    /// The actual schema columns with types (for showing expected signature)
    pub actual_schema: Vec<(String, String)>,
}

impl fmt::Display for MockSchemaMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{}: mock '{}' schema doesn't match actual schema",
            "error".bright_red().bold(),
            self.mock_fqn.cyan()
        )?;
        writeln!(
            f,
            " {} in test: {}",
            "-->".bright_blue().bold(),
            self.test_name.yellow()
        )?;
        writeln!(f)?;

        if !self.missing_columns.is_empty() {
            writeln!(
                f,
                "  {} Missing columns (required but not in mock):",
                "|".bright_blue().bold()
            )?;
            for (col, typ) in &self.missing_columns {
                writeln!(
                    f,
                    "  {}   - {} {}",
                    "|".bright_blue().bold(),
                    col.red(),
                    typ.to_uppercase().dimmed()
                )?;
            }
        }

        if !self.extra_columns.is_empty() {
            writeln!(
                f,
                "  {} Extra columns (in mock but not in actual schema):",
                "|".bright_blue().bold()
            )?;
            for col in &self.extra_columns {
                writeln!(f, "  {}   - {}", "|".bright_blue().bold(), col.yellow())?;
            }
        }

        if !self.type_mismatches.is_empty() {
            writeln!(f, "  {} Type mismatches:", "|".bright_blue().bold())?;
            for (col, mock_type, actual_type) in &self.type_mismatches {
                writeln!(
                    f,
                    "  {}   - {}: mock has '{}', expected '{}'",
                    "|".bright_blue().bold(),
                    col.cyan(),
                    mock_type.red(),
                    actual_type.green()
                )?;
            }
        }

        writeln!(f)?;

        // Show the expected mock signature
        if !self.actual_schema.is_empty() {
            writeln!(f, "  {} Expected mock signature:", "=".bright_blue().bold())?;
            let cols: Vec<String> = self
                .actual_schema
                .iter()
                .map(|(name, typ)| format!("{} {}", name, typ.to_uppercase()))
                .collect();
            writeln!(
                f,
                "  {}   MOCK {}({}) AS (...)",
                "|".bright_blue().bold(),
                self.mock_fqn.green(),
                cols.join(", ").green()
            )?;
        }

        Ok(())
    }
}

impl std::error::Error for MockSchemaMismatchError {}

/// Error: Expected output columns don't match the target view schema.
#[derive(Debug)]
pub struct ExpectedSchemaMismatchError {
    /// Test name
    pub test_name: String,
    /// The target view being tested
    pub target_view: String,
    /// Columns in expected that don't exist in target schema
    pub extra_columns: Vec<String>,
    /// Columns in target schema missing from expected (name, type)
    pub missing_columns: Vec<(String, String)>,
    /// Columns with wrong types (column_name, expected_type, actual_type)
    pub type_mismatches: Vec<(String, String, String)>,
    /// The actual schema columns with types (for showing expected signature)
    pub actual_schema: Vec<(String, String)>,
}

impl fmt::Display for ExpectedSchemaMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{}: expected output schema doesn't match target view",
            "error".bright_red().bold()
        )?;
        writeln!(
            f,
            " {} target: {} | test: {}",
            "-->".bright_blue().bold(),
            self.target_view.cyan(),
            self.test_name.yellow()
        )?;
        writeln!(f)?;

        if !self.missing_columns.is_empty() {
            writeln!(
                f,
                "  {} Missing columns (in target view but not in expected):",
                "|".bright_blue().bold()
            )?;
            for (col, typ) in &self.missing_columns {
                writeln!(
                    f,
                    "  {}   - {} {}",
                    "|".bright_blue().bold(),
                    col.red(),
                    typ.to_uppercase().dimmed()
                )?;
            }
        }

        if !self.extra_columns.is_empty() {
            writeln!(
                f,
                "  {} Extra columns (in expected but not in target view):",
                "|".bright_blue().bold()
            )?;
            for col in &self.extra_columns {
                writeln!(f, "  {}   - {}", "|".bright_blue().bold(), col.yellow())?;
            }
        }

        if !self.type_mismatches.is_empty() {
            writeln!(f, "  {} Type mismatches:", "|".bright_blue().bold())?;
            for (col, expected_type, actual_type) in &self.type_mismatches {
                writeln!(
                    f,
                    "  {}   - {}: has '{}', expected '{}'",
                    "|".bright_blue().bold(),
                    col.cyan(),
                    expected_type.red(),
                    actual_type.green()
                )?;
            }
        }

        writeln!(f)?;

        // Show the expected signature
        if !self.actual_schema.is_empty() {
            writeln!(f, "  {} Expected signature:", "=".bright_blue().bold())?;
            let cols: Vec<String> = self
                .actual_schema
                .iter()
                .map(|(name, typ)| format!("{} {}", name, typ.to_uppercase()))
                .collect();
            writeln!(
                f,
                "  {}   EXPECTED({}) AS (...)",
                "|".bright_blue().bold(),
                cols.join(", ").green()
            )?;
        }

        Ok(())
    }
}

impl std::error::Error for ExpectedSchemaMismatchError {}

/// Validate a unit test against the known types.
///
/// This function performs three validations:
/// 1. All dependencies of the target view are mocked
/// 2. Each mock's columns match the actual schema of the mocked object
/// 3. The expected output columns match the target view's output schema
///
/// # Arguments
/// * `test` - The unit test to validate
/// * `target_id` - The ObjectId of the target view
/// * `types` - Combined types from types.lock (external) and types.cache (internal)
/// * `dependencies` - Dependencies of the target view from the project's dependency graph
///
/// # Returns
/// Ok(()) if validation passes, Err with detailed error messages if validation fails
pub fn validate_unit_test(
    test: &UnitTest,
    target_id: &ObjectId,
    types: &Types,
    dependencies: &BTreeSet<ObjectId>,
) -> Result<(), TestValidationError> {
    let target_fqn = target_id.to_string();

    // Build set of mocked FQNs (normalize to fully qualified)
    let mocked_fqns: BTreeSet<String> = test
        .mocks
        .iter()
        .map(|m| normalize_fqn(&m.fqn, target_id))
        .collect();

    // 1. Check that all dependencies are mocked
    let missing_mocks: Vec<String> = dependencies
        .iter()
        .filter(|dep| !mocked_fqns.contains(&dep.to_string()))
        .map(|dep| dep.to_string())
        .collect();

    if !missing_mocks.is_empty() {
        return Err(TestValidationError::UnmockedDependency(
            UnmockedDependencyError {
                test_name: test.name.clone(),
                target_view: target_fqn.clone(),
                missing_mocks,
            },
        ));
    }

    // 2. Validate each mock's schema against the actual types
    for mock in &test.mocks {
        let mock_fqn = normalize_fqn(&mock.fqn, target_id);

        if let Some(actual_columns) = types.get_object(&mock_fqn) {
            let (extra, missing, type_mismatches) = compare_columns(&mock.columns, actual_columns);

            if !extra.is_empty() || !missing.is_empty() || !type_mismatches.is_empty() {
                // Extract actual schema for error message
                let actual_schema: Vec<(String, String)> = actual_columns
                    .iter()
                    .map(|(name, col_type)| (name.clone(), col_type.r#type.clone()))
                    .collect();

                return Err(TestValidationError::MockSchemaMismatch(
                    MockSchemaMismatchError {
                        test_name: test.name.clone(),
                        mock_fqn,
                        extra_columns: extra,
                        missing_columns: missing,
                        type_mismatches,
                        actual_schema,
                    },
                ));
            }
        }
        // If the mock isn't in types, it might be an external dependency not in types.lock
        // We allow this to be permissive - the database will catch it during execution
    }

    // 3. Validate expected output schema against target view
    if let Some(target_columns) = types.get_object(&target_fqn) {
        let (extra, missing, type_mismatches) =
            compare_columns(&test.expected.columns, target_columns);

        if !extra.is_empty() || !missing.is_empty() || !type_mismatches.is_empty() {
            // Extract actual schema for error message
            let actual_schema: Vec<(String, String)> = target_columns
                .iter()
                .map(|(name, col_type)| (name.clone(), col_type.r#type.clone()))
                .collect();

            return Err(TestValidationError::ExpectedSchemaMismatch(
                ExpectedSchemaMismatchError {
                    test_name: test.name.clone(),
                    target_view: target_fqn,
                    extra_columns: extra,
                    missing_columns: missing,
                    type_mismatches,
                    actual_schema,
                },
            ));
        }
    }
    // If target isn't in types, we'll catch it during test execution

    Ok(())
}

/// Normalize a potentially partial FQN to a fully qualified name using the target's context.
fn normalize_fqn(fqn: &str, target_id: &ObjectId) -> String {
    let parts: Vec<&str> = fqn.split('.').collect();
    match parts.len() {
        1 => format!("{}.{}.{}", target_id.database, target_id.schema, fqn),
        2 => format!("{}.{}", target_id.database, fqn),
        _ => fqn.to_string(),
    }
}

/// Compare test columns against actual schema columns.
///
/// Returns (extra_columns, missing_columns_with_types, type_mismatches).
fn compare_columns(
    test_columns: &[(String, String)],
    actual_columns: &std::collections::BTreeMap<String, crate::types::ColumnType>,
) -> (
    Vec<String>,
    Vec<(String, String)>,
    Vec<(String, String, String)>,
) {
    let test_col_names: BTreeSet<&str> = test_columns.iter().map(|(n, _)| n.as_str()).collect();
    let actual_col_names: BTreeSet<&str> = actual_columns.keys().map(|s| s.as_str()).collect();

    // Extra columns in test but not in actual
    let extra: Vec<String> = test_col_names
        .difference(&actual_col_names)
        .map(|s| (*s).to_string())
        .collect();

    // Missing columns in actual but not in test (with their types)
    let missing: Vec<(String, String)> = actual_col_names
        .difference(&test_col_names)
        .map(|s| {
            let typ = actual_columns
                .get(*s)
                .map(|c| c.r#type.clone())
                .unwrap_or_default();
            ((*s).to_string(), typ)
        })
        .collect();

    // Type mismatches for columns present in both
    let type_mismatches: Vec<(String, String, String)> = test_columns
        .iter()
        .filter_map(|(name, test_type)| {
            actual_columns.get(name).and_then(|actual| {
                // Normalize types for comparison (case-insensitive, strip whitespace)
                let test_normalized = normalize_type(test_type);
                let actual_normalized = normalize_type(&actual.r#type);

                if test_normalized != actual_normalized {
                    Some((name.clone(), test_type.clone(), actual.r#type.clone()))
                } else {
                    None
                }
            })
        })
        .collect();

    (extra, missing, type_mismatches)
}

/// Normalize a SQL type for comparison.
///
/// This handles Materialize type aliases so that equivalent types compare equal.
/// Based on: https://materialize.com/docs/sql/types/
fn normalize_type(t: &str) -> String {
    let normalized = t.trim().to_lowercase();

    // Map Materialize type aliases to canonical forms
    match normalized.as_str() {
        // Integer types
        "int" | "int4" | "integer" => "integer".to_string(),
        "int8" | "bigint" => "bigint".to_string(),
        "int2" | "smallint" => "smallint".to_string(),

        // Floating point types
        "float4" | "real" => "real".to_string(),
        "float" | "float8" | "double" | "double precision" => "double precision".to_string(),

        // Boolean
        "bool" | "boolean" => "boolean".to_string(),

        // Text/String types
        "string" | "text" => "text".to_string(),
        "varchar" | "character varying" => "text".to_string(),

        // Numeric/Decimal
        "decimal" | "numeric" => "numeric".to_string(),

        // JSON types
        "json" | "jsonb" => "jsonb".to_string(),

        // Timestamp types
        "timestamptz" | "timestamp with time zone" => "timestamp with time zone".to_string(),

        _ => {
            // Handle parameterized types like varchar(255) -> text, numeric(10,2) -> numeric
            if normalized.starts_with("varchar") || normalized.starts_with("character varying") {
                "text".to_string()
            } else if normalized.starts_with("numeric") || normalized.starts_with("decimal") {
                "numeric".to_string()
            } else if normalized.starts_with("timestamp with time zone")
                || normalized.starts_with("timestamptz")
            {
                "timestamp with time zone".to_string()
            } else {
                normalized
            }
        }
    }
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

/// Quote a fully qualified name as a single identifier with dots.
///
/// # Example
///
/// ```ignore
/// assert_eq!(flatten_fqn("materialize.public.flippers"), "\"materialize.public.flippers\"");
/// ```
fn flatten_fqn(fqn: &str) -> String {
    format!("\"{}\"", fqn)
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
    use crate::types::ColumnType;
    use std::collections::BTreeMap;

    #[test]
    fn test_flatten_fqn() {
        assert_eq!(
            flatten_fqn("materialize.public.flippers"),
            "\"materialize.public.flippers\""
        );
        assert_eq!(flatten_fqn("a.b.c"), "\"a.b.c\"");
        assert_eq!(flatten_fqn("single"), "\"single\"");
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

        assert!(sql.contains("CREATE TEMPORARY VIEW \"materialize.public.users\""));
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

    // =========================================================================
    // Validation Tests
    // =========================================================================

    fn make_test_types() -> Types {
        let mut objects = BTreeMap::new();

        // Add users table schema
        let mut users_cols = BTreeMap::new();
        users_cols.insert(
            "id".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: false,
            },
        );
        users_cols.insert(
            "name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
            },
        );
        users_cols.insert(
            "email".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
            },
        );
        objects.insert("materialize.public.users".to_string(), users_cols);

        // Add orders table schema
        let mut orders_cols = BTreeMap::new();
        orders_cols.insert(
            "id".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: false,
            },
        );
        orders_cols.insert(
            "user_id".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: false,
            },
        );
        orders_cols.insert(
            "amount".to_string(),
            ColumnType {
                r#type: "numeric".to_string(),
                nullable: true,
            },
        );
        objects.insert("materialize.public.orders".to_string(), orders_cols);

        // Add target view schema (user_order_summary)
        let mut summary_cols = BTreeMap::new();
        summary_cols.insert(
            "user_id".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: false,
            },
        );
        summary_cols.insert(
            "user_name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
            },
        );
        summary_cols.insert(
            "total_orders".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: true,
            },
        );
        objects.insert(
            "materialize.public.user_order_summary".to_string(),
            summary_cols,
        );

        Types {
            version: 1,
            objects,
        }
    }

    fn make_target_id() -> ObjectId {
        ObjectId {
            database: "materialize".to_string(),
            schema: "public".to_string(),
            object: "user_order_summary".to_string(),
        }
    }

    fn make_dependencies() -> BTreeSet<ObjectId> {
        let mut deps = BTreeSet::new();
        deps.insert(ObjectId {
            database: "materialize".to_string(),
            schema: "public".to_string(),
            object: "users".to_string(),
        });
        deps.insert(ObjectId {
            database: "materialize".to_string(),
            schema: "public".to_string(),
            object: "orders".to_string(),
        });
        deps
    }

    #[test]
    fn test_validate_unit_test_passes_with_correct_mocks() {
        let test = UnitTest {
            name: "test_user_summary".to_string(),
            target_view: "materialize.public.user_order_summary".to_string(),
            mocks: vec![
                MockView {
                    fqn: "materialize.public.users".to_string(),
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("name".to_string(), "text".to_string()),
                        ("email".to_string(), "text".to_string()),
                    ],
                    query: "SELECT * FROM VALUES (1, 'alice', 'alice@example.com')".to_string(),
                },
                MockView {
                    fqn: "materialize.public.orders".to_string(),
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("user_id".to_string(), "bigint".to_string()),
                        ("amount".to_string(), "numeric".to_string()),
                    ],
                    query: "SELECT * FROM VALUES (1, 1, 100.00)".to_string(),
                },
            ],
            expected: ExpectedResult {
                columns: vec![
                    ("user_id".to_string(), "bigint".to_string()),
                    ("user_name".to_string(), "text".to_string()),
                    ("total_orders".to_string(), "bigint".to_string()),
                ],
                query: "SELECT * FROM VALUES (1, 'alice', 1)".to_string(),
            },
        };

        let types = make_test_types();
        let target_id = make_target_id();
        let dependencies = make_dependencies();

        let result = validate_unit_test(&test, &target_id, &types, &dependencies);
        assert!(result.is_ok(), "Expected validation to pass: {:?}", result);
    }

    #[test]
    fn test_validate_unit_test_fails_with_unmocked_dependency() {
        let test = UnitTest {
            name: "test_user_summary".to_string(),
            target_view: "materialize.public.user_order_summary".to_string(),
            mocks: vec![
                // Only mocking users, not orders
                MockView {
                    fqn: "materialize.public.users".to_string(),
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("name".to_string(), "text".to_string()),
                        ("email".to_string(), "text".to_string()),
                    ],
                    query: "SELECT * FROM VALUES (1, 'alice', 'alice@example.com')".to_string(),
                },
            ],
            expected: ExpectedResult {
                columns: vec![
                    ("user_id".to_string(), "bigint".to_string()),
                    ("user_name".to_string(), "text".to_string()),
                    ("total_orders".to_string(), "bigint".to_string()),
                ],
                query: "SELECT * FROM VALUES (1, 'alice', 1)".to_string(),
            },
        };

        let types = make_test_types();
        let target_id = make_target_id();
        let dependencies = make_dependencies();

        let result = validate_unit_test(&test, &target_id, &types, &dependencies);
        assert!(result.is_err());

        match result.unwrap_err() {
            TestValidationError::UnmockedDependency(err) => {
                assert_eq!(err.test_name, "test_user_summary");
                assert!(
                    err.missing_mocks
                        .contains(&"materialize.public.orders".to_string())
                );
            }
            other => panic!("Expected UnmockedDependency error, got: {:?}", other),
        }
    }

    #[test]
    fn test_validate_unit_test_fails_with_missing_mock_column() {
        let test = UnitTest {
            name: "test_user_summary".to_string(),
            target_view: "materialize.public.user_order_summary".to_string(),
            mocks: vec![
                MockView {
                    fqn: "materialize.public.users".to_string(),
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("name".to_string(), "text".to_string()),
                        // Missing 'email' column
                    ],
                    query: "SELECT * FROM VALUES (1, 'alice')".to_string(),
                },
                MockView {
                    fqn: "materialize.public.orders".to_string(),
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("user_id".to_string(), "bigint".to_string()),
                        ("amount".to_string(), "numeric".to_string()),
                    ],
                    query: "SELECT * FROM VALUES (1, 1, 100.00)".to_string(),
                },
            ],
            expected: ExpectedResult {
                columns: vec![
                    ("user_id".to_string(), "bigint".to_string()),
                    ("user_name".to_string(), "text".to_string()),
                    ("total_orders".to_string(), "bigint".to_string()),
                ],
                query: "SELECT * FROM VALUES (1, 'alice', 1)".to_string(),
            },
        };

        let types = make_test_types();
        let target_id = make_target_id();
        let dependencies = make_dependencies();

        let result = validate_unit_test(&test, &target_id, &types, &dependencies);
        assert!(result.is_err());

        match result.unwrap_err() {
            TestValidationError::MockSchemaMismatch(err) => {
                assert_eq!(err.test_name, "test_user_summary");
                assert_eq!(err.mock_fqn, "materialize.public.users");
                assert!(err.missing_columns.iter().any(|(name, _)| name == "email"));
                assert!(err.extra_columns.is_empty());
            }
            other => panic!("Expected MockSchemaMismatch error, got: {:?}", other),
        }
    }

    #[test]
    fn test_validate_unit_test_fails_with_extra_mock_column() {
        let test = UnitTest {
            name: "test_user_summary".to_string(),
            target_view: "materialize.public.user_order_summary".to_string(),
            mocks: vec![
                MockView {
                    fqn: "materialize.public.users".to_string(),
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("name".to_string(), "text".to_string()),
                        ("email".to_string(), "text".to_string()),
                        ("extra_column".to_string(), "int".to_string()), // Extra column
                    ],
                    query: "SELECT * FROM VALUES (1, 'alice', 'alice@example.com', 42)".to_string(),
                },
                MockView {
                    fqn: "materialize.public.orders".to_string(),
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("user_id".to_string(), "bigint".to_string()),
                        ("amount".to_string(), "numeric".to_string()),
                    ],
                    query: "SELECT * FROM VALUES (1, 1, 100.00)".to_string(),
                },
            ],
            expected: ExpectedResult {
                columns: vec![
                    ("user_id".to_string(), "bigint".to_string()),
                    ("user_name".to_string(), "text".to_string()),
                    ("total_orders".to_string(), "bigint".to_string()),
                ],
                query: "SELECT * FROM VALUES (1, 'alice', 1)".to_string(),
            },
        };

        let types = make_test_types();
        let target_id = make_target_id();
        let dependencies = make_dependencies();

        let result = validate_unit_test(&test, &target_id, &types, &dependencies);
        assert!(result.is_err());

        match result.unwrap_err() {
            TestValidationError::MockSchemaMismatch(err) => {
                assert_eq!(err.mock_fqn, "materialize.public.users");
                assert!(err.extra_columns.contains(&"extra_column".to_string()));
                assert!(err.missing_columns.is_empty());
            }
            other => panic!("Expected MockSchemaMismatch error, got: {:?}", other),
        }
    }

    #[test]
    fn test_validate_unit_test_fails_with_type_mismatch() {
        let test = UnitTest {
            name: "test_user_summary".to_string(),
            target_view: "materialize.public.user_order_summary".to_string(),
            mocks: vec![
                MockView {
                    fqn: "materialize.public.users".to_string(),
                    columns: vec![
                        ("id".to_string(), "text".to_string()), // Wrong type: should be bigint
                        ("name".to_string(), "text".to_string()),
                        ("email".to_string(), "text".to_string()),
                    ],
                    query: "SELECT * FROM VALUES ('1', 'alice', 'alice@example.com')".to_string(),
                },
                MockView {
                    fqn: "materialize.public.orders".to_string(),
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("user_id".to_string(), "bigint".to_string()),
                        ("amount".to_string(), "numeric".to_string()),
                    ],
                    query: "SELECT * FROM VALUES (1, 1, 100.00)".to_string(),
                },
            ],
            expected: ExpectedResult {
                columns: vec![
                    ("user_id".to_string(), "bigint".to_string()),
                    ("user_name".to_string(), "text".to_string()),
                    ("total_orders".to_string(), "bigint".to_string()),
                ],
                query: "SELECT * FROM VALUES (1, 'alice', 1)".to_string(),
            },
        };

        let types = make_test_types();
        let target_id = make_target_id();
        let dependencies = make_dependencies();

        let result = validate_unit_test(&test, &target_id, &types, &dependencies);
        assert!(result.is_err());

        match result.unwrap_err() {
            TestValidationError::MockSchemaMismatch(err) => {
                assert_eq!(err.mock_fqn, "materialize.public.users");
                assert!(
                    err.type_mismatches
                        .iter()
                        .any(|(col, mock_t, _)| { col == "id" && mock_t == "text" })
                );
            }
            other => panic!("Expected MockSchemaMismatch error, got: {:?}", other),
        }
    }

    #[test]
    fn test_validate_unit_test_fails_with_expected_schema_mismatch() {
        let test = UnitTest {
            name: "test_user_summary".to_string(),
            target_view: "materialize.public.user_order_summary".to_string(),
            mocks: vec![
                MockView {
                    fqn: "materialize.public.users".to_string(),
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("name".to_string(), "text".to_string()),
                        ("email".to_string(), "text".to_string()),
                    ],
                    query: "SELECT * FROM VALUES (1, 'alice', 'alice@example.com')".to_string(),
                },
                MockView {
                    fqn: "materialize.public.orders".to_string(),
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("user_id".to_string(), "bigint".to_string()),
                        ("amount".to_string(), "numeric".to_string()),
                    ],
                    query: "SELECT * FROM VALUES (1, 1, 100.00)".to_string(),
                },
            ],
            expected: ExpectedResult {
                columns: vec![
                    ("user_id".to_string(), "bigint".to_string()),
                    // Missing 'user_name' column
                    ("total_orders".to_string(), "bigint".to_string()),
                ],
                query: "SELECT * FROM VALUES (1, 1)".to_string(),
            },
        };

        let types = make_test_types();
        let target_id = make_target_id();
        let dependencies = make_dependencies();

        let result = validate_unit_test(&test, &target_id, &types, &dependencies);
        assert!(result.is_err());

        match result.unwrap_err() {
            TestValidationError::ExpectedSchemaMismatch(err) => {
                assert_eq!(err.test_name, "test_user_summary");
                assert_eq!(err.target_view, "materialize.public.user_order_summary");
                assert!(
                    err.missing_columns
                        .iter()
                        .any(|(name, _)| name == "user_name")
                );
            }
            other => panic!("Expected ExpectedSchemaMismatch error, got: {:?}", other),
        }
    }

    #[test]
    fn test_validate_unit_test_fails_with_expected_type_mismatch() {
        let test = UnitTest {
            name: "test_user_summary".to_string(),
            target_view: "materialize.public.user_order_summary".to_string(),
            mocks: vec![
                MockView {
                    fqn: "materialize.public.users".to_string(),
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("name".to_string(), "text".to_string()),
                        ("email".to_string(), "text".to_string()),
                    ],
                    query: "SELECT * FROM VALUES (1, 'alice', 'alice@example.com')".to_string(),
                },
                MockView {
                    fqn: "materialize.public.orders".to_string(),
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("user_id".to_string(), "bigint".to_string()),
                        ("amount".to_string(), "numeric".to_string()),
                    ],
                    query: "SELECT * FROM VALUES (1, 1, 100.00)".to_string(),
                },
            ],
            expected: ExpectedResult {
                columns: vec![
                    ("user_id".to_string(), "bigint".to_string()),
                    ("user_name".to_string(), "bigint".to_string()), // Wrong type: should be text
                    ("total_orders".to_string(), "bigint".to_string()),
                ],
                query: "SELECT * FROM VALUES (1, 1, 1)".to_string(),
            },
        };

        let types = make_test_types();
        let target_id = make_target_id();
        let dependencies = make_dependencies();

        let result = validate_unit_test(&test, &target_id, &types, &dependencies);
        assert!(result.is_err());

        match result.unwrap_err() {
            TestValidationError::ExpectedSchemaMismatch(err) => {
                assert!(
                    err.type_mismatches
                        .iter()
                        .any(|(col, exp_t, _)| { col == "user_name" && exp_t == "bigint" })
                );
            }
            other => panic!("Expected ExpectedSchemaMismatch error, got: {:?}", other),
        }
    }

    // =========================================================================
    // FQN Normalization Tests
    // =========================================================================

    #[test]
    fn test_normalize_fqn_unqualified() {
        let target_id = ObjectId {
            database: "mydb".to_string(),
            schema: "myschema".to_string(),
            object: "myview".to_string(),
        };

        let normalized = normalize_fqn("users", &target_id);
        assert_eq!(normalized, "mydb.myschema.users");
    }

    #[test]
    fn test_normalize_fqn_schema_qualified() {
        let target_id = ObjectId {
            database: "mydb".to_string(),
            schema: "myschema".to_string(),
            object: "myview".to_string(),
        };

        let normalized = normalize_fqn("other_schema.users", &target_id);
        assert_eq!(normalized, "mydb.other_schema.users");
    }

    #[test]
    fn test_normalize_fqn_fully_qualified() {
        let target_id = ObjectId {
            database: "mydb".to_string(),
            schema: "myschema".to_string(),
            object: "myview".to_string(),
        };

        let normalized = normalize_fqn("other_db.other_schema.users", &target_id);
        assert_eq!(normalized, "other_db.other_schema.users");
    }

    // =========================================================================
    // Type Normalization Tests (based on https://materialize.com/docs/sql/types/)
    // =========================================================================

    #[test]
    fn test_normalize_type_integer_aliases() {
        // integer = int = int4
        assert_eq!(normalize_type("INT"), "integer");
        assert_eq!(normalize_type("int4"), "integer");
        assert_eq!(normalize_type("integer"), "integer");
        assert_eq!(normalize_type("INTEGER"), "integer");
    }

    #[test]
    fn test_normalize_type_bigint_aliases() {
        // bigint = int8
        assert_eq!(normalize_type("INT8"), "bigint");
        assert_eq!(normalize_type("bigint"), "bigint");
        assert_eq!(normalize_type("BIGINT"), "bigint");
    }

    #[test]
    fn test_normalize_type_smallint_aliases() {
        // smallint = int2
        assert_eq!(normalize_type("INT2"), "smallint");
        assert_eq!(normalize_type("smallint"), "smallint");
        assert_eq!(normalize_type("SMALLINT"), "smallint");
    }

    #[test]
    fn test_normalize_type_real_aliases() {
        // real = float4
        assert_eq!(normalize_type("float4"), "real");
        assert_eq!(normalize_type("FLOAT4"), "real");
        assert_eq!(normalize_type("real"), "real");
        assert_eq!(normalize_type("REAL"), "real");
    }

    #[test]
    fn test_normalize_type_double_precision_aliases() {
        // double precision = float = float8 = double
        assert_eq!(normalize_type("float"), "double precision");
        assert_eq!(normalize_type("FLOAT"), "double precision");
        assert_eq!(normalize_type("float8"), "double precision");
        assert_eq!(normalize_type("FLOAT8"), "double precision");
        assert_eq!(normalize_type("double"), "double precision");
        assert_eq!(normalize_type("DOUBLE"), "double precision");
        assert_eq!(normalize_type("double precision"), "double precision");
        assert_eq!(normalize_type("DOUBLE PRECISION"), "double precision");
    }

    #[test]
    fn test_normalize_type_boolean_aliases() {
        // boolean = bool
        assert_eq!(normalize_type("bool"), "boolean");
        assert_eq!(normalize_type("boolean"), "boolean");
        assert_eq!(normalize_type("BOOL"), "boolean");
        assert_eq!(normalize_type("BOOLEAN"), "boolean");
    }

    #[test]
    fn test_normalize_type_text_aliases() {
        // text = string = varchar = character varying
        assert_eq!(normalize_type("text"), "text");
        assert_eq!(normalize_type("TEXT"), "text");
        assert_eq!(normalize_type("string"), "text");
        assert_eq!(normalize_type("STRING"), "text");
        assert_eq!(normalize_type("varchar"), "text");
        assert_eq!(normalize_type("VARCHAR"), "text");
        assert_eq!(normalize_type("varchar(255)"), "text");
        assert_eq!(normalize_type("character varying"), "text");
        assert_eq!(normalize_type("character varying(100)"), "text");
    }

    #[test]
    fn test_normalize_type_numeric_aliases() {
        // numeric = decimal
        assert_eq!(normalize_type("numeric"), "numeric");
        assert_eq!(normalize_type("NUMERIC"), "numeric");
        assert_eq!(normalize_type("decimal"), "numeric");
        assert_eq!(normalize_type("DECIMAL"), "numeric");
        assert_eq!(normalize_type("numeric(10,2)"), "numeric");
        assert_eq!(normalize_type("decimal(18,4)"), "numeric");
    }

    #[test]
    fn test_normalize_type_jsonb_aliases() {
        // jsonb = json
        assert_eq!(normalize_type("json"), "jsonb");
        assert_eq!(normalize_type("JSON"), "jsonb");
        assert_eq!(normalize_type("jsonb"), "jsonb");
        assert_eq!(normalize_type("JSONB"), "jsonb");
    }

    #[test]
    fn test_normalize_type_timestamptz_aliases() {
        // timestamp with time zone = timestamptz
        assert_eq!(normalize_type("timestamptz"), "timestamp with time zone");
        assert_eq!(normalize_type("TIMESTAMPTZ"), "timestamp with time zone");
        assert_eq!(
            normalize_type("timestamp with time zone"),
            "timestamp with time zone"
        );
        assert_eq!(
            normalize_type("TIMESTAMP WITH TIME ZONE"),
            "timestamp with time zone"
        );
    }

    #[test]
    fn test_normalize_type_preserves_other_types() {
        // Types without aliases should be preserved as-is (lowercased)
        assert_eq!(normalize_type("timestamp"), "timestamp");
        assert_eq!(normalize_type("TIMESTAMP"), "timestamp");
        assert_eq!(normalize_type("date"), "date");
        assert_eq!(normalize_type("time"), "time");
        assert_eq!(normalize_type("interval"), "interval");
        assert_eq!(normalize_type("uuid"), "uuid");
        assert_eq!(normalize_type("bytea"), "bytea");
        assert_eq!(normalize_type("oid"), "oid");
        assert_eq!(normalize_type("uint2"), "uint2");
        assert_eq!(normalize_type("uint4"), "uint4");
        assert_eq!(normalize_type("uint8"), "uint8");
    }

    #[test]
    fn test_normalize_type_handles_whitespace() {
        assert_eq!(normalize_type("  INT  "), "integer");
        assert_eq!(normalize_type("\ttext\n"), "text");
        assert_eq!(normalize_type("  double precision  "), "double precision");
    }

    #[test]
    fn test_normalize_type_case_insensitive() {
        // Validation should be completely case-agnostic
        // integer variants
        assert_eq!(normalize_type("integer"), normalize_type("INTEGER"));
        assert_eq!(normalize_type("integer"), normalize_type("Integer"));
        assert_eq!(normalize_type("integer"), normalize_type("iNtEgEr"));
        assert_eq!(normalize_type("int"), normalize_type("INT"));
        assert_eq!(normalize_type("int"), normalize_type("Int"));

        // bigint variants
        assert_eq!(normalize_type("bigint"), normalize_type("BIGINT"));
        assert_eq!(normalize_type("bigint"), normalize_type("BigInt"));
        assert_eq!(normalize_type("int8"), normalize_type("INT8"));

        // text variants
        assert_eq!(normalize_type("text"), normalize_type("TEXT"));
        assert_eq!(normalize_type("text"), normalize_type("Text"));
        assert_eq!(normalize_type("string"), normalize_type("STRING"));
        assert_eq!(normalize_type("string"), normalize_type("String"));

        // boolean variants
        assert_eq!(normalize_type("boolean"), normalize_type("BOOLEAN"));
        assert_eq!(normalize_type("boolean"), normalize_type("Boolean"));
        assert_eq!(normalize_type("bool"), normalize_type("BOOL"));
        assert_eq!(normalize_type("bool"), normalize_type("Bool"));

        // numeric variants
        assert_eq!(normalize_type("numeric"), normalize_type("NUMERIC"));
        assert_eq!(normalize_type("numeric"), normalize_type("Numeric"));
        assert_eq!(normalize_type("decimal"), normalize_type("DECIMAL"));

        // double precision variants
        assert_eq!(
            normalize_type("double precision"),
            normalize_type("DOUBLE PRECISION")
        );
        assert_eq!(
            normalize_type("double precision"),
            normalize_type("Double Precision")
        );

        // timestamp with time zone variants
        assert_eq!(
            normalize_type("timestamp with time zone"),
            normalize_type("TIMESTAMP WITH TIME ZONE")
        );
        assert_eq!(normalize_type("timestamptz"), normalize_type("TIMESTAMPTZ"));
        assert_eq!(normalize_type("timestamptz"), normalize_type("TimestampTZ"));

        // jsonb variants
        assert_eq!(normalize_type("jsonb"), normalize_type("JSONB"));
        assert_eq!(normalize_type("jsonb"), normalize_type("JsonB"));
        assert_eq!(normalize_type("json"), normalize_type("JSON"));
    }

    // =========================================================================
    // Column Comparison Tests
    // =========================================================================

    #[test]
    fn test_compare_columns_exact_match() {
        let test_columns = vec![
            ("id".to_string(), "bigint".to_string()),
            ("name".to_string(), "text".to_string()),
        ];

        let mut actual_columns = BTreeMap::new();
        actual_columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: false,
            },
        );
        actual_columns.insert(
            "name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
            },
        );

        let (extra, missing, type_mismatches) = compare_columns(&test_columns, &actual_columns);
        assert!(extra.is_empty());
        assert!(missing.is_empty());
        assert!(type_mismatches.is_empty());
    }

    #[test]
    fn test_compare_columns_with_type_aliases() {
        let test_columns = vec![
            ("id".to_string(), "INT".to_string()), // Should match "integer"
            ("count".to_string(), "INT8".to_string()), // Should match "bigint"
        ];

        let mut actual_columns = BTreeMap::new();
        actual_columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: false,
            },
        );
        actual_columns.insert(
            "count".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: false,
            },
        );

        let (extra, missing, type_mismatches) = compare_columns(&test_columns, &actual_columns);
        assert!(extra.is_empty());
        assert!(missing.is_empty());
        assert!(type_mismatches.is_empty());
    }

    #[test]
    fn test_compare_columns_detects_extra() {
        let test_columns = vec![
            ("id".to_string(), "bigint".to_string()),
            ("extra".to_string(), "text".to_string()),
        ];

        let mut actual_columns = BTreeMap::new();
        actual_columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: false,
            },
        );

        let (extra, missing, _) = compare_columns(&test_columns, &actual_columns);
        assert_eq!(extra, vec!["extra".to_string()]);
        assert!(missing.is_empty());
    }

    #[test]
    fn test_compare_columns_detects_missing() {
        let test_columns = vec![("id".to_string(), "bigint".to_string())];

        let mut actual_columns = BTreeMap::new();
        actual_columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: false,
            },
        );
        actual_columns.insert(
            "name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
            },
        );

        let (extra, missing, _) = compare_columns(&test_columns, &actual_columns);
        assert!(extra.is_empty());
        assert_eq!(missing, vec![("name".to_string(), "text".to_string())]);
    }

    #[test]
    fn test_compare_columns_detects_type_mismatch() {
        let test_columns = vec![("id".to_string(), "text".to_string())];

        let mut actual_columns = BTreeMap::new();
        actual_columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: false,
            },
        );

        let (_, _, type_mismatches) = compare_columns(&test_columns, &actual_columns);
        assert_eq!(type_mismatches.len(), 1);
        assert_eq!(type_mismatches[0].0, "id");
        assert_eq!(type_mismatches[0].1, "text");
        assert_eq!(type_mismatches[0].2, "bigint");
    }

    // =========================================================================
    // Partial FQN Mock Resolution Tests
    // =========================================================================

    #[test]
    fn test_validate_with_unqualified_mock_name() {
        // Test that unqualified mock names get resolved correctly
        let test = UnitTest {
            name: "test_partial_fqn".to_string(),
            target_view: "materialize.public.user_order_summary".to_string(),
            mocks: vec![
                MockView {
                    fqn: "users".to_string(), // Unqualified - should resolve to materialize.public.users
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("name".to_string(), "text".to_string()),
                        ("email".to_string(), "text".to_string()),
                    ],
                    query: "SELECT * FROM VALUES (1, 'alice', 'alice@example.com')".to_string(),
                },
                MockView {
                    fqn: "public.orders".to_string(), // Schema qualified - should resolve to materialize.public.orders
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("user_id".to_string(), "bigint".to_string()),
                        ("amount".to_string(), "numeric".to_string()),
                    ],
                    query: "SELECT * FROM VALUES (1, 1, 100.00)".to_string(),
                },
            ],
            expected: ExpectedResult {
                columns: vec![
                    ("user_id".to_string(), "bigint".to_string()),
                    ("user_name".to_string(), "text".to_string()),
                    ("total_orders".to_string(), "bigint".to_string()),
                ],
                query: "SELECT * FROM VALUES (1, 'alice', 1)".to_string(),
            },
        };

        let types = make_test_types();
        let target_id = make_target_id();
        let dependencies = make_dependencies();

        let result = validate_unit_test(&test, &target_id, &types, &dependencies);
        assert!(result.is_ok(), "Expected validation to pass: {:?}", result);
    }

    #[test]
    fn test_validate_passes_with_no_dependencies() {
        let test = UnitTest {
            name: "test_no_deps".to_string(),
            target_view: "materialize.public.my_view".to_string(),
            mocks: vec![],
            expected: ExpectedResult {
                columns: vec![("result".to_string(), "integer".to_string())],
                query: "SELECT * FROM VALUES (42)".to_string(),
            },
        };

        let types = Types::default();
        let target_id = ObjectId {
            database: "materialize".to_string(),
            schema: "public".to_string(),
            object: "my_view".to_string(),
        };
        let dependencies = BTreeSet::new(); // No dependencies

        let result = validate_unit_test(&test, &target_id, &types, &dependencies);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_skips_unknown_mock() {
        // If a mock isn't in types, validation should be permissive
        let test = UnitTest {
            name: "test_unknown_mock".to_string(),
            target_view: "materialize.public.my_view".to_string(),
            mocks: vec![MockView {
                fqn: "materialize.public.unknown_table".to_string(),
                columns: vec![("id".to_string(), "bigint".to_string())],
                query: "SELECT * FROM VALUES (1)".to_string(),
            }],
            expected: ExpectedResult {
                columns: vec![("result".to_string(), "integer".to_string())],
                query: "SELECT * FROM VALUES (42)".to_string(),
            },
        };

        let types = Types::default(); // Empty types - unknown_table not in types
        let target_id = ObjectId {
            database: "materialize".to_string(),
            schema: "public".to_string(),
            object: "my_view".to_string(),
        };

        // Dependency is present but not in types
        let mut dependencies = BTreeSet::new();
        dependencies.insert(ObjectId {
            database: "materialize".to_string(),
            schema: "public".to_string(),
            object: "unknown_table".to_string(),
        });

        // Should pass because mock covers the dependency, even though type info is missing
        let result = validate_unit_test(&test, &target_id, &types, &dependencies);
        assert!(result.is_ok());
    }
}
