// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Validate a [`UnitTest`] and lower it into SQL Materialize can execute.
//!
//! The lowered form is a sequence of `CREATE TEMPORARY VIEW` statements
//! (mocks, expected, target) followed by an assertion query whose rows
//! describe mismatches. An empty result means the test passed.
//!
//! ```sql
//! EXECUTE UNIT TEST test_name
//! FOR database.schema.view_name
//! [AT TIME 'timestamp']  -- optional, sets mz_now() during test
//! MOCK database.schema.mock1(col1 TYPE1, col2 TYPE2) AS (
//!   SELECT * FROM VALUES (...)
//! ),
//! MOCK database.schema.mock2(col TYPE) AS (
//!   SELECT * FROM VALUES (...)
//! )
//! EXPECTED(col1 TYPE1, col2 TYPE2) AS (
//!   SELECT * FROM VALUES (...)
//! );
//! ```

use crate::project::ast::Statement;
use crate::project::ir::compiled::FullyQualifiedName;
use crate::project::ir::object_id::ObjectId;
use crate::project::ir::unit_test::{ExpectedResult, MockView, UnitTest};
use crate::project::resolve::normalize::NormalizingVisitor;
use crate::types::ColumnType;
#[cfg(test)]
use crate::types::Types;
use mz_sql_parser::ast::{CreateViewStatement, IfExistsBehavior, ViewDefinition};
use owo_colors::{OwoColorize, Stream, Style};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use thiserror::Error;

/// Errors that can occur during unit test validation.
#[derive(Debug, Error, Serialize)]
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

    /// The AT TIME value is not a valid timestamp
    #[error("invalid at_time timestamp")]
    InvalidAtTime(InvalidAtTimeError),

    /// Types cache is missing or stale
    #[error("types cache unavailable: {reason}")]
    TypesCacheUnavailable { reason: String },
}

/// Error: A dependency of the target view is not mocked.
#[derive(Debug, Serialize)]
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
        let error_style = Style::new().bright_red().bold();
        let marker_style = Style::new().bright_blue().bold();
        writeln!(
            f,
            "{}: test '{}' has unmocked dependencies",
            "error".if_supports_color(Stream::Stderr, |t| error_style.style(t)),
            self.test_name
                .if_supports_color(Stream::Stderr, |t| t.cyan())
        )?;
        writeln!(
            f,
            " {} target view: {}",
            "-->".if_supports_color(Stream::Stderr, |t| marker_style.style(t)),
            self.target_view
                .if_supports_color(Stream::Stderr, |t| t.yellow())
        )?;
        writeln!(f)?;
        writeln!(
            f,
            "  {} The following dependencies must be mocked:",
            "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t))
        )?;
        for dep in &self.missing_mocks {
            writeln!(
                f,
                "  {}   - {}",
                "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t)),
                dep.if_supports_color(Stream::Stderr, |t| t.yellow())
            )?;
        }
        writeln!(f)?;
        writeln!(
            f,
            "  {} Add mocks for these dependencies in the WITH clause of the test",
            "=".if_supports_color(Stream::Stderr, |t| marker_style.style(t))
        )?;
        Ok(())
    }
}

impl std::error::Error for UnmockedDependencyError {}

/// Error: A mock's columns don't match the actual schema.
#[derive(Debug, Serialize)]
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
        let error_style = Style::new().bright_red().bold();
        let marker_style = Style::new().bright_blue().bold();
        writeln!(
            f,
            "{}: mock '{}' schema doesn't match actual schema",
            "error".if_supports_color(Stream::Stderr, |t| error_style.style(t)),
            self.mock_fqn
                .if_supports_color(Stream::Stderr, |t| t.cyan())
        )?;
        writeln!(
            f,
            " {} in test: {}",
            "-->".if_supports_color(Stream::Stderr, |t| marker_style.style(t)),
            self.test_name
                .if_supports_color(Stream::Stderr, |t| t.yellow())
        )?;
        writeln!(f)?;

        if !self.missing_columns.is_empty() {
            writeln!(
                f,
                "  {} Missing columns (required but not in mock):",
                "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t))
            )?;
            for (col, typ) in &self.missing_columns {
                writeln!(
                    f,
                    "  {}   - {} {}",
                    "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t)),
                    col.if_supports_color(Stream::Stderr, |t| t.red()),
                    typ.to_uppercase()
                        .if_supports_color(Stream::Stderr, |t| t.dimmed())
                )?;
            }
        }

        if !self.extra_columns.is_empty() {
            writeln!(
                f,
                "  {} Extra columns (in mock but not in actual schema):",
                "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t))
            )?;
            for col in &self.extra_columns {
                writeln!(
                    f,
                    "  {}   - {}",
                    "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t)),
                    col.if_supports_color(Stream::Stderr, |t| t.yellow())
                )?;
            }
        }

        if !self.type_mismatches.is_empty() {
            writeln!(
                f,
                "  {} Type mismatches:",
                "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t))
            )?;
            for (col, mock_type, actual_type) in &self.type_mismatches {
                writeln!(
                    f,
                    "  {}   - {}: mock has '{}', expected '{}'",
                    "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t)),
                    col.if_supports_color(Stream::Stderr, |t| t.cyan()),
                    mock_type.if_supports_color(Stream::Stderr, |t| t.red()),
                    actual_type.if_supports_color(Stream::Stderr, |t| t.green())
                )?;
            }
        }

        writeln!(f)?;

        if !self.actual_schema.is_empty() {
            writeln!(
                f,
                "  {} Expected mock signature:",
                "=".if_supports_color(Stream::Stderr, |t| marker_style.style(t))
            )?;
            let cols: Vec<String> = self
                .actual_schema
                .iter()
                .map(|(name, typ)| format!("{} {}", name, typ.to_uppercase()))
                .collect();
            writeln!(
                f,
                "  {}   MOCK {}({}) AS (...)",
                "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t)),
                self.mock_fqn
                    .if_supports_color(Stream::Stderr, |t| t.green()),
                cols.join(", ")
                    .if_supports_color(Stream::Stderr, |t| t.green())
            )?;
        }

        Ok(())
    }
}

impl std::error::Error for MockSchemaMismatchError {}

/// Error: Expected output columns don't match the target view schema.
#[derive(Debug, Serialize)]
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
        let error_style = Style::new().bright_red().bold();
        let marker_style = Style::new().bright_blue().bold();
        writeln!(
            f,
            "{}: expected output schema doesn't match target view",
            "error".if_supports_color(Stream::Stderr, |t| error_style.style(t))
        )?;
        writeln!(
            f,
            " {} target: {} | test: {}",
            "-->".if_supports_color(Stream::Stderr, |t| marker_style.style(t)),
            self.target_view
                .if_supports_color(Stream::Stderr, |t| t.cyan()),
            self.test_name
                .if_supports_color(Stream::Stderr, |t| t.yellow())
        )?;
        writeln!(f)?;

        if !self.missing_columns.is_empty() {
            writeln!(
                f,
                "  {} Missing columns (in target view but not in expected):",
                "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t))
            )?;
            for (col, typ) in &self.missing_columns {
                writeln!(
                    f,
                    "  {}   - {} {}",
                    "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t)),
                    col.if_supports_color(Stream::Stderr, |t| t.red()),
                    typ.to_uppercase()
                        .if_supports_color(Stream::Stderr, |t| t.dimmed())
                )?;
            }
        }

        if !self.extra_columns.is_empty() {
            writeln!(
                f,
                "  {} Extra columns (in expected but not in target view):",
                "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t))
            )?;
            for col in &self.extra_columns {
                writeln!(
                    f,
                    "  {}   - {}",
                    "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t)),
                    col.if_supports_color(Stream::Stderr, |t| t.yellow())
                )?;
            }
        }

        if !self.type_mismatches.is_empty() {
            writeln!(
                f,
                "  {} Type mismatches:",
                "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t))
            )?;
            for (col, expected_type, actual_type) in &self.type_mismatches {
                writeln!(
                    f,
                    "  {}   - {}: has '{}', expected '{}'",
                    "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t)),
                    col.if_supports_color(Stream::Stderr, |t| t.cyan()),
                    expected_type.if_supports_color(Stream::Stderr, |t| t.red()),
                    actual_type.if_supports_color(Stream::Stderr, |t| t.green())
                )?;
            }
        }

        writeln!(f)?;

        if !self.actual_schema.is_empty() {
            writeln!(
                f,
                "  {} Expected signature:",
                "=".if_supports_color(Stream::Stderr, |t| marker_style.style(t))
            )?;
            let cols: Vec<String> = self
                .actual_schema
                .iter()
                .map(|(name, typ)| format!("{} {}", name, typ.to_uppercase()))
                .collect();
            writeln!(
                f,
                "  {}   EXPECTED({}) AS (...)",
                "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t)),
                cols.join(", ")
                    .if_supports_color(Stream::Stderr, |t| t.green())
            )?;
        }

        Ok(())
    }
}

impl std::error::Error for ExpectedSchemaMismatchError {}

/// Error: The AT TIME value is not a valid timestamp.
#[derive(Debug, Serialize)]
pub struct InvalidAtTimeError {
    /// Test name
    pub test_name: String,
    /// The invalid AT TIME value
    pub at_time_value: String,
    /// The database error message
    pub db_error: String,
}

impl fmt::Display for InvalidAtTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let error_style = Style::new().bright_red().bold();
        let marker_style = Style::new().bright_blue().bold();
        writeln!(
            f,
            "{}: test '{}' has invalid AT TIME value",
            "error".if_supports_color(Stream::Stderr, |t| error_style.style(t)),
            self.test_name
                .if_supports_color(Stream::Stderr, |t| t.cyan())
        )?;
        writeln!(
            f,
            " {} value: {}",
            "-->".if_supports_color(Stream::Stderr, |t| marker_style.style(t)),
            self.at_time_value
                .if_supports_color(Stream::Stderr, |t| t.yellow())
        )?;
        writeln!(f)?;

        // Show the useful tail of DB errors like:
        //   "Error: invalid input syntax for type mz_timestamp: ..."
        let display_error = self
            .db_error
            .find("invalid input syntax")
            .map(|idx| &self.db_error[idx..])
            .unwrap_or(&self.db_error);

        writeln!(
            f,
            "  {} {}",
            "|".if_supports_color(Stream::Stderr, |t| marker_style.style(t)),
            display_error.if_supports_color(Stream::Stderr, |t| t.red())
        )?;
        writeln!(f)?;
        writeln!(
            f,
            "  {} The AT TIME value must be a valid timestamp that can be cast to mz_timestamp",
            "=".if_supports_color(Stream::Stderr, |t| marker_style.style(t))
        )?;
        writeln!(
            f,
            "  {} Example: AT TIME '2024-01-15 10:00:00'",
            "=".if_supports_color(Stream::Stderr, |t| marker_style.style(t))
        )?;
        Ok(())
    }
}

impl std::error::Error for InvalidAtTimeError {}

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
/// * `get_columns` - Lookup for the column schema of an object, sourced from
///   types.lock (external) and the build artifact database (internal)
/// * `dependencies` - Dependencies of the target view from the project's
///   dependency graph
pub(super) fn validate_unit_test(
    test: &UnitTest,
    target_id: &ObjectId,
    get_columns: &dyn Fn(&ObjectId) -> Option<BTreeMap<String, ColumnType>>,
    dependencies: &BTreeSet<ObjectId>,
) -> Result<(), TestValidationError> {
    let mocked_ids: BTreeSet<ObjectId> = test
        .mocks
        .iter()
        .map(|m| normalize_fqn(&m.fqn, target_id))
        .collect();

    let missing_mocks: Vec<String> = dependencies
        .iter()
        .filter(|dep| !mocked_ids.contains(*dep))
        .map(|dep| dep.to_string())
        .collect();

    if !missing_mocks.is_empty() {
        return Err(TestValidationError::UnmockedDependency(
            UnmockedDependencyError {
                test_name: test.name.clone(),
                target_view: target_id.to_string(),
                missing_mocks,
            },
        ));
    }

    for mock in &test.mocks {
        let mock_id = normalize_fqn(&mock.fqn, target_id);

        if let Some(actual_columns) = get_columns(&mock_id) {
            let (extra, missing, type_mismatches) = compare_columns(&mock.columns, &actual_columns);

            if !extra.is_empty() || !missing.is_empty() || !type_mismatches.is_empty() {
                let actual_schema: Vec<(String, String)> = actual_columns
                    .iter()
                    .map(|(name, col_type)| (name.clone(), col_type.r#type.clone()))
                    .collect();

                return Err(TestValidationError::MockSchemaMismatch(
                    MockSchemaMismatchError {
                        test_name: test.name.clone(),
                        mock_fqn: mock_id.to_string(),
                        extra_columns: extra,
                        missing_columns: missing,
                        type_mismatches,
                        actual_schema,
                    },
                ));
            }
        }
        // Mocks not present in types are likely external dependencies not in
        // types.lock; allow them through and let the database surface any
        // mismatch at execution time.
    }

    if let Some(target_columns) = get_columns(target_id) {
        let (extra, missing, type_mismatches) =
            compare_columns(&test.expected.columns, &target_columns);

        if !extra.is_empty() || !missing.is_empty() || !type_mismatches.is_empty() {
            let actual_schema: Vec<(String, String)> = target_columns
                .iter()
                .map(|(name, col_type)| (name.clone(), col_type.r#type.clone()))
                .collect();

            return Err(TestValidationError::ExpectedSchemaMismatch(
                ExpectedSchemaMismatchError {
                    test_name: test.name.clone(),
                    target_view: target_id.to_string(),
                    extra_columns: extra,
                    missing_columns: missing,
                    type_mismatches,
                    actual_schema,
                },
            ));
        }
    }
    // If target isn't in types, we'll catch it during test execution.

    Ok(())
}

/// Normalize a potentially partial FQN to a fully qualified `ObjectId` using the target's context.
fn normalize_fqn(fqn: &str, target_id: &ObjectId) -> ObjectId {
    let parts: Vec<&str> = fqn.split('.').collect();
    match parts.as_slice() {
        [object] => ObjectId::new(
            target_id.expect_database().to_string(),
            target_id.schema().to_string(),
            (*object).to_string(),
        ),
        [schema, object] => ObjectId::new(
            target_id.expect_database().to_string(),
            (*schema).to_string(),
            (*object).to_string(),
        ),
        [db, schema, object] => ObjectId::new(
            (*db).to_string(),
            (*schema).to_string(),
            (*object).to_string(),
        ),
        _ => ObjectId::new(String::new(), String::new(), fqn.to_string()),
    }
}

/// Compare test columns against actual schema columns.
///
/// Returns (extra_columns, missing_columns_with_types, type_mismatches).
fn compare_columns(
    test_columns: &[(String, String)],
    actual_columns: &BTreeMap<String, ColumnType>,
) -> (
    Vec<String>,
    Vec<(String, String)>,
    Vec<(String, String, String)>,
) {
    let test_col_names: BTreeSet<&str> = test_columns.iter().map(|(n, _)| n.as_str()).collect();
    let actual_col_names: BTreeSet<&str> = actual_columns.keys().map(|s| s.as_str()).collect();

    let extra: Vec<String> = test_col_names
        .difference(&actual_col_names)
        .map(|s| (*s).to_string())
        .collect();

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

    let type_mismatches: Vec<(String, String, String)> = test_columns
        .iter()
        .filter_map(|(name, test_type)| {
            actual_columns.get(name).and_then(|actual| {
                let test_normalized = normalize_type(test_type);
                let actual_normalized = normalize_type(&actual.r#type);

                if test_normalized != actual_normalized {
                    // SHOW COLUMNS returns bare container types (e.g. "list"
                    // instead of "int8 list"); treat bare containers as matching
                    // any parameterized variant.
                    if types_match_with_bare_containers(&test_normalized, &actual_normalized) {
                        None
                    } else {
                        Some((name.clone(), test_type.clone(), actual.r#type.clone()))
                    }
                } else {
                    None
                }
            })
        })
        .collect();

    (extra, missing, type_mismatches)
}

/// Check if two normalized types match when accounting for bare container types.
///
/// SHOW COLUMNS returns bare container types (e.g. "list" instead of "int8 list"),
/// stripping the element type. This function treats bare containers as matching
/// any parameterized variant of the same container.
fn types_match_with_bare_containers(a: &str, b: &str) -> bool {
    if a == "list" && b.ends_with(" list") || b == "list" && a.ends_with(" list") {
        return true;
    }
    if a == "[]" && b.ends_with("[]") || b == "[]" && a.ends_with("[]") {
        return true;
    }
    if a == "map" && b.starts_with("map[") || b == "map" && a.starts_with("map[") {
        return true;
    }
    false
}

/// Normalize a SQL type for comparison.
///
/// Handles Materialize type aliases so that equivalent types compare equal.
/// See: https://materialize.com/docs/sql/types/
fn normalize_type(t: &str) -> String {
    let normalized = t.trim().to_lowercase();

    if let Some(element) = normalized.strip_suffix(" list") {
        if !element.is_empty() {
            return format!("{} list", normalize_type(element));
        }
    }

    if let Some(element) = normalized.strip_suffix("[]") {
        if !element.is_empty() {
            return format!("{}[]", normalize_type(element));
        }
    }

    if let Some(inner) = normalized
        .strip_prefix("map[")
        .and_then(|s| s.strip_suffix(']'))
    {
        if let Some((key, value)) = inner.split_once("=>") {
            return format!("map[{}=>{}]", normalize_type(key), normalize_type(value));
        }
    }

    match normalized.as_str() {
        "int" | "int4" | "integer" => "integer".to_string(),
        "int8" | "bigint" => "bigint".to_string(),
        "int2" | "smallint" => "smallint".to_string(),

        "float4" | "real" => "real".to_string(),
        "float" | "float8" | "double" | "double precision" => "double precision".to_string(),

        "bool" | "boolean" => "boolean".to_string(),

        "string" | "text" => "text".to_string(),
        "varchar" | "character varying" => "text".to_string(),

        "decimal" | "numeric" => "numeric".to_string(),

        "json" | "jsonb" => "jsonb".to_string(),

        "timestamp" | "timestamp without time zone" => "timestamp without time zone".to_string(),
        "timestamptz" | "timestamp with time zone" => "timestamp with time zone".to_string(),

        _ => {
            if normalized.starts_with("varchar") || normalized.starts_with("character varying") {
                "text".to_string()
            } else if normalized.starts_with("numeric") || normalized.starts_with("decimal") {
                "numeric".to_string()
            } else if normalized.starts_with("timestamp with time zone")
                || normalized.starts_with("timestamptz")
            {
                "timestamp with time zone".to_string()
            } else if normalized.starts_with("timestamp without time zone")
                || normalized == "timestamp"
            {
                "timestamp without time zone".to_string()
            } else {
                normalized
            }
        }
    }
}

/// Lower a unit test into executable SQL statements.
///
/// Returns a vector of SQL strings in order:
/// 1. CREATE TEMPORARY VIEW for each mock
/// 2. CREATE TEMPORARY VIEW for expected
/// 3. CREATE TEMPORARY VIEW for the target (flattened)
/// 4. Test query with status column
pub(super) fn lower_unit_test(
    test: &UnitTest,
    target_stmt: &Statement,
    target_fqn: &FullyQualifiedName,
) -> Result<Vec<String>, String> {
    let mut statements = Vec::new();

    for mock in &test.mocks {
        let qualified_mock = qualify_mock_name(mock, target_fqn);
        statements.push(create_mock_view_sql(&qualified_mock));
    }

    statements.push(create_expected_view_sql(&test.expected));

    statements.push(create_target_view_sql(target_stmt, target_fqn)?);

    let target_fqn_str = format!(
        "{}.{}.{}",
        target_fqn.database(),
        target_fqn.schema(),
        target_fqn.object()
    );
    let flattened_target_name = flatten_fqn(&target_fqn_str);
    statements.push(create_test_query_sql(
        &flattened_target_name,
        test.at_time.as_deref(),
    ));

    Ok(statements)
}

/// Quote a fully qualified name as a single identifier with dots.
fn flatten_fqn(fqn: &str) -> String {
    format!("\"{}\"", fqn)
}

/// Qualify a mock name with the target's FQN context if it's not already qualified.
fn qualify_mock_name(mock: &MockView, target_fqn: &FullyQualifiedName) -> MockView {
    let parts = mock.fqn.matches('.').count() + 1;

    let qualified_fqn = match parts {
        1 => format!(
            "{}.{}.{}",
            target_fqn.database(),
            target_fqn.schema(),
            mock.fqn
        ),
        2 => format!("{}.{}", target_fqn.database(), mock.fqn),
        _ => mock.fqn.clone(),
    };

    MockView {
        fqn: qualified_fqn,
        columns: mock.columns.clone(),
        query: mock.query.clone(),
    }
}

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
///
/// Returns an error if the target statement is not a `CREATE VIEW` or
/// `CREATE MATERIALIZED VIEW` — unit tests only apply to those object types.
fn create_target_view_sql(stmt: &Statement, fqn: &FullyQualifiedName) -> Result<String, String> {
    let mut visitor = NormalizingVisitor::flattening(fqn);
    let transformed_stmt = stmt
        .clone()
        .normalize_name_with(&visitor, &fqn.to_item_name())
        .normalize_dependencies_with(&mut visitor);

    let view_stmt = match transformed_stmt {
        Statement::CreateView(view) => CreateViewStatement {
            if_exists: IfExistsBehavior::Error,
            temporary: true,
            definition: view.definition.clone(),
        },
        Statement::CreateMaterializedView(mv) => CreateViewStatement {
            if_exists: IfExistsBehavior::Error,
            temporary: true,
            definition: ViewDefinition {
                name: mv.name,
                columns: mv.columns,
                query: mv.query,
            },
        },
        other => {
            return Err(format!(
                "unit tests are only supported on views and materialized views; \
                 target '{}.{}.{}' is a {}",
                fqn.database(),
                fqn.schema(),
                fqn.object(),
                other.kind(),
            ));
        }
    };
    Ok(view_stmt.to_string())
}

/// Create the test assertion query that returns failures.
///
/// Returns rows with a 'status' column indicating the failure mode:
/// - 'MISSING': Expected rows not found in actual results
/// - 'UNEXPECTED': Actual rows not found in expected results
///
/// Empty result means the test passed.
///
/// If `at_time` is provided, the query includes an `AS OF` clause to set
/// the value of `mz_now()` during test execution.
fn create_test_query_sql(flattened_target_name: &str, at_time: Option<&str>) -> String {
    let as_of_clause = at_time
        .map(|t| format!(" AS OF {}::mz_timestamp", t))
        .unwrap_or_default();
    format!(
        r#"SELECT 'MISSING' as status, * FROM expected
EXCEPT
SELECT 'MISSING', * FROM {}

UNION ALL

SELECT 'UNEXPECTED' as status, * FROM {}
EXCEPT
SELECT 'UNEXPECTED', * FROM expected{}"#,
        flattened_target_name, flattened_target_name, as_of_clause
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
        let sql = create_test_query_sql("materialize_public_my_view", None);

        assert!(sql.contains("SELECT 'MISSING' as status, * FROM expected"));
        assert!(sql.contains("SELECT 'MISSING', * FROM materialize_public_my_view"));
        assert!(sql.contains("SELECT 'UNEXPECTED' as status, * FROM materialize_public_my_view"));
        assert!(sql.contains("SELECT 'UNEXPECTED', * FROM expected"));
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("EXCEPT"));
        assert!(!sql.contains("AS OF"));
    }

    #[test]
    fn test_create_test_query_sql_with_at_time() {
        let sql =
            create_test_query_sql("materialize_public_my_view", Some("'2024-01-15 10:00:00'"));

        assert!(sql.contains("SELECT 'MISSING' as status, * FROM expected"));
        assert!(sql.contains("AS OF '2024-01-15 10:00:00'::mz_timestamp"));
    }

    fn make_test_types() -> Types {
        let mut objects = BTreeMap::new();

        let mut users_cols = BTreeMap::new();
        users_cols.insert(
            "id".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: false,
                position: 0,
                comment: None,
            },
        );
        users_cols.insert(
            "name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
                position: 1,
                comment: None,
            },
        );
        users_cols.insert(
            "email".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
                position: 2,
                comment: None,
            },
        );
        objects.insert(
            "materialize.public.users".parse::<ObjectId>().unwrap(),
            users_cols,
        );

        let mut orders_cols = BTreeMap::new();
        orders_cols.insert(
            "id".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: false,
                position: 0,
                comment: None,
            },
        );
        orders_cols.insert(
            "user_id".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: false,
                position: 1,
                comment: None,
            },
        );
        orders_cols.insert(
            "amount".to_string(),
            ColumnType {
                r#type: "numeric".to_string(),
                nullable: true,
                position: 2,
                comment: None,
            },
        );
        objects.insert(
            "materialize.public.orders".parse::<ObjectId>().unwrap(),
            orders_cols,
        );

        let mut summary_cols = BTreeMap::new();
        summary_cols.insert(
            "user_id".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: false,
                position: 0,
                comment: None,
            },
        );
        summary_cols.insert(
            "user_name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
                position: 1,
                comment: None,
            },
        );
        summary_cols.insert(
            "total_orders".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: true,
                position: 2,
                comment: None,
            },
        );
        objects.insert(
            "materialize.public.user_order_summary"
                .parse::<ObjectId>()
                .unwrap(),
            summary_cols,
        );

        Types {
            version: 1,
            tables: objects,
            kinds: BTreeMap::new(),
            comments: BTreeMap::new(),
        }
    }

    fn make_target_id() -> ObjectId {
        ObjectId::new(
            "materialize".to_string(),
            "public".to_string(),
            "user_order_summary".to_string(),
        )
    }

    fn make_dependencies() -> BTreeSet<ObjectId> {
        let mut deps = BTreeSet::new();
        deps.insert(ObjectId::new(
            "materialize".to_string(),
            "public".to_string(),
            "users".to_string(),
        ));
        deps.insert(ObjectId::new(
            "materialize".to_string(),
            "public".to_string(),
            "orders".to_string(),
        ));
        deps
    }

    #[test]
    fn test_validate_unit_test_passes_with_correct_mocks() {
        let test = UnitTest {
            name: "test_user_summary".to_string(),
            target_view: "materialize.public.user_order_summary".to_string(),
            at_time: None,
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

        let result = validate_unit_test(
            &test,
            &target_id,
            &|fqn| types.get_table(fqn).cloned(),
            &dependencies,
        );
        assert!(result.is_ok(), "Expected validation to pass: {:?}", result);
    }

    #[test]
    fn test_validate_unit_test_fails_with_unmocked_dependency() {
        let test = UnitTest {
            name: "test_user_summary".to_string(),
            target_view: "materialize.public.user_order_summary".to_string(),
            at_time: None,
            mocks: vec![MockView {
                fqn: "materialize.public.users".to_string(),
                columns: vec![
                    ("id".to_string(), "bigint".to_string()),
                    ("name".to_string(), "text".to_string()),
                    ("email".to_string(), "text".to_string()),
                ],
                query: "SELECT * FROM VALUES (1, 'alice', 'alice@example.com')".to_string(),
            }],
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

        let result = validate_unit_test(
            &test,
            &target_id,
            &|fqn| types.get_table(fqn).cloned(),
            &dependencies,
        );
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
            at_time: None,
            mocks: vec![
                MockView {
                    fqn: "materialize.public.users".to_string(),
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("name".to_string(), "text".to_string()),
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

        let result = validate_unit_test(
            &test,
            &target_id,
            &|fqn| types.get_table(fqn).cloned(),
            &dependencies,
        );
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
            at_time: None,
            mocks: vec![
                MockView {
                    fqn: "materialize.public.users".to_string(),
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("name".to_string(), "text".to_string()),
                        ("email".to_string(), "text".to_string()),
                        ("extra_column".to_string(), "int".to_string()),
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

        let result = validate_unit_test(
            &test,
            &target_id,
            &|fqn| types.get_table(fqn).cloned(),
            &dependencies,
        );
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
            at_time: None,
            mocks: vec![
                MockView {
                    fqn: "materialize.public.users".to_string(),
                    columns: vec![
                        ("id".to_string(), "text".to_string()),
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

        let result = validate_unit_test(
            &test,
            &target_id,
            &|fqn| types.get_table(fqn).cloned(),
            &dependencies,
        );
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
            at_time: None,
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
                    ("total_orders".to_string(), "bigint".to_string()),
                ],
                query: "SELECT * FROM VALUES (1, 1)".to_string(),
            },
        };

        let types = make_test_types();
        let target_id = make_target_id();
        let dependencies = make_dependencies();

        let result = validate_unit_test(
            &test,
            &target_id,
            &|fqn| types.get_table(fqn).cloned(),
            &dependencies,
        );
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
            at_time: None,
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
                    ("user_name".to_string(), "bigint".to_string()),
                    ("total_orders".to_string(), "bigint".to_string()),
                ],
                query: "SELECT * FROM VALUES (1, 1, 1)".to_string(),
            },
        };

        let types = make_test_types();
        let target_id = make_target_id();
        let dependencies = make_dependencies();

        let result = validate_unit_test(
            &test,
            &target_id,
            &|fqn| types.get_table(fqn).cloned(),
            &dependencies,
        );
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

    #[test]
    fn test_normalize_fqn_unqualified() {
        let target_id = ObjectId::new(
            "mydb".to_string(),
            "myschema".to_string(),
            "myview".to_string(),
        );

        let normalized = normalize_fqn("users", &target_id);
        assert_eq!(normalized.to_string(), "mydb.myschema.users");
    }

    #[test]
    fn test_normalize_fqn_schema_qualified() {
        let target_id = ObjectId::new(
            "mydb".to_string(),
            "myschema".to_string(),
            "myview".to_string(),
        );

        let normalized = normalize_fqn("other_schema.users", &target_id);
        assert_eq!(normalized.to_string(), "mydb.other_schema.users");
    }

    #[test]
    fn test_normalize_fqn_fully_qualified() {
        let target_id = ObjectId::new(
            "mydb".to_string(),
            "myschema".to_string(),
            "myview".to_string(),
        );

        let normalized = normalize_fqn("other_db.other_schema.users", &target_id);
        assert_eq!(normalized.to_string(), "other_db.other_schema.users");
    }

    #[test]
    fn test_normalize_type_integer_aliases() {
        assert_eq!(normalize_type("INT"), "integer");
        assert_eq!(normalize_type("int4"), "integer");
        assert_eq!(normalize_type("integer"), "integer");
        assert_eq!(normalize_type("INTEGER"), "integer");
    }

    #[test]
    fn test_normalize_type_bigint_aliases() {
        assert_eq!(normalize_type("INT8"), "bigint");
        assert_eq!(normalize_type("bigint"), "bigint");
        assert_eq!(normalize_type("BIGINT"), "bigint");
    }

    #[test]
    fn test_normalize_type_smallint_aliases() {
        assert_eq!(normalize_type("INT2"), "smallint");
        assert_eq!(normalize_type("smallint"), "smallint");
        assert_eq!(normalize_type("SMALLINT"), "smallint");
    }

    #[test]
    fn test_normalize_type_real_aliases() {
        assert_eq!(normalize_type("float4"), "real");
        assert_eq!(normalize_type("FLOAT4"), "real");
        assert_eq!(normalize_type("real"), "real");
        assert_eq!(normalize_type("REAL"), "real");
    }

    #[test]
    fn test_normalize_type_double_precision_aliases() {
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
        assert_eq!(normalize_type("bool"), "boolean");
        assert_eq!(normalize_type("boolean"), "boolean");
        assert_eq!(normalize_type("BOOL"), "boolean");
        assert_eq!(normalize_type("BOOLEAN"), "boolean");
    }

    #[test]
    fn test_normalize_type_text_aliases() {
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
        assert_eq!(normalize_type("numeric"), "numeric");
        assert_eq!(normalize_type("NUMERIC"), "numeric");
        assert_eq!(normalize_type("decimal"), "numeric");
        assert_eq!(normalize_type("DECIMAL"), "numeric");
        assert_eq!(normalize_type("numeric(10,2)"), "numeric");
        assert_eq!(normalize_type("decimal(18,4)"), "numeric");
    }

    #[test]
    fn test_normalize_type_jsonb_aliases() {
        assert_eq!(normalize_type("json"), "jsonb");
        assert_eq!(normalize_type("JSON"), "jsonb");
        assert_eq!(normalize_type("jsonb"), "jsonb");
        assert_eq!(normalize_type("JSONB"), "jsonb");
    }

    #[test]
    fn test_normalize_type_timestamptz_aliases() {
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
        assert_eq!(normalize_type("timestamp"), "timestamp without time zone");
        assert_eq!(normalize_type("TIMESTAMP"), "timestamp without time zone");
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
        assert_eq!(normalize_type("integer"), normalize_type("INTEGER"));
        assert_eq!(normalize_type("integer"), normalize_type("Integer"));
        assert_eq!(normalize_type("integer"), normalize_type("iNtEgEr"));
        assert_eq!(normalize_type("int"), normalize_type("INT"));
        assert_eq!(normalize_type("int"), normalize_type("Int"));

        assert_eq!(normalize_type("bigint"), normalize_type("BIGINT"));
        assert_eq!(normalize_type("bigint"), normalize_type("BigInt"));
        assert_eq!(normalize_type("int8"), normalize_type("INT8"));

        assert_eq!(normalize_type("text"), normalize_type("TEXT"));
        assert_eq!(normalize_type("text"), normalize_type("Text"));
        assert_eq!(normalize_type("string"), normalize_type("STRING"));
        assert_eq!(normalize_type("string"), normalize_type("String"));

        assert_eq!(normalize_type("boolean"), normalize_type("BOOLEAN"));
        assert_eq!(normalize_type("boolean"), normalize_type("Boolean"));
        assert_eq!(normalize_type("bool"), normalize_type("BOOL"));
        assert_eq!(normalize_type("bool"), normalize_type("Bool"));

        assert_eq!(normalize_type("numeric"), normalize_type("NUMERIC"));
        assert_eq!(normalize_type("numeric"), normalize_type("Numeric"));
        assert_eq!(normalize_type("decimal"), normalize_type("DECIMAL"));

        assert_eq!(
            normalize_type("double precision"),
            normalize_type("DOUBLE PRECISION")
        );
        assert_eq!(
            normalize_type("double precision"),
            normalize_type("Double Precision")
        );

        assert_eq!(
            normalize_type("timestamp with time zone"),
            normalize_type("TIMESTAMP WITH TIME ZONE")
        );
        assert_eq!(normalize_type("timestamptz"), normalize_type("TIMESTAMPTZ"));
        assert_eq!(normalize_type("timestamptz"), normalize_type("TimestampTZ"));

        assert_eq!(normalize_type("jsonb"), normalize_type("JSONB"));
        assert_eq!(normalize_type("jsonb"), normalize_type("JsonB"));
        assert_eq!(normalize_type("json"), normalize_type("JSON"));
    }

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
                position: 0,
                comment: None,
            },
        );
        actual_columns.insert(
            "name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
                position: 0,
                comment: None,
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
            ("id".to_string(), "INT".to_string()),
            ("count".to_string(), "INT8".to_string()),
        ];

        let mut actual_columns = BTreeMap::new();
        actual_columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: false,
                position: 0,
                comment: None,
            },
        );
        actual_columns.insert(
            "count".to_string(),
            ColumnType {
                r#type: "bigint".to_string(),
                nullable: false,
                position: 0,
                comment: None,
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
                position: 0,
                comment: None,
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
                position: 0,
                comment: None,
            },
        );
        actual_columns.insert(
            "name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
                position: 0,
                comment: None,
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
                position: 0,
                comment: None,
            },
        );

        let (_, _, type_mismatches) = compare_columns(&test_columns, &actual_columns);
        assert_eq!(type_mismatches.len(), 1);
        assert_eq!(type_mismatches[0].0, "id");
        assert_eq!(type_mismatches[0].1, "text");
        assert_eq!(type_mismatches[0].2, "bigint");
    }

    #[test]
    fn test_validate_with_unqualified_mock_name() {
        let test = UnitTest {
            name: "test_partial_fqn".to_string(),
            target_view: "materialize.public.user_order_summary".to_string(),
            at_time: None,
            mocks: vec![
                MockView {
                    fqn: "users".to_string(),
                    columns: vec![
                        ("id".to_string(), "bigint".to_string()),
                        ("name".to_string(), "text".to_string()),
                        ("email".to_string(), "text".to_string()),
                    ],
                    query: "SELECT * FROM VALUES (1, 'alice', 'alice@example.com')".to_string(),
                },
                MockView {
                    fqn: "public.orders".to_string(),
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

        let result = validate_unit_test(
            &test,
            &target_id,
            &|fqn| types.get_table(fqn).cloned(),
            &dependencies,
        );
        assert!(result.is_ok(), "Expected validation to pass: {:?}", result);
    }

    #[test]
    fn test_validate_passes_with_no_dependencies() {
        let test = UnitTest {
            name: "test_no_deps".to_string(),
            target_view: "materialize.public.my_view".to_string(),
            at_time: None,
            mocks: vec![],
            expected: ExpectedResult {
                columns: vec![("result".to_string(), "integer".to_string())],
                query: "SELECT * FROM VALUES (42)".to_string(),
            },
        };

        let types = Types::default();
        let target_id = ObjectId::new(
            "materialize".to_string(),
            "public".to_string(),
            "my_view".to_string(),
        );
        let dependencies = BTreeSet::new();

        let result = validate_unit_test(
            &test,
            &target_id,
            &|fqn| types.get_table(fqn).cloned(),
            &dependencies,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_skips_unknown_mock() {
        let test = UnitTest {
            name: "test_unknown_mock".to_string(),
            target_view: "materialize.public.my_view".to_string(),
            at_time: None,
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

        let types = Types::default();
        let target_id = ObjectId::new(
            "materialize".to_string(),
            "public".to_string(),
            "my_view".to_string(),
        );

        let mut dependencies = BTreeSet::new();
        dependencies.insert(ObjectId::new(
            "materialize".to_string(),
            "public".to_string(),
            "unknown_table".to_string(),
        ));

        let result = validate_unit_test(
            &test,
            &target_id,
            &|fqn| types.get_table(fqn).cloned(),
            &dependencies,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_normalize_type_list() {
        assert_eq!(normalize_type("int8 list"), "bigint list");
        assert_eq!(normalize_type("INT LIST"), "integer list");
        assert_eq!(normalize_type("text list"), "text list");
        assert_eq!(normalize_type("INT8 LIST"), "bigint list");
    }

    #[test]
    fn test_normalize_type_array() {
        assert_eq!(normalize_type("int8[]"), "bigint[]");
        assert_eq!(normalize_type("INT[]"), "integer[]");
        assert_eq!(normalize_type("text[]"), "text[]");
    }

    #[test]
    fn test_normalize_type_map() {
        assert_eq!(normalize_type("map[text=>int8]"), "map[text=>bigint]");
        assert_eq!(normalize_type("map[STRING=>BOOL]"), "map[text=>boolean]");
    }

    #[test]
    fn test_normalize_type_bare_list() {
        assert_eq!(normalize_type("list"), "list");
        assert_eq!(normalize_type("LIST"), "list");
    }

    #[test]
    fn test_compare_columns_list_matches_bare() {
        let test_columns = vec![("ids".to_string(), "int8 list".to_string())];

        let mut actual_columns = BTreeMap::new();
        actual_columns.insert(
            "ids".to_string(),
            ColumnType {
                r#type: "list".to_string(),
                nullable: true,
                position: 0,
                comment: None,
            },
        );

        let (extra, missing, type_mismatches) = compare_columns(&test_columns, &actual_columns);
        assert!(extra.is_empty());
        assert!(missing.is_empty());
        assert!(
            type_mismatches.is_empty(),
            "Expected no type mismatches for 'int8 list' vs bare 'list', got: {:?}",
            type_mismatches
        );
    }

    #[test]
    fn test_compare_columns_map_matches_bare() {
        let test_columns = vec![("data".to_string(), "map[text=>int8]".to_string())];

        let mut actual_columns = BTreeMap::new();
        actual_columns.insert(
            "data".to_string(),
            ColumnType {
                r#type: "map".to_string(),
                nullable: true,
                position: 0,
                comment: None,
            },
        );

        let (_, _, type_mismatches) = compare_columns(&test_columns, &actual_columns);
        assert!(
            type_mismatches.is_empty(),
            "Expected no type mismatches for 'map[text=>int8]' vs bare 'map', got: {:?}",
            type_mismatches
        );
    }
}
