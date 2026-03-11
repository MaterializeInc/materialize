//! Test command - run unit tests against the database.

use crate::cli::CliError;
use crate::cli::progress;
use crate::client::Client;
use crate::config::Settings;
use crate::project::{self, typed};
use crate::types::docker_runtime::DockerRuntime;
use crate::types::{self, TypeCheckError, Types};
use crate::unit_test;
use crate::{info, info_nonl};
use mz_sql_parser::ast::Ident;
use owo_colors::OwoColorize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::fs::File;
use std::path::Path;
use std::time::Instant;

/// Filter to select a subset of tests to run.
///
/// Supports patterns like:
/// - `database.*` — all tests in a database
/// - `database.schema.*` — all tests in a schema
/// - `database.schema.object` — all tests for an object
/// - `database.schema.object#test_name` — a single named test
struct TestFilter {
    database: Option<String>,
    schema: Option<String>,
    object: Option<String>,
    test_name: Option<String>,
}

impl TestFilter {
    fn parse(filter: &str) -> Self {
        let (object_part, test_name) = match filter.split_once('#') {
            Some((left, name)) => (left, Some(name.to_string())),
            None => (filter, None),
        };

        let segments: Vec<&str> = object_part.split('.').collect();
        let (database, schema, object) = match segments.as_slice() {
            [db, schema, obj] => {
                let db = if *db == "*" {
                    None
                } else {
                    Some(db.to_string())
                };
                let schema = if *schema == "*" {
                    None
                } else {
                    Some(schema.to_string())
                };
                let obj = if *obj == "*" {
                    None
                } else {
                    Some(obj.to_string())
                };
                (db, schema, obj)
            }
            [db, schema_or_star] => {
                let db = if *db == "*" {
                    None
                } else {
                    Some(db.to_string())
                };
                let schema = if *schema_or_star == "*" {
                    None
                } else {
                    Some(schema_or_star.to_string())
                };
                (db, schema, None)
            }
            [db_or_star] => {
                let db = if *db_or_star == "*" {
                    None
                } else {
                    Some(db_or_star.to_string())
                };
                (db, None, None)
            }
            _ => (None, None, None),
        };

        TestFilter {
            database,
            schema,
            object,
            test_name,
        }
    }

    fn matches(
        &self,
        object_id: &project::object_id::ObjectId,
        test: &unit_test::UnitTest,
    ) -> bool {
        if let Some(ref db) = self.database {
            if db != &object_id.database {
                return false;
            }
        }
        if let Some(ref schema) = self.schema {
            if schema != &object_id.schema {
                return false;
            }
        }
        if let Some(ref obj) = self.object {
            if obj != &object_id.object {
                return false;
            }
        }
        if let Some(ref name) = self.test_name {
            if name != &test.name {
                return false;
            }
        }
        true
    }
}

/// Final classification for a single test invocation.
///
/// `ValidationFailed` is tracked separately from execution failures so summary output
/// can distinguish broken test definitions from assertion/runtime failures.
enum TestOutcome {
    Passed,
    Failed(ExecutionFailure),
    ValidationFailed(ValidationFailure),
}

enum ValidationFailure {
    UnitTest(unit_test::TestValidationError),
    AtTime(unit_test::InvalidAtTimeError),
}

enum ExecutionFailure {
    /// Setup or query execution error.
    Error(String),
    /// Test assertion mismatch with pre-formatted display output.
    AssertionFailed(String),
}

impl TestOutcome {
    fn to_test_case(
        &self,
        name: &str,
        object_id: &project::object_id::ObjectId,
        elapsed: time::Duration,
    ) -> junit_report::TestCase {
        let mut test_case = match self {
            TestOutcome::Passed => junit_report::TestCase::success(name, elapsed),
            TestOutcome::Failed(failure) => {
                let msg = match failure {
                    ExecutionFailure::Error(msg) => msg.replace('\n', "&#10;"),
                    ExecutionFailure::AssertionFailed(_) => "test assertion failed".to_string(),
                };
                junit_report::TestCase::failure(name, elapsed, "failure", &msg)
            }
            TestOutcome::ValidationFailed(failure) => {
                let msg = match failure {
                    ValidationFailure::UnitTest(e) => e.to_string().replace('\n', "&#10;"),
                    ValidationFailure::AtTime(e) => e.to_string().replace('\n', "&#10;"),
                };
                junit_report::TestCase::failure(name, elapsed, "validation", &msg)
            }
        };
        test_case.set_classname(&format!(
            "{}.{}.{}",
            object_id.database, object_id.schema, object_id.object
        ));
        test_case.set_filepath(&format!(
            "models/{}/{}/{}.sql",
            object_id.database, object_id.schema, object_id.object
        ));
        test_case
    }
}

/// Run unit tests against the database.
///
/// This command:
/// - Loads the project from the filesystem
/// - Connects to the database
/// - Finds all test files in the `test/` directory
/// - Parses test files (`.mztest` format)
/// - For each test:
///   - Locates the target view in the project
///   - Desugars the test into SQL statements
///   - Executes setup statements (CREATE TEMP TABLE, etc.)
///   - Runs the test query (a query that returns rows only on failure)
///   - Reports pass/fail with detailed output
/// - Cleans up after each test with DISCARD ALL
///
/// # Test file format
///
/// Tests use a custom format:
/// ```text
/// # test_name
/// target_view
///
/// field1 field2 field3
/// ------
/// value1 value2 value3
/// value4 value5 value6
/// ```
///
/// The test passes if the query returns no rows. Rows are returned when:
/// - Expected rows are MISSING from the actual results
/// - Unexpected rows appear in actual results
///
/// # Arguments
/// * `directory` - Project root directory
///
/// # Returns
/// Ok(()) if all tests pass
///
/// # Errors
/// Returns `CliError::Project` if project loading fails
/// Returns `CliError::Connection` if database connection fails
/// Returns error if tests fail (exits with code 1)
pub async fn run(
    settings: &Settings,
    filter: Option<&str>,
    junit_xml: Option<&Path>,
) -> Result<(), CliError> {
    let directory = &settings.directory;
    let planned_project = project::plan(
        directory,
        &settings.profile_name,
        settings.suffix(),
        settings.cluster_suffix(),
        settings.variables(),
    )?;
    let empty_types = Types::default();
    let runtime = DockerRuntime::new().with_image(&settings.docker_image);
    let test_filter = filter.map(TestFilter::parse);

    if planned_project.tests.is_empty() {
        progress::info(&format!("No tests found in {}", directory.display()));
        return Ok(());
    }

    let mut combined_types = types::load_types_lock(directory).unwrap_or_default();
    let internal_types =
        load_or_generate_types_cache(directory, &planned_project, &runtime).await?;
    combined_types.merge(&internal_types);

    info!(
        "{}\n",
        format!("Running tests from {}:", directory.display()).bold()
    );

    let mut junit_suites: BTreeMap<project::object_id::ObjectId, junit_report::TestSuite> =
        BTreeMap::new();
    let (mut passed_tests, mut failed_tests, mut validation_failed) = (0, 0, 0);
    for (object_id, test) in &planned_project.tests {
        if let Some(ref f) = test_filter {
            if !f.matches(object_id, test) {
                continue;
            }
        }
        let start_time = Instant::now();
        let outcome = run_single_test(
            &planned_project,
            object_id,
            test,
            &combined_types,
            &runtime,
            &empty_types,
        )
        .await?;

        let elapsed =
            time::Duration::try_from(start_time.elapsed()).unwrap_or(time::Duration::ZERO);

        print_test_outcome(&test.name, &outcome);
        junit_suites
            .entry(object_id.clone())
            .or_insert_with(|| junit_report::TestSuite::new(&object_id.to_string()))
            .add_testcase(outcome.to_test_case(&test.name, object_id, elapsed));

        match &outcome {
            TestOutcome::Passed => passed_tests += 1,
            TestOutcome::Failed(_) => failed_tests += 1,
            TestOutcome::ValidationFailed(_) => validation_failed += 1,
        }
    }

    let total_run = passed_tests + failed_tests + validation_failed;
    if total_run == 0 {
        if let Some(f) = filter {
            return Err(CliError::TestsFilterMissed { filter: f.into() });
        }
        return Ok(());
    }

    if let Some(path) = junit_xml {
        let report = junit_report::ReportBuilder::new()
            .add_testsuites(junit_suites.into_values())
            .build();
        let mut file = File::create(path)
            .map_err(|e| CliError::Message(format!("failed to create JUnit XML file: {}", e)))?;
        report
            .write_xml(&mut file)
            .map_err(|e| CliError::Message(format!("failed to write JUnit XML report: {}", e)))?;
    }

    print_summary(passed_tests, failed_tests, validation_failed);

    let total_failed = failed_tests + validation_failed;
    if total_failed > 0 {
        return Err(CliError::TestsFailed {
            failed: total_failed,
            passed: passed_tests,
        });
    }

    Ok(())
}

/// Executes one test case through validation, setup SQL, assertion query, and cleanup.
///
/// Returns a `TestOutcome` without performing any terminal output so the caller
/// can own all presentation (printing, JUnit building, counting).
async fn run_single_test(
    planned_project: &project::planned::Project,
    object_id: &project::object_id::ObjectId,
    test: &unit_test::UnitTest,
    combined_types: &Types,
    runtime: &DockerRuntime,
    empty_types: &Types,
) -> Result<TestOutcome, CliError> {
    let dependencies = planned_project
        .dependency_graph
        .get(object_id)
        .cloned()
        .unwrap_or_else(BTreeSet::new);

    if let Err(e) = unit_test::validate_unit_test(test, object_id, combined_types, &dependencies) {
        return Ok(TestOutcome::ValidationFailed(ValidationFailure::UnitTest(
            e,
        )));
    }

    let client = runtime_client(runtime, empty_types).await?;
    if let Err(e) = validate_at_time(&client, test).await? {
        return Ok(TestOutcome::ValidationFailed(ValidationFailure::AtTime(e)));
    }

    let Some(target_obj) = planned_project.find_object(object_id) else {
        return Ok(TestOutcome::Failed(ExecutionFailure::Error(format!(
            "target object '{}' not found in project",
            object_id
        ))));
    };

    let typed_fqn = typed_fqn_from_object_id(object_id);
    let sql_statements =
        unit_test::desugar_unit_test(test, &target_obj.typed_object.stmt, &typed_fqn);

    for sql in &sql_statements[..sql_statements.len() - 1] {
        if let Err(e) = client.batch_execute(sql).await {
            return Ok(TestOutcome::Failed(ExecutionFailure::Error(format!(
                "failed to execute SQL: {:?}\n  statement: {}",
                e, sql
            ))));
        }
    }

    let test_query = &sql_statements[sql_statements.len() - 1];
    let outcome = match client.simple_query(test_query).await {
        Ok(messages) => {
            let rows: Vec<_> = messages
                .into_iter()
                .filter_map(|m| match m {
                    tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                    _ => None,
                })
                .collect();
            if rows.is_empty() {
                TestOutcome::Passed
            } else {
                TestOutcome::Failed(ExecutionFailure::AssertionFailed(format_assertion_rows(
                    &rows,
                )))
            }
        }
        Err(e) => TestOutcome::Failed(ExecutionFailure::Error(format!(
            "failed to execute test query: {}",
            e
        ))),
    };

    if let Err(e) = client.batch_execute("DISCARD ALL").await {
        eprintln!("warning: failed to execute DISCARD ALL: {}", e);
    }
    Ok(outcome)
}

/// Prints the test summary line showing pass/fail counts.
fn print_summary(passed: usize, failed: usize, validation_failed: usize) {
    let total_failed = failed + validation_failed;
    info_nonl!("\n{}: ", "test result".bold());
    if total_failed == 0 {
        info_nonl!("{}. ", "ok".green().bold());
    } else {
        info_nonl!("{}. ", "FAILED".red().bold());
    }
    info_nonl!("{}; ", format!("{} passed", passed).green());
    if failed > 0 {
        info_nonl!("{}; ", format!("{} failed", failed).red());
    } else {
        info_nonl!("{} failed; ", failed);
    }
    if validation_failed > 0 {
        info!(
            "{}",
            format!("{} validation errors", validation_failed).red()
        );
    } else {
        info!("{} validation errors", validation_failed);
    }
}

/// Prints the complete status line and any detail output for a single test outcome.
fn print_test_outcome(name: &str, outcome: &TestOutcome) {
    match outcome {
        TestOutcome::Passed => {
            info!(
                "{} {} ... {}",
                "test".cyan(),
                name.cyan(),
                "ok".green().bold()
            );
        }
        TestOutcome::ValidationFailed(failure) => {
            info!(
                "{} {} ... {}",
                "test".cyan(),
                name.cyan(),
                "VALIDATION FAILED".red().bold()
            );
            match failure {
                ValidationFailure::UnitTest(e) => print_test_validation_error(e),
                ValidationFailure::AtTime(e) => eprintln!("{}", e),
            }
        }
        TestOutcome::Failed(failure) => {
            info!(
                "{} {} ... {}",
                "test".cyan(),
                name.cyan(),
                "FAILED".red().bold()
            );
            match failure {
                ExecutionFailure::AssertionFailed(display) => eprint!("{}", display),
                ExecutionFailure::Error(msg) => {
                    eprintln!("  {}: {}", "error".red().bold(), msg)
                }
            }
        }
    }
}

/// Renders validation failures with the same detail shape as legacy test output.
fn print_test_validation_error(error: &unit_test::TestValidationError) {
    match error {
        unit_test::TestValidationError::UnmockedDependency(inner) => eprintln!("{}", inner),
        unit_test::TestValidationError::MockSchemaMismatch(inner) => eprintln!("{}", inner),
        unit_test::TestValidationError::ExpectedSchemaMismatch(inner) => eprintln!("{}", inner),
        unit_test::TestValidationError::InvalidAtTime(inner) => eprintln!("{}", inner),
        unit_test::TestValidationError::TypesCacheUnavailable { reason } => {
            eprintln!(
                "{}: types cache unavailable: {}",
                "error".bright_red().bold(),
                reason
            );
        }
    }
}

/// Creates an isolated runtime client for test execution.
///
/// Converts runtime startup failures into user-facing CLI messages with actionable wording.
async fn runtime_client(runtime: &DockerRuntime, empty_types: &Types) -> Result<Client, CliError> {
    match runtime.get_client(empty_types).await {
        Ok(client) => Ok(client),
        Err(TypeCheckError::ContainerStartFailed(e)) => Err(CliError::Message(format!(
            "Docker not available for running tests: {}",
            e
        ))),
        Err(e) => Err(CliError::Message(format!(
            "Failed to start test environment: {}",
            e
        ))),
    }
}

/// Pre-validates optional `AT TIME` test expressions against `mz_timestamp` casting.
///
/// Returns `Ok(Ok(()))` when valid, `Ok(Err(message))` on validation failure,
/// and `Err(CliError)` on connection errors.
async fn validate_at_time(
    client: &Client,
    test: &unit_test::UnitTest,
) -> Result<Result<(), unit_test::InvalidAtTimeError>, CliError> {
    if let Some(at_time) = &test.at_time {
        let validation_query = format!("SELECT {}::mz_timestamp", at_time);
        if let Err(e) = client.simple_query(&validation_query).await {
            let error = unit_test::InvalidAtTimeError {
                test_name: test.name.clone(),
                at_time_value: at_time.clone(),
                db_error: e.to_string(),
            };
            return Ok(Err(error));
        }
    }
    Ok(Ok(()))
}

/// Converts planned object identity into the AST form expected by unit test desugaring.
fn typed_fqn_from_object_id(object_id: &project::object_id::ObjectId) -> typed::FullyQualifiedName {
    typed::FullyQualifiedName::from(mz_sql_parser::ast::UnresolvedItemName(vec![
        Ident::new(&object_id.database)
            .expect("database name from parsed SQL should be valid identifier"),
        Ident::new(&object_id.schema)
            .expect("schema name from parsed SQL should be valid identifier"),
        Ident::new(&object_id.object)
            .expect("object name from parsed SQL should be valid identifier"),
    ]))
}

/// Formats failing assertion rows into a readable table-like string.
///
/// This is intentionally display-only and does not affect pass/fail decisions.
fn format_assertion_rows(rows: &[tokio_postgres::SimpleQueryRow]) -> String {
    let mut out = String::new();
    writeln!(out, "  {}:", "Test assertion failed".yellow().bold()).unwrap();
    if let Some(first_row) = rows.first() {
        let columns: Vec<String> = first_row
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();
        let header = columns.join(" | ");
        writeln!(out, "  {}", header.bold().cyan()).unwrap();
        writeln!(out, "  {}", "-".repeat(header.len()).cyan()).unwrap();
    }

    for row in rows {
        let mut values = Vec::new();
        for i in 0..row.columns().len() {
            let value_str = row.get(i).unwrap_or("<null>").to_string();

            if i == 0 {
                let colored = match value_str.as_str() {
                    "MISSING" => value_str.red().bold().to_string(),
                    "UNEXPECTED" => value_str.yellow().bold().to_string(),
                    _ => value_str,
                };
                values.push(colored);
            } else {
                values.push(value_str);
            }
        }
        writeln!(out, "  {}", values.join(" | ")).unwrap();
    }
    out
}

/// Load types.cache or generate it by running type checking if stale/missing.
async fn load_or_generate_types_cache(
    directory: &Path,
    planned_project: &project::planned::Project,
    runtime: &DockerRuntime,
) -> Result<Types, CliError> {
    // Check if types.cache exists and is up-to-date
    if !types::is_types_cache_stale(directory) {
        if let Ok(cached) = types::load_types_cache(directory) {
            info!(
                "{}",
                "Using cached types from .mz-deploy/types.cache".dimmed()
            );
            return Ok(cached);
        }
    }

    // Types cache is stale or missing - regenerate by type checking
    info!(
        "{}",
        "Types cache stale or missing, running type check...".yellow()
    );

    // Load types.lock for external dependencies (used in Docker runtime)
    let external_types = types::load_types_lock(directory).unwrap_or_default();

    // Get a client from the Docker runtime with external dependencies staged
    let mut client = match runtime.get_client(&external_types).await {
        Ok(client) => client,
        Err(TypeCheckError::ContainerStartFailed(e)) => {
            // Docker not available - warn and return empty types
            // Tests will still run but validation will be limited
            eprintln!(
                "{}: Docker not available for type checking: {}",
                "warning".yellow().bold(),
                e
            );
            eprintln!(
                "{}",
                "Test validation will be limited without types.cache".yellow()
            );
            return Ok(Types::default());
        }
        Err(e) => {
            return Err(CliError::Message(format!(
                "Failed to start type check environment: {}",
                e
            )));
        }
    };

    // Run type checking (this will also generate types.cache)
    match types::typecheck_with_client(&mut client, planned_project, directory).await {
        Ok(()) => {
            // Type checking succeeded and wrote types.cache - load it
            types::load_types_cache(directory).map_err(|e| {
                CliError::Message(format!("Failed to load generated types.cache: {}", e))
            })
        }
        Err(e) => {
            // Type check failed - return error
            Err(CliError::TypeCheckFailed(e))
        }
    }
}
