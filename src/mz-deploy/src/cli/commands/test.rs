//! Test command - run unit tests against the database.

use crate::cli::CliError;
use crate::cli::progress;
use crate::client::Client;
use crate::config::Settings;
use crate::project::{self, typed};
use crate::types::docker_runtime::DockerRuntime;
use crate::types::{self, TypeCheckError, Types};
use crate::unit_test;
use mz_sql_parser::ast::Ident;
use owo_colors::OwoColorize;
use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;
use std::time::Instant;

/// Final classification for a single test invocation.
///
/// `ValidationFailed` is tracked separately from execution failures so summary output
/// can distinguish broken test definitions from assertion/runtime failures.
enum TestOutcome {
    Passed,
    Failed,
    ValidationFailed,
}

/// Result of running a single test, combining the outcome with an optional error message
/// for JUnit reporting.
struct TestResult {
    outcome: TestOutcome,
    message: Option<String>,
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
pub async fn run(settings: &Settings, junit_xml: Option<&Path>) -> Result<(), CliError> {
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
    if planned_project.tests.is_empty() {
        progress::info(&format!("No tests found in {}", directory.display()));
        return Ok(());
    }

    let mut combined_types = types::load_types_lock(directory).unwrap_or_default();
    let internal_types =
        load_or_generate_types_cache(directory, &planned_project, &runtime).await?;
    combined_types.merge(&internal_types);

    println!(
        "{}\n",
        format!("Running tests from {}:", directory.display()).bold()
    );

    let mut junit_suite = junit_xml.map(|_| junit_report::TestSuite::new("mz-deploy"));
    let (mut passed_tests, mut failed_tests, mut validation_failed) = (0, 0, 0);
    for (object_id, test) in &planned_project.tests {
        let start_time = Instant::now();
        let result = run_single_test(
            &planned_project,
            object_id,
            test,
            &combined_types,
            &runtime,
            &empty_types,
        )
        .await?;

        if let Some(junit_suite) = &mut junit_suite {
            let elapsed =
                time::Duration::try_from(start_time.elapsed()).unwrap_or(time::Duration::ZERO);
            let mut test_case = match &result.outcome {
                TestOutcome::Passed => junit_report::TestCase::success(&test.name, elapsed),
                TestOutcome::Failed => junit_report::TestCase::failure(
                    &test.name,
                    elapsed,
                    "failure",
                    &result
                        .message
                        .as_deref()
                        .unwrap_or("test failed")
                        .replace('\n', "&#10;"),
                ),
                TestOutcome::ValidationFailed => junit_report::TestCase::failure(
                    &test.name,
                    elapsed,
                    "validation",
                    &result
                        .message
                        .as_deref()
                        .unwrap_or("validation failed")
                        .replace('\n', "&#10;"),
                ),
            };
            test_case.set_classname("mz-deploy");
            junit_suite.add_testcase(test_case);
        }

        match result.outcome {
            TestOutcome::Passed => passed_tests += 1,
            TestOutcome::Failed => failed_tests += 1,
            TestOutcome::ValidationFailed => validation_failed += 1,
        }
    }

    if let Some(junit_suite) = junit_suite {
        let path = junit_xml.expect("junit_suite is Some only when junit_xml is Some");
        let report = junit_report::ReportBuilder::new()
            .add_testsuite(junit_suite)
            .build();
        let mut file = File::create(path)
            .map_err(|e| CliError::Message(format!("failed to create JUnit XML file: {}", e)))?;
        report
            .write_xml(&mut file)
            .map_err(|e| CliError::Message(format!("failed to write JUnit XML report: {}", e)))?;
    }

    let total_failed = failed_tests + validation_failed;
    print!("\n{}: ", "test result".bold());
    if total_failed == 0 {
        print!("{}. ", "ok".green().bold());
    } else {
        print!("{}. ", "FAILED".red().bold());
    }
    print!("{}; ", format!("{} passed", passed_tests).green());
    if failed_tests > 0 {
        print!("{}; ", format!("{} failed", failed_tests).red());
    } else {
        print!("{} failed; ", failed_tests);
    }
    if validation_failed > 0 {
        println!(
            "{}",
            format!("{} validation errors", validation_failed).red()
        );
    } else {
        println!("{} validation errors", validation_failed);
    }

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
/// Returns a `TestResult` instead of mutating counters so top-level `run` remains
/// a simple coordinator over independent per-test executions.
async fn run_single_test(
    planned_project: &project::planned::Project,
    object_id: &project::object_id::ObjectId,
    test: &unit_test::UnitTest,
    combined_types: &Types,
    runtime: &DockerRuntime,
    empty_types: &Types,
) -> Result<TestResult, CliError> {
    let dependencies = planned_project
        .dependency_graph
        .get(object_id)
        .cloned()
        .unwrap_or_else(BTreeSet::new);

    if let Err(e) = unit_test::validate_unit_test(test, object_id, combined_types, &dependencies) {
        println!(
            "{} {} ... {}",
            "test".cyan(),
            test.name.cyan(),
            "VALIDATION FAILED".red().bold()
        );
        print_test_validation_error(&e);
        return Ok(TestResult {
            outcome: TestOutcome::ValidationFailed,
            message: Some(e.to_string()),
        });
    }

    let client = runtime_client(runtime, empty_types).await?;
    if let Some(msg) = validate_test_at_time_message(&client, test).await? {
        return Ok(TestResult {
            outcome: TestOutcome::ValidationFailed,
            message: Some(msg),
        });
    }

    print!("{} {} ... ", "test".cyan(), test.name.cyan());

    let Some(target_obj) = planned_project.find_object(object_id) else {
        println!("{}", "FAILED".red().bold());
        let msg = format!("target object '{}' not found in project", object_id);
        eprintln!("  {}: {}", "error".red().bold(), msg);
        return Ok(TestResult {
            outcome: TestOutcome::Failed,
            message: Some(msg),
        });
    };

    let typed_fqn = typed_fqn_from_object_id(object_id);
    let sql_statements =
        unit_test::desugar_unit_test(test, &target_obj.typed_object.stmt, &typed_fqn);

    for sql in &sql_statements[..sql_statements.len() - 1] {
        if let Err(e) = client.execute(sql, &[]).await {
            println!("{}", "FAILED".red().bold());
            let msg = format!("failed to execute SQL: {:?}\n  statement: {}", e, sql);
            eprintln!("  {}: {}", "error".red().bold(), msg);
            return Ok(TestResult {
                outcome: TestOutcome::Failed,
                message: Some(msg),
            });
        }
    }

    let test_query = &sql_statements[sql_statements.len() - 1];
    let result = match client.query(test_query, &[]).await {
        Ok(rows) if rows.is_empty() => {
            println!("{}", "ok".green().bold());
            TestResult {
                outcome: TestOutcome::Passed,
                message: None,
            }
        }
        Ok(rows) => {
            println!("{}", "FAILED".red().bold());
            print_test_assertion_rows(&rows);
            TestResult {
                outcome: TestOutcome::Failed,
                message: Some("test assertion failed".to_string()),
            }
        }
        Err(e) => {
            println!("{}", "FAILED".red().bold());
            let msg = format!("failed to execute test query: {}", e);
            eprintln!("  {}: {}", "error".red().bold(), msg);
            TestResult {
                outcome: TestOutcome::Failed,
                message: Some(msg),
            }
        }
    };

    if let Err(e) = client.execute("DISCARD ALL", &[]).await {
        eprintln!("warning: failed to execute DISCARD ALL: {}", e);
    }
    Ok(result)
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
/// Returns `Some(error_message)` on validation failure, `None` on success.
async fn validate_test_at_time_message(
    client: &Client,
    test: &unit_test::UnitTest,
) -> Result<Option<String>, CliError> {
    if let Some(at_time) = &test.at_time {
        let validation_query = format!("SELECT {}::mz_timestamp", at_time);
        if let Err(e) = client.query(&validation_query, &[]).await {
            println!(
                "{} {} ... {}",
                "test".cyan(),
                test.name.cyan(),
                "VALIDATION FAILED".red().bold()
            );
            let error = unit_test::InvalidAtTimeError {
                test_name: test.name.clone(),
                at_time_value: at_time.clone(),
                db_error: e.to_string(),
            };
            eprintln!("{}", error);
            return Ok(Some(error.to_string()));
        }
    }
    Ok(None)
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

/// Prints failing assertion rows in a readable table-like format.
///
/// This is intentionally display-only and does not affect pass/fail decisions.
fn print_test_assertion_rows(rows: &[tokio_postgres::Row]) {
    eprintln!("  {}:", "Test assertion failed".yellow().bold());
    if let Some(first_row) = rows.first() {
        let columns: Vec<String> = first_row
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();
        let header = columns.join(" | ");
        eprintln!("  {}", header.bold().cyan());
        eprintln!("  {}", "-".repeat(header.len()).cyan());
    }

    for row in rows {
        let mut values = Vec::new();
        for i in 0..row.len() {
            let value_str = if let Ok(v) = row.try_get::<_, String>(i) {
                v
            } else if let Ok(v) = row.try_get::<_, i64>(i) {
                v.to_string()
            } else if let Ok(v) = row.try_get::<_, i32>(i) {
                v.to_string()
            } else if let Ok(v) = row.try_get::<_, f64>(i) {
                v.to_string()
            } else if let Ok(v) = row.try_get::<_, bool>(i) {
                v.to_string()
            } else {
                "<unprintable>".to_string()
            };

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
        eprintln!("  {}", values.join(" | "));
    }
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
            println!(
                "{}",
                "Using cached types from .mz-deploy/types.cache".dimmed()
            );
            return Ok(cached);
        }
    }

    // Types cache is stale or missing - regenerate by type checking
    println!(
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
