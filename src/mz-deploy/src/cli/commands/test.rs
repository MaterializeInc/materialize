// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Test command — run unit tests against the database.
//!
//! Executes `EXECUTE UNIT TEST` statements attached to project objects
//! against a Materialize instance spun up in Docker. The pipeline:
//!
//! 1. **Load** — Compile the project and optionally load/generate type metadata.
//! 2. **Filter** — Select tests matching the user's filter pattern (supports
//!    `database.schema.object#test_name` with wildcards at any level).
//! 3. **Connect** — Establish a database connection to the target environment.
//! 4. **Execute** — For each test: locate the target view, desugar the test
//!    into SQL, execute setup statements, run the assertion query, and classify
//!    the result.
//! 5. **Report** — Print pass/fail output and, when requested, produce a
//!    JUnit XML report.
//!
//! ## Outcome Classification
//!
//! - **Passed** — Assertion query returned zero rows (no mismatches).
//! - **Failed** — Assertion query returned rows (missing or unexpected data)
//!   or a runtime error occurred during execution.
//! - **ValidationFailed** — Test definition is invalid (bad target, malformed
//!   `AT TIME`, etc.). Tracked separately so summary output distinguishes
//!   broken definitions from runtime failures.

use crate::cli::CliError;
use crate::cli::progress;
use crate::client::Client;
use crate::config::Settings;
use crate::docker_runtime::{DockerRuntime, DockerRuntimeError};
use crate::project::compiler::cache::ProjectCache;
use crate::project::ir::compiled::FullyQualifiedName;
use crate::project::ir::graph::Project;
use crate::project::ir::object_id::ObjectId;
use crate::project::ir::unit_test;
use crate::project::{self};
use crate::types::{self, Types};

mod lower;

use crate::{info, info_nonl, verbose};
pub(crate) use lower::TestValidationError;
use owo_colors::{OwoColorize, Stream, Style};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::fs::File;
use std::path::Path;
use std::time::Instant;
use time::Duration;

/// Aggregate pass/fail counts for the test run.
#[derive(Serialize)]
struct TestSummary {
    passed: usize,
    failed: usize,
    validation_failed: usize,
}

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

    fn matches(&self, object_id: &ObjectId, test: &unit_test::UnitTest) -> bool {
        if let Some(ref db) = self.database {
            if Some(db.as_str()) != object_id.database() {
                return false;
            }
        }
        if let Some(ref schema) = self.schema {
            if schema != object_id.schema() {
                return false;
            }
        }
        if let Some(ref obj) = self.object {
            if obj != object_id.object() {
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
#[derive(Serialize)]
enum TestOutcome {
    Passed,
    Failed(ExecutionFailure),
    ValidationFailed(ValidationFailure),
}

/// Pre-execution validation failure in a test definition.
#[derive(Serialize)]
enum ValidationFailure {
    UnitTest(TestValidationError),
    AtTime(lower::InvalidAtTimeError),
}

#[derive(Serialize)]
enum ExecutionFailure {
    /// Setup or query execution error.
    Error(String),
    /// Test assertion mismatch with raw row data for structured reporting.
    /// Display output is formatted on demand from these fields.
    AssertionFailed {
        columns: Vec<String>,
        missing: Vec<BTreeMap<String, String>>,
        unexpected: Vec<BTreeMap<String, String>>,
    },
}

/// Per-test result captured during execution and used to build the JUnit
/// report. Serializable so the full result set can be emitted structurally
/// (e.g. for future machine-readable reporting).
#[derive(Serialize)]
struct TestResultEntry {
    name: String,
    object_id: ObjectId,
    status: TestOutcome,
    elapsed_ms: u64,
}

impl TestResultEntry {
    fn new(
        name: &str,
        object_id: &ObjectId,
        elapsed: Duration,
        status: TestOutcome,
    ) -> TestResultEntry {
        let elapsed_ms = u64::try_from(elapsed.whole_milliseconds().max(0)).unwrap_or(0);
        Self {
            name: name.to_string(),
            object_id: object_id.clone(),
            status,
            elapsed_ms,
        }
    }

    /// Render this entry as a JUnit `TestCase`. The failure message for
    /// assertion mismatches is regenerated from the structured row data.
    fn to_junit_test_case(&self) -> junit_report::TestCase {
        let elapsed = Duration::milliseconds(i64::try_from(self.elapsed_ms).unwrap_or(i64::MAX));
        let mut test_case = match &self.status {
            TestOutcome::Passed => junit_report::TestCase::success(&self.name, elapsed),
            TestOutcome::Failed(ExecutionFailure::AssertionFailed {
                columns,
                missing,
                unexpected,
            }) => {
                let msg = format_assertion_rows_for_junit(columns, missing, unexpected);
                junit_report::TestCase::failure(&self.name, elapsed, "assertion", &msg)
            }
            TestOutcome::Failed(ExecutionFailure::Error(message)) => {
                junit_report::TestCase::failure(
                    &self.name,
                    elapsed,
                    "assertion",
                    &message.replace('\n', "&#10;"),
                )
            }
            TestOutcome::ValidationFailed(failure) => {
                let message = match failure {
                    ValidationFailure::UnitTest(e) => e.to_string(),
                    ValidationFailure::AtTime(e) => e.to_string(),
                };
                junit_report::TestCase::failure(
                    &self.name,
                    elapsed,
                    "validation",
                    &message.replace('\n', "&#10;"),
                )
            }
        };
        test_case.set_classname(&self.object_id.to_string());
        test_case.set_filepath(&format!(
            "models/{}/{}/{}.sql",
            self.object_id.expect_database(),
            self.object_id.schema(),
            self.object_id.object()
        ));
        test_case
    }
}

/// Aggregated results of a test run. Acts as the canonical intermediate form
/// from which the JUnit XML report is derived, and is kept serializable so the
/// same shape can back a structured JSON report in the future.
#[derive(Serialize)]
struct TestResults {
    results: Vec<TestResultEntry>,
    summary: TestSummary,
}

impl TestResults {
    /// Build a JUnit XML report by grouping entries into one `TestSuite` per
    /// target `object_id`.
    fn to_junit_report(&self) -> junit_report::Report {
        let mut suites: BTreeMap<String, junit_report::TestSuite> = BTreeMap::new();
        for entry in &self.results {
            suites
                .entry(entry.object_id.to_string())
                .or_insert_with(|| junit_report::TestSuite::new(&entry.object_id.to_string()))
                .add_testcase(entry.to_junit_test_case());
        }
        junit_report::ReportBuilder::new()
            .add_testsuites(suites.into_values())
            .build()
    }
}

/// Run unit tests against the database.
///
/// Delegates test execution to `run_tests`, then owns all presentation:
/// printing per-test outcomes, printing the summary line, and optionally
/// writing a JUnit XML report.
///
/// # Test statement syntax
///
/// Tests are authored as SQL statements attached to the object under test:
///
/// ```sql
/// EXECUTE UNIT TEST <name>
///   FOR <target>
///   [AT TIME <expr>]
///   [MOCK <mock_view>(<columns>) AS <query>]*
///   EXPECTED <expected_result>
/// ```
///
/// A test passes when the derived assertion query returns no rows. Rows are
/// returned when:
/// - Expected rows are MISSING from the actual results
/// - Unexpected rows appear in the actual results
///
/// # Arguments
/// * `settings` - Resolved CLI settings (project directory, profile, Docker
///   image, etc.)
/// * `filter` - Optional `database.schema.object#test_name` pattern; `*`
///   and omitted trailing segments act as wildcards
/// * `junit_xml` - Optional path to write a JUnit XML report to
///
/// # Returns
/// `Ok(())` if all executed tests pass (including when there were no tests
/// to run and no filter was supplied).
///
/// # Errors
/// - [`CliError::TestsFailed`] if any test failed or failed validation
/// - [`CliError::TestsFilterMissed`] if `filter` matched no tests
/// - Project planning, type-check, or JUnit I/O errors are propagated as
///   [`CliError::Message`] or the corresponding variant
/// Connection source for executing unit tests: an ephemeral Docker container
/// (the default) or the profile's configured Materialize region (`--no-docker`).
/// Where a single test runs. Both variants yield a *fresh* per-test connection
/// in `run_single_test`: Docker spins up (or reuses) an ephemeral container and
/// connects without the server-cluster pin; Profile opens a new pinned
/// connection to the profile's region. A per-test connection keeps tests
/// isolated — its session vars and `CREATE TEMPORARY VIEW` objects are discarded
/// when the connection is dropped — without relying on in-session cleanup.
enum TestTarget<'a> {
    Docker(&'a DockerRuntime),
    Profile(&'a Settings),
}

pub async fn run(
    settings: &Settings,
    filter: Option<&str>,
    junit_xml: Option<&Path>,
    overlay: Option<&Path>,
    no_docker: bool,
) -> Result<(), CliError> {
    let results = run_tests(settings, filter, overlay, no_docker).await?;
    let test_results = match results {
        Some(results) => results,
        None => {
            progress::success("No tests found");
            return Ok(());
        }
    };

    for entry in &test_results.results {
        print_test_outcome(&entry.name, &entry.status);
    }

    print_summary(&test_results.summary);

    if let Some(path) = junit_xml {
        let report = test_results.to_junit_report();
        let mut file = File::create(path)
            .map_err(|e| CliError::Message(format!("failed to create JUnit XML file: {}", e)))?;
        report
            .write_xml(&mut file)
            .map_err(|e| CliError::Message(format!("failed to write JUnit XML report: {}", e)))?;
    }

    let summary = &test_results.summary;
    let total_failed = summary.failed + summary.validation_failed;
    if total_failed > 0 {
        return Err(CliError::TestsFailed {
            failed: total_failed,
            passed: summary.passed,
        });
    }

    Ok(())
}

async fn run_tests(
    settings: &Settings,
    filter: Option<&str>,
    overlay: Option<&Path>,
    no_docker: bool,
) -> Result<Option<TestResults>, CliError> {
    let directory = &settings.directory;
    let fs = match overlay {
        Some(p) => crate::fs::FileSystem::from_overlay_file(p).map_err(|e| {
            CliError::Message(format!("failed to load overlay {}: {}", p.display(), e))
        })?,
        None => crate::fs::FileSystem::new(),
    };
    let planned_project = project::plan(
        directory.clone(),
        settings.profile_name.clone(),
        settings.profile_suffix().map(|s| s.to_owned()),
        settings.variables().clone(),
        fs,
    )
    .await?;
    let empty_types = Types::default();
    let runtime = DockerRuntime::new().with_image(&settings.docker_image);
    // With `--no-docker`, each test runs against the profile's region instead of
    // an ephemeral container. Like the Docker path, every test gets its own
    // connection (see `TestTarget`), so a test's temporary objects and any
    // session-var changes can't leak into the next test.
    let test_filter = filter.map(TestFilter::parse);

    if planned_project.tests.is_empty() {
        return Ok(None);
    }

    let types_lock = types::load_types_lock(directory).unwrap_or_default();
    let types_cache = load_or_generate_types_cache(settings, &planned_project)?;

    let mut test_entries: Vec<TestResultEntry> = Vec::new();
    let (mut passed_tests, mut failed_tests, mut validation_failed) = (0, 0, 0);
    for (object_id, test) in &planned_project.tests {
        if let Some(ref f) = test_filter {
            if !f.matches(object_id, test) {
                continue;
            }
        }
        let target = if no_docker {
            TestTarget::Profile(settings)
        } else {
            TestTarget::Docker(&runtime)
        };
        let start_time = Instant::now();
        let outcome = run_single_test(
            &planned_project,
            object_id,
            test,
            &types_cache,
            &types_lock,
            target,
            &empty_types,
        )
        .await?;

        let elapsed = Duration::try_from(start_time.elapsed()).unwrap_or(Duration::ZERO);

        match &outcome {
            TestOutcome::Passed => passed_tests += 1,
            TestOutcome::Failed(_) => failed_tests += 1,
            TestOutcome::ValidationFailed(_) => validation_failed += 1,
        }

        test_entries.push(TestResultEntry::new(
            &test.name, object_id, elapsed, outcome,
        ));
    }

    let total_run = passed_tests + failed_tests + validation_failed;
    if total_run == 0 {
        if let Some(f) = filter {
            return Err(CliError::TestsFilterMissed { filter: f.into() });
        }
        return Ok(None);
    }

    Ok(Some(TestResults {
        results: test_entries,
        summary: TestSummary {
            passed: passed_tests,
            failed: failed_tests,
            validation_failed,
        },
    }))
}

/// Executes one test case through validation, setup SQL, assertion query, and cleanup.
///
/// Returns a `TestOutcome` without performing any terminal output so the caller
/// can own all presentation (printing, JUnit building, counting).
async fn run_single_test(
    planned_project: &Project,
    object_id: &ObjectId,
    test: &unit_test::UnitTest,
    types_cache: &Option<ProjectCache>,
    types_lock: &Types,
    target: TestTarget<'_>,
    empty_types: &Types,
) -> Result<TestOutcome, CliError> {
    let dependencies = planned_project
        .dependency_graph
        .get(object_id)
        .cloned()
        .unwrap_or_else(BTreeSet::new);

    let get_columns = |id: &ObjectId| -> Option<BTreeMap<String, types::ColumnType>> {
        types_cache
            .as_ref()
            .and_then(|tc| tc.get_columns(id))
            .or_else(|| types_lock.get_table(id).cloned())
    };

    if let Err(e) = lower::validate_unit_test(test, object_id, &get_columns, &dependencies) {
        return Ok(TestOutcome::ValidationFailed(ValidationFailure::UnitTest(
            e,
        )));
    }

    // Each test gets its own connection so its temporary objects and session
    // state die with the connection, keeping tests isolated.
    let owned_client = match target {
        TestTarget::Profile(settings) => {
            Client::connect_with_profile(settings.connection().clone())
                .await
                .map_err(CliError::Connection)?
        }
        TestTarget::Docker(runtime) => runtime_client(runtime, empty_types).await?,
    };
    let client: &Client = &owned_client;
    if let Err(e) = validate_at_time(client, test).await? {
        return Ok(TestOutcome::ValidationFailed(ValidationFailure::AtTime(e)));
    }

    let Some(target_obj) = planned_project.find_object(object_id) else {
        return Ok(TestOutcome::Failed(ExecutionFailure::Error(format!(
            "target object '{}' not found in project",
            object_id
        ))));
    };

    let typed_fqn: FullyQualifiedName = object_id.clone().into();
    let sql_statements = lower::lower_unit_test(test, &target_obj.typed_object.stmt, &typed_fqn)
        .map_err(|reason| CliError::InvalidUnitTestTarget {
            test_name: test.name.clone(),
            object_id: object_id.to_string(),
            reason,
        })?;

    for (i, sql) in sql_statements[..sql_statements.len() - 1]
        .iter()
        .enumerate()
    {
        verbose!("executing: {}", sql);
        if let Err(e) = client.batch_execute(sql).await {
            let source = if i < test.mocks.len() {
                format!("MOCK {}", test.mocks[i].fqn)
            } else if i == test.mocks.len() {
                "EXPECTED".to_string()
            } else {
                format!("target view '{}'", test.target_view)
            };
            return Ok(TestOutcome::Failed(ExecutionFailure::Error(format!(
                "failed to execute {}: {}",
                source, e
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
                let (columns, missing, unexpected) = extract_assertion_data(&rows);
                TestOutcome::Failed(ExecutionFailure::AssertionFailed {
                    columns,
                    missing,
                    unexpected,
                })
            }
        }
        Err(e) => TestOutcome::Failed(ExecutionFailure::Error(format!(
            "failed to execute test query: {}",
            e
        ))),
    };

    // No explicit cleanup: `owned_client` is dropped when this function returns,
    // ending the session and discarding its TEMPORARY views. This holds on every
    // exit path (including the early returns above on setup failure), so a broken
    // test can't leak fixed-name temp views into the next one.
    Ok(outcome)
}

/// Prints the test summary line showing pass/fail counts.
fn print_summary(summary: &TestSummary) {
    let total_failed = summary.failed + summary.validation_failed;
    info_nonl!(
        "\n{}: ",
        "test result".if_supports_color(Stream::Stderr, |t| t.bold())
    );
    if total_failed == 0 {
        let style = Style::new().green().bold();
        info_nonl!(
            "{}. ",
            "ok".if_supports_color(Stream::Stderr, |t| style.style(t))
        );
    } else {
        let style = Style::new().red().bold();
        info_nonl!(
            "{}. ",
            "FAILED".if_supports_color(Stream::Stderr, |t| style.style(t))
        );
    }
    info_nonl!(
        "{}; ",
        format!("{} passed", summary.passed).if_supports_color(Stream::Stderr, |t| t.green())
    );
    if summary.failed > 0 {
        info_nonl!(
            "{}; ",
            format!("{} failed", summary.failed).if_supports_color(Stream::Stderr, |t| t.red())
        );
    } else {
        info_nonl!("{} failed; ", summary.failed);
    }
    if summary.validation_failed > 0 {
        info!(
            "{}",
            format!("{} validation errors", summary.validation_failed)
                .if_supports_color(Stream::Stderr, |t| t.red())
        );
    } else {
        info!("{} validation errors", summary.validation_failed);
    }
}

/// Prints the complete status line and any detail output for a single test outcome.
fn print_test_outcome(name: &str, outcome: &TestOutcome) {
    match outcome {
        TestOutcome::Passed => {
            let ok_style = Style::new().green().bold();
            info!(
                "{} {} ... {}",
                "test".if_supports_color(Stream::Stderr, |t| t.cyan()),
                name.if_supports_color(Stream::Stderr, |t| t.cyan()),
                "ok".if_supports_color(Stream::Stderr, |t| ok_style.style(t))
            );
        }
        TestOutcome::ValidationFailed(failure) => {
            let fail_style = Style::new().red().bold();
            info!(
                "{} {} ... {}",
                "test".if_supports_color(Stream::Stderr, |t| t.cyan()),
                name.if_supports_color(Stream::Stderr, |t| t.cyan()),
                "VALIDATION FAILED".if_supports_color(Stream::Stderr, |t| fail_style.style(t))
            );
            match failure {
                ValidationFailure::UnitTest(e) => print_test_validation_error(e),
                ValidationFailure::AtTime(e) => {
                    info!("{}", e)
                }
            }
        }
        TestOutcome::Failed(failure) => {
            let fail_style = Style::new().red().bold();
            info!(
                "{} {} ... {}",
                "test".if_supports_color(Stream::Stderr, |t| t.cyan()),
                name.if_supports_color(Stream::Stderr, |t| t.cyan()),
                "FAILED".if_supports_color(Stream::Stderr, |t| fail_style.style(t))
            );
            match failure {
                ExecutionFailure::AssertionFailed {
                    columns,
                    missing,
                    unexpected,
                } => {
                    info_nonl!("{}", format_assertion_rows(columns, missing, unexpected))
                }
                ExecutionFailure::Error(msg) => {
                    let err_style = Style::new().red().bold();
                    info!(
                        "  {}: {}",
                        "error".if_supports_color(Stream::Stderr, |t| err_style.style(t)),
                        msg
                    )
                }
            }
        }
    }
}

/// Renders validation failures in the standard test output format.
fn print_test_validation_error(error: &TestValidationError) {
    match error {
        TestValidationError::UnmockedDependency(inner) => {
            info!("{}", inner)
        }
        TestValidationError::MockSchemaMismatch(inner) => {
            info!("{}", inner)
        }
        TestValidationError::ExpectedSchemaMismatch(inner) => {
            info!("{}", inner)
        }
        TestValidationError::InvalidAtTime(inner) => {
            info!("{}", inner)
        }
        TestValidationError::TypesCacheUnavailable { reason } => {
            let style = Style::new().bright_red().bold();
            info!(
                "{}: types cache unavailable: {}",
                "error".if_supports_color(Stream::Stderr, |t| style.style(t)),
                reason
            );
        }
    }
}

/// Creates an isolated runtime client for test execution.
///
/// Converts runtime startup failures into user-facing CLI messages with actionable wording.
async fn runtime_client(runtime: &DockerRuntime, _empty_types: &Types) -> Result<Client, CliError> {
    match runtime.get_client().await {
        Ok(client) => Ok(client),
        Err(DockerRuntimeError::ContainerStartFailed(e)) => Err(CliError::Message(format!(
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
/// Returns `Ok(Ok(()))` when valid, `Ok(Err(InvalidAtTimeError))` on
/// validation failure, and `Err(CliError)` on connection errors.
async fn validate_at_time(
    client: &Client,
    test: &unit_test::UnitTest,
) -> Result<Result<(), lower::InvalidAtTimeError>, CliError> {
    if let Some(at_time) = &test.at_time {
        let validation_query = format!("SELECT {}::mz_timestamp", at_time);
        if let Err(e) = client.simple_query(&validation_query).await {
            let error = lower::InvalidAtTimeError {
                test_name: test.name.clone(),
                at_time_value: at_time.clone(),
                db_error: e.to_string(),
            };
            return Ok(Err(error));
        }
    }
    Ok(Ok(()))
}

/// Formats failing assertion rows into a readable, ANSI-colored table for
/// terminal output.
///
/// Display-only; does not affect pass/fail decisions.
fn format_assertion_rows(
    columns: &[String],
    missing: &[BTreeMap<String, String>],
    unexpected: &[BTreeMap<String, String>],
) -> String {
    let mut out = String::new();
    let title_style = Style::new().yellow().bold();
    writeln!(
        out,
        "  {}:",
        "Test assertion failed".if_supports_color(Stream::Stderr, |t| title_style.style(t))
    )
    .unwrap();
    if missing.is_empty() && unexpected.is_empty() {
        return out;
    }
    let header = std::iter::once("status".to_string())
        .chain(columns.iter().cloned())
        .collect::<Vec<_>>()
        .join(" | ");
    let header_style = Style::new().bold().cyan();
    writeln!(
        out,
        "  {}",
        header.if_supports_color(Stream::Stderr, |t| header_style.style(t))
    )
    .unwrap();
    writeln!(
        out,
        "  {}",
        "-".repeat(header.len())
            .if_supports_color(Stream::Stderr, |t| t.cyan())
    )
    .unwrap();

    let groups: [(&str, &[BTreeMap<String, String>]); 2] =
        [("MISSING", missing), ("UNEXPECTED", unexpected)];
    for (status, rows) in groups {
        let status_colored = match status {
            "MISSING" => {
                let style = Style::new().red().bold();
                status
                    .if_supports_color(Stream::Stderr, |t| style.style(t))
                    .to_string()
            }
            "UNEXPECTED" => {
                let style = Style::new().yellow().bold();
                status
                    .if_supports_color(Stream::Stderr, |t| style.style(t))
                    .to_string()
            }
            _ => status.to_string(),
        };
        for row in rows {
            let mut values = vec![status_colored.clone()];
            for col_name in columns {
                values.push(
                    row.get(col_name)
                        .cloned()
                        .unwrap_or_else(|| "<null>".to_string()),
                );
            }
            writeln!(out, "  {}", values.join(" | ")).unwrap();
        }
    }
    out
}

/// Formats structured assertion data into plain-text for JUnit XML output.
///
/// Renders each row as `column_name=value` pairs, grouped by status
/// (MISSING / UNEXPECTED), making failures easy to diagnose from CI reports.
fn format_assertion_rows_for_junit(
    columns: &[String],
    missing: &[BTreeMap<String, String>],
    unexpected: &[BTreeMap<String, String>],
) -> String {
    let groups: [(&str, &str, &[BTreeMap<String, String>]); 2] = [
        ("MISSING", "expected but not produced", missing),
        ("UNEXPECTED", "produced but not expected", unexpected),
    ];

    let mut out = String::new();
    let mut first = true;
    for (status, description, rows) in groups {
        if rows.is_empty() {
            continue;
        }
        if !first {
            writeln!(out).unwrap();
        }
        first = false;
        writeln!(out, "{} rows ({}):", status, description).unwrap();
        for row in rows {
            let pairs: Vec<String> = columns
                .iter()
                .map(|col_name| {
                    let value = row.get(col_name).map(String::as_str).unwrap_or("<null>");
                    format!("{}={}", col_name, value)
                })
                .collect();
            writeln!(out, "  {}", pairs.join(", ")).unwrap();
        }
    }
    out
}

/// Extracts structured assertion data from failing query rows.
///
/// Column 0 is the status (`MISSING` or `UNEXPECTED`); remaining columns are
/// data columns. Returns `(column_names, missing_rows, unexpected_rows)`.
fn extract_assertion_data(
    rows: &[tokio_postgres::SimpleQueryRow],
) -> (
    Vec<String>,
    Vec<BTreeMap<String, String>>,
    Vec<BTreeMap<String, String>>,
) {
    if rows.is_empty() {
        return (Vec::new(), Vec::new(), Vec::new());
    }

    let columns: Vec<String> = rows[0]
        .columns()
        .iter()
        .skip(1)
        .map(|col| col.name().to_string())
        .collect();

    let mut missing = Vec::new();
    let mut unexpected = Vec::new();

    for row in rows {
        let status = row.get(0).unwrap_or("UNKNOWN");
        let mut map = BTreeMap::new();
        for (i, col_name) in columns.iter().enumerate() {
            let value = row.get(i + 1).unwrap_or("<null>").to_string();
            map.insert(col_name.clone(), value);
        }
        match status {
            "MISSING" => missing.push(map),
            "UNEXPECTED" => unexpected.push(map),
            _ => unexpected.push(map),
        }
    }

    (columns, missing, unexpected)
}

/// Load or generate type metadata needed for test execution.
///
/// Reuses previously validated schemas when possible, re-validating only
/// objects whose definitions have changed. Returns a project cache handle
/// for resolving column metadata during test runs.
fn load_or_generate_types_cache(
    settings: &Settings,
    planned_project: &Project,
) -> Result<Option<ProjectCache>, CliError> {
    use crate::project::compiler::typecheck;

    let directory = &settings.directory;
    let external_types = types::load_types_lock(directory).unwrap_or_default();

    typecheck::run(
        directory,
        settings.profile_name().unwrap_or(""),
        settings.profile_suffix(),
        settings.variables(),
        planned_project,
        external_types,
    )
    .map_err(CliError::TypeCheckFailed)?;

    ProjectCache::open(
        directory,
        settings.profile_name().unwrap_or(""),
        settings.profile_suffix(),
        settings.variables(),
    )
    .map_err(|e| CliError::Message(format!("Failed to open types cache: {}", e)))
}
