//! Test command - run unit tests against the database.

use crate::cli::CliError;
use crate::project::{self, typed};
use crate::types::{self, TypeCheckError, Types};
use crate::unit_test;
use crate::utils::docker_runtime::DockerRuntime;
use mz_sql_parser::ast::Ident;
use owo_colors::OwoColorize;
use std::collections::BTreeSet;
use std::path::Path;

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
pub async fn run(directory: &Path) -> Result<(), CliError> {
    // Load the project (tests are loaded during compilation)
    let planned_project = project::plan(directory)?;

    // Tests use their own mocks, so don't pre-create tables from types.lock
    let empty_types = Types::default();

    // Create Docker runtime and get connected client
    let runtime = DockerRuntime::new();
    if planned_project.tests.is_empty() {
        println!("No tests found in {}", directory.display());
        return Ok(());
    }

    // Load types for validation
    // 1. Load types.lock (external dependencies) - ok if missing
    let mut combined_types = types::load_types_lock(directory).unwrap_or_default();

    // 2. Load types.cache (internal views) or trigger typecheck if stale/missing
    let internal_types =
        load_or_generate_types_cache(directory, &planned_project, &runtime).await?;
    combined_types.merge(&internal_types);

    println!(
        "{}\n",
        format!("Running tests from {}:", directory.display()).bold()
    );

    let mut passed_tests = 0;
    let mut failed_tests = 0;
    let mut validation_failed = 0;

    // Run each test from the compiled project
    for (object_id, test) in &planned_project.tests {
        // Get dependencies for this object from the project's dependency graph
        let dependencies = planned_project
            .dependency_graph
            .get(object_id)
            .cloned()
            .unwrap_or_else(BTreeSet::new);

        // Validate test before running
        if let Err(e) =
            unit_test::validate_unit_test(test, object_id, &combined_types, &dependencies)
        {
            println!(
                "{} {} ... {}",
                "test".cyan(),
                test.name.cyan(),
                "VALIDATION FAILED".red().bold()
            );
            // Print the inner error which has the detailed display
            match &e {
                unit_test::TestValidationError::UnmockedDependency(inner) => eprintln!("{}", inner),
                unit_test::TestValidationError::MockSchemaMismatch(inner) => eprintln!("{}", inner),
                unit_test::TestValidationError::ExpectedSchemaMismatch(inner) => {
                    eprintln!("{}", inner)
                }
                unit_test::TestValidationError::TypesCacheUnavailable { reason } => {
                    eprintln!(
                        "{}: types cache unavailable: {}",
                        "error".bright_red().bold(),
                        reason
                    );
                }
            }
            validation_failed += 1;
            continue;
        }

        let client = match runtime.get_client(&empty_types).await {
            Ok(client) => client,
            Err(TypeCheckError::ContainerStartFailed(e)) => {
                return Err(CliError::Message(format!(
                    "Docker not available for running tests: {}",
                    e
                )));
            }
            Err(e) => {
                return Err(CliError::Message(format!(
                    "Failed to start test environment: {}",
                    e
                )));
            }
        };

        print!("{} {} ... ", "test".cyan(), test.name.cyan());

        // Find the target object in the project
        let target_obj = match planned_project.find_object(object_id) {
            Some(obj) => obj,
            None => {
                println!("{}", "FAILED".red().bold());
                eprintln!(
                    "  {}: target object '{}' not found in project",
                    "error".red().bold(),
                    object_id
                );
                failed_tests += 1;
                continue;
            }
        };

        // Convert planned::ObjectId to typed::FullyQualifiedName for unit test processing
        // Note: Ident::new() only fails for invalid SQL identifiers, but ObjectIds
        // are created from successfully parsed SQL files, so identifiers are always valid.
        let typed_fqn =
            typed::FullyQualifiedName::from(mz_sql_parser::ast::UnresolvedItemName(vec![
                Ident::new(&object_id.database)
                    .expect("database name from parsed SQL should be valid identifier"),
                Ident::new(&object_id.schema)
                    .expect("schema name from parsed SQL should be valid identifier"),
                Ident::new(&object_id.object)
                    .expect("object name from parsed SQL should be valid identifier"),
            ]));

        // Desugar the test
        let sql_statements =
            unit_test::desugar_unit_test(test, &target_obj.typed_object.stmt, &typed_fqn);

        // Execute all SQL statements except the last one (which is the test query)
        let mut execution_failed = false;
        for sql in &sql_statements[..sql_statements.len() - 1] {
            if let Err(e) = client.execute(sql, &[]).await {
                println!("{}", "FAILED".red().bold());
                eprintln!("  {}: failed to execute SQL: {:?}", "error".red().bold(), e);
                eprintln!("  statement: {}", sql);
                execution_failed = true;
                failed_tests += 1;
                break;
            }
        }

        if execution_failed {
            continue;
        }

        // Execute the test query (last statement)
        let test_query = &sql_statements[sql_statements.len() - 1];
        match client.query(test_query, &[]).await {
            Ok(rows) => {
                if rows.is_empty() {
                    println!("{}", "ok".green().bold());
                    passed_tests += 1;
                } else {
                    println!("{}", "FAILED".red().bold());
                    eprintln!("  {}:", "Test assertion failed".yellow().bold());

                    // Print column headers
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

                    // Print rows
                    for row in &rows {
                        let mut values: Vec<String> = Vec::new();
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

                            // Color the status column (first column) differently
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

                    failed_tests += 1;
                }
            }
            Err(e) => {
                println!("{}", "FAILED".red().bold());
                eprintln!(
                    "  {}: failed to execute test query: {}",
                    "error".red().bold(),
                    e
                );
                failed_tests += 1;
            }
        }

        // Clean up with DISCARD ALL
        if let Err(e) = client.execute("DISCARD ALL", &[]).await {
            eprintln!("warning: failed to execute DISCARD ALL: {}", e);
        }
    }

    // Print test summary
    print!("\n{}: ", "test result".bold());
    let total_failed = failed_tests + validation_failed;
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
