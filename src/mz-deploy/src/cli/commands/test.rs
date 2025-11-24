//! Test command - run unit tests against the database.

use crate::cli::{CliError, helpers};
use crate::project::{self, hir};
use crate::unit_test;
use mz_sql_parser::ast::Ident;
use owo_colors::OwoColorize;
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
/// * `profile_name` - Optional database profile name
/// * `directory` - Project root directory
///
/// # Returns
/// Ok(()) if all tests pass
///
/// # Errors
/// Returns `CliError::Project` if project loading fails
/// Returns `CliError::Connection` if database connection fails
/// Returns error if tests fail (exits with code 1)
pub async fn run(profile_name: Option<&str>, directory: &Path) -> Result<(), CliError> {
    // Load the project
    let src_directory = directory.join("src");
    let mir_project = project::plan(&src_directory)?;

    // Connect to the database
    let client = helpers::connect_to_database(profile_name).await?;

    // Find test files
    let test_directory = directory.join("test");
    let test_files = unit_test::find_test_files(&test_directory)?;

    if test_files.is_empty() {
        println!("No test files found in {}", test_directory.display());
        return Ok(());
    }

    println!(
        "{}\n",
        format!("Running tests from {}:", test_directory.display()).bold()
    );

    let mut passed_tests = 0;
    let mut failed_tests = 0;

    // Process each test file
    for test_file in test_files {
        let content = std::fs::read_to_string(&test_file).map_err(|e| {
            CliError::Project(crate::project::error::ProjectError::Load(
                crate::project::error::LoadError::FileReadFailed {
                    path: test_file.clone(),
                    source: e,
                },
            ))
        })?;

        let tests = unit_test::parse_test_file(&content)?;

        // Run each test
        for test in tests {
            print!("{} {} ... ", "test".cyan(), test.name.cyan());

            // Find the target view statement in the project
            let mut target_fqn = None;
            let mut hir_obj = None;

            'outer: for database in &mir_project.databases {
                for schema in &database.schemas {
                    for obj in &schema.objects {
                        if obj.id.object == test.target_view {
                            target_fqn = Some(&obj.id);
                            hir_obj = Some(&obj.hir_object);
                            break 'outer;
                        }
                    }
                }
            }

            let (target_fqn, hir_obj) = match (target_fqn, hir_obj) {
                (Some(fqn), Some(obj)) => (fqn, obj),
                _ => {
                    println!("{}", "FAILED".red().bold());
                    eprintln!(
                        "  {}: target view '{}' not found in project",
                        "error".red().bold(),
                        test.target_view
                    );
                    failed_tests += 1;
                    continue;
                }
            };

            // Convert mir::ObjectId to hir::FullyQualifiedName for unit test processing
            let hir_fqn =
                hir::FullyQualifiedName::from(mz_sql_parser::ast::UnresolvedItemName(vec![
                    Ident::new(&target_fqn.database).unwrap(),
                    Ident::new(&target_fqn.schema).unwrap(),
                    Ident::new(&target_fqn.object).unwrap(),
                ]));

            // Desugar the test
            let sql_statements = unit_test::desugar_unit_test(&test, &hir_obj.stmt, &hir_fqn);

            // Execute all SQL statements except the last one (which is the test query)
            let mut execution_failed = false;
            for sql in &sql_statements[..sql_statements.len() - 1] {
                if let Err(e) = client.pg_client().execute(sql, &[]).await {
                    println!("{}", "FAILED".red().bold());
                    eprintln!("  {}: failed to execute SQL: {:?}", "error".red().bold(), e);
                    eprintln!("  statement: {}", sql);
                    execution_failed = true;
                    failed_tests += 1;
                    break;
                }
            }

            if execution_failed {
                let _ = client.pg_client().execute("DISCARD ALL", &[]).await;
                continue;
            }

            // Execute the test query (last statement)
            let test_query = &sql_statements[sql_statements.len() - 1];
            match client.pg_client().query(test_query, &[]).await {
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
            if let Err(e) = client.pg_client().execute("DISCARD ALL", &[]).await {
                eprintln!("warning: failed to execute DISCARD ALL: {}", e);
            }
        }
    }

    // Print test summary
    print!("\n{}: ", "test result".bold());
    if failed_tests == 0 {
        print!("{}. ", "ok".green().bold());
    } else {
        print!("{}. ", "FAILED".red().bold());
    }
    print!("{}; ", format!("{} passed", passed_tests).green());
    if failed_tests > 0 {
        println!("{}", format!("{} failed", failed_tests).red());
    } else {
        println!("{} failed", failed_tests);
    }

    if failed_tests > 0 {
        return Err(CliError::TestsFailed {
            failed: failed_tests,
            passed: passed_tests,
        });
    }

    Ok(())
}
