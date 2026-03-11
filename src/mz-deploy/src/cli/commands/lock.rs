//! Generate data contracts command - creates types.lock for external dependencies.

use crate::cli::CliError;
use crate::cli::progress;
use crate::client::Client;
use crate::config::Settings;
use crate::project;

/// Generate data contracts (types.lock) for external dependencies.
///
/// This command:
/// - Loads and parses the project
/// - Connects to the database
/// - Queries schema information for external dependencies
/// - Writes types.lock file with type information
///
/// This is useful for:
/// - CI/CD pipelines that need to validate data contracts
/// - External tooling that validates schemas
/// - Developers who want type information without full compile validation
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `directory` - Project root directory
///
/// # Returns
/// Ok(()) if types.lock is successfully generated
///
/// # Errors
/// Returns `CliError::Project` if project loading fails
/// Returns `CliError::Connection` if database connection fails
pub async fn run(settings: &Settings) -> Result<(), CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;

    progress::info("Generating data contracts for external dependencies...");

    // Connect to the database
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    // Load and plan the project
    let planned_project = project::plan(
        directory,
        &settings.profile_name,
        settings.suffix(),
        settings.cluster_suffix(),
        settings.variables(),
    )?;

    let has_tables = planned_project.get_tables().next().is_some();
    if planned_project.external_dependencies.is_empty() && !has_tables {
        progress::info("No external dependencies or tables found - types.lock not needed");
        return Ok(());
    }

    let table_count = planned_project.get_tables().count();
    progress::info(&format!(
        "Found {} external dependencies and {} tables",
        planned_project.external_dependencies.len(),
        table_count
    ));

    // Query external types and write types.lock
    let types = client
        .types()
        .query_external_types(&planned_project)
        .await?;
    types.write_types_lock(directory)?;

    progress::success(&format!(
        "Successfully generated types.lock with {} object schemas",
        types.objects.len()
    ));

    Ok(())
}
