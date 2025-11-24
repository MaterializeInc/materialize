//! Generate data contracts command - creates types.lock for external dependencies.

use crate::cli::{CliError, helpers};
use crate::client::Profile;
use crate::project;
use std::path::Path;

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
pub async fn run(profile: Option<&Profile>, directory: &Path) -> Result<(), CliError> {
    println!("Generating data contracts for external dependencies...");

    // Connect to the database
    let mut client = helpers::connect_to_database(profile.unwrap()).await?;

    // Load and plan the project
    let mir_project = project::plan(directory)?;

    if mir_project.external_dependencies.is_empty() {
        println!("No external dependencies found - types.lock not needed");
        return Ok(());
    }

    println!(
        "Found {} external dependencies",
        mir_project.external_dependencies.len()
    );

    // Query external types and write types.lock
    let types = client.query_external_types(&mir_project).await?;
    types.write_types_lock(directory)?;

    println!(
        "Successfully generated types.lock with {} object schemas",
        types.objects.len()
    );

    Ok(())
}
