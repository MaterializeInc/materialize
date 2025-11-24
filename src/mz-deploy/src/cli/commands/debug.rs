//! Debug command - test database connection.

use crate::cli::{CliError, helpers};
use std::path::Path;

/// Test database connection with the specified profile.
///
/// # Arguments
/// * `profile_name` - Optional profile name (defaults to "default")
/// * `_directory` - Project directory (unused, for signature consistency)
///
/// # Returns
/// Ok(()) if connection succeeds
///
/// # Errors
/// Returns `CliError::Connection` if connection fails
pub async fn run(profile_name: Option<&str>, _directory: &Path) -> Result<(), CliError> {
    let profile_display = profile_name.unwrap_or("default");
    println!("Testing connection with profile: {}", profile_display);

    let _client = helpers::connect_to_database(profile_name).await?;

    println!("Connection test successful!");

    Ok(())
}
