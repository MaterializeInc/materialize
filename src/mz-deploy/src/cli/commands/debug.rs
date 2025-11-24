//! Debug command - test database connection.

use crate::cli::{CliError, helpers};
use crate::client::Profile;
use std::path::Path;

/// Test database connection with the specified profile.
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `_directory` - Project directory (unused, for signature consistency)
///
/// # Returns
/// Ok(()) if connection succeeds
///
/// # Errors
/// Returns `CliError::Connection` if connection fails
pub async fn run(profile: Option<&Profile>, _directory: &Path) -> Result<(), CliError> {
    let profile_display = profile.map(|p| p.name.as_str()).unwrap_or("default");
    println!("Testing connection with profile: {}", profile_display);

    let _client = helpers::connect_to_database(profile.unwrap()).await?;

    println!("Connection test successful!");

    Ok(())
}
