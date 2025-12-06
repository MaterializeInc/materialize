//! Debug command - test database connection.

use crate::cli::CliError;
use crate::client::{Client, Profile};

/// Test database connection with the specified profile.
///
/// # Arguments
/// * `profile` - Database profile containing connection information
///
/// # Returns
/// Ok(()) if connection succeeds
///
/// # Errors
/// Returns `CliError::Connection` if connection fails
pub async fn run(profile: &Profile) -> Result<(), CliError> {
    let profile_display = profile.name.as_str();
    println!("Testing connection with profile: {}", profile_display);

    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    client
        .log_connection_info()
        .await
        .map_err(CliError::Connection)?;

    println!("Connection test successful!");

    Ok(())
}
