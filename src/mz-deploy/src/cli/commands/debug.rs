//! Debug command - test database connection.

use crate::cli::{CliError, helpers};
use crate::client::Profile;

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

    let _client = helpers::connect_to_database(profile).await?;

    println!("Connection test successful!");

    Ok(())
}
