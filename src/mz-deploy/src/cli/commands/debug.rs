//! Debug command - test database connection.

use crossterm::style::Stylize;
use crate::cli::CliError;
use crate::client::{Client, Profile};
use owo_colors::OwoColorize;

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
    println!("{}: {}", "Profile".green(), profile_display.cyan());

    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    let row = client.query_one(r#"
        SELECT
            mz_version() AS version,
            mz_environment_id() AS environment_id,
            current_role() as role"#, &[]).await?;

    let version: String = row.get("version");
    let environment_id: String = row.get("environment_id");
    let role: String = row.get("role");

    let row = client.query_one("show cluster", &[]).await?;
    let cluster: String = row.get("cluster");

    println!(
        "{} {}:{}",
        "Connected to".green(),
        profile.host.to_string().cyan(),
        profile.port.to_string().cyan()
    );
    println!("  {}: {}", "Environment".dimmed(), environment_id);
    println!("  {}: {}", "Cluster".dimmed(), cluster);
    println!("  {}: {}", "Version".dimmed(), version);
    println!("  {}: {}", "Role".dimmed(), role.yellow());

    Ok(())
}
