//! Debug command - test database connection.

use crate::cli::CliError;
use crate::client::{Client, Profile};
use crate::types::docker_runtime::{DockerRuntime, DockerStatus};
use crossterm::style::Stylize;
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

    // Run database connection and Docker check in parallel since they're independent.
    let (db_result, docker_status) = tokio::join!(
        connect_and_query(profile),
        DockerRuntime::check_availability(),
    );

    let (version, environment_id, role, cluster) = db_result?;

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

    match docker_status {
        DockerStatus::Running => {
            println!(
                "{}: {}",
                "Docker".green(),
                "installed, daemon running".green()
            );
        }
        DockerStatus::NotRunning => {
            println!(
                "{}: {}",
                "Docker".green(),
                "installed, daemon not running".yellow()
            );
        }
        DockerStatus::NotInstalled => {
            println!("{}: {}", "Docker".green(), "not installed".yellow());
        }
    }

    Ok(())
}

async fn connect_and_query(
    profile: &Profile,
) -> Result<(String, String, String, String), CliError> {
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    let row = client
        .query_one(
            r#"
        SELECT
            mz_version() AS version,
            mz_environment_id() AS environment_id,
            current_role() as role"#,
            &[],
        )
        .await?;

    let version: String = row.get("version");
    let environment_id: String = row.get("environment_id");
    let role: String = row.get("role");

    let row = client.query_one("show cluster", &[]).await?;
    let cluster: String = row.get("cluster");

    Ok((version, environment_id, role, cluster))
}
