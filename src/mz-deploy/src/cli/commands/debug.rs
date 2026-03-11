//! Debug command - test database connection.

use crate::cli::CliError;
use crate::client::{Client, Profile};
use crate::config::Settings;
use crate::info;
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
pub async fn run(settings: &Settings) -> Result<(), CliError> {
    let profile = settings.connection();
    let profile_display = profile.name.as_str();
    info!("{}: {}", "Profile".green(), profile_display.cyan());

    // Run database connection and Docker check in parallel since they're independent.
    let (db_result, docker_status) = tokio::join!(
        connect_and_query(profile),
        DockerRuntime::check_availability(),
    );

    let (version, environment_id, role, cluster) = db_result?;

    info!(
        "{} {}:{}",
        "Connected to".green(),
        profile.host.to_string().cyan(),
        profile.port.to_string().cyan()
    );
    info!("  {}: {}", "Environment".dimmed(), environment_id);
    info!("  {}: {}", "Cluster".dimmed(), cluster);
    info!("  {}: {}", "Version".dimmed(), version);
    info!("  {}: {}", "Role".dimmed(), role.yellow());

    match docker_status {
        DockerStatus::Running => {
            info!(
                "{}: {}",
                "Docker".green(),
                "installed, daemon running".green()
            );
        }
        DockerStatus::NotRunning => {
            info!(
                "{}: {}",
                "Docker".green(),
                "installed, daemon not running".yellow()
            );
        }
        DockerStatus::NotInstalled => {
            info!("{}: {}", "Docker".green(), "not installed".yellow());
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
