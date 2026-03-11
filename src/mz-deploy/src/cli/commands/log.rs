//! History command - show deployment history in chronological order.

use crate::cli::CliError;
use crate::client::{Client, DeploymentHistoryEntry};
use crate::config::Settings;
use crate::{info, info_nonl, log};
use chrono::{DateTime, Local};
use owo_colors::OwoColorize;
use std::fmt;
use std::io::{IsTerminal, Write};
use std::process::{Command, Stdio};

/// Render struct for deployment history — single source of truth for formatting.
#[derive(serde::Serialize)]
#[serde(transparent)]
struct HistoryOutput {
    entries: Vec<DeploymentHistoryEntry>,
}

impl fmt::Display for HistoryOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Deployment history (promoted):\n")?;

        for entry in &self.entries {
            let datetime: DateTime<Local> = entry.promoted_at.with_timezone(&Local);
            let date_str = datetime.format("%a %b %d %H:%M:%S %Y %z").to_string();

            writeln!(
                f,
                "{} {} [{}]",
                "deployment".yellow().bold(),
                entry.deploy_id.cyan(),
                entry.kind.to_string().dimmed()
            )?;
            if let Some(commit_sha) = &entry.git_commit {
                writeln!(f, "{}: {}", "Commit".dimmed(), commit_sha)?;
            }
            writeln!(
                f,
                "{}: {}",
                "Promoted by".dimmed(),
                entry.deployed_by.yellow()
            )?;
            writeln!(f, "{}:   {}", "Date".dimmed(), date_str)?;
            writeln!(f)?;

            for sq in &entry.schemas {
                writeln!(f, "    {}.{}", sq.database.dimmed(), sq.schema)?;
            }
            writeln!(f)?;
        }

        Ok(())
    }
}

/// Show deployment history in chronological order (promoted deployments only).
///
/// This command:
/// - Queries all promoted deployments (promoted_at IS NOT NULL) ordered by promoted_at DESC
/// - Groups deployments by environment and promotion time
/// - Lists all schemas included in each deployment
///
/// Similar to `git log` - shows historical production deployment activity with
/// each deployment showing the "commit message" (schemas changed).
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `limit` - Optional limit on number of deployments to show
///
/// # Returns
/// Ok(()) if listing succeeds
///
/// # Errors
/// Returns `CliError::Connection` for database errors
pub async fn run(settings: &Settings, limit: Option<usize>) -> Result<(), CliError> {
    let profile = settings.connection();
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    client.deployments().create_deployments().await?;
    let history = client.deployments().list_deployment_history(limit).await?;

    let output = HistoryOutput { entries: history };

    if log::json_output_enabled() {
        log::output(&output);
    } else if output.entries.is_empty() {
        info!("No deployment history found.");
        info!();
        info!("To create and promote a deployment, run:");
        info!("  {} {} {}", "mz-deploy".cyan(), "stage".cyan(), ".".cyan());
        info!(
            "  {} {} {}",
            "mz-deploy".cyan(),
            "apply".cyan(),
            "--staging-env <name>".cyan()
        );
    } else {
        let formatted = format!("{output}");
        if std::io::stderr().is_terminal() {
            display_with_pager(&formatted);
        } else {
            info_nonl!("{}", formatted);
        }
    }

    Ok(())
}

/// Display output through a pager (less) if available, otherwise print directly.
fn display_with_pager(content: &str) {
    // Try to spawn less with flags:
    // -R: interpret ANSI color codes
    // -F: exit immediately if content fits on one screen
    // -X: don't clear screen on exit
    if let Ok(mut child) = Command::new("less")
        .args(["-RFX"])
        .stdin(Stdio::piped())
        .spawn()
    {
        if let Some(mut stdin) = child.stdin.take() {
            // Write content to less, ignore errors (e.g., broken pipe if user quits early)
            let _ = stdin.write_all(content.as_bytes());
        }
        // Wait for less to exit
        let _ = child.wait();
    } else {
        // Fallback: print directly if less isn't available
        info_nonl!("{}", content);
    }
}
