// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! History command - show deployment history in chronological order.

use crate::cli::CliError;
use crate::client::{Client, DeploymentHistoryEntry};
use crate::config::Settings;
use crate::{info, info_nonl, log};
use chrono::{DateTime, Local};
use owo_colors::{OwoColorize, Stream, Style};
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

        let deployment_style = Style::new().yellow().bold();
        for entry in &self.entries {
            let datetime: DateTime<Local> = entry.promoted_at.with_timezone(&Local);
            let date_str = datetime.format("%a %b %d %H:%M:%S %Y %z").to_string();

            writeln!(
                f,
                "{} {} [{}]",
                "deployment".if_supports_color(Stream::Stderr, |t| deployment_style.style(t)),
                entry
                    .deploy_id
                    .if_supports_color(Stream::Stderr, |t| t.cyan()),
                entry
                    .kind
                    .to_string()
                    .if_supports_color(Stream::Stderr, |t| t.dimmed())
            )?;
            if let Some(commit_sha) = &entry.git_commit {
                writeln!(
                    f,
                    "{}: {}",
                    "Commit".if_supports_color(Stream::Stderr, |t| t.dimmed()),
                    commit_sha
                )?;
            }
            writeln!(
                f,
                "{}: {}",
                "Promoted by".if_supports_color(Stream::Stderr, |t| t.dimmed()),
                entry
                    .deployed_by
                    .if_supports_color(Stream::Stderr, |t| t.yellow())
            )?;
            writeln!(
                f,
                "{}:   {}",
                "Date".if_supports_color(Stream::Stderr, |t| t.dimmed()),
                date_str
            )?;
            writeln!(f)?;

            for sq in &entry.schemas {
                writeln!(
                    f,
                    "    {}.{}",
                    sq.database
                        .if_supports_color(Stream::Stderr, |t| t.dimmed()),
                    sq.schema
                )?;
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
/// * `settings` - Resolved CLI settings (profile, project directory, etc.)
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

    super::setup::verify(&client, settings.emulator()).await?;
    super::setup::validate_connection(&client, settings.emulator()).await?;
    let history = client.deployments().list_deployment_history(limit).await?;

    let output = HistoryOutput { entries: history };

    if log::json_output_enabled() {
        log::output(&output);
    } else if output.entries.is_empty() {
        info!("No deployment history found.");
        info!();
        info!("To create and promote a deployment, run:");
        info!(
            "  {} {} {}",
            "mz-deploy".if_supports_color(Stream::Stderr, |t| t.cyan()),
            "stage".if_supports_color(Stream::Stderr, |t| t.cyan()),
            ".".if_supports_color(Stream::Stderr, |t| t.cyan())
        );
        info!(
            "  {} {} {}",
            "mz-deploy".if_supports_color(Stream::Stderr, |t| t.cyan()),
            "apply".if_supports_color(Stream::Stderr, |t| t.cyan()),
            "--staging-env <name>".if_supports_color(Stream::Stderr, |t| t.cyan())
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
