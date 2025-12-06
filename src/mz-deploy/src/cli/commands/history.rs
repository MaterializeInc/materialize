//! History command - show deployment history in chronological order.

use crate::cli::CliError;
use crate::client::{Client, Profile};
use chrono::{DateTime, Local};
use owo_colors::OwoColorize;
use std::io::Write;
use std::process::{Command, Stdio};

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
pub async fn run(profile: &Profile, limit: Option<usize>) -> Result<(), CliError> {
    // Connect to database
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    client.create_deployments().await?;
    let history = client.list_deployment_history(limit).await?;

    if history.is_empty() {
        println!("No deployment history found.");
        println!();
        println!("To create and promote a deployment, run:");
        println!("  {} {} {}", "mz-deploy".cyan(), "stage".cyan(), ".".cyan());
        println!(
            "  {} {} {}",
            "mz-deploy".cyan(),
            "apply".cyan(),
            "--staging-env <name>".cyan()
        );
        return Ok(());
    }

    // Build output string first (while we still have async context)
    let mut output = String::new();
    output.push_str("Deployment history (promoted):\n\n");

    for (environment, promoted_at, deployed_by, commit, kind, schemas) in history {
        // Convert SystemTime to DateTime for formatting
        let datetime: DateTime<Local> = promoted_at.into();
        let date_str = datetime.format("%a %b %d %H:%M:%S %Y %z").to_string();

        // Display deployment header (like a git commit)
        output.push_str(&format!(
            "{} {} [{}]\n",
            "deployment".yellow().bold(),
            environment.cyan(),
            kind.dimmed()
        ));
        if let Some(commit_sha) = commit {
            output.push_str(&format!("{}: {}\n", "Commit".dimmed(), commit_sha));
        }
        output.push_str(&format!(
            "{}: {}\n",
            "Promoted by".dimmed(),
            deployed_by.yellow()
        ));
        output.push_str(&format!("{}:   {}\n", "Date".dimmed(), date_str));
        output.push('\n');

        // List all schemas in this deployment (like files in a git commit)
        for (database, schema) in schemas {
            output.push_str(&format!("    {}.{}\n", database.dimmed(), schema));
        }
        output.push('\n');
    }

    // Display with pager (spawned after async work is complete)
    display_with_pager(&output);

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
        print!("{}", content);
    }
}
