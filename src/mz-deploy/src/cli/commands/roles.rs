//! Roles apply command - converge live role state to match definitions.

use crate::cli::CliError;
use crate::client::{Client, ConnectionError, Profile};
use crate::project::roles::{self, RoleDefinition};
use crate::utils::progress;
use owo_colors::OwoColorize;
use std::path::Path;
use std::time::Instant;

/// Run the `roles apply` command.
///
/// Loads role definitions from `<directory>/roles/` and converges
/// the live Materialize state to match: creating missing roles and
/// applying ALTER, GRANT, and COMMENT statements idempotently.
pub async fn run(directory: &Path, profile: &Profile) -> Result<(), CliError> {
    let start_time = Instant::now();

    // Load role definitions
    progress::stage_start("Loading role definitions");
    let load_start = Instant::now();
    let definitions = roles::load_roles(directory)?;

    if definitions.is_empty() {
        println!(
            "  {} No roles/ directory or no .sql files found — nothing to do.",
            "info:".blue().bold()
        );
        return Ok(());
    }

    progress::stage_success(
        &format!("Found {} role definition(s)", definitions.len()),
        load_start.elapsed(),
    );

    // Connect to Materialize
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    // Apply each role definition
    for def in &definitions {
        apply_role(&client, def).await?;
    }

    let total_duration = start_time.elapsed();
    progress::summary("Roles applied successfully", total_duration);

    Ok(())
}

/// Apply a single role definition: create if missing, then execute
/// ALTER, GRANT, and COMMENT statements idempotently.
async fn apply_role(client: &Client, def: &RoleDefinition) -> Result<(), CliError> {
    let role_name = &def.name;

    // Check if role already exists
    let exists = client
        .role_exists(role_name)
        .await
        .map_err(CliError::Connection)?;

    if exists {
        println!("  {} Role '{}' exists", "=".dimmed(), role_name);
    } else {
        // Create role from the parsed CREATE ROLE statement
        println!("  {} Creating role '{}'", "+".green().bold(), role_name);
        let sql = format!("{}", def.create_stmt);
        client.execute(&sql, &[]).await.map_err(|e| {
            CliError::Connection(ConnectionError::Message(format!(
                "Failed to create role '{}': {}",
                role_name, e
            )))
        })?;
    }

    // Execute ALTER ROLE statements (idempotent)
    for alter in &def.alter_stmts {
        let sql = format!("{}", alter);
        client.execute(&sql, &[]).await.map_err(|e| {
            CliError::Connection(ConnectionError::Message(format!(
                "Failed to execute ALTER ROLE on '{}': {}",
                role_name, e
            )))
        })?;
    }

    // Execute GRANT ROLE statements (idempotent — granting an already-granted role is a no-op)
    for grant in &def.grants {
        let sql = format!("{}", grant);
        client.execute(&sql, &[]).await.map_err(|e| {
            CliError::Connection(ConnectionError::Message(format!(
                "Failed to execute GRANT ROLE on '{}': {}",
                role_name, e
            )))
        })?;
    }

    // Execute COMMENT statements (idempotent)
    for comment in &def.comments {
        let sql = format!("{}", comment);
        client.execute(&sql, &[]).await.map_err(|e| {
            CliError::Connection(ConnectionError::Message(format!(
                "Failed to execute COMMENT on role '{}': {}",
                role_name, e
            )))
        })?;
    }

    Ok(())
}
