//! Roles apply command - converge live role state to match definitions.

use crate::cli::CliError;
use crate::cli::executor::{DeploymentExecutor, SqlCollector};
use crate::cli::progress;
use crate::client::Client;
use crate::client::quote_identifier;
use crate::config::Settings;
use crate::project::roles::{self, RoleDefinition};
use mz_sql_parser::ast::AlterRoleOption;
use mz_sql_parser::ast::SetRoleVar;
use owo_colors::OwoColorize;
use std::collections::BTreeSet;
use std::time::Instant;

/// Run the `roles apply` command.
///
/// Loads role definitions from `<directory>/roles/` and converges
/// the live Materialize state to match: creating missing roles and
/// applying ALTER, GRANT, and COMMENT statements idempotently.
pub async fn run(
    settings: &Settings,
    dry_run: bool,
    collector: Option<SqlCollector>,
) -> Result<(), CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;
    let start_time = Instant::now();

    // Load role definitions
    progress::stage_start("Loading role definitions");
    let load_start = Instant::now();
    let definitions = roles::load_roles(directory, &profile.name, settings.variables())?;

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

    let executor = DeploymentExecutor::with_collector(&client, dry_run, collector);

    // Apply each role definition
    for def in &definitions {
        apply_role(&client, &executor, def).await?;
    }

    let total_duration = start_time.elapsed();
    if dry_run {
        progress::summary("Roles dry run complete", total_duration);
    } else {
        progress::summary("Roles applied successfully", total_duration);
    }

    Ok(())
}

/// Apply a single role definition: create if missing, then execute
/// ALTER, GRANT, REVOKE, RESET, and COMMENT statements.
async fn apply_role(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    def: &RoleDefinition,
) -> Result<(), CliError> {
    let role_name = &def.name;

    // Check if role already exists
    let exists = client
        .introspection()
        .role_exists(role_name)
        .await
        .map_err(CliError::Connection)?;

    if exists {
        if !executor.is_dry_run() {
            println!("  {} Role '{}' exists", "=".dimmed(), role_name);
        }
    } else {
        // Create role from the parsed CREATE ROLE statement
        if !executor.is_dry_run() {
            println!("  {} Creating role '{}'", "+".green().bold(), role_name);
        }
        executor.execute_sql(&def.create_stmt).await?;
    }

    // Execute ALTER ROLE statements
    for alter in &def.alter_stmts {
        executor.execute_sql(alter).await?;
    }

    // Execute GRANT ROLE statements
    for grant in &def.grants {
        executor.execute_sql(grant).await?;
    }

    // Execute COMMENT statements
    for comment in &def.comments {
        executor.execute_sql(comment).await?;
    }

    // Revoke stale grants
    let current_members = client
        .introspection()
        .get_role_members(role_name)
        .await
        .map_err(CliError::Connection)?;

    let desired_members: BTreeSet<String> = def
        .grants
        .iter()
        .flat_map(|g| g.member_names.iter().map(|m| m.as_str().to_lowercase()))
        .collect();

    for member in &current_members {
        if !desired_members.contains(&member.to_lowercase()) {
            if !executor.is_dry_run() {
                println!(
                    "  {} Revoking role '{}' from '{}'",
                    "-".red().bold(),
                    role_name,
                    member
                );
            }
            let sql = format!(
                "REVOKE {} FROM {}",
                quote_identifier(role_name),
                quote_identifier(member)
            );
            executor.execute_sql(&sql).await?;
        }
    }

    // Reset stale session defaults
    let current_params = client
        .introspection()
        .get_role_parameters(role_name)
        .await
        .map_err(CliError::Connection)?;

    let desired_params: BTreeSet<String> = def
        .alter_stmts
        .iter()
        .filter_map(|alter| match &alter.option {
            AlterRoleOption::Variable(SetRoleVar::Set { name, .. }) => {
                Some(name.as_str().to_lowercase())
            }
            _ => None,
        })
        .collect();

    for param in &current_params {
        if !desired_params.contains(&param.to_lowercase()) {
            if !executor.is_dry_run() {
                println!(
                    "  {} Resetting '{}' on role '{}'",
                    "-".red().bold(),
                    param,
                    role_name
                );
            }
            let sql = format!(
                "ALTER ROLE {} RESET {}",
                quote_identifier(role_name),
                quote_identifier(param)
            );
            executor.execute_sql(&sql).await?;
        }
    }

    Ok(())
}
