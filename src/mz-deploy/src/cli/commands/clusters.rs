//! Clusters apply command - converge live cluster state to match definitions.

use crate::cli::CliError;
use crate::cli::commands::grants;
use crate::cli::executor::{DeploymentExecutor, SqlCollector};
use crate::cli::progress;
use crate::client::{Client, ClusterOptions, quote_identifier};
use crate::config::Settings;
use crate::info;
use crate::project::clusters::{self, ClusterDefinition, extract_replication_factor, extract_size};
use owo_colors::OwoColorize;
use std::time::Instant;

/// Run the `clusters apply` command.
///
/// Loads cluster definitions from `<directory>/clusters/` and converges
/// the live Materialize state to match: creating missing clusters and
/// altering ones whose configuration has drifted.
pub async fn run(
    settings: &Settings,
    dry_run: bool,
    collector: Option<SqlCollector>,
) -> Result<(), CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;
    let start_time = Instant::now();

    // Load cluster definitions
    progress::stage_start("Loading cluster definitions");
    let load_start = Instant::now();
    let definitions = clusters::load_clusters(
        directory,
        &profile.name,
        settings.cluster_suffix(),
        settings.variables(),
    )?;

    if definitions.is_empty() {
        info!(
            "  {} No clusters/ directory or no .sql files found — nothing to do.",
            "info:".blue().bold()
        );
        return Ok(());
    }

    progress::stage_success(
        &format!("Found {} cluster definition(s)", definitions.len()),
        load_start.elapsed(),
    );

    // Connect to Materialize
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    let executor = DeploymentExecutor::with_collector(&client, dry_run, collector);

    // Apply each cluster definition
    for def in &definitions {
        apply_cluster(&client, &executor, def).await?;
    }

    let total_duration = start_time.elapsed();
    if dry_run {
        progress::summary("Clusters dry run complete", total_duration);
    } else {
        progress::summary("Clusters applied successfully", total_duration);
    }

    Ok(())
}

/// Apply a single cluster definition: create if missing, alter if drifted,
/// then execute grants, revocations, and comments.
async fn apply_cluster(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    def: &ClusterDefinition,
) -> Result<(), CliError> {
    let cluster_name = &def.name;

    // Check if cluster already exists
    let existing = client
        .introspection()
        .get_cluster(cluster_name)
        .await
        .map_err(CliError::Connection)?;

    match existing {
        None => {
            // Create cluster from the parsed CREATE CLUSTER statement
            if !executor.is_dry_run() {
                info!(
                    "  {} Creating cluster '{}'",
                    "+".green().bold(),
                    cluster_name
                );
            }
            executor.execute_sql(&def.create_stmt).await?;
        }
        Some(existing_cluster) => {
            // Compare desired vs actual configuration
            let desired_size = extract_size(&def.create_stmt);
            let desired_rf = extract_replication_factor(&def.create_stmt);

            let needs_alter = {
                let size_differs = desired_size.as_deref() != existing_cluster.size.as_deref();
                let rf_differs = desired_rf.map(i64::from) != existing_cluster.replication_factor;
                size_differs || rf_differs
            };

            if needs_alter {
                let size = desired_size.unwrap_or_else(|| {
                    existing_cluster
                        .size
                        .clone()
                        .unwrap_or_else(|| "25cc".to_string())
                });
                let rf = desired_rf.unwrap_or_else(|| {
                    existing_cluster
                        .replication_factor
                        .unwrap_or(1)
                        .try_into()
                        .unwrap_or(1)
                });

                if !executor.is_dry_run() {
                    info!(
                        "  {} Altering cluster '{}'",
                        "~".yellow().bold(),
                        cluster_name
                    );
                }
                let options = ClusterOptions {
                    size,
                    replication_factor: rf,
                };
                let alter_sql = format!(
                    "ALTER CLUSTER {} SET (SIZE = '{}', REPLICATION FACTOR = {})",
                    quote_identifier(cluster_name),
                    options.size,
                    options.replication_factor
                );
                executor.execute_sql(&alter_sql).await?;
            } else if !executor.is_dry_run() {
                info!(
                    "  {} Cluster '{}' is up to date",
                    "=".dimmed(),
                    cluster_name
                );
            }
        }
    }

    // Execute GRANT statements
    for grant in &def.grants {
        executor.execute_sql(grant).await?;
    }

    // Revoke stale grants
    let current_grants = client
        .introspection()
        .get_cluster_grants(cluster_name)
        .await
        .map_err(CliError::Connection)?;
    let desired = grants::desired_grants(&def.grants, &["USAGE", "CREATE"]);
    let revocations = grants::stale_grant_revocations(
        &current_grants,
        &desired,
        "CLUSTER",
        &quote_identifier(cluster_name),
    );
    grants::execute_revocations(executor, &revocations, "cluster", &cluster_name).await?;

    // Execute COMMENT statements
    for comment in &def.comments {
        executor.execute_sql(comment).await?;
    }

    Ok(())
}
