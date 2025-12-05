//! Ready command - wait for staging deployment cluster hydration.

use crate::cli::{CliError, helpers};
use crate::client::{ClusterDeploymentStatus, ClusterStatusContext, FailureReason, Profile};
use crossterm::{
    cursor::{Hide, MoveToColumn, MoveUp, Show},
    execute,
    style::Stylize,
    terminal::{Clear, ClearType},
};
use owo_colors::OwoColorize;
use std::collections::HashMap;
use std::io::{self, Write};
use std::time::{Duration, Instant};

/// Wait for a staging deployment to become ready by monitoring hydration status.
///
/// This command:
/// - Validates the staging deployment exists and hasn't been promoted
/// - Subscribes to cluster hydration status
/// - Shows a live dashboard tracking hydration, lag, and health for each cluster
/// - Exits when all clusters are ready or timeout is reached
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `deploy_id` - Staging deployment ID
/// * `snapshot` - If true, check once and exit; if false, track continuously
/// * `timeout` - Optional timeout in seconds
/// * `allowed_lag_secs` - Maximum allowed lag in seconds before marking as "lagging"
///
/// # Returns
/// Ok(()) if deployment becomes ready
///
/// # Errors
/// Returns `CliError::StagingEnvironmentNotFound` if deployment doesn't exist
/// Returns `CliError::StagingAlreadyPromoted` if already promoted
/// Returns `CliError::ReadyTimeout` if timeout is reached
pub async fn run(
    profile: &Profile,
    deploy_id: &str,
    snapshot: bool,
    timeout: Option<u64>,
    allowed_lag_secs: i64,
) -> Result<(), CliError> {
    // Connect to database
    let mut client = helpers::connect_to_database(profile).await?;

    // Validate staging deployment exists and is not promoted
    let metadata = client.get_deployment_metadata(deploy_id).await?;
    match metadata {
        Some(meta) if meta.promoted_at.is_some() => {
            return Err(CliError::StagingAlreadyPromoted {
                name: deploy_id.to_string(),
            });
        }
        Some(_) => {}
        None => {
            return Err(CliError::StagingEnvironmentNotFound {
                name: deploy_id.to_string(),
            });
        }
    }

    if snapshot {
        // Snapshot mode: query once and display status
        run_snapshot(deploy_id, &client, allowed_lag_secs).await
    } else {
        // Continuous mode: subscribe and track with live dashboard
        run_continuous(deploy_id, &mut client, timeout, allowed_lag_secs).await
    }
}

/// Run in snapshot mode: query hydration status once and display.
async fn run_snapshot(
    deploy_id: &str,
    client: &crate::client::Client,
    allowed_lag_secs: i64,
) -> Result<(), CliError> {
    let statuses = client
        .get_deployment_hydration_status_with_lag(deploy_id, allowed_lag_secs)
        .await?;

    if statuses.is_empty() {
        println!("No clusters found in staging deployment '{}'", deploy_id);
        return Ok(());
    }

    println!();
    println!("{}", format!("  Deployment: {}", deploy_id).cyan().bold());
    println!();

    let mut has_failures = false;
    let mut has_non_ready = false;

    for ctx in &statuses {
        print_cluster_status(ctx, allowed_lag_secs);

        match &ctx.status {
            ClusterDeploymentStatus::Failing { .. } => has_failures = true,
            ClusterDeploymentStatus::Ready => {}
            _ => has_non_ready = true,
        }
    }

    println!();
    print_summary(&statuses);
    println!();

    if has_failures {
        Err(CliError::DeploymentFailing {
            name: deploy_id.to_string(),
        })
    } else if has_non_ready {
        Err(CliError::ClustersHydrating)
    } else {
        println!("{}", "  All clusters are ready!".green().bold());
        Ok(())
    }
}

/// Print status for a single cluster with visual formatting.
fn print_cluster_status(ctx: &ClusterStatusContext, allowed_lag_secs: i64) {
    let (status_icon, status_label, status_color) = match &ctx.status {
        ClusterDeploymentStatus::Ready => ("✓", "ready", "green"),
        ClusterDeploymentStatus::Hydrating { .. } => ("◐", "hydrating", "yellow"),
        ClusterDeploymentStatus::Lagging { .. } => ("⚠", "lagging", "yellow"),
        ClusterDeploymentStatus::Failing { .. } => ("✗", "failing", "red"),
    };

    // Cluster name header
    println!("  {}", ctx.cluster_name.as_str().bold());

    // Progress bar
    let bar = render_progress_bar(ctx.hydrated_count, ctx.total_count, 40);
    let progress_str = format!("{}/{} objects", ctx.hydrated_count, ctx.total_count);

    print!("  {} ", bar);
    match status_color {
        "green" => print!("{} {}", status_icon.green(), status_label.green()),
        "yellow" => print!("{} {}", status_icon.yellow(), status_label.yellow()),
        "red" => print!("{} {}", status_icon.red(), status_label.red()),
        _ => print!("{} {}", status_icon, status_label),
    }
    println!("  {}", progress_str.dimmed());

    // Additional context based on status
    match &ctx.status {
        ClusterDeploymentStatus::Ready => {
            println!(
                "  {} lag: {}s",
                "└".dimmed(),
                ctx.max_lag_secs.to_string().green()
            );
        }
        ClusterDeploymentStatus::Hydrating { hydrated, total } => {
            let pct = if *total > 0 {
                (*hydrated as f64 / *total as f64 * 100.0) as u8
            } else {
                0
            };
            println!("  {} {}% complete", "└".dimmed(), pct.to_string().yellow());
        }
        ClusterDeploymentStatus::Lagging { max_lag_secs } => {
            println!(
                "  {} lag: {}s (threshold: {}s)",
                "└".dimmed(),
                max_lag_secs.to_string().yellow().bold(),
                allowed_lag_secs
            );
        }
        ClusterDeploymentStatus::Failing { reason } => {
            println!("  {} {}", "└".dimmed(), reason.to_string().red());
        }
    }
    println!();
}

/// Render a Unicode progress bar.
fn render_progress_bar(current: i64, total: i64, width: usize) -> String {
    if total == 0 {
        return format!("[{}]", "░".repeat(width).dimmed());
    }

    let filled = ((current as f64 / total as f64) * width as f64) as usize;
    let empty = width.saturating_sub(filled);

    format!(
        "[{}{}]",
        "█".repeat(filled).cyan(),
        "░".repeat(empty).dimmed()
    )
}

/// Print summary footer with counts.
fn print_summary(statuses: &[ClusterStatusContext]) {
    let mut ready = 0;
    let mut hydrating = 0;
    let mut lagging = 0;
    let mut failing = 0;

    for ctx in statuses {
        match ctx.status {
            ClusterDeploymentStatus::Ready => ready += 1,
            ClusterDeploymentStatus::Hydrating { .. } => hydrating += 1,
            ClusterDeploymentStatus::Lagging { .. } => lagging += 1,
            ClusterDeploymentStatus::Failing { .. } => failing += 1,
        }
    }

    print!("  ");
    let mut parts = Vec::new();
    if ready > 0 {
        parts.push(format!("{} ready", ready).green().to_string());
    }
    if hydrating > 0 {
        parts.push(format!("{} hydrating", hydrating).yellow().to_string());
    }
    if lagging > 0 {
        parts.push(format!("{} lagging", lagging).yellow().to_string());
    }
    if failing > 0 {
        parts.push(format!("{} failing", failing).red().to_string());
    }
    println!("{}", parts.join(" · "));
}

/// Run in continuous mode: subscribe to hydration updates and show live dashboard.
async fn run_continuous(
    deploy_id: &str,
    client: &mut crate::client::Client,
    timeout: Option<u64>,
    allowed_lag_secs: i64,
) -> Result<(), CliError> {
    // Get initial hydration status
    let initial_statuses = client
        .get_deployment_hydration_status_with_lag(deploy_id, allowed_lag_secs)
        .await?;

    if initial_statuses.is_empty() {
        println!("No clusters found in staging deployment '{}'", deploy_id);
        return Ok(());
    }

    let start_time = Instant::now();

    // Subscribe to hydration updates and monitor
    let monitor_future = monitor_hydration_live(
        deploy_id,
        client,
        initial_statuses,
        start_time,
        allowed_lag_secs,
    );

    if let Some(secs) = timeout {
        match tokio::time::timeout(Duration::from_secs(secs), monitor_future).await {
            Ok(result) => result,
            Err(_) => Err(CliError::ReadyTimeout {
                name: deploy_id.to_string(),
                seconds: secs,
            }),
        }
    } else {
        monitor_future.await
    }
}

/// Monitor hydration status via SUBSCRIBE and update live dashboard.
async fn monitor_hydration_live(
    deploy_id: &str,
    client: &mut crate::client::Client,
    initial_statuses: Vec<ClusterStatusContext>,
    start_time: Instant,
    allowed_lag_secs: i64,
) -> Result<(), CliError> {
    let mut stdout = io::stdout();
    let num_clusters = initial_statuses.len();

    // Build initial state map
    let mut cluster_states: HashMap<String, ClusterStatusContext> = initial_statuses
        .into_iter()
        .map(|ctx| (ctx.cluster_name.clone(), ctx))
        .collect();

    // Calculate lines per render (header + per-cluster lines + summary)
    // Header: 3 lines, per cluster: 4 lines, summary: 2 lines
    let lines_per_render = 3 + (num_clusters * 4) + 2;

    // Initial render
    render_dashboard(
        &mut stdout,
        deploy_id,
        &cluster_states,
        start_time,
        false,
        allowed_lag_secs,
    )?;

    // Subscribe to updates
    let txn = client
        .subscribe_deployment_hydration(deploy_id, allowed_lag_secs)
        .await?;

    // Hide cursor during updates
    execute!(stdout, Hide).ok();

    loop {
        // Fetch next batch of updates
        let rows = txn
            .query("FETCH ALL c", &[])
            .await
            .map_err(|e| CliError::Message(format!("Failed to fetch subscription data: {}", e)))?;

        let mut updated = false;

        for row in rows {
            // Parse SUBSCRIBE row format:
            // [mz_timestamp, mz_diff, cluster_name, cluster_id, status, failure_reason,
            //  hydrated_count, total_count, max_lag_secs, total_replicas, problematic_replicas]
            let mz_diff: i64 = row.get(1);

            if mz_diff == -1 {
                // Retraction - ignore
                continue;
            }

            // Parse the new row format
            let cluster_name: String = row.get(2);
            let cluster_id: String = row.get(3);
            let status_str: String = row.get(4);
            let failure_reason: Option<String> = row.get(5);
            let hydrated_count: i64 = row.get(6);
            let total_count: i64 = row.get(7);
            let max_lag_secs: i64 = row.get(8);
            let total_replicas: i64 = row.get(9);
            let problematic_replicas: i64 = row.get(10);

            let status = parse_status(
                &status_str,
                failure_reason.as_deref(),
                hydrated_count,
                total_count,
                max_lag_secs,
                total_replicas,
                problematic_replicas,
            );

            cluster_states.insert(
                cluster_name.clone(),
                ClusterStatusContext {
                    cluster_name,
                    cluster_id,
                    status,
                    hydrated_count,
                    total_count,
                    max_lag_secs,
                    total_replicas,
                    problematic_replicas,
                },
            );
            updated = true;
        }

        if updated {
            // Move cursor up and clear, then re-render
            execute!(stdout, MoveUp(lines_per_render as u16), MoveToColumn(0)).ok();
            for _ in 0..lines_per_render {
                execute!(stdout, Clear(ClearType::CurrentLine)).ok();
                println!();
            }
            execute!(stdout, MoveUp(lines_per_render as u16), MoveToColumn(0)).ok();

            render_dashboard(
                &mut stdout,
                deploy_id,
                &cluster_states,
                start_time,
                false,
                allowed_lag_secs,
            )?;
        }

        let all_ready = cluster_states
            .values()
            .all(|ctx| matches!(ctx.status, ClusterDeploymentStatus::Ready));

        execute!(stdout, Show).ok();

        if all_ready {
            println!();
            println!("{}", "  All clusters are ready!".green().bold());
            return Ok(());
        }
    }
}

/// Parse status from string values returned by SUBSCRIBE.
fn parse_status(
    status_str: &str,
    failure_reason: Option<&str>,
    hydrated_count: i64,
    total_count: i64,
    max_lag_secs: i64,
    total_replicas: i64,
    problematic_replicas: i64,
) -> ClusterDeploymentStatus {
    match status_str {
        "ready" => ClusterDeploymentStatus::Ready,
        "hydrating" => ClusterDeploymentStatus::Hydrating {
            hydrated: hydrated_count,
            total: total_count,
        },
        "lagging" => ClusterDeploymentStatus::Lagging { max_lag_secs },
        "failing" => {
            let reason = match failure_reason {
                Some("no_replicas") => FailureReason::NoReplicas,
                Some("all_replicas_problematic") => FailureReason::AllReplicasProblematic {
                    problematic: problematic_replicas,
                    total: total_replicas,
                },
                _ => FailureReason::NoReplicas,
            };
            ClusterDeploymentStatus::Failing { reason }
        }
        _ => ClusterDeploymentStatus::Ready,
    }
}

/// Render the live dashboard.
fn render_dashboard(
    stdout: &mut io::Stdout,
    deploy_id: &str,
    cluster_states: &HashMap<String, ClusterStatusContext>,
    start_time: Instant,
    _is_update: bool,
    allowed_lag_secs: i64,
) -> Result<(), CliError> {
    let elapsed = start_time.elapsed();
    let elapsed_str = format_duration(elapsed);

    // Header
    println!();
    println!(
        "{}",
        format!("  mz-deploy ready · deployment: {}", deploy_id)
            .cyan()
            .bold()
    );
    println!("  {} {}", "elapsed:".dimmed(), elapsed_str.dimmed());
    println!();

    // Sort clusters by name for consistent ordering
    let mut cluster_names: Vec<_> = cluster_states.keys().collect();
    cluster_names.sort();

    // Render each cluster
    for name in cluster_names {
        if let Some(ctx) = cluster_states.get(name) {
            print_cluster_status(ctx, allowed_lag_secs);
        }
    }

    // Summary
    let statuses: Vec<_> = cluster_states.values().cloned().collect();
    print_summary(&statuses);

    stdout.flush().ok();
    Ok(())
}

/// Format a duration as human-readable string.
fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        let mins = secs / 60;
        let secs = secs % 60;
        format!("{}m {}s", mins, secs)
    } else {
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        format!("{}h {}m", hours, mins)
    }
}
