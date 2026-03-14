//! Apply-all orchestrator — runs all infrastructure apply steps in dependency order.
//!
//! Dependency order: clusters → roles → network policies → secrets → connections → sources → tables.

use crate::cli::CliError;
use crate::cli::executor::{ApplyPlan, DeploymentExecutor};
use crate::client::Client;
use crate::config::Settings;
use crate::log;

/// Run all infrastructure apply steps in dependency order.
///
/// Plans all phases first with a shared client, then executes if not dry-run.
/// Applies: clusters → roles → network policies → secrets (unless skipped) → connections → sources → tables.
pub async fn run(
    settings: &Settings,
    skip_secrets: bool,
    dry_run: bool,
) -> Result<ApplyPlan, CliError> {
    let show_progress = !log::json_output_enabled();
    let planned_project = super::compile::run(settings, true, show_progress).await?;
    let client = Client::connect_with_profile(settings.connection().clone())
        .await
        .map_err(CliError::Connection)?;

    let mut plan = ApplyPlan::new();
    let executor = DeploymentExecutor::new_dry_run(&client);

    // Infrastructure phases (no schemas needed)
    plan.add_phase(super::clusters::plan(settings, &client, &executor).await?);
    plan.add_phase(super::roles::plan(settings, &client, &executor).await?);
    plan.add_phase(super::apply_network_policies::plan(settings, &client, &executor).await?);

    // Database object phases (schemas deduplicated via plan)
    if !skip_secrets {
        let phase =
            super::apply_secrets::plan(settings, &client, &executor, &planned_project, &mut plan)
                .await?;
        plan.add_phase(phase);
    }

    let phase = super::apply_connections::plan(
        settings,
        &client,
        &executor,
        &planned_project,
        &mut plan,
    )
    .await?;
    plan.add_phase(phase);

    let phase = super::apply_sources::plan(
        settings,
        &client,
        &executor,
        &planned_project,
        &mut plan,
    )
    .await?;
    plan.add_phase(phase);

    let phase = super::apply_tables::plan(
        settings,
        &client,
        &executor,
        &planned_project,
        &mut plan,
    )
    .await?;
    plan.add_phase(phase);

    if !dry_run {
        plan.execute(&client).await?;
        super::lock::run(settings).await?;
    }

    Ok(plan)
}
