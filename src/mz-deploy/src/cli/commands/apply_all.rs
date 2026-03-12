//! Apply-all orchestrator — runs all infrastructure apply steps in dependency order.
//!
//! Dependency order: clusters → roles → network policies → secrets → connections → sources → tables.

use crate::cli::CliError;
use crate::cli::executor::{ApplyAllResult, ApplyResult};
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
) -> Result<ApplyAllResult, CliError> {
    let show_progress = !log::json_output_enabled();
    let planned_project = super::compile::run(settings, true, show_progress).await?;
    let client = Client::connect_with_profile(settings.connection().clone())
        .await
        .map_err(CliError::Connection)?;

    let mut results = Vec::new();

    results.push(super::clusters::plan(settings, &client).await?);
    results.push(super::roles::plan(settings, &client).await?);
    results.push(super::apply_network_policies::plan(settings, &client).await?);

    if skip_secrets {
        results.push(ApplyResult {
            phase: "secrets".to_string(),
            setup_statements: vec![],
            results: vec![],
        });
    } else {
        results.push(super::apply_secrets::plan(settings, &client, &planned_project).await?);
    }

    results.push(super::apply_connections::plan(settings, &client, &planned_project).await?);
    results.push(super::apply_tables::plan_sources(settings, &client, &planned_project).await?);
    results.push(super::apply_tables::plan_tables(settings, &client, &planned_project).await?);

    let all = ApplyAllResult { results };

    if !dry_run {
        all.execute(&client).await?;
        super::lock::run(settings).await?;
    }

    Ok(all)
}
