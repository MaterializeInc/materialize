// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Apply-all orchestrator — runs all infrastructure apply steps in dependency order.
//!
//! Dependency order: clusters → roles → network policies → secrets → connections → sources → tables.
//!
//! **Key Insight:** The ordering ensures referential integrity — clusters must
//! exist before MVs can reference them, connections must exist before sources
//! and sinks can use them, and sources must exist before `CREATE TABLE FROM
//! SOURCE` can reference them. On partial failure, objects created in earlier
//! phases remain in place (all operations are idempotent) and re-running
//! `apply-all` will skip already-existing objects and retry only the failed
//! phase onward.
//!
//! **Requires:** `materialize_deployer` role (enforced before any phase runs).

use crate::cli::CliError;
use crate::cli::executor::{ApplyPlan, DeploymentExecutor, compile_apply_project_and_connect};
use crate::config::Settings;

/// Run all infrastructure apply steps in dependency order.
///
/// Plans all phases first with a shared client, then executes if not dry-run.
/// Applies: clusters → roles → network policies → secrets (unless skipped) → connections → sources → tables.
pub async fn run(
    settings: &Settings,
    skip_secrets: bool,
    dry_run: bool,
) -> Result<ApplyPlan, CliError> {
    let (planned_project, client) = compile_apply_project_and_connect(settings).await?;

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

    let phase =
        super::apply_connections::plan(settings, &client, &executor, &planned_project, &mut plan)
            .await?;
    plan.add_phase(phase);

    let phase =
        super::apply_sources::plan(settings, &client, &executor, &planned_project, &mut plan)
            .await?;
    plan.add_phase(phase);

    let phase =
        super::apply_tables::plan(settings, &client, &executor, &planned_project, &mut plan)
            .await?;
    plan.add_phase(phase);

    if !dry_run {
        plan.execute(&client).await?;
        super::lock::run(settings).await?;
    }

    Ok(plan)
}
