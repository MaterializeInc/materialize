//! Apply tables command - create tables that don't exist in the database.

use crate::cli::CliError;
use crate::cli::commands::apply_objects;
use crate::cli::commands::grants;
use crate::cli::executor::{ApplyPlan, ApplyResult, DeploymentExecutor};
use crate::client::Client;
use crate::config::Settings;
use crate::project;
use crate::project::ast::Statement;

const PHASE_NAME: &str = "tables";
const GRANT_KIND: grants::GrantObjectKind = grants::GrantObjectKind::Table;

fn matches(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateTable(_) | Statement::CreateTableFromSource(_)
    )
}

/// Plan only table objects (no deployment tracking, no execution).
pub async fn plan(
    _settings: &Settings,
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    planned_project: &project::planned::Project,
    apply_plan: &mut ApplyPlan,
) -> Result<ApplyResult, CliError> {
    let input = apply_objects::prepare_phase(
        &GRANT_KIND,
        matches,
        client,
        executor,
        planned_project,
        apply_plan,
    )
    .await?;

    let mut results = Vec::new();

    for (obj_id, typed_obj) in &input.existing_objects {
        executor.take_statements();
        let hr = apply_objects::default_handle_existing(
            client,
            executor,
            obj_id,
            typed_obj,
            &GRANT_KIND,
        )
        .await?;
        results.push(apply_objects::to_object_result(obj_id, executor, hr));
    }

    for (obj_id, typed_obj) in &input.to_create {
        executor.take_statements();
        let hr =
            apply_objects::default_handle_new(client, executor, obj_id, typed_obj, &GRANT_KIND)
                .await?;
        results.push(apply_objects::to_object_result(obj_id, executor, hr));
    }

    Ok(ApplyResult {
        phase: PHASE_NAME.to_string(),
        results,
    })
}

/// Run the `apply tables` command: compile, plan, optionally execute, then lock.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<ApplyPlan, CliError> {
    let plan = apply_objects::run_compiled_phase(settings, dry_run, |s, c, e, pp, ap| {
        Box::pin(self::plan(s, c, e, pp, ap))
    })
    .await?;
    if !dry_run {
        super::lock::run(settings).await?;
    }
    Ok(plan)
}
