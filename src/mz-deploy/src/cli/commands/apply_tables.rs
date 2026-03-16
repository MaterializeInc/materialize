//! Apply tables command - create tables that don't exist in the database.

use crate::cli::CliError;
use crate::cli::commands::apply_objects;
use crate::cli::commands::grants;
use crate::cli::executor::{
    ApplyPlan, ApplyResult, DeploymentExecutor, ObjectAction, ObjectResult,
    compile_apply_project_and_connect,
};
use crate::client::Client;
use crate::config::Settings;
use crate::project;
use crate::project::ast::Statement;
use std::collections::BTreeSet;

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
    let mut target_ids = BTreeSet::new();
    for obj in planned_project.iter_objects() {
        if matches(&obj.typed_object.stmt) {
            target_ids.insert(obj.id.clone());
        }
    }

    if target_ids.is_empty() {
        return Ok(ApplyResult {
            phase: PHASE_NAME.to_string(),
            results: vec![],
        });
    }

    let target_objects = planned_project.get_sorted_objects_filtered(&target_ids)?;
    let existing = client
        .introspection()
        .check_catalog_objects_exist(&target_ids, GRANT_KIND.catalog_table())
        .await
        .map_err(CliError::Connection)?;

    let schemas: BTreeSet<_> = target_objects
        .iter()
        .filter(|(obj_id, _)| !existing.contains(obj_id))
        .map(|(obj_id, _)| {
            project::SchemaQualifier::new(obj_id.database.clone(), obj_id.schema.clone())
        })
        .collect();
    apply_plan
        .prepare_schemas(executor, planned_project, &schemas)
        .await?;

    let mut results = Vec::new();

    for (obj_id, typed_obj) in target_objects {
        executor.take_statements();

        let action = if existing.contains(&obj_id) {
            apply_objects::reconcile_grants_and_comments(
                client,
                executor,
                &obj_id,
                typed_obj,
                &GRANT_KIND,
            )
            .await?;
            ObjectAction::UpToDate
        } else {
            executor.execute_sql(&typed_obj.stmt).await?;
            for index in &typed_obj.indexes {
                executor.execute_sql(index).await?;
            }
            apply_objects::reconcile_grants_and_comments(
                client,
                executor,
                &obj_id,
                typed_obj,
                &GRANT_KIND,
            )
            .await?;
            ObjectAction::Created
        };

        results.push(ObjectResult {
            object: obj_id.to_string(),
            action,
            statements: executor.take_statements(),
            redacted_statements: vec![],
        });
    }

    Ok(ApplyResult {
        phase: PHASE_NAME.to_string(),
        results,
    })
}

/// Run the `apply tables` command: compile, plan, optionally execute, then lock.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<ApplyPlan, CliError> {
    let (planned_project, client) = compile_apply_project_and_connect(settings).await?;
    let mut apply_plan = ApplyPlan::new();
    let executor = DeploymentExecutor::new_dry_run(&client);
    let phase = self::plan(
        settings,
        &client,
        &executor,
        &planned_project,
        &mut apply_plan,
    )
    .await?;
    apply_plan.add_phase(phase);

    if !dry_run {
        apply_plan.execute(&client).await?;
        super::lock::run(settings).await?;
    }

    Ok(apply_plan)
}
