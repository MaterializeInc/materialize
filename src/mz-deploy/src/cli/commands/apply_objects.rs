//! Shared helpers for database-object apply commands.
//!
//! Four apply modules (tables, sources, secrets, connections) share the same
//! high-level flow but differ in per-object handling. This module provides
//! shared orchestration helpers (`prepare_phase`, `run_compiled_phase`, etc.)
//! that each module composes with its own dispatch loop.

use crate::cli::CliError;
use crate::cli::commands::grants;
use crate::cli::executor::{
    ApplyPlan, ApplyResult, DeploymentExecutor, ObjectAction, ObjectResult,
};
use crate::client::Client;
use crate::config::Settings;
use crate::project;
use crate::project::ast::Statement;
use crate::project::object_id::ObjectId;
use crate::project::typed;

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;

/// Result of handling a single object (existing or new).
pub struct HandleResult {
    pub action: ObjectAction,
    pub redacted_statements: Vec<String>,
}

/// Prepared phase data: objects partitioned into existing and new.
pub struct PhaseInput<'a> {
    pub existing_objects: Vec<(ObjectId, &'a typed::DatabaseObject)>,
    pub to_create: Vec<(ObjectId, &'a typed::DatabaseObject)>,
}

/// Shared preparation steps for a database-object phase.
///
/// Filters objects by `matches_fn`, sorts in dependency order, batch-checks
/// existence, partitions into existing/new, and prepares schemas for new objects.
pub async fn prepare_phase<'a>(
    grant_kind: &grants::GrantObjectKind,
    matches_fn: fn(&Statement) -> bool,
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    planned_project: &'a project::planned::Project,
    apply_plan: &mut ApplyPlan,
) -> Result<PhaseInput<'a>, CliError> {
    // 1. Collect target IDs via matches_fn
    let mut target_ids = BTreeSet::new();
    for obj in planned_project.iter_objects() {
        if matches_fn(&obj.typed_object.stmt) {
            target_ids.insert(obj.id.clone());
        }
    }

    // 2. Early return if empty
    if target_ids.is_empty() {
        return Ok(PhaseInput {
            existing_objects: vec![],
            to_create: vec![],
        });
    }

    // 3. Get sorted objects in dependency order
    let target_objects = planned_project.get_sorted_objects_filtered(&target_ids)?;

    // 4. Batch existence check
    let existing = client
        .introspection()
        .check_catalog_objects_exist(&target_ids, grant_kind.catalog_table())
        .await
        .map_err(CliError::Connection)?;

    // 5. Partition into existing_objects / to_create
    let mut existing_objects = Vec::new();
    let mut to_create = Vec::new();
    for (obj_id, typed_obj) in target_objects {
        if existing.contains(&obj_id) {
            existing_objects.push((obj_id, typed_obj));
        } else {
            to_create.push((obj_id, typed_obj));
        }
    }

    // 6. Prepare schemas for new objects (deduplicated via apply_plan)
    let schemas: BTreeSet<_> = to_create
        .iter()
        .map(|(id, _)| project::SchemaQualifier::new(id.database.clone(), id.schema.clone()))
        .collect();
    apply_plan
        .prepare_schemas(executor, planned_project, &schemas)
        .await?;

    Ok(PhaseInput {
        existing_objects,
        to_create,
    })
}

/// Construct an `ObjectResult` from a `HandleResult`.
pub fn to_object_result(
    obj_id: &ObjectId,
    executor: &DeploymentExecutor<'_>,
    hr: HandleResult,
) -> ObjectResult {
    ObjectResult {
        object: format!("{}", obj_id),
        action: hr.action,
        statements: executor.take_statements(),
        redacted_statements: hr.redacted_statements,
    }
}

/// Reconcile grants and apply comments for an object.
pub async fn reconcile_grants_and_comments(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    obj_id: &ObjectId,
    typed_obj: &typed::DatabaseObject,
    grant_kind: &grants::GrantObjectKind,
) -> Result<(), CliError> {
    grants::reconcile(client, executor, obj_id, &typed_obj.grants, grant_kind).await?;
    for comment in &typed_obj.comments {
        executor.execute_sql(comment).await?;
    }
    Ok(())
}

/// Default handler for an existing object: reconcile grants, return `UpToDate`.
pub async fn default_handle_existing(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    obj_id: &ObjectId,
    typed_obj: &typed::DatabaseObject,
    grant_kind: &grants::GrantObjectKind,
) -> Result<HandleResult, CliError> {
    reconcile_grants_and_comments(client, executor, obj_id, typed_obj, grant_kind).await?;
    Ok(HandleResult {
        action: ObjectAction::UpToDate,
        redacted_statements: vec![],
    })
}

/// Default handler for a new object: execute stmt + indexes + grants + comments, return `Created`.
pub async fn default_handle_new(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    obj_id: &ObjectId,
    typed_obj: &typed::DatabaseObject,
    grant_kind: &grants::GrantObjectKind,
) -> Result<HandleResult, CliError> {
    executor.execute_sql(&typed_obj.stmt).await?;
    for index in &typed_obj.indexes {
        executor.execute_sql(index).await?;
    }
    reconcile_grants_and_comments(client, executor, obj_id, typed_obj, grant_kind).await?;
    Ok(HandleResult {
        action: ObjectAction::Created,
        redacted_statements: vec![],
    })
}

/// Run a standalone `apply <phase>` command for database objects:
/// compile project → connect → create executor → call plan_fn → add phase → optionally execute.
pub async fn run_compiled_phase<F>(
    settings: &Settings,
    dry_run: bool,
    plan_fn: F,
) -> Result<ApplyPlan, CliError>
where
    F: for<'a> FnOnce(
        &'a Settings,
        &'a Client,
        &'a DeploymentExecutor<'a>,
        &'a project::planned::Project,
        &'a mut ApplyPlan,
    ) -> Pin<Box<dyn Future<Output = Result<ApplyResult, CliError>> + 'a>>,
{
    let planned_project =
        super::compile::run(settings, true, !crate::log::json_output_enabled()).await?;
    let client = Client::connect_with_profile(settings.connection().clone())
        .await
        .map_err(CliError::Connection)?;

    let mut apply_plan = ApplyPlan::new();
    let executor = DeploymentExecutor::new_dry_run(&client);
    let phase = plan_fn(
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
    }

    Ok(apply_plan)
}
