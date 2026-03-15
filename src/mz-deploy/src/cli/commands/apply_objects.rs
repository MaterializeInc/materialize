//! Generic trait and orchestration for database-object apply commands.
//!
//! Four apply modules (tables, sources, secrets, connections) share the same
//! high-level flow but differ in per-object handling. The [`DatabaseObjectPhase`]
//! trait captures these differences so each module is a small, self-documenting
//! impl while [`plan`] and [`run`] handle all shared orchestration.

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

/// Result of handling a single object (existing or new).
pub struct HandleResult {
    pub action: ObjectAction,
    pub redacted_statements: Vec<String>,
}

/// Trait for database object types managed via `apply`.
///
/// Implementors define which statements they own and how to handle
/// existing vs new objects. The generic [`plan()`] handles all shared
/// orchestration: filtering, batch existence checking, partitioning,
/// schema preparation, and result collection.
#[allow(async_fn_in_trait)]
pub trait DatabaseObjectPhase: Sized {
    const PHASE_NAME: &'static str;
    const GRANT_KIND: grants::GrantObjectKind;

    /// Construct per-invocation state (e.g., SecretResolver).
    fn new(settings: &Settings) -> Result<Self, CliError>;

    /// Returns true if this statement belongs to this object type.
    fn matches(stmt: &Statement) -> bool;

    /// Handle an object that already exists.
    ///
    /// Default: reconcile grants, return `UpToDate`.
    async fn handle_existing(
        &self,
        client: &Client,
        executor: &DeploymentExecutor<'_>,
        obj_id: &ObjectId,
        typed_obj: &typed::DatabaseObject,
    ) -> Result<HandleResult, CliError> {
        reconcile_grants_and_comments::<Self>(client, executor, obj_id, typed_obj).await?;
        Ok(HandleResult {
            action: ObjectAction::UpToDate,
            redacted_statements: vec![],
        })
    }

    /// Handle a new object.
    ///
    /// Default: execute stmt + indexes + grants + comments, return `Created`.
    async fn handle_new(
        &self,
        client: &Client,
        executor: &DeploymentExecutor<'_>,
        obj_id: &ObjectId,
        typed_obj: &typed::DatabaseObject,
    ) -> Result<HandleResult, CliError> {
        executor.execute_sql(&typed_obj.stmt).await?;
        for index in &typed_obj.indexes {
            executor.execute_sql(index).await?;
        }
        reconcile_grants_and_comments::<Self>(client, executor, obj_id, typed_obj).await?;
        Ok(HandleResult {
            action: ObjectAction::Created,
            redacted_statements: vec![],
        })
    }
}

/// Reconcile grants and apply comments for an object.
///
/// Shared tail logic used by both default and custom `handle_existing`/`handle_new`
/// implementations.
pub async fn reconcile_grants_and_comments<K: DatabaseObjectPhase>(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    obj_id: &ObjectId,
    typed_obj: &typed::DatabaseObject,
) -> Result<(), CliError> {
    grants::reconcile(client, executor, obj_id, &typed_obj.grants, &K::GRANT_KIND).await?;
    for comment in &typed_obj.comments {
        executor.execute_sql(comment).await?;
    }
    Ok(())
}

/// Plan database object changes using the given phase type.
///
/// Handles: filtering by statement type, batch existence checking,
/// partitioning into existing/new, schema preparation, and per-object
/// dispatch to `handle_existing`/`handle_new`.
pub async fn plan<K: DatabaseObjectPhase>(
    settings: &Settings,
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    planned_project: &project::planned::Project,
    apply_plan: &mut ApplyPlan,
) -> Result<ApplyResult, CliError> {
    let phase = K::new(settings)?;

    // 1. Collect target IDs via K::matches()
    let mut target_ids = BTreeSet::new();
    for obj in planned_project.iter_objects() {
        if K::matches(&obj.typed_object.stmt) {
            target_ids.insert(obj.id.clone());
        }
    }

    // 2. Early return if empty
    if target_ids.is_empty() {
        return Ok(ApplyResult {
            phase: K::PHASE_NAME.to_string(),
            results: vec![],
        });
    }

    // 3. Get sorted objects in dependency order
    let target_objects = planned_project.get_sorted_objects_filtered(&target_ids)?;

    // 4. Batch existence check
    let existing = client
        .introspection()
        .check_catalog_objects_exist(&target_ids, K::GRANT_KIND.catalog_table())
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

    let mut object_results = Vec::new();

    // 7. Handle existing objects
    for (obj_id, typed_obj) in &existing_objects {
        executor.take_statements();
        let result = phase
            .handle_existing(client, executor, obj_id, typed_obj)
            .await?;
        object_results.push(ObjectResult {
            object: format!("{}", obj_id),
            action: result.action,
            statements: executor.take_statements(),
            redacted_statements: result.redacted_statements,
        });
    }

    // 8. Handle new objects
    for (obj_id, typed_obj) in &to_create {
        executor.take_statements();
        let result = phase
            .handle_new(client, executor, obj_id, typed_obj)
            .await?;
        object_results.push(ObjectResult {
            object: format!("{}", obj_id),
            action: result.action,
            statements: executor.take_statements(),
            redacted_statements: result.redacted_statements,
        });
    }

    Ok(ApplyResult {
        phase: K::PHASE_NAME.to_string(),
        results: object_results,
    })
}

/// Run a standalone `apply <phase>` command: compile, plan, optionally execute.
pub async fn run<K: DatabaseObjectPhase>(
    settings: &Settings,
    dry_run: bool,
) -> Result<ApplyPlan, CliError> {
    let planned_project =
        super::compile::run(settings, true, !crate::log::json_output_enabled()).await?;
    let client = Client::connect_with_profile(settings.connection().clone())
        .await
        .map_err(CliError::Connection)?;

    let mut apply_plan = ApplyPlan::new();
    let executor = DeploymentExecutor::new_dry_run(&client);
    let phase = plan::<K>(
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
