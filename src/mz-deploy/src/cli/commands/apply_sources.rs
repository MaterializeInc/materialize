// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Apply sources command - create sources that don't exist in the database.

use crate::cli::CliError;
use crate::cli::commands::reconcile::{self, ObjectKind};
use crate::cli::executor::{
    ApplyPlan, ApplyResult, DeploymentExecutor, ObjectAction, ObjectResult,
    compile_apply_project_and_connect,
};
use crate::client::Client;
use crate::config::Settings;
use crate::project;
use crate::project::ast::Statement;
use std::collections::BTreeSet;

const PHASE_NAME: &str = "sources";
const OBJECT_KIND: ObjectKind = ObjectKind::Source;

fn matches(stmt: &Statement) -> bool {
    matches!(stmt, Statement::CreateSource(_))
}

/// Plan only source objects (no deployment tracking, no execution).
pub async fn plan(
    _settings: &Settings,
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    planned_project: &project::ir::graph::Project,
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
    let (existing, reconcile_state) = futures::try_join!(
        async {
            client
                .introspection()
                .check_catalog_objects_exist(&target_ids, OBJECT_KIND.catalog_object_type())
                .await
                .map_err(CliError::Connection)
        },
        reconcile::ReconcileState::for_database_objects(client, OBJECT_KIND, &target_ids),
    )?;

    // Every schema this phase manages, not just the ones hosting a missing
    // object. `prepare_schemas` creates only what is absent, and reconciles the
    // database and schema configuration declared in mod files on every apply, so
    // drift there is closed even when no object needs creating.
    let schemas: BTreeSet<_> = target_objects
        .iter()
        .map(|(obj_id, _)| {
            project::SchemaQualifier::new(
                obj_id.expect_database().to_string(),
                obj_id.schema().to_string(),
            )
        })
        .collect();
    apply_plan
        .prepare_schemas(executor, planned_project, &schemas)
        .await?;

    let mut results = Vec::new();

    for (obj_id, typed_obj) in target_objects {
        executor.take_statements();

        let action = if existing.contains(&obj_id) {
            reconcile::database_object(executor, &obj_id, typed_obj, OBJECT_KIND, &reconcile_state)
                .await?;
            ObjectAction::UpToDate
        } else {
            executor.execute_sql(&typed_obj.stmt).await?;
            for index in &typed_obj.indexes {
                executor.execute_sql(index).await?;
            }
            reconcile::database_object(executor, &obj_id, typed_obj, OBJECT_KIND, &reconcile_state)
                .await?;
            ObjectAction::Created
        };

        let statements = executor.take_statements();
        results.push(ObjectResult {
            object: obj_id.to_string(),
            action: action.with_reconciled(!statements.is_empty()),
            statements,
            redacted_statements: vec![],
            transaction_group: None,
            post_statements: vec![],
        });
    }

    Ok(ApplyResult {
        phase: PHASE_NAME.to_string(),
        results,
    })
}

/// Run the `apply sources` command: compile, plan, optionally execute.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<ApplyPlan, CliError> {
    let (planned_project, client) = compile_apply_project_and_connect(settings).await?;
    let mut apply_plan = ApplyPlan::new();
    let executor = DeploymentExecutor::new_dry_run(&client);
    let phase = plan(
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
