// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Network policies apply command - converge live network policy state to match definitions.

use crate::cli::CliError;
use crate::cli::commands::reconcile::{ObjectKind, ReconcileTarget};
use crate::cli::commands::{comments, grants};
use crate::cli::executor::{
    ApplyPlan, ApplyResult, DeploymentExecutor, ObjectAction, ObjectResult, connect_apply_client,
};
use crate::client::{Client, ObjectComment};
use crate::config::Settings;
use crate::project::network_policies::{self, NetworkPolicyDefinition};
use mz_sql_parser::ast::AlterNetworkPolicyStatement;

const OBJECT_KIND: ObjectKind = ObjectKind::NetworkPolicy;

/// Plan network policy changes without executing or printing.
pub async fn plan(
    settings: &Settings,
    client: &Client,
    executor: &DeploymentExecutor<'_>,
) -> Result<ApplyResult, CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;

    let definitions =
        network_policies::load_network_policies(directory, &profile.name, settings.variables())?;

    if definitions.is_empty() {
        return Ok(ApplyResult {
            phase: "network_policies".to_string(),
            results: vec![],
        });
    }

    let names: Vec<&str> = definitions.iter().map(|def| def.name.as_str()).collect();
    let introspection = client.introspection();
    let (existing, current_comments) = futures::try_join!(
        introspection.existing_network_policies(&names),
        introspection.get_named_object_comments(OBJECT_KIND.catalog_table(), &names),
    )
    .map_err(CliError::Connection)?;

    let mut object_results = Vec::new();
    for def in &definitions {
        let obj_result = plan_network_policy(
            client,
            executor,
            def,
            existing.contains(&def.name),
            current_comments.get(&def.name).map_or(&[], Vec::as_slice),
        )
        .await?;
        object_results.push(obj_result);
    }

    Ok(ApplyResult {
        phase: "network_policies".to_string(),
        results: object_results,
    })
}

/// Run the `network-policies apply` command: plan, render, optionally execute.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<ApplyPlan, CliError> {
    let client = connect_apply_client(settings).await?;
    let executor = DeploymentExecutor::new_dry_run(&client);
    let mut plan_result = ApplyPlan::new();
    let phase = plan(settings, &client, &executor).await?;
    plan_result.add_phase(phase);

    if !dry_run {
        plan_result.execute(&client).await?;
    }

    Ok(plan_result)
}

/// Plan a single network policy definition: create if missing, alter if exists,
/// then plan grants, revocations, and comments.
async fn plan_network_policy(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    def: &NetworkPolicyDefinition,
    exists: bool,
    current_comments: &[ObjectComment],
) -> Result<ObjectResult, CliError> {
    let policy_name = &def.name;

    // Drain any prior statements
    executor.take_statements();

    let action = if exists {
        // ALTER NETWORK POLICY to converge rules
        let alter_stmt = AlterNetworkPolicyStatement {
            name: def.create_stmt.name.clone(),
            options: def.create_stmt.options.clone(),
        };
        executor.execute_sql(&alter_stmt).await?;
        ObjectAction::Altered
    } else {
        executor.execute_sql(&def.create_stmt).await?;
        ObjectAction::Created
    };

    let target = ReconcileTarget::named(OBJECT_KIND, policy_name);
    grants::reconcile(client, executor, &target, &def.grants).await?;
    comments::reconcile(executor, &target, &def.comments, current_comments).await?;

    Ok(ObjectResult {
        object: policy_name.clone(),
        action,
        statements: executor.take_statements(),
        redacted_statements: vec![],
        transaction_group: None,
        post_statements: vec![],
    })
}
