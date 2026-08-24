// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Roles apply command - converge live role state to match definitions.

use crate::cli::CliError;
use crate::cli::commands::reconcile::{self, ObjectKind, ReconcileTarget};
use crate::cli::executor::{
    ApplyPlan, ApplyResult, DeploymentExecutor, ObjectAction, ObjectResult, connect_apply_client,
};
use crate::client::{Client, CurrentObjectState, ObjectComment, quote_identifier};
use crate::config::Settings;
use crate::project::roles::{self, RoleDefinition};
use itertools::Itertools;
use mz_sql_parser::ast::{
    AlterRoleOption, GrantRoleStatement, Ident, Raw, RevokeRoleStatement, SetRoleVar,
};
use std::collections::BTreeSet;

/// Plan role changes without executing or printing.
pub async fn plan(
    settings: &Settings,
    client: &Client,
    executor: &DeploymentExecutor<'_>,
) -> Result<ApplyResult, CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;

    let definitions = roles::load_roles(directory, &profile.name, settings.variables())?;

    if definitions.is_empty() {
        return Ok(ApplyResult {
            phase: "roles".to_string(),
            results: vec![],
        });
    }

    // Every read the two passes below need, taken up front. The catalog
    // relations are small and each read costs a full round trip, so one read per
    // relation for the whole set beats four reads per role.
    let names: Vec<&str> = definitions.iter().map(|def| def.name.as_str()).collect();
    let introspection = client.introspection();
    let (existing, members, comments, parameters) = futures::try_join!(
        introspection.existing_roles(&names),
        introspection.get_role_members_bulk(&names),
        introspection.get_named_object_comments("mz_roles", &names),
        introspection.get_role_parameters_bulk(&names),
    )
    .map_err(CliError::Connection)?;

    // Pass 1: Create all roles so inter-role GRANT ROLE dependencies are satisfied.
    let mut actions = Vec::new();
    for def in &definitions {
        executor.take_statements();
        let action = if existing.contains(&def.name) {
            ObjectAction::UpToDate
        } else {
            executor.execute_sql(&def.create_stmt).await?;
            ObjectAction::Created
        };
        actions.push((action, executor.take_statements()));
    }

    // Pass 2: Configure each role (ALTER, GRANT, COMMENT, reconcile).
    let mut object_results = Vec::new();
    for (def, (action, create_stmts)) in definitions.iter().zip_eq(actions) {
        executor.take_statements();
        configure_role(
            executor,
            def,
            members.get(&def.name).map_or(&[], Vec::as_slice),
            comments.get(&def.name).map_or(&[], Vec::as_slice),
            parameters.get(&def.name).map_or(&[], Vec::as_slice),
        )
        .await?;
        let mut statements = create_stmts;
        statements.extend(executor.take_statements());
        object_results.push(ObjectResult {
            object: def.name.clone(),
            action: action.with_reconciled(!statements.is_empty()),
            statements,
            redacted_statements: vec![],
            transaction_group: None,
            post_statements: vec![],
        });
    }

    Ok(ApplyResult {
        phase: "roles".to_string(),
        results: object_results,
    })
}

/// Run the `roles apply` command: plan, render, optionally execute.
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

/// Configure a role: ALTER, GRANT, COMMENT statements and reconcile stale grants/params.
async fn configure_role(
    executor: &DeploymentExecutor<'_>,
    def: &RoleDefinition,
    current_members: &[String],
    current_comments: &[ObjectComment],
    current_params: &[String],
) -> Result<(), CliError> {
    let role_name = &def.name;

    // Execute ALTER ROLE statements
    for alter in &def.alter_stmts {
        executor.execute_sql(alter).await?;
    }

    reconcile_members(executor, def, current_members).await?;

    reconcile::grants_and_comments(
        executor,
        &ReconcileTarget::named(ObjectKind::Role, role_name),
        &[],
        &def.comments,
        &CurrentObjectState {
            comments: current_comments.to_vec(),
            ..Default::default()
        },
    )
    .await?;

    // Reset stale session defaults
    let desired_params: BTreeSet<String> = def
        .alter_stmts
        .iter()
        .filter_map(|alter| match &alter.option {
            AlterRoleOption::Variable(SetRoleVar::Set { name, .. }) => {
                Some(name.as_str().to_lowercase())
            }
            _ => None,
        })
        .collect();

    for param in current_params {
        if !desired_params.contains(&param.to_lowercase()) {
            let sql = format!(
                "ALTER ROLE {} RESET {}",
                quote_identifier(role_name),
                quote_identifier(param)
            );
            executor.execute_sql(&sql).await?;
        }
    }

    Ok(())
}

/// Reconcile the role's membership: grant the members it is missing, revoke the
/// ones the project no longer declares.
async fn reconcile_members(
    executor: &DeploymentExecutor<'_>,
    def: &RoleDefinition,
    current_members: &[String],
) -> Result<(), CliError> {
    let role_name = &def.name;
    // Member names are identifiers, so they are compared and emitted exactly.
    // The parser has already folded unquoted names to the casing the catalog
    // stores, and a role created as `"Reader"` cannot be reached as `reader`.
    let current: BTreeSet<&str> = current_members.iter().map(String::as_str).collect();
    let desired: BTreeSet<&str> = def
        .grants
        .iter()
        .flat_map(|g| g.member_names.iter().map(|m| m.as_str()))
        .collect();

    let role = Ident::new_unchecked(role_name);

    for member in desired.difference(&current) {
        let stmt = GrantRoleStatement::<Raw> {
            role_names: vec![role.clone()],
            member_names: vec![Ident::new_unchecked(*member)],
        };
        executor.execute_sql(&stmt).await?;
    }

    for member in current.difference(&desired) {
        let stmt = RevokeRoleStatement::<Raw> {
            role_names: vec![role.clone()],
            member_names: vec![Ident::new_unchecked(*member)],
        };
        executor.execute_sql(&stmt).await?;
    }

    Ok(())
}
