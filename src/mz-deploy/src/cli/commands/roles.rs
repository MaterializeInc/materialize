//! Roles apply command - converge live role state to match definitions.

use crate::cli::CliError;
use crate::cli::executor::{
    ApplyPlan, ApplyResult, DeploymentExecutor, ObjectAction, ObjectResult, connect_apply_client,
};
use crate::client::Client;
use crate::client::quote_identifier;
use crate::config::Settings;
use crate::project::roles::{self, RoleDefinition};
use itertools::Itertools;
use mz_sql_parser::ast::AlterRoleOption;
use mz_sql_parser::ast::SetRoleVar;
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

    // Pass 1: Create all roles so inter-role GRANT ROLE dependencies are satisfied.
    let mut actions = Vec::new();
    for def in &definitions {
        executor.take_statements();
        let action = create_role(client, executor, def).await?;
        actions.push((action, executor.take_statements()));
    }

    // Pass 2: Configure each role (ALTER, GRANT, COMMENT, reconcile).
    let mut object_results = Vec::new();
    for (def, (action, create_stmts)) in definitions.iter().zip_eq(actions) {
        executor.take_statements();
        configure_role(client, executor, def).await?;
        let mut statements = create_stmts;
        statements.extend(executor.take_statements());
        object_results.push(ObjectResult {
            object: def.name.clone(),
            action,
            statements,
            redacted_statements: vec![],
            transaction_group: None,
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
    let phase = self::plan(settings, &client, &executor).await?;
    plan_result.add_phase(phase);

    if !dry_run {
        plan_result.execute(&client).await?;
    }

    Ok(plan_result)
}

/// Create a role if it doesn't already exist.
async fn create_role(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    def: &RoleDefinition,
) -> Result<ObjectAction, CliError> {
    let exists = client
        .introspection()
        .role_exists(&def.name)
        .await
        .map_err(CliError::Connection)?;

    if exists {
        Ok(ObjectAction::UpToDate)
    } else {
        executor.execute_sql(&def.create_stmt).await?;
        Ok(ObjectAction::Created)
    }
}

/// Configure a role: ALTER, GRANT, COMMENT statements and reconcile stale grants/params.
async fn configure_role(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    def: &RoleDefinition,
) -> Result<(), CliError> {
    let role_name = &def.name;

    // Execute ALTER ROLE statements
    for alter in &def.alter_stmts {
        executor.execute_sql(alter).await?;
    }

    // Execute GRANT ROLE statements
    for grant in &def.grants {
        executor.execute_sql(grant).await?;
    }

    // Execute COMMENT statements
    for comment in &def.comments {
        executor.execute_sql(comment).await?;
    }

    // Revoke stale grants
    let current_members = client
        .introspection()
        .get_role_members(role_name)
        .await
        .map_err(CliError::Connection)?;

    let desired_members: BTreeSet<String> = def
        .grants
        .iter()
        .flat_map(|g| g.member_names.iter().map(|m| m.as_str().to_lowercase()))
        .collect();

    for member in &current_members {
        if !desired_members.contains(&member.to_lowercase()) {
            let sql = format!(
                "REVOKE {} FROM {}",
                quote_identifier(role_name),
                quote_identifier(member)
            );
            executor.execute_sql(&sql).await?;
        }
    }

    // Reset stale session defaults
    let current_params = client
        .introspection()
        .get_role_parameters(role_name)
        .await
        .map_err(CliError::Connection)?;

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

    for param in &current_params {
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
