//! Roles apply command - converge live role state to match definitions.

use crate::cli::CliError;
use crate::cli::executor::{ApplyResult, DeploymentExecutor, ObjectResult};
use crate::client::Client;
use crate::client::quote_identifier;
use crate::config::Settings;
use crate::project::roles::{self, RoleDefinition};
use mz_sql_parser::ast::AlterRoleOption;
use mz_sql_parser::ast::SetRoleVar;
use std::collections::BTreeSet;

/// Plan role changes without executing or printing.
pub async fn plan(settings: &Settings, client: &Client) -> Result<ApplyResult, CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;

    let definitions = roles::load_roles(directory, &profile.name, settings.variables())?;

    if definitions.is_empty() {
        return Ok(ApplyResult {
            phase: "roles".to_string(),
            setup_statements: vec![],
            results: vec![],
        });
    }

    let executor = DeploymentExecutor::new_dry_run(client);

    let mut object_results = Vec::new();
    for def in &definitions {
        let obj_result = plan_role(client, &executor, def).await?;
        object_results.push(obj_result);
    }

    Ok(ApplyResult {
        phase: "roles".to_string(),
        setup_statements: vec![],
        results: object_results,
    })
}

/// Run the `roles apply` command: plan, render, optionally execute.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<ApplyResult, CliError> {
    let client = Client::connect_with_profile(settings.connection().clone())
        .await
        .map_err(CliError::Connection)?;

    let result = plan(settings, &client).await?;

    if !dry_run {
        result.execute(&client).await?;
    }

    Ok(result)
}

/// Plan a single role definition: create if missing, then plan
/// ALTER, GRANT, REVOKE, RESET, and COMMENT statements.
async fn plan_role(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    def: &RoleDefinition,
) -> Result<ObjectResult, CliError> {
    let role_name = &def.name;

    // Drain any prior statements
    executor.take_statements();

    // Check if role already exists
    let exists = client
        .introspection()
        .role_exists(role_name)
        .await
        .map_err(CliError::Connection)?;

    let action = if exists {
        "up_to_date"
    } else {
        executor.execute_sql(&def.create_stmt).await?;
        "created"
    };

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

    Ok(ObjectResult {
        object: role_name.clone(),
        action: action.to_string(),
        statements: executor.take_statements(),
        redacted_statements: vec![],
    })
}
