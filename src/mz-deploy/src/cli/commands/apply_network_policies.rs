//! Network policies apply command - converge live network policy state to match definitions.

use crate::cli::CliError;
use crate::cli::commands::grants;
use crate::cli::executor::{ApplyResult, DeploymentExecutor, ObjectResult};
use crate::client::{Client, quote_identifier};
use crate::config::Settings;
use crate::project::network_policies::{self, NetworkPolicyDefinition};
use mz_sql_parser::ast::AlterNetworkPolicyStatement;
use std::collections::BTreeSet;

/// Plan network policy changes without executing or printing.
pub async fn plan(settings: &Settings, client: &Client) -> Result<ApplyResult, CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;

    let definitions =
        network_policies::load_network_policies(directory, &profile.name, settings.variables())?;

    if definitions.is_empty() {
        return Ok(ApplyResult {
            phase: "network_policies".to_string(),
            setup_statements: vec![],
            results: vec![],
        });
    }

    let executor = DeploymentExecutor::new_dry_run(client);

    let mut object_results = Vec::new();
    for def in &definitions {
        let obj_result = plan_network_policy(client, &executor, def).await?;
        object_results.push(obj_result);
    }

    Ok(ApplyResult {
        phase: "network_policies".to_string(),
        setup_statements: vec![],
        results: object_results,
    })
}

/// Run the `network-policies apply` command: plan, render, optionally execute.
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

/// Plan a single network policy definition: create if missing, alter if exists,
/// then plan grants, revocations, and comments.
async fn plan_network_policy(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    def: &NetworkPolicyDefinition,
) -> Result<ObjectResult, CliError> {
    let policy_name = &def.name;

    // Drain any prior statements
    executor.take_statements();

    // Check if network policy already exists
    let exists = client
        .introspection()
        .network_policy_exists(policy_name)
        .await
        .map_err(CliError::Connection)?;

    let action = if exists {
        // ALTER NETWORK POLICY to converge rules
        let alter_stmt = AlterNetworkPolicyStatement {
            name: def.create_stmt.name.clone(),
            options: def.create_stmt.options.clone(),
        };
        executor.execute_sql(&alter_stmt).await?;
        "altered"
    } else {
        executor.execute_sql(&def.create_stmt).await?;
        "created"
    };

    // Execute GRANT statements
    for grant in &def.grants {
        executor.execute_sql(grant).await?;
    }

    // Revoke stale grants (protecting default privilege grants)
    let current_grants = client
        .introspection()
        .get_network_policy_grants(policy_name)
        .await
        .map_err(CliError::Connection)?;
    let default_privs = client
        .introspection()
        .get_default_privilege_grants_for_network_policy(policy_name)
        .await
        .map_err(CliError::Connection)?;
    let protected: BTreeSet<_> = default_privs
        .iter()
        .map(|g| (g.grantee.to_lowercase(), g.privilege_type.to_uppercase()))
        .collect();
    let desired = grants::desired_grants(&def.grants, &["USAGE"]);
    let revocations = grants::stale_grant_revocations(
        &current_grants,
        &desired,
        &protected,
        "NETWORK POLICY",
        &quote_identifier(policy_name),
    );
    grants::execute_revocations(executor, &revocations, "network policy", &policy_name).await?;

    // Execute COMMENT statements
    for comment in &def.comments {
        executor.execute_sql(comment).await?;
    }

    Ok(ObjectResult {
        object: policy_name.clone(),
        action: action.to_string(),
        statements: executor.take_statements(),
        redacted_statements: vec![],
    })
}
