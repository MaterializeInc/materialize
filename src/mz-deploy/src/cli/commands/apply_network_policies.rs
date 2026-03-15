//! Network policies apply command - converge live network policy state to match definitions.

use crate::cli::CliError;
use crate::cli::commands::grants;
use crate::cli::executor::{
    ApplyPlan, ApplyResult, DeploymentExecutor, ObjectAction, ObjectResult,
};
use crate::client::Client;
use crate::config::Settings;
use crate::project::network_policies::{self, NetworkPolicyDefinition};
use mz_sql_parser::ast::AlterNetworkPolicyStatement;

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

    let mut object_results = Vec::new();
    for def in &definitions {
        let obj_result = plan_network_policy(client, executor, def).await?;
        object_results.push(obj_result);
    }

    Ok(ApplyResult {
        phase: "network_policies".to_string(),
        results: object_results,
    })
}

/// Run the `network-policies apply` command: plan, render, optionally execute.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<ApplyPlan, CliError> {
    let client = Client::connect_with_profile(settings.connection().clone())
        .await
        .map_err(CliError::Connection)?;

    let mut plan = ApplyPlan::new();
    let executor = DeploymentExecutor::new_dry_run(&client);
    plan.add_phase(self::plan(settings, &client, &executor).await?);

    if !dry_run {
        plan.execute(&client).await?;
    }

    Ok(plan)
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
        ObjectAction::Altered
    } else {
        executor.execute_sql(&def.create_stmt).await?;
        ObjectAction::Created
    };

    // Reconcile grants
    grants::reconcile_named_object(
        client,
        executor,
        policy_name,
        &def.grants,
        &grants::GrantNamedObjectKind::NetworkPolicy,
    )
    .await?;

    // Execute COMMENT statements
    for comment in &def.comments {
        executor.execute_sql(comment).await?;
    }

    Ok(ObjectResult {
        object: policy_name.clone(),
        action,
        statements: executor.take_statements(),
        redacted_statements: vec![],
    })
}
