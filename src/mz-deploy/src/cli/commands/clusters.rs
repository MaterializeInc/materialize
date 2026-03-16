//! Clusters apply command - converge live cluster state to match definitions.

use crate::cli::CliError;
use crate::cli::commands::grants;
use crate::cli::executor::{
    ApplyPlan, ApplyResult, DeploymentExecutor, ObjectAction, ObjectResult, connect_apply_client,
};
use crate::client::{Client, ClusterOptions, quote_identifier};
use crate::config::Settings;
use crate::project::clusters::{self, ClusterDefinition, extract_replication_factor, extract_size};

/// Plan cluster changes without executing or printing.
pub async fn plan(
    settings: &Settings,
    client: &Client,
    executor: &DeploymentExecutor<'_>,
) -> Result<ApplyResult, CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;

    let definitions = clusters::load_clusters(
        directory,
        &profile.name,
        settings.profile_suffix(),
        settings.variables(),
    )?;

    if definitions.is_empty() {
        return Ok(ApplyResult {
            phase: "clusters".to_string(),
            results: vec![],
        });
    }

    let mut object_results = Vec::new();
    for def in &definitions {
        let obj_result = plan_cluster(client, executor, def).await?;
        object_results.push(obj_result);
    }

    Ok(ApplyResult {
        phase: "clusters".to_string(),
        results: object_results,
    })
}

/// Run the `clusters apply` command: plan, render, optionally execute.
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

/// Plan a single cluster definition: create if missing, alter if drifted,
/// then plan grants, revocations, and comments.
async fn plan_cluster(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    def: &ClusterDefinition,
) -> Result<ObjectResult, CliError> {
    let cluster_name = &def.name;

    // Drain any prior statements
    executor.take_statements();

    // Check if cluster already exists
    let existing = client
        .introspection()
        .get_cluster(cluster_name)
        .await
        .map_err(CliError::Connection)?;

    let action = match existing {
        None => {
            executor.execute_sql(&def.create_stmt).await?;
            ObjectAction::Created
        }
        Some(existing_cluster) => {
            let desired_size = extract_size(&def.create_stmt);
            let desired_rf = extract_replication_factor(&def.create_stmt);

            let needs_alter = {
                let size_differs = desired_size.as_deref() != existing_cluster.size.as_deref();
                let rf_differs = desired_rf.map(i64::from) != existing_cluster.replication_factor;
                size_differs || rf_differs
            };

            if needs_alter {
                let size = desired_size.unwrap_or_else(|| {
                    existing_cluster
                        .size
                        .clone()
                        .unwrap_or_else(|| "25cc".to_string())
                });
                let rf = desired_rf.unwrap_or_else(|| {
                    existing_cluster
                        .replication_factor
                        .unwrap_or(1)
                        .try_into()
                        .unwrap_or(1)
                });

                let options = ClusterOptions {
                    size,
                    replication_factor: rf,
                };
                let alter_sql = format!(
                    "ALTER CLUSTER {} SET (SIZE = '{}', REPLICATION FACTOR = {})",
                    quote_identifier(cluster_name),
                    options.size,
                    options.replication_factor
                );
                executor.execute_sql(&alter_sql).await?;
                ObjectAction::Altered
            } else {
                ObjectAction::UpToDate
            }
        }
    };

    // Reconcile grants
    grants::reconcile_named_object(
        client,
        executor,
        cluster_name,
        &def.grants,
        &grants::GrantNamedObjectKind::Cluster,
    )
    .await?;

    // Execute COMMENT statements
    for comment in &def.comments {
        executor.execute_sql(comment).await?;
    }

    Ok(ObjectResult {
        object: cluster_name.clone(),
        action,
        statements: executor.take_statements(),
        redacted_statements: vec![],
        transaction_group: None,
    })
}
