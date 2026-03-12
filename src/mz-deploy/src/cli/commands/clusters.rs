//! Clusters apply command - converge live cluster state to match definitions.

use crate::cli::CliError;
use crate::cli::commands::grants;
use crate::cli::executor::{ApplyResult, DeploymentExecutor, ObjectResult};
use crate::client::{Client, ClusterOptions, quote_identifier};
use crate::config::Settings;
use crate::project::clusters::{self, ClusterDefinition, extract_replication_factor, extract_size};
use std::collections::BTreeSet;

/// Plan cluster changes without executing or printing.
pub async fn plan(settings: &Settings, client: &Client) -> Result<ApplyResult, CliError> {
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
            setup_statements: vec![],
            results: vec![],
        });
    }

    let executor = DeploymentExecutor::new_dry_run(client);

    let mut object_results = Vec::new();
    for def in &definitions {
        let obj_result = plan_cluster(client, &executor, def).await?;
        object_results.push(obj_result);
    }

    Ok(ApplyResult {
        phase: "clusters".to_string(),
        setup_statements: vec![],
        results: object_results,
    })
}

/// Run the `clusters apply` command: plan, render, optionally execute.
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
            "created"
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
                "altered"
            } else {
                "up_to_date"
            }
        }
    };

    // Execute GRANT statements
    for grant in &def.grants {
        executor.execute_sql(grant).await?;
    }

    // Revoke stale grants (protecting default privilege grants)
    let current_grants = client
        .introspection()
        .get_cluster_grants(cluster_name)
        .await
        .map_err(CliError::Connection)?;
    let default_privs = client
        .introspection()
        .get_default_privilege_grants_for_cluster(cluster_name)
        .await
        .map_err(CliError::Connection)?;
    let protected: BTreeSet<_> = default_privs
        .iter()
        .map(|g| (g.grantee.to_lowercase(), g.privilege_type.to_uppercase()))
        .collect();
    let desired = grants::desired_grants(&def.grants, &["USAGE", "CREATE"]);
    let revocations = grants::stale_grant_revocations(
        &current_grants,
        &desired,
        &protected,
        "CLUSTER",
        &quote_identifier(cluster_name),
    );
    grants::execute_revocations(executor, &revocations, "cluster", &cluster_name).await?;

    // Execute COMMENT statements
    for comment in &def.comments {
        executor.execute_sql(comment).await?;
    }

    Ok(ObjectResult {
        object: cluster_name.clone(),
        action: action.to_string(),
        statements: executor.take_statements(),
        redacted_statements: vec![],
    })
}
