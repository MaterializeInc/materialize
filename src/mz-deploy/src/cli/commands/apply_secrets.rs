//! Apply secrets command - create missing secrets and update existing ones.

use crate::cli::commands::grants;
use crate::cli::executor::{ApplyResult, ObjectResult};
use crate::cli::{CliError, executor};
use crate::client::Client;
use crate::config::Settings;
use crate::project;
use crate::project::ast::Statement;
use crate::secret_resolver::SecretResolver;
use mz_sql_parser::ast::{AlterSecretStatement, Raw};

/// Plan secret changes without executing or printing.
pub async fn plan(
    settings: &Settings,
    client: &Client,
    planned_project: &project::planned::Project,
) -> Result<ApplyResult, CliError> {
    let secrets: Vec<_> = planned_project
        .iter_objects()
        .filter(|obj| matches!(obj.typed_object.stmt, Statement::CreateSecret(_)))
        .collect();

    if secrets.is_empty() {
        return Ok(ApplyResult {
            phase: "secrets".to_string(),
            setup_statements: vec![],
            results: vec![],
        });
    }

    let executor = executor::DeploymentExecutor::new_dry_run(client);

    // Prepare schemas and mod statements for secret schemas
    let secret_schemas = project::SchemaQualifier::collect_from(&secrets);
    executor
        .prepare_databases_and_schemas(&planned_project, &secret_schemas, None)
        .await?;

    let setup_statements = executor.take_statements();
    let resolver = SecretResolver::new(&settings.profile_config.security);

    let mut object_results = Vec::new();
    for obj in &secrets {
        let typed_obj = &obj.typed_object;
        if let Statement::CreateSecret(ref create_stmt) = typed_obj.stmt {
            let name = &create_stmt.name;

            // Drain prior statements
            executor.take_statements();

            let resolved_stmt = resolver.resolve_secret_for_cli(create_stmt).await?;

            // Build the secret-bearing SQL but do NOT log it through the executor
            // to avoid leaking secret values into output or serialization.
            let mut create_if_not_exists = resolved_stmt.clone();
            create_if_not_exists.if_not_exists = true;
            let create_sql = create_if_not_exists.to_string();

            let alter_stmt = AlterSecretStatement::<Raw> {
                name: name.clone(),
                if_exists: false,
                value: resolved_stmt.value.clone(),
            };
            let alter_sql = alter_stmt.to_string();

            let redacted_statements = vec![create_sql, alter_sql];

            grants::reconcile(
                client,
                &executor,
                &obj.id,
                &typed_obj.grants,
                &grants::GrantObjectKind::Secret,
            )
            .await?;

            for comment in &typed_obj.comments {
                executor.execute_sql(comment).await?;
            }

            object_results.push(ObjectResult {
                object: format!("{}", obj.id),
                action: "created".to_string(),
                statements: executor.take_statements(),
                redacted_statements,
            });
        }
    }

    Ok(ApplyResult {
        phase: "secrets".to_string(),
        setup_statements,
        results: object_results,
    })
}

/// Run the `apply secrets` command: plan, render, optionally execute.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<ApplyResult, CliError> {
    let planned_project =
        super::compile::run(settings, true, !crate::log::json_output_enabled()).await?;
    let client = Client::connect_with_profile(settings.connection().clone())
        .await
        .map_err(CliError::Connection)?;

    let result = plan(settings, &client, &planned_project).await?;

    if !dry_run {
        result.execute(&client).await?;
    }

    Ok(result)
}
