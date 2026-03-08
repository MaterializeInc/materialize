//! Apply secrets command - create missing secrets and update existing ones.

use crate::cli::commands::grants;
use crate::cli::progress;
use crate::cli::{CliError, TypeCheckMode, executor};
use crate::client::{Client, Profile};
use crate::config::ProjectSettings;
use crate::project;
use crate::project::ast::Statement;
use crate::project::planned;
use crate::secret_resolver::SecretResolver;
use mz_sql_parser::ast::{AlterSecretStatement, Raw};
use std::collections::BTreeSet;
use std::path::Path;
use std::time::Instant;

/// Run the `apply secrets` command.
///
/// Compiles the project, collects all secret objects, and for each secret:
/// 1. Executes `CREATE SECRET IF NOT EXISTS` (idempotent create)
/// 2. Executes `ALTER SECRET` (always update the value to match the file)
/// 3. Applies any associated grants and comments
pub async fn run(
    directory: &Path,
    profile: &Profile,
    settings: &ProjectSettings,
    dry_run: bool,
) -> Result<(), CliError> {
    let start_time = Instant::now();

    let planned_project = super::compile::run(directory, TypeCheckMode::Disabled).await?;

    let secrets: Vec<_> = planned_project
        .iter_objects()
        .filter(|obj| matches!(obj.typed_object.stmt, Statement::CreateSecret(_)))
        .collect();

    if secrets.is_empty() {
        progress::info("No secrets found in project — nothing to do.");
        return Ok(());
    }

    progress::info(&format!("Found {} secret(s) in project", secrets.len()));

    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    let executor = executor::DeploymentExecutor::with_dry_run(&client, dry_run);

    // Prepare schemas and mod statements for secret schemas
    let secret_schemas = collect_secret_schemas(&secrets);
    executor
        .prepare_databases_and_schemas(&planned_project, &secret_schemas, None)
        .await?;

    let resolver = SecretResolver::new(&settings.secret_config);

    for obj in &secrets {
        let typed_obj = &obj.typed_object;
        if let Statement::CreateSecret(ref create_stmt) = typed_obj.stmt {
            let name = &create_stmt.name;

            let resolved_stmt = resolver.resolve_secret_for_cli(create_stmt).await?;

            let mut create_if_not_exists = resolved_stmt.clone();
            create_if_not_exists.if_not_exists = true;
            executor.execute_sql(&create_if_not_exists).await?;

            let alter_stmt = AlterSecretStatement::<Raw> {
                name: name.clone(),
                if_exists: false,
                value: resolved_stmt.value.clone(),
            };
            executor.execute_sql(&alter_stmt).await?;

            for grant in &typed_obj.grants {
                executor.execute_sql(grant).await?;
            }

            // Revoke stale grants
            if !dry_run {
                let fqn = grants::quoted_fqn(&obj.id.database, &obj.id.schema, &obj.id.object);
                let current_grants = client
                    .introspection()
                    .get_database_object_grants(
                        "mz_secrets",
                        &obj.id.database,
                        &obj.id.schema,
                        &obj.id.object,
                    )
                    .await
                    .map_err(CliError::Connection)?;
                let desired = grants::desired_grants(&typed_obj.grants, &["USAGE"]);
                let revocations =
                    grants::stale_grant_revocations(&current_grants, &desired, "SECRET", &fqn);
                grants::execute_revocations(&client, &revocations, "secret", &obj.id).await?;
            }

            for comment in &typed_obj.comments {
                executor.execute_sql(comment).await?;
            }

            progress::success(&format!("{}", obj.id));
        }
    }

    let duration = start_time.elapsed();
    progress::success(&format!(
        "Applied {} secret(s) in {:.1}s",
        secrets.len(),
        duration.as_secs_f64()
    ));

    Ok(())
}

fn collect_secret_schemas(
    secrets: &[&planned::DatabaseObject],
) -> BTreeSet<project::SchemaQualifier> {
    let mut schemas = BTreeSet::new();
    for obj in secrets {
        schemas.insert(project::SchemaQualifier::new(
            obj.id.database.clone(),
            obj.id.schema.clone(),
        ));
    }
    schemas
}
