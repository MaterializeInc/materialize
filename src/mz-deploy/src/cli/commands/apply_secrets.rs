//! Apply secrets command - create missing secrets and update existing ones.

use crate::cli::{CliError, TypeCheckMode, executor};
use crate::client::{Client, Profile};
use crate::config::ProjectSettings;
use crate::project;
use crate::project::ast::Statement;
use crate::project::planned;
use crate::secret_resolver::SecretResolver;
use mz_sql_parser::ast::{AlterSecretStatement, Raw};
use owo_colors::OwoColorize;
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
) -> Result<(), CliError> {
    let start_time = Instant::now();

    let planned_project = super::compile::run(directory, TypeCheckMode::Disabled).await?;

    let secrets: Vec<_> = planned_project
        .iter_objects()
        .filter(|obj| matches!(obj.typed_object.stmt, Statement::CreateSecret(_)))
        .collect();

    if secrets.is_empty() {
        println!("No secrets found in project — nothing to do.");
        return Ok(());
    }

    println!("Found {} secret(s) in project", secrets.len());

    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    let executor = executor::DeploymentExecutor::new(&client);

    // Prepare schemas and mod statements for secret schemas
    let secret_schemas = collect_secret_schemas(&secrets);
    super::create_tables::prepare_schemas_and_mod_statements(
        &executor,
        &planned_project,
        &secret_schemas,
        false,
    )
    .await?;

    let resolver = SecretResolver::new(&settings.secret_config);

    for obj in &secrets {
        let typed_obj = &obj.typed_object;
        if let Statement::CreateSecret(ref create_stmt) = typed_obj.stmt {
            let name = &create_stmt.name;

            // Resolve client-side secret providers (e.g. env_var)
            let resolved_stmt = resolver.resolve_secret_for_cli(create_stmt).await?;

            // CREATE SECRET IF NOT EXISTS
            let mut create_if_not_exists = resolved_stmt.clone();
            create_if_not_exists.if_not_exists = true;
            executor.execute_sql(&create_if_not_exists).await?;

            // ALTER SECRET to update the value
            let alter_stmt = AlterSecretStatement::<Raw> {
                name: name.clone(),
                if_exists: false,
                value: resolved_stmt.value.clone(),
            };
            executor.execute_sql(&alter_stmt).await?;

            // Execute grants
            for grant in &typed_obj.grants {
                executor.execute_sql(grant).await?;
            }

            // Execute comments
            for comment in &typed_obj.comments {
                executor.execute_sql(comment).await?;
            }

            println!("  {} {}", "✓".green().bold(), obj.id,);
        }
    }

    let duration = start_time.elapsed();
    println!(
        "\nApplied {} secret(s) in {:.1}s",
        secrets.len(),
        duration.as_secs_f64()
    );

    Ok(())
}

fn collect_secret_schemas(
    secrets: &[&planned::DatabaseObject],
) -> std::collections::BTreeMap<project::SchemaQualifier, crate::client::DeploymentKind> {
    let mut schemas = std::collections::BTreeMap::new();
    for obj in secrets {
        schemas.insert(
            project::SchemaQualifier::new(obj.id.database.clone(), obj.id.schema.clone()),
            crate::client::DeploymentKind::Tables,
        );
    }
    schemas
}
