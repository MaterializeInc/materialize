//! Apply secrets command - create missing secrets and update existing ones.

use crate::cli::CliError;
use crate::cli::commands::apply_objects;
use crate::cli::commands::grants;
use crate::cli::executor::ObjectAction;
use crate::cli::executor::{
    ApplyPlan, ApplyResult, DeploymentExecutor, ObjectResult, compile_apply_project_and_connect,
};
use crate::client::Client;
use crate::config::Settings;
use crate::project;
use crate::project::ast::Statement;
use crate::project::object_id::ObjectId;
use crate::project::typed;
use crate::secret_resolver::SecretResolver;
use mz_sql_parser::ast::{AlterSecretStatement, Raw};
use std::collections::BTreeSet;

const PHASE_NAME: &str = "secrets";
const GRANT_KIND: grants::GrantObjectKind = grants::GrantObjectKind::Secret;

fn matches(stmt: &Statement) -> bool {
    matches!(stmt, Statement::CreateSecret(_))
}

struct Secrets {
    resolver: SecretResolver,
}

impl Secrets {
    fn new(settings: &Settings) -> Result<Self, CliError> {
        Ok(Secrets {
            resolver: SecretResolver::new(&settings.profile_config.security),
        })
    }

    async fn handle_existing(
        &self,
        client: &Client,
        executor: &DeploymentExecutor<'_>,
        obj_id: &ObjectId,
        typed_obj: &typed::DatabaseObject,
    ) -> Result<(ObjectAction, Vec<String>), CliError> {
        let Statement::CreateSecret(ref create_stmt) = typed_obj.stmt else {
            unreachable!("filtered for CreateSecret");
        };
        let resolved_stmt = self.resolver.resolve_secret_for_cli(create_stmt).await?;
        let alter_stmt = AlterSecretStatement::<Raw> {
            name: create_stmt.name.clone(),
            if_exists: false,
            value: resolved_stmt.value.clone(),
        };
        let redacted_statements = vec![alter_stmt.to_string()];

        apply_objects::reconcile_grants_and_comments(
            client,
            executor,
            obj_id,
            typed_obj,
            &GRANT_KIND,
        )
        .await?;

        Ok((ObjectAction::Altered, redacted_statements))
    }

    async fn handle_new(
        &self,
        client: &Client,
        executor: &DeploymentExecutor<'_>,
        obj_id: &ObjectId,
        typed_obj: &typed::DatabaseObject,
    ) -> Result<(ObjectAction, Vec<String>), CliError> {
        let Statement::CreateSecret(ref create_stmt) = typed_obj.stmt else {
            unreachable!("filtered for CreateSecret");
        };
        let resolved_stmt = self.resolver.resolve_secret_for_cli(create_stmt).await?;
        let redacted_statements = vec![resolved_stmt.to_string()];

        apply_objects::reconcile_grants_and_comments(
            client,
            executor,
            obj_id,
            typed_obj,
            &GRANT_KIND,
        )
        .await?;

        Ok((ObjectAction::Created, redacted_statements))
    }
}

/// Plan secret changes without executing or printing.
pub async fn plan(
    settings: &Settings,
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    planned_project: &project::planned::Project,
    apply_plan: &mut ApplyPlan,
) -> Result<ApplyResult, CliError> {
    let secrets = Secrets::new(settings)?;
    let mut target_ids = BTreeSet::new();
    for obj in planned_project.iter_objects() {
        if matches(&obj.typed_object.stmt) {
            target_ids.insert(obj.id.clone());
        }
    }

    if target_ids.is_empty() {
        return Ok(ApplyResult {
            phase: PHASE_NAME.to_string(),
            results: vec![],
        });
    }

    let target_objects = planned_project.get_sorted_objects_filtered(&target_ids)?;
    let existing = client
        .introspection()
        .check_catalog_objects_exist(&target_ids, GRANT_KIND.catalog_table())
        .await
        .map_err(CliError::Connection)?;

    let schemas: BTreeSet<_> = target_objects
        .iter()
        .filter(|(obj_id, _)| !existing.contains(obj_id))
        .map(|(obj_id, _)| {
            project::SchemaQualifier::new(obj_id.database.clone(), obj_id.schema.clone())
        })
        .collect();
    apply_plan
        .prepare_schemas(executor, planned_project, &schemas)
        .await?;

    let mut results = Vec::new();

    for (obj_id, typed_obj) in target_objects {
        executor.take_statements();
        let (action, redacted_statements) = if existing.contains(&obj_id) {
            secrets
                .handle_existing(client, executor, &obj_id, typed_obj)
                .await?
        } else {
            secrets
                .handle_new(client, executor, &obj_id, typed_obj)
                .await?
        };
        results.push(ObjectResult {
            object: obj_id.to_string(),
            action,
            statements: executor.take_statements(),
            redacted_statements,
        });
    }

    Ok(ApplyResult {
        phase: PHASE_NAME.to_string(),
        results,
    })
}

/// Run the `apply secrets` command: plan, render, optionally execute.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<ApplyPlan, CliError> {
    let (planned_project, client) = compile_apply_project_and_connect(settings).await?;
    let mut apply_plan = ApplyPlan::new();
    let executor = DeploymentExecutor::new_dry_run(&client);
    let phase = self::plan(
        settings,
        &client,
        &executor,
        &planned_project,
        &mut apply_plan,
    )
    .await?;
    apply_plan.add_phase(phase);

    if !dry_run {
        apply_plan.execute(&client).await?;
    }

    Ok(apply_plan)
}
