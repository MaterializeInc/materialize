//! Apply secrets command - create missing secrets and update existing ones.

use crate::cli::CliError;
use crate::cli::commands::apply_objects::{self, HandleResult};
use crate::cli::commands::grants;
use crate::cli::executor::ObjectAction;
use crate::cli::executor::{ApplyPlan, ApplyResult, DeploymentExecutor};
use crate::client::Client;
use crate::config::Settings;
use crate::project;
use crate::project::ast::Statement;
use crate::project::object_id::ObjectId;
use crate::project::typed;
use crate::secret_resolver::SecretResolver;
use mz_sql_parser::ast::{AlterSecretStatement, Raw};

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
    ) -> Result<HandleResult, CliError> {
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

        Ok(HandleResult {
            action: ObjectAction::Altered,
            redacted_statements,
        })
    }

    async fn handle_new(
        &self,
        client: &Client,
        executor: &DeploymentExecutor<'_>,
        obj_id: &ObjectId,
        typed_obj: &typed::DatabaseObject,
    ) -> Result<HandleResult, CliError> {
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

        Ok(HandleResult {
            action: ObjectAction::Created,
            redacted_statements,
        })
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
    let input = apply_objects::prepare_phase(
        &GRANT_KIND,
        matches,
        client,
        executor,
        planned_project,
        apply_plan,
    )
    .await?;

    let mut results = Vec::new();

    for (obj_id, typed_obj) in &input.existing_objects {
        executor.take_statements();
        let hr = secrets
            .handle_existing(client, executor, obj_id, typed_obj)
            .await?;
        results.push(apply_objects::to_object_result(obj_id, executor, hr));
    }

    for (obj_id, typed_obj) in &input.to_create {
        executor.take_statements();
        let hr = secrets
            .handle_new(client, executor, obj_id, typed_obj)
            .await?;
        results.push(apply_objects::to_object_result(obj_id, executor, hr));
    }

    Ok(ApplyResult {
        phase: PHASE_NAME.to_string(),
        results,
    })
}

/// Run the `apply secrets` command: plan, render, optionally execute.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<ApplyPlan, CliError> {
    apply_objects::run_compiled_phase(settings, dry_run, |s, c, e, pp, ap| {
        Box::pin(self::plan(s, c, e, pp, ap))
    })
    .await
}
