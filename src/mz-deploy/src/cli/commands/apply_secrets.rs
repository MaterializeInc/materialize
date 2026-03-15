//! Apply secrets command - create missing secrets and update existing ones.

use crate::cli::CliError;
use crate::cli::commands::apply_objects::{
    self, DatabaseObjectPhase, HandleResult, reconcile_grants_and_comments,
};
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

pub struct Secrets {
    resolver: SecretResolver,
}

impl DatabaseObjectPhase for Secrets {
    const PHASE_NAME: &'static str = "secrets";
    const GRANT_KIND: grants::GrantObjectKind = grants::GrantObjectKind::Secret;

    fn new(settings: &Settings) -> Result<Self, CliError> {
        Ok(Secrets {
            resolver: SecretResolver::new(&settings.profile_config.security),
        })
    }

    fn matches(stmt: &Statement) -> bool {
        matches!(stmt, Statement::CreateSecret(_))
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

        reconcile_grants_and_comments::<Self>(client, executor, obj_id, typed_obj).await?;

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

        reconcile_grants_and_comments::<Self>(client, executor, obj_id, typed_obj).await?;

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
    apply_objects::plan::<Secrets>(settings, client, executor, planned_project, apply_plan).await
}

/// Run the `apply secrets` command: plan, render, optionally execute.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<ApplyPlan, CliError> {
    apply_objects::run::<Secrets>(settings, dry_run).await
}
