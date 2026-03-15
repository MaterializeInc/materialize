//! Apply tables command - create tables that don't exist in the database.

use crate::cli::CliError;
use crate::cli::commands::apply_objects::{self, DatabaseObjectPhase};
use crate::cli::commands::grants;
use crate::cli::executor::{ApplyPlan, ApplyResult, DeploymentExecutor};
use crate::client::Client;
use crate::config::Settings;
use crate::project;
use crate::project::ast::Statement;

pub struct Tables;

impl DatabaseObjectPhase for Tables {
    const PHASE_NAME: &'static str = "tables";
    const GRANT_KIND: grants::GrantObjectKind = grants::GrantObjectKind::Table;

    fn new(_settings: &Settings) -> Result<Self, CliError> {
        Ok(Tables)
    }

    fn matches(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::CreateTable(_) | Statement::CreateTableFromSource(_)
        )
    }
    // Uses default handle_existing (reconcile grants → "up_to_date")
    // Uses default handle_new (execute stmt + indexes + grants + comments → "created")
}

/// Plan only table objects (no deployment tracking, no execution).
pub async fn plan(
    settings: &Settings,
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    planned_project: &project::planned::Project,
    apply_plan: &mut ApplyPlan,
) -> Result<ApplyResult, CliError> {
    apply_objects::plan::<Tables>(settings, client, executor, planned_project, apply_plan).await
}

/// Run the `apply tables` command: compile, plan, optionally execute.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<ApplyPlan, CliError> {
    apply_objects::run::<Tables>(settings, dry_run).await
}
