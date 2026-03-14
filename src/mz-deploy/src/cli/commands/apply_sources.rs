//! Apply sources command - create sources that don't exist in the database.

use crate::cli::commands::apply_objects::{self, DatabaseObjectPhase};
use crate::cli::commands::grants;
use crate::cli::executor::{ApplyPlan, ApplyResult, DeploymentExecutor};
use crate::cli::CliError;
use crate::client::Client;
use crate::config::Settings;
use crate::project;
use crate::project::ast::Statement;

pub struct Sources;

impl DatabaseObjectPhase for Sources {
    const PHASE_NAME: &'static str = "sources";
    const GRANT_KIND: grants::GrantObjectKind = grants::GrantObjectKind::Source;

    fn new(_settings: &Settings) -> Result<Self, CliError> {
        Ok(Sources)
    }

    fn matches(stmt: &Statement) -> bool {
        matches!(stmt, Statement::CreateSource(_))
    }
    // Uses default handle_existing (reconcile grants → "up_to_date")
    // Uses default handle_new (execute stmt + indexes + grants + comments → "created")
}

/// Plan only source objects (no deployment tracking, no execution).
pub async fn plan(
    settings: &Settings,
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    planned_project: &project::planned::Project,
    apply_plan: &mut ApplyPlan,
) -> Result<ApplyResult, CliError> {
    apply_objects::plan::<Sources>(settings, client, executor, planned_project, apply_plan).await
}

/// Run the `apply sources` command: compile, plan, optionally execute.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<ApplyPlan, CliError> {
    apply_objects::run::<Sources>(settings, dry_run).await
}
