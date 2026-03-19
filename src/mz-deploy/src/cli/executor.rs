//! Deployment execution utilities.
//!
//! This module contains the `DeploymentExecutor` for running SQL statements
//! during deployment, along with helper functions for collecting deployment
//! metadata and generating environment names.
//!
//! ## ApplyPlan Lifecycle
//!
//! ```text
//! compile project → connect client → plan phases → execute
//!                                        │
//!                     ┌──────────────────┼───────────────────┐
//!                     ▼                  ▼                   ▼
//!              prepare_schemas     add_phase(...)      execute(&client)
//!              (CREATE DB/SCHEMA)  (dry-run SQL)       (run all SQL)
//! ```
//!
//! 1. **Compile** — `compile_apply_project_and_connect` loads and validates the project.
//! 2. **Plan** — Each apply subcommand calls `DeploymentExecutor::new_dry_run()` to
//!    collect SQL without executing it, then packages results as `ApplyResult` phases.
//! 3. **Execute** — `ApplyPlan::execute()` runs setup statements first, then per-phase
//!    object statements. Objects sharing a `transaction_group` key are wrapped in a
//!    single `BEGIN`/`COMMIT` block with automatic `ROLLBACK` on failure.

use crate::cli::CliError;
use crate::cli::git::get_git_commit;
use crate::client::{Client, ClusterConfig, quote_identifier};
use crate::config::Settings;
use crate::project::{self, typed};
use crate::{info, verbose};
use owo_colors::OwoColorize;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::fmt;
use std::path::Path;

/// What happened when applying a single object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectAction {
    Created,
    Altered,
    UpToDate,
    Skipped,
}

impl fmt::Display for ObjectAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectAction::Created => write!(f, "created"),
            ObjectAction::Altered => write!(f, "altered"),
            ObjectAction::UpToDate => write!(f, "up_to_date"),
            ObjectAction::Skipped => write!(f, "skipped"),
        }
    }
}

/// Result of applying a single object (cluster, role, connection, etc.).
#[derive(Clone, Serialize)]
pub struct ObjectResult {
    /// Fully-qualified object name, e.g. "materialize.raw.pgconn".
    pub object: String,
    /// What happened: created, altered, up_to_date, skipped.
    pub action: ObjectAction,
    /// SQL statements that were (or would be) executed.
    pub statements: Vec<String>,
    /// SQL statements that must be executed but contain sensitive values
    /// (e.g. CREATE SECRET, ALTER SECRET). These are never serialized or displayed.
    #[serde(skip)]
    pub redacted_statements: Vec<String>,
    /// Optional transaction group key. Objects with the same group key are
    /// executed inside a single BEGIN/COMMIT transaction block.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_group: Option<String>,
}

impl fmt::Display for ObjectResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.action {
            ObjectAction::Created | ObjectAction::Altered => {
                write!(f, "  {} {}", "✓".green(), self.object)
            }
            ObjectAction::UpToDate => write!(
                f,
                "  {} {} ({})",
                "=".dimmed(),
                self.object,
                "up to date".dimmed()
            ),
            ObjectAction::Skipped => write!(
                f,
                "  {} {} ({})",
                "-".dimmed(),
                self.object,
                "skipped".dimmed()
            ),
        }
    }
}

/// Result of applying one phase (e.g. clusters, connections, etc.).
#[derive(Clone, Serialize)]
pub struct ApplyResult {
    /// Phase name: "clusters", "roles", "connections", etc.
    pub phase: String,
    /// Per-object results.
    pub results: Vec<ObjectResult>,
}

impl fmt::Display for ApplyResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.results.is_empty() {
            return Ok(());
        }

        let created = self
            .results
            .iter()
            .filter(|r| r.action == ObjectAction::Created)
            .count();
        let altered = self
            .results
            .iter()
            .filter(|r| r.action == ObjectAction::Altered)
            .count();
        let up_to_date = self
            .results
            .iter()
            .filter(|r| r.action == ObjectAction::UpToDate)
            .count();

        let label = &self.phase;
        let mut lines = Vec::new();

        if created > 0 {
            lines.push(format!(
                "  {} Creating {} new {}...",
                "ℹ".blue(),
                created,
                label
            ));
        }
        if altered > 0 {
            lines.push(format!(
                "  {} Altering {} {}...",
                "ℹ".blue(),
                altered,
                label
            ));
        }
        if up_to_date > 0 && created == 0 && altered == 0 {
            lines.push(format!(
                "  {} {} {} up to date",
                "ℹ".blue(),
                up_to_date,
                label
            ));
        }

        for r in &self.results {
            if r.action != ObjectAction::UpToDate {
                lines.push(format!("{}", r));
            }
        }

        write!(f, "{}", lines.join("\n"))
    }
}

/// A complete apply plan: global setup + ordered phase results.
/// Built incrementally by adding phases, then executed as a unit.
#[derive(Serialize)]
pub struct ApplyPlan {
    /// Global setup SQL (CREATE DATABASE/SCHEMA, mod_statements).
    /// Deduplicated across all phases.
    pub setup_statements: Vec<String>,
    /// Per-phase results in dependency order.
    pub phases: Vec<ApplyResult>,
    /// Tracks which schemas have already been prepared (for deduplication).
    #[serde(skip)]
    prepared_schemas: BTreeSet<project::SchemaQualifier>,
}

/// A batch of objects to execute together, optionally inside a transaction.
struct ExecutionBatch<'a> {
    /// If Some, these objects are wrapped in BEGIN/COMMIT.
    transaction_group: Option<&'a str>,
    /// The objects in this batch.
    objects: Vec<&'a ObjectResult>,
}

impl ApplyPlan {
    pub fn new() -> Self {
        Self {
            setup_statements: Vec::new(),
            phases: Vec::new(),
            prepared_schemas: BTreeSet::new(),
        }
    }

    /// Prepare databases and schemas for the given schema set.
    /// Deduplicates against previously prepared schemas.
    pub async fn prepare_schemas(
        &mut self,
        executor: &DeploymentExecutor<'_>,
        planned_project: &project::planned::Project,
        schema_set: &BTreeSet<project::SchemaQualifier>,
    ) -> Result<(), CliError> {
        let new_schemas: BTreeSet<_> = schema_set
            .difference(&self.prepared_schemas)
            .cloned()
            .collect();
        if new_schemas.is_empty() {
            return Ok(());
        }
        executor
            .prepare_databases_and_schemas(planned_project, &new_schemas, None)
            .await?;
        self.setup_statements.extend(executor.take_statements());
        self.prepared_schemas.extend(new_schemas);
        Ok(())
    }

    /// Add a completed phase result.
    pub fn add_phase(&mut self, result: ApplyResult) {
        self.phases.push(result);
    }

    /// Execute: run global setup, then each phase's per-object SQL.
    ///
    /// Objects that share a `transaction_group` key are wrapped in a single
    /// BEGIN/COMMIT block. On error inside a transaction, ROLLBACK is issued
    /// before returning the error.
    pub async fn execute(&self, client: &Client) -> Result<(), CliError> {
        // Phase 1: setup statements
        for sql in &self.setup_statements {
            client
                .execute(sql, &[])
                .await
                .map_err(|source| CliError::SqlExecutionFailed {
                    statement: sql.clone(),
                    source,
                })?;
        }

        // Phase 2: Group objects into execution batches
        let mut batches: Vec<ExecutionBatch<'_>> = Vec::new();
        for phase in &self.phases {
            for obj in &phase.results {
                let obj_txn = obj.transaction_group.as_deref();
                match batches.last_mut() {
                    Some(batch) if batch.transaction_group == obj_txn && obj_txn.is_some() => {
                        // Same transaction group — append to current batch
                        batch.objects.push(obj);
                    }
                    _ => {
                        // New batch (different group, no group, or first object)
                        batches.push(ExecutionBatch {
                            transaction_group: obj_txn,
                            objects: vec![obj],
                        });
                    }
                }
            }
        }

        // Phase 3: Execute batches
        for batch in &batches {
            let in_txn = batch.transaction_group.is_some();
            if in_txn {
                client.execute("BEGIN", &[]).await.map_err(|source| {
                    CliError::SqlExecutionFailed {
                        statement: "BEGIN".to_string(),
                        source,
                    }
                })?;
            }

            for obj in &batch.objects {
                for sql in obj.redacted_statements.iter().chain(&obj.statements) {
                    if let Err(e) = client.execute(sql, &[]).await {
                        if in_txn {
                            let _ = client.execute("ROLLBACK", &[]).await;
                        }
                        return Err(CliError::SqlExecutionFailed {
                            statement: if obj.redacted_statements.contains(sql) {
                                "[REDACTED — contains secret value]".to_string()
                            } else {
                                sql.clone()
                            },
                            source: e,
                        });
                    }
                }
            }

            if in_txn {
                client.execute("COMMIT", &[]).await.map_err(|source| {
                    CliError::SqlExecutionFailed {
                        statement: "COMMIT".to_string(),
                        source,
                    }
                })?;
            }
        }

        Ok(())
    }
}

impl fmt::Display for ApplyPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for phase in &self.phases {
            if phase.results.is_empty() {
                continue;
            }
            if !first {
                writeln!(f)?;
            }
            write!(f, "{}", phase)?;
            first = false;
        }
        Ok(())
    }
}

/// Collect deployment metadata (user and git commit).
///
/// This function retrieves the current database user and git commit hash
/// for recording deployment provenance. If the current user cannot be
/// determined, it defaults to "unknown".
pub async fn collect_deployment_metadata(
    client: &Client,
    directory: &Path,
) -> project::deployment_snapshot::DeploymentMetadata {
    let deployed_by = client
        .introspection()
        .get_current_user()
        .await
        .unwrap_or_else(|e| {
            info!("warning: failed to get current user: {}", e);
            "unknown".to_string()
        });

    let git_commit = get_git_commit(directory);

    project::deployment_snapshot::DeploymentMetadata {
        deployed_by,
        git_commit,
    }
}

/// Connect a planning client for apply commands.
pub async fn connect_apply_client(settings: &Settings) -> Result<Client, CliError> {
    Client::connect_with_profile(settings.connection().clone())
        .await
        .map_err(CliError::Connection)
}

/// Compile the project and connect a planning client for database-object apply commands.
pub async fn compile_apply_project_and_connect(
    settings: &Settings,
) -> Result<(project::planned::Project, Client), CliError> {
    let planned_project =
        crate::cli::commands::compile::run(settings, true, !crate::log::json_output_enabled())
            .await?;
    let client = connect_apply_client(settings).await?;
    Ok((planned_project, client))
}

/// Generate a unique 7-character hex identifier for deployments when no
/// explicit environment name is provided.
pub fn generate_random_env_name() -> String {
    use sha2::{Digest, Sha256};
    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_nanos();

    let mut hasher = Sha256::new();
    hasher.update(now.to_le_bytes());
    let hash = hasher.finalize();

    // Take first 4 bytes of hash and format as 7-char hex
    format!(
        "{:07x}",
        u32::from_le_bytes([hash[0], hash[1], hash[2], hash[3]]) & 0xFFFFFFF
    )
}

/// Helper for executing database object deployments.
///
/// This struct consolidates the pattern of executing a database object's
/// SQL statements (main statement + indexes + grants + comments) with
/// consistent error handling. Supports dry-run mode where SQL is printed
/// instead of executed.
///
/// In dry-run mode, statements are always recorded in an internal log
/// that can be drained per-object via [`take_statements()`].
pub struct DeploymentExecutor<'a> {
    client: &'a Client,
    dry_run: bool,
    statement_log: RefCell<Vec<String>>,
}

impl<'a> DeploymentExecutor<'a> {
    /// Create a new deployment executor that executes SQL.
    pub fn new(client: &'a Client) -> Self {
        Self {
            client,
            dry_run: false,
            statement_log: RefCell::new(Vec::new()),
        }
    }

    /// Create a deployment executor that always runs in dry-run mode (for planning).
    pub fn new_dry_run(client: &'a Client) -> Self {
        Self {
            client,
            dry_run: true,
            statement_log: RefCell::new(Vec::new()),
        }
    }

    /// Create a deployment executor with configurable dry-run mode.
    pub fn with_dry_run(client: &'a Client, dry_run: bool) -> Self {
        Self {
            client,
            dry_run,
            statement_log: RefCell::new(Vec::new()),
        }
    }

    /// Returns true if this executor is in dry-run mode.
    pub fn is_dry_run(&self) -> bool {
        self.dry_run
    }

    /// Drain and return all statements recorded since the last call.
    ///
    /// Use this between objects to capture per-object statement lists
    /// for `ObjectResult`.
    pub fn take_statements(&self) -> Vec<String> {
        self.statement_log.borrow_mut().drain(..).collect()
    }

    /// Execute all SQL statements for a database object.
    ///
    /// This executes the main CREATE statement, followed by any indexes,
    /// grants, and comments associated with the object.
    pub async fn execute_object(&self, typed_obj: &typed::DatabaseObject) -> Result<(), CliError> {
        // Execute main statement
        self.execute_sql(&typed_obj.stmt).await?;

        // Execute indexes
        for index in &typed_obj.indexes {
            self.execute_sql(index).await?;
        }

        // Execute grants
        for grant in &typed_obj.grants {
            self.execute_sql(grant).await?;
        }

        // Execute comments
        for comment in &typed_obj.comments {
            self.execute_sql(comment).await?;
        }

        Ok(())
    }

    /// Create databases and schemas for `schema_set`, then execute filtered mod_statements.
    ///
    /// When `staging_suffix` is `Some`, schema names are suffixed and mod_statement
    /// references are transformed to target staging schemas.
    pub async fn prepare_databases_and_schemas(
        &self,
        planned_project: &project::planned::Project,
        schema_set: &BTreeSet<project::SchemaQualifier>,
        staging_suffix: Option<&str>,
    ) -> Result<(), CliError> {
        if schema_set.is_empty() {
            return Ok(());
        }

        // Step 1: Create databases
        let databases: BTreeSet<&str> = schema_set.iter().map(|sq| sq.database.as_str()).collect();
        for db in &databases {
            let sql = format!("CREATE DATABASE IF NOT EXISTS {}", quote_identifier(db));
            self.execute_sql(&sql).await?;
        }

        // Step 2: Create schemas (with optional staging suffix)
        for sq in schema_set {
            let schema_name = match staging_suffix {
                Some(suffix) => format!("{}{}", sq.schema, suffix),
                None => sq.schema.clone(),
            };
            verbose!(
                "Creating schema {}.{} if not exists",
                sq.database,
                schema_name
            );
            let sql = format!(
                "CREATE SCHEMA IF NOT EXISTS {}.{}",
                quote_identifier(&sq.database),
                quote_identifier(&schema_name)
            );
            self.execute_sql(&sql).await?;
        }

        // Step 3: Execute mod_statements filtered by schema_set membership
        for mod_stmt in planned_project.iter_mod_statements() {
            match mod_stmt {
                project::ModStatement::Database {
                    database,
                    statement,
                } => {
                    let has_schema = schema_set.iter().any(|sq| sq.database == *database);
                    if has_schema {
                        verbose!("Applying database setup for: {}", database);
                        self.execute_sql(statement).await?;
                    }
                }
                project::ModStatement::Schema {
                    database,
                    schema,
                    statement,
                } => {
                    if schema_set.contains(&project::SchemaQualifier::new(
                        database.to_string(),
                        schema.to_string(),
                    )) {
                        if let Some(suffix) = staging_suffix {
                            let staging_schema = format!("{}{}", schema, suffix);
                            let transformed_stmt = statement.to_string().replace(
                                &format!("{}.{}", database, schema),
                                &format!("{}.{}", database, staging_schema),
                            );
                            verbose!("Applying schema setup for: {}.{}", database, staging_schema);
                            self.execute_sql(&transformed_stmt).await?;
                        } else {
                            verbose!("Applying schema setup for: {}.{}", database, schema);
                            self.execute_sql(statement).await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Execute (or print in dry-run mode) a single SQL statement.
    ///
    /// In dry-run mode, statements are always logged to the internal
    /// statement buffer (retrievable via `take_statements()`).
    pub async fn execute_sql(&self, stmt: &impl ToString) -> Result<(), CliError> {
        let sql = stmt.to_string();

        if self.dry_run {
            self.statement_log.borrow_mut().push(sql.clone());
            return Ok(());
        }

        self.client
            .execute(&sql, &[])
            .await
            .map_err(|source| CliError::SqlExecutionFailed {
                statement: sql,
                source,
            })?;
        Ok(())
    }

    /// Ensure a database exists.
    ///
    /// Real mode: delegates to `client.provisioning().create_database()`.
    /// Dry-run: logs `CREATE DATABASE IF NOT EXISTS ...`.
    pub async fn ensure_database(&self, name: &str) -> Result<(), CliError> {
        if self.dry_run {
            let sql = format!("CREATE DATABASE IF NOT EXISTS {}", quote_identifier(name));
            self.statement_log.borrow_mut().push(sql);
        } else {
            self.client.provisioning().create_database(name).await?;
        }
        Ok(())
    }

    /// Ensure a schema exists in the given database.
    ///
    /// Real mode: delegates to `client.provisioning().create_schema()`.
    /// Dry-run: logs `CREATE SCHEMA IF NOT EXISTS ...`.
    pub async fn ensure_schema(&self, database: &str, schema: &str) -> Result<(), CliError> {
        if self.dry_run {
            let sql = format!(
                "CREATE SCHEMA IF NOT EXISTS {}.{}",
                quote_identifier(database),
                quote_identifier(schema)
            );
            self.statement_log.borrow_mut().push(sql);
        } else {
            self.client
                .provisioning()
                .create_schema(database, schema)
                .await?;
        }
        Ok(())
    }

    /// Create a staging cluster by cloning a production cluster's configuration.
    ///
    /// Real mode: delegates to `client.provisioning().create_cluster_with_config()`.
    /// Dry-run: logs a placeholder `CREATE CLUSTER ...` statement.
    pub async fn create_cluster(
        &self,
        staging_name: &str,
        prod_name: &str,
        config: &ClusterConfig,
    ) -> Result<(), CliError> {
        if self.dry_run {
            let sql = format!(
                "CREATE CLUSTER {} (SIZE = '<from {}')",
                quote_identifier(staging_name),
                prod_name
            );
            self.statement_log.borrow_mut().push(sql);
        } else {
            self.client
                .provisioning()
                .create_cluster_with_config(staging_name, config)
                .await?;
        }
        Ok(())
    }

    /// Record cluster mappings for a deployment.
    ///
    /// Real mode: delegates to `client.deployments().insert_deployment_clusters()`.
    /// Dry-run: no-op (internal bookkeeping).
    pub async fn record_deployment_clusters(
        &self,
        stage_name: &str,
        clusters: &[String],
    ) -> Result<(), CliError> {
        if !self.dry_run {
            self.client
                .deployments()
                .insert_deployment_clusters(stage_name, clusters)
                .await?;
            verbose!("Cluster mappings recorded");
        }
        Ok(())
    }
}
