//! Deployment execution utilities.
//!
//! This module contains the `DeploymentExecutor` for running SQL statements
//! during deployment, along with helper functions for collecting deployment
//! metadata and generating environment names.

use crate::cli::CliError;
use crate::cli::git::get_git_commit;
use crate::client::{Client, quote_identifier};
use crate::project::{self, typed};
use crate::verbose;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::path::Path;
use std::rc::Rc;

/// Collects SQL statements during dry-run for JSON output.
///
/// Wraps a shared, mutable vector of SQL strings. Cloning is cheap (Rc refcount bump).
/// After all commands complete, call [`SqlCollector::into_statements`] to extract the collected SQL.
#[derive(Clone)]
pub struct SqlCollector(Rc<RefCell<Vec<String>>>);

impl SqlCollector {
    /// Create a new empty collector.
    pub fn new() -> Self {
        Self(Rc::new(RefCell::new(Vec::new())))
    }

    /// Push a SQL statement into the collector.
    pub fn push(&self, sql: String) {
        self.0.borrow_mut().push(sql);
    }

    /// Consume the collector and return the collected statements.
    pub fn into_statements(self) -> Vec<String> {
        Rc::try_unwrap(self.0)
            .map(|cell| cell.into_inner())
            .unwrap_or_else(|rc| rc.borrow().clone())
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
            eprintln!("warning: failed to get current user: {}", e);
            "unknown".to_string()
        });

    let git_commit = get_git_commit(directory);

    project::deployment_snapshot::DeploymentMetadata {
        deployed_by,
        git_commit,
    }
}

/// Generate a random 7-character hex environment name.
///
/// Uses SHA256 hash of current timestamp to generate a unique identifier
/// for deployments when no explicit name is provided.
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
pub struct DeploymentExecutor<'a> {
    client: &'a Client,
    dry_run: bool,
    collected_sql: Option<SqlCollector>,
}

impl<'a> DeploymentExecutor<'a> {
    /// Create a new deployment executor that executes SQL.
    pub fn new(client: &'a Client) -> Self {
        Self {
            client,
            dry_run: false,
            collected_sql: None,
        }
    }

    /// Create a deployment executor with configurable dry-run mode.
    pub fn with_dry_run(client: &'a Client, dry_run: bool) -> Self {
        Self {
            client,
            dry_run,
            collected_sql: None,
        }
    }

    /// Create a deployment executor with an optional SQL collector for JSON output.
    pub fn with_collector(
        client: &'a Client,
        dry_run: bool,
        collector: Option<SqlCollector>,
    ) -> Self {
        Self {
            client,
            dry_run,
            collected_sql: collector,
        }
    }

    /// Returns true if this executor is in dry-run mode.
    pub fn is_dry_run(&self) -> bool {
        self.dry_run
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
    /// When a collector is present (JSON mode), statements are collected instead of printed.
    pub async fn execute_sql(&self, stmt: &impl ToString) -> Result<(), CliError> {
        let sql = stmt.to_string();

        if self.dry_run {
            let formatted = format!("{};", sql);
            if let Some(ref collector) = self.collected_sql {
                collector.push(formatted);
            } else {
                println!("{}", formatted);
                println!();
            }
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
}
