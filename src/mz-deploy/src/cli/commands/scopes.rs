// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reconcile the database- and schema-level configuration declared in mod files.
//!
//! A `<database>.sql` or `<schema>.sql` file declares comments, grants, and
//! default privileges for the scope it names. Each category is reconciled
//! against the catalog by the module that owns it, so a scope that already
//! matches the project emits no SQL.
//!
//! Declaring a mod file claims the scope: every category is reconciled, so a
//! grant or comment the file stops declaring is removed even if the file never
//! mentioned that category. A scope with no mod file declares nothing and is
//! left alone entirely, which is why the caller only reconciles scopes that
//! produced at least one mod statement.

use crate::cli::CliError;
use crate::cli::commands::reconcile::{self, ObjectKind, ReconcileTarget};
use crate::cli::executor::DeploymentExecutor;
use crate::client::Client;
use mz_sql_parser::ast::{
    AlterDefaultPrivilegesStatement, CommentStatement, GrantPrivilegesStatement, Raw, Statement,
};

/// The statements a mod file declares, split by the state each one describes.
#[derive(Default)]
struct ScopeConfig {
    comments: Vec<CommentStatement<Raw>>,
    grants: Vec<GrantPrivilegesStatement<Raw>>,
    default_privileges: Vec<AlterDefaultPrivilegesStatement<Raw>>,
    /// Anything else the file contains.
    ///
    /// Validation restricts mod files to the three categories above, so this is
    /// normally empty. Statements land here rather than being dropped so a
    /// future statement type keeps working, unreconciled, instead of silently
    /// disappearing from the plan.
    other: Vec<Statement<Raw>>,
}

impl ScopeConfig {
    fn from_statements(statements: &[Statement<Raw>]) -> Self {
        let mut config = ScopeConfig::default();
        for stmt in statements {
            match stmt {
                Statement::Comment(comment) => config.comments.push(comment.clone()),
                Statement::GrantPrivileges(grant) => config.grants.push(grant.clone()),
                Statement::AlterDefaultPrivileges(alter) => {
                    config.default_privileges.push(alter.clone())
                }
                other => config.other.push(other.clone()),
            }
        }
        config
    }
}

/// Reconcile the configuration a `<database>.sql` file declares.
pub async fn reconcile_database(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    database: &str,
    statements: &[Statement<Raw>],
) -> Result<(), CliError> {
    let config = ScopeConfig::from_statements(statements);
    reconcile::scope(
        client,
        executor,
        &ReconcileTarget::named(ObjectKind::Database, database),
        &config.grants,
        &config.comments,
        &config.default_privileges,
    )
    .await?;
    execute_other(executor, &config).await
}

/// Reconcile the configuration a `<schema>.sql` file declares.
pub async fn reconcile_schema(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    database: &str,
    schema: &str,
    statements: &[Statement<Raw>],
) -> Result<(), CliError> {
    let config = ScopeConfig::from_statements(statements);
    reconcile::scope(
        client,
        executor,
        &ReconcileTarget::schema(database, schema),
        &config.grants,
        &config.comments,
        &config.default_privileges,
    )
    .await?;
    execute_other(executor, &config).await
}

async fn execute_other(
    executor: &DeploymentExecutor<'_>,
    config: &ScopeConfig,
) -> Result<(), CliError> {
    for stmt in &config.other {
        executor.execute_sql(stmt).await?;
    }
    Ok(())
}
