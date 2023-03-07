// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::catalog::Catalog;
use crate::command::Command;
use crate::session::Session;
use crate::AdapterError;
use mz_sql::catalog::{CatalogRole, SessionCatalog};
use mz_sql::plan::Plan;
use mz_sql_parser::ast::{Raw, Statement};
use std::error::Error;
use std::fmt;
use std::fmt::Formatter;

/// Attributes that allow a role to execute certain plans.
///
/// Note: This is a subset of all role attributes used for privilege checks.
#[derive(Debug)]
pub enum Attribute {
    /// Allows creating, altering, and dropping roles.
    CreateRole,
    /// Allows creating databases.
    CreateDB,
    /// Allows creating clusters.
    CreateCluster,
}

impl Attribute {
    /// Reports whether a role has the privilege granted by the attribute.
    fn check_role(&self, role: &dyn CatalogRole) -> bool {
        match self {
            Attribute::CreateRole => role.create_role(),
            Attribute::CreateDB => role.create_db(),
            Attribute::CreateCluster => role.create_cluster(),
        }
    }
}

impl std::fmt::Display for Attribute {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Attribute::CreateRole => f.write_string("CREATEROLE"),
            Attribute::CreateDB => f.write_string("CREATEDB"),
            Attribute::CreateCluster => f.write_string("CREATECLUSTER"),
        }
    }
}

/// Errors that can occur due to an unauthorized action.
#[derive(Debug)]
pub struct UnauthorizedError {
    /// The action that was forbidden.
    action: String,
    kind: UnauthorizedErrorKind,
}

#[derive(Debug)]
enum UnauthorizedErrorKind {
    /// The action can only be performed by a superuser.
    Superuser,
    /// The action requires a specific attribute.
    Attribute(Attribute),
    /// The action requires one or more privileges.
    Privilege { reason: String },
}

impl fmt::Display for UnauthorizedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "permission denied to {}", self.action)
    }
}

impl UnauthorizedError {
    pub fn superuser(action: String) -> UnauthorizedError {
        UnauthorizedError {
            action,
            kind: UnauthorizedErrorKind::Superuser,
        }
    }

    pub fn attribute(action: String, attribute: Attribute) -> UnauthorizedError {
        UnauthorizedError {
            action,
            kind: UnauthorizedErrorKind::Attribute(attribute),
        }
    }

    pub fn privilege(action: String, reason: String) -> UnauthorizedError {
        UnauthorizedError {
            action,
            kind: UnauthorizedErrorKind::Unstructured { reason },
        }
    }

    pub fn detail(&self) -> Option<String> {
        match &self.kind {
            UnauthorizedErrorKind::Superuser => {
                Some(format!("You must be a superuser to {}", self.action))
            }
            UnauthorizedErrorKind::Attribute(attribute) => Some(format!(
                "You must have the {} attribute to {}",
                attribute.to_string(),
                self.action
            )),
            UnauthorizedErrorKind::Unstructured { reason } => {
                Some(format!("{} to {}", reason, self.action))
            }
        }
    }
}

impl Error for UnauthorizedError {}

/// Checks if a session is authorized to execute a command. If not, an error is returned.
///
/// Note: The session and role ID are stored in the command itself.
pub fn check_command(catalog: &Catalog, cmd: &Command) -> Result<(), AdapterError> {
    if !catalog.system_config().enable_rbac_checks() {
        return Ok(());
    }

    let role = if !matches!(cmd, Command::Startup { .. }) {
        if let Some(session) = cmd.session() {
            let conn_catalog = catalog.for_session(session);
            let role_id = session.role_id();
            let role = conn_catalog.try_get_role(role_id);
            if role.is_none() {
                // PostgreSQL allows users that have their role dropped to perform some actions,
                // such as `SET ROLE` and certain `SELECT` queries. We haven't implemented
                // `SET ROLE` and feel it's safer to force to user to re-authenticate if their
                // role is dropped.
                return Err(AdapterError::ConcurrentRoleDrop(role_id.clone()));
            }
            role
        } else {
            None
        }
    } else {
        None
    };

    match cmd {
        Command::DumpCatalog { session, .. } => {
            if session.is_superuser() {
                Ok(())
            } else {
                Err(AdapterError::Unauthorized(UnauthorizedError::superuser(
                    "dump catalog".into(),
                )))
            }
        }
        Command::Execute {
            portal_name,
            session,
            ..
        } => {
            if let Some(portal) = session.get_portal_unverified(portal_name) {
                if let Some(stmt) = &portal.stmt {
                    if let Some(required_attribute) = generate_statement_attribute(stmt) {
                        let role = role.expect("role checked to exist for all commands with a session that are not Startup");
                        if !required_attribute.check_role(role) {
                            return Err(AdapterError::Unauthorized(UnauthorizedError::attribute(
                                stmt.name().to_string(),
                                required_attribute,
                            )));
                        }
                    }
                }
            }
        }
        Command::Startup { .. }
        | Command::Declare { .. }
        | Command::Describe { .. }
        | Command::VerifyPreparedStatement { .. }
        | Command::StartTransaction { .. }
        | Command::Commit { .. }
        | Command::CancelRequest { .. }
        | Command::CopyRows { .. }
        | Command::GetSystemVars { .. }
        | Command::SetSystemVars { .. }
        | Command::Terminate { .. } => Ok(()),
    }
}

/// Checks if a session is authorized to execute a plan. If not, an error is returned.
pub fn check_plan(
    catalog: &impl SessionCatalog,
    session: &Session,
    plan: &Plan,
) -> Result<(), AdapterError> {
    if !catalog.rbac_checks_enabled() {
        return Ok(());
    }

    if session.is_superuser() {
        return Ok(());
    }

    if let Some(required_attribute) = generate_plan_attribute(plan) {
        if !required_attribute.check_role(role) {
            return Err(AdapterError::Unauthorized(UnauthorizedError::attribute(
                plan.name().to_string(),
                required_attribute,
            )));
        }
    }

    Ok(())
}

/// Generates the attributes required to execute a given statement.
fn generate_statement_attribute(stmt: &Statement<Raw>) -> Option<Attribute> {
    match stmt {
        Statement::CreateDatabase(_) => Some(Attribute::CreateDB),
        Statement::CreateRole(_) | Statement::AlterRole(_) | Statement::DropRoles(_) => {
            Some(Attribute::CreateRole)
        }
        Statement::CreateCluster(_) => Some(Attribute::CreateCluster),
        Statement::Select(_)
        | Statement::Insert(_)
        | Statement::Copy(_)
        | Statement::Update(_)
        | Statement::Delete(_)
        | Statement::CreateConnection(_)
        | Statement::CreateSchema(_)
        | Statement::CreateSource(_)
        | Statement::CreateSubsource(_)
        | Statement::CreateSink(_)
        | Statement::CreateView(_)
        | Statement::CreateMaterializedView(_)
        | Statement::CreateTable(_)
        | Statement::CreateIndex(_)
        | Statement::CreateType(_)
        | Statement::CreateClusterReplica(_)
        | Statement::CreateSecret(_)
        | Statement::AlterObjectRename(_)
        | Statement::AlterIndex(_)
        | Statement::AlterSecret(_)
        | Statement::AlterSink(_)
        | Statement::AlterSource(_)
        | Statement::AlterSystemSet(_)
        | Statement::AlterSystemReset(_)
        | Statement::AlterSystemResetAll(_)
        | Statement::AlterConnection(_)
        | Statement::Discard(_)
        | Statement::DropDatabase(_)
        | Statement::DropSchema(_)
        | Statement::DropObjects(_)
        | Statement::DropClusters(_)
        | Statement::DropClusterReplicas(_)
        | Statement::SetVariable(_)
        | Statement::ResetVariable(_)
        | Statement::Show(_)
        | Statement::StartTransaction(_)
        | Statement::SetTransaction(_)
        | Statement::Commit(_)
        | Statement::Rollback(_)
        | Statement::Subscribe(_)
        | Statement::Explain(_)
        | Statement::Declare(_)
        | Statement::Fetch(_)
        | Statement::Close(_)
        | Statement::Prepare(_)
        | Statement::Execute(_)
        | Statement::Deallocate(_)
        | Statement::Raise(_) => None,
    }
}

/// Generates the attributes required to execute a given plan.
fn generate_plan_attribute(plan: &Plan) -> Option<Attribute> {
    match plan {
        Plan::CreateDatabase(_) => Some(Attribute::CreateDB),
        Plan::CreateRole(_) | Plan::AlterRole(_) | Plan::DropRoles(_) => {
            Some(Attribute::CreateRole)
        }
        Plan::CreateCluster(_) => Some(Attribute::CreateCluster),
        Plan::CreateSource(_)
        | Plan::CreateTable(_)
        | Plan::CreateMaterializedView(_)
        | Plan::CreateConnection(_)
        | Plan::CreateSchema(_)
        | Plan::CreateClusterReplica(_)
        | Plan::CreateSecret(_)
        | Plan::CreateSink(_)
        | Plan::CreateView(_)
        | Plan::CreateIndex(_)
        | Plan::CreateType(_)
        | Plan::DiscardTemp
        | Plan::DiscardAll
        | Plan::DropDatabase(_)
        | Plan::DropSchema(_)
        | Plan::DropClusters(_)
        | Plan::DropClusterReplicas(_)
        | Plan::DropItems(_)
        | Plan::EmptyQuery
        | Plan::ShowAllVariables
        | Plan::ShowVariable(_)
        | Plan::SetVariable(_)
        | Plan::ResetVariable(_)
        | Plan::StartTransaction(_)
        | Plan::CommitTransaction
        | Plan::AbortTransaction
        | Plan::Peek(_)
        | Plan::Subscribe(_)
        | Plan::SendRows(_)
        | Plan::CopyFrom(_)
        | Plan::Explain(_)
        | Plan::SendDiffs(_)
        | Plan::Insert(_)
        | Plan::AlterNoop(_)
        | Plan::AlterIndexSetOptions(_)
        | Plan::AlterIndexResetOptions(_)
        | Plan::AlterSink(_)
        | Plan::AlterSource(_)
        | Plan::AlterItemRename(_)
        | Plan::AlterSecret(_)
        | Plan::AlterSystemSet(_)
        | Plan::AlterSystemReset(_)
        | Plan::AlterSystemResetAll(_)
        | Plan::Declare(_)
        | Plan::Fetch(_)
        | Plan::Close(_)
        | Plan::ReadThenWrite(_)
        | Plan::Prepare(_)
        | Plan::Execute(_)
        | Plan::Deallocate(_)
        | Plan::Raise(_)
        | Plan::RotateKeys(_) => None,
    }
}
