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
            Attribute::CreateRole => f.write_str("CREATEROLE"),
            Attribute::CreateDB => f.write_str("CREATEDB"),
            Attribute::CreateCluster => f.write_str("CREATECLUSTER"),
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
    Privilege { reason: Option<String> },
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

    pub fn privilege(action: String, reason: Option<String>) -> UnauthorizedError {
        UnauthorizedError {
            action,
            kind: UnauthorizedErrorKind::Privilege { reason },
        }
    }

    pub fn detail(&self) -> Option<String> {
        match &self.kind {
            UnauthorizedErrorKind::Superuser => {
                Some(format!("You must be a superuser to {}", self.action))
            }
            UnauthorizedErrorKind::Attribute(attribute) => Some(format!(
                "You must have the {} attribute to {}",
                attribute, self.action
            )),
            UnauthorizedErrorKind::Privilege { reason } => reason
                .as_ref()
                .map(|reason| format!("{} to {}", reason, self.action)),
        }
    }
}

impl Error for UnauthorizedError {}

/// Checks if a session is authorized to execute a command. If not, an error is returned.
///
/// Note: The session and role ID are stored in the command itself.
pub fn check_command(catalog: &Catalog, cmd: &Command) -> Result<(), UnauthorizedError> {
    if !catalog.system_config().enable_rbac_checks() {
        return Ok(());
    }

    match cmd {
        Command::DumpCatalog { session, .. } => {
            if session.is_superuser() {
                Ok(())
            } else {
                Err(UnauthorizedError::superuser("dump catalog".into()))
            }
        }
        Command::Startup { .. }
        | Command::Declare { .. }
        | Command::Describe { .. }
        | Command::VerifyPreparedStatement { .. }
        | Command::Execute { .. }
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
    if !catalog.system_vars().enable_rbac_checks() {
        return Ok(());
    }

    if session.is_superuser() {
        return Ok(());
    }

    let role_id = session.role_id();
    let Some(role) = catalog.try_get_role(role_id) else {
        // PostgreSQL allows users that have their role dropped to perform some actions,
        // such as `SET ROLE` and certain `SELECT` queries. We haven't implemented
        // `SET ROLE` and feel it's safer to force to user to re-authenticate if their
        // role is dropped.
        return Err(AdapterError::ConcurrentRoleDrop(role_id.clone()));
    };

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
