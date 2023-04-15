// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Formatter;

use itertools::Itertools;

use mz_ore::str::StrExt;
use mz_repr::role_id::RoleId;
use mz_sql::catalog::SessionCatalog;
use mz_sql::names::{ObjectId, ResolvedDatabaseSpecifier, SchemaSpecifier};
use mz_sql::plan::{AlterOwnerPlan, CreateMaterializedViewPlan, CreateViewPlan, Plan};
use mz_sql::session::vars::SystemVars;
use mz_sql_parser::ast::{ObjectType, QualifiedReplica};

use crate::catalog::Catalog;
use crate::command::Command;
use crate::session::Session;
use crate::AdapterError;

/// Errors that can occur due to an unauthorized action.
#[derive(Debug, thiserror::Error)]
pub enum UnauthorizedError {
    /// The action can only be performed by a superuser.
    #[error("permission denied to {action}")]
    Superuser { action: String },
    /// The action requires a specific attribute.
    #[error("permission denied to {action}")]
    Attribute {
        action: String,
        attribute: Attribute,
    },
    /// The action requires ownership of an object.
    #[error("must be owner of {}", objects.iter().map(|(object_type, object_name)| format!("{object_type} {object_name}")).join(", "))]
    Ownership { objects: Vec<(ObjectType, String)> },
    /// Altering an owner requires membership of the new owner role.
    #[error("must be a member of {}", role_name.to_string().quoted())]
    AlterOwnerMembership { role_name: String },
    /// The action requires one or more privileges.
    #[error("permission denied to {action}")]
    Privilege {
        action: String,
        reason: Option<String>,
    },
}

impl UnauthorizedError {
    pub fn detail(&self) -> Option<String> {
        match &self {
            UnauthorizedError::Superuser { action } => {
                Some(format!("You must be a superuser to {}", action))
            }
            UnauthorizedError::Attribute { action, attribute } => Some(format!(
                "You must have the {} attribute to {}",
                attribute, action
            )),
            UnauthorizedError::Privilege { action, reason } => reason
                .as_ref()
                .map(|reason| format!("{} to {}", reason, action)),
            UnauthorizedError::Ownership { .. }
            | UnauthorizedError::AlterOwnerMembership { .. } => None,
        }
    }
}

/// Checks if a session is authorized to execute a command. If not, an error is returned.
///
/// Note: The session and role ID are stored in the command itself.
pub fn check_command(catalog: &Catalog, cmd: &Command) -> Result<(), UnauthorizedError> {
    if let Some(session) = cmd.session() {
        if !is_rbac_enabled_for_session(catalog.system_config(), session) {
            return Ok(());
        }
    } else if !is_rbac_enabled_for_system(catalog.system_config()) {
        return Ok(());
    }

    match cmd {
        Command::DumpCatalog { session, .. } => {
            if session.is_superuser() {
                Ok(())
            } else {
                Err(UnauthorizedError::Superuser {
                    action: "dump catalog".into(),
                })
            }
        }
        Command::Startup { .. }
        | Command::Declare { .. }
        | Command::Describe { .. }
        | Command::VerifyPreparedStatement { .. }
        | Command::Execute { .. }
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
    let role_id = session.role_id();
    if catalog.try_get_role(role_id).is_none() {
        // PostgreSQL allows users that have their role dropped to perform some actions,
        // such as `SET ROLE` and certain `SELECT` queries. We haven't implemented
        // `SET ROLE` and feel it's safer to force to user to re-authenticate if their
        // role is dropped.
        return Err(AdapterError::ConcurrentRoleDrop(role_id.clone()));
    };

    if !is_rbac_enabled_for_session(catalog.system_vars(), session) {
        return Ok(());
    }

    if session.is_superuser() {
        return Ok(());
    }

    // Obtain all roles that the current session is a member of.
    let role_membership = catalog.collect_role_membership(role_id);

    // Validate that the current role has the required membership to alter an object's owner.
    if let Plan::AlterOwner(AlterOwnerPlan { new_owner, .. }) = plan {
        if !role_membership.contains(new_owner) {
            return Err(AdapterError::Unauthorized(
                UnauthorizedError::AlterOwnerMembership {
                    role_name: catalog.get_role(new_owner).name().to_string(),
                },
            ));
        }
    }

    // Validate that the current session has the required attributes to execute the provided plan.
    // Note: role attributes are not inherited by role membership.
    if let Some(required_attribute) = generate_required_plan_attribute(plan) {
        if !required_attribute.check_role(role_id, catalog) {
            return Err(AdapterError::Unauthorized(UnauthorizedError::Attribute {
                action: plan.name().to_string(),
                attribute: required_attribute,
            }));
        }
    }

    // Validate that the current session has the required object ownership to execute the provided
    // plan.
    let required_ownership = generate_required_ownership(plan);
    let unheld_ownership = required_ownership
        .into_iter()
        .filter(|ownership| !check_owner_roles(ownership, &role_membership, catalog))
        .collect();
    ownership_err(unheld_ownership, catalog)?;

    Ok(())
}

/// Returns true if RBAC is turned on for a session, false otherwise.
pub fn is_rbac_enabled_for_session(system_vars: &SystemVars, session: &Session) -> bool {
    let ld_enabled = system_vars.enable_ld_rbac_checks();
    let server_enabled = system_vars.enable_rbac_checks();
    let session_enabled = session.vars().enable_session_rbac_checks();

    // The LD flag acts as a global off switch in case we need to turn the feature off for
    // everyone. Users will still need to turn one of the non-LD flags on to enable RBAC.
    // The session flag allows users to turn RBAC on for just their session while the server flag
    // allows users to turn RBAC on for everyone.
    ld_enabled && (server_enabled || session_enabled)
}

/// Returns true if RBAC is turned on for the system, false otherwise.
pub fn is_rbac_enabled_for_system(system_vars: &SystemVars) -> bool {
    let ld_enabled = system_vars.enable_ld_rbac_checks();
    let server_enabled = system_vars.enable_rbac_checks();

    // The LD flag acts as a global off switch in case we need to turn the feature off for
    // everyone. Users will still need to turn one of the non-LD flags on to enable RBAC.
    // The server flag allows users to turn RBAC on for everyone.
    ld_enabled && server_enabled
}

/// Generates the attributes required to execute a given plan.
fn generate_required_plan_attribute(plan: &Plan) -> Option<Attribute> {
    match plan {
        Plan::CreateDatabase(_) => Some(Attribute::CreateDB),
        Plan::CreateCluster(_) => Some(Attribute::CreateCluster),
        Plan::CreateRole(_) | Plan::AlterRole(_) | Plan::GrantRole(_) | Plan::RevokeRole(_) => {
            Some(Attribute::CreateRole)
        }
        Plan::DropObjects(plan) if plan.object_type == ObjectType::Role => {
            Some(Attribute::CreateRole)
        }
        Plan::CreateSource(_)
        | Plan::CreateSources(_)
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
        | Plan::DropObjects(_)
        | Plan::EmptyQuery
        | Plan::ShowAllVariables
        | Plan::ShowCreate(_)
        | Plan::ShowVariable(_)
        | Plan::SetVariable(_)
        | Plan::ResetVariable(_)
        | Plan::StartTransaction(_)
        | Plan::CommitTransaction(_)
        | Plan::AbortTransaction(_)
        | Plan::Peek(_)
        | Plan::Subscribe(_)
        | Plan::SendRows(_)
        | Plan::CopyRows(_)
        | Plan::CopyFrom(_)
        | Plan::Explain(_)
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
        | Plan::AlterOwner(_)
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
    fn check_role(&self, role_id: &RoleId, catalog: &impl SessionCatalog) -> bool {
        let role = catalog.get_role(role_id);
        match self {
            Attribute::CreateRole => role.create_role(),
            Attribute::CreateDB => role.create_db(),
            Attribute::CreateCluster => role.create_cluster(),
        }
    }
}

impl fmt::Display for Attribute {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Attribute::CreateRole => f.write_str("CREATEROLE"),
            Attribute::CreateDB => f.write_str("CREATEDB"),
            Attribute::CreateCluster => f.write_str("CREATECLUSTER"),
        }
    }
}

/// Generates the ownership required to execute a given plan.
fn generate_required_ownership(plan: &Plan) -> Vec<ObjectId> {
    match plan {
        Plan::CreateConnection(_)
        | Plan::CreateDatabase(_)
        | Plan::CreateSchema(_)
        | Plan::CreateRole(_)
        | Plan::CreateCluster(_)
        | Plan::CreateClusterReplica(_)
        | Plan::CreateSource(_)
        | Plan::CreateSources(_)
        | Plan::CreateSecret(_)
        | Plan::CreateSink(_)
        | Plan::CreateTable(_)
        | Plan::CreateIndex(_)
        | Plan::CreateType(_)
        | Plan::DiscardTemp
        | Plan::DiscardAll
        | Plan::EmptyQuery
        | Plan::ShowAllVariables
        | Plan::ShowVariable(_)
        | Plan::SetVariable(_)
        | Plan::ResetVariable(_)
        | Plan::StartTransaction(_)
        | Plan::CommitTransaction(_)
        | Plan::AbortTransaction(_)
        | Plan::Peek(_)
        | Plan::Subscribe(_)
        | Plan::SendRows(_)
        | Plan::ShowCreate(_)
        | Plan::CopyFrom(_)
        | Plan::CopyRows(_)
        | Plan::Explain(_)
        | Plan::Insert(_)
        | Plan::AlterNoop(_)
        | Plan::AlterSystemSet(_)
        | Plan::AlterSystemReset(_)
        | Plan::AlterSystemResetAll(_)
        | Plan::AlterRole(_)
        | Plan::Declare(_)
        | Plan::Fetch(_)
        | Plan::Close(_)
        | Plan::ReadThenWrite(_)
        | Plan::Prepare(_)
        | Plan::Execute(_)
        | Plan::Deallocate(_)
        | Plan::Raise(_)
        | Plan::GrantRole(_)
        | Plan::RevokeRole(_) => Vec::new(),
        Plan::CreateView(CreateViewPlan { replace, .. })
        | Plan::CreateMaterializedView(CreateMaterializedViewPlan { replace, .. }) => {
            replace.iter().map(|id| ObjectId::Item(*id)).collect()
        }
        Plan::DropObjects(plan) => plan.ids.clone(),
        Plan::AlterIndexSetOptions(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterIndexResetOptions(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterSink(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterSource(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterItemRename(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterSecret(plan) => vec![ObjectId::Item(plan.id)],
        Plan::RotateKeys(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterOwner(plan) => vec![plan.id.clone()],
    }
}

/// Reports whether any role has ownership over an object.
fn check_owner_roles(
    object_id: &ObjectId,
    role_ids: &BTreeSet<RoleId>,
    catalog: &impl SessionCatalog,
) -> bool {
    if let Some(owner_id) = catalog.get_owner_id(object_id) {
        role_ids.contains(&owner_id)
    } else {
        true
    }
}

fn ownership_err(
    unheld_ownership: Vec<ObjectId>,
    catalog: &impl SessionCatalog,
) -> Result<(), UnauthorizedError> {
    if !unheld_ownership.is_empty() {
        let objects = unheld_ownership
            .into_iter()
            .map(|ownership| match ownership {
                ObjectId::Cluster(id) => (
                    ObjectType::Cluster,
                    catalog.get_cluster(id).name().to_string(),
                ),
                ObjectId::ClusterReplica((cluster_id, replica_id)) => {
                    let cluster = catalog.get_cluster(cluster_id);
                    let replica = catalog.get_cluster_replica(cluster_id, replica_id);
                    let name = QualifiedReplica {
                        cluster: cluster.name().into(),
                        replica: replica.name().into(),
                    };
                    (ObjectType::ClusterReplica, name.to_string())
                }
                ObjectId::Database(id) => (
                    ObjectType::Database,
                    catalog.get_database(&id).name().to_string(),
                ),
                ObjectId::Schema((database_id, schema_id)) => {
                    let schema = catalog.get_schema(
                        &ResolvedDatabaseSpecifier::Id(database_id),
                        &SchemaSpecifier::Id(schema_id),
                    );
                    let name = catalog.resolve_full_schema_name(schema.name());
                    (ObjectType::Schema, name.to_string())
                }
                ObjectId::Item(id) => {
                    let item = catalog.get_item(&id);
                    let name = catalog.resolve_full_name(item.name());
                    (item.item_type().into(), name.to_string())
                }
                ObjectId::Role(_) => unreachable!("roles have no owner"),
            })
            .collect();
        Err(UnauthorizedError::Ownership { objects })
    } else {
        Ok(())
    }
}
