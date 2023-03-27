// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fmt::Formatter;

use itertools::Itertools;

use mz_sql::catalog::SessionCatalog;
use mz_sql::names::{ObjectId, ResolvedDatabaseSpecifier, RoleId, SchemaSpecifier};
use mz_sql::plan::Plan;
use mz_sql_parser::ast::{ObjectType, QualifiedReplica};

use crate::catalog::Catalog;
use crate::command::Command;
use crate::session::Session;
use crate::AdapterError;

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

    /// Reports whether any role has the privilege granted by the attribute.
    fn check_roles(&self, role_ids: &BTreeSet<RoleId>, catalog: &impl SessionCatalog) -> bool {
        role_ids
            .iter()
            .any(|role_id| self.check_role(role_id, catalog))
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

#[derive(Debug)]
pub struct Ownership(ObjectId);

impl Ownership {
    /// Reports whether a role has ownership over an object.
    fn check_role(&self, role_id: &RoleId, catalog: &impl SessionCatalog) -> bool {
        match self.0 {
            ObjectId::Cluster(id) => catalog.get_cluster(id).owner_id() == *role_id,
            ObjectId::ClusterReplica((cluster_id, replica_id)) => {
                catalog
                    .get_cluster_replica(cluster_id, replica_id)
                    .owner_id()
                    == *role_id
            }
            ObjectId::Database(id) => catalog.get_database(&id).owner_id() == *role_id,
            ObjectId::Schema((database_id, schema_id)) => {
                catalog
                    .get_schema(
                        &ResolvedDatabaseSpecifier::Id(database_id),
                        &SchemaSpecifier::Id(schema_id),
                    )
                    .owner_id()
                    == *role_id
            }
            ObjectId::Item(id) => catalog.get_item(&id).owner_id() == *role_id,
        }
    }

    /// Reports whether any role has ownership over an object.
    fn check_roles(&self, role_ids: &BTreeSet<RoleId>, catalog: &impl SessionCatalog) -> bool {
        role_ids
            .iter()
            .any(|role_id| self.check_role(role_id, catalog))
    }
}

/// Errors that can occur due to an unauthorized action.
#[derive(Debug)]
pub enum UnauthorizedError {
    /// The action can only be performed by a superuser.
    Superuser { action: String },
    /// The action requires a specific attribute.
    Attribute {
        action: String,
        attribute: Attribute,
    },
    /// The action requires ownership of an object.
    Ownership { objects: Vec<(ObjectType, String)> },
    /// The action requires one or more privileges.
    Privilege {
        action: String,
        reason: Option<String>,
    },
}

impl UnauthorizedError {
    pub fn superuser(action: String) -> UnauthorizedError {
        UnauthorizedError::Superuser { action }
    }

    pub fn attribute(action: String, attribute: Attribute) -> UnauthorizedError {
        UnauthorizedError::Attribute { action, attribute }
    }

    pub fn ownership(objects: Vec<(ObjectType, String)>) -> UnauthorizedError {
        UnauthorizedError::Ownership { objects }
    }

    pub fn privilege(action: String, reason: Option<String>) -> UnauthorizedError {
        UnauthorizedError::Privilege { action, reason }
    }

    pub fn detail(&self) -> Option<String> {
        match &self {
            UnauthorizedError::Superuser { action } => {
                Some(format!("You must be a superuser to {}", action))
            }
            UnauthorizedError::Attribute { action, attribute } => Some(format!(
                "You must have the {} attribute to {}",
                attribute, action
            )),
            UnauthorizedError::Ownership { .. } => None,
            UnauthorizedError::Privilege { action, reason } => reason
                .as_ref()
                .map(|reason| format!("{} to {}", reason, action)),
        }
    }
}

impl Error for UnauthorizedError {}

impl fmt::Display for UnauthorizedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            UnauthorizedError::Superuser { action }
            | UnauthorizedError::Attribute { action, .. }
            | UnauthorizedError::Privilege { action, .. } => {
                write!(f, "permission denied to {}", action)
            }
            UnauthorizedError::Ownership { objects } => {
                let mut objects = objects
                    .iter()
                    .map(|(object_type, object_name)| format!("{object_type} {object_name}"));
                write!(f, "must be owner of {}", objects.join(", "))
            }
        }
    }
}

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

    if !catalog.system_vars().enable_rbac_checks() {
        return Ok(());
    }

    if session.is_superuser() {
        return Ok(());
    }

    // Obtain all roles that the current session is a member of.
    let role_membership = catalog.collect_role_membership(role_id);

    // Validate that the current session has the required attributes to execute the provided plan.
    if let Some(required_attribute) = generate_required_plan_attribute(plan) {
        if !required_attribute.check_roles(&role_membership, catalog) {
            return Err(AdapterError::Unauthorized(UnauthorizedError::attribute(
                plan.name().to_string(),
                required_attribute,
            )));
        }
    }

    // Validate that the current session has the required object ownership to execute the provided
    // plan.
    let required_ownership = generate_required_ownership(plan);
    let unheld_ownership: Vec<Ownership> = required_ownership
        .into_iter()
        .filter(|ownership| !ownership.check_roles(&role_membership, catalog))
        .collect();
    if !unheld_ownership.is_empty() {
        let objects = unheld_ownership
            .into_iter()
            .map(|ownership| match ownership.0 {
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
            })
            .collect();
        return Err(AdapterError::Unauthorized(UnauthorizedError::ownership(
            objects,
        )));
    }

    Ok(())
}

/// Generates the attributes required to execute a given plan.
fn generate_required_plan_attribute(plan: &Plan) -> Option<Attribute> {
    match plan {
        Plan::CreateDatabase(_) => Some(Attribute::CreateDB),
        Plan::CreateCluster(_) => Some(Attribute::CreateCluster),
        Plan::CreateRole(_)
        | Plan::AlterRole(_)
        | Plan::DropRoles(_)
        | Plan::GrantRole(_)
        | Plan::RevokeRole(_) => Some(Attribute::CreateRole),
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
        | Plan::DropDatabase(_)
        | Plan::DropSchema(_)
        | Plan::DropClusters(_)
        | Plan::DropClusterReplicas(_)
        | Plan::DropItems(_)
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

/// Generates the ownership required to execute a given plan.
fn generate_required_ownership(plan: &Plan) -> Vec<Ownership> {
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
        | Plan::CreateView(_)
        | Plan::CreateMaterializedView(_)
        | Plan::CreateIndex(_)
        | Plan::CreateType(_)
        | Plan::DiscardTemp
        | Plan::DiscardAll
        | Plan::DropRoles(_)
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
        Plan::DropDatabase(plan) => match plan.id {
            Some(id) => vec![Ownership(ObjectId::Database(id))],
            None => Vec::new(),
        },
        Plan::DropSchema(plan) => match plan.id {
            Some((database_id, schema_id)) => {
                vec![Ownership(ObjectId::Schema((database_id, schema_id)))]
            }
            None => Vec::new(),
        },
        Plan::DropClusters(plan) => plan
            .ids
            .iter()
            .map(|id| Ownership(ObjectId::Cluster(*id)))
            .collect(),
        Plan::DropClusterReplicas(plan) => plan
            .ids
            .iter()
            .map(|(cluster_id, replica_id)| {
                Ownership(ObjectId::ClusterReplica((*cluster_id, *replica_id)))
            })
            .collect(),
        Plan::DropItems(plan) => plan
            .items
            .iter()
            .map(|id| Ownership(ObjectId::Item(*id)))
            .collect(),
        Plan::AlterIndexSetOptions(plan) => vec![Ownership(ObjectId::Item(plan.id))],
        Plan::AlterIndexResetOptions(plan) => vec![Ownership(ObjectId::Item(plan.id))],
        Plan::AlterSink(plan) => vec![Ownership(ObjectId::Item(plan.id))],
        Plan::AlterSource(plan) => vec![Ownership(ObjectId::Item(plan.id))],
        Plan::AlterItemRename(plan) => vec![Ownership(ObjectId::Item(plan.id))],
        Plan::AlterSecret(plan) => vec![Ownership(ObjectId::Item(plan.id))],
        Plan::RotateKeys(plan) => vec![Ownership(ObjectId::Item(plan.id))],
        Plan::AlterOwner(plan) => vec![Ownership(plan.id.clone())],
    }
}
