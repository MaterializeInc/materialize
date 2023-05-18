// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Formatter;
use std::{fmt, iter};

use itertools::Itertools;
use mz_controller::clusters::ClusterId;
use mz_expr::{CollectionPlan, MirRelationExpr};
use mz_ore::str::StrExt;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::catalog::{CatalogItemType, RoleAttributes, SessionCatalog};
use mz_sql::names::{ObjectId, QualifiedItemName, ResolvedDatabaseSpecifier};
use mz_sql::plan::{
    AbortTransactionPlan, AlterIndexResetOptionsPlan, AlterIndexSetOptionsPlan,
    AlterItemRenamePlan, AlterNoopPlan, AlterOwnerPlan, AlterRolePlan, AlterSecretPlan,
    AlterSinkPlan, AlterSourcePlan, AlterSystemResetAllPlan, AlterSystemResetPlan,
    AlterSystemSetPlan, ClosePlan, CommitTransactionPlan, CopyFromPlan, CopyRowsPlan,
    CreateClusterPlan, CreateClusterReplicaPlan, CreateConnectionPlan, CreateDatabasePlan,
    CreateIndexPlan, CreateMaterializedViewPlan, CreateRolePlan, CreateSchemaPlan,
    CreateSecretPlan, CreateSinkPlan, CreateSourcePlan, CreateSourcePlans, CreateTablePlan,
    CreateTypePlan, CreateViewPlan, DeallocatePlan, DeclarePlan, DropObjectsPlan, DropOwnedPlan,
    ExecutePlan, ExplainPlan, FetchPlan, GrantPrivilegePlan, GrantRolePlan, InsertPlan,
    MutationKind, PeekPlan, Plan, PlannedRoleAttributes, PreparePlan, RaisePlan, ReadThenWritePlan,
    ReassignOwnedPlan, ResetVariablePlan, RevokePrivilegePlan, RevokeRolePlan, RotateKeysPlan,
    SetVariablePlan, ShowCreatePlan, ShowVariablePlan, SourceSinkClusterConfig,
    StartTransactionPlan, SubscribePlan,
};
use mz_sql::session::user::{INTROSPECTION_USER, SYSTEM_USER};
use mz_sql::session::vars::SystemVars;
use mz_sql_parser::ast::{ObjectType, QualifiedReplica};

use crate::catalog::storage::MZ_SYSTEM_ROLE_ID;
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
        attributes: Vec<Attribute>,
    },
    /// The action requires ownership of an object.
    #[error("must be owner of {}", objects.iter().map(|(object_type, object_name)| format!("{object_type} {object_name}")).join(", "))]
    Ownership { objects: Vec<(ObjectType, String)> },
    /// Altering an owner requires membership of the new owner role.
    #[error("must be a member of {}", role_names.iter().map(|role| role.quoted()).join(", "))]
    RoleMembership { role_names: Vec<String> },
    /// The action requires one or more privileges.
    #[error("permission denied for {object_type} {object_name}")]
    Privilege {
        object_type: ObjectType,
        object_name: String,
    },
    // TODO(jkosh44) When we implement parameter privileges, this can be replaced with a regular
    //  privilege error.
    /// The action can only be performed by the mz_system role.
    #[error("permission denied to {action}")]
    MzSystem { action: String },
    /// The action cannot be performed by the mz_introspection role.
    #[error("permission denied to {action}")]
    MzIntrospection { action: String },
}

impl UnauthorizedError {
    pub fn detail(&self) -> Option<String> {
        match &self {
            UnauthorizedError::Superuser { action } => {
                Some(format!("You must be a superuser to {}", action))
            }
            UnauthorizedError::Attribute { action, attributes } => Some(format!(
                "You must have the {} attribute{} to {}",
                attributes.iter().join(", "),
                if attributes.len() > 1 { "s" } else { "" },
                action
            )),
            UnauthorizedError::MzSystem { .. } => {
                Some(format!("You must be the '{}' role", SYSTEM_USER.name))
            }
            UnauthorizedError::MzIntrospection { .. } => Some(format!(
                "The '{}' role has very limited privileges",
                INTROSPECTION_USER.name
            )),
            UnauthorizedError::Ownership { .. }
            | UnauthorizedError::RoleMembership { .. }
            | UnauthorizedError::Privilege { .. } => None,
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
    target_cluster_id: Option<ClusterId>,
    depends_on: &Vec<GlobalId>,
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

    // Validate that the current session has the required role membership to execute the provided
    // plan.
    let required_membership: BTreeSet<_> = generate_required_role_membership(plan)
        .into_iter()
        .collect();
    let unheld_membership: Vec<_> = required_membership.difference(&role_membership).collect();
    if !unheld_membership.is_empty() {
        let role_names = unheld_membership
            .into_iter()
            .map(|role_id| catalog.get_role(role_id).name().to_string())
            .collect();
        return Err(AdapterError::Unauthorized(
            UnauthorizedError::RoleMembership { role_names },
        ));
    }

    // Validate that the current session has the required attributes to execute the provided plan.
    // Note: role attributes are not inherited by role membership.
    let required_attributes = generate_required_plan_attribute(plan);
    let unheld_attributes: Vec<_> = required_attributes
        .into_iter()
        .filter(|attribute| !attribute.check_role(role_id, catalog))
        .collect();
    attribute_err(unheld_attributes, plan)?;

    // Validate that the current session has the required object ownership to execute the provided
    // plan.
    let required_ownership = generate_required_ownership(plan);
    let unheld_ownership = required_ownership
        .into_iter()
        .filter(|ownership| !check_owner_roles(ownership, &role_membership, catalog))
        .collect();
    ownership_err(unheld_ownership, catalog)?;

    let required_privileges =
        generate_required_privileges(catalog, plan, target_cluster_id, depends_on, *role_id);
    let mut role_memberships = BTreeMap::new();
    role_memberships.insert(*role_id, role_membership);
    check_object_privileges(catalog, required_privileges, role_memberships)?;

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

/// Generates the role membership required to execute a give plan.
pub fn generate_required_role_membership(plan: &Plan) -> Vec<RoleId> {
    match plan {
        Plan::AlterOwner(AlterOwnerPlan { new_owner, .. }) => vec![*new_owner],
        Plan::DropOwned(DropOwnedPlan { role_ids, .. }) => role_ids.clone(),
        Plan::ReassignOwned(ReassignOwnedPlan {
            old_roles,
            new_role,
            ..
        }) => {
            let mut roles = old_roles.clone();
            roles.push(*new_role);
            roles
        }
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
        | Plan::CopyFrom(_)
        | Plan::CopyRows(_)
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
        | Plan::AlterRole(_)
        | Plan::Declare(_)
        | Plan::Fetch(_)
        | Plan::Close(_)
        | Plan::ReadThenWrite(_)
        | Plan::Prepare(_)
        | Plan::Execute(_)
        | Plan::Deallocate(_)
        | Plan::Raise(_)
        | Plan::RotateKeys(_)
        | Plan::GrantRole(_)
        | Plan::RevokeRole(_)
        | Plan::GrantPrivilege(_)
        | Plan::RevokePrivilege(_) => Vec::new(),
    }
}

/// Generates the attributes required to execute a given plan.
fn generate_required_plan_attribute(plan: &Plan) -> Vec<Attribute> {
    match plan {
        Plan::CreateDatabase(_) => vec![Attribute::CreateDB],
        Plan::CreateCluster(_) => vec![Attribute::CreateCluster],
        Plan::CreateRole(CreateRolePlan { attributes, .. }) => {
            let mut attributes = Attribute::from_role_attributes(attributes);
            if !attributes.contains(&Attribute::CreateRole) {
                attributes.push(Attribute::CreateRole);
            }
            attributes
        }
        Plan::AlterRole(AlterRolePlan { attributes, .. }) => {
            let mut attributes = Attribute::from_planned_role_attributes(attributes);
            if !attributes.contains(&Attribute::CreateRole) {
                attributes.push(Attribute::CreateRole);
            }
            attributes
        }
        Plan::GrantRole(_) | Plan::RevokeRole(_) => vec![Attribute::CreateRole],
        Plan::DropObjects(plan) if plan.object_type == ObjectType::Role => {
            vec![Attribute::CreateRole]
        }
        Plan::CreateSource(CreateSourcePlan { cluster_config, .. })
        | Plan::CreateSink(CreateSinkPlan { cluster_config, .. }) => {
            if cluster_config.cluster_id().is_none() {
                vec![Attribute::CreateCluster]
            } else {
                Vec::new()
            }
        }
        Plan::CreateSources(plans) => {
            if plans
                .iter()
                .any(|plan| plan.plan.cluster_config.cluster_id().is_none())
            {
                vec![Attribute::CreateCluster]
            } else {
                Vec::new()
            }
        }
        Plan::CreateTable(_)
        | Plan::CreateMaterializedView(_)
        | Plan::CreateConnection(_)
        | Plan::CreateSchema(_)
        | Plan::CreateClusterReplica(_)
        | Plan::CreateSecret(_)
        | Plan::CreateView(_)
        | Plan::CreateIndex(_)
        | Plan::CreateType(_)
        | Plan::DiscardTemp
        | Plan::DiscardAll
        | Plan::DropObjects(_)
        | Plan::DropOwned(_)
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
        | Plan::RotateKeys(_)
        | Plan::GrantPrivilege(_)
        | Plan::RevokePrivilege(_)
        | Plan::ReassignOwned(_) => Vec::new(),
    }
}

/// Attributes that allow a role to execute certain plans.
///
/// Note: This is a subset of all role attributes used for privilege checks.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
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

    fn from_role_attributes(role_attributes: &RoleAttributes) -> Vec<Self> {
        let mut attributes = Vec::new();
        if role_attributes.create_role {
            attributes.push(Attribute::CreateRole);
        }
        if role_attributes.create_db {
            attributes.push(Attribute::CreateDB);
        }
        if role_attributes.create_cluster {
            attributes.push(Attribute::CreateCluster);
        }
        attributes
    }

    fn from_planned_role_attributes(planned_role_attributes: &PlannedRoleAttributes) -> Vec<Self> {
        let mut attributes = Vec::new();
        if let Some(true) = planned_role_attributes.create_role {
            attributes.push(Attribute::CreateRole);
        }
        if let Some(true) = planned_role_attributes.create_db {
            attributes.push(Attribute::CreateDB);
        }
        if let Some(true) = planned_role_attributes.create_cluster {
            attributes.push(Attribute::CreateCluster);
        }
        attributes
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

fn attribute_err(unheld_attributes: Vec<Attribute>, plan: &Plan) -> Result<(), UnauthorizedError> {
    if unheld_attributes.is_empty() {
        return Ok(());
    }

    let mut action = plan.name().to_string();
    // If the plan is `CREATE ROLE` or `ALTER ROLE` then add some more details about the
    // attributes being granted.
    let attributes: Vec<_> = if let Plan::CreateRole(CreateRolePlan { attributes, .. }) = plan {
        Attribute::from_role_attributes(attributes)
            .into_iter()
            .filter(|attribute| unheld_attributes.contains(attribute))
            .collect()
    } else if let Plan::AlterRole(AlterRolePlan { attributes, .. }) = plan {
        Attribute::from_planned_role_attributes(attributes)
            .into_iter()
            .filter(|attribute| unheld_attributes.contains(attribute))
            .collect()
    } else {
        Vec::new()
    };
    if !attributes.is_empty() {
        action = format!(
            "{} with attribute{} {}",
            action,
            if attributes.len() > 1 { "s" } else { "" },
            attributes.iter().join(", ")
        );
    }

    Err(UnauthorizedError::Attribute {
        action,
        attributes: unheld_attributes,
    })
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
        | Plan::RevokeRole(_)
        | Plan::DropOwned(_)
        | Plan::ReassignOwned(_) => Vec::new(),
        Plan::CreateIndex(plan) => vec![ObjectId::Item(plan.index.on)],
        Plan::CreateView(CreateViewPlan { replace, .. })
        | Plan::CreateMaterializedView(CreateMaterializedViewPlan { replace, .. }) => replace
            .map(|id| vec![ObjectId::Item(id)])
            .unwrap_or_default(),
        // Do not need ownership of descendant objects.
        Plan::DropObjects(plan) => plan.referenced_ids.clone(),
        Plan::AlterIndexSetOptions(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterIndexResetOptions(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterSink(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterSource(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterItemRename(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterSecret(plan) => vec![ObjectId::Item(plan.id)],
        Plan::RotateKeys(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterOwner(plan) => vec![plan.id.clone()],
        Plan::GrantPrivilege(plan) => vec![plan.object_id.clone()],
        Plan::RevokePrivilege(plan) => vec![plan.object_id.clone()],
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
                ObjectId::Schema((database_spec, schema_spec)) => {
                    let schema = catalog.get_schema(&database_spec, &schema_spec);
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

/// Generates the privileges required to execute a given plan.
///
/// The result of this function is a set of tuples of the form
/// (What object the privilege is on, What privilege is required, Who must possess the privilege).
fn generate_required_privileges(
    catalog: &impl SessionCatalog,
    plan: &Plan,
    target_cluster_id: Option<ClusterId>,
    depends_on: &Vec<GlobalId>,
    role_id: RoleId,
) -> Vec<(ObjectId, AclMode, RoleId)> {
    // When adding a new plan, make sure that the plan struct is fully expanded. This helps ensure
    // that when someone adds a field to a plan they get a compiler error and must consider any
    // required changes to privileges.
    match plan {
        Plan::CreateConnection(CreateConnectionPlan {
            name,
            if_not_exists: _,
            connection: _,
        }) => {
            let mut privileges = vec![(name.qualifiers.clone().into(), AclMode::CREATE, role_id)];
            privileges.extend(generate_item_usage_privileges(catalog, depends_on, role_id));
            privileges
        }
        Plan::CreateSchema(CreateSchemaPlan {
            database_spec,
            schema_name: _,
            if_not_exists: _,
        }) => match database_spec {
            ResolvedDatabaseSpecifier::Ambient => Vec::new(),
            ResolvedDatabaseSpecifier::Id(database_id) => {
                vec![(database_id.into(), AclMode::CREATE, role_id)]
            }
        },
        Plan::CreateClusterReplica(CreateClusterReplicaPlan {
            cluster_id,
            name: _,
            config: _,
        }) => {
            vec![(cluster_id.into(), AclMode::CREATE, role_id)]
        }
        Plan::CreateSource(CreateSourcePlan {
            name,
            source: _,
            if_not_exists: _,
            timeline: _,
            cluster_config,
        }) => {
            generate_required_source_privileges(catalog, name, cluster_config, depends_on, role_id)
        }
        Plan::CreateSources(plans) => plans
            .iter()
            .flat_map(
                |CreateSourcePlans {
                     source_id: _,
                     plan:
                         CreateSourcePlan {
                             name,
                             source: _,
                             if_not_exists: _,
                             timeline: _,
                             cluster_config,
                         },
                     depends_on,
                 }| {
                    // Sub-sources depend on not-yet created sources, so we need to filter those out.
                    let existing_depends_on = depends_on
                        .iter()
                        .filter(|id| catalog.try_get_item(id).is_some())
                        .cloned()
                        .collect();
                    generate_required_source_privileges(
                        catalog,
                        name,
                        cluster_config,
                        &existing_depends_on,
                        role_id,
                    )
                    .into_iter()
                },
            )
            .collect(),
        Plan::CreateSecret(CreateSecretPlan {
            name,
            secret: _,
            if_not_exists: _,
        }) => vec![(name.qualifiers.clone().into(), AclMode::CREATE, role_id)],
        Plan::CreateSink(CreateSinkPlan {
            name,
            sink,
            with_snapshot: _,
            if_not_exists: _,
            cluster_config,
        }) => {
            let mut privileges = vec![(name.qualifiers.clone().into(), AclMode::CREATE, role_id)];
            privileges.extend_from_slice(&generate_read_privileges(
                catalog,
                iter::once(sink.from),
                role_id,
            ));
            if let Some(id) = cluster_config.cluster_id() {
                privileges.push((id.into(), AclMode::CREATE, role_id));
            } else if let Ok(cluster) = catalog.resolve_cluster(None) {
                privileges.push((cluster.id().into(), AclMode::CREATE, role_id));
            }
            privileges
        }
        Plan::CreateTable(CreateTablePlan {
            name,
            table: _,
            if_not_exists: _,
        }) => {
            let mut privileges = vec![(name.qualifiers.clone().into(), AclMode::CREATE, role_id)];
            privileges.extend(generate_item_usage_privileges(catalog, depends_on, role_id));
            privileges
        }
        Plan::CreateView(CreateViewPlan {
            name,
            view: _,
            replace: _,
            drop_ids: _,
            if_not_exists: _,
            ambiguous_columns: _,
        }) => {
            let mut privileges = vec![(name.qualifiers.clone().into(), AclMode::CREATE, role_id)];
            privileges.extend(generate_item_usage_privileges(catalog, depends_on, role_id));
            privileges
        }
        Plan::CreateMaterializedView(CreateMaterializedViewPlan {
            name,
            materialized_view,
            replace: _,
            drop_ids: _,
            if_not_exists: _,
            ambiguous_columns: _,
        }) => {
            let mut privileges = vec![
                (name.qualifiers.clone().into(), AclMode::CREATE, role_id),
                (
                    materialized_view.cluster_id.into(),
                    AclMode::CREATE,
                    role_id,
                ),
            ];
            privileges.extend(generate_item_usage_privileges(catalog, depends_on, role_id));
            privileges
        }
        Plan::CreateIndex(CreateIndexPlan {
            name,
            index,
            options: _,
            if_not_exists: _,
        }) => {
            let mut privileges = vec![
                (name.qualifiers.clone().into(), AclMode::CREATE, role_id),
                (index.cluster_id.into(), AclMode::CREATE, role_id),
            ];
            privileges.extend(generate_item_usage_privileges(catalog, depends_on, role_id));
            privileges
        }
        Plan::CreateType(CreateTypePlan { name, typ: _ }) => {
            let mut privileges = vec![(name.qualifiers.clone().into(), AclMode::CREATE, role_id)];
            privileges.extend(generate_item_usage_privileges(catalog, depends_on, role_id));
            privileges
        }
        Plan::DropObjects(DropObjectsPlan {
            referenced_ids,
            drop_ids: _,
            object_type: _,
        }) => referenced_ids
            .iter()
            .filter_map(|id| match id {
                ObjectId::ClusterReplica((cluster_id, _)) => {
                    Some((cluster_id.into(), AclMode::USAGE, role_id))
                }
                ObjectId::Schema((database_spec, _)) => match database_spec {
                    ResolvedDatabaseSpecifier::Ambient => None,
                    ResolvedDatabaseSpecifier::Id(database_id) => {
                        Some((database_id.into(), AclMode::USAGE, role_id))
                    }
                },
                ObjectId::Item(item_id) => {
                    let item = catalog.get_item(item_id);
                    Some((
                        item.name().qualifiers.clone().into(),
                        AclMode::USAGE,
                        role_id,
                    ))
                }
                ObjectId::Cluster(_) | ObjectId::Database(_) | ObjectId::Role(_) => None,
            })
            .collect(),
        Plan::ShowCreate(ShowCreatePlan { id, row: _ }) => {
            let item = catalog.get_item(id);
            vec![(
                item.name().qualifiers.clone().into(),
                AclMode::USAGE,
                role_id,
            )]
        }

        Plan::Peek(PeekPlan {
            source,
            when: _,
            finishing: _,
            copy_to: _,
        }) => {
            let mut privileges =
                generate_read_privileges(catalog, depends_on.iter().cloned(), role_id);
            if let Some(privilege) =
                generate_cluster_usage_privileges(source, target_cluster_id, role_id)
            {
                privileges.push(privilege);
            }
            privileges
        }
        Plan::Subscribe(SubscribePlan {
            from: _,
            with_snapshot: _,
            when: _,
            up_to: _,
            copy_to: _,
            emit_progress: _,
            output: _,
        }) => {
            let mut privileges =
                generate_read_privileges(catalog, depends_on.iter().cloned(), role_id);
            if let Some(cluster_id) = target_cluster_id {
                privileges.push((cluster_id.into(), AclMode::USAGE, role_id));
            }
            privileges
        }
        Plan::Explain(ExplainPlan {
            raw_plan: _,
            row_set_finishing: _,
            stage: _,
            format: _,
            config: _,
            no_errors: _,
            explainee: _,
        }) => generate_read_privileges(catalog, depends_on.iter().cloned(), role_id),
        Plan::CopyFrom(CopyFromPlan {
            id,
            columns: _,
            params: _,
        }) => {
            let item = catalog.get_item(id);
            vec![
                (
                    item.name().qualifiers.clone().into(),
                    AclMode::USAGE,
                    role_id,
                ),
                (id.into(), AclMode::INSERT, role_id),
            ]
        }
        Plan::Insert(InsertPlan {
            id,
            values,
            returning,
        }) => {
            let schema_id: ObjectId = catalog.get_item(id).name().qualifiers.clone().into();
            let mut privileges = vec![
                (schema_id.clone(), AclMode::USAGE, role_id),
                (id.into(), AclMode::INSERT, role_id),
            ];
            let mut seen = BTreeSet::from([(schema_id, role_id)]);

            // We don't allow arbitrary sub-queries in `returning`. So either it
            // contains a column reference to the outer table or it's constant.
            if returning
                .iter()
                .any(|assignment| assignment.contains_column())
            {
                privileges.push((id.into(), AclMode::SELECT, role_id));
                seen.insert((id.into(), role_id));
            }

            privileges.extend_from_slice(&generate_read_privileges_inner(
                catalog,
                values.depends_on().into_iter(),
                role_id,
                &mut seen,
            ));

            // Collect additional USAGE privileges, like those in the returning clause.
            let seen = privileges
                .iter()
                .filter_map(|(id, _, _)| match id {
                    ObjectId::Item(id) => Some(*id),
                    _ => None,
                })
                .collect();
            privileges.extend(generate_item_usage_privileges_inner(
                catalog, depends_on, role_id, seen,
            ));

            if let Some(privilege) =
                generate_cluster_usage_privileges(values, target_cluster_id, role_id)
            {
                privileges.push(privilege);
            } else if !returning.is_empty() {
                // TODO(jkosh44) returning may be a constant, but for now we are overly protective
                //  and require cluster privileges for all returning.
                if let Some(cluster_id) = target_cluster_id {
                    privileges.push((cluster_id.into(), AclMode::USAGE, role_id));
                }
            }
            privileges
        }
        Plan::ReadThenWrite(ReadThenWritePlan {
            id,
            selection,
            finishing: _,
            assignments,
            kind,
            returning,
        }) => {
            let acl_mode = match kind {
                MutationKind::Insert => AclMode::INSERT,
                MutationKind::Update => AclMode::UPDATE,
                MutationKind::Delete => AclMode::DELETE,
            };
            let schema_id: ObjectId = catalog.get_item(id).name().qualifiers.clone().into();
            let mut privileges = vec![
                (schema_id.clone(), AclMode::USAGE, role_id),
                (id.into(), acl_mode, role_id),
            ];
            let mut seen = BTreeSet::from([(schema_id, role_id)]);

            // We don't allow arbitrary sub-queries in `assignments` or `returning`. So either they
            // contains a column reference to the outer table or it's constant.
            if assignments
                .values()
                .chain(returning.iter())
                .any(|assignment| assignment.contains_column())
            {
                privileges.push((id.into(), AclMode::SELECT, role_id));
                seen.insert((id.into(), role_id));
            }

            // TODO(jkosh44) It's fairly difficult to determine what part of `selection` is from a
            //  user specified read and what part is from the implementation of the read then write.
            //  instead we are overly protective and always require SELECT privileges even though
            //  PostgreSQL doesn't always do this.
            //  As a concrete example, we require SELECT and UPDATE privileges to execute
            //  `UPDATE t SET a = 42;`, while PostgreSQL only requires UPDATE privileges.
            privileges.extend_from_slice(&generate_read_privileges_inner(
                catalog,
                selection.depends_on().into_iter(),
                role_id,
                &mut seen,
            ));

            // Collect additional USAGE privileges, like those in the returning clause.
            let seen = privileges
                .iter()
                .filter_map(|(id, _, _)| match id {
                    ObjectId::Item(id) => Some(*id),
                    _ => None,
                })
                .collect();
            privileges.extend(generate_item_usage_privileges_inner(
                catalog, depends_on, role_id, seen,
            ));

            if let Some(privilege) =
                generate_cluster_usage_privileges(selection, target_cluster_id, role_id)
            {
                privileges.push(privilege);
            }
            privileges
        }
        Plan::AlterIndexSetOptions(AlterIndexSetOptionsPlan { id, options: _ })
        | Plan::AlterIndexResetOptions(AlterIndexResetOptionsPlan { id, options: _ })
        | Plan::AlterSink(AlterSinkPlan { id, size: _ })
        | Plan::AlterSource(AlterSourcePlan { id, size: _ })
        | Plan::AlterItemRename(AlterItemRenamePlan {
            id,
            current_full_name: _,
            to_name: _,
            object_type: _,
        })
        | Plan::AlterSecret(AlterSecretPlan { id, secret_as: _ })
        | Plan::RotateKeys(RotateKeysPlan { id }) => {
            let item = catalog.get_item(id);
            vec![(
                item.name().qualifiers.clone().into(),
                AclMode::CREATE,
                role_id,
            )]
        }
        Plan::AlterOwner(AlterOwnerPlan {
            id,
            object_type: _,
            new_owner: _,
        }) => match id {
            ObjectId::ClusterReplica((cluster_id, _)) => {
                vec![(cluster_id.into(), AclMode::CREATE, role_id)]
            }
            ObjectId::Schema((database_spec, _)) => match database_spec {
                ResolvedDatabaseSpecifier::Ambient => Vec::new(),
                ResolvedDatabaseSpecifier::Id(database_id) => {
                    vec![(database_id.into(), AclMode::CREATE, role_id)]
                }
            },
            ObjectId::Item(item_id) => {
                let item = catalog.get_item(item_id);
                vec![(
                    item.name().qualifiers.clone().into(),
                    AclMode::CREATE,
                    role_id,
                )]
            }
            ObjectId::Cluster(_) | ObjectId::Database(_) | ObjectId::Role(_) => Vec::new(),
        },
        Plan::GrantPrivilege(GrantPrivilegePlan {
            acl_mode: _,
            object_id,
            grantees: _,
            grantor: _,
        })
        | Plan::RevokePrivilege(RevokePrivilegePlan {
            acl_mode: _,
            object_id,
            revokees: _,
            grantor: _,
        }) => match object_id {
            ObjectId::ClusterReplica((cluster_id, _)) => {
                vec![(cluster_id.into(), AclMode::USAGE, role_id)]
            }
            ObjectId::Schema((database_spec, _)) => match database_spec {
                ResolvedDatabaseSpecifier::Ambient => Vec::new(),
                ResolvedDatabaseSpecifier::Id(database_id) => {
                    vec![(database_id.into(), AclMode::USAGE, role_id)]
                }
            },
            ObjectId::Item(item_id) => {
                let item = catalog.get_item(item_id);
                vec![(
                    item.name().qualifiers.clone().into(),
                    AclMode::USAGE,
                    role_id,
                )]
            }
            ObjectId::Cluster(_) | ObjectId::Database(_) | ObjectId::Role(_) => Vec::new(),
        },
        Plan::CreateDatabase(CreateDatabasePlan {
            name: _,
            if_not_exists: _,
        })
        | Plan::CreateRole(CreateRolePlan {
            name: _,
            attributes: _,
        })
        | Plan::CreateCluster(CreateClusterPlan {
            name: _,
            replicas: _,
        })
        | Plan::DiscardTemp
        | Plan::DiscardAll
        | Plan::EmptyQuery
        | Plan::ShowAllVariables
        | Plan::ShowVariable(ShowVariablePlan { name: _ })
        | Plan::SetVariable(SetVariablePlan {
            name: _,
            value: _,
            local: _,
        })
        | Plan::ResetVariable(ResetVariablePlan { name: _ })
        | Plan::StartTransaction(StartTransactionPlan {
            access: _,
            isolation_level: _,
        })
        | Plan::CommitTransaction(CommitTransactionPlan {
            transaction_type: _,
        })
        | Plan::AbortTransaction(AbortTransactionPlan {
            transaction_type: _,
        })
        | Plan::CopyRows(CopyRowsPlan {
            id: _,
            columns: _,
            rows: _,
        })
        | Plan::AlterNoop(AlterNoopPlan { object_type: _ })
        | Plan::AlterSystemSet(AlterSystemSetPlan { name: _, value: _ })
        | Plan::AlterSystemReset(AlterSystemResetPlan { name: _ })
        | Plan::AlterSystemResetAll(AlterSystemResetAllPlan {})
        | Plan::AlterRole(AlterRolePlan {
            id: _,
            name: _,
            attributes: _,
        })
        | Plan::Declare(DeclarePlan { name: _, stmt: _ })
        | Plan::Fetch(FetchPlan {
            name: _,
            count: _,
            timeout: _,
        })
        | Plan::Close(ClosePlan { name: _ })
        | Plan::Prepare(PreparePlan {
            name: _,
            stmt: _,
            desc: _,
        })
        | Plan::Execute(ExecutePlan { name: _, params: _ })
        | Plan::Deallocate(DeallocatePlan { name: _ })
        | Plan::Raise(RaisePlan { severity: _ })
        | Plan::GrantRole(GrantRolePlan {
            role_id: _,
            member_ids: _,
            grantor_id: _,
        })
        | Plan::RevokeRole(RevokeRolePlan {
            role_id: _,
            member_ids: _,
            grantor_id: _,
        })
        | Plan::DropOwned(DropOwnedPlan {
            role_ids: _,
            drop_ids: _,
            revokes: _,
        })
        | Plan::ReassignOwned(ReassignOwnedPlan {
            old_roles: _,
            new_role: _,
            reassign_ids: _,
        }) => Vec::new(),
    }
}

fn generate_required_source_privileges(
    catalog: &impl SessionCatalog,
    name: &QualifiedItemName,
    cluster_config: &SourceSinkClusterConfig,
    depends_on: &Vec<GlobalId>,
    role_id: RoleId,
) -> Vec<(ObjectId, AclMode, RoleId)> {
    let mut privileges = vec![(name.qualifiers.clone().into(), AclMode::CREATE, role_id)];
    if let Some(id) = cluster_config.cluster_id() {
        privileges.push((id.into(), AclMode::CREATE, role_id));
    }
    privileges.extend(generate_item_usage_privileges(catalog, depends_on, role_id));
    privileges
}

/// Generates all the privileges required to execute a read that includes the objects in `ids`.
///
/// Not only do we need to validate that `role_id` has read privileges on all relations in `ids`,
/// but if any object is a view or materialized view then we need to validate that the owner of
/// that view has all of the privileges required to execute the query within the view.
///
/// For more details see: <https://www.postgresql.org/docs/15/rules-privileges.html>
fn generate_read_privileges(
    catalog: &impl SessionCatalog,
    ids: impl Iterator<Item = GlobalId>,
    role_id: RoleId,
) -> Vec<(ObjectId, AclMode, RoleId)> {
    generate_read_privileges_inner(catalog, ids, role_id, &mut BTreeSet::new())
}

fn generate_read_privileges_inner(
    catalog: &impl SessionCatalog,
    ids: impl Iterator<Item = GlobalId>,
    role_id: RoleId,
    seen: &mut BTreeSet<(ObjectId, RoleId)>,
) -> Vec<(ObjectId, AclMode, RoleId)> {
    let mut privileges = Vec::new();
    let mut views = Vec::new();

    for id in ids {
        if seen.insert((id.into(), role_id)) {
            let item = catalog.get_item(&id);
            let schema_id: ObjectId = item.name().qualifiers.clone().into();
            if seen.insert((schema_id.clone(), role_id)) {
                privileges.push((schema_id, AclMode::USAGE, role_id))
            }
            match item.item_type() {
                CatalogItemType::View | CatalogItemType::MaterializedView => {
                    privileges.push((id.into(), AclMode::SELECT, role_id));
                    views.push((item.uses().iter().cloned(), item.owner_id()));
                }
                CatalogItemType::Table | CatalogItemType::Source => {
                    privileges.push((id.into(), AclMode::SELECT, role_id));
                }
                CatalogItemType::Type | CatalogItemType::Secret | CatalogItemType::Connection => {
                    privileges.push((id.into(), AclMode::USAGE, role_id));
                }
                CatalogItemType::Sink | CatalogItemType::Index | CatalogItemType::Func => {}
            }
        }
    }

    for (view_ids, view_owner) in views {
        privileges.extend_from_slice(&generate_read_privileges_inner(
            catalog, view_ids, view_owner, seen,
        ));
    }

    privileges
}

fn generate_item_usage_privileges<'a>(
    catalog: &'a impl SessionCatalog,
    ids: &'a Vec<GlobalId>,
    role_id: RoleId,
) -> impl Iterator<Item = (ObjectId, AclMode, RoleId)> + 'a {
    generate_item_usage_privileges_inner(catalog, ids, role_id, BTreeSet::new())
}

fn generate_item_usage_privileges_inner<'a>(
    catalog: &'a impl SessionCatalog,
    ids: &'a Vec<GlobalId>,
    role_id: RoleId,
    seen: BTreeSet<GlobalId>,
) -> impl Iterator<Item = (ObjectId, AclMode, RoleId)> + 'a {
    // Use a `BTreeSet` to remove duplicate IDs.
    BTreeSet::from_iter(ids.iter())
        .into_iter()
        .filter(move |id| !seen.contains(id))
        .filter_map(move |id| {
            let item = catalog.get_item(id);
            match item.item_type() {
                CatalogItemType::Type | CatalogItemType::Secret | CatalogItemType::Connection => {
                    Some((id.into(), AclMode::USAGE, role_id))
                }
                CatalogItemType::Table
                | CatalogItemType::Source
                | CatalogItemType::Sink
                | CatalogItemType::View
                | CatalogItemType::MaterializedView
                | CatalogItemType::Index
                | CatalogItemType::Func => None,
            }
        })
}

fn generate_cluster_usage_privileges(
    expr: &MirRelationExpr,
    target_cluster_id: Option<ClusterId>,
    role_id: RoleId,
) -> Option<(ObjectId, AclMode, RoleId)> {
    // TODO(jkosh44) expr hasn't been fully optimized yet, so it might actually be a constant,
    //  but we mistakenly think that it's not. For now it's ok to be overly protective.
    if expr.as_const().is_none() {
        if let Some(cluster_id) = target_cluster_id {
            return Some((cluster_id.into(), AclMode::USAGE, role_id));
        }
    }

    None
}

fn check_object_privileges(
    catalog: &impl SessionCatalog,
    privileges: Vec<(ObjectId, AclMode, RoleId)>,
    mut role_memberships: BTreeMap<RoleId, BTreeSet<RoleId>>,
) -> Result<(), UnauthorizedError> {
    for (object_id, acl_mode, role_id) in privileges {
        let role_membership = role_memberships
            .entry(role_id)
            .or_insert_with_key(|role_id| catalog.collect_role_membership(role_id));
        let object_privileges = catalog
            .get_privileges(&object_id)
            .expect("only object types with privileges will generate required privileges");
        let role_privileges = role_membership
            .iter()
            .filter_map(|role_id| object_privileges.0.get(role_id))
            .flat_map(|mz_acl_items| mz_acl_items.iter())
            .map(|mz_acl_item| mz_acl_item.acl_mode)
            .fold(AclMode::empty(), |accum, acl_mode| accum.union(acl_mode));
        if !role_privileges.contains(acl_mode) {
            return Err(UnauthorizedError::Privilege {
                object_type: catalog.get_object_type(&object_id),
                object_name: catalog.get_object_name(&object_id),
            });
        }
    }

    Ok(())
}

pub(crate) const fn all_object_privileges(object_type: ObjectType) -> AclMode {
    const TABLE_ACL_MODE: AclMode = AclMode::INSERT
        .union(AclMode::SELECT)
        .union(AclMode::UPDATE)
        .union(AclMode::DELETE);
    const USAGE_CREATE_ACL_MODE: AclMode = AclMode::USAGE.union(AclMode::CREATE);
    const EMPTY_ACL_MODE: AclMode = AclMode::empty();
    match object_type {
        ObjectType::Table => TABLE_ACL_MODE,
        ObjectType::View => AclMode::SELECT,
        ObjectType::MaterializedView => AclMode::SELECT,
        ObjectType::Source => AclMode::SELECT,
        ObjectType::Sink => EMPTY_ACL_MODE,
        ObjectType::Index => EMPTY_ACL_MODE,
        ObjectType::Type => AclMode::USAGE,
        ObjectType::Role => EMPTY_ACL_MODE,
        ObjectType::Cluster => USAGE_CREATE_ACL_MODE,
        ObjectType::ClusterReplica => EMPTY_ACL_MODE,
        ObjectType::Secret => AclMode::USAGE,
        ObjectType::Connection => AclMode::USAGE,
        ObjectType::Database => USAGE_CREATE_ACL_MODE,
        ObjectType::Schema => USAGE_CREATE_ACL_MODE,
        ObjectType::Func => EMPTY_ACL_MODE,
    }
}

pub(crate) const fn owner_privilege(object_type: ObjectType, owner_id: RoleId) -> MzAclItem {
    MzAclItem {
        grantee: owner_id,
        grantor: owner_id,
        acl_mode: all_object_privileges(object_type),
    }
}

pub(crate) const fn default_catalog_privilege(object_type: ObjectType) -> MzAclItem {
    let acl_mode = match object_type {
        ObjectType::Table
        | ObjectType::View
        | ObjectType::MaterializedView
        | ObjectType::Source => AclMode::SELECT,
        ObjectType::Type | ObjectType::Schema => AclMode::USAGE,
        ObjectType::Sink
        | ObjectType::Index
        | ObjectType::Role
        | ObjectType::Cluster
        | ObjectType::ClusterReplica
        | ObjectType::Secret
        | ObjectType::Connection
        | ObjectType::Database
        | ObjectType::Func => AclMode::empty(),
    };
    MzAclItem {
        grantee: RoleId::Public,
        grantor: MZ_SYSTEM_ROLE_ID,
        acl_mode,
    }
}
