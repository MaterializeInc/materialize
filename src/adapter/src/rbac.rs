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
use mz_expr::{CollectionPlan, MirRelationExpr};

use mz_ore::str::StrExt;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::catalog::{CatalogItemType, SessionCatalog};
use mz_sql::names::{ObjectId, ResolvedDatabaseSpecifier};
use mz_sql::plan::{
    AlterIndexResetOptionsPlan, AlterIndexSetOptionsPlan, AlterItemRenamePlan, AlterOwnerPlan,
    AlterSecretPlan, AlterSinkPlan, AlterSourcePlan, CreateMaterializedViewPlan, CreateSinkPlan,
    CreateSourcePlan, CreateViewPlan, GrantPrivilegePlan, MutationKind, Plan, RevokePrivilegePlan,
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
        attribute: Attribute,
    },
    /// The action requires ownership of an object.
    #[error("must be owner of {}", objects.iter().map(|(object_type, object_name)| format!("{object_type} {object_name}")).join(", "))]
    Ownership { objects: Vec<(ObjectType, String)> },
    /// Altering an owner requires membership of the new owner role.
    #[error("must be a member of {}", role_name.to_string().quoted())]
    AlterOwnerMembership { role_name: String },
    /// The action requires one or more privileges.
    #[error("permission denied for {object_type} {object_name}")]
    Privilege {
        object_type: ObjectType,
        object_name: String,
    },
    // TODO(jkosh44) When we implement parameter privileges, this can be replaced with a regular
    // privilege error.
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
            UnauthorizedError::Attribute { action, attribute } => Some(format!(
                "You must have the {} attribute to {}",
                attribute, action
            )),
            UnauthorizedError::MzSystem { .. } => {
                Some(format!("You must be the '{}' role", SYSTEM_USER.name))
            }
            UnauthorizedError::MzIntrospection { .. } => Some(format!(
                "The '{}' role has very limited privileges",
                INTROSPECTION_USER.name
            )),
            UnauthorizedError::Ownership { .. }
            | UnauthorizedError::AlterOwnerMembership { .. }
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

    let required_privileges = generate_required_privileges(catalog, plan, depends_on, *role_id);
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
        Plan::CreateSource(CreateSourcePlan { cluster_config, .. })
        | Plan::CreateSink(CreateSinkPlan { cluster_config, .. }) => {
            if cluster_config.cluster_id().is_none() {
                Some(Attribute::CreateCluster)
            } else {
                None
            }
        }
        Plan::CreateSources(plans) => {
            if plans
                .iter()
                .any(|plan| plan.plan.cluster_config.cluster_id().is_none())
            {
                Some(Attribute::CreateCluster)
            } else {
                None
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
        | Plan::RevokePrivilege(_) => None,
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
        | Plan::RevokeRole(_) => Vec::new(),
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
    depends_on: &Vec<GlobalId>,
    role_id: RoleId,
) -> Vec<(ObjectId, AclMode, RoleId)> {
    match plan {
        Plan::CreateConnection(plan) => {
            let mut privileges = vec![(
                plan.name.qualifiers.clone().into(),
                AclMode::CREATE,
                role_id,
            )];
            privileges.extend(generate_item_usage_privileges(catalog, depends_on, role_id));
            privileges
        }
        Plan::CreateSchema(plan) => match plan.database_spec {
            ResolvedDatabaseSpecifier::Ambient => Vec::new(),
            ResolvedDatabaseSpecifier::Id(database_id) => {
                vec![(database_id.into(), AclMode::CREATE, role_id)]
            }
        },
        Plan::CreateClusterReplica(plan) => {
            vec![(plan.cluster_id.into(), AclMode::CREATE, role_id)]
        }
        Plan::CreateSource(plan) => {
            generate_required_source_privileges(catalog, plan, depends_on, role_id)
        }
        Plan::CreateSources(plans) => plans
            .iter()
            .flat_map(|plan| {
                // Sub-sources depend on not-yet created sources, so we need to filter those out.
                let existing_depends_on = plan
                    .depends_on
                    .iter()
                    .filter(|id| catalog.try_get_item(id).is_some())
                    .cloned()
                    .collect();
                generate_required_source_privileges(
                    catalog,
                    &plan.plan,
                    &existing_depends_on,
                    role_id,
                )
                .into_iter()
            })
            .collect(),
        Plan::CreateSecret(plan) => vec![(
            plan.name.qualifiers.clone().into(),
            AclMode::CREATE,
            role_id,
        )],
        Plan::CreateSink(plan) => {
            let mut privileges = vec![(
                plan.name.qualifiers.clone().into(),
                AclMode::CREATE,
                role_id,
            )];
            privileges.extend_from_slice(&generate_read_privileges(
                catalog,
                iter::once(plan.sink.from),
                role_id,
            ));
            if let Some(id) = plan.cluster_config.cluster_id() {
                privileges.push((id.into(), AclMode::CREATE, role_id));
            } else if let Ok(cluster) = catalog.resolve_cluster(None) {
                privileges.push((cluster.id().into(), AclMode::CREATE, role_id));
            }
            privileges
        }
        Plan::CreateTable(plan) => {
            let mut privileges = vec![(
                plan.name.qualifiers.clone().into(),
                AclMode::CREATE,
                role_id,
            )];
            privileges.extend(generate_item_usage_privileges(catalog, depends_on, role_id));
            privileges
        }
        Plan::CreateView(plan) => {
            let mut privileges = vec![(
                plan.name.qualifiers.clone().into(),
                AclMode::CREATE,
                role_id,
            )];
            privileges.extend(generate_item_usage_privileges(catalog, depends_on, role_id));
            privileges
        }
        Plan::CreateMaterializedView(plan) => {
            let mut privileges = vec![
                (
                    plan.name.qualifiers.clone().into(),
                    AclMode::CREATE,
                    role_id,
                ),
                (
                    plan.materialized_view.cluster_id.into(),
                    AclMode::CREATE,
                    role_id,
                ),
            ];
            privileges.extend(generate_item_usage_privileges(catalog, depends_on, role_id));
            privileges
        }
        Plan::CreateIndex(plan) => {
            let mut privileges = vec![
                (
                    plan.name.qualifiers.clone().into(),
                    AclMode::CREATE,
                    role_id,
                ),
                (plan.index.cluster_id.into(), AclMode::CREATE, role_id),
            ];
            privileges.extend(generate_item_usage_privileges(catalog, depends_on, role_id));
            privileges
        }
        Plan::CreateType(plan) => {
            let mut privileges = vec![(
                plan.name.qualifiers.clone().into(),
                AclMode::CREATE,
                role_id,
            )];
            privileges.extend(generate_item_usage_privileges(catalog, depends_on, role_id));
            privileges
        }
        Plan::DropObjects(plan) => plan
            .referenced_ids
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
        Plan::ShowCreate(plan) => {
            let item = catalog.get_item(&plan.id);
            vec![(
                item.name().qualifiers.clone().into(),
                AclMode::USAGE,
                role_id,
            )]
        }

        Plan::Peek(plan) => {
            let mut privileges =
                generate_read_privileges(catalog, depends_on.iter().cloned(), role_id);
            if let Some(privilege) =
                generate_cluster_usage_privileges(catalog, &plan.source, role_id)
            {
                privileges.push(privilege);
            }
            privileges
        }
        Plan::Subscribe(_) => {
            let mut privileges =
                generate_read_privileges(catalog, depends_on.iter().cloned(), role_id);
            if let Ok(cluster) = catalog.resolve_cluster(None) {
                privileges.push((cluster.id().into(), AclMode::USAGE, role_id));
            }
            privileges
        }
        Plan::Explain(_) => generate_read_privileges(catalog, depends_on.iter().cloned(), role_id),
        Plan::CopyFrom(plan) => {
            let item = catalog.get_item(&plan.id);
            vec![
                (
                    item.name().qualifiers.clone().into(),
                    AclMode::USAGE,
                    role_id,
                ),
                (plan.id.into(), AclMode::INSERT, role_id),
            ]
        }
        Plan::Insert(plan) => {
            let update_schema_id: ObjectId =
                catalog.get_item(&plan.id).name().qualifiers.clone().into();
            let mut privileges = vec![
                (update_schema_id.clone(), AclMode::USAGE, role_id),
                (plan.id.into(), AclMode::INSERT, role_id),
            ];
            privileges.extend_from_slice(&generate_read_privileges_inner(
                catalog,
                plan.values.depends_on().into_iter(),
                role_id,
                &mut BTreeSet::from([(update_schema_id, role_id)]),
            ));
            // Collect any USAGE privileges from the returning clause.
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
                generate_cluster_usage_privileges(catalog, &plan.values, role_id)
            {
                privileges.push(privilege);
            }
            privileges
        }
        Plan::ReadThenWrite(plan) => {
            let acl_mode = match plan.kind {
                MutationKind::Insert => AclMode::INSERT,
                MutationKind::Update => AclMode::UPDATE,
                MutationKind::Delete => AclMode::DELETE,
            };
            let update_schema_id: ObjectId =
                catalog.get_item(&plan.id).name().qualifiers.clone().into();
            let mut privileges = vec![
                (update_schema_id.clone(), AclMode::USAGE, role_id),
                (plan.id.into(), acl_mode, role_id),
            ];
            if plan.contains_user_specified_read {
                privileges.extend_from_slice(&generate_read_privileges_inner(
                    catalog,
                    plan.selection.depends_on().into_iter(),
                    role_id,
                    &mut BTreeSet::from([(update_schema_id, role_id)]),
                ));
            }
            // Collect any USAGE privileges from the returning clause.
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
                generate_cluster_usage_privileges(catalog, &plan.selection, role_id)
            {
                privileges.push(privilege);
            }
            privileges
        }
        Plan::AlterIndexSetOptions(AlterIndexSetOptionsPlan { id, .. })
        | Plan::AlterIndexResetOptions(AlterIndexResetOptionsPlan { id, .. })
        | Plan::AlterSink(AlterSinkPlan { id, .. })
        | Plan::AlterSource(AlterSourcePlan { id, .. })
        | Plan::AlterItemRename(AlterItemRenamePlan { id, .. })
        | Plan::AlterSecret(AlterSecretPlan { id, .. }) => {
            let item = catalog.get_item(id);
            vec![(
                item.name().qualifiers.clone().into(),
                AclMode::CREATE,
                role_id,
            )]
        }
        Plan::AlterOwner(AlterOwnerPlan { id, .. }) => match id {
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
        Plan::GrantPrivilege(GrantPrivilegePlan { object_id, .. })
        | Plan::RevokePrivilege(RevokePrivilegePlan { object_id, .. }) => match object_id {
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
        Plan::CreateDatabase(_)
        | Plan::CreateRole(_)
        | Plan::CreateCluster(_)
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
        | Plan::CopyRows(_)
        | Plan::AlterNoop(_)
        | Plan::AlterSystemSet(_)
        | Plan::AlterSystemReset(_)
        | Plan::AlterSystemResetAll(_)
        | Plan::AlterRole(_)
        | Plan::Declare(_)
        | Plan::Fetch(_)
        | Plan::Close(_)
        | Plan::Prepare(_)
        | Plan::Execute(_)
        | Plan::Deallocate(_)
        | Plan::Raise(_)
        | Plan::RotateKeys(_)
        | Plan::GrantRole(_)
        | Plan::RevokeRole(_) => Vec::new(),
    }
}

fn generate_required_source_privileges(
    catalog: &impl SessionCatalog,
    plan: &CreateSourcePlan,
    depends_on: &Vec<GlobalId>,
    role_id: RoleId,
) -> Vec<(ObjectId, AclMode, RoleId)> {
    let mut privileges = vec![(
        plan.name.qualifiers.clone().into(),
        AclMode::CREATE,
        role_id,
    )];
    if let Some(id) = plan.cluster_config.cluster_id() {
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

fn generate_cluster_usage_privileges<'a>(
    catalog: &'a impl SessionCatalog,
    expr: &MirRelationExpr,
    role_id: RoleId,
) -> Option<(ObjectId, AclMode, RoleId)> {
    // expr hasn't been fully optimized yet, so it might actually be a constant,
    // but we mistakenly think that it's not.
    if expr.as_const().is_none() {
        if let Ok(cluster) = catalog.resolve_cluster(None) {
            return Some((cluster.id().into(), AclMode::USAGE, role_id));
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
            .filter_map(|role_id| object_privileges.get(role_id))
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
