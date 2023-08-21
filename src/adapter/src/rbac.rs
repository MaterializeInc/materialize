// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::iter;

use itertools::Itertools;
use mz_controller::clusters::ClusterId;
use mz_expr::{CollectionPlan, MirRelationExpr};
use mz_ore::str::StrExt;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::catalog::{
    CatalogItemType, ErrorMessageObjectDescription, ObjectType, SessionCatalog, SystemObjectType,
};
use mz_sql::names::{
    ObjectId, QualifiedItemName, ResolvedDatabaseSpecifier, ResolvedIds, SystemObjectId,
};
use mz_sql::plan;
use mz_sql::plan::{MutationKind, Plan, SourceSinkClusterConfig, UpdatePrivilege};
use mz_sql::session::user::{SUPPORT_USER, SYSTEM_USER};
use mz_sql::session::vars::SystemVars;
use mz_sql_parser::ast::QualifiedReplica;

use crate::catalog::storage::MZ_SYSTEM_ROLE_ID;
use crate::catalog::Catalog;
use crate::client::ConnectionId;
use crate::command::Command;
use crate::coord::{ConnMeta, Coordinator};
use crate::session::Session;
use crate::AdapterError;

/// Common checks that need to be performed before we can start checking a role's privileges.
macro_rules! rbac_preamble {
    ($catalog:expr, $session:expr) => {
        // PostgreSQL allows users that have their role dropped to perform some actions,
        // such as `SET ROLE` and certain `SELECT` queries. We haven't implemented
        // `SET ROLE` and feel it's safer to force to user to re-authenticate if their
        // role is dropped.
        let current_role_id = $session.current_role_id();
        if $catalog.try_get_role(current_role_id).is_none() {
            return Err(AdapterError::ConcurrentRoleDrop(current_role_id.clone()));
        };
        let session_role_id = $session.session_role_id();
        if $catalog.try_get_role(session_role_id).is_none() {
            return Err(AdapterError::ConcurrentRoleDrop(session_role_id.clone()));
        };

        // Skip RBAC checks if RBAC is disabled.
        if !is_rbac_enabled_for_session($catalog.system_vars(), $session) {
            return Ok(());
        }

        // Skip RBAC checks if the session is a superuser.
        if $session.is_superuser() {
            return Ok(());
        }
    };
}

/// Errors that can occur due to an unauthorized action.
#[derive(Debug, thiserror::Error)]
pub enum UnauthorizedError {
    /// The action can only be performed by a superuser.
    #[error("permission denied to {action}")]
    Superuser { action: String },
    /// The action requires ownership of an object.
    #[error("must be owner of {}", objects.iter().map(|(object_type, object_name)| format!("{object_type} {object_name}")).join(", "))]
    Ownership { objects: Vec<(ObjectType, String)> },
    /// Altering an owner requires membership of the new owner role.
    #[error("must be a member of {}", role_names.iter().map(|role| role.quoted()).join(", "))]
    RoleMembership { role_names: Vec<String> },
    /// The action requires one or more privileges.
    #[error("permission denied for {object_description}")]
    Privilege {
        object_description: ErrorMessageObjectDescription,
    },
    // TODO(jkosh44) When we implement parameter privileges, this can be replaced with a regular
    //  privilege error.
    /// The action can only be performed by the mz_system role.
    #[error("permission denied to {action}")]
    MzSystem { action: String },
    /// The action cannot be performed by the mz_introspection role.
    #[error("permission denied to {action}")]
    MzSupport { action: String },
}

impl UnauthorizedError {
    pub fn detail(&self) -> Option<String> {
        match &self {
            UnauthorizedError::Superuser { action } => {
                Some(format!("You must be a superuser to {}", action))
            }
            UnauthorizedError::MzSystem { .. } => {
                Some(format!("You must be the '{}' role", SYSTEM_USER.name))
            }
            UnauthorizedError::MzSupport { .. } => Some(format!(
                "The '{}' role has very limited privileges",
                SUPPORT_USER.name
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
        | Command::Prepare { .. }
        | Command::VerifyPreparedStatement { .. }
        | Command::Execute { .. }
        | Command::Commit { .. }
        | Command::CancelRequest { .. }
        | Command::PrivilegedCancelRequest { .. }
        | Command::CopyRows { .. }
        | Command::GetSystemVars { .. }
        | Command::SetSystemVars { .. }
        | Command::AppendWebhook { .. }
        | Command::Terminate { .. }
        | Command::RetireExecute { .. } => Ok(()),
    }
}

/// Checks if a `session` is authorized to use `resolved_ids`. If not, an error is returned.
pub fn check_item_usage(
    catalog: &impl SessionCatalog,
    session: &Session,
    resolved_ids: &ResolvedIds,
) -> Result<(), AdapterError> {
    rbac_preamble!(catalog, session);

    // Obtain all roles that the current session is a member of.
    let current_role_id = session.current_role_id();
    let role_membership = catalog.collect_role_membership(current_role_id);

    // Certain statements depend on objects that haven't been created yet, like sub-sources, so we
    // need to filter those out.
    let existing_resolved_ids = resolved_ids
        .0
        .iter()
        .filter(|id| catalog.try_get_item(id).is_some())
        .cloned()
        .collect();
    let existing_resolved_ids = ResolvedIds(existing_resolved_ids);
    let required_privileges =
        generate_item_usage_privileges(catalog, &existing_resolved_ids, *current_role_id)
            .into_iter()
            .collect();
    check_object_privileges(
        catalog,
        required_privileges,
        role_membership,
        *current_role_id,
    )?;

    Ok(())
}

/// Checks if a session is authorized to execute a plan. If not, an error is returned.
pub fn check_plan(
    coord: &Coordinator,
    catalog: &impl SessionCatalog,
    session: &Session,
    plan: &Plan,
    target_cluster_id: Option<ClusterId>,
    resolved_ids: &ResolvedIds,
) -> Result<(), AdapterError> {
    rbac_preamble!(catalog, session);

    check_item_usage(catalog, session, resolved_ids)?;

    // Obtain all roles that the current session is a member of.
    let current_role_id = session.current_role_id();
    let role_membership = catalog.collect_role_membership(current_role_id);

    // Validate that the current session has the required role membership to execute the provided
    // plan.
    let required_membership: BTreeSet<_> =
        generate_required_role_membership(plan, coord.active_conns())
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

    // Validate that the current session has the required object ownership to execute the provided
    // plan.
    let required_ownership = generate_required_ownership(plan);
    let unheld_ownership = required_ownership
        .into_iter()
        .filter(|ownership| !check_owner_roles(ownership, &role_membership, catalog))
        .collect();
    ownership_err(unheld_ownership, catalog)?;

    let required_privileges = generate_required_privileges(
        catalog,
        plan,
        target_cluster_id,
        resolved_ids,
        *current_role_id,
    );
    check_object_privileges(
        catalog,
        required_privileges,
        role_membership,
        *current_role_id,
    )?;

    check_superuser_required(plan)?;

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
pub fn generate_required_role_membership(
    plan: &Plan,
    active_conns: &BTreeMap<ConnectionId, ConnMeta>,
) -> Vec<RoleId> {
    match plan {
        Plan::AlterOwner(plan::AlterOwnerPlan { new_owner, .. }) => vec![*new_owner],
        Plan::DropOwned(plan::DropOwnedPlan { role_ids, .. }) => role_ids.clone(),
        Plan::ReassignOwned(plan::ReassignOwnedPlan {
            old_roles,
            new_role,
            ..
        }) => {
            let mut roles = old_roles.clone();
            roles.push(*new_role);
            roles
        }
        Plan::AlterDefaultPrivileges(plan::AlterDefaultPrivilegesPlan {
            privilege_objects,
            ..
        }) => privilege_objects
            .iter()
            .map(|privilege_object| privilege_object.role_id)
            .collect(),
        Plan::SideEffectingFunc(plan::SideEffectingFunc::PgCancelBackend { connection_id }) => {
            let mut roles = Vec::new();
            if let Some(conn) = active_conns.get(connection_id) {
                roles.push(conn.authenticated_role);
            }
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
        | Plan::ShowColumns(_)
        | Plan::ShowVariable(_)
        | Plan::InspectShard(_)
        | Plan::SetVariable(_)
        | Plan::ResetVariable(_)
        | Plan::SetTransaction(_)
        | Plan::StartTransaction(_)
        | Plan::CommitTransaction(_)
        | Plan::AbortTransaction(_)
        | Plan::Select(_)
        | Plan::Subscribe(_)
        | Plan::CopyFrom(_)
        | Plan::CopyRows(_)
        | Plan::Explain(_)
        | Plan::Insert(_)
        | Plan::AlterClusterRename(_)
        | Plan::AlterClusterReplicaRename(_)
        | Plan::AlterCluster(_)
        | Plan::AlterNoop(_)
        | Plan::AlterSetCluster(_)
        | Plan::AlterIndexSetOptions(_)
        | Plan::AlterIndexResetOptions(_)
        | Plan::AlterSink(_)
        | Plan::AlterSource(_)
        | Plan::PurifiedAlterSource { .. }
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
        | Plan::GrantPrivileges(_)
        | Plan::RevokePrivileges(_)
        | Plan::ValidateConnection(_) => Vec::new(),
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
        | Plan::InspectShard(_)
        | Plan::ResetVariable(_)
        | Plan::SetTransaction(_)
        | Plan::StartTransaction(_)
        | Plan::CommitTransaction(_)
        | Plan::AbortTransaction(_)
        | Plan::Select(_)
        | Plan::Subscribe(_)
        | Plan::ShowCreate(_)
        | Plan::ShowColumns(_)
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
        | Plan::ReassignOwned(_)
        | Plan::AlterDefaultPrivileges(_)
        | Plan::ValidateConnection(_)
        | Plan::SideEffectingFunc(_) => Vec::new(),
        Plan::CreateClusterReplica(plan) => vec![ObjectId::Cluster(plan.cluster_id)],
        Plan::CreateIndex(plan) => vec![ObjectId::Item(plan.index.on)],
        Plan::CreateView(plan::CreateViewPlan { replace, .. })
        | Plan::CreateMaterializedView(plan::CreateMaterializedViewPlan { replace, .. }) => replace
            .map(|id| vec![ObjectId::Item(id)])
            .unwrap_or_default(),
        // Do not need ownership of descendant objects.
        Plan::DropObjects(plan) => plan.referenced_ids.clone(),
        Plan::AlterClusterRename(plan::AlterClusterRenamePlan { id, .. })
        | Plan::AlterCluster(plan::AlterClusterPlan { id, .. }) => {
            vec![ObjectId::Cluster(*id)]
        }
        Plan::AlterClusterReplicaRename(plan) => {
            vec![ObjectId::ClusterReplica((plan.cluster_id, plan.replica_id))]
        }
        Plan::AlterIndexSetOptions(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterIndexResetOptions(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterSink(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterSource(alter_source) | Plan::PurifiedAlterSource { alter_source, .. } => {
            vec![ObjectId::Item(alter_source.id)]
        }
        Plan::AlterSetCluster(plan) => {
            vec![ObjectId::Item(plan.id)]
        }
        Plan::AlterItemRename(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterSecret(plan) => vec![ObjectId::Item(plan.id)],
        Plan::RotateKeys(plan) => vec![ObjectId::Item(plan.id)],
        Plan::AlterOwner(plan) => vec![plan.id.clone()],
        Plan::GrantPrivileges(plan) => plan
            .update_privileges
            .iter()
            .filter_map(|update_privilege| update_privilege.target_id.object_id())
            .cloned()
            .collect(),
        Plan::RevokePrivileges(plan) => plan
            .update_privileges
            .iter()
            .filter_map(|update_privilege| update_privilege.target_id.object_id())
            .cloned()
            .collect(),
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
    resolved_ids: &ResolvedIds,
    role_id: RoleId,
) -> Vec<(SystemObjectId, AclMode, RoleId)> {
    // When adding a new plan, make sure that the plan struct is fully expanded. This helps ensure
    // that when someone adds a field to a plan they get a compiler error and must consider any
    // required changes to privileges.
    match plan {
        Plan::CreateConnection(plan::CreateConnectionPlan {
            name,
            if_not_exists: _,
            connection: _,
            validate: _,
        }) => {
            vec![(
                SystemObjectId::Object(name.qualifiers.clone().into()),
                AclMode::CREATE,
                role_id,
            )]
        }
        Plan::CreateDatabase(plan::CreateDatabasePlan {
            name: _,
            if_not_exists: _,
        }) => vec![(SystemObjectId::System, AclMode::CREATE_DB, role_id)],
        Plan::CreateCluster(plan::CreateClusterPlan {
            name: _,
            variant: _,
        }) => vec![(SystemObjectId::System, AclMode::CREATE_CLUSTER, role_id)],
        Plan::ValidateConnection(plan::ValidateConnectionPlan { id, connection: _ }) => {
            let schema_id: ObjectId = catalog.get_item(id).name().qualifiers.clone().into();
            vec![
                (SystemObjectId::Object(schema_id), AclMode::USAGE, role_id),
                (SystemObjectId::Object(id.into()), AclMode::USAGE, role_id),
            ]
        }
        Plan::CreateSchema(plan::CreateSchemaPlan {
            database_spec,
            schema_name: _,
            if_not_exists: _,
        }) => match database_spec {
            ResolvedDatabaseSpecifier::Ambient => Vec::new(),
            ResolvedDatabaseSpecifier::Id(database_id) => {
                vec![(
                    SystemObjectId::Object(database_id.into()),
                    AclMode::CREATE,
                    role_id,
                )]
            }
        },
        Plan::CreateRole(plan::CreateRolePlan {
            name: _,
            attributes: _,
        }) => {
            vec![(SystemObjectId::System, AclMode::CREATE_ROLE, role_id)]
        }
        Plan::AlterRole(plan::AlterRolePlan {
            id: _,
            name: _,
            attributes: _,
        }) => {
            vec![(SystemObjectId::System, AclMode::CREATE_ROLE, role_id)]
        }
        Plan::CreateSource(plan::CreateSourcePlan {
            name,
            source: _,
            if_not_exists: _,
            timeline: _,
            cluster_config,
        }) => generate_required_source_privileges(name, cluster_config, role_id),
        Plan::CreateSources(plans) => plans
            .iter()
            .flat_map(
                |plan::CreateSourcePlans {
                     source_id: _,
                     plan:
                         plan::CreateSourcePlan {
                             name,
                             source: _,
                             if_not_exists: _,
                             timeline: _,
                             cluster_config,
                         },
                     resolved_ids: _,
                 }| {
                    generate_required_source_privileges(name, cluster_config, role_id).into_iter()
                },
            )
            .collect(),
        Plan::PurifiedAlterSource {
            // Keep in sync with  AlterSourcePlan elsewhere; right now this does
            // not affect the output privileges.
            alter_source: plan::AlterSourcePlan { id: _, action: _ },
            subsources,
        } => subsources
            .iter()
            .flat_map(
                |plan::CreateSourcePlans {
                     source_id: _,
                     plan:
                         plan::CreateSourcePlan {
                             name,
                             source: _,
                             if_not_exists: _,
                             timeline: _,
                             cluster_config,
                         },
                     resolved_ids: _,
                 }| {
                    generate_required_source_privileges(name, cluster_config, role_id).into_iter()
                },
            )
            .collect(),
        Plan::CreateSecret(plan::CreateSecretPlan {
            name,
            secret: _,
            if_not_exists: _,
        }) => vec![(
            SystemObjectId::Object(name.qualifiers.clone().into()),
            AclMode::CREATE,
            role_id,
        )],
        Plan::CreateSink(plan::CreateSinkPlan {
            name,
            sink,
            with_snapshot: _,
            if_not_exists: _,
            cluster_config,
        }) => {
            let mut privileges = vec![(
                SystemObjectId::Object(name.qualifiers.clone().into()),
                AclMode::CREATE,
                role_id,
            )];
            privileges.extend_from_slice(&generate_read_privileges(
                catalog,
                iter::once(sink.from),
                role_id,
            ));
            match cluster_config.cluster_id() {
                Some(id) => {
                    privileges.push((SystemObjectId::Object(id.into()), AclMode::CREATE, role_id))
                }
                None => privileges.push((SystemObjectId::System, AclMode::CREATE_CLUSTER, role_id)),
            }
            privileges
        }
        Plan::CreateTable(plan::CreateTablePlan {
            name,
            table: _,
            if_not_exists: _,
        }) => {
            vec![(
                SystemObjectId::Object(name.qualifiers.clone().into()),
                AclMode::CREATE,
                role_id,
            )]
        }
        Plan::CreateView(plan::CreateViewPlan {
            name,
            view: _,
            replace: _,
            drop_ids: _,
            if_not_exists: _,
            ambiguous_columns: _,
        }) => {
            vec![(
                SystemObjectId::Object(name.qualifiers.clone().into()),
                AclMode::CREATE,
                role_id,
            )]
        }
        Plan::CreateMaterializedView(plan::CreateMaterializedViewPlan {
            name,
            materialized_view,
            replace: _,
            drop_ids: _,
            if_not_exists: _,
            ambiguous_columns: _,
        }) => {
            vec![
                (
                    SystemObjectId::Object(name.qualifiers.clone().into()),
                    AclMode::CREATE,
                    role_id,
                ),
                (
                    SystemObjectId::Object(materialized_view.cluster_id.into()),
                    AclMode::CREATE,
                    role_id,
                ),
            ]
        }
        Plan::CreateIndex(plan::CreateIndexPlan {
            name,
            index,
            options: _,
            if_not_exists: _,
        }) => {
            vec![
                (
                    SystemObjectId::Object(name.qualifiers.clone().into()),
                    AclMode::CREATE,
                    role_id,
                ),
                (
                    SystemObjectId::Object(index.cluster_id.into()),
                    AclMode::CREATE,
                    role_id,
                ),
            ]
        }
        Plan::CreateType(plan::CreateTypePlan { name, typ: _ }) => {
            vec![(
                SystemObjectId::Object(name.qualifiers.clone().into()),
                AclMode::CREATE,
                role_id,
            )]
        }
        Plan::DropObjects(plan::DropObjectsPlan {
            referenced_ids,
            drop_ids: _,
            object_type,
        }) => {
            if object_type == &ObjectType::Role {
                vec![(SystemObjectId::System, AclMode::CREATE_ROLE, role_id)]
            } else {
                referenced_ids
                    .iter()
                    .filter_map(|id| match id {
                        ObjectId::ClusterReplica((cluster_id, _)) => Some((
                            SystemObjectId::Object(cluster_id.into()),
                            AclMode::USAGE,
                            role_id,
                        )),
                        ObjectId::Schema((database_spec, _)) => match database_spec {
                            ResolvedDatabaseSpecifier::Ambient => None,
                            ResolvedDatabaseSpecifier::Id(database_id) => Some((
                                SystemObjectId::Object(database_id.into()),
                                AclMode::USAGE,
                                role_id,
                            )),
                        },
                        ObjectId::Item(item_id) => {
                            let item = catalog.get_item(item_id);
                            Some((
                                SystemObjectId::Object(item.name().qualifiers.clone().into()),
                                AclMode::USAGE,
                                role_id,
                            ))
                        }
                        ObjectId::Cluster(_) | ObjectId::Database(_) | ObjectId::Role(_) => None,
                    })
                    .collect()
            }
        }
        Plan::ShowCreate(plan::ShowCreatePlan { id, row: _ }) => {
            let item = catalog.get_item(id);
            vec![(
                SystemObjectId::Object(item.name().qualifiers.clone().into()),
                AclMode::USAGE,
                role_id,
            )]
        }
        Plan::ShowColumns(plan::ShowColumnsPlan {
            id,
            select_plan,
            new_resolved_ids,
        }) => {
            let item = catalog.get_item(id);
            let mut privileges = vec![(
                SystemObjectId::Object(item.name().qualifiers.clone().into()),
                AclMode::USAGE,
                role_id,
            )];

            for privilege in generate_required_privileges(
                catalog,
                &Plan::Select(select_plan.clone()),
                target_cluster_id,
                new_resolved_ids,
                role_id,
            ) {
                privileges.push(privilege);
            }
            privileges
        }
        Plan::Select(plan::SelectPlan {
            source,
            when: _,
            finishing: _,
            copy_to: _,
        }) => {
            let mut privileges =
                generate_read_privileges(catalog, resolved_ids.0.iter().cloned(), role_id);
            if let Some(privilege) =
                generate_cluster_usage_privileges(source, target_cluster_id, role_id)
            {
                privileges.push(privilege);
            }
            privileges
        }
        Plan::Subscribe(plan::SubscribePlan {
            from: _,
            with_snapshot: _,
            when: _,
            up_to: _,
            copy_to: _,
            emit_progress: _,
            output: _,
        }) => {
            let mut privileges =
                generate_read_privileges(catalog, resolved_ids.0.iter().cloned(), role_id);
            if let Some(cluster_id) = target_cluster_id {
                privileges.push((
                    SystemObjectId::Object(cluster_id.into()),
                    AclMode::USAGE,
                    role_id,
                ));
            }
            privileges
        }
        Plan::Explain(plan::ExplainPlan {
            raw_plan: _,
            row_set_finishing: _,
            stage: _,
            format: _,
            config: _,
            no_errors: _,
            explainee: _,
        }) => generate_read_privileges(catalog, resolved_ids.0.iter().cloned(), role_id),
        Plan::CopyFrom(plan::CopyFromPlan {
            id,
            columns: _,
            params: _,
        }) => {
            let item = catalog.get_item(id);
            vec![
                (
                    SystemObjectId::Object(item.name().qualifiers.clone().into()),
                    AclMode::USAGE,
                    role_id,
                ),
                (SystemObjectId::Object(id.into()), AclMode::INSERT, role_id),
            ]
        }
        Plan::Insert(plan::InsertPlan {
            id,
            values,
            returning,
        }) => {
            let schema_id: ObjectId = catalog.get_item(id).name().qualifiers.clone().into();
            let mut privileges = vec![
                (
                    SystemObjectId::Object(schema_id.clone()),
                    AclMode::USAGE,
                    role_id,
                ),
                (SystemObjectId::Object(id.into()), AclMode::INSERT, role_id),
            ];
            let mut seen = BTreeSet::from([(schema_id, role_id)]);

            // We don't allow arbitrary sub-queries in `returning`. So either it
            // contains a column reference to the outer table or it's constant.
            if returning
                .iter()
                .any(|assignment| assignment.contains_column())
            {
                privileges.push((SystemObjectId::Object(id.into()), AclMode::SELECT, role_id));
                seen.insert((id.into(), role_id));
            }

            privileges.extend_from_slice(&generate_read_privileges_inner(
                catalog,
                values.depends_on().into_iter(),
                role_id,
                &mut seen,
            ));

            if let Some(privilege) =
                generate_cluster_usage_privileges(values, target_cluster_id, role_id)
            {
                privileges.push(privilege);
            } else if !returning.is_empty() {
                // TODO(jkosh44) returning may be a constant, but for now we are overly protective
                //  and require cluster privileges for all returning.
                if let Some(cluster_id) = target_cluster_id {
                    privileges.push((
                        SystemObjectId::Object(cluster_id.into()),
                        AclMode::USAGE,
                        role_id,
                    ));
                }
            }
            privileges
        }
        Plan::ReadThenWrite(plan::ReadThenWritePlan {
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
                (
                    SystemObjectId::Object(schema_id.clone()),
                    AclMode::USAGE,
                    role_id,
                ),
                (SystemObjectId::Object(id.into()), acl_mode, role_id),
            ];
            let mut seen = BTreeSet::from([(schema_id, role_id)]);

            // We don't allow arbitrary sub-queries in `assignments` or `returning`. So either they
            // contains a column reference to the outer table or it's constant.
            if assignments
                .values()
                .chain(returning.iter())
                .any(|assignment| assignment.contains_column())
            {
                privileges.push((SystemObjectId::Object(id.into()), AclMode::SELECT, role_id));
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

            if let Some(privilege) =
                generate_cluster_usage_privileges(selection, target_cluster_id, role_id)
            {
                privileges.push(privilege);
            }
            privileges
        }
        Plan::AlterOwner(plan::AlterOwnerPlan {
            id,
            object_type: _,
            new_owner: _,
        }) => match id {
            ObjectId::ClusterReplica((cluster_id, _)) => {
                vec![(
                    SystemObjectId::Object(cluster_id.into()),
                    AclMode::CREATE,
                    role_id,
                )]
            }
            ObjectId::Schema((database_spec, _)) => match database_spec {
                ResolvedDatabaseSpecifier::Ambient => Vec::new(),
                ResolvedDatabaseSpecifier::Id(database_id) => {
                    vec![(
                        SystemObjectId::Object(database_id.into()),
                        AclMode::CREATE,
                        role_id,
                    )]
                }
            },
            ObjectId::Item(item_id) => {
                let item = catalog.get_item(item_id);
                vec![(
                    SystemObjectId::Object(item.name().qualifiers.clone().into()),
                    AclMode::CREATE,
                    role_id,
                )]
            }
            ObjectId::Cluster(_) | ObjectId::Database(_) | ObjectId::Role(_) => Vec::new(),
        },
        Plan::AlterSetCluster(plan::AlterSetClusterPlan { id: _, set_cluster }) => {
            vec![(
                SystemObjectId::Object(set_cluster.into()),
                AclMode::CREATE,
                role_id,
            )]
        }
        Plan::GrantRole(plan::GrantRolePlan {
            role_ids: _,
            member_ids: _,
            grantor_id: _,
        })
        | Plan::RevokeRole(plan::RevokeRolePlan {
            role_ids: _,
            member_ids: _,
            grantor_id: _,
        }) => vec![(SystemObjectId::System, AclMode::CREATE_ROLE, role_id)],
        Plan::GrantPrivileges(plan::GrantPrivilegesPlan {
            update_privileges,
            grantees: _,
        })
        | Plan::RevokePrivileges(plan::RevokePrivilegesPlan {
            update_privileges,
            revokees: _,
        }) => {
            let mut privleges = Vec::with_capacity(update_privileges.len());
            for UpdatePrivilege { target_id, .. } in update_privileges {
                match target_id {
                    SystemObjectId::Object(object_id) => match object_id {
                        ObjectId::ClusterReplica((cluster_id, _)) => {
                            privleges.push((
                                SystemObjectId::Object(cluster_id.into()),
                                AclMode::USAGE,
                                role_id,
                            ));
                        }
                        ObjectId::Schema((database_spec, _)) => match database_spec {
                            ResolvedDatabaseSpecifier::Ambient => {}
                            ResolvedDatabaseSpecifier::Id(database_id) => {
                                privleges.push((
                                    SystemObjectId::Object(database_id.into()),
                                    AclMode::USAGE,
                                    role_id,
                                ));
                            }
                        },
                        ObjectId::Item(item_id) => {
                            let item = catalog.get_item(item_id);
                            privleges.push((
                                SystemObjectId::Object(item.name().qualifiers.clone().into()),
                                AclMode::USAGE,
                                role_id,
                            ))
                        }
                        ObjectId::Cluster(_) | ObjectId::Database(_) | ObjectId::Role(_) => {}
                    },
                    SystemObjectId::System => {}
                }
            }
            privleges
        }
        Plan::AlterDefaultPrivileges(plan::AlterDefaultPrivilegesPlan {
            privilege_objects,
            privilege_acl_items: _,
            is_grant: _,
        }) => privilege_objects
            .into_iter()
            .filter_map(|privilege_object| {
                if let (Some(database_id), Some(_)) =
                    (privilege_object.database_id, privilege_object.schema_id)
                {
                    Some((
                        SystemObjectId::Object(database_id.into()),
                        AclMode::USAGE,
                        role_id,
                    ))
                } else {
                    None
                }
            })
            .collect(),
        Plan::AlterClusterRename(plan::AlterClusterRenamePlan {
            id: _,
            name: _,
            to_name: _,
        })
        | Plan::AlterClusterReplicaRename(plan::AlterClusterReplicaRenamePlan {
            cluster_id: _,
            replica_id: _,
            name: _,
            to_name: _,
        })
        | Plan::AlterCluster(plan::AlterClusterPlan {
            id: _,
            name: _,
            options: _,
        })
        | Plan::CreateClusterReplica(plan::CreateClusterReplicaPlan {
            cluster_id: _,
            name: _,
            config: _,
        })
        | Plan::DiscardTemp
        | Plan::DiscardAll
        | Plan::EmptyQuery
        | Plan::ShowAllVariables
        | Plan::ShowVariable(plan::ShowVariablePlan { name: _ })
        | Plan::InspectShard(plan::InspectShardPlan { id: _ })
        | Plan::SetVariable(plan::SetVariablePlan {
            name: _,
            value: _,
            local: _,
        })
        | Plan::ResetVariable(plan::ResetVariablePlan { name: _ })
        | Plan::SetTransaction(plan::SetTransactionPlan { local: _, modes: _ })
        | Plan::StartTransaction(plan::StartTransactionPlan {
            access: _,
            isolation_level: _,
        })
        | Plan::CommitTransaction(plan::CommitTransactionPlan {
            transaction_type: _,
        })
        | Plan::AbortTransaction(plan::AbortTransactionPlan {
            transaction_type: _,
        })
        | Plan::CopyRows(plan::CopyRowsPlan {
            id: _,
            columns: _,
            rows: _,
        })
        | Plan::AlterNoop(plan::AlterNoopPlan { object_type: _ })
        | Plan::AlterIndexSetOptions(plan::AlterIndexSetOptionsPlan { id: _, options: _ })
        | Plan::AlterIndexResetOptions(plan::AlterIndexResetOptionsPlan { id: _, options: _ })
        | Plan::AlterSink(plan::AlterSinkPlan { id: _, size: _ })
        | Plan::AlterSource(plan::AlterSourcePlan { id: _, action: _ })
        | Plan::AlterItemRename(plan::AlterItemRenamePlan {
            id: _,
            current_full_name: _,
            to_name: _,
            object_type: _,
        })
        | Plan::AlterSecret(plan::AlterSecretPlan {
            id: _,
            secret_as: _,
        })
        | Plan::RotateKeys(plan::RotateKeysPlan { id: _ })
        | Plan::AlterSystemSet(plan::AlterSystemSetPlan { name: _, value: _ })
        | Plan::AlterSystemReset(plan::AlterSystemResetPlan { name: _ })
        | Plan::AlterSystemResetAll(plan::AlterSystemResetAllPlan {})
        | Plan::Declare(plan::DeclarePlan {
            name: _,
            stmt: _,
            sql: _,
        })
        | Plan::Fetch(plan::FetchPlan {
            name: _,
            count: _,
            timeout: _,
        })
        | Plan::Close(plan::ClosePlan { name: _ })
        | Plan::Prepare(plan::PreparePlan {
            name: _,
            stmt: _,
            desc: _,
            sql: _,
        })
        | Plan::Execute(plan::ExecutePlan { name: _, params: _ })
        | Plan::Deallocate(plan::DeallocatePlan { name: _ })
        | Plan::Raise(plan::RaisePlan { severity: _ })
        | Plan::DropOwned(plan::DropOwnedPlan {
            role_ids: _,
            drop_ids: _,
            privilege_revokes: _,
            default_privilege_revokes: _,
        })
        | Plan::ReassignOwned(plan::ReassignOwnedPlan {
            old_roles: _,
            new_role: _,
            reassign_ids: _,
        })
        | Plan::SideEffectingFunc(_) => Vec::new(),
    }
}

fn generate_required_source_privileges(
    name: &QualifiedItemName,
    cluster_config: &SourceSinkClusterConfig,
    role_id: RoleId,
) -> Vec<(SystemObjectId, AclMode, RoleId)> {
    let mut privileges = vec![(
        SystemObjectId::Object(name.qualifiers.clone().into()),
        AclMode::CREATE,
        role_id,
    )];
    match cluster_config.cluster_id() {
        Some(id) => privileges.push((SystemObjectId::Object(id.into()), AclMode::CREATE, role_id)),
        None => privileges.push((SystemObjectId::System, AclMode::CREATE_CLUSTER, role_id)),
    }
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
) -> Vec<(SystemObjectId, AclMode, RoleId)> {
    generate_read_privileges_inner(catalog, ids, role_id, &mut BTreeSet::new())
}

fn generate_read_privileges_inner(
    catalog: &impl SessionCatalog,
    ids: impl Iterator<Item = GlobalId>,
    role_id: RoleId,
    seen: &mut BTreeSet<(ObjectId, RoleId)>,
) -> Vec<(SystemObjectId, AclMode, RoleId)> {
    let mut privileges = Vec::new();
    let mut views = Vec::new();

    for id in ids {
        if seen.insert((id.into(), role_id)) {
            let item = catalog.get_item(&id);
            let schema_id: ObjectId = item.name().qualifiers.clone().into();
            if seen.insert((schema_id.clone(), role_id)) {
                privileges.push((SystemObjectId::Object(schema_id), AclMode::USAGE, role_id))
            }
            match item.item_type() {
                CatalogItemType::View | CatalogItemType::MaterializedView => {
                    privileges.push((SystemObjectId::Object(id.into()), AclMode::SELECT, role_id));
                    views.push((item.uses().0.iter().cloned(), item.owner_id()));
                }
                CatalogItemType::Table | CatalogItemType::Source => {
                    privileges.push((SystemObjectId::Object(id.into()), AclMode::SELECT, role_id));
                }
                CatalogItemType::Type | CatalogItemType::Secret | CatalogItemType::Connection => {
                    privileges.push((SystemObjectId::Object(id.into()), AclMode::USAGE, role_id));
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

fn generate_item_usage_privileges(
    catalog: &impl SessionCatalog,
    ids: &ResolvedIds,
    role_id: RoleId,
) -> BTreeSet<(SystemObjectId, AclMode, RoleId)> {
    // Use a `BTreeSet` to remove duplicate privileges.
    ids.0
        .iter()
        .filter_map(move |id| {
            let item = catalog.get_item(id);
            match item.item_type() {
                CatalogItemType::Type | CatalogItemType::Secret | CatalogItemType::Connection => {
                    let schema_id = item.name().qualifiers.clone().into();
                    Some([
                        (SystemObjectId::Object(schema_id), AclMode::USAGE, role_id),
                        (SystemObjectId::Object(id.into()), AclMode::USAGE, role_id),
                    ])
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
        .flatten()
        .collect()
}

fn generate_cluster_usage_privileges(
    expr: &MirRelationExpr,
    target_cluster_id: Option<ClusterId>,
    role_id: RoleId,
) -> Option<(SystemObjectId, AclMode, RoleId)> {
    // TODO(jkosh44) expr hasn't been fully optimized yet, so it might actually be a constant,
    //  but we mistakenly think that it's not. For now it's ok to be overly protective.
    if expr.as_const().is_none() {
        if let Some(cluster_id) = target_cluster_id {
            return Some((
                SystemObjectId::Object(cluster_id.into()),
                AclMode::USAGE,
                role_id,
            ));
        }
    }

    None
}

fn check_object_privileges(
    catalog: &impl SessionCatalog,
    privileges: Vec<(SystemObjectId, AclMode, RoleId)>,
    role_membership: BTreeSet<RoleId>,
    current_role_id: RoleId,
) -> Result<(), UnauthorizedError> {
    let mut role_memberships: BTreeMap<RoleId, BTreeSet<RoleId>> = BTreeMap::new();
    role_memberships.insert(current_role_id, role_membership);
    for (object_id, acl_mode, role_id) in privileges {
        let role_membership = role_memberships
            .entry(role_id)
            .or_insert_with_key(|role_id| catalog.collect_role_membership(role_id));
        let object_privileges = catalog
            .get_privileges(&object_id)
            .expect("only object types with privileges will generate required privileges");
        let role_privileges = role_membership
            .iter()
            .flat_map(|role_id| object_privileges.get_acl_items_for_grantee(role_id))
            .map(|mz_acl_item| mz_acl_item.acl_mode)
            .fold(AclMode::empty(), |accum, acl_mode| accum.union(acl_mode));
        if !role_privileges.contains(acl_mode) {
            return Err(UnauthorizedError::Privilege {
                object_description: ErrorMessageObjectDescription::from_id(&object_id, catalog),
            });
        }
    }

    Ok(())
}

fn check_superuser_required(plan: &Plan) -> Result<(), UnauthorizedError> {
    match plan {
        // Altering the default privileges for the PUBLIC role (aka ALL ROLES) will affect all roles
        // that currently exist and roles that will exist in the future. It's impossible for an exising
        // role to be a member of a role that doesn't exist yet, so no current role could possibly have
        // the privileges required to alter default privileges for the PUBLIC role. Therefore we
        // only superusers can alter default privileges for the PUBLIC role.
        Plan::AlterDefaultPrivileges(plan::AlterDefaultPrivilegesPlan {
            privilege_objects,
            ..
        }) if privilege_objects
            .iter()
            .any(|privilege_object| privilege_object.role_id.is_public()) =>
        {
            Err(UnauthorizedError::Superuser {
                action: "ALTER DEFAULT PRIVILEGES FOR ALL ROLES".to_string(),
            })
        }
        // To grant/revoke a privilege on some object, generally the grantor/revoker must be the
        // owner of that object (or have a grant option on that object which isn't implemented in
        // Materialize yet). There is no owner of the entire system, so it's only reasonable to
        // restrict granting/revoking system privileges to superusers.
        Plan::GrantPrivileges(plan::GrantPrivilegesPlan {
            update_privileges, ..
        })
        | Plan::RevokePrivileges(plan::RevokePrivilegesPlan {
            update_privileges, ..
        }) if update_privileges
            .iter()
            .any(|update_privilege| update_privilege.target_id.is_system()) =>
        {
            Err(UnauthorizedError::Superuser {
                action: "GRANT/REVOKE SYSTEM PRIVILEGES".to_string(),
            })
        }
        _ => Ok(()),
    }
}

pub(crate) const fn all_object_privileges(object_type: SystemObjectType) -> AclMode {
    const TABLE_ACL_MODE: AclMode = AclMode::INSERT
        .union(AclMode::SELECT)
        .union(AclMode::UPDATE)
        .union(AclMode::DELETE);
    const USAGE_CREATE_ACL_MODE: AclMode = AclMode::USAGE.union(AclMode::CREATE);
    const ALL_SYSTEM_PRIVILEGES: AclMode = AclMode::CREATE_ROLE
        .union(AclMode::CREATE_DB)
        .union(AclMode::CREATE_CLUSTER);
    const EMPTY_ACL_MODE: AclMode = AclMode::empty();
    match object_type {
        SystemObjectType::Object(ObjectType::Table) => TABLE_ACL_MODE,
        SystemObjectType::Object(ObjectType::View) => AclMode::SELECT,
        SystemObjectType::Object(ObjectType::MaterializedView) => AclMode::SELECT,
        SystemObjectType::Object(ObjectType::Source) => AclMode::SELECT,
        SystemObjectType::Object(ObjectType::Sink) => EMPTY_ACL_MODE,
        SystemObjectType::Object(ObjectType::Index) => EMPTY_ACL_MODE,
        SystemObjectType::Object(ObjectType::Type) => AclMode::USAGE,
        SystemObjectType::Object(ObjectType::Role) => EMPTY_ACL_MODE,
        SystemObjectType::Object(ObjectType::Cluster) => USAGE_CREATE_ACL_MODE,
        SystemObjectType::Object(ObjectType::ClusterReplica) => EMPTY_ACL_MODE,
        SystemObjectType::Object(ObjectType::Secret) => AclMode::USAGE,
        SystemObjectType::Object(ObjectType::Connection) => AclMode::USAGE,
        SystemObjectType::Object(ObjectType::Database) => USAGE_CREATE_ACL_MODE,
        SystemObjectType::Object(ObjectType::Schema) => USAGE_CREATE_ACL_MODE,
        SystemObjectType::Object(ObjectType::Func) => EMPTY_ACL_MODE,
        SystemObjectType::System => ALL_SYSTEM_PRIVILEGES,
    }
}

pub(crate) const fn owner_privilege(object_type: ObjectType, owner_id: RoleId) -> MzAclItem {
    MzAclItem {
        grantee: owner_id,
        grantor: owner_id,
        acl_mode: all_object_privileges(SystemObjectType::Object(object_type)),
    }
}

pub(crate) const fn default_builtin_object_privilege(object_type: ObjectType) -> MzAclItem {
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
