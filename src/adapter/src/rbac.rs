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
use maplit::btreeset;
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
    CommentObjectId, ObjectId, QualifiedItemName, ResolvedDatabaseSpecifier, ResolvedIds,
    SystemObjectId,
};
use mz_sql::plan;
use mz_sql::plan::{
    Explainee, MutationKind, Plan, SideEffectingFunc, SourceSinkClusterConfig, UpdatePrivilege,
};
use mz_sql::session::user::{SUPPORT_USER, SYSTEM_USER};
use mz_sql::session::vars::SystemVars;
use mz_sql_parser::ast::QualifiedReplica;

use crate::catalog::storage::MZ_SYSTEM_ROLE_ID;
use crate::client::ConnectionId;
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

/// RBAC requirements for executing a given plan.
struct RbacRequirements {
    /// The role memberships required.
    role_membership: BTreeSet<RoleId>,
    /// The object ownerships required.
    ownership: Vec<ObjectId>,
    /// The privileges required. The tuples are of the form:
    /// (What object the privilege is on, What privilege is required, Who must possess the privilege).
    privileges: Vec<(SystemObjectId, AclMode, RoleId)>,
    /// Whether or not referenced item usage privileges are required.
    item_usage: bool,
    /// Some action if superuser is required to perform that action, None otherwise.
    superuser_action: Option<String>,
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
    /// The action cannot be performed by the mz_support role.
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

    // Obtain all roles that the current session is a member of.
    let current_role_id = session.current_role_id();
    let role_membership = catalog.collect_role_membership(current_role_id);

    let rbac_requirements = generate_rbac_requirements(
        catalog,
        plan,
        coord.active_conns(),
        target_cluster_id,
        resolved_ids,
        *current_role_id,
    );

    if rbac_requirements.item_usage {
        check_item_usage(catalog, session, resolved_ids)?;
    }

    // Validate that the current session has the required role membership to execute the provided
    // plan.
    let unheld_membership: Vec<_> = rbac_requirements
        .role_membership
        .difference(&role_membership)
        .collect();
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
    let unheld_ownership = rbac_requirements
        .ownership
        .into_iter()
        .filter(|ownership| !check_owner_roles(ownership, &role_membership, catalog))
        .collect();
    ownership_err(unheld_ownership, catalog)?;

    check_object_privileges(
        catalog,
        rbac_requirements.privileges,
        role_membership,
        *current_role_id,
    )?;

    if let Some(action) = rbac_requirements.superuser_action {
        return Err(AdapterError::Unauthorized(UnauthorizedError::Superuser {
            action,
        }));
    }

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

/// Generates all requirements needed to execute a given plan.
fn generate_rbac_requirements(
    catalog: &impl SessionCatalog,
    plan: &Plan,
    active_conns: &BTreeMap<ConnectionId, ConnMeta>,
    target_cluster_id: Option<ClusterId>,
    resolved_ids: &ResolvedIds,
    role_id: RoleId,
) -> RbacRequirements {
    match plan {
        Plan::CreateConnection(plan::CreateConnectionPlan {
            name,
            if_not_exists: _,
            connection: _,
            validate: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: vec![(
                SystemObjectId::Object(name.qualifiers.clone().into()),
                AclMode::CREATE,
                role_id,
            )],
            item_usage: true,
            superuser_action: None,
        },
        Plan::CreateDatabase(plan::CreateDatabasePlan {
            name: _,
            if_not_exists: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: vec![(SystemObjectId::System, AclMode::CREATE_DB, role_id)],
            item_usage: true,
            superuser_action: None,
        },
        Plan::CreateSchema(plan::CreateSchemaPlan {
            database_spec,
            schema_name: _,
            if_not_exists: _,
        }) => {
            let privileges = match database_spec {
                ResolvedDatabaseSpecifier::Ambient => Vec::new(),
                ResolvedDatabaseSpecifier::Id(database_id) => {
                    vec![(
                        SystemObjectId::Object(database_id.into()),
                        AclMode::CREATE,
                        role_id,
                    )]
                }
            };
            RbacRequirements {
                role_membership: BTreeSet::new(),
                ownership: Vec::new(),
                privileges,
                item_usage: true,
                superuser_action: None,
            }
        }
        Plan::CreateRole(plan::CreateRolePlan {
            name: _,
            attributes: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: vec![(SystemObjectId::System, AclMode::CREATE_ROLE, role_id)],
            item_usage: true,
            superuser_action: None,
        },
        Plan::CreateCluster(plan::CreateClusterPlan {
            name: _,
            variant: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: vec![(SystemObjectId::System, AclMode::CREATE_CLUSTER, role_id)],
            item_usage: true,
            superuser_action: None,
        },
        Plan::CreateClusterReplica(plan::CreateClusterReplicaPlan {
            cluster_id,
            name: _,
            config: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: vec![ObjectId::Cluster(*cluster_id)],
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::CreateSource(plan::CreateSourcePlan {
            name,
            source: _,
            if_not_exists: _,
            timeline: _,
            cluster_config,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: generate_required_source_privileges(name, cluster_config, role_id),
            item_usage: true,
            superuser_action: None,
        },
        Plan::CreateSources(plans) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: plans
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
                        generate_required_source_privileges(name, cluster_config, role_id)
                            .into_iter()
                    },
                )
                .collect(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::CreateSecret(plan::CreateSecretPlan {
            name,
            secret: _,
            if_not_exists: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: vec![(
                SystemObjectId::Object(name.qualifiers.clone().into()),
                AclMode::CREATE,
                role_id,
            )],
            item_usage: true,
            superuser_action: None,
        },
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
            RbacRequirements {
                role_membership: BTreeSet::new(),
                ownership: Vec::new(),
                privileges,
                item_usage: true,
                superuser_action: None,
            }
        }
        Plan::CreateTable(plan::CreateTablePlan {
            name,
            table: _,
            if_not_exists: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: vec![(
                SystemObjectId::Object(name.qualifiers.clone().into()),
                AclMode::CREATE,
                role_id,
            )],
            item_usage: true,
            superuser_action: None,
        },
        Plan::CreateView(plan::CreateViewPlan {
            name,
            view: _,
            replace,
            drop_ids: _,
            if_not_exists: _,
            ambiguous_columns: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: replace
                .map(|id| vec![ObjectId::Item(id)])
                .unwrap_or_default(),
            privileges: vec![(
                SystemObjectId::Object(name.qualifiers.clone().into()),
                AclMode::CREATE,
                role_id,
            )],
            item_usage: true,
            superuser_action: None,
        },
        Plan::CreateMaterializedView(plan::CreateMaterializedViewPlan {
            name,
            materialized_view,
            replace,
            drop_ids: _,
            if_not_exists: _,
            ambiguous_columns: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: replace
                .map(|id| vec![ObjectId::Item(id)])
                .unwrap_or_default(),
            privileges: vec![
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
            ],
            item_usage: true,
            superuser_action: None,
        },
        Plan::CreateIndex(plan::CreateIndexPlan {
            name,
            index,
            options: _,
            if_not_exists: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: vec![ObjectId::Item(index.on)],
            privileges: vec![
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
            ],
            item_usage: true,
            superuser_action: None,
        },
        Plan::CreateType(plan::CreateTypePlan { name, typ: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: vec![(
                SystemObjectId::Object(name.qualifiers.clone().into()),
                AclMode::CREATE,
                role_id,
            )],
            item_usage: true,
            superuser_action: None,
        },
        Plan::Comment(plan::CommentPlan {
            object_id,
            sub_component: _,
            comment: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: match object_id {
                CommentObjectId::Table(global_id) | CommentObjectId::View(global_id) => {
                    vec![ObjectId::Item(*global_id)]
                }
            },
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::DiscardTemp => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::DiscardAll => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::DropObjects(plan::DropObjectsPlan {
            referenced_ids,
            drop_ids: _,
            object_type,
        }) => {
            let privileges = if object_type == &ObjectType::Role {
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
            };
            RbacRequirements {
                role_membership: BTreeSet::new(),
                // Do not need ownership of descendant objects.
                ownership: referenced_ids.clone(),
                privileges,
                item_usage: true,
                superuser_action: None,
            }
        }
        Plan::DropOwned(plan::DropOwnedPlan {
            role_ids,
            drop_ids: _,
            privilege_revokes: _,
            default_privilege_revokes: _,
        }) => RbacRequirements {
            role_membership: role_ids.into_iter().cloned().collect(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::EmptyQuery => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::ShowAllVariables => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::ShowCreate(plan::ShowCreatePlan { id, row: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: vec![(
                SystemObjectId::Object(catalog.get_item(id).name().qualifiers.clone().into()),
                AclMode::USAGE,
                role_id,
            )],
            item_usage: false,
            superuser_action: None,
        },
        Plan::ShowColumns(plan::ShowColumnsPlan {
            id,
            select_plan,
            new_resolved_ids,
        }) => {
            let mut privileges = vec![(
                SystemObjectId::Object(catalog.get_item(id).name().qualifiers.clone().into()),
                AclMode::USAGE,
                role_id,
            )];

            for privilege in generate_rbac_requirements(
                catalog,
                &Plan::Select(select_plan.clone()),
                active_conns,
                target_cluster_id,
                new_resolved_ids,
                role_id,
            )
            .privileges
            {
                privileges.push(privilege);
            }
            RbacRequirements {
                role_membership: BTreeSet::new(),
                ownership: Vec::new(),
                privileges,
                item_usage: true,
                superuser_action: None,
            }
        }
        Plan::ShowVariable(plan::ShowVariablePlan { name: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::InspectShard(plan::InspectShardPlan { id: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::SetVariable(plan::SetVariablePlan {
            name: _,
            value: _,
            local: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::ResetVariable(plan::ResetVariablePlan { name: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::SetTransaction(plan::SetTransactionPlan { local: _, modes: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::StartTransaction(plan::StartTransactionPlan {
            access: _,
            isolation_level: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::CommitTransaction(plan::CommitTransactionPlan {
            transaction_type: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::AbortTransaction(plan::AbortTransactionPlan {
            transaction_type: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
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
            RbacRequirements {
                role_membership: BTreeSet::new(),
                ownership: Vec::new(),
                privileges,
                item_usage: true,
                superuser_action: None,
            }
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
            RbacRequirements {
                role_membership: BTreeSet::new(),
                ownership: Vec::new(),
                privileges,
                item_usage: true,
                superuser_action: None,
            }
        }
        Plan::CopyFrom(plan::CopyFromPlan {
            id,
            columns: _,
            params: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: vec![
                (
                    SystemObjectId::Object(catalog.get_item(id).name().qualifiers.clone().into()),
                    AclMode::USAGE,
                    role_id,
                ),
                (SystemObjectId::Object(id.into()), AclMode::INSERT, role_id),
            ],
            item_usage: true,
            superuser_action: None,
        },
        Plan::ExplainPlan(plan::ExplainPlanPlan {
            stage: _,
            format: _,
            config: _,
            explainee,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: match explainee {
                Explainee::MaterializedView(id) | Explainee::Index(id) => {
                    let item = catalog.get_item(id);
                    let schema_id: ObjectId = item.name().qualifiers.clone().into();
                    vec![(SystemObjectId::Object(schema_id), AclMode::USAGE, role_id)]
                }
                Explainee::Query { .. } => {
                    generate_read_privileges(catalog, resolved_ids.0.iter().cloned(), role_id)
                }
            },
            item_usage: match explainee {
                Explainee::MaterializedView(_) | Explainee::Index(_) => false,
                Explainee::Query { .. } => true,
            },
            superuser_action: None,
        },
        Plan::ExplainTimestamp(plan::ExplainTimestampPlan {
            format: _,
            raw_plan: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: generate_read_privileges(catalog, resolved_ids.0.iter().cloned(), role_id),
            item_usage: true,
            superuser_action: None,
        },
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
            RbacRequirements {
                role_membership: BTreeSet::new(),
                ownership: Vec::new(),
                privileges,
                item_usage: true,
                superuser_action: None,
            }
        }
        Plan::AlterCluster(plan::AlterClusterPlan {
            id,
            name: _,
            options: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: vec![ObjectId::Cluster(*id)],
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::AlterNoop(plan::AlterNoopPlan { object_type: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::AlterIndexSetOptions(plan::AlterIndexSetOptionsPlan { id, options: _ }) => {
            RbacRequirements {
                role_membership: BTreeSet::new(),
                ownership: vec![ObjectId::Item(*id)],
                privileges: Vec::new(),
                item_usage: true,
                superuser_action: None,
            }
        }
        Plan::AlterIndexResetOptions(plan::AlterIndexResetOptionsPlan { id, options: _ }) => {
            RbacRequirements {
                role_membership: BTreeSet::new(),
                ownership: vec![ObjectId::Item(*id)],
                privileges: Vec::new(),
                item_usage: true,
                superuser_action: None,
            }
        }
        Plan::AlterSetCluster(plan::AlterSetClusterPlan { id, set_cluster }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: vec![ObjectId::Item(*id)],
            privileges: vec![(
                SystemObjectId::Object(set_cluster.into()),
                AclMode::CREATE,
                role_id,
            )],
            item_usage: true,
            superuser_action: None,
        },
        Plan::AlterSink(plan::AlterSinkPlan { id, size: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: vec![ObjectId::Item(*id)],
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::AlterSource(plan::AlterSourcePlan { id, action: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: vec![ObjectId::Item(*id)],
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::PurifiedAlterSource {
            // Keep in sync with  AlterSourcePlan elsewhere; right now this does
            // not affect the output privileges.
            alter_source: plan::AlterSourcePlan { id, action: _ },
            subsources,
        } => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: vec![ObjectId::Item(*id)],
            privileges: subsources
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
                        generate_required_source_privileges(name, cluster_config, role_id)
                            .into_iter()
                    },
                )
                .collect(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::AlterClusterRename(plan::AlterClusterRenamePlan {
            id,
            name: _,
            to_name: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: vec![ObjectId::Cluster(*id)],
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::AlterClusterReplicaRename(plan::AlterClusterReplicaRenamePlan {
            cluster_id,
            replica_id,
            name: _,
            to_name: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: vec![ObjectId::ClusterReplica((*cluster_id, *replica_id))],
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::AlterItemRename(plan::AlterItemRenamePlan {
            id,
            current_full_name: _,
            to_name: _,
            object_type: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: vec![ObjectId::Item(*id)],
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::AlterSecret(plan::AlterSecretPlan { id, secret_as: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: vec![ObjectId::Item(*id)],
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::AlterSystemSet(plan::AlterSystemSetPlan { name: _, value: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::AlterSystemReset(plan::AlterSystemResetPlan { name: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::AlterSystemResetAll(plan::AlterSystemResetAllPlan {}) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::AlterRole(plan::AlterRolePlan {
            id: _,
            name: _,
            attributes: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: vec![(SystemObjectId::System, AclMode::CREATE_ROLE, role_id)],
            item_usage: true,
            superuser_action: None,
        },
        Plan::AlterOwner(plan::AlterOwnerPlan {
            id,
            object_type: _,
            new_owner,
        }) => {
            let privileges = match id {
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
            };
            RbacRequirements {
                role_membership: btreeset![*new_owner],
                ownership: vec![id.clone()],
                privileges,
                item_usage: true,
                superuser_action: None,
            }
        }
        Plan::Declare(plan::DeclarePlan {
            name: _,
            stmt: _,
            sql: _,
            params: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::Fetch(plan::FetchPlan {
            name: _,
            count: _,
            timeout: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::Close(plan::ClosePlan { name: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
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
            RbacRequirements {
                role_membership: BTreeSet::new(),
                ownership: Vec::new(),
                privileges,
                item_usage: true,
                superuser_action: None,
            }
        }
        Plan::Prepare(plan::PreparePlan {
            name: _,
            stmt: _,
            desc: _,
            sql: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::Execute(plan::ExecutePlan { name: _, params: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::Deallocate(plan::DeallocatePlan { name: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::Raise(plan::RaisePlan { severity: _ }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::RotateKeys(plan::RotateKeysPlan { id }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: vec![ObjectId::Item(*id)],
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::GrantRole(plan::GrantRolePlan {
            role_ids: _,
            member_ids: _,
            grantor_id: _,
        })
        | Plan::RevokeRole(plan::RevokeRolePlan {
            role_ids: _,
            member_ids: _,
            grantor_id: _,
        }) => RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: vec![(SystemObjectId::System, AclMode::CREATE_ROLE, role_id)],
            item_usage: true,
            superuser_action: None,
        },
        Plan::GrantPrivileges(plan::GrantPrivilegesPlan {
            update_privileges,
            grantees: _,
        })
        | Plan::RevokePrivileges(plan::RevokePrivilegesPlan {
            update_privileges,
            revokees: _,
        }) => {
            let mut privileges = Vec::with_capacity(update_privileges.len());
            for UpdatePrivilege { target_id, .. } in update_privileges {
                match target_id {
                    SystemObjectId::Object(object_id) => match object_id {
                        ObjectId::ClusterReplica((cluster_id, _)) => {
                            privileges.push((
                                SystemObjectId::Object(cluster_id.into()),
                                AclMode::USAGE,
                                role_id,
                            ));
                        }
                        ObjectId::Schema((database_spec, _)) => match database_spec {
                            ResolvedDatabaseSpecifier::Ambient => {}
                            ResolvedDatabaseSpecifier::Id(database_id) => {
                                privileges.push((
                                    SystemObjectId::Object(database_id.into()),
                                    AclMode::USAGE,
                                    role_id,
                                ));
                            }
                        },
                        ObjectId::Item(item_id) => {
                            let item = catalog.get_item(item_id);
                            privileges.push((
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
            RbacRequirements {
                role_membership: BTreeSet::new(),
                ownership: update_privileges
                    .iter()
                    .filter_map(|update_privilege| update_privilege.target_id.object_id())
                    .cloned()
                    .collect(),
                privileges,
                item_usage: true,
                // To grant/revoke a privilege on some object, generally the grantor/revoker must be the
                // owner of that object (or have a grant option on that object which isn't implemented in
                // Materialize yet). There is no owner of the entire system, so it's only reasonable to
                // restrict granting/revoking system privileges to superusers.
                superuser_action: if update_privileges
                    .iter()
                    .any(|update_privilege| update_privilege.target_id.is_system())
                {
                    Some("GRANT/REVOKE SYSTEM PRIVILEGES".to_string())
                } else {
                    None
                },
            }
        }
        Plan::AlterDefaultPrivileges(plan::AlterDefaultPrivilegesPlan {
            privilege_objects,
            privilege_acl_items: _,
            is_grant: _,
        }) => RbacRequirements {
            role_membership: privilege_objects
                .iter()
                .map(|privilege_object| privilege_object.role_id)
                .collect(),
            ownership: Vec::new(),
            privileges: privilege_objects
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
            item_usage: true,
            // Altering the default privileges for the PUBLIC role (aka ALL ROLES) will affect all roles
            // that currently exist and roles that will exist in the future. It's impossible for an exising
            // role to be a member of a role that doesn't exist yet, so no current role could possibly have
            // the privileges required to alter default privileges for the PUBLIC role. Therefore we
            // only superusers can alter default privileges for the PUBLIC role.
            superuser_action: if privilege_objects
                .iter()
                .any(|privilege_object| privilege_object.role_id.is_public())
            {
                Some("ALTER DEFAULT PRIVILEGES FOR ALL ROLES".to_string())
            } else {
                None
            },
        },
        Plan::ReassignOwned(plan::ReassignOwnedPlan {
            old_roles,
            new_role,
            reassign_ids: _,
        }) => RbacRequirements {
            role_membership: old_roles
                .into_iter()
                .cloned()
                .chain(iter::once(*new_role))
                .collect(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: true,
            superuser_action: None,
        },
        Plan::SideEffectingFunc(func) => {
            let role_membership = match func {
                SideEffectingFunc::PgCancelBackend { connection_id } => {
                    match active_conns.get(connection_id) {
                        Some(conn) => btreeset![conn.authenticated_role],
                        None => BTreeSet::new(),
                    }
                }
            };
            RbacRequirements {
                role_membership,
                ownership: Vec::new(),
                privileges: Vec::new(),
                item_usage: true,
                superuser_action: None,
            }
        }
        Plan::ValidateConnection(plan::ValidateConnectionPlan { id, connection: _ }) => {
            let schema_id: ObjectId = catalog.get_item(id).name().qualifiers.clone().into();
            RbacRequirements {
                role_membership: BTreeSet::new(),
                ownership: Vec::new(),
                privileges: vec![
                    (SystemObjectId::Object(schema_id), AclMode::USAGE, role_id),
                    (SystemObjectId::Object(id.into()), AclMode::USAGE, role_id),
                ],
                item_usage: true,
                superuser_action: None,
            }
        }
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
