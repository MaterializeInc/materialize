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
use mz_controller_types::ClusterId;
use mz_expr::CollectionPlan;
use mz_ore::str::StrExt;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql_parser::ast::{Ident, QualifiedReplica};
use once_cell::sync::Lazy;
use tracing::debug;

use crate::catalog::{
    CatalogItemType, ErrorMessageObjectDescription, ObjectType, SessionCatalog, SystemObjectType,
};
use crate::names::{
    CommentObjectId, ObjectId, QualifiedItemName, ResolvedDatabaseSpecifier, ResolvedIds,
    SystemObjectId,
};
use crate::plan::{self, PlanKind};
use crate::plan::{
    DataSourceDesc, Explainee, MutationKind, Plan, SideEffectingFunc, UpdatePrivilege,
};
use crate::session::metadata::SessionMetadata;
use crate::session::user::{MZ_SUPPORT_ROLE_ID, MZ_SYSTEM_ROLE_ID, SUPPORT_USER, SYSTEM_USER};
use crate::session::vars::SystemVars;

/// Common checks that need to be performed before we can start checking a role's privileges.
fn rbac_preamble(
    catalog: &impl SessionCatalog,
    session_meta: &dyn SessionMetadata,
    rbac_requirements: RbacRequirements,
) -> Result<RbacRequirements, UnauthorizedError> {
    // PostgreSQL allows users that have their role dropped to perform some actions,
    // such as `SET ROLE` and certain `SELECT` queries. We haven't implemented
    // `SET ROLE` and feel it's safer to force to user to re-authenticate if their
    // role is dropped.
    if catalog
        .try_get_role(&session_meta.role_metadata().current_role)
        .is_none()
    {
        return Err(UnauthorizedError::ConcurrentRoleDrop(
            session_meta.role_metadata().current_role.clone(),
        ));
    };
    if catalog
        .try_get_role(&session_meta.role_metadata().session_role)
        .is_none()
    {
        return Err(UnauthorizedError::ConcurrentRoleDrop(
            session_meta.role_metadata().session_role.clone(),
        ));
    };
    if catalog
        .try_get_role(&session_meta.role_metadata().authenticated_role)
        .is_none()
    {
        return Err(UnauthorizedError::ConcurrentRoleDrop(
            session_meta.role_metadata().authenticated_role.clone(),
        ));
    };

    // Skip RBAC non-mandatory checks if RBAC is disabled. However, we never skip RBAC checks for
    // system roles. This allows us to limit access of system users even when RBAC is off.
    let is_rbac_disabled = !is_rbac_enabled_for_session(catalog.system_vars(), session_meta)
        && !session_meta.role_metadata().current_role.is_system()
        && !session_meta.role_metadata().session_role.is_system();
    // Skip RBAC checks on user items if the session is a superuser.
    let is_superuser = session_meta.is_superuser();
    if is_rbac_disabled || is_superuser {
        return Ok(rbac_requirements.filter_to_mandatory_requirements());
    }

    Ok(rbac_requirements)
}

// The default item types that most statements require USAGE privileges for.
static DEFAULT_ITEM_USAGE: Lazy<BTreeSet<CatalogItemType>> = Lazy::new(|| {
    btreeset! {CatalogItemType::Secret, CatalogItemType::Connection}
});
// CREATE statements require USAGE privileges on the default item types and USAGE privileges on
// Types.
pub static CREATE_ITEM_USAGE: Lazy<BTreeSet<CatalogItemType>> = Lazy::new(|| {
    let mut items = DEFAULT_ITEM_USAGE.clone();
    items.insert(CatalogItemType::Type);
    items
});
pub static EMPTY_ITEM_USAGE: Lazy<BTreeSet<CatalogItemType>> = Lazy::new(BTreeSet::new);

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
    /// The active role was dropped while a user was logged in.
    #[error("role {0} was concurrently dropped")]
    ConcurrentRoleDrop(RoleId),
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
            UnauthorizedError::ConcurrentRoleDrop(_) => {
                Some("Please disconnect and re-connect with a valid role.".into())
            }
            UnauthorizedError::Ownership { .. }
            | UnauthorizedError::RoleMembership { .. }
            | UnauthorizedError::Privilege { .. } => None,
        }
    }
}

/// RBAC requirements for executing a given plan.
#[derive(Debug)]
struct RbacRequirements {
    /// The role memberships required.
    role_membership: BTreeSet<RoleId>,
    /// The object ownerships required.
    ownership: Vec<ObjectId>,
    /// The privileges required. The tuples are of the form:
    /// (What object the privilege is on, What privilege is required, Who must possess the privilege).
    privileges: Vec<(SystemObjectId, AclMode, RoleId)>,
    /// The types of catalog items that this plan requires USAGE privileges on.
    ///
    /// Most plans will require USAGE on secrets and connections but some plans, like SHOW CREATE,
    /// can reference an item without requiring any privileges on that item.
    item_usage: &'static BTreeSet<CatalogItemType>,
    /// Some action if superuser is required to perform that action, None otherwise.
    superuser_action: Option<String>,
}

impl RbacRequirements {
    fn empty() -> RbacRequirements {
        RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: &EMPTY_ITEM_USAGE,
            superuser_action: None,
        }
    }

    fn validate(
        self,
        catalog: &impl SessionCatalog,
        session: &dyn SessionMetadata,
        resolved_ids: &ResolvedIds,
    ) -> Result<(), UnauthorizedError> {
        // Obtain all roles that the current session is a member of.
        let role_membership =
            catalog.collect_role_membership(&session.role_metadata().current_role);

        check_usage(catalog, session, resolved_ids, self.item_usage)?;

        // Validate that the current session has the required role membership to execute the provided
        // plan.
        let unheld_membership: Vec<_> = self.role_membership.difference(&role_membership).collect();
        if !unheld_membership.is_empty() {
            let role_names = unheld_membership
                .into_iter()
                .map(|role_id| {
                    // Some role references may no longer exist due to concurrent drops.
                    catalog
                        .try_get_role(role_id)
                        .map(|role| role.name().to_string())
                        .unwrap_or(role_id.to_string())
                })
                .collect();
            return Err(UnauthorizedError::RoleMembership { role_names });
        }

        // Validate that the current session has the required object ownership to execute the provided
        // plan.
        let unheld_ownership = self
            .ownership
            .into_iter()
            .filter(|ownership| !check_owner_roles(ownership, &role_membership, catalog))
            .collect();
        ownership_err(unheld_ownership, catalog)?;

        check_object_privileges(
            catalog,
            self.privileges,
            role_membership,
            session.role_metadata().current_role,
        )?;

        if let Some(action) = self.superuser_action {
            return Err(UnauthorizedError::Superuser { action });
        }

        Ok(())
    }

    fn filter_to_mandatory_requirements(self) -> RbacRequirements {
        let RbacRequirements {
            role_membership,
            ownership,
            privileges,
            item_usage,
            superuser_action: _,
        } = self;
        let role_membership = role_membership
            .into_iter()
            .filter(|id| id.is_system())
            .collect();
        let ownership = ownership.into_iter().filter(|id| id.is_system()).collect();
        let privileges = privileges
            .into_iter()
            .filter(|(id, _, _)| matches!(id, SystemObjectId::Object(oid) if oid.is_system()))
            // We allow reading objects for superusers and when RBAC is off.
            .update(|(_, acl_mode, _)| acl_mode.remove(AclMode::SELECT))
            .filter(|(_, acl_mode, _)| !acl_mode.is_empty())
            .collect();
        let superuser_action = None;
        RbacRequirements {
            role_membership,
            ownership,
            privileges,
            item_usage,
            superuser_action,
        }
    }
}

impl Default for RbacRequirements {
    fn default() -> Self {
        RbacRequirements {
            role_membership: BTreeSet::new(),
            ownership: Vec::new(),
            privileges: Vec::new(),
            item_usage: &DEFAULT_ITEM_USAGE,
            superuser_action: None,
        }
    }
}

/// Checks if a `session` is authorized to use `resolved_ids`. If not, an error is returned.
pub fn check_usage(
    catalog: &impl SessionCatalog,
    session: &dyn SessionMetadata,
    resolved_ids: &ResolvedIds,
    item_types: &BTreeSet<CatalogItemType>,
) -> Result<(), UnauthorizedError> {
    // Obtain all roles that the current session is a member of.
    let role_membership = catalog.collect_role_membership(&session.role_metadata().current_role);

    // Certain statements depend on objects that haven't been created yet, like sub-sources, so we
    // need to filter those out.
    let existing_resolved_ids = resolved_ids
        .0
        .iter()
        .filter(|id| catalog.try_get_item(id).is_some())
        .cloned()
        .collect();
    let existing_resolved_ids = ResolvedIds(existing_resolved_ids);

    let required_privileges = generate_usage_privileges(
        catalog,
        &existing_resolved_ids,
        session.role_metadata().current_role,
        item_types,
    )
    .into_iter()
    .collect();

    let mut rbac_requirements = RbacRequirements::empty();
    rbac_requirements.privileges = required_privileges;
    let rbac_requirements = rbac_preamble(catalog, session, rbac_requirements)?;
    let required_privileges = rbac_requirements.privileges;

    check_object_privileges(
        catalog,
        required_privileges,
        role_membership,
        session.role_metadata().current_role,
    )?;

    Ok(())
}

/// Checks if a session is authorized to execute a plan. If not, an error is returned.
pub fn check_plan(
    catalog: &impl SessionCatalog,
    // Map from connection IDs to authenticated roles. The roles may have been dropped concurrently.
    active_conns: &BTreeMap<u32, RoleId>,
    session: &dyn SessionMetadata,
    plan: &Plan,
    target_cluster_id: Option<ClusterId>,
    resolved_ids: &ResolvedIds,
) -> Result<(), UnauthorizedError> {
    let rbac_requirements = generate_rbac_requirements(
        catalog,
        plan,
        active_conns,
        target_cluster_id,
        session.role_metadata().current_role,
    );
    let rbac_requirements = rbac_preamble(catalog, session, rbac_requirements)?;
    debug!(
        "rbac requirements {rbac_requirements:?} for plan {:?}",
        PlanKind::from(plan)
    );
    rbac_requirements.validate(catalog, session, resolved_ids)
}

/// Returns true if RBAC is turned on for a session, false otherwise.
pub fn is_rbac_enabled_for_session(
    system_vars: &SystemVars,
    session: &dyn SessionMetadata,
) -> bool {
    let server_enabled = system_vars.enable_rbac_checks();
    let session_enabled = session.enable_session_rbac_checks();

    // The session flag allows users to turn RBAC on for just their session while the server flag
    // allows users to turn RBAC on for everyone.
    server_enabled || session_enabled
}

/// Generates all requirements needed to execute a given plan.
fn generate_rbac_requirements(
    catalog: &impl SessionCatalog,
    plan: &Plan,
    active_conns: &BTreeMap<u32, RoleId>,
    target_cluster_id: Option<ClusterId>,
    role_id: RoleId,
) -> RbacRequirements {
    match plan {
        Plan::CreateConnection(plan::CreateConnectionPlan {
            name,
            if_not_exists: _,
            connection: _,
            validate: _,
        }) => RbacRequirements {
            privileges: vec![(
                SystemObjectId::Object(name.qualifiers.clone().into()),
                AclMode::CREATE,
                role_id,
            )],
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::CreateDatabase(plan::CreateDatabasePlan {
            name: _,
            if_not_exists: _,
        }) => RbacRequirements {
            privileges: vec![(SystemObjectId::System, AclMode::CREATE_DB, role_id)],
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
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
                privileges,
                item_usage: &CREATE_ITEM_USAGE,
                ..Default::default()
            }
        }
        Plan::CreateRole(plan::CreateRolePlan {
            name: _,
            attributes: _,
        }) => RbacRequirements {
            privileges: vec![(SystemObjectId::System, AclMode::CREATE_ROLE, role_id)],
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::CreateCluster(plan::CreateClusterPlan {
            name: _,
            variant: _,
        }) => RbacRequirements {
            privileges: vec![(SystemObjectId::System, AclMode::CREATE_CLUSTER, role_id)],
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::CreateClusterReplica(plan::CreateClusterReplicaPlan {
            cluster_id,
            name: _,
            config: _,
        }) => RbacRequirements {
            ownership: vec![ObjectId::Cluster(*cluster_id)],
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::CreateSource(plan::CreateSourcePlan {
            name,
            source,
            if_not_exists: _,
            timeline: _,
            in_cluster,
        }) => RbacRequirements {
            privileges: generate_required_source_privileges(
                name,
                &source.data_source,
                *in_cluster,
                role_id,
            ),
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::CreateSources(plans) => RbacRequirements {
            privileges: plans
                .iter()
                .flat_map(
                    |plan::CreateSourcePlans {
                         source_id: _,
                         plan:
                             plan::CreateSourcePlan {
                                 name,
                                 source,
                                 if_not_exists: _,
                                 timeline: _,
                                 in_cluster,
                             },
                         resolved_ids: _,
                     }| {
                        generate_required_source_privileges(
                            name,
                            &source.data_source,
                            *in_cluster,
                            role_id,
                        )
                        .into_iter()
                    },
                )
                .collect(),
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::CreateSecret(plan::CreateSecretPlan {
            name,
            secret: _,
            if_not_exists: _,
        }) => RbacRequirements {
            privileges: vec![(
                SystemObjectId::Object(name.qualifiers.clone().into()),
                AclMode::CREATE,
                role_id,
            )],
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::CreateSink(plan::CreateSinkPlan {
            name,
            sink,
            with_snapshot: _,
            if_not_exists: _,
            in_cluster,
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
            privileges.push((
                SystemObjectId::Object(in_cluster.into()),
                AclMode::CREATE,
                role_id,
            ));
            RbacRequirements {
                privileges,
                item_usage: &CREATE_ITEM_USAGE,
                ..Default::default()
            }
        }
        Plan::CreateTable(plan::CreateTablePlan {
            name,
            table: _,
            if_not_exists: _,
        }) => RbacRequirements {
            privileges: vec![(
                SystemObjectId::Object(name.qualifiers.clone().into()),
                AclMode::CREATE,
                role_id,
            )],
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::CreateView(plan::CreateViewPlan {
            name,
            view: _,
            replace,
            drop_ids: _,
            if_not_exists: _,
            ambiguous_columns: _,
        }) => RbacRequirements {
            ownership: replace
                .map(|id| vec![ObjectId::Item(id)])
                .unwrap_or_default(),
            privileges: vec![(
                SystemObjectId::Object(name.qualifiers.clone().into()),
                AclMode::CREATE,
                role_id,
            )],
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::CreateMaterializedView(plan::CreateMaterializedViewPlan {
            name,
            materialized_view,
            replace,
            drop_ids: _,
            if_not_exists: _,
            ambiguous_columns: _,
        }) => RbacRequirements {
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
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::CreateIndex(plan::CreateIndexPlan {
            name,
            index,
            if_not_exists: _,
        }) => RbacRequirements {
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
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::CreateType(plan::CreateTypePlan { name, typ: _ }) => RbacRequirements {
            privileges: vec![(
                SystemObjectId::Object(name.qualifiers.clone().into()),
                AclMode::CREATE,
                role_id,
            )],
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::Comment(plan::CommentPlan {
            object_id,
            sub_component: _,
            comment: _,
        }) => {
            let (ownership, privileges) = match object_id {
                // Roles don't have owners, instead we require the current session to have the
                // `CREATEROLE` privilege.
                CommentObjectId::Role(_) => (
                    Vec::new(),
                    vec![(SystemObjectId::System, AclMode::CREATE_ROLE, role_id)],
                ),
                _ => (vec![ObjectId::from(*object_id)], Vec::new()),
            };
            RbacRequirements {
                ownership,
                privileges,
                ..Default::default()
            }
        }
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
                // Do not need ownership of descendant objects.
                ownership: referenced_ids.clone(),
                privileges,
                ..Default::default()
            }
        }
        Plan::DropOwned(plan::DropOwnedPlan {
            role_ids,
            drop_ids: _,
            privilege_revokes: _,
            default_privilege_revokes: _,
        }) => RbacRequirements {
            role_membership: role_ids.into_iter().cloned().collect(),
            ..Default::default()
        },
        Plan::ShowCreate(plan::ShowCreatePlan { id, row: _ }) => RbacRequirements {
            privileges: vec![(
                SystemObjectId::Object(catalog.get_item(id).name().qualifiers.clone().into()),
                AclMode::USAGE,
                role_id,
            )],
            item_usage: &EMPTY_ITEM_USAGE,
            ..Default::default()
        },
        Plan::ShowColumns(plan::ShowColumnsPlan {
            id,
            select_plan,
            new_resolved_ids: _,
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
                role_id,
            )
            .privileges
            {
                privileges.push(privilege);
            }
            RbacRequirements {
                privileges,
                ..Default::default()
            }
        }
        Plan::Select(plan::SelectPlan {
            source,
            when: _,
            finishing: _,
            copy_to: _,
        }) => {
            let mut privileges =
                generate_read_privileges(catalog, source.depends_on().into_iter(), role_id);
            if let Some(privilege) = generate_cluster_usage_privileges(
                source.as_const().is_some(),
                target_cluster_id,
                role_id,
            ) {
                privileges.push(privilege);
            }
            RbacRequirements {
                privileges,
                ..Default::default()
            }
        }
        Plan::Subscribe(plan::SubscribePlan {
            from,
            with_snapshot: _,
            when: _,
            up_to: _,
            copy_to: _,
            emit_progress: _,
            output: _,
        }) => {
            let mut privileges =
                generate_read_privileges(catalog, from.depends_on().into_iter(), role_id);
            if let Some(cluster_id) = target_cluster_id {
                privileges.push((
                    SystemObjectId::Object(cluster_id.into()),
                    AclMode::USAGE,
                    role_id,
                ));
            }
            RbacRequirements {
                privileges,
                ..Default::default()
            }
        }
        Plan::CopyFrom(plan::CopyFromPlan {
            id,
            columns: _,
            params: _,
        }) => RbacRequirements {
            privileges: vec![
                (
                    SystemObjectId::Object(catalog.get_item(id).name().qualifiers.clone().into()),
                    AclMode::USAGE,
                    role_id,
                ),
                (SystemObjectId::Object(id.into()), AclMode::INSERT, role_id),
            ],
            ..Default::default()
        },
        Plan::CopyTo(plan::CopyToPlan {
            select_plan,
            desc: _,
            to: _,
            connection: _,
            connection_id: _,
            format_params: _,
            max_file_size: _,
        }) => {
            let mut privileges = generate_read_privileges(
                catalog,
                select_plan.source.depends_on().into_iter(),
                role_id,
            );
            if let Some(cluster_id) = target_cluster_id {
                privileges.push((
                    SystemObjectId::Object(cluster_id.into()),
                    AclMode::USAGE,
                    role_id,
                ));
            }
            RbacRequirements {
                privileges,
                ..Default::default()
            }
        }
        Plan::ExplainPlan(plan::ExplainPlanPlan {
            stage: _,
            format: _,
            config: _,
            explainee,
        })
        | Plan::ExplainPushdown(plan::ExplainPushdownPlan { explainee }) => RbacRequirements {
            privileges: match explainee {
                Explainee::View(id)
                | Explainee::MaterializedView(id)
                | Explainee::Index(id)
                | Explainee::ReplanView(id)
                | Explainee::ReplanMaterializedView(id)
                | Explainee::ReplanIndex(id) => {
                    let item = catalog.get_item(id);
                    let schema_id: ObjectId = item.name().qualifiers.clone().into();
                    vec![(SystemObjectId::Object(schema_id), AclMode::USAGE, role_id)]
                }
                Explainee::Statement(stmt) => stmt
                    .depends_on()
                    .into_iter()
                    .map(|id| {
                        let item = catalog.get_item(&id);
                        let schema_id: ObjectId = item.name().qualifiers.clone().into();
                        (SystemObjectId::Object(schema_id), AclMode::USAGE, role_id)
                    })
                    .collect(),
            },
            item_usage: match explainee {
                Explainee::View(..)
                | Explainee::MaterializedView(..)
                | Explainee::Index(..)
                | Explainee::ReplanView(..)
                | Explainee::ReplanMaterializedView(..)
                | Explainee::ReplanIndex(..) => &EMPTY_ITEM_USAGE,
                Explainee::Statement(_) => &DEFAULT_ITEM_USAGE,
            },
            ..Default::default()
        },
        Plan::ExplainSinkSchema(plan::ExplainSinkSchemaPlan { sink_from, .. }) => {
            RbacRequirements {
                privileges: {
                    let item = catalog.get_item(sink_from);
                    let schema_id: ObjectId = item.name().qualifiers.clone().into();
                    vec![(SystemObjectId::Object(schema_id), AclMode::USAGE, role_id)]
                },
                item_usage: &EMPTY_ITEM_USAGE,
                ..Default::default()
            }
        }
        Plan::ExplainTimestamp(plan::ExplainTimestampPlan {
            format: _,
            raw_plan,
            when: _,
        }) => RbacRequirements {
            privileges: raw_plan
                .depends_on()
                .into_iter()
                .map(|id| {
                    let item = catalog.get_item(&id);
                    let schema_id: ObjectId = item.name().qualifiers.clone().into();
                    (SystemObjectId::Object(schema_id), AclMode::USAGE, role_id)
                })
                .collect(),
            ..Default::default()
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

            if let Some(privilege) = generate_cluster_usage_privileges(
                values.as_const().is_some(),
                target_cluster_id,
                role_id,
            ) {
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
                privileges,
                ..Default::default()
            }
        }
        Plan::AlterCluster(plan::AlterClusterPlan {
            id,
            name: _,
            options: _,
        }) => RbacRequirements {
            ownership: vec![ObjectId::Cluster(*id)],
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::AlterSetCluster(plan::AlterSetClusterPlan { id, set_cluster }) => RbacRequirements {
            ownership: vec![ObjectId::Item(*id)],
            privileges: vec![(
                SystemObjectId::Object(set_cluster.into()),
                AclMode::CREATE,
                role_id,
            )],
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::AlterRetainHistory(plan::AlterRetainHistoryPlan {
            id,
            window: _,
            value: _,
            object_type: _,
        }) => RbacRequirements {
            ownership: vec![ObjectId::Item(*id)],
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::AlterConnection(plan::AlterConnectionPlan { id, action: _ }) => RbacRequirements {
            ownership: vec![ObjectId::Item(*id)],
            ..Default::default()
        },
        Plan::AlterSource(plan::AlterSourcePlan { id, action: _ }) => RbacRequirements {
            ownership: vec![ObjectId::Item(*id)],
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },

        Plan::AlterClusterRename(plan::AlterClusterRenamePlan {
            id,
            name: _,
            to_name: _,
        }) => RbacRequirements {
            ownership: vec![ObjectId::Cluster(*id)],
            ..Default::default()
        },
        Plan::AlterClusterSwap(plan::AlterClusterSwapPlan {
            id_a,
            id_b,
            name_a: _,
            name_b: _,
            name_temp: _,
        }) => RbacRequirements {
            ownership: vec![ObjectId::Cluster(*id_a), ObjectId::Cluster(*id_b)],
            ..Default::default()
        },
        Plan::AlterClusterReplicaRename(plan::AlterClusterReplicaRenamePlan {
            cluster_id,
            replica_id,
            name: _,
            to_name: _,
        }) => RbacRequirements {
            ownership: vec![ObjectId::ClusterReplica((*cluster_id, *replica_id))],
            ..Default::default()
        },
        Plan::AlterItemRename(plan::AlterItemRenamePlan {
            id,
            current_full_name: _,
            to_name: _,
            object_type: _,
        }) => RbacRequirements {
            ownership: vec![ObjectId::Item(*id)],
            ..Default::default()
        },
        Plan::AlterItemSwap(plan::AlterItemSwapPlan {
            id_a,
            id_b,
            full_name_a: _,
            full_name_b: _,
            object_type: _,
        }) => RbacRequirements {
            ownership: vec![ObjectId::Item(*id_a), ObjectId::Item(*id_b)],
            ..Default::default()
        },
        Plan::AlterSchemaRename(plan::AlterSchemaRenamePlan {
            cur_schema_spec,
            new_schema_name: _,
        }) => {
            let privileges = match cur_schema_spec.0 {
                ResolvedDatabaseSpecifier::Id(db_id) => vec![(
                    SystemObjectId::Object(ObjectId::Database(db_id)),
                    AclMode::CREATE,
                    role_id,
                )],
                ResolvedDatabaseSpecifier::Ambient => vec![],
            };

            RbacRequirements {
                ownership: vec![ObjectId::Schema(*cur_schema_spec)],
                privileges,
                ..Default::default()
            }
        }
        Plan::AlterSchemaSwap(plan::AlterSchemaSwapPlan {
            schema_a_spec,
            schema_a_name: _,
            schema_b_spec,
            schema_b_name: _,
            name_temp: _,
        }) => {
            let mut privileges = vec![];
            if let ResolvedDatabaseSpecifier::Id(id_a) = schema_a_spec.0 {
                privileges.push((
                    SystemObjectId::Object(ObjectId::Database(id_a)),
                    AclMode::CREATE,
                    role_id,
                ));
            }
            if let ResolvedDatabaseSpecifier::Id(id_b) = schema_b_spec.0 {
                privileges.push((
                    SystemObjectId::Object(ObjectId::Database(id_b)),
                    AclMode::CREATE,
                    role_id,
                ));
            }

            RbacRequirements {
                ownership: vec![
                    ObjectId::Schema(*schema_a_spec),
                    ObjectId::Schema(*schema_b_spec),
                ],
                privileges,
                ..Default::default()
            }
        }
        Plan::AlterSecret(plan::AlterSecretPlan { id, secret_as: _ }) => RbacRequirements {
            ownership: vec![ObjectId::Item(*id)],
            item_usage: &CREATE_ITEM_USAGE,
            ..Default::default()
        },
        Plan::AlterRole(plan::AlterRolePlan {
            id,
            name: _,
            option,
        }) => match option {
            // Roles are allowed to change their own variables.
            plan::PlannedAlterRoleOption::Variable(_) if role_id == *id => {
                RbacRequirements::default()
            }
            // Otherwise to ALTER a role, you need to have the CREATE_ROLE privilege.
            _ => RbacRequirements {
                privileges: vec![(SystemObjectId::System, AclMode::CREATE_ROLE, role_id)],
                item_usage: &CREATE_ITEM_USAGE,
                ..Default::default()
            },
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
                role_membership: BTreeSet::from([*new_owner]),
                ownership: vec![id.clone()],
                privileges,
                ..Default::default()
            }
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

            if let Some(privilege) = generate_cluster_usage_privileges(
                selection.as_const().is_some(),
                target_cluster_id,
                role_id,
            ) {
                privileges.push(privilege);
            }
            RbacRequirements {
                privileges,
                ..Default::default()
            }
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
        }) => RbacRequirements {
            privileges: vec![(SystemObjectId::System, AclMode::CREATE_ROLE, role_id)],
            ..Default::default()
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
                ownership: update_privileges
                    .iter()
                    .filter_map(|update_privilege| update_privilege.target_id.object_id())
                    .cloned()
                    .collect(),
                privileges,
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
                ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
        },
        Plan::SideEffectingFunc(func) => {
            let role_membership = match func {
                SideEffectingFunc::PgCancelBackend { connection_id } => {
                    match active_conns.get(connection_id) {
                        Some(authenticated_role) => BTreeSet::from([*authenticated_role]),
                        None => BTreeSet::new(),
                    }
                }
            };
            RbacRequirements {
                role_membership,
                ..Default::default()
            }
        }
        Plan::ValidateConnection(plan::ValidateConnectionPlan { id, connection: _ }) => {
            let schema_id: ObjectId = catalog.get_item(id).name().qualifiers.clone().into();
            RbacRequirements {
                privileges: vec![
                    (SystemObjectId::Object(schema_id), AclMode::USAGE, role_id),
                    (SystemObjectId::Object(id.into()), AclMode::USAGE, role_id),
                ],
                ..Default::default()
            }
        }
        Plan::DiscardTemp
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
        | Plan::AlterNoop(plan::AlterNoopPlan { object_type: _ })
        | Plan::AlterSystemSet(plan::AlterSystemSetPlan { name: _, value: _ })
        | Plan::AlterSystemReset(plan::AlterSystemResetPlan { name: _ })
        | Plan::AlterSystemResetAll(plan::AlterSystemResetAllPlan {})
        | Plan::Declare(plan::DeclarePlan {
            name: _,
            stmt: _,
            sql: _,
            params: _,
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
        | Plan::Raise(plan::RaisePlan { severity: _ }) => Default::default(),
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
                    // Note: using unchecked here is okay because the values are coming from an
                    // already existing name.
                    let name = QualifiedReplica {
                        cluster: Ident::new_unchecked(cluster.name()),
                        replica: Ident::new_unchecked(replica.name()),
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
    data_source: &DataSourceDesc,
    in_cluster: Option<ClusterId>,
    role_id: RoleId,
) -> Vec<(SystemObjectId, AclMode, RoleId)> {
    let mut privileges = vec![(
        SystemObjectId::Object(name.qualifiers.clone().into()),
        AclMode::CREATE,
        role_id,
    )];
    match (data_source, in_cluster) {
        (_, Some(id)) => {
            privileges.push((SystemObjectId::Object(id.into()), AclMode::CREATE, role_id))
        }
        (DataSourceDesc::Ingestion(_), None) => {
            privileges.push((SystemObjectId::System, AclMode::CREATE_CLUSTER, role_id))
        }
        // Non-ingestion data-sources have meaningless cluster config's (for now...) and they need
        // to be ignored.
        // This feels very brittle, but there's not much we can do until the UNDEFINED cluster
        // config is removed.
        (_, None) => {}
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
                    views.push((item.references().0.clone().into_iter(), item.owner_id()));
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

fn generate_usage_privileges(
    catalog: &impl SessionCatalog,
    ids: &ResolvedIds,
    role_id: RoleId,
    item_types: &BTreeSet<CatalogItemType>,
) -> BTreeSet<(SystemObjectId, AclMode, RoleId)> {
    // Use a `BTreeSet` to remove duplicate privileges.
    ids.0
        .iter()
        .filter_map(move |id| {
            let item = catalog.get_item(id);
            if item_types.contains(&item.item_type()) {
                let schema_id = item.name().qualifiers.clone().into();
                Some([
                    (SystemObjectId::Object(schema_id), AclMode::USAGE, role_id),
                    (SystemObjectId::Object(id.into()), AclMode::USAGE, role_id),
                ])
            } else {
                None
            }
        })
        .flatten()
        .collect()
}

fn generate_cluster_usage_privileges(
    expr_is_const: bool,
    target_cluster_id: Option<ClusterId>,
    role_id: RoleId,
) -> Option<(SystemObjectId, AclMode, RoleId)> {
    // TODO(jkosh44) expr hasn't been fully optimized yet, so it might actually be a constant,
    //  but we mistakenly think that it's not. For now it's ok to be overly protective.
    if !expr_is_const {
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
                object_description: ErrorMessageObjectDescription::from_sys_id(&object_id, catalog),
            });
        }
    }

    Ok(())
}

pub const fn all_object_privileges(object_type: SystemObjectType) -> AclMode {
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

pub const fn owner_privilege(object_type: ObjectType, owner_id: RoleId) -> MzAclItem {
    MzAclItem {
        grantee: owner_id,
        grantor: owner_id,
        acl_mode: all_object_privileges(SystemObjectType::Object(object_type)),
    }
}

const fn default_builtin_object_acl_mode(object_type: ObjectType) -> AclMode {
    match object_type {
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
    }
}

pub const fn support_builtin_object_privilege(object_type: ObjectType) -> MzAclItem {
    let acl_mode = default_builtin_object_acl_mode(object_type);
    MzAclItem {
        grantee: MZ_SUPPORT_ROLE_ID,
        grantor: MZ_SYSTEM_ROLE_ID,
        acl_mode,
    }
}

pub const fn default_builtin_object_privilege(object_type: ObjectType) -> MzAclItem {
    let acl_mode = default_builtin_object_acl_mode(object_type);
    MzAclItem {
        grantee: RoleId::Public,
        grantor: MZ_SYSTEM_ROLE_ID,
        acl_mode,
    }
}
