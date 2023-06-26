// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Access control list (ACL).
//!
//! This module houses the handlers for statements that modify privileges in the catalog, like
//! `GRANT`, `REVOKE`, and `REASSIGN OWNED`.

use std::collections::BTreeSet;

use itertools::Itertools;
use mz_sql_parser::ast::display::AstDisplay;

use crate::ast::{Ident, QualifiedReplica, UnresolvedDatabaseName};
use crate::catalog::{
    CatalogItemType, DefaultPrivilegeAclItem, DefaultPrivilegeObject, ObjectType, SystemObjectType,
};
use crate::names::{
    Aug, ObjectId, ResolvedDatabaseSpecifier, ResolvedRoleName, SchemaSpecifier, SystemObjectId,
};
use crate::plan::error::PlanError;
use crate::plan::statement::ddl::{
    ensure_cluster_is_not_managed, resolve_cluster, resolve_cluster_replica, resolve_database,
    resolve_item, resolve_schema,
};
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::{
    AlterDefaultPrivilegesPlan, AlterNoopPlan, AlterOwnerPlan, GrantPrivilegesPlan, GrantRolePlan,
    Plan, PlanNotice, ReassignOwnedPlan, RevokePrivilegesPlan, RevokeRolePlan, UpdatePrivilege,
};
use crate::session::user::SYSTEM_USER;
use mz_ore::str::StrExt;
use mz_repr::adt::mz_acl_item::AclMode;
use mz_repr::role_id::RoleId;
use mz_sql_parser::ast::{
    AbbreviatedGrantOrRevokeStatement, AlterDefaultPrivilegesStatement, AlterOwnerStatement,
    GrantPrivilegesStatement, GrantRoleStatement, GrantTargetAllSpecification,
    GrantTargetSpecification, GrantTargetSpecificationInner, Privilege, PrivilegeSpecification,
    ReassignOwnedStatement, RevokePrivilegesStatement, RevokeRoleStatement,
    TargetRoleSpecification, UnresolvedItemName, UnresolvedObjectName, UnresolvedSchemaName,
};

pub fn describe_alter_owner(
    _: &StatementContext,
    _: AlterOwnerStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_owner(
    scx: &StatementContext,
    AlterOwnerStatement {
        object_type,
        if_exists,
        name,
        new_owner,
    }: AlterOwnerStatement<Aug>,
) -> Result<Plan, PlanError> {
    let object_type = object_type.into();
    match (object_type, name) {
        (ObjectType::Cluster, UnresolvedObjectName::Cluster(name)) => {
            plan_alter_cluster_owner(scx, if_exists, name, new_owner.id)
        }
        (ObjectType::ClusterReplica, UnresolvedObjectName::ClusterReplica(name)) => {
            plan_alter_cluster_replica_owner(scx, if_exists, name, new_owner.id)
        }
        (ObjectType::Database, UnresolvedObjectName::Database(name)) => {
            plan_alter_database_owner(scx, if_exists, name, new_owner.id)
        }
        (ObjectType::Schema, UnresolvedObjectName::Schema(name)) => {
            plan_alter_schema_owner(scx, if_exists, name, new_owner.id)
        }
        (ObjectType::Role, UnresolvedObjectName::Role(_)) => unreachable!("rejected by the parser"),
        (
            object_type @ ObjectType::Cluster
            | object_type @ ObjectType::ClusterReplica
            | object_type @ ObjectType::Database
            | object_type @ ObjectType::Schema
            | object_type @ ObjectType::Role,
            name,
        )
        | (
            object_type,
            name @ UnresolvedObjectName::Cluster(_)
            | name @ UnresolvedObjectName::ClusterReplica(_)
            | name @ UnresolvedObjectName::Database(_)
            | name @ UnresolvedObjectName::Schema(_)
            | name @ UnresolvedObjectName::Role(_),
        ) => {
            unreachable!("parser set the wrong object type '{object_type:?}' for name {name:?}")
        }
        (object_type, UnresolvedObjectName::Item(name)) => {
            plan_alter_item_owner(scx, object_type, if_exists, name, new_owner.id)
        }
    }
}

fn plan_alter_cluster_owner(
    scx: &StatementContext,
    if_exists: bool,
    name: Ident,
    new_owner: RoleId,
) -> Result<Plan, PlanError> {
    match resolve_cluster(scx, &name, if_exists)? {
        Some(cluster) => Ok(Plan::AlterOwner(AlterOwnerPlan {
            id: ObjectId::Cluster(cluster.id()),
            object_type: ObjectType::Cluster,
            new_owner,
        })),
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type: ObjectType::Cluster,
            });
            Ok(Plan::AlterNoop(AlterNoopPlan {
                object_type: ObjectType::Cluster,
            }))
        }
    }
}

fn plan_alter_cluster_replica_owner(
    scx: &StatementContext,
    if_exists: bool,
    name: QualifiedReplica,
    new_owner: RoleId,
) -> Result<Plan, PlanError> {
    match resolve_cluster_replica(scx, &name, if_exists)? {
        Some((cluster, replica_id)) => {
            ensure_cluster_is_not_managed(scx, cluster.id())?;
            Ok(Plan::AlterOwner(AlterOwnerPlan {
                id: ObjectId::ClusterReplica((cluster.id(), replica_id)),
                object_type: ObjectType::ClusterReplica,
                new_owner,
            }))
        }
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type: ObjectType::ClusterReplica,
            });

            Ok(Plan::AlterNoop(AlterNoopPlan {
                object_type: ObjectType::ClusterReplica,
            }))
        }
    }
}

fn plan_alter_database_owner(
    scx: &StatementContext,
    if_exists: bool,
    name: UnresolvedDatabaseName,
    new_owner: RoleId,
) -> Result<Plan, PlanError> {
    match resolve_database(scx, &name, if_exists)? {
        Some(database) => Ok(Plan::AlterOwner(AlterOwnerPlan {
            id: ObjectId::Database(database.id()),
            object_type: ObjectType::Database,
            new_owner,
        })),
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type: ObjectType::Database,
            });

            Ok(Plan::AlterNoop(AlterNoopPlan {
                object_type: ObjectType::Database,
            }))
        }
    }
}

fn plan_alter_schema_owner(
    scx: &StatementContext,
    if_exists: bool,
    name: UnresolvedSchemaName,
    new_owner: RoleId,
) -> Result<Plan, PlanError> {
    match resolve_schema(scx, name.clone(), if_exists)? {
        Some((database_spec, schema_spec)) => {
            if let ResolvedDatabaseSpecifier::Ambient = database_spec {
                sql_bail!(
                    "cannot alter schema {name} because it is required by the database system",
                );
            }
            if let SchemaSpecifier::Temporary = schema_spec {
                sql_bail!("cannot alter schema {name} because it is a temporary schema",)
            }
            Ok(Plan::AlterOwner(AlterOwnerPlan {
                id: ObjectId::Schema((database_spec, schema_spec)),
                object_type: ObjectType::Schema,
                new_owner,
            }))
        }
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type: ObjectType::Schema,
            });

            Ok(Plan::AlterNoop(AlterNoopPlan {
                object_type: ObjectType::Schema,
            }))
        }
    }
}

fn plan_alter_item_owner(
    scx: &StatementContext,
    object_type: ObjectType,
    if_exists: bool,
    name: UnresolvedItemName,
    new_owner: RoleId,
) -> Result<Plan, PlanError> {
    match resolve_item(scx, name.clone(), if_exists)? {
        Some(item) => {
            if item.id().is_system() {
                sql_bail!(
                    "cannot alter item {} because it is required by the database system",
                    scx.catalog.resolve_full_name(item.name()),
                );
            }
            let item_type = item.item_type();

            // Return a more helpful error on `ALTER VIEW <materialized-view>`.
            if object_type == ObjectType::View && item_type == CatalogItemType::MaterializedView {
                let name = scx.catalog.resolve_full_name(item.name()).to_string();
                return Err(PlanError::AlterViewOnMaterializedView(name));
            } else if object_type != item_type {
                sql_bail!(
                    "{} is a {} not a {}",
                    scx.catalog
                        .resolve_full_name(item.name())
                        .to_string()
                        .quoted(),
                    item.item_type(),
                    format!("{object_type}").to_lowercase(),
                );
            }
            Ok(Plan::AlterOwner(AlterOwnerPlan {
                id: ObjectId::Item(item.id()),
                object_type,
                new_owner,
            }))
        }
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type,
            });

            Ok(Plan::AlterNoop(AlterNoopPlan { object_type }))
        }
    }
}

pub fn describe_grant_role(
    _: &StatementContext,
    _: GrantRoleStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_grant_role(
    scx: &StatementContext,
    GrantRoleStatement {
        role_names,
        member_names,
    }: GrantRoleStatement<Aug>,
) -> Result<Plan, PlanError> {
    // In PostgreSQL, the grantor must either be a role with ADMIN OPTION on the role being granted,
    // or the bootstrap superuser. We do not have ADMIN OPTION implemented and 'mz_system' is our
    // equivalent of the bootstrap superuser. Therefore the grantor is always 'mz_system'.
    // For more details see:
    // https://github.com/postgres/postgres/blob/064eb89e83ea0f59426c92906329f1e6c423dfa4/src/backend/commands/user.c#L2180-L2238
    let grantor_id = scx
        .catalog
        .resolve_role(&SYSTEM_USER.name)
        .expect("system user must exist")
        .id();
    Ok(Plan::GrantRole(GrantRolePlan {
        role_ids: role_names
            .into_iter()
            .map(|role_name| role_name.id)
            .collect(),
        member_ids: member_names
            .into_iter()
            .map(|member_name| member_name.id)
            .collect(),
        grantor_id,
    }))
}

pub fn describe_revoke_role(
    _: &StatementContext,
    _: RevokeRoleStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_revoke_role(
    scx: &StatementContext,
    RevokeRoleStatement {
        role_names,
        member_names,
    }: RevokeRoleStatement<Aug>,
) -> Result<Plan, PlanError> {
    // In PostgreSQL, the same role membership can be granted multiple times by different grantors.
    // When revoking a role membership, only the membership granted by the specified grantor is
    // revoked. The grantor must either be a role with ADMIN OPTION on the role being granted,
    // or the bootstrap superuser. We do not have ADMIN OPTION implemented and 'mz_system' is our
    // equivalent of the bootstrap superuser. Therefore the grantor is always 'mz_system'.
    // For more details see:
    // https://github.com/postgres/postgres/blob/064eb89e83ea0f59426c92906329f1e6c423dfa4/src/backend/commands/user.c#L2180-L2238
    let grantor_id = scx
        .catalog
        .resolve_role(&SYSTEM_USER.name)
        .expect("system user must exist")
        .id();
    Ok(Plan::RevokeRole(RevokeRolePlan {
        role_ids: role_names
            .into_iter()
            .map(|role_name| role_name.id)
            .collect(),
        member_ids: member_names
            .into_iter()
            .map(|member_name| member_name.id)
            .collect(),
        grantor_id,
    }))
}

pub fn describe_grant_privileges(
    _: &StatementContext,
    _: GrantPrivilegesStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_grant_privileges(
    scx: &StatementContext,
    GrantPrivilegesStatement {
        privileges,
        target,
        roles,
    }: GrantPrivilegesStatement<Aug>,
) -> Result<Plan, PlanError> {
    let plan = plan_update_privilege(scx, privileges, target, roles)?;
    Ok(Plan::GrantPrivileges(plan.into()))
}

pub fn describe_revoke_privileges(
    _: &StatementContext,
    _: RevokePrivilegesStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_revoke_privileges(
    scx: &StatementContext,
    RevokePrivilegesStatement {
        privileges,
        target,
        roles,
    }: RevokePrivilegesStatement<Aug>,
) -> Result<Plan, PlanError> {
    let plan = plan_update_privilege(scx, privileges, target, roles)?;
    Ok(Plan::RevokePrivileges(plan.into()))
}

struct UpdatePrivilegesPlan {
    update_privileges: Vec<UpdatePrivilege>,
    grantees: Vec<RoleId>,
}

impl From<UpdatePrivilegesPlan> for GrantPrivilegesPlan {
    fn from(
        UpdatePrivilegesPlan {
            update_privileges,
            grantees,
        }: UpdatePrivilegesPlan,
    ) -> GrantPrivilegesPlan {
        GrantPrivilegesPlan {
            update_privileges,
            grantees,
        }
    }
}

impl From<UpdatePrivilegesPlan> for RevokePrivilegesPlan {
    fn from(
        UpdatePrivilegesPlan {
            update_privileges,
            grantees,
        }: UpdatePrivilegesPlan,
    ) -> RevokePrivilegesPlan {
        RevokePrivilegesPlan {
            update_privileges,
            revokees: grantees,
        }
    }
}

fn plan_update_privilege(
    scx: &StatementContext,
    privileges: PrivilegeSpecification,
    target: GrantTargetSpecification<Aug>,
    roles: Vec<ResolvedRoleName>,
) -> Result<UpdatePrivilegesPlan, PlanError> {
    let (object_type, target_ids) = match target {
        GrantTargetSpecification::Object {
            object_type,
            object_spec_inner,
        } => {
            fn object_type_filter(
                object_id: &ObjectId,
                object_type: &ObjectType,
                scx: &StatementContext,
            ) -> bool {
                if object_type == &ObjectType::Table {
                    scx.get_object_type(object_id).is_relation()
                } else {
                    object_type == &scx.get_object_type(object_id)
                }
            }
            let object_type = object_type.into();
            let object_ids: Vec<ObjectId> = match object_spec_inner {
                GrantTargetSpecificationInner::All(GrantTargetAllSpecification::All) => {
                    let cluster_ids = scx
                        .catalog
                        .get_clusters()
                        .into_iter()
                        .map(|cluster| cluster.id().into());
                    let database_ids = scx
                        .catalog
                        .get_databases()
                        .into_iter()
                        .map(|database| database.id().into());
                    let schema_ids = scx
                        .catalog
                        .get_schemas()
                        .into_iter()
                        .filter(|schema| !schema.id().is_temporary())
                        .map(|schema| (schema.database().clone(), schema.id().clone()).into());
                    let item_ids = scx
                        .catalog
                        .get_items()
                        .into_iter()
                        .map(|item| item.id().into());
                    cluster_ids
                        .chain(database_ids)
                        .chain(schema_ids)
                        .chain(item_ids)
                        .filter(|object_id| object_type_filter(object_id, &object_type, scx))
                        .filter(|object_id| object_id.is_user())
                        .collect()
                }
                GrantTargetSpecificationInner::All(GrantTargetAllSpecification::AllDatabases {
                    databases,
                }) => {
                    let schema_ids = databases
                        .iter()
                        .map(|database| scx.get_database(database.database_id()))
                        .flat_map(|database| database.schemas().into_iter())
                        .filter(|schema| !schema.id().is_temporary())
                        .map(|schema| (schema.database().clone(), schema.id().clone()).into());

                    let item_ids = databases
                        .iter()
                        .map(|database| scx.get_database(database.database_id()))
                        .flat_map(|database| database.schemas().into_iter())
                        .flat_map(|schema| schema.item_ids().values())
                        .map(|item_id| (*item_id).into());

                    item_ids
                        .chain(schema_ids)
                        .filter(|object_id| object_type_filter(object_id, &object_type, scx))
                        .collect()
                }
                GrantTargetSpecificationInner::All(GrantTargetAllSpecification::AllSchemas {
                    schemas,
                }) => schemas
                    .into_iter()
                    .map(|schema| scx.get_schema(schema.database_spec(), schema.schema_spec()))
                    .flat_map(|schema| schema.item_ids().values())
                    .map(|item_id| (*item_id).into())
                    .filter(|object_id| object_type_filter(object_id, &object_type, scx))
                    .collect(),
                GrantTargetSpecificationInner::Objects { names } => names
                    .into_iter()
                    .map(|name| {
                        name.try_into()
                            .expect("name resolution should handle invalid objects")
                    })
                    .collect(),
            };
            let target_ids = object_ids.into_iter().map(|id| id.into()).collect();
            (SystemObjectType::Object(object_type), target_ids)
        }
        GrantTargetSpecification::System => {
            (SystemObjectType::System, vec![SystemObjectId::System])
        }
    };

    let mut update_privileges = Vec::with_capacity(target_ids.len());

    for target_id in target_ids {
        // The actual type of the object.
        let actual_object_type = scx.get_system_object_type(&target_id);
        // The type used for privileges, for example if the actual type is a view, the reference
        // type is table.
        let mut reference_object_type = actual_object_type.clone();

        let acl_mode = privilege_spec_to_acl_mode(scx, &privileges, actual_object_type);

        if let SystemObjectId::Object(ObjectId::Item(id)) = &target_id {
            let item = scx.get_item(id);
            let item_type: ObjectType = item.item_type().into();
            if (item_type == ObjectType::View
                || item_type == ObjectType::MaterializedView
                || item_type == ObjectType::Source)
                && object_type == SystemObjectType::Object(ObjectType::Table)
            {
                // This is an expected mis-match to match PostgreSQL semantics.
                reference_object_type = SystemObjectType::Object(ObjectType::Table);
            } else if SystemObjectType::Object(item_type) != object_type {
                let object_name = scx.catalog.resolve_full_name(item.name()).to_string();
                return Err(PlanError::InvalidObjectType {
                    expected_type: object_type,
                    actual_type: actual_object_type,
                    object_name,
                });
            }
        }

        let all_object_privileges = scx.catalog.all_object_privileges(reference_object_type);
        let invalid_privileges = acl_mode.difference(all_object_privileges);
        if !invalid_privileges.is_empty() {
            let object_name = Some(scx.catalog.get_system_object_name(&target_id));
            return Err(PlanError::InvalidPrivilegeTypes {
                invalid_privileges,
                object_type: actual_object_type,
                object_name,
            });
        }

        // In PostgreSQL, the grantor must always be either the object owner or some role that has been
        // been explicitly granted grant options. In Materialize, we haven't implemented grant options
        // so the grantor is always the object owner.
        //
        // For more details see:
        // https://github.com/postgres/postgres/blob/78d5952dd0e66afc4447eec07f770991fa406cce/src/backend/utils/adt/acl.c#L5154-L5246
        let grantor = match &target_id {
            SystemObjectId::Object(object_id) => scx
                .catalog
                .get_owner_id(object_id)
                .expect("cannot revoke privileges on objects without owners"),
            SystemObjectId::System => scx.catalog.mz_system_role_id(),
        };

        update_privileges.push(UpdatePrivilege {
            acl_mode,
            target_id,
            grantor,
        });
    }

    let grantees = roles.into_iter().map(|role| role.id).collect();

    Ok(UpdatePrivilegesPlan {
        update_privileges,
        grantees,
    })
}

fn privilege_spec_to_acl_mode(
    scx: &StatementContext,
    privilege_spec: &PrivilegeSpecification,
    object_type: SystemObjectType,
) -> AclMode {
    match privilege_spec {
        PrivilegeSpecification::All => scx.catalog.all_object_privileges(object_type),
        PrivilegeSpecification::Privileges(privileges) => privileges
            .into_iter()
            .map(|privilege| privilege_to_acl_mode(privilege.clone()))
            // PostgreSQL doesn't care about duplicate privileges, so we don't either.
            .fold(AclMode::empty(), |accum, acl_mode| accum.union(acl_mode)),
    }
}

fn privilege_to_acl_mode(privilege: Privilege) -> AclMode {
    match privilege {
        Privilege::SELECT => AclMode::SELECT,
        Privilege::INSERT => AclMode::INSERT,
        Privilege::UPDATE => AclMode::UPDATE,
        Privilege::DELETE => AclMode::DELETE,
        Privilege::USAGE => AclMode::USAGE,
        Privilege::CREATE => AclMode::CREATE,
        Privilege::CREATEROLE => AclMode::CREATE_ROLE,
        Privilege::CREATEDB => AclMode::CREATE_DB,
        Privilege::CREATECLUSTER => AclMode::CREATE_CLUSTER,
    }
}

pub fn describe_alter_default_privileges(
    _: &StatementContext,
    _: AlterDefaultPrivilegesStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_default_privileges(
    scx: &StatementContext,
    AlterDefaultPrivilegesStatement {
        target_roles,
        target_objects,
        grant_or_revoke,
    }: AlterDefaultPrivilegesStatement<Aug>,
) -> Result<Plan, PlanError> {
    let object_type: ObjectType = (*grant_or_revoke.object_type()).into();
    match object_type {
        ObjectType::View | ObjectType::MaterializedView | ObjectType::Source => sql_bail!(
            "{object_type}S is not valid for ALTER DEFAULT PRIVILEGES, use TABLES instead"
        ),
        ObjectType::Sink | ObjectType::ClusterReplica | ObjectType::Role | ObjectType::Func => {
            sql_bail!("{object_type}S do not have privileges")
        }
        ObjectType::Cluster | ObjectType::Database
            if matches!(
                target_objects,
                GrantTargetAllSpecification::AllDatabases { .. }
            ) =>
        {
            sql_bail!("cannot specify {object_type}S and IN DATABASE")
        }

        ObjectType::Cluster | ObjectType::Database | ObjectType::Schema
            if matches!(
                target_objects,
                GrantTargetAllSpecification::AllSchemas { .. }
            ) =>
        {
            sql_bail!("cannot specify {object_type}S and IN SCHEMA")
        }
        ObjectType::Table
        | ObjectType::Index
        | ObjectType::Type
        | ObjectType::Secret
        | ObjectType::Connection
        | ObjectType::Cluster
        | ObjectType::Database
        | ObjectType::Schema => {}
    }

    let acl_mode = privilege_spec_to_acl_mode(
        scx,
        grant_or_revoke.privileges(),
        SystemObjectType::Object(object_type),
    );
    let all_object_privileges = scx
        .catalog
        .all_object_privileges(SystemObjectType::Object(object_type));
    let invalid_privileges = acl_mode.difference(all_object_privileges);
    if !invalid_privileges.is_empty() {
        return Err(PlanError::InvalidPrivilegeTypes {
            invalid_privileges,
            object_type: SystemObjectType::Object(object_type),
            object_name: None,
        });
    }

    let target_roles = match target_roles {
        TargetRoleSpecification::Roles(roles) => roles.into_iter().map(|role| role.id).collect(),
        TargetRoleSpecification::CurrentRole => vec![*scx.catalog.active_role_id()],
        TargetRoleSpecification::AllRoles => vec![RoleId::Public],
    };
    let mut privilege_objects = Vec::with_capacity(target_roles.len() * target_objects.len());
    for target_role in target_roles {
        match &target_objects {
            GrantTargetAllSpecification::All => privilege_objects.push(DefaultPrivilegeObject {
                role_id: target_role,
                database_id: None,
                schema_id: None,
                object_type,
            }),
            GrantTargetAllSpecification::AllDatabases { databases } => {
                for database in databases {
                    privilege_objects.push(DefaultPrivilegeObject {
                        role_id: target_role,
                        database_id: Some(*database.database_id()),
                        schema_id: None,
                        object_type,
                    });
                }
            }
            GrantTargetAllSpecification::AllSchemas { schemas } => {
                for schema in schemas {
                    privilege_objects.push(DefaultPrivilegeObject {
                        role_id: target_role,
                        database_id: schema.database_spec().id(),
                        schema_id: Some(schema.schema_spec().into()),
                        object_type,
                    });
                }
            }
        }
    }

    let privilege_acl_items = grant_or_revoke
        .roles()
        .into_iter()
        .map(|grantee| DefaultPrivilegeAclItem {
            grantee: grantee.id,
            acl_mode,
        })
        .collect();

    let is_grant = match grant_or_revoke {
        AbbreviatedGrantOrRevokeStatement::Grant(_) => true,
        AbbreviatedGrantOrRevokeStatement::Revoke(_) => false,
    };

    Ok(Plan::AlterDefaultPrivileges(AlterDefaultPrivilegesPlan {
        privilege_objects,
        privilege_acl_items,
        is_grant,
    }))
}

pub fn describe_reassign_owned(
    _: &StatementContext,
    _: ReassignOwnedStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_reassign_owned(
    scx: &StatementContext,
    ReassignOwnedStatement {
        old_roles,
        new_role,
    }: ReassignOwnedStatement<Aug>,
) -> Result<Plan, PlanError> {
    let old_roles: BTreeSet<_> = old_roles.into_iter().map(|role| role.id).collect();
    let mut reassign_ids: Vec<ObjectId> = Vec::new();

    // Replicas
    for replica in scx.catalog.get_cluster_replicas() {
        if old_roles.contains(&replica.owner_id()) {
            reassign_ids.push((replica.cluster_id(), replica.replica_id()).into());
        }
    }
    // Clusters
    for cluster in scx.catalog.get_clusters() {
        if old_roles.contains(&cluster.owner_id()) {
            reassign_ids.push(cluster.id().into());
        }
    }
    // Items
    for item in scx.catalog.get_items() {
        if old_roles.contains(&item.owner_id()) {
            reassign_ids.push(item.id().into());
        }
    }
    // Schemas
    for schema in scx.catalog.get_schemas() {
        if !schema.id().is_temporary() {
            if old_roles.contains(&schema.owner_id()) {
                reassign_ids.push((*schema.database(), *schema.id()).into())
            }
        }
    }
    // Databases
    for database in scx.catalog.get_databases() {
        if old_roles.contains(&database.owner_id()) {
            reassign_ids.push(database.id().into());
        }
    }

    let system_ids: Vec<_> = reassign_ids.iter().filter(|id| id.is_system()).collect();
    if !system_ids.is_empty() {
        let mut owners = system_ids
            .into_iter()
            .filter_map(|object_id| scx.catalog.get_owner_id(object_id))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|role_id| scx.catalog.get_role(&role_id).name().quoted());
        sql_bail!(
            "cannot reassign objects owned by role {} because they are required by the database system",
            owners.join(", "),
        );
    }

    Ok(Plan::ReassignOwned(ReassignOwnedPlan {
        old_roles: old_roles.into_iter().collect(),
        new_role: new_role.id,
        reassign_ids,
    }))
}
