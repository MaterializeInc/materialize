// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use itertools::max;
use mz_audit_log::{EventV1, VersionedEvent};
use mz_controller::clusters::ReplicaLogging;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::now::EpochMillis;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;
use mz_sql::catalog::{
    DefaultPrivilegeAclItem, DefaultPrivilegeObject, ObjectType, RoleAttributes, RoleMembership,
    RoleVars, SystemObjectType,
};
use mz_sql::names::{
    DatabaseId, ObjectId, ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier, PUBLIC_ROLE_NAME,
};
use mz_sql::rbac;
use mz_sql::session::user::{MZ_SUPPORT_ROLE_ID, MZ_SYSTEM_ROLE_ID};
use mz_storage_types::sources::Timeline;

use crate::builtin::BUILTIN_ROLES;
use crate::durable::upgrade::CATALOG_VERSION;
use crate::durable::{
    BootstrapArgs, CatalogError, ClusterConfig, ClusterVariant, ClusterVariantManaged,
    DefaultPrivilege, ReplicaConfig, ReplicaLocation, Role, Schema, Transaction,
    AUDIT_LOG_ID_ALLOC_KEY, DATABASE_ID_ALLOC_KEY, SCHEMA_ID_ALLOC_KEY, STORAGE_USAGE_ID_ALLOC_KEY,
    SYSTEM_CLUSTER_ID_ALLOC_KEY, SYSTEM_REPLICA_ID_ALLOC_KEY, USER_CLUSTER_ID_ALLOC_KEY,
    USER_REPLICA_ID_ALLOC_KEY, USER_ROLE_ID_ALLOC_KEY,
};

/// The key used within the "config" collection stores the deploy generation.
pub(crate) const DEPLOY_GENERATION: &str = "deploy_generation";
/// The key within the "config" Collection that stores the version of the catalog.
pub const USER_VERSION_KEY: &str = "user_version";
/// The key within the "config" collection that stores whether the remote configuration was
/// synchronized at least once.
pub(crate) const SYSTEM_CONFIG_SYNCED_KEY: &str = "system_config_synced";

/// The key used within the "config" collection where we store a mirror of the
/// `persist_txn_tables` "system var" value. This is mirrored so that we
/// can toggle the flag with Launch Darkly, but use it in boot before Launch
/// Darkly is available.
pub(crate) const PERSIST_TXN_TABLES: &str = "persist_txn_tables";

/// The key within the "config" collection that stores whether the current durable store is actively
/// being used for the catalog. This is used to implement migrations/rollbacks to/from persist.
pub(crate) const TOMBSTONE_KEY: &str = "tombstone";

/// The key used within the "config" collection where we store a mirror of the
/// `catalog_kind` "system var" value. This is mirrored so that we
/// can toggle the flag with Launch Darkly, but use it in boot before Launch
/// Darkly is available.
pub const CATALOG_KIND_KEY: &str = "catalog_kind";

const USER_ID_ALLOC_KEY: &str = "user";
const SYSTEM_ID_ALLOC_KEY: &str = "system";

const DEFAULT_USER_CLUSTER_ID: ClusterId = ClusterId::User(1);
const DEFAULT_USER_CLUSTER_NAME: &str = "quickstart";

const DEFAULT_USER_REPLICA_ID: ReplicaId = ReplicaId::User(1);
const DEFAULT_USER_REPLICA_NAME: &str = "r1";

const MATERIALIZE_DATABASE_ID_VAL: u64 = 1;
const MATERIALIZE_DATABASE_ID: DatabaseId = DatabaseId::User(MATERIALIZE_DATABASE_ID_VAL);

const MZ_CATALOG_SCHEMA_ID: u64 = 1;
const PG_CATALOG_SCHEMA_ID: u64 = 2;
const PUBLIC_SCHEMA_ID: u64 = 3;
const MZ_INTERNAL_SCHEMA_ID: u64 = 4;
const INFORMATION_SCHEMA_ID: u64 = 5;
pub const MZ_UNSAFE_SCHEMA_ID: u64 = 6;

const DEFAULT_ALLOCATOR_ID: u64 = 1;

/// Initializes the Catalog with some default objects.
#[mz_ore::instrument]
pub async fn initialize(
    tx: &mut Transaction<'_>,
    options: &BootstrapArgs,
    initial_ts: EpochMillis,
    deploy_generation: Option<u64>,
) -> Result<(), CatalogError> {
    // Collect audit events so we can commit them once at the very end.
    let mut audit_events = vec![];

    // First thing we need to do is persist the timestamp we're booting with.
    tx.insert_timestamp(Timeline::EpochMilliseconds, initial_ts.into())?;

    for (name, next_id) in [
        (USER_ID_ALLOC_KEY.to_string(), DEFAULT_ALLOCATOR_ID),
        (SYSTEM_ID_ALLOC_KEY.to_string(), DEFAULT_ALLOCATOR_ID),
        (
            DATABASE_ID_ALLOC_KEY.to_string(),
            MATERIALIZE_DATABASE_ID_VAL + 1,
        ),
        (
            SCHEMA_ID_ALLOC_KEY.to_string(),
            max(&[
                MZ_CATALOG_SCHEMA_ID,
                PG_CATALOG_SCHEMA_ID,
                PUBLIC_SCHEMA_ID,
                MZ_INTERNAL_SCHEMA_ID,
                INFORMATION_SCHEMA_ID,
                MZ_UNSAFE_SCHEMA_ID,
            ])
            .expect("known to be non-empty")
                + 1,
        ),
        (USER_ROLE_ID_ALLOC_KEY.to_string(), DEFAULT_ALLOCATOR_ID),
        (
            USER_CLUSTER_ID_ALLOC_KEY.to_string(),
            DEFAULT_USER_CLUSTER_ID.inner_id() + 1,
        ),
        (
            SYSTEM_CLUSTER_ID_ALLOC_KEY.to_string(),
            DEFAULT_ALLOCATOR_ID,
        ),
        (
            USER_REPLICA_ID_ALLOC_KEY.to_string(),
            DEFAULT_USER_REPLICA_ID.inner_id() + 1,
        ),
        (
            SYSTEM_REPLICA_ID_ALLOC_KEY.to_string(),
            DEFAULT_ALLOCATOR_ID,
        ),
        (AUDIT_LOG_ID_ALLOC_KEY.to_string(), DEFAULT_ALLOCATOR_ID),
        (STORAGE_USAGE_ID_ALLOC_KEY.to_string(), DEFAULT_ALLOCATOR_ID),
    ] {
        tx.insert_id_allocator(name, next_id)?;
    }

    for role in BUILTIN_ROLES {
        tx.insert_role(
            role.id,
            role.name.to_string(),
            role.attributes.clone(),
            RoleMembership::new(),
            RoleVars::default(),
        )?;
    }
    tx.insert_role(
        RoleId::Public,
        PUBLIC_ROLE_NAME.as_str().to_lowercase(),
        RoleAttributes::new(),
        RoleMembership::new(),
        RoleVars::default(),
    )?;

    // If provided, generate a new Id for the bootstrap role.
    let bootstrap_role = if let Some(role) = &options.bootstrap_role {
        let attributes = RoleAttributes::new();
        let membership = RoleMembership::new();
        let vars = RoleVars::default();

        let id = tx.insert_user_role(
            role.to_string(),
            attributes.clone(),
            membership.clone(),
            vars.clone(),
        )?;

        audit_events.push((
            mz_audit_log::EventType::Create,
            mz_audit_log::ObjectType::Role,
            mz_audit_log::EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                id: id.to_string(),
                name: role.to_string(),
            }),
        ));

        Some(Role {
            id,
            name: role.to_string(),
            attributes,
            membership,
            vars,
        })
    } else {
        None
    };

    let default_privileges = [
        // mz_support needs USAGE privileges on all clusters, databases, and schemas for
        // debugging.
        DefaultPrivilege {
            object: DefaultPrivilegeObject {
                role_id: RoleId::Public,
                database_id: None,
                schema_id: None,
                object_type: mz_sql::catalog::ObjectType::Cluster,
            },
            acl_item: DefaultPrivilegeAclItem {
                grantee: MZ_SUPPORT_ROLE_ID,
                acl_mode: AclMode::USAGE,
            },
        },
        DefaultPrivilege {
            object: DefaultPrivilegeObject {
                role_id: RoleId::Public,
                database_id: None,
                schema_id: None,
                object_type: mz_sql::catalog::ObjectType::Database,
            },
            acl_item: DefaultPrivilegeAclItem {
                grantee: MZ_SUPPORT_ROLE_ID,
                acl_mode: AclMode::USAGE,
            },
        },
        DefaultPrivilege {
            object: DefaultPrivilegeObject {
                role_id: RoleId::Public,
                database_id: None,
                schema_id: None,
                object_type: mz_sql::catalog::ObjectType::Schema,
            },
            acl_item: DefaultPrivilegeAclItem {
                grantee: MZ_SUPPORT_ROLE_ID,
                acl_mode: AclMode::USAGE,
            },
        },
        DefaultPrivilege {
            object: DefaultPrivilegeObject {
                role_id: RoleId::Public,
                database_id: None,
                schema_id: None,
                object_type: mz_sql::catalog::ObjectType::Type,
            },
            acl_item: DefaultPrivilegeAclItem {
                grantee: RoleId::Public,
                acl_mode: AclMode::USAGE,
            },
        },
    ];
    tx.set_default_privileges(default_privileges.to_vec())?;
    for DefaultPrivilege { object, acl_item } in default_privileges {
        let object_type = match object.object_type {
            ObjectType::Table => mz_audit_log::ObjectType::Table,
            ObjectType::View => mz_audit_log::ObjectType::View,
            ObjectType::MaterializedView => mz_audit_log::ObjectType::MaterializedView,
            ObjectType::Source => mz_audit_log::ObjectType::Source,
            ObjectType::Sink => mz_audit_log::ObjectType::Sink,
            ObjectType::Index => mz_audit_log::ObjectType::Index,
            ObjectType::Type => mz_audit_log::ObjectType::Type,
            ObjectType::Role => mz_audit_log::ObjectType::Role,
            ObjectType::Cluster => mz_audit_log::ObjectType::Cluster,
            ObjectType::ClusterReplica => mz_audit_log::ObjectType::ClusterReplica,
            ObjectType::Secret => mz_audit_log::ObjectType::Secret,
            ObjectType::Connection => mz_audit_log::ObjectType::Connection,
            ObjectType::Database => mz_audit_log::ObjectType::Database,
            ObjectType::Schema => mz_audit_log::ObjectType::Schema,
            ObjectType::Func => mz_audit_log::ObjectType::Func,
        };
        audit_events.push((
            mz_audit_log::EventType::Grant,
            object_type,
            mz_audit_log::EventDetails::AlterDefaultPrivilegeV1(
                mz_audit_log::AlterDefaultPrivilegeV1 {
                    role_id: object.role_id.to_string(),
                    database_id: object.database_id.map(|id| id.to_string()),
                    schema_id: object.schema_id.map(|id| id.to_string()),
                    grantee_id: acl_item.grantee.to_string(),
                    privileges: acl_item.acl_mode.to_string(),
                },
            ),
        ));
    }

    let mut db_privileges = vec![
        MzAclItem {
            grantee: RoleId::Public,
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: AclMode::USAGE,
        },
        MzAclItem {
            grantee: MZ_SUPPORT_ROLE_ID,
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: AclMode::USAGE,
        },
        rbac::owner_privilege(mz_sql::catalog::ObjectType::Database, MZ_SYSTEM_ROLE_ID),
    ];
    // Optionally add a privilege for the bootstrap role.
    if let Some(role) = &bootstrap_role {
        db_privileges.push(MzAclItem {
            grantee: role.id.clone(),
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: rbac::all_object_privileges(SystemObjectType::Object(
                mz_sql::catalog::ObjectType::Database,
            )),
        })
    };

    tx.insert_database(
        MATERIALIZE_DATABASE_ID,
        "materialize",
        MZ_SYSTEM_ROLE_ID,
        db_privileges,
    )?;
    audit_events.extend([
        (
            mz_audit_log::EventType::Create,
            mz_audit_log::ObjectType::Database,
            mz_audit_log::EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                id: MATERIALIZE_DATABASE_ID.to_string(),
                name: "materialize".to_string(),
            }),
        ),
        (
            mz_audit_log::EventType::Grant,
            mz_audit_log::ObjectType::Database,
            mz_audit_log::EventDetails::UpdatePrivilegeV1(mz_audit_log::UpdatePrivilegeV1 {
                object_id: ObjectId::Database(MATERIALIZE_DATABASE_ID).to_string(),
                grantee_id: RoleId::Public.to_string(),
                grantor_id: MZ_SYSTEM_ROLE_ID.to_string(),
                privileges: AclMode::USAGE.to_string(),
            }),
        ),
    ]);
    // Optionally add a privilege for the bootstrap role.
    if let Some(role) = &bootstrap_role {
        let role_id: RoleId = role.id.clone();
        audit_events.push((
            mz_audit_log::EventType::Grant,
            mz_audit_log::ObjectType::Database,
            mz_audit_log::EventDetails::UpdatePrivilegeV1(mz_audit_log::UpdatePrivilegeV1 {
                object_id: ObjectId::Database(MATERIALIZE_DATABASE_ID).to_string(),
                grantee_id: role_id.to_string(),
                grantor_id: MZ_SYSTEM_ROLE_ID.to_string(),
                privileges: rbac::all_object_privileges(SystemObjectType::Object(
                    mz_sql::catalog::ObjectType::Database,
                ))
                .to_string(),
            }),
        ));
    }

    let schema_privileges = vec![
        rbac::default_builtin_object_privilege(mz_sql::catalog::ObjectType::Schema),
        MzAclItem {
            grantee: MZ_SUPPORT_ROLE_ID,
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: AclMode::USAGE,
        },
        rbac::owner_privilege(mz_sql::catalog::ObjectType::Schema, MZ_SYSTEM_ROLE_ID),
    ];

    let mz_catalog_schema = Schema {
        id: SchemaId::System(MZ_CATALOG_SCHEMA_ID),
        database_id: None,
        name: "mz_catalog".to_string(),
        owner_id: MZ_SYSTEM_ROLE_ID,
        privileges: schema_privileges.clone(),
    };
    let pg_catalog_schema = Schema {
        id: SchemaId::System(PG_CATALOG_SCHEMA_ID),
        database_id: None,
        name: "pg_catalog".to_string(),
        owner_id: MZ_SYSTEM_ROLE_ID,
        privileges: schema_privileges.clone(),
    };
    let mz_internal_schema = Schema {
        id: SchemaId::System(MZ_INTERNAL_SCHEMA_ID),
        database_id: None,
        name: "mz_internal".to_string(),
        owner_id: MZ_SYSTEM_ROLE_ID,
        privileges: schema_privileges.clone(),
    };
    let information_schema = Schema {
        id: SchemaId::System(INFORMATION_SCHEMA_ID),
        database_id: None,
        name: "information_schema".to_string(),
        owner_id: MZ_SYSTEM_ROLE_ID,
        privileges: schema_privileges.clone(),
    };
    let mz_unsafe_schema = Schema {
        id: SchemaId::System(MZ_UNSAFE_SCHEMA_ID),
        database_id: None,
        name: "mz_unsafe".to_string(),
        owner_id: MZ_SYSTEM_ROLE_ID,
        privileges: schema_privileges.clone(),
    };
    let public_schema = Schema {
        id: SchemaId::User(PUBLIC_SCHEMA_ID),
        database_id: Some(MATERIALIZE_DATABASE_ID),
        name: "public".to_string(),
        owner_id: MZ_SYSTEM_ROLE_ID,
        privileges: vec![
            MzAclItem {
                grantee: RoleId::Public,
                grantor: MZ_SYSTEM_ROLE_ID,
                acl_mode: AclMode::USAGE,
            },
            MzAclItem {
                grantee: MZ_SUPPORT_ROLE_ID,
                grantor: MZ_SYSTEM_ROLE_ID,
                acl_mode: AclMode::USAGE,
            },
            rbac::owner_privilege(mz_sql::catalog::ObjectType::Schema, MZ_SYSTEM_ROLE_ID),
        ]
        .into_iter()
        // Optionally add the bootstrap role to the public schema.
        .chain(bootstrap_role.as_ref().map(|role| MzAclItem {
            grantee: role.id.clone(),
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: rbac::all_object_privileges(SystemObjectType::Object(
                mz_sql::catalog::ObjectType::Schema,
            )),
        }))
        .collect(),
    };

    for schema in [
        mz_catalog_schema,
        pg_catalog_schema,
        public_schema,
        mz_internal_schema,
        information_schema,
        mz_unsafe_schema,
    ] {
        tx.insert_schema(
            schema.id,
            schema.database_id,
            schema.name,
            schema.owner_id,
            schema.privileges,
        )?;
    }
    audit_events.push((
        mz_audit_log::EventType::Create,
        mz_audit_log::ObjectType::Schema,
        mz_audit_log::EventDetails::SchemaV2(mz_audit_log::SchemaV2 {
            id: PUBLIC_SCHEMA_ID.to_string(),
            name: "public".to_string(),
            database_name: Some("materialize".to_string()),
        }),
    ));
    if let Some(role) = &bootstrap_role {
        let role_id: RoleId = role.id.clone();
        audit_events.push((
            mz_audit_log::EventType::Grant,
            mz_audit_log::ObjectType::Schema,
            mz_audit_log::EventDetails::UpdatePrivilegeV1(mz_audit_log::UpdatePrivilegeV1 {
                object_id: ObjectId::Schema((
                    ResolvedDatabaseSpecifier::Id(MATERIALIZE_DATABASE_ID),
                    SchemaSpecifier::Id(SchemaId::User(PUBLIC_SCHEMA_ID)),
                ))
                .to_string(),
                grantee_id: role_id.to_string(),
                grantor_id: MZ_SYSTEM_ROLE_ID.to_string(),
                privileges: rbac::all_object_privileges(SystemObjectType::Object(
                    mz_sql::catalog::ObjectType::Schema,
                ))
                .to_string(),
            }),
        ));
    }

    let mut cluster_privileges = vec![
        MzAclItem {
            grantee: RoleId::Public,
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: AclMode::USAGE,
        },
        MzAclItem {
            grantee: MZ_SUPPORT_ROLE_ID,
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: AclMode::USAGE,
        },
        rbac::owner_privilege(mz_sql::catalog::ObjectType::Cluster, MZ_SYSTEM_ROLE_ID),
    ];

    // Optionally add a privilege for the bootstrap role.
    if let Some(role) = &bootstrap_role {
        cluster_privileges.push(MzAclItem {
            grantee: role.id.clone(),
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: rbac::all_object_privileges(SystemObjectType::Object(
                mz_sql::catalog::ObjectType::Cluster,
            )),
        });
    };

    tx.insert_user_cluster(
        DEFAULT_USER_CLUSTER_ID,
        DEFAULT_USER_CLUSTER_NAME,
        Vec::new(),
        MZ_SYSTEM_ROLE_ID,
        cluster_privileges,
        default_cluster_config(options),
    )?;
    audit_events.extend([
        (
            mz_audit_log::EventType::Create,
            mz_audit_log::ObjectType::Cluster,
            mz_audit_log::EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                id: DEFAULT_USER_CLUSTER_ID.to_string(),
                name: DEFAULT_USER_CLUSTER_NAME.to_string(),
            }),
        ),
        (
            mz_audit_log::EventType::Grant,
            mz_audit_log::ObjectType::Cluster,
            mz_audit_log::EventDetails::UpdatePrivilegeV1(mz_audit_log::UpdatePrivilegeV1 {
                object_id: ObjectId::Cluster(DEFAULT_USER_CLUSTER_ID).to_string(),
                grantee_id: RoleId::Public.to_string(),
                grantor_id: MZ_SYSTEM_ROLE_ID.to_string(),
                privileges: AclMode::USAGE.to_string(),
            }),
        ),
    ]);
    // Optionally add a privilege for the bootstrap role.
    if let Some(role) = &bootstrap_role {
        let role_id: RoleId = role.id.clone();
        audit_events.push((
            mz_audit_log::EventType::Grant,
            mz_audit_log::ObjectType::Cluster,
            mz_audit_log::EventDetails::UpdatePrivilegeV1(mz_audit_log::UpdatePrivilegeV1 {
                object_id: ObjectId::Cluster(DEFAULT_USER_CLUSTER_ID).to_string(),
                grantee_id: role_id.to_string(),
                grantor_id: MZ_SYSTEM_ROLE_ID.to_string(),
                privileges: rbac::all_object_privileges(SystemObjectType::Object(
                    mz_sql::catalog::ObjectType::Cluster,
                ))
                .to_string(),
            }),
        ));
    }

    tx.insert_cluster_replica(
        DEFAULT_USER_CLUSTER_ID,
        DEFAULT_USER_REPLICA_ID,
        DEFAULT_USER_REPLICA_NAME,
        default_replica_config(options),
        MZ_SYSTEM_ROLE_ID,
    )?;
    audit_events.push((
        mz_audit_log::EventType::Create,
        mz_audit_log::ObjectType::ClusterReplica,
        mz_audit_log::EventDetails::CreateClusterReplicaV1(mz_audit_log::CreateClusterReplicaV1 {
            cluster_id: DEFAULT_USER_CLUSTER_ID.to_string(),
            cluster_name: DEFAULT_USER_CLUSTER_NAME.to_string(),
            replica_name: DEFAULT_USER_REPLICA_NAME.to_string(),
            replica_id: Some(DEFAULT_USER_REPLICA_ID.to_string()),
            logical_size: options.default_cluster_replica_size.to_string(),
            disk: false,
            billed_as: None,
            internal: false,
        }),
    ));

    let system_privileges = [MzAclItem {
        grantee: MZ_SYSTEM_ROLE_ID,
        grantor: MZ_SYSTEM_ROLE_ID,
        acl_mode: rbac::all_object_privileges(SystemObjectType::System),
    }]
    .into_iter()
    // Optionally add system privileges for the bootstrap role.
    .chain(bootstrap_role.as_ref().map(|role| MzAclItem {
        grantee: role.id.clone(),
        grantor: MZ_SYSTEM_ROLE_ID,
        acl_mode: rbac::all_object_privileges(SystemObjectType::System),
    }));
    tx.set_system_privileges(system_privileges.clone().collect())?;
    for system_privilege in system_privileges {
        audit_events.push((
            mz_audit_log::EventType::Grant,
            mz_audit_log::ObjectType::System,
            mz_audit_log::EventDetails::UpdatePrivilegeV1(mz_audit_log::UpdatePrivilegeV1 {
                object_id: "SYSTEM".to_string(),
                grantee_id: system_privilege.grantee.to_string(),
                grantor_id: system_privilege.grantor.to_string(),
                privileges: system_privilege.acl_mode.to_string(),
            }),
        ));
    }

    // Allocate an ID for each audit log event.
    let mut audit_events_with_id = Vec::with_capacity(audit_events.len());
    for (ty, obj, details) in audit_events {
        let id = tx.get_and_increment_id(AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
        audit_events_with_id.push((id, ty, obj, details));
    }

    for (id, event_type, object_type, details) in audit_events_with_id {
        tx.insert_audit_log_event(VersionedEvent::V1(EventV1 {
            id,
            event_type,
            object_type,
            details,
            user: None,
            occurred_at: initial_ts,
        }));
    }

    for (key, value) in [
        (USER_VERSION_KEY.to_string(), CATALOG_VERSION),
        (
            DEPLOY_GENERATION.to_string(),
            deploy_generation.unwrap_or(0),
        ),
        (SYSTEM_CONFIG_SYNCED_KEY.to_string(), 0),
    ] {
        tx.insert_config(key, value)?;
    }

    Ok(())
}

/// Defines the default config for a Cluster.
fn default_cluster_config(args: &BootstrapArgs) -> ClusterConfig {
    ClusterConfig {
        variant: ClusterVariant::Managed(ClusterVariantManaged {
            size: args.default_cluster_replica_size.to_string(),
            replication_factor: 1,
            availability_zones: vec![],
            logging: ReplicaLogging {
                log_logging: false,
                interval: Some(Duration::from_secs(1)),
            },
            idle_arrangement_merge_effort: None,
            disk: false,
            optimizer_feature_overrides: Default::default(),
        }),
    }
}

/// Defines the default config for a Cluster Replica.
fn default_replica_config(args: &BootstrapArgs) -> ReplicaConfig {
    ReplicaConfig {
        location: ReplicaLocation::Managed {
            size: args.default_cluster_replica_size.to_string(),
            availability_zone: None,
            disk: false,
            internal: false,
            billed_as: None,
        },
        logging: ReplicaLogging {
            log_logging: false,
            interval: Some(Duration::from_secs(1)),
        },
        idle_arrangement_merge_effort: None,
    }
}
