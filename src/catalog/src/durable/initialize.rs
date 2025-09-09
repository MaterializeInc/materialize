// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::iter;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;

use ipnet::IpNet;
use itertools::max;
use mz_audit_log::{CreateOrDropClusterReplicaReasonV1, EventV1, VersionedEvent};
use mz_controller::clusters::ReplicaLogging;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::collections::HashSet;
use mz_ore::now::EpochMillis;
use mz_persist_types::ShardId;
use mz_pgrepr::oid::{
    FIRST_USER_OID, NETWORK_POLICIES_DEFAULT_POLICY_OID, ROLE_PUBLIC_OID,
    SCHEMA_INFORMATION_SCHEMA_OID, SCHEMA_MZ_CATALOG_OID, SCHEMA_MZ_CATALOG_UNSTABLE_OID,
    SCHEMA_MZ_INTERNAL_OID, SCHEMA_MZ_INTROSPECTION_OID, SCHEMA_MZ_UNSAFE_OID,
    SCHEMA_PG_CATALOG_OID,
};
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::role_id::RoleId;
use mz_sql::catalog::{
    DefaultPrivilegeAclItem, DefaultPrivilegeObject, ObjectType, RoleAttributes, RoleMembership,
    RoleVars, SystemObjectType,
};
use mz_sql::names::{
    DatabaseId, ObjectId, PUBLIC_ROLE_NAME, ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier,
};
use mz_sql::plan::{NetworkPolicyRule, PolicyAddress};
use mz_sql::rbac;
use mz_sql::session::user::{MZ_SUPPORT_ROLE_ID, MZ_SYSTEM_ROLE_ID};

use crate::builtin::BUILTIN_ROLES;
use crate::durable::upgrade::CATALOG_VERSION;
use crate::durable::{
    AUDIT_LOG_ID_ALLOC_KEY, BUILTIN_MIGRATION_SHARD_KEY, BootstrapArgs,
    CATALOG_CONTENT_VERSION_KEY, CatalogError, ClusterConfig, ClusterVariant,
    ClusterVariantManaged, DATABASE_ID_ALLOC_KEY, DefaultPrivilege, EXPRESSION_CACHE_SHARD_KEY,
    OID_ALLOC_KEY, ReplicaConfig, ReplicaLocation, Role, SCHEMA_ID_ALLOC_KEY,
    STORAGE_USAGE_ID_ALLOC_KEY, SYSTEM_CLUSTER_ID_ALLOC_KEY, SYSTEM_REPLICA_ID_ALLOC_KEY, Schema,
    Transaction, USER_CLUSTER_ID_ALLOC_KEY, USER_NETWORK_POLICY_ID_ALLOC_KEY,
    USER_REPLICA_ID_ALLOC_KEY, USER_ROLE_ID_ALLOC_KEY,
};

/// The key within the "config" Collection that stores the version of the catalog.
pub const USER_VERSION_KEY: &str = "user_version";
/// The key within the "config" collection that stores whether the remote configuration was
/// synchronized at least once.
pub(crate) const SYSTEM_CONFIG_SYNCED_KEY: &str = "system_config_synced";

/// The key used within the "config" collection where we store a mirror of the
/// `with_0dt_deployment_max_wait` "system var" value. This is mirrored so that
/// we can toggle the flag with LaunchDarkly, but use it in boot before
/// LaunchDarkly is available.
///
/// NOTE: Weird prefix because we can't start with a `0`.
pub(crate) const WITH_0DT_DEPLOYMENT_MAX_WAIT: &str = "with_0dt_deployment_max_wait";

/// The key used within the "config" collection where we store a mirror of the
/// `with_0dt_deployment_ddl_check_interval` "system var" value. This is
/// mirrored so that we can toggle the flag with LaunchDarkly, but use it in
/// boot before LaunchDarkly is available.
///
/// NOTE: Weird prefix because we can't start with a `0`.
pub(crate) const WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL: &str =
    "with_0dt_deployment_ddl_check_interval";

/// The key used within the "config" collection where we store a mirror of the
/// `enable_0dt_deployment_panic_after_timeout` "system var" value. This is
/// mirrored so that we can toggle the flag with LaunchDarkly, but use it in
/// boot before LaunchDarkly is available.
pub(crate) const ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT: &str =
    "enable_0dt_deployment_panic_after_timeout";

const USER_ID_ALLOC_KEY: &str = "user";
const SYSTEM_ID_ALLOC_KEY: &str = "system";

const DEFAULT_USER_CLUSTER_ID: ClusterId = ClusterId::User(1);
const DEFAULT_USER_CLUSTER_NAME: &str = "quickstart";

const DEFAULT_USER_REPLICA_ID: ReplicaId = ReplicaId::User(1);

const MATERIALIZE_DATABASE_ID_VAL: u64 = 1;
const MATERIALIZE_DATABASE_ID: DatabaseId = DatabaseId::User(MATERIALIZE_DATABASE_ID_VAL);

const MZ_CATALOG_SCHEMA_ID: u64 = 1;
const PG_CATALOG_SCHEMA_ID: u64 = 2;
const PUBLIC_SCHEMA_ID: u64 = 3;
const MZ_INTERNAL_SCHEMA_ID: u64 = 4;
const INFORMATION_SCHEMA_ID: u64 = 5;
pub const MZ_UNSAFE_SCHEMA_ID: u64 = 6;
pub const MZ_CATALOG_UNSTABLE_SCHEMA_ID: u64 = 7;
pub const MZ_INTROSPECTION_SCHEMA_ID: u64 = 8;

const DEFAULT_ALLOCATOR_ID: u64 = 1;

pub const DEFAULT_USER_NETWORK_POLICY_ID: NetworkPolicyId = NetworkPolicyId::User(1);
pub const DEFAULT_USER_NETWORK_POLICY_NAME: &str = "default";
pub const DEFAULT_USER_NETWORK_POLICY_RULES: &[(
    &str,
    mz_sql::plan::NetworkPolicyRuleAction,
    mz_sql::plan::NetworkPolicyRuleDirection,
    &str,
)] = &[(
    "open_ingress",
    mz_sql::plan::NetworkPolicyRuleAction::Allow,
    mz_sql::plan::NetworkPolicyRuleDirection::Ingress,
    "0.0.0.0/0",
)];

static DEFAULT_USER_NETWORK_POLICY_PRIVILEGES: LazyLock<Vec<MzAclItem>> = LazyLock::new(|| {
    vec![rbac::owner_privilege(
        ObjectType::NetworkPolicy,
        MZ_SYSTEM_ROLE_ID,
    )]
});

static SYSTEM_SCHEMA_PRIVILEGES: LazyLock<Vec<MzAclItem>> = LazyLock::new(|| {
    vec![
        rbac::default_builtin_object_privilege(mz_sql::catalog::ObjectType::Schema),
        MzAclItem {
            grantee: MZ_SUPPORT_ROLE_ID,
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: AclMode::USAGE,
        },
        rbac::owner_privilege(mz_sql::catalog::ObjectType::Schema, MZ_SYSTEM_ROLE_ID),
    ]
});

static MZ_CATALOG_SCHEMA: LazyLock<Schema> = LazyLock::new(|| Schema {
    id: SchemaId::System(MZ_CATALOG_SCHEMA_ID),
    oid: SCHEMA_MZ_CATALOG_OID,
    database_id: None,
    name: "mz_catalog".to_string(),
    owner_id: MZ_SYSTEM_ROLE_ID,
    privileges: SYSTEM_SCHEMA_PRIVILEGES.clone(),
});
static PG_CATALOG_SCHEMA: LazyLock<Schema> = LazyLock::new(|| Schema {
    id: SchemaId::System(PG_CATALOG_SCHEMA_ID),
    oid: SCHEMA_PG_CATALOG_OID,
    database_id: None,
    name: "pg_catalog".to_string(),
    owner_id: MZ_SYSTEM_ROLE_ID,
    privileges: SYSTEM_SCHEMA_PRIVILEGES.clone(),
});
static MZ_INTERNAL_SCHEMA: LazyLock<Schema> = LazyLock::new(|| Schema {
    id: SchemaId::System(MZ_INTERNAL_SCHEMA_ID),
    oid: SCHEMA_MZ_INTERNAL_OID,
    database_id: None,
    name: "mz_internal".to_string(),
    owner_id: MZ_SYSTEM_ROLE_ID,
    privileges: SYSTEM_SCHEMA_PRIVILEGES.clone(),
});
static INFORMATION_SCHEMA: LazyLock<Schema> = LazyLock::new(|| Schema {
    id: SchemaId::System(INFORMATION_SCHEMA_ID),
    oid: SCHEMA_INFORMATION_SCHEMA_OID,
    database_id: None,
    name: "information_schema".to_string(),
    owner_id: MZ_SYSTEM_ROLE_ID,
    privileges: SYSTEM_SCHEMA_PRIVILEGES.clone(),
});
static MZ_UNSAFE_SCHEMA: LazyLock<Schema> = LazyLock::new(|| Schema {
    id: SchemaId::System(MZ_UNSAFE_SCHEMA_ID),
    oid: SCHEMA_MZ_UNSAFE_OID,
    database_id: None,
    name: "mz_unsafe".to_string(),
    owner_id: MZ_SYSTEM_ROLE_ID,
    privileges: SYSTEM_SCHEMA_PRIVILEGES.clone(),
});
static MZ_CATALOG_UNSTABLE_SCHEMA: LazyLock<Schema> = LazyLock::new(|| Schema {
    id: SchemaId::System(MZ_CATALOG_UNSTABLE_SCHEMA_ID),
    oid: SCHEMA_MZ_CATALOG_UNSTABLE_OID,
    database_id: None,
    name: "mz_catalog_unstable".to_string(),
    owner_id: MZ_SYSTEM_ROLE_ID,
    privileges: SYSTEM_SCHEMA_PRIVILEGES.clone(),
});
static MZ_INTROSPECTION_SCHEMA: LazyLock<Schema> = LazyLock::new(|| Schema {
    id: SchemaId::System(MZ_INTROSPECTION_SCHEMA_ID),
    oid: SCHEMA_MZ_INTROSPECTION_OID,
    database_id: None,
    name: "mz_introspection".to_string(),
    owner_id: MZ_SYSTEM_ROLE_ID,
    privileges: SYSTEM_SCHEMA_PRIVILEGES.clone(),
});
static SYSTEM_SCHEMAS: LazyLock<BTreeMap<&str, &Schema>> = LazyLock::new(|| {
    [
        &*MZ_CATALOG_SCHEMA,
        &*PG_CATALOG_SCHEMA,
        &*MZ_INTERNAL_SCHEMA,
        &*INFORMATION_SCHEMA,
        &*MZ_UNSAFE_SCHEMA,
        &*MZ_CATALOG_UNSTABLE_SCHEMA,
        &*MZ_INTROSPECTION_SCHEMA,
    ]
    .into_iter()
    .map(|s| (&*s.name, s))
    .collect()
});

/// Initializes the Catalog with some default objects.
#[mz_ore::instrument]
pub(crate) async fn initialize(
    tx: &mut Transaction<'_>,
    options: &BootstrapArgs,
    initial_ts: EpochMillis,
    catalog_content_version: String,
) -> Result<(), CatalogError> {
    // Collect audit events so we can commit them once at the very end.
    let mut audit_events = vec![];

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
                MZ_CATALOG_UNSTABLE_SCHEMA_ID,
                MZ_INTROSPECTION_SCHEMA_ID,
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
            DEFAULT_USER_REPLICA_ID.inner_id()
                + u64::from(options.default_cluster_replication_factor),
        ),
        (
            SYSTEM_REPLICA_ID_ALLOC_KEY.to_string(),
            DEFAULT_ALLOCATOR_ID,
        ),
        (
            USER_NETWORK_POLICY_ID_ALLOC_KEY.to_string(),
            DEFAULT_ALLOCATOR_ID,
        ),
        (AUDIT_LOG_ID_ALLOC_KEY.to_string(), DEFAULT_ALLOCATOR_ID),
        (STORAGE_USAGE_ID_ALLOC_KEY.to_string(), DEFAULT_ALLOCATOR_ID),
        (OID_ALLOC_KEY.to_string(), FIRST_USER_OID.into()),
    ] {
        tx.insert_id_allocator(name, next_id)?;
    }

    for role in BUILTIN_ROLES {
        tx.insert_builtin_role(
            role.id,
            role.name.to_string(),
            role.attributes.clone(),
            RoleMembership::new(),
            RoleVars::default(),
            role.oid,
        )?;
    }
    tx.insert_builtin_role(
        RoleId::Public,
        PUBLIC_ROLE_NAME.as_str().to_lowercase(),
        RoleAttributes::new(),
        RoleMembership::new(),
        RoleVars::default(),
        ROLE_PUBLIC_OID,
    )?;

    // If provided, generate a new Id for the bootstrap role.
    let bootstrap_role = if let Some(role) = &options.bootstrap_role {
        let attributes = RoleAttributes::new();
        let membership = RoleMembership::new();
        let vars = RoleVars::default();

        let (id, oid) = tx.insert_user_role(
            role.to_string(),
            attributes.clone(),
            membership.clone(),
            vars.clone(),
            &HashSet::new(),
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
            oid,
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
            ObjectType::ContinualTask => mz_audit_log::ObjectType::ContinualTask,
            ObjectType::NetworkPolicy => mz_audit_log::ObjectType::NetworkPolicy,
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

    let materialize_db_oid = tx.allocate_oid(&HashSet::new())?;
    tx.insert_database(
        MATERIALIZE_DATABASE_ID,
        "materialize",
        MZ_SYSTEM_ROLE_ID,
        db_privileges,
        materialize_db_oid,
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

    let public_schema_oid = tx.allocate_oid(&HashSet::new())?;
    let public_schema = Schema {
        id: SchemaId::User(PUBLIC_SCHEMA_ID),
        oid: public_schema_oid,
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

    for schema in SYSTEM_SCHEMAS.values().chain(iter::once(&&public_schema)) {
        tx.insert_schema(
            schema.id,
            schema.database_id,
            schema.name.clone(),
            schema.owner_id,
            schema.privileges.clone(),
            schema.oid,
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

    tx.insert_network_policy(
        DEFAULT_USER_NETWORK_POLICY_ID,
        DEFAULT_USER_NETWORK_POLICY_NAME.to_string(),
        DEFAULT_USER_NETWORK_POLICY_RULES
            .into_iter()
            .map(|(name, action, direction, ip_str)| NetworkPolicyRule {
                name: name.to_string(),
                action: action.clone(),
                direction: direction.clone(),
                address: PolicyAddress(
                    IpNet::from_str(ip_str).expect("default policy must provide valid ip"),
                ),
            })
            .collect::<Vec<NetworkPolicyRule>>(),
        DEFAULT_USER_NETWORK_POLICY_PRIVILEGES.clone(),
        MZ_SYSTEM_ROLE_ID,
        NETWORK_POLICIES_DEFAULT_POLICY_OID,
    )?;
    // We created a network policy with a prefined ID user(1) and OID. We need
    // to increment the id alloc key. It should be safe to assume that there's
    // no user(1), as a sanity check, we'll assert this is the case.
    let id = tx.get_and_increment_id(USER_NETWORK_POLICY_ID_ALLOC_KEY.to_string())?;
    assert!(DEFAULT_USER_NETWORK_POLICY_ID == NetworkPolicyId::User(id));

    audit_events.extend([(
        mz_audit_log::EventType::Create,
        mz_audit_log::ObjectType::NetworkPolicy,
        mz_audit_log::EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
            id: DEFAULT_USER_NETWORK_POLICY_ID.to_string(),
            name: DEFAULT_USER_NETWORK_POLICY_NAME.to_string(),
        }),
    )]);

    tx.insert_user_cluster(
        DEFAULT_USER_CLUSTER_ID,
        DEFAULT_USER_CLUSTER_NAME,
        Vec::new(),
        MZ_SYSTEM_ROLE_ID,
        cluster_privileges,
        default_cluster_config(options)?,
        &HashSet::new(),
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

    for i in 0..options.default_cluster_replication_factor {
        let replica_id = ReplicaId::User(DEFAULT_USER_REPLICA_ID.inner_id() + u64::from(i));
        let replica_name = format!("r{}", i + 1);
        tx.insert_cluster_replica_with_id(
            DEFAULT_USER_CLUSTER_ID,
            replica_id,
            &replica_name,
            default_replica_config(options)?,
            MZ_SYSTEM_ROLE_ID,
        )?;
        audit_events.push((
            mz_audit_log::EventType::Create,
            mz_audit_log::ObjectType::ClusterReplica,
            mz_audit_log::EventDetails::CreateClusterReplicaV4(
                mz_audit_log::CreateClusterReplicaV4 {
                    cluster_id: DEFAULT_USER_CLUSTER_ID.to_string(),
                    cluster_name: DEFAULT_USER_CLUSTER_NAME.to_string(),
                    replica_name,
                    replica_id: Some(replica_id.to_string()),
                    logical_size: options.default_cluster_replica_size.to_string(),
                    billed_as: None,
                    internal: false,
                    reason: CreateOrDropClusterReplicaReasonV1::System,
                    scheduling_policies: None,
                },
            ),
        ));
    }

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
        (SYSTEM_CONFIG_SYNCED_KEY.to_string(), 0),
    ] {
        tx.insert_config(key, value)?;
    }

    for (name, value) in [
        (
            CATALOG_CONTENT_VERSION_KEY.to_string(),
            catalog_content_version,
        ),
        (
            BUILTIN_MIGRATION_SHARD_KEY.to_string(),
            ShardId::new().to_string(),
        ),
        (
            EXPRESSION_CACHE_SHARD_KEY.to_string(),
            ShardId::new().to_string(),
        ),
    ] {
        tx.set_setting(name, Some(value))?;
    }

    Ok(())
}

pub fn resolve_system_schema(name: &str) -> &Schema {
    SYSTEM_SCHEMAS
        .get(name)
        .unwrap_or_else(|| panic!("unable to resolve system schema: {name}"))
}

/// Defines the default config for a Cluster.
fn default_cluster_config(args: &BootstrapArgs) -> Result<ClusterConfig, CatalogError> {
    Ok(ClusterConfig {
        variant: ClusterVariant::Managed(ClusterVariantManaged {
            size: args.default_cluster_replica_size.to_string(),
            replication_factor: args.default_cluster_replication_factor,
            availability_zones: vec![],
            logging: ReplicaLogging {
                log_logging: false,
                interval: Some(Duration::from_secs(1)),
            },
            optimizer_feature_overrides: Default::default(),
            schedule: Default::default(),
        }),
        workload_class: None,
    })
}

/// Defines the default config for a Cluster Replica.
fn default_replica_config(args: &BootstrapArgs) -> Result<ReplicaConfig, CatalogError> {
    Ok(ReplicaConfig {
        location: ReplicaLocation::Managed {
            size: args.default_cluster_replica_size.to_string(),
            availability_zone: None,
            internal: false,
            billed_as: None,
            pending: false,
        },
        logging: ReplicaLogging {
            log_logging: false,
            interval: Some(Duration::from_secs(1)),
        },
    })
}
