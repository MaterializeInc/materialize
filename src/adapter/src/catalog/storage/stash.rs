// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use itertools::max;
use mz_controller::clusters::ClusterId;
use mz_ore::now::EpochMillis;
use mz_repr::adt::mz_acl_item::AclMode;
use mz_repr::role_id::RoleId;
use mz_sql::catalog::RoleAttributes;
use mz_sql::names::{DatabaseId, SchemaId, PUBLIC_ROLE_NAME};
use mz_stash::objects::{proto, RustType};
use mz_stash::{StashError, Transaction, TypedCollection, STASH_VERSION};
use mz_storage_client::types::sources::Timeline;

use crate::catalog::builtin::{MZ_INTROSPECTION_ROLE, MZ_SYSTEM_ROLE};
use crate::catalog::storage::{
    BootstrapArgs, RoleMembership, AUDIT_LOG_ID_ALLOC_KEY, DATABASE_ID_ALLOC_KEY,
    MZ_INTROSPECTION_ROLE_ID, MZ_SYSTEM_ROLE_ID, REPLICA_ID_ALLOC_KEY, SCHEMA_ID_ALLOC_KEY,
    STORAGE_USAGE_ID_ALLOC_KEY, SYSTEM_CLUSTER_ID_ALLOC_KEY, USER_CLUSTER_ID_ALLOC_KEY,
    USER_ROLE_ID_ALLOC_KEY,
};
use crate::rbac;

/// The key used within the "config" collection where we store the Stash version.
const USER_VERSION: &str = "user_version";

pub const SETTING_COLLECTION: TypedCollection<proto::SettingKey, proto::SettingValue> =
    TypedCollection::new("setting");
pub const SYSTEM_GID_MAPPING_COLLECTION: TypedCollection<
    proto::GidMappingKey,
    proto::GidMappingValue,
> = TypedCollection::new("system_gid_mapping");
pub const CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION: TypedCollection<
    proto::ClusterIntrospectionSourceIndexKey,
    proto::ClusterIntrospectionSourceIndexValue,
> = TypedCollection::new("compute_introspection_source_index"); // historical name
pub const ROLES_COLLECTION: TypedCollection<proto::RoleKey, proto::RoleValue> =
    TypedCollection::new("role");
pub const DATABASES_COLLECTION: TypedCollection<proto::DatabaseKey, proto::DatabaseValue> =
    TypedCollection::new("database");
pub const SCHEMAS_COLLECTION: TypedCollection<proto::SchemaKey, proto::SchemaValue> =
    TypedCollection::new("schema");
pub const ITEM_COLLECTION: TypedCollection<proto::ItemKey, proto::ItemValue> =
    TypedCollection::new("item");
pub const TIMESTAMP_COLLECTION: TypedCollection<proto::TimestampKey, proto::TimestampValue> =
    TypedCollection::new("timestamp");
pub const SYSTEM_CONFIGURATION_COLLECTION: TypedCollection<
    proto::ServerConfigurationKey,
    proto::ServerConfigurationValue,
> = TypedCollection::new("system_configuration");
pub const CLUSTER_COLLECTION: TypedCollection<proto::ClusterKey, proto::ClusterValue> =
    TypedCollection::new("compute_instance");
pub const CLUSTER_REPLICA_COLLECTION: TypedCollection<
    proto::ClusterReplicaKey,
    proto::ClusterReplicaValue,
> = TypedCollection::new("compute_replicas");
pub const AUDIT_LOG_COLLECTION: TypedCollection<proto::AuditLogKey, ()> =
    TypedCollection::new("audit_log");
pub const CONFIG_COLLECTION: TypedCollection<proto::ConfigKey, proto::ConfigValue> =
    TypedCollection::new("config");
pub const ID_ALLOCATOR_COLLECTION: TypedCollection<proto::IdAllocKey, proto::IdAllocValue> =
    TypedCollection::new("id_alloc");
pub const STORAGE_USAGE_COLLECTION: TypedCollection<proto::StorageUsageKey, ()> =
    TypedCollection::new("storage_usage");
pub const DEFAULT_PRIVILEGES_COLLECTION: TypedCollection<
    proto::DefaultPrivilegesKey,
    proto::DefaultPrivilegesValue,
> = TypedCollection::new("default_privileges");

const USER_ID_ALLOC_KEY: &str = "user";
const SYSTEM_ID_ALLOC_KEY: &str = "system";

const DEFAULT_USER_CLUSTER_ID: ClusterId = ClusterId::User(1);
const DEFAULT_USER_CLUSTER_NAME: &str = "default";

const DEFAULT_REPLICA_ID: u64 = 1;
const DEFAULT_REPLICA_NAME: &str = "r1";

const MATERIALIZE_DATABASE_ID_VAL: u64 = 1;
const MATERIALIZE_DATABASE_ID: DatabaseId = DatabaseId::User(MATERIALIZE_DATABASE_ID_VAL);

const MZ_CATALOG_SCHEMA_ID: u64 = 1;
const PG_CATALOG_SCHEMA_ID: u64 = 2;
const PUBLIC_SCHEMA_ID: u64 = 3;
const MZ_INTERNAL_SCHEMA_ID: u64 = 4;
const INFORMATION_SCHEMA_ID: u64 = 5;

/// Initializes the Stash with some default objects.
///
/// Note: We should only use the latest types here from the `super::objects` module, we should
/// __not__ use any versioned protos, e.g. `objects_v15`.
#[tracing::instrument(level = "info", skip_all)]
pub async fn initialize(
    tx: &mut Transaction<'_>,
    options: &BootstrapArgs,
    now: EpochMillis,
) -> Result<(), StashError> {
    // During initialization we need to allocate IDs for certain things. We'll track what IDs we've
    // allocated, persisting the "next ids" at the end.
    let mut id_allocator = IdAllocator::new();

    // Collect audit events so we can commit them once at the very end.
    let mut audit_events = vec![];

    // First thing we need to do is persist the timestamp we're booting with.
    TIMESTAMP_COLLECTION
        .initialize(
            tx,
            vec![(
                proto::TimestampKey {
                    id: Timeline::EpochMilliseconds.to_string(),
                },
                proto::TimestampValue {
                    ts: Some(proto::Timestamp { internal: now }),
                },
            )],
        )
        .await?;

    // If provided, generate a new Id and attributes for the bootstrap role.
    //
    // Note: Make sure we do this _after_ initializing the ID_ALLOCATOR_COLLECTION.
    let bootstrap_role = if let Some(role) = &options.bootstrap_role {
        let role_id = RoleId::User(id_allocator.allocate(USER_ROLE_ID_ALLOC_KEY.to_string()));
        let role_val = proto::RoleValue {
            name: role.to_string(),
            attributes: Some(
                RoleAttributes::new()
                    .with_create_db()
                    .with_create_cluster()
                    .with_create_role()
                    .into_proto(),
            ),
            membership: Some(RoleMembership::new().into_proto()),
        };

        audit_events.push((
            proto::audit_log_event_v1::EventType::Create,
            proto::audit_log_event_v1::ObjectType::Role,
            proto::audit_log_event_v1::Details::IdNameV1(proto::audit_log_event_v1::IdNameV1 {
                id: role_id.to_string(),
                name: role.to_string(),
            }),
        ));

        let proto_role_id: proto::RoleId = role_id.into_proto();
        Some((proto_role_id, role_val))
    } else {
        None
    };

    ROLES_COLLECTION
        .initialize(
            tx,
            [
                (
                    proto::RoleKey {
                        id: Some(MZ_SYSTEM_ROLE_ID.into_proto()),
                    },
                    proto::RoleValue {
                        name: MZ_SYSTEM_ROLE.name.to_string(),
                        attributes: Some(MZ_SYSTEM_ROLE.attributes.clone().into_proto()),
                        membership: Some(RoleMembership::new().into_proto()),
                    },
                ),
                (
                    proto::RoleKey {
                        id: Some(MZ_INTROSPECTION_ROLE_ID.into_proto()),
                    },
                    proto::RoleValue {
                        name: MZ_INTROSPECTION_ROLE.name.to_string(),
                        attributes: Some(MZ_INTROSPECTION_ROLE.attributes.clone().into_proto()),
                        membership: Some(RoleMembership::new().into_proto()),
                    },
                ),
                (
                    proto::RoleKey {
                        id: Some(RoleId::Public.into_proto()),
                    },
                    proto::RoleValue {
                        name: PUBLIC_ROLE_NAME.as_str().to_lowercase(),
                        attributes: Some(RoleAttributes::new().into_proto()),
                        membership: Some(RoleMembership::new().into_proto()),
                    },
                ),
            ]
            .into_iter()
            // Optionally insert a privilege for the bootstrap role.
            .chain(bootstrap_role.as_ref().map(|(role_id, role_val)| {
                let key = proto::RoleKey {
                    id: Some(role_id.clone()),
                };
                (key, role_val.clone())
            })),
        )
        .await?;

    let mut db_privileges = vec![
        proto::MzAclItem {
            grantee: Some(RoleId::Public.into_proto()),
            grantor: Some(MZ_SYSTEM_ROLE_ID.into_proto()),
            acl_mode: Some(AclMode::USAGE.into_proto()),
        },
        rbac::owner_privilege(mz_sql::catalog::ObjectType::Database, MZ_SYSTEM_ROLE_ID)
            .into_proto(),
    ];
    // Optionally add a privilege for the bootstrap role.
    if let Some((role_id, _)) = &bootstrap_role {
        db_privileges.push(proto::MzAclItem {
            grantee: Some(role_id.clone()),
            grantor: Some(MZ_SYSTEM_ROLE_ID.into_proto()),
            acl_mode: Some(
                rbac::all_object_privileges(mz_sql::catalog::ObjectType::Database).into_proto(),
            ),
        })
    };

    DATABASES_COLLECTION
        .initialize(
            tx,
            [(
                proto::DatabaseKey {
                    id: Some(MATERIALIZE_DATABASE_ID.into_proto()),
                },
                proto::DatabaseValue {
                    name: "materialize".into(),
                    owner_id: Some(MZ_SYSTEM_ROLE_ID.into_proto()),
                    privileges: db_privileges,
                },
            )],
        )
        .await?;
    audit_events.push((
        proto::audit_log_event_v1::EventType::Create,
        proto::audit_log_event_v1::ObjectType::Database,
        proto::audit_log_event_v1::Details::IdNameV1(proto::audit_log_event_v1::IdNameV1 {
            id: MATERIALIZE_DATABASE_ID.to_string(),
            name: "materialize".to_string(),
        }),
    ));

    let schema_privileges = vec![
        rbac::default_catalog_privilege(mz_sql::catalog::ObjectType::Schema).into_proto(),
        rbac::owner_privilege(mz_sql::catalog::ObjectType::Schema, MZ_SYSTEM_ROLE_ID).into_proto(),
    ];

    let mz_catalog_schema_key = proto::SchemaKey {
        id: Some(SchemaId::System(MZ_CATALOG_SCHEMA_ID).into_proto()),
    };
    let mz_catalog_schema = proto::SchemaValue {
        database_id: None,
        name: "mz_catalog".to_string(),
        owner_id: Some(MZ_SYSTEM_ROLE_ID.into_proto()),
        privileges: schema_privileges.clone(),
    };

    let pg_catalog_schema_key = proto::SchemaKey {
        id: Some(SchemaId::System(PG_CATALOG_SCHEMA_ID).into_proto()),
    };
    let pg_catalog_schema = proto::SchemaValue {
        database_id: None,
        name: "pg_catalog".to_string(),
        owner_id: Some(MZ_SYSTEM_ROLE_ID.into_proto()),
        privileges: schema_privileges.clone(),
    };

    let mz_internal_schema_key = proto::SchemaKey {
        id: Some(SchemaId::System(MZ_INTERNAL_SCHEMA_ID).into_proto()),
    };
    let mz_internal_schema = proto::SchemaValue {
        database_id: None,
        name: "mz_internal".to_string(),
        owner_id: Some(MZ_SYSTEM_ROLE_ID.into_proto()),
        privileges: schema_privileges.clone(),
    };

    let information_schema_key = proto::SchemaKey {
        id: Some(SchemaId::System(INFORMATION_SCHEMA_ID).into_proto()),
    };
    let information_schema = proto::SchemaValue {
        database_id: None,
        name: "information_schema".to_string(),
        owner_id: Some(MZ_SYSTEM_ROLE_ID.into_proto()),
        privileges: schema_privileges.clone(),
    };

    let public_schema_key = proto::SchemaKey {
        id: Some(SchemaId::User(PUBLIC_SCHEMA_ID).into_proto()),
    };
    let public_schema = proto::SchemaValue {
        database_id: Some(MATERIALIZE_DATABASE_ID.into_proto()),
        name: "public".to_string(),
        owner_id: Some(MZ_SYSTEM_ROLE_ID.into_proto()),
        privileges: vec![
            proto::MzAclItem {
                grantee: Some(RoleId::Public.into_proto()),
                grantor: Some(MZ_SYSTEM_ROLE_ID.into_proto()),
                acl_mode: Some(AclMode::USAGE.into_proto()),
            },
            rbac::owner_privilege(mz_sql::catalog::ObjectType::Schema, MZ_SYSTEM_ROLE_ID)
                .into_proto(),
        ]
        .into_iter()
        // Optionally add the bootstrap role to the public schema.
        .chain(
            bootstrap_role
                .as_ref()
                .map(|(role_id, _)| proto::MzAclItem {
                    grantee: Some(role_id.clone()),
                    grantor: Some(MZ_SYSTEM_ROLE_ID.into_proto()),
                    acl_mode: Some(
                        rbac::all_object_privileges(mz_sql::catalog::ObjectType::Schema)
                            .into_proto(),
                    ),
                }),
        )
        .collect(),
    };

    SCHEMAS_COLLECTION
        .initialize(
            tx,
            [
                (mz_catalog_schema_key, mz_catalog_schema),
                (pg_catalog_schema_key, pg_catalog_schema),
                (public_schema_key, public_schema),
                (mz_internal_schema_key, mz_internal_schema),
                (information_schema_key, information_schema),
            ],
        )
        .await?;
    audit_events.push((
        proto::audit_log_event_v1::EventType::Create,
        proto::audit_log_event_v1::ObjectType::Schema,
        proto::audit_log_event_v1::Details::SchemaV2(proto::audit_log_event_v1::SchemaV2 {
            id: PUBLIC_SCHEMA_ID.to_string(),
            name: "public".to_string(),
            database_name: Some("materialize".to_string().into()),
        }),
    ));

    let mut cluster_privileges = vec![
        proto::MzAclItem {
            grantee: Some(RoleId::Public.into_proto()),
            grantor: Some(MZ_SYSTEM_ROLE_ID.into_proto()),
            acl_mode: Some(AclMode::USAGE.into_proto()),
        },
        rbac::owner_privilege(mz_sql::catalog::ObjectType::Cluster, MZ_SYSTEM_ROLE_ID).into_proto(),
    ];

    // Optionally add a privilege for the bootstrap role.
    if let Some((role_id, _)) = &bootstrap_role {
        cluster_privileges.push(proto::MzAclItem {
            grantee: Some(role_id.clone()),
            grantor: Some(MZ_SYSTEM_ROLE_ID.into_proto()),
            acl_mode: Some(
                rbac::all_object_privileges(mz_sql::catalog::ObjectType::Cluster).into_proto(),
            ),
        });
    };

    CLUSTER_COLLECTION
        .initialize(
            tx,
            [(
                proto::ClusterKey {
                    id: Some(DEFAULT_USER_CLUSTER_ID.into_proto()),
                },
                proto::ClusterValue {
                    name: DEFAULT_USER_CLUSTER_NAME.to_string(),
                    linked_object_id: None,
                    owner_id: Some(MZ_SYSTEM_ROLE_ID.into_proto()),
                    privileges: cluster_privileges,
                },
            )],
        )
        .await?;
    audit_events.push((
        proto::audit_log_event_v1::EventType::Create,
        proto::audit_log_event_v1::ObjectType::Cluster,
        proto::audit_log_event_v1::Details::IdNameV1(proto::audit_log_event_v1::IdNameV1 {
            id: DEFAULT_USER_CLUSTER_ID.to_string(),
            name: DEFAULT_USER_CLUSTER_NAME.to_string(),
        }),
    ));

    CLUSTER_REPLICA_COLLECTION
        .initialize(
            tx,
            [(
                proto::ClusterReplicaKey {
                    id: Some(proto::ReplicaId {
                        value: DEFAULT_REPLICA_ID,
                    }),
                },
                proto::ClusterReplicaValue {
                    cluster_id: Some(DEFAULT_USER_CLUSTER_ID.into_proto()),
                    name: DEFAULT_REPLICA_NAME.to_string(),
                    config: Some(default_replica_config(options)),
                    owner_id: Some(MZ_SYSTEM_ROLE_ID.into_proto()),
                },
            )],
        )
        .await?;
    audit_events.push((
        proto::audit_log_event_v1::EventType::Create,
        proto::audit_log_event_v1::ObjectType::ClusterReplica,
        proto::audit_log_event_v1::Details::CreateClusterReplicaV1(
            proto::audit_log_event_v1::CreateClusterReplicaV1 {
                cluster_id: DEFAULT_USER_CLUSTER_ID.to_string(),
                cluser_name: DEFAULT_USER_CLUSTER_NAME.to_string(),
                replica_name: DEFAULT_REPLICA_NAME.to_string(),
                replica_id: Some(DEFAULT_REPLICA_ID.to_string().into()),
                logical_size: options.default_cluster_replica_size.to_string(),
            },
        ),
    ));

    // Allocate an ID for each audit log event.
    let mut audit_events_with_id = Vec::with_capacity(audit_events.len());
    for (ty, obj, details) in audit_events {
        let id = id_allocator.allocate(AUDIT_LOG_ID_ALLOC_KEY.to_string());
        audit_events_with_id.push((id, ty, obj, details));
    }

    // Push all of our events onto the audit log.
    AUDIT_LOG_COLLECTION
        .initialize(
            tx,
            audit_events_with_id
                .into_iter()
                .map(|(id, ty, obj, details)| {
                    let event = proto::audit_log_key::Event::V1(proto::AuditLogEventV1 {
                        id,
                        event_type: ty.into(),
                        object_type: obj.into(),
                        user: None,
                        occurred_at: Some(now.into_proto()),
                        details: Some(details),
                    });
                    (proto::AuditLogKey { event: Some(event) }, ())
                }),
        )
        .await?;

    // Record all of our used ids.
    ID_ALLOCATOR_COLLECTION
        .initialize(
            tx,
            [
                (
                    proto::IdAllocKey {
                        name: USER_ID_ALLOC_KEY.to_string(),
                    },
                    proto::IdAllocValue {
                        next_id: id_allocator.next_id(USER_ID_ALLOC_KEY),
                    },
                ),
                (
                    proto::IdAllocKey {
                        name: SYSTEM_ID_ALLOC_KEY.to_string(),
                    },
                    proto::IdAllocValue {
                        next_id: id_allocator.next_id(SYSTEM_ID_ALLOC_KEY),
                    },
                ),
                (
                    proto::IdAllocKey {
                        name: DATABASE_ID_ALLOC_KEY.to_string(),
                    },
                    proto::IdAllocValue {
                        next_id: MATERIALIZE_DATABASE_ID_VAL + 1,
                    },
                ),
                (
                    proto::IdAllocKey {
                        name: SCHEMA_ID_ALLOC_KEY.to_string(),
                    },
                    proto::IdAllocValue {
                        next_id: max(&[
                            MZ_CATALOG_SCHEMA_ID,
                            PG_CATALOG_SCHEMA_ID,
                            PUBLIC_SCHEMA_ID,
                            MZ_INTERNAL_SCHEMA_ID,
                            INFORMATION_SCHEMA_ID,
                        ])
                        .expect("known to be non-empty")
                            + 1,
                    },
                ),
                (
                    proto::IdAllocKey {
                        name: USER_ROLE_ID_ALLOC_KEY.to_string(),
                    },
                    proto::IdAllocValue {
                        next_id: id_allocator.next_id(USER_ROLE_ID_ALLOC_KEY),
                    },
                ),
                (
                    proto::IdAllocKey {
                        name: USER_CLUSTER_ID_ALLOC_KEY.to_string(),
                    },
                    proto::IdAllocValue {
                        next_id: DEFAULT_USER_CLUSTER_ID.inner_id() + 1,
                    },
                ),
                (
                    proto::IdAllocKey {
                        name: SYSTEM_CLUSTER_ID_ALLOC_KEY.to_string(),
                    },
                    proto::IdAllocValue {
                        next_id: id_allocator.next_id(SYSTEM_CLUSTER_ID_ALLOC_KEY),
                    },
                ),
                (
                    proto::IdAllocKey {
                        name: REPLICA_ID_ALLOC_KEY.to_string(),
                    },
                    proto::IdAllocValue {
                        next_id: DEFAULT_REPLICA_ID + 1,
                    },
                ),
                (
                    proto::IdAllocKey {
                        name: AUDIT_LOG_ID_ALLOC_KEY.to_string(),
                    },
                    proto::IdAllocValue {
                        next_id: id_allocator.next_id(AUDIT_LOG_ID_ALLOC_KEY),
                    },
                ),
                (
                    proto::IdAllocKey {
                        name: STORAGE_USAGE_ID_ALLOC_KEY.to_string(),
                    },
                    proto::IdAllocValue {
                        next_id: id_allocator.next_id(STORAGE_USAGE_ID_ALLOC_KEY),
                    },
                ),
            ],
        )
        .await?;
    DEFAULT_PRIVILEGES_COLLECTION
        .initialize(
            tx,
            vec![(
                proto::DefaultPrivilegesKey {
                    role_id: Some(RoleId::Public.into_proto()),
                    database_id: None,
                    schema_id: None,
                    object_type: mz_sql::catalog::ObjectType::Type.into_proto().into(),
                    grantee: Some(RoleId::Public.into_proto()),
                },
                proto::DefaultPrivilegesValue {
                    privileges: Some(AclMode::USAGE.into_proto()),
                },
            )],
        )
        .await?;

    // Initialize all other collections to be empty.
    SETTING_COLLECTION.initialize(tx, vec![]).await?;
    SYSTEM_GID_MAPPING_COLLECTION.initialize(tx, vec![]).await?;
    CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION
        .initialize(tx, vec![])
        .await?;
    ITEM_COLLECTION.initialize(tx, vec![]).await?;
    SYSTEM_CONFIGURATION_COLLECTION
        .initialize(tx, vec![])
        .await?;
    STORAGE_USAGE_COLLECTION.initialize(tx, vec![]).await?;

    // Set our initial version.
    CONFIG_COLLECTION
        .initialize(
            tx,
            [(
                proto::ConfigKey {
                    key: USER_VERSION.to_string(),
                },
                proto::ConfigValue {
                    value: STASH_VERSION,
                },
            )],
        )
        .await?;

    Ok(())
}

/// Defines the default config for a Cluster Replica.
pub fn default_replica_config(args: &BootstrapArgs) -> proto::ReplicaConfig {
    proto::ReplicaConfig {
        location: Some(proto::replica_config::Location::Managed(
            proto::replica_config::ManagedLocation {
                size: args.default_cluster_replica_size.to_string(),
                availability_zone: args.default_availability_zone.to_string(),
                az_user_specified: false,
            },
        )),
        logging: Some(proto::replica_config::Logging {
            log_logging: false,
            interval: Some(proto::Duration::from_secs(1)),
        }),
        idle_arrangement_merge_effort: None,
    }
}

/// A small struct which keeps track of what Ids we've used during initialization.
#[derive(Debug, Clone)]
struct IdAllocator {
    ids: BTreeMap<String, u64>,
}

impl IdAllocator {
    const DEFAULT_ID: u64 = 1;

    pub fn new() -> Self {
        IdAllocator {
            ids: BTreeMap::default(),
        }
    }

    /// For a given key, returns the current value and bumps the allocator.
    pub fn allocate(&mut self, key: String) -> u64 {
        let next_id = self.ids.entry(key).or_insert(Self::DEFAULT_ID);
        let copy = *next_id;
        *next_id = next_id
            .checked_add(1)
            .expect("allocated more than u64::MAX ids!");
        copy
    }

    /// Gets the next ID, without bumping the allocator.
    pub fn next_id(&self, key: &str) -> u64 {
        self.ids.get(key).copied().unwrap_or(Self::DEFAULT_ID)
    }
}

#[cfg(test)]
mod test {
    use super::IdAllocator;

    #[mz_ore::test]
    fn smoke_test_id_allocator() {
        let mut allocator = IdAllocator::new();

        assert_eq!(allocator.allocate("a".to_string()), 1);
        assert_eq!(allocator.next_id("a"), 2);

        assert_eq!(allocator.next_id("b"), 1);
        assert_eq!(allocator.allocate("b".to_string()), 1);
        assert_eq!(allocator.next_id("b"), 2);
    }
}
