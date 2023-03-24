// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::Hash;
use std::iter::once;
use std::sync::Arc;
use std::time::Duration;

use itertools::{max, Itertools};
use serde::{Deserialize, Serialize};

use mz_audit_log::{
    EventDetails, EventType, EventV1, ObjectType, VersionedEvent, VersionedStorageUsage,
};
use mz_controller::clusters::{ClusterId, ReplicaConfig, ReplicaId};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::now::{EpochMillis, NowFn};
use mz_repr::GlobalId;
use mz_sql::catalog::{CatalogError as SqlCatalogError, CatalogItemType, RoleAttributes};
use mz_sql::names::{
    DatabaseId, ObjectQualifiers, QualifiedObjectName, ResolvedDatabaseSpecifier, RoleId, SchemaId,
    SchemaSpecifier, PUBLIC_ROLE_NAME,
};
use mz_stash::{AppendBatch, Stash, StashError, TableTransaction, TypedCollection};
use mz_storage_client::types::sources::Timeline;

use crate::catalog;
use crate::catalog::builtin::{
    BuiltinLog, BUILTIN_CLUSTERS, BUILTIN_CLUSTER_REPLICAS, BUILTIN_PREFIXES,
    MZ_INTROSPECTION_ROLE, MZ_SYSTEM_ROLE,
};
use crate::catalog::error::{Error, ErrorKind};
use crate::catalog::{
    is_public_role, is_reserved_name, RoleMembership, SerializedRole, SystemObjectMapping,
};
use crate::catalog::{SerializedReplicaConfig, DEFAULT_CLUSTER_REPLICA_NAME};
use crate::coord::timeline;

use super::{SerializedCatalogItem, SerializedReplicaLocation, SerializedReplicaLogging};

const USER_VERSION: &str = "user_version";

const MATERIALIZE_DATABASE_ID: u64 = 1;
const MZ_CATALOG_SCHEMA_ID: u64 = 1;
const PG_CATALOG_SCHEMA_ID: u64 = 2;
const PUBLIC_SCHEMA_ID: u64 = 3;
const MZ_INTERNAL_SCHEMA_ID: u64 = 4;
const INFORMATION_SCHEMA_ID: u64 = 5;
const DEFAULT_USER_CLUSTER_ID: ClusterId = ClusterId::User(1);
const DEFAULT_REPLICA_ID: u64 = 1;
pub const MZ_SYSTEM_ROLE_ID: RoleId = RoleId::System(1);
pub const MZ_INTROSPECTION_ROLE_ID: RoleId = RoleId::System(2);

const DATABASE_ID_ALLOC_KEY: &str = "database";
const SCHEMA_ID_ALLOC_KEY: &str = "schema";
const USER_ROLE_ID_ALLOC_KEY: &str = "user_role";
const USER_CLUSTER_ID_ALLOC_KEY: &str = "user_compute";
const SYSTEM_CLUSTER_ID_ALLOC_KEY: &str = "system_compute";
const REPLICA_ID_ALLOC_KEY: &str = "replica";
pub(crate) const AUDIT_LOG_ID_ALLOC_KEY: &str = "auditlog";
pub(crate) const STORAGE_USAGE_ID_ALLOC_KEY: &str = "storage_usage";

async fn migrate(
    stash: &mut Stash,
    version: u64,
    now: EpochMillis,
    bootstrap_args: &BootstrapArgs,
) -> Result<(), catalog::error::Error> {
    // Initial state.
    let migrations: &[for<'a> fn(
        &mut Transaction<'a>,
        EpochMillis,
        &'a BootstrapArgs,
    ) -> Result<(), catalog::error::Error>] = &[
        |txn: &mut Transaction<'_>, now, bootstrap_args| {
            txn.id_allocator.insert(
                IdAllocKey {
                    name: "user".into(),
                },
                IdAllocValue { next_id: 1 },
            )?;
            txn.id_allocator.insert(
                IdAllocKey {
                    name: "system".into(),
                },
                IdAllocValue { next_id: 1 },
            )?;
            txn.id_allocator.insert(
                IdAllocKey {
                    name: DATABASE_ID_ALLOC_KEY.into(),
                },
                IdAllocValue {
                    next_id: MATERIALIZE_DATABASE_ID + 1,
                },
            )?;
            txn.id_allocator.insert(
                IdAllocKey {
                    name: SCHEMA_ID_ALLOC_KEY.into(),
                },
                IdAllocValue {
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
            )?;
            txn.id_allocator.insert(
                IdAllocKey {
                    name: USER_ROLE_ID_ALLOC_KEY.into(),
                },
                IdAllocValue { next_id: 1 },
            )?;
            txn.id_allocator.insert(
                IdAllocKey {
                    name: USER_CLUSTER_ID_ALLOC_KEY.into(),
                },
                IdAllocValue {
                    next_id: DEFAULT_USER_CLUSTER_ID.inner_id() + 1,
                },
            )?;
            txn.id_allocator.insert(
                IdAllocKey {
                    name: SYSTEM_CLUSTER_ID_ALLOC_KEY.into(),
                },
                IdAllocValue { next_id: 1 },
            )?;
            txn.id_allocator.insert(
                IdAllocKey {
                    name: REPLICA_ID_ALLOC_KEY.into(),
                },
                IdAllocValue {
                    next_id: DEFAULT_REPLICA_ID + 1,
                },
            )?;
            txn.id_allocator.insert(
                IdAllocKey {
                    name: AUDIT_LOG_ID_ALLOC_KEY.into(),
                },
                IdAllocValue { next_id: 1 },
            )?;
            txn.id_allocator.insert(
                IdAllocKey {
                    name: STORAGE_USAGE_ID_ALLOC_KEY.into(),
                },
                IdAllocValue { next_id: 1 },
            )?;
            txn.roles.insert(
                RoleKey {
                    id: MZ_SYSTEM_ROLE_ID,
                },
                RoleValue {
                    role: (&*MZ_SYSTEM_ROLE).into(),
                },
            )?;
            txn.roles.insert(
                RoleKey {
                    id: MZ_INTROSPECTION_ROLE_ID,
                },
                RoleValue {
                    role: (&*MZ_INTROSPECTION_ROLE).into(),
                },
            )?;
            txn.databases.insert(
                DatabaseKey {
                    id: MATERIALIZE_DATABASE_ID,
                },
                DatabaseValue {
                    name: "materialize".into(),
                    owner_id: Some(MZ_SYSTEM_ROLE_ID),
                },
            )?;
            let id = txn.get_and_increment_id(AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
            txn.audit_log_updates.push((
                AuditLogKey {
                    event: VersionedEvent::new(
                        id,
                        EventType::Create,
                        ObjectType::Database,
                        EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                            id: MATERIALIZE_DATABASE_ID.to_string(),
                            name: "materialize".into(),
                        }),
                        None,
                        now,
                    ),
                },
                (),
                1,
            ));
            txn.schemas.insert(
                SchemaKey {
                    id: MZ_CATALOG_SCHEMA_ID,
                },
                SchemaValue {
                    database_id: None,
                    name: "mz_catalog".into(),
                    owner_id: Some(MZ_SYSTEM_ROLE_ID),
                },
            )?;
            txn.schemas.insert(
                SchemaKey {
                    id: PG_CATALOG_SCHEMA_ID,
                },
                SchemaValue {
                    database_id: None,
                    name: "pg_catalog".into(),
                    owner_id: Some(MZ_SYSTEM_ROLE_ID),
                },
            )?;
            txn.schemas.insert(
                SchemaKey {
                    id: PUBLIC_SCHEMA_ID,
                },
                SchemaValue {
                    database_id: Some(MATERIALIZE_DATABASE_ID),
                    name: "public".into(),
                    owner_id: Some(MZ_SYSTEM_ROLE_ID),
                },
            )?;
            let id = txn.get_and_increment_id(AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
            txn.audit_log_updates.push((
                AuditLogKey {
                    event: VersionedEvent::new(
                        id,
                        EventType::Create,
                        ObjectType::Schema,
                        EventDetails::SchemaV1(mz_audit_log::SchemaV1 {
                            id: PUBLIC_SCHEMA_ID.to_string(),
                            name: "public".into(),
                            database_name: "materialize".into(),
                        }),
                        None,
                        now,
                    ),
                },
                (),
                1,
            ));
            txn.schemas.insert(
                SchemaKey {
                    id: MZ_INTERNAL_SCHEMA_ID,
                },
                SchemaValue {
                    database_id: None,
                    name: "mz_internal".into(),
                    owner_id: Some(MZ_SYSTEM_ROLE_ID),
                },
            )?;
            txn.schemas.insert(
                SchemaKey {
                    id: INFORMATION_SCHEMA_ID,
                },
                SchemaValue {
                    database_id: None,
                    name: "information_schema".into(),
                    owner_id: Some(MZ_SYSTEM_ROLE_ID),
                },
            )?;
            let default_cluster = ClusterValue {
                name: "default".into(),
                linked_object_id: None,
                owner_id: Some(MZ_SYSTEM_ROLE_ID),
            };
            let default_replica = ClusterReplicaValue {
                cluster_id: DEFAULT_USER_CLUSTER_ID,
                name: DEFAULT_CLUSTER_REPLICA_NAME.into(),
                config: default_cluster_replica_config(bootstrap_args),
                owner_id: Some(MZ_SYSTEM_ROLE_ID),
            };
            txn.clusters.insert(
                ClusterKey {
                    id: DEFAULT_USER_CLUSTER_ID,
                },
                default_cluster.clone(),
            )?;
            txn.cluster_replicas.insert(
                ClusterReplicaKey {
                    id: DEFAULT_REPLICA_ID,
                },
                default_replica.clone(),
            )?;
            let id = txn.get_and_increment_id(AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
            txn.audit_log_updates.push((
                AuditLogKey {
                    event: VersionedEvent::new(
                        id,
                        EventType::Create,
                        ObjectType::Cluster,
                        EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                            id: DEFAULT_USER_CLUSTER_ID.to_string(),
                            name: default_cluster.name.clone(),
                        }),
                        None,
                        now,
                    ),
                },
                (),
                1,
            ));
            let id = txn.get_and_increment_id(AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
            txn.audit_log_updates.push((
                AuditLogKey {
                    event: VersionedEvent::new(
                        id,
                        EventType::Create,
                        ObjectType::ClusterReplica,
                        EventDetails::CreateClusterReplicaV1(
                            mz_audit_log::CreateClusterReplicaV1 {
                                cluster_id: DEFAULT_USER_CLUSTER_ID.to_string(),
                                cluster_name: default_cluster.name,
                                replica_name: default_replica.name,
                                replica_id: Some(DEFAULT_REPLICA_ID.to_string()),
                                logical_size: bootstrap_args.default_cluster_replica_size.clone(),
                            },
                        ),
                        None,
                        now,
                    ),
                },
                (),
                1,
            ));
            txn.configs
                .insert(USER_VERSION.to_string(), ConfigValue { value: 0 })?;
            Ok(())
        },
        // These migrations were removed, but we need to keep empty migrations because the
        // user version depends on the length of this array. New migrations should still go after
        // these empty migrations.
        |_, _, _| Ok(()),
        |_, _, _| Ok(()),
        |_, _, _| Ok(()),
        |_, _, _| Ok(()),
        |_, _, _| Ok(()),
        |_, _, _| Ok(()),
        // Role memberships were added to role definitions.
        //
        // Introduced in v0.46.0
        //
        // TODO(jkosh44) Can be cleared (patched to be empty) in v0.49.0
        |txn: &mut Transaction<'_>, _now, _bootstrap_args| {
            txn.roles.update(|_role_key, role_value| {
                let mut role_value = role_value.clone();
                if role_value.role.membership.is_none() {
                    role_value.role.membership = Some(RoleMembership::new());
                }
                Some(role_value)
            })?;
            Ok(())
        },
        // The PUBLIC psuedo role was added.
        //
        // Introduced in v0.48.0
        //
        // TODO(jkosh44) Can be coalesced into the first migration in v0.50.0
        |txn: &mut Transaction<'_>, now, _bootstrap_args| {
            // Delete any existing PUBLIC role.
            let roles = txn
                .roles
                .delete(|_role_key, role_value| is_public_role(role_value.role.name.as_str()));
            assert!(roles.len() <= 1, "duplicate roles are not allowed");
            for (role_key, role_value) in roles {
                let id = txn.get_and_increment_id(AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
                txn.audit_log_updates.push((
                    AuditLogKey {
                        event: VersionedEvent::V1(EventV1 {
                            id,
                            event_type: EventType::Drop,
                            object_type: ObjectType::Role,
                            details: EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                                id: role_key.id.to_string(),
                                name: role_value.role.name,
                            }),
                            user: None,
                            occurred_at: now,
                        }),
                    },
                    (),
                    1,
                ));
            }

            txn.roles.insert(
                RoleKey { id: RoleId::Public },
                RoleValue {
                    role: SerializedRole {
                        name: PUBLIC_ROLE_NAME.as_str().to_lowercase(),
                        attributes: RoleAttributes::new(),
                        membership: Some(RoleMembership::new()),
                    },
                },
            )?;
            Ok(())
        },
        // Object owners were added to object definitions.
        //
        // Introduced in v0.47.0
        //
        // TODO(jkosh44) Can be cleared (patched to be empty) in v0.50.0
        |txn: &mut Transaction<'_>, now, _bootstrap_args| {
            // Delete the system role id allocator. All system role ids will need
            // to be hard-coded going forward.
            txn.id_allocator
                .delete(|id_alloc_key, _| id_alloc_key.name == "system_role");

            // Delete all system roles so we can re-insert them with known IDs.
            let sys_roles = txn.roles.delete(|role_key, _| role_key.id.is_system());
            assert_eq!(2, sys_roles.len(), "unexpected number of system roles");
            txn.roles.insert(
                RoleKey {
                    id: MZ_SYSTEM_ROLE_ID,
                },
                RoleValue {
                    role: (&*MZ_SYSTEM_ROLE).into(),
                },
            )?;
            txn.roles.insert(
                RoleKey {
                    id: MZ_INTROSPECTION_ROLE_ID,
                },
                RoleValue {
                    role: (&*MZ_INTROSPECTION_ROLE).into(),
                },
            )?;

            // We need a default owner for all existing objects. So we just create a new role for
            // this purpose named "default_owner".
            let default_owner_name = "default_owner";
            let default_owner_id = match txn
                .roles
                .items()
                .iter()
                .filter(|(_, role_value)| role_value.role.name == default_owner_name)
                .next()
            {
                Some((_, _)) => {
                    panic!(
                        "Migration failed! Role named `default_owner` already exists. Please \
                    contact @jkosh44 (joe@materialize.com) and let him know. In order to fix this \
                    we'll need to ask the client to delete the `default_owner` role."
                    );
                }
                None => {
                    let role_id = txn.insert_user_role(SerializedRole {
                        name: default_owner_name.to_string(),
                        attributes: RoleAttributes::new(),
                        membership: Some(RoleMembership::new()),
                    })?;
                    let audit_id = txn.get_and_increment_id(AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
                    txn.audit_log_updates.push((
                        AuditLogKey {
                            event: VersionedEvent::new(
                                audit_id,
                                EventType::Create,
                                ObjectType::Role,
                                EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                                    id: role_id.to_string(),
                                    name: default_owner_name.to_string(),
                                }),
                                None,
                                now,
                            ),
                        },
                        (),
                        1,
                    ));
                    role_id
                }
            };

            txn.databases.update(|database_key, database_value| {
                let mut database_value = database_value.clone();
                if database_value.owner_id.is_none() {
                    if database_key.id == MATERIALIZE_DATABASE_ID {
                        database_value.owner_id = Some(MZ_SYSTEM_ROLE_ID);
                    } else {
                        database_value.owner_id = Some(default_owner_id);
                    }
                }
                Some(database_value)
            })?;

            txn.schemas.update(|schema_key, schema_value| {
                let mut schema_value = schema_value.clone();
                if schema_value.owner_id.is_none() {
                    if [
                        MZ_CATALOG_SCHEMA_ID,
                        PG_CATALOG_SCHEMA_ID,
                        PUBLIC_SCHEMA_ID,
                        MZ_INTERNAL_SCHEMA_ID,
                        INFORMATION_SCHEMA_ID,
                    ]
                    .into_iter()
                    .any(|schema_id| schema_id == schema_key.id)
                    {
                        schema_value.owner_id = Some(MZ_SYSTEM_ROLE_ID);
                    } else {
                        schema_value.owner_id = Some(default_owner_id);
                    }
                }
                Some(schema_value)
            })?;

            txn.items.update(|item_key, item_value| {
                let mut item_value = item_value.clone();
                if item_value.owner_id.is_none() {
                    if item_key.gid.is_system() {
                        item_value.owner_id = Some(MZ_SYSTEM_ROLE_ID);
                    } else {
                        item_value.owner_id = Some(default_owner_id);
                    }
                }
                Some(item_value)
            })?;

            txn.clusters.update(|cluster_key, cluster_value| {
                let mut cluster_value = cluster_value.clone();
                if cluster_value.owner_id.is_none() {
                    if cluster_key.id == DEFAULT_USER_CLUSTER_ID
                        || BUILTIN_CLUSTERS
                            .iter()
                            .any(|cluster| cluster.name == cluster_value.name)
                    {
                        cluster_value.owner_id = Some(MZ_SYSTEM_ROLE_ID);
                    } else {
                        cluster_value.owner_id = Some(default_owner_id);
                    }
                }
                Some(cluster_value)
            })?;

            txn.cluster_replicas.update(|replica_key, replica_value| {
                let mut replica_value = replica_value.clone();
                if replica_value.owner_id.is_none() {
                    if replica_key.id == DEFAULT_REPLICA_ID
                        || BUILTIN_CLUSTER_REPLICAS
                            .iter()
                            .any(|replica| replica.name == replica_value.name)
                    {
                        replica_value.owner_id = Some(MZ_SYSTEM_ROLE_ID);
                    } else {
                        replica_value.owner_id = Some(default_owner_id);
                    }
                }
                Some(replica_value)
            })?;

            Ok(())
        },
        // Add new migrations above.
        //
        // Migrations should be preceded with a comment of the following form:
        //
        //     > Short summary of migration's purpose.
        //     >
        //     > Introduced in <VERSION>.
        //     >
        //     > Optional additional commentary about safety or approach.
        //
        // Please include @mjibson and @jkosh44 on any code reviews that add or
        // edit migrations.
        // Migrations must preserve backwards compatibility with all past releases
        // of Materialize. Migrations can be edited up until they ship in a
        // release, after which they must never be removed, only patched by future
        // migrations. Migrations must be transactional or idempotent (in case of
        // midway failure).
    ];

    let mut txn = transaction(stash).await?;
    for (i, migration) in migrations
        .iter()
        .enumerate()
        .skip(usize::cast_from(version))
    {
        (migration)(&mut txn, now, bootstrap_args)?;
        txn.update_user_version(u64::cast_from(i))?;
    }
    add_new_builtin_clusters_migration(&mut txn)?;
    add_new_builtin_cluster_replicas_migration(&mut txn, bootstrap_args)?;
    txn.commit().await?;
    Ok(())
}

fn add_new_builtin_clusters_migration(
    txn: &mut Transaction<'_>,
) -> Result<(), catalog::error::Error> {
    let cluster_names: BTreeSet<_> = txn
        .clusters
        .items()
        .into_values()
        .map(|value| value.name)
        .collect();

    for builtin_cluster in &*BUILTIN_CLUSTERS {
        assert!(
            is_reserved_name(builtin_cluster.name),
            "builtin cluster {builtin_cluster:?} must start with one of the following prefixes {}",
            BUILTIN_PREFIXES.join(", ")
        );
        if !cluster_names.contains(builtin_cluster.name) {
            let id = txn.get_and_increment_id(SYSTEM_CLUSTER_ID_ALLOC_KEY.to_string())?;
            let id = ClusterId::System(id);
            txn.insert_system_cluster(id, builtin_cluster.name, &vec![])?;
        }
    }
    Ok(())
}

fn add_new_builtin_cluster_replicas_migration(
    txn: &mut Transaction<'_>,
    bootstrap_args: &BootstrapArgs,
) -> Result<(), catalog::error::Error> {
    let cluster_lookup: BTreeMap<_, _> = txn
        .clusters
        .items()
        .into_iter()
        .map(|(key, value)| (value.name, key.id))
        .collect();

    let replicas: BTreeMap<_, _> =
        txn.cluster_replicas
            .items()
            .into_values()
            .fold(BTreeMap::new(), |mut acc, value| {
                acc.entry(value.cluster_id)
                    .or_insert_with(BTreeSet::new)
                    .insert(value.name);
                acc
            });

    for builtin_replica in &*BUILTIN_CLUSTER_REPLICAS {
        let cluster_id = cluster_lookup
            .get(builtin_replica.cluster_name)
            .expect("builtin cluster replica references non-existent cluster");

        let replica_names = replicas.get(cluster_id);
        if matches!(replica_names, None)
            || matches!(replica_names, Some(names) if !names.contains(builtin_replica.name))
        {
            let replica_id = txn.get_and_increment_id(REPLICA_ID_ALLOC_KEY.to_string())?;
            let config = builtin_cluster_replica_config(bootstrap_args);
            txn.insert_cluster_replica(
                *cluster_id,
                replica_id,
                builtin_replica.name,
                &config,
                MZ_SYSTEM_ROLE_ID,
            )?;
        }
    }
    Ok(())
}

fn default_cluster_replica_config(bootstrap_args: &BootstrapArgs) -> SerializedReplicaConfig {
    SerializedReplicaConfig {
        location: SerializedReplicaLocation::Managed {
            size: bootstrap_args.default_cluster_replica_size.clone(),
            availability_zone: bootstrap_args.default_availability_zone.clone(),
            az_user_specified: false,
        },
        logging: default_logging_config(),
        idle_arrangement_merge_effort: None,
    }
}

fn builtin_cluster_replica_config(bootstrap_args: &BootstrapArgs) -> SerializedReplicaConfig {
    SerializedReplicaConfig {
        location: SerializedReplicaLocation::Managed {
            size: bootstrap_args.builtin_cluster_replica_size.clone(),
            availability_zone: bootstrap_args.default_availability_zone.clone(),
            az_user_specified: false,
        },
        logging: default_logging_config(),
        idle_arrangement_merge_effort: None,
    }
}

fn default_logging_config() -> SerializedReplicaLogging {
    SerializedReplicaLogging {
        log_logging: false,
        interval: Some(Duration::from_secs(1)),
    }
}

pub struct BootstrapArgs {
    pub default_cluster_replica_size: String,
    pub builtin_cluster_replica_size: String,
    pub default_availability_zone: String,
}

#[derive(Debug)]
pub struct Connection {
    stash: Stash,
    boot_ts: mz_repr::Timestamp,
}

impl Connection {
    #[tracing::instrument(name = "storage::open", level = "info", skip_all)]
    pub async fn open(
        mut stash: Stash,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Connection, Error> {
        // The `user_version` field stores the index of the last migration that
        // was run. If the upper is min, the config collection is empty.
        let skip = if is_collection_uninitialized(&mut stash, &COLLECTION_CONFIG).await? {
            0
        } else {
            // An advanced collection must have had its user version set, so the unwrap
            // must succeed.
            COLLECTION_CONFIG
                .peek_key_one(&mut stash, USER_VERSION.to_string())
                .await?
                .expect("user_version must exist")
                .value
                + 1
        };

        // Initialize connection.
        initialize_stash(&mut stash).await?;

        // Choose a time at which to boot. This is the time at which we will run
        // internal migrations, and is also exposed upwards in case higher
        // layers want to run their own migrations at the same timestamp.
        //
        // This time is usually the current system time, but with protection
        // against backwards time jumps, even across restarts.
        let previous_now_ts = try_get_persisted_timestamp(&mut stash, &Timeline::EpochMilliseconds)
            .await?
            .unwrap_or(mz_repr::Timestamp::MIN);
        let boot_ts = timeline::monotonic_now(now, previous_now_ts);

        let mut conn = Connection { stash, boot_ts };

        if !conn.stash.is_readonly() {
            // IMPORTANT: we durably record the new timestamp before using it.
            conn.persist_timestamp(&Timeline::EpochMilliseconds, boot_ts)
                .await?;
            migrate(&mut conn.stash, skip, boot_ts.into(), bootstrap_args).await?;
        }

        Ok(conn)
    }

    pub async fn set_connect_timeout(&mut self, connect_timeout: Duration) {
        self.stash.set_connect_timeout(connect_timeout).await;
    }

    /// Returns the timestamp at which the storage layer booted.
    ///
    /// This is the timestamp that will have been used to write any data during
    /// migrations. It is exposed so that higher layers performing their own
    /// migrations can write data at the same timestamp, if desired.
    ///
    /// The boot timestamp is derived from the durable timestamp oracle and is
    /// guaranteed to never go backwards, even in the face of backwards time
    /// jumps across restarts.
    pub fn boot_ts(&self) -> mz_repr::Timestamp {
        self.boot_ts
    }
}

impl Connection {
    async fn get_setting(&mut self, key: &str) -> Result<Option<String>, Error> {
        Self::get_setting_stash(&mut self.stash, key).await
    }

    async fn set_setting(&mut self, key: &str, value: &str) -> Result<(), Error> {
        Self::set_setting_stash(&mut self.stash, key, value).await
    }

    async fn get_setting_stash(stash: &mut Stash, key: &str) -> Result<Option<String>, Error> {
        let v = COLLECTION_SETTING
            .peek_key_one(
                stash,
                SettingKey {
                    name: key.to_string(),
                },
            )
            .await?;
        Ok(v.map(|v| v.value))
    }

    async fn set_setting_stash<V: Into<String> + std::fmt::Display>(
        stash: &mut Stash,
        key: &str,
        value: V,
    ) -> Result<(), Error> {
        let key = SettingKey {
            name: key.to_string(),
        };
        let value = SettingValue {
            value: value.into(),
        };
        COLLECTION_SETTING
            .upsert(stash, once((key, value)))
            .await
            .map_err(|e| e.into())
    }

    pub async fn get_catalog_content_version(&mut self) -> Result<Option<String>, Error> {
        self.get_setting("catalog_content_version").await
    }

    pub async fn set_catalog_content_version(&mut self, new_version: &str) -> Result<(), Error> {
        self.set_setting("catalog_content_version", new_version)
            .await
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_databases(&mut self) -> Result<Vec<(DatabaseId, String, RoleId)>, Error> {
        Ok(COLLECTION_DATABASE
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| {
                (
                    DatabaseId::new(k.id),
                    v.name,
                    v.owner_id.expect("owner ID not migrated"),
                )
            })
            .collect())
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_schemas(
        &mut self,
    ) -> Result<Vec<(SchemaId, String, Option<DatabaseId>, RoleId)>, Error> {
        Ok(COLLECTION_SCHEMA
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| {
                (
                    SchemaId::new(k.id),
                    v.name,
                    v.database_id.map(DatabaseId::new),
                    v.owner_id.expect("owner ID not migrated"),
                )
            })
            .collect())
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_roles(&mut self) -> Result<Vec<(RoleId, SerializedRole)>, Error> {
        Ok(COLLECTION_ROLE
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| (k.id, v.role))
            .collect())
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_clusters(
        &mut self,
    ) -> Result<Vec<(ClusterId, String, Option<GlobalId>, RoleId)>, Error> {
        Ok(COLLECTION_CLUSTERS
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| {
                (
                    k.id,
                    v.name,
                    v.linked_object_id,
                    v.owner_id.expect("owner ID not migrated"),
                )
            })
            .collect())
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_cluster_replicas(
        &mut self,
    ) -> Result<
        Vec<(
            ClusterId,
            ReplicaId,
            String,
            SerializedReplicaConfig,
            RoleId,
        )>,
        Error,
    > {
        Ok(COLLECTION_CLUSTER_REPLICAS
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| {
                (
                    v.cluster_id,
                    k.id,
                    v.name,
                    v.config,
                    v.owner_id.expect("owner ID not migrated"),
                )
            })
            .collect())
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_audit_log(&mut self) -> Result<impl Iterator<Item = VersionedEvent>, Error> {
        Ok(COLLECTION_AUDIT_LOG
            .peek_one(&mut self.stash)
            .await?
            .into_keys()
            .map(|ev| ev.event))
    }

    /// Loads storage usage events and permanently deletes from the stash those
    /// that happened more than the retention period ago from boot_ts.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn fetch_and_prune_storage_usage(
        &mut self,
        retention_period: Option<Duration>,
    ) -> Result<Vec<VersionedStorageUsage>, Error> {
        // If no usage retention period is set, set the cutoff to MIN so nothing
        // is removed.
        let cutoff_ts = match retention_period {
            None => u128::MIN,
            Some(period) => u128::from(self.boot_ts).saturating_sub(period.as_millis()),
        };
        Ok(self
            .stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    let collection = COLLECTION_STORAGE_USAGE.from_tx(&tx).await?;
                    let rows = tx.peek_one(collection).await?;
                    let mut events = Vec::with_capacity(rows.len());
                    let mut batch = collection.make_batch_tx(&tx).await?;
                    for ev in rows.into_keys() {
                        if u128::from(ev.metric.timestamp()) >= cutoff_ts {
                            events.push(ev.metric);
                        } else if retention_period.is_some() {
                            collection.append_to_batch(&mut batch, &ev, &(), -1);
                        }
                    }
                    // Delete things only if a retention period is
                    // specified (otherwise opening readonly catalogs
                    // can fail).
                    if retention_period.is_some() {
                        tx.append(vec![batch]).await?;
                    }
                    Ok(events)
                })
            })
            .await?)
    }

    /// Load the persisted mapping of system object to global ID. Key is (schema-name, object-name).
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_system_gids(
        &mut self,
    ) -> Result<BTreeMap<(String, CatalogItemType, String), (GlobalId, String)>, Error> {
        Ok(COLLECTION_SYSTEM_GID_MAPPING
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| {
                (
                    (k.schema_name, k.object_type, k.object_name),
                    (GlobalId::System(v.id), v.fingerprint),
                )
            })
            .collect())
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_introspection_source_index_gids(
        &mut self,
        cluster_id: ClusterId,
    ) -> Result<BTreeMap<String, GlobalId>, Error> {
        Ok(COLLECTION_CLUSTER_INTROSPECTION_SOURCE_INDEX
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .filter_map(|(k, v)| {
                if k.cluster_id == cluster_id {
                    Some((k.name, GlobalId::System(v.index_id)))
                } else {
                    None
                }
            })
            .collect())
    }

    /// Load the persisted server configurations.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_system_configuration(&mut self) -> Result<BTreeMap<String, String>, Error> {
        COLLECTION_SYSTEM_CONFIGURATION
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| Ok((k.name, v.value)))
            .collect()
    }

    /// Persist mapping from system objects to global IDs and fingerprints.
    ///
    /// Panics if provided id is not a system id.
    pub async fn set_system_object_mapping(
        &mut self,
        mappings: Vec<SystemObjectMapping>,
    ) -> Result<(), Error> {
        if mappings.is_empty() {
            return Ok(());
        }

        let mappings = mappings.into_iter().map(
            |SystemObjectMapping {
                 schema_name,
                 object_type,
                 object_name,
                 id,
                 fingerprint,
             }| {
                let id = if let GlobalId::System(id) = id {
                    id
                } else {
                    panic!("non-system id provided")
                };
                (
                    GidMappingKey {
                        schema_name,
                        object_type,
                        object_name,
                    },
                    GidMappingValue { id, fingerprint },
                )
            },
        );
        COLLECTION_SYSTEM_GID_MAPPING
            .upsert(&mut self.stash, mappings)
            .await
            .map_err(|e| e.into())
    }

    /// Panics if provided id is not a system id
    pub async fn set_introspection_source_index_gids(
        &mut self,
        mappings: Vec<(ClusterId, &str, GlobalId)>,
    ) -> Result<(), Error> {
        if mappings.is_empty() {
            return Ok(());
        }

        let mappings = mappings.into_iter().map(|(cluster_id, name, index_id)| {
            let index_id = if let GlobalId::System(id) = index_id {
                id
            } else {
                panic!("non-system id provided")
            };
            (
                ClusterIntrospectionSourceIndexKey {
                    cluster_id,
                    name: name.to_string(),
                },
                ClusterIntrospectionSourceIndexValue { index_id },
            )
        });
        COLLECTION_CLUSTER_INTROSPECTION_SOURCE_INDEX
            .upsert(&mut self.stash, mappings)
            .await
            .map_err(|e| e.into())
    }

    /// Set the configuration of a replica.
    /// This accepts only one item, as we currently use this only for the default cluster
    pub async fn set_replica_config(
        &mut self,
        replica_id: ReplicaId,
        cluster_id: ClusterId,
        name: String,
        config: &ReplicaConfig,
        owner_id: RoleId,
    ) -> Result<(), Error> {
        let key = ClusterReplicaKey { id: replica_id };
        let val = ClusterReplicaValue {
            cluster_id,
            name,
            config: config.clone().into(),
            owner_id: Some(owner_id),
        };
        COLLECTION_CLUSTER_REPLICAS
            .upsert_key(&mut self.stash, key, |_| Ok::<_, Error>(val))
            .await??;
        Ok(())
    }

    pub async fn allocate_system_ids(&mut self, amount: u64) -> Result<Vec<GlobalId>, Error> {
        let id = self.allocate_id("system", amount).await?;

        Ok(id.into_iter().map(GlobalId::System).collect())
    }

    pub async fn allocate_user_id(&mut self) -> Result<GlobalId, Error> {
        let id = self.allocate_id("user", 1).await?;
        let id = id.into_element();
        Ok(GlobalId::User(id))
    }

    pub async fn allocate_system_cluster_id(&mut self) -> Result<ClusterId, Error> {
        let id = self.allocate_id(SYSTEM_CLUSTER_ID_ALLOC_KEY, 1).await?;
        let id = id.into_element();
        Ok(ClusterId::System(id))
    }

    pub async fn allocate_user_cluster_id(&mut self) -> Result<ClusterId, Error> {
        let id = self.allocate_id(USER_CLUSTER_ID_ALLOC_KEY, 1).await?;
        let id = id.into_element();
        Ok(ClusterId::User(id))
    }

    pub async fn allocate_replica_id(&mut self) -> Result<ReplicaId, Error> {
        let id = self.allocate_id(REPLICA_ID_ALLOC_KEY, 1).await?;
        Ok(id.into_element())
    }

    /// Get the next user id without allocating it.
    pub async fn get_next_user_global_id(&mut self) -> Result<GlobalId, Error> {
        self.get_next_id("user").await.map(GlobalId::User)
    }

    /// Get the next replica id without allocating it.
    pub async fn get_next_replica_id(&mut self) -> Result<ReplicaId, Error> {
        self.get_next_id(REPLICA_ID_ALLOC_KEY).await
    }

    async fn get_next_id(&mut self, id_type: &str) -> Result<u64, Error> {
        COLLECTION_ID_ALLOC
            .peek_key_one(
                &mut self.stash,
                IdAllocKey {
                    name: id_type.to_string(),
                },
            )
            .await
            .map(|x| x.expect("must exist").next_id)
            .map_err(Into::into)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn allocate_id(&mut self, id_type: &str, amount: u64) -> Result<Vec<u64>, Error> {
        if amount == 0 {
            return Ok(Vec::new());
        }
        let key = IdAllocKey {
            name: id_type.to_string(),
        };
        let (prev, next) = COLLECTION_ID_ALLOC
            .upsert_key(&mut self.stash, key, move |prev| {
                let id = prev.expect("must exist").next_id;
                match id.checked_add(amount) {
                    Some(next_gid) => Ok(IdAllocValue { next_id: next_gid }),
                    None => Err(Error::new(ErrorKind::IdExhaustion)),
                }
            })
            .await??;
        let id = prev.expect("must exist").next_id;
        Ok((id..next.next_id).collect())
    }

    /// Get all global timestamps that has been persisted to disk.
    pub async fn get_all_persisted_timestamps(
        &mut self,
    ) -> Result<BTreeMap<Timeline, mz_repr::Timestamp>, Error> {
        Ok(COLLECTION_TIMESTAMP
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| (k.id.parse().expect("invalid timeline persisted"), v.ts))
            .collect())
    }

    /// Persist new global timestamp for a timeline to disk.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn persist_timestamp(
        &mut self,
        timeline: &Timeline,
        timestamp: mz_repr::Timestamp,
    ) -> Result<(), Error> {
        let key = TimestampKey {
            id: timeline.to_string(),
        };
        let (prev, next) = COLLECTION_TIMESTAMP
            .upsert_key(&mut self.stash, key, move |_| {
                Ok::<_, Error>(TimestampValue { ts: timestamp })
            })
            .await??;
        if let Some(prev) = prev {
            assert!(next >= prev, "global timestamp must always go up");
        }
        Ok(())
    }

    pub async fn transaction<'a>(&'a mut self) -> Result<Transaction<'a>, Error> {
        transaction(&mut self.stash).await
    }

    pub async fn confirm_leadership(&mut self) -> Result<(), Error> {
        Ok(self.stash.confirm_leadership().await?)
    }

    pub async fn consolidate(&mut self, collections: &[mz_stash::Id]) -> Result<(), Error> {
        Ok(self.stash.consolidate_batch(collections).await?)
    }
}

/// Gets a global timestamp for a timeline that has been persisted to disk.
///
/// Returns `None` if no persisted timestamp for the specified timeline exists.
async fn try_get_persisted_timestamp(
    stash: &mut Stash,
    timeline: &Timeline,
) -> Result<Option<mz_repr::Timestamp>, Error>
where
{
    let key = TimestampKey {
        id: timeline.to_string(),
    };
    Ok(COLLECTION_TIMESTAMP
        .peek_key_one(stash, key)
        .await?
        .map(|v| v.ts))
}

#[tracing::instrument(name = "storage::transaction", level = "debug", skip_all)]
pub async fn transaction<'a>(stash: &'a mut Stash) -> Result<Transaction<'a>, Error> {
    let (
        databases,
        schemas,
        roles,
        items,
        clusters,
        cluster_replicas,
        introspection_sources,
        id_allocator,
        configs,
        settings,
        timestamps,
        system_gid_mapping,
        system_configurations,
    ) = stash
        .with_transaction(|tx| {
            Box::pin(async move {
                // Peek the catalog collections in any order and a single transaction.
                futures::try_join!(
                    tx.peek_one(tx.collection(COLLECTION_DATABASE.name()).await?),
                    tx.peek_one(tx.collection(COLLECTION_SCHEMA.name()).await?),
                    tx.peek_one(tx.collection(COLLECTION_ROLE.name()).await?),
                    tx.peek_one(tx.collection(COLLECTION_ITEM.name()).await?),
                    tx.peek_one(tx.collection(COLLECTION_CLUSTERS.name()).await?),
                    tx.peek_one(tx.collection(COLLECTION_CLUSTER_REPLICAS.name()).await?),
                    tx.peek_one(
                        tx.collection(COLLECTION_CLUSTER_INTROSPECTION_SOURCE_INDEX.name())
                            .await?,
                    ),
                    tx.peek_one(tx.collection(COLLECTION_ID_ALLOC.name()).await?),
                    tx.peek_one(tx.collection(COLLECTION_CONFIG.name()).await?),
                    tx.peek_one(tx.collection(COLLECTION_SETTING.name()).await?),
                    tx.peek_one(tx.collection(COLLECTION_TIMESTAMP.name()).await?),
                    tx.peek_one(tx.collection(COLLECTION_SYSTEM_GID_MAPPING.name()).await?),
                    tx.peek_one(
                        tx.collection(COLLECTION_SYSTEM_CONFIGURATION.name())
                            .await?,
                    )
                )
            })
        })
        .await?;

    Ok(Transaction {
        stash,
        databases: TableTransaction::new(databases, |a, b| a.name == b.name),
        schemas: TableTransaction::new(schemas, |a, b| {
            a.database_id == b.database_id && a.name == b.name
        }),
        items: TableTransaction::new(items, |a, b| a.schema_id == b.schema_id && a.name == b.name),
        roles: TableTransaction::new(roles, |a, b| a.role.name == b.role.name),
        clusters: TableTransaction::new(clusters, |a, b| a.name == b.name),
        cluster_replicas: TableTransaction::new(cluster_replicas, |a, b| {
            a.cluster_id == b.cluster_id && a.name == b.name
        }),
        introspection_sources: TableTransaction::new(introspection_sources, |_a, _b| false),
        id_allocator: TableTransaction::new(id_allocator, |_a, _b| false),
        configs: TableTransaction::new(configs, |_a, _b| false),
        settings: TableTransaction::new(settings, |_a, _b| false),
        timestamps: TableTransaction::new(timestamps, |_a, _b| false),
        system_gid_mapping: TableTransaction::new(system_gid_mapping, |_a, _b| false),
        system_configurations: TableTransaction::new(system_configurations, |_a, _b| false),
        audit_log_updates: Vec::new(),
        storage_usage_updates: Vec::new(),
    })
}

pub struct Transaction<'a> {
    stash: &'a mut Stash,
    databases: TableTransaction<DatabaseKey, DatabaseValue>,
    schemas: TableTransaction<SchemaKey, SchemaValue>,
    items: TableTransaction<ItemKey, ItemValue>,
    roles: TableTransaction<RoleKey, RoleValue>,
    clusters: TableTransaction<ClusterKey, ClusterValue>,
    cluster_replicas: TableTransaction<ClusterReplicaKey, ClusterReplicaValue>,
    introspection_sources:
        TableTransaction<ClusterIntrospectionSourceIndexKey, ClusterIntrospectionSourceIndexValue>,
    id_allocator: TableTransaction<IdAllocKey, IdAllocValue>,
    configs: TableTransaction<String, ConfigValue>,
    settings: TableTransaction<SettingKey, SettingValue>,
    timestamps: TableTransaction<TimestampKey, TimestampValue>,
    system_gid_mapping: TableTransaction<GidMappingKey, GidMappingValue>,
    system_configurations: TableTransaction<ServerConfigurationKey, ServerConfigurationValue>,
    // Don't make this a table transaction so that it's not read into the stash
    // memory cache.
    audit_log_updates: Vec<(AuditLogKey, (), i64)>,
    storage_usage_updates: Vec<(StorageUsageKey, (), i64)>,
}

impl<'a> Transaction<'a> {
    pub fn loaded_items(
        &self,
    ) -> Vec<(GlobalId, QualifiedObjectName, SerializedCatalogItem, RoleId)> {
        let databases = self.databases.items();
        let schemas = self.schemas.items();
        let mut items = Vec::new();
        self.items.for_values(|k, v| {
            let schema = match schemas.get(&SchemaKey { id: v.schema_id }) {
                Some(schema) => schema,
                None => panic!(
                    "corrupt stash! unknown schema id {}, for item with key \
                        {k:?} and value {v:?}",
                    v.schema_id
                ),
            };
            let database_spec = match schema.database_id {
                Some(id) => {
                    if databases.get(&DatabaseKey { id }).is_none() {
                        panic!(
                            "corrupt stash! unknown database id {id}, for item with key \
                        {k:?} and value {v:?}"
                        );
                    }
                    ResolvedDatabaseSpecifier::from(id)
                }
                None => ResolvedDatabaseSpecifier::Ambient,
            };
            items.push((
                k.gid,
                QualifiedObjectName {
                    qualifiers: ObjectQualifiers {
                        database_spec,
                        schema_spec: SchemaSpecifier::from(v.schema_id),
                    },
                    item: v.name.clone(),
                },
                v.definition.clone(),
                v.owner_id.expect("owner ID not migrated"),
            ));
        });
        items.sort_by_key(|(id, _, _, _)| *id);
        items
    }

    pub fn insert_audit_log_event(&mut self, event: VersionedEvent) {
        self.audit_log_updates.push((AuditLogKey { event }, (), 1));
    }

    pub fn insert_storage_usage_event(&mut self, metric: VersionedStorageUsage) {
        self.storage_usage_updates
            .push((StorageUsageKey { metric }, (), 1));
    }

    pub fn insert_database(
        &mut self,
        database_name: &str,
        owner_id: RoleId,
    ) -> Result<DatabaseId, Error> {
        let id = self.get_and_increment_id(DATABASE_ID_ALLOC_KEY.to_string())?;
        match self.databases.insert(
            DatabaseKey { id },
            DatabaseValue {
                name: database_name.to_string(),
                owner_id: Some(owner_id),
            },
        ) {
            Ok(_) => Ok(DatabaseId::new(id)),
            Err(_) => Err(Error::new(ErrorKind::DatabaseAlreadyExists(
                database_name.to_owned(),
            ))),
        }
    }

    pub fn insert_schema(
        &mut self,
        database_id: DatabaseId,
        schema_name: &str,
        owner_id: RoleId,
    ) -> Result<SchemaId, Error> {
        let id = self.get_and_increment_id(SCHEMA_ID_ALLOC_KEY.to_string())?;
        match self.schemas.insert(
            SchemaKey { id },
            SchemaValue {
                database_id: Some(database_id.0),
                name: schema_name.to_string(),
                owner_id: Some(owner_id),
            },
        ) {
            Ok(_) => Ok(SchemaId::new(id)),
            Err(_) => Err(Error::new(ErrorKind::SchemaAlreadyExists(
                schema_name.to_owned(),
            ))),
        }
    }

    pub fn insert_user_role(&mut self, role: SerializedRole) -> Result<RoleId, Error> {
        let id = self.get_and_increment_id(USER_ROLE_ID_ALLOC_KEY.to_string())?;
        let id = RoleId::User(id);
        let name = role.name.clone();
        match self.roles.insert(RoleKey { id }, RoleValue { role }) {
            Ok(_) => Ok(id),
            Err(_) => Err(Error::new(ErrorKind::RoleAlreadyExists(name))),
        }
    }

    /// Panics if any introspection source id is not a system id
    pub fn insert_user_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        linked_object_id: Option<GlobalId>,
        introspection_source_indexes: &Vec<(&'static BuiltinLog, GlobalId)>,
        owner_id: RoleId,
    ) -> Result<(), Error> {
        self.insert_cluster(
            cluster_id,
            cluster_name,
            linked_object_id,
            introspection_source_indexes,
            owner_id,
        )
    }

    /// Panics if any introspection source id is not a system id
    pub fn insert_system_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        introspection_source_indexes: &Vec<(&'static BuiltinLog, GlobalId)>,
    ) -> Result<(), Error> {
        self.insert_cluster(
            cluster_id,
            cluster_name,
            None,
            introspection_source_indexes,
            MZ_SYSTEM_ROLE_ID,
        )
    }

    fn insert_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        linked_object_id: Option<GlobalId>,
        introspection_source_indexes: &Vec<(&'static BuiltinLog, GlobalId)>,
        owner_id: RoleId,
    ) -> Result<(), Error> {
        if let Err(_) = self.clusters.insert(
            ClusterKey { id: cluster_id },
            ClusterValue {
                name: cluster_name.to_string(),
                linked_object_id,
                owner_id: Some(owner_id),
            },
        ) {
            return Err(Error::new(ErrorKind::ClusterAlreadyExists(
                cluster_name.to_owned(),
            )));
        };

        for (builtin, index_id) in introspection_source_indexes {
            let index_id = if let GlobalId::System(id) = index_id {
                *id
            } else {
                panic!("non-system id provided")
            };
            self.introspection_sources
                .insert(
                    ClusterIntrospectionSourceIndexKey {
                        cluster_id,
                        name: builtin.name.to_string(),
                    },
                    ClusterIntrospectionSourceIndexValue { index_id },
                )
                .expect("no uniqueness violation");
        }

        Ok(())
    }

    pub fn insert_cluster_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        replica_name: &str,
        config: &SerializedReplicaConfig,
        owner_id: RoleId,
    ) -> Result<(), Error> {
        if let Err(_) = self.cluster_replicas.insert(
            ClusterReplicaKey { id: replica_id },
            ClusterReplicaValue {
                cluster_id,
                name: replica_name.into(),
                config: config.clone(),
                owner_id: Some(owner_id),
            },
        ) {
            let cluster = self
                .clusters
                .get(&ClusterKey { id: cluster_id })
                .expect("cluster exists");
            return Err(Error::new(ErrorKind::DuplicateReplica(
                replica_name.to_string(),
                cluster.name.to_string(),
            )));
        };
        Ok(())
    }

    /// Updates persisted information about persisted introspection source
    /// indexes.
    ///
    /// Panics if provided id is not a system id.
    pub fn update_introspection_source_index_gids(
        &mut self,
        mappings: impl Iterator<Item = (ClusterId, impl Iterator<Item = (String, GlobalId)>)>,
    ) -> Result<(), Error> {
        for (cluster_id, updates) in mappings {
            for (name, id) in updates {
                let index_id = if let GlobalId::System(index_id) = id {
                    index_id
                } else {
                    panic!("Introspection source index should have a system id")
                };
                let prev = self.introspection_sources.set(
                    ClusterIntrospectionSourceIndexKey { cluster_id, name },
                    Some(ClusterIntrospectionSourceIndexValue { index_id }),
                )?;
                if prev.is_none() {
                    return Err(Error {
                        kind: ErrorKind::FailedBuiltinSchemaMigration(format!("{id}")),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn insert_item(
        &mut self,
        id: GlobalId,
        schema_id: SchemaId,
        item_name: &str,
        item: SerializedCatalogItem,
        owner_id: RoleId,
    ) -> Result<(), Error> {
        match self.items.insert(
            ItemKey { gid: id },
            ItemValue {
                schema_id: schema_id.0,
                name: item_name.to_string(),
                definition: item,
                owner_id: Some(owner_id),
            },
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::new(ErrorKind::ItemAlreadyExists(
                id,
                item_name.to_owned(),
            ))),
        }
    }

    pub fn get_and_increment_id(&mut self, key: String) -> Result<u64, Error> {
        let id = self
            .id_allocator
            .items()
            .get(&IdAllocKey { name: key.clone() })
            .unwrap_or_else(|| panic!("{key} id allocator missing"))
            .next_id;
        let next_id = id
            .checked_add(1)
            .ok_or_else(|| Error::new(ErrorKind::IdExhaustion))?;
        let prev = self
            .id_allocator
            .set(IdAllocKey { name: key }, Some(IdAllocValue { next_id }))?;
        assert!(prev.is_some());
        Ok(id)
    }

    pub fn remove_database(&mut self, id: &DatabaseId) -> Result<(), Error> {
        let prev = self.databases.set(DatabaseKey { id: id.0 }, None)?;
        if prev.is_some() {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownDatabase(id.to_string()).into())
        }
    }

    pub fn remove_schema(
        &mut self,
        database_id: &DatabaseId,
        schema_id: &SchemaId,
    ) -> Result<(), Error> {
        let prev = self.schemas.set(SchemaKey { id: schema_id.0 }, None)?;
        if prev.is_some() {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownSchema(format!("{}.{}", database_id.0, schema_id.0)).into())
        }
    }

    pub fn remove_role(&mut self, name: &str) -> Result<(), Error> {
        let roles = self.roles.delete(|_k, v| v.role.name == name);
        assert!(
            roles.iter().all(|(k, _)| k.id.is_user()),
            "cannot delete non-user roles"
        );
        let n = roles.len();
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownRole(name.to_owned()).into())
        }
    }

    pub fn remove_cluster(&mut self, id: ClusterId) -> Result<(), Error> {
        let deleted = self.clusters.delete(|k, _v| k.id == id);
        if deleted.is_empty() {
            Err(SqlCatalogError::UnknownCluster(id.to_string()).into())
        } else {
            assert_eq!(deleted.len(), 1);
            // Cascade delete introspection sources and cluster replicas.
            //
            // TODO(benesch): this doesn't seem right. Cascade deletions should
            // be entirely the domain of the higher catalog layer, not the
            // storage layer.
            self.cluster_replicas.delete(|_k, v| v.cluster_id == id);
            self.introspection_sources
                .delete(|k, _v| k.cluster_id == id);
            Ok(())
        }
    }

    pub fn remove_cluster_replica(&mut self, id: ReplicaId) -> Result<(), Error> {
        let deleted = self.cluster_replicas.delete(|k, _v| k.id == id);
        if deleted.len() == 1 {
            Ok(())
        } else {
            assert!(deleted.is_empty());
            Err(SqlCatalogError::UnknownClusterReplica(id.to_string()).into())
        }
    }

    /// Removes item `id` from the transaction.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of items in the stash.
    /// DO NOT call this function in a loop, use [`Self::remove_items`] instead.
    pub fn remove_item(&mut self, id: GlobalId) -> Result<(), Error> {
        let prev = self.items.set(ItemKey { gid: id }, None)?;
        if prev.is_some() {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownItem(id.to_string()).into())
        }
    }

    /// Removes all items in `ids` from the transaction.
    ///
    /// Returns an error if any id in `ids` is not found.
    ///
    /// NOTE: On error, there still may be some items removed from the transaction. It is
    /// up to the called to either abort the transaction or commit.
    pub fn remove_items(&mut self, ids: BTreeSet<GlobalId>) -> Result<(), Error> {
        let n = self.items.delete(|k, _v| ids.contains(&k.gid)).len();
        if n == ids.len() {
            Ok(())
        } else {
            let item_gids = self.items.items().keys().map(|k| k.gid).collect();
            let mut unknown = ids.difference(&item_gids);
            Err(SqlCatalogError::UnknownItem(unknown.join(", ")).into())
        }
    }

    /// Updates item `id` in the transaction to `item_name` and `item`.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of items in the stash.
    /// DO NOT call this function in a loop, use [`Self::update_items`] instead.
    pub fn update_item(
        &mut self,
        id: GlobalId,
        item_name: &str,
        item: &SerializedCatalogItem,
    ) -> Result<(), Error> {
        let n = self.items.update(|k, v| {
            if k.gid == id {
                Some(ItemValue {
                    schema_id: v.schema_id,
                    name: item_name.to_string(),
                    definition: item.clone(),
                    owner_id: v.owner_id,
                })
            } else {
                None
            }
        })?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownItem(id.to_string()).into())
        }
    }

    /// Updates all items with ids matching the keys of `items` in the transaction, to the
    /// corresponding value in `items`.
    ///
    /// Returns an error if any id in `items` is not found.
    ///
    /// NOTE: On error, there still may be some items updated in the transaction. It is
    /// up to the called to either abort the transaction or commit.
    pub fn update_items(
        &mut self,
        items: BTreeMap<GlobalId, (String, SerializedCatalogItem)>,
    ) -> Result<(), Error> {
        let n = self.items.update(|k, v| {
            if let Some((item_name, item)) = items.get(&k.gid) {
                Some(ItemValue {
                    schema_id: v.schema_id,
                    name: item_name.clone(),
                    definition: item.clone(),
                    owner_id: v.owner_id,
                })
            } else {
                None
            }
        })?;
        let n = usize::try_from(n).expect("Must be positive and fit in usize");
        if n == items.len() {
            Ok(())
        } else {
            let update_ids: BTreeSet<_> = items.into_keys().collect();
            let item_ids: BTreeSet<_> = self.items.items().keys().map(|k| k.gid).collect();
            let mut unknown = update_ids.difference(&item_ids);
            Err(SqlCatalogError::UnknownItem(unknown.join(", ")).into())
        }
    }

    /// Updates role `id` in the transaction to `role`.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of items in the stash.
    /// DO NOT call this function in a loop, implement and use some `Self::update_roles` instead.
    /// You should model it after [`Self::update_items`].
    pub fn update_role(&mut self, id: RoleId, role: SerializedRole) -> Result<(), Error> {
        let n = self.roles.update(move |k, _v| {
            if k.id == id {
                Some(RoleValue { role: role.clone() })
            } else {
                None
            }
        })?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownItem(id.to_string()).into())
        }
    }

    pub fn update_user_version(&mut self, version: u64) -> Result<(), Error> {
        let prev = self.configs.set(
            USER_VERSION.to_string(),
            Some(ConfigValue { value: version }),
        )?;
        assert!(prev.is_some());
        Ok(())
    }

    /// Updates persisted mapping from system objects to global IDs and fingerprints. Each element
    /// of `mappings` should be (old-global-id, new-system-object-mapping).
    ///
    /// Panics if provided id is not a system id.
    pub fn update_system_object_mappings(
        &mut self,
        mappings: BTreeMap<GlobalId, SystemObjectMapping>,
    ) -> Result<(), Error> {
        let n = self.system_gid_mapping.update(|_k, v| {
            if let Some(mapping) = mappings.get(&GlobalId::System(v.id)) {
                let id = if let GlobalId::System(id) = mapping.id {
                    id
                } else {
                    panic!("non-system id provided")
                };
                Some(GidMappingValue {
                    id,
                    fingerprint: mapping.fingerprint.clone(),
                })
            } else {
                None
            }
        })?;

        if usize::try_from(n).expect("update diff should fit into usize") != mappings.len() {
            let id_str = mappings.keys().map(|id| id.to_string()).join(",");
            return Err(Error {
                kind: ErrorKind::FailedBuiltinSchemaMigration(id_str),
            });
        }

        Ok(())
    }

    /// Upserts persisted system configuration `name` to `value`.
    pub fn upsert_system_config(&mut self, name: &str, value: String) -> Result<(), Error> {
        let key = ServerConfigurationKey {
            name: name.to_string(),
        };
        let value = ServerConfigurationValue { value };
        self.system_configurations.set(key, Some(value))?;
        Ok(())
    }

    /// Removes persisted system configuration `name`.
    pub fn remove_system_config(&mut self, name: &str) {
        let key = ServerConfigurationKey {
            name: name.to_string(),
        };
        self.system_configurations
            .set(key, None)
            .expect("cannot have uniqueness violation");
    }

    /// Removes all persisted system configurations.
    pub fn clear_system_configs(&mut self) {
        self.system_configurations.delete(|_k, _v| true);
    }

    pub fn remove_timestamp(&mut self, timeline: Timeline) {
        let timeline_str = timeline.to_string();
        let prev = self
            .timestamps
            .set(TimestampKey { id: timeline_str }, None)
            .expect("cannot have uniqueness violation");
        assert!(prev.is_some());
    }

    /// Commits the storage transaction to the stash. Any error returned
    /// indicates the stash may be in an indeterminate state and needs to be
    /// fully re-read before proceeding. In general, this must be fatal to the
    /// calling process. We do not panic/halt inside this function itself so
    /// that errors can bubble up during initialization.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn commit(self) -> Result<(), Error> {
        async fn add_batch<'tx, K, V>(
            tx: &'tx mz_stash::Transaction<'tx>,
            batches: &mut Vec<AppendBatch>,
            typed: &TypedCollection<K, V>,
            changes: &[(K, V, mz_stash::Diff)],
        ) -> Result<(), StashError>
        where
            K: mz_stash::Data + 'tx,
            V: mz_stash::Data + 'tx,
        {
            if changes.is_empty() {
                return Ok(());
            }
            let collection = tx.collection::<K, V>(typed.name()).await?;
            let upper = tx.upper(collection.id).await?;
            let mut batch = collection.make_batch_lower(upper)?;
            for (k, v, diff) in changes {
                collection.append_to_batch(&mut batch, k, v, *diff);
            }
            batches.push(batch);
            Ok(())
        }

        // The with_transaction fn below requires a Fn that can be cloned,
        // meaning anything it closes over must be Clone. The .pending() here
        // return Vecs. Thus, the Arcs here aren't strictly necessary because
        // Vecs are Clone. However, using an Arc means that we never clone the
        // Vec (which would happen at least one time when the txn starts), and
        // instead only clone the Arc.
        let databases = Arc::new(self.databases.pending());
        let schemas = Arc::new(self.schemas.pending());
        let items = Arc::new(self.items.pending());
        let roles = Arc::new(self.roles.pending());
        let clusters = Arc::new(self.clusters.pending());
        let cluster_replicas = Arc::new(self.cluster_replicas.pending());
        let introspection_sources = Arc::new(self.introspection_sources.pending());
        let id_allocator = Arc::new(self.id_allocator.pending());
        let configs = Arc::new(self.configs.pending());
        let settings = Arc::new(self.settings.pending());
        let timestamps = Arc::new(self.timestamps.pending());
        let system_gid_mapping = Arc::new(self.system_gid_mapping.pending());
        let system_configurations = Arc::new(self.system_configurations.pending());
        let audit_log_updates = Arc::new(self.audit_log_updates);
        let storage_usage_updates = Arc::new(self.storage_usage_updates);

        self.stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    let mut batches = Vec::new();
                    add_batch(&tx, &mut batches, &COLLECTION_DATABASE, &databases).await?;
                    add_batch(&tx, &mut batches, &COLLECTION_SCHEMA, &schemas).await?;
                    add_batch(&tx, &mut batches, &COLLECTION_ITEM, &items).await?;
                    add_batch(&tx, &mut batches, &COLLECTION_ROLE, &roles).await?;
                    add_batch(&tx, &mut batches, &COLLECTION_CLUSTERS, &clusters).await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &COLLECTION_CLUSTER_REPLICAS,
                        &cluster_replicas,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &COLLECTION_CLUSTER_INTROSPECTION_SOURCE_INDEX,
                        &introspection_sources,
                    )
                    .await?;
                    add_batch(&tx, &mut batches, &COLLECTION_ID_ALLOC, &id_allocator).await?;
                    add_batch(&tx, &mut batches, &COLLECTION_CONFIG, &configs).await?;
                    add_batch(&tx, &mut batches, &COLLECTION_SETTING, &settings).await?;
                    add_batch(&tx, &mut batches, &COLLECTION_TIMESTAMP, &timestamps).await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &COLLECTION_SYSTEM_GID_MAPPING,
                        &system_gid_mapping,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &COLLECTION_SYSTEM_CONFIGURATION,
                        &system_configurations,
                    )
                    .await?;
                    add_batch(&tx, &mut batches, &COLLECTION_AUDIT_LOG, &audit_log_updates).await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &COLLECTION_STORAGE_USAGE,
                        &storage_usage_updates,
                    )
                    .await?;
                    tx.append(batches).await?;
                    Ok(())
                })
            })
            .await?;
        Ok(())
    }
}

/// Returns true if collection is uninitialized, false otherwise.
pub async fn is_collection_uninitialized<K, V>(
    stash: &mut Stash,
    collection: &TypedCollection<K, V>,
) -> Result<bool, Error>
where
    K: mz_stash::Data,
    V: mz_stash::Data,
{
    Ok(collection.upper(stash).await?.elements() == [mz_stash::Timestamp::MIN])
}

/// Inserts empty values into all new collections, so the collections are readable.
#[tracing::instrument(level = "info", skip_all)]
pub async fn initialize_stash(stash: &mut Stash) -> Result<(), Error> {
    async fn add_batch<'tx, K, V>(
        tx: &'tx mz_stash::Transaction<'tx>,
        typed: &TypedCollection<K, V>,
    ) -> Result<Option<AppendBatch>, StashError>
    where
        K: mz_stash::Data,
        V: mz_stash::Data,
    {
        let collection = tx.collection::<K, V>(typed.name()).await?;
        let upper = tx.upper(collection.id).await?;
        if upper.elements() == [mz_stash::Timestamp::MIN] {
            Ok(Some(collection.make_batch_lower(upper)?))
        } else {
            Ok(None)
        }
    }

    stash
        .with_transaction(move |tx| {
            Box::pin(async move {
                // Query all collections in parallel. Makes for triplicated
                // names, but runs quick.
                let (
                    config,
                    setting,
                    id_alloc,
                    system_gid_mapping,
                    clusters,
                    cluster_introspection,
                    cluster_replicas,
                    database,
                    schema,
                    item,
                    role,
                    timestamp,
                    system_configuration,
                    audit_log,
                    storage_usage,
                ) = futures::try_join!(
                    add_batch(&tx, &COLLECTION_CONFIG),
                    add_batch(&tx, &COLLECTION_SETTING),
                    add_batch(&tx, &COLLECTION_ID_ALLOC),
                    add_batch(&tx, &COLLECTION_SYSTEM_GID_MAPPING),
                    add_batch(&tx, &COLLECTION_CLUSTERS),
                    add_batch(&tx, &COLLECTION_CLUSTER_INTROSPECTION_SOURCE_INDEX),
                    add_batch(&tx, &COLLECTION_CLUSTER_REPLICAS),
                    add_batch(&tx, &COLLECTION_DATABASE),
                    add_batch(&tx, &COLLECTION_SCHEMA),
                    add_batch(&tx, &COLLECTION_ITEM),
                    add_batch(&tx, &COLLECTION_ROLE),
                    add_batch(&tx, &COLLECTION_TIMESTAMP),
                    add_batch(&tx, &COLLECTION_SYSTEM_CONFIGURATION),
                    add_batch(&tx, &COLLECTION_AUDIT_LOG),
                    add_batch(&tx, &COLLECTION_STORAGE_USAGE),
                )?;
                let batches: Vec<AppendBatch> = [
                    config,
                    setting,
                    id_alloc,
                    system_gid_mapping,
                    clusters,
                    cluster_introspection,
                    cluster_replicas,
                    database,
                    schema,
                    item,
                    role,
                    timestamp,
                    system_configuration,
                    audit_log,
                    storage_usage,
                ]
                .into_iter()
                .filter_map(|b| b)
                .collect();
                tx.append(batches).await?;
                Ok(())
            })
        })
        .await
        .map_err(|err| err.into())
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SettingKey {
    name: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct SettingValue {
    value: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct IdAllocKey {
    name: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct IdAllocValue {
    next_id: u64,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct GidMappingKey {
    schema_name: String,
    object_type: CatalogItemType,
    object_name: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct GidMappingValue {
    id: u64,
    fingerprint: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterKey {
    id: ClusterId,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterValue {
    name: String,
    linked_object_id: Option<GlobalId>,
    // TODO(jkosh44) Remove option in v0.50.0
    owner_id: Option<RoleId>,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterIntrospectionSourceIndexKey {
    #[serde(rename = "compute_id")] // historical name
    cluster_id: ClusterId,
    name: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterIntrospectionSourceIndexValue {
    index_id: u64,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct DatabaseKey {
    id: u64,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterReplicaKey {
    id: ReplicaId,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterReplicaValue {
    #[serde(rename = "compute_instance_id")] // historical name
    cluster_id: ClusterId,
    name: String,
    config: SerializedReplicaConfig,
    // TODO(jkosh44) Remove option in v0.50.0
    owner_id: Option<RoleId>,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct DatabaseValue {
    name: String,
    // TODO(jkosh44) Remove option in v0.50.0
    owner_id: Option<RoleId>,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SchemaKey {
    id: u64,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct SchemaValue {
    database_id: Option<u64>,
    name: String,
    // TODO(jkosh44) Remove option in v0.50.0
    owner_id: Option<RoleId>,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash, Debug)]
pub struct ItemKey {
    gid: GlobalId,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Debug)]
pub struct ItemValue {
    schema_id: u64,
    name: String,
    definition: SerializedCatalogItem,
    // TODO(jkosh44) Remove option in v0.50.0
    owner_id: Option<RoleId>,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash, Debug)]
pub struct RoleKey {
    id: RoleId,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Debug)]
pub struct RoleValue {
    // flatten needed for backwards compatibility.
    #[serde(flatten)]
    role: SerializedRole,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ConfigValue {
    value: u64,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct AuditLogKey {
    event: VersionedEvent,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct StorageUsageKey {
    metric: VersionedStorageUsage,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct TimestampKey {
    id: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct TimestampValue {
    ts: mz_repr::Timestamp,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ServerConfigurationKey {
    name: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct ServerConfigurationValue {
    value: String,
}

pub static COLLECTION_CONFIG: TypedCollection<String, ConfigValue> = TypedCollection::new("config");
pub static COLLECTION_SETTING: TypedCollection<SettingKey, SettingValue> =
    TypedCollection::new("setting");
pub static COLLECTION_ID_ALLOC: TypedCollection<IdAllocKey, IdAllocValue> =
    TypedCollection::new("id_alloc");
pub static COLLECTION_SYSTEM_GID_MAPPING: TypedCollection<GidMappingKey, GidMappingValue> =
    TypedCollection::new("system_gid_mapping");
pub static COLLECTION_CLUSTERS: TypedCollection<ClusterKey, ClusterValue> =
    TypedCollection::new("compute_instance"); // historical name
pub static COLLECTION_CLUSTER_INTROSPECTION_SOURCE_INDEX: TypedCollection<
    ClusterIntrospectionSourceIndexKey,
    ClusterIntrospectionSourceIndexValue,
> = TypedCollection::new("compute_introspection_source_index"); // historical name
pub static COLLECTION_CLUSTER_REPLICAS: TypedCollection<ClusterReplicaKey, ClusterReplicaValue> =
    TypedCollection::new("compute_replicas"); // historical name
pub static COLLECTION_DATABASE: TypedCollection<DatabaseKey, DatabaseValue> =
    TypedCollection::new("database");
pub static COLLECTION_SCHEMA: TypedCollection<SchemaKey, SchemaValue> =
    TypedCollection::new("schema");
pub static COLLECTION_ITEM: TypedCollection<ItemKey, ItemValue> = TypedCollection::new("item");
pub static COLLECTION_ROLE: TypedCollection<RoleKey, RoleValue> = TypedCollection::new("role");
pub static COLLECTION_TIMESTAMP: TypedCollection<TimestampKey, TimestampValue> =
    TypedCollection::new("timestamp");
pub static COLLECTION_SYSTEM_CONFIGURATION: TypedCollection<
    ServerConfigurationKey,
    ServerConfigurationValue,
> = TypedCollection::new("system_configuration");
pub static COLLECTION_AUDIT_LOG: TypedCollection<AuditLogKey, ()> =
    TypedCollection::new("audit_log");
pub static COLLECTION_STORAGE_USAGE: TypedCollection<StorageUsageKey, ()> =
    TypedCollection::new("storage_usage");

pub static ALL_COLLECTIONS: &[&str] = &[
    COLLECTION_CONFIG.name(),
    COLLECTION_SETTING.name(),
    COLLECTION_ID_ALLOC.name(),
    COLLECTION_SYSTEM_GID_MAPPING.name(),
    COLLECTION_CLUSTERS.name(),
    COLLECTION_CLUSTER_INTROSPECTION_SOURCE_INDEX.name(),
    COLLECTION_CLUSTER_REPLICAS.name(),
    COLLECTION_DATABASE.name(),
    COLLECTION_SCHEMA.name(),
    COLLECTION_ITEM.name(),
    COLLECTION_ROLE.name(),
    COLLECTION_TIMESTAMP.name(),
    COLLECTION_SYSTEM_CONFIGURATION.name(),
    COLLECTION_AUDIT_LOG.name(),
    COLLECTION_STORAGE_USAGE.name(),
];
