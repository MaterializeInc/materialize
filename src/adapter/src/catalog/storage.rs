// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::Hash;
use std::iter::once;
use std::time::Duration;

use itertools::{max, Itertools};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use mz_audit_log::{EventDetails, EventType, ObjectType, VersionedEvent, VersionedStorageUsage};
use mz_controller::clusters::{ClusterId, ReplicaConfig, ReplicaId};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::now::{EpochMillis, NowFn};
use mz_repr::GlobalId;
use mz_sql::catalog::{CatalogError as SqlCatalogError, CatalogItemType};
use mz_sql::names::{
    DatabaseId, ObjectQualifiers, QualifiedObjectName, ResolvedDatabaseSpecifier, RoleId, SchemaId,
    SchemaSpecifier,
};
use mz_stash::{AppendBatch, Stash, StashError, TableTransaction, TypedCollection};
use mz_storage_client::types::sources::Timeline;

use crate::catalog;
use crate::catalog::builtin::{
    BuiltinLog, BUILTIN_CLUSTERS, BUILTIN_CLUSTER_REPLICAS, BUILTIN_PREFIXES, BUILTIN_ROLES,
};
use crate::catalog::error::{Error, ErrorKind};
use crate::catalog::{is_reserved_name, SystemObjectMapping};
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
const MATERIALIZE_ROLE_ID: u64 = 1;
const DEFAULT_USER_CLUSTER_ID: ClusterId = ClusterId::User(1);
const DEFAULT_REPLICA_ID: u64 = 1;

const DATABASE_ID_ALLOC_KEY: &str = "database";
const SCHEMA_ID_ALLOC_KEY: &str = "schema";
const USER_ROLE_ID_ALLOC_KEY: &str = "user_role";
const SYSTEM_ROLE_ID_ALLOC_KEY: &str = "system_role";
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
                    .unwrap()
                        + 1,
                },
            )?;
            txn.id_allocator.insert(
                IdAllocKey {
                    name: USER_ROLE_ID_ALLOC_KEY.into(),
                },
                IdAllocValue {
                    next_id: MATERIALIZE_ROLE_ID + 1,
                },
            )?;
            txn.id_allocator.insert(
                IdAllocKey {
                    name: SYSTEM_ROLE_ID_ALLOC_KEY.into(),
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
            txn.databases.insert(
                DatabaseKey {
                    id: MATERIALIZE_DATABASE_ID,
                },
                DatabaseValue {
                    name: "materialize".into(),
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
                },
            )?;
            txn.schemas.insert(
                SchemaKey {
                    id: PG_CATALOG_SCHEMA_ID,
                },
                SchemaValue {
                    database_id: None,
                    name: "pg_catalog".into(),
                },
            )?;
            txn.schemas.insert(
                SchemaKey {
                    id: PUBLIC_SCHEMA_ID,
                },
                SchemaValue {
                    database_id: Some(1),
                    name: "public".into(),
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
                },
            )?;
            txn.schemas.insert(
                SchemaKey {
                    id: INFORMATION_SCHEMA_ID,
                },
                SchemaValue {
                    database_id: None,
                    name: "information_schema".into(),
                },
            )?;
            txn.roles.insert(
                RoleKey {
                    id: RoleId::User(MATERIALIZE_ROLE_ID),
                },
                RoleValue {
                    name: "materialize".into(),
                },
            )?;
            let id = txn.get_and_increment_id(AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
            txn.audit_log_updates.push((
                AuditLogKey {
                    event: VersionedEvent::new(
                        id,
                        EventType::Create,
                        ObjectType::Role,
                        EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                            id: MATERIALIZE_ROLE_ID.to_string(),
                            name: "materialize".into(),
                        }),
                        None,
                        now,
                    ),
                },
                (),
                1,
            ));
            let default_cluster = ClusterValue {
                name: "default".into(),
                linked_object_id: None,
            };
            let default_replica = ClusterReplicaValue {
                cluster_id: DEFAULT_USER_CLUSTER_ID,
                name: DEFAULT_CLUSTER_REPLICA_NAME.into(),
                config: default_cluster_replica_config(bootstrap_args),
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
        // These three migrations were removed, but we need to keep empty migrations because the
        // user version depends on the length of this array. New migrations should still go after
        // these empty migrations.
        |_, _, _| Ok(()),
        |_, _, _| Ok(()),
        |_, _, _| Ok(()),
        // An optional field, `idle_arrangement_merge_effort`, was added to replica configs, which
        // should default to `None` for existing configs. The deserialization is able deserialize
        // the missing values as `None`. This migration updates the on-disk version to explicitly
        // have a `None` value instead of a missing value.
        //
        // Introduced in v0.39.0
        |txn: &mut Transaction<'_>, _now, _bootstrap_args| {
            txn.cluster_replicas.update(|_k, v| Some(v.clone()))?;
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
    add_new_builtin_roles_migration(&mut txn)?;
    add_new_builtin_clusters_migration(&mut txn)?;
    add_new_builtin_cluster_replicas_migration(&mut txn, bootstrap_args)?;
    txn.commit().await?;
    Ok(())
}

fn add_new_builtin_roles_migration(txn: &mut Transaction<'_>) -> Result<(), catalog::error::Error> {
    let role_names: HashSet<_> = txn
        .roles
        .items()
        .into_values()
        .map(|value| value.name)
        .collect();
    for builtin_role in &*BUILTIN_ROLES {
        assert!(
            is_reserved_name(builtin_role.name),
            "builtin role {builtin_role:?} must start with one of the following prefixes {}",
            BUILTIN_PREFIXES.join(", ")
        );
        if !role_names.contains(builtin_role.name) {
            txn.insert_system_role(builtin_role.name)?;
        }
    }
    Ok(())
}

fn add_new_builtin_clusters_migration(
    txn: &mut Transaction<'_>,
) -> Result<(), catalog::error::Error> {
    let cluster_names: HashSet<_> = txn
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
    let cluster_lookup: HashMap<_, _> = txn
        .clusters
        .items()
        .into_iter()
        .map(|(key, value)| (value.name, key.id))
        .collect();

    let replicas: HashMap<_, _> =
        txn.cluster_replicas
            .items()
            .into_values()
            .fold(HashMap::new(), |mut acc, value| {
                acc.entry(value.cluster_id)
                    .or_insert_with(HashSet::new)
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
            txn.insert_cluster_replica(*cluster_id, replica_id, builtin_replica.name, &config)?;
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
        sources: None,
        views: None,
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
    consolidations_tx: mpsc::UnboundedSender<Vec<mz_stash::Id>>,
    boot_ts: mz_repr::Timestamp,
}

impl Connection {
    pub async fn open(
        mut stash: Stash,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
        consolidations_tx: mpsc::UnboundedSender<Vec<mz_stash::Id>>,
    ) -> Result<Connection, Error> {
        // The `user_version` field stores the index of the last migration that
        // was run. If the upper is min, the config collection is empty.
        let skip = if is_collection_uninitialized(&mut stash, &COLLECTION_CONFIG).await? {
            0
        } else {
            // An advanced collection must have had its user version set, so the unwrap
            // must succeed.
            COLLECTION_CONFIG
                .peek_key_one(&mut stash, &USER_VERSION.to_string())
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

        let mut conn = Connection {
            stash,
            consolidations_tx,
            boot_ts,
        };

        if !conn.stash.is_readonly() {
            // IMPORTANT: we durably record the new timestamp before using it.
            conn.persist_timestamp(&Timeline::EpochMilliseconds, boot_ts)
                .await?;
            migrate(&mut conn.stash, skip, boot_ts.into(), bootstrap_args).await?;
        }

        Ok(conn)
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
        let settings = COLLECTION_SETTING.get(stash).await?;
        let v = stash
            .peek_key_one(
                settings,
                &SettingKey {
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

    pub async fn load_databases(&mut self) -> Result<Vec<(DatabaseId, String)>, Error> {
        Ok(COLLECTION_DATABASE
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| (DatabaseId::new(k.id), v.name))
            .collect())
    }

    pub async fn load_schemas(
        &mut self,
    ) -> Result<Vec<(SchemaId, String, Option<DatabaseId>)>, Error> {
        Ok(COLLECTION_SCHEMA
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| {
                (
                    SchemaId::new(k.id),
                    v.name,
                    v.database_id.map(DatabaseId::new),
                )
            })
            .collect())
    }

    pub async fn load_roles(&mut self) -> Result<Vec<(RoleId, String)>, Error> {
        Ok(COLLECTION_ROLE
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| (k.id, v.name))
            .collect())
    }

    pub async fn load_clusters(
        &mut self,
    ) -> Result<Vec<(ClusterId, String, Option<GlobalId>)>, Error> {
        Ok(COLLECTION_CLUSTERS
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| (k.id, v.name, v.linked_object_id))
            .collect())
    }

    pub async fn load_cluster_replicas(
        &mut self,
    ) -> Result<Vec<(ClusterId, ReplicaId, String, SerializedReplicaConfig)>, Error> {
        Ok(COLLECTION_CLUSTER_REPLICAS
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| (v.cluster_id, k.id, v.name, v.config))
            .collect())
    }

    pub async fn load_audit_log(&mut self) -> Result<impl Iterator<Item = VersionedEvent>, Error> {
        Ok(COLLECTION_AUDIT_LOG
            .peek_one(&mut self.stash)
            .await?
            .into_keys()
            .map(|ev| ev.event))
    }

    pub async fn storage_usage(
        &mut self,
    ) -> Result<impl Iterator<Item = VersionedStorageUsage>, Error> {
        Ok(COLLECTION_STORAGE_USAGE
            .peek_one(&mut self.stash)
            .await?
            .into_keys()
            .map(|ev| ev.metric))
    }

    /// Load the persisted mapping of system object to global ID. Key is (schema-name, object-name).
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
    ) -> Result<(), Error> {
        let key = ClusterReplicaKey { id: replica_id };
        let val = ClusterReplicaValue {
            cluster_id,
            name,
            config: config.clone().into(),
        };
        COLLECTION_CLUSTER_REPLICAS
            .upsert_key(&mut self.stash, &key, |_| Ok::<_, Error>(val))
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

    /// Get the next user cluster ID without allocating it.
    pub async fn get_next_user_cluster_id(&mut self) -> Result<ClusterId, Error> {
        self.get_next_id(USER_CLUSTER_ID_ALLOC_KEY)
            .await
            .map(ClusterId::User)
    }

    /// Get the next replica id without allocating it.
    pub async fn get_next_replica_id(&mut self) -> Result<ReplicaId, Error> {
        self.get_next_id(REPLICA_ID_ALLOC_KEY).await
    }

    async fn get_next_id(&mut self, id_type: &str) -> Result<u64, Error> {
        COLLECTION_ID_ALLOC
            .peek_key_one(
                &mut self.stash,
                &IdAllocKey {
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
        let (prev, next, consolidate_ids) = COLLECTION_ID_ALLOC
            .upsert_key_no_consolidate(&mut self.stash, &key, |prev| {
                let id = prev.expect("must exist").next_id;
                match id.checked_add(amount) {
                    Some(next_gid) => Ok(IdAllocValue { next_id: next_gid }),
                    None => Err(Error::new(ErrorKind::IdExhaustion)),
                }
            })
            .await??;
        self.consolidations_tx
            .send(consolidate_ids)
            .expect("coordinator unexpectedly gone");
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
        let (prev, next, consolidate_ids) = COLLECTION_TIMESTAMP
            .upsert_key_no_consolidate(&mut self.stash, &key, |_| {
                Ok::<_, Error>(TimestampValue { ts: timestamp })
            })
            .await??;
        if let Some(prev) = prev {
            assert!(next >= prev, "global timestamp must always go up");
        }
        self.consolidations_tx
            .send(consolidate_ids)
            .expect("coordinator unexpectedly gone");
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
        .peek_key_one(stash, &key)
        .await?
        .map(|v| v.ts))
}

#[tracing::instrument(level = "trace", skip_all)]
pub async fn transaction<'a>(stash: &'a mut Stash) -> Result<Transaction<'a>, Error> {
    let databases = COLLECTION_DATABASE.peek_one(stash).await?;
    let schemas = COLLECTION_SCHEMA.peek_one(stash).await?;
    let roles = COLLECTION_ROLE.peek_one(stash).await?;
    let items = COLLECTION_ITEM.peek_one(stash).await?;
    let clusters = COLLECTION_CLUSTERS.peek_one(stash).await?;
    let cluster_replicas = COLLECTION_CLUSTER_REPLICAS.peek_one(stash).await?;
    let introspection_sources = COLLECTION_CLUSTER_INTROSPECTION_SOURCE_INDEX
        .peek_one(stash)
        .await?;
    let id_allocator = COLLECTION_ID_ALLOC.peek_one(stash).await?;
    let configs = COLLECTION_CONFIG.peek_one(stash).await?;
    let settings = COLLECTION_SETTING.peek_one(stash).await?;
    let timestamps = COLLECTION_TIMESTAMP.peek_one(stash).await?;
    let system_gid_mapping = COLLECTION_SYSTEM_GID_MAPPING.peek_one(stash).await?;
    let system_configurations = COLLECTION_SYSTEM_CONFIGURATION.peek_one(stash).await?;

    Ok(Transaction {
        stash,
        databases: TableTransaction::new(databases, |a, b| a.name == b.name),
        schemas: TableTransaction::new(schemas, |a, b| {
            a.database_id == b.database_id && a.name == b.name
        }),
        items: TableTransaction::new(items, |a, b| a.schema_id == b.schema_id && a.name == b.name),
        roles: TableTransaction::new(roles, |a, b| a.name == b.name),
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
    pub fn loaded_items(&self) -> Vec<(GlobalId, QualifiedObjectName, SerializedCatalogItem)> {
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
            ));
        });
        items.sort_by_key(|(id, _, _)| *id);
        items
    }

    pub fn insert_audit_log_event(&mut self, event: VersionedEvent) {
        self.audit_log_updates.push((AuditLogKey { event }, (), 1));
    }

    pub fn insert_storage_usage_event(&mut self, metric: VersionedStorageUsage) {
        self.storage_usage_updates
            .push((StorageUsageKey { metric }, (), 1));
    }

    pub fn insert_database(&mut self, database_name: &str) -> Result<DatabaseId, Error> {
        let id = self.get_and_increment_id(DATABASE_ID_ALLOC_KEY.to_string())?;
        match self.databases.insert(
            DatabaseKey { id },
            DatabaseValue {
                name: database_name.to_string(),
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
    ) -> Result<SchemaId, Error> {
        let id = self.get_and_increment_id(SCHEMA_ID_ALLOC_KEY.to_string())?;
        match self.schemas.insert(
            SchemaKey { id },
            SchemaValue {
                database_id: Some(database_id.0),
                name: schema_name.to_string(),
            },
        ) {
            Ok(_) => Ok(SchemaId::new(id)),
            Err(_) => Err(Error::new(ErrorKind::SchemaAlreadyExists(
                schema_name.to_owned(),
            ))),
        }
    }

    pub fn insert_user_role(&mut self, role_name: &str) -> Result<RoleId, Error> {
        self.insert_role(role_name, USER_ROLE_ID_ALLOC_KEY, RoleId::User)
    }

    fn insert_system_role(&mut self, role_name: &str) -> Result<RoleId, Error> {
        self.insert_role(role_name, SYSTEM_ROLE_ID_ALLOC_KEY, RoleId::System)
    }

    fn insert_role<F>(
        &mut self,
        role_name: &str,
        id_alloc_key: &str,
        role_id_variant: F,
    ) -> Result<RoleId, Error>
    where
        F: Fn(u64) -> RoleId,
    {
        let id = self.get_and_increment_id(id_alloc_key.to_string())?;
        let id = role_id_variant(id);
        match self.roles.insert(
            RoleKey { id },
            RoleValue {
                name: role_name.to_string(),
            },
        ) {
            Ok(_) => Ok(id),
            Err(_) => Err(Error::new(ErrorKind::RoleAlreadyExists(
                role_name.to_owned(),
            ))),
        }
    }

    /// Panics if any introspection source id is not a system id
    pub fn insert_user_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        linked_object_id: Option<GlobalId>,
        introspection_source_indexes: &Vec<(&'static BuiltinLog, GlobalId)>,
    ) -> Result<(), Error> {
        self.insert_cluster(
            cluster_id,
            cluster_name,
            linked_object_id,
            introspection_source_indexes,
        )
    }

    /// Panics if any introspection source id is not a system id
    pub fn insert_system_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        introspection_source_indexes: &Vec<(&'static BuiltinLog, GlobalId)>,
    ) -> Result<(), Error> {
        self.insert_cluster(cluster_id, cluster_name, None, introspection_source_indexes)
    }

    fn insert_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        linked_object_id: Option<GlobalId>,
        introspection_source_indexes: &Vec<(&'static BuiltinLog, GlobalId)>,
    ) -> Result<(), Error> {
        if let Err(_) = self.clusters.insert(
            ClusterKey { id: cluster_id },
            ClusterValue {
                name: cluster_name.to_string(),
                linked_object_id,
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
    ) -> Result<(), Error> {
        if let Err(_) = self.cluster_replicas.insert(
            ClusterReplicaKey { id: replica_id },
            ClusterReplicaValue {
                cluster_id,
                name: replica_name.into(),
                config: config.clone(),
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
    ) -> Result<(), Error> {
        match self.items.insert(
            ItemKey { gid: id },
            ItemValue {
                schema_id: schema_id.0,
                name: item_name.to_string(),
                definition: item,
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
        let n = self.roles.delete(|_k, v| v.name == name).len();
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
    /// Returns an error if any id in `ids` is not found.
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
        mappings: HashMap<GlobalId, SystemObjectMapping>,
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
    pub fn upsert_system_config(&mut self, name: &str, value: &str) -> Result<(), Error> {
        let key = ServerConfigurationKey {
            name: name.to_string(),
        };
        let value = ServerConfigurationValue {
            value: value.to_string(),
        };
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

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn commit(self) -> Result<(), Error> {
        let (stash, collections) = self.commit_without_consolidate().await?;
        stash.consolidate_batch(&collections).await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn commit_without_consolidate(
        self,
    ) -> Result<(&'a mut Stash, Vec<mz_stash::Id>), Error> {
        let mut batches = Vec::new();
        async fn add_batch<K, V, I>(
            stash: &mut Stash,
            batches: &mut Vec<AppendBatch>,
            collection: &TypedCollection<K, V>,
            changes: I,
        ) -> Result<(), Error>
        where
            K: mz_stash::Data,
            V: mz_stash::Data,

            I: IntoIterator<Item = (K, V, mz_stash::Diff)>,
        {
            let mut changes = changes.into_iter().peekable();
            if changes.peek().is_none() {
                return Ok(());
            }
            let collection = collection.get(stash).await?;
            let mut batch = collection.make_batch(stash).await?;
            for (k, v, diff) in changes {
                collection.append_to_batch(&mut batch, &k, &v, diff);
            }
            batches.push(batch);
            Ok(())
        }
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_DATABASE,
            self.databases.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_SCHEMA,
            self.schemas.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_ITEM,
            self.items.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_ROLE,
            self.roles.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_CLUSTERS,
            self.clusters.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_CLUSTER_REPLICAS,
            self.cluster_replicas.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_CLUSTER_INTROSPECTION_SOURCE_INDEX,
            self.introspection_sources.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_ID_ALLOC,
            self.id_allocator.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_CONFIG,
            self.configs.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_SETTING,
            self.settings.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_TIMESTAMP,
            self.timestamps.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_SYSTEM_GID_MAPPING,
            self.system_gid_mapping.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_SYSTEM_CONFIGURATION,
            self.system_configurations.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_AUDIT_LOG,
            self.audit_log_updates,
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_STORAGE_USAGE,
            self.storage_usage_updates,
        )
        .await?;

        let ids = batches
            .iter()
            .map(|batch| batch.collection_id)
            .collect::<Vec<_>>();
        if !batches.is_empty() {
            self.stash
                .append_batch(&batches)
                .await
                .map_err(StashError::from)?;
        }
        Ok((self.stash, ids))
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
pub async fn initialize_stash(stash: &mut Stash) -> Result<(), Error> {
    let mut batches = Vec::new();
    async fn add_batch<K, V>(
        stash: &mut Stash,
        batches: &mut Vec<AppendBatch>,
        collection: &TypedCollection<K, V>,
    ) -> Result<(), Error>
    where
        K: mz_stash::Data,
        V: mz_stash::Data,
    {
        if is_collection_uninitialized(stash, collection).await? {
            let collection = collection.get(stash).await?;
            let batch = collection.make_batch(stash).await?;
            batches.push(batch);
        }
        Ok(())
    }
    add_batch(stash, &mut batches, &COLLECTION_CONFIG).await?;
    add_batch(stash, &mut batches, &COLLECTION_SETTING).await?;
    add_batch(stash, &mut batches, &COLLECTION_ID_ALLOC).await?;
    add_batch(stash, &mut batches, &COLLECTION_SYSTEM_GID_MAPPING).await?;
    add_batch(stash, &mut batches, &COLLECTION_CLUSTERS).await?;
    add_batch(
        stash,
        &mut batches,
        &COLLECTION_CLUSTER_INTROSPECTION_SOURCE_INDEX,
    )
    .await?;
    add_batch(stash, &mut batches, &COLLECTION_CLUSTER_REPLICAS).await?;
    add_batch(stash, &mut batches, &COLLECTION_DATABASE).await?;
    add_batch(stash, &mut batches, &COLLECTION_SCHEMA).await?;
    add_batch(stash, &mut batches, &COLLECTION_ITEM).await?;
    add_batch(stash, &mut batches, &COLLECTION_ROLE).await?;
    add_batch(stash, &mut batches, &COLLECTION_TIMESTAMP).await?;
    add_batch(stash, &mut batches, &COLLECTION_SYSTEM_CONFIGURATION).await?;
    add_batch(stash, &mut batches, &COLLECTION_AUDIT_LOG).await?;
    add_batch(stash, &mut batches, &COLLECTION_STORAGE_USAGE).await?;
    stash.append(&batches).await.map_err(|e| e.into())
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
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct DatabaseValue {
    name: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SchemaKey {
    id: u64,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct SchemaValue {
    database_id: Option<u64>,
    name: String,
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
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct RoleKey {
    id: RoleId,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct RoleValue {
    name: String,
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
