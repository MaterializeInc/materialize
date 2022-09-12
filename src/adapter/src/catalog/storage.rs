// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::iter::once;
use std::str::FromStr;
use std::time::Duration;

use itertools::{max, Itertools};
use serde::{Deserialize, Serialize};
use timely::progress::Timestamp;
use uuid::Uuid;

use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_compute_client::command::ReplicaId;
use mz_compute_client::controller::ComputeInstanceId;
use mz_controller::ConcreteComputeInstanceReplicaConfig;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_repr::GlobalId;
use mz_sql::catalog::CatalogError as SqlCatalogError;
use mz_sql::names::{
    DatabaseId, ObjectQualifiers, QualifiedObjectName, ResolvedDatabaseSpecifier, RoleId, SchemaId,
    SchemaSpecifier,
};
use mz_sql::plan::ComputeInstanceIntrospectionConfig;
use mz_stash::{Append, AppendBatch, Stash, StashError, TableTransaction, TypedCollection};
use mz_storage::types::sources::Timeline;

use crate::catalog;
use crate::catalog::builtin::{BuiltinLog, BUILTIN_ROLES};
use crate::catalog::error::{Error, ErrorKind};
use crate::catalog::SerializedComputeInstanceReplicaConfig;
use crate::catalog::SystemObjectMapping;

use super::{
    SerializedCatalogItem, SerializedComputeInstanceReplicaLocation,
    SerializedComputeInstanceReplicaLogging,
};

const USER_VERSION: &str = "user_version";

const MATERIALIZE_DATABASE_ID: u64 = 1;
const MZ_CATALOG_SCHEMA_ID: u64 = 1;
const PG_CATALOG_SCHEMA_ID: u64 = 2;
const PUBLIC_SCHEMA_ID: u64 = 3;
const MZ_INTERNAL_SCHEMA_ID: u64 = 4;
const INFORMATION_SCHEMA_ID: u64 = 5;
const MATERIALIZE_ROLE_ID: u64 = 1;
const DEFAULT_COMPUTE_INSTANCE_ID: u64 = 1;
const DEFAULT_REPLICA_ID: u64 = 1;

const DATABASE_ID_ALLOC_KEY: &str = "database";
const SCHEMA_ID_ALLOC_KEY: &str = "schema";
const USER_ROLE_ID_ALLOC_KEY: &str = "user_role";
const SYSTEM_ROLE_ID_ALLOC_KEY: &str = "system_role";
const COMPUTE_ID_ALLOC_KEY: &str = "compute";
const REPLICA_ID_ALLOC_KEY: &str = "replica";
pub(crate) const AUDIT_LOG_ID_ALLOC_KEY: &str = "auditlog";
pub(crate) const STORAGE_USAGE_ID_ALLOC_KEY: &str = "storage_usage";

async fn migrate<S: Append>(
    stash: &mut S,
    version: u64,
    bootstrap_args: &BootstrapArgs,
) -> Result<(), catalog::error::Error> {
    // Initial state.
    let migrations: &[for<'a> fn(
        &mut Transaction<'a, S>,
        &'a BootstrapArgs,
    ) -> Result<(), StashError>] = &[
        |txn: &mut Transaction<'_, S>, bootstrap_args| {
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
                    name: COMPUTE_ID_ALLOC_KEY.into(),
                },
                IdAllocValue {
                    next_id: DEFAULT_COMPUTE_INSTANCE_ID + 1,
                },
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
                    id: RoleId::User(MATERIALIZE_ROLE_ID).to_string(),
                },
                RoleValue {
                    name: "materialize".into(),
                },
            )?;
            txn.compute_instances.insert(
                ComputeInstanceKey {
                    id: DEFAULT_COMPUTE_INSTANCE_ID,
                },
                ComputeInstanceValue {
                    name: "default".into(),
                    config: Some(ComputeInstanceIntrospectionConfig {
                        debugging: false,
                        interval: Duration::from_secs(1),
                    }),
                },
            )?;
            txn.compute_instance_replicas.insert(
                ComputeInstanceReplicaKey {
                    id: DEFAULT_REPLICA_ID,
                },
                ComputeInstanceReplicaValue {
                    compute_instance_id: DEFAULT_COMPUTE_INSTANCE_ID,
                    name: "default_replica".into(),
                    config: SerializedComputeInstanceReplicaConfig {
                        persisted_logs: SerializedComputeInstanceReplicaLogging::Default,
                        location: SerializedComputeInstanceReplicaLocation::Managed {
                            size: bootstrap_args.default_cluster_replica_size.clone(),
                            availability_zone: bootstrap_args.default_availability_zone.clone(),
                            az_user_specified: false,
                        },
                    },
                },
            )?;
            txn.timestamps.insert(
                TimestampKey {
                    id: Timeline::EpochMilliseconds.to_string(),
                },
                TimestampValue {
                    ts: mz_repr::Timestamp::minimum(),
                },
            )?;
            txn.configs
                .insert(USER_VERSION.to_string(), ConfigValue { value: 0 })?;
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
        (migration)(&mut txn, bootstrap_args)?;
        txn.update_user_version(u64::cast_from(i))?;
    }
    txn.commit().await?;
    Ok(())
}

pub struct BootstrapArgs {
    pub default_cluster_replica_size: String,
    pub default_availability_zone: String,
}

#[derive(Debug)]
pub struct Connection<S> {
    stash: S,
    cluster_id: Uuid,
}

impl<S: Append> Connection<S> {
    pub async fn open(
        mut stash: S,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Connection<S>, Error> {
        // Run unapplied migrations. The `user_version` field stores the index
        // of the last migration that was run. If the upper is min, the config
        // collection is empty.
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
        initialize_stash(&mut stash).await?;
        migrate(&mut stash, skip, bootstrap_args).await?;

        let conn = Connection {
            cluster_id: Self::set_or_get_cluster_id(&mut stash).await?,
            stash,
        };

        Ok(conn)
    }
}

impl<S: Append> Connection<S> {
    async fn get_setting(&mut self, key: &str) -> Result<Option<String>, Error> {
        Self::get_setting_stash(&mut self.stash, key).await
    }

    async fn set_setting(&mut self, key: &str, value: &str) -> Result<(), Error> {
        Self::set_setting_stash(&mut self.stash, key, value).await
    }

    async fn get_setting_stash(stash: &mut impl Stash, key: &str) -> Result<Option<String>, Error> {
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
        stash: &mut impl Append,
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

    /// Sets catalog's `cluster_id` setting on initialization or gets that value.
    async fn set_or_get_cluster_id(stash: &mut impl Append) -> Result<Uuid, Error> {
        let current_setting = Self::get_setting_stash(stash, "cluster_id").await?;
        match current_setting {
            // Server init
            None => {
                // Generate a new version 4 UUID. These are generated from random input.
                let cluster_id = Uuid::new_v4();
                Self::set_setting_stash(stash, "cluster_id", cluster_id.to_string()).await?;
                Ok(cluster_id)
            }
            // Server reboot
            Some(cs) => Ok(Uuid::parse_str(&cs)?),
        }
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
        // Add in any new builtin roles.
        let mut tx = self.transaction().await?;
        let role_names: HashSet<_> = tx
            .roles
            .items()
            .into_values()
            .map(|value| value.name)
            .collect();
        for builtin_role in &*BUILTIN_ROLES {
            if !role_names.contains(builtin_role.name) {
                tx.insert_system_role(builtin_role.name)?;
            }
        }
        tx.commit().await?;

        Ok(COLLECTION_ROLE
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| {
                (
                    RoleId::from_str(&k.id)
                        .unwrap_or_else(|_| panic!("Invalid persisted role id {}", k.id)),
                    v.name,
                )
            })
            .collect())
    }

    pub async fn load_compute_instances(
        &mut self,
    ) -> Result<
        Vec<(
            ComputeInstanceId,
            String,
            Option<ComputeInstanceIntrospectionConfig>,
        )>,
        Error,
    > {
        Ok(COLLECTION_COMPUTE_INSTANCES
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| (k.id, v.name, v.config))
            .collect())
    }

    pub async fn load_compute_instance_replicas(
        &mut self,
    ) -> Result<
        Vec<(
            ComputeInstanceId,
            ReplicaId,
            String,
            SerializedComputeInstanceReplicaConfig,
        )>,
        Error,
    > {
        Ok(COLLECTION_COMPUTE_INSTANCE_REPLICAS
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| (v.compute_instance_id, k.id, v.name, v.config))
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
    ) -> Result<BTreeMap<(String, String), (GlobalId, u64)>, Error> {
        Ok(COLLECTION_SYSTEM_GID_MAPPING
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| {
                (
                    (k.schema_name, k.object_name),
                    (GlobalId::System(v.id), v.fingerprint),
                )
            })
            .collect())
    }

    pub async fn load_introspection_source_index_gids(
        &mut self,
        compute_id: ComputeInstanceId,
    ) -> Result<BTreeMap<String, GlobalId>, Error> {
        Ok(COLLECTION_COMPUTE_INTROSPECTION_SOURCE_INDEX
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .filter_map(|(k, v)| {
                if k.compute_id == compute_id {
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
        mappings: Vec<(ComputeInstanceId, &str, GlobalId)>,
    ) -> Result<(), Error> {
        if mappings.is_empty() {
            return Ok(());
        }

        let mappings = mappings.into_iter().map(|(compute_id, name, index_id)| {
            let index_id = if let GlobalId::System(id) = index_id {
                id
            } else {
                panic!("non-system id provided")
            };
            (
                ComputeIntrospectionSourceIndexKey {
                    compute_id,
                    name: name.to_string(),
                },
                ComputeIntrospectionSourceIndexValue { index_id },
            )
        });
        COLLECTION_COMPUTE_INTROSPECTION_SOURCE_INDEX
            .upsert(&mut self.stash, mappings)
            .await
            .map_err(|e| e.into())
    }

    /// Set the configuration of a replica.
    /// This accepts only one item, as we currently use this only for the default cluster
    pub async fn set_replica_config(
        &mut self,
        replica_id: ReplicaId,
        compute_instance_id: ComputeInstanceId,
        name: String,
        config: &ConcreteComputeInstanceReplicaConfig,
    ) -> Result<(), Error> {
        let key = ComputeInstanceReplicaKey { id: replica_id };
        let val = ComputeInstanceReplicaValue {
            compute_instance_id,
            name,
            config: config.clone().into(),
        };
        COLLECTION_COMPUTE_INSTANCE_REPLICAS
            .upsert_key(&mut self.stash, &key, &val)
            .await?;
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

    #[tracing::instrument(level = "debug", skip(self))]
    async fn allocate_id(&mut self, id_type: &str, amount: u64) -> Result<Vec<u64>, Error> {
        let key = IdAllocKey {
            name: id_type.to_string(),
        };
        let prev = COLLECTION_ID_ALLOC
            .peek_key_one(&mut self.stash, &key)
            .await?;
        let id = prev.expect("must exist").next_id;
        let next = match id.checked_add(amount) {
            Some(next_gid) => IdAllocValue { next_id: next_gid },
            None => return Err(Error::new(ErrorKind::IdExhaustion)),
        };
        COLLECTION_ID_ALLOC
            .upsert_key(&mut self.stash, &key, &next)
            .await?;
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

    /// Get a global timestamp for a timeline that has been persisted to disk.
    pub async fn get_persisted_timestamp(
        &mut self,
        timeline: &Timeline,
    ) -> Result<mz_repr::Timestamp, Error> {
        let key = TimestampKey {
            id: timeline.to_string(),
        };
        Ok(COLLECTION_TIMESTAMP
            .peek_key_one(&mut self.stash, &key)
            .await?
            .expect("must have a persisted timestamp")
            .ts)
    }

    /// Persist new global timestamp for a timeline to disk.
    pub async fn persist_timestamp(
        &mut self,
        timeline: &Timeline,
        timestamp: mz_repr::Timestamp,
    ) -> Result<(), Error> {
        let key = TimestampKey {
            id: timeline.to_string(),
        };
        let value = TimestampValue { ts: timestamp };
        let old_value = COLLECTION_TIMESTAMP
            .upsert_key(&mut self.stash, &key, &value)
            .await?;
        if let Some(old_value) = old_value {
            assert!(value >= old_value, "global timestamp must always go up");
        }
        Ok(())
    }

    pub async fn transaction<'a>(&'a mut self) -> Result<Transaction<'a, S>, Error> {
        transaction(&mut self.stash).await
    }

    pub async fn confirm_leadership(&mut self) -> Result<(), Error> {
        Ok(self.stash.confirm_leadership().await?)
    }

    pub async fn consolidate(&mut self, collections: &[mz_stash::Id]) -> Result<(), Error> {
        Ok(self.stash.consolidate_batch(&collections).await?)
    }

    pub fn cluster_id(&self) -> Uuid {
        self.cluster_id
    }
}

#[tracing::instrument(level = "trace", skip_all)]
pub async fn transaction<'a, S: Append>(stash: &'a mut S) -> Result<Transaction<'a, S>, Error> {
    let databases = COLLECTION_DATABASE.peek_one(stash).await?;
    let schemas = COLLECTION_SCHEMA.peek_one(stash).await?;
    let roles = COLLECTION_ROLE.peek_one(stash).await?;
    let items = COLLECTION_ITEM.peek_one(stash).await?;
    let compute_instances = COLLECTION_COMPUTE_INSTANCES.peek_one(stash).await?;
    let compute_instance_replicas = COLLECTION_COMPUTE_INSTANCE_REPLICAS.peek_one(stash).await?;
    let introspection_sources = COLLECTION_COMPUTE_INTROSPECTION_SOURCE_INDEX
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
        compute_instances: TableTransaction::new(compute_instances, |a, b| a.name == b.name),
        compute_instance_replicas: TableTransaction::new(compute_instance_replicas, |a, b| {
            a.compute_instance_id == b.compute_instance_id && a.name == b.name
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

pub struct Transaction<'a, S> {
    stash: &'a mut S,
    databases: TableTransaction<DatabaseKey, DatabaseValue>,
    schemas: TableTransaction<SchemaKey, SchemaValue>,
    items: TableTransaction<ItemKey, ItemValue>,
    roles: TableTransaction<RoleKey, RoleValue>,
    compute_instances: TableTransaction<ComputeInstanceKey, ComputeInstanceValue>,
    compute_instance_replicas:
        TableTransaction<ComputeInstanceReplicaKey, ComputeInstanceReplicaValue>,
    introspection_sources:
        TableTransaction<ComputeIntrospectionSourceIndexKey, ComputeIntrospectionSourceIndexValue>,
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

impl<'a, S: Append> Transaction<'a, S> {
    pub fn loaded_items(&self) -> Vec<(GlobalId, QualifiedObjectName, SerializedCatalogItem)> {
        let databases = self.databases.items();
        let schemas = self.schemas.items();
        let mut items = Vec::new();
        self.items.for_values(|k, v| {
            let schema = match schemas.get(&SchemaKey { id: v.schema_id }) {
                Some(schema) => schema,
                None => return,
            };
            let database_id = match schema.database_id {
                Some(id) => id,
                None => return,
            };
            let _database = match databases.get(&DatabaseKey { id: database_id }) {
                Some(database) => database,
                None => return,
            };
            items.push((
                k.gid,
                QualifiedObjectName {
                    qualifiers: ObjectQualifiers {
                        database_spec: ResolvedDatabaseSpecifier::from(database_id),
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
            RoleKey { id: id.to_string() },
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
    pub fn insert_compute_instance(
        &mut self,
        cluster_name: &str,
        config: &Option<ComputeInstanceIntrospectionConfig>,
        introspection_source_indexes: &Vec<(&'static BuiltinLog, GlobalId)>,
    ) -> Result<ComputeInstanceId, Error> {
        let id = self.get_and_increment_id(COMPUTE_ID_ALLOC_KEY.to_string())?;
        if let Err(_) = self.compute_instances.insert(
            ComputeInstanceKey { id },
            ComputeInstanceValue {
                name: cluster_name.to_string(),
                config: config.clone(),
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
                    ComputeIntrospectionSourceIndexKey {
                        compute_id: id,
                        name: builtin.name.to_string(),
                    },
                    ComputeIntrospectionSourceIndexValue { index_id },
                )
                .expect("no uniqueness violation");
        }

        Ok(id)
    }

    pub fn insert_compute_instance_replica(
        &mut self,
        compute_name: &str,
        replica_name: &str,
        config: &SerializedComputeInstanceReplicaConfig,
    ) -> Result<ReplicaId, Error> {
        let id = self.get_and_increment_id(REPLICA_ID_ALLOC_KEY.to_string())?;
        let mut compute_instance_id = None;
        for (
            ComputeInstanceKey { id },
            ComputeInstanceValue {
                name,
                config: _config,
            },
        ) in self.compute_instances.items()
        {
            if &name == compute_name {
                compute_instance_id = Some(id);
                break;
            }
        }
        if let Err(_) = self.compute_instance_replicas.insert(
            ComputeInstanceReplicaKey { id },
            ComputeInstanceReplicaValue {
                compute_instance_id: compute_instance_id.unwrap(),
                name: replica_name.into(),
                config: config.clone(),
            },
        ) {
            return Err(Error::new(ErrorKind::DuplicateReplica(
                replica_name.to_string(),
                compute_name.to_string(),
            )));
        };
        Ok(id)
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
        let diff = self.id_allocator.update(|k, _v| {
            if k.name == key {
                Some(IdAllocValue { next_id })
            } else {
                None
            }
        })?;
        assert_eq!(diff, 1);
        Ok(id)
    }

    pub fn remove_database(&mut self, id: &DatabaseId) -> Result<(), Error> {
        let n = self.databases.delete(|k, _v| k.id == id.0).len();
        assert!(n <= 1);
        if n == 1 {
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
        let n = self.schemas.delete(|k, _v| k.id == schema_id.0).len();
        assert!(n <= 1);
        if n == 1 {
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

    pub fn remove_compute_instance(&mut self, name: &str) -> Result<Vec<GlobalId>, Error> {
        let deleted = self.compute_instances.delete(|_k, v| v.name == name);
        if deleted.is_empty() {
            Err(SqlCatalogError::UnknownComputeInstance(name.to_owned()).into())
        } else {
            assert_eq!(deleted.len(), 1);
            // Cascade delete introsepction sources and cluster replicas.
            let id = deleted.into_element().0.id;
            self.compute_instance_replicas
                .delete(|_k, v| v.compute_instance_id == id);
            let introspection_source_indexes = self
                .introspection_sources
                .delete(|k, _v| k.compute_id == id);
            Ok(introspection_source_indexes
                .into_iter()
                .map(|(_, v)| GlobalId::System(v.index_id))
                .collect())
        }
    }

    pub fn remove_compute_instance_replica(
        &mut self,
        name: &str,
        compute_id: ComputeInstanceId,
    ) -> Result<(), Error> {
        let deleted = self
            .compute_instance_replicas
            .delete(|_k, v| v.compute_instance_id == compute_id && v.name == name);
        if deleted.len() == 1 {
            Ok(())
        } else {
            assert!(deleted.is_empty());
            Err(SqlCatalogError::UnknownComputeInstanceReplica(name.to_owned()).into())
        }
    }

    pub fn remove_item(&mut self, id: GlobalId) -> Result<(), Error> {
        let n = self.items.delete(|k, _v| k.gid == id).len();
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownItem(id.to_string()).into())
        }
    }

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

    pub fn update_user_version(&mut self, version: u64) -> Result<(), Error> {
        let n = self.configs.update(|k, _v| {
            if k == USER_VERSION {
                Some(ConfigValue { value: version })
            } else {
                None
            }
        })?;
        assert_eq!(n, 1);
        Ok(())
    }

    /// Updates persisted mapping from system objects to global IDs and fingerprints. Each element
    /// of `mappings` should be (old-global-id, new-system-object-mapping).
    ///
    /// Panics if provided id is not a system id.
    pub fn update_system_object_mappings(
        &mut self,
        mappings: &HashMap<GlobalId, SystemObjectMapping>,
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
                    fingerprint: mapping.fingerprint,
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
    pub async fn upsert_system_config(&mut self, name: &str, value: &str) -> Result<(), Error> {
        let key = ServerConfigurationKey {
            name: name.to_string(),
        };
        let value = ServerConfigurationValue {
            value: value.to_string(),
        };
        self.system_configurations.delete(|k, _v| k == &key);
        self.system_configurations.insert(key, value)?;
        Ok(())
    }

    /// Removes persisted system configuration `name`.
    pub async fn remove_system_config(&mut self, name: &str) {
        let key = ServerConfigurationKey {
            name: name.to_string(),
        };
        self.system_configurations.delete(|k, _v| k == &key);
    }

    /// Removes all persisted system configurations.
    pub async fn clear_system_configs(&mut self) {
        self.system_configurations.delete(|_k, _v| true);
    }

    pub fn remove_timestamp(&mut self, timeline: Timeline) {
        let timeline_str = timeline.to_string();
        let n = self.timestamps.delete(|k, _v| k.id == timeline_str).len();
        assert_eq!(n, 1);
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn commit(self) -> Result<(), Error> {
        let (stash, collections) = self.commit_without_consolidate().await?;
        stash.consolidate_batch(&collections).await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn commit_without_consolidate(self) -> Result<(&'a mut S, Vec<mz_stash::Id>), Error> {
        let mut batches = Vec::new();
        async fn add_batch<K, V, S, I>(
            stash: &mut S,
            batches: &mut Vec<AppendBatch>,
            collection: &TypedCollection<K, V>,
            changes: I,
        ) -> Result<(), Error>
        where
            K: mz_stash::Data,
            V: mz_stash::Data,
            S: Append,
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
            &COLLECTION_COMPUTE_INSTANCES,
            self.compute_instances.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_COMPUTE_INSTANCE_REPLICAS,
            self.compute_instance_replicas.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_COMPUTE_INTROSPECTION_SOURCE_INDEX,
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
pub async fn is_collection_uninitialized<K, V, S>(
    stash: &mut S,
    collection: &TypedCollection<K, V>,
) -> Result<bool, Error>
where
    K: mz_stash::Data,
    V: mz_stash::Data,
    S: Append,
{
    Ok(collection.upper(stash).await?.elements() == [mz_stash::Timestamp::MIN])
}

/// Inserts empty values into all new collections, so the collections are readable.
pub async fn initialize_stash<S: Append>(stash: &mut S) -> Result<(), Error> {
    let mut batches = Vec::new();
    async fn add_batch<K, V, S>(
        stash: &mut S,
        batches: &mut Vec<AppendBatch>,
        collection: &TypedCollection<K, V>,
    ) -> Result<(), Error>
    where
        K: mz_stash::Data,
        V: mz_stash::Data,
        S: Append,
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
    add_batch(stash, &mut batches, &COLLECTION_COMPUTE_INSTANCES).await?;
    add_batch(
        stash,
        &mut batches,
        &COLLECTION_COMPUTE_INTROSPECTION_SOURCE_INDEX,
    )
    .await?;
    add_batch(stash, &mut batches, &COLLECTION_COMPUTE_INSTANCE_REPLICAS).await?;
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
struct SettingKey {
    name: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
struct SettingValue {
    value: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct IdAllocKey {
    name: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
struct IdAllocValue {
    next_id: u64,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct GidMappingKey {
    schema_name: String,
    object_name: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
struct GidMappingValue {
    id: u64,
    fingerprint: u64,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct ComputeInstanceKey {
    id: u64,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
struct ComputeInstanceValue {
    name: String,
    config: Option<ComputeInstanceIntrospectionConfig>,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct ComputeIntrospectionSourceIndexKey {
    compute_id: ComputeInstanceId,
    name: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
struct ComputeIntrospectionSourceIndexValue {
    index_id: u64,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct DatabaseKey {
    id: u64,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct ComputeInstanceReplicaKey {
    id: ReplicaId,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
struct ComputeInstanceReplicaValue {
    compute_instance_id: ComputeInstanceId,
    name: String,
    config: SerializedComputeInstanceReplicaConfig,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
struct DatabaseValue {
    name: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct SchemaKey {
    id: u64,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
struct SchemaValue {
    database_id: Option<u64>,
    name: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct ItemKey {
    gid: GlobalId,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
struct ItemValue {
    schema_id: u64,
    name: String,
    definition: SerializedCatalogItem,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct RoleKey {
    id: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
struct RoleValue {
    name: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct ConfigValue {
    value: u64,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct AuditLogKey {
    event: VersionedEvent,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct StorageUsageKey {
    metric: VersionedStorageUsage,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct TimestampKey {
    id: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
struct TimestampValue {
    ts: mz_repr::Timestamp,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct ServerConfigurationKey {
    name: String,
}

#[derive(Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
struct ServerConfigurationValue {
    value: String,
}

static COLLECTION_CONFIG: TypedCollection<String, ConfigValue> = TypedCollection::new("config");
static COLLECTION_SETTING: TypedCollection<SettingKey, SettingValue> =
    TypedCollection::new("setting");
static COLLECTION_ID_ALLOC: TypedCollection<IdAllocKey, IdAllocValue> =
    TypedCollection::new("id_alloc");
static COLLECTION_SYSTEM_GID_MAPPING: TypedCollection<GidMappingKey, GidMappingValue> =
    TypedCollection::new("system_gid_mapping");
static COLLECTION_COMPUTE_INSTANCES: TypedCollection<ComputeInstanceKey, ComputeInstanceValue> =
    TypedCollection::new("compute_instance");
static COLLECTION_COMPUTE_INTROSPECTION_SOURCE_INDEX: TypedCollection<
    ComputeIntrospectionSourceIndexKey,
    ComputeIntrospectionSourceIndexValue,
> = TypedCollection::new("compute_introspection_source_index");
static COLLECTION_COMPUTE_INSTANCE_REPLICAS: TypedCollection<
    ComputeInstanceReplicaKey,
    ComputeInstanceReplicaValue,
> = TypedCollection::new("compute_instance_replicas");
static COLLECTION_DATABASE: TypedCollection<DatabaseKey, DatabaseValue> =
    TypedCollection::new("database");
static COLLECTION_SCHEMA: TypedCollection<SchemaKey, SchemaValue> = TypedCollection::new("schema");
static COLLECTION_ITEM: TypedCollection<ItemKey, ItemValue> = TypedCollection::new("item");
static COLLECTION_ROLE: TypedCollection<RoleKey, RoleValue> = TypedCollection::new("role");
static COLLECTION_TIMESTAMP: TypedCollection<TimestampKey, TimestampValue> =
    TypedCollection::new("timestamp");
static COLLECTION_SYSTEM_CONFIGURATION: TypedCollection<
    ServerConfigurationKey,
    ServerConfigurationValue,
> = TypedCollection::new("system_configuration");
static COLLECTION_AUDIT_LOG: TypedCollection<AuditLogKey, ()> = TypedCollection::new("audit_log");
static COLLECTION_STORAGE_USAGE: TypedCollection<StorageUsageKey, ()> =
    TypedCollection::new("storage_usage");
