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

use itertools::Itertools;
use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_controller::clusters::{ClusterId, ReplicaConfig, ReplicaId};
use mz_ore::collections::CollectionExt;
use mz_ore::now::NowFn;
use mz_proto::{ProtoType, RustType};
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::catalog::{
    CatalogCluster, CatalogDatabase, CatalogError as SqlCatalogError, CatalogItemType,
    CatalogSchema, ObjectType, RoleAttributes,
};
use mz_sql::names::{
    DatabaseId, ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier, SchemaId,
    SchemaSpecifier,
};
use mz_sql_parser::ast::QualifiedReplica;
use mz_stash::objects::proto;
use mz_stash::{AppendBatch, Stash, StashError, TableTransaction, TypedCollection};
use mz_storage_client::types::sources::Timeline;
use proptest_derive::Arbitrary;

use crate::catalog::builtin::{
    BuiltinLog, BUILTIN_CLUSTERS, BUILTIN_CLUSTER_REPLICAS, BUILTIN_PREFIXES,
};
use crate::catalog::error::{Error, ErrorKind};
use crate::catalog::storage::stash::{
    DEPLOY_GENERATION, SYSTEM_PRIVILEGES_COLLECTION, USER_VERSION,
};
use crate::catalog::{
    self, is_reserved_name, ClusterConfig, ClusterVariant, DefaultPrivilegeAclItem,
    DefaultPrivilegeObject, RoleMembership, SerializedCatalogItem, SerializedReplicaConfig,
    SerializedReplicaLocation, SerializedReplicaLogging, SerializedRole, SystemObjectMapping,
};
use crate::coord::timeline;

pub mod objects;
pub mod stash;

use crate::catalog::storage::stash::DEFAULT_PRIVILEGES_COLLECTION;
pub use stash::{
    AUDIT_LOG_COLLECTION, CLUSTER_COLLECTION, CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION,
    CLUSTER_REPLICA_COLLECTION, CONFIG_COLLECTION, DATABASES_COLLECTION, ID_ALLOCATOR_COLLECTION,
    ITEM_COLLECTION, ROLES_COLLECTION, SCHEMAS_COLLECTION, SETTING_COLLECTION,
    STORAGE_USAGE_COLLECTION, SYSTEM_CONFIGURATION_COLLECTION, SYSTEM_GID_MAPPING_COLLECTION,
    TIMESTAMP_COLLECTION,
};

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
            txn.insert_system_cluster(
                id,
                builtin_cluster.name,
                &vec![],
                builtin_cluster.privileges.clone(),
                ClusterConfig {
                    // TODO: Should builtin clusters be managed or unmanaged?
                    variant: ClusterVariant::Unmanaged,
                },
            )?;
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

#[derive(Clone, Debug)]
pub struct BootstrapArgs {
    pub default_cluster_replica_size: String,
    pub builtin_cluster_replica_size: String,
    pub default_availability_zone: String,
    pub bootstrap_role: Option<String>,
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
        deploy_generation: Option<u64>,
    ) -> Result<Connection, Error> {
        // Initialize the Stash if it hasn't been already
        let mut conn = if !stash.is_initialized().await? {
            // Get the current timestamp so we can record when we booted.
            let previous_now = mz_repr::Timestamp::MIN;
            let boot_ts = timeline::monotonic_now(now, previous_now);

            // Initialize the Stash
            let args = bootstrap_args.clone();
            stash
                .with_transaction(move |mut tx| {
                    Box::pin(async move {
                        stash::initialize(&mut tx, &args, boot_ts.into(), deploy_generation).await
                    })
                })
                .await?;

            Connection { stash, boot_ts }
        } else {
            // Before we do anything with the Stash, we need to run any pending upgrades and
            // initialize new collections.
            if !stash.is_readonly() {
                stash.upgrade().await?;
            }

            // Choose a time at which to boot. This is the time at which we will run
            // internal migrations, and is also exposed upwards in case higher
            // layers want to run their own migrations at the same timestamp.
            //
            // This time is usually the current system time, but with protection
            // against backwards time jumps, even across restarts.
            let previous = try_get_persisted_timestamp(&mut stash, &Timeline::EpochMilliseconds)
                .await?
                .unwrap_or(mz_repr::Timestamp::MIN);
            let boot_ts = timeline::monotonic_now(now, previous);

            let mut conn = Connection { stash, boot_ts };

            if !conn.stash.is_readonly() {
                // IMPORTANT: we durably record the new timestamp before using it.
                conn.persist_timestamp(&Timeline::EpochMilliseconds, boot_ts)
                    .await?;
                if let Some(deploy_generation) = deploy_generation {
                    conn.persist_deploy_generation(deploy_generation).await?;
                }
            }

            conn
        };

        // Add any new builtin Clusters or Cluster Replicas that may be newly defined.
        if !conn.stash.is_readonly() {
            let mut txn = transaction(&mut conn.stash).await?;
            add_new_builtin_clusters_migration(&mut txn)?;
            add_new_builtin_cluster_replicas_migration(&mut txn, bootstrap_args)?;
            txn.commit().await?;
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
        let v = SETTING_COLLECTION
            .peek_key_one(
                stash,
                proto::SettingKey {
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
        let key = proto::SettingKey {
            name: key.to_string(),
        };
        let value = proto::SettingValue {
            value: value.into(),
        };
        SETTING_COLLECTION
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
    pub async fn load_databases(&mut self) -> Result<Vec<Database>, Error> {
        let entries = DATABASES_COLLECTION.peek_one(&mut self.stash).await?;
        let databases = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (DatabaseKey, DatabaseValue)| Database {
                id: k.id,
                name: v.name,
                owner_id: v.owner_id,
                privileges: v.privileges,
            })
            .collect::<Result<_, _>>()?;

        Ok(databases)
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_schemas(&mut self) -> Result<Vec<Schema>, Error> {
        let entries = SCHEMAS_COLLECTION.peek_one(&mut self.stash).await?;
        let schemas = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (SchemaKey, SchemaValue)| Schema {
                id: k.id,
                name: v.name,
                database_id: v.database_id,
                owner_id: v.owner_id,
                privileges: v.privileges,
            })
            .collect::<Result<_, _>>()?;

        Ok(schemas)
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_roles(&mut self) -> Result<Vec<Role>, Error> {
        let entries = ROLES_COLLECTION.peek_one(&mut self.stash).await?;
        let roles = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (RoleKey, RoleValue)| Role {
                id: k.id,
                name: v.role.name,
                attributes: v.role.attributes.expect("attributes not migrated"),
                membership: v.role.membership.expect("membership not migrated"),
            })
            .collect::<Result<_, _>>()?;

        Ok(roles)
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_clusters(&mut self) -> Result<Vec<Cluster>, Error> {
        let entries = CLUSTER_COLLECTION.peek_one(&mut self.stash).await?;
        let clusters = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (ClusterKey, ClusterValue)| Cluster {
                id: k.id,
                name: v.name,
                linked_object_id: v.linked_object_id,
                owner_id: v.owner_id,
                privileges: v.privileges,
                config: v.config,
            })
            .collect::<Result<_, _>>()?;

        Ok(clusters)
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_cluster_replicas(&mut self) -> Result<Vec<ClusterReplica>, Error> {
        let entries = CLUSTER_REPLICA_COLLECTION.peek_one(&mut self.stash).await?;
        let replicas = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(
                |(k, v): (ClusterReplicaKey, ClusterReplicaValue)| ClusterReplica {
                    cluster_id: v.cluster_id,
                    replica_id: k.id,
                    name: v.name,
                    serialized_config: v.config,
                    owner_id: v.owner_id,
                },
            )
            .collect::<Result<_, _>>()?;

        Ok(replicas)
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_audit_log(&mut self) -> Result<impl Iterator<Item = VersionedEvent>, Error> {
        let entries = AUDIT_LOG_COLLECTION.peek_one(&mut self.stash).await?;
        let logs: Vec<_> = entries
            .into_keys()
            .map(AuditLogKey::from_proto)
            .map_ok(|e| e.event)
            .collect::<Result<_, _>>()?;

        Ok(logs.into_iter())
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
                    let collection = STORAGE_USAGE_COLLECTION.from_tx(&tx).await?;
                    let rows = tx.peek_one(collection).await?;
                    let mut events = Vec::with_capacity(rows.len());
                    let mut batch = collection.make_batch_tx(&tx).await?;
                    for ev in rows.into_keys() {
                        let event: StorageUsageKey = ev.clone().into_rust()?;
                        if u128::from(event.metric.timestamp()) >= cutoff_ts {
                            events.push(event.metric);
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
        let entries = SYSTEM_GID_MAPPING_COLLECTION
            .peek_one(&mut self.stash)
            .await?;
        let system_gid_mappings = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (GidMappingKey, GidMappingValue)| {
                (
                    (k.schema_name, k.object_type, k.object_name),
                    (GlobalId::System(v.id), v.fingerprint),
                )
            })
            .collect::<Result<_, _>>()?;

        Ok(system_gid_mappings)
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_introspection_source_index_gids(
        &mut self,
        cluster_id: ClusterId,
    ) -> Result<BTreeMap<String, GlobalId>, Error> {
        let entries = CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION
            .peek_one(&mut self.stash)
            .await?;
        let sources = entries
            .into_iter()
            .map(RustType::from_proto)
            .filter_map_ok(
                |(k, v): (
                    ClusterIntrospectionSourceIndexKey,
                    ClusterIntrospectionSourceIndexValue,
                )| {
                    if k.cluster_id == cluster_id {
                        Some((k.name, GlobalId::System(v.index_id)))
                    } else {
                        None
                    }
                },
            )
            .collect::<Result<_, _>>()?;

        Ok(sources)
    }

    /// Load the persisted default privileges.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_default_privileges(
        &mut self,
    ) -> Result<Vec<(DefaultPrivilegeObject, DefaultPrivilegeAclItem)>, Error> {
        Ok(DEFAULT_PRIVILEGES_COLLECTION
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (DefaultPrivilegesKey, DefaultPrivilegesValue)| {
                (
                    DefaultPrivilegeObject::new(
                        k.role_id,
                        k.database_id,
                        k.schema_id,
                        k.object_type,
                    ),
                    DefaultPrivilegeAclItem::new(k.grantee, v.privileges),
                )
            })
            .collect::<Result<_, _>>()?)
    }

    /// Load the persisted system privileges.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_system_privileges(&mut self) -> Result<Vec<MzAclItem>, Error> {
        Ok(SYSTEM_PRIVILEGES_COLLECTION
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, _): (SystemPrivilegesKey, ())| k.privileges)
            .collect::<Result<_, _>>()?)
    }

    /// Load the persisted server configurations.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_system_configuration(&mut self) -> Result<BTreeMap<String, String>, Error> {
        SYSTEM_CONFIGURATION_COLLECTION
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

        let mappings = mappings
            .into_iter()
            .map(|mapping| {
                let id = if let GlobalId::System(id) = mapping.id {
                    id
                } else {
                    panic!("non-system id provided")
                };
                (
                    GidMappingKey {
                        schema_name: mapping.schema_name,
                        object_type: mapping.object_type,
                        object_name: mapping.object_name,
                    },
                    GidMappingValue {
                        id,
                        fingerprint: mapping.fingerprint,
                    },
                )
            })
            .map(|e| RustType::into_proto(&e));
        SYSTEM_GID_MAPPING_COLLECTION
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

        let mappings = mappings
            .into_iter()
            .map(|(cluster_id, name, index_id)| {
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
            })
            .map(|e| RustType::into_proto(&e));
        CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION
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
        let key = ClusterReplicaKey { id: replica_id }.into_proto();
        let val = ClusterReplicaValue {
            cluster_id,
            name,
            config: config.clone().into(),
            owner_id,
        }
        .into_proto();
        CLUSTER_REPLICA_COLLECTION
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
        ID_ALLOCATOR_COLLECTION
            .peek_key_one(
                &mut self.stash,
                IdAllocKey {
                    name: id_type.to_string(),
                }
                .into_proto(),
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
        }
        .into_proto();
        let (prev, next) = ID_ALLOCATOR_COLLECTION
            .upsert_key(&mut self.stash, key, move |prev| {
                let id = prev.expect("must exist").next_id;
                match id.checked_add(amount) {
                    Some(next_gid) => Ok(IdAllocValue { next_id: next_gid }.into_proto()),
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
        let entries = TIMESTAMP_COLLECTION.peek_one(&mut self.stash).await?;
        let timestamps = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (TimestampKey, TimestampValue)| {
                (k.id.parse().expect("invalid timeline persisted"), v.ts)
            })
            .collect::<Result<_, _>>()?;

        Ok(timestamps)
    }

    /// Persist new global timestamp for a timeline to disk.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn persist_timestamp(
        &mut self,
        timeline: &Timeline,
        timestamp: mz_repr::Timestamp,
    ) -> Result<(), Error> {
        let key = proto::TimestampKey {
            id: timeline.to_string(),
        };
        let (prev, next) = TIMESTAMP_COLLECTION
            .upsert_key(&mut self.stash, key, move |_| {
                Ok::<_, Error>(TimestampValue { ts: timestamp }.into_proto())
            })
            .await??;
        if let Some(prev) = prev {
            assert!(next >= prev, "global timestamp must always go up");
        }
        Ok(())
    }

    pub async fn persist_deploy_generation(&mut self, deploy_generation: u64) -> Result<(), Error> {
        CONFIG_COLLECTION
            .upsert_key(
                &mut self.stash,
                proto::ConfigKey {
                    key: DEPLOY_GENERATION.into(),
                },
                move |_| {
                    Ok::<_, Error>(proto::ConfigValue {
                        value: deploy_generation,
                    })
                },
            )
            .await??;
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
    let key = proto::TimestampKey {
        id: timeline.to_string(),
    };
    let val: Option<TimestampValue> = TIMESTAMP_COLLECTION
        .peek_key_one(stash, key)
        .await?
        .map(RustType::from_proto)
        .transpose()?;

    Ok(val.map(|v| v.ts))
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
        default_privileges,
        system_privileges,
    ) = stash
        .with_transaction(|tx| {
            Box::pin(async move {
                // Peek the catalog collections in any order and a single transaction.
                futures::try_join!(
                    tx.peek_one(tx.collection(DATABASES_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(SCHEMAS_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(ROLES_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(ITEM_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(CLUSTER_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(CLUSTER_REPLICA_COLLECTION.name()).await?),
                    tx.peek_one(
                        tx.collection(CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION.name())
                            .await?,
                    ),
                    tx.peek_one(tx.collection(ID_ALLOCATOR_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(CONFIG_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(SETTING_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(TIMESTAMP_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(SYSTEM_GID_MAPPING_COLLECTION.name()).await?),
                    tx.peek_one(
                        tx.collection(SYSTEM_CONFIGURATION_COLLECTION.name())
                            .await?
                    ),
                    tx.peek_one(tx.collection(DEFAULT_PRIVILEGES_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(SYSTEM_PRIVILEGES_COLLECTION.name()).await?),
                )
            })
        })
        .await?;

    Ok(Transaction {
        stash,
        databases: TableTransaction::new(databases, |a: &DatabaseValue, b| a.name == b.name)?,
        schemas: TableTransaction::new(schemas, |a: &SchemaValue, b| {
            a.database_id == b.database_id && a.name == b.name
        })?,
        items: TableTransaction::new(items, |a: &ItemValue, b| {
            a.schema_id == b.schema_id && a.name == b.name
        })?,
        roles: TableTransaction::new(roles, |a: &RoleValue, b| a.role.name == b.role.name)?,
        clusters: TableTransaction::new(clusters, |a: &ClusterValue, b| a.name == b.name)?,
        cluster_replicas: TableTransaction::new(cluster_replicas, |a: &ClusterReplicaValue, b| {
            a.cluster_id == b.cluster_id && a.name == b.name
        })?,
        introspection_sources: TableTransaction::new(introspection_sources, |_a, _b| false)?,
        id_allocator: TableTransaction::new(id_allocator, |_a, _b| false)?,
        configs: TableTransaction::new(configs, |_a, _b| false)?,
        settings: TableTransaction::new(settings, |_a, _b| false)?,
        timestamps: TableTransaction::new(timestamps, |_a, _b| false)?,
        system_gid_mapping: TableTransaction::new(system_gid_mapping, |_a, _b| false)?,
        system_configurations: TableTransaction::new(system_configurations, |_a, _b| false)?,
        default_privileges: TableTransaction::new(default_privileges, |_a, _b| false)?,
        system_privileges: TableTransaction::new(system_privileges, |_a, _b| false)?,
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
    configs: TableTransaction<ConfigKey, ConfigValue>,
    settings: TableTransaction<SettingKey, SettingValue>,
    timestamps: TableTransaction<TimestampKey, TimestampValue>,
    system_gid_mapping: TableTransaction<GidMappingKey, GidMappingValue>,
    system_configurations: TableTransaction<ServerConfigurationKey, ServerConfigurationValue>,
    default_privileges: TableTransaction<DefaultPrivilegesKey, DefaultPrivilegesValue>,
    system_privileges: TableTransaction<SystemPrivilegesKey, ()>,
    // Don't make this a table transaction so that it's not read into the stash
    // memory cache.
    audit_log_updates: Vec<(proto::AuditLogKey, (), i64)>,
    storage_usage_updates: Vec<(proto::StorageUsageKey, (), i64)>,
}

impl<'a> Transaction<'a> {
    pub fn loaded_items(&self) -> Vec<Item> {
        let databases = self.databases.items();
        let schemas = self.schemas.items();
        let mut items = Vec::new();
        self.items.for_values(|k, v| {
            let schema_key = SchemaKey { id: v.schema_id };
            let schema = match schemas.get(&schema_key) {
                Some(schema) => schema,
                None => panic!(
                    "corrupt stash! unknown schema id {}, for item with key \
                        {k:?} and value {v:?}",
                    v.schema_id
                ),
            };
            let database_spec = match schema.database_id {
                Some(id) => {
                    let key = DatabaseKey { id };
                    if databases.get(&key).is_none() {
                        panic!(
                            "corrupt stash! unknown database id {key:?}, for item with key \
                        {k:?} and value {v:?}"
                        );
                    }
                    ResolvedDatabaseSpecifier::from(id)
                }
                None => ResolvedDatabaseSpecifier::Ambient,
            };
            items.push(Item {
                id: k.gid,
                name: QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec,
                        schema_spec: SchemaSpecifier::from(v.schema_id),
                    },
                    item: v.name.clone(),
                },
                definition: v.definition.clone(),
                owner_id: v.owner_id,
                privileges: v.privileges.clone(),
            });
        });
        items.sort_by_key(|Item { id, .. }| *id);
        items
    }

    pub fn insert_audit_log_event(&mut self, event: VersionedEvent) {
        self.audit_log_updates
            .push((AuditLogKey { event }.into_proto(), (), 1));
    }

    pub fn insert_storage_usage_event(&mut self, metric: VersionedStorageUsage) {
        self.storage_usage_updates
            .push((StorageUsageKey { metric }.into_proto(), (), 1));
    }

    pub fn insert_user_database(
        &mut self,
        database_name: &str,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
    ) -> Result<DatabaseId, Error> {
        let id = self.get_and_increment_id(DATABASE_ID_ALLOC_KEY.to_string())?;
        match self.databases.insert(
            DatabaseKey {
                // TODO(parkertimmerman): Support creating databases in the System namespace.
                id: DatabaseId::User(id),
            },
            DatabaseValue {
                name: database_name.to_string(),
                owner_id,
                privileges,
            },
        ) {
            // TODO(parkertimmerman): Support creating databases in the System namespace.
            Ok(_) => Ok(DatabaseId::User(id)),
            Err(_) => Err(Error::new(ErrorKind::DatabaseAlreadyExists(
                database_name.to_owned(),
            ))),
        }
    }

    pub fn insert_user_schema(
        &mut self,
        database_id: DatabaseId,
        schema_name: &str,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
    ) -> Result<SchemaId, Error> {
        let id = self.get_and_increment_id(SCHEMA_ID_ALLOC_KEY.to_string())?;
        match self.schemas.insert(
            SchemaKey {
                // TODO(parkertimmerman): Support creating schemas in the System namespace.
                id: SchemaId::User(id),
            },
            SchemaValue {
                database_id: Some(database_id),
                name: schema_name.to_string(),
                owner_id,
                privileges,
            },
        ) {
            // TODO(parkertimmerman): Support creating schemas in the System namespace.
            Ok(_) => Ok(SchemaId::User(id)),
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
        privileges: Vec<MzAclItem>,
        config: ClusterConfig,
    ) -> Result<(), Error> {
        self.insert_cluster(
            cluster_id,
            cluster_name,
            linked_object_id,
            introspection_source_indexes,
            owner_id,
            privileges,
            config,
        )
    }

    /// Panics if any introspection source id is not a system id
    pub fn insert_system_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        introspection_source_indexes: &Vec<(&'static BuiltinLog, GlobalId)>,
        privileges: Vec<MzAclItem>,
        config: ClusterConfig,
    ) -> Result<(), Error> {
        self.insert_cluster(
            cluster_id,
            cluster_name,
            None,
            introspection_source_indexes,
            MZ_SYSTEM_ROLE_ID,
            privileges,
            config,
        )
    }

    fn insert_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        linked_object_id: Option<GlobalId>,
        introspection_source_indexes: &Vec<(&'static BuiltinLog, GlobalId)>,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
        config: ClusterConfig,
    ) -> Result<(), Error> {
        if let Err(_) = self.clusters.insert(
            ClusterKey { id: cluster_id },
            ClusterValue {
                name: cluster_name.to_string(),
                linked_object_id,
                owner_id,
                privileges,
                config,
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

    pub fn rename_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        cluster_to_name: &str,
    ) -> Result<(), Error> {
        let key = ClusterKey { id: cluster_id };

        match self.clusters.update(|k, v| {
            if *k == key {
                let mut value = v.clone();
                value.name = cluster_to_name.to_string();
                Some(value)
            } else {
                None
            }
        })? {
            0 => Err(SqlCatalogError::UnknownCluster(cluster_name.to_string()).into()),
            1 => Ok(()),
            n => panic!(
                "Expected to update single cluster {cluster_name} ({cluster_id}), updated {n}"
            ),
        }
    }

    pub fn rename_cluster_replica(
        &mut self,
        replica_id: ReplicaId,
        replica_name: &QualifiedReplica,
        replica_to_name: &str,
    ) -> Result<(), Error> {
        let key = ClusterReplicaKey { id: replica_id };

        match self.cluster_replicas.update(|k, v| {
            if *k == key {
                let mut value = v.clone();
                value.name = replica_to_name.to_string();
                Some(value)
            } else {
                None
            }
        })? {
            0 => Err(SqlCatalogError::UnknownClusterReplica(replica_name.to_string()).into()),
            1 => Ok(()),
            n => panic!(
                "Expected to update single cluster replica {replica_name} ({replica_id}), updated {n}"
            ),
        }
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
                owner_id,
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
        privileges: Vec<MzAclItem>,
    ) -> Result<(), Error> {
        match self.items.insert(
            ItemKey { gid: id },
            ItemValue {
                schema_id,
                name: item_name.to_string(),
                definition: item,
                owner_id,
                privileges,
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
        let prev = self.databases.set(DatabaseKey { id: *id }, None)?;
        if prev.is_some() {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownDatabase(id.to_string()).into())
        }
    }

    pub fn remove_schema(
        &mut self,
        database_id: &Option<DatabaseId>,
        schema_id: &SchemaId,
    ) -> Result<(), Error> {
        let prev = self.schemas.set(SchemaKey { id: *schema_id }, None)?;
        if prev.is_some() {
            Ok(())
        } else {
            let database_name = match database_id {
                Some(id) => format!("{id}."),
                None => "".to_string(),
            };
            Err(SqlCatalogError::UnknownSchema(format!("{}.{}", database_name, schema_id)).into())
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
                    privileges: v.clone().privileges,
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
                    privileges: v.privileges.clone(),
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
            ConfigKey {
                key: USER_VERSION.to_string(),
            },
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

    /// Updates cluster `id` in the transaction to `cluster`.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of clusters in the stash.
    /// DO NOT call this function in a loop.
    pub fn update_cluster(
        &mut self,
        id: ClusterId,
        cluster: &catalog::Cluster,
    ) -> Result<(), Error> {
        let n = self.clusters.update(|k, _v| {
            if k.id == id {
                Some(ClusterValue {
                    name: cluster.name().to_string(),
                    linked_object_id: cluster.linked_object_id(),
                    owner_id: cluster.owner_id,
                    privileges: cluster.privileges().all_values_owned().collect(),
                    config: cluster.config.clone(),
                })
            } else {
                None
            }
        })?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownCluster(id.to_string()).into())
        }
    }

    /// Updates cluster replica `replica_id` in the transaction to `replica`.
    ///
    /// Returns an error if `replica_id` is not found.
    ///
    /// Runtime is linear with respect to the total number of cluster replicas in the stash.
    /// DO NOT call this function in a loop.
    pub fn update_cluster_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        replica: &catalog::ClusterReplica,
    ) -> Result<(), Error> {
        let n = self.cluster_replicas.update(|k, _v| {
            if k.id == replica_id {
                Some(ClusterReplicaValue {
                    cluster_id,
                    name: replica.name.clone(),
                    config: replica.config.clone().into(),
                    owner_id: replica.owner_id,
                })
            } else {
                None
            }
        })?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownClusterReplica(replica_id.to_string()).into())
        }
    }

    /// Updates database `id` in the transaction to `database`.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of databases in the stash.
    /// DO NOT call this function in a loop.
    pub fn update_database(
        &mut self,
        id: DatabaseId,
        database: &catalog::Database,
    ) -> Result<(), Error> {
        let n = self.databases.update(|k, _v| {
            if id == k.id {
                Some(DatabaseValue {
                    name: database.name().to_string(),
                    owner_id: database.owner_id,
                    privileges: database.privileges().all_values_owned().collect(),
                })
            } else {
                None
            }
        })?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownDatabase(id.to_string()).into())
        }
    }

    /// Updates schema `schema_id` in the transaction to `schema`.
    ///
    /// Returns an error if `schema_id` is not found.
    ///
    /// Runtime is linear with respect to the total number of schemas in the stash.
    /// DO NOT call this function in a loop.
    pub fn update_schema(
        &mut self,
        database_id: Option<DatabaseId>,
        schema_id: SchemaId,
        schema: &catalog::Schema,
    ) -> Result<(), Error> {
        let n = self.schemas.update(|k, _v| {
            if schema_id == k.id {
                Some(SchemaValue {
                    database_id,
                    name: schema.name().schema.clone(),
                    owner_id: schema.owner_id,
                    privileges: schema.privileges().all_values_owned().collect(),
                })
            } else {
                None
            }
        })?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownSchema(schema_id.to_string()).into())
        }
    }

    /// Set persisted default privilege.
    pub fn set_default_privilege(
        &mut self,
        role_id: RoleId,
        database_id: Option<DatabaseId>,
        schema_id: Option<SchemaId>,
        object_type: ObjectType,
        grantee: RoleId,
        privileges: Option<AclMode>,
    ) -> Result<(), Error> {
        self.default_privileges.set(
            DefaultPrivilegesKey {
                role_id,
                database_id,
                schema_id,
                object_type,
                grantee,
            },
            privileges.map(|privileges| DefaultPrivilegesValue { privileges }),
        )?;
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

    /// Commits the storage transaction to the stash. Any error returned indicates the stash may be
    /// in an indeterminate state and needs to be fully re-read before proceeding. In general, this
    /// must be fatal to the calling process. We do not panic/halt inside this function itself so
    /// that errors can bubble up during initialization.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn commit(self) -> Result<(), Error> {
        self.commit_inner().await
    }

    /// If `fix_migration_retractions` is true, additionally fix collections that have retracted
    /// non-existent JSON rows, but whose Rust consolidations are the same (i.e., an Option field
    /// was added, so we no longer know how to retract the None variant).
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn commit_inner(self) -> Result<(), Error> {
        async fn add_batch<'tx, K, V>(
            tx: &'tx mz_stash::Transaction<'tx>,
            batches: &mut Vec<AppendBatch>,
            typed: &'tx TypedCollection<K, V>,
            changes: &[(K, V, mz_stash::Diff)],
        ) -> Result<(), StashError>
        where
            K: mz_stash::Data + 'tx,
            V: mz_stash::Data + 'tx,
        {
            if changes.is_empty() {
                return Ok(());
            }
            let collection = typed.from_tx(tx).await?;
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
        let default_privileges = Arc::new(self.default_privileges.pending());
        let system_privileges = Arc::new(self.system_privileges.pending());
        let audit_log_updates = Arc::new(self.audit_log_updates);
        let storage_usage_updates = Arc::new(self.storage_usage_updates);

        self.stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    let mut batches = Vec::new();

                    add_batch(&tx, &mut batches, &DATABASES_COLLECTION, &databases).await?;
                    add_batch(&tx, &mut batches, &SCHEMAS_COLLECTION, &schemas).await?;
                    add_batch(&tx, &mut batches, &ITEM_COLLECTION, &items).await?;
                    add_batch(&tx, &mut batches, &ROLES_COLLECTION, &roles).await?;
                    add_batch(&tx, &mut batches, &CLUSTER_COLLECTION, &clusters).await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &CLUSTER_REPLICA_COLLECTION,
                        &cluster_replicas,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION,
                        &introspection_sources,
                    )
                    .await?;
                    add_batch(&tx, &mut batches, &ID_ALLOCATOR_COLLECTION, &id_allocator).await?;
                    add_batch(&tx, &mut batches, &CONFIG_COLLECTION, &configs).await?;
                    add_batch(&tx, &mut batches, &SETTING_COLLECTION, &settings).await?;
                    add_batch(&tx, &mut batches, &TIMESTAMP_COLLECTION, &timestamps).await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &SYSTEM_GID_MAPPING_COLLECTION,
                        &system_gid_mapping,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &SYSTEM_CONFIGURATION_COLLECTION,
                        &system_configurations,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &DEFAULT_PRIVILEGES_COLLECTION,
                        &default_privileges,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &SYSTEM_PRIVILEGES_COLLECTION,
                        &system_privileges,
                    )
                    .await?;
                    add_batch(&tx, &mut batches, &AUDIT_LOG_COLLECTION, &audit_log_updates).await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &STORAGE_USAGE_COLLECTION,
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

// Structs used to pass information to outside modules.

pub struct Database {
    pub id: DatabaseId,
    pub name: String,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
}

pub struct Schema {
    pub id: SchemaId,
    pub name: String,
    pub database_id: Option<DatabaseId>,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
}

pub struct Role {
    pub id: RoleId,
    pub name: String,
    pub attributes: RoleAttributes,
    pub membership: RoleMembership,
}

pub struct Cluster {
    pub id: ClusterId,
    pub name: String,
    pub linked_object_id: Option<GlobalId>,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
    pub config: ClusterConfig,
}

pub struct ClusterReplica {
    pub cluster_id: ClusterId,
    pub replica_id: ReplicaId,
    pub name: String,
    pub serialized_config: SerializedReplicaConfig,
    pub owner_id: RoleId,
}

pub struct Item {
    pub id: GlobalId,
    pub name: QualifiedItemName,
    pub definition: SerializedCatalogItem,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
}

// Structs used internally to represent on disk-state.

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SettingKey {
    name: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct SettingValue {
    value: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct IdAllocKey {
    name: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct IdAllocValue {
    next_id: u64,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct GidMappingKey {
    schema_name: String,
    object_type: CatalogItemType,
    object_name: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct GidMappingValue {
    id: u64,
    fingerprint: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterKey {
    id: ClusterId,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterValue {
    name: String,
    linked_object_id: Option<GlobalId>,
    owner_id: RoleId,
    privileges: Vec<MzAclItem>,
    config: ClusterConfig,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterIntrospectionSourceIndexKey {
    cluster_id: ClusterId,
    name: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterIntrospectionSourceIndexValue {
    index_id: u64,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterReplicaKey {
    id: ReplicaId,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterReplicaValue {
    cluster_id: ClusterId,
    name: String,
    config: SerializedReplicaConfig,
    owner_id: RoleId,
}

#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct DatabaseKey {
    id: DatabaseId,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct DatabaseValue {
    name: String,
    owner_id: RoleId,
    privileges: Vec<MzAclItem>,
}

#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct SchemaKey {
    id: SchemaId,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct SchemaValue {
    database_id: Option<DatabaseId>,
    name: String,
    owner_id: RoleId,
    privileges: Vec<MzAclItem>,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash, Debug, Arbitrary)]
pub struct ItemKey {
    gid: GlobalId,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ItemValue {
    schema_id: SchemaId,
    name: String,
    definition: SerializedCatalogItem,
    owner_id: RoleId,
    privileges: Vec<MzAclItem>,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash, Debug)]
pub struct RoleKey {
    id: RoleId,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Debug)]
pub struct RoleValue {
    // flatten needed for backwards compatibility.
    role: SerializedRole,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ConfigKey {
    key: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ConfigValue {
    value: u64,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct AuditLogKey {
    event: VersionedEvent,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct StorageUsageKey {
    metric: VersionedStorageUsage,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct TimestampKey {
    id: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct TimestampValue {
    ts: mz_repr::Timestamp,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ServerConfigurationKey {
    name: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ServerConfigurationValue {
    value: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct DefaultPrivilegesKey {
    role_id: RoleId,
    database_id: Option<DatabaseId>,
    schema_id: Option<SchemaId>,
    object_type: ObjectType,
    grantee: RoleId,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct DefaultPrivilegesValue {
    privileges: AclMode,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SystemPrivilegesKey {
    privileges: MzAclItem,
}

pub const ALL_COLLECTIONS: &[&str] = &[
    AUDIT_LOG_COLLECTION.name(),
    CLUSTER_COLLECTION.name(),
    CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION.name(),
    CLUSTER_REPLICA_COLLECTION.name(),
    CONFIG_COLLECTION.name(),
    DATABASES_COLLECTION.name(),
    ID_ALLOCATOR_COLLECTION.name(),
    ITEM_COLLECTION.name(),
    ROLES_COLLECTION.name(),
    SCHEMAS_COLLECTION.name(),
    SETTING_COLLECTION.name(),
    STORAGE_USAGE_COLLECTION.name(),
    SYSTEM_CONFIGURATION_COLLECTION.name(),
    SYSTEM_GID_MAPPING_COLLECTION.name(),
    TIMESTAMP_COLLECTION.name(),
    DEFAULT_PRIVILEGES_COLLECTION.name(),
    SYSTEM_PRIVILEGES_COLLECTION.name(),
];

#[cfg(test)]
mod test {
    use mz_proto::{ProtoType, RustType};
    use proptest::prelude::*;

    use super::{DatabaseKey, DatabaseValue, ItemKey, ItemValue, SchemaKey, SchemaValue};

    proptest! {
        #[mz_ore::test]
        fn proptest_database_key_roundtrip(key: DatabaseKey) {
            let proto = key.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(key, round);
        }

        #[mz_ore::test]
        fn proptest_database_value_roundtrip(value: DatabaseValue) {
            let proto = value.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(value, round);
        }

        #[mz_ore::test]
        fn proptest_schema_key_roundtrip(key: SchemaKey) {
            let proto = key.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(key, round);
        }

        #[mz_ore::test]
        fn proptest_schema_value_roundtrip(value: SchemaValue) {
            let proto = value.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(value, round);
        }

        #[mz_ore::test]
        fn proptest_item_key_roundtrip(key: ItemKey) {
            let proto = key.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(key, round);
        }

        #[mz_ore::test]
        fn proptest_item_value_roundtrip(value: ItemValue) {
            let proto = value.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(value, round);
        }
    }
}
